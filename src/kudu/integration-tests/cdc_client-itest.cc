// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// End-to-end tests for the client-side CDC library (CDCClient + CDCConsumer)
// driving a real mini-cluster: stream lifecycle, live tailing, multi-tablet
// fan-out, FULL before/after images, snapshot bootstrap, and durable resume.

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(cdc_bg_scan_interval_ms);

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCDecodedRecord;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamInfo;
using kudu::cdc::CDCStreamOptions;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

namespace {
// Thread-safe collector: the consumer callback runs on one thread per tablet.
class RecordSink {
 public:
  Status Deliver(const CDCRecordBatch& batch) {
    std::lock_guard<std::mutex> l(lock_);
    for (const auto& r : batch.records) {
      records_.push_back(r);
    }
    return Status::OK();
  }

  vector<CDCDecodedRecord> Snapshot() const {
    std::lock_guard<std::mutex> l(lock_);
    return records_;
  }

  int CountOp(cdc::CDCOpTypePB op) const {
    std::lock_guard<std::mutex> l(lock_);
    int n = 0;
    for (const auto& r : records_) {
      if (r.op_type == op) n++;
    }
    return n;
  }

  int Total() const {
    std::lock_guard<std::mutex> l(lock_);
    return static_cast<int>(records_.size());
  }

 private:
  mutable std::mutex lock_;
  vector<CDCDecodedRecord> records_;
};
}  // namespace

class CDCClientITest : public MiniClusterITestBase {
 protected:
  static const char* const kTableName;
  static const MonoDelta kTimeout;

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
    FLAGS_catalog_manager_bg_task_wait_ms = 100;
    FLAGS_cdc_bg_scan_interval_ms = 100;
  }

  // Starts a single-tserver cluster and creates a single-tablet table.
  void StartClusterAndTable() {
    NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
    CreateTable(/*num_partitions=*/1);
  }

  void CreateTable(int num_partitions) {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(b.Build(&schema));

    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name(kTableName)
        .schema(&schema)
        .set_range_partition_columns({"key"})
        .num_replicas(1);
    // Split into 'num_partitions' contiguous range partitions of width 1000.
    for (int i = 0; i < num_partitions; i++) {
      unique_ptr<KuduPartialRow> lb(schema.NewRow());
      unique_ptr<KuduPartialRow> ub(schema.NewRow());
      ASSERT_OK(lb->SetInt32("key", i * 1000));
      ASSERT_OK(ub->SetInt32("key", (i + 1) * 1000));
      creator->add_range_partition(lb.release(), ub.release());
    }
    ASSERT_OK(creator->Create());
  }

  unique_ptr<CDCClient> MakeClient() {
    CDCClient::Options opts;
    opts.master_addresses.push_back(
        cluster_->mini_master()->bound_rpc_addr().ToString());
    opts.rpc_timeout = kTimeout;
    unique_ptr<CDCClient> client;
    CHECK_OK(CDCClient::Create(std::move(opts), &client));
    return client;
  }

  void InsertRows(int start, int count) {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = start; i < start + count; i++) {
      client::KuduInsert* insert = table->NewInsert();
      ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i * 10));
      ASSERT_OK(session->Apply(insert));
    }
  }

  void UpdateRow(int key, int new_val) {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    client::KuduUpdate* update = table->NewUpdate();
    ASSERT_OK(update->mutable_row()->SetInt32("key", key));
    ASSERT_OK(update->mutable_row()->SetInt32("val", new_val));
    ASSERT_OK(session->Apply(update));
  }

  void DeleteRow(int key) {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    client::KuduDelete* del = table->NewDelete();
    ASSERT_OK(del->mutable_row()->SetInt32("key", key));
    ASSERT_OK(session->Apply(del));
  }

  // Finds the first record of the given op-type in 'records', or nullptr.
  static const CDCDecodedRecord* FindOp(const vector<CDCDecodedRecord>& records,
                                        cdc::CDCOpTypePB op) {
    for (const auto& r : records) {
      if (r.op_type == op) return &r;
    }
    return nullptr;
  }

  static string ColValue(const CDCDecodedRecord& r, bool before, const string& name) {
    const auto& cols = before ? r.before : r.after;
    for (const auto& c : cols) {
      if (c.name == name) return c.is_null ? "<null>" : c.value;
    }
    return "<missing>";
  }
};

const char* const CDCClientITest::kTableName = "cdc_client_itest_table";
const MonoDelta CDCClientITest::kTimeout = MonoDelta::FromSeconds(30);

// CreateStream / ListStreams / GetStreamInfo / DeleteStream round-trip.
TEST_F(CDCClientITest, StreamLifecycle) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  opts.record_type = master::CDCStreamConfigPB::FULL;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  ASSERT_FALSE(stream_id.empty());

  vector<CDCStreamInfo> streams;
  ASSERT_OK(client->ListStreams(/*table_id_filter=*/"", &streams));
  ASSERT_EQ(1, streams.size());
  EXPECT_EQ(stream_id, streams[0].stream_id);
  EXPECT_EQ(master::CDCStreamConfigPB::FULL, streams[0].record_type);
  EXPECT_EQ(master::CDCStreamConfigPB::INITIAL_AND_CONTINUE, streams[0].snapshot_mode);

  CDCStreamInfo info;
  ASSERT_OK(client->GetStreamInfo(stream_id, &info));
  EXPECT_EQ(stream_id, info.stream_id);
  ASSERT_EQ(1, info.table_ids.size());

  ASSERT_OK(client->DeleteStream(stream_id));
  ASSERT_OK(client->ListStreams(/*table_id_filter=*/"", &streams));
  EXPECT_TRUE(streams.empty());

  // Describing a deleted stream fails.
  Status s = client->GetStreamInfo(stream_id, &info);
  EXPECT_FALSE(s.ok()) << s.ToString();
}

// Consuming from the earliest WAL delivers all previously-written inserts.
TEST_F(CDCClientITest, ConsumeInsertsFromEarliest) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;  // CHANGE / NEVER
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));

  NO_FATALS(InsertRows(0, 10));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(10, sink.CountOp(cdc::INSERT));
  });
  consumer->Stop();

  // Every INSERT carries a decoded key/val after-image.
  const auto records = sink.Snapshot();
  const auto* first = FindOp(records, cdc::INSERT);
  ASSERT_NE(nullptr, first);
  EXPECT_EQ("0", ColValue(*first, /*before=*/false, "key"));
  EXPECT_EQ("0", ColValue(*first, /*before=*/false, "val"));
}

// Live tailing (kNow) skips prior history but delivers changes written after
// the consumer starts.
TEST_F(CDCClientITest, ConsumeTailFromNow) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));

  // History that must be skipped by a "now" tail.
  NO_FATALS(InsertRows(0, 5));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kNow;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));

  // New rows written after the tail started must be delivered.
  NO_FATALS(InsertRows(100, 3));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(3, sink.CountOp(cdc::INSERT));
  });
  consumer->Stop();

  // The skipped history (keys 0..4) must not appear.
  for (const auto& r : sink.Snapshot()) {
    if (r.op_type == cdc::INSERT) {
      EXPECT_EQ("1", ColValue(r, /*before=*/false, "key").substr(0, 1))
          << "unexpected historical key " << ColValue(r, false, "key");
    }
  }
}

// Fan-out across multiple tablets: all rows across both partitions arrive.
TEST_F(CDCClientITest, ConsumeMultiTablet) {
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateTable(/*num_partitions=*/2));
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));

  // Rows in both partition ranges ([0,1000) and [1000,2000)).
  NO_FATALS(InsertRows(0, 6));
  NO_FATALS(InsertRows(1000, 4));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(10, sink.CountOp(cdc::INSERT));
  });

  // Progress should be reported for two tablets.
  vector<cdc::CDCTabletProgress> progress;
  consumer->GetProgress(&progress);
  EXPECT_EQ(2, progress.size());
  consumer->Stop();
}

// FULL streams: UPDATE and DELETE carry decoded before-images.
TEST_F(CDCClientITest, ConsumeFullBeforeAfterImages) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  opts.record_type = master::CDCStreamConfigPB::FULL;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));

  NO_FATALS(InsertRows(0, 1));   // key=0, val=0
  NO_FATALS(UpdateRow(0, 999));  // val 0 -> 999
  NO_FATALS(DeleteRow(0));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, sink.CountOp(cdc::INSERT));
    ASSERT_EQ(1, sink.CountOp(cdc::UPDATE));
    ASSERT_EQ(1, sink.CountOp(cdc::DELETE));
  });
  consumer->Stop();

  const auto records = sink.Snapshot();
  const auto* upd = FindOp(records, cdc::UPDATE);
  ASSERT_NE(nullptr, upd);
  EXPECT_EQ("0", ColValue(*upd, /*before=*/true, "val"));
  EXPECT_EQ("999", ColValue(*upd, /*before=*/false, "val"));

  const auto* del = FindOp(records, cdc::DELETE);
  ASSERT_NE(nullptr, del);
  EXPECT_EQ("999", ColValue(*del, /*before=*/true, "val"));
}

// Snapshot bootstrap: existing rows arrive as READ records, then live changes
// follow.
TEST_F(CDCClientITest, ConsumeSnapshotThenStream) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  opts.record_type = master::CDCStreamConfigPB::FULL;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));

  // Rows that exist before the consumer starts -> delivered via snapshot.
  NO_FATALS(InsertRows(0, 20));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kSnapshot;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(20, sink.CountOp(cdc::READ));
  });

  // Live changes after the snapshot hand-off must also arrive. Keys stay within
  // the single partition's range [0, 1000) and clear of the snapshot rows.
  NO_FATALS(InsertRows(500, 4));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(4, sink.CountOp(cdc::INSERT));
  });
  consumer->Stop();
}

// After a durable checkpoint (Flush), a fresh consumer resumes from where the
// previous one left off rather than replaying delivered history.
TEST_F(CDCClientITest, ResumeFromDurableCheckpoint) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  NO_FATALS(InsertRows(0, 10));

  // First consumer: read all, checkpoint durably, then stop.
  {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = CDCConsumer::kEarliest;
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
    RecordSink sink;
    ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(10, sink.CountOp(cdc::INSERT));
    });
    ASSERT_OK(consumer->Flush());
    consumer->Stop();
  }

  // The stream now has a durable per-tablet checkpoint. Persistence to the
  // master happens asynchronously after the Checkpoint RPC is acknowledged, so
  // poll until it lands.
  CDCStreamInfo info;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_FALSE(info.tablet_checkpoints.empty());
    int64_t max_cp = 0;
    for (const auto& e : info.tablet_checkpoints) {
      max_cp = std::max(max_cp, e.second);
    }
    ASSERT_GT(max_cp, 0);
  });

  // More rows after the checkpoint.
  NO_FATALS(InsertRows(100, 5));

  // Second consumer resumes from the durable checkpoint (start_mode is only a
  // fallback for tablets without one) and sees only the 5 new rows.
  {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = CDCConsumer::kNow;
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
    RecordSink sink;
    ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(5, sink.CountOp(cdc::INSERT));
    });
    consumer->Stop();
  }
}

}  // namespace kudu
