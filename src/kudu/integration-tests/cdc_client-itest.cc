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
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/cdc/cdc_service.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_bool(cdc_enabled);
DECLARE_bool(cdc_inject_full_reconstruction_timeout);
DECLARE_bool(cdc_inject_tablet_not_running);
DECLARE_bool(enable_txn_system_client_init);
DECLARE_bool(master_support_authz_tokens);
DECLARE_bool(master_support_cdc);
DECLARE_bool(txn_manager_enabled);
DECLARE_bool(txn_manager_lazily_initialized);
DECLARE_bool(tserver_enforce_access_control);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_client_tablet_locations_page_size);
DECLARE_int32(cdc_max_records_per_response);
DECLARE_int32(cdc_max_resnapshot_attempts);
DECLARE_int32(cdc_tablet_rescan_interval_ms);
DECLARE_int32(log_min_segments_to_retain);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_int64(authz_token_validity_seconds);
DECLARE_int64(cdc_max_staleness_ms);
DECLARE_int64(cdc_snapshot_max_bytes_per_response);
DECLARE_int64(cdc_stream_expiry_ms);
DECLARE_int64(rpc_max_message_size);
DECLARE_uint32(txn_manager_status_table_num_replicas);

METRIC_DECLARE_counter(cdc_duplicate_consumers_detected);
METRIC_DECLARE_counter(cdc_maintenance_runs);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_GetTableSchema);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_UpdateCDCCheckpoint);

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCDecodedRecord;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamInfo;
using kudu::cdc::CDCStreamOptions;
using kudu::cdc::CDCTabletProgress;
using kudu::cdc::CDCTabletState;
using kudu::consensus::RaftPeerPB;
using kudu::tablet::TabletReplica;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

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
    FLAGS_cdc_enabled = true;
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
    FLAGS_catalog_manager_bg_task_wait_ms = 100;
    FLAGS_cdc_bg_scan_interval_ms = 100;
  }

  // Starts a single-tserver cluster and creates a single-tablet table.
  void StartClusterAndTable() {
    NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
    CreateTable(/*num_partitions=*/1);
  }

  void CreateTable(int num_partitions, int num_replicas = 1) {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(b.Build(&schema));

    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name(kTableName)
        .schema(&schema)
        .set_range_partition_columns({"key"})
        .num_replicas(num_replicas);
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

  // Adds the range partition [lower, upper).
  void AddRangePartition(int lower, int upper) {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
    unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
    ASSERT_OK(lb->SetInt32("key", lower));
    ASSERT_OK(ub->SetInt32("key", upper));
    alterer->AddRangePartition(lb.release(), ub.release());
    ASSERT_OK(alterer->Alter());
  }

  // Drops the range partition [lower, upper).
  void DropRangePartition(int lower, int upper) {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
    unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
    ASSERT_OK(lb->SetInt32("key", lower));
    ASSERT_OK(ub->SetInt32("key", upper));
    alterer->DropRangePartition(lb.release(), ub.release());
    ASSERT_OK(alterer->Alter());
  }

  // The ids of the table's tablets. Waits while the master answers that a
  // tablet is not running yet, as it does right after a partition was added.
  vector<string> TabletIds(CDCClient* client) {
    cdc::CDCTableMetadata md;
    CHECK_OK(client->GetTableMetadata(kTableName, /*by_id=*/false, &md));
    vector<cdc::CDCTabletInfo> tablets;
    const MonoTime deadline = MonoTime::Now() + kTimeout;
    Status s;
    while (true) {
      s = client->GetTabletLocations(md.table_id, &tablets);
      if (!s.IsServiceUnavailable() || MonoTime::Now() > deadline) {
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(100));
    }
    CHECK_OK(s);
    vector<string> ids;
    for (const auto& t : tablets) {
      ids.push_back(t.tablet_id);
    }
    return ids;
  }

  // The index of the tablet server that leads 'tablet_id', or -1.
  int FindLeaderIndex(const string& tablet_id) {
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      if (!cluster_->mini_tablet_server(i)->is_started()) continue;
      scoped_refptr<TabletReplica> replica;
      if (!cluster_->mini_tablet_server(i)->server()->tablet_manager()
              ->GetTabletReplica(tablet_id, &replica).ok()) {
        continue;
      }
      auto consensus = replica->shared_consensus();
      if (consensus && consensus->role() == RaftPeerPB::LEADER) {
        return i;
      }
    }
    return -1;
  }

  // Moves the leadership of 'tablet_id' away from its current leader.
  void ChangeLeader(const string& tablet_id) {
    int old_leader_idx = -1;
    ASSERT_EVENTUALLY([&] {
      old_leader_idx = FindLeaderIndex(tablet_id);
      ASSERT_GE(old_leader_idx, 0);
    });
    scoped_refptr<TabletReplica> old_leader;
    ASSERT_OK(cluster_->mini_tablet_server(old_leader_idx)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &old_leader));
    ASSERT_EVENTUALLY([&] {
      const int leader_idx = FindLeaderIndex(tablet_id);
      if (leader_idx == old_leader_idx) {
        consensus::LeaderStepDownResponsePB resp;
        WARN_NOT_OK(old_leader->shared_consensus()->StepDown(&resp), "step down failed");
      }
      ASSERT_GE(leader_idx, 0);
      ASSERT_NE(old_leader_idx, leader_idx);
    });
  }

  // Waits until 'sink' has 'n' records of type 'op', and checks that it has
  // no more after the consumer had the time to poll every tablet again, so a
  // late duplicate would show.
  static void WaitForStableCount(const RecordSink& sink, cdc::CDCOpTypePB op, int n) {
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(n, sink.CountOp(op));
    });
    // Longer than the default CDCConsumer::Options::max_poll_backoff.
    SleepFor(MonoDelta::FromMilliseconds(2500));
    ASSERT_EQ(n, sink.CountOp(op));
  }

  static CDCTabletProgress ProgressOf(const CDCConsumer& consumer, const string& tablet_id) {
    vector<CDCTabletProgress> progress;
    consumer.GetProgress(&progress);
    for (const auto& p : progress) {
      if (p.tablet_id == tablet_id) {
        return p;
      }
    }
    CDCTabletProgress none;
    none.tablet_id = "<not polled>";
    return none;
  }

  int64_t DuplicateConsumersDetected(int ts_idx) {
    return METRIC_cdc_duplicate_consumers_detected.Instantiate(
        cluster_->mini_tablet_server(ts_idx)->server()->metric_entity())->value();
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

// DROP TABLE is not deferred for CDC, unlike the drop of a range partition: it
// deletes the table's tablets at once, also while its stream has changes on
// them that the consumer did not read yet, and marks the stream DELETING. The
// loss is not silent: every poller stops in kFailed with an error that names
// the dropped table, and the stream is gone.
TEST_F(CDCClientITest, DropTable_UnreadChangesFailTheConsumerLoudly) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  cdc::CDCTableMetadata md;
  ASSERT_OK(client->GetTableMetadata(kTableName, /*by_id=*/false, &md));
  const vector<string> tablet_ids = TabletIds(client.get());
  ASSERT_FALSE(tablet_ids.empty());
  constexpr int kRows = 10;
  NO_FATALS(InsertRows(0, kRows));

  // The callback holds the first batch until the table is dropped, so the
  // rows written meanwhile are never read.
  CountDownLatch in_first_batch(1);
  CountDownLatch table_dropped(1);
  std::atomic<int> batches(0);
  RecordSink sink;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    if (batches++ == 0) {
      in_first_batch.CountDown();
      table_dropped.Wait();
    }
    return sink.Deliver(b);
  }));
  ASSERT_TRUE(in_first_batch.WaitFor(kTimeout));
  {
    SCOPED_CLEANUP({ table_dropped.CountDown(); });
    NO_FATALS(InsertRows(kRows, kRows));
    ASSERT_OK(client_->DeleteTable(kTableName));
    // The drop marks the stream DELETING at once: it is no longer found.
    CDCStreamInfo info;
    const Status s = client->GetStreamInfo(stream_id, &info);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  }

  for (const auto& tablet_id : tablet_ids) {
    SCOPED_TRACE(tablet_id);
    ASSERT_EVENTUALLY([&] {
      const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
      ASSERT_EQ(CDCTabletState::kFailed, p.state) << cdc::CDCTabletStateToString(p.state);
      ASSERT_NE(string::npos, p.terminal_error.ToString().find(
          Substitute("its table $0 was dropped", md.table_id))) << p.terminal_error.ToString();
    });
  }
  consumer->Stop();
  // The rows written after the first batch were never delivered.
  EXPECT_LT(sink.CountOp(cdc::INSERT), 2 * kRows);
  // The maintenance passes reap the stream.
  ASSERT_EVENTUALLY([&] {
    vector<CDCStreamInfo> streams;
    ASSERT_OK(client->ListStreams(/*table_id_filter=*/"", &streams,
                                  /*include_deleting=*/true));
    for (const auto& st : streams) {
      ASSERT_NE(stream_id, st.stream_id);
    }
  });
}

// Dropping a range partition ends its tablet's poller with the drop signal
// (kDropped, with the partition's bounds) after every change of the tablet,
// the poller is removed by the next ReScan(), and the master deletes the
// tablet once the consumer acknowledged the drop, which it does only after
// the signal was delivered. Repeated add/drop cycles leave no pollers behind.
TEST_F(CDCClientITest, DropRangePartition_ConsumerGetsDropSignalAndRetiresPoller) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const vector<string> initial_tablets = TabletIds(client.get());
  ASSERT_EQ(1, initial_tablets.size());

  std::mutex stopped_lock;
  map<string, CDCTabletProgress> stopped;
  // The tablets whose drop the master learned before the signal was delivered.
  vector<string> acked_before_signal;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
    // The master keeps the tablet while the signal is being delivered.
    for (int i = 0; p.state == CDCTabletState::kDropped && i < 10; ++i) {
      CDCStreamInfo info;
      CHECK_OK(client->GetStreamInfo(stream_id, &info));
      const auto* d = FindOrNull(info.dropped_tablets, p.tablet_id);
      if (!d || d->state() != master::CDCStreamDroppedTabletPB::RETAINED) {
        std::lock_guard<std::mutex> l(stopped_lock);
        acked_before_signal.emplace_back(p.tablet_id);
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(100));
    }
    std::lock_guard<std::mutex> l(stopped_lock);
    stopped[p.tablet_id] = p;
  };
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));

  constexpr int kCycles = 3;
  constexpr int kRowsPerPartition = 10;
  for (int cycle = 0; cycle < kCycles; ++cycle) {
    SCOPED_TRACE(cycle);
    const int lower = (cycle + 1) * 1000;
    const int upper = lower + 1000;
    NO_FATALS(AddRangePartition(lower, upper));
    string tablet_id;
    for (const auto& id : TabletIds(client.get())) {
      if (id != initial_tablets[0]) {
        tablet_id = id;
      }
    }
    ASSERT_FALSE(tablet_id.empty());
    ASSERT_OK(consumer->ReScan());
    NO_FATALS(InsertRows(lower, kRowsPerPartition));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ((cycle + 1) * kRowsPerPartition, sink.CountOp(cdc::INSERT));
    });

    NO_FATALS(DropRangePartition(lower, upper));
    ASSERT_EVENTUALLY([&] {
      std::lock_guard<std::mutex> l(stopped_lock);
      const auto* p = FindOrNull(stopped, tablet_id);
      ASSERT_NE(nullptr, p);
      ASSERT_EQ(CDCTabletState::kDropped, p->state);
      EXPECT_FALSE(p->dropped_tail_lost);
      EXPECT_NE(string::npos, p->dropped_partition_description.find(
          Substitute("$0 <= VALUES < $1", lower, upper))) << p->dropped_partition_description;
      EXPECT_FALSE(p->dropped_partition.partition_key_start().empty());
    });

    // The poller is removed once it acknowledged the drop: only the initial
    // tablet is polled.
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(consumer->ReScan());
      vector<CDCTabletProgress> progress;
      consumer->GetProgress(&progress);
      ASSERT_EQ(1, progress.size());
      EXPECT_EQ(initial_tablets[0], progress[0].tablet_id);
      EXPECT_EQ(CDCTabletState::kStreaming, progress[0].state);
    });

    // The consumer acknowledged the drop, so the master deleted the tablet.
    ASSERT_EVENTUALLY([&] {
      scoped_refptr<TabletReplica> replica;
      ASSERT_FALSE(cluster_->mini_tablet_server(0)->server()->tablet_manager()
                       ->LookupTablet(tablet_id, &replica));
      CDCStreamInfo info;
      ASSERT_OK(client->GetStreamInfo(stream_id, &info));
      ASSERT_FALSE(ContainsKey(info.dropped_tablets, tablet_id));
      ASSERT_FALSE(ContainsKey(info.tablet_checkpoints, tablet_id));
    });
  }

  // The remaining tablet still streams.
  NO_FATALS(InsertRows(0, 5));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(kCycles * kRowsPerPartition + 5, sink.CountOp(cdc::INSERT));
  });
  consumer->Stop();
  std::lock_guard<std::mutex> l(stopped_lock);
  EXPECT_TRUE(acked_before_signal.empty()) << JoinStrings(acked_before_signal, ",");
}

// A rescan does not wait for the stop callback of a dropped tablet: the poller
// is removed only by a rescan after the callback returned.
TEST_F(CDCClientITest, DropRangePartition_ReScanDoesNotWaitForStopCallback) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateTable(/*num_partitions=*/2));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  ASSERT_EQ(2, TabletIds(client.get()).size());

  CountDownLatch entered(1);
  CountDownLatch release(1);
  std::atomic<bool> returned(false);
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
    if (p.state != CDCTabletState::kDropped) {
      return;
    }
    entered.CountDown();
    release.WaitFor(MonoDelta::FromSeconds(10));
    returned.store(true);
  };
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  // Lets the callback return if an assertion fails.
  auto cleanup = MakeScopedCleanup([&]() {
    release.CountDown();
    consumer->Stop();
  });
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  NO_FATALS(InsertRows(1000, 5));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(5, sink.CountOp(cdc::INSERT));
  });

  NO_FATALS(DropRangePartition(1000, 2000));
  ASSERT_TRUE(entered.WaitFor(kTimeout));
  ASSERT_OK(consumer->ReScan());
  EXPECT_FALSE(returned.load());
  vector<CDCTabletProgress> progress;
  consumer->GetProgress(&progress);
  ASSERT_EQ(2, progress.size());

  release.CountDown();
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(consumer->ReScan());
    consumer->GetProgress(&progress);
    ASSERT_EQ(1, progress.size());
    EXPECT_EQ(CDCTabletState::kStreaming, progress[0].state);
  });
  EXPECT_TRUE(returned.load());
}

// A rescan that was running when Stop() began removes no poller, not even the
// finished poller of a dropped tablet: Stop() joins the pollers without the
// lock. The rescan is held after its master RPCs while a record callback
// starts stopping the consumer (a Stop() from a callback keeps the pollers
// until the next Stop()), then let go.
TEST_F(CDCClientITest, DropRangePartition_ReScanDuringStopRemovesNoPoller) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateTable(/*num_partitions=*/2));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  ASSERT_EQ(2, TabletIds(client.get()).size());

  std::mutex dropped_lock;
  string dropped_tablet_id;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
    if (p.state == CDCTabletState::kDropped) {
      std::lock_guard<std::mutex> l(dropped_lock);
      dropped_tablet_id = p.tablet_id;
    }
  };
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  CountDownLatch in_rescan(1);
  CountDownLatch resume(1);
  std::thread rescanner;
  auto cleanup = MakeScopedCleanup([&]() {
    resume.CountDown();
    if (rescanner.joinable()) {
      rescanner.join();
    }
    consumer->Stop();
  });
  RecordSink sink;
  std::atomic<bool> stop_in_callback(false);
  CountDownLatch stopped_in_callback(1);
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    RETURN_NOT_OK(sink.Deliver(b));
    if (stop_in_callback.exchange(false)) {
      // On a poller thread: only asks the pollers to stop.
      consumer->Stop();
      stopped_in_callback.CountDown();
    }
    return Status::OK();
  }));
  NO_FATALS(InsertRows(1000, 5));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(5, sink.CountOp(cdc::INSERT));
  });

  // The poller of the dropped tablet is done: it delivered the signal and
  // acknowledged the drop, so the master deleted the tablet.
  NO_FATALS(DropRangePartition(1000, 2000));
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
    std::lock_guard<std::mutex> l(dropped_lock);
    ASSERT_FALSE(dropped_tablet_id.empty());
    tablet_id = dropped_tablet_id;
  });
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_FALSE(ContainsKey(info.dropped_tablets, tablet_id));
    ASSERT_FALSE(ContainsKey(info.tablet_checkpoints, tablet_id));
  });
  vector<CDCTabletProgress> progress;
  consumer->GetProgress(&progress);
  ASSERT_EQ(2, progress.size());

  // Hold a rescan after its master RPCs, then start stopping the consumer.
  consumer->SetReScanHookForTests([&]() {
    in_rescan.CountDown();
    resume.WaitFor(kTimeout);
  });
  Status rescan_status;
  rescanner = std::thread([&]() { rescan_status = consumer->ReScan(); });
  ASSERT_TRUE(in_rescan.WaitFor(kTimeout));
  stop_in_callback = true;
  NO_FATALS(InsertRows(0, 1));
  ASSERT_TRUE(stopped_in_callback.WaitFor(kTimeout));
  resume.CountDown();
  rescanner.join();

  EXPECT_TRUE(rescan_status.IsAborted()) << rescan_status.ToString();
  consumer->GetProgress(&progress);
  ASSERT_EQ(2, progress.size());
  bool dropped_listed = false;
  for (const auto& p : progress) {
    if (p.tablet_id == tablet_id) {
      EXPECT_EQ(CDCTabletState::kDropped, p.state);
      dropped_listed = true;
    }
  }
  EXPECT_TRUE(dropped_listed);

  consumer->Stop();
  consumer->GetProgress(&progress);
  EXPECT_TRUE(progress.empty());
}

// A consumer that stops after it delivered the drop signal of a tablet, but
// before it acknowledged the drop, leaves the tablet kept: the next consumer
// gets the signal again. Here the acknowledgement fails because the tablet
// server is down.
TEST_F(CDCClientITest, DropRangePartition_SignalRedeliveredWhenAckIsLost) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateTable(/*num_partitions=*/2));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_EQ(2, info.tablet_checkpoints.size());
  });

  std::mutex stopped_lock;
  vector<CDCTabletProgress> stopped;
  CountDownLatch entered(1);
  CountDownLatch release(1);
  auto make_options = [&]() {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = CDCConsumer::kEarliest;
    copts.min_poll_backoff = MonoDelta::FromMilliseconds(50);
    copts.max_poll_backoff = MonoDelta::FromMilliseconds(200);
    copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
      if (p.state != CDCTabletState::kDropped) {
        return;
      }
      {
        std::lock_guard<std::mutex> l(stopped_lock);
        stopped.emplace_back(p);
      }
      entered.CountDown();
      release.WaitFor(kTimeout);
    };
    return copts;
  };
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), make_options(), &consumer));
  auto cleanup = MakeScopedCleanup([&]() {
    release.CountDown();
    consumer->Stop();
  });
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  NO_FATALS(InsertRows(1000, 5));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(5, sink.CountOp(cdc::INSERT));
  });

  NO_FATALS(DropRangePartition(1000, 2000));
  ASSERT_TRUE(entered.WaitFor(kTimeout));
  {
    std::lock_guard<std::mutex> l(stopped_lock);
    ASSERT_EQ(1, stopped.size());
    tablet_id = stopped[0].tablet_id;
  }
  // The acknowledgement after the signal fails.
  cluster_->mini_tablet_server(0)->Shutdown();
  release.CountDown();
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(consumer->ReScan());
    vector<CDCTabletProgress> progress;
    consumer->GetProgress(&progress);
    ASSERT_EQ(1, progress.size());
  });
  consumer->Stop();
  {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    const auto* d = FindOrNull(info.dropped_tablets, tablet_id);
    ASSERT_NE(nullptr, d);
    ASSERT_EQ(master::CDCStreamDroppedTabletPB::RETAINED, d->state());
  }

  // The next consumer gets the signal again, and acknowledges the drop.
  ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());
  ASSERT_OK(CDCConsumer::Create(client.get(), make_options(), &consumer));
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    std::lock_guard<std::mutex> l(stopped_lock);
    ASSERT_EQ(2, stopped.size());
    EXPECT_EQ(tablet_id, stopped[1].tablet_id);
    EXPECT_FALSE(stopped[1].dropped_tail_lost);
  });
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_FALSE(ContainsKey(info.dropped_tablets, tablet_id));
    ASSERT_FALSE(ContainsKey(info.tablet_checkpoints, tablet_id));
  });
}

// A consumer that starts after a range partition was dropped reads the rest of
// the kept tablet's changes from its leader, which it finds by trying the
// tablet's replicas, and gets the drop signal although a leader change wrote
// an op after the tablet's last write.
TEST_F(CDCClientITest, DropRangePartition_ConsumerReadsKeptTabletAfterLeaderChange) {
  NO_FATALS(StartCluster(/*num_tablet_servers=*/3));
  NO_FATALS(CreateTable(/*num_partitions=*/1, /*num_replicas=*/3));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const vector<string> initial_tablets = TabletIds(client.get());
  ASSERT_EQ(1, initial_tablets.size());
  NO_FATALS(AddRangePartition(1000, 2000));
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    for (const auto& e : info.tablet_checkpoints) {
      if (e.first != initial_tablets[0]) {
        tablet_id = e.first;
      }
    }
    ASSERT_FALSE(tablet_id.empty());
  });
  NO_FATALS(InsertRows(1000, 10));
  NO_FATALS(DropRangePartition(1000, 2000));

  // Move the leadership to a replica other than the first one the master
  // lists, so that the consumer has to try more than one.
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(ChangeLeader(tablet_id));
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    const auto* d = FindOrNull(info.dropped_tablets, tablet_id);
    ASSERT_NE(nullptr, d);
    ASSERT_EQ(master::CDCStreamDroppedTabletPB::RETAINED, d->state());
    ASSERT_EQ(3, d->replicas_size());
    const int leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
    const auto& addr = cluster_->mini_tablet_server(leader_idx)->bound_rpc_addr();
    ASSERT_FALSE(d->replicas(0).host() == addr.host() && d->replicas(0).port() == addr.port());
  });

  std::mutex stopped_lock;
  map<string, CDCTabletProgress> stopped;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
    std::lock_guard<std::mutex> l(stopped_lock);
    stopped[p.tablet_id] = p;
  };
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    std::lock_guard<std::mutex> l(stopped_lock);
    const auto* p = FindOrNull(stopped, tablet_id);
    ASSERT_NE(nullptr, p);
    ASSERT_EQ(CDCTabletState::kDropped, p->state);
    EXPECT_FALSE(p->dropped_tail_lost);
  });
  EXPECT_EQ(10, sink.CountOp(cdc::INSERT));
  ASSERT_EVENTUALLY([&] {
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      scoped_refptr<TabletReplica> replica;
      ASSERT_FALSE(cluster_->mini_tablet_server(i)->server()->tablet_manager()
                       ->LookupTablet(tablet_id, &replica));
    }
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_FALSE(ContainsKey(info.dropped_tablets, tablet_id));
  });
  consumer->Stop();
}

// A stream whose snapshot mode is INITIAL_ONLY outputs a snapshot only: the
// consumer must start with a snapshot, and each tablet's poller stops in
// kSnapshotComplete after it, instead of failing on the WAL that the stream
// does not stream. A consumer that starts after the snapshot was done finds
// nothing to do.
TEST_F(CDCClientITest, InitialOnly_ConsumerEndsInSnapshotComplete) {
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateTable(/*num_partitions=*/2));
  unique_ptr<CDCClient> client = MakeClient();
  NO_FATALS(InsertRows(0, 10));
  NO_FATALS(InsertRows(1000, 10));

  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_ONLY;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));

  unique_ptr<CDCConsumer> consumer;
  for (const auto mode : { CDCConsumer::kNow, CDCConsumer::kEarliest }) {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = mode;
    Status s = CDCConsumer::Create(client.get(), std::move(copts), &consumer);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }

  std::atomic<int> num_stopped(0);
  const auto make_options = [&]() {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = CDCConsumer::kSnapshot;
    copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
      EXPECT_EQ(CDCTabletState::kSnapshotComplete, p.state);
      num_stopped++;
    };
    return copts;
  };
  const auto expect_all_complete = [&](const CDCConsumer& c) {
    vector<CDCTabletProgress> progress;
    c.GetProgress(&progress);
    ASSERT_EQ(2, progress.size());
    for (const auto& p : progress) {
      ASSERT_EQ(CDCTabletState::kSnapshotComplete, p.state) << p.terminal_error.ToString();
    }
  };
  ASSERT_OK(CDCConsumer::Create(client.get(), make_options(), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] { NO_FATALS(expect_all_complete(*consumer)); });
  EXPECT_EQ(20, sink.CountOp(cdc::READ));
  EXPECT_EQ(20, sink.Total());
  EXPECT_EQ(2, num_stopped.load());
  ASSERT_OK(consumer->Flush());
  consumer->Stop();

  // Once the master knows the snapshots are done, a new consumer of the
  // stream has nothing to deliver.
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_TRUE(info.tablets_in_snapshot.empty());
    ASSERT_EQ(2, info.tablet_checkpoints.size());
  });
  num_stopped = 0;
  ASSERT_OK(CDCConsumer::Create(client.get(), make_options(), &consumer));
  RecordSink sink2;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink2.Deliver(b); }));
  ASSERT_EVENTUALLY([&] { NO_FATALS(expect_all_complete(*consumer)); });
  EXPECT_EQ(0, sink2.Total());
  EXPECT_EQ(2, num_stopped.load());
  consumer->Stop();

  // A stream that takes no snapshots cannot be consumed starting with one.
  string never_stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &never_stream_id));
  CDCConsumer::Options copts;
  copts.stream_id = never_stream_id;
  copts.start_mode = CDCConsumer::kSnapshot;
  Status s = CDCConsumer::Create(client.get(), std::move(copts), &consumer);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

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

// Against a master without CDC support (for example, one not upgraded yet),
// the client's CDC calls fail with NotSupported, not with a misleading "unable
// to reach a leader master".
TEST_F(CDCClientITest, CreateStreamAgainstNonCdcMaster) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  FLAGS_master_support_cdc = false;
  string stream_id;
  Status s = client->CreateStream(kTableName, CDCStreamOptions(), &stream_id);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  vector<CDCStreamInfo> streams;
  s = client->ListStreams(/*table_id_filter=*/"", &streams);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();

  FLAGS_master_support_cdc = true;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
}

// --cdc_enabled controls only the creation of streams: a stream that exists
// when it is turned off can still be listed, described, consumed and deleted
// through the client.
TEST_F(CDCClientITest, CdcDisabled_ExistingStreamStillUsable) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  NO_FATALS(InsertRows(0, 5));

  FLAGS_cdc_enabled = false;
  string other_stream_id;
  Status s = client->CreateStream(kTableName, CDCStreamOptions(), &other_stream_id);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "--cdc_enabled");

  vector<CDCStreamInfo> streams;
  ASSERT_OK(client->ListStreams(/*table_id_filter=*/"", &streams));
  ASSERT_EQ(1, streams.size());
  EXPECT_EQ(stream_id, streams[0].stream_id);
  CDCStreamInfo info;
  ASSERT_OK(client->GetStreamInfo(stream_id, &info));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.checkpoint_interval = MonoDelta::FromMilliseconds(100);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  NO_FATALS(InsertRows(5, 5));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(10, sink.CountOp(cdc::INSERT));
    ASSERT_GT(ProgressOf(*consumer, tablet_id).last_checkpointed_op_index, 0);
  });
  ASSERT_OK(consumer->ReScan());
  consumer->Stop();

  ASSERT_OK(client->DeleteStream(stream_id));
  ASSERT_OK(client->ListStreams(/*table_id_filter=*/"", &streams));
  EXPECT_TRUE(streams.empty());
}

// With fine-grained authorization enforced, the tablet servers reject an
// expired authz token. The consumer gets a new one and keeps streaming and
// checkpointing.
TEST_F(CDCClientITest, SecureCluster_TokenExpiry_ConsumerContinues) {
  FLAGS_tserver_enforce_access_control = true;
  FLAGS_authz_token_validity_seconds = 2;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const string tablet_id = TabletIds(client.get())[0];

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.checkpoint_interval = MonoDelta::FromMilliseconds(100);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));

  NO_FATALS(InsertRows(0, 5));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(5, sink.CountOp(cdc::INSERT));
  });
  int64_t checkpoint = -1;
  ASSERT_EVENTUALLY([&] {
    checkpoint = ProgressOf(*consumer, tablet_id).last_checkpointed_op_index;
    ASSERT_GT(checkpoint, 0);
  });

  // Write while the consumer's token expires a few times.
  for (int i = 1; i <= 4; ++i) {
    SleepFor(MonoDelta::FromMilliseconds(2500));
    NO_FATALS(InsertRows(i * 5, 5));
  }
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_EQ(25, sink.CountOp(cdc::INSERT));
    ASSERT_EQ(CDCTabletState::kStreaming, p.state) << p.terminal_error.ToString();
    ASSERT_GT(p.last_checkpointed_op_index, checkpoint);
  });
  // The tablet server did reject the expired token.
  EXPECT_GT(ProgressOf(*consumer, tablet_id).num_errors, 0);
  consumer->Stop();
}

// A token that the tablet servers keep rejecting however often it is renewed
// (here: the master issues none; in practice, e.g., a tablet server clock
// ahead of the master's by more than the token validity) does not make the
// pollers renew it in a tight loop: the pollers share a renewal and back off
// as for any other error.
TEST_F(CDCClientITest, SecureCluster_RejectedTokenRenewalBacksOff) {
  FLAGS_tserver_enforce_access_control = true;
  FLAGS_master_support_authz_tokens = false;
  constexpr int kNumTablets = 4;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateTable(kNumTablets));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));

  const auto num_get_table_schema = [&] {
    return METRIC_handler_latency_kudu_master_MasterService_GetTableSchema.Instantiate(
        cluster_->mini_master()->master()->metric_entity())->TotalCount();
  };
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  const uint64_t num_before = num_get_table_schema();
  ASSERT_OK(consumer->Start([](const CDCRecordBatch& /*batch*/) { return Status::OK(); }));
  SleepFor(MonoDelta::FromSeconds(6));
  const uint64_t num_renewals = num_get_table_schema() - num_before;
  LOG(INFO) << "authz token renewals: " << num_renewals;

  vector<CDCTabletProgress> progress;
  consumer->GetProgress(&progress);
  ASSERT_EQ(kNumTablets, progress.size());
  for (const auto& p : progress) {
    EXPECT_GT(p.num_errors, 0) << p.tablet_id;
    EXPECT_EQ(CDCTabletState::kStreaming, p.state) << p.terminal_error.ToString();
  }
  // Backing off from 200 ms to 2 s, a poller is rejected at most seven times
  // in six seconds. Renewing at the minimum backoff after each rejection, each
  // poller would renew about 25 times.
  EXPECT_LE(num_renewals, 8 * kNumTablets);
  consumer->Stop();
}

// The authz tokens of a consumer's pollers expire together: the consumer gets
// one new token for all of them, not one per poller.
TEST_F(CDCClientITest, SecureCluster_TokenExpiry_RenewedOncePerConsumer) {
  FLAGS_tserver_enforce_access_control = true;
  FLAGS_authz_token_validity_seconds = 2;
  constexpr int kNumTablets = 4;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateTable(kNumTablets));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  // Only GetChanges uses the token, so that every rejection is a poller's.
  copts.checkpoint_enabled = false;
  // Poll often, so that every poller finds its token expired soon after it
  // expires.
  copts.max_poll_backoff = copts.min_poll_backoff;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  // Without help, a poller that polls after another poller got the new token
  // never sees the expired one. So each poller that is rejected first waits
  // until every poller was rejected. Then they get a new token, and once it
  // expires too, every poller is rejected again and waits until the end of the
  // test, with no new token taken: by then, each poller was rejected for both
  // tokens, and the consumer renewed the token once.
  std::atomic<int> num_rejections(0);
  CountDownLatch first_expiry(kNumTablets);
  CountDownLatch second_expiry(kNumTablets);
  CountDownLatch test_done(1);
  consumer->SetAuthzTokenRejectedHookForTests([&] {
    if (num_rejections++ < kNumTablets) {
      first_expiry.CountDown();
      first_expiry.WaitFor(kTimeout);
    } else {
      second_expiry.CountDown();
      test_done.WaitFor(kTimeout);
    }
  });
  auto stop_consumer = MakeScopedCleanup([&] {
    test_done.CountDown();
    consumer->Stop();
  });
  const auto num_get_table_schema = [&] {
    return METRIC_handler_latency_kudu_master_MasterService_GetTableSchema.Instantiate(
        cluster_->mini_master()->master()->metric_entity())->TotalCount();
  };
  const uint64_t num_before = num_get_table_schema();
  ASSERT_OK(consumer->Start([](const CDCRecordBatch& /*batch*/) { return Status::OK(); }));
  ASSERT_TRUE(second_expiry.WaitFor(kTimeout));
  EXPECT_EQ(2 * kNumTablets, num_rejections);
  EXPECT_EQ(1, num_get_table_schema() - num_before);

  vector<CDCTabletProgress> progress;
  consumer->GetProgress(&progress);
  ASSERT_EQ(kNumTablets, progress.size());
  for (const auto& p : progress) {
    EXPECT_EQ(CDCTabletState::kStreaming, p.state) << p.terminal_error.ToString();
    EXPECT_GE(p.num_errors, 2) << p.tablet_id;
  }
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
  NO_FATALS(WaitForStableCount(sink, cdc::INSERT, 10));
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
  NO_FATALS(WaitForStableCount(sink, cdc::INSERT, 3));
  consumer->Stop();

  // Only the new rows: the skipped history (keys 0..4) must not appear.
  std::set<string> keys;
  for (const auto& r : sink.Snapshot()) {
    if (r.op_type == cdc::INSERT) {
      keys.insert(ColValue(r, /*before=*/false, "key"));
    }
  }
  EXPECT_EQ((std::set<string>{ "100", "101", "102" }), keys);
}

// A kNow consumer whose start position probe fails starts its poller at the
// "from now" sentinel and anchors on the first successful poll, which also asks
// for the schema. Before, that poll failed with SCHEMA_UNAVAILABLE and the
// poller stopped in kNeedsResnapshot.
TEST_F(CDCClientITest, ConsumeTailFromNow_AnchorProbeFails_StillStreams) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  NO_FATALS(InsertRows(0, 2));
  const string tablet_id = TabletIds(client.get())[0];

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kNow;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  FLAGS_cdc_inject_tablet_not_running = true;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  SCOPED_CLEANUP({
    FLAGS_cdc_inject_tablet_not_running = false;
    consumer->Stop();
  });
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(ProgressOf(*consumer, tablet_id).num_errors, 1);
  });
  FLAGS_cdc_inject_tablet_not_running = false;
  // Wait for the first successful poll, which anchors the poller.
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_EQ(CDCTabletState::kStreaming, p.state) << p.terminal_error.ToString();
    ASSERT_TRUE(p.last_error.ok()) << p.last_error.ToString();
  });

  NO_FATALS(InsertRows(100, 1));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, sink.CountOp(cdc::INSERT));
  });
  const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
  EXPECT_EQ(CDCTabletState::kStreaming, p.state) << p.terminal_error.ToString();
  const auto records = sink.Snapshot();
  const CDCDecodedRecord* ins = FindOp(records, cdc::INSERT);
  ASSERT_NE(nullptr, ins);
  EXPECT_EQ("100", ColValue(*ins, /*before=*/false, "key"));
  // The schema record of the first poll is not delivered.
  EXPECT_EQ(nullptr, FindOp(records, cdc::DDL));
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
  NO_FATALS(WaitForStableCount(sink, cdc::INSERT, 10));

  // Progress should be reported for two tablets.
  vector<cdc::CDCTabletProgress> progress;
  consumer->GetProgress(&progress);
  EXPECT_EQ(2, progress.size());
  consumer->Stop();
}

// A page of GetTableLocations starts at the tablet that contains, or precedes,
// the requested key. When a page ends right before a non-covered range, the
// next page starts again with the previous page's last tablet. That tablet
// must be listed, and polled, once.
TEST_F(CDCClientITest, TabletLocations_PageBoundaryBeforeRangeGap_NoDuplicates) {
  // Each page ends at a hash bucket's last range or right before the gap.
  FLAGS_cdc_client_tablet_locations_page_size = 2;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));

  // HASH(key) x 2 buckets, and the bounded ranges [0, 1000) and [2000, 3000).
  // In partition key order: (b0, r0), (b0, r1), (b1, r0), (b1, r1), with a
  // non-covered range after each of them.
  client::KuduSchema schema;
  client::KuduSchemaBuilder b;
  b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(b.Build(&schema));
  unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
  creator->table_name(kTableName)
      .schema(&schema)
      .add_hash_partitions({ "key" }, 2)
      .set_range_partition_columns({ "key" })
      .num_replicas(1);
  for (const auto& range : vector<std::pair<int, int>>{ { 0, 1000 }, { 2000, 3000 } }) {
    unique_ptr<KuduPartialRow> lb(schema.NewRow());
    unique_ptr<KuduPartialRow> ub(schema.NewRow());
    ASSERT_OK(lb->SetInt32("key", range.first));
    ASSERT_OK(ub->SetInt32("key", range.second));
    creator->add_range_partition(lb.release(), ub.release());
  }
  ASSERT_OK(creator->Create());

  unique_ptr<CDCClient> client = MakeClient();
  const vector<string> ids = TabletIds(client.get());
  const std::set<string> unique_ids(ids.begin(), ids.end());
  ASSERT_EQ(4, unique_ids.size());
  ASSERT_EQ(unique_ids.size(), ids.size()) << JoinStrings(ids, ",");

  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  constexpr int kRowsPerRange = 20;
  NO_FATALS(InsertRows(0, kRowsPerRange));
  NO_FATALS(InsertRows(2000, kRowsPerRange));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  SCOPED_CLEANUP({ consumer->Stop(); });

  // One poller per tablet.
  vector<CDCTabletProgress> progress;
  consumer->GetProgress(&progress);
  vector<string> polled;
  for (const auto& p : progress) {
    polled.push_back(p.tablet_id);
  }
  ASSERT_EQ(unique_ids.size(), polled.size()) << JoinStrings(polled, ",");
  ASSERT_EQ(unique_ids, std::set<string>(polled.begin(), polled.end()));

  // Every row is delivered exactly once.
  NO_FATALS(WaitForStableCount(sink, cdc::INSERT, 2 * kRowsPerRange));
  std::set<string> keys;
  for (const auto& r : sink.Snapshot()) {
    if (r.op_type != cdc::INSERT) {
      continue;
    }
    for (const auto& c : r.after) {
      if (c.name == "key") {
        ASSERT_TRUE(keys.insert(c.value).second) << "key delivered twice: " << c.value;
      }
    }
  }
  ASSERT_EQ(2 * kRowsPerRange, keys.size());
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

// Restarts a consumer whose predecessor did not complete a snapshot, with the
// start mode given as the test parameter.
class CDCClientRestartITest : public CDCClientITest,
                              public ::testing::WithParamInterface<CDCConsumer::StartMode> {
 protected:
  // Starts a consumer in the test's start mode and checks that it delivers
  // every row of the table as a READ record, then the changes after it.
  void RestartAndExpectAllRows(CDCClient* client, const string& stream_id, int num_rows) {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = GetParam();
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client, std::move(copts), &consumer));
    RecordSink sink;
    ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
    ASSERT_EVENTUALLY([&] {
      std::set<string> keys;
      for (const auto& r : sink.Snapshot()) {
        if (r.op_type == cdc::READ) {
          keys.insert(ColValue(r, /*before=*/false, "key"));
        }
      }
      ASSERT_EQ(num_rows, keys.size());
    });
    NO_FATALS(InsertRows(500, 3));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(3, sink.CountOp(cdc::INSERT));
    });
    consumer->Stop();
  }
};

INSTANTIATE_TEST_SUITE_P(
    StartModes, CDCClientRestartITest,
    ::testing::Values(CDCConsumer::kSnapshot, CDCConsumer::kNow, CDCConsumer::kEarliest),
    [](const ::testing::TestParamInfo<CDCConsumer::StartMode>& info) {
      switch (info.param) {
        case CDCConsumer::kSnapshot: return "Snapshot";
        case CDCConsumer::kNow: return "Now";
        case CDCConsumer::kEarliest: return "Earliest";
      }
      return "Unknown";
    });

// A consumer that stops in the middle of a snapshot leaves the tablet's
// snapshot in progress on the master. A new consumer must take the snapshot
// again before it streams the tablet, whatever its start mode: before, it took
// the streaming start point that the first page persisted for a checkpoint,
// streamed the WAL from there and never delivered the rows of the pages it had
// not read.
TEST_P(CDCClientRestartITest, ConsumerRestartMidSnapshot_DeliversAllRows) {
  // A page per row or two, so the snapshot takes many pages.
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  const int kRows = 100;
  NO_FATALS(InsertRows(0, kRows));

  // The first consumer stops after a few pages.
  {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = CDCConsumer::kSnapshot;
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
    std::atomic<int> batches(0);
    ASSERT_OK(consumer->Start([&](const CDCRecordBatch& /*b*/) {
      return ++batches > 3 ? Status::Aborted("the consumer stops") : Status::OK();
    }));
    ASSERT_EVENTUALLY([&] {
      ASSERT_GT(batches.load(), 3);
    });
    consumer->Stop();
  }
  // The master knows the snapshot is in progress.
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info, /*include_details=*/true));
    ASSERT_EQ(1, info.tablet_status.size());
    ASSERT_TRUE(info.tablet_status[0].snapshot_active());
  });

  NO_FATALS(RestartAndExpectAllRows(client.get(), stream_id, kRows));
}

// A consumer that fails to deliver the last page of a snapshot leaves the
// snapshot in progress too: the page is not acknowledged until the consumer
// streams the WAL after it. Before, the last page completed the snapshot on the
// master before its response was sent, so a new consumer streamed the WAL and
// never delivered the rows of that page.
TEST_P(CDCClientRestartITest, ConsumerFailsOnLastSnapshotPage_RestartDeliversAllRows) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  const int kRows = 100;
  NO_FATALS(InsertRows(0, kRows));

  // The snapshot is a single page, and the first consumer fails to deliver it.
  {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = CDCConsumer::kSnapshot;
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
    std::atomic<int> batches(0);
    std::atomic<bool> last_page(false);
    ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
      ++batches;
      last_page = b.records.size() == kRows;
      return Status::Aborted("the consumer fails");
    }));
    ASSERT_EVENTUALLY([&] {
      ASSERT_GE(batches.load(), 1);
    });
    consumer->Stop();
    ASSERT_EQ(1, batches.load());
    ASSERT_TRUE(last_page.load());
  }
  {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_EQ(1, info.tablets_in_snapshot.size());
  }

  NO_FATALS(RestartAndExpectAllRows(client.get(), stream_id, kRows));
}

// A consumer that restarts an unfinished snapshot after the tablet server lost
// the snapshot session ('lose_session' makes it lose the session) takes it
// again at the same snapshot timestamp, so the stream after the snapshot has
// every change after the rows it delivered. Here the restarted consumer must
// get the DELETE of a row that the first consumer delivered. Before, the
// tablet server that had no session took a new snapshot at a later timestamp:
// the row was in neither that snapshot nor the stream after it, and stayed in
// the sink.
class CDCSnapshotRestartITest : public CDCClientITest {
 protected:
  void RestartAfterSessionLossKeepsSnapTs(
      int num_tablet_servers, const std::function<void(const string&)>& lose_session) {
    FLAGS_cdc_tablet_rescan_interval_ms = 0;
    // A page per row or two, so the snapshot takes many pages.
    FLAGS_cdc_snapshot_max_bytes_per_response = 64;
    NO_FATALS(StartCluster(num_tablet_servers));
    NO_FATALS(CreateTable(/*num_partitions=*/1, /*num_replicas=*/num_tablet_servers));
    unique_ptr<CDCClient> client = MakeClient();
    string stream_id;
    CDCStreamOptions opts;
    opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
    ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
    const string tablet_id = TabletIds(client.get())[0];
    const int kRows = 100;
    NO_FATALS(InsertRows(0, kRows));

    const auto make_options = [&]() {
      CDCConsumer::Options copts;
      copts.stream_id = stream_id;
      copts.start_mode = CDCConsumer::kSnapshot;
      return copts;
    };
    // The first consumer stops after three pages.
    RecordSink first_sink;
    {
      unique_ptr<CDCConsumer> consumer;
      ASSERT_OK(CDCConsumer::Create(client.get(), make_options(), &consumer));
      std::atomic<int> batches(0);
      ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
        return ++batches > 3 ? Status::Aborted("the consumer stops") : first_sink.Deliver(b);
      }));
      ASSERT_EVENTUALLY([&] {
        ASSERT_GT(batches.load(), 3);
      });
      consumer->Stop();
    }
    const vector<CDCDecodedRecord> first = first_sink.Snapshot();
    ASSERT_FALSE(first.empty());
    const uint64_t snap_ts = first[0].timestamp;
    std::set<string> first_keys;
    for (const auto& r : first) {
      ASSERT_EQ(cdc::READ, r.op_type);
      ASSERT_EQ(snap_ts, r.timestamp);
      first_keys.insert(ColValue(r, /*before=*/false, "key"));
    }
    ASSERT_TRUE(ContainsKey(first_keys, "0"));
    ASSERT_LT(first_keys.size(), kRows);
    ASSERT_EVENTUALLY([&] {
      CDCStreamInfo info;
      ASSERT_OK(client->GetStreamInfo(stream_id, &info));
      ASSERT_TRUE(ContainsKey(info.tablets_in_snapshot, tablet_id));
    });

    // Delete a row that the first consumer delivered, and lose the session.
    NO_FATALS(DeleteRow(0));
    NO_FATALS(lose_session(tablet_id));

    RecordSink second_sink;
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client.get(), make_options(), &consumer));
    ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return second_sink.Deliver(b); }));
    ASSERT_EVENTUALLY([&] {
      const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
      ASSERT_EQ(CDCTabletState::kStreaming, p.state) << p.terminal_error.ToString();
      ASSERT_EQ(1, second_sink.CountOp(cdc::DELETE));
    });
    consumer->Stop();

    // The snapshot was read at the first consumer's snapshot timestamp, and
    // the sink that applied both consumers' records in order has every row but
    // the deleted one.
    std::set<string> keys;
    for (const auto* sink : {&first_sink, &second_sink}) {
      for (const auto& r : sink->Snapshot()) {
        const string key = ColValue(r, /*before=*/false, "key");
        switch (r.op_type) {
          case cdc::READ:
            EXPECT_EQ(snap_ts, r.timestamp) << "READ record of key " << key
                                            << " from a new snapshot";
            keys.insert(key);
            break;
          case cdc::INSERT:
            keys.insert(key);
            break;
          case cdc::DELETE:
            EXPECT_EQ("0", key);
            keys.erase(key);
            break;
          default:
            break;
        }
      }
    }
    EXPECT_EQ(kRows - 1, keys.size());
    EXPECT_FALSE(ContainsKey(keys, "0")) << "a deleted row stayed in the sink";
  }
};

TEST_F(CDCSnapshotRestartITest, Snapshot_RestartAfterLeaderChange_KeepsSnapTs) {
  NO_FATALS(RestartAfterSessionLossKeepsSnapTs(
      /*num_tablet_servers=*/3, [&](const string& tablet_id) {
        NO_FATALS(ChangeLeader(tablet_id));
      }));
}

TEST_F(CDCSnapshotRestartITest, Snapshot_RestartAfterTabletServerRestart_KeepsSnapTs) {
  NO_FATALS(RestartAfterSessionLossKeepsSnapTs(
      /*num_tablet_servers=*/1, [&](const string& tablet_id) {
        cluster_->mini_tablet_server(0)->Shutdown();
        ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());
        ASSERT_EVENTUALLY([&] {
          ASSERT_EQ(0, FindLeaderIndex(tablet_id));
        });
      }));
}

// RestartTablet(snapshot=false) of a poller that stopped in its snapshot takes
// that snapshot again at its snapshot timestamp, also after the tablet server
// lost the session; RestartTablet(snapshot=true) takes a new snapshot. Before,
// both took a new snapshot once the session was lost.
TEST_F(CDCSnapshotRestartITest, Snapshot_RestartTablet_ContinuesOrTakesNewSnapshot) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  const int kRows = 100;
  NO_FATALS(InsertRows(0, kRows));

  // The callback accepts 'budget' batches, then fails. The READ timestamps of
  // the batches of each poller ('phase') are kept.
  std::atomic<int> budget(3);
  std::atomic<int> phase(0);
  std::mutex lock;
  vector<std::set<uint64_t>> timestamps(3);
  std::set<string> last_keys;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kSnapshot;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    if (budget.fetch_sub(1) <= 0) {
      return Status::Aborted("the callback fails");
    }
    std::lock_guard<std::mutex> l(lock);
    for (const auto& r : b.records) {
      if (r.op_type == cdc::READ) {
        timestamps[phase].insert(r.timestamp);
        if (phase == 2) {
          last_keys.insert(ColValue(r, /*before=*/false, "key"));
        }
      }
    }
    return Status::OK();
  }));
  const auto wait_for_state = [&](CDCTabletState state) {
    ASSERT_EVENTUALLY([&] {
      const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
      ASSERT_EQ(state, p.state) << cdc::CDCTabletStateToString(p.state) << ": "
                                << p.terminal_error.ToString();
    });
  };
  NO_FATALS(wait_for_state(CDCTabletState::kFailed));

  // The tablet server loses the session.
  cluster_->mini_tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->mini_tablet_server(0)->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, FindLeaderIndex(tablet_id));
  });

  phase = 1;
  budget = 2;
  ASSERT_OK(consumer->RestartTablet(tablet_id, /*snapshot=*/false));
  NO_FATALS(wait_for_state(CDCTabletState::kFailed));

  phase = 2;
  budget = std::numeric_limits<int>::max() / 2;
  ASSERT_OK(consumer->RestartTablet(tablet_id, /*snapshot=*/true));
  NO_FATALS(wait_for_state(CDCTabletState::kStreaming));
  consumer->Stop();

  std::lock_guard<std::mutex> l(lock);
  ASSERT_EQ(1, timestamps[0].size());
  const uint64_t snap_ts = *timestamps[0].begin();
  EXPECT_EQ(timestamps[0], timestamps[1]) << "RestartTablet(snapshot=false) took a new snapshot";
  ASSERT_EQ(1, timestamps[2].size());
  EXPECT_GT(*timestamps[2].begin(), snap_ts)
      << "RestartTablet(snapshot=true) did not take a new snapshot";
  EXPECT_EQ(kRows, last_keys.size());
}

// A snapshot that loses its history in the middle is taken again as a new
// snapshot when --cdc_max_resnapshot_attempts allows it: the poller asks for a
// new snapshot generation. Without that, the tablet server would refuse to
// continue the lost snapshot again and again, and the poller would stop in
// kNeedsResnapshot.
TEST_F(CDCSnapshotRestartITest, Snapshot_HistoryLost_ResnapshotTakesNewSnapshot) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;
  FLAGS_cdc_max_resnapshot_attempts = 1;
  // The master does not push the history floor of the snapshot again while
  // the test collects the history.
  FLAGS_cdc_bg_scan_interval_ms = 3600 * 1000;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  const int kRows = 50;
  NO_FATALS(InsertRows(0, kRows));

  // The callback holds the second batch until the history is gone.
  CountDownLatch in_second_batch(1);
  CountDownLatch history_gone(1);
  std::atomic<int> batches(0);
  std::mutex lock;
  vector<uint64_t> read_timestamps;
  std::set<string> keys_after_loss;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kSnapshot;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    if (++batches == 2) {
      in_second_batch.CountDown();
      history_gone.Wait();
    }
    std::lock_guard<std::mutex> l(lock);
    for (const auto& r : b.records) {
      if (r.op_type != cdc::READ) continue;
      if (read_timestamps.empty() || read_timestamps.back() != r.timestamp) {
        read_timestamps.push_back(r.timestamp);
      }
      if (read_timestamps.size() > 1) {
        keys_after_loss.insert(ColValue(r, /*before=*/false, "key"));
      }
    }
    return Status::OK();
  }));
  ASSERT_TRUE(in_second_batch.WaitFor(kTimeout));
  {
    SCOPED_CLEANUP({ history_gone.CountDown(); });
    scoped_refptr<TabletReplica> replica;
    ASSERT_OK(cluster_->mini_tablet_server(0)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &replica));
    for (int i = 0; i < kRows; ++i) {
      NO_FATALS(UpdateRow(i, -1));
    }
    const std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
    tablet->ClearCDCHistoryFloorHolders();
    FLAGS_tablet_history_max_age_sec = 0;
    ASSERT_OK(tablet->Flush());
    ASSERT_OK(tablet->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
    FLAGS_tablet_history_max_age_sec = 3600;
    ASSERT_GT(tablet->cdc_history_gc_water_mark(), Timestamp(0));
  }
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_EQ(CDCTabletState::kStreaming, p.state)
        << cdc::CDCTabletStateToString(p.state) << ": " << p.terminal_error.ToString();
  });
  consumer->Stop();
  std::lock_guard<std::mutex> l(lock);
  ASSERT_EQ(2, read_timestamps.size());
  EXPECT_GT(read_timestamps[1], read_timestamps[0]);
  EXPECT_EQ(kRows, keys_after_loss.size());
}

// The first page of a new snapshot generation (new_snapshot_generation) is
// answered only after the master has its cursor. Until then the master has the
// cursor of the earlier generation, and a new leader would continue that one
// from the client's next resume key, which is a position of the new one.
TEST_F(CDCSnapshotRestartITest, Snapshot_NewGenerationCursorPersistedWithFirstPage) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  NO_FATALS(InsertRows(0, 20));
  const HostPort leader(cluster_->mini_tablet_server(0)->bound_rpc_addr());
  const auto start = [&](bool new_generation, uint64_t* snap_ts) {
    cdc::GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_is_snapshot_start(true);
    req.set_new_snapshot_generation(new_generation);
    cdc::GetChangesResponsePB resp;
    ASSERT_OK(client->GetChanges(leader, req, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
    ASSERT_FALSE(resp.snapshot_done());
    ASSERT_GT(resp.records_size(), 0);
    *snap_ts = resp.records(0).timestamp();
  };
  const auto durable_snap_ts = [&]() {
    master::GetCDCStreamInfoRequestPB req;
    req.set_stream_id(stream_id);
    master::GetCDCStreamInfoResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(kTimeout);
    master::MasterServiceProxy proxy(client->messenger(),
                                     cluster_->mini_master()->bound_rpc_addr(),
                                     cluster_->mini_master()->bound_rpc_addr().host());
    CHECK_OK(proxy.GetCDCStreamInfo(req, &resp, &rpc));
    CHECK(!resp.has_error()) << resp.error().ShortDebugString();
    for (const auto& cur : resp.stream().tablet_snapshot_cursors()) {
      if (cur.tablet_id() == tablet_id && cur.snapshot_active()) {
        return cur.snapshot_snap_ts();
      }
    }
    return static_cast<uint64_t>(0);
  };
  const auto sync_persists = [&]() {
    return METRIC_handler_latency_kudu_master_MasterService_UpdateCDCCheckpoint.Instantiate(
        cluster_->mini_master()->master()->metric_entity())->TotalCount();
  };

  uint64_t first_snap_ts = 0;
  NO_FATALS(start(/*new_generation=*/false, &first_snap_ts));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(first_snap_ts, durable_snap_ts());
  });
  const uint64_t persists = sync_persists();
  uint64_t second_snap_ts = 0;
  NO_FATALS(start(/*new_generation=*/true, &second_snap_ts));
  ASSERT_GT(second_snap_ts, first_snap_ts);
  EXPECT_EQ(persists + 1, sync_persists());
  EXPECT_EQ(second_snap_ts, durable_snap_ts());
}

// The ways a snapshot is abandoned past the master's retention valve, and
// what the history at its snapshot timestamp is when its consumer returns.
struct AbandonedSnapshotCase {
  const char* name;
  master::CDCStreamConfigPB::SnapshotMode snapshot_mode;
  // The consumer is gone for longer than --cdc_stream_expiry_ms if true, its
  // snapshot cursor does not advance for longer than --cdc_max_staleness_ms
  // otherwise.
  bool expire;
  // The history at the snapshot timestamp is removed before the consumer
  // returns.
  bool history_gone;
};

void PrintTo(const AbandonedSnapshotCase& c, std::ostream* os) {
  *os << c.name;
}

class CDCAbandonedSnapshotITest : public CDCClientITest,
                                  public ::testing::WithParamInterface<AbandonedSnapshotCase> {
};

INSTANTIATE_TEST_SUITE_P(
    Cases, CDCAbandonedSnapshotITest,
    ::testing::Values(
        AbandonedSnapshotCase{"Continue_Stale",
                              master::CDCStreamConfigPB::INITIAL_AND_CONTINUE, false, false},
        AbandonedSnapshotCase{"Continue_Expired",
                              master::CDCStreamConfigPB::INITIAL_AND_CONTINUE, true, false},
        AbandonedSnapshotCase{"Continue_Stale_HistoryGone",
                              master::CDCStreamConfigPB::INITIAL_AND_CONTINUE, false, true},
        AbandonedSnapshotCase{"InitialOnly_Stale",
                              master::CDCStreamConfigPB::INITIAL_ONLY, false, false},
        AbandonedSnapshotCase{"InitialOnly_Expired",
                              master::CDCStreamConfigPB::INITIAL_ONLY, true, false},
        AbandonedSnapshotCase{"InitialOnly_Stale_HistoryGone",
                              master::CDCStreamConfigPB::INITIAL_ONLY, false, true}),
    [](const ::testing::TestParamInfo<AbandonedSnapshotCase>& info) {
      return string(info.param.name);
    });

// A snapshot whose consumer went away for longer than the master's staleness
// or expiry valve loses its retention, but it is not complete: a consumer that
// returns takes it again at its snapshot timestamp and delivers every row, or,
// if that history is gone, stops in kNeedsResnapshot. Before, the maintenance
// pass that released the retention also cleared the snapshot's cursor, so the
// master reported its streaming start point as the tablet's checkpoint: the
// returning consumer streamed the WAL from there (INITIAL_ONLY: reported
// kSnapshotComplete at once) and never delivered the rows that the first
// consumer had not read.
TEST_P(CDCAbandonedSnapshotITest, Snapshot_AbandonedPastStaleness_NotResumedAsComplete) {
  const AbandonedSnapshotCase& param = GetParam();
  const bool initial_only = param.snapshot_mode == master::CDCStreamConfigPB::INITIAL_ONLY;
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  // A page per row or two, so the snapshot takes many pages.
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = param.snapshot_mode;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  const int kRows = 100;
  NO_FATALS(InsertRows(0, kRows));

  const auto make_options = [&]() {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = CDCConsumer::kSnapshot;
    return copts;
  };
  // The first consumer stops after three pages.
  RecordSink first_sink;
  {
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client.get(), make_options(), &consumer));
    std::atomic<int> batches(0);
    ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
      return ++batches > 3 ? Status::Aborted("the consumer stops") : first_sink.Deliver(b);
    }));
    ASSERT_EVENTUALLY([&] {
      ASSERT_GT(batches.load(), 3);
    });
    consumer->Stop();
  }
  const vector<CDCDecodedRecord> first = first_sink.Snapshot();
  ASSERT_FALSE(first.empty());
  ASSERT_LT(first.size(), kRows);
  const uint64_t snap_ts = first[0].timestamp;
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_TRUE(ContainsKey(info.tablets_in_snapshot, tablet_id));
  });

  // The consumer stays away for longer than the valve. A maintenance pass that
  // started after the tablet's row became stale (or expired) has released the
  // tablet's retention.
  if (param.expire) {
    FLAGS_cdc_stream_expiry_ms = 1000;
  } else {
    FLAGS_cdc_max_staleness_ms = 1000;
  }
  auto* cdc = cluster_->mini_tablet_server(0)->server()->cdc_service();
  const auto maintenance_runs = [&]() {
    return METRIC_cdc_maintenance_runs.Instantiate(
        cluster_->mini_master()->master()->metric_entity())->value();
  };
  int64_t runs_when_released = 0;
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info, /*include_details=*/true));
    ASSERT_EQ(1, info.tablet_status.size());
    ASSERT_TRUE(param.expire ? info.tablet_status[0].expired()
                             : info.tablet_status[0].stale());
    runs_when_released = maintenance_runs();
  });
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(maintenance_runs(), runs_when_released + 2);
    ASSERT_FALSE(cdc->RetentionBarrierForTests(tablet_id));
  });
  // The snapshot is still unfinished on the master.
  {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info, /*include_details=*/true));
    EXPECT_TRUE(ContainsKey(info.tablets_in_snapshot, tablet_id));
    EXPECT_FALSE(ContainsKey(info.tablet_checkpoints, tablet_id));
    ASSERT_EQ(1, info.tablet_status.size());
    EXPECT_TRUE(info.tablet_status[0].snapshot_active());
  }

  // The tablet server no longer has the snapshot's session.
  cdc->ClearSnapshotSessionsForTests();
  if (param.history_gone) {
    for (int i = 0; i < kRows; ++i) {
      NO_FATALS(UpdateRow(i, -1));
    }
    scoped_refptr<TabletReplica> replica;
    ASSERT_OK(cluster_->mini_tablet_server(0)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &replica));
    const std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
    tablet->ClearCDCHistoryFloorHolders();
    const int32_t history_max_age_sec = FLAGS_tablet_history_max_age_sec;
    FLAGS_tablet_history_max_age_sec = 0;
    ASSERT_OK(tablet->Flush());
    ASSERT_OK(tablet->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
    FLAGS_tablet_history_max_age_sec = history_max_age_sec;
    ASSERT_GT(tablet->cdc_history_gc_water_mark(), Timestamp(snap_ts));
  }

  // A consumer returns.
  RecordSink second_sink;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), make_options(), &consumer));
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return second_sink.Deliver(b); }));
  if (param.history_gone) {
    // The snapshot cannot be continued, and is not taken anew without the
    // application's consent (--cdc_max_resnapshot_attempts=0).
    ASSERT_EVENTUALLY([&] {
      const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
      ASSERT_EQ(CDCTabletState::kNeedsResnapshot, p.state)
          << cdc::CDCTabletStateToString(p.state) << ": " << p.terminal_error.ToString();
    });
    consumer->Stop();
    EXPECT_EQ(0, second_sink.Total());
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    EXPECT_TRUE(ContainsKey(info.tablets_in_snapshot, tablet_id));
    return;
  }
  const CDCTabletState done =
      initial_only ? CDCTabletState::kSnapshotComplete : CDCTabletState::kStreaming;
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_EQ(done, p.state)
        << cdc::CDCTabletStateToString(p.state) << ": " << p.terminal_error.ToString();
  });
  // The snapshot was taken again from the start, at its snapshot timestamp.
  std::set<string> keys;
  for (const auto& r : second_sink.Snapshot()) {
    ASSERT_EQ(cdc::READ, r.op_type);
    EXPECT_EQ(snap_ts, r.timestamp);
    keys.insert(ColValue(r, /*before=*/false, "key"));
  }
  EXPECT_EQ(kRows, keys.size()) << "rows of the abandoned snapshot were skipped";
  if (!initial_only) {
    NO_FATALS(InsertRows(500, 3));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(3, second_sink.CountOp(cdc::INSERT));
    });
  }
  consumer->Stop();
}

// A table whose primary key is a single STRING column, for the tests of the
// empty key.
class CDCStringKeyITest : public CDCClientITest {
 protected:
  static const char* const kStringKeyTable;

  void CreateStringKeyTable() {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::STRING)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(client::KuduColumnSchema::STRING)->NotNull();
    ASSERT_OK(b.Build(&schema));
    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(kStringKeyTable)
                  .schema(&schema)
                  .set_range_partition_columns({"key"})
                  .num_replicas(1)
                  .Create());
  }

  // Inserts a row with a value larger than a 64-byte snapshot page.
  void InsertStringKeyRow(const string& key) {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kStringKeyTable, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    client::KuduInsert* ins = table->NewInsert();
    ASSERT_OK(ins->mutable_row()->SetString("key", key));
    ASSERT_OK(ins->mutable_row()->SetString("val", string(100, 'x')));
    ASSERT_OK(session->Apply(ins));
  }

  // The STRING key of a READ or INSERT record.
  static string RawKey(const cdc::CDCRecordPB& r) {
    for (const auto& c : r.changes()) {
      if (c.column_name() == "key") return c.value();
    }
    return "<missing>";
  }
};

const char* const CDCStringKeyITest::kStringKeyTable = "cdc_client_itest_string_key";

// The snapshot of a table whose primary key is a single STRING column, with a
// row whose key is the empty string, completes: the first page ends after that
// row, and its resume key, the encoded key of the row, is empty. Before, the
// consumer sent no resume key for an empty one, so its next call was not a
// snapshot call: it streamed the WAL from op 0, delivered the WAL records as
// snapshot output, and never completed the snapshot.
TEST_F(CDCStringKeyITest, Snapshot_EmptyStringPrimaryKey) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  // A page per row.
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateStringKeyTable());
  for (const char* key : {"", "a", "b"}) {
    NO_FATALS(InsertStringKeyRow(key));
  }

  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kStringKeyTable, opts, &stream_id));
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kSnapshot;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    vector<CDCTabletProgress> progress;
    consumer->GetProgress(&progress);
    ASSERT_EQ(1, progress.size());
    ASSERT_TRUE(progress[0].snapshot_done) << progress[0].last_error.ToString();
    ASSERT_EQ(CDCTabletState::kStreaming, progress[0].state)
        << progress[0].terminal_error.ToString();
  });
  NO_FATALS(InsertStringKeyRow("c"));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, sink.CountOp(cdc::INSERT));
  });
  consumer->Stop();

  // Each row once as a READ record, then the INSERT: no WAL record was
  // delivered as part of the snapshot.
  const auto raw_key = [](const CDCDecodedRecord& r) {
    for (const auto& c : r.after) {
      if (c.name == "key") return c.raw_value;
    }
    return string("<missing>");
  };
  const vector<CDCDecodedRecord> records = sink.Snapshot();
  ASSERT_EQ(4, records.size());
  std::multiset<string> read_keys;
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(cdc::READ, records[i].op_type);
    read_keys.insert(raw_key(records[i]));
  }
  EXPECT_EQ((std::multiset<string>{"", "a", "b"}), read_keys);
  EXPECT_EQ(cdc::INSERT, records[3].op_type);
  EXPECT_EQ("c", raw_key(records[3]));
}

// The tablet server refuses a call that is neither a snapshot start nor a
// continuation while the last page of the snapshot in progress ended on the
// empty key, and it continues the snapshot after the empty key for an empty
// resume key. Before, such a call streamed the WAL from op 0 (and the
// reference consumer delivered those records as the rest of the snapshot),
// and an empty resume key, taken for the start of the table, served the first
// page again.
TEST_F(CDCStringKeyITest, Snapshot_EmptyResumeKey_ContinuesAfterEmptyKey) {
  // A page per row.
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateStringKeyTable());
  for (const char* key : {"", "a", "b"}) {
    NO_FATALS(InsertStringKeyRow(key));
  }
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kStringKeyTable, opts, &stream_id));
  cdc::CDCTableMetadata md;
  ASSERT_OK(client->GetTableMetadata(kStringKeyTable, /*by_id=*/false, &md));
  vector<cdc::CDCTabletInfo> tablets;
  ASSERT_OK(client->GetTabletLocations(md.table_id, &tablets));
  ASSERT_EQ(1, tablets.size());
  const auto get_changes = [&](cdc::GetChangesRequestPB req, cdc::GetChangesResponsePB* resp) {
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablets[0].tablet_id);
    return client->GetChanges(tablets[0].leader, req, resp);
  };

  cdc::GetChangesResponsePB first;
  {
    cdc::GetChangesRequestPB req;
    req.set_is_snapshot_start(true);
    ASSERT_OK(get_changes(req, &first));
  }
  ASSERT_FALSE(first.has_error()) << first.error().ShortDebugString();
  ASSERT_FALSE(first.snapshot_done());
  ASSERT_EQ(1, first.records_size());
  ASSERT_EQ("", RawKey(first.records(0)));
  ASSERT_TRUE(first.has_snapshot_resume_key());
  ASSERT_EQ("", first.snapshot_resume_key());

  // The resume key taken for none.
  {
    cdc::GetChangesRequestPB req;
    req.set_from_op_index(0);
    cdc::GetChangesResponsePB resp;
    ASSERT_OK(get_changes(req, &resp));
    ASSERT_TRUE(resp.has_error()) << "WAL streamed in the middle of the snapshot: "
                                  << resp.records_size() << " records";
    EXPECT_EQ(cdc::CDCErrorPB::UNKNOWN_ERROR, resp.error().code());
    EXPECT_STR_CONTAINS(resp.error().status().message(), "empty snapshot_resume_key");
    EXPECT_EQ(0, resp.records_size());
  }
  // The empty resume key continues after the empty key.
  vector<string> keys;
  string resume_key = first.snapshot_resume_key();
  for (int pages = 0;; ++pages) {
    ASSERT_LT(pages, 10);
    cdc::GetChangesRequestPB req;
    req.set_snapshot_resume_key(resume_key);
    cdc::GetChangesResponsePB resp;
    ASSERT_OK(get_changes(req, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
    for (const auto& r : resp.records()) {
      keys.push_back(RawKey(r));
    }
    if (resp.snapshot_done()) break;
    ASSERT_TRUE(resp.has_snapshot_resume_key());
    resume_key = resp.snapshot_resume_key();
  }
  EXPECT_EQ((vector<string>{"a", "b"}), keys);
}

// A snapshot page that a replica serves while it loses leadership (here in the
// middle of the snapshot establish, after the replica ended its sessions on
// learning the new leader) fails as TABLET_NOT_LEADER and ends the session it
// created, so that no session of a non-leader pins history at its snapshot
// timestamp. Before, the page was served and the session stayed on the deposed
// leader: nothing ended it, and its snapshot kept the history floor at
// snap_ts, also once that replica led again.
TEST_F(CDCClientITest, Snapshot_EstablishOnDeposedLeaderLeavesNoSession) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/3));
  NO_FATALS(CreateTable(/*num_partitions=*/1, /*num_replicas=*/3));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  NO_FATALS(InsertRows(0, 10));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  tserver::TabletServer* ts = cluster_->mini_tablet_server(leader_idx)->server();
  cdc::CDCServiceImpl* cdc = ts->cdc_service();
  scoped_refptr<TabletReplica> replica;
  ASSERT_OK(ts->tablet_manager()->GetTabletReplica(tablet_id, &replica));
  std::atomic<bool> hook_ran(false);
  cdc->SetSnapshotEstablishHookForTests([&]() {
    hook_ran = true;
    consensus::LeaderStepDownResponsePB resp;
    CHECK_OK(replica->shared_consensus()->StepDown(&resp));
    // Wait until another replica leads and this one has heard from it, and
    // end the sessions of this replica, as it does when it learns of the new
    // leader.
    const MonoTime deadline = MonoTime::Now() + kTimeout;
    while (true) {
      const int idx = FindLeaderIndex(tablet_id);
      if (idx >= 0 && idx != leader_idx) {
        consensus::ConsensusStatePB cstate;
        CHECK_OK(replica->shared_consensus()->ConsensusState(&cstate));
        if (cstate.leader_uuid() == cluster_->mini_tablet_server(idx)->uuid()) {
          break;
        }
      }
      CHECK(MonoTime::Now() < deadline) << "no new leader";
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    cdc->EndSessionsIfNotLeader(tablet_id);
  });
  cdc::GetChangesRequestPB req;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_is_snapshot_start(true);
  cdc::GetChangesResponsePB resp;
  const Status s = client->GetChanges(
      HostPort(cluster_->mini_tablet_server(leader_idx)->bound_rpc_addr()), req, &resp);
  cdc->SetSnapshotEstablishHookForTests(nullptr);
  ASSERT_OK(s);
  ASSERT_TRUE(hook_ran);
  ASSERT_TRUE(resp.has_error()) << "served by a replica that is not the leader: "
                                << resp.records_size() << " records";
  EXPECT_EQ(cdc::CDCErrorPB::TABLET_NOT_LEADER, resp.error().code())
      << resp.error().ShortDebugString();
  EXPECT_EQ(0, resp.records_size());
  EXPECT_FALSE(cdc->HasSessionForTests(stream_id, tablet_id));
  EXPECT_EQ(Timestamp(0), replica->shared_tablet()->cdc_history_floor());
}

// ReScan takes the snapshot of a tablet that it finds with its snapshot in
// progress, whatever the consumer's start mode, like Start() does. Before, a
// kNow consumer started such a tablet at its current tail, past every row the
// snapshot had not delivered.
TEST_F(CDCClientITest, ReScan_CompletesSnapshotInProgress) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kNow;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  cdc::CDCTableMetadata md;
  ASSERT_OK(client->GetTableMetadata(kTableName, /*by_id=*/false, &md));
  vector<cdc::CDCTabletInfo> tablets;
  ASSERT_OK(client->GetTabletLocations(md.table_id, &tablets));
  ASSERT_EQ(1, tablets.size());
  const string old_tablet = tablets[0].tablet_id;

  // Add a partition and fill it.
  {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
    unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
    ASSERT_OK(lb->SetInt32("key", 1000));
    ASSERT_OK(ub->SetInt32("key", 2000));
    alterer->AddRangePartition(lb.release(), ub.release());
    ASSERT_OK(alterer->Alter());
  }
  const int kRows = 20;
  NO_FATALS(InsertRows(1000, kRows));

  // An earlier consumer read the first page of the new tablet's snapshot.
  string new_tablet;
  ASSERT_EVENTUALLY([&] {
    tablets.clear();
    ASSERT_OK(client->GetTabletLocations(md.table_id, &tablets));
    ASSERT_EQ(2, tablets.size());
    for (const auto& t : tablets) {
      if (t.tablet_id == old_tablet) continue;
      new_tablet = t.tablet_id;
      ASSERT_TRUE(t.leader.Initialized());
      cdc::GetChangesRequestPB req;
      req.set_stream_id(stream_id);
      req.set_tablet_id(t.tablet_id);
      req.set_is_snapshot_start(true);
      cdc::GetChangesResponsePB resp;
      ASSERT_OK(client->GetChanges(t.leader, req, &resp));
      ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
      ASSERT_FALSE(resp.snapshot_done());
    }
  });
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_EQ(1, info.tablets_in_snapshot.count(new_tablet));
  });

  ASSERT_OK(consumer->ReScan());
  ASSERT_EVENTUALLY([&] {
    std::set<string> keys;
    for (const auto& r : sink.Snapshot()) {
      if (r.op_type == cdc::READ) {
        keys.insert(ColValue(r, /*before=*/false, "key"));
      }
    }
    ASSERT_EQ(kRows, keys.size());
  });
  consumer->Stop();
}

// ReScan discovers a tablet added by ADD_RANGE_PARTITION after Start() and
// delivers records from the new partition. This exercises the consumer-side
// half of the end-to-end partition-change fix.
TEST_F(CDCClientITest, ReScan_PicksUpNewPartition) {
  // Disable the automatic background rescan thread for determinism; we call
  // ReScan() manually at exactly the right moment.
  FLAGS_cdc_tablet_rescan_interval_ms = 0;

  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();

  string stream_id;
  CDCStreamOptions opts;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));

  // Write 5 rows into the initial partition [0, 1000).
  NO_FATALS(InsertRows(0, 5));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));

  // Wait for the first 5 rows to arrive.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(5, sink.CountOp(cdc::INSERT));
  });

  // Add a second range partition [1000, 2000). The client library calls the
  // master to add the partition; the master's eager hook (or reconcile pass)
  // writes the op_index=0 checkpoint row so the new tablet's WAL is retained.
  {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
    unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
    ASSERT_OK(lb->SetInt32("key", 1000));
    ASSERT_OK(ub->SetInt32("key", 2000));
    alterer->AddRangePartition(lb.release(), ub.release());
    ASSERT_OK(alterer->Alter());
  }

  // ReScan must discover the new tablet and spawn a poller for it, once the
  // master knows the tablet's location.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(consumer->ReScan());
    vector<CDCTabletProgress> progress;
    consumer->GetProgress(&progress);
    ASSERT_EQ(2, progress.size());
  });

  // Write 5 rows into the new partition [1000, 2000).
  NO_FATALS(InsertRows(1000, 5));

  // All 10 rows (5 from each partition) must eventually be delivered.
  NO_FATALS(WaitForStableCount(sink, cdc::INSERT, 10));

  consumer->Stop();
}

// A kNow consumer reads a partition added after it started from the
// partition's first op: every change of the new tablet is new. Before, ReScan()
// started the tablet at its current tail, past the rows written before the
// rescan found it.
TEST_F(CDCClientITest, ReScan_KNowConsumerDeliversWritesMadeBeforeDiscovery) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kNow;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));

  NO_FATALS(AddRangePartition(1000, 2000));
  const int kRows = 100;
  NO_FATALS(InsertRows(1000, kRows));
  ASSERT_OK(consumer->ReScan());
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(kRows, sink.CountOp(cdc::INSERT));
  });
  consumer->Stop();
}

// A kNow consumer that starts after a partition was added (and that no earlier
// consumer read) reads the partition from its first op too: the master marks
// the tablet as added after the stream was created. Before, the restarted
// consumer started the tablet at its current tail.
TEST_F(CDCClientITest, Restart_NewPartitionNotSkipped) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const vector<string> old_tablets = TabletIds(client.get());
  ASSERT_EQ(1, old_tablets.size());

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kNow;
  {
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client.get(), copts, &consumer));
    ASSERT_OK(consumer->Start([](const CDCRecordBatch& /*b*/) { return Status::OK(); }));
    NO_FATALS(AddRangePartition(1000, 2000));
    NO_FATALS(InsertRows(1000, 50));
    // The consumer stops before it found the new partition.
    consumer->Stop();
  }
  string new_tablet;
  for (const auto& id : TabletIds(client.get())) {
    if (id != old_tablets[0]) new_tablet = id;
  }
  ASSERT_FALSE(new_tablet.empty());
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_EQ(1, info.tablets_added_after_creation.count(new_tablet));
    ASSERT_EQ(0, info.tablets_added_after_creation.count(old_tablets[0]));
  });

  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), copts, &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(50, sink.CountOp(cdc::INSERT));
  });
  consumer->Stop();
}

// A kNow consumer checkpoints the tail it starts from right away, so that a
// consumer that crashes before its first periodic checkpoint resumes from
// there rather than from a later tail. Before, nothing was checkpointed until
// Options::checkpoint_interval passed.
TEST_F(CDCClientITest, KNow_StartPositionCheckpointedAtStart) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  NO_FATALS(InsertRows(0, 5));
  const string tablet_id = TabletIds(client.get())[0];

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kNow;
  copts.checkpoint_interval = MonoDelta::FromSeconds(3600);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  ASSERT_OK(consumer->Start([](const CDCRecordBatch& /*b*/) { return Status::OK(); }));
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_GT(FindWithDefault(info.tablet_checkpoints, tablet_id, -1), 0);
  });
  consumer->Stop();
}

// After the leader of a tablet changes, the consumer finds the new leader
// without waiting for a rescan. Before, it kept polling the cached old leader,
// forever with the rescan disabled.
TEST_F(CDCClientITest, LeaderStepDown_ConsumerFollows) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/3));
  NO_FATALS(CreateTable(/*num_partitions=*/1, /*num_replicas=*/3));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const string tablet_id = TabletIds(client.get())[0];

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  NO_FATALS(InsertRows(0, 5));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(5, sink.CountOp(cdc::INSERT));
  });

  NO_FATALS(ChangeLeader(tablet_id));
  NO_FATALS(InsertRows(5, 5));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(10, sink.CountOp(cdc::INSERT));
  });
  EXPECT_EQ(CDCTabletState::kStreaming, ProgressOf(*consumer, tablet_id).state);
  consumer->Stop();
}

// Flush() while the leader changes: under TSAN, this checks that Flush() and
// the pollers do not race on the poller state (the leader, the checkpointed
// position).
TEST_F(CDCClientITest, Flush_ConcurrentWithLeaderChange) {
  FLAGS_cdc_tablet_rescan_interval_ms = 0;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/3));
  NO_FATALS(CreateTable(/*num_partitions=*/1, /*num_replicas=*/3));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const string tablet_id = TabletIds(client.get())[0];

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.checkpoint_interval = MonoDelta::FromMilliseconds(1);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));

  std::atomic<bool> done(false);
  std::thread flusher([&]() {
    while (!done) {
      // A flush during the leader change may fail; it must not crash or race.
      WARN_NOT_OK(consumer->Flush(), "flush failed");
      vector<CDCTabletProgress> progress;
      consumer->GetProgress(&progress);
      SleepFor(MonoDelta::FromMilliseconds(5));
    }
  });
  auto join_flusher = MakeScopedCleanup([&]() {
    done = true;
    flusher.join();
  });
  for (int i = 0; i < 3; i++) {
    NO_FATALS(InsertRows(i * 10, 10));
    NO_FATALS(ChangeLeader(tablet_id));
  }
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(30, sink.CountOp(cdc::INSERT));
  });
  join_flusher.run();
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(consumer->Flush());
  });
  consumer->Stop();
}

// A FULL image that cannot be reconstructed in time is a transient failure:
// the poller retries until it can. Before, the server reported it as
// UNKNOWN_ERROR and the tablet's poller stopped for good.
TEST_F(CDCClientITest, FullMode_TransientReconstructionFailureDoesNotKillPoller) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.record_type = master::CDCStreamConfigPB::FULL;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  NO_FATALS(InsertRows(0, 1));
  NO_FATALS(UpdateRow(0, 999));

  FLAGS_cdc_inject_full_reconstruction_timeout = true;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.min_poll_backoff = MonoDelta::FromMilliseconds(10);
  copts.max_poll_backoff = MonoDelta::FromMilliseconds(100);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(ProgressOf(*consumer, tablet_id).num_errors, 3);
  });
  FLAGS_cdc_inject_full_reconstruction_timeout = false;
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, sink.CountOp(cdc::UPDATE));
  });
  EXPECT_EQ(CDCTabletState::kStreaming, ProgressOf(*consumer, tablet_id).state);
  consumer->Stop();
}

// A tablet whose stream position is gone stops in kNeedsResnapshot and is
// reported to Options::on_tablet_stopped: by default the consumer does not
// take a new snapshot by itself, since the callback could not tell which rows
// were deleted meanwhile. Before, the consumer re-snapshotted, which a NEVER
// stream refuses, so the poller stopped with an unrelated error.
TEST_F(CDCClientITest, ExpiredPosition_StopsInNeedsResnapshot) {
  FLAGS_log_min_segments_to_retain = 1;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  const string tablet_id = TabletIds(client.get())[0];

  // Garbage-collect the tablet's first ops before the stream exists.
  scoped_refptr<TabletReplica> replica;
  ASSERT_OK(cluster_->mini_tablet_server(0)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &replica));
  NO_FATALS(InsertRows(0, 3));
  ASSERT_OK(replica->log()->WaitUntilAllFlushed());
  ASSERT_OK(replica->log()->AllocateSegmentAndRollOverForTests());
  NO_FATALS(InsertRows(3, 3));
  ASSERT_OK(replica->log()->WaitUntilAllFlushed());
  ASSERT_OK(replica->log()->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(replica->tablet()->Flush());
  replica->RunLogGC();
  ASSERT_GT(replica->log()->reader()->GetMinReplicateIndex(), 1);

  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  std::mutex lock;
  vector<CDCTabletProgress> stopped;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
    std::lock_guard<std::mutex> l(lock);
    stopped.push_back(p);
  };
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_EQ(CDCTabletState::kNeedsResnapshot, p.state)
        << cdc::CDCTabletStateToString(p.state) << ": " << p.terminal_error.ToString();
    ASSERT_TRUE(p.needs_resnapshot);
    ASSERT_TRUE(p.terminal_error.IsNotFound()) << p.terminal_error.ToString();
    // The poller publishes its state before it runs the callback.
    std::lock_guard<std::mutex> l(lock);
    ASSERT_EQ(1, stopped.size());
    ASSERT_EQ(CDCTabletState::kNeedsResnapshot, stopped[0].state);
  });
  EXPECT_EQ(0, sink.Total());
  consumer->Stop();
}

// A consumer that does not checkpoint takes no snapshot, which would move the
// stream's durable snapshot state: it cannot start with one, and a tablet whose
// position expired stops in kNeedsResnapshot instead of taking a new one.
TEST_F(CDCClientITest, ReadOnly_TakesNoSnapshot) {
  FLAGS_log_min_segments_to_retain = 1;
  FLAGS_cdc_max_resnapshot_attempts = 1;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  const string tablet_id = TabletIds(client.get())[0];

  // Garbage-collect the tablet's first ops before the stream exists.
  scoped_refptr<TabletReplica> replica;
  ASSERT_OK(cluster_->mini_tablet_server(0)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &replica));
  NO_FATALS(InsertRows(0, 3));
  ASSERT_OK(replica->log()->WaitUntilAllFlushed());
  ASSERT_OK(replica->log()->AllocateSegmentAndRollOverForTests());
  NO_FATALS(InsertRows(3, 3));
  ASSERT_OK(replica->log()->WaitUntilAllFlushed());
  ASSERT_OK(replica->log()->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(replica->tablet()->Flush());
  replica->RunLogGC();
  ASSERT_GT(replica->log()->reader()->GetMinReplicateIndex(), 1);

  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kSnapshot;
  copts.checkpoint_enabled = false;
  unique_ptr<CDCConsumer> consumer;
  Status s = CDCConsumer::Create(client.get(), copts, &consumer);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  copts.start_mode = CDCConsumer::kEarliest;
  ASSERT_OK(CDCConsumer::Create(client.get(), copts, &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_EQ(CDCTabletState::kNeedsResnapshot, p.state)
        << cdc::CDCTabletStateToString(p.state) << ": " << p.terminal_error.ToString();
  });
  s = consumer->RestartTablet(tablet_id, /*snapshot=*/true);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  EXPECT_EQ(0, sink.CountOp(cdc::READ));
  CDCStreamInfo info;
  ASSERT_OK(client->GetStreamInfo(stream_id, &info));
  EXPECT_TRUE(info.tablets_in_snapshot.empty());
  consumer->Stop();
}

// A consumer that does not checkpoint does not complete the snapshot of the
// stream's consumer when it reads past the snapshot's streaming start point:
// until that consumer streams, the last page of its snapshot is served again.
TEST_F(CDCClientITest, ReadOnly_DoesNotCompleteAnotherConsumersSnapshot) {
  FLAGS_cdc_max_records_per_response = 2;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  NO_FATALS(InsertRows(0, 4));
  cdc::CDCTableMetadata md;
  ASSERT_OK(client->GetTableMetadata(kTableName, /*by_id=*/false, &md));
  vector<cdc::CDCTabletInfo> tablets;
  ASSERT_OK(client->GetTabletLocations(md.table_id, &tablets));
  ASSERT_EQ(1, tablets.size());
  const cdc::CDCTabletInfo& tablet = tablets[0];

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kNow;
  copts.checkpoint_enabled = false;
  copts.min_poll_backoff = MonoDelta::FromMilliseconds(10);
  copts.max_poll_backoff = MonoDelta::FromMilliseconds(50);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), copts, &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));

  // The stream's consumer reads its snapshot to the last page.
  const auto read_page = [&](const string& resume_key, cdc::GetChangesResponsePB* resp) {
    cdc::GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet.tablet_id);
    req.set_is_snapshot_start(resume_key.empty());
    if (!resume_key.empty()) {
      req.set_snapshot_resume_key(resume_key);
    }
    ASSERT_OK(client->GetChanges(tablet.leader, req, resp));
    ASSERT_FALSE(resp->has_error()) << resp->error().DebugString();
  };
  cdc::GetChangesResponsePB p1;
  NO_FATALS(read_page("", &p1));
  ASSERT_FALSE(p1.snapshot_done());
  cdc::GetChangesResponsePB p2;
  NO_FATALS(read_page(p1.snapshot_resume_key(), &p2));
  ASSERT_TRUE(p2.snapshot_done());

  // The other consumer reads past the streaming start point. The read that
  // gets the second row starts after the first row was delivered, so after
  // the last page was served.
  for (int i = 1; i <= 2; ++i) {
    NO_FATALS(InsertRows(100 + i, 1));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(i, sink.CountOp(cdc::INSERT));
    });
  }
  // The last page is served again, by the snapshot session. Had the session
  // ended, the page could be served only by a session rebuilt from the
  // master's copy of the snapshot, until the master learns that it ended.
  auto* cdc = cluster_->mini_tablet_server(0)->server()->cdc_service();
  const int64_t resumed = cdc->SnapshotSessionsResumedForTests();
  cdc::GetChangesResponsePB again;
  NO_FATALS(read_page(p1.snapshot_resume_key(), &again));
  EXPECT_EQ(resumed, cdc->SnapshotSessionsResumedForTests());
  EXPECT_TRUE(again.snapshot_done());
  EXPECT_EQ(p2.snapshot_streaming_start_op_index(),
            again.snapshot_streaming_start_op_index());
  consumer->Stop();
}

// A callback error stops the tablet's poller in kFailed with the error, and
// the poller is reported to Options::on_tablet_stopped; RestartTablet()
// resumes it without losing the batch the callback failed.
TEST_F(CDCClientITest, CallbackError_SurfacesTerminalState) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  NO_FATALS(InsertRows(0, 10));

  std::atomic<int> num_stopped(0);
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
    CHECK_EQ(tablet_id, p.tablet_id);
    num_stopped++;
  };
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  std::atomic<bool> sink_down(true);
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    return sink_down ? Status::IOError("the sink is down") : sink.Deliver(b);
  }));
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_EQ(CDCTabletState::kFailed, p.state);
    ASSERT_TRUE(p.terminal_error.IsIOError()) << p.terminal_error.ToString();
    // The poller publishes its state before it runs the callback.
    ASSERT_EQ(1, num_stopped);
  });
  ASSERT_EQ(0, sink.Total());

  sink_down = false;
  ASSERT_OK(consumer->RestartTablet(tablet_id, /*snapshot=*/false));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(10, sink.CountOp(cdc::INSERT));
  });
  EXPECT_EQ(CDCTabletState::kStreaming, ProgressOf(*consumer, tablet_id).state);
  Status s = consumer->RestartTablet(tablet_id, /*snapshot=*/false);
  EXPECT_TRUE(s.IsIllegalState()) << s.ToString();
  s = consumer->RestartTablet("no-such-tablet", /*snapshot=*/false);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
  consumer->Stop();
}

// Flush() reports a tablet it could not checkpoint. Before, it returned OK
// whether or not anything was checkpointed.
TEST_F(CDCClientITest, Flush_ReturnsErrorWhenCheckpointFails) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  NO_FATALS(InsertRows(0, 5));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.checkpoint_interval = MonoDelta::FromSeconds(3600);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(5, sink.CountOp(cdc::INSERT));
  });
  cluster_->mini_tablet_server(0)->Shutdown();
  Status s = consumer->Flush();
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "1 of 1 tablet(s) could not be checkpointed");
  consumer->Stop();
}

// A snapshot whose leader is down is retried with backoff. Before, the
// snapshot phase retried a failed call immediately, in a busy loop.
TEST_F(CDCClientITest, SnapshotLeaderDown_NoBusyLoop) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  NO_FATALS(InsertRows(0, 10));
  const string tablet_id = TabletIds(client.get())[0];

  cluster_->mini_tablet_server(0)->Shutdown();
  // The master still reports the stopped server as the leader.
  {
    cdc::CDCTableMetadata md;
    ASSERT_OK(client->GetTableMetadata(kTableName, /*by_id=*/false, &md));
    vector<cdc::CDCTabletInfo> tablets;
    ASSERT_OK(client->GetTabletLocations(md.table_id, &tablets));
    ASSERT_EQ(1, tablets.size());
    ASSERT_TRUE(tablets[0].leader.Initialized());
  }
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kSnapshot;
  copts.min_poll_backoff = MonoDelta::FromMilliseconds(100);
  copts.max_poll_backoff = MonoDelta::FromMilliseconds(500);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  ASSERT_OK(consumer->Start([](const CDCRecordBatch& /*b*/) { return Status::OK(); }));
  SleepFor(MonoDelta::FromSeconds(3));
  const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
  EXPECT_EQ(CDCTabletState::kSnapshotting, p.state);
  // About one failed call per backoff: 100+200+400+500+... ms.
  EXPECT_GE(p.num_errors, 1);
  EXPECT_LE(p.num_errors, 20);
  consumer->Stop();
}

// Stop() called from the record callback stops the consumer without joining
// the calling thread. Before, the poller thread joined itself, which aborts
// the process.
TEST_F(CDCClientITest, StopFromCallback_DoesNotAbort) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  const string tablet_id = TabletIds(client.get())[0];
  NO_FATALS(InsertRows(0, 5));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  std::atomic<int> batches(0);
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& /*b*/) {
    batches++;
    consumer->Stop();
    return Status::OK();
  }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(CDCTabletState::kStopped, ProgressOf(*consumer, tablet_id).state);
  });
  ASSERT_EQ(1, batches);
  // Pollers are not added after a Stop() from a callback.
  Status s = consumer->ReScan();
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  consumer->Stop();
  vector<CDCTabletProgress> progress;
  consumer->GetProgress(&progress);
  ASSERT_TRUE(progress.empty());
  // The consumer can be started again.
  ASSERT_OK(consumer->Start([](const CDCRecordBatch& /*b*/) { return Status::OK(); }));
  consumer->Stop();
}

// The batch of a transaction carries the WAL position after its COMMIT: the
// transaction's rows keep the op index of the write that made them, below that
// of their BEGIN record, so only the batch's checkpoint_op_index tells a sink
// what it applied.
TEST_F(CDCClientITest, Transaction_BatchCarriesCheckpointIndex) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  FLAGS_txn_manager_status_table_num_replicas = 1;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<client::KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    client::sp::shared_ptr<client::KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = 0; i < 3; i++) {
      client::KuduInsert* insert = table->NewInsert();
      ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i));
      ASSERT_OK(session->Apply(insert));
    }
    ASSERT_OK(txn->Commit());
  }

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  std::mutex lock;
  vector<CDCRecordBatch> batches;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    std::lock_guard<std::mutex> l(lock);
    batches.push_back(b);
    return Status::OK();
  }));
  CDCRecordBatch txn_batch;
  ASSERT_EVENTUALLY([&] {
    std::lock_guard<std::mutex> l(lock);
    bool found = false;
    for (const auto& b : batches) {
      for (const auto& r : b.records) {
        if (r.op_type == cdc::COMMIT) {
          txn_batch = b;
          found = true;
        }
      }
    }
    ASSERT_TRUE(found);
  });
  consumer->Stop();

  const CDCDecodedRecord* begin = FindOp(txn_batch.records, cdc::BEGIN);
  const CDCDecodedRecord* commit = FindOp(txn_batch.records, cdc::COMMIT);
  ASSERT_NE(nullptr, begin);
  ASSERT_NE(nullptr, commit);
  ASSERT_GE(txn_batch.checkpoint_op_index, commit->op_index);
  int rows = 0;
  for (const auto& r : txn_batch.records) {
    EXPECT_LE(r.op_index, txn_batch.checkpoint_op_index);
    if (r.op_type == cdc::INSERT) {
      rows++;
      EXPECT_LT(r.op_index, begin->op_index);
    }
  }
  EXPECT_EQ(3, rows);
}

// A transaction whose change records do not fit in one RPC message stops the
// consumer's poller in kFailed with TRANSACTION_TOO_LARGE. Before, the tablet
// server sent the response anyway, the consumer's messenger rejected it, and
// the poller retried the same call forever.
TEST_F(CDCClientITest, TransactionLargerThanRpcMaxMessageSize) {
  // The smallest message size the RPC layer accepts, for the cluster and the
  // consumer alike.
  FLAGS_rpc_max_message_size = 1024 * 1024;
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  FLAGS_txn_manager_status_table_num_replicas = 1;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  // A single-tablet table with wide rows.
  constexpr const char* const kWideTable = "cdc_client_itest_wide_table";
  {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(client::KuduColumnSchema::STRING)->NotNull();
    ASSERT_OK(b.Build(&schema));
    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(kWideTable)
                  .schema(&schema)
                  .set_range_partition_columns({ "key" })
                  .num_replicas(1)
                  .Create());
  }
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kWideTable, CDCStreamOptions(), &stream_id));
  {
    // About 1.2 MiB of records, written in batches that fit in a message.
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kWideTable, &table));
    client::sp::shared_ptr<client::KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    client::sp::shared_ptr<client::KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    ASSERT_OK(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));
    const string value(1024, 'x');
    for (int i = 0; i < 1200; i++) {
      client::KuduInsert* insert = table->NewInsert();
      ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
      ASSERT_OK(insert->mutable_row()->SetString("val", value));
      ASSERT_OK(session->Apply(insert));
      if (i % 200 == 199) {
        ASSERT_OK(session->Flush());
      }
    }
    ASSERT_OK(txn->Commit());
  }
  cdc::CDCTableMetadata md;
  ASSERT_OK(client->GetTableMetadata(kWideTable, /*by_id=*/false, &md));
  vector<cdc::CDCTabletInfo> tablets;
  ASSERT_OK(client->GetTabletLocations(md.table_id, &tablets));
  ASSERT_EQ(1, tablets.size());
  const string tablet_id = tablets[0].tablet_id;

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_EQ(CDCTabletState::kFailed, p.state) << p.last_error.ToString();
    ASSERT_STR_CONTAINS(p.terminal_error.ToString(), "--rpc_max_message_size");
  });
  consumer->Stop();
  EXPECT_EQ(0, sink.CountOp(cdc::INSERT));
}

// The tablet servers count two consumers that read one stream, and do not
// count a consumer that replaces another one.
TEST_F(CDCClientITest, TwoConsumersSameStream_DuplicateDetected) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client1 = MakeClient();
  unique_ptr<CDCClient> client2 = MakeClient();
  string stream_id;
  ASSERT_OK(client1->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.min_poll_backoff = MonoDelta::FromMilliseconds(10);
  copts.max_poll_backoff = MonoDelta::FromMilliseconds(50);
  copts.checkpoint_enabled = false;
  const auto noop = [](const CDCRecordBatch& /*b*/) { return Status::OK(); };

  // One consumer, then another one.
  for (CDCClient* client : { client1.get(), client2.get() }) {
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client, copts, &consumer));
    ASSERT_OK(consumer->Start(noop));
    SleepFor(MonoDelta::FromMilliseconds(500));
    consumer->Stop();
  }
  ASSERT_EQ(0, DuplicateConsumersDetected(0));

  // Two at once.
  unique_ptr<CDCConsumer> consumer1;
  ASSERT_OK(CDCConsumer::Create(client1.get(), copts, &consumer1));
  unique_ptr<CDCConsumer> consumer2;
  ASSERT_OK(CDCConsumer::Create(client2.get(), copts, &consumer2));
  ASSERT_OK(consumer1->Start(noop));
  ASSERT_OK(consumer2->Start(noop));
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(DuplicateConsumersDetected(0), 0);
  });
  consumer1->Stop();
  consumer2->Stop();
}

namespace {
// Returns the record of 'op' whose 'key' column decodes to 'key', or nullptr.
const CDCDecodedRecord* FindRowByKey(const vector<CDCDecodedRecord>& records,
                                     cdc::CDCOpTypePB op, const string& key) {
  for (const auto& r : records) {
    if (r.op_type != op) continue;
    for (const auto& c : r.after) {
      if (c.name == "key" && c.value == key) return &r;
    }
  }
  return nullptr;
}

// Checks that every value of 'records' was decoded.
void ExpectAllDecoded(const vector<CDCDecodedRecord>& records) {
  for (const auto& r : records) {
    for (const auto* cols : { &r.after, &r.before }) {
      for (const auto& c : *cols) {
        EXPECT_TRUE(c.decode_ok) << "op " << r.op_index << " column " << c.name;
      }
    }
  }
}
}  // anonymous namespace

// A tablet that lags behind with the DDL records of a table's ALTERs does not
// change how the consumer decodes the records of the other tablets. Before, the
// consumer kept one schema for all its tablets, and the lagging tablet's older
// DDL record replaced the newer schema that the other tablet's records needed:
// a column added by the newer ALTER was not decoded.
TEST_F(CDCClientITest, MultiTablet_LaggingAlter_SchemaDoesNotRegress) {
  // One record per response, so that each DDL record is its own batch.
  FLAGS_cdc_max_records_per_response = 1;
  NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
  NO_FATALS(CreateTable(/*num_partitions=*/2));
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  // A row of the second tablet, at which its poller is held.
  NO_FATALS(InsertRows(1000, 1));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  copts.min_poll_backoff = MonoDelta::FromMilliseconds(10);
  copts.max_poll_backoff = MonoDelta::FromMilliseconds(50);
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  // The second tablet's poller waits in the callback at its first row until
  // 'release_row' is counted down, and then at the DDL record of the first
  // ALTER until 'release_ddl' is.
  CountDownLatch at_ddl(1);
  CountDownLatch release_row(1);
  CountDownLatch release_ddl(1);
  std::mutex lock;
  string lagging_tablet;
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    for (const auto& r : b.records) {
      if (r.op_type == cdc::INSERT && ColValue(r, /*before=*/false, "key") == "1000") {
        {
          std::lock_guard<std::mutex> l(lock);
          lagging_tablet = b.tablet_id;
        }
        release_row.WaitFor(kTimeout);
      }
      bool lagging;
      {
        std::lock_guard<std::mutex> l(lock);
        lagging = b.tablet_id == lagging_tablet;
      }
      if (lagging && r.op_type == cdc::DDL && r.new_schema_version == 1) {
        at_ddl.CountDown();
        release_ddl.WaitFor(kTimeout);
      }
    }
    return sink.Deliver(b);
  }));
  SCOPED_CLEANUP({
    release_row.CountDown();
    release_ddl.CountDown();
    consumer->Stop();
  });
  ASSERT_EVENTUALLY([&] {
    std::lock_guard<std::mutex> l(lock);
    ASSERT_FALSE(lagging_tablet.empty());
  });

  // Two ALTERs: version 1 adds 'c', version 2 adds 'd'.
  for (const char* col : { "c", "d" }) {
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    alterer->AddColumn(col)->Type(client::KuduColumnSchema::INT32);
    ASSERT_OK(alterer->Alter());
  }
  const auto insert_cd = [&](int key, int c, int d) {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    client::KuduInsert* insert = table->NewInsert();
    ASSERT_OK(insert->mutable_row()->SetInt32("key", key));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", key));
    ASSERT_OK(insert->mutable_row()->SetInt32("c", c));
    ASSERT_OK(insert->mutable_row()->SetInt32("d", d));
    ASSERT_OK(session->Apply(insert));
  };
  // The first tablet gets both DDL records and a row with 'd'.
  NO_FATALS(insert_cd(1, 5, 7));
  ASSERT_EVENTUALLY([&] {
    ASSERT_NE(nullptr, FindRowByKey(sink.Snapshot(), cdc::INSERT, "1"));
  });

  // The second tablet gets the DDL record of version 1 only now; then the
  // first tablet gets another row with 'd'.
  release_row.CountDown();
  ASSERT_TRUE(at_ddl.WaitFor(kTimeout));
  NO_FATALS(insert_cd(2, 6, 9));
  ASSERT_EVENTUALLY([&] {
    ASSERT_NE(nullptr, FindRowByKey(sink.Snapshot(), cdc::INSERT, "2"));
  });
  release_ddl.CountDown();

  const auto records = sink.Snapshot();
  const CDCDecodedRecord* r1 = FindRowByKey(records, cdc::INSERT, "1");
  const CDCDecodedRecord* r2 = FindRowByKey(records, cdc::INSERT, "2");
  ASSERT_NE(nullptr, r1);
  ASSERT_NE(nullptr, r2);
  EXPECT_EQ(2, r1->schema_version);
  EXPECT_EQ(2, r2->schema_version);
  EXPECT_EQ("5", ColValue(*r1, /*before=*/false, "c"));
  EXPECT_EQ("7", ColValue(*r1, /*before=*/false, "d"));
  EXPECT_EQ("6", ColValue(*r2, /*before=*/false, "c"));
  EXPECT_EQ("9", ColValue(*r2, /*before=*/false, "d"));
  // The lagging tablet's records decode too, once it is done.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(3, sink.CountOp(cdc::INSERT));
  });
  NO_FATALS(ExpectAllDecoded(sink.Snapshot()));
}

// A column dropped and added again with the same name and another type is a
// different column: a consumer that resumes before the DROP decodes the
// records written before it with the type of the dropped column. Before, the
// consumer decoded by name with the schema it got at its start, and showed the
// bits of the INT64 value as a DOUBLE.
TEST_F(CDCClientITest, Resume_DropThenReAddSameNameDifferentType_NoTypeConfusion) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));

  client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
  const auto alter = [&](const std::function<void(client::KuduTableAlterer*)>& f) {
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    f(alterer.get());
    ASSERT_OK(alterer->Alter());
  };
  const auto insert = [&](int key, const std::function<Status(KuduPartialRow*)>& set_x) {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::KuduInsert* insert = table->NewInsert();
    ASSERT_OK(insert->mutable_row()->SetInt32("key", key));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", key));
    ASSERT_OK(set_x(insert->mutable_row()));
    ASSERT_OK(session->Apply(insert));
  };

  NO_FATALS(alter([](client::KuduTableAlterer* a) {
    a->AddColumn("x")->Type(client::KuduColumnSchema::INT64);
  }));
  NO_FATALS(insert(100, [](KuduPartialRow* row) { return row->SetInt64("x", 1); }));
  // The first consumer checkpoints the stream after that row.
  {
    CDCConsumer::Options copts;
    copts.stream_id = stream_id;
    copts.start_mode = CDCConsumer::kEarliest;
    unique_ptr<CDCConsumer> consumer;
    ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
    RecordSink sink;
    ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(1, sink.CountOp(cdc::INSERT));
    });
    ASSERT_OK(consumer->Flush());
    consumer->Stop();
  }
  const string tablet_id = TabletIds(client.get())[0];
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    ASSERT_GT(FindWithDefault(info.tablet_checkpoints, tablet_id, -1), 0);
  });

  NO_FATALS(insert(0, [](KuduPartialRow* row) { return row->SetInt64("x", 42); }));
  NO_FATALS(alter([](client::KuduTableAlterer* a) { a->DropColumn("x"); }));
  NO_FATALS(alter([](client::KuduTableAlterer* a) {
    a->AddColumn("x")->Type(client::KuduColumnSchema::DOUBLE);
  }));
  NO_FATALS(insert(1, [](KuduPartialRow* row) { return row->SetDouble("x", 1.5); }));

  // The second consumer starts with the table's current schema, where 'x' is a
  // DOUBLE, and resumes before the INT64 row.
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kNow;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(2, sink.CountOp(cdc::INSERT));
  });
  consumer->Stop();

  const auto records = sink.Snapshot();
  const CDCDecodedRecord* r0 = FindRowByKey(records, cdc::INSERT, "0");
  const CDCDecodedRecord* r1 = FindRowByKey(records, cdc::INSERT, "1");
  ASSERT_NE(nullptr, r0);
  ASSERT_NE(nullptr, r1);
  EXPECT_EQ(1, r0->schema_version);
  EXPECT_EQ("42", ColValue(*r0, /*before=*/false, "x"));
  EXPECT_EQ(3, r1->schema_version);
  EXPECT_EQ("1.5", ColValue(*r1, /*before=*/false, "x"));
  NO_FATALS(ExpectAllDecoded(records));
  // The DDL records of the ALTERs are delivered, but not the one the consumer
  // asked for to learn the schema it resumes with.
  vector<uint32_t> ddl_versions;
  for (const auto& r : records) {
    if (r.op_type == cdc::DDL) {
      ddl_versions.push_back(r.new_schema_version);
    }
  }
  EXPECT_EQ(vector<uint32_t>({ 2, 3 }), ddl_versions);
  for (const auto& c : r0->after) {
    if (c.name == "x") {
      ASSERT_NE(nullptr, c.type_info);
      EXPECT_EQ(INT64, c.type_info->type());
    }
  }
}

// The images of a FULL record carry the columns of the tablet's schema when
// they are read, which can have a column that no schema the consumer got so
// far has: the consumer gets it from the master.
TEST_F(CDCClientITest, FullMode_ImageWithColumnAddedLaterIsDecoded) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.record_type = master::CDCStreamConfigPB::FULL;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  NO_FATALS(InsertRows(0, 1));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  // The poller waits at the INSERT until the UPDATE and the ALTER are done.
  CountDownLatch at_insert(1);
  CountDownLatch release(1);
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    for (const auto& r : b.records) {
      if (r.op_type == cdc::INSERT) {
        at_insert.CountDown();
        release.WaitFor(kTimeout);
      }
    }
    return sink.Deliver(b);
  }));
  SCOPED_CLEANUP({
    release.CountDown();
    consumer->Stop();
  });
  ASSERT_TRUE(at_insert.WaitFor(kTimeout));
  NO_FATALS(UpdateRow(0, 5));
  {
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    alterer->AddColumn("c")->Type(client::KuduColumnSchema::INT32)->NotNull()
        ->Default(client::KuduValue::FromInt(7));
    ASSERT_OK(alterer->Alter());
  }
  release.CountDown();
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, sink.CountOp(cdc::UPDATE));
  });

  const auto records = sink.Snapshot();
  const CDCDecodedRecord* upd = FindOp(records, cdc::UPDATE);
  ASSERT_NE(nullptr, upd);
  EXPECT_EQ(0, upd->schema_version);
  EXPECT_EQ("0", ColValue(*upd, /*before=*/true, "val"));
  EXPECT_EQ("5", ColValue(*upd, /*before=*/false, "val"));
  EXPECT_EQ("7", ColValue(*upd, /*before=*/true, "c"));
  EXPECT_EQ("7", ColValue(*upd, /*before=*/false, "c"));
  NO_FATALS(ExpectAllDecoded(records));
}

// As above, with the master down when the poller needs the new column: the
// poller fetches the batch again until the master answers, rather than
// delivering the record without the column's value. One record per response,
// so that the UPDATE comes before the ALTER's DDL record.
TEST_F(CDCClientITest, FullMode_ImageWithColumnAddedLater_MasterDown_Retried) {
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  CDCStreamOptions opts;
  opts.record_type = master::CDCStreamConfigPB::FULL;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  NO_FATALS(InsertRows(0, 1));
  const string tablet_id = TabletIds(client.get())[0];
  FLAGS_cdc_max_records_per_response = 1;

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  CountDownLatch at_insert(1);
  CountDownLatch release(1);
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) {
    for (const auto& r : b.records) {
      if (r.op_type == cdc::INSERT) {
        at_insert.CountDown();
        release.WaitFor(kTimeout);
      }
    }
    return sink.Deliver(b);
  }));
  SCOPED_CLEANUP({
    release.CountDown();
    consumer->Stop();
  });
  ASSERT_TRUE(at_insert.WaitFor(kTimeout));
  NO_FATALS(UpdateRow(0, 5));
  {
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    alterer->AddColumn("c")->Type(client::KuduColumnSchema::INT32)->NotNull()
        ->Default(client::KuduValue::FromInt(7));
    ASSERT_OK(alterer->Alter());
  }
  cluster_->mini_master()->Shutdown();
  const int64_t errors_before = ProgressOf(*consumer, tablet_id).num_errors;
  release.CountDown();
  // The poller retries the UPDATE's batch.
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = ProgressOf(*consumer, tablet_id);
    ASSERT_GE(p.num_errors, errors_before + 2);
    ASSERT_STR_CONTAINS(p.last_error.ToString(), "from the master");
  });
  EXPECT_EQ(0, sink.CountOp(cdc::UPDATE));
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, sink.CountOp(cdc::UPDATE));
  });

  const auto records = sink.Snapshot();
  const CDCDecodedRecord* upd = FindOp(records, cdc::UPDATE);
  ASSERT_NE(nullptr, upd);
  EXPECT_EQ("7", ColValue(*upd, /*before=*/true, "c"));
  EXPECT_EQ("7", ColValue(*upd, /*before=*/false, "c"));
  NO_FATALS(ExpectAllDecoded(records));
  EXPECT_EQ(CDCTabletState::kStreaming, ProgressOf(*consumer, tablet_id).state);
}

// While an ALTER is in progress, the master reports the table's schema from
// before it, but an image of a FULL record of a tablet that applied it already
// carries the new column. The consumer asks the master for that column at most
// once a second rather than once per batch. Before, every batch of such
// records made a GetTableSchema call.
TEST_F(CDCClientITest, FullMode_AlterInProgress_SchemaRefreshRateLimited) {
  NO_FATALS(StartCluster(/*num_tablet_servers=*/2));
  NO_FATALS(CreateTable(/*num_partitions=*/2));
  unique_ptr<CDCClient> client = MakeClient();
  const vector<string> tablet_ids = TabletIds(client.get());
  ASSERT_EQ(2, tablet_ids.size());
  const int ts_a = FindLeaderIndex(tablet_ids[0]);
  const int ts_b = FindLeaderIndex(tablet_ids[1]);
  ASSERT_GE(ts_a, 0);
  ASSERT_GE(ts_b, 0);
  ASSERT_NE(ts_a, ts_b) << "both tablets were placed on one tablet server";
  string stream_id;
  CDCStreamOptions opts;
  opts.record_type = master::CDCStreamConfigPB::FULL;
  ASSERT_OK(client->CreateStream(kTableName, opts, &stream_id));
  // Records of the first tablet (keys below 1000) before the ALTER.
  constexpr int kNumUpdates = 4;
  NO_FATALS(InsertRows(0, 1));
  for (int i = 1; i <= kNumUpdates; i++) {
    NO_FATALS(UpdateRow(0, i));
  }
  // The ALTER cannot complete while the second tablet is down.
  cluster_->mini_tablet_server(ts_b)->Shutdown();
  {
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    alterer->AddColumn("c")->Type(client::KuduColumnSchema::INT32)->NotNull()
        ->Default(client::KuduValue::FromInt(7));
    alterer->wait(false);
    ASSERT_OK(alterer->Alter());
  }
  ASSERT_EVENTUALLY([&] {
    scoped_refptr<TabletReplica> replica;
    ASSERT_OK(cluster_->mini_tablet_server(ts_a)->server()->tablet_manager()
        ->GetTabletReplica(tablet_ids[0], &replica));
    ASSERT_EQ(1, replica->tablet_metadata()->schema_version());
  });
  {
    cdc::CDCTableMetadata md;
    ASSERT_OK(client->GetTableMetadata(kTableName, /*by_id=*/false, &md));
    ASSERT_EQ(-1, md.schema.find_column("c")) << md.schema.ToString();
  }

  FLAGS_cdc_max_records_per_response = 1;
  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  const auto num_get_table_schema = [&] {
    return METRIC_handler_latency_kudu_master_MasterService_GetTableSchema.Instantiate(
        cluster_->mini_master()->master()->metric_entity())->TotalCount();
  };
  const uint64_t num_before = num_get_table_schema();
  RecordSink sink;
  const MonoTime start = MonoTime::Now();
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));
  SCOPED_CLEANUP({ consumer->Stop(); });
  // The INSERT and the UPDATEs, one per batch, then the ALTER's DDL record.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, sink.CountOp(cdc::DDL));
  });
  const MonoDelta elapsed = MonoTime::Now() - start;
  const uint64_t num_refreshes = num_get_table_schema() - num_before;
  EXPECT_EQ(1, sink.CountOp(cdc::INSERT));
  EXPECT_EQ(kNumUpdates, sink.CountOp(cdc::UPDATE));
  EXPECT_GE(num_refreshes, 1);
  EXPECT_LE(num_refreshes, 1 + static_cast<uint64_t>(elapsed.ToSeconds()))
      << "elapsed: " << elapsed.ToString();
}

// The rows of a transaction decode with the schema they were written with,
// even if an ALTER that renamed one of their columns committed while the
// transaction was open: its DDL record comes before the transaction's rows.
// Before, the consumer decoded those rows with the schema of that DDL record,
// which did not have the column's old name.
TEST_F(CDCClientITest, Txn_RenameInsideOpenTxn_RowsDecodedWithWriteTimeSchema) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  FLAGS_txn_manager_status_table_num_replicas = 1;
  NO_FATALS(StartClusterAndTable());
  unique_ptr<CDCClient> client = MakeClient();
  string stream_id;
  ASSERT_OK(client->CreateStream(kTableName, CDCStreamOptions(), &stream_id));

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = CDCConsumer::kEarliest;
  unique_ptr<CDCConsumer> consumer;
  ASSERT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  RecordSink sink;
  ASSERT_OK(consumer->Start([&](const CDCRecordBatch& b) { return sink.Deliver(b); }));

  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  client::sp::shared_ptr<client::KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));
  client::sp::shared_ptr<client::KuduSession> session;
  ASSERT_OK(txn->CreateSession(&session));
  ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
  for (int i = 0; i < 3; i++) {
    client::KuduInsert* insert = table->NewInsert();
    ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", i * 10));
    ASSERT_OK(session->Apply(insert));
  }
  {
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    alterer->AlterColumn("val")->RenameTo("val2");
    ASSERT_OK(alterer->Alter());
  }
  ASSERT_OK(txn->Commit());

  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, sink.CountOp(cdc::COMMIT));
  });
  consumer->Stop();

  const auto records = sink.Snapshot();
  int ddl_pos = -1;
  int rows = 0;
  for (int i = 0; i < static_cast<int>(records.size()); i++) {
    const auto& r = records[i];
    if (r.op_type == cdc::DDL) {
      ddl_pos = i;
      EXPECT_EQ(1, r.new_schema_version);
    }
    if (r.op_type != cdc::INSERT) continue;
    rows++;
    // The DDL record comes first: the ALTER committed before the transaction.
    EXPECT_GE(ddl_pos, 0) << "row " << i << " delivered before the DDL record";
    EXPECT_EQ(0, r.schema_version);
    const string key = ColValue(r, /*before=*/false, "key");
    EXPECT_EQ(std::to_string(std::stoi(key) * 10), ColValue(r, /*before=*/false, "val"));
  }
  EXPECT_EQ(3, rows);
  EXPECT_GE(ddl_pos, 0);
  NO_FATALS(ExpectAllDecoded(records));
}

}  // namespace kudu
