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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.proxy.h"
#include "kudu/cdc/cdc_service.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_bool(cdc_enabled);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_inject_latency_after_checkpoint_respond_ms);
DECLARE_int32(cdc_stream_config_cache_ttl_ms);
DECLARE_int32(cdc_stream_config_negative_cache_ttl_ms);
DECLARE_int64(cdc_stream_expiry_ms);
DECLARE_int64(cdc_stream_idle_expiry_ms);

METRIC_DECLARE_entity(cdc_stream);
METRIC_DECLARE_gauge_int64(cdc_stream_time_to_expiry_micros);

using kudu::cdc::CDCServiceProxy;
using kudu::cdc::CheckpointRequestPB;
using kudu::cdc::CheckpointResponsePB;
using kudu::cdc::CDCErrorPB;
using kudu::cdc::CDCOpTypePB;
using kudu::cdc::GetChangesRequestPB;
using kudu::cdc::GetChangesResponsePB;
using kudu::master::CreateCDCStreamRequestPB;
using kudu::master::CreateCDCStreamResponsePB;
using kudu::master::DeleteCDCStreamRequestPB;
using kudu::master::DeleteCDCStreamResponsePB;
using kudu::master::GetCDCStreamInfoRequestPB;
using kudu::master::GetCDCStreamInfoResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::SysCDCStreamEntryPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tablet::TabletReplica;
using std::function;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

class CDCITest : public MiniClusterITestBase {
 protected:
  static const MonoDelta kTimeout;

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_cdc_enabled = true;
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
  }

  void StartClusterAndCreateTable(int num_tablets = 1, int num_tservers = 1) {
    NO_FATALS(StartCluster(num_tservers));

    // Create a simple table.
    client::KuduSchema schema;
    client::KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    schema_builder.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(schema_builder.Build(&schema));

    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name("cdc_test_table")
        .schema(&schema)
        .set_range_partition_columns({"key"})
        .num_replicas(1);

    for (int i = 1; i < num_tablets; ++i) {
      KuduPartialRow* split = schema.NewRow();
      ASSERT_OK(split->SetInt32("key", i * 1000));
      creator->add_range_partition_split(split);
    }
    ASSERT_OK(creator->Create());
    table_name_ = "cdc_test_table";

    // CreateCDCStream takes table ids, not table names.
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name_, &table));
    table_id_ = table->id();

    // Build CDC proxy to the tserver.
    MessengerBuilder bld("CDCITestClient");
    ASSERT_OK(bld.Build(&messenger_));
    cdc_proxy_.reset(new CDCServiceProxy(
        messenger_,
        cluster_->mini_tablet_server(0)->bound_rpc_addr(),
        cluster_->mini_tablet_server(0)->bound_rpc_addr().host()));

    // Build master proxy.
    master_proxy_.reset(new MasterServiceProxy(
        messenger_,
        cluster_->mini_master()->bound_rpc_addr(),
        cluster_->mini_master()->bound_rpc_addr().host()));
  }

  Status CreateCDCStream(const string& table_id, string* stream_id) {
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.add_table_ids(table_id);

    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(master_proxy_->CreateCDCStream(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    *stream_id = resp.stream_id();
    return Status::OK();
  }

  Status DeleteCDCStream(const string& stream_id) {
    DeleteCDCStreamRequestPB req;
    DeleteCDCStreamResponsePB resp;
    req.set_stream_id(stream_id);

    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(master_proxy_->DeleteCDCStream(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  Status DoGetChanges(const string& stream_id, const string& tablet_id,
                      int64_t from_op_index, GetChangesResponsePB* resp) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from_op_index);

    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(cdc_proxy_->GetChanges(req, resp, &rpc));
    return Status::OK();
  }

  Status DoCheckpoint(const string& stream_id, const string& tablet_id,
                      int64_t op_index, CheckpointResponsePB* resp) {
    CheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);

    RpcController rpc;
    rpc.set_timeout(kTimeout);
    return cdc_proxy_->Checkpoint(req, resp, &rpc);
  }

  Status DoCheckpoint(const string& stream_id, const string& tablet_id,
                      int64_t op_index) {
    CheckpointResponsePB resp;
    RETURN_NOT_OK(DoCheckpoint(stream_id, tablet_id, op_index, &resp));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  void InsertRows(int start_key, int count) {
    client::sp::shared_ptr<client::KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    CHECK_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = start_key; i < start_key + count; ++i) {
      client::KuduInsert* insert = table->NewInsert();
      CHECK_OK(insert->mutable_row()->SetInt32("key", i));
      CHECK_OK(insert->mutable_row()->SetInt32("val", i * 10));
      CHECK_OK(session->Apply(insert));
    }
  }

  string GetTabletId(int ts_idx = 0) {
    vector<scoped_refptr<TabletReplica>> replicas;
    cluster_->mini_tablet_server(ts_idx)->server()->tablet_manager()->GetTabletReplicas(&replicas);
    CHECK(!replicas.empty());
    return replicas[0]->tablet_id();
  }

  vector<string> GetAllTabletIds(int ts_idx = 0) {
    vector<scoped_refptr<TabletReplica>> replicas;
    cluster_->mini_tablet_server(ts_idx)->server()->tablet_manager()->GetTabletReplicas(&replicas);
    vector<string> ids;
    ids.reserve(replicas.size());
    for (const auto& r : replicas) {
      ids.push_back(r->tablet_id());
    }
    return ids;
  }

  int CountRecordsByType(const GetChangesResponsePB& resp, CDCOpTypePB type) {
    int count = 0;
    for (int i = 0; i < resp.records_size(); ++i) {
      if (resp.records(i).op_type() == type) count++;
    }
    return count;
  }

  string table_name_;
  string table_id_;
  shared_ptr<Messenger> messenger_;
  unique_ptr<CDCServiceProxy> cdc_proxy_;
  unique_ptr<MasterServiceProxy> master_proxy_;
};

const MonoDelta CDCITest::kTimeout = MonoDelta::FromSeconds(30);

// Full pipeline: create stream, insert, GetChanges, verify records.
TEST_F(CDCITest, CreateStreamAndGetChanges) {
  NO_FATALS(StartClusterAndCreateTable());
  string tablet_id = GetTabletId();

  // Create a CDC stream on the master.
  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));
  ASSERT_FALSE(stream_id.empty());

  // Insert some rows.
  NO_FATALS(InsertRows(0, 5));

  // GetChanges from the tserver's CDC service.
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  int inserts = CountRecordsByType(resp, CDCOpTypePB::INSERT);
  EXPECT_EQ(5, inserts);
  EXPECT_GT(resp.checkpoint_op_index(), 0);
}

// After a checkpoint we can still read from the checkpoint.
TEST_F(CDCITest, CheckpointPreservesWAL) {
  NO_FATALS(StartClusterAndCreateTable());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));

  NO_FATALS(InsertRows(0, 10));

  // Read all changes.
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
  ASSERT_FALSE(resp.has_error());
  int64_t checkpoint = resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  // Checkpoint at the end.
  ASSERT_OK(DoCheckpoint(stream_id, tablet_id, checkpoint));

  // Insert more rows.
  NO_FATALS(InsertRows(10, 5));

  // Read from checkpoint: should get only the new rows.
  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, checkpoint, &resp2));
  ASSERT_FALSE(resp2.has_error());

  int new_inserts = CountRecordsByType(resp2, CDCOpTypePB::INSERT);
  EXPECT_EQ(5, new_inserts);
}

// Resume from checkpoint returns only new rows.
TEST_F(CDCITest, ResumeAfterCheckpoint) {
  NO_FATALS(StartClusterAndCreateTable());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));

  // Insert batch 1.
  NO_FATALS(InsertRows(0, 3));

  GetChangesResponsePB resp1;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp1));
  ASSERT_FALSE(resp1.has_error());
  int64_t cp1 = resp1.checkpoint_op_index();
  ASSERT_OK(DoCheckpoint(stream_id, tablet_id, cp1));

  // Insert batch 2.
  NO_FATALS(InsertRows(100, 4));

  // Read from checkpoint.
  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, cp1, &resp2));
  ASSERT_FALSE(resp2.has_error());
  EXPECT_EQ(4, CountRecordsByType(resp2, CDCOpTypePB::INSERT));
}

// Multi-tablet table: per-tablet GetChanges totals match inserts.
TEST_F(CDCITest, MultiTabletStream) {
  NO_FATALS(StartClusterAndCreateTable(/*num_tablets=*/3));
  vector<string> tablet_ids = GetAllTabletIds();
  ASSERT_EQ(3, tablet_ids.size());

  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));

  // Insert rows that span all tablets (keys 0-2999 across 3 splits at 1000, 2000).
  NO_FATALS(InsertRows(0, 30));

  // Read from all tablets and count total inserts.
  int total_inserts = 0;
  for (const auto& tid : tablet_ids) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(stream_id, tid, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    total_inserts += CountRecordsByType(resp, CDCOpTypePB::INSERT);
  }
  EXPECT_EQ(30, total_inserts);
}

// Deleting a stream while a consumer is attached must end the consumer's
// session and release the tablet's retention barrier. Once that release has
// landed, the tablet server must reject the consumer's next GetChanges and
// Checkpoint with STREAM_NOT_FOUND instead of serving the deleted stream from
// its config cache or re-creating its session.
TEST_F(CDCITest, StreamDeletedWhileConsuming) {
  // Run the master's CDC maintenance pass, which reaps deleted streams, often.
  FLAGS_catalog_manager_bg_task_wait_ms = 100;
  FLAGS_cdc_bg_scan_interval_ms = 100;
  NO_FATALS(StartClusterAndCreateTable());
  const string tablet_id = GetTabletId();
  tserver::TabletServer* ts = cluster_->mini_tablet_server(0)->server();
  cdc::CDCServiceImpl* cdc_service = ts->cdc_service();
  ASSERT_NE(nullptr, cdc_service);
  scoped_refptr<TabletReplica> replica;
  ASSERT_OK(ts->tablet_manager()->GetTabletReplica(tablet_id, &replica));

  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));
  NO_FATALS(InsertRows(0, 5));

  // Consume and checkpoint: this warms the tablet server's stream config cache
  // and establishes the consumer session.
  GetChangesResponsePB resp1;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp1));
  ASSERT_FALSE(resp1.has_error()) << resp1.error().DebugString();
  EXPECT_EQ(5, CountRecordsByType(resp1, CDCOpTypePB::INSERT));
  const int64_t checkpoint = resp1.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);
  ASSERT_OK(DoCheckpoint(stream_id, tablet_id, checkpoint));
  ASSERT_TRUE(cdc_service->HasSessionForTests(stream_id, tablet_id));
  ASSERT_EVENTUALLY([&] {
    ASSERT_TRUE(cdc_service->RetentionBarrierForTests(tablet_id));
    ASSERT_GE(replica->tablet_metadata()->cdc_min_retained_op_index(), 0);
  });

  // Delete the stream on the master.
  ASSERT_OK(DeleteCDCStream(stream_id));

  // The master no longer reports the stream.
  {
    GetCDCStreamInfoRequestPB info_req;
    GetCDCStreamInfoResponsePB info_resp;
    info_req.set_stream_id(stream_id);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(master_proxy_->GetCDCStreamInfo(info_req, &info_resp, &rpc));
    ASSERT_TRUE(info_resp.has_error());
    ASSERT_TRUE(StatusFromPB(info_resp.error().status()).IsNotFound())
        << info_resp.error().DebugString();
  }

  // The reap ends the consumer session and releases the retention barrier.
  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(cdc_service->HasSessionForTests(stream_id, tablet_id));
    ASSERT_FALSE(cdc_service->RetentionBarrierForTests(tablet_id));
    ASSERT_EQ(-1, replica->tablet_metadata()->cdc_min_retained_op_index());
  });

  // The consumer keeps polling: GetChanges is rejected...
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(stream_id, tablet_id, checkpoint, &resp));
    ASSERT_TRUE(resp.has_error());
    EXPECT_EQ(cdc::CDCErrorPB::STREAM_NOT_FOUND, resp.error().code())
        << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
  }
  // ...and so is Checkpoint, which must not re-create the consumer session.
  {
    CheckpointResponsePB resp;
    ASSERT_OK(DoCheckpoint(stream_id, tablet_id, checkpoint, &resp));
    ASSERT_TRUE(resp.has_error());
    EXPECT_EQ(cdc::CDCErrorPB::STREAM_NOT_FOUND, resp.error().code())
        << resp.error().DebugString();
  }
  ASSERT_FALSE(cdc_service->HasSessionForTests(stream_id, tablet_id));
  ASSERT_FALSE(cdc_service->RetentionBarrierForTests(tablet_id));
}

// Checkpoint responds to the consumer before its checkpoint reaches the master.
// The request, and the ids it carries, can be freed as soon as the response is
// sent, so neither the rest of the call nor the persist may read them from the
// request.
TEST_F(CDCITest, CheckpointPersistAfterRespondUsesCopies) {
  NO_FATALS(StartClusterAndCreateTable());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));
  NO_FATALS(InsertRows(0, 5));

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const int64_t checkpoint = resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  // Keep the call running after its response is sent and its request freed.
  FLAGS_cdc_inject_latency_after_checkpoint_respond_ms = 1000;
  ASSERT_OK(DoCheckpoint(stream_id, tablet_id, checkpoint));

  // The master records the checkpoint under the exact (stream, tablet) pair.
  ASSERT_EVENTUALLY([&] {
    GetCDCStreamInfoRequestPB info_req;
    GetCDCStreamInfoResponsePB info_resp;
    info_req.set_stream_id(stream_id);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(master_proxy_->GetCDCStreamInfo(info_req, &info_resp, &rpc));
    ASSERT_FALSE(info_resp.has_error()) << info_resp.error().DebugString();
    const auto& checkpoints = info_resp.stream().tablet_checkpoints();
    const auto it = checkpoints.find(tablet_id);
    ASSERT_NE(checkpoints.end(), it);
    ASSERT_EQ(checkpoint, it->second);
  });
}

// CreateCDCStream takes table ids. A table name is not a valid table id and
// must be rejected with NotFound rather than silently matched.
TEST_F(CDCITest, CreateStreamWithTableNameRejected) {
  NO_FATALS(StartClusterAndCreateTable());
  ASSERT_NE(table_name_, table_id_);

  string stream_id;
  Status s = CreateCDCStream(table_name_, &stream_id);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_TRUE(stream_id.empty());
}

// Verify records from multiple insert batches are ordered by op_index.
TEST_F(CDCITest, RecordsOrderedByOpIndex) {
  NO_FATALS(StartClusterAndCreateTable());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));

  NO_FATALS(InsertRows(0, 5));
  NO_FATALS(InsertRows(100, 5));

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
  ASSERT_FALSE(resp.has_error());
  EXPECT_EQ(10, CountRecordsByType(resp, CDCOpTypePB::INSERT));

  // Verify op_index is non-decreasing.
  int64_t prev_index = 0;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::INSERT) {
      EXPECT_GE(resp.records(i).op_index(), prev_index)
          << "Record " << i << " has out-of-order op_index";
      prev_index = resp.records(i).op_index();
    }
  }
}

// A negative barrier index releases the barrier of a tablet. A checkpoint row
// with a negative op index, which an older master accepted from any client,
// must not make the master release the barrier that the tablet's streams need.
TEST_F(CDCITest, NegativeCheckpointRowDoesNotReleaseBarrier) {
  FLAGS_catalog_manager_bg_task_wait_ms = 100;
  FLAGS_cdc_bg_scan_interval_ms = 100;
  NO_FATALS(StartClusterAndCreateTable());
  const string tablet_id = GetTabletId();
  tserver::TabletServer* ts = cluster_->mini_tablet_server(0)->server();
  scoped_refptr<TabletReplica> replica;
  ASSERT_OK(ts->tablet_manager()->GetTabletReplica(tablet_id, &replica));
  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, replica->tablet_metadata()->cdc_min_retained_op_index());
  });

  const int64_t seq_before = replica->tablet_metadata()->cdc_barrier_seq();

  // Plant the row, and restart the master to load it.
  master::SysCDCTabletCheckpointEntryPB row;
  row.set_stream_id(stream_id);
  row.set_tablet_id(tablet_id);
  row.set_op_index(-1);
  row.set_last_active_time_micros(GetCurrentTimeMicros());
  row.set_last_checkpoint_advance_time_micros(GetCurrentTimeMicros());
  ASSERT_OK(cluster_->mini_master()->master()->catalog_manager()->sys_catalog()
                ->WriteCDCTabletCheckpoint(stream_id, tablet_id, row));
  cluster_->mini_master()->Shutdown();
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_OK(cluster_->mini_master()->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      kTimeout));
  // Wait for the new leader master to update the barrier.
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(replica->tablet_metadata()->cdc_barrier_seq(), seq_before);
  });
  EXPECT_TRUE(ts->cdc_service()->RetentionBarrierForTests(tablet_id));
  EXPECT_EQ(0, replica->tablet_metadata()->cdc_min_retained_op_index());
}

// With a config cache TTL of 0, a tablet server fetches the stream config from
// the master for every request instead of caching it forever.
TEST_F(CDCITest, StreamConfigCacheTtlZeroRefetches) {
  FLAGS_cdc_stream_config_cache_ttl_ms = 0;
  NO_FATALS(StartClusterAndCreateTable());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));
  auto* cdc = cluster_->mini_tablet_server(0)->server()->cdc_service();

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const int64_t fetches = cdc->StreamConfigMasterFetchesForTests();
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  EXPECT_EQ(fetches + 1, cdc->StreamConfigMasterFetchesForTests());
}

class CDCUnsupportedStreamConfigITest : public CDCITest {
 protected:
  // Rewrites the sys catalog row of a stream with 'mutate' applied to it, as a
  // newer master that knows more stream options would have written it, and
  // restarts the master to load it. Then checks that the tablet server refuses
  // to serve the stream instead of serving it without those options: both
  // GetChanges and Checkpoint fail with a non-retryable error, even though the
  // tablet server had a valid config of the stream cached.
  void CheckRejected(const function<void(SysCDCStreamEntryPB*)>& mutate) {
    // Refetch the config for every call, so the tablet server would serve the
    // stale cached one if the fetched one were treated as a transient failure.
    FLAGS_cdc_stream_config_cache_ttl_ms = 0;
    NO_FATALS(StartClusterAndCreateTable());
    const string tablet_id = GetTabletId();
    string stream_id;
    ASSERT_OK(CreateCDCStream(table_id_, &stream_id));
    NO_FATALS(InsertRows(0, 5));
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_GT(resp.records_size(), 0);
    ASSERT_OK(DoCheckpoint(stream_id, tablet_id, 0));

    master::CatalogManager* catalog = cluster_->mini_master()->master()->catalog_manager();
    StreamRowVisitor visitor(stream_id);
    ASSERT_OK(catalog->sys_catalog()->VisitCDCStreams(&visitor));
    ASSERT_TRUE(visitor.found);
    SysCDCStreamEntryPB row = visitor.row;
    mutate(&row);
    ASSERT_OK(catalog->sys_catalog()->WriteCDCStream(stream_id, row));
    // Do not remember failed fetches: every call below fetches the config.
    FLAGS_cdc_stream_config_negative_cache_ttl_ms = 0;
    NO_FATALS(RestartMaster());

    cdc::CDCServiceImpl* cdc = cluster_->mini_tablet_server(0)->server()->cdc_service();
    const auto check_rejected = [&]() {
      GetChangesResponsePB resp;
      ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
      ASSERT_TRUE(resp.has_error()) << "served " << resp.records_size() << " records";
      NO_FATALS(CheckNotSupported(resp.error()));
      CheckpointResponsePB cp_resp;
      ASSERT_OK(DoCheckpoint(stream_id, tablet_id, 0, &cp_resp));
      ASSERT_TRUE(cp_resp.has_error());
      NO_FATALS(CheckNotSupported(cp_resp.error()));
    };
    // While the tablet server cannot reach the restarted master, it may serve
    // the cached config.
    ASSERT_EVENTUALLY([&] {
      NO_FATALS(check_rejected());
    });
    // Each call fetches the config and is refused.
    for (int i = 0; i < 3; ++i) {
      const int64_t fetches = cdc->StreamConfigMasterFetchesForTests();
      NO_FATALS(check_rejected());
      ASSERT_EQ(fetches + 2, cdc->StreamConfigMasterFetchesForTests());
    }

    // With the master down, the tablet server must not fall back to the valid
    // config it had cached before: the calls are shed as retryable.
    cluster_->mini_master()->Shutdown();
    {
      GetChangesResponsePB resp;
      ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
      ASSERT_TRUE(resp.has_error()) << "served " << resp.records_size() << " records";
      ASSERT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code())
          << resp.error().DebugString();
      ASSERT_TRUE(resp.error().is_retryable());
      CheckpointResponsePB cp_resp;
      ASSERT_OK(DoCheckpoint(stream_id, tablet_id, 0, &cp_resp));
      ASSERT_TRUE(cp_resp.has_error());
      ASSERT_EQ(CDCErrorPB::SERVER_TOO_BUSY, cp_resp.error().code())
          << cp_resp.error().DebugString();
    }
    NO_FATALS(RestartMaster());
    ASSERT_EVENTUALLY([&] {
      NO_FATALS(check_rejected());
    });

    // A refused fetch is remembered: only the first call fetches the config.
    FLAGS_cdc_stream_config_negative_cache_ttl_ms = 60 * 1000;
    const int64_t fetches = cdc->StreamConfigMasterFetchesForTests();
    for (int i = 0; i < 3; ++i) {
      NO_FATALS(check_rejected());
      ASSERT_EQ(fetches + 1, cdc->StreamConfigMasterFetchesForTests());
    }
  }

 private:
  void RestartMaster() {
    cluster_->mini_master()->Shutdown();
    ASSERT_OK(cluster_->mini_master()->Restart());
    ASSERT_OK(cluster_->mini_master()->master()
                  ->WaitUntilCatalogManagerIsLeaderAndReadyForTests(kTimeout));
  }

  static void CheckNotSupported(const CDCErrorPB& error) {
    ASSERT_EQ(CDCErrorPB::UNKNOWN_ERROR, error.code()) << error.DebugString();
    ASSERT_FALSE(error.is_retryable());
    ASSERT_FALSE(error.needs_resnapshot());
    ASSERT_STR_CONTAINS(error.status().message(), "does not support");
  }

  class StreamRowVisitor : public master::CDCStreamVisitor {
   public:
    explicit StreamRowVisitor(string stream_id) : stream_id_(std::move(stream_id)) {}
    Status Visit(const string& stream_id, const SysCDCStreamEntryPB& metadata) override {
      if (stream_id == stream_id_) {
        row = metadata;
        found = true;
      }
      return Status::OK();
    }
    SysCDCStreamEntryPB row;
    bool found = false;

   private:
    const string stream_id_;
  };
};

// A stream config with a field this tablet server does not know.
TEST_F(CDCUnsupportedStreamConfigITest, UnknownConfigField) {
  NO_FATALS(CheckRejected([](SysCDCStreamEntryPB* row) {
    row->mutable_config()->mutable_unknown_fields()->AddVarint(1000, 1);
  }));
}

// A stream config with a record type this tablet server does not know.
TEST_F(CDCUnsupportedStreamConfigITest, UnknownRecordType) {
  NO_FATALS(CheckRejected([](SysCDCStreamEntryPB* row) {
    row->mutable_config()->clear_record_type();
    row->mutable_config()->mutable_unknown_fields()->AddVarint(
        master::CDCStreamConfigPB::kRecordTypeFieldNumber, 100);
  }));
}

// The tablet server reports the time to expiry of a stream against the
// master's --cdc_stream_expiry_ms, which the master sends with the stream
// config, not against its own --cdc_stream_idle_expiry_ms.
TEST_F(CDCITest, TimeToExpiryFollowsMasterExpiry) {
  FLAGS_cdc_stream_expiry_ms = 60 * 1000;
  FLAGS_cdc_stream_idle_expiry_ms = 8LL * 3600 * 1000;
  NO_FATALS(StartClusterAndCreateTable());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateCDCStream(table_id_, &stream_id));
  NO_FATALS(InsertRows(0, 1));
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  auto entity = cluster_->mini_tablet_server(0)->server()->metric_registry()
      ->FindOrCreateEntity(&METRIC_ENTITY_cdc_stream,
                           strings::Substitute("$0-$1", stream_id, tablet_id),
                           {{"stream_id", stream_id}, {"tablet_id", tablet_id}});
  scoped_refptr<Metric> m = entity->FindOrNull(METRIC_cdc_stream_time_to_expiry_micros);
  ASSERT_NE(nullptr, m.get());
  const int64_t time_to_expiry = down_cast<FunctionGauge<int64_t>*>(m.get())->value();
  EXPECT_GT(time_to_expiry, 0);
  EXPECT_LE(time_to_expiry, FLAGS_cdc_stream_expiry_ms * 1000);
}

} // namespace kudu
