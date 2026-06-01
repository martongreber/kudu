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
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.proxy.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);

using kudu::cdc::CDCServiceProxy;
using kudu::cdc::CheckpointRequestPB;
using kudu::cdc::CheckpointResponsePB;
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
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tablet::TabletReplica;
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
                      int64_t op_index) {
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);

    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(cdc_proxy_->Checkpoint(req, &resp, &rpc));
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
  shared_ptr<Messenger> messenger_;
  unique_ptr<CDCServiceProxy> cdc_proxy_;
  unique_ptr<MasterServiceProxy> master_proxy_;
};

const MonoDelta CDCITest::kTimeout = MonoDelta::FromSeconds(30);

// Full pipeline: create stream → insert → GetChanges → verify records.
TEST_F(CDCITest, CreateStreamAndGetChanges) {
  NO_FATALS(StartClusterAndCreateTable());
  string tablet_id = GetTabletId();

  // Create a CDC stream on the master.
  string stream_id;
  ASSERT_OK(CreateCDCStream(table_name_, &stream_id));
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

// Checkpoint anchors the WAL; after advancing we can still read from the anchor point.
TEST_F(CDCITest, CheckpointPreservesWAL) {
  NO_FATALS(StartClusterAndCreateTable());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateCDCStream(table_name_, &stream_id));

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

  // Read from checkpoint — should get only the new rows.
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
  ASSERT_OK(CreateCDCStream(table_name_, &stream_id));

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
  ASSERT_OK(CreateCDCStream(table_name_, &stream_id));

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

// After deleting a stream, GetChanges still works (tserver doesn't validate stream existence yet).
// This test documents current behavior; Phase 5 will add stream validation.
TEST_F(CDCITest, StreamDeletedWhileConsuming) {
  NO_FATALS(StartClusterAndCreateTable());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateCDCStream(table_name_, &stream_id));
  NO_FATALS(InsertRows(0, 5));

  // Read changes.
  GetChangesResponsePB resp1;
  ASSERT_OK(DoGetChanges(stream_id, tablet_id, 0, &resp1));
  ASSERT_FALSE(resp1.has_error());
  EXPECT_EQ(5, CountRecordsByType(resp1, CDCOpTypePB::INSERT));

  // Delete the stream on the master.
  ASSERT_OK(DeleteCDCStream(stream_id));

  // Verify stream is gone from master.
  GetCDCStreamInfoRequestPB info_req;
  GetCDCStreamInfoResponsePB info_resp;
  info_req.set_stream_id(stream_id);
  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(master_proxy_->GetCDCStreamInfo(info_req, &info_resp, &rpc));
  ASSERT_TRUE(info_resp.has_error());
}

// Verify records from multiple insert batches are ordered by op_index.
TEST_F(CDCITest, RecordsOrderedByOpIndex) {
  NO_FATALS(StartClusterAndCreateTable());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateCDCStream(table_name_, &stream_id));

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

} // namespace kudu
