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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.proxy.h"
#include "kudu/cdc/cdc_service.h"
#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_wal_retention_secs);
DECLARE_int32(log_segment_size_bytes_for_tests);
DECLARE_int32(log_max_segments_to_retain);
DECLARE_int64(cdc_stream_expiry_ms);
DECLARE_int64(cdc_max_staleness_ms);
DECLARE_int32(cdc_max_barrier_releases_per_run);
DECLARE_int64(cdc_max_transaction_span_bytes);
DECLARE_bool(enable_txn_system_client_init);
DECLARE_bool(txn_manager_enabled);
DECLARE_bool(txn_manager_lazily_initialized);

using kudu::cdc::CDCErrorPB;
using kudu::cdc::CDCServiceProxy;
using kudu::cdc::CheckpointRequestPB;
using kudu::cdc::CheckpointResponsePB;
using kudu::cdc::CDCOpTypePB;
using kudu::cdc::GetChangesRequestPB;
using kudu::cdc::GetChangesResponsePB;
using kudu::consensus::RaftPeerPB;
using kudu::master::CreateCDCStreamRequestPB;
using kudu::master::CreateCDCStreamResponsePB;
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

class CDCFailoverITest : public MiniClusterITestBase {
 protected:
  static const MonoDelta kTimeout;

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
    // Make the master's CDC retention-barrier maintenance run frequently so
    // tests don't wait the 60s production default.
    FLAGS_catalog_manager_bg_task_wait_ms = 100;
    FLAGS_cdc_bg_scan_interval_ms = 100;
  }

  void StartClusterWithRF3() {
    NO_FATALS(StartCluster(3));

    // Create a table with RF=3.
    client::KuduSchema schema;
    client::KuduSchemaBuilder schema_builder;
    schema_builder.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    schema_builder.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    CHECK_OK(schema_builder.Build(&schema));

    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name("cdc_failover_table")
        .schema(&schema)
        .set_range_partition_columns({"key"})
        .num_replicas(3);
    CHECK_OK(creator->Create());
    table_name_ = "cdc_failover_table";

    MessengerBuilder bld("CDCFailoverClient");
    CHECK_OK(bld.Build(&messenger_));

    master_proxy_.reset(new MasterServiceProxy(
        messenger_,
        cluster_->mini_master()->bound_rpc_addr(),
        cluster_->mini_master()->bound_rpc_addr().host()));
  }

  unique_ptr<CDCServiceProxy> MakeCDCProxy(int ts_idx) {
    return unique_ptr<CDCServiceProxy>(new CDCServiceProxy(
        messenger_,
        cluster_->mini_tablet_server(ts_idx)->bound_rpc_addr(),
        cluster_->mini_tablet_server(ts_idx)->bound_rpc_addr().host()));
  }

  Status CreateStream(string* stream_id) {
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.add_table_ids(table_name_);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(master_proxy_->CreateCDCStream(req, &resp, &rpc));
    if (resp.has_error()) return StatusFromPB(resp.error().status());
    *stream_id = resp.stream_id();
    return Status::OK();
  }

  Status CreateStreamWithConfig(master::CDCStreamConfigPB::RecordType record_type,
                                master::CDCStreamConfigPB::SnapshotMode snapshot_mode,
                                string* stream_id) {
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.add_table_ids(table_name_);
    req.mutable_config()->set_record_type(record_type);
    req.mutable_config()->set_snapshot_mode(snapshot_mode);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(master_proxy_->CreateCDCStream(req, &resp, &rpc));
    if (resp.has_error()) return StatusFromPB(resp.error().status());
    *stream_id = resp.stream_id();
    return Status::OK();
  }

  Status DeleteStream(const string& stream_id) {
    master::DeleteCDCStreamRequestPB req;
    master::DeleteCDCStreamResponsePB resp;
    req.set_stream_id(stream_id);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(master_proxy_->DeleteCDCStream(req, &resp, &rpc));
    if (resp.has_error()) return StatusFromPB(resp.error().status());
    return Status::OK();
  }

  void UpdateRow(int key, int new_val) {
    client::sp::shared_ptr<client::KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    CHECK_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    client::KuduUpdate* update = table->NewUpdate();
    CHECK_OK(update->mutable_row()->SetInt32("key", key));
    CHECK_OK(update->mutable_row()->SetInt32("val", new_val));
    CHECK_OK(session->Apply(update));
  }

  void DeleteRow(int key) {
    client::sp::shared_ptr<client::KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    CHECK_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    client::KuduDelete* del = table->NewDelete();
    CHECK_OK(del->mutable_row()->SetInt32("key", key));
    CHECK_OK(session->Apply(del));
  }

  static bool GetInt32(const cdc::CDCRecordPB& r, bool before,
                       const string& name, int32_t* out) {
    const auto& cols = before ? r.old_changes() : r.changes();
    for (const auto& c : cols) {
      if (c.column_name() == name) {
        if (c.is_null() || c.value().size() < sizeof(int32_t)) return false;
        memcpy(out, c.value().data(), sizeof(int32_t));
        return true;
      }
    }
    return false;
  }

  void InsertRows(int start, int count) {
    client::sp::shared_ptr<client::KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    CHECK_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = start; i < start + count; ++i) {
      client::KuduInsert* insert = table->NewInsert();
      CHECK_OK(insert->mutable_row()->SetInt32("key", i));
      CHECK_OK(insert->mutable_row()->SetInt32("val", i * 10));
      CHECK_OK(session->Apply(insert));
    }
  }

  // Adds a NULLABLE INT32 column via a synchronous online ALTER. This lands an
  // ALTER_SCHEMA_OP in the tablet's WAL, which CDC surfaces as a DDL record.
  void AddNullableIntColumn(const string& name) {
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(table_name_));
    alterer->AddColumn(name)->Type(client::KuduColumnSchema::INT32);
    CHECK_OK(alterer->Alter());
  }

  // Inserts rows carrying the post-ALTER 3-column schema (key, val, 'extra_col').
  // Opens the table fresh so the client picks up the altered schema. 'extra_col'
  // is set to key * 100 so its decoded value can be asserted.
  void InsertRowsWithExtra(int start, int count, const string& extra_col) {
    client::sp::shared_ptr<client::KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    CHECK_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = start; i < start + count; ++i) {
      client::KuduInsert* insert = table->NewInsert();
      CHECK_OK(insert->mutable_row()->SetInt32("key", i));
      CHECK_OK(insert->mutable_row()->SetInt32("val", i * 10));
      CHECK_OK(insert->mutable_row()->SetInt32(extra_col, i * 100));
      CHECK_OK(session->Apply(insert));
    }
  }

  // Writes 'count' rows (keys [start, start+count)) inside a single committed
  // transaction. Each row is flushed as its own WRITE_OP so the transaction
  // occupies many WAL ops, letting a test exceed a small per-response byte cap.
  void WriteCommittedTransaction(int start, int count) {
    client::sp::shared_ptr<client::KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduTransaction> txn;
    CHECK_OK(client_->NewTransaction(&txn));
    client::sp::shared_ptr<client::KuduSession> session;
    CHECK_OK(txn->CreateSession(&session));
    CHECK_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = start; i < start + count; ++i) {
      client::KuduInsert* insert = table->NewInsert();
      CHECK_OK(insert->mutable_row()->SetInt32("key", i));
      CHECK_OK(insert->mutable_row()->SetInt32("val", i * 10));
      CHECK_OK(session->Apply(insert));
    }
    CHECK_OK(txn->Commit());
  }

  // Materializes the live table (leader-only) into a key->val map. Used as
  // ground truth to check that snapshot + WAL replay reconstructs reality.
  std::map<int32_t, int32_t> ScanTable() {
    client::sp::shared_ptr<client::KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    client::KuduScanner scanner(table.get());
    CHECK_OK(scanner.SetSelection(client::KuduClient::LEADER_ONLY));
    CHECK_OK(scanner.SetReadMode(client::KuduScanner::READ_LATEST));
    CHECK_OK(scanner.Open());
    std::map<int32_t, int32_t> out;
    client::KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      CHECK_OK(scanner.NextBatch(&batch));
      for (const auto& row : batch) {
        int32_t k = -1;
        int32_t v = -1;
        CHECK_OK(row.GetInt32("key", &k));
        CHECK_OK(row.GetInt32("val", &v));
        out[k] = v;
      }
    }
    return out;
  }

  string GetTabletId() {
    vector<scoped_refptr<TabletReplica>> replicas;
    cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletReplicas(&replicas);
    CHECK(!replicas.empty());
    // Select the tablet for our data table; with transactions enabled the
    // tserver also hosts txn-status system-table tablets.
    for (const auto& r : replicas) {
      if (r->tablet_metadata()->table_name() == table_name_) {
        return r->tablet_id();
      }
    }
    return replicas[0]->tablet_id();
  }

  int FindLeaderIndex(const string& tablet_id) {
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      if (!cluster_->mini_tablet_server(i)->is_started()) continue;
      scoped_refptr<TabletReplica> replica;
      Status s = cluster_->mini_tablet_server(i)->server()->tablet_manager()
          ->GetTabletReplica(tablet_id, &replica);
      if (!s.ok()) continue;
      auto consensus = replica->shared_consensus();
      if (consensus && consensus->role() == RaftPeerPB::LEADER) {
        return i;
      }
    }
    return -1;
  }

  int FindFollowerIndex(const string& tablet_id) {
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      if (!cluster_->mini_tablet_server(i)->is_started()) continue;
      scoped_refptr<TabletReplica> replica;
      Status s = cluster_->mini_tablet_server(i)->server()->tablet_manager()
          ->GetTabletReplica(tablet_id, &replica);
      if (!s.ok()) continue;
      auto consensus = replica->shared_consensus();
      if (consensus && consensus->role() != RaftPeerPB::LEADER) {
        return i;
      }
    }
    return -1;
  }

  int CountInserts(const GetChangesResponsePB& resp) {
    int count = 0;
    for (int i = 0; i < resp.records_size(); ++i) {
      if (resp.records(i).op_type() == CDCOpTypePB::INSERT) count++;
    }
    return count;
  }

  // Persists a consumer checkpoint at 'op_index' via the CDC service on tserver
  // 'ts_idx'. The master's next maintenance pass turns this into a retention
  // barrier fanned out to (and persisted by) every replica.
  Status DoCheckpoint(int ts_idx, const string& stream_id,
                      const string& tablet_id, int64_t op_index) {
    auto proxy = MakeCDCProxy(ts_idx);
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(proxy->Checkpoint(req, &resp, &rpc));
    if (resp.has_error()) return StatusFromPB(resp.error().status());
    return Status::OK();
  }

  // Returns the tablet replica for 'tablet_id' on every currently-started
  // tserver.
  vector<scoped_refptr<TabletReplica>> GetAllReplicas(const string& tablet_id) {
    vector<scoped_refptr<TabletReplica>> out;
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      if (!cluster_->mini_tablet_server(i)->is_started()) continue;
      scoped_refptr<TabletReplica> r;
      if (cluster_->mini_tablet_server(i)->server()->tablet_manager()
              ->GetTabletReplica(tablet_id, &r).ok()) {
        out.push_back(std::move(r));
      }
    }
    return out;
  }

  string table_name_;
  shared_ptr<Messenger> messenger_;
  unique_ptr<MasterServiceProxy> master_proxy_;
};

const MonoDelta CDCFailoverITest::kTimeout = MonoDelta::FromSeconds(30);

// GetChanges on a non-leader should return TABLET_NOT_LEADER.
TEST_F(CDCFailoverITest, GetChanges_NonLeader) {
  NO_FATALS(StartClusterWithRF3());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  // Wait for leader election to settle.
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(FindLeaderIndex(tablet_id), 0);
  });

  int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);

  auto proxy = MakeCDCProxy(follower_idx);
  GetChangesRequestPB req;
  GetChangesResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);

  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_LEADER, resp.error().code());
}

// After leader failover, the new leader can serve changes without data loss.
TEST_F(CDCFailoverITest, GetChanges_AfterLeaderFailover) {
  NO_FATALS(StartClusterWithRF3());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  NO_FATALS(InsertRows(0, 5));

  // Wait for leader to be established.
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  // Read from leader.
  auto leader_proxy = MakeCDCProxy(leader_idx);
  GetChangesRequestPB req;
  GetChangesResponsePB resp1;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);

  RpcController rpc1;
  rpc1.set_timeout(kTimeout);
  ASSERT_OK(leader_proxy->GetChanges(req, &resp1, &rpc1));
  ASSERT_FALSE(resp1.has_error()) << resp1.error().DebugString();
  EXPECT_EQ(5, CountInserts(resp1));
  int64_t checkpoint = resp1.checkpoint_op_index();

  // Kill the leader.
  cluster_->mini_tablet_server(leader_idx)->Shutdown();

  // Wait for a new leader to emerge.
  int new_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    new_leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(new_leader_idx, 0);
    ASSERT_NE(new_leader_idx, leader_idx);
  });

  // Insert more rows on the new leader.
  NO_FATALS(InsertRows(100, 3));

  // Read from new leader starting at the old checkpoint.
  auto new_proxy = MakeCDCProxy(new_leader_idx);
  GetChangesResponsePB resp2;
  RpcController rpc2;
  rpc2.set_timeout(kTimeout);
  req.set_from_op_index(checkpoint);
  ASSERT_OK(new_proxy->GetChanges(req, &resp2, &rpc2));
  ASSERT_FALSE(resp2.has_error()) << resp2.error().DebugString();
  EXPECT_EQ(3, CountInserts(resp2));
}

// GetChanges with a tablet_id that doesn't exist returns TABLET_NOT_FOUND.
TEST_F(CDCFailoverITest, GetChanges_TabletNotFound) {
  NO_FATALS(StartClusterWithRF3());

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  auto proxy = MakeCDCProxy(0);
  GetChangesRequestPB req;
  GetChangesResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id("nonexistent-tablet-xyz");
  req.set_from_op_index(0);

  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_FOUND, resp.error().code());
}

// GetChanges for a stream the master has never heard of must return
// STREAM_NOT_FOUND, not silently fall through as CHANGE-mode WAL data. The
// tablet is valid; only the stream is unknown, so the stream-existence check
// (which precedes the leadership/WAL read) is what must reject the request.
TEST_F(CDCFailoverITest, GetChanges_UnknownStreamReturnsStreamNotFound) {
  NO_FATALS(StartClusterWithRF3());

  // A real stream/tablet exists, but we query a bogus stream_id.
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  const string tablet_id = GetTabletId();

  auto proxy = MakeCDCProxy(0);
  GetChangesRequestPB req;
  GetChangesResponsePB resp;
  req.set_stream_id("this-stream-was-never-created");
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);

  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::STREAM_NOT_FOUND, resp.error().code())
      << resp.error().status().message();
}

// GetChanges for a stream that was deleted must return STREAM_NOT_FOUND once the
// tserver refetches its (cold) config from the master, rather than resurrecting
// the deleted stream and serving WAL data for it.
TEST_F(CDCFailoverITest, GetChanges_DeletedStreamReturnsStreamNotFound) {
  NO_FATALS(StartClusterWithRF3());

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  const string tablet_id = GetTabletId();
  ASSERT_OK(DeleteStream(stream_id));

  auto proxy = MakeCDCProxy(0);
  GetChangesRequestPB req;
  GetChangesResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);

  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::STREAM_NOT_FOUND, resp.error().code())
      << resp.error().status().message();
}

// GetChanges with a negative from_op_index is a malformed request and must be
// rejected up front rather than passed through to the WAL reader.
TEST_F(CDCFailoverITest, GetChanges_NegativeFromOpIndexRejected) {
  NO_FATALS(StartClusterWithRF3());

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  const string tablet_id = GetTabletId();

  auto proxy = MakeCDCProxy(0);
  GetChangesRequestPB req;
  GetChangesResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(-5);

  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::UNKNOWN_ERROR, resp.error().code());
  EXPECT_STR_CONTAINS(resp.error().status().message(), "from_op_index");
}

// An INITIAL_ONLY stream emits its bootstrap snapshot and then stops; it must
// never stream the WAL. A streaming GetChanges (no snapshot flags) against such
// a stream is a consumer bug and must be rejected loudly rather than silently
// serving WAL records that violate the stream's contract.
TEST_F(CDCFailoverITest, GetChanges_InitialOnlyRejectsWalStreaming) {
  NO_FATALS(StartClusterWithRF3());

  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::CHANGE,
                                   master::CDCStreamConfigPB::INITIAL_ONLY,
                                   &stream_id));
  const string tablet_id = GetTabletId();

  auto proxy = MakeCDCProxy(0);
  GetChangesRequestPB req;
  GetChangesResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);
  // No is_snapshot_start: this is a plain WAL-streaming request.

  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::UNKNOWN_ERROR, resp.error().code())
      << resp.error().status().message();
  EXPECT_STR_CONTAINS(resp.error().status().message(), "INITIAL_ONLY");
}

// Checkpoint on a non-existent tablet returns TABLET_NOT_FOUND.
TEST_F(CDCFailoverITest, Checkpoint_TabletNotFound) {
  NO_FATALS(StartClusterWithRF3());

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  auto proxy = MakeCDCProxy(0);
  CheckpointRequestPB req;
  CheckpointResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id("nonexistent-tablet-zzz");
  req.set_op_index(5);

  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_FOUND, resp.error().code());
}

// The master's periodic CDC maintenance must push the WAL retention barrier to
// EVERY replica of a CDC tablet, not just the leader, so retention survives a
// leader change and independent follower log GC. Uses an empty tablet so the
// only possible LogAnchorRegistry anchor on a follower is the CDC barrier.
TEST_F(CDCFailoverITest, RetentionBarrierPushedToAllReplicas) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  // Wait for a leader, then pick a follower.
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);

  scoped_refptr<TabletReplica> follower;
  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &follower));

  // No checkpoint has been persisted yet, so the follower has no CDC anchor
  // (the tablet is empty, so there are no MemRowSet/DiskRowSet anchors either).
  int64_t idx = -1;
  Status s = follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(&idx);
  EXPECT_TRUE(s.IsNotFound()) << "unexpected pre-existing anchor at index " << idx;

  // Persist a checkpoint via the leader; this writes it durably to the master.
  {
    auto proxy = MakeCDCProxy(leader_idx);
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(1);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // The master's maintenance scan must push the barrier to the follower, which
  // then registers a CDC retention anchor at (or below) the checkpoint. This
  // proves the barrier reaches non-leader replicas.
  ASSERT_EVENTUALLY([&] {
    int64_t follower_anchor = -1;
    ASSERT_OK(follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(
        &follower_anchor));
    ASSERT_LE(follower_anchor, 1);
  });
}

// Phase 6.1/6.2: once a stream is idle beyond --cdc_stream_expiry_ms, the master
// releases its retention barrier on every replica.
TEST_F(CDCFailoverITest, Expiry_ReleasesBarrierOnAllReplicas) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  scoped_refptr<TabletReplica> follower;
  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &follower));

  // Persist a checkpoint so the master sets the barrier on all replicas.
  {
    auto proxy = MakeCDCProxy(leader_idx);
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(1);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // Barrier reaches the follower.
  ASSERT_EVENTUALLY([&] {
    int64_t follower_anchor = -1;
    ASSERT_OK(follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(
        &follower_anchor));
    ASSERT_LE(follower_anchor, 1);
  });

  // Force the stream to appear idle; the next maintenance pass must release the
  // barrier on all replicas.
  FLAGS_cdc_stream_expiry_ms = 1;
  ASSERT_EVENTUALLY([&] {
    int64_t follower_anchor = -1;
    Status s = follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(
        &follower_anchor);
    ASSERT_TRUE(s.IsNotFound())
        << "retention anchor still present at index " << follower_anchor;
  });
}

// A consumer that keeps polling (refreshing last-active) but never advances its
// checkpoint must not pin retention forever: once its checkpoint has not moved
// for longer than --cdc_max_staleness_ms, the master releases the barrier on
// every replica. This is distinct from idle-expiry, which polling defeats.
TEST_F(CDCFailoverITest, MaxStaleness_ReleasesBarrierDespitePolling) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  scoped_refptr<TabletReplica> follower;
  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &follower));

  auto proxy = MakeCDCProxy(leader_idx);
  // Persist a checkpoint at index 1 so the master sets the barrier on all
  // replicas and records this as the stream's last checkpoint advance.
  auto checkpoint_at = [&](int64_t op_index) {
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  };
  NO_FATALS(checkpoint_at(1));

  // Barrier reaches the follower.
  ASSERT_EVENTUALLY([&] {
    int64_t follower_anchor = -1;
    ASSERT_OK(follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(
        &follower_anchor));
    ASSERT_LE(follower_anchor, 1);
  });

  // Keep idle-expiry disabled (default 8h) so only the staleness guard can act,
  // then set a tiny staleness window. The checkpoint has not advanced past 1, so
  // even though we keep re-checkpointing at the same index below (refreshing
  // last-active), the maintenance pass must release the barrier.
  FLAGS_cdc_max_staleness_ms = 1;
  ASSERT_EVENTUALLY([&] {
    // Re-send the same checkpoint index: this refreshes last-active (as a
    // polling-but-not-advancing consumer would) but is not forward progress, so
    // it must not reset the staleness timer.
    NO_FATALS(checkpoint_at(1));
    int64_t follower_anchor = -1;
    Status s = follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(
        &follower_anchor);
    ASSERT_TRUE(s.IsNotFound())
        << "retention anchor still present at index " << follower_anchor;
  });
}

// A mass expiry (many tablets losing their pin in one maintenance tick) must not
// flood the master's outbound RPC path: --cdc_max_barrier_releases_per_run caps
// the number of release RPCs per pass and defers the rest to later passes. The
// critical property is that deferred releases are RETRIED, not dropped -- with a
// cap of 1 and three pinned tablets, all three barriers must still eventually be
// released. (If the deferred set were forgotten, the excess tablets would keep
// their barrier forever and this test would time out.)
TEST_F(CDCFailoverITest, BarrierReleaseFanoutIsCappedButRetried) {
  NO_FATALS(StartCluster(3));

  // Build a table with three range partitions -> three data tablets.
  client::KuduSchema schema;
  client::KuduSchemaBuilder b;
  b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(b.Build(&schema));

  unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
  auto make_bound = [&](int v) {
    unique_ptr<KuduPartialRow> row(schema.NewRow());
    CHECK_OK(row->SetInt32("key", v));
    return row;
  };
  creator->table_name("cdc_fanout_table")
      .schema(&schema)
      .set_range_partition_columns({"key"})
      .num_replicas(3);
  creator->add_range_partition(make_bound(0).release(), make_bound(100).release());
  creator->add_range_partition(make_bound(100).release(), make_bound(200).release());
  creator->add_range_partition(make_bound(200).release(), make_bound(300).release());
  ASSERT_OK(creator->Create());
  table_name_ = "cdc_fanout_table";

  MessengerBuilder bld("CDCFanoutClient");
  ASSERT_OK(bld.Build(&messenger_));
  master_proxy_.reset(new MasterServiceProxy(
      messenger_, cluster_->mini_master()->bound_rpc_addr(),
      cluster_->mini_master()->bound_rpc_addr().host()));

  // Collect the three data tablets and a local replica for each (TS 0 hosts a
  // replica of every tablet since RF=3 on a 3-node cluster).
  std::unordered_map<string, scoped_refptr<TabletReplica>> replicas_by_tablet;
  ASSERT_EVENTUALLY([&] {
    replicas_by_tablet.clear();
    vector<scoped_refptr<TabletReplica>> replicas;
    cluster_->mini_tablet_server(0)->server()->tablet_manager()
        ->GetTabletReplicas(&replicas);
    for (const auto& r : replicas) {
      if (r->tablet_metadata()->table_name() == table_name_) {
        replicas_by_tablet[r->tablet_id()] = r;
      }
    }
    ASSERT_EQ(3, replicas_by_tablet.size());
  });

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  // Checkpoint every tablet so the master pins (barriers) all three.
  for (const auto& e : replicas_by_tablet) {
    const string& tablet_id = e.first;
    int leader_idx = -1;
    ASSERT_EVENTUALLY([&] {
      leader_idx = FindLeaderIndex(tablet_id);
      ASSERT_GE(leader_idx, 0);
    });
    auto proxy = MakeCDCProxy(leader_idx);
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(1);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // Wait until the master has actually pinned all three tablets under a
  // retention barrier (as opposed to merely the consumer's own checkpoint
  // anchor, which the Checkpoint RPC registers directly). Only then does a
  // subsequent expiry give the maintenance pass three barriers to release.
  master::CatalogManager* catalog =
      cluster_->mini_master()->master()->catalog_manager();
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(3, catalog->cdc_barriered_tablet_count());
  });

  // Cap releases at one per maintenance pass, then expire every stream at once.
  // We assert on the master's own dispatch accounting rather than on the tablet
  // replicas' anchor state: the actual barrier-release RPC is best-effort and a
  // late barrier-*set* task from a prior pass can re-anchor a replica after its
  // release (a separate, pre-existing reordering hazard), which makes observing
  // the resulting anchor inherently flaky. The cap logic itself lives entirely
  // in the master, so the master-side counters are the precise, deterministic
  // signal for this fix.
  //
  // With three pinned tablets expiring at once and a cap of 1, the master must:
  //   pass 1: release 1, defer 2;  pass 2: release 1, defer 1;  pass 3: release 1.
  // So it dispatches exactly three releases total and records exactly three
  // deferrals (2 + 1). Without the cap, all three release in one pass and the
  // deferred counter never moves -- which is what the temp-revert check relies on.
  const int64_t releases_before = catalog->cdc_barrier_releases_total();
  const int64_t deferred_before =
      catalog->cdc_barrier_releases_deferred_total();

  FLAGS_cdc_max_barrier_releases_per_run = 1;
  FLAGS_cdc_stream_expiry_ms = 1;

  // The cap must have deferred releases (proving the throttle engaged) AND all
  // three releases must eventually be dispatched (proving deferred releases are
  // retried, not dropped).
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(catalog->cdc_barrier_releases_deferred_total() - deferred_before, 3)
        << "cap did not defer any releases";
    ASSERT_GE(catalog->cdc_barrier_releases_total() - releases_before, 3)
        << "not all deferred releases were retried";
  });
}

// Phase 6.2: deleting a stream releases its retention barrier on every replica.
TEST_F(CDCFailoverITest, DeleteStream_ReleasesRetention) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  scoped_refptr<TabletReplica> follower;
  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &follower));

  {
    auto proxy = MakeCDCProxy(leader_idx);
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(1);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  ASSERT_EVENTUALLY([&] {
    int64_t follower_anchor = -1;
    ASSERT_OK(follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(
        &follower_anchor));
    ASSERT_LE(follower_anchor, 1);
  });

  // Delete the stream; DeleteCDCStream must release the orphaned tablet's barrier.
  {
    master::DeleteCDCStreamRequestPB req;
    master::DeleteCDCStreamResponsePB resp;
    req.set_stream_id(stream_id);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(master_proxy_->DeleteCDCStream(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  ASSERT_EVENTUALLY([&] {
    int64_t follower_anchor = -1;
    Status s = follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(
        &follower_anchor);
    ASSERT_TRUE(s.IsNotFound())
        << "retention anchor still present at index " << follower_anchor;
  });
}

// A4: the per-(stream, tablet) consumer anchor is established by the consumer's
// own GetChanges/Checkpoint polling and lives only on the leader (in
// stream_tablet_state_), distinct from the master-pushed aggregate barrier that
// DeleteStream_ReleasesRetention covers. Without an explicit release on stream
// delete it would be freed only when the tablet itself is deleted, so a deleted
// stream would keep pinning the leader's WAL. Verify DeleteCDCStream fans a
// consumer-anchor release out to the replicas so the leader's consumer anchor
// is gone after the delete.
TEST_F(CDCFailoverITest, DeleteStream_ReleasesConsumerAnchor) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  scoped_refptr<TabletReplica> follower;
  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &follower));

  cdc::CDCServiceImpl* leader_cdc =
      cluster_->mini_tablet_server(leader_idx)->server()->cdc_service();
  ASSERT_NE(nullptr, leader_cdc);

  // A Checkpoint establishes the per-(stream, tablet) consumer anchor on the
  // leader.
  {
    auto proxy = MakeCDCProxy(leader_idx);
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(1);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
  ASSERT_TRUE(leader_cdc->ConsumerAnchorForTests(stream_id, tablet_id))
      << "consumer anchor was not established by the Checkpoint poll";

  // The checkpoint persist to the master is best-effort and asynchronous, so
  // wait until the master has learned it and its maintenance pass has fanned the
  // aggregate barrier out to a follower. This guarantees the deleted stream's
  // tablet_checkpoints entry exists on the master, so DeleteCDCStream knows to
  // fan the consumer-anchor release out for this tablet.
  ASSERT_EVENTUALLY([&] {
    int64_t follower_anchor = -1;
    ASSERT_OK(follower->log_anchor_registry()->GetEarliestRegisteredLogIndex(
        &follower_anchor));
    ASSERT_LE(follower_anchor, 1);
  });

  // Delete the stream; DeleteCDCStream must fan a consumer-anchor release out to
  // the replicas.
  {
    master::DeleteCDCStreamRequestPB req;
    master::DeleteCDCStreamResponsePB resp;
    req.set_stream_id(stream_id);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(master_proxy_->DeleteCDCStream(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(leader_cdc->ConsumerAnchorForTests(stream_id, tablet_id))
        << "consumer anchor still pinned on the leader after stream delete (A4 leak)";
  });
}

// A committed transaction's rows are emitted wrapped in BEGIN/COMMIT with the
// commit timestamp; an aborted transaction's rows are dropped entirely.
TEST_F(CDCFailoverITest, TransactionalWrites) {
  // Enable Kudu transactions for this test's cluster.
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name_, &table));

  // Committed transaction: 3 rows.
  {
    client::sp::shared_ptr<client::KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    client::sp::shared_ptr<client::KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = 0; i < 3; i++) {
      client::KuduInsert* insert = table->NewInsert();
      ASSERT_OK(insert->mutable_row()->SetInt32("key", 100 + i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i));
      ASSERT_OK(session->Apply(insert));
    }
    ASSERT_OK(txn->Commit());
  }

  // Aborted transaction: 3 rows that must never be emitted.
  {
    client::sp::shared_ptr<client::KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    client::sp::shared_ptr<client::KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = 0; i < 3; i++) {
      client::KuduInsert* insert = table->NewInsert();
      ASSERT_OK(insert->mutable_row()->SetInt32("key", 200 + i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i));
      ASSERT_OK(session->Apply(insert));
    }
    ASSERT_OK(txn->Rollback());
  }

  // Read all changes from the leader.
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  GetChangesResponsePB resp;
  {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(0);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  int begins = 0;
  int commits = 0;
  int inserts = 0;
  for (const auto& rec : resp.records()) {
    switch (rec.op_type()) {
      case CDCOpTypePB::BEGIN:
        begins++;
        EXPECT_TRUE(rec.has_commit_timestamp());
        break;
      case CDCOpTypePB::COMMIT:
        commits++;
        EXPECT_TRUE(rec.has_commit_timestamp());
        break;
      case CDCOpTypePB::INSERT:
        inserts++;
        // Committed transactional rows carry the transaction's commit timestamp.
        EXPECT_TRUE(rec.has_commit_timestamp());
        break;
      default:
        break;
    }
  }
  // Only the committed transaction's 3 rows, wrapped in exactly one BEGIN/COMMIT.
  EXPECT_EQ(3, inserts) << "aborted rows must not be emitted";
  EXPECT_EQ(1, begins);
  EXPECT_EQ(1, commits);
}

// A1 (liveness): a committed transaction whose WAL span exceeds the per-response
// byte cap must still be emitted atomically -- the read window is escalated to
// reach the FINALIZE_COMMIT -- rather than pinning the checkpoint at the
// transaction's first write forever. Without the fix this loop never observes
// the COMMIT and spins until the iteration bound trips.
TEST_F(CDCFailoverITest, LargeTransactionDoesNotWedgeStream) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  // Plenty of headroom to escalate and read the whole transaction.
  FLAGS_cdc_max_transaction_span_bytes = 512 * 1024 * 1024;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  // A transaction with many single-row writes so its span far exceeds the tiny
  // per-response cap set below.
  const int kRows = 40;
  NO_FATALS(WriteCommittedTransaction(/*start=*/1000, kRows));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);

  // Drive GetChanges following the checkpoint with a tiny per-request byte cap,
  // so the transaction's span cannot fit in an un-escalated window.
  int64_t from = 0;
  int inserts = 0;
  int commits = 0;
  int begins = 0;
  bool advanced_past_commit = false;
  const int kMaxCalls = 200;
  int calls = 0;
  for (; calls < kMaxCalls && commits == 0; ++calls) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    req.set_max_bytes(256);  // far smaller than the transaction's WAL span
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& rec : resp.records()) {
      switch (rec.op_type()) {
        case CDCOpTypePB::BEGIN: begins++; break;
        case CDCOpTypePB::INSERT: inserts++; break;
        case CDCOpTypePB::COMMIT: commits++; break;
        default: break;
      }
    }
    const int64_t next = resp.checkpoint_op_index();
    // The checkpoint must make progress; a wedge would leave it pinned.
    ASSERT_GE(next, from);
    if (commits > 0) advanced_past_commit = (next > from);
    from = next;
  }
  ASSERT_LT(calls, kMaxCalls) << "stream wedged: never observed the commit";
  EXPECT_EQ(1, begins);
  EXPECT_EQ(1, commits);
  EXPECT_EQ(kRows, inserts);
  EXPECT_TRUE(advanced_past_commit)
      << "checkpoint did not advance past the emitted transaction";
}

// A1 (loud failure): a transaction whose span exceeds
// --cdc_max_transaction_span_bytes cannot be emitted; GetChanges returns
// TRANSACTION_TOO_LARGE rather than silently stalling.
TEST_F(CDCFailoverITest, TransactionExceedingSpanCapFailsLoudly) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  // Tiny span cap: the transaction cannot fit even after escalation.
  FLAGS_cdc_max_transaction_span_bytes = 64;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  const int kRows = 40;
  NO_FATALS(WriteCommittedTransaction(/*start=*/2000, kRows));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);

  // Follow the checkpoint until the transaction becomes the first op in the read
  // window, at which point the (too-small) span cap forces a loud error.
  int64_t from = 0;
  bool saw_too_large = false;
  for (int calls = 0; calls < 200; ++calls) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    req.set_max_bytes(64);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    if (resp.has_error()) {
      ASSERT_EQ(CDCErrorPB::TRANSACTION_TOO_LARGE, resp.error().code())
          << resp.error().DebugString();
      saw_too_large = true;
      break;
    }
    const int64_t next = resp.checkpoint_op_index();
    if (next <= from) break;  // no progress and no error would be a wedge
    from = next;
  }
  ASSERT_TRUE(saw_too_large)
      << "expected TRANSACTION_TOO_LARGE for a transaction exceeding the span cap";
}

// Two streams with different checkpoints maintain independent anchors.
TEST_F(CDCFailoverITest, MultipleStreams_IndependentCheckpoints) {
  NO_FATALS(StartClusterWithRF3());
  string tablet_id = GetTabletId();

  string stream_id_1, stream_id_2;
  ASSERT_OK(CreateStream(&stream_id_1));
  ASSERT_OK(CreateStream(&stream_id_2));

  NO_FATALS(InsertRows(0, 10));

  // Wait for leader.
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  auto proxy = MakeCDCProxy(leader_idx);

  // Both streams read from the beginning.
  GetChangesRequestPB req;
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);

  req.set_stream_id(stream_id_1);
  GetChangesResponsePB resp1;
  RpcController rpc1;
  rpc1.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp1, &rpc1));
  ASSERT_FALSE(resp1.has_error());
  int64_t cp1 = resp1.checkpoint_op_index();

  req.set_stream_id(stream_id_2);
  GetChangesResponsePB resp2;
  RpcController rpc2;
  rpc2.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp2, &rpc2));
  ASSERT_FALSE(resp2.has_error());

  // Checkpoint stream 1 all the way to end.
  CheckpointRequestPB cp_req;
  CheckpointResponsePB cp_resp;
  cp_req.set_stream_id(stream_id_1);
  cp_req.set_tablet_id(tablet_id);
  cp_req.set_op_index(cp1);
  RpcController rpc3;
  rpc3.set_timeout(kTimeout);
  ASSERT_OK(proxy->Checkpoint(cp_req, &cp_resp, &rpc3));
  ASSERT_FALSE(cp_resp.has_error());

  // Stream 2 can still read from 0 (its anchor is independent).
  GetChangesResponsePB resp3;
  req.set_stream_id(stream_id_2);
  req.set_from_op_index(0);
  RpcController rpc4;
  rpc4.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp3, &rpc4));
  ASSERT_FALSE(resp3.has_error());
  EXPECT_EQ(10, CountInserts(resp3));
}

// Concurrent checkpoints for the same stream are idempotent.
TEST_F(CDCFailoverITest, ConcurrentCheckpoints) {
  NO_FATALS(StartClusterWithRF3());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 5));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  auto proxy = MakeCDCProxy(leader_idx);

  GetChangesRequestPB get_req;
  GetChangesResponsePB get_resp;
  get_req.set_stream_id(stream_id);
  get_req.set_tablet_id(tablet_id);
  get_req.set_from_op_index(0);
  RpcController get_rpc;
  get_rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(get_req, &get_resp, &get_rpc));
  ASSERT_FALSE(get_resp.has_error());
  int64_t cp = get_resp.checkpoint_op_index();

  // Checkpoint twice with the same value — both should succeed.
  for (int i = 0; i < 2; ++i) {
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(cp);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
}

// GetChanges returns empty when already caught up (no error).
TEST_F(CDCFailoverITest, GetChanges_CaughtUp) {
  NO_FATALS(StartClusterWithRF3());
  string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 3));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  auto proxy = MakeCDCProxy(leader_idx);

  // Read all.
  GetChangesRequestPB req;
  GetChangesResponsePB resp1;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);
  RpcController rpc1;
  rpc1.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp1, &rpc1));
  ASSERT_FALSE(resp1.has_error());
  int64_t cp = resp1.checkpoint_op_index();

  // Read from checkpoint — should be empty, no error.
  GetChangesResponsePB resp2;
  req.set_from_op_index(cp);
  RpcController rpc2;
  rpc2.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp2, &rpc2));
  ASSERT_FALSE(resp2.has_error());
  EXPECT_EQ(0, resp2.records_size());
}

// End-to-end FULL record type: INSERT/UPDATE/DELETE carry complete before- and
// after-images reconstructed from the tablet's MVCC storage.
TEST_F(CDCFailoverITest, FullImage_EndToEnd) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::NEVER, &stream_id));

  NO_FATALS(InsertRows(0, 1));   // key=0, val=0
  NO_FATALS(UpdateRow(0, 999));  // val -> 999
  NO_FATALS(DeleteRow(0));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  GetChangesRequestPB req;
  GetChangesResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);
  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  const cdc::CDCRecordPB* ins = nullptr;
  const cdc::CDCRecordPB* upd = nullptr;
  const cdc::CDCRecordPB* del = nullptr;
  for (const auto& r : resp.records()) {
    if (r.op_type() == CDCOpTypePB::INSERT) ins = &r;
    else if (r.op_type() == CDCOpTypePB::UPDATE) upd = &r;
    else if (r.op_type() == CDCOpTypePB::DELETE) del = &r;
  }
  ASSERT_NE(nullptr, ins) << resp.DebugString();
  ASSERT_NE(nullptr, upd) << resp.DebugString();
  ASSERT_NE(nullptr, del) << resp.DebugString();

  // INSERT: no before-image; after-image has val=0.
  EXPECT_EQ(0, ins->old_changes_size());
  int32_t v = -1;
  ASSERT_TRUE(GetInt32(*ins, /*before=*/false, "val", &v));
  EXPECT_EQ(0, v);

  // UPDATE: before-image val=0, full after-image val=999 (key + val).
  ASSERT_TRUE(GetInt32(*upd, /*before=*/true, "val", &v));
  EXPECT_EQ(0, v);
  ASSERT_TRUE(GetInt32(*upd, /*before=*/false, "val", &v));
  EXPECT_EQ(999, v);
  EXPECT_EQ(2, upd->changes_size());
  EXPECT_EQ(2, upd->old_changes_size());

  // DELETE: before-image is the full pre-delete row (val=999).
  ASSERT_TRUE(GetInt32(*del, /*before=*/true, "val", &v));
  EXPECT_EQ(999, v);
  EXPECT_EQ(2, del->old_changes_size());
  EXPECT_EQ(1, del->changes_size());  // primary key only
}

// End-to-end server-driven snapshot: paginate the initial snapshot, then hand
// off to WAL streaming from the captured op-index.
TEST_F(CDCFailoverITest, Snapshot_EndToEnd) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::INITIAL_AND_CONTINUE,
                                   &stream_id));
  NO_FATALS(InsertRows(0, 20));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);

  // Drive the paginated snapshot to completion (tiny max_bytes forces paging).
  int reads = 0;
  int64_t last_key = -1;
  bool done = false;
  bool first = true;
  string resume_key;
  int64_t streaming_start = -1;
  int pages = 0;
  while (!done) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_is_snapshot_start(first);
    if (!resume_key.empty()) req.set_snapshot_resume_key(resume_key);
    req.set_max_bytes(64);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    GetChangesResponsePB resp;
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (const auto& r : resp.records()) {
      ASSERT_EQ(CDCOpTypePB::READ, r.op_type());
      int32_t k = -1;
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &k));
      EXPECT_GT(k, last_key);
      last_key = k;
      reads++;
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    if (done) streaming_start = resp.snapshot_streaming_start_op_index();
    ASSERT_LT(++pages, 1000);
  }
  EXPECT_EQ(20, reads);
  EXPECT_GT(pages, 1);
  ASSERT_GE(streaming_start, 0);

  // Rows written after the snapshot are seen by streaming from the handoff.
  NO_FATALS(InsertRows(1000, 4));
  GetChangesRequestPB req;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(streaming_start);
  RpcController rpc;
  rpc.set_timeout(kTimeout);
  GetChangesResponsePB resp;
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  EXPECT_EQ(4, CountInserts(resp));
}

// Item 1 from CDC/test_coverage_comparison.md (highest-value gap): DML that runs
// concurrently with a server-driven snapshot must be captured exactly once
// across the snapshot -> WAL handoff -- no gap (a lost mutation) and no duplicate
// (a row emitted by both the snapshot and the WAL).
//
// A snapshot fixes a consistent point-in-time (snap_ts) and a WAL streaming-start
// op-index when its first page establishes the session. Rows mutated AFTER that
// establish must be invisible to the snapshot scan (MVCC isolation at snap_ts)
// and instead surface exactly once when the consumer hands off to WAL streaming
// from the captured op-index.
//
// This drives a paginated snapshot, injects inserts/updates/deletes mid-drain
// (the "during snapshot" window), then asserts three independent properties:
//   1. the snapshot READ set is exactly the pre-DML table (isolation holds),
//   2. the WAL stream from the handoff is exactly the concurrent DML, once each,
//   3. snapshot(state) + WAL(replay) reconstructs the live table exactly.
TEST_F(CDCFailoverITest, Snapshot_ConcurrentDmlCapturedExactlyOnce) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::CHANGE,
                                   master::CDCStreamConfigPB::INITIAL_AND_CONTINUE,
                                   &stream_id));

  // Pre-snapshot state: keys [0, 20), val = key * 10.
  NO_FATALS(InsertRows(0, 20));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);

  // A consumer's reconstructed view: filled by the snapshot, then by WAL replay.
  std::map<int32_t, int32_t> reconstructed;
  // key -> val for every row the snapshot READ emitted (dup-detected below).
  std::map<int32_t, int32_t> snapshot_vals;

  // --- Snapshot phase: paginate, injecting DML after the session establishes ---
  int64_t streaming_start = -1;
  string resume_key;
  bool first = true;
  bool done = false;
  bool injected = false;
  int pages = 0;
  while (!done) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_is_snapshot_start(first);
    if (!resume_key.empty()) req.set_snapshot_resume_key(resume_key);
    req.set_max_bytes(64);  // tiny cap forces many pages
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    GetChangesResponsePB resp;
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (const auto& r : resp.records()) {
      ASSERT_EQ(CDCOpTypePB::READ, r.op_type());
      int32_t k = -1;
      int32_t v = -1;
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &k));
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "val", &v));
      ASSERT_EQ(0, snapshot_vals.count(k))
          << "snapshot emitted key " << k << " more than once";
      snapshot_vals[k] = v;
      reconstructed[k] = v;
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    if (done) streaming_start = resp.snapshot_streaming_start_op_index();

    // Inject the concurrent DML exactly once: after the first page has
    // established snap_ts / streaming_start, but before the snapshot finishes
    // draining. This is the racing window the handoff boundary must get right.
    if (!injected && !done) {
      NO_FATALS(InsertRows(20, 10));                  // new keys [20, 30)
      for (int k = 0; k < 5; ++k) {
        NO_FATALS(UpdateRow(k, k * 100));             // update keys 0..4
      }
      for (int k = 15; k < 20; ++k) {
        NO_FATALS(DeleteRow(k));                       // delete keys 15..19
      }
      injected = true;
    }
    ASSERT_LT(++pages, 1000);
  }
  ASSERT_TRUE(injected) << "DML was not injected during the snapshot";
  ASSERT_GT(pages, 1) << "snapshot did not paginate; race window not exercised";
  ASSERT_GE(streaming_start, 0);

  // Property 1: snapshot isolation. The scan reflects the table as of snap_ts --
  // exactly the 20 pre-DML rows with original values. None of the concurrent
  // mutations may leak in: no new keys, no updated vals, deleted rows still seen.
  ASSERT_EQ(20, snapshot_vals.size());
  for (int k = 0; k < 20; ++k) {
    ASSERT_EQ(1, snapshot_vals.count(k)) << "snapshot missing pre-DML key " << k;
    ASSERT_EQ(k * 10, snapshot_vals[k])
        << "snapshot leaked a concurrent mutation on key " << k;
  }

  // --- WAL phase: stream from the handoff; must be exactly the concurrent DML ---
  int wal_inserts = 0;
  int wal_updates = 0;
  int wal_deletes = 0;
  int64_t from = streaming_start;
  int drains = 0;
  while (true) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    GetChangesResponsePB resp;
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    if (resp.records_size() == 0) break;  // caught up
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &k));
      switch (r.op_type()) {
        case CDCOpTypePB::INSERT: {
          int32_t v = -1;
          ASSERT_TRUE(GetInt32(r, /*before=*/false, "val", &v));
          wal_inserts++;
          reconstructed[k] = v;
          break;
        }
        case CDCOpTypePB::UPDATE: {
          int32_t v = -1;
          ASSERT_TRUE(GetInt32(r, /*before=*/false, "val", &v));
          wal_updates++;
          reconstructed[k] = v;
          break;
        }
        case CDCOpTypePB::DELETE:
          wal_deletes++;
          reconstructed.erase(k);
          break;
        default:
          FAIL() << "unexpected WAL op_type " << r.op_type()
                 << " for key " << k;
      }
    }
    const int64_t next = resp.checkpoint_op_index();
    ASSERT_GT(next, from) << "WAL stream did not advance the checkpoint";
    from = next;
    ASSERT_LT(++drains, 1000);
  }

  // Property 2: the WAL stream from the handoff is exactly the concurrent DML,
  // each op once -- proving no pre-snapshot row was replayed (would inflate
  // inserts) and no mutation was dropped (would deflate a count).
  EXPECT_EQ(10, wal_inserts) << "expected the 10 concurrent inserts, once each";
  EXPECT_EQ(5, wal_updates) << "expected the 5 concurrent updates, once each";
  EXPECT_EQ(5, wal_deletes) << "expected the 5 concurrent deletes, once each";

  // Property 3: snapshot + WAL reconstructs the live table exactly (ground truth
  // from a direct scan). This is the end-to-end exactly-once, no-gap guarantee.
  const std::map<int32_t, int32_t> actual = ScanTable();
  ASSERT_EQ(actual, reconstructed)
      << "CDC snapshot+WAL did not reconstruct the live table";
  // Sanity-check the expected shape independently of the scan: keys 0..4 updated,
  // 5..14 original, 15..19 deleted, 20..29 inserted.
  ASSERT_EQ(25, actual.size());
}

// Item 3 from CDC/test_coverage_comparison.md: an ALTER TABLE on an active stream
// must surface end-to-end. A consumer reading across the ALTER boundary must see,
// in WAL order: the pre-ALTER rows decoded with the old schema, then a single DDL
// record carrying the post-ALTER schema, then the post-ALTER rows decoded with
// the new schema (the added column present and correctly typed).
//
// This exercises the full path -- an ALTER_SCHEMA_OP in the WAL decoded into a
// DDL record with new_schema/new_schema_version, and the running-schema-version
// stamping that governs every record on either side of the ALTER -- rather than
// the decode-unit coverage in cdc_util-test.cc (which never streams an ALTER
// alongside real writes over GetChanges).
TEST_F(CDCFailoverITest, AlterTableMidStream_EmitsDdlThenNewSchemaRows) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));  // CHANGE mode.

  // Pre-ALTER: 5 rows on the original (key, val) schema.
  NO_FATALS(InsertRows(0, 5));

  // Online ALTER adds a nullable column, landing an ALTER_SCHEMA_OP in the WAL.
  const string kExtra = "extra";
  NO_FATALS(AddNullableIntColumn(kExtra));

  // Post-ALTER: 5 rows on the new (key, val, extra) schema.
  NO_FATALS(InsertRowsWithExtra(100, 5, kExtra));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);

  // Drain the whole stream from the beginning, preserving WAL order across pages.
  vector<cdc::CDCRecordPB> records;
  int64_t from = 0;
  int drains = 0;
  while (true) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    GetChangesResponsePB resp;
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    if (resp.records_size() == 0) break;  // caught up
    for (const auto& r : resp.records()) records.push_back(r);
    const int64_t next = resp.checkpoint_op_index();
    ASSERT_GT(next, from) << "stream did not advance the checkpoint";
    from = next;
    ASSERT_LT(++drains, 1000);
  }

  // Exactly one DDL record must be emitted, and it partitions the stream into a
  // pre-ALTER and a post-ALTER half.
  int ddl_idx = -1;
  for (int i = 0; i < static_cast<int>(records.size()); ++i) {
    if (records[i].op_type() == CDCOpTypePB::DDL) {
      ASSERT_EQ(-1, ddl_idx) << "more than one DDL record emitted";
      ddl_idx = i;
    }
  }
  ASSERT_GE(ddl_idx, 0) << "no DDL record emitted for the ALTER";

  // The DDL record carries the post-ALTER schema: it includes the added column,
  // and its new_schema_version is exactly one past the version stamped on the
  // pre-ALTER rows (schema_version == version in effect before the ALTER).
  const auto& ddl = records[ddl_idx];
  ASSERT_TRUE(ddl.has_new_schema());
  bool found_extra = false;
  for (const auto& c : ddl.new_schema().columns()) {
    if (c.name() == kExtra) found_extra = true;
  }
  ASSERT_TRUE(found_extra) << "DDL new_schema is missing the added column";
  ASSERT_EQ(ddl.schema_version() + 1, ddl.new_schema_version());

  // Pre-ALTER rows: exactly the 5 inserts, old schema (no 'extra' column),
  // stamped with the pre-ALTER version.
  int pre_inserts = 0;
  for (int i = 0; i < ddl_idx; ++i) {
    const auto& r = records[i];
    if (r.op_type() != CDCOpTypePB::INSERT) continue;
    pre_inserts++;
    int32_t dummy = -1;
    EXPECT_FALSE(GetInt32(r, /*before=*/false, kExtra, &dummy))
        << "pre-ALTER row unexpectedly carries the added column";
    EXPECT_EQ(ddl.schema_version(), r.schema_version());
  }
  EXPECT_EQ(5, pre_inserts);

  // Post-ALTER rows: exactly the 5 inserts, new schema ('extra' present and
  // correctly typed as key * 100), stamped with the new version.
  int post_inserts = 0;
  for (int i = ddl_idx + 1; i < static_cast<int>(records.size()); ++i) {
    const auto& r = records[i];
    if (r.op_type() != CDCOpTypePB::INSERT) continue;
    post_inserts++;
    int32_t key = -1;
    int32_t extra = -1;
    ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &key));
    ASSERT_TRUE(GetInt32(r, /*before=*/false, kExtra, &extra))
        << "post-ALTER row missing the added column";
    EXPECT_EQ(key * 100, extra) << "added column decoded with the wrong value";
    EXPECT_EQ(ddl.new_schema_version(), r.schema_version());
  }
  EXPECT_EQ(5, post_inserts);
}

// For FULL streams, the master fans the MVCC history floor out to every replica,
// so a follower's tablet pins its history (survives a leader change).
TEST_F(CDCFailoverITest, FullMode_HistoryFloorPushedToAllReplicas) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 2));
  NO_FATALS(UpdateRow(0, 5));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  // A FULL-mode read pins the leader's history and records the floor; the
  // Checkpoint then persists that floor to the master.
  auto proxy = MakeCDCProxy(leader_idx);
  int64_t cp = -1;
  {
    GetChangesRequestPB req;
    GetChangesResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(0);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    cp = resp.checkpoint_op_index();
  }
  {
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(cp);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  scoped_refptr<TabletReplica> follower;
  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &follower));

  // The master's maintenance scan pushes the history floor to the follower.
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(follower->shared_tablet()->cdc_history_floor().value(), 0);
  });
}

// Phase 6.2: dropping a range partition prunes the now-gone tablet's entry from
// every stream's checkpoint map (self-healing during the maintenance scan).
TEST_F(CDCFailoverITest, DropRangePartition_CleansCheckpoints) {
  NO_FATALS(StartCluster(3));

  // Build a table with two range partitions: [0, 100) and [100, 200).
  client::KuduSchema schema;
  client::KuduSchemaBuilder b;
  b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
  ASSERT_OK(b.Build(&schema));

  unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
  auto make_bound = [&](int v) {
    unique_ptr<KuduPartialRow> row(schema.NewRow());
    CHECK_OK(row->SetInt32("key", v));
    return row;
  };
  creator->table_name("cdc_drop_partition_table")
      .schema(&schema)
      .set_range_partition_columns({"key"})
      .num_replicas(3);
  creator->add_range_partition(make_bound(0).release(), make_bound(100).release());
  creator->add_range_partition(make_bound(100).release(), make_bound(200).release());
  ASSERT_OK(creator->Create());
  table_name_ = "cdc_drop_partition_table";

  MessengerBuilder bld("CDCDropClient");
  ASSERT_OK(bld.Build(&messenger_));
  master_proxy_.reset(new MasterServiceProxy(
      messenger_, cluster_->mini_master()->bound_rpc_addr(),
      cluster_->mini_master()->bound_rpc_addr().host()));

  // Collect the two data tablets.
  vector<string> tablet_ids;
  ASSERT_EVENTUALLY([&] {
    tablet_ids.clear();
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      vector<scoped_refptr<TabletReplica>> replicas;
      cluster_->mini_tablet_server(i)->server()->tablet_manager()
          ->GetTabletReplicas(&replicas);
      for (const auto& r : replicas) {
        if (r->tablet_metadata()->table_name() == table_name_) {
          if (std::find(tablet_ids.begin(), tablet_ids.end(), r->tablet_id()) ==
              tablet_ids.end()) {
            tablet_ids.push_back(r->tablet_id());
          }
        }
      }
    }
    ASSERT_EQ(2, tablet_ids.size());
  });

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  // Checkpoint both tablets so both appear in the master's checkpoint map.
  for (const auto& tablet_id : tablet_ids) {
    int leader_idx = -1;
    ASSERT_EVENTUALLY([&] {
      leader_idx = FindLeaderIndex(tablet_id);
      ASSERT_GE(leader_idx, 0);
    });
    auto proxy = MakeCDCProxy(leader_idx);
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(1);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // Both tablets are now durably checkpointed on the master.
  auto stream_checkpoint_count = [&]() -> int {
    master::GetCDCStreamInfoRequestPB req;
    master::GetCDCStreamInfoResponsePB resp;
    req.set_stream_id(stream_id);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    CHECK_OK(master_proxy_->GetCDCStreamInfo(req, &resp, &rpc));
    CHECK(!resp.has_error()) << resp.error().DebugString();
    return resp.stream().tablet_checkpoints_size();
  };
  ASSERT_EVENTUALLY([&] { ASSERT_EQ(2, stream_checkpoint_count()); });

  // Drop the [100, 200) range partition.
  unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(table_name_));
  unique_ptr<KuduPartialRow> lb(schema.NewRow());
  unique_ptr<KuduPartialRow> ub(schema.NewRow());
  ASSERT_OK(lb->SetInt32("key", 100));
  ASSERT_OK(ub->SetInt32("key", 200));
  alterer->DropRangePartition(lb.release(), ub.release());
  ASSERT_OK(alterer->Alter());

  // The maintenance scan must prune the dropped tablet's checkpoint entry,
  // leaving exactly the one surviving tablet.
  ASSERT_EVENTUALLY([&] { ASSERT_EQ(1, stream_checkpoint_count()); });
}

// Tier 1 blocker #1 (Prong A): the retention barrier is durable. After the
// master pushes a barrier, every replica must persist it in its own tablet
// superblock so WAL/history retention survives a restart or leader change
// without depending on the master's next maintenance pass.
TEST_F(CDCFailoverITest, RetentionBarrierPersistedToSuperblockOnAllReplicas) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 5));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  const int64_t kCheckpoint = 3;
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, kCheckpoint));

  // The maintenance pass fans the barrier to all three replicas; each persists
  // it. Poll the in-memory metadata (which mirrors what was flushed) until every
  // replica carries a durable (>= 0) barrier at or below the checkpoint.
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      const int64_t idx = r->tablet_metadata()->cdc_min_retained_op_index();
      ASSERT_GE(idx, 0) << "replica " << r->permanent_uuid()
                        << " has no persisted CDC barrier";
      ASSERT_LE(idx, kCheckpoint);
    }
  });
}

// Tier 1 blocker #1 (Prong A restore): after a tserver restart, a CDC-enabled
// tablet must honor the last durably-known barrier immediately -- restored from
// the superblock at bootstrap -- with no dependence on the master's next pass.
// Verified both in the reloaded metadata and in the WAL retention indexes the
// replica hands to Log GC.
TEST_F(CDCFailoverITest, RetentionBarrierRestoredAfterTserverRestart) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 5));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  const int64_t kCheckpoint = 2;
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, kCheckpoint));

  // Restart a follower so this is a pure restart, not a leader change. Wait
  // until it has persisted the barrier before restarting.
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  ASSERT_EVENTUALLY([&] {
    scoped_refptr<TabletReplica> r;
    ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &r));
    ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0);
  });

  // Suppress further master pushes: after this, the only way the barrier can be
  // present on the restarted node is a restore from its persisted superblock.
  FLAGS_cdc_bg_scan_interval_ms = 1000000;

  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->Restart());

  scoped_refptr<TabletReplica> r;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &r));
    ASSERT_OK(r->CheckRunning());
  });

  // The persisted barrier is restored into metadata at bootstrap...
  const int64_t restored = r->tablet_metadata()->cdc_min_retained_op_index();
  ASSERT_GE(restored, 0);
  ASSERT_LE(restored, kCheckpoint);
  // ...and folded into the WAL retention indexes on the very first GC pass, so
  // Log GC cannot reclaim segments the consumer still needs.
  ASSERT_LE(r->GetRetentionIndexes().for_durability, restored);
}

// Tier 1 blocker #1 (Prong A release durability): deleting a stream releases the
// barrier on every replica, clearing the persisted value back to the -1 sentinel
// so a later restart cannot resurrect stale retention and pin the WAL forever.
TEST_F(CDCFailoverITest, RetentionBarrierClearedFromSuperblockOnStreamDelete) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, 1));

  // Barrier persisted on all replicas.
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0);
    }
  });

  ASSERT_OK(DeleteStream(stream_id));

  // The release fans out to all replicas, each clearing its persisted barrier.
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_EQ(-1, r->tablet_metadata()->cdc_min_retained_op_index())
          << "replica " << r->permanent_uuid()
          << " still pins a released CDC barrier";
    }
  });
}

// Tier 1 blocker #1 (Prong B): the hard --cdc_wal_retention_secs floor is wired
// into the WAL retention indexes exactly while a CDC stream pins the tablet, and
// is off otherwise. This is the coordination-loop-independent backstop that keeps
// recently-closed WAL segments regardless of consumer progress.
TEST_F(CDCFailoverITest, CDCWalRetentionSecsFloorWiredWhileStreamActive) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  scoped_refptr<TabletReplica> leader;
  ASSERT_OK(cluster_->mini_tablet_server(leader_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &leader));

  // A generous retention window so the deadline computation is unambiguous.
  FLAGS_cdc_wal_retention_secs = 8 * 3600;

  // No barrier yet -> the time floor must be inactive (a tablet with no CDC
  // stream pinning it gets normal GC).
  ASSERT_EQ(0, leader->GetRetentionIndexes().cdc_wal_retention_deadline_micros);

  // Persist a checkpoint so the master installs the barrier on the leader.
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, 1));
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(leader->tablet_metadata()->cdc_min_retained_op_index(), 0);
  });

  // With a barrier present and the flag > 0, a positive wall-clock deadline is
  // handed to Log GC.
  ASSERT_GT(leader->GetRetentionIndexes().cdc_wal_retention_deadline_micros, 0);

  // Disabling the flag turns the floor off even while the stream is active.
  FLAGS_cdc_wal_retention_secs = 0;
  ASSERT_EQ(0, leader->GetRetentionIndexes().cdc_wal_retention_deadline_micros);
}

// Tier 1 blocker #1 (Prong A across a leader change): the durable retention
// barrier must survive an actual leader *kill*, not just a graceful restart. A
// former follower that is elected leader after the old leader is killed must
// honor the persisted barrier on its very first GC decision -- from its own
// superblock, with no dependence on the master's next maintenance pass.
//
// This is the companion to RetentionBarrierRestoredAfterTserverRestart (which
// covers a pure restart): here the node is never restarted, so what is proven is
// that the persisted value the follower already held is picked up by
// GetRetentionIndexes() the moment it becomes leader, even with the master
// suppressed.
TEST_F(CDCFailoverITest, RetentionBarrierRetainedByNewLeaderAfterKill) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 5));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  const int64_t kCheckpoint = 2;
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, kCheckpoint));

  // Wait until every replica -- crucially including the followers, one of which
  // will become the next leader -- has persisted the barrier in its superblock.
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0)
          << "replica " << r->permanent_uuid() << " has not persisted the barrier";
    }
  });

  // Suppress further master pushes: after this point the only source of the
  // barrier on the new leader is the value it already persisted to disk.
  FLAGS_cdc_bg_scan_interval_ms = 1000000;

  // Kill the leader and force a leader change.
  cluster_->mini_tablet_server(leader_idx)->Shutdown();

  int new_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    new_leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(new_leader_idx, 0);
    ASSERT_NE(new_leader_idx, leader_idx);
  });

  scoped_refptr<TabletReplica> new_leader;
  ASSERT_OK(cluster_->mini_tablet_server(new_leader_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &new_leader));

  // The new leader honors the persisted barrier immediately: it is still in the
  // reloaded metadata (never lost, since this node was not restarted)...
  const int64_t retained = new_leader->tablet_metadata()->cdc_min_retained_op_index();
  ASSERT_GE(retained, 0);
  ASSERT_LE(retained, kCheckpoint);
  // ...and it is folded into the WAL retention indexes the new leader hands to
  // Log GC, so a GC on the freshly-elected leader cannot reclaim segments the
  // consumer still needs -- without waiting on the (suppressed) master pass.
  ASSERT_LE(new_leader->GetRetentionIndexes().for_durability, retained);
}

// Tier 1 blocker #1 (end-to-end invariant): a consumer reading a CHANGE stream
// must observe every mutation exactly once -- no gap, no duplicate -- across a
// leader failover that happens while unconsumed WAL is outstanding, even when Log
// GC actually reclaims segments on the newly-elected leader, and even when the
// *only* thing holding the WAL is the durably persisted retention barrier. This
// is the integration-level proof that guarantee #1 holds through a real failover
// + GC, with the persisted superblock barrier shown to be load-bearing rather
// than shadowed by a redundant in-memory anchor.
//
// Shape: consume a prefix and checkpoint at it (leaving unconsumed WAL beyond the
// checkpoint), wait for the barrier to persist on all replicas, write more, then
// kill the leader (the failover). Restart the surviving replicas so their
// in-memory retention anchors are dropped -- see below -- then, on the new
// leader, flush (making the consumed prefix GC-eligible) and run Log GC. The test
// proves GC is non-vacuous -- the new leader's WAL segment count strictly drops
// -- yet the consumer, resuming from the checkpoint, still observes every
// mutation exactly once. If GC had reclaimed past the checkpoint, the resume
// would return WAL_EXPIRED and fail here.
//
// THREE retention sources that would otherwise mask the persisted barrier are
// removed so it (folded into for_durability) is the *sole* thing bounding GC --
// making it provably load-bearing (verified: neutering the fold makes this test
// fail with WAL_EXPIRED):
//   1. The in-memory LogAnchorRegistry anchor. The master's barrier push both
//      persists the superblock field AND registers this anchor on every replica,
//      so a killed leader's successor still holds it -- it, not the persisted
//      value, would hold the WAL. Restarting the survivors (with the bg scan
//      frozen and no GetChanges since the kill) drops the anchor and forces the
//      barrier to be restored from the durable superblock alone.
//   2. The hard time-based WAL floor (--cdc_wal_retention_secs), disabled.
//   3. The killed peer's now-futile catch-up retention (for_peers pinned at 0),
//      defeated by capping --log_max_segments_to_retain low.
// With all three gone, GC reclaims exactly up to the barrier and no further.
TEST_F(CDCFailoverITest, NoGapNoDupAcrossFailoverWithGc) {
  // Tiny segments so writes roll many WAL segments, giving Log GC real,
  // segment-granular work to do rather than a single un-GC-able segment. Must be
  // set before the cluster starts so every tserver's Log honors it.
  FLAGS_log_segment_size_bytes_for_tests = 1024;
  // Disable the hard time floor: it would retain every recently-closed segment
  // regardless of the barrier and mask what this test exercises.
  FLAGS_cdc_wal_retention_secs = 0;
  // Cap segment retention low so a killed peer's catch-up retention (for_peers=0)
  // does not blanket-retain the WAL; this leaves the CDC barrier as the sole GC
  // floor. The surviving replicas are caught up and do not need the old segments.
  FLAGS_log_max_segments_to_retain = 3;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));  // CHANGE mode.

  const int kPhase1 = 80;
  const int kPhase2 = 40;
  const int kPhase3 = 40;
  const int kTotal = kPhase1 + kPhase2 + kPhase3;

  // Phase 1: pre-failover writes, keys [0, kPhase1).
  NO_FATALS(InsertRows(0, kPhase1));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  // Consumer's running tally of every INSERT it observes, keyed by row key. The
  // no-gap/no-dup invariant is: at the end, exactly keys [0, kTotal) each once.
  std::map<int32_t, int> seen;
  auto proxy = MakeCDCProxy(leader_idx);

  // Consume roughly half of phase 1 in small pages, advancing the checkpoint as
  // we go, then stop. This leaves several *whole* WAL segments below the
  // checkpoint (so GC has segments to reclaim) while the tail of phase 1 -- plus
  // everything written later -- stays unconsumed beyond the checkpoint, which is
  // exactly the WAL that must survive GC.
  int64_t checkpoint = 0;
  {
    int pages = 0;
    while (static_cast<int>(seen.size()) < kPhase1 / 2) {
      GetChangesRequestPB req;
      req.set_stream_id(stream_id);
      req.set_tablet_id(tablet_id);
      req.set_from_op_index(checkpoint);
      req.set_max_bytes(512);
      GetChangesResponsePB resp;
      RpcController rpc;
      rpc.set_timeout(kTimeout);
      ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
      ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
      for (const auto& r : resp.records()) {
        if (r.op_type() != CDCOpTypePB::INSERT) continue;
        int32_t k = -1;
        ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &k));
        seen[k]++;
      }
      const int64_t next = resp.checkpoint_op_index();
      ASSERT_GT(next, checkpoint) << "prefix read did not advance the checkpoint";
      checkpoint = next;
      ASSERT_LT(++pages, 1000);
    }
  }
  ASSERT_LT(static_cast<int>(seen.size()), kPhase1)
      << "phase-1 read was not a strict prefix; race window not exercised";

  // Persist the checkpoint so the master installs the barrier, then wait until
  // every replica (including the future new leader) has it durably. Only then is
  // the barrier guaranteed to survive the kill on whichever follower wins.
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, checkpoint));
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      const int64_t idx = r->tablet_metadata()->cdc_min_retained_op_index();
      ASSERT_GE(idx, 0) << "replica " << r->permanent_uuid()
                        << " has not persisted the barrier";
      ASSERT_LE(idx, checkpoint);
    }
  });

  // Isolate the durability guarantee under test. A replica has two CDC WAL-floor
  // sources: the durable superblock barrier (honored on the first GC after a
  // leader change) and an in-memory LogAnchorRegistry anchor. The latter is
  // (re-)established on a fresh leader only by the master's bg-scan re-push
  // (UpdateCDCRetentionBarrier) or by a GetChanges served on that leader. If we
  // let either happen before the post-failover GC, the anchor would hold the WAL
  // and this test would pass even if the persisted barrier were ignored (it did,
  // until this line was added). Freezing the bg scan now -- after the barrier is
  // durable everywhere, and before any GetChanges hits the new leader (the resume
  // read below happens only after GC) -- leaves the persisted superblock barrier
  // as the sole thing standing between the checkpoint and the GC.
  FLAGS_cdc_bg_scan_interval_ms = 1000000;

  // Phase 2: more writes, keys [kPhase1, kPhase1+kPhase2). All beyond the
  // checkpoint and unconsumed -- this is the WAL that must not be GC'd away.
  NO_FATALS(InsertRows(kPhase1, kPhase2));

  // Kill the leader; a former follower (already holding the persisted barrier)
  // must take over. This is the failover.
  cluster_->mini_tablet_server(leader_idx)->Shutdown();
  ASSERT_EVENTUALLY([&] {
    const int idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(idx, 0);
    ASSERT_NE(idx, leader_idx);
  });

  // Wipe the in-memory retention anchor from the surviving replicas. The master
  // pushed the barrier to all three replicas earlier, and that push both persists
  // the superblock field AND registers an in-memory LogAnchorRegistry anchor; the
  // anchor lives in the replica's process and would survive the leader kill on
  // whichever follower took over. If we left it in place it -- not the persisted
  // barrier -- could be what holds the WAL, and this test would still pass even if
  // the superblock barrier were ignored. Restarting each survivor drops its anchor
  // (a fresh process re-bootstraps with an empty registry); the bg scan is frozen
  // and no GetChanges has hit these nodes since the kill, so nothing re-registers
  // it before the GC below. The barrier is restored into metadata from the durable
  // superblock at bootstrap -- so after these restarts the persisted superblock
  // barrier is the ONLY thing that can hold the pre-checkpoint WAL. Restart one at
  // a time to keep a quorum recoverable. This is exactly the durability #1
  // guarantees: retention survives without the master's coordination loop.
  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    if (i == leader_idx) continue;  // the killed node stays down
    ASSERT_OK(cluster_->mini_tablet_server(i)->Restart());
    ASSERT_EVENTUALLY([&] {
      scoped_refptr<TabletReplica> r;
      ASSERT_OK(cluster_->mini_tablet_server(i)->server()->tablet_manager()
          ->GetTabletReplica(tablet_id, &r));
      ASSERT_OK(r->CheckRunning());
    });
  }

  // A leader must re-emerge among the (now anchor-free) survivors.
  int new_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    new_leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(new_leader_idx, 0);
    ASSERT_NE(new_leader_idx, leader_idx);
  });

  scoped_refptr<TabletReplica> new_leader;
  ASSERT_OK(cluster_->mini_tablet_server(new_leader_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &new_leader));

  // Force flush + Log GC on every surviving replica. Flush advances the
  // durability index (well past the checkpoint) so that, absent a retention
  // floor, GC would reclaim the consumed prefix AND the unconsumed WAL the
  // consumer still needs. The only floor left is the persisted superblock
  // barrier: it must hold GC at the checkpoint. Capture the new leader's segment
  // count around its GC to prove reclamation actually happened (the test would
  // pass vacuously if GC had nothing to do).
  const int segments_before = new_leader->log()->reader()->num_segments();
  for (auto& r : GetAllReplicas(tablet_id)) {
    ASSERT_OK(r->tablet()->Flush());
    r->RunLogGC();
  }
  const int segments_after = new_leader->log()->reader()->num_segments();
  ASSERT_LT(segments_after, segments_before)
      << "Log GC reclaimed no segments; the with-GC path was not exercised";

  // Phase 3: writes on the new leader, keys [kPhase1+kPhase2, kTotal). Proves the
  // stream keeps flowing after the failover, not just that history was preserved.
  auto new_proxy = MakeCDCProxy(new_leader_idx);
  NO_FATALS(InsertRows(kPhase1 + kPhase2, kPhase3));

  // Resume from the checkpoint against the new leader and drain to the end. Every
  // record here is at or after the checkpoint, so it cannot overlap the phase-1
  // prefix already tallied -- union is disjoint by construction.
  int64_t from = checkpoint;
  int drains = 0;
  while (true) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(new_proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error())
        << "resume after failover hit an error (barrier failed to retain WAL?): "
        << resp.error().DebugString();
    if (resp.records_size() == 0) break;  // caught up
    for (const auto& r : resp.records()) {
      if (r.op_type() != CDCOpTypePB::INSERT) continue;
      int32_t k = -1;
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &k));
      seen[k]++;
    }
    const int64_t next = resp.checkpoint_op_index();
    ASSERT_GE(next, from) << "checkpoint went backwards across the resume";
    if (next == from) break;  // no further progress
    from = next;
    ASSERT_LT(++drains, 1000);
  }

  // The invariant: exactly keys [0, kTotal), each observed exactly once. A gap
  // (GC reclaimed needed WAL) would drop a key; a duplicate (prefix replayed by
  // the resume) would push a count to 2.
  ASSERT_EQ(kTotal, static_cast<int>(seen.size()))
      << "expected every key exactly once across the failover";
  for (int k = 0; k < kTotal; ++k) {
    ASSERT_EQ(1, seen[k]) << "key " << k << " seen " << seen[k]
                          << " times (0 = gap, >1 = duplicate)";
  }
}

} // namespace kudu
