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
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
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
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
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
#include "kudu/server/monitored_task.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/tablet/txn_participant-test-util.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_bool(cdc_enabled);
DECLARE_bool(cdc_inject_commit_lookup_deadline_expired);
DECLARE_bool(enable_tablet_copy);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_dropped_tablet_retention_secs);
DECLARE_int64(cdc_snapshot_cursor_persist_interval_ms);
DECLARE_int32(cdc_wal_retention_secs);
DECLARE_int32(log_segment_size_bytes_for_tests);
DECLARE_int32(log_max_segments_to_retain);
DECLARE_int64(cdc_stream_expiry_ms);
DECLARE_int64(cdc_max_staleness_ms);
DECLARE_int32(cdc_max_barrier_releases_per_run);
DECLARE_int64(cdc_max_transaction_span_bytes);
DECLARE_int64(cdc_inject_decode_failure_at_op_index);
DECLARE_int64(cdc_active_time_report_interval_ms);
DECLARE_int64(cdc_checkpoint_persist_interval_ms);
DECLARE_int32(unresponsive_ts_rpc_timeout_ms);
DECLARE_bool(enable_txn_system_client_init);
DECLARE_bool(txn_manager_enabled);
DECLARE_bool(txn_manager_lazily_initialized);
DECLARE_uint32(txn_keepalive_interval_ms);
DECLARE_uint32(txn_staleness_tracker_interval_ms);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_bool(tserver_support_cdc);
DECLARE_int32(cdc_barrier_refresh_interval_ms);
DECLARE_int32(cdc_barrier_reorder_window_secs);
DECLARE_int32(catalog_manager_cdc_maintenance_wait_tablet_deleted_ms);
DECLARE_int32(catalog_manager_inject_latency_drop_partition_ms);

METRIC_DECLARE_counter(cdc_maintenance_runs);
METRIC_DECLARE_counter(cdc_wal_read_escalations);

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
using strings::Substitute;

namespace kudu {

class CDCFailoverITest : public MiniClusterITestBase {
 protected:
  static const MonoDelta kTimeout;

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_cdc_enabled = true;
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
    // Reset every CDC tuning flag that individual tests mutate back to its
    // production default. gflags are process-global and gtest does not restore
    // them between tests, so without this a test that (say) sets
    // cdc_stream_expiry_ms=1 would leave every subsequent test expiring streams
    // in 1ms -- making the suite pass in isolation but fail depending on run
    // order. Establishing a known baseline here keeps each test hermetic.
    FLAGS_cdc_stream_expiry_ms = 8 * 3600 * 1000;      // 8 hours
    FLAGS_cdc_max_staleness_ms = 4 * 3600 * 1000;      // 4 hours
    FLAGS_cdc_max_barrier_releases_per_run = 1000;
    FLAGS_cdc_wal_retention_secs = 3600;               // 1 hour
    FLAGS_cdc_max_transaction_span_bytes = 536870912;  // 512 MiB
    FLAGS_cdc_snapshot_cursor_persist_interval_ms = 10 * 1000;  // 10 s
    FLAGS_cdc_inject_decode_failure_at_op_index = -1;
    FLAGS_cdc_dropped_tablet_retention_secs = 24 * 3600;  // 1 day
    FLAGS_catalog_manager_inject_latency_drop_partition_ms = 0;
    FLAGS_catalog_manager_cdc_maintenance_wait_tablet_deleted_ms = 0;
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

  // CreateCDCStream identifies tables by their catalog table id, not their name.
  // The cluster helper only knows the table by name, so resolve the id here.
  Status ResolveTableId(string* table_id) {
    client::sp::shared_ptr<client::KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(table_name_, &table));
    *table_id = table->id();
    return Status::OK();
  }

  Status CreateStream(string* stream_id) {
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    string table_id;
    RETURN_NOT_OK(ResolveTableId(&table_id));
    req.add_table_ids(table_id);
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
    string table_id;
    RETURN_NOT_OK(ResolveTableId(&table_id));
    req.add_table_ids(table_id);
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
                      const string& tablet_id, int64_t op_index,
                      bool drop_applied = false) {
    auto proxy = MakeCDCProxy(ts_idx);
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);
    if (drop_applied) {
      req.set_drop_applied(true);
    }
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

  // Whether the CDC service holds a WAL anchor on 'replica'. It anchors the WAL
  // only for the duration of a read: the master's retention barrier, persisted
  // in the tablet metadata, is the only long-lived CDC retention.
  static bool HasCDCLogAnchor(const scoped_refptr<TabletReplica>& replica) {
    return replica->log_anchor_registry()->DumpAnchorInfo().find("owner=CDC") !=
           string::npos;
  }

  scoped_refptr<TabletReplica> GetReplica(int ts_idx, const string& tablet_id) {
    scoped_refptr<TabletReplica> replica;
    CHECK_OK(cluster_->mini_tablet_server(ts_idx)->server()->tablet_manager()
                 ->GetTabletReplica(tablet_id, &replica));
    return replica;
  }

  // Polls 'tablet_id' of 'stream_id' from 'from_op_index' on tserver 'ts_idx'.
  Status DoGetChanges(int ts_idx, const string& stream_id, const string& tablet_id,
                      int64_t from_op_index, GetChangesResponsePB* resp) {
    auto proxy = MakeCDCProxy(ts_idx);
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from_op_index);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    return proxy->GetChanges(req, resp, &rpc);
  }

  // Streams 'tablet_id' of 'stream_id' on tserver 'ts_idx' after
  // 'from_op_index' until it is caught up, and appends the keys of the INSERTs
  // to 'keys'.
  void ReadInsertKeys(int ts_idx, const string& stream_id, const string& tablet_id,
                      int64_t from_op_index, vector<int32_t>* keys,
                      int64_t* checkpoint = nullptr) {
    for (int i = 0; i < 1000; ++i) {
      GetChangesResponsePB resp;
      ASSERT_OK(DoGetChanges(ts_idx, stream_id, tablet_id, from_op_index, &resp));
      ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
      for (const auto& r : resp.records()) {
        int32_t k = -1;
        if (r.op_type() == CDCOpTypePB::INSERT && GetInt32(r, /*before=*/false, "key", &k)) {
          keys->push_back(k);
        }
      }
      if (resp.checkpoint_op_index() <= from_op_index && !resp.have_more_records()) {
        if (checkpoint) {
          *checkpoint = from_op_index;
        }
        return;
      }
      from_op_index = std::max(from_op_index, resp.checkpoint_op_index());
    }
    FAIL() << "the stream did not catch up";
  }

  // Replaces the replica of 'tablet_id' on tserver 'dst_idx' by a copy from
  // the leader on tserver 'leader_idx' (as the leader does for a follower that
  // fell behind its WAL), and waits until the new replica is running.
  void CopyTabletFromLeader(int leader_idx, int dst_idx, const string& tablet_id) {
    auto* leader_ts = cluster_->mini_tablet_server(leader_idx);
    const int64_t term =
        GetReplica(leader_idx, tablet_id)->shared_consensus()->CurrentTerm();
    ASSERT_OK(itest::StartTabletCopy(ts_map_[cluster_->mini_tablet_server(dst_idx)->uuid()],
                                     tablet_id, leader_ts->uuid(),
                                     HostPort(leader_ts->bound_rpc_addr()), term, kTimeout));
    ASSERT_EVENTUALLY([&] {
      scoped_refptr<TabletReplica> replica = GetReplica(dst_idx, tablet_id);
      ASSERT_EQ(tablet::RUNNING, replica->state());
      ASSERT_EQ(tablet::TABLET_DATA_READY, replica->tablet_metadata()->tablet_data_state());
    });
  }

  // Waits until the leader master has completed 'n' more CDC maintenance
  // passes.
  // Creates the table 'name', with RF 3 and the range partitions
  // [lower, lower + 100) for each of 'lowers', and makes it table_name_.
  void CreateRangeTable(const string& name, const vector<int>& lowers) {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(b.Build(&schema));
    table_name_ = name;
    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name(table_name_).schema(&schema).set_range_partition_columns({"key"})
        .num_replicas(3);
    for (int lower : lowers) {
      unique_ptr<KuduPartialRow> lb(schema.NewRow());
      unique_ptr<KuduPartialRow> ub(schema.NewRow());
      ASSERT_OK(lb->SetInt32("key", lower));
      ASSERT_OK(ub->SetInt32("key", lower + 100));
      creator->add_range_partition(lb.release(), ub.release());
    }
    ASSERT_OK(creator->Create());
    MessengerBuilder bld("CDCRangeTableClient");
    ASSERT_OK(bld.Build(&messenger_));
    master_proxy_.reset(new MasterServiceProxy(
        messenger_, cluster_->mini_master()->bound_rpc_addr(),
        cluster_->mini_master()->bound_rpc_addr().host()));
  }

  // The tablet of the range partition [lower, lower + 100) of table_name_, as
  // the first tablet server knows it, or "" if it has none.
  string TabletOfRange(int lower) {
    const string range = Substitute("$0 <= VALUES < $1", lower, lower + 100);
    vector<scoped_refptr<TabletReplica>> replicas;
    cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletReplicas(&replicas);
    for (const auto& r : replicas) {
      const auto& meta = r->tablet_metadata();
      if (meta->table_name() == table_name_ &&
          meta->partition_schema().PartitionDebugString(meta->partition(), *meta->schema())
              .find(range) != string::npos) {
        return r->tablet_id();
      }
    }
    return string();
  }

  // Drops the range partition [lower, lower + 100) of table_name_.
  Status DropRange(int lower) {
    client::sp::shared_ptr<client::KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(table_name_, &table));
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(table_name_));
    unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
    unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
    RETURN_NOT_OK(lb->SetInt32("key", lower));
    RETURN_NOT_OK(ub->SetInt32("key", lower + 100));
    alterer->DropRangePartition(lb.release(), ub.release());
    return alterer->Alter();
  }

  Status GetStreamInfo(const string& stream_id, master::CDCStreamInfoPB* info) {
    master::GetCDCStreamInfoRequestPB req;
    master::GetCDCStreamInfoResponsePB resp;
    req.set_stream_id(stream_id);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    RETURN_NOT_OK(master_proxy_->GetCDCStreamInfo(req, &resp, &rpc));
    if (resp.has_error()) return StatusFromPB(resp.error().status());
    *info = resp.stream();
    return Status::OK();
  }

  static const master::CDCStreamDroppedTabletPB* FindDropped(
      const master::CDCStreamInfoPB& info, const string& tablet_id) {
    for (const auto& d : info.dropped_tablets()) {
      if (d.tablet_id() == tablet_id) return &d;
    }
    return nullptr;
  }

  void WaitForMaintenancePasses(int n) {
    scoped_refptr<Counter> runs = METRIC_cdc_maintenance_runs.Instantiate(
        cluster_->mini_master()->master()->metric_entity());
    const int64_t target = runs->value() + n;
    ASSERT_EVENTUALLY([&] {
      ASSERT_GE(runs->value(), target);
    });
  }

  string table_name_;
  shared_ptr<Messenger> messenger_;
  unique_ptr<MasterServiceProxy> master_proxy_;
};

const MonoDelta CDCFailoverITest::kTimeout = MonoDelta::FromSeconds(30);

// GetChanges on a non-leader should return TABLET_NOT_LEADER, without asking
// the master for the stream's config first.
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

  auto* cdc = cluster_->mini_tablet_server(follower_idx)->server()->cdc_service();
  const int64_t fetches_before = cdc->StreamConfigMasterFetchesForTests();
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
  EXPECT_EQ(fetches_before, cdc->StreamConfigMasterFetchesForTests());
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

  // Kill the leader while rows are written.
  std::atomic<bool> stop(false);
  // 'acked' is read only once the writer is joined.
  vector<int32_t> acked;
  std::atomic<size_t> num_acked(0);
  std::thread writer([&] {
    client::sp::shared_ptr<client::KuduTable> table;
    CHECK_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    CHECK_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int32_t key = 100; !stop; key++) {
      client::KuduInsert* insert = table->NewInsert();
      CHECK_OK(insert->mutable_row()->SetInt32("key", key));
      CHECK_OK(insert->mutable_row()->SetInt32("val", key * 10));
      if (session->Apply(insert).ok()) {
        acked.push_back(key);
        num_acked++;
      }
    }
  });
  auto stop_writer = MakeScopedCleanup([&] {
    stop = true;
    writer.join();
  });
  SleepFor(MonoDelta::FromMilliseconds(200));
  cluster_->mini_tablet_server(leader_idx)->Shutdown();

  // Wait for a new leader to emerge.
  int new_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    new_leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(new_leader_idx, 0);
    ASSERT_NE(new_leader_idx, leader_idx);
  });
  // And let rows be written through it.
  const size_t acked_at_failover = [&] {
    SleepFor(MonoDelta::FromMilliseconds(200));
    return num_acked.load();
  }();
  stop_writer.run();
  ASSERT_GT(acked.size(), acked_at_failover);

  // The new leader, read from the old checkpoint, has every row that was
  // written after it, once. A row whose write was not acknowledged (it timed
  // out while the leader died) may be there too.
  vector<int32_t> keys;
  NO_FATALS(ReadInsertKeys(new_leader_idx, stream_id, tablet_id, checkpoint, &keys));
  std::sort(keys.begin(), keys.end());
  EXPECT_TRUE(std::adjacent_find(keys.begin(), keys.end()) == keys.end())
      << "a row was streamed twice";
  ASSERT_FALSE(keys.empty());
  EXPECT_GE(keys.front(), 100) << "a row from before the checkpoint was streamed";
  for (int32_t key : acked) {
    EXPECT_TRUE(std::binary_search(keys.begin(), keys.end(), key)) << "row " << key << " lost";
  }
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
// tablet is valid and the request goes to its leader; only the stream is
// unknown, so the stream-existence check (which precedes the WAL read) is what
// must reject the request.
TEST_F(CDCFailoverITest, GetChanges_UnknownStreamReturnsStreamNotFound) {
  NO_FATALS(StartClusterWithRF3());

  // A real stream/tablet exists, but we query a bogus stream_id.
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  const string tablet_id = GetTabletId();

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
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
// leader change and independent follower log GC.
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

  // Even before any checkpoint, CreateCDCStream's creation-time eager barrier
  // push (min_retained_op_index=0 to every replica) sets a retain-all barrier
  // at index 0 on the follower. Wait for it to land: this alone proves the
  // barrier reaches non-leader replicas.
  ASSERT_EVENTUALLY([&] {
    const int64_t follower_barrier =
        follower->tablet_metadata()->cdc_min_retained_op_index();
    ASSERT_EQ(0, follower_barrier)
        << "eager creation-time barrier did not reach the follower";
  });

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

  // The master's maintenance scan must push the advanced barrier to the follower:
  // step 3 sets the barrier to the tablet's min durable checkpoint op_index (1),
  // so the follower's barrier advances from the eager 0 to 1. This proves the
  // barrier both reaches non-leader replicas and tracks the consumer checkpoint.
  ASSERT_EVENTUALLY([&] {
    const int64_t follower_barrier =
        follower->tablet_metadata()->cdc_min_retained_op_index();
    ASSERT_EQ(1, follower_barrier);
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

  // The consumer reads, then persists a checkpoint so the master sets the
  // barrier on all replicas.
  NO_FATALS(InsertRows(0, 3));
  GetChangesResponsePB gc_resp;
  ASSERT_OK(DoGetChanges(leader_idx, stream_id, tablet_id, 0, &gc_resp));
  ASSERT_FALSE(gc_resp.has_error()) << gc_resp.error().DebugString();
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, 1));

  // Barrier reaches the follower.
  ASSERT_EVENTUALLY([&] {
    const int64_t follower_barrier =
        follower->tablet_metadata()->cdc_min_retained_op_index();
    ASSERT_GE(follower_barrier, 0);
    ASSERT_LE(follower_barrier, 1);
  });

  // Force the stream to appear idle; the next maintenance pass must release the
  // barrier on all replicas.
  FLAGS_cdc_stream_expiry_ms = 1;
  scoped_refptr<TabletReplica> leader = GetReplica(leader_idx, tablet_id);
  ASSERT_EVENTUALLY([&] {
    for (const auto& replica : {follower, leader}) {
      const int64_t barrier = replica->tablet_metadata()->cdc_min_retained_op_index();
      ASSERT_EQ(-1, barrier) << "retention barrier still set at index " << barrier;
    }
  });
  // Nothing else keeps the leader's WAL for the expired stream.
  ASSERT_FALSE(HasCDCLogAnchor(leader)) << leader->log_anchor_registry()->DumpAnchorInfo();
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
    const int64_t follower_barrier =
        follower->tablet_metadata()->cdc_min_retained_op_index();
    ASSERT_GE(follower_barrier, 0);
    ASSERT_LE(follower_barrier, 1);
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
    const int64_t follower_barrier =
        follower->tablet_metadata()->cdc_min_retained_op_index();
    ASSERT_EQ(-1, follower_barrier)
        << "retention barrier still set at index " << follower_barrier;
  });
  // The polling consumer does not keep the leader's WAL either.
  scoped_refptr<TabletReplica> leader = GetReplica(leader_idx, tablet_id);
  ASSERT_EQ(-1, leader->tablet_metadata()->cdc_min_retained_op_index());
  ASSERT_FALSE(HasCDCLogAnchor(leader)) << leader->log_anchor_registry()->DumpAnchorInfo();
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
  // retention barrier. Only then does a subsequent expiry give the maintenance
  // pass three barriers to release.
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
    const int64_t follower_barrier =
        follower->tablet_metadata()->cdc_min_retained_op_index();
    ASSERT_GE(follower_barrier, 0);
    ASSERT_LE(follower_barrier, 1);
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
    const int64_t follower_barrier =
        follower->tablet_metadata()->cdc_min_retained_op_index();
    ASSERT_EQ(-1, follower_barrier)
        << "retention barrier still set at index " << follower_barrier;
  });
}

// A4: deleting a stream while its consumer keeps polling the leader leaves
// nothing behind on the leader: DeleteCDCStream fans a session release out to
// the replicas, the consumer's later polls of the deleted stream are refused
// without re-creating its session, and at no point does the CDC service hold
// a WAL anchor that would keep the leader's WAL past the released barrier.
TEST_F(CDCFailoverITest, DeleteStream_WithActivePollerLeavesNoAnchor) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 3));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  scoped_refptr<TabletReplica> leader = GetReplica(leader_idx, tablet_id);
  cdc::CDCServiceImpl* leader_cdc =
      cluster_->mini_tablet_server(leader_idx)->server()->cdc_service();
  ASSERT_NE(nullptr, leader_cdc);

  // The consumer reads and checkpoints: that creates its session on the
  // leader, but no WAL anchor.
  GetChangesResponsePB gc_resp;
  ASSERT_OK(DoGetChanges(leader_idx, stream_id, tablet_id, 0, &gc_resp));
  ASSERT_FALSE(gc_resp.has_error()) << gc_resp.error().DebugString();
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, 1));
  ASSERT_TRUE(leader_cdc->HasSessionForTests(stream_id, tablet_id));
  ASSERT_FALSE(HasCDCLogAnchor(leader)) << leader->log_anchor_registry()->DumpAnchorInfo();

  ASSERT_OK(DeleteStream(stream_id));
  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(leader_cdc->HasSessionForTests(stream_id, tablet_id))
        << "session still present on the leader after stream delete (A4 leak)";
    ASSERT_EQ(-1, leader->tablet_metadata()->cdc_min_retained_op_index());
  });

  // The consumer has not noticed the delete yet and polls again.
  gc_resp.Clear();
  ASSERT_OK(DoGetChanges(leader_idx, stream_id, tablet_id, 1, &gc_resp));
  ASSERT_TRUE(gc_resp.has_error());
  EXPECT_EQ(CDCErrorPB::STREAM_NOT_FOUND, gc_resp.error().code());
  Status cs = DoCheckpoint(leader_idx, stream_id, tablet_id, 2);
  EXPECT_FALSE(cs.ok());
  EXPECT_FALSE(leader_cdc->HasSessionForTests(stream_id, tablet_id));
  EXPECT_FALSE(HasCDCLogAnchor(leader)) << leader->log_anchor_registry()->DumpAnchorInfo();
}

// Only the leader serves CDC: a Checkpoint sent to a follower (e.g. by a
// consumer that has not noticed a leader change yet) is refused, and creates no
// session there that nothing would end.
TEST_F(CDCFailoverITest, Checkpoint_OnFollowerRejected) {
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

  auto proxy = MakeCDCProxy(follower_idx);
  CheckpointRequestPB req;
  CheckpointResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_op_index(1);
  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_LEADER, resp.error().code());
  cdc::CDCServiceImpl* follower_cdc =
      cluster_->mini_tablet_server(follower_idx)->server()->cdc_service();
  EXPECT_FALSE(follower_cdc->HasSessionForTests(stream_id, tablet_id));
  EXPECT_EQ(0, follower_cdc->ActiveStreamCount());
}

// A replica that loses the leadership ends its consumer sessions: it releases
// their history floors, and it no longer reports their (frozen) lag and activity
// in its metrics.
TEST_F(CDCFailoverITest, SessionsEndedOnLeadershipLoss) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 3));

  int old_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    old_leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(old_leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(old_leader_idx);
  GetChangesRequestPB req;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(0);
  GetChangesResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  cdc::CDCServiceImpl* old_leader_cdc =
      cluster_->mini_tablet_server(old_leader_idx)->server()->cdc_service();
  ASSERT_EQ(1, old_leader_cdc->ActiveStreamCount());
  ASSERT_TRUE(old_leader_cdc->HasSessionForTests(stream_id, tablet_id));

  scoped_refptr<TabletReplica> old_leader;
  ASSERT_OK(cluster_->mini_tablet_server(old_leader_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &old_leader));
  ASSERT_EVENTUALLY([&] {
    const int leader_idx = FindLeaderIndex(tablet_id);
    if (leader_idx == old_leader_idx) {
      consensus::LeaderStepDownResponsePB step_down_resp;
      WARN_NOT_OK(old_leader->shared_consensus()->StepDown(&step_down_resp),
                  "step down failed");
    }
    ASSERT_GE(leader_idx, 0);
    ASSERT_NE(old_leader_idx, leader_idx);
  });
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, old_leader_cdc->ActiveStreamCount());
    ASSERT_FALSE(old_leader_cdc->HasSessionForTests(stream_id, tablet_id));
  });
  EXPECT_EQ(0, old_leader_cdc->MaxActiveAgeMicros());
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

// A transaction that is open when a snapshot is established is not in the
// snapshot. Its rows must reach the consumer from the WAL when it commits,
// including the rows it wrote before the snapshot began: the streaming start
// point is before the transaction's first op. Before, the stream started at the
// committed index and emitted the transaction without the rows written before
// it.
TEST_F(CDCFailoverITest, SnapshotWithOpenTransactionDeliversTxnAfterCommit) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::CHANGE,
                                   master::CDCStreamConfigPB::INITIAL_AND_CONTINUE,
                                   &stream_id));
  NO_FATALS(InsertRows(0, 5));

  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name_, &table));
  client::sp::shared_ptr<client::KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));
  client::sp::shared_ptr<client::KuduSession> session;
  ASSERT_OK(txn->CreateSession(&session));
  ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
  for (int i = 100; i < 103; ++i) {
    client::KuduInsert* insert = table->NewInsert();
    ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", i));
    ASSERT_OK(session->Apply(insert));
  }

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  std::set<int32_t> snapshot_keys;
  int64_t streaming_start = -1;
  bool first = true;
  string resume_key;
  for (int pages = 0; streaming_start < 0; ++pages) {
    ASSERT_LT(pages, 1000);
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
      int32_t k = -1;
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &k));
      snapshot_keys.insert(k);
    }
    resume_key = resp.snapshot_resume_key();
    if (resp.snapshot_done()) streaming_start = resp.snapshot_streaming_start_op_index();
  }
  EXPECT_EQ((std::set<int32_t>{ 0, 1, 2, 3, 4 }), snapshot_keys);

  ASSERT_OK(txn->Commit());
  ASSERT_EVENTUALLY([&] {
    vector<int32_t> keys;
    NO_FATALS(ReadInsertKeys(leader_idx, stream_id, tablet_id, streaming_start, &keys));
    std::set<int32_t> txn_keys;
    for (int32_t k : keys) {
      if (k >= 100) txn_keys.insert(k);
    }
    ASSERT_EQ((std::set<int32_t>{ 100, 101, 102 }), txn_keys);
  });
}

// A consumer that starts streaming from now while a transaction is open must
// get the transaction when it commits, including the rows it wrote before now.
// Before, the "now" checkpoint was the committed index, past those writes.
TEST_F(CDCFailoverITest, TailFromNow_OpenTxnCommittedLaterIsDelivered) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 3));

  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name_, &table));
  client::sp::shared_ptr<client::KuduTransaction> txn;
  ASSERT_OK(client_->NewTransaction(&txn));
  client::sp::shared_ptr<client::KuduSession> session;
  ASSERT_OK(txn->CreateSession(&session));
  ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
  for (int i = 100; i < 103; ++i) {
    client::KuduInsert* insert = table->NewInsert();
    ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", i));
    ASSERT_OK(session->Apply(insert));
  }

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  // Start from now, as CDCConsumer::kNow does.
  GetChangesResponsePB now;
  ASSERT_OK(DoGetChanges(leader_idx, stream_id, tablet_id,
                         std::numeric_limits<int64_t>::max(), &now));
  ASSERT_FALSE(now.has_error()) << now.error().DebugString();
  ASSERT_GE(now.checkpoint_op_index(), 0);

  ASSERT_OK(txn->Commit());
  ASSERT_EVENTUALLY([&] {
    vector<int32_t> keys;
    NO_FATALS(ReadInsertKeys(leader_idx, stream_id, tablet_id, now.checkpoint_op_index(),
                             &keys));
    // The rows written before the transaction began are not streamed again.
    ASSERT_EQ((vector<int32_t>{ 100, 101, 102 }), keys);
  });
}

// A table with a non-unique primary key, in CHANGE and FULL mode: INSERTs carry
// the auto_incrementing_id the tablet assigned, so later UPDATE/DELETE records
// can be matched to them, and an UPDATE the tablet rejects for changing an
// immutable column does not drop the valid UPDATEs of the same batch.
TEST_F(CDCFailoverITest, NonUniquePrimaryKeyTable) {
  NO_FATALS(StartClusterWithRF3());
  {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()
        ->NonUniquePrimaryKey();
    b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    b.AddColumn("tag")->Type(client::KuduColumnSchema::INT32)->Nullable()->Immutable();
    ASSERT_OK(b.Build(&schema));
    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name("cdc_non_unique_pk_table")
                  .schema(&schema)
                  .set_range_partition_columns({ "key" })
                  .num_replicas(3)
                  .Create());
  }
  table_name_ = "cdc_non_unique_pk_table";
  const string tablet_id = GetTabletId();
  const string kAutoIncCol = "auto_incrementing_id";

  string change_stream;
  string full_stream;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::CHANGE,
                                   master::CDCStreamConfigPB::NEVER, &change_stream));
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::NEVER, &full_stream));

  client::sp::shared_ptr<client::KuduTable> table;
  ASSERT_OK(client_->OpenTable(table_name_, &table));
  client::sp::shared_ptr<client::KuduSession> session = client_->NewSession();
  session->SetTimeoutMillis(kTimeout.ToMilliseconds());
  ASSERT_OK(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));
  for (int k = 1; k <= 3; ++k) {
    client::KuduInsert* insert = table->NewInsert();
    ASSERT_OK(insert->mutable_row()->SetInt32("key", k));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", k * 10));
    ASSERT_OK(insert->mutable_row()->SetInt32("tag", k));
    ASSERT_OK(session->Apply(insert));
  }
  ASSERT_OK(session->Flush());

  // The ids the tablet assigned, as ground truth.
  std::map<int32_t, int64_t> ids;
  {
    client::KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetProjectedColumnNames({ "key", kAutoIncCol }));
    ASSERT_OK(scanner.SetSelection(client::KuduClient::LEADER_ONLY));
    ASSERT_OK(scanner.Open());
    client::KuduScanBatch batch;
    while (scanner.HasMoreRows()) {
      ASSERT_OK(scanner.NextBatch(&batch));
      for (const auto& row : batch) {
        int32_t k;
        int64_t id;
        ASSERT_OK(row.GetInt32("key", &k));
        ASSERT_OK(row.GetInt64(kAutoIncCol, &id));
        ids[k] = id;
      }
    }
  }
  ASSERT_EQ(3, ids.size());

  // One batch: valid UPDATEs of keys 1 and 3, and an UPDATE of key 2's immutable
  // column that the tablet rejects.
  for (int k = 1; k <= 3; ++k) {
    client::KuduUpdate* update = table->NewUpdate();
    ASSERT_OK(update->mutable_row()->SetInt32("key", k));
    ASSERT_OK(update->mutable_row()->SetInt64(kAutoIncCol, ids[k]));
    if (k == 2) {
      ASSERT_OK(update->mutable_row()->SetInt32("tag", 99));
    } else {
      ASSERT_OK(update->mutable_row()->SetInt32("val", k * 10 + k));
    }
    ASSERT_OK(session->Apply(update));
  }
  ASSERT_TRUE(session->Flush().IsIOError());
  {
    vector<client::KuduError*> errors;
    ElementDeleter d(&errors);
    session->GetPendingErrors(&errors, nullptr);
    ASSERT_EQ(1, errors.size());
    ASSERT_TRUE(errors[0]->status().IsImmutable()) << errors[0]->status().ToString();
  }
  {
    client::KuduDelete* del = table->NewDelete();
    ASSERT_OK(del->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(del->mutable_row()->SetInt64(kAutoIncCol, ids[1]));
    ASSERT_OK(session->Apply(del));
    ASSERT_OK(session->Flush());
  }

  const auto get_int64 = [](const cdc::CDCRecordPB& r, bool before, const string& name,
                            int64_t* out) {
    for (const auto& c : before ? r.old_changes() : r.changes()) {
      if (c.column_name() == name && !c.is_null() && c.value().size() == sizeof(*out)) {
        memcpy(out, c.value().data(), sizeof(*out));
        return true;
      }
    }
    return false;
  };

  for (const string* stream_id : { &change_stream, &full_stream }) {
    const bool full = stream_id == &full_stream;
    SCOPED_TRACE(full ? "FULL" : "CHANGE");
    GetChangesResponsePB resp;
    ASSERT_EVENTUALLY([&] {
      const int leader_idx = FindLeaderIndex(tablet_id);
      ASSERT_GE(leader_idx, 0);
      auto proxy = MakeCDCProxy(leader_idx);
      GetChangesRequestPB req;
      req.set_stream_id(*stream_id);
      req.set_tablet_id(tablet_id);
      req.set_from_op_index(0);
      RpcController rpc;
      rpc.set_timeout(kTimeout);
      resp.Clear();
      ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
      ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    });

    std::map<int32_t, const cdc::CDCRecordPB*> inserts;
    std::map<int32_t, const cdc::CDCRecordPB*> updates;
    std::map<int32_t, const cdc::CDCRecordPB*> deletes;
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      int64_t id = -1;
      if (r.op_type() != CDCOpTypePB::INSERT && r.op_type() != CDCOpTypePB::UPDATE &&
          r.op_type() != CDCOpTypePB::DELETE) {
        continue;
      }
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &k)) << r.DebugString();
      ASSERT_TRUE(get_int64(r, /*before=*/false, kAutoIncCol, &id)) << r.DebugString();
      EXPECT_EQ(ids[k], id) << r.DebugString();
      if (r.op_type() == CDCOpTypePB::INSERT) inserts[k] = &r;
      if (r.op_type() == CDCOpTypePB::UPDATE) updates[k] = &r;
      if (r.op_type() == CDCOpTypePB::DELETE) deletes[k] = &r;
    }
    ASSERT_EQ(3, inserts.size()) << resp.DebugString();
    ASSERT_EQ(2, updates.size()) << resp.DebugString();
    ASSERT_EQ(1, deletes.size()) << resp.DebugString();
    ASSERT_EQ(0, updates.count(2)) << resp.DebugString();
    for (int k : { 1, 3 }) {
      int32_t v = -1;
      ASSERT_TRUE(GetInt32(*updates[k], /*before=*/false, "val", &v));
      EXPECT_EQ(k * 10 + k, v);
      if (full) {
        ASSERT_TRUE(GetInt32(*updates[k], /*before=*/true, "val", &v));
        EXPECT_EQ(k * 10, v);
        ASSERT_TRUE(GetInt32(*updates[k], /*before=*/false, "tag", &v));
        EXPECT_EQ(k, v);
      }
    }
    ASSERT_EQ(1, deletes.count(1));
    if (full) {
      int32_t v = -1;
      ASSERT_TRUE(GetInt32(*deletes[1], /*before=*/true, "val", &v));
      EXPECT_EQ(11, v);
    }
  }
}

// A transactional write that cannot be decoded must not let a partial
// transaction through: nothing of the transaction is emitted, the checkpoint
// stays before its first write, and the stream then fails with DECODE_FAILED
// until the write decodes again.
TEST_F(CDCFailoverITest, TxnWriteDecodeFailureFailsBatch) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(WriteCommittedTransaction(/*start=*/100, /*count=*/3));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  const auto get_changes = [&](int64_t from, GetChangesResponsePB* resp) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    resp->Clear();
    return proxy->GetChanges(req, resp, &rpc);
  };

  vector<int64_t> write_indexes;
  {
    GetChangesResponsePB resp;
    ASSERT_OK(get_changes(0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& r : resp.records()) {
      if (r.op_type() == CDCOpTypePB::INSERT) write_indexes.push_back(r.op_index());
    }
  }
  ASSERT_EQ(3, write_indexes.size());
  std::sort(write_indexes.begin(), write_indexes.end());
  FLAGS_cdc_inject_decode_failure_at_op_index = write_indexes[1];

  int64_t from = 0;
  GetChangesResponsePB resp;
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(get_changes(from, &resp));
    if (resp.has_error()) break;
    for (const auto& r : resp.records()) {
      EXPECT_FALSE(r.has_txn_id()) << "partial transaction emitted: " << r.DebugString();
    }
    ASSERT_GT(resp.checkpoint_op_index(), from);
    ASSERT_LT(resp.checkpoint_op_index(), write_indexes[0]);
    from = resp.checkpoint_op_index();
  }
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::DECODE_FAILED, resp.error().code());
  EXPECT_EQ(0, resp.records_size());

  FLAGS_cdc_inject_decode_failure_at_op_index = -1;
  ASSERT_OK(get_changes(from, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  int begins = 0;
  int inserts = 0;
  int commits = 0;
  for (const auto& r : resp.records()) {
    if (r.op_type() == CDCOpTypePB::BEGIN) ++begins;
    if (r.op_type() == CDCOpTypePB::INSERT) ++inserts;
    if (r.op_type() == CDCOpTypePB::COMMIT) ++commits;
  }
  EXPECT_EQ(1, begins);
  EXPECT_EQ(3, inserts);
  EXPECT_EQ(1, commits);
}

// A transactional INSERT_IGNORE of an existing key is ignored by the tablet but
// still in the REPLICATE. It must not be emitted inside BEGIN..COMMIT: a sink
// applying INSERTs as UPSERTs would overwrite the committed row. Replaying the
// stream as documented must reproduce the table.
TEST_F(CDCFailoverITest, TransactionalInsertIgnoreDuplicateNotEmitted) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(/*start=*/1, /*count=*/1));  // (1, 10)
  {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    client::sp::shared_ptr<client::KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    unique_ptr<client::KuduInsertIgnore> dup(table->NewInsertIgnore());
    ASSERT_OK(dup->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(dup->mutable_row()->SetInt32("val", 999));
    ASSERT_OK(session->Apply(dup.release()));
    unique_ptr<client::KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt32("key", 2));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", 5));
    ASSERT_OK(session->Apply(insert.release()));
    ASSERT_OK(txn->Commit());
  }

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  GetChangesResponsePB resp;
  ASSERT_EVENTUALLY([&] {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(0);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    resp.Clear();
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_TRUE(std::any_of(resp.records().begin(), resp.records().end(),
                            [](const cdc::CDCRecordPB& r) {
                              return r.op_type() == CDCOpTypePB::COMMIT;
                            }))
        << resp.DebugString();
  });

  vector<CDCOpTypePB> txn_ops;
  std::map<int32_t, int32_t> mirror;
  for (const auto& r : resp.records()) {
    if (r.has_txn_id()) {
      txn_ops.push_back(r.op_type());
    }
    if (r.op_type() == CDCOpTypePB::INSERT) {
      int32_t key = -1;
      int32_t val = -1;
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &key)) << r.DebugString();
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "val", &val)) << r.DebugString();
      mirror[key] = val;
    }
  }
  EXPECT_EQ(vector<CDCOpTypePB>({ CDCOpTypePB::BEGIN, CDCOpTypePB::INSERT,
                                  CDCOpTypePB::COMMIT }),
            txn_ops)
      << resp.DebugString();
  EXPECT_EQ(ScanTable(), mirror);
  EXPECT_EQ((std::map<int32_t, int32_t>{ { 1, 10 }, { 2, 5 } }), mirror);
}

// A committed transaction at the head of the read window streams even if the
// COMMIT lookup of every GetChanges call runs out of the call's time budget
// after the first op. Before, each call ended its batch at the transaction's
// second write, so the transaction stayed open, the checkpoint stayed below its
// first write, and every call reported more records while returning none.
TEST_F(CDCFailoverITest, TxnAtHeadStreamsWhenCommitLookupIsCut) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(WriteCommittedTransaction(/*start=*/1, /*count=*/3));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  const auto get_changes = [&](int64_t from, GetChangesResponsePB* resp) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    resp->Clear();
    return proxy->GetChanges(req, resp, &rpc);
  };
  // Every COMMIT of the transaction is in the WAL once its COMMIT record streams.
  ASSERT_EVENTUALLY([&] {
    GetChangesResponsePB resp;
    ASSERT_OK(get_changes(0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_TRUE(std::any_of(resp.records().begin(), resp.records().end(),
                            [](const cdc::CDCRecordPB& r) {
                              return r.op_type() == CDCOpTypePB::COMMIT;
                            }))
        << resp.DebugString();
  });

  SCOPED_CLEANUP({ FLAGS_cdc_inject_commit_lookup_deadline_expired = false; });
  FLAGS_cdc_inject_commit_lookup_deadline_expired = true;
  int64_t from = 0;
  vector<CDCOpTypePB> txn_ops;
  std::map<int32_t, int32_t> mirror;
  int calls = 0;
  for (; calls < 20; ++calls) {
    GetChangesResponsePB resp;
    ASSERT_OK(get_changes(from, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& r : resp.records()) {
      if (r.has_txn_id()) {
        txn_ops.push_back(r.op_type());
      }
      if (r.op_type() == CDCOpTypePB::INSERT) {
        int32_t key = -1;
        int32_t val = -1;
        ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &key)) << r.DebugString();
        ASSERT_TRUE(GetInt32(r, /*before=*/false, "val", &val)) << r.DebugString();
        mirror[key] = val;
      }
    }
    const bool progressed = resp.checkpoint_op_index() > from;
    ASSERT_TRUE(progressed || !resp.have_more_records())
        << "call " << calls << " from op index " << from
        << " neither advanced nor ended: " << resp.ShortDebugString();
    from = std::max(from, resp.checkpoint_op_index());
    if (!txn_ops.empty() && txn_ops.back() == CDCOpTypePB::COMMIT) {
      break;
    }
  }
  ASSERT_LT(calls, 20) << "the transaction never streamed";
  EXPECT_EQ(vector<CDCOpTypePB>({ CDCOpTypePB::BEGIN, CDCOpTypePB::INSERT,
                                  CDCOpTypePB::INSERT, CDCOpTypePB::INSERT,
                                  CDCOpTypePB::COMMIT }),
            txn_ops);
  EXPECT_EQ(ScanTable(), mirror);
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

  // A consumer retrying the poll does not log the error every time.
  StringVectorSink sink;
  {
    ScopedRegisterSink reg(&sink);
    for (int i = 0; i < 3; ++i) {
      GetChangesRequestPB req;
      req.set_stream_id(stream_id);
      req.set_tablet_id(tablet_id);
      req.set_from_op_index(from);
      req.set_max_bytes(64);
      GetChangesResponsePB resp;
      RpcController rpc;
      rpc.set_timeout(kTimeout);
      ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
      ASSERT_TRUE(resp.has_error());
      ASSERT_EQ(CDCErrorPB::TRANSACTION_TOO_LARGE, resp.error().code());
    }
  }
  int num_logged = 0;
  for (const auto& msg : sink.logged_msgs()) {
    if (msg.find("--cdc_max_transaction_span_bytes") != string::npos) {
      ++num_logged;
    }
  }
  EXPECT_EQ(0, num_logged) << "the error was logged within the last minute already";
}

// A transaction whose WAL spans several segments is read across them, with
// the read window escalated to reach its commit, and emitted once and whole.
TEST_F(CDCFailoverITest, TxnSpansWalSegments) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  FLAGS_log_segment_size_bytes_for_tests = 8 * 1024;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  constexpr int kRows = 2000;
  {
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    client::sp::shared_ptr<client::KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(client::KuduSession::MANUAL_FLUSH));
    for (int i = 0; i < kRows; ++i) {
      client::KuduInsert* insert = table->NewInsert();
      ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i * 10));
      ASSERT_OK(session->Apply(insert));
      if (i % 50 == 49) {
        ASSERT_OK(session->Flush());
      }
    }
    ASSERT_OK(txn->Commit());
  }

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  ASSERT_GE(GetReplica(leader_idx, tablet_id)->log()->reader()->num_segments(), 3);
  const auto escalations = METRIC_cdc_wal_read_escalations.Instantiate(
      cluster_->mini_tablet_server(leader_idx)->server()->metric_entity());
  const int64_t escalations_before = escalations->value();
  auto proxy = MakeCDCProxy(leader_idx);
  int64_t from = 0;
  int begins = 0;
  int commits = 0;
  std::map<int32_t, int32_t> mirror;
  for (int calls = 0; calls < 1000; ++calls) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    req.set_max_bytes(512);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& r : resp.records()) {
      if (r.op_type() == CDCOpTypePB::BEGIN) {
        ++begins;
      } else if (r.op_type() == CDCOpTypePB::COMMIT) {
        ++commits;
      } else if (r.op_type() == CDCOpTypePB::INSERT) {
        int32_t key = -1;
        int32_t val = -1;
        ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &key)) << r.DebugString();
        ASSERT_TRUE(GetInt32(r, /*before=*/false, "val", &val)) << r.DebugString();
        ASSERT_TRUE(mirror.emplace(key, val).second) << "duplicate row " << key;
      }
    }
    if (resp.checkpoint_op_index() <= from && !resp.have_more_records()) {
      break;
    }
    from = std::max(from, resp.checkpoint_op_index());
  }
  EXPECT_EQ(1, begins);
  EXPECT_EQ(1, commits);
  EXPECT_EQ(kRows, mirror.size());
  EXPECT_EQ(ScanTable(), mirror);
  EXPECT_GT(escalations->value(), escalations_before);
}

// A leadership change between a transaction's BEGIN_COMMIT and its
// FINALIZE_COMMIT: the new leader applies the FINALIZE_COMMIT and streams the
// transaction once, with all of its rows. The participant ops are driven
// directly, so the change of leader lands between the two deterministically.
TEST_F(CDCFailoverITest, TxnLeaderFailoverMidCommit) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 2));

  int old_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    old_leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(old_leader_idx, 0);
  });
  scoped_refptr<TabletReplica> old_leader = GetReplica(old_leader_idx, tablet_id);
  constexpr int64_t kTxnId = 1;
  const auto participant_op = [&](TabletReplica* replica,
                                  tserver::ParticipantOpPB::ParticipantOpType type,
                                  int64_t commit_ts) {
    tserver::ParticipantResponsePB resp;
    RETURN_NOT_OK(tablet::CallParticipantOp(replica, kTxnId, type, commit_ts, &resp));
    return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
  };
  ASSERT_OK(participant_op(old_leader.get(), tserver::ParticipantOpPB::BEGIN_TXN, -1));
  for (int32_t key = 100; key < 103; ++key) {
    const Schema schema = old_leader->tablet_metadata()->schema()->CopyWithoutColumnIds();
    tserver::WriteRequestPB req;
    req.set_tablet_id(tablet_id);
    req.set_txn_id(kTxnId);
    ASSERT_OK(SchemaToPB(schema, req.mutable_schema()));
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("key", key));
    ASSERT_OK(row.SetInt32("val", key * 10));
    RowOperationsPBEncoder(req.mutable_row_operations()).Add(RowOperationsPB::INSERT, row);
    tserver::WriteResponsePB resp;
    unique_ptr<tablet::WriteOpState> op_state(
        new tablet::WriteOpState(old_leader.get(), &req, nullptr, &resp));
    CountDownLatch latch(1);
    op_state->set_completion_callback(unique_ptr<tablet::OpCompletionCallback>(
        new tablet::LatchOpCompletionCallback<tserver::WriteResponsePB>(&latch, &resp)));
    ASSERT_OK(old_leader->SubmitWrite(std::move(op_state)));
    latch.Wait();
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }
  ASSERT_OK(participant_op(old_leader.get(), tserver::ParticipantOpPB::BEGIN_COMMIT, -1));
  const auto txn_meta = FindOrDie(old_leader->tablet_metadata()->GetTxnMetadata(), kTxnId);
  ASSERT_TRUE(txn_meta->commit_mvcc_op_timestamp());
  const int64_t commit_ts = txn_meta->commit_mvcc_op_timestamp()->value();

  // The old leader streams nothing of the transaction yet.
  vector<int32_t> keys;
  NO_FATALS(ReadInsertKeys(old_leader_idx, stream_id, tablet_id, 0, &keys));
  EXPECT_EQ((vector<int32_t>{ 0, 1 }), keys);

  int new_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    new_leader_idx = FindLeaderIndex(tablet_id);
    if (new_leader_idx == old_leader_idx) {
      consensus::LeaderStepDownResponsePB resp;
      WARN_NOT_OK(old_leader->shared_consensus()->StepDown(&resp), "step down failed");
    }
    ASSERT_GE(new_leader_idx, 0);
    ASSERT_NE(old_leader_idx, new_leader_idx);
  });
  scoped_refptr<TabletReplica> new_leader = GetReplica(new_leader_idx, tablet_id);
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(participant_op(new_leader.get(), tserver::ParticipantOpPB::FINALIZE_COMMIT,
                             commit_ts));
  });

  keys.clear();
  NO_FATALS(ReadInsertKeys(new_leader_idx, stream_id, tablet_id, 0, &keys));
  EXPECT_EQ((vector<int32_t>{ 0, 1, 100, 101, 102 }), keys);
  std::map<int32_t, int32_t> expected;
  for (int32_t key : keys) {
    expected[key] = key * 10;
  }
  EXPECT_EQ(expected, ScanTable());
}

// A transaction that its client abandons is aborted by the transaction status
// manager once it gets no keepalives. Until then it holds the stream's
// checkpoint before its writes; after the abort the stream moves past it, emits
// none of its rows, and streams the writes that follow.
TEST_F(CDCFailoverITest, AbandonedTxnAutoAbortUnpinsCheckpoint) {
  FLAGS_txn_manager_enabled = true;
  FLAGS_txn_manager_lazily_initialized = false;
  FLAGS_enable_txn_system_client_init = true;
  FLAGS_txn_keepalive_interval_ms = 1000;
  FLAGS_txn_staleness_tracker_interval_ms = 100;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 2));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  vector<int32_t> keys;
  int64_t checkpoint = -1;
  NO_FATALS(ReadInsertKeys(leader_idx, stream_id, tablet_id, 0, &keys, &checkpoint));
  ASSERT_EQ((vector<int32_t>{ 0, 1 }), keys);
  {
    // The handle and its session go away without a commit or an abort, and
    // with them the keepalives.
    client::sp::shared_ptr<client::KuduTable> table;
    ASSERT_OK(client_->OpenTable(table_name_, &table));
    client::sp::shared_ptr<client::KuduTransaction> txn;
    ASSERT_OK(client_->NewTransaction(&txn));
    client::sp::shared_ptr<client::KuduSession> session;
    ASSERT_OK(txn->CreateSession(&session));
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    for (int i = 100; i < 103; ++i) {
      client::KuduInsert* insert = table->NewInsert();
      ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i * 10));
      ASSERT_OK(session->Apply(insert));
    }
  }

  // While the transaction is open the checkpoint stays before its writes.
  keys.clear();
  int64_t pinned = -1;
  NO_FATALS(ReadInsertKeys(leader_idx, stream_id, tablet_id, checkpoint, &keys, &pinned));
  EXPECT_TRUE(keys.empty());

  // It is aborted for lack of keepalives.
  ASSERT_EVENTUALLY([&] {
    const auto txns = GetReplica(leader_idx, tablet_id)->tablet_metadata()->GetTxnMetadata();
    ASSERT_EQ(1, txns.size());
    ASSERT_TRUE(txns.begin()->second->aborted());
  });
  // The partition lock is free again.
  NO_FATALS(InsertRows(200, 2));
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  keys.clear();
  int64_t after = -1;
  NO_FATALS(ReadInsertKeys(leader_idx, stream_id, tablet_id, pinned, &keys, &after));
  EXPECT_EQ((vector<int32_t>{ 200, 201 }), keys);
  EXPECT_GT(after, pinned);
  EXPECT_EQ((std::map<int32_t, int32_t>{ { 0, 0 }, { 1, 10 }, { 200, 2000 }, { 201, 2010 } }),
            ScanTable());
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

  // Checkpoint twice with the same value; both should succeed.
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

  // Read from checkpoint: should be empty, no error.
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

// Resumable snapshot bootstrap: an initial snapshot that loses its in-memory
// session mid-scan (leader change / master failover) must RESUME from the durable
// (snap_ts, resume_key) cursor instead of returning SNAPSHOT_SESSION_LOST and
// forcing a full re-snapshot.
//
// The in-memory session loss is simulated with ClearSnapshotSessionsForTests(),
// which discards exactly the per-(stream, tablet) snapshot state a newly elected
// leader would start without, while keeping the same tablet (and thus the same
// retained history at snap_ts) -- isolating the resume path from unrelated
// failover mechanics. Correctness assertions:
//   1. after the loss, the next page does NOT error (it cold-resumes),
//   2. the resume counter advances (the cold-resume path actually ran),
//   3. every key [0, 20) is read across the whole scan -- E4 holds because the
//      resume reuses the ORIGINAL snap_ts, so no row is torn or skipped (the
//      page served before the loss is served again, at-least-once).
TEST_F(CDCFailoverITest, Snapshot_ResumesAcrossSessionLoss) {
  // Persist the durable cursor on every page so the resume has a fresh cursor to
  // rebuild from regardless of timing.
  FLAGS_cdc_snapshot_cursor_persist_interval_ms = 0;

  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::INITIAL_AND_CONTINUE,
                                   &stream_id));
  const int kRows = 20;
  NO_FATALS(InsertRows(0, kRows));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  cdc::CDCServiceImpl* leader_cdc =
      cluster_->mini_tablet_server(leader_idx)->server()->cdc_service();
  ASSERT_NE(nullptr, leader_cdc);

  std::set<int32_t> keys_seen;
  int64_t streaming_start = -1;
  bool done = false;
  bool first = true;
  string resume_key;
  int pages = 0;
  bool cleared_once = false;

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
    ASSERT_FALSE(resp.has_error())
        << "page " << pages << ": " << resp.error().DebugString();
    first = false;

    for (const auto& r : resp.records()) {
      ASSERT_EQ(CDCOpTypePB::READ, r.op_type());
      int32_t k = -1;
      ASSERT_TRUE(GetInt32(r, /*before=*/false, "key", &k));
      // At-least-once: the page served before the loss is served again by the
      // resume, so its keys repeat.
      keys_seen.insert(k);
    }

    // After the first page has established the session and emitted a resume key,
    // simulate a leader change by discarding the in-memory snapshot session
    // exactly once. The next request (still is_snapshot_start=false with the same
    // resume key) must cold-resume from the durable cursor.
    if (!cleared_once && !resp.snapshot_done() && !resp.snapshot_resume_key().empty()) {
      ASSERT_EQ(0, leader_cdc->SnapshotSessionsResumedForTests());
      // The page queued its cursor persist: wait for the cursor to reach the
      // master, as the resume rebuilds the session from it. The cursor is the
      // position this page was served after (the newest one the client
      // acknowledged), so the resume serves this page again.
      ASSERT_EVENTUALLY([&] {
        master::GetCDCStreamInfoRequestPB info_req;
        master::GetCDCStreamInfoResponsePB info_resp;
        info_req.set_stream_id(stream_id);
        RpcController info_rpc;
        info_rpc.set_timeout(kTimeout);
        ASSERT_OK(master_proxy_->GetCDCStreamInfo(info_req, &info_resp, &info_rpc));
        ASSERT_FALSE(info_resp.has_error()) << info_resp.error().DebugString();
        bool cursor_persisted = false;
        for (const auto& cur : info_resp.stream().tablet_snapshot_cursors()) {
          cursor_persisted |= cur.tablet_id() == tablet_id &&
                              cur.snapshot_resume_key() == req.snapshot_resume_key();
        }
        ASSERT_TRUE(cursor_persisted);
      });
      leader_cdc->ClearSnapshotSessionsForTests();
      cleared_once = true;
    }

    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    if (done) streaming_start = resp.snapshot_streaming_start_op_index();
    ASSERT_LT(++pages, 1000);
  }

  // The loss was simulated and the scan still completed: the cold-resume path ran.
  ASSERT_TRUE(cleared_once) << "test did not exercise the session-loss window";
  EXPECT_GE(leader_cdc->SnapshotSessionsResumedForTests(), 1)
      << "snapshot did not resume from the durable cursor after session loss";
  EXPECT_EQ(0, leader_cdc->SnapshotSessionsLostForTests())
      << "snapshot was declared lost even though history was retained";

  // Every key is present: no gap (E4 snapshot completeness).
  ASSERT_EQ(static_cast<size_t>(kRows), keys_seen.size());
  EXPECT_EQ(0, *keys_seen.begin());
  EXPECT_EQ(kRows - 1, *keys_seen.rbegin());
  ASSERT_GE(streaming_start, 0);
}

// A page that a cold leader resumed after a durable cursor older than the
// client's key (the cursor persists are throttled) is served again when its
// response is lost and the client re-sends its key. Before, the re-sent key
// matched neither the start nor the end of the page the session had served,
// and the snapshot was declared lost.
TEST_F(CDCFailoverITest, Snapshot_ColdResumedPageLostResponseIsReserved) {
  // Only the establish persists the cursor.
  FLAGS_cdc_snapshot_cursor_persist_interval_ms = 3600 * 1000;

  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::INITIAL_AND_CONTINUE,
                                   &stream_id));
  const int kRows = 20;
  NO_FATALS(InsertRows(0, kRows));

  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  cdc::CDCServiceImpl* leader_cdc =
      cluster_->mini_tablet_server(leader_idx)->server()->cdc_service();
  const auto page = [&](bool is_start, const string& resume_key, GetChangesResponsePB* resp) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_is_snapshot_start(is_start);
    if (!resume_key.empty()) req.set_snapshot_resume_key(resume_key);
    req.set_max_bytes(64);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    resp->Clear();
    return proxy->GetChanges(req, resp, &rpc);
  };
  const auto page_keys = [&](const GetChangesResponsePB& resp) {
    vector<int32_t> keys;
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      CHECK(GetInt32(r, /*before=*/false, "key", &k));
      keys.push_back(k);
    }
    return keys;
  };

  std::set<int32_t> keys_seen;
  GetChangesResponsePB resp;
  string resume_key;
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(page(/*is_start=*/i == 0, resume_key, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_FALSE(resp.snapshot_done());
    for (int32_t k : page_keys(resp)) keys_seen.insert(k);
    resume_key = resp.snapshot_resume_key();
  }
  // The establish's cursor (at the start of the table) is on the master.
  ASSERT_EVENTUALLY([&] {
    master::GetCDCStreamInfoRequestPB info_req;
    master::GetCDCStreamInfoResponsePB info_resp;
    info_req.set_stream_id(stream_id);
    RpcController info_rpc;
    info_rpc.set_timeout(kTimeout);
    ASSERT_OK(master_proxy_->GetCDCStreamInfo(info_req, &info_resp, &info_rpc));
    ASSERT_FALSE(info_resp.has_error()) << info_resp.error().DebugString();
    ASSERT_EQ(1, info_resp.stream().tablet_snapshot_cursors_size());
    ASSERT_TRUE(info_resp.stream().tablet_snapshot_cursors(0).snapshot_active());
  });

  // The session is lost; the next page cold-resumes, but its response is lost.
  leader_cdc->ClearSnapshotSessionsForTests();
  GetChangesResponsePB lost;
  ASSERT_OK(page(/*is_start=*/false, resume_key, &lost));
  ASSERT_FALSE(lost.has_error()) << lost.error().DebugString();
  ASSERT_EQ(1, leader_cdc->SnapshotSessionsResumedForTests());

  // The client re-sends its key and gets the same page again.
  GetChangesResponsePB again;
  ASSERT_OK(page(/*is_start=*/false, resume_key, &again));
  ASSERT_FALSE(again.has_error()) << again.error().DebugString();
  EXPECT_EQ(page_keys(lost), page_keys(again));
  EXPECT_EQ(lost.snapshot_resume_key(), again.snapshot_resume_key());
  EXPECT_EQ(1, leader_cdc->SnapshotSessionsResumedForTests());

  // The client continues from there and gets every row.
  resp = again;
  for (int pages = 0; ; ++pages) {
    ASSERT_LT(pages, 1000);
    for (int32_t k : page_keys(resp)) keys_seen.insert(k);
    if (resp.snapshot_done()) break;
    const string key = resp.snapshot_resume_key();
    ASSERT_OK(page(/*is_start=*/false, key, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
  EXPECT_EQ(0, leader_cdc->SnapshotSessionsLostForTests());
  ASSERT_EQ(static_cast<size_t>(kRows), keys_seen.size());
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

// Creating a FULL stream pins history on every replica right away: the eager
// barrier carries the stream's creation-time history floor. Without it the
// first poll of a consumer that starts later than --tablet_history_max_age_sec
// after the creation would get HISTORY_EXPIRED.
TEST_F(CDCFailoverITest, FullStreamCreation_PinsHistoryOnAllReplicas) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  // Pause the maintenance pass: only the eager barrier reaches the replicas.
  FLAGS_cdc_bg_scan_interval_ms = 1000000;

  const int64_t before = GetCurrentTimeMicros();
  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::NEVER, &stream_id));
  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    scoped_refptr<TabletReplica> replica;
    ASSERT_OK(cluster_->mini_tablet_server(i)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &replica));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(0, replica->tablet_metadata()->cdc_min_retained_op_index());
      ASSERT_GE(replica->tablet_metadata()->cdc_history_safe_time_micros(), before);
      ASSERT_GT(replica->shared_tablet()->cdc_history_floor().value(), 0);
    });
  }
}

// Creating a stream pushes an eager barrier to every replica. That barrier
// replaces the tablet's aggregate one, so it must carry the aggregate history
// floor of the streams that already exist: a floor of 0 would release the
// history another FULL stream needs until the next maintenance pass.
TEST_F(CDCFailoverITest, CreateSecondStream_PreservesExistingHistoryFloor) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();

  string full_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::NEVER, &full_id));
  NO_FATALS(InsertRows(0, 2));
  NO_FATALS(UpdateRow(0, 5));
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  int64_t cp = -1;
  {
    auto proxy = MakeCDCProxy(leader_idx);
    GetChangesRequestPB req;
    GetChangesResponsePB resp;
    req.set_stream_id(full_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(0);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    cp = resp.checkpoint_op_index();
  }
  ASSERT_GT(cp, 0);
  ASSERT_OK(DoCheckpoint(leader_idx, full_id, tablet_id, cp));

  // Wait for the maintenance pass to push the FULL stream's floor everywhere.
  vector<scoped_refptr<TabletReplica>> replicas;
  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    scoped_refptr<TabletReplica> replica;
    ASSERT_OK(cluster_->mini_tablet_server(i)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &replica));
    replicas.emplace_back(std::move(replica));
  }
  uint64_t floor_micros = 0;
  ASSERT_EVENTUALLY([&] {
    floor_micros = replicas[0]->tablet_metadata()->cdc_history_safe_time_micros();
    ASSERT_GT(floor_micros, 0);
    for (const auto& r : replicas) {
      ASSERT_EQ(cp, r->tablet_metadata()->cdc_min_retained_op_index());
      ASSERT_EQ(floor_micros, r->tablet_metadata()->cdc_history_safe_time_micros());
    }
  });

  // Pause the maintenance pass, so that the eager barrier of the new stream is
  // the last barrier the replicas get, and create a CHANGE stream.
  FLAGS_cdc_bg_scan_interval_ms = 1000000;
  string change_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::CHANGE,
                                   master::CDCStreamConfigPB::NEVER, &change_id));

  // The eager barrier (op index 0 for the new stream) lands and keeps the
  // FULL stream's floor on every replica.
  const Timestamp floor = clock::HybridClock::TimestampFromMicroseconds(floor_micros);
  for (const auto& r : replicas) {
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(0, r->tablet_metadata()->cdc_min_retained_op_index());
    });
    EXPECT_EQ(floor_micros, r->tablet_metadata()->cdc_history_safe_time_micros());
    const Timestamp tablet_floor = r->shared_tablet()->cdc_history_floor();
    EXPECT_GT(tablet_floor.value(), 0);
    EXPECT_LE(tablet_floor, floor);
  }
}

// The history floor of a CHANGE-mode snapshot in progress is fanned out to
// every replica like a FULL stream's, so a new leader keeps the history at
// snap_ts and can resume the snapshot.
TEST_F(CDCFailoverITest, Snapshot_ChangeMode_HistoryFloorOnAllReplicas) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::CHANGE,
                                   master::CDCStreamConfigPB::INITIAL_AND_CONTINUE,
                                   &stream_id));
  NO_FATALS(InsertRows(0, 10));
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });

  // The first page establishes the snapshot and persists its cursor.
  uint64_t snap_ts = 0;
  {
    auto proxy = MakeCDCProxy(leader_idx);
    GetChangesRequestPB req;
    GetChangesResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_is_snapshot_start(true);
    req.set_max_bytes(64);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_FALSE(resp.snapshot_done());
    ASSERT_GT(resp.records_size(), 0);
    snap_ts = resp.records(0).timestamp();
  }
  const Timestamp expected = clock::HybridClock::TimestampFromMicroseconds(
      clock::HybridClock::GetPhysicalValueMicros(Timestamp(snap_ts)));

  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    if (i == leader_idx) continue;
    scoped_refptr<TabletReplica> follower;
    ASSERT_OK(cluster_->mini_tablet_server(i)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &follower));
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(expected, follower->shared_tablet()->cdc_history_floor());
    });
  }
}

// A cold resume of a snapshot (no in-memory session on the leader) must be
// refused when the history at the original snap_ts was garbage-collected, even
// if the current ancient history mark is back below snap_ts because a floor was
// re-pinned there after the GC. The history-GC water mark records what was
// removed; the current AHM does not.
TEST_F(CDCFailoverITest, Snapshot_ColdResumeRefusedAfterHistoryGc) {
  FLAGS_cdc_snapshot_cursor_persist_interval_ms = 0;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::INITIAL_AND_CONTINUE,
                                   &stream_id));
  const int kRows = 10;
  NO_FATALS(InsertRows(0, kRows));
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  cdc::CDCServiceImpl* leader_cdc =
      cluster_->mini_tablet_server(leader_idx)->server()->cdc_service();
  scoped_refptr<TabletReplica> leader;
  ASSERT_OK(cluster_->mini_tablet_server(leader_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &leader));

  string resume_key;
  Timestamp snap_ts;
  {
    GetChangesRequestPB req;
    GetChangesResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_is_snapshot_start(true);
    req.set_max_bytes(64);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_FALSE(resp.snapshot_done());
    ASSERT_GT(resp.records_size(), 0);
    resume_key = resp.snapshot_resume_key();
    snap_ts = Timestamp(resp.records(0).timestamp());
  }
  ASSERT_FALSE(resume_key.empty());

  // The session is lost, and with the maintenance pass paused, every history
  // floor on the leader is released (as the CDC valve does). The rows change
  // and a compaction GCs the history at snap_ts. A maintenance pass that was
  // already running may still push the master's floor and pin the history
  // again, so release the floors and compact until the history is GCed.
  FLAGS_cdc_bg_scan_interval_ms = 1000000;
  leader_cdc->ClearSnapshotSessionsForTests();
  for (int i = 0; i < kRows; ++i) {
    NO_FATALS(UpdateRow(i, 777));
  }
  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_EVENTUALLY([&] {
    leader->shared_tablet()->ClearCDCHistoryFloorHolders();
    ASSERT_OK(leader->tablet()->Flush());
    ASSERT_OK(leader->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
    ASSERT_GT(leader->shared_tablet()->cdc_history_gc_water_mark(), snap_ts);
  });

  // The master re-pins the snapshot's floor, so the current AHM is at or below
  // snap_ts again.
  FLAGS_cdc_bg_scan_interval_ms = 100;
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(leader->shared_tablet()->cdc_history_floor().value(), 0);
  });

  GetChangesRequestPB req;
  GetChangesResponsePB resp;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_snapshot_resume_key(resume_key);
  req.set_max_bytes(64);
  RpcController rpc;
  rpc.set_timeout(kTimeout);
  ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, resp.error().code())
      << resp.error().DebugString();
  EXPECT_EQ(0, resp.records_size());
  EXPECT_EQ(0, leader_cdc->SnapshotSessionsResumedForTests());
}

// After a leader change, the history floor of the former leader follows the
// master's floor again: the session the consumer had there no longer gets
// reads or checkpoints, so the floor it held must not keep that replica's
// history pinned once the master's floor has moved past it.
TEST_F(CDCFailoverITest, FullMode_ExLeaderFollowsMasterHistoryFloor) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 2));
  NO_FATALS(UpdateRow(0, 5));

  const auto read_and_checkpoint = [&](int ts_idx, int64_t from_op_index,
                                       int64_t* checkpoint) {
    auto proxy = MakeCDCProxy(ts_idx);
    GetChangesRequestPB req;
    GetChangesResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from_op_index);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_GT(resp.records_size(), 0);
    *checkpoint = resp.checkpoint_op_index();
    CheckpointRequestPB cp_req;
    CheckpointResponsePB cp_resp;
    cp_req.set_stream_id(stream_id);
    cp_req.set_tablet_id(tablet_id);
    cp_req.set_op_index(*checkpoint);
    RpcController cp_rpc;
    cp_rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(cp_req, &cp_resp, &cp_rpc));
    ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  };

  int old_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    old_leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(old_leader_idx, 0);
  });
  scoped_refptr<TabletReplica> old_leader;
  ASSERT_OK(cluster_->mini_tablet_server(old_leader_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &old_leader));
  int64_t cp = -1;
  NO_FATALS(read_and_checkpoint(old_leader_idx, 0, &cp));
  const Timestamp session_floor = old_leader->shared_tablet()->cdc_history_floor();
  ASSERT_GT(session_floor.value(), 0);

  // Move the leadership away, then the consumer reads on and checkpoints on
  // the new leader, which raises the master's floor.
  int new_leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    new_leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(new_leader_idx, 0);
    if (new_leader_idx == old_leader_idx) {
      consensus::LeaderStepDownResponsePB resp;
      WARN_NOT_OK(old_leader->shared_consensus()->StepDown(&resp), "step down failed");
      FAIL() << "the old leader is still the leader";
    }
  });
  NO_FATALS(InsertRows(100, 2));
  int64_t new_cp = -1;
  NO_FATALS(read_and_checkpoint(new_leader_idx, cp, &new_cp));

  ASSERT_EVENTUALLY([&] {
    const uint64_t master_micros =
        old_leader->tablet_metadata()->cdc_history_safe_time_micros();
    ASSERT_GT(master_micros, clock::HybridClock::GetPhysicalValueMicros(session_floor));
    ASSERT_EQ(clock::HybridClock::TimestampFromMicroseconds(master_micros),
              old_leader->shared_tablet()->cdc_history_floor());
  });
}

// A consumer whose first read on a leader starts after its checkpoint (as
// after a leader change with reads in flight) must not have the checkpoint
// report the floor of that later read: the history between the checkpoint and
// the read would then be unprotected. The master keeps the floor it has.
TEST_F(CDCFailoverITest, FullMode_CheckpointBelowServedReadsKeepsMasterFloor) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStreamWithConfig(master::CDCStreamConfigPB::FULL,
                                   master::CDCStreamConfigPB::NEVER, &stream_id));
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  scoped_refptr<TabletReplica> leader;
  ASSERT_OK(cluster_->mini_tablet_server(leader_idx)->server()->tablet_manager()
      ->GetTabletReplica(tablet_id, &leader));
  uint64_t creation_floor_micros = 0;
  ASSERT_EVENTUALLY([&] {
    creation_floor_micros = leader->tablet_metadata()->cdc_history_safe_time_micros();
    ASSERT_GT(creation_floor_micros, 0);
  });

  NO_FATALS(InsertRows(0, 3));
  const auto last = leader->shared_consensus()->GetLastOpId(consensus::COMMITTED_OPID);
  ASSERT_TRUE(last.has_value());
  const int64_t ahead = last->index() - 1;
  const int64_t checkpoint = 1;
  ASSERT_GT(ahead, checkpoint);

  auto proxy = MakeCDCProxy(leader_idx);
  {
    GetChangesRequestPB req;
    GetChangesResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(ahead);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_GT(resp.records_size(), 0);
  }
  {
    CheckpointRequestPB req;
    CheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(checkpoint);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->Checkpoint(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // The barrier that carries the new checkpoint also carries the floor the
  // master derived from the same checkpoint row.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(checkpoint, leader->tablet_metadata()->cdc_min_retained_op_index());
  });
  EXPECT_EQ(creation_floor_micros,
            leader->tablet_metadata()->cdc_history_safe_time_micros());
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

// A dropped range partition is kept for a lagging stream: its replicas reject
// writes (also from a client that still routes them there), keep it across a
// master restart, and serve the changes the stream has not read, followed by
// TABLET_DROPPED with the partition's bounds. Once the stream acknowledged the
// drop at the tablet's last op, the master deletes the tablet. A tablet whose changes the
// stream does not read within --cdc_dropped_tablet_retention_secs is deleted
// anyway, and the master reports that the stream lost them.
TEST_F(CDCFailoverITest, DropRangePartition_LaggingConsumerTailDrainedOrSignalled) {
  NO_FATALS(StartCluster(3));
  NO_FATALS(CreateRangeTable("cdc_drop_tail_table", { 0, 100, 200 }));
  const auto tablet_of_range = [&](int lower) { return TabletOfRange(lower); };
  const auto drop_range = [&](int lower) { return DropRange(lower); };
  const auto stream_info = [&](const string& stream_id, master::CDCStreamInfoPB* info) {
    return GetStreamInfo(stream_id, info);
  };
  const auto find_dropped = &FindDropped;
  string drained_tablet;
  string lost_tablet;
  ASSERT_EVENTUALLY([&] {
    drained_tablet = tablet_of_range(100);
    lost_tablet = tablet_of_range(200);
    ASSERT_FALSE(drained_tablet.empty());
    ASSERT_FALSE(lost_tablet.empty());
  });

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  // A client that wrote to the table before the drop, but did not drop the
  // partition itself, routes writes by the partitions it cached.
  client::sp::shared_ptr<client::KuduClient> stale_client;
  ASSERT_OK(cluster_->CreateClient(nullptr, &stale_client));
  client::sp::shared_ptr<client::KuduTable> stale_table;
  ASSERT_OK(stale_client->OpenTable(table_name_, &stale_table));
  NO_FATALS(InsertRows(100, 5));
  NO_FATALS(InsertRows(200, 5));
  {
    client::sp::shared_ptr<client::KuduSession> session = stale_client->NewSession();
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    client::KuduInsert* insert = stale_table->NewInsert();
    ASSERT_OK(insert->mutable_row()->SetInt32("key", 105));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", 1050));
    ASSERT_OK(session->Apply(insert));
  }

  // The stream has read nothing when the partition [100, 200) is dropped.
  ASSERT_OK(drop_range(100));
  ASSERT_EVENTUALLY([&] {
    const auto replicas = GetAllReplicas(drained_tablet);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_TRUE(r->rejects_writes_of_dropped_tablet());
    }
  });
  {
    client::sp::shared_ptr<client::KuduSession> session = stale_client->NewSession();
    session->SetTimeoutMillis(10000);
    ASSERT_OK(session->SetFlushMode(client::KuduSession::AUTO_FLUSH_SYNC));
    client::KuduInsert* insert = stale_table->NewInsert();
    ASSERT_OK(insert->mutable_row()->SetInt32("key", 150));
    ASSERT_OK(insert->mutable_row()->SetInt32("val", 1500));
    Status s = session->Apply(insert);
    ASSERT_FALSE(s.ok()) << "a write to the dropped partition was accepted";
  }

  // The kept tablet survives a master restart and the tablet reports after it.
  cluster_->mini_master()->Shutdown();
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_OK(cluster_->mini_master()->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      kTimeout));
  NO_FATALS(WaitForMaintenancePasses(3));
  ASSERT_EQ(3, GetAllReplicas(drained_tablet).size());
  master::CDCStreamInfoPB info;
  ASSERT_OK(stream_info(stream_id, &info));
  ASSERT_FALSE(ContainsKey(info.tablet_checkpoints(), drained_tablet));
  const auto* dropped = find_dropped(info, drained_tablet);
  ASSERT_NE(nullptr, dropped);
  EXPECT_EQ(master::CDCStreamDroppedTabletPB::RETAINED, dropped->state());
  EXPECT_EQ(3, dropped->replicas_size());

  // The stream reads the rest of the tablet's changes, then the drop.
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(drained_tablet);
    ASSERT_GE(leader_idx, 0);
  });
  auto proxy = MakeCDCProxy(leader_idx);
  int64_t from = 0;
  int inserts = 0;
  GetChangesResponsePB resp;
  for (int i = 0; i < 100; ++i) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(drained_tablet);
    req.set_from_op_index(from);
    resp.Clear();
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    if (resp.has_error()) {
      break;
    }
    inserts += CountInserts(resp);
    from = resp.checkpoint_op_index();
  }
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(CDCErrorPB::TABLET_DROPPED, resp.error().code()) << resp.error().DebugString();
  EXPECT_FALSE(resp.error().is_retryable());
  EXPECT_FALSE(resp.error().needs_resnapshot());
  EXPECT_EQ(6, inserts);
  ASSERT_TRUE(resp.has_dropped_tablet());
  EXPECT_NE(string::npos,
            resp.dropped_tablet().partition_description().find("100 <= VALUES < 200"))
      << resp.dropped_tablet().partition_description();
  EXPECT_FALSE(resp.dropped_tablet().partition().partition_key_start().empty());
  EXPECT_GE(resp.dropped_tablet().last_op_index(), from);

  // A checkpoint of the last op does not end the tablet: the consumer may not
  // have applied the drop yet.
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, drained_tablet,
                         resp.dropped_tablet().last_op_index()));
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(stream_info(stream_id, &info));
    dropped = find_dropped(info, drained_tablet);
    ASSERT_NE(nullptr, dropped);
    ASSERT_EQ(resp.dropped_tablet().last_op_index(), dropped->op_index());
  });
  EXPECT_EQ(master::CDCStreamDroppedTabletPB::RETAINED, dropped->state());
  NO_FATALS(WaitForMaintenancePasses(3));
  ASSERT_EQ(3, GetAllReplicas(drained_tablet).size());

  // Once the stream acknowledged the drop, the tablet and its row are gone.
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, drained_tablet,
                         resp.dropped_tablet().last_op_index(), /*drop_applied=*/true));
  ASSERT_EVENTUALLY([&] {
    ASSERT_TRUE(GetAllReplicas(drained_tablet).empty());
    ASSERT_OK(stream_info(stream_id, &info));
    ASSERT_EQ(nullptr, find_dropped(info, drained_tablet));
  });

  // The stream does not read the changes of [200, 300) before the retention
  // ends: the tablet is deleted, and the master reports the loss.
  FLAGS_cdc_dropped_tablet_retention_secs = 1;
  ASSERT_OK(drop_range(200));
  ASSERT_EVENTUALLY([&] {
    ASSERT_TRUE(GetAllReplicas(lost_tablet).empty());
    ASSERT_OK(stream_info(stream_id, &info));
    const auto* d = find_dropped(info, lost_tablet);
    ASSERT_NE(nullptr, d);
    EXPECT_EQ(master::CDCStreamDroppedTabletPB::TAIL_LOST, d->state());
    EXPECT_TRUE(d->has_op_index());
    EXPECT_EQ(0, d->replicas_size());
    EXPECT_NE(string::npos, d->partition_description().find("200 <= VALUES < 300"))
        << d->partition_description();
  });
  ASSERT_FALSE(ContainsKey(info.tablet_checkpoints(), lost_tablet));
}

// The master decides to keep the tablet of a dropped range partition before
// the drop is visible in memory, so that neither a maintenance pass nor a
// tablet report in between treats the tablet as gone.
TEST_F(CDCFailoverITest, DropRangePartition_MaintenanceDuringDropKeepsTablet) {
  NO_FATALS(StartCluster(3));
  NO_FATALS(CreateRangeTable("cdc_drop_race_table", { 0, 100 }));
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
    tablet_id = TabletOfRange(100);
    ASSERT_FALSE(tablet_id.empty());
  });
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(100, 5));

  // Several maintenance passes run while the drop is in progress.
  FLAGS_catalog_manager_inject_latency_drop_partition_ms = 1500;
  ASSERT_OK(DropRange(100));
  FLAGS_catalog_manager_inject_latency_drop_partition_ms = 0;
  NO_FATALS(WaitForMaintenancePasses(3));

  const auto replicas = GetAllReplicas(tablet_id);
  ASSERT_EQ(3, replicas.size());
  for (const auto& r : replicas) {
    EXPECT_TRUE(r->rejects_writes_of_dropped_tablet());
  }
  master::CDCStreamInfoPB info;
  ASSERT_OK(GetStreamInfo(stream_id, &info));
  const auto* dropped = FindDropped(info, tablet_id);
  ASSERT_NE(nullptr, dropped);
  EXPECT_EQ(master::CDCStreamDroppedTabletPB::RETAINED, dropped->state());
}

// A maintenance pass that looked the tablet up before the drop and reads its
// state after the drop still sees that the tablet is kept for CDC.
TEST_F(CDCFailoverITest, DropRangePartition_MaintenanceScanAcrossDropKeepsTablet) {
  NO_FATALS(StartCluster(3));
  // A single range, so the pass waits on the tablet that is dropped.
  NO_FATALS(CreateRangeTable("cdc_drop_scan_race_table", { 100 }));
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
    tablet_id = TabletOfRange(100);
    ASSERT_FALSE(tablet_id.empty());
  });
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(100, 5));
  NO_FATALS(WaitForMaintenancePasses(1));

  // Every pass that starts from now on waits, after looking the tablet up,
  // until the drop marks it deleted. Let one of them start before the drop.
  FLAGS_catalog_manager_cdc_maintenance_wait_tablet_deleted_ms = 30000;
  SleepFor(MonoDelta::FromMilliseconds(1000));
  ASSERT_OK(DropRange(100));
  NO_FATALS(WaitForMaintenancePasses(1));
  FLAGS_catalog_manager_cdc_maintenance_wait_tablet_deleted_ms = 0;
  NO_FATALS(WaitForMaintenancePasses(3));

  const auto replicas = GetAllReplicas(tablet_id);
  ASSERT_EQ(3, replicas.size());
  for (const auto& r : replicas) {
    EXPECT_TRUE(r->rejects_writes_of_dropped_tablet());
  }
  master::CDCStreamInfoPB info;
  ASSERT_OK(GetStreamInfo(stream_id, &info));
  const auto* dropped = FindDropped(info, tablet_id);
  ASSERT_NE(nullptr, dropped);
  EXPECT_EQ(master::CDCStreamDroppedTabletPB::RETAINED, dropped->state());
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

  // No stream yet, so no barrier -> the time floor must be inactive (a tablet
  // with no CDC stream pinning it gets normal GC). This must be checked before
  // CreateCDCStream: stream creation pushes an initial barrier to every replica
  // right away.
  ASSERT_EQ(-1, leader->tablet_metadata()->cdc_min_retained_op_index());
  ASSERT_EQ(0, leader->GetRetentionIndexes().cdc_wal_retention_deadline_micros);

  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));

  // Persist a checkpoint so the master installs the barrier on the leader.
  ASSERT_OK(DoCheckpoint(leader_idx, stream_id, tablet_id, 1));
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(leader->tablet_metadata()->cdc_min_retained_op_index(), 0);
  });

  // With a barrier present and the flag > 0, the wall-clock deadline handed to
  // Log GC is --cdc_wal_retention_secs in the past.
  const int64_t before_micros = GetCurrentTimeMicros();
  const int64_t deadline_micros =
      leader->GetRetentionIndexes().cdc_wal_retention_deadline_micros;
  const int64_t after_micros = GetCurrentTimeMicros();
  const int64_t window_micros = 8LL * 3600 * 1000000;
  ASSERT_GE(deadline_micros, before_micros - window_micros);
  ASSERT_LE(deadline_micros, after_micros - window_micros);

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
// + GC, with the persisted superblock barrier shown to be load-bearing.
//
// Shape: consume a prefix and checkpoint at it (leaving unconsumed WAL beyond the
// checkpoint), wait for the barrier to persist on all replicas, write more, then
// kill the leader (the failover). Restart the surviving replicas so only their
// durable state is left -- see below -- then, on the new
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
//   1. In-memory state of the survivors. Restarting them (with the bg scan
//      frozen and no GetChanges since the kill) forces the barrier to be
//      restored from the durable superblock alone.
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
      // Require the barrier to have ADVANCED past the eager creation-time value
      // (op_index 0) to the durable consumer checkpoint. Accepting idx >= 0 here
      // would let the loop return on the eager 0 before any maintenance pass
      // installs the real barrier; the scan is frozen immediately after, so the
      // persisted floor would stay pinned at 0 and post-failover GC would reclaim
      // nothing (for_durability clamped to 0). The barrier must track the
      // checkpoint for the WAL below it to be reclaimable.
      ASSERT_GT(idx, 0) << "replica " << r->permanent_uuid()
                        << " has not advanced the barrier past the eager value";
      ASSERT_LE(idx, checkpoint);
    }
  });

  // Isolate the durability guarantee under test: the barrier must hold the WAL
  // as restored from the superblock, without the master's bg-scan re-push
  // (UpdateCDCRetentionBarrier) or a GetChanges served on the new leader before
  // the post-failover GC. Freezing the bg scan now -- after the barrier is
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

  // Restart the surviving replicas, so that only their durable state is left:
  // the bg scan is frozen and no GetChanges has hit these nodes since the kill.
  // The barrier is restored into metadata from the durable superblock at
  // bootstrap -- so after these restarts the persisted superblock barrier is the
  // ONLY thing that can hold the pre-checkpoint WAL. Restart one at a time to
  // keep a quorum recoverable. This is exactly the durability #1 guarantees:
  // retention survives without the master's coordination loop.
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

  // A leader must re-emerge among the restarted survivors.
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

// A barrier RELEASE that cannot reach a replica because its tablet server is
// down for longer than --unresponsive_ts_rpc_timeout_ms must be delivered once
// the server is back. The master used to forget the release when the task gave
// up, and the restarted replica kept its persisted barrier.
TEST_F(CDCFailoverITest, ReleaseDeliveredAfterTserverOutageLongerThanUnresponsiveTimeout) {
  FLAGS_unresponsive_ts_rpc_timeout_ms = 3000;
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
  const auto persisted_barrier = [&](int ts_idx, int64_t* barrier) {
    scoped_refptr<TabletReplica> replica;
    ASSERT_OK(cluster_->mini_tablet_server(ts_idx)->server()->tablet_manager()
        ->GetTabletReplica(tablet_id, &replica));
    *barrier = replica->tablet_metadata()->cdc_min_retained_op_index();
  };
  ASSERT_EVENTUALLY([&] {
    int64_t barrier = -2;
    NO_FATALS(persisted_barrier(follower_idx, &barrier));
    ASSERT_EQ(1, barrier);
  });

  // Take the follower down, then expire the stream. The release reaches the
  // leader, but the task to the follower gives up after the timeout.
  cluster_->mini_tablet_server(follower_idx)->Shutdown();
  FLAGS_cdc_stream_expiry_ms = 1;
  ASSERT_EVENTUALLY([&] {
    int64_t barrier = -2;
    NO_FATALS(persisted_barrier(leader_idx, &barrier));
    ASSERT_EQ(-1, barrier);
  });
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_unresponsive_ts_rpc_timeout_ms + 1000));

  // The follower comes back with its persisted barrier; the master must still
  // release it.
  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->Restart());
  ASSERT_EVENTUALLY([&] {
    int64_t barrier = -2;
    NO_FATALS(persisted_barrier(follower_idx, &barrier));
    ASSERT_EQ(-1, barrier) << "the restarted follower kept its barrier";
  });
}

// A stream that expires while the master is down must have its barrier
// released by the master once it is back. The restarted master used to start
// with no record of the barriers it had set and skipped the expired rows, so no
// RELEASE was ever sent.
TEST_F(CDCFailoverITest, ExpiryDuringMasterRestartReleasesBarrier) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
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
    for (const auto& r : GetAllReplicas(tablet_id)) {
      ASSERT_EQ(1, r->tablet_metadata()->cdc_min_retained_op_index());
    }
  });

  cluster_->mini_master()->Shutdown();
  FLAGS_cdc_stream_expiry_ms = 1;
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_OK(cluster_->mini_master()->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      kTimeout));
  ASSERT_EVENTUALLY([&] {
    const auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_EQ(-1, r->tablet_metadata()->cdc_min_retained_op_index())
          << "replica " << r->permanent_uuid() << " kept its barrier";
    }
  });
}

// A consumer that has read everything from an idle tablet cannot advance its
// checkpoint, but it is healthy: its polls must keep the barrier in place past
// --cdc_max_staleness_ms. Once it stops polling, the guard releases the barrier
// as before. The checkpoint persist throttle is left at its default: the last
// checkpoint before the tablet goes idle lands within the throttle interval of
// the one before it, and must still reach the master (F-74).
TEST_F(CDCFailoverITest, Staleness_IdleCaughtUpConsumerKeepsBarrier) {
  FLAGS_cdc_active_time_report_interval_ms = 100;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 3));
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
  const auto poll = [&](int64_t from, int64_t* checkpoint) {
    GetChangesRequestPB req;
    GetChangesResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from);
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    ASSERT_OK(proxy->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    *checkpoint = resp.checkpoint_op_index();
  };
  const auto checkpoint_at = [&](int64_t op_index) {
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
  // Read and checkpoint the first burst: the first checkpoint of the session
  // is persisted at once.
  int64_t first_checkpoint = 0;
  NO_FATALS(poll(0, &first_checkpoint));
  ASSERT_GT(first_checkpoint, 0);
  NO_FATALS(checkpoint_at(first_checkpoint));
  // Read and checkpoint the second, last burst right away, within the persist
  // interval: the throttle defers this checkpoint.
  NO_FATALS(InsertRows(3, 3));
  int64_t checkpoint = 0;
  NO_FATALS(poll(first_checkpoint, &checkpoint));
  ASSERT_GT(checkpoint, first_checkpoint);
  NO_FATALS(checkpoint_at(checkpoint));
  // The deferred checkpoint reaches the master once the interval ends.
  AssertEventually([&] {
    ASSERT_EQ(checkpoint, follower->tablet_metadata()->cdc_min_retained_op_index())
        << "the throttled trailing checkpoint was not persisted";
  }, MonoDelta::FromMilliseconds(FLAGS_cdc_checkpoint_persist_interval_ms) + kTimeout);
  NO_PENDING_FATALS();

  // Keep polling the idle tablet for well over the staleness window.
  FLAGS_cdc_max_staleness_ms = 2000;
  const MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(6);
  while (MonoTime::Now() < deadline) {
    int64_t next = -1;
    NO_FATALS(poll(checkpoint, &next));
    ASSERT_EQ(checkpoint, next);
    ASSERT_EQ(checkpoint, follower->tablet_metadata()->cdc_min_retained_op_index())
        << "the barrier of a caught-up consumer was released";
    SleepFor(MonoDelta::FromMilliseconds(50));
  }

  // A consumer that stops polling is released by the guard.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(-1, follower->tablet_metadata()->cdc_min_retained_op_index());
  });
}

// A tablet server that does not support CDC (for example, one not upgraded
// yet) must fail the master's barrier tasks right away instead of having them
// retried for --unresponsive_ts_rpc_timeout_ms. The test makes the tablet
// servers not advertise the CDC feature, so no barrier must be applied.
TEST_F(CDCFailoverITest, BarrierTaskToCdcUnawareTserverFailsFast) {
  FLAGS_tserver_support_cdc = false;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(WaitForMaintenancePasses(5));
  for (const auto& r : GetAllReplicas(tablet_id)) {
    ASSERT_EQ(-1, r->tablet_metadata()->cdc_min_retained_op_index())
        << "a barrier reached a tablet server without the CDC feature";
  }
  // Each task fails on its first attempt, so none keeps running, although
  // the deadline of a retried task is 30 s (--master_ts_rpc_timeout_ms).
  scoped_refptr<master::TableInfo> table;
  {
    master::CatalogManager::ScopedLeaderSharedLock l(
        cluster_->mini_master()->master()->catalog_manager());
    ASSERT_OK(l.first_failed_status());
    string table_id;
    ASSERT_OK(ResolveTableId(&table_id));
    ASSERT_OK(cluster_->mini_master()->master()->catalog_manager()->GetTableInfo(
        table_id, &table));
  }
  const MonoTime sample_end = MonoTime::Now() + MonoDelta::FromSeconds(4);
  while (MonoTime::Now() < sample_end) {
    vector<scoped_refptr<MonitoredTask>> tasks;
    table->GetTaskList(&tasks);
    for (const auto& t : tasks) {
      if (t->type_name() == "UpdateCDCRetentionBarrier" &&
          t->state() == MonitoredTask::kStateRunning) {
        ASSERT_LT((MonoTime::Now() - t->start_timestamp()).ToMilliseconds(), 2000)
            << t->description() << " is being retried";
      }
    }
    SleepFor(MonoDelta::FromMilliseconds(50));
  }

  // Once the tablet servers advertise CDC, the barrier is applied.
  FLAGS_tserver_support_cdc = true;
  ASSERT_EVENTUALLY([&] {
    for (const auto& r : GetAllReplicas(tablet_id)) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0);
    }
  });
}

// An unchanged barrier that a replica acknowledged is not sent again on every
// maintenance pass, only after --cdc_barrier_refresh_interval_ms or after the
// replica's tablet server restarted. The replica's barrier refresh time shows
// when it last received the barrier.
TEST_F(CDCFailoverITest, Maintenance_DoesNotResendUnchangedBarrier) {
  FLAGS_cdc_barrier_refresh_interval_ms = 3600 * 1000;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0);
    }
  });
  // Let the passes that react to the stream creation settle.
  NO_FATALS(WaitForMaintenancePasses(3));
  const auto refresh_times = [&]() {
    std::map<string, int64_t> out;
    for (const auto& r : GetAllReplicas(tablet_id)) {
      out[r->permanent_uuid()] = r->tablet_metadata()->cdc_barrier_refresh_micros();
    }
    return out;
  };
  const auto before = refresh_times();
  NO_FATALS(WaitForMaintenancePasses(10));
  ASSERT_EQ(before, refresh_times()) << "an unchanged barrier was sent again";

  // A restarted tablet server lost the in-memory part of the barrier, so it
  // gets the barrier again.
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  const string follower_uuid = cluster_->mini_tablet_server(follower_idx)->uuid();
  cluster_->mini_tablet_server(follower_idx)->Shutdown();
  ASSERT_OK(cluster_->mini_tablet_server(follower_idx)->Restart());
  ASSERT_EVENTUALLY([&] {
    auto now = refresh_times();
    ASSERT_EQ(3, now.size());
    ASSERT_GT(now[follower_uuid], before.at(follower_uuid));
  });
  const auto after_restart = refresh_times();
  for (const auto& e : after_restart) {
    if (e.first != follower_uuid) {
      ASSERT_EQ(before.at(e.first), e.second);
    }
  }

  // With a refresh interval of 0, every pass refreshes every replica.
  FLAGS_cdc_barrier_refresh_interval_ms = 0;
  ASSERT_EVENTUALLY([&] {
    auto now = refresh_times();
    for (const auto& e : after_restart) {
      ASSERT_GT(now[e.first], e.second);
    }
  });
}

// A finished barrier task holds its request, response and RPC buffers. The
// maintenance pass keeps only the outcome of a finished task, not the task,
// so a master with many pinned tablets does not keep a task per replica.
TEST_F(CDCFailoverITest, Maintenance_DoesNotHoldFinishedBarrierTasks) {
  FLAGS_cdc_barrier_refresh_interval_ms = 3600 * 1000;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0);
    }
  });
  NO_FATALS(WaitForMaintenancePasses(3));
  ASSERT_EQ(0, cluster_->mini_master()->master()->catalog_manager()->
                   NumFinishedCDCBarrierTasksHeldForTests(tablet_id));
}

// When the leader master's barrier sequence went backwards, as after
// 'kudu master unsafe_rebuild' (the rebuilt system catalog starts again at the
// first term), the replicas reject its updates only for
// --cdc_barrier_reorder_window_secs, and then apply them again.
TEST_F(CDCFailoverITest, BarrierRecoversAfterMasterSeqWentBack) {
  FLAGS_cdc_barrier_reorder_window_secs = 10;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0);
    }
  });
  // Let the master record every replica as up to date, so it would not send
  // to them again before --cdc_barrier_refresh_interval_ms.
  NO_FATALS(WaitForMaintenancePasses(3));

  // The last update of a lost system catalog, with a term far above the
  // current master's: it releases the barrier.
  constexpr int64_t kLostCatalogSeq = int64_t{1000} << 32;
  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    ASSERT_OK(cluster_->mini_tablet_server(i)->server()->cdc_service()->SetRetentionBarrier(
        tablet_id, /*min_retained_op_index=*/-1, /*history_safe_time_micros=*/0,
        kLostCatalogSeq));
  }
  // Like a rebuilt master, the restarted one starts with no record of what it
  // sent to the replicas, and with a new term still far below the lost one.
  cluster_->mini_master()->Shutdown();
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_OK(cluster_->mini_master()->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      kTimeout));
  // Within the window, the master's updates are superseded.
  NO_FATALS(WaitForMaintenancePasses(3));
  for (const auto& r : GetAllReplicas(tablet_id)) {
    ASSERT_EQ(-1, r->tablet_metadata()->cdc_min_retained_op_index());
  }
  // After it, they are applied again.
  ASSERT_EVENTUALLY([&] {
    for (const auto& r : GetAllReplicas(tablet_id)) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0)
          << "the replica still rejects the master's barrier";
      ASSERT_LT(r->tablet_metadata()->cdc_barrier_seq(), kLostCatalogSeq);
    }
  });
}

// Barrier updates are ordered by the leader master's term first, so after a
// leader change the new leader's updates are applied even if the previous
// leader stamped its last update with a much later sequence, as a wall-clock
// sequence would after the old leader's clock ran an hour ahead.
TEST_F(CDCFailoverITest, BarrierSeqOfNewLeaderOutranksPreviousLeader) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  int64_t seq = 0;
  ASSERT_EVENTUALLY([&] {
    auto replicas = GetAllReplicas(tablet_id);
    ASSERT_EQ(3, replicas.size());
    for (const auto& r : replicas) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0);
      seq = std::max(seq, r->tablet_metadata()->cdc_barrier_seq());
    }
  });
  ASSERT_GT(seq, 0);

  // The previous leader's last update: a release stamped an hour "later".
  const int64_t skewed_seq = seq + 3600LL * 1000 * 1000;
  for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
    ASSERT_OK(cluster_->mini_tablet_server(i)->server()->cdc_service()->SetRetentionBarrier(
        tablet_id, /*min_retained_op_index=*/-1, /*history_safe_time_micros=*/0, skewed_seq));
  }
  for (const auto& r : GetAllReplicas(tablet_id)) {
    ASSERT_EQ(-1, r->tablet_metadata()->cdc_min_retained_op_index());
  }

  // A new leadership term: the new leader's barrier is applied on every replica.
  cluster_->mini_master()->Shutdown();
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_EVENTUALLY([&] {
    for (const auto& r : GetAllReplicas(tablet_id)) {
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0)
          << "the new leader's barrier was dropped as superseded";
      ASSERT_GT(r->tablet_metadata()->cdc_barrier_seq(), skewed_seq);
    }
  });
}

// A leader copies the tablet over a running follower that fell behind its WAL.
// The follower holds the CDC retention barrier the master pushed to it; the
// copy shuts the old replica down and destroys it, which must not trip
// ~LogAnchorRegistry's CHECK that no anchor is left. The new replica takes the
// barrier over with the copied superblock.
TEST_F(CDCFailoverITest, TabletCopyOverReadyFollowerWithCdcBarrier_NoCrash) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 3));
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(GetReplica(follower_idx, tablet_id)->tablet_metadata()
                  ->cdc_min_retained_op_index(), 0);
  });

  NO_FATALS(CopyTabletFromLeader(leader_idx, follower_idx, tablet_id));
  scoped_refptr<TabletReplica> copied = GetReplica(follower_idx, tablet_id);
  EXPECT_GE(copied->tablet_metadata()->cdc_min_retained_op_index(), 0);
  EXPECT_FALSE(HasCDCLogAnchor(copied)) << copied->log_anchor_registry()->DumpAnchorInfo();
}

// Like TabletCopyOverReadyFollowerWithCdcBarrier_NoCrash, for a replica that
// is tombstoned (e.g. by a failed earlier copy) when the master pushes the
// barrier to it, and is then copied again.
TEST_F(CDCFailoverITest, TabletCopyOverTombstonedReplicaWithCdcBarrier_NoCrash) {
  // Copy only when this test asks for it.
  FLAGS_enable_tablet_copy = false;
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 3));
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  const int follower_idx = FindFollowerIndex(tablet_id);
  ASSERT_GE(follower_idx, 0);
  auto* follower_ts = cluster_->mini_tablet_server(follower_idx);
  ASSERT_OK(itest::DeleteTablet(ts_map_[follower_ts->uuid()], tablet_id,
                                tablet::TABLET_DATA_TOMBSTONED, kTimeout));
  ASSERT_OK(follower_ts->server()->cdc_service()->SetRetentionBarrier(
      tablet_id, /*min_retained_op_index=*/1));

  NO_FATALS(CopyTabletFromLeader(leader_idx, follower_idx, tablet_id));
  scoped_refptr<TabletReplica> copied = GetReplica(follower_idx, tablet_id);
  EXPECT_GE(copied->tablet_metadata()->cdc_min_retained_op_index(), 0);
  EXPECT_FALSE(HasCDCLogAnchor(copied)) << copied->log_anchor_registry()->DumpAnchorInfo();
}

namespace {
// Blocks the first append to a log after its data was written and before it
// was indexed, until Release() is called.
class BlockingAppendHooks : public log::LogFaultHooks {
 public:
  Status PostAppend() override {
    blocked_.CountDown();
    release_.Wait();
    return Status::OK();
  }
  void WaitUntilBlocked() { blocked_.Wait(); }
  void Release() { release_.CountDown(); }

 private:
  CountDownLatch blocked_{1};
  CountDownLatch release_{1};
};
} // anonymous namespace

// A leader writes an op to its own WAL while it replicates it, so the op can
// be committed by the followers, and applied, before it is in the leader's
// WAL. GetChanges must not take the missing op for one that was GCed: that
// would stop the consumer for a new snapshot of a tablet whose WAL is whole.
TEST_F(CDCFailoverITest, GetChanges_CommittedOpNotYetInLeaderWal_NotExpired) {
  NO_FATALS(StartClusterWithRF3());
  const string tablet_id = GetTabletId();
  string stream_id;
  ASSERT_OK(CreateStream(&stream_id));
  NO_FATALS(InsertRows(0, 1));
  int leader_idx = -1;
  ASSERT_EVENTUALLY([&] {
    leader_idx = FindLeaderIndex(tablet_id);
    ASSERT_GE(leader_idx, 0);
  });
  vector<int32_t> keys;
  int64_t checkpoint = 0;
  NO_FATALS(ReadInsertKeys(leader_idx, stream_id, tablet_id, 0, &keys, &checkpoint));
  ASSERT_EQ(vector<int32_t>{ 0 }, keys);

  scoped_refptr<TabletReplica> leader = GetReplica(leader_idx, tablet_id);
  auto hooks = std::make_shared<BlockingAppendHooks>();
  leader->log()->SetLogFaultHooksForTests(hooks);
  SCOPED_CLEANUP({
    hooks->Release();
    leader->log()->SetLogFaultHooksForTests(nullptr);
  });
  std::thread writer([&] { InsertRows(1, 1); });
  SCOPED_CLEANUP({
    hooks->Release();
    writer.join();
  });
  hooks->WaitUntilBlocked();
  // The followers commit the write.
  int64_t committed = -1;
  ASSERT_EVENTUALLY([&] {
    committed = leader->shared_consensus()->GetLastOpId(consensus::COMMITTED_OPID)->index();
    ASSERT_GT(committed, checkpoint);
  });

  // A consumer positioned at the committed op, e.g. by a snapshot, keeps its
  // position: it is not moved back to the end of the leader's WAL.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(leader_idx, stream_id, tablet_id, committed, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.records_size());
    ASSERT_EQ(committed, resp.checkpoint_op_index());
  }

  // The committed op is not in the leader's WAL yet: the stream has nothing
  // to return past what the WAL has, or a retryable error, but its position
  // did not expire.
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(leader_idx, stream_id, tablet_id, checkpoint, &resp));
  if (resp.has_error()) {
    ASSERT_NE(CDCErrorPB::WAL_EXPIRED, resp.error().code()) << resp.error().DebugString();
    ASSERT_NE(CDCErrorPB::STREAM_EXPIRED, resp.error().code()) << resp.error().DebugString();
  } else {
    ASSERT_EQ(0, CountInserts(resp));
  }

  // Once the op is in the WAL, the stream returns it.
  hooks->Release();
  keys.clear();
  ASSERT_EVENTUALLY([&] {
    keys.clear();
    NO_FATALS(ReadInsertKeys(leader_idx, stream_id, tablet_id, checkpoint, &keys));
    ASSERT_EQ(vector<int32_t>{ 1 }, keys);
  });
}

} // namespace kudu
