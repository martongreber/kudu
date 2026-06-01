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
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include <vector>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.proxy.h"
#include "kudu/cdc/cdc_service.h"
#include "kudu/cdc/cdc_util.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/master/master.pb.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/server/rpc_server.h"
#include "kudu/security/crypto.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_verifier.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/ops/alter_schema_op.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/tablet/txn_participant-test-util.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/metrics.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(tserver_enforce_access_control);
DECLARE_bool(enable_txn_partition_lock);
DECLARE_int64(rpc_max_message_size);
DECLARE_bool(cdc_inject_full_reconstruction_failure);
DECLARE_bool(cdc_inject_full_reconstruction_timeout);
DECLARE_int64(cdc_inject_full_reconstruction_timeout_at_op_index);
DECLARE_int32(tablet_inject_latency_on_apply_write_op_ms);
DECLARE_int32(tablet_inject_latency_on_apply_alter_schema_op_ms);
DECLARE_int32(cdc_stream_config_cache_ttl_ms);
DECLARE_int32(cdc_stream_config_negative_cache_ttl_ms);
DECLARE_int32(cdc_snapshot_wait_timeout_ms);
DECLARE_int32(cdc_inject_latency_before_snapshot_establish_ms);
DECLARE_int32(cdc_inject_latency_before_stream_config_fetch_ms);
DECLARE_int32(cdc_inject_latency_before_session_update_ms);
DECLARE_int32(cdc_inject_latency_before_decode_ms);
DECLARE_int32(delete_tablet_inject_latency_after_transition_ms);
DECLARE_double(cdc_read_safe_deadline_ratio);
DECLARE_double(cdc_get_changes_free_rpc_ratio);
DECLARE_int32(rpc_num_service_threads);
DECLARE_int32(rpc_service_queue_length);
DECLARE_int32(cdc_svc_queue_length);
DECLARE_bool(cdc_inject_tablet_not_running);
DECLARE_bool(cdc_inject_post_read_leadership_loss);
DECLARE_bool(cdc_inject_server_memory_pressure);
DECLARE_bool(cdc_inject_checkpoint_persist_failure);
DECLARE_int64(cdc_snapshot_max_bytes_per_response);
DECLARE_int32(cdc_max_concurrent_scans);
DECLARE_int64(cdc_scan_mem_limit_bytes);
DECLARE_int64(cdc_max_bytes_per_response);
DECLARE_int64(cdc_max_transaction_span_bytes);
DECLARE_int32(log_min_segments_to_retain);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_int64(cdc_stream_idle_expiry_ms);
DECLARE_int64(cdc_session_history_floor_max_age_secs);
DECLARE_int64(cdc_checkpoint_persist_interval_ms);
DECLARE_bool(cdc_inject_commit_lookup_deadline_expired);
DECLARE_bool(cdc_inject_wal_read_deadline_expired);
DECLARE_bool(cdc_inject_escalation_deadline_expired);
DECLARE_bool(cdc_inject_snapshot_scan_deadline_expired);
DECLARE_int64(cdc_wal_read_mem_limit_bytes);
DECLARE_int32(cdc_max_records_per_response);
DECLARE_int32(max_cell_size_bytes);
DECLARE_int64(cdc_inject_decode_failure_at_op_index);
DECLARE_int32(cdc_master_rpc_timeout_ms);
DECLARE_int64(cdc_active_time_report_interval_ms);
DECLARE_int32(cdc_barrier_reorder_window_secs);
DECLARE_int64(cdc_barrier_lease_secs);
DECLARE_int64(cdc_max_wal_retention_secs);
DECLARE_int64(cdc_stop_retaining_min_disk_mb);
DECLARE_bool(tablet_inject_omit_alter_prior_schema_version);
DECLARE_double(env_inject_eio);
DECLARE_string(env_inject_eio_globs);

METRIC_DECLARE_entity(cdc_stream);
METRIC_DECLARE_counter(cdc_errors_decode_failed);
METRIC_DECLARE_counter(cdc_wal_reads_rejected_memory);
METRIC_DECLARE_counter(cdc_wal_reads_rejected_outcome_pending);
METRIC_DECLARE_counter(cdc_wal_read_escalations);
METRIC_DECLARE_counter(cdc_write_ops_decoded);
METRIC_DECLARE_counter(cdc_full_image_rows_read);
METRIC_DECLARE_gauge_int64(cdc_stream_sent_lag_micros);
METRIC_DECLARE_gauge_int64(cdc_stream_active_age_micros);
METRIC_DECLARE_counter(cdc_checkpoint_requests);
METRIC_DECLARE_counter(cdc_get_changes_requests);
METRIC_DECLARE_counter(cdc_checkpoint_persists);
METRIC_DECLARE_counter(cdc_checkpoint_persist_failures);
METRIC_DECLARE_counter(cdc_barrier_updates_superseded);
METRIC_DECLARE_counter(cdc_activity_reports);
METRIC_DECLARE_counter(cdc_scans_rejected_server_memory);
METRIC_DECLARE_counter(cdc_bytes_sent);
METRIC_DECLARE_gauge_int64(cdc_stream_time_to_expiry_micros);
METRIC_DECLARE_histogram(cdc_get_changes_response_bytes);
METRIC_DECLARE_gauge_int64(cdc_stream_committed_lag_micros);
METRIC_DECLARE_gauge_int64(cdc_stream_ops_behind);

using kudu::clock::HybridClock;
using kudu::rpc::RpcController;
using kudu::security::PrivateKey;
using kudu::security::SignedTokenPB;
using kudu::security::TablePrivilegePB;
using kudu::security::TokenSigner;
using kudu::security::TokenSigningPrivateKeyPB;
using kudu::security::TokenSigningPublicKeyPB;
using kudu::security::TokenVerifier;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::ParticipantResponsePB;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cdc {

class CDCServiceTest : public tserver::TabletServerTestBase {
 public:
  void TearDown() override {
    if (mini_server_ && mini_server_->server()) {
      mini_server_->server()->cdc_service()->SetSnapshotCursorFetchHookForTests(nullptr);
    }
    TabletServerTestBase::TearDown();
  }

  void SetUp() override {
    TabletServerTestBase::SetUp();
    // This fixture has no reachable master, so every fetch of a stream config
    // that is not seeded fails. Do not remember the failures: the tests that do
    // not seed a config rely on each call being served without one.
    FLAGS_cdc_stream_config_negative_cache_ttl_ms = 0;
    StartTabletServer(/*num_data_dirs=*/1);

    cdc_proxy_.reset(new CDCServiceProxy(
        client_messenger_,
        mini_server_->bound_rpc_addr(),
        mini_server_->bound_rpc_addr().host()));
    // The tablet server serves a stream only if it knows its config. Most
    // tests use this one; they seed another config to change it.
    SeedStreamConfig(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
    InstallSnapshotCursorFetchHook();
  }

 protected:
  Status DoGetChanges(const string& tablet_id, int64_t from_op_index,
                      GetChangesResponsePB* resp, int64_t max_bytes = 0,
                      bool need_schema_info = false,
                      const security::SignedTokenPB* authz_token = nullptr) {
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(from_op_index);
    if (max_bytes > 0) {
      req.set_max_bytes(max_bytes);
    }
    if (need_schema_info) {
      req.set_need_schema_info(true);
    }
    if (authz_token) {
      *req.mutable_authz_token() = *authz_token;
    }

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    return cdc_proxy_->GetChanges(req, resp, &rpc);
  }

  // Issues GetChanges from 'from_op_index' with the declared schema version
  // 'declared_version' (none if negative).
  Status DoGetChangesWithSchemaVersion(int64_t from_op_index, int32_t declared_version,
                                       bool need_schema_info, GetChangesResponsePB* resp) {
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(from_op_index);
    if (declared_version >= 0) {
      req.set_schema_version(declared_version);
    }
    if (need_schema_info) {
      req.set_need_schema_info(true);
    }
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    return cdc_proxy_->GetChanges(req, resp, &rpc);
  }

  Status DoCheckpoint(const string& tablet_id, int64_t op_index,
                      CheckpointResponsePB* resp,
                      const security::SignedTokenPB* authz_token = nullptr) {
    CheckpointRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);
    if (authz_token) {
      *req.mutable_authz_token() = *authz_token;
    }

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    return cdc_proxy_->Checkpoint(req, resp, &rpc);
  }

  // Whether the CDC service holds a WAL anchor on the tablet replica. The CDC
  // service anchors the WAL only for the duration of a read: the master's
  // retention barrier, persisted in the tablet metadata, is the only long-lived
  // CDC retention.
  bool HasCDCLogAnchor() const {
    return tablet_replica_->log_anchor_registry()->DumpAnchorInfo().find("owner=CDC") !=
           string::npos;
  }

  // Reads a server-entity CDC counter by its metric prototype. Returns -1 if the
  // counter has not been instantiated.
  int64_t CDCCounterValue(const CounterPrototype& proto) {
    scoped_refptr<Metric> m =
        mini_server_->server()->metric_entity()->FindOrNull(proto);
    if (!m) {
      return -1;
    }
    return down_cast<Counter*>(m.get())->value();
  }

  // Returns the server-entity histogram for 'proto', instantiating it if needed.
  // The returned scoped_refptr is valid for the lifetime of this test.
  scoped_refptr<Histogram> CDCHistogram(HistogramPrototype& proto) {
    return proto.Instantiate(mini_server_->server()->metric_entity());
  }

  // Drives one snapshot page. 'is_start' begins a new snapshot; 'resume_key'
  // continues a paginated one.
  Status DoSnapshot(const string& tablet_id, bool is_start,
                    const string& resume_key, GetChangesResponsePB* resp,
                    int64_t max_bytes = 0,
                    MonoDelta timeout = MonoDelta::FromSeconds(10)) {
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(tablet_id);
    req.set_is_snapshot_start(is_start);
    if (!resume_key.empty()) {
      req.set_snapshot_resume_key(resume_key);
    }
    if (max_bytes > 0) {
      req.set_max_bytes(max_bytes);
    }
    RpcController rpc;
    rpc.set_timeout(timeout);
    return cdc_proxy_->GetChanges(req, resp, &rpc);
  }

  // Inserts a single row with the given primary key via the tserver Write RPC.
  // Unlike InsertTestRowsRemote (which computes first_row + count and thus
  // overflows at INT32_MAX), this inserts one explicit key, so it can seed the
  // maximum-valued key needed by the E5 wedge regression test.
  void InsertOneRow(int32_t key) {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
    AddTestRowWithNullableStringToPB(RowOperationsPB::INSERT, schema_, key, key,
                                     "maxkey", req.mutable_row_operations());
    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }

  // Issues an AlterSchema RPC appending nullable INT32 column 'col_name' to
  // 'base', targeting schema version 'new_version'. Returns the resulting schema
  // so successive alters can chain off it. The RPC is synchronous and returns
  // only after the op is applied -- unless apply latency is injected, in which
  // case the caller should run this on a background thread. Writes the RPC status
  // to 'status_out' if given, else CHECK_OKs it.
  Schema AlterAddColumn(const Schema& base, const string& col_name,
                        int32_t new_version, Status* status_out = nullptr) {
    SchemaBuilder builder(base);
    CHECK_OK(builder.AddNullableColumn(col_name, INT32));
    Schema new_schema = builder.Build();

    tserver::AlterSchemaRequestPB req;
    tserver::AlterSchemaResponsePB resp;
    req.set_dest_uuid(mini_server_->server()->fs_manager()->uuid());
    req.set_tablet_id(kTabletId);
    req.set_schema_version(new_version);
    CHECK_OK(SchemaToPB(new_schema, req.mutable_schema()));
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(30));
    Status s = admin_proxy_->AlterSchema(req, &resp, &rpc);
    if (s.ok() && resp.has_error()) {
      s = StatusFromPB(resp.error().status());
    }
    if (status_out) {
      *status_out = s;
    } else {
      CHECK_OK(s);
    }
    return new_schema;
  }

  // Alters the test tablet's schema to 'schema' (with column ids) at version
  // 'version' with an AlterSchema RPC.
  Status DoAlterSchema(const Schema& schema, uint32_t version) {
    tserver::AlterSchemaRequestPB req;
    tserver::AlterSchemaResponsePB resp;
    req.set_dest_uuid(mini_server_->server()->fs_manager()->uuid());
    req.set_tablet_id(kTabletId);
    req.set_schema_version(version);
    RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(30));
    RETURN_NOT_OK(admin_proxy_->AlterSchema(req, &resp, &rpc));
    return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
  }

  // Writes the rows 'ops', encoded with the client schema 'client_schema' (no
  // column ids), in one Write RPC, and expects the tablet to apply them all.
  void WriteOps(const Schema& client_schema, const RowOperationsPB& ops) {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(client_schema, req.mutable_schema()));
    *req.mutable_row_operations() = ops;
    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }

  // Returns the row records (INSERT/UPDATE/DELETE/UPSERT) of 'resp'.
  static vector<const CDCRecordPB*> RowRecords(const GetChangesResponsePB& resp) {
    vector<const CDCRecordPB*> rows;
    for (const auto& r : resp.records()) {
      if (r.op_type() == CDCOpTypePB::INSERT || r.op_type() == CDCOpTypePB::UPDATE ||
          r.op_type() == CDCOpTypePB::DELETE || r.op_type() == CDCOpTypePB::UPSERT) {
        rows.push_back(&r);
      }
    }
    return rows;
  }

  // Seed the CDCService's stream-config cache so tests exercise FULL-mode and
  // snapshot behavior without a live master.
  void SeedStreamConfig(CDCStreamConfigPB::RecordType record_type,
                        CDCStreamConfigPB::SnapshotMode snapshot_mode =
                            CDCStreamConfigPB::NEVER,
                        const string& stream_id = "test-stream-1") {
    CDCStreamConfigPB config;
    config.set_record_type(record_type);
    config.set_snapshot_mode(snapshot_mode);
    mini_server_->server()->cdc_service()->SetStreamConfigForTests(
        stream_id, config, { TableId() });
  }

  // The id of the table of the test tablet.
  string TableId() const {
    return tablet_replica_->tablet_metadata()->table_id();
  }

  // Pins a retention barrier at op index 0 on the test tablet, as the master
  // does when it creates a stream. A FULL session holds the history floors of
  // the batches it serves only while the tablet has a barrier.
  void PinRetentionBarrier() {
    ASSERT_OK(mini_server_->server()->cdc_service()->SetRetentionBarrier(
        kTabletId, /*min_retained_op_index=*/0));
  }

  // GetChanges for an explicit stream id (DoGetChanges uses "test-stream-1").
  Status DoGetChangesForStream(const string& stream_id, int64_t from_op_index,
                               GetChangesResponsePB* resp) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(from_op_index);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    return cdc_proxy_->GetChanges(req, resp, &rpc);
  }

  // Restarts the tablet server (the tablet is re-opened from its superblock and
  // WAL) and reconnects the CDC proxy. The stream-config cache is lost: it is
  // re-seeded as in SetUp(), and a caller that seeded another config re-seeds
  // it.
  Status RestartTabletServer() {
    RETURN_NOT_OK(ShutdownAndRebuildTablet());
    cdc_proxy_.reset(new CDCServiceProxy(
        client_messenger_,
        mini_server_->bound_rpc_addr(),
        mini_server_->bound_rpc_addr().host()));
    SeedStreamConfig(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
    InstallSnapshotCursorFetchHook();
    return Status::OK();
  }

  // This fixture has no master: the tablet server reads the durable snapshot
  // cursor of a tablet from these instead (see
  // CDCServiceImpl::SetSnapshotCursorFetchHookForTests()). By default the
  // master has no cursor.
  void SetDurableSnapshotCursor(const SnapshotCursorUpdate& cursor,
                                int64_t streaming_start_op_index) {
    std::lock_guard<std::mutex> l(durable_cursor_lock_);
    durable_cursor_ = cursor;
    durable_cursor_streaming_start_ = streaming_start_op_index;
  }
  void SetDurableSnapshotCursorFetchStatus(const Status& s) {
    std::lock_guard<std::mutex> l(durable_cursor_lock_);
    durable_cursor_fetch_status_ = s;
  }
  int DurableSnapshotCursorFetches() {
    std::lock_guard<std::mutex> l(durable_cursor_lock_);
    return durable_cursor_fetches_;
  }
  void InstallSnapshotCursorFetchHook() {
    mini_server_->server()->cdc_service()->SetSnapshotCursorFetchHookForTests(
        [this](const string& /*stream_id*/, const string& /*tablet_id*/, bool* found,
               SnapshotCursorUpdate* cursor, int64_t* streaming_start_op_index) {
          std::lock_guard<std::mutex> l(durable_cursor_lock_);
          ++durable_cursor_fetches_;
          RETURN_NOT_OK(durable_cursor_fetch_status_);
          *found = durable_cursor_.active;
          *cursor = durable_cursor_;
          *streaming_start_op_index = durable_cursor_streaming_start_;
          return Status::OK();
        });
  }

  // Returns the before-image int_val of the first UPDATE in 'resp', or -1 if
  // there is no UPDATE with a before-image int_val.
  static int32_t FirstUpdateBeforeIntVal(const GetChangesResponsePB& resp) {
    for (const auto& r : resp.records()) {
      int32_t v = -1;
      if (r.op_type() == CDCOpTypePB::UPDATE &&
          GetInt32Col(r, /*before=*/true, "int_val", &v)) {
        return v;
      }
    }
    return -1;
  }

  // Extract an INT32 column value by name from a record's after-image
  // ('before'=false) or before-image ('before'=true). Returns false if absent
  // or null.
  static bool GetInt32Col(const CDCRecordPB& r, bool before,
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

  // Returns the column-value entry named 'name' from a record's before-image
  // ('before'=true) or after-image ('before'=false), or nullptr if the column
  // is absent from that image.
  static const CDCColumnValuePB* FindCol(const CDCRecordPB& r, bool before,
                                         const string& name) {
    const auto& cols = before ? r.old_changes() : r.changes();
    for (const auto& c : cols) {
      if (c.column_name() == name) return &c;
    }
    return nullptr;
  }

  // Runs participant op 'type' of transaction 'txn_id' on the test tablet.
  Status DoParticipantOp(int64_t txn_id, ParticipantOpPB::ParticipantOpType type,
                         int64_t commit_ts = -1) {
    ParticipantResponsePB resp;
    RETURN_NOT_OK(tablet::CallParticipantOp(tablet_replica_.get(), txn_id, type, commit_ts,
                                            &resp));
    return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
  }

  // Commits transaction 'txn_id', of which the test tablet is the only
  // participant: such a transaction commits at its BEGIN_COMMIT timestamp.
  Status CommitTxn(int64_t txn_id) {
    RETURN_NOT_OK(DoParticipantOp(txn_id, ParticipantOpPB::BEGIN_COMMIT));
    const auto txn_meta =
        FindOrDie(tablet_replica_->tablet_metadata()->GetTxnMetadata(), txn_id);
    if (!txn_meta->commit_mvcc_op_timestamp()) {
      return Status::IllegalState("no BEGIN_COMMIT timestamp");
    }
    return DoParticipantOp(txn_id, ParticipantOpPB::FINALIZE_COMMIT,
                           txn_meta->commit_mvcc_op_timestamp()->value());
  }

  // Applies one write op of type 'type' with a row for each of 'keys' (int_val
  // = key + 'int_val_delta', string_val = 'value_size' bytes, or not set if 0),
  // in transaction 'txn_id' or, if it is negative, outside of any transaction.
  // The op is submitted to the replica directly, so its size is not bounded by
  // --rpc_max_message_size.
  void SubmitRows(RowOperationsPB::Type type, const vector<int32_t>& keys,
                  int64_t txn_id = -1, size_t value_size = 8, int32_t int_val_delta = 0) {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    if (txn_id >= 0) {
      req.set_txn_id(txn_id);
    }
    ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
    const string value(value_size, 'x');
    for (int32_t key : keys) {
      AddTestRowWithNullableStringToPB(type, schema_, key, key + int_val_delta,
                                       value_size > 0 ? value.c_str() : nullptr,
                                       req.mutable_row_operations());
    }
    tserver::WriteResponsePB resp;
    unique_ptr<tablet::WriteOpState> op_state(
        new tablet::WriteOpState(tablet_replica_.get(), &req, nullptr, &resp));
    CountDownLatch latch(1);
    op_state->set_completion_callback(unique_ptr<tablet::OpCompletionCallback>(
        new tablet::LatchOpCompletionCallback<tserver::WriteResponsePB>(&latch, &resp)));
    ASSERT_OK(tablet_replica_->SubmitWrite(std::move(op_state)));
    latch.Wait();
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }

  // Keys [first, first + count).
  static vector<int32_t> Keys(int32_t first, int32_t count) {
    vector<int32_t> keys(count);
    for (int32_t i = 0; i < count; ++i) {
      keys[i] = first + i;
    }
    return keys;
  }

  // Streams the test tablet after 'from_op_index', asking for 'max_bytes' per
  // call and following the checkpoint until the stream is caught up. Appends
  // the keys of the INSERT records to 'keys' and returns the last checkpoint in
  // 'checkpoint' if given.
  void StreamInsertKeys(int64_t from_op_index, int64_t max_bytes, vector<int32_t>* keys,
                        int64_t* checkpoint = nullptr) {
    for (int i = 0; i < 1000; ++i) {
      GetChangesResponsePB resp;
      ASSERT_OK(DoGetChanges(kTabletId, from_op_index, &resp, max_bytes));
      ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
      for (const auto& r : resp.records()) {
        int32_t k = -1;
        if (r.op_type() == CDCOpTypePB::INSERT) {
          ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k)) << r.DebugString();
          keys->push_back(k);
        }
      }
      const bool progressed = resp.checkpoint_op_index() > from_op_index;
      from_op_index = std::max(from_op_index, resp.checkpoint_op_index());
      if (!progressed && !resp.have_more_records()) {
        if (checkpoint) {
          *checkpoint = from_op_index;
        }
        return;
      }
    }
    FAIL() << "the stream did not catch up";
  }

  // The checkpoint of a stream that is caught up with the test tablet.
  int64_t CaughtUpCheckpoint() {
    vector<int32_t> keys;
    int64_t checkpoint = -1;
    StreamInsertKeys(0, /*max_bytes=*/0, &keys, &checkpoint);
    return checkpoint;
  }

  // Streams the test tablet after 'from_op_index' with the COMMIT lookup of
  // every call cut by the deadline after its first op, following the
  // checkpoint until the stream is caught up. Appends the op types of the
  // records to 'ops' and the keys of the INSERTs to 'keys'. Every call must
  // either advance the checkpoint, or not ask for an immediate re-poll: one
  // that does neither would get the same response forever.
  void PollWithCommitLookupCut(int64_t from_op_index, vector<CDCOpTypePB>* ops,
                               vector<int32_t>* keys) {
    SCOPED_CLEANUP({ FLAGS_cdc_inject_commit_lookup_deadline_expired = false; });
    FLAGS_cdc_inject_commit_lookup_deadline_expired = true;
    for (int i = 0; i < 20; ++i) {
      GetChangesResponsePB resp;
      ASSERT_OK(DoGetChanges(kTabletId, from_op_index, &resp));
      ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
      for (const auto& r : resp.records()) {
        ops->push_back(r.op_type());
        int32_t k = -1;
        if (r.op_type() == CDCOpTypePB::INSERT) {
          ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k)) << r.DebugString();
          keys->push_back(k);
        }
      }
      const bool progressed = resp.checkpoint_op_index() > from_op_index;
      ASSERT_TRUE(progressed || !resp.have_more_records())
          << "call " << i << " from op index " << from_op_index
          << " neither advanced nor ended: " << resp.ShortDebugString();
      from_op_index = std::max(from_op_index, resp.checkpoint_op_index());
      if (!progressed) {
        return;
      }
    }
    FAIL() << "the stream did not catch up";
  }

  unique_ptr<CDCServiceProxy> cdc_proxy_;

  // See SetDurableSnapshotCursor().
  std::mutex durable_cursor_lock_;
  SnapshotCursorUpdate durable_cursor_;
  int64_t durable_cursor_streaming_start_ = 0;
  Status durable_cursor_fetch_status_;
  int durable_cursor_fetches_ = 0;
};

TEST_F(CDCServiceTest, ServiceReachable) {
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  // The service should respond (no connection-level error).
  // It may or may not have records depending on bootstrap ops.
}

TEST_F(CDCServiceTest, GetChanges_AfterInserts) {
  // Insert 5 rows.
  InsertTestRowsRemote(0, 5, 5);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  // Count INSERT records.
  int insert_count = 0;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::INSERT) {
      insert_count++;
    }
  }
  EXPECT_EQ(5, insert_count);
  EXPECT_GT(resp.checkpoint_op_index(), 0);
}

TEST_F(CDCServiceTest, GetChanges_MultiRowBatch) {
  // Insert 5 rows in a single batch (num_batches=1).
  InsertTestRowsRemote(0, 5, /*num_batches=*/1);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  int insert_count = 0;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::INSERT) {
      insert_count++;
    }
  }
  // All 5 rows must be decoded even though they were in a single WriteRequestPB.
  EXPECT_EQ(5, insert_count);
}

TEST_F(CDCServiceTest, GetChanges_Pagination) {
  InsertTestRowsRemote(0, 10, 10);

  // First call: read from beginning to get first batch.
  GetChangesResponsePB resp1;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp1));
  ASSERT_FALSE(resp1.has_error()) << resp1.error().DebugString();
  int64_t checkpoint_all = resp1.checkpoint_op_index();
  ASSERT_GT(checkpoint_all, 0);

  // Count total inserts in the full response.
  int total_inserts_full = 0;
  for (int i = 0; i < resp1.records_size(); ++i) {
    if (resp1.records(i).op_type() == CDCOpTypePB::INSERT) total_inserts_full++;
  }
  ASSERT_EQ(10, total_inserts_full);

  // Now test pagination: read in two halves using a midpoint checkpoint.
  // Find the op_index of the 5th INSERT record.
  int insert_count = 0;
  int64_t mid_checkpoint = 0;
  for (int i = 0; i < resp1.records_size(); ++i) {
    if (resp1.records(i).op_type() == CDCOpTypePB::INSERT) {
      insert_count++;
      if (insert_count == 5) {
        mid_checkpoint = resp1.records(i).op_index();
        break;
      }
    }
  }
  ASSERT_GT(mid_checkpoint, 0);

  // Read from mid_checkpoint -- should get remaining inserts.
  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(kTabletId, mid_checkpoint, &resp2));
  ASSERT_FALSE(resp2.has_error()) << resp2.error().DebugString();

  int remaining_inserts = 0;
  for (int i = 0; i < resp2.records_size(); ++i) {
    if (resp2.records(i).op_type() == CDCOpTypePB::INSERT) remaining_inserts++;
  }
  EXPECT_EQ(5, remaining_inserts);
}

TEST_F(CDCServiceTest, GetChanges_TabletNotFound) {
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges("nonexistent-tablet-id", 0, &resp));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_FOUND, resp.error().code());
}

// have_more_records must be true when the per-response byte budget cut the read
// short of the committed watermark (so the consumer keeps polling now), and
// false once the batch reaches the committed head (caught up, poll normally).
TEST_F(CDCServiceTest, GetChanges_HaveMoreRecords) {
  InsertTestRowsRemote(0, 10, 10);

  // Tiny byte budget: the read returns the first record(s) and stops short of
  // the committed head -> more records are immediately available.
  GetChangesResponsePB resp_truncated;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp_truncated, /*max_bytes=*/1));
  ASSERT_FALSE(resp_truncated.has_error()) << resp_truncated.error().DebugString();
  EXPECT_TRUE(resp_truncated.have_more_records());
  EXPECT_LT(resp_truncated.records_size(), 10);

  // Ample byte budget (default): the read reaches the committed head -> caught
  // up, no more records.
  GetChangesResponsePB resp_full;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp_full));
  ASSERT_FALSE(resp_full.has_error()) << resp_full.error().DebugString();
  EXPECT_FALSE(resp_full.have_more_records());

  // Polling from the caught-up checkpoint also reports no more records.
  GetChangesResponsePB resp_caught_up;
  ASSERT_OK(DoGetChanges(kTabletId, resp_full.checkpoint_op_index(),
                         &resp_caught_up));
  ASSERT_FALSE(resp_caught_up.has_error());
  EXPECT_FALSE(resp_caught_up.have_more_records());
}

// A replica that exists but is not yet RUNNING (bootstrapping / catching up)
// must return TABLET_NOT_RUNNING, not a misleading TABLET_NOT_FOUND /
// TABLET_NOT_LEADER. The not-running condition is injected because a tablet in
// this fixture is RUNNING by the time the test executes.
TEST_F(CDCServiceTest, GetChanges_TabletNotRunning) {
  InsertTestRowsRemote(0, 3, 3);
  FLAGS_cdc_inject_tablet_not_running = true;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_RUNNING, resp.error().code());
}

// If leadership is lost (or the term advances) between the initial leader check
// and the end of the WAL scan, the assembled batch may be from a log the new
// leader has diverged from. The post-read recheck must reject the read as
// TABLET_NOT_LEADER so the consumer retries against the current leader. The
// leadership loss is injected because forcing a real leader change precisely
// mid-read is not deterministic in a single-replica fixture.
TEST_F(CDCServiceTest, GetChanges_PostReadLeadershipLossRejected) {
  InsertTestRowsRemote(0, 3, 3);
  FLAGS_cdc_inject_post_read_leadership_loss = true;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_LEADER, resp.error().code());
}

TEST_F(CDCServiceTest, GetChanges_CaughtUp) {
  InsertTestRowsRemote(0, 2, 2);

  GetChangesResponsePB resp1;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp1));
  ASSERT_FALSE(resp1.has_error());
  int64_t checkpoint = resp1.checkpoint_op_index();

  // Re-poll from that checkpoint -- should get nothing.
  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp2));
  ASSERT_FALSE(resp2.has_error());
  EXPECT_EQ(0, resp2.records_size());
}

// GetChanges and Checkpoint leave no WAL anchor behind: an anchor that outlives
// the call is released by nothing but the end of its session, so a former
// leader or an abandoned consumer would pin the WAL past the master's barrier
// and past the retention valves.
TEST_F(CDCServiceTest, GetChangesAndCheckpointLeaveNoWalAnchor) {
  InsertTestRowsRemote(0, 3, 3);

  GetChangesResponsePB get_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &get_resp));
  ASSERT_FALSE(get_resp.has_error());
  int64_t checkpoint = get_resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);
  EXPECT_FALSE(HasCDCLogAnchor())
      << tablet_replica_->log_anchor_registry()->DumpAnchorInfo();

  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, checkpoint, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  EXPECT_FALSE(HasCDCLogAnchor())
      << tablet_replica_->log_anchor_registry()->DumpAnchorInfo();
  EXPECT_TRUE(mini_server_->server()->cdc_service()->HasSessionForTests("test-stream-1",
                                                                       kTabletId));
}

// A Checkpoint never creates a session: only the leader's GetChanges does. A
// checkpoint of a fabricated stream or tablet, or one sent before any read, is
// persisted to the master (which validates it) and leaves nothing behind here.
TEST_F(CDCServiceTest, Checkpoint_WithoutSessionCreatesNoSessionOrAnchor) {
  InsertTestRowsRemote(0, 3, 3);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, 0, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));
  EXPECT_EQ(0, cdc->ActiveStreamCount());
  EXPECT_FALSE(HasCDCLogAnchor())
      << tablet_replica_->log_anchor_registry()->DumpAnchorInfo();
}

TEST_F(CDCServiceTest, Checkpoint_TabletNotFound) {
  CheckpointResponsePB resp;
  ASSERT_OK(DoCheckpoint("nonexistent-tablet", 5, &resp));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_FOUND, resp.error().code());
}

// A checkpoint must be a position in the tablet's WAL. A negative one would
// release the tablet's retention once persisted, and one past the end would
// make the consumer skip the ops in between when it resumes.
TEST_F(CDCServiceTest, Checkpoint_NegativeOrFutureOpIndexRejected) {
  InsertTestRowsRemote(0, 3, 3);
  const int64_t last_index = tablet_replica_->consensus()->GetLastOpId(
      consensus::RECEIVED_OPID)->index();
  const int64_t persists_before = CDCCounterValue(METRIC_cdc_checkpoint_persists);
  for (int64_t op_index : { int64_t{-1}, last_index + 1, std::numeric_limits<int64_t>::max() }) {
    SCOPED_TRACE(op_index);
    CheckpointResponsePB resp;
    ASSERT_OK(DoCheckpoint(kTabletId, op_index, &resp));
    ASSERT_TRUE(resp.has_error());
    EXPECT_EQ(CDCErrorPB::UNKNOWN_ERROR, resp.error().code());
    EXPECT_TRUE(StatusFromPB(resp.error().status()).IsInvalidArgument())
        << resp.error().ShortDebugString();
  }
  EXPECT_EQ(persists_before, CDCCounterValue(METRIC_cdc_checkpoint_persists));
  EXPECT_FALSE(HasCDCLogAnchor());

  // The last index of the WAL is a valid checkpoint.
  CheckpointResponsePB resp;
  ASSERT_OK(DoCheckpoint(kTabletId, last_index, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  EXPECT_EQ(persists_before + 1, CDCCounterValue(METRIC_cdc_checkpoint_persists));
}

// A stream serves and checkpoints only the tablets of its tables.
TEST_F(CDCServiceTest, TabletOfOtherTableRejected) {
  InsertTestRowsRemote(0, 3, 3);
  CDCStreamConfigPB config;
  config.set_record_type(CDCStreamConfigPB::CHANGE);
  config.set_snapshot_mode(CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  const string kStream = "other-table-stream";
  auto* cdc = mini_server_->server()->cdc_service();
  cdc->SetStreamConfigForTests(kStream, config, { "some-other-table-id" });
  const auto expect_rejected = [](const CDCErrorPB& error) {
    EXPECT_EQ(CDCErrorPB::TABLET_NOT_IN_STREAM, error.code()) << error.ShortDebugString();
    EXPECT_FALSE(error.is_retryable());
    EXPECT_FALSE(error.needs_resnapshot());
  };

  for (bool snapshot : { false, true }) {
    SCOPED_TRACE(snapshot);
    GetChangesRequestPB req;
    req.set_stream_id(kStream);
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(0);
    req.set_is_snapshot_start(snapshot);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(cdc_proxy_->GetChanges(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    NO_FATALS(expect_rejected(resp.error()));
    EXPECT_EQ(0, resp.records_size());
  }
  CheckpointRequestPB cp_req;
  cp_req.set_stream_id(kStream);
  cp_req.set_tablet_id(kTabletId);
  cp_req.set_op_index(1);
  CheckpointResponsePB cp_resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  const int64_t persists_before = CDCCounterValue(METRIC_cdc_checkpoint_persists);
  ASSERT_OK(cdc_proxy_->Checkpoint(cp_req, &cp_resp, &rpc));
  ASSERT_TRUE(cp_resp.has_error());
  NO_FATALS(expect_rejected(cp_resp.error()));
  EXPECT_EQ(persists_before, CDCCounterValue(METRIC_cdc_checkpoint_persists));
  EXPECT_FALSE(cdc->HasSessionForTests(kStream, kTabletId));
  EXPECT_EQ(0, cdc->SnapshotSessionsEstablishedForTests());
}

// Lever 3: durable checkpoint persistence to the master is rate-limited per
// (stream, tablet) by --cdc_checkpoint_persist_interval_ms. Rapid Checkpoint RPCs
// within one interval must persist at most once (write-combining the latest
// value).
TEST_F(CDCServiceTest, Checkpoint_PersistThrottled) {
  // Large interval so every checkpoint after the first is throttled.
  FLAGS_cdc_checkpoint_persist_interval_ms = 60 * 1000;

  InsertTestRowsRemote(0, 5, 5);

  // Establish the consumer session (Checkpoint only throttles/persists for a
  // tablet that has an active GetChanges session).
  GetChangesResponsePB get_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &get_resp));
  ASSERT_FALSE(get_resp.has_error());
  const int64_t checkpoint = get_resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  const int64_t persists_before = CDCCounterValue(METRIC_cdc_checkpoint_persists);
  const int64_t requests_before = CDCCounterValue(METRIC_cdc_checkpoint_requests);
  ASSERT_GE(persists_before, 0);

  // Fire several checkpoints in quick succession.
  const int kNumCheckpoints = 5;
  for (int i = 0; i < kNumCheckpoints; i++) {
    CheckpointResponsePB cp_resp;
    ASSERT_OK(DoCheckpoint(kTabletId, checkpoint, &cp_resp));
    ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  }

  // Every RPC was served...
  EXPECT_EQ(kNumCheckpoints,
            CDCCounterValue(METRIC_cdc_checkpoint_requests) - requests_before);
  // ...but only the first triggered a durable persist to the master.
  EXPECT_EQ(1, CDCCounterValue(METRIC_cdc_checkpoint_persists) - persists_before)
      << "throttle must write-combine rapid checkpoints into one persist";
}

// Lever 3: setting --cdc_checkpoint_persist_interval_ms=0 disables the throttle,
// so every Checkpoint RPC persists durably (matching the pre-throttle behavior).
TEST_F(CDCServiceTest, Checkpoint_PersistIntervalZeroAlwaysPersists) {
  FLAGS_cdc_checkpoint_persist_interval_ms = 0;

  InsertTestRowsRemote(0, 5, 5);

  GetChangesResponsePB get_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &get_resp));
  ASSERT_FALSE(get_resp.has_error());
  const int64_t checkpoint = get_resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  const int64_t persists_before = CDCCounterValue(METRIC_cdc_checkpoint_persists);
  ASSERT_GE(persists_before, 0);

  const int kNumCheckpoints = 4;
  for (int i = 0; i < kNumCheckpoints; i++) {
    CheckpointResponsePB cp_resp;
    ASSERT_OK(DoCheckpoint(kTabletId, checkpoint, &cp_resp));
    ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  }

  // With the throttle disabled, every checkpoint persists.
  EXPECT_EQ(kNumCheckpoints,
            CDCCounterValue(METRIC_cdc_checkpoint_persists) - persists_before);
}

// A checkpoint that the persist throttle holds back is persisted once the
// interval ends, not dropped (F-74). A consumer does not checkpoint an
// unchanged position again, so the last checkpoint before a tablet goes idle
// would otherwise never reach the master, whose staleness guard would then
// release the barrier of the caught-up consumer.
TEST_F(CDCServiceTest, Checkpoint_ThrottledCheckpointIsDeferredNotDropped) {
  constexpr int64_t kIntervalMs = 2000;
  FLAGS_cdc_checkpoint_persist_interval_ms = kIntervalMs;
  FLAGS_cdc_active_time_report_interval_ms = 3600 * 1000;
  // This fixture has no master: fail every batch at once, so that the updates
  // stay queued (a failed batch is queued again) and can be read.
  FLAGS_cdc_inject_checkpoint_persist_failure = true;
  SCOPED_CLEANUP({ FLAGS_cdc_inject_checkpoint_persist_failure = false; });
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();

  InsertTestRowsRemote(0, 5, 5);
  GetChangesResponsePB get_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &get_resp));
  ASSERT_FALSE(get_resp.has_error()) << get_resp.error().DebugString();
  const int64_t last = get_resp.checkpoint_op_index();
  ASSERT_GT(last, 1);

  // The first checkpoint of the session persists at once, and the ones that
  // follow within the interval are throttled.
  const int64_t persists_before = CDCCounterValue(METRIC_cdc_checkpoint_persists);
  const MonoTime first_persist = MonoTime::Now();
  for (int64_t op_index : { last - 1, last, last }) {
    CheckpointResponsePB cp_resp;
    ASSERT_OK(DoCheckpoint(kTabletId, op_index, &cp_resp));
    ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  }
  if (MonoTime::Now() - first_persist < MonoDelta::FromMilliseconds(kIntervalMs)) {
    EXPECT_EQ(1, CDCCounterValue(METRIC_cdc_checkpoint_persists) - persists_before);
    master::UpdateCDCCheckpointRequestPB queued;
    ASSERT_EVENTUALLY([&] {
      ASSERT_TRUE(cdc->QueuedCheckpointUpdateForTests("test-stream-1", kTabletId, &queued));
    });
    if (MonoTime::Now() - first_persist < MonoDelta::FromMilliseconds(kIntervalMs)) {
      EXPECT_EQ(last - 1, queued.op_index()) << queued.ShortDebugString();
    }
  }

  // When the interval ends, the throttled checkpoints are persisted as one.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(2, CDCCounterValue(METRIC_cdc_checkpoint_persists) - persists_before)
        << "the throttle dropped the trailing checkpoint";
    master::UpdateCDCCheckpointRequestPB queued;
    ASSERT_TRUE(cdc->QueuedCheckpointUpdateForTests("test-stream-1", kTabletId, &queued));
    ASSERT_EQ(last, queued.op_index()) << queued.ShortDebugString();
    ASSERT_FALSE(queued.refresh_active_time_only()) << queued.ShortDebugString();
    // The master counts the caught-up consumer as progressing.
    ASSERT_EQ(last, queued.up_to_op_index()) << queued.ShortDebugString();
  });
  EXPECT_GE(MonoTime::Now() - first_persist, MonoDelta::FromMilliseconds(kIntervalMs));
}

// A checkpoint sent while a snapshot is active reports no committed op index
// ('up_to_op_index') to the master: the snapshot cursor tracks the progress of
// a snapshot, and the index seen before it would count a stuck snapshot as
// progressing (see the staleness guard of the master).
TEST_F(CDCServiceTest, Checkpoint_DuringSnapshotReportsNoUpTo) {
  FLAGS_cdc_checkpoint_persist_interval_ms = 0;
  FLAGS_cdc_active_time_report_interval_ms = 3600 * 1000;
  FLAGS_cdc_inject_checkpoint_persist_failure = true;
  SCOPED_CLEANUP({ FLAGS_cdc_inject_checkpoint_persist_failure = false; });
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();

  InsertTestRowsRemote(0, 5, 5);
  // A WAL read, which records the committed op index of the tablet.
  GetChangesResponsePB get_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &get_resp));
  ASSERT_FALSE(get_resp.has_error()) << get_resp.error().DebugString();
  ASSERT_GT(get_resp.checkpoint_op_index(), 0);
  const auto take_queued = [&](master::UpdateCDCCheckpointRequestPB* queued) {
    ASSERT_EVENTUALLY([&] {
      ASSERT_TRUE(cdc->QueuedCheckpointUpdateForTests(
          "test-stream-1", kTabletId, queued, /*take=*/true));
    });
  };
  master::UpdateCDCCheckpointRequestPB queued;
  {
    CheckpointResponsePB cp_resp;
    ASSERT_OK(DoCheckpoint(kTabletId, 1, &cp_resp));
    ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
    NO_FATALS(take_queued(&queued));
    ASSERT_TRUE(queued.has_up_to_op_index()) << queued.ShortDebugString();
  }

  // A snapshot starts, and the consumer checkpoints before its streaming
  // start, so the snapshot stays active.
  GetChangesResponsePB snap_resp;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &snap_resp));
  ASSERT_FALSE(snap_resp.has_error()) << snap_resp.error().DebugString();
  ASSERT_TRUE(cdc->HasActiveSnapshotForTests("test-stream-1", kTabletId));
  // Drop what the snapshot queued.
  cdc->QueuedCheckpointUpdateForTests("test-stream-1", kTabletId, &queued, /*take=*/true);
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, 1, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  ASSERT_TRUE(cdc->HasActiveSnapshotForTests("test-stream-1", kTabletId));
  NO_FATALS(take_queued(&queued));
  EXPECT_EQ(1, queued.op_index()) << queued.ShortDebugString();
  EXPECT_FALSE(queued.has_up_to_op_index()) << queued.ShortDebugString();
}

// CF-2/DR-018: a checkpoint persist must increment
// cdc_checkpoint_persist_failures when all master candidates fail (here: the
// inject flag forces failure so the test does not require a real master
// connectivity failure).
//
// Non-vacuous: reverting the counter increment from the persist path causes
// the final ASSERT_GT to fail.
TEST_F(CDCServiceTest, Checkpoint_PersistFailureCounterIncremented) {
  // Disable the throttle so every Checkpoint call issues a persist attempt,
  // giving us a 1:1 mapping between Checkpoint RPCs and persist failures.
  FLAGS_cdc_checkpoint_persist_interval_ms = 0;

  InsertTestRowsRemote(0, 3, 3);

  GetChangesResponsePB get_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &get_resp));
  ASSERT_FALSE(get_resp.has_error());
  const int64_t checkpoint = get_resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  const int64_t failures_before = CDCCounterValue(METRIC_cdc_checkpoint_persist_failures);

  // Inject persist failures: every persist attempt fails immediately without
  // sending any master RPC and increments the failure counter.
  FLAGS_cdc_inject_checkpoint_persist_failure = true;
  SCOPED_CLEANUP({ FLAGS_cdc_inject_checkpoint_persist_failure = false; });

  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, checkpoint, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();

  // The Checkpoint RPC succeeded (consumer sees success) but the durable
  // persist to the master, which happens in the background, failed. The failure
  // counter must advance.
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(CDCCounterValue(METRIC_cdc_checkpoint_persist_failures), failures_before)
        << "CF-2 fix: a persist failure must increment cdc_checkpoint_persist_failures "
           "so operators can observe the condition on monitoring dashboards";
  });
}

// A checkpoint persist refreshes the stream's activity on the master, so the
// GetChanges that follows it within --cdc_active_time_report_interval_ms does
// not send an activity report of its own.
TEST_F(CDCServiceTest, ActivityReportSuppressedAfterCheckpointPersist) {
  constexpr int64_t kReportIntervalMs = 2000;
  FLAGS_cdc_active_time_report_interval_ms = kReportIntervalMs;
  InsertTestRowsRemote(0, 5, 5);

  // The first GetChanges of a session creates it and reports its activity.
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp, /*max_bytes=*/1));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  SleepFor(MonoDelta::FromMilliseconds(kReportIntervalMs + 100));

  // The first Checkpoint of a session always persists.
  const int64_t persists_before = CDCCounterValue(METRIC_cdc_checkpoint_persists);
  const int64_t reports_before = CDCCounterValue(METRIC_cdc_activity_reports);
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, 1, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  ASSERT_EQ(1, CDCCounterValue(METRIC_cdc_checkpoint_persists) - persists_before);

  // The last report is older than the interval, but the persist refreshed the
  // activity on the master.
  ASSERT_OK(DoGetChanges(kTabletId, 1, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  EXPECT_EQ(0, CDCCounterValue(METRIC_cdc_activity_reports) - reports_before);
}

// Requests for unknown stream ids do not grow the per-stream maps and cost at
// most one master fetch per stream id: a request for a tablet this server does
// not host is rejected before the master is asked, the fetch lock of a stream
// is dropped after its fetch, and a failed fetch is remembered for a while.
TEST_F(CDCServiceTest, UnknownStreamIdDoesNotGrowMaps) {
  FLAGS_cdc_stream_config_negative_cache_ttl_ms = 60000;
  auto* cdc = mini_server_->server()->cdc_service();
  InsertTestRowsRemote(0, 5, 5);

  const int64_t fetches_before = cdc->StreamConfigMasterFetchesForTests();
  for (int i = 0; i < 100; ++i) {
    GetChangesRequestPB req;
    req.set_stream_id(Substitute("unknown-stream-$0", i));
    req.set_tablet_id("no-such-tablet");
    req.set_from_op_index(0);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(cdc_proxy_->GetChanges(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(CDCErrorPB::TABLET_NOT_FOUND, resp.error().code());
  }
  EXPECT_EQ(fetches_before, cdc->StreamConfigMasterFetchesForTests())
      << "a request for an unknown tablet must not reach the master";

  // This server's master is unreachable, so every fetch fails.
  constexpr int kNumStreams = 20;
  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < kNumStreams; ++i) {
      GetChangesResponsePB resp;
      ASSERT_OK(DoGetChangesForStream(Substitute("unknown-stream-$0", i), 0, &resp));
    }
  }
  EXPECT_EQ(kNumStreams, cdc->StreamConfigMasterFetchesForTests() - fetches_before);
  EXPECT_EQ(0, cdc->StreamConfigFetchLockCountForTests());
}

// A call that cannot get the config of its stream is shed as retryable, the
// call whose fetch failed as well as the calls that find the failure
// remembered: without the config, the server does not know the stream's record
// type, snapshot mode or tables. It would serve, e.g., a snapshot for a NEVER
// stream, CHANGE records for a FULL one, or a tablet of another table.
TEST_F(CDCServiceTest, ConfigFetchFailureIsShed) {
  FLAGS_cdc_stream_config_negative_cache_ttl_ms = 60000;
  auto* cdc = mini_server_->server()->cdc_service();
  InsertTestRowsRemote(0, 3, 3);
  // This stream's config is not seeded, and this server's master is
  // unreachable, so the fetch fails.
  const string kStream = "unseeded-stream";
  const auto get_changes = [&](bool snapshot, GetChangesResponsePB* resp) {
    GetChangesRequestPB req;
    req.set_stream_id(kStream);
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(0);
    req.set_is_snapshot_start(snapshot);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    return cdc_proxy_->GetChanges(req, resp, &rpc);
  };
  const auto expect_shed = [](const CDCErrorPB& error) {
    EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, error.code()) << error.ShortDebugString();
    EXPECT_TRUE(error.is_retryable());
  };

  const int64_t fetches_before = cdc->StreamConfigMasterFetchesForTests();
  GetChangesResponsePB resp;
  ASSERT_OK(get_changes(/*snapshot=*/false, &resp));
  ASSERT_TRUE(resp.has_error());
  NO_FATALS(expect_shed(resp.error()));
  EXPECT_EQ(0, resp.records_size());
  ASSERT_EQ(1, cdc->StreamConfigMasterFetchesForTests() - fetches_before);
  EXPECT_FALSE(cdc->HasSessionForTests(kStream, kTabletId));

  resp.Clear();
  ASSERT_OK(get_changes(/*snapshot=*/true, &resp));
  ASSERT_TRUE(resp.has_error());
  NO_FATALS(expect_shed(resp.error()));
  EXPECT_EQ(0, cdc->SnapshotSessionsEstablishedForTests());
  resp.Clear();
  ASSERT_OK(get_changes(/*snapshot=*/false, &resp));
  ASSERT_TRUE(resp.has_error());
  NO_FATALS(expect_shed(resp.error()));

  CheckpointRequestPB cp_req;
  cp_req.set_stream_id(kStream);
  cp_req.set_tablet_id(kTabletId);
  cp_req.set_op_index(1);
  CheckpointResponsePB cp_resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(cdc_proxy_->Checkpoint(cp_req, &cp_resp, &rpc));
  ASSERT_TRUE(cp_resp.has_error());
  NO_FATALS(expect_shed(cp_resp.error()));
  EXPECT_EQ(1, cdc->StreamConfigMasterFetchesForTests() - fetches_before);
}

// Exercises the per-tablet retention barrier that the master pushes to every
// replica (independent of the per-consumer Checkpoint path).
TEST_F(CDCServiceTest, SetRetentionBarrier) {
  InsertTestRowsRemote(0, 3, 3);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);

  // Setting a barrier at op index 1 pins WAL retention to at most index 1. The
  // barrier lives in the tablet metadata, not in the LogAnchorRegistry.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, 1));
  EXPECT_EQ(1, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());
  EXPECT_LE(tablet_replica_->GetRetentionIndexes().for_durability, 1);
  EXPECT_FALSE(HasCDCLogAnchor())
      << tablet_replica_->log_anchor_registry()->DumpAnchorInfo();

  // Releasing the barrier (negative index) succeeds and is idempotent.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, -1));
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, -1));

  // A barrier for a tablet not hosted here returns NotFound.
  Status s = cdc->SetRetentionBarrier("nonexistent-tablet", 1);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

// A replica that is not running may share its consensus metadata with a tablet
// copy, which updates the metadata without the consensus lock. The CDC service
// must not read the Raft role of such a replica: with the writes of a real
// copy that is a data race, and in debug builds the thread collision warner of
// the consensus metadata aborts on any concurrent access, as below.
TEST_F(CDCServiceTest, NoRaftRoleReadOfReplicaThatIsNotRunning) {
  InsertTestRowsRemote(0, 3, 3);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  scoped_refptr<consensus::ConsensusMetadata> cmeta;
  ASSERT_OK(mini_server_->server()->tablet_manager()->cmeta_manager_for_tests()->Load(
      kTabletId, &cmeta));
  // Shut the replica down as a tablet copy over it does, leaving it registered.
  tablet_replica_->Shutdown();
  ASSERT_EQ(tablet::SHUTDOWN, tablet_replica_->state());

  // Access the consensus metadata the way a tablet copy does: from another
  // thread, without the consensus lock.
  std::atomic<bool> stop(false);
  std::atomic<int64_t> num_accesses(0);
  std::thread copier([&] {
    while (!stop) {
      consensus::ConsensusStatePB cstate = cmeta->ToConsensusStatePB();
      num_accesses++;
    }
  });
  auto join_copier = MakeScopedCleanup([&] {
    stop = true;
    copier.join();
  });
  for (int i = 0; i < 300; i++) {
    ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1));

    CheckpointResponsePB cp_resp;
    ASSERT_OK(DoCheckpoint(kTabletId, 1, &cp_resp));
    ASSERT_TRUE(cp_resp.has_error());
    ASSERT_EQ(CDCErrorPB::TABLET_NOT_LEADER, cp_resp.error().code())
        << cp_resp.error().ShortDebugString();

    GetChangesResponsePB snap_resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, /*resume_key=*/"", &snap_resp));
    ASSERT_TRUE(snap_resp.has_error());

    cdc->EndSessionsIfNotLeader(kTabletId);
  }
  join_copier.run();
  ASSERT_GT(num_accesses, 0);
  EXPECT_EQ(1, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());
}

// Barrier SET/RELEASE RPCs are async, best-effort and unordered, so a stale SET
// from an earlier master maintenance pass can arrive after a later pass's
// RELEASE. Without last-writer-wins gating that stale SET would re-anchor the
// replica forever -- a WAL/history-retention leak. The replica must discard any
// barrier update whose master sequence is lower than the highest it has applied.
TEST_F(CDCServiceTest, SetRetentionBarrier_LastWriterWinsOnReorder) {
  InsertTestRowsRemote(0, 3, 3);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);
  int64_t idx = -1;

  // Pass @seq=100 sets a barrier at op index 1.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/100));
  ASSERT_TRUE(cdc->RetentionBarrierForTests(kTabletId, &idx));
  EXPECT_EQ(1, idx);

  // A later pass @seq=200 releases the barrier.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/200));
  EXPECT_FALSE(cdc->RetentionBarrierForTests(kTabletId));

  // The delayed SET from the earlier pass @seq=100 now arrives out of order. It
  // must be discarded (superseded by seq=200), NOT re-anchor the replica, and
  // the caller is told so.
  bool superseded = false;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/100,
                                     /*release_consumer_stream_id=*/"",
                                     /*skip_barrier_update=*/false, &superseded));
  EXPECT_TRUE(superseded);
  EXPECT_FALSE(cdc->RetentionBarrierForTests(kTabletId))
      << "stale SET re-anchored the replica after a newer RELEASE (leak)";

  // A genuinely newer pass @seq=300 is still honored.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/2,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/300,
                                     /*release_consumer_stream_id=*/"",
                                     /*skip_barrier_update=*/false, &superseded));
  EXPECT_FALSE(superseded);
  ASSERT_TRUE(cdc->RetentionBarrierForTests(kTabletId, &idx));
  EXPECT_EQ(2, idx);

  // Symmetric case: a stale RELEASE @seq=250 (older than the live SET@300) must
  // not tear down the barrier the newer SET established.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/250));
  ASSERT_TRUE(cdc->RetentionBarrierForTests(kTabletId, &idx))
      << "stale RELEASE tore down the barrier a newer SET established (leak)";
  EXPECT_EQ(2, idx);
}

// The last applied barrier sequence is persisted with the barrier, so a stale
// SET that is still being retried by the master when this server restarts
// cannot re-pin a barrier that a newer update released before the restart.
TEST_F(CDCServiceTest, SetRetentionBarrier_SeqSurvivesRestart) {
  InsertTestRowsRemote(0, 3, 3);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/100));
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/200));
  ASSERT_OK(RestartTabletServer());
  cdc = mini_server_->server()->cdc_service();
  ASSERT_OK(mini_server_->server()->tablet_manager()->GetTabletReplica(kTabletId,
                                                                      &tablet_replica_));
  EXPECT_EQ(200, tablet_replica_->tablet_metadata()->cdc_barrier_seq());

  bool superseded = false;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/100,
                                     /*release_consumer_stream_id=*/"",
                                     /*skip_barrier_update=*/false, &superseded));
  EXPECT_TRUE(superseded);
  EXPECT_FALSE(cdc->RetentionBarrierForTests(kTabletId))
      << "a stale SET re-pinned the barrier after a restart";
  EXPECT_EQ(-1, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());

  // A newer update still applies.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/2,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/300,
                                     /*release_consumer_stream_id=*/"",
                                     /*skip_barrier_update=*/false, &superseded));
  EXPECT_FALSE(superseded);
  EXPECT_EQ(2, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());
}

// An unchanged release still persists its sequence: a released barrier is not
// refreshed, so if a restart lost the sequence, a stale SET that the master
// sent before the release could re-pin the barrier after the restart.
TEST_F(CDCServiceTest, SetRetentionBarrier_UnchangedReleaseSeqSurvivesRestart) {
  InsertTestRowsRemote(0, 3, 3);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/100));
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/200));
  // The master re-pins the tablet with a SET that is delayed, then releases
  // the tablet again: the barrier is unchanged by the second release.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/300));
  ASSERT_OK(RestartTabletServer());
  cdc = mini_server_->server()->cdc_service();
  ASSERT_OK(mini_server_->server()->tablet_manager()->GetTabletReplica(kTabletId,
                                                                      &tablet_replica_));
  EXPECT_EQ(300, tablet_replica_->tablet_metadata()->cdc_barrier_seq());

  // The delayed SET lands after the restart.
  bool superseded = false;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/250,
                                     /*release_consumer_stream_id=*/"",
                                     /*skip_barrier_update=*/false, &superseded));
  EXPECT_TRUE(superseded);
  EXPECT_FALSE(cdc->RetentionBarrierForTests(kTabletId))
      << "a stale SET re-pinned a released barrier after a restart";
  EXPECT_EQ(-1, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());
}

// A replica rejects a lower barrier sequence only for
// --cdc_barrier_reorder_window_secs after it applied the last one. After that
// it applies a lower sequence, so it recovers when the master's sequence went
// backwards, as after 'kudu master unsafe_rebuild': the rebuilt system catalog
// starts again at the first term, below the term the replica persisted. The
// lower sequence is persisted, so a restart does not bring back the old one.
TEST_F(CDCServiceTest, SetRetentionBarrier_LowerSeqAppliedAfterReorderWindow) {
  InsertTestRowsRemote(0, 3, 3);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  const scoped_refptr<tablet::TabletMetadata> meta = tablet_replica_->tablet_metadata();
  constexpr int64_t kOldTermSeq = (int64_t{5} << 32) | 7;
  constexpr int64_t kNewTermSeq = (int64_t{1} << 32) | 1;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0, kOldTermSeq));

  // Within the window, the lower sequence is superseded.
  bool superseded = false;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0, kNewTermSeq,
                                     /*release_consumer_stream_id=*/"",
                                     /*skip_barrier_update=*/false, &superseded));
  ASSERT_TRUE(superseded);
  ASSERT_EQ(1, meta->cdc_min_retained_op_index());

  // Once the last sequence is older than the window, the lower one is applied,
  // also when it leaves the barrier unchanged. With a window of 0, it already is.
  FLAGS_cdc_barrier_reorder_window_secs = 0;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0, kNewTermSeq,
                                     /*release_consumer_stream_id=*/"",
                                     /*skip_barrier_update=*/false, &superseded));
  ASSERT_FALSE(superseded) << "a lower sequence was rejected after the reorder window";
  ASSERT_EQ(kNewTermSeq, meta->cdc_barrier_seq());

  // The applied sequence rejects lower ones again.
  FLAGS_cdc_barrier_reorder_window_secs = 3600;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0, kNewTermSeq - 1,
                                     /*release_consumer_stream_id=*/"",
                                     /*skip_barrier_update=*/false, &superseded));
  ASSERT_TRUE(superseded);
  ASSERT_EQ(1, meta->cdc_min_retained_op_index());

  ASSERT_OK(RestartTabletServer());
  ASSERT_OK(mini_server_->server()->tablet_manager()->GetTabletReplica(kTabletId,
                                                                      &tablet_replica_));
  ASSERT_EQ(kNewTermSeq, tablet_replica_->tablet_metadata()->cdc_barrier_seq());
  ASSERT_OK(mini_server_->server()->cdc_service()->SetRetentionBarrier(
      kTabletId, /*min_retained_op_index=*/2, /*history_safe_time_micros=*/0, kNewTermSeq + 1,
      /*release_consumer_stream_id=*/"", /*skip_barrier_update=*/false, &superseded));
  ASSERT_FALSE(superseded);
  ASSERT_EQ(2, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());
}

// A superseded update is reported to the master in the RPC response and
// counted, instead of looking like an applied one.
TEST_F(CDCServiceTest, UpdateCDCRetentionBarrier_ReportsSuperseded) {
  InsertTestRowsRemote(0, 3, 3);
  scoped_refptr<Counter> superseded_counter =
      METRIC_cdc_barrier_updates_superseded.Instantiate(
          mini_server_->server()->metric_entity());
  const int64_t superseded_before = superseded_counter->value();
  const auto send = [&](int64_t index, int64_t seq,
                        tserver::UpdateCDCRetentionBarrierResponsePB* resp) {
    tserver::UpdateCDCRetentionBarrierRequestPB req;
    req.set_dest_uuid(mini_server_->server()->fs_manager()->uuid());
    req.set_tablet_id(kTabletId);
    req.set_min_retained_op_index(index);
    req.set_barrier_seq(seq);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(admin_proxy_->UpdateCDCRetentionBarrier(req, resp, &rpc));
    ASSERT_FALSE(resp->has_error()) << resp->error().ShortDebugString();
  };
  tserver::UpdateCDCRetentionBarrierResponsePB resp;
  NO_FATALS(send(/*index=*/2, /*seq=*/200, &resp));
  EXPECT_FALSE(resp.superseded());
  EXPECT_EQ(superseded_before, superseded_counter->value());

  resp.Clear();
  NO_FATALS(send(/*index=*/1, /*seq=*/100, &resp));
  EXPECT_TRUE(resp.superseded());
  EXPECT_EQ(superseded_before + 1, superseded_counter->value());
  EXPECT_EQ(2, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());
}

// A4: on stream deletion the master fans a session release to every replica via
// SetRetentionBarrier's release_consumer_stream_id. The session (with its
// history floor and metrics) is established by the consumer's own polling
// (leader only), separate from the master-pushed aggregate retention barrier.
// Verify the release ends the session, independently of the aggregate barrier,
// in both the shared-tablet (barrier left intact) and orphaned-tablet (barrier
// also released) cases.
TEST_F(CDCServiceTest, SetRetentionBarrier_EndsSessionOnStreamDelete) {
  InsertTestRowsRemote(0, 3, 3);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);

  // A consumer poll (not caught up) establishes the session.
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_TRUE(cdc->HasSessionForTests("test-stream-1", kTabletId));

  // Independently set the aggregate barrier so we can prove the two release
  // paths do not interfere.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1));
  ASSERT_TRUE(cdc->RetentionBarrierForTests(kTabletId));

  // Shared-tablet case: skip_barrier_update releases ONLY the session,
  // leaving the aggregate barrier for the master's next pass to recompute across
  // the surviving streams.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/0,
                                     /*release_consumer_stream_id=*/"test-stream-1",
                                     /*skip_barrier_update=*/true));
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId))
      << "session not released on stream delete (A4 leak)";
  EXPECT_TRUE(cdc->RetentionBarrierForTests(kTabletId))
      << "skip_barrier_update must leave the aggregate barrier untouched";

  // Releasing an already-released session is a harmless no-op.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/0,
                                     /*release_consumer_stream_id=*/"test-stream-1",
                                     /*skip_barrier_update=*/true));
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));

  // A deleted stream's id is never reused, and a poll of it must not re-create
  // the released session.
  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp2));
  ASSERT_TRUE(resp2.has_error()) << resp2.DebugString();
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));

  // Orphaned-tablet case: establish a session and the barrier for another
  // stream, then release them together in one call (skip_barrier_update=false,
  // min_retained_op_index=-1).
  SeedStreamConfig(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::NEVER, "test-stream-2");
  GetChangesRequestPB req3;
  req3.set_stream_id("test-stream-2");
  req3.set_tablet_id(kTabletId);
  req3.set_from_op_index(0);
  GetChangesResponsePB resp3;
  RpcController rpc3;
  rpc3.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(cdc_proxy_->GetChanges(req3, &resp3, &rpc3));
  ASSERT_FALSE(resp3.has_error()) << resp3.error().DebugString();
  ASSERT_TRUE(cdc->HasSessionForTests("test-stream-2", kTabletId));
  ASSERT_TRUE(cdc->RetentionBarrierForTests(kTabletId));

  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/0,
                                     /*release_consumer_stream_id=*/"test-stream-2",
                                     /*skip_barrier_update=*/false));
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-2", kTabletId))
      << "session not released alongside the aggregate barrier";
  EXPECT_FALSE(cdc->RetentionBarrierForTests(kTabletId))
      << "aggregate barrier not released for an orphaned tablet";
}

// A stream delete releasing the session while a Checkpoint for the same stream
// is about to update it must neither touch the freed session state nor leave
// a session or an anchor behind for the deleted stream.
TEST_F(CDCServiceTest, Checkpoint_RacingStreamReleaseNoUAF) {
  SeedStreamConfig(CDCStreamConfigPB::CHANGE);
  FLAGS_cdc_master_rpc_timeout_ms = 1000;
  InsertTestRowsRemote(0, 3, 3);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_TRUE(cdc->HasSessionForTests("test-stream-1", kTabletId));

  // Hold the Checkpoint before its session update while the stream is deleted.
  FLAGS_cdc_inject_latency_before_session_update_ms = 3000;
  const int64_t checkpoints_before = CDCCounterValue(METRIC_cdc_checkpoint_requests);
  CheckpointResponsePB cp_resp;
  Status cp_status;
  std::thread checkpointer([&]() {
    cp_status = DoCheckpoint(kTabletId, resp.checkpoint_op_index(), &cp_resp);
  });
  auto join = MakeScopedCleanup([&]() { checkpointer.join(); });
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(checkpoints_before + 1, CDCCounterValue(METRIC_cdc_checkpoint_requests));
  });
  SleepFor(MonoDelta::FromMilliseconds(500));
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/0,
                                     /*release_consumer_stream_id=*/"test-stream-1",
                                     /*skip_barrier_update=*/true));
  ASSERT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));
  join.run();
  FLAGS_cdc_inject_latency_before_session_update_ms = 0;

  // The in-flight Checkpoint is refused and creates nothing.
  ASSERT_OK(cp_status);
  ASSERT_TRUE(cp_resp.has_error()) << cp_resp.DebugString();
  EXPECT_EQ(CDCErrorPB::STREAM_NOT_FOUND, cp_resp.error().code());
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));
  EXPECT_FALSE(HasCDCLogAnchor())
      << tablet_replica_->log_anchor_registry()->DumpAnchorInfo();

  // A later poll of the deleted stream does not re-create the session either.
  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp2));
  EXPECT_TRUE(resp2.has_error()) << resp2.DebugString();
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));
  EXPECT_FALSE(HasCDCLogAnchor())
      << tablet_replica_->log_anchor_registry()->DumpAnchorInfo();
}

// A GetChanges that creates its session while DeleteTablet stops the replica
// must not leave the session behind: the tablet manager ends the replica's
// sessions right after stopping it, so a session created later would outlive
// the replica and be reused by a replica copied in under the same tablet id.
TEST_F(CDCServiceTest, GetChanges_RacingDeleteTabletLeavesNoSession) {
  SeedStreamConfig(CDCStreamConfigPB::CHANGE);
  InsertTestRowsRemote(0, 3, 3);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);
  ASSERT_EQ(0, cdc->ActiveStreamCount());

  // Hold the poll right before it creates its session while the tablet is
  // deleted.
  FLAGS_cdc_inject_latency_before_session_update_ms = 3000;
  const int64_t polls_before = CDCCounterValue(METRIC_cdc_get_changes_requests);
  GetChangesResponsePB resp;
  Status poll_status;
  std::thread poller([&]() { poll_status = DoGetChanges(kTabletId, 0, &resp); });
  auto join = MakeScopedCleanup([&]() { poller.join(); });
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(polls_before + 1, CDCCounterValue(METRIC_cdc_get_changes_requests));
  });
  SleepFor(MonoDelta::FromMilliseconds(500));
  ASSERT_OK(mini_server_->server()->tablet_manager()->DeleteTablet(
      kTabletId, tablet::TABLET_DATA_TOMBSTONED, std::nullopt));
  ASSERT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));
  join.run();
  FLAGS_cdc_inject_latency_before_session_update_ms = 0;

  ASSERT_OK(poll_status);
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_RUNNING, resp.error().code()) << resp.DebugString();
  EXPECT_TRUE(resp.error().is_retryable());
  EXPECT_EQ(0, resp.records_size());
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));
  EXPECT_EQ(0, cdc->ActiveStreamCount());
}

// A GetChanges whose stream is deleted while it is about to create its session
// is refused with STREAM_NOT_FOUND and creates no session.
TEST_F(CDCServiceTest, GetChanges_RacingStreamReleaseReturnsStreamNotFound) {
  SeedStreamConfig(CDCStreamConfigPB::CHANGE);
  InsertTestRowsRemote(0, 3, 3);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);
  FLAGS_cdc_inject_latency_before_session_update_ms = 3000;
  const int64_t polls_before = CDCCounterValue(METRIC_cdc_get_changes_requests);
  GetChangesResponsePB resp;
  Status poll_status;
  std::thread poller([&]() { poll_status = DoGetChanges(kTabletId, 0, &resp); });
  auto join = MakeScopedCleanup([&]() { poller.join(); });
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(polls_before + 1, CDCCounterValue(METRIC_cdc_get_changes_requests));
  });
  SleepFor(MonoDelta::FromMilliseconds(500));
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/0,
                                     /*release_consumer_stream_id=*/"test-stream-1",
                                     /*skip_barrier_update=*/true));
  join.run();
  FLAGS_cdc_inject_latency_before_session_update_ms = 0;

  ASSERT_OK(poll_status);
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::STREAM_NOT_FOUND, resp.error().code()) << resp.DebugString();
  EXPECT_EQ(0, resp.records_size());
  EXPECT_FALSE(cdc->HasSessionForTests("test-stream-1", kTabletId));
  EXPECT_EQ(0, cdc->ActiveStreamCount());
}

// The disk-pressure valve frees the WAL of a tablet whose barrier the master
// set through the CDC service and whose consumer polls it: no CDC anchor in
// the LogAnchorRegistry re-pins the WAL after the valve skipped the barrier.
TEST_F(CDCServiceTest, DiskPressureValveReleasesWalWithLiveBarrierAndSession) {
  FLAGS_log_min_segments_to_retain = 1;
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  log::Log* log = tablet_replica_->log();
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  InsertTestRowsRemote(0, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  InsertTestRowsRemote(3, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1));
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp, /*max_bytes=*/1));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, 1, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  InsertTestRowsRemote(6, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_LE(tablet_replica_->GetRetentionIndexes().for_durability, 1);

  FLAGS_cdc_stop_retaining_min_disk_mb = std::numeric_limits<int64_t>::max();
  ASSERT_GT(tablet_replica_->GetRetentionIndexes().for_durability, 1)
      << "the WAL stays pinned: " << tablet_replica_->log_anchor_registry()->DumpAnchorInfo();
  const int before = log->reader()->num_segments();
  tablet_replica_->RunLogGC();
  EXPECT_LT(log->reader()->num_segments(), before);
}

// Like DiskPressureValveReleasesWalWithLiveBarrierAndSession, for the
// barrier-age ceiling: a barrier the master stopped refreshing frees the WAL
// although the consumer still polls the tablet.
TEST_F(CDCServiceTest, AgeValveReleasesWalWithLiveBarrierAndSession) {
  FLAGS_log_min_segments_to_retain = 1;
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  FLAGS_cdc_barrier_lease_secs = 0;
  FLAGS_cdc_max_wal_retention_secs = 1;
  log::Log* log = tablet_replica_->log();
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  InsertTestRowsRemote(0, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  InsertTestRowsRemote(3, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_LE(tablet_replica_->GetRetentionIndexes().for_durability, 1);

  // The master last refreshed the barrier longer ago than the ceiling. The
  // consumer keeps polling and checkpointing at the barrier, which must not
  // hold the WAL on its own.
  tablet_replica_->tablet_metadata()->set_cdc_barrier_refresh_monotime_for_tests(
      MonoTime::Now() - MonoDelta::FromSeconds(2));
  for (int i = 0; i < 3; i++) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 1, &resp, /*max_bytes=*/1));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    CheckpointResponsePB cp_resp;
    ASSERT_OK(DoCheckpoint(kTabletId, 1, &cp_resp));
    ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  }
  InsertTestRowsRemote(6, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_GT(tablet_replica_->GetRetentionIndexes().for_durability, 1)
      << "the WAL stays pinned: " << tablet_replica_->log_anchor_registry()->DumpAnchorInfo();
  const int before = log->reader()->num_segments();
  tablet_replica_->RunLogGC();
  EXPECT_LT(log->reader()->num_segments(), before);
}

// ---------------------------------------------------------------------------
// Phase 4: RecordType.FULL before/after image
// ---------------------------------------------------------------------------

TEST_F(CDCServiceTest, FullMode_InsertHasNoBeforeImage) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  const CDCRecordPB* ins = nullptr;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::INSERT) ins = &resp.records(i);
  }
  ASSERT_NE(nullptr, ins);
  EXPECT_EQ(0, ins->old_changes_size());
  EXPECT_GT(ins->changes_size(), 0);
}

TEST_F(CDCServiceTest, FullMode_UpdateHasBeforeAndFullAfter) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  UpdateTestRowRemote(0, 12345);   // int_val -> 12345

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  const CDCRecordPB* upd = nullptr;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::UPDATE) upd = &resp.records(i);
  }
  ASSERT_NE(nullptr, upd) << resp.DebugString();

  // Before-image is the complete pre-update row (int_val == 0).
  int32_t before_val = -1;
  ASSERT_TRUE(GetInt32Col(*upd, /*before=*/true, "int_val", &before_val));
  EXPECT_EQ(0, before_val);

  // After-image is the complete post-update row (int_val == 12345), not just the
  // changed column: key + int_val + string_val.
  int32_t after_val = -1;
  ASSERT_TRUE(GetInt32Col(*upd, /*before=*/false, "int_val", &after_val));
  EXPECT_EQ(12345, after_val);
  EXPECT_EQ(3, upd->changes_size());
  EXPECT_EQ(3, upd->old_changes_size());
}

// A FULL image whose ops were not applied in time (most likely right after a
// leader change) is a transient failure: the consumer must retry the same
// request, not stop. Before, the timeout was returned as UNKNOWN_ERROR, which
// is fatal.
TEST_F(CDCServiceTest, FullMode_ReconstructionTimeoutIsRetryable) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 12345);

  FLAGS_cdc_inject_full_reconstruction_timeout = true;
  SCOPED_CLEANUP({ FLAGS_cdc_inject_full_reconstruction_timeout = false; });
  // The INSERT needs no read, so it is returned; the batch ends before the
  // UPDATE.
  GetChangesResponsePB first;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &first));
  ASSERT_FALSE(first.has_error()) << first.DebugString();
  ASSERT_EQ(1, first.records_size()) << first.DebugString();
  EXPECT_EQ(CDCOpTypePB::INSERT, first.records(0).op_type());
  EXPECT_EQ(first.records(0).op_index(), first.checkpoint_op_index());
  EXPECT_TRUE(first.have_more_records());

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, first.checkpoint_op_index(), &resp));
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code()) << resp.error().DebugString();
  EXPECT_TRUE(resp.error().is_retryable());
  EXPECT_FALSE(resp.error().needs_resnapshot());
  EXPECT_EQ(0, resp.records_size());
}

// An I/O error while reading the history of a row for a FULL image, on a
// replica that keeps running, is transient: the consumer must poll again, not
// stop (F-44). Before, the IOError was returned as UNKNOWN_ERROR, which is
// fatal.
TEST_F(CDCServiceTest, FullMode_ReconstructionIOErrorIsRetryable) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 12345);

  // The injected failure is an IOError.
  FLAGS_cdc_inject_full_reconstruction_failure = true;
  SCOPED_CLEANUP({ FLAGS_cdc_inject_full_reconstruction_failure = false; });
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_TRUE(StatusFromPB(resp.error().status()).IsIOError()) << resp.error().DebugString();
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code()) << resp.error().DebugString();
  EXPECT_TRUE(resp.error().is_retryable());
  EXPECT_FALSE(resp.error().needs_resnapshot());
  EXPECT_EQ(0, resp.records_size());
}

// The same for an I/O error reading the WAL of a replica that keeps running.
TEST_F(CDCServiceTest, WalReadIOErrorIsRetryable) {
  InsertTestRowsRemote(0, 3, 3);
  ASSERT_OK(tablet_replica_->log()->WaitUntilAllFlushed());

  {
    FLAGS_env_inject_eio_globs = JoinPathSegments(
        mini_server_->server()->fs_manager()->GetTabletWalDir(kTabletId), "wal-*");
    FLAGS_env_inject_eio = 1.0;
    SCOPED_CLEANUP({ FLAGS_env_inject_eio = 0; });
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_TRUE(resp.has_error()) << resp.DebugString();
    EXPECT_TRUE(StatusFromPB(resp.error().status()).IsIOError()) << resp.error().DebugString();
    EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code()) << resp.error().DebugString();
    EXPECT_TRUE(resp.error().is_retryable());
    EXPECT_FALSE(resp.error().needs_resnapshot());
    EXPECT_EQ(0, resp.records_size());
  }

  // The retry succeeds once the disk reads again.
  ASSERT_OK(tablet_replica_->CheckRunning());
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  EXPECT_EQ(3, resp.records_size()) << resp.DebugString();
}

// Regression test for E1: when FULL-mode before/after image reconstruction
// fails for a reason other than history-GC (IsIncomplete) -- e.g. a transient
// IOError/Corruption during the MVCC scan -- the batch must be aborted with an
// error, never emitted as a truncated record with an empty before-image.
TEST_F(CDCServiceTest, FullMode_ReconstructionFailureDoesNotEmitTruncatedRecord) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  UpdateTestRowRemote(0, 12345);   // int_val -> 12345 (needs reconstruction)

  // Force reconstruction to fail with a non-Incomplete, non-timeout error.
  FLAGS_cdc_inject_full_reconstruction_failure = true;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));

  // The batch must surface an error and emit no records -- specifically no
  // UPDATE record with an empty old_changes (before-image).
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(0, resp.records_size()) << resp.DebugString();
  for (int i = 0; i < resp.records_size(); ++i) {
    EXPECT_NE(CDCOpTypePB::UPDATE, resp.records(i).op_type())
        << "truncated UPDATE emitted despite reconstruction failure: "
        << resp.records(i).DebugString();
  }

  // With injection cleared, the same read succeeds and produces a full image.
  FLAGS_cdc_inject_full_reconstruction_failure = false;
  GetChangesResponsePB ok_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &ok_resp));
  ASSERT_FALSE(ok_resp.has_error()) << ok_resp.error().DebugString();
  const CDCRecordPB* upd = nullptr;
  for (int i = 0; i < ok_resp.records_size(); ++i) {
    if (ok_resp.records(i).op_type() == CDCOpTypePB::UPDATE) upd = &ok_resp.records(i);
  }
  ASSERT_NE(nullptr, upd) << ok_resp.DebugString();
  EXPECT_EQ(3, upd->old_changes_size());
}

// A FULL poll whose tablet is deleted between the WAL read and the decode must
// fail with a retryable TABLET_NOT_RUNNING instead of dereferencing the
// stopped replica's tablet.
TEST_F(CDCServiceTest, FullMode_TabletStoppedMidBatchReturnsNotRunning) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0
  UpdateTestRowRemote(0, 12345);   // needs before/after image reconstruction
  InsertTestRowsRemote(1, 1, 1);   // key=1

  FLAGS_cdc_inject_latency_before_decode_ms = 3000;
  const int64_t polls_before = CDCCounterValue(METRIC_cdc_get_changes_requests);
  GetChangesResponsePB resp;
  Status poll_status;
  std::thread poller([&]() { poll_status = DoGetChanges(kTabletId, 0, &resp); });
  auto join = MakeScopedCleanup([&]() { poller.join(); });
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(polls_before + 1, CDCCounterValue(METRIC_cdc_get_changes_requests));
  });
  SleepFor(MonoDelta::FromMilliseconds(500));
  ASSERT_OK(mini_server_->server()->tablet_manager()->DeleteTablet(
      kTabletId, tablet::TABLET_DATA_TOMBSTONED, std::nullopt));
  join.run();
  FLAGS_cdc_inject_latency_before_decode_ms = 0;

  ASSERT_OK(poll_status);
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_RUNNING, resp.error().code()) << resp.DebugString();
  EXPECT_TRUE(resp.error().is_retryable());
  EXPECT_EQ(0, resp.records_size());
}

#ifdef TCMALLOC_ENABLED
// A FULL poll that fails image reconstruction part way through a batch must
// free the rest of the batch. The ops after the failing one carry large rows,
// so leaking them on every failed poll shows up in the allocated heap.
TEST_F(CDCServiceTest, FullMode_ReconstructionFailureNoLeak) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0
  UpdateTestRowRemote(0, 12345);   // reconstruction fails here (injected)

  constexpr int kTrailingRows = 64;
  constexpr int kRowBytes = 60 * 1024;
  const string payload(kRowBytes, 'x');
  for (int key = 1; key <= kTrailingRows; ++key) {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
    AddTestRowWithNullableStringToPB(RowOperationsPB::INSERT, schema_, key, key,
                                     payload.c_str(), req.mutable_row_operations());
    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  FLAGS_cdc_inject_full_reconstruction_failure = true;
  SCOPED_CLEANUP({ FLAGS_cdc_inject_full_reconstruction_failure = false; });
  const auto poll = [&]() {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_TRUE(resp.has_error()) << resp.DebugString();
    ASSERT_EQ(0, resp.records_size());
  };
  NO_FATALS(poll());

  constexpr int kPolls = 10;
  const int64_t allocated_before = process_memory::GetTCMallocCurrentAllocatedBytes();
  for (int i = 0; i < kPolls; ++i) {
    NO_FATALS(poll());
  }
  const int64_t growth =
      process_memory::GetTCMallocCurrentAllocatedBytes() - allocated_before;
  // Leaking the trailing ops would grow the heap by about
  // kPolls * kTrailingRows * kRowBytes (~37 MiB).
  EXPECT_LT(growth, kTrailingRows * kRowBytes) << "heap grew by " << growth << " bytes";
}
#endif  // #ifdef TCMALLOC_ENABLED

// Regression test for E2: when a single write batch contains more than one
// operation on the same primary key (here two UPSERTs to a pre-existing key),
// FULL-mode reconstruction must classify *every* such record against the
// pre-existing row. The buggy code mapped a key to only the last record index,
// leaving the earlier UPSERT unmatched and misclassified as INSERT -- which
// violates the invariant that a key cannot be inserted twice without an
// intervening DELETE.
TEST_F(CDCServiceTest, FullMode_DuplicateKeyUpsertsClassifiedAsUpdate) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);

  // Pre-existing committed row: key=0, int_val=0.
  InsertTestRowsRemote(0, 1, 1);

  // One write batch with two UPSERTs on the SAME pre-existing key. The tablet
  // applies them sequentially and both reach the WAL as separate ops in one
  // replicate.
  {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
    AddTestRowToPB(RowOperationsPB::UPSERT, schema_, /*key=*/0, /*int_val=*/100,
                   "upsert-a", req.mutable_row_operations());
    AddTestRowToPB(RowOperationsPB::UPSERT, schema_, /*key=*/0, /*int_val=*/200,
                   "upsert-b", req.mutable_row_operations());

    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  // Expect exactly one INSERT (the original row) and two UPDATEs (both UPSERTs,
  // since the key pre-existed). No second INSERT for key 0.
  int inserts = 0;
  int updates = 0;
  for (int i = 0; i < resp.records_size(); ++i) {
    const auto& r = resp.records(i);
    if (r.op_type() == CDCOpTypePB::INSERT) {
      inserts++;
    } else if (r.op_type() == CDCOpTypePB::UPDATE) {
      updates++;
      // A reclassified UPDATE must carry the reconstructed before-image.
      EXPECT_GT(r.old_changes_size(), 0)
          << "UPDATE missing before-image: " << r.DebugString();
    }
  }
  EXPECT_EQ(1, inserts) << resp.DebugString();
  EXPECT_EQ(2, updates) << resp.DebugString();

  // Each UPDATE has its own images: 0 -> 100, then 100 -> 200.
  const auto rows = RowRecords(resp);
  ASSERT_EQ(3, rows.size()) << resp.DebugString();
  int32_t v = -1;
  ASSERT_TRUE(GetInt32Col(*rows[1], /*before=*/true, "int_val", &v));
  EXPECT_EQ(0, v);
  ASSERT_TRUE(GetInt32Col(*rows[1], /*before=*/false, "int_val", &v));
  EXPECT_EQ(100, v);
  ASSERT_TRUE(GetInt32Col(*rows[2], /*before=*/true, "int_val", &v));
  EXPECT_EQ(100, v);
  ASSERT_TRUE(GetInt32Col(*rows[2], /*before=*/false, "int_val", &v));
  EXPECT_EQ(200, v);
}

// One op that changes a row several times gives each change its own before-
// and after-image, chained in the order the tablet applied them.
TEST_F(CDCServiceTest, FullMode_SameKeyTwoUpdatesChainImages) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);  // key=0, int_val=0
  const int64_t checkpoint = CaughtUpCheckpoint();
  RowOperationsPB ops;
  AddTestRowWithNullableStringToPB(RowOperationsPB::UPDATE, schema_, 0, 1, nullptr, &ops);
  AddTestRowWithNullableStringToPB(RowOperationsPB::UPDATE, schema_, 0, 2, nullptr, &ops);
  NO_FATALS(WriteOps(schema_, ops));

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const auto rows = RowRecords(resp);
  ASSERT_EQ(2, rows.size()) << resp.DebugString();
  for (int i = 0; i < 2; ++i) {
    SCOPED_TRACE(rows[i]->DebugString());
    EXPECT_EQ(CDCOpTypePB::UPDATE, rows[i]->op_type());
    int32_t v = -1;
    ASSERT_TRUE(GetInt32Col(*rows[i], /*before=*/true, "int_val", &v));
    EXPECT_EQ(i, v);
    ASSERT_TRUE(GetInt32Col(*rows[i], /*before=*/false, "int_val", &v));
    EXPECT_EQ(i + 1, v);
    EXPECT_EQ(3, rows[i]->changes_size());
    EXPECT_EQ(3, rows[i]->old_changes_size());
  }
}

TEST_F(CDCServiceTest, FullMode_SameKeyUpsertUpsertNewKeyIsInsertThenUpdate) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  const int64_t checkpoint = CaughtUpCheckpoint();
  RowOperationsPB ops;
  AddTestRowToPB(RowOperationsPB::UPSERT, schema_, 5, 1, "a", &ops);
  AddTestRowToPB(RowOperationsPB::UPSERT, schema_, 5, 2, "b", &ops);
  NO_FATALS(WriteOps(schema_, ops));

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const auto rows = RowRecords(resp);
  ASSERT_EQ(2, rows.size()) << resp.DebugString();
  int32_t v = -1;
  EXPECT_EQ(CDCOpTypePB::INSERT, rows[0]->op_type());
  EXPECT_EQ(0, rows[0]->old_changes_size());
  ASSERT_TRUE(GetInt32Col(*rows[0], /*before=*/false, "int_val", &v));
  EXPECT_EQ(1, v);
  EXPECT_EQ(CDCOpTypePB::UPDATE, rows[1]->op_type());
  ASSERT_TRUE(GetInt32Col(*rows[1], /*before=*/true, "int_val", &v));
  EXPECT_EQ(1, v);
  ASSERT_TRUE(GetInt32Col(*rows[1], /*before=*/false, "int_val", &v));
  EXPECT_EQ(2, v);
}

TEST_F(CDCServiceTest, FullMode_SameKeyInsertThenUpdateHasBeforeImage) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  const int64_t checkpoint = CaughtUpCheckpoint();
  RowOperationsPB ops;
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 5, 1, "a", &ops);
  AddTestRowWithNullableStringToPB(RowOperationsPB::UPDATE, schema_, 5, 2, nullptr, &ops);
  NO_FATALS(WriteOps(schema_, ops));

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const auto rows = RowRecords(resp);
  ASSERT_EQ(2, rows.size()) << resp.DebugString();
  int32_t v = -1;
  EXPECT_EQ(CDCOpTypePB::INSERT, rows[0]->op_type());
  EXPECT_EQ(0, rows[0]->old_changes_size());
  ASSERT_TRUE(GetInt32Col(*rows[0], /*before=*/false, "int_val", &v));
  EXPECT_EQ(1, v);
  EXPECT_EQ(CDCOpTypePB::UPDATE, rows[1]->op_type());
  ASSERT_TRUE(GetInt32Col(*rows[1], /*before=*/true, "int_val", &v));
  EXPECT_EQ(1, v);
  ASSERT_TRUE(GetInt32Col(*rows[1], /*before=*/false, "int_val", &v));
  EXPECT_EQ(2, v);
  const CDCColumnValuePB* str = FindCol(*rows[1], /*before=*/false, "string_val");
  ASSERT_NE(nullptr, str) << rows[1]->DebugString();
  EXPECT_EQ("a", str->value());
}

TEST_F(CDCServiceTest, FullMode_SameKeyUpdateThenDeleteImages) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);  // key=0, int_val=0
  const int64_t checkpoint = CaughtUpCheckpoint();
  RowOperationsPB ops;
  AddTestRowWithNullableStringToPB(RowOperationsPB::UPDATE, schema_, 0, 1, nullptr, &ops);
  AddTestKeyToPB(RowOperationsPB::DELETE, schema_, 0, &ops);
  NO_FATALS(WriteOps(schema_, ops));

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const auto rows = RowRecords(resp);
  ASSERT_EQ(2, rows.size()) << resp.DebugString();
  int32_t v = -1;
  EXPECT_EQ(CDCOpTypePB::UPDATE, rows[0]->op_type());
  ASSERT_TRUE(GetInt32Col(*rows[0], /*before=*/true, "int_val", &v));
  EXPECT_EQ(0, v);
  ASSERT_TRUE(GetInt32Col(*rows[0], /*before=*/false, "int_val", &v));
  EXPECT_EQ(1, v);
  EXPECT_EQ(3, rows[0]->changes_size());
  EXPECT_EQ(CDCOpTypePB::DELETE, rows[1]->op_type());
  ASSERT_TRUE(GetInt32Col(*rows[1], /*before=*/true, "int_val", &v));
  EXPECT_EQ(1, v);
  EXPECT_EQ(1, rows[1]->changes_size());
}

// The images of an op are read with a lookup of each key it changes, not with
// a scan of the key range between its smallest and largest key.
TEST_F(CDCServiceTest, FullMode_RandomKeyBatchBoundedWork) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  constexpr int kNumRows = 200;
  InsertTestRowsRemote(0, kNumRows, 1);
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  const int64_t checkpoint = CaughtUpCheckpoint();
  RowOperationsPB ops;
  AddTestRowWithNullableStringToPB(RowOperationsPB::UPDATE, schema_, 0, -1, nullptr, &ops);
  AddTestRowWithNullableStringToPB(RowOperationsPB::UPDATE, schema_, kNumRows - 1, -1, nullptr,
                                   &ops);
  NO_FATALS(WriteOps(schema_, ops));

  const int64_t rows_read_before = CDCCounterValue(METRIC_cdc_full_image_rows_read);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(2, RowRecords(resp).size()) << resp.DebugString();
  // Each of the two rows is read as of before and after the op.
  EXPECT_EQ(4, CDCCounterValue(METRIC_cdc_full_image_rows_read) - rows_read_before);
}

// If the images of an op cannot be read before the deadline, the ops before it
// are returned, and the consumer polls again at once from right before it.
TEST_F(CDCServiceTest, FullMode_DeadlineReturnsPartialBatch) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 2, 2);
  const int64_t checkpoint = CaughtUpCheckpoint();
  UpdateTestRowRemote(0, 100);
  UpdateTestRowRemote(1, 101);
  const int64_t second_update_index = checkpoint + 2;

  FLAGS_cdc_inject_full_reconstruction_timeout_at_op_index = second_update_index;
  SCOPED_CLEANUP({ FLAGS_cdc_inject_full_reconstruction_timeout_at_op_index = -1; });
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
  EXPECT_EQ(CDCOpTypePB::UPDATE, resp.records(0).op_type());
  EXPECT_EQ(second_update_index - 1, resp.records(0).op_index());
  EXPECT_EQ(3, resp.records(0).old_changes_size());
  EXPECT_EQ(second_update_index - 1, resp.checkpoint_op_index());
  EXPECT_TRUE(resp.have_more_records());

  FLAGS_cdc_inject_full_reconstruction_timeout_at_op_index = -1;
  GetChangesResponsePB rest;
  ASSERT_OK(DoGetChanges(kTabletId, resp.checkpoint_op_index(), &rest));
  ASSERT_FALSE(rest.has_error()) << rest.error().DebugString();
  ASSERT_EQ(1, rest.records_size()) << rest.DebugString();
  EXPECT_EQ(second_update_index, rest.records(0).op_index());
  EXPECT_EQ(3, rest.records(0).old_changes_size());
}

// An INSERT carries the complete row as the tablet stored it, including the
// columns that the tablet filled with their defaults: those the client left
// unset, and those its schema does not know yet (it predates the ALTER that
// added them).
TEST_F(CDCServiceTest, FullMode_InsertIncludesServerDefaults) {
  const int32_t kDefault = 7;
  SchemaBuilder builder(*tablet_replica_->tablet_metadata()->schema());
  ASSERT_OK(builder.AddColumn(ColumnSchemaBuilder()
                                  .name("region")
                                  .type(INT32)
                                  .read_default(&kDefault)
                                  .write_default(&kDefault)));
  const Schema new_schema = builder.Build();
  ASSERT_OK(DoAlterSchema(new_schema, 1));
  const int64_t checkpoint = CaughtUpCheckpoint();
  RowOperationsPB ops;
  AddTestRowToPB(RowOperationsPB::INSERT, new_schema.CopyWithoutColumnIds(), 1, 1, "a", &ops);
  NO_FATALS(WriteOps(new_schema.CopyWithoutColumnIds(), ops));

  for (const auto mode : { CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL }) {
    SCOPED_TRACE(CDCStreamConfigPB::RecordType_Name(mode));
    SeedStreamConfig(mode);
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    const auto rows = RowRecords(resp);
    ASSERT_EQ(1, rows.size()) << resp.DebugString();
    EXPECT_EQ(4, rows[0]->changes_size());
    int32_t v = -1;
    ASSERT_TRUE(GetInt32Col(*rows[0], /*before=*/false, "region", &v)) << rows[0]->DebugString();
    EXPECT_EQ(kDefault, v);
  }
}

TEST_F(CDCServiceTest, FullMode_InsertFromStaleClientSchemaIncludesNewColumn) {
  const int32_t kDefault = 7;
  SchemaBuilder builder(*tablet_replica_->tablet_metadata()->schema());
  ASSERT_OK(builder.AddColumn(ColumnSchemaBuilder()
                                  .name("region")
                                  .type(INT32)
                                  .read_default(&kDefault)
                                  .write_default(&kDefault)));
  ASSERT_OK(DoAlterSchema(builder.Build(), 1));
  const int64_t checkpoint = CaughtUpCheckpoint();
  // The client still has the schema from before the ALTER.
  InsertTestRowsRemote(1, 1, 1);
  UpdateTestRowRemote(1, 100);

  SeedStreamConfig(CDCStreamConfigPB::FULL);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const auto rows = RowRecords(resp);
  ASSERT_EQ(2, rows.size()) << resp.DebugString();
  for (const auto* r : rows) {
    SCOPED_TRACE(r->DebugString());
    EXPECT_EQ(1, r->schema_version());
    EXPECT_EQ(1, r->image_schema_version());
    EXPECT_EQ(4, r->changes_size());
    int32_t v = -1;
    ASSERT_TRUE(GetInt32Col(*r, /*before=*/false, "region", &v));
    EXPECT_EQ(kDefault, v);
  }
}

// The column ids of the records are the tablet's, as in the schemas of DDL
// records, also after a column was dropped and another added, which leaves a
// gap in the ids.
TEST_F(CDCServiceTest, ColumnIdsMatchTabletSchemaAfterDropAdd) {
  InsertTestRowsRemote(0, 1, 1);
  SchemaBuilder builder(*tablet_replica_->tablet_metadata()->schema());
  ASSERT_OK(builder.RemoveColumn("string_val"));
  ASSERT_OK(builder.AddNullableColumn("c", INT32));
  const Schema new_schema = builder.Build();
  ASSERT_OK(DoAlterSchema(new_schema, 1));
  const Schema client_schema = new_schema.CopyWithoutColumnIds();
  {
    RowOperationsPB ops;
    KuduPartialRow row(&client_schema);
    ASSERT_OK(row.SetInt32("key", 1));
    ASSERT_OK(row.SetInt32("int_val", 1));
    ASSERT_OK(row.SetInt32("c", 1));
    RowOperationsPBEncoder enc(&ops);
    enc.Add(RowOperationsPB::INSERT, row);
    KuduPartialRow update(&client_schema);
    ASSERT_OK(update.SetInt32("key", 0));
    ASSERT_OK(update.SetInt32("c", 2));
    enc.Add(RowOperationsPB::UPDATE, update);
    NO_FATALS(WriteOps(client_schema, ops));
  }

  for (const auto mode : { CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL }) {
    SCOPED_TRACE(CDCStreamConfigPB::RecordType_Name(mode));
    SeedStreamConfig(mode);
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    int ddls = 0;
    int checked = 0;
    for (const auto& r : resp.records()) {
      if (r.op_type() == CDCOpTypePB::DDL) {
        ++ddls;
        Schema ddl_schema;
        ASSERT_OK(SchemaFromPB(r.new_schema(), &ddl_schema));
        for (size_t i = 0; i < new_schema.num_columns(); ++i) {
          EXPECT_EQ(new_schema.column_id(i),
                    ddl_schema.column_id(ddl_schema.find_column(new_schema.column(i).name())));
        }
        continue;
      }
      // The records after the ALTER.
      if (r.schema_version() != 1) {
        continue;
      }
      for (const auto* cols : { &r.changes(), &r.old_changes() }) {
        for (const auto& c : *cols) {
          SCOPED_TRACE(r.DebugString());
          const int idx = new_schema.find_column(c.column_name());
          ASSERT_NE(Schema::kColumnNotFound, idx);
          EXPECT_EQ(new_schema.column_id(idx), c.column_id());
          ++checked;
        }
      }
    }
    EXPECT_EQ(1, ddls) << resp.DebugString();
    EXPECT_GT(checked, 0) << resp.DebugString();
  }
}

// A tablet whose schema goes from version 0 to 2 in one ALTER (the master sends
// only the latest version) never had version 1: the records before the ALTER
// and the DDL record are stamped 0.
TEST_F(CDCServiceTest, SchemaVersion_SkippedVersionStampsActualPriorVersion) {
  InsertTestRowsRemote(0, 1, 1);
  SchemaBuilder builder(*tablet_replica_->tablet_metadata()->schema());
  ASSERT_OK(builder.AddNullableColumn("c", INT32));
  ASSERT_OK(DoAlterSchema(builder.Build(), 2));
  InsertTestRowsRemote(1, 1, 1);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  vector<std::pair<CDCOpTypePB, uint32_t>> stamps;
  for (const auto& r : resp.records()) {
    stamps.emplace_back(r.op_type(), r.schema_version());
  }
  const vector<std::pair<CDCOpTypePB, uint32_t>> expected = {
      { CDCOpTypePB::INSERT, 0 }, { CDCOpTypePB::DDL, 0 }, { CDCOpTypePB::INSERT, 2 } };
  EXPECT_EQ(expected, stamps) << resp.DebugString();
}

// Like the above, but the ALTER comes from a leader that does not record the
// version it replaced in the op (it predates that): the tablet's schema
// history gives it.
TEST_F(CDCServiceTest, SchemaVersion_SkippedVersionWithoutRecordedPriorVersion) {
  SCOPED_CLEANUP({ FLAGS_tablet_inject_omit_alter_prior_schema_version = false; });
  FLAGS_tablet_inject_omit_alter_prior_schema_version = true;
  InsertTestRowsRemote(0, 1, 1);
  SchemaBuilder builder(*tablet_replica_->tablet_metadata()->schema());
  ASSERT_OK(builder.AddNullableColumn("c", INT32));
  ASSERT_OK(DoAlterSchema(builder.Build(), 2));
  InsertTestRowsRemote(1, 1, 1);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChangesWithSchemaVersion(0, 0, /*need_schema_info=*/true, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  vector<std::tuple<CDCOpTypePB, uint32_t, uint32_t>> stamps;
  for (const auto& r : resp.records()) {
    stamps.emplace_back(r.op_type(), r.schema_version(), r.new_schema_version());
  }
  const vector<std::tuple<CDCOpTypePB, uint32_t, uint32_t>> expected = {
      { CDCOpTypePB::DDL, 0, 0 },
      { CDCOpTypePB::INSERT, 0, 0 },
      { CDCOpTypePB::DDL, 0, 2 },
      { CDCOpTypePB::INSERT, 2, 0 } };
  ASSERT_EQ(expected, stamps) << resp.DebugString();
  Schema base_schema;
  ASSERT_OK(SchemaFromPB(resp.records(0).new_schema(), &base_schema));
  EXPECT_EQ(Schema::kColumnNotFound, base_schema.find_column("c")) << base_schema.ToString();
}

// The read window can end before an ALTER that the tablet has applied. The
// ops in the window are decoded and stamped with the schema that they were
// applied with, and need_schema_info returns that schema, not the ALTER's.
TEST_F(CDCServiceTest, SchemaVersion_WindowEndsBeforeAppliedAlter) {
  const int64_t checkpoint = CaughtUpCheckpoint();
  InsertTestRowsRemote(0, 1, 1);
  SchemaBuilder builder(*tablet_replica_->tablet_metadata()->schema());
  ASSERT_OK(builder.RemoveColumn("string_val"));
  ASSERT_OK(DoAlterSchema(builder.Build(), 1));
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->schema_version());

  // The window ends after its first op, the INSERT.
  SCOPED_CLEANUP({ FLAGS_cdc_inject_wal_read_deadline_expired = false; });
  FLAGS_cdc_inject_wal_read_deadline_expired = true;
  for (const auto mode : { CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL }) {
    SCOPED_TRACE(CDCStreamConfigPB::RecordType_Name(mode));
    SeedStreamConfig(mode);
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChangesWithSchemaVersion(checkpoint, 0, /*need_schema_info=*/false, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
    EXPECT_TRUE(resp.have_more_records());
    const CDCRecordPB& insert = resp.records(0);
    EXPECT_EQ(CDCOpTypePB::INSERT, insert.op_type());
    EXPECT_EQ(0, insert.schema_version());
    EXPECT_NE(nullptr, FindCol(insert, /*before=*/false, "string_val")) << insert.DebugString();

    resp.Clear();
    ASSERT_OK(DoGetChangesWithSchemaVersion(checkpoint, -1, /*need_schema_info=*/true, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(2, resp.records_size()) << resp.DebugString();
    const CDCRecordPB& ddl = resp.records(0);
    EXPECT_EQ(CDCOpTypePB::DDL, ddl.op_type());
    EXPECT_EQ(0, ddl.new_schema_version());
    Schema schema;
    ASSERT_OK(SchemaFromPB(ddl.new_schema(), &schema));
    EXPECT_NE(Schema::kColumnNotFound, schema.find_column("string_val")) << schema.ToString();
    EXPECT_EQ(CDCOpTypePB::INSERT, resp.records(1).op_type());
    EXPECT_EQ(0, resp.records(1).schema_version());
  }
}

// FULL images are read with the tablet's current schema. After a column was
// dropped, they lack it, and image_schema_version says which schema they
// have, while schema_version is the one that the op was applied with.
TEST_F(CDCServiceTest, FullMode_ImagesAfterDropColumnAreStampedConsistently) {
  InsertTestRowsRemote(0, 1, 1);
  const int64_t checkpoint = CaughtUpCheckpoint();
  UpdateTestRowRemote(0, 100);
  SchemaBuilder builder(*tablet_replica_->tablet_metadata()->schema());
  ASSERT_OK(builder.RemoveColumn("string_val"));
  ASSERT_OK(DoAlterSchema(builder.Build(), 1));

  SeedStreamConfig(CDCStreamConfigPB::FULL);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const auto rows = RowRecords(resp);
  ASSERT_EQ(1, rows.size()) << resp.DebugString();
  const CDCRecordPB& update = *rows[0];
  SCOPED_TRACE(update.DebugString());
  EXPECT_EQ(CDCOpTypePB::UPDATE, update.op_type());
  EXPECT_EQ(0, update.schema_version());
  EXPECT_EQ(1, update.image_schema_version());
  for (const bool before : { true, false }) {
    EXPECT_EQ(nullptr, FindCol(update, before, "string_val"));
  }
  int32_t v = -1;
  ASSERT_TRUE(GetInt32Col(update, /*before=*/true, "int_val", &v));
  EXPECT_EQ(0, v);
  ASSERT_TRUE(GetInt32Col(update, /*before=*/false, "int_val", &v));
  EXPECT_EQ(100, v);
}

// The tablet decodes a write with its own schema, so an UPDATE of a column
// that a long-lived client still sees as immutable is applied once the column
// was made mutable. CDC must decode it the same way and emit the change.
TEST_F(CDCServiceTest, Update_StaleClientImmutableView_AfterAlterSetMutable_ChangeEmitted) {
  SchemaBuilder builder(*tablet_replica_->tablet_metadata()->schema());
  ASSERT_OK(builder.AddColumn(
      ColumnSchemaBuilder().name("c").type(INT32).nullable(true).immutable(true), false));
  const Schema immutable_schema = builder.Build();
  ASSERT_OK(DoAlterSchema(immutable_schema, 1));
  ColumnSchemaDelta delta("c");
  delta.immutable = false;
  ASSERT_OK(builder.ApplyColumnSchemaDelta(delta));
  ASSERT_OK(DoAlterSchema(builder.Build(), 2));
  InsertTestRowsRemote(0, 1, 1);
  const int64_t checkpoint = CaughtUpCheckpoint();

  // The client's schema still marks 'c' immutable.
  const Schema client_schema = immutable_schema.CopyWithoutColumnIds();
  RowOperationsPB ops;
  KuduPartialRow row(&client_schema);
  ASSERT_OK(row.SetInt32("key", 0));
  ASSERT_OK(row.SetInt32("c", 5));
  RowOperationsPBEncoder(&ops).Add(RowOperationsPB::UPDATE, row);
  NO_FATALS(WriteOps(client_schema, ops));

  for (const auto mode : { CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL }) {
    SCOPED_TRACE(CDCStreamConfigPB::RecordType_Name(mode));
    SeedStreamConfig(mode);
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    const auto rows = RowRecords(resp);
    ASSERT_EQ(1, rows.size()) << resp.DebugString();
    EXPECT_EQ(CDCOpTypePB::UPDATE, rows[0]->op_type());
    int32_t v = -1;
    ASSERT_TRUE(GetInt32Col(*rows[0], /*before=*/false, "c", &v)) << rows[0]->DebugString();
    EXPECT_EQ(5, v);
  }
}

// The schema history survives a restart, in which the tablet replays the
// ALTER from the WAL.
TEST_F(CDCServiceTest, NeedSchemaInfo_OlderSchemaSurvivesRestart) {
  const int64_t checkpoint = CaughtUpCheckpoint();
  InsertTestRowsRemote(0, 1, 1);
  AlterAddColumn(schema_, "c1", /*new_version=*/1);
  ASSERT_OK(RestartTabletServer());
  ASSERT_OK(WaitForTabletRunning(kTabletId));

  GetChangesResponsePB resp;
  ASSERT_EVENTUALLY([&] {
    resp.Clear();
    ASSERT_OK(DoGetChangesWithSchemaVersion(checkpoint, -1, /*need_schema_info=*/true, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  });
  ASSERT_GE(resp.records_size(), 3) << resp.DebugString();
  EXPECT_EQ(CDCOpTypePB::DDL, resp.records(0).op_type());
  EXPECT_EQ(0, resp.records(0).new_schema_version());
  EXPECT_EQ(schema_.num_columns(), resp.records(0).new_schema().columns_size());
  EXPECT_EQ(CDCOpTypePB::INSERT, resp.records(1).op_type());
  EXPECT_EQ(0, resp.records(1).schema_version());
  EXPECT_EQ(CDCOpTypePB::DDL, resp.records(2).op_type());
}

// Regression test for the FULL after-image apply-race: a committed op is
// readable from the WAL (and visible in COMMITTED_OPID) BEFORE the apply pool
// applies it to the MemRowSet. The FULL reconstruction must wait for the op to
// be applied before scanning MVCC, or it would emit a stale/empty after-image.
TEST_F(CDCServiceTest, FullMode_AfterImageWaitsForApply) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0

  // Hold the UPDATE in the apply phase so it is committed-but-not-applied while
  // we poll. The write RPC only returns after apply, so run it on a background
  // thread and poll GetChanges from the main thread during the apply window.
  FLAGS_tablet_inject_latency_on_apply_write_op_ms = 4000;
  std::thread updater([&]() {
    UpdateTestRowRemote(0, 12345);   // int_val -> 12345 (blocks ~4s in apply)
  });

  // Poll until the UPDATE surfaces. Whenever it does, its after-image must
  // already reflect the applied value (12345), never the stale pre-update row.
  bool saw_update = false;
  const MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  while (MonoTime::Now() < deadline && !saw_update) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (int i = 0; i < resp.records_size(); ++i) {
      if (resp.records(i).op_type() != CDCOpTypePB::UPDATE) continue;
      const CDCRecordPB& upd = resp.records(i);
      int32_t after_val = -1;
      ASSERT_TRUE(GetInt32Col(upd, /*before=*/false, "int_val", &after_val))
          << "after-image missing int_val: " << upd.DebugString();
      EXPECT_EQ(12345, after_val)
          << "stale after-image (apply-race not fixed): " << upd.DebugString();
      EXPECT_EQ(3, upd.changes_size());  // complete after-image, not key-only
      saw_update = true;
      break;
    }
  }
  updater.join();
  ASSERT_TRUE(saw_update) << "UPDATE record never surfaced within deadline";
}

TEST_F(CDCServiceTest, FullMode_DeleteHasBeforeImage) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  DeleteTestRowsRemote(0, 1);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  const CDCRecordPB* del = nullptr;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::DELETE) del = &resp.records(i);
  }
  ASSERT_NE(nullptr, del) << resp.DebugString();

  // Before-image is the full row; after-image ('changes') stays key-only.
  EXPECT_EQ(3, del->old_changes_size());
  int32_t before_val = -1;
  ASSERT_TRUE(GetInt32Col(*del, /*before=*/true, "int_val", &before_val));
  EXPECT_EQ(0, before_val);
  EXPECT_EQ(1, del->changes_size());  // primary key only
}

// FULL-mode before/after-image reconstruction must span an online schema
// change. A row inserted before ADD COLUMN, then updated, must yield before-
// and after-images that carry the newly added column -- reported present-but-
// null for the pre-existing row, which predates the column (YB:
// TestAddColumnBeforeImage, TestBeforeImageForNewlyAddedColumn). The
// reconstruction scan projects the *current* tablet schema onto rows written
// under the old schema, so the added column materializes as its default (null)
// for the old row rather than being dropped from the image.
TEST_F(CDCServiceTest, FullMode_BeforeImageAcrossAddColumn) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);

  // Pre-existing row on the original 3-column schema: key=0, int_val=0.
  InsertTestRowsRemote(0, 1, 1);

  // Online ALTER appends a nullable INT32 column (v0 -> v1).
  const string kAdded = "added";
  const Schema new_schema = AlterAddColumn(schema_, kAdded, /*new_version=*/1);
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->schema_version());

  // Update int_val on the OLD schema; the added column stays null for this row.
  UpdateTestRowRemote(0, 12345);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  const CDCRecordPB* upd = nullptr;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::UPDATE) upd = &resp.records(i);
  }
  ASSERT_NE(nullptr, upd) << resp.DebugString();

  // Before-image is the complete pre-update row reconstructed under the NEW
  // schema: int_val==0 and the added column present-but-null.
  int32_t before_int = -1;
  ASSERT_TRUE(GetInt32Col(*upd, /*before=*/true, "int_val", &before_int));
  EXPECT_EQ(0, before_int);
  const CDCColumnValuePB* before_added = FindCol(*upd, /*before=*/true, kAdded);
  ASSERT_NE(nullptr, before_added)
      << "before-image dropped the added column: " << upd->DebugString();
  EXPECT_TRUE(before_added->is_null())
      << "added column must be null in the before-image of a row that predates "
         "it: " << before_added->DebugString();

  // After-image reflects the update (int_val==12345) and carries the added
  // column as null (the update did not set it).
  int32_t after_int = -1;
  ASSERT_TRUE(GetInt32Col(*upd, /*before=*/false, "int_val", &after_int));
  EXPECT_EQ(12345, after_int);
  const CDCColumnValuePB* after_added = FindCol(*upd, /*before=*/false, kAdded);
  ASSERT_NE(nullptr, after_added)
      << "after-image dropped the added column: " << upd->DebugString();
  EXPECT_TRUE(after_added->is_null()) << after_added->DebugString();

  // Now update the added column itself (key + added only, on the NEW schema),
  // leaving int_val unchanged. The before-image must still report the added
  // column as null (its value just before this update), and the after-image
  // must carry the new non-null value -- a null-to-non-null transition on the
  // freshly added column.
  const int64_t checkpoint = resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);
  {
    // Client writes carry a column-ID-free schema; AlterAddColumn returns one
    // built with IDs, so strip them for the RPC.
    const Schema client_schema = new_schema.CopyWithoutColumnIds();
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(client_schema, req.mutable_schema()));
    KuduPartialRow row(&client_schema);
    ASSERT_OK(row.SetInt32("key", 0));
    ASSERT_OK(row.SetInt32(kAdded, 999));
    RowOperationsPBEncoder enc(req.mutable_row_operations());
    enc.Add(RowOperationsPB::UPDATE, row);

    tserver::WriteResponsePB wresp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &wresp, &rpc));
    ASSERT_FALSE(wresp.has_error()) << wresp.error().DebugString();
    ASSERT_EQ(0, wresp.per_row_errors_size()) << wresp.DebugString();
  }

  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp2));
  ASSERT_FALSE(resp2.has_error()) << resp2.error().DebugString();

  const CDCRecordPB* upd2 = nullptr;
  for (int i = 0; i < resp2.records_size(); ++i) {
    if (resp2.records(i).op_type() == CDCOpTypePB::UPDATE) upd2 = &resp2.records(i);
  }
  ASSERT_NE(nullptr, upd2) << resp2.DebugString();

  const CDCColumnValuePB* b2 = FindCol(*upd2, /*before=*/true, kAdded);
  ASSERT_NE(nullptr, b2) << upd2->DebugString();
  EXPECT_TRUE(b2->is_null())
      << "added column must be null in the before-image (its value before this "
         "update): " << b2->DebugString();
  int32_t after_added_val = -1;
  ASSERT_TRUE(GetInt32Col(*upd2, /*before=*/false, kAdded, &after_added_val))
      << "after-image missing the updated added column: " << upd2->DebugString();
  EXPECT_EQ(999, after_added_val);
}

// FULL-mode before/after images must carry a nullable column explicitly even
// when it is null on both sides of an update (YB:
// TestBeforeImageForNullOnNullUpdates). Updating a non-null column while a
// nullable column stays null must emit that column in both images with
// is_null=true -- never omit it and never report a stale/empty value.
TEST_F(CDCServiceTest, FullMode_BeforeImageNullToNullUpdate) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);

  // Pre-existing row with string_val explicitly NULL: key=0, int_val=0.
  {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
    AddTestRowWithNullableStringToPB(RowOperationsPB::INSERT, schema_, /*key=*/0,
                                     /*int_val=*/0, /*string_val=*/nullptr,
                                     req.mutable_row_operations());
    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }

  // Update only int_val (key + int_val); string_val stays NULL across the update.
  {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
    KuduPartialRow row(&schema_);
    ASSERT_OK(row.SetInt32("key", 0));
    ASSERT_OK(row.SetInt32("int_val", 77));
    RowOperationsPBEncoder enc(req.mutable_row_operations());
    enc.Add(RowOperationsPB::UPDATE, row);

    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  const CDCRecordPB* upd = nullptr;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::UPDATE) upd = &resp.records(i);
  }
  ASSERT_NE(nullptr, upd) << resp.DebugString();

  // int_val changed 0 -> 77.
  int32_t before_int = -1;
  int32_t after_int = -1;
  ASSERT_TRUE(GetInt32Col(*upd, /*before=*/true, "int_val", &before_int));
  ASSERT_TRUE(GetInt32Col(*upd, /*before=*/false, "int_val", &after_int));
  EXPECT_EQ(0, before_int);
  EXPECT_EQ(77, after_int);

  // string_val is present in BOTH images, explicitly null on each side.
  const CDCColumnValuePB* before_str = FindCol(*upd, /*before=*/true, "string_val");
  ASSERT_NE(nullptr, before_str)
      << "before-image dropped the null column: " << upd->DebugString();
  EXPECT_TRUE(before_str->is_null()) << before_str->DebugString();
  const CDCColumnValuePB* after_str = FindCol(*upd, /*before=*/false, "string_val");
  ASSERT_NE(nullptr, after_str)
      << "after-image dropped the null column: " << upd->DebugString();
  EXPECT_TRUE(after_str->is_null()) << after_str->DebugString();
}

TEST_F(CDCServiceTest, FullMode_SetsHistoryFloor) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 7);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  // After a FULL read the tablet's CDC history floor is pinned (non-zero).
  EXPECT_GT(tablet_replica_->shared_tablet()->cdc_history_floor().value(), 0);
}

TEST_F(CDCServiceTest, ChangeMode_DoesNotSetHistoryFloor) {
  // Default CHANGE-mode stream (no seeded config; fetch fails -> CHANGE).
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 7);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  EXPECT_EQ(0, tablet_replica_->shared_tablet()->cdc_history_floor().value());
  // And CHANGE-mode UPDATE has no before-image.
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::UPDATE) {
      EXPECT_EQ(0, resp.records(i).old_changes_size());
    }
  }
}

// FULL-mode before-image vs. an aggressive compaction, positive case: while a
// FULL stream holds the tablet's CDC history floor, a forced merge compaction
// with history GC turned all the way up (tablet_history_max_age_sec = 0, so the
// ancient history mark would otherwise sit at "now") must NOT reclaim the UNDO
// history the stream still needs. Re-reading the same batch after the compaction
// still reconstructs the correct before-image. This proves the floor actually
// protects against a real compaction (YB: TestCompactionWithBeforeImage...),
// not merely that the floor value is stored (FullMode_SetsHistoryFloor).
TEST_F(CDCServiceTest, FullMode_HistoryFloorProtectsBeforeImageAcrossCompaction) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  UpdateTestRowRemote(0, 12345);   // int_val 0 -> 12345

  // First FULL read reconstructs the before-image (0) and, as a side effect,
  // pins the history floor at this batch's minimum op timestamp so the UNDO
  // history behind the update is retained.
  GetChangesResponsePB before_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &before_resp));
  ASSERT_FALSE(before_resp.has_error()) << before_resp.error().DebugString();
  ASSERT_GT(tablet_replica_->shared_tablet()->cdc_history_floor().value(), 0);

  // Turn history GC all the way up and force a full merge compaction. Absent the
  // floor this would advance the ancient history mark to ~now and reclaim the
  // update's UNDO delta.
  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));

  // Re-read the same batch: the before-image is still present and correct (0),
  // and the after-image is the full post-update row -- the floor held.
  GetChangesResponsePB after_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &after_resp));
  ASSERT_FALSE(after_resp.has_error()) << after_resp.error().DebugString();

  const CDCRecordPB* upd = nullptr;
  for (int i = 0; i < after_resp.records_size(); ++i) {
    if (after_resp.records(i).op_type() == CDCOpTypePB::UPDATE) {
      upd = &after_resp.records(i);
    }
  }
  ASSERT_NE(nullptr, upd) << after_resp.DebugString();
  int32_t before_val = -1;
  ASSERT_TRUE(GetInt32Col(*upd, /*before=*/true, "int_val", &before_val))
      << upd->DebugString();
  EXPECT_EQ(0, before_val);
  int32_t after_val = -1;
  ASSERT_TRUE(GetInt32Col(*upd, /*before=*/false, "int_val", &after_val));
  EXPECT_EQ(12345, after_val);
  EXPECT_EQ(3, upd->old_changes_size());
}

// FULL-mode before-image vs. an aggressive compaction, negative case: when the
// UNDO history a before-image needs has genuinely been garbage-collected --
// because no CDC floor was protecting it when the compaction ran (a stream that
// lapsed, or CDC enabled on a table that already had aggressive history GC) --
// a subsequent FULL replay of the affected op must return a well-defined
// HISTORY_EXPIRED error, telling the consumer to re-establish from a snapshot.
// It must NOT silently emit the *current* row value as the before-image.
//
// This is a regression test for a real correctness bug: the FULL path re-pins
// the history floor to each batch's minimum op timestamp *before* reconstructing
// its images, which lowered the current ancient history mark back below the
// point at which the earlier (unprotected) compaction had already reclaimed the
// UNDO history. The current-AHM guard was thus fooled into scanning reclaimed
// history, and a time-travel scan below the GC point returns the live row --
// so an UPDATE's before-image came back as the post-update value with no error.
// The fix gates reconstruction on a monotonic history-GC water mark (the highest
// AHM ever actually applied), which the per-batch floor re-pin cannot lower.
// YB analog: TestCompactionWithBeforeImageGetChangesCallFailed.
TEST_F(CDCServiceTest, FullMode_BeforeImageGcedReturnsHistoryExpired) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  UpdateTestRowRemote(0, 12345);   // int_val 0 -> 12345

  // No FULL read yet, so no history floor is pinned. Turn history GC all the way
  // up and force a merge compaction: the update's UNDO delta -- the only source
  // for the before-image (0) -- is reclaimed, and the history-GC water mark
  // advances to ~now.
  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
  ASSERT_EQ(0, tablet_replica_->shared_tablet()->cdc_history_floor().value())
      << "no floor should have been pinned before the compaction";

  // Replaying the update in FULL mode must fail with HISTORY_EXPIRED (the RPC
  // itself succeeds; the error is in-band), not emit a stale before-image.
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::HISTORY_EXPIRED, resp.error().code())
      << resp.error().DebugString();

  // The batch is aborted rather than partially emitted: no UPDATE record with a
  // fabricated before-image leaks out.
  for (int i = 0; i < resp.records_size(); ++i) {
    EXPECT_NE(CDCOpTypePB::UPDATE, resp.records(i).op_type())
        << "stale UPDATE emitted despite GC'd history: "
        << resp.records(i).DebugString();
  }
}

// The history-GC water mark must survive a restart. Same scenario as
// FullMode_BeforeImageGcedReturnsHistoryExpired, with the tablet server
// restarted between the compaction and the replay. If the mark were kept only
// in memory, it would restart at 0; the replay re-pins the floor to its batch
// minimum, so the current-AHM check passes too, and the scan over the reclaimed
// UNDO history would return the post-update row as the before-image.
TEST_F(CDCServiceTest, FullMode_BeforeImageGcedReturnsHistoryExpiredAfterRestart) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  UpdateTestRowRemote(0, 12345);   // int_val 0 -> 12345

  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
  const Timestamp water_mark =
      tablet_replica_->shared_tablet()->cdc_history_gc_water_mark();
  ASSERT_GT(water_mark.value(), 0);

  // Restart with a long history max age, so after the restart only the
  // persisted water mark knows that the update's UNDO history is gone.
  FLAGS_tablet_history_max_age_sec = 3600;
  ASSERT_OK(RestartTabletServer());
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  ASSERT_EQ(water_mark, tablet_replica_->shared_tablet()->cdc_history_gc_water_mark());

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::HISTORY_EXPIRED, resp.error().code())
      << resp.error().DebugString();
  for (const auto& r : resp.records()) {
    EXPECT_NE(CDCOpTypePB::UPDATE, r.op_type())
        << "stale UPDATE emitted after restart: " << r.DebugString();
  }
}

// The history-GC water mark of a tablet whose superblock does not record one
// is unknown, not 0. That is the state after an upgrade from a binary that did
// not track the mark (or a downgrade and re-upgrade, or a tablet copy from such
// a binary): the old binary may have GC'd UNDO history, and this binary cannot
// tell how far. Same scenario as
// FullMode_BeforeImageGcedReturnsHistoryExpiredAfterRestart, with the mark
// removed from the superblock while the tablet server is down. If the missing
// mark were read as 0, the replay would pin the floor to its batch minimum, the
// current-AHM check would pass, and the read over the reclaimed UNDO history
// would return the post-update row as the before-image.
TEST_F(CDCServiceTest, FullMode_BeforeImageGcedReturnsHistoryExpiredAfterUpgrade) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  UpdateTestRowRemote(0, 12345);   // int_val 0 -> 12345

  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
  const Timestamp water_mark =
      tablet_replica_->shared_tablet()->cdc_history_gc_water_mark();
  ASSERT_GT(water_mark.value(), 0);

  // Rewrite the superblock without the water mark, as a binary that does not
  // know the field would have written it.
  const string sb_path =
      mini_server_->server()->fs_manager()->GetTabletMetadataPath(kTabletId);
  ShutdownTablet();
  {
    Env* env = Env::Default();
    tablet::TabletSuperBlockPB sb;
    ASSERT_OK(pb_util::ReadPBContainerFromPath(env, sb_path, &sb, pb_util::SENSITIVE));
    ASSERT_TRUE(sb.has_cdc_history_gc_water_mark());
    sb.clear_cdc_history_gc_water_mark();
    ASSERT_OK(pb_util::WritePBContainerToPath(env, sb_path, sb, pb_util::OVERWRITE,
                                              pb_util::SYNC, pb_util::SENSITIVE));
  }

  FLAGS_tablet_history_max_age_sec = 3600;
  ASSERT_OK(RestartTabletServer());
  SeedStreamConfig(CDCStreamConfigPB::FULL);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::HISTORY_EXPIRED, resp.error().code())
      << resp.error().DebugString();
  for (const auto& r : resp.records()) {
    EXPECT_NE(CDCOpTypePB::UPDATE, r.op_type())
        << "stale UPDATE emitted after upgrade: " << r.DebugString();
  }

  // The tablet seeded the unknown mark with the time it opened, which is above
  // the point the earlier GC reached, and persisted it.
  const Timestamp seeded =
      tablet_replica_->shared_tablet()->cdc_history_gc_water_mark();
  ASSERT_GT(seeded.value(), water_mark.value());
  {
    tablet::TabletSuperBlockPB sb;
    ASSERT_OK(tablet_replica_->tablet()->metadata()->ReadSuperBlockFromDisk(&sb));
    ASSERT_TRUE(sb.has_cdc_history_gc_water_mark());
    ASSERT_EQ(seeded.value(), sb.cdc_history_gc_water_mark());
  }

  // The seed is durable: another restart keeps it rather than seeding anew.
  ASSERT_OK(RestartTabletServer());
  ASSERT_EQ(seeded, tablet_replica_->shared_tablet()->cdc_history_gc_water_mark());
}

// FULL reconstruction must recheck the history after it reads the row images,
// not only before. The session floor that protects the batch can be cleared
// while the images are read (a release valve opening, or a master release),
// and a GC can then remove the history the reads need. Here the hook between
// the pre-check and the reads clears every floor holder and runs a GC that
// removes the update's UNDO history: the before-image read then returns the
// post-update row. That must be reported as HISTORY_EXPIRED, not emitted.
TEST_F(CDCServiceTest, FullMode_HistoryGcedDuringReconstructionReturnsHistoryExpired) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  const int64_t checkpoint = CaughtUpCheckpoint();
  UpdateTestRowRemote(0, 12345);   // int_val 0 -> 12345

  // History GC all the way up: only the floor that the FULL read pins keeps the
  // update's UNDO history.
  FLAGS_tablet_history_max_age_sec = 0;
  const auto tablet = tablet_replica_->shared_tablet();
  int hook_calls = 0;
  Status hook_status;
  SetReconstructImagesHookForTests([&]() {
    if (hook_calls++ > 0) {
      return;
    }
    ASSERT_GT(tablet->cdc_history_floor().value(), 0);
    tablet->ClearCDCHistoryFloorHolders();
    hook_status = tablet->Flush();
    if (hook_status.ok()) {
      hook_status = tablet->Compact(tablet::Tablet::FORCE_COMPACT_ALL);
    }
  });
  SCOPED_CLEANUP({ SetReconstructImagesHookForTests(nullptr); });

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_EQ(1, hook_calls) << resp.DebugString();
  ASSERT_OK(hook_status);
  ASSERT_GT(tablet->cdc_history_gc_water_mark().value(), 0);
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::HISTORY_EXPIRED, resp.error().code())
      << resp.error().DebugString();
  for (const auto& r : resp.records()) {
    EXPECT_NE(CDCOpTypePB::UPDATE, r.op_type())
        << "stale UPDATE emitted despite a GC during reconstruction: "
        << r.DebugString();
  }
}

// Two FULL streams on one tablet: a stream that is caught up must not raise the
// history floor above what a lagging stream still needs. Each stream holds its
// own floor and the tablet clamps the ancient history mark to the minimum, so
// a compaction with history GC turned all the way up keeps the lagging
// stream's before-image.
TEST_F(CDCServiceTest, FullMode_FastStreamDoesNotUnprotectLaggingStream) {
  const string kLagging = "test-stream-1";
  const string kFast = "test-stream-2";
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::NEVER, kLagging);
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::NEVER, kFast);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  UpdateTestRowRemote(0, 111);     // int_val 0 -> 111

  // The lagging stream reads the first update and then stops (no checkpoint).
  GetChangesResponsePB lag_resp;
  ASSERT_OK(DoGetChangesForStream(kLagging, 0, &lag_resp));
  ASSERT_FALSE(lag_resp.has_error()) << lag_resp.error().DebugString();
  ASSERT_EQ(0, FirstUpdateBeforeIntVal(lag_resp)) << lag_resp.DebugString();
  const int64_t cp = lag_resp.checkpoint_op_index();
  ASSERT_GT(cp, 0);
  const Timestamp lagging_floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  ASSERT_GT(lagging_floor.value(), 0);

  // The fast stream reads only the newer update.
  UpdateTestRowRemote(0, 222);     // int_val 111 -> 222
  GetChangesResponsePB fast_resp;
  ASSERT_OK(DoGetChangesForStream(kFast, cp, &fast_resp));
  ASSERT_FALSE(fast_resp.has_error()) << fast_resp.error().DebugString();
  ASSERT_EQ(111, FirstUpdateBeforeIntVal(fast_resp)) << fast_resp.DebugString();
  EXPECT_EQ(lagging_floor, tablet_replica_->shared_tablet()->cdc_history_floor());

  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));

  // The lagging stream still gets the correct before-image of the first update.
  GetChangesResponsePB replay;
  ASSERT_OK(DoGetChangesForStream(kLagging, 0, &replay));
  ASSERT_FALSE(replay.has_error()) << replay.error().DebugString();
  EXPECT_EQ(0, FirstUpdateBeforeIntVal(replay)) << replay.DebugString();
}

// A single FULL stream that reads ahead of its checkpoint must not unprotect
// the history its checkpoint still needs: the consumer resumes from the
// checkpoint after a crash. The floor advances only when the checkpoint does.
TEST_F(CDCServiceTest, FullMode_ReadAheadKeepsCheckpointHistory) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);   // key=0, int_val=0
  UpdateTestRowRemote(0, 111);     // int_val 0 -> 111

  GetChangesResponsePB first;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &first));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  const int64_t cp = first.checkpoint_op_index();
  ASSERT_GT(cp, 0);
  const Timestamp checkpoint_floor =
      tablet_replica_->shared_tablet()->cdc_history_floor();
  ASSERT_GT(checkpoint_floor.value(), 0);

  // Read ahead from 'cp' without checkpointing it.
  UpdateTestRowRemote(0, 222);
  GetChangesResponsePB ahead;
  ASSERT_OK(DoGetChanges(kTabletId, cp, &ahead));
  ASSERT_FALSE(ahead.has_error()) << ahead.error().DebugString();
  ASSERT_EQ(111, FirstUpdateBeforeIntVal(ahead)) << ahead.DebugString();
  EXPECT_EQ(checkpoint_floor, tablet_replica_->shared_tablet()->cdc_history_floor());

  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));

  // Resuming from the (never advanced) checkpoint still works.
  GetChangesResponsePB replay;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &replay));
  ASSERT_FALSE(replay.has_error()) << replay.error().DebugString();
  EXPECT_EQ(0, FirstUpdateBeforeIntVal(replay)) << replay.DebugString();

  // Checkpointing 'cp' advances the floor to the batch read from 'cp'.
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, cp, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  EXPECT_GT(tablet_replica_->shared_tablet()->cdc_history_floor(), checkpoint_floor);

  // A consumer that restarts from an older checkpoint re-reads from there, and
  // the floor moves back down with it, also across further read-ahead.
  GetChangesResponsePB rewind;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &rewind));
  ASSERT_FALSE(rewind.has_error()) << rewind.error().DebugString();
  EXPECT_EQ(checkpoint_floor, tablet_replica_->shared_tablet()->cdc_history_floor());
  GetChangesResponsePB ahead_again;
  ASSERT_OK(DoGetChanges(kTabletId, cp, &ahead_again));
  ASSERT_FALSE(ahead_again.has_error()) << ahead_again.error().DebugString();
  EXPECT_EQ(checkpoint_floor, tablet_replica_->shared_tablet()->cdc_history_floor());
}

// Deleting one of two FULL streams on a tablet releases the history floor the
// deleted stream held. The release is fanned out with skip_barrier_update set
// (the aggregate barrier is left to the next maintenance pass), so it is the
// only thing that unpins history the deleted stream no longer needs.
TEST_F(CDCServiceTest, FullMode_StreamDeleteReleasesItsHistoryFloor) {
  const string kDeleted = "test-stream-1";
  const string kSurvivor = "test-stream-2";
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::NEVER, kDeleted);
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::NEVER, kSurvivor);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 111);

  GetChangesResponsePB deleted_resp;
  ASSERT_OK(DoGetChangesForStream(kDeleted, 0, &deleted_resp));
  ASSERT_FALSE(deleted_resp.has_error()) << deleted_resp.error().DebugString();
  const int64_t cp = deleted_resp.checkpoint_op_index();
  ASSERT_GT(cp, 0);
  const Timestamp deleted_floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  ASSERT_GT(deleted_floor.value(), 0);

  // The survivor reads only the newer update, so it holds a higher floor.
  UpdateTestRowRemote(0, 222);
  GetChangesResponsePB survivor_resp;
  ASSERT_OK(DoGetChangesForStream(kSurvivor, cp, &survivor_resp));
  ASSERT_FALSE(survivor_resp.has_error()) << survivor_resp.error().DebugString();
  ASSERT_GT(survivor_resp.records_size(), 0);
  uint64_t survivor_min_ts = std::numeric_limits<uint64_t>::max();
  for (const auto& r : survivor_resp.records()) {
    survivor_min_ts = std::min<uint64_t>(survivor_min_ts, r.timestamp());
  }
  ASSERT_EQ(deleted_floor, tablet_replica_->shared_tablet()->cdc_history_floor());

  ASSERT_OK(mini_server_->server()->cdc_service()->SetRetentionBarrier(
      kTabletId, /*min_retained_op_index=*/0, /*history_safe_time_micros=*/0,
      /*barrier_seq=*/0, /*release_consumer_stream_id=*/kDeleted,
      /*skip_barrier_update=*/true));
  const Timestamp floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  EXPECT_GT(floor, deleted_floor);
  EXPECT_LE(floor.value(), survivor_min_ts);
}

// A session that has been idle for longer than --cdc_stream_idle_expiry_ms
// stops holding its own history floor at the next master push, so an
// abandoned session cannot pin history below the master's floor. An active
// session keeps its floor, and an idle one pins it again on its next read.
TEST_F(CDCServiceTest, FullMode_IdleSessionFloorDroppedOnMasterPush) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 111);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const Timestamp session_floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  ASSERT_GT(session_floor.value(), 0);

  // The master's floor is above the session's.
  const uint64_t master_micros = GetCurrentTimeMicros();
  const Timestamp master_floor = HybridClock::TimestampFromMicroseconds(master_micros);
  ASSERT_GT(master_floor, session_floor);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/0,
                                     master_micros, /*barrier_seq=*/1));
  EXPECT_EQ(session_floor, tablet_replica_->shared_tablet()->cdc_history_floor());

  FLAGS_cdc_stream_idle_expiry_ms = 1;
  SleepFor(MonoDelta::FromMilliseconds(10));
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/0,
                                     master_micros, /*barrier_seq=*/2));
  EXPECT_EQ(master_floor, tablet_replica_->shared_tablet()->cdc_history_floor());

  // The next read from the checkpoint pins the session's floor again.
  resp.Clear();
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  EXPECT_EQ(session_floor, tablet_replica_->shared_tablet()->cdc_history_floor());
}

// Once the master released the tablet (barrier -1), as it does when it stops
// counting its only stream as stale, a consumer that keeps polling from its
// checkpoint without advancing it pins no history again: no master update
// would ever drop that floor. The stream holds history again once the master
// pins a barrier again.
TEST_F(CDCServiceTest, FullMode_SessionFloorNotRepinnedAfterRelease) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 111);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  const Timestamp session_floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  ASSERT_GT(session_floor.value(), 0);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0, /*barrier_seq=*/1));
  ASSERT_EQ(Timestamp(0), tablet_replica_->shared_tablet()->cdc_history_floor());

  // The stuck consumer polls from its checkpoint again and again, and
  // checkpoints the same position.
  for (int i = 0; i < 3; ++i) {
    resp.Clear();
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_GT(resp.records_size(), 0);
    EXPECT_EQ(Timestamp(0), tablet_replica_->shared_tablet()->cdc_history_floor());
  }
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, 0, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  EXPECT_EQ(Timestamp(0), tablet_replica_->shared_tablet()->cdc_history_floor());

  // The master pins the tablet again: the next read holds the floor again.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/0,
                                     /*history_safe_time_micros=*/0, /*barrier_seq=*/2));
  resp.Clear();
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  EXPECT_EQ(session_floor, tablet_replica_->shared_tablet()->cdc_history_floor());
}

// A read-only client ('kudu cdc consume' without --checkpoint) is not the
// consumer that resumes from the stream's checkpoint: its reads pin no
// history, and they do not move the floor of the consumer's session down to
// the position the read-only client reads from.
TEST_F(CDCServiceTest, FullMode_ReadOnlyPollDoesNotPinHistory) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 111);

  const auto read_only_get_changes = [&](int64_t from_op_index) {
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(from_op_index);
    req.set_read_only(true);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(cdc_proxy_->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_GT(resp.records_size(), 0);
  };

  // A read-only client alone pins nothing.
  NO_FATALS(read_only_get_changes(0));
  EXPECT_EQ(Timestamp(0), tablet_replica_->shared_tablet()->cdc_history_floor());

  // The consumer reads the first update and checkpoints after it.
  GetChangesResponsePB first;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &first));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  const int64_t cp = first.checkpoint_op_index();
  ASSERT_GT(cp, 0);
  const Timestamp first_floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  ASSERT_GT(first_floor.value(), 0);
  UpdateTestRowRemote(0, 222);
  GetChangesResponsePB second;
  ASSERT_OK(DoGetChanges(kTabletId, cp, &second));
  ASSERT_FALSE(second.has_error()) << second.error().DebugString();
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, cp, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  const Timestamp consumer_floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  ASSERT_GT(consumer_floor, first_floor);

  // A read-only client that reads from the start leaves the consumer's floor.
  NO_FATALS(read_only_get_changes(0));
  EXPECT_EQ(consumer_floor, tablet_replica_->shared_tablet()->cdc_history_floor());
}

// On a tablet shared with a healthy stream, the master keeps refreshing the
// barrier, but no longer counts a stream whose consumer polls a fixed position
// without checkpointing. That session's floor is held only for
// --cdc_session_history_floor_max_age_secs: the next master refresh drops it,
// although the session is not idle, and the stuck consumer's next read does
// not pin it again. The healthy stream's floor stays.
TEST_F(CDCServiceTest, FullMode_StaleSessionFloorDroppedOnSharedTablet) {
  const string kHealthy = "test-stream-1";
  const string kStuck = "test-stream-2";
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::NEVER, kHealthy);
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::NEVER, kStuck);
  PinRetentionBarrier();
  InsertTestRowsRemote(0, 1, 1);
  UpdateTestRowRemote(0, 111);

  // The stuck stream reads the first update and never checkpoints.
  GetChangesResponsePB stuck_resp;
  ASSERT_OK(DoGetChangesForStream(kStuck, 0, &stuck_resp));
  ASSERT_FALSE(stuck_resp.has_error()) << stuck_resp.error().DebugString();
  const int64_t cp = stuck_resp.checkpoint_op_index();
  ASSERT_GT(cp, 0);
  const Timestamp stuck_floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  ASSERT_GT(stuck_floor.value(), 0);

  // Let the stuck stream's floor grow older than the age bound used below.
  // The healthy stream's ops come after that, so its floor stays younger than
  // the bound for the rest of the test.
  constexpr int64_t kMaxAgeSecs = 3;
  const int64_t stuck_floor_micros = HybridClock::GetPhysicalValueMicros(stuck_floor);
  const int64_t wait_micros =
      stuck_floor_micros + kMaxAgeSecs * 1000000 + 100000 - GetCurrentTimeMicros();
  if (wait_micros > 0) {
    SleepFor(MonoDelta::FromMicroseconds(wait_micros));
  }

  // The healthy stream reads the newer update and checkpoints its position.
  UpdateTestRowRemote(0, 222);
  GetChangesResponsePB healthy_resp;
  ASSERT_OK(DoGetChangesForStream(kHealthy, cp, &healthy_resp));
  ASSERT_FALSE(healthy_resp.has_error()) << healthy_resp.error().DebugString();
  ASSERT_GT(healthy_resp.records_size(), 0);
  uint64_t healthy_min_ts = std::numeric_limits<uint64_t>::max();
  for (const auto& r : healthy_resp.records()) {
    healthy_min_ts = std::min<uint64_t>(healthy_min_ts, r.timestamp());
  }
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, cp, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  ASSERT_EQ(stuck_floor, tablet_replica_->shared_tablet()->cdc_history_floor());

  // Without an age bound, a master refresh keeps the floor of the stuck
  // session, which is not idle.
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  FLAGS_cdc_session_history_floor_max_age_secs = 0;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/0,
                                     /*history_safe_time_micros=*/0, /*barrier_seq=*/1));
  ASSERT_EQ(stuck_floor, tablet_replica_->shared_tablet()->cdc_history_floor());

  // With the bound, the next master refresh drops the stuck session's floor.
  FLAGS_cdc_session_history_floor_max_age_secs = kMaxAgeSecs;
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/0,
                                     /*history_safe_time_micros=*/0, /*barrier_seq=*/2));
  const Timestamp floor = tablet_replica_->shared_tablet()->cdc_history_floor();
  EXPECT_GT(floor, stuck_floor);
  EXPECT_LE(floor.value(), healthy_min_ts);

  // The stuck consumer polls its position again: its floor stays dropped.
  stuck_resp.Clear();
  ASSERT_OK(DoGetChangesForStream(kStuck, 0, &stuck_resp));
  ASSERT_FALSE(stuck_resp.has_error()) << stuck_resp.error().DebugString();
  EXPECT_EQ(floor, tablet_replica_->shared_tablet()->cdc_history_floor());
}

// When a consumer requests a from_op_index whose WAL segment has already been
// garbage-collected, GetChanges must return a well-defined WAL_EXPIRED error --
// not a silent empty batch (which the consumer would misread as "caught up",
// losing data) and not an opaque internal failure. This is the "consumer fell
// behind WAL GC" contract (YB: TestWALPrematureGCErrorCode,
// TestGetChangesFromGCedCheckpointWithNewerWal): the code tells the consumer to
// re-establish from a snapshot rather than resume from a lost position.
//
// Repro without a master (so the only WAL anchor is CDC's own per-session
// anchor): fill several WAL segments, advance the anchor past the oldest by
// polling from a checkpoint, GC the now-unanchored segments, then replay from
// op index 0 -- whose segment is gone.
TEST_F(CDCServiceTest, GetChanges_WalGcedBelowFromOpIndexReturnsWalExpired) {
  // Retain only the single open segment so a rolled-over, unanchored closed
  // segment is reclaimable (this is the default, set explicitly for the record).
  FLAGS_log_min_segments_to_retain = 1;

  log::Log* log = tablet_replica_->log();
  ASSERT_NE(nullptr, log);

  // Lay down three closed WAL segments plus an open one. Each insert batch is a
  // separate write op, so op index 1 lands in the first (oldest) segment.
  InsertTestRowsRemote(0, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  InsertTestRowsRemote(3, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  InsertTestRowsRemote(6, 3, /*num_batches=*/3);

  // Read from the start: registers the per-session anchor at 0 (pinning every
  // segment) and reports a checkpoint at the current committed tail.
  GetChangesResponsePB full;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &full));
  ASSERT_FALSE(full.has_error()) << full.error().DebugString();
  const int64_t checkpoint = full.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  // Write more so the checkpoint is strictly behind the committed tail; a poll
  // from the checkpoint is then NOT a caught-up read, so it advances the
  // per-session anchor up to the checkpoint (the caught-up fast path would
  // otherwise skip the anchor update and leave it pinned at 0).
  InsertTestRowsRemote(9, 3, /*num_batches=*/3);
  GetChangesResponsePB advanced;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &advanced));
  ASSERT_FALSE(advanced.has_error()) << advanced.error().DebugString();

  // With the anchor now at 'checkpoint', the segments holding the earlier ops
  // are unanchored. Roll so they are all closed, flush to drop MRS/DMS anchors,
  // then GC. The oldest segment (holding op index 1) is reclaimed.
  const int before = log->reader()->num_segments();
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  tablet_replica_->RunLogGC();
  const int after = log->reader()->num_segments();
  ASSERT_LT(after, before)
      << "log GC did not reclaim any segment (before=" << before
      << ", after=" << after << "); the WAL_EXPIRED path would not be exercised";
  ASSERT_GT(log->reader()->GetMinReplicateIndex(), 1)
      << "op index 1 is still present in the WAL; GC did not reach it";

  // Replaying from op index 0 now targets the reclaimed segment. The RPC itself
  // succeeds (the error is in-band) and the code is WAL_EXPIRED.
  GetChangesResponsePB expired;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &expired));
  ASSERT_TRUE(expired.has_error()) << expired.DebugString();
  EXPECT_EQ(CDCErrorPB::WAL_EXPIRED, expired.error().code())
      << expired.error().DebugString();
  // E1: WAL_EXPIRED is a re-snapshot signal, not a retry-in-place one. The
  // machine-readable classification must say so for external consumers.
  EXPECT_TRUE(expired.error().needs_resnapshot())
      << "WAL_EXPIRED must flag needs_resnapshot for external consumers";
  EXPECT_FALSE(expired.error().is_retryable())
      << "WAL_EXPIRED is not retryable in place";

  // A request that starts within the surviving WAL still succeeds -- GC pruned
  // only the prefix, it did not wedge the stream.
  GetChangesResponsePB survivor;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &survivor));
  ASSERT_FALSE(survivor.has_error()) << survivor.error().DebugString();
}

// When the requested WAL is gone AND the stream's session has been idle beyond
// --cdc_stream_idle_expiry_ms, GetChanges reports STREAM_EXPIRED rather than
// WAL_EXPIRED. This is the "permanently expired, re-bootstrap now" signal: the
// consumer must reset to a fresh snapshot instead of retrying a lost position.
// It is the same GC scenario as GetChanges_WalGcedBelowFromOpIndexReturnsWal-
// Expired, but with a session that has aged out of the expiry window; that test
// (recent activity, default 8h window) already proves the WAL_EXPIRED branch, so
// the two together pin both sides of the disambiguation.
TEST_F(CDCServiceTest, GetChanges_WalGcedAndSessionIdleReturnsStreamExpired) {
  FLAGS_log_min_segments_to_retain = 1;

  log::Log* log = tablet_replica_->log();
  ASSERT_NE(nullptr, log);

  // Same layout as the WAL_EXPIRED test: three closed segments plus an open one,
  // op index 1 in the oldest.
  InsertTestRowsRemote(0, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  InsertTestRowsRemote(3, 3, /*num_batches=*/3);
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  InsertTestRowsRemote(6, 3, /*num_batches=*/3);

  // Establish the per-session anchor and record activity, then advance the
  // anchor off the oldest segment so it can be GC'd.
  GetChangesResponsePB full;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &full));
  ASSERT_FALSE(full.has_error()) << full.error().DebugString();
  const int64_t checkpoint = full.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  InsertTestRowsRemote(9, 3, /*num_batches=*/3);
  GetChangesResponsePB advanced;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &advanced));
  ASSERT_FALSE(advanced.has_error()) << advanced.error().DebugString();

  // Make the session look idle: shrink the expiry window to 1ms and let it
  // elapse. The last successful poll above set the session's last-active time;
  // no successful poll happens after this point (the GC'd read errors out before
  // recording activity), so the session ages past the window.
  FLAGS_cdc_stream_idle_expiry_ms = 1;
  SleepFor(MonoDelta::FromMilliseconds(20));

  // GC the oldest segment (holding op index 1).
  const int before = log->reader()->num_segments();
  ASSERT_OK(log->WaitUntilAllFlushed());
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  tablet_replica_->RunLogGC();
  ASSERT_LT(log->reader()->num_segments(), before)
      << "log GC did not reclaim any segment; the expiry path would not be exercised";
  ASSERT_GT(log->reader()->GetMinReplicateIndex(), 1)
      << "op index 1 is still present in the WAL; GC did not reach it";

  // Replaying from op index 0 now targets the reclaimed segment. Because the
  // session is idle beyond the window, the code is STREAM_EXPIRED, not
  // WAL_EXPIRED.
  GetChangesResponsePB expired;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &expired));
  ASSERT_TRUE(expired.has_error()) << expired.DebugString();
  EXPECT_EQ(CDCErrorPB::STREAM_EXPIRED, expired.error().code())
      << expired.error().DebugString();
}

// E6: regression for the stream-config cache that never expired, so a
// record_type change was ignored for the life of the process. The fix stamps
// each entry with a freshness deadline (--cdc_stream_config_cache_ttl_ms) and
// refetches once it passes. This test drives real GetChanges RPCs and observes
// the two prod-visible behaviors the fix introduces: a *fresh* entry is served
// from cache without any refetch (its short deadline is left intact), while a
// *stale* entry triggers the refetch path (which, with no master in this
// fixture, serves the stale value and re-stamps a fresh deadline). The mini
// tablet server has no master, so the master-side record_type change cannot be
// exercised end to end here; the eviction/refresh decision that gates it is.
TEST_F(CDCServiceTest, StreamConfig_CacheEntryExpiresAndRefetches) {
  // A long TTL so that once the stale entry is refreshed via the refetch path
  // it stays fresh well past the end of the test (no reliance on timing there).
  FLAGS_cdc_stream_config_cache_ttl_ms = 60000;

  InsertTestRowsRemote(0, 1, 1);

  auto* cdc = mini_server_->server()->cdc_service();
  const string kStream = "test-stream-1";

  // Seed a CHANGE-mode entry with a short, finite deadline (like a real fetch).
  CDCStreamConfigPB config;
  config.set_record_type(CDCStreamConfigPB::CHANGE);
  config.set_snapshot_mode(CDCStreamConfigPB::NEVER);
  cdc->SetStreamConfigForTestsWithTtl(kStream, config, { TableId() },
                                      MonoDelta::FromMilliseconds(1000));
  ASSERT_TRUE(cdc->IsStreamConfigFreshForTests(kStream));

  // A GetChanges while the entry is still fresh must take the cache fast path
  // and NOT extend the deadline; the entry therefore goes stale on schedule.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
  // Had the call extended the deadline, the entry would stay fresh for the
  // TTL of a minute, past the wait here.
  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(cdc->IsStreamConfigFreshForTests(kStream))
        << "a fresh-hit GetChanges must not extend the cache deadline";
  });

  // A GetChanges once the entry is stale must run the refetch path. With no
  // master reachable it serves the stale value (streaming stays alive) and
  // re-stamps a fresh deadline, so the entry is fresh again afterward.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
  ASSERT_TRUE(cdc->IsStreamConfigFreshForTests(kStream))
      << "a stale-hit GetChanges must refetch and re-stamp the deadline";
}

// E9: regression for the commit/apply race that stamped a batch's pre-ALTER
// WRITEs with schema version N-1. The batch upper bound is the committed
// watermark, but tablet_metadata()->schema_version() only reflects *applied*
// ops. While an ALTER (N -> N+1) is committed but not yet applied, the applied
// version reads N; deriving the base as (applied_version - alters_in_batch)
// would yield N-1 and mis-stamp every WRITE preceding the ALTER in the batch.
// Whether such an ALTER takes effect is only known once it is applied, so the
// batch ends before it: the pre-ALTER WRITE is stamped N, and the DDL follows
// once the ALTER is applied.
TEST_F(CDCServiceTest, SchemaVersion_CommittedUnappliedAlterStampsPreAlterVersion) {
  // Default CHANGE-mode stream is fine; schema_version stamping is mode-agnostic.
  // First, apply one alter (v0 -> v1) so the true base for the racy batch is 1
  // (>0). This matters because the buggy path clamps a negative running version
  // to 0, which would accidentally match a true base of 0 and hide the bug.
  const Schema v1_schema = AlterAddColumn(schema_, "c1", /*new_version=*/1);
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->schema_version());

  // Consume through the applied alter so the racy read window starts after it.
  int64_t checkpoint = 0;
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    checkpoint = resp.checkpoint_op_index();
    ASSERT_GT(checkpoint, 0);
  }

  // A WRITE at schema version 1, which must be stamped 1 in the racy batch.
  InsertTestRowsRemote(0, 1, 1);
  const int64_t alter_index = checkpoint + 2;

  // Now issue a second alter (v1 -> v2) whose apply is delayed, so it is
  // committed-but-not-applied while we read. The RPC returns only after apply,
  // so run it on a background thread and read during the apply window.
  FLAGS_tablet_inject_latency_on_apply_alter_schema_op_ms = 5000;
  Status alter_status;
  std::thread alterer([&]() {
    AlterAddColumn(v1_schema, "c2", /*new_version=*/2, &alter_status);
  });
  auto join = MakeScopedCleanup([&]() { alterer.join(); });
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(tablet_replica_->consensus()->GetLastOpId(consensus::COMMITTED_OPID)->index(),
              alter_index);
  });

  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    // Still inside the committed-but-unapplied window.
    ASSERT_EQ(1, tablet_replica_->tablet_metadata()->schema_version());
    ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
    EXPECT_EQ(CDCOpTypePB::INSERT, resp.records(0).op_type());
    EXPECT_EQ(1, resp.records(0).schema_version())
        << "pre-ALTER WRITE mis-stamped during commit/apply race: " << resp.DebugString();
    EXPECT_EQ(alter_index - 1, resp.checkpoint_op_index());
    EXPECT_FALSE(resp.have_more_records());
    checkpoint = resp.checkpoint_op_index();
  }
  join.run();
  ASSERT_OK(alter_status);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
  EXPECT_EQ(CDCOpTypePB::DDL, resp.records(0).op_type());
  EXPECT_EQ(1, resp.records(0).schema_version());
  EXPECT_EQ(2, resp.records(0).new_schema_version());
  EXPECT_EQ(alter_index, resp.checkpoint_op_index());
}

// E1: the wire-level retry classification (is_retryable / needs_resnapshot) must
// be populated on error responses so external consumers need not re-implement
// the per-code taxonomy. This covers the retryable-in-place branch; the
// needs_resnapshot branch is covered additively in the WAL_EXPIRED test above.
TEST_F(CDCServiceTest, ErrorContract_RetryableClassification) {
  InsertTestRowsRemote(0, 3, 3);

  // TABLET_NOT_FOUND (unknown tablet): retry after resolving the leader; not a
  // re-snapshot.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges("no-such-tablet", 0, &resp));
    ASSERT_TRUE(resp.has_error());
    EXPECT_EQ(CDCErrorPB::TABLET_NOT_FOUND, resp.error().code());
    EXPECT_TRUE(resp.error().is_retryable()) << resp.error().DebugString();
    EXPECT_FALSE(resp.error().needs_resnapshot()) << resp.error().DebugString();
  }

  // TABLET_NOT_RUNNING (replica bootstrapping): retryable, not a re-snapshot.
  {
    FLAGS_cdc_inject_tablet_not_running = true;
    SCOPED_CLEANUP({ FLAGS_cdc_inject_tablet_not_running = false; });
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_TRUE(resp.has_error());
    EXPECT_EQ(CDCErrorPB::TABLET_NOT_RUNNING, resp.error().code());
    EXPECT_TRUE(resp.error().is_retryable()) << resp.error().DebugString();
    EXPECT_FALSE(resp.error().needs_resnapshot()) << resp.error().DebugString();
  }

  // TABLET_NOT_LEADER (leadership changed mid-read): retryable, not a re-snapshot.
  // Uses the post-read injection hook because forcing a real leader change
  // precisely mid-read is not deterministic in a single-replica fixture.
  {
    FLAGS_cdc_inject_post_read_leadership_loss = true;
    SCOPED_CLEANUP({ FLAGS_cdc_inject_post_read_leadership_loss = false; });
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_TRUE(resp.has_error());
    EXPECT_EQ(CDCErrorPB::TABLET_NOT_LEADER, resp.error().code());
    EXPECT_TRUE(resp.error().is_retryable()) << resp.error().DebugString();
    EXPECT_FALSE(resp.error().needs_resnapshot()) << resp.error().DebugString();
  }
}

// Writes one batch against pre-inserted keys 0..3: a valid UPDATE of key 0, an
// UPDATE of key 1 with no non-key column, an UPDATE of key 2 with an oversized
// cell, and a valid UPDATE of key 3. The tablet rejects the middle two rows and
// applies the others.
namespace {
void WriteBatchWithRejectedUpdates(tserver::TabletServerServiceProxy* proxy,
                                   const Schema& schema) {
  FLAGS_max_cell_size_bytes = 16;
  tserver::WriteRequestPB req;
  req.set_tablet_id(tserver::TabletServerTestBase::kTabletId);
  ASSERT_OK(SchemaToPB(schema, req.mutable_schema()));
  auto* ops = req.mutable_row_operations();
  AddTestRowToPB(RowOperationsPB::UPDATE, schema, 0, 100, "u0", ops);
  AddTestKeyToPB(RowOperationsPB::UPDATE, schema, 1, ops);
  AddTestRowToPB(RowOperationsPB::UPDATE, schema, 2, 200, string(64, 'x'), ops);
  AddTestRowToPB(RowOperationsPB::UPDATE, schema, 3, 300, "u3", ops);
  tserver::WriteResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(proxy->Write(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(2, resp.per_row_errors_size()) << resp.DebugString();
}
} // anonymous namespace

// Rows the tablet rejected while decoding the write must be skipped one by one:
// the applied sibling rows of the same op are still emitted.
TEST_F(CDCServiceTest, ChangeMode_RejectedUpdateRowDoesNotDropBatch) {
  InsertTestRowsRemote(0, 4, 1);
  GetChangesResponsePB base;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &base));
  ASSERT_FALSE(base.has_error()) << base.error().DebugString();
  NO_FATALS(WriteBatchWithRejectedUpdates(proxy_.get(), schema_));

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, base.checkpoint_op_index(), &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  vector<int32_t> keys;
  for (const auto& r : resp.records()) {
    ASSERT_EQ(CDCOpTypePB::UPDATE, r.op_type()) << r.DebugString();
    int32_t key = -1;
    ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &key));
    keys.push_back(key);
  }
  EXPECT_EQ(vector<int32_t>({ 0, 3 }), keys) << resp.DebugString();
}

// Same as above for a FULL stream: the before/after images must be matched to
// the right rows, i.e. the rejected rows must not shift the alignment between
// the decoded records and the re-decoded row keys.
TEST_F(CDCServiceTest, FullMode_RejectedUpdateRowDoesNotDropBatch) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 4, 1);
  GetChangesResponsePB base;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &base));
  ASSERT_FALSE(base.has_error()) << base.error().DebugString();
  NO_FATALS(WriteBatchWithRejectedUpdates(proxy_.get(), schema_));

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, base.checkpoint_op_index(), &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(2, resp.records_size()) << resp.DebugString();
  const int32_t expected_keys[] = { 0, 3 };
  const int32_t expected_after[] = { 100, 300 };
  for (int i = 0; i < 2; ++i) {
    const CDCRecordPB& r = resp.records(i);
    SCOPED_TRACE(r.DebugString());
    EXPECT_EQ(CDCOpTypePB::UPDATE, r.op_type());
    int32_t v = -1;
    ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &v));
    EXPECT_EQ(expected_keys[i], v);
    ASSERT_TRUE(GetInt32Col(r, /*before=*/true, "key", &v));
    EXPECT_EQ(expected_keys[i], v);
    ASSERT_TRUE(GetInt32Col(r, /*before=*/true, "int_val", &v));
    EXPECT_EQ(expected_keys[i], v);
    ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "int_val", &v));
    EXPECT_EQ(expected_after[i], v);
  }
}

// The Java client sends a column's default value only as 'read_default_value'
// in the write request's schema. Rows that leave such a NOT NULL column unset
// are applied by the tablet with the default, so CDC must emit them. An INSERT
// record carries the complete row, default value included; on a FULL stream an
// UPSERT is resolved against the tablet (here: to an INSERT of a new row) and
// carries the complete row too, while on a CHANGE stream it carries the
// columns the operation set.
TEST_F(CDCServiceTest, ReadDefaultOnlyClientSchemaRowsAreEmitted) {
  const int32_t kDefault = 7;
  SchemaBuilder builder(schema_);
  ASSERT_OK(builder.AddColumn(ColumnSchemaBuilder()
                                  .name("d")
                                  .type(INT32)
                                  .read_default(&kDefault)
                                  .write_default(&kDefault)));
  const Schema new_schema = builder.Build();
  {
    tserver::AlterSchemaRequestPB req;
    tserver::AlterSchemaResponsePB resp;
    req.set_dest_uuid(mini_server_->server()->fs_manager()->uuid());
    req.set_tablet_id(kTabletId);
    req.set_schema_version(1);
    ASSERT_OK(SchemaToPB(new_schema, req.mutable_schema()));
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(30));
    ASSERT_OK(admin_proxy_->AlterSchema(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
  GetChangesResponsePB base;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &base));
  ASSERT_FALSE(base.has_error()) << base.error().DebugString();
  {
    const Schema client_schema = new_schema.CopyWithoutColumnIds();
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(client_schema, req.mutable_schema(),
                         SCHEMA_PB_WITHOUT_IDS | SCHEMA_PB_WITHOUT_WRITE_DEFAULT));
    RowOperationsPBEncoder enc(req.mutable_row_operations());
    for (const auto& [type, key] : vector<std::pair<RowOperationsPB::Type, int32_t>>{
             { RowOperationsPB::INSERT, 10 }, { RowOperationsPB::UPSERT, 11 } }) {
      KuduPartialRow row(&client_schema);
      ASSERT_OK(row.SetInt32("key", key));
      ASSERT_OK(row.SetInt32("int_val", key));
      enc.Add(type, row);
    }
    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }

  for (const auto mode : { CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL }) {
    SCOPED_TRACE(CDCStreamConfigPB::RecordType_Name(mode));
    SeedStreamConfig(mode);
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, base.checkpoint_op_index(), &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(2, resp.records_size()) << resp.DebugString();
    const bool full = mode == CDCStreamConfigPB::FULL;
    const CDCRecordPB& ins = resp.records(0);
    const CDCRecordPB& ups = resp.records(1);
    int32_t v = -1;
    EXPECT_EQ(CDCOpTypePB::INSERT, ins.op_type());
    ASSERT_TRUE(GetInt32Col(ins, /*before=*/false, "key", &v)) << ins.DebugString();
    EXPECT_EQ(10, v);
    ASSERT_TRUE(GetInt32Col(ins, /*before=*/false, "d", &v)) << ins.DebugString();
    EXPECT_EQ(kDefault, v);

    EXPECT_EQ(full ? CDCOpTypePB::INSERT : CDCOpTypePB::UPSERT, ups.op_type());
    ASSERT_TRUE(GetInt32Col(ups, /*before=*/false, "key", &v)) << ups.DebugString();
    EXPECT_EQ(11, v);
    if (full) {
      ASSERT_TRUE(GetInt32Col(ups, /*before=*/false, "d", &v)) << ups.DebugString();
      EXPECT_EQ(kDefault, v);
    } else {
      EXPECT_EQ(nullptr, FindCol(ups, /*before=*/false, "d")) << ups.DebugString();
    }
  }
}

// Fixture for the tests of rows and ALTERs whose outcome is recorded only in
// their op's COMMIT entry.
class CDCServiceOutcomeTest : public CDCServiceTest {
 protected:
  // Writes one batch against the pre-inserted keys 0..2 (int_val = key): a
  // duplicate INSERT, an ignored INSERT_IGNORE, an UPDATE and a DELETE of
  // missing keys, an ignored UPDATE_IGNORE and DELETE_IGNORE, and three rows
  // the tablet applies: INSERT of key 10, UPDATE of key 2, DELETE of key 1.
  void WriteBatchWithFailedAndIgnoredRows() {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
    auto* ops = req.mutable_row_operations();
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 0, 999, "dup", ops);
    AddTestRowToPB(RowOperationsPB::INSERT_IGNORE, schema_, 1, 999, "dup", ops);
    AddTestRowToPB(RowOperationsPB::INSERT, schema_, 10, 10, "new", ops);
    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 20, 999, "missing", ops);
    AddTestRowToPB(RowOperationsPB::UPDATE_IGNORE, schema_, 21, 999, "missing", ops);
    AddTestKeyToPB(RowOperationsPB::DELETE, schema_, 22, ops);
    AddTestKeyToPB(RowOperationsPB::DELETE_IGNORE, schema_, 23, ops);
    AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 2, 222, "upd", ops);
    AddTestKeyToPB(RowOperationsPB::DELETE, schema_, 1, ops);
    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(3, resp.per_row_errors_size()) << resp.DebugString();
  }

  // Checks that 'resp' holds exactly the records of the rows applied by
  // WriteBatchWithFailedAndIgnoredRows(), with FULL images if 'full'.
  static void CheckOnlyAppliedRowsEmitted(const GetChangesResponsePB& resp, bool full) {
    ASSERT_EQ(3, resp.records_size()) << resp.DebugString();
    const CDCOpTypePB expected_types[] = {
        CDCOpTypePB::INSERT, CDCOpTypePB::UPDATE, CDCOpTypePB::DELETE };
    const int32_t expected_keys[] = { 10, 2, 1 };
    for (int i = 0; i < 3; ++i) {
      const CDCRecordPB& r = resp.records(i);
      SCOPED_TRACE(r.DebugString());
      EXPECT_EQ(expected_types[i], r.op_type());
      int32_t v = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &v));
      EXPECT_EQ(expected_keys[i], v);
    }
    int32_t v = -1;
    ASSERT_TRUE(GetInt32Col(resp.records(1), /*before=*/false, "int_val", &v));
    EXPECT_EQ(222, v);
    if (full) {
      EXPECT_EQ(0, resp.records(0).old_changes_size());
      ASSERT_TRUE(GetInt32Col(resp.records(1), /*before=*/true, "int_val", &v));
      EXPECT_EQ(2, v);
      ASSERT_TRUE(GetInt32Col(resp.records(2), /*before=*/true, "int_val", &v));
      EXPECT_EQ(1, v);
    }
  }

  // Inserts keys 0..2, then writes the batch of WriteBatchWithFailedAndIgnoredRows()
  // and sets 'checkpoint' to the op index right before it.
  void InsertRowsAndWriteMixedBatch(int64_t* checkpoint) {
    InsertTestRowsRemote(0, 3, 1);
    GetChangesResponsePB base;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &base));
    ASSERT_FALSE(base.has_error()) << base.error().DebugString();
    *checkpoint = base.checkpoint_op_index();
    NO_FATALS(WriteBatchWithFailedAndIgnoredRows());
  }

  // Polls GetChanges after 'checkpoint' on a stream of record type 'mode' and
  // checks that only the applied rows of the mixed batch are emitted.
  void CheckMixedBatch(CDCStreamConfigPB::RecordType mode, int64_t checkpoint) {
    SCOPED_TRACE(CDCStreamConfigPB::RecordType_Name(mode));
    SeedStreamConfig(mode);
    GetChangesResponsePB resp;
    ASSERT_EVENTUALLY([&] {
      resp.Clear();
      ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
      ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    });
    NO_FATALS(CheckOnlyAppliedRowsEmitted(resp, mode == CDCStreamConfigPB::FULL));
  }
};

// A row the tablet rejects at apply time (a duplicate INSERT, an UPDATE or
// DELETE of a missing key) or that changes nothing (an ignored *_IGNORE row) is
// still in the REPLICATE, which holds the request. Only the rows the tablet
// applied may be emitted: a sink applying the documented contract would
// otherwise diverge from the source.
TEST_F(CDCServiceOutcomeTest, ChangeMode_FailedRowsNotEmitted) {
  int64_t checkpoint;
  NO_FATALS(InsertRowsAndWriteMixedBatch(&checkpoint));
  NO_FATALS(CheckMixedBatch(CDCStreamConfigPB::CHANGE, checkpoint));
}

TEST_F(CDCServiceOutcomeTest, FullMode_FailedRowsNotEmitted) {
  int64_t checkpoint;
  NO_FATALS(InsertRowsAndWriteMixedBatch(&checkpoint));
  NO_FATALS(CheckMixedBatch(CDCStreamConfigPB::FULL, checkpoint));
}

// Same, after the rows are flushed and the tablet server restarts: tablet
// bootstrap rewrites the COMMIT entries, which must keep the row outcomes.
TEST_F(CDCServiceOutcomeTest, FailedRowsNotEmittedAfterFlushAndRestart) {
  int64_t checkpoint;
  NO_FATALS(InsertRowsAndWriteMixedBatch(&checkpoint));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(RestartTabletServer());
  NO_FATALS(CheckMixedBatch(CDCStreamConfigPB::CHANGE, checkpoint));
  NO_FATALS(CheckMixedBatch(CDCStreamConfigPB::FULL, checkpoint));
}

// An ALTER that does not advance the schema version (e.g. a stale or duplicate
// request) is replicated, but the tablet refuses it when applying it. It must
// not be emitted as DDL, and the rows after it keep the version in effect.
TEST_F(CDCServiceOutcomeTest, AlterFailedOnApply_NotEmittedAsDdl) {
  AlterAddColumn(schema_, "c1", /*new_version=*/1);
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->schema_version());

  // The AlterSchema RPC refuses a stale version up front, so submit the op
  // directly, the way a request that raced with the first one reaches the log.
  tserver::AlterSchemaRequestPB req;
  req.set_dest_uuid(mini_server_->server()->fs_manager()->uuid());
  req.set_tablet_id(kTabletId);
  req.set_schema_version(0);
  ASSERT_OK(SchemaToPB(SchemaBuilder(schema_).Build(), req.mutable_schema()));
  tserver::AlterSchemaResponsePB resp;
  CountDownLatch latch(1);
  auto op_state = std::make_unique<tablet::AlterSchemaOpState>(
      tablet_replica_.get(), &req, &resp);
  op_state->set_completion_callback(
      std::make_unique<tablet::LatchOpCompletionCallback<tserver::AlterSchemaResponsePB>>(
          &latch, &resp));
  ASSERT_OK(tablet_replica_->SubmitAlterSchema(std::move(op_state)));
  latch.Wait();
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->schema_version());

  InsertTestRowsRemote(0, 1, 1);

  GetChangesResponsePB changes;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &changes));
  ASSERT_FALSE(changes.has_error()) << changes.error().DebugString();
  int ddls = 0;
  int inserts = 0;
  for (const auto& r : changes.records()) {
    SCOPED_TRACE(r.DebugString());
    if (r.op_type() == CDCOpTypePB::DDL) {
      ++ddls;
      EXPECT_EQ(1, r.new_schema_version());
    } else if (r.op_type() == CDCOpTypePB::INSERT) {
      ++inserts;
      EXPECT_EQ(1, r.schema_version());
    }
  }
  EXPECT_EQ(1, ddls) << changes.DebugString();
  EXPECT_EQ(1, inserts) << changes.DebugString();
  EXPECT_EQ(tablet_replica_->consensus()->GetLastOpId(consensus::COMMITTED_OPID)->index(),
            changes.checkpoint_op_index());
}

// On an existing row the tablet applies an UPSERT_IGNORE as an update that
// skips immutable columns, and on a new row as an INSERT. A CHANGE record does
// not tell which, so it is flagged for the sink not to overwrite immutable
// columns. A FULL stream resolves the UPSERT and needs no flag.
TEST_F(CDCServiceOutcomeTest, ChangeMode_UpsertIgnoreImmutableColumnFlagged) {
  SchemaBuilder builder(schema_);
  ASSERT_OK(builder.AddColumn(ColumnSchemaBuilder()
                                  .name("imm")
                                  .type(INT32)
                                  .nullable(true)
                                  .immutable(true)));
  const Schema new_schema = builder.Build();
  {
    tserver::AlterSchemaRequestPB req;
    tserver::AlterSchemaResponsePB resp;
    req.set_dest_uuid(mini_server_->server()->fs_manager()->uuid());
    req.set_tablet_id(kTabletId);
    req.set_schema_version(1);
    ASSERT_OK(SchemaToPB(new_schema, req.mutable_schema()));
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(30));
    ASSERT_OK(admin_proxy_->AlterSchema(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
  const Schema client_schema = new_schema.CopyWithoutColumnIds();
  // Writes rows of (type, key, imm) with int_val = key * 10.
  using Row = std::tuple<RowOperationsPB::Type, int32_t, int32_t>;
  const auto write = [&](const vector<Row>& rows) {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    ASSERT_OK(SchemaToPB(client_schema, req.mutable_schema(), SCHEMA_PB_WITHOUT_IDS));
    RowOperationsPBEncoder enc(req.mutable_row_operations());
    for (const auto& [type, key, imm] : rows) {
      KuduPartialRow row(&client_schema);
      ASSERT_OK(row.SetInt32("key", key));
      ASSERT_OK(row.SetInt32("int_val", key * 10));
      ASSERT_OK(row.SetInt32("imm", imm));
      enc.Add(type, row);
    }
    tserver::WriteResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->Write(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  };
  NO_FATALS(write({ { RowOperationsPB::INSERT, 5, 1 } }));
  GetChangesResponsePB base;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &base));
  ASSERT_FALSE(base.has_error()) << base.error().DebugString();
  // Key 5 exists, so its 'imm' stays 1; key 6 is new and gets 'imm' 3.
  NO_FATALS(write({ { RowOperationsPB::UPSERT_IGNORE, 5, 2 },
                    { RowOperationsPB::UPSERT_IGNORE, 6, 3 } }));

  for (const auto mode : { CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL }) {
    SCOPED_TRACE(CDCStreamConfigPB::RecordType_Name(mode));
    const bool full = mode == CDCStreamConfigPB::FULL;
    SeedStreamConfig(mode);
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, base.checkpoint_op_index(), &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(2, resp.records_size()) << resp.DebugString();
    const CDCRecordPB& existing = resp.records(0);
    const CDCRecordPB& created = resp.records(1);
    EXPECT_EQ(full ? CDCOpTypePB::UPDATE : CDCOpTypePB::UPSERT, existing.op_type());
    EXPECT_EQ(full ? CDCOpTypePB::INSERT : CDCOpTypePB::UPSERT, created.op_type());
    EXPECT_EQ(!full, existing.keep_immutable_columns_on_update()) << existing.DebugString();
    EXPECT_EQ(!full, created.keep_immutable_columns_on_update()) << created.DebugString();
    int32_t v = -1;
    ASSERT_TRUE(GetInt32Col(existing, /*before=*/false, "imm", &v));
    // FULL carries the row as stored; CHANGE carries the request.
    EXPECT_EQ(full ? 1 : 2, v);
    ASSERT_TRUE(GetInt32Col(existing, /*before=*/false, "int_val", &v));
    EXPECT_EQ(50, v);
    ASSERT_TRUE(GetInt32Col(created, /*before=*/false, "imm", &v));
    EXPECT_EQ(3, v);
  }
}

// The outcome of an op is only known once the op is applied. A committed op
// that is not applied yet ends the batch before it; if it is the first op
// after the checkpoint, the poll fails with a retryable error.
TEST_F(CDCServiceOutcomeTest, UnappliedOpEndsBatch) {
  InsertTestRowsRemote(0, 1, 1);
  GetChangesResponsePB base;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &base));
  ASSERT_FALSE(base.has_error()) << base.error().DebugString();
  const int64_t checkpoint = base.checkpoint_op_index();

  // Hold an UPDATE in the apply phase: it is committed but not applied.
  FLAGS_tablet_inject_latency_on_apply_write_op_ms = 4000;
  std::thread updater([&]() { UpdateTestRowRemote(0, 12345); });
  auto join = MakeScopedCleanup([&]() { updater.join(); });
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(tablet_replica_->consensus()->GetLastOpId(consensus::COMMITTED_OPID)->index(),
              checkpoint);
  });
  {
    const int64_t pending_before =
        CDCCounterValue(METRIC_cdc_wal_reads_rejected_outcome_pending);
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
    ASSERT_TRUE(resp.has_error()) << resp.DebugString();
    EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code()) << resp.DebugString();
    EXPECT_TRUE(resp.error().is_retryable());
    EXPECT_EQ(0, resp.records_size());
    EXPECT_EQ(1, CDCCounterValue(METRIC_cdc_wal_reads_rejected_outcome_pending) -
                 pending_before);
  }
  {
    // A poll from before the INSERT gets the INSERT and stops before the UPDATE.
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, checkpoint - 1, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
    EXPECT_EQ(CDCOpTypePB::INSERT, resp.records(0).op_type());
    EXPECT_EQ(checkpoint, resp.checkpoint_op_index());
    EXPECT_FALSE(resp.have_more_records());
    // The UPDATE is committed but not streamed yet: the stream is behind.
    CheckpointResponsePB cp_resp;
    ASSERT_OK(DoCheckpoint(kTabletId, checkpoint, &cp_resp));
    ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
    EXPECT_EQ(1, mini_server_->server()->cdc_service()->MaxOpsBehind());
  }
  join.run();
  FLAGS_tablet_inject_latency_on_apply_write_op_ms = 0;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
  EXPECT_EQ(CDCOpTypePB::UPDATE, resp.records(0).op_type());
}

// An op that cannot be decoded ends the batch right before it, and a poll that
// cannot make progress past it fails with a non-retryable DECODE_FAILED. The
// checkpoint never moves past the op, so nothing is lost once it decodes.
TEST_F(CDCServiceTest, DecodeFailure_ReturnsErrorAndDoesNotAdvance) {
  InsertTestRowsRemote(0, 3, 3);
  vector<int64_t> write_indexes;
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& r : resp.records()) {
      if (r.op_type() == CDCOpTypePB::INSERT) write_indexes.push_back(r.op_index());
    }
  }
  ASSERT_EQ(3, write_indexes.size());
  const int64_t bad_index = write_indexes[1];
  FLAGS_cdc_inject_decode_failure_at_op_index = bad_index;
  const int64_t errors_before = CDCCounterValue(METRIC_cdc_errors_decode_failed);
  ASSERT_GE(errors_before, 0);

  GetChangesResponsePB first;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &first));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  EXPECT_EQ(bad_index - 1, first.checkpoint_op_index());
  EXPECT_TRUE(first.have_more_records());
  int inserts = 0;
  for (const auto& r : first.records()) {
    EXPECT_LT(r.op_index(), bad_index) << r.DebugString();
    if (r.op_type() == CDCOpTypePB::INSERT) ++inserts;
  }
  EXPECT_EQ(1, inserts) << first.DebugString();

  GetChangesResponsePB second;
  ASSERT_OK(DoGetChanges(kTabletId, first.checkpoint_op_index(), &second));
  ASSERT_TRUE(second.has_error()) << second.DebugString();
  EXPECT_EQ(CDCErrorPB::DECODE_FAILED, second.error().code());
  EXPECT_FALSE(second.error().is_retryable());
  EXPECT_FALSE(second.error().needs_resnapshot());
  ASSERT_STR_CONTAINS(second.error().status().message(),
                      strings::Substitute("op index $0", bad_index));
  EXPECT_EQ(0, second.records_size());
  // Error responses carry the -1 checkpoint sentinel, never a real index.
  EXPECT_EQ(-1, second.checkpoint_op_index());
  EXPECT_EQ(errors_before + 1, CDCCounterValue(METRIC_cdc_errors_decode_failed));

  // Once the op decodes, the stream resumes exactly at it.
  FLAGS_cdc_inject_decode_failure_at_op_index = -1;
  GetChangesResponsePB third;
  ASSERT_OK(DoGetChanges(kTabletId, first.checkpoint_op_index(), &third));
  ASSERT_FALSE(third.has_error()) << third.error().DebugString();
  vector<int64_t> resumed;
  for (const auto& r : third.records()) {
    if (r.op_type() == CDCOpTypePB::INSERT) resumed.push_back(r.op_index());
  }
  EXPECT_EQ(vector<int64_t>({ write_indexes[1], write_indexes[2] }), resumed);
}

// An ALTER that cannot be decoded is handled like a WRITE: its DDL record is
// not dropped, and the rows after it are not stamped with the old version.
TEST_F(CDCServiceTest, DecodeFailure_AlterNotDroppedOrMisstamped) {
  InsertTestRowsRemote(0, 1, 1);
  AlterAddColumn(schema_, "c1", /*new_version=*/1);
  InsertTestRowsRemote(1, 1, 1);
  int64_t ddl_index = -1;
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& r : resp.records()) {
      if (r.op_type() == CDCOpTypePB::DDL) ddl_index = r.op_index();
    }
  }
  ASSERT_GT(ddl_index, 0);
  FLAGS_cdc_inject_decode_failure_at_op_index = ddl_index;

  GetChangesResponsePB first;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &first));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  EXPECT_EQ(ddl_index - 1, first.checkpoint_op_index());
  for (const auto& r : first.records()) {
    EXPECT_LT(r.op_index(), ddl_index) << r.DebugString();
  }
  GetChangesResponsePB second;
  ASSERT_OK(DoGetChanges(kTabletId, first.checkpoint_op_index(), &second));
  ASSERT_TRUE(second.has_error()) << second.DebugString();
  EXPECT_EQ(CDCErrorPB::DECODE_FAILED, second.error().code());

  FLAGS_cdc_inject_decode_failure_at_op_index = -1;
  GetChangesResponsePB third;
  ASSERT_OK(DoGetChanges(kTabletId, first.checkpoint_op_index(), &third));
  ASSERT_FALSE(third.has_error()) << third.error().DebugString();
  ASSERT_EQ(2, third.records_size()) << third.DebugString();
  EXPECT_EQ(CDCOpTypePB::DDL, third.records(0).op_type());
  EXPECT_EQ(1, third.records(0).new_schema_version());
  EXPECT_EQ(CDCOpTypePB::INSERT, third.records(1).op_type());
  EXPECT_EQ(1, third.records(1).schema_version());
}

// E2: a consumer that declares (via req.schema_version) a schema version older
// than the tablet's current applied schema version gets SCHEMA_VERSION_MISMATCH
// -- a retryable-after-refresh signal -- instead of records it would decode
// against a stale layout. need_schema_info=true (refreshing) and an up-to-date
// declared version both suppress the check.
TEST_F(CDCServiceTest, SchemaVersionMismatch_StaleConsumerVersionRejected) {
  // Base applied schema version is 0; bump it to 1 with an applied ALTER.
  AlterAddColumn(schema_, "c1", /*new_version=*/1);
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->schema_version());
  InsertTestRowsRemote(0, 3, 3);

  // A consumer that read the ALTER decodes the records after it with version 1.
  int64_t alter_index = -1;
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& r : resp.records()) {
      if (r.op_type() == CDCOpTypePB::DDL) {
        alter_index = r.op_index();
      }
    }
    ASSERT_GT(alter_index, 0) << resp.DebugString();
  }

  // Declared version 0 < 1, the version of the ops after the ALTER, not
  // refreshing: rejected with a retryable SCHEMA_VERSION_MISMATCH and no
  // records leaked.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChangesWithSchemaVersion(alter_index, 0, /*need_schema_info=*/false, &resp));
    ASSERT_TRUE(resp.has_error()) << resp.DebugString();
    EXPECT_EQ(CDCErrorPB::SCHEMA_VERSION_MISMATCH, resp.error().code())
        << resp.error().DebugString();
    EXPECT_TRUE(resp.error().is_retryable()) << resp.error().DebugString();
    EXPECT_FALSE(resp.error().needs_resnapshot()) << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
  }

  // Same stale version, but need_schema_info=true means the consumer is
  // refreshing: the schema of version 1 is prepended and the check is skipped.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChangesWithSchemaVersion(alter_index, 0, /*need_schema_info=*/true, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_GT(resp.records_size(), 0);
    EXPECT_EQ(CDCOpTypePB::DDL, resp.records(0).op_type());
    EXPECT_EQ(1, resp.records(0).new_schema_version());
  }

  // Declared version equal to the version of the ops: no mismatch.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChangesWithSchemaVersion(alter_index, 1, /*need_schema_info=*/false, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // No declared version at all (default): backward-compatible, check disabled.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, alter_index, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // Declared version AHEAD of the tablet's current version: must not be
  // rejected. A consumer that pre-loaded a schema from DDL records may hold a
  // version the tablet has committed but not yet applied; that is valid and must
  // not trigger SCHEMA_VERSION_MISMATCH (the check is strictly <, not !=).
  {
    GetChangesResponsePB resp;
    // Tablet is at version 1; consumer claims version 2 (ahead).
    ASSERT_OK(DoGetChangesWithSchemaVersion(alter_index, 2, /*need_schema_info=*/false, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
}

// A consumer whose position precedes an ALTER that the tablet has applied
// holds the right schema for the records up to the ALTER: the older one. It is
// not rejected, and a refresh returns that schema, not the current one.
TEST_F(CDCServiceTest, SchemaVersionMismatch_ConsumerAtCorrectOlderVersionNotRejected) {
  InsertTestRowsRemote(0, 2, 2);
  const int64_t checkpoint = CaughtUpCheckpoint();
  InsertTestRowsRemote(2, 1, 1);
  AlterAddColumn(schema_, "c1", /*new_version=*/1);
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->schema_version());

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChangesWithSchemaVersion(checkpoint, 0, /*need_schema_info=*/false, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(2, resp.records_size()) << resp.DebugString();
  EXPECT_EQ(CDCOpTypePB::INSERT, resp.records(0).op_type());
  EXPECT_EQ(0, resp.records(0).schema_version());
  EXPECT_EQ(CDCOpTypePB::DDL, resp.records(1).op_type());
  EXPECT_EQ(0, resp.records(1).schema_version());
  EXPECT_EQ(1, resp.records(1).new_schema_version());
}

// need_schema_info returns the schema that the ops after the requested
// position were applied with, even when an ALTER after them is applied.
TEST_F(CDCServiceTest, NeedSchemaInfo_UnreadAlterAhead_DoesNotReturnFutureSchema) {
  const int64_t checkpoint = CaughtUpCheckpoint();
  InsertTestRowsRemote(0, 1, 1);
  AlterAddColumn(schema_, "c1", /*new_version=*/1);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChangesWithSchemaVersion(checkpoint, -1, /*need_schema_info=*/true, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_GE(resp.records_size(), 2) << resp.DebugString();
  const CDCRecordPB& ddl = resp.records(0);
  EXPECT_EQ(CDCOpTypePB::DDL, ddl.op_type());
  EXPECT_EQ(checkpoint, ddl.op_index());
  EXPECT_EQ(0, ddl.new_schema_version());
  Schema schema;
  ASSERT_OK(SchemaFromPB(ddl.new_schema(), &schema));
  EXPECT_EQ(Schema::kColumnNotFound, schema.find_column("c1")) << schema.ToString();
  EXPECT_EQ(schema_.num_columns(), schema.num_columns());
  EXPECT_EQ(CDCOpTypePB::INSERT, resp.records(1).op_type());
  EXPECT_EQ(0, resp.records(1).schema_version());
}

// A consumer that starts streaming from now passes INT64_MAX as from_op_index.
// need_schema_info returns the schema of the ops after the position that the
// response starts it at. Before, the server looked up the schema of op index
// INT64_MAX + 1 and failed with SCHEMA_UNAVAILABLE (needs_resnapshot).
TEST_F(CDCServiceTest, NeedSchemaInfo_StartFromNowGetsSchemaOfStart) {
  InsertTestRowsRemote(0, 1, 1);
  AlterAddColumn(schema_, "c1", /*new_version=*/1);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChangesWithSchemaVersion(std::numeric_limits<int64_t>::max(), -1,
                                          /*need_schema_info=*/true, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_TRUE(resp.has_checkpoint_op_index());
  EXPECT_GT(resp.checkpoint_op_index(), 0);
  ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
  const CDCRecordPB& ddl = resp.records(0);
  EXPECT_EQ(CDCOpTypePB::DDL, ddl.op_type());
  EXPECT_EQ(1, ddl.schema_version());
  EXPECT_EQ(1, ddl.new_schema_version());
  Schema schema;
  ASSERT_OK(SchemaFromPB(ddl.new_schema(), &schema));
  EXPECT_NE(Schema::kColumnNotFound, schema.find_column("c1")) << schema.ToString();
}

// A consumer that starts streaming from now and declares a schema version is
// checked against the schema of the ops after its start, as at any other
// position.
TEST_F(CDCServiceTest, SchemaVersion_StartFromNowChecksDeclaredVersion) {
  InsertTestRowsRemote(0, 1, 1);
  AlterAddColumn(schema_, "c1", /*new_version=*/1);
  constexpr int64_t kNow = std::numeric_limits<int64_t>::max();
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChangesWithSchemaVersion(kNow, 1, /*need_schema_info=*/false, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_TRUE(resp.has_checkpoint_op_index());
    EXPECT_EQ(0, resp.records_size()) << resp.DebugString();
  }
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChangesWithSchemaVersion(kNow, 0, /*need_schema_info=*/false, &resp));
    ASSERT_TRUE(resp.has_error()) << resp.DebugString();
    EXPECT_EQ(CDCErrorPB::SCHEMA_VERSION_MISMATCH, resp.error().code())
        << resp.error().DebugString();
  }
}

// A consumer that starts streaming from now while a transaction is open starts
// before the transaction's BEGIN_TXN op. need_schema_info returns the schema of
// the ops after that start, which the transaction's rows were written with,
// even if an ALTER committed since.
TEST_F(CDCServiceTest, NeedSchemaInfo_StartFromNowBelowOpenTxnGetsSchemaOfStart) {
  SeedStreamConfig(CDCStreamConfigPB::CHANGE);
  InsertTestRowsRemote(0, 1, 1);
  const int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 100 }, kTxnId));
  AlterAddColumn(schema_, "c1", /*new_version=*/1);

  GetChangesResponsePB now;
  ASSERT_OK(DoGetChangesWithSchemaVersion(std::numeric_limits<int64_t>::max(), -1,
                                          /*need_schema_info=*/true, &now));
  ASSERT_FALSE(now.has_error()) << now.error().DebugString();
  ASSERT_EQ(1, now.records_size()) << now.DebugString();
  EXPECT_EQ(CDCOpTypePB::DDL, now.records(0).op_type());
  EXPECT_EQ(0, now.records(0).new_schema_version());
  Schema schema;
  ASSERT_OK(SchemaFromPB(now.records(0).new_schema(), &schema));
  EXPECT_EQ(Schema::kColumnNotFound, schema.find_column("c1")) << schema.ToString();

  ASSERT_OK(CommitTxn(kTxnId));
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, now.checkpoint_op_index(), &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  bool saw_alter = false;
  bool saw_txn_row = false;
  for (const auto& r : resp.records()) {
    if (r.op_type() == CDCOpTypePB::DDL) {
      EXPECT_EQ(0, r.schema_version());
      EXPECT_EQ(1, r.new_schema_version());
      saw_alter = true;
    } else if (r.op_type() == CDCOpTypePB::INSERT) {
      EXPECT_EQ(0, r.schema_version()) << r.DebugString();
      saw_txn_row = true;
    }
  }
  EXPECT_TRUE(saw_alter) << resp.DebugString();
  EXPECT_TRUE(saw_txn_row) << resp.DebugString();
}

// ---------------------------------------------------------------------------
// Phase 5: server-driven consistent snapshot
// ---------------------------------------------------------------------------

TEST_F(CDCServiceTest, Snapshot_Basic) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 10, 10);

  GetChangesResponsePB resp;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  int reads = 0;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::READ) reads++;
  }
  EXPECT_EQ(10, reads);
  EXPECT_TRUE(resp.snapshot_done());
  EXPECT_GT(resp.snapshot_streaming_start_op_index(), 0);
}

TEST_F(CDCServiceTest, Snapshot_Pagination) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 50, 50);

  int total_reads = 0;
  int pages = 0;
  bool done = false;
  string resume_key;
  int32_t last_key = -1;
  bool first = true;
  while (!done) {
    GetChangesResponsePB resp;
    // Tiny max_bytes to force pagination (at least one row per page is returned).
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/first, resume_key, &resp, 64));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (int i = 0; i < resp.records_size(); ++i) {
      ASSERT_EQ(CDCOpTypePB::READ, resp.records(i).op_type());
      int32_t k = -1;
      ASSERT_TRUE(GetInt32Col(resp.records(i), /*before=*/false, "key", &k));
      EXPECT_GT(k, last_key);  // strictly increasing across the whole scan
      last_key = k;
      total_reads++;
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    pages++;
    ASSERT_LT(pages, 1000) << "snapshot did not terminate";
  }
  EXPECT_EQ(50, total_reads);
  EXPECT_GT(pages, 1);  // actually paginated
}

// A CHANGE-mode snapshot needs history at snap_ts just like a FULL one. The
// master pushes no history floor for a tablet that has only CHANGE streams;
// that push must not release the floor of the snapshot session in progress.
// Otherwise a compaction between pages reclaims the history at snap_ts and the
// next page silently returns post-snap_ts rows (a torn snapshot).
TEST_F(CDCServiceTest, Snapshot_ChangeMode_FloorSurvivesMasterPush) {
  SeedStreamConfig(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  const int kRows = 10;
  InsertTestRowsRemote(0, kRows, kRows);

  GetChangesResponsePB page;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &page, /*max_bytes=*/64));
  ASSERT_FALSE(page.has_error()) << page.error().DebugString();
  ASSERT_FALSE(page.snapshot_done());
  std::set<int32_t> keys;
  for (const auto& r : page.records()) {
    int32_t k = -1;
    ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
    keys.insert(k);
  }

  // The master's barrier for a CHANGE-only tablet: an op-index floor, no
  // history floor.
  ASSERT_OK(mini_server_->server()->cdc_service()->SetRetentionBarrier(
      kTabletId, /*min_retained_op_index=*/0, /*history_safe_time_micros=*/0,
      /*barrier_seq=*/1));

  // Mutate every row after snap_ts, then GC all history the floor allows.
  for (int i = 0; i < kRows; ++i) {
    UpdateTestRowRemote(i, 777);
  }
  DeleteTestRowsRemote(kRows - 1, 1);
  InsertTestRowsRemote(1000, 1, 1);
  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));

  // The remaining pages are still read at snap_ts.
  string resume_key = page.snapshot_resume_key();
  for (int pages = 0; !page.snapshot_done(); ++pages) {
    ASSERT_LT(pages, 1000);
    page.Clear();
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, resume_key, &page, 64));
    ASSERT_FALSE(page.has_error()) << page.error().DebugString();
    for (const auto& r : page.records()) {
      int32_t k = -1;
      int32_t v = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "int_val", &v));
      EXPECT_NE(777, v) << "post-snapshot value read for key " << k;
      keys.insert(k);
    }
    resume_key = page.snapshot_resume_key();
  }
  ASSERT_EQ(kRows, keys.size());
  EXPECT_EQ(0, *keys.begin());
  EXPECT_EQ(kRows - 1, *keys.rbegin());
}

// If the history at snap_ts is reclaimed between two pages anyway (here the
// master released the tablet's barrier, which drops every history floor), the
// next page must fail with SNAPSHOT_SESSION_LOST rather than return rows read
// over the reclaimed history. A restart of the snapshot then succeeds.
TEST_F(CDCServiceTest, Snapshot_PageAfterHistoryGcFailsLoudly) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  const int kRows = 10;
  InsertTestRowsRemote(0, kRows, kRows);

  GetChangesResponsePB page;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &page, /*max_bytes=*/64));
  ASSERT_FALSE(page.has_error()) << page.error().DebugString();
  ASSERT_FALSE(page.snapshot_done());

  ASSERT_OK(mini_server_->server()->cdc_service()->SetRetentionBarrier(
      kTabletId, /*min_retained_op_index=*/-1, /*history_safe_time_micros=*/0,
      /*barrier_seq=*/1));
  for (int i = 0; i < kRows; ++i) {
    UpdateTestRowRemote(i, 777);
  }
  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(tablet_replica_->tablet()->Compact(tablet::Tablet::FORCE_COMPACT_ALL));

  GetChangesResponsePB next;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, page.snapshot_resume_key(),
                       &next, 64));
  ASSERT_TRUE(next.has_error()) << next.DebugString();
  EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, next.error().code())
      << next.error().DebugString();
  EXPECT_EQ(0, next.records_size()) << next.DebugString();

  // Restarting the snapshot picks a fresh snap_ts and reads the current rows.
  FLAGS_tablet_history_max_age_sec = 3600;
  int reads = 0;
  bool first = true;
  string resume_key;
  for (int pages = 0;; ++pages) {
    ASSERT_LT(pages, 1000);
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, first, resume_key, &resp, 64));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (const auto& r : resp.records()) {
      int32_t v = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "int_val", &v));
      EXPECT_EQ(777, v);
      reads++;
    }
    if (resp.snapshot_done()) break;
    resume_key = resp.snapshot_resume_key();
  }
  EXPECT_EQ(kRows, reads);
}

// A history GC that runs while a page is being scanned (here: right after the
// scan, before the page is returned) must also fail the page, since rows read
// over the reclaimed history may be newer than snap_ts.
TEST_F(CDCServiceTest, Snapshot_HistoryGcDuringPageScanFailsLoudly) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  const int kRows = 10;
  InsertTestRowsRemote(0, kRows, kRows);

  GetChangesResponsePB page;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &page, /*max_bytes=*/64));
  ASSERT_FALSE(page.has_error()) << page.error().DebugString();
  ASSERT_FALSE(page.snapshot_done());
  for (int i = 0; i < kRows; ++i) {
    UpdateTestRowRemote(i, 777);
  }

  // Drop every floor (as the CDC valve does) and GC all history, mid-page.
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  const std::shared_ptr<tablet::Tablet> tablet = tablet_replica_->shared_tablet();
  bool hook_ran = false;
  cdc->SetSnapshotPageScannedHookForTests([&]() {
    hook_ran = true;
    tablet->ClearCDCHistoryFloorHolders();
    FLAGS_tablet_history_max_age_sec = 0;
    CHECK_OK(tablet->Flush());
    CHECK_OK(tablet->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
  });
  GetChangesResponsePB next;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, page.snapshot_resume_key(),
                       &next, 64));
  cdc->SetSnapshotPageScannedHookForTests(nullptr);
  ASSERT_TRUE(hook_ran);
  ASSERT_TRUE(next.has_error()) << next.DebugString();
  EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, next.error().code())
      << next.error().DebugString();
  EXPECT_EQ(0, next.records_size()) << next.DebugString();
}

// A snapshot page is served after the position the client sends: the end of
// the last page (the client got it) or its start (the client lost the
// response). Any other key, here the key of an earlier page that the client
// replays, is refused with SNAPSHOT_SESSION_LOST rather than silently
// repositioning the scan: serving after it would repeat rows, and serving after
// the server's own position would skip the rows of a page the client lost. A
// restart from the beginning still works and scans from the start of the
// table.
TEST_F(CDCServiceTest, Snapshot_StaleResumeKeyRefused) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  FLAGS_cdc_max_records_per_response = 1;
  InsertTestRowsRemote(0, 10, 10);

  GetChangesResponsePB p1;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &p1));
  ASSERT_FALSE(p1.has_error()) << p1.error().DebugString();
  ASSERT_FALSE(p1.snapshot_done());
  GetChangesResponsePB p2;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &p2));
  ASSERT_FALSE(p2.has_error()) << p2.error().DebugString();
  GetChangesResponsePB p3;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p2.snapshot_resume_key(), &p3));
  ASSERT_FALSE(p3.has_error()) << p3.error().DebugString();
  ASSERT_FALSE(p3.snapshot_done());

  // The key of the first page is neither the start nor the end of the last one.
  GetChangesResponsePB stale;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &stale));
  ASSERT_TRUE(stale.has_error()) << stale.DebugString();
  EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, stale.error().code());
  EXPECT_EQ(0, stale.records_size());
  // The refusal keeps the session: none was lost.
  EXPECT_EQ(0, mini_server_->server()->cdc_service()->SnapshotSessionsLostForTests());

  // A restart from the beginning scans the whole table again.
  vector<int32_t> keys;
  bool first = true;
  string resume_key;
  bool done = false;
  while (!done) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, first, resume_key, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
      keys.push_back(k);
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    ASSERT_LT(keys.size(), 100);
  }
  ASSERT_EQ(10, keys.size());
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(i, keys[i]);
  }
}

// A page whose response is lost (e.g. the RPC timed out on the client) is
// served again when the client re-sends the same resume key. Before, the server
// had already moved its cursor past the page and served the next one, so the
// rows of the lost page were never delivered.
TEST_F(CDCServiceTest, Snapshot_LostResponseIsReserved) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  FLAGS_cdc_max_records_per_response = 2;
  const int kRows = 10;
  InsertTestRowsRemote(0, kRows, kRows);

  const auto page_keys = [&](const GetChangesResponsePB& resp) {
    vector<int32_t> keys;
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      CHECK(GetInt32Col(r, /*before=*/false, "key", &k));
      keys.push_back(k);
    }
    return keys;
  };

  vector<int32_t> delivered;
  GetChangesResponsePB p1;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &p1));
  ASSERT_FALSE(p1.has_error()) << p1.error().DebugString();
  const vector<int32_t> p1_keys = page_keys(p1);
  delivered.insert(delivered.end(), p1_keys.begin(), p1_keys.end());

  // The second page is served, but its response never reaches the client.
  GetChangesResponsePB lost;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &lost));
  ASSERT_FALSE(lost.has_error()) << lost.error().DebugString();
  ASSERT_FALSE(lost.snapshot_done());

  // The client re-sends its key and gets the same page again.
  GetChangesResponsePB again;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &again));
  ASSERT_FALSE(again.has_error()) << again.error().DebugString();
  EXPECT_EQ(page_keys(lost), page_keys(again));
  EXPECT_EQ(lost.snapshot_resume_key(), again.snapshot_resume_key());

  // Drain the rest from the page the client did get.
  GetChangesResponsePB resp = again;
  while (true) {
    const vector<int32_t> keys = page_keys(resp);
    delivered.insert(delivered.end(), keys.begin(), keys.end());
    if (resp.snapshot_done()) {
      break;
    }
    const string key = resp.snapshot_resume_key();
    resp.Clear();
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, key, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_LT(delivered.size(), 100);
  }
  ASSERT_EQ(kRows, delivered.size());
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(i, delivered[i]);
  }
}

// A client that lost the response of the first page re-sends
// is_snapshot_start=true. The active session is kept (same snapshot timestamp,
// one establish), and the scan starts again at the beginning of the table.
// Before, the second start continued after the lost page, so the head of the
// table was never delivered.
TEST_F(CDCServiceTest, Snapshot_LostFirstPageRestartRescansHead) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  FLAGS_cdc_max_records_per_response = 2;
  const int kRows = 6;
  InsertTestRowsRemote(0, kRows, kRows);
  auto* cdc = mini_server_->server()->cdc_service();

  GetChangesResponsePB lost;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &lost));
  ASSERT_FALSE(lost.has_error()) << lost.error().DebugString();
  ASSERT_FALSE(lost.snapshot_done());

  vector<int32_t> delivered;
  bool first = true;
  string resume_key;
  bool done = false;
  while (!done) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, first, resume_key, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
      delivered.push_back(k);
      EXPECT_EQ(lost.records(0).timestamp(), r.timestamp()) << "snapshot timestamp changed";
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    ASSERT_LT(delivered.size(), 100);
  }
  EXPECT_EQ(1, cdc->SnapshotSessionsEstablishedForTests());
  ASSERT_EQ(kRows, delivered.size());
  for (int i = 0; i < kRows; ++i) {
    EXPECT_EQ(i, delivered[i]);
  }
}

// A snapshot page stops scanning once its deadline passed and returns the rows
// it read so far, instead of running past the client's deadline (whereupon the
// client gives up on the page and re-sends it). The following pages continue
// after them.
TEST_F(CDCServiceTest, Snapshot_PageEndsAtDeadline) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  const int kRows = 1000;
  InsertTestRowsRemote(0, kRows, kRows);
  FLAGS_cdc_inject_snapshot_scan_deadline_expired = true;

  std::set<int32_t> keys;
  int pages = 0;
  bool first = true;
  string resume_key;
  bool done = false;
  while (!done) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, first, resume_key, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    if (pages == 0) {
      EXPECT_FALSE(resp.snapshot_done()) << "the first page ignored its deadline";
      EXPECT_LT(resp.records_size(), kRows);
    }
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
      EXPECT_TRUE(keys.insert(k).second) << "key " << k << " delivered twice";
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    ASSERT_LT(++pages, 100);
  }
  EXPECT_EQ(kRows, keys.size());
  EXPECT_GT(pages, 1);
}

// The snapshot timestamp is picked after the WAL streaming start point. An op
// that commits while the snapshot is established, between the two, is then
// below the snapshot timestamp, so the snapshot sees it (the stream repeats
// it). Picked the other way round, the op is above the snapshot timestamp but
// at or below the streaming start point: in neither the snapshot nor the
// stream.
TEST_F(CDCServiceTest, Snapshot_NoLossWhenOpsCommitDuringEstablish) {
  SeedStreamConfig(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 5, 5);
  const int32_t kRacingKey = 1000;
  auto* cdc = mini_server_->server()->cdc_service();
  cdc->SetSnapshotEstablishHookForTests([&]() {
    InsertOneRow(kRacingKey);
  });

  GetChangesResponsePB snap;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &snap));
  cdc->SetSnapshotEstablishHookForTests(nullptr);
  ASSERT_FALSE(snap.has_error()) << snap.error().DebugString();
  ASSERT_TRUE(snap.snapshot_done());
  bool seen = false;
  for (const auto& r : snap.records()) {
    int32_t k = -1;
    ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
    seen |= k == kRacingKey;
  }

  GetChangesResponsePB stream;
  ASSERT_OK(DoGetChanges(kTabletId, snap.snapshot_streaming_start_op_index(), &stream));
  ASSERT_FALSE(stream.has_error()) << stream.error().DebugString();
  for (const auto& r : stream.records()) {
    int32_t k = -1;
    if (r.op_type() == CDCOpTypePB::INSERT &&
        GetInt32Col(r, /*before=*/false, "key", &k)) {
      seen |= k == kRacingKey;
    }
  }
  EXPECT_TRUE(seen) << "a row written during the establish was lost";
}

// The pages of a snapshot read with the schema the snapshot started with: a
// column added by an ALTER between two pages is not in the later pages (its
// DDL reaches the consumer later, from the WAL), and every READ record carries
// the version of the schema it was read with.
TEST_F(CDCServiceTest, Snapshot_AlterAddColumnBetweenPages_ProjectionStableAndStamped) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  FLAGS_cdc_max_records_per_response = 1;
  InsertTestRowsRemote(0, 3, 3);
  const uint32_t version = tablet_replica_->tablet_metadata()->schema_version();

  const auto column_names = [](const CDCRecordPB& r) {
    vector<string> names;
    for (const auto& c : r.changes()) {
      names.push_back(c.column_name());
    }
    return names;
  };

  GetChangesResponsePB p1;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &p1));
  ASSERT_FALSE(p1.has_error()) << p1.error().DebugString();
  ASSERT_EQ(1, p1.records_size());
  ASSERT_TRUE(p1.records(0).has_schema_version());
  EXPECT_EQ(version, p1.records(0).schema_version());
  const vector<string> columns = column_names(p1.records(0));

  AlterAddColumn(schema_, "extra", version + 1);

  string resume_key = p1.snapshot_resume_key();
  int rows = 1;
  bool done = false;
  while (!done) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, resume_key, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& r : resp.records()) {
      EXPECT_EQ(columns, column_names(r));
      ASSERT_TRUE(r.has_schema_version());
      EXPECT_EQ(version, r.schema_version());
      rows++;
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    ASSERT_LT(rows, 100);
  }
  EXPECT_EQ(3, rows);
}

// A column of the snapshot schema that an ALTER drops between two pages cannot
// be read any more: the next page fails with SNAPSHOT_SESSION_LOST instead of
// silently returning rows without it, and a new snapshot reads with the new
// schema.
TEST_F(CDCServiceTest, Snapshot_AlterDropColumnBetweenPages_RequestsResnapshot) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  FLAGS_cdc_max_records_per_response = 1;
  InsertTestRowsRemote(0, 3, 3);

  GetChangesResponsePB p1;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &p1));
  ASSERT_FALSE(p1.has_error()) << p1.error().DebugString();
  ASSERT_NE(nullptr, FindCol(p1.records(0), /*before=*/false, "string_val"));

  {
    SchemaBuilder builder(schema_);
    ASSERT_OK(builder.RemoveColumn("string_val"));
    const Schema new_schema = builder.Build();
    tserver::AlterSchemaRequestPB req;
    tserver::AlterSchemaResponsePB resp;
    req.set_dest_uuid(mini_server_->server()->fs_manager()->uuid());
    req.set_tablet_id(kTabletId);
    req.set_schema_version(tablet_replica_->tablet_metadata()->schema_version() + 1);
    ASSERT_OK(SchemaToPB(new_schema, req.mutable_schema()));
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(30));
    ASSERT_OK(admin_proxy_->AlterSchema(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  GetChangesResponsePB p2;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &p2));
  ASSERT_TRUE(p2.has_error()) << p2.DebugString();
  EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, p2.error().code());
  EXPECT_EQ(0, p2.records_size());

  // A new snapshot reads every row with the new schema.
  int rows = 0;
  bool first = true;
  string resume_key;
  bool done = false;
  while (!done) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, first, resume_key, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (const auto& r : resp.records()) {
      EXPECT_EQ(nullptr, FindCol(r, /*before=*/false, "string_val"));
      rows++;
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    ASSERT_LT(rows, 100);
  }
  EXPECT_EQ(3, rows);
}

// The response of the last page of a snapshot can be lost too. That page is
// served again, and a restart of the snapshot scans again at the same snapshot
// timestamp, until the client acknowledges the page by streaming the WAL from
// the streaming start point. Before, the last page ended the session at once,
// so its re-sent request got SNAPSHOT_SESSION_LOST and its rows were never
// delivered.
TEST_F(CDCServiceTest, Snapshot_LostFinalPageIsReservedUntilStreamed) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  FLAGS_cdc_max_records_per_response = 2;
  InsertTestRowsRemote(0, 4, 4);
  auto* cdc = mini_server_->server()->cdc_service();

  const auto page_keys = [&](const GetChangesResponsePB& resp) {
    vector<int32_t> keys;
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      CHECK(GetInt32Col(r, /*before=*/false, "key", &k));
      keys.push_back(k);
    }
    return keys;
  };

  GetChangesResponsePB p1;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &p1));
  ASSERT_FALSE(p1.has_error()) << p1.error().DebugString();
  ASSERT_FALSE(p1.snapshot_done());
  ASSERT_EQ((vector<int32_t>{ 0, 1 }), page_keys(p1));
  const uint64_t snap_ts = p1.records(0).timestamp();

  // The last page is served, but its response never reaches the client.
  GetChangesResponsePB lost;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &lost));
  ASSERT_FALSE(lost.has_error()) << lost.error().DebugString();
  ASSERT_TRUE(lost.snapshot_done());
  ASSERT_EQ((vector<int32_t>{ 2, 3 }), page_keys(lost));
  const int64_t start = lost.snapshot_streaming_start_op_index();

  // The client re-sends its key and gets the same page again.
  GetChangesResponsePB again;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &again));
  ASSERT_FALSE(again.has_error()) << again.error().DebugString();
  ASSERT_TRUE(again.snapshot_done());
  EXPECT_EQ((vector<int32_t>{ 2, 3 }), page_keys(again));
  EXPECT_EQ(start, again.snapshot_streaming_start_op_index());
  EXPECT_EQ(snap_ts, again.records(0).timestamp());

  // A client that restarts the snapshot (it failed to deliver the last page)
  // reads at the same snapshot timestamp.
  GetChangesResponsePB restart;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &restart));
  ASSERT_FALSE(restart.has_error()) << restart.error().DebugString();
  EXPECT_EQ((vector<int32_t>{ 0, 1 }), page_keys(restart));
  EXPECT_EQ(snap_ts, restart.records(0).timestamp());
  EXPECT_EQ(1, cdc->SnapshotSessionsEstablishedForTests());
  GetChangesResponsePB last;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, restart.snapshot_resume_key(), &last));
  ASSERT_FALSE(last.has_error()) << last.error().DebugString();
  ASSERT_TRUE(last.snapshot_done());
  EXPECT_EQ(start, last.snapshot_streaming_start_op_index());

  // Streaming the WAL from the streaming start point acknowledges the last
  // page: the snapshot is complete, and a new one takes a new timestamp.
  GetChangesResponsePB stream;
  ASSERT_OK(DoGetChanges(kTabletId, start, &stream));
  ASSERT_FALSE(stream.has_error()) << stream.error().DebugString();
  GetChangesResponsePB stale;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, restart.snapshot_resume_key(), &stale));
  ASSERT_TRUE(stale.has_error()) << stale.DebugString();
  EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, stale.error().code());
  GetChangesResponsePB fresh;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &fresh));
  ASSERT_FALSE(fresh.has_error()) << fresh.error().DebugString();
  EXPECT_EQ(2, cdc->SnapshotSessionsEstablishedForTests());
  EXPECT_GT(fresh.records(0).timestamp(), snap_ts);
}

// A checkpoint at or past the streaming start point acknowledges the last page
// of a snapshot as well; one below it (a consumer that failed in the middle of
// the snapshot checkpoints its initial position) does not.
TEST_F(CDCServiceTest, Snapshot_CheckpointAcknowledgesFinalPage) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  FLAGS_cdc_max_records_per_response = 2;
  InsertTestRowsRemote(0, 4, 4);
  auto* cdc = mini_server_->server()->cdc_service();

  GetChangesResponsePB p1;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &p1));
  ASSERT_FALSE(p1.has_error()) << p1.error().DebugString();
  GetChangesResponsePB p2;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &p2));
  ASSERT_FALSE(p2.has_error()) << p2.error().DebugString();
  ASSERT_TRUE(p2.snapshot_done());
  const int64_t start = p2.snapshot_streaming_start_op_index();
  ASSERT_GT(start, 0);

  CheckpointResponsePB ckpt;
  ASSERT_OK(DoCheckpoint(kTabletId, 0, &ckpt));
  ASSERT_FALSE(ckpt.has_error()) << ckpt.error().DebugString();
  GetChangesResponsePB again;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &again));
  ASSERT_FALSE(again.has_error()) << again.error().DebugString();
  EXPECT_TRUE(again.snapshot_done());
  EXPECT_EQ(2, again.records_size());

  ckpt.Clear();
  ASSERT_OK(DoCheckpoint(kTabletId, start, &ckpt));
  ASSERT_FALSE(ckpt.has_error()) << ckpt.error().DebugString();
  GetChangesResponsePB stale;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &stale));
  ASSERT_TRUE(stale.has_error()) << stale.DebugString();
  EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, stale.error().code());
  GetChangesResponsePB fresh;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &fresh));
  ASSERT_FALSE(fresh.has_error()) << fresh.error().DebugString();
  EXPECT_EQ(2, cdc->SnapshotSessionsEstablishedForTests());
}

// A read-only GetChanges, from a consumer that does not checkpoint the
// stream, does not acknowledge the last page of another consumer's snapshot,
// and cannot take a snapshot itself.
TEST_F(CDCServiceTest, Snapshot_ReadOnlyReadDoesNotAcknowledge) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  FLAGS_cdc_max_records_per_response = 2;
  InsertTestRowsRemote(0, 4, 4);

  GetChangesResponsePB p1;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &p1));
  ASSERT_FALSE(p1.has_error()) << p1.error().DebugString();
  GetChangesResponsePB p2;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &p2));
  ASSERT_FALSE(p2.has_error()) << p2.error().DebugString();
  ASSERT_TRUE(p2.snapshot_done());
  const int64_t start = p2.snapshot_streaming_start_op_index();

  const auto read_only_get_changes = [&](bool is_snapshot_start, GetChangesResponsePB* resp) {
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(start);
    req.set_is_snapshot_start(is_snapshot_start);
    req.set_read_only(true);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    return cdc_proxy_->GetChanges(req, resp, &rpc);
  };
  GetChangesResponsePB snap;
  ASSERT_OK(read_only_get_changes(/*is_snapshot_start=*/true, &snap));
  ASSERT_TRUE(snap.has_error()) << snap.DebugString();
  ASSERT_STR_CONTAINS(snap.error().status().message(), "read-only");
  GetChangesResponsePB stream;
  ASSERT_OK(read_only_get_changes(/*is_snapshot_start=*/false, &stream));
  ASSERT_FALSE(stream.has_error()) << stream.error().DebugString();

  // The last page is still served again: the snapshot is not complete.
  GetChangesResponsePB again;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, p1.snapshot_resume_key(), &again));
  ASSERT_FALSE(again.has_error()) << again.error().DebugString();
  EXPECT_TRUE(again.snapshot_done());
  EXPECT_EQ(start, again.snapshot_streaming_start_op_index());
}

// An ALTER that applies while a snapshot is being established is not ordered
// against the snapshot timestamp. The establish fails with a retryable error
// instead of pinning a schema version that the WAL after the streaming start
// point only introduces later, and the retry reads with the new schema. Before,
// the READ records were stamped with the new version, and the stream then
// delivered the ALTER to that version again.
TEST_F(CDCServiceTest, Snapshot_AlterDuringEstablishIsRetried) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 3, 3);
  auto* cdc = mini_server_->server()->cdc_service();
  const uint32_t version = tablet_replica_->tablet_metadata()->schema_version();
  bool altered = false;
  cdc->SetSnapshotEstablishHookForTests([&]() {
    if (!altered) {
      altered = true;
      AlterAddColumn(schema_, "extra", version + 1);
    }
  });

  GetChangesResponsePB snap;
  int busy = 0;
  for (int attempt = 0; attempt < 5; ++attempt) {
    snap.Clear();
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &snap));
    if (!snap.has_error()) {
      break;
    }
    ASSERT_EQ(CDCErrorPB::SERVER_TOO_BUSY, snap.error().code()) << snap.DebugString();
    EXPECT_EQ(0, snap.records_size());
    busy++;
  }
  cdc->SetSnapshotEstablishHookForTests(nullptr);
  ASSERT_TRUE(altered);
  ASSERT_FALSE(snap.has_error()) << snap.error().DebugString();
  ASSERT_TRUE(snap.snapshot_done());
  ASSERT_EQ(3, snap.records_size());
  EXPECT_EQ(1, busy);

  // The stream after the snapshot holds no ALTER to a version the snapshot
  // records already have.
  GetChangesResponsePB stream;
  ASSERT_OK(DoGetChanges(kTabletId, snap.snapshot_streaming_start_op_index(), &stream));
  ASSERT_FALSE(stream.has_error()) << stream.error().DebugString();
  for (const auto& r : snap.records()) {
    ASSERT_TRUE(r.has_schema_version());
    EXPECT_EQ(version + 1, r.schema_version());
    for (const auto& s : stream.records()) {
      if (s.op_type() == CDCOpTypePB::DDL) {
        EXPECT_LT(r.schema_version(), s.new_schema_version()) << s.DebugString();
      }
    }
  }
}

// A consumer that starts streaming from now while a transaction is committing
// (BEGIN_COMMIT applied, FINALIZE_COMMIT not yet) must get the transaction's
// rows when it commits. The call waits for the commit to apply; before, it
// then saw the transaction committed below "now" and started the stream past
// the transaction's writes, which the stream then emitted as an empty
// BEGIN/COMMIT.
TEST_F(CDCServiceTest, TailFromNow_TxnInBeginCommitIsDelivered) {
  SeedStreamConfig(CDCStreamConfigPB::CHANGE);
  InsertTestRowsRemote(0, 2, 2);
  const int64_t kTxnId = 1;
  const auto participant_op = [&](ParticipantOpPB::ParticipantOpType type, int64_t commit_ts) {
    ParticipantResponsePB resp;
    RETURN_NOT_OK(tablet::CallParticipantOp(tablet_replica_.get(), kTxnId, type, commit_ts,
                                            &resp));
    return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
  };
  ASSERT_OK(participant_op(ParticipantOpPB::BEGIN_TXN, -1));
  for (int32_t key = 100; key < 103; ++key) {
    tserver::WriteRequestPB req;
    req.set_tablet_id(kTabletId);
    req.set_txn_id(kTxnId);
    ASSERT_OK(SchemaToPB(schema_, req.mutable_schema()));
    AddTestRowWithNullableStringToPB(RowOperationsPB::INSERT, schema_, key, key, "txn",
                                     req.mutable_row_operations());
    tserver::WriteResponsePB resp;
    unique_ptr<tablet::WriteOpState> op_state(
        new tablet::WriteOpState(tablet_replica_.get(), &req, nullptr, &resp));
    CountDownLatch latch(1);
    op_state->set_completion_callback(unique_ptr<tablet::OpCompletionCallback>(
        new tablet::LatchOpCompletionCallback<tserver::WriteResponsePB>(&latch, &resp)));
    ASSERT_OK(tablet_replica_->SubmitWrite(std::move(op_state)));
    latch.Wait();
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_EQ(0, resp.per_row_errors_size()) << resp.DebugString();
  }
  ASSERT_OK(participant_op(ParticipantOpPB::BEGIN_COMMIT, -1));
  // A single-participant transaction commits at its BEGIN_COMMIT timestamp,
  // which is below "now" below.
  const auto txn_meta =
      FindOrDie(tablet_replica_->tablet_metadata()->GetTxnMetadata(), kTxnId);
  ASSERT_TRUE(txn_meta->commit_mvcc_op_timestamp());
  const Timestamp commit_ts = *txn_meta->commit_mvcc_op_timestamp();

  // Start from now, as CDCConsumer::kNow does, and let the transaction commit
  // while the call waits for it.
  GetChangesResponsePB now;
  Status now_status;
  std::thread prober([&]() {
    now_status = DoGetChanges(kTabletId, std::numeric_limits<int64_t>::max(), &now);
  });
  ASSERT_EVENTUALLY([&]() {
    ASSERT_GE(tablet_replica_->tablet()->mvcc_manager()->GetNumWaitersForTests(), 1);
  });
  ASSERT_OK(participant_op(ParticipantOpPB::FINALIZE_COMMIT, commit_ts.value()));
  prober.join();
  ASSERT_OK(now_status);
  ASSERT_FALSE(now.has_error()) << now.error().DebugString();

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, now.checkpoint_op_index(), &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  vector<int32_t> keys;
  for (const auto& r : resp.records()) {
    if (r.op_type() == CDCOpTypePB::INSERT) {
      int32_t k = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
      keys.push_back(k);
    }
  }
  EXPECT_EQ((vector<int32_t>{ 100, 101, 102 }), keys);
}

// E11: concurrent cache misses for the same stream_id must collapse to a single
// master GetCDCStreamInfo RPC. Without single-flight, N tablets of one stream
// re-streaming on one tserver right after a restart each issue their own fetch,
// spiking master catalog-lock contention. With it, one caller fetches and the
// rest wait on the per-stream lock, then re-read the just-populated cache.
TEST_F(CDCServiceTest, StreamConfig_ConcurrentMissesSingleFlight) {
  // Once the first fetch refreshes the entry, keep it fresh for the whole test
  // so every waiter's re-check is a hit.
  FLAGS_cdc_stream_config_cache_ttl_ms = 60000;
  // Widen the fetch window so all threads pile up on the single-flight lock
  // before the first fetch completes.
  FLAGS_cdc_inject_latency_before_stream_config_fetch_ms = 500;

  // Seed a valid FULL config but already expired, so the next access misses the
  // fast path and takes the fetch route. There is no live master in this
  // fixture: the fetch finds no master address and serves this stale entry --
  // which is all the single-flight path needs to exercise.
  CDCStreamConfigPB config;
  config.set_record_type(CDCStreamConfigPB::FULL);
  config.set_snapshot_mode(CDCStreamConfigPB::NEVER);
  auto* cdc = mini_server_->server()->cdc_service();
  cdc->SetStreamConfigForTestsWithTtl("test-stream-1", config, { TableId() },
                                      MonoDelta::FromMilliseconds(-1));

  const int64_t before = cdc->StreamConfigMasterFetchesForTests();

  // Fire N concurrent GetChanges that all miss the expired cache for the same
  // stream. Their GetChanges results are irrelevant; only the fetch count is.
  const int kThreads = 8;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([this]() {
      GetChangesResponsePB resp;
      Status s = DoGetChanges(kTabletId, 0, &resp);
      (void)s;
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  const int64_t fetches = cdc->StreamConfigMasterFetchesForTests() - before;
  EXPECT_EQ(1, fetches)
      << "concurrent cache misses did not collapse to a single master fetch";
}

TEST_F(CDCServiceTest, Snapshot_HandoffToWal) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 5, 5);

  GetChangesResponsePB snap;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &snap));
  ASSERT_FALSE(snap.has_error()) << snap.error().DebugString();
  ASSERT_TRUE(snap.snapshot_done());
  const int64_t start = snap.snapshot_streaming_start_op_index();

  // Write more rows after the snapshot, then stream from the handoff point.
  InsertTestRowsRemote(100, 3, 3);

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, start, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  int inserts = 0;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::INSERT) inserts++;
  }
  EXPECT_EQ(3, inserts);
}

TEST_F(CDCServiceTest, Snapshot_RejectedWhenModeNever) {
  SeedStreamConfig(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::NEVER);
  InsertTestRowsRemote(0, 3, 3);

  GetChangesResponsePB resp;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::UNKNOWN_ERROR, resp.error().code());
  EXPECT_EQ(0, resp.records_size());
}

// E4: a snapshot resume whose in-memory session was lost (e.g. leader change)
// must be rejected with SNAPSHOT_SESSION_LOST rather than silently rescanning
// the table tail at a freshly picked timestamp -- which would splice a tail read
// at T2 onto a head read at T1 and produce an inconsistent snapshot. This server
// never established a session for these resume keys, which is exactly the state a
// newly elected leader is in mid-snapshot.
TEST_F(CDCServiceTest, Snapshot_ResumeWithoutSessionRejected) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 10, 10);

  // Obtain a plausible resume_key by starting (and here fully completing) a
  // snapshot, then use it as though a new leader had never seen the session.
  GetChangesResponsePB first;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &first, /*max_bytes=*/64));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  const string resume_key = first.snapshot_resume_key();
  ASSERT_FALSE(resume_key.empty());

  // Drop the in-memory session to simulate a leader change discarding it.
  mini_server_->server()->cdc_service()->ClearSnapshotSessionsForTests();

  // (a) Continue-style resume (is_snapshot_start=false + resume_key) with no live
  //     session -> rejected.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, resume_key, &resp));
    ASSERT_TRUE(resp.has_error());
    EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, resp.error().code());
    // SNAPSHOT_SESSION_LOST is not retryable in place: the consumer must take
    // a new snapshot (is_snapshot_start=true, no resume key).
    EXPECT_FALSE(resp.error().is_retryable()) << resp.error().DebugString();
    EXPECT_TRUE(resp.error().needs_resnapshot()) << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
  }

  // (b) The E4-precise case: the consumer retries with is_snapshot_start=true but
  //     still carries its old resume_key. A fresh snap_ts must not honor a stale
  //     resume key -> rejected.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, resume_key, &resp));
    ASSERT_TRUE(resp.has_error());
    EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, resp.error().code());
    EXPECT_FALSE(resp.error().is_retryable()) << resp.error().DebugString();
    EXPECT_TRUE(resp.error().needs_resnapshot()) << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
  }

  // A clean restart (is_snapshot_start=true, no resume key) still succeeds.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
}

// The first page of a snapshot restarted by a consumer (is_snapshot_start)
// after the tablet server lost the snapshot session continues the snapshot of
// the master's durable cursor: it is read at the same snapshot timestamp, from
// the start of the table, and streams from the same streaming start point, so
// a row that the consumer got from the earlier pages and that was deleted
// since is in the snapshot again and its DELETE in the stream. Before, the
// tablet server took a new snapshot: the deleted row was in neither.
TEST_F(CDCServiceTest, Snapshot_StartWithoutSessionContinuesDurableSnapshot) {
  const int kRows = 10;
  InsertTestRowsRemote(0, kRows, kRows);
  const int64_t streaming_start =
      tablet_replica_->shared_consensus()->GetLastOpId(consensus::COMMITTED_OPID)->index();
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();

  GetChangesResponsePB first;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &first, /*max_bytes=*/64));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  ASSERT_FALSE(first.snapshot_done());
  ASSERT_GT(first.records_size(), 0);
  const uint64_t snap_ts = first.records(0).timestamp();
  int32_t first_key = -1;
  ASSERT_TRUE(GetInt32Col(first.records(0), /*before=*/false, "key", &first_key));
  ASSERT_EQ(0, first_key);

  // The master has the cursor of the first page, and the tablet server loses
  // the session. The consumer's first row is deleted.
  SnapshotCursorUpdate cursor;
  cursor.snap_ts = snap_ts;
  cursor.active = true;
  SetDurableSnapshotCursor(cursor, streaming_start);
  cdc->ClearSnapshotSessionsForTests();
  NO_FATALS(DeleteTestRowsRemote(0, 1));
  const int64_t established = cdc->SnapshotSessionsEstablishedForTests();
  const int64_t resumed = cdc->SnapshotSessionsResumedForTests();

  std::set<int32_t> keys;
  bool is_start = true;
  string resume_key;
  GetChangesResponsePB page;
  for (int pages = 0;; ++pages) {
    ASSERT_LT(pages, 1000);
    page.Clear();
    ASSERT_OK(DoSnapshot(kTabletId, is_start, resume_key, &page, /*max_bytes=*/64));
    ASSERT_FALSE(page.has_error()) << page.error().DebugString();
    is_start = false;
    for (const auto& r : page.records()) {
      EXPECT_EQ(snap_ts, r.timestamp()) << "read at a new snapshot timestamp";
      int32_t k = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
      keys.insert(k);
    }
    if (page.snapshot_done()) break;
    resume_key = page.snapshot_resume_key();
  }
  EXPECT_EQ(kRows, keys.size());
  EXPECT_EQ(1, keys.count(0)) << "the snapshot is not read at its snapshot timestamp";
  EXPECT_EQ(streaming_start, page.snapshot_streaming_start_op_index());
  EXPECT_EQ(established, cdc->SnapshotSessionsEstablishedForTests());
  EXPECT_EQ(resumed + 1, cdc->SnapshotSessionsResumedForTests());

  // The stream after the snapshot has the DELETE.
  GetChangesResponsePB stream;
  ASSERT_OK(DoGetChanges(kTabletId, page.snapshot_streaming_start_op_index(), &stream));
  ASSERT_FALSE(stream.has_error()) << stream.error().DebugString();
  int deletes = 0;
  for (const auto& r : stream.records()) {
    if (r.op_type() == CDCOpTypePB::DELETE) {
      int32_t k = -1;
      ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k));
      EXPECT_EQ(0, k);
      ++deletes;
    }
  }
  EXPECT_EQ(1, deletes);
}

// A snapshot start that would continue the snapshot of the durable cursor,
// whose history is gone, fails with SNAPSHOT_SESSION_LOST (needs_resnapshot)
// instead of taking a new snapshot that the client did not ask for. One with
// new_snapshot_generation takes a new snapshot. Before, the start took a new
// snapshot without telling the client.
TEST_F(CDCServiceTest, Snapshot_StartWithoutSessionHistoryGoneIsLost) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  const int kRows = 10;
  InsertTestRowsRemote(0, kRows, kRows);
  const int64_t streaming_start =
      tablet_replica_->shared_consensus()->GetLastOpId(consensus::COMMITTED_OPID)->index();
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  const std::shared_ptr<tablet::Tablet> tablet = tablet_replica_->shared_tablet();

  GetChangesResponsePB first;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &first, /*max_bytes=*/64));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  ASSERT_FALSE(first.snapshot_done());
  const uint64_t snap_ts = first.records(0).timestamp();
  SnapshotCursorUpdate cursor;
  cursor.snap_ts = snap_ts;
  cursor.active = true;
  SetDurableSnapshotCursor(cursor, streaming_start);

  // The session is lost, and the history at snap_ts is collected.
  cdc->ClearSnapshotSessionsForTests();
  for (int i = 0; i < kRows; ++i) {
    UpdateTestRowRemote(i, 777);
  }
  tablet->ClearCDCHistoryFloorHolders();
  FLAGS_tablet_history_max_age_sec = 0;
  ASSERT_OK(tablet->Flush());
  ASSERT_OK(tablet->Compact(tablet::Tablet::FORCE_COMPACT_ALL));
  FLAGS_tablet_history_max_age_sec = 3600;
  ASSERT_GT(tablet->cdc_history_gc_water_mark(), Timestamp(snap_ts));

  const int64_t established = cdc->SnapshotSessionsEstablishedForTests();
  for (int attempt = 0; attempt < 2; ++attempt) {
    SCOPED_TRACE(Substitute("attempt $0", attempt));
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp, /*max_bytes=*/64));
    ASSERT_TRUE(resp.has_error()) << "a new snapshot was taken: " << resp.ShortDebugString();
    EXPECT_EQ(CDCErrorPB::SNAPSHOT_SESSION_LOST, resp.error().code());
    EXPECT_FALSE(resp.error().is_retryable());
    EXPECT_TRUE(resp.error().needs_resnapshot());
    EXPECT_STR_CONTAINS(resp.error().status().message(), "new_snapshot_generation");
    EXPECT_EQ(0, resp.records_size());
    EXPECT_FALSE(cdc->HasActiveSnapshotForTests("test-stream-1", kTabletId));
    // The pin taken for the attempt is undone.
    EXPECT_EQ(Timestamp(0), tablet->cdc_history_floor());
  }
  EXPECT_EQ(established, cdc->SnapshotSessionsEstablishedForTests());

  // The client asks for a new snapshot: it is read at a new timestamp, with
  // the current rows.
  GetChangesRequestPB req;
  req.set_stream_id("test-stream-1");
  req.set_tablet_id(kTabletId);
  req.set_is_snapshot_start(true);
  req.set_new_snapshot_generation(true);
  GetChangesResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(cdc_proxy_->GetChanges(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_GT(resp.records_size(), 0);
  EXPECT_GT(resp.records(0).timestamp(), snap_ts);
  int32_t v = -1;
  ASSERT_TRUE(GetInt32Col(resp.records(0), /*before=*/false, "int_val", &v));
  EXPECT_EQ(777, v);
  EXPECT_EQ(established + 1, cdc->SnapshotSessionsEstablishedForTests());
}

// A snapshot page for which the tablet server has no session needs the
// master's durable cursor. If the master cannot be reached, the page fails
// with the retryable SERVER_TOO_BUSY: neither a new snapshot nor
// SNAPSHOT_SESSION_LOST. Before, a start took a new snapshot and a resume
// failed with SNAPSHOT_SESSION_LOST.
TEST_F(CDCServiceTest, Snapshot_WithoutSessionMasterUnreachableIsRetried) {
  InsertTestRowsRemote(0, 10, 10);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  GetChangesResponsePB first;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &first, /*max_bytes=*/64));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  ASSERT_FALSE(first.snapshot_done());
  cdc->ClearSnapshotSessionsForTests();

  SetDurableSnapshotCursorFetchStatus(Status::NetworkError("the master is unreachable"));
  const int64_t established = cdc->SnapshotSessionsEstablishedForTests();
  const int64_t lost = cdc->SnapshotSessionsLostForTests();
  const int fetches = DurableSnapshotCursorFetches();
  for (bool is_start : {true, false}) {
    SCOPED_TRACE(is_start ? "start" : "resume");
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, is_start, is_start ? "" : first.snapshot_resume_key(),
                         &resp, /*max_bytes=*/64));
    ASSERT_TRUE(resp.has_error()) << resp.ShortDebugString();
    EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code())
        << resp.error().DebugString();
    EXPECT_TRUE(resp.error().is_retryable());
    EXPECT_EQ(0, resp.records_size());
    EXPECT_FALSE(cdc->HasActiveSnapshotForTests("test-stream-1", kTabletId));
  }
  EXPECT_EQ(fetches + 2, DurableSnapshotCursorFetches());
  EXPECT_EQ(established, cdc->SnapshotSessionsEstablishedForTests());
  EXPECT_EQ(lost, cdc->SnapshotSessionsLostForTests());

  // Once the master answers (with no cursor), the start takes a snapshot.
  SetDurableSnapshotCursorFetchStatus(Status::OK());
  GetChangesResponsePB resp;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp, /*max_bytes=*/64));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  EXPECT_EQ(established + 1, cdc->SnapshotSessionsEstablishedForTests());
}

// The master releases the stream (it was deleted) while a snapshot start
// sets up its session, after the session was tracked: the page fails with
// STREAM_NOT_FOUND, and leaves no session and no history pin behind. The same
// for the rebuild of a session from the durable cursor. Before, the page
// created the session again and pinned history at snap_ts for the deleted
// stream, with nothing left to release it.
TEST_F(CDCServiceTest, Snapshot_StreamReleasedDuringSetupLeavesNoSession) {
  InsertTestRowsRemote(0, 10, 10);
  const int64_t streaming_start =
      tablet_replica_->shared_consensus()->GetLastOpId(consensus::COMMITTED_OPID)->index();
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  const std::shared_ptr<tablet::Tablet> tablet = tablet_replica_->shared_tablet();
  for (bool rebuild : {false, true}) {
    SCOPED_TRACE(rebuild ? "rebuild from the durable cursor" : "new snapshot");
    // A stream per case: a released stream stays released.
    const string stream_id = rebuild ? "test-stream-rebuild" : "test-stream-new";
    SeedStreamConfig(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::INITIAL_AND_CONTINUE,
                     stream_id);
    SnapshotCursorUpdate cursor;
    if (rebuild) {
      cursor.snap_ts = tablet_replica_->clock()->Now().value();
      cursor.active = true;
    }
    SetDurableSnapshotCursor(cursor, streaming_start);
    bool hook_ran = false;
    cdc->SetSnapshotSessionTrackedHookForTests([&]() {
      hook_ran = true;
      CHECK(cdc->HasSessionForTests(stream_id, kTabletId));
      CHECK_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                        /*history_safe_time_micros=*/0,
                                        /*barrier_seq=*/0,
                                        /*release_consumer_stream_id=*/stream_id,
                                        /*skip_barrier_update=*/true));
    });
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(kTabletId);
    req.set_is_snapshot_start(true);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(cdc_proxy_->GetChanges(req, &resp, &rpc));
    cdc->SetSnapshotSessionTrackedHookForTests(nullptr);
    ASSERT_TRUE(hook_ran);
    ASSERT_TRUE(resp.has_error()) << resp.ShortDebugString();
    EXPECT_EQ(CDCErrorPB::STREAM_NOT_FOUND, resp.error().code())
        << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
    EXPECT_FALSE(cdc->HasSessionForTests(stream_id, kTabletId));
    EXPECT_EQ(Timestamp(0), tablet->cdc_history_floor());
  }
}

// A snapshot page fails as TABLET_NOT_LEADER if this replica's leadership
// changed during the page (injected here; see
// CDCClientITest.Snapshot_EstablishOnDeposedLeaderLeavesNoSession for a real
// leader change), so the client asks the new leader. A retry then serves the
// page.
TEST_F(CDCServiceTest, Snapshot_PostScanLeadershipLossRejected) {
  InsertTestRowsRemote(0, 10, 10);
  GetChangesResponsePB first;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &first, /*max_bytes=*/64));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  ASSERT_FALSE(first.snapshot_done());
  for (bool is_start : {true, false}) {
    SCOPED_TRACE(is_start ? "start" : "continue");
    const string key = is_start ? "" : first.snapshot_resume_key();
    {
      FLAGS_cdc_inject_post_read_leadership_loss = true;
      SCOPED_CLEANUP({ FLAGS_cdc_inject_post_read_leadership_loss = false; });
      GetChangesResponsePB resp;
      ASSERT_OK(DoSnapshot(kTabletId, is_start, key, &resp, /*max_bytes=*/64));
      ASSERT_TRUE(resp.has_error()) << resp.ShortDebugString();
      EXPECT_EQ(CDCErrorPB::TABLET_NOT_LEADER, resp.error().code())
          << resp.error().DebugString();
      EXPECT_TRUE(resp.error().is_retryable());
      EXPECT_EQ(0, resp.records_size());
    }
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, is_start, key, &resp, /*max_bytes=*/64));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ASSERT_GT(resp.records_size(), 0);
    if (is_start) {
      EXPECT_EQ(first.records(0).DebugString(), resp.records(0).DebugString());
    } else {
      EXPECT_NE(first.records(0).DebugString(), resp.records(0).DebugString());
    }
  }
}

// A checkpoint, or a read, at op index 0 does not acknowledge the last page of
// a snapshot whose streaming start point is 0: it is the position of a client
// that has not streamed yet. Before, it completed the snapshot, and a consumer
// that restarted then did not take the rest of the snapshot.
TEST_F(CDCServiceTest, Snapshot_PositionZeroDoesNotCompleteSnapshot) {
  InsertTestRowsRemote(0, 3, 3);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  // A streaming start point of 0 is reachable only through the transaction
  // fallback of the establish; the session of a durable cursor with a
  // streaming start point of 0 is the same.
  SnapshotCursorUpdate cursor;
  cursor.snap_ts = tablet_replica_->clock()->Now().value();
  cursor.active = true;
  SetDurableSnapshotCursor(cursor, /*streaming_start_op_index=*/0);
  GetChangesResponsePB resp;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_TRUE(resp.snapshot_done());
  ASSERT_EQ(0, resp.snapshot_streaming_start_op_index());

  CheckpointResponsePB cp;
  ASSERT_OK(DoCheckpoint(kTabletId, 0, &cp));
  ASSERT_FALSE(cp.has_error()) << cp.error().DebugString();
  EXPECT_TRUE(cdc->HasActiveSnapshotForTests("test-stream-1", kTabletId))
      << "a checkpoint at 0 completed the snapshot";
  GetChangesResponsePB stream;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &stream));
  ASSERT_FALSE(stream.has_error()) << stream.error().DebugString();
  EXPECT_TRUE(cdc->HasActiveSnapshotForTests("test-stream-1", kTabletId))
      << "a read from 0 completed the snapshot";
  // A position past 0 acknowledges the page.
  ASSERT_GT(stream.checkpoint_op_index(), 0);
  cp.Clear();
  ASSERT_OK(DoCheckpoint(kTabletId, stream.checkpoint_op_index(), &cp));
  ASSERT_FALSE(cp.has_error()) << cp.error().DebugString();
  EXPECT_FALSE(cdc->HasActiveSnapshotForTests("test-stream-1", kTabletId));
}

// E5: a row whose primary key is the lexicographically maximum encoded value
// (here INT32_MAX) must not permanently wedge snapshot pagination. When a page
// ends exactly on that row, its resume key has no strictly-greater successor;
// the server must recognize this and return a done/empty terminal page instead
// of propagating IncrementEncodedKey's "no greater key exists" failure on every
// subsequent resume.
TEST_F(CDCServiceTest, Snapshot_MaxKeyDoesNotWedgePagination) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  // A couple of ordinary rows plus one at the maximum INT32 key, which is the
  // last row the ordered scan visits. int_val is set equal to the key by
  // InsertTestRowsRemote, so INT32_MAX involves no signed overflow.
  InsertTestRowsRemote(0, 3, 3);
  InsertOneRow(std::numeric_limits<int32_t>::max());
  const int kExpectedRows = 4;

  int total_reads = 0;
  int pages = 0;
  bool done = false;
  bool saw_max_key = false;
  string resume_key;
  bool first = true;
  int32_t last_key = std::numeric_limits<int32_t>::min();
  while (!done) {
    GetChangesResponsePB resp;
    // Tiny max_bytes forces one row per page, so a page ends squarely on the
    // INT32_MAX row and the following resume triggers the max-key path.
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/first, resume_key, &resp, 64));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (int i = 0; i < resp.records_size(); ++i) {
      ASSERT_EQ(CDCOpTypePB::READ, resp.records(i).op_type());
      int32_t k = 0;
      ASSERT_TRUE(GetInt32Col(resp.records(i), /*before=*/false, "key", &k));
      EXPECT_GT(k, last_key);  // strictly increasing across the whole scan
      last_key = k;
      if (k == std::numeric_limits<int32_t>::max()) saw_max_key = true;
      total_reads++;
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    pages++;
    ASSERT_LT(pages, 1000) << "snapshot wedged on max-valued key";
  }
  EXPECT_EQ(kExpectedRows, total_reads);
  EXPECT_TRUE(saw_max_key);
  EXPECT_GT(pages, 1);  // actually paginated through the max key
}

// A2: two concurrent is_snapshot_start calls for the same (stream, tablet) must
// establish exactly one snapshot session. Without the per-(stream, tablet)
// start mutex both requests observe "no active session" and both run the
// establish path, racing on snap_ts / streaming_start_op_index. Latency is
// injected into the establish window so the concurrent calls overlap inside it.
TEST_F(CDCServiceTest, Snapshot_ConcurrentStartsEstablishOnce) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 5, 5);

  auto* cdc = mini_server_->server()->cdc_service();
  ASSERT_EQ(0, cdc->SnapshotSessionsEstablishedForTests());

  // Widen the establish window so all concurrent starts overlap within it.
  FLAGS_cdc_inject_latency_before_snapshot_establish_ms = 1500;

  const int kThreads = 4;
  std::vector<Status> rpc_status(kThreads);
  std::vector<GetChangesResponsePB> resps(kThreads);
  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i]() {
      // A tiny page keeps the session active after the first page, so the
      // later starts find it.
      rpc_status[i] = DoSnapshot(kTabletId, /*is_start=*/true, "", &resps[i],
                                 /*max_bytes=*/64);
    });
  }
  for (auto& t : threads) t.join();

  for (int i = 0; i < kThreads; ++i) {
    ASSERT_OK(rpc_status[i]);
    ASSERT_FALSE(resps[i].has_error()) << resps[i].error().DebugString();
    // Every start scans from the beginning of the table, at the same snapshot
    // timestamp.
    ASSERT_GT(resps[i].records_size(), 0);
    int32_t k = -1;
    ASSERT_TRUE(GetInt32Col(resps[i].records(0), /*before=*/false, "key", &k));
    EXPECT_EQ(0, k);
    EXPECT_EQ(resps[0].records(0).timestamp(), resps[i].records(0).timestamp());
  }
  // Exactly one fresh session despite kThreads concurrent starts.
  EXPECT_EQ(1, cdc->SnapshotSessionsEstablishedForTests());
  // The last user of the per-session page lock dropped it.
  EXPECT_EQ(0, cdc->SnapshotPageLockCountForTests());
}

// A3: a snapshot page derives its wait deadline from the client deadline capped
// by --cdc_snapshot_wait_timeout_ms, rather than a hardcoded 30s. With the cap
// set well below the injected establish latency (and below the client's RPC
// deadline), the establish aborts at ~the cap with a retryable error instead of
// blocking a service thread for the full latency.
TEST_F(CDCServiceTest, Snapshot_HonorsDeadlineWhenEstablishSlow) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 5, 5);

  FLAGS_cdc_snapshot_wait_timeout_ms = 500;
  FLAGS_cdc_inject_latency_before_snapshot_establish_ms = 5000;

  const MonoTime begin = MonoTime::Now();
  GetChangesResponsePB resp;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp));  // RPC-level OK
  const MonoDelta elapsed = MonoTime::Now() - begin;

  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code());
  // Aborted near the 500ms cap, nowhere near the 5000ms injected latency.
  EXPECT_LT(elapsed.ToMilliseconds(), 3000)
      << "snapshot ignored the client/flag deadline";
}

// B (safe-deadline ratio): --cdc_read_safe_deadline_ratio reserves a fraction of
// the client's remaining deadline as response-build headroom, so the server
// stops waiting before the client deadline elapses. With the absolute wait cap
// (--cdc_snapshot_wait_timeout_ms) set well above the client deadline, only the
// client-derived deadline binds, so the ratio alone decides the outcome: a small
// ratio leaves enough budget to outlast the injected establish latency and the
// snapshot succeeds; a large ratio shrinks the budget below that latency and the
// establish aborts early with a retryable SERVER_TOO_BUSY.
TEST_F(CDCServiceTest, Snapshot_SafeDeadlineRatioReservesHeadroom) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 5, 5);

  // Absolute wait cap far above the 8s client deadline, so it never binds; the
  // safe-deadline ratio applied to the client deadline is the only thing that
  // can cut the establish wait short.
  FLAGS_cdc_snapshot_wait_timeout_ms = 60000;
  FLAGS_cdc_inject_latency_before_snapshot_establish_ms = 3500;
  const MonoDelta kClientTimeout = MonoDelta::FromSeconds(8);

  // Small ratio: safe budget ~= 8s * 0.90 = 7.2s > 3.5s injected latency, so the
  // establish rides out the latency and the snapshot page is produced.
  FLAGS_cdc_read_safe_deadline_ratio = 0.10;
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp,
                         /*max_bytes=*/0, kClientTimeout));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    EXPECT_GT(resp.records_size(), 0);
    ASSERT_TRUE(resp.snapshot_done());
    // Acknowledge the snapshot, so that the next start establishes a new one.
    GetChangesResponsePB stream;
    ASSERT_OK(DoGetChanges(kTabletId, resp.snapshot_streaming_start_op_index(), &stream));
    ASSERT_FALSE(stream.has_error()) << stream.error().DebugString();
  }

  // Large ratio: safe budget ~= 8s * 0.30 = 2.4s < 3.5s injected latency, so the
  // establish aborts at ~the shrunk deadline with a retryable error, well before
  // the client's 8s deadline would have elapsed.
  FLAGS_cdc_read_safe_deadline_ratio = 0.70;
  {
    const MonoTime begin = MonoTime::Now();
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp,
                         /*max_bytes=*/0, kClientTimeout));  // RPC-level OK
    const MonoDelta elapsed = MonoTime::Now() - begin;
    ASSERT_TRUE(resp.has_error()) << resp.DebugString();
    EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code());
    // Aborted near the ~2.4s shrunk deadline, nowhere near the 8s client
    // deadline -- the reservation, not the client deadline, ended the wait.
    EXPECT_LT(elapsed.ToMilliseconds(), 5000)
        << "safe-deadline ratio did not shrink the establish wait";
  }
}

// B (RPC-worker reservation): --cdc_get_changes_free_rpc_ratio caps the number
// of concurrent GetChanges calls at floor((1 - ratio) * rpc_num_service_threads)
// so a burst of CDC consumers cannot occupy every worker thread in the CDC
// service pool. With the cap forced to 1, a second GetChanges issued while the
// first is still in flight is shed with a retryable SERVER_TOO_BUSY, and the
// slot is released so a later call succeeds again.
TEST_F(CDCServiceTest, Admission_GetChangesRpcWorkerReservation) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 5, 5);

  // Force the cap to 1: floor(1 * (1 - ratio)) is 0 for any ratio > 0, clamped
  // up to the always-admit-one floor. The real service pool still has its full
  // thread count, so call B below is genuinely served concurrently and hits the
  // cap check rather than queueing behind A.
  FLAGS_rpc_num_service_threads = 1;
  FLAGS_cdc_get_changes_free_rpc_ratio = 0.10;

  // Call A holds an in-flight slot for ~2s by sleeping inside the snapshot
  // establish; it must ride the latency out and be served, not shed.
  FLAGS_cdc_inject_latency_before_snapshot_establish_ms = 2000;
  Status a_status;
  GetChangesResponsePB a_resp;
  std::thread a([&]() {
    a_status = DoSnapshot(kTabletId, /*is_start=*/true, "", &a_resp);
  });
  auto join_a = MakeScopedCleanup([&]() { if (a.joinable()) a.join(); });

  // Give A time to increment the in-flight counter and enter the injected sleep.
  SleepFor(MonoDelta::FromMilliseconds(500));

  // Call B arrives while A is still in flight: inflight (2) > cap (1) -> shed.
  GetChangesResponsePB b_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &b_resp));  // RPC-level OK
  ASSERT_TRUE(b_resp.has_error()) << b_resp.DebugString();
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, b_resp.error().code());
  EXPECT_STR_CONTAINS(StatusFromPB(b_resp.error().status()).ToString(),
                      "too many concurrent");

  a.join();
  // A rode out the injected latency and was served (not shed).
  ASSERT_OK(a_status);
  ASSERT_FALSE(a_resp.has_error()) << a_resp.error().DebugString();

  // With A drained the slot is released, so a fresh GetChanges is admitted.
  FLAGS_cdc_inject_latency_before_snapshot_establish_ms = 0;
  GetChangesResponsePB c_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &c_resp));
  EXPECT_FALSE(c_resp.has_error()) << c_resp.error().DebugString();
}

// Isolation: --cdc_snapshot_max_bytes_per_response caps snapshot page size even
// when the request asks for the (large) streaming default, so the initial bulk
// scan is broken into small pages that compete less with user traffic.
TEST_F(CDCServiceTest, Isolation_SnapshotPageCapForcesPagination) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 50, 50);

  // Tiny snapshot cap; the request itself sets no max_bytes (so it would default
  // to the streaming cap (--cdc_max_bytes_per_response) were the snapshot cap not
  // applied).
  FLAGS_cdc_snapshot_max_bytes_per_response = 64;

  GetChangesResponsePB first;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &first, /*max_bytes=*/0));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  // The flag capped the page far below all 50 rows, so the scan is not done.
  EXPECT_FALSE(first.snapshot_done());
  EXPECT_FALSE(first.snapshot_resume_key().empty());

  // Draining the rest still yields exactly the full row set.
  int total_reads = first.records_size();
  string resume_key = first.snapshot_resume_key();
  bool done = false;
  int pages = 1;
  while (!done) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/false, resume_key, &resp, 0));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    total_reads += resp.records_size();
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    ASSERT_LT(++pages, 1000) << "snapshot did not terminate";
  }
  EXPECT_EQ(50, total_reads);
  EXPECT_GT(pages, 1);  // the cap actually forced multiple pages
}

// Isolation: when the CDC scan heap budget is exhausted, a heavy scan is shed
// with a retryable SERVER_TOO_BUSY error, and the slot/budget is released so a
// later request (or one after the budget is raised) succeeds.
TEST_F(CDCServiceTest, Isolation_ScanMemBudgetShedsAndRecovers) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 10, 10);

  // A 1-byte budget cannot admit any reservation.
  FLAGS_cdc_scan_mem_limit_bytes = 1;
  GetChangesResponsePB shed;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &shed));
  ASSERT_TRUE(shed.has_error());
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, shed.error().code());
  EXPECT_EQ(0, shed.records_size());

  // Lifting the budget lets the same request through (the shed request released
  // its slot; nothing is leaked).
  FLAGS_cdc_scan_mem_limit_bytes = 0;
  GetChangesResponsePB ok;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &ok));
  ASSERT_FALSE(ok.has_error()) << ok.error().DebugString();
  EXPECT_GT(ok.records_size(), 0);
}

// R2: a heavy CDC scan must be shed when the *whole server* is over its soft
// memory limit -- the same signal the tablet read/write path uses to shed user
// requests -- not just when the CDC-local budget is exhausted. Otherwise a
// lagging consumer keeps admitting FULL/snapshot scans while the server is
// already OOM-shedding user traffic. The shed is retryable (SERVER_TOO_BUSY),
// increments the reason counter, and releases the slot so the request succeeds
// once pressure clears.
TEST_F(CDCServiceTest, Isolation_ServerMemoryPressureShedsAndRecovers) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 10, 10);

  const int64_t rejected_before =
      CDCCounterValue(METRIC_cdc_scans_rejected_server_memory);

  // Server reports over-soft-limit: the heavy scan is shed before it reserves
  // any heap, with a retryable error, and the reason counter advances.
  FLAGS_cdc_inject_server_memory_pressure = true;
  // Safety net: reset the flag even if an ASSERT below aborts the test, so
  // subsequent tests are not contaminated.
  SCOPED_CLEANUP({ FLAGS_cdc_inject_server_memory_pressure = false; });
  GetChangesResponsePB shed;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &shed));
  ASSERT_TRUE(shed.has_error());
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, shed.error().code());
  // Wire-level retry contract: SERVER_TOO_BUSY is retryable (transient; back off
  // and retry the same request) and is NOT a re-snapshot signal. A consumer that
  // misreads it as needs_resnapshot would discard progress unnecessarily.
  EXPECT_TRUE(shed.error().is_retryable())
      << "SERVER_TOO_BUSY from memory pressure must be flagged retryable";
  EXPECT_FALSE(shed.error().needs_resnapshot())
      << "SERVER_TOO_BUSY from memory pressure must NOT flag needs_resnapshot";
  EXPECT_EQ(0, shed.records_size());
  EXPECT_EQ(1, CDCCounterValue(METRIC_cdc_scans_rejected_server_memory) - rejected_before)
      << "server-memory-pressure shed must increment its reason counter";

  // Pressure clears: the same request succeeds (the shed released its slot so
  // active_scans_ is back at zero -- no concurrency slot was permanently lost).
  FLAGS_cdc_inject_server_memory_pressure = false;
  GetChangesResponsePB ok;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &ok));
  ASSERT_FALSE(ok.has_error()) << ok.error().DebugString();
  EXPECT_GT(ok.records_size(), 0);
}

// R1: the CDC service must be registered with its OWN incoming-RPC queue
// (--cdc_svc_queue_length), separate from the shared --rpc_service_queue_length
// used by the tablet/consensus/admin/tablet-copy services. Otherwise a burst of
// CDC consumers fills the one shared queue and inbound consensus RPCs are
// rejected with SERVER_TOO_BUSY before any CDC admission control runs, turning a
// consumer burst into a cluster-wide consensus-availability event. This fixture
// pins a distinct, non-default queue length before the server starts so the test
// proves the CDC pool got its own depth rather than falling back to the shared
// queue (a silent regression that leaves all other tests still passing).
class CDCServiceDedicatedQueueTest : public CDCServiceTest {
 public:
  void SetUp() override {
    FLAGS_cdc_svc_queue_length = 4242;
    CDCServiceTest::SetUp();
  }
};

TEST_F(CDCServiceDedicatedQueueTest, CDCGetsItsOwnServiceQueue) {
  const RpcServer* rpc = mini_server_->server()->rpc_server();
  ASSERT_NE(nullptr, rpc);

  const rpc::ServicePool* cdc_pool = rpc->service_pool("kudu.cdc.CDCService");
  ASSERT_NE(nullptr, cdc_pool) << "CDC service is not registered";
  EXPECT_EQ(FLAGS_cdc_svc_queue_length,
            static_cast<int32_t>(cdc_pool->queue_length_for_tests()))
      << "CDC service pool must use --cdc_svc_queue_length, not the shared queue";

  // A co-resident service still uses the shared --rpc_service_queue_length,
  // proving CDC got an isolated queue rather than having changed the global one.
  const rpc::ServicePool* ts_pool =
      rpc->service_pool("kudu.tserver.TabletServerService");
  ASSERT_NE(nullptr, ts_pool) << "TabletServerService is not registered";
  EXPECT_EQ(FLAGS_rpc_service_queue_length,
            static_cast<int32_t>(ts_pool->queue_length_for_tests()))
      << "non-CDC services must keep the shared queue length";

  EXPECT_NE(cdc_pool->queue_length_for_tests(), ts_pool->queue_length_for_tests())
      << "CDC must have a dedicated queue depth distinct from the shared one";
}

// Isolation: the heavy-scan admission-control knobs ship enabled with safe
// defaults, not disabled. This guards against a regression to the historical
// "ships unsafe" defaults (concurrency unlimited, no memory budget, 64 MiB
// response cap) that let a burst of FULL-mode / snapshot consumers OOM a
// tablet server. Values are intentionally asserted loosely (enabled + sane
// ordering), not pinned exactly, so they can be retuned without churn.
TEST_F(CDCServiceTest, Isolation_HeavyScanDefaultsShipSafe) {
  // Concurrency of heavy scans is capped (not unlimited).
  EXPECT_GT(FLAGS_cdc_max_concurrent_scans, 0)
      << "CDC heavy-scan concurrency must ship capped, not unlimited";

  // A server-wide heap budget is enforced (not unlimited).
  EXPECT_GT(FLAGS_cdc_scan_mem_limit_bytes, 0)
      << "CDC scan memory budget must ship enabled";

  // The per-response cap is bounded well below the old 64 MiB default.
  EXPECT_GT(FLAGS_cdc_max_bytes_per_response, 0);
  EXPECT_LE(FLAGS_cdc_max_bytes_per_response, 16 * 1024 * 1024)
      << "per-response cap should be small enough to bound per-scan heap";

  // The invariant relied on by the transaction-span escalation logic
  // (span cap >= response cap) holds with the shipped defaults.
  EXPECT_GE(FLAGS_cdc_max_transaction_span_bytes, FLAGS_cdc_max_bytes_per_response);

  // The memory budget leaves room for at least one full concurrency-cap's worth
  // of response-sized reservations, so the concurrency cap (not the budget)
  // is the primary limiter under normal operation.
  EXPECT_GE(FLAGS_cdc_scan_mem_limit_bytes,
            static_cast<int64_t>(FLAGS_cdc_max_concurrent_scans) *
                FLAGS_cdc_max_bytes_per_response);
}

// T15/DR-029: --cdc_master_rpc_timeout_ms ships with a sane default (10 s) and
// controls both tserver->master CDC RPCs. Behavioral testing of the actual RPC
// paths requires a running master; this test verifies the flag default and wiring
// (that the flag is accessible and the default is reasonable).
TEST_F(CDCServiceTest, MasterRpcTimeoutFlag_DefaultAndSanity) {
  // Default is 10000 ms (10 s), preserving pre-flag behavior exactly.
  EXPECT_EQ(10000, FLAGS_cdc_master_rpc_timeout_ms)
      << "cdc_master_rpc_timeout_ms default changed; update DR-029 if intentional";

  // The timeout must be positive (zero would make every RPC time out immediately).
  EXPECT_GT(FLAGS_cdc_master_rpc_timeout_ms, 0)
      << "cdc_master_rpc_timeout_ms must be positive";

  // Changing the flag is reflected immediately on next call (runtime-safe).
  FLAGS_cdc_master_rpc_timeout_ms = 5000;
  EXPECT_EQ(5000, FLAGS_cdc_master_rpc_timeout_ms);
  FLAGS_cdc_master_rpc_timeout_ms = 10000;  // restore
}

// T15/DR-029: --cdc_active_time_report_interval_ms ships at 30 s (lowered from
// 5 min). Asserts that the default is in the expected range: not less than 15 s
// (YB's value; going lower is a separate decision) and not more than 60 s (above
// which expiry jitter becomes significant again).
TEST_F(CDCServiceTest, ActiveTimeIntervalFlag_DefaultLoweredTo30s) {
  // Default is 30 000 ms (30 s).
  EXPECT_EQ(30 * 1000, FLAGS_cdc_active_time_report_interval_ms)
      << "cdc_active_time_report_interval_ms default changed; update DR-029";

  // Must be at least 15 s (no benefit to going below YB's 15 s without data).
  EXPECT_GE(FLAGS_cdc_active_time_report_interval_ms, 15 * 1000)
      << "default below 15 s requires justification (see DR-029)";

  // Must be at most 60 s so expiry jitter stays bounded.
  EXPECT_LE(FLAGS_cdc_active_time_report_interval_ms, 60 * 1000)
      << "default above 60 s reintroduces significant expiry jitter";
}

// Isolation: --cdc_max_concurrent_scans sheds a heavy scan when the cap is full
// and recovers once slots free up. A FULL GetChanges that is held before its
// decode keeps the only scan slot, so a second concurrent FULL GetChanges is
// then shed with SERVER_TOO_BUSY.
TEST_F(CDCServiceTest, Isolation_ConcurrentScanCapShedsAndRecovers) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);      // key=0
  UpdateTestRowRemote(0, 12345);      // needs before/after image reconstruction
  FLAGS_cdc_max_concurrent_scans = 1;
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_EQ(0, cdc->ScanMemTrackerConsumptionForTests());

  FLAGS_cdc_inject_latency_before_decode_ms = 3000;
  Status s1;
  GetChangesResponsePB r1;
  std::thread t1([&]() { s1 = DoGetChanges(kTabletId, 0, &r1); });
  auto join = MakeScopedCleanup([&]() { t1.join(); });
  // Wait until the first scan holds the slot.
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(cdc->ScanMemTrackerConsumptionForTests(), 0);
  });
  GetChangesResponsePB r2;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &r2));
  join.run();
  FLAGS_cdc_inject_latency_before_decode_ms = 0;
  ASSERT_OK(s1);
  ASSERT_FALSE(r1.has_error()) << r1.error().DebugString();
  ASSERT_TRUE(r2.has_error()) << r2.DebugString();
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, r2.error().code()) << r2.DebugString();

  // The slot is free again: a fresh scan is admitted.
  GetChangesResponsePB after;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &after));
  ASSERT_FALSE(after.has_error()) << after.error().DebugString();
}

// Phase 6.3: a request with need_schema_info gets a synthetic DDL record with
// the current schema prepended, without changing the checkpoint.
TEST_F(CDCServiceTest, NeedSchemaInfo_PrependsCurrentSchema) {
  InsertTestRowsRemote(0, 3, 3);

  // Baseline call without the flag to capture the checkpoint.
  GetChangesResponsePB base;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &base));
  ASSERT_FALSE(base.has_error()) << base.error().DebugString();

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp, /*max_bytes=*/0,
                         /*need_schema_info=*/true));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  // First record is the synthetic schema DDL, carrying the full schema.
  ASSERT_GT(resp.records_size(), 0);
  const CDCRecordPB& first = resp.records(0);
  EXPECT_EQ(CDCOpTypePB::DDL, first.op_type());
  EXPECT_TRUE(first.has_new_schema());
  EXPECT_GT(first.new_schema().columns_size(), 0);
  EXPECT_TRUE(first.has_new_schema_version());

  // The synthetic record must not advance the checkpoint: it matches the
  // baseline call, and it reuses from_op_index (0 here).
  EXPECT_EQ(base.checkpoint_op_index(), resp.checkpoint_op_index());
  EXPECT_EQ(0, first.op_index());

  // Even when caught up, the schema is still returned.
  GetChangesResponsePB caught_up;
  ASSERT_OK(DoGetChanges(kTabletId, resp.checkpoint_op_index(), &caught_up,
                         /*max_bytes=*/0, /*need_schema_info=*/true));
  ASSERT_FALSE(caught_up.has_error()) << caught_up.error().DebugString();
  ASSERT_GT(caught_up.records_size(), 0);
  EXPECT_EQ(CDCOpTypePB::DDL, caught_up.records(0).op_type());
}

// Phase 7.2: the lag/retention gauges reflect an active consumer session.
TEST_F(CDCServiceTest, Metrics_TrackConsumerActivity) {
  auto* cdc = mini_server_->server()->cdc_service();
  // No sessions before any GetChanges.
  EXPECT_EQ(0, cdc->ActiveStreamCount());

  InsertTestRowsRemote(0, 4, 4);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  // A session now exists, its activity is recent, and the newest record's
  // physical time yields a non-negative sent lag.
  EXPECT_EQ(1, cdc->ActiveStreamCount());
  EXPECT_GE(cdc->MaxActiveAgeMicros(), 0);
  EXPECT_GE(cdc->MaxSentLagMicros(), 0);
}

// A GetChanges session publishes a dedicated per-(stream, tablet) metric entity
// carrying stream_id/tablet_id attributes and per-stream lag/age gauges, so lag
// is attributable to a specific stream (not just the server-wide max).
TEST_F(CDCServiceTest, Metrics_PerStreamGauges) {
  InsertTestRowsRemote(0, 4, 4);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  MetricRegistry* registry = mini_server_->server()->metric_registry();
  ASSERT_NE(nullptr, registry);

  // The entity, its attributes, and both gauges are present in the registry.
  std::ostringstream out;
  JsonWriter w(&out, JsonWriter::COMPACT);
  ASSERT_OK(registry->WriteAsJson(&w, MetricJsonOptions()));
  const string json = out.str();
  ASSERT_STR_CONTAINS(json, "cdc_stream");
  ASSERT_STR_CONTAINS(json, "test-stream-1");
  ASSERT_STR_CONTAINS(json, kTabletId);
  ASSERT_STR_CONTAINS(json, "cdc_stream_sent_lag_micros");
  ASSERT_STR_CONTAINS(json, "cdc_stream_active_age_micros");

  // The per-stream active-age gauge grows with wall-clock time while the
  // consumer is idle (no further activity refreshes last-active).
  const string entity_id = strings::Substitute("$0-$1", "test-stream-1", kTabletId);
  auto entity = registry->FindOrCreateEntity(
      &METRIC_ENTITY_cdc_stream, entity_id,
      {{"stream_id", "test-stream-1"}, {"tablet_id", kTabletId}});
  scoped_refptr<Metric> m = entity->FindOrNull(METRIC_cdc_stream_active_age_micros);
  ASSERT_NE(nullptr, m.get()) << "per-stream active-age gauge not instantiated";
  auto* age_gauge = down_cast<FunctionGauge<int64_t>*>(m.get());
  const int64_t age1 = age_gauge->value();
  ASSERT_GE(age1, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  const int64_t age2 = age_gauge->value();
  ASSERT_GT(age2, age1);
}

// O1 (DR-023): cdc_bytes_sent increments by the serialized response size on
// each successful GetChanges call so operators can detect byte-throughput
// growth, large-row storms, and bandwidth saturation. The assertion ties the
// counter value to the actual response bytes -- not just "non-zero" -- by
// comparing it with the response's own ByteSizeLong().
TEST_F(CDCServiceTest, Metrics_BytesSentCounter) {
  // Counter starts at 0 before any GetChanges.
  ASSERT_EQ(0, CDCCounterValue(METRIC_cdc_bytes_sent));

  // Insert rows so the response carries real CHANGE records.
  InsertTestRowsRemote(0, 4, 4);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_GT(resp.records_size(), 0) << "need records for a non-trivial byte count";

  // The counter must have advanced by exactly the response's serialized byte
  // size -- this ties the metric to real bytes, not just a fixed increment.
  const int64_t expected_bytes = static_cast<int64_t>(resp.ByteSizeLong());
  ASSERT_GT(expected_bytes, 0);
  ASSERT_EQ(expected_bytes, CDCCounterValue(METRIC_cdc_bytes_sent))
      << "bytes_sent must equal the serialized response byte size";

  // A second call accumulates: counter grows by the second response's bytes.
  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(kTabletId, resp.checkpoint_op_index(), &resp2));
  ASSERT_FALSE(resp2.has_error()) << resp2.error().DebugString();
  const int64_t expected_total =
      expected_bytes + static_cast<int64_t>(resp2.ByteSizeLong());
  ASSERT_EQ(expected_total, CDCCounterValue(METRIC_cdc_bytes_sent))
      << "bytes_sent must accumulate across calls";
}

// O2 (DR-023): cdc_stream_time_to_expiry_micros is a per-stream countdown
// gauge: near the full threshold immediately after activity, clamping to 0
// once the idle window elapses, and returning INT64_MAX when expiry is
// disabled. Proves non-vacuous by asserting distinct values before/after the
// expiry window and for the disabled case.
TEST_F(CDCServiceTest, Metrics_TimeToExpiryGauge) {
  // Use a short threshold (300 ms) so the test completes quickly.
  const int64_t expiry_ms = 300;
  FLAGS_cdc_stream_idle_expiry_ms = expiry_ms;

  InsertTestRowsRemote(0, 2, 2);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  // Retrieve the per-stream time-to-expiry gauge from the metric registry.
  MetricRegistry* registry = mini_server_->server()->metric_registry();
  ASSERT_NE(nullptr, registry);
  const string entity_id = strings::Substitute("$0-$1", "test-stream-1", kTabletId);
  auto entity = registry->FindOrCreateEntity(
      &METRIC_ENTITY_cdc_stream, entity_id,
      {{"stream_id", "test-stream-1"}, {"tablet_id", kTabletId}});
  scoped_refptr<Metric> m = entity->FindOrNull(METRIC_cdc_stream_time_to_expiry_micros);
  ASSERT_NE(nullptr, m.get()) << "time-to-expiry gauge not registered";
  auto* gauge = down_cast<FunctionGauge<int64_t>*>(m.get());

  // Right after activity the remaining budget must be positive and at most the
  // full threshold (it decreases from there as wall-clock time passes).
  const int64_t v1 = gauge->value();
  ASSERT_GT(v1, 0) << "gauge must be positive immediately after activity";
  ASSERT_LE(v1, expiry_ms * 1000) << "gauge must not exceed the configured threshold";

  // Sleep past the expiry window; the gauge must clamp to 0.
  std::this_thread::sleep_for(std::chrono::milliseconds(expiry_ms + 100));
  const int64_t v2 = gauge->value();
  ASSERT_EQ(0, v2) << "gauge must be 0 once the idle window has elapsed";

  // Prove non-vacuity: v1 and v2 differ (the gauge computed distinct values).
  ASSERT_GT(v1, v2) << "gauge must decrease as the stream goes idle";

  // When expiry is disabled (flag = 0), the gauge returns INT64_MAX so alert
  // rules can distinguish "no expiry" from "stream about to expire".
  FLAGS_cdc_stream_idle_expiry_ms = 0;
  const int64_t v3 = gauge->value();
  ASSERT_EQ(std::numeric_limits<int64_t>::max(), v3)
      << "gauge must return INT64_MAX when expiry is disabled";
}

// O3 Part A (DR-026): cdc_get_changes_response_bytes histogram records one
// observation per record-bearing GetChanges response and its recorded values
// must be consistent with the actual serialized byte sizes. Empty/heartbeat
// polls (records_size() == 0) must NOT increment the histogram so the
// distribution is not polluted by idle-poll noise.
//
// Non-vacuity proof:
// 1. TotalCount before any call is 0.
// 2. TotalCount after one record-bearing call is exactly 1.
// 3. The histogram's max bucket covers the actual response byte size.
// 4. TotalCount does not change after a verified empty poll (records_size==0).
//    This assertion would fail if the records_size > 0 guard were removed.
TEST_F(CDCServiceTest, Metrics_ResponseBytesHistogram) {
  auto hist = CDCHistogram(METRIC_cdc_get_changes_response_bytes);
  ASSERT_EQ(0UL, hist->TotalCount())
      << "histogram must start empty before any GetChanges call";

  // Insert rows so the first GetChanges response carries real CHANGE records.
  InsertTestRowsRemote(0, 4, 4);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_GT(resp.records_size(), 0)
      << "test requires a record-bearing response; adjust row count if this fires";

  // Exactly one record-bearing response -> TotalCount == 1.
  ASSERT_EQ(1UL, hist->TotalCount())
      << "histogram must record exactly one observation for the record-bearing response";

  // The histogram max bucket must be >= the actual serialized byte size and
  // the min bucket must be <= it. Since only one observation was recorded,
  // min == max == the actual payload size (to the histogram's 1-significant-digit
  // precision).
  const uint64_t bytes = static_cast<uint64_t>(resp.ByteSizeLong());
  ASSERT_GT(bytes, 0UL);
  ASSERT_GE(hist->MaxValueForTests(), bytes)
      << "histogram max value must be >= actual response bytes";
  ASSERT_LE(hist->MinValueForTests(), bytes)
      << "histogram min value must be <= actual response bytes";

  // Drive an empty poll (no new rows; consumer is fully caught up).
  // The response must have records_size() == 0 -- this is the condition the
  // guard protects against.
  GetChangesResponsePB resp_empty;
  ASSERT_OK(DoGetChanges(kTabletId, resp.checkpoint_op_index(), &resp_empty));
  ASSERT_FALSE(resp_empty.has_error()) << resp_empty.error().DebugString();
  ASSERT_EQ(0, resp_empty.records_size())
      << "expected empty response after consuming all records; guard test requires "
         "records_size()==0";

  // The empty poll must NOT have incremented the histogram.
  ASSERT_EQ(1UL, hist->TotalCount())
      << "empty poll must not increment histogram (records_size > 0 guard)";
}

// T11 R5 (DR-024): inject an already-expired deadline into
// ReadReplicatesInRange; the per-entry deadline check must fire after the first
// WAL entry and return a partial batch signaled by have_more_records=true.
// Verifies that the deadline path mirrors the byte-cap "at least one op" rule.
// NOTE: the "at least one op" guarantee is at the WAL-entry level, not the
// CDC-record level. The first WAL entry is a Raft config change and produces
// zero CDCRecords; that is expected. What matters is that the response is OK
// (not SERVER_TOO_BUSY) and have_more_records signals the truncation so the
// consumer re-polls.
TEST_F(CDCServiceTest, Deadline_WalReadDeadlineTruncatesPartialBatch) {
  InsertTestRowsRemote(0, 5, 5);

  // Ensure the inject flag is reset even if an ASSERT aborts the test.
  SCOPED_CLEANUP({ FLAGS_cdc_inject_wal_read_deadline_expired = false; });
  FLAGS_cdc_inject_wal_read_deadline_expired = true;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  // The deadline fired inside ReadReplicatesInRange, not at the escalation-loop
  // level, so the response must be OK (no error), not SERVER_TOO_BUSY.
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  // The batch was truncated by the deadline before reaching up_to_op_index;
  // have_more_records must be set so the consumer re-polls immediately.
  EXPECT_TRUE(resp.have_more_records())
      << "have_more_records must be true when a deadline truncated the batch";
}

// Like the above, but the first op of the window is a WRITE_OP, whose outcome
// must be read from its COMMIT under the same expired deadline. The COMMIT
// lookup also always covers the first op, so the poll still emits that op and
// advances instead of failing with SERVER_TOO_BUSY on every retry.
TEST_F(CDCServiceTest, Deadline_WalReadDeadlineStillEmitsFirstWriteOp) {
  InsertTestRowsRemote(0, 5, 5);
  GetChangesResponsePB all;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &all));
  ASSERT_FALSE(all.has_error()) << all.error().DebugString();
  ASSERT_EQ(5, all.records_size()) << all.DebugString();
  const int64_t first_write_index = all.records(0).op_index();

  SCOPED_CLEANUP({ FLAGS_cdc_inject_wal_read_deadline_expired = false; });
  FLAGS_cdc_inject_wal_read_deadline_expired = true;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, first_write_index - 1, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
  EXPECT_EQ(CDCOpTypePB::INSERT, resp.records(0).op_type());
  EXPECT_EQ(first_write_index, resp.records(0).op_index());
  EXPECT_EQ(first_write_index, resp.checkpoint_op_index());
  EXPECT_TRUE(resp.have_more_records());
}

// The REPLICATE read fits in the call's time budget but the COMMIT lookup runs
// out of it after the first op. The batch ends after that op, but the ops after
// it are committed and only their lookup was cut short: the poll reports more
// records, and the stream is not reported caught up.
TEST_F(CDCServiceTest, Deadline_CommitLookupDeadlineReportsMoreRecords) {
  auto* cdc = mini_server_->server()->cdc_service();
  InsertTestRowsRemote(0, 5, 5);
  GetChangesResponsePB all;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &all));
  ASSERT_FALSE(all.has_error()) << all.error().DebugString();
  ASSERT_EQ(5, all.records_size()) << all.DebugString();
  const int64_t first_write_index = all.records(0).op_index();

  SCOPED_CLEANUP({ FLAGS_cdc_inject_commit_lookup_deadline_expired = false; });
  FLAGS_cdc_inject_commit_lookup_deadline_expired = true;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, first_write_index - 1, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(1, resp.records_size()) << resp.DebugString();
  EXPECT_EQ(first_write_index, resp.records(0).op_index());
  EXPECT_EQ(first_write_index, resp.checkpoint_op_index());
  EXPECT_TRUE(resp.have_more_records());
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, resp.checkpoint_op_index(), &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();
  EXPECT_EQ(all.checkpoint_op_index() - first_write_index, cdc->MaxOpsBehind());

  FLAGS_cdc_inject_commit_lookup_deadline_expired = false;
  ASSERT_OK(DoGetChanges(kTabletId, resp.checkpoint_op_index(), &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  EXPECT_EQ(4, resp.records_size()) << resp.DebugString();
  EXPECT_FALSE(resp.have_more_records());
}

// A transaction at the head of the read window is streamed whole even if the
// COMMIT lookup is cut by the deadline after the first op: the lookup covers
// every op up to the transaction's FINALIZE_COMMIT. Before, the batch ended at
// the transaction's second write, so the transaction stayed open, the
// checkpoint stayed at the op before its first write, and every poll asked for
// an immediate re-poll that returned the same nothing.
TEST_F(CDCServiceTest, Deadline_CommitLookupDeadlineStillEmitsTxnAtHead) {
  for (const auto record_type : { CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL }) {
    SCOPED_TRACE(CDCStreamConfigPB::RecordType_Name(record_type));
    SeedStreamConfig(record_type);
    const int64_t base = CaughtUpCheckpoint();
    const int32_t k = record_type == CDCStreamConfigPB::CHANGE ? 100 : 200;
    const int64_t txn_id = k;
    ASSERT_OK(DoParticipantOp(txn_id, ParticipantOpPB::BEGIN_TXN));
    for (int32_t key = k; key < k + 3; ++key) {
      NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { key }, txn_id));
    }
    ASSERT_OK(CommitTxn(txn_id));
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { k + 50 }));

    vector<CDCOpTypePB> ops;
    vector<int32_t> keys;
    NO_FATALS(PollWithCommitLookupCut(base, &ops, &keys));
    EXPECT_EQ(vector<CDCOpTypePB>({ CDCOpTypePB::BEGIN, CDCOpTypePB::INSERT,
                                    CDCOpTypePB::INSERT, CDCOpTypePB::INSERT,
                                    CDCOpTypePB::COMMIT, CDCOpTypePB::INSERT }),
              ops);
    EXPECT_EQ(vector<int32_t>({ k, k + 1, k + 2, k + 50 }), keys);
  }
}

// Likewise for a transaction at the head of the window that is aborted: the
// stream moves past it.
TEST_F(CDCServiceTest, Deadline_CommitLookupDeadlineMovesPastAbortedTxnAtHead) {
  const int64_t base = CaughtUpCheckpoint();
  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 100 }, kTxnId));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 101 }, kTxnId));
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::ABORT_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 200 }));

  vector<CDCOpTypePB> ops;
  vector<int32_t> keys;
  NO_FATALS(PollWithCommitLookupCut(base, &ops, &keys));
  EXPECT_EQ(vector<CDCOpTypePB>({ CDCOpTypePB::INSERT }), ops);
  EXPECT_EQ(vector<int32_t>({ 200 }), keys);
}

// While the transaction at the head of the window is open, a poll whose COMMIT
// lookup is cut by the deadline cannot stream anything. It must not ask for an
// immediate re-poll, which would return the same nothing. Once the transaction
// commits, it is streamed.
TEST_F(CDCServiceTest, Deadline_CommitLookupDeadlineOpenTxnAtHeadDoesNotSpin) {
  const int64_t base = CaughtUpCheckpoint();
  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 100 }, kTxnId));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 101 }, kTxnId));

  vector<CDCOpTypePB> ops;
  vector<int32_t> keys;
  NO_FATALS(PollWithCommitLookupCut(base, &ops, &keys));
  EXPECT_TRUE(ops.empty());

  ASSERT_OK(CommitTxn(kTxnId));
  NO_FATALS(PollWithCommitLookupCut(base, &ops, &keys));
  EXPECT_EQ(vector<CDCOpTypePB>({ CDCOpTypePB::BEGIN, CDCOpTypePB::INSERT,
                                  CDCOpTypePB::INSERT, CDCOpTypePB::COMMIT }),
            ops);
  EXPECT_EQ(vector<int32_t>({ 100, 101 }), keys);
}

// T11 R6 (DR-024): inject the escalation-loop deadline check; the while(true)
// loop must bail out with a retryable SERVER_TOO_BUSY before issuing any WAL
// read. This validates the R6 guard against a deadline-overrun escalation loop
// monopolizing the RPC worker thread.
TEST_F(CDCServiceTest, Deadline_EscalationDeadlineReturnsToBusy) {
  InsertTestRowsRemote(0, 5, 5);

  SCOPED_CLEANUP({ FLAGS_cdc_inject_escalation_deadline_expired = false; });
  FLAGS_cdc_inject_escalation_deadline_expired = true;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code());
  EXPECT_TRUE(resp.error().is_retryable())
      << "escalation-loop deadline must produce a retryable error";
  EXPECT_FALSE(resp.error().needs_resnapshot())
      << "escalation-loop deadline must NOT flag needs_resnapshot";
  EXPECT_EQ(0, resp.records_size());
}

// T11 R3 (DR-024): when --cdc_wal_read_mem_limit_bytes is 0 (default), CHANGE-
// mode reads proceed without any shed; when the cap is set to 1 byte, the next
// read is shed with SERVER_TOO_BUSY (retryable) and the rejection counter
// advances. After the cap is raised, reads succeed again.
TEST_F(CDCServiceTest, WalReadMemory_CapShedsAndDefaultUnlimited) {
  InsertTestRowsRemote(0, 5, 5);

  const int64_t rejected_before =
      CDCCounterValue(METRIC_cdc_wal_reads_rejected_memory);

  // Default 0 = unlimited: CHANGE-mode read must succeed.
  ASSERT_EQ(0, FLAGS_cdc_wal_read_mem_limit_bytes)
      << "default must be 0 (unlimited)";
  GetChangesResponsePB ok;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &ok));
  ASSERT_FALSE(ok.has_error()) << ok.error().DebugString();
  EXPECT_GE(ok.records_size(), 1);
  EXPECT_EQ(rejected_before, CDCCounterValue(METRIC_cdc_wal_reads_rejected_memory))
      << "unlimited mode must not increment rejection counter";

  // Cap = 1 byte: the next CHANGE-mode read is shed before any WAL I/O.
  SCOPED_CLEANUP({ FLAGS_cdc_wal_read_mem_limit_bytes = 0; });
  FLAGS_cdc_wal_read_mem_limit_bytes = 1;
  GetChangesResponsePB shed;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &shed));
  ASSERT_TRUE(shed.has_error());
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, shed.error().code());
  EXPECT_TRUE(shed.error().is_retryable())
      << "WAL-read memory shed must be flagged retryable";
  EXPECT_FALSE(shed.error().needs_resnapshot())
      << "WAL-read memory shed must NOT flag needs_resnapshot";
  EXPECT_EQ(0, shed.records_size());
  EXPECT_EQ(1, CDCCounterValue(METRIC_cdc_wal_reads_rejected_memory) - rejected_before)
      << "WAL-read memory shed must increment wal_reads_rejected_memory counter";

  // Restore: cap raised back to 0 (unlimited), reads succeed again.
  FLAGS_cdc_wal_read_mem_limit_bytes = 0;
  GetChangesResponsePB recover;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &recover));
  ASSERT_FALSE(recover.has_error()) << recover.error().DebugString();
  EXPECT_GE(recover.records_size(), 1);
}

// ---------------------------------------------------------------------------
// T12 R4 tests: cdc_max_records_per_response (record-count cap)
// ---------------------------------------------------------------------------

// R4 (T12/DR-025): GetChanges in CHANGE mode must return at most
// cdc_max_records_per_response records per call. When truncation fires:
//   * have_more_records is set to true.
//   * checkpoint_op_index is set to the last EMITTED group's index, not the
//     end of the WAL window, so the next call from that checkpoint picks up
//     exactly where this one stopped.
TEST_F(CDCServiceTest, RecordCountCap_Changes_RespectsCap) {
  // Insert 10 rows in 10 separate batches so each row is its own emit group.
  InsertTestRowsRemote(0, 10, 10);

  // Uncapped: get all records and the final checkpoint.
  GetChangesResponsePB full;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &full));
  ASSERT_FALSE(full.has_error()) << full.error().DebugString();
  const int total_records = full.records_size();
  // At least 10 INSERT records must be present.
  ASSERT_GE(total_records, 10);

  // Set cap smaller than the total but > 0 so truncation fires.
  SCOPED_CLEANUP({ FLAGS_cdc_max_records_per_response = 4096; });
  FLAGS_cdc_max_records_per_response = 3;

  // First capped call from the beginning.
  GetChangesResponsePB first;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &first));
  ASSERT_FALSE(first.has_error()) << first.error().DebugString();
  EXPECT_LE(first.records_size(), 3)
      << "capped response must not exceed the cap";
  EXPECT_GE(first.records_size(), 1)
      << "must return at least one record (forward progress)";
  EXPECT_TRUE(first.have_more_records())
      << "have_more_records must be true when count cap truncates";
  EXPECT_GT(first.checkpoint_op_index(), 0)
      << "checkpoint must advance past 0";
  EXPECT_LT(first.checkpoint_op_index(), full.checkpoint_op_index())
      << "partial checkpoint must be before the full checkpoint";

  // Second call from the partial checkpoint must return at least one more
  // record (forward progress).
  GetChangesResponsePB second;
  ASSERT_OK(DoGetChanges(kTabletId, first.checkpoint_op_index(), &second));
  ASSERT_FALSE(second.has_error()) << second.error().DebugString();
  EXPECT_GE(second.records_size(), 1)
      << "second page must return at least one record";

  // The capped pages, read to the end, have the records of the uncapped
  // response, each once and in order.
  vector<int64_t> expected;
  for (const auto& r : full.records()) {
    expected.push_back(r.op_index());
  }
  vector<int64_t> paged;
  int64_t from = 0;
  for (int i = 0; i < 100 && from < full.checkpoint_op_index(); i++) {
    GetChangesResponsePB page;
    ASSERT_OK(DoGetChanges(kTabletId, from, &page));
    ASSERT_FALSE(page.has_error()) << page.error().DebugString();
    ASSERT_LE(page.records_size(), 3);
    for (const auto& r : page.records()) {
      paged.push_back(r.op_index());
    }
    ASSERT_GT(page.checkpoint_op_index(), from) << "no progress";
    from = page.checkpoint_op_index();
  }
  EXPECT_EQ(full.checkpoint_op_index(), from);
  EXPECT_EQ(expected, paged);
}

// R4 (T12/DR-025): a write op whose single emit group contains more records
// than the cap must still be emitted whole -- the group is never split.
// Strategy: get a baseline checkpoint, then insert N rows in one batch
// (one emit group), then set cap < N and verify all N INSERT records come back.
TEST_F(CDCServiceTest, RecordCountCap_Changes_WriteOpNotSplit) {
  // Get baseline checkpoint past any initial bootstrap ops.
  GetChangesResponsePB base;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &base));
  ASSERT_FALSE(base.has_error()) << base.error().DebugString();
  const int64_t base_cp = base.checkpoint_op_index();

  // Insert 6 rows in one batch: one WAL entry, one emit group, 6 CDCRecords.
  InsertTestRowsRemote(0, 6, /*num_batches=*/1);

  // Cap = 4 < 6: the group exceeds the cap but must be emitted whole because
  // it is the first group in the window (any_group_emitted guard).
  SCOPED_CLEANUP({ FLAGS_cdc_max_records_per_response = 4096; });
  FLAGS_cdc_max_records_per_response = 4;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, base_cp, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  int insert_count = 0;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::INSERT) {
      insert_count++;
    }
  }
  EXPECT_EQ(6, insert_count)
      << "all 6 rows from the single batch must be returned without splitting";
}

// A transaction whose BEGIN, rows and COMMIT together exceed the record cap is
// emitted whole.
TEST_F(CDCServiceTest, RecordCountCap_Changes_TransactionNotSplit) {
  const int64_t base = CaughtUpCheckpoint();
  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  for (int32_t first : { 100, 102, 104 }) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(first, 2), kTxnId));
  }
  ASSERT_OK(CommitTxn(kTxnId));
  FLAGS_cdc_max_records_per_response = 4;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, base, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  vector<CDCOpTypePB> ops;
  for (const auto& r : resp.records()) {
    ops.push_back(r.op_type());
  }
  vector<CDCOpTypePB> expected(8, CDCOpTypePB::INSERT);
  expected.front() = CDCOpTypePB::BEGIN;
  expected.back() = CDCOpTypePB::COMMIT;
  EXPECT_EQ(expected, ops) << resp.DebugString();
}

// An ALTER takes no partition lock, so it can land between a transaction's
// first write and its commit. When the record cap ends the response right after
// the ALTER, the checkpoint must not move past the transaction's writes: before,
// it was set to the ALTER's index, and the next call emitted the transaction as
// an empty BEGIN/COMMIT.
TEST_F(CDCServiceTest, TxnRecordCap_InterleavedNonTxnDoesNotLoseRows) {
  const int64_t base = CaughtUpCheckpoint();
  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(100, 3), kTxnId));
  AlterAddColumn(schema_, "extra", tablet_replica_->tablet_metadata()->schema_version() + 1);
  ASSERT_OK(CommitTxn(kTxnId));

  FLAGS_cdc_max_records_per_response = 1;
  vector<int32_t> keys;
  NO_FATALS(StreamInsertKeys(base, /*max_bytes=*/0, &keys));
  EXPECT_EQ(Keys(100, 3), keys);
}

// Without the partition lock, two transactions can be open on a tablet at once.
// A transaction that commits while another one that wrote after it is still
// open is held back, and the checkpoint must stay before its writes too, not
// only before the open transaction's. Before, the held-back transaction was
// later emitted without its rows.
TEST_F(CDCServiceTest, TwoInterleavedTxns_NoRowLoss) {
  FLAGS_enable_txn_partition_lock = false;
  const int64_t base = CaughtUpCheckpoint();
  ASSERT_OK(DoParticipantOp(1, ParticipantOpPB::BEGIN_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 200 }, /*txn_id=*/1));
  ASSERT_OK(DoParticipantOp(2, ParticipantOpPB::BEGIN_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 300 }, /*txn_id=*/2));
  ASSERT_OK(CommitTxn(1));

  // Transaction 1 committed after transaction 2 wrote, so it waits for it.
  vector<int32_t> keys;
  int64_t checkpoint = -1;
  NO_FATALS(StreamInsertKeys(base, /*max_bytes=*/0, &keys, &checkpoint));
  EXPECT_TRUE(keys.empty());

  ASSERT_OK(CommitTxn(2));
  NO_FATALS(StreamInsertKeys(checkpoint, /*max_bytes=*/0, &keys));
  EXPECT_EQ((vector<int32_t>{ 200, 300 }), keys);
}

// Pins the contract of GetChangesResponsePB.checkpoint_op_index and the order
// of the records (cdc.proto), which sinks dedup and resume by. A caught-up
// response without records carries the last committed op, not -1. An open
// transaction keeps the checkpoint below its first write, although the call
// read past it. Once the transaction commits, its rows come after the changes
// that became visible before its commit, and keep the op index of their
// write, which is below the op index of those changes.
TEST_F(CDCServiceTest, CheckpointOpIndexSemantics_CaughtUpAndOpenTxn) {
  // Without the partition lock, a write outside the transaction can land
  // between its first write and its commit.
  FLAGS_enable_txn_partition_lock = false;
  const auto last_committed_index = [&]() {
    return tablet_replica_->consensus()->GetLastOpId(consensus::COMMITTED_OPID)->index();
  };
  const int64_t base = CaughtUpCheckpoint();
  ASSERT_EQ(last_committed_index(), base);
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, base, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
    EXPECT_EQ(base, resp.checkpoint_op_index());
  }

  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 100 }, kTxnId));
  const int64_t txn_write_index = last_committed_index();
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 1 }));
  const int64_t write_index = last_committed_index();
  ASSERT_GT(write_index, txn_write_index);
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, base, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size()) << resp.DebugString();
    EXPECT_EQ(txn_write_index - 1, resp.checkpoint_op_index());
  }

  ASSERT_OK(CommitTxn(kTxnId));
  const int64_t commit_index = last_committed_index();
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, txn_write_index - 1, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(4, resp.records_size()) << resp.DebugString();
  const auto& write = resp.records(0);
  const auto& begin = resp.records(1);
  const auto& txn_row = resp.records(2);
  const auto& commit = resp.records(3);
  EXPECT_EQ(CDCOpTypePB::INSERT, write.op_type());
  EXPECT_EQ(write_index, write.op_index());
  EXPECT_FALSE(write.has_commit_timestamp());
  EXPECT_EQ(CDCOpTypePB::BEGIN, begin.op_type());
  EXPECT_EQ(commit_index, begin.op_index());
  EXPECT_EQ(CDCOpTypePB::INSERT, txn_row.op_type());
  EXPECT_EQ(txn_write_index, txn_row.op_index());
  EXPECT_LT(txn_row.op_index(), write.op_index());
  EXPECT_EQ(begin.commit_timestamp(), txn_row.commit_timestamp());
  EXPECT_EQ(CDCOpTypePB::COMMIT, commit.op_type());
  EXPECT_EQ(commit_index, commit.op_index());
  EXPECT_EQ(commit_index, resp.checkpoint_op_index());
}

// An open transaction whose first write is followed by more WAL than the
// transaction span cap must not fail the stream: it has not committed, so
// reading further cannot emit it. Before, every call escalated the read to the
// span cap and then failed with TRANSACTION_TOO_LARGE. Only without the
// partition lock can other writes land inside an open transaction's span.
TEST_F(CDCServiceTest, OpenTxn_PaddedByNonTxnTraffic_DoesNotFailStream) {
  FLAGS_enable_txn_partition_lock = false;
  FLAGS_cdc_max_transaction_span_bytes = 8 * 1024;
  const int64_t base = CaughtUpCheckpoint();
  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { 100 }, kTxnId));
  for (int32_t key = 1000; key < 1040; ++key) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { key }, /*txn_id=*/-1, /*value_size=*/512));
  }

  vector<int32_t> keys;
  int64_t checkpoint = -1;
  for (int i = 0; i < 3; ++i) {
    NO_FATALS(StreamInsertKeys(base, /*max_bytes=*/1024, &keys, &checkpoint));
  }
  // Everything after the transaction's first write waits for the transaction.
  EXPECT_TRUE(keys.empty());
  EXPECT_GT(checkpoint, base);

  // Once the transaction ends, the stream moves on. The span of the aborted
  // transaction includes the other writes, so the span cap must cover them.
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::ABORT_TXN));
  FLAGS_cdc_max_transaction_span_bytes = 64 * 1024 * 1024;
  NO_FATALS(StreamInsertKeys(checkpoint, /*max_bytes=*/1024, &keys));
  EXPECT_EQ(Keys(1000, 40), keys);
}

// While the transaction that pins the checkpoint is open, a call does not
// escalate its read window: the commit is not in the WAL, so a larger window
// cannot reach it. Before, every call re-read the transaction's whole span.
TEST_F(CDCServiceTest, EscalationSkippedWhileTxnOpen) {
  const int64_t base = CaughtUpCheckpoint();
  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  for (int32_t key = 100; key < 120; ++key) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { key }, kTxnId, /*value_size=*/256));
  }
  const int64_t escalations = CDCCounterValue(METRIC_cdc_wal_read_escalations);

  vector<int32_t> keys;
  int64_t checkpoint = -1;
  NO_FATALS(StreamInsertKeys(base, /*max_bytes=*/1024, &keys, &checkpoint));
  for (int i = 0; i < 3; ++i) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp, /*max_bytes=*/1024));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
    EXPECT_EQ(checkpoint, resp.checkpoint_op_index());
    EXPECT_FALSE(resp.have_more_records());
  }
  EXPECT_TRUE(keys.empty());
  EXPECT_EQ(escalations, CDCCounterValue(METRIC_cdc_wal_read_escalations));

  // After the commit, the window is escalated to reach it.
  ASSERT_OK(CommitTxn(kTxnId));
  NO_FATALS(StreamInsertKeys(checkpoint, /*max_bytes=*/1024, &keys));
  EXPECT_EQ(Keys(100, 20), keys);
  EXPECT_GT(CDCCounterValue(METRIC_cdc_wal_read_escalations), escalations);
}

// An escalated read is charged to the WAL-read memory budget. A transaction
// whose span needs a larger read than the whole budget fails loudly; before,
// the escalated reads were not charged at all.
TEST_F(CDCServiceTest, Escalation_ChargesWalReadMemLimit) {
  const int64_t base = CaughtUpCheckpoint();
  FLAGS_cdc_wal_read_mem_limit_bytes = 8 * 1024;
  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  for (int32_t key = 100; key < 160; ++key) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { key }, kTxnId, /*value_size=*/256));
  }
  ASSERT_OK(CommitTxn(kTxnId));
  auto* cdc = mini_server_->server()->cdc_service();

  int64_t from = base;
  GetChangesResponsePB resp;
  for (int i = 0; i < 10; ++i) {
    resp.Clear();
    ASSERT_OK(DoGetChanges(kTabletId, from, &resp, /*max_bytes=*/1024));
    if (resp.has_error() || resp.checkpoint_op_index() <= from) {
      break;
    }
    from = resp.checkpoint_op_index();
  }
  ASSERT_TRUE(resp.has_error()) << resp.DebugString();
  EXPECT_EQ(CDCErrorPB::TRANSACTION_TOO_LARGE, resp.error().code());
  EXPECT_STR_CONTAINS(resp.error().status().message(), "--cdc_wal_read_mem_limit_bytes");
  EXPECT_EQ(0, cdc->WalReadMemTrackerConsumptionForTests());

  // With a large enough budget the transaction is streamed.
  FLAGS_cdc_wal_read_mem_limit_bytes = 1024 * 1024;
  vector<int32_t> keys;
  NO_FATALS(StreamInsertKeys(from, /*max_bytes=*/1024, &keys));
  EXPECT_EQ(Keys(100, 60), keys);
  EXPECT_EQ(0, cdc->WalReadMemTrackerConsumptionForTests());
}

// A GetChanges request can ask for less than --cdc_max_bytes_per_response, but
// not for more, and asking for 0 or less means the server default. Before, 0 or
// less meant an unlimited WAL read.
TEST_F(CDCServiceTest, GetChanges_MaxBytesZeroOrNegativeIsClamped) {
  FLAGS_cdc_max_bytes_per_response = 4 * 1024;
  const int64_t base = CaughtUpCheckpoint();
  for (int32_t key = 0; key < 40; ++key) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { key }, /*txn_id=*/-1, /*value_size=*/512));
  }
  for (int64_t max_bytes : { int64_t{0}, int64_t{-1}, std::numeric_limits<int64_t>::max() }) {
    SCOPED_TRACE(max_bytes);
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(base);
    req.set_max_bytes(max_bytes);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(cdc_proxy_->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    EXPECT_TRUE(resp.have_more_records());
    EXPECT_GT(resp.records_size(), 0);
    EXPECT_LT(resp.records_size(), 40);
    // At most the cap plus one record.
    EXPECT_LE(resp.ByteSizeLong(), 4 * 1024 + 1024);
  }
}

// A negative max_bytes is not charged to the WAL-read budget as a negative
// reservation. Before, each such call lowered the budget's consumption for good.
TEST_F(CDCServiceTest, WalReadTracker_NegativeMaxBytesDoesNotDrift) {
  FLAGS_cdc_wal_read_mem_limit_bytes = 64 * 1024 * 1024;
  InsertTestRowsRemote(0, 3, 3);
  auto* cdc = mini_server_->server()->cdc_service();
  for (int i = 0; i < 3; ++i) {
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(0);
    req.set_max_bytes(-1);
    GetChangesResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(cdc_proxy_->GetChanges(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
  EXPECT_EQ(0, cdc->WalReadMemTrackerConsumptionForTests());
}

// The records of a response are bounded by max_bytes even when they are much
// larger than the WAL they come from, as FULL images of narrow updates of wide
// rows are. The first group is always returned. Before, only the WAL read was
// bounded.
TEST_F(CDCServiceTest, FullMode_ResponseBoundedByMaxBytes) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  for (int32_t key = 0; key < 30; ++key) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { key }, /*txn_id=*/-1,
                         /*value_size=*/2048));
  }
  const int64_t base = CaughtUpCheckpoint();
  // Each update changes int_val only: little WAL, large before/after images.
  for (int32_t key = 0; key < 30; ++key) {
    NO_FATALS(SubmitRows(RowOperationsPB::UPDATE, { key }, /*txn_id=*/-1, /*value_size=*/0,
                         /*int_val_delta=*/1));
  }

  constexpr int64_t kMaxBytes = 16 * 1024;
  int64_t from = base;
  vector<int32_t> updated;
  for (int i = 0; i < 100; ++i) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, from, &resp, kMaxBytes));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    size_t largest = 0;
    for (const auto& r : resp.records()) {
      largest = std::max(largest, r.ByteSizeLong());
      int32_t k = -1;
      if (r.op_type() == CDCOpTypePB::UPDATE) {
        ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k)) << r.DebugString();
        updated.push_back(k);
      }
    }
    EXPECT_LE(resp.ByteSizeLong(), kMaxBytes + largest + 64);
    if (resp.checkpoint_op_index() <= from && !resp.have_more_records()) {
      break;
    }
    from = std::max(from, resp.checkpoint_op_index());
  }
  EXPECT_EQ(Keys(0, 30), updated);
}

// A call stops decoding once it has the records of a full response: ops
// whose records it cannot carry are not decoded (nor, in FULL mode, their
// images reconstructed) just to be dropped and decoded again by the next call.
// Before, every call decoded its whole read window, so with records several
// times larger than their WAL entries each op was decoded several times.
TEST_F(CDCServiceTest, FullMode_DecodesEachOpAboutOnce) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  for (int32_t key = 0; key < 30; ++key) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, { key }, /*txn_id=*/-1,
                         /*value_size=*/2048));
  }
  const int64_t base = CaughtUpCheckpoint();
  // Each update changes int_val only: little WAL, large before/after images.
  for (int32_t key = 0; key < 30; ++key) {
    NO_FATALS(SubmitRows(RowOperationsPB::UPDATE, { key }, /*txn_id=*/-1, /*value_size=*/0,
                         /*int_val_delta=*/1));
  }

  const int64_t decoded_before = CDCCounterValue(METRIC_cdc_write_ops_decoded);
  int64_t from = base;
  int num_calls = 0;
  vector<int32_t> updated;
  for (; num_calls < 100; ++num_calls) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, from, &resp, /*max_bytes=*/16 * 1024));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    for (const auto& r : resp.records()) {
      int32_t k = -1;
      if (r.op_type() == CDCOpTypePB::UPDATE) {
        ASSERT_TRUE(GetInt32Col(r, /*before=*/false, "key", &k)) << r.DebugString();
        updated.push_back(k);
      }
    }
    if (resp.checkpoint_op_index() <= from && !resp.have_more_records()) {
      break;
    }
    from = std::max(from, resp.checkpoint_op_index());
  }
  EXPECT_EQ(Keys(0, 30), updated);
  ASSERT_GT(num_calls, 3);
  // Each call decodes at most one op whose records it does not send.
  EXPECT_LE(CDCCounterValue(METRIC_cdc_write_ops_decoded) - decoded_before,
            30 + num_calls);
}

// --rpc_max_message_size is 1 MiB for the server and the client.
class CDCServiceSmallRpcTest : public CDCServiceTest {
 public:
  void SetUp() override {
    FLAGS_rpc_max_message_size = 1024 * 1024;
    CDCServiceTest::SetUp();
  }

 protected:
  // Follows the checkpoint from 'from_op_index' until a call fails with a CDC
  // error, which it returns in 'resp'.
  void StreamUntilError(int64_t from_op_index, GetChangesResponsePB* resp) {
    for (int i = 0; i < 100; ++i) {
      resp->Clear();
      ASSERT_OK(DoGetChanges(kTabletId, from_op_index, resp));
      if (resp->has_error()) {
        return;
      }
      ASSERT_GT(resp->checkpoint_op_index(), from_op_index) << resp->DebugString();
      from_op_index = resp->checkpoint_op_index();
    }
    FAIL() << "no error";
  }
};

// A transaction that does not fit in one response fails loudly with
// TRANSACTION_TOO_LARGE. Before, the server sent the response anyway, the
// client rejected it, and the consumer retried the same call forever.
TEST_F(CDCServiceSmallRpcTest, TransactionLargerThanRpcMaxMessageSize) {
  const int64_t base = CaughtUpCheckpoint();
  constexpr int64_t kTxnId = 1;
  ASSERT_OK(DoParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN));
  for (int32_t first = 0; first < 1500; first += 100) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(first, 100), kTxnId,
                         /*value_size=*/1024));
  }
  ASSERT_OK(CommitTxn(kTxnId));

  GetChangesResponsePB resp;
  NO_FATALS(StreamUntilError(base, &resp));
  EXPECT_EQ(CDCErrorPB::TRANSACTION_TOO_LARGE, resp.error().code());
  EXPECT_FALSE(resp.error().is_retryable());
  EXPECT_FALSE(resp.error().needs_resnapshot());
  EXPECT_STR_CONTAINS(resp.error().status().message(), "--rpc_max_message_size");
  EXPECT_EQ(0, resp.records_size());
}

// The same holds for the records of a single write op, which cannot be split
// across responses either.
TEST_F(CDCServiceSmallRpcTest, SingleHugeWriteOpResponseBounded) {
  const int64_t base = CaughtUpCheckpoint();
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(0, 1200), /*txn_id=*/-1,
                       /*value_size=*/1024));

  GetChangesResponsePB resp;
  NO_FATALS(StreamUntilError(base, &resp));
  EXPECT_EQ(CDCErrorPB::RESPONSE_TOO_LARGE, resp.error().code());
  EXPECT_FALSE(resp.error().is_retryable());
  EXPECT_FALSE(resp.error().needs_resnapshot());
  EXPECT_STR_CONTAINS(resp.error().status().message(), "--rpc_max_message_size");
  EXPECT_EQ(0, resp.records_size());
}

// In FULL mode a small op can produce large records: here, an update of one
// narrow column of many wide rows.
TEST_F(CDCServiceSmallRpcTest, SingleHugeWriteOpResponseBounded_Full) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  for (int32_t first = 0; first < 600; first += 100) {
    NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(first, 100), /*txn_id=*/-1,
                         /*value_size=*/1024));
  }
  const int64_t base = CaughtUpCheckpoint();
  NO_FATALS(SubmitRows(RowOperationsPB::UPDATE, Keys(0, 600), /*txn_id=*/-1,
                       /*value_size=*/0, /*int_val_delta=*/1));

  GetChangesResponsePB resp;
  NO_FATALS(StreamUntilError(base, &resp));
  EXPECT_EQ(CDCErrorPB::RESPONSE_TOO_LARGE, resp.error().code());
  EXPECT_EQ(0, resp.records_size());
}

// The plan of which emit groups one GetChanges response carries, and its
// checkpoint. Groups are {effective_index, min_index, num_records, num_bytes}.
namespace {
constexpr int64_t kNoOpenTxn = std::numeric_limits<int64_t>::max();

ChangesEmitPlan Plan(const vector<ChangesEmitGroupInfo>& groups, int64_t open_min_index,
                     int64_t end_index, int32_t max_records = 0,
                     int64_t max_bytes = 1 << 20, int64_t hard_max_bytes = 1 << 20) {
  ChangesEmitLimits limits;
  limits.max_records = max_records;
  limits.max_bytes = max_bytes;
  limits.hard_max_bytes = hard_max_bytes;
  return PlanChangesEmission(groups, open_min_index, end_index, /*base_records=*/0,
                             /*base_bytes=*/0, limits);
}
} // anonymous namespace

TEST(CDCEmitPlanTest, AllGroupsFit) {
  const auto plan = Plan({ { 5, 5, 1, 100 }, { 6, 6, 1, 100 } }, kNoOpenTxn, 8);
  EXPECT_EQ(2, plan.num_groups);
  EXPECT_EQ(8, plan.checkpoint_op_index);
  EXPECT_FALSE(plan.truncated);
  EXPECT_FALSE(plan.too_large);
}

TEST(CDCEmitPlanTest, CapBetweenGroups) {
  // Record cap.
  auto plan = Plan({ { 5, 5, 1, 100 }, { 6, 6, 1, 100 } }, kNoOpenTxn, 8, /*max_records=*/1);
  EXPECT_EQ(1, plan.num_groups);
  EXPECT_EQ(5, plan.checkpoint_op_index);
  EXPECT_TRUE(plan.truncated);
  // Byte cap.
  plan = Plan({ { 5, 5, 1, 100 }, { 6, 6, 1, 100 }, { 7, 7, 1, 100 } }, kNoOpenTxn, 8,
              /*max_records=*/0, /*max_bytes=*/250);
  EXPECT_EQ(2, plan.num_groups);
  EXPECT_EQ(6, plan.checkpoint_op_index);
  EXPECT_TRUE(plan.truncated);
}

TEST(CDCEmitPlanTest, FirstGroupAlwaysEmittedUnlessOverHardLimit) {
  auto plan = Plan({ { 5, 5, 10, 100 }, { 6, 6, 1, 10 } }, kNoOpenTxn, 6,
                   /*max_records=*/1, /*max_bytes=*/50, /*hard_max_bytes=*/200);
  EXPECT_EQ(1, plan.num_groups);
  EXPECT_EQ(5, plan.checkpoint_op_index);
  EXPECT_TRUE(plan.truncated);
  EXPECT_FALSE(plan.too_large);

  plan = Plan({ { 5, 5, 10, 300 } }, kNoOpenTxn, 6, /*max_records=*/0, /*max_bytes=*/50,
              /*hard_max_bytes=*/200);
  EXPECT_TRUE(plan.too_large);
  EXPECT_EQ(1, plan.num_groups);
}

TEST(CDCEmitPlanTest, AlterInsideTxnSpanWithCap) {
  // A transaction writes at 5 and commits at 9; an ALTER lands at 7. Ending
  // the response after the ALTER would need a checkpoint >= 7 and < 5, so the
  // transaction is emitted with it.
  const auto plan = Plan({ { 7, 7, 1, 100 }, { 9, 5, 5, 100 } }, kNoOpenTxn, 9,
                         /*max_records=*/1);
  EXPECT_EQ(2, plan.num_groups);
  EXPECT_EQ(9, plan.checkpoint_op_index);
  EXPECT_FALSE(plan.truncated);
  EXPECT_FALSE(plan.too_large);

  // Over the hard limit, the forced extension fails loudly.
  const auto too_large = Plan({ { 7, 7, 1, 100 }, { 9, 5, 5, 100 } }, kNoOpenTxn, 9,
                              /*max_records=*/1, /*max_bytes=*/150, /*hard_max_bytes=*/150);
  EXPECT_TRUE(too_large.too_large);
  EXPECT_EQ(2, too_large.num_groups);
}

TEST(CDCEmitPlanTest, ShrinkToValidPrefix) {
  // The cap allows A and B, but B is inside the span of T (writes at 5):
  // emitting B would need a checkpoint >= 7, which skips T's write. A alone
  // can be emitted.
  const auto plan = Plan({ { 3, 3, 1, 10 }, { 7, 7, 1, 10 }, { 9, 5, 3, 10 }, { 10, 10, 1, 10 } },
                         kNoOpenTxn, 10, /*max_records=*/2);
  EXPECT_EQ(1, plan.num_groups);
  EXPECT_EQ(3, plan.checkpoint_op_index);
  EXPECT_TRUE(plan.truncated);
}

TEST(CDCEmitPlanTest, OpenTxnAtWindowEnd) {
  // The transaction open since 6 holds the checkpoint at 5.
  const auto plan = Plan({ { 3, 3, 1, 10 }, { 4, 4, 1, 10 } }, /*open_min_index=*/6, 8);
  EXPECT_EQ(2, plan.num_groups);
  EXPECT_EQ(5, plan.checkpoint_op_index);
  EXPECT_FALSE(plan.truncated);
}

TEST(CDCEmitPlanTest, InterleavedTxns) {
  // T1 writes at 5 and commits at 7, T2 writes at 6 and is open. T1 waits for
  // T2, and the checkpoint stays before T1's write.
  auto plan = Plan({ { 7, 5, 3, 10 } }, /*open_min_index=*/6, 7);
  EXPECT_EQ(0, plan.num_groups);
  EXPECT_EQ(4, plan.checkpoint_op_index);
  EXPECT_FALSE(plan.truncated);

  // Same, with a write at 6 and T2 open since 7: the write at 6 is held back as
  // well, since it is inside T1's span.
  plan = Plan({ { 6, 6, 1, 10 }, { 8, 5, 3, 10 } }, /*open_min_index=*/7, 8);
  EXPECT_EQ(0, plan.num_groups);
  EXPECT_EQ(4, plan.checkpoint_op_index);
  EXPECT_FALSE(plan.truncated);
}

// R4 (T12/DR-025): cdc_max_records_per_response = 0 means unlimited; every
// record from the inserted rows must be returned without truncation.
TEST_F(CDCServiceTest, RecordCountCap_ZeroMeansUnlimited) {
  InsertTestRowsRemote(0, 10, 10);

  // Default is 4096 so it would not truncate 10 rows anyway, but set it to 0
  // explicitly to test the "unlimited" code path.
  SCOPED_CLEANUP({ FLAGS_cdc_max_records_per_response = 4096; });
  FLAGS_cdc_max_records_per_response = 0;

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  int insert_count = 0;
  for (int i = 0; i < resp.records_size(); ++i) {
    if (resp.records(i).op_type() == CDCOpTypePB::INSERT) {
      insert_count++;
    }
  }
  EXPECT_EQ(10, insert_count)
      << "cap=0 (unlimited) must return all records";
  EXPECT_FALSE(resp.have_more_records())
      << "cap=0 must not set have_more_records when all records fit";
}

// R4 (T12/DR-025): snapshot ReadSnapshot path also enforces the count cap.
// Each page must contain at most cdc_max_records_per_response records, and the
// full snapshot must eventually return all inserted rows across pages.
TEST_F(CDCServiceTest, RecordCountCap_Snapshot_RespectsCap) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  // Insert 10 rows for the snapshot to return.
  InsertTestRowsRemote(0, 10, 10);

  // Set cap = 3 so we exercise multi-page snapshot behavior.
  SCOPED_CLEANUP({ FLAGS_cdc_max_records_per_response = 4096; });
  FLAGS_cdc_max_records_per_response = 3;

  int total_reads = 0;
  int pages = 0;
  bool done = false;
  string resume_key;
  bool first = true;
  while (!done) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/first, resume_key, &resp,
                         /*max_bytes=*/0));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    // Every page must respect the cap (except the last which may be short).
    EXPECT_LE(resp.records_size(), 3)
        << "snapshot page " << pages << " exceeds the record count cap";
    for (int i = 0; i < resp.records_size(); ++i) {
      EXPECT_EQ(CDCOpTypePB::READ, resp.records(i).op_type());
      total_reads++;
    }
    done = resp.snapshot_done();
    resume_key = resp.snapshot_resume_key();
    ++pages;
    ASSERT_LT(pages, 200) << "snapshot did not terminate";
  }
  EXPECT_EQ(10, total_reads)
      << "all 10 rows must be returned across all snapshot pages";
  EXPECT_GT(pages, 1) << "snapshot must span multiple pages with cap=3";
}

// ---------------------------------------------------------------------------
// T12 R7 tests: decoded-heap MemTracker true-up
// ---------------------------------------------------------------------------

// R7 (T12/DR-025): after a CHANGE-mode GetChanges, the wal_read_mem_tracker_
// reservation must be trued-up to the actual response size and released back
// to zero. Enables the limit by setting cdc_wal_read_mem_limit_bytes large
// enough to not shed, then verifies the tracker returns to baseline.
TEST_F(CDCServiceTest, MemTrackerTrueUp_WalReadTrackerReleasedAfterGetChanges) {
  InsertTestRowsRemote(0, 5, 5);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();

  // Baseline: tracker must be at 0 before the call.
  ASSERT_EQ(0, cdc->WalReadMemTrackerConsumptionForTests())
      << "wal_read_mem_tracker_ must be at 0 before the call";

  // Enable the limit (large enough to admit the call but non-zero so the
  // tracker is actually exercised by the reservation path).
  SCOPED_CLEANUP({ FLAGS_cdc_wal_read_mem_limit_bytes = 0; });
  FLAGS_cdc_wal_read_mem_limit_bytes = 64 * 1024 * 1024;  // 64 MiB

  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_GE(resp.records_size(), 1);

  // After the call the tracker must be back at 0: the true-up and scoped
  // cleanup together must have released the full reservation.
  EXPECT_EQ(0, cdc->WalReadMemTrackerConsumptionForTests())
      << "wal_read_mem_tracker_ must return to 0 after GetChanges";
}

// R7 (T12/DR-025): after a snapshot GetChanges, the scan_mem_tracker_
// reservation must be trued-up to the actual response size and released back
// to zero by the scoped cleanup.
TEST_F(CDCServiceTest, MemTrackerTrueUp_ScanTrackerReleasedAfterSnapshot) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 5, 5);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();

  // Baseline: tracker must be at 0 before the call.
  ASSERT_EQ(0, cdc->ScanMemTrackerConsumptionForTests())
      << "scan_mem_tracker_ must be at 0 before the snapshot call";

  GetChangesResponsePB resp;
  ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp));
  // Whether the snapshot is done in one page or paginates, check after the
  // first page: the scan slot must have been released.
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  // After the call (scoped cleanup has fired), the tracker must be at 0.
  EXPECT_EQ(0, cdc->ScanMemTrackerConsumptionForTests())
      << "scan_mem_tracker_ must return to 0 after snapshot GetChanges";
}

// O3 Part B (DR-028): cdc_stream_committed_lag_micros reports -1 before any
// consumer sends last_applied_phystime_micros, and a non-negative lag once a
// Checkpoint carries the field. Proves the -1 sentinel semantics, the
// positive-after-report transition, and that the clamping to 0 (no negative
// lags) holds when the reported time is in the future (clock skew).
TEST_F(CDCServiceTest, Metrics_CommittedLagGauge) {
  // Establish a consumer session via GetChanges. The rows are written in two
  // ops and read one op at a time, so the consumer is not caught up: a
  // caught-up consumer has no committed lag (see
  // Metrics_LagIsZeroWhenCaughtUpOnIdleTablet).
  InsertTestRowsRemote(0, 2, 2);
  GetChangesResponsePB gc_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &gc_resp, /*max_bytes=*/1));
  ASSERT_FALSE(gc_resp.has_error()) << gc_resp.error().DebugString();
  ASSERT_GT(gc_resp.checkpoint_op_index(), 0);
  ASSERT_TRUE(gc_resp.have_more_records());

  // Look up the per-stream metric entity and the committed-lag gauge.
  MetricRegistry* registry = mini_server_->server()->metric_registry();
  ASSERT_NE(nullptr, registry);
  const string entity_id = strings::Substitute("$0-$1", "test-stream-1", kTabletId);
  auto entity = registry->FindOrCreateEntity(
      &METRIC_ENTITY_cdc_stream, entity_id,
      {{"stream_id", "test-stream-1"}, {"tablet_id", kTabletId}});
  scoped_refptr<Metric> m = entity->FindOrNull(METRIC_cdc_stream_committed_lag_micros);
  ASSERT_NE(nullptr, m.get()) << "committed-lag gauge not registered";
  auto* gauge = down_cast<FunctionGauge<int64_t>*>(m.get());

  // Before any Checkpoint with last_applied_phystime_micros, the gauge must
  // report -1 (consumer has not reported an applied timestamp yet).
  ASSERT_EQ(-1, gauge->value())
      << "gauge must be -1 before any applied timestamp is reported";

  // Send a Checkpoint with last_applied_phystime_micros set to a known value
  // that is safely in the past so the lag is non-negative.
  const int64_t applied_ts = GetCurrentTimeMicros() - 1000000;  // 1 second ago

  // Build and send the Checkpoint request directly via the proxy (bypassing
  // DoCheckpoint which does not expose the new field).
  CheckpointRequestPB req;
  req.set_stream_id("test-stream-1");
  req.set_tablet_id(kTabletId);
  req.set_op_index(gc_resp.checkpoint_op_index());
  req.set_last_applied_phystime_micros(applied_ts);
  CheckpointResponsePB cp_resp;
  rpc::RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(cdc_proxy_->Checkpoint(req, &cp_resp, &rpc));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();

  // After the Checkpoint the gauge must report a non-negative lag.
  const int64_t lag = gauge->value();
  ASSERT_GE(lag, 0)
      << "gauge must be >= 0 after a valid applied timestamp is reported";
  // The lag must be at least as large as the gap we set (1 second = 1,000,000 us)
  // minus a generous wall-clock tolerance for test execution time.
  ASSERT_GE(lag, 900000)
      << "gauge lag must be >= ~1 second (the applied_ts was 1 second in the past)";

  // Sending a future timestamp must clamp to 0 (no negative lag).
  const int64_t future_ts = GetCurrentTimeMicros() + 10000000;  // 10 seconds ahead
  CheckpointRequestPB req2;
  req2.set_stream_id("test-stream-1");
  req2.set_tablet_id(kTabletId);
  req2.set_op_index(gc_resp.checkpoint_op_index());
  req2.set_last_applied_phystime_micros(future_ts);
  CheckpointResponsePB cp_resp2;
  rpc::RpcController rpc2;
  rpc2.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(cdc_proxy_->Checkpoint(req2, &cp_resp2, &rpc2));
  ASSERT_FALSE(cp_resp2.has_error()) << cp_resp2.error().DebugString();
  ASSERT_EQ(0, gauge->value())
      << "gauge must clamp to 0 when applied_ts is in the future (clock skew)";
}

// A consumer that has read and checkpointed everything an idle tablet
// committed is not lagging, however long ago the last write was: the sent and
// committed lag gauges report 0, and grow again once it falls behind.
TEST_F(CDCServiceTest, Metrics_LagIsZeroWhenCaughtUpOnIdleTablet) {
  auto* cdc = mini_server_->server()->cdc_service();
  InsertTestRowsRemote(0, 2, 2);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_GT(resp.records_size(), 0);
  ASSERT_FALSE(resp.have_more_records());
  const int64_t caught_up_index = resp.checkpoint_op_index();

  CheckpointRequestPB req;
  req.set_stream_id("test-stream-1");
  req.set_tablet_id(kTabletId);
  req.set_op_index(caught_up_index);
  req.set_last_applied_phystime_micros(GetCurrentTimeMicros() - 1000000);
  CheckpointResponsePB cp_resp;
  RpcController rpc;
  ASSERT_OK(cdc_proxy_->Checkpoint(req, &cp_resp, &rpc));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();

  MetricRegistry* registry = mini_server_->server()->metric_registry();
  auto entity = registry->FindOrCreateEntity(
      &METRIC_ENTITY_cdc_stream, Substitute("$0-$1", "test-stream-1", kTabletId),
      {{"stream_id", "test-stream-1"}, {"tablet_id", kTabletId}});
  const auto gauge_value = [&](const GaugePrototype<int64_t>& proto) {
    scoped_refptr<Metric> m = entity->FindOrNull(proto);
    CHECK(m);
    return down_cast<FunctionGauge<int64_t>*>(m.get())->value();
  };
  // The tablet stays idle; the newest record sent gets older and older.
  SleepFor(MonoDelta::FromMilliseconds(50));
  EXPECT_EQ(0, gauge_value(METRIC_cdc_stream_sent_lag_micros));
  EXPECT_EQ(0, gauge_value(METRIC_cdc_stream_committed_lag_micros));
  EXPECT_EQ(0, cdc->MaxSentLagMicros());
  EXPECT_EQ(0, cdc->MaxCommittedLagMicros());

  // New writes, of which the consumer reads only the first op: it lags again.
  InsertTestRowsRemote(100, 2, 2);
  ASSERT_OK(DoGetChanges(kTabletId, caught_up_index, &resp, /*max_bytes=*/1));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_TRUE(resp.have_more_records());
  SleepFor(MonoDelta::FromMilliseconds(10));
  EXPECT_GT(gauge_value(METRIC_cdc_stream_sent_lag_micros), 0);
  EXPECT_GE(gauge_value(METRIC_cdc_stream_committed_lag_micros), 1000000);
  EXPECT_GT(cdc->MaxSentLagMicros(), 0);
}

// A session ended and created again for the same (stream, tablet) gets new
// per-session gauges that track the new session, not the frozen gauges of the
// old one.
TEST_F(CDCServiceTest, Metrics_RecreatedSessionGaugesTrackNewState) {
  auto* cdc = mini_server_->server()->cdc_service();
  InsertTestRowsRemote(0, 2, 2);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, resp.checkpoint_op_index(), &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();

  MetricRegistry* registry = mini_server_->server()->metric_registry();
  const auto ops_behind = [&]() {
    auto entity = registry->FindOrCreateEntity(
        &METRIC_ENTITY_cdc_stream, Substitute("$0-$1", "test-stream-1", kTabletId),
        {{"stream_id", "test-stream-1"}, {"tablet_id", kTabletId}});
    scoped_refptr<Metric> m = entity->FindOrNull(METRIC_cdc_stream_ops_behind);
    CHECK(m);
    return down_cast<FunctionGauge<int64_t>*>(m.get())->value();
  };
  ASSERT_EQ(0, ops_behind());

  // End the session, then create it again with a read from the beginning: the
  // new session's checkpoint is 0 while the tablet has committed ops.
  cdc->EndSessionsForTablet(kTabletId);
  ASSERT_EQ(0, cdc->ActiveStreamCount());
  InsertTestRowsRemote(100, 2, 2);
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(1, cdc->ActiveStreamCount());
  EXPECT_EQ(resp.checkpoint_op_index(), ops_behind());
  EXPECT_EQ(resp.checkpoint_op_index(), cdc->MaxOpsBehind());
}

// The CDC flags reject values outside their documented ranges, at runtime as
// well as at startup.
TEST_F(CDCServiceTest, FlagValidatorsRejectBadValues) {
  for (const char* flag : { "cdc_max_concurrent_scans", "cdc_scan_mem_limit_bytes",
                            "cdc_wal_read_mem_limit_bytes", "cdc_max_records_per_response",
                            "cdc_stream_idle_expiry_ms", "cdc_stream_config_cache_ttl_ms",
                            "cdc_checkpoint_flush_interval_ms" }) {
    SCOPED_TRACE(flag);
    EXPECT_TRUE(google::SetCommandLineOption(flag, "-1").empty());
    EXPECT_FALSE(google::SetCommandLineOption(flag, "0").empty());
  }
  for (const char* flag : { "cdc_max_bytes_per_response", "cdc_master_rpc_timeout_ms",
                            "cdc_full_apply_wait_timeout_ms",
                            "cdc_snapshot_wait_timeout_ms" }) {
    SCOPED_TRACE(flag);
    EXPECT_TRUE(google::SetCommandLineOption(flag, "0").empty());
  }

  // The span cap must not be smaller than the response cap.
  const auto& validators = GetFlagValidators();
  const auto* response_size = FindOrNull(validators, "cdc_response_size_flags");
  ASSERT_NE(nullptr, response_size);
  ASSERT_TRUE((*response_size)());
  FLAGS_cdc_max_transaction_span_bytes = FLAGS_cdc_max_bytes_per_response - 1;
  EXPECT_FALSE((*response_size)());
}


// Phase 7.1: with enforcement enabled, CDC RPCs require a valid authz token
// granting SCAN privilege on the target table.
// Shutting the tablet server down while a DeleteTablet is in flight must not
// call into a destroyed CDC service: DeleteTablet releases the tablet's CDC
// anchors, and the delete pool is drained only by the tablet manager shutdown,
// after the RPC services have been unregistered.
TEST_F(CDCServiceTest, ShutdownDuringDeleteTabletWithCdc) {
  InsertTestRowsRemote(0, 3, 3);
  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_TRUE(cdc->HasSessionForTests("test-stream-1", kTabletId));
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1));

  FLAGS_delete_tablet_inject_latency_after_transition_ms = 3000;
  CountDownLatch deleted(1);
  Status delete_status;
  mini_server_->server()->tablet_manager()->DeleteTabletAsync(
      kTabletId, tablet::TABLET_DATA_TOMBSTONED, std::nullopt,
      [&](const Status& s, tserver::TabletServerErrorPB::Code /*code*/) {
        delete_status = s;
        deleted.CountDown();
      });
  // Let the delete enter its injected latency, then shut down underneath it.
  SleepFor(MonoDelta::FromMilliseconds(500));
  NO_FATALS(ShutdownTablet());
  ASSERT_TRUE(deleted.WaitFor(MonoDelta::FromSeconds(30)));
  ASSERT_OK(delete_status);
}

class CDCAuthzTest : public CDCServiceTest {
 public:
  void SetUp() override {
    // The CDC data path enforces fine-grained authorization along with the
    // rest of the tablet server.
    FLAGS_tserver_enforce_access_control = true;
    NO_FATALS(CDCServiceTest::SetUp());

    rpc::UserCredentials user;
    user.set_real_user(kUser);
    cdc_proxy_->set_user_credentials(user);

    // Set up a token signer whose public key the tablet server trusts.
    TokenSigningPrivateKeyPB tsk;
    PrivateKey private_key;
    CHECK_OK(security::GeneratePrivateKey(512, &private_key));
    string key_der;
    CHECK_OK(private_key.ToString(&key_der, security::DataFormat::DER));
    tsk.set_rsa_key_der(key_der);
    tsk.set_key_seq_num(1);
    tsk.set_expire_unix_epoch_seconds(WallTime_Now() + 3600);

    auto verifier(std::make_shared<TokenVerifier>());
    signer_.reset(new TokenSigner(3600, 3600, 3600, verifier));
    ASSERT_OK(signer_->ImportKeys({ tsk }));
    vector<TokenSigningPublicKeyPB> public_keys = verifier->ExportKeys();
    ASSERT_OK(mini_server_->server()->mutable_token_verifier()->ImportKeys(public_keys));

    // Discover the table id backing the tablet, needed for the token.
    scoped_refptr<tablet::TabletReplica> replica;
    ASSERT_OK(mini_server_->server()->tablet_manager()->GetTabletReplica(
        kTabletId, &replica));
    table_id_ = replica->tablet_metadata()->table_id();
  }

 protected:
  // Mints a token for kUser granting (or not) SCAN on 'table_id'.
  SignedTokenPB MakeToken(const string& table_id, bool scan_privilege) {
    TablePrivilegePB privilege;
    privilege.set_table_id(table_id);
    if (scan_privilege) {
      privilege.set_scan_privilege(true);
    }
    SignedTokenPB token;
    CHECK_OK(signer_->GenerateAuthzToken(kUser, privilege, &token));
    return token;
  }

  static constexpr const char* kUser = "cdc-user";
  unique_ptr<TokenSigner> signer_;
  string table_id_;
};

TEST_F(CDCAuthzTest, RejectsMissingToken) {
  // Writes are submitted to the replica directly: a Write RPC needs a token.
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(0, 2)));
  {
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(0);
    GetChangesResponsePB resp;
    RpcController rpc;
    Status s = cdc_proxy_->GetChanges(req, &resp, &rpc);
    EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
    ASSERT_NE(nullptr, rpc.error_response());
    EXPECT_EQ(rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
              rpc.error_response()->code());
  }
  // Snapshot pages and checkpoints need a token as well.
  GetChangesResponsePB resp;
  Status s = DoSnapshot(kTabletId, /*is_start=*/true, "", &resp);
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
  EXPECT_EQ(0, mini_server_->server()->cdc_service()->SnapshotSessionsEstablishedForTests());
  CheckpointResponsePB cp_resp;
  s = DoCheckpoint(kTabletId, 1, &cp_resp);
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
}

TEST_F(CDCAuthzTest, RejectsWrongTableToken) {
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(0, 2)));
  SignedTokenPB token = MakeToken("some-other-table", /*scan_privilege=*/true);
  GetChangesResponsePB resp;
  Status s = DoGetChanges(kTabletId, 0, &resp, /*max_bytes=*/0,
                          /*need_schema_info=*/false, &token);
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
}

TEST_F(CDCAuthzTest, RejectsTokenWithoutScanPrivilege) {
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(0, 2)));
  // A token for some columns only is not enough: CDC records carry every
  // column.
  TablePrivilegePB privilege;
  privilege.set_table_id(table_id_);
  (*privilege.mutable_column_privileges())[0].set_scan_privilege(true);
  SignedTokenPB token;
  ASSERT_OK(signer_->GenerateAuthzToken(kUser, privilege, &token));
  GetChangesRequestPB req;
  req.set_stream_id("test-stream-1");
  req.set_tablet_id(kTabletId);
  req.set_from_op_index(0);
  *req.mutable_authz_token() = token;
  GetChangesResponsePB resp;
  RpcController rpc;
  Status s = cdc_proxy_->GetChanges(req, &resp, &rpc);
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_NE(nullptr, rpc.error_response());
  EXPECT_EQ(rpc::ErrorStatusPB::FATAL_UNAUTHORIZED, rpc.error_response()->code());
}

TEST_F(CDCAuthzTest, AllowsValidScanToken) {
  NO_FATALS(SubmitRows(RowOperationsPB::INSERT, Keys(0, 2)));
  SignedTokenPB token = MakeToken(table_id_, /*scan_privilege=*/true);
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp, /*max_bytes=*/0,
                         /*need_schema_info=*/false, &token));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

  // Checkpoint is likewise authorized with the same token.
  CheckpointResponsePB cp;
  ASSERT_OK(DoCheckpoint(kTabletId, resp.checkpoint_op_index(), &cp, &token));
  ASSERT_FALSE(cp.has_error()) << cp.error().DebugString();
}

// The tablet servers parse the master's CDCStreamConfigPB as their own (see
// StreamConfigFromMaster()), so the two copies must have the same fields and
// enum values.
TEST(CDCStreamConfigTest, ConfigProtosStayInSync) {
  google::protobuf::DescriptorProto cdc_config;
  google::protobuf::DescriptorProto master_config;
  CDCStreamConfigPB::descriptor()->CopyTo(&cdc_config);
  master::CDCStreamConfigPB::descriptor()->CopyTo(&master_config);
  // An enum field names its type with the package.
  const auto strip_package = [](const string& package,
                                google::protobuf::DescriptorProto* config) {
    const string prefix = "." + package + ".";
    for (auto& field : *config->mutable_field()) {
      if (HasPrefixString(field.type_name(), prefix)) {
        field.set_type_name(field.type_name().substr(prefix.size()));
      }
    }
  };
  strip_package(CDCStreamConfigPB::descriptor()->file()->package(), &cdc_config);
  strip_package(master::CDCStreamConfigPB::descriptor()->file()->package(), &master_config);
  EXPECT_EQ(master_config.DebugString(), cdc_config.DebugString());
}

// A stream configuration with an option this tablet server does not know,
// from a newer master, is rejected rather than served as if the option were
// not set. Before, an unknown record type was served as CHANGE, and an unknown
// field was ignored.
TEST(CDCStreamConfigTest, StreamConfigWithUnknownOptionsRejected) {
  master::CDCStreamConfigPB master_config;
  master_config.set_record_type(master::CDCStreamConfigPB::FULL);
  master_config.set_snapshot_mode(master::CDCStreamConfigPB::INITIAL_ONLY);
  CDCStreamConfigPB config;
  ASSERT_OK(StreamConfigFromMaster(master_config, &config));
  EXPECT_EQ(CDCStreamConfigPB::FULL, config.record_type());
  EXPECT_EQ(CDCStreamConfigPB::INITIAL_ONLY, config.snapshot_mode());

  // A field this server does not know.
  master::CDCStreamConfigPB newer_field = master_config;
  newer_field.mutable_unknown_fields()->AddVarint(100, 1);
  Status s = StreamConfigFromMaster(newer_field, &config);
  EXPECT_TRUE(s.IsNotSupported()) << s.ToString();

  // A record type this server does not know.
  master::CDCStreamConfigPB newer_enum;
  newer_enum.mutable_unknown_fields()->AddVarint(
      master::CDCStreamConfigPB::kRecordTypeFieldNumber, 100);
  s = StreamConfigFromMaster(newer_enum, &config);
  EXPECT_TRUE(s.IsNotSupported()) << s.ToString();
}

} // namespace cdc
} // namespace kudu
