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
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <vector>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.proxy.h"
#include "kudu/cdc/cdc_service.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/server/rpc_server.h"
#include "kudu/security/crypto.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_verifier.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(cdc_enforce_access_control);
DECLARE_bool(cdc_inject_full_reconstruction_failure);
DECLARE_int32(tablet_inject_latency_on_apply_write_op_ms);
DECLARE_int32(tablet_inject_latency_on_apply_alter_schema_op_ms);
DECLARE_int32(cdc_stream_config_cache_ttl_ms);
DECLARE_int32(cdc_snapshot_wait_timeout_ms);
DECLARE_int32(cdc_inject_latency_before_snapshot_establish_ms);
DECLARE_int32(cdc_inject_latency_before_stream_config_fetch_ms);
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
DECLARE_int64(cdc_checkpoint_persist_interval_ms);

METRIC_DECLARE_entity(cdc_stream);
METRIC_DECLARE_gauge_int64(cdc_stream_sent_lag_micros);
METRIC_DECLARE_gauge_int64(cdc_stream_active_age_micros);
METRIC_DECLARE_counter(cdc_checkpoint_requests);
METRIC_DECLARE_counter(cdc_checkpoint_persists);
METRIC_DECLARE_counter(cdc_checkpoint_persist_failures);
METRIC_DECLARE_counter(cdc_scans_rejected_server_memory);

using kudu::rpc::RpcController;
using kudu::security::PrivateKey;
using kudu::security::SignedTokenPB;
using kudu::security::TablePrivilegePB;
using kudu::security::TokenSigner;
using kudu::security::TokenSigningPrivateKeyPB;
using kudu::security::TokenSigningPublicKeyPB;
using kudu::security::TokenVerifier;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace cdc {

class CDCServiceTest : public tserver::TabletServerTestBase {
 public:
  void SetUp() override {
    TabletServerTestBase::SetUp();
    StartTabletServer(/*num_data_dirs=*/1);

    cdc_proxy_.reset(new CDCServiceProxy(
        client_messenger_,
        mini_server_->bound_rpc_addr(),
        mini_server_->bound_rpc_addr().host()));
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

  // Seed the CDCService's stream-config cache so tests exercise FULL-mode and
  // snapshot behavior without a live master.
  void SeedStreamConfig(CDCStreamConfigPB::RecordType record_type,
                        CDCStreamConfigPB::SnapshotMode snapshot_mode =
                            CDCStreamConfigPB::NEVER) {
    CDCStreamConfigPB config;
    config.set_record_type(record_type);
    config.set_snapshot_mode(snapshot_mode);
    mini_server_->server()->cdc_service()->SetStreamConfigForTests(
        "test-stream-1", config);
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

  unique_ptr<CDCServiceProxy> cdc_proxy_;
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

TEST_F(CDCServiceTest, Checkpoint_AdvancesAnchor) {
  InsertTestRowsRemote(0, 3, 3);

  GetChangesResponsePB get_resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &get_resp));
  ASSERT_FALSE(get_resp.has_error());
  int64_t checkpoint = get_resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  // Checkpoint should succeed.
  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, checkpoint, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();

  // Verify the anchor is registered at the checkpoint index.
  int64_t min_anchor_idx;
  ASSERT_OK(tablet_replica_->log_anchor_registry()->GetEarliestRegisteredLogIndex(
      &min_anchor_idx));
  EXPECT_LE(min_anchor_idx, checkpoint);
}

TEST_F(CDCServiceTest, Checkpoint_TabletNotFound) {
  CheckpointResponsePB resp;
  ASSERT_OK(DoCheckpoint("nonexistent-tablet", 5, &resp));
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::TABLET_NOT_FOUND, resp.error().code());
}

// Lever 3: durable checkpoint persistence to the master is rate-limited per
// (stream, tablet) by --cdc_checkpoint_persist_interval_ms. Rapid Checkpoint RPCs
// within one interval must persist at most once (write-combining the latest
// value), while the in-memory WAL anchor still advances on EVERY call so log GC is
// never delayed by the throttle.
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

  // The WAL anchor still advanced to the checkpoint (in-memory, every call).
  int64_t min_anchor_idx;
  ASSERT_OK(tablet_replica_->log_anchor_registry()->GetEarliestRegisteredLogIndex(
      &min_anchor_idx));
  EXPECT_LE(min_anchor_idx, checkpoint);
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

// CF-2/DR-018: PersistCheckpoint must increment cdc_checkpoint_persist_failures
// when all master candidates fail (here: the inject flag forces failure so the
// test does not require a real master connectivity failure).
//
// Non-vacuous: reverting the counter increment from PersistCheckpoint causes
// the final EXPECT_GT to fail.
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

  // Inject persist failures: every PersistCheckpoint call bails immediately
  // without sending any master RPC and increments the failure counter.
  FLAGS_cdc_inject_checkpoint_persist_failure = true;
  SCOPED_CLEANUP({ FLAGS_cdc_inject_checkpoint_persist_failure = false; });

  CheckpointResponsePB cp_resp;
  ASSERT_OK(DoCheckpoint(kTabletId, checkpoint, &cp_resp));
  ASSERT_FALSE(cp_resp.has_error()) << cp_resp.error().DebugString();

  // The Checkpoint RPC succeeded (consumer sees success) but the durable
  // persist to the master failed. The failure counter must have advanced.
  EXPECT_GT(CDCCounterValue(METRIC_cdc_checkpoint_persist_failures), failures_before)
      << "CF-2 fix: a persist failure must increment cdc_checkpoint_persist_failures "
         "so operators can observe the condition on monitoring dashboards";
}

// Exercises the per-tablet retention barrier that the master pushes to every
// replica (independent of the per-consumer Checkpoint path).
TEST_F(CDCServiceTest, SetRetentionBarrier) {
  InsertTestRowsRemote(0, 3, 3);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);

  // Setting a barrier at op index 1 pins WAL retention to at most index 1.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, 1));
  int64_t min_anchor_idx;
  ASSERT_OK(tablet_replica_->log_anchor_registry()->GetEarliestRegisteredLogIndex(
      &min_anchor_idx));
  EXPECT_LE(min_anchor_idx, 1);

  // Releasing the barrier (negative index) succeeds and is idempotent.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, -1));
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, -1));

  // A barrier for a tablet not hosted here returns NotFound.
  Status s = cdc->SetRetentionBarrier("nonexistent-tablet", 1);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
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
  // Observe only the CDC-owned retention anchor. The tablet's own machinery
  // (MRS/DMS flush anchors, etc.) keeps unrelated anchors in the same shared
  // LogAnchorRegistry, so querying the registry's earliest index would not
  // isolate the barrier under test.
  int64_t idx = -1;

  // Pass @seq=100 sets a barrier at op index 1.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/100));
  ASSERT_TRUE(cdc->RetentionAnchorForTests(kTabletId, &idx));
  EXPECT_EQ(1, idx);

  // A later pass @seq=200 releases the barrier.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/200));
  EXPECT_FALSE(cdc->RetentionAnchorForTests(kTabletId));

  // The delayed SET from the earlier pass @seq=100 now arrives out of order. It
  // must be discarded (superseded by seq=200), NOT re-anchor the replica.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/100));
  EXPECT_FALSE(cdc->RetentionAnchorForTests(kTabletId))
      << "stale SET re-anchored the replica after a newer RELEASE (leak)";

  // A genuinely newer pass @seq=300 is still honored.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/2,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/300));
  ASSERT_TRUE(cdc->RetentionAnchorForTests(kTabletId, &idx));
  EXPECT_EQ(2, idx);

  // Symmetric case: a stale RELEASE @seq=250 (older than the live SET@300) must
  // not tear down the barrier the newer SET established.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/250));
  ASSERT_TRUE(cdc->RetentionAnchorForTests(kTabletId, &idx))
      << "stale RELEASE tore down the barrier a newer SET established (leak)";
  EXPECT_EQ(2, idx);
}

// A4: on stream deletion the master fans a consumer-anchor release to every
// replica via SetRetentionBarrier's release_consumer_stream_id. The
// per-(stream, tablet) consumer anchor is established by the consumer's own
// polling (leader only) and lives in stream_tablet_state_, separate from the
// master-pushed aggregate retention barrier; without this release it would be
// freed only when the tablet itself is deleted, so a deleted stream would keep
// pinning the WAL. Verify the release frees the consumer anchor, independently
// of the aggregate barrier, in both the shared-tablet (barrier left intact) and
// orphaned-tablet (barrier also released) cases.
TEST_F(CDCServiceTest, SetRetentionBarrier_ReleasesConsumerAnchorOnStreamDelete) {
  InsertTestRowsRemote(0, 3, 3);

  CDCServiceImpl* cdc = mini_server_->server()->cdc_service();
  ASSERT_NE(nullptr, cdc);

  // A consumer poll (not caught up) establishes the per-session consumer anchor.
  GetChangesResponsePB resp;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_TRUE(cdc->ConsumerAnchorForTests("test-stream-1", kTabletId));

  // Independently set the aggregate barrier so we can prove the two release
  // paths do not interfere.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/1));
  ASSERT_TRUE(cdc->RetentionAnchorForTests(kTabletId));

  // Shared-tablet case: skip_barrier_update releases ONLY the consumer anchor,
  // leaving the aggregate barrier for the master's next pass to recompute across
  // the surviving streams.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/0,
                                     /*release_consumer_stream_id=*/"test-stream-1",
                                     /*skip_barrier_update=*/true));
  EXPECT_FALSE(cdc->ConsumerAnchorForTests("test-stream-1", kTabletId))
      << "consumer anchor not released on stream delete (A4 leak)";
  EXPECT_TRUE(cdc->RetentionAnchorForTests(kTabletId))
      << "skip_barrier_update must leave the aggregate barrier untouched";

  // Releasing an already-released consumer anchor is a harmless no-op.
  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/0,
                                     /*release_consumer_stream_id=*/"test-stream-1",
                                     /*skip_barrier_update=*/true));
  EXPECT_FALSE(cdc->ConsumerAnchorForTests("test-stream-1", kTabletId));

  // Orphaned-tablet case: re-establish both anchors, then release them together
  // in one call (skip_barrier_update=false, min_retained_op_index=-1).
  GetChangesResponsePB resp2;
  ASSERT_OK(DoGetChanges(kTabletId, 0, &resp2));
  ASSERT_FALSE(resp2.has_error()) << resp2.error().DebugString();
  ASSERT_TRUE(cdc->ConsumerAnchorForTests("test-stream-1", kTabletId));
  ASSERT_TRUE(cdc->RetentionAnchorForTests(kTabletId));

  ASSERT_OK(cdc->SetRetentionBarrier(kTabletId, /*min_retained_op_index=*/-1,
                                     /*history_safe_time_micros=*/0,
                                     /*barrier_seq=*/0,
                                     /*release_consumer_stream_id=*/"test-stream-1",
                                     /*skip_barrier_update=*/false));
  EXPECT_FALSE(cdc->ConsumerAnchorForTests("test-stream-1", kTabletId))
      << "consumer anchor not released alongside the aggregate barrier";
  EXPECT_FALSE(cdc->RetentionAnchorForTests(kTabletId))
      << "aggregate barrier not released for an orphaned tablet";
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
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  InsertTestRowsRemote(3, 3, /*num_batches=*/3);
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
  ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  InsertTestRowsRemote(3, 3, /*num_batches=*/3);
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
  cdc->SetStreamConfigForTestsWithTtl(kStream, config,
                                      MonoDelta::FromMilliseconds(1000));
  ASSERT_TRUE(cdc->IsStreamConfigFreshForTests(kStream));

  // A GetChanges while the entry is still fresh must take the cache fast path
  // and NOT extend the deadline; the entry therefore goes stale on schedule.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
  SleepFor(MonoDelta::FromMilliseconds(1500));
  ASSERT_FALSE(cdc->IsStreamConfigFreshForTests(kStream))
      << "a fresh-hit GetChanges must not extend the cache deadline";

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
// ops. While an ALTER (N -> N+1) inside the window is committed but not yet
// applied, the applied version reads N; deriving the base as
// (applied_version - alters_in_batch) yields N-1, so every WRITE preceding the
// ALTER in the batch is mis-stamped one version too low and a consumer would
// decode it missing the column(s) already added by the N-1 -> N alter. The base
// must instead come from the ALTER op's own recorded new version.
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

  // Now issue a second alter (v1 -> v2) whose apply is delayed, so it is
  // committed-but-not-applied while we read. The RPC returns only after apply,
  // so run it on a background thread and read during the apply window.
  FLAGS_tablet_inject_latency_on_apply_alter_schema_op_ms = 5000;
  Status alter_status;
  std::thread alterer([&]() {
    AlterAddColumn(v1_schema, "c2", /*new_version=*/2, &alter_status);
  });

  // Poll until the v2 DDL surfaces in the batch while the applied schema version
  // is still 1 (i.e. we are inside the commit-but-not-applied window). At that
  // instant the pre-alter WRITE must carry schema_version 1, not 0.
  bool verified = false;
  const MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  while (MonoTime::Now() < deadline && !verified) {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, checkpoint, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();

    bool saw_ddl_to_v2 = false;
    int32_t insert_schema_version = -1;
    for (int i = 0; i < resp.records_size(); ++i) {
      const CDCRecordPB& r = resp.records(i);
      if (r.op_type() == CDCOpTypePB::DDL && r.new_schema_version() == 2) {
        saw_ddl_to_v2 = true;
      } else if (r.op_type() == CDCOpTypePB::INSERT) {
        insert_schema_version = static_cast<int32_t>(r.schema_version());
      }
    }
    // Only assert when both the DDL is in-window AND the alter is still
    // unapplied -- that is precisely the racy state the fix targets.
    if (saw_ddl_to_v2 &&
        tablet_replica_->tablet_metadata()->schema_version() == 1) {
      ASSERT_EQ(1, insert_schema_version)
          << "pre-ALTER WRITE mis-stamped during commit/apply race: "
          << resp.DebugString();
      verified = true;
    }
  }
  alterer.join();
  ASSERT_OK(alter_status);
  ASSERT_TRUE(verified)
      << "never observed the committed-but-unapplied alter window";
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

  // Issues GetChanges with an explicit declared schema_version. Builds the
  // request inline because the shared DoGetChanges helper does not expose it.
  auto get_changes_with_schema =
      [&](int32_t declared_version, bool need_schema_info,
          GetChangesResponsePB* resp) -> Status {
    GetChangesRequestPB req;
    req.set_stream_id("test-stream-1");
    req.set_tablet_id(kTabletId);
    req.set_from_op_index(0);
    req.set_schema_version(declared_version);
    if (need_schema_info) {
      req.set_need_schema_info(true);
    }
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    return cdc_proxy_->GetChanges(req, resp, &rpc);
  };

  // Declared version 0 < current 1, not refreshing: rejected with a retryable
  // SCHEMA_VERSION_MISMATCH and no records leaked.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(get_changes_with_schema(0, /*need_schema_info=*/false, &resp));
    ASSERT_TRUE(resp.has_error()) << resp.DebugString();
    EXPECT_EQ(CDCErrorPB::SCHEMA_VERSION_MISMATCH, resp.error().code())
        << resp.error().DebugString();
    EXPECT_TRUE(resp.error().is_retryable()) << resp.error().DebugString();
    EXPECT_FALSE(resp.error().needs_resnapshot()) << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
  }

  // Same stale version, but need_schema_info=true means the consumer is
  // refreshing: the current schema is prepended and the check is skipped.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(get_changes_with_schema(0, /*need_schema_info=*/true, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    EXPECT_GT(resp.records_size(), 0);
  }

  // Declared version equal to the current applied version: no mismatch.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(get_changes_with_schema(1, /*need_schema_info=*/false, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // No declared version at all (default): backward-compatible, check disabled.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoGetChanges(kTabletId, 0, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }

  // Declared version AHEAD of the tablet's current version: must not be
  // rejected. A consumer that pre-loaded a schema from DDL records may hold a
  // version the tablet has committed but not yet applied; that is valid and must
  // not trigger SCHEMA_VERSION_MISMATCH (the check is strictly <, not !=).
  {
    GetChangesResponsePB resp;
    // Tablet is at version 1; consumer claims version 2 (ahead).
    ASSERT_OK(get_changes_with_schema(2, /*need_schema_info=*/false, &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
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

// E10: while a snapshot session is live, the server resumes from its OWN stored
// last-scanned key -- the authoritative bound -- not from the value the client
// echoes back. A misbehaving or lagging consumer that keeps sending a stale
// resume_key (e.g. it lost its position and replays an early key) must not be
// able to reposition the scan backward: doing so would re-read the head of the
// table (duplicates) and could wedge pagination forever. The resume_key on the
// request is still required as the "continue this snapshot" signal (see the
// routing in GetChanges), but its VALUE must be ignored while the session lives.
//
// Here the client latches the resume_key from the first page and then sends that
// same stale key on every subsequent continuation. Pre-fix (server trusts the
// client key) the scan would restart just after the first page every time and
// never terminate; with the fix the server's advancing stored key drives the
// scan to completion, each row seen exactly once in strictly increasing order.
TEST_F(CDCServiceTest, Snapshot_ResumesFromServerAuthoritativeKey) {
  SeedStreamConfig(CDCStreamConfigPB::FULL, CDCStreamConfigPB::INITIAL_AND_CONTINUE);
  InsertTestRowsRemote(0, 50, 50);

  int total_reads = 0;
  int pages = 0;
  bool done = false;
  int32_t last_key = -1;
  bool first = true;
  // The stale key the client keeps replaying: the resume_key returned by the
  // very first page. Empty until that page completes.
  string stale_resume_key;
  while (!done) {
    GetChangesResponsePB resp;
    // Tiny page to force pagination. First page starts the snapshot; every
    // continuation deliberately replays the stale first-page key rather than the
    // latest one, so only the server's authoritative key can drive progress.
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/first, stale_resume_key, &resp, 64));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    first = false;
    for (int i = 0; i < resp.records_size(); ++i) {
      ASSERT_EQ(CDCOpTypePB::READ, resp.records(i).op_type());
      int32_t k = -1;
      ASSERT_TRUE(GetInt32Col(resp.records(i), /*before=*/false, "key", &k));
      EXPECT_GT(k, last_key) << "server did not resume from its authoritative key";
      last_key = k;
      total_reads++;
    }
    done = resp.snapshot_done();
    // Latch the first page's key once and never update it: the client is stuck
    // replaying a stale position.
    if (stale_resume_key.empty()) {
      stale_resume_key = resp.snapshot_resume_key();
    }
    pages++;
    ASSERT_LT(pages, 1000)
        << "snapshot did not terminate (server honored the stale client key)";
  }
  EXPECT_EQ(50, total_reads);
  EXPECT_GT(pages, 1);  // actually paginated across multiple calls
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
  cdc->SetStreamConfigForTestsWithTtl("test-stream-1", config,
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
    // E1: SNAPSHOT_SESSION_LOST is fatal (not retryable in place, not a
    // re-snapshot in the WAL sense). The consumer must restart the snapshot
    // protocol from the beginning (is_snapshot_start=true, no resume key).
    EXPECT_FALSE(resp.error().is_retryable()) << resp.error().DebugString();
    EXPECT_FALSE(resp.error().needs_resnapshot()) << resp.error().DebugString();
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
    EXPECT_FALSE(resp.error().needs_resnapshot()) << resp.error().DebugString();
    EXPECT_EQ(0, resp.records_size());
  }

  // A clean restart (is_snapshot_start=true, no resume key) still succeeds.
  {
    GetChangesResponsePB resp;
    ASSERT_OK(DoSnapshot(kTabletId, /*is_start=*/true, "", &resp));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  }
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
      rpc_status[i] = DoSnapshot(kTabletId, /*is_start=*/true, "", &resps[i]);
    });
  }
  for (auto& t : threads) t.join();

  for (int i = 0; i < kThreads; ++i) {
    ASSERT_OK(rpc_status[i]);
    ASSERT_FALSE(resps[i].has_error()) << resps[i].error().DebugString();
  }
  // Exactly one fresh session despite kThreads concurrent starts.
  EXPECT_EQ(1, cdc->SnapshotSessionsEstablishedForTests());
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

// Isolation: --cdc_max_concurrent_scans sheds a heavy scan when the cap is full
// and recovers once slots free up. A committed-but-slow-to-apply UPDATE makes a
// FULL reconstruction block in WaitForSnapshotWithAllApplied, so the GetChanges
// serving it holds the only scan slot; a second concurrent FULL GetChanges is
// then shed with SERVER_TOO_BUSY.
TEST_F(CDCServiceTest, Isolation_ConcurrentScanCapShedsAndRecovers) {
  SeedStreamConfig(CDCStreamConfigPB::FULL);
  InsertTestRowsRemote(0, 1, 1);      // key=0
  FLAGS_cdc_max_concurrent_scans = 1;

  // Commit-fast, apply-slow UPDATE: it becomes visible to GetChanges (committed)
  // well before it applies, so a FULL scan reading it blocks ~3s in the apply
  // wait while holding its scan slot.
  FLAGS_tablet_inject_latency_on_apply_write_op_ms = 3000;
  std::thread updater([&]() { UpdateTestRowRemote(0, 12345); });
  // Let the UPDATE reach COMMITTED_OPID (commit is fast; only apply is delayed)
  // so both racing scans below actually attempt heavy reconstruction.
  SleepFor(MonoDelta::FromMilliseconds(500));

  // Two concurrent FULL scans, cap = 1: exactly one wins the slot (and blocks in
  // the apply wait); the other is shed immediately.
  Status s1, s2;
  GetChangesResponsePB r1, r2;
  std::thread t1([&]() { s1 = DoGetChanges(kTabletId, 0, &r1); });
  std::thread t2([&]() { s2 = DoGetChanges(kTabletId, 0, &r2); });
  t1.join();
  t2.join();
  updater.join();
  ASSERT_OK(s1);
  ASSERT_OK(s2);

  auto is_busy = [](const GetChangesResponsePB& r) {
    return r.has_error() && r.error().code() == CDCErrorPB::SERVER_TOO_BUSY;
  };
  const int busy = (is_busy(r1) ? 1 : 0) + (is_busy(r2) ? 1 : 0);
  const int served = (!r1.has_error() ? 1 : 0) + (!r2.has_error() ? 1 : 0);
  EXPECT_EQ(1, busy) << "expected exactly one concurrent FULL scan to be shed";
  EXPECT_EQ(1, served) << "expected exactly one concurrent FULL scan to succeed";

  // Slot freed and latency cleared: a fresh scan is admitted again.
  FLAGS_tablet_inject_latency_on_apply_write_op_ms = 0;
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

// Phase 7.1: with enforcement enabled, CDC RPCs require a valid authz token
// granting SCAN privilege on the target table.
class CDCAuthzTest : public CDCServiceTest {
 public:
  void SetUp() override {
    FLAGS_cdc_enforce_access_control = true;
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
  InsertTestRowsRemote(0, 2, 2);
  GetChangesResponsePB resp;
  Status s = DoGetChanges(kTabletId, 0, &resp);
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
}

TEST_F(CDCAuthzTest, RejectsWrongTableToken) {
  InsertTestRowsRemote(0, 2, 2);
  SignedTokenPB token = MakeToken("some-other-table", /*scan_privilege=*/true);
  GetChangesResponsePB resp;
  Status s = DoGetChanges(kTabletId, 0, &resp, /*max_bytes=*/0,
                          /*need_schema_info=*/false, &token);
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
}

TEST_F(CDCAuthzTest, RejectsTokenWithoutScanPrivilege) {
  InsertTestRowsRemote(0, 2, 2);
  SignedTokenPB token = MakeToken(table_id_, /*scan_privilege=*/false);
  GetChangesResponsePB resp;
  Status s = DoGetChanges(kTabletId, 0, &resp, /*max_bytes=*/0,
                          /*need_schema_info=*/false, &token);
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
}

TEST_F(CDCAuthzTest, AllowsValidScanToken) {
  InsertTestRowsRemote(0, 2, 2);
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

} // namespace cdc
} // namespace kudu
