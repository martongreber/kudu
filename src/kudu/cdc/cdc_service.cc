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

#include "kudu/cdc/cdc_service.h"

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_util.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_verification_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/server_base.h"
#include "kudu/util/flag_tags.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

// Number of RPC worker threads per service pool (owned by rpc_server.cc). The
// CDC service pool is inited with this many threads, so it is the denominator
// for --cdc_get_changes_free_rpc_ratio's worker-thread reservation.
DECLARE_int32(rpc_num_service_threads);

DEFINE_int64(cdc_max_bytes_per_response, 8 * 1024 * 1024, // 8 MiB
             "Default maximum total size of CDCRecordPB entries returned in a single "
             "GetChanges response (8 MiB). Bounds the per-response heap a CDC consumer "
             "can force a tablet server to buffer; a smaller cap means more (lighter) "
             "pages rather than one large one, so a burst of consumers competes less "
             "with user traffic for heap. Overridden by the per-stream config and, for "
             "the snapshot bootstrap path, by the (smaller) "
             "--cdc_snapshot_max_bytes_per_response. Must be "
             "<= --cdc_max_transaction_span_bytes.");
TAG_FLAG(cdc_max_bytes_per_response, advanced);
TAG_FLAG(cdc_max_bytes_per_response, runtime);

DEFINE_int64(cdc_max_transaction_span_bytes, 536870912, // 512 MiB
             "Maximum WAL span (bytes, from a transaction's first write to its "
             "FINALIZE_COMMIT) that CDC will read in order to emit an open "
             "transaction atomically. A transaction is only visible to consumers "
             "at commit, so its entire span must fit in one read window; if it "
             "exceeds the per-response byte cap the read window would never reach "
             "the commit and the stream's checkpoint could never advance past the "
             "transaction (a permanent wedge). When such a transaction is "
             "detected the effective read cap is escalated (up to this limit) to "
             "reach the commit. A transaction whose span exceeds this limit "
             "cannot be emitted; GetChanges then fails loudly with "
             "TRANSACTION_TOO_LARGE rather than stalling silently. Must be >= "
             "--cdc_max_bytes_per_response.");
TAG_FLAG(cdc_max_transaction_span_bytes, advanced);
TAG_FLAG(cdc_max_transaction_span_bytes, runtime);

DEFINE_int32(cdc_full_apply_wait_timeout_ms, 30000, // 30 s
             "Maximum time a FULL-mode GetChanges will wait for committed ops to be "
             "applied to the MemRowSet before reconstructing before/after images. "
             "Caps the effective deadline derived from the RPC's own deadline. On "
             "timeout the call returns a retryable error and the consumer re-polls.");
TAG_FLAG(cdc_full_apply_wait_timeout_ms, advanced);
TAG_FLAG(cdc_full_apply_wait_timeout_ms, runtime);

DEFINE_int32(cdc_snapshot_wait_timeout_ms, 30000, // 30 s
             "Maximum time a snapshot GetChanges page will wait for the tablet's "
             "MVCC state to be safe/applied at the snapshot timestamp. Caps the "
             "effective deadline derived from the RPC's own client deadline, so a "
             "caller with a very long (or absent) deadline cannot block a service "
             "thread longer than this. On timeout the call returns a retryable "
             "error and the consumer re-polls.");
TAG_FLAG(cdc_snapshot_wait_timeout_ms, advanced);
TAG_FLAG(cdc_snapshot_wait_timeout_ms, runtime);

DEFINE_double(cdc_read_safe_deadline_ratio, 0.10,
              "Fraction of a GetChanges call's remaining client-deadline budget "
              "to reserve as headroom for building and sending the response. The "
              "server stops waiting for committed ops (FULL apply-wait) or for "
              "the snapshot timestamp to become safe once (1 - ratio) of the "
              "remaining time is consumed, so the partial result it already has "
              "can be serialized and returned before the RPC deadline elapses "
              "rather than the whole call timing out with nothing. Only shrinks "
              "the client-derived deadline; the absolute --cdc_full_apply_wait_"
              "timeout_ms / --cdc_snapshot_wait_timeout_ms caps still apply. "
              "Mirrors YugabyteDB's cdc_read_safe_deadline_ratio. Must be in "
              "[0.0, 1.0); 0 disables the reservation.");
TAG_FLAG(cdc_read_safe_deadline_ratio, advanced);
TAG_FLAG(cdc_read_safe_deadline_ratio, runtime);

static bool ValidateCdcReadSafeDeadlineRatio(const char* name, double value) {
  if (value >= 0.0 && value < 1.0) {
    return true;
  }
  LOG(ERROR) << "--" << name << " must be in [0.0, 1.0); got " << value;
  return false;
}
DEFINE_validator(cdc_read_safe_deadline_ratio, &ValidateCdcReadSafeDeadlineRatio);

DEFINE_double(cdc_get_changes_free_rpc_ratio, 0.10,
              "Fraction of the RPC service worker threads to keep free from CDC "
              "GetChanges calls so a burst of CDC consumers cannot occupy every "
              "worker and starve non-GetChanges traffic (e.g. Checkpoint and "
              "other control RPCs sharing the CDC service pool). At most "
              "floor((1 - ratio) * --rpc_num_service_threads) GetChanges calls "
              "run concurrently on this server (always at least 1); excess calls "
              "are shed immediately with a retryable SERVER_TOO_BUSY so the "
              "consumer backs off and retries. Unlike --cdc_max_concurrent_scans "
              "/ --cdc_scan_mem_limit_bytes (which bound only heavy FULL-mode / "
              "snapshot scans by heap), this bounds worker-thread occupancy for "
              "all GetChanges calls, including the otherwise-unguarded CHANGE-mode "
              "WAL-streaming path. Mirrors YugabyteDB's "
              "cdc_get_changes_free_rpc_ratio. Must be in [0.0, 1.0); 0 lets "
              "GetChanges use every worker thread.");
TAG_FLAG(cdc_get_changes_free_rpc_ratio, advanced);
TAG_FLAG(cdc_get_changes_free_rpc_ratio, runtime);

static bool ValidateCdcGetChangesFreeRpcRatio(const char* name, double value) {
  if (value >= 0.0 && value < 1.0) {
    return true;
  }
  LOG(ERROR) << "--" << name << " must be in [0.0, 1.0); got " << value;
  return false;
}
DEFINE_validator(cdc_get_changes_free_rpc_ratio, &ValidateCdcGetChangesFreeRpcRatio);

DEFINE_int32(cdc_inject_latency_before_snapshot_establish_ms, 0,
             "How much latency (in milliseconds) to inject at the start of the "
             "snapshot establish sequence, before the snapshot timestamp is "
             "waited on. Widens the window for the snapshot-start race and lets "
             "tests exercise the client deadline. If the injected latency would "
             "run past the effective deadline, the establish fails with TimedOut. "
             "For testing only!");
TAG_FLAG(cdc_inject_latency_before_snapshot_establish_ms, unsafe);
TAG_FLAG(cdc_inject_latency_before_snapshot_establish_ms, runtime);

DEFINE_int32(cdc_inject_latency_before_stream_config_fetch_ms, 0,
             "How much latency (in milliseconds) to inject immediately before "
             "the master GetCDCStreamInfo RPC in GetOrFetchStreamConfig. Widens "
             "the config-fetch window so a test can pile up concurrent cache "
             "misses on the single-flight lock (E11). For testing only!");
TAG_FLAG(cdc_inject_latency_before_stream_config_fetch_ms, unsafe);
TAG_FLAG(cdc_inject_latency_before_stream_config_fetch_ms, runtime);

DEFINE_bool(cdc_inject_tablet_not_running, false,
            "If true, ReadChanges treats the tablet as not RUNNING and returns "
            "TABLET_NOT_RUNNING, so tests can exercise the not-running "
            "classification without racing tablet bootstrap. For testing only!");
TAG_FLAG(cdc_inject_tablet_not_running, unsafe);
TAG_FLAG(cdc_inject_tablet_not_running, runtime);

DEFINE_bool(cdc_inject_post_read_leadership_loss, false,
            "If true, the post-read leader-term recheck in ReadChanges fires as "
            "though this replica lost leadership during the scan, so tests can "
            "exercise the recheck deterministically without forcing a real "
            "leader change mid-read. For testing only!");
TAG_FLAG(cdc_inject_post_read_leadership_loss, unsafe);
TAG_FLAG(cdc_inject_post_read_leadership_loss, runtime);

DEFINE_bool(cdc_inject_server_memory_pressure, false,
            "If true, TryAcquireScanSlot behaves as though the whole server is "
            "over its soft memory limit, so tests can exercise the server-pressure "
            "shed deterministically without driving real process memory above the "
            "(process-wide, once-initialized) soft limit. For testing only!");
TAG_FLAG(cdc_inject_server_memory_pressure, unsafe);
TAG_FLAG(cdc_inject_server_memory_pressure, runtime);

DEFINE_bool(cdc_inject_checkpoint_persist_failure, false,
            "If true, every PersistCheckpoint call fails immediately (before "
            "attempting any master RPC), incrementing cdc_checkpoint_persist_failures "
            "and emitting a WARNING. Used by tests to exercise the CF-2 failure "
            "counter without requiring a real master connectivity failure. "
            "For testing only!");
TAG_FLAG(cdc_inject_checkpoint_persist_failure, unsafe);
TAG_FLAG(cdc_inject_checkpoint_persist_failure, runtime);

DEFINE_int64(cdc_snapshot_max_bytes_per_response, 8 * 1024 * 1024, // 8 MiB
             "Maximum total size of READ records returned in a single snapshot "
             "GetChanges page. Kept well below --cdc_max_bytes_per_response so a "
             "consumer's initial bulk snapshot scan produces smaller pages and "
             "competes less with user traffic for heap and the shared tablet "
             "iterator. Applies only to the snapshot bootstrap path; WAL "
             "streaming still uses the (larger) streaming cap. 0 disables the "
             "override (snapshot pages then use the streaming cap).");
TAG_FLAG(cdc_snapshot_max_bytes_per_response, advanced);
TAG_FLAG(cdc_snapshot_max_bytes_per_response, runtime);

DEFINE_int32(cdc_max_concurrent_scans, 8,
             "Maximum number of CDC heavy scans (snapshot bootstrap pages and "
             "FULL-mode before/after-image reconstruction) that may run "
             "concurrently on this tablet server. These read the tablet's "
             "rowsets/MVCC and thus compete with user scans for the shared "
             "iterator and file cache; capping them shields user-facing traffic "
             "from a burst of CDC consumers. Excess requests are shed with a "
             "retryable SERVER_TOO_BUSY error, so consumers back off and retry "
             "rather than overwhelming the server. 0 means unlimited (not "
             "recommended in production). CHANGE-mode WAL streaming is cheap and "
             "never counts against this cap.");
TAG_FLAG(cdc_max_concurrent_scans, advanced);
TAG_FLAG(cdc_max_concurrent_scans, runtime);

DEFINE_int64(cdc_scan_mem_limit_bytes, 256 * 1024 * 1024, // 256 MiB
             "Soft heap budget (bytes) for in-flight CDC heavy-scan responses. "
             "Before a snapshot or FULL scan runs, its response byte cap is "
             "reserved against a dedicated 'cdc_scans' MemTracker (a child of "
             "the server root tracker); if the reservation would exceed this "
             "limit the request is shed with a retryable SERVER_TOO_BUSY error so "
             "large CDC batches cannot exhaust the heap out from under user "
             "traffic. Acts as a server-wide backstop behind "
             "--cdc_max_concurrent_scans (which usually binds first at "
             "cap * --cdc_max_bytes_per_response); the default leaves headroom for "
             "FULL-mode transaction-span escalation. 0 means no limit (not "
             "recommended in production).");
TAG_FLAG(cdc_scan_mem_limit_bytes, advanced);
TAG_FLAG(cdc_scan_mem_limit_bytes, runtime);

DEFINE_int64(cdc_active_time_report_interval_ms, 5 * 60 * 1000, // 5 min
             "Minimum interval between consumer-activity heartbeats sent from a "
             "tablet server to the master on GetChanges. Refreshes the stream's "
             "last-active time so a live but not-yet-checkpointing consumer is "
             "not expired. Should be well below the master's --cdc_stream_expiry_ms.");
TAG_FLAG(cdc_active_time_report_interval_ms, advanced);
TAG_FLAG(cdc_active_time_report_interval_ms, runtime);

DEFINE_int64(cdc_checkpoint_persist_interval_ms, 15 * 1000, // 15 s
             "Minimum interval between durable checkpoint writes sent from a "
             "tablet server to the master per (stream, tablet). The in-memory WAL "
             "retention anchor still advances on every Checkpoint RPC regardless "
             "of this throttle; only the master-side durable persist is rate "
             "limited, which merely lets the master's persisted checkpoint lag "
             "(retention stays conservative). On a tablet-server crash a consumer "
             "may re-read up to one interval of already-processed records (the "
             "accepted at-least-once semantics). Set to 0 to persist on every "
             "Checkpoint (no throttle).");
TAG_FLAG(cdc_checkpoint_persist_interval_ms, advanced);
TAG_FLAG(cdc_checkpoint_persist_interval_ms, runtime);

DEFINE_int64(cdc_stream_idle_expiry_ms, 8LL * 60 * 60 * 1000, // 8 h
             "How long a CDC stream's per-(stream, tablet) session on this "
             "tablet server may sit idle (no successful GetChanges / Checkpoint) "
             "before the server, on the next GetChanges that finds the requested "
             "WAL already garbage-collected, reports STREAM_EXPIRED instead of "
             "WAL_EXPIRED. STREAM_EXPIRED tells the consumer the stream is "
             "permanently expired and it must re-bootstrap from a fresh snapshot; "
             "WAL_EXPIRED (the reply for a session still within this window) "
             "means the gap may be transient GC during a failover, so the "
             "consumer can retry. This is a reactive classification only -- it is "
             "checked at the moment the WAL is found missing, never proactively, "
             "so a still-served stream is never expired. Should match the "
             "master's --cdc_stream_expiry_ms. Set to 0 to disable the "
             "disambiguation (always report WAL_EXPIRED).");
TAG_FLAG(cdc_stream_idle_expiry_ms, advanced);
TAG_FLAG(cdc_stream_idle_expiry_ms, runtime);

DEFINE_int32(cdc_stream_config_cache_ttl_ms, 5 * 60 * 1000, // 5 min
             "How long (in milliseconds) a tablet server caches a CDC stream's "
             "configuration (record type, snapshot mode) fetched from the master "
             "before refetching it. Bounds how long a stream reconfigure takes to "
             "take effect on this server. Set to 0 to cache for the process "
             "lifetime (never refetch).");
TAG_FLAG(cdc_stream_config_cache_ttl_ms, advanced);
TAG_FLAG(cdc_stream_config_cache_ttl_ms, runtime);

DEFINE_bool(cdc_enforce_access_control, false,
            "Whether to require CDC callers to present a signed authorization "
            "token granting SCAN privilege on the target table (reuses the "
            "scan-token machinery). When false, CDC RPCs require only an "
            "authenticated client/service/super user, as before. Enable on "
            "clusters that enforce fine-grained authorization.");
TAG_FLAG(cdc_enforce_access_control, advanced);
TAG_FLAG(cdc_enforce_access_control, runtime);

METRIC_DEFINE_counter(
    server, cdc_get_changes_requests, "CDC GetChanges Requests",
    kudu::MetricUnit::kRequests,
    "Number of CDC GetChanges requests served by this tablet server",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(
    server, cdc_records_produced, "CDC Records Produced",
    kudu::MetricUnit::kEntries,
    "Number of CDC change records returned to consumers",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(
    server, cdc_checkpoint_requests, "CDC Checkpoint Requests",
    kudu::MetricUnit::kRequests,
    "Number of CDC Checkpoint requests served by this tablet server",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(
    server, cdc_checkpoint_persists, "CDC Checkpoint Durable Persists",
    kudu::MetricUnit::kRequests,
    "Number of CDC Checkpoint requests that issued a durable checkpoint write to "
    "the master. Rate-limited per (stream, tablet) by "
    "--cdc_checkpoint_persist_interval_ms, so this is at most "
    "cdc_checkpoint_requests; the gap between the two is throttled (write-combined) "
    "persists. A ratio near 1.0 under steady polling indicates the throttle is "
    "ineffective (interval too low).",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(
    server, cdc_checkpoint_persist_failures, "CDC Checkpoint Persist Failures",
    kudu::MetricUnit::kRequests,
    "Number of CDC durable checkpoint writes that failed to reach any master. "
    "Incremented whenever PersistCheckpoint exhausts all master candidates "
    "without a successful response. A non-zero rate while consumers are "
    "advancing means the master's last_checkpoint_advance_time_micros is "
    "falling behind and the staleness guard's advance-attempt grace floor "
    "(CF-2) is suppressing spurious barrier releases. Operators should "
    "investigate master connectivity and sys-catalog health.",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors, "CDC Errors",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned an error to the consumer",
    kudu::MetricLevel::kWarn);

// Per-error-code CDC error counters. Each incremented by SetCDCError for the
// matching CDCErrorPB::Code. cdc_errors is the aggregate roll-up; these give
// the per-code breakdown so operators can distinguish WAL_EXPIRED (consumer
// must resnapshot) from admission sheds (add capacity) from
// TRANSACTION_TOO_LARGE (workload/config).
METRIC_DEFINE_counter(
    server, cdc_errors_unknown, "CDC Errors: UNKNOWN_ERROR",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned an UNKNOWN_ERROR",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_stream_not_found, "CDC Errors: STREAM_NOT_FOUND",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned STREAM_NOT_FOUND (stream deleted or "
    "never existed; consumer should stop)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_tablet_not_found, "CDC Errors: TABLET_NOT_FOUND",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned TABLET_NOT_FOUND",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_tablet_not_leader, "CDC Errors: TABLET_NOT_LEADER",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned TABLET_NOT_LEADER (retryable; "
    "consumer should re-discover the leader)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_wal_expired, "CDC Errors: WAL_EXPIRED",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned WAL_EXPIRED (WAL GC'd; if sustained "
    "the consumer must resnapshot)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_tablet_not_running, "CDC Errors: TABLET_NOT_RUNNING",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned TABLET_NOT_RUNNING (retryable)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_history_expired, "CDC Errors: HISTORY_EXPIRED",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned HISTORY_EXPIRED (MVCC history GC'd "
    "before FULL-mode image reconstruction; consumer must resnapshot)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_stream_expired, "CDC Errors: STREAM_EXPIRED",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned STREAM_EXPIRED (stream idle past "
    "expiry; consumer must re-bootstrap from a fresh snapshot)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_not_authorized, "CDC Errors: NOT_AUTHORIZED",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned NOT_AUTHORIZED",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_server_too_busy, "CDC Errors: SERVER_TOO_BUSY",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests shed with SERVER_TOO_BUSY (retryable; see also "
    "cdc_scans_rejected_* for the reason breakdown)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_snapshot_session_lost, "CDC Errors: SNAPSHOT_SESSION_LOST",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned SNAPSHOT_SESSION_LOST (leader change "
    "mid-snapshot; consumer must restart the snapshot)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_transaction_too_large, "CDC Errors: TRANSACTION_TOO_LARGE",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned TRANSACTION_TOO_LARGE (raise "
    "--cdc_max_transaction_span_bytes or reduce transaction size)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_schema_version_mismatch, "CDC Errors: SCHEMA_VERSION_MISMATCH",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned SCHEMA_VERSION_MISMATCH (consumer "
    "declared a schema version older than the tablet's; it should re-request "
    "with need_schema_info=true)",
    kudu::MetricLevel::kWarn);

// Admission-shed counters: broken down by rejection reason within
// TryAcquireScanSlot and the RPC-worker reservation. All three ultimately
// return SERVER_TOO_BUSY; these counters give the reason breakdown.
METRIC_DEFINE_counter(
    server, cdc_scans_rejected_concurrency, "CDC Scans Rejected: Concurrency Limit",
    kudu::MetricUnit::kRequests,
    "Number of CDC heavy scans (snapshot/FULL) shed because "
    "--cdc_max_concurrent_scans was reached; raise the flag to add capacity",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_scans_rejected_memory, "CDC Scans Rejected: Memory Budget",
    kudu::MetricUnit::kRequests,
    "Number of CDC heavy scans shed because the --cdc_scan_mem_limit_bytes "
    "budget was exhausted; raise the flag or reduce concurrent scan footprint",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_scans_rejected_server_memory, "CDC Scans Rejected: Server Memory Pressure",
    kudu::MetricUnit::kRequests,
    "Number of CDC heavy scans shed because the whole server was above its "
    "soft memory limit (process_memory::SoftLimitExceeded); the tablet read "
    "path is already shedding user reads for the same reason",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_scans_rejected_worker_pool, "CDC Scans Rejected: Worker Pool",
    kudu::MetricUnit::kRequests,
    "Number of CDC GetChanges calls shed because the RPC worker reservation "
    "cap was reached (--cdc_get_changes_free_rpc_ratio); add RPC threads or "
    "reduce --cdc_get_changes_free_rpc_ratio",
    kudu::MetricLevel::kWarn);

METRIC_DEFINE_gauge_int64(
    server, cdc_max_sent_lag_micros, "CDC Max Consumer Sent Lag",
    kudu::MetricUnit::kMicroseconds,
    "Across all active CDC (stream, tablet) sessions on this server, the maximum "
    "wall-clock lag between now and the physical time of the newest record sent "
    "to the consumer. High values indicate a lagging or stuck consumer.",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_int64(
    server, cdc_max_active_age_micros, "CDC Max Consumer Inactivity Age",
    kudu::MetricUnit::kMicroseconds,
    "Across all active CDC (stream, tablet) sessions on this server, the maximum "
    "wall-clock time since the last consumer activity (GetChanges/Checkpoint). As "
    "this approaches the master's --cdc_stream_expiry_ms, the stream is nearing "
    "expiry and release of its retention barrier.",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_uint64(
    server, cdc_active_streams, "CDC Active Stream Sessions",
    kudu::MetricUnit::kUnits,
    "Number of active CDC (stream, tablet) consumer sessions tracked on this "
    "tablet server.",
    kudu::MetricLevel::kInfo);

// Per-(stream, tablet) CDC metric entity. One instance per active consumer
// session, carrying stream_id and tablet_id attributes so the gauges below are
// attributable to a specific stream (the server-level gauges only report the
// max/count across all sessions).
METRIC_DEFINE_entity(cdc_stream);
METRIC_DEFINE_gauge_int64(
    cdc_stream, cdc_stream_sent_lag_micros, "CDC Stream Consumer Sent Lag",
    kudu::MetricUnit::kMicroseconds,
    "For this CDC (stream, tablet) session, the wall-clock lag between now and "
    "the physical time of the newest record sent to the consumer. High values "
    "indicate a lagging or stuck consumer.",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_int64(
    cdc_stream, cdc_stream_active_age_micros, "CDC Stream Consumer Inactivity Age",
    kudu::MetricUnit::kMicroseconds,
    "For this CDC (stream, tablet) session, the wall-clock time since the last "
    "consumer activity (GetChanges/Checkpoint). As this grows without the "
    "checkpoint advancing, the stream nears release of its retention barrier "
    "(see the master's --cdc_stream_expiry_ms and --cdc_max_staleness_ms).",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_int64(
    cdc_stream, cdc_stream_ops_behind, "CDC Stream Ops Behind",
    kudu::MetricUnit::kEntries,
    "For this CDC (stream, tablet) session, the number of committed WAL ops "
    "the consumer has not yet acknowledged: (tablet last committed op index) "
    "minus (consumer last checkpointed op index), clamped to zero. A sustained "
    "non-zero value indicates a lagging consumer; a value growing without bound "
    "indicates a stuck or disconnected consumer.",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_int64(
    cdc_stream, cdc_stream_bootstrap_required, "CDC Stream Bootstrap Required",
    kudu::MetricUnit::kUnits,
    "For this CDC (stream, tablet) session, 1 if the consumer's last "
    "checkpointed op index is below the earliest op index still retained in "
    "the WAL, meaning the WAL this consumer needs has been or is about to be "
    "GC'd and the consumer must resnapshot before it can resume streaming. "
    "0 if the consumer is still within the retained WAL window.",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_gauge_int64(
    server, cdc_max_ops_behind, "CDC Max Consumer Ops Behind",
    kudu::MetricUnit::kEntries,
    "Across all active CDC (stream, tablet) sessions on this server, the "
    "maximum number of committed WAL ops any consumer has not yet acknowledged. "
    "High values indicate a lagging or stuck consumer.",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_int64(
    server, cdc_bootstrap_required_streams, "CDC Bootstrap Required Stream Count",
    kudu::MetricUnit::kUnits,
    "Number of active CDC (stream, tablet) sessions on this server whose "
    "consumer checkpoint has fallen below the earliest retained WAL op index. "
    "These consumers must resnapshot before they can resume streaming. A "
    "non-zero value signals imminent or actual data-loss risk for those streams.",
    kudu::MetricLevel::kWarn);

using kudu::clock::HybridClock;
using kudu::consensus::OpId;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::ReplicateMsg;
using kudu::log::Log;
using kudu::log::LogAnchor;
using kudu::log::LogAnchorRegistry;
using kudu::log::LogReader;
using kudu::tablet::TabletReplica;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cdc {

namespace {

// When --cdc_enforce_access_control is set, verifies that 'req' carries a valid
// signed authz token granting SCAN privilege on 'table_id'. Returns true if the
// caller is authorized (or enforcement is disabled). Otherwise responds at the
// RPC level with the same error codes the scan path uses — so a client's
// token-refresh logic works uniformly — and returns false. 'req' must expose
// has_authz_token()/authz_token() (GetChangesRequestPB and CheckpointRequestPB).
template <class Request>
bool AuthorizeCDCTableOrRespond(const security::TokenVerifier& token_verifier,
                                const Request& req,
                                const std::string& table_id,
                                rpc::RpcContext* context) {
  if (!FLAGS_cdc_enforce_access_control) {
    return true;
  }
  if (!req.has_authz_token()) {
    context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
        Status::NotAuthorized("no authorization token presented"));
    return false;
  }
  security::TokenPB token_pb;
  const auto result = token_verifier.VerifyTokenSignature(req.authz_token(), &token_pb);
  rpc::ErrorStatusPB::RpcErrorCodePB error;
  Status s = rpc::ParseTokenVerificationResult(
      result, rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN, &error);
  if (!s.ok()) {
    context->RespondRpcFailure(error, s.CloneAndPrepend("authz token verification failure"));
    return false;
  }
  if (!token_pb.has_authz() ||
      !token_pb.authz().has_table_privilege() ||
      token_pb.authz().username() != context->remote_user().username()) {
    context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
        Status::NotAuthorized("invalid authorization token presented"));
    return false;
  }
  const security::TablePrivilegePB& privilege = token_pb.authz().table_privilege();
  if (privilege.table_id() != table_id) {
    context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
        Status::NotAuthorized("authorization token is for the wrong table ID"));
    return false;
  }
  // Reading CDC changes exposes whole rows, so a full-table SCAN privilege is
  // the required granularity (column-level scan privileges are insufficient).
  if (!privilege.scan_privilege()) {
    context->RespondRpcFailure(rpc::ErrorStatusPB::FATAL_UNAUTHORIZED,
        Status::NotAuthorized("not authorized to read CDC changes for this table"));
    return false;
  }
  return true;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// CDCServiceImpl
// ---------------------------------------------------------------------------

CDCServiceImpl::CDCServiceImpl(tserver::TabletServer* server)
    : CDCServiceIf(server->metric_entity(), server->result_tracker()),
      server_(server) {
  DCHECK(server_);

  // Dedicated child of the server root tracker for CDC heavy-scan responses.
  // Created without a hard limit; --cdc_scan_mem_limit_bytes is enforced against
  // this tracker's consumption at reservation time so it can be tuned at runtime.
  scan_mem_tracker_ = MemTracker::CreateTracker(
      -1, "cdc_scans", server_->mem_tracker());

  const auto& entity = server->metric_entity();
  get_changes_requests_ = METRIC_cdc_get_changes_requests.Instantiate(entity);
  records_produced_ = METRIC_cdc_records_produced.Instantiate(entity);
  checkpoint_requests_ = METRIC_cdc_checkpoint_requests.Instantiate(entity);
  checkpoint_persists_ = METRIC_cdc_checkpoint_persists.Instantiate(entity);
  checkpoint_persist_failures_ = METRIC_cdc_checkpoint_persist_failures.Instantiate(entity);
  errors_ = METRIC_cdc_errors.Instantiate(entity);

  // Per-error-code counters -- index matches CDCErrorPB::Code (1-13).
  error_code_counters_[CDCErrorPB::UNKNOWN_ERROR] =
      METRIC_cdc_errors_unknown.Instantiate(entity);
  error_code_counters_[CDCErrorPB::STREAM_NOT_FOUND] =
      METRIC_cdc_errors_stream_not_found.Instantiate(entity);
  error_code_counters_[CDCErrorPB::TABLET_NOT_FOUND] =
      METRIC_cdc_errors_tablet_not_found.Instantiate(entity);
  error_code_counters_[CDCErrorPB::TABLET_NOT_LEADER] =
      METRIC_cdc_errors_tablet_not_leader.Instantiate(entity);
  error_code_counters_[CDCErrorPB::WAL_EXPIRED] =
      METRIC_cdc_errors_wal_expired.Instantiate(entity);
  error_code_counters_[CDCErrorPB::TABLET_NOT_RUNNING] =
      METRIC_cdc_errors_tablet_not_running.Instantiate(entity);
  error_code_counters_[CDCErrorPB::HISTORY_EXPIRED] =
      METRIC_cdc_errors_history_expired.Instantiate(entity);
  error_code_counters_[CDCErrorPB::STREAM_EXPIRED] =
      METRIC_cdc_errors_stream_expired.Instantiate(entity);
  error_code_counters_[CDCErrorPB::NOT_AUTHORIZED] =
      METRIC_cdc_errors_not_authorized.Instantiate(entity);
  error_code_counters_[CDCErrorPB::SERVER_TOO_BUSY] =
      METRIC_cdc_errors_server_too_busy.Instantiate(entity);
  error_code_counters_[CDCErrorPB::SNAPSHOT_SESSION_LOST] =
      METRIC_cdc_errors_snapshot_session_lost.Instantiate(entity);
  error_code_counters_[CDCErrorPB::TRANSACTION_TOO_LARGE] =
      METRIC_cdc_errors_transaction_too_large.Instantiate(entity);
  error_code_counters_[CDCErrorPB::SCHEMA_VERSION_MISMATCH] =
      METRIC_cdc_errors_schema_version_mismatch.Instantiate(entity);

  // Admission-shed breakdown counters.
  scans_rejected_concurrency_ =
      METRIC_cdc_scans_rejected_concurrency.Instantiate(entity);
  scans_rejected_memory_ =
      METRIC_cdc_scans_rejected_memory.Instantiate(entity);
  scans_rejected_server_memory_ =
      METRIC_cdc_scans_rejected_server_memory.Instantiate(entity);
  scans_rejected_worker_pool_ =
      METRIC_cdc_scans_rejected_worker_pool.Instantiate(entity);

  METRIC_cdc_max_sent_lag_micros.InstantiateFunctionGauge(
      entity, [this]() { return this->MaxSentLagMicros(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_max_active_age_micros.InstantiateFunctionGauge(
      entity, [this]() { return this->MaxActiveAgeMicros(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_active_streams.InstantiateFunctionGauge(
      entity, [this]() { return this->ActiveStreamCount(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_max_ops_behind.InstantiateFunctionGauge(
      entity, [this]() { return this->MaxOpsBehind(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_bootstrap_required_streams.InstantiateFunctionGauge(
      entity, [this]() { return this->BootstrapRequiredStreamCount(); })
      ->AutoDetach(&metric_detacher_);
}

namespace {
// The published retry contract per CDCErrorPB::Code (see cdc.proto CDCErrorPB).
// Mirrors the reference consumer's CDCTabletPoller::ClassifyCdcError so the
// wire-level fields are a single source of truth for external consumers.
// Returns (*is_retryable, *needs_resnapshot); both false means
// fatal/operator-action. The two are mutually exclusive.
void ClassifyCDCErrorCode(CDCErrorPB::Code code,
                          bool* is_retryable,
                          bool* needs_resnapshot) {
  *is_retryable = false;
  *needs_resnapshot = false;
  switch (code) {
    case CDCErrorPB::TABLET_NOT_LEADER:
    case CDCErrorPB::TABLET_NOT_FOUND:
    case CDCErrorPB::TABLET_NOT_RUNNING:
    case CDCErrorPB::SERVER_TOO_BUSY:
    case CDCErrorPB::NOT_AUTHORIZED:
    case CDCErrorPB::SCHEMA_VERSION_MISMATCH:
      *is_retryable = true;
      break;
    case CDCErrorPB::WAL_EXPIRED:
    case CDCErrorPB::HISTORY_EXPIRED:
    case CDCErrorPB::STREAM_EXPIRED:
      *needs_resnapshot = true;
      break;
    default:
      // UNKNOWN_ERROR, STREAM_NOT_FOUND, SNAPSHOT_SESSION_LOST,
      // TRANSACTION_TOO_LARGE: fatal, requires operator action.
      break;
  }
}
}  // anonymous namespace

void CDCServiceImpl::SetCDCError(CDCErrorPB::Code code,
                                  const Status& status,
                                  CDCErrorPB* error) {
  error->set_code(code);
  StatusToPB(status, error->mutable_status());
  // Publish the machine-readable retry classification for external consumers.
  bool is_retryable = false;
  bool needs_resnapshot = false;
  ClassifyCDCErrorCode(code, &is_retryable, &needs_resnapshot);
  error->set_is_retryable(is_retryable);
  error->set_needs_resnapshot(needs_resnapshot);
  // Aggregate roll-up counter (may be null in tests without a metric entity).
  if (errors_) errors_->Increment();
  // Per-code counter: map the enum value to its array slot. A code outside
  // [1, 13] falls back to UNKNOWN_ERROR (index 1) defensively.
  int idx = static_cast<int>(code);
  if (idx < 1 || idx >= static_cast<int>(error_code_counters_.size())) {
    idx = static_cast<int>(CDCErrorPB::UNKNOWN_ERROR);
  }
  if (error_code_counters_[idx]) error_code_counters_[idx]->Increment();
}

int64_t CDCServiceImpl::MaxSentLagMicros() const {
  const int64_t now_micros = GetCurrentTimeMicros();
  int64_t max_lag = 0;
  std::lock_guard lock(lock_);
  for (const auto& entry : stream_tablet_state_) {
    const uint64_t sent =
        entry.second->last_sent_record_phys_micros.load(std::memory_order_relaxed);
    if (sent == 0) {
      continue;
    }
    const int64_t lag = now_micros - static_cast<int64_t>(sent);
    if (lag > max_lag) {
      max_lag = lag;
    }
  }
  return max_lag;
}

int64_t CDCServiceImpl::MaxActiveAgeMicros() const {
  const int64_t now_micros = GetCurrentTimeMicros();
  int64_t max_age = 0;
  std::lock_guard lock(lock_);
  for (const auto& entry : stream_tablet_state_) {
    const int64_t last_active =
        entry.second->last_active_time_micros.load(std::memory_order_relaxed);
    if (last_active == 0) {
      continue;
    }
    const int64_t age = now_micros - last_active;
    if (age > max_age) {
      max_age = age;
    }
  }
  return max_age;
}

uint64_t CDCServiceImpl::ActiveStreamCount() const {
  std::lock_guard lock(lock_);
  return stream_tablet_state_.size();
}

int64_t CDCServiceImpl::MaxOpsBehind() const {
  int64_t max_behind = 0;
  std::lock_guard lock(lock_);
  for (const auto& entry : stream_tablet_state_) {
    const int64_t up_to =
        entry.second->last_known_up_to_op_index.load(std::memory_order_relaxed);
    if (up_to < 0) {
      continue;
    }
    const int64_t ckpt =
        entry.second->checkpoint_op_index.load(std::memory_order_relaxed);
    const int64_t behind = up_to - ckpt;
    if (behind > max_behind) {
      max_behind = behind;
    }
  }
  return max_behind;
}

int64_t CDCServiceImpl::BootstrapRequiredStreamCount() const {
  int64_t count = 0;
  std::lock_guard lock(lock_);
  for (const auto& entry : stream_tablet_state_) {
    const int64_t min_idx =
        entry.second->last_known_min_replicate_index.load(std::memory_order_relaxed);
    const int64_t ckpt =
        entry.second->checkpoint_op_index.load(std::memory_order_relaxed);
    // min_idx <= 0 means no valid WAL data yet; ckpt <= 0 means the consumer
    // has not advanced past the initial position, so we cannot positively assert
    // bootstrap is required.
    if (min_idx <= 0 || ckpt <= 0) {
      continue;
    }
    if (ckpt < min_idx) {
      count++;
    }
  }
  return count;
}

void CDCServiceImpl::SetupSessionMetrics(const CDCStreamTabletKey& key,
                                         CDCStreamTabletState* state) {
  MetricRegistry* registry = server_->metric_registry();
  if (!registry) {
    return;  // No registry (e.g. some tests): skip per-session metrics.
  }
  MetricEntity::AttributeMap attrs;
  attrs["stream_id"] = key.stream_id;
  attrs["tablet_id"] = key.tablet_id;
  // The (stream_id, tablet_id) pair uniquely identifies the session, so it is a
  // stable, unique metric-entity id.
  state->metric_entity = METRIC_ENTITY_cdc_stream.Instantiate(
      registry, Substitute("$0-$1", key.stream_id, key.tablet_id), attrs);

  // The callbacks read only the atomic fields of 'state' -- never lock_ -- so
  // they cannot deadlock against the detacher, which runs while an erase holds
  // lock_. 'state' outlives the gauges because the detacher (a member of
  // 'state', destroyed first) resets them to a constant before the atomics die.
  CDCStreamTabletState* st = state;
  METRIC_cdc_stream_sent_lag_micros.InstantiateFunctionGauge(
      state->metric_entity,
      [st]() -> int64_t {
        const uint64_t sent =
            st->last_sent_record_phys_micros.load(std::memory_order_relaxed);
        if (sent == 0) {
          return 0;
        }
        const int64_t lag = GetCurrentTimeMicros() - static_cast<int64_t>(sent);
        return lag > 0 ? lag : 0;
      })
      ->AutoDetach(&state->metric_detacher);
  METRIC_cdc_stream_active_age_micros.InstantiateFunctionGauge(
      state->metric_entity,
      [st]() -> int64_t {
        const int64_t last_active =
            st->last_active_time_micros.load(std::memory_order_relaxed);
        if (last_active == 0) {
          return 0;
        }
        const int64_t age = GetCurrentTimeMicros() - last_active;
        return age > 0 ? age : 0;
      })
      ->AutoDetach(&state->metric_detacher);
  METRIC_cdc_stream_ops_behind.InstantiateFunctionGauge(
      state->metric_entity,
      [st]() -> int64_t {
        const int64_t up_to =
            st->last_known_up_to_op_index.load(std::memory_order_relaxed);
        if (up_to < 0) {
          return 0;
        }
        const int64_t ckpt =
            st->checkpoint_op_index.load(std::memory_order_relaxed);
        const int64_t diff = up_to - ckpt;
        return diff > 0 ? diff : 0;
      })
      ->AutoDetach(&state->metric_detacher);
  METRIC_cdc_stream_bootstrap_required.InstantiateFunctionGauge(
      state->metric_entity,
      [st]() -> int64_t {
        const int64_t min_idx =
            st->last_known_min_replicate_index.load(std::memory_order_relaxed);
        const int64_t ckpt =
            st->checkpoint_op_index.load(std::memory_order_relaxed);
        if (min_idx <= 0 || ckpt <= 0) {
          return 0;
        }
        return ckpt < min_idx ? 1 : 0;
      })
      ->AutoDetach(&state->metric_detacher);
}

Status CDCServiceImpl::TryAcquireScanSlot(int64_t reserve_bytes) {
  const int32_t cap = FLAGS_cdc_max_concurrent_scans;
  if (cap > 0) {
    // Optimistically claim a slot, then roll back if we overran the cap. This
    // keeps the fast path lock-free while still bounding concurrency.
    if (active_scans_.fetch_add(1, std::memory_order_acq_rel) >= cap) {
      active_scans_.fetch_sub(1, std::memory_order_acq_rel);
      if (scans_rejected_concurrency_) scans_rejected_concurrency_->Increment();
      return Status::ServiceUnavailable(Substitute(
          "at CDC heavy-scan concurrency limit ($0); retry", cap));
    }
  } else {
    active_scans_.fetch_add(1, std::memory_order_acq_rel);
  }

  // Server-wide memory pressure. The CDC-local budget below bounds only what CDC
  // scans consume against each other; it says nothing about the health of the
  // whole server. The tablet read/write path already sheds user requests once
  // process_memory::SoftLimitExceeded() trips (tablet_service.cc), but the CDC
  // scan path historically bypassed that gate -- so a lagging consumer could keep
  // admitting heavy FULL/snapshot scans (up to --cdc_scan_mem_limit_bytes) while
  // the server was already OOM-shedding user reads, tipping it over. Gate CDC on
  // the same server-wide signal. Checked after the concurrency slot is claimed,
  // so roll that slot back on rejection (mirrors the mem-budget branch below).
  double capacity_pct = 0;
  // Evaluate the inject flag first; when it fires, set capacity_pct to 100 so
  // the rejection message is coherent in tests. In production the flag is always
  // false and the real SoftLimitExceeded path is unchanged.
  const bool inject = PREDICT_FALSE(FLAGS_cdc_inject_server_memory_pressure);
  if (inject) {
    capacity_pct = 100;
  }
  if (inject || process_memory::SoftLimitExceeded(&capacity_pct)) {
    active_scans_.fetch_sub(1, std::memory_order_acq_rel);
    if (scans_rejected_server_memory_) scans_rejected_server_memory_->Increment();
    return Status::ServiceUnavailable(Substitute(
        "server soft memory limit exceeded ($0% of capacity); CDC scan deferred",
        capacity_pct));
  }

  if (reserve_bytes > 0) {
    // Soft heap budget: enforce the runtime limit against current CDC scan
    // consumption before reserving. Concurrent acquirers may momentarily
    // overshoot by up to one reservation; that is acceptable for a guard whose
    // point is to shed sustained pressure, not to be an exact allocator. A
    // successful acquire always Consumes, so ReleaseScanSlot always mirrors it.
    const int64_t mem_limit = FLAGS_cdc_scan_mem_limit_bytes;
    if (mem_limit > 0 &&
        scan_mem_tracker_->consumption() + reserve_bytes > mem_limit) {
      active_scans_.fetch_sub(1, std::memory_order_acq_rel);
      if (scans_rejected_memory_) scans_rejected_memory_->Increment();
      return Status::ServiceUnavailable(Substitute(
          "CDC scan memory budget exhausted (limit $0 bytes); retry", mem_limit));
    }
    scan_mem_tracker_->Consume(reserve_bytes);
  }
  return Status::OK();
}

void CDCServiceImpl::ReleaseScanSlot(int64_t reserve_bytes) {
  if (reserve_bytes > 0) {
    scan_mem_tracker_->Release(reserve_bytes);
  }
  active_scans_.fetch_sub(1, std::memory_order_acq_rel);
}

CDCServiceImpl::~CDCServiceImpl() {
  // Release all WAL anchors on shutdown.
  std::lock_guard lock(lock_);
  for (auto& entry : stream_tablet_state_) {
    CDCStreamTabletState* state = entry.second.get();
    // Retrieve the registry via the tablet replica if still available.
    scoped_refptr<TabletReplica> replica;
    const string& tablet_id = entry.first.tablet_id;
    Status s = server_->tablet_manager()->GetTabletReplica(tablet_id, &replica);
    if (s.ok() && replica && replica->log_anchor_registry()) {
      WARN_NOT_OK(replica->log_anchor_registry()->UnregisterIfAnchored(&state->anchor),
                  Substitute("CDC shutdown: failed to unregister anchor for tablet $0",
                             tablet_id));
    }
  }
  // Release all per-tablet retention anchors as well.
  for (auto& entry : retention_anchors_) {
    const string& tablet_id = entry.first;
    scoped_refptr<TabletReplica> replica;
    Status s = server_->tablet_manager()->GetTabletReplica(tablet_id, &replica);
    if (s.ok() && replica && replica->log_anchor_registry()) {
      WARN_NOT_OK(
          replica->log_anchor_registry()->UnregisterIfAnchored(&entry.second->anchor),
          Substitute("CDC shutdown: failed to unregister retention anchor for tablet $0",
                     tablet_id));
    }
  }
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

bool CDCServiceImpl::AuthorizeClientOrServiceUser(const google::protobuf::Message* /*req*/,
                                                  google::protobuf::Message* /*resp*/,
                                                  rpc::RpcContext* context) {
  return server_->Authorize(context,
                            server::ServerBase::SUPER_USER |
                            server::ServerBase::USER |
                            server::ServerBase::SERVICE_USER);
}

// Shrink a client-provided RPC deadline by --cdc_read_safe_deadline_ratio so
// the server stops waiting early enough to serialize and send whatever it has
// before the client's deadline elapses. Returns the client deadline unchanged
// if it is uninitialized (no deadline), already passed, or the ratio is 0.
static MonoTime SafeClientDeadline(const MonoTime& now,
                                   const MonoTime& client_deadline) {
  const double ratio = FLAGS_cdc_read_safe_deadline_ratio;
  if (!client_deadline.Initialized() || ratio <= 0.0 || client_deadline <= now) {
    return client_deadline;
  }
  const int64_t remaining_ns = (client_deadline - now).ToNanoseconds();
  const int64_t safe_ns =
      static_cast<int64_t>(static_cast<double>(remaining_ns) * (1.0 - ratio));
  return now + MonoDelta::FromNanoseconds(safe_ns);
}

// ---------------------------------------------------------------------------
// GetChanges
// ---------------------------------------------------------------------------

void CDCServiceImpl::GetChanges(const GetChangesRequestPB* req,
                                GetChangesResponsePB* resp,
                                rpc::RpcContext* context) {
  const string& stream_id = req->stream_id();
  const string& tablet_id = req->tablet_id();
  const int64_t from_op_index = req->from_op_index();
  const int64_t max_bytes = req->has_max_bytes()
      ? req->max_bytes()
      : FLAGS_cdc_max_bytes_per_response;

  resp->set_tablet_id(tablet_id);
  resp->set_checkpoint_op_index(-1);
  get_changes_requests_->Increment();

  // RPC-worker reservation (B): cap concurrent GetChanges calls at
  // floor((1 - free_ratio) * rpc_num_service_threads) so a burst of CDC
  // consumers cannot occupy every worker in the CDC service pool and starve
  // non-GetChanges traffic (Checkpoint and other control RPCs). This bounds
  // worker-thread occupancy for all modes, including the CHANGE-mode streaming
  // path that has no scan-slot guard. Reserve the slot lock-free and release it
  // on every return path; shed the excess with a retryable SERVER_TOO_BUSY.
  const int32_t inflight = get_changes_inflight_.fetch_add(1) + 1;
  auto release_inflight =
      MakeScopedCleanup([&]() { get_changes_inflight_.fetch_sub(1); });
  {
    const double free_ratio = FLAGS_cdc_get_changes_free_rpc_ratio;
    const int32_t threads = FLAGS_rpc_num_service_threads;
    int32_t cap = static_cast<int32_t>(threads * (1.0 - free_ratio));
    if (cap < 1) {
      cap = 1;  // always admit at least one, even with few threads / high ratio
    }
    if (inflight > cap) {
      if (scans_rejected_worker_pool_) scans_rejected_worker_pool_->Increment();
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY,
                  Status::ServiceUnavailable(Substitute(
                      "too many concurrent CDC GetChanges calls ($0); at most $1 "
                      "run at once (--cdc_get_changes_free_rpc_ratio reserves the "
                      "rest of the $2 RPC worker threads); retry shortly",
                      inflight, cap, threads)),
                  resp->mutable_error());
      context->RespondSuccess();
      return;
    }
  }

  // Fine-grained authorization: require SCAN privilege on the tablet's table.
  if (FLAGS_cdc_enforce_access_control) {
    scoped_refptr<TabletReplica> replica;
    Status s = server_->tablet_manager()->GetTabletReplica(tablet_id, &replica);
    if (!s.ok()) {
      SetCDCError(CDCErrorPB::TABLET_NOT_FOUND, s, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    if (!AuthorizeCDCTableOrRespond(server_->token_verifier(), *req,
                                    replica->tablet_metadata()->table_id(), context)) {
      errors_->Increment();
      return;  // helper already responded at the RPC level
    }
  }

  // Validate from_op_index: it is the last op index the consumer has already
  // consumed, so it must be non-negative (0 means "from the beginning"). A
  // negative value would otherwise be passed through to ReadReplicatesInRange;
  // reject it up front as a client error.
  if (from_op_index < 0) {
    SetCDCError(CDCErrorPB::UNKNOWN_ERROR,
                Status::InvalidArgument(Substitute(
                    "from_op_index must be non-negative, got $0", from_op_index)),
                resp->mutable_error());
    context->RespondSuccess();
    return;
  }

  // Validate the stream is known to the master before serving any data. An
  // authoritative NotFound (the stream was deleted or never existed) is
  // surfaced as STREAM_NOT_FOUND rather than silently served as CHANGE-mode WAL
  // data. A transient fetch failure is tolerated: GetOrFetchStreamConfig serves
  // a stale cached entry when one exists, and with a cold cache we proceed
  // best-effort (assume CHANGE, matching ReadChanges) rather than stalling
  // streaming on a master blip. The result is reused by both the snapshot and
  // streaming branches below and warms the cache for ReadChanges.
  CDCStreamConfigPB config;
  bool have_config = false;
  {
    Status cs = GetOrFetchStreamConfig(stream_id, &config);
    if (cs.IsNotFound()) {
      SetCDCError(CDCErrorPB::STREAM_NOT_FOUND, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    have_config = cs.ok();
  }

  // Snapshot protocol: a request that starts or continues a snapshot is served
  // by ReadSnapshot instead of the WAL-streaming path.
  if (req->is_snapshot_start() || req->has_snapshot_resume_key()) {
    if (have_config && config.snapshot_mode() == CDCStreamConfigPB::NEVER) {
      SetCDCError(CDCErrorPB::UNKNOWN_ERROR,
                  Status::InvalidArgument(
                      "snapshot not allowed for this stream (snapshot_mode=NEVER)"),
                  resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    // Snapshot pages use the smaller snapshot cap so the initial bulk scan
    // produces lighter responses than WAL streaming.
    const int64_t snap_max_bytes = FLAGS_cdc_snapshot_max_bytes_per_response > 0
        ? std::min(max_bytes, FLAGS_cdc_snapshot_max_bytes_per_response)
        : max_bytes;

    // Snapshot scans read the tablet's rowsets, so they are heavy: bound their
    // concurrency and heap against user traffic, shedding as SERVER_TOO_BUSY.
    Status acq = TryAcquireScanSlot(snap_max_bytes);
    if (!acq.ok()) {
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, acq, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    auto release_slot = MakeScopedCleanup([&]() { ReleaseScanSlot(snap_max_bytes); });

    // Derive the snapshot wait deadline from the RPC's own client deadline,
    // capped by --cdc_snapshot_wait_timeout_ms so a caller with a very long (or
    // absent) deadline cannot block a service thread indefinitely (A3).
    const MonoTime snap_now = MonoTime::Now();
    MonoTime snapshot_deadline =
        snap_now + MonoDelta::FromMilliseconds(FLAGS_cdc_snapshot_wait_timeout_ms);
    // Reserve response-build headroom out of the client's remaining budget (B:
    // safe-deadline ratio) before capping, so a partial page is returned in
    // time rather than the whole call timing out.
    const MonoTime snap_client_deadline =
        SafeClientDeadline(snap_now, context->GetClientDeadline());
    if (snap_client_deadline.Initialized() &&
        snap_client_deadline < snapshot_deadline) {
      snapshot_deadline = snap_client_deadline;
    }

    Status ss = ReadSnapshot(stream_id, tablet_id, req->is_snapshot_start(),
                             req->snapshot_resume_key(), snap_max_bytes,
                             snapshot_deadline, resp);
    if (!ss.ok()) {
      if (!resp->has_error()) {
        CDCErrorPB::Code code = CDCErrorPB::UNKNOWN_ERROR;
        if (ss.IsNotFound()) code = CDCErrorPB::TABLET_NOT_FOUND;
        if (ss.IsIllegalState()) code = CDCErrorPB::TABLET_NOT_LEADER;
        if (ss.IsTimedOut()) code = CDCErrorPB::SERVER_TOO_BUSY;  // retryable
        SetCDCError(code, ss, resp->mutable_error());
      }
      context->RespondSuccess();
      return;
    }
    records_produced_->IncrementBy(resp->records_size());
    RecordActivity(stream_id, tablet_id);
    context->RespondSuccess();
    return;
  }

  // INITIAL_ONLY streams bootstrap from a one-time snapshot and must NOT stream
  // the WAL afterward. This request reached the WAL-streaming path (no snapshot
  // flags set), so reject it -- symmetric with the snapshot path rejecting a
  // NEVER stream above. A consumer of an INITIAL_ONLY stream stops after
  // snapshot_done and should never issue a streaming GetChanges.
  if (have_config &&
      config.snapshot_mode() == CDCStreamConfigPB::INITIAL_ONLY) {
    SetCDCError(CDCErrorPB::UNKNOWN_ERROR,
                Status::InvalidArgument(
                    "WAL streaming not allowed for this stream "
                    "(snapshot_mode=INITIAL_ONLY); the snapshot is the only "
                    "output"),
                resp->mutable_error());
    context->RespondSuccess();
    return;
  }

  // FULL-mode streaming reconstructs before/after images by scanning the
  // tablet's MVCC storage, so like snapshots it is a heavy scan: bound its
  // concurrency and heap against user traffic. CHANGE-mode streaming only reads
  // the WAL and is left uncapped. Reuses the config fetched above; on a lookup
  // miss we assume CHANGE (matching ReadChanges).
  const bool is_full_mode =
      have_config && config.record_type() == CDCStreamConfigPB::FULL;
  bool scan_slot_held = false;
  auto release_slot = MakeScopedCleanup([&]() {
    if (scan_slot_held) ReleaseScanSlot(max_bytes);
  });
  if (is_full_mode) {
    Status acq = TryAcquireScanSlot(max_bytes);
    if (!acq.ok()) {
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, acq, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    scan_slot_held = true;
  }

  // Derive the FULL-mode apply-wait deadline from the RPC's own client deadline,
  // capped by --cdc_full_apply_wait_timeout_ms so a caller with a very long (or
  // absent) deadline cannot block a service thread indefinitely.
  const MonoTime apply_now = MonoTime::Now();
  MonoTime apply_deadline =
      apply_now + MonoDelta::FromMilliseconds(FLAGS_cdc_full_apply_wait_timeout_ms);
  // Reserve response-build headroom out of the client's remaining budget (B:
  // safe-deadline ratio) before capping, so a partial batch is returned in
  // time rather than the whole call timing out with nothing.
  const MonoTime apply_client_deadline =
      SafeClientDeadline(apply_now, context->GetClientDeadline());
  if (apply_client_deadline.Initialized() &&
      apply_client_deadline < apply_deadline) {
    apply_deadline = apply_client_deadline;
  }

  const int64_t consumer_schema_version =
      req->has_schema_version() ? static_cast<int64_t>(req->schema_version()) : -1;
  Status s = ReadChanges(stream_id, tablet_id, from_op_index, max_bytes,
                         req->need_schema_info(), consumer_schema_version,
                         apply_deadline, resp);
  if (!s.ok()) {
    // Translate status to a CDCError if not already set. If ReadChanges already
    // set the error (e.g. TABLET_NOT_RUNNING, WAL_EXPIRED), SetCDCError was
    // called and the counters were incremented there; skip it here to avoid
    // double-counting. If no error is set, set it now (handles the
    // TABLET_NOT_LEADER post-read check and other generic failures).
    if (!resp->has_error()) {
      CDCErrorPB::Code code = CDCErrorPB::UNKNOWN_ERROR;
      if (s.IsNotFound()) code = CDCErrorPB::TABLET_NOT_FOUND;
      if (s.IsIllegalState()) code = CDCErrorPB::TABLET_NOT_LEADER;
      SetCDCError(code, s, resp->mutable_error());
    }
    context->RespondSuccess();
    return;
  }

  records_produced_->IncrementBy(resp->records_size());
  RecordActivity(stream_id, tablet_id);
  context->RespondSuccess();
}

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

void CDCServiceImpl::Checkpoint(const CheckpointRequestPB* req,
                                CheckpointResponsePB* resp,
                                rpc::RpcContext* context) {
  const string& stream_id = req->stream_id();
  const string& tablet_id = req->tablet_id();
  const int64_t op_index = req->op_index();
  checkpoint_requests_->Increment();

  scoped_refptr<TabletReplica> replica;
  Status s = server_->tablet_manager()->GetTabletReplica(tablet_id, &replica);
  if (!s.ok()) {
    SetCDCError(CDCErrorPB::TABLET_NOT_FOUND, s, resp->mutable_error());
    context->RespondSuccess();
    return;
  }

  // Fine-grained authorization: require SCAN privilege on the tablet's table.
  if (FLAGS_cdc_enforce_access_control &&
      !AuthorizeCDCTableOrRespond(server_->token_verifier(), *req,
                                  replica->tablet_metadata()->table_id(), context)) {
    errors_->Increment();
    return;  // helper already responded at the RPC level
  }

  // Advance the WAL anchor to op_index, allowing GC of earlier segments.
  s = UpdateAnchor(stream_id, tablet_id, op_index, replica->log_anchor_registry().get());
  if (!s.ok()) {
    SetCDCError(CDCErrorPB::UNKNOWN_ERROR, s, resp->mutable_error());
    context->RespondSuccess();
    return;
  }

  // Update in-memory checkpoint. Also read the FULL-mode history floor so it can
  // be forwarded to the master along with the checkpoint, and decide whether this
  // checkpoint should be persisted durably to the master now or throttled.
  uint64_t history_safe_time_micros = 0;
  bool persist = true;
  {
    const int64_t now_micros = GetCurrentTimeMicros();
    const int64_t persist_interval_micros =
        FLAGS_cdc_checkpoint_persist_interval_ms > 0
            ? FLAGS_cdc_checkpoint_persist_interval_ms * 1000 : 0;
    std::lock_guard lock(lock_);
    CDCStreamTabletKey key{stream_id, tablet_id};
    auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end()) {
      it->second->checkpoint_op_index.store(op_index, std::memory_order_relaxed);
      history_safe_time_micros = it->second->cdc_min_history_ts_micros;
      it->second->last_active_time_micros.store(now_micros,
                                                std::memory_order_relaxed);
      // Throttle durable persistence to at most once per interval per session.
      // The WAL anchor was already advanced above (in-memory), so log GC is
      // unaffected; throttling only lets the master's persisted checkpoint lag,
      // which is safe (retention stays conservative). A brand-new session
      // (last_checkpoint_persist_micros == 0) always persists.
      if (persist_interval_micros > 0 &&
          it->second->last_checkpoint_persist_micros != 0 &&
          now_micros - it->second->last_checkpoint_persist_micros <
              persist_interval_micros) {
        persist = false;
      } else {
        it->second->last_checkpoint_persist_micros = now_micros;
      }
    }
  }

  // Respond to the consumer before the (potentially slower) durable persist to
  // the master, so consumer checkpoint latency is not tied to a master round-trip.
  context->RespondSuccess();

  // Best-effort durable persistence to the master, throttled per session. Copies
  // are taken because the request PB is only guaranteed valid until this handler
  // returns.
  if (persist) {
    checkpoint_persists_->Increment();
    PersistCheckpoint(string(stream_id), string(tablet_id), op_index,
                      history_safe_time_micros);
  }
}

namespace {

// Returns the WAL index of the first write of the oldest transaction that is
// still open at the end of 'replicates' -- i.e. a transaction that has a
// WRITE_OP carrying a txn_id but no matching FINALIZE_COMMIT or ABORT_TXN within
// the window. Returns int64_t max if no transaction is left open. This mirrors
// the txn_first_index bookkeeping that the full decode below performs, but reads
// op types only (no row decoding, no before/after-image reconstruction), so it
// is cheap enough to run on read windows that may be discarded and re-read at a
// larger byte cap.
int64_t OldestOpenTxnFirstIndex(const vector<ReplicateMsg*>& replicates) {
  std::map<int64_t, int64_t> txn_first_index;
  for (const ReplicateMsg* r : replicates) {
    if (r->op_type() == consensus::WRITE_OP && r->write_request().has_txn_id()) {
      txn_first_index.try_emplace(r->write_request().txn_id(), r->id().index());
    } else if (r->op_type() == consensus::PARTICIPANT_OP &&
               r->has_participant_request() &&
               r->participant_request().has_op()) {
      const auto& pop = r->participant_request().op();
      if (pop.type() == tserver::ParticipantOpPB::FINALIZE_COMMIT ||
          pop.type() == tserver::ParticipantOpPB::ABORT_TXN) {
        txn_first_index.erase(pop.txn_id());
      }
    }
  }
  int64_t open_min = std::numeric_limits<int64_t>::max();
  for (const auto& e : txn_first_index) {
    open_min = std::min(open_min, e.second);
  }
  return open_min;
}

}  // anonymous namespace

// ---------------------------------------------------------------------------
// ReadChanges (internal)
// ---------------------------------------------------------------------------

Status CDCServiceImpl::ReadChanges(const string& stream_id,
                                   const string& tablet_id,
                                   int64_t from_op_index,
                                   int64_t max_bytes,
                                   bool need_schema_info,
                                   int64_t consumer_schema_version,
                                   const MonoTime& deadline,
                                   GetChangesResponsePB* resp) {
  // 1. Locate the tablet replica.
  scoped_refptr<TabletReplica> replica;
  RETURN_NOT_OK_PREPEND(
      server_->tablet_manager()->GetTabletReplica(tablet_id, &replica),
      Substitute("tablet $0 not found on this server", tablet_id));

  // 1b. The replica exists but may still be bootstrapping / catching up and not
  // yet RUNNING (e.g. just after startup or a tablet copy). Distinguish this
  // from "not the leader" so the consumer gets an accurate, retryable
  // TABLET_NOT_RUNNING instead of a misleading TABLET_NOT_LEADER /
  // TABLET_NOT_FOUND. Set the CDC error directly: CheckRunning returns
  // IllegalState, which GetChanges' status-based translation would otherwise
  // reclassify as TABLET_NOT_LEADER.
  {
    Status rs = replica->CheckRunning();
    if (PREDICT_FALSE(FLAGS_cdc_inject_tablet_not_running) && rs.ok()) {
      rs = Status::IllegalState("injected: tablet not in a running state");
    }
    if (!rs.ok()) {
      SetCDCError(CDCErrorPB::TABLET_NOT_RUNNING, rs, resp->mutable_error());
      return rs;
    }
  }

  // 2. Verify this replica is the Raft leader, and remember the leader term so
  // the read can be re-validated after the scan (see the post-read recheck at
  // the end of this function).
  std::shared_ptr<consensus::RaftConsensus> consensus = replica->shared_consensus();
  if (!consensus) {
    return Status::IllegalState(
        Substitute("tablet $0 has no consensus instance", tablet_id));
  }
  const int64_t leader_term = consensus->CurrentTerm();
  {
    RaftPeerPB::Role role = consensus->role();
    if (role != RaftPeerPB::LEADER) {
      return Status::IllegalState(
          Substitute("tablet $0 is not the leader (role: $1)", tablet_id,
                     RaftPeerPB::Role_Name(role)));
    }
  }

  // E2: reject a consumer that declared (via req.schema_version) that it is
  // decoding against a schema version older than the tablet's current schema.
  // An ALTER has landed that the consumer never saw, so it would silently decode
  // the new columns in the WAL records below as hex. Returning
  // SCHEMA_VERSION_MISMATCH (retryable, not a re-snapshot) tells the consumer to
  // re-issue with need_schema_info=true and refresh its layout.
  //   - Skipped when need_schema_info is set: the current schema is prepended
  //     just below, so the consumer is already refreshing.
  //   - Baseline is the APPLIED schema version (tablet_metadata()->schema_version()),
  //     the same "current schema" the need_schema_info path sends and the version
  //     the consumer would refresh to. Committed-but-unapplied ALTERs are
  //     deliberately NOT treated as a mismatch here: their records have not been
  //     produced yet, and the pre-ALTER records still decode correctly against
  //     the older version the consumer holds.
  // Emitted before any record is added, so the error response carries no records.
  if (consumer_schema_version >= 0 && !need_schema_info) {
    const int64_t current_schema_version =
        static_cast<int64_t>(replica->tablet_metadata()->schema_version());
    if (consumer_schema_version < current_schema_version) {
      Status s = Status::InvalidArgument(Substitute(
          "consumer schema version $0 is older than the tablet's current schema "
          "version $1; re-request with need_schema_info=true to refresh the schema",
          consumer_schema_version, current_schema_version));
      SetCDCError(CDCErrorPB::SCHEMA_VERSION_MISMATCH, s, resp->mutable_error());
      return s;
    }
  }

  // On request, prepend a synthetic DDL record carrying the tablet's current
  // schema so a consumer attaching mid-stream (e.g. after an ALTER it never saw)
  // has a base schema to decode subsequent records against. The record reuses
  // from_op_index and does not advance checkpoint_op_index (that stays driven by
  // the real WAL records below), so re-requesting is idempotent.
  if (need_schema_info) {
    const uint32_t schema_version = replica->tablet_metadata()->schema_version();
    CDCRecordPB* ddl = resp->add_records();
    ddl->set_op_type(DDL);
    ddl->set_op_index(from_op_index);
    ddl->set_schema_version(schema_version);
    ddl->set_new_schema_version(schema_version);
    RETURN_NOT_OK(SchemaToPB(*replica->tablet_metadata()->schema(),
                             ddl->mutable_new_schema()));
  }

  // Determine the record type. FULL streams reconstruct complete before/after
  // images from the tablet's MVCC storage; CHANGE streams use the WAL only.
  bool is_full_mode = false;
  {
    CDCStreamConfigPB config;
    Status cs = GetOrFetchStreamConfig(stream_id, &config);
    if (cs.ok()) {
      is_full_mode = (config.record_type() == CDCStreamConfigPB::FULL);
    } else {
      KLOG_EVERY_N_SECS(WARNING, 60)
          << "CDC: could not fetch config for stream " << stream_id << ": "
          << cs.ToString() << "; proceeding in CHANGE mode";
    }
  }

  // 3. Determine the upper bound (last committed op index on this leader).
  int64_t up_to_op_index;
  {
    std::optional<OpId> last_committed =
        consensus->GetLastOpId(consensus::COMMITTED_OPID);
    if (!last_committed) {
      // No committed ops yet; return empty batch.
      return Status::OK();
    }
    up_to_op_index = last_committed->index();
  }

  if (from_op_index >= up_to_op_index) {
    // Consumer is already caught up; return an empty batch but report the
    // current committed index so the consumer knows it is current (and so a
    // just-completed snapshot can capture a streaming start point).
    resp->set_checkpoint_op_index(up_to_op_index);
    return Status::OK();
  }

  // 4. Update the WAL anchor so segments in [from_op_index, ...] are not GC'd.
  RETURN_NOT_OK(UpdateAnchor(stream_id, tablet_id, from_op_index,
                             replica->log_anchor_registry().get()));

  // 5. Read ReplicateMsgs from the WAL via LogReader.
  Log* log = replica->log();
  if (!log) {
    return Status::IllegalState(
        Substitute("tablet $0 log is not available", tablet_id));
  }
  std::shared_ptr<LogReader> reader = log->reader();
  if (!reader) {
    return Status::IllegalState(
        Substitute("tablet $0 WAL reader is not available", tablet_id));
  }

  // A1: a transaction is only visible to consumers at its FINALIZE_COMMIT, so
  // its entire WAL span (first write .. commit) must be read in one window to be
  // emitted. If that span exceeds the byte cap, the commit never enters the read
  // window: the transaction stays open, open_min is pinned at its first write,
  // and checkpoint_op_index is stuck at open_min - 1 on every call. The stream
  // wedges permanently.
  //
  // Detect the wedge -- the oldest open transaction starts at the very first op
  // we can read (so the checkpoint cannot advance at all) and the read was cut
  // short by the byte cap (so the commit may lie just beyond the window) -- and
  // escalate the effective read cap so the commit comes into view. Bound the
  // escalation by --cdc_max_transaction_span_bytes; a transaction larger than
  // that cannot be emitted and fails loudly with TRANSACTION_TOO_LARGE rather
  // than stalling silently.
  //
  // Only truncated-by-cap reads escalate: if the window already reached the
  // committed watermark (up_to_op_index) with the transaction still open, the
  // transaction simply has not committed yet -- that is normal back-pressure,
  // not a wedge, and is left to resolve on a later call.
  const int64_t span_cap = std::max(FLAGS_cdc_max_transaction_span_bytes, max_bytes);
  vector<ReplicateMsg*> replicates;
  int64_t effective_max_bytes = max_bytes;
  while (true) {
    vector<ReplicateMsg*> batch;
    Status s = reader->ReadReplicatesInRange(
        from_op_index + 1,  // exclusive lower bound → inclusive starting_at
        up_to_op_index,
        effective_max_bytes,
        &batch);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        // The requested index has been GC'd. Disambiguate the cause for the
        // consumer: if this session has been idle beyond the stream-expiry
        // window, the gap is permanent and the consumer must re-bootstrap from a
        // fresh snapshot (STREAM_EXPIRED); otherwise the GC may be transient
        // (e.g. a barrier gap during a failover) and the consumer can retry
        // (WAL_EXPIRED). Checked only here, at the moment the WAL is found
        // missing -- never proactively -- so a still-served stream is never
        // reported expired.
        const CDCErrorPB::Code code = StreamIdleExpired(stream_id, tablet_id)
            ? CDCErrorPB::STREAM_EXPIRED : CDCErrorPB::WAL_EXPIRED;
        SetCDCError(code, s, resp->mutable_error());
      }
      for (ReplicateMsg* r : batch) delete r;
      return s;
    }

    const bool truncated_by_cap =
        !batch.empty() && batch.back()->id().index() < up_to_op_index;
    const int64_t open_first = OldestOpenTxnFirstIndex(batch);
    // No checkpoint progress is possible when the oldest open transaction starts
    // at the first op in the window (checkpoint would be open_first - 1 ==
    // from_op_index).
    const bool wedged = open_first != std::numeric_limits<int64_t>::max() &&
                        truncated_by_cap &&
                        open_first == from_op_index + 1;
    if (wedged && effective_max_bytes > 0 && effective_max_bytes < span_cap) {
      for (ReplicateMsg* r : batch) delete r;
      // Grow the window (doubling, clamped to the span cap) and retry so the
      // open transaction's commit can be reached.
      effective_max_bytes = std::min(span_cap, effective_max_bytes * 2);
      continue;
    }
    if (wedged) {
      for (ReplicateMsg* r : batch) delete r;
      Status too_large = Status::IllegalState(Substitute(
          "open transaction starting at op index $0 on tablet $1 spans more than "
          "--cdc_max_transaction_span_bytes ($2 bytes); the CDC checkpoint cannot "
          "advance past it. Raise --cdc_max_transaction_span_bytes to emit it.",
          open_first, tablet_id, span_cap));
      LOG(WARNING) << "CDC: " << too_large.ToString();
      SetCDCError(CDCErrorPB::TRANSACTION_TOO_LARGE, too_large, resp->mutable_error());
      return too_large;
    }
    replicates.swap(batch);
    break;
  }

  // Progress signal: if the read stopped before the committed watermark, the
  // byte budget cut it short and more records are immediately available, so the
  // consumer should keep polling now rather than backing off. Reaching
  // up_to_op_index (or an empty batch) means it is caught up. Records deferred
  // by an open transaction are deliberately NOT reported as "more" here: those
  // will not appear on an immediate re-poll until the transaction commits, so
  // treating them as back-pressure (poll normally) is correct.
  const bool have_more_records =
      !replicates.empty() && replicates.back()->id().index() < up_to_op_index;
  resp->set_have_more_records(have_more_records);

  // For FULL streams, pin the tablet's MVCC history at the oldest op timestamp
  // in this batch so before-images remain reconstructable, and remember the
  // floor so it can be forwarded to the master (and thus to followers) on the
  // next Checkpoint.
  if (is_full_mode && !replicates.empty()) {
    uint64_t batch_min_ts = std::numeric_limits<uint64_t>::max();
    for (const ReplicateMsg* r : replicates) {
      if (r->has_timestamp()) {
        batch_min_ts = std::min(batch_min_ts, static_cast<uint64_t>(r->timestamp()));
      }
    }
    if (batch_min_ts != std::numeric_limits<uint64_t>::max()) {
      if (auto tablet = replica->shared_tablet()) {
        tablet->SetCDCHistoryFloor(Timestamp(batch_min_ts));
      }
      const uint64_t floor_micros =
          HybridClock::GetPhysicalValueMicros(Timestamp(batch_min_ts));
      std::lock_guard lock(lock_);
      CDCStreamTabletKey key{stream_id, tablet_id};
      auto it = stream_tablet_state_.find(key);
      if (it != stream_tablet_state_.end()) {
        it->second->cdc_min_history_ts_micros = floor_micros;
      }
    }
  }

  // 6. Determine the schema version in effect at the start of this batch. The
  //    decode loop below advances running_schema_version at each ALTER it
  //    processes (using that ALTER op's own recorded new version), so here we
  //    only need the base: the version in effect for the batch's ops that
  //    precede its first ALTER.
  //
  //    E9: the base must NOT be derived from tablet_metadata()->schema_version()
  //    minus the number of ALTERs in the batch. That metadata version reflects
  //    only *applied* ops, which can lag the committed watermark (up_to_op_index)
  //    that bounds this batch: apply happens asynchronously after commit. If an
  //    ALTER (N -> N+1) inside the window is committed but not yet applied, the
  //    metadata still reads N while the batch already contains the ALTER, so the
  //    backward computation yields N-1 and every pre-ALTER WRITE gets stamped
  //    N-1. A consumer decoding those rows with the N-1 schema would drop the
  //    column(s) that the N-1 -> N ALTER had already added. Instead, when the
  //    batch contains an ALTER, take the base straight from the WAL: the first
  //    ALTER records its new version W, so the version in effect before it is
  //    W - 1. This is independent of apply progress. Only when the batch has no
  //    ALTER at all (every op shares a single version) do we fall back to the
  //    applied metadata version, which is exact in that case.
  int64_t running_schema_version = -1;
  for (const ReplicateMsg* replicate : replicates) {
    if (replicate->op_type() == consensus::ALTER_SCHEMA_OP &&
        replicate->has_alter_schema_request()) {
      running_schema_version =
          static_cast<int64_t>(replicate->alter_schema_request().schema_version()) - 1;
      break;
    }
  }
  if (running_schema_version < 0) {
    running_schema_version =
        static_cast<int64_t>(replica->tablet_metadata()->schema_version());
  }

  // 7. Decode ReplicateMsgs. Transactional writes (those carrying a txn_id) are
  //    buffered per transaction and only emitted when the transaction's
  //    FINALIZE_COMMIT is seen (dropped on ABORT_TXN), so uncommitted or aborted
  //    data is never published. Each emit group carries an "effective index":
  //    the op index at which it becomes visible (the commit index for a
  //    transaction, otherwise the op's own index).
  struct EmitGroup {
    int64_t effective_index;
    vector<CDCRecordPB> records;
  };
  vector<EmitGroup> emit_groups;
  std::map<int64_t, vector<CDCRecordPB>> txn_buffers;   // txn_id -> buffered rows
  std::map<int64_t, int64_t> txn_first_index;           // txn_id -> first write index

  // FULL mode: transactional writes become visible at their commit timestamp, so
  // their before/after images must be read at commit time. Retain each write op
  // (with its rows decoded at write time for a stable schema_version) until the
  // FINALIZE_COMMIT is seen.
  struct FullTxnOp {
    unique_ptr<ReplicateMsg> replicate;
    vector<CDCRecordPB> rows;
  };
  std::map<int64_t, vector<FullTxnOp>> txn_full_ops;    // txn_id -> deferred ops

  int64_t last_index = from_op_index;
  for (ReplicateMsg* replicate : replicates) {
    unique_ptr<ReplicateMsg> owned(replicate);
    last_index = replicate->id().index();
    const int64_t op_index = replicate->id().index();

    if (replicate->op_type() == consensus::WRITE_OP) {
      vector<CDCRecordPB> row_records;
      Status s = DecodeWriteOpAllRows(
          *replicate, static_cast<int32_t>(running_schema_version), &row_records);
      if (!s.ok()) {
        if (!s.IsAborted()) {
          LOG(WARNING) << "CDC: failed to decode WRITE_OP at op_index=" << op_index
                       << " for tablet " << tablet_id << ": " << s.ToString();
        }
        continue;
      }
      const bool is_txn = replicate->write_request().has_txn_id();
      if (is_txn) {
        const int64_t txn_id = replicate->write_request().txn_id();
        if (txn_first_index.find(txn_id) == txn_first_index.end()) {
          txn_first_index[txn_id] = op_index;
        }
        if (is_full_mode) {
          // Defer image reconstruction to commit time; keep rows + the replicate.
          txn_full_ops[txn_id].push_back(
              FullTxnOp{std::move(owned), std::move(row_records)});
        } else {
          auto& buf = txn_buffers[txn_id];
          for (auto& r : row_records) {
            buf.emplace_back(std::move(r));
          }
        }
      } else {
        if (is_full_mode) {
          Status rs = ReconstructBeforeAfterImages(
              replica->shared_tablet().get(), replica->time_manager(), *replicate,
              static_cast<uint64_t>(replicate->timestamp()), deadline, &row_records);
          if (!rs.ok()) {
            if (rs.IsIncomplete()) {
              SetCDCError(CDCErrorPB::HISTORY_EXPIRED, rs, resp->mutable_error());
              return rs;
            }
            // Any other failure means we cannot produce a correct before/after
            // image: a timeout/unavailable because committed ops were not applied
            // in time (most likely right after a leader change), or a transient
            // IOError/Corruption during the historical scan. Abort the batch
            // rather than emit a truncated or possibly-wrong record with no
            // old_changes; the consumer re-polls from its last checkpoint.
            LOG(WARNING) << "CDC: FULL image reconstruction failed at op_index="
                         << op_index << " for tablet " << tablet_id << ": "
                         << rs.ToString();
            return rs;
          }
        }
        emit_groups.push_back({op_index, std::move(row_records)});
      }
    } else if (replicate->op_type() == consensus::PARTICIPANT_OP &&
               replicate->has_participant_request() &&
               replicate->participant_request().has_op()) {
      const auto& pop = replicate->participant_request().op();
      const int64_t txn_id = pop.txn_id();
      if (pop.type() == tserver::ParticipantOpPB::FINALIZE_COMMIT) {
        const int64_t commit_ts = pop.has_finalized_commit_timestamp()
            ? pop.finalized_commit_timestamp()
            : static_cast<int64_t>(replicate->timestamp());
        vector<CDCRecordPB> group;
        CDCRecordPB begin;
        begin.set_op_type(CDCOpTypePB::BEGIN);
        begin.set_op_index(op_index);
        begin.set_op_term(replicate->id().term());
        begin.set_commit_timestamp(commit_ts);
        begin.set_txn_id(std::to_string(txn_id));
        group.emplace_back(std::move(begin));

        if (is_full_mode) {
          // Reconstruct before/after images at the commit timestamp, since the
          // transaction's writes become visible only at commit.
          auto fit = txn_full_ops.find(txn_id);
          if (fit != txn_full_ops.end()) {
            for (auto& fop : fit->second) {
              Status rs = ReconstructBeforeAfterImages(
                  replica->shared_tablet().get(), replica->time_manager(), *fop.replicate,
                  static_cast<uint64_t>(commit_ts), deadline, &fop.rows);
              if (!rs.ok()) {
                if (rs.IsIncomplete()) {
                  SetCDCError(CDCErrorPB::HISTORY_EXPIRED, rs, resp->mutable_error());
                  return rs;
                }
                // Any other failure means we cannot produce a correct
                // before/after image: a timeout/unavailable because committed
                // ops were not applied in time (most likely right after a leader
                // change), or a transient IOError/Corruption during the
                // historical scan. Abort the batch rather than emit a truncated
                // or possibly-wrong record with no old_changes; the consumer
                // re-polls from its last checkpoint.
                LOG(WARNING) << "CDC: FULL image reconstruction failed for txn "
                             << txn_id << " at commit op_index=" << op_index
                             << " for tablet " << tablet_id << ": " << rs.ToString();
                return rs;
              }
              for (auto& r : fop.rows) {
                r.set_commit_timestamp(commit_ts);
                group.emplace_back(std::move(r));
              }
            }
            txn_full_ops.erase(fit);
          }
        } else {
          auto it = txn_buffers.find(txn_id);
          if (it != txn_buffers.end()) {
            for (auto& r : it->second) {
              r.set_commit_timestamp(commit_ts);
              group.emplace_back(std::move(r));
            }
            txn_buffers.erase(it);
          }
        }
        txn_first_index.erase(txn_id);

        CDCRecordPB commit;
        commit.set_op_type(CDCOpTypePB::COMMIT);
        commit.set_op_index(op_index);
        commit.set_op_term(replicate->id().term());
        commit.set_commit_timestamp(commit_ts);
        commit.set_txn_id(std::to_string(txn_id));
        group.emplace_back(std::move(commit));

        emit_groups.push_back({op_index, std::move(group)});
      } else if (pop.type() == tserver::ParticipantOpPB::ABORT_TXN) {
        // Drop any buffered rows for the aborted transaction.
        txn_buffers.erase(txn_id);
        txn_full_ops.erase(txn_id);
        txn_first_index.erase(txn_id);
      }
      // BEGIN_TXN / BEGIN_COMMIT / GET_METADATA produce no CDC output.
    } else if (replicate->op_type() == consensus::ALTER_SCHEMA_OP) {
      CDCRecordPB record;
      Status s = DecodeNonWriteReplicateMsg(*replicate, &record);
      if (s.ok()) {
        // Subsequent records are governed by the post-alter schema version.
        running_schema_version = record.new_schema_version();
        vector<CDCRecordPB> g;
        g.emplace_back(std::move(record));
        emit_groups.push_back({op_index, std::move(g)});
      }
    }
    // Other op types (NO_OP, etc.) produce no CDC records.
  }

  // The oldest still-open transaction pins the stream: nothing at or after its
  // first write may be emitted, and the checkpoint may not advance past it (so
  // a later batch re-reads and can emit the transaction atomically once it
  // commits). The result is committed rows only, grouped by transaction in
  // commit order, with no partial or aborted transactions.
  int64_t open_min = std::numeric_limits<int64_t>::max();
  for (const auto& e : txn_first_index) {
    open_min = std::min(open_min, e.second);
  }

  for (auto& group : emit_groups) {
    if (group.effective_index >= open_min) {
      continue;  // deferred until the older open transaction commits
    }
    for (auto& r : group.records) {
      *resp->add_records() = std::move(r);
    }
  }

  if (open_min != std::numeric_limits<int64_t>::max()) {
    resp->set_checkpoint_op_index(open_min - 1);
  } else {
    resp->set_checkpoint_op_index(
        last_index > from_op_index ? last_index : up_to_op_index);
  }

  // Record the physical time of the newest record sent (for the sent-lag
  // metric), the tablet's committed upper bound, and the WAL's earliest retained
  // index (for the ops-behind and bootstrap-required metrics).
  uint64_t newest_phys_micros = 0;
  for (const auto& r : resp->records()) {
    if (r.has_timestamp()) {
      newest_phys_micros = std::max(
          newest_phys_micros,
          HybridClock::GetPhysicalValueMicros(Timestamp(r.timestamp())));
    }
  }
  const int64_t min_replicate_index = reader->GetMinReplicateIndex();
  {
    std::lock_guard lock(lock_);
    CDCStreamTabletKey key{stream_id, tablet_id};
    auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end()) {
      if (newest_phys_micros > 0) {
        it->second->last_sent_record_phys_micros.store(newest_phys_micros,
                                                       std::memory_order_relaxed);
      }
      it->second->last_known_up_to_op_index.store(up_to_op_index,
                                                  std::memory_order_relaxed);
      it->second->last_known_min_replicate_index.store(min_replicate_index,
                                                       std::memory_order_relaxed);
    }
  }

  // Post-read leader-term recheck: if this replica lost leadership or the term
  // advanced between the initial check and here, the batch we just assembled
  // was read from a log the new leader may have since diverged from (a
  // leadership change can truncate uncommitted tail ops). Reject the read as
  // TABLET_NOT_LEADER (via the IllegalState mapping) so the consumer
  // rediscovers the current leader and retries, rather than acting on a batch
  // that could be rolled back. The consumer's checkpoint has not advanced, so
  // the retry is idempotent.
  if (PREDICT_FALSE(FLAGS_cdc_inject_post_read_leadership_loss) ||
      consensus->role() != RaftPeerPB::LEADER ||
      consensus->CurrentTerm() != leader_term) {
    return Status::IllegalState(Substitute(
        "tablet $0 leadership changed during the read (term $1 -> $2); retry "
        "against the current leader",
        tablet_id, leader_term, consensus->CurrentTerm()));
  }

  return Status::OK();
}

std::shared_ptr<std::mutex> CDCServiceImpl::GetSnapshotStartLock(
    const CDCStreamTabletKey& key) {
  std::lock_guard lock(lock_);
  auto& slot = snapshot_start_locks_[key];
  if (!slot) {
    slot = std::make_shared<std::mutex>();
  }
  return slot;
}

std::shared_ptr<std::mutex> CDCServiceImpl::GetStreamConfigFetchLock(
    const string& stream_id) {
  std::lock_guard lock(lock_);
  auto& slot = stream_config_fetch_locks_[stream_id];
  if (!slot) {
    slot = std::make_shared<std::mutex>();
  }
  return slot;
}

// ---------------------------------------------------------------------------
// ReadSnapshot (internal) - server-driven consistent snapshot (Phase 5)
// ---------------------------------------------------------------------------

Status CDCServiceImpl::ReadSnapshot(const string& stream_id,
                                    const string& tablet_id,
                                    bool is_start,
                                    const string& req_resume_key,
                                    int64_t max_bytes,
                                    const MonoTime& deadline,
                                    GetChangesResponsePB* resp) {
  // 1. Locate the replica and verify leadership.
  scoped_refptr<TabletReplica> replica;
  RETURN_NOT_OK_PREPEND(
      server_->tablet_manager()->GetTabletReplica(tablet_id, &replica),
      Substitute("tablet $0 not found on this server", tablet_id));
  {
    std::shared_ptr<consensus::RaftConsensus> consensus = replica->shared_consensus();
    if (!consensus) {
      return Status::IllegalState(
          Substitute("tablet $0 has no consensus instance", tablet_id));
    }
    if (consensus->role() != RaftPeerPB::LEADER) {
      return Status::IllegalState(
          Substitute("tablet $0 is not the leader", tablet_id));
    }
  }
  std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
  if (!tablet) {
    return Status::IllegalState(Substitute("tablet $0 not available", tablet_id));
  }

  const CDCStreamTabletKey key{stream_id, tablet_id};

  // A2: serialize the start-decision + establish sequence per (stream, tablet).
  // Two concurrent is_snapshot_start calls would otherwise both observe "no
  // active session", both run the establish path, and the second would overwrite
  // the first's snap_ts / streaming_start_op_index -- corrupting a snapshot whose
  // first pages were already read at the original timestamp. The establish work
  // blocks, so it is guarded by this per-key mutex rather than lock_. The scan
  // (steps 3-7) runs after the lock is released; once a session is active a
  // concurrent start observes it and continues instead of restarting.
  bool start_new = is_start;
  {
    std::shared_ptr<std::mutex> start_lock = GetSnapshotStartLock(key);
    std::lock_guard<std::mutex> start_guard(*start_lock);

    // 2. Decide whether to begin a fresh snapshot. A snapshot already in
    //    progress (state.snapshot.active) is continued rather than restarted.
    bool has_active_session = false;
    {
      std::lock_guard lock(lock_);
      auto it = stream_tablet_state_.find(key);
      if (it != stream_tablet_state_.end() && it->second->snapshot.active) {
        has_active_session = true;
        start_new = false;
      }
    }

    // E4: a resume_key is only valid while continuing the active snapshot session
    // on this same leader -- the session is what fixes snap_ts. If the client
    // presents a resume_key but this server has no active session (the common
    // cause being a leader change that discarded the in-memory session, but also a
    // client that mispaired is_snapshot_start=true with a stale resume key),
    // honoring the key would scan the table tail (rows > resume_key) at a freshly
    // chosen snap_ts while the head (rows <= resume_key) was read at the original
    // snap_ts. That is not a self-consistent snapshot. Refuse and make the
    // consumer restart from the beginning via a dedicated error code rather than
    // silently rescanning at a new timestamp.
    if (!req_resume_key.empty() && !has_active_session) {
      Status s = Status::IllegalState(
          "snapshot session not found on this server (leader change?); restart "
          "the snapshot from the beginning with is_snapshot_start=true and no "
          "resume key");
      SetCDCError(CDCErrorPB::SNAPSHOT_SESSION_LOST, s, resp->mutable_error());
      return s;
    }

    if (start_new) {
      // Test hook: widen the establish window (for the A2 race) and let a test
      // exercise the deadline (A3). If the injected latency would exceed the
      // effective deadline, fail as the real waits below would.
      if (PREDICT_FALSE(FLAGS_cdc_inject_latency_before_snapshot_establish_ms > 0)) {
        const MonoDelta injected = MonoDelta::FromMilliseconds(
            FLAGS_cdc_inject_latency_before_snapshot_establish_ms);
        const MonoTime wake = MonoTime::Now() + injected;
        if (deadline.Initialized() && deadline < wake) {
          const MonoDelta remaining = deadline - MonoTime::Now();
          if (remaining.ToNanoseconds() > 0) {
            SleepFor(remaining);
          }
          return Status::TimedOut(Substitute(
              "snapshot start exceeded deadline (injected $0ms latency)",
              FLAGS_cdc_inject_latency_before_snapshot_establish_ms));
        }
        SleepFor(injected);
      }

      // Capture the snapshot timestamp and the WAL streaming start point, wait
      // for the snapshot to be clean, and pin the tablet's MVCC history at
      // snap_ts.
      Timestamp snap_ts = replica->clock()->Now();
      int64_t streaming_start_op_index = 0;
      if (auto last = replica->shared_consensus()->GetLastOpId(consensus::COMMITTED_OPID)) {
        streaming_start_op_index = last->index();
      }
      RETURN_NOT_OK(replica->time_manager()->WaitUntilSafe(snap_ts, deadline));
      tablet::MvccSnapshot ignored;
      RETURN_NOT_OK(tablet->mvcc_manager()->WaitForSnapshotWithAllApplied(
          snap_ts, &ignored, deadline));
      tablet->SetCDCHistoryFloor(snap_ts);
      // Retain WAL from the streaming start point on this leader for the scan.
      RETURN_NOT_OK(UpdateAnchor(stream_id, tablet_id, streaming_start_op_index,
                                 replica->log_anchor_registry().get()));

      std::lock_guard lock(lock_);
      auto it = stream_tablet_state_.find(key);
      CDCStreamTabletState* state;
      if (it == stream_tablet_state_.end()) {
        auto ns = std::make_unique<CDCStreamTabletState>();
        ns->anchor_owner = Substitute("CDC[stream=$0]", stream_id);
        state = ns.get();
        stream_tablet_state_.emplace(key, std::move(ns));
        SetupSessionMetrics(key, state);
      } else {
        state = it->second.get();
      }
      state->snapshot.active = true;
      state->snapshot.snap_ts = snap_ts;
      state->snapshot.streaming_start_op_index = streaming_start_op_index;
      state->snapshot.resume_key.clear();
      state->cdc_min_history_ts_micros = HybridClock::GetPhysicalValueMicros(snap_ts);
      snapshot_sessions_established_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  // 3. Load the (now-established) snapshot session state.
  Timestamp snap_ts;
  int64_t streaming_start_op_index = 0;
  string resume_key;
  {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(key);
    if (it == stream_tablet_state_.end() || !it->second->snapshot.active) {
      return Status::IllegalState(
          "no active snapshot; call GetChanges with is_snapshot_start=true");
    }
    snap_ts = it->second->snapshot.snap_ts;
    streaming_start_op_index = it->second->snapshot.streaming_start_op_index;
    // E10: resume from the key this server last emitted for this session -- the
    // authoritative bound -- not the client-supplied key. The client key is only
    // validated for presence (E4, above); a stale or empty client key (e.g. a
    // consumer that restarted without its durable last key) must not reposition
    // the scan, which would skip rows (a forward jump) or replay them (a
    // backward jump). While the session is live the server's stored key is the
    // single source of truth. It is empty on the first page of a fresh session
    // (cleared at establish), so that page scans from the beginning.
    resume_key = it->second->snapshot.resume_key;
  }

  // 4. Rebuild the MVCC snapshot at snap_ts (fast: the timestamp is in the past
  //    and all ops are applied) and re-assert the history floor for this page.
  tablet::MvccSnapshot snap;
  RETURN_NOT_OK(tablet->mvcc_manager()->WaitForSnapshotWithAllApplied(
      snap_ts, &snap, deadline));
  tablet->SetCDCHistoryFloor(snap_ts);

  // 5. Build the ordered scan, resuming after the server's authoritative resume
  // key (E10) if the session has emitted any rows yet.
  // Project all columns. Pass a column-id-free projection; the iterator maps it
  // onto the tablet's column IDs internally (iter->schema() is the mapped form).
  const SchemaPtr schema = tablet->schema();
  Schema projection = schema->CopyWithoutColumnIds();

  tablet::RowIteratorOptions opts;
  opts.projection = &projection;
  opts.snap_to_include = snap;
  opts.order = ORDERED;
  opts.include_deleted_rows = false;
  unique_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(tablet->NewRowIterator(std::move(opts), &iter));

  Arena arena(1024);
  ScanSpec spec;
  // True when the resume key is already the lexicographically maximum encoded
  // key, so no strictly-greater lower bound exists. The previous page ended on
  // the very last row of the tablet; the scan is therefore complete and this
  // page is empty. Detected here (rather than propagating IncrementEncodedKey's
  // IllegalState) so a max-valued primary key cannot permanently wedge snapshot
  // pagination -- see the same guard in cdc_util.cc.
  bool resume_key_is_max = false;
  if (!resume_key.empty()) {
    EncodedKey* lower = nullptr;
    RETURN_NOT_OK(EncodedKey::DecodeEncodedString(
        *schema, &arena, Slice(resume_key), &lower));
    if (EncodedKey::IncrementEncodedKey(*schema, &lower, &arena).ok()) {
      spec.SetLowerBoundKey(lower);
    } else {
      resume_key_is_max = true;
    }
  }

  // 6. Emit READ records up to max_bytes. Skip the scan entirely when the resume
  // key was the maximum key: there can be no rows strictly after it, so this is
  // an empty terminal page.
  int64_t bytes = 0;
  string last_key;
  bool truncated = false;
  if (!resume_key_is_max) {
    RETURN_NOT_OK(iter->Init(&spec));
    const Schema& iter_schema = iter->schema();
    RowBlockMemory mem(1024);
    RowBlock block(&iter_schema, 256, &mem);
    Arena key_arena(256);
    while (iter->HasNext()) {
      RETURN_NOT_OK(iter->NextBlock(&block));
      for (size_t i = 0; i < block.nrows(); ++i) {
        if (!block.selection_vector()->IsRowSelected(i)) {
          continue;
        }
        RowBlockRow row = block.row(i);
        CDCRecordPB* rec = resp->add_records();
        rec->set_op_type(CDCOpTypePB::READ);
        rec->set_timestamp(snap_ts.value());
        RETURN_NOT_OK(PopulateReadRecord(iter_schema, row, rec));
        key_arena.Reset();
        last_key = SerializeSnapshotKey(iter_schema, row, &key_arena);
        bytes += static_cast<int64_t>(rec->ByteSizeLong());
        if (bytes >= max_bytes) {
          truncated = true;
          break;
        }
      }
      if (truncated) {
        break;
      }
    }
  }

  // The scan is complete when we exhausted the iterator (did not stop early).
  const bool done = !truncated;

  // 7. Update session state and response fields.
  {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end()) {
      if (!last_key.empty()) {
        it->second->snapshot.resume_key = last_key;
      }
      if (done) {
        it->second->snapshot.active = false;
      }
    }
  }

  if (done) {
    resp->set_snapshot_done(true);
    resp->set_snapshot_streaming_start_op_index(streaming_start_op_index);
    resp->set_checkpoint_op_index(streaming_start_op_index);
  } else {
    resp->set_snapshot_resume_key(last_key);
    resp->set_checkpoint_op_index(-1);
  }

  // Persist the history floor (and the streaming-start retention point) to the
  // master so followers hold the snapshot's history. Done once at the start of
  // the scan and again when it completes; the master re-fans-out periodically.
  if (start_new || done) {
    const uint64_t floor_micros = HybridClock::GetPhysicalValueMicros(snap_ts);
    PersistCheckpoint(stream_id, tablet_id, streaming_start_op_index, floor_micros);
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// UpdateAnchor (internal)
// ---------------------------------------------------------------------------

Status CDCServiceImpl::UpdateAnchor(const string& stream_id,
                                    const string& tablet_id,
                                    int64_t anchor_op_index,
                                    LogAnchorRegistry* registry) {
  if (!registry) {
    return Status::InvalidArgument("null LogAnchorRegistry");
  }

  CDCStreamTabletKey key{stream_id, tablet_id};
  CDCStreamTabletState* state = nullptr;

  {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(key);
    if (it == stream_tablet_state_.end()) {
      auto new_state = std::make_unique<CDCStreamTabletState>();
      new_state->anchor_owner = Substitute("CDC[stream=$0]", stream_id);
      new_state->checkpoint_op_index.store(anchor_op_index, std::memory_order_relaxed);
      state = new_state.get();
      stream_tablet_state_.emplace(key, std::move(new_state));
      SetupSessionMetrics(key, state);
    } else {
      state = it->second.get();
    }
  }

  return registry->RegisterOrUpdate(anchor_op_index, state->anchor_owner, &state->anchor);
}

// ---------------------------------------------------------------------------
// SetRetentionBarrier
// ---------------------------------------------------------------------------

Status CDCServiceImpl::SetRetentionBarrier(const string& tablet_id,
                                           int64_t min_retained_op_index,
                                           uint64_t history_safe_time_micros,
                                           int64_t barrier_seq,
                                           const string& release_consumer_stream_id,
                                           bool skip_barrier_update) {
  scoped_refptr<TabletReplica> replica;
  RETURN_NOT_OK_PREPEND(
      server_->tablet_manager()->GetTabletReplica(tablet_id, &replica),
      Substitute("tablet $0 not found on this server", tablet_id));
  LogAnchorRegistry* registry = replica->log_anchor_registry().get();
  if (!registry) {
    return Status::IllegalState(
        Substitute("tablet $0 has no log anchor registry", tablet_id));
  }

  // Status of the in-memory anchor register/release, reported to the caller.
  Status anchor_status;
  // Whether the aggregate WAL retention barrier is actually being applied on
  // this call. Set false when the caller only wants a consumer-anchor release
  // ('skip_barrier_update') or when the sequence gate finds this update stale.
  // Hoisted out of the lock so the superblock persist below can be skipped
  // accordingly.
  bool apply_barrier = !skip_barrier_update;
  {
    // Hold 'lock_' across the in-memory update -- the sequence gate, the
    // history-floor apply, the retention anchor register/release, and the
    // consumer-anchor release -- so a stale update cannot slip its apply in
    // between another update's gate and apply. SetCDCHistoryFloor is a cheap
    // atomic store, so this does not meaningfully widen the critical section.
    // The metadata Flush() below is deliberately kept OUTSIDE this lock: it may
    // fsync the superblock, and holding 'lock_' across a blocking flush would
    // serialize all CDC operations on this server.
    std::lock_guard lock(lock_);

    // Last-writer-wins gate: discard a reordered barrier update that a newer one
    // has already superseded. Barrier RPCs are async, best-effort and unordered,
    // so without this a slow SET from an earlier maintenance pass could land after
    // a later pass's RELEASE and re-anchor this replica forever (a WAL/history
    // leak), or vice versa. barrier_seq == 0 means an unsequenced (legacy) master;
    // such updates are always applied. The seq map deliberately outlives the
    // anchor (it is not erased on release) so a stale SET after a RELEASE is still
    // recognized as superseded. The gate guards ONLY the aggregate barrier; a
    // consumer-anchor release (stream delete) is terminal and always applied.
    if (apply_barrier && barrier_seq > 0) {
      auto sit = barrier_last_seq_.find(tablet_id);
      if (sit != barrier_last_seq_.end() && barrier_seq < sit->second) {
        // A newer update already applied for this tablet; skip the aggregate
        // barrier (and its persist) -- the superseding update already did (or
        // will) persist. Fall through to the consumer-anchor release.
        apply_barrier = false;
      } else {
        barrier_last_seq_[tablet_id] = barrier_seq;
      }
    }

    if (apply_barrier) {
      // Apply (or release) the MVCC history floor for FULL/snapshot streams.
      {
        std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
        if (tablet) {
          // history_safe_time_micros == 0 releases the floor (Timestamp(0) = no floor).
          Timestamp floor = history_safe_time_micros > 0
              ? HybridClock::TimestampFromMicroseconds(history_safe_time_micros)
              : Timestamp(0);
          tablet->SetCDCHistoryFloor(floor);
        }
      }

      auto it = retention_anchors_.find(tablet_id);
      if (min_retained_op_index < 0) {
        // A negative index releases the barrier (no active CDC stream for this tablet).
        if (it != retention_anchors_.end()) {
          WARN_NOT_OK(registry->UnregisterIfAnchored(&it->second->anchor),
                      Substitute("CDC: failed to release retention anchor for tablet $0",
                                 tablet_id));
          retention_anchors_.erase(it);
        }
      } else {
        TabletRetentionState* state;
        if (it == retention_anchors_.end()) {
          auto new_state = std::make_unique<TabletRetentionState>();
          new_state->anchor_owner = Substitute("CDC-retention[$0]", tablet_id);
          state = new_state.get();
          retention_anchors_.emplace(tablet_id, std::move(new_state));
        } else {
          state = it->second.get();
        }
        state->history_safe_time_micros = history_safe_time_micros;
        state->min_retained_op_index = min_retained_op_index;
        anchor_status = registry->RegisterOrUpdate(min_retained_op_index,
                                                   state->anchor_owner, &state->anchor);
      }
    }

    // Release the per-(stream, tablet) consumer anchor on stream deletion. This
    // anchor is established by the consumer's own GetChanges/Checkpoint polling
    // (leader only) and lives in stream_tablet_state_, distinct from the
    // master-pushed aggregate barrier above; without this it would be freed only
    // when the tablet itself is deleted, so a deleted stream would keep pinning
    // the WAL (A4). Fanned to every replica, so it also covers the case where
    // the anchor was established on a replica that has since lost leadership.
    if (!release_consumer_stream_id.empty()) {
      CDCStreamTabletKey key{release_consumer_stream_id, tablet_id};
      auto it = stream_tablet_state_.find(key);
      if (it != stream_tablet_state_.end()) {
        WARN_NOT_OK(registry->UnregisterIfAnchored(&it->second->anchor),
                    Substitute("CDC: failed to release consumer anchor for deleted "
                               "stream $0 tablet $1",
                               release_consumer_stream_id, tablet_id));
        stream_tablet_state_.erase(it);
      }
    }
  }

  if (apply_barrier) {
    // Persist the barrier in the tablet superblock so WAL/history retention
    // survives a tserver restart or leader change without depending on the
    // master's next maintenance pass. This runs on every replica (the master
    // fans the barrier out to all peers), and only flushes when the persisted
    // value actually changed, so it is bounded to roughly one superblock flush
    // per maintenance pass per tablet. Skipped when only a consumer anchor was
    // released (nothing durable changed) or when the update was superseded.
    const scoped_refptr<tablet::TabletMetadata>& meta = replica->tablet_metadata();
    if (meta && meta->SetCDCRetentionBarrier(min_retained_op_index, history_safe_time_micros)) {
      WARN_NOT_OK(meta->Flush(),
                  Substitute("CDC: failed to persist retention barrier for tablet $0",
                             tablet_id));
    }
  }
  return anchor_status;
}

// ---------------------------------------------------------------------------
// ReleaseAnchorsForTablet
// ---------------------------------------------------------------------------

void CDCServiceImpl::ReleaseAnchorsForTablet(const string& tablet_id) {
  scoped_refptr<TabletReplica> replica;
  Status s = server_->tablet_manager()->GetTabletReplica(tablet_id, &replica);
  if (!s.ok() || !replica || !replica->log_anchor_registry()) {
    return;
  }
  LogAnchorRegistry* registry = replica->log_anchor_registry().get();

  std::lock_guard lock(lock_);
  // Per-tablet retention anchor.
  auto rit = retention_anchors_.find(tablet_id);
  if (rit != retention_anchors_.end()) {
    WARN_NOT_OK(registry->UnregisterIfAnchored(&rit->second->anchor),
                Substitute("CDC: failed to release retention anchor for deleted tablet $0",
                           tablet_id));
    retention_anchors_.erase(rit);
  }
  // Per-(stream, tablet) consumer anchors.
  for (auto it = stream_tablet_state_.begin(); it != stream_tablet_state_.end(); ) {
    if (it->first.tablet_id == tablet_id) {
      WARN_NOT_OK(registry->UnregisterIfAnchored(&it->second->anchor),
                  Substitute("CDC: failed to release consumer anchor for deleted tablet $0",
                             tablet_id));
      it = stream_tablet_state_.erase(it);
    } else {
      ++it;
    }
  }
}

// ---------------------------------------------------------------------------
// ClearSnapshotSessionsForTests
// ---------------------------------------------------------------------------

void CDCServiceImpl::ClearSnapshotSessionsForTests() {
  std::lock_guard lock(lock_);
  for (auto& entry : stream_tablet_state_) {
    entry.second->snapshot = CDCSnapshotState();
  }
}

// ---------------------------------------------------------------------------
// PersistCheckpointAsync (internal)
// ---------------------------------------------------------------------------

void CDCServiceImpl::PersistCheckpoint(const string& stream_id,
                                       const string& tablet_id,
                                       int64_t op_index,
                                       uint64_t history_safe_time_micros,
                                       bool refresh_active_time_only) {
  const auto& masters = server_->master_addresses();
  if (masters.empty()) {
    return;
  }

  master::UpdateCDCCheckpointRequestPB req;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_op_index(op_index);
  if (history_safe_time_micros > 0) {
    req.set_history_safe_time_micros(history_safe_time_micros);
  }
  if (refresh_active_time_only) {
    req.set_refresh_active_time_only(true);
  }

  // Try each master until one accepts the update. Only the leader master will
  // apply it; others reply with an error and we move on to the next.
  Status last_status = Status::NetworkError("no master reachable");
  if (PREDICT_FALSE(FLAGS_cdc_inject_checkpoint_persist_failure)) {
    last_status = Status::ServiceUnavailable("injected checkpoint persist failure");
  } else {
    for (const auto& hp : masters) {
      vector<Sockaddr> addrs;
      Status s = server_->dns_resolver()->ResolveAddresses(hp, &addrs);
      if (!s.ok() || addrs.empty()) {
        last_status = s.ok() ? Status::NetworkError("no addresses for master") : s;
        continue;
      }
      master::MasterServiceProxy proxy(server_->messenger(), addrs[0], hp.host());
      master::UpdateCDCCheckpointResponsePB resp;
      rpc::RpcController rpc;
      rpc.set_timeout(MonoDelta::FromSeconds(10));
      s = proxy.UpdateCDCCheckpoint(req, &resp, &rpc);
      if (s.ok() && !resp.has_error()) {
        VLOG(2) << "CDC: persisted checkpoint [stream=" << stream_id
                << " tablet=" << tablet_id << " op_index=" << op_index << "]";
        return;
      }
      last_status = s.ok() ? StatusFromPB(resp.error().status()) : s;
    }
  }
  // All master candidates failed (or inject flag forced failure). Log at WARNING
  // so operators can see that the master's durable checkpoint is falling behind.
  // Increment the failure counter so monitoring dashboards can alert on this
  // condition (CF-2): a non-zero rate while consumers are advancing means the
  // staleness guard's advance-attempt grace floor is actively suppressing
  // spurious barrier releases.
  KLOG_EVERY_N_SECS(WARNING, 60)
      << "CDC: failed to persist checkpoint [stream=" << stream_id
      << " tablet=" << tablet_id << " op_index=" << op_index
      << "] to master: " << last_status.ToString();
  checkpoint_persist_failures_->Increment();
}

// ---------------------------------------------------------------------------
// RecordActivity (internal)
// ---------------------------------------------------------------------------

void CDCServiceImpl::RecordActivity(const string& stream_id,
                                    const string& tablet_id) {
  const int64_t now_micros = GetCurrentTimeMicros();
  const int64_t report_interval_micros =
      FLAGS_cdc_active_time_report_interval_ms > 0
          ? FLAGS_cdc_active_time_report_interval_ms * 1000 : 0;
  bool should_report = false;
  {
    std::lock_guard lock(lock_);
    CDCStreamTabletKey key{stream_id, tablet_id};
    auto it = stream_tablet_state_.find(key);
    if (it == stream_tablet_state_.end()) {
      return;
    }
    CDCStreamTabletState* state = it->second.get();
    state->last_active_time_micros.store(now_micros, std::memory_order_relaxed);
    // Throttle heartbeats to the master. The very first activity (report time 0)
    // is always reported so the master's clock-based active time is anchored.
    if (now_micros - state->last_active_report_micros >= report_interval_micros) {
      state->last_active_report_micros = now_micros;
      should_report = true;
    }
  }
  if (should_report) {
    // op_index is ignored by the master for an active-time-only refresh.
    PersistCheckpoint(stream_id, tablet_id, /*op_index=*/0,
                      /*history_safe_time_micros=*/0,
                      /*refresh_active_time_only=*/true);
  }
}

// ---------------------------------------------------------------------------
// StreamIdleExpired (internal)
// ---------------------------------------------------------------------------

bool CDCServiceImpl::StreamIdleExpired(const string& stream_id,
                                       const string& tablet_id) const {
  const int64_t expiry_ms = FLAGS_cdc_stream_idle_expiry_ms;
  if (expiry_ms <= 0) {
    // Disambiguation disabled: always fall back to WAL_EXPIRED.
    return false;
  }
  int64_t last_active_micros = 0;
  {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(CDCStreamTabletKey{stream_id, tablet_id});
    if (it == stream_tablet_state_.end()) {
      return false;
    }
    last_active_micros =
        it->second->last_active_time_micros.load(std::memory_order_relaxed);
  }
  if (last_active_micros <= 0) {
    // No successful poll recorded yet; cannot assert the stream has been idle,
    // so report the more conservative WAL_EXPIRED.
    return false;
  }
  const int64_t idle_micros = GetCurrentTimeMicros() - last_active_micros;
  return idle_micros > expiry_ms * 1000;
}

// ---------------------------------------------------------------------------
// GetOrFetchStreamConfig (internal)
// ---------------------------------------------------------------------------

Status CDCServiceImpl::GetOrFetchStreamConfig(const string& stream_id,
                                              CDCStreamConfigPB* config) {
  const MonoTime now = MonoTime::Now();

  // Fast path: a cached entry that is still within its TTL.
  {
    std::lock_guard lock(lock_);
    auto it = stream_config_cache_.find(stream_id);
    if (it != stream_config_cache_.end() && now < it->second.expiry) {
      *config = it->second.config;
      return Status::OK();
    }
  }

  // Either no entry, or the entry is stale and must be refetched from the
  // master so a stream reconfigure (e.g. record_type change) is picked up.
  //
  // E11: single-flight the fetch. Concurrent callers that all miss for the same
  // stream_id serialize on this per-stream mutex so only one master RPC is in
  // flight at a time. The mutex must be held across the blocking RPC, so it is a
  // std::mutex, not lock_ (a spinlock).
  std::shared_ptr<std::mutex> fetch_lock = GetStreamConfigFetchLock(stream_id);
  std::lock_guard<std::mutex> fetch_guard(*fetch_lock);

  // Re-check the cache now that we hold the fetch lock: while we waited, another
  // caller may have populated (or stale-refreshed) the entry. Use a fresh 'now'
  // -- we may have blocked for a full master RPC round-trip. This is what makes
  // the waiters cheap: on the common path they return the just-fetched config
  // without issuing an RPC of their own.
  {
    const MonoTime after_wait = MonoTime::Now();
    std::lock_guard lock(lock_);
    auto it = stream_config_cache_.find(stream_id);
    if (it != stream_config_cache_.end() && after_wait < it->second.expiry) {
      *config = it->second.config;
      return Status::OK();
    }
  }

  const int32_t ttl_ms = FLAGS_cdc_stream_config_cache_ttl_ms;
  const MonoTime new_expiry = ttl_ms > 0
      ? MonoTime::Now() + MonoDelta::FromMilliseconds(ttl_ms)
      : MonoTime::Max();

  // Test hook: widen the fetch window so concurrent cache misses reliably pile
  // up on the single-flight lock (E11).
  if (PREDICT_FALSE(FLAGS_cdc_inject_latency_before_stream_config_fetch_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(
        FLAGS_cdc_inject_latency_before_stream_config_fetch_ms));
  }
  // A real fetch reaches the master loop. Counted once here (not per cache hit)
  // so a test can assert the single-flight collapse.
  stream_config_master_fetches_.fetch_add(1, std::memory_order_relaxed);

  master::GetCDCStreamInfoRequestPB req;
  req.set_stream_id(stream_id);

  Status last_status = Status::IllegalState("no master addresses configured");
  const auto& masters = server_->master_addresses();
  for (const auto& hp : masters) {
    vector<Sockaddr> addrs;
    Status s = server_->dns_resolver()->ResolveAddresses(hp, &addrs);
    if (!s.ok() || addrs.empty()) {
      last_status = s.ok() ? Status::NetworkError("no addresses for master") : s;
      continue;
    }
    master::MasterServiceProxy proxy(server_->messenger(), addrs[0], hp.host());
    master::GetCDCStreamInfoResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    s = proxy.GetCDCStreamInfo(req, &resp, &rpc);
    if (s.ok() && !resp.has_error()) {
      // master::CDCStreamConfigPB and cdc::CDCStreamConfigPB are distinct C++
      // types kept structurally identical (same field numbers), so bridge them
      // with a serialize/parse round-trip.
      CDCStreamConfigPB result;
      if (!result.ParseFromString(resp.stream().config().SerializeAsString())) {
        return Status::Corruption(
            Substitute("could not parse config for CDC stream $0", stream_id));
      }
      {
        std::lock_guard lock(lock_);
        stream_config_cache_[stream_id] = {result, new_expiry};
      }
      *config = std::move(result);
      return Status::OK();
    }
    if (s.ok() && resp.has_error()) {
      Status stream_status = StatusFromPB(resp.error().status());
      if (stream_status.IsNotFound()) {
        // Authoritative: the leader master reports this stream does not exist
        // (deleted or never created). Only the leader reaches the stream-map
        // lookup -- followers respond NOT_LEADER before it -- so a NotFound here
        // is definitive. Evict any now-invalid cached entry and return NotFound
        // so the read path surfaces STREAM_NOT_FOUND rather than continuing to
        // serve a stale config (or falling through as CHANGE-mode WAL data).
        // Unlike the transient-failure path below, we do NOT serve stale here.
        std::lock_guard lock(lock_);
        stream_config_cache_.erase(stream_id);
        return stream_status;
      }
      last_status = stream_status;
    } else {
      last_status = s;
    }
  }

  // Refetch failed (master unreachable/erroring, or none configured). If we
  // still hold a now-stale entry, serve it rather than failing the consumer --
  // a transient master outage should not stall streaming -- and back off its
  // expiry so we do not attempt (and stall on) a refetch on every call. A
  // genuinely changed config is still picked up within one more TTL once the
  // master recovers. With no cached entry at all, surface the error.
  {
    std::lock_guard lock(lock_);
    auto it = stream_config_cache_.find(stream_id);
    if (it != stream_config_cache_.end()) {
      KLOG_EVERY_N_SECS(WARNING, 60)
          << "serving stale config for CDC stream " << stream_id
          << "; refetch from master failed: " << last_status.ToString();
      it->second.expiry = new_expiry;
      *config = it->second.config;
      return Status::OK();
    }
  }
  return last_status;
}

} // namespace cdc
} // namespace kudu
