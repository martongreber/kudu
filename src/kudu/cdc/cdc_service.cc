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

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_util.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partition.h"
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
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
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
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/logging.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"

// Number of RPC worker threads per service pool (owned by rpc_server.cc). The
// CDC service pool is inited with this many threads, so it is the denominator
// for --cdc_get_changes_free_rpc_ratio's worker-thread reservation.
DECLARE_int32(rpc_num_service_threads);
DECLARE_int64(rpc_max_message_size);
DECLARE_int64(cdc_barrier_lease_secs);

DEFINE_int64(cdc_max_bytes_per_response, 8 * 1024 * 1024, // 8 MiB
             "Maximum total size of CDCRecordPB entries returned in a single "
             "GetChanges response (8 MiB), and of the WAL read to build it. Bounds "
             "the per-response heap a CDC consumer can force a tablet server to "
             "buffer; a smaller cap means more (lighter) pages rather than one large "
             "one, so a burst of consumers competes less with user traffic for heap. "
             "A request may ask for less, but not for more. The records of one op "
             "or one transaction are never split, so a response carries them whole "
             "even if they exceed the cap, as long as they fit in one RPC message "
             "(--rpc_max_message_size). The snapshot bootstrap path uses the "
             "(smaller) --cdc_snapshot_max_bytes_per_response. Must be "
             "<= --cdc_max_transaction_span_bytes.");
TAG_FLAG(cdc_max_bytes_per_response, advanced);
TAG_FLAG(cdc_max_bytes_per_response, runtime);
TAG_FLAG(cdc_max_bytes_per_response, experimental);

DEFINE_int64(cdc_max_transaction_span_bytes, 536870912, // 512 MiB
             "Maximum WAL span (bytes, from a transaction's first write to its "
             "FINALIZE_COMMIT) that CDC will read in order to emit an open "
             "transaction atomically. A transaction is only visible to consumers "
             "at commit, so its entire span must fit in one read window; if it "
             "exceeds the per-response byte cap the read window would never reach "
             "the commit and the stream's checkpoint could never advance past the "
             "transaction (a permanent wedge). When such a transaction is "
             "detected the effective read cap is escalated (up to this limit, and "
             "up to the CDC read memory budget if one is set) to reach the commit; "
             "it is not escalated while the transaction is still open. A "
             "transaction whose span exceeds this limit cannot be emitted; "
             "GetChanges then fails loudly with TRANSACTION_TOO_LARGE rather than "
             "stalling silently. The same happens if the transaction's change "
             "records do not fit in one RPC message (--rpc_max_message_size), "
             "which usually binds first. Must be >= --cdc_max_bytes_per_response.");
TAG_FLAG(cdc_max_transaction_span_bytes, advanced);
TAG_FLAG(cdc_max_transaction_span_bytes, runtime);
TAG_FLAG(cdc_max_transaction_span_bytes, experimental);

DEFINE_int32(cdc_full_apply_wait_timeout_ms, 30000, // 30 s
             "Maximum time a FULL-mode GetChanges will wait for committed ops to be "
             "applied to the MemRowSet before reconstructing before/after images. "
             "Caps the effective deadline derived from the RPC's own deadline. On "
             "timeout the call returns a retryable error and the consumer re-polls.");
TAG_FLAG(cdc_full_apply_wait_timeout_ms, advanced);
TAG_FLAG(cdc_full_apply_wait_timeout_ms, runtime);
TAG_FLAG(cdc_full_apply_wait_timeout_ms, experimental);

DEFINE_int32(cdc_snapshot_wait_timeout_ms, 30000, // 30 s
             "Maximum time a snapshot GetChanges page will wait for the tablet's "
             "MVCC state to be safe/applied at the snapshot timestamp. Caps the "
             "effective deadline derived from the RPC's own client deadline, so a "
             "caller with a very long (or absent) deadline cannot block a service "
             "thread longer than this. On timeout the call returns a retryable "
             "error and the consumer re-polls.");
TAG_FLAG(cdc_snapshot_wait_timeout_ms, advanced);
TAG_FLAG(cdc_snapshot_wait_timeout_ms, runtime);
TAG_FLAG(cdc_snapshot_wait_timeout_ms, experimental);

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
              "Must be in [0.0, 1.0); 0 disables the reservation.");
TAG_FLAG(cdc_read_safe_deadline_ratio, advanced);
TAG_FLAG(cdc_read_safe_deadline_ratio, runtime);
TAG_FLAG(cdc_read_safe_deadline_ratio, experimental);

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
              "WAL-streaming path. Must be in [0.0, 1.0); 0 lets GetChanges use "
              "every worker thread.");
TAG_FLAG(cdc_get_changes_free_rpc_ratio, advanced);
TAG_FLAG(cdc_get_changes_free_rpc_ratio, runtime);
TAG_FLAG(cdc_get_changes_free_rpc_ratio, experimental);

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

DEFINE_bool(cdc_inject_snapshot_scan_deadline_expired, false,
            "If true, the scan of a snapshot page treats the page's deadline "
            "as expired, so the page ends after its first block of rows. For "
            "testing only!");
TAG_FLAG(cdc_inject_snapshot_scan_deadline_expired, unsafe);
TAG_FLAG(cdc_inject_snapshot_scan_deadline_expired, runtime);

DEFINE_int32(cdc_inject_latency_before_stream_config_fetch_ms, 0,
             "How much latency (in milliseconds) to inject immediately before "
             "the master GetCDCStreamInfo RPC in GetOrFetchStreamConfig. Widens "
             "the config-fetch window so a test can pile up concurrent cache "
             "misses on the single-flight lock. For testing only!");
TAG_FLAG(cdc_inject_latency_before_stream_config_fetch_ms, unsafe);
TAG_FLAG(cdc_inject_latency_before_stream_config_fetch_ms, runtime);

DEFINE_int32(cdc_inject_latency_before_session_update_ms, 0,
             "How much latency (in milliseconds) to inject when a GetChanges or "
             "Checkpoint call is about to look up or create its consumer session. "
             "Widens the window for a concurrent stream delete to release the "
             "session. For testing only!");
TAG_FLAG(cdc_inject_latency_before_session_update_ms, unsafe);
TAG_FLAG(cdc_inject_latency_before_session_update_ms, runtime);

DEFINE_int32(cdc_inject_latency_after_checkpoint_respond_ms, 0,
             "How much latency (in milliseconds) to inject in Checkpoint after the "
             "response has been sent, before the call returns. For testing only!");
TAG_FLAG(cdc_inject_latency_after_checkpoint_respond_ms, unsafe);
TAG_FLAG(cdc_inject_latency_after_checkpoint_respond_ms, runtime);

DEFINE_int32(cdc_inject_latency_before_decode_ms, 0,
             "How much latency (in milliseconds) to inject in GetChanges after the "
             "WAL read and before the batch is decoded. Widens the window for the "
             "tablet to be stopped mid-batch. For testing only!");
TAG_FLAG(cdc_inject_latency_before_decode_ms, unsafe);
TAG_FLAG(cdc_inject_latency_before_decode_ms, runtime);

DEFINE_bool(cdc_inject_tablet_not_running, false,
            "If true, ReadChanges treats the tablet as not RUNNING and returns "
            "TABLET_NOT_RUNNING, so tests can exercise the not-running "
            "classification without racing tablet bootstrap. For testing only!");
TAG_FLAG(cdc_inject_tablet_not_running, unsafe);
TAG_FLAG(cdc_inject_tablet_not_running, runtime);

DEFINE_bool(cdc_inject_post_read_leadership_loss, false,
            "If true, the post-read leader-term recheck in ReadChanges (and the "
            "post-scan one of a snapshot page) fires as "
            "though this replica lost leadership during the scan, so tests can "
            "exercise the recheck deterministically without forcing a real "
            "leader change mid-read. For testing only!");
TAG_FLAG(cdc_inject_post_read_leadership_loss, unsafe);
TAG_FLAG(cdc_inject_post_read_leadership_loss, runtime);

DEFINE_int64(cdc_inject_decode_failure_at_op_index, -1,
             "If non-negative, decoding the WRITE_OP or ALTER_SCHEMA_OP at this "
             "WAL op index into CDC records fails with an injected error. For "
             "testing only!");
TAG_FLAG(cdc_inject_decode_failure_at_op_index, hidden);
TAG_FLAG(cdc_inject_decode_failure_at_op_index, unsafe);
TAG_FLAG(cdc_inject_decode_failure_at_op_index, runtime);

DEFINE_bool(cdc_inject_server_memory_pressure, false,
            "If true, TryAcquireScanSlot behaves as though the whole server is "
            "over its soft memory limit, so tests can exercise the server-pressure "
            "shed deterministically without driving real process memory above the "
            "(process-wide, once-initialized) soft limit. For testing only!");
TAG_FLAG(cdc_inject_server_memory_pressure, unsafe);
TAG_FLAG(cdc_inject_server_memory_pressure, runtime);

DEFINE_bool(cdc_inject_checkpoint_persist_failure, false,
            "If true, every checkpoint persist to the master fails immediately "
            "(before attempting any master RPC), incrementing cdc_checkpoint_persist_failures "
            "and emitting a WARNING. Used by tests to exercise the failure "
            "counter without requiring a real master connectivity failure. "
            "For testing only!");
TAG_FLAG(cdc_inject_checkpoint_persist_failure, unsafe);
TAG_FLAG(cdc_inject_checkpoint_persist_failure, runtime);

DEFINE_int64(cdc_wal_read_mem_limit_bytes, 0,
             "Soft heap budget (bytes) for in-flight CHANGE-mode WAL reads. "
             "When a GetChanges call in CHANGE mode would push the total "
             "WAL-read reservation above this limit, it is shed with a "
             "retryable SERVER_TOO_BUSY so CHANGE-mode streaming cannot "
             "exhaust heap undetected. A read window grown to reach the commit "
             "of a large transaction is charged to the same budget, so when set, "
             "this also caps the WAL span of a transaction a CHANGE stream can "
             "emit (see --cdc_max_transaction_span_bytes). 0 = unlimited "
             "(default). Tuned independently of --cdc_scan_mem_limit_bytes "
             "which covers only snapshot/FULL scans.");
TAG_FLAG(cdc_wal_read_mem_limit_bytes, advanced);
TAG_FLAG(cdc_wal_read_mem_limit_bytes, runtime);
TAG_FLAG(cdc_wal_read_mem_limit_bytes, experimental);

DEFINE_bool(cdc_inject_wal_read_deadline_expired, false,
            "If true, ReadChanges passes an already-expired deadline to "
            "the WAL reads of a poll (the REPLICATE read and the COMMIT "
            "lookup), causing their deadline checks to fire after the "
            "first op and return a partial batch, so tests can exercise a "
            "partial batch with have_more_records deterministically. "
            "For testing only!");
TAG_FLAG(cdc_inject_wal_read_deadline_expired, unsafe);
TAG_FLAG(cdc_inject_wal_read_deadline_expired, runtime);

DEFINE_bool(cdc_inject_commit_lookup_deadline_expired, false,
            "If true, ReadChanges passes an already-expired deadline to the "
            "COMMIT lookup of a poll only, as if the REPLICATE read had used "
            "up the call's time budget. For testing only!");
TAG_FLAG(cdc_inject_commit_lookup_deadline_expired, unsafe);
TAG_FLAG(cdc_inject_commit_lookup_deadline_expired, runtime);

DEFINE_bool(cdc_inject_escalation_deadline_expired, false,
            "If true, the deadline check at the top of the ReadChanges "
            "transaction-span escalation loop fires immediately, returning "
            "SERVER_TOO_BUSY (retryable) without issuing a WAL read, so tests can "
            "exercise the escalation-loop deadline without a large transaction. "
            "For testing only!");
TAG_FLAG(cdc_inject_escalation_deadline_expired, unsafe);
TAG_FLAG(cdc_inject_escalation_deadline_expired, runtime);

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
TAG_FLAG(cdc_snapshot_max_bytes_per_response, experimental);

// Maximum number of CDCRecordPB records per GetChanges response, independent
// of the byte cap. Narrow rows can pack tens of
// thousands of tiny records into one 8 MiB byte-capped response, ballooning
// consumer-side proto deserialization far past 8 MiB of decoded heap.
// Default 4096: generous enough to preserve current behavior for typical rows,
// tight enough to bound deserialization cost at the consumer. 0 = unlimited.
// Applied in both ReadChanges (at committed-transaction-group granularity to
// preserve atomicity) and ReadSnapshot (per-row).
DEFINE_int32(cdc_max_records_per_response, 4096,
             "Maximum number of CDCRecordPB records returned in a single "
             "GetChanges response, independent of the byte cap. Bounds "
             "consumer-side proto deserialization cost for narrow-row tables "
             "where many tiny records fit into one byte-capped page. In "
             "ReadChanges, truncation only occurs at committed-transaction "
             "boundaries (BEGIN+rows+COMMIT are never split). 0 = unlimited.");
TAG_FLAG(cdc_max_records_per_response, advanced);
TAG_FLAG(cdc_max_records_per_response, runtime);
TAG_FLAG(cdc_max_records_per_response, experimental);

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
TAG_FLAG(cdc_max_concurrent_scans, experimental);

DEFINE_int64(cdc_scan_mem_limit_bytes, 256 * 1024 * 1024, // 256 MiB
             "Soft heap budget (bytes) for in-flight CDC heavy-scan responses. "
             "Before a snapshot or FULL scan runs, its response byte cap is "
             "reserved against a dedicated 'cdc_scans' MemTracker (a child of "
             "the server root tracker); if the reservation would exceed this "
             "limit the request is shed with a retryable SERVER_TOO_BUSY error so "
             "large CDC batches cannot exhaust the heap out from under user "
             "traffic. Acts as a server-wide backstop behind "
             "--cdc_max_concurrent_scans (which usually binds first at "
             "cap * --cdc_max_bytes_per_response). A FULL-mode read window grown "
             "to reach the commit of a large transaction is charged to this "
             "budget too, so it also caps the WAL span of a transaction a FULL "
             "stream can emit (see --cdc_max_transaction_span_bytes), and a "
             "grown window that does not fit next to concurrent scans is shed "
             "with SERVER_TOO_BUSY. 0 means no limit (not recommended in "
             "production).");
TAG_FLAG(cdc_scan_mem_limit_bytes, advanced);
TAG_FLAG(cdc_scan_mem_limit_bytes, runtime);
TAG_FLAG(cdc_scan_mem_limit_bytes, experimental);

DEFINE_int64(cdc_active_time_report_interval_ms, 30 * 1000, // 30 s
             "Minimum interval between consumer-activity heartbeats sent from a "
             "tablet server to the master on GetChanges. Refreshes the stream's "
             "last-active time so a live but not-yet-checkpointing consumer is not "
             "expired. Default 30 s (was 5 min): cuts stream-expiry jitter from "
             "5 min to 30 s for GetChanges-only consumers while keeping master "
             "chatter moderate. Should be well below the master's "
             "--cdc_stream_expiry_ms.");
TAG_FLAG(cdc_active_time_report_interval_ms, advanced);
TAG_FLAG(cdc_active_time_report_interval_ms, runtime);
TAG_FLAG(cdc_active_time_report_interval_ms, experimental);

DEFINE_int64(cdc_checkpoint_persist_interval_ms, 15 * 1000, // 15 s
             "Minimum interval between durable checkpoint writes sent from a "
             "tablet server to the master per (stream, tablet). This merely lets "
             "the master's persisted checkpoint, and so the WAL retention barrier "
             "computed from it, lag (retention stays conservative): a checkpoint "
             "within the interval is persisted when the interval ends. On a "
             "tablet-server crash a consumer may re-read up to one interval of "
             "already-processed records (the accepted at-least-once semantics). "
             "Set to 0 to persist on every Checkpoint (no throttle).");
TAG_FLAG(cdc_checkpoint_persist_interval_ms, advanced);
TAG_FLAG(cdc_checkpoint_persist_interval_ms, runtime);
TAG_FLAG(cdc_checkpoint_persist_interval_ms, experimental);

DEFINE_int64(cdc_snapshot_cursor_persist_interval_ms, 10 * 1000, // 10 s
             "Minimum interval between durable snapshot-progress cursor writes "
             "(snap_ts, resume_key) sent from a tablet server to the master during "
             "an initial snapshot per (stream, tablet). The cursor lets a newly "
             "elected leader (or a master failover) resume the snapshot instead of "
             "restarting it. This throttle bounds how many already-emitted rows a "
             "resume re-scans (at most one interval's worth -- the accepted "
             "at-least-once semantics); the snapshot establish and completion "
             "always persist regardless. Set to 0 to persist on every page.");
TAG_FLAG(cdc_snapshot_cursor_persist_interval_ms, advanced);
TAG_FLAG(cdc_snapshot_cursor_persist_interval_ms, runtime);
TAG_FLAG(cdc_snapshot_cursor_persist_interval_ms, experimental);

DEFINE_int32(cdc_master_rpc_timeout_ms, 10000,
             "Timeout (in milliseconds) for one attempt of a tserver-to-master "
             "RPC issued by the CDC service: checkpoint persistence and "
             "GetCDCStreamInfo (stream-config fetch). Default 10000 ms (10 s). "
             "Safe to change at runtime; each call reads the flag at issue time.");
TAG_FLAG(cdc_master_rpc_timeout_ms, advanced);
TAG_FLAG(cdc_master_rpc_timeout_ms, runtime);
TAG_FLAG(cdc_master_rpc_timeout_ms, experimental);

DEFINE_int32(cdc_checkpoint_flush_interval_ms, 1000,
             "Minimum interval between two batches of CDC checkpoint updates "
             "(checkpoints, snapshot cursors and activity reports) that a tablet "
             "server sends to the leader master. Updates queued in between are "
             "coalesced per (stream, tablet), so this bounds the master write rate "
             "to one batch per tablet server per interval.");
TAG_FLAG(cdc_checkpoint_flush_interval_ms, advanced);
TAG_FLAG(cdc_checkpoint_flush_interval_ms, runtime);
TAG_FLAG(cdc_checkpoint_flush_interval_ms, experimental);

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
             "so a still-served stream is never expired. The leader master "
             "reports each stream's expiry (its --cdc_stream_expiry_ms) with the "
             "stream's configuration, and that value takes precedence; this "
             "flag applies only to a stream whose expiry is not known. Set to 0 "
             "to disable the disambiguation for such a stream (always report "
             "WAL_EXPIRED).");
TAG_FLAG(cdc_stream_idle_expiry_ms, advanced);
TAG_FLAG(cdc_stream_idle_expiry_ms, runtime);
TAG_FLAG(cdc_stream_idle_expiry_ms, experimental);

DEFINE_int64(cdc_session_history_floor_max_age_secs, 86400, // 24 h
             "Maximum age, in seconds, of the UNDO history that a FULL stream's "
             "consumer session on a tablet leader pins on its own, measured from "
             "the timestamps of the ops it read to now. A session's floor for an "
             "older batch is not held, so a consumer that keeps polling without "
             "checkpointing (stuck on a batch, or a read-only 'kudu cdc consume') "
             "cannot pin history forever after the master stopped counting its "
             "stream. History that a checkpointed position still needs is held "
             "by the master's floor, which this limit does not affect. A consumer "
             "whose before-images are then reclaimed receives HISTORY_EXPIRED. "
             "Set to 0 to disable.");
TAG_FLAG(cdc_session_history_floor_max_age_secs, advanced);
TAG_FLAG(cdc_session_history_floor_max_age_secs, runtime);
TAG_FLAG(cdc_session_history_floor_max_age_secs, experimental);

DEFINE_int32(cdc_stream_config_negative_cache_ttl_ms, 5000,
             "How long a tablet server remembers that it failed to fetch the "
             "config of a CDC stream it has no cached config for (because the "
             "masters were unreachable, the stream does not exist, or its config "
             "has options this server does not support). Requests for the stream "
             "in that time do not ask the master again: they fail with "
             "STREAM_NOT_FOUND if the stream does not exist, with the "
             "non-retryable UNKNOWN_ERROR if its config is not supported, and "
             "are shed as retryable SERVER_TOO_BUSY otherwise. 0 disables this.");
TAG_FLAG(cdc_stream_config_negative_cache_ttl_ms, advanced);
TAG_FLAG(cdc_stream_config_negative_cache_ttl_ms, runtime);
TAG_FLAG(cdc_stream_config_negative_cache_ttl_ms, experimental);

DEFINE_int32(cdc_stream_config_cache_ttl_ms, 5 * 60 * 1000, // 5 min
             "How long (in milliseconds) a tablet server caches a CDC stream's "
             "configuration (record type, snapshot mode) fetched from the master "
             "before refetching it. Bounds how long a stream reconfigure takes to "
             "take effect on this server. 0 refetches the configuration on every "
             "request (the cached copy is then used only while the masters are "
             "unreachable).");
TAG_FLAG(cdc_stream_config_cache_ttl_ms, advanced);
TAG_FLAG(cdc_stream_config_cache_ttl_ms, runtime);
TAG_FLAG(cdc_stream_config_cache_ttl_ms, experimental);

DEFINE_int32(cdc_barrier_reorder_window_secs, 3600,
             "Number of seconds the sequence of the last CDC retention barrier "
             "update applied on a tablet replica makes the replica reject "
             "updates with a lower sequence, which are reordered or stale ones. "
             "Must be longer than the time for which a master retries a barrier "
             "update (at most its --unresponsive_ts_rpc_timeout_ms). Once it has "
             "passed, a lower sequence is applied, so the replica recovers when "
             "the master's sequence went backwards, for example after the "
             "master's system catalog was rebuilt with 'kudu master "
             "unsafe_rebuild'.");
TAG_FLAG(cdc_barrier_reorder_window_secs, advanced);
TAG_FLAG(cdc_barrier_reorder_window_secs, runtime);
TAG_FLAG(cdc_barrier_reorder_window_secs, experimental);

namespace {

template <typename T>
bool ValidatePositive(const char* name, T value) {
  if (value > 0) {
    return true;
  }
  LOG(ERROR) << "--" << name << " must be positive; got " << value;
  return false;
}

template <typename T>
bool ValidateNonNegative(const char* name, T value) {
  if (value >= 0) {
    return true;
  }
  LOG(ERROR) << "--" << name << " must not be negative; got " << value;
  return false;
}

// The read window is escalated from the per-response cap up to the
// transaction span cap, so the latter must not be smaller.
bool ValidateCdcResponseSizeFlags() {
  if (FLAGS_cdc_max_transaction_span_bytes < FLAGS_cdc_max_bytes_per_response) {
    LOG(ERROR) << "--cdc_max_transaction_span_bytes ("
               << FLAGS_cdc_max_transaction_span_bytes
               << ") must not be smaller than --cdc_max_bytes_per_response ("
               << FLAGS_cdc_max_bytes_per_response << ")";
    return false;
  }
  return true;
}

} // anonymous namespace

DEFINE_validator(cdc_max_bytes_per_response, &ValidatePositive<int64_t>);
DEFINE_validator(cdc_max_transaction_span_bytes, &ValidatePositive<int64_t>);
DEFINE_validator(cdc_full_apply_wait_timeout_ms, &ValidatePositive<int32_t>);
DEFINE_validator(cdc_snapshot_wait_timeout_ms, &ValidatePositive<int32_t>);
DEFINE_validator(cdc_master_rpc_timeout_ms, &ValidatePositive<int32_t>);
DEFINE_validator(cdc_wal_read_mem_limit_bytes, &ValidateNonNegative<int64_t>);
DEFINE_validator(cdc_snapshot_max_bytes_per_response, &ValidateNonNegative<int64_t>);
DEFINE_validator(cdc_max_records_per_response, &ValidateNonNegative<int32_t>);
DEFINE_validator(cdc_max_concurrent_scans, &ValidateNonNegative<int32_t>);
DEFINE_validator(cdc_scan_mem_limit_bytes, &ValidateNonNegative<int64_t>);
DEFINE_validator(cdc_active_time_report_interval_ms, &ValidateNonNegative<int64_t>);
DEFINE_validator(cdc_checkpoint_persist_interval_ms, &ValidateNonNegative<int64_t>);
DEFINE_validator(cdc_snapshot_cursor_persist_interval_ms, &ValidateNonNegative<int64_t>);
DEFINE_validator(cdc_checkpoint_flush_interval_ms, &ValidateNonNegative<int32_t>);
DEFINE_validator(cdc_stream_idle_expiry_ms, &ValidateNonNegative<int64_t>);
DEFINE_validator(cdc_session_history_floor_max_age_secs, &ValidateNonNegative<int64_t>);
DEFINE_validator(cdc_stream_config_negative_cache_ttl_ms, &ValidateNonNegative<int32_t>);
DEFINE_validator(cdc_stream_config_cache_ttl_ms, &ValidateNonNegative<int32_t>);
DEFINE_validator(cdc_barrier_reorder_window_secs, &ValidateNonNegative<int32_t>);
GROUP_FLAG_VALIDATOR(cdc_response_size_flags, ValidateCdcResponseSizeFlags);

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
    "Number of durable checkpoint writes queued for the master by CDC Checkpoint "
    "requests. Rate-limited per (stream, tablet) by "
    "--cdc_checkpoint_persist_interval_ms: a checkpoint that the throttle holds "
    "back is deferred, and write-combined with the later ones of the interval, "
    "so this is at most cdc_checkpoint_requests. A ratio near 1.0 under steady "
    "polling indicates the throttle is ineffective (interval too low).",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(
    server, cdc_barrier_updates_superseded, "CDC Barrier Updates Superseded",
    kudu::MetricUnit::kRequests,
    "Number of CDC retention barrier updates from the master that were ignored "
    "because a newer update had already been applied to the tablet replica. "
    "Occasional increments are expected (barrier updates are unordered); a "
    "steady rate means a master that is no longer the leader is still sending "
    "updates.",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(
    server, cdc_checkpoint_persist_failures, "CDC Checkpoint Persist Failures",
    kudu::MetricUnit::kRequests,
    "Number of times a CDC checkpoint update failed to reach the master. Each "
    "attempt that fails counts every update it carried, so while the masters "
    "are unreachable this grows by the number of queued updates per flush "
    "interval; the updates are retried. A non-zero rate while consumers are "
    "advancing means the master's last_checkpoint_advance_time_micros is "
    "falling behind and the staleness guard's advance-attempt grace floor "
    "is suppressing spurious barrier releases. Operators should "
    "investigate master connectivity and sys-catalog health.",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_activity_reports, "CDC Activity Reports",
    kudu::MetricUnit::kRequests,
    "Number of consumer-activity reports this tablet server sent to the master, "
    "at most one per (stream, tablet) per --cdc_active_time_report_interval_ms. "
    "A checkpoint or snapshot-cursor persist refreshes the activity as well, so "
    "no report is sent within that interval after one.",
    kudu::MetricLevel::kDebug);
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
    "cdc_scans_rejected_* and cdc_wal_reads_rejected_* for the reason "
    "breakdown)",
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
METRIC_DEFINE_counter(
    server, cdc_errors_decode_failed, "CDC Errors: DECODE_FAILED",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned DECODE_FAILED (a WAL op could not be "
    "decoded into change records; the stream cannot advance past it until an "
    "operator intervenes)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_response_too_large, "CDC Errors: RESPONSE_TOO_LARGE",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned RESPONSE_TOO_LARGE (the records of "
    "one op do not fit in a response; raise --rpc_max_message_size)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_tablet_not_in_stream, "CDC Errors: TABLET_NOT_IN_STREAM",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned TABLET_NOT_IN_STREAM (the tablet is "
    "not a tablet of the stream's tables)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_schema_unavailable, "CDC Errors: SCHEMA_UNAVAILABLE",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned SCHEMA_UNAVAILABLE (the schema the "
    "requested ops were applied with is no longer known; the consumer must "
    "take a new snapshot)",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_errors_tablet_dropped, "CDC Errors: TABLET_DROPPED",
    kudu::MetricUnit::kRequests,
    "Number of CDC requests that returned TABLET_DROPPED (the tablet's range "
    "partition was dropped and the stream has every change of the tablet)",
    kudu::MetricLevel::kInfo);

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
METRIC_DEFINE_counter(
    server, cdc_wal_reads_rejected_memory,
    "CDC WAL Reads Rejected: Memory Budget",
    kudu::MetricUnit::kRequests,
    "Number of CHANGE-mode CDC GetChanges calls shed because the "
    "--cdc_wal_read_mem_limit_bytes budget was exhausted. The server returns "
    "SERVER_TOO_BUSY (retryable) rather than admitting the read and risking "
    "untracked heap growth.",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_wal_read_escalations,
    "CDC WAL Read Escalations",
    kudu::MetricUnit::kRequests,
    "Number of times a CDC GetChanges call re-read the WAL with a larger read "
    "window to reach the commit of a transaction whose WAL span exceeds the "
    "per-response byte cap. Each re-read starts at the transaction's first "
    "write, so a high rate means large transactions are re-read often.",
    kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(
    server, cdc_write_ops_decoded,
    "CDC Write Ops Decoded",
    kudu::MetricUnit::kOperations,
    "Number of WAL write ops that CDC GetChanges calls decoded into change "
    "records (in FULL mode, each also reconstructs before/after images). An op "
    "is decoded again if the response it was decoded for could not carry its "
    "records, so a rate well above the rate of streamed write ops means "
    "responses re-decode the same ops.",
    kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(
    server, cdc_full_image_rows_read,
    "CDC FULL Image Rows Read",
    kudu::MetricUnit::kRows,
    "Number of rows that FULL-mode CDC GetChanges calls read from tablets to "
    "reconstruct before/after images: about two per row changed by a streamed "
    "op other than an INSERT.",
    kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(
    server, cdc_duplicate_consumers_detected,
    "CDC Duplicate Consumers Detected",
    kudu::MetricUnit::kRequests,
    "Number of times GetChanges calls for one (stream, tablet) came from one "
    "remote address, then another, then the first again within a minute: two "
    "consumers are reading one stream. Each advances the stream's shared "
    "checkpoint, so each may skip changes that only the other one applied.",
    kudu::MetricLevel::kWarn);
METRIC_DEFINE_counter(
    server, cdc_wal_reads_rejected_outcome_pending,
    "CDC WAL Reads Rejected: Op Outcome Pending",
    kudu::MetricUnit::kRequests,
    "Number of CDC GetChanges calls that returned SERVER_TOO_BUSY (retryable) "
    "because the outcome of the first op to stream was not known yet: the op "
    "was committed but not yet applied, or its COMMIT entry was not yet "
    "written to the WAL. Sustained growth indicates that applying writes lags "
    "behind replicating them on this tablet server, not load shedding.",
    kudu::MetricLevel::kInfo);

METRIC_DEFINE_gauge_int64(
    server, cdc_max_sent_lag_micros, "CDC Max Consumer Sent Lag",
    kudu::MetricUnit::kMicroseconds,
    "Across all active CDC (stream, tablet) sessions on this server, the maximum "
    "wall-clock lag between now and the physical time of the newest record sent "
    "to the consumer. A session whose consumer has read everything the tablet "
    "committed reports 0. High values indicate a lagging or stuck consumer.",
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
METRIC_DEFINE_gauge_uint64(
    server, cdc_checkpoint_updates_pending, "CDC Checkpoint Updates Pending",
    kudu::MetricUnit::kEntries,
    "Number of CDC checkpoint updates (checkpoints, snapshot cursors and activity "
    "reports) this tablet server has queued for the master, at most one per "
    "(stream, tablet). It grows while the masters are unreachable.",
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
    "the physical time of the newest record sent to the consumer, or 0 when the "
    "consumer has read everything the tablet committed. High values indicate a "
    "lagging or stuck consumer; see cdc_stream_active_age_micros for a consumer "
    "that stopped polling.",
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

// Byte-throughput counter, next to the record counts. Counts serialized proto
// bytes of every successful GetChanges response, covering both WAL-streaming and
// snapshot bootstrap paths. Together with cdc_records_produced it gives average
// record size; growth rate reveals bandwidth saturation and large-row storms.
METRIC_DEFINE_counter(
    server, cdc_bytes_sent, "CDC Bytes Sent",
    kudu::MetricUnit::kBytes,
    "Cumulative payload bytes returned to CDC consumers across all successful "
    "GetChanges calls on this tablet server (serialized proto response size, "
    "both WAL-streaming and snapshot bootstrap paths). Together with "
    "cdc_records_produced this gives an average per-record byte size; sustained "
    "high growth rate reveals bandwidth saturation or large-row storms. "
    "Per-stream byte granularity is deferred.",
    kudu::MetricLevel::kInfo);

// Response-size histogram. Records the serialized proto byte size
// of every GetChanges response that carried at least one record
// (records_size() > 0). Empty/heartbeat polls are excluded so the distribution
// reflects real payload sizes. p50/p95/p99 reveal whether responses are
// consistently near the --cdc_max_bytes_per_response ceiling. A consistently
// high p99 relative to the ceiling may indicate consumer-side memory spikes on
// burst traffic.
// Max bucket: 16 MiB -- 2x the default --cdc_max_bytes_per_response (8 MiB)
// so operators who raise the flag are still tracked accurately.
// Modeled after ranger_server_inbound_queue_size_bytes (4*1024*1024, 1) in
// src/kudu/ranger/ranger_client.cc; same unit/significant-digit convention.
METRIC_DEFINE_histogram(
    server, cdc_get_changes_response_bytes,
    "CDC GetChanges Response Bytes",
    kudu::MetricUnit::kBytes,
    "Histogram of serialized proto payload sizes (bytes) of GetChanges "
    "responses that contained at least one record. Empty/heartbeat responses "
    "(records_size() == 0) are excluded so the distribution reflects the "
    "actual payload-size profile. Percentiles p50/p95/p99 reveal whether "
    "responses are consistently near the --cdc_max_bytes_per_response ceiling; "
    "a high p99 close to the ceiling may indicate consumer-side memory spikes "
    "on burst traffic.",
    kudu::MetricLevel::kInfo,
    16 * 1024 * 1024, 1);

// Per-stream time-to-expiry gauge. Exposes the remaining idle budget
// directly as a countdown, so operators can alert on "expires in < 30m"
// without mental arithmetic against cdc_stream_active_age_micros.
METRIC_DEFINE_gauge_int64(
    cdc_stream, cdc_stream_time_to_expiry_micros,
    "CDC Stream Time to Expiry",
    kudu::MetricUnit::kMicroseconds,
    "For this CDC (stream, tablet) session, the remaining time in microseconds "
    "before the stream expires due to inactivity: the stream's expiry (as "
    "reported by the master) minus the time since the last consumer activity, "
    "clamped to 0. A value of 0 means the stream has already expired or is at "
    "the edge. Returns INT64_MAX when expiry is disabled. Recomputed at every "
    "scrape so it is always fresh.",
    kudu::MetricLevel::kWarn);

// Per-stream consumer-applied (end-to-end) committed lag.
// Sourced from CheckpointRequestPB.last_applied_phystime_micros, which the
// reference consumer populates with the physical timestamp of the last record it
// has fully applied (DecodeAndDeliver'd). Returns -1 when no applied time has
// been received from the consumer (field not set / old consumer). Returns the
// non-negative lag once the consumer starts reporting. Recomputed at every scrape.
METRIC_DEFINE_gauge_int64(
    cdc_stream, cdc_stream_committed_lag_micros,
    "CDC Stream Consumer Committed Lag",
    kudu::MetricUnit::kMicroseconds,
    "For this CDC (stream, tablet) session, the wall-clock lag between now and "
    "the physical timestamp of the last operation the consumer has fully applied "
    "(i.e. committed downstream), or 0 when the consumer has read and "
    "checkpointed everything the tablet committed. Populated via "
    "CheckpointRequestPB.last_applied_phystime_micros. Returns -1 when no applied "
    "time has ever been reported (old consumer or no Checkpoint received yet).",
    kudu::MetricLevel::kInfo);

// Server-level aggregate of consumer-applied committed lag.
// Max across all active (stream, tablet) sessions that have reported at least one
// applied timestamp. Returns -1 when NO session has reported yet.
METRIC_DEFINE_gauge_int64(
    server, cdc_max_committed_lag_micros,
    "CDC Max Consumer Committed Lag",
    kudu::MetricUnit::kMicroseconds,
    "Across all active CDC (stream, tablet) sessions on this server, the maximum "
    "wall-clock lag between now and the physical timestamp of the last operation "
    "each consumer has fully applied; a caught-up session counts as 0. Returns -1 "
    "when no session has ever reported an applied timestamp (all consumers are old "
    "or no Checkpoint received yet). Sessions that have never reported are skipped "
    "rather than dragging the max to -1 when at least one session is reporting.",
    kudu::MetricLevel::kInfo);

using kudu::clock::HybridClock;
using kudu::consensus::CommitMsg;
using kudu::consensus::OpId;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::ReplicateMsg;
using kudu::log::Log;
using kudu::log::LogReader;
using kudu::tablet::TabletReplica;
using std::optional;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cdc {

namespace {

// The refresh time of a CDC retention barrier is persisted at this
// granularity, so after a restart the lease (--cdc_barrier_lease_secs) can be
// shorter by at most an eighth.
int64_t BarrierRefreshFlushIntervalMicros() {
  constexpr int64_t kDefaultLeaseSecs = 24 * 3600;
  const int64_t lease_secs =
      FLAGS_cdc_barrier_lease_secs > 0 ? FLAGS_cdc_barrier_lease_secs : kDefaultLeaseSecs;
  return lease_secs * 1000000 / 8;
}

// Verifies that 'req' carries a valid signed authz token granting SCAN
// privilege on 'table_id'. Called under --tserver_enforce_access_control, as
// for scans. Returns true if the caller is authorized. Otherwise responds at the
// RPC level with the same error codes the scan path uses, so a client's
// token-refresh logic works uniformly, and returns false. 'req' must expose
// has_authz_token()/authz_token() (GetChangesRequestPB and CheckpointRequestPB).
template <class Request>
bool AuthorizeCDCTableOrRespond(const security::TokenVerifier& token_verifier,
                                const Request& req,
                                const std::string& table_id,
                                rpc::RpcContext* context) {
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

// Maximum number of checkpoint updates in one UpdateCDCCheckpoints RPC (an
// update is about 100 bytes).
constexpr size_t kMaxCheckpointUpdatesPerRpc = 1000;

// Maximum number of remembered config-fetch failures (see
// CDCServiceImpl::stream_config_fetch_failures_).
constexpr size_t kMaxStreamConfigFetchFailures = 1000;

// Returns the UpdateCDCCheckpoint request for a checkpoint of (stream_id,
// tablet_id) at 'op_index'. 'history_safe_time_micros' is the history floor the
// checkpoint needs (0 for none). 'refresh_active_time_only' makes it an
// activity report, for which the master ignores the checkpoint fields.
// 'snapshot_cursor', if not null, is the durable snapshot cursor to persist.
// 'up_to_op_index', if >= 0, is the tablet's committed op index as last seen
// by GetChanges for this session; the master uses it to tell a caught-up
// consumer on an idle tablet from a stuck one.
unique_ptr<master::UpdateCDCCheckpointRequestPB> MakeCheckpointUpdate(
    const string& stream_id,
    const string& tablet_id,
    int64_t op_index,
    uint64_t history_safe_time_micros = 0,
    bool refresh_active_time_only = false,
    const SnapshotCursorUpdate* snapshot_cursor = nullptr,
    int64_t up_to_op_index = -1) {
  auto req = std::make_unique<master::UpdateCDCCheckpointRequestPB>();
  req->set_stream_id(stream_id);
  req->set_tablet_id(tablet_id);
  req->set_op_index(op_index);
  if (history_safe_time_micros > 0) {
    req->set_history_safe_time_micros(history_safe_time_micros);
  }
  if (refresh_active_time_only) {
    req->set_refresh_active_time_only(true);
  }
  if (up_to_op_index >= 0) {
    req->set_up_to_op_index(up_to_op_index);
  }
  // Durable snapshot-progress cursor (resumable snapshot bootstrap). Sent only by
  // the snapshot path; the master applies it under its generation/monotonicity
  // rule.
  if (snapshot_cursor != nullptr) {
    req->set_snapshot_snap_ts(snapshot_cursor->snap_ts);
    req->set_snapshot_resume_key(snapshot_cursor->resume_key);
    req->set_snapshot_active(snapshot_cursor->active);
  }
  return req;
}

// Merges 'update', the later of two updates for the same (stream, tablet),
// into 'pending', so that the master's row ends up as it would after applying
// both in order (see CatalogManager::UpdateCDCCheckpoint).
void MergeCheckpointUpdate(const master::UpdateCDCCheckpointRequestPB& update,
                           master::UpdateCDCCheckpointRequestPB* pending) {
  // Either report counts as progress if the checkpoint is caught up with the
  // index it saw, so keep the lower of the two. While the masters are
  // unreachable the updates keep being merged, so a consumer that was caught up
  // when the outage began is counted as progressing once the masters are
  // reachable again, even if it stopped during the outage.
  std::optional<int64_t> up_to;
  if (pending->has_up_to_op_index()) {
    up_to = pending->up_to_op_index();
  }
  if (update.has_up_to_op_index()) {
    up_to = std::min(up_to.value_or(update.up_to_op_index()), update.up_to_op_index());
  }
  if (update.refresh_active_time_only()) {
    // An activity report adds nothing else: any update refreshes the activity.
  } else if (pending->refresh_active_time_only()) {
    *pending = update;
  } else {
    // Set fields of 'update' win and unset ones keep the earlier value, which is
    // how the master applies them. The master keeps the highest checkpoint.
    const int64_t op_index = std::max(pending->op_index(), update.op_index());
    pending->MergeFrom(update);
    pending->set_op_index(op_index);
  }
  if (up_to) {
    pending->set_up_to_op_index(*up_to);
  }
}

// Erases (*map)[key] if the map holds its only reference. Requires the lock
// guarding 'map', under which every reference to an element is taken.
template <class Map, class Key>
void EraseIfUnused(Map* map, const Key& key) {
  auto it = map->find(key);
  if (it != map->end() && it->second.use_count() == 1) {
    map->erase(it);
  }
}

// Returns OK if the tablet's 'current' schema still has every column of the
// 'pinned' snapshot schema, with the same id, name and type, so that a page
// read with 'pinned' returns the same columns as the pages before it.
Status CheckSnapshotSchemaUnchanged(const Schema& pinned, const Schema& current) {
  for (size_t i = 0; i < pinned.num_columns(); ++i) {
    const ColumnSchema& col = pinned.column(i);
    const int idx = current.find_column_by_id(pinned.column_id(i));
    if (idx == Schema::kColumnNotFound ||
        current.column(idx).name() != col.name() ||
        current.column(idx).type_info()->type() != col.type_info()->type()) {
      return Status::IllegalState(Substitute(
          "column $0 of the snapshot schema was dropped, renamed or changed "
          "during the snapshot; restart the snapshot from the beginning",
          col.name()));
    }
  }
  return Status::OK();
}

// Lowers '*start_index', the op index after which a consumer streams the WAL
// of 'replica''s tablet, below 'min_begin_op_index', the lowest BEGIN_TXN op
// index of the transactions whose rows the consumer must get from the stream
// (std::nullopt if there are none). The stream emits a transaction's rows when
// it commits, from the ops it read since BEGIN_TXN, so starting after one of
// its writes would lose that write. Never lowers the start below the oldest op
// the WAL still holds.
void LowerStreamingStartBelowTxns(const TabletReplica* replica,
                                  std::optional<int64_t> min_begin_op_index,
                                  int64_t* start_index) {
  if (!min_begin_op_index || *min_begin_op_index > *start_index) {
    return;
  }
  int64_t lowered = *min_begin_op_index - 1;
  const Log* log = replica->log();
  const std::shared_ptr<LogReader> reader = log ? log->reader() : nullptr;
  if (reader) {
    lowered = std::max(lowered, reader->GetMinReplicateIndex() - 1);
  }
  *start_index = std::max<int64_t>(0, std::min(*start_index, lowered));
}

// Whether 'replica', which leads a tablet whose range partition was dropped,
// has no change after op 'op_index': it rejects writes, no op is pending, and
// the ops after 'op_index' up to the last committed one, if any, change no
// rows (such as the NO_OP of a new leader). Sets '*last_op_index' to the index
// of the last committed op if so.
bool DroppedTabletHasNoChangesAfter(const TabletReplica& replica,
                                    int64_t op_index,
                                    int64_t* last_op_index) {
  if (!replica.rejects_writes_of_dropped_tablet()) {
    return false;
  }
  // Every write that was not rejected is a pending op until it is applied, and
  // so committed (see TabletReplica::RejectWritesOfDroppedTablet()).
  if (!replica.op_tracker()->GetPendingOps().empty()) {
    return false;
  }
  const std::shared_ptr<consensus::RaftConsensus> consensus = replica.shared_consensus();
  const std::optional<OpId> committed =
      consensus ? consensus->GetLastOpId(consensus::COMMITTED_OPID) : std::nullopt;
  if (!committed) {
    return false;
  }
  if (op_index < committed->index()) {
    // Only the ops of leader elections and config changes may follow the last
    // change, so there are few of them.
    constexpr int64_t kMaxOpsToCheck = 64;
    const Log* log = replica.log();
    const std::shared_ptr<LogReader> reader = log ? log->reader() : nullptr;
    if (!reader || committed->index() - op_index > kMaxOpsToCheck) {
      return false;
    }
    vector<ReplicateMsg*> replicates;
    ElementDeleter deleter(&replicates);
    if (!reader->ReadReplicatesInRange(op_index + 1, committed->index(),
                                       LogReader::kNoSizeLimit, &replicates).ok()) {
      return false;
    }
    for (const ReplicateMsg* r : replicates) {
      if (r->op_type() != consensus::NO_OP && r->op_type() != consensus::CHANGE_CONFIG_OP) {
        return false;
      }
    }
  }
  *last_op_index = committed->index();
  return true;
}

// Describes the tablet of 'replica', whose last op is 'last_op_index', as a
// dropped tablet.
void FillDroppedTablet(const TabletReplica& replica,
                       int64_t last_op_index,
                       CDCDroppedTabletPB* dropped) {
  const auto& meta = replica.tablet_metadata();
  meta->partition().ToPB(dropped->mutable_partition());
  dropped->set_partition_description(
      meta->partition_schema().PartitionDebugString(meta->partition(), *meta->schema()));
  dropped->set_last_op_index(last_op_index);
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// CDCServiceImpl
// ---------------------------------------------------------------------------

CDCServiceImpl::CDCServiceImpl(tserver::TabletServer* server,
                               std::function<bool()> enforce_access_control)
    : CDCServiceIf(server->metric_entity(), server->result_tracker()),
      server_(server),
      enforce_access_control_(std::move(enforce_access_control)) {
  DCHECK(server_);
  DCHECK(enforce_access_control_);

  // Dedicated child of the server root tracker for CDC heavy-scan responses.
  // Created without a hard limit; --cdc_scan_mem_limit_bytes is enforced against
  // this tracker's consumption at reservation time so it can be tuned at runtime.
  scan_mem_tracker_ = MemTracker::CreateTracker(
      -1, "cdc_scans", server_->mem_tracker());

  // Separate tracker for CHANGE-mode WAL reads, bounded by
  // --cdc_wal_read_mem_limit_bytes. Intentionally separate from scan_mem_tracker_
  // so CHANGE-mode streaming (the hot path) has its own soft cap and does not
  // count against the concurrency limit that guards only heavy FULL/snapshot scans.
  wal_read_mem_tracker_ = MemTracker::CreateTracker(
      -1, "cdc_wal_reads", server_->mem_tracker());

  const auto& entity = server->metric_entity();
  get_changes_requests_ = METRIC_cdc_get_changes_requests.Instantiate(entity);
  records_produced_ = METRIC_cdc_records_produced.Instantiate(entity);
  checkpoint_requests_ = METRIC_cdc_checkpoint_requests.Instantiate(entity);
  checkpoint_persists_ = METRIC_cdc_checkpoint_persists.Instantiate(entity);
  checkpoint_persist_failures_ = METRIC_cdc_checkpoint_persist_failures.Instantiate(entity);
  barrier_updates_superseded_ = METRIC_cdc_barrier_updates_superseded.Instantiate(entity);
  activity_reports_ = METRIC_cdc_activity_reports.Instantiate(entity);
  duplicate_consumers_detected_ = METRIC_cdc_duplicate_consumers_detected.Instantiate(entity);
  errors_ = METRIC_cdc_errors.Instantiate(entity);

  // Per-error-code counters -- index matches CDCErrorPB::Code (1-18).
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
  error_code_counters_[CDCErrorPB::DECODE_FAILED] =
      METRIC_cdc_errors_decode_failed.Instantiate(entity);
  error_code_counters_[CDCErrorPB::RESPONSE_TOO_LARGE] =
      METRIC_cdc_errors_response_too_large.Instantiate(entity);
  error_code_counters_[CDCErrorPB::TABLET_NOT_IN_STREAM] =
      METRIC_cdc_errors_tablet_not_in_stream.Instantiate(entity);
  error_code_counters_[CDCErrorPB::SCHEMA_UNAVAILABLE] =
      METRIC_cdc_errors_schema_unavailable.Instantiate(entity);
  error_code_counters_[CDCErrorPB::TABLET_DROPPED] =
      METRIC_cdc_errors_tablet_dropped.Instantiate(entity);

  // Admission-shed breakdown counters.
  scans_rejected_concurrency_ =
      METRIC_cdc_scans_rejected_concurrency.Instantiate(entity);
  scans_rejected_memory_ =
      METRIC_cdc_scans_rejected_memory.Instantiate(entity);
  scans_rejected_server_memory_ =
      METRIC_cdc_scans_rejected_server_memory.Instantiate(entity);
  scans_rejected_worker_pool_ =
      METRIC_cdc_scans_rejected_worker_pool.Instantiate(entity);
  wal_reads_rejected_memory_ =
      METRIC_cdc_wal_reads_rejected_memory.Instantiate(entity);
  wal_reads_rejected_outcome_pending_ =
      METRIC_cdc_wal_reads_rejected_outcome_pending.Instantiate(entity);
  wal_read_escalations_ = METRIC_cdc_wal_read_escalations.Instantiate(entity);
  write_ops_decoded_ = METRIC_cdc_write_ops_decoded.Instantiate(entity);
  full_image_rows_read_ = METRIC_cdc_full_image_rows_read.Instantiate(entity);

  // Byte-throughput counter.
  bytes_sent_ = METRIC_cdc_bytes_sent.Instantiate(entity);

  // Response-size histogram.
  get_changes_response_bytes_ = METRIC_cdc_get_changes_response_bytes.Instantiate(entity);

  METRIC_cdc_max_sent_lag_micros.InstantiateFunctionGauge(
      entity, [this]() { return this->MaxSentLagMicros(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_max_active_age_micros.InstantiateFunctionGauge(
      entity, [this]() { return this->MaxActiveAgeMicros(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_active_streams.InstantiateFunctionGauge(
      entity, [this]() { return this->ActiveStreamCount(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_checkpoint_updates_pending.InstantiateFunctionGauge(
      entity, [this]() { return this->PendingCheckpointUpdates(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_max_ops_behind.InstantiateFunctionGauge(
      entity, [this]() { return this->MaxOpsBehind(); })
      ->AutoDetach(&metric_detacher_);
  METRIC_cdc_bootstrap_required_streams.InstantiateFunctionGauge(
      entity, [this]() { return this->BootstrapRequiredStreamCount(); })
      ->AutoDetach(&metric_detacher_);
  // Server-level max consumer-applied committed lag.
  METRIC_cdc_max_committed_lag_micros.InstantiateFunctionGauge(
      entity, [this]() { return this->MaxCommittedLagMicros(); })
      ->AutoDetach(&metric_detacher_);
}

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
    case CDCErrorPB::SCHEMA_VERSION_MISMATCH:
      *is_retryable = true;
      break;
    case CDCErrorPB::WAL_EXPIRED:
    case CDCErrorPB::HISTORY_EXPIRED:
    case CDCErrorPB::STREAM_EXPIRED:
    case CDCErrorPB::SNAPSHOT_SESSION_LOST:
    case CDCErrorPB::SCHEMA_UNAVAILABLE:
      *needs_resnapshot = true;
      break;
    default:
      // UNKNOWN_ERROR, STREAM_NOT_FOUND, NOT_AUTHORIZED (not sent),
      // TRANSACTION_TOO_LARGE, DECODE_FAILED, RESPONSE_TOO_LARGE,
      // TABLET_NOT_IN_STREAM: fatal, requires operator action. TABLET_DROPPED:
      // the tablet has no more changes.
      break;
  }
}

Status StreamConfigFromMaster(const master::CDCStreamConfigPB& master_config,
                              CDCStreamConfigPB* config) {
  // The two messages have the same fields, so the master's one parses as this
  // server's. Fields and enum values this server does not know end up in the
  // unknown fields of either.
  CDCStreamConfigPB result;
  if (!result.ParseFromString(master_config.SerializeAsString())) {
    return Status::Corruption("could not parse the CDC stream configuration");
  }
  if (!result.unknown_fields().empty()) {
    return Status::NotSupported(
        "the CDC stream configuration has options that this tablet server does not "
        "support; upgrade the tablet server");
  }
  *config = std::move(result);
  return Status::OK();
}

ChangesEmitPlan PlanChangesEmission(const vector<ChangesEmitGroupInfo>& groups,
                                    int64_t open_min_index,
                                    int64_t end_index,
                                    int64_t base_records,
                                    int64_t base_bytes,
                                    const ChangesEmitLimits& limits) {
  constexpr int64_t kNone = std::numeric_limits<int64_t>::max();
  const size_t n = groups.size();
  // Groups visible at or after the first write of an open transaction wait for
  // it; since 'groups' are in visibility order, those are a suffix.
  size_t num_eligible = 0;
  while (num_eligible < n && groups[num_eligible].effective_index < open_min_index) {
    DCHECK(num_eligible == 0 ||
           groups[num_eligible - 1].effective_index < groups[num_eligible].effective_index);
    ++num_eligible;
  }
  // pinned[k]: the checkpoint must stay below this if groups [k, n) are left
  // out.
  vector<int64_t> pinned(n + 1);
  pinned[n] = open_min_index;
  for (size_t j = n; j > 0; --j) {
    pinned[j - 1] = std::min(pinned[j], groups[j - 1].min_index);
  }
  // Emitting groups [0, k) needs a checkpoint at or past the last of them and
  // below every group left out.
  const auto valid = [&](size_t k) {
    return k == 0 || groups[k - 1].effective_index < pinned[k];
  };

  // The most groups the limits allow; the first group is always included.
  const int64_t max_bytes = std::min(limits.max_bytes, limits.hard_max_bytes);
  int64_t records = base_records;
  int64_t bytes = base_bytes;
  size_t num_fit = 0;
  for (; num_fit < num_eligible; ++num_fit) {
    const auto& g = groups[num_fit];
    if (num_fit > 0 &&
        ((limits.max_records > 0 && records >= limits.max_records) ||
         bytes + g.num_bytes > max_bytes)) {
      break;
    }
    records += g.num_records;
    bytes += g.num_bytes;
  }

  ChangesEmitPlan plan;
  size_t k = num_fit;
  while (k > 0 && !valid(k)) {
    --k;
  }
  if (k == 0 && num_fit > 0) {
    // Not even the first group can be emitted alone: emit up to the first
    // point after it that allows a checkpoint.
    for (k = num_fit + 1; k <= num_eligible && !valid(k); ++k) {}
    if (k > num_eligible) {
      k = 0;
    }
  }
  plan.num_groups = k;
  int64_t emitted_bytes = base_bytes;
  for (size_t j = 0; j < k; ++j) {
    emitted_bytes += groups[j].num_bytes;
  }
  plan.too_large = k > 0 && emitted_bytes > limits.hard_max_bytes;
  plan.truncated = k > 0 && k < num_eligible;
  if (plan.truncated) {
    plan.checkpoint_op_index = groups[k - 1].effective_index;
  } else {
    plan.checkpoint_op_index = pinned[k] == kNone ? end_index : pinned[k] - 1;
  }
  return plan;
}

namespace {
// The Raft role of 'replica', or UNKNOWN_ROLE if it is not RUNNING. The role of
// a replica that is not running is not read: it may share its consensus
// metadata with a tablet copy, which updates the metadata without the
// consensus lock. Takes the consensus lock.
RaftPeerPB::Role RoleIfRunning(const TabletReplica* replica) {
  if (!replica->CheckRunning().ok()) {
    return RaftPeerPB::UNKNOWN_ROLE;
  }
  const std::shared_ptr<consensus::RaftConsensus> consensus = replica->shared_consensus();
  return consensus ? consensus->role() : RaftPeerPB::UNKNOWN_ROLE;
}

// The CDC error code for a failed read 's' that did not set one itself.
// Timeouts, shed load and I/O errors are transient. An I/O error of a replica
// that is still running, such as a failed read of a WAL segment or of the
// history of a row, is retried: if the disk failed, the replica is failed
// (or the server stops), and the consumer moves on to another leader. Other
// errors, such as Corruption, are not retried.
CDCErrorPB::Code ReadErrorCode(const Status& s) {
  if (s.IsNotFound()) return CDCErrorPB::TABLET_NOT_FOUND;
  if (s.IsIllegalState()) return CDCErrorPB::TABLET_NOT_LEADER;
  if (s.IsTimedOut() || s.IsServiceUnavailable() || s.IsIOError()) {
    return CDCErrorPB::SERVER_TOO_BUSY;
  }
  return CDCErrorPB::UNKNOWN_ERROR;
}

// The sent lag of 'st' at 'now_micros': the wall-clock time since the newest
// record sent to the consumer, or 0 if nothing was sent yet or the consumer has
// caught up with the tablet.
int64_t SessionSentLagMicros(const CDCStreamTabletState& st, int64_t now_micros) {
  const uint64_t sent = st.last_sent_record_phys_micros.load(std::memory_order_relaxed);
  if (sent == 0 || st.caught_up.load(std::memory_order_relaxed)) {
    return 0;
  }
  return std::max<int64_t>(0, now_micros - static_cast<int64_t>(sent));
}

// The committed lag of 'st' at 'now_micros': the wall-clock time since the
// newest record the consumer reported as applied, 0 if it has caught up with
// the tablet and checkpointed everything, or -1 if it never reported.
int64_t SessionCommittedLagMicros(const CDCStreamTabletState& st, int64_t now_micros) {
  const int64_t applied = st.last_applied_phystime_micros.load(std::memory_order_relaxed);
  if (applied == 0) {
    return -1;
  }
  if (st.caught_up.load(std::memory_order_relaxed) &&
      st.checkpoint_op_index.load(std::memory_order_relaxed) >=
          st.last_known_up_to_op_index.load(std::memory_order_relaxed)) {
    return 0;
  }
  return std::max<int64_t>(0, now_micros - applied);
}

// The CDC error code for a failed TrackSession() call: the stream was deleted
// while the call was in flight, the replica stopped, or the service is
// shutting down.
CDCErrorPB::Code TrackSessionErrorCode(const Status& s) {
  if (s.IsNotFound()) return CDCErrorPB::STREAM_NOT_FOUND;
  if (s.IsIllegalState()) return CDCErrorPB::TABLET_NOT_RUNNING;
  if (s.IsServiceUnavailable()) return CDCErrorPB::SERVER_TOO_BUSY;
  return CDCErrorPB::UNKNOWN_ERROR;
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
  // [1, 18] falls back to UNKNOWN_ERROR (index 1) defensively.
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
    max_lag = std::max(max_lag, SessionSentLagMicros(*entry.second, now_micros));
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

uint64_t CDCServiceImpl::PendingCheckpointUpdates() const {
  std::lock_guard lock(flush_lock_);
  return pending_checkpoint_updates_.size();
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

int64_t CDCServiceImpl::MaxCommittedLagMicros() const {
  const int64_t now_micros = GetCurrentTimeMicros();
  int64_t max_lag = -1;  // -1: no session has ever reported an applied time
  std::lock_guard lock(lock_);
  for (const auto& entry : stream_tablet_state_) {
    // A session that never reported (-1) does not lower the max below 0.
    max_lag = std::max(max_lag, SessionCommittedLagMicros(*entry.second, now_micros));
  }
  return max_lag;
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
      [st]() { return SessionSentLagMicros(*st, GetCurrentTimeMicros()); })
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
  // Time-to-expiry countdown gauge.
  // Reads only the atomic last_active_time_micros and the runtime gflag --
  // no lock, no dangling captures (same pattern as the sibling gauges above).
  // Returns INT64_MAX when expiry is disabled (flag <= 0) so alerting rules
  // can distinguish "no expiry configured" from "stream about to expire".
  METRIC_cdc_stream_time_to_expiry_micros.InstantiateFunctionGauge(
      state->metric_entity,
      [st]() -> int64_t {
        const int64_t stream_expiry_ms = st->idle_expiry_ms.load(std::memory_order_relaxed);
        const int64_t expiry_ms =
            stream_expiry_ms >= 0 ? stream_expiry_ms : FLAGS_cdc_stream_idle_expiry_ms;
        if (expiry_ms <= 0) {
          return std::numeric_limits<int64_t>::max();  // expiry disabled
        }
        const int64_t expiry_us = expiry_ms * 1000;
        const int64_t last_active =
            st->last_active_time_micros.load(std::memory_order_relaxed);
        if (last_active == 0) {
          return expiry_us;  // no activity recorded yet; full budget remains
        }
        const int64_t idle_us = GetCurrentTimeMicros() - last_active;
        const int64_t remaining = expiry_us - idle_us;
        return remaining > 0 ? remaining : 0;
      })
      ->AutoDetach(&state->metric_detacher);
  // Per-stream consumer-applied committed lag.
  // Returns -1 when no applied timestamp has been received (old consumer or no
  // Checkpoint yet); returns max(0, now - last_applied) once the consumer
  // starts reporting. Reads only the atomic last_applied_phystime_micros -- no
  // lock, no dangling captures (same pattern as the sent_lag gauge above).
  METRIC_cdc_stream_committed_lag_micros.InstantiateFunctionGauge(
      state->metric_entity,
      [st]() { return SessionCommittedLagMicros(*st, GetCurrentTimeMicros()); })
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
  Shutdown();
}

Status CDCServiceImpl::Start() {
  return Thread::Create("cdc", "cdc-checkpoint-flusher",
                        [this]() { this->CheckpointFlusherThread(); },
                        &flusher_thread_);
}

void CDCServiceImpl::Shutdown() {
  // The updates still queued are dropped: the durable checkpoint lags, which
  // the at-least-once contract allows.
  StopCheckpointFlusher();
  // Stop new sessions from being created, then end the existing ones tablet
  // by tablet. The replicas are looked up outside 'lock_'.
  unordered_set<string> tablet_ids;
  {
    std::lock_guard lock(lock_);
    if (shutting_down_) {
      return;
    }
    shutting_down_ = true;
    for (const auto& entry : stream_tablet_state_) {
      tablet_ids.insert(entry.first.tablet_id);
    }
  }
  for (const auto& tablet_id : tablet_ids) {
    EndSessionsForTablet(tablet_id);
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
  // A request can ask for less than the server's cap, but not for more; 0 or
  // less means the cap.
  const int64_t max_bytes = req->has_max_bytes() && req->max_bytes() > 0
      ? std::min(req->max_bytes(), FLAGS_cdc_max_bytes_per_response)
      : FLAGS_cdc_max_bytes_per_response;

  resp->set_tablet_id(tablet_id);
  resp->set_checkpoint_op_index(-1);
  get_changes_requests_->Increment();

  // RPC-worker reservation: cap concurrent GetChanges calls at
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

  scoped_refptr<TabletReplica> replica;
  {
    Status s = server_->tablet_manager()->GetTabletReplica(tablet_id, &replica);
    if (!s.ok()) {
      SetCDCError(CDCErrorPB::TABLET_NOT_FOUND, s, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
  }

  // Fine-grained authorization: require SCAN privilege on the tablet's table.
  if (enforce_access_control_() &&
      !AuthorizeCDCTableOrRespond(server_->token_verifier(), *req,
                                  replica->tablet_metadata()->table_id(), context)) {
    errors_->Increment();
    return;  // helper already responded at the RPC level
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

  // Only the leader serves CDC. Check it before the stream-config fetch below,
  // so that a request this server cannot serve costs no master round trip.
  // ReadChanges() and ReadSnapshot() check again; a replica that is not running
  // is left to them, so that it is reported as TABLET_NOT_RUNNING.
  {
    const auto consensus = replica->shared_consensus();
    if (consensus && replica->CheckRunning().ok() &&
        consensus->role() != RaftPeerPB::LEADER) {
      SetCDCError(CDCErrorPB::TABLET_NOT_LEADER,
                  Status::IllegalState(Substitute(
                      "tablet $0 is not the leader (role: $1)", tablet_id,
                      RaftPeerPB::Role_Name(consensus->role()))),
                  resp->mutable_error());
      context->RespondSuccess();
      return;
    }
  }

  // Validate the stream is known to the master before serving any data. An
  // authoritative NotFound (the stream was deleted or never existed) is
  // surfaced as STREAM_NOT_FOUND rather than silently served as CHANGE-mode WAL
  // data. A transient fetch failure is tolerated if GetOrFetchStreamConfig has
  // a stale cached entry to serve. Without one, the call is shed as retryable:
  // without the config, the server cannot tell the record type, the snapshot
  // mode, or which tables the stream covers. The result is used by both the
  // snapshot and streaming branches below; it is fetched once per call.
  CDCStreamConfigPB config;
  {
    vector<string> stream_table_ids;
    Status cs = GetOrFetchStreamConfig(
        stream_id, SafeClientDeadline(MonoTime::Now(), context->GetClientDeadline()),
        &config, &stream_table_ids);
    if (cs.IsNotFound()) {
      SetCDCError(CDCErrorPB::STREAM_NOT_FOUND, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    if (cs.IsNotSupported()) {
      // The stream needs a newer tablet server: retrying here cannot help.
      SetCDCError(CDCErrorPB::UNKNOWN_ERROR, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    if (!cs.ok()) {
      KLOG_EVERY_N_SECS(WARNING, 60)
          << "CDC: shedding GetChanges for stream " << stream_id << ": "
          << cs.ToString();
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    cs = CheckTabletInStream(stream_id, *replica.get(), stream_table_ids);
    if (!cs.ok()) {
      SetCDCError(CDCErrorPB::TABLET_NOT_IN_STREAM, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
  }

  // The range partition of the tablet was dropped. A consumer that read every
  // change of the tablet is told so. A snapshot of the dropped rows is of no
  // use, so the snapshot of a tablet that has no more changes ends that way
  // too.
  if (replica->rejects_writes_of_dropped_tablet()) {
    const bool is_snapshot = req->is_snapshot_start() || req->has_snapshot_resume_key();
    int64_t last_op_index;
    if (DroppedTabletHasNoChangesAfter(
            *replica.get(), is_snapshot ? std::numeric_limits<int64_t>::max() : from_op_index,
            &last_op_index)) {
      SetCDCError(CDCErrorPB::TABLET_DROPPED,
                  Status::NotFound(Substitute(
                      "the range partition of tablet $0 was dropped, and op index $1 "
                      "is its last op", tablet_id, last_op_index)),
                  resp->mutable_error());
      FillDroppedTablet(*replica.get(), last_op_index, resp->mutable_dropped_tablet());
      RecordActivity(stream_id, tablet_id, context->remote_address().ToString());
      context->RespondSuccess();
      return;
    }
  }

  // Snapshot protocol: a request that starts or continues a snapshot is served
  // by ReadSnapshot instead of the WAL-streaming path.
  if (req->is_snapshot_start() || req->has_snapshot_resume_key()) {
    if (req->read_only()) {
      SetCDCError(CDCErrorPB::UNKNOWN_ERROR,
                  Status::InvalidArgument("a read-only request cannot take a snapshot"),
                  resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    if (config.snapshot_mode() == CDCStreamConfigPB::NEVER) {
      SetCDCError(CDCErrorPB::UNKNOWN_ERROR,
                  Status::InvalidArgument(
                      "snapshot not allowed for this stream (snapshot_mode=NEVER)"),
                  resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    // Snapshot pages use the smaller snapshot cap so the initial bulk scan
    // produces lighter responses than WAL streaming.
    // Not const so the true-up below can update it to the
    // actual response size before the release_slot cleanup fires.
    int64_t snap_max_bytes = FLAGS_cdc_snapshot_max_bytes_per_response > 0
        ? std::min(max_bytes, FLAGS_cdc_snapshot_max_bytes_per_response)
        : max_bytes;

    // Derive the snapshot wait deadline from the RPC's own client deadline,
    // capped by --cdc_snapshot_wait_timeout_ms so a caller with a very long (or
    // absent) deadline cannot block a service thread indefinitely.
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

    // A page for which this server holds no snapshot session needs the
    // master's durable snapshot cursor (see ReadSnapshot()). Read it before
    // taking a scan slot, so that a slow or unreachable master does not hold
    // one.
    const bool is_start = req->is_snapshot_start();
    const bool new_generation = is_start && req->new_snapshot_generation();
    SnapshotCursorFetch cursor_fetch;
    if (!new_generation) {
      bool has_active_session = false;
      {
        std::lock_guard lock(lock_);
        const auto it = stream_tablet_state_.find(CDCStreamTabletKey{stream_id, tablet_id});
        has_active_session = it != stream_tablet_state_.end() && it->second->snapshot.active;
      }
      if (!has_active_session) {
        cursor_fetch.fetched = true;
        cursor_fetch.status = FetchSnapshotCursor(
            stream_id, tablet_id, snapshot_deadline, &cursor_fetch.found,
            &cursor_fetch.cursor, &cursor_fetch.streaming_start_op_index);
      }
    }

    // Snapshot scans read the tablet's rowsets, so they are heavy: bound their
    // concurrency and heap against user traffic, shedding as SERVER_TOO_BUSY.
    Status acq = TryAcquireScanSlot(snap_max_bytes);
    if (!acq.ok()) {
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, acq, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    auto release_slot = MakeScopedCleanup([&]() { ReleaseScanSlot(snap_max_bytes); });

    const bool is_full_mode = config.record_type() == CDCStreamConfigPB::FULL;
    TRACE("CDC: reading snapshot page of stream $0", stream_id);
    Status ss = ReadSnapshot(stream_id, tablet_id, is_full_mode, is_start, new_generation,
                             req->has_snapshot_resume_key() ? &req->snapshot_resume_key()
                                                            : nullptr,
                             cursor_fetch, snap_max_bytes, snapshot_deadline, resp);
    TRACE("CDC: snapshot page done: $0 records", resp->records_size());
    if (!ss.ok()) {
      if (!resp->has_error()) {
        SetCDCError(ReadErrorCode(ss), ss, resp->mutable_error());
      }
      context->RespondSuccess();
      return;
    }
    // True-up the snapshot scan-slot reservation to the
    // actual serialized response size.  snap_max_bytes is updated so the
    // release_slot scoped cleanup releases exactly what remains charged:
    //   actual < reserved: Release excess -- concurrent streams see real slack.
    //   actual > reserved: Consume delta -- honest accounting; not a gate.
    // The cleanup fires via [&] and calls ReleaseScanSlot(snap_max_bytes)
    // which in turn calls scan_mem_tracker_->Release(snap_max_bytes), so
    // updating snap_max_bytes here is the only change needed for net-zero.
    {
      const int64_t actual_bytes = static_cast<int64_t>(resp->ByteSizeLong());
      if (actual_bytes < snap_max_bytes) {
        scan_mem_tracker_->Release(snap_max_bytes - actual_bytes);
      } else if (actual_bytes > snap_max_bytes) {
        scan_mem_tracker_->Consume(actual_bytes - snap_max_bytes);
      }
      snap_max_bytes = actual_bytes;
    }
    records_produced_->IncrementBy(resp->records_size());
    // Count wire bytes returned to this consumer.
    bytes_sent_->IncrementBy(static_cast<int64_t>(resp->ByteSizeLong()));
    // Record payload size only for responses that carried records.
    // Excludes empty/heartbeat polls so the histogram reflects the
    // actual payload-size distribution without heartbeat noise.
    if (resp->records_size() > 0) {
      get_changes_response_bytes_->Increment(
          static_cast<int64_t>(resp->ByteSizeLong()));
    }
    RecordActivity(stream_id, tablet_id, context->remote_address().ToString());
    context->RespondSuccess();
    return;
  }

  // INITIAL_ONLY streams bootstrap from a one-time snapshot and must NOT stream
  // the WAL afterward. This request reached the WAL-streaming path (no snapshot
  // flags set), so reject it -- symmetric with the snapshot path rejecting a
  // NEVER stream above. A consumer of an INITIAL_ONLY stream stops after
  // snapshot_done and should never issue a streaming GetChanges.
  if (config.snapshot_mode() == CDCStreamConfigPB::INITIAL_ONLY) {
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
  // the WAL and is left uncapped. Reuses the config fetched above.
  const bool is_full_mode = config.record_type() == CDCStreamConfigPB::FULL;
  bool scan_slot_held = false;
  // Tracks the actual charged amount for the FULL-mode scan
  // slot so the release_slot cleanup releases exactly what remains after the
  // true-up below. Initialized to max_bytes on acquire and updated to the
  // actual response size after ReadChanges returns.
  int64_t full_scan_reserved_bytes = 0;
  auto release_slot = MakeScopedCleanup([&]() {
    if (scan_slot_held) ReleaseScanSlot(full_scan_reserved_bytes);
  });
  if (is_full_mode) {
    Status acq = TryAcquireScanSlot(max_bytes);
    if (!acq.ok()) {
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, acq, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    scan_slot_held = true;
    full_scan_reserved_bytes = max_bytes;
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
  TRACE("CDC: reading changes of stream $0 after op index $1", stream_id, from_op_index);
  Status s = ReadChanges(stream_id, tablet_id, from_op_index, max_bytes,
                         req->need_schema_info(), consumer_schema_version,
                         req->read_only(), is_full_mode, apply_deadline, resp);
  TRACE("CDC: read changes done: $0 records, checkpoint op index $1",
        resp->records_size(), resp->checkpoint_op_index());
  if (!s.ok()) {
    // Translate status to a CDCError if not already set. If ReadChanges already
    // set the error (e.g. TABLET_NOT_RUNNING, WAL_EXPIRED), SetCDCError was
    // called and the counters were incremented there; skip it here to avoid
    // double-counting. If no error is set, set it now (handles the
    // TABLET_NOT_LEADER post-read check and other generic failures, such as a
    // FULL image whose ops were not applied in time, which is transient).
    if (!resp->has_error()) {
      SetCDCError(ReadErrorCode(s), s, resp->mutable_error());
    }
    context->RespondSuccess();
    return;
  }

  // True-up the FULL-mode scan-slot reservation to the
  // actual serialized response size.  full_scan_reserved_bytes is updated so
  // the release_slot scoped cleanup (which calls ReleaseScanSlot with that
  // value) releases exactly what remains charged on scan_mem_tracker_.
  //   actual < reserved: Release excess -- other streams see real slack.
  //   actual > reserved: Consume delta -- honest accounting; not a gate.
  // The CHANGE-mode WAL tracker (wal_read_mem_tracker_) is trued-up inside
  // ReadChanges itself, so no second adjustment is needed here.
  if (scan_slot_held) {
    const int64_t actual_bytes = static_cast<int64_t>(resp->ByteSizeLong());
    if (actual_bytes < full_scan_reserved_bytes) {
      scan_mem_tracker_->Release(full_scan_reserved_bytes - actual_bytes);
    } else if (actual_bytes > full_scan_reserved_bytes) {
      scan_mem_tracker_->Consume(actual_bytes - full_scan_reserved_bytes);
    }
    full_scan_reserved_bytes = actual_bytes;
  }

  records_produced_->IncrementBy(resp->records_size());
  // Count wire bytes returned to this consumer.
  bytes_sent_->IncrementBy(static_cast<int64_t>(resp->ByteSizeLong()));
  // Record payload size only for responses that carried records.
  // Excludes empty/heartbeat polls so the histogram reflects the
  // actual payload-size distribution without heartbeat noise.
  if (resp->records_size() > 0) {
    get_changes_response_bytes_->Increment(
        static_cast<int64_t>(resp->ByteSizeLong()));
  }
  RecordActivity(stream_id, tablet_id, context->remote_address().ToString());
  context->RespondSuccess();
}

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

void CDCServiceImpl::Checkpoint(const CheckpointRequestPB* req,
                                CheckpointResponsePB* resp,
                                rpc::RpcContext* context) {
  // Copies: the request is freed with the call once the response goes out.
  const string stream_id = req->stream_id();
  const string tablet_id = req->tablet_id();
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
  if (enforce_access_control_() &&
      !AuthorizeCDCTableOrRespond(server_->token_verifier(), *req,
                                  replica->tablet_metadata()->table_id(), context)) {
    errors_->Increment();
    return;  // helper already responded at the RPC level
  }

  // Only the leader serves CDC. A checkpoint sent to a former leader would
  // otherwise update a session that no longer exists there; the consumer
  // checkpoints again once it found the new leader.
  {
    const std::shared_ptr<consensus::RaftConsensus> consensus = replica->shared_consensus();
    const RaftPeerPB::Role role = RoleIfRunning(replica.get());
    if (role != RaftPeerPB::LEADER) {
      Status ls = Status::IllegalState(Substitute(
          "tablet $0 is not the leader (role: $1)", tablet_id, RaftPeerPB::Role_Name(role)));
      SetCDCError(CDCErrorPB::TABLET_NOT_LEADER, ls, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    // The checkpoint must be a position in the WAL. Persisted, a negative one
    // would release the retention of the tablet, and one past the end of the
    // WAL would make the consumer skip the ops in between when it resumes. A
    // new leader has every op that a former leader served.
    const std::optional<consensus::OpId> last_received =
        consensus->GetLastOpId(consensus::RECEIVED_OPID);
    if (op_index < 0 || !last_received || op_index > last_received->index()) {
      SetCDCError(CDCErrorPB::UNKNOWN_ERROR,
                  Status::InvalidArgument(Substitute(
                      "checkpoint op index $0 of tablet $1 is not in [0, $2]", op_index,
                      tablet_id, last_received ? last_received->index() : -1)),
                  resp->mutable_error());
      context->RespondSuccess();
      return;
    }
  }

  // Reject a checkpoint for a stream the master authoritatively reports as
  // deleted or unknown, or for a tablet the stream does not cover. As in
  // GetChanges, a call that cannot get the stream config is shed.
  {
    CDCStreamConfigPB config;
    vector<string> stream_table_ids;
    Status cs = GetOrFetchStreamConfig(
        stream_id, SafeClientDeadline(MonoTime::Now(), context->GetClientDeadline()),
        &config, &stream_table_ids);
    if (cs.IsNotFound()) {
      SetCDCError(CDCErrorPB::STREAM_NOT_FOUND, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    if (cs.IsNotSupported()) {
      // The stream needs a newer tablet server: retrying here cannot help.
      SetCDCError(CDCErrorPB::UNKNOWN_ERROR, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    if (!cs.ok()) {
      KLOG_EVERY_N_SECS(WARNING, 60)
          << "CDC: shedding Checkpoint for stream " << stream_id << ": "
          << cs.ToString();
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    cs = CheckTabletInStream(stream_id, *replica.get(), stream_table_ids);
    if (!cs.ok()) {
      SetCDCError(CDCErrorPB::TABLET_NOT_IN_STREAM, cs, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
  }

  if (PREDICT_FALSE(FLAGS_cdc_inject_latency_before_session_update_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_cdc_inject_latency_before_session_update_ms));
  }

  // A checkpoint at or past the streaming start point of a snapshot
  // acknowledges the snapshot's last page.
  CompleteSnapshotIfAcknowledged(stream_id, tablet_id, op_index,
                                 replica->shared_tablet().get());

  // Update the in-memory checkpoint of the session, if there is one: a
  // Checkpoint never creates a session (GetChanges does). Also read the
  // FULL-mode history floor so it can be forwarded to the master along with
  // the checkpoint, and decide whether this checkpoint should be persisted
  // durably to the master now or throttled. Without a session, the checkpoint
  // is just persisted.
  uint64_t history_safe_time_micros = 0;
  int64_t up_to_op_index = -1;
  bool persist = true;
  // If the throttle defers the persist: in how many microseconds it is due.
  int64_t persist_delay_micros = 0;
  {
    const int64_t now_micros = GetCurrentTimeMicros();
    const int64_t persist_interval_micros =
        FLAGS_cdc_checkpoint_persist_interval_ms > 0
            ? FLAGS_cdc_checkpoint_persist_interval_ms * 1000 : 0;
    const std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
    std::lock_guard lock(lock_);
    // The master released the stream because it was deleted: the checkpoint
    // passed the stream check above just before the release.
    if (ContainsKey(released_consumer_streams_, stream_id)) {
      Status ns = Status::NotFound(Substitute("CDC stream $0 has been deleted", stream_id));
      SetCDCError(CDCErrorPB::STREAM_NOT_FOUND, ns, resp->mutable_error());
      context->RespondSuccess();
      return;
    }
    CDCStreamTabletKey key{stream_id, tablet_id};
    auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end()) {
      it->second->checkpoint_op_index.store(op_index, std::memory_order_relaxed);
      // The checkpoint advanced: release the history the consumer no longer
      // needs, and report the floor that the checkpoint does need. If no batch
      // this session served starts at or before the checkpoint (e.g. a new
      // leader that the consumer first read from further ahead), the session
      // does not know that floor: report none, so the master keeps the floor it
      // has for the checkpoint.
      CDCStreamTabletState* state = it->second.get();
      state->history_resume_op_index = op_index;
      PublishHistoryFloorUnlocked(stream_id, state, tablet.get());
      const auto& floors = state->history_floor_by_from_index;
      const bool floor_covers_checkpoint = state->snapshot.active ||
          (!floors.empty() && floors.begin()->first <= op_index);
      if (floor_covers_checkpoint && state->history_floor > 0) {
        history_safe_time_micros =
            HybridClock::GetPhysicalValueMicros(Timestamp(it->second->history_floor));
      }
      // During a snapshot the snapshot cursor tracks progress, not the WAL
      // (see RecordActivity()).
      if (!state->snapshot.active) {
        up_to_op_index =
            it->second->last_known_up_to_op_index.load(std::memory_order_relaxed);
      }
      it->second->last_active_time_micros.store(now_micros,
                                                std::memory_order_relaxed);
      // Store the consumer-applied timestamp when present.
      // Only update when the field is actually set so an old consumer that never
      // sends it does not clobber a previously-reported value with 0.
      if (req->has_last_applied_phystime_micros()) {
        it->second->last_applied_phystime_micros.store(
            req->last_applied_phystime_micros(), std::memory_order_relaxed);
      }
      // Throttle durable persistence to at most once per interval per session.
      // Throttling only lets the master's persisted checkpoint, and so the
      // retention barrier, lag, which is safe (retention stays conservative).
      // A brand-new session (last_checkpoint_persist_micros == 0) always
      // persists. A throttled checkpoint is deferred to the next persist of
      // the session, one interval after the last, not dropped: a consumer
      // does not send an unchanged checkpoint again, so the last checkpoint
      // before a tablet goes idle would otherwise never reach the master,
      // which would then take the caught-up consumer for a stuck one (see
      // --cdc_max_staleness_ms). 'last_checkpoint_persist_micros' is then the
      // time of that next persist, which later throttled checkpoints join.
      int64_t& last_persist_micros = it->second->last_checkpoint_persist_micros;
      if (persist_interval_micros > 0 && last_persist_micros != 0 &&
          now_micros - last_persist_micros < persist_interval_micros) {
        persist = false;
        if (now_micros >= last_persist_micros) {
          last_persist_micros += persist_interval_micros;
        }
        // At most one interval, also if the wall clock went back.
        persist_delay_micros = std::clamp<int64_t>(
            last_persist_micros - now_micros, 0, persist_interval_micros);
      } else {
        last_persist_micros = now_micros;
        // The persist refreshes the activity on the master too.
        it->second->last_active_report_micros = now_micros;
      }
    }
  }

  // A consumer that applied the drop of the tablet, and has every change of
  // it, tells the master so that it may delete the tablet. Not throttled: the
  // consumer stops polling the tablet.
  int64_t dropped_last_op_index;
  const bool dropped_tablet_drained = req->drop_applied() &&
      DroppedTabletHasNoChangesAfter(*replica.get(), op_index, &dropped_last_op_index);
  // Queue the durable persist for the master (best-effort, throttled per
  // session). The consumer's checkpoint latency is not tied to the master.
  auto update = MakeCheckpointUpdate(stream_id, tablet_id, op_index,
                                     history_safe_time_micros,
                                     /*refresh_active_time_only=*/false,
                                     /*snapshot_cursor=*/nullptr,
                                     up_to_op_index);
  if (persist || dropped_tablet_drained) {
    checkpoint_persists_->Increment();
    if (dropped_tablet_drained) {
      update->set_dropped_tablet_drained(true);
    }
    QueueCheckpointUpdate(std::move(update));
  } else {
    // Counted in 'checkpoint_persists_' once it is due.
    DeferCheckpointUpdate(std::move(update),
                          MonoTime::Now() + MonoDelta::FromMicroseconds(persist_delay_micros));
  }
  context->RespondSuccess();

  // Only local copies may be used from here on: 'req' and 'resp' may already
  // have been freed.
  if (PREDICT_FALSE(FLAGS_cdc_inject_latency_after_checkpoint_respond_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_cdc_inject_latency_after_checkpoint_respond_ms));
  }
}

namespace {

// Returns the WAL index of the first write of the oldest transaction that is
// still open at the end of 'replicates' -- i.e. a transaction that has a
// WRITE_OP carrying a txn_id but no matching FINALIZE_COMMIT or ABORT_TXN within
// the window -- and sets 'txn_id' to its id. Returns int64_t max if no
// transaction is left open. This mirrors the txn_first_index bookkeeping that
// the full decode below performs, but reads op types only (no row decoding, no
// before/after-image reconstruction), so it is cheap enough to run on read
// windows that may be discarded and re-read at a larger byte cap.
int64_t OldestOpenTxnFirstIndex(const vector<ReplicateMsg*>& replicates, int64_t* txn_id) {
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
  *txn_id = -1;
  for (const auto& e : txn_first_index) {
    if (e.second < open_min) {
      open_min = e.second;
      *txn_id = e.first;
    }
  }
  return open_min;
}

// Whether this replica knows transaction 'txn_id' as neither committed nor
// aborted. Its FINALIZE_COMMIT or ABORT_TXN is then not applied yet, and most
// likely not written yet either.
bool TxnIsOpen(const tablet::Tablet& tablet, int64_t txn_id) {
  tablet::TxnMetadataPB txn_meta;
  return tablet.metadata()->GetTxnMetadataPB(txn_id, &txn_meta) &&
         !txn_meta.aborted() && !txn_meta.has_commit_timestamp();
}

// The most bytes of records a GetChanges response may carry: what fits in one
// RPC message, less some headroom for the other fields and the RPC framing.
// The consumer's messenger rejects a larger message, and a retry would build
// the same response again.
int64_t MaxResponseRecordBytes() {
  const int64_t rpc_max = FLAGS_rpc_max_message_size;
  return rpc_max - std::min<int64_t>(64 * 1024, rpc_max / 2);
}

// The bytes 'record' adds to the serialized 'records' field of a response.
int64_t RecordWireBytes(const CDCRecordPB& record) {
  const size_t size = record.ByteSizeLong();
  return static_cast<int64_t>(
      google::protobuf::io::CodedOutputStream::VarintSize64(
          static_cast<uint64_t>(GetChangesResponsePB::kRecordsFieldNumber) << 3) +
      google::protobuf::io::CodedOutputStream::VarintSize64(size) + size);
}

// Returns true if 'replicate' can only be streamed once its outcome is known
// from its COMMIT entry: the REPLICATE of a WRITE_OP or an ALTER_SCHEMA_OP
// holds the request, while which rows were applied, or whether the ALTER took
// effect, is recorded only in the COMMIT.
bool NeedsOutcome(const ReplicateMsg& replicate) {
  return replicate.op_type() == consensus::WRITE_OP ||
         replicate.op_type() == consensus::ALTER_SCHEMA_OP;
}

// Returns true if the ALTER_SCHEMA_OP with COMMIT entry 'commit' took effect.
// The tablet refuses an ALTER that does not advance its schema version (e.g. a
// duplicate request) when applying it, and records that in the COMMIT.
bool AlterTookEffect(const CommitMsg& commit) {
  for (const auto& op : commit.result().ops()) {
    if (op.has_failed_status()) {
      return false;
    }
  }
  return true;
}

// Sets 'version' and 'schema' to the schema that the ALTER_SCHEMA_OP 'alter',
// which took effect, replaced ('schema' to nullptr if the tablet no longer has
// it). The leader records that version in the op (see PriorSchemaVersion()).
// An op written by an older leader does not have it, and the version before
// the new one is wrong if the ALTER skipped versions, so the tablet's schema
// history gives the schema the op was applied over, if it has it. The history
// has a wrong one only if the tablet applied the ALTER before it kept the
// history: that schema was established at or after the ALTER, so its version
// is not below the new one.
void GetSchemaReplacedByAlter(const tablet::TabletMetadata& metadata,
                              const ReplicateMsg& alter,
                              uint32_t* version,
                              SchemaPtr* schema) {
  const auto& req = alter.alter_schema_request();
  if (!req.has_prior_schema_version()) {
    SchemaPtr applied_over;
    uint32_t applied_over_version;
    if (metadata.GetSchemaForOpIndex(alter.id().index(), &applied_over,
                                     &applied_over_version) &&
        applied_over_version < req.schema_version()) {
      *version = applied_over_version;
      *schema = std::move(applied_over);
      return;
    }
  }
  *version = PriorSchemaVersion(req);
  *schema = metadata.GetSchemaForVersion(*version);
}

// Returns the end (one past the last position) of the first unit of
// 'replicates' that can advance the checkpoint, if the op at 'pos' is the
// first one that needs its outcome. That is the op itself, unless it is a
// write of a transaction that ends (FINALIZE_COMMIT or ABORT_TXN) within
// 'replicates': nothing of the transaction can be streamed, and the
// checkpoint cannot pass its first write, until the outcomes of all the ops up
// to its end are known.
size_t FirstUnitEnd(const vector<unique_ptr<ReplicateMsg>>& replicates, size_t pos) {
  const ReplicateMsg& first = *replicates[pos];
  if (first.op_type() != consensus::WRITE_OP || !first.write_request().has_txn_id()) {
    return pos + 1;
  }
  const int64_t txn_id = first.write_request().txn_id();
  for (size_t i = pos + 1; i < replicates.size(); ++i) {
    const ReplicateMsg& r = *replicates[i];
    if (r.op_type() == consensus::PARTICIPANT_OP && r.has_participant_request() &&
        r.participant_request().has_op()) {
      const auto& pop = r.participant_request().op();
      if (pop.txn_id() == txn_id &&
          (pop.type() == tserver::ParticipantOpPB::FINALIZE_COMMIT ||
           pop.type() == tserver::ParticipantOpPB::ABORT_TXN)) {
        return i + 1;
      }
    }
  }
  return pos + 1;
}

// Sets 'commits' to the COMMIT entries of 'replicates', element by element:
// nullptr for an op that does not need its outcome, or whose COMMIT is not in
// the WAL. A replica writes an op's COMMIT asynchronously once it has applied
// the op, so if some are missing and 'deadline' has not passed yet, this waits
// (until 'deadline') for the WAL to write the COMMITs already queued and looks
// for the missing ones again.
//
// The COMMITs of the first unit that can advance the checkpoint (see
// FirstUnitEnd()) are looked up even if 'deadline' has passed, so a read
// slower than the call budget still lets the stream advance. That unit is
// bounded by 'replicates', which are already in memory. Sets
// 'deadline_exceeded' to whether, once 'deadline' has passed, the first op
// still without a COMMIT is past that unit: its lookup may then have been cut
// short, so the COMMIT may well be in the WAL already.
Status ReadCommits(Log* log,
                   const LogReader& reader,
                   const vector<unique_ptr<ReplicateMsg>>& replicates,
                   const MonoTime& deadline,
                   vector<unique_ptr<CommitMsg>>* commits,
                   bool* deadline_exceeded) {
  commits->clear();
  commits->resize(replicates.size());
  *deadline_exceeded = false;
  vector<size_t> missing;
  for (size_t i = 0; i < replicates.size(); ++i) {
    if (NeedsOutcome(*replicates[i])) {
      missing.push_back(i);
    }
  }
  if (missing.empty()) {
    return Status::OK();
  }
  const size_t first_pos = missing[0];
  const size_t first_unit_end = FirstUnitEnd(replicates, first_pos);
  // Looks up the COMMITs of the ops at 'positions' (ascending) until
  // 'lookup_deadline', and leaves the positions still without one in
  // 'positions'.
  const auto lookup = [&](const MonoTime& lookup_deadline, vector<size_t>* positions) {
    vector<OpId> op_ids;
    op_ids.reserve(positions->size());
    for (size_t pos : *positions) {
      op_ids.push_back(replicates[pos]->id());
    }
    vector<unique_ptr<CommitMsg>> found;
    RETURN_NOT_OK(reader.ReadCommitsForOps(op_ids, &found, lookup_deadline));
    vector<size_t> still_missing;
    for (size_t i = 0; i < positions->size(); ++i) {
      if (found[i]) {
        (*commits)[(*positions)[i]] = std::move(found[i]);
      } else {
        still_missing.push_back((*positions)[i]);
      }
    }
    positions->swap(still_missing);
    return Status::OK();
  };
  for (int attempt = 0; attempt < 2 && !missing.empty(); ++attempt) {
    if (attempt > 0) {
      if (MonoTime::Now() >= deadline) {
        break;
      }
      // Note: the flush marker makes the WAL append thread sync this group
      // even if it holds only COMMITs (an fsync with --log_force_fsync_all).
      Status s = log->WaitUntilAllFlushed(deadline);
      if (!s.ok()) {
        VLOG(1) << "CDC: not waiting for queued COMMIT entries: " << s.ToString();
        break;
      }
    }
    RETURN_NOT_OK(lookup(deadline, &missing));
  }
  if (missing.empty() || MonoTime::Now() < deadline || missing[0] == first_pos) {
    // Every lookup ran to the end of the log, or the first op's did (whatever
    // the deadline): the first missing COMMIT is not written.
    return Status::OK();
  }
  // ReadCommitsForOps() covers the first op whatever the deadline. If the
  // first unit is a transaction, look up the rest of it without a deadline:
  // otherwise the batch would end inside the transaction, and every call cut
  // the same way would return nothing and leave the checkpoint where it is.
  vector<size_t> unit_missing;
  vector<size_t> rest_missing;
  for (size_t pos : missing) {
    if (pos < first_unit_end) {
      unit_missing.push_back(pos);
    } else {
      rest_missing.push_back(pos);
    }
  }
  if (!unit_missing.empty()) {
    RETURN_NOT_OK(lookup(MonoTime::Max(), &unit_missing));
  }
  // A COMMIT of the first unit that is still missing is not written: the
  // lookup ran to the end of the log.
  *deadline_exceeded = unit_missing.empty() && !rest_missing.empty();
  return Status::OK();
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
                                   bool read_only,
                                   bool is_full_mode,
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

  // Pin the tablet for the whole call: a concurrent Stop() (e.g. DeleteTablet)
  // resets the replica's tablet while the batch below is still being decoded.
  const std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
  if (!tablet) {
    Status s = Status::IllegalState(Substitute("tablet $0 is not running", tablet_id));
    SetCDCError(CDCErrorPB::TABLET_NOT_RUNNING, s, resp->mutable_error());
    return s;
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

  // A client that streams from at or past the streaming start point of a
  // snapshot got the snapshot's last page. A read-only client is not the one
  // that took the snapshot.
  if (!read_only) {
    CompleteSnapshotIfAcknowledged(stream_id, tablet_id, from_op_index, tablet.get());
    // The last page of the snapshot in progress ended on the empty key (of a
    // table whose primary key is a single STRING or BINARY column), and this
    // request is neither a snapshot start nor a continuation: a client that
    // took an empty resume key for none. Streaming the WAL from before the
    // streaming start point would deliver WAL records as if they were the rest
    // of the snapshot, and the snapshot would never complete.
    bool lost_empty_resume_key = false;
    {
      std::lock_guard lock(lock_);
      const auto it = stream_tablet_state_.find(CDCStreamTabletKey{stream_id, tablet_id});
      if (it != stream_tablet_state_.end()) {
        const CDCSnapshotState& snapshot = it->second->snapshot;
        lost_empty_resume_key = snapshot.active && !snapshot.done &&
            snapshot.page_end_key && snapshot.page_end_key->empty() &&
            from_op_index < snapshot.streaming_start_op_index;
      }
    }
    if (PREDICT_FALSE(lost_empty_resume_key)) {
      Status s = Status::InvalidArgument(Substitute(
          "a snapshot of tablet $0 is in progress and its last page returned an "
          "empty snapshot_resume_key: pass it back to continue the snapshot",
          tablet_id));
      SetCDCError(CDCErrorPB::UNKNOWN_ERROR, s, resp->mutable_error());
      return s;
    }
  }

  // The ops after from_op_index were applied with schema version 'base_version'
  // ('base_schema', or nullptr if the tablet no longer has it). Rejects a
  // consumer that declared (via req.schema_version) that it decodes records
  // with an older version: an ALTER has landed that the consumer never saw, so
  // it would silently decode the new columns as hex. SCHEMA_VERSION_MISMATCH
  // (retryable, not a re-snapshot) tells the consumer to re-issue with
  // need_schema_info=true, which prepends a synthetic DDL record with that
  // schema, so a consumer attaching mid-stream (e.g. after an ALTER it never
  // saw) has a base schema to decode the records against. The record reuses
  // from_op_index and does not advance checkpoint_op_index, so re-requesting is
  // idempotent. A consumer declaring a newer version is not rejected: it may
  // have read the schema of an ALTER that this replica has not applied yet.
  const auto check_base_schema = [&](uint32_t base_version, const SchemaPtr& base_schema) {
    if (consumer_schema_version >= 0 && !need_schema_info &&
        consumer_schema_version < static_cast<int64_t>(base_version)) {
      Status s = Status::InvalidArgument(Substitute(
          "consumer schema version $0 is older than schema version $1 of the ops "
          "after op index $2; re-request with need_schema_info=true to refresh the "
          "schema", consumer_schema_version, base_version, from_op_index));
      SetCDCError(CDCErrorPB::SCHEMA_VERSION_MISMATCH, s, resp->mutable_error());
      return s;
    }
    if (need_schema_info) {
      if (!base_schema) {
        Status s = Status::NotFound(Substitute(
            "schema version $0 of the ops after op index $1 of tablet $2 is no "
            "longer known", base_version, from_op_index, tablet_id));
        SetCDCError(CDCErrorPB::SCHEMA_UNAVAILABLE, s, resp->mutable_error());
        return s;
      }
      CDCRecordPB* ddl = resp->add_records();
      ddl->set_op_type(DDL);
      ddl->set_op_index(from_op_index);
      ddl->set_schema_version(base_version);
      ddl->set_new_schema_version(base_version);
      RETURN_NOT_OK(SchemaToPB(*base_schema, ddl->mutable_new_schema()));
    }
    return Status::OK();
  };

  // 3. Determine the upper bound: the last committed op index on this leader
  //    that is also in its WAL.
  int64_t committed_op_index;
  int64_t up_to_op_index;
  {
    std::optional<OpId> last_committed =
        consensus->GetLastOpId(consensus::COMMITTED_OPID);
    if (!last_committed) {
      // No committed ops yet; return empty batch.
      return Status::OK();
    }
    committed_op_index = last_committed->index();
    up_to_op_index = committed_op_index;
    // A leader writes an op to its own WAL while it replicates it, so the
    // followers can commit an op before this replica's WAL has it. Read only
    // as far as the WAL has ops. The log of a running replica starts empty
    // (bootstrap appends the replayed ops to a new log), so nothing is
    // readable before its first append.
    const Log* log = replica->log();
    if (log) {
      up_to_op_index = std::min(up_to_op_index,
                                std::max<int64_t>(log->last_indexed_op_index(), 0));
    }
  }

  if (from_op_index >= up_to_op_index) {
    // Consumer is already caught up; return an empty batch but report the
    // current committed index so the consumer knows it is current (and so a
    // just-completed snapshot can capture a streaming start point).
    int64_t checkpoint_op_index = up_to_op_index;
    if (from_op_index > up_to_op_index && from_op_index <= committed_op_index) {
      // The consumer's position is committed but not in this replica's WAL
      // yet: it stays where it is.
      checkpoint_op_index = from_op_index;
    } else if (from_op_index > up_to_op_index) {
      // A consumer that starts streaming from now asks from past the committed
      // index. It must still get the transactions that commit after it,
      // including the writes they made before: start it before the BEGIN_TXN
      // op of every transaction whose FINALIZE_COMMIT op is not at or below
      // the start. Every op at or below the committed index has a timestamp
      // below 'now', so it is applied once every op below 'now' is.
      const Timestamp now = replica->clock()->Now();
      tablet::MvccSnapshot all_applied;
      Status s = replica->time_manager()->WaitUntilSafe(now, deadline);
      if (s.ok()) {
        s = tablet->mvcc_manager()->WaitForSnapshotWithAllApplied(now, &all_applied, deadline);
      }
      if (PREDICT_FALSE(!s.ok())) {
        SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, s, resp->mutable_error());
        return s;
      }
      LowerStreamingStartBelowTxns(
          replica.get(), tablet->MinBeginOpIndexOfTxnsNotCommittedAt(up_to_op_index),
          &checkpoint_op_index);
    }
    // The consumer continues after 'checkpoint_op_index'; 'from_op_index' may
    // be INT64_MAX (start from now).
    if (need_schema_info || consumer_schema_version >= 0) {
      SchemaPtr schema;
      uint32_t schema_version;
      if (!replica->tablet_metadata()->GetSchemaForOpIndex(checkpoint_op_index + 1, &schema,
                                                           &schema_version)) {
        Status s = Status::NotFound(Substitute(
            "the schema of the ops after op index $0 of tablet $1 is no longer known",
            checkpoint_op_index, tablet_id));
        SetCDCError(CDCErrorPB::SCHEMA_UNAVAILABLE, s, resp->mutable_error());
        return s;
      }
      RETURN_NOT_OK(check_base_schema(schema_version, schema));
    }
    resp->set_checkpoint_op_index(checkpoint_op_index);
    return Status::OK();
  }

  // 4. Track the consumer session, and keep the WAL from from_op_index on for
  // the duration of this read. Between reads, the master's retention barrier,
  // which is at or below the stream's durable checkpoint, retains the WAL.
  {
    const Status ts = TrackSession(stream_id, tablet_id, from_op_index, replica.get());
    if (PREDICT_FALSE(!ts.ok())) {
      SetCDCError(TrackSessionErrorCode(ts), ts, resp->mutable_error());
      return ts;
    }
  }
  log::MinLogIndexAnchorer read_anchor(replica->log_anchor_registry().get(),
                                       Substitute("CDC-read[stream=$0]", stream_id));
  RETURN_NOT_OK(read_anchor.AnchorIfMinimum(from_op_index));

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

  // A transaction is only visible to consumers at its FINALIZE_COMMIT, so
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
  // not a wedge, and is left to resolve on a later call. Neither does a read
  // escalate while this replica knows the transaction as open: its commit is
  // not in the WAL yet, so a larger window cannot reach it, and every call
  // would re-read the transaction's whole span for nothing.
  const int64_t span_cap = FLAGS_cdc_max_transaction_span_bytes;

  // For CHANGE-mode reads, reserve max_bytes against the
  // WAL-read tracker before issuing the first ReadReplicatesInRange call.
  // FULL-mode reads are already covered by TryAcquireScanSlot/scan_mem_tracker_,
  // so routing them here would double-count and violate the CHANGE-only intent.
  // When --cdc_wal_read_mem_limit_bytes is 0 (default), the check is skipped
  // entirely and no memory budget applies.
  int64_t wal_read_reserved_bytes = 0;
  if (!is_full_mode) {
    const int64_t wal_mem_limit = FLAGS_cdc_wal_read_mem_limit_bytes;
    if (wal_mem_limit > 0) {
      if (wal_read_mem_tracker_->consumption() + max_bytes > wal_mem_limit) {
        if (wal_reads_rejected_memory_) wal_reads_rejected_memory_->Increment();
        Status oom = Status::ServiceUnavailable(Substitute(
            "CDC WAL read memory budget exhausted (limit $0 bytes); retry",
            wal_mem_limit));
        SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, oom, resp->mutable_error());
        return oom;
      }
      wal_read_mem_tracker_->Consume(max_bytes);
      wal_read_reserved_bytes = max_bytes;
    }
  }
  auto release_wal_mem = MakeScopedCleanup([&]() {
    if (wal_read_reserved_bytes > 0) {
      wal_read_mem_tracker_->Release(wal_read_reserved_bytes);
    }
  });

  // An escalated read window is charged to the same budget as the first one:
  // the WAL-read budget in CHANGE mode, if it is enabled, and the scan budget in
  // FULL mode, whose first reservation GetChanges holds. A transaction whose
  // span needs a larger window than the whole budget can never be read, so the
  // budget also bounds the escalation.
  MemTracker* const escalation_tracker =
      is_full_mode ? scan_mem_tracker_.get() : wal_read_mem_tracker_.get();
  const int64_t escalation_mem_limit =
      is_full_mode ? FLAGS_cdc_scan_mem_limit_bytes : FLAGS_cdc_wal_read_mem_limit_bytes;
  const bool charge_escalation = is_full_mode || wal_read_reserved_bytes > 0;
  int64_t escalation_reserved_bytes = 0;
  auto release_escalation_mem = MakeScopedCleanup([&]() {
    if (escalation_reserved_bytes > 0) {
      escalation_tracker->Release(escalation_reserved_bytes);
    }
  });
  const int64_t escalation_cap = charge_escalation && escalation_mem_limit > 0
      ? std::min(span_cap, escalation_mem_limit)
      : span_cap;

  // Pass the call deadline into the WAL reads (ReadReplicatesInRange and the
  // COMMIT lookup) so their per-entry deadline checks can return a partial
  // batch on expiry; the REPLICATE read always returns at least the first op,
  // and the COMMIT lookup covers the first unit that can advance the checkpoint
  // (see ReadCommits()), so a slow disk still lets the stream advance. The
  // inject flag overrides the real deadline with an already-expired one so
  // tests can verify partial-batch + have_more_records without requiring a
  // genuinely slow disk.
  const MonoTime wal_read_deadline =
      PREDICT_FALSE(FLAGS_cdc_inject_wal_read_deadline_expired)
      ? MonoTime::Now() - MonoDelta::FromMilliseconds(1)
      : deadline;

  vector<unique_ptr<ReplicateMsg>> replicates;
  int64_t effective_max_bytes = max_bytes;
  // Set if the read window ends inside the span of an open transaction that
  // holds the checkpoint at from_op_index: nothing can be streamed until the
  // transaction ends.
  bool blocked_by_open_txn = false;
  while (true) {
    // Check the deadline before each WAL read attempt,
    // including escalated ones (8MB->16MB->...->512MB). Each escalated read
    // can take many seconds on a cold/slow disk; bailing out here rather than
    // monopolizing the RPC worker past the client deadline. The first iteration
    // passes through (deadline has not expired yet in the normal case); the
    // check fires only when a prior escalated read consumed nearly all budget.
    // MonoTime::Max() (the "no deadline" sentinel) is never <= Now(), so this
    // is a no-op when no deadline was set.
    if (PREDICT_FALSE(FLAGS_cdc_inject_escalation_deadline_expired) ||
        (deadline.Initialized() && MonoTime::Now() >= deadline)) {
      Status s = Status::TimedOut(Substitute(
          "CDC GetChanges deadline exceeded on tablet $0 (effective cap $1 bytes); "
          "retry",
          tablet_id, effective_max_bytes));
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, s, resp->mutable_error());
      return s;
    }

    vector<ReplicateMsg*> batch;
    Status s = reader->ReadReplicatesInRange(
        from_op_index + 1,  // exclusive lower bound -> inclusive starting_at
        up_to_op_index,
        effective_max_bytes,
        &batch,
        wal_read_deadline);
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

    TRACE("CDC: read $0 WAL ops after op index $1 (cap $2 bytes)",
          batch.size(), from_op_index, effective_max_bytes);
    const bool truncated_by_cap =
        !batch.empty() && batch.back()->id().index() < up_to_op_index;
    int64_t open_txn_id = -1;
    const int64_t open_first = OldestOpenTxnFirstIndex(batch, &open_txn_id);
    // No checkpoint progress is possible when the oldest open transaction starts
    // at the first op in the window (checkpoint would be open_first - 1 ==
    // from_op_index).
    const bool wedged = open_first != std::numeric_limits<int64_t>::max() &&
                        truncated_by_cap &&
                        open_first == from_op_index + 1;
    if (wedged && TxnIsOpen(*tablet, open_txn_id)) {
      blocked_by_open_txn = true;
    } else if (wedged && effective_max_bytes < escalation_cap) {
      // Grow the window (doubling, clamped to the cap) and retry so the
      // transaction's commit can be reached.
      const int64_t next_max_bytes = std::min(escalation_cap, effective_max_bytes * 2);
      if (charge_escalation) {
        const int64_t delta = next_max_bytes - effective_max_bytes;
        if (escalation_mem_limit > 0 &&
            escalation_tracker->consumption() + delta > escalation_mem_limit) {
          for (ReplicateMsg* r : batch) delete r;
          const auto& rejected = is_full_mode ? scans_rejected_memory_
                                              : wal_reads_rejected_memory_;
          if (rejected) rejected->Increment();
          Status busy = Status::ServiceUnavailable(Substitute(
              "CDC $0 memory budget exhausted (limit $1 bytes) while reading the "
              "transaction starting at op index $2 of tablet $3; retry",
              is_full_mode ? "scan" : "WAL read", escalation_mem_limit, open_first,
              tablet_id));
          SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, busy, resp->mutable_error());
          return busy;
        }
        escalation_tracker->Consume(delta);
        escalation_reserved_bytes += delta;
      }
      for (ReplicateMsg* r : batch) delete r;
      effective_max_bytes = next_max_bytes;
      if (wal_read_escalations_) wal_read_escalations_->Increment();
      VLOG(1) << Substitute("T $0: CDC read window escalated to $1 bytes to reach "
                            "the commit of the transaction starting at op index $2",
                            tablet_id, effective_max_bytes, open_first);
      continue;
    } else if (wedged) {
      for (ReplicateMsg* r : batch) delete r;
      Status too_large = Status::IllegalState(Substitute(
          "the transaction starting at op index $0 on tablet $1 spans more than "
          "$2 bytes of WAL, the most CDC reads to emit a transaction "
          "(--cdc_max_transaction_span_bytes$3); the CDC checkpoint cannot advance "
          "past it. Raise the limit to emit it.",
          open_first, tablet_id, escalation_cap,
          escalation_cap < span_cap
              ? (is_full_mode ? ", bounded by --cdc_scan_mem_limit_bytes"
                              : ", bounded by --cdc_wal_read_mem_limit_bytes")
              : ""));
      KLOG_EVERY_N_SECS(WARNING, 60) << "CDC: " << too_large.ToString();
      SetCDCError(CDCErrorPB::TRANSACTION_TOO_LARGE, too_large, resp->mutable_error());
      return too_large;
    }
    replicates.reserve(batch.size());
    for (ReplicateMsg* r : batch) {
      replicates.emplace_back(r);
    }
    break;
  }

  // 5b. Pair the ops with their outcomes. The newest committed ops may not be
  //     applied yet, so their outcome is unknown: end the batch before the
  //     first of them (it is streamed on a later call), or fail with a
  //     retryable error if that is the first op of the batch.
  //
  //     'batch_up_to_op_index' is the index up to which this call can stream.
  //     It stays at the committed watermark if the COMMIT lookup was cut short
  //     by the deadline: the next call can stream more right away, so the
  //     response reports more records. Otherwise it ends before the first op
  //     whose outcome is unknown, and the consumer polls again normally.
  //     'up_to_op_index' remains the readable watermark (see step 3) either
  //     way, as the stream's lag is measured against it.
  vector<unique_ptr<CommitMsg>> commits;
  bool commit_lookup_cut = false;
  int64_t batch_up_to_op_index = up_to_op_index;
  {
    const MonoTime commit_lookup_deadline =
        PREDICT_FALSE(FLAGS_cdc_inject_commit_lookup_deadline_expired)
        ? MonoTime::Now() - MonoDelta::FromMilliseconds(1)
        : wal_read_deadline;
    Status s = ReadCommits(log, *reader, replicates, commit_lookup_deadline, &commits,
                           &commit_lookup_cut);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        const CDCErrorPB::Code code = StreamIdleExpired(stream_id, tablet_id)
            ? CDCErrorPB::STREAM_EXPIRED : CDCErrorPB::WAL_EXPIRED;
        SetCDCError(code, s, resp->mutable_error());
      }
      return s;
    }
  }
  for (size_t i = 0; i < replicates.size(); ++i) {
    if (!NeedsOutcome(*replicates[i]) || commits[i]) {
      continue;
    }
    const int64_t unknown_index = replicates[i]->id().index();
    if (i == 0) {
      Status s = Status::ServiceUnavailable(Substitute(
          "the outcome of op index $0 of tablet $1 is not known yet: the op is "
          "committed but not applied, or its COMMIT is not written yet; retry",
          unknown_index, tablet_id));
      if (wal_reads_rejected_outcome_pending_) {
        wal_reads_rejected_outcome_pending_->Increment();
      }
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, s, resp->mutable_error());
      return s;
    }
    TRACE("CDC: batch ends before op index $0, whose outcome is not known yet$1",
          unknown_index, commit_lookup_cut ? " (COMMIT lookup cut by the deadline)" : "");
    replicates.resize(i);
    commits.resize(i);
    if (!commit_lookup_cut) {
      batch_up_to_op_index = unknown_index - 1;
    }
    break;
  }

  if (PREDICT_FALSE(FLAGS_cdc_inject_latency_before_decode_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_cdc_inject_latency_before_decode_ms));
  }

  // Progress signal: if the read stopped before batch_up_to_op_index, the byte
  // budget or the deadline cut it short and more records are immediately
  // available, so the consumer should keep polling now rather than backing off.
  // Reaching batch_up_to_op_index (or an empty batch) means nothing more can be
  // streamed right now. Records deferred
  // by an open transaction are deliberately NOT reported as "more" here: those
  // will not appear on an immediate re-poll until the transaction commits, so
  // treating them as back-pressure (poll normally) is correct.
  const bool have_more_records = !blocked_by_open_txn && !replicates.empty() &&
                                 replicates.back()->id().index() < batch_up_to_op_index;
  resp->set_have_more_records(have_more_records);

  // For FULL streams, record the oldest op timestamp in this batch so the
  // session keeps the tablet's MVCC history from there on, and before-images
  // remain reconstructable when the consumer resumes from its checkpoint. The
  // session's floor is forwarded to the master (and thus to followers) on the
  // next Checkpoint. This only sets the floor held by this stream: other
  // streams and the master hold their own (see PublishHistoryFloorUnlocked()).
  // A read-only client is not the consumer that resumes from the checkpoint:
  // its reads neither pin history nor lower the session's resume point.
  if (is_full_mode && !read_only && !replicates.empty()) {
    uint64_t batch_min_ts = std::numeric_limits<uint64_t>::max();
    for (const auto& r : replicates) {
      if (r->has_timestamp()) {
        batch_min_ts = std::min(batch_min_ts, static_cast<uint64_t>(r->timestamp()));
      }
    }
    if (batch_min_ts != std::numeric_limits<uint64_t>::max()) {
      // Enough to cover any realistic read-ahead window; see
      // CDCStreamTabletState::history_floor_by_from_index.
      constexpr size_t kMaxHistoryFloorEntries = 64;
      std::lock_guard lock(lock_);
      CDCStreamTabletKey key{stream_id, tablet_id};
      auto it = stream_tablet_state_.find(key);
      if (it != stream_tablet_state_.end()) {
        CDCStreamTabletState* state = it->second.get();
        const int64_t resume_op_index = state->history_resume_op_index >= 0
            ? state->history_resume_op_index
            : state->checkpoint_op_index.load(std::memory_order_relaxed);
        state->history_resume_op_index = std::min(resume_op_index, from_op_index);
        auto& floors = state->history_floor_by_from_index;
        auto emplaced = floors.emplace(from_op_index, batch_min_ts);
        if (!emplaced.second) {
          emplaced.first->second = std::min(emplaced.first->second, batch_min_ts);
        }
        while (floors.size() > kMaxHistoryFloorEntries) {
          floors.erase(std::prev(floors.end()));
        }
        PublishHistoryFloorUnlocked(stream_id, state, tablet.get());
      }
    }
  }

  // 6. Determine the schema of the batch's first ops, those before its first
  //    ALTER (that took effect); the decode loop below moves to the schema
  //    that each ALTER establishes. That is the schema the first ALTER
  //    replaced (see GetSchemaReplacedByAlter()). Without an ALTER, the
  //    tablet's schema history gives the schema of the batch's first op, which
  //    is exact once every ALTER before that op is applied. This holds however
  //    far apply lags the committed index: an ALTER holds the tablet's schema
  //    lock exclusively from its Prepare through its Apply, and a write takes
  //    it shared in its Prepare (ops are prepared in order) before it is
  //    decoded. So every ALTER before the batch's first write was applied
  //    before that write was prepared, and that write's outcome is known (see
  //    5b).
  size_t first_alter = replicates.size();
  for (size_t i = 0; i < replicates.size(); ++i) {
    if (replicates[i]->op_type() == consensus::ALTER_SCHEMA_OP &&
        replicates[i]->has_alter_schema_request() && AlterTookEffect(*commits[i])) {
      first_alter = i;
      break;
    }
  }
  bool writes_before_alter = false;
  for (size_t i = 0; i < first_alter; ++i) {
    writes_before_alter |= replicates[i]->op_type() == consensus::WRITE_OP;
  }
  uint32_t base_version = 0;
  SchemaPtr base_schema;
  bool base_known = true;
  if (first_alter < replicates.size()) {
    GetSchemaReplacedByAlter(*replica->tablet_metadata().get(), *replicates[first_alter],
                             &base_version, &base_schema);
  } else {
    base_known = replica->tablet_metadata()->GetSchemaForOpIndex(
        from_op_index + 1, &base_schema, &base_version);
  }
  const bool needs_base_version =
      writes_before_alter || need_schema_info || consumer_schema_version >= 0;
  if ((!base_known && needs_base_version) || (writes_before_alter && !base_schema)) {
    Status s = Status::NotFound(Substitute(
        "the schema of the ops after op index $0 of tablet $1 is no longer known",
        from_op_index, tablet_id));
    SetCDCError(CDCErrorPB::SCHEMA_UNAVAILABLE, s, resp->mutable_error());
    return s;
  }
  if (base_known) {
    RETURN_NOT_OK(check_base_schema(base_version, base_schema));
  }
  int64_t running_schema_version = base_version;
  SchemaPtr running_schema = base_schema;

  // 7. Decode ReplicateMsgs. Transactional writes (those carrying a txn_id) are
  //    buffered per transaction and only emitted when the transaction's
  //    FINALIZE_COMMIT is seen (dropped on ABORT_TXN), so uncommitted or aborted
  //    data is never published. Each emit group carries an "effective index":
  //    the op index at which it becomes visible (the commit index for a
  //    transaction, otherwise the op's own index), and a "min index": the
  //    lowest op index its records come from (the first write of a
  //    transaction, otherwise the op's own index).
  struct EmitGroup {
    int64_t effective_index;
    int64_t min_index;
    bool is_txn;
    vector<CDCRecordPB> records;
    int64_t num_bytes;  // serialized size of 'records' in a response
  };
  vector<EmitGroup> emit_groups;
  // The records and bytes the response would carry with every group so far.
  int64_t base_bytes = 0;
  for (const auto& r : resp->records()) {
    base_bytes += RecordWireBytes(r);
  }
  int64_t decoded_records = resp->records_size();
  int64_t decoded_bytes = base_bytes;
  const auto add_group = [&](int64_t effective_index, int64_t min_index, bool is_txn,
                             vector<CDCRecordPB> records) {
    int64_t num_bytes = 0;
    for (const auto& r : records) {
      num_bytes += RecordWireBytes(r);
    }
    decoded_records += static_cast<int64_t>(records.size());
    decoded_bytes += num_bytes;
    emit_groups.push_back({effective_index, min_index, is_txn, std::move(records), num_bytes});
  };
  std::map<int64_t, vector<CDCRecordPB>> txn_buffers;   // txn_id -> buffered rows
  std::map<int64_t, int64_t> txn_first_index;           // txn_id -> first write index

  // FULL mode: transactional writes become visible at their commit timestamp, so
  // their before/after images must be read at commit time. Retain each write op
  // (with its rows decoded at write time for a stable schema_version) until the
  // FINALIZE_COMMIT is seen.
  struct FullTxnOp {
    unique_ptr<ReplicateMsg> replicate;
    unique_ptr<CommitMsg> commit;
    SchemaPtr schema;  // the schema the op was applied with
    vector<CDCRecordPB> rows;
  };
  std::map<int64_t, vector<FullTxnOp>> txn_full_ops;    // txn_id -> deferred ops

  // An op that cannot be decoded ends the batch right before it, so the
  // checkpoint never moves past an op whose records were not emitted. So does
  // an op whose FULL images cannot be read before the deadline: the ops before
  // it are returned and the consumer re-polls at once.
  //
  // Decoding also stops once the groups decoded so far fill a response and no
  // transaction is pending: the response cannot carry the records of the ops
  // after that, so decoding them (and, in FULL mode, reconstructing their
  // images) would only be repeated by the next call. The checkpoint then ends
  // at the last op decoded, and the consumer re-polls at once.
  Status decode_failure;
  Status images_cut;
  int64_t full_image_rows_read = 0;
  int64_t last_index = from_op_index;
  bool decode_stopped_at_limit = false;
  for (size_t i = 0; i < replicates.size(); ++i) {
    if (!emit_groups.empty() && txn_first_index.empty() &&
        ((FLAGS_cdc_max_records_per_response > 0 &&
          decoded_records >= FLAGS_cdc_max_records_per_response) ||
         decoded_bytes >= max_bytes)) {
      decode_stopped_at_limit = true;
      break;
    }
    auto& owned = replicates[i];
    const ReplicateMsg* replicate = owned.get();
    const CommitMsg* commit_msg = commits[i].get();
    const int64_t op_index = replicate->id().index();
    const int64_t prev_index = last_index;
    last_index = op_index;

    if (replicate->op_type() == consensus::WRITE_OP) {
      vector<CDCRecordPB> row_records;
      if (write_ops_decoded_) write_ops_decoded_->Increment();
      DCHECK(running_schema);
      Status s = PREDICT_FALSE(op_index == FLAGS_cdc_inject_decode_failure_at_op_index)
          ? Status::Corruption("injected CDC decode failure")
          : DecodeWriteOpAllRows(*replicate,
                                 commit_msg->result(),
                                 static_cast<int32_t>(running_schema_version),
                                 *running_schema,
                                 &row_records);
      if (s.IsAborted()) {
        continue;  // no row operations
      }
      if (PREDICT_FALSE(!s.ok())) {
        decode_failure = s.CloneAndPrepend(Substitute(
            "failed to decode WRITE_OP at op index $0 of tablet $1", op_index, tablet_id));
        last_index = prev_index;
        break;
      }
      const bool is_txn = replicate->write_request().has_txn_id();
      if (is_txn) {
        const int64_t txn_id = replicate->write_request().txn_id();
        if (txn_first_index.find(txn_id) == txn_first_index.end()) {
          txn_first_index[txn_id] = op_index;
        }
        if (is_full_mode) {
          // Defer image reconstruction to commit time; keep rows + the replicate.
          txn_full_ops[txn_id].push_back(FullTxnOp{std::move(owned), std::move(commits[i]),
                                                   running_schema, std::move(row_records)});
        } else {
          auto& buf = txn_buffers[txn_id];
          for (auto& r : row_records) {
            buf.emplace_back(std::move(r));
          }
        }
      } else {
        if (is_full_mode) {
          Status rs = ReconstructBeforeAfterImages(
              tablet.get(), replica->time_manager(), *replicate, commit_msg->result(),
              *running_schema, static_cast<uint64_t>(replicate->timestamp()), deadline,
              &row_records, &full_image_rows_read);
          if (!rs.ok()) {
            if (rs.IsIncomplete()) {
              SetCDCError(CDCErrorPB::HISTORY_EXPIRED, rs, resp->mutable_error());
              return rs;
            }
            if (!replica->CheckRunning().ok()) {
              // The replica was stopped while the batch was being decoded.
              SetCDCError(CDCErrorPB::TABLET_NOT_RUNNING, rs, resp->mutable_error());
              return rs;
            }
            if (rs.IsTimedOut() || rs.IsServiceUnavailable()) {
              // Committed ops were not applied in time (most likely right after
              // a leader change), the reads ran out of time, or the schema
              // changed during them: end the batch before this op.
              images_cut = rs;
              last_index = prev_index;
              break;
            }
            // Any other failure means we cannot produce a correct before/after
            // image, e.g. an IOError or Corruption during the historical read.
            // Abort the batch rather than emit a truncated or possibly-wrong
            // record with no old_changes. GetChanges reports an IOError as
            // retryable, so the consumer polls again from its last checkpoint,
            // and other errors as fatal (see ReadErrorCode()).
            KLOG_EVERY_N_SECS(WARNING, 60)
                << "CDC: FULL image reconstruction failed at op_index="
                << op_index << " for tablet " << tablet_id << ": " << rs.ToString();
            return rs;
          }
        }
        add_group(op_index, op_index, /*is_txn=*/false, std::move(row_records));
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
          // transaction's writes become visible only at commit. A transaction
          // may only insert (see WriteOp::Prepare()), so this reads nothing.
          // Ops that read would need more care: every op of the transaction
          // has the same timestamp, so the rows read at it hold the effects
          // of all of them, not those of the ops before.
          auto fit = txn_full_ops.find(txn_id);
          if (fit != txn_full_ops.end()) {
            for (auto& fop : fit->second) {
              Status rs = ReconstructBeforeAfterImages(
                  tablet.get(), replica->time_manager(), *fop.replicate,
                  fop.commit->result(), *fop.schema, static_cast<uint64_t>(commit_ts),
                  deadline, &fop.rows, &full_image_rows_read);
              if (!rs.ok()) {
                if (rs.IsIncomplete()) {
                  SetCDCError(CDCErrorPB::HISTORY_EXPIRED, rs, resp->mutable_error());
                  return rs;
                }
                if (!replica->CheckRunning().ok()) {
                  // The replica was stopped while the batch was being decoded.
                  SetCDCError(CDCErrorPB::TABLET_NOT_RUNNING, rs, resp->mutable_error());
                  return rs;
                }
                if (rs.IsTimedOut() || rs.IsServiceUnavailable()) {
                  images_cut = rs;  // see the non-transactional case above
                  break;
                }
                // Any other failure means we cannot produce a correct
                // before/after image: see the non-transactional case above.
                KLOG_EVERY_N_SECS(WARNING, 60)
                    << "CDC: FULL image reconstruction failed for txn " << txn_id
                    << " at commit op_index=" << op_index << " for tablet "
                    << tablet_id << ": " << rs.ToString();
                return rs;
              }
              for (auto& r : fop.rows) {
                r.set_commit_timestamp(commit_ts);
                group.emplace_back(std::move(r));
              }
            }
            if (!images_cut.ok()) {
              // The transaction stays open, so the checkpoint stays before it.
              last_index = prev_index;
              break;
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
        const int64_t min_index = FindWithDefault(txn_first_index, txn_id, op_index);
        txn_first_index.erase(txn_id);

        CDCRecordPB commit;
        commit.set_op_type(CDCOpTypePB::COMMIT);
        commit.set_op_index(op_index);
        commit.set_op_term(replicate->id().term());
        commit.set_commit_timestamp(commit_ts);
        commit.set_txn_id(std::to_string(txn_id));
        group.emplace_back(std::move(commit));

        add_group(op_index, min_index, /*is_txn=*/true, std::move(group));
      } else if (pop.type() == tserver::ParticipantOpPB::ABORT_TXN) {
        // Drop any buffered rows for the aborted transaction.
        txn_buffers.erase(txn_id);
        txn_full_ops.erase(txn_id);
        txn_first_index.erase(txn_id);
      }
      // BEGIN_TXN / BEGIN_COMMIT / GET_METADATA produce no CDC output.
    } else if (replicate->op_type() == consensus::ALTER_SCHEMA_OP) {
      if (!AlterTookEffect(*commit_msg)) {
        VLOG(1) << Substitute("T $0: skipping ALTER_SCHEMA_OP at op index $1, which did "
                              "not take effect", tablet_id, op_index);
        continue;
      }
      CDCRecordPB record;
      SchemaPtr new_schema = std::make_shared<Schema>();
      Status s = PREDICT_FALSE(op_index == FLAGS_cdc_inject_decode_failure_at_op_index)
          ? Status::Corruption("injected CDC decode failure")
          : DecodeNonWriteReplicateMsg(*replicate, &record);
      if (s.ok()) {
        s = SchemaFromPB(replicate->alter_schema_request().schema(), new_schema.get());
      }
      if (PREDICT_FALSE(!s.ok())) {
        decode_failure = s.CloneAndPrepend(Substitute(
            "failed to decode ALTER_SCHEMA_OP at op index $0 of tablet $1",
            op_index, tablet_id));
        last_index = prev_index;
        break;
      }
      // The version the ALTER replaced: the one the ops before it in the batch
      // are stamped with (the op itself may not record it, see
      // GetSchemaReplacedByAlter()).
      record.set_schema_version(static_cast<uint32_t>(running_schema_version));
      // Subsequent records are governed by the post-alter schema version. An
      // ALTER that took effect always advances the version.
      DCHECK_GT(record.new_schema_version(), running_schema_version);
      running_schema_version = std::max<int64_t>(running_schema_version,
                                                 record.new_schema_version());
      running_schema = std::move(new_schema);
      vector<CDCRecordPB> g;
      g.emplace_back(std::move(record));
      add_group(op_index, op_index, /*is_txn=*/false, std::move(g));
    }
    // Other op types (NO_OP, etc.) produce no CDC records.
  }
  if (full_image_rows_read > 0 && full_image_rows_read_) {
    full_image_rows_read_->IncrementBy(full_image_rows_read);
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

  // Plan the response. Each emit group is an atomic unit (BEGIN+rows+COMMIT
  // for a transaction, or the records of one op), so a response ends only
  // between groups. It ends at the record cap (--cdc_max_records_per_response)
  // or at max_bytes of records, whichever comes first, but always carries the
  // first group (forward progress) unless that does not fit in one RPC message.
  // The checkpoint stays below the first write of every group left out.
  vector<ChangesEmitGroupInfo> group_infos;
  group_infos.reserve(emit_groups.size());
  for (const auto& group : emit_groups) {
    group_infos.push_back({group.effective_index, group.min_index,
                           static_cast<int64_t>(group.records.size()), group.num_bytes});
  }
  ChangesEmitLimits limits;
  limits.max_records = FLAGS_cdc_max_records_per_response;
  limits.max_bytes = max_bytes;
  limits.hard_max_bytes = MaxResponseRecordBytes();
  // With every group emitted, the checkpoint is the last op read, or the end of
  // the batch if it is empty. After a decode failure, or if the FULL images of
  // an op could not be read in time, it stays before that op.
  const bool cut_short = !decode_failure.ok() || !images_cut.ok() || decode_stopped_at_limit;
  const int64_t end_index =
      !cut_short && last_index <= from_op_index ? batch_up_to_op_index : last_index;
  const ChangesEmitPlan plan = PlanChangesEmission(
      group_infos, open_min, end_index, resp->records_size(), base_bytes, limits);

  // After a decode failure, return what precedes the failed op and have the
  // consumer re-poll at once. If nothing precedes it that would advance the
  // checkpoint, fail loudly rather than return the same empty batch forever.
  // The error is returned ahead of the post-read leadership recheck below: the
  // decode of a committed op is deterministic, so the new leader would fail on
  // it the same way and a retryable TABLET_NOT_LEADER would gain nothing.
  if (PREDICT_FALSE(!decode_failure.ok())) {
    KLOG_EVERY_N_SECS(WARNING, 60) << "CDC: " << decode_failure.ToString();
    if (plan.checkpoint_op_index <= from_op_index) {
      resp->clear_have_more_records();
      SetCDCError(CDCErrorPB::DECODE_FAILED, decode_failure, resp->mutable_error());
      return decode_failure;
    }
    resp->set_have_more_records(true);
  }
  // Likewise if the FULL images of an op could not be read in time, but the
  // error is retryable (SERVER_TOO_BUSY).
  if (PREDICT_FALSE(!images_cut.ok())) {
    if (plan.checkpoint_op_index <= from_op_index) {
      KLOG_EVERY_N_SECS(WARNING, 60)
          << "CDC: FULL image reconstruction cut short on tablet " << tablet_id << ": "
          << images_cut.ToString();
      resp->clear_have_more_records();
      return images_cut;
    }
    resp->set_have_more_records(true);
  }

  // Records that do not fit in one response cannot be sent at all: the
  // consumer's messenger would reject the response, and every retry would build
  // it again. Fail loudly instead. Like a decode failure, this is deterministic,
  // so it is returned ahead of the leadership recheck.
  if (PREDICT_FALSE(plan.too_large)) {
    const EmitGroup& last = emit_groups[plan.num_groups - 1];
    int64_t num_bytes = base_bytes;
    for (size_t i = 0; i < plan.num_groups; ++i) {
      num_bytes += group_infos[i].num_bytes;
    }
    Status too_large = Status::IllegalState(Substitute(
        "the change records of the $0 at op index $1 of tablet $2 take $3 bytes, "
        "more than fit in one GetChanges response ($4 bytes, derived from "
        "--rpc_max_message_size); the CDC checkpoint cannot advance past it. "
        "Raise --rpc_max_message_size on the tablet servers and the consumer to "
        "stream it.",
        last.is_txn ? "transaction committed" : "op", last.effective_index, tablet_id,
        num_bytes, limits.hard_max_bytes));
    KLOG_EVERY_N_SECS(WARNING, 60) << "CDC: " << too_large.ToString();
    resp->clear_records();
    resp->clear_have_more_records();
    SetCDCError(last.is_txn ? CDCErrorPB::TRANSACTION_TOO_LARGE
                            : CDCErrorPB::RESPONSE_TOO_LARGE,
                too_large, resp->mutable_error());
    return too_large;
  }

  for (size_t i = 0; i < plan.num_groups; ++i) {
    for (auto& r : emit_groups[i].records) {
      *resp->add_records() = std::move(r);
    }
  }
  resp->set_checkpoint_op_index(plan.checkpoint_op_index);
  if (plan.truncated || decode_stopped_at_limit) {
    VLOG(1) << Substitute("T $0: CDC response ends after $1 records at op index $2 "
                          "(--cdc_max_records_per_response or max_bytes)",
                          tablet_id, resp->records_size(), plan.checkpoint_op_index);
    resp->set_have_more_records(true);
  }
  // A response that streams nothing and leaves the checkpoint where it is must
  // not report more records: the consumer would re-poll at once and get the
  // same response again. It polls at its normal pace instead.
  if (resp->records_size() == 0 && plan.checkpoint_op_index <= from_op_index &&
      resp->have_more_records()) {
    TRACE("CDC: no progress possible after op index $0; not reporting more records",
          from_op_index);
    resp->set_have_more_records(false);
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
      it->second->caught_up.store(
          !resp->have_more_records() && resp->checkpoint_op_index() >= up_to_op_index,
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
    // This read may have created the session after the leadership change ended
    // the sessions on this replica.
    EndSessionsIfNotLeader(tablet_id);
    return Status::IllegalState(Substitute(
        "tablet $0 leadership changed during the read (term $1 -> $2); retry "
        "against the current leader",
        tablet_id, leader_term, consensus->CurrentTerm()));
  }

  // True-up the WAL-read MemTracker reservation to the
  // actual serialized response size, correcting the pre-read proxy estimate.
  // Only fires when a WAL-read reservation was made (CHANGE-mode + limit set).
  // Direction:
  //   actual < reserved: Release the excess so concurrent streams see the
  //     real slack and are not unnecessarily shed.
  //   actual > reserved (e.g. FULL-mode >5x before-image expansion): Consume
  //     the delta for accurate accounting; this is NOT a gate -- the call
  //     already succeeded. wal_read_reserved_bytes is updated so the
  //     release_wal_mem scoped cleanup releases exactly actual_bytes.
  // Note: this true-up addresses over-reservation and per-call accounting
  // accuracy. It does NOT eliminate the transient decoded-heap peak (which
  // can exceed raw WAL bytes by >5x in FULL mode).
  if (wal_read_reserved_bytes > 0) {
    const int64_t actual_bytes = static_cast<int64_t>(resp->ByteSizeLong());
    if (actual_bytes < wal_read_reserved_bytes) {
      wal_read_mem_tracker_->Release(wal_read_reserved_bytes - actual_bytes);
    } else if (actual_bytes > wal_read_reserved_bytes) {
      wal_read_mem_tracker_->Consume(actual_bytes - wal_read_reserved_bytes);
    }
    wal_read_reserved_bytes = actual_bytes;
  }

  return Status::OK();
}

std::shared_ptr<std::timed_mutex> CDCServiceImpl::GetSnapshotPageLock(
    const CDCStreamTabletKey& key) {
  std::lock_guard lock(lock_);
  auto& slot = snapshot_page_locks_[key];
  if (!slot) {
    slot = std::make_shared<std::timed_mutex>();
  }
  return slot;
}

std::shared_ptr<std::timed_mutex> CDCServiceImpl::GetStreamConfigFetchLock(
    const string& stream_id) {
  std::lock_guard lock(lock_);
  auto& slot = stream_config_fetch_locks_[stream_id];
  if (!slot) {
    slot = std::make_shared<std::timed_mutex>();
  }
  return slot;
}

// ---------------------------------------------------------------------------
// ReadSnapshot (internal) - server-driven consistent snapshot
// ---------------------------------------------------------------------------

Status CDCServiceImpl::ReadSnapshot(const string& stream_id,
                                    const string& tablet_id,
                                    bool is_full_mode,
                                    bool is_start,
                                    bool new_generation,
                                    const string* req_resume_key,
                                    const SnapshotCursorFetch& cursor_fetch,
                                    int64_t max_bytes,
                                    const MonoTime& deadline,
                                    GetChangesResponsePB* resp) {
  if (PREDICT_FALSE(!is_start && !req_resume_key)) {
    return Status::InvalidArgument("a snapshot page needs is_snapshot_start or a resume key");
  }
  // 1. Locate the replica and verify leadership, and remember the leader term
  //    so that the page can be re-validated after the scan (see the post-scan
  //    recheck below).
  scoped_refptr<TabletReplica> replica;
  RETURN_NOT_OK_PREPEND(
      server_->tablet_manager()->GetTabletReplica(tablet_id, &replica),
      Substitute("tablet $0 not found on this server", tablet_id));
  if (RoleIfRunning(replica.get()) != RaftPeerPB::LEADER) {
    return Status::IllegalState(
        Substitute("tablet $0 is not running or not the leader", tablet_id));
  }
  std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
  if (!tablet) {
    return Status::IllegalState(Substitute("tablet $0 not available", tablet_id));
  }
  std::shared_ptr<consensus::RaftConsensus> consensus = replica->shared_consensus();
  if (!consensus) {
    return Status::IllegalState(
        Substitute("tablet $0 has no consensus instance", tablet_id));
  }
  const int64_t leader_term = consensus->CurrentTerm();

  const CDCStreamTabletKey key{stream_id, tablet_id};
  const string floor_holder = HistoryFloorHolder(stream_id);

  // Sets this stream's history floor on the tablet back to what its session
  // state implies, undoing a pin taken for a resume or an establish that did not
  // complete.
  const auto restore_history_floor = [&]() {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end()) {
      PublishHistoryFloorUnlocked(stream_id, it->second.get(), tablet.get());
    } else {
      tablet->SetCDCHistoryFloorHolder(floor_holder, Timestamp(0));
    }
  };

  // Ends the active snapshot session (if it is still the one with 'snap_ts')
  // and fails the page with SNAPSHOT_SESSION_LOST: the consumer must restart
  // the snapshot from the beginning.
  const auto lose_session = [&](const Timestamp& snap_ts, const Status& s) {
    {
      std::lock_guard lock(lock_);
      auto it = stream_tablet_state_.find(key);
      if (it != stream_tablet_state_.end() && it->second->snapshot.active &&
          it->second->snapshot.snap_ts == snap_ts) {
        it->second->snapshot = CDCSnapshotState();
        PublishHistoryFloorUnlocked(stream_id, it->second.get(), tablet.get());
      }
    }
    snapshot_sessions_lost_.fetch_add(1, std::memory_order_relaxed);
    resp->clear_records();
    SetCDCError(CDCErrorPB::SNAPSHOT_SESSION_LOST, s, resp->mutable_error());
    return s;
  };

  // Serve the pages of one (stream, tablet) one at a time. A page decides where
  // to scan from by the paging state the previous page left, and two concurrent
  // is_snapshot_start calls must not both establish a session (the second would
  // overwrite the snap_ts the first's pages were read at). The establish and
  // the scan block, so this is a per-key mutex rather than lock_. A page that
  // cannot get it by its deadline (the client gave up on it) is shed.
  std::shared_ptr<std::timed_mutex> page_lock = GetSnapshotPageLock(key);
  SCOPED_CLEANUP({
    page_lock.reset();
    std::lock_guard lock(lock_);
    EraseIfUnused(&snapshot_page_locks_, key);
  });
  std::unique_lock<std::timed_mutex> page_guard(*page_lock, std::defer_lock);
  if (deadline.Initialized()) {
    const MonoDelta remaining = deadline - MonoTime::Now();
    if (!page_guard.try_lock_for(std::chrono::nanoseconds(
            std::max<int64_t>(0, remaining.ToNanoseconds())))) {
      return Status::TimedOut(Substitute(
          "another page of the snapshot of tablet $0 is being served", tablet_id));
    }
  } else {
    page_guard.lock();
  }

  // 2. Decide where this page starts: 'scan_after' is the key the scan
  //    resumes strictly after (none for the start of the table).
  bool has_active_session = false;
  optional<string> page_start_key;
  optional<string> page_end_key;
  optional<string> page_request_key;
  {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end() && it->second->snapshot.active) {
      has_active_session = true;
      page_start_key = it->second->snapshot.page_start_key;
      page_end_key = it->second->snapshot.page_end_key;
      page_request_key = it->second->snapshot.page_request_key;
    }
  }
  const auto matches = [&](const optional<string>& k) {
    return req_resume_key && k && *req_resume_key == *k;
  };
  bool start_new = false;
  optional<string> scan_after;
  // Set if the session is rebuilt from the master's durable snapshot cursor.
  bool rebuild = false;
  SnapshotCursorUpdate cursor;
  int64_t cursor_streaming_start = 0;
  if (new_generation) {
    // The client asked for a new snapshot (see
    // GetChangesRequestPB.new_snapshot_generation).
    start_new = true;
  } else if (has_active_session) {
    if (is_start) {
      // The consumer restarts the snapshot (e.g. it restarted, or lost the
      // response of the first page): scan again from the start of the table, at
      // the same snapshot timestamp.
      scan_after.reset();
    } else if (matches(page_end_key)) {
      // The consumer got the last page and continues after it.
      scan_after = page_end_key;
    } else if (matches(page_start_key) || matches(page_request_key)) {
      // The consumer lost the response of the last page and asks for it again.
      scan_after = page_start_key;
    } else {
      // Not a position this session served, e.g. a key of an earlier page or of
      // another session: serving after it would skip or repeat rows the
      // consumer cannot tell apart. The session is kept.
      Status s = Status::IllegalState(
          "snapshot resume key does not match the last page of the snapshot "
          "session; restart the snapshot from the beginning with "
          "is_snapshot_start=true and no resume key");
      SetCDCError(CDCErrorPB::SNAPSHOT_SESSION_LOST, s, resp->mutable_error());
      return s;
    }
  } else {
    // This server holds no session for the snapshot: a leader change, a
    // restart of this server or a lost session discarded it, or the snapshot
    // was never taken. Whether there is an unfinished snapshot to continue is
    // known only from the master's durable cursor (snap_ts, the acknowledged
    // resume key and the streaming start point), so it is read even for a
    // start. Taking a new snapshot instead would read the tablet at a new
    // timestamp: the rows the consumer got from the earlier pages and deleted
    // since would be in neither that snapshot nor the stream after it, and
    // nothing would tell the consumer.
    SnapshotCursorFetch fetched = cursor_fetch;
    if (!fetched.fetched) {
      fetched.fetched = true;
      fetched.status = FetchSnapshotCursor(stream_id, tablet_id, deadline, &fetched.found,
                                           &fetched.cursor,
                                           &fetched.streaming_start_op_index);
    }
    if (!fetched.status.ok()) {
      // Neither a new snapshot nor a resume is safe without the cursor. The
      // master may be back soon: retry.
      Status s = Status::ServiceUnavailable(
          Substitute("could not read the snapshot cursor of tablet $0 from the master; "
                     "retry", tablet_id),
          fetched.status.ToString());
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, s, resp->mutable_error());
      return s;
    }
    const bool found = fetched.found && fetched.cursor.active && fetched.cursor.snap_ts != 0;
    if (req_resume_key) {
      // Cold-leader resume. A resume_key is only valid while continuing a
      // snapshot at the timestamp its earlier pages were read at: scanning the
      // table tail (rows > resume_key) at a freshly chosen snap_ts while the
      // head was read at the original snap_ts would not be a self-consistent
      // snapshot. Rebuild the session from the durable cursor, so the tail is
      // read at the ORIGINAL snap_ts. The durable key is a position the client
      // acknowledged, so the client has every row up to it, and its own key is
      // the end of a page it received. Resume after the lower of the two: rows
      // between them are repeated, which at-least-once delivery allows, and
      // none is skipped (the client may be behind the durable cursor if it
      // restarted the snapshot). An empty durable key is the start of the
      // table.
      if (!found) {
        Status s = Status::IllegalState(
            "snapshot session not found and cannot be resumed (the master has no "
            "cursor for an unfinished snapshot of the tablet); restart the "
            "snapshot from the beginning with is_snapshot_start=true and no "
            "resume key");
        snapshot_sessions_lost_.fetch_add(1, std::memory_order_relaxed);
        SetCDCError(CDCErrorPB::SNAPSHOT_SESSION_LOST, s, resp->mutable_error());
        return s;
      }
      if (!fetched.cursor.resume_key.empty() &&
          Slice(*req_resume_key).compare(Slice(fetched.cursor.resume_key)) >= 0) {
        scan_after = fetched.cursor.resume_key;
      } else if (fetched.cursor.resume_key.empty()) {
        scan_after.reset();
      } else {
        scan_after = *req_resume_key;
      }
      rebuild = true;
    } else if (found) {
      // A start of a snapshot that is unfinished: take it again from the start
      // of the table at its snapshot timestamp.
      scan_after.reset();
      rebuild = true;
    } else {
      start_new = true;
    }
    cursor = fetched.cursor;
    cursor_streaming_start = fetched.streaming_start_op_index;
  }

  if (rebuild) {
    // Rebuild the session of the unfinished snapshot of the durable cursor at
    // its snapshot timestamp and streaming start point. It is only valid if the
    // tablet still retains history at snap_ts. Pin history at snap_ts first, so
    // no later GC can remove it, then check that no earlier GC already did: the
    // history-GC water mark records what was actually removed. The current
    // ancient history mark is not the right test: it may be above snap_ts with
    // the history still intact, or have been lowered by a floor pinned after
    // the history was gone.
    const Timestamp resumed_snap_ts(cursor.snap_ts);
    tablet->SetCDCHistoryFloorHolder(floor_holder, resumed_snap_ts);
    auto unpin = MakeScopedCleanup(restore_history_floor);
    const Timestamp water_mark = tablet->cdc_history_gc_water_mark();
    if (water_mark > resumed_snap_ts) {
      // The snapshot cannot be continued, and a new one is taken only if the
      // client asks for it: it does not tell the client which of the rows that
      // it got were deleted meanwhile.
      Status s = Status::IllegalState(Substitute(
          "the snapshot of tablet $0 in progress cannot be continued: the history "
          "at its snapshot timestamp $1 is no longer retained (history GC water "
          "mark $2); take a new snapshot (is_snapshot_start=true, "
          "new_snapshot_generation=true) and replace the rows of the tablet with it",
          tablet_id, resumed_snap_ts.ToString(), water_mark.ToString()));
      snapshot_sessions_lost_.fetch_add(1, std::memory_order_relaxed);
      SetCDCError(CDCErrorPB::SNAPSHOT_SESSION_LOST, s, resp->mutable_error());
      return s;
    }
    RETURN_NOT_OK(replica->time_manager()->WaitUntilSafe(resumed_snap_ts, deadline));
    {
      tablet::MvccSnapshot ignored;
      RETURN_NOT_OK(tablet->mvcc_manager()->WaitForSnapshotWithAllApplied(
          resumed_snap_ts, &ignored, deadline));
    }
    CDCStreamTabletState* tracked = nullptr;
    {
      const Status ts = TrackSession(stream_id, tablet_id, cursor_streaming_start,
                                     replica.get(), &tracked);
      if (PREDICT_FALSE(!ts.ok())) {
        SetCDCError(TrackSessionErrorCode(ts), ts, resp->mutable_error());
        return ts;
      }
    }
    if (snapshot_session_tracked_hook_for_tests_) {
      snapshot_session_tracked_hook_for_tests_();
    }
    // The schema at the original snap_ts is not known: pin the current one.
    // Its version is stamped on every record.
    SchemaPtr schema;
    uint32_t schema_version;
    replica->tablet_metadata()->GetSchemaAndVersion(&schema, &schema_version);
    std::lock_guard lock(lock_);
    const Status cs = CheckTrackedSessionUnlocked(key, tracked);
    if (PREDICT_FALSE(!cs.ok())) {
      SetCDCError(TrackSessionErrorCode(cs), cs, resp->mutable_error());
      return cs;
    }
    CDCStreamTabletState* state = tracked;
    state->snapshot = CDCSnapshotState();
    state->snapshot.active = true;
    state->snapshot.snap_ts = resumed_snap_ts;
    state->snapshot.streaming_start_op_index = cursor_streaming_start;
    state->snapshot.schema = std::move(schema);
    state->snapshot.schema_version = schema_version;
    state->history_floor_by_from_index.clear();
    PublishHistoryFloorUnlocked(stream_id, state, tablet.get());
    unpin.cancel();
    snapshot_sessions_resumed_.fetch_add(1, std::memory_order_relaxed);
  }

  if (start_new) {
    // Test hook: widen the establish window (for the race of concurrent first
    // pages, which must establish one session) and let a test
    // exercise the deadline. If the injected latency would exceed the
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

    // Capture the WAL streaming start point, then the snapshot timestamp. In
    // this order every op at or below the start point got its timestamp before
    // it committed, so below snap_ts, and the snapshot sees it. The other way
    // round, an op that got a timestamp above snap_ts and committed before the
    // start point was read would be in neither the snapshot nor the stream.
    // An ALTER is not ordered against snap_ts by the waits below: remember the
    // schema version, so that the establish can tell whether one applied
    // meanwhile.
    const uint32_t schema_version_before = replica->tablet_metadata()->schema_version();
    int64_t streaming_start_op_index = 0;
    if (auto last = consensus->GetLastOpId(consensus::COMMITTED_OPID)) {
      streaming_start_op_index = last->index();
    }
    if (snapshot_establish_hook_for_tests_) {
      snapshot_establish_hook_for_tests_();
    }
    const Timestamp snap_ts = replica->clock()->Now();
    // Pin history at snap_ts right away. A new snapshot supersedes whatever
    // this stream held before, so it replaces the stream's floor; the pin is
    // undone if the establish does not complete.
    tablet->SetCDCHistoryFloorHolder(floor_holder, snap_ts);
    auto unpin = MakeScopedCleanup(restore_history_floor);
    RETURN_NOT_OK(replica->time_manager()->WaitUntilSafe(snap_ts, deadline));
    tablet::MvccSnapshot all_applied;
    RETURN_NOT_OK(tablet->mvcc_manager()->WaitForSnapshotWithAllApplied(
        snap_ts, &all_applied, deadline));
    // Every op at or below the start point is applied now. A transaction the
    // snapshot does not see (it is open, or commits after snap_ts) is streamed
    // from its beginning.
    LowerStreamingStartBelowTxns(replica.get(),
                                 tablet->MinBeginOpIndexOfTxnsNotCommittedIn(all_applied),
                                 &streaming_start_op_index);
    SchemaPtr schema;
    uint32_t schema_version;
    replica->tablet_metadata()->GetSchemaAndVersion(&schema, &schema_version);
    // An ALTER that applied during the establish may be above the streaming
    // start point, so that the stream delivers it after READ records that
    // already have its schema. Establish again.
    if (PREDICT_FALSE(schema_version != schema_version_before)) {
      Status s = Status::ServiceUnavailable(Substitute(
          "the schema of tablet $0 changed while its snapshot was being "
          "established; retry", tablet_id));
      SetCDCError(CDCErrorPB::SERVER_TOO_BUSY, s, resp->mutable_error());
      return s;
    }
    // The establish persists the streaming start point as the stream's
    // checkpoint, so the master's retention barrier keeps the WAL from there
    // on until the consumer streams from it.
    CDCStreamTabletState* tracked = nullptr;
    {
      const Status ts = TrackSession(stream_id, tablet_id, streaming_start_op_index,
                                     replica.get(), &tracked);
      if (PREDICT_FALSE(!ts.ok())) {
        SetCDCError(TrackSessionErrorCode(ts), ts, resp->mutable_error());
        return ts;
      }
    }
    if (snapshot_session_tracked_hook_for_tests_) {
      snapshot_session_tracked_hook_for_tests_();
    }

    std::lock_guard lock(lock_);
    // The session may have ended since it was tracked (the stream was deleted,
    // this replica lost leadership or stopped). Creating it again here would
    // pin history at snap_ts for a session that nothing ends.
    const Status cs = CheckTrackedSessionUnlocked(key, tracked);
    if (PREDICT_FALSE(!cs.ok())) {
      SetCDCError(TrackSessionErrorCode(cs), cs, resp->mutable_error());
      return cs;
    }
    CDCStreamTabletState* state = tracked;
    state->snapshot = CDCSnapshotState();
    state->snapshot.active = true;
    state->snapshot.snap_ts = snap_ts;
    state->snapshot.streaming_start_op_index = streaming_start_op_index;
    state->snapshot.schema = std::move(schema);
    state->snapshot.schema_version = schema_version;
    state->history_floor_by_from_index.clear();
    PublishHistoryFloorUnlocked(stream_id, state, tablet.get());
    unpin.cancel();
    snapshot_sessions_established_.fetch_add(1, std::memory_order_relaxed);
  }

  // 3. Load the (now-established) snapshot session state, and record where
  //    this page starts. Until the page is done its end is its start, so a
  //    retry of a page that failed serves it again.
  Timestamp snap_ts;
  int64_t streaming_start_op_index = 0;
  std::shared_ptr<const Schema> schema;
  uint32_t schema_version = 0;
  {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(key);
    if (it == stream_tablet_state_.end() || !it->second->snapshot.active) {
      return Status::IllegalState(
          "no active snapshot; call GetChanges with is_snapshot_start=true");
    }
    CDCSnapshotState* snapshot = &it->second->snapshot;
    snap_ts = snapshot->snap_ts;
    streaming_start_op_index = snapshot->streaming_start_op_index;
    schema = snapshot->schema;
    schema_version = snapshot->schema_version;
    snapshot->done = false;
    snapshot->page_start_key = scan_after;
    snapshot->page_end_key = scan_after;
    if (req_resume_key && !is_start) {
      snapshot->page_request_key = *req_resume_key;
    } else {
      snapshot->page_request_key.reset();
    }
    // Re-assert the session's history floor for this page, in case a CDC
    // retention release dropped it since the previous page.
    PublishHistoryFloorUnlocked(stream_id, it->second.get(), tablet.get());
  }
  DCHECK(schema);

  // 4. Rebuild the MVCC snapshot at snap_ts (fast: the timestamp is in the past
  //    and all ops are applied), and make sure the history at snap_ts has not
  //    been garbage-collected since the snapshot began. The floor normally
  //    prevents that, but a CDC retention release (the disk or age valve, or
  //    the master releasing the tablet) drops it. A scan below the GC point
  //    silently returns newer row versions, so the session is lost instead.
  //    The check is repeated after the scan, for a GC that ran concurrently.
  const auto check_history = [&]() {
    const Timestamp water_mark = tablet->cdc_history_gc_water_mark();
    if (PREDICT_TRUE(water_mark <= snap_ts)) {
      return Status::OK();
    }
    return lose_session(snap_ts, Status::IllegalState(Substitute(
        "snapshot history has been garbage-collected (snapshot ts $0 < history "
        "GC water mark $1); restart the snapshot from the beginning",
        snap_ts.ToString(), water_mark.ToString())));
  };
  tablet::MvccSnapshot snap;
  RETURN_NOT_OK(tablet->mvcc_manager()->WaitForSnapshotWithAllApplied(
      snap_ts, &snap, deadline));
  RETURN_NOT_OK(check_history());

  // 5. Build the ordered scan with the session's schema, resuming after
  // 'scan_after' if it is set. A page must return the same columns as the
  // pages before it: if an ALTER dropped, renamed or retyped one of them, the
  // snapshot cannot be completed and the consumer must take a new one.
  {
    const Status ss = CheckSnapshotSchemaUnchanged(*schema, *tablet->schema());
    if (PREDICT_FALSE(!ss.ok())) {
      return lose_session(snap_ts, ss);
    }
  }
  // Pass a column-id-free projection; the iterator maps it onto the tablet's
  // column IDs internally (iter->schema() is the mapped form).
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
  if (scan_after) {
    EncodedKey* lower = nullptr;
    RETURN_NOT_OK(EncodedKey::DecodeEncodedString(
        *schema, &arena, Slice(*scan_after), &lower));
    if (EncodedKey::IncrementEncodedKey(*schema, &lower, &arena).ok()) {
      spec.SetLowerBoundKey(lower);
    } else {
      resume_key_is_max = true;
    }
  }

  // 6. Emit READ records up to max_bytes (and up to the record-count cap), and
  // end the page early once its deadline passed, so that the client gets the
  // rows read so far instead of timing out. Skip the scan entirely when the
  // resume key was the maximum key: there can be no rows strictly after it, so
  // this is an empty terminal page.
  int64_t bytes = 0;
  string last_key;
  bool truncated = false;
  const int32_t rec_cap = FLAGS_cdc_max_records_per_response;
  if (!resume_key_is_max) {
    RETURN_NOT_OK(iter->Init(&spec));
    const Schema& iter_schema = iter->schema();
    RowBlockMemory mem(1024);
    RowBlock block(&iter_schema, 256, &mem);
    Arena key_arena(256);
    while (iter->HasNext()) {
      if (resp->records_size() > 0 &&
          (PREDICT_FALSE(FLAGS_cdc_inject_snapshot_scan_deadline_expired) ||
           (deadline.Initialized() && MonoTime::Now() >= deadline))) {
        truncated = true;
        break;
      }
      RETURN_NOT_OK(iter->NextBlock(&block));
      for (size_t i = 0; i < block.nrows(); ++i) {
        if (!block.selection_vector()->IsRowSelected(i)) {
          continue;
        }
        // Record-count cap. Checked before adding so we
        // never exceed the cap; the existing byte cap is checked after
        // (consistent with its existing semantics). Snapshot rows have no
        // transaction grouping concern so a strict per-row cap is safe here.
        if (rec_cap > 0 && resp->records_size() >= rec_cap) {
          truncated = true;
          break;
        }
        RowBlockRow row = block.row(i);
        CDCRecordPB* rec = resp->add_records();
        rec->set_op_type(CDCOpTypePB::READ);
        rec->set_timestamp(snap_ts.value());
        rec->set_schema_version(schema_version);
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

  if (snapshot_page_scanned_hook_for_tests_) {
    snapshot_page_scanned_hook_for_tests_();
  }
  // A history GC may have raced the scan.
  RETURN_NOT_OK(check_history());

  // Post-scan leader-term recheck, as in ReadChanges(): if this replica lost
  // leadership or the term advanced since the check at the top, fail the page
  // as TABLET_NOT_LEADER (via the IllegalState mapping), so that the consumer
  // retries against the current leader. The paging state still has the page's
  // start, so the retry serves the page again. A session that this page
  // established or rebuilt after the leadership change ended the sessions on
  // this replica is ended here: nothing else would end it, and its snapshot
  // would pin history at snap_ts.
  if (PREDICT_FALSE(FLAGS_cdc_inject_post_read_leadership_loss) ||
      consensus->role() != RaftPeerPB::LEADER ||
      consensus->CurrentTerm() != leader_term) {
    EndSessionsIfNotLeader(tablet_id);
    resp->clear_records();
    return Status::IllegalState(Substitute(
        "tablet $0 leadership changed during the snapshot page (term $1 -> $2); "
        "retry against the current leader",
        tablet_id, leader_term, consensus->CurrentTerm()));
  }

  // The scan is complete when we exhausted the iterator (did not stop early).
  const bool done = !truncated;

  // 7. Update session state and response fields, and decide whether to durably
  // persist the snapshot cursor to the master this page. The cursor is the
  // position this page started after, the newest one the client acknowledged:
  // the rows of this page are not acknowledged until the client asks for the
  // next one. It is persisted when the snapshot starts, on its last page, and
  // otherwise at most once per --cdc_snapshot_cursor_persist_interval_ms
  // (throttled like the checkpoint). A throttled page only defers the durable
  // cursor: on a leader change before the next persist the consumer replays at
  // most one interval's worth of rows, which the at-least-once contract
  // already permits.
  bool persist_cursor = false;
  {
    const int64_t now_micros = GetCurrentTimeMicros();
    const int64_t persist_interval_micros =
        FLAGS_cdc_snapshot_cursor_persist_interval_ms > 0
            ? FLAGS_cdc_snapshot_cursor_persist_interval_ms * 1000 : 0;
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end() && it->second->snapshot.active &&
        it->second->snapshot.snap_ts == snap_ts) {
      if (done) {
        // The session stays active until the client acknowledges this page
        // (see CompleteSnapshotIfAcknowledged()). After that, a FULL stream
        // keeps history from snap_ts until its checkpoint moves past the
        // streaming start point; a CHANGE stream needs no history.
        it->second->snapshot.done = true;
        if (is_full_mode) {
          it->second->history_floor_by_from_index.emplace(streaming_start_op_index,
                                                          snap_ts.value());
        }
        PublishHistoryFloorUnlocked(stream_id, it->second.get(), tablet.get());
      } else {
        it->second->snapshot.page_end_key = last_key;
      }
      // Always persist on the establishing page and on the last page;
      // otherwise throttle. A brand-new cursor (last_snapshot_cursor_persist_micros == 0)
      // always persists so a resume is possible from the very first page.
      if (start_new || done ||
          persist_interval_micros <= 0 ||
          it->second->last_snapshot_cursor_persist_micros == 0 ||
          now_micros - it->second->last_snapshot_cursor_persist_micros >=
              persist_interval_micros) {
        persist_cursor = true;
        it->second->last_snapshot_cursor_persist_micros = now_micros;
        // The persist refreshes the activity on the master too.
        it->second->last_active_report_micros = now_micros;
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
  // The durable snapshot cursor (snap_ts + resume_key + active) rides along so a
  // cold leader can rebuild the in-memory session and resume (see
  // FetchSnapshotCursor / the cold-resume path above).
  //
  // The pages before the last one queue the persist: they hold a scan slot,
  // and a lost cursor only costs a re-snapshot. The last page persists before
  // it responds, within the page's deadline, so that the master holds the
  // streaming start point before the consumer streams from it; if that fails,
  // the persist is queued like the others. The cursor stays active until the
  // client acknowledged the last page, so that until then a new leader can
  // serve that page again and a restarted consumer takes the snapshot again.
  // The first page of a new snapshot generation persists before it responds
  // too: until the master has the new cursor, it has the one of the earlier
  // generation, and a new leader would continue that one from the client's
  // next resume key, which is a position of the new generation.
  if (persist_cursor) {
    const uint64_t floor_micros = HybridClock::GetPhysicalValueMicros(snap_ts);
    SnapshotCursorUpdate cursor;
    cursor.snap_ts = snap_ts.value();
    // An empty key is the start of the table to a resume (see
    // SnapshotCursorUpdate::resume_key).
    cursor.resume_key = scan_after.value_or(string());
    cursor.active = true;
    auto update = MakeCheckpointUpdate(stream_id, tablet_id, streaming_start_op_index,
                                       floor_micros, /*refresh_active_time_only=*/false,
                                       &cursor);
    if (done || (start_new && new_generation)) {
      PersistCheckpointNow(std::move(update), deadline);
    } else {
      QueueCheckpointUpdate(std::move(update));
    }
  }
  return Status::OK();
}

void CDCServiceImpl::CompleteSnapshotIfAcknowledged(const string& stream_id,
                                                    const string& tablet_id,
                                                    int64_t op_index,
                                                    tablet::Tablet* tablet) {
  unique_ptr<master::UpdateCDCCheckpointRequestPB> update;
  {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(CDCStreamTabletKey{stream_id, tablet_id});
    if (it == stream_tablet_state_.end()) {
      return;
    }
    CDCSnapshotState* snapshot = &it->second->snapshot;
    // A position of 0 acknowledges nothing: it is where a consumer that has
    // not streamed yet is, even if the streaming start point is 0 (see
    // LowerStreamingStartBelowTxns()).
    if (!snapshot->active || !snapshot->done || op_index <= 0 ||
        op_index < snapshot->streaming_start_op_index) {
      return;
    }
    SnapshotCursorUpdate cursor;
    cursor.snap_ts = snapshot->snap_ts.value();
    cursor.resume_key = snapshot->page_start_key.value_or(string());
    cursor.active = false;
    update = MakeCheckpointUpdate(stream_id, tablet_id, snapshot->streaming_start_op_index,
                                  HybridClock::GetPhysicalValueMicros(snapshot->snap_ts),
                                  /*refresh_active_time_only=*/false, &cursor);
    *snapshot = CDCSnapshotState();
    PublishHistoryFloorUnlocked(stream_id, it->second.get(), tablet);
  }
  QueueCheckpointUpdate(std::move(update));
}

// ---------------------------------------------------------------------------
// History floor (internal)
// ---------------------------------------------------------------------------

string CDCServiceImpl::HistoryFloorHolder(const string& stream_id) {
  return Substitute("cdc-stream:$0", stream_id);
}

void CDCServiceImpl::PublishHistoryFloorUnlocked(const string& stream_id,
                                                 CDCStreamTabletState* state,
                                                 tablet::Tablet* tablet) {
  // Drop the batches before the one that covers the resume point: a consumer
  // resumes after it, and op timestamps grow with the op index.
  auto& floors = state->history_floor_by_from_index;
  const int64_t resume_op_index = state->history_resume_op_index >= 0
      ? state->history_resume_op_index
      : state->checkpoint_op_index.load(std::memory_order_relaxed);
  const auto covering = floors.upper_bound(resume_op_index);
  if (covering != floors.begin()) {
    floors.erase(floors.begin(), std::prev(covering));
  }
  uint64_t floor = 0;
  for (const auto& f : floors) {
    floor = floor == 0 ? f.second : std::min(floor, f.second);
  }
  if (state->snapshot.active) {
    const uint64_t snap = state->snapshot.snap_ts.value();
    floor = floor == 0 ? snap : std::min(floor, snap);
  }
  state->history_floor = floor;
  if (tablet) {
    tablet->SetCDCHistoryFloorHolder(HistoryFloorHolder(stream_id),
                                     Timestamp(HeldHistoryFloorUnlocked(*state, *tablet)));
  }
}

uint64_t CDCServiceImpl::HeldHistoryFloorUnlocked(const CDCStreamTabletState& state,
                                                  const tablet::Tablet& tablet) {
  // The floors of the batches served are held only while the tablet has a
  // retention barrier. Without one (the master released the tablet, e.g. once
  // it stopped counting the stream as stale or expired), the stream has no
  // retention guarantee, and the master sends nothing more that would drop a
  // floor pinned again by a consumer that keeps polling without checkpointing.
  // Such a consumer gets WAL_EXPIRED or HISTORY_EXPIRED once GC passes its
  // position. The barrier is updated under 'lock_' (see SetRetentionBarrier()),
  // and so is every holder of a session.
  uint64_t floor = 0;
  if (tablet.metadata()->cdc_min_retained_op_index() >= 0) {
    // A batch floor older than --cdc_session_history_floor_max_age_secs is not
    // held either: the master holds the floor of a checkpointed position, so
    // this only bounds the history pinned by a consumer that does not
    // checkpoint.
    int64_t cutoff_micros = 0;
    const int64_t max_age_secs = FLAGS_cdc_session_history_floor_max_age_secs;
    if (max_age_secs > 0) {
      const int64_t now_micros = GetCurrentTimeMicros();
      if (max_age_secs < now_micros / 1000000) {
        cutoff_micros = now_micros - max_age_secs * 1000000;
      }
    }
    for (const auto& f : state.history_floor_by_from_index) {
      if (static_cast<int64_t>(HybridClock::GetPhysicalValueMicros(Timestamp(f.second))) <
          cutoff_micros) {
        continue;
      }
      floor = floor == 0 ? f.second : std::min(floor, f.second);
    }
  }
  // The snapshot pin is bounded by the snapshot session.
  if (state.snapshot.active) {
    const uint64_t snap = state.snapshot.snap_ts.value();
    floor = floor == 0 ? snap : std::min(floor, snap);
  }
  return floor;
}

int64_t CDCServiceImpl::StreamIdleExpiryMsUnlocked(const string& stream_id) const {
  const auto it = stream_config_cache_.find(stream_id);
  if (it != stream_config_cache_.end() && it->second.idle_expiry_ms >= 0) {
    return it->second.idle_expiry_ms;
  }
  return FLAGS_cdc_stream_idle_expiry_ms;
}

void CDCServiceImpl::DropSessionHistoryFloorsUnlocked(const string& tablet_id,
                                                      bool is_leader,
                                                      tablet::Tablet* tablet) {
  if (!tablet) {
    return;
  }
  const int64_t now_micros = GetCurrentTimeMicros();
  for (const auto& entry : stream_tablet_state_) {
    if (entry.first.tablet_id != tablet_id) {
      continue;
    }
    if (is_leader) {
      const int64_t expiry_ms = StreamIdleExpiryMsUnlocked(entry.first.stream_id);
      const int64_t last_active =
          entry.second->last_active_time_micros.load(std::memory_order_relaxed);
      if (expiry_ms <= 0 || last_active <= 0 ||
          now_micros - last_active <= expiry_ms * 1000) {
        // An active session keeps its floor, but drops the batch floors that
        // have grown older than --cdc_session_history_floor_max_age_secs since
        // it last pinned them, also when it no longer reads. This never pins a
        // floor that was cleared (e.g. by the retention valve).
        tablet->RaiseCDCHistoryFloorHolder(
            HistoryFloorHolder(entry.first.stream_id),
            Timestamp(HeldHistoryFloorUnlocked(*entry.second, *tablet)));
        continue;
      }
    }
    tablet->SetCDCHistoryFloorHolder(HistoryFloorHolder(entry.first.stream_id),
                                     Timestamp(0));
  }
}

CDCStreamTabletStateMap::iterator CDCServiceImpl::EraseSessionUnlocked(
    CDCStreamTabletStateMap::iterator it,
    tablet::Tablet* tablet) {
  const CDCStreamTabletKey& key = it->first;
  if (tablet) {
    tablet->SetCDCHistoryFloorHolder(HistoryFloorHolder(key.stream_id), Timestamp(0));
  }
  // Unpublish the session's metric entity, so a session created later for the
  // same (stream, tablet) gets a new entity with new gauges instead of the
  // detached gauges of this one, which keep reporting their last value.
  if (it->second->metric_entity) {
    it->second->metric_entity->Unpublish();
  }
  return stream_tablet_state_.erase(it);
}

// ---------------------------------------------------------------------------
// TrackSession (internal)
// ---------------------------------------------------------------------------

Status CDCServiceImpl::TrackSession(const string& stream_id,
                                    const string& tablet_id,
                                    int64_t op_index,
                                    TabletReplica* replica,
                                    CDCStreamTabletState** session) {
  if (PREDICT_FALSE(FLAGS_cdc_inject_latency_before_session_update_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_cdc_inject_latency_before_session_update_ms));
  }
  const CDCStreamTabletKey key{stream_id, tablet_id};
  CDCStreamTabletState* created = nullptr;
  {
    std::lock_guard lock(lock_);
    if (shutting_down_) {
      return Status::ServiceUnavailable("CDC service is shutting down");
    }
    if (ContainsKey(released_consumer_streams_, stream_id)) {
      return Status::NotFound(Substitute("CDC stream $0 has been deleted", stream_id));
    }
    const auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end()) {
      if (session) {
        *session = it->second.get();
      }
      return Status::OK();
    }
    auto new_state = std::make_unique<CDCStreamTabletState>();
    new_state->checkpoint_op_index.store(op_index, std::memory_order_relaxed);
    created = new_state.get();
    stream_tablet_state_.emplace(key, std::move(new_state));
    SetupSessionMetrics(key, created);
  }
  // The tablet manager ends the sessions of a replica after stopping it (see
  // EndSessionsForTablet()). A session created after that would outlive the
  // replica, so re-check that it is still running once the session is visible.
  const Status s = replica->CheckRunning();
  if (PREDICT_FALSE(!s.ok())) {
    std::lock_guard lock(lock_);
    auto it = stream_tablet_state_.find(key);
    if (it != stream_tablet_state_.end() && it->second.get() == created) {
      EraseSessionUnlocked(it, /*tablet=*/nullptr);
    }
    return s;
  }
  if (session) {
    *session = created;
  }
  return Status::OK();
}

Status CDCServiceImpl::CheckTrackedSessionUnlocked(const CDCStreamTabletKey& key,
                                                   const CDCStreamTabletState* tracked) const {
  DCHECK(lock_.is_locked());
  if (ContainsKey(released_consumer_streams_, key.stream_id)) {
    return Status::NotFound(Substitute("CDC stream $0 has been deleted", key.stream_id));
  }
  const auto it = stream_tablet_state_.find(key);
  if (it == stream_tablet_state_.end() || it->second.get() != tracked) {
    return Status::IllegalState(Substitute(
        "the CDC session of tablet $0 ended while its snapshot was being set up "
        "(this replica lost leadership or stopped); retry", key.tablet_id));
  }
  return Status::OK();
}

bool CDCServiceImpl::RetentionBarrierForTests(const string& tablet_id,
                                              int64_t* min_retained_op_index) const {
  scoped_refptr<TabletReplica> replica;
  if (!server_->tablet_manager()->GetTabletReplica(tablet_id, &replica).ok()) {
    return false;
  }
  const int64_t barrier = replica->tablet_metadata()->cdc_min_retained_op_index();
  if (barrier < 0) {
    return false;
  }
  if (min_retained_op_index) {
    *min_retained_op_index = barrier;
  }
  return true;
}

// ---------------------------------------------------------------------------
// SetRetentionBarrier
// ---------------------------------------------------------------------------

Status CDCServiceImpl::SetRetentionBarrier(const string& tablet_id,
                                           int64_t min_retained_op_index,
                                           uint64_t history_safe_time_micros,
                                           int64_t barrier_seq,
                                           const string& release_consumer_stream_id,
                                           bool skip_barrier_update,
                                           bool* superseded,
                                           bool tablet_dropped) {
  scoped_refptr<TabletReplica> replica;
  RETURN_NOT_OK_PREPEND(
      server_->tablet_manager()->GetTabletReplica(tablet_id, &replica),
      Substitute("tablet $0 not found on this server", tablet_id));
  // Whether the aggregate WAL retention barrier is actually being applied on
  // this call. Set false when the caller only wants a session release
  // ('skip_barrier_update') or when the sequence gate finds this update stale.
  // Hoisted out of the lock so the superblock persist below can be skipped
  // accordingly.
  bool apply_barrier = !skip_barrier_update;
  // Whether the barrier persisted in the superblock must be flushed.
  bool needs_flush = false;
  // Barrier and session transitions to log once 'lock_' is released.
  vector<string> transitions;
  const scoped_refptr<tablet::TabletMetadata>& meta = replica->tablet_metadata();
  if (superseded) {
    *superseded = false;
  }
  if (tablet_dropped && replica->RejectWritesOfDroppedTablet()) {
    needs_flush = true;
    transitions.emplace_back(
        "rejecting writes: the range partition was dropped, the tablet is kept for CDC");
  }
  // Read outside 'lock_': it takes the consensus lock. A replica that is not
  // running, such as one under tablet copy, is not the leader.
  const bool is_leader = RoleIfRunning(replica.get()) == RaftPeerPB::LEADER;
  {
    // Hold 'lock_' across the in-memory update -- the sequence gate, the
    // barrier update in the tablet metadata, the history-floor apply, and the
    // session release -- so a stale update cannot slip its apply in
    // between another update's gate and apply. Setting a history floor holder
    // takes only a leaf spinlock, and the session scan is the same in-memory
    // walk as in EndSessionsForTablet(), so this does not meaningfully widen
    // the critical section.
    // The metadata Flush() below is deliberately kept OUTSIDE this lock: it may
    // fsync the superblock, and holding 'lock_' across a blocking flush would
    // serialize all CDC operations on this server.
    std::lock_guard lock(lock_);
    if (shutting_down_) {
      return Status::ServiceUnavailable("CDC service is shutting down");
    }

    // Last-writer-wins gate: discard a reordered barrier update that a newer one
    // has already superseded. Barrier RPCs are async, best-effort and unordered,
    // so without this a slow SET from an earlier maintenance pass could land after
    // a later pass's RELEASE and re-anchor this replica forever (a WAL/history
    // leak), or vice versa. The master's sequence orders updates by its leader
    // term first, so a new leader always supersedes the previous one. The last
    // applied sequence lives in the tablet metadata and is persisted with the
    // barrier, so the gate also holds across a restart of this server.
    // barrier_seq == 0 means an unsequenced update, which is always applied. The
    // gate guards ONLY the aggregate barrier; a session release (stream
    // delete) is terminal and always applied.
    //
    // A reordered update outlives the newer one by no more than the lifetime
    // of a master task, so the last sequence rejects lower ones only for
    // --cdc_barrier_reorder_window_secs after it was applied (or after this
    // server started). Otherwise a master whose terms started again from the
    // beginning, as after 'kudu master unsafe_rebuild', could never update
    // the barrier.
    if (apply_barrier && barrier_seq > 0 && meta) {
      const int64_t last_seq = meta->cdc_barrier_seq();
      if (barrier_seq < last_seq) {
        const MonoDelta age = MonoTime::Now() - meta->cdc_barrier_seq_time();
        if (age < MonoDelta::FromSeconds(FLAGS_cdc_barrier_reorder_window_secs)) {
          // Skip the aggregate barrier and its persist; the newer update
          // already did both. Fall through to the session release.
          apply_barrier = false;
          if (superseded) {
            *superseded = true;
          }
          barrier_updates_superseded_->Increment();
        } else {
          LOG(WARNING) << Substitute(
              "T $0: CDC retention barrier sequence went back from $1 to $2, "
              "applying the update since the last one was applied $3 ago",
              tablet_id, last_seq, barrier_seq, age.ToString());
        }
      }
    }

    if (apply_barrier && meta) {
      // Record the barrier, its sequence and the refresh time (for
      // --cdc_barrier_lease_secs and --cdc_max_wal_retention_secs) in the
      // metadata under 'lock_', so updates reach it in gate order. Log GC
      // applies the in-memory value right away; the flush happens below,
      // outside 'lock_'.
      const bool was_set = meta->cdc_min_retained_op_index() >= 0;
      needs_flush |= meta->ApplyCDCRetentionBarrierUpdate(
          min_retained_op_index, history_safe_time_micros, barrier_seq,
          GetCurrentTimeMicros(), BarrierRefreshFlushIntervalMicros());
      if (!was_set && min_retained_op_index >= 0) {
        transitions.emplace_back(Substitute("CDC retention barrier set at op index $0",
                                            min_retained_op_index));
      } else if (was_set && min_retained_op_index < 0) {
        transitions.emplace_back("CDC retention barrier released");
      }
    }

    // Read the tablet after updating the metadata: the replica applies the
    // history floor of the metadata once it publishes the tablet at the end of
    // its bootstrap (see TabletReplica::Start()). So if the tablet is not
    // published yet, the replica applies this update, and if it is, this call
    // does.
    std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
    if (apply_barrier && tablet) {
      if (min_retained_op_index < 0) {
        // The master releases the tablet: no live stream needs its WAL or
        // history any more, so drop every history floor, including the ones
        // held by local sessions of the streams the master has expired.
        tablet->ClearCDCHistoryFloorHolders();
      } else {
        // Otherwise the master's own floor changes; history_safe_time_micros
        // == 0 means no FULL/snapshot stream needs history on this tablet, which
        // removes the master's floor. The local sessions keep theirs, except
        // where the master's floor covers them (see
        // DropSessionHistoryFloorsUnlocked()).
        tablet->SetCDCHistoryFloorHolder(
            tablet::Tablet::kCDCMasterHistoryFloorHolder,
            history_safe_time_micros > 0
                ? HybridClock::TimestampFromMicroseconds(history_safe_time_micros)
                : Timestamp(0));
        DropSessionHistoryFloorsUnlocked(tablet_id, is_leader, tablet.get());
      }
    }

    // End the stream's session on stream deletion. Fanned to every replica, so
    // it also covers a session on a replica that has since lost leadership.
    if (!release_consumer_stream_id.empty()) {
      // Remember the release so an in-flight GetChanges or Checkpoint for the
      // deleted stream cannot create the session again (see TrackSession()).
      released_consumer_streams_.insert(release_consumer_stream_id);
      CDCStreamTabletKey key{release_consumer_stream_id, tablet_id};
      auto it = stream_tablet_state_.find(key);
      if (it != stream_tablet_state_.end()) {
        EraseSessionUnlocked(it, tablet.get());
        transitions.emplace_back(Substitute(
            "ended the CDC consumer session of deleted stream $0",
            release_consumer_stream_id));
      } else if (tablet) {
        // A snapshot establish pins the floor before it creates the session.
        tablet->SetCDCHistoryFloorHolder(HistoryFloorHolder(release_consumer_stream_id),
                                         Timestamp(0));
      }
      // The stream is gone: drop its cached config so the next GetChanges or
      // Checkpoint refetches it from the master and gets STREAM_NOT_FOUND,
      // instead of being served from the cache until its TTL expires.
      stream_config_cache_.erase(release_consumer_stream_id);
    }
  }

  for (const auto& t : transitions) {
    LOG(INFO) << Substitute("T $0: $1", tablet_id, t);
  }
  if (needs_flush) {
    // Persist the barrier in the tablet superblock so WAL/history retention
    // survives a tserver restart or leader change without depending on the
    // master's next maintenance pass. This runs on every replica (the master
    // fans the barrier out to all peers), and only flushes when the persisted
    // value actually changed, or when the persisted refresh time is getting
    // old, so it is bounded to roughly one superblock flush per barrier change
    // per tablet. Skipped when only a session was released (nothing durable
    // changed) or when the update was superseded.
    WARN_NOT_OK(meta->Flush(),
                Substitute("CDC: failed to persist retention barrier for tablet $0",
                           tablet_id));
  }
  return Status::OK();
}

// ---------------------------------------------------------------------------
// EndSessionsForTablet
// ---------------------------------------------------------------------------

void CDCServiceImpl::EndSessionsForTablet(const string& tablet_id) {
  scoped_refptr<TabletReplica> replica;
  std::shared_ptr<tablet::Tablet> tablet;
  if (server_->tablet_manager()->GetTabletReplica(tablet_id, &replica).ok()) {
    tablet = replica->shared_tablet();
  }
  std::lock_guard lock(lock_);
  for (auto it = stream_tablet_state_.begin(); it != stream_tablet_state_.end(); ) {
    if (it->first.tablet_id == tablet_id) {
      it = EraseSessionUnlocked(it, tablet.get());
    } else {
      ++it;
    }
  }
}

void CDCServiceImpl::EndSessionsIfNotLeader(const string& tablet_id) {
  scoped_refptr<TabletReplica> replica;
  if (!server_->tablet_manager()->GetTabletReplica(tablet_id, &replica).ok()) {
    return;
  }
  // Read outside 'lock_': it takes the consensus lock.
  if (RoleIfRunning(replica.get()) == RaftPeerPB::LEADER) {
    return;
  }
  const std::shared_ptr<tablet::Tablet> tablet = replica->shared_tablet();
  int num_ended = 0;
  {
    std::lock_guard lock(lock_);
    for (auto it = stream_tablet_state_.begin(); it != stream_tablet_state_.end();) {
      if (it->first.tablet_id == tablet_id) {
        it = EraseSessionUnlocked(it, tablet.get());
        ++num_ended;
      } else {
        ++it;
      }
    }
  }
  if (num_ended > 0) {
    LOG(INFO) << Substitute("T $0: ended $1 CDC consumer session(s): this replica "
                            "is no longer the leader", tablet_id, num_ended);
  }
}

// ---------------------------------------------------------------------------
// ClearSnapshotSessionsForTests / MemTracker test accessors
// ---------------------------------------------------------------------------

// Test accessors: expose the current byte consumption of each CDC
// MemTracker so tests can assert the true-up correctly returns both trackers
// to their pre-call baseline after a GetChanges. MemTracker::consumption()
// cannot be called from the header because MemTracker is only forward-declared
// there; the implementations live here where the full definition is visible.
int64_t CDCServiceImpl::ScanMemTrackerConsumptionForTests() const {
  return scan_mem_tracker_->consumption();
}

int64_t CDCServiceImpl::WalReadMemTrackerConsumptionForTests() const {
  return wal_read_mem_tracker_->consumption();
}

void CDCServiceImpl::ClearSnapshotSessionsForTests() {
  std::lock_guard lock(lock_);
  for (auto& entry : stream_tablet_state_) {
    entry.second->snapshot = CDCSnapshotState();
  }
}

// ---------------------------------------------------------------------------
// Checkpoint persistence to the master (internal)
// ---------------------------------------------------------------------------

void CDCServiceImpl::QueueCheckpointUpdate(
    unique_ptr<master::UpdateCDCCheckpointRequestPB> update) {
  CDCStreamTabletKey key{update->stream_id(), update->tablet_id()};
  std::lock_guard l(flush_lock_);
  if (!update->refresh_active_time_only()) {
    TakeOverDeferredCheckpointUpdateUnlocked(&update);
  }
  const bool was_empty = pending_checkpoint_updates_.empty();
  auto& pending = pending_checkpoint_updates_[std::move(key)];
  if (pending) {
    MergeCheckpointUpdate(*update, pending.get());
  } else {
    pending = std::move(update);
  }
  if (was_empty) {
    flush_cond_.notify_all();
  }
}

void CDCServiceImpl::DeferCheckpointUpdate(
    unique_ptr<master::UpdateCDCCheckpointRequestPB> update,
    const MonoTime& due) {
  CDCStreamTabletKey key{update->stream_id(), update->tablet_id()};
  std::lock_guard l(flush_lock_);
  auto& deferred = deferred_checkpoint_updates_[std::move(key)];
  if (deferred.update) {
    MergeCheckpointUpdate(*update, deferred.update.get());
    deferred.due = std::min(deferred.due, due);
  } else {
    deferred.update = std::move(update);
    deferred.due = due;
  }
  // The flusher may be waiting for a later due time, or for no time at all.
  flush_cond_.notify_all();
}

void CDCServiceImpl::TakeOverDeferredCheckpointUpdateUnlocked(
    unique_ptr<master::UpdateCDCCheckpointRequestPB>* update) {
  auto it = deferred_checkpoint_updates_.find(
      CDCStreamTabletKey{(*update)->stream_id(), (*update)->tablet_id()});
  if (it == deferred_checkpoint_updates_.end()) {
    return;
  }
  MergeCheckpointUpdate(**update, it->second.update.get());
  *update = std::move(it->second.update);
  deferred_checkpoint_updates_.erase(it);
}

MonoTime CDCServiceImpl::QueueDueDeferredCheckpointUpdatesUnlocked(const MonoTime& now) {
  MonoTime next_due = MonoTime::Max();
  for (auto it = deferred_checkpoint_updates_.begin();
       it != deferred_checkpoint_updates_.end();) {
    if (it->second.due > now) {
      next_due = std::min(next_due, it->second.due);
      ++it;
      continue;
    }
    // The deferred update is the newer one: merge it into what is queued.
    checkpoint_persists_->Increment();
    auto& pending = pending_checkpoint_updates_[it->first];
    if (pending) {
      MergeCheckpointUpdate(*it->second.update, pending.get());
    } else {
      pending = std::move(it->second.update);
    }
    it = deferred_checkpoint_updates_.erase(it);
  }
  return next_due;
}

bool CDCServiceImpl::QueuedCheckpointUpdateForTests(
    const string& stream_id,
    const string& tablet_id,
    master::UpdateCDCCheckpointRequestPB* update,
    bool take) {
  std::lock_guard l(flush_lock_);
  const auto it = pending_checkpoint_updates_.find(CDCStreamTabletKey{stream_id, tablet_id});
  if (it == pending_checkpoint_updates_.end()) {
    return false;
  }
  *update = *it->second;
  if (take) {
    pending_checkpoint_updates_.erase(it);
  }
  return true;
}

void CDCServiceImpl::PersistCheckpointNow(
    unique_ptr<master::UpdateCDCCheckpointRequestPB> update,
    const MonoTime& deadline) {
  // Take over what is queued or deferred for this (stream, tablet), so that
  // the older queued update is not sent after this one.
  {
    std::lock_guard l(flush_lock_);
    TakeOverDeferredCheckpointUpdateUnlocked(&update);
    auto it = pending_checkpoint_updates_.find(
        CDCStreamTabletKey{update->stream_id(), update->tablet_id()});
    if (it != pending_checkpoint_updates_.end()) {
      MergeCheckpointUpdate(*update, it->second.get());
      update = std::move(it->second);
      pending_checkpoint_updates_.erase(it);
    }
  }
  Status s;
  if (PREDICT_FALSE(FLAGS_cdc_inject_checkpoint_persist_failure)) {
    s = Status::ServiceUnavailable("injected checkpoint persist failure");
  } else {
    s = CallLeaderMaster(deadline, [&](master::MasterServiceProxy* proxy,
                                       rpc::RpcController* rpc) {
      master::UpdateCDCCheckpointResponsePB resp;
      RETURN_NOT_OK(proxy->UpdateCDCCheckpoint(*update, &resp, rpc));
      return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
    });
  }
  if (s.ok() || s.IsNotFound()) {
    // NotFound: the stream is gone, so there is nothing left to persist.
    return;
  }
  KLOG_EVERY_N_SECS(WARNING, 60)
      << "CDC: failed to persist checkpoint [stream=" << update->stream_id()
      << " tablet=" << update->tablet_id() << " op_index=" << update->op_index()
      << "] to master, will retry: " << s.ToString();
  checkpoint_persist_failures_->Increment();
  QueueCheckpointUpdate(std::move(update));
}

void CDCServiceImpl::CheckpointFlusherThread() {
  MonoTime next_flush = MonoTime::Now();
  while (true) {
    vector<unique_ptr<master::UpdateCDCCheckpointRequestPB>> updates;
    {
      std::unique_lock l(flush_lock_);
      // Wait for a queued update, or for a deferred one to become due.
      while (!flusher_stopped_) {
        const MonoTime now = MonoTime::Now();
        const MonoTime next_due = QueueDueDeferredCheckpointUpdatesUnlocked(now);
        if (!pending_checkpoint_updates_.empty()) {
          break;
        }
        if (next_due == MonoTime::Max()) {
          flush_cond_.wait(l);
        } else {
          flush_cond_.wait_for(l, std::chrono::nanoseconds((next_due - now).ToNanoseconds()));
        }
      }
      // Pace the batches; updates queued meanwhile are coalesced.
      while (!flusher_stopped_) {
        const int64_t wait_ns = (next_flush - MonoTime::Now()).ToNanoseconds();
        if (wait_ns <= 0) {
          break;
        }
        flush_cond_.wait_for(l, std::chrono::nanoseconds(wait_ns));
      }
      if (flusher_stopped_) {
        return;
      }
      QueueDueDeferredCheckpointUpdatesUnlocked(MonoTime::Now());
      updates.reserve(pending_checkpoint_updates_.size());
      for (auto& e : pending_checkpoint_updates_) {
        updates.emplace_back(std::move(e.second));
      }
      pending_checkpoint_updates_.clear();
    }
    next_flush = MonoTime::Now() +
        MonoDelta::FromMilliseconds(FLAGS_cdc_checkpoint_flush_interval_ms);
    FlushCheckpointUpdates(std::move(updates));
  }
}

void CDCServiceImpl::FlushCheckpointUpdates(
    vector<unique_ptr<master::UpdateCDCCheckpointRequestPB>> updates) {
  size_t sent = 0;
  Status s;
  while (sent < updates.size()) {
    const size_t end = std::min(sent + kMaxCheckpointUpdatesPerRpc, updates.size());
    master::UpdateCDCCheckpointsRequestPB req;
    for (size_t i = sent; i < end; ++i) {
      *req.add_updates() = *updates[i];
    }
    if (PREDICT_FALSE(FLAGS_cdc_inject_checkpoint_persist_failure)) {
      s = Status::ServiceUnavailable("injected checkpoint persist failure");
    } else {
      s = CallLeaderMaster(MonoTime(), [&](master::MasterServiceProxy* proxy,
                                           rpc::RpcController* rpc) {
        master::UpdateCDCCheckpointsResponsePB resp;
        CountDownLatch done(1);
        proxy->UpdateCDCCheckpointsAsync(req, &resp, rpc, [&done]() { done.CountDown(); });
        // Let StopCheckpointFlusher() cancel the call.
        {
          std::lock_guard l(flush_lock_);
          if (flusher_stopped_) {
            rpc->Cancel();
          } else {
            flusher_rpc_ = rpc;
          }
        }
        done.Wait();
        {
          std::lock_guard l(flush_lock_);
          flusher_rpc_ = nullptr;
        }
        RETURN_NOT_OK(rpc->status());
        return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
      });
    }
    if (!s.ok()) {
      break;
    }
    VLOG(2) << "CDC: persisted " << (end - sent) << " checkpoint updates";
    sent = end;
  }
  if (sent == updates.size()) {
    return;
  }

  // Queue the updates that were not sent again, under any newer update that was
  // queued for the same (stream, tablet) meanwhile. The flusher retries them
  // after --cdc_checkpoint_flush_interval_ms.
  KLOG_EVERY_N_SECS(WARNING, 60)
      << "CDC: failed to persist " << (updates.size() - sent)
      << " checkpoint updates to the master, will retry: " << s.ToString();
  checkpoint_persist_failures_->IncrementBy(static_cast<int64_t>(updates.size() - sent));
  std::lock_guard l(flush_lock_);
  for (size_t i = sent; i < updates.size(); ++i) {
    auto& u = updates[i];
    auto& pending = pending_checkpoint_updates_[
        CDCStreamTabletKey{u->stream_id(), u->tablet_id()}];
    if (pending) {
      MergeCheckpointUpdate(*pending, u.get());
    }
    pending = std::move(u);
  }
}

void CDCServiceImpl::StopCheckpointFlusher() {
  {
    std::lock_guard l(flush_lock_);
    if (flusher_stopped_) {
      return;
    }
    flusher_stopped_ = true;
    if (flusher_rpc_) {
      flusher_rpc_->Cancel();
    }
    flush_cond_.notify_all();
  }
  if (flusher_thread_) {
    flusher_thread_->Join();
  }
}

Status CDCServiceImpl::CallLeaderMaster(
    const MonoTime& deadline,
    const std::function<Status(master::MasterServiceProxy*, rpc::RpcController*)>& call,
    bool* deadline_cut) {
  if (deadline_cut) {
    *deadline_cut = false;
  }
  const auto& masters = server_->master_addresses();
  if (masters.empty()) {
    return Status::IllegalState("no master addresses configured");
  }
  const int num_masters = static_cast<int>(masters.size());
  const int first = master_hint_.load(std::memory_order_relaxed) % num_masters;
  Status s;
  // Whether the last attempt was shortened by 'deadline' and timed out.
  bool last_attempt_cut = false;
  for (int i = 0; i < num_masters; ++i) {
    const int idx = (first + i) % num_masters;
    const MonoDelta max_timeout =
        MonoDelta::FromMilliseconds(FLAGS_cdc_master_rpc_timeout_ms);
    MonoDelta timeout = max_timeout;
    if (deadline.Initialized()) {
      const MonoDelta remaining = deadline - MonoTime::Now();
      if (remaining.ToNanoseconds() <= 0) {
        if (deadline_cut) {
          *deadline_cut = true;
        }
        return s.ok() ? Status::TimedOut("deadline passed before a master was tried")
                      : s.CloneAndPrepend("deadline passed");
      }
      timeout = std::min(timeout, remaining);
    }
    const HostPort& hp = masters[idx];
    vector<Sockaddr> addrs;
    s = server_->dns_resolver()->ResolveAddresses(hp, &addrs);
    if (s.ok() && addrs.empty()) {
      s = Status::NetworkError("no addresses for master", hp.ToString());
    }
    if (s.ok()) {
      master::MasterServiceProxy proxy(server_->messenger(), addrs[0], hp.host());
      rpc::RpcController rpc;
      rpc.set_timeout(timeout);
      s = call(&proxy, &rpc);
      if (s.ok()) {
        master_hint_.store(idx, std::memory_order_relaxed);
        return s;
      }
      if (s.IsNotFound() || s.IsAborted()) {
        return s;
      }
    }
    last_attempt_cut = s.IsTimedOut() && timeout < max_timeout;
    // Start the next call with the next master, unless another call already
    // found a different one.
    int expected = idx;
    master_hint_.compare_exchange_strong(expected, (idx + 1) % num_masters,
                                         std::memory_order_relaxed);
  }
  if (deadline_cut) {
    *deadline_cut = last_attempt_cut;
  }
  return s;
}

// ---------------------------------------------------------------------------
// RecordActivity (internal)
// ---------------------------------------------------------------------------

void CDCServiceImpl::RecordActivity(const string& stream_id,
                                    const string& tablet_id,
                                    const string& consumer_addr) {
  // Two consumers that read one (stream, tablet) alternate: the calls come
  // from one address, then the other, then the first again. A consumer that
  // restarts (from a new address) changes it only once.
  static constexpr int64_t kDuplicateConsumerWindowMicros = 60 * 1000 * 1000;
  const int64_t now_micros = GetCurrentTimeMicros();
  const int64_t report_interval_micros =
      FLAGS_cdc_active_time_report_interval_ms > 0
          ? FLAGS_cdc_active_time_report_interval_ms * 1000 : 0;
  bool should_report = false;
  bool duplicate_consumer = false;
  string other_consumer_addr;
  int64_t up_to_op_index = -1;
  {
    std::lock_guard lock(lock_);
    CDCStreamTabletKey key{stream_id, tablet_id};
    auto it = stream_tablet_state_.find(key);
    if (it == stream_tablet_state_.end()) {
      return;
    }
    CDCStreamTabletState* state = it->second.get();
    state->last_active_time_micros.store(now_micros, std::memory_order_relaxed);
    if (state->consumer_addr != consumer_addr) {
      if (!state->consumer_addr.empty() && consumer_addr == state->prev_consumer_addr &&
          now_micros - state->consumer_addr_change_micros < kDuplicateConsumerWindowMicros) {
        duplicate_consumer = true;
        other_consumer_addr = state->consumer_addr;
      }
      state->prev_consumer_addr = std::move(state->consumer_addr);
      state->consumer_addr = consumer_addr;
      state->consumer_addr_change_micros = now_micros;
    }
    // The expiry the master reported, if any; the gauge falls back to the
    // (runtime) flag otherwise.
    const auto cit = stream_config_cache_.find(stream_id);
    state->idle_expiry_ms.store(
        cit != stream_config_cache_.end() ? cit->second.idle_expiry_ms : -1,
        std::memory_order_relaxed);
    // Throttle heartbeats to the master. The very first activity (report time 0)
    // is always reported so the master's clock-based active time is anchored.
    if (now_micros - state->last_active_report_micros >= report_interval_micros) {
      state->last_active_report_micros = now_micros;
      should_report = true;
      // During a snapshot the snapshot cursor tracks progress, not the WAL.
      if (!state->snapshot.active) {
        up_to_op_index = state->last_known_up_to_op_index.load(std::memory_order_relaxed);
      }
    }
  }
  if (duplicate_consumer) {
    duplicate_consumers_detected_->Increment();
    KLOG_EVERY_N_SECS(WARNING, 60) << Substitute(
        "CDC: two consumers ($0 and $1) are reading stream $2 on tablet $3; each "
        "may skip changes that only the other one applied",
        consumer_addr, other_consumer_addr, stream_id, tablet_id);
  }
  if (should_report) {
    activity_reports_->Increment();
    // op_index is ignored by the master for an active-time-only refresh.
    QueueCheckpointUpdate(MakeCheckpointUpdate(stream_id, tablet_id, /*op_index=*/0,
                                               /*history_safe_time_micros=*/0,
                                               /*refresh_active_time_only=*/true,
                                               /*snapshot_cursor=*/nullptr,
                                               up_to_op_index));
  }
}

// ---------------------------------------------------------------------------
// StreamIdleExpired (internal)
// ---------------------------------------------------------------------------

bool CDCServiceImpl::StreamIdleExpired(const string& stream_id,
                                       const string& tablet_id) const {
  int64_t expiry_ms = 0;
  int64_t last_active_micros = 0;
  {
    std::lock_guard lock(lock_);
    expiry_ms = StreamIdleExpiryMsUnlocked(stream_id);
    if (expiry_ms <= 0) {
      // Disambiguation disabled: always fall back to WAL_EXPIRED.
      return false;
    }
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

Status CDCServiceImpl::CheckTabletInStream(const string& stream_id,
                                           const TabletReplica& replica,
                                           const vector<string>& stream_table_ids) {
  const string& table_id = replica.tablet_metadata()->table_id();
  if (std::find(stream_table_ids.begin(), stream_table_ids.end(), table_id) !=
      stream_table_ids.end()) {
    return Status::OK();
  }
  return Status::InvalidArgument(Substitute(
      "tablet $0 of table $1 is not covered by CDC stream $2",
      replica.tablet_id(), table_id, stream_id));
}

Status CDCServiceImpl::GetOrFetchStreamConfig(const string& stream_id,
                                              const MonoTime& deadline,
                                              CDCStreamConfigPB* config,
                                              vector<string>* table_ids) {
  // Returns true, and sets 'result', if the cache answers the lookup: a cached
  // entry that is still within its TTL, or a failed fetch that is remembered.
  // Requires 'lock_'.
  const auto lookup_cache_unlocked = [&](const MonoTime& now, Status* result) {
    auto it = stream_config_cache_.find(stream_id);
    if (it != stream_config_cache_.end() && now < it->second.expiry) {
      *config = it->second.config;
      *table_ids = it->second.table_ids;
      *result = Status::OK();
      return true;
    }
    auto fit = stream_config_fetch_failures_.find(stream_id);
    if (fit != stream_config_fetch_failures_.end()) {
      if (now < fit->second.expiry) {
        *result = fit->second.status;
        return true;
      }
      stream_config_fetch_failures_.erase(fit);
    }
    return false;
  };

  // Fast path: answered from the cache.
  {
    std::lock_guard lock(lock_);
    Status result;
    if (lookup_cache_unlocked(MonoTime::Now(), &result)) {
      return result;
    }
  }

  // Either no entry, or the entry is stale and must be refetched from the
  // master so a stream reconfigure (e.g. record_type change) is picked up.
  //
  // Single-flight the fetch. Concurrent callers that all miss for the same
  // stream_id serialize on this per-stream mutex so only one master RPC is in
  // flight at a time. The mutex must be held across the blocking RPC, so it is a
  // std::timed_mutex, not lock_ (a spinlock). A waiter gives up at its deadline.
  // The last user of the mutex removes it from the map.
  std::shared_ptr<std::timed_mutex> fetch_lock = GetStreamConfigFetchLock(stream_id);
  SCOPED_CLEANUP({
    fetch_lock.reset();
    std::lock_guard lock(lock_);
    EraseIfUnused(&stream_config_fetch_locks_, stream_id);
  });
  std::unique_lock<std::timed_mutex> fetch_guard(*fetch_lock, std::defer_lock);
  if (deadline.Initialized()) {
    const MonoDelta remaining = deadline - MonoTime::Now();
    if (!fetch_guard.try_lock_for(std::chrono::nanoseconds(
            std::max<int64_t>(0, remaining.ToNanoseconds())))) {
      return Status::ServiceUnavailable(Substitute(
          "timed out waiting for the config of CDC stream $0 to be fetched from the "
          "master", stream_id));
    }
  } else {
    fetch_guard.lock();
  }

  // Re-check the cache now that we hold the fetch lock: while we waited, another
  // caller may have populated (or stale-refreshed) the entry, or failed to. Use a
  // fresh 'now' -- we may have blocked for a full master RPC round-trip. This is
  // what makes the waiters cheap: on the common path they return the
  // just-fetched config without issuing an RPC of their own.
  {
    std::lock_guard lock(lock_);
    Status result;
    if (lookup_cache_unlocked(MonoTime::Now(), &result)) {
      return result;
    }
  }

  const MonoTime new_expiry =
      MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_cdc_stream_config_cache_ttl_ms);

  // Test hook: widen the fetch window so concurrent cache misses reliably pile
  // up on the single-flight lock.
  if (PREDICT_FALSE(FLAGS_cdc_inject_latency_before_stream_config_fetch_ms > 0)) {
    SleepFor(MonoDelta::FromMilliseconds(
        FLAGS_cdc_inject_latency_before_stream_config_fetch_ms));
  }
  // A real fetch reaches the master loop. Counted once here (not per cache hit)
  // so a test can assert the single-flight collapse.
  stream_config_master_fetches_.fetch_add(1, std::memory_order_relaxed);

  master::GetCDCStreamInfoRequestPB req;
  req.set_stream_id(stream_id);
  master::GetCDCStreamInfoResponsePB resp;
  bool deadline_cut = false;
  Status s = CallLeaderMaster(deadline, [&](master::MasterServiceProxy* proxy,
                                            rpc::RpcController* rpc) {
    resp.Clear();
    RETURN_NOT_OK(proxy->GetCDCStreamInfo(req, &resp, rpc));
    return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
  }, &deadline_cut);
  // Remembers the failed fetch, unless that is disabled. Requires 'lock_'.
  const auto remember_failure_unlocked = [&](const Status& status) {
    const int32_t negative_ttl_ms = FLAGS_cdc_stream_config_negative_cache_ttl_ms;
    if (negative_ttl_ms <= 0) {
      return;
    }
    const MonoTime now = MonoTime::Now();
    if (stream_config_fetch_failures_.size() >= kMaxStreamConfigFetchFailures) {
      for (auto it = stream_config_fetch_failures_.begin();
           it != stream_config_fetch_failures_.end();) {
        it = now < it->second.expiry ? std::next(it) : stream_config_fetch_failures_.erase(it);
      }
      if (stream_config_fetch_failures_.size() >= kMaxStreamConfigFetchFailures) {
        stream_config_fetch_failures_.erase(stream_config_fetch_failures_.begin());
      }
    }
    stream_config_fetch_failures_[stream_id] =
        { status, now + MonoDelta::FromMilliseconds(negative_ttl_ms) };
  };

  if (s.ok()) {
    CDCStreamConfigPB result;
    s = StreamConfigFromMaster(resp.stream().config(), &result);
    if (!s.ok()) {
      // Not transient. Serving the stream with a cached config, or without the
      // options this server does not know, would stream the wrong records.
      s = s.CloneAndPrepend(Substitute("CDC stream $0", stream_id));
      std::lock_guard lock(lock_);
      stream_config_cache_.erase(stream_id);
      remember_failure_unlocked(s);
      return s;
    }
    vector<string> result_table_ids(resp.stream().table_ids().begin(),
                                    resp.stream().table_ids().end());
    {
      std::lock_guard lock(lock_);
      stream_config_cache_[stream_id] = {
          result, result_table_ids, new_expiry,
          resp.has_stream_expiry_ms() ? resp.stream_expiry_ms() : -1};
    }
    *config = std::move(result);
    *table_ids = std::move(result_table_ids);
    return Status::OK();
  }

  if (s.IsNotFound()) {
    // Authoritative: the leader master reports this stream does not exist
    // (deleted or never created). Only the leader reaches the stream-map
    // lookup -- followers respond NOT_LEADER before it -- so a NotFound here is
    // definitive. Evict any now-invalid cached entry and return NotFound so the
    // read path surfaces STREAM_NOT_FOUND rather than continuing to serve a stale
    // config (or falling through as CHANGE-mode WAL data). Unlike the
    // transient-failure path below, we do NOT serve stale here.
    std::lock_guard lock(lock_);
    stream_config_cache_.erase(stream_id);
    remember_failure_unlocked(s);
    return s;
  }

  // Refetch failed (master unreachable/erroring, or none configured). If we
  // still hold a now-stale entry, serve it rather than failing the consumer --
  // a transient master outage should not stall streaming -- and back off its
  // expiry so we do not attempt (and stall on) a refetch on every call. A
  // genuinely changed config is still picked up within one more TTL once the
  // master recovers. With no cached entry at all, remember the failure as
  // ServiceUnavailable: it may have been caused by one caller's deadline alone,
  // so the callers that find it remembered are shed as retryable rather than
  // served without knowing the stream's record type and snapshot mode. For the
  // same reason, this caller is shed too if its deadline cut the fetch short.
  // A fetch that tried every master in full returns NetworkError.
  std::lock_guard lock(lock_);
  auto it = stream_config_cache_.find(stream_id);
  if (it != stream_config_cache_.end()) {
    KLOG_EVERY_N_SECS(WARNING, 60)
        << "serving stale config for CDC stream " << stream_id
        << "; refetch from master failed: " << s.ToString();
    it->second.expiry = new_expiry;
    *config = it->second.config;
    *table_ids = it->second.table_ids;
    return Status::OK();
  }
  remember_failure_unlocked(Status::ServiceUnavailable(
      Substitute("the config of CDC stream $0 could not be fetched from the master "
                 "recently", stream_id), s.ToString()));
  if (deadline_cut) {
    return Status::ServiceUnavailable(
        Substitute("timed out fetching the config of CDC stream $0 from the master",
                   stream_id), s.ToString());
  }
  return Status::NetworkError(
      Substitute("could not fetch the config of CDC stream $0 from the master",
                 stream_id), s.ToString());
}

// ---------------------------------------------------------------------------
// FetchSnapshotCursor (internal) - cold-leader snapshot resume
// ---------------------------------------------------------------------------

Status CDCServiceImpl::FetchSnapshotCursor(const string& stream_id,
                                           const string& tablet_id,
                                           const MonoTime& deadline,
                                           bool* found,
                                           SnapshotCursorUpdate* out,
                                           int64_t* streaming_start_op_index) {
  *found = false;
  SnapshotCursorFetchHook hook;
  {
    std::lock_guard lock(lock_);
    hook = snapshot_cursor_fetch_hook_for_tests_;
  }
  if (hook) {
    return hook(stream_id, tablet_id, found, out, streaming_start_op_index);
  }
  // Direct, uncached read: the resume decision must see the latest durable cursor,
  // not a TTL-cached config snapshot.
  master::GetCDCStreamInfoRequestPB req;
  req.set_stream_id(stream_id);
  master::GetCDCStreamInfoResponsePB resp;
  RETURN_NOT_OK(CallLeaderMaster(deadline, [&](master::MasterServiceProxy* proxy,
                                               rpc::RpcController* rpc) {
    resp.Clear();
    RETURN_NOT_OK(proxy->GetCDCStreamInfo(req, &resp, rpc));
    return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
  }));
  for (const auto& cur : resp.stream().tablet_snapshot_cursors()) {
    if (cur.tablet_id() == tablet_id && cur.snapshot_active() &&
        cur.has_snapshot_snap_ts()) {
      out->snap_ts = cur.snapshot_snap_ts();
      out->resume_key = cur.snapshot_resume_key();
      out->active = true;
      *streaming_start_op_index = cur.streaming_start_op_index();
      *found = true;
      break;
    }
  }
  return Status::OK();
}

} // namespace cdc
} // namespace kudu
