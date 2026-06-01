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
#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.service.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
class Message;
} // namespace protobuf
} // namespace google

namespace kudu {

class Counter;
class MemTracker;
class Schema;
class Thread;

namespace master {
class CDCStreamConfigPB;
class MasterServiceProxy;
class UpdateCDCCheckpointRequestPB;
} // namespace master

namespace rpc {
class RpcContext;
class RpcController;
} // namespace rpc

namespace tablet {
class Tablet;
class TabletReplica;
} // namespace tablet

namespace tserver {
class TabletServer;
} // namespace tserver

namespace cdc {

// The published retry contract of a CDCErrorPB::Code (see CDCErrorPB in
// cdc.proto), which the server sends as CDCErrorPB.is_retryable and
// needs_resnapshot. Both false means fatal (operator action); the two are
// mutually exclusive.
void ClassifyCDCErrorCode(CDCErrorPB::Code code, bool* is_retryable, bool* needs_resnapshot);

// Converts the configuration of a stream, as the master returns it, into the
// one this server applies. Returns NotSupported if it has a field or an enum
// value that this server does not know: the stream needs a newer tablet
// server, and serving it as if the option were not set would stream the wrong
// records.
Status StreamConfigFromMaster(const master::CDCStreamConfigPB& master_config,
                              CDCStreamConfigPB* config);

// A unit of change records that a GetChanges response carries whole or not at
// all: the records of one op, or the BEGIN, rows and COMMIT of a transaction.
struct ChangesEmitGroupInfo {
  // The WAL index at which the records become visible: the FINALIZE_COMMIT of
  // a transaction, otherwise the op's own index.
  int64_t effective_index;
  // The lowest WAL index the records come from: the first write of a
  // transaction, otherwise 'effective_index'. A checkpoint at or past it makes
  // the next call skip records of the group.
  int64_t min_index;
  int64_t num_records;
  // The size the records add to a serialized response.
  int64_t num_bytes;
};

struct ChangesEmitLimits {
  // At most this many records, unless the first group alone has more; 0 means
  // no limit.
  int32_t max_records = 0;
  // At most this many bytes of records, unless the first group alone has more.
  int64_t max_bytes = 0;
  // Never more than this many bytes of records: a response must fit in one
  // RPC message.
  int64_t hard_max_bytes = 0;
};

struct ChangesEmitPlan {
  // The response carries groups [0, num_groups).
  size_t num_groups = 0;
  int64_t checkpoint_op_index = -1;
  // Groups that could be emitted now were left out, so the consumer can call
  // again right away.
  bool truncated = false;
  // Groups [0, num_groups) cannot be emitted without one response exceeding
  // 'hard_max_bytes', and none of them can be emitted alone.
  bool too_large = false;
};

// Plans which of 'groups' a GetChanges response carries, and its checkpoint.
// 'groups' are in increasing effective_index order. 'open_min_index' is the
// first write of the oldest transaction that is still open at the end of the
// read window (int64_t max if none): no group visible at or after it is
// emitted. 'end_index' is the checkpoint when every group is emitted and
// nothing holds the checkpoint back. 'base_records' and 'base_bytes' are what
// the response already carries.
//
// A response ends only between groups, and its checkpoint is below the
// min_index of every group it leaves out, so the next call reads all of each
// such group, and at or above the effective_index of every group it carries,
// so the next call emits none of them again. Where the limits would end the
// response at a point that allows no such checkpoint (an op inside the span of
// a transaction that commits later), the response ends earlier or, if only the
// first group fits, later.
ChangesEmitPlan PlanChangesEmission(const std::vector<ChangesEmitGroupInfo>& groups,
                                    int64_t open_min_index,
                                    int64_t end_index,
                                    int64_t base_records,
                                    int64_t base_bytes,
                                    const ChangesEmitLimits& limits);

// Server-driven consistent-snapshot session state for a (stream, tablet) pair.
// Active during the multi-RPC paginated snapshot scan that bootstraps a consumer
// from the table's current contents before WAL streaming begins.
struct CDCSnapshotState {
  // True while a snapshot is in progress for this (stream, tablet): from its
  // establish until the client acknowledges its last page.
  bool active = false;

  // True once a page completed the scan. The client acknowledges that page by
  // streaming the WAL, or checkpointing, from the streaming start point on;
  // until then the session stays active, so that the page can be served again
  // if its response was lost, and a restart of the snapshot reads at the same
  // snap_ts.
  bool done = false;

  // The hybrid-clock timestamp at which the snapshot is read. Every READ record
  // reflects the tablet state at this timestamp; held constant across all pages.
  Timestamp snap_ts;

  // The op index after which the consumer streams the WAL once the scan
  // completes: the committed op index when the snapshot began, lowered below
  // the first op of every transaction the snapshot does not see.
  int64_t streaming_start_op_index = 0;

  // The tablet schema every page reads with, and its version, pinned when the
  // session begins so that a concurrent ALTER does not change the columns of
  // the rows from one page to the next.
  std::shared_ptr<const Schema> schema;
  uint32_t schema_version = 0;

  // Paging. 'page_start_key' is the position the last page was scanned after
  // (the serialized encoded primary key the client sent, none for the start of
  // the table), and 'page_end_key' the key of the last row it emitted. A key
  // can be empty (the empty STRING or BINARY key), so "none" is not the empty
  // key. A
  // request continues after 'page_end_key' if the client got the last page,
  // or serves the last page again if the client sends 'page_start_key' (its
  // response was lost); any other key is refused. 'page_start_key' is thus
  // the newest position the client has acknowledged, and the only one that
  // is persisted as the durable snapshot cursor. 'page_request_key' is the key
  // the client sent for the last page (none on a start), which also serves
  // it again: after a cold resume the page was scanned after the durable
  // cursor, which may be behind the client's key.
  std::optional<std::string> page_start_key;
  std::optional<std::string> page_end_key;
  std::optional<std::string> page_request_key;
};

// One durable snapshot-progress cursor update, persisted with a checkpoint so a
// resumable snapshot can persist (snap_ts, resume_key) to the master.
struct SnapshotCursorUpdate {
  // Full hybrid-clock value (Timestamp::value()) of the snapshot. Orders snapshot
  // generations for the master's monotonicity rule.
  uint64_t snap_ts = 0;
  // Serialized encoded primary key of the newest position the client
  // acknowledged (empty at the start of the table). Empty is also the key of
  // the empty STRING or BINARY key: a resume from an empty cursor starts from
  // the start of the table, which repeats at most that row.
  std::string resume_key;
  // True while the snapshot is in progress; false when it completes.
  bool active = false;
};

// The result of reading the durable snapshot cursor of a (stream, tablet) from
// the master (see CDCServiceImpl::FetchSnapshotCursor()). GetChanges reads it
// before a snapshot page that may need it, so that the read does not hold a
// snapshot scan slot.
struct SnapshotCursorFetch {
  // Whether the cursor was read; the fields below are set only if it was.
  bool fetched = false;
  // The result of the read. 'found' and the fields after it are set only if
  // it is OK.
  Status status;
  // Whether the master has an active (unfinished) snapshot cursor.
  bool found = false;
  SnapshotCursorUpdate cursor;
  int64_t streaming_start_op_index = 0;
};

// State tracked per (stream_id, tablet_id) pair for a single CDC consumer
// session on the tablet's leader: the last known checkpoint index, the history
// floor, the snapshot session and the per-session metrics. A session holds no
// WAL anchor: the master's retention barrier retains the WAL its stream needs.
struct CDCStreamTabletState {
  CDCStreamTabletState() = default;

  // The last op_index successfully checkpointed by the consumer via the
  // Checkpoint RPC. Atomic so the per-session FunctionGauges (ops_behind,
  // bootstrap_required) can read it without holding lock_.
  std::atomic<int64_t> checkpoint_op_index{0};

  // For FULL-mode streams: the minimum op timestamp (raw Timestamp value) of
  // each batch served, by the batch's from_op_index. Only the entry that covers
  // 'history_resume_op_index' and later ones are kept, so a consumer that reads
  // ahead of its checkpoint does not unprotect the history it needs when it
  // resumes from that checkpoint. Bounded by dropping the highest entries,
  // which only makes the floor more conservative.
  std::map<int64_t, uint64_t> history_floor_by_from_index;

  // The lowest op index the consumer may resume from: its last checkpoint,
  // lowered by any read from an earlier index since then (a consumer that
  // restarted from an older checkpoint). -1 until the first Checkpoint or FULL
  // read, which means checkpoint_op_index.
  int64_t history_resume_op_index = -1;

  // The MVCC history floor this session holds on the tablet (raw Timestamp
  // value, 0 if none): the minimum of snapshot.snap_ts while a snapshot is
  // active and of 'history_floor_by_from_index'. Reported to the master on
  // Checkpoint so the master can fan the history barrier out to followers.
  // The session itself holds only part of it on the tablet (see
  // CDCServiceImpl::HeldHistoryFloorUnlocked()).
  // See CDCServiceImpl::PublishHistoryFloorUnlocked().
  uint64_t history_floor = 0;

  // The stream's idle expiry (milliseconds) as of the last activity, or -1 if
  // not known yet, in which case --cdc_stream_idle_expiry_ms applies. Atomic
  // for the time-to-expiry gauge; written under lock_.
  std::atomic<int64_t> idle_expiry_ms{-1};

  // Wall-clock time (microseconds since the Unix epoch) of the last consumer
  // activity (GetChanges/Checkpoint) observed for this session. Drives the
  // expiry-budget metric and throttles activity reports to the master. Atomic
  // because the per-session active-age FunctionGauge (see cdc_service.cc) reads
  // it without holding lock_; all writers hold lock_.
  std::atomic<int64_t> last_active_time_micros{0};

  // Wall-clock time (microseconds) of the last activity report sent to the
  // master. Used to throttle active-time refreshes (see
  // --cdc_active_time_report_interval_ms).
  int64_t last_active_report_micros = 0;

  // Wall-clock time (microseconds) of the last checkpoint persisted to the
  // master, or of the next one if the throttle deferred a checkpoint to it.
  // Used to throttle durable checkpoint writes (see
  // --cdc_checkpoint_persist_interval_ms).
  int64_t last_checkpoint_persist_micros = 0;

  // Wall-clock time (microseconds) of the last snapshot-cursor persist to the
  // master, throttled by --cdc_snapshot_cursor_persist_interval_ms. Bounds how
  // many rows a resume re-scans (at most one interval's worth). Establish and
  // snapshot-done persists ignore this throttle.
  int64_t last_snapshot_cursor_persist_micros = 0;

  // The remote address of the last consumer that read from this session, and
  // of the one before it, with the time the address last changed, to detect
  // two consumers reading the same (stream, tablet). Guarded by lock_.
  std::string consumer_addr;
  std::string prev_consumer_addr;
  int64_t consumer_addr_change_micros = 0;

  // Physical-time component (microseconds) of the newest record emitted to the
  // consumer for this session. Used to compute the consumer sent-lag metric.
  // Atomic for the same reason as last_active_time_micros above.
  std::atomic<uint64_t> last_sent_record_phys_micros{0};

  // Whether the most recent GetChanges for this session left nothing to read:
  // its checkpoint reached the tablet's last committed op index and it did not
  // report more records. The lag gauges report 0 while this is set, so an idle
  // tablet with a caught-up consumer does not look like a lagging one.
  std::atomic<bool> caught_up{false};

  // The tablet's last committed op index as of the most recent GetChanges call
  // for this session. Used to compute ops_behind = up_to - checkpoint.
  // Atomic for the same reason as last_active_time_micros above. -1 = not yet set.
  std::atomic<int64_t> last_known_up_to_op_index{-1};

  // The log reader's minimum retained replicate index as of the most recent
  // GetChanges call. Used to compute bootstrap_required: if the consumer's
  // checkpoint_op_index is below this value, the WAL it needs may have been GC'd.
  // Atomic for the same reason as last_active_time_micros above. -1 = not yet set.
  std::atomic<int64_t> last_known_min_replicate_index{-1};

  // Physical timestamp (microseconds) of the last operation the consumer has
  // fully APPLIED and reported back via CheckpointRequestPB.last_applied_phystime_micros.
  // 0 means the consumer has never reported an applied time (e.g. an old consumer
  // that does not set the field, or no Checkpoint has been received yet). Atomic
  // so the per-session cdc_stream_committed_lag_micros FunctionGauge can read it
  // without holding lock_, matching the pattern of last_sent_record_phys_micros.
  std::atomic<int64_t> last_applied_phystime_micros{0};

  // Snapshot scan session state.
  CDCSnapshotState snapshot;

  // Dedicated metric entity for this (stream, tablet) session, exposing the
  // per-stream lag/age FunctionGauges attributed by stream_id and tablet_id so
  // operators can attribute CDC lag to a specific stream. Instantiated when the
  // session is first tracked; null in contexts without a metric registry.
  scoped_refptr<MetricEntity> metric_entity;

  // Detaches the per-session FunctionGauges above (resetting them to a constant)
  // when this state is destroyed, so their callbacks stop reading this struct's
  // fields first. Declared LAST so it is destroyed FIRST. The callbacks touch
  // only the atomic fields above -- never lock_ -- so detaching (which runs
  // under an erase holding lock_) cannot deadlock against a concurrent scrape.
  FunctionGaugeDetacher metric_detacher;
};

// Key for the per-session state map.
struct CDCStreamTabletKey {
  std::string stream_id;
  std::string tablet_id;

  bool operator==(const CDCStreamTabletKey& o) const {
    return stream_id == o.stream_id && tablet_id == o.tablet_id;
  }
};

struct CDCStreamTabletKeyHash {
  // The combine wraps around on purpose: unsigned overflow is defined, but
  // -fsanitize=integer flags it, and UBSAN builds do not recover from it.
  ATTRIBUTE_NO_SANITIZE_INTEGER
  size_t operator()(const CDCStreamTabletKey& k) const {
    size_t h = std::hash<std::string>{}(k.stream_id);
    h ^= std::hash<std::string>{}(k.tablet_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
  }
};

typedef std::unordered_map<CDCStreamTabletKey,
                           std::unique_ptr<CDCStreamTabletState>,
                           CDCStreamTabletKeyHash> CDCStreamTabletStateMap;

// CDCServiceImpl serves the CDCService RPC interface on a tablet server.
//
// It exposes two RPCs:
//   GetChanges: returns a batch of CDCRecordPB entries from the WAL
//   Checkpoint: durably persists consumer progress
//
// Design notes:
//   - Reads are sourced from the tablet's LogCache (hot) then LogReader (cold).
//   - WAL retention: the master computes a retention barrier per tablet from
//     the durable consumer checkpoints of its streams and pushes it to every
//     replica (SetRetentionBarrier), which persists it in the tablet metadata.
//     It is the only long-lived CDC retention. GetChanges anchors the WAL only
//     while it reads it.
//   - Only the tablet leader serves CDC (followers return TABLET_NOT_LEADER).
//   - Stream metadata (existence, config) is validated against the master on
//     first access and cached locally.
class CDCServiceImpl : public CDCServiceIf {
 public:
  // 'enforce_access_control' tells whether to require an authz token with
  // the SCAN privilege on the table of the tablet (the server's
  // --tserver_enforce_access_control).
  CDCServiceImpl(tserver::TabletServer* server, std::function<bool()> enforce_access_control);
  ~CDCServiceImpl() override;

  // Starts the thread that sends checkpoint updates to the master. Called once,
  // before the service is registered.
  Status Start();

  // Ends every consumer session this server holds. Afterwards no session can be
  // created again, and SetRetentionBarrier() returns ServiceUnavailable. The
  // tablet server calls this after unregistering its RPC services and before
  // shutting down the tablet manager. Idempotent.
  void Shutdown() override;

  // RPC handlers, called by the generated service stub.
  void GetChanges(const GetChangesRequestPB* req,
                  GetChangesResponsePB* resp,
                  rpc::RpcContext* context) override;

  void Checkpoint(const CheckpointRequestPB* req,
                  CheckpointResponsePB* resp,
                  rpc::RpcContext* context) override;

  // Authorization hook referenced by cdc.proto's default_authz_method. Requires
  // the caller to be an authenticated client, service, or super user.
  bool AuthorizeClientOrServiceUser(const google::protobuf::Message* req,
                                    google::protobuf::Message* resp,
                                    rpc::RpcContext* context);

  // Sets (min_retained_op_index >= 0) or releases (< 0) this replica's CDC WAL
  // retention barrier, which the tablet metadata persists and
  // TabletReplica::GetRetentionIndexes() applies. Invoked on every replica of a
  // CDC tablet by the leader master's periodic maintenance (via the
  // UpdateCDCRetentionBarrier RPC), so retention survives leader changes and
  // independent follower log GC.
  //
  // 'barrier_seq' is the master's monotonic sequence for this update. Barrier
  // RPCs are async, best-effort and unordered, so a stale SET could otherwise
  // land after a newer RELEASE (leaking WAL/history retention) or vice versa.
  // The replica persists the sequence of the last applied update with the
  // barrier and, for --cdc_barrier_reorder_window_secs after that update was
  // applied (or after this server started), ignores any request whose sequence
  // is lower (last-writer-wins), setting '*superseded' (if not null) to true.
  // 0 means unsequenced and is always applied.
  //
  // If 'release_consumer_stream_id' is non-empty, the consumer session of that
  // stream on this tablet is also ended, and no session of the stream can be
  // created on this server any more. This is driven by the master on stream
  // deletion. The seq gate does NOT guard the session release -- a stream
  // delete is terminal, so the release is always applied regardless of
  // 'barrier_seq'.
  //
  // If 'skip_barrier_update' is true, the aggregate barrier is left untouched
  // ('min_retained_op_index' / 'history_safe_time_micros' are ignored) and only
  // the session is released. Used when the deleted stream's tablet still
  // has other live streams, so releasing the aggregate barrier here would
  // briefly drop retention the survivors need before the master's next
  // maintenance pass recomputes it.
  //
  // If 'tablet_dropped' is true, the tablet's range partition was dropped and
  // the master keeps the tablet for CDC streams to read the rest of its
  // changes: the replica rejects writes from now on (persisted), whatever the
  // other arguments are.
  Status SetRetentionBarrier(const std::string& tablet_id,
                             int64_t min_retained_op_index,
                             uint64_t history_safe_time_micros = 0,
                             int64_t barrier_seq = 0,
                             const std::string& release_consumer_stream_id = "",
                             bool skip_barrier_update = false,
                             bool* superseded = nullptr,
                             bool tablet_dropped = false);

  // Ends every consumer session on 'tablet_id', releasing their history floors
  // and metrics. Called by the tablet manager when it stops a replica of the
  // tablet to delete or replace it. Safe to call for a tablet with no sessions.
  void EndSessionsForTablet(const std::string& tablet_id);

  // Ends the consumer sessions on 'tablet_id' if this replica is not the
  // tablet's leader, releasing their history floors and metrics.
  // Only the leader serves CDC; the master's retention barrier keeps the WAL and
  // history the stream's durable checkpoints need on every replica. Called by
  // the tablet manager whenever the tablet's consensus state changed, e.g. when
  // this replica learned of a new leader.
  void EndSessionsIfNotLeader(const std::string& tablet_id);

  // Test-only: seed the per-stream config cache so tests can exercise FULL-mode
  // and snapshot behavior without a live master issuing GetCDCStreamInfo. The
  // seeded entry never expires, so it is served for the life of the test
  // regardless of --cdc_stream_config_cache_ttl_ms.
  void SetStreamConfigForTests(const std::string& stream_id,
                               const CDCStreamConfigPB& config,
                               std::vector<std::string> table_ids,
                               int64_t idle_expiry_ms = -1) {
    std::lock_guard<simple_spinlock> lock(lock_);
    stream_config_cache_[stream_id] =
        {config, std::move(table_ids), MonoTime::Max(), idle_expiry_ms};
  }

  // Test-only: seed the per-stream config cache with a finite freshness
  // deadline 'ttl' from now, mimicking a real master fetch. Used to exercise
  // TTL-based staleness/eviction.
  void SetStreamConfigForTestsWithTtl(const std::string& stream_id,
                                      const CDCStreamConfigPB& config,
                                      std::vector<std::string> table_ids,
                                      const MonoDelta& ttl) {
    std::lock_guard<simple_spinlock> lock(lock_);
    stream_config_cache_[stream_id] = {config, std::move(table_ids), MonoTime::Now() + ttl};
  }

  // Test-only: true iff 'stream_id' has a cache entry that is still fresh --
  // i.e. GetOrFetchStreamConfig would serve it without attempting a refetch.
  // Mirrors the production freshness check exactly.
  bool IsStreamConfigFreshForTests(const std::string& stream_id) const {
    std::lock_guard<simple_spinlock> lock(lock_);
    auto it = stream_config_cache_.find(stream_id);
    return it != stream_config_cache_.end() && MonoTime::Now() < it->second.expiry;
  }

  // Test-only: mark every in-progress snapshot session inactive, simulating the
  // loss of in-memory session state that a newly elected leader would see after
  // a leader change mid-snapshot.
  void ClearSnapshotSessionsForTests();

  // Test-only: 'hook' runs in each snapshot page after its scan and before the
  // page re-checks that the history at snap_ts is still intact. Set it before
  // issuing the page.
  void SetSnapshotPageScannedHookForTests(std::function<void()> hook) {
    snapshot_page_scanned_hook_for_tests_ = std::move(hook);
  }

  // Test-only: 'hook' runs in a snapshot establish after it captured the WAL
  // streaming start point and before it picks the snapshot timestamp. Set it
  // before issuing the page.
  void SetSnapshotEstablishHookForTests(std::function<void()> hook) {
    snapshot_establish_hook_for_tests_ = std::move(hook);
  }

  // Test-only: 'hook' runs in a snapshot establish, or in the rebuild of a
  // session from the durable cursor, right after the session was tracked and
  // before the snapshot is recorded in it. Set it before issuing the page.
  void SetSnapshotSessionTrackedHookForTests(std::function<void()> hook) {
    snapshot_session_tracked_hook_for_tests_ = std::move(hook);
  }

  // Test-only: if set, FetchSnapshotCursor() calls 'hook' instead of asking the
  // master for the durable snapshot cursor. For fixtures that have no master.
  typedef std::function<Status(const std::string& stream_id,
                               const std::string& tablet_id,
                               bool* found,
                               SnapshotCursorUpdate* cursor,
                               int64_t* streaming_start_op_index)> SnapshotCursorFetchHook;
  void SetSnapshotCursorFetchHookForTests(SnapshotCursorFetchHook hook) {
    std::lock_guard<simple_spinlock> lock(lock_);
    snapshot_cursor_fetch_hook_for_tests_ = std::move(hook);
  }

  // Test-only: true iff a snapshot of 'tablet_id' is in progress (active) in
  // the session of ('stream_id', 'tablet_id'). If so and 'snap_ts' is not
  // null, sets it to the snapshot's timestamp.
  bool HasActiveSnapshotForTests(const std::string& stream_id,
                                 const std::string& tablet_id,
                                 Timestamp* snap_ts = nullptr) const {
    std::lock_guard<simple_spinlock> lock(lock_);
    const auto it = stream_tablet_state_.find(CDCStreamTabletKey{stream_id, tablet_id});
    if (it == stream_tablet_state_.end() || !it->second->snapshot.active) {
      return false;
    }
    if (snap_ts) {
      *snap_ts = it->second->snapshot.snap_ts;
    }
    return true;
  }

  // Test-only: number of times a fresh snapshot session has been established
  // (the establish path ran and set snapshot.active from false). Two concurrent
  // is_snapshot_start calls for one (stream, tablet) must establish exactly
  // once; this counter lets a test assert that.
  int64_t SnapshotSessionsEstablishedForTests() const {
    return snapshot_sessions_established_.load(std::memory_order_relaxed);
  }

  // Test-only: number of times a cold leader re-established (resumed) an
  // in-progress snapshot session from the durable master cursor, and the number
  // of times it returned SNAPSHOT_SESSION_LOST because no resumable cursor existed
  // (absent/inactive) or its snap_ts history was already gone.
  int64_t SnapshotSessionsResumedForTests() const {
    return snapshot_sessions_resumed_.load(std::memory_order_relaxed);
  }
  int64_t SnapshotSessionsLostForTests() const {
    return snapshot_sessions_lost_.load(std::memory_order_relaxed);
  }

  // Count of actual master GetCDCStreamInfo RPCs issued by GetOrFetchStreamConfig
  // (one per real fetch attempt, not per cache hit). Lets a test assert that N
  // concurrent cache misses for one stream collapse to a single fetch.
  int64_t StreamConfigMasterFetchesForTests() const {
    return stream_config_master_fetches_.load(std::memory_order_relaxed);
  }

  // Test-only: the number of per-stream config-fetch locks and per-session
  // snapshot-page locks currently allocated.
  size_t StreamConfigFetchLockCountForTests() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return stream_config_fetch_locks_.size();
  }
  size_t SnapshotPageLockCountForTests() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return snapshot_page_locks_.size();
  }

  // Test-only: true iff the replica of 'tablet_id' on this server has a CDC
  // retention barrier set. When true and 'min_retained_op_index' is non-null,
  // stores the barrier's op index.
  bool RetentionBarrierForTests(const std::string& tablet_id,
                                int64_t* min_retained_op_index = nullptr) const;

  // Test-only: copies the checkpoint update queued for the master for
  // ('stream_id', 'tablet_id') into '*update' and returns true, or returns
  // false if none is queued. A deferred update is not queued until it is due.
  // If 'take' is true, also removes the update from the queue.
  bool QueuedCheckpointUpdateForTests(const std::string& stream_id,
                                      const std::string& tablet_id,
                                      master::UpdateCDCCheckpointRequestPB* update,
                                      bool take = false);

  // Test-only: true iff a consumer session is currently tracked for
  // ('stream_id', 'tablet_id').
  bool HasSessionForTests(const std::string& stream_id,
                          const std::string& tablet_id) const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return stream_tablet_state_.find(CDCStreamTabletKey{stream_id, tablet_id}) !=
           stream_tablet_state_.end();
  }

  // Lag/retention gauge values, computed over the active (stream, tablet)
  // sessions. Backing the identically-named FunctionGauges; also exposed for
  // tests. MaxSentLagMicros/MaxActiveAgeMicros/MaxCommittedLagMicros return microseconds.
  int64_t MaxSentLagMicros() const;
  int64_t MaxActiveAgeMicros() const;
  uint64_t ActiveStreamCount() const;
  // The number of checkpoint updates queued for the master.
  uint64_t PendingCheckpointUpdates() const;
  int64_t MaxOpsBehind() const;
  int64_t BootstrapRequiredStreamCount() const;
  // The max consumer-applied (end-to-end) committed lag
  // across all active sessions. Returns -1 when NO session has ever reported an
  // applied time (last_applied_phystime_micros == 0 for all sessions). Sessions
  // that have never reported are skipped (treated as not-yet-known) so they do
  // not drag the maximum to -1 when at least one session IS reporting.
  int64_t MaxCommittedLagMicros() const;

  // Test-only: current byte consumption of the CDC scan MemTracker
  // (scan_mem_tracker_). Used by the true-up tests to assert
  // the reservation returns to its pre-call baseline after a snapshot or
  // FULL-mode GetChanges. Defined in cdc_service.cc (MemTracker is only
  // forward-declared here).
  int64_t ScanMemTrackerConsumptionForTests() const;

  // Test-only: current byte consumption of the CDC WAL-read MemTracker
  // (wal_read_mem_tracker_). Used by the true-up tests to assert
  // the reservation returns to its pre-call baseline after a CHANGE-mode
  // GetChanges. Defined in cdc_service.cc (MemTracker is only forward-declared
  // here).
  int64_t WalReadMemTrackerConsumptionForTests() const;

 private:
  // Reads up to max_bytes worth of CDCRecordPB entries from the tablet's WAL,
  // starting immediately after from_op_index. Populates resp->records and
  // resp->checkpoint_op_index.
  Status ReadChanges(const std::string& stream_id,
                     const std::string& tablet_id,
                     int64_t from_op_index,
                     int64_t max_bytes,
                     bool need_schema_info,
                     // The schema version the consumer declared it is decoding
                     // against, or -1 if it declared none. If >= 0 and older
                     // than the tablet's current schema version, ReadChanges
                     // returns SCHEMA_VERSION_MISMATCH (see cdc.proto).
                     int64_t consumer_schema_version,
                     // Whether the request is read-only: it then does not
                     // complete a snapshot (see GetChangesRequestPB.read_only).
                     bool read_only,
                     // Whether the stream is FULL (see GetChanges()).
                     bool is_full_mode,
                     const MonoTime& deadline,
                     GetChangesResponsePB* resp);

  // Executes one page of the server-driven consistent snapshot for
  // (stream_id, tablet_id). On is_start, restarts the snapshot in progress
  // from the start of the table at its snapshot timestamp: the one this server
  // holds, or else the one of the master's durable cursor, whose session it
  // rebuilds (SNAPSHOT_SESSION_LOST if the history at that timestamp is gone).
  // If there is none, or with 'new_generation', it captures a new snapshot
  // timestamp and streaming start op-index and pins tablet history. Otherwise
  // it continues after, or serves again, the last page (see
  // CDCSnapshotState), rebuilding the session from the durable cursor if this
  // server has none. 'req_resume_key' is the client's resume key, or null if
  // it sent none. 'cursor_fetch' is the durable cursor if GetChanges already
  // read it (see SnapshotCursorFetch); it is read here if it is needed and was
  // not. Emits READ records into resp and sets the snapshot response fields
  // (snapshot_done, snapshot_resume_key, snapshot_streaming_start_op_index).
  // 'is_full_mode' tells whether the stream is FULL, which keeps the history
  // floor at snap_ts after the snapshot completes.
  Status ReadSnapshot(const std::string& stream_id,
                      const std::string& tablet_id,
                      bool is_full_mode,
                      bool is_start,
                      bool new_generation,
                      const std::string* req_resume_key,
                      const SnapshotCursorFetch& cursor_fetch,
                      int64_t max_bytes,
                      const MonoTime& deadline,
                      GetChangesResponsePB* resp);

  // Completes the snapshot of (stream_id, tablet_id) if its last page has been
  // served and 'op_index', the position the client streams or checkpoints
  // from, is at or past its streaming start point: the client got that page.
  // Releases the session's pin on the history at snap_ts on 'tablet' (except
  // for a FULL stream, see ReadSnapshot()) and queues the completed cursor for
  // the master.
  void CompleteSnapshotIfAcknowledged(const std::string& stream_id,
                                      const std::string& tablet_id,
                                      int64_t op_index,
                                      tablet::Tablet* tablet);

  // Returns the name under which the sessions of 'stream_id' hold their MVCC
  // history floor on a tablet (see tablet::Tablet::SetCDCHistoryFloorHolder()).
  static std::string HistoryFloorHolder(const std::string& stream_id);

  // Recomputes 'state''s history floor (see CDCStreamTabletState::history_floor)
  // after its checkpoint, served batches or snapshot changed, and publishes it to
  // 'tablet' (if non-null) as the floor held by 'stream_id'. Requires 'lock_'.
  static void PublishHistoryFloorUnlocked(const std::string& stream_id,
                                          CDCStreamTabletState* state,
                                          tablet::Tablet* tablet);

  // Returns the part of 'state''s history floor that the session holds on
  // 'tablet' (raw Timestamp value, 0 if none): the snapshot pin while a
  // snapshot is active and, while 'tablet' has a retention barrier, the floors
  // of the batches served that are not older than
  // --cdc_session_history_floor_max_age_secs. Requires 'lock_'.
  static uint64_t HeldHistoryFloorUnlocked(const CDCStreamTabletState& state,
                                           const tablet::Tablet& tablet);

  // The idle time after which a session of 'stream_id' counts as expired: the
  // stream's expiry as reported by the master with its config, or else
  // --cdc_stream_idle_expiry_ms. 0 disables expiry. Requires 'lock_'.
  int64_t StreamIdleExpiryMsUnlocked(const std::string& stream_id) const;

  // Called on every master barrier SET for 'tablet_id': drops the history
  // floors held by the local sessions on 'tablet' that no longer need them
  // beyond the master's floor, i.e. all of them when this replica is not the
  // leader ('is_leader' false), and otherwise those idle for longer than
  // their stream's idle expiry (see StreamIdleExpiryMsUnlocked()). The other
  // sessions on a leader drop the batch floors that have aged out (see
  // HeldHistoryFloorUnlocked()). A dropped
  // session pins its floor again on its next GetChanges, Checkpoint or snapshot
  // page; until then the master's floor (fed by the session's checkpoints)
  // covers its resume point. Requires 'lock_'.
  void DropSessionHistoryFloorsUnlocked(const std::string& tablet_id,
                                        bool is_leader,
                                        tablet::Tablet* tablet);

  // Erases the session at 'it', releasing the history floor its stream holds
  // on 'tablet' (if non-null). Returns the iterator following 'it'. Requires
  // 'lock_'.
  CDCStreamTabletStateMap::iterator EraseSessionUnlocked(
      CDCStreamTabletStateMap::iterator it,
      tablet::Tablet* tablet);

  // Returns the per-(stream, tablet) mutex that serializes the snapshot pages,
  // creating it on first use. See snapshot_page_locks_.
  std::shared_ptr<std::timed_mutex> GetSnapshotPageLock(const CDCStreamTabletKey& key);

  // Returns the config for 'stream_id'. On the first access, fetches it from the
  // leader master via GetCDCStreamInfo and caches it; subsequent calls return the
  // cached value without an RPC. Gives up at 'deadline' (if initialized):
  // returns ServiceUnavailable if another caller's fetch of the same config
  // did not finish by then, or if 'deadline' cut this caller's fetch short.
  // Returns NotFound if the master reports that the stream does not exist,
  // NotSupported if the config has options this server does not know (see
  // StreamConfigFromMaster()), and NetworkError for any other failed fetch. A
  // failed fetch is remembered for --cdc_stream_config_negative_cache_ttl_ms:
  // until then, calls return NotFound, NotSupported, or ServiceUnavailable for
  // a fetch that failed for another reason.
  // Also returns the ids of the stream's tables in 'table_ids'.
  Status GetOrFetchStreamConfig(const std::string& stream_id,
                                const MonoTime& deadline,
                                CDCStreamConfigPB* config,
                                std::vector<std::string>* table_ids);

  // Returns InvalidArgument unless 'replica' is a tablet of one of
  // 'stream_table_ids', the tables of stream 'stream_id'.
  static Status CheckTabletInStream(const std::string& stream_id,
                                    const tablet::TabletReplica& replica,
                                    const std::vector<std::string>& stream_table_ids);

  // Returns the per-stream mutex that single-flights the master config fetch,
  // creating it on first use. See stream_config_fetch_locks_.
  std::shared_ptr<std::timed_mutex> GetStreamConfigFetchLock(const std::string& stream_id);

  // Instantiates the per-session lag/age FunctionGauges on a dedicated
  // 'cdc_stream' metric entity for 'key', backed by the atomic fields in
  // 'state'. Called (with lock_ held) when a (stream, tablet) session is first
  // created. Gauges detach via state->metric_detacher when the session ends.
  void SetupSessionMetrics(const CDCStreamTabletKey& key,
                           CDCStreamTabletState* state);

  // Reserves a slot for one CDC heavy scan (snapshot page or FULL-mode image
  // reconstruction): counts it against the --cdc_max_concurrent_scans cap and
  // reserves 'reserve_bytes' against the CDC scan MemTracker. On success returns
  // OK and the caller MUST call ReleaseScanSlot(reserve_bytes) exactly once
  // (typically via a scoped cleanup). Returns ServiceUnavailable -- mapped to a
  // retryable SERVER_TOO_BUSY CDC error -- when either budget is exhausted, so
  // heavy CDC scans cannot starve user-facing traffic.
  Status TryAcquireScanSlot(int64_t reserve_bytes);

  // Releases a slot previously acquired via TryAcquireScanSlot with the same
  // 'reserve_bytes'.
  void ReleaseScanSlot(int64_t reserve_bytes);

  // Looks up the consumer session for ('stream_id', 'tablet_id') on 'replica',
  // creating it with checkpoint 'op_index' if there is none. Returns NotFound,
  // and creates nothing, if the master released the stream because it was
  // deleted, and IllegalState if 'replica' is not running. On success, if
  // 'session' is not null, sets it to the session. The pointer identifies the
  // session only: it may be dereferenced only under 'lock_', after a lookup
  // found the same pointer (see CheckTrackedSessionUnlocked()).
  Status TrackSession(const std::string& stream_id,
                      const std::string& tablet_id,
                      int64_t op_index,
                      tablet::TabletReplica* replica,
                      CDCStreamTabletState** session = nullptr);

  // Returns OK if the session of 'key' is still 'tracked', the session that
  // TrackSession() returned, and its stream was not released since. Otherwise
  // the session ended meanwhile (the stream was deleted, this replica lost
  // leadership or stopped): returns NotFound if the stream was released, else
  // IllegalState. Requires 'lock_'.
  Status CheckTrackedSessionUnlocked(const CDCStreamTabletKey& key,
                                     const CDCStreamTabletState* tracked) const;

  // Queues 'update' for the master: the checkpoint flusher thread sends it with
  // the other queued updates in the next UpdateCDCCheckpoints batch. An update
  // already queued for the same (stream, tablet) is merged with it, so the
  // queue holds at most one update per session. Never blocks on the master.
  // The persists are best-effort: a failed batch is queued again, and the
  // consumer can always re-checkpoint.
  void QueueCheckpointUpdate(std::unique_ptr<master::UpdateCDCCheckpointRequestPB> update);

  // Queues 'update', a checkpoint that the persist throttle holds back, for
  // the master once 'due' has passed: the flusher thread then moves it to the
  // queue of QueueCheckpointUpdate(). An update already deferred for the same
  // (stream, tablet) is merged with it, keeping the earlier due time. A later
  // update for that (stream, tablet) that is not an activity report takes it
  // over, so it is sent with that update instead.
  void DeferCheckpointUpdate(std::unique_ptr<master::UpdateCDCCheckpointRequestPB> update,
                             const MonoTime& due);

  // Removes the update deferred for the (stream, tablet) of '*update', if
  // any, and merges '*update' into it, so that '*update' is what both of them
  // would do if applied in order. Requires 'flush_lock_'.
  void TakeOverDeferredCheckpointUpdateUnlocked(
      std::unique_ptr<master::UpdateCDCCheckpointRequestPB>* update);

  // Moves the deferred updates that are due at 'now' to the queue of
  // QueueCheckpointUpdate(). Returns the due time of the earliest update left
  // deferred, or MonoTime::Max() if there is none. Requires 'flush_lock_'.
  MonoTime QueueDueDeferredCheckpointUpdatesUnlocked(const MonoTime& now);

  // Sends 'update', merged with whatever is queued for its (stream, tablet),
  // to the leader master now, giving up at 'deadline'. If that fails, the
  // update is queued as by QueueCheckpointUpdate().
  void PersistCheckpointNow(std::unique_ptr<master::UpdateCDCCheckpointRequestPB> update,
                            const MonoTime& deadline);

  // Body of the checkpoint flusher thread: sends the queued updates in batches,
  // at most one batch every --cdc_checkpoint_flush_interval_ms.
  void CheckpointFlusherThread();

  // Sends 'updates' to the leader master in UpdateCDCCheckpoints batches. The
  // updates of a batch that fails, and of all batches after it, are queued
  // again.
  void FlushCheckpointUpdates(
      std::vector<std::unique_ptr<master::UpdateCDCCheckpointRequestPB>> updates);

  // Stops the checkpoint flusher thread, cancelling its in-flight RPC.
  // Idempotent.
  void StopCheckpointFlusher();

  // Calls 'call' with a proxy to one master after another, starting with the
  // master that last succeeded, until a call succeeds, returns NotFound (only
  // the leader master looks streams up, so NotFound is authoritative) or
  // Aborted, or 'deadline' (if initialized) passes. Each attempt is bounded by
  // --cdc_master_rpc_timeout_ms and by 'deadline', set on the controller passed
  // to 'call'. Returns the status of the last attempt. If 'deadline_cut' is
  // not null, sets it to whether a failure is due to 'deadline': it passed
  // before every master was tried, or it shortened the last attempt, which
  // then timed out.
  Status CallLeaderMaster(
      const MonoTime& deadline,
      const std::function<Status(master::MasterServiceProxy*, rpc::RpcController*)>& call,
      bool* deadline_cut = nullptr);

  // Fetches the durable snapshot cursor for (stream_id, tablet_id) from the
  // leader master (uncached, direct GetCDCStreamInfo), giving up at 'deadline'.
  // On success sets *found to whether an active cursor exists and, if so,
  // fills *out. Used by ReadSnapshot() when this server has no session for the
  // snapshot.
  Status FetchSnapshotCursor(const std::string& stream_id,
                             const std::string& tablet_id,
                             const MonoTime& deadline,
                             bool* found,
                             SnapshotCursorUpdate* out,
                             int64_t* streaming_start_op_index);

  // Records consumer activity for (stream_id, tablet_id): updates the in-memory
  // last-active time and, throttled by --cdc_active_time_report_interval_ms,
  // queues a lightweight activity heartbeat for the master so a live but
  // not-yet-checkpointing consumer is not expired. A checkpoint or snapshot
  // cursor persist counts as such a report. 'consumer_addr' is the remote
  // address of the call, see CDCStreamTabletState::consumer_addr.
  void RecordActivity(const std::string& stream_id, const std::string& tablet_id,
                      const std::string& consumer_addr);

  // Returns true iff the (stream_id, tablet_id) session has been idle -- no
  // successful GetChanges/Checkpoint -- for longer than
  // --cdc_stream_idle_expiry_ms. Used to disambiguate a garbage-collected WAL
  // read into STREAM_EXPIRED (permanently expired, re-bootstrap) versus
  // WAL_EXPIRED (possibly transient GC during a failover, safe to retry). This
  // is checked only when the WAL is already found missing, never proactively.
  // Returns false when the flag is <= 0 (disabled), the session is untracked,
  // or it has no recorded activity yet (last-active == 0), so the fallback is
  // always the more conservative WAL_EXPIRED.
  bool StreamIdleExpired(const std::string& stream_id,
                         const std::string& tablet_id) const;

  tserver::TabletServer* const server_;
  const std::function<bool()> enforce_access_control_;

  // Number of CDC heavy scans (snapshot/FULL) currently in flight, enforced
  // against --cdc_max_concurrent_scans. Lock-free so the shed decision adds no
  // contention on lock_.
  std::atomic<int32_t> active_scans_{0};

  // Number of GetChanges calls currently in flight (all modes), enforced
  // against the --cdc_get_changes_free_rpc_ratio worker-thread reservation so
  // CDC streaming cannot occupy every RPC worker. Lock-free; incremented at the
  // top of GetChanges and decremented when the call returns.
  std::atomic<int32_t> get_changes_inflight_{0};

  // Dedicated MemTracker (child of the server root tracker) accounting the
  // worst-case response size reserved by each in-flight CDC heavy scan, bounded
  // by --cdc_scan_mem_limit_bytes. Never null after construction.
  std::shared_ptr<MemTracker> scan_mem_tracker_;

  // Dedicated MemTracker for CHANGE-mode WAL read allocations,
  // bounded by --cdc_wal_read_mem_limit_bytes. Separate from scan_mem_tracker_ so
  // CHANGE-mode (the hot streaming path) has its own soft cap without counting
  // against the heavy-scan concurrency limit. Never null after construction.
  std::shared_ptr<MemTracker> wal_read_mem_tracker_;

  // Sets a CDC error code and status on 'error', increments the aggregate
  // cdc_errors counter, and increments the per-code counter for 'code'. This
  // is the single funnel for all CDC error accounting; call it at every error
  // return path instead of the free SetCDCError helper (which is removed).
  void SetCDCError(CDCErrorPB::Code code, const Status& status, CDCErrorPB* error);

  // CDC observability counters, instantiated on the server metric entity.
  scoped_refptr<Counter> get_changes_requests_;
  scoped_refptr<Counter> records_produced_;
  scoped_refptr<Counter> checkpoint_requests_;
  scoped_refptr<Counter> checkpoint_persists_;
  // Incremented whenever a checkpoint persist exhausts all master
  // candidates without a successful response. A non-zero rate while consumers are advancing
  // is the observable signal that the staleness-guard advance-attempt floor is
  // actively protecting the retention barrier.
  scoped_refptr<Counter> checkpoint_persist_failures_;
  // Master barrier updates ignored because a newer one was already applied.
  scoped_refptr<Counter> barrier_updates_superseded_;
  // Consumer-activity reports queued for the master by RecordActivity().
  scoped_refptr<Counter> activity_reports_;
  // Times RecordActivity() saw two consumers read one (stream, tablet).
  scoped_refptr<Counter> duplicate_consumers_detected_;
  scoped_refptr<Counter> errors_;

  // Cumulative serialized response bytes returned to CDC consumers across
  // all successful GetChanges calls. Complements cdc_records_produced
  // with byte-throughput visibility; together they give average record size.
  scoped_refptr<Counter> bytes_sent_;

  // Histogram of serialized response sizes for record-bearing
  // GetChanges responses (records_size() > 0). Empty/heartbeat polls do not
  // contribute so p50/p95/p99 reflect the actual payload-size distribution.
  // Operators use this to detect responses near the --cdc_max_bytes_per_response
  // ceiling. Incremented at both GetChanges success paths (snapshot and CHANGE
  // mode).
  scoped_refptr<Histogram> get_changes_response_bytes_;

  // Per-error-code counters indexed by CDCErrorPB::Code (values 1-17). Index
  // 0 is unused; a code outside [1, 17] falls back to index 1 (UNKNOWN_ERROR).
  std::array<scoped_refptr<Counter>, 18> error_code_counters_;

  // Admission-shed breakdown counters (all three ultimately become
  // SERVER_TOO_BUSY, but the reason is lost by the time SetCDCError is called).
  scoped_refptr<Counter> scans_rejected_concurrency_;
  scoped_refptr<Counter> scans_rejected_memory_;
  scoped_refptr<Counter> scans_rejected_server_memory_;
  scoped_refptr<Counter> scans_rejected_worker_pool_;

  // Incremented when a CHANGE-mode WAL read is shed because
  // --cdc_wal_read_mem_limit_bytes is exceeded. SERVER_TOO_BUSY (retryable) is
  // returned to the consumer.
  scoped_refptr<Counter> wal_reads_rejected_memory_;

  // Incremented when a GetChanges poll returns SERVER_TOO_BUSY because the
  // outcome of its first op is not known yet (the op is committed but not
  // applied, or its COMMIT is not written yet).
  scoped_refptr<Counter> wal_reads_rejected_outcome_pending_;

  // Incremented each time a read window is escalated to reach the commit of a
  // transaction whose WAL span exceeds the per-response byte cap.
  scoped_refptr<Counter> wal_read_escalations_;

  // Incremented for each WAL write op decoded into change records.
  scoped_refptr<Counter> write_ops_decoded_;
  scoped_refptr<Counter> full_image_rows_read_;

  // Guards stream_tablet_state_.
  mutable simple_spinlock lock_;

  // Per (stream, tablet) consumer sessions. Entries are created by
  // TrackSession() and the snapshot establish/resume paths, and erased only
  // through EraseSessionUnlocked().
  CDCStreamTabletStateMap stream_tablet_state_;

  // Serializes the snapshot pages of each (stream, tablet): a page decides
  // where to scan from by the session's paging state and updates it when its
  // scan is done, and two concurrent is_snapshot_start calls must not both
  // establish a session. The establish (WaitUntilSafe,
  // WaitForSnapshotWithAllApplied) and the scan block, so they cannot run under
  // lock_ (a spinlock); this per-key mutex is held across the page instead, and
  // a page that cannot get it by its deadline is shed. Entries are created
  // lazily via GetSnapshotPageLock and erased by their last user, so the map
  // holds only the sessions serving a page right now. Lookup/insert of the map
  // itself is guarded by lock_.
  std::unordered_map<CDCStreamTabletKey,
                     std::shared_ptr<std::timed_mutex>,
                     CDCStreamTabletKeyHash> snapshot_page_locks_;

  // Count of fresh snapshot sessions established (establish path set
  // snapshot.active from false). Test-observable via
  // SnapshotSessionsEstablishedForTests to check that concurrent first pages
  // establish a single session. Incremented in ReadSnapshot's establish block.
  std::atomic<int64_t> snapshot_sessions_established_{0};

  // Count of in-progress snapshot sessions re-established (resumed) by a cold
  // leader from the durable master cursor, and of SNAPSHOT_SESSION_LOST responses
  // returned because no resumable cursor existed or its history was gone.
  // Test-observable via SnapshotSessionsResumed/LostForTests. Incremented in
  // ReadSnapshot's cold-resume block.
  std::atomic<int64_t> snapshot_sessions_resumed_{0};
  std::atomic<int64_t> snapshot_sessions_lost_{0};

  // See SetSnapshotPageScannedHookForTests(),
  // SetSnapshotEstablishHookForTests(), SetSnapshotSessionTrackedHookForTests()
  // and SetSnapshotCursorFetchHookForTests() (guarded by lock_).
  std::function<void()> snapshot_page_scanned_hook_for_tests_;
  std::function<void()> snapshot_establish_hook_for_tests_;
  std::function<void()> snapshot_session_tracked_hook_for_tests_;
  SnapshotCursorFetchHook snapshot_cursor_fetch_hook_for_tests_;

  // Count of actual master GetCDCStreamInfo RPCs issued (one per real fetch that
  // reaches the master loop, not per cache hit). Test-observable via
  // StreamConfigMasterFetchesForTests to check that concurrent cache misses
  // share a single master fetch.
  std::atomic<int64_t> stream_config_master_fetches_{0};

  // Per-stream config cache, populated on first access via the master
  // GetCDCStreamInfo RPC and refreshed once an entry passes its 'expiry'
  // deadline (bounded by --cdc_stream_config_cache_ttl_ms), so a stream
  // reconfigure (e.g. record_type CHANGE -> FULL) is picked up within the TTL
  // rather than never. Guarded by lock_.
  struct CachedStreamConfig {
    CDCStreamConfigPB config;
    // The ids of the stream's tables.
    std::vector<std::string> table_ids;
    // The entry is fresh while MonoTime::Now() < expiry. MonoTime::Max() means
    // it never expires (test-seeded entries).
    MonoTime expiry;
    // The stream's idle expiry reported by the master, or -1 if it did not
    // report one (see StreamIdleExpiryMsUnlocked()).
    int64_t idle_expiry_ms = -1;
  };
  std::unordered_map<std::string, CachedStreamConfig> stream_config_cache_;

  // Single-flights the master GetCDCStreamInfo fetch per stream_id. Without
  // it, N concurrent GetChanges that all miss the cache for the same stream (the
  // classic case: dozens of tablets for one stream re-streaming on one tserver
  // right after a restart) each issue their own GetCDCStreamInfo RPC, spiking
  // master catalog-lock contention. The blocking master RPC cannot run under
  // lock_ (a spinlock), so -- exactly like snapshot_page_locks_ -- a per-stream
  // mutex serializes the fetch: the first caller does the RPC and populates the
  // cache; the others block on the mutex, then re-check the cache and return the
  // just-fetched value without an RPC of their own. Entries are created lazily
  // and erased by the last user once its fetch is over, so the map holds only
  // the streams being fetched. Lookup/insert of the map itself is guarded by
  // lock_. A waiter gives up at its deadline (a std::timed_mutex).
  std::unordered_map<std::string, std::shared_ptr<std::timed_mutex>>
      stream_config_fetch_locks_;

  // Failed config fetches for streams with no cached config, so that a master
  // outage, or requests for a stream that does not exist, do not cost a master
  // fetch per call. An entry is used while MonoTime::Now() < expiry. Bounded:
  // see kMaxStreamConfigFetchFailures. Guarded by lock_.
  struct StreamConfigFetchFailure {
    Status status;
    MonoTime expiry;
  };
  std::unordered_map<std::string, StreamConfigFetchFailure> stream_config_fetch_failures_;

  // Checkpoint updates waiting for the flusher thread, one per (stream,
  // tablet). See QueueCheckpointUpdate(). Guarded by flush_lock_.
  std::unordered_map<CDCStreamTabletKey,
                     std::unique_ptr<master::UpdateCDCCheckpointRequestPB>,
                     CDCStreamTabletKeyHash> pending_checkpoint_updates_;
  // Checkpoint updates that the persist throttle deferred, one per (stream,
  // tablet), with the time they are due. See DeferCheckpointUpdate(). Guarded
  // by flush_lock_.
  struct DeferredCheckpointUpdate {
    MonoTime due;
    std::unique_ptr<master::UpdateCDCCheckpointRequestPB> update;
  };
  std::unordered_map<CDCStreamTabletKey, DeferredCheckpointUpdate,
                     CDCStreamTabletKeyHash> deferred_checkpoint_updates_;
  // Set by StopCheckpointFlusher(). Guarded by flush_lock_.
  bool flusher_stopped_ = false;
  // The flusher's in-flight master RPC, if any, so that stopping the flusher can
  // cancel it. Guarded by flush_lock_.
  rpc::RpcController* flusher_rpc_ = nullptr;
  // Protects the four members above. Never held across a master RPC.
  mutable std::mutex flush_lock_;
  // Signaled when an update is queued for an empty queue or deferred, and on
  // stop.
  std::condition_variable flush_cond_;
  scoped_refptr<Thread> flusher_thread_;

  // Index (into the tablet server's master addresses) of the master that last
  // answered a CDC master RPC: the leader, most likely. See CallLeaderMaster().
  std::atomic<int> master_hint_{0};

  // Streams whose sessions the master released on stream deletion (see
  // SetRetentionBarrier). TrackSession() and Checkpoint refuse these, so a
  // GetChanges or Checkpoint racing the release cannot re-create a session
  // (and its history floor and metrics) for a deleted stream. Stream ids are
  // never reused; bounded by the number of streams deleted while this server
  // is up. Guarded by lock_.
  std::unordered_set<std::string> released_consumer_streams_;

  // Set by Shutdown(). Guarded by lock_.
  bool shutting_down_ = false;

  // Detaches the lag/retention FunctionGauges at destruction so their callbacks
  // stop firing before the state they read (above) is torn down. Declared last
  // so it is destroyed first.
  FunctionGaugeDetacher metric_detacher_;

  DISALLOW_COPY_AND_ASSIGN(CDCServiceImpl);
};

} // namespace cdc
} // namespace kudu
