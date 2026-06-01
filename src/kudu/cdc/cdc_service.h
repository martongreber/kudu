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
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.service.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/gutil/macros.h"
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

namespace rpc {
class RpcContext;
} // namespace rpc

namespace tserver {
class TabletServer;
} // namespace tserver

namespace cdc {

// Server-driven consistent-snapshot session state for a (stream, tablet) pair.
// Active during the multi-RPC paginated snapshot scan that bootstraps a consumer
// from the table's current contents before WAL streaming begins.
struct CDCSnapshotState {
  // True while a snapshot scan is in progress for this (stream, tablet).
  bool active = false;

  // The hybrid-clock timestamp at which the snapshot is read. Every READ record
  // reflects the tablet state at this timestamp; held constant across all pages.
  Timestamp snap_ts;

  // The committed op-index captured when the snapshot began. After the scan
  // completes the consumer starts WAL streaming from this index.
  int64_t streaming_start_op_index = 0;

  // Serialized encoded primary key of the last row emitted in the previous page.
  // Empty means "scan from the beginning"; the next page resumes strictly after
  // this key.
  std::string resume_key;
};

// State tracked per (stream_id, tablet_id) pair for a single CDC consumer session.
// Holds the WAL retention anchor and the last known checkpoint index.
struct CDCStreamTabletState {
  CDCStreamTabletState() = default;

  // WAL anchor preventing GC of entries at or after checkpoint_op_index.
  // Registered with the tablet's LogAnchorRegistry.
  log::LogAnchor anchor;

  // The last op_index successfully checkpointed by the consumer via the
  // Checkpoint RPC. Atomic so the per-session FunctionGauges (ops_behind,
  // bootstrap_required) can read it without holding lock_.
  std::atomic<int64_t> checkpoint_op_index{0};

  // Human-readable owner string for the anchor (includes stream_id for debugging).
  std::string anchor_owner;

  // For FULL-mode streams: the physical-time floor (microseconds) of the oldest
  // op not yet consumed by this stream. Reported to the master on each Checkpoint
  // so the master can fan the history barrier out to followers. 0 for CHANGE mode
  // or when not yet set.
  uint64_t cdc_min_history_ts_micros = 0;

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
  // master. Used to throttle durable checkpoint writes (see
  // --cdc_checkpoint_persist_interval_ms); the in-memory WAL anchor still
  // advances on every Checkpoint regardless of this throttle.
  int64_t last_checkpoint_persist_micros = 0;

  // Physical-time component (microseconds) of the newest record emitted to the
  // consumer for this session. Used to compute the consumer sent-lag metric.
  // Atomic for the same reason as last_active_time_micros above.
  std::atomic<uint64_t> last_sent_record_phys_micros{0};

  // The tablet's last committed op index as of the most recent GetChanges call
  // for this session. Used to compute ops_behind = up_to - checkpoint.
  // Atomic for the same reason as last_active_time_micros above. -1 = not yet set.
  std::atomic<int64_t> last_known_up_to_op_index{-1};

  // The log reader's minimum retained replicate index as of the most recent
  // GetChanges call. Used to compute bootstrap_required: if the consumer's
  // checkpoint_op_index is below this value, the WAL it needs may have been GC'd.
  // Atomic for the same reason as last_active_time_micros above. -1 = not yet set.
  std::atomic<int64_t> last_known_min_replicate_index{-1};

  // Phase 5: snapshot scan session state.
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
  size_t operator()(const CDCStreamTabletKey& k) const {
    size_t h = std::hash<std::string>{}(k.stream_id);
    h ^= std::hash<std::string>{}(k.tablet_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
  }
};

// CDCServiceImpl serves the CDCService RPC interface on a tablet server.
//
// It exposes two RPCs:
//   GetChanges  — returns a batch of CDCRecordPB entries from the WAL
//   Checkpoint  — durably persists consumer progress and advances WAL anchor
//
// Design notes:
//   - Reads are sourced from the tablet's LogCache (hot) then LogReader (cold).
//   - One LogAnchor per (stream_id, tablet_id) prevents WAL GC past the
//     consumer's checkpoint.
//   - Only the tablet leader serves CDC (followers return TABLET_NOT_LEADER).
//   - Stream metadata (existence, config) is validated against the master on
//     first access and cached locally.
class CDCServiceImpl : public CDCServiceIf {
 public:
  explicit CDCServiceImpl(tserver::TabletServer* server);
  ~CDCServiceImpl() override;

  // RPC handlers — called by the generated service stub.
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

  // Sets (min_retained_op_index >= 0) or releases (< 0) this replica's per-tablet
  // CDC WAL retention anchor. Invoked on every replica of a CDC tablet by the
  // leader master's periodic maintenance (via the UpdateCDCRetentionBarrier RPC),
  // so retention survives leader changes and independent follower log GC. Unlike
  // the per-(stream, tablet) anchors registered on GetChanges (leader only), this
  // anchor is maintained on followers too.
  //
  // 'barrier_seq' is the master's monotonic sequence for this update. Barrier
  // RPCs are async, best-effort and unordered, so a stale SET could otherwise
  // land after a newer RELEASE (leaking WAL/history retention) or vice versa.
  // The replica records the highest sequence applied per tablet and ignores any
  // request whose sequence is lower (last-writer-wins). 0 means unsequenced (a
  // legacy master) and is always applied.
  //
  // If 'release_consumer_stream_id' is non-empty, the per-(stream, tablet)
  // consumer anchor for that stream is also released (unregistered from the
  // LogAnchorRegistry and erased from stream_tablet_state_). This is driven by
  // the master on stream deletion: the aggregate retention anchor here is
  // master-pushed, but the consumer anchor is established by the consumer's own
  // GetChanges/Checkpoint polling (leader only) and would otherwise be freed
  // only when the tablet itself is deleted (A4). The seq gate does NOT guard the
  // consumer-anchor release -- a stream delete is terminal, so the release is
  // always applied regardless of 'barrier_seq'.
  //
  // If 'skip_barrier_update' is true, the aggregate barrier is left untouched
  // ('min_retained_op_index' / 'history_safe_time_micros' are ignored) and only
  // the consumer anchor is released. Used when the deleted stream's tablet still
  // has other live streams, so releasing the aggregate barrier here would
  // briefly drop retention the survivors need before the master's next
  // maintenance pass recomputes it.
  Status SetRetentionBarrier(const std::string& tablet_id,
                             int64_t min_retained_op_index,
                             uint64_t history_safe_time_micros = 0,
                             int64_t barrier_seq = 0,
                             const std::string& release_consumer_stream_id = "",
                             bool skip_barrier_update = false);

  // Releases every CDC WAL anchor (the per-tablet retention anchor and all
  // per-(stream, tablet) consumer anchors) registered on 'tablet_id'. Called by
  // the tablet manager just before it deletes a tablet, so the tablet's
  // LogAnchorRegistry is empty when destroyed. Safe to call for a tablet with no
  // CDC anchors.
  void ReleaseAnchorsForTablet(const std::string& tablet_id);

  // Test-only: seed the per-stream config cache so tests can exercise FULL-mode
  // and snapshot behavior without a live master issuing GetCDCStreamInfo. The
  // seeded entry never expires, so it is served for the life of the test
  // regardless of --cdc_stream_config_cache_ttl_ms.
  void SetStreamConfigForTests(const std::string& stream_id,
                               const CDCStreamConfigPB& config) {
    std::lock_guard<simple_spinlock> lock(lock_);
    stream_config_cache_[stream_id] = {config, MonoTime::Max()};
  }

  // Test-only: seed the per-stream config cache with a finite freshness
  // deadline 'ttl' from now, mimicking a real master fetch. Used to exercise
  // TTL-based staleness/eviction.
  void SetStreamConfigForTestsWithTtl(const std::string& stream_id,
                                      const CDCStreamConfigPB& config,
                                      const MonoDelta& ttl) {
    std::lock_guard<simple_spinlock> lock(lock_);
    stream_config_cache_[stream_id] = {config, MonoTime::Now() + ttl};
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

  // Test-only: number of times a fresh snapshot session has been established
  // (the establish path ran and set snapshot.active from false). Two concurrent
  // is_snapshot_start calls for one (stream, tablet) must establish exactly once
  // (A2); this counter lets a test assert that.
  int64_t SnapshotSessionsEstablishedForTests() const {
    return snapshot_sessions_established_.load(std::memory_order_relaxed);
  }

  // Count of actual master GetCDCStreamInfo RPCs issued by GetOrFetchStreamConfig
  // (one per real fetch attempt, not per cache hit). Lets a test assert that N
  // concurrent cache misses for one stream collapse to a single fetch (E11).
  int64_t StreamConfigMasterFetchesForTests() const {
    return stream_config_master_fetches_.load(std::memory_order_relaxed);
  }

  // Test-only: true iff SetRetentionBarrier currently holds a per-tablet
  // retention anchor for 'tablet_id'. When true and 'min_retained_op_index' is
  // non-null, stores the anchored minimum op index. Lets a barrier test observe
  // only the CDC-owned anchor, independent of unrelated anchors the tablet's
  // own machinery (MRS/DMS flush, etc.) keeps in the shared LogAnchorRegistry.
  bool RetentionAnchorForTests(const std::string& tablet_id,
                               int64_t* min_retained_op_index = nullptr) const {
    std::lock_guard<simple_spinlock> lock(lock_);
    auto it = retention_anchors_.find(tablet_id);
    if (it == retention_anchors_.end()) {
      return false;
    }
    if (min_retained_op_index) {
      *min_retained_op_index = it->second->min_retained_op_index;
    }
    return true;
  }

  // Test-only: true iff a per-(stream, tablet) consumer session is currently
  // tracked for ('stream_id', 'tablet_id') -- i.e. the consumer anchor has been
  // established and not yet released. Lets a test assert that a stream delete
  // releases the leader-only consumer anchor (A4), which
  // RetentionAnchorForTests (the master-pushed aggregate barrier) does not
  // observe.
  bool ConsumerAnchorForTests(const std::string& stream_id,
                              const std::string& tablet_id) const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return stream_tablet_state_.find(CDCStreamTabletKey{stream_id, tablet_id}) !=
           stream_tablet_state_.end();
  }

  // Lag/retention gauge values, computed over the active (stream, tablet)
  // sessions. Backing the identically-named FunctionGauges; also exposed for
  // tests. MaxSentLagMicros/MaxActiveAgeMicros return microseconds.
  int64_t MaxSentLagMicros() const;
  int64_t MaxActiveAgeMicros() const;
  uint64_t ActiveStreamCount() const;
  int64_t MaxOpsBehind() const;
  int64_t BootstrapRequiredStreamCount() const;

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
                     const MonoTime& deadline,
                     GetChangesResponsePB* resp);

  // Executes one page of the server-driven consistent snapshot for
  // (stream_id, tablet_id). On is_start, captures the snapshot timestamp and the
  // streaming start op-index and pins tablet history; otherwise resumes from the
  // stored resume key. Emits READ records into resp and sets the snapshot
  // response fields (snapshot_done, snapshot_resume_key,
  // snapshot_streaming_start_op_index).
  Status ReadSnapshot(const std::string& stream_id,
                      const std::string& tablet_id,
                      bool is_start,
                      const std::string& req_resume_key,
                      int64_t max_bytes,
                      const MonoTime& deadline,
                      GetChangesResponsePB* resp);

  // Returns the per-(stream, tablet) mutex that serializes the snapshot-start
  // sequence, creating it on first use. See snapshot_start_locks_.
  std::shared_ptr<std::mutex> GetSnapshotStartLock(const CDCStreamTabletKey& key);

  // Returns the config for 'stream_id'. On the first access, fetches it from the
  // leader master via GetCDCStreamInfo and caches it; subsequent calls return the
  // cached value without an RPC.
  Status GetOrFetchStreamConfig(const std::string& stream_id,
                                CDCStreamConfigPB* config);

  // Returns the per-stream mutex that single-flights the master config fetch,
  // creating it on first use. See stream_config_fetch_locks_ (E11).
  std::shared_ptr<std::mutex> GetStreamConfigFetchLock(const std::string& stream_id);

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

  // Updates (or registers) the WAL anchor for (stream_id, tablet_id) to
  // anchor_op_index, preventing GC of WAL segments at or after that index.
  Status UpdateAnchor(const std::string& stream_id,
                      const std::string& tablet_id,
                      int64_t anchor_op_index,
                      log::LogAnchorRegistry* registry);

  // Persists the checkpoint to the leader master via RPC so that consumers can
  // resume, and so a newly-elected tablet leader can restore the WAL anchor,
  // after a crash or leader change. Best-effort: invoked after the local
  // Checkpoint() RPC has already responded, and errors are logged rather than
  // surfaced (the consumer can always re-checkpoint).
  void PersistCheckpoint(const std::string& stream_id,
                         const std::string& tablet_id,
                         int64_t op_index,
                         uint64_t history_safe_time_micros = 0,
                         bool refresh_active_time_only = false);

  // Records consumer activity for (stream_id, tablet_id): updates the in-memory
  // last-active time and, throttled by --cdc_active_time_report_interval_ms,
  // sends a lightweight activity heartbeat to the master so a live but
  // not-yet-checkpointing consumer is not expired.
  void RecordActivity(const std::string& stream_id, const std::string& tablet_id);

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
  // CF-2: incremented whenever PersistCheckpoint exhausts all master candidates
  // without a successful response. A non-zero rate while consumers are advancing
  // is the observable signal that the staleness-guard advance-attempt floor is
  // actively protecting the retention barrier.
  scoped_refptr<Counter> checkpoint_persist_failures_;
  scoped_refptr<Counter> errors_;

  // Per-error-code counters indexed by CDCErrorPB::Code (values 1-12). Index
  // 0 is unused; a code outside [1, 12] falls back to index 1 (UNKNOWN_ERROR).
  std::array<scoped_refptr<Counter>, 14> error_code_counters_;

  // Admission-shed breakdown counters (all three ultimately become
  // SERVER_TOO_BUSY, but the reason is lost by the time SetCDCError is called).
  scoped_refptr<Counter> scans_rejected_concurrency_;
  scoped_refptr<Counter> scans_rejected_memory_;
  scoped_refptr<Counter> scans_rejected_server_memory_;
  scoped_refptr<Counter> scans_rejected_worker_pool_;

  // Guards stream_tablet_state_.
  mutable simple_spinlock lock_;

  // Per (stream, tablet) state: anchor + last checkpoint.
  std::unordered_map<CDCStreamTabletKey,
                     std::unique_ptr<CDCStreamTabletState>,
                     CDCStreamTabletKeyHash> stream_tablet_state_;

  // Serializes the snapshot-start sequence per (stream, tablet) so two
  // concurrent is_snapshot_start calls cannot both run the establish path and
  // race on snap_ts / streaming_start_op_index (A2). The establish work blocks
  // (WaitUntilSafe, WaitForSnapshotWithAllApplied), so it cannot run under lock_
  // (a spinlock); this per-key mutex is held across it instead. Entries are
  // created lazily via GetSnapshotStartLock and never removed -- bounded by the
  // number of distinct (stream, tablet) pairs, like stream_tablet_state_.
  // Lookup/insert of the map itself is guarded by lock_.
  std::unordered_map<CDCStreamTabletKey,
                     std::shared_ptr<std::mutex>,
                     CDCStreamTabletKeyHash> snapshot_start_locks_;

  // Count of fresh snapshot sessions established (establish path set
  // snapshot.active from false). Test-observable via
  // SnapshotSessionsEstablishedForTests to guard the A2 single-establish
  // invariant. Incremented in ReadSnapshot's establish block.
  std::atomic<int64_t> snapshot_sessions_established_{0};

  // Count of actual master GetCDCStreamInfo RPCs issued (one per real fetch that
  // reaches the master loop, not per cache hit). Test-observable via
  // StreamConfigMasterFetchesForTests to guard the E11 single-flight invariant.
  std::atomic<int64_t> stream_config_master_fetches_{0};

  // Per-stream config cache, populated on first access via the master
  // GetCDCStreamInfo RPC and refreshed once an entry passes its 'expiry'
  // deadline (bounded by --cdc_stream_config_cache_ttl_ms), so a stream
  // reconfigure (e.g. record_type CHANGE -> FULL) is picked up within the TTL
  // rather than never. Guarded by lock_.
  struct CachedStreamConfig {
    CDCStreamConfigPB config;
    // The entry is fresh while MonoTime::Now() < expiry. MonoTime::Max() means
    // it never expires (test-seeded entries).
    MonoTime expiry;
  };
  std::unordered_map<std::string, CachedStreamConfig> stream_config_cache_;

  // E11: single-flights the master GetCDCStreamInfo fetch per stream_id. Without
  // it, N concurrent GetChanges that all miss the cache for the same stream (the
  // classic case: dozens of tablets for one stream re-streaming on one tserver
  // right after a restart) each issue their own GetCDCStreamInfo RPC, spiking
  // master catalog-lock contention. The blocking master RPC cannot run under
  // lock_ (a spinlock), so -- exactly like snapshot_start_locks_ -- a per-stream
  // mutex serializes the fetch: the first caller does the RPC and populates the
  // cache; the others block on the mutex, then re-check the cache and return the
  // just-fetched value without an RPC of their own. Entries are created lazily
  // and never removed, bounded by the number of distinct streams. Lookup/insert
  // of the map itself is guarded by lock_.
  std::unordered_map<std::string, std::shared_ptr<std::mutex>>
      stream_config_fetch_locks_;

  // Per-tablet WAL retention anchor holding the minimum op index across all
  // streams, maintained on every replica by SetRetentionBarrier. Keyed by
  // tablet_id. Guarded by lock_.
  struct TabletRetentionState {
    log::LogAnchor anchor;
    std::string anchor_owner;
    // History floor (microseconds) most recently pushed by the master for this
    // tablet; 0 if none. Tracked for observability/debugging.
    uint64_t history_safe_time_micros = 0;
    // Minimum retained op index currently anchored. Tracked for tests/debugging.
    int64_t min_retained_op_index = -1;
  };
  std::unordered_map<std::string,
                     std::unique_ptr<TabletRetentionState>> retention_anchors_;

  // Highest barrier sequence applied per tablet, for last-writer-wins ordering
  // of reordered SET/RELEASE barrier updates (see SetRetentionBarrier). Unlike
  // retention_anchors_, an entry here is NOT erased on release: it must outlive
  // the anchor so a stale SET arriving after a RELEASE is still recognized as
  // superseded. Bounded by the number of distinct tablets ever barriered on this
  // server (same order as retention_anchors_'s historical footprint). Guarded by
  // lock_.
  std::unordered_map<std::string, int64_t> barrier_last_seq_;

  // Detaches the lag/retention FunctionGauges at destruction so their callbacks
  // stop firing before the state they read (above) is torn down. Declared last
  // so it is destroyed first.
  FunctionGaugeDetacher metric_detacher_;

  DISALLOW_COPY_AND_ASSIGN(CDCServiceImpl);
};

} // namespace cdc
} // namespace kudu
