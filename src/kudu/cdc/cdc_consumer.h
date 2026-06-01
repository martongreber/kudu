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

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/master.pb.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

namespace kudu {
namespace cdc {

class CDCClient;
class CDCRecordPB;
class CDCTabletPoller;
struct CDCStreamInfo;
struct CDCTabletInfo;

// A single decoded column value handed to the application.
struct CDCDecodedColumn {
  // The column's name when the operation was applied.
  std::string name;
  // The column's id (CDCColumnValuePB.column_id), or -1 if the record has none.
  int32_t column_id = -1;
  bool is_null = false;
  // Whether the column's type is known and 'raw_value' is a valid value of it.
  // False if the column is in none of the schemas the consumer learned (for
  // example, it was added and dropped again before the consumer saw a schema
  // with it), or if 'raw_value' has the wrong size for the type: then 'value'
  // is empty (and in the first case 'type_info' is null), but 'raw_value'
  // still holds the value.
  bool decode_ok = false;
  // The column's type, and its precision and scale (DECIMAL) or length
  // (VARCHAR). A static singleton, see GetTypeInfo().
  const TypeInfo* type_info = nullptr;
  ColumnTypeAttributes type_attributes;
  // The value as the tablet server sent it: the cell's bytes for a fixed-size
  // type (in the server's byte order: the unscaled value for a DECIMAL,
  // microseconds since the epoch for UNIXTIME_MICROS, days since the epoch for
  // DATE), the bytes themselves for STRING, VARCHAR and BINARY. Empty when
  // 'is_null' is true.
  std::string raw_value;
  // Human-readable value, for logging and display: Kudu's debug rendering of
  // the value (a quoted and escaped STRING or BINARY, for example), except
  // that a DECIMAL is shown with its scale ("12.34"). Empty when 'is_null' or
  // not 'decode_ok'.
  std::string value;
};

// A single change record, decoded against the table schema.
struct CDCDecodedRecord {
  CDCOpTypePB op_type = INSERT;
  int64_t op_index = -1;
  int64_t op_term = -1;
  uint64_t timestamp = 0;
  uint32_t schema_version = 0;
  std::string tablet_id;

  bool has_commit_timestamp = false;
  uint64_t commit_timestamp = 0;

  bool has_txn_id = false;
  std::string txn_id;  // raw bytes

  // See CDCRecordPB.keep_immutable_columns_on_update.
  bool keep_immutable_columns_on_update = false;

  // After-image (from 'changes'). For DELETE, only the primary key columns.
  std::vector<CDCDecodedColumn> after;
  // Before-image (from 'old_changes'); populated only for FULL streams.
  std::vector<CDCDecodedColumn> before;

  // For DDL records. 'new_table_name' is empty if the record has none (see
  // CDCRecordPB.new_table_name).
  bool has_new_schema = false;
  uint32_t new_schema_version = 0;
  std::string new_table_name;
};

// The schemas of a table that a consumer learned, by schema version, and every
// column they had, by column id. A column id is never reused and the type of a
// column never changes, so a value can be decoded by its column id even if the
// consumer did not learn the schema version of its record. Entries are never
// removed or changed. Thread-safe.
class CDCSchemaHistory {
 public:
  CDCSchemaHistory() = default;

  // Adds 'schema' as schema version 'version'. A version that was added before
  // is kept: the master gives every tablet the same schema for a version.
  void AddVersion(uint32_t version, const Schema& schema);

  // Adds the columns of 'schema', whose version is not known.
  void AddColumns(const Schema& schema);

  // Returns the column of 'cv', a value of a record of schema version
  // 'version', or nullptr if it is not known. The column is looked up by its
  // id if 'cv' has one, else by its name in the schema of 'version'. The
  // returned pointer stays valid for the lifetime of this object.
  const ColumnSchema* FindColumn(uint32_t version, const CDCColumnValuePB& cv) const;

  // The largest column id in the history, or -1 if none.
  int32_t max_column_id() const {
    return max_column_id_.load(std::memory_order_acquire);
  }

 private:
  // Adds the columns of 'schema' to 'columns_by_id_'. Requires 'lock_' held
  // exclusively.
  void AddColumnsUnlocked(const Schema& schema);

  // Read by every decoded value, written once per new schema.
  mutable rw_spinlock lock_;
  std::map<uint32_t, Schema> schemas_by_version_;
  std::map<int32_t, ColumnSchema> columns_by_id_;
  std::atomic<int32_t> max_column_id_{-1};

  DISALLOW_COPY_AND_ASSIGN(CDCSchemaHistory);
};

// A batch of decoded records for a single tablet, in WAL order. The records of
// a transaction are delivered together, between its BEGIN and COMMIT records,
// when the transaction commits: they keep the op index of the write that made
// them, which is below the op index of the BEGIN and COMMIT records, so the op
// index of the records in a batch is not monotonic. Use 'checkpoint_op_index',
// not a record's op index, to tell which changes were applied.
struct CDCRecordBatch {
  std::string tablet_id;
  std::vector<CDCDecodedRecord> records;
  // The tablet's WAL position after this batch: a stream that resumes from it
  // gets every change after the batch, and none of the batch's. -1 for a
  // snapshot batch (READ records), which has no WAL position.
  int64_t checkpoint_op_index = -1;
};

// The state of a tablet's poller.
enum class CDCTabletState {
  // Taking the tablet's snapshot (READ records).
  kSnapshotting,
  // Following the tablet's WAL.
  kStreaming,
  // Stopped by CDCConsumer::Stop().
  kStopped,
  // Stopped on an error that needs operator action (see 'terminal_error'),
  // including an error returned by the RecordCallback.
  kFailed,
  // Stopped because the stream's position on the tablet is gone (the WAL or
  // the history it needs was garbage-collected, or the stream expired): the
  // tablet needs a new snapshot. See --cdc_max_resnapshot_attempts.
  kNeedsResnapshot,
  // Stopped because the tablet's range partition was dropped (see
  // CDCTabletProgress::dropped_partition). Not an error: the tablet has no
  // more changes. A poller in this state is removed by the first ReScan()
  // after it is done: Options::on_tablet_stopped returned and the drop was
  // acknowledged to the master.
  kDropped,
  // Stopped after the tablet's snapshot was done, on a stream whose snapshot
  // mode is INITIAL_ONLY: the snapshot is the stream's only output.
  kSnapshotComplete,
};

// Returns true if 'state' is a state that the poller does not leave by itself
// because of an error: kFailed or kNeedsResnapshot. See
// CDCConsumer::RestartTablet().
bool IsTerminal(CDCTabletState state);

// Returns true if the poller stopped by itself in 'state': a terminal state,
// kDropped or kSnapshotComplete.
bool IsFinished(CDCTabletState state);

const char* CDCTabletStateToString(CDCTabletState state);

// Per-tablet progress, for introspection / lag monitoring.
struct CDCTabletProgress {
  std::string tablet_id;
  CDCTabletState state = CDCTabletState::kStreaming;
  int64_t last_delivered_op_index = -1;
  int64_t last_checkpointed_op_index = -1;
  bool snapshot_done = false;
  // Set while the tablet needs a new snapshot (WAL/HISTORY/STREAM expired, or
  // its snapshot could not be completed), until a snapshot completes.
  bool needs_resnapshot = false;
  // The last error, transient or not. Cleared by the next successful poll.
  Status last_error;
  // The error that stopped the poller, if 'state' is terminal.
  Status terminal_error;
  // Number of GetChanges calls that failed (with a transport or CDC error)
  // since the poller started.
  int64_t num_errors = 0;
  // Set when 'state' is kDropped: the partition of the tablet, which bounds
  // the rows that were dropped, and its human-readable form.
  PartitionPB dropped_partition;
  std::string dropped_partition_description;
  // Set when 'state' is kDropped and the tablet was deleted before the
  // consumer got all of its changes: those after 'last_delivered_op_index'
  // (or, if it is -1, after 'last_checkpointed_op_index') are lost. See
  // --cdc_dropped_tablet_retention_secs on the masters.
  bool dropped_tail_lost = false;
};

// Delivery callback: invoked with one batch for one tablet. May be called
// concurrently from different tablet threads, but never concurrently for the
// same tablet. Returning a non-OK Status stops that tablet's poller in state
// kFailed, without advancing its position past the batch.
typedef std::function<Status(const CDCRecordBatch&)> RecordCallback;

// Invoked on the tablet's poller thread when a poller stops by itself (see
// IsFinished()), with the tablet's progress at that point. For a tablet whose
// range partition was dropped (kDropped), this is the signal to apply the drop:
// to remove the rows delivered from this tablet. It comes after every batch of
// the tablet, and at least once: the consumer acknowledges the drop to the
// server only after the callback returned, so a consumer restarted before
// that gets it again. A range partition with the same bounds that is added
// again is a new tablet, whose rows may be delivered before the old tablet's
// drop signal, so the sink must not remove every row within the bounds.
typedef std::function<void(const CDCTabletProgress&)> TabletStoppedCallback;

// Consumes a CDC stream: discovers the covered table's tablets, spawns one
// poller per tablet, decodes records against the schema, and delivers them via
// a RecordCallback. Handles per-tablet leader failover, periodic checkpointing,
// resume from durable checkpoints, snapshot bootstrap, and expiry detection.
//
// Experimental: this is the reference consumer that the 'kudu cdc' tool uses.
// CDCClient is the supported interface for building other consumers.
//
// Ordering and atomicity hold per tablet only. Each tablet's records come in
// the order the changes became visible on it, and a transaction's rows on a
// tablet come in one batch, but tablets are polled independently: there is no
// order across tablets, and the rows of a transaction that wrote several
// tablets come in one batch per tablet, at different times (they share txn_id
// and commit_timestamp). A tablet whose poller stopped on an error before it
// got to the transaction's commit delivers none of its part.
//
// Each tablet is polled by its own thread, and one consumer polls every tablet
// of the stream, from the positions checkpointed on the master: it cannot poll
// only some of the tablets, or start from positions the embedder stored. A
// consumer that needs either uses CDCClient directly.
//
// Each record is decoded with the schema it was written with, whatever the
// schema of the table or of the consumer's other tablets is by then: the
// consumer keeps the schemas it learned (the table's at Create(), each
// tablet's at its first GetChanges call, and those of the DDL records), see
// CDCSchemaHistory.
//
// The consumer does not own the CDCClient; the caller must keep it alive for
// the consumer's lifetime.
//
// The consumer assumes it is the only consumer of its stream: two consumers of
// one stream advance the same checkpoint, so each may resume past changes that
// only the other one applied. The tablet servers count it when they see that
// (the cdc_duplicate_consumers_detected metric).
//
// Thread-safety: Start/Stop/Flush/ReScan/RestartTablet are meant to be called
// from a single controlling thread. GetProgress is safe to call concurrently
// from any thread, including from the callbacks. Stop() may also be called from
// a callback, see Stop(). When --cdc_tablet_rescan_interval_ms > 0, a
// background rescan thread also calls ReScan() periodically. All accesses to
// the 'pollers_' vector are guarded by 'pollers_lock_'.
class CDCConsumer {
 public:
  // Where each tablet should start when it has no durable server-side
  // checkpoint. Tablets that DO have a durable checkpoint always resume from
  // it, and tablets whose snapshot is in progress always take their snapshot
  // again from the beginning, regardless of this mode. Tablets added to the
  // table after the stream was created (ADD RANGE PARTITION) start from their
  // first op, or with a snapshot in kSnapshot mode: the mode only applies to
  // the tablets the stream was created with.
  enum StartMode {
    // Skip existing history; start at the current tail ("tail -f").
    kNow,
    // Emit a consistent snapshot first, then follow live changes. Requires the
    // stream to have been created with a snapshot mode other than NEVER.
    kSnapshot,
    // Start from the earliest WAL still retained on the server.
    kEarliest,
  };

  struct Options {
    std::string stream_id;
    StartMode start_mode = kNow;

    // How often (wall-clock) to durably checkpoint per tablet.
    MonoDelta checkpoint_interval = MonoDelta::FromSeconds(10);

    // Idle backoff bounds applied when a tablet returns no new records.
    MonoDelta min_poll_backoff = MonoDelta::FromMilliseconds(200);
    MonoDelta max_poll_backoff = MonoDelta::FromSeconds(2);

    // Per-response byte cap forwarded to GetChanges. 0 leaves the tablet
    // server's default in place.
    int64_t max_bytes_per_response = 0;

    // Whether to checkpoint the stream at all. Without checkpoints, a later
    // consumer of the stream starts where the last checkpointing one stopped,
    // and the stream keeps retaining the WAL from there. A consumer that does
    // not checkpoint changes no durable state of the stream, so it takes no
    // snapshot either (snapshot progress is durable): kSnapshot is rejected,
    // Start() fails if a tablet's snapshot is in progress, ReScan() does not
    // poll such a tablet, and a tablet whose position expired stops in
    // kNeedsResnapshot.
    bool checkpoint_enabled = true;

    // Called when a tablet's poller stops by itself. Optional.
    TabletStoppedCallback on_tablet_stopped;
  };

  // Builds a consumer for the given (already-created) stream. Resolves the
  // stream's table and current schema. Does not spawn any pollers yet. Fails
  // with InvalidArgument if the start mode does not fit the stream's snapshot
  // mode: kSnapshot needs a stream that takes snapshots, and a stream whose
  // snapshot mode is INITIAL_ONLY must be consumed with kSnapshot (each
  // tablet's poller then stops in kSnapshotComplete after its snapshot).
  static Status Create(CDCClient* client,
                       Options options,
                       std::unique_ptr<CDCConsumer>* consumer);

  ~CDCConsumer();

  // Discovers tablets and spawns one poller per tablet. Returns immediately;
  // records are delivered on background threads via 'cb'. In kNow mode, the
  // position each tablet starts from is checkpointed as soon as its poller
  // knows it: when the poller starts, if Start() could read the tablet's tail,
  // or else after its first read. The tablet server persists the checkpoint on
  // the master asynchronously, so a consumer that stops before that resumes
  // from a later tail. On failure, nothing is left running.
  //
  // Without Options::checkpoint_enabled, fails with IllegalState if a tablet's
  // snapshot is in progress.
  Status Start(RecordCallback cb);

  // Signals all pollers to stop and joins them. Idempotent. Called from a
  // callback (on a poller thread), it only signals the pollers to stop and
  // returns: a later Stop() from another thread, or the destructor, joins
  // them.
  void Stop();

  // Checkpoints every tablet at its current position now, and returns an error
  // if any tablet could not be checkpointed (for example, its leader is not
  // known or did not accept the checkpoint). Safe to call while running. A
  // checkpoint the tablet server accepted is persisted on the master
  // asynchronously. A no-op if Options::checkpoint_enabled is false.
  Status Flush();

  // Restarts the poller of a tablet that stopped in a terminal state. With
  // 'snapshot', the tablet starts over with a new snapshot (the stream must
  // allow snapshots), even if its snapshot is unfinished: the new snapshot is
  // read at a new timestamp and does not tell which of the rows delivered
  // before were deleted meanwhile, so the sink must replace the tablet's rows
  // with it. Otherwise it resumes from where the poller stopped: a poller that
  // stopped in its snapshot takes that snapshot again at its snapshot
  // timestamp, from the start of the tablet, and stops in kNeedsResnapshot if
  // that is no longer possible.
  // Returns NotFound if the tablet is not polled and IllegalState if its poller
  // is not in a terminal state, or if called from a callback.
  Status RestartTablet(const std::string& tablet_id, bool snapshot);

  // Returns a snapshot of per-tablet progress.
  void GetProgress(std::vector<CDCTabletProgress>* out) const;

  // Re-enumerates the stream's tablets from the master and spawns a new
  // CDCTabletPoller for each tablet that does not already have one. Used to
  // discover tablets added via ADD_RANGE_PARTITION after Start() was called.
  // Also removes the pollers that stopped in kDropped: their tablets are not
  // polled again by this consumer.
  // A new poller resumes from the tablet's durable checkpoint if it has one,
  // and otherwise starts from the tablet's first op (with a snapshot in
  // kSnapshot mode), whatever the start mode: the tablet did not exist when
  // the consumer started.
  //
  // Safe to call concurrently with Stop(): once Stop() began, it neither adds
  // nor removes a poller. Returns NotFound if the consumer has not been
  // started, Aborted if Stop() began while it ran, or propagates
  // GetTabletLocations / GetStreamInfo errors.
  //
  // A background thread calls this automatically when
  // --cdc_tablet_rescan_interval_ms > 0 (the default). Applications may also
  // call it manually when they know a partition was added.
  Status ReScan();

  // Decodes a single record PB with the schemas of 'schemas': the values of a
  // row record with the schema version they were written with (for a FULL
  // stream, 'image_schema_version'). Exposed for testing.
  static Status DecodeRecord(const CDCSchemaHistory& schemas,
                             const std::string& tablet_id,
                             const CDCRecordPB& pb,
                             CDCDecodedRecord* out);

  // What a consumer does about a CDC error: retry the request, take a new
  // snapshot, or stop.
  enum class ErrorAction { kRetry, kResnapshot, kFatal };

  // Classifies 'err' by its is_retryable/needs_resnapshot fields, or, if the
  // server did not set them, by its code. Exposed for testing.
  static ErrorAction ClassifyError(const CDCErrorPB& err);

  // Computes the next backoff value by doubling 'current' and adding jitter.
  // 'jitter_frac' must be in [0.0, 1.0): the returned value is in the range
  // [2*current, 3*current) nanoseconds, capped at 'max'.  Exposed as a static
  // so callers (including tests) can exercise the formula deterministically
  // without needing a live Random instance.  In production, CDCTabletPoller
  // passes rng_.NextDoubleFraction() so each poller drifts independently.
  static MonoDelta ComputeJitteredBackoff(MonoDelta current, MonoDelta max,
                                          double jitter_frac);

  // For tests: 'hook' runs in every ReScan() after its master RPCs, right
  // before it removes the pollers of dropped tablets. Not thread-safe: set it
  // before ReScan() can run (with the background rescan disabled).
  void SetReScanHookForTests(std::function<void()> hook) {
    rescan_hook_for_tests_ = std::move(hook);
  }

  // For tests: 'hook' runs on a poller's thread each time a GetChanges call of
  // the poller is rejected for its authz token, right before the poller gets a
  // new token. Not thread-safe: set it before Start().
  void SetAuthzTokenRejectedHookForTests(std::function<void()> hook) {
    authz_token_rejected_hook_for_tests_ = std::move(hook);
  }

 private:
  friend class CDCTabletPoller;

  CDCConsumer(CDCClient* client, Options options);

  // Why a poller cannot poll its tablet any more.
  struct TabletEnd {
    // The tablet's range partition was dropped and the tablet cannot be read
    // any more: its partition, and whether changes the consumer did not get
    // are lost.
    bool dropped = false;
    PartitionPB partition;
    std::string partition_description;
    bool tail_lost = false;
    // Otherwise, the error the poller stops with, such as a deleted stream.
    Status error;
  };

  // Called by pollers: resolve the current leader for a tablet, optionally
  // forcing a fresh lookup from the master. The leader of a tablet whose range
  // partition was dropped, which the master keeps for the stream to read the
  // rest of its changes, is one of its replicas, a different one on every
  // forced lookup. Returns OK without setting '*leader' if the tablet cannot be
  // read any more; then '*end', if not null, says why.
  Status ResolveLeader(const std::string& tablet_id, bool force, HostPort* leader,
                       TabletEnd* end = nullptr);

  // Called by ResolveLeader() for a tablet that is not in the locations of the
  // table, or whose table is gone ('table_status' is the error that looking up
  // the table's locations returned).
  Status ResolveTabletNotInTable(const std::string& tablet_id, const Status& table_status,
                                 HostPort* leader, TabletEnd* end);

  // Adds the tablets of 'info' whose range partition was dropped, and whose
  // changes the stream did not drain, to 'tablets', unless they are in it.
  void AddDroppedTablets(const CDCStreamInfo& info, std::vector<CDCTabletInfo>* tablets) const;

  // Whether the stream's snapshot mode is INITIAL_ONLY.
  bool initial_snapshot_only() const {
    return snapshot_mode_ == master::CDCStreamConfigPB::INITIAL_ONLY;
  }

  // Probes 'tablet_id' for its current committed op index so a "now" tail is
  // anchored deterministically at Start() time (rather than whenever the poller
  // happens to make its first request). Returns the committed index on success,
  // or the "start from now" sentinel if the probe fails -- in which case the
  // poller anchors on its first poll, as before.
  int64_t AnchorNow(const std::string& tablet_id);

  // Where a new poller for 'tablet_id' starts, given the stream's state 'info'
  // on the master. 'discovered_after_start' is true for a tablet found by
  // ReScan(). Sets '*anchor_now' if the start is the tablet's current tail, to
  // be found with AnchorNow().
  void ChooseStart(const CDCStreamInfo& info, const std::string& tablet_id,
                   bool discovered_after_start, int64_t* initial, bool* do_snapshot,
                   bool* anchor_now) const;

  // Whether the calling thread is the thread of one of this consumer's pollers.
  bool OnPollerThread() const;

  // Called by pollers before DecodeAndDeliver(): learns the schemas of the
  // DDL records of 'records'. If a value of 'records' has a column id above
  // every known one, also learns the table's current schema from the master:
  // an image of a FULL record carries the columns of the tablet's current
  // schema, which may be newer than every DDL record the consumer got. The
  // master is asked at most once a second while it does not have such a
  // column either (its ALTER is still in progress). Returns an
  // error, after which the poller fetches the batch again, only if the master
  // could not be reached.
  Status LearnSchemas(const std::string& tablet_id,
                      const google::protobuf::RepeatedPtrField<CDCRecordPB>& records);

  // Called by pollers: decodes 'records' and delivers the batch, with
  // 'checkpoint_op_index', to the user callback. A DDL record that only
  // carries the schema the poller asked for (need_schema_info) is not
  // delivered. Raises '*schema_version' to the newest schema version of the
  // records.
  Status DecodeAndDeliver(const std::string& tablet_id,
                          const google::protobuf::RepeatedPtrField<CDCRecordPB>& records,
                          int64_t checkpoint_op_index,
                          int64_t* last_delivered_op_index,
                          int64_t* schema_version);

  // Called by pollers to obtain the current authz token (may be empty on
  // unsecured clusters).
  Status GetAuthzToken(bool* has_token, security::SignedTokenPB* token);

  // Called by pollers whose 'rejected_token' was rejected by a tablet server:
  // gets a new token from the master, unless the current token is a newer one
  // already.
  Status RenewAuthzToken(const security::SignedTokenPB& rejected_token);

  CDCClient* const client_;
  const Options options_;

  std::string table_id_;
  master::CDCStreamConfigPB::SnapshotMode snapshot_mode_ = master::CDCStreamConfigPB::NEVER;

  // The schemas used for decoding: the table's schema at Create(), and those
  // of the DDL records.
  CDCSchemaHistory schemas_;

  // Serializes the master schema refreshes of LearnSchemas(). Guards
  // 'last_unresolved_refresh_': the time of the last refresh after which a
  // column id that triggered it was still unknown, or uninitialized.
  std::mutex schema_refresh_lock_;
  MonoTime last_unresolved_refresh_;

  // Serializes RenewAuthzToken().
  std::mutex authz_renew_lock_;
  // Cached authz token. Guarded by 'authz_lock_'.
  mutable simple_spinlock authz_lock_;
  bool has_authz_token_ = false;
  security::SignedTokenPB authz_token_;

  // tablet_id -> leader HostPort. Guarded by 'leader_lock_'.
  mutable simple_spinlock leader_lock_;
  std::map<std::string, HostPort> tablet_leaders_;
  // The number of forced lookups of the leader of a dropped tablet that the
  // master keeps, by tablet id, to try its replicas in turn. Guarded by
  // 'leader_lock_'.
  std::map<std::string, uint64_t> dropped_tablet_lookups_;

  RecordCallback callback_;

  // See SetReScanHookForTests().
  std::function<void()> rescan_hook_for_tests_;

  // See SetAuthzTokenRejectedHookForTests().
  std::function<void()> authz_token_rejected_hook_for_tests_;

  // Per-tablet change pollers. Mutations (Start fill, ReScan append and
  // removal of kDropped pollers, RestartTablet replace, Stop clear) are guarded
  // by pollers_lock_. Reads for iteration in Flush() and GetProgress() also
  // acquire pollers_lock_; Stop() joins the pollers without it (see
  // 'stopping_').
  mutable Mutex pollers_lock_;
  std::vector<std::unique_ptr<CDCTabletPoller>> pollers_;

  // Set (under pollers_lock_) by Stop() before it touches the pollers, and
  // cleared once they are gone. While it is set, pollers_ does not change
  // until Stop() clears it: ReScan() and RestartTablet() check it under
  // pollers_lock_ before every change and add, remove or replace no poller.
  bool stopping_ = false;

  // The tablets whose poller stopped in kDropped and was removed. Guarded by
  // pollers_lock_.
  std::unordered_set<std::string> dropped_tablets_;

  // Background rescan thread that calls ReScan() at the interval set by
  // --cdc_tablet_rescan_interval_ms. Null when disabled (interval <= 0).
  // Stopped and joined at the top of Stop() before pollers_ is touched.
  scoped_refptr<kudu::Thread> rescan_thread_;
  // Signals the rescan thread to exit. CountDown()'d by Stop().
  CountDownLatch rescan_stop_{1};

  // Guarded by pollers_lock_.
  bool started_ = false;

  DISALLOW_COPY_AND_ASSIGN(CDCConsumer);
};

}  // namespace cdc
}  // namespace kudu
