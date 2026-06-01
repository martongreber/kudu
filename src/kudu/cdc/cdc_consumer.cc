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

#include "kudu/cdc/cdc_consumer.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/repeated_ptr_field.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/int128.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

// Maximum number of consecutive automatic re-snapshots of a tablet. A
// re-snapshot delivers the rows the tablet has now as READ records, but nothing
// tells the callback which rows were deleted while the stream's position was
// lost, so it is off by default: the poller stops in state kNeedsResnapshot
// and the application decides (see CDCConsumer::RestartTablet()).
DEFINE_int32(cdc_max_resnapshot_attempts, 0,
             "Maximum number of consecutive automatic re-snapshots of a tablet "
             "by a CDC consumer after its stream position is lost (WAL_EXPIRED, "
             "HISTORY_EXPIRED, STREAM_EXPIRED or SNAPSHOT_SESSION_LOST). A "
             "re-snapshot does not tell the application which rows were deleted "
             "meanwhile. 0 disables it: the tablet's poller stops in the "
             "NEEDS_RESNAPSHOT state. The count resets after each successful "
             "re-snapshot.");
TAG_FLAG(cdc_max_resnapshot_attempts, advanced);

// How often the background rescan thread re-enumerates the table's tablets to
// pick up partitions added via ADD_RANGE_PARTITION after Start() was called.
// <= 0 disables the automatic background thread; applications must call
// ReScan() manually when they know a partition was added.
DEFINE_int32(cdc_tablet_rescan_interval_ms, 30 * 1000,
             "How often CDCConsumer re-enumerates the table's tablet list to "
             "discover range partitions added via ADD_RANGE_PARTITION after "
             "Start() was called. 0 or negative disables the automatic "
             "background rescan; applications must call ReScan() manually.");
TAG_FLAG(cdc_tablet_rescan_interval_ms, advanced);
TAG_FLAG(cdc_tablet_rescan_interval_ms, runtime);

using google::protobuf::RepeatedPtrField;
using kudu::clock::HybridClock;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cdc {

namespace {
// Sentinel from_op_index used to skip to the current tail: any value >= the
// tablet's committed index makes the server return an empty batch whose
// checkpoint_op_index is the current committed index.
constexpr int64_t kStartFromNow = std::numeric_limits<int64_t>::max();

// The minimum time between two master schema refreshes of LearnSchemas() while
// the master does not have a column that a record has.
const MonoDelta kMinSchemaRefreshInterval = MonoDelta::FromSeconds(1);

// Returns the maximum physical-time component (microseconds) of HybridClock
// timestamps across 'records'. Returns 0 if no record carries a timestamp.
// Mirrors the producer-side newest_phys_micros computation in cdc_service.cc.
uint64_t MaxPhysMicros(const RepeatedPtrField<CDCRecordPB>& records) {
  uint64_t max_phys = 0;
  for (const auto& r : records) {
    if (r.has_timestamp()) {
      max_phys = std::max(
          max_phys,
          HybridClock::GetPhysicalValueMicros(Timestamp(r.timestamp())));
    }
  }
  return max_phys;
}

// Renders 'bytes', a serialized value of 'col', for humans.
Status FormatCell(const ColumnSchema& col, const string& bytes, string* out) {
  out->clear();
  const TypeInfo* ti = col.type_info();
  if (ti->physical_type() == BINARY) {
    Slice s(bytes);
    ti->AppendDebugStringForValue(&s, out);
    return Status::OK();
  }
  if (bytes.size() != ti->size()) {
    return Status::Corruption(Substitute(
        "unexpected serialized size for column '$0': got $1, expected $2",
        col.name(), bytes.size(), ti->size()));
  }
  // The bytes of a std::string may not be aligned for the type.
  alignas(int128_t) uint8_t cell[sizeof(int128_t)];
  DCHECK_LE(bytes.size(), sizeof(cell));
  memcpy(cell, bytes.data(), bytes.size());
  switch (ti->type()) {
    case DECIMAL32:
      *out = DecimalToString(*reinterpret_cast<const int32_t*>(cell),
                             col.type_attributes().scale);
      break;
    case DECIMAL64:
      *out = DecimalToString(*reinterpret_cast<const int64_t*>(cell),
                             col.type_attributes().scale);
      break;
    case DECIMAL128:
      *out = DecimalToString(*reinterpret_cast<const int128_t*>(cell),
                             col.type_attributes().scale);
      break;
    default:
      ti->AppendDebugStringForValue(cell, out);
      break;
  }
  return Status::OK();
}

void DecodeColumn(const CDCSchemaHistory& schemas,
                  uint32_t schema_version,
                  const CDCColumnValuePB& cv,
                  CDCDecodedColumn* out) {
  out->name = cv.column_name();
  out->column_id = cv.has_column_id() ? cv.column_id() : -1;
  out->is_null = cv.is_null();
  out->raw_value = cv.value();
  out->value.clear();
  const ColumnSchema* col = schemas.FindColumn(schema_version, cv);
  if (!col) {
    out->decode_ok = false;
    return;
  }
  out->type_info = col->type_info();
  out->type_attributes = col->type_attributes();
  out->decode_ok = true;
  if (out->is_null || !cv.has_value()) {
    return;
  }
  Status s = FormatCell(*col, cv.value(), &out->value);
  if (!s.ok()) {
    KLOG_EVERY_N_SECS(WARNING, 60) << s.ToString();
    out->value.clear();
    out->decode_ok = false;
  }
}

// Whether 'pb' is the DDL record that a GetChanges call with need_schema_info
// prepends: it changes no schema.
bool IsSchemaInfoRecord(const CDCRecordPB& pb) {
  return pb.op_type() == DDL && pb.has_new_schema_version() &&
         pb.schema_version() == pb.new_schema_version();
}

// Whether 'pb' carries the values of a row.
bool IsRowRecord(const CDCRecordPB& pb) {
  switch (pb.op_type()) {
    case INSERT:
    case UPDATE:
    case UPSERT:
    case DELETE:
    case READ:
      return true;
    default:
      return false;
  }
}
}  // anonymous namespace

bool IsTerminal(CDCTabletState state) {
  return state == CDCTabletState::kFailed || state == CDCTabletState::kNeedsResnapshot;
}

bool IsFinished(CDCTabletState state) {
  return IsTerminal(state) || state == CDCTabletState::kDropped ||
         state == CDCTabletState::kSnapshotComplete;
}

const char* CDCTabletStateToString(CDCTabletState state) {
  switch (state) {
    case CDCTabletState::kSnapshotting: return "SNAPSHOTTING";
    case CDCTabletState::kStreaming: return "STREAMING";
    case CDCTabletState::kStopped: return "STOPPED";
    case CDCTabletState::kFailed: return "FAILED";
    case CDCTabletState::kNeedsResnapshot: return "NEEDS_RESNAPSHOT";
    case CDCTabletState::kDropped: return "DROPPED";
    case CDCTabletState::kSnapshotComplete: return "SNAPSHOT_COMPLETE";
  }
  return "UNKNOWN";
}

// ===========================================================================
// CDCTabletPoller: per-tablet state machine (declared here, used by CDCConsumer)
// ===========================================================================

// One instance per tablet. Runs a dedicated thread that drives GetChanges,
// delivers decoded records, and periodically checkpoints, handling leader
// failover and expiry.
class CDCTabletPoller {
 public:
  // With 'checkpoint_start', the poller checkpoints the first position it
  // knows (the tail it starts from) right away rather than after
  // Options::checkpoint_interval.
  CDCTabletPoller(CDCConsumer* consumer,
                  string tablet_id,
                  int64_t initial_from_op,
                  bool do_snapshot,
                  bool checkpoint_start);
  ~CDCTabletPoller();

  // Returns the tablet id this poller is polling.
  const string& tablet_id() const { return tablet_id_; }

  // The poller's thread; null until Start() succeeded.
  const Thread* thread() const { return thread_.get(); }

  // Whether the poller's thread is done: Run(), including the stop callback
  // and the acknowledgement of a drop, returned.
  bool run_done() const { return run_done_.load(); }

  // Spawns the background thread.
  Status Start();

  // Makes the poller's snapshot a new one even if an earlier snapshot of the
  // tablet is unfinished (see GetChangesRequestPB.new_snapshot_generation).
  // Without it, the poller continues an unfinished snapshot at its snapshot
  // timestamp. Call before Start().
  void set_new_snapshot_generation() { new_snapshot_generation_ = true; }

  // Requests the poller to stop (non-blocking).
  void RequestStop();

  // Joins the background thread. Must be preceded by RequestStop(), or the
  // poller must have stopped by itself.
  void Join();

  // Checkpoints the current position now. If that fails, looks the tablet's
  // leader up again and retries once.
  Status ForceCheckpoint();

  void GetProgress(CDCTabletProgress* out) const;

  // Where a new poller for the tablet should start after this one stopped:
  // its position, and whether it stopped before its snapshot was done.
  void GetRestartPoint(int64_t* from_op, bool* in_snapshot) const;

 private:
  // kEnded: the tablet cannot be polled any more, see 'end_'.
  enum class PollOutcome { kOk, kRetry, kFatal, kResnapshot, kEnded };

  void Run();
  // Resolves the tablet's leader, retrying with backoff; with 'force', asks
  // the master even if a leader is cached. Returns false if the poller was
  // asked to stop, or if the tablet cannot be polled any more ('ended_').
  bool ResolveLeaderWithRetry(bool force);
  // Called when GetChanges returned TABLET_DROPPED for 'dropped': moves to the
  // tablet's last op and ends the poller ('ended_'). Finish() then reports the
  // drop and acknowledges it with AckDrop().
  void EndDropped(const CDCDroppedTabletPB& dropped);
  // Checkpoints the tablet's last op with CheckpointRequestPB.drop_applied, so
  // that the master may delete the tablet. A few attempts: if they fail, the
  // master deletes the tablet once --cdc_dropped_tablet_retention_secs passed.
  void AckDrop();
  Status RunSnapshotPhase();
  // 'have_more' is set to true when the server's response carries
  // have_more_records=true, signalling that more WAL data is immediately
  // available and the poll loop should skip idle backoff.
  PollOutcome PollOnce(bool* got_records, bool* have_more);
  // Acts on a CDC error in a GetChanges response and returns what to do next.
  PollOutcome HandleCdcError(const CDCErrorPB& err);
  // Acts on a failed GetChanges RPC, which carried 'authz_token' and returned
  // 's' and the RPC error code 'rpc_error', and returns what to do next:
  // kRetry, or kFatal if the caller is not authorized to read the table.
  PollOutcome HandleRpcFailure(const Status& s, rpc::ErrorStatusPB::RpcErrorCodePB rpc_error,
                               const security::SignedTokenPB& authz_token);
  // Counts a failed GetChanges call and records its error.
  void RecordError(const Status& s);
  // Checkpoints the current position, if it advanced and either 'force' is set
  // or Options::checkpoint_interval passed since the last checkpoint. With
  // 'resend', also a position that was checkpointed already. With
  // 'drop_applied', see CheckpointRequestPB.drop_applied.
  Status MaybeCheckpoint(bool force, bool resend = false, bool drop_applied = false);
  // With 'checkpoint_start_', checkpoints the start position once it is known.
  void MaybeCheckpointStart();
  // Ends Run(): checkpoints the current position, moves to 'state' and, if the
  // poller stopped by itself (see IsFinished()), reports it to
  // Options::on_tablet_stopped. The drop of a tablet that GetChanges reported
  // dropped is acknowledged only after that.
  void Finish(CDCTabletState state, const Status& error);
  // Ends Run() after the poller was asked to stop, or after the tablet could
  // not be polled any more ('ended_').
  void FinishStopped();
  void SetState(CDCTabletState state);
  bool needs_resnapshot() const;
  HostPort leader() const;
  void set_leader(const HostPort& hp);
  // Interruptible sleep; returns true if the poller was asked to stop.
  bool SleepFor(const MonoDelta& d);
  void GrowBackoff();
  void ResetBackoff();

  CDCConsumer* const consumer_;
  const string tablet_id_;
  const bool do_snapshot_;

  CountDownLatch stop_latch_;
  std::atomic<bool> stopping_;
  std::atomic<bool> run_done_;
  scoped_refptr<Thread> thread_;

  // Only accessed on the poller thread.
  MonoDelta backoff_;
  // Per-poller PRNG for backoff jitter.  Each poller is seeded independently
  // at construction time so a fleet of pollers does not retry in lockstep.
  Random rng_;
  // Checkpoint the first known position as soon as it is known.
  bool checkpoint_start_;
  // The next snapshot start asks for a new snapshot (see
  // set_new_snapshot_generation()). Set too before every snapshot that the
  // poller takes after the stream's position on the tablet was lost
  // (--cdc_max_resnapshot_attempts).
  bool new_snapshot_generation_ = false;
  // Ask for the schema of the ops after the position in the next GetChanges
  // call: on the first call, so that the records decode with the schema they
  // were written with even if the consumer resumes before an ALTER, and after
  // a new snapshot or a SCHEMA_VERSION_MISMATCH error.
  bool need_schema_info_ = true;
  // The newest schema version of the tablet's records so far, or -1. Sent as
  // GetChangesRequestPB.schema_version.
  int64_t schema_version_ = -1;
  // Whether a GetChanges call passed the authorization with the current authz
  // token.
  bool authz_token_accepted_ = false;
  // Whether the tablet cannot be polled any more, and why.
  bool ended_ = false;
  CDCConsumer::TabletEnd end_;
  // Whether GetChanges reported the tablet dropped: Finish() acknowledges the
  // drop after reporting it.
  bool drop_to_ack_ = false;

  // Serializes MaybeCheckpoint(), which runs on the poller thread and, through
  // Flush(), on the caller's. Guards 'last_checkpoint_time_'.
  Mutex checkpoint_lock_;
  MonoTime last_checkpoint_time_;

  // Guarded by 'progress_lock_'. 'leader_' is cleared on an error to look the
  // leader up again.
  mutable simple_spinlock progress_lock_;
  HostPort leader_;
  CDCTabletState state_;
  int64_t from_op_;
  int64_t last_delivered_op_;
  int64_t last_checkpointed_op_;
  bool snapshot_done_;
  bool needs_resnapshot_;
  // Whether the poller stopped before its snapshot was done.
  bool stopped_in_snapshot_;
  Status last_error_;
  Status terminal_error_;
  int64_t num_errors_;
  // See CDCTabletProgress.
  PartitionPB dropped_partition_;
  string dropped_partition_description_;
  bool dropped_tail_lost_ = false;
  // Physical timestamp (microseconds, HybridClock physical component) of the
  // last record this poller has successfully applied (DecodeAndDeliver'd).
  // 0 means no record has been applied yet.
  // Reported to the server via CheckpointRequestPB.last_applied_phystime_micros
  // in MaybeCheckpoint so the server can compute end-to-end committed lag.
  // Updated from the raw CDCRecordPB.timestamp field using
  // HybridClock::GetPhysicalValueMicros, matching the producer's convention.
  uint64_t last_applied_phys_micros_;

  DISALLOW_COPY_AND_ASSIGN(CDCTabletPoller);
};

CDCTabletPoller::CDCTabletPoller(CDCConsumer* consumer,
                                 string tablet_id,
                                 int64_t initial_from_op,
                                 bool do_snapshot,
                                 bool checkpoint_start)
    : consumer_(consumer),
      tablet_id_(std::move(tablet_id)),
      do_snapshot_(do_snapshot),
      stop_latch_(1),
      stopping_(false),
      run_done_(false),
      backoff_(consumer->options_.min_poll_backoff),
      rng_(GetRandomSeed32()),
      checkpoint_start_(checkpoint_start),
      last_checkpoint_time_(MonoTime::Now()),
      state_(do_snapshot ? CDCTabletState::kSnapshotting : CDCTabletState::kStreaming),
      from_op_(initial_from_op),
      last_delivered_op_(-1),
      last_checkpointed_op_(-1),
      snapshot_done_(false),
      needs_resnapshot_(false),
      stopped_in_snapshot_(false),
      num_errors_(0),
      last_applied_phys_micros_(0) {}

CDCTabletPoller::~CDCTabletPoller() {
  // Destroying a poller from a callback would free it while its thread runs.
  DCHECK(!thread_ || Thread::current_thread() != thread_.get());
  RequestStop();
  Join();
}

Status CDCTabletPoller::Start() {
  return Thread::Create("cdc_consumer", Substitute("poller-$0", tablet_id_),
                        [this]() {
                          this->Run();
                          run_done_.store(true);
                        }, &thread_);
}

void CDCTabletPoller::RequestStop() {
  stopping_.store(true);
  stop_latch_.CountDown();
}

void CDCTabletPoller::Join() {
  if (thread_) {
    thread_->Join();
  }
}

bool CDCTabletPoller::SleepFor(const MonoDelta& d) {
  // WaitFor returns true if the latch reached zero (stop requested).
  return stop_latch_.WaitFor(d);
}

void CDCTabletPoller::GrowBackoff() {
  // Delegate to the static helper with this poller's PRNG so the jitter
  // fraction differs per poller and per call, decorrelating retry schedules.
  backoff_ = CDCConsumer::ComputeJitteredBackoff(
      backoff_, consumer_->options_.max_poll_backoff, rng_.NextDoubleFraction());
}

void CDCTabletPoller::ResetBackoff() {
  backoff_ = consumer_->options_.min_poll_backoff;
}

HostPort CDCTabletPoller::leader() const {
  std::lock_guard<simple_spinlock> l(progress_lock_);
  return leader_;
}

void CDCTabletPoller::set_leader(const HostPort& hp) {
  std::lock_guard<simple_spinlock> l(progress_lock_);
  leader_ = hp;
}

void CDCTabletPoller::SetState(CDCTabletState state) {
  std::lock_guard<simple_spinlock> l(progress_lock_);
  state_ = state;
}

bool CDCTabletPoller::needs_resnapshot() const {
  std::lock_guard<simple_spinlock> l(progress_lock_);
  return needs_resnapshot_;
}

void CDCTabletPoller::RecordError(const Status& s) {
  std::lock_guard<simple_spinlock> l(progress_lock_);
  last_error_ = s;
  num_errors_++;
}

bool CDCTabletPoller::ResolveLeaderWithRetry(bool force) {
  while (!stopping_.load()) {
    HostPort hp;
    CDCConsumer::TabletEnd end;
    Status s = consumer_->ResolveLeader(tablet_id_, force, &hp, &end);
    if (s.ok() && hp.Initialized()) {
      set_leader(hp);
      return true;
    }
    if (s.ok()) {
      end_ = std::move(end);
      ended_ = true;
      return false;
    }
    {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = s.ok() ? Status::NotFound("no leader for tablet", tablet_id_) : s;
    }
    force = true;
    if (SleepFor(backoff_)) return false;
    GrowBackoff();
  }
  return false;
}

void CDCTabletPoller::Finish(CDCTabletState state, const Status& error) {
  // Persist the latest position, best effort.
  Status s = MaybeCheckpoint(/*force=*/true);
  if (!s.ok()) {
    VLOG(1) << "final checkpoint of tablet " << tablet_id_ << " failed: " << s.ToString();
  }
  {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    stopped_in_snapshot_ = state_ == CDCTabletState::kSnapshotting;
    state_ = state;
    terminal_error_ = error;
  }
  if (!IsFinished(state)) {
    return;
  }
  if (IsTerminal(state)) {
    LOG(WARNING) << Substitute("CDC poller of tablet $0 stopped ($1): $2",
                               tablet_id_, CDCTabletStateToString(state), error.ToString());
  } else if (state == CDCTabletState::kDropped) {
    LOG(INFO) << Substitute("CDC poller of tablet $0 stopped: its range partition $1 was "
                            "dropped$2", tablet_id_, dropped_partition_description_,
                            dropped_tail_lost_ ? ", and changes it did not get are lost" : "");
  }
  if (consumer_->options_.on_tablet_stopped) {
    CDCTabletProgress progress;
    GetProgress(&progress);
    consumer_->options_.on_tablet_stopped(progress);
  }
  if (state == CDCTabletState::kDropped && drop_to_ack_) {
    AckDrop();
  }
}

void CDCTabletPoller::FinishStopped() {
  if (!ended_) {
    Finish(CDCTabletState::kStopped, Status::OK());
    return;
  }
  if (!end_.dropped) {
    {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = end_.error;
    }
    Finish(CDCTabletState::kFailed, end_.error);
    return;
  }
  {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    dropped_partition_ = end_.partition;
    dropped_partition_description_ = end_.partition_description;
    dropped_tail_lost_ = end_.tail_lost;
  }
  Finish(CDCTabletState::kDropped, Status::OK());
}

void CDCTabletPoller::EndDropped(const CDCDroppedTabletPB& dropped) {
  {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    // The ops after the position, if any, change no rows.
    if (dropped.has_last_op_index() &&
        (from_op_ == kStartFromNow || from_op_ < dropped.last_op_index())) {
      from_op_ = dropped.last_op_index();
    }
    last_error_ = Status::OK();
  }
  end_ = CDCConsumer::TabletEnd();
  end_.dropped = true;
  end_.partition = dropped.partition();
  end_.partition_description = dropped.partition_description();
  ended_ = true;
  drop_to_ack_ = true;
}

void CDCTabletPoller::AckDrop() {
  // Also if the position was checkpointed before the partition was dropped.
  constexpr int kMaxAttempts = 5;
  for (int i = 0; consumer_->options_.checkpoint_enabled && i < kMaxAttempts; ++i) {
    Status s = MaybeCheckpoint(/*force=*/true, /*resend=*/true, /*drop_applied=*/true);
    if (s.ok()) {
      return;
    }
    LOG(WARNING) << Substitute("tablet $0: could not acknowledge the drop of the tablet: $1",
                               tablet_id_, s.ToString());
    HostPort hp;
    if (consumer_->ResolveLeader(tablet_id_, /*force=*/true, &hp).ok()) {
      set_leader(hp);
    }
    if (SleepFor(backoff_)) {
      return;
    }
    GrowBackoff();
  }
}

void CDCTabletPoller::MaybeCheckpointStart() {
  if (!checkpoint_start_) {
    return;
  }
  {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    if (from_op_ == kStartFromNow) {
      return;
    }
  }
  if (MaybeCheckpoint(/*force=*/true).ok()) {
    checkpoint_start_ = false;
  }
}

void CDCTabletPoller::Run() {
  if (consumer_->initial_snapshot_only() && !do_snapshot_) {
    // The snapshot of the tablet was done before, and it is the stream's only
    // output.
    Finish(CDCTabletState::kSnapshotComplete, Status::OK());
    return;
  }
  if (!ResolveLeaderWithRetry(/*force=*/false)) {
    FinishStopped();
    return;
  }
  ResetBackoff();
  // Checkpoint the start position, if Start() found it, so that a restart
  // resumes from it rather than from a later tail.
  MaybeCheckpointStart();

  if (do_snapshot_) {
    Status s = RunSnapshotPhase();
    if (stopping_.load() || ended_) {
      FinishStopped();
      return;
    }
    if (!s.ok()) {
      Finish(needs_resnapshot() ? CDCTabletState::kNeedsResnapshot
                                : CDCTabletState::kFailed, s);
      return;
    }
    if (consumer_->initial_snapshot_only()) {
      // Finish() checkpoints the streaming start point, which completes the
      // snapshot.
      Finish(CDCTabletState::kSnapshotComplete, Status::OK());
      return;
    }
    SetState(CDCTabletState::kStreaming);
  }

  // Counts consecutive resnapshot failures for this poller lifetime.
  // Reset to 0 after each successful re-bootstrap so the limit is per-incident.
  int resnapshot_attempts = 0;

  while (!stopping_.load()) {
    bool got_records = false;
    bool have_more = false;
    PollOutcome outcome = PollOnce(&got_records, &have_more);
    switch (outcome) {
      case PollOutcome::kOk:
        if (checkpoint_start_) {
          // The start position may be known only now.
          MaybeCheckpointStart();
        } else {
          Status s = MaybeCheckpoint(/*force=*/false);
          if (!s.ok()) {
            KLOG_EVERY_N_SECS(WARNING, 60) << s.ToString();
          }
        }
        // Skip idle backoff when the server signals more records are
        // immediately available (have_more_records=true), or when we just
        // received records.  In both cases reset backoff so the next poll
        // is as fast as possible.
        if (have_more || got_records) {
          ResetBackoff();
        } else {
          if (SleepFor(backoff_)) break;
          GrowBackoff();
        }
        break;
      case PollOutcome::kRetry:
        if (SleepFor(backoff_)) break;
        GrowBackoff();
        break;
      case PollOutcome::kResnapshot: {
        // Re-bootstrap up to cdc_max_resnapshot_attempts times with
        // exponential backoff.
        Status cause;
        {
          std::lock_guard<simple_spinlock> l(progress_lock_);
          cause = last_error_;
        }
        resnapshot_attempts++;
        if (resnapshot_attempts > FLAGS_cdc_max_resnapshot_attempts ||
            !consumer_->options_.checkpoint_enabled) {
          Finish(CDCTabletState::kNeedsResnapshot, cause);
          return;
        }
        LOG(WARNING) << "tablet " << tablet_id_
                     << " WAL/history expired; scheduling re-snapshot "
                     << "(attempt " << resnapshot_attempts << " of "
                     << FLAGS_cdc_max_resnapshot_attempts << ").";
        // Apply backoff before re-attempting so a flapping server is not
        // hammered.  GrowBackoff also adds jitter to decorrelate
        // concurrent pollers that all hit expiry at the same time.
        if (SleepFor(backoff_)) {
          Finish(CDCTabletState::kStopped, Status::OK());
          return;
        }
        GrowBackoff();
        // Reset the position for a clean re-bootstrap. 'needs_resnapshot_'
        // stays set until the snapshot is done. The embedder allowed a new
        // snapshot with --cdc_max_resnapshot_attempts.
        new_snapshot_generation_ = true;
        {
          std::lock_guard<simple_spinlock> l(progress_lock_);
          from_op_ = 0;
          snapshot_done_ = false;
          state_ = CDCTabletState::kSnapshotting;
        }
        // Re-resolve the leader: it may have changed since the expiry.
        if (!ResolveLeaderWithRetry(/*force=*/true)) {
          FinishStopped();
          return;
        }
        Status s = RunSnapshotPhase();
        if (stopping_.load() || ended_) {
          FinishStopped();
          return;
        }
        if (!s.ok()) {
          Finish(needs_resnapshot() ? CDCTabletState::kNeedsResnapshot
                                    : CDCTabletState::kFailed,
                 s.CloneAndPrepend(Substitute("new snapshot after '$0' failed",
                                              cause.ToString())));
          return;
        }
        SetState(CDCTabletState::kStreaming);
        need_schema_info_ = true;
        // Re-bootstrap succeeded.  Reset backoff and attempt counter so a
        // future expiry starts a new per-incident retry budget.
        ResetBackoff();
        resnapshot_attempts = 0;
        break;  // continue the main poll loop
      }
      case PollOutcome::kEnded:
        FinishStopped();
        return;
      case PollOutcome::kFatal: {
        // Truly unrecoverable (TRANSACTION_TOO_LARGE, RESPONSE_TOO_LARGE,
        // STREAM_NOT_FOUND, a callback error, etc.): operator action required.
        Status error;
        {
          std::lock_guard<simple_spinlock> l(progress_lock_);
          error = last_error_;
        }
        Finish(CDCTabletState::kFailed, error);
        return;
      }
    }
  }
  // Clean shutdown.
  Finish(CDCTabletState::kStopped, Status::OK());
}

Status CDCTabletPoller::RunSnapshotPhase() {
  DCHECK(consumer_->options_.checkpoint_enabled);
  bool first = true;
  // The resume key of the last page. It can be empty (see
  // GetChangesResponsePB.snapshot_resume_key): 'first' tells whether there is
  // one.
  string resume_key;
  // Counts full restarts triggered from within this phase. A leader change
  // mid-snapshot is NOT counted here: it surfaces as a transport error or
  // TABLET_NOT_LEADER (kRetry) and is retried with the resume key preserved,
  // so the new leader rebuilds the session and continues. This budget only
  // bounds genuine SNAPSHOT_SESSION_LOST restarts (history irrecoverably
  // gone), matching the main loop's kResnapshot budget.
  int resnapshot_attempts = 0;
  while (!stopping_.load()) {
    if (!leader().Initialized() && !ResolveLeaderWithRetry(/*force=*/true)) {
      return Status::Aborted("stopped");
    }
    bool has_token = false;
    security::SignedTokenPB token;
    RETURN_NOT_OK(consumer_->GetAuthzToken(&has_token, &token));

    GetChangesRequestPB req;
    req.set_stream_id(consumer_->options_.stream_id);
    req.set_tablet_id(tablet_id_);
    req.set_from_op_index(0);
    if (consumer_->options_.max_bytes_per_response > 0) {
      req.set_max_bytes(consumer_->options_.max_bytes_per_response);
    }
    req.set_is_snapshot_start(first);
    if (first && new_snapshot_generation_) {
      req.set_new_snapshot_generation(true);
    }
    if (!first) {
      req.set_snapshot_resume_key(resume_key);
    }
    if (has_token) {
      *req.mutable_authz_token() = token;
    }

    GetChangesResponsePB resp;
    rpc::ErrorStatusPB::RpcErrorCodePB rpc_error;
    Status s = consumer_->client_->GetChanges(leader(), req, &resp, &rpc_error);
    if (!s.ok()) {
      if (HandleRpcFailure(s, rpc_error, req.authz_token()) == PollOutcome::kFatal) {
        return s;
      }
      if (SleepFor(backoff_)) return Status::Aborted("stopped");
      GrowBackoff();
      continue;
    }
    authz_token_accepted_ = true;
    if (resp.has_error() && resp.error().code() == CDCErrorPB::TABLET_DROPPED) {
      // The snapshot of dropped rows is of no use.
      EndDropped(resp.dropped_tablet());
      return Status::Aborted("the tablet's range partition was dropped");
    }
    if (resp.has_error()) {
      RecordError(StatusFromPB(resp.error().status()));
      PollOutcome oc = HandleCdcError(resp.error());
      if (oc == PollOutcome::kRetry) {
        if (SleepFor(backoff_)) return Status::Aborted("stopped");
        GrowBackoff();
        continue;
      }
      if (oc == PollOutcome::kResnapshot) {
        // The snapshot cannot be continued: restart it from the beginning,
        // bounded by the same attempt budget as the main loop. Returning here
        // (as kFatal does) stops the poller.
        resnapshot_attempts++;
        if (resnapshot_attempts > FLAGS_cdc_max_resnapshot_attempts) {
          return StatusFromPB(resp.error().status());
        }
        LOG(WARNING) << "tablet " << tablet_id_
                     << " restarting snapshot from the beginning (attempt "
                     << resnapshot_attempts << " of "
                     << FLAGS_cdc_max_resnapshot_attempts << ").";
        if (SleepFor(backoff_)) return Status::Aborted("stopped");
        GrowBackoff();
        // Discard any partial progress and re-bootstrap cleanly, with a new
        // snapshot: the one in progress cannot be continued.
        first = true;
        new_snapshot_generation_ = true;
        resume_key.clear();
        {
          std::lock_guard<simple_spinlock> l(progress_lock_);
          from_op_ = 0;
          snapshot_done_ = false;
        }
        // The leader may have changed alongside the loss; re-resolve it.
        if (!ResolveLeaderWithRetry(/*force=*/true)) return Status::Aborted("stopped");
        ResetBackoff();
        continue;
      }
      return StatusFromPB(resp.error().status());
    }

    if (resp.records_size() > 0) {
      const Status ls = consumer_->LearnSchemas(tablet_id_, resp.records());
      if (!ls.ok()) {
        // Fetch the same page again.
        RecordError(ls);
        if (SleepFor(backoff_)) return Status::Aborted("stopped");
        GrowBackoff();
        continue;
      }
      int64_t delivered = -1;
      Status ds = consumer_->DecodeAndDeliver(tablet_id_, resp.records(),
                                              /*checkpoint_op_index=*/-1, &delivered,
                                              &schema_version_);
      if (!ds.ok()) {
        std::lock_guard<simple_spinlock> l(progress_lock_);
        last_error_ = ds;
        return ds;
      }
      // Track the max physical timestamp of successfully applied records so
      // MaybeCheckpoint can report end-to-end committed lag.
      const uint64_t batch_phys = MaxPhysMicros(resp.records());
      {
        std::lock_guard<simple_spinlock> l(progress_lock_);
        if (delivered >= 0) {
          last_delivered_op_ = delivered;
        }
        if (batch_phys > last_applied_phys_micros_) {
          last_applied_phys_micros_ = batch_phys;
        }
      }
    }

    if (resp.snapshot_done()) {
      const int64_t streaming_start =
          resp.has_snapshot_streaming_start_op_index()
              ? resp.snapshot_streaming_start_op_index()
              : 0;
      std::lock_guard<simple_spinlock> l(progress_lock_);
      from_op_ = streaming_start;
      snapshot_done_ = true;
      needs_resnapshot_ = false;
      last_error_ = Status::OK();
      return Status::OK();
    }
    if (PREDICT_FALSE(!resp.has_snapshot_resume_key())) {
      Status s = Status::IllegalState(
          "the tablet server returned a snapshot page that is not the last one "
          "without a resume key");
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = s;
      return s;
    }
    resume_key = resp.snapshot_resume_key();
    first = false;
    // The snapshot is established: a restart of it from here on continues it.
    new_snapshot_generation_ = false;
  }
  return Status::Aborted("stopped");
}

// static
CDCConsumer::ErrorAction CDCConsumer::ClassifyError(const CDCErrorPB& err) {
  // A server sets both fields on every error.
  if (err.has_is_retryable() || err.has_needs_resnapshot()) {
    if (err.is_retryable()) return ErrorAction::kRetry;
    if (err.needs_resnapshot()) return ErrorAction::kResnapshot;
    return ErrorAction::kFatal;
  }
  // An older server: classify by code, as a current server would.
  switch (err.code()) {
    case CDCErrorPB::TABLET_NOT_LEADER:
    case CDCErrorPB::TABLET_NOT_FOUND:
    case CDCErrorPB::TABLET_NOT_RUNNING:
    case CDCErrorPB::SERVER_TOO_BUSY:
    case CDCErrorPB::SCHEMA_VERSION_MISMATCH:
      return ErrorAction::kRetry;
    case CDCErrorPB::WAL_EXPIRED:
    case CDCErrorPB::HISTORY_EXPIRED:
    case CDCErrorPB::STREAM_EXPIRED:
    case CDCErrorPB::SNAPSHOT_SESSION_LOST:
    case CDCErrorPB::SCHEMA_UNAVAILABLE:
      return ErrorAction::kResnapshot;
    default:
      return ErrorAction::kFatal;
  }
}

CDCTabletPoller::PollOutcome CDCTabletPoller::HandleCdcError(const CDCErrorPB& err) {
  const Status status = StatusFromPB(err.status());
  switch (err.code()) {
    case CDCErrorPB::TABLET_NOT_LEADER:
    case CDCErrorPB::TABLET_NOT_FOUND:
    case CDCErrorPB::TABLET_NOT_RUNNING:
      // The leader moved or the tablet is transiently unavailable: look the
      // leader up again before the next call.
      set_leader(HostPort());
      break;
    case CDCErrorPB::SCHEMA_VERSION_MISMATCH:
      // Get the current schema (a DDL record) with the next call.
      need_schema_info_ = true;
      break;
    default:
      break;
  }
  switch (CDCConsumer::ClassifyError(err)) {
    case CDCConsumer::ErrorAction::kRetry:
      return PollOutcome::kRetry;
    case CDCConsumer::ErrorAction::kResnapshot: {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      needs_resnapshot_ = true;
      LOG(WARNING) << "tablet " << tablet_id_
                   << " requires a fresh snapshot: " << status.ToString();
      return PollOutcome::kResnapshot;
    }
    case CDCConsumer::ErrorAction::kFatal:
      break;
  }
  LOG(WARNING) << "tablet " << tablet_id_ << " fatal CDC error: " << status.ToString();
  return PollOutcome::kFatal;
}

CDCTabletPoller::PollOutcome CDCTabletPoller::HandleRpcFailure(
    const Status& s, rpc::ErrorStatusPB::RpcErrorCodePB rpc_error,
    const security::SignedTokenPB& authz_token) {
  RecordError(s);
  switch (rpc_error) {
    case rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN: {
      // The authz token expired, or it is missing or invalid: get a new one.
      // If the token was accepted before, it expired: retry soon, before the
      // new one expires too. Otherwise back off as on any other error, rather
      // than renew a token the server keeps rejecting in a tight loop.
      if (consumer_->authz_token_rejected_hook_for_tests_) {
        consumer_->authz_token_rejected_hook_for_tests_();
      }
      Status ts = consumer_->RenewAuthzToken(authz_token);
      if (!ts.ok()) {
        LOG(WARNING) << "tablet " << tablet_id_ << " could not refresh its authz token: "
                     << ts.ToString();
      } else if (authz_token_accepted_) {
        ResetBackoff();
      }
      authz_token_accepted_ = false;
      return PollOutcome::kRetry;
    }
    case rpc::ErrorStatusPB::FATAL_UNAUTHORIZED:
      LOG(WARNING) << "tablet " << tablet_id_ << " is not authorized for CDC: "
                   << s.ToString();
      return PollOutcome::kFatal;
    default:
      // The leader may be gone: look it up again.
      set_leader(HostPort());
      return PollOutcome::kRetry;
  }
}

CDCTabletPoller::PollOutcome CDCTabletPoller::PollOnce(bool* got_records,
                                                       bool* have_more) {
  *got_records = false;
  *have_more = false;

  // The leader is cleared only on an error: ask the master again.
  if (!leader().Initialized() && !ResolveLeaderWithRetry(/*force=*/true)) {
    return ended_ ? PollOutcome::kEnded : PollOutcome::kRetry;
  }

  bool has_token = false;
  security::SignedTokenPB token;
  Status ts = consumer_->GetAuthzToken(&has_token, &token);
  if (!ts.ok()) {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    last_error_ = ts;
    return PollOutcome::kRetry;
  }

  int64_t from;
  {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    from = from_op_;
  }

  GetChangesRequestPB req;
  req.set_stream_id(consumer_->options_.stream_id);
  req.set_tablet_id(tablet_id_);
  req.set_from_op_index(from);
  req.set_read_only(!consumer_->options_.checkpoint_enabled);
  if (consumer_->options_.max_bytes_per_response > 0) {
    req.set_max_bytes(consumer_->options_.max_bytes_per_response);
  }
  if (need_schema_info_) {
    req.set_need_schema_info(true);
  } else if (schema_version_ >= 0) {
    req.set_schema_version(static_cast<uint32_t>(schema_version_));
  }
  if (has_token) {
    *req.mutable_authz_token() = token;
  }

  GetChangesResponsePB resp;
  rpc::ErrorStatusPB::RpcErrorCodePB rpc_error;
  Status s = consumer_->client_->GetChanges(leader(), req, &resp, &rpc_error);
  if (!s.ok()) {
    return HandleRpcFailure(s, rpc_error, req.authz_token());
  }
  authz_token_accepted_ = true;
  if (resp.has_error() && resp.error().code() == CDCErrorPB::TABLET_DROPPED) {
    EndDropped(resp.dropped_tablet());
    return PollOutcome::kEnded;
  }
  if (resp.has_error()) {
    RecordError(StatusFromPB(resp.error().status()));
    return HandleCdcError(resp.error());
  }

  if (resp.records_size() > 0) {
    const Status ls = consumer_->LearnSchemas(tablet_id_, resp.records());
    if (!ls.ok()) {
      // Fetch the batch again.
      RecordError(ls);
      return PollOutcome::kRetry;
    }
  }
  need_schema_info_ = false;

  if (resp.records_size() > 0) {
    int64_t delivered = -1;
    Status ds = consumer_->DecodeAndDeliver(
        tablet_id_, resp.records(),
        resp.has_checkpoint_op_index() ? resp.checkpoint_op_index() : -1, &delivered,
        &schema_version_);
    if (!ds.ok()) {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = ds;
      return PollOutcome::kFatal;  // callback asked to stop or decode failed
    }
    // Track the max physical timestamp of successfully applied records so
    // MaybeCheckpoint can report end-to-end committed lag.
    const uint64_t batch_phys = MaxPhysMicros(resp.records());
    *got_records = true;
    {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      if (delivered >= 0) {
        last_delivered_op_ = delivered;
      }
      if (batch_phys > last_applied_phys_micros_) {
        last_applied_phys_micros_ = batch_phys;
      }
    }
  }

  // Advance the read position from the server-reported checkpoint.
  if (resp.has_checkpoint_op_index() && resp.checkpoint_op_index() >= 0) {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    from_op_ = resp.checkpoint_op_index();
    last_error_ = Status::OK();
  }

  // Propagate the server's have_more_records signal to the Run() loop so
  // it can skip idle backoff when more WAL data is immediately available.
  *have_more = resp.have_more_records();

  return PollOutcome::kOk;
}

Status CDCTabletPoller::MaybeCheckpoint(bool force, bool resend, bool drop_applied) {
  if (!consumer_->options_.checkpoint_enabled) {
    return Status::OK();
  }
  std::lock_guard<Mutex> cl(checkpoint_lock_);
  int64_t target;
  int64_t last_checkpointed;
  uint64_t applied_phys;
  HostPort leader;
  {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    target = from_op_;
    last_checkpointed = last_checkpointed_op_;
    applied_phys = last_applied_phys_micros_;
    leader = leader_;
  }
  // Never persist the "start from now" sentinel or a non-advancing position.
  if (target < 0 || target == kStartFromNow ||
      (target <= last_checkpointed && !(resend && target == last_checkpointed))) {
    return Status::OK();
  }
  if (!force) {
    const MonoDelta since = MonoTime::Now() - last_checkpoint_time_;
    if (since.ToNanoseconds() <
        consumer_->options_.checkpoint_interval.ToNanoseconds()) {
      return Status::OK();
    }
  }
  if (!leader.Initialized()) {
    return Status::ServiceUnavailable("no leader known for tablet", tablet_id_);
  }

  bool has_token = false;
  security::SignedTokenPB token;
  RETURN_NOT_OK(consumer_->GetAuthzToken(&has_token, &token));

  CheckpointRequestPB req;
  req.set_stream_id(consumer_->options_.stream_id);
  req.set_tablet_id(tablet_id_);
  req.set_op_index(target);
  if (drop_applied) {
    req.set_drop_applied(true);
  }
  if (has_token) {
    *req.mutable_authz_token() = token;
  }
  // Report the physical timestamp of the last applied
  // record so the server can compute end-to-end consumer committed lag. Only
  // set the field when at least one record has been applied (applied_phys > 0)
  // so old-protocol consumers that never deliver any record do not accidentally
  // overwrite a previously reported value with 0 on the server side.
  if (applied_phys > 0) {
    req.set_last_applied_phystime_micros(static_cast<int64_t>(applied_phys));
  }
  CheckpointResponsePB resp;
  rpc::ErrorStatusPB::RpcErrorCodePB rpc_error;
  Status s = consumer_->client_->Checkpoint(leader, req, &resp, &rpc_error);
  if (!s.ok()) {
    // The next checkpoint uses the new token.
    if (rpc_error == rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN) {
      WARN_NOT_OK(consumer_->RenewAuthzToken(req.authz_token()),
                  Substitute("tablet $0 could not refresh its authz token", tablet_id_));
    }
    return s.CloneAndPrepend(Substitute("checkpoint of tablet $0 failed", tablet_id_));
  }
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status()).CloneAndPrepend(
        Substitute("checkpoint of tablet $0 rejected", tablet_id_));
  }
  {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    last_checkpointed_op_ = target;
  }
  last_checkpoint_time_ = MonoTime::Now();
  return Status::OK();
}

Status CDCTabletPoller::ForceCheckpoint() {
  Status s = MaybeCheckpoint(/*force=*/true);
  if (s.ok()) {
    return s;
  }
  // The leader may have changed: look it up again and retry once.
  HostPort hp;
  if (consumer_->ResolveLeader(tablet_id_, /*force=*/true, &hp).ok() &&
      hp.Initialized()) {
    set_leader(hp);
    s = MaybeCheckpoint(/*force=*/true);
  }
  return s;
}

void CDCTabletPoller::GetProgress(CDCTabletProgress* out) const {
  std::lock_guard<simple_spinlock> l(progress_lock_);
  out->tablet_id = tablet_id_;
  out->state = state_;
  out->last_delivered_op_index = last_delivered_op_;
  out->last_checkpointed_op_index = last_checkpointed_op_;
  out->snapshot_done = snapshot_done_;
  out->needs_resnapshot = needs_resnapshot_;
  out->last_error = last_error_;
  out->terminal_error = terminal_error_;
  out->num_errors = num_errors_;
  out->dropped_partition = dropped_partition_;
  out->dropped_partition_description = dropped_partition_description_;
  out->dropped_tail_lost = dropped_tail_lost_;
}

void CDCTabletPoller::GetRestartPoint(int64_t* from_op, bool* in_snapshot) const {
  std::lock_guard<simple_spinlock> l(progress_lock_);
  *from_op = from_op_;
  *in_snapshot = stopped_in_snapshot_;
}

// ===========================================================================
// CDCConsumer
// ===========================================================================

CDCConsumer::CDCConsumer(CDCClient* client, Options options)
    : client_(client), options_(std::move(options)) {}

CDCConsumer::~CDCConsumer() {
  Stop();
}

Status CDCConsumer::Create(CDCClient* client,
                           Options options,
                           unique_ptr<CDCConsumer>* consumer) {
  if (options.stream_id.empty()) {
    return Status::InvalidArgument("stream_id is required");
  }
  if (!options.checkpoint_enabled && options.start_mode == kSnapshot) {
    return Status::InvalidArgument(
        "a consumer that does not checkpoint cannot start with a snapshot");
  }
  unique_ptr<CDCConsumer> c(new CDCConsumer(client, std::move(options)));

  CDCStreamInfo info;
  RETURN_NOT_OK_PREPEND(client->GetStreamInfo(c->options_.stream_id, &info),
                        "could not look up stream");
  if (info.table_ids.size() != 1) {
    return Status::NotSupported(Substitute(
        "stream $0 covers $1 tables, but a consumer follows exactly one",
        c->options_.stream_id, info.table_ids.size()));
  }
  c->table_id_ = info.table_ids.front();
  c->snapshot_mode_ = info.snapshot_mode;
  if (c->options_.start_mode == kSnapshot &&
      info.snapshot_mode == master::CDCStreamConfigPB::NEVER) {
    return Status::InvalidArgument(Substitute(
        "stream $0 takes no snapshots (snapshot mode NEVER): it cannot be consumed "
        "starting with a snapshot", c->options_.stream_id));
  }
  if (c->initial_snapshot_only() && c->options_.start_mode != kSnapshot) {
    return Status::InvalidArgument(Substitute(
        "the output of stream $0 is a snapshot only (snapshot mode INITIAL_ONLY): it "
        "must be consumed starting with a snapshot", c->options_.stream_id));
  }

  CDCTableMetadata md;
  RETURN_NOT_OK_PREPEND(client->GetTableMetadata(c->table_id_, /*by_id=*/true, &md),
                        "could not resolve stream's table");
  c->schemas_.AddColumns(md.schema);
  c->has_authz_token_ = md.has_authz_token;
  c->authz_token_ = md.authz_token;

  *consumer = std::move(c);
  return Status::OK();
}

void CDCConsumer::ChooseStart(const CDCStreamInfo& info,
                              const string& tablet_id,
                              bool discovered_after_start,
                              int64_t* initial,
                              bool* do_snapshot,
                              bool* anchor_now) const {
  *do_snapshot = false;
  *anchor_now = false;
  if (ContainsKey(info.tablets_in_snapshot, tablet_id)) {
    // An earlier consumer did not complete the tablet's snapshot: take it
    // again, regardless of start mode, before streaming the WAL.
    *initial = 0;
    *do_snapshot = true;
    return;
  }
  const auto it = info.tablet_checkpoints.find(tablet_id);
  if (it != info.tablet_checkpoints.end() && it->second > 0) {
    // Resume from the durable checkpoint regardless of start mode. On an
    // INITIAL_ONLY stream, the tablet's snapshot was done.
    *initial = it->second;
    return;
  }
  const auto* dropped = FindOrNull(info.dropped_tablets, tablet_id);
  if (dropped) {
    // The tablet's range partition was dropped: read the rest of its changes,
    // from its first op if it has no checkpoint. A snapshot of dropped rows is
    // of no use.
    *initial = dropped->has_op_index() ? dropped->op_index() : 0;
    return;
  }
  if (discovered_after_start ||
      ContainsKey(info.tablets_added_after_creation, tablet_id)) {
    // The tablet was added to the table after the consumer started, or after
    // the stream was created: every change it has is new, and the stream
    // retains its WAL from the first op.
    *initial = 0;
    *do_snapshot = options_.start_mode == kSnapshot;
    return;
  }
  switch (options_.start_mode) {
    case kNow:
      *initial = kStartFromNow;
      *anchor_now = true;
      break;
    case kEarliest:
      *initial = 0;
      break;
    case kSnapshot:
      *initial = 0;
      *do_snapshot = true;
      break;
  }
}

bool CDCConsumer::OnPollerThread() const {
  const Thread* current = Thread::current_thread();
  if (current == nullptr) {
    return false;
  }
  std::lock_guard<Mutex> l(pollers_lock_);
  for (const auto& p : pollers_) {
    if (p->thread() == current) {
      return true;
    }
  }
  return false;
}

Status CDCConsumer::Start(RecordCallback cb) {
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    if (started_ || stopping_ || !pollers_.empty()) {
      return Status::IllegalState("consumer already started");
    }
  }
  callback_ = std::move(cb);

  // Refresh durable checkpoints so resuming tablets pick up where they left off.
  CDCStreamInfo info;
  RETURN_NOT_OK(client_->GetStreamInfo(options_.stream_id, &info));

  vector<CDCTabletInfo> tablets;
  RETURN_NOT_OK_PREPEND(client_->GetTabletLocations(table_id_, &tablets),
                        "could not discover tablets");
  if (tablets.empty()) {
    return Status::NotFound("table has no tablets", table_id_);
  }
  AddDroppedTablets(info, &tablets);
  // One poller per tablet: the callback is never called concurrently for one
  // tablet, and a tablet's records are delivered in order.
  std::unordered_set<string> unique_ids;
  tablets.erase(std::remove_if(tablets.begin(), tablets.end(),
                               [&](const CDCTabletInfo& t) {
                                 return !InsertIfNotPresent(&unique_ids, t.tablet_id);
                               }),
                tablets.end());

  {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    for (const auto& t : tablets) {
      if (t.leader.Initialized()) {
        tablet_leaders_[t.tablet_id] = t.leader;
      }
    }
  }

  // Choose every tablet's start point before any poller runs; the probes for
  // the current tail are RPCs, so run them without holding pollers_lock_.
  vector<unique_ptr<CDCTabletPoller>> pollers;
  pollers.reserve(tablets.size());
  for (const auto& t : tablets) {
    if (!options_.checkpoint_enabled &&
        ContainsKey(info.tablets_in_snapshot, t.tablet_id)) {
      return Status::IllegalState(Substitute(
          "tablet $0 has a snapshot in progress, which a consumer that does not "
          "checkpoint cannot take", t.tablet_id));
    }
    int64_t initial;
    bool do_snapshot;
    bool anchor_now;
    ChooseStart(info, t.tablet_id, /*discovered_after_start=*/false,
                &initial, &do_snapshot, &anchor_now);
    if (anchor_now) {
      initial = AnchorNow(t.tablet_id);
    }
    pollers.emplace_back(new CDCTabletPoller(this, t.tablet_id, initial, do_snapshot,
                                             /*checkpoint_start=*/anchor_now));
  }

  Status s;
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    started_ = true;
    for (auto& p : pollers) {
      pollers_.emplace_back(std::move(p));
      s = pollers_.back()->Start();
      if (!s.ok()) {
        break;
      }
    }
  }

  // Start the background rescan thread if the interval is positive. The thread
  // periodically calls ReScan() to discover tablets added via ADD_RANGE_PARTITION
  // after Start() was called.
  if (s.ok() && FLAGS_cdc_tablet_rescan_interval_ms > 0) {
    // Re-arm the latch before each spawn so that a Stop()/Start() restart does
    // not leave the latch at 0, which would cause the new rescan thread to see
    // WaitFor() return true immediately and exit without ever scanning.
    rescan_stop_.Reset(1);
    s = kudu::Thread::Create(
        "cdc_consumer", "tablet_rescan",
        [this]() {
          while (true) {
            // Sleep for the configured interval, waking early if Stop() fires.
            bool stopped = rescan_stop_.WaitFor(
                MonoDelta::FromMilliseconds(FLAGS_cdc_tablet_rescan_interval_ms));
            if (stopped) {
              break;  // Stop() was called.
            }
            Status s = ReScan();
            // Aborted: Stop() began while the rescan ran.
            if (!s.ok() && !s.IsAborted()) {
              LOG(WARNING) << Substitute(
                  "CDCConsumer background rescan failed: $0 (will retry)",
                  s.ToString());
            }
          }
        },
        &rescan_thread_);
  }
  if (!s.ok()) {
    // Leave nothing running.
    Stop();
    return s;
  }
  return Status::OK();
}

void CDCConsumer::Stop() {
  if (OnPollerThread()) {
    // A thread cannot join itself: only ask everything to stop. The pollers
    // stay until a Stop() from another thread (or the destructor) joins them.
    std::lock_guard<Mutex> l(pollers_lock_);
    stopping_ = true;
    rescan_stop_.CountDown();
    for (auto& p : pollers_) {
      p->RequestStop();
    }
    return;
  }
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    if (!started_ && pollers_.empty()) {
      return;
    }
    // From here on, ReScan() adds no poller.
    stopping_ = true;
  }
  // Tear down the background rescan thread first.
  if (rescan_thread_) {
    rescan_stop_.CountDown();
    rescan_thread_->Join();
    rescan_thread_.reset();
  }

  // Signal all pollers to stop.
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    for (auto& p : pollers_) {
      p->RequestStop();
    }
  }
  // Join pollers without the lock so concurrent GetProgress() calls, including
  // the ones from callbacks, can still complete. 'stopping_' keeps pollers_
  // stable: ReScan() and RestartTablet() check it under the lock before they
  // add, remove or replace a poller.
  for (auto& p : pollers_) {
    p->Join();
  }
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    pollers_.clear();
    started_ = false;
    stopping_ = false;
  }
}

Status CDCConsumer::Flush() {
  std::lock_guard<Mutex> l(pollers_lock_);
  Status first_error;
  int num_failed = 0;
  for (auto& p : pollers_) {
    CDCTabletProgress progress;
    p->GetProgress(&progress);
    if (progress.state == CDCTabletState::kDropped ||
        progress.state == CDCTabletState::kSnapshotComplete) {
      // Checkpointed when it stopped; there may be no tablet to contact.
      continue;
    }
    Status s = p->ForceCheckpoint();
    if (!s.ok()) {
      num_failed++;
      if (first_error.ok()) {
        first_error = std::move(s);
      }
    }
  }
  if (num_failed > 0) {
    return first_error.CloneAndPrepend(Substitute(
        "$0 of $1 tablet(s) could not be checkpointed", num_failed, pollers_.size()));
  }
  return Status::OK();
}

void CDCConsumer::GetProgress(vector<CDCTabletProgress>* out) const {
  std::lock_guard<Mutex> l(pollers_lock_);
  out->clear();
  out->reserve(pollers_.size());
  for (const auto& p : pollers_) {
    CDCTabletProgress prog;
    p->GetProgress(&prog);
    out->emplace_back(std::move(prog));
  }
}

Status CDCConsumer::RestartTablet(const string& tablet_id, bool snapshot) {
  if (OnPollerThread()) {
    return Status::IllegalState("RestartTablet() cannot be called from a callback");
  }
  if (snapshot && !options_.checkpoint_enabled) {
    return Status::IllegalState("a consumer that does not checkpoint cannot take a snapshot");
  }
  CDCTabletPoller* old_poller = nullptr;
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    if (!started_ || stopping_) {
      return Status::IllegalState("consumer is not running");
    }
    for (const auto& p : pollers_) {
      if (p->tablet_id() == tablet_id) {
        old_poller = p.get();
        break;
      }
    }
    if (old_poller == nullptr) {
      return Status::NotFound("tablet is not polled", tablet_id);
    }
    CDCTabletProgress progress;
    old_poller->GetProgress(&progress);
    if (!IsTerminal(progress.state)) {
      return Status::IllegalState(Substitute(
          "the poller of tablet $0 is $1, not stopped on an error", tablet_id,
          CDCTabletStateToString(progress.state)));
    }
  }
  // The old poller's thread is done polling; it may still be running
  // Options::on_tablet_stopped. Only Stop() removes a poller in a terminal
  // state (ReScan() removes kDropped ones only), and Stop() runs on the
  // controlling thread, like this (a Stop() from a callback removes none), so
  // 'old_poller' stays valid.
  old_poller->Join();
  int64_t from_op;
  bool in_snapshot;
  old_poller->GetRestartPoint(&from_op, &in_snapshot);
  const bool do_snapshot = snapshot || in_snapshot;
  unique_ptr<CDCTabletPoller> poller(new CDCTabletPoller(
      this, tablet_id, do_snapshot ? 0 : from_op, do_snapshot,
      /*checkpoint_start=*/false));
  if (snapshot) {
    // The embedder asks for a new snapshot: the one in progress, if any, is
    // not continued.
    poller->set_new_snapshot_generation();
  }
  unique_ptr<CDCTabletPoller> replaced;
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    // A callback may have stopped the consumer during the Join() above.
    if (!started_ || stopping_) {
      return Status::IllegalState("consumer is not running");
    }
    for (auto& p : pollers_) {
      if (p.get() == old_poller) {
        RETURN_NOT_OK(poller->Start());
        replaced = std::move(p);
        p = std::move(poller);
        break;
      }
    }
  }
  LOG(INFO) << Substitute("CDCConsumer: restarted the poller of tablet $0$1",
                          tablet_id, do_snapshot ? " with a new snapshot" : "");
  return Status::OK();
}

Status CDCConsumer::ReScan() {
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    if (!started_ || stopping_) {
      return Status::NotFound("consumer not started");
    }
  }

  // Fetch the current tablet list and durable checkpoints from the master.
  // This is done outside pollers_lock_ to avoid holding the lock during RPCs.
  CDCStreamInfo info;
  RETURN_NOT_OK(client_->GetStreamInfo(options_.stream_id, &info));

  vector<CDCTabletInfo> tablets;
  RETURN_NOT_OK_PREPEND(client_->GetTabletLocations(table_id_, &tablets),
                        "ReScan: could not enumerate tablets");
  AddDroppedTablets(info, &tablets);

  if (rescan_hook_for_tests_) {
    rescan_hook_for_tests_();
  }

  // Remove the pollers of the tablets whose range partition was dropped, once
  // they are done. A poller still in Options::on_tablet_stopped, or still
  // acknowledging the drop, stays until a later rescan, so that this does not
  // wait for it and Stop() can still stop it.
  vector<unique_ptr<CDCTabletPoller>> done;
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    // Stop() may have started meanwhile: it joins the pollers without the
    // lock, so pollers_ must not change any more.
    if (!started_ || stopping_) {
      return Status::Aborted("consumer is stopping");
    }
    for (auto it = pollers_.begin(); it != pollers_.end();) {
      CDCTabletProgress progress;
      (*it)->GetProgress(&progress);
      if (progress.state != CDCTabletState::kDropped || !(*it)->run_done()) {
        ++it;
        continue;
      }
      dropped_tablets_.insert((*it)->tablet_id());
      done.emplace_back(std::move(*it));
      it = pollers_.erase(it);
    }
  }
  for (const auto& p : done) {
    // The thread exited, or is about to.
    p->Join();
  }
  if (!done.empty()) {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    for (const auto& p : done) {
      tablet_leaders_.erase(p->tablet_id());
      dropped_tablet_lookups_.erase(p->tablet_id());
    }
  }
  done.clear();

  // Update tablet_leaders_ for all tablets (new and existing).
  {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    for (const auto& t : tablets) {
      if (t.leader.Initialized()) {
        tablet_leaders_[t.tablet_id] = t.leader;
      }
    }
  }

  // Determine which tablet ids are already being polled, or were dropped.
  std::unordered_set<string> polled_ids;
  {
    std::lock_guard<Mutex> l(pollers_lock_);
    for (const auto& p : pollers_) {
      polled_ids.insert(p->tablet_id());
    }
    polled_ids.insert(dropped_tablets_.begin(), dropped_tablets_.end());
  }

  // For each tablet not yet being polled, create and start a new poller. The
  // tablet was created after the consumer started (ADD_RANGE_PARTITION), so
  // it starts from its first op unless it already has a durable checkpoint:
  // the master retains its WAL from there (its checkpoint row starts at 0).
  for (const auto& t : tablets) {
    if (ContainsKey(polled_ids, t.tablet_id)) {
      continue;  // Already being polled.
    }
    if (!options_.checkpoint_enabled &&
        ContainsKey(info.tablets_in_snapshot, t.tablet_id)) {
      KLOG_EVERY_N_SECS(WARNING, 60) << Substitute(
          "CDCConsumer ReScan: not polling new tablet $0 while its snapshot is in "
          "progress: a consumer that does not checkpoint cannot take it", t.tablet_id);
      continue;
    }
    int64_t initial;
    bool do_snapshot;
    bool anchor_now;
    ChooseStart(info, t.tablet_id, /*discovered_after_start=*/true,
                &initial, &do_snapshot, &anchor_now);
    DCHECK(!anchor_now);
    unique_ptr<CDCTabletPoller> poller(new CDCTabletPoller(
        this, t.tablet_id, initial, do_snapshot, /*checkpoint_start=*/false));
    {
      std::lock_guard<Mutex> l(pollers_lock_);
      if (!started_ || stopping_) {
        return Status::Aborted("consumer is stopping");
      }
      // A concurrent ReScan() may have added it meanwhile.
      bool polled = ContainsKey(dropped_tablets_, t.tablet_id);
      for (const auto& p : pollers_) {
        if (p->tablet_id() == t.tablet_id) {
          polled = true;
          break;
        }
      }
      if (polled) {
        continue;
      }
      Status s = poller->Start();
      if (!s.ok()) {
        LOG(WARNING) << Substitute(
            "CDCConsumer ReScan: failed to start poller for new tablet $0: $1 "
            "(will retry on next rescan)",
            t.tablet_id, s.ToString());
        continue;
      }
      pollers_.push_back(std::move(poller));
    }
    LOG(INFO) << Substitute(
        "CDCConsumer ReScan: started poller for new tablet $0 "
        "(ADD_RANGE_PARTITION discovery)",
        t.tablet_id);
  }
  return Status::OK();
}

int64_t CDCConsumer::AnchorNow(const string& tablet_id) {
  HostPort leader;
  {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    auto it = tablet_leaders_.find(tablet_id);
    if (it != tablet_leaders_.end()) {
      leader = it->second;
    }
  }
  if (!leader.Initialized()) {
    return kStartFromNow;
  }

  bool has_token = false;
  security::SignedTokenPB token;
  // Best-effort: on an unsecured cluster there is simply no token.
  WARN_NOT_OK(GetAuthzToken(&has_token, &token),
              "could not fetch authz token for now-anchor probe");

  // A from_op_index at or beyond the committed tail makes the server return an
  // empty batch whose checkpoint_op_index is the current committed index.
  GetChangesRequestPB req;
  req.set_stream_id(options_.stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(kStartFromNow);
  req.set_read_only(!options_.checkpoint_enabled);
  if (options_.max_bytes_per_response > 0) {
    req.set_max_bytes(options_.max_bytes_per_response);
  }
  if (has_token) {
    *req.mutable_authz_token() = token;
  }

  GetChangesResponsePB resp;
  Status s = client_->GetChanges(leader, req, &resp);
  if (s.ok() && !resp.has_error() && resp.has_checkpoint_op_index() &&
      resp.checkpoint_op_index() >= 0) {
    return resp.checkpoint_op_index();
  }
  // Probe failed (leader moved, transient error, ...): let the poller anchor on
  // its first successful poll instead.
  return kStartFromNow;
}

Status CDCConsumer::ResolveLeader(const string& tablet_id,
                                  bool force,
                                  HostPort* leader,
                                  TabletEnd* end) {
  if (!force) {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    auto it = tablet_leaders_.find(tablet_id);
    if (it != tablet_leaders_.end() && it->second.Initialized()) {
      *leader = it->second;
      return Status::OK();
    }
  }

  vector<CDCTabletInfo> tablets;
  const Status s = client_->GetTabletLocations(table_id_, &tablets);
  // NotFound: the table is gone.
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  bool in_table = false;
  {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    for (const auto& t : tablets) {
      in_table |= t.tablet_id == tablet_id;
      if (t.leader.Initialized()) {
        tablet_leaders_[t.tablet_id] = t.leader;
      } else {
        // The master knows no leader for it now: do not keep a stale one.
        tablet_leaders_.erase(t.tablet_id);
      }
    }
    auto it = tablet_leaders_.find(tablet_id);
    if (in_table && it != tablet_leaders_.end() && it->second.Initialized()) {
      *leader = it->second;
      return Status::OK();
    }
  }
  if (in_table) {
    return Status::NotFound("no leader currently known for tablet", tablet_id);
  }
  return ResolveTabletNotInTable(tablet_id, s, leader, end);
}

Status CDCConsumer::ResolveTabletNotInTable(const string& tablet_id,
                                            const Status& table_status,
                                            HostPort* leader,
                                            TabletEnd* end) {
  TabletEnd unused;
  if (!end) {
    end = &unused;
  }
  CDCStreamInfo info;
  Status s = client_->GetStreamInfo(options_.stream_id, &info);
  if (s.IsNotFound()) {
    end->error = table_status.ok()
        ? s.CloneAndPrepend("the stream was deleted")
        : s.CloneAndPrepend(Substitute("the stream was deleted: its table $0 was dropped",
                                       table_id_));
    return Status::OK();
  }
  RETURN_NOT_OK(s);
  const auto* dropped = FindOrNull(info.dropped_tablets, tablet_id);
  if (dropped && dropped->state() == master::CDCStreamDroppedTabletPB::RETAINED &&
      dropped->replicas_size() > 0) {
    // The master keeps the tablet for the stream, but does not know its leader:
    // try its replicas in turn.
    std::lock_guard<simple_spinlock> l(leader_lock_);
    const uint64_t n = dropped_tablet_lookups_[tablet_id]++;
    *leader = HostPortFromPB(dropped->replicas(static_cast<int>(n % dropped->replicas_size())));
    tablet_leaders_[tablet_id] = *leader;
    return Status::OK();
  }
  if (dropped && dropped->state() != master::CDCStreamDroppedTabletPB::RETAINED) {
    end->dropped = true;
    end->partition = dropped->partition();
    end->partition_description = dropped->partition_description();
    end->tail_lost = dropped->state() != master::CDCStreamDroppedTabletPB::DRAINED;
    return Status::OK();
  }
  if (dropped || !table_status.ok() ||
      ContainsKey(info.tablet_checkpoints, tablet_id) ||
      ContainsKey(info.tablets_in_snapshot, tablet_id)) {
    // The master still counts the tablet as one of the stream.
    return table_status.ok() ? Status::NotFound("no location known for tablet", tablet_id)
                             : table_status;
  }
  // The tablet is not in the table, and the master no longer knows it as a
  // tablet of the stream: it was dropped long ago.
  end->dropped = true;
  end->tail_lost = true;
  return Status::OK();
}

void CDCConsumer::AddDroppedTablets(const CDCStreamInfo& info,
                                    vector<CDCTabletInfo>* tablets) const {
  for (const auto& e : info.dropped_tablets) {
    // A tablet the stream drained has nothing to deliver.
    if (e.second.state() == master::CDCStreamDroppedTabletPB::DRAINED) {
      continue;
    }
    if (std::none_of(tablets->begin(), tablets->end(),
                     [&](const CDCTabletInfo& t) { return t.tablet_id == e.first; })) {
      CDCTabletInfo t;
      t.tablet_id = e.first;
      tablets->emplace_back(std::move(t));
    }
  }
}

Status CDCConsumer::DecodeAndDeliver(const string& tablet_id,
                                     const RepeatedPtrField<CDCRecordPB>& records,
                                     int64_t checkpoint_op_index,
                                     int64_t* last_delivered_op_index,
                                     int64_t* schema_version) {
  CDCRecordBatch batch;
  batch.tablet_id = tablet_id;
  batch.checkpoint_op_index = checkpoint_op_index;
  batch.records.reserve(records.size());

  for (const auto& pb : records) {
    if (pb.op_type() == DDL && pb.has_new_schema_version()) {
      *schema_version = std::max<int64_t>(*schema_version, pb.new_schema_version());
    } else if (IsRowRecord(pb) && pb.has_schema_version()) {
      *schema_version = std::max<int64_t>(*schema_version, pb.schema_version());
    }
    if (IsSchemaInfoRecord(pb)) {
      continue;
    }

    CDCDecodedRecord r;
    RETURN_NOT_OK(DecodeRecord(schemas_, tablet_id, pb, &r));
    if (r.op_index >= 0) {
      *last_delivered_op_index = r.op_index;
    }
    batch.records.emplace_back(std::move(r));
  }

  if (!batch.records.empty() && callback_) {
    return callback_(batch);
  }
  return Status::OK();
}

Status CDCConsumer::LearnSchemas(const string& tablet_id,
                                 const RepeatedPtrField<CDCRecordPB>& records) {
  int32_t max_value_column_id = -1;
  const auto add_values = [&](const RepeatedPtrField<CDCColumnValuePB>& cols) {
    for (const auto& cv : cols) {
      if (cv.has_column_id()) {
        max_value_column_id = std::max(max_value_column_id, cv.column_id());
      }
    }
  };
  for (const auto& pb : records) {
    if (pb.op_type() == DDL && pb.has_new_schema()) {
      Schema new_schema;
      Status s = SchemaFromPB(pb.new_schema(), &new_schema);
      if (s.ok()) {
        schemas_.AddVersion(pb.new_schema_version(), new_schema);
      } else {
        LOG(WARNING) << "could not decode DDL schema for tablet " << tablet_id
                     << ": " << s.ToString();
      }
    }
    add_values(pb.changes());
    add_values(pb.old_changes());
  }
  if (max_value_column_id <= schemas_.max_column_id()) {
    return Status::OK();
  }

  // Another poller may be refreshing for the same column.
  std::lock_guard<std::mutex> l(schema_refresh_lock_);
  if (max_value_column_id <= schemas_.max_column_id() ||
      (last_unresolved_refresh_.Initialized() &&
       MonoTime::Now() - last_unresolved_refresh_ < kMinSchemaRefreshInterval)) {
    return Status::OK();
  }
  CDCTableMetadata md;
  const Status s = client_->GetTableMetadata(table_id_, /*by_id=*/true, &md);
  if (s.IsNotFound() || s.IsNotAuthorized()) {
    // The master answered: decode what is known.
    LOG(WARNING) << Substitute("could not get the schema of tablet $0: $1",
                               tablet_id, s.ToString());
    last_unresolved_refresh_ = MonoTime::Now();
    return Status::OK();
  }
  RETURN_NOT_OK_PREPEND(s, Substitute("could not get the schema of tablet $0 from the master",
                                      tablet_id));
  schemas_.AddColumns(md.schema);
  last_unresolved_refresh_ = max_value_column_id > schemas_.max_column_id()
      ? MonoTime::Now() : MonoTime();
  return Status::OK();
}

Status CDCConsumer::RenewAuthzToken(const security::SignedTokenPB& rejected_token) {
  // The tokens of all pollers expire at once: get one new token for all.
  std::lock_guard<std::mutex> renew_lock(authz_renew_lock_);
  {
    std::lock_guard<simple_spinlock> l(authz_lock_);
    if (has_authz_token_ && authz_token_.token_data() != rejected_token.token_data()) {
      return Status::OK();
    }
  }
  CDCTableMetadata md;
  RETURN_NOT_OK(client_->GetTableMetadata(table_id_, /*by_id=*/true, &md));
  std::lock_guard<simple_spinlock> l(authz_lock_);
  has_authz_token_ = md.has_authz_token;
  authz_token_ = md.authz_token;
  return Status::OK();
}

Status CDCConsumer::GetAuthzToken(bool* has_token, security::SignedTokenPB* token) {
  std::lock_guard<simple_spinlock> l(authz_lock_);
  *has_token = has_authz_token_;
  if (has_authz_token_) {
    *token = authz_token_;
  }
  return Status::OK();
}

// static
MonoDelta CDCConsumer::ComputeJitteredBackoff(MonoDelta current, MonoDelta max,
                                              double jitter_frac) {
  // Double the current backoff, then add a jitter fraction of [0, cur_ns)
  // so the resulting value is in [2*cur_ns, 3*cur_ns) before capping.
  // Adding noise proportional to the current value decorrelates the retry
  // schedules of many pollers that all started backing off at the same time
  // (thundering-herd mitigation on a failed tserver).
  // jitter_frac is supplied by the caller (rng_.NextDoubleFraction() in
  // production, a fixed value in tests) so the formula is deterministic and
  // unit-testable without needing a live Random instance.
  int64_t cur_ns = current.ToNanoseconds();
  int64_t max_ns = max.ToNanoseconds();
  int64_t next_ns = static_cast<int64_t>(
      static_cast<double>(cur_ns) * (2.0 + jitter_frac));
  return MonoDelta::FromNanoseconds(next_ns > max_ns ? max_ns : next_ns);
}

Status CDCConsumer::DecodeRecord(const CDCSchemaHistory& schemas,
                                 const string& tablet_id,
                                 const CDCRecordPB& pb,
                                 CDCDecodedRecord* out) {
  out->op_type = pb.op_type();
  out->op_index = pb.has_op_index() ? pb.op_index() : -1;
  out->op_term = pb.has_op_term() ? pb.op_term() : -1;
  out->timestamp = pb.timestamp();
  out->schema_version = pb.schema_version();
  out->tablet_id = tablet_id;

  if (pb.has_commit_timestamp()) {
    out->has_commit_timestamp = true;
    out->commit_timestamp = pb.commit_timestamp();
  }
  if (pb.has_txn_id()) {
    out->has_txn_id = true;
    out->txn_id = pb.txn_id();
  }
  out->keep_immutable_columns_on_update = pb.keep_immutable_columns_on_update();

  // The images of a FULL record carry the columns of 'image_schema_version'.
  const uint32_t values_version =
      pb.has_image_schema_version() ? pb.image_schema_version() : pb.schema_version();
  out->after.reserve(pb.changes_size());
  for (const auto& cv : pb.changes()) {
    CDCDecodedColumn col;
    DecodeColumn(schemas, values_version, cv, &col);
    out->after.emplace_back(std::move(col));
  }
  out->before.reserve(pb.old_changes_size());
  for (const auto& cv : pb.old_changes()) {
    CDCDecodedColumn col;
    DecodeColumn(schemas, values_version, cv, &col);
    out->before.emplace_back(std::move(col));
  }

  if (pb.has_new_schema_version()) {
    out->has_new_schema = true;
    out->new_schema_version = pb.new_schema_version();
  }
  out->new_table_name = pb.new_table_name();
  return Status::OK();
}

// ===========================================================================
// CDCSchemaHistory
// ===========================================================================

void CDCSchemaHistory::AddVersion(uint32_t version, const Schema& schema) {
  {
    std::shared_lock<rw_spinlock> l(lock_);
    if (ContainsKey(schemas_by_version_, version)) {
      return;
    }
  }
  // Copy outside of the lock.
  Schema copy(schema);
  std::lock_guard<rw_spinlock> l(lock_);
  if (schemas_by_version_.emplace(version, std::move(copy)).second) {
    AddColumnsUnlocked(schema);
  }
}

void CDCSchemaHistory::AddColumns(const Schema& schema) {
  std::lock_guard<rw_spinlock> l(lock_);
  AddColumnsUnlocked(schema);
}

void CDCSchemaHistory::AddColumnsUnlocked(const Schema& schema) {
  if (!schema.has_column_ids()) {
    return;
  }
  for (int i = 0; i < schema.num_columns(); i++) {
    columns_by_id_.emplace(static_cast<int32_t>(schema.column_id(i)), schema.column(i));
  }
  if (!columns_by_id_.empty()) {
    max_column_id_.store(columns_by_id_.rbegin()->first, std::memory_order_release);
  }
}

const ColumnSchema* CDCSchemaHistory::FindColumn(uint32_t version,
                                                 const CDCColumnValuePB& cv) const {
  std::shared_lock<rw_spinlock> l(lock_);
  if (cv.has_column_id()) {
    return FindOrNull(columns_by_id_, cv.column_id());
  }
  const Schema* schema = FindOrNull(schemas_by_version_, version);
  if (!schema) {
    return nullptr;
  }
  const int idx = schema->find_column(cv.column_name());
  return idx == Schema::kColumnNotFound ? nullptr : &schema->column(idx);
}

}  // namespace cdc
}  // namespace kudu
