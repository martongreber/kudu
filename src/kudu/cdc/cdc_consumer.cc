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
#include <limits>
#include <mutex>
#include <ostream>
#include <thread>
#include <utility>

#include <glog/logging.h>
#include <google/protobuf/repeated_ptr_field.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

using google::protobuf::RepeatedPtrField;
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

// Renders a raw serialized cell value against its column type.
Status DecodeCell(const ColumnSchema& col, const string& bytes, string* out) {
  out->clear();
  const TypeInfo* ti = col.type_info();
  if (ti->physical_type() == BINARY) {
    Slice s(bytes);
    ti->AppendDebugStringForValue(&s, out);
  } else {
    if (bytes.size() != ti->size()) {
      return Status::Corruption(Substitute(
          "unexpected serialized size for column '$0': got $1, expected $2",
          col.name(), bytes.size(), ti->size()));
    }
    ti->AppendDebugStringForValue(bytes.data(), out);
  }
  return Status::OK();
}

void DecodeColumn(const Schema& schema,
                  const CDCColumnValuePB& cv,
                  CDCDecodedColumn* out) {
  out->name = cv.column_name();
  out->is_null = cv.is_null();
  out->value.clear();
  if (out->is_null || !cv.has_value()) {
    return;
  }
  const int idx = schema.find_column(cv.column_name());
  if (idx == Schema::kColumnNotFound) {
    // Column not in the current schema (renamed/dropped, or older version):
    // fall back to a debug/hex rendering so nothing is silently lost.
    out->value = Slice(cv.value()).ToDebugString();
    return;
  }
  Status s = DecodeCell(schema.column(idx), cv.value(), &out->value);
  if (!s.ok()) {
    out->value = Slice(cv.value()).ToDebugString();
  }
}
}  // anonymous namespace

// ===========================================================================
// CDCTabletPoller: per-tablet state machine (declared here, used by CDCConsumer)
// ===========================================================================

// One instance per tablet. Runs a dedicated thread that drives GetChanges,
// delivers decoded records, and periodically checkpoints, handling leader
// failover and expiry.
class CDCTabletPoller {
 public:
  CDCTabletPoller(CDCConsumer* consumer,
                  string tablet_id,
                  int64_t initial_from_op,
                  bool do_snapshot);
  ~CDCTabletPoller();

  // Spawns the background thread.
  Status Start();

  // Requests the poller to stop (non-blocking).
  void RequestStop();

  // Joins the background thread. Must be preceded by RequestStop().
  void Join();

  // Forces a durable checkpoint at the current position (best-effort).
  Status ForceCheckpoint();

  void GetProgress(CDCTabletProgress* out) const;

 private:
  enum class PollOutcome { kOk, kRetry, kFatal, kResnapshot };

  void Run();
  bool ResolveLeaderWithRetry();
  Status RunSnapshotPhase();
  PollOutcome PollOnce(bool* got_records);
  PollOutcome ClassifyCdcError(const CDCErrorPB& err);
  void MaybeCheckpoint(bool force);
  // Interruptible sleep; returns true if the poller was asked to stop.
  bool SleepFor(const MonoDelta& d);
  void GrowBackoff();
  void ResetBackoff();

  CDCConsumer* const consumer_;
  const string tablet_id_;
  const bool do_snapshot_;

  CountDownLatch stop_latch_;
  std::atomic<bool> stopping_;
  std::thread thread_;

  // Only accessed on the poller thread except where noted.
  HostPort leader_;
  MonoDelta backoff_;
  int64_t last_checkpointed_op_;
  MonoTime last_checkpoint_time_;

  // Progress fields, guarded by 'progress_lock_' for GetProgress().
  mutable simple_spinlock progress_lock_;
  int64_t from_op_;
  int64_t last_delivered_op_;
  bool snapshot_done_;
  bool needs_resnapshot_;
  Status last_error_;

  DISALLOW_COPY_AND_ASSIGN(CDCTabletPoller);
};

CDCTabletPoller::CDCTabletPoller(CDCConsumer* consumer,
                                 string tablet_id,
                                 int64_t initial_from_op,
                                 bool do_snapshot)
    : consumer_(consumer),
      tablet_id_(std::move(tablet_id)),
      do_snapshot_(do_snapshot),
      stop_latch_(1),
      stopping_(false),
      backoff_(consumer->options_.min_poll_backoff),
      last_checkpointed_op_(-1),
      last_checkpoint_time_(MonoTime::Now()),
      from_op_(initial_from_op),
      last_delivered_op_(-1),
      snapshot_done_(false),
      needs_resnapshot_(false) {}

CDCTabletPoller::~CDCTabletPoller() {
  RequestStop();
  Join();
}

Status CDCTabletPoller::Start() {
  thread_ = std::thread([this]() { this->Run(); });
  return Status::OK();
}

void CDCTabletPoller::RequestStop() {
  stopping_.store(true);
  stop_latch_.CountDown();
}

void CDCTabletPoller::Join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

bool CDCTabletPoller::SleepFor(const MonoDelta& d) {
  // WaitFor returns true if the latch reached zero (stop requested).
  return stop_latch_.WaitFor(d);
}

void CDCTabletPoller::GrowBackoff() {
  const MonoDelta max = consumer_->options_.max_poll_backoff;
  MonoDelta next = MonoDelta::FromNanoseconds(backoff_.ToNanoseconds() * 2);
  backoff_ = (next.ToNanoseconds() > max.ToNanoseconds()) ? max : next;
}

void CDCTabletPoller::ResetBackoff() {
  backoff_ = consumer_->options_.min_poll_backoff;
}

bool CDCTabletPoller::ResolveLeaderWithRetry() {
  bool force = false;
  while (!stopping_.load()) {
    HostPort hp;
    Status s = consumer_->ResolveLeader(tablet_id_, force, &hp);
    if (s.ok() && hp.Initialized()) {
      leader_ = hp;
      return true;
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

void CDCTabletPoller::Run() {
  if (!ResolveLeaderWithRetry()) {
    return;
  }
  ResetBackoff();

  if (do_snapshot_) {
    Status s = RunSnapshotPhase();
    if (!s.ok()) {
      // Snapshot failed (error already recorded); fall through to stop.
      MaybeCheckpoint(true);
      return;
    }
    if (stopping_.load()) {
      MaybeCheckpoint(true);
      return;
    }
  }

  while (!stopping_.load()) {
    bool got_records = false;
    PollOutcome outcome = PollOnce(&got_records);
    switch (outcome) {
      case PollOutcome::kOk:
        MaybeCheckpoint(false);
        if (got_records) {
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
      case PollOutcome::kResnapshot:
      case PollOutcome::kFatal:
        // Unrecoverable for this tablet in v1: record and stop the loop.
        MaybeCheckpoint(true);
        return;
    }
  }
  // Clean shutdown: persist the latest position.
  MaybeCheckpoint(true);
}

Status CDCTabletPoller::RunSnapshotPhase() {
  bool first = true;
  string resume_key;
  while (!stopping_.load()) {
    bool has_token = false;
    security::SignedTokenPB token;
    RETURN_NOT_OK(consumer_->GetAuthzToken(/*force=*/false, &has_token, &token));

    GetChangesRequestPB req;
    req.set_stream_id(consumer_->options_.stream_id);
    req.set_tablet_id(tablet_id_);
    req.set_from_op_index(0);
    if (consumer_->options_.max_bytes_per_response > 0) {
      req.set_max_bytes(consumer_->options_.max_bytes_per_response);
    }
    req.set_is_snapshot_start(first);
    if (!first && !resume_key.empty()) {
      req.set_snapshot_resume_key(resume_key);
    }
    if (has_token) {
      *req.mutable_authz_token() = token;
    }

    GetChangesResponsePB resp;
    Status s = consumer_->client_->GetChanges(leader_, req, &resp);
    if (!s.ok()) {
      if (!ResolveLeaderWithRetry()) return Status::Aborted("stopped");
      continue;
    }
    if (resp.has_error()) {
      PollOutcome oc = ClassifyCdcError(resp.error());
      if (oc == PollOutcome::kRetry) {
        if (SleepFor(backoff_)) return Status::Aborted("stopped");
        GrowBackoff();
        continue;
      }
      return StatusFromPB(resp.error().status());
    }

    if (resp.records_size() > 0) {
      int64_t delivered = -1;
      RETURN_NOT_OK(consumer_->DecodeAndDeliver(tablet_id_, resp.records(), &delivered));
      if (delivered >= 0) {
        std::lock_guard<simple_spinlock> l(progress_lock_);
        last_delivered_op_ = delivered;
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
      return Status::OK();
    }
    resume_key = resp.snapshot_resume_key();
    first = false;
  }
  return Status::Aborted("stopped");
}

CDCTabletPoller::PollOutcome CDCTabletPoller::ClassifyCdcError(
    const CDCErrorPB& err) {
  const Status status = StatusFromPB(err.status());
  switch (err.code()) {
    case CDCErrorPB::TABLET_NOT_LEADER:
    case CDCErrorPB::TABLET_NOT_FOUND:
    case CDCErrorPB::TABLET_NOT_RUNNING: {
      // Leader moved or tablet transiently unavailable: refresh and retry.
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = status;
      // Trigger a forced leader re-resolution on the next iteration.
      leader_ = HostPort();
      return PollOutcome::kRetry;
    }
    case CDCErrorPB::SERVER_TOO_BUSY: {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = status;
      return PollOutcome::kRetry;
    }
    case CDCErrorPB::NOT_AUTHORIZED: {
      bool has_token = false;
      security::SignedTokenPB token;
      Status s = consumer_->GetAuthzToken(/*force=*/true, &has_token, &token);
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = status;
      return s.ok() ? PollOutcome::kRetry : PollOutcome::kFatal;
    }
    case CDCErrorPB::WAL_EXPIRED:
    case CDCErrorPB::HISTORY_EXPIRED:
    case CDCErrorPB::STREAM_EXPIRED: {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = status;
      needs_resnapshot_ = true;
      LOG(WARNING) << "tablet " << tablet_id_
                   << " requires a fresh snapshot: " << status.ToString();
      return PollOutcome::kResnapshot;
    }
    default: {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = status;
      LOG(WARNING) << "tablet " << tablet_id_
                   << " fatal CDC error: " << status.ToString();
      return PollOutcome::kFatal;
    }
  }
}

CDCTabletPoller::PollOutcome CDCTabletPoller::PollOnce(bool* got_records) {
  *got_records = false;

  if (!leader_.Initialized()) {
    if (!ResolveLeaderWithRetry()) return PollOutcome::kRetry;
  }

  bool has_token = false;
  security::SignedTokenPB token;
  Status ts = consumer_->GetAuthzToken(/*force=*/false, &has_token, &token);
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
  if (consumer_->options_.max_bytes_per_response > 0) {
    req.set_max_bytes(consumer_->options_.max_bytes_per_response);
  }
  if (has_token) {
    *req.mutable_authz_token() = token;
  }

  GetChangesResponsePB resp;
  Status s = consumer_->client_->GetChanges(leader_, req, &resp);
  if (!s.ok()) {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    last_error_ = s;
    leader_ = HostPort();  // force re-resolution next time
    return PollOutcome::kRetry;
  }
  if (resp.has_error()) {
    return ClassifyCdcError(resp.error());
  }

  if (resp.records_size() > 0) {
    int64_t delivered = -1;
    Status ds = consumer_->DecodeAndDeliver(tablet_id_, resp.records(), &delivered);
    if (!ds.ok()) {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_error_ = ds;
      return PollOutcome::kFatal;  // callback asked to stop or decode failed
    }
    *got_records = true;
    if (delivered >= 0) {
      std::lock_guard<simple_spinlock> l(progress_lock_);
      last_delivered_op_ = delivered;
    }
  }

  // Advance the read position from the server-reported checkpoint.
  if (resp.has_checkpoint_op_index() && resp.checkpoint_op_index() >= 0) {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    from_op_ = resp.checkpoint_op_index();
    last_error_ = Status::OK();
  }
  return PollOutcome::kOk;
}

void CDCTabletPoller::MaybeCheckpoint(bool force) {
  int64_t target;
  {
    std::lock_guard<simple_spinlock> l(progress_lock_);
    target = from_op_;
  }
  // Never persist the "start from now" sentinel or a non-advancing position.
  if (target < 0 || target == kStartFromNow || target <= last_checkpointed_op_) {
    return;
  }
  if (!force) {
    const MonoDelta since = MonoTime::Now() - last_checkpoint_time_;
    if (since.ToNanoseconds() <
        consumer_->options_.checkpoint_interval.ToNanoseconds()) {
      return;
    }
  }
  if (!leader_.Initialized()) {
    return;
  }

  bool has_token = false;
  security::SignedTokenPB token;
  if (!consumer_->GetAuthzToken(/*force=*/false, &has_token, &token).ok()) {
    return;
  }

  CheckpointRequestPB req;
  req.set_stream_id(consumer_->options_.stream_id);
  req.set_tablet_id(tablet_id_);
  req.set_op_index(target);
  if (has_token) {
    *req.mutable_authz_token() = token;
  }
  CheckpointResponsePB resp;
  Status s = consumer_->client_->Checkpoint(leader_, req, &resp);
  if (!s.ok()) {
    VLOG(1) << "checkpoint RPC to tablet " << tablet_id_
            << " failed: " << s.ToString();
    return;
  }
  if (resp.has_error()) {
    VLOG(1) << "checkpoint of tablet " << tablet_id_
            << " rejected: " << StatusFromPB(resp.error().status()).ToString();
    return;
  }
  last_checkpointed_op_ = target;
  last_checkpoint_time_ = MonoTime::Now();
}

Status CDCTabletPoller::ForceCheckpoint() {
  MaybeCheckpoint(/*force=*/true);
  return Status::OK();
}

void CDCTabletPoller::GetProgress(CDCTabletProgress* out) const {
  std::lock_guard<simple_spinlock> l(progress_lock_);
  out->tablet_id = tablet_id_;
  out->last_delivered_op_index = last_delivered_op_;
  out->last_checkpointed_op_index = last_checkpointed_op_;
  out->snapshot_done = snapshot_done_;
  out->needs_resnapshot = needs_resnapshot_;
  out->last_error = last_error_;
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
  unique_ptr<CDCConsumer> c(new CDCConsumer(client, std::move(options)));

  CDCStreamInfo info;
  RETURN_NOT_OK_PREPEND(client->GetStreamInfo(c->options_.stream_id, &info),
                        "could not look up stream");
  if (info.table_ids.empty()) {
    return Status::IllegalState("stream covers no tables", c->options_.stream_id);
  }
  c->table_id_ = info.table_ids.front();

  CDCTableMetadata md;
  RETURN_NOT_OK_PREPEND(client->GetTableMetadata(c->table_id_, /*by_id=*/true, &md),
                        "could not resolve stream's table");
  c->schema_ = md.schema;
  c->has_authz_token_ = md.has_authz_token;
  c->authz_token_ = md.authz_token;

  *consumer = std::move(c);
  return Status::OK();
}

Status CDCConsumer::Start(RecordCallback cb) {
  if (started_) {
    return Status::IllegalState("consumer already started");
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

  {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    for (const auto& t : tablets) {
      if (t.leader.Initialized()) {
        tablet_leaders_[t.tablet_id] = t.leader;
      }
    }
  }

  for (const auto& t : tablets) {
    int64_t initial;
    bool do_snapshot = false;
    auto it = info.tablet_checkpoints.find(t.tablet_id);
    if (it != info.tablet_checkpoints.end() && it->second > 0) {
      // Resume from the durable checkpoint regardless of start mode.
      initial = it->second;
    } else {
      switch (options_.start_mode) {
        case kNow:
          initial = AnchorNow(t.tablet_id);
          break;
        case kEarliest:
          initial = 0;
          break;
        case kSnapshot:
          initial = 0;
          do_snapshot = true;
          break;
        default:
          initial = kStartFromNow;
          break;
      }
    }
    pollers_.emplace_back(new CDCTabletPoller(this, t.tablet_id, initial, do_snapshot));
  }

  for (auto& p : pollers_) {
    RETURN_NOT_OK(p->Start());
  }
  started_ = true;
  return Status::OK();
}

void CDCConsumer::Stop() {
  if (!started_ && pollers_.empty()) {
    return;
  }
  for (auto& p : pollers_) {
    p->RequestStop();
  }
  for (auto& p : pollers_) {
    p->Join();
  }
  pollers_.clear();
  started_ = false;
}

Status CDCConsumer::Flush() {
  for (auto& p : pollers_) {
    p->ForceCheckpoint();
  }
  return Status::OK();
}

void CDCConsumer::GetProgress(vector<CDCTabletProgress>* out) const {
  out->clear();
  out->reserve(pollers_.size());
  for (const auto& p : pollers_) {
    CDCTabletProgress prog;
    p->GetProgress(&prog);
    out->emplace_back(std::move(prog));
  }
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
  WARN_NOT_OK(GetAuthzToken(/*force=*/false, &has_token, &token),
              "could not fetch authz token for now-anchor probe");

  // A from_op_index at or beyond the committed tail makes the server return an
  // empty batch whose checkpoint_op_index is the current committed index.
  GetChangesRequestPB req;
  req.set_stream_id(options_.stream_id);
  req.set_tablet_id(tablet_id);
  req.set_from_op_index(kStartFromNow);
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
                                  HostPort* leader) {
  if (!force) {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    auto it = tablet_leaders_.find(tablet_id);
    if (it != tablet_leaders_.end() && it->second.Initialized()) {
      *leader = it->second;
      return Status::OK();
    }
  }

  vector<CDCTabletInfo> tablets;
  RETURN_NOT_OK(client_->GetTabletLocations(table_id_, &tablets));
  {
    std::lock_guard<simple_spinlock> l(leader_lock_);
    for (const auto& t : tablets) {
      if (t.leader.Initialized()) {
        tablet_leaders_[t.tablet_id] = t.leader;
      }
    }
    auto it = tablet_leaders_.find(tablet_id);
    if (it != tablet_leaders_.end() && it->second.Initialized()) {
      *leader = it->second;
      return Status::OK();
    }
  }
  return Status::NotFound("no leader currently known for tablet", tablet_id);
}

Status CDCConsumer::DecodeAndDeliver(const string& tablet_id,
                                     const RepeatedPtrField<CDCRecordPB>& records,
                                     int64_t* last_delivered_op_index) {
  CDCRecordBatch batch;
  batch.tablet_id = tablet_id;
  batch.records.reserve(records.size());

  Schema schema;
  {
    std::lock_guard<simple_spinlock> l(schema_lock_);
    schema = schema_;
  }

  for (const auto& pb : records) {
    CDCDecodedRecord r;
    RETURN_NOT_OK(DecodeRecord(schema, tablet_id, pb, &r));

    // Apply DDL schema updates so subsequent records decode against the new
    // schema.
    if (pb.op_type() == DDL && pb.has_new_schema()) {
      Schema new_schema;
      Status s = SchemaFromPB(pb.new_schema(), &new_schema);
      if (s.ok()) {
        schema = new_schema;
        std::lock_guard<simple_spinlock> l(schema_lock_);
        schema_ = new_schema;
      } else {
        LOG(WARNING) << "could not decode DDL schema for tablet " << tablet_id
                     << ": " << s.ToString();
      }
    }

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

Status CDCConsumer::GetAuthzToken(bool force,
                                  bool* has_token,
                                  security::SignedTokenPB* token) {
  if (force) {
    CDCTableMetadata md;
    RETURN_NOT_OK(client_->GetTableMetadata(table_id_, /*by_id=*/true, &md));
    std::lock_guard<simple_spinlock> l(authz_lock_);
    has_authz_token_ = md.has_authz_token;
    authz_token_ = md.authz_token;
  }
  std::lock_guard<simple_spinlock> l(authz_lock_);
  *has_token = has_authz_token_;
  if (has_authz_token_) {
    *token = authz_token_;
  }
  return Status::OK();
}

Status CDCConsumer::DecodeRecord(const Schema& schema,
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

  out->after.reserve(pb.changes_size());
  for (const auto& cv : pb.changes()) {
    CDCDecodedColumn col;
    DecodeColumn(schema, cv, &col);
    out->after.emplace_back(std::move(col));
  }
  out->before.reserve(pb.old_changes_size());
  for (const auto& cv : pb.old_changes()) {
    CDCDecodedColumn col;
    DecodeColumn(schema, cv, &col);
    out->before.emplace_back(std::move(col));
  }

  if (pb.has_new_schema_version()) {
    out->has_new_schema = true;
    out->new_schema_version = pb.new_schema_version();
  }
  return Status::OK();
}

}  // namespace cdc
}  // namespace kudu
