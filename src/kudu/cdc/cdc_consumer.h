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

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/macros.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace cdc {

class CDCClient;
class CDCRecordPB;
class CDCTabletPoller;

// A single decoded column value handed to the application.
struct CDCDecodedColumn {
  std::string name;
  bool is_null = false;
  // Human-readable decoded value. Empty when 'is_null' is true. Falls back to a
  // hex representation for types that cannot be rendered against the schema.
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

  // After-image (from 'changes'). For DELETE, only the primary key columns.
  std::vector<CDCDecodedColumn> after;
  // Before-image (from 'old_changes'); populated only for FULL streams.
  std::vector<CDCDecodedColumn> before;

  // For DDL records.
  bool has_new_schema = false;
  uint32_t new_schema_version = 0;
};

// A batch of decoded records for a single tablet, in ascending op-index order.
struct CDCRecordBatch {
  std::string tablet_id;
  std::vector<CDCDecodedRecord> records;
};

// Per-tablet progress, for introspection / lag monitoring.
struct CDCTabletProgress {
  std::string tablet_id;
  int64_t last_delivered_op_index = -1;
  int64_t last_checkpointed_op_index = -1;
  bool snapshot_done = false;
  // Set if the tablet hit an unrecoverable-without-resnapshot condition
  // (WAL/HISTORY/STREAM expired).
  bool needs_resnapshot = false;
  Status last_error;
};

// Delivery callback: invoked with one batch for one tablet, in op-index order.
// May be called concurrently from different tablet threads, but never
// concurrently for the same tablet. Returning a non-OK Status stops that
// tablet's poller.
typedef std::function<Status(const CDCRecordBatch&)> RecordCallback;

// Consumes a CDC stream: discovers the covered table's tablets, spawns one
// poller per tablet, decodes records against the schema, and delivers them via
// a RecordCallback. Handles per-tablet leader failover, periodic checkpointing,
// resume from durable checkpoints, snapshot bootstrap, and expiry detection.
//
// The consumer does not own the CDCClient; the caller must keep it alive for
// the consumer's lifetime.
//
// Thread-safety: Start/Stop/Flush are meant to be called from a single
// controlling thread. GetProgress is safe to call concurrently.
class CDCConsumer {
 public:
  // Where each tablet should start when it has no durable server-side
  // checkpoint. Tablets that DO have a durable checkpoint always resume from
  // it, regardless of this mode.
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

    // Per-response byte cap forwarded to GetChanges. 0 leaves the stream's
    // configured default in place.
    int64_t max_bytes_per_response = 0;
  };

  // Builds a consumer for the given (already-created) stream. Resolves the
  // stream's table and current schema. Does not spawn any pollers yet.
  static Status Create(CDCClient* client,
                       Options options,
                       std::unique_ptr<CDCConsumer>* consumer);

  ~CDCConsumer();

  // Discovers tablets and spawns one poller per tablet. Returns immediately;
  // records are delivered on background threads via 'cb'.
  Status Start(RecordCallback cb);

  // Signals all pollers to stop and joins them. Idempotent.
  void Stop();

  // Forces an immediate durable checkpoint on every tablet at its current
  // position. Safe to call while running.
  Status Flush();

  // Returns a snapshot of per-tablet progress.
  void GetProgress(std::vector<CDCTabletProgress>* out) const;

  // Decodes a single record PB against 'schema'. Exposed for testing.
  static Status DecodeRecord(const Schema& schema,
                             const std::string& tablet_id,
                             const CDCRecordPB& pb,
                             CDCDecodedRecord* out);

 private:
  friend class CDCTabletPoller;

  CDCConsumer(CDCClient* client, Options options);

  // Called by pollers: resolve the current leader for a tablet, optionally
  // forcing a fresh lookup from the master.
  Status ResolveLeader(const std::string& tablet_id, bool force, HostPort* leader);

  // Probes 'tablet_id' for its current committed op index so a "now" tail is
  // anchored deterministically at Start() time (rather than whenever the poller
  // happens to make its first request). Returns the committed index on success,
  // or the "start from now" sentinel if the probe fails -- in which case the
  // poller anchors on its first poll, as before.
  int64_t AnchorNow(const std::string& tablet_id);

  // Called by pollers: decode 'records' against the current schema, apply any
  // DDL schema updates, and deliver the batch to the user callback.
  Status DecodeAndDeliver(const std::string& tablet_id,
                          const google::protobuf::RepeatedPtrField<CDCRecordPB>& records,
                          int64_t* last_delivered_op_index);

  // Called by pollers to obtain the current authz token (may be empty on
  // unsecured clusters). If 'force' is true, refreshes it from the master.
  Status GetAuthzToken(bool force, bool* has_token, security::SignedTokenPB* token);

  CDCClient* const client_;
  const Options options_;

  std::string table_id_;

  // Current schema used for decoding. Updated on DDL records. Guarded by
  // 'schema_lock_'.
  mutable simple_spinlock schema_lock_;
  Schema schema_;

  // Cached authz token. Guarded by 'authz_lock_'.
  mutable simple_spinlock authz_lock_;
  bool has_authz_token_ = false;
  security::SignedTokenPB authz_token_;

  // tablet_id -> leader HostPort. Guarded by 'leader_lock_'.
  mutable simple_spinlock leader_lock_;
  std::map<std::string, HostPort> tablet_leaders_;

  RecordCallback callback_;
  std::vector<std::unique_ptr<CDCTabletPoller>> pollers_;
  bool started_ = false;

  DISALLOW_COPY_AND_ASSIGN(CDCConsumer);
};

}  // namespace cdc
}  // namespace kudu
