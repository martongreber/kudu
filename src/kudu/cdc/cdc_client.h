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
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/gutil/macros.h"
#include "kudu/master/master.pb.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {

class Sockaddr;

namespace rpc {
class Messenger;
class RpcController;
}  // namespace rpc

namespace master {
class MasterServiceProxy;
}  // namespace master

namespace cdc {

class CDCServiceProxy;
class CheckpointRequestPB;
class CheckpointResponsePB;
class GetChangesRequestPB;
class GetChangesResponsePB;

// Options controlling how a new CDC stream is created. Mirrors the relevant
// subset of master::CDCStreamConfigPB.
struct CDCStreamOptions {
  master::CDCStreamConfigPB::RecordType record_type = master::CDCStreamConfigPB::CHANGE;
  master::CDCStreamConfigPB::SnapshotMode snapshot_mode = master::CDCStreamConfigPB::NEVER;

  // Create a temporary stream, which the master deletes once all its tablets
  // have been idle for --cdc_ephemeral_stream_expiry_ms.
  bool ephemeral = false;
};

// Describes an existing CDC stream, as reported by the master.
struct CDCStreamInfo {
  std::string stream_id;

  // The table ids (as stored on the master) that this stream covers.
  std::vector<std::string> table_ids;

  master::CDCStreamConfigPB::RecordType record_type = master::CDCStreamConfigPB::CHANGE;
  master::CDCStreamConfigPB::SnapshotMode snapshot_mode = master::CDCStreamConfigPB::NEVER;

  // Per-tablet durable checkpoints: tablet_id -> last committed op_index.
  // A tablet whose snapshot is in progress has none.
  std::map<std::string, int64_t> tablet_checkpoints;

  // The tablets whose snapshot is in progress: started and not completed. A
  // consumer must complete their snapshot before it streams their WAL. Filled
  // in by GetStreamInfo() only.
  std::set<std::string> tablets_in_snapshot;

  // The tablets added to the table after the stream was created (ADD RANGE
  // PARTITION) that have no checkpoint past their start yet: a consumer reads
  // them from their first op. Filled in by GetStreamInfo() only.
  std::set<std::string> tablets_added_after_creation;

  master::SysCDCStreamEntryPB::State state = master::SysCDCStreamEntryPB::ACTIVE;

  // Wall-clock creation time (microseconds since the Unix epoch), 0 if unknown.
  int64_t creation_time_micros = 0;

  bool ephemeral = false;

  // The master's view of each tablet's checkpoint; filled in only by
  // GetStreamInfo() with 'include_details'.
  std::vector<master::CDCTabletStatusPB> tablet_status;

  // The tablets whose range partition was dropped, by tablet id. They are not
  // in 'tablet_checkpoints'. Filled in by GetStreamInfo() only.
  std::map<std::string, master::CDCStreamDroppedTabletPB> dropped_tablets;
};

// Resolved metadata for a table backing a CDC stream: its canonical id, the
// current schema, and (on secured clusters) a signed authorization token to
// pass to GetChanges/Checkpoint.
struct CDCTableMetadata {
  std::string table_id;
  std::string table_name;
  Schema schema;
  bool has_authz_token = false;
  security::SignedTokenPB authz_token;
};

// Location of a single tablet: its id, current leader (may be empty if no
// leader is currently known), and the set of replica endpoints.
struct CDCTabletInfo {
  std::string tablet_id;
  HostPort leader;
  std::vector<HostPort> replicas;
};

// A self-contained client for Kudu Change Data Capture.
//
// The client owns its own Messenger and a set of MasterServiceProxy objects
// (one per configured master) and performs leader-master failover internally.
// It exposes:
//   - stream lifecycle management (create / delete / list / describe),
//   - table metadata + authz-token resolution,
//   - tablet/leader discovery,
//   - the per-tablet data-path RPCs (GetChanges / Checkpoint).
//
// It deliberately does not depend on KuduClient internals; this keeps the CDC
// client decoupled from the public client library while reusing only the
// generated proxies. Higher-level orchestration (per-tablet pollers, record
// decoding, fan-out) lives in CDCConsumer (cdc_consumer.h).
//
// Thread-safety: an instance is safe to use from multiple threads. The
// data-path methods (GetChanges/Checkpoint/GetCDCProxy) and master failover
// state are internally synchronized.
class CDCClient {
 public:
  struct Options {
    // "host:port" master addresses. At least one is required.
    std::vector<std::string> master_addresses;

    // Per-RPC timeout applied to every master and tablet-server call. While
    // the masters have no leader (an election, or a majority of them down), a
    // master call is retried until this much time passed before it fails.
    MonoDelta rpc_timeout = MonoDelta::FromSeconds(30);

    // Name used to label the underlying Messenger (shows up in RPC traces).
    std::string client_name = "kudu-cdc-client";
  };

  // Builds and initializes a client. On success, '*client' owns a ready-to-use
  // instance.
  static Status Create(Options options, std::unique_ptr<CDCClient>* client);

  ~CDCClient();

  // ---- Stream lifecycle -------------------------------------------------

  // Creates a stream over 'table_name'. The table name is resolved to its
  // canonical table id before the stream is created, so the stream is bound to
  // the table's identity rather than its (mutable) name. On success, the
  // server-assigned id is returned in '*stream_id'.
  Status CreateStream(const std::string& table_name,
                      const CDCStreamOptions& opts,
                      std::string* stream_id);

  // Deletes the stream with the given id.
  Status DeleteStream(const std::string& stream_id);

  // Lists streams. If 'table_id_filter' is non-empty, only streams covering
  // that table id are returned. Streams that are being created or deleted are
  // returned only if 'include_deleting' is true.
  Status ListStreams(const std::string& table_id_filter,
                     std::vector<CDCStreamInfo>* streams,
                     bool include_deleting = false);

  // Fetches details for a single stream. With 'include_details', a stream that
  // is being created or deleted is described as well (instead of NotFound), and
  // the per-tablet status is filled in.
  Status GetStreamInfo(const std::string& stream_id, CDCStreamInfo* info,
                       bool include_details = false);

  // ---- Table metadata ---------------------------------------------------

  // Resolves table metadata. If 'by_id' is true, 'table_name_or_id' is treated
  // as a canonical table id; otherwise as a table name. Populates the schema
  // and (on secured clusters) the signed authz token.
  Status GetTableMetadata(const std::string& table_name_or_id,
                          bool by_id,
                          CDCTableMetadata* metadata);

  // ---- Topology ---------------------------------------------------------

  // Discovers all tablets of the table with the given id and their current
  // leaders. Paginates through the full partition range; each tablet is listed
  // once.
  Status GetTabletLocations(const std::string& table_id,
                            std::vector<CDCTabletInfo>* tablets);

  // ---- Data path --------------------------------------------------------

  // Issues a GetChanges RPC to the given tablet leader. The caller is
  // responsible for handling CDC-level errors reported inside 'resp'. If the
  // server failed the RPC and 'rpc_error' is set, it is set to the error code
  // the server returned, such as ERROR_INVALID_AUTHORIZATION_TOKEN when the
  // authz token must be renewed, or to FATAL_UNKNOWN if there is none.
  Status GetChanges(const HostPort& leader,
                    const GetChangesRequestPB& req,
                    GetChangesResponsePB* resp,
                    rpc::ErrorStatusPB::RpcErrorCodePB* rpc_error = nullptr);

  // Issues a Checkpoint RPC to the given tablet leader. 'rpc_error' is as for
  // GetChanges().
  Status Checkpoint(const HostPort& leader,
                    const CheckpointRequestPB& req,
                    CheckpointResponsePB* resp,
                    rpc::ErrorStatusPB::RpcErrorCodePB* rpc_error = nullptr);

  // ---- Accessors --------------------------------------------------------

  const std::shared_ptr<rpc::Messenger>& messenger() const { return messenger_; }
  const MonoDelta& rpc_timeout() const { return rpc_timeout_; }
  const std::vector<HostPort>& master_addresses() const { return master_hps_; }

 private:
  explicit CDCClient(Options options);

  // Resolves master addresses and constructs one MasterServiceProxy per master.
  Status Init();

  // Invokes a master RPC via 'func', cycling through masters until the leader
  // is found (or all masters have been tried). Handles NOT_THE_LEADER and
  // transport errors by advancing to the next master. If a master answered
  // that it is not the leader, tries them all again, with backoff, until
  // 'rpc_timeout_' passed since the call began. If 'require_cdc' is
  // true, the RPC requires the master's CDC feature, and NotSupported is
  // returned if no leader was found but a master did not support CDC.
  template <class Req, class Resp>
  Status CallMaster(
      Status (master::MasterServiceProxy::*func)(const Req&, Resp*, rpc::RpcController*),
      const Req& req,
      Resp* resp,
      const char* rpc_name,
      bool require_cdc = false);

  // Returns a (cached) CDCServiceProxy for the given tablet-server endpoint.
  Status GetCDCProxy(const HostPort& hp, std::shared_ptr<CDCServiceProxy>* proxy);

  const MonoDelta rpc_timeout_;
  const std::string client_name_;
  const std::vector<std::string> master_addr_strings_;

  std::shared_ptr<rpc::Messenger> messenger_;

  std::vector<HostPort> master_hps_;
  std::vector<std::shared_ptr<master::MasterServiceProxy>> master_proxies_;

  // Index of the master believed to be the leader. Guarded by 'master_lock_'.
  mutable simple_spinlock master_lock_;
  int leader_master_idx_ = 0;

  // Cache of CDCServiceProxy keyed by "host:port". Guarded by 'proxy_lock_'.
  mutable simple_spinlock proxy_lock_;
  std::map<std::string, std::shared_ptr<CDCServiceProxy>> cdc_proxies_;

  DISALLOW_COPY_AND_ASSIGN(CDCClient);
};

}  // namespace cdc
}  // namespace kudu
