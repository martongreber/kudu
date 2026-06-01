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
#include <string>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/gutil/macros.h"
#include "kudu/master/master.pb.h"
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

  // Maximum serialized size of records in a single GetChanges response.
  // 0 leaves the server-side default in place.
  int64_t max_bytes_per_response = 0;
};

// Describes an existing CDC stream, as reported by the master.
struct CDCStreamInfo {
  std::string stream_id;

  // The table ids (as stored on the master) that this stream covers.
  std::vector<std::string> table_ids;

  master::CDCStreamConfigPB::RecordType record_type = master::CDCStreamConfigPB::CHANGE;
  master::CDCStreamConfigPB::SnapshotMode snapshot_mode = master::CDCStreamConfigPB::NEVER;

  // Per-tablet durable checkpoints: tablet_id -> last committed op_index.
  std::map<std::string, int64_t> tablet_checkpoints;
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

    // Per-RPC timeout applied to every master and tablet-server call.
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
  // that table id are returned.
  Status ListStreams(const std::string& table_id_filter,
                     std::vector<CDCStreamInfo>* streams);

  // Fetches details for a single stream.
  Status GetStreamInfo(const std::string& stream_id, CDCStreamInfo* info);

  // ---- Table metadata ---------------------------------------------------

  // Resolves table metadata. If 'by_id' is true, 'table_name_or_id' is treated
  // as a canonical table id; otherwise as a table name. Populates the schema
  // and (on secured clusters) the signed authz token.
  Status GetTableMetadata(const std::string& table_name_or_id,
                          bool by_id,
                          CDCTableMetadata* metadata);

  // ---- Topology ---------------------------------------------------------

  // Discovers all tablets of the table with the given id and their current
  // leaders. Paginates through the full partition range.
  Status GetTabletLocations(const std::string& table_id,
                            std::vector<CDCTabletInfo>* tablets);

  // ---- Data path --------------------------------------------------------

  // Issues a GetChanges RPC to the given tablet leader. The caller is
  // responsible for handling CDC-level errors reported inside 'resp'.
  Status GetChanges(const HostPort& leader,
                    const GetChangesRequestPB& req,
                    GetChangesResponsePB* resp);

  // Issues a Checkpoint RPC to the given tablet leader.
  Status Checkpoint(const HostPort& leader,
                    const CheckpointRequestPB& req,
                    CheckpointResponsePB* resp);

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
  // transport errors by advancing to the next master.
  template <class Req, class Resp>
  Status CallMaster(
      Status (master::MasterServiceProxy::*func)(const Req&, Resp*, rpc::RpcController*),
      const Req& req,
      Resp* resp,
      const char* rpc_name);

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
