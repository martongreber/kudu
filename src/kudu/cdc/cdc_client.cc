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

#include "kudu/cdc/cdc_client.h"

#include <mutex>
#include <ostream>
#include <utility>

#include <glog/logging.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.proxy.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/net/sockaddr.h"

using kudu::master::MasterServiceProxy;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cdc {

namespace {
// Default master RPC port, used when a master address omits an explicit port.
// Kept in sync with master::Master::kDefaultPort without pulling in master.h.
constexpr uint16_t kDefaultMasterPort = 7051;

// Upper bound on tablet locations fetched per GetTableLocations page.
constexpr int kTabletLocationsPageSize = 1000;

// Safety cap on tablet-location pagination iterations.
constexpr int kMaxTabletLocationPages = 10000;
}  // anonymous namespace

CDCClient::CDCClient(Options options)
    : rpc_timeout_(options.rpc_timeout),
      client_name_(std::move(options.client_name)),
      master_addr_strings_(std::move(options.master_addresses)) {}

CDCClient::~CDCClient() {
  if (messenger_) {
    messenger_->Shutdown();
  }
}

Status CDCClient::Create(Options options, unique_ptr<CDCClient>* client) {
  if (options.master_addresses.empty()) {
    return Status::InvalidArgument("at least one master address is required");
  }
  if (!options.rpc_timeout.Initialized() ||
      options.rpc_timeout.ToNanoseconds() <= 0) {
    return Status::InvalidArgument("rpc_timeout must be positive");
  }
  unique_ptr<CDCClient> c(new CDCClient(std::move(options)));
  RETURN_NOT_OK(c->Init());
  *client = std::move(c);
  return Status::OK();
}

Status CDCClient::Init() {
  MessengerBuilder bld(client_name_);
  RETURN_NOT_OK_PREPEND(bld.Build(&messenger_), "could not build messenger");

  master_hps_.reserve(master_addr_strings_.size());
  master_proxies_.reserve(master_addr_strings_.size());
  for (const auto& addr : master_addr_strings_) {
    HostPort hp;
    RETURN_NOT_OK_PREPEND(hp.ParseString(addr, kDefaultMasterPort),
                          Substitute("could not parse master address '$0'", addr));
    vector<Sockaddr> resolved;
    RETURN_NOT_OK_PREPEND(hp.ResolveAddresses(&resolved),
                          Substitute("could not resolve master address '$0'", addr));
    if (resolved.empty()) {
      return Status::NetworkError(
          Substitute("master address '$0' did not resolve to any endpoint", addr));
    }
    master_hps_.emplace_back(hp);
    master_proxies_.emplace_back(
        std::make_shared<MasterServiceProxy>(messenger_, resolved.front(), hp.host()));
  }
  return Status::OK();
}

template <class Req, class Resp>
Status CDCClient::CallMaster(
    Status (MasterServiceProxy::*func)(const Req&, Resp*, RpcController*),
    const Req& req,
    Resp* resp,
    const char* rpc_name) {
  const int n = static_cast<int>(master_proxies_.size());
  int start;
  {
    std::lock_guard<simple_spinlock> l(master_lock_);
    start = leader_master_idx_;
  }
  Status last_status = Status::OK();
  for (int i = 0; i < n; i++) {
    const int idx = (start + i) % n;
    RpcController rpc;
    rpc.set_timeout(rpc_timeout_);
    Status s = ((*master_proxies_[idx]).*func)(req, resp, &rpc);
    if (!s.ok()) {
      last_status = s.CloneAndPrepend(
          Substitute("$0 to master $1 failed", rpc_name, master_hps_[idx].ToString()));
      continue;
    }
    if (resp->has_error() &&
        resp->error().code() == master::MasterErrorPB::NOT_THE_LEADER) {
      last_status = StatusFromPB(resp->error().status());
      continue;
    }
    // Got a definitive response from the leader. Remember it and let the
    // caller interpret any application-level error still present in 'resp'.
    {
      std::lock_guard<simple_spinlock> l(master_lock_);
      leader_master_idx_ = idx;
    }
    return Status::OK();
  }
  if (last_status.ok()) {
    return Status::ServiceUnavailable(
        Substitute("$0: unable to find a leader master", rpc_name));
  }
  return last_status.CloneAndPrepend(
      Substitute("$0: unable to reach a leader master", rpc_name));
}

Status CDCClient::CreateStream(const string& table_name,
                               const CDCStreamOptions& opts,
                               string* stream_id) {
  // Resolve the real table id so the stream is bound to the table's identity.
  CDCTableMetadata md;
  RETURN_NOT_OK_PREPEND(GetTableMetadata(table_name, /*by_id=*/false, &md),
                        Substitute("could not resolve table '$0'", table_name));

  master::CreateCDCStreamRequestPB req;
  master::CreateCDCStreamResponsePB resp;
  req.add_table_ids(md.table_id);
  auto* config = req.mutable_config();
  config->set_record_type(opts.record_type);
  config->set_snapshot_mode(opts.snapshot_mode);
  if (opts.max_bytes_per_response > 0) {
    config->set_max_bytes_per_response(opts.max_bytes_per_response);
  }

  RETURN_NOT_OK(CallMaster(&MasterServiceProxy::CreateCDCStream, req, &resp,
                           "CreateCDCStream"));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (!resp.has_stream_id()) {
    return Status::IllegalState("master did not return a stream id");
  }
  *stream_id = resp.stream_id();
  return Status::OK();
}

Status CDCClient::DeleteStream(const string& stream_id) {
  master::DeleteCDCStreamRequestPB req;
  master::DeleteCDCStreamResponsePB resp;
  req.set_stream_id(stream_id);
  RETURN_NOT_OK(CallMaster(&MasterServiceProxy::DeleteCDCStream, req, &resp,
                           "DeleteCDCStream"));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

namespace {
void FillStreamInfo(const master::CDCStreamInfoPB& pb, CDCStreamInfo* out) {
  out->stream_id = pb.stream_id();
  out->table_ids.assign(pb.table_ids().begin(), pb.table_ids().end());
  if (pb.has_config()) {
    if (pb.config().has_record_type()) {
      out->record_type = pb.config().record_type();
    }
    if (pb.config().has_snapshot_mode()) {
      out->snapshot_mode = pb.config().snapshot_mode();
    }
  }
  for (const auto& e : pb.tablet_checkpoints()) {
    out->tablet_checkpoints[e.first] = e.second;
  }
}
}  // anonymous namespace

Status CDCClient::ListStreams(const string& table_id_filter,
                              vector<CDCStreamInfo>* streams) {
  master::ListCDCStreamsRequestPB req;
  master::ListCDCStreamsResponsePB resp;
  if (!table_id_filter.empty()) {
    req.set_table_id(table_id_filter);
  }
  RETURN_NOT_OK(CallMaster(&MasterServiceProxy::ListCDCStreams, req, &resp,
                           "ListCDCStreams"));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  streams->clear();
  streams->reserve(resp.streams_size());
  for (const auto& s : resp.streams()) {
    CDCStreamInfo info;
    FillStreamInfo(s, &info);
    streams->emplace_back(std::move(info));
  }
  return Status::OK();
}

Status CDCClient::GetStreamInfo(const string& stream_id, CDCStreamInfo* info) {
  master::GetCDCStreamInfoRequestPB req;
  master::GetCDCStreamInfoResponsePB resp;
  req.set_stream_id(stream_id);
  RETURN_NOT_OK(CallMaster(&MasterServiceProxy::GetCDCStreamInfo, req, &resp,
                           "GetCDCStreamInfo"));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  if (!resp.has_stream()) {
    return Status::NotFound("stream not found", stream_id);
  }
  FillStreamInfo(resp.stream(), info);
  return Status::OK();
}

Status CDCClient::GetTableMetadata(const string& table_name_or_id,
                                   bool by_id,
                                   CDCTableMetadata* metadata) {
  master::GetTableSchemaRequestPB req;
  master::GetTableSchemaResponsePB resp;
  if (by_id) {
    req.mutable_table()->set_table_id(table_name_or_id);
  } else {
    req.mutable_table()->set_table_name(table_name_or_id);
  }
  RETURN_NOT_OK(CallMaster(&MasterServiceProxy::GetTableSchema, req, &resp,
                           "GetTableSchema"));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  Schema schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(resp.schema(), &schema),
                        "could not decode table schema");
  metadata->schema = std::move(schema);
  metadata->table_id = resp.table_id();
  metadata->table_name = resp.table_name();
  if (resp.has_authz_token()) {
    metadata->has_authz_token = true;
    metadata->authz_token = resp.authz_token();
  } else {
    metadata->has_authz_token = false;
  }
  return Status::OK();
}

Status CDCClient::GetTabletLocations(const string& table_id,
                                     vector<CDCTabletInfo>* tablets) {
  tablets->clear();
  string partition_key_start;
  for (int page = 0; page < kMaxTabletLocationPages; page++) {
    master::GetTableLocationsRequestPB req;
    master::GetTableLocationsResponsePB resp;
    req.mutable_table()->set_table_id(table_id);
    req.set_partition_key_start(partition_key_start);
    req.set_max_returned_locations(kTabletLocationsPageSize);
    // Use the non-interned representation for simplicity: each replica carries
    // its own TSInfoPB.
    req.set_intern_ts_infos_in_response(false);

    RETURN_NOT_OK(CallMaster(&MasterServiceProxy::GetTableLocations, req, &resp,
                             "GetTableLocations"));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    if (resp.tablet_locations().empty()) {
      break;
    }

    // The master returns the tablet containing 'partition_key_start' plus every
    // tablet after it. For a table whose key space is not covered all the way to
    // +infinity (e.g. a bounded range partition), a request whose start key is at
    // or past the final tablet's upper bound echoes that final tablet again
    // rather than returning an empty page. Detect that non-advancing echo via the
    // last tablet's exclusive end key and stop, so a bounded table does not loop
    // forever re-adding its last tablet.
    const auto& last = *resp.tablet_locations().rbegin();
    string end_key;
    if (last.has_partition() && last.partition().has_partition_key_end()) {
      end_key = last.partition().partition_key_end();
    } else if (last.has_end_key()) {
      end_key = last.end_key();
    }
    if (!partition_key_start.empty() && !end_key.empty() &&
        end_key <= partition_key_start) {
      // No progress beyond the previous page: everything here was already
      // collected on an earlier iteration.
      break;
    }

    for (const auto& tl : resp.tablet_locations()) {
      CDCTabletInfo info;
      info.tablet_id = tl.tablet_id();
      for (const auto& replica : tl.deprecated_replicas()) {
        if (replica.ts_info().rpc_addresses().empty()) {
          continue;
        }
        const HostPort hp = HostPortFromPB(replica.ts_info().rpc_addresses(0));
        info.replicas.emplace_back(hp);
        if (replica.role() == consensus::RaftPeerPB::LEADER) {
          info.leader = hp;
        }
      }
      tablets->emplace_back(std::move(info));
    }

    if (end_key.empty()) {
      // Reached the unbounded end of the partition range.
      break;
    }
    // Advance to the next page using the last tablet's exclusive end key.
    partition_key_start = end_key;
  }
  return Status::OK();
}

Status CDCClient::GetCDCProxy(const HostPort& hp,
                              shared_ptr<CDCServiceProxy>* proxy) {
  const string key = hp.ToString();
  {
    std::lock_guard<simple_spinlock> l(proxy_lock_);
    auto it = cdc_proxies_.find(key);
    if (it != cdc_proxies_.end()) {
      *proxy = it->second;
      return Status::OK();
    }
  }

  vector<Sockaddr> resolved;
  RETURN_NOT_OK_PREPEND(hp.ResolveAddresses(&resolved),
                        Substitute("could not resolve tablet server '$0'", key));
  if (resolved.empty()) {
    return Status::NetworkError(
        Substitute("tablet server '$0' did not resolve to any endpoint", key));
  }
  auto p = std::make_shared<CDCServiceProxy>(messenger_, resolved.front(), hp.host());
  {
    std::lock_guard<simple_spinlock> l(proxy_lock_);
    // Another thread may have inserted concurrently; keep the existing one.
    auto it = cdc_proxies_.find(key);
    if (it != cdc_proxies_.end()) {
      *proxy = it->second;
    } else {
      cdc_proxies_[key] = p;
      *proxy = p;
    }
  }
  return Status::OK();
}

Status CDCClient::GetChanges(const HostPort& leader,
                             const GetChangesRequestPB& req,
                             GetChangesResponsePB* resp) {
  if (!leader.Initialized()) {
    return Status::IllegalState("no known leader for tablet",
                                req.tablet_id());
  }
  shared_ptr<CDCServiceProxy> proxy;
  RETURN_NOT_OK(GetCDCProxy(leader, &proxy));
  RpcController rpc;
  rpc.set_timeout(rpc_timeout_);
  return proxy->GetChanges(req, resp, &rpc);
}

Status CDCClient::Checkpoint(const HostPort& leader,
                             const CheckpointRequestPB& req,
                             CheckpointResponsePB* resp) {
  if (!leader.Initialized()) {
    return Status::IllegalState("no known leader for tablet",
                                req.tablet_id());
  }
  shared_ptr<CDCServiceProxy> proxy;
  RETURN_NOT_OK(GetCDCProxy(leader, &proxy));
  RpcController rpc;
  rpc.set_timeout(rpc_timeout_);
  return proxy->Checkpoint(req, resp, &rpc);
}

}  // namespace cdc
}  // namespace kudu
