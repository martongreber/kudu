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

// remote_cluster.cc -- RPC fan-out engine for "cluster gather".
//
// Milestone assignments:
//   M1 -- ResolvePlan body (target resolution from master metadata).
//   M2 -- RemoteGatherer::Create and Run (fan-out loop + probes + rollup).
//   M3 -- entity-scoped projection (ReplicaSetView construction).

#include "kudu/tools/remote_cluster.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_predicate.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"  // IWYU pragma: keep
#include "kudu/client/value.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/server/server_base.pb.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/locks.h"
#include "kudu/util/mem_tracker.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

// FLAGS_fetch_info_concurrency is DEFINED at ksck.cc:63 (default 20).
// FLAGS_timeout_ms is DEFINED at tool_action_common.cc:123.
// Both are referenced here via DECLARE to avoid duplicate-registration errors.
DECLARE_int32(fetch_info_concurrency);
DECLARE_int64(timeout_ms);

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTablet;
using kudu::client::KuduValue;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::GetConsensusStateRequestPB;
using kudu::consensus::GetConsensusStateResponsePB;
using kudu::master::ListTabletServersRequestPB;
using kudu::master::ListTabletServersResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::TServerStatePB;
using kudu::rpc::Messenger;
using kudu::rpc::RpcController;
using kudu::server::DumpMemTrackersRequestPB;
using kudu::server::DumpMemTrackersResponsePB;
using kudu::server::GenericServiceProxy;
using kudu::server::GetFlagsRequestPB;
using kudu::server::GetFlagsResponsePB;
using kudu::server::GetStatusRequestPB;
using kudu::server::GetStatusResponsePB;
using kudu::server::ServerClockRequestPB;
using kudu::server::ServerClockResponsePB;
using kudu::tserver::ListTabletsRequestPB;
using kudu::tserver::ListTabletsResponsePB;
using kudu::tserver::QuiesceTabletServerRequestPB;
using kudu::tserver::QuiesceTabletServerResponsePB;
using kudu::tserver::TabletServerAdminServiceProxy;
using kudu::tserver::TabletServerServiceProxy;
using std::make_shared;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// A server whose heartbeat is older than this threshold (ms) is considered
// presumed-dead by the gather engine.  The RPC is skipped for such servers;
// their record is annotated PRESUMED_DEAD rather than spending a full
// --timeout_ms waiting for a connection that will almost certainly time out.
// 30 s is chosen as half the master's default 60-second replica timeout.
static const int64_t kPresumptiveDeadMs = 30000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

MonoDelta GetDefaultTimeout() {
  return MonoDelta::FromMilliseconds(FLAGS_timeout_ms);
}

// Returns true if the status indicates an authorization failure (the method
// is not accessible to the caller).  Replicates the predicate from ksck.cc.
bool IsNotAuthorizedMethodAccess(const Status& s) {
  return s.IsRemoteError() &&
         s.ToString().find("Not authorized: unauthorized access to method")
             != string::npos;
}

// Map a Section enum value to the string label used in the ProbeStatus.
string SectionToString(Section s) {
  switch (s) {
    case Section::kIdentity:  return "identity";
    case Section::kInventory: return "inventory";
    case Section::kConsensus: return "consensus";
    case Section::kQuiescing: return "quiescing";
    case Section::kFlags:     return "flags";
    case Section::kMemory:    return "memory";
    case Section::kClock:     return "clock";
  }
  return "unknown";
}

// Returns true if the given section is requested in opts (an empty set means
// all sections are requested).
bool SectionRequested(const set<Section>& sections, Section s) {
  return sections.empty() || sections.count(s) > 0;
}

// Append a ProbeStatus entry to a ServerRecord.
void AddProbeStatus(GatherResultsPB::ServerRecord* record,
                    Section section,
                    GatherResultsPB::ProbeStatus::Code code,
                    const string& message = "") {
  auto* ps = record->add_section_status();
  ps->set_section(SectionToString(section));
  ps->set_code(code);
  if (!message.empty()) {
    ps->set_message(message);
  }
}

// Convert a kudu::PartitionPB (from common.pb.h) to a short range string.
// Uses the global ::kudu:: namespace qualifier to avoid conflict with
// kudu::tools::PartitionPB (from tool.pb.h) which does NOT have
// partition_key_start/end fields.
string PartitionPBToString(const ::kudu::PartitionPB& pb) {
  string start = strings::CHexEscape(pb.partition_key_start());
  string end   = strings::CHexEscape(pb.partition_key_end());
  if (start.empty() && end.empty()) {
    return "[unbounded]";
  }
  if (start.empty()) {
    return Substitute("[-inf, $0)", end);
  }
  if (end.empty()) {
    return Substitute("[$0, +inf)", start);
  }
  return Substitute("[$0, $1)", start, end);
}

// Flatten a recursive MemTrackerPB tree into a flat list of MemTrackerEntry
// messages in the given ServerRecord.
void FlattenMemTrackers(const MemTrackerPB& tracker,
                        GatherResultsPB::ServerRecord* record) {
  auto* entry = record->add_mem_trackers();
  entry->set_id(tracker.id());
  if (tracker.has_parent_id()) {
    entry->set_parent_id(tracker.parent_id());
  }
  entry->set_limit_bytes(tracker.limit());
  entry->set_current_consumption_bytes(tracker.current_consumption());
  entry->set_peak_consumption_bytes(tracker.peak_consumption());
  for (const auto& child : tracker.child_trackers()) {
    FlattenMemTrackers(child, record);
  }
}

// ---------------------------------------------------------------------------
// RemoteGatherServer: holds the four proxies for one tablet server.
// Standalone, NOT derived from any ksck class.
// ---------------------------------------------------------------------------

class RemoteGatherServer {
 public:
  RemoteGatherServer() = default;

  // Initializes the four service proxies for the given host:port.
  // Mirrors RemoteKsckTabletServer::Init (ksck_remote.cc:203-215).
  Status Init(const shared_ptr<Messenger>& messenger, const string& host_port);

  // Runs the identity probe (GetStatus).
  // Populates uuid/version and sets the identity ProbeStatus.
  void ProbeIdentity(GatherResultsPB::ServerRecord* record);

  // Runs the inventory probe (ListTablets).
  // When scoped_tablet_ids is non-empty only those tablets are included.
  void ProbeInventory(const vector<string>& scoped_tablet_ids,
                      GatherResultsPB::ServerRecord* record);

  // Runs the clock probe (ServerClock).
  void ProbeClock(GatherResultsPB::ServerRecord* record);

  // Runs the quiescing probe (Quiesce with return_stats=true).
  // Read-only; does not change server quiescing state.
  void ProbeQuiescing(GatherResultsPB::ServerRecord* record);

  // Runs the consensus probe (GetConsensusState).
  // When scoped_tablet_ids is non-empty, only those tablets are requested.
  // Annotates term/leader_uuid/raft_role on the ReplicaInfo entries that
  // ProbeInventory already added to record.
  void ProbeConsensus(const string& ts_uuid,
                      const vector<string>& scoped_tablet_ids,
                      GatherResultsPB::ServerRecord* record);

  // Runs the flags probe (GetFlags, non-default flags only).
  void ProbeFlags(GatherResultsPB::ServerRecord* record);

  // Runs the memory probe (DumpMemTrackers).
  // Flattens the tracker tree into MemTrackerEntry records.
  void ProbeMemory(GatherResultsPB::ServerRecord* record);

 private:
  shared_ptr<GenericServiceProxy> generic_proxy_;
  shared_ptr<TabletServerServiceProxy> ts_proxy_;
  shared_ptr<TabletServerAdminServiceProxy> ts_admin_proxy_;
  shared_ptr<ConsensusServiceProxy> consensus_proxy_;

  DISALLOW_COPY_AND_ASSIGN(RemoteGatherServer);
};

Status RemoteGatherServer::Init(const shared_ptr<Messenger>& messenger,
                                const string& host_port) {
  vector<Sockaddr> addresses;
  RETURN_NOT_OK(ParseAddressList(
      host_port, tserver::TabletServer::kDefaultPort, &addresses));
  const auto& addr = addresses[0];
  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(host_port, tserver::TabletServer::kDefaultPort));
  const auto& host = hp.host();
  generic_proxy_   = make_shared<GenericServiceProxy>(messenger, addr, host);
  ts_proxy_        = make_shared<TabletServerServiceProxy>(messenger, addr, host);
  ts_admin_proxy_  = make_shared<TabletServerAdminServiceProxy>(messenger, addr, host);
  consensus_proxy_ = make_shared<ConsensusServiceProxy>(messenger, addr, host);
  return Status::OK();
}

void RemoteGatherServer::ProbeIdentity(GatherResultsPB::ServerRecord* record) {
  GetStatusRequestPB req;
  GetStatusResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  Status s = generic_proxy_->GetStatus(req, &resp, &rpc);
  if (!s.ok()) {
    GatherResultsPB::ProbeStatus::Code code =
        s.IsTimedOut() ? GatherResultsPB::ProbeStatus::TIMED_OUT
        : IsNotAuthorizedMethodAccess(s) ? GatherResultsPB::ProbeStatus::UNAUTHORIZED
                                         : GatherResultsPB::ProbeStatus::UNREACHABLE;
    AddProbeStatus(record, Section::kIdentity, code, s.message().ToString());
    record->set_status_error(s.ToString());
    return;
  }
  AddProbeStatus(record, Section::kIdentity, GatherResultsPB::ProbeStatus::OK);
  if (resp.status().has_version_info()) {
    record->set_version(resp.status().version_info().version_string());
  }
}

void RemoteGatherServer::ProbeInventory(const vector<string>& scoped_tablet_ids,
                                        GatherResultsPB::ServerRecord* record) {
  ListTabletsRequestPB req;
  ListTabletsResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  req.set_need_schema_info(false);
  Status s = ts_proxy_->ListTablets(req, &resp, &rpc);
  if (!s.ok()) {
    GatherResultsPB::ProbeStatus::Code code =
        s.IsTimedOut() ? GatherResultsPB::ProbeStatus::TIMED_OUT
        : IsNotAuthorizedMethodAccess(s) ? GatherResultsPB::ProbeStatus::UNAUTHORIZED
                                         : GatherResultsPB::ProbeStatus::UNREACHABLE;
    AddProbeStatus(record, Section::kInventory, code, s.message().ToString());
    return;
  }
  if (resp.has_error()) {
    AddProbeStatus(record, Section::kInventory,
                   GatherResultsPB::ProbeStatus::UNREACHABLE,
                   resp.error().status().message());
    return;
  }
  AddProbeStatus(record, Section::kInventory, GatherResultsPB::ProbeStatus::OK);

  // Build a set for fast lookup if scope is restricted.
  set<string> tablet_id_filter(scoped_tablet_ids.begin(), scoped_tablet_ids.end());

  for (const auto& ss : resp.status_and_schema()) {
    const auto& ts = ss.tablet_status();
    if (!tablet_id_filter.empty() &&
        tablet_id_filter.find(ts.tablet_id()) == tablet_id_filter.end()) {
      continue;
    }
    auto* ri = record->add_replicas();
    ri->set_tablet_id(ts.tablet_id());
    ri->set_table_name(ts.table_name());
    if (ts.has_table_id()) {
      ri->set_table_id(ts.table_id());
    }
    if (ts.has_state()) {
      ri->set_state(ts.state());
    }
    if (ts.has_tablet_data_state()) {
      ri->set_tablet_data_state(ts.tablet_data_state());
    }
    ri->set_last_status(ts.last_status());
    if (ts.has_partition()) {
      ri->set_partition(PartitionPBToString(ts.partition()));
    }
    if (ts.has_estimated_on_disk_size()) {
      ri->set_estimated_on_disk_size(ts.estimated_on_disk_size());
    }
    for (const auto& dir : ts.data_dirs()) {
      ri->add_data_dirs(dir);
    }
    // term and leader_uuid are filled in by ProbeConsensus.
  }
}

void RemoteGatherServer::ProbeClock(GatherResultsPB::ServerRecord* record) {
  ServerClockRequestPB req;
  ServerClockResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  Status s = generic_proxy_->ServerClock(req, &resp, &rpc);
  if (!s.ok()) {
    GatherResultsPB::ProbeStatus::Code code =
        s.IsTimedOut() ? GatherResultsPB::ProbeStatus::TIMED_OUT
        : IsNotAuthorizedMethodAccess(s) ? GatherResultsPB::ProbeStatus::UNAUTHORIZED
                                         : GatherResultsPB::ProbeStatus::UNREACHABLE;
    AddProbeStatus(record, Section::kClock, code, s.message().ToString());
    return;
  }
  AddProbeStatus(record, Section::kClock, GatherResultsPB::ProbeStatus::OK);
  if (resp.has_timestamp()) {
    record->set_hybrid_time(static_cast<int64_t>(resp.timestamp()));
  }
}

void RemoteGatherServer::ProbeQuiescing(GatherResultsPB::ServerRecord* record) {
  QuiesceTabletServerRequestPB req;
  QuiesceTabletServerResponsePB resp;
  req.set_return_stats(true);
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  rpc.RequireServerFeature(tserver::TabletServerFeatures::QUIESCING);
  Status s = ts_admin_proxy_->Quiesce(req, &resp, &rpc);
  if (!s.ok()) {
    GatherResultsPB::ProbeStatus::Code code =
        s.IsTimedOut() ? GatherResultsPB::ProbeStatus::TIMED_OUT
        : IsNotAuthorizedMethodAccess(s) ? GatherResultsPB::ProbeStatus::UNAUTHORIZED
                                         : GatherResultsPB::ProbeStatus::UNREACHABLE;
    AddProbeStatus(record, Section::kQuiescing, code, s.message().ToString());
    return;
  }
  AddProbeStatus(record, Section::kQuiescing, GatherResultsPB::ProbeStatus::OK);
  if (resp.has_is_quiescing()) {
    record->set_is_quiescing(resp.is_quiescing());
  }
  if (resp.has_num_active_scanners()) {
    record->set_num_active_scanners(resp.num_active_scanners());
  }
  if (resp.has_num_leaders()) {
    record->set_num_leaders(resp.num_leaders());
  }
}

void RemoteGatherServer::ProbeConsensus(const string& ts_uuid,
                                        const vector<string>& scoped_tablet_ids,
                                        GatherResultsPB::ServerRecord* record) {
  GetConsensusStateRequestPB req;
  GetConsensusStateResponsePB resp;
  req.set_dest_uuid(ts_uuid);
  // When scoped, request only those tablet IDs to minimize response size.
  for (const auto& tid : scoped_tablet_ids) {
    req.add_tablet_ids(tid);
  }
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  Status s = consensus_proxy_->GetConsensusState(req, &resp, &rpc);
  if (!s.ok()) {
    GatherResultsPB::ProbeStatus::Code code =
        s.IsTimedOut() ? GatherResultsPB::ProbeStatus::TIMED_OUT
        : IsNotAuthorizedMethodAccess(s) ? GatherResultsPB::ProbeStatus::UNAUTHORIZED
                                         : GatherResultsPB::ProbeStatus::UNREACHABLE;
    AddProbeStatus(record, Section::kConsensus, code, s.message().ToString());
    return;
  }
  AddProbeStatus(record, Section::kConsensus, GatherResultsPB::ProbeStatus::OK);

  // Build a map from tablet_id -> cstate for quick lookup.
  unordered_map<string, const consensus::ConsensusStatePB*> cstate_map;
  for (const auto& info : resp.tablets()) {
    if (info.has_cstate()) {
      cstate_map[info.tablet_id()] = &info.cstate();
    }
  }

  // Annotate ReplicaInfo entries that inventory already added.
  for (int i = 0; i < record->replicas_size(); ++i) {
    auto* ri = record->mutable_replicas(i);
    auto it = cstate_map.find(ri->tablet_id());
    if (it == cstate_map.end()) continue;
    const auto& cstate = *it->second;
    ri->set_term(cstate.current_term());
    if (cstate.has_leader_uuid()) {
      ri->set_leader_uuid(cstate.leader_uuid());
    }
    // Determine the Raft role of this server for this tablet.
    if (cstate.has_leader_uuid() && cstate.leader_uuid() == ts_uuid) {
      ri->set_raft_role("LEADER");
    } else {
      ri->set_raft_role("FOLLOWER");
    }
  }
}

void RemoteGatherServer::ProbeFlags(GatherResultsPB::ServerRecord* record) {
  GetFlagsRequestPB req;
  GetFlagsResponsePB resp;
  // Request all non-default flags (no tag filter, all_flags=false).
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  Status s = generic_proxy_->GetFlags(req, &resp, &rpc);
  if (!s.ok()) {
    GatherResultsPB::ProbeStatus::Code code =
        s.IsTimedOut() ? GatherResultsPB::ProbeStatus::TIMED_OUT
        : IsNotAuthorizedMethodAccess(s) ? GatherResultsPB::ProbeStatus::UNAUTHORIZED
                                         : GatherResultsPB::ProbeStatus::UNREACHABLE;
    AddProbeStatus(record, Section::kFlags, code, s.message().ToString());
    return;
  }
  AddProbeStatus(record, Section::kFlags, GatherResultsPB::ProbeStatus::OK);
  for (const auto& flag : resp.flags()) {
    // Skip flags at their default value.
    if (flag.has_is_default_value() && flag.is_default_value()) {
      continue;
    }
    auto* fe = record->add_flags();
    fe->set_name(flag.name());
    fe->set_value(flag.value());
    for (const auto& tag : flag.tags()) {
      fe->add_tags(tag);
    }
  }
}

void RemoteGatherServer::ProbeMemory(GatherResultsPB::ServerRecord* record) {
  DumpMemTrackersRequestPB req;
  DumpMemTrackersResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(GetDefaultTimeout());
  // DumpMemTrackers RPC response carries root_tracker (MemTrackerPB) which
  // contains nested child_trackers.  Confirmed against server_base.proto:189.
  Status s = generic_proxy_->DumpMemTrackers(req, &resp, &rpc);
  if (!s.ok()) {
    GatherResultsPB::ProbeStatus::Code code =
        s.IsTimedOut() ? GatherResultsPB::ProbeStatus::TIMED_OUT
        : IsNotAuthorizedMethodAccess(s) ? GatherResultsPB::ProbeStatus::UNAUTHORIZED
                                         : GatherResultsPB::ProbeStatus::UNREACHABLE;
    AddProbeStatus(record, Section::kMemory, code, s.message().ToString());
    return;
  }
  AddProbeStatus(record, Section::kMemory, GatherResultsPB::ProbeStatus::OK);
  if (resp.has_root_tracker()) {
    FlattenMemTrackers(resp.root_tracker(), record);
  }
}

// ---------------------------------------------------------------------------
// ResolvePlan helpers
// ---------------------------------------------------------------------------

// Build a ResolvedTarget from a ListTabletServersResponsePB::Entry.
// Returns a target with uuid=="" if the entry has no registration or
// no RPC addresses.
ResolvedTarget TargetFromEntry(const ListTabletServersResponsePB::Entry& entry) {
  ResolvedTarget t;
  t.uuid = entry.instance_id().permanent_uuid();
  if (!entry.has_registration()) {
    return t;
  }
  const auto& reg = entry.registration();
  if (reg.rpc_addresses_size() > 0) {
    HostPort hp = HostPortFromPB(reg.rpc_addresses(0));
    t.host = hp.ToString();
  }
  t.version = reg.software_version();
  t.location = entry.location();
  t.millis_since_heartbeat = entry.millis_since_heartbeat();
  t.presumed_dead = (t.millis_since_heartbeat > kPresumptiveDeadMs);
  return t;
}

// ---------------------------------------------------------------------------
// Row-to-tablet resolution (mirrors LocateRow in tool_action_table.cc).
// Uses JsonReader (kudu/util/jsonreader.h) to parse the primary key JSON array
// so that no #include appears inside a function body.
// Returns the owning tablet_id in *tablet_id_out, or error.
// ---------------------------------------------------------------------------
Status ResolveRowToTablet(KuduClient* client,
                          const string& table_name,
                          const string& row_pk_json,
                          string* tablet_id_out) {
  client::sp::shared_ptr<KuduTable> table;
  RETURN_NOT_OK_PREPEND(client->OpenTable(table_name, &table),
                        Substitute("cannot open table '$0'", table_name));

  const auto& schema = table->schema();
  vector<int> key_indexes;
  schema.GetPrimaryKeyColumnIndexes(&key_indexes);

  // Parse the JSON primary key array with JsonReader (same pattern as
  // LocateRow in tool_action_table.cc).
  JsonReader reader(row_pk_json);
  RETURN_NOT_OK(reader.Init());
  vector<const rapidjson::Value*> values;
  RETURN_NOT_OK(reader.ExtractObjectArray(reader.root(), nullptr, &values));

  if (values.size() != key_indexes.size()) {
    return Status::InvalidArgument(
        Substitute("wrong number of key columns: expected $0 but got $1",
                   key_indexes.size(), values.size()));
  }

  vector<unique_ptr<KuduPredicate>> predicates;
  for (int i = 0; i < static_cast<int>(values.size()); ++i) {
    const int key_idx = key_indexes[i];
    const auto& col = schema.Column(key_idx);
    const auto& col_name = col.name();
    const auto type = col.type();
    const auto* val = values[i];
    switch (type) {
      case KuduColumnSchema::INT8:
      case KuduColumnSchema::INT16:
      case KuduColumnSchema::INT32:
      case KuduColumnSchema::INT64:
      case KuduColumnSchema::DATE:
      case KuduColumnSchema::SERIAL:
      case KuduColumnSchema::UNIXTIME_MICROS: {
        int64_t v;
        RETURN_NOT_OK_PREPEND(reader.ExtractInt64(val, nullptr, &v),
                              Substitute("bad value for column '$0'", col_name));
        predicates.emplace_back(
            table->NewComparisonPredicate(col_name, KuduPredicate::EQUAL,
                                          KuduValue::FromInt(v)));
        break;
      }
      case KuduColumnSchema::BINARY:
      case KuduColumnSchema::STRING:
      case KuduColumnSchema::VARCHAR: {
        string v;
        RETURN_NOT_OK_PREPEND(reader.ExtractString(val, nullptr, &v),
                              Substitute("bad value for column '$0'", col_name));
        predicates.emplace_back(
            table->NewComparisonPredicate(col_name, KuduPredicate::EQUAL,
                                          KuduValue::CopyString(v)));
        break;
      }
      case KuduColumnSchema::FLOAT:
      case KuduColumnSchema::DOUBLE: {
        double v;
        RETURN_NOT_OK_PREPEND(reader.ExtractDouble(val, nullptr, &v),
                              Substitute("bad value for column '$0'", col_name));
        predicates.emplace_back(
            table->NewComparisonPredicate(col_name, KuduPredicate::EQUAL,
                                          KuduValue::FromDouble(v)));
        break;
      }
      default:
        return Status::NotSupported(
            Substitute("unsupported key column type $0 for '$1'",
                       KuduColumnSchema::DataTypeToString(type), col_name));
    }
  }

  vector<KuduScanToken*> tokens;
  ElementDeleter deleter(&tokens);
  KuduScanTokenBuilder builder(table.get());
  RETURN_NOT_OK(builder.SetSelection(KuduClient::ReplicaSelection::LEADER_ONLY));
  for (auto& p : predicates) {
    RETURN_NOT_OK(builder.AddConjunctPredicate(p.release()));
  }
  RETURN_NOT_OK(builder.Build(&tokens));

  if (tokens.empty()) {
    return Status::NotFound("row does not belong to any existing tablet",
                            row_pk_json);
  }
  *tablet_id_out = tokens[0]->tablet().id();
  return Status::OK();
}

// ---------------------------------------------------------------------------
// Find the replica holders for a given tablet_id.
// Uses KuduClient::GetTablet (public API, KUDU_NO_EXPORT, no friend access).
// ---------------------------------------------------------------------------
struct TabletReplicaInfo {
  string uuid;
  string host;   // host:port
};

Status FindTabletReplicas(KuduClient* client,
                          const string& tablet_id,
                          vector<TabletReplicaInfo>* replicas_out) {
  KuduTablet* tablet_ptr = nullptr;
  RETURN_NOT_OK_PREPEND(client->GetTablet(tablet_id, &tablet_ptr),
                        Substitute("cannot look up tablet '$0'", tablet_id));
  unique_ptr<KuduTablet> tablet(tablet_ptr);
  for (const auto* replica : tablet->replicas()) {
    TabletReplicaInfo ri;
    ri.uuid = replica->ts().uuid();
    ri.host = Substitute("$0:$1", replica->ts().hostname(), replica->ts().port());
    replicas_out->push_back(ri);
  }
  return Status::OK();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// M1: ResolvePlan
// ---------------------------------------------------------------------------

Status ResolvePlan(KuduClient* client,
                   const GatherOptions& opts,
                   ResolvedPlan* out_plan) {
  out_plan->entity_centric = false;

  // Step 1: List all tablet servers from the master using LeaderMasterProxy.
  // This avoids any need for KuduClient friendship or data_ access.
  // KuduClient::GetMasterAddresses() is a public (KUDU_NO_EXPORT) API that
  // returns a comma-separated string of master addresses.
  string addrs_csv = client->GetMasterAddresses();
  vector<string> master_addrs = strings::Split(addrs_csv, ",");
  // Filter empty strings that may appear from trailing commas.
  master_addrs.erase(
      std::remove_if(master_addrs.begin(), master_addrs.end(),
                     [](const string& s) { return s.empty(); }),
      master_addrs.end());

  LeaderMasterProxy master_proxy;
  MonoDelta timeout = GetDefaultTimeout();
  RETURN_NOT_OK_PREPEND(
      master_proxy.Init(master_addrs, timeout, timeout),
      "cannot connect to master for target resolution");

  ListTabletServersRequestPB req;
  req.set_include_states(true);
  ListTabletServersResponsePB ts_resp;
  RETURN_NOT_OK_PREPEND(
      (master_proxy.SyncRpc<ListTabletServersRequestPB,
                             ListTabletServersResponsePB>(
          req, &ts_resp, "ListTabletServers",
          &MasterServiceProxy::ListTabletServersAsync)),
      "ListTabletServers failed");
  if (ts_resp.has_error()) {
    return StatusFromPB(ts_resp.error().status());
  }

  // Step 2: Determine the target UUID set based on the selector.

  // For entity scopes (kTable, kTablet, kRow): UUID -> scoped_tablet_ids map.
  unordered_map<string, vector<string>> uuid_to_scoped_tablets;
  // For filtered scopes: set of target UUIDs.
  set<string> target_uuids;

  switch (opts.scope) {
    case GatherOptions::Scope::kCluster:
      // All tablet servers; no filtering.
      out_plan->applied_selector =
          Substitute("all $0 tablet server(s)", ts_resp.servers_size());
      break;

    case GatherOptions::Scope::kServers: {
      set<string> wanted(opts.servers.begin(), opts.servers.end());
      for (const auto& entry : ts_resp.servers()) {
        const string& uuid = entry.instance_id().permanent_uuid();
        if (wanted.count(uuid)) {
          target_uuids.insert(uuid);
          continue;
        }
        if (entry.has_registration() &&
            entry.registration().rpc_addresses_size() > 0) {
          HostPort hp = HostPortFromPB(entry.registration().rpc_addresses(0));
          if (wanted.count(hp.host()) || wanted.count(hp.ToString())) {
            target_uuids.insert(uuid);
          }
        }
      }
      out_plan->applied_selector =
          Substitute("$0 selected server(s)", target_uuids.size());
      break;
    }

    case GatherOptions::Scope::kLocation: {
      for (const auto& entry : ts_resp.servers()) {
        if (HasPrefixString(entry.location(), opts.location)) {
          target_uuids.insert(entry.instance_id().permanent_uuid());
        }
      }
      out_plan->applied_selector =
          Substitute("$0 server(s) with location prefix '$1'",
                     target_uuids.size(), opts.location);
      break;
    }

    case GatherOptions::Scope::kTable: {
      client::sp::shared_ptr<KuduTable> table;
      RETURN_NOT_OK_PREPEND(client->OpenTable(opts.table, &table),
                            Substitute("cannot open table '$0'", opts.table));
      vector<KuduScanToken*> tokens;
      ElementDeleter deleter(&tokens);
      KuduScanTokenBuilder builder(table.get());
      RETURN_NOT_OK(builder.Build(&tokens));
      for (const auto* tok : tokens) {
        const string& tid = tok->tablet().id();
        for (const auto* replica : tok->tablet().replicas()) {
          const string& uuid = replica->ts().uuid();
          target_uuids.insert(uuid);
          uuid_to_scoped_tablets[uuid].push_back(tid);
        }
      }
      out_plan->applied_selector =
          Substitute("$0 server(s) hosting table '$1' ($2 tablet(s))",
                     target_uuids.size(), opts.table, tokens.size());
      break;
    }

    case GatherOptions::Scope::kTablet: {
      vector<TabletReplicaInfo> replicas;
      RETURN_NOT_OK(FindTabletReplicas(client, opts.tablet_id, &replicas));
      if (replicas.empty()) {
        return Status::NotFound(
            Substitute("tablet '$0' has no replicas", opts.tablet_id));
      }
      for (const auto& ri : replicas) {
        target_uuids.insert(ri.uuid);
        uuid_to_scoped_tablets[ri.uuid].push_back(opts.tablet_id);
      }
      out_plan->entity_centric = true;
      out_plan->applied_selector =
          Substitute("tablet '$0' ($1 replica-holder(s))",
                     opts.tablet_id, replicas.size());
      break;
    }

    case GatherOptions::Scope::kRow: {
      if (opts.table.empty()) {
        return Status::InvalidArgument("--row scope requires --table to be set");
      }
      string owning_tablet_id;
      RETURN_NOT_OK(ResolveRowToTablet(client, opts.table, opts.row_pk_json,
                                        &owning_tablet_id));
      vector<TabletReplicaInfo> replicas;
      RETURN_NOT_OK(FindTabletReplicas(client, owning_tablet_id, &replicas));
      if (replicas.empty()) {
        return Status::NotFound(
            Substitute("tablet '$0' for row has no replicas", owning_tablet_id));
      }
      for (const auto& ri : replicas) {
        target_uuids.insert(ri.uuid);
        uuid_to_scoped_tablets[ri.uuid].push_back(owning_tablet_id);
      }
      out_plan->entity_centric = true;
      out_plan->applied_selector =
          Substitute("row in table '$0' -> tablet '$1' ($2 replica-holder(s))",
                     opts.table, owning_tablet_id, replicas.size());
      break;
    }
  }

  // Step 3: Build the target list from master metadata.
  for (const auto& entry : ts_resp.servers()) {
    const string& uuid = entry.instance_id().permanent_uuid();

    bool include = false;
    if (opts.scope == GatherOptions::Scope::kCluster) {
      include = true;
    } else {
      include = (target_uuids.count(uuid) > 0);
    }
    if (!include) continue;
    if (!entry.has_registration()) continue;
    if (entry.registration().rpc_addresses_size() == 0) continue;

    ResolvedTarget t = TargetFromEntry(entry);
    if (t.uuid.empty()) continue;

    // Attach scoped tablet IDs for entity scopes.
    auto it = uuid_to_scoped_tablets.find(uuid);
    if (it != uuid_to_scoped_tablets.end()) {
      t.scoped_tablet_ids = it->second;
    }

    out_plan->targets.push_back(std::move(t));
  }

  return Status::OK();
}

// ---------------------------------------------------------------------------
// M2: RemoteGatherer::Create
// ---------------------------------------------------------------------------

RemoteGatherer::~RemoteGatherer() = default;

// static
Status RemoteGatherer::Create(const vector<string>& master_addresses,
                              unique_ptr<RemoteGatherer>* out) {
  unique_ptr<RemoteGatherer> g(new RemoteGatherer());

  // Build a shared Messenger for all proxy connections.
  RETURN_NOT_OK(BuildMessenger("remote-gather", &g->messenger_));

  // Build a KuduClient with multi-master failover.  Use can_see_all_replicas
  // so that scan tokens expose all replica types (including non-voter).
  RETURN_NOT_OK(CreateKuduClient(master_addresses, &g->client_,
                                  /*can_see_all_replicas=*/true));

  // Build a bounded fetch ThreadPool mirroring ksck.cc:259-266.
  RETURN_NOT_OK(ThreadPoolBuilder("remote-gather-fetch")
                    .set_max_threads(FLAGS_fetch_info_concurrency)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(10))
                    .Build(&g->pool_));

  *out = std::move(g);
  return Status::OK();
}

// ---------------------------------------------------------------------------
// M2: RemoteGatherer::Run
// ---------------------------------------------------------------------------

Status RemoteGatherer::Run(const GatherOptions& opts, GatherResultsPB* out) {
  // --- M1: resolve plan ---
  ResolvedPlan plan;
  RETURN_NOT_OK_PREPEND(ResolvePlan(client_.get(), opts, &plan),
                        "target resolution failed");

  // --- Fill ClusterInfo ---
  auto* ci = out->mutable_cluster();
  // KuduClient::GetMasterAddresses() returns a CSV string (public API).
  {
    string addrs_csv = client_->GetMasterAddresses();
    vector<string> addrs = strings::Split(addrs_csv, ",");
    for (const auto& addr : addrs) {
      if (!addr.empty()) {
        ci->add_master_addresses(string(addr));
      }
    }
  }
  ci->set_applied_selector(plan.applied_selector);
  ci->set_fetch_info_concurrency(FLAGS_fetch_info_concurrency);
  ci->set_timeout_ms(FLAGS_timeout_ms);
  ci->set_report_time_unix_ms(static_cast<int64_t>(WallTime_Now() * 1000));

  // --- M2: concurrent fan-out ---
  const size_t num_targets = plan.targets.size();
  std::atomic<size_t> bad_servers(0);

  // Pre-allocate one record per target.  Each task writes to its own slot
  // so no inter-task data races; the spinlock ensures happens-before for
  // the final move.
  vector<GatherResultsPB::ServerRecord> records(num_targets);
  simple_spinlock records_lock;

  for (size_t idx = 0; idx < num_targets; ++idx) {
    const ResolvedTarget& target = plan.targets[idx];
    RETURN_NOT_OK(pool_->Submit([this, &opts, &target, &bad_servers,
                                  &records, &records_lock, idx]() {
      GatherResultsPB::ServerRecord rec;
      rec.set_uuid(target.uuid);
      rec.set_host(target.host);
      rec.set_location(target.location);
      rec.set_version(target.version);
      rec.set_millis_since_heartbeat(target.millis_since_heartbeat);

      if (target.presumed_dead) {
        // Skip RPC; annotate all requested sections as PRESUMED_DEAD.
        static const Section kAllSections[] = {
          Section::kIdentity, Section::kInventory, Section::kClock,
          Section::kQuiescing, Section::kConsensus, Section::kFlags,
          Section::kMemory
        };
        for (auto s : kAllSections) {
          if (SectionRequested(opts.sections, s)) {
            AddProbeStatus(&rec, s,
                           GatherResultsPB::ProbeStatus::PRESUMED_DEAD,
                           "server heartbeat is stale; RPC skipped");
          }
        }
        ++bad_servers;
        std::lock_guard<simple_spinlock> lk(records_lock);
        records[idx] = std::move(rec);
        return;
      }

      // Initialize the proxy bundle for this target.
      RemoteGatherServer srv;
      Status init_s = srv.Init(messenger_, target.host);
      if (!init_s.ok()) {
        static const Section kAllSections[] = {
          Section::kIdentity, Section::kInventory, Section::kClock,
          Section::kQuiescing, Section::kConsensus, Section::kFlags,
          Section::kMemory
        };
        for (auto s : kAllSections) {
          if (SectionRequested(opts.sections, s)) {
            AddProbeStatus(&rec, s,
                           GatherResultsPB::ProbeStatus::UNREACHABLE,
                           init_s.message().ToString());
          }
        }
        ++bad_servers;
        std::lock_guard<simple_spinlock> lk(records_lock);
        records[idx] = std::move(rec);
        return;
      }

      // Run selected probes in a fixed order.
      if (SectionRequested(opts.sections, Section::kIdentity)) {
        srv.ProbeIdentity(&rec);
      }
      if (SectionRequested(opts.sections, Section::kInventory)) {
        srv.ProbeInventory(target.scoped_tablet_ids, &rec);
      }
      if (SectionRequested(opts.sections, Section::kClock)) {
        srv.ProbeClock(&rec);
      }
      if (SectionRequested(opts.sections, Section::kQuiescing)) {
        srv.ProbeQuiescing(&rec);
      }
      if (SectionRequested(opts.sections, Section::kConsensus)) {
        srv.ProbeConsensus(target.uuid, target.scoped_tablet_ids, &rec);
      }
      if (SectionRequested(opts.sections, Section::kFlags)) {
        srv.ProbeFlags(&rec);
      }
      if (SectionRequested(opts.sections, Section::kMemory)) {
        srv.ProbeMemory(&rec);
      }

      // Mark skipped sections explicitly.
      static const Section kAllSections[] = {
        Section::kIdentity, Section::kInventory, Section::kClock,
        Section::kQuiescing, Section::kConsensus, Section::kFlags,
        Section::kMemory
      };
      for (auto s : kAllSections) {
        if (!SectionRequested(opts.sections, s)) {
          AddProbeStatus(&rec, s, GatherResultsPB::ProbeStatus::SKIPPED);
        }
      }

      std::lock_guard<simple_spinlock> lk(records_lock);
      records[idx] = std::move(rec);
    }));
  }
  pool_->Wait();

  // Move per-target records into the output PB.
  for (auto& rec : records) {
    *out->add_servers() = std::move(rec);
  }

  // --- M3: entity-centric projection ---
  if (plan.entity_centric) {
    // Collect all scoped tablet IDs across all targets.
    set<string> all_tablet_ids;
    for (const auto& t : plan.targets) {
      for (const auto& tid : t.scoped_tablet_ids) {
        all_tablet_ids.insert(tid);
      }
    }

    // Build a uuid -> ServerRecord* map for O(1) lookup.
    unordered_map<string, const GatherResultsPB::ServerRecord*> uuid_to_record;
    for (const auto& rec : out->servers()) {
      uuid_to_record[rec.uuid()] = &rec;
    }

    for (const auto& tid : all_tablet_ids) {
      auto* view = out->add_entities();
      view->set_tablet_id(tid);

      // Determine leader UUID and fill table_name/partition from the first
      // available replica that reported data for this tablet.
      string leader_uuid;
      for (const auto& target : plan.targets) {
        auto rec_it = uuid_to_record.find(target.uuid);
        if (rec_it == uuid_to_record.end()) continue;
        for (const auto& ri : rec_it->second->replicas()) {
          if (ri.tablet_id() != tid) continue;
          if (!view->has_table_name() && !ri.table_name().empty()) {
            view->set_table_name(ri.table_name());
          }
          if (!view->has_table_id() && ri.has_table_id()) {
            view->set_table_id(ri.table_id());
          }
          if (!view->has_partition() && ri.has_partition()) {
            view->set_partition(ri.partition());
          }
          if (leader_uuid.empty() && ri.has_leader_uuid()) {
            leader_uuid = ri.leader_uuid();
          }
        }
      }

      // Build one ReplicaCopy per target that hosts this tablet.
      for (const auto& target : plan.targets) {
        bool hosts_tablet = false;
        for (const auto& stid : target.scoped_tablet_ids) {
          if (stid == tid) { hosts_tablet = true; break; }
        }
        if (!hosts_tablet) continue;

        auto* copy = view->add_copies();
        copy->set_ts_uuid(target.uuid);
        copy->set_ts_host(target.host);

        // Find the ReplicaInfo for this tablet on this server.
        const GatherResultsPB::ReplicaInfo* ri = nullptr;
        auto rec_it = uuid_to_record.find(target.uuid);
        if (rec_it != uuid_to_record.end()) {
          for (const auto& rr : rec_it->second->replicas()) {
            if (rr.tablet_id() == tid) {
              ri = &rr;
              break;
            }
          }
        }

        if (ri != nullptr) {
          copy->set_is_leader(!leader_uuid.empty() &&
                              leader_uuid == target.uuid);
          copy->set_term(ri->term());
          if (ri->has_estimated_on_disk_size()) {
            copy->set_estimated_on_disk_size(ri->estimated_on_disk_size());
          }
          for (const auto& dir : ri->data_dirs()) {
            copy->add_data_dirs(dir);
          }
          if (ri->has_state()) {
            copy->set_state(ri->state());
          }
          if (ri->has_tablet_data_state()) {
            copy->set_tablet_data_state(ri->tablet_data_state());
          }
        } else {
          // Expected replica holder reported no data -> UNREACHABLE.
          copy->set_is_leader(false);
          auto* ps = copy->mutable_replica_probe_status();
          ps->set_section("inventory");
          ps->set_code(target.presumed_dead
                       ? GatherResultsPB::ProbeStatus::PRESUMED_DEAD
                       : GatherResultsPB::ProbeStatus::UNREACHABLE);
          ps->set_message(target.presumed_dead
                          ? "server heartbeat stale"
                          : "replica not reported by server");
        }
      }

      // Sort copies so the leader comes first.
      if (!leader_uuid.empty()) {
        auto* copies = view->mutable_copies();
        std::stable_sort(copies->begin(), copies->end(),
                         [&leader_uuid](
                             const GatherResultsPB::ReplicaCopy& a,
                             const GatherResultsPB::ReplicaCopy& b) {
                           return (a.ts_uuid() == leader_uuid) &&
                                  (b.ts_uuid() != leader_uuid);
                         });
      }
    }
  }

  // --- Rollup ---
  auto* rollup = out->mutable_rollup();

  vector<int64_t> replica_counts;
  vector<int64_t> disk_sizes;
  vector<int64_t> leader_counts;
  set<string> versions_seen;
  // flag_name -> (value -> set<uuid>)
  unordered_map<string, unordered_map<string, set<string>>> flag_groups;

  for (const auto& rec : out->servers()) {
    int64_t replica_count = rec.replicas_size();
    int64_t disk_bytes = 0;
    for (const auto& ri : rec.replicas()) {
      disk_bytes += ri.estimated_on_disk_size();
    }
    replica_counts.push_back(replica_count);
    disk_sizes.push_back(disk_bytes);

    int64_t leaders = 0;
    if (rec.has_num_leaders()) {
      leaders = rec.num_leaders();
    } else {
      for (const auto& ri : rec.replicas()) {
        if (ri.has_raft_role() && ri.raft_role() == "LEADER") {
          ++leaders;
        }
      }
    }
    leader_counts.push_back(leaders);

    if (!rec.version().empty()) {
      versions_seen.insert(rec.version());
    }

    for (const auto& fe : rec.flags()) {
      flag_groups[fe.name()][fe.value()].insert(rec.uuid());
    }
  }

  auto compute_stats = [](const vector<int64_t>& vals,
                           int64_t* total_out, int64_t* mn_out,
                           int64_t* mx_out, double* mean_out,
                           double* stddev_out) {
    if (vals.empty()) {
      *total_out = *mn_out = *mx_out = 0;
      *mean_out = *stddev_out = 0.0;
      return;
    }
    *total_out = std::accumulate(vals.begin(), vals.end(), int64_t{0});
    *mn_out = *std::min_element(vals.begin(), vals.end());
    *mx_out = *std::max_element(vals.begin(), vals.end());
    double n = static_cast<double>(vals.size());
    *mean_out = static_cast<double>(*total_out) / n;
    double sum_sq = 0.0;
    for (auto v : vals) {
      double d = static_cast<double>(v) - *mean_out;
      sum_sq += d * d;
    }
    *stddev_out = (n > 1) ? std::sqrt(sum_sq / (n - 1)) : 0.0;
  };

  {
    int64_t total, mn, mx;
    double mean, stddev;
    compute_stats(replica_counts, &total, &mn, &mx, &mean, &stddev);
    rollup->set_total_replica_count(total);
    rollup->set_min_replica_count(mn);
    rollup->set_max_replica_count(mx);
    rollup->set_mean_replica_count(mean);
    rollup->set_stddev_replica_count(stddev);
  }
  {
    int64_t total, mn, mx;
    double mean, stddev;
    compute_stats(disk_sizes, &total, &mn, &mx, &mean, &stddev);
    rollup->set_total_on_disk_bytes(total);
    rollup->set_min_on_disk_bytes(mn);
    rollup->set_max_on_disk_bytes(mx);
    rollup->set_mean_on_disk_bytes(mean);
    rollup->set_stddev_on_disk_bytes(stddev);
  }
  {
    int64_t total, mn, mx;
    double mean, stddev;
    compute_stats(leader_counts, &total, &mn, &mx, &mean, &stddev);
    rollup->set_total_leader_count(total);
    rollup->set_min_leader_count(mn);
    rollup->set_max_leader_count(mx);
    (void)mean; (void)stddev;  // leader mean/stddev not in rollup proto
  }

  for (const auto& v : versions_seen) {
    rollup->add_distinct_versions(v);
  }

  // Flag divergence: emit one group per (flag_name, value) pair when
  // a flag has more than one distinct value across the cluster.
  for (const auto& flag_kv : flag_groups) {
    if (flag_kv.second.size() <= 1) continue;
    for (const auto& val_kv : flag_kv.second) {
      auto* grp = rollup->add_flag_divergence_groups();
      grp->set_flag_name(flag_kv.first);
      grp->set_value(val_kv.first);
      for (const auto& uuid : val_kv.second) {
        grp->add_ts_uuids(uuid);
      }
    }
  }

  // Probe-status counts: for each (section, code) pair, count servers.
  unordered_map<string, int> status_count;
  for (const auto& rec : out->servers()) {
    for (const auto& ps : rec.section_status()) {
      string key = ps.section() + ":" +
                   std::to_string(static_cast<int>(ps.code()));
      ++status_count[key];
    }
  }
  for (const auto& kv : status_count) {
    auto* psc = rollup->add_servers_by_probe_status();
    size_t colon = kv.first.find(':');
    psc->set_section(kv.first.substr(0, colon));
    psc->set_probe_code(std::stoi(kv.first.substr(colon + 1)));
    psc->set_count(kv.second);
  }

  return Status::OK();
}

}  // namespace tools
}  // namespace kudu
