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

// remote_cluster.h -- RPC fan-out engine interface for "cluster gather".
//
// This header is the frozen M0 contract.  Downstream milestones M1, M2, and
// M3 build against these exact signatures.  Signature changes are coordinated
// changes only; do not silently diverge.
//
// Design reference: docs/design-docs/kudu_mcp_fanout_implementation.md
// sections 7 and 8.

#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "kudu/client/shared_ptr.h"  // IWYU pragma: keep
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

class ThreadPool;

namespace client {
class KuduClient;
}  // namespace client

namespace rpc {
class Messenger;
}  // namespace rpc

namespace tools {

class GatherResultsPB;

// ---------------------------------------------------------------------------
// Section: identifies which probes should be executed per server.
// ---------------------------------------------------------------------------

// A Section names one category of per-server data that the fan-out engine
// can collect.  The caller populates GatherOptions::sections to request any
// combination.  Probes that require superuser/admin authorization
// (kConsensus, kFlags, kMemory, kQuiescing) return UNAUTHORIZED for
// unprivileged callers; client-accessible sections are still returned.
enum class Section {
  // GetStatus: uuid, version_string, git_hash, bound addresses.
  kIdentity,
  // ListTablets: per-replica state, data-dir placement, on-disk size.
  kInventory,
  // GetConsensusState: Raft term, leader uuid, per-peer health.
  // Requires admin/superuser identity.
  kConsensus,
  // Quiesce(return_stats=true): quiescing state, active scanners, leader count.
  // Requires admin/superuser identity.  Read-only; does not change state.
  kQuiescing,
  // GetFlags: non-default flags with tags.
  // Requires admin/superuser identity.
  kFlags,
  // DumpMemTrackers: memory tracker tree (limit, current, peak per subsystem).
  // Requires admin/superuser identity.
  kMemory,
  // ServerClock: HybridTime for clock-skew analysis.
  kClock,
};

// ---------------------------------------------------------------------------
// GatherOptions: describes what the caller wants to gather.
// ---------------------------------------------------------------------------

// GatherOptions is built by the RunGather action runner from the action's
// flags and handed to ResolvePlan / RemoteGatherer::Run.
struct GatherOptions {
  // The selector scope: which servers / entities to target.
  enum class Scope {
    // Default: every registered tablet server.
    kCluster,
    // Explicit server subset by uuid or host.
    kServers,
    // All servers whose location field matches a prefix.
    kLocation,
    // All servers that host at least one replica of the named table.
    kTable,
    // Only the ~3 servers that host the named tablet's replicas.
    kTablet,
    // Like kTablet but resolved from a primary-key JSON value.
    kRow,
  };

  // The selector scope.  Default is kCluster (whole fleet).
  Scope scope = Scope::kCluster;

  // For kServers: UUIDs or host[:port] strings to contact.
  std::vector<std::string> servers;

  // For kLocation: location prefix to match (e.g. "/dc1/rack2").
  std::string location;

  // For kTable, kRow: table name.
  std::string table;

  // For kTablet: tablet identifier.
  std::string tablet_id;

  // For kRow: primary key as a JSON array (same format as "table locate_row").
  std::string row_pk_json;

  // Which sections to collect.  An empty set is treated as all sections.
  std::set<Section> sections;

  // When true, the engine also fetches /metrics via HTTP for each server
  // and merges the result into the ServerRecord.  Off by default because it
  // uses a separate transport (HTTP) and a different auth path.
  bool include_http_metrics = false;
};

// ---------------------------------------------------------------------------
// Resolution types: plan computed by ResolvePlan from master metadata only.
// ---------------------------------------------------------------------------

// A single resolved target server.  Populated entirely from master metadata
// (no per-server RPC) by ResolvePlan.
struct ResolvedTarget {
  std::string uuid;
  std::string host;       // primary RPC host:port
  std::string location;
  std::string version;    // software version as reported by the master
  int64_t millis_since_heartbeat = 0;

  // True when the master considers the server dead (stale heartbeat).
  // The engine skips the RPC for presumed-dead servers and marks their
  // record PRESUMED_DEAD rather than spending a full --timeout_ms.
  bool presumed_dead = false;

  // For entity scopes (kTablet, kRow, kTable with per-tablet scoping):
  // the tablet IDs on this server that the probes should be restricted to.
  // Empty for cluster/server/location scopes (probe all tablets).
  std::vector<std::string> scoped_tablet_ids;
};

// The fully resolved plan: concrete target list plus projection type.
// Produced by ResolvePlan; consumed by RemoteGatherer::Run.
struct ResolvedPlan {
  std::vector<ResolvedTarget> targets;

  // True when the output should be projected around entities (tablet/row)
  // rather than around servers.  Set for kTablet and kRow scopes.
  bool entity_centric = false;

  // Human-readable description of the applied selector, e.g.
  // "all 12 tablet servers" or "tablet abcdef123 (3 replica-holders)".
  std::string applied_selector;
};

// ---------------------------------------------------------------------------
// ResolvePlan: master-only resolution step.  M1 owns the body.
// ---------------------------------------------------------------------------

// Resolves a GatherOptions selector into a concrete target plan using only
// master metadata (no per-server RPC).  On success, *out_plan is populated.
//
// Possible errors:
//   NotFound    -- named table / tablet does not exist.
//   InvalidArgument -- row_pk_json cannot be decoded against the table schema.
//   InvalidArgument -- more than one entity scope is set.
//
// The client must already be connected (CreateKuduClient succeeded).
//
// M1 owns the implementation body.  This stub returns Status::NotSupported.
Status ResolvePlan(client::KuduClient* client,
                   const GatherOptions& opts,
                   ResolvedPlan* out_plan) WARN_UNUSED_RESULT;

// ---------------------------------------------------------------------------
// RemoteGatherer: fan-out engine.  M2 owns the body.
// ---------------------------------------------------------------------------

// RemoteGatherer executes the fan-out: for each target in a ResolvedPlan it
// constructs four service proxies (Generic, TabletServerService,
// TabletServerAdmin, Consensus), submits the selected probes to a bounded
// ThreadPool, collects results under a spinlock, and assembles the output PB.
//
// Usage:
//   unique_ptr<RemoteGatherer> g;
//   RETURN_NOT_OK(RemoteGatherer::Create(master_addrs, &g));
//   GatherResultsPB out;
//   RETURN_NOT_OK(g->Run(opts, &out));
//
// Create() is called once; Run() is called once per gather operation.
// RemoteGatherer is not designed for reuse across multiple gather operations.
class RemoteGatherer {
 public:
  // Constructs a RemoteGatherer connected to the given masters.
  //
  // Internally this function (M2 body):
  //   - Calls BuildMessenger("remote-gather", &messenger_) for a single
  //     shared RPC messenger honoring all TLS/SASL/negotiation settings.
  //   - Calls CreateKuduClient(master_addresses, &client_) for target
  //     resolution and ListTabletServers discovery.
  //   - Builds a ThreadPool("remote-gather-fetch") bounded to
  //     FLAGS_fetch_info_concurrency threads with a 10ms idle timeout,
  //     mirroring the ksck fetch pool (ksck.cc:259-266).
  //
  // M2 owns the implementation body.  The M0 stub returns NotSupported.
  static Status Create(const std::vector<std::string>& master_addresses,
                       std::unique_ptr<RemoteGatherer>* out) WARN_UNUSED_RESULT;

  // Runs the full gather: calls ResolvePlan, then fans out the selected
  // probes concurrently across the resolved targets, then computes the
  // Rollup and optional entity projection, then fills *out.
  //
  // Per-target failures (timeout, unreachable, unauthorized) are isolated:
  // the failed server's record is annotated with the appropriate ProbeStatus
  // codes and the rest of the report is still returned.
  //
  // M2 owns the implementation body.  The M0 stub returns NotSupported.
  Status Run(const GatherOptions& opts, GatherResultsPB* out) WARN_UNUSED_RESULT;

  ~RemoteGatherer();

 private:
  RemoteGatherer() = default;

  std::shared_ptr<rpc::Messenger> messenger_;
  client::sp::shared_ptr<client::KuduClient> client_;
  std::unique_ptr<ThreadPool> pool_;

  DISALLOW_COPY_AND_ASSIGN(RemoteGatherer);
};

}  // namespace tools
}  // namespace kudu
