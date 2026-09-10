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

#include "kudu/tools/mcp_disposition.h"

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action.h"

using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// Shorthand for the flags in the raw table below, to keep rows readable.
constexpr bool kLocal = true;    // node_local
constexpr bool kSafe = false;    // not unsafe
constexpr bool kUnsafe = true;   // unsafe_* / raw-consensus edits

// The name of the test-only mode (present only in KUDU_CLI_TEST_TOOL_ENABLED
// builds). Its actions are not real MCP tools; see DispositionFor().
const char* const kTestModeName = "test";

// ---------------------------------------------------------------------------
// The curated disposition table.
//
// This is the authoritative safety spec, transcribed from the PRD
// (kudu_mcp_prd.md, section 6) and reconciled against the live action tree
// (`kudu --helpxml`). It is keyed by full command path relative to the root
// mode. Multi-level paths (submodes) are spelled out in full, e.g.
// "tserver quiesce status" and "table set_limit disk_size".
//
// Precedence rules used to classify (PRD section 6):
//   1. Interactive           -> EXCLUDE
//   2. Blocking              -> REJECT
//   3. Read-only             -> SURFACE (node-local reads tagged node_local)
//   4. Mutating: cluster     -> GATED; node-local -> EXCLUDE
//
// When adding a CLI action, add a row here as well; the startup coverage guard
// (ValidateDispositionCoverageOrDie) fails loudly until it is classified.
// ---------------------------------------------------------------------------
const vector<RawDispositionEntry>& RawEntries() {
  static const vector<RawDispositionEntry>* const kEntries =
      new vector<RawDispositionEntry>{
    // cluster (RPC, cluster-wide)
    {"cluster ksck",                             Disposition::SURFACE, !kLocal, kSafe},
    {"cluster gather",                           Disposition::SURFACE, !kLocal, kSafe},
    {"cluster rebalance",                        Disposition::REJECT,  !kLocal, kSafe},

    // diagnose (offline log/metric/TLS analysis)
    {"diagnose parse_stacks",                    Disposition::SURFACE, !kLocal, kSafe},
    {"diagnose parse_metrics",                   Disposition::SURFACE, !kLocal, kSafe},
    {"diagnose tls_debug",                       Disposition::SURFACE, !kLocal, kSafe},

    // fs (NODE-LOCAL -- on-disk filesystem of the local node)
    {"fs check",                                 Disposition::SURFACE, kLocal,  kSafe},
    {"fs format",                                Disposition::EXCLUDE, kLocal,  kSafe},
    {"fs list",                                  Disposition::SURFACE, kLocal,  kSafe},
    {"fs locate_block",                          Disposition::SURFACE, kLocal,  kSafe},
    {"fs update_dirs",                           Disposition::EXCLUDE, kLocal,  kSafe},
    {"fs upgrade_encryption_key",                Disposition::EXCLUDE, kLocal,  kSafe},
    {"fs dump block",                            Disposition::SURFACE, kLocal,  kSafe},
    {"fs dump cfile",                            Disposition::SURFACE, kLocal,  kSafe},
    {"fs dump tree",                             Disposition::SURFACE, kLocal,  kSafe},
    {"fs dump uuid",                             Disposition::SURFACE, kLocal,  kSafe},

    // hms (Hive Metastore integration, cluster-level)
    {"hms check",                                Disposition::SURFACE, !kLocal, kSafe},
    {"hms downgrade",                            Disposition::GATED,   !kLocal, kSafe},
    {"hms fix",                                  Disposition::GATED,   !kLocal, kSafe},
    {"hms list",                                 Disposition::SURFACE, !kLocal, kSafe},
    {"hms precheck",                             Disposition::SURFACE, !kLocal, kSafe},

    // local_replica (NODE-LOCAL -- usually requires the tserver STOPPED)
    {"local_replica copy_from_local",            Disposition::EXCLUDE, kLocal,  kSafe},
    {"local_replica copy_from_remote",           Disposition::EXCLUDE, kLocal,  kSafe},
    {"local_replica data_size",                  Disposition::SURFACE, kLocal,  kSafe},
    {"local_replica delete",                     Disposition::EXCLUDE, kLocal,  kSafe},
    {"local_replica list",                       Disposition::SURFACE, kLocal,  kSafe},
    {"local_replica cmeta print_replica_uuids",  Disposition::SURFACE, kLocal,  kSafe},
    {"local_replica cmeta rewrite_raft_config",  Disposition::EXCLUDE, kLocal,  kUnsafe},
    {"local_replica cmeta set_term",             Disposition::EXCLUDE, kLocal,  kUnsafe},
    {"local_replica cmeta unsafe_recreate",      Disposition::EXCLUDE, kLocal,  kUnsafe},
    {"local_replica tmeta delete_rowsets",       Disposition::EXCLUDE, kLocal,  kSafe},
    {"local_replica dump block_ids",             Disposition::SURFACE, kLocal,  kSafe},
    {"local_replica dump data_dirs",             Disposition::SURFACE, kLocal,  kSafe},
    {"local_replica dump meta",                  Disposition::SURFACE, kLocal,  kSafe},
    {"local_replica dump rowset",                Disposition::SURFACE, kLocal,  kSafe},
    {"local_replica dump wals",                  Disposition::SURFACE, kLocal,  kSafe},

    // master (RPC, cluster-wide; 'run' is node-local)
    {"master dump_memtrackers",                  Disposition::SURFACE, !kLocal, kSafe},
    {"master get_flags",                         Disposition::SURFACE, !kLocal, kSafe},
    {"master run",                               Disposition::REJECT,  !kLocal, kSafe},
    {"master set_flag",                          Disposition::GATED,   !kLocal, kSafe},
    {"master set_flag_for_all",                  Disposition::GATED,   !kLocal, kSafe},
    {"master status",                            Disposition::SURFACE, !kLocal, kSafe},
    {"master timestamp",                         Disposition::SURFACE, !kLocal, kSafe},
    {"master list",                              Disposition::SURFACE, !kLocal, kSafe},
    {"master add",                               Disposition::GATED,   !kLocal, kSafe},
    {"master remove",                            Disposition::GATED,   !kLocal, kSafe},
    {"master unsafe_rebuild",                    Disposition::EXCLUDE, kLocal,  kUnsafe},
    {"master authz_cache refresh",               Disposition::GATED,   !kLocal, kSafe},

    // mcp (the MCP server itself; not in PRD section 6 -- see the progress log)
    {"mcp serve",                                Disposition::REJECT,  !kLocal, kSafe},

    // pbc (NODE-LOCAL -- protobuf container files)
    {"pbc dump",                                 Disposition::SURFACE, kLocal,  kSafe},
    {"pbc edit",                                 Disposition::EXCLUDE, kLocal,  kSafe},

    // perf
    {"perf loadgen",                             Disposition::REJECT,  !kLocal, kSafe},
    {"perf table_scan",                          Disposition::SURFACE, !kLocal, kSafe},
    {"perf tablet_scan",                         Disposition::SURFACE, !kLocal, kSafe},

    // remote_replica (RPC to a tserver)
    {"remote_replica check",                     Disposition::SURFACE, !kLocal, kSafe},
    {"remote_replica copy",                      Disposition::REJECT,  !kLocal, kSafe},
    {"remote_replica delete",                    Disposition::GATED,   !kLocal, kSafe},
    {"remote_replica dump",                      Disposition::SURFACE, !kLocal, kSafe},
    {"remote_replica list",                      Disposition::SURFACE, !kLocal, kSafe},
    {"remote_replica unsafe_change_config",      Disposition::GATED,   !kLocal, kUnsafe},

    // table (RPC, cluster-wide -- the workhorse mode)
    {"table add_column",                         Disposition::GATED,   !kLocal, kSafe},
    {"table add_range_partition",                Disposition::GATED,   !kLocal, kSafe},
    {"table clear_comment",                      Disposition::GATED,   !kLocal, kSafe},
    {"table column_remove_default",              Disposition::GATED,   !kLocal, kSafe},
    {"table column_set_block_size",              Disposition::GATED,   !kLocal, kSafe},
    {"table column_set_compression",             Disposition::GATED,   !kLocal, kSafe},
    {"table column_set_default",                 Disposition::GATED,   !kLocal, kSafe},
    {"table column_set_encoding",                Disposition::GATED,   !kLocal, kSafe},
    {"table column_set_comment",                 Disposition::GATED,   !kLocal, kSafe},
    {"table copy",                               Disposition::REJECT,  !kLocal, kSafe},
    {"table create",                             Disposition::GATED,   !kLocal, kSafe},
    {"table delete_column",                      Disposition::GATED,   !kLocal, kSafe},
    {"table delete",                             Disposition::GATED,   !kLocal, kSafe},
    {"table describe",                           Disposition::SURFACE, !kLocal, kSafe},
    {"table drop_range_partition",               Disposition::GATED,   !kLocal, kSafe},
    {"table get_extra_configs",                  Disposition::SURFACE, !kLocal, kSafe},
    {"table list_in_flight",                     Disposition::SURFACE, !kLocal, kSafe},
    {"table list",                               Disposition::SURFACE, !kLocal, kSafe},
    {"table locate_row",                         Disposition::SURFACE, !kLocal, kSafe},
    {"table recall",                             Disposition::GATED,   !kLocal, kSafe},
    {"table rename_column",                      Disposition::GATED,   !kLocal, kSafe},
    {"table rename_table",                       Disposition::GATED,   !kLocal, kSafe},
    {"table scan",                               Disposition::SURFACE, !kLocal, kSafe},
    {"table set_comment",                        Disposition::GATED,   !kLocal, kSafe},
    {"table set_extra_config",                   Disposition::GATED,   !kLocal, kSafe},
    {"table set_replication_factor",             Disposition::GATED,   !kLocal, kSafe},
    {"table statistics",                         Disposition::SURFACE, !kLocal, kSafe},
    {"table set_limit disk_size",                Disposition::GATED,   !kLocal, kSafe},
    {"table set_limit row_count",                Disposition::GATED,   !kLocal, kSafe},

    // tablet (RPC, cluster-wide)
    {"tablet leader_step_down",                  Disposition::GATED,   !kLocal, kSafe},
    {"tablet unsafe_replace_tablet",             Disposition::GATED,   !kLocal, kUnsafe},
    {"tablet info",                              Disposition::SURFACE, !kLocal, kSafe},
    {"tablet change_config add_replica",         Disposition::GATED,   !kLocal, kSafe},
    {"tablet change_config change_replica_type", Disposition::GATED,   !kLocal, kSafe},
    {"tablet change_config move_replica",        Disposition::REJECT,  !kLocal, kSafe},
    {"tablet change_config remove_replica",      Disposition::GATED,   !kLocal, kSafe},

    // txn (RPC, cluster-wide)
    {"txn list",                                 Disposition::SURFACE, !kLocal, kSafe},
    {"txn show",                                 Disposition::SURFACE, !kLocal, kSafe},

    // tserver (RPC, cluster-wide; 'run' is node-local)
    {"tserver dump_memtrackers",                 Disposition::SURFACE, !kLocal, kSafe},
    {"tserver get_flags",                        Disposition::SURFACE, !kLocal, kSafe},
    {"tserver run",                              Disposition::REJECT,  !kLocal, kSafe},
    {"tserver set_flag",                         Disposition::GATED,   !kLocal, kSafe},
    {"tserver set_flag_for_all",                 Disposition::GATED,   !kLocal, kSafe},
    {"tserver status",                           Disposition::SURFACE, !kLocal, kSafe},
    {"tserver timestamp",                        Disposition::SURFACE, !kLocal, kSafe},
    {"tserver list",                             Disposition::SURFACE, !kLocal, kSafe},
    {"tserver unregister",                       Disposition::GATED,   !kLocal, kSafe},
    {"tserver quiesce status",                   Disposition::SURFACE, !kLocal, kSafe},
    {"tserver quiesce start",                    Disposition::GATED,   !kLocal, kSafe},
    {"tserver quiesce stop",                     Disposition::GATED,   !kLocal, kSafe},
    {"tserver state enter_maintenance",          Disposition::GATED,   !kLocal, kSafe},
    {"tserver state exit_maintenance",           Disposition::GATED,   !kLocal, kSafe},

    // wal (NODE-LOCAL)
    {"wal dump",                                 Disposition::SURFACE, kLocal,  kSafe},
  };
  return *kEntries;
}

// Returns the lazily-built lookup map from command path to classification.
// CHECK-fails on a duplicate key: a duplicated entry in the static table is a
// programming error that must be caught the first time the table is consulted.
const unordered_map<string, DispositionInfo>& DispositionMap() {
  static const unordered_map<string, DispositionInfo>* const kMap = []() {
    auto* m = new unordered_map<string, DispositionInfo>();
    for (const auto& e : RawEntries()) {
      DispositionInfo info;
      info.disposition = e.disposition;
      info.node_local = e.node_local;
      info.unsafe = e.unsafe;
      info.classified = true;
      CHECK(InsertIfNotPresent(m, e.command_path, info))
          << "duplicate disposition table entry for command path '"
          << e.command_path << "'";
    }
    return m;
  }();
  return *kMap;
}

} // anonymous namespace

const char* DispositionToString(Disposition d) {
  switch (d) {
    case Disposition::SURFACE: return "SURFACE";
    case Disposition::GATED:   return "GATED";
    case Disposition::REJECT:  return "REJECT";
    case Disposition::EXCLUDE: return "EXCLUDE";
  }
  return "UNKNOWN";
}

const vector<RawDispositionEntry>& DispositionTableEntries() {
  return RawEntries();
}

string DispositionCommandPath(const vector<Mode*>& chain,
                              const Action* action) {
  string path;
  // Skip the root mode (chain.front()); join the remaining mode names.
  for (size_t i = 1; i < chain.size(); i++) {
    if (!path.empty()) {
      path += " ";
    }
    path += chain[i]->name();
  }
  if (action != nullptr) {
    if (!path.empty()) {
      path += " ";
    }
    path += action->name();
  }
  return path;
}

DispositionInfo DispositionFor(const vector<Mode*>& chain,
                               const Action* action) {
  // Actions under the test-only "test" mode are not real MCP tools. Treat the
  // entire mode as EXCLUDE without a table lookup, so the coverage invariant
  // does not require table entries for test tooling that only exists in
  // KUDU_CLI_TEST_TOOL_ENABLED builds.
  if (chain.size() >= 2 && chain[1]->name() == kTestModeName) {
    DispositionInfo info;
    info.disposition = Disposition::EXCLUDE;
    info.classified = true;
    return info;
  }

  const string path = DispositionCommandPath(chain, action);
  const DispositionInfo* info = FindOrNull(DispositionMap(), path);
  if (info == nullptr) {
    // Not classified. classified stays false; caller must not surface it.
    return DispositionInfo();
  }
  return *info;
}

Status CheckDispositionEntriesUnique(
    const vector<RawDispositionEntry>& entries) {
  unordered_map<string, bool> seen;
  for (const auto& e : entries) {
    if (!InsertIfNotPresent(&seen, e.command_path, true)) {
      return Status::AlreadyPresent(Substitute(
          "duplicate disposition table entry for command path '$0'",
          e.command_path));
    }
  }
  return Status::OK();
}

namespace {

// Recursively walks 'mode' (whose full chain from the root is 'chain', with
// 'mode' as its last element), verifying every action resolves to a classified
// disposition. Returns an error naming the first offending path.
Status ValidateModeCoverage(const vector<Mode*>& chain, const Mode* mode) {
  for (const auto& action : mode->actions()) {
    const DispositionInfo info = DispositionFor(chain, action.get());
    if (!info.classified) {
      return Status::NotFound(Substitute(
          "CLI action is not classified in the MCP disposition table: '$0'",
          DispositionCommandPath(chain, action.get())));
    }
  }
  for (const auto& submode : mode->modes()) {
    vector<Mode*> child_chain(chain);
    child_chain.push_back(submode.get());
    RETURN_NOT_OK(ValidateModeCoverage(child_chain, submode.get()));
  }
  return Status::OK();
}

} // anonymous namespace

Status ValidateDispositionCoverage(const Mode* root) {
  CHECK(root != nullptr);
  // First surface a duplicate table entry as a clean Status (rather than the
  // CHECK inside DispositionMap()) so this form is fully usable in tests.
  RETURN_NOT_OK(CheckDispositionEntriesUnique(RawEntries()));

  // The root itself is not an addressable command; walk each top-level mode.
  vector<Mode*> chain = { const_cast<Mode*>(root) };
  for (const auto& mode : root->modes()) {
    vector<Mode*> child_chain(chain);
    child_chain.push_back(mode.get());
    RETURN_NOT_OK(ValidateModeCoverage(child_chain, mode.get()));
  }
  return Status::OK();
}

void ValidateDispositionCoverageOrDie(const Mode* root) {
  const Status s = ValidateDispositionCoverage(root);
  CHECK(s.ok()) << "MCP disposition table is incomplete: " << s.ToString();
}

} // namespace tools
} // namespace kudu
