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

#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {
namespace tools {

class Action;
class Mode;

// The safety classification of a single CLI action for the MCP server. This is
// the machine-readable form of the "Action policy and full disposition" table
// in the PRD (kudu_mcp_prd.md, section 6). Every action reachable from the CLI
// action tree resolves to exactly one of these before it can be surfaced as an
// MCP tool.
//
//  - SURFACE: read-only. Always exposed. Node-local reads are surfaced but
//             tagged (see DispositionInfo::node_local) because they only mean
//             anything when the server runs on that node.
//  - GATED:   cluster mutation. Hidden unless --allow-writes is set; surfaced
//             with a host confirm prompt and dry-run.
//  - REJECT:  blocking / never-promptly-returning. Would freeze the
//             single-threaded serve loop (long-running admin ops and daemon
//             launchers). Never surfaced in v1.
//  - EXCLUDE: interactive (drives an editor / stdin) or node-local mutating.
//             Never surfaced.
enum class Disposition {
  SURFACE,
  GATED,
  REJECT,
  EXCLUDE,
};

// Returns a stable ASCII name for 'd' (e.g. "SURFACE"). Never returns null.
const char* DispositionToString(Disposition d);

// Classification metadata for a single action.
struct DispositionInfo {
  // The action's disposition. Only meaningful when 'classified' is true.
  Disposition disposition = Disposition::EXCLUDE;

  // True if the action operates on the on-disk data of the single node the
  // server runs on (the fs / wal / pbc / local_replica families). Such actions
  // only mean anything when the server is co-located with that node's data;
  // node-local reads are still SURFACE-d but callers should tag them.
  bool node_local = false;

  // True for "unsafe_*" and other unsafe operations (e.g. raw Raft config
  // edits). Advisory metadata: the PRD recommends a host may additionally hide
  // these even when their disposition is GATED.
  bool unsafe = false;

  // False if the command path was not found in the disposition table (i.e. the
  // action is not classified). When false, all other fields are unspecified and
  // must not be trusted. Callers MUST check this before acting on the result.
  bool classified = false;
};

// A single raw row of the disposition table, keyed by full command path.
struct RawDispositionEntry {
  // The full command path relative to the root mode, with mode names and the
  // action name joined by single spaces (e.g. "tserver quiesce status").
  const char* command_path;
  Disposition disposition;
  bool node_local;
  bool unsafe;
};

// Returns the full, curated disposition table transcribed from the PRD. Every
// entry is keyed by its full command path. Exposed so tests can inspect the
// table directly.
const std::vector<RawDispositionEntry>& DispositionTableEntries();

// Builds the full command path key for an action from its mode chain. 'chain'
// is the sequence of modes from the root to the action's parent (chain.front()
// is the root). The key drops the root mode's name and joins the remaining mode
// names followed by the action name with single spaces, e.g. a chain of
// {root, "tserver", "quiesce"} with action "status" yields
// "tserver quiesce status".
std::string DispositionCommandPath(const std::vector<Mode*>& chain,
                                   const Action* action);

// Looks up the classification for 'action' given its mode 'chain'. Returns a
// DispositionInfo whose 'classified' field is false if the action's command
// path is absent from the table (the caller must not surface such an action).
//
// Actions under the "test" mode (KUDU_CLI_TEST_TOOL_ENABLED test-only tooling)
// are not real MCP tools; they always resolve to a classified EXCLUDE without a
// table lookup, so the coverage invariant does not require table entries for
// them.
DispositionInfo DispositionFor(const std::vector<Mode*>& chain,
                               const Action* action);

// Returns an error (naming the offending path) if 'entries' contains any
// duplicated command_path. Exposed so both the startup guard and unit tests can
// exercise duplicate detection without relying on process death.
Status CheckDispositionEntriesUnique(
    const std::vector<RawDispositionEntry>& entries);

// Walks the entire action tree rooted at 'root' and verifies that every action
// resolves to exactly one disposition-table entry (actions under the "test"
// mode are treated as EXCLUDE and skipped, see DispositionFor). Returns an
// error naming the first offending full command path if an action is not
// classified, or if the table contains a duplicate entry.
//
// This is the Status-returning form, suitable for unit tests. The MCP serve
// startup path should use ValidateDispositionCoverageOrDie() instead so a newly
// added but unclassified CLI action fails loudly and immediately.
Status ValidateDispositionCoverage(const Mode* root);

// CHECK-based form of ValidateDispositionCoverage() for use at server startup:
// crashes with the offending path if the invariant does not hold. A missing
// classification is a programming error (a CLI action was added without being
// classified), so failing fast at startup is the intended behavior.
void ValidateDispositionCoverageOrDie(const Mode* root);

} // namespace tools
} // namespace kudu
