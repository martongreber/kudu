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
#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {
namespace tools {

class Action;
class Mode;

// Whether an action operates on a whole Kudu cluster (over RPC) or on the data
// of the single node it runs on (the fs / wal / pbc / local_replica families,
// local files). It is not chosen by hand: an action built by RpcActionBuilder or
// one of its subclasses is CLUSTER, any other action is NODE_LOCAL.
enum class McpScope : uint8_t {
  CLUSTER,
  NODE_LOCAL,
};

// The MCP safety classification of a single CLI action. Every action carries
// exactly one: it is a required ActionBuilder constructor argument, so a CLI
// action that is not classified does not compile.
//
// A Disposition is created only via the factories below, each of which asks
// just the questions that matter for its kind, so an incoherent classification
// (e.g. a mutating action tagged as a user-data read) cannot be expressed:
//
//  - SurfaceMetadata(): read-only, returns metadata. Always exposed.
//  - SurfaceUserData(): read-only, returns row-level user data (e.g. 'table
//                       scan', WAL / rowset dumps). The CLI does not redact
//                       such output, so it is exposed only when the server
//                       runs with --mcp_allow_data_reads.
//  - Gated():           mutates cluster state. Exposed only when the server
//                       runs with --mcp_allow_writes, with dry-run support.
//  - Exclude():         never exposed: blocking / long-running (would freeze
//                       the single-threaded serve loop), interactive (drives an
//                       editor / stdin), node-local mutating, or an unsafe
//                       last-resort recovery operation.
class Disposition {
 public:
  enum class Kind : uint8_t {
    SURFACE,
    GATED,
    EXCLUDE,
  };

  static constexpr Disposition SurfaceMetadata() {
    return Disposition(Kind::SURFACE, /*user_data=*/false);
  }
  static constexpr Disposition SurfaceUserData() {
    return Disposition(Kind::SURFACE, /*user_data=*/true);
  }
  static constexpr Disposition Gated() {
    return Disposition(Kind::GATED, /*user_data=*/false);
  }
  static constexpr Disposition Exclude() {
    return Disposition(Kind::EXCLUDE, /*user_data=*/false);
  }

  constexpr Kind kind() const { return kind_; }

  // True if the action returns row-level user data rather than metadata.
  constexpr bool user_data() const { return user_data_; }

 private:
  constexpr Disposition(Kind kind, bool user_data)
      : kind_(kind),
        user_data_(user_data) {
  }

  Kind kind_;
  bool user_data_;
};

// Returns a stable ASCII name for 'kind' (e.g. "SURFACE"). Never returns null.
const char* DispositionKindToString(Disposition::Kind kind);

// Builds the full command path key for an action from its mode chain. 'chain'
// is the sequence of modes from the root to the action's parent (chain.front()
// is the root). The key drops the root mode's name and joins the remaining mode
// names followed by the action name with single spaces, e.g. a chain of
// {root, "tserver", "quiesce"} with action "status" yields
// "tserver quiesce status".
std::string DispositionCommandPath(const std::vector<Mode*>& chain,
                                   const Action* action);

// Callback for VisitActions(). 'chain' is the sequence of modes from the root
// to the action's parent (chain.front() is the root), as expected by
// DispositionCommandPath().
typedef std::function<Status(const std::vector<Mode*>& chain,
                             const Action* action)> ActionVisitor;

// Walks the entire action tree rooted at 'root' depth-first (a mode's own
// actions before its submodes) and invokes 'visitor' on every action. Stops at
// and returns the first non-OK status returned by 'visitor'.
Status VisitActions(const Mode* root, const ActionVisitor& visitor);

// Returns a human-readable table of the classification of every action in the
// tree rooted at 'root', one action per line (full command path, disposition
// kind, and node_local / user_data tags). This is the generated audit view
// of the MCP safety surface -- what would otherwise be a hand-maintained table.
std::string DumpDispositions(const Mode* root);

} // namespace tools
} // namespace kudu
