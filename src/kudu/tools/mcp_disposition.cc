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

#include <stddef.h>

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action.h"

using std::optional;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// The name of the test-only mode (present only in KUDU_CLI_TEST_TOOL_ENABLED
// builds). Its actions are not real MCP tools; see DispositionFor().
const char* const kTestModeName = "test";

// True if 'chain' descends through the test-only "test" mode. Actions there are
// not real MCP tools and resolve to a classified EXCLUDE without consulting the
// action's own disposition.
bool IsUnderTestMode(const vector<Mode*>& chain) {
  return chain.size() >= 2 && chain[1]->name() == kTestModeName;
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
  // entire mode as EXCLUDE without consulting the action, so the coverage
  // invariant does not require test tooling (which only exists in
  // KUDU_CLI_TEST_TOOL_ENABLED builds) to be classified.
  if (IsUnderTestMode(chain)) {
    DispositionInfo info;
    info.disposition = Disposition::EXCLUDE;
    info.classified = true;
    return info;
  }

  const optional<Disposition>& d = action->mcp_disposition();
  if (!d.has_value()) {
    // Not classified. classified stays false; caller must not surface it.
    return DispositionInfo();
  }
  DispositionInfo info;
  info.disposition = *d;
  info.node_local = action->mcp_node_local();
  info.unsafe = action->mcp_unsafe();
  info.classified = true;
  return info;
}

namespace {

// Recursively appends one line per action under 'mode' (whose full chain from
// the root is 'chain') to 'out'.
void DumpModeDispositions(const vector<Mode*>& chain, const Mode* mode,
                          string* out) {
  for (const auto& action : mode->actions()) {
    const DispositionInfo info = DispositionFor(chain, action.get());
    const string path = DispositionCommandPath(chain, action.get());
    string tags;
    if (info.node_local) {
      tags += " node_local";
    }
    if (info.unsafe) {
      tags += " unsafe";
    }
    out->append(Substitute("$0\t$1$2\n", path,
                           info.classified ? DispositionToString(info.disposition)
                                           : "UNCLASSIFIED",
                           tags));
  }
  for (const auto& submode : mode->modes()) {
    vector<Mode*> child_chain(chain);
    child_chain.push_back(submode.get());
    DumpModeDispositions(child_chain, submode.get(), out);
  }
}

} // anonymous namespace

string DumpDispositions(const Mode* root) {
  CHECK(root != nullptr);
  string out;
  vector<Mode*> chain = { const_cast<Mode*>(root) };
  for (const auto& mode : root->modes()) {
    vector<Mode*> child_chain(chain);
    child_chain.push_back(mode.get());
    DumpModeDispositions(child_chain, mode.get(), &out);
  }
  return out;
}

namespace {

// Recursively walks 'mode' (whose full chain from the root is 'chain', with
// 'mode' as its last element), verifying every action carries a disposition.
// Returns an error naming the first offending path.
Status ValidateModeCoverage(const vector<Mode*>& chain, const Mode* mode) {
  for (const auto& action : mode->actions()) {
    const DispositionInfo info = DispositionFor(chain, action.get());
    if (!info.classified) {
      return Status::NotFound(Substitute(
          "CLI action is not classified for the MCP server (missing "
          "ActionBuilder::McpDisposition): '$0'",
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
  CHECK(s.ok()) << "MCP disposition coverage is incomplete: " << s.ToString();
}

} // namespace tools
} // namespace kudu
