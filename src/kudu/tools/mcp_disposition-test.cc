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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// A no-op action runner for synthetic trees built in tests.
Status NoOpRunner(const RunnerContext& /*context*/) {
  return Status::OK();
}

unique_ptr<Action> MakeAction(const string& name) {
  return ActionBuilder(name, &NoOpRunner)
      .Description("synthetic test action")
      .Build();
}

// Reconstructs the identical production action tree by delegating to the shared
// BuildRootMode() (tool_action.h) -- the single source of truth also used by
// tool_main.cc:RootMode() and the MCP server's BuildMcpRootMode(). This is what
// exercises the coverage invariant against the real action set.
unique_ptr<Mode> BuildFullRootMode() {
  return BuildRootMode("kudu");
}

// Resolves a command path (e.g. {"tserver", "quiesce", "status"}) against
// 'root', returning the mode chain (root..parent) and the leaf action. CHECKs
// if the path cannot be resolved.
void ResolvePath(const Mode* root,
                 const vector<string>& path,
                 vector<Mode*>* chain,
                 const Action** action) {
  ASSERT_FALSE(path.empty());
  chain->clear();
  chain->push_back(const_cast<Mode*>(root));
  const Mode* cur = root;
  // All but the last component are modes.
  for (size_t i = 0; i + 1 < path.size(); i++) {
    const Mode* next = nullptr;
    for (const auto& m : cur->modes()) {
      if (m->name() == path[i]) {
        next = m.get();
        break;
      }
    }
    ASSERT_NE(nullptr, next) << "no submode '" << path[i] << "'";
    chain->push_back(const_cast<Mode*>(next));
    cur = next;
  }
  // The last component is the action.
  const Action* found = nullptr;
  for (const auto& a : cur->actions()) {
    if (a->name() == path.back()) {
      found = a.get();
      break;
    }
  }
  ASSERT_NE(nullptr, found) << "no action '" << path.back() << "'";
  *action = found;
}

// Convenience: resolve 'path' against 'root' and return its disposition.
DispositionInfo Lookup(const Mode* root, const vector<string>& path) {
  vector<Mode*> chain;
  const Action* action = nullptr;
  ResolvePath(root, path, &chain, &action);
  return DispositionFor(chain, action);
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Happy path: a representative action of each disposition class resolves to the
// expected classification and flags against the real action tree.
// ---------------------------------------------------------------------------
TEST(McpDispositionTest, RepresentativeActionsOfEachClass) {
  unique_ptr<Mode> root = BuildFullRootMode();

  // SURFACE: a core cluster read.
  {
    const DispositionInfo info = Lookup(root.get(), {"cluster", "ksck"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::SURFACE, info.disposition);
    EXPECT_FALSE(info.node_local);
    EXPECT_FALSE(info.unsafe);
  }
  // GATED: a destructive cluster mutation.
  {
    const DispositionInfo info = Lookup(root.get(), {"table", "delete"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::GATED, info.disposition);
    EXPECT_FALSE(info.node_local);
  }
  // REJECT: a blocking long-runner.
  {
    const DispositionInfo info = Lookup(root.get(), {"cluster", "rebalance"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::REJECT, info.disposition);
  }
  // EXCLUDE: an interactive node-local action.
  {
    const DispositionInfo info = Lookup(root.get(), {"pbc", "edit"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::EXCLUDE, info.disposition);
    EXPECT_TRUE(info.node_local);
  }
  // SURFACE + node_local: a node-local read.
  {
    const DispositionInfo info = Lookup(root.get(), {"fs", "list"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::SURFACE, info.disposition);
    EXPECT_TRUE(info.node_local);
  }
  // unsafe flag: an unsafe_* op is tagged.
  {
    const DispositionInfo info =
        Lookup(root.get(), {"tablet", "unsafe_replace_tablet"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::GATED, info.disposition);
    EXPECT_TRUE(info.unsafe);
  }
}

// "cluster gather" is the RPC fan-out surface command added in M5/M6.
// It is a cluster-wide read: SURFACE, not node-local, not unsafe.
TEST(McpDispositionTest, ClusterGatherIsSurface) {
  unique_ptr<Mode> root = BuildFullRootMode();
  const DispositionInfo info = Lookup(root.get(), {"cluster", "gather"});
  ASSERT_TRUE(info.classified);
  EXPECT_EQ(Disposition::SURFACE, info.disposition);
  EXPECT_FALSE(info.node_local);
  EXPECT_FALSE(info.unsafe);
}

// Multi-level (submode) command paths key on the full chain, not just parent
// mode + action. These are the paths that differ from the flat PRD tables.
TEST(McpDispositionTest, MultiLevelSubmodePaths) {
  unique_ptr<Mode> root = BuildFullRootMode();

  // tserver quiesce status: SURFACE read under the 'quiesce' submode.
  {
    const DispositionInfo info =
        Lookup(root.get(), {"tserver", "quiesce", "status"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::SURFACE, info.disposition);
  }
  // tserver quiesce start: GATED mutation under the 'quiesce' submode.
  {
    const DispositionInfo info =
        Lookup(root.get(), {"tserver", "quiesce", "start"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::GATED, info.disposition);
  }
  // table set_limit disk_size: GATED under the 'set_limit' submode.
  {
    const DispositionInfo info =
        Lookup(root.get(), {"table", "set_limit", "disk_size"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::GATED, info.disposition);
  }
  // fs dump uuid: SURFACE node-local read under the 'dump' submode.
  {
    const DispositionInfo info = Lookup(root.get(), {"fs", "dump", "uuid"});
    ASSERT_TRUE(info.classified);
    EXPECT_EQ(Disposition::SURFACE, info.disposition);
    EXPECT_TRUE(info.node_local);
  }
}

// The full command path key is built from the mode chain (minus root) plus the
// action name.
TEST(McpDispositionTest, CommandPathKeyBuilding) {
  unique_ptr<Mode> root = BuildFullRootMode();
  vector<Mode*> chain;
  const Action* action = nullptr;
  ResolvePath(root.get(), {"tserver", "quiesce", "status"}, &chain, &action);
  EXPECT_EQ("tserver quiesce status",
            DispositionCommandPath(chain, action));
}

// The whole action tree resolves: every action has exactly one table entry.
// This is the invariant that catches a newly added but unclassified CLI action.
TEST(McpDispositionTest, ValidateCoveragePassesAgainstCurrentTree) {
  unique_ptr<Mode> root = BuildFullRootMode();
  ASSERT_OK(ValidateDispositionCoverage(root.get()));
}

// The test-only "test" mode is classified EXCLUDE without needing a table
// entry, so the coverage invariant does not spuriously fail on it.
TEST(McpDispositionTest, TestModeIsExcluded) {
#if defined(KUDU_CLI_TEST_TOOL_ENABLED)
  unique_ptr<Mode> root = BuildFullRootMode();
  const DispositionInfo info = Lookup(root.get(), {"test", "mini_cluster"});
  ASSERT_TRUE(info.classified);
  EXPECT_EQ(Disposition::EXCLUDE, info.disposition);
#else
  GTEST_SKIP() << "test mode is not compiled in this build";
#endif
}

// ---------------------------------------------------------------------------
// Bad paths.
// ---------------------------------------------------------------------------

// An unknown command path reports "not classified" rather than silently
// defaulting to some disposition.
TEST(McpDispositionTest, UnknownPathIsNotClassified) {
  // Build a synthetic tree with an action that is absent from the table.
  unique_ptr<Mode> child = ModeBuilder("nonsense")
      .Description("synthetic mode")
      .AddAction(MakeAction("bogus_action"))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(child))
      .Build();

  const DispositionInfo info = Lookup(root.get(), {"nonsense", "bogus_action"});
  EXPECT_FALSE(info.classified);
}

// The validator fails and names the offending full command path when the tree
// contains an action missing from the table.
TEST(McpDispositionTest, ValidatorFailsOnMissingClassification) {
  // "cluster" is a real mode, but "cluster bogus_action" is not in the table,
  // so the full path must be reported.
  unique_ptr<Mode> cluster = ModeBuilder("cluster")
      .Description("synthetic cluster mode")
      .AddAction(MakeAction("bogus_action"))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(cluster))
      .Build();

  const Status s = ValidateDispositionCoverage(root.get());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "cluster bogus_action");
}

// The validator reports the full multi-level path for an unclassified action
// nested under a submode.
TEST(McpDispositionTest, ValidatorReportsFullSubmodePath) {
  unique_ptr<Mode> submode = ModeBuilder("subthing")
      .Description("synthetic submode")
      .AddAction(MakeAction("bogus_action"))
      .Build();
  unique_ptr<Mode> parent = ModeBuilder("thing")
      .Description("synthetic parent mode")
      .AddMode(std::move(submode))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(parent))
      .Build();

  const Status s = ValidateDispositionCoverage(root.get());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "thing subthing bogus_action");
}

} // namespace tools
} // namespace kudu
