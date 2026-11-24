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

#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tools {

// Every CLI action must be classified for the MCP server, and that is enforced
// at compile time: the disposition is a required constructor argument of every
// action builder, so a builder cannot be created from just a name and a runner.
static_assert(!std::is_constructible_v<ActionBuilder, string, ActionRunner>);
static_assert(!std::is_constructible_v<RpcActionBuilder, string, ActionRunner>);
static_assert(!std::is_constructible_v<ClusterActionBuilder, string, ActionRunner>);
static_assert(!std::is_constructible_v<MasterActionBuilder, string, ActionRunner>);
static_assert(!std::is_constructible_v<TServerActionBuilder, string, ActionRunner>);
static_assert(std::is_constructible_v<ActionBuilder, string, ActionRunner, Disposition>);
// The scope is derived from the builder type, not passed by hand: the
// constructor that takes one is not public.
static_assert(
    !std::is_constructible_v<ActionBuilder, string, ActionRunner, Disposition, McpScope>);

// A Disposition can only come from its factories: there is no default
// (unclassified) value.
static_assert(!std::is_default_constructible_v<Disposition>);

// The factories set the expected kind and user-data tag.
static_assert(Disposition::SurfaceMetadata().kind() == Disposition::Kind::SURFACE);
static_assert(!Disposition::SurfaceMetadata().user_data());
static_assert(Disposition::SurfaceUserData().kind() == Disposition::Kind::SURFACE);
static_assert(Disposition::SurfaceUserData().user_data());
static_assert(Disposition::Gated().kind() == Disposition::Kind::GATED);
static_assert(!Disposition::Gated().user_data());
static_assert(Disposition::Exclude().kind() == Disposition::Kind::EXCLUDE);
static_assert(!Disposition::Exclude().user_data());

namespace {

// A no-op action runner for synthetic trees built in tests.
Status NoOpRunner(const RunnerContext& /*context*/) {
  return Status::OK();
}

unique_ptr<Action> MakeAction(const string& name, Disposition d) {
  return ActionBuilder(name, &NoOpRunner, d)
      .Description("synthetic test action")
      .Build();
}

// Resolves a command path (e.g. {"tserver", "quiesce", "status"}) against
// 'root', returning the mode chain (root..parent) and the leaf action. Records
// a fatal failure if the path cannot be resolved.
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

// Resolves 'path' against 'root' and checks its classification. Call it under
// ASSERT_NO_FATAL_FAILURE().
void CheckAction(const Mode* root,
                 const vector<string>& path,
                 Disposition::Kind kind,
                 McpScope scope,
                 bool user_data = false) {
  SCOPED_TRACE(JoinStrings(path, " "));
  vector<Mode*> chain;
  const Action* action = nullptr;
  ASSERT_NO_FATAL_FAILURE(ResolvePath(root, path, &chain, &action));
  const Disposition d = action->mcp_disposition();
  ASSERT_EQ(DispositionKindToString(kind), DispositionKindToString(d.kind()));
  ASSERT_EQ(scope, action->mcp_scope());
  ASSERT_EQ(user_data, d.user_data());
}

constexpr auto kSurface = Disposition::Kind::SURFACE;
constexpr auto kGated = Disposition::Kind::GATED;
constexpr auto kExclude = Disposition::Kind::EXCLUDE;
constexpr auto kCluster = McpScope::CLUSTER;
constexpr auto kNodeLocal = McpScope::NODE_LOCAL;

} // anonymous namespace

// A representative action of each disposition kind resolves to the expected
// classification against the real action tree.
TEST(McpDispositionTest, RepresentativeActionsOfEachKind) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  // SURFACE: a core cluster read.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"cluster", "ksck"}, kSurface, kCluster));
  // SURFACE: a metadata read is not tagged user_data.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"table", "describe"}, kSurface, kCluster));
  // SURFACE + node-local: a node-local read.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"fs", "list"}, kSurface, kNodeLocal));
  // SURFACE + user_data: a row-level read.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"table", "scan"}, kSurface, kCluster,
                                      /*user_data=*/true));
  // GATED: a destructive cluster mutation.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"table", "delete"}, kGated, kCluster));
  // EXCLUDE: a blocking long-runner.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"cluster", "rebalance"}, kExclude, kCluster));
  // EXCLUDE: an interactive node-local action.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"pbc", "edit"}, kExclude, kNodeLocal));
}

// Actions that read row-level user data are all tagged, so a host can gate
// them separately from metadata reads.
TEST(McpDispositionTest, UserDataReadsAreTagged) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"table", "scan"}, kSurface, kCluster,
                                      /*user_data=*/true));
  const vector<vector<string>> kNodeLocalUserDataPaths = {
    {"wal", "dump"},
    {"local_replica", "dump", "rowset"},
    {"local_replica", "dump", "wals"},
    {"fs", "dump", "cfile"},
    {"fs", "dump", "block"},
  };
  for (const auto& path : kNodeLocalUserDataPaths) {
    ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), path, kSurface, kNodeLocal,
                                        /*user_data=*/true));
  }
}

// Actions that mutate local state, run for a long time, or are unsafe recovery
// operations are never surfaced, even though parts of them look like reads or
// ordinary cluster mutations.
TEST(McpDispositionTest, MutatingLongRunningOrUnsafeActionsAreExcluded) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  const vector<vector<string>> kExcludedPaths = {
    // 'fs check --repair' writes to disk.
    {"fs", "check"},
    // 'master add' is a multi-step job on the new master's node.
    {"master", "add"},
    // 'perf loadgen' runs long by design.
    {"perf", "loadgen"},
    // Unsafe last-resort recovery operations.
    {"tablet", "unsafe_replace_tablet"},
    {"remote_replica", "unsafe_change_config"},
    {"master", "unsafe_rebuild"},
    // The MCP server must never re-expose itself.
    {"mcp", "serve"},
  };
  for (const auto& path : kExcludedPaths) {
    SCOPED_TRACE(JoinStrings(path, " "));
    vector<Mode*> chain;
    const Action* action = nullptr;
    ASSERT_NO_FATAL_FAILURE(ResolvePath(root.get(), path, &chain, &action));
    ASSERT_EQ(DispositionKindToString(kExclude),
              DispositionKindToString(action->mcp_disposition().kind()));
  }
}

// Actions that read files on the local node are node-local.
TEST(McpDispositionTest, LocalFileReadsAreNodeLocal) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  const vector<vector<string>> kNodeLocalPaths = {
    {"diagnose", "parse_stacks"},
    {"diagnose", "parse_metrics"},
    {"perf", "tablet_scan"},
  };
  for (const auto& path : kNodeLocalPaths) {
    ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), path, kSurface, kNodeLocal));
  }
}

// The scope is derived from the builder type rather than chosen by hand: an
// RPC action builder yields CLUSTER, the plain builder yields NODE_LOCAL.
TEST(McpDispositionTest, ScopeIsDerivedFromBuilderType) {
  unique_ptr<Action> local =
      ActionBuilder("local", &NoOpRunner, Disposition::SurfaceMetadata())
      .Description("d")
      .Build();
  ASSERT_EQ(McpScope::NODE_LOCAL, local->mcp_scope());

  unique_ptr<Action> rpc =
      RpcActionBuilder("rpc", &NoOpRunner, Disposition::SurfaceMetadata())
      .Description("d")
      .Build();
  ASSERT_EQ(McpScope::CLUSTER, rpc->mcp_scope());

  unique_ptr<Action> cluster =
      ClusterActionBuilder("cluster", &NoOpRunner, Disposition::Gated())
      .Description("d")
      .Build();
  ASSERT_EQ(McpScope::CLUSTER, cluster->mcp_scope());

  unique_ptr<Action> master =
      MasterActionBuilder("master", &NoOpRunner, Disposition::SurfaceMetadata())
      .Description("d")
      .Build();
  ASSERT_EQ(McpScope::CLUSTER, master->mcp_scope());

  unique_ptr<Action> tserver =
      TServerActionBuilder("tserver", &NoOpRunner, Disposition::Exclude())
      .Description("d")
      .Build();
  ASSERT_EQ(McpScope::CLUSTER, tserver->mcp_scope());
}

// Cluster RPC actions are built by an RPC action builder, so their derived scope
// is CLUSTER. These three used to declare master_addresses by hand on the plain
// builder, which would have made them NODE_LOCAL.
TEST(McpDispositionTest, ClusterRpcActionsAreClusterScoped) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"master", "remove"}, kGated, kCluster));
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"table", "recall"}, kGated, kCluster));
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"table", "add_column"}, kGated, kCluster));
}

// Multi-level (submode) command paths key on the full chain, not just parent
// mode + action.
TEST(McpDispositionTest, MultiLevelSubmodePaths) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  // tserver quiesce status: SURFACE read under the 'quiesce' submode.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"tserver", "quiesce", "status"},
                                      kSurface, kCluster));
  // tserver quiesce start: GATED mutation under the 'quiesce' submode.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"tserver", "quiesce", "start"},
                                      kGated, kCluster));
  // table set_limit disk_size: GATED under the 'set_limit' submode.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"table", "set_limit", "disk_size"},
                                      kGated, kCluster));
  // fs dump uuid: SURFACE node-local read under the 'dump' submode.
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"fs", "dump", "uuid"},
                                      kSurface, kNodeLocal));
}

// The full command path key is built from the mode chain (minus root) plus the
// action name.
TEST(McpDispositionTest, CommandPathKeyBuilding) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  vector<Mode*> chain;
  const Action* action = nullptr;
  // Propagate ResolvePath()'s fatal assertions so a failed resolution stops the
  // test before DispositionCommandPath() dereferences a null 'action'.
  ASSERT_NO_FATAL_FAILURE(
      ResolvePath(root.get(), {"tserver", "quiesce", "status"}, &chain, &action));
  ASSERT_EQ("tserver quiesce status",
            DispositionCommandPath(chain, action));
}

// The test-only "test" mode actions are explicitly classified EXCLUDE.
TEST(McpDispositionTest, TestModeIsExcluded) {
#if defined(KUDU_CLI_TEST_TOOL_ENABLED)
  unique_ptr<Mode> root = BuildRootMode("kudu");
  ASSERT_NO_FATAL_FAILURE(CheckAction(root.get(), {"test", "mini_cluster"},
                                      kExclude, kNodeLocal));
#else
  GTEST_SKIP() << "test mode is not compiled in this build";
#endif
}

// VisitActions() reaches every action, including ones nested under submodes,
// with the full mode chain, and visits a mode's actions before its submodes.
TEST(McpDispositionTest, VisitActionsWalksWholeTree) {
  unique_ptr<Mode> submode = ModeBuilder("sub")
      .Description("synthetic submode")
      .AddAction(MakeAction("b", Disposition::Gated()))
      .Build();
  unique_ptr<Mode> parent = ModeBuilder("top")
      .Description("synthetic parent mode")
      .AddAction(MakeAction("a", Disposition::SurfaceMetadata()))
      .AddMode(std::move(submode))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(parent))
      .Build();

  vector<string> visited;
  ASSERT_OK(VisitActions(root.get(), [&](const vector<Mode*>& chain,
                                         const Action* action) {
    visited.emplace_back(DispositionCommandPath(chain, action));
    return Status::OK();
  }));
  ASSERT_EQ(vector<string>({ "top a", "top sub b" }), visited);

  // A non-OK status from the visitor stops the walk and is returned.
  visited.clear();
  const Status s = VisitActions(root.get(), [&](const vector<Mode*>& chain,
                                                const Action* action) {
    visited.emplace_back(DispositionCommandPath(chain, action));
    return Status::Aborted("stop");
  });
  ASSERT_TRUE(s.IsAborted()) << s.ToString();
  ASSERT_EQ(vector<string>({ "top a" }), visited);
}

// The audit dump lists every action with its kind and tags.
TEST(McpDispositionTest, DumpDispositionsListsKindAndTags) {
  unique_ptr<Mode> child = ModeBuilder("top")
      .Description("synthetic mode")
      .AddAction(MakeAction("meta", Disposition::SurfaceMetadata()))
      .AddAction(MakeAction("rows", Disposition::SurfaceUserData()))
      .AddAction(ClusterActionBuilder("write", &NoOpRunner, Disposition::Gated())
                 .Description("d")
                 .Build())
      .AddAction(MakeAction("never", Disposition::Exclude()))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(child))
      .Build();
  ASSERT_EQ("top meta\tSURFACE node_local\n"
            "top rows\tSURFACE node_local user_data\n"
            "top write\tGATED\n"
            "top never\tEXCLUDE node_local\n",
            DumpDispositions(root.get()));
}

} // namespace tools
} // namespace kudu
