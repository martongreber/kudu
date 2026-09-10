# kudu cluster gather -- Implementation Doc (RPC Fan-Out)

Status: Implementation plan, ready to build

Companion to [kudu_mcp_fanout.md](kudu_mcp_fanout.md) (the approved design).
This doc is the shared, authoritative build context: every implementation agent
reads it. It fixes the module boundaries, the interfaces each milestone must
expose, the exact file:line anchors grounded against the tree, and the test
plan. Where this doc and the design doc disagree on a mechanical detail, THIS
doc wins (it reflects grounded reads of the current source); where they disagree
on intent, the design doc wins.

ASCII only. No unicode characters anywhere in code, comments, or docs.

---

## 0. Ground rules for every agent

- **Branch discipline.** Do not commit to or modify `mcp-poc` (the active
  branch) as part of these operations. If you need git, create a new branch off
  the current HEAD and build the commit stack there. Never push to any remote.
- **No unicode.** Verify with `grep -nP '[^\x00-\x7F]'` on any file you touch.
- **Do not re-add auto-injected flags.** `RpcActionBuilder::Build()`
  (`tools/tool_action_common.cc:531-535`) already appends `negotiation_timeout_ms`
  and `timeout_ms` to every RPC action; `ClusterActionBuilder`
  (`tools/tool_action_common.h:96`) already prepends the `master_addresses`
  positional. Re-adding any of these to `cluster gather` double-registers the
  arg and crashes at startup. Add ONLY gather-specific flags.
- **Reuse a gflag via DECLARE, never a second DEFINE.** gflags are process
  global; a second `DEFINE_*` of an existing name is a link-time/runtime
  duplicate-registration failure. Before adding any `DEFINE_*`, grep the tree
  for that flag name; if it exists, `DECLARE_*` it instead.
- **Fixed interfaces are contracts.** M0 freezes the proto and the
  `remote_cluster.h` header. M1/M2/M3 build against those signatures without
  changing them. If a signature must change, it is a coordinated change: flag it
  to the orchestrator, do not silently diverge.
- **Match surrounding style.** These files use Google C++ style, `Status`
  return + `RETURN_NOT_OK`, `gscoped`/`unique_ptr`, `kudu::` namespaces. Read
  the neighbor before you write.

---

## 1. Module map (what gets created / touched)

New files:

- `src/kudu/tools/remote_cluster.h` -- the fan-out engine + resolver interface.
- `src/kudu/tools/remote_cluster.cc` -- its implementation.
- `src/kudu/tools/remote_cluster-test.cc` -- unit/integration test (M6).

Edited files:

- `src/kudu/tools/tool.proto` -- add `GatherResultsPB` and nested messages.
  This is the ONLY tools proto; `KsckResultsPB` already lives here, so this is
  zero new CMake wiring (see section 3).
- `src/kudu/tools/tool_action_cluster.cc` -- add the `cluster gather` action
  builder + `RunGather` runner; add gather-specific `DEFINE_*` flags.
- `src/kudu/tools/mcp_disposition.cc` -- add the one SURFACE table entry.
- `src/kudu/tools/CMakeLists.txt` -- add `remote_cluster.cc` to the
  `kudu_tools_util` library sources and the `-test` to the test list.

Explicitly NOT touched: `ksck.h`, `ksck.cc`, `ksck_remote.h`, `ksck_remote.cc`.
The engine is a sibling that reuses standalone helpers; it does not derive from
or edit ksck. (Grounded decision; see section 2.)

---

## 2. The reuse-vs-fork decision (settled: sibling)

Agent-grounded read of the ksck fetch path concluded: build a sibling, reuse the
standalone pieces, do not refactor ksck. Concretely:

Reused as-is (no edits to their files):

| Piece | Anchor | How gather uses it |
|-------|--------|--------------------|
| `BuildMessenger(name, &messenger)` | `tools/tool_action_common.cc:552-583` | Call once; standalone free function; carries all TLS/SASL/negotiation settings. |
| The four service proxies | pattern at `ksck_remote.cc:203-215` | Construct `GenericServiceProxy`, `TabletServerServiceProxy`, `TabletServerAdminServiceProxy`, `ConsensusServiceProxy` from `(messenger, Sockaddr, host)`. |
| `ListTabletServers(include_states=true)` | `ksck_remote.cc:548-581` | ~3 lines via a `KuduClient`; discovery + `TServerStatePB`. |
| `FLAGS_fetch_info_concurrency` | `DEFINE_int32` at `ksck.cc:63` (default 20) | `DECLARE_int32` in `remote_cluster.cc`; do not re-DEFINE. |
| `FLAGS_timeout_ms` | defined `tool_action_common.cc:123`, `DECLARE_int64` pattern at `ksck_remote.cc:75` | already auto-added to the action; `DECLARE` in engine if the fetch loop needs the value. |
| Fetch-loop idiom | `Ksck::FetchInfoFromTabletServers` `ksck.cc:466-540` | Reproduce shape: `atomic<size_t>` bad/unauth counters, `simple_spinlock` + `lock_guard` around the result vector, `pool_->Submit([&]{...})` per target, `pool_->Wait()`, assemble after. |
| ThreadPool build | `Ksck::Ksck()` `ksck.cc:259-266` (named "ksck-fetch") | `ThreadPoolBuilder("remote-gather-fetch").set_max_threads(FLAGS_fetch_info_concurrency).set_idle_timeout(...).Build(&pool_)`. |

Why not extract a shared base: `FetchInfoFromTabletServers` writes directly into
`Ksck`'s private `KsckResults` (`ksck.cc:524`), so the loop is inseparable from
ksck's output type; the genuinely reusable parts (`BuildMessenger`, proxy
construction) are already standalone. Extraction would edit load-bearing
`ksck.h`/`ksck_remote.h` for a ~35-line gain. Sibling it is.

Per-server probe RPC inventory (all proxies exist after Init; only one probe is
new code at the proxy layer):

| Probe | Service / proxy | Call site precedent | New? |
|-------|-----------------|---------------------|------|
| identity/version | `GetStatus` (Generic) | `ksck_remote.cc:226` | no |
| replica inventory | `ListTablets` (TabletServerService) | `ksck_remote.cc:244` | no |
| clock | `ServerClock` (Generic) | `ksck_remote.cc:285-293` | no |
| quiescing | `Quiesce(return_stats=true)` (TabletServerAdmin) | `ksck_remote.cc:295-313` | no |
| consensus | `GetConsensusState(dest_uuid=uuid)` (Consensus) | `ksck_remote.cc:315-338` | no |
| flags | `GetFlags` (Generic) | `ksck_remote.cc:340-354` | no |
| memory | `DumpMemTrackers` (Generic) | NONE -- ksck never calls it | YES (proxy exists; add call; RPC at `server_base.proto:189-190`) |

---

## 3. Proto placement (M0)

Add `GatherResultsPB` and its nested messages to `src/kudu/tools/tool.proto`
(Option A). Rationale: `tool.proto` already hosts `KsckResultsPB`; the CMake
stanza at `tools/CMakeLists.txt:22-35` runs `PROTOBUF_GENERATE_CPP` on it and
`tool_proto` is already linked into `kudu_tools_util` (`:59`) and the `kudu`
exe (`:155`). Adding messages to it is zero new build wiring. A separate
`.proto` would need a new generate stanza + link edges -- avoid.

Message shape (mirror the design doc section 3.7; keep fields minimal and
additive-friendly, reserve nothing yet since this is new):

```
message GatherResultsPB {
  message ClusterInfo { ... master_addresses, cluster_id, report_time,
                            applied_selector (string), fetch_info_concurrency,
                            timeout_ms ... }
  message ProbeStatus { enum { OK, TIMED_OUT, UNAUTHORIZED, UNREACHABLE,
                               PRESUMED_DEAD, SKIPPED } ... per-section ... }
  message ReplicaInfo { tablet_id, table_name, state, tablet_data_state,
                        last_status, partition, estimated_on_disk_size,
                        repeated data_dirs, role, term, leader_uuid ... }
  message ServerRecord { uuid, host, location, version, start_time,
                         millis_since_heartbeat, ts_state,
                         repeated ProbeStatus section_status,
                         repeated ReplicaInfo replicas,
                         flags..., memtrackers..., quiescing..., clock... }
  message ReplicaSetView { // entity-centric projection (tablet/row selector)
                           tablet_id, table_name,
                           repeated ReplicaCopy copies (leader first) ... }
  message Rollup { replica-count + on-disk-size distribution (min/max/mean/
                   stddev, top-N), leader imbalance, version spread,
                   flag-divergence groups, counts by ProbeStatus ... }
  ClusterInfo cluster = 1;
  repeated ServerRecord servers = 2;    // server-centric projection
  repeated ReplicaSetView entities = 3; // entity-centric projection
  Rollup rollup = 4;
}
```

Exact field numbers/types are the M0 agent's to finalize; once merged they are
frozen for the downstream milestones. Reuse existing PB enums where they exist
(e.g. `tablet::TabletStatePB`, `TabletDataState`) rather than redefining.

---

## 4. Output path (M4, grounded by Agent C)

- Serialize with `JsonWriter::ToJson(const google::protobuf::Message&, Mode)`
  (`util/jsonwriter.h:84-85`; `Mode` = `PRETTY` / `COMPACT` at `:50-55`),
  written to `std::cout` followed by `endl` -- the exact pattern ksck uses in
  `PrintJsonTo` (`ksck_results.cc:1072-1083`).
- The MCP serve loop captures `cout` via `ScopedCoutRedirect`
  (`tool_action_mcp.cc:650-660`, capture block `:1017-1036`) and returns it
  verbatim as the tool result text. No serve-loop change needed.
- **Format flag: dedicated `--gather_format`** with values `json_pretty` /
  `json_compact` (default `json_pretty`), mirroring ksck's `--ksck_format`.
  Do NOT reuse the generic `--format` (pretty/space/tsv/csv/json,
  `tool_action_common.cc:129`) -- gather emits only JSON and needs its own
  default. Map `json_pretty -> JsonWriter::PRETTY`, `json_compact -> COMPACT`.

---

## 5. Action wiring (M4, grounded by Agent A)

In `src/kudu/tools/tool_action_cluster.cc`:

- Builder goes in `BuildClusterMode()` (`:391`), beside the ksck builder
  (`:395-425`) and rebalance (`:427-459`), before `builder.Build()` (`:461`).
- Use `ClusterActionBuilder("gather", &RunGather)` -- it auto-adds the
  `master_addresses` positional. Chain `.Description(...)`, `.ExtraDescription(...)`,
  and `.AddOptionalParameter("<flag>")` for each gather flag. Do NOT add
  `master_addresses`, `timeout_ms`, or `negotiation_timeout_ms` (auto-added;
  see section 0).
- Runner signature (anonymous namespace, matching neighbors):
  `Status RunGather(const RunnerContext& context)`.
- Parse addresses: `vector<string> master_addresses; RETURN_NOT_OK(
  ParseMasterAddresses(context, &master_addresses));` then hand them to the
  engine.

Gather-specific flags to add at file scope in `tool_action_cluster.cc` (the
`sections` flag is already DEFINEd at `:73` -- reuse it; grep each of the
following before DEFINE and DECLARE instead if a name already exists):

- `--tservers` (string, csv of uuid|host) -- NEW.
- `--location` (string, prefix) -- NEW.
- `--table` (string) -- CHECK: plural `tables` exists at
  `tool_action_common.cc:140` and plural `tablets` at `:120`; the singular
  `table`/`tablet` likely do not, but grep to confirm. If `table`/`tablet`
  collide, use `gather_table` / `gather_tablet` as flag names.
- `--tablet` (string, id) -- CHECK as above.
- `--row` (string, `table:pk-json`) -- NEW.
- `--include_http_metrics` (bool, default false) -- NEW.
- `--gather_format` (string, default `json_pretty`) -- NEW (section 4).
- `--fetch_info_concurrency` -- already DEFINEd at `ksck.cc:63`; DECLARE_int32,
  do not re-DEFINE.

Selector flags are mutually exclusive scopes; `RunGather` validates that at most
one entity scope is set and returns `Status::InvalidArgument` otherwise.

---

## 6. Disposition entry (M5, grounded by Agent C -- mandatory, not optional)

In `src/kudu/tools/mcp_disposition.cc`, add to the `RawEntries()` table
(`:68-213`), immediately after the `cluster ksck` row (`:72`):

```
{"cluster gather", Disposition::SURFACE, !kLocal, kSafe},
```

Struct is `RawDispositionEntry { const char* command_path; Disposition
disposition; bool node_local; bool unsafe; }` (`mcp_disposition.h:80-87`);
shorthands `kLocal=true`, `kSafe=false`, `kUnsafe=true`. Omitting this entry
hard-crashes the serve loop at startup: `ValidateDispositionCoverageOrDie`
(`mcp_disposition.cc:347-350`, called from the serve loop) aborts on any
reachable action without exactly one entry. So M5 is required for the tool to
even start under `mcp serve`.

Tool name derives automatically: `McpToolName` (`tool_action_mcp.cc:255-259`)
maps `"cluster gather" -> "cluster_gather"`. The input schema is built by
reflection (`WriteMcpToolObject`, `:374-520`); `master_addresses` is omitted
because `IsInjectedConnectionArg` (`:229-231`) injects it at call time
(`InjectedConnectionValue`, `:667-676`, reads `FLAGS_master_addresses`). No
`tserver_address` property will appear -- which is the whole point of the
feature.

---

## 7. remote_cluster.h -- the frozen interface (M0 produces this)

M0 delivers a compiling header + stub .cc so M1/M2/M3 build in parallel against
stable signatures. Shape (names indicative; M0 finalizes and freezes):

```cpp
namespace kudu { namespace tools {

// What the caller asked to gather. Built from the action's flags.
struct GatherOptions {
  enum class Scope { kCluster, kServers, kLocation, kTable, kTablet, kRow };
  Scope scope = Scope::kCluster;
  std::vector<std::string> servers;   // uuid or host, for kServers
  std::string location;               // for kLocation
  std::string table;                  // for kTable / kRow
  std::string tablet_id;              // for kTablet
  std::string row_pk_json;            // for kRow
  std::set<Section> sections;         // inventory, consensus, quiescing, flags,
                                      // memory, identity
  bool include_http_metrics = false;
};

// Resolves a selector into targets + projection using master metadata only,
// before any per-server RPC. (M1 owns the body.)
struct ResolvedTarget {
  std::string uuid, host, location, version;
  int64 millis_since_heartbeat;
  bool presumed_dead;
  // For entity scopes: which tablet(s) on this server the probes scope to.
  std::vector<std::string> scoped_tablet_ids;
};
struct ResolvedPlan {
  std::vector<ResolvedTarget> targets;
  bool entity_centric;                // tablet/row -> replica-set projection
  std::string applied_selector;       // human string for the report
};
Status ResolvePlan(client::KuduClient* client,
                   const GatherOptions& opts,
                   ResolvedPlan* out_plan);

// Runs the probes across targets concurrently, isolates failures, fills the PB.
// (M2 owns the body; M3 owns entity projection.)
class RemoteGatherer {
 public:
  static Status Create(const std::vector<std::string>& master_addresses,
                       std::unique_ptr<RemoteGatherer>* out);
  Status Run(const GatherOptions& opts, GatherResultsPB* out);
 private:
  std::shared_ptr<rpc::Messenger> messenger_;
  client::sp::shared_ptr<client::KuduClient> client_;
  std::unique_ptr<ThreadPool> pool_;
};

}} // namespace kudu::tools
```

`RemoteGatherer::Create` calls `BuildMessenger("remote-gather", &messenger_)`,
builds the `KuduClient` (multi-master failover, as ksck does), and builds the
bounded `pool_`. `Run` calls `ResolvePlan`, then the fan-out loop, then the
rollup + projection.

A single per-target `RemoteGatherServer` helper (holds the four proxies; built
via an `Init(messenger, Sockaddr, host)` mirroring `ksck_remote.cc:203-215`)
performs the selected probes into a `GatherResultsPB::ServerRecord`. This is a
standalone class, NOT derived from `KsckTabletServer`.

---

## 8. Milestone DAG and agent assignment

Waves (dependency-ordered; within a wave, agents run in parallel against frozen
interfaces):

- **Wave 1 -- M0 (foundational, single agent, blocking).**
  - Add `GatherResultsPB` to `tool.proto`; confirm it generates and links.
  - Write `remote_cluster.h` with the frozen structs/signatures (section 7) and
    a `remote_cluster.cc` that compiles with stubbed bodies
    (`return Status::NotSupported("TODO")`).
  - Add `remote_cluster.cc` to `tools/CMakeLists.txt` (`kudu_tools_util`).
  - Gate: the tree builds clean (`GatherResultsPB` symbols resolve, header
    parses). Nothing functional yet. This freezes contracts for Wave 2/3.

- **Wave 2 -- parallel against M0's interfaces.**
  - **M1 (resolver):** implement `ResolvePlan` -- `ListTabletServers`
    enumeration for cluster/servers/location; `GetTableLocations` for table;
    master tablet-by-id lookup for tablet; partition-schema key resolution
    (as `table locate_row`) for row. Sets `presumed_dead` from
    `millis_since_heartbeat`. Pure master-metadata; no per-server RPC.
  - **M2 (engine):** implement `RemoteGatherer::Create`/`Run` fan-out loop and
    the `RemoteGatherServer` probes (GetStatus, ListTablets, ServerClock,
    Quiesce, GetConsensusState, GetFlags, DumpMemTrackers) into `ServerRecord`,
    plus per-target failure isolation and the `Rollup`. Server-centric
    projection only.

- **Wave 3 -- against M1+M2.**
  - **M3 (entity-scoped):** per-probe scoping to `scoped_tablet_ids` and the
    `ReplicaSetView` (rows-are-replicas) projection for tablet/row scopes;
    `table` per-tablet map option.
  - **M4 (action wiring):** flags + `RunGather` + builder in
    `tool_action_cluster.cc`; `--gather_format` JSON emission. Depends on the
    engine's `Run` being callable (M2) but the flag/builder scaffolding can
    start as soon as M0's header is frozen.
  - **M5 (disposition):** the one-line entry (section 6). Tiny; can land any
    time after M4 names the action, but MUST land before any `mcp serve` smoke
    test.

- **Wave 4 -- M6 (tests):** see section 9. Depends on M2/M3/M4.

M4 and M5 are small and tightly coupled (both about the action's existence); one
agent may own both. M1 and M2 are the substantial parallel leaves. M3 extends
M2's projection and should be the same agent as M2 or a close follow-on to avoid
churning the engine's internals across two owners.

---

## 9. Test plan (M6)

- **Resolver unit tests (M1):** with a faked/mini master or the existing
  cluster-test harness, assert each selector maps to the expected target set:
  cluster -> all; tservers -> subset; location prefix -> matching; table ->
  union of replica-holders; tablet -> exactly the replica set; row -> the
  owning tablet's replica set. Assert mutually-exclusive-scope validation
  returns `InvalidArgument`.
- **Engine integration tests (M2):** stand up an `ExternalMiniCluster` (or
  `InternalMiniCluster`) with a few tservers, run gather cluster-wide, assert:
  every server has a record; `ServerRecord` fields populated (uuid/version/
  inventory); rollup totals equal the sum of per-server replica counts; killing
  one tserver yields a `UNREACHABLE`/`TIMED_OUT` record for it while the others
  still return (failure isolation). Follow the pattern in existing ksck /
  tool_action cluster tests under `src/kudu/tools/`.
- **Entity projection tests (M3):** create a table, pick a tablet, run
  `--tablet=<id>`; assert only the replica-holders were contacted and the
  `ReplicaSetView` lists the expected copies leader-first with per-copy term /
  size / data-dir. Run `--row` and assert it resolves to the same tablet.
- **Authz degradation test (M3/M2):** run as a non-superuser identity (or with
  access control raised) and assert consensus/flags/memory sections come back
  `UNAUTHORIZED` while identity/inventory/clock still populate -- i.e. the call
  does not fail wholesale.
- **Disposition/reflection test (M5):** assert `cluster gather` appears in
  `tools/list` as `cluster_gather` with SURFACE semantics and NO
  `tserver_address` property, and that the server starts (coverage invariant
  passes). If a disposition golden test exists, update it.
- **No-unicode check:** `grep -nP '[^\x00-\x7F]'` on all new/edited files.

---

## 10. Risk register

- **Duplicate gflag DEFINE.** Highest-probability compile/runtime break. Every
  agent adding a flag greps first; reuse via DECLARE. `fetch_info_concurrency`
  and `timeout_ms` already exist -- DECLARE only.
- **Double-add of auto-injected args** (`master_addresses`, `timeout_ms`,
  `negotiation_timeout_ms`). Startup crash. M4 must not add them (section 0).
- **Missing disposition entry.** Startup crash under `mcp serve` via
  `ValidateDispositionCoverageOrDie`. M5 is mandatory (section 6).
- **Proto churn after freeze.** If M1/M2 discover a needed field mid-build,
  route it through the orchestrator as a coordinated M0 amendment; do not fork
  the proto shape per-agent.
- **Interface drift between M1 and M2.** Both consume M0's `remote_cluster.h`;
  neither edits its signatures unilaterally.
- **DumpMemTrackers is the one new proxy call.** Its response shape differs from
  the ksck-fetched probes; the M2 agent confirms the field mapping against
  `server_base.proto` rather than assuming.
- **Entity resolution edge cases** (deleted tablet, moved replica, bad pk json).
  M1 returns a clean `NotFound`/`InvalidArgument`, never a crash; M6 covers.
```
