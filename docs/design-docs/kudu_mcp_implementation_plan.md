# kudu mcp serve -- Implementation Plan and Progress Tracker

Companion to `kudu_mcp_prd.md`. The PRD says *what* and *why*; this doc says
*in what order* and *tracks how far we got*. Update the checkboxes as work lands.

Status legend: `[ ]` not started, `[~]` in progress, `[x]` done.

Ground rules (from `.claude/CLAUDE.md`):
- No unicode characters in code or docs.
- When breaking a big change into smaller commits with git: never push to any
  remote, never modify the active branch; build the new commit stack on a fresh
  branch created from it.

Testing rule (applies to every milestone below): each milestone ships with unit
tests covering both the happy path and the bad paths (malformed input, missing
required args, wrong types, unknown tools, disposition violations, error
`Status` from the action). A milestone is not "done" until its bad-path tests
pass too. Bad paths are where the confidence lives: this is a long-lived server
process parsing untrusted-shape JSON and touching process-global gflag state, so
the failure modes matter as much as the success case. Per-milestone test
expectations are listed inline; M6 is end-to-end hardening on top, not a
substitute for the earlier unit tests.

---

## 0. Scope of this plan

Implements PRD v1 only: stdio transport, `tools` capability, read-only tools
surfaced freely, one gated write behind `--allow-writes` + dry-run + confirm.
Out of scope (tracked in PRD section 9): async long-runners, resources, HTTP
transport, node-local mutations.

The whole feature is additive. It touches:
- `src/kudu/tools/tool_main.cc` -- one line to add `BuildMcpMode()` to
  `RootMode()` (`tool_main.cc:62`).
- `src/kudu/tools/tool_action_mcp.cc` -- NEW, the entire server.
- `src/kudu/tools/mcp_disposition.{h,cc}` -- NEW, the curated safety table.
- `src/kudu/tools/tool_action.h` -- add `BuildMcpMode()` declaration alongside
  the other `Build*Mode()` decls (`tool_action.h:333-346`).
- `src/kudu/tools/CMakeLists.txt` -- add the new sources to `KUDU_CLI_TOOL_SRCS`
  (around `CMakeLists.txt:106-119`) and a new `ADD_KUDU_TEST`.

Nothing in the existing action tree changes. `Action::Run()`
(`tool_action.cc:229`) is called exactly as `DispatchCommand` calls it today.

---

## 1. Key design decisions to lock before coding

These are the "how", not yet decided in the PRD. Resolve at the top of M0.

- **D-plan-1: `mcp serve` is a normal Mode+Action.** Add `BuildMcpMode()`
  returning a mode `mcp` with one action `serve`. Its runner enters the
  JSON-RPC loop and does not return until stdin EOF. This reuses the entire
  existing dispatch/help/flag-parse path; `main()` needs no special-casing.

- **D-plan-2: The serve loop owns its own copy of the action tree.** The runner
  calls `RootMode(gflags::GetArgv0())` once to build the tree, then walks it for
  `tools/list` and dispatch. Cheap; matches how `DumpToolXML` (`tool_main.cc:151`)
  already builds a throwaway tree.

- **D-plan-3: Disposition is a static curated table keyed by full command path**
  (e.g. `"table describe"`, `"master set_flag"`), NOT per-`ActionBuilder`
  annotation. Rationale: keeps the change additive (no edits to 14 builder
  files), and matches the PRD's "regenerate it from `kudu --helpxml`" intent
  (section 6). A CHECK at startup asserts every reflected action appears in the
  table exactly once, so a newly added CLI action fails loudly until classified.

- **D-plan-4: stdout is the protocol channel; capture `cout` around `Run()`.**
  Dup fd 1 (or hold the original `std::cout.rdbuf()`) before the loop starts,
  write all JSON-RPC to that held stream, and swap `cout.rdbuf()` to a
  `std::ostringstream` for the duration of each `Run()` (R3/S4). glog is already
  on stderr (`logtostderr` default true, `tool_main.cc:261`), so it never
  corrupts the channel.

Open PRD decisions (section 8) that this plan defers to M5:
- Which write ships first. Plan recommendation: `add_range_partition` /
  `drop_range_partition` -- it demonstrates "partition work" directly, is a
  normal (non-blocking) mutation, and is lower-blast-radius than `delete`.
  `leader_step_down` is the fallback if range-partition args prove fiddly.

---

## 2. Milestones (each is roughly one reviewable commit/PR)

### M0 -- Scaffolding and a stdin echo loop  `[x]`
Goal: `kudu mcp serve` builds, starts, reads newline-delimited JSON from stdin,
writes a hardcoded response, exits cleanly on EOF.
- [x] Add `BuildMcpMode()` decl (`tool_action.h`) and definition
      (`tool_action_mcp.cc`); wire into `RootMode()` (`tool_main.cc:62`).
- [x] Add `--allow-writes` (default false) and `--mcp_dry_run`-style flags as
      gflags in `tool_action_mcp.cc`; register `--allow-writes` as an
      `AddOptionalParameter` on the `serve` action.
      (M0 added only `--allow-writes`; the dry-run flag is deferred to M5 where
      it is actually consumed.)
- [x] CMake: add `tool_action_mcp.cc` + `mcp_disposition.cc` to
      `KUDU_CLI_TOOL_SRCS` (`CMakeLists.txt:106`).
      (M0 added only `tool_action_mcp.cc`; `mcp_disposition.cc` is created in M2.)
- [x] Loop: read a line, parse with `JsonReader` (`util/jsonreader.h`), log the
      method to stderr, respond, loop until EOF.
- [x] Tests -- happy: one well-formed line in, one response line out, clean EOF
      exit 0. Bad: empty stdin, a blank line, a truncated/garbage line -> loop
      does not crash and keeps serving the next line.
Exit criteria: `echo '{}' | kudu mcp serve` prints one line and exits 0.

### M1 -- JSON-RPC 2.0 framing + MCP lifecycle  `[x]`
Goal: correct `initialize` handshake and framing; a real MCP host connects.
- [x] Implement request/response/error envelope with `JsonWriter`
      (`util/jsonwriter.h`): `id`, `result` | `error {code,message}`.
- [x] Handle `initialize` -> reply `protocolVersion`, `serverInfo`,
      `capabilities: { tools: {} }` (tools only per PRD S4).
- [x] Handle `notifications/initialized` (no reply), unknown methods ->
      JSON-RPC error -32601, malformed JSON -> -32700.
- [x] Handle EOF and a `shutdown`/EOF path without crashing.
- [x] Tests -- happy: `initialize` returns the expected `protocolVersion` /
      `serverInfo` / `capabilities`; a valid request `id` echoes back on the
      response. Bad: unknown method -> -32601, malformed JSON -> -32700, missing
      `jsonrpc`/`id` fields, a notification (no `id`) produces no response line.
Exit criteria: `claude mcp add kudu-local -- kudu mcp serve --master_addresses=...`
completes the handshake and lists zero tools.

### M2 -- Disposition table + startup validation  `[x]`
Goal: the safety spec exists as code, independently testable, before any tool is
exposed.
- [x] `mcp_disposition.h`: enum `{ SURFACE, GATED, REJECT, EXCLUDE }` plus flags
      `node_local`, `unsafe`.
- [x] `mcp_disposition.cc`: the full table transcribed from PRD section 6, keyed
      by `"<mode> <action>"` (and multi-level like `"tserver quiesce status"`).
- [x] Lookup helper `DispositionFor(const vector<Mode*>& chain, const Action*)`.
- [x] Startup CHECK: walk `RootMode()`, assert every action has exactly one table
      entry; fail with the offending path if not (catches future CLI additions).
- [x] Tests `mcp_disposition-test` -- happy: a sample of each class
      (SURFACE/GATED/REJECT/EXCLUDE) resolves to the expected disposition and
      flags; the "every action classified exactly once" invariant holds against
      the current tree. Bad: a synthetic action absent from the table trips the
      startup CHECK; a duplicate entry is rejected; an unknown command path
      lookup fails cleanly.
Exit criteria: table compiles, invariant test passes against current tree.

### M3 -- tools/list via reflection  `[x]`
Goal: `tools/list` emits MCP tool JSON, filtered by disposition.
- [x] Port the traversal in `Action::BuildHelpXML` (`tool_action.cc:308-380`) to
      emit MCP tool objects: `name` (flattened path, e.g. `table_describe`),
      `description`, `inputSchema` (JSON Schema `object`).
- [x] required args -> `string` properties in `required[]`; variadic -> array of
      strings; optional flags -> typed properties, type read from
      `google::GetCommandLineFlagInfoOrDie(...).type` (bool/int -> matching JSON
      type, else string), exactly as the XML walk reads it.
- [x] Filter: SURFACE always; GATED only when `--allow-writes`; REJECT/EXCLUDE
      never listed. Tag GATED tools (annotation for host confirm) and node-local
      SURFACE tools (description note).
- [x] Reserve/scrub the `--allow-writes` and dry-run flags from every tool's
      input schema so the model cannot set them per-call.
- [x] Tests -- happy: a known read action emits valid JSON Schema with the right
      required/optional properties and types (bool/int/string) pulled from
      gflags; SURFACE-only listing without `--allow-writes`; GATED appear with
      `--allow-writes`. Bad: REJECT/EXCLUDE never appear in either mode; the
      control flags (`--allow-writes`, dry-run) never leak into any input schema;
      an optional flag with an odd gflag type falls back to `string` rather than
      producing invalid schema.
Exit criteria: host shows the read-only tool set; adding `--allow-writes` adds
the gated ones.

### M4 -- tools/call for read-only actions  `[x]`
Goal: the model can actually run reads and get text back. This is the core of
S3/S4 and the two "genuinely mechanical" guards R2/R3.
- [x] Resolve tool name -> `(chain, Action*)`.
- [x] Build `required_args` map and `variadic_args` from the JSON `arguments`
      object (analog of `MarshalArgs`, `tool_main.cc:85`); return JSON-RPC
      -32602 on missing required args.
- [x] R2 (gflags save/restore): for each declared optional flag present in
      `arguments`, save via `google::GetCommandLineOption`, set via
      `google::SetCommandLineOption`, run, then restore in a scope guard.
      Precedent: `SetOptionalParameterDefaultValues` (`tool_action.cc:382`).
- [x] R3/S4 (cout capture): swap `std::cout.rdbuf()` to an `ostringstream`
      around `action->Run(...)` (`tool_action.cc:229`), restore after, put the
      captured text in `result.content[].text`. Map non-OK `Status` to a
      tool-call error (isError=true) with `Status::ToString()`.
- [x] Bring up incrementally: (a) zero-arg read `master status`; (b) required-arg
      read `table describe <table>`; (c) optional-flag read (e.g. a `scan` with a
      limit) to exercise R2.
- [~] Add `cluster rebalance` with a forced `report_only=true` as the read-only
      imbalance planner (PRD v1 scope; flag at `tool_action_cluster.cc:118`).
      DEFERRED (2026-09-09): `cluster rebalance` is classified REJECT in the
      disposition table (a blocking action). tools/call resolves names against
      the M3 registry, which only holds SURFACE and (behind --allow-writes)
      GATED entries; REJECT/EXCLUDE actions are never surfaced. Exposing
      rebalance would require either weakening its REJECT disposition (forbidden
      -- the gate for blocking actions must stay intact) or building synthetic
      "forced-flag tool" machinery outside the registry. Both are out of scope
      for M4, so the disposition table is left untouched and rebalance stays
      unreachable. Revisit under M5/M6 if a report-only planner tool is desired.
- [x] Tests -- happy: zero-arg, required-arg, and optional-flag reads each
      return the expected captured text against a MiniCluster; captured `cout`
      matches the CLI output for the same command. Bad: missing required arg ->
      -32602; wrong-type arg; unknown tool name; an action returning non-OK
      `Status` -> tool result `isError=true` carrying `Status::ToString()`.
- [x] Tests -- R2 regression: after a call that sets optional flags, assert every
      touched `FLAGS_*` is restored to its prior value (including when `Run()`
      returns an error, i.e. the scope guard restores on the error path too).
- [x] Tests -- R3 regression: assert nothing an action prints to `cout` leaks
      onto the protocol stream, and that glog output on stderr never appears in a
      response.
Exit criteria: against a MiniCluster, `ksck`, `table list/describe`,
`master status`, and `rebalance --report_only` return correct captured text.
Met for `table list/describe` and `master status` (see tool_action_mcp-itest);
the `rebalance --report_only` planner is deferred (see the item above).

### M5 -- Write gating: one gated write, end to end  `[x]`
Goal: prove the D4 gate: hidden without `--allow-writes`, dry-run echo, confirm
annotation, real execution.
- [x] Mark GATED tools with the MCP annotation that triggers the host confirm
      prompt (destructive/confirm hint). (Present from M3:
      `readOnlyHint:false, destructiveHint:true`; verified + kept.)
- [x] Dry-run: when the call sets the dry-run arg, do NOT `Run()`; return the
      exact literal command line that would execute (the PRD's "echoes the exact
      command"), reconstructed from chain + args.
- [x] Ship `add_range_partition` as the first gated write (D-plan-4
      recommendation; `leader_step_down` fallback not needed).
- [x] Verify GATED tools are absent from `tools/list` without `--allow-writes`
      and that calling one anyway is rejected server-side (defense in depth),
      now with a distinct "requires --allow-writes" message.
- [x] Tests -- happy: with `--allow-writes`, a range-partition add executes and
      the change is observable on the cluster; the GATED tool carries the confirm
      annotation; dry-run returns the exact literal command and leaves the
      cluster unchanged. Bad: without `--allow-writes`, the gated call is
      rejected server-side (not just hidden); a dry-run call is asserted to make
      zero mutating RPCs; malformed partition args -> -32602, not a partial
      mutation.
Exit criteria: with `--allow-writes`, a range-partition add runs after confirm;
without it, the tool is neither listed nor callable; dry-run never mutates.

### M6 -- Hardening and tests  `[x]`
- [x] Integration test in the style of `kudu-tool-test.cc` /
      `kudu-admin-test.cc`: spawn the server as a subprocess, drive stdin with
      recorded JSON-RPC, assert responses (list, a read call, a gated-call
      rejection, malformed input). Landed as `SubprocessServeEndToEnd`
      (`tool_action_mcp-itest.cc`): spawns a real `kudu mcp serve`, speaks
      JSON-RPC over its stdin/stdout pipes, and asserts exit-status 0 after EOF.
- [x] R4: confirm exposed admin actions return `Status` (not `exit`/`CHECK`) on
      the error paths we surface; note that node-local fatal-heavy actions are
      EXCLUDE anyway. Keep `serve` cheap to relaunch. Survival is proved by the
      gated-then-survive sequence in the subprocess test and the in-process
      error-path tests; the residual `ValidateFlags()` exit path is documented
      at the `Run()` call site and mechanically guarded by
      `NoSurfacedToolExposesUnsafeOrExperimentalFlag` (no surfaced tool exposes
      an unsafe/experimental optional flag).
- [x] R1 sanity: assert REJECT/EXCLUDE actions never reach dispatch. Landed as
      `NoRejectOrExcludeActionIsEverExposed` (exhaustive tree walk: every
      REJECT/EXCLUDE action's tool name is absent from `tools/list` in both write
      modes) plus `ToolsCallRejectOrExcludeToolIsRefused` (dispatch side).
- [x] Scrub for raw `printf`/fd-1 writes in the SURFACE/GATED action set (R3
      residual); the common `DataTable::PrintTo(ostream&)` path
      (`tool_action_common.h:282`) is already captured. Scrub clean: no surfaced
      C++ action writes to fd 1 directly (all use `std::cout`/ostream, captured
      by the `rdbuf` swap); the only `printf` in the tool sources is in
      `trace_io.stp` (SystemTap, not compiled).
- [x] Tech debt (introduced M2/M3): the root-mode tree was assembled in THREE
      places -- `RootMode()` (`tool_main.cc`), `BuildFullRootMode()`
      (`mcp_disposition-test.cc`), and `BuildMcpRootMode()`
      (`tool_action_mcp.cc`). De-duplicated: a single `BuildRootMode(name)` is
      declared in `tool_action.h` and defined in `tool_action_mcp.cc`; all three
      now delegate to it, so a new top-level mode is wired in exactly one place.
      (Defined in `tool_action_mcp.cc`, not `tool_action.cc` as first sketched:
      `tool_action.cc` is in the low-level `kudu_tools_util` library, and
      referencing every `Build*Mode()` factory from there would inject undefined
      symbols into every `kudu_tools_util` consumer, e.g. `ksck-test`.
      `tool_action_mcp.cc` already lives in the CLI sources and already
      references every factory, so it is the link-safe home.)

### M7 -- Docs and registration guidance  `[x]`
- [x] Operator doc: the `claude mcp add ... -- ssh -T -q ... kudu mcp serve`
      registration recipe (PRD section 3), `--allow-writes` semantics, and the
      list of surfaced vs gated tools.
- [x] Cross-link from the PRD status line.

---

## 3. Dependency order and what can parallelize

```
M0 -> M1 -> M3 -> M4 -> M5 -> M6 -> M7
             ^
M2 ----------+   (M2 is independent of M0/M1; needed by M3)
```
M2 (disposition table + test) has no dependency on the JSON-RPC loop and can be
built and reviewed in parallel with M0/M1. M3 needs both M1 (framing) and M2
(filter). M4 is the largest single milestone; if it needs splitting, cut it at
the "zero-arg read works" line (M4a) vs "optional flags via gflags save/restore"
(M4b).

---

## 4. Risk-to-milestone map (PRD section 5)

| PRD guard | Where handled | Notes |
|-----------|---------------|-------|
| R1 blocking actions | M2 (table) + M6 (assert) | v1 REJECTs them; async is Future work |
| R2 gflags global state | M4 | save/restore scope guard around `Run()` |
| R3 stdout cleanliness | M4 (+M6 scrub) | `cout.rdbuf()` swap; glog already stderr |
| R4 process-fatal paths | M6 | exposed set returns `Status`; keep relaunch cheap |
| R5 per-call init | none needed | verified idempotent in PRD; no action |
| R6 reflection to schema | M3 | mechanical port of `BuildHelpXML` |

---

## 5. Progress log

Append dated entries as milestones land. Keep newest last.

- 2026-09-09 -- Plan drafted from PRD v0.1. Verified all PRD code references
  against the tree (RootMode `tool_main.cc:62`, BuildHelpXML `tool_action.cc:308`,
  MarshalArgs/DispatchCommand `tool_main.cc:85/124`, gflags-global optional args
  `tool_action.h:146`, SetOptionalParameterDefaultValues `tool_action.cc:382`,
  JSON utils present, `DataTable::PrintTo` `tool_action_common.h:282`,
  `report_only` `tool_action_cluster.cc:118`). Nothing started.

- 2026-09-09 -- M0 landed. `kudu mcp serve` now builds, reads newline-delimited
  JSON from stdin, writes one placeholder response line per parseable request,
  and exits 0 on EOF. Malformed/blank lines are logged (glog -> stderr) and
  skipped without crashing.
  Files added:
    - `src/kudu/tools/tool_action_mcp.cc` -- the `mcp` mode + `serve` action, the
      `--allow-writes` gflag (DEFINE_bool, default false, registered via
      `AddOptionalParameter` on `serve`; NOT enforced yet -- reserved for M5),
      and the read-loop.
    - `src/kudu/tools/tool_action_mcp-test.cc` -- gtest for the loop (6 cases).
  Files changed:
    - `src/kudu/tools/tool_action.h` -- added `BuildMcpMode()` decl.
    - `src/kudu/tools/tool_main.cc` -- wired `BuildMcpMode()` into `RootMode()`.
    - `src/kudu/tools/CMakeLists.txt` -- added `tool_action_mcp.cc` to
      `KUDU_CLI_TOOL_SRCS`; added `ADD_KUDU_TEST(tool_action_mcp-test)` plus a
      `target_sources(... tool_action_mcp.cc)` so the test can link the loop
      (the tool sources compile into the `kudu` executable, not a library).
  Build: `ninja kudu` and `ninja tool_action_mcp-test` both succeed.
  Test: `./bin/tool_action_mcp-test` -> 6/6 PASSED. Smoke: happy line -> one
    response + exit 0; empty stdin -> no output + exit 0; garbage line -> skipped,
    following valid line still answered, exit 0.
  Handoff for M1:
    - The loop body is `RunMcpServeLoop(std::istream&, std::ostream&)` in
      `tool_action_mcp.cc` (non-static, so tests link it). The action runner
      `RunMcpServe` just calls it with `std::cin`/`std::cout`.
    - Responses are currently a fixed literal line
      `{"jsonrpc":"2.0","result":{}}` written with `std::endl` (flushes per line).
      Replace this with real JSON-RPC 2.0 framing via `JsonWriter` (id echo,
      result|error envelope, `initialize` handshake). There is a
      `TODO(mcp-M1)` marker at that spot.
    - M0 deliberately writes NO response for malformed/blank lines. M1 should add
      the `-32700` parse-error reply (and decide notification-vs-request framing).
    - `--allow-writes` exists but is inert; enforcement + dry-run flag land in M5.

- 2026-09-09 -- M1 landed. `kudu mcp serve` now speaks JSON-RPC 2.0 with the MCP
  lifecycle. The M0 placeholder line is gone; `RunMcpServeLoop(istream&,
  ostream&)` now parses each line, distinguishes requests (carry an `id`) from
  notifications (no `id`, or any `notifications/*` method -> zero output), and
  dispatches by method. Every response is built with `JsonWriter`, carries
  `"jsonrpc":"2.0"`, echoes the request `id` with its original type preserved
  (number vs string; null/absent -> JSON null), and contains EITHER `result` OR
  `error {code,message}`, never both.
    - `initialize` -> `protocolVersion` + `capabilities:{tools:{}}` (tools only,
      no resources) + `serverInfo:{name,version}`. Chosen serverInfo name
      `"kudu-mcp"`; version is `VersionInfo::GetShortVersionInfo()` (e.g.
      `1.19.0-SNAPSHOT`). Protocol version negotiation: the client's requested
      `params.protocolVersion` is echoed if supported (set: `2024-11-05`,
      `2025-03-26`, `2025-06-18`), else the server default `2025-06-18`.
    - `tools/list` -> `{ "tools": [] }` (empty for now; populated in M3).
    - Unknown method -> `-32601`; malformed JSON line -> `-32700` (null id, loop
      continues); valid non-object JSON or a request with an `id` but no
      `method` -> `-32600`. Blank / whitespace-only lines ignored.
  Files changed:
    - `src/kudu/tools/tool_action_mcp.cc` -- replaced the placeholder with the
      JSON-RPC 2.0 + MCP handshake layer (envelope builders, id echo, protocol
      negotiation, method dispatch). Removed the `TODO(mcp-M1)` marker; left
      `TODO(mcp-M3)` on `tools/list` population and `TODO(mcp-M4)` on
      `tools/call`. `BuildMcpMode()`/`--allow-writes` plumbing unchanged.
    - `src/kudu/tools/tool_action_mcp-test.cc` -- rewritten to assert with
      `JsonReader` (not brittle substring matches): initialize handshake fields,
      numeric + string id echo, unsupported-version fallback, empty tools/list,
      `-32601`/`-32700`/`-32600` bad paths, notification -> no output, and a
      "never both result and error" envelope invariant.
  Build: `ninja kudu` and `ninja tool_action_mcp-test` both succeed.
  Test: `./bin/tool_action_mcp-test` -> 13/13 PASSED. Smoke: `initialize` emits
    one well-formed handshake and exits 0; unknown method -> `-32601`; a garbage
    line -> `-32700` then the next valid line is still answered; a
    `notifications/initialized` line -> no output.
  Handoff for M2/M3:
    - M2 is independent of this work: it builds `mcp_disposition.{h,cc}` (the
      curated SURFACE/GATED/REJECT/EXCLUDE table + startup invariant CHECK) and
      does not touch the loop.
    - M3 populates the currently-empty `tools/list` array left here. The
      response builder is `BuildToolsListResponse(const rapidjson::Value* id)`
      in `tool_action_mcp.cc`; today it writes `"tools":[]`. M3 fills that array
      by reflecting over `RootMode()`, filtered by the M2 disposition table.

- 2026-09-09 -- M2 landed. The curated safety spec now exists as code and is
  independently unit-tested against the live action tree, before any tool is
  surfaced.
  Files added:
    - `src/kudu/tools/mcp_disposition.h` -- `enum class Disposition
      { SURFACE, GATED, REJECT, EXCLUDE }`, `struct DispositionInfo`
      (disposition + `node_local` + `unsafe` + a `classified` flag that is false
      on a table miss so callers can never silently default), `struct
      RawDispositionEntry` (a raw table row), and the public API (see the M3
      handoff for exact signatures).
    - `src/kudu/tools/mcp_disposition.cc` -- the full static table (112 entries)
      transcribed from PRD section 6 and reconciled against `kudu --helpxml`,
      plus the lookup, coverage walk, and duplicate check.
    - `src/kudu/tools/mcp_disposition-test.cc` -- gtest (10 cases).
  Files changed:
    - `src/kudu/tools/CMakeLists.txt` -- added `mcp_disposition.cc` to
      `KUDU_CLI_TOOL_SRCS`; introduced `KUDU_CLI_TOOL_SRCS_NO_MAIN` (the tool
      sources minus `tool_main.cc`); added `ADD_KUDU_TEST(mcp_disposition-test)`
      that `target_sources`es `KUDU_CLI_TOOL_SRCS_NO_MAIN` and links
      `KUDU_CLI_TOOL_LINK_LIBS` (see "how the tree is obtained in tests" below).
  How the key is built: `DispositionCommandPath(chain, action)` joins the mode
  chain names AFTER the root with the action name using single spaces, e.g. a
  chain of {root, "tserver", "quiesce"} + action "status" -> the key
  `"tserver quiesce status"`. This handles MULTI-LEVEL submodes natively; the
  table stores the full path, never just parent-mode + action. Multi-level
  paths present in the real tree that the PRD section 6 tables had flattened or
  named differently (all classified per the PRD's intent, only the key changed):
    - `fs dump {block,cfile,tree,uuid}` (PRD listed these flat under `fs`).
    - `local_replica dump {block_ids,data_dirs,meta,rowset,wals}`,
      `local_replica cmeta {print_replica_uuids,rewrite_raft_config,set_term,
      unsafe_recreate}`, `local_replica tmeta delete_rowsets` (PRD listed flat).
    - `master authz_cache refresh` (PRD called it `master refresh`).
    - `table set_limit {disk_size,row_count}` (PRD listed flat under `table`).
    - `tserver quiesce {status,start,stop}` (PRD said `quiescing`; real submode
      is `quiesce`) and `tserver state {enter_maintenance,exit_maintenance}`
      (PRD listed flat under `tserver`).
    - `tablet change_config {add_replica,change_replica_type,move_replica,
      remove_replica}` (PRD listed flat under `tablet`).
  The `test` mode decision: `DispositionFor` special-cases any chain whose first
  post-root mode is named `test` and returns a CLASSIFIED `EXCLUDE` without a
  table lookup. Rationale: the `test` mode (`kudu test mini_cluster`) exists only
  in `KUDU_CLI_TEST_TOOL_ENABLED` builds (the unit-test binary is such a build),
  and its actions are not real MCP tools. Special-casing keeps the coverage
  invariant green without table churn if test-only tooling changes. This is the
  single, consistent rule (no separate "skip in validator" path).
  Actions classified BEYOND PRD section 6 (genuinely new, a human should
  confirm):
    - `mcp serve` -> REJECT. Not in the PRD. It is the MCP server entrypoint
      itself: a blocking daemon-style loop that never returns until stdin EOF.
      Classified REJECT by rule 2 (blocking), consistent with `master run` /
      `tserver run` in the PRD's "never tools" REJECT bucket. Never surfaced.
    - `test mini_cluster` -> EXCLUDE via the `test`-mode special-case above
      (test-only tooling, not a real tool).
  unsafe=true is set on: `master unsafe_rebuild`, `tablet
  unsafe_replace_tablet`, `remote_replica unsafe_change_config`, and the raw
  consensus edits `local_replica cmeta {rewrite_raft_config,set_term,
  unsafe_recreate}`. Note: the PRD marks `tablet unsafe_replace_tablet` and
  `remote_replica unsafe_change_config` as GATED in the Disposition column but
  recommends EXCLUDE for v1 -- transcribed faithfully as GATED with unsafe=true
  so a later milestone (M3/M5) can additionally hide unsafe ops via the flag
  rather than by overriding the table. node_local=true is set on every fs / wal
  / pbc / local_replica entry (reads and node-local mutations alike).
  How the tree is obtained in tests: `RootMode()` lives in `tool_main.cc`
  alongside `main()`, so it cannot be linked into a gtest binary. The test
  compiles the tool sources minus `tool_main.cc` and reconstructs the identical
  tree via a `BuildFullRootMode()` helper that mirrors `RootMode()` (kept in
  sync by calling the very same `Build*Mode()` factories). This exercises the
  invariant against the REAL production action set, not a hand-copied tree.
  Build: `ninja kudu` and `ninja mcp_disposition-test` both succeed.
  Test: `./bin/mcp_disposition-test` -> 10/10 PASSED, including
  `ValidateDispositionCoverage(BuildFullRootMode().get())` (the "every action
  classified exactly once" invariant) against the current tree.
  Handoff for M3 (tools/list filtering):
    - Include `kudu/tools/mcp_disposition.h`. For each candidate action, build
      the mode chain (root..parent) exactly as the reflection walk already does
      and call:
        `DispositionInfo DispositionFor(const std::vector<Mode*>& chain,
                                        const Action* action);`
      Check `info.classified` first (must be true for any tree the server
      built). Then filter: SURFACE -> always list; GATED -> list only when
      `--allow-writes`; REJECT / EXCLUDE -> never list. Tag GATED tools for the
      host confirm prompt, and annotate `info.node_local` SURFACE tools in the
      description. Consider hiding `info.unsafe` GATED tools even under
      `--allow-writes` (PRD v1 recommendation).
    - Call `void ValidateDispositionCoverageOrDie(const Mode* root)` once at
      serve startup (after building `RootMode()`) so a newly added but
      unclassified CLI action fails loudly and immediately. A `Status`-returning
      `ValidateDispositionCoverage(const Mode* root)` is available for tests.
    - Helpers: `std::string DispositionCommandPath(chain, action)` builds the
      display/key path; `const char* DispositionToString(Disposition)` for
      logging.

- 2026-09-09 -- M3 landed. `tools/list` now reflects the action tree into MCP
  tool objects, filtered by the M2 disposition table. The M1 empty-array
  placeholder is gone.
  Files changed:
    - `src/kudu/tools/tool_action_mcp.cc` -- added the shared exposure registry
      and the reflection-to-schema layer, all in the file's first anonymous
      namespace so both M3 (tools/list, now) and M4 (tools/call, next) use it:
        * `struct McpToolEntry { std::string tool_name; std::vector<Mode*> chain;
          const Action* action; DispositionInfo disposition; };`
        * `std::string McpToolName(const std::vector<Mode*>& chain,
          const Action* action)` -- THE tool-name scheme. Full command path
          (root dropped) with the path-separator spaces replaced by underscores:
          `{root,"table"}`+`describe` -> `table_describe`;
          `{root,"tserver","quiesce"}`+`status` -> `tserver_quiesce_status`. It
          is `DispositionCommandPath(chain, action)` with `' '`->`'_'`. NOTE: not
          reversible by splitting on `_` (mode/action names contain underscores),
          so M4 MUST resolve names via the registry, never by splitting.
        * `std::vector<McpToolEntry> BuildMcpToolRegistry(const Mode* root,
          bool allow_writes)` -- walks `root` once and returns one record per
          EXPOSED action. Exposed = SURFACE always; GATED iff `allow_writes`;
          REJECT/EXCLUDE/unclassified never. Entries borrow from `root`, which
          must outlive them. (Internal helper `CollectMcpTools(chain, mode,
          allow_writes, out)` does the recursion.)
        * `unique_ptr<Mode> BuildMcpRootMode()` -- mirrors `RootMode()`
          (`tool_main.cc`) via the same `Build*Mode()` factories, since
          `RootMode()` shares a TU with `main()` and cannot be linked into the
          test. Root name is irrelevant (dropped from paths/names).
      Schema mapping (mirrors `Action::BuildHelpXML`): required args ->
      `{"type":"string","description":...}` and listed in `required[]`; the
      variadic arg -> `{"type":"array","items":{"type":"string"},...}` and listed
      in `required[]`; optional flags -> typed property whose type comes from
      `google::GetCommandLineFlagInfoOrDie(name).type` via
      `JsonSchemaTypeForGflag()`: `bool`->`boolean`,
      `int32/int64/uint32/uint64`->`integer`, `double`->`number`, else->`string`.
      Each optional property also carries the flag `description` (action override
      wins) and a typed `default` (`WriteFlagDefault()` converts the gflag's
      string default to a JSON bool/number when it parses, else emits the raw
      string). Optional flags are never in `required[]`.
      Control-flag scrub: `IsReservedControlFlag()` drops `allow_writes` and any
      flag whose name contains `dry_run` from every schema, so the model cannot
      set the server-level gate/dry-run per call.
      Annotations: GATED -> `{"readOnlyHint":false,"destructiveHint":true}`;
      SURFACE/other -> `{"readOnlyHint":true}` (M5 does the real confirm/dry-run
      enforcement; M3 only tags). node_local SURFACE tools get a sentence
      appended to their description noting the result reflects only the local
      node.
      Startup invariant: `RunMcpServeLoop()` now builds the tree once, calls
      `ValidateDispositionCoverageOrDie(root)` before the read loop, and builds
      the registry with `FLAGS_allow_writes`; `tools/list` iterates it. Removed
      the resolved `TODO(mcp-M3)`; the `TODO(mcp-M4)` now points at the registry.
    - `src/kudu/tools/tool_action_mcp-test.cc` -- drives `tools/list` through the
      loop harness; toggles `FLAGS_allow_writes` (save/restore) via `RunToolsList`.
      Replaced the obsolete M1 `ToolsListReturnsEmptyArray` with M3 cases:
      SURFACE-only reflection + well-formed envelope; `table_describe` schema
      (required `table_name` string in `required[]`, optional bool
      `show_attributes` -> `boolean` and NOT required); `table_scan`
      `scan_batch_size` int32 -> `integer`; GATED `table_delete` appears only
      with `--allow-writes` and carries the not-read-only/destructive
      annotations; REJECT `cluster_rebalance` / EXCLUDE `pbc_edit` / `mcp_serve`
      absent in BOTH modes; `allow_writes` never leaks into any inputSchema.
    - `src/kudu/tools/CMakeLists.txt` -- `tool_action_mcp-test` now compiles
      `KUDU_CLI_TOOL_SRCS_NO_MAIN` and links `KUDU_CLI_TOOL_LINK_LIBS` (same
      pattern as `mcp_disposition-test`), because the loop now reflects the full
      action tree and thus references every `Build*Mode()` and the disposition
      table.
  Build: `ninja kudu tool_action_mcp-test mcp_disposition-test` succeeds.
  Test: `tool_action_mcp-test` -> 18/18 PASSED; `mcp_disposition-test` -> 10/10
    PASSED. Smoke: `tools/list` lists 50 SURFACE tools without `--allow-writes`
    and 92 with it (42 GATED appear tagged); `cluster_rebalance`, `pbc_edit`,
    `mcp_serve` never appear; no tool schema carries an `allow_writes` property.
  Handoff for M4 (tools/call):
    - Build the registry once (as the loop already does) and resolve the
      incoming `params.name` against it: `for (const auto& e : tools) if
      (e.tool_name == name) ...`. Do NOT reconstruct the action by splitting the
      name on `_`. `e.chain` (root..parent) and `e.action` are exactly what
      `Action::Run(chain, required_args, variadic_args)` wants. Note the registry
      is already gated: a GATED tool is absent from it when `!allow_writes`, so a
      name miss is the natural server-side rejection (defense in depth for M5).
    - Marshal `params.arguments` into `required_args` (map, from
      `e.action->args().required` by name) + `variadic_args` (from
      `args().variadic`); return `-32602` on a missing required arg (analog of
      `MarshalArgs`, `tool_main.cc:85`).
    - R2 (gflags save/restore): for each optional flag present in `arguments`
      (skip `IsReservedControlFlag`), `google::GetCommandLineOption` to save,
      `SetCommandLineOption` to set, run, restore in a scope guard (also on the
      error path). R3 (cout capture): swap `std::cout.rdbuf()` to an
      `ostringstream` around `e.action->Run(...)`, put the captured text in
      `result.content[].text`; map a non-OK `Status` to `isError=true` +
      `Status::ToString()`.

- 2026-09-09 -- M4 landed. `tools/call` now executes read-only actions and
  returns their captured text. Changes:
    - `src/kudu/tools/tool_action_mcp.cc` -- added the `tools/call` handler
      (`HandleToolsCall`), wired into the loop dispatch next to `tools/list`.
      Resolves `params.name` against the M3 registry by `entry.tool_name`
      (never by splitting on `_`); an unknown/absent name (GATED tools are
      absent when `!allow_writes`) returns JSON-RPC `-32602`.
      Connection-arg injection: added `DECLARE_string(master_addresses)` and
      registered it as an optional parameter on the `serve` action. A new
      `IsInjectedConnectionArg(name)` covers BOTH the plural
      `master_addresses` (cluster/table actions) and the singular
      `master_address` (master actions); when an action requires one and it is
      not supplied in `arguments`, the value is injected from
      `FLAGS_master_addresses` (`InjectedConnectionValue` takes the first
      comma-token for the singular form). An empty `FLAGS_master_addresses`
      yields a clear tool error (not a protocol error). `tserver_address` is
      NOT scrubbed -- it stays a model-supplied required arg. Schema generation
      (`WriteMcpToolObject`) now omits injected connection args from both the
      `properties` map and the `required[]` array of every inputSchema.
      Arg marshalling mirrors `MarshalArgs`: required args by name (missing ->
      `-32602` naming the arg), variadic from a JSON array, scalars accepted as
      string/number/bool (`JsonScalarToString`). R2: a `ScopedFlagSaver` RAII
      saves each touched optional flag (skipping reserved control flags and the
      injected `master_addresses`) via `GetCommandLineOption` and restores it in
      the destructor, even on the error path. R3: a `ScopedCoutRedirect` RAII
      swaps `std::cout.rdbuf()` to an `ostringstream` around `Run()` and
      restores it; captured text goes into `content:[{type:text,text}]`. A
      non-OK `Status` becomes a successful JSON-RPC result with `isError=true`
      carrying `Status::ToString()` plus any partial cout. glog stays on stderr.
      Removed the resolved `TODO(mcp-M4)`; left a `TODO(mcp-M5)` where write
      gating / dry-run will hook in.
    - `src/kudu/tools/tool_action_mcp-test.cc` -- 8 new no-cluster cases through
      `RunMcpServeLoop`: unknown-tool `-32602`; GATED tool rejected without
      `--allow-writes`; missing-required-arg `-32602`; `master_addresses` absent
      from every inputSchema; unconfigured `master_addresses` -> tool error;
      read against an unreachable cluster -> `isError=true`; R2 optional flags
      restored even on the error path (verified via `GetCommandLineOption`); R3
      action output stays in `content` with clean single-line framing.
    - `src/kudu/tools/tool_action_mcp-itest.cc` (new) -- `ExternalMiniCluster`
      happy-path via `ADD_KUDU_TEST`. Creates a table with `TestWorkload`, sets
      `FLAGS_master_addresses` to the cluster master, and drives `tools/call`
      in-process for `master_status` (injected singular `master_address`),
      `table_list` (injected plural, table name appears), and `table_describe`
      (model-supplied `table_name` + injected addresses); asserts captured
      content substrings with `isError=false`. A second case proves an explicit
      `master_addresses` argument overrides the injection. Note: running an
      `Action` in-process calls `kudu::ValidateFlags()`, and `KuduTest` defaults
      `--time_source=system_unsync`, so the fixture sets
      `--unlock_unsafe_flags=true` (restored by KuduTest's `FlagSaver`).
    - `src/kudu/tools/CMakeLists.txt` -- `ADD_KUDU_TEST(tool_action_mcp-itest)`
      compiling `KUDU_CLI_TOOL_SRCS_NO_MAIN` and linking `KUDU_CLI_TOOL_LINK_LIBS`
      (same pattern as `tool_action_mcp-test`; cluster/itest libs come from
      `SET_KUDU_TEST_LINK_LIBS`), plus a `kudu` build dependency.
  Rebalance planner: DEFERRED. `cluster rebalance` is REJECT in the disposition
    table (a blocking action) and is therefore absent from the registry that
    `tools/call` resolves against. Exposing a forced `report_only=true` variant
    would require weakening its REJECT disposition (forbidden) or synthetic
    forced-flag tool machinery outside the registry (out of M4 scope). The
    disposition table is left untouched; rebalance stays unreachable.
  Build: `ninja kudu tool_action_mcp-test mcp_disposition-test
    tool_action_mcp-itest` succeeds.
  Test: `tool_action_mcp-test` -> 26/26 PASSED; `mcp_disposition-test` -> 10/10
    PASSED; `tool_action_mcp-itest` -> 2/2 PASSED. Smoke: `tools/call` for a read
    returns its captured text in `content[].text`; stdout carries exactly one
    JSON response line per request (glog stays on stderr).
  Handoff for M5 (write gating): the `TODO(mcp-M5)` block above
    `RunMcpServeLoop` marks the hook. GATED tools already only enter the registry
    with `--allow-writes`, so name resolution is the first gate; M5 adds the MCP
    destructive/confirm annotations (partly present from M3) and the dry-run
    path -- when the dry-run arg is set, do NOT call `Run()`; echo the resolved
    command instead. The `ScopedFlagSaver`/`ScopedCoutRedirect` RAII and the
    arg-marshalling helpers in `tool_action_mcp.cc` are reusable as-is.

- 2026-09-09 -- M5 landed. The D4 write gate is now proven end to end: a GATED
  tool is hidden without `--allow-writes`, rejected server-side (not merely
  hidden) if called anyway, previewable via dry-run, and executes for real with
  the gate open.
    - `src/kudu/tools/tool_action_mcp.cc`:
        * Dry-run is a synthetic MCP-level control, NOT a gflag.
          `WriteMcpToolObject` now emits a boolean `dry_run` property on GATED
          tools ONLY (gated on `disposition == GATED`), described as "If true, do
          not execute; return the exact kudu command that would run, making no
          changes." It is deliberately absent from `required[]`. It cannot
          collide with a real flag: no action gflag contains the substring
          `dry_run` (the only lookalike is `hms fix --dryrun`, no underscore),
          and `IsReservedControlFlag` already reserves the `dry_run` namespace,
          so the synthetic property is the sole occupant of that name.
        * `HandleToolsCall` detects `dry_run:true` AFTER resolving the entry and
          honors it only for GATED tools (for SURFACE it is an ignored unknown
          arg -- and never advertised in a SURFACE schema). On dry-run it still
          fully validates/marshals required + variadic args (so a missing/bad
          arg still returns `-32602`, never a partial mutation), collects the
          supplied optional flags (validating scalar-ness), then returns the
          reconstructed command via `BuildToolResultResponse(..., is_error=false)`
          WITHOUT calling `Run()` and WITHOUT touching any gflag (the
          `ScopedFlagSaver` block is skipped entirely on this path). The
          `dry_run` key is a control key: consumed here, excluded from flag
          marshalling exactly like the reserved/injected args.
        * Command reconstruction: new helpers `BuildDryRunCommand()` and
          `ShellQuote()`. Format is
          `kudu <chain-names...> <action-name> <positionals-in-declared-order> [--opt=value ...]`.
          Positionals follow `args().required` declaration order (so the injected
          `master_addresses`/`master_address` value appears at its real slot --
          it is part of the command), then variadic values, then supplied
          optional flags in declaration order. `ShellQuote` leaves shell-literal
          tokens bare and single-quote-wraps anything else (escaping embedded
          single quotes as `'\''`), so a JSON-array bound like `[0]` renders as
          `'[0]'`. Example (master addresses `master-1:7051`, table `my_range_tbl`,
          bounds `[0]`/`[100]`):
          `kudu table add_range_partition master-1:7051 my_range_tbl '[0]' '[100]'`.
        * Clearer server-side gating rejection: `RunMcpServeLoop` now also
          computes the FULL (ungated) set of GATED tool names once (a second
          `BuildMcpToolRegistry(root, /*allow_writes=*/true)` walk) and threads it
          into `HandleToolsCall`. When a name misses the active (write-gated)
          registry, a name that IS a known GATED tool now returns a DISTINCT
          `-32602` naming `--allow-writes` ("tool '...' is a gated (mutating) tool
          and is not enabled; start the server with --allow-writes to use it"); a
          genuinely unknown name still returns the "unknown tool" `-32602`. Both
          stay single-threaded and allocation-light (one `unordered_set<string>`
          built at startup).
        * The confirm annotation (`readOnlyHint:false, destructiveHint:true`) was
          already emitted for GATED tools in M3; verified and kept (MCP has no
          separate "confirm" field -- destructiveHint is the host confirm
          signal). Not over-engineered with `idempotentHint`.
        * Replaced the `TODO(mcp-M5)` block above `RunMcpServeLoop` with a
          description of the now-implemented gating/dry-run behavior.
      First gated write: `table add_range_partition` (its MCP name is
      `table_add_range_partition`), a normal non-blocking cluster mutation.
      Fallback to `tablet leader_step_down` was NOT needed -- range-partition
      args worked end to end (required order: injected `master_addresses`,
      `table_name`, `table_range_lower_bound`, `table_range_upper_bound`; bounds
      are JSON-array strings like `[0]`).
    - `src/kudu/tools/tool_action_mcp-test.cc` -- 5 new no-cluster cases through
      `RunMcpServeLoop` (new `ScopedAllowWrites` RAII toggles `FLAGS_allow_writes`
      with save/restore): GATED schema carries a boolean `dry_run` not in
      `required[]` while a SURFACE schema has no `dry_run`; dry-run happy asserts
      the EXACT reconstructed command string (injected master addresses,
      positional order, `[0]`/`[100]` quoting); dry-run with a missing required
      arg still returns `-32602` naming it; without `--allow-writes` the gated
      tool is absent from `tools/list` and its call is rejected with a message
      naming `--allow-writes` (distinct from the "unknown tool" message a bogus
      name gets); a dry-run call leaves `lower_bound_type`'s gflag value
      unchanged (before/after `GetCommandLineOption`) while echoing it into the
      command.
    - `src/kudu/tools/tool_action_mcp-itest.cc` -- 2 new `ExternalMiniCluster`
      cases. A `CreateRangePartitionedTable` fixture helper builds a RANGE-only
      table on an int32 PK column `key` (via `KuduSchema` + `KuduTableCreator`,
      not TestWorkload's hash tables) with an initial range `[0, 100)`. Both set
      `FLAGS_allow_writes=true` and `FLAGS_master_addresses` (save/restore).
      Happy: a real `table_add_range_partition` of `[100, 200)` returns
      `isError=false`, and OBSERVATION is proven by a second identical add now
      FAILING (already present / overlap -> `isError=true`), which is only
      possible if the first add persisted. Dry-run: a `dry_run:true` add of
      `[200, 300)` returns the literal command with `isError=false`, and a
      subsequent REAL add of that same range SUCCEEDS -- proving the dry-run
      created nothing.
    - `CMakeLists.txt`: unchanged (all three test binaries were already wired).
  Build: `ninja kudu tool_action_mcp-test mcp_disposition-test
    tool_action_mcp-itest` succeeds.
  Test: `tool_action_mcp-test` -> 31/31 PASSED; `mcp_disposition-test` -> 10/10
    PASSED (coverage invariant intact -- no disposition weakened);
    `tool_action_mcp-itest` -> 4/4 PASSED. ASCII check clean on all touched
    files.
  Handoff for M6 (hardening): M5 drives the gate in-process via
    `RunMcpServeLoop`. M6 should add a subprocess-style integration test in the
    `kudu-tool-test.cc` / `kudu-admin-test.cc` idiom: spawn `kudu mcp serve
    --master_addresses=... [--allow-writes]` as a child, feed recorded JSON-RPC
    on stdin, and assert responses (list, a read call, a gated-call rejection
    both with and without `--allow-writes`, a dry-run echo, malformed input).
    Note the dry-run command reconstruction (`BuildDryRunCommand`/`ShellQuote`)
    and the gated-name set are already in place and reusable. Remaining M6 items
    are unchanged: R4 (exposed actions return `Status`, not `exit`/`CHECK`), R1
    (assert REJECT/EXCLUDE never reach dispatch), the `printf`/fd-1 scrub, and
    the three-copies-of-the-root-tree tech debt (`RootMode()` /
    `BuildFullRootMode()` / `BuildMcpRootMode()`).

- 2026-09-09 -- M6 landed. Hardening + full-stack tests: the server is now
  exercised as a real subprocess, the never-surface invariant is asserted
  exhaustively against the live action tree, the process-fatal (R4) and
  stdout-cleanliness (R3) audits are recorded, and the triplicated root-mode
  tree is de-duplicated behind one factory.
    - Root-tree de-duplication (M6 tech debt). New
      `std::unique_ptr<Mode> BuildRootMode(const std::string& name)` declared in
      `src/kudu/tools/tool_action.h` -- the single source of truth for the
      top-level mode list. `tool_main.cc:RootMode()`, the MCP server's
      `BuildMcpRootMode()`, and the disposition test's `BuildFullRootMode()` are
      now three-line delegators to it. Definition placement: it is defined in
      `tool_action_mcp.cc`, NOT `tool_action.cc` as the task first sketched.
      `tool_action.cc` is compiled into the low-level `kudu_tools_util` library;
      defining `BuildRootMode` there would inject undefined references to every
      `Build*Mode()` factory into all `kudu_tools_util` consumers that do not
      link the CLI factories (e.g. `ksck-test`), breaking their link.
      `tool_action_mcp.cc` already lives in `KUDU_CLI_TOOL_SRCS`
      (and `..._NO_MAIN`) and already references every factory, so it is the
      natural, link-safe home. Net behavior is unchanged; a newly added
      top-level mode is now wired in exactly one place.
    - `src/kudu/tools/tool_action_mcp-itest.cc` -- new `SubprocessServeEndToEnd`
      (P0-a headline). Spawns a REAL `kudu mcp serve --master_addresses=<addr>`
      as a `Subprocess` (stdin/stdout PIPEd via `ShareParentStdin(false)` /
      `ShareParentStdout(false)`, fds taken with `ReleaseChildStd*Fd`), writes
      six newline-delimited JSON-RPC lines with raw `write(2)`, closes stdin so
      the server sees EOF, drains stdout to EOF with `read(2)`, and asserts the
      process exits status 0. Cases in one session: (1) `initialize` handshake
      (protocolVersion / serverInfo `kudu-mcp` / capabilities.tools); (2)
      `tools/list` contains SURFACE `table_list` and, with the gate CLOSED, does
      NOT contain GATED `table_add_range_partition`; (3) `tools/call table_list`
      runs end to end against the cluster -> `isError:false` naming the created
      table (master addresses supplied ONLY via the serve flag, never in tool
      args); (4) a gated call -> `-32602` naming `--allow-writes`; (5) a
      malformed line -> `-32700`, and the FOLLOWING valid `tools/list` (id 6) is
      still answered -- proving the loop survives a parse error. The subprocess
      is a production `kudu` binary (not `KuduTest`), so its default
      `--time_source` does not trip the clock guardrail and no `--unlock_*`
      flags are needed at serve startup.
    - `src/kudu/tools/tool_action_mcp-test.cc` -- three new no-cluster cases
      (34 total, up from 31), plus shared helpers `VisitActions` (recursive tree
      walk) and `McpToolNameForTest` (recomputes the tool name from the exported
      `DispositionCommandPath`, spaces -> underscores, without widening
      `McpToolName`'s visibility):
        * `NoRejectOrExcludeActionIsEverExposed` (P0-b / R1): walks the REAL
          `BuildRootMode("kudu")` tree; for EVERY action whose `DispositionFor`
          is REJECT or EXCLUDE, asserts its MCP tool name is absent from
          `tools/list` in BOTH `--allow-writes` modes. Asserts it saw at least
          one such action (not vacuously green). This is the exhaustive form of
          the pre-existing three-name `RejectAndExcludeToolsNeverAppear`.
        * `ToolsCallRejectOrExcludeToolIsRefused` (P0-b dispatch side): a
          `tools/call` for `cluster_rebalance` (REJECT) and `pbc_edit` (EXCLUDE)
          returns `-32602` "unknown tool" in both write modes -- and, being
          never-surfaced (not merely gated), the message does NOT mention
          `--allow-writes`.
        * `NoSurfacedToolExposesUnsafeOrExperimentalFlag` (P1-a / R4,
          mechanized): walks every SURFACE/GATED action and asserts none of its
          optional parameters is tagged `unsafe` or `experimental` (via
          `GetFlagTags`). This is the guard that keeps model input off the one
          residual `ValidateFlags()`->`exit(1)` path (`CheckFlagsAllowed`).
    - R4 documentation (P1-a): a comment at the `entry->action->Run(...)` call
      site in `tool_action_mcp.cc` records that surfaced actions fail via
      `Status` (survival is proven by the subprocess gated-then-survive sequence
      and the in-process error-path tests), names the sole residual
      framework-level exit path (`kudu::ValidateFlags()` in `tool_action.cc`,
      reachable only if an unsafe/experimental flag is set without its `--unlock`
      or a custom validator fails), explains why it is not reachable from model
      input under a normal operator config, and states we deliberately do NOT
      wrap/fork `Run()` (that flag-validation framework is shared with the CLI).
    - R3 scrub (P1-b): clean. No surfaced C++ tool action writes to fd 1
      directly; all output goes through `std::cout`/ostream (e.g. the table
      scanner via `SetOutput(&cout)`), captured by the `ScopedCoutRedirect`
      `rdbuf` swap. The only `printf` in the tool tree is in `trace_io.stp`
      (SystemTap, not compiled). No code change needed.
    - `CMakeLists.txt`: unchanged (all three test binaries and the `kudu`
      dependency were already wired in M3/M4/M5).
  Build: `ninja kudu tool_action_mcp-test mcp_disposition-test
    tool_action_mcp-itest` succeeds, no warnings.
  Test: `tool_action_mcp-test` -> 34/34 PASSED; `mcp_disposition-test` -> 10/10
    PASSED (coverage invariant intact -- no disposition weakened);
    `tool_action_mcp-itest` -> 5/5 PASSED. ASCII check clean on all touched
    files.
  Handoff for M7 (docs + registration): the server is feature-complete and
    hardened for v1. M7 is documentation only, no code: (1) an operator doc with
    the `claude mcp add ... -- ssh -T -q ... kudu mcp serve` registration recipe
    (PRD section 3), the `--allow-writes` semantics (closed by default; opens
    GATED cluster mutations, each of which also carries dry-run + a destructive
    confirm hint), and the surfaced-vs-gated tool list; (2) a cross-link from the
    PRD status line. The tool inventory for that list can be generated from
    `tools/list` (SURFACE names with the gate closed; GATED names are the
    additional entries that appear with `--allow-writes`). No open risks: R1-R6
    are all handled and tested; the single residual `ValidateFlags()` exit path
    is documented and mechanically guarded, and is an operator-config concern
    (which `--unlock_*` flags the serve process itself is started with), not a
    model-input one.

- 2026-09-09 -- M7 landed. Documentation milestone complete; no code changes.
  Files added:
    - `docs/design-docs/kudu_mcp_operator_guide.md` -- the operator-facing
      reference doc. Verified against `tool_action_mcp.cc` and
      `mcp_disposition.cc` before writing. Sections:
        A. Overview: what the server is, who runs it, safety model summary.
        B. Registration recipe: local and SSH examples with exact flag names
           (`--master_addresses`, `--allow-writes`) verified from the code;
           rationale for -T -q and 1>&2 and exec in the SSH form.
        C. --allow-writes semantics: what appears/disappears, the exact
           rejection message from the code ("start the server with
           --allow-writes to use it"), and the read-only-by-default
           recommendation.
        D. dry_run semantics: synthetic MCP-level boolean on GATED tools only;
           full arg validation before returning command; no gflag mutation;
           still requires --allow-writes for tool visibility; concrete
           table_add_range_partition example with shell-quoted bounds.
        E. Tool disposition listing: SURFACE (50 tools, including 17 node-local
           tagged), GATED (42 tools, 2 unsafe-flagged), REJECT (8 blocking
           actions with reason), EXCLUDE (12 interactive/node-local-mutating
           with reason). Tool names derived from mcp_disposition.cc command
           paths (spaces to underscores). Totals match M3 progress log (50 + 42
           = 92).
        F. Protocol notes: JSON-RPC 2.0 over stdio, one object per line, methods
           handled, supported protocol versions, serverInfo name ("kudu-mcp"),
           error codes, EOF behavior, stdout discipline.
        G. Troubleshooting: 5 common issues (gated tool not listed, parse error
           -32700, cluster connection failure, unknown tool -32602, isError:true)
           all grounded in real code paths and messages.
  Files changed:
    - `docs/design-docs/kudu_mcp_prd.md` -- added "See also:" cross-link near
      the top pointing to the new operator guide.
    - `docs/design-docs/kudu_mcp_implementation_plan.md` -- M7 marked [x];
      this progress log entry appended.
  Discrepancies resolved: PRD section 6 uses simplified flat action names for
  several submoded actions (tserver quiescing vs quiesce, tablet
  add_replica vs change_config add_replica, master refresh vs authz_cache
  refresh, table disk_size/row_count vs set_limit disk_size/row_count, and the
  local_replica/fs dump submodes). The operator guide documents the actual tool
  names from mcp_disposition.cc with a note that names reflect the real command
  path. The PRD's "recommend EXCLUDE for v1" note on tablet_unsafe_replace_tablet
  and remote_replica_unsafe_change_config is reflected as GATED+unsafe in the
  listing (matching the implementation). The stale DEFINE_bool description
  ("not enforced yet") is not repeated in the guide; the guide documents actual
  enforced behavior.
  ASCII check: `grep -nP "[^\x00-\x7F]"` on all three modified/created files
  produced no output (confirmed clean).
