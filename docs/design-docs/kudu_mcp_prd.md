# kudu mcp serve -- PRD (Draft v0.1)

Status: Draft, starting instruction for implementation
v1 scope: read-only tools + one gated write

See also: [kudu_mcp_operator_guide.md](kudu_mcp_operator_guide.md) -- operator
registration recipe, --allow-writes and dry_run semantics, full tool listing,
protocol notes, and troubleshooting.

---

## 1. The key idea

Add a new mode to the `kudu` CLI binary -- `kudu mcp serve` -- that exposes
Kudu's existing admin actions as agent-callable MCP tools and resources. An
operator points Claude (or any MCP host) at a live cluster and, in plain
language, checks metrics, inspects partitions, diagnoses health, and remediates.

The wrapper itself is not the product. The value is the **reasoning across tool
calls** that a human does by hand today (run `ksck`, read it, check a tserver,
read that, decide) and the fact that MCP lets Kudu **compose** with other MCP
servers later (metrics stores, ticketing, chat). We build the capability layer
once; the intelligence lives in the model.

### Why baked into the CLI, not a standalone service

The MCP server is a Kudu *client*, not a cluster component. Living inside the
`kudu` binary means it inherits the client's entire Kerberos + authn-token + TLS
+ SPNEGO stack for free, ships in the box, always matches the cluster version,
and auto-tracks every new CLI action. We write zero auth code. (A Python server
that shells out to `kudu` could prototype the same tool surface, but this PRD
targets the baked-in C++ implementation.)

---

## 2. Decisions already locked

**D1 -- Bake into the `kudu` CLI (C++).**
Inherits auth, ships in-box, auto-tracks new commands.

**D2 -- stdio transport + the operator's own Kerberos ticket.**
No listening socket, no port, no new attack surface. Actions run as the real
user, so Ranger authz and audit stay intact end-to-end -- the AI cannot exceed
what the human could. HTTP transport + service keytab is reserved for a future
shared-team service (it breaks the identity chain, so not now).

**D3 -- Reflect the existing action registry into tools, but filtered.**
The CLI's `Mode`/`Action` tree already carries name, description, and typed
args; `--helpxml` proves the whole tree is serializable. We re-target that walk
to emit JSON Schema. Not blind reflection -- every action passes through a
disposition table (see section 6).

**D4 -- Read-only by default; writes are gated.**
Mutating tools are hidden unless `--allow-writes` is set, are marked so the host
shows its confirm prompt, and support a dry-run that echoes the exact command
before running it. Diagnosis flows freely; state changes stop for a human.

---

## 3. End-user experience

Register once. For a remote Kerberized cluster reachable only via a bastion,
SSH-tunneled stdio carries the MCP protocol and reuses access the operator
already has:

```
# one-time registration; ssh -T -q keeps stdout clean for the protocol
claude mcp add kudu-qa -- ssh -T -q qa-bastion \
  "kinit -kt /home/you/you.keytab you@QA.EXAMPLE.COM 1>&2 \
   && exec kudu mcp serve --master_addresses=qa-m1,qa-m2,qa-m3"
```

After that the operator just talks. Reads are free; a mutating call trips the
host confirm gate and shows the literal command:

```
you>    how's the qa cluster looking?
Claude> 3 of 240 tablets under-replicated, all on qa-t7; that box is
        94% full while others sit at 40-60%. qa-t7 holds 61% of the
        events table -- the 2026-08-15 range partition is 4x the others
        and landed entirely there. Rebalance is the safe first move.
```

No password typed, no cert paths, no keytab in the conversation -- the baked-in
server inherited the ticket. One registration, then natural language forever.

---

## 4. Implementation steps

All grounded in the current code under `src/kudu/tools/`. The framework
(`tool_action.h`, `tool_main.cc`) already gives us the tree, the metadata, and a
working dispatch path to copy.

**S1 -- JSON-RPC stdio loop.**
New `tool_action_mcp.cc` + `BuildMcpMode()` added to `RootMode()`
(`tool_main.cc:62`). Handle `initialize` / `tools/list` / `tools/call` over
newline-delimited JSON. JSON libs (`jsonwriter.h`, `jsonreader.h`, rapidjson)
are already in-tree.

**S2 -- tools/list via reflection.**
Port `Action::BuildHelpXML` (`tool_action.cc:308`) to emit MCP tool JSON.
Required/variadic args become string properties; optional flags become typed
properties -- the type is read from gflags (`gflag_info.type`), exactly as the
XML walk already does.

**S3 -- tools/call dispatch.**
Mirror `DispatchCommand` / `MarshalArgs` (`tool_main.cc:124`). Build the
`required_args` map from the JSON arguments object instead of argv, then call
the identical `action->Run()`. See R2 for the optional-args (gflags) handling.

**S4 -- Output capture.**
Actions print results to `cout` (e.g. `DataTable::PrintTo(cout)`,
`tool_action_common.h:282`). Redirect `cout`'s streambuf to a string around
`Run()`; write the protocol on a separately held stream. glog already goes to
stderr (`logtostderr` defaulted true, `tool_main.cc:261`), so logs never corrupt
the channel.

**S5 -- Disposition table + write gating.**
The one piece of genuinely new metadata: classify each action (surface / gated /
exclude) and enforce `--allow-writes` + confirm + dry-run. Detailed in section 6.

v1 declares only the `tools` capability -- no MCP resources. The read tools
already return every piece of state a resource would, so resources add surface
without adding capability. They earn their place only once we want to *push*
state changes (subscriptions); see Future work.

---

## 5. Technical guards, hardest to easiest

Ranked by real risk after reading the code. The scariest-sounding one turned out
to be a non-issue on inspection; the genuine engineering is narrow.

**R1 -- Blocking / never-returning actions (hardest).**
The loop is single-threaded. `perf loadgen` (`tool_action_perf.cc:1236`),
`local_replica copy` (`while(true)`, `pool->Wait()`,
`tool_action_local_replica.cc:396,473`), `pbc edit` (`tool_action_pbc.cc:274`),
and long `rebalance` runs would freeze it.
Solve: the disposition table excludes blocking/interactive actions; genuinely
useful long-runners (rebalance) run on a worker thread returning "started" +
progress. v1 simply excludes them.

**R2 -- gflags global state for optional params (real work).**
Confirmed at `tool_action.h:146` -- optional args are read as process-global
`FLAGS_foo`, not passed via the context.
Solve: `SetCommandLineOption` returns the prior value; save each declared flag,
set from JSON, run, restore. Serialize calls (the loop already does). Precedent
exists in `SetOptionalParameterDefaultValues()` (`tool_action.h:311`).

**R3 -- Keeping the protocol stdout clean (standard).**
`cout` is both the action output and the protocol channel.
Solve: redirect the `cout` streambuf during `Run()`; glog is already on stderr.
Residual: grep for any raw `printf` / fd-1 writes in exposed actions (the common
`DataTable` path uses `ostream`, so it is captured).

**R4 -- Process-fatal paths crash the server (low).**
A `CHECK`/`exit()` ends a long-lived server, not just one command.
Solve: the scary counts are mostly `DCHECK` (compiled out in release; the
cluster/table hits at cluster.cc:242,247,375 and table.cc:443,983 are all
DCHECK). Real fatals cluster in node-local `fs`/`test`/`pbc` -- excluded anyway.
Exposed admin actions return `Status` on error. Keep `serve` cheap to relaunch.

**R5 -- Repeated per-call init (non-issue).**
`Run()` calls `InitGoogleLoggingSafe()` + `ValidateFlags()` every time.
Verified safe: `InitGoogleLoggingSafe` is idempotent (`logging.cc:231`,
`if (logging_initialized) return;`); `ValidateFlags` (`flags.cc:570`) just
re-runs validators. Calling `Run()` in a loop is fine.

**R6 -- Reflection to JSON Schema (low).**
A mechanical port of the existing `BuildHelpXML` traversal; arg types come free
from gflags.

### The synthesis

R1, R4, and S6 all collapse into **one per-action disposition table** -- which
is why blind full reflection is unsafe and a curated classifier is mandatory,
not just nicer. That leaves only two genuinely mechanical problems in the
invoke-in-process path: **R2 (gflags save/restore)** and **R3 (cout capture)**.
Everything else is free, a port, or judgment.

---

## 6. Action policy and full disposition

This is the safety spec. Every action passes through it before it can be
surfaced. It is the authoritative record of what is expected -- regenerate it
from `kudu --helpxml` when actions are added.

### Classification policy

Each action carries three properties:

- **Access:** read-only, or mutating (changes cluster or on-disk state).
- **Locality:** cluster (acts over RPC on the whole cluster) or node-local
  (acts on the on-disk data of the single node the server runs on; several of
  these require that node's Kudu process to be stopped, so they are not usable
  against a live cluster).
- **Execution:** normal (runs and returns), blocking (runs a server or a
  long/unbounded operation and does not return promptly), or interactive
  (prompts on stdin / opens an editor).

Four rules decide disposition, applied in this precedence:

1. **Interactive -> EXCLUDE.** Cannot drive an editor/stdin over the protocol.
2. **Blocking -> REJECT.** Would freeze the single-threaded loop. Long-running
   mutations (rebalance, replica copy/move) are rejected in v1 and are the
   candidates for the future worker-thread wrapper (see R1).
3. **Read-only -> SURFACE.** Always exposed. Node-local read-only is surfaced
   but tagged, because it only means anything when the server runs on that node.
4. **Mutating -> GATED if cluster (non-node-local); EXCLUDE if node-local.**
   Gated = hidden unless `--allow-writes`, host confirm, dry-run. Node-local
   mutation falls outside rule 2 and is excluded.

Legend: SURFACE (read, free) | GATED (write, behind gate) | REJECT (blocking) |
EXCLUDE (interactive / node-local mutating / unsafe).

### cluster  (RPC, cluster-wide)

| Action      | Access   | Exec     | Disposition | Note                                  |
|-------------|----------|----------|-------------|---------------------------------------|
| `ksck`      | read     | normal   | SURFACE     | core health diagnosis                 |
| `rebalance` | mutating | blocking | REJECT      | flagship remediation; async candidate |

### diagnose  (offline log/metric/TLS analysis)

| Action         | Access | Exec   | Disposition | Note                        |
|----------------|--------|--------|-------------|-----------------------------|
| `parse_stacks` | read   | normal | SURFACE     | parse a stacks dump         |
| `parse_metrics`| read   | normal | SURFACE     | parse a metrics log         |
| `tls_debug`    | read   | normal | SURFACE     | probe a server's TLS        |

### table  (RPC, cluster-wide -- the workhorse mode)

| Action                    | Access   | Exec     | Disposition | Note                          |
|---------------------------|----------|----------|-------------|-------------------------------|
| `list`                    | read     | normal   | SURFACE     |                               |
| `describe`                | read     | normal   | SURFACE     | schema + partitioning         |
| `list_in_flight`          | read     | normal   | SURFACE     |                               |
| `locate_row`              | read     | normal   | SURFACE     |                               |
| `get_extra_configs`       | read     | normal   | SURFACE     |                               |
| `statistics`              | read     | normal   | SURFACE     |                               |
| `scan`                    | read     | normal   | SURFACE     | may be heavy on large tables  |
| `create`                  | mutating | normal   | GATED       | complex schema args           |
| `delete`                  | mutating | normal   | GATED       | destructive                   |
| `recall`                  | mutating | normal   | GATED       | undelete soft-deleted table   |
| `rename_table`            | mutating | normal   | GATED       |                               |
| `rename_column`           | mutating | normal   | GATED       |                               |
| `add_column`              | mutating | normal   | GATED       |                               |
| `delete_column`           | mutating | normal   | GATED       |                               |
| `add_range_partition`     | mutating | normal   | GATED       | the realistic "partition work"|
| `drop_range_partition`    | mutating | normal   | GATED       | the realistic "partition work"|
| `set_replication_factor`  | mutating | normal   | GATED       |                               |
| `set_extra_config`        | mutating | normal   | GATED       |                               |
| `disk_size`               | mutating | normal   | GATED       | set disk-size limit           |
| `row_count`               | mutating | normal   | GATED       | set row-count limit           |
| `set_comment`             | mutating | normal   | GATED       |                               |
| `clear_comment`           | mutating | normal   | GATED       |                               |
| `column_set_default`      | mutating | normal   | GATED       |                               |
| `column_remove_default`   | mutating | normal   | GATED       |                               |
| `column_set_compression`  | mutating | normal   | GATED       |                               |
| `column_set_encoding`     | mutating | normal   | GATED       |                               |
| `column_set_block_size`   | mutating | normal   | GATED       |                               |
| `column_set_comment`      | mutating | normal   | GATED       |                               |
| `copy`                    | mutating | blocking | REJECT      | copies rows; async candidate  |

Note: hash partition count is fixed at table creation -- live "partition work"
means range-partition management, not arbitrary re-sharding.

### tablet  (RPC, cluster-wide)

| Action                  | Access   | Exec     | Disposition | Note                              |
|-------------------------|----------|----------|-------------|-----------------------------------|
| `info`                  | read     | normal   | SURFACE     |                                   |
| `leader_step_down`      | mutating | normal   | GATED       |                                   |
| `add_replica`           | mutating | normal   | GATED       |                                   |
| `remove_replica`        | mutating | normal   | GATED       |                                   |
| `change_replica_type`   | mutating | normal   | GATED       |                                   |
| `move_replica`          | mutating | blocking | REJECT      | waits for copy; async candidate   |
| `unsafe_replace_tablet` | mutating | normal   | GATED       | unsafe -- recommend EXCLUDE in v1 |

### master  (RPC, cluster-wide; `run` is node-local)

| Action             | Access   | Exec     | Disposition | Note                               |
|--------------------|----------|----------|-------------|------------------------------------|
| `status`           | read     | normal   | SURFACE     |                                    |
| `timestamp`        | read     | normal   | SURFACE     |                                    |
| `list`             | read     | normal   | SURFACE     |                                    |
| `get_flags`        | read     | normal   | SURFACE     |                                    |
| `dump_memtrackers` | read     | normal   | SURFACE     |                                    |
| `set_flag`         | mutating | normal   | GATED       | runtime flag change                |
| `set_flag_for_all` | mutating | normal   | GATED       |                                    |
| `refresh`          | mutating | normal   | GATED       | refresh authz cache                |
| `add`              | mutating | normal   | GATED       | may be long-running (catalog copy) |
| `remove`           | mutating | normal   | GATED       |                                    |
| `unsafe_rebuild`   | mutating | normal   | EXCLUDE     | node-local + unsafe                |
| `run`              | mutating | blocking | REJECT      | starts a master process            |

### tserver  (RPC, cluster-wide; `run` is node-local)

| Action                 | Access   | Exec     | Disposition | Note                     |
|------------------------|----------|----------|-------------|--------------------------|
| `status`               | read     | normal   | SURFACE     |                          |
| `timestamp`            | read     | normal   | SURFACE     |                          |
| `list`                 | read     | normal   | SURFACE     |                          |
| `get_flags`            | read     | normal   | SURFACE     |                          |
| `dump_memtrackers`     | read     | normal   | SURFACE     |                          |
| `quiescing status`     | read     | normal   | SURFACE     |                          |
| `set_flag`             | mutating | normal   | GATED       | runtime flag change      |
| `set_flag_for_all`     | mutating | normal   | GATED       |                          |
| `quiescing start`      | mutating | normal   | GATED       |                          |
| `quiescing stop`       | mutating | normal   | GATED       |                          |
| `enter_maintenance`    | mutating | normal   | GATED       |                          |
| `exit_maintenance`     | mutating | normal   | GATED       |                          |
| `unregister`           | mutating | normal   | GATED       |                          |
| `run`                  | mutating | blocking | REJECT      | starts a tserver process |

### remote_replica  (RPC to a tserver)

| Action                | Access   | Exec     | Disposition | Note                              |
|-----------------------|----------|----------|-------------|-----------------------------------|
| `list`                | read     | normal   | SURFACE     |                                   |
| `check`               | read     | normal   | SURFACE     |                                   |
| `dump`                | read     | normal   | SURFACE     |                                   |
| `delete`              | mutating | normal   | GATED       |                                   |
| `unsafe_change_config`| mutating | normal   | GATED       | unsafe -- recommend EXCLUDE in v1 |
| `copy`                | mutating | blocking | REJECT      | copies replica; async candidate   |

### txn  (RPC, cluster-wide)

| Action | Access | Exec   | Disposition | Note |
|--------|--------|--------|-------------|------|
| `list` | read   | normal | SURFACE     |      |
| `show` | read   | normal | SURFACE     |      |

### hms  (Hive Metastore integration, cluster-level)

| Action     | Access   | Exec   | Disposition | Note                  |
|------------|----------|--------|-------------|-----------------------|
| `list`     | read     | normal | SURFACE     |                       |
| `check`    | read     | normal | SURFACE     |                       |
| `precheck` | read     | normal | SURFACE     |                       |
| `fix`      | mutating | normal | GATED       | reconciles HMS <-> Kudu|
| `downgrade`| mutating | normal | GATED       |                       |

### perf

| Action        | Access   | Exec     | Disposition | Note                          |
|---------------|----------|----------|-------------|-------------------------------|
| `table_scan`  | read     | normal   | SURFACE     | benchmark scan; may be heavy  |
| `tablet_scan` | read     | normal   | SURFACE     | benchmark scan; may be heavy  |
| `loadgen`     | mutating | blocking | REJECT      | writes load; runs a generator |

### fs  (NODE-LOCAL -- on-disk filesystem of the local node)

| Action                   | Access   | Exec   | Disposition        | Note                    |
|--------------------------|----------|--------|--------------------|-------------------------|
| `list`                   | read     | normal | SURFACE node-local |                         |
| `tree`                   | read     | normal | SURFACE node-local |                         |
| `uuid`                   | read     | normal | SURFACE node-local |                         |
| `cfile`                  | read     | normal | SURFACE node-local | dump a cfile            |
| `block`                  | read     | normal | SURFACE node-local | dump a block            |
| `locate_block`           | read     | normal | SURFACE node-local |                         |
| `check`                  | read     | normal | SURFACE node-local | may be heavy            |
| `format`                 | mutating | normal | EXCLUDE            | node-local; destructive |
| `update_dirs`            | mutating | normal | EXCLUDE            | node-local              |
| `upgrade_encryption_key` | mutating | normal | EXCLUDE            | node-local              |

### wal  (NODE-LOCAL)

| Action | Access | Exec   | Disposition        | Note              |
|--------|--------|--------|--------------------|-------------------|
| `dump` | read   | normal | SURFACE node-local | dump a WAL segment|

### pbc  (NODE-LOCAL -- protobuf container files)

| Action | Access   | Exec        | Disposition        | Note                 |
|--------|----------|-------------|--------------------|----------------------|
| `dump` | read     | normal      | SURFACE node-local |                      |
| `edit` | mutating | interactive | EXCLUDE            | opens an editor      |

### local_replica  (NODE-LOCAL -- usually requires the tserver STOPPED)

| Action              | Access   | Exec     | Disposition        | Note                     |
|---------------------|----------|----------|--------------------|--------------------------|
| `list`              | read     | normal   | SURFACE node-local |                          |
| `data_size`         | read     | normal   | SURFACE node-local |                          |
| `data_dirs`         | read     | normal   | SURFACE node-local |                          |
| `block_ids`         | read     | normal   | SURFACE node-local |                          |
| `meta`              | read     | normal   | SURFACE node-local |                          |
| `rowset`            | read     | normal   | SURFACE node-local |                          |
| `wals`              | read     | normal   | SURFACE node-local |                          |
| `print_replica_uuids`| read    | normal   | SURFACE node-local |                          |
| `delete`            | mutating | normal   | EXCLUDE            | node-local               |
| `delete_rowsets`    | mutating | normal   | EXCLUDE            | node-local               |
| `set_term`          | mutating | normal   | EXCLUDE            | node-local; unsafe raft  |
| `rewrite_raft_config`| mutating| normal   | EXCLUDE            | node-local; unsafe raft  |
| `unsafe_recreate`   | mutating | normal   | EXCLUDE            | node-local; unsafe       |
| `copy_from_remote`  | mutating | blocking | EXCLUDE            | node-local; copies data  |
| `copy_from_local`   | mutating | blocking | EXCLUDE            | node-local; copies data  |

### Rollup

- **SURFACE (read-only, free):** all of `cluster ksck`, `diagnose *`, `txn *`,
  the read actions of `table` / `tablet` / `master` / `tserver` /
  `remote_replica` / `hms` / `perf`, plus the node-local read actions of `fs` /
  `wal` / `pbc` / `local_replica` (tagged node-local).
- **GATED (cluster mutations, behind `--allow-writes` + confirm + dry-run):**
  the write actions of `table`, `tablet`, `master`, `tserver`,
  `remote_replica`, `hms`. Two `unsafe_*` config ops are gated but recommended
  EXCLUDE for v1.
- **REJECT (blocking):** two kinds. Async candidates -- `cluster rebalance`,
  `table copy`, `tablet move_replica`, `remote_replica copy` -- are long-running
  admin ops that a future async wrapper can support (see Future work). Never
  tools -- `master run`, `tserver run` (start a daemon), `perf loadgen` (a load
  generator) -- are not operations and stay out permanently.
- **EXCLUDE (interactive / node-local mutating):** `pbc edit`, all node-local
  mutations under `fs`, `local_replica`, and `master unsafe_rebuild`.

---

## 7. Scope

**v1**
- stdio transport, per-user ticket
- `tools` capability only (no resources)
- read-only tools: ksck, table list/describe, tablet layout, metrics, and
  `rebalance --report_only` as the read-only imbalance planner
- one gated write (add/drop range partition, or `leader_step_down`) with
  dry-run + confirm
- curated disposition table
- text output captured from `cout`

**Out of scope (for now -- see Future work)**
- blocking long-runners (rebalance execution, replica copy/move)
- MCP resources / subscriptions
- HTTP transport / shared-team service
- service-keytab identity model
- node-local (fs, wal, local_replica) actions
- autonomous (unconfirmed) mutation

---

## 8. Open decisions

- **Reflection scope:** curated ~15 hand-annotated tools vs. filtered full
  reflection. Recommendation: curate the core now, let reflection cover the long
  tail behind read-only.
- **Which write ships first:** rebalance (visual, safe, reversible) vs. add/drop
  range partition (shows "partition work" directly).

---

## 9. Future work

Everything below is deliberately out of v1. It is recorded here so the shape is
agreed before anyone starts, not discovered later.

### Async long-runners (solving R1 properly)

The single-threaded loop rejects blocking actions. The long-running *admin* ops
-- `cluster rebalance`, `tablet move_replica`, `remote_replica copy`,
`table copy` -- are worth supporting later via two patterns:

- **Decompose (preferred where it fits).** `rebalance --report_only` already
  returns the move plan without executing (`tool_action_cluster.cc:118,356`).
  Surface that read-only planner (in v1), then let the agent execute the plan as
  a sequence of discrete gated `tablet move_replica` steps, polling `ksck`
  between them. One long op becomes many quick gated calls; the model is the
  scheduler, which is exactly what an agent loop is good at.
- **Job handle (for ops that do not decompose, e.g. `table copy`).**
  `tools/call` spawns a worker thread running `Run()` and returns
  `{job_id, status: "running"}` immediately; add `<op>_status(job_id)` and
  `<op>_cancel(job_id)` tools, and optionally push `notifications/progress` on
  the original request via its `progressToken`.

Durability caveat: with stdio-over-SSH the server lives only as long as the
session, so a background job dies if the tunnel drops. This is acceptable
because Kudu rebalance and replica moves are restartable -- server-side copies
continue on their own and re-running resumes -- so a dropped session is a pause,
not corruption. `master run` / `tserver run` / `perf loadgen` are never
candidates for this; they are not operations.

### Subscribable resources

v1 is tools-only because the read tools already return every piece of state a
resource would. Resources earn their place only when we want to *push* a change
rather than have the model poll. The first candidate is `kudu://health`: declare
the `resources` capability, handle `resources/subscribe`, and run a background
poller (the same worker thread as above) that re-runs ksck and emits
`notifications/resources/updated` when the cluster summary changes. Scope
resources to state worth watching (health, per-table under-replication), not a
mirror of every read tool.

### Shared-team HTTP service

The stdio + per-user-ticket model deliberately keeps identity intact. A future
multi-user deployment could run `kudu mcp serve` as a long-lived HTTP
(streamable/SSE) service behind SPNEGO. This trades the clean identity chain for
a shared service keytab and needs its own authz/audit story, so it is a separate
project, not an increment on v1.
