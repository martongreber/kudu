# kudu mcp serve -- Operator Guide

Companion to `kudu_mcp_prd.md` (design intent) and
`kudu_mcp_implementation_plan.md` (milestone tracker).

---

## A. Overview

`kudu mcp serve` runs a Model Context Protocol (MCP) server over stdio,
exposing Kudu's existing CLI admin actions as agent-callable tools. It is
launched by the operator -- typically on a bastion or jumpbox with access to
the cluster -- and registered once with an MCP host such as Claude Code. The
server inherits the launching user's Kerberos ticket and TLS configuration, so
Ranger authorization and audit apply end-to-end without additional credential
setup. The safety model: read-only diagnostic tools are surfaced by default;
cluster-mutating tools are hidden behind `--allow-writes` and each carries a
destructive-confirm hint; blocking, interactive, and node-local destructive
operations are never exposed.

---

## B. Registration Recipe

The operator provides `--master_addresses` once at registration. Tools that
require master addresses receive the value automatically at call time and never
ask the model to supply it.

**Local cluster (development or test):**

```sh
claude mcp add kudu-local -- \
  kudu mcp serve --master_addresses=localhost:7051
```

**Remote Kerberized cluster over SSH (read-only):**

```sh
# -T disables pseudo-tty allocation; -q suppresses ssh banners.
# Both keep stdout clean for the JSON-RPC protocol.
# kinit stdout is redirected to stderr (1>&2) so it cannot corrupt the channel.
# exec replaces the shell with kudu, keeping the process tree clean.
claude mcp add kudu-qa -- ssh -T -q qa-bastion \
  "kinit -kt /home/you/you.keytab you@QA.EXAMPLE.COM 1>&2 \
   && exec kudu mcp serve --master_addresses=qa-m1,qa-m2,qa-m3"
```

**Remote cluster with write access:**

```sh
claude mcp add kudu-qa-rw -- ssh -T -q qa-bastion \
  "kinit -kt /home/you/you.keytab you@QA.EXAMPLE.COM 1>&2 \
   && exec kudu mcp serve \
       --master_addresses=qa-m1,qa-m2,qa-m3 \
       --allow-writes"
```

Adding `--allow-writes` is a server-launch decision, not a per-call setting.
Register two separate entries (one read-only, one with writes) rather than
toggling the flag mid-session.

---

## C. --allow-writes Semantics

By default the server is read-only: only SURFACE tools appear in `tools/list`
(50 tools; see section E). Starting with `--allow-writes` adds the full set of
GATED (mutating) tools (42 additional tools; 92 total).

Without `--allow-writes`, calling a GATED tool by name is rejected
server-side even if the caller somehow obtained the name from another source:

```
error -32602: Invalid params: tool 'table_add_range_partition' is a gated
(mutating) tool and is not enabled; start the server with --allow-writes to
use it
```

This is defense in depth: the tool is both absent from `tools/list` AND
refused at dispatch. The rejection message explicitly distinguishes "the gate
is closed" from "no such tool" (which returns "unknown tool '<name>'" instead).

Each GATED tool also carries `readOnlyHint:false` and `destructiveHint:true`
in its MCP annotations so a well-behaved host presents a confirm prompt before
executing.

**Recommendation:** register two separate MCP server entries -- one without
`--allow-writes` for routine diagnosis and one with it for deliberate
remediation workflows. Do not set `--allow-writes` on servers used for
automated or unattended queries.

---

## D. dry_run Semantics

Every GATED tool exposes a boolean input property named `dry_run` (default
false, not required). When the caller sets `dry_run: true`:

- All required arguments are still validated; a missing or malformed required
  argument returns -32602 as normal (no partial mutations on a bad dry run).
- The server reconstructs the exact `kudu ...` command that would execute and
  returns it as the tool result text.
- It does NOT call the underlying action and makes zero cluster changes.
- No process-global gflags are modified; the flag save/restore scope guard is
  skipped entirely.

`dry_run` is a synthetic MCP-level property, not a gflag. It appears only in
the input schema of GATED tools. SURFACE tools do not advertise it and ignore
it if a caller sends it. The server must still be started with `--allow-writes`
for a GATED tool to be visible; `dry_run` does not bypass the write gate.

**Example -- table_add_range_partition with dry_run:true**

Request arguments:
```json
{
  "name": "table_add_range_partition",
  "arguments": {
    "table_name": "events",
    "table_range_lower_bound": "[1000]",
    "table_range_upper_bound": "[2000]",
    "dry_run": true
  }
}
```

Expected result content (cluster unchanged, addresses injected from
`--master_addresses=qa-m1,qa-m2,qa-m3`):
```
kudu table add_range_partition qa-m1,qa-m2,qa-m3 events '[1000]' '[2000]'
```

Arguments containing shell-special characters (such as the JSON-array bounds
`[1000]` and `[2000]`) are single-quote-wrapped in the reconstruction so the
output is copy-pasteable into a shell without reinterpretation. To actually
execute, resubmit the same arguments with `dry_run` omitted or set to false.

---

## E. Tool Disposition Listing

The curated disposition table in `src/kudu/tools/mcp_disposition.cc` classifies
every CLI action into one of four dispositions (SURFACE / GATED / REJECT /
EXCLUDE). A startup coverage invariant (`ValidateDispositionCoverageOrDie`)
verifies at server launch that every reachable CLI action has exactly one table
entry; a newly added action that lacks an entry causes the server to crash
immediately with the offending path, rather than silently exposing an
unclassified operation.

Tool names are derived from the action's full command path relative to the root
(with spaces replaced by underscores). Because mode and action names can
themselves contain underscores, names are not reversibly split; the server
resolves a name back to an action via the registry, never by string-splitting.

Note: a few tool names differ from the simplified names in PRD section 6
because the actual CLI tree places some actions under submodes (for example
`tablet change_config add_replica` rather than `tablet add_replica`). The names
below are the authoritative names as they appear in `tools/list`.

### SURFACE -- 50 tools, always available, read-only

**Cluster:**
- `cluster_ksck` -- core health diagnosis

**Diagnose:**
- `diagnose_parse_metrics`
- `diagnose_parse_stacks`
- `diagnose_tls_debug`

**HMS (Hive Metastore):**
- `hms_check`
- `hms_list`
- `hms_precheck`

**Master:**
- `master_dump_memtrackers`
- `master_get_flags`
- `master_list`
- `master_status`
- `master_timestamp`

**Perf:**
- `perf_table_scan`
- `perf_tablet_scan`

**Remote replica:**
- `remote_replica_check`
- `remote_replica_dump`
- `remote_replica_list`

**Table:**
- `table_describe`
- `table_get_extra_configs`
- `table_list`
- `table_list_in_flight`
- `table_locate_row`
- `table_scan`
- `table_statistics`

**Tablet:**
- `tablet_info`

**Transaction:**
- `txn_list`
- `txn_show`

**TServer:**
- `tserver_dump_memtrackers`
- `tserver_get_flags`
- `tserver_list`
- `tserver_quiesce_status`
- `tserver_status`
- `tserver_timestamp`

**Node-local SURFACE -- 17 tools (descriptions note the result reflects only
the local node this server runs on):**
- `fs_check`
- `fs_dump_block`
- `fs_dump_cfile`
- `fs_dump_tree`
- `fs_dump_uuid`
- `fs_list`
- `fs_locate_block`
- `local_replica_cmeta_print_replica_uuids`
- `local_replica_data_size`
- `local_replica_dump_block_ids`
- `local_replica_dump_data_dirs`
- `local_replica_dump_meta`
- `local_replica_dump_rowset`
- `local_replica_dump_wals`
- `local_replica_list`
- `pbc_dump`
- `wal_dump`

### GATED -- 42 tools, visible only with --allow-writes, mutating

Each GATED tool carries `readOnlyHint:false`, `destructiveHint:true` in its MCP
annotations, and a `dry_run` boolean property in its input schema (see
section D). Two entries (`remote_replica_unsafe_change_config`,
`tablet_unsafe_replace_tablet`) are marked unsafe in the disposition table.

**HMS:**
- `hms_downgrade`
- `hms_fix`

**Master:**
- `master_add`
- `master_authz_cache_refresh`
- `master_remove`
- `master_set_flag`
- `master_set_flag_for_all`

**Remote replica:**
- `remote_replica_delete`
- `remote_replica_unsafe_change_config` (unsafe)

**Table:**
- `table_add_column`
- `table_add_range_partition`
- `table_clear_comment`
- `table_column_remove_default`
- `table_column_set_block_size`
- `table_column_set_comment`
- `table_column_set_compression`
- `table_column_set_default`
- `table_column_set_encoding`
- `table_create`
- `table_delete`
- `table_delete_column`
- `table_drop_range_partition`
- `table_recall`
- `table_rename_column`
- `table_rename_table`
- `table_set_comment`
- `table_set_extra_config`
- `table_set_limit_disk_size`
- `table_set_limit_row_count`
- `table_set_replication_factor`

**Tablet:**
- `tablet_change_config_add_replica`
- `tablet_change_config_change_replica_type`
- `tablet_change_config_remove_replica`
- `tablet_leader_step_down`
- `tablet_unsafe_replace_tablet` (unsafe)

**TServer:**
- `tserver_quiesce_start`
- `tserver_quiesce_stop`
- `tserver_set_flag`
- `tserver_set_flag_for_all`
- `tserver_state_enter_maintenance`
- `tserver_state_exit_maintenance`
- `tserver_unregister`

### REJECT -- 8 actions, never surfaced, blocking

These actions block the single-threaded serve loop or start long-lived
processes. They are absent from `tools/list` in both write modes and return
"unknown tool" if called directly.

- `cluster_rebalance` -- long-running admin op; async candidate (PRD section 9)
- `master_run`, `tserver_run`, `mcp_serve` -- start daemon processes; not tools
- `perf_loadgen` -- write load generator
- `remote_replica_copy` -- copies replica data; async candidate
- `table_copy` -- copies table rows; async candidate
- `tablet_change_config_move_replica` -- waits for copy completion; async candidate

### EXCLUDE -- 12 actions, never surfaced

These actions are interactive (require stdin or an editor), or are node-local
mutations that require on-disk access (often with the tserver stopped), or are
flagged unsafe for cluster-wide use from the MCP context.

- `pbc_edit` -- interactive; opens an editor
- `fs_format`, `fs_update_dirs`, `fs_upgrade_encryption_key` -- node-local mutating
- `local_replica_copy_from_local`, `local_replica_copy_from_remote`,
  `local_replica_delete`, `local_replica_cmeta_rewrite_raft_config`,
  `local_replica_cmeta_set_term`, `local_replica_cmeta_unsafe_recreate`,
  `local_replica_tmeta_delete_rowsets` -- node-local; several require tserver stopped
- `master_unsafe_rebuild` -- node-local; unsafe

REJECT and EXCLUDE actions return the same "unknown tool" error in both write
modes; the error message does not reveal that `--allow-writes` would help,
because these tools are permanently unreachable regardless of the flag.

---

## F. Protocol Notes for Integrators

The server implements JSON-RPC 2.0 over stdio. Each request and each response
is a single JSON object on a single line (newline-terminated, one object per
line). Batch requests (JSON arrays) are not supported and return a null-id
invalid request error.

### Handled methods

| Method                  | Behavior                                              |
|-------------------------|-------------------------------------------------------|
| `initialize`            | Returns protocolVersion, capabilities, serverInfo     |
| `tools/list`            | Returns the active tool set (gated by --allow-writes) |
| `tools/call`            | Executes one tool; see sections C and D               |
| `notifications/*`       | One-way; no response                                  |
| anything else           | Returns -32601 method not found                       |

The `initialize` response carries:
- `protocolVersion`: the client's requested version if supported (supported set:
  `2024-11-05`, `2025-03-26`, `2025-06-18`); otherwise the server default
  `2025-06-18`.
- `capabilities`: `{"tools": {}}` (tools only; no resources in v1).
- `serverInfo`: `{"name": "kudu-mcp", "version": "<Kudu build version>"}`.

### Error codes (JSON-RPC 2.0 standard)

| Code    | Meaning          | When produced                                          |
|---------|------------------|--------------------------------------------------------|
| -32700  | Parse error      | Input line is not valid JSON; id is null in response   |
| -32600  | Invalid request  | Valid JSON but not an object, or missing method field  |
| -32601  | Method not found | Unknown method name                                    |
| -32602  | Invalid params   | Bad tool call parameters; see error message for detail |

A tool-execution failure (the underlying action returns a non-OK Status) is
returned as a successful JSON-RPC result with `isError: true` and the Status
description in `content[0].text`, not as a JSON-RPC error.

### EOF behavior

When stdin closes the `getline` loop exits cleanly and the process exits with
status 0. No special shutdown request is needed; SSH session teardown or pipe
close is sufficient.

### stdout discipline

All JSON-RPC responses go to the file descriptor held before the loop starts.
During each `tools/call`, action output is captured by redirecting
`std::cout`'s streambuf to an internal buffer around the `Run()` call and
returned in the tool result `content` array. It never appears directly on the
protocol channel. glog diagnostic output goes to stderr and never corrupts the
channel.

---

## G. Troubleshooting

### Gated tool not listed

A GATED (mutating) tool does not appear in `tools/list` unless `--allow-writes`
was set when the server was started. Calling the tool by name without the flag
returns:

```
-32602: Invalid params: tool '<name>' is a gated (mutating) tool and is not
enabled; start the server with --allow-writes to use it
```

Fix: restart the server (or register a separate MCP entry) with `--allow-writes`.

### Parse error -32700

Each JSON-RPC request must be a complete, valid JSON object on exactly one
line. A truncated line, an embedded newline, or non-JSON input returns:

```
-32700: Parse error
```

The server logs the offending line to stderr and continues; the next
well-formed line is answered normally. The serve loop does not exit on a parse
error.

### Cluster connection failure (isError: true)

A `tools/call` that cannot reach the cluster returns a successful JSON-RPC
result with `isError: true` and a Status message in `content[0].text`. The
server is not restarted. Common causes:

- `--master_addresses` was not provided at launch. The tool result will say
  "No master addresses are configured. Launch the server with
  'kudu mcp serve --master_addresses=<addr>[,<addr>...]'."
- Wrong addresses or unreachable hosts.
- Kerberos ticket expired (re-kinit and relaunch the server).

### Unknown tool name (-32602)

```
-32602: Invalid params: unknown tool '<name>'
```

This is returned for names that are not in the active registry, including REJECT
and EXCLUDE actions. Verify the name against `tools/list`. Tool names use
underscores (e.g. `table_describe`), not spaces. REJECT/EXCLUDE tools never
admit that `--allow-writes` would help; they are permanently unreachable.

### Tool call returns isError: true

The underlying CLI action returned a non-OK Status. The content text carries
the Status description. Typical causes: table or tablet not found, permission
denied, network error, invalid argument. Address the issue described in the
Status message and reissue the call.
