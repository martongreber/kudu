# kudu mcp serve -- RPC Fan-Out for Per-TServer Info (Design, Draft v0.1)

Status: Draft, design for review

Companion to [kudu_mcp_prd.md](kudu_mcp_prd.md) (design intent),
[kudu_mcp_operator_guide.md](kudu_mcp_operator_guide.md) (registration and tool
listing), and [kudu_mcp_implementation_plan.md](kudu_mcp_implementation_plan.md)
(milestone tracker).

---

## 1. Context and scope

`kudu mcp serve` exposes the `kudu` CLI's admin actions as MCP tools by
reflecting the in-process `Mode`/`Action` tree. It runs single-threaded over
stdio, typically launched on a bastion, and issues RPCs to the cluster as the
operator's own Kerberos principal (see PRD sections 1-2). A `tools/call`
executes exactly one action in-process and returns its captured `cout` output.

The CLI's per-server introspection tools split into two families:

- **Cluster / RPC tools** talk to a running server over the network:
  `tserver get_flags`, `tserver dump_memtrackers`, `remote_replica list`,
  `tablet info`, and so on. Each takes a single `--tserver_address` (or one
  master set) and reports on that one endpoint.
- **Node-local tools** open the on-disk `FsManager` of whatever host the
  process runs on: `fs *`, `local_replica *`, `wal dump`, `pbc dump`. They
  report only that node's storage, and several require the local Kudu process
  to be stopped.

Two facts about the current state matter for this design:

1. **Node-local tools are effectively dead in the recommended deployment.**
   They reflect the filesystem of the host running `kudu mcp serve` -- a
   bastion with no Kudu data. Even co-located on a tserver they would see one
   node, and the read-only ones (`fs list`, `local_replica *`, `fs check`
   without `--repair`) take an *optional* directory lock: against a live
   server the `flock(LOCK_EX|LOCK_NB)` fails, the tool logs
   "Could not lock instance file ... Proceeding without lock"
   (`fs/dir_util.cc`, `fs/dir_manager.cc:LoadInstances`) and reads on-disk
   state that the server is concurrently mutating. `fs check --repair` and the
   mutating local tools take a *mandatory* lock and hard-fail while the server
   runs. So node-local tooling gives either no data (wrong host), one node's
   data, or racy data -- never a consistent fleet-wide view.

2. **The RPC per-server tools do not fan out.** They target one
   `--tserver_address`, which is not an injected connection arg
   (`IsInjectedConnectionArg` injects only `master_addresses`/`master_address`),
   so the model must discover and supply each address itself. "Give me per
   tserver X" therefore becomes an LLM-driven `tserver list` followed by a
   manual loop of single-server calls, with each server's full text streamed
   back through the model and no aggregation. The only existing fan-out is
   `tserver set_flag_for_all` / `master set_flag_for_all`, which iterate
   `ListTabletServers` in a sequential loop inside one action.

This document designs an **RPC-based fan-out**: a single tool that discovers all
(or a selected subset of) tablet servers via the master, queries each
concurrently over the wire, and returns an aggregated, machine-readable
per-server report -- online, with no SSH to the data nodes and no requirement
that any server be stopped.

SSH-per-node execution of the node-local tools is explicitly out of bounds: it
is the security posture the MCP design already rejected (PRD D2), it needs a
login on every data node, and for the read-only local tools it would still read
racy on-disk state. The mechanism here is Kudu RPC (and, as an optional
enrichment, the servers' HTTP metrics endpoint), reusing the client auth stack
`kudu mcp serve` already inherits.

---

## 2. Goals and non-goals

### Goals

- **G1 -- One call, whole fleet.** A single MCP tool call answers "per-tserver
  X across the cluster" without the model supplying addresses or looping. The
  master is the only endpoint the operator configures.
- **G2 -- Online and non-disruptive.** All data is read from running servers'
  served / in-memory state over RPC. Nothing requires a server to be stopped;
  nothing takes a directory lock; reads are consistent with what the server is
  actually serving.
- **G3 -- Parity with the *online-reachable* subset of node-local info.**
  Recover, over the wire, the per-node facts operators reach for today: what
  replicas a server hosts, their states, on-disk sizes and data-dir placement,
  Raft roles/health, flags, memory, quiescing, clock offset, version/uptime.
- **G4 -- Reuse the proven fan-out engine.** Build on ksck's remote-cluster
  machinery (shared `Messenger`, bounded fetch `ThreadPool`, per-server failure
  isolation, `PB -> JsonWriter` output) rather than inventing a new one.
- **G5 -- Structured, aggregable output.** Emit JSON: a per-server record array
  plus a cluster rollup (totals, skew, outliers), so both the model and CLI
  users get an analyzable result, not prose.
- **G6 -- Honest partial results.** A slow, unreachable, or authorization-denied
  server degrades to a per-server error entry; the rest of the report still
  returns.
- **G7 -- Reflect cleanly into MCP.** The result is a normal read-only
  (`SURFACE`) action that returns promptly, needs a disposition-table entry, and
  requires no change to the single-threaded serve loop.
- **G8 -- Topology-aware targeting, not just hard fan-out.** A caller can name an
  *entity* -- a table, a tablet, or a row's primary key -- and the tool resolves,
  via master metadata, the *minimal* set of servers that host it, then projects
  the result around that entity (rows-are-replicas), not around the server. A
  whole-cluster fan-out is then just the degenerate selector "the cluster". This
  makes "inspect tablet abcd" a first-class, ~3-server operation rather than a
  200-server scrape that is filtered afterward.

### Non-goals

- **N1 -- Raw on-disk storage internals.** Block bytes, CFile contents, rowset
  dumps, filesystem trees, and WAL entry contents (`fs dump block|cfile|tree`,
  `local_replica dump block_ids|rowset|wals`, `wal dump`) are not serialized by
  any server RPC. They stay node-local and offline. Fan-out does not attempt
  them.
- **N2 -- Block-level filesystem consistency checking.** `fs check`'s
  orphaned/missing-block analysis has no online equivalent; cluster-level
  replica health is already `cluster ksck`'s job.
- **N3 -- A new cluster-component RPC.** No new master- or tserver-side service
  is added; the master is not turned into an aggregation proxy (see
  Alternatives, section 8).
- **N4 -- Long-running / mutating fan-outs.** This is a read-only inventory.
  Fan-out of mutations (a general `--all` for writes) and of long data-plane
  operations reuse patterns already sketched in the PRD (Future work, section 9)
  and are out of v1 scope here.
- **N5 -- Historical / time-series data.** Fan-out returns point-in-time state.
  `diagnose parse_metrics|parse_stacks` operate on captured log files and remain
  their own thing.

---

## 3. The design

### 3.1 Overview

Three pieces, layered so each is independently testable and the top one reflects
into MCP for free:

1. **A target resolver** that turns "the cluster" (plus optional filters) into a
   concrete list of tablet-server endpoints, using the master's
   `ListTabletServers`.
2. **A reusable fan-out engine** -- the generalized form of what ksck already
   does internally: one shared `Messenger`, a `ThreadPool` bounded by
   `--fetch_info_concurrency`, one task per target, results collected under a
   lock, per-target failures isolated.
3. **A flagship read action**, `cluster gather`, that runs a selected set of
   probes across the resolved targets (fleet-wide or entity-scoped) and emits a
   JSON document projected around either the server or the named entity.

All three sit on the existing client RPC stack, so they inherit the operator's
Kerberos/TLS/token identity with zero new auth code -- the same property that
motivated baking the MCP server into the CLI in the first place.

### 3.2 System context

```
   Claude (MCP host)
        |
        |  stdio JSON-RPC : tools/call "cluster_gather"
        v
   +-----------------------------------------------+
   |  kudu mcp serve   (bastion; operator ticket)  |
   |                                               |
   |   tool dispatch -> action Run()               |
   |        |                                      |
   |        v                                      |
   |   target resolver ---- ListTabletServers ---------> master(s)
   |        |                                      |
   |        v                                      |
   |   fan-out engine                              |
   |   [ shared Messenger + fetch ThreadPool ]     |
   |     |    |    |          (bounded N)           |
   +-----|----|----|-------------------------------+
         |    |    |   per-target KRPC (parallel)
         v    v    v
      tserver tserver tserver ...        (running; no lock, no SSH)
       GetStatus / ListTablets / GetConsensusState /
       Quiesce / ServerClock / GetFlags / DumpMemTrackers
         |
         v
   aggregate -> KuduFanOutResultPB -> JsonWriter::ToJson -> tools/call result
```

The shape is deliberately the same as `cluster ksck`: connect to the master,
enumerate tservers, hit each one concurrently, aggregate, serialize. ksck proves
the pattern works at cluster scale and already fetches most of the data we want.

### 3.3 Target resolution: a selector, not just a filter

A fan-out is defined by a **selector** that the resolver turns, using master
metadata alone (before any per-server RPC), into two things: a concrete **target
set** of endpoints and an output **projection** (server-centric or
entity-centric). The hard fleet-wide fan-out is simply the `cluster` selector;
naming a finer entity narrows the target set to the minimum that can answer and
flips the projection around that entity. This is the core generalization -- the
engine below does not care which selector produced the targets.

Two resolution sources feed it:

- **Fleet enumeration** -- the master RPC `ListTabletServers(include_states=true)`
  (`master/master.proto`), exactly as ksck's
  `RemoteKsckCluster::RetrieveTabletServers` and the `*_set_flag_for_all` actions
  use. Each entry carries what we need to both address and describe a server
  without contacting it: `registration.rpc_addresses[]` / `http_addresses[]`,
  `software_version`, `start_time`, `https_enabled`;
  `instance_id.permanent_uuid`, `millis_since_heartbeat`, `location`, and
  `state` (`TServerStatePB`, e.g. maintenance mode).
- **Location lookup** -- the master tablet/table location metadata
  (`GetTableLocations`, and the tablet-by-id lookup that `tablet info` already
  performs) maps an entity to the exact `(tablet -> replica -> ts uuid, role)`
  set, without touching any tserver.

The selectors, coarse to fine:

- **`cluster`** (default) -- every registered tserver. Server-centric projection.
- **by uuid / host** -- an explicit server subset. Server-centric.
- **by location** -- a rack/location prefix (`location` field), for "the tservers
  in rack /dc1/rackA". Server-centric.
- **`table=T`** -- resolve T's tablets via `GetTableLocations`, take the union of
  their replica-holders. Answers "which nodes does table events live on" before
  fanning out. Projectable either server-centric or as a per-tablet table map.
- **`tablet=abcd`** -- resolve the tablet's ~3 replica-holders via the master
  lookup, contact only those, scope each probe to that one tablet.
  Entity-centric (replica-set) projection.
- **`row=[pk]`** -- resolve the primary key to its owning tablet via the table's
  partition schema (the same computation `table locate_row` does), then proceed
  as `tablet`. Entity-centric.

Entity selectors are the innovation this design adds over a plain fan-out; they
are detailed in section 3.3.1.

A server whose `millis_since_heartbeat` exceeds a threshold, or that is not
`registration`-complete, is still listed but flagged `presumed_dead` and skipped
for RPC (its record carries the last master-known facts and an
`unreachable`-class status), rather than spending a full `--timeout_ms` on it.

#### 3.3.1 Entity-scoped (topology-aware) fan-out

Naming a `tablet` or `row` selector produces a fundamentally different, and more
useful, report than a filtered whole-cluster scrape:

- **Minimal blast radius.** The resolver computes the ~3 replica-holders from
  master metadata and contacts only them. Debugging one tablet touches three
  servers, not the whole fleet -- cheap, fast, and safe to run repeatedly.
- **Per-probe scoping.** Each probe is narrowed to the entity: `ListTablets` is
  filtered to the tablet id (per-replica `estimated_on_disk_size`, `data_dirs[]`,
  `tablet_data_state`, `last_status`), and `GetConsensusState` is asked for that
  one tablet (term, leader_uuid, committed/pending peers, per-replica health).
  Both fields already exist per-replica in the RPCs from section 3.5 -- no new
  server code is required, only a scoped request and a different projection.
- **Replica-set projection (rows-are-replicas).** The output is organized around
  the entity: one row per replica (leader first), columns being the servers that
  hold it, so the *divergence* is what the reader sees -- which copy trails in
  term, which is size-skewed against its peers, which data-dir each copy landed
  on, which replica-holder is unreachable (a missing copy is visible precisely
  because the expected replica set is known up front). No existing single tool
  produces this cross-replica comparison; `tablet info` gives locations and
  `cluster ksck` gives a health verdict, but neither diffs the copies.
- **The natural ksck drill-down.** ksck flags "tablet abcd unhealthy";
  `cluster gather --tablet=abcd` says *how* the copies differ. The two compose:
  verdict, then explanation.

The `table` selector sits between the two: it can render server-centric (disk
skew across the nodes hosting the table) or as a per-tablet map (each tablet's
replica set), letting "inspect table events" answer either "which nodes carry it
and how loaded are they" or "which of its tablets are mis-replicated".

Authorization caveat: the per-replica consensus detail rides `GetConsensusState`,
an admin/superuser-tier RPC (section 3.9). Without that identity the entity view
still returns the client-accessible replica facts (existence, state, size via
`ListTablets`) and marks the consensus columns `UNAUTHORIZED` rather than failing.

### 3.4 The fan-out engine

The engine generalizes ksck's `Ksck::FetchInfoFromTabletServers`
(`tools/ksck.cc`) so it is not welded to ksck's health-summary output:

- **One shared `rpc::Messenger`**, built once via `BuildMessenger`
  (`tools/tool_action_common.cc`), honoring `--negotiation_timeout_ms` and the
  TLS/SASL settings the operator's environment already carries. Every per-target
  proxy is constructed against this one messenger and the resolved `Sockaddr`,
  as ksck's `RemoteKsckTabletServer::Init` does for its four proxies
  (`GenericServiceProxy`, `TabletServerServiceProxy`,
  `TabletServerAdminServiceProxy`, `ConsensusServiceProxy`).
- **A `ThreadPool` bounded by `--fetch_info_concurrency`** (default 20, reused
  verbatim from ksck). One task per target submits the selected probes,
  synchronously within the task, under the per-RPC `--timeout_ms` (default 60s).
- **Per-target isolation.** A task writes exactly one per-server record. A
  failed probe sets that record's status and, for probes that partially
  succeed, fills the fields it could reach and marks the rest. Failures never
  abort the pool; the driver calls `pool->Wait()` and then assembles the report.
  This mirrors ksck's atomic bad-server counter and post-`Wait` reporting.

Because the engine bounds both concurrency (N in flight) and per-RPC time, the
whole action returns in roughly `ceil(num_targets / N) * slowest_probe`
bounded by the timeout -- a *normal*, promptly-returning action for the
single-threaded MCP loop, not a blocking one (PRD R1). On very large clusters
the operator can raise `--fetch_info_concurrency` or narrow the target filter.

### 3.5 Per-server data sources (what the probes fetch)

Each probe is an RPC that a *running* server answers from served state. The set
below is exactly what ksck already fetches per tserver, plus the two generic
introspection RPCs, minus nothing we cannot reach online:

| Probe | RPC (service) | Per-server data |
|-------|---------------|-----------------|
| identity/version | `GetStatus` (Generic) | uuid, version_string, git_hash, build info, bound rpc/http addrs |
| replica inventory | `ListTablets` (TabletServerService) | per-tablet `TabletStatusPB`: tablet_id, table_name/id, state, tablet_data_state, last_status, partition, `estimated_on_disk_size`, `data_dirs[]`; +schema/partition_schema/role when `need_schema_info` |
| consensus | `GetConsensusState` (Consensus) | per-tablet term, leader_uuid, committed/pending config peers, raft_role, replica health |
| quiescing | `Quiesce(return_stats=true)` (TabletServerAdmin) | is_quiescing, num_active_scanners, num_leaders (read-only; does not change state) |
| clock | `ServerClock` (Generic) | HybridTime, for clock-offset/skew |
| flags | `GetFlags` (Generic) | non-default flags, tags; supports divergence detection |
| memory | `DumpMemTrackers` (Generic) | MemTracker tree: limit, current/peak consumption per subsystem |

The action exposes probe selection (e.g. `--sections=inventory,consensus,flags`)
so a caller can fetch a cheap inventory without paying for the heavier consensus
or memtracker calls, analogous to ksck's `PrintSections` bitmask.

Optional HTTP enrichment (section 3.8) adds the few gauges RPC does not carry:
`on_disk_data_size`, `memrowset_size`, `num_rowsets_on_disk`,
`last_read/write_elapsed_time`, and the server-level `tablets_num_running /
_failed / _bootstrapping` counters, from each server's `/metrics` JSON.

### 3.6 Node-local to online parity map

This is the heart of the "parity" question: for each thing the node-local tools
give, what does an online fan-out recover, and at what fidelity.

Legend: **FULL** = equivalent online source exists; **PARTIAL** = summary/estimate
online but not the full on-disk detail; **NONE** = no online source (offline-only,
Non-goal N1/N2).

| Node-local tool | Online source (RPC unless noted) | Parity |
|-----------------|-----------------------------------|--------|
| `fs dump uuid` | `GetStatus.permanent_uuid` / `ListTabletServers` | FULL |
| `local_replica list` | `ListTablets` -> tablet_id, state, data_state per replica | FULL |
| `local_replica dump data_dirs` | `TabletStatusPB.data_dirs[]` | FULL |
| `local_replica cmeta print_replica_uuids` | `GetConsensusState.committed_config.peers[]` (or master `GetTableLocations` / `tablet info`) | FULL |
| `local_replica data_size` | `TabletStatusPB.estimated_on_disk_size` + `/metrics` `on_disk_size`, `on_disk_data_size`, `memrowset_size` | PARTIAL -- totals/estimate, not the per-column/redo/undo/bloom breakdown |
| `local_replica dump meta` | `ListTablets(need_schema_info=true)` -> schema, partition; state | PARTIAL -- no rowset list (only `num_rowsets_on_disk` via metrics) |
| `pbc dump` (cmeta / tablet-meta / instance) | consensus RPC / `ListTablets` / `GetStatus` respectively | PARTIAL -- decoded fields, not raw PB container |
| `diagnose parse_metrics` | `/metrics` (HTTP), point-in-time | PARTIAL -- live snapshot, no history |
| `diagnose parse_stacks` | `/stacks`, `/threadz` (HTTP) | PARTIAL -- different format |
| `perf tablet_scan` (local) | `perf table_scan` / checksum scan (RPC) | PARTIAL -- table/replica-scoped, online |
| `fs list` (tablet/rowset/block/cfile meta) | `ListTablets` for tablets only | PARTIAL -- rowset/block/cfile metadata: NONE |
| `fs dump block` / `dump cfile` / `dump tree` | -- | NONE |
| `local_replica dump block_ids` / `dump rowset` / `dump wals` | -- | NONE |
| `wal dump` | -- | NONE (only WAL size via `/log-anchors`, metrics) |
| `fs check` | -- (replica health via `cluster ksck`) | NONE at block level |

Beyond one-to-one parity, fan-out surfaces per-server facts that have *no*
node-local tool at all but are exactly what "per tserver info" usually means in
practice: version/uptime and heartbeat age, maintenance/quiescing state, flag
divergence, memory pressure, leader counts, and scanner/RPC activity (the last
via `/metrics` and `/scans`). Fan-out is thus a superset of the online-reachable
node-local data, not merely a reimplementation.

### 3.7 Output: state, shape, and the tool result

Fan-out introduces **no persistent state** -- no sys-catalog column, no on-disk
structure, no WAL entries. It reads live server state and produces an in-memory
result that is serialized once and returned. The result is transient: it lives
for the duration of one `tools/call`.

The result is a protobuf (`KuduFanOutResultPB`, working name) rendered to JSON
via `JsonWriter::ToJson(pb, mode)` -- the same PB-then-JSON path ksck uses
(`ksck_results.cc:PrintJsonTo`). Rough shape (fields relevant to the design, not
a schema dump):

- `cluster`: master addresses, cluster_id, timestamp of the report, target
  filter that was applied, `--fetch_info_concurrency` and `--timeout_ms` in
  effect.
- `servers[]`: one record per target -- `uuid`, `host`, `location`, `version`,
  `start_time`, `millis_since_heartbeat`, `state` (maintenance etc.),
  `probe_status` (per probe: OK / TIMED_OUT / UNAUTHORIZED / UNREACHABLE), and
  the fetched sections (replica inventory, consensus, quiescing, flags, memory).
- `rollup`: cluster totals and skew -- replica-count and on-disk-size
  distribution across servers (min/max/mean/stddev, top-N heaviest), leader
  imbalance, version spread, flag-divergence groups, count of servers by
  probe_status. This is where the analytical value is; it is what a human would
  compute by hand after looping.

For MCP the action prints this JSON to `cout`, which the serve loop captures and
returns as the tool result text -- identical to every other reflected read tool,
so no serve-loop change is needed. The action needs one `SURFACE` entry in
`mcp_disposition.cc` (read-only, cluster, normal execution); the startup
`ValidateDispositionCoverageOrDie` invariant otherwise crashes on an
unclassified action.

### 3.8 Optional: HTTP metrics as a complementary source

The RPC set covers inventory, consensus, quiescing, flags, memory, and clock.
A handful of per-tablet gauges (`on_disk_data_size`, `memrowset_size`,
`num_rowsets_on_disk`, last read/write times) and rich server-level activity
live only in the metrics system, reachable at each server's `/metrics` JSON
endpoint (filterable by `?types=`, `?ids=`, `?metrics=`, `?compact=1`).

This is a genuinely different transport (HTTP on the web port, not KRPC on the
RPC port) with a different auth story: endpoints are open unless
`--webserver_require_spnego=true`, in which case the fan-out client must perform
SPNEGO. It is therefore proposed as an *optional enrichment section*, off by
default, so the core RPC fan-out has no dependency on web ports being reachable
or on an HTTP-auth code path. When enabled, the same fan-out engine drives it
(one bounded task per server's `http_addresses[0]`), and a per-server HTTP
failure degrades that server's metric fields exactly like an RPC failure.

### 3.9 Authorization: the real constraint, and graceful degradation

Because `kudu mcp serve` acts as the operator's own principal (PRD D2), the
report's completeness depends on that principal's role. The probe RPCs are not
uniform (`server/server_base.cc` authz tiers):

- **Client-accessible** (any authenticated user, or anyone when
  `--rpc_authentication=disabled`): `GetStatus`, `ServerClock`, and `ListTablets`
  (unless `--tserver_enforce_access_control=true`, which raises it to
  superuser).
- **Superuser / service-user**: `GetFlags`, `DumpMemTrackers`,
  `GetConsensusState`, `Quiesce`.

ksck already calls all of these, which means ksck in practice assumes an admin
identity; operators typically run these tools as a principal in
`--superuser_acl`. Fan-out inherits that assumption but must fail *softly*: a
probe that returns an authorization error sets that section's `probe_status` to
`UNAUTHORIZED` on that server and the report still returns the client-accessible
sections (identity, inventory, clock). The tool description states plainly that
consensus/flags/memory/quiescing sections require an admin/superuser or service
identity, so the model does not misread an authz gap as a cluster fault. This
also means fan-out never performs "permission laundering": it can read only what
the operator's own ticket could already read one server at a time.

### 3.10 Consistency and cost notes

- **Consistency.** RPC/HTTP data reflects the server's served/in-memory state at
  answer time -- internally consistent per server, and free of the on-disk race
  the optional-lock local tools suffer. `estimated_on_disk_size` is, as named, an
  estimate; the report labels it so and does not present it as an exact byte
  count.
- **Snapshot skew.** The fan-out is not a global snapshot: servers are sampled
  as tasks complete, within one `--timeout_ms` window. For inventory/skew
  analysis this is fine (the same looseness ksck accepts). The report timestamps
  each server record.
- **Cost.** `ListTablets` and `GetConsensusState` scale with replicas per
  server; `--sections` lets a caller avoid the heavy probes. Bounding
  concurrency protects both the bastion and the servers from a thundering-herd.

---

## 4. API sketch (CLI and MCP)

CLI (the source of truth; MCP reflects it):

```
kudu cluster gather <master_addresses>
    # selector: at most one entity scope; default is the whole cluster
    [--tservers=<uuid|host>,...]        # explicit server subset
    [--location=<prefix>]               # by rack/location
    [--table=<name>]                    # servers/tablets of one table
    [--tablet=<id>]                     # only that tablet's replica-holders
    [--row=<table>:<pk-json>]           # the tablet owning a row's key
    # probe selection + engine knobs (apply to any selector)
    [--sections=inventory,consensus,quiescing,flags,memory,identity]
    [--include_http_metrics]            # optional /metrics enrichment
    [--fetch_info_concurrency=20] [--timeout_ms=60000]
    [--gather_format=json_pretty|json_compact]
```

The output-format flag is a dedicated `--gather_format` (values `json_pretty` /
`json_compact`), mirroring ksck's own `--ksck_format` rather than reusing the
generic `--format` (pretty/space/tsv/csv/json). Gather emits only JSON, and a
dedicated flag keeps its schema and default independent of the tabular actions.

The selector flags are mutually exclusive scopes; with none set the scope is the
whole cluster. `--tablet`/`--row` switch the output to the entity-centric
(replica-set) projection of section 3.3.1; the others project server-centric
(with `--table` also offering a per-tablet map).

MCP tool `cluster_gather` (SURFACE, read-only): `master_addresses` is injected
from `--master_addresses` at launch and omitted from the schema; the remaining
flags become optional typed properties via the existing reflection
(`WriteMcpToolObject`). Crucially, **no `tserver_address` property exists** -- the
model names an entity (or nothing), and the server resolves the target set from
master metadata. That absence is the whole point of the feature.

Two model-facing flows the same tool covers:

- *"How is disk usage spread across the qa cluster?"* -> one call, no selector,
  `--sections=inventory` -> the rollup's on-disk-size distribution and top-N
  heaviest servers answer directly; no looping, no per-server text through the
  model.
- *"ksck says tablet abcd is unhealthy -- why?"* -> one call,
  `--tablet=abcd` -> a three-row replica-set diff (term, size, data-dir, health
  per copy) contacting only the three replica-holders. The drill-down that no
  single existing tool provides.

---

## 5. Degree of constraint

The solution space is tightly constrained, which is why this reads as a
"compose existing pieces" design rather than a greenfield one:

- The transport must be Kudu RPC (SSH and node-local execution are ruled out);
  the auth must be the inherited client stack (no new auth code, PRD D2).
- The fan-out engine, the discovery RPC, the concurrency knob, the failure-
  isolation idiom, and the PB-to-JSON output all already exist inside ksck.
- The reflected-action contract (prompt-returning, `cout`-captured, one
  disposition entry) is fixed by the MCP serve loop.

The design work is therefore mostly *selection and generalization*: lift ksck's
per-tserver fetch out of its health-summary framing into a reusable engine, add
selector-based targeting and a projected output, and expose it as one read
action. The genuinely new content, beyond composing existing parts, is: the
selector-driven target resolver -- especially the entity-scoped
(tablet/row) resolution and replica-set projection of section 3.3.1, which turns
fan-out from "scrape and filter" into topology-aware inspection; the parity map
(section 3.6); and the graceful-authz contract (section 3.9). The transport,
concurrency engine, and serialization are reused, not invented.

---

## 6. Data storage

None introduced. No sys-catalog change, no new on-disk structures, no WAL
impact. All produced state is in-memory and per-call, serialized to JSON and
discarded when the `tools/call` returns. This is a pure client-side read
feature; the durability/recovery/consistency surface is empty by construction,
which is a deliberate property (it keeps fan-out a safe, always-available
diagnostic).

---

## 7. Failure handling

- **Unreachable / slow server:** per-server `probe_status = UNREACHABLE` or
  `TIMED_OUT`; bounded by `--timeout_ms`; other servers unaffected
  (ksck-style isolation).
- **Presumed-dead server** (stale heartbeat at the master): listed from the
  master's last-known facts, RPC skipped, marked `presumed_dead` -- no timeout
  spent.
- **Authorization denied:** per-section `UNAUTHORIZED`; client-accessible
  sections still returned (section 3.9).
- **Master unreachable / leader failover:** discovery uses the client library's
  multi-master failover (as ksck does via `CreateKuduClient`); if no master is
  reachable the whole call returns a single cluster-level error (the model sees
  `isError:true` with the Status text, per the operator guide's connection-
  failure behavior).
- **Partial cluster:** the report always distinguishes "server reported X" from
  "we could not ask server" so the model never treats a probe gap as a fact.

---

## 8. Alternatives considered

**A1 -- Model-driven fan-out (status quo).** The model calls `tserver_list`,
then loops `remote_replica_list` / `tserver_get_flags` per address.
*Rejected as the primary path:* N+1 LLM-driven round trips, every server's full
text streamed back through the model (token blow-up), serial latency, no
aggregation/skew analysis, and it cannot reach the per-tablet on-disk sizes and
consensus health that the single-server tools do not expose. It remains
*available* as a fallback for one-off single-server questions; fan-out does not
remove those tools.

**A2 -- A new `--all` flag on each existing single-server action.** Generalize
`set_flag_for_all` to every per-server read. *Rejected as the shape:* it scatters
discovery + concurrency + aggregation logic across many actions, and each action
would still print one-server-shaped text, giving no rollup. A single engine with
one flagship action concentrates the logic and produces the analytical output.
(A thin `--all` convenience on top of the shared engine is reasonable Future
work once the engine exists.)

**A3 -- A new master-side aggregation RPC** (master fans out to tservers and
returns a cluster-wide blob). *Rejected (N3):* it adds a cross-version RPC and
server-side code to a cluster component, breaks the "MCP is a pure client"
property, and duplicates fan-out logic the client already has. The master
already aggregates the narrow case it should -- `GetTableStatistics` returns
per-tablet sizes -- and we reuse that; general per-tserver introspection does not
justify a new service.

**A4 -- HTTP `/metrics` (or `/dump-entities`) as the primary source.** Fan out
over the web port instead of RPC. *Rejected as primary, kept as enrichment
(3.8):* different transport and auth (SPNEGO), web ports may be firewalled off
from the bastion when the RPC port is not, and the RPC path reuses the inherited
client auth for free. Metrics are the right *complement* for the few gauges RPC
lacks, not the base.

**A5 -- SSH to each node and run the node-local tools.** *Rejected (section 1,
PRD D2):* rejected security posture, needs a login on every data node, and for
the read-only local tools still reads racy on-disk state under an optional lock.

---

## 9. Future work

- **Reuse the engine for online data-plane parity.** ksck's checksum scan is
  already a callback-driven, per-tserver-slotted (`--checksum_scan_concurrency`)
  online fan-out that verifies replica *data* consistency -- the online analogue
  of the offline block/rowset dumps we mark NONE. A future `cluster gather
  --verify` could surface that through the same engine.
- **`--all` mutating fan-out** behind `--allow-writes` + confirm + dry-run, for
  the cases where per-server *action* (not just read) across the fleet is wanted;
  it rides the same target resolver and engine but is GATED, not SURFACE.
- **Async job handle for very large fleets.** If a gather on a huge cluster ever
  exceeds a comfortable synchronous window, wrap it in the job-handle pattern the
  PRD already defines for long-runners (Future work, section 9 of the PRD):
  return `{job_id, running}` and add a status tool. Not needed at expected
  scales given bounded concurrency and `--sections`.
- **Resource / subscription.** A `kudu://fleet` MCP resource that re-runs a cheap
  inventory gather and pushes `notifications/resources/updated` on drift, once
  the PRD's resources capability lands.

---

## 10. Decisions and open questions

**Decided.**

- **Action placement/name: `cluster gather`** -- a new action under the existing
  `cluster` mode, beside `ksck`/`rebalance`, rather than extending `cluster ksck`
  with an inventory section. This keeps ksck a health *verdict* and gather an
  *inventory/drill-down*, and lets the two compose (ksck flags, gather explains).

**Open.**

- **Entity-selector output detail.** How much of the replica-set projection to
  render inline versus behind a verbosity flag -- e.g. whether `--tablet` always
  dumps full per-peer consensus config or only the divergence summary by default.
- **Default sections.** Which probes run when `--sections` is unset -- likely the
  cheap client-accessible set (identity + inventory + clock) so the default works
  for a non-superuser operator, with heavier admin sections opt-in.
- **HTTP enrichment in v1 or deferred.** Whether `--include_http_metrics` ships
  with the first cut or waits until the RPC-only core is proven.

**Decided (was open).**

- **Reuse vs. fork of ksck internals: sibling, not refactor.** A grounded read of
  the ksck fetch loop settled this. The engine is built as a new sibling in
  `src/kudu/tools/remote_cluster.{h,cc}` that *reuses the standalone pieces*
  (`BuildMessenger`, the four service proxies, the `ListTabletServers` discovery
  call, and the `--fetch_info_concurrency` gflag) but does *not* derive from or
  modify `RemoteKsckCluster` / `Ksck`. Rationale: the reusable parts
  (`BuildMessenger`, proxy construction) are already standalone and have zero
  ksck coupling; the only thing worth "extracting" is a ~35-line
  `pool->Submit`/`Wait` loop that is trivially reproduced, whereas a genuine
  extraction would edit `ksck.h` / `ksck_remote.h` -- load-bearing, well-tested
  files -- for modest gain. `FetchInfoFromTabletServers` writes into ksck's
  private `KsckResults` (`ksck.cc:524`), so the loop is welded to ksck's output
  type regardless. The sibling reproduces the idiom against `GatherResultsPB`.
  This is detailed in the implementation doc
  ([kudu_mcp_fanout_implementation.md](kudu_mcp_fanout_implementation.md)).
