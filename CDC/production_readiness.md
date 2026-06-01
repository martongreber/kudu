# Kudu CDC Production Readiness: Scorecard + Backlog

> Fresh production-readiness lens on the Kudu CDC port, benchmarked against
> YugabyteDB (CDCSDK / xrepl) as the reference implementation. Focus: features,
> gauges, safety mechanisms, and limits that are production-shaping and that
> Kudu does not yet have (or has only partially).
>
> This is an architecture/coverage document -- what moving pieces must exist for
> a prod-ready shape. Implementation, test, and review are later phases.
>
> Companion to `design.md` (living design), `gaps.md` (correctness audit), and
> `design_decisions.md` (DR-001..DR-009). This doc does not re-litigate settled
> decisions; see "Deliberately not ported" below.
>
> Last updated: 2026-08-26. Branch: cdc.

---

## How to read this

Two parts:

1. **Scorecard** -- category-by-category YB-vs-Kudu verdict. Every verdict is
   anchored to code on this branch, not to prior docs.
   - `DONE`    -- present and production-adequate.
   - `PARTIAL` -- present but with a gap that shapes operability.
   - `MISSING` -- absent; production-shaping.
   - `N/A`     -- deliberately does not apply to Kudu (see "Deliberately not ported").

2. **Backlog** -- the still-missing production-shaping pieces, prioritized P0..P3,
   each with the YB analog, why it shapes production, a Kudu-idiomatic design
   sketch, and rough surface (flags / metrics / files).

Severity legend for the backlog:
- `P0` -- can cause resource exhaustion or data-safety failure in normal fleets. Blocks prod.
- `P1` -- needed to operate the feature safely (observability, error attribution).
- `P2` -- hardening / bounded resource use under pathological input.
- `P3` -- operability polish and cleanups.

---

## Scorecard

### A. Observability / gauges

| Capability                          | YB reference                                   | Kudu today                                                        | Verdict |
|-------------------------------------|------------------------------------------------|-------------------------------------------------------------------|---------|
| Consumer wall-clock lag             | time_since_last_getchanges, cdcsdk_flush_lag   | cdc_max_sent_lag_micros + per-stream cdc_stream_sent_lag_micros   | DONE    |
| Consumer inactivity age             | cdcsdk_expiry_time_ms (proxy)                  | cdc_max_active_age_micros + per-stream cdc_stream_active_age_micros| DONE    |
| Active session count                | active poller count                            | cdc_active_streams                                                | DONE    |
| Request / record / checkpoint rate  | change_event_count, traffic counters           | cdc_get_changes_requests, cdc_records_produced, cdc_checkpoint_*  | DONE    |
| Op-index lag (ops/WAL behind)       | last_read_opid_index, last_readable_opid_index | cdc_stream_ops_behind + server rollup cdc_max_ops_behind (P1-1)   | DONE    |
| Bootstrap-required signal           | is_bootstrap_required                          | cdc_stream_bootstrap_required + cdc_bootstrap_required_streams (P1-1) | DONE |
| WAL bytes/segments retained by CDC  | log_retention_diagnostics, retained-index gauges| cdc_wal_retained_bytes (GCable diff: raft floor vs CDC clamp) (P1-2)| DONE  |
| History-floor age                   | history retention diagnostics                  | cdc_history_floor_age_micros (from Tablet::cdc_history_floor()) (P1-2)| DONE |
| Per-error-code counters             | per xrepl error counters                       | 12 cdc_errors_* counters via SetCDCError funnel; aggregate kept (P1-4)| DONE |
| Admission-shed counters             | (implicit via LEADER_NOT_READY)                | cdc_scans_rejected_concurrency/memory/worker_pool (P1-4)          | DONE |
| Master maintenance-loop health      | catalog task metrics                           | cdc_maintenance_last_run_micros/_duration + runs counter + 3 barrier gauges (P1-3) | DONE |

Note: master already tracks `cdc_barrier_releases_total_`,
`cdc_barrier_releases_deferred_total_`, `cdc_barriered_tablet_count_`
(catalog_manager.h:1601-1603) as atomics, but they are not published as metrics.

### B. WAL & history resource bounds (limits)

| Capability                          | YB reference                                   | Kudu today                                                        | Verdict |
|-------------------------------------|------------------------------------------------|-------------------------------------------------------------------|---------|
| Minimum WAL retention (time floor)  | (implicit)                                     | --cdc_wal_retention_secs (8h floor), enforced in log GC (log.cc:1005) | DONE |
| Hard WAL retention ceiling (time)   | log_max_seconds_to_retain (24h hard GC)        | --cdc_max_wal_retention_secs (24h) skips the clamp in GetRetentionIndexes | DONE |
| Disk-pressure safety valve          | log_stop_retaining_min_disk_mb (100MB floor)   | --cdc_stop_retaining_min_disk_mb (100MB) releases barrier under pressure | DONE |
| Tserver-local barrier auto-release  | cdc_min_replicated_index_considered_stale_secs (30m) | age ceiling doubles as dead-master backstop (barrier-not-advanced) | DONE |
| Byte/segment retention ceiling      | (segment count caps)                           | none (acknowledged as PLANNED in design.md Sec.8)                 | MISSING |
| Per-transaction WAL-span cap        | (intents-based)                                | --cdc_max_transaction_span_bytes (512MB) -> TRANSACTION_TOO_LARGE | DONE    |
| Per-transaction decoded-row memory  | (intents streamed)                             | span cap bounds WAL window, not decoded buffer                    | PARTIAL |
| Scan memory ceiling                 | consumption-based                              | --cdc_scan_mem_limit_bytes (256MB MemTracker)                     | DONE    |
| Fleet-level stream/tablet caps      | cdc_max_virtual_wal_per_tserver                | none -- unbounded active streams / barriered tablets              | MISSING |

**The central B-column finding:** the CDC retention barrier pins WAL through the
log's `for_durability` floor, which log GC respects unconditionally
(log.cc:996 -- "If removing this segment would compromise durability, we cannot
remove it"). There is no disk-space override, no hard time ceiling, and no
tserver-local auto-release. The **only** thing that lifts the barrier is the
master maintenance loop (`RunCDCStreamMaintenance`) pushing a higher retained
index on expiry/staleness. YB has three independent backstops here; Kudu has
zero. This is the dominant production-shaping gap and drives P0 below.

### C. Admission control / backpressure

| Capability                          | YB reference                                   | Kudu today                                                        | Verdict |
|-------------------------------------|------------------------------------------------|-------------------------------------------------------------------|---------|
| Worker-thread reservation           | RPC queue limits                               | --cdc_get_changes_free_rpc_ratio (0.10)                           | DONE    |
| Heavy-scan concurrency cap          | (semaphore)                                    | --cdc_max_concurrent_scans (8)                                    | DONE    |
| Scan memory cap                     | consumption-based                              | --cdc_scan_mem_limit_bytes (256MB)                                | DONE    |
| Safe read deadline                  | deadline propagation                           | --cdc_read_safe_deadline_ratio (0.10)                            | DONE    |
| Rejection visibility                | LEADER_NOT_READY surfaced                      | rejections happen but are not counted/visible                     | MISSING |
| Send-rate limiter                   | rate limiter                                   | descoped (DR-001)                                                 | N/A     |

### D. Retention & expiry safety (lifecycle correctness)

| Capability                          | YB reference                                   | Kudu today                                                        | Verdict |
|-------------------------------------|------------------------------------------------|-------------------------------------------------------------------|---------|
| Idle-stream expiry                  | cdcsdk stream expiry                            | --cdc_stream_expiry_ms (8h), master loop                          | DONE    |
| Non-advancing (stale) expiry        | staleness detection                            | --cdc_max_staleness_ms (4h), master loop                          | DONE    |
| Barrier SET/RELEASE reconciliation  | UpdateCdcReplicatedIndex fanout                | master loop 5-step, capped by --cdc_max_barrier_releases_per_run  | DONE    |
| Last-writer-wins barrier ordering   | index monotonicity                             | barrier_seq gate                                                  | DONE    |
| History-floor GC clamp              | history retention                              | Tablet::SetCDCHistoryFloor + monotonic guard                      | DONE    |
| Range-drop cleanup                  | tablet-drop cleanup                             | implemented (DR-009)                                              | DONE    |
| Independent-of-master safety        | tserver-local stale reset                      | disk valve + age ceiling release locally (P0-1); master no longer the only release path | DONE |

### E. Lifecycle / operational (server)

| Capability                          | YB reference                                   | Kudu today                                                        | Verdict |
|-------------------------------------|------------------------------------------------|-------------------------------------------------------------------|---------|
| Create / delete / list stream       | master_replication.proto                       | master RPCs + CLI                                                  | DONE    |
| Durable checkpoint                  | cdc_state YCQL table                            | ack-before-persist (DR-007), master-persisted                     | DONE    |
| Checkpoint write-combining          | (periodic flush)                               | --cdc_checkpoint_persist_interval_ms + cdc_checkpoint_persists    | DONE    |
| Committed-only transactions         | intents + commit records                       | in-call WAL-scan buffering (DR-005)                               | DONE    |
| Consistent snapshot bootstrap       | server snapshot + safe time                    | in-memory session (DR-006), SNAPSHOT_SESSION_LOST                 | DONE    |
| DELETING transition state           | stream state machine                           | proto state exists but unused                                     | PARTIAL |
| Hardcoded internal RPC timeouts     | flag-driven                                    | master RPC timeout hardcoded 10s                                  | PARTIAL |

### F. Consumer liveness (in-tree consumer)

| Capability                          | YB reference                                   | Kudu today                                                        | Verdict |
|-------------------------------------|------------------------------------------------|-------------------------------------------------------------------|---------|
| Pull-based poll loop                | XClusterPoller                                 | cdc_consumer poller per (stream, tablet)                          | DONE    |
| Retry with backoff                  | replication_failure_delay_exponent (expo backoff)| stops on terminal error; no supervised restart/backoff          | MISSING |
| Auto-resnapshot on WAL loss         | is_bootstrap_required -> re-bootstrap          | surfaces needs_resnapshot; no automatic re-snapshot               | MISSING |
| Consumer-side health/lag metrics    | poller metrics                                 | minimal                                                           | PARTIAL |

Note: the in-tree consumer is a reference/tool, not the only possible consumer,
so its gaps are operability rather than correctness. External consumers still
get the server-side contract (checkpoints, error codes).

### G. Operator / CLI surface

| Capability                          | YB reference                                   | Kudu today                                                        | Verdict |
|-------------------------------------|------------------------------------------------|-------------------------------------------------------------------|---------|
| Create / list / describe / delete   | yb-admin CDC commands                           | kudu cdc create/list/describe/delete (tool_action_cdc.cc)         | DONE    |
| Tail / consume for inspection       | (external tools)                               | kudu cdc consume                                                  | DONE    |
| Show lag / state / bootstrap in list| yb-admin list with status                       | list does not surface lag / bootstrap-required / retained bytes   | MISSING |
| Force-expire / force-release barrier| yb-admin drop / expire                          | none -- no operator remediation for a stuck stream                | MISSING |
| Inspect per-tablet retention barrier| yb-admin get_replication_status                 | none                                                              | MISSING |

### H. Error taxonomy / consumer contract

| Capability                          | YB reference                                   | Kudu today                                                        | Verdict |
|-------------------------------------|------------------------------------------------|-------------------------------------------------------------------|---------|
| Structured error codes              | 14-code xrepl taxonomy                          | 12-code CDCErrorPB                                                | DONE    |
| WAL-loss vs stream-expired split    | is_bootstrap_required vs expired                | WAL_EXPIRED vs STREAM_EXPIRED disambiguated                       | DONE    |
| Large-txn signal                    | (n/a)                                          | TRANSACTION_TOO_LARGE                                             | DONE    |
| Snapshot-session-lost signal        | snapshot invalidation                          | SNAPSHOT_SESSION_LOST                                            | DONE    |
| Dead code / unused fields           | --                                             | NOT_AUTHORIZED path dead; max_bytes_per_response field dead       | PARTIAL |

---

## Deliberately not ported (do not re-litigate)

These YB mechanisms are intentionally out of scope for Kudu's architecture and
are recorded so the backlog stays focused. See `design_decisions.md` and
`gaps.md` section D.

- **Virtual WAL / cross-tablet consistent ordering** (D1/D4) -- Kudu streams are
  per-tablet; no global LSN ordering layer.
- **Intents DB / dual-source read** -- Kudu's WAL is a complete logical change
  log (full WriteRequestPB per ReplicateMsg); no separate intents store.
- **Split lineage** (D2) -- Kudu has no online tablet split.
- **JSON wire format** (D5) -- PROTO only for now; JSON reserved.
- **Send-rate limiter** (DR-001) -- descoped.
- **Distributed cdc_state table** (DR-002) -- deferred; master-persisted
  checkpoints used instead.

---

## Backlog (prioritized)

### P0-1: Bounded-WAL guarantee (disk/time ceiling + independent auto-release)

**Gap.** The CDC barrier pins WAL through the log's `for_durability` floor
(clamped by `cdc_min_retained_op_index`, tablet_replica.cc:773; respected
unconditionally in log GC, log.cc:996). Nothing overrides it: no disk-space
valve, no hard time ceiling, and no tserver-local release. The single release
path is the master maintenance loop. If that loop stalls or dies -- or if enough
slow-but-advancing streams accumulate -- WAL grows without bound until the disk
fills and the tserver crashes or is evicted. `--cdc_wal_retention_secs` is a
*floor* (minimum retention), not a ceiling, so it does not help.

**YB analog.** Three independent backstops, all of which Kudu lacks:
- `log_max_seconds_to_retain` (24h) -- hard GC even if the CDC barrier still points earlier.
- `log_stop_retaining_min_disk_mb` (100MB) -- abandon CDC retention under disk pressure.
- `cdc_min_replicated_index_considered_stale_secs` (30m) -- tserver-local reset of the barrier to max if the master stops refreshing it.

**Why prod-shaping.** This is the failure that takes down nodes. A single point
of release with no automatic relief is not safe for a multi-tenant fleet.

**Kudu-idiomatic design.**
- (a) *Disk-pressure valve.* In `TabletReplica::GetRetentionIndexes` /
  `GetPrefixSizeToGC`, when free space on the WAL dir drops below
  `--cdc_stop_retaining_min_disk_mb`, stop clamping `for_durability` by the CDC
  barrier (allow GC past it). Log a WARNING, bump a metric, and mark affected
  streams so their next GetChanges returns `WAL_EXPIRED` (driving resnapshot).
- (b) *Local staleness backstop.* Track the wall-clock time the barrier index was
  last refreshed by the master. If it exceeds `--cdc_barrier_local_stale_secs`
  (default generous, e.g. 2x the expiry window -- this only fires on a clearly
  dead master loop, not on normal lag), locally release the barrier and log/count
  it. Conservative by construction so it does not re-introduce the risk DR-003
  avoided.
- (c) *Optional hard time ceiling.* `--cdc_max_wal_retention_secs` (analog of
  `log_max_seconds_to_retain`): an absolute cap after which a segment is GC-able
  regardless of the barrier.

**Surface.** Flags: `--cdc_stop_retaining_min_disk_mb`,
`--cdc_barrier_local_stale_secs`, `--cdc_max_wal_retention_secs`. Files:
`consensus/log.cc` (`GetPrefixSizeToGC`), `tablet/tablet_replica.cc`
(`GetRetentionIndexes`), `cdc/cdc_service.cc` (mark streams WAL_EXPIRED). Metrics:
`cdc_barrier_forced_releases` (see P1-3).

**Severity: P0.**

---

### P1-1: Op-index lag and bootstrap-required signal

**Gap.** Only wall-clock lag is exposed (`cdc_max_sent_lag_micros`). There is no
"how many ops / how much WAL behind" gauge, and no "this consumer will need a
resnapshot" signal until it fails with `WAL_EXPIRED`.

**YB analog.** `last_read_opid_index`, `last_readable_opid_index`,
`is_bootstrap_required`, `cdcsdk_flush_lag`.

**Why prod-shaping.** Wall-clock lag does not measure proximity to WAL GC: a
consumer with low time-lag can still be near the retention edge if the WAL churns
fast. `is_bootstrap_required` is the single most actionable operational signal --
it tells operators a consumer has fallen past the retained WAL before it hard-fails.

**Kudu-idiomatic design.** Add per-(stream, tablet) gauges:
`cdc_stream_ops_behind` = last_readable_index - consumer_checkpoint_index; and
`cdc_stream_bootstrap_required` (0/1) = consumer checkpoint older than the
earliest retained WAL op. Compute from the cached checkpoint plus the log's
earliest-retained OpId. Roll up to server-level max/any.

**Surface.** Metrics only. Files: `cdc/cdc_service.cc` (metric defs + update in
GetChanges), `consensus/log.h` (earliest-retained accessor if not present).

**Severity: P1.**

---

### P1-2: WAL / history retention cost gauges

**Gap.** The disk cost of CDC retention is invisible: no gauge for bytes/segments
a tablet retains on CDC's behalf, and none for history-floor age.

**Why prod-shaping.** P0 is a disk-exhaustion risk; you cannot manage a risk you
cannot see. These gauges are the early-warning signal before the P0 valve fires.

**Kudu-idiomatic design.** Expose `cdc_wal_retained_bytes` per tablet (the log
already computes GCable data size, `Log::GetGCableDataSize`) and
`cdc_history_floor_age_micros` from `Tablet::cdc_history_floor_`.

**Surface.** Metrics only. Files: `tablet/tablet.cc`, `consensus/log.cc`,
`cdc/cdc_service.cc`.

**Severity: P1.**

---

### P1-3: Master maintenance-loop observability

**Gap.** The master already tracks barrier atomics
(`cdc_barrier_releases_total_`, `cdc_barrier_releases_deferred_total_`,
`cdc_barriered_tablet_count_`; catalog_manager.h:1601-1603) but does not publish
them as metrics, and there is no "last successful maintenance scan" gauge.

**Why prod-shaping.** The master loop is the single release point (P0). If it
silently stops, nothing surfaces it -- and WAL quietly accumulates. A last-run-age
gauge makes a stalled loop immediately visible.

**Kudu-idiomatic design.** Publish the three atomics as master metrics; add
`cdc_maintenance_last_run_micros` (gauge) and `cdc_maintenance_runs` (counter)
plus a run-duration gauge. Alert on last-run age exceeding a few scan intervals.

**Surface.** Metrics only. Files: `master/catalog_manager.cc/.h`.

**Severity: P1.** (Directly de-risks P0.)

---

### P1-4: Per-error-code and admission-shed counters

**Gap.** A single `cdc_errors` counter with no breakdown by the 12 CDCErrorPB
codes, and no counters for admission rejections (concurrency / memory /
worker-thread caps) or safe-deadline sheds.

**Why prod-shaping.** "errors went up" is not actionable. Operators need to
distinguish WAL_EXPIRED (resnapshot needed) from admission sheds (add capacity)
from TRANSACTION_TOO_LARGE (workload issue).

**Kudu-idiomatic design.** Convert `cdc_errors` into a per-code counter family
(or add labeled counters per code); add `cdc_scans_rejected_concurrency`,
`cdc_scans_rejected_memory`, `cdc_scans_rejected_worker_pool`, incremented at the
three admission layers.

**Surface.** Metrics only. Files: `cdc/cdc_service.cc`.

**Severity: P1.**

---

### P2-1: Per-transaction decoded-row memory cap

**Gap.** `--cdc_max_transaction_span_bytes` bounds the WAL *read window*, but the
in-call buffer of a committed transaction's decoded rows is not independently
capped by a MemTracker. A pathologically wide transaction can balloon RSS even
within the span limit.

**Kudu-idiomatic design.** Charge the transaction row buffer against the existing
`--cdc_scan_mem_limit_bytes` MemTracker (or a dedicated child tracker); on
exceed, return `TRANSACTION_TOO_LARGE`.

**Surface.** Files: `cdc/cdc_service.cc` (txn buffering path).

**Severity: P2.**

---

### P2-2: Fleet-level stream / barrier caps

**Gap.** No cap on the number of active streams or barriered tablets. Barrier
fan-out cost (master loop) and per-session memory (tserver) grow unbounded with
stream count.

**YB analog.** `cdc_max_virtual_wal_per_tserver` and related per-node caps.

**Kudu-idiomatic design.** `--cdc_max_streams` (master; reject `CreateCDCStream`
beyond the cap with a clear error) and optionally
`--cdc_max_barriered_tablets_per_server`.

**Surface.** Flags + master validation. Files: `master/catalog_manager.cc`.

**Severity: P2.**

---

### P3-1: Consumer supervised restart + auto-resnapshot

**Gap.** The in-tree consumer poller stops on a terminal error and surfaces
`needs_resnapshot`; there is no supervised restart with backoff and no automatic
re-snapshot on WAL loss.

**YB analog.** `XClusterPoller` exponential backoff
(`replication_failure_delay_exponent`) and automatic re-bootstrap on
`is_bootstrap_required`.

**Kudu-idiomatic design.** Add a supervised restart loop in `cdc_consumer` with
exponential backoff for retryable errors; add an opt-in auto-resnapshot on
`WAL_EXPIRED`/`STREAM_EXPIRED` (drop checkpoint, restart from snapshot), gated
behind a flag since it re-reads all data.

**Surface.** Files: `cdc/cdc_consumer.cc/.h`.

**Severity: P3.** (In-tree consumer is a reference, not the only consumer.)

---

### P3-2: Operator remediation CLI

**Gap.** `kudu cdc` has create/list/describe/delete/consume but no remediation:
no force-release of a stuck stream's barrier, no lag/bootstrap columns in `list`,
no per-tablet retention-barrier inspection.

**Kudu-idiomatic design.**
- `kudu cdc release_barrier <stream>` -- admin force-release (master pushes an
  immediate barrier release for the stream).
- Add lag / state / bootstrap-required columns to `list`.
- Extend `describe` to show per-tablet barrier index, retained bytes, and
  last-advance time.

**Surface.** Files: `tools/tool_action_cdc.cc`, plus a master admin RPC for
force-release.

**Severity: P3.**

---

### P3-3: Cleanups

From the current-branch audit; low risk, reduce confusion:
- Remove the dead `NOT_AUTHORIZED` code path (or wire it to a real authz gate).
- Remove the dead `max_bytes_per_response` proto field (or implement it as the
  byte ceiling folded into P0-c).
- Add a GROUP_FLAG_VALIDATOR for any idle-expiry flag divergence between server
  and consumer assumptions.
- Replace the hardcoded 10s master RPC timeout with a flag.
- Remove or implement the unused `DELETING` stream state.

**Severity: P3.**

---

## Proposed new flags (summary)

| Flag                                   | Scope   | Purpose                                    | Backlog |
|----------------------------------------|---------|--------------------------------------------|---------|
| --cdc_stop_retaining_min_disk_mb       | tserver | Disk-pressure barrier release              | P0-1a   |
| --cdc_barrier_local_stale_secs         | tserver | Local backstop if master loop dies         | P0-1b   |
| --cdc_max_wal_retention_secs           | tserver | Hard time ceiling on barrier retention     | P0-1c   |
| --cdc_max_streams                      | master  | Fleet-level stream cap                     | P2-2    |
| --cdc_max_barriered_tablets_per_server | master  | Fleet-level barriered-tablet cap           | P2-2    |

## Proposed new metrics (summary)

| Metric                              | Scope        | Purpose                          | Backlog |
|-------------------------------------|--------------|----------------------------------|---------|
| cdc_stream_ops_behind               | stream       | Ops behind readable WAL          | P1-1    |
| cdc_stream_bootstrap_required       | stream       | Consumer past retained WAL (0/1) | P1-1    |
| cdc_wal_retained_bytes              | tablet       | Disk cost of CDC retention       | P1-2    |
| cdc_history_floor_age_micros        | tablet       | History-floor age                | P1-2    |
| cdc_barrier_releases_total          | master       | Publish existing atomic          | P1-3    |
| cdc_barrier_releases_deferred_total | master       | Publish existing atomic          | P1-3    |
| cdc_barriered_tablet_count          | master       | Publish existing atomic          | P1-3    |
| cdc_maintenance_last_run_micros     | master       | Detect stalled maintenance loop  | P1-3    |
| cdc_maintenance_runs                | master       | Maintenance scan counter         | P1-3    |
| cdc_errors{code=...}                | server       | Per-error-code breakdown         | P1-4    |
| cdc_scans_rejected_concurrency      | server       | Admission shed (concurrency)     | P1-4    |
| cdc_scans_rejected_memory           | server       | Admission shed (memory)          | P1-4    |
| cdc_scans_rejected_worker_pool      | server       | Admission shed (worker threads)  | P1-4    |
| cdc_barrier_forced_releases         | tserver      | P0 valve fired                   | P0-1    |

---

## Implementation progress (P0 -> P1 run, started 2026-08-26)

Autonomous run on branch `cdc`. Gate per item: incremental ninja build + full CDC
test suite (cdc_util-test, cdc_service-test, cdc_client-test, cdc-itest,
cdc_client-itest, cdc_failover-itest, cdc_manager-test; plus log-test and
tablet_metadata-test where retention/superblock code is touched). Changes are left
in the working tree uncommitted; the developer commits manually at logical points.

Pre-existing failure found and fixed during this run: cdc_util-test failed on branch
HEAD with a libcdc.so undefined-symbol (FLAGS_rpc_num_service_threads) load error.
Root cause: the admission-control code in cdc_service.cc reads that flag (defined in
server/rpc_server.cc -> server_process library), but the `cdc` library's CMake
target_link_libraries never declared the server_process dependency. cdc_service-test
only passed because its own source references the flag, forcing symbol resolution;
cdc_util-test did not, exposing the missing link dep. Fix: add server_process to
target_link_libraries(cdc ...) in src/kudu/cdc/CMakeLists.txt. Verified pre-existing
by stashing all edits on clean HEAD. FIX VERIFIED: cdc_util-test now links and
passes; full suite 9/9 green at the P1-1 gate.

| Item | Description                                   | State       | In tree | Notes |
|------|-----------------------------------------------|-------------|---------|-------|
| P0-1 | Bounded-WAL guarantee (disk valve + age ceiling) | DONE     | yes     | 2 release conditions in GetRetentionIndexes + cdc_barrier_forced_releases metric. Suite green modulo pre-existing cdc_util-test |
| P1-1 | Op-index lag + bootstrap-required gauges      | DONE        | yes     | 4 gauges (cdc_stream_ops_behind, cdc_stream_bootstrap_required + server rollups); readable idx from GetLastOpId(COMMITTED), earliest-retained from LogReader::GetMinReplicateIndex. Suite 9/9 green (incl. now-fixed cdc_util-test) |
| P1-2 | WAL/history retention cost gauges             | DONE        | yes     | 2 tablet FunctionGauges: cdc_wal_retained_bytes (GCableDataSize(raftFloor) - GCableDataSize(cdcClamped)) and cdc_history_floor_age_micros (from existing Tablet::cdc_history_floor()); registered in Start() under metric-null guard, AutoDetach(metric_detacher_). Suite 9/9 green |
| P1-3 | Master maintenance-loop observability         | DONE        | yes     | 6 master-entity metrics: 3 FunctionGauges over existing atomics (releases_total, releases_deferred_total, barriered_tablet_count) + cdc_maintenance_last_run_micros / _duration_micros gauges + cdc_maintenance_runs counter; stamped at end of RunCDCStreamMaintenance, detached via FunctionGaugeDetacher. Suite 9/9 green |
| P1-4 | Per-error-code + admission-shed counters      | DONE        | yes     | 12 per-code counters (cdc_errors_*) routed through SetCDCError (now a member; 11 scattered errors_->Increment removed, aggregate cdc_errors preserved) + 3 shed counters (cdc_scans_rejected_concurrency/memory/worker_pool) at the TryAcquireScanSlot + worker-reservation branches. Suite 9/9 green |

## One-paragraph summary

The Kudu CDC port is functionally mature: the change model, correctness bugs
(E1-E12), expiry/retention lifecycle, admission control, and error taxonomy are
all in place and code-verified. The production-shaping gaps that remain cluster
into three themes. **First and most important (P0): WAL retention has no
automatic relief.** The barrier pins WAL unconditionally and only the master
maintenance loop releases it -- no disk-pressure valve, no time ceiling, no local
backstop -- so a stalled master loop or a fleet of slow consumers can fill disks
and take down nodes. **Second (P1): observability is time-based only.** There is
no op-index lag, no bootstrap-required signal, no view of CDC's disk cost, no
per-error-code attribution, and the master's own barrier atomics are not even
published. **Third (P2/P3): bounded-resource hardening and operability** --
per-transaction memory cap, fleet-level stream caps, consumer supervised restart,
and operator remediation CLI. P0 and P1-3 together are the minimum bar for
running this safely in a shared fleet.
