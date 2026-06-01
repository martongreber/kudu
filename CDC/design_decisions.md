# Kudu CDC Design Decisions Log

> Status: Living, append-only decision log. Started 2026-08-26.
>
> This is the rolling record of notable CDC design decisions: things we chose to
> build a certain way, defer, or descope, and -- most importantly -- *why*. It
> complements the other CDC docs, which answer different questions:
>
> - `design.md` -- what the design is and what is DONE/PARTIAL/PLANNED.
> - `gaps.md` -- open items and the resolved-bug audit.
> - `dev_docs/YB_KUDU_CDC_COMPARISON.md` -- feature/architecture gaps vs. YB.
>
> A readiness verdict goes stale; a decision and its rationale do not. When a
> later decision reverses an earlier one, do NOT edit the old entry's conclusion
> -- add a new entry and mark the old one `Superseded by DR-NNN`. That way the
> reasoning trail survives.

## How to use this log

- **Append, newest first.** Give each decision a stable `DR-NNN` id (never reuse).
- **Status values:** `Accepted` (we are doing it), `Deferred` (not now, will
  revisit -- say the trigger that would revisit it), `Descoped` (decided not to
  do it, with the bar that would change that), `Superseded by DR-NNN`.
- Keep each entry to: Context, Decision, Rationale, and Links. Cite `file:line`
  where a claim is code-backed.
- Convert relative dates to absolute (YYYY-MM-DD).

## Index

| ID | Date | Status | Decision |
|---|---|---|---|
| DR-022 | 2026-09-01 | Accepted | `--cdc_svc_queue_length` default raised 50 -> 5000 (matching YB's `xcluster_svc_queue_length`); the dedicated CDC pool is fully isolated (own queue + own worker threads), so a deep queue only buffers catch-up/re-bootstrap bursts before shedding, at ~2 MB worst-case memory, with no risk to consensus/tablet RPCs. Amends DR-012's default |
| DR-021 | 2026-09-01 | Accepted | `CreateCDCStream` partial checkpoint-fanout failure now best-effort marks the half-written stream `DELETING` so the two-phase reap cleans it within one maintenance pass instead of a ~4h staleness wait; if the mark write also fails, falls back to the old self-heal (no worse than before). Hardens DR-011 |
| DR-020 | 2026-09-01 | Accepted | Two-phase reap gates durable stream-row removal on the barrier-RELEASE task reaching a terminal state (two passes: dispatch+defer, then remove once tasks terminal), closing the DR-010 F-2 crash window where a post-removal master crash could permanently pin WAL/MVCC before the async RELEASE landed. Residual narrowed to a tserver offline > `--unresponsive_ts_rpc_timeout_ms` during the reap. Extends DR-010 |
| DR-019 | 2026-09-01 | Accepted | Staleness release also honors a per-(stream,tablet) in-memory advance-ATTEMPT timestamp: `effective_advance = max(last_persisted, leader_ready, advance_attempt)`, so a consumer that is actively advancing but whose checkpoint cannot be durably persisted (master up, sys-catalog write failing) does not get its retention barrier silently released (CF-2 Component B). Persist failures also surface via a new `cdc_checkpoint_persist_failures` counter instead of being silently discarded. Extends DR-015 |
| DR-018 | 2026-08-29 | Accepted | CDC error contract: every `CDCErrorPB` carries machine-readable `is_retryable`/`needs_resnapshot` (E1); `GetChanges` returns `SCHEMA_VERSION_MISMATCH` when a consumer declares a `schema_version` older than the tablet's APPLIED schema, so it refreshes rather than decoding new columns as hex (E2). Request-scoped adaptation of YB's out-of-band schema-version negotiation (E1,E2) |
| DR-017 | 2026-08-29 | Accepted | CDC scan admission also honors the server-wide soft-memory limit (`process_memory::SoftLimitExceeded()`), not just the CDC-local heap budget -- a `GetChanges`/snapshot scan is shed with `ServiceUnavailable` when the whole process is under memory pressure, so CDC cannot push a memory-stressed tserver over the hard limit (R2/G4) |
| DR-016 | 2026-08-29 | Accepted | Both WAL force-release backstops (disk-pressure valve, age ceiling) are covered by deterministic `TabletReplica`-level tests that drive `GetRetentionIndexes()`+`Log::GC()` directly -- disk via an oversized threshold, age via a backdated-clock test hook -- rather than a full-cluster disk-full simulation (V2/G2) |
| DR-015 | 2026-08-29 | Accepted | Max-staleness barrier release is measured from `max(last_advance, leader_ready)` via a per-leadership grace floor (`cdc_leader_ready_micros_`), so a master outage longer than the window cannot make a fresh leader mass-release every barrier on its first pass (V4/CF-2) |
| DR-014 | 2026-08-29 | Accepted | CDC force-release valve (disk pressure / barrier age) also releases the in-memory MVCC/UNDO history floor, not just the WAL clamp -- closes the half-open valve where UNDO could not be reclaimed during a disk-full event (V1) |
| DR-013 | 2026-08-29 | Accepted | Table drop condemns referencing CDC streams: mark ACTIVE stream `DELETING` via eager `DeleteTable` hook + maintenance-loop backstop; hooks only the terminal REMOVED transition so soft-delete/recall stays intact (L2) |
| DR-012 | 2026-08-29 | Accepted | Dedicated CDC RPC service-pool queue (`--cdc_svc_queue_length`, default 50) via a generic `RegisterService(service, queue_length)` overload. NB (audit 2026-08-31): each `ServicePool` also gets its own dedicated worker threads, so CDC is isolated at both the queue AND the worker level -- stronger than the original "shared workers" framing and stronger than YB's xCluster (R1) |
| DR-011 | 2026-08-29 | Accepted | `CreateCDCStream`: durable op_index=0 initial checkpoint row + eager barrier push (L5); reject missing/deleted/soft-deleted/not-running tables (L6) |
| DR-010 | 2026-08-29 | Accepted | Two-phase durable `DELETING` stream lifecycle: mark-then-idempotent-reap, RELEASE-before-removal (closes L1/L3/L4) |
| DR-009 | 2026-08-26 | Accepted | Kudu-specific operational surface: range-drop cleanup, in-tree consumer + CLI, authz gate, schema-current-only |
| DR-008 | 2026-08-26 | Accepted | 3-layer heavy-scan admission control and a 12-code Kudu-original error taxonomy |
| DR-007 | 2026-08-26 | Accepted | Checkpoint ACK-before-persist (at-least-once), not persist-before-ACK |
| DR-006 | 2026-08-26 | Accepted | Server-driven in-memory snapshot session, not a master-persisted one |
| DR-005 | 2026-08-26 | Accepted | Committed-only transactions via in-call WAL-scan buffering, not an intents store |
| DR-004 | 2026-08-26 | Accepted | Before-image via MVCC/UNDO time-travel with a strict, escape-hatch-free HISTORY_EXPIRED |
| DR-003 | 2026-08-26 | Accepted | Master-push retention-barrier propagation, not tserver-pull from a state table |
| DR-002 | 2026-08-26 | Deferred | Do not build a distributed `cdc_state`-style checkpoint table (Tier 2 #3, lever 1) |
| DR-001 | 2026-08-26 | Descoped | Do not implement a CDC send-rate limiter (Tier 2 #7) |

---

## DR-022 -- Raise the dedicated CDC RPC queue default to YB parity (5000)

- **Date:** 2026-09-01
- **Status:** Accepted
- **Area:** Cross-service resource isolation -- CDC RPC admission

### Context

DR-012 gave CDC its own RPC `ServicePool` (own queue + own worker threads) but
kept the queue default at 50 -- the same value as the shared
`--rpc_service_queue_length`. Under a large CDC catch-up burst or a
re-bootstrap storm (many consumers reconnecting at once), a 50-deep queue fills
and starts shedding `GetChanges` RPCs (retriable errors) until an operator
manually raises the flag. YB ships `xcluster_svc_queue_length=5000`.

### Decision

Raise the `--cdc_svc_queue_length` default from 50 to 5000, matching YB's
`xcluster_svc_queue_length`, and expand the flag help to explain the isolation
and the shed-vs-buffer tradeoff.

### Rationale

- The CDC pool is fully isolated (DR-012 audit): its own queue AND its own
  worker threads. A deeper queue therefore only buffers more pending CDC calls
  before shedding; it cannot starve or interfere with consensus/tablet RPCs.
- Memory cost is negligible: an `InboundCall` is ~200-400 bytes, so a full
  5000-deep queue is ~2 MB -- trivial on any tserver. No Kudu-specific reason to
  pick lower.
- Behavior is otherwise unchanged: when the (now deeper) queue does fill, CDC
  RPCs are still shed with a retriable error, so backpressure is preserved.

### Links

- Impl: `src/kudu/tserver/tablet_server.cc:53` (`--cdc_svc_queue_length` default
  + help text).
- Test: `CDCServiceDedicatedQueueTest` sets its own explicit value (4242), so the
  default change does not affect it -- still passing. cdc_service-test 60/60.
- Amends DR-012 (which established the dedicated pool at default 50).

---

## DR-021 -- Best-effort DELETING on partial checkpoint-fanout failure at create

- **Date:** 2026-09-01
- **Status:** Accepted
- **Area:** Stream lifecycle -- create-side crash/error boundary

### Context

DR-011 writes the `CreateCDCStream` stream row `ACTIVE`, then fans out a durable
`op_index=0` checkpoint row per tablet. If one `WriteCDCTabletCheckpoint` fails
mid-fanout, the caller gets an error but the stream row stays `ACTIVE` in
sys-catalog with a partial set of checkpoint rows -- a zombie that pins WAL on
the already-written tablets and only self-heals after `--cdc_max_staleness_ms`
(~4h) via the staleness path.

### Decision

On a mid-fanout checkpoint-write failure, before returning the error,
best-effort mark the just-written stream row `state=DELETING` (durable
`WriteCDCStream`) so the two-phase reap (DR-010/DR-020) cleans it up on the next
maintenance pass -- fast, instead of a 4h wait. If the DELETING mark write also
fails, log and return the original error: we are then no worse than before (the
staleness path still self-heals). The success path is unchanged.

### Rationale

- Reuses the two-phase reap rather than adding a bespoke rollback: the created
  stream is condemned by the same machinery that handles all other deletes.
- Best-effort, not transactional: the tighter "guaranteed rollback" is not
  achievable because the recovery write can fail under the same conditions that
  caused the original failure -- so we fall back to the pre-existing self-heal
  rather than pretend to a guarantee we cannot keep.

### Links

- Impl: `src/kudu/master/catalog_manager.cc` -- `CreateCDCStream` partial-failure
  recovery block (~8522-8593); test-only injection flag
  `--cdc_create_stream_fail_checkpoint_idx` (~209-221, hidden/unsafe/runtime).
- Test: `CDCManagerTest.CreateStream_PartialCheckpointFanoutMarksDeleting`
  (injects failure on the 2nd of 2 tablets; asserts error return, one checkpoint
  row, stream `DELETING` not `ACTIVE`, then reaped). cdc_manager-test 37/37.
- Hardens DR-011; composes with DR-010/DR-020 reap.

---

## DR-020 -- Close the F-2 reap crash window: gate row removal on RELEASE completion

- **Date:** 2026-09-01
- **Status:** Accepted
- **Area:** Stream lifecycle -- delete-side crash boundary

### Context

The DR-010 two-phase reap dispatched the retention-barrier RELEASE as a
fire-and-forget async task, then removed the checkpoint rows and stream row.
Residual F-2 (P2): if the master crashed *after* the durable stream-row removal
but *before* the async RELEASE landed on the tablet, the new leader found no
DELETING marker, no checkpoint rows, and no in-memory `cdc_barriered_tablets_`
entry -- so it sent no RELEASE, and the tablet reloaded `cdc_min_retained_op_index`
from its superblock on restart and pinned WAL + MVCC history **permanently**
(same consequence as the original L1, far lower probability).

### Decision

Make the reap two-pass and gate durable `RemoveCDCStream` on confirmed RELEASE
task completion:

- **Pass A** (first encounter of a DELETING stream): dispatch the RELEASE RPCs,
  collecting the task handles into an in-memory
  `pending_release_tasks_[stream_id]`; if any task is live, `continue` and defer
  all row removal. (If there is no consensus config -- e.g. unassigned tablets in
  tests -- fall through to immediate removal.)
- **Pass B+**: proceed to remove checkpoint rows, then the stream row, only once
  every task for that stream is terminal (`Complete`/`Failed`/`Aborted`), then
  erase the `pending_release_tasks_` entry.

Because the checkpoint rows and stream row are not removed until the RELEASE is
terminal, a post-removal crash can no longer precede a landed RELEASE. On
failover the new leader re-runs Pass A against the still-intact DELETING row +
checkpoint rows.

### Rationale

- **Bounded, not unbounded.** Tasks reach a terminal state within
  `--unresponsive_ts_rpc_timeout_ms` (default 10 min); an unreachable tserver
  times the task out rather than stalling the reap forever.
- **vs. YB:** YB has no such window because release is tserver-*pull* -- a tserver
  observes the absence of its `cdc_state` row on its next poll and self-releases
  (`DeleteCDCStateTableMetadata`). Kudu's master-*push* model (DR-003) cannot
  replicate that without tservers periodically querying the master (a large
  architectural change); the two-pass completion gate is the bounded,
  non-disruptive equivalent.
- `pending_release_tasks_` is intentionally in-memory (background-thread-only):
  after failover the new leader rebuilds it from the durable DELETING row, so no
  new persistent state is needed.

### Residual

Narrowed, not eliminated. If a tserver is unreachable for the entire
`--unresponsive_ts_rpc_timeout_ms` window during the reap, the task ends
`Failed`/`Aborted`, the stream row is removed anyway, and that tablet keeps its
superblock barrier -- a leak scoped to "tserver offline > ~10 min across the
reap." Fully closing this requires tserver self-release (YB's model), which is
out of scope here.

### Links

- Impl: `src/kudu/master/catalog_manager.cc` -- `ReapDeletedCDCStreams` two-pass
  body (~8801-8924); `SendCDCRetentionBarrierToAllReplicas` gains an `out_tasks`
  out-param (`catalog_manager.cc:9030`, decl `catalog_manager.h:1180-1188`);
  `pending_release_tasks_` field (`catalog_manager.h:1627-1641`);
  `inject_pending_release_task_for_tests` (`catalog_manager.h:1661-1672`).
- Test: `CDCManagerTest.Reap_F2Guard_DeferredUntilTasksTerminal` (task in
  `Running` -> row survives; advanced to `Complete` -> fully removed).
  cdc_manager-test 37/37.
- Extends DR-010; constrained by DR-003 (master-push).

---

## DR-019 -- Staleness release honors an advance-attempt floor (CF-2 Component B)

- **Date:** 2026-09-01
- **Status:** Accepted
- **Area:** Retention safety -- max-staleness barrier release

### Context

DR-015 stopped a recovering master from mass-releasing barriers after an outage
by measuring staleness from `max(last_advance, leader_ready)`. That covered the
master-*outage* half of CF-2. The other half remained: the master is *up*, but
the checkpoint persist path fails. A consumer's checkpoint RPC returns SUCCESS
to the consumer *before* the master persist (fire-and-forget, latency-decoupled);
if the master-side `WriteCDCTabletCheckpoint` then fails, the error was silently
discarded and `last_checkpoint_advance_time_micros` stayed stale. With the master
long past `leader_ready`, after `--cdc_max_staleness_ms` the maintenance loop
classified the stream stale and released the barrier -- silently GC-ing WAL an
actively-advancing consumer still needs. DR-018's error contract does not help:
the consumer never received an error.

### Decision

Distinguish "consumer is not advancing" (safe to release after staleness) from
"consumer IS advancing but we cannot durably persist" (must NOT release):

- Add a per-(stream,tablet) in-memory `last_checkpoint_advance_attempt_micros_`
  (atomic, non-durable) on the master's `CDCTabletCheckpointInfo`, stamped in
  `UpdateCDCCheckpoint` whenever a forward-progress advance is *attempted*
  (`advances == true`), **before** the sys-catalog write -- so it records the
  attempt regardless of whether the write succeeds.
- The staleness guard becomes a three-way max:
  `effective_advance = max(last_advance, leader_ready_micros, advance_attempt)`.
  The `last_advance > 0` gate is unchanged.
- Stop silently discarding persist failures: increment a new server counter
  `cdc_checkpoint_persist_failures` (and keep the throttled WARNING log) when
  `PersistCheckpoint` exhausts all masters.

### Rationale

- **Truly-stuck consumer is still released.** A consumer that stops advancing
  never re-stamps `advance_attempt` (it is only stamped on a real forward
  `op_index`); term (c) goes stale exactly like term (a), so the safety release
  still fires on schedule. Verified by control cases in the test.
- **Minimal and race-free.** The master is the single entity that both receives
  advance attempts and runs the staleness guard, so an in-memory attempt
  timestamp on its own per-row object is the least-mechanism signal; no new
  durable/persistent state.
- **vs. YB:** YB writes the checkpoint synchronously inside `GetChanges` and
  returns the write error to the consumer, which retries -- so it never needs a
  persist-failure grace floor. Kudu deliberately decouples checkpoint latency
  from a master round-trip (async persist), which creates the false-SUCCESS gap;
  the attempt floor is the master-push equivalent of YB's synchronous retry.

### Residual

`advance_attempt` is in-memory: after a master restart it resets to 0 and
protection falls back to DR-015's `leader_ready` grace for the first staleness
window (correct -- equivalent to an outage). If persists fail continuously past
that window AND the consumer also stops advancing, the barrier is released --
which is the intended behavior for an indefinitely-stuck stream. The new test
injects at the master level (via a test accessor); no full tserver-through-RPC
end-to-end injection test.

### Links

- Impl: `src/kudu/master/catalog_manager.h:598`
  (`last_checkpoint_advance_attempt_micros_` on `CDCTabletCheckpointInfo`),
  `catalog_manager.cc:~8988` (stamp before write), `catalog_manager.cc:~9143`
  (three-way max in `RunCDCStreamMaintenance`); `src/kudu/cdc/cdc_service.cc`
  -- `cdc_checkpoint_persist_failures` counter (~360, incremented at
  `PersistCheckpoint` failure exit ~2515), inject flag
  `--cdc_inject_checkpoint_persist_failure` (~229).
- Tests: `CDCManagerTest.StalenessGuardAdvanceAttemptSuppressesRelease`
  (control: stuck consumer released; fix: advancing-but-unpersisted held; and
  ancient-attempt released), `CDCServiceTest.Checkpoint_PersistFailureCounterIncremented`.
  cdc_manager-test 35/35 and cdc_service-test 60/60 at the time of the fix.
- Extends DR-015; complements DR-018 (which only helps consumer-visible errors).

---

## DR-018 -- CDC error contract: retry classification + schema-version mismatch

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** Consumer error contract -- GetChanges

### Context

E1/E2 (P1, `06_consumer_error_contract.md`): CDC's 12-code error taxonomy
(DR-008) told a consumer *what* went wrong but not *what to do* -- each consumer
had to re-derive the per-code retry/re-snapshot policy, and a drift there means
either a busy-loop on a fatal error or a silent stall on a transient one.
Separately, Kudu ships schema-current-only decoding (DR-009): if an ALTER lands
that a consumer never saw, `GetChanges` would hand back records whose new columns
the consumer decodes as raw hex, silently corrupting the consumer's view with no
signal.

### Decision

Two additions, both backward-compatible (new optional proto fields):

- **E1 -- retry classification.** `CDCErrorPB` gains `is_retryable` (field 3) and
  `needs_resnapshot` (field 4). The server sets them on *every* CDC error via a
  single `ClassifyCDCErrorCode()` source of truth wired through `SetCDCError`
  (cdc_service.cc). Mutually exclusive: `is_retryable` = re-issue the same request
  after backoff (TABLET_NOT_LEADER/NOT_FOUND/NOT_RUNNING, SERVER_TOO_BUSY,
  NOT_AUTHORIZED, SCHEMA_VERSION_MISMATCH); `needs_resnapshot` = the WAL/history
  position is gone, discard progress and re-snapshot (WAL_EXPIRED,
  HISTORY_EXPIRED, STREAM_EXPIRED); both false = fatal/operator-action.

- **E2 -- SCHEMA_VERSION_MISMATCH (code 13).** `GetChangesRequestPB` gains an
  optional `schema_version` (field 9). When a consumer sets it and it is older
  than the tablet's current schema, `ReadChanges` returns SCHEMA_VERSION_MISMATCH
  (classified retryable, not re-snapshot) *before* adding any record, instead of
  shipping records the consumer would misdecode (cdc_service.cc:1446). The
  comparison baseline is the **applied** schema version
  (`tablet_metadata()->schema_version()`), not the committed-but-unapplied
  `running_schema_version`. Skipped when `need_schema_info=true` (the current
  schema is being prepended anyway) and when `schema_version` is unset (default:
  check disabled, preserving existing consumers and in-band-DDL consumers).

### Rationale

- One server-side classifier removes the per-consumer taxonomy duplication; the
  fields are advisory metadata on the existing error, so old consumers ignore
  them and new ones need no code-to-policy table.
- Applied-schema baseline is deliberate: pre-ALTER records still decode correctly
  against the older version the consumer holds, and an in-flight (committed but
  unapplied) ALTER has produced no records yet -- treating it as a mismatch would
  spuriously reject valid catch-up reads. This keeps the existing
  `SchemaVersion_CommittedUnappliedAlterStampsPreAlterVersion` stamping behavior
  intact.
- **vs. YB:** YB has no per-request schema-mismatch error from GetChanges -- it
  pushes schema-version maps to pollers via consumer heartbeat
  (`XClusterConsumer::UpdateSchemaVersions`) and negotiates drift out-of-band.
  Kudu adopts the *intent* (server is authoritative on schema version; signal the
  consumer to refresh rather than let it decode blind) as a simpler synchronous,
  request-scoped check, since Kudu has no equivalent consumer-heartbeat channel.

### Links

- Proto: `src/kudu/cdc/cdc.proto` -- `CDCErrorPB.is_retryable`/`needs_resnapshot`,
  `SCHEMA_VERSION_MISMATCH = 13`, `GetChangesRequestPB.schema_version`.
- Impl: `src/kudu/cdc/cdc_service.cc` -- `ClassifyCDCErrorCode`/`SetCDCError`
  (~696/~731); E2 emission (1446-1457).
- Tests: `cdc_service-test.cc` -- `ErrorContract_RetryableClassification`,
  `SchemaVersionMismatch_StaleConsumerVersionRejected`, and the resnapshot branch
  asserted in `GetChanges_WalGcedBelowFromOpIndexReturnsWalExpired`. Suite 59/59.
- Supersedes nothing; extends DR-008 (taxonomy) and DR-009 (schema-current-only).

---

## DR-017 -- CDC scan admission honors the server-wide soft-memory limit

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** Scan admission control -- heavy-scan memory safety

### Context

R2/G4 (P1): `TryAcquireScanSlot` (cdc_service.cc) gated a CDC scan on two things
only -- a concurrency cap (`--cdc_max_concurrent_scans`) and a CDC-*local* heap
budget (`--cdc_scan_mem_limit_bytes` vs. the CDC scan MemTracker). Neither sees
the rest of the process. A tserver already near its `--memory_limit_hard_bytes`
from writes/compactions/other reads would still admit a large `GetChanges` or
snapshot scan, because the CDC-local budget was nowhere near its own ceiling.
The scan's decode/build then piles onto process memory and can trip the hard
limit (which aborts RPCs indiscriminately). Kudu already exposes the idiomatic
guard for exactly this: `process_memory::SoftLimitExceeded()`, used by the write
path (`tablet_service.cc:1706`) to shed load before the hard limit. CDC scans --
the heaviest read the server serves -- were the one heavy path not consulting it.

### Decision

Add a server-wide soft-memory gate to `TryAcquireScanSlot`, evaluated after the
concurrency slot is taken and *before* the CDC-local budget reservation. If
`process_memory::SoftLimitExceeded()` is true (randomized rejection above the
soft limit, same as the write path), release the concurrency slot, increment a
new `cdc_scans_rejected_server_memory` counter, and return
`Status::ServiceUnavailable` (maps to SERVER_TOO_BUSY -- a retryable/back-off
signal for the consumer, not a re-snapshot). The check is OR'd with a test-only
injection flag `--cdc_inject_server_memory_pressure` (unsafe+runtime, following
the `cdc_inject_*` convention) because `SoftLimitExceeded` reads process-wide
GoogleOnceInit-cached limits plus randomization and cannot be toggled per test.

### Rationale

- **Layered, not redundant.** The CDC-local budget bounds *CDC's own* footprint
  (fairness across concurrent CDC scans); the soft-limit gate bounds CDC's
  contribution to *whole-process* pressure. A tserver can be under global
  pressure with CDC nowhere near its local budget -- only the process gate
  catches that. Ordered before the local reservation so a pressured server sheds
  without first reserving bytes it will immediately release.
- **Reuses the established mechanism.** Same call, same randomized-shedding
  semantics, same SERVER_TOO_BUSY contract as the write path -- consumers
  already back off and retry on SERVER_TOO_BUSY, so no new client behavior.
- **YB parallel.** YB's CDCService scans run under a per-tablet MemTracker
  parented to the server MemTracker; a scan allocation that would breach the
  root tracker is rejected by the tracker hierarchy itself. Kudu's CDC scan
  MemTracker is CDC-local (not parented to enforce the process ceiling), so the
  process-limit check is added explicitly to get the equivalent whole-server
  backpressure.
- **Not a hard-limit substitute.** The hard limit still exists as the last
  resort; this only moves the shed point earlier for the CDC path so the hard
  limit is not reached via CDC in the first place.

### Links

- `src/kudu/cdc/cdc_service.cc` -- `TryAcquireScanSlot` soft-limit gate,
  `cdc_scans_rejected_server_memory` counter, `cdc_inject_server_memory_pressure`.
- `src/kudu/cdc/cdc_service.h` -- `scans_rejected_server_memory_` member.
- `src/kudu/util/process_memory.cc:176-295` -- `SoftLimitExceeded` semantics.
- `src/kudu/tserver/tablet_service.cc:1706` -- write-path precedent.
- `CDC/analysis/05_admission_flags.md` (G4) -- the gap.
- Test: `cdc_service-test.cc` `Isolation_ServerMemoryPressureShedsAndRecovers`.

---

## DR-016 -- Backstop coverage via replica-level valve tests, not a cluster disk-full sim

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** Test strategy -- WAL force-release backstops

### Context

G2 (P1): the two P0-1 WAL-retention backstops in
`TabletReplica::GetRetentionIndexes()` -- the disk-pressure valve
(`--cdc_stop_retaining_min_disk_mb`) and the barrier-age ceiling
(`--cdc_max_wal_retention_secs`) -- had no regression test on the Kudu side. The
analysis (`02_retention_barrier.md` G2) noted untested WAL backstops have failed
silently in both YB and Kudu history, and disk exhaustion is the most common CDC
production incident. The analysis sketched cluster-level integration tests
(cdc_service-int-test / cdc_failover-itest style).

### Decision

Cover both backstops with deterministic tests at the `TabletReplica` level
(`tablet_replica-test.cc`), driving the real `GetRetentionIndexes()` +
`Log::GC()` path rather than standing up a multi-node cluster:

- **Disk-pressure:** set `--cdc_stop_retaining_min_disk_mb` to `INT64_MAX` so the
  measured free space is always below threshold -- the valve fires without
  needing to actually fill a disk.
- **Age ceiling:** add a test-only hook
  `set_cdc_barrier_last_advanced_micros_for_tests()` to backdate the
  barrier-advanced clock past the ceiling, so the dead-master backstop is
  exercised without a real multi-second sleep.

Each test asserts the observable contract of a fired valve: the
`cdc_barrier_forced_releases` counter increments (read via a new
`cdc_barrier_forced_releases_for_tests()` accessor), `for_durability` reverts
above the pinned barrier index (clamp released), and a subsequent `Log::GC()`
actually reclaims WAL segments.

### Rationale

- Real disk exhaustion is not reproducible deterministically in a unit/integration
  test; `GetSpaceInfo` returns true free space. The oversized-threshold trick is
  precisely how the production code path is meant to trip, so the test exercises
  the identical branch an operator's disk-full event would.
- The age ceiling is measured in whole seconds against wall-clock; a
  time-advancing hook keeps the test sub-second and flake-free (same philosophy
  as DR-015's `set_cdc_leader_ready_micros_for_tests`).
- Replica-level tests exercise the exact production code
  (`GetRetentionIndexes()` + `Log::GC()`) that owns the release decision, with far
  less surface than a cluster harness -- and pair with the existing T5/DR-014
  `TestCDCValveReleasesHistoryFloor`, which already covers the history-floor half
  of the disk-pressure release.
- Test hooks are additive and `_for_tests`-named; they change no production
  behavior.

### Links

- Tests: `src/kudu/tablet/tablet_replica-test.cc`
  `TestCDCDiskPressureValveReleasesWAL`, `TestCDCAgeCeilingValveReleasesWAL`
  (+ existing `TestCDCValveReleasesHistoryFloor`).
- Hooks: `src/kudu/tablet/tablet_replica.h`
  `cdc_barrier_forced_releases_for_tests()`,
  `set_cdc_barrier_last_advanced_micros_for_tests()`.
- Code under test: `src/kudu/tablet/tablet_replica.cc:894-1002`
  (`GetRetentionIndexes()` valve). Verified: tablet_replica-test 15/15.
- Analysis: `CDC/analysis/02_retention_barrier.md` (G2).

---

## DR-015 -- Staleness release honors a per-leadership grace period

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** Master maintenance loop -- max-staleness retention-barrier release

### Context

V4/CF-2 (P1). `RunCDCStreamMaintenance()` releases a tablet's retention barrier
once its durable checkpoint has not advanced for `--cdc_max_staleness_ms` -- the
upper bound on how long a non-advancing consumer may pin WAL/UNDO. Staleness was
measured purely from the persisted `last_checkpoint_advance_time_micros`. That
wall clock keeps ticking while the *master* is down. A master that recovers from
an outage longer than the staleness window would, on its very first maintenance
pass, find every not-recently-advanced stream "stale" and drop its barrier at
once -- punishing consumers for the master's own downtime and forcing needless
re-snapshots. This is the same failure the CF-2 checkpoint-persist race can
create: PersistCheckpoint is best-effort/silent (`cdc_service.cc:2363`), so a
consumer can be making real progress the master never recorded, then get
barrier-released the instant a new leader comes up "stale".

### Decision

Stamp a per-leadership grace floor `cdc_leader_ready_micros_` (atomic) in
`PrepareForLeadershipTask` just before publishing `leader_ready_term_`, and
measure staleness from `effective_advance = max(last_advance, leader_ready)`:

```
stale = staleness_micros > 0 && last_advance > 0 &&
        (now - max(last_advance, leader_ready)) > staleness_micros
```

A newly-ready leader therefore grants every stream one full staleness window of
grace before it may release on non-advancement; after the leader has itself been
up longer than the window, `max()` is dominated by `last_advance` again and the
guard behaves exactly as before -- a genuinely stuck consumer is still released
on schedule. The floor is per-leadership (re-stamped on every election), so a
flapping master does not accumulate immunity.

### Rationale

- Directly implements analysis option (b/c) in `03_checkpoint_state.md` (V4
  Scenario C): the release decision must not count master downtime against the
  consumer, but must still bound a truly idle consumer.
- Chosen over analysis option (a) -- surfacing a retriable error to the consumer
  on repeated PersistCheckpoint failure -- which is complementary, not a
  substitute, and is folded into T9 (error contract). The grace floor protects
  the *barrier* regardless of whether the consumer ever learns of the failure.
- Minimal + failover-safe: one atomic, stamped on the existing leadership-ready
  path; no new persistence, no schema change. A follower that never becomes
  leader never reads it.
- YB parallel: YB measures xCluster/CDC liveness against per-tablet peer state
  that is only trusted once the hosting peer is `LEADER` and caught up; the grace
  floor is the single-master-clock analogue -- "do not judge staleness until this
  authority has been in charge long enough to have observed progress."

### Links

- `src/kudu/master/catalog_manager.cc:1748` (stamp in `PrepareForLeadershipTask`),
  `:9050` (load), `:9104-9110` (`effective_advance`/`stale`).
- `src/kudu/master/catalog_manager.h:1743` (`cdc_leader_ready_micros_`), `:1632`
  (`set_cdc_leader_ready_micros_for_tests`).
- Test: `src/kudu/master/cdc_manager-test.cc`
  `StalenessGuardGracePeriodAfterLeaderReady` (grace retains; elapsed-window
  releases). Verified: cdc_manager-test 30/30.
- Analysis: `CDC/analysis/03_checkpoint_state.md` (CF-2, V4 Scenario C).

---

## DR-014 -- Force-release valve also releases the MVCC history floor

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** WAL/history retention safety valve (disk-pressure + dead-master backstops)

### Context

V1 (P1, degrades a P0). `TabletReplica::GetRetentionIndexes()` has two
force-release conditions that set `skip_cdc_clamp` and let `for_durability` revert
to its true Raft floor so WAL GC can proceed: the disk-pressure valve
(`--cdc_stop_retaining_min_disk_mb`) and the barrier-age ceiling
(`--cdc_max_wal_retention_secs`, the dead-master backstop). But `skip_cdc_clamp`
only released the **WAL** clamp -- it never touched the in-memory MVCC/UNDO
history floor (`Tablet::cdc_history_floor_`, set by the master's
`SetRetentionBarrier` push for FULL/snapshot streams).
`GetTabletAncientHistoryMark()` clamps the AHM to that floor, so during a
disk-full event WAL GC would resume while compaction / flush-UNDO GC stayed
pinned to the stale floor and could not reclaim rowset history -- the valve was
half-open, and disk kept filling from UNDO precisely when the valve was supposed
to relieve pressure.

### Decision

When either valve condition fires (`skip_cdc_clamp == true`), also release the
in-memory history floor: `shared_tablet()->SetCDCHistoryFloor(Timestamp(0))`
(`Timestamp(0)` is the "no floor" sentinel), guarded for a null live tablet. Done
inside the existing `if (skip_cdc_clamp)` block in `GetRetentionIndexes()`, right
next to the `cdc_barrier_forced_releases_` counter increment.

### Rationale

- **Both signals mean "stop retaining for CDC."** Disk pressure is an emergency
  that must free *all* CDC-pinned space (WAL and UNDO); the age ceiling means the
  master has stopped refreshing the barrier, so neither WAL nor history is being
  advanced. Releasing both together is correct for Kudu's model, where WAL and
  history staleness advance on the *same* master-push clock
  (`cdc_barrier_last_advanced_micros_`). YB reaches the same end state via
  *separate* WAL and history staleness clocks that each release their barrier
  independently (`tablet_peer.cc` `reset_cdc_retention_barriers_if_stale`); Kudu's
  single-clock equivalent is to release both when the clock goes stale.
- **Re-raise on recovery is automatic.** If the master later resumes and re-pushes
  the barrier, `SetRetentionBarrier` re-applies the floor. Under *sustained* disk
  pressure with a live master, the floor is re-cleared each GC pass, so UNDO is
  reclaimed opportunistically across passes -- the intended emergency behavior,
  not a leak.
- **Consistent consumer contract.** A FULL/snapshot consumer whose history is
  subsequently GC'd receives `HISTORY_EXPIRED`, mirroring the `WAL_EXPIRED` the
  WAL-side release already yields. The valve deliberately sacrifices the lagging
  consumer to save the server -- history now behaves like WAL.
- **Minimal + placed at the decision point.** The release lives exactly where
  `skip_cdc_clamp` is decided, so it fires for both conditions and cannot drift
  out of sync with the WAL release. `GetGCableDataSize()` also calls
  `GetRetentionIndexes()`; the extra atomic store there is a harmless idempotent
  no-op-or-release consistent with the valve already being open.

### Links

- Closes V1 (`00_SYNTHESIS.md`; `02_retention_barrier.md` G1,
  `tablet_replica.cc` `GetRetentionIndexes`; `tablet.cc:1564-1566`
  `GetTabletAncientHistoryMark`).
- `tablet/tablet_replica.cc` (`GetRetentionIndexes` history-floor release);
  `tablet/tablet_replica-test.cc` (`TestCDCValveReleasesHistoryFloor`).
- Verified: build clean; `tablet_replica-test` 13/13 (new
  `TestCDCValveReleasesHistoryFloor` asserts the floor survives with the valve
  closed and is cleared when the disk-pressure valve fires).

---

## DR-013 -- Table drop condemns referencing CDC streams

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** CDC stream lifecycle vs. table lifecycle (retention-barrier leak)

### Context

L2 (P0). Dropping a table left every CDC stream that referenced it stuck ACTIVE
forever. The stream's per-tablet checkpoint rows kept its retention barrier
pinned (`cdc_min_retained_op_index`), but the tablets were gone, so nothing ever
advanced or released it -- and the ACTIVE stream row itself was never reaped.
The result was an unbounded accumulation of dead ACTIVE streams and (for any
surviving replica) an un-droppable history floor. YB handles the equivalent case
by marking the whole xCluster stream `DELETING` as soon as any table it covers is
dropped, then letting its stream-reaper GC it (`catalog_manager.cc`
`MarkCDCStreamsForMetadataCleanup` / drop path).

### Decision

On the terminal table transition to REMOVED, condemn every ACTIVE CDC stream that
references the dropped table by marking it `DELETING`, then let the existing
DR-010 two-phase reap machinery RELEASE its barrier and remove it. Implemented as
`CatalogManager::MarkDeletingStreamsForDroppedTables()`:

- **Phase A (under `lock_`, shared):** snapshot each ACTIVE stream's refptr and,
  for each `table_id` it references, `FindPtrOrNull(table_ids_map_, table_id)`
  (nullptr if the table is already gone from the map).
- **Phase B (outside `lock_`):** a stream is condemned if any referenced table
  refptr is null OR a `TableMetadataLock` READ shows `is_deleted()` (== REMOVED,
  terminal). For each condemned stream take a `CDCStreamMetadataLock` WRITE,
  re-check `state == ACTIVE`, `set_state(DELETING)`,
  `sys_catalog_->WriteCDCStream(...)`, then `Commit()`. A failed durable write is
  logged (warning) and left for the next pass -- never fatal.

Two call sites:
- **Eager:** the last step of `CatalogManager::DeleteTable()` (the hard-delete /
  REMOVED path), so a drop condemns its streams immediately.
- **Backstop:** the first step of `RunCDCStreamMaintenance()`, *before*
  `ReapDeletedCDCStreams()`, so a stream condemned in a pass is also reaped in the
  same pass, and so failover / a failed eager write self-heals.

### Rationale

- **Hook only the terminal REMOVED transition, never soft-delete.** Routing is:
  `SoftDeleteTable` (state SOFT_DELETED, recallable via `RecallDeletedTable`) vs.
  `DeleteTable` (state REMOVED, terminal). Condemning on soft-delete would destroy
  a stream that a subsequent recall should have kept, so the hook lives only in
  `DeleteTable`. A soft-deleted table's stream stays ACTIVE (verified by
  `SoftDeleteTable_LeavesStreamActive`); it is condemned only if the table is
  later hard-deleted or its reservation expires into REMOVED.
- **Coarse per-stream semantics, matching YB.** If a stream references multiple
  tables, dropping *any one* condemns the whole stream. This mirrors YB and the
  analysis' G3 sketch. Kudu CDC streams are effectively single-table today, so
  the multi-table partial-drop case (condemn vs. narrow the stream to survivors)
  is noted and deferred rather than solved here.
- **Reuse DR-010, don't duplicate teardown.** Marking DELETING and delegating to
  the reap path means barrier RELEASE-before-removal and idempotency are already
  handled; this change only adds the *trigger*.
- **Backstop before reap** guarantees single-pass convergence and makes the
  feature robust to a leader change or a transient sys-catalog write failure
  during the eager hook.

### Links

- Closes L2 (`00_SYNTHESIS.md`; `07_master_lifecycle.md` L2, sketch G3).
- Builds on DR-010 (two-phase DELETING reap).
- `master/catalog_manager.{h,cc}` (`MarkDeletingStreamsForDroppedTables()`;
  eager call in `DeleteTable`; backstop in `RunCDCStreamMaintenance`).
- Verified: build clean; `cdc_manager-test` 29/29 (5 new:
  `DropTable_MarksReferencingStreamDeleting`,
  `DropTable_StreamReapedByMaintenance`, `SoftDeleteTable_LeavesStreamActive`,
  `DropUnrelatedTable_LeavesStreamActive`,
  `DropTable_BackstopMarksDeletingAfterReload`).

---

## DR-012 -- Dedicated CDC RPC service-pool queue

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** Cross-service resource isolation (CDC burst vs. Raft availability)

### Context

R1 (P0). The CDC service was registered through the single-argument
`RegisterService`, which builds its `ServicePool` with the shared
`--rpc_service_queue_length` (default 50) used by the tablet, admin, consensus,
and tablet-copy services. That queue is a single bounded inbox: a burst of CDC
`GetChanges` consumers can fill all 50 slots and the pool then starts rejecting
*every* service's incoming RPCs -- including Raft consensus -- with
`ERROR_SERVER_TOO_BUSY`, before any CDC-specific admission code
(`TryAcquireScanSlot`, the heap budget) runs. A CDC-consumer surge thus becomes a
cluster-wide consensus-availability event. YB avoids this by giving xCluster its
own `xcluster_svc_queue_length` (default 5000) service pool
(`tserver/tablet_server.cc:218`).

### Decision

Give the CDC service its own dedicated service-pool queue, sized by a new
`--cdc_svc_queue_length` flag (default 50, `advanced`), defined in
`tserver/tablet_server.cc` next to where CDC is registered.

Mechanism: add a generic `RegisterService(service, service_queue_length)`
overload at both `RpcServer` and `ServerBase`; the existing single-argument form
now delegates to it passing `options_.service_queue_length`, so there is exactly
one `ServicePool`-construction body and every other caller is byte-for-byte
unchanged. Only the CDC registration in `TabletServer::Start()` passes the new
flag. NB (audit 2026-08-31): the original text claimed the RPC worker thread
pool "stays shared". That was factually wrong about Kudu's RPC model:
`ServicePool::Init(num_threads)` (`rpc/service_pool.cc:88`) creates
`--rpc_num_service_threads` DEDICATED threads for each pool, each draining only
its own queue. So registering CDC as its own service pool isolates CDC at BOTH
the queue AND the worker-thread level -- the fix is strictly stronger than the
queue-only isolation originally described.

### Rationale

Isolation is achieved by any *separate* queue, independent of its depth: once CDC
has its own inbox, a CDC flood fills and rejects only *CDC* RPCs (exactly the
desired backpressure) while the shared queue that carries consensus stays clear.
The default is therefore set to 50 -- equal to the shared default -- so the
change is purely structural and behavior is unchanged until an operator tunes it;
we did not copy YB's 5000, which is a throughput/buffering choice that would also
raise the worst-case queued-CDC memory footprint. The flag lives in the
CDC-aware tserver TU rather than the generic `rpc_server.cc` to avoid a layering
inversion (the generic RPC layer should not name a specific service); the
overload it calls is fully generic and reusable by any future service that wants
its own queue. (Per the 2026-08-31 audit note above, CDC additionally receives
its own dedicated worker threads by virtue of being its own `ServicePool`, so no
CDC-scan flood can occupy the threads that serve consensus;
`--cdc_get_changes_free_rpc_ratio` remains an additional intra-CDC headroom
guard.)

### Links

- Closes R1 (`00_SYNTHESIS.md` Theme 2; `05_admission_flags.md` G1).
- `server/rpc_server.{h,cc}` (`RegisterService` overload + delegation);
  `server/server_base.{h,cc}` (matching overload); `tserver/tablet_server.cc`
  (`--cdc_svc_queue_length` flag + CDC registered with it).
- Verified: build clean (`krpc server_process tserver cdc`); `rpc-test` 252
  passed / 0 failed / 12 env-gated skips; `mini_tablet_server-test` 2/2 (tserver
  registers CDC via the new overload and starts cleanly).

---

## DR-011 -- Initial retention barrier and table validation at `CreateCDCStream`

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** Stream creation crash-safety (first-checkpoint WAL race) and input validation

### Context

Two creation-boundary gaps. **L5 (P0):** `CreateCDCStream` pushed no initial
barrier, so until the consumer's first `PersistCheckpoint` landed, every replica
had `cdc_min_retained_op_index = -1`. A leader crash in that window let normal
Raft GC discard ops the consumer had already stored as its checkpoint ->
`WAL_EXPIRED` despite a stored checkpoint (all *later* checkpoints are safe; the
superblock barrier at N-X guards them). **L6 (P1):** the method validated only
`table_ids_size() != 0`, so streams could be created over nonexistent, deleted,
or being-deleted tables -- inert garbage that pins WAL until it expires.

### Decision

**L5 -- durable initial checkpoint row, not just a best-effort RPC.** After the
stream row is persisted, write a per-tablet checkpoint row at `op_index = 0`
(retain from the log start) for every tablet of every table, with
`last_active_time_micros` / `last_checkpoint_advance_time_micros` set to now.
Then eagerly push the barrier (`min_retained_op_index = 0`,
`history_safe_time_micros = 0`, `barrier_seq = now`) to all replicas.

**L6 -- validate before persisting.** Snapshot the table refptrs under `lock_`,
then under each table's metadata lock reject: `is_deleted()` -> `NotFound`,
`is_soft_deleted()` -> `InvalidArgument`, `!is_running()` (PREPARING/UNKNOWN) ->
`ServiceUnavailable`.

### Rationale

The stronger L5 form (durable row + push) was chosen over a bare best-effort
barrier RPC because it makes **the master's maintenance loop the durable owner of
the barrier from its very first pass**: the `op_index=0` row is reloaded on any
failover, so a new leader keeps pinning WAL without ever having seen the
`CreateCDCStream` RPC, and it later releases the barrier through the normal
expiry/staleness path if the stream is never used (the activity timestamps let an
unused stream auto-expire instead of pinning WAL forever). The eager push just
removes the up-to-`--cdc_bg_scan_interval_ms` latency before retention takes
effect. `op_index=0` is superseded monotonically by the consumer's first real
checkpoint (`UpdateCDCCheckpoint` keeps `max(existing, op_index)`), so the
conservative floor costs at most one maintenance interval of extra WAL on an
actively-advancing stream. This composes cleanly with the two-phase delete
(DR-010): a created-then-deleted stream's row is reaped by the same path.

Residual window (documented, not closed): the initial-barrier *push* is still a
best-effort RPC, but if it is lost the maintenance loop re-derives and re-pushes
from the now-durable `op_index=0` row, and the consumer has not yet been told
anything durable -- strictly smaller than the pre-fix window, in which no barrier
existed anywhere.

### Links

- Closes CF-1 (`03_checkpoint_state.md` Scenario B) and G2 (`07_master_lifecycle.md`).
- Reuses the master-push barrier machinery [[DR-003]]; monotonic checkpoint from
  the existing `UpdateCDCCheckpoint`; composes with [[DR-010]].
- `master/catalog_manager.cc` (`CreateCDCStream`); `master/cdc_manager-test.cc`
  (`CreateStream_RejectsMissingTable`, `_RejectsSoftDeletedTable`,
  `_PushesInitialBarrier`, `_InitialBarrierReapedOnDelete`).
- Verified: `cdc_manager-test` 24/24. (One test-setup bug -- soft-delete request
  needs `table_name`, not just `table_id` -- was fixed during review.)

---

## DR-010 -- Two-phase durable `DELETING` stream lifecycle

- **Date:** 2026-08-29
- **Status:** Accepted
- **Area:** Stream delete crash-safety; orphaned-row + barrier-seq cleanup

### Context

`DeleteCDCStream` was single-phase: it removed the sys-catalog stream row and
per-tablet checkpoint rows durably, then fired best-effort fire-and-forget
barrier-RELEASE RPCs. A master crash between the durable removal and the RELEASE
landing left the new master with an empty, never-persisted `cdc_barriered_tablets_`,
so it sent no RELEASE; the tablet's superblock kept `cdc_min_retained_op_index`
**forever** -- a permanent WAL + MVCC-history leak (L1, P0). Two secondary leaks
rode on the same boundary: orphaned checkpoint rows reloaded on failover with no
owning stream (L3), and a same-microsecond SET/RELEASE `barrier_seq` collision that
could re-pin a just-released barrier (L4).

### Decision

Adopt YB's two-phase model (`DropXReplStreams` + `CleanUpDeletedXReplStreams`),
Kudu-idiomatically:

1. **Phase 1 (`DeleteCDCStream`):** persist `state = DELETING` to the sys-catalog
   stream row (durable, idempotent) and return success. The destructive work is
   NOT the source of truth on this path.
2. **Phase 2 (`ReapDeletedCDCStreams`, driven from `RunCDCStreamMaintenance`):**
   for each `DELETING` stream, send barrier RELEASE, then remove checkpoint rows,
   then remove the stream row -- **RELEASE-before-durable-removal**. The durable
   `DELETING` marker survives any failover; whichever master is leader finishes
   cleanup idempotently. Every crash window resolves on the next pass. (Closes L1.)
3. The reap sends the aggregate RELEASE **explicitly** (driven by the marker), not
   via the step-4 barriered-set diff -- because a fresh leader's
   `cdc_barriered_tablets_` is empty and the diff would never fire. `skip_barrier_update`
   is set only when a surviving stream still pins a shared tablet.
4. **L4:** stamp the RELEASE with `barrier_seq = now_micros + 1` so it strictly
   outranks a same-microsecond SET in the replica's last-writer-wins gate.
5. **L3:** checkpoint rows with no owning stream row are treated as implicit
   deletions and collected by the same reap.
6. `DELETING` streams are hidden from `ListCDCStreams`/`GetCDCStreamInfo` and
   excluded from barrier computation (the forward-looking ACTIVE guard is now live).

### Rationale

The decisive property is that **the crash-safe boundary must be a durable state
transition, not an in-flight RPC.** Kudu's retention barrier lives in the tablet
superblock (durable) but the knowledge of "this stream is being deleted" lived
only in the fire-and-forget RPC and transient master memory. Persisting `DELETING`
first turns delete into a resumable, idempotent operation owned by whatever master
is leader -- exactly how YB avoids the failover window -- while reusing Kudu's
existing master-push barrier machinery (DR-003) rather than YB's cdc_state-driven
self-release. Doing the RELEASE before removing the marker guarantees no window
exists where the barrier is pinned but nothing records that it must be released.

### Links

- Builds on the master-push barrier model [[DR-003]]; the ACTIVE guard was the
  forward-looking hook noted in `07_master_lifecycle.md` (G1) and `06` (F-08).
- `master/catalog_manager.cc` (`DeleteCDCStream`, `ReapDeletedCDCStreams`,
  `RunCDCStreamMaintenance`, `ListCDCStreams`), `master/catalog_manager.h:1125`.
- YB: `xrepl_catalog_manager.cc` (`DropXReplStreams` ~2151-2153, `CleanUpDeletedXReplStreams` ~3794-3870).
- Verified: `cdc_manager-test` 20/20 (phase-1 marking, idempotency, failover-resumed
  reap, orphaned-row reap, survivor isolation).

---

## DR-009 -- Kudu-specific operational surface

- **Date:** 2026-08-26
- **Status:** Accepted (schema-current-only is a deferred gap, see below)
- **Area:** Stream lifecycle, consumer/tooling, authorization, schema handling

### Context

A cluster of smaller choices that follow from Kudu's storage engine and tree, not
from porting YB behavior. They are grouped here because none is large enough to
warrant its own entry, but together they define how CDC actually operates.

### Decision

1. **Range-partition-drop cleanup, not split-lineage tracking.** Kudu tablets do
   not online-split, so there is no parent/child checkpoint lineage to carry. When
   a range partition is dropped, the maintenance loop reaps its checkpoint rows
   (`catalog_manager.cc:8790-8855`). YB's `cdc_state` child-tablet inheritance has
   no analog and is deliberately not built.
2. **In-tree consumer and CLI.** A `CDCConsumer` ships in-tree
   (`cdc/cdc_consumer.h:110`) alongside the `kudu cdc` admin tools
   (`tools/tool_action_cdc.cc:548-568`), rather than leaving consumption entirely
   to external connectors.
3. **Dedicated authorization gate.** CDC RPCs are gated by
   `AuthorizeCDCTableOrRespond` (`cdc_service.cc:423-460`) behind
   `--cdc_enforce_access_control` (defined `cdc_service.cc:307`), reusing Kudu's
   existing table-level authz rather than inventing a CDC-specific scheme.
4. **Schema-current-only (deferred gap D3).** The service resolves rows against the
   tablet's current schema (`cdc_service.cc:1105-1113`); it does not reconstruct a
   historical schema for a change emitted under an older version. Accepted as a
   known limitation for now.

### Rationale

Each item is the minimal thing that Kudu's model needs and no more. The absence of
split lineage is a direct consequence of Kudu not splitting tablets; shipping the
consumer/CLI in-tree matches how Kudu ships its other subsystems; the authz gate
reuses existing machinery; schema-current-only is a scoped-out gap, not an
oversight, tracked in `gaps.md` (D3).

### Links

- `gaps.md` D3 (schema-current-only).
- `catalog_manager.cc:8790-8855`, `cdc_service.cc:423-460,307,1105-1113`,
  `cdc/cdc_consumer.h:110`, `tools/tool_action_cdc.cc:548-568`.

---

## DR-008 -- 3-layer admission control and a Kudu-original error taxonomy

- **Date:** 2026-08-26
- **Status:** Accepted
- **Area:** Heavy-scan admission control; client-facing error contract

### Context

CDC GetChanges calls can trigger heavy MVCC/UNDO scans (before-image, snapshot
reads). The service must protect the tserver from overload and must tell clients
precisely why a call failed so they can react correctly (retry, reseek, re-bootstrap).

### Decision

Ship a **3-layer admission control** stack and a **12-code error taxonomy**, both
authored for Kudu rather than mapped from YB.

- Admission control: (1) an RPC-queue-overload ratio check
  (`cdc_service.cc:703-732`), (2) a heavy-scan concurrency cap via
  `TryAcquireScanSlot` / `--cdc_max_concurrent_scans` (`cdc_service.cc:588-617`),
  and (3) a heap budget enforced by a `cdc_scans` MemTracker
  (`cdc_service.cc:479-482,602-615`).
- Error taxonomy: 12 codes in `cdc.proto:32-88`, including CDC-specific ones that
  YB does not distinguish, e.g. reactive `STREAM_EXPIRED` vs `WAL_EXPIRED`
  disambiguation (`cdc_service.cc:1197-1207,2157-2181`). Only `TABLET_NOT_FOUND`
  and `TABLET_NOT_RUNNING` overlap with YB's 14-code set
  (`cdc_service.proto:82-103`).

### Rationale

YB bounds heavy scans with a single semaphore plus an xCluster-only rate limiter
(`cdc_service.cc:2154-2156`). Kudu's three orthogonal caps each target a distinct
failure axis -- queue pressure, scan concurrency, and heap -- so one being generous
does not defeat another. The bespoke error taxonomy exists because a client needs
to tell "your stream was expired by policy" (STREAM_EXPIRED) from "the WAL you
asked for was GC'd" (WAL_EXPIRED); collapsing them, as a naive port would, loses
the signal the consumer needs to decide between reseek and re-bootstrap.

### Links

- Bounds the same burst-pressure surface as [[DR-001]] (send-rate limiter descoped).
- `cdc_service.cc:479-482,588-617,602-615,703-732,1197-1207,2157-2181`;
  `cdc.proto:32-88`; YB `cdc_service.proto:82-103`, `cdc_service.cc:2154-2156`.

---

## DR-007 -- Checkpoint ACK-before-persist (at-least-once)

- **Date:** 2026-08-26
- **Status:** Accepted
- **Area:** Checkpoint durability semantics

### Context

When a consumer acks a checkpoint, the service must both (a) keep serving and
(b) durably record the new resume point. The order of "persist" vs "respond
success" fixes the delivery guarantee.

### Decision

Kudu **acks the consumer first, then persists** the checkpoint. On GetChanges the
service syncs the log anchor (`cdc_service.cc:958`), responds success
(`cdc_service.cc:1001`), and only then writes the checkpoint row
(`cdc_service.cc:1003-1010`), throttled by
`--cdc_checkpoint_persist_interval_ms` (`cdc_service.cc:273-276`). This yields
**at-least-once** delivery.

### Rationale

YB persists before acking (persist-before-ACK). Kudu's inversion is deliberate: the
anchor is synced before the response, so WAL cannot be GC'd out from under an
un-persisted checkpoint -- retention stays safe regardless of persist order. Given
that, acking first removes the persist write from the client-latency path, and the
worst case is a consumer replaying a bounded suffix after a crash (at-least-once),
which CDC consumers must already tolerate. Choosing at-least-once over
exactly-once-persist is a latency/throughput win with no retention-safety cost,
precisely because retention correctness is local (see [[DR-002]]).

### Links

- Depends on the local, fail-safe retention model in [[DR-002]] and [[DR-003]].
- `cdc_service.cc:958,1001,1003-1010,273-276`.

---

## DR-006 -- Server-driven in-memory snapshot session

- **Date:** 2026-08-26
- **Status:** Accepted
- **Area:** Initial snapshot (bootstrap) protocol

### Context

Before streaming WAL changes, a consumer must read a consistent snapshot of the
table. The snapshot can span many GetChanges calls, so its cursor and consistency
point must survive across calls -- the question is where that state lives.

### Decision

Hold snapshot state in an **in-memory, server-driven session** on the serving
tserver, not in a master-persisted record.

- The session is established server-side (`cdc_service.cc:1653-1708`) under a
  `snapshot_start_lock` mutex (`cdc_service.cc:1619`).
- If the session is lost (leader change, eviction), the service returns
  `SNAPSHOT_SESSION_LOST` (code 11, `cdc.proto:79`; raised
  `cdc_service.cc:1644-1651`) so the consumer re-bootstraps cleanly.
- The **resume key is server-authoritative** (`cdc_service.cc:1724-1732`): the
  server dictates the next read point rather than trusting a client-supplied cursor.
- A **post-read leader-term recheck** (`cdc_service.cc:1535-1550`) rejects results
  produced across a silent leadership change; handoff at `cdc_service.cc:1831-1838`.
- Schema version for snapshot rows is derived from the WAL, not assumed
  (E9 fix, `cdc_service.cc:1288-1313`).

### Rationale

A master-persisted snapshot cursor (YB-style bookkeeping) would add write traffic
and a correctness dependency on that store for what is inherently transient,
single-consumer, single-leader state. Keeping it in memory on the leader makes the
failure model explicit and safe: any disruption surfaces as SNAPSHOT_SESSION_LOST
and a re-bootstrap, never as silently divergent data. The server-authoritative
resume key and leader-term recheck close the window where a client cursor or a
stale leader could hand back inconsistent rows.

### Links

- `cdc_service.cc:1535-1550,1619,1644-1651,1653-1708,1724-1732,1831-1838,1288-1313`;
  `cdc.proto:79`.

---

## DR-005 -- Committed-only transactions via in-call WAL-scan buffering

- **Date:** 2026-08-26
- **Status:** Accepted
- **Area:** Multi-op transaction emission

### Context

Kudu multi-row transactions land in the WAL as participant ops bracketed by
control entries, and must be emitted to CDC only if the transaction commits, in
commit order -- without a separate intents/uncommitted store.

### Decision

Buffer a transaction's ops **in-call, by scanning the WAL**, and emit or drop at
the terminal control op.

- Ops are buffered per-txn during the scan (`txn_buffers`,
  `cdc_service.cc:1327-1473`); on `FINALIZE_COMMIT` they are emitted, on `ABORT`
  dropped.
- Emitted changes are stamped at the **commit timestamp**, not the write timestamp
  (`cdc_service.cc:1415-1446`).
- The retention/read floor is pinned to the **oldest open transaction**
  (`open_min`, `cdc_service.cc:1490-1514`) so an in-flight txn's ops are not GC'd
  before it commits.
- A transaction whose buffered span exceeds `--cdc_max_transaction_span_bytes`
  (512MB) escalates to `TRANSACTION_TOO_LARGE` (code 12, `cdc.proto:87`; raised
  `cdc_service.cc:1167-1243`) instead of blowing the heap.

### Rationale

YB reads committed transaction data from a persistent IntentsDB. Kudu has no such
store, and building one purely for CDC would be a large, permanent structure for a
read-time need. Scanning the WAL in-call and buffering reconstructs committed-only,
commit-ordered output from data Kudu already persists. The `open_min` pin and the
commit-timestamp stamping make the result correct; the `TRANSACTION_TOO_LARGE`
escalation is the deliberate safety valve that keeps the "buffer in memory" strategy
from becoming an OOM vector, converting an unbounded buffer into an explicit,
client-visible error.

### Links

- `cdc_service.cc:1167-1243,1327-1473,1415-1446,1490-1514`; `cdc.proto:87`.

---

## DR-004 -- Before-image via MVCC/UNDO time-travel, strict HISTORY_EXPIRED

- **Date:** 2026-08-26
- **Status:** Accepted
- **Area:** Before-image (pre-update row state) reconstruction

### Context

CDC consumers need the row's prior value on UPDATE/DELETE (the "before image").
Kudu stores history as MVCC/UNDO deltas subject to history GC, not as separate
intents.

### Decision

Reconstruct the before-image with a **two-pass MVCC/UNDO time-travel scan**, guard
it against GC'd history, and when the history is gone **always fail with
HISTORY_EXPIRED -- never emit a guessed or null before-image.**

- Reconstruction and its guards: `cdc_util.cc:443,513-527,655,675`.
- When history required for a correct before-image has been GC'd, the service
  returns the row state it does have and does not silently fabricate
  (E1 fix, `cdc_service.cc:1375-1395,1421-1438`).
- Two Kudu-original history guards back this: a persisted `cdc_history_floor_`
  (`tablet.h:937`) and a monotonic `history_gc_water_mark_` (`tablet.h:947`,
  updated via `RecordHistoryGcWaterMark`).

### Rationale

YB offers `cdc_send_null_before_image_if_not_exists`
(`cdcsdk_producer.cc:584-590`), an escape hatch that emits a null before-image when
history is missing. Kudu **deliberately has no such flag**. A before-image that is
silently null or reconstructed from insufficient history is worse than an error: a
consumer maintaining a downstream copy would corrupt it and not know. Failing
loudly with HISTORY_EXPIRED forces the consumer to re-bootstrap from a snapshot,
which is the only correct recovery. The persisted floor plus monotonic water mark
ensure the guard cannot regress even across restarts or races.

### Links

- `cdc_util.cc:443,513-527,655,675`; `cdc_service.cc:1375-1395,1421-1438`;
  `tablet.h:937,947`. YB escape hatch: `cdcsdk_producer.cc:584-590`.

---

## DR-003 -- Master-push retention-barrier propagation

- **Date:** 2026-08-26
- **Status:** Accepted
- **Area:** WAL/history retention-barrier propagation

### Context

Every replica of a CDC'd tablet must know how much WAL/history to retain. The
barrier must reach all replicas (not just the leader) and must be recomputed as
consumers advance.

### Decision

The **master pushes** the retention barrier to all replicas via
`UpdateCDCRetentionBarrier`, rather than each tserver pulling it from a state table.

- The master's `RunCDCStreamMaintenance` recomputes per-tablet minima and fans the
  barrier out to all peers (`catalog_manager.cc:8675,8687-8920`); the replica sets
  it in `SetRetentionBarrier` (`cdc_service.cc:1887`).
- Because the push is asynchronous, a last-writer-wins sequence gate
  `barrier_last_seq_` (`cdc_service.cc:1931-1939`) rejects stale/out-of-order pushes
  -- a Kudu-original mechanism forced by the push model.
- The barrier reuses the existing `LogAnchorRegistry` (`cdc_service.cc:1977`) rather
  than a standalone atomic (YB uses a dedicated atomic, `log.h:769`).

### Rationale

YB tservers pull from `cdc_state` in `UpdatePeersAndMetrics`, which is why an
unavailable `cdc_state` becomes a correctness cliff there (see [[DR-002]]). Pushing
from the master means the barrier reaches every replica through the same control
plane that already fans out tablet operations, and -- critically -- if the master
maintenance loop stalls, replicas simply keep their last barrier and **fail toward
holding WAL** (safe), never toward releasing it. The sequence gate is the necessary
cost of going async: it makes reordered pushes harmless. Reusing LogAnchorRegistry
avoids duplicating anchor machinery.

### Links

- The push model is why retention is local and fail-safe, the core of [[DR-002]];
  its ordering guarantees underpin [[DR-007]].
- `catalog_manager.cc:8675,8687-8920`; `cdc_service.cc:1887,1931-1939,1977`.
  YB: `log.h:769`.

---

## DR-002 -- Defer the distributed `cdc_state`-style checkpoint table

- **Date:** 2026-08-26
- **Status:** Deferred (revisit when the master's single sys-catalog tablet write
  or scan throughput becomes the ceiling on a very wide fleet)
- **Area:** Checkpoint store scaling

### Context

YB's checkpoint store scales on three stacked levers: (1) a dedicated distributed
hash-sharded `cdc_state` table, (2) per-`(tablet_id, stream_id)` row keying, and
(3) a ~15s persist rate-limit. Kudu has implemented levers 2 and 3 (per-`(stream,
tablet)` sys-catalog rows in `CDC_TABLET_CHECKPOINT`, and
`--cdc_checkpoint_persist_interval_ms`=15s). Lever 1 -- moving the store off the
single master sys-catalog tablet onto a distributed, tserver-hosted table -- is
the remaining, deferred piece.

Two feasibility facts established this session:

- Kudu *can* build a distributed, tserver-hosted, Kudu-internal system table:
  the transaction-status table `kudu_system.kudu_transactions`
  (`TableTypePB::TXN_STATUS_TABLE`, `common/common.proto:523-528`) is exactly
  that pattern -- created via the standard `KuduTableCreator` API, tablets served
  by tservers, standard Raft. A `cdc_state` table would add a `CDC_STATE_TABLE`
  type and follow it. So lever 1 is feasible, not architecturally blocked.
- The checkpoint row key was intentionally chosen (`stream_id + '\0' + tablet_id`,
  `sys_catalog.cc:1062-1073`) so the migration to a two-column PK is clean; the
  design docs anticipated it (`design.md:479-482`,
  `dev_docs/CDC_IMPLEMENTATION_PLAN.md:160-165,335`).

### Decision

Do not build the distributed `cdc_state` table now. Keep checkpoints in the
single master sys-catalog tablet. Revisit only when the master's *aggregate*
checkpoint write rate or the 60s maintenance scan is demonstrably the bottleneck
on a very wide fleet.

### Rationale

The decisive point is that **the distributed form is a pure horizontal-scale
lever for Kudu, whereas in YB it is a correctness dependency.** Kudu's need is
genuinely weaker than YB's, for two reasons that came out of a code-grounded
comparison:

1. **Retention correctness is local and fails safe in Kudu; it depends on the
   store's availability in YB.**
   - Both systems enforce WAL GC off a value in the local tablet superblock
     (Kudu: `cdc_min_retained_op_index` / `cdc_history_safe_time_micros`,
     `metadata.proto:202,207`, read by `GetRetentionIndexes` at
     `tablet_replica.cc:767-783`; YB: `cdc_min_replicated_index`,
     superblock field 26, seeded into the `Log` atomic). In the *instantaneous*
     GC decision the two are symmetric.
   - The difference is the periodic recompute loop's failure mode. YB's
     `UpdatePeersAndMetrics` scans `cdc_state` every 60s on every tserver to
     recompute the barrier; if `cdc_state` is unreadable, after
     `cdc_min_replicated_index_considered_stale_secs` (30 min) the barrier
     **resets to `INT64_MAX`** (`tablet_peer.cc:900`) -- GC runs and unread WAL
     is lost. YB therefore *needs* `cdc_state` distributed + HA to avoid a
     correctness cliff.
   - Kudu's equivalent loop is the master's `RunCDCStreamMaintenance`
     (`catalog_manager.cc:8687-8920`). If it stalls, the superblock barriers
     simply persist -- Kudu fails **toward holding WAL** (safe/conservative),
     not toward release. So the store's availability/throughput is a freshness
     and scale concern, never a correctness one.

2. **Same write rate, just concentrated -- and already capped.** Steady state is
   O(tablets x streams / persist-interval) either way. YB spreads it across the
   fleet; Kudu funnels it to one master tablet, but lever 3 throttles it to that
   same rate. For 1000 tablets x 10 streams that is ~667 small UPSERTs/s to one
   Raft tablet -- within a single Kudu tablet's capacity, uncomfortable only at
   much wider fleets.

Note also the structural symmetry worth remembering: **the master sys-catalog
checkpoint store IS Kudu's `cdc_state` analog** -- the store the periodic
maintenance loop reads to recompute per-tablet min barriers and push them to
replicas. So this is not "a need YB has that Kudu lacks"; it is "the same
structural role, centralized on the master, where the distributed form buys scale
but -- unlike YB -- not correctness."

### Links

- `design.md:479-482`; `dev_docs/CDC_IMPLEMENTATION_PLAN.md:160-165,335`.
- Precedent: `common/common.proto:523-528`, `transactions/txn_status_tablet.cc`,
  `transactions/txn_system_client.cc`.

---

## DR-001 -- Descope the CDC send-rate limiter

- **Date:** 2026-08-26
- **Status:** Descoped (revisit only if sustained aggregate CDC egress is shown
  to cause real network/CPU pressure that the existing caps do not contain)
- **Area:** Heavy-scan admission control

### Context

YB caps CDC outbound bandwidth with `xcluster_get_changes_max_send_rate_mbps`
(100MB/s). Kudu has no equivalent sustained-throughput throttle. Kudu does bound
burst pressure three other ways, all shipping enabled: heavy-scan concurrency
(`--cdc_max_concurrent_scans`=8), per-response size
(`--cdc_max_bytes_per_response`=8MB), and a heap budget
(`--cdc_scan_mem_limit_bytes`=256MB).

### Decision

Do not implement a send-rate limiter.

### Rationale

The concurrency + response-size + heap caps already bound burst pressure, which
is the failure mode that matters (OOM / overload). A sustained aggregate-throughput
throttle is a smoothing knob, not a safety blocker, and was always optional in
this port. Adding a knob with no demonstrated need is not worth the surface area.

### Links
