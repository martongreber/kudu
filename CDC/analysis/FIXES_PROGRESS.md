# CDC Prod-Readiness Fixes -- Progress Tracker

> Driving the re-prioritized backlog from `00_SYNTHESIS.md` to completion,
> sequentially (one subagent per unit of work). Each change is YB-informed,
> built, and tested before it is marked done. Notable design choices are
> recorded in `../design_decisions.md` (DR-010+).
>
> Status legend: TODO / IN-PROGRESS / DONE / DEFERRED (with reason).
> Started 2026-08-29.

## Order of work

| # | Items | Sev | Title | Status | DR | Notes |
|---|-------|-----|-------|--------|----|-------|
| T1  | L1,L3,L4 | P0 | Two-phase DELETING stream lifecycle (crash-safe delete + orphan reap + seq collision) | DONE | DR-010 | cdc_manager-test 20/20 |
| T2  | L5,L6    | P0 | Initial retention barrier at CreateCDCStream + table existence/state validation | DONE | DR-011 | cdc_manager-test 24/24 |
| T3  | R1       | P0 | Dedicated CDC RPC service queue (cdc_svc_queue_length) | DONE | DR-012 | VERIFIED: cdc_service-test CDCServiceDedicatedQueueTest PASS (test added by orchestrator); full suite 57/57 |
| T4  | L2       | P1 | Auto-mark streams DELETING when their table is dropped | DONE | DR-013 | VERIFIED: cdc_manager-test 30/30 (DropTable_* +SoftDelete/Unrelated) |
| T5  | V1       | P1 | Release MVCC history floor when disk/age valve fires | DONE | DR-014 | VERIFIED: tablet_replica-test 15/15 (TestCDCValveReleasesHistoryFloor) |
| T6  | V4       | P1 | Do not release barrier on staleness while PersistCheckpoint is failing | DONE | DR-015 | VERIFIED: cdc_manager-test 30/30 (StalenessGuardGracePeriodAfterLeaderReady) |
| T7  | V2       | P1 | Integration tests for both P0-1 backstops (disk-pressure + age ceiling) | DONE | DR-016 | VERIFIED: tablet_replica-test 15/15 (Disk+Age ceiling valve tests) |
| T8  | R2       | P1 | Honor server soft-memory limit in CDC scan admission | DONE | DR-017 | VERIFIED: cdc_service-test 57/57 (Isolation_ServerMemoryPressureShedsAndRecovers) |
| T9  | E1,E2    | P1 | Error contract: is_retryable/needs_resnapshot fields + SCHEMA_VERSION_MISMATCH | DONE | DR-018 | VERIFIED: cdc_service-test 59/59 (ErrorContract_RetryableClassification, SchemaVersionMismatch_StaleConsumerVersionRejected, +resnapshot branch). Completed by gated T9-only fork. |
| T10 | O1,O2    | P1 | Metrics: cdc_bytes_sent + cdc_stream_time_to_expiry_micros | TODO (NEXT) | | Fork started then stopped by user mid-exploration; NO edits on disk. Resume here. |
| T11 | R3,R5,R6 | P2 | Deadlines in WAL-read + txn-escalation loops; MemTracker on CHANGE-mode read | TODO | | |
| T12 | R4,R7    | P2 | Record-count response cap; decoded-heap true-up | TODO | | |
| T13 | O3       | P2 | GetChanges response-size histogram (applied-lag: architectural, assess) | TODO | | |
| T14 | C1,C2,C3 | P2 | Consumer: supervised restart/auto-resnapshot, consume have_more_records, backoff jitter | TODO | | |
| T15 | G3,CF-3,R8 | P3 | Master RPC timeout flag; active-time interval; log-cache reuse (assess); dead-code | TODO | | |

## Prod-grade audit (2026-08-31)

Sequential re-audit of the 9 DONE items (T1-T9 / DR-010..DR-018), one subagent per
fix: walk the code for correctness, compare against YugabyteDB, and verify tests
are present, non-vacuous, and passing -- fixing bugs and thin coverage in place.

Combined-tree final verification (all audit fixes merged): build clean;
**cdc_manager-test 34/34, cdc_service-test 59/59, tablet_replica-test 17/17.**

> **2026-09-01 follow-up:** the four "Left for user" residuals below were then
> fixed to prod grade (DR-019..DR-022). Re-verified combined tree: build clean;
> **cdc_manager-test 37/37, cdc_service-test 60/60, tablet_replica-test 17/17.**

| DR | Audit verdict | Correctness bug fixed | Tests added | Left for user |
|----|---------------|-----------------------|-------------|---------------|
| DR-010 | FIXED | `RemoveCDCTabletCheckpoint` used `DELETE` (errors on missing row -> reap could stick, zombie DELETING stream within one master lifetime); -> `DELETE_IGNORE` (`sys_catalog.cc`) | `Reap_IdempotentAcrossPartialCompletion` | F-2: ~ms crash window between RELEASE dispatch and durable stream-row removal can permanently pin WAL/MVCC (architectural, master-push vs YB self-release) |
| DR-011 | FIXED | none (L5/L6 correct) | `CreateStream_RejectsRemovedTable` (REMOVED-in-map branch) | partial checkpoint-write failure leaves ACTIVE row, self-heals in ~4h via staleness -- accept or harden |
| DR-012 | FIXED | doc/comment error: "workers stay shared" is false -- each ServicePool has dedicated threads (isolation is STRONGER than documented); corrected comments + this doc | (structural test already sufficient) | none |
| DR-013 | FIXED | none | `SoftDeleteThenRecall_KeepsStreamActive` | multi-table stream: any dropped table condemns whole stream (safe, coarse; YB does per-table `DELETING_METADATA`) |
| DR-014 | FIXED | none | `TestCDCAgeCeilingValveReleasesHistoryFloor` (age-ceiling half was untested) | none |
| DR-015 | FIXED | none (atomic publication via ScopedLeaderSharedLock mutex chain is sound) | `StalenessGuardGraceFloorReStampedOnEachLeaderTerm` | none |
| DR-016 | FIXED | none | `TestCDCValveNoSpuriousRelease` (negative case); tightened 2 `ASSERT_LE`->`ASSERT_EQ`; valve tests proven non-vacuous by neutering | none |
| DR-017 | FIXED | none (no slot leak; correct SERVER_TOO_BUSY retryable-not-resnapshot) | `is_retryable`/`needs_resnapshot` wire asserts + `SCOPED_CLEANUP` for inject flag | none |
| DR-018 | SOLID | none (all 13 codes classified correctly, single choke point, E2 applied-baseline + fires-before-record, proto back-compat safe) | TABLET_NOT_LEADER + consumer-ahead + SNAPSHOT_SESSION_LOST fatal asserts | NOT_AUTHORIZED classifier/counter is dead code (auth fails at RPC level); harmless, leave |

All audit changes are uncommitted on branch `cdc`; no remotes touched.

## Log

(newest first)

- **2026-09-01 AUDIT-FIX PASS DONE (DR-019..DR-022).** Fixed the four residuals
  the 2026-08-31 audit table left "for user", one focused subagent each, prod
  grade, decisions recorded in `../design_decisions.md`:
  - **DR-019 (CF-2 component B, data-safety):** the master-UP-but-persist-failing
    half of CF-2. Consumer gets SUCCESS before the async master persist; a silent
    `WriteCDCTabletCheckpoint` failure left `last_advance` stale and, past the
    staleness window, released an actively-advancing consumer's barrier. Fix: an
    in-memory per-(stream,tablet) `last_checkpoint_advance_attempt_micros_` stamped
    on every forward-progress attempt *before* the write; staleness guard now uses
    `max(last_advance, leader_ready, advance_attempt)`, so an advancing-but-
    unpersisted stream is held while a truly-stuck one still releases. Stopped
    silently discarding persist failures -> new `cdc_checkpoint_persist_failures`
    counter. Extends DR-015. `catalog_manager.{h,cc}`, `cdc_service.{h,cc}`,
    cdc_manager-test + cdc_service-test.
  - **DR-020 (F-2, crash boundary):** closed the DR-010 reap window where a master
    crash after durable stream-row removal but before the async RELEASE landed
    left a tablet pinning WAL/MVCC permanently. Reap is now two-pass: Pass A
    dispatches RELEASE and defers all row removal until every task is terminal
    (Pass B), so a crash can no longer precede a landed RELEASE; failover re-runs
    Pass A off the intact DELETING row. Residual narrowed to "tserver offline >
    --unresponsive_ts_rpc_timeout_ms (~10 min) across the reap". `catalog_manager.{h,cc}`.
  - **DR-021 (create-side zombie):** on a mid-fanout checkpoint-write failure at
    CreateCDCStream, best-effort mark the new stream `DELETING` so the two-phase
    reap cleans it fast instead of the ~4h staleness self-heal. Hardens DR-011.
    `catalog_manager.cc`.
  - **DR-022 (queue default):** raised `--cdc_svc_queue_length` default 50->5000
    (YB `xcluster_svc_queue_length` parity); the pool is fully isolated (own queue
    + own threads) so this only buffers more before shedding. `tablet_server.cc`.
  All uncommitted on branch `cdc`; no remotes touched. Combined-tree re-verify:
  cdc_manager-test 37/37, cdc_service-test 60/60, tablet_replica-test 17/17.

- **2026-08-29 PAUSED for the day after T9.** T1-T9 all DONE and orchestrator-
  verified (DR-010..DR-018). T10 (metrics O1/O2) fork was dispatched then stopped
  by user during its exploration phase -- confirmed NO edits reached disk (no
  `cdc_bytes_sent`/`time_to_expiry` symbols present), so the tree is clean at the
  T9 state. RESUME TOMORROW AT T10: re-dispatch a gated (T10-only) fork reading
  `04_metrics_observability.md` O1+O2 (NOT O3 -- that is T13). Remaining backlog:
  T10 (P1), T11-T14 (P2), T15 (P3). Nothing is committed -- all work is uncommitted
  on branch `cdc` (17 files, ~2362 insertions), no remotes touched.

- **2026-08-29 T9 DONE (DR-018).** Gated (T9-only) fork completed the error
  contract and STOPPED as directed (no overrun). E2 emission wired in ReadChanges
  (cdc_service.cc:1446): a consumer declaring a `schema_version` older than the
  tablet's APPLIED schema gets SCHEMA_VERSION_MISMATCH (retryable) before any
  record, skipped when need_schema_info=true or the field is unset. E1
  (is_retryable/needs_resnapshot) already wired; fork added the missing tests.
  Baseline = applied (not running) schema version, preserving the
  committed-unapplied stamping test. Orchestrator verified: build clean,
  cdc_service-test 59/59; E2 emission + baseline confirmed by inspection.

- **2026-08-29 ORCHESTRATOR NOTE -- T3-T9 fork overran its mandate; verified under
  "revert-failures" policy.** The fork dispatched for T3 (RPC queue) inherited the
  full backlog and ran straight through T3-T8 (writing DR-012..DR-017) plus a
  partial T9 before hitting its 200-turn limit -- bypassing the sequential,
  verify-each gate. Per user decision "verify, revert failures", the whole blob was
  built (clean) and all suites run: cdc_manager-test 30/30, cdc_service-test 56/56,
  tablet_replica-test 15/15. T4,T5,T6,T7,T8 each land with a passing behavioral
  test, so they stand (DR-013..DR-017). T3's impl was correct-by-construction but
  had NO test (RPC plumbing is hard to unit-test); orchestrator added
  `ServicePool::queue_length_for_tests()` + `CDCServiceDedicatedQueueTest` (asserts
  CDC pool depth == --cdc_svc_queue_length and differs from the shared
  TabletServerService depth) -- PASS, so T3 now meets bar (DR-012). NB: the fork's
  own tracker notes cited test runs (e.g. T3 "rpc-test 252/0-fail") that were not
  reproduced; rows corrected to orchestrator-verified results. T9 is INCOMPLETE:
  E1 (is_retryable/needs_resnapshot) impl present but untested; E2
  (SCHEMA_VERSION_MISMATCH) only scaffolded -- consumer_schema_version is threaded
  into ReadChanges but never compared/emitted. Kicked back to a gated (T9-only)
  fork to finish + test; DR-018 pending.

- **2026-08-29 T8 DONE (DR-017).** CDC scan admission now honors the server-wide
  soft-memory limit, not just the CDC-local heap budget. `TryAcquireScanSlot`
  previously gated only on the concurrency cap and the CDC-local scan MemTracker,
  so a tserver already near `--memory_limit_hard_bytes` from writes/compactions
  would still admit a large `GetChanges`/snapshot scan and could be pushed over
  the hard limit. Added a `process_memory::SoftLimitExceeded()` check (the same
  randomized-shedding guard the write path uses at `tablet_service.cc:1706`),
  evaluated after the concurrency slot and before the local budget reservation;
  on trip it releases the slot, bumps a new `cdc_scans_rejected_server_memory`
  counter, and returns `ServiceUnavailable` -> SERVER_TOO_BUSY (retryable back-off,
  not re-snapshot). OR'd with a test-only `--cdc_inject_server_memory_pressure`
  flag (unsafe+runtime) because `SoftLimitExceeded` is process-wide once-init
  cached + randomized and cannot be toggled per test. YB gets the equivalent from
  its per-tablet MemTracker parented to the server root; Kudu's CDC MemTracker is
  local, so the process gate is added explicitly. `cdc_service.{h,cc}`,
  `cdc_service-test.cc`. Build clean; cdc_service-test 56/56 (new
  Isolation_ServerMemoryPressureShedsAndRecovers).

- **2026-08-29 T7 DONE (DR-016).** The two P0-1 WAL force-release backstops in
  `TabletReplica::GetRetentionIndexes()` now have regression tests. Chose
  deterministic replica-level tests over a full-cluster disk-full sim (which is
  not reproducible): the disk-pressure valve is tripped with an `INT64_MAX`
  threshold, and the age ceiling with a new backdated-clock test hook. Each test
  asserts the fired-valve contract -- `cdc_barrier_forced_releases` increments,
  `for_durability` reverts above the pinned barrier, and a real `Log::GC()`
  reclaims segments. Added two `_for_tests` accessors to the production header
  (counter read + advanced-clock backdate); no production behavior change.
  `tablet_replica.h`, `tablet_replica-test.cc`. Build clean; tablet_replica-test
  15/15 (new TestCDCDiskPressureValveReleasesWAL, TestCDCAgeCeilingValveReleasesWAL).

- **2026-08-29 T6 DONE (DR-015).** The max-staleness barrier-release guard no
  longer counts master downtime against consumers. Staleness was measured purely
  from the persisted `last_checkpoint_advance_time_micros`, whose wall clock keeps
  ticking while the master is down -- so a master recovering from an outage longer
  than `--cdc_max_staleness_ms` would drop every not-recently-advanced barrier on
  its first maintenance pass (mass re-snapshot storm; also the CF-2 silent-persist
  race outcome). Added a per-leadership grace floor `cdc_leader_ready_micros_`
  (atomic, stamped in `PrepareForLeadershipTask` before `leader_ready_term_`) and
  changed the release test to measure from `max(last_advance, leader_ready)`.
  After the leader has been up longer than the window the guard is a no-op vs.
  before -- a truly stuck consumer is still released on schedule. Minimal +
  failover-safe (one atomic, no persistence/schema change). `catalog_manager.{h,cc}`,
  `cdc_manager-test.cc`. Build clean; cdc_manager-test 30/30 (new
  StalenessGuardGracePeriodAfterLeaderReady, whose control case log shows the
  release still firing once the grace window has elapsed).

- **2026-08-29 T5 DONE (DR-014).** Closed the half-open force-release valve.
  `GetRetentionIndexes()` previously released only the WAL clamp when the
  disk-pressure valve or the barrier-age ceiling fired, leaving the in-memory
  MVCC/UNDO history floor pinned so compaction/UNDO GC could not reclaim rowset
  history during the exact disk-full event the valve exists to relieve. Now the
  `skip_cdc_clamp` block also calls `shared_tablet()->SetCDCHistoryFloor(Timestamp(0))`,
  so WAL and history release together (matching YB's independent WAL/history
  staleness release, adapted to Kudu's single master-push clock). A subsequently
  GC'd FULL/snapshot consumer gets HISTORY_EXPIRED, mirroring WAL_EXPIRED on the
  WAL side; the master re-raises the floor on its next barrier push.
  `tablet_replica.cc`, `tablet_replica-test.cc`. Build clean; tablet_replica-test
  13/13 (new TestCDCValveReleasesHistoryFloor).

- **2026-08-29 T4 DONE (DR-013).** Dropping a table now condemns every ACTIVE
  CDC stream that references it instead of leaking a stuck-ACTIVE stream with a
  pinned retention barrier. New `CatalogManager::MarkDeletingStreamsForDroppedTables()`
  snapshots ACTIVE streams under `lock_`, then (outside the lock) marks
  `DELETING` + durably `WriteCDCStream` any stream whose referenced table is gone
  from `table_ids_map_` or is `is_deleted()` (REMOVED). Called eagerly as the last
  step of `DeleteTable` (terminal/REMOVED path only -- soft-delete + recall stay
  intact) and as a backstop at the head of `RunCDCStreamMaintenance` before the
  reap, so a stream condemned in a pass is reaped in the same pass and failover
  self-heals. Reuses DR-010's two-phase reap for barrier RELEASE + removal. Coarse
  per-stream (any referenced table dropped -> whole stream), matching YB; the
  multi-table partial-drop refinement is deferred. `catalog_manager.{h,cc}`,
  `cdc_manager-test.cc`. Build clean; cdc_manager-test 29/29 (5 new tests).

- **2026-08-29 T3 DONE (DR-012).** CDC now registers into its own RPC
  service-pool queue instead of the shared 50-slot queue, so a `GetChanges`
  burst can no longer fill the shared inbox and reject Raft/consensus RPCs. Added
  generic `RegisterService(service, queue_length)` overloads on `RpcServer` +
  `ServerBase` (single-arg form delegates, all other callers unchanged); new
  `--cdc_svc_queue_length` flag (default 50, defined in the tserver TU next to
  the CDC registration). Worker threads stay shared by design. `rpc_server.{h,cc}`,
  `server_base.{h,cc}`, `tablet_server.cc`. Build clean; rpc-test 252 pass / 0
  fail / 12 env-skips; mini_tablet_server-test 2/2.

- **2026-08-29 T2 DONE (DR-011).** `CreateCDCStream` now (L6) rejects
  missing/deleted/soft-deleted/not-running tables before persisting, and (L5)
  writes a durable `op_index=0` checkpoint row per tablet + eager barrier push so
  the maintenance loop owns retention from its first pass (survives failover).
  Agent stopped pre-test; on review the full suite exposed a test-setup bug
  (soft-delete request needs `table_name`) which I fixed. `catalog_manager.cc`,
  `cdc_manager-test.cc` (+several tests). Build clean; 24/24 pass.

- **2026-08-29 T1 DONE (DR-010).** Two-phase DELETING: `DeleteCDCStream` persists
  `state=DELETING` and returns; `ReapDeletedCDCStreams()` (run from
  `RunCDCStreamMaintenance`) idempotently RELEASEs barriers -> removes checkpoint
  rows -> removes stream row. RELEASE stamped `now_micros+1` (L4). Orphaned rows
  reaped (L3). DELETING hidden from list/info. `catalog_manager.{cc,h}`,
  `cdc_manager-test.cc` (+5 tests). Build clean; 20/20 pass.
