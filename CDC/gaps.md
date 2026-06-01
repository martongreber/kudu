# CDC Gaps & Follow-up Items

Rolling list of gaps, missing features, and open questions for the Kudu CDC
implementation. Items here are candidates for future phases -- not all will
necessarily be implemented.

Line references point at the `cdc` branch checkpoint commit. This file was
re-audited against the actual code (not the earlier design docs) on 2026-08-03;
several items previously listed as gaps are now implemented and have been moved
to "Recently resolved".

## Recently resolved (implemented in the checkpoint commit)

These were open gaps in earlier revisions of this doc and are now in the code.
Kept here so the history is clear and so the comparison docs can be trusted.

- **Before-images / RecordType.FULL** -- FULL mode reconstructs before/after
  images from the tablet's MVCC/UNDO history at the op timestamp
  (`ReconstructBeforeAfterImages`, `cdc_util.cc`). A per-tablet history floor
  (`Tablet::SetCDCHistoryFloor`, `tablet.h`) clamps the ancient-history-mark so
  the versions CDC needs are not compacted away.
- **Leader-change retention continuity** -- the master recomputes per-tablet
  retention barriers every `--cdc_bg_scan_interval_ms` (60s) and fans them out
  to *every* replica via `SendCDCRetentionBarrierToAllReplicas` /
  `UpdateCDCRetentionBarrier`, so a newly elected leader already holds the WAL
  and history barrier. Closes the old "anchor is tserver-local" hole.
- **Server-driven consistent snapshot** -- `ReadSnapshot` (`cdc_service.cc`)
  scans each tablet at a chosen HybridTime and hands off to WAL streaming at the
  corresponding op index.
- **Stream expiry / GC of abandoned streams** -- idle expiry via
  `--cdc_stream_expiry_ms` (8h) and non-advancing-checkpoint expiry via
  `--cdc_max_staleness_ms` (4h); both release the tablet's retention barriers on
  all replicas.
- **Durable checkpoint** -- `PersistCheckpoint` writes to the master via
  `UpdateCDCCheckpoint` (`cdc_service.cc:1406`); no longer a stub.
- **Committed-only transaction emission (per tablet)** -- transactional writes
  are buffered per `txn_id` and emitted only on `FINALIZE_COMMIT`, dropped on
  `ABORT_TXN` (`cdc_service.cc:856-1051`). Aborted/uncommitted rows are no longer
  published. Remaining transaction gaps are below.
- **On-demand schema info** -- `need_schema_info` prepends the current schema as
  a DDL record (`cdc_service.cc:732`).
- **Fine-grained authz** -- `--cdc_enforce_access_control` requires a signed
  authz token granting SCAN on the target table (`AuthorizeCDCTableOrRespond`,
  `cdc_service.cc:423`; flag defined at `:307`), reusing the scan-token machinery.
- **Heavy-scan admission control** -- `--cdc_max_concurrent_scans` and
  `--cdc_scan_mem_limit_bytes` shed snapshot/FULL scans with a retryable
  `SERVER_TOO_BUSY` when over budget.

## E. Correctness audit findings (2026-08-03)

Deep correctness pass over the shipped code (FULL mode, snapshot pagination,
stream delete, schema stamping, checkpoint persistence). These are bugs in code
that already ships in the checkpoint commit -- not missing features. They take
priority over the YB feature-parity gaps below. Line references are the `cdc`
branch checkpoint commit.

### E1. FULL-mode reconstruction failure emits truncated records silently (CRITICAL) -- FIXED 2026-08-03

- `cdc_service.cc:919-937` (non-txn) and `966-988` (txn). The error path for
  `ReconstructBeforeAfterImages` handles `IsIncomplete` and
  `IsTimedOut`/`IsServiceUnavailable` (abort or return error), but any other
  failure -- transient `IOError` from `NewRowIterator`, `Corruption` during a
  rowset scan -- falls through to `emit_groups.push_back(...)`. The record ships
  with only the WAL-decoded `changes` field: no `old_changes`, and for UPDATE
  the `changes` still holds only the changed columns. The consumer receives
  structurally valid but semantically wrong FULL-mode records with no CDCErrorPB
  or other signal.
- Failing scenario: a flaky DiskRowSet (IOError on `NewRowIterator`) makes every
  FULL-mode UPDATE in the batch emit empty `old_changes`; the consumer computes
  deltas against an empty before-image and cannot detect the corruption.
- Fix (done): both FULL-mode error paths (`cdc_service.cc` non-txn and txn
  commit-time) now `return rs` on any non-`IsIncomplete` failure instead of
  falling through to emit. `IsIncomplete` still maps to `HISTORY_EXPIRED`; every
  other error aborts the batch, and `GetChanges` translates the returned status
  to a CDCErrorPB (`UNKNOWN_ERROR`) so the consumer sees an error rather than a
  truncated record. Regression test:
  `CDCServiceTest.FullMode_ReconstructionFailureDoesNotEmitTruncatedRecord`,
  driven by the test-only flag `--cdc_inject_full_reconstruction_failure`.

### E2. Duplicate primary key in one batch misclassifies UPSERT as INSERT (HIGH) -- FIXED 2026-08-04

- `cdc_util.cc:527-537`. `key_to_record[key_str] = rec_idx` was a plain
  assignment; two ops targeting the same PK in one batch/transaction left only
  the last registered. The before-image scan matched only the last index; the
  earlier record kept `matched_before[i] == false`, and an `UPSERT` with
  `matched_before == false` is reclassified to `INSERT` (line 617).
- Failing scenario: a transaction with two UPSERTs to a pre-existing key K emits
  `INSERT` then `UPDATE` for K, violating the invariant that a key cannot be
  inserted twice without an intervening DELETE. (Confirmed reproducible: the
  tablet applies both same-key UPSERTs sequentially and both reach the WAL as
  separate ops in one replicate.)
- Fix (done): `key_to_record` is now `key_to_records`, an
  `unordered_map<string, vector<int>>` mapping a key to *every* record targeting
  it; the before/after-image scan fans its single matched row to all of those
  record indices, so every record with a matched pre-existing row is classified
  correctly. Regression test:
  `CDCServiceTest.FullMode_DuplicateKeyUpsertsClassifiedAsUpdate` (two UPSERTs to
  a pre-existing key in one write batch must emit two UPDATEs, not INSERT+UPDATE).

### E3. DeleteCDCStream erases in-memory entry before durable removal (HIGH) -- FIXED 2026-08-04

- `catalog_manager.cc:8198-8201`. `cdc_stream_map_.erase(it)` runs at 8198 (under
  `lock_`); `lock_` is dropped, then `sys_catalog_->RemoveCDCStream(stream_id)`
  runs at 8201. A master crash between the two leaves the stream in sys_catalog
  and it reloads on recovery -- but the caller already got success. The
  resurrected stream has no consumer yet pins WAL/MVCC anchors on all its tablets
  forever, and the orphaned-tablets barrier-release fan-out (8204) is lost.
- Fix (done): `DeleteCDCStream` now computes the orphaned-tablet set under
  `lock_`, releases the lock, calls `sys_catalog_->RemoveCDCStream(stream_id)`
  (durable removal) *first*, and only then re-acquires `lock_` to erase the
  in-memory entry -- the order `CreateCDCStream` already uses (durable write
  8150, in-memory insert 8156). A crash between the durable removal and the
  in-memory erase now leaves the stream simply gone (durable state is
  authoritative on recovery) rather than resurrecting it. Addresses the master-side
  root of the same anchor-leak class; the tserver-side consumer-anchor release on
  delete is the separate A4 fix (FIXED 2026-08-25). Regression test:
  `CDCManagerTest.DeleteStream_PersistsAcrossRestart` (delete a stream, restart
  the master, and confirm it does not reload from sys_catalog).

### E4. Snapshot resume after leader change scans at a different timestamp (HIGH) -- FIXED 2026-08-04

- `cdc_service.cc:1118-1161, 1200`. `CDCSnapshotState` (snap_ts, resume_key) is
  in-memory only. After a leader change mid-snapshot the new leader has no
  session state, so the consumer retries with `is_snapshot_start=true`; line 1127
  picks a fresh `snap_ts = Now()` (T2 > T1) while line 1200 still honors the
  client's old `resume_key` unconditionally. Rows `< K` were read at T1, rows
  `> K` at T2. Inserts in (T1,T2] with key > K appear as extra READ records;
  deletes in (T1,T2] with key > K go missing. The snapshot is not self-consistent.
- Fix (done): `ReadSnapshot` now rejects any request carrying a
  `snapshot_resume_key` when this server has no active in-memory snapshot session
  for the (stream, tablet). The check covers both the continue-style resume
  (`is_snapshot_start=false` + resume_key) and the E4-precise case
  (`is_snapshot_start=true` still carrying the old resume_key): a fresh `snap_ts`
  never honors a stale resume key. Surfaced via a dedicated
  `CDCErrorPB::SNAPSHOT_SESSION_LOST` code so the consumer restarts the snapshot
  from the beginning rather than silently rescanning at a new timestamp.
  Regression test:
  `CDCServiceTest.Snapshot_ResumeWithoutSessionRejected` (uses the test hook
  `ClearSnapshotSessionsForTests` to simulate a leader change discarding the
  session). Distinct from and more severe than A2 (which is the
  concurrent-start race).

### E5. Max-valued key permanently wedges snapshot pagination (MEDIUM-HIGH) -- FIXED

- `cdc_service.cc:1203-1204`. `IncrementEncodedKey` returns
  `IllegalState("No lexicographically greater key exists")` when the key cannot
  be incremented (single INT32 key at 2147483647, all-0xFF BINARY key). If the
  last row on a non-final page has the max key, the response carries it as
  `snapshot_resume_key`; the next call fails and every retry re-fails. The
  snapshot never completes.
- Fix: apply the pattern already used at `cdc_util.cc:544-545` -- check the
  return value and fall back to an open upper bound when increment fails.
- FIXED (2026-08-04): `ReadSnapshot` no longer propagates the increment failure.
  When `IncrementEncodedKey` on the resume key fails, it sets `resume_key_is_max`
  and skips the scan, producing an empty terminal page (`snapshot_done=true`) --
  the previous page already ended on the tablet's last row, so no row can be
  strictly greater. Regression test:
  `CDCServiceTest.Snapshot_MaxKeyDoesNotWedgePagination` inserts a row at
  INT32_MAX (via a direct single-row write, since `InsertTestRowsRemote` overflows
  computing `first_row + count` at the max key) and paginates with a tiny
  `max_bytes` so a page ends squarely on the max-key row, asserting the scan
  terminates with all rows emitted instead of wedging.

### E6. stream_config_cache_ is never evicted -- stale record_type for process life (MEDIUM-HIGH) -- FIXED

- `cdc_service.cc:1494-1545`. `GetOrFetchStreamConfig` writes the cache on first
  access (1538) and never invalidates: no TTL, no leadership-change eviction, no
  master signal. A `record_type` change from `CHANGE` to `FULL` after the first
  GetChanges means the tserver keeps emitting CHANGE-mode records (no
  `old_changes`) for the rest of the process lifetime. Same underlying gap as
  section C "stream-config cache is never invalidated," recorded here with the
  correctness impact.
- Fix: add TTL / leadership-change invalidation, and ideally a master push on
  stream reconfigure.

FIXED 2026-08-04:
- The cache value is now `{CDCStreamConfigPB config; MonoTime expiry;}`. Each
  master fetch stamps `expiry = now + --cdc_stream_config_cache_ttl_ms` (new
  flag, default 5 min; `advanced`+`runtime`; 0 = cache for process lifetime,
  the old behavior). `GetOrFetchStreamConfig` serves from cache only while
  `now < expiry`; once stale it refetches from the master, so a reconfigure is
  picked up within one TTL instead of never.
- Graceful degradation: if the refetch fails (master unreachable/erroring) but
  a stale entry is present, it is served rather than failing the consumer -- a
  transient master outage must not stall streaming -- and its expiry is pushed
  forward so we do not attempt (and block on) a refetch on every call. A
  throttled WARNING is logged. With no cached entry at all, the error is
  surfaced as before. Master-side record_type changes on a reconfigure thus
  converge within a bounded window rather than requiring a tserver restart.
- Regression: `cdc_service-test.cc`
  `StreamConfig_CacheEntryExpiresAndRefetches` drives real GetChanges RPCs and
  asserts (a) a fresh-hit does NOT extend the deadline (cache fast path taken)
  and (b) a stale-hit runs the refetch path and re-stamps a fresh deadline.
  Verified it fails against a serve-regardless-of-expiry revert and passes
  after. Full end-to-end record_type flip needs a live master, which the mini
  tserver fixture lacks; the eviction/refresh decision that gates it is tested.
- Test seam: `SetStreamConfigForTests` now seeds a non-expiring entry
  (`MonoTime::Max()`) so existing tests are unaffected by the TTL;
  `SetStreamConfigForTestsWithTtl` and `IsStreamConfigFreshForTests` were added
  for the TTL test.

### E7. UpdateCDCCheckpoint writes checkpoint non-monotonically (MEDIUM) -- FIXED

- `catalog_manager.cc:8304`. `(*checkpoints)[tablet_id] = op_index` is
  unconditional; the `op_index > it->second` check at 8301 only gates the
  `last_checkpoint_advance_time_micros` refresh. A new leader whose local anchor
  lags can push a lower op_index and the master stores it, so
  `RunCDCStreamMaintenance` fans out a lower `min_retained_op_index` and retains
  more WAL than needed (safe but wasteful, and latent/confusing).
- Fix: store `std::max(existing, op_index)` so the persisted checkpoint is
  monotonic.
- FIXED (2026-08-05): `UpdateCDCCheckpoint` now computes a single `advances`
  bool (no prior entry, or `op_index` strictly greater than the stored value)
  and only writes `(*checkpoints)[tablet_id] = op_index` when it holds -- the
  same condition that already gated the last-advance-time refresh, so the store
  and the timestamp stay consistent and the persisted checkpoint never moves
  backward. A lagging leader's lower op_index still refreshes the stream's
  last-active time (the consumer is alive) but does not lower the durable
  checkpoint or the fanned-out `min_retained_op_index`. Regression:
  `cdc_manager-test.cc` `UpdateCheckpoint_IsMonotonic` advances to 100, reports
  50 (must stay 100), re-reports 100 (no-op), then advances to 150. Verified by
  temp-revert: making the store unconditional lets the 50 overwrite 100 and the
  test fails ("checkpoint moved backward").

### E8. DDL record sets schema_version == new_schema_version (MEDIUM) -- FIXED

- `cdc_util.cc:379-381`. `DecodeNonWriteReplicateMsg` for `ALTER_SCHEMA_OP` sets
  both `schema_version` and `new_schema_version` to `req.schema_version()` (the
  new version). Everywhere else `schema_version` means "the version in effect for
  this op" (pre-op). A consumer reconstructing schema history from the stream
  alone cannot tell what was in effect before the ALTER -- both fields say N.
- Fix: set the DDL record's `schema_version` to `req.schema_version() - 1` and
  keep `new_schema_version` at `req.schema_version()`.

FIXED 2026-08-04:
- `cdc_util.cc` `DecodeNonWriteReplicateMsg` ALTER_SCHEMA_OP case now stamps
  `schema_version = req.schema_version() - 1` (the pre-op version) while
  `new_schema_version` stays at `req.schema_version()`. The subtraction is
  guarded against underflow for the theoretical `req.schema_version() == 0`
  case (an ALTER always advances the version, so in practice it is >= 1).
- The `cdc_service.cc` decode loop rolls `running_schema_version` forward via
  `record.new_schema_version()` (not `schema_version()`), so this change does
  not perturb the schema-version roll-forward introduced for E9.
- Regression: `cdc_util-test.cc` `AlterSchemaOp` updated to expect the pre-op
  version (4 for a new version of 5), and a new `AlterSchemaOpSchemaVersion
  ZeroDoesNotUnderflow` test covers the underflow guard. Verified the updated
  `AlterSchemaOp` test fails against the pre-fix code and passes after.

### E9. commit/apply race stamps the batch with schema version N-1 (MEDIUM) -- FIXED

- `cdc_service.cc:843-854`. `up_to_op_index` is the committed watermark
  (`GetLastOpId(COMMITTED_OPID)`, 763) but `current_schema_version` is read from
  `tablet_metadata()->schema_version()` (843), which reflects only *applied*
  ops. If an ALTER in the batch window is committed but not yet applied,
  `current_schema_version = N` when it should be N+1; `alters_in_batch = 1`, so
  `running_schema_version = N - 1`. Every pre-ALTER WRITE in the batch is stamped
  N-1, and a consumer applying the N-1 decoder may miss columns added by the
  N-1 -> N ALTER.
- Fix: derive the running schema version from a source consistent with the
  committed watermark (or wait for apply of ALTERs within the window before
  stamping), so pre-ALTER WRITEs carry their true schema version.
- FIXED (2026-08-04): the base schema version is no longer computed backward from
  the applied metadata version. When the batch contains an ALTER, the base is
  taken straight from the WAL -- the first ALTER records its new version W, so the
  version in effect before it is W - 1 -- which is independent of apply progress.
  Only when the batch has no ALTER (every op shares one version) does the code
  fall back to the applied metadata version, which is exact in that case. The
  decode loop still rolls the version forward at each ALTER as before. Added a
  test-only apply-latency hook (`--tablet_inject_latency_on_apply_alter_schema_op_ms`,
  tagged unsafe+runtime, mirroring the existing write-op hook) so the
  committed-but-unapplied window can be reproduced deterministically. Regression
  test: `CDCServiceTest.SchemaVersion_CommittedUnappliedAlterStampsPreAlterVersion`
  applies one alter (v0->v1), then delays apply of a second (v1->v2) and reads the
  window while it is committed-but-unapplied, asserting the pre-ALTER WRITE is
  stamped 1 (not 0). Verified the test fails against the old backward computation.

### E10. Server-stored snapshot resume_key is never read back (MEDIUM) -- FIXED 2026-08-05

- `cdc_service.cc:1200, 1248-1251`. The server stores the authoritative
  last-scanned key in `state->snapshot.resume_key` (1249) but never reads it;
  all resume logic uses the client-supplied `req_resume_key`. A stale/empty
  client key (consumer restart without a durable last key) makes the server scan
  from the wrong position -- forward-skip (missing rows) or backward-replay
  (duplicates).
- Fix: use the server-side stored key as the authoritative resume bound when a
  live session exists; reconcile with E4 for the session-lost case.
- FIXED (2026-08-05): `ReadSnapshot` step 3 now loads the resume bound from the
  live session (`resume_key = it->second->snapshot.resume_key`) and step 5 uses
  that value -- not `req_resume_key` -- as the scan's strictly-greater lower
  bound. While a session is live the server-stored key is the single source of
  truth: it is cleared at establish (so the first page scans from the beginning)
  and advanced to the last emitted key on each page (step 7). The client's
  `req_resume_key` is retained only as (a) the "continue this snapshot" routing
  signal in `GetChanges` (`is_snapshot_start || has_snapshot_resume_key`) and
  (b) the E4 presence guard (a non-empty client key with no active session ->
  SNAPSHOT_SESSION_LOST). A consumer that replays a stale key can therefore no
  longer reposition the scan backward (duplicates / non-termination) or forward
  (skipped rows).
- Regression: `CDCServiceTest.Snapshot_ResumesFromServerAuthoritativeKey`
  (cdc_service-test.cc). Seeds a FULL/INITIAL_AND_CONTINUE stream with 50 rows,
  drains the snapshot with a tiny page (max_bytes=64) while the client latches
  the first page's resume_key and replays that SAME stale key on every
  continuation. With the fix the server's advancing stored key drives the scan
  to completion (50 rows, each once, strictly increasing, >1 page). Verified by
  temp-revert (step 3 set to `req_resume_key`): the scan restarts just past the
  first page every call and never terminates -> test FAILS (pages hit the 1000
  cap); restoring the fix -> PASSES. Full cdc_service-test suite: 43/43 pass.

### E11. Concurrent cache misses cause a thundering herd of GetCDCStreamInfo (LOW-MEDIUM) -- FIXED 2026-08-05

- `cdc_service.cc:1494-1545`. The lock is released before the master RPC, so N
  concurrent `GetChanges` that all miss for the same `stream_id` each issue their
  own `GetCDCStreamInfo` (e.g. 100 tablets on one tserver after restart -> 100
  simultaneous RPCs). No correctness impact; can spike master catalog lock
  contention on startup.
- Fix: single-flight the fetch (in-flight map keyed by stream_id) so concurrent
  misses share one RPC.
- FIXED (2026-08-05): `GetOrFetchStreamConfig` now single-flights the master
  fetch per stream_id. After the lock-free fast-path cache check misses, the
  caller takes a per-stream `std::mutex` (from `stream_config_fetch_locks_`, via
  `GetStreamConfigFetchLock` -- same lazy-create pattern as
  `snapshot_start_locks_`; a std::mutex, not the `lock_` spinlock, since it is
  held across the blocking master RPC). Under that mutex it re-checks the cache
  with a fresh `now`: the first caller misses again and issues the one RPC (or,
  with no master reachable, serves the backed-off stale entry), populating the
  cache; every waiter that piled up behind the mutex then finds the just-written
  fresh entry and returns it without an RPC of its own. The fetch lock map is
  bounded by the number of distinct streams and never pruned. Because the mutex
  serializes even the miss-and-refetch case, the simultaneous herd (100 tablets
  of one stream re-streaming after a restart) collapses to a single master
  round-trip regardless of interleaving.
- Regression: `CDCServiceTest.StreamConfig_ConcurrentMissesSingleFlight`
  (cdc_service-test.cc) plus test hooks: a `stream_config_master_fetches_`
  counter (incremented once per real fetch that reaches the master loop, exposed
  via `StreamConfigMasterFetchesForTests`) and an unsafe
  `--cdc_inject_latency_before_stream_config_fetch_ms` flag that widens the fetch
  window so callers reliably pile up on the lock. The test seeds an already
  expired FULL config, injects 500ms of fetch latency, then fires 8 concurrent
  `GetChanges` for the same stream and asserts exactly 1 master fetch occurred.
  Verified by temp-revert (dropping the `lock_guard` on the fetch mutex): the
  count becomes 8 and the test FAILS; restoring the guard -> 1, PASSES. Full
  cdc_service-test suite: 44/44 pass.

### E12. FULL-mode before-image reads reclaimed history as the live row (HIGH) -- FIXED 2026-08-05

- `cdc_util.cc` `ReconstructBeforeAfterImages` guarded a before-image read only
  against the tablet's *current* ancient history mark (`before_ts <
  ancient_history_mark`). But the FULL `GetChanges` path re-pins the CDC history
  floor to each batch's minimum op timestamp (`cdc_service.cc:1174`,
  `SetCDCHistoryFloor(batch_min_ts)`) *before* the decode loop reconstructs the
  batch's images. When a stream replays old ops, that re-pin lowers the current
  AHM (which is clamped to the floor) back below a point at which an earlier
  compaction -- run while no floor protected it -- had already GC'd the UNDO
  history. The pre-check then passes, and the historical scan at
  `MvccSnapshot(before_ts)` over reclaimed history silently returns the *current*
  row. Extends E1: the record is structurally valid (populated `old_changes`) but
  semantically wrong, with no `CDCErrorPB`.
- Failing scenario: FULL stream, INSERT (v0) then UPDATE (v1) a row; no read yet
  (no floor). Set `--tablet_history_max_age_sec=0` and `FORCE_COMPACT_ALL` -> the
  update's UNDO is reclaimed. A subsequent FULL `GetChanges` from op 0 emitted the
  UPDATE with before-image == v1 (the post-update value) instead of v0, and no
  error. Consumers computing deltas against the before-image silently corrupt.
- Fix (done): `Tablet` tracks a monotonic history-GC water mark -- the highest
  AHM ever actually applied to GC history, updated lock-free via
  `Tablet::RecordHistoryGcWaterMark()` (a CAS-max at `tablet.cc:1593`) called by
  every flush/compaction/UNDO-GC path (the callers of `GetHistoryGcOpts()`, e.g.
  `tablet.cc:1529,2133,2942,2978,3187,3240`).
  `ReconstructBeforeAfterImages` adds a second guard: `before_ts <
  cdc_history_gc_water_mark()` -> `Status::Incomplete` -> `HISTORY_EXPIRED`. The
  water mark is monotonic, so the per-batch floor re-pin cannot lower it; a
  legitimately protected read (floor held at GC time, so GC ran at the
  clamped-low AHM) keeps the mark low and is unaffected. The guard is
  conservative (may report `HISTORY_EXPIRED` for a rowset whose history was not
  in fact removed) but never emits wrong data -- the consumer re-establishes from
  a snapshot.
- Regression: `CDCServiceTest.FullMode_BeforeImageGcedReturnsHistoryExpired`
  (asserts `HISTORY_EXPIRED` and no leaked UPDATE) and
  `FullMode_HistoryFloorProtectsBeforeImageAcrossCompaction` (asserts the
  positive case still reconstructs correctly across `FORCE_COMPACT_ALL` with a
  held floor). cdc_service-test: 50/50 pass; tablet-test 175/175,
  tablet_history_gc-test 20/20 pass.

## A. Correctness / liveness bugs (address before production)

### A1. Large-transaction wedge (liveness) -- FIXED 2026-08-04

- `replicates` is bounded by `--cdc_max_bytes_per_response` (64 MiB default) at
  WAL read time (`cdc_service.cc:881`). If a transaction's WAL span from its
  first write (`txn_first_index`) to its `FINALIZE_COMMIT` exceeds that cap, the
  commit is never in the read window, `open_min` stays unresolved, and
  `checkpoint_op_index` is pinned at `open_min - 1` on every call
  (`cdc_service.cc:1053`). The stream stops advancing permanently.
- Fix (done): `ReadChanges` now wraps the WAL read in an escalation loop. A
  cheap pre-scan (`OldestOpenTxnFirstIndex`, op-types only, no row decode)
  identifies the oldest still-open transaction in the window. The wedge is
  detected when that transaction starts at the very first op of the window (so
  `checkpoint = open_min - 1 == from_op_index`, i.e. no progress is possible)
  *and* the read was truncated by the byte cap (so the commit may lie just
  beyond it). In that case the effective read cap is doubled (up to
  `--cdc_max_transaction_span_bytes`, default 512 MiB) and the read retried, so
  the commit comes into the window and the transaction is emitted atomically.
  A transaction whose span exceeds that limit cannot be emitted and fails loudly
  with a dedicated `CDCErrorPB::TRANSACTION_TOO_LARGE` rather than stalling
  silently. Reads that reach the committed watermark with a transaction still
  open are *not* escalated (that is normal back-pressure -- the transaction has
  not committed yet -- not a wedge). Regression tests (integration, transactions
  enabled): `CDCFailoverITest.LargeTransactionDoesNotWedgeStream` (a 40-write
  txn read under a 256-byte per-response cap is emitted in full and the
  checkpoint advances past the commit) and
  `CDCFailoverITest.TransactionExceedingSpanCapFailsLoudly` (a 64-byte span cap
  yields TRANSACTION_TOO_LARGE instead of a silent stall).

### A2. Snapshot-start race -- FIXED 2026-08-04

- `start_new` is evaluated under `lock_`, but the full snapshot-start sequence
  (timestamp pick, `streaming_start_op_index`, `SetCDCHistoryFloor`,
  `state->snapshot`) is not atomic (`cdc_service.cc:~1119`). Two concurrent
  first-calls can both run the start path and produce an inconsistent snapshot
  (the second overwrites the first's `snap_ts` / `streaming_start_op_index`).
- Fix: hold a per-(stream,tablet) mutex across the whole start sequence.

FIXED 2026-08-04:
- Added a per-(stream, tablet) `std::mutex` map (`snapshot_start_locks_`,
  lazily created via `GetSnapshotStartLock`, guarded by `lock_` for lookup).
  `ReadSnapshot` holds this mutex across the entire start-decision + establish
  sequence (the active-session check, the E4 resume-key check, and the
  establish block with its blocking `WaitUntilSafe` /
  `WaitForSnapshotWithAllApplied`). The mutex is a real (blocking) lock, not the
  `lock_` spinlock, so it can wrap the blocking waits. It is released before the
  scan/emit so concurrent pages of an established session are not serialized.
  Once a session is active, a concurrent start observes it under the mutex and
  continues instead of re-establishing.
- Regression: `cdc_service-test.cc` `Snapshot_ConcurrentStartsEstablishOnce`
  fires 4 concurrent `is_snapshot_start=true` RPCs with latency injected into
  the establish window (new `--cdc_inject_latency_before_snapshot_establish_ms`)
  and asserts exactly one session is established (via new test counter
  `SnapshotSessionsEstablishedForTests`). Verified it reports 4 without the lock
  and 1 with it.

### A3. Snapshot ignores the client deadline -- FIXED 2026-08-04

- `ReadSnapshot` hardcodes a 30s deadline (`cdc_service.cc:1113`) instead of
  deriving it from `context->GetClientDeadline()` the way FULL mode does. A
  client with a shorter deadline can still block a service thread up to 30s.

FIXED 2026-08-04:
- The GetChanges handler now derives the snapshot deadline as
  `min(context->GetClientDeadline(), now + --cdc_snapshot_wait_timeout_ms)`
  (new flag, default 30s, `advanced`+`runtime`) and passes it into
  `ReadSnapshot`, which uses it for `WaitUntilSafe` /
  `WaitForSnapshotWithAllApplied` -- mirroring the FULL-mode apply-wait
  deadline. A snapshot wait that exceeds the deadline now returns a retryable
  error (`SERVER_TOO_BUSY`) rather than blocking a service thread up to 30s
  regardless of the caller.
- Regression: `Snapshot_HonorsDeadlineWhenEstablishSlow` sets the cap to 500ms,
  injects 5000ms of establish latency, and asserts the call aborts near the cap
  with `SERVER_TOO_BUSY`. Verified it hangs ~5s with no error when the deadline
  derivation is reverted to the hardcoded 30s.

### A4. Consumer anchor not released on stream delete

- FIXED 2026-08-25. `DeleteCDCStream` now fans a consumer-anchor release out to
  every replica of each tablet the deleted stream referenced, via two new
  `UpdateCDCRetentionBarrierRequestPB` fields: `release_consumer_stream_id` (6)
  and `skip_barrier_update` (7). On the tserver,
  `CDCServiceImpl::SetRetentionBarrier` unregisters (`UnregisterIfAnchored`) and
  erases the per-(stream,tablet) consumer anchor in `stream_tablet_state_` for
  the named stream. The release is unconditional -- a delete is terminal, so the
  barrier last-writer-wins `barrier_seq` gate does not suppress it -- and is
  fanned to all replicas because the anchor may live on a replica that has since
  lost leadership. For an orphaned tablet the same RPC also releases the
  aggregate retention barrier (index = -1); for a tablet still shared by other
  streams `skip_barrier_update=true` releases only the consumer anchor, leaving
  the aggregate barrier for the master's next maintenance pass to recompute so
  the surviving streams keep their retention (avoids a retention gap). Tests:
  `cdc_service-test` `SetRetentionBarrier_ReleasesConsumerAnchorOnStreamDelete`
  (shared-tablet + orphaned cases + idempotency); `cdc_failover-itest`
  `DeleteStream_ReleasesConsumerAnchor` (RF3, leader-only anchor gone after
  delete, gated on the master having learned the checkpoint first).
- Original analysis: `DeleteCDCStream` released the master-pushed retention
  barrier (index = -1), but the per-(stream,tablet) consumer anchor in
  `stream_tablet_state_` on the tserver was only released on *tablet* deletion
  (`ReleaseAnchorsForTablet`), not on stream deletion. A deleted or abandoned
  stream's consumer anchor kept pinning the WAL until the tablet itself went away.
- See also E3, which fixes the durable-delete ordering that makes this leak
  permanent after a master crash.

## B. Missing admission control / defensive flags (present in YugabyteDB, apply to Kudu)

- **No RPC-thread reservation for non-CDC traffic.** FIXED 2026-08-05. New flag
  `--cdc_get_changes_free_rpc_ratio` (default 0.10, validated to [0.0, 1.0),
  advanced+runtime) mirrors YB's `cdc_get_changes_free_rpc_ratio`. `GetChanges`
  now caps the number of concurrent calls at
  `floor((1 - ratio) * --rpc_num_service_threads)` (always at least 1) via a
  lock-free in-flight counter (`get_changes_inflight_`) reserved at the top of
  the handler and released on every return path. Excess calls are shed
  immediately with a retryable `SERVER_TOO_BUSY` (`ServiceUnavailable`) so the
  consumer backs off, leaving `ratio` of the CDC service pool's worker threads
  free for non-GetChanges traffic (`Checkpoint` and other control RPCs). Unlike
  `--cdc_max_concurrent_scans` / `--cdc_scan_mem_limit_bytes` (which bound only
  heavy FULL-mode / snapshot scans by heap), this bounds worker-thread
  occupancy for *all* GetChanges calls, closing the gap on the otherwise
  unguarded CHANGE-mode WAL-streaming path. Regression: `cdc_service-test.cc`
  `Admission_GetChangesRpcWorkerReservation` forces the cap to 1
  (`--rpc_num_service_threads=1`), holds one call in flight via the snapshot
  establish-latency injection, and asserts a concurrent second GetChanges is
  shed with `SERVER_TOO_BUSY` ("too many concurrent"), that the first call is
  served (not shed), and that a later call is admitted once the slot is
  released. Verified by temp-revert: disabling the shed lets the second call
  succeed and the test fails.
- **No safe-deadline ratio.** FIXED 2026-08-05. New flag
  `--cdc_read_safe_deadline_ratio` (default 0.10, validated to [0.0, 1.0),
  advanced+runtime) mirrors YB's `cdc_read_safe_deadline_ratio`. `GetChanges`
  now derives its effective wait deadline for both heavy paths -- the FULL-mode
  apply-wait and the snapshot MVCC-safe wait -- from a shrunk copy of the
  client's RPC deadline: `SafeClientDeadline(now, client_deadline)` returns
  `now + (client_deadline - now) * (1 - ratio)`, reserving that fraction as
  headroom to serialize and send whatever partial result is in hand before the
  RPC deadline elapses. The absolute per-path caps
  (`--cdc_full_apply_wait_timeout_ms`, `--cdc_snapshot_wait_timeout_ms`) still
  apply on top; the ratio only shrinks the client-derived deadline and is a
  no-op when there is no client deadline, the deadline already passed, or the
  ratio is 0. Regression: `cdc_service-test.cc`
  `Snapshot_SafeDeadlineRatioReservesHeadroom` sets the absolute snapshot cap
  far above an 8s client deadline (so only the client-derived deadline binds)
  and injects a 3.5s establish latency; with ratio 0.10 the ~7.2s safe budget
  outlasts the latency and the page is produced, while with ratio 0.70 the
  ~2.4s safe budget aborts the establish early with SERVER_TOO_BUSY well before
  the 8s client deadline. Verified by temp-revert: making `SafeClientDeadline`
  return the deadline unchanged lets the high-ratio case succeed and the test
  fails.
- **No cap on barrier-release fan-out per maintenance scan.** FIXED 2026-08-04.
  `RunCDCStreamMaintenance` now caps the number of barrier-release RPCs it fans
  out per pass at `--cdc_max_barrier_releases_per_run` (default 1000, 0 =
  unlimited). When more tablets need releasing in one tick (e.g. a mass expiry),
  the excess is left in `cdc_barriered_tablets_` (step 5 re-adds the un-released
  ones alongside the still-pinned set) so subsequent passes retry it. Only
  barrier *releases* are throttled -- barrier *sets*, which pin retention for
  correctness, are never deferred, so the cap only delays cleanup (WAL/history
  GC) by a few passes and never risks dropping data a live consumer needs. This
  mirrors YB's `cdcsdk_max_expired_tables_to_clean_per_run`. The step-4 loop
  exposes deterministic master-side atomic counters
  (`cdc_barrier_releases_total`, `cdc_barrier_releases_deferred_total`,
  `cdc_barriered_tablet_count`) so the effect is observable without racing on
  tserver-side anchors. Regression: `cdc_failover-itest.cc`
  `BarrierReleaseFanoutIsCappedButRetried` (three pinned-then-expired tablets;
  waits for `cdc_barriered_tablet_count()==3`, then sets cap=1 and expires all
  three; asserts the deferred-release counter delta reaches >=3 -- proving the
  cap defers -- AND the releases-total counter delta reaches >=3 -- proving the
  deferred releases are retried on later passes and not dropped). Verified by
  temp-revert: disabling the cap check drives the deferred delta to 0 and the
  test fails.

  NOTE (pre-existing hazard, not introduced by this fix): the async
  SET/RELEASE fan-out has a reordering race. Barrier SET and RELEASE tasks are
  best-effort, one-shot, and unordered; a SET task dispatched in an earlier
  maintenance pass can land on a replica *after* a later pass's RELEASE, leaving
  that replica re-anchored with no retry (the master has already dropped the
  tablet from `cdc_barriered_tablets_`). This is a real WAL/history-retention
  leak hazard independent of the fan-out cap and is tracked as its own gap
  below.

- **Async barrier SET/RELEASE fan-out can reorder, leaking WAL retention.**
  FIXED 2026-08-05.
  `SendCDCRetentionBarrierToAllReplicas` dispatches best-effort, one-shot,
  unordered `AsyncUpdateCDCRetentionBarrier` tasks. A SET (min_retained>=0)
  dispatched by one maintenance pass and a RELEASE (min_retained=-1) dispatched
  by a later pass are not ordered against each other, so a slow SET can be
  applied on a replica *after* the RELEASE. That replica is left re-anchored at
  a stale op-index with no retry, because the master has already dropped the
  tablet from `cdc_barriered_tablets_` (step 5) and will not revisit it. The
  result is a per-replica WAL/history-retention leak that persists until the
  next event that re-pins and then cleanly releases the tablet.

  FIX: last-writer-wins ordering by monotonic sequence number. A new optional
  `barrier_seq` field on `UpdateCDCRetentionBarrierRequestPB` carries the
  wall-clock-micros timestamp of the maintenance pass that dispatched the
  update. The master stamps every SET (step 3) and RELEASE (step 4) of a pass
  with that pass's `now_micros`, and `DeleteCDCStream`'s release with its own
  `GetCurrentTimeMicros()`. On the tserver, `SetRetentionBarrier` records the
  highest `barrier_seq` applied per tablet in `barrier_last_seq_` and discards
  any update whose seq is strictly lower -- so a stale SET arriving after a
  newer RELEASE (or a stale RELEASE arriving after a newer SET) is ignored. The
  seq gate, the MVCC history-floor apply, and the anchor register/release all
  run under a single hold of `lock_`, so a stale update cannot interleave its
  apply between another update's gate and apply. The seq map deliberately
  outlives the anchor (not erased on release) so a stale SET after a RELEASE is
  still recognized as superseded. `barrier_seq == 0` means an unsequenced
  (legacy) master and is always applied, preserving mixed-version behavior.

  Regression: `CDCServiceTest.SetRetentionBarrier_LastWriterWinsOnReorder`
  drives SET@seq100 -> RELEASE@seq200 -> stale SET@seq100 (must stay released)
  -> SET@seq300 -> stale RELEASE@seq250 (must stay anchored), asserting via a
  test-only `RetentionAnchorForTests` accessor that observes only the CDC-owned
  anchor (independent of unrelated MRS/DMS anchors in the shared registry).
  Verified by temp-revert: disabling the seq gate makes both the stale-SET and
  stale-RELEASE assertions fail; restoring it passes. Full cdc_service-test
  suite: 45/45 pass.

## C. Missing edge-case classification / validation

- **Deleted/unknown stream silently returns WAL data.** FIXED 2026-08-04.
  Previously, on stream-config fetch failure the read path logged a warning and
  fell through as CHANGE mode, and `STREAM_NOT_FOUND` was defined in the proto
  but never set. `GetChanges` now validates the stream up front:
  `GetOrFetchStreamConfig` distinguishes an authoritative NotFound from the
  leader master (the stream was deleted or never created -- only the leader
  reaches the stream-map lookup, so a NotFound is definitive) from a transient
  fetch failure. On the authoritative NotFound it evicts any stale cache entry
  and returns NotFound; `GetChanges` maps that to `STREAM_NOT_FOUND` and stops.
  A transient fetch failure is still tolerated (serve-stale-on-refetch-failure
  from E6, or best-effort CHANGE mode with a cold cache) so a master blip does
  not stall streaming. The single up-front fetch also replaces the two separate
  config lookups the snapshot and streaming branches used to do.
  Note: within the E6 TTL window a still-fresh cached entry is served without a
  refetch, so a just-deleted stream can be served for up to
  `--cdc_stream_config_cache_ttl_ms` before the eviction fires -- bounded, and
  the permanent silent-serve is gone. Regression:
  `cdc_failover-itest.cc` `GetChanges_UnknownStreamReturnsStreamNotFound` and
  `GetChanges_DeletedStreamReturnsStreamNotFound` (both verified to return code
  4/no-error without the fix). `STREAM_EXPIRED` (idle-expiry) is now emitted
  server-side (FIXED 2026-08-25, see the dedicated bullet below); the `DELETING`
  two-phase state remains a separate item (see C list below).
- **Stream-config cache is never invalidated.** FIXED (see E6): entries now
  carry a TTL (`--cdc_stream_config_cache_ttl_ms`, default 5 min) and are
  refetched from the master once stale, so a reconfigured stream (e.g.
  CHANGE -> FULL record_type) is picked up within the TTL rather than requiring
  a tserver restart. (A deleted/unknown stream is still not distinguished on the
  read path -- see the first bullet above.)
- **`STREAM_EXPIRED` never emitted server-side.** FIXED 2026-08-25. `GetChanges`
  now disambiguates a garbage-collected WAL read: when the requested
  `from_op_index` is gone (`ReadReplicatesInRange` -> NotFound) AND the session
  has been idle beyond the new `--cdc_stream_idle_expiry_ms` (default 8h,
  advanced+runtime; matches the master's `--cdc_stream_expiry_ms`), it returns
  `STREAM_EXPIRED` ("permanently expired, re-bootstrap from a snapshot") instead
  of `WAL_EXPIRED` ("possibly transient GC during a failover, safe to retry").
  The idle test (`CDCServiceImpl::StreamIdleExpired`) is reactive only --
  evaluated at the moment the WAL is found missing, measured from the session's
  last successful-poll time recorded in `stream_tablet_state_` -- so a
  still-served stream is never expired, and a session with no recorded activity
  (no `stream_tablet_state_` entry, or `last_active <= 0`) falls back to the
  conservative `WAL_EXPIRED`. `--cdc_stream_idle_expiry_ms=0` disables the
  disambiguation entirely (always `WAL_EXPIRED`). The consumer (`cdc_consumer.cc`)
  already treated `STREAM_EXPIRED` identically to `WAL_EXPIRED` as a re-bootstrap
  trigger (`needs_resnapshot_`), so no consumer change was needed. Regression:
  `cdc_service-test.cc` `GetChanges_WalGcedAndSessionIdleReturnsStreamExpired`
  (same WAL-GC setup as the existing `...ReturnsWalExpired` test but with the idle
  window forced to 1ms after a successful poll; the two tests pin the two
  branches of the classification).
- **`TABLET_NOT_RUNNING` never distinguished.** FIXED 2026-08-04. `ReadChanges`
  now calls `TabletReplica::CheckRunning()` right after locating the replica
  (before the leader check) and, if the tablet is not RUNNING (bootstrapping /
  catching up), sets `TABLET_NOT_RUNNING` directly on the response and returns.
  Setting the CDC error directly avoids GetChanges' status-based translation
  reclassifying `CheckRunning`'s IllegalState as `TABLET_NOT_LEADER`. Regression:
  `cdc_service-test.cc` `GetChanges_TabletNotRunning` (drives the condition via
  the new unsafe test flag `--cdc_inject_tablet_not_running`; verified to return
  no error without the fix).
- **No post-read leader-term recheck.** FIXED 2026-08-04. `ReadChanges` captures
  `RaftConsensus::CurrentTerm()` at the initial leader check and, after the WAL
  scan assembles the batch, rechecks that the replica is still LEADER at the
  same term. If leadership was lost or the term advanced mid-read (the batch may
  have been read from a log the new leader diverged from / truncated), it
  returns IllegalState -> `TABLET_NOT_LEADER` so the consumer rediscovers the
  leader and retries; the consumer checkpoint has not advanced, so the retry is
  idempotent. Regression: `cdc_service-test.cc`
  `GetChanges_PostReadLeadershipLossRejected` (drives the condition via the new
  unsafe test flag `--cdc_inject_post_read_leadership_loss`; verified to return
  no error without the fix).
- **`from_op_index` is not validated** -- FIXED 2026-08-04. `GetChanges` now
  rejects a negative `from_op_index` up front (before the config fetch and the
  WAL read) with an `InvalidArgument` status, rather than passing it through to
  `ReadReplicatesInRange`. (Implausibly large values are already handled: when
  `from_op_index >= up_to_op_index` `ReadChanges` returns an empty batch with
  the current committed index.) Regression:
  `cdc_failover-itest.cc` `GetChanges_NegativeFromOpIndexRejected`.
- **No `have_more_records` / safe-time progress signal.** `have_more_records`
  FIXED 2026-08-04; safe-time/SAFEPOINT record DEFERRED to D1. `GetChanges` now
  sets `have_more_records` (new response field 8) true when the WAL read was cut
  short of the committed watermark by the per-response byte budget -- more
  records are immediately available and the consumer should keep polling now --
  and false once the batch reaches the committed head (caught up; poll on the
  normal interval). Records deferred by an open transaction are treated as
  back-pressure (not "more"), since an immediate re-poll cannot surface them
  until the transaction commits. Regression: `cdc_service-test.cc`
  `GetChanges_HaveMoreRecords` (asserts true under a tiny byte budget, false
  under an ample one and when caught up). The safe-time / SAFEPOINT record (a
  watermark an idle consumer can advance even with no records) has ordering
  semantics that belong with the per-tablet safe-time framing in D1, so it is
  deferred there rather than shipped with fuzzy semantics now.
- **`INITIAL_ONLY` snapshot mode and the two-phase `DELETING` stream state are
  defined in the protos but not enforced server-side.** FIXED 2026-08-04.
  `INITIAL_ONLY` is now enforced on the WAL-streaming path: a plain (non-snapshot)
  `GetChanges` against an `INITIAL_ONLY` stream is rejected with
  `UNKNOWN_ERROR` / `InvalidArgument` ("WAL streaming not allowed ...
  snapshot_mode=INITIAL_ONLY"), symmetric with the existing `NEVER`-stream
  snapshot rejection -- the snapshot is the only output for such a stream, so a
  streaming request is a consumer bug that must fail loudly rather than silently
  serve WAL records that violate the stream's contract (`cdc_service.cc`,
  streaming branch). For `DELETING`, `GetCDCStreamInfo` now treats any non-ACTIVE
  stream as absent (returns `NotFound`, which callers translate to
  `STREAM_NOT_FOUND`), so a stream being torn down cannot hand back a config that
  points at WAL/history already being released (`catalog_manager.cc`). Delete is
  single-phase today, so `DELETING` is never actually persisted; this guard is
  forward-looking for a future two-phase delete and is therefore not
  end-to-end testable now. Regression:
  `cdc_failover-itest.cc` `GetChanges_InitialOnlyRejectsWalStreaming` (verified
  by temp-revert: without the check, the response carries no error).

## D. Larger open gaps

### D1. Transaction consistency beyond a single tablet

Per-tablet committed-only emission is implemented (see Recently resolved), but:

- **No safe-time signal.** Consumers wanting snapshot-consistent reads have no
  server-provided safe time to gate on.
- **Per-tablet framing only.** A multi-tablet transaction spans multiple
  GetChanges streams; the consumer must correlate by `txn_id` across tablets to
  reconstruct the boundary. There is no global ordering.
- **WAL order vs commit-timestamp order.** CDC emits in WAL op-index order; the
  true commit timestamp may be later than non-transactional writes that appear
  after the txn's writes in the WAL.
- See also A1 (large-transaction wedge).

### D2. Tablet split lineage

- No CDC-aware tablet listing and no parent-drain-then-children handoff. A split
  can briefly reorder or duplicate across the boundary. YB returns a
  `TABLET_SPLIT` error and defers the split report until the parent batch
  drains. Applies to Kudu range splits / partition changes.

### D3. On-demand schema-by-version

- `need_schema_info` prepends the current schema, but there is no lookup for a
  specific historical schema version, so a consumer starting mid-stream after an
  ALTER has no base schema until the next DDL record.

### D4. Cross-tablet consistent ordering (Virtual WAL)

- No cross-tablet merge into a single ordered stream. Only meaningful once D1 is
  addressed; for single-row-transaction workloads per-tablet ordering suffices.

### D5. Wire format / serialization

- Column values are Kudu on-wire bytes; the consumer must know the schema to
  decode. No self-describing format (`RecordFormat.JSON` is reserved but not
  implemented).

## Does NOT apply to Kudu (do not copy from YugabyteDB)

- xCluster send-rate limiter (`xcluster_get_changes_max_send_rate_mbps`).
- Virtual-WAL per-tserver caps (`cdc_max_virtual_wal_per_tserver`) unless D4 is
  pursued.
- Colocated-table checkpoint keying (Kudu has no colocation).
- Intent-lag force-resolve (`cdc_resolve_intent_lag_threshold_ms`) -- tied to
  YB's IntentsDB, not Kudu's participant-based transactions.
- AutoFlags version gating, YSQL/YCQL logical-replication / walsender protocol.

## Suggested priority for production

Correctness audit (section E) findings on shipped code come first, because they
silently corrupt or wedge features that already ship:

1. E1 (FULL-mode silent corruption) -- CRITICAL, blocks v1. FIXED.
2. E2 (duplicate-key UPSERT misclassification) -- FIXED. E3 (delete resurrection
   / permanent anchor leak; master-side root of A4) -- FIXED. A4 (tserver-side
   consumer-anchor release on delete) -- FIXED. E4 (snapshot cross-timestamp
   resume) -- FIXED.
3. A1 (large-transaction wedge) -- top pre-existing liveness bug. FIXED.
4. E5 (max-key snapshot wedge) -- FIXED. E9 (schema N-1 stamp) -- FIXED. E8 (DDL
   schema_version) -- FIXED. E6 (stream-config cache staleness; ties into C) -- FIXED.
5. A2/A3 (snapshot start race + deadline) -- FIXED. C (stream validation +
   error codes, incl. INITIAL_ONLY/DELETING enforcement) -- FIXED. B
   (barrier-release fan-out cap) -- FIXED. B (safe-deadline ratio) -- FIXED.
   B (CHANGE-mode streaming admission control / RPC-thread reservation) -- FIXED.
   B (async barrier SET/RELEASE reorder; last-writer-wins by seq) -- FIXED.
6. E7 (monotonic checkpoint) -- FIXED. E10 (authoritative resume_key) -- FIXED.
   E11 (single-flight config fetch) -- FIXED.
7. D2/D3 (split lineage, schema-by-version) -- post-v1.

---

_Last updated: 2026-08-25_
