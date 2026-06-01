<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# CDC test-coverage comparison: Kudu vs. YugabyteDB

Compares the *test scenarios* each project has for CDC: which suites exist, what
they exercise, and where Kudu has (or lacks) an equivalent scenario. Companion to
`YB_KUDU_CDC_COMPARISON.md` (feature/architecture gaps) and `gaps.md` (open
items). This is about test coverage, not features.

Sources are the actual test files:
- Kudu: `src/kudu/cdc/*-test.cc`, `src/kudu/integration-tests/cdc_failover-itest.cc`,
  `src/kudu/master/cdc_manager-test.cc`.
- YugabyteDB: `~/yugabyte-db/src/yb/integration-tests/cdcsdk_*` and
  `src/yb/integration-tests/cdc_*`, counted on 2026-08-05.

A large part of YB's suite exercises features that do not apply to Kudu (see
section 4). "Coverage" below is measured only against scenarios that are
meaningful for Kudu's design.

---

## 1. Suite inventory (counts)

### Kudu (~105 CDC tests)

| File | Tests | Scope |
|---|---|---|
| `cdc/cdc_service-test.cc` | 44 | GetChanges/Checkpoint RPC, FULL mode, snapshot, admission, authz, metrics (mini-cluster) |
| `cdc/cdc_util-test.cc` | 18 | WAL-record -> `CDCRecordPB` decode unit tests |
| `cdc/cdc_client-test.cc` | 6 | Consumer-side record decode |
| `integration-tests/cdc_failover-itest.cc` | 23 | End-to-end: leader failover, retention, expiry, txns, snapshot |
| `master/cdc_manager-test.cc` | 13 | Stream CRUD + checkpoint persistence on the master |

### YugabyteDB CDCSDK (~600 tests; xCluster excluded)

| File | Tests | Scope |
|---|---|---|
| `cdcsdk_ysql-test.cc` | 278 | The catch-all CDCSDK suite: GetChanges, DDL, txns, cleanup, retention, colocation, replication slots |
| `cdcsdk_consumption_consistent_changes-test.cc` | 102 | Virtual WAL / `GetConsistentChanges` / LSN determinism / publication refresh |
| `cdcsdk_consistent_snapshot-test.cc` | 41 | Consistent-snapshot establishment + rollback-failure matrix |
| `cdcsdk_before_image-test.cc` | 32 | Before-image + history retention + compaction interaction |
| `cdc_service-int-test.cc` | 32 | Lower-level cdc_service (mixes xCluster + CDCSDK: checkpoint, min-replicated-index, safe time, log retention) |
| `cdcsdk_consistent_stream-test.cc` | 22 | Ordered streaming with many txns |
| `cdcsdk_snapshot-test.cc` | 20 | Non-consistent snapshot mechanics |
| `cdcsdk_replica_identity-test.cc` | 20 | PG replica-identity before-image modes |
| `cdcsdk_stream-test.cc` | 19 | Stream lifecycle (create/list/delete, checkpoint validate) |
| `cdcsdk_tablet_split-test.cc` | 11 | Tablet-split lineage handoff |
| `cdc_state_table-test.cc` | 11 | `cdc_state` system-table CRUD |
| `cdc_intratx_before_image-test.cc` | 7 | Before-image within a single txn |
| `cdc_service-txn-test.cc` | 3 | Transaction-focused service tests |
| `cdcsdk_gflag-test.cc` | 1 | Flag validation |

The raw count gap (~105 vs ~600) overstates the real gap: roughly half of YB's
tests target replication-slot / Virtual-WAL / colocation / YSQL surfaces that
Kudu does not implement (section 4). The design-relevant gap is narrower and is
itemized in section 3.

---

## 2. Scenario-category matrix

Legend: YES = comparable scenario exists; PARTIAL = some but shallower; NO =
absent; N/A = feature does not apply to Kudu.

| Category | YB | Kudu | Kudu tests (representative) |
|---|---|---|---|
| Basic GetChanges (insert/update/delete) | YES | YES | `GetChanges_AfterInserts`, `_MultiRowBatch`, `cdc_util` `InsertSingleRow`/`DeleteSingleRow`/`UpsertSingleRow` |
| Response pagination / byte cap | YES | YES | `GetChanges_Pagination`, `GetChanges_HaveMoreRecords`, `TestGetChangesResponseSize` (YB) |
| Caught-up / empty read | YES | YES | `GetChanges_CaughtUp` (unit + itest) |
| Record decode edge cases (null, missing schema, OOB) | PARTIAL | YES | `cdc_util` `NullableColumnIsNull`, `WriteOpMissingSchema`, `DecodeWriteOpRow_OutOfBounds` |
| Before-image / FULL mode | YES (32+ tests) | PARTIAL | `FullMode_*` (7 tests), `cdc_client` `DecodeFullImageWithBeforeAndNull` |
| Before-image after ADD COLUMN | YES | YES | `FullMode_BeforeImageAcrossAddColumn` (added column present-but-null in before/after image, then null->non-null on the new column), `FullMode_BeforeImageNullToNullUpdate` |
| Before-image vs compaction / history GC | YES | YES | `FullMode_HistoryFloorProtectsBeforeImageAcrossCompaction` (floor survives FORCE_COMPACT_ALL), `FullMode_BeforeImageGcedReturnsHistoryExpired` (GC'd history -> HISTORY_EXPIRED, not wrong data), `FullMode_SetsHistoryFloor` |
| Consistent snapshot (point-in-time) | YES (41) | PARTIAL | `Snapshot_Basic`, `_HandoffToWal`, `Snapshot_EndToEnd` |
| Snapshot pagination / resume | YES | YES | `Snapshot_Pagination`, `_ResumesFromServerAuthoritativeKey`, `_MaxKeyDoesNotWedgePagination` |
| Insert/update/delete during snapshot | YES | YES | `Snapshot_ConcurrentDmlCapturedExactlyOnce` (DML mid-drain captured exactly once across the snapshot->WAL handoff) |
| Leadership change during snapshot | YES | PARTIAL | `Snapshot_ResumeWithoutSessionRejected` (rejects stale resume; no full mid-snapshot failover replay) |
| Server failure during snapshot | YES | NO | -- (`TestServerFailureDuringSnapshot`) |
| Snapshot no-data / invalid from-op | YES | PARTIAL | `Snapshot_RejectedWhenModeNever` (no `SnapshotNoData` equivalent) |
| Checkpoint advance + durability | YES | YES | `Checkpoint_AdvancesAnchor`, `cdc_manager` `UpdateCheckpoint_*`, `_PersistsAcrossRestart` |
| Checkpoint monotonicity | (implicit) | YES | `UpdateCheckpoint_IsMonotonic` (Kudu-specific hardening) |
| Checkpoint = min over multiple streams | YES | PARTIAL | `MultipleStreams_IndependentCheckpoints` (independent, not min-fanout assert) |
| Leader failover continuity | YES | YES | `GetChanges_AfterLeaderFailover`, `RetentionBarrierPushedToAllReplicas` |
| WAL retention by op-index | YES | YES | `SetRetentionBarrier`, `FullMode_HistoryFloorPushedToAllReplicas` |
| Retention barrier reorder / last-writer-wins | NO | YES | `SetRetentionBarrier_LastWriterWinsOnReorder` (Kudu-specific) |
| Barrier-release fan-out cap | YES | YES | `BarrierReleaseFanoutIsCappedButRetried` |
| Stream expiry / GC (idle + non-advancing) | YES | YES | `Expiry_ReleasesBarrierOnAllReplicas`, `MaxStaleness_ReleasesBarrierDespitePolling` |
| Premature WAL GC error code | YES | YES | `GetChanges_WalGcedBelowFromOpIndexReturnsWalExpired` (replay of a GC'd from_op_index returns in-band `WAL_EXPIRED`) |
| Stream lifecycle CRUD | YES | YES | `cdc_manager` `CreateStream_*`/`ListStreams_*`/`DeleteStream_*`/`GetStreamInfo_*` |
| Stream persists across master restart | YES | YES | `DeleteStream_PersistsAcrossRestart`, `UpdateCheckpoint_PersistsAcrossRestart` |
| Transactional writes: commit emitted | YES | YES | `TransactionalWrites`, `cdc_util` `ParticipantOp_Commit` |
| Transactional writes: abort dropped | YES | YES | `cdc_util` `ParticipantOp_Abort`, `_BeginCommitSkipped` |
| Large transaction (no wedge) | YES | YES | `LargeTransactionDoesNotWedgeStream`, `TransactionExceedingSpanCapFailsLoudly` |
| Multi-shard / cross-tablet txn ordering | YES (102) | NO | -- (Virtual WAL; N/A unless D1/D4 pursued) |
| Savepoints / partial-rollback in txn | YES | NO | -- (`TestCDCWithSavePoint`) |
| Schema change (ALTER) mid-stream (DDL record) | YES | YES | `AlterTableMidStream_EmitsDdlThenNewSchemaRows` (end-to-end: pre-ALTER rows -> DDL record -> post-ALTER rows, correctly typed), plus decode-level `cdc_util` `AlterSchemaOp`, `SchemaVersion_CommittedUnappliedAlterStampsPreAlterVersion` |
| need_schema_info | YES | YES | `NeedSchemaInfo_PrependsCurrentSchema` |
| Schema-by-version lookup | YES | NO | -- (gap D3) |
| Tablet split lineage handoff | YES (11) | NO | -- (Kudu has no split; range-partition drop is the analog: `DropRangePartition_CleansCheckpoints`) |
| Range/partition change cleanup | (n/a) | YES | `DropRangePartition_CleansCheckpoints` (Kudu-specific analog) |
| Error: unknown/deleted stream | YES | YES | `GetChanges_UnknownStreamReturnsStreamNotFound`, `_DeletedStreamReturnsStreamNotFound` |
| Error: not leader / non-leader | YES | YES | `GetChanges_NonLeader`, `_PostReadLeadershipLossRejected` |
| Error: tablet not found / not running | YES | YES | `GetChanges_TabletNotFound`, `_TabletNotRunning` |
| Error: invalid from_op_index | YES | YES | `GetChanges_NegativeFromOpIndexRejected`, `TestSnapshotWithInvalidFromOpId` (YB) |
| Error: INITIAL_ONLY streaming rejected | (n/a) | YES | `GetChanges_InitialOnlyRejectsWalStreaming` |
| Admission control / RPC reservation | YES | YES | `Admission_GetChangesRpcWorkerReservation`, `Isolation_*` (3), `Snapshot_SafeDeadlineRatioReservesHeadroom` |
| Deadline handling on slow read | YES | YES | `Snapshot_HonorsDeadlineWhenEstablishSlow`, `TestHitDeadlineOnWalReadMidTransaction` (YB) |
| Safe-time / SAFEPOINT signal | YES | NO | -- (`TestSafeTime`; gap D1) |
| Concurrency races (start/config fetch) | PARTIAL | YES | `Snapshot_ConcurrentStartsEstablishOnce`, `StreamConfig_ConcurrentMissesSingleFlight` |
| Stream-config cache staleness | (n/a) | YES | `StreamConfig_CacheEntryExpiresAndRefetches` |
| Metrics / lag gauges | YES (rich) | PARTIAL | `Metrics_TrackConsumerActivity`, `_PerStreamGauges` (no lag/throughput gauge like YB `TestUpdateLagMetrics`) |
| Authorization | PARTIAL | YES | `CDCAuthzTest` (4): missing/wrong-table/no-scan-priv token, valid token |
| Colocated tables | YES (many) | N/A | -- |
| Replication slots / LSN / ordering mode | YES (many) | N/A | -- |
| Bootstrap producer / IsBootstrapRequired | YES | N/A (covered by snapshot) | -- |

---

## 3. Gaps: YB scenarios worth adding to Kudu

Ordered by value. These are scenarios that *do* apply to Kudu's design and that
YB tests but we do not.

1. ~~**DML concurrent with snapshot**~~ **(DONE:
   `Snapshot_ConcurrentDmlCapturedExactlyOnce`)** (YB:
   `InsertBeforeDuringAfterSnapshot`, `UpdateInsertedRowSnapshot`,
   `DeleteInsertedRowSnapshot`, `InsertedRowInbetweenSnapshot`). Now covered: the
   test paginates a snapshot, injects inserts/updates/deletes after the session
   establishes but before it finishes draining, and asserts (a) snapshot
   isolation -- the READ set is exactly the pre-DML table as of snap_ts; (b) the
   WAL stream from the handoff op-index is exactly the concurrent DML, each op
   once (no pre-snapshot row replayed, no mutation dropped); and (c) snapshot +
   WAL replay reconstructs the live table exactly (checked against a direct
   scan). This directly exercises the snapshot-timestamp -> op-index handoff
   boundary.

2. **Server/leader failure mid-snapshot** (YB: `TestServerFailureDuringSnapshot`,
   `TestLeadershipChangeDuringSnapshot`, `TestCheckpointUpdatedDuringSnapshot`).
   `Snapshot_ResumeWithoutSessionRejected` covers the "reject stale resume" half;
   the "resume correctly on the new leader and finish the snapshot" half is not
   tested end-to-end.

3. ~~**ALTER TABLE during an active stream, end-to-end**~~ **(DONE:
   `AlterTableMidStream_EmitsDdlThenNewSchemaRows`)** (YB: many, e.g.
   `TestMultipleTableAlterWithSnapshot`, add-column paths). Previously we had only
   decode-level schema-version stamping (`SchemaVersion_CommittedUnappliedAlter...`,
   `cdc_util` `AlterSchemaOp`). Now covered end-to-end: the test writes rows on the
   original schema, runs an online ALTER that adds a nullable column, writes rows
   on the new schema, then drains the stream from the start and asserts the WAL
   order -- pre-ALTER rows decoded with the old schema (no added column), then
   exactly one DDL record carrying the post-ALTER schema (added column present,
   `new_schema_version == schema_version + 1`), then post-ALTER rows decoded with
   the new schema (added column present and correctly typed). This exercises the
   ALTER_SCHEMA_OP -> DDL-record path and the running-schema-version stamping on
   both sides of the ALTER over GetChanges, not just the decode unit.

4. ~~**Before-image after ADD COLUMN / on null-to-null update**~~ **(DONE:
   `FullMode_BeforeImageAcrossAddColumn`, `FullMode_BeforeImageNullToNullUpdate`)**
   (YB: `TestAddColumnBeforeImage`, `TestBeforeImageForNewlyAddedColumn`,
   `TestBeforeImageForNullOnNullUpdates`). Previously the FULL-mode tests used a
   static schema, so reconstruction across a schema change was untested. Now
   covered: a row inserted before an online ADD COLUMN is then updated, and the
   test asserts the FULL reconstruction (which projects the *current* tablet
   schema onto rows written under the old schema) emits the newly added column
   in both the before- and after-image, reported present-but-null for the row
   that predates it; a follow-up update that sets the new column asserts the
   null->non-null transition (before-image null, after-image the new value).
   `FullMode_BeforeImageNullToNullUpdate` covers the null-on-null case: updating
   a non-null column while a nullable column stays null must still emit that
   column in both images with `is_null=true`, never dropping it or reporting a
   stale value.

5. ~~**Before-image vs. compaction race**~~ **(DONE:
   `FullMode_HistoryFloorProtectsBeforeImageAcrossCompaction`,
   `FullMode_BeforeImageGcedReturnsHistoryExpired`)** (YB:
   `TestCompactionDuringSnapshot`,
   `TestCompactionWithBeforeImageGetChangesCallFailed`). Previously we asserted
   only that the history floor is *set* (`FullMode_SetsHistoryFloor`), never
   forcing a real compaction against it. Now covered on both sides, and the
   negative side surfaced (and fixed) a real correctness bug:
   - Positive: with a FULL stream holding the floor, `tablet_history_max_age_sec`
     is turned to 0 and a `FORCE_COMPACT_ALL` merge compaction is run; the UNDO
     history the stream needs is retained and the re-read before-image is still
     correct -- the floor genuinely protects against compaction, not just stores
     a value.
   - Negative: when no floor protected the UNDO history at compaction time (a
     lapsed stream, or CDC enabled on a table with aggressive history GC), a FULL
     replay of the affected op now returns an in-band `HISTORY_EXPIRED` and
     aborts the batch, rather than silently emitting the *current* row as the
     before-image. The bug: the FULL path re-pins the history floor to each
     batch's minimum op timestamp *before* reconstruction, which lowered the
     current ancient history mark back below a point where an earlier
     (unprotected) compaction had already reclaimed the UNDO history, fooling the
     current-AHM guard into a time-travel scan of reclaimed history (which
     returns the live row). Fix: `Tablet` now tracks a monotonic history-GC water
     mark (the highest AHM ever applied during GC, recorded in
     `GetHistoryGcOpts()`); `ReconstructBeforeAfterImages` gates on that water
     mark, which the per-batch floor re-pin cannot lower. Extends correctness
     item E1 (FULL-mode must never emit a semantically wrong before-image
     silently).

6. **Safe-time / SAFEPOINT progress signal** (YB: `TestSafeTime`,
   safe-time-from-explicit-checkpoint). Deferred as gap D1; no test because the
   feature is not built.

7. ~~**Premature WAL-GC error path, end-to-end**~~ **(DONE:
   `GetChanges_WalGcedBelowFromOpIndexReturnsWalExpired`)** (YB:
   `TestWALPrematureGCErrorCode`, `TestGetChangesFromGCedCheckpointWithNewerWal`).
   Previously we relied only on the unit-level history floor; the
   "consumer fell behind WAL GC -> defined error" path was untested. Now covered
   end-to-end through the GetChanges RPC: the test fills several WAL segments,
   advances the CDC per-session anchor past the oldest by polling from a
   checkpoint (the caught-up fast path deliberately does not advance the anchor,
   so a post-checkpoint write is needed to force a non-caught-up poll), rolls +
   flushes + runs log GC to reclaim the now-unanchored oldest segment (asserting
   the segment count drops and the min replicate index moves past op index 1),
   then replays from op index 0 -- whose segment is gone -- and asserts the RPC
   succeeds at the transport layer while returning an in-band `WAL_EXPIRED` error
   (not a silent empty batch the consumer would misread as caught-up, nor an
   opaque failure). A follow-up poll from a surviving index still succeeds,
   proving GC pruned only the prefix and did not wedge the stream.

Deliberately NOT added (tracked as larger features): multi-shard/cross-tablet
transaction ordering, tablet-split lineage (Kudu has no split -- range-partition
drop is the analog and is covered by `DropRangePartition_CleansCheckpoints`).

---

## 4. YB test scenarios that do NOT apply to Kudu

These make up a large share of YB's ~600 tests and are correctly absent from our
suite (see `gaps.md` "Does NOT apply to Kudu"):

- **Replication slots / LSN / ordering-mode / walsender** -- the entire
  `TestCreateReplicationSlot*`, `LsnType*`, `OrderingMode*` family, and most of
  `cdcsdk_consumption_consistent_changes-test.cc` (102 tests). Kudu has no
  PostgreSQL logical-replication contract.
- **Virtual WAL / GetConsistentChanges / publication refresh / LSN determinism**
  -- the cross-tablet merged-stream API. N/A unless gap D4 is pursued.
- **Colocated tables** -- `TestSnapshotForColocatedTablet`, colocated cleanup /
  index / checkpoint-keying tests. Kudu has no colocation.
- **xCluster** -- `xcluster_producer-test.cc`, the xCluster half of
  `cdc_service-int-test.cc` (`TestCreateXClusterStream`, `BootstrapProducer`,
  `CheckReplicationDrain`). Separate feature, not a Kudu goal.
- **Intent-store mechanics** -- `TestIntentGC`, `TestIntentSSTFileCleanup...`,
  `IntentsAreDeletedOn...`. Tied to YB's IntentsDB; Kudu uses participant-based
  transactions with a different mechanism.
- **YSQL catalog / DB-drop / upgrade** -- `TestGRPCStreamsDroppedWhenMasterRestart...`,
  `...DuringUpgradeFrom...`, PG-catalog-tablet polling. YSQL-specific.
- **`cdc_state` system-table CRUD** (`cdc_state_table-test.cc`, 11 tests) -- Kudu
  stores checkpoints in the master sys-catalog; the equivalent coverage lives in
  `cdc_manager-test.cc` instead.

---

## 5. Summary

- **Parity is strong on the core contract.** Every fundamental CDCSDK scenario --
  basic GetChanges, pagination, checkpointing + durability, leader-failover
  continuity, WAL retention, stream expiry, stream CRUD, per-tablet transactional
  commit/abort, snapshot + WAL handoff, error-code classification, admission
  control, authz -- has a Kudu test.
- **Kudu tests some things YB does not**, reflecting hardening of our own design:
  retention-barrier reorder (`_LastWriterWinsOnReorder`), checkpoint monotonicity,
  stream-config cache staleness / single-flight, INITIAL_ONLY streaming rejection.
- **The remaining coverage gaps are concentrated in snapshot/stream interaction
  under concurrency and schema change** (section 3): DML during snapshot (item 1),
  ALTER mid-stream end-to-end (item 3), before-image across ADD COLUMN /
  null-to-null updates (item 4), before-image vs. compaction (item 5), and the
  premature WAL-GC error path (item 7) are now done; the remaining open item is
  failover mid-snapshot (item 2). That is the recommended next addition.
- **Roughly half of YB's suite is out of scope for Kudu** (replication slots,
  Virtual WAL, colocation, xCluster, IntentsDB), so the ~105-vs-~600 raw ratio is
  not the coverage gap.

_Last updated: 2026-08-05_
