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

# Kudu CDC: production-grade server-side implementation plan

This is the target-architecture and delivery plan for a production-grade,
YugabyteDB-inspired server-side CDC in Kudu. Its companion `YB_KUDU_CDC_COMPARISON.md`
explains *why* (the gap analysis); this document is the *what and how*, mapping
each capability YB's CDCSDK provides onto a concrete, Kudu-idiomatic mechanism. It
supersedes the earlier incremental phase list. The Debezium connector is out of
scope here - it is a downstream consumer that mostly falls out of a solid server
contract.

## Implementation status

- Phase 1 (leader-change anchor restoration): DONE (goal satisfied by Phase 2's
  continuous all-replica push; an immediate-on-election fast path remains an
  optional latency optimization).
- Phase 2 (all-replica retention barrier): DONE and verified. `UpdateCDCRetentionBarrier`
  admin RPC (`tserver_admin.proto`), tserver handler + `CDCServiceImpl::SetRetentionBarrier`
  (per-tablet anchor on every replica, incl. followers), and master
  `RunCDCStreamMaintenance()` fan-out via `CatalogManagerBgTasks`
  (`--cdc_bg_scan_interval_ms`). Tests: `cdc_service-test` SetRetentionBarrier;
  `cdc_failover-itest` RetentionBarrierPushedToAllReplicas (verifies a follower
  registers the barrier).
- Phase 3 (transaction consistency): DONE and verified.
  `CDCRecordPB.commit_timestamp` added; `ReadChanges` buffers `WRITE_OP`s
  carrying `txn_id` and emits BEGIN + rows (stamped with
  `ParticipantOpPB.finalized_commit_timestamp`) + COMMIT on `FINALIZE_COMMIT`,
  drops on `ABORT_TXN`. Correctness rule: the oldest still-open transaction pins
  the stream - records at/after its first write are deferred and the checkpoint
  is capped at (its first index - 1), so a transaction is never partially
  published and never lost (re-read next batch until it commits). Chosen
  semantics: per-tablet, commit-order, head-of-line-blocking on the oldest open
  txn (no cross-tablet ordering). Tests: `cdc_service-test` (non-txn regression,
  9 tests) and `cdc_failover-itest.TransactionalWrites` (committed txn emits
  BEGIN + rows + COMMIT with commit_timestamp; aborted txn rows dropped).
- Phase 4 (before/after image, RecordType.FULL): DONE and verified.
  `CDCStreamConfigPB.RecordType.FULL` promoted from reserved; `CDCRecordPB.old_changes`
  added for the before-image. New MVCC history barrier: `Tablet::SetCDCHistoryFloor`
  clamps `GetTabletAncientHistoryMark` (`tablet/tablet.cc`) so UNDO deltas CDC needs
  are not GC'd; delivered on every replica via a new
  `UpdateCDCRetentionBarrierRequestPB.history_safe_time_micros`
  (`tserver_admin.proto`) that `SetRetentionBarrier` applies, with the master
  computing the per-tablet min floor across FULL streams in
  `RunCDCStreamMaintenance` and persisting it via
  `SysCDCStreamEntryPB.tablet_history_floors` +
  `UpdateCDCCheckpointRequestPB.history_safe_time_micros`. `ReadChanges` gates on
  `record_type` (fetched + cached via `GetOrFetchStreamConfig`) and calls
  `cdc_util.cc::ReconstructBeforeAfterImages`, which opens one before-image
  (`MvccSnapshot(T)`) and one after-image (`MvccSnapshot(T+1)`) ordered scan per
  WRITE_OP over the touched PK range. Transactional writes reconstruct at the
  commit timestamp (deferred to FINALIZE_COMMIT), matching Kudu's
  apply-at-commit semantics. UPSERT is reclassified to INSERT/UPDATE by whether a
  before-image row exists; DELETE keeps a key-only after-image; a before-image
  whose history has been GC'd returns `CDCErrorPB::HISTORY_EXPIRED`. Tests:
  `cdc_service-test` FULL insert/update/delete + history-floor set/not-set;
  `cdc_failover-itest.FullImage_EndToEnd` and
  `FullMode_HistoryFloorPushedToAllReplicas`.
- Phase 5 (server-driven consistent snapshot): DONE and verified.
  `CDCStreamConfigPB.SnapshotMode` (INITIAL_AND_CONTINUE/NEVER/INITIAL_ONLY),
  `CDCOpTypePB.READ`, and snapshot fields on `GetChangesRequestPB`
  (`is_snapshot_start`, `snapshot_resume_key`) / `GetChangesResponsePB`
  (`snapshot_done`, `snapshot_streaming_start_op_index`, `snapshot_resume_key`).
  `CDCServiceImpl::ReadSnapshot` captures a snapshot `Timestamp` + the committed
  op-index at start, waits via `TimeManager::WaitUntilSafe` +
  `MvccManager::WaitForSnapshotWithAllApplied`, pins history at snap_ts, and
  emits paginated READ records from an ordered `NewRowIterator`, resuming after
  an encoded-PK key each page; on exhaustion it reports `snapshot_done` and the
  streaming start op-index for WAL hand-off. Tests: `cdc_service-test` snapshot
  basic/pagination/handoff/mode-NEVER; `cdc_failover-itest.Snapshot_EndToEnd`.
- Phase 6 (lifecycle/GC completeness): DONE and verified. (a) Idle-stream expiry:
  `SysCDCStreamEntryPB.last_active_time_micros` stamped by the master on
  `CreateCDCStream`/`UpdateCDCCheckpoint`; a new `--cdc_stream_expiry_ms` (default
  8h, matching YB's `cdc_intent_retention_ms`) excludes idle streams from the
  retention-min in `RunCDCStreamMaintenance`, mirroring YB's keep-stream/
  release-barrier model (no hard delete). The tserver refreshes activity from
  `GetChanges` via a throttled (`--cdc_active_time_report_interval_ms`)
  active-time-only `UpdateCDCCheckpoint` (`refresh_active_time_only`). (b) Barrier
  release: `RunCDCStreamMaintenance` is now reconciling - it diffs against
  `cdc_barriered_tablets_` and sends the release sentinel (`min_retained_op_index
  = -1`) for tablets no longer pinned; `DeleteCDCStream` releases orphaned tablets
  synchronously. Dropped-partition tablets (absent from `tablet_map_` or
  `is_deleted()`) are pruned from every stream's checkpoint/history maps.
  `TSTabletManager::DeleteTablet` calls `CDCServiceImpl::ReleaseAnchorsForTablet`
  before shutdown so a deleted tablet's `LogAnchorRegistry` is empty. (c) On-demand
  schema: `GetChangesRequestPB.need_schema_info` prepends a synthetic DDL record
  with the tablet's current schema without advancing the checkpoint. Tests:
  `cdc_service-test` NeedSchemaInfo; `cdc_failover-itest`
  Expiry_ReleasesBarrierOnAllReplicas, DeleteStream_ReleasesRetention,
  DropRangePartition_CleansCheckpoints.
- Phase 7 (fine-grained authz + lag metrics): DONE and verified. (a) Authz: a new
  `--cdc_enforce_access_control` gate requires `GetChanges`/`Checkpoint` to carry a
  signed `authz_token` (reusing the scan-token machinery: `TokenVerifier` +
  `ParseTokenVerificationResult`) granting SCAN privilege on the tablet's table;
  rejections use the scan path's RPC error codes so clients refresh tokens.
  `authz_token` added to `GetChangesRequestPB`/`CheckpointRequestPB`. (b) Metrics:
  three server-level `FunctionGauge`s - `cdc_max_sent_lag_micros`,
  `cdc_max_active_age_micros`, `cdc_active_streams` (all wall-clock/count, no WAL
  I/O), following YB's time-based lag model. Tests: `cdc_service-test`
  Metrics_TrackConsumerActivity + CDCAuthzTest (missing/wrong-table/no-scan/valid).

## 1. Scope, principles, and non-goals

Borrow YB's *principles*, not its machinery. Research on both trees confirms
several YB subsystems do not apply to Kudu, which lets the Kudu design be
substantially leaner:

- **No online tablet splitting in Kudu** (`docs/schema_design.adoc:645`;
  `client/meta_cache.cc:537`). The only runtime tablet-set change is `ADD`/`DROP
  RANGE PARTITION`. So YB's split-lineage machinery
  (`GetTabletListToPollForCDC` parent-drain-then-children) is **N/A**.
- **No PostgreSQL logical-replication contract.** YB's Virtual WAL, LSN/txn-id
  generation, and replication-slot RPCs (`InitVirtualWALForCDC`,
  `GetConsistentChanges`, `UpdateAndPersistLSN`) exist to serve PG walsender
  semantics. **N/A** for Kudu.
- **Simpler transactions.** Kudu uses participant ops, not a distributed
  transaction-status model with a separate intents DB. YB's intent-retention
  barrier and cross-tablet virtual-WAL merger are **not required** for the common
  single-tablet-consumer case.
- **No xCluster / BootstrapProducer** goal here.

What Kudu *does* need to match, and where it maps:

## 2. Target architecture

### A. Stream and state model (durable)

- Primary store: master sys-catalog. `SysCDCStreamEntryPB` is persisted as a
  CowObject `CDCStreamInfo` (`master/catalog_manager.h:520-551`; write path
  `master/sys_catalog.cc:1022`), and already carries the tables, `config`,
  `state`, and a per-tablet `tablet_checkpoints` map
  (`master/master.proto:1248-1263`). This survives master failover and is loaded
  into memory on leader election (`LoadCDCStreams`).
- Extend `CDCStreamConfigPB` (`master.proto` / `cdc/cdc.proto`): `record_type`
  CHANGE|FULL (both now implemented; FULL was a reserved enum), an
  `active_time`/expiry notion, and a
  snapshot option.
- Tserver-side durability: add a per-tablet `cdc_min_retained_op_index` (and, for
  FULL streams, `cdc_history_safe_time`) to the tablet superblock
  (`tablet/metadata.proto` `TabletSuperBlockPB` / `TableExtraConfigPB`). Written by
  the retention-barrier RPC handler and flushed via `TabletMetadata::Flush`, so a
  restarted tserver restores its retention floors without a master round-trip.
- Scale note: the embedded `tablet_checkpoints` map rewrites the whole stream row
  on each checkpoint update - fine up to a few hundred tablets per stream. The
  migration path (not needed for v1) is a dedicated internal `cdc_state`-style
  table with one row per (stream, tablet), as YB does. Design the checkpoint key
  now so this migration is clean.

### B. Retention subsystem (the prod-grade core)

This is the analog of YB's `UpdateCdcReplicatedIndex` + 60s background scan, and
it is the single most important piece for correctness under consumer lag and
leader change.

Problem. A CDC `LogAnchor` today floors WAL retention (`for_durability` in
`RetentionIndexes`, `consensus/log.h:531`) only on the **leader**
(`tablet/tablet_replica.cc:740` assembles the floors; `consensus/log.cc` GC uses
them). There are two independent holes:

1. Leader-change window: a new leader has no CDC anchor until the consumer's next
   poll reaches it; the maintenance manager can GC needed segments in between.
2. Follower GC: every replica runs `LogGCOp` independently with
   `for_durability = committed_index` and no CDC anchor, so a follower can GC
   segments a future consumer still needs *before* it ever becomes leader. Anchor
   restoration on the new leader cannot recover already-deleted data.

Design.

- **Master background task.** Add `RunCDCStreamMaintenance()` to the leader branch
  of `CatalogManagerBgTasks::Run()` (`master/catalog_manager.cc:798`), gated by a
  new `--cdc_bg_scan_interval_ms` (default ~60s). Each pass: (a) compute the
  per-tablet minimum checkpoint across all active streams; (b) expire idle streams
  by `active_time`; (c) push a retention barrier to **every replica** of each CDC
  tablet.
- **All-replica fan-out.** Reuse the `SendDeleteTabletRequest` pattern
  (`master/catalog_manager.cc:5982`): one `RetrySpecificTSRpcTask` per peer in
  `cstate.committed_config().peers()` (the `RetryingTSRpcTask` / `TSPicker`
  machinery, `catalog_manager.cc:4535-4878`). This guarantees the barrier is set
  on followers too, so it holds across a leadership change.
- **New tserver admin RPC** `UpdateCDCRetentionBarrier(tablet_id, min_op_index,
  history_safe_time)`. The handler looks up the `TabletReplica` and calls
  `replica->log_anchor_registry()->RegisterOrUpdate(min_op_index, ...)` - anchor
  registration already works on followers (proven by MemRowSet and
  transaction-participant anchors) - and sets the history barrier; then persists
  the floors to the superblock.
- **Two barriers.** WAL retention (the `LogAnchor` floors `for_durability`) and
  MVCC history retention (floors `GetTabletAncientHistoryMark`,
  `tablet/tablet.cc:1523`). The history barrier is set **only** for FULL/snapshot
  streams, so change-only streams keep the cheap path (no history hold-back).
- **Leader-change fast path.** Hook `MarkDirtyCallback` / `BecomeLeaderUnlocked`
  (`consensus/raft_consensus.h:111`) so a newly elected leader restores its anchor
  immediately from the persisted checkpoint; the background scan is the
  steady-state, all-replica path.
- **Alternative considered, not chosen for v1.** Piggyback an optional
  `cdc_min_retain_index` on `ConsensusRequestPB` (`consensus/consensus.proto`) so
  every Raft heartbeat delivers the floor for free. Lowest latency, but it changes
  a hot-path proto and couples consensus to CDC. Prefer the RPC + background scan
  (isolated and idiomatic); revisit the piggyback as an optimization.

### C. Read / producer path (GetChanges)

- WAL read bounded by `COMMITTED_OPID` via `LogReader::ReadReplicatesInRange`
  (exists, `cdc/cdc_service.cc`).
- **Transaction consistency.** Buffer `WRITE_OP`s that carry a `txn_id`, keyed by
  transaction, in `CDCStreamTabletState`. Release the buffer on the
  `PARTICIPANT_OP FINALIZE_COMMIT` for that txn - stamping the commit timestamp
  (`tablet/ops/participant_op.cc:188`) - emitting BEGIN, the rows, then COMMIT
  (optionally a SAFEPOINT). Drop the buffer on `ABORT_TXN`. Never advance the
  returned checkpoint past the oldest still-open buffered transaction, so a txn
  spanning multiple GetChanges responses is never partially published. Bound
  buffer memory and fail the batch with a clear error on overflow. Ordering is
  per-tablet (cross-tablet is section H).
- **Record model / images.** `record_type` gates cost. CHANGE = today's WAL-only
  changed columns (plus key on delete), no history barrier. FULL = full
  after-image and before-image, reconstructed via
  `Tablet::NewRowIterator(RowIteratorOptions, ...)` (`tablet/tablet.h:269`, options
  in `tablet/rowset.h:70`) at the op timestamp (after) and at op timestamp minus
  one (before); this requires the history barrier from (B). Add before/after
  column lists and `commit_timestamp` to `CDCRecordPB`.
- **Schema.** Correct `schema_version` (done). Add `need_schema_info` to
  `GetChangesRequestPB`; when set (or for a fresh consumer), prepend a DDL/schema
  record so a consumer starting mid-stream after an ALTER has a base schema.
  Reconstructing the schema in effect at an arbitrary past timestamp is a
  documented limitation (use current tablet schema plus emitted DDL records).

### D. Snapshot subsystem (server-driven consistent snapshot)

- Add a snapshot mode to GetChanges (a request flag or `from_op_index` sentinel).
  On the first snapshot call, capture a snapshot `Timestamp` (clock now) and the
  tablet's current committed op-index as the streaming start point; scan via
  `NewRowIterator` in READ_AT_SNAPSHOT mode, paginated by primary key returned in
  the checkpoint, emitting READ records; when the scan is exhausted, hand off to
  streaming from the captured op-index. The history barrier (B) keeps the snapshot
  timestamp readable for the snapshot's duration. Per-stream mode
  (initial / never / initial_only).

### E. Lifecycle and GC

- Create / Delete / List / GetInfo / UpdateCDCCheckpoint on the master exist. On
  `DeleteCDCStream` and on `DROP RANGE PARTITION`, drop the affected tablets from
  the background-scan minimum computation so the barrier rises to max and WAL /
  history are freed on all replicas.
- Idle-stream expiry via `active_time` in the background scan (YB
  `cdc_intent_retention_ms` analog); this is also the safeguard against a stuck
  FULL consumer pinning history forever.
- No split lineage: new tablets from `ADD RANGE PARTITION` are discovered by the
  consumer via `GetTableLocations`; dropped-partition tablets are cleaned up.

### F. RPC surface delta

- Add: `UpdateCDCRetentionBarrier` (master to all tserver replicas); snapshot-mode
  fields on `GetChanges` (prefer extending the existing RPC over a new one);
  `need_schema_info` on `GetChangesRequestPB`; before/after columns +
  `commit_timestamp` (+ SAFEPOINT op) on `CDCRecordPB`.
- Keep: `GetChanges`, `Checkpoint`; master CDC CRUD + `UpdateCDCCheckpoint`.
- Skip (N/A): the Virtual WAL trio, intents machinery, split-lineage tablet-list
  semantics, `BootstrapProducer` / xCluster.

### G. Security and observability

- Authz: coarse authenticated client/service/super user is in place; add
  fine-grained per-table authorization using authz tokens (reuse the scan-token
  machinery).
- Metrics: the request/record/error counters are in place; add gauges for consumer
  lag (committed op-index minus consumer checkpoint), WAL-anchor age,
  history-barrier age, and active stream count.

### H. Cross-tablet ordering (optional; deferred)

Only if multi-tablet transactional consistency becomes a goal. Prefer a
client-side merger keyed on commit HybridTime with a per-tablet safe-time gate; do
not build YB's server-side Virtual WAL.

## 3. YB -> Kudu mapping

| YB mechanism | Kudu equivalent in this design |
|---|---|
| `cdc_state` table | sys-catalog `SysCDCStreamEntryPB.tablet_checkpoints` (+ superblock field); dedicated table later |
| `UpdateCdcReplicatedIndex` + 60s scan | `UpdateCDCRetentionBarrier` RPC + `RunCDCStreamMaintenance()` in `CatalogManagerBgTasks` |
| `cdc_min_replicated_index` (WAL barrier) | per-tablet CDC `LogAnchor` on every replica |
| `cdc_sdk_safe_time` (history barrier) | floor on `GetTabletAncientHistoryMark` (FULL/snapshot only) |
| intent retention | N/A (no intents DB) |
| record types / replica identity | `CDCStreamConfigPB.record_type` CHANGE\|FULL |
| before-image via DocDB read | `NewRowIterator` MVCC read at op-ts minus one |
| server consistent snapshot | GetChanges snapshot mode + `NewRowIterator` READ_AT_SNAPSHOT |
| `need_schema_info` | `need_schema_info` on `GetChangesRequestPB` |
| stream expiry (`active_time`) | `active_time` + background-scan expiry |
| Virtual WAL / LSN / slots | N/A |
| split lineage | N/A (no tablet splitting) |

## 4. Phased delivery

Sequenced by dependency; each phase lands with its own tests, green before the
next begins.

1. **Retention part 1 - leader-change anchor restore** (small). Hook
   become-leader to restore the anchor from the persisted checkpoint. Closes hole
   #1.
2. **Retention part 2 - all-replica barrier** (medium; prod-grade keystone).
   Background scan + `UpdateCDCRetentionBarrier` + superblock persistence. Closes
   hole #2 (follower GC). Biggest correctness/durability win; this is what makes
   consumer lag safe.
3. **Transaction consistency** (large; defines the record contract - settle before
   4/5). Buffer/commit/abort + safe-time + BEGIN/COMMIT.
4. **Before/after image (RecordType.FULL)** (large; depends on the history barrier
   from #2).
5. **Server-driven consistent snapshot** (large; depends on the history barrier
   from #2).
6. **Lifecycle/GC completeness** (medium): stream expiry, delete / drop-partition
   barrier release, on-demand schema (`need_schema_info`).
7. **Fine-grained authz + lag metrics** (small-medium).
8. **Cross-tablet ordering** (optional).

## 5. Risks and open decisions

- Barrier delivery: RPC + background scan (chosen) vs. Raft-heartbeat piggyback
  (faster, but hot-path coupling). Decide before building Phase 2.
- sys-catalog checkpoint-map scale vs. a dedicated `cdc_state` table (defer, but
  key the checkpoints so migration is clean).
- Transaction buffer memory bound and overflow behavior (error vs. spill).
- History-barrier vs. compaction/space: a stuck FULL consumer can bloat a tablet;
  bound it with the Phase 6 max-lag/expiry safeguard.

## 6. Verification

- Unit: extend `src/kudu/cdc/cdc_util-test.cc` (transaction buffering,
  before/after decode) and `src/kudu/cdc/cdc_service-test.cc` (RPC behavior,
  retention-barrier handler).
- Integration: extend `src/kudu/integration-tests/cdc-itest.cc` and
  `cdc_failover-itest.cc`; add a before-image itest and a snapshot itest modeled on
  YB's `cdcsdk_before_image-test.cc` / `cdcsdk_snapshot-test.cc`; and a
  follower-GC-across-leader-change itest for Phase 2 (write, checkpoint, kill the
  leader, assert the new leader still retains the needed WAL).
- Build affected targets incrementally with ninja:
  `ninja cdc cdc_service-test cdc-itest cdc_failover-itest cdc_util-test`.
