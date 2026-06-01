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

# CDC server/client gap analysis: Kudu vs. YugabyteDB

## 1. Purpose and scope

This document compares Apache Kudu's Change Data Capture (CDC) implementation
against YugabyteDB's CDCSDK, on two axes:

1. **Server-side CDC** - the tablet server and master machinery that tails the
   WAL, manages streams and checkpoints, and produces change records.
2. **Pure Kudu-client CDC API** - the RPC/contract surface a client uses to
   consume changes, independent of any downstream framework.

The goal is to enumerate what is **missing in Kudu relative to YugabyteDB**,
where it applies, and to serve as the roadmap for the remaining server-side
work. It deliberately treats the Debezium connector as out of scope: the
connector is a downstream consumer, and most connector complexity either
compensates for a missing server primitive or is inherent Kafka Connect
plumbing. Build the server contract first; the connector largely follows.

YugabyteDB is a useful reference because it is architecturally close to Kudu:
tablet-sharded, Raft-replicated per tablet, HybridTime clock, LSM storage with
MVCC. Where YB features derive from parts of its architecture Kudu does not
share (see section 5), that is called out rather than treated as a gap.

References use repo-relative paths: Kudu paths are under this tree; YugabyteDB
paths (`src/yb/...`) refer to the yugabytedb/yugabyte-db source.

## 2. TL;DR gap table

Re-audited against the checkpoint commit on 2026-08-03, and refreshed 2026-08-24
for the admission-control, edge-case error-code, and metrics rows (the B/C
hardening landed after the first audit). Many rows that were gaps in the initial
design have since been implemented; the table now reflects the actual code.

| Capability | YugabyteDB | Kudu today | Gap severity | Applies to Kudu? |
|---|---|---|---|---|
| Read path / GetChanges | `GetChanges` + safe-time bounded WAL read | `CDCService.GetChanges`, committed-index bounded | none (parity) | yes |
| Durable checkpoint store | replicated `cdc_state` system table | master sys-catalog via `PersistCheckpoint`/`UpdateCDCCheckpoint` | none (done) | yes |
| Leader-change continuity | new leader reads `cdc_state` | master fans retention barrier to all replicas every `cdc_bg_scan_interval_ms` | none (done) | yes |
| Explicit checkpointing | explicit checkpoint piggybacked on GetChanges | separate `Checkpoint` RPC | minor (design choice) | yes |
| WAL/history retention barriers | index + intent + history cutoff, fanned out | op-index barrier + CDC history floor, fanned out to all replicas | none (done) | yes |
| Stream expiry / GC | time-based via `active_time` | `cdc_stream_expiry_ms` (idle) + `cdc_max_staleness_ms` (non-advancing) | none (done) | yes |
| Before-image | reconstructed from DocDB by record type / replica identity | FULL mode reconstructs from MVCC/UNDO + history floor | none (done) | yes |
| Transaction consistency (per tablet) | committed-only via intents + safe-time; BEGIN/COMMIT | buffered per `txn_id`, emitted on FINALIZE_COMMIT, dropped on ABORT | minor (done; see below) | yes |
| Transaction: safe-time + cross-tablet | safe-time gate, intents | no safe-time signal; per-tablet framing only (large-txn wedge A1 now fixed) | medium | yes |
| Cross-tablet ordering | Virtual WAL merges tablets into one LSN stream | none (per-tablet independent) | medium | partial |
| Consistent snapshot | server-driven at one `consistent_snapshot_time` | `ReadSnapshot` server-driven at chosen HybridTime | none (done) | yes |
| Split lineage | `GetTabletListToPollForCDC` parent-drains-then-children | none (generic tablet locations) | medium | yes |
| Schema delivery | `need_schema_info` + point-in-time schema + DDL records | `need_schema_info` + correct `schema_version` + DDL; no schema-by-version | minor | yes |
| Bootstrap / IsBootstrapRequired | dedicated RPCs | snapshot mode covers bootstrap; no IsBootstrapRequired RPC | minor | yes |
| Authz | coarse + per-object | authenticated user + optional signed authz token (`cdc_enforce_access_control`) | none (done) | yes |
| Streaming admission control | free-rpc-ratio + safe-deadline ratio | free-rpc-ratio + safe-deadline ratio + heavy-scan caps | none (done) | yes |
| Edge-case error codes | STREAM_EXPIRED / TABLET_SPLIT / NOT_READY set | STREAM_NOT_FOUND + TABLET_NOT_RUNNING now set; STREAM_EXPIRED still unset server-side | minor | yes |
| Metrics | rich lag/throughput | counters + server-level and per-(stream,tablet) lag/age gauges | none (done) | yes |

The remaining server-side hardening items (admission control, safe-deadline,
error-code classification, stream-config cache invalidation, and the
correctness/liveness bugs A1-A4) are tracked in detail in `../gaps.md`.

## 3. Server-side comparison

### 3.1 RPC surface and stream lifecycle

**YB.** A large RPC set on the tserver `CDCService` (`src/yb/cdc/cdc_service.proto`):
`GetChanges`, `GetCheckpoint`, `SetCDCCheckpoint`, `GetTabletListToPollForCDC`,
`UpdateCdcReplicatedIndex`, `GetLatestEntryOpId`, `IsBootstrapRequired`,
`BootstrapProducer`, `CheckReplicationDrain`, plus a Virtual-WAL set
(`InitVirtualWALForCDC`, `GetConsistentChanges`, `UpdateAndPersistLSN`,
`DestroyVirtualWALForCDC`, `GetLagMetrics`). Stream CRUD lives on the master
(`src/yb/master/master_replication.proto`: `CreateCDCStream`, `DeleteCDCStream`,
`ListCDCStreams`, `GetCDCStream`, `UpdateCDCStream`). Stream metadata is a
`SysCDCStreamEntryPB` in the master sys-catalog; per-tablet runtime state lives
in the replicated `cdc_state` table (`src/yb/cdc/cdc_state_table.cc`).

**Kudu.** Two tserver RPCs (`src/kudu/cdc/cdc.proto`): `GetChanges`,
`Checkpoint`. Stream CRUD on the master (`src/kudu/master/master.proto`):
`CreateCDCStream`, `DeleteCDCStream`, `ListCDCStreams`, `GetCDCStreamInfo`,
`UpdateCDCCheckpoint`, implemented in `src/kudu/master/catalog_manager.cc`
(~lines 7983-8139). Stream metadata is a `SysCDCStreamEntryPB` CowObject loaded
on leader election (`LoadCDCStreams`).

**Gap.** Kudu lacks a split-aware tablet-list RPC and a bootstrap/
IsBootstrapRequired RPC. Basic create/list/delete/checkpoint parity exists.
Applies: yes. (UPDATE 2026-08-03: stream expiry is now implemented via
`--cdc_stream_expiry_ms` and `--cdc_max_staleness_ms`, which release the
tablet's retention barriers on all replicas; snapshot mode covers bootstrap.)

### 3.2 Checkpoints, retention, and leader-change continuity

**YB.** A CDCSDK checkpoint is rich: `CDCSDKCheckpointPB{term, index, key,
write_id, snapshot_time}` (`src/yb/cdc/cdc_service.proto`), letting a consumer
resume mid-transaction-batch. Checkpoints are stored in the replicated
`cdc_state` table (per stream+tablet, plus a slot sentinel row), so **a newly
elected tablet leader recovers CDC position by reading `cdc_state`** rather than
relying on any in-memory state. A background scan (default 60s,
`update_min_cdc_indices_interval_secs`) computes the per-tablet minimum
checkpoint across all streams and fans it out to every peer via
`UpdateCdcReplicatedIndex`, which sets three barriers on the tablet peer: WAL
segment retention (`cdc_min_replicated_index`), intent retention
(`cdc_sdk_min_checkpoint_op_id` + expiration), and history/compaction cutoff
(`cdc_sdk_safe_time`). Writes to `cdc_state` are throttled
(`cdc_state_checkpoint_update_interval_ms`, 15s).

**Kudu.** Checkpoint is a single op-index. Two mechanisms
(`src/kudu/cdc/cdc_service.cc`): an in-memory `LogAnchor` per (stream, tablet)
registered on `GetChanges`/`Checkpoint` to hold WAL segments
(`src/kudu/consensus/log_anchor_registry.h`), and durable persistence to the
master sys-catalog `SysCDCStreamEntryPB.tablet_checkpoints` via
`PersistCheckpoint` -> master `UpdateCDCCheckpoint`.

**UPDATE (2026-08-03): resolved.** The master now recomputes per-tablet
retention barriers every `--cdc_bg_scan_interval_ms` (60s) and pushes them to
*every* replica via `SendCDCRetentionBarrierToAllReplicas` /
`UpdateCDCRetentionBarrier` (`catalog_manager.cc`, `cdc_service.cc:SetRetentionBarrier`),
plus a CDC history floor (`Tablet::SetCDCHistoryFloor`). A newly elected leader
therefore already holds both the WAL and history barrier; the consumer's own
anchor is re-registered on its next `GetChanges`. Residual gap: the per-tablet
consumer anchor is not released on stream *delete* (see `../gaps.md` A4).

**Original gap analysis (HIGH, now closed).** The WAL-retention anchor was
tserver-local and only re-established when the consumer next polled the new
leader, leaving a `WAL_EXPIRED` window on leadership change.

### 3.3 WAL read path and record model

**YB.** `GetChangesForCDCSDK` (`src/yb/cdc/cdcsdk_producer.cc`) reads the Raft
log bounded by a computed safe time (segment-by-segment when
`cdc_read_wal_segment_by_segment` is set). The record is a `RowMessage`
(`src/yb/cdc/cdc_service.proto`) with ops INSERT/UPDATE/DELETE/BEGIN/COMMIT/DDL/
TRUNCATE/READ/SAFEPOINT and `new_tuple`/`old_tuple` column lists.

**Kudu.** `ReadChanges` (`src/kudu/cdc/cdc_service.cc`) reads via
`LogReader::ReadReplicatesInRange` (`src/kudu/consensus/log_reader.h`), bounded
by the leader's `COMMITTED_OPID`, and decodes `WRITE_OP`/`ALTER_SCHEMA_OP`/
`PARTICIPANT_OP` into `CDCRecordPB` (`src/kudu/cdc/cdc_util.cc`). Ops:
INSERT/UPDATE/DELETE/UPSERT/DDL/BEGIN/COMMIT/ABORT/READ. CHANGE mode is
after-image only (UPDATE = changed columns, DELETE = key); FULL mode adds the
before-image (see 3.4). RecordType `CHANGE` and `FULL` are both implemented;
RecordFormat `PROTO` only (`JSON` reserved) (`src/kudu/cdc/cdc.proto`).

**Gap.** The read path is at parity. The record model gaps (before-image,
SAFEPOINT/safe-time, no separate READ/snapshot op used) are covered in 3.4-3.6.
Applies: yes.

### 3.4 Before-image / record types

**YB.** Configurable per stream via record type / PostgreSQL replica identity
(CHANGE / FULL / DEFAULT / NOTHING). When active, the before-image is
reconstructed by reading the row from DocDB at `commit_time.Decremented()` using
a point-key `DocRowwiseIterator` (`DoPopulateBeforeImage`,
`src/yb/cdc/cdcsdk_producer.cc`). Correctness depends on a history-retention
barrier (`cdc_sdk_safe_time`) preventing compaction of the needed MVCC version;
if the safe time has advanced past the read time the call errors rather than
returning wrong data.

**Kudu.** CHANGE mode: UPDATE carries only changed columns; DELETE carries only
the primary key (`PopulateUpdateDeleteColumns`, `src/kudu/cdc/cdc_util.cc`).
FULL mode: before/after images are reconstructed from the tablet's MVCC/UNDO
history at the op timestamp (`ReconstructBeforeAfterImages`, `cdc_util.cc`).

**UPDATE (2026-08-03): resolved.** `RecordType.FULL` is implemented. Server-side
row reconstruction reads the tablet's MVCC/delta stores at the op's timestamp,
protected by a history-retention barrier (`Tablet::SetCDCHistoryFloor` clamps
the ancient-history-mark, `tablet.h`). If the needed version has been compacted
away the call returns `HISTORY_EXPIRED` rather than wrong data
(`cdc_service.cc:920`).

**Original gap analysis (HIGH, now closed).** Kudu previously had no
before-image; sinks assuming a full before/after image were unsupported.

### 3.5 Transaction consistency

**YB.** Only committed data is emitted. On an `UPDATE_TRANSACTION_OP` with
status `APPLYING`, the producer reads the transaction's committed intents from
the IntentsDB (`GetIntentsForCDC`), skipping aborted subtransactions, and emits
BEGIN...DML...COMMIT (`src/yb/cdc/cdcsdk_producer.cc`). A per-tablet safe time
(`GetMinStartHTRunningTxnsForCDCProducer`, the min start-time of in-flight
transactions) bounds what may be emitted so an in-flight transaction is never
partially published. A SAFEPOINT record advertises progress.

**Kudu.** Transactional writes (carrying a `txn_id`) are now buffered per
transaction and emitted only when `FINALIZE_COMMIT` is seen, and dropped on
`ABORT_TXN` (`cdc_service.cc:856-1051`). Aborted/uncommitted rows are not
published. Records are still bounded by the committed op index.

**UPDATE (2026-08-03, refreshed 2026-08-24): option (a) implemented; A1 fixed.**
Per-tablet committed-only emission (buffer per `txn_id`, drop aborts) is done, and
the large-transaction wedge (A1) is fixed: the read window escalates to span the
whole transaction and fails loudly with `TRANSACTION_TOO_LARGE` past
`--cdc_max_transaction_span_bytes` rather than pinning the checkpoint forever.
Remaining:
- **No safe-time signal:** consumers cannot gate on a server-provided safe time.
- **Per-tablet framing only:** multi-tablet transactions need cross-tablet
  correlation by `txn_id` (see 3.7, gaps.md D1).

**Original gap analysis (HIGH).** The server did not buffer, filter aborts, or
emit a safe time, so a consumer could observe rows from an aborting transaction.

### 3.6 Consistent snapshot and streaming handoff

**YB.** The snapshot is server-driven at a single point in time. Stream
creation (`src/yb/master/xrepl_catalog_manager.cc`) sets retention/history
barriers on all tablets, waits for each tablet to record a Raft-committed
`SnapshotSafeOpId`, then takes one `consistent_snapshot_time` from
`Clock::MaxGlobalNow()`. `GetChanges` in snapshot mode reads each tablet at
exactly that time via a paginated DocDB iterator emitting READ records
(`HandleGetChangesForSnapshotRequest`, `src/yb/cdc/cdcsdk_producer.cc`), and
streaming then resumes from the `SnapshotSafeOpId` - no gap, no cross-tablet
skew.

**Kudu.** `ReadSnapshot` (`cdc_service.cc:1086`) is a server-driven snapshot: it
scans the tablet at a chosen HybridTime, emits paginated READ records
(`--cdc_snapshot_max_bytes_per_response`), and hands off to WAL streaming at the
corresponding op index, protected by the CDC history floor.

**UPDATE (2026-08-03, refreshed 2026-08-24): resolved.** The timestamp -> op-index
handoff and the history-retention barrier are implemented, and both prior
hardening items are fixed: the snapshot-start sequence is now race-safe on
concurrent first calls (A2), and the snapshot path honors the client deadline
instead of a hardcoded 30s (A3).

**Original gap analysis (HIGH, now closed).** Kudu had no server-side snapshot;
bootstrapping was a client-side scan that was at-least-once and not
point-consistent across tablets.

### 3.7 Virtual WAL / cross-tablet ordering

**YB.** `CDCSDKVirtualWAL` (`src/yb/cdc/cdcsdk_virtual_wal.cc`) merges per-tablet
change queues into a single, globally ordered, LSN-stamped stream using a
min-heap over `CDCSDKUniqueRecordID` (`src/yb/cdc/cdcsdk_unique_record_id.cc`):
sort by commit_time, then record type (BEGIN < DML < COMMIT), then write_id.
Publication is gated until every tablet has reported a SAFEPOINT at least up to
the candidate time (min-safe-time across tablets), guaranteeing no
lower-timestamped record can still arrive. A two-level checkpoint (scan-ahead
`from` vs consumer-acknowledged `explicit` via `UpdateAndPersistLSN`) drives WAL
release.

**Kudu.** No cross-tablet merge. Each tablet is consumed independently; there is
no global order across tablets and no safe-time gate.

**Gap (medium; partially applies).** Full multi-tablet transactional ordering is
only meaningful once 3.5 exists. For single-row-transaction workloads (the
common Kudu case) per-tablet ordering is sufficient and this is largely N/A. If
multi-tablet transactional consistency is a goal, a merger keyed on commit
HybridTime with a per-tablet safe-time gate is the design. This can live
client-side (as YB's could conceptually) or server-side.

### 3.8 Tablet split lineage

**YB.** `GetTabletListToPollForCDC` (`src/yb/cdc/cdc_service.cc`) returns the
tablets to poll and encodes split lineage: a parent is polled until drained,
then its children (whose `cdc_state` rows are created at split time) take over.
`split_parent_tablet_id` in tablet metadata tracks lineage.

**Kudu.** No CDC-aware tablet listing. The Debezium connector periodically
re-discovers tablets from generic tablet locations and streams any new tablet
from op-index 0. There is no parent-drain-before-child ordering, so a split can
briefly reorder or duplicate across the boundary.

**Gap (medium).** A CDC-aware tablet-list RPC (or extending stream metadata with
split lineage) would give correct ordered handoff. Applies: yes; Kudu supports
range splits/merges and partition additions.

### 3.9 Schema delivery and evolution

**YB.** A `need_schema_info` request flag forces a DDL record at the head of the
response; schema versions are looked up point-in-time from the sys-catalog at
the intent's HybridTime; a per-tablet schema cache tracks versions; mid-stream
`CHANGE_METADATA_OP` emits DDL records (`src/yb/cdc/cdcsdk_producer.cc`).

**Kudu.** DDL records are emitted for `ALTER_SCHEMA_OP` with the new schema and
version, and WRITE records now carry the correct `schema_version`
(`src/kudu/cdc/cdc_service.cc`, `src/kudu/cdc/cdc_util.cc`). There is no
on-demand "give me the schema for version N / at stream start" mechanism, so a
consumer starting mid-stream after an ALTER has no base schema until the next
DDL.

**UPDATE (2026-08-03): partially resolved.** `need_schema_info` is implemented
(`cdc_service.cc:732`) and prepends the current schema as a DDL record on a
fresh read. Remaining (minor): no schema-*by-version* lookup, so a consumer
starting mid-stream after an ALTER has no base schema until the next DDL record.

**Original gap (medium).** Kudu had no on-demand schema mechanism.

### 3.10 Security / authz

**YB.** Coarse plus object-level authorization on the CDC RPCs.

**Kudu.** The tserver CDC RPCs now require an authenticated client/service/super
user via `AuthorizeClientOrServiceUser` (`src/kudu/cdc/cdc.proto`,
`src/kudu/cdc/cdc_service.cc`); master CDC RPCs use `AuthorizeClient`.

**UPDATE (2026-08-03): resolved.** Fine-grained, per-table CDC authorization is
implemented behind `--cdc_enforce_access_control`: callers must present a signed
authz token granting SCAN on the target table, verified via
`AuthorizeCDCTableOrRespond` (`cdc_service.cc:245`), reusing the scan-token
machinery. When the flag is off, RPCs still require an authenticated
client/service/super user.

**Original gap (medium).** Only coarse authenticated-user authz existed.

### 3.11 Metrics / observability

**YB.** Rich lag and throughput metrics, including a per-stream flush-lag metric
served by `GetLagMetrics`.

**Kudu.** Counters on the tserver CDC service (GetChanges requests, records
produced, Checkpoint requests, errors), server-level aggregate gauges
(`cdc_max_sent_lag_micros`, `cdc_max_active_age_micros`, `cdc_active_streams`),
and per-(stream,tablet) gauges (`cdc_stream_sent_lag_micros`,
`cdc_stream_active_age_micros`) on a dedicated `cdc_stream` metric entity tagged
with stream_id/tablet_id (`src/kudu/cdc/cdc_service.cc`).

**UPDATE (2026-08-24): resolved.** Server-level and per-(stream,tablet) lag/age
gauges are implemented, so lag is attributable to a specific stream and tablet.
Residual (minor): no op-index-behind-committed lag gauge and no explicit
WAL-anchor-age gauge.

## 4. Client-side comparison (pure Kudu client)

**YB consumer surface.** `GetTabletListToPollForCDC` (discover tablets +
checkpoints, split-aware), `GetCheckpoint`/`SetCDCCheckpoint` (read/write durable
checkpoints), `GetChanges` (rich CDCSDK checkpoint), and the Virtual-WAL API
(`InitVirtualWALForCDC` / `GetConsistentChanges` / `UpdateAndPersistLSN`) for a
single consistent LSN stream with restart from `cdc_state`.

**Kudu client API** (`src/kudu/client` / Java `org.apache.kudu.client`):
`createCDCStream`, `deleteCDCStream`, `listCDCStreams`, `getCDCStreamInfo`
(returns per-tablet checkpoints from the master), `getChanges`, `cdcCheckpoint`.
Tablet discovery reuses generic table tablet locations.

**What a pure Kudu client is missing vs. YB:**
- A **CDC-aware tablet-list** call that returns tablets with their per-tablet
  checkpoints and split lineage in one shot (today: separate generic tablet
  locations + `getCDCStreamInfo`).
- A **read-checkpoint** call scoped to (stream, tablet); today checkpoints are
  read in bulk from the master via `getCDCStreamInfo`.
- A **consistent-changes / merged-stream** API (YB's Virtual WAL). N/A unless
  cross-tablet consistency (3.7) is pursued.
- **Bootstrap / IsBootstrapRequired** helpers.
- Richer checkpoint token (mid-batch resume). Kudu's op-index is coarser but
  sufficient given batches are whole-op.

Base create/list/delete/get/getChanges/checkpoint parity exists and is enough
for an at-least-once, per-tablet consumer.

## 5. What does NOT apply to Kudu

To avoid over-copying YB:
- **Colocated tables** - Kudu has no colocation; the per-table-within-tablet
  checkpoint keying is unnecessary.
- **YSQL/YCQL duality and PostgreSQL logical-replication compatibility** - the
  Virtual WAL's LSN/txn-id generation, replica identity, publication/slot model,
  and walsender protocol exist to serve PostgreSQL logical replication. Kudu has
  no such consumer contract to satisfy.
- **xCluster** - YB's asynchronous cross-cluster replication shares CDC plumbing
  but is a separate feature; not a Kudu goal here.
- **Transaction-status-tablet model** - YB's distributed transaction
  coordinator and IntentsDB differ from Kudu's participant-based transactions;
  the mechanism for "committed-only" must be built on Kudu's own transaction
  primitives, not copied.

## 6. Prioritized gap list

Updated 2026-08-03, refreshed 2026-08-24. The original list's items 1-4, 6
(expiry half), 7 (need_schema_info half), and 8 (authz half) were implemented by
the first refresh; the correctness/liveness (A1-A4) and hardening (B/C) items
below have since landed too (see `../gaps.md` for the FIXED audit). What remains
is the D feature set.

**Correctness / liveness (`../gaps.md` A1-A4) -- resolved except A4:**

1. **Large-transaction wedge** [done] - escalation loop grows the read window and
   fails loudly with `TRANSACTION_TOO_LARGE` past `--cdc_max_transaction_span_bytes`
   (3.5, A1).
2. **Consumer anchor leak on stream delete** [partial] - the durable-delete
   ordering root cause is fixed (E3), but releasing the per-(stream,tablet)
   tserver anchor on `DeleteCDCStream` is still open (3.2, A4).
3. **Snapshot-start race + hardcoded snapshot deadline** [done] - concurrent
   first-call race (A2) and client-deadline honoring (A3) fixed (3.6).

**Server-side hardening (`../gaps.md` B and C) -- resolved:**

4. **Streaming admission control** [done] - `--cdc_get_changes_free_rpc_ratio`
   reserves RPC threads for non-CDC traffic, `--cdc_read_safe_deadline_ratio`
   bounds partial returns, and heavy-scan caps (`--cdc_max_concurrent_scans`,
   `--cdc_scan_mem_limit_bytes`) shed excess with a retryable `SERVER_TOO_BUSY`.
5. **Edge-case error-code classification** [mostly done] - `STREAM_NOT_FOUND` and
   `TABLET_NOT_RUNNING` are set, the stream is validated on each read, and the
   stream-config cache is TTL-invalidated. Residual: `STREAM_EXPIRED` is not yet
   set server-side on the read path.
6. **Post-read leader-term recheck and `from_op_index` validation** [done].

**Larger features (still open; only if the goal is adopted):**

7. **Split-aware tablet-list RPC** [applies, medium] - ordered
   parent-then-child handoff (3.8, gaps.md D2).
8. **Schema-by-version lookup** [applies, minor] - beyond `need_schema_info`
   (3.9, gaps.md D3).
9. **Cross-tablet consistent ordering / Virtual WAL** [partial] - only if
   multi-tablet transactional consistency becomes a goal; depends on the
   transaction safe-time work (3.7, 3.5, gaps.md D1/D4).
