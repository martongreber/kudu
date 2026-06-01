# Kudu CDC Design Document

> Status: Living design + implementation-status doc.
> Original design: Draft v1.0 (2026-05-28). Last reconciled with the code:
> 2026-08-24.

This is the single "design and what is done" document. It states the original
design and, for each part, its current implementation status:

- **[DONE]** implemented as designed.
- **[PARTIAL]** partially implemented; the design is kept and the current state
  is noted.
- **[DONE, CHANGED]** implemented, but by a different mechanism than the original
  sketch; the current mechanism is described.
- **[PLANNED]** not started; forward-looking.

Where this doc and the code disagree, the code wins. Companion docs go deeper:
`gaps.md` (open items + resolved-bug audit), `dev_docs/CDC_IMPLEMENTATION_PLAN.md`
(phase-by-phase what shipped), `test_coverage_comparison.md` (test inventory),
`concept.md` (short overview).

---

## 1. Goals -- [DONE]

All Phase 1 goals are met:

- [DONE] Expose an ordered stream of committed row-level changes per tablet
- [DONE] Support INSERT, UPDATE, DELETE, UPSERT operation types
- [DONE] Emit schema change (DDL) events
- [DONE] Provide durable per-tablet checkpoints so consumers can resume after restart
- [DONE] Prevent WAL GC past slow consumers
- [DONE] Pull-based consumer model (no server-initiated push)
- [DONE] Minimal impact to the write path on tablet leaders (WAL-only source; no
  write-path hook)

---

## 2. Non-Goals (original Phase 1 scope)

These were declared out of scope for Phase 1. Two were subsequently built; the
rest remain out of scope.

- [DONE] Before-images for UPDATE operations -- now implemented via the FULL
  record type (see Section 6). Note: reconstructed from MVCC/UNDO history at read
  time, NOT via a storage read on the write path (see the next bullet).
- Before-image via storage read at apply time -- still not done, and deliberately
  so: FULL mode reconstructs from MVCC history instead, keeping the write path
  untouched.
- [DONE] Full transactional grouping -- now implemented per tablet: writes
  carrying a txn_id are buffered and emitted only on FINALIZE_COMMIT, dropped on
  ABORT_TXN (see Section 5). Cross-tablet grouping is still out of scope.
- [PLANNED] Multi-tablet virtual WAL / globally consistent ordering -- still out
  of scope (gaps.md D4).
- [PLANNED] NON_VOTER replica for CDC read offload -- still out of scope.
- [PLANNED] Kafka Connect / Debezium connector -- still out of scope. A C++
  client + consumer and a `kudu cdc` CLI ship instead; there is no Java connector
  in the tree.

---

## 3. Architecture -- [DONE]

Implemented as designed, with the retention path strengthened (master-driven,
all-replica; see Section 8) and FULL/snapshot paths added.

```
+-----------------------------------------------------------------------+
| Master                                                                |
| CatalogManager                                                        |
|   CreateCDCStream(table_ids, config) -> stream_id                     |
|   DeleteCDCStream(stream_id)                                          |
|   ListCDCStreams(table_id) -> [stream_info]                          |
|   GetCDCStreamInfo / UpdateCDCCheckpoint                             |
|   RunCDCStreamMaintenance: recompute + fan out retention barriers    |
|   SysCDCStreamEntryPB stored in sys catalog                          |
+-------------------------------+---------------------------------------+
                                | stream metadata + tablet locations
                                v
+-----------------------------------------------------------------------+
| Tablet Server (per node)                                              |
| CDCService                                                            |
|   GetChanges(stream_id, tablet_id, from_op_index) -> [record]        |
|   Checkpoint(stream_id, tablet_id, op_index)                         |
|                                                                       |
|   Internal:                                                           |
|   LogReader::ReadReplicatesInRange -- read WAL segments              |
|   LogAnchorRegistry -- per-(stream,tablet) consumer anchor + a       |
|     per-tablet retention anchor set on ALL replicas by the master    |
|   Tablet::SetCDCHistoryFloor -- hold MVCC/UNDO history (FULL mode)   |
|   RowOpsDecoder -- decode RowOperationsPB -> CDCColumnValuePB        |
|   ReadSnapshot -- consistent point-in-time scan, then WAL hand-off   |
+-------------------------------+---------------------------------------+
                                | CDCRecordPB batch
                                v
+-----------------------------------------------------------------------+
| External Consumer  (C++ CDCClient/CDCConsumer, or `kudu cdc` CLI)     |
|   Discover tablets: GetTableLocations (existing master RPC)          |
|   Poll: GetChanges per tablet leader                                 |
|   Process: decode CDCRecordPB, publish downstream                    |
|   Commit: Checkpoint(op_index) to advance the anchor                 |
+-----------------------------------------------------------------------+
```

---

## 4. Protobuf API (`src/kudu/cdc/cdc.proto`) -- [DONE, EXTENDED]

The original design proto was implemented and then extended. Below is the current
shape. See `cdc.proto` for exact field numbers and defaults.

### Sys catalog entry

```proto
message SysCDCStreamEntryPB {
  enum State { ACTIVE = 0; DELETING = 1; }
  repeated bytes table_ids = 1;
  optional CDCStreamConfigPB config = 2;
  optional State state = 3 [default = ACTIVE];
  // Added since the original design:
  map<string, int64>  tablet_checkpoints    = ...; // tablet_id -> op_index
  map<string, fixed64> tablet_history_floors = ...; // FULL-mode history floor
  optional fixed64     last_active_time_micros = ...; // for idle-stream expiry
}

message CDCStreamConfigPB {
  enum RecordType   { CHANGE = 0; FULL = 1; }        // FULL = before+after images
  enum RecordFormat { PROTO = 0; JSON = 1; }         // JSON reserved, NOT implemented
  enum SnapshotMode { INITIAL_AND_CONTINUE = 0; NEVER = 1; INITIAL_ONLY = 2; }
  optional RecordType   record_type   = 1 [default = CHANGE];
  optional RecordFormat record_format = 2 [default = PROTO];
  optional int64 max_bytes_per_response = 3 [default = 67108864]; // see note below
  optional SnapshotMode snapshot_mode = ... [default = NEVER];
}
```

Original design vs. now: `RecordType.FULL`, the whole `SnapshotMode` enum, and the
`tablet_checkpoints` / `tablet_history_floors` / `last_active_time_micros` maps
were added. `RecordFormat.JSON` is reserved but not implemented (PROTO only) --
this is gap D5.

### Change record

```proto
enum CDCOpTypePB {
  INSERT = 0; UPDATE = 1; DELETE = 2; UPSERT = 3;
  DDL = 4; BEGIN = 5; COMMIT = 6;
  ABORT = 7;   // added: aborted-txn marker
  READ  = 8;   // added: snapshot row
}

message CDCColumnValuePB {
  optional int32  column_id   = 1;
  optional string column_name = 2;
  optional bytes  value       = 3;   // serialized cell value (type in schema)
  optional bool   is_null     = 4 [default = false];
}

message CDCRecordPB {
  optional CDCOpTypePB op_type        = 1;
  optional int64       op_index       = 2; // log index (monotone within tablet)
  optional int32       op_term        = 3; // Raft term
  optional fixed64     timestamp      = 4; // hybrid logical clock micros
  optional uint32      schema_version = 5; // version in effect for this op (pre-op)
  optional bytes       txn_id         = 6; // set for transactional writes
  repeated CDCColumnValuePB changes   = 7; // after-image columns
  // Added since the original design:
  optional SchemaPB    new_schema         = 8;  // DDL-only
  optional uint32      new_schema_version = ...; // DDL-only: version after the ALTER
  optional fixed64     commit_timestamp   = ...; // transactional writes
  repeated CDCColumnValuePB old_changes   = ...; // before-image (FULL mode)
}
```

### RPC messages

```proto
message GetChangesRequestPB {
  required bytes stream_id     = 1;
  required bytes tablet_id     = 2;
  optional int64 from_op_index = 3 [default = 0]; // exclusive lower bound
  optional int64 max_bytes     = 4;               // response size cap
  // Added since the original design:
  optional bool  is_snapshot_start   = 5 [default = false];
  optional bytes snapshot_resume_key = 6;         // opaque, from prior response
  optional bool  need_schema_info    = 7 [default = false]; // prepend a DDL record
  optional SignedTokenPB authz_token = 8;         // fine-grained authz
}

message GetChangesResponsePB {
  optional CDCErrorPB  error              = 1;
  repeated CDCRecordPB records            = 2;
  optional int64 checkpoint_op_index      = 3 [default = -1]; // next from_op_index
  // Added since the original design:
  optional bytes tablet_id                            = 4;
  optional bool  snapshot_done                        = 5 [default = false];
  optional int64 snapshot_streaming_start_op_index    = 6; // WAL hand-off point
  optional bytes snapshot_resume_key                  = 7;
  optional bool  have_more_records                    = 8 [default = false];
}

message CheckpointRequestPB {
  required bytes stream_id = 1;
  required bytes tablet_id = 2;
  required int64 op_index  = 3;
  optional SignedTokenPB authz_token = 4; // added
}

message CheckpointResponsePB {
  optional CDCErrorPB error = 1;
}
```

### Error codes

Original design implied two error states. The shipped `CDCErrorPB` defines 12:
`UNKNOWN_ERROR`, `STREAM_NOT_FOUND`, `TABLET_NOT_FOUND`, `TABLET_NOT_LEADER`,
`WAL_EXPIRED`, `TABLET_NOT_RUNNING`, `HISTORY_EXPIRED`, `STREAM_EXPIRED`,
`NOT_AUTHORIZED`, `SERVER_TOO_BUSY`, `SNAPSHOT_SESSION_LOST`,
`TRANSACTION_TOO_LARGE`.

### RPC service

```proto
service CDCService {
  rpc GetChanges(GetChangesRequestPB) returns (GetChangesResponsePB);
  rpc Checkpoint(CheckpointRequestPB) returns (CheckpointResponsePB);
}
```

Default authorization is `AuthorizeClientOrServiceUser`; fine-grained per-table
authz is opt-in via `--cdc_enforce_access_control` (Section 7 / gaps.md).

---

## 5. CDCService Implementation -- [DONE]

### Key responsibilities (all implemented)

1. [DONE] **Validate** stream_id exists (fetched + cached from the master),
   tablet is locally hosted and leader, and `from_op_index >= 0`.
2. [DONE] **Read WAL** from `from_op_index + 1` via
   `LogReader::ReadReplicatesInRange`, bounded by the committed op-index.
3. [DONE] **Filter** op types: `WRITE_OP`, `ALTER_SCHEMA_OP`, `PARTICIPANT_OP`;
   others produce no records.
4. [DONE] **Decode** `RowOperationsPB` -> `CDCRecordPB` using the schema at the
   op's schema_version.
5. [DONE] **Enforce** response size and set `have_more_records` when cut short of
   the committed head. NOTE: the server reads `req.max_bytes()` (relayed by the
   consumer) or falls back to the `--cdc_max_bytes_per_response` flag (default
   8MiB); it does NOT read the `CDCStreamConfigPB.max_bytes_per_response` field
   (default above) back from the stream config -- that field is written at stream
   create and relayed to the server by the consumer as `req.max_bytes()`
   (`cdc_service.cc:695`; `cdc_consumer.cc:299`). So the per-stream cap flows
   through the consumer, not through server-side stream-config enforcement.
6. [DONE] **Update** the WAL anchor on every call to prevent GC.

Additions beyond the original list (all implemented): committed-only transaction
buffering with a large-transaction escalation guard (`TRANSACTION_TOO_LARGE`);
FULL-mode before/after reconstruction; server-driven snapshot; a post-read
leader-term recheck; `TABLET_NOT_RUNNING` classification; and admission control
(`--cdc_get_changes_free_rpc_ratio`, `--cdc_max_concurrent_scans`,
`--cdc_scan_mem_limit_bytes`, `--cdc_read_safe_deadline_ratio`).

### WAL retention / anchor management -- [DONE, CHANGED]

The original sketch registered one leader-local `LogAnchor` per
`(stream_id, tablet_id)`, updated on each GetChanges/Checkpoint and released on
stream deletion. That is still the per-consumer anchor, but retention is now
primarily **master-driven and set on every replica** so it survives leader change
(see Section 8). The tserver keeps:

- a per-(stream, tablet) consumer anchor (leader only), advanced by Checkpoint; and
- a per-tablet retention anchor + MVCC history floor set on ALL replicas by the
  master-pushed `UpdateCDCRetentionBarrier` RPC.

Known residual: on stream delete the master releases the all-replica barrier, but
the tserver-side per-(stream,tablet) consumer anchor is only released on tablet
deletion (gaps.md A4).

### Checkpoint persistence -- [DONE]

On `Checkpoint`, the tserver advances the in-memory anchor and responds
immediately, then asynchronously persists to the master via `UpdateCDCCheckpoint`
(monotonic store). On reconnection a consumer reads the last persisted checkpoint
per tablet from the master and resumes.

### Leader-only serving -- [DONE]

`GetChanges` returns `TABLET_NOT_LEADER` if the local replica is not the leader
(and rechecks leadership/term after the read); the consumer rediscovers the leader
and retries.

---

## 6. Row Decoding -- [DONE]

### Approach -- [DONE]

Reuses the `RowOperationsPB` decoding approach (as in the `wal dump` tooling). The
`cdc_util` decoder iterates entries, reads op type / isset / null bitmaps, and
populates `CDCColumnValuePB` per column.

### Schema versioning -- [DONE]

The decoder maintains a running schema version, rolled forward as
`ALTER_SCHEMA_OP` entries are seen; DDL records carry both the pre-op
`schema_version` and the post-op `new_schema_version`. (Reconstructing the schema
for an arbitrary historical version on demand is still open -- gaps.md D3.)

### Operation type mapping -- [DONE]

| `RowOperationsPB::Type`   | `CDCOpTypePB` |
|---------------------------|---------------|
| `INSERT`, `INSERT_IGNORE` | `INSERT`      |
| `UPDATE`, `UPDATE_IGNORE` | `UPDATE`      |
| `DELETE`, `DELETE_IGNORE` | `DELETE`      |
| `UPSERT`, `UPSERT_IGNORE` | `UPSERT`      |

### Before/after images (FULL mode) -- [DONE]

For `RecordType.FULL`, `ReconstructBeforeAfterImages` opens ordered MVCC scans at
the op timestamp (before-image) and just after it (after-image) over the touched
key range, populating `old_changes` and `changes`. UPSERT is reclassified to
INSERT/UPDATE by whether a before-image row exists. If the required history has
been GC'd, the batch returns `HISTORY_EXPIRED` rather than emitting a wrong image.

---

## 7. Master Integration -- [DONE]

Fully implemented in `CatalogManager` (an early scaffolding step returned
NotSupported; that is no longer the case).

### Sys catalog entry -- [DONE]

`SysCDCStreamEntryPB` is persisted in the sys catalog (one entry per stream,
keyed by UUID) and reloaded into memory on leader election (`LoadCDCStreams`). It
holds table IDs, config, state, per-tablet checkpoints, per-tablet history floors,
and last-active time.

### Master RPCs -- [DONE]

`CreateCDCStream`, `DeleteCDCStream`, `ListCDCStreams`, `GetCDCStreamInfo`, and
`UpdateCDCCheckpoint` are implemented on the master. A tserver admin RPC
`UpdateCDCRetentionBarrier` (in `tserver_admin.proto`) carries the per-tablet
retention op-index and history-safe-time to every replica. See `master.proto` and
`tserver_admin.proto` for the message shapes.

### Fine-grained authz -- [DONE]

Opt-in via `--cdc_enforce_access_control`: `GetChanges`/`Checkpoint` must carry a
signed authz token granting SCAN on the target table (reusing the scan-token
machinery).

---

## 8. Log Retention -- [DONE, CHANGED] + [PARTIAL]

The original design used a leader-local anchor and flagged unbounded WAL growth
for a stalled consumer as a post-POC risk, with a safety valve "to implement
before production." Both were addressed:

### Master-driven, all-replica retention -- [DONE, CHANGED]

Instead of a leader-only anchor, the master recomputes per-tablet retention
barriers every `--cdc_bg_scan_interval_ms` (default 60s) in
`RunCDCStreamMaintenance` and fans them out to every replica via
`UpdateCDCRetentionBarrier` (last-writer-wins by sequence number, with a
per-run release fan-out cap `--cdc_max_barrier_releases_per_run`). A newly elected
leader therefore already holds the WAL (and, for FULL streams, the MVCC history
floor). This closes the old "anchor is leader-local" hole.

### Safety valve -- [DONE]

- [DONE] Idle-stream expiry: `--cdc_stream_expiry_ms` (default 8h) excludes idle
  streams from the retention minimum and releases their barriers on all replicas.
- [DONE] Non-advancing-checkpoint (staleness) expiry: `--cdc_max_staleness_ms`
  (default 4h).
- [DONE] Consumers that fall behind WAL/history GC get an in-band `WAL_EXPIRED`
  (or `HISTORY_EXPIRED` for FULL) error rather than silent data loss.

### Still open -- [PARTIAL]

- [DONE] A hard time-based WAL floor IS implemented: `--cdc_wal_retention_secs`
  (default 8h) is enforced directly in log GC for CDC-enabled tablets
  (`tablet_replica.cc:86,779`; `log.cc:1005`), independent of any consumer poll
  or master maintenance pass -- the YugabyteDB `cdc_wal_retention_time_secs`
  analog. (This entry previously read "[PLANNED] ... not implemented"; that was
  stale -- the floor was added 2026-08-24.)
- [PLANNED] A hard *byte/op-index* growth ceiling is still not implemented;
  beyond the time floor, retention is bounded by idle/staleness expiry, not by a
  byte or segment cap.

Observability is [DONE]: server-level aggregate gauges (`cdc_max_sent_lag_micros`,
`cdc_max_active_age_micros`, `cdc_active_streams`) plus per-(stream,tablet) gauges
(`cdc_stream_sent_lag_micros`, `cdc_stream_active_age_micros`) on a dedicated
`cdc_stream` metric entity tagged with stream_id/tablet_id, so a lagging stream can
be pinpointed.

---

## 9. Consumer Protocol -- [DONE]

Implemented as designed, plus a shipped C++ client/consumer and CLI.

### Bootstrap sequence -- [DONE]

```
1. CreateCDCStream(table_ids=[...], config={...}) -> stream_id
2. GetTableLocations(table_id) -> tablet_locations (existing RPC)
3. For each tablet:
   a. Find leader from tablet_locations
   b. (optional) snapshot: GetChanges(is_snapshot_start=true) paginated to
      snapshot_done, then hand off at snapshot_streaming_start_op_index
   c. GetChanges(stream_id, tablet_id, from_op_index) -> batch
   d. Process records, publish downstream
   e. Checkpoint(stream_id, tablet_id, op_index=last_processed)
   f. Loop from (c) using checkpoint_op_index
```

### Failure recovery -- [DONE]

On consumer crash: read the last persisted checkpoint per tablet from the master
and resume. On `WAL_EXPIRED`/`HISTORY_EXPIRED`/`STREAM_EXPIRED`, re-snapshot.

### Leader failover -- [DONE]

On `TABLET_NOT_LEADER`, rediscover the leader via `GetTableLocations` and retry
from the same checkpoint.

### Client / consumer / CLI -- [DONE]

- `cdc_client.*` -- a self-contained C++ client (stream CRUD, table metadata,
  tablet topology, GetChanges/Checkpoint, master failover).
- `cdc_consumer.*` -- a poller fleet (one `CDCTabletPoller` per tablet) that does
  leader-following, optional snapshot phase, polling, decode, and checkpointing.
  Note: on a terminal error (resnapshot/fatal) a poller stops and surfaces
  `needs_resnapshot`; there is no automatic re-snapshot/restart -- the caller
  must intervene.
- `tools/tool_action_cdc.cc` -- the `kudu cdc` CLI (create/delete/list streams,
  tail changes).

---

## 10. Source Layout -- [DONE]

(Original "files to create/modify" table, updated to the files that actually
landed.)

```
src/kudu/cdc/
  cdc.proto              CDCService + record definitions
  cdc_service.h/cc       GetChanges, Checkpoint, snapshot, retention barrier
  cdc_util.h/cc          WAL -> CDCRecordPB decoder (+ FULL before/after images)
  cdc_client.h/cc        C++ consumer-side client
  cdc_consumer.h/cc      Poller fleet
  cdc_util-test.cc / cdc_service-test.cc / cdc_client-test.cc
  CMakeLists.txt

src/kudu/master/
  master.proto           SysCDCStreamEntryPB + master CDC RPCs
  sys_catalog.h/cc       CDC stream persistence
  catalog_manager.h/cc   CRUD + RunCDCStreamMaintenance retention fan-out
  master_service.cc      RPC handlers
  cdc_manager-test.cc

src/kudu/tserver/
  tserver_admin.proto    UpdateCDCRetentionBarrier
  tablet_service.cc      UpdateCDCRetentionBarrier handler
  tablet_server.cc       registers CDCService

src/kudu/tablet/
  tablet.h/cc            SetCDCHistoryFloor + history-GC water mark (FULL mode)

src/kudu/tools/
  tool_action_cdc.cc     `kudu cdc` CLI

src/kudu/integration-tests/
  cdc-itest.cc / cdc_failover-itest.cc / cdc_client-itest.cc
```

Note: `java/kudu-cdc-connector/` on disk contains only stale build output -- there
is no tracked Java source and it is not wired into the Gradle build.

---

## 11. Open Questions -- resolutions

1. **Before-images** -- [DONE] via FULL mode, reconstructed from MVCC/UNDO history
   at read time (not the `OpDriver::Finalize` write-path option floated originally).
2. **Checkpoint store** -- [PARTIAL] still the master sys catalog. A dedicated
   `cdc_state`-style tablet remains a future scale option (checkpoint keys were
   designed so this migration is clean).
3. **NON_VOTER CDC replicas** -- [PLANNED] not started.
4. **Transaction grouping** -- [DONE] per tablet (buffer by txn_id, emit on
   commit, drop on abort). Cross-tablet ordering / safe-time still open (gaps.md
   D1).
5. **Consumer protocol** -- internal KRPC only, as designed. A public REST/gRPC
   gateway is still [PLANNED].
6. **Snapshot bootstrap** -- [DONE] server-driven consistent snapshot, then WAL
   hand-off.

Remaining feature gaps are tracked in `gaps.md` section D: cross-tablet
transaction consistency (D1), tablet range-partition split lineage (D2),
schema-by-version (D3), cross-tablet consistent ordering / Virtual WAL (D4), and a
self-describing wire format such as JSON (D5).

---

## 12. Future Direction: Kudu-to-Kudu Native Replication -- [PLANNED]

Not started. This section is forward-looking design only.

The CDC infrastructure is designed to extend into active-passive cross-cluster
replication (similar to YugabyteDB's xCluster mode). The source cluster is
authoritative; the target cluster is a read-only replica that tails the CDC
stream.

### Architecture

```
+------------------------------+       +------------------------------+
| Source Cluster               |       | Target Cluster (passive)     |
|                              |       |                              |
|  Master (stream lifecycle)   |       |  Master (replication config) |
|  TServers (CDCService)       |<------|  TServers (ReplicationPoller) |
|                              | poll  |                              |
|  Tables: read/write          |       |  Tables: read-only replicas  |
+------------------------------+       +------------------------------+
```

The target cluster embeds a **ReplicationPoller** service in each tserver that:
1. Polls `GetChanges` on the source cluster's tablet leaders
2. Applies writes locally via the normal write path (using `UPSERT` for idempotency)
3. Manages its own checkpoints (persisted on the target master)
4. Handles source leader failover by re-discovering via `GetTableLocations`

### Design implications for the current CDC API

**WAL record format mode.** For replication, decoded `CDCRecordPB` is unnecessary
overhead -- the target just replays the write. A `WAL` format option on
`CDCStreamConfigPB.RecordFormat` could ship raw `WriteRequestPB` bytes without row
decoding, avoiding the decode-on-source / re-encode-on-target roundtrip.

**Idempotent replay on target.** After a crash the target poller resumes from its
last checkpoint and may re-apply operations. Replicated INSERTs applied as UPSERT
(or INSERT_IGNORE) and DELETEs as DELETE_IGNORE make replay safe without
exactly-once delivery.

**Tablet mapping.** Simplest when both clusters use identical partition schemas
(1:1 tablet mapping by partition-key range). Otherwise a partition-key routing
layer re-partitions incoming rows. Start by requiring identical schemas. The
target master stores source master addresses, source-stream-to-target-table
mapping, and per-tablet checkpoint state.

**DDL / schema propagation.** (1) Manual: pause replication, apply the same ALTER
on both clusters, resume; the poller pauses on a schema-version mismatch. (2)
Automatic: the poller observes DDL records and applies the ALTER on the target
before replaying subsequent writes.

**Snapshot bootstrap for initial sync.** A new relationship needs an initial full
copy before WAL tailing. Options: existing backup/restore tooling, or a CDC
snapshot mode that scans full tablet contents as synthetic records, then
transitions to WAL tailing. (The consistent-snapshot mechanism in Section 5 is the
building block.)

**What does NOT change (current design is already compatible):** pull-based
`GetChanges` per tablet; anchor/history-floor WAL retention; master stream
lifecycle; checkpoint semantics; leader-only serving with failover retry.

### Scope boundary

Active-active (bidirectional) replication is explicitly out of scope: it would
require loop detection and conflict resolution with limited initial use cases.
Active-passive covers disaster recovery, read scaling, and geographic read
replicas.
