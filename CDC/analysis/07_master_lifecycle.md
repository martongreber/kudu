# 07 Master CDC Stream Lifecycle: YB vs Kudu Gap Analysis

## 1. Summary

Five production-shaping gaps identified in Kudu's CDC stream lifecycle vs YB. One is critical:
the DELETE-side barrier release is not crash-safe because the retention barrier is persisted
to the tablet superblock (disk) but the RELEASE RPC that clears it is fire-and-forget after an
in-memory erase. A master failover in that window leaves tablets retaining WAL and MVCC history
permanently, with no recovery path short of manual intervention or a direct new RELEASE RPC.

The other gaps are: missing table validation on CreateCDCStream, no auto-delete of streams when
a referenced table is dropped, orphaned checkpoint rows from partial DeleteCDCStream failures,
and a theoretical but non-zero barrier_seq collision between a concurrent maintenance SET and a
delete RELEASE.

YB's two-phase DELETING state (persist DELETING to sys catalog -> background task releases
cdc_state rows -> tablets self-heal) avoids all of these because: (a) the DELETING marker
survives failover, (b) any leader can finish cleanup, and (c) tablet retention is driven by
cdc_state table presence rather than explicit RPC.

---

## 2. Findings Table

| # | Gap | Sev | YB anchor | Kudu status / anchor | Why prod-shaping | Kudu sketch | New/Dup |
|---|-----|-----|-----------|----------------------|-----------------|-------------|---------|
| G1 | DeleteCDCStream barrier leak on master failover | CRITICAL | xrepl_catalog_manager.cc:2151-2153 (mark DELETING), 3794-3870 (background GC) | UNSAFE -- catalog_manager.cc:8491-8544, cdc_service.cc:2255-2260 (Flush to superblock) | Barrier persisted to tablet superblock; if master crashes after sys_catalog removal but before RELEASE RPC, new master has no knowledge of the release. Barrier never cleared. | Two-phase delete: mark stream DELETING in sys_catalog first; background RunCDCStreamMaintenance pass detects DELETING streams and fans out RELEASE RPCs, then removes from sys catalog and maps | NEW |
| G2 | CreateCDCStream no table-existence / state check | HIGH | xcluster_source_manager.cc:571-576 (visible_to_client + is_deleting check) | MISSING -- catalog_manager.cc:8408-8443 only checks `table_ids_size() == 0` | Streams can be created on deleted, being-deleted, or non-existent tables, producing inert garbage entries in sys catalog and ListCDCStreams output | Lock table under shared lock_, check `!is_deleted()` and `!is_soft_deleted()` before writing stream to sys catalog; return NotFound otherwise | NEW |
| G3 | Table drop while stream ACTIVE -- no auto-delete | MEDIUM | catalog_manager.cc:10472-10487 (`DropCDCSDKStreams` or `HandleDroppedTablesForCDCSDKStreams` called from DeleteTable) | MISSING -- catalog_manager.cc:2990-3089 has no CDC interaction | Streams for dropped tables stay ACTIVE in sys catalog indefinitely; ListCDCStreams returns stale entries; barrier SETs are no longer sent (tablets are gone) but the stream entry leaks catalog space and confuses operators | In DeleteTable, scan cdc_stream_map_ under lock_ for streams referencing the table; mark them DELETING (or call DeleteCDCStream) after table tablets are scheduled for deletion | NEW |
| G4 | Orphaned checkpoint rows from partial DeleteCDCStream + failover | MEDIUM | xrepl_catalog_manager.cc:3794-3870 (background always re-scans and re-attempts, DELETING state survives failover) | MISSING -- catalog_manager.cc:8500-8511 (stream row gone from sys_catalog before checkpoint rows fully removed; on failover LoadCDCTabletCheckpoints reloads orphans with no owning stream, never GC'd) | Orphaned checkpoint rows accumulate in cdc_tablet_checkpoint_map_ and sys catalog; do not pin barriers but waste memory and disk indefinitely | Add a GC pass in RunCDCStreamMaintenance or LoadCDCStreams: for any stream_id present in cdc_tablet_checkpoint_map_ but absent from cdc_stream_map_, delete its rows from sys catalog and erase from in-memory map | NEW |
| G5 | Concurrent maintenance SET and delete RELEASE barrier_seq collision | LOW | YB avoids via cdc_state-based self-release (no explicit RPC race) | THEORETICAL -- cdc_service.cc:2182-2190 (barrier_last_seq_ gate); catalog_manager.cc:8534, 8929 (both use GetCurrentTimeMicros, epsilon normally ms-range but zero under CPU saturation) | If `now_micros == release_seq` (same microsecond) and RELEASE arrives at tablet before SET, SET re-pins the barrier; last-writer-wins gate only excludes strictly-less-than seq | `release_seq = now_micros + 1` in DeleteCDCStream (or use a monotonic counter) to guarantee RELEASE always wins; alternatively implement two-phase delete (G1 fix) which eliminates the race entirely | NEW |
| DUP-P2-2 | Fleet-level --cdc_max_streams cap missing | P2 | xrepl_catalog_manager.cc:1289-1292 (max_replication_slots check) | NOT IMPLEMENTED | See backlog | Add flag + check in CreateCDCStream | DUPLICATE |
| DUP-P3-3a | DELETING stream state proto exists but never persisted | P3 | N/A | master.proto:1277 defines DELETING; GetCDCStreamInfo:8619-8623 references it forward | Addressed by G1 fix | Implement as part of G1 | DUPLICATE (context for G1) |

---

## 3. Dedicated Walkthroughs

### 3a. STREAM DELETE Race -- UNSAFE

**Kudu sequence (catalog_manager.cc:8445-8545)**:

```
DeleteCDCStream:
  [1] lock_guard: find stream, collect tablet IDs                  (8460-8488)
  [2] sys_catalog_->RemoveCDCStream(stream_id)                     (8500)  <- durable removal
  [3] for each tablet: RemoveCDCTabletCheckpoint(...)              (8506-8511)
  [4] lock_guard: cdc_stream_map_.erase, cdc_tablet_checkpoint_map_.erase  (8513-8523)
  [5] for each tablet: SendCDCRetentionBarrierToAllReplicas(-1, seq=now)   (8534-8543)
```

Barrier persistence chain:
- In step 5, `SendCDCRetentionBarrierToAllReplicas` dispatches `AsyncUpdateCDCRetentionBarrier`
  fire-and-forget async tasks to every peer of every tablet.
- Each task calls `CDCServiceImpl::SetRetentionBarrier` on the tserver, which, when accepted:
  (a) updates in-memory `retention_anchors_` and `barrier_last_seq_`
  (b) calls `meta->SetCDCRetentionBarrier(-1, 0)` and `meta->Flush()` --
      **persisting `cdc_min_retained_op_index = -1` to the tablet superblock on disk**
      (cdc_service.cc:2255-2260, tablet_metadata.cc:1076-1082, 796-797)

**Crash window**: Master crashes after step 2 (sys_catalog write durable) but before
step 5 RPCs complete (or even before step 5 starts). The new master:

- `LoadCDCStreams` at line 8383: stream is gone from sys catalog. Not loaded.
- `LoadCDCTabletCheckpoints` at line 8351: if step 3 also completed, checkpoint rows
  are gone. If step 3 did NOT complete, checkpoint rows are in `cdc_tablet_checkpoint_map_`
  but with no owning stream in `cdc_stream_map_` (orphaned rows, see G4).
- `cdc_barriered_tablets_` at line 1581 is a plain `unordered_set` with no persistence.
  New master initializes it empty.
- First `RunCDCStreamMaintenance` pass:
  - `tablet_min_index` is computed from active streams; deleted stream's tablets absent.
  - Step 3 (SET loop, line 8921): sends no RPC for those tablets (not in `tablet_min_index`).
  - Step 4 (release loop, line 8948): iterates `cdc_barriered_tablets_` (empty); sends
    no RELEASE RPCs.
  - Step 5 (rebuild, line 8975): `cdc_barriered_tablets_` stays empty.

**Tablet state after the crash**: The tablet's superblock still has
`cdc_min_retained_op_index = <last_set_value>` (set by the last maintenance-pass SET before
the delete). `tablet_replica.cc:909` uses this value directly for WAL GC decisions. No master
ever sends a new RELEASE RPC. WAL and MVCC history are retained forever.

**After tserver restart**: `TabletMetadata::Load` at tablet_metadata.cc:407-409 reloads
`cdc_min_retained_op_index_` from the superblock. `CDCServiceImpl::barrier_last_seq_` starts
empty (in-memory only). The new master still sends no RPC for this tablet. The barrier remains.

**Verdict: UNSAFE -- permanent WAL/history retention leak on any master failover during DeleteCDCStream.**

**YB fix** (xrepl_catalog_manager.cc): `DeleteCDCStream` -> `DropXReplStreams` persists
`DELETING` state to sys catalog and returns. Any master leader running `RunXReplBgTasks` ->
`CleanUpDeletedXReplStreams` finds DELETING streams, deletes their cdc_state rows, then removes
the stream row from sys catalog. Tablets observe the absence of their cdc_state entry and
release intent/WAL retention. No explicit barrier RPC; no RPC window. DELETING state in sys
catalog survives any failover.

**Kudu fix sketch**:
1. In `DeleteCDCStream`, before step 2: write `state = DELETING` to sys catalog (persist marker).
2. Return success to caller (stream is logically deleted, won't be served by GetCDCStreamInfo).
3. In `RunCDCStreamMaintenance` (or a dedicated pass), find streams with state DELETING:
   fan out RELEASE RPCs for all their tablets, remove checkpoint rows, then remove stream row.
4. On recovery, `LoadCDCStreams` sees DELETING streams; they are excluded from barrier computation
   but queued for cleanup. The first maintenance pass after leader election completes them.

---

### 3b. TABLE DROP Under Active Stream -- NEEDS-GUARD

**YB sequence** (catalog_manager.cc:10472-10487 in `DeleteTabletList`):

```cpp
TRACE("Deleting CDC streams on table");
// ...
if (FLAGS_cdcsdk_use_dropped_table_list_for_cleanup) {
    RETURN_NOT_OK(HandleDroppedTablesForCDCSDKStreams(table_ids));
} else {
    RETURN_NOT_OK(DropCDCSDKStreams(table_ids));
}
```

`DropCDCSDKStreams` marks streams as `DELETING_METADATA` (for multi-table streams: remove the
dropped table from stream metadata) or `DELETING` (for single-table streams). Background task
completes cleanup. xCluster streams for the table are also marked DELETING via
`GetXReplStreamsForTable` + `DropXReplStreams` at xrepl_catalog_manager.cc:722-735.

**Kudu sequence** (catalog_manager.cc:2990-3089 `DeleteTable`):

DeleteTable marks table + tablets DELETED in sys catalog, removes from name map, aborts tasks,
sends DeleteTablet RPCs, purges table locations cache. **Zero CDC interaction.** No scan of
`cdc_stream_map_`, no stream state change, no DeleteCDCStream call.

**Result**:

After the table deletion completes:
- The stream entry in sys catalog still has `state = ACTIVE` and `table_ids = [deleted_table_id]`.
- Tablets of the deleted table are DELETED in sys catalog.
- Next `RunCDCStreamMaintenance` pass: `gone_tablets` detects all of the stream's tablets as
  deleted (`l.data().is_deleted()`, line 8875). Step 2 (line 8896) prunes their checkpoint rows.
  Step 4 sends RELEASE RPCs for them (they were in `cdc_barriered_tablets_` from the prior pass).
  After one maintenance interval: **barriers ARE released, no WAL leak**.
- The stream entry stays in sys catalog as ACTIVE indefinitely. `ListCDCStreams` returns it.
  `GetCDCStreamInfo` returns it with `state = ACTIVE` even though the table is gone.
  Users see ghost streams. Accumulates over time in busy environments.
- `UpdateCDCCheckpoint` for the stream returns OK (stream found in map) but checkpoints go
  nowhere useful -- consumer is effectively broken.

**Verdict: NEEDS-GUARD.** WAL barriers are self-healed by the maintenance loop within one
maintenance interval. However, dangling ACTIVE stream entries in sys catalog constitute a catalog
correctness gap: ListCDCStreams lies, GetCDCStreamInfo lies, and operators/tooling cannot
distinguish live from dead streams without cross-referencing table existence.

**Kudu fix sketch**: In `DeleteTable` (after or before `SendDeleteTableRequest`), under a
shared `lock_guard lock(lock_)`, scan `cdc_stream_map_` for streams whose `table_ids` contains
`table->id()`. For each such stream, call `DeleteCDCStream` asynchronously (or directly inline
if the barrier leak in G1 is fixed first, making it crash-safe). Alternatively, mark them
DELETING in sys catalog and let the maintenance cleanup handle it.

---

## 4. What is FINE

**Barrier cap (`cdc_max_barrier_releases_per_run`) correctness** (catalog_manager.cc:8945-8969):
Deferred releases are carried in `cdc_barriered_tablets_` to the next pass. Barrier SETs are
never throttled. Permanently deferring is not possible because deferred tablets are re-tried
every pass until released. No starvation of a specific tablet class. The `released` counter
correctly counts only actual RELEASE calls (not the `ContainsKey` skips), so 1000 releases
always happen if 1000 tablets need releasing. Flagged in backlog as P3 observability item only.

**Maintenance re-entrancy / master failover convergence** (catalog_manager.cc:8750-8993):
`RunCDCStreamMaintenance` is triggered by the background task loop only on the leader. On new
leader election the loop restarts. `cdc_stream_map_` and `cdc_tablet_checkpoint_map_` are
loaded fresh from sys catalog before the first maintenance pass. `tablet_min_index` is computed
from scratch. All SET RPCs carry the current wall-clock `barrier_seq`, so they win over any
stale SET from the old leader (last-writer-wins, cdc_service.cc:2182-2190). Maintenance is
idempotent and converges correctly after failover for the normal (non-crashed-mid-delete) case.

**range-partition DROP handled correctly** (RunCDCStreamMaintenance step 2, line 8886-8918):
Dropped range partition tablets are detected via `l.data().is_deleted()`, their checkpoint rows
are pruned from sys catalog and memory, and their barriers are released in step 4. No residual
WAL pin after one maintenance interval.

**Monotone checkpoint advancement** (UpdateCDCCheckpoint, line 8702-8706):
Checkpoint is written as `max(existing, incoming)` -- a new leader with a lagging local WAL
anchor cannot roll the durable checkpoint backward. Prevents spurious barrier retraction.

**Stream list consistency across master failover**: `LoadCDCStreams` and `LoadCDCTabletCheckpoints`
are both called in the leader-ready sequence (catalog_manager.cc:1709-1726) before the
maintenance loop starts. The in-memory maps are fully populated before any RPC is served.

**ListCDCStreams for table_id filter** (catalog_manager.cc:8555-8563): Iterates all streams
and checks each stream's `table_ids` list. Consistent with sys catalog state because the shared
`lock_` is held throughout.

**DeleteCDCStream orphaned-tablet barrier release** (catalog_manager.cc:8479-8488 + 8535-8543):
Correctly distinguishes orphaned tablets (no other stream references them, release aggregate
barrier) from shared tablets (skip aggregate barrier update, release only consumer anchor).
Semantics are sound.

**GetCDCStreamInfo ACTIVE guard** (catalog_manager.cc:8621-8623): Forward-looking guard for the
two-phase delete rejects non-ACTIVE streams. Correct and prod-safe once DELETING is used.
