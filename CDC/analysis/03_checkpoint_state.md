# 03 Checkpoint Durability & CDC State Persistence / Recovery

Subsystem: where the resume point and stream state live and how they survive crashes,
leader changes, and restarts.

Reviewed against: YugabyteDB `src/yb/cdc/` and Apache Kudu `src/kudu/cdc/` + `src/kudu/master/`.

---

## 1. Summary

Kudu's master-persisted checkpoint design (DR-002) is architecturally sound. The
superblock-persisted aggregate barrier on every replica (including future leader
candidates) is the core safety mechanism: it means a new leader starts with WAL
retention already in effect from its own disk, without waiting for a master RPC.

Two production-shaping gaps were found:

**CF-1 (HIGH)**: The ACK-before-persist window (DR-007) is UNSAFE specifically for
the very first Checkpoint call on a brand-new stream. Until `PersistCheckpoint`
succeeds once, no CDC barrier exists anywhere (superblock = -1 on all replicas,
master has no checkpoint row). A tserver crash in this window leaves the new leader
with no WAL retention guard; normal Raft GC can discard the ops the consumer stored
as its checkpoint. Every subsequent checkpoint is safe (superblock barrier at N-X
protects the consumer's N). Fix: push an initial barrier to all replicas at stream
creation.

**CF-2 (HIGH)**: The `--cdc_max_staleness_ms` guard (catalog_manager.cc:8825)
fires on `last_checkpoint_advance_time_micros` staleness in the master's sys
catalog. `PersistCheckpoint` is best-effort and silent on failure (cdc_service.cc:
2363). A prolonged master outage causes `PersistCheckpoint` to fail while the
consumer's Checkpoint RPC returns SUCCESS and the local WAL anchor advances. After
the outage lasts > `cdc_max_staleness_ms` (default 4h), the maintenance pass
classifies the stream as stale, releases the barrier, and WAL GC can discard ops
the consumer has NOT yet durable-checkpointed to the master. The consumer last saw
OK; WAL_EXPIRED comes later from a different leader. Fix: surface PersistCheckpoint
failures to the consumer (or guard against staleness from master outage separately).

Active-time staleness (CF-3) is a minor operational gap (5 min vs YB's 15s).
DR-006 snapshot-session handling is SAFE. Master failover is SAFE (sys catalog
durable load on recovery).

---

## 2. Findings Table

| # | Gap | Severity | YB anchor file:line | Kudu status file:line | Why prod-shaping | Kudu sketch | New/Dup |
|---|-----|----------|--------------------|-----------------------|-----------------|-------------|---------|
| CF-1 | First-checkpoint WAL retention race: no barrier before first successful PersistCheckpoint | HIGH | cdc_state_table updates active on every GetChanges (cdc_service.cc:4966-5040) | cdc_service.cc:1243-1250 (RespondSuccess before PersistCheckpoint); catalog_manager.cc:8429-8432 (no row until first UpdateCDCCheckpoint) | Consumer sees OK; crashes in window; new leader can GC WAL; consumer gets WAL_EXPIRED on reconnect | Send initial barrier to all replicas at CreateCDCStream with barrier_index = tablet's current committed op_index | NEW |
| CF-2 | `cdc_max_staleness_ms` fires during master outage while consumer is live | HIGH | N/A (YB cdc_state updates inline in GetChanges; no equivalent master-outage silence) | catalog_manager.cc:8824-8834 (staleness check on `last_checkpoint_advance_time_micros`); cdc_service.cc:2363 (KLOG silent failure of PersistCheckpoint) | Master unreachable > staleness_ms -> barrier released -> WAL GC'd -> WAL_EXPIRED despite consumer having valid checkpoint | Options: (a) emit retriable error to consumer on repeated PersistCheckpoint failure so consumer knows to pause; (b) track persist failure count and reflect in stream status; (c) separate staleness from master-outage by heartbeating advance time through the activity-only path even when no new checkpoint | NEW |
| CF-3 | Active-time staleness: 5 min vs YB's 15 s | LOW | cdc_service.cc:4966 (active_time written on every 15s GetChanges window) | cdc_service.cc:2399 (refresh_active_time_only, throttled by `cdc_active_time_report_interval_ms`=5min); cdc_service.h:112 (last_active_report_micros) | 5-min staleness window means a consumer that polls GetChanges rapidly but never Checkpoints reports as idle for 5min intervals; stream expiry jitter up to 5min | Lower `--cdc_active_time_report_interval_ms` default to match 30s; or drive the heartbeat from the Checkpoint path too | NEW |
| CF-4 | No snapshot_key durability: in-memory only; session lost on leader change | DESIGN (DR-006) | cdc_state_table.h:97 (snapshot_key field); cdc_service.cc:4966+ (UpdateCheckpointAndActiveTime persists snapshot_key) | cdc_service.h:61-77 (CDCSnapshotState in-memory); cdc_service.cc:1895-1901 (SNAPSHOT_SESSION_LOST on missing session) | Full re-snapshot on leader change; no stale-cursor safety issue (SNAPSHOT_SESSION_LOST is always returned) | Accepted (DR-006). See walkthrough below | DESIGN |
| CF-5 | Master maintenance cycle gap: up to 60s between barrier pushes to new leader | LOW | UpdatePeersAndMetrics continuous loop | catalog_manager.cc:994 (`cdc_bg_scan_interval_ms`=60s); tablet_replica.cc:909 (superblock barrier used on first GC without waiting for master) | New leader uses superblock barrier during the 60s window before master's next pass; retention is conservative (superblock value was the master's last push), safe by design | Already mitigated by superblock persistence on every replica | FINE (noted for completeness) |

---

## 3. Crash Scenario Walkthroughs

### Scenario A: DR-007 ACK-before-persist -- non-first checkpoint (SAFE)

**Setup**: Stream S has been running. Master has persisted checkpoint N-X for (S, T).
`RunCDCStreamMaintenance` has pushed barrier N-X to all replicas; every replica's
superblock has `cdc_min_retained_op_index = N-X`.

**Sequence**:
1. Consumer calls Checkpoint(N). N > N-X.
2. tserver Checkpoint() handler (cdc_service.cc:1200): `UpdateAnchor` moves
   in-memory WAL anchor to N on the leader. LogAnchorRegistry now holds N.
3. cdc_service.cc:1243: `context->RespondSuccess()`. Consumer stores checkpoint N.
4. cdc_service.cc:1250: `PersistCheckpoint(N)` scheduled. **Leader crashes here**.
5. No other replica got `PersistCheckpoint` RPC. Master's stored checkpoint stays
   at N-X.
6. New leader elected (was a Raft follower, received all committed ops through N+k).
   - Its superblock: `cdc_min_retained_op_index = N-X` (set by master's last
     maintenance pass, tablet_replica.cc:909).
   - No `stream_tablet_state_` for this consumer (in-memory only, cdc_service.h:81).
7. New leader's WAL GC: uses superblock barrier N-X from
   `meta_->cdc_min_retained_op_index()` (tablet_replica.cc:909). WAL segments below
   N-X can be freed; segments from N-X onward are protected.
8. Master's next `RunCDCStreamMaintenance` (within 60s): reads checkpoint N-X,
   pushes barrier N-X to all replicas (no change, confirms existing state).
9. Consumer reconnects, sends GetChanges(from=N) or Checkpoint(N).
   - New leader has WAL from N-X onward. N >= N-X. WAL at N is present.
   - At-least-once semantics: consumer may re-read records from N-X to N. Correct.

**Verdict: SAFE**. The superblock barrier at N-X on every replica is the guarantor.
The consumer's N >= N-X, so the WAL it needs is retained. The in-memory anchor
is only needed while the old leader is live; once it crashes the superblock takes
over.

---

### Scenario B: DR-007 ACK-before-persist -- FIRST checkpoint of a new stream (UNSAFE)

**Setup**: Stream S just created (catalog_manager.cc:8429-8432: no checkpoint rows).
No barrier has ever been pushed to any replica. All replicas' superblock:
`cdc_min_retained_op_index = -1` (tablet_metadata.cc:407-409).

**Sequence**:
1. Consumer calls GetChanges(from=0). Leader serves records 1..100.
2. Consumer calls Checkpoint(100). This is the FIRST Checkpoint for (S, T).
3. tserver cdc_service.cc:1200: `UpdateAnchor(100)`. In-memory anchor at 100.
4. cdc_service.cc:1243: `context->RespondSuccess()`. Consumer stores checkpoint 100.
5. `PersistCheckpoint` enters the rate-limiter check. `last_checkpoint_persist_micros
   == 0` so `persist = true` (cdc_service.cc:1230-1237). PersistCheckpoint is called.
   **Crash between RespondSuccess and PersistCheckpoint completing**.
   (Or PersistCheckpoint call itself crashes the process, or the RPC is sent but
   the tserver crashes before the RPC is acked by the master.)
6. Master has NO checkpoint row for (S, T). `cdc_tablet_checkpoint_map_` has no
   entry.
7. New leader elected. `cdc_min_retained_op_index = -1` in superblock. No CDC
   clamp applied in `GetRetentionIndexes()` (tablet_replica.cc:909: `if
   (cdc_min_op_index >= 0)` branch not taken).
8. Master's next `RunCDCStreamMaintenance`: no checkpoint row -> no entry in
   `tablet_min_index` for T -> no barrier RPC sent to any replica for T.
   Superblock stays at -1 on new leader.
9. Raft WAL GC runs on new leader based on Raft-consensus `for_durability` only.
   Op 100 is committed. Raft allows GC of segments below the commit watermark.
   `log_min_segments_to_retain` (default 2) might protect op 100 temporarily but
   this is time-bounded, not a correctness guarantee.
10. Consumer reconnects, calls GetChanges(from=100). Server returns WAL_EXPIRED
    (segment containing op 100 already GC'd).

**Verdict: UNSAFE**. The at-least-once guarantee does NOT hold for the first
checkpoint if the tserver crashes before PersistCheckpoint delivers.

**Root cause**: `CreateCDCStream` (catalog_manager.cc:8429) does not send an
initial barrier to any replica. The per-stream WAL barrier only exists after the
first successful `UpdateCDCCheckpoint`. There is a window between stream creation
and the first durable checkpoint where NO replica has a CDC barrier.

**Proposed guard**: In `CreateCDCStream`, after persisting the stream entry,
call `SendCDCRetentionBarrierToAllReplicas` for each tablet in the stream's tables
with `min_retained_op_index = 0` (retain all WAL from the current committed index
of each tablet). This is conservative but establishes a floor immediately. The
consumer's first `PersistCheckpoint` will advance it to the actual checkpoint.

---

### Scenario C: DR-007 -- master outage + staleness guard (UNSAFE, configuration-dependent)

**Setup**: Stream S running. Master's last persisted checkpoint for (S, T): N.
`cdc_max_staleness_ms = 4h` (default). `cdc_checkpoint_persist_interval_ms = 15s`.

**Sequence**:
1. Master becomes unreachable (network partition, crash, etc.).
2. Consumer keeps calling GetChanges and Checkpoint. Tserver responds SUCCESS to
   all Checkpoints. In-memory anchor advances to N+K for large K.
3. Each Checkpoint attempt calls `PersistCheckpoint` (cdc_service.cc:2320-2367).
   Every attempt fails with NetworkError. The failure is logged at WARNING rate
   (KLOG_EVERY_N_SECS(60)) but the caller (`Checkpoint()`) already responded
   SUCCESS and discarded the error.
4. Master recovers after 4h + epsilon. New master leader loaded sys catalog;
   checkpoint row shows op_index = N, `last_checkpoint_advance_time_micros = T_n`
   (when N was persisted, 4h ago).
5. Master runs `RunCDCStreamMaintenance` (catalog_manager.cc:8750).
   `last_advance = T_n`. `now_micros - last_advance > cdc_max_staleness_ms * 1000`.
   Condition at line 8824: `stale = true`. This tablet+stream is excluded from
   `tablet_min_index`.
6. `SendCDCRetentionBarrierToAllReplicas` is called with `min_retained_op_index=-1`
   (release) for this tablet (catalog_manager.cc:8957).
7. All replicas receive the release. Each calls `SetRetentionBarrier` with
   `min_retained_op_index=-1`. Superblock `cdc_min_retained_op_index` is cleared
   to -1 (tablet_metadata.cc:1076-1081). LogAnchorRegistry anchor is released.
8. Normal Raft GC resumes. WAL segments below Raft's `for_durability` floor are
   freed. The consumer is at in-memory checkpoint N+K (never persisted to master).
   WAL from N+1 onward, needed for the consumer's next GetChanges, may be GC'd.
9. Consumer calls GetChanges(from=N+K). WAL at N+K is gone. WAL_EXPIRED.
   Consumer stored checkpoint N+K. All work since N is lost; must re-bootstrap.

**Verdict: UNSAFE** when master downtime exceeds `cdc_max_staleness_ms`. The
consumer received continuous SUCCESS from Checkpoint but the master silently
abandoned retention. With default 4h staleness this requires a prolonged master
outage, but the risk is real in multi-region or scheduled-maintenance scenarios.

**Note on severity**: With default `cdc_max_staleness_ms=4h`, a master outage
must last > 4h to trigger this. If the operator lowers the staleness guard (e.g.,
to 30min to catch truly stuck consumers), the risk surface widens dramatically.

**Proposed guards**:
(a) When PersistCheckpoint has failed for > N consecutive attempts (e.g., for >
    `cdc_checkpoint_persist_interval_ms * 10`), surface a retriable error to the
    consumer so it pauses rather than accepting an unsafe OK.
(b) Separate the staleness guard from master-outage scenarios: only count staleness
    when the master CAN be reached but the checkpoint still does not advance (i.e.,
    distinguish "consumer not advancing" from "tserver cannot reach master").
(c) Persist a "last_active_contact_time" field separate from
    `last_checkpoint_advance_time_micros` so the staleness guard compares against
    consumer checkpoint staleness, not master-contact staleness.

---

### Scenario D: DR-006 -- leader change mid-snapshot (SAFE)

**Setup**: Consumer S on tablet T is mid-snapshot. Old leader has
`CDCSnapshotState{active=true, snap_ts=T0, streaming_start_op_index=X,
resume_key=K}` in its `stream_tablet_state_` (in-memory only).

**Sequence**:
1. Old leader crashes (or loses leadership).
2. New leader elected. `stream_tablet_state_` is empty (cdc_service.h:479-480).
   No snapshot session exists for (S, T).
3. Consumer reconnects to new leader. Sends GetChanges with
   `snapshot_resume_key = K` (continuation).
4. ReadSnapshot (cdc_service.cc:1895-1901):
   ```
   if (!req_resume_key.empty() && !has_active_session) {
       SNAPSHOT_SESSION_LOST
   }
   ```
   `has_active_session = false` (new leader, empty state map). Returns
   SNAPSHOT_SESSION_LOST immediately.
5. Consumer receives SNAPSHOT_SESSION_LOST. Restarts snapshot from the beginning
   with `is_snapshot_start=true`.
6. New leader establishes a fresh session with new `snap_ts`, captures
   `streaming_start_op_index` from current committed op-id (cdc_service.cc:1929).
   Snapshot proceeds correctly.

**Is there a window where a stale cursor is trusted?** No. The check at step 4 is
unconditional: any non-empty resume_key without an active server-side session
returns SNAPSHOT_SESSION_LOST. The server-side resume_key (E10, cdc_service.cc:1976)
is always used, never the client's key. A concurrent `is_snapshot_start=true`
request on the new leader establishes a fresh session with an empty server-side
resume key (correct restart from beginning).

**Verdict: SAFE** by design. Consumer must re-do the entire snapshot after a leader
change, but correctness is preserved. The `PersistCheckpoint` at snapshot start
and completion (cdc_service.cc:2094-2096) -- bypassing the rate limiter --
ensures the `streaming_start_op_index` is durably stored so followers have the
correct WAL barrier.

---

### Scenario E: Master failover -- checkpoint state recovery (SAFE)

**Setup**: Active master has `cdc_tablet_checkpoint_map_` in memory. All checkpoint
rows are durably written to sys catalog via `WriteCDCTabletCheckpoint`
(sys_catalog.cc:1079-1100) via `SyncWrite` (Raft-committed write to the sys
catalog tablet).

**Sequence**:
1. Master leader crashes.
2. New master elected. Calls `LoadCDCTabletCheckpoints`
   (catalog_manager.cc:8340-8380): reads all CDC_TABLET_CHECKPOINT rows from sys
   catalog via `VisitCDCTabletCheckpoints`. Full state is restored including
   `op_index`, `history_safe_time_micros`, `last_active_time_micros`,
   `last_checkpoint_advance_time_micros`.
3. `RunCDCStreamMaintenance` runs within 60s. Computes min checkpoints from the
   loaded rows. Pushes barriers to all replicas.
4. Tservers receive `UpdateCDCRetentionBarrier` RPC. Their superblocks are updated
   if the barrier changed (tablet_metadata.cc:1070-1081 via `SetCDCRetentionBarrier`
   + `Flush`).

**Verdict: SAFE**. The sys catalog is Raft-replicated; writes via `SyncWrite` are
durable before the caller returns. The checkpoint state survives master failover
completely. The maximum data loss window is bounded by `cdc_checkpoint_persist_
interval_ms` (15s) -- the time between checkpoint writes to master -- which is the
intended at-least-once contract.

---

## 4. What Is FINE

**Superblock barrier restoration on tserver restart/leader-change**: Every
`SetRetentionBarrier` call on every replica includes a `meta->Flush()` that
writes `cdc_min_retained_op_index` and `cdc_history_safe_time_micros` to the
tablet superblock (tablet_replica.cc:909, cdc_service.cc:2253-2267). A new leader
uses this value from the FIRST GC cycle, without waiting for the master's next
maintenance pass. This is the key crash-safety mechanism.

**Monotonic checkpoint persistence (E7)**: `UpdateCDCCheckpoint`
(catalog_manager.cc:8697-8703) only advances `op_index` when the incoming value is
strictly greater than the stored one. A new tserver leader whose in-memory anchor
lags cannot move the durable checkpoint backward.

**SNAPSHOT_SESSION_LOST always returned correctly (E4)**: The check
`!req_resume_key.empty() && !has_active_session` (cdc_service.cc:1895) is
unconditional. There is no code path on a fresh leader where a client-supplied
resume key is honored without a matching server-side session. Server-side resume
key is always the authoritative source (E10, cdc_service.cc:1976-1983).

**Multi-tablet checkpoint atomicity**: Each (stream, tablet) checkpoint is
independent. A master crash mid-fanout leaves some tablets at N and others at N-X.
The barrier is set to the min, which is conservative (retains more WAL). The
consumer will re-read at-most one `cdc_checkpoint_persist_interval_ms` of records
per tablet. Correct.

**Last-writer-wins barrier sequencing**: `barrier_seq` (wall-clock time from the
master's maintenance pass) prevents a reordered SET from landing after a RELEASE
(cdc_service.cc:2182-2191, cdc_service.h:547-554). A stale RELEASE cannot
spuriously drop retention.

**Stream config cache coherence**: The config cache (`CDCStreamConfigPB`) covers
only `record_type` and `snapshot_mode`, not schema. Schema is read from WAL records
directly with the E9 fix (cdc_service.cc:1544-1556). ALTER TABLE schema changes
are correctly attributed to the WAL op that carries them. The 5-min cache TTL for
record_type is a known operational lag, not a correctness issue.

**PersistCheckpoint for snapshot start/completion bypasses rate-limiter**:
cdc_service.cc:2094-2096 calls `PersistCheckpoint` unconditionally at snapshot
start and completion, ensuring `streaming_start_op_index` is durable before the
consumer begins WAL streaming. This closes the bootstrap-to-streaming gap.

**Barrier RPC sequence guard on delete**: `DeleteCDCStream`
(catalog_manager.cc:8500+) stamps the release RPC with `release_seq =
GetCurrentTimeMicros()`. This is > any previous SET from an earlier maintenance
pass, so the release always wins the last-writer-wins gate on replicas.
