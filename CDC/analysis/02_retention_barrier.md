# CDC WAL Retention, Log GC, and Retention-Barrier Lifecycle
## Analysis: Kudu CDC vs YugabyteDB CDC

---

## 1. Summary

Kudu's three P0-1 WAL-retention backstops (disk-pressure valve, age ceiling, time floor) are
correctly implemented and structurally sound. The architecture -- master-push barriers persisted
into the tablet superblock, read back on first GC after restart -- provides durable protection
across leader changes and tserver restarts without depending on any in-flight RPC.

Three production-shaping gaps remain:

1. **The force-release path (disk pressure / age ceiling) releases WAL GC but does not release
   the in-memory MVCC/UNDO history floor.** When disk pressure fires, WAL GC resumes but
   compaction/flush-UNDO GC stays blocked by the stale `cdc_history_floor_`, preventing
   reclamation of rowset history data. YB separates WAL and history staleness into independent
   clocks, so each can be released without the other.

2. **No integration-test coverage for either P0-1 backstop flag.** YB has a multi-hundred-line
   integration test (`cdc_service-int-test.cc`) that exercises `log_stop_retaining_min_disk_mb`
   and `log_max_seconds_to_retain`. Kudu has zero such coverage; the backstops are code-review-
   only verified.

3. **YB's `log_retention_diagnostics_min_age_secs` produces a structured per-tablet retention
   factor string** (naming which anchor -- CDC index, txn coordinator, pending ops -- is
   blocking GC) that feeds into logs and alerting. Kudu has no equivalent: when a tablet holds
   excess WAL, operators have only the `cdc_wal_retained_bytes` gauge to detect CDC as the
   culprit but cannot identify which factor is pinning without adding VLOG.

A lower-severity race (barrier_seq clock-skew across master failover) is documented in finding
G4 below; it is bounded by NTP sync time and the 86400s age ceiling, so it is not an immediate
production blocker.

---

## 2. Findings Table

| # | Gap | Severity | YB anchor | Kudu status | Why prod-shaping | Kudu sketch | New/Dup |
|---|-----|----------|-----------|-------------|-----------------|-------------|---------|
| G1 | Force-release (disk/age) releases WAL GC but NOT history floor | HIGH | `tablet_peer.cc:1365-1397` (separate staleness clocks for WAL and history; each released independently) | `tablet_replica.cc:926-999` sets `skip_cdc_clamp` but never calls `SetCDCHistoryFloor(Timestamp(0))`; `cdc_history_floor_` stays set until master sends release RPC | During disk-pressure event: WAL GC unblocked but UNDO compaction cannot reclaim rowset history; disk fill continues from UNDO; defeats the disk-pressure valve's purpose | In `GetRetentionIndexes()`, when `skip_cdc_clamp=true`, additionally call `replica->tablet()->SetCDCHistoryFloor(Timestamp(0))` to release MVCC floor; OR add a separate `cdc_history_floor_max_age_secs` backstop parallel to `cdc_max_wal_retention_secs` | NEW |
| G2 | No integration tests for cdc_stop_retaining_min_disk_mb or cdc_max_wal_retention_secs | MEDIUM | `cdc_service-int-test.cc:2299-2361` injects `log_stop_retaining_min_disk_mb=1`, uses `TEST_record_segments_violate_min_space_policy` to verify segments are released; `xcluster-test.cc:3062-3088` covers age path | `cdc_failover-itest.cc` and `log-test.cc` have zero references to either flag | Untested backstops have failed silently in both YB and Kudu histories; disk-exhaustion incidents are the most common CDC production incident | Add test that: (a) creates a CDC stream, (b) injects a mock `GetSpaceInfo` returning < threshold, (c) asserts `cdc_barrier_forced_releases` counter increments and GC proceeds; similarly for age ceiling by setting `cdc_max_wal_retention_secs=1` and advancing time | NEW |
| G3 | No per-tablet retention factor diagnostics string | MEDIUM | `tablet_peer.cc:1047-1198` `GetEarliestNeededLogIndex()` builds a structured factor string naming each contributor; `log.h:108-110` `WalRetentionDiagnostics`; `tablet_peer.cc:1228-1255` logs if oldest retained segment exceeds `log_retention_diagnostics_min_age_secs` | `tablet_replica.cc:818` `RunLogGC()` calls `GetRetentionIndexes()` which returns only the computed index; no factor decomposition; `cdc_wal_retained_bytes` gauge confirms CDC is the cause but does not say why the barrier is pinned | Ops cannot distinguish "master stopped sending barriers" vs "consumer checkpoint stuck" vs "disk check failing" without enabling VLOG_4 on every tserver; critical for SLA incidents | Add a `GetRetentionDetails(RetentionIndexes* ret, std::string* factors_out)` variant in `GetRetentionIndexes()` that records the CDC barrier index and whether disk/age forced a release; log it every N GC passes when `cdc_wal_retained_bytes > 0` and retention age exceeds a threshold | NEW |
| G4 | barrier_seq clock-skew window after master leader failover | LOW | N/A -- YB uses tserver-local staleness (no barrier_seq concept); no wall-clock-based sequencing across masters | `cdc_service.cc:2182-2190` barrier gate compares `barrier_seq` (wall clock micros) to `barrier_last_seq_[tablet_id]`; after master M1 sends seq=T and fails, new master M2 with clock T-delta sends seq=T-delta which is rejected by tservers still holding seq=T; barrier advancement stalls until M2's clock catches up or tservers restart | Stalls for O(NTP_skew) typically < 1min; harmless given 86400s age ceiling; but if clocks are misconfigured (skew > max_wal_retention_secs) the barrier is permanently frozen until age ceiling fires | On master election, stamp an initial seq = max(now, last_known_seq+1) by reading barrier_last_seq from a quorum-replicated field, or use logical epoch counters; alternatively reset tserver barrier_last_seq_ on tserver restart (already happens -- restart clears the map -- so recovery is automatic after tserver restart) | NEW |

---

## 3. Pressure-Test of the P0-1 Backstops

### 3a. Disk-Pressure Valve (--cdc_stop_retaining_min_disk_mb)

**Implementation** (`tablet_replica.cc:938-952`):
```
SpaceInfo si;
const string wal_dir = meta_->fs_manager()->GetTabletWalDir(tablet_id());
if (meta_->fs_manager()->GetEnv()->GetSpaceInfo(wal_dir, &si).ok()) {
  const int64_t free_mb = si.free_bytes / (1024LL * 1024);
  if (free_mb < min_disk_mb) {
    skip_cdc_clamp = true;
  }
}
```

**Verdict: Correct on the WAL path; incomplete on the history path.**

- Path correctness: checks `GetTabletWalDir(tablet_id())` = `<wals_root>/<tablet_id>`, which
  is the filesystem where WAL segments live. The `statvfs` call on the directory gives free
  bytes for that mount point. This is semantically correct and cheaper than YB's per-segment
  approach (`log_reader.cc:270` calls `GetFreeSpaceBytes(segment->path())` once per segment).

- Threshold semantics: `free_mb < min_disk_mb` is a binary all-or-nothing gate. When true, the
  entire retention barrier is skipped (all CDC WAL floor removed). YB's `ViolatesMinSpacePolicy`
  at `log_reader.cc:266-289` is per-segment and accumulates `potential_reclaimed_space` across
  segments, allowing partial release. Kudu's approach is more aggressive but intentionally so
  (barrier released = GC resumes = disk freed quickly).

- Race-free: the check is inside `GetRetentionIndexes()` which is called under no lock that
  could produce a deadlock. The function is `const`; the spinlock it takes (`cdc_barrier_lock_`)
  is for the age-clock state only.

- **GAP (G1)**: When `skip_cdc_clamp` is set, the function returns `ret` with only the Raft
  durability floor -- it does NOT call `tablet->SetCDCHistoryFloor(Timestamp(0))`. The
  in-memory `cdc_history_floor_` (`tablet.h:937`) stays at the last master-pushed value.
  `GetTabletAncientHistoryMark()` at `tablet.cc:1564-1566` clamps the AHM to the CDC floor,
  so UNDO GC and compaction keep honoring the stale floor. During a disk-full event, WAL GC
  proceeds but rowset UNDO history cannot be reclaimed -- the valve is half-open.

- **GAP (G2)**: No integration test exercises this code path. YB has
  `cdc_service-int-test.cc:2300-2361` which sets `log_stop_retaining_min_disk_mb=1`, injects
  free-space below threshold, and asserts segments are released.

### 3b. Age Ceiling (--cdc_max_wal_retention_secs, dead-master backstop)

**Implementation** (`tablet_replica.cc:962-973`):
```
const int64_t max_retain_secs = FLAGS_cdc_max_wal_retention_secs;
if (!skip_cdc_clamp && max_retain_secs > 0 &&
    cdc_min_op_index < ret.for_durability && last_advanced_us > 0) {
  const int64_t age_secs = (now_us - last_advanced_us) / 1000000LL;
  if (age_secs > max_retain_secs) {
    skip_cdc_clamp = true;
  }
}
```

**Verdict: Correct and safe; initialisation prevents false-fire on restart.**

- Clock initialization: `cdc_barrier_prev_op_index_` starts at -2 (`tablet_replica.h:634`),
  a sentinel that cannot be a real barrier index. On the first GC pass after restart (or
  leader-change), the test `cdc_min_op_index != cdc_barrier_prev_op_index_` is true, so
  `cdc_barrier_last_advanced_micros_` is set to NOW (`tablet_replica.cc:919-921`). The guard
  `last_advanced_us > 0` (`line 964`) also prevents firing when the clock has never been set.
  Together these ensure no immediate fire on restart.

- The guard `cdc_min_op_index < ret.for_durability` (`line 964`) skips the check when the
  barrier is not actually holding extra WAL (barrier is at or above the Raft floor), which
  is correct: no point firing if nothing is being retained.

- YB equivalent: `tablet_peer.cc:1284-1318` `reset_cdc_min_replicated_index_if_stale()`,
  stale secs = `FLAGS_cdc_min_replicated_index_considered_stale_secs` (default 1800s for
  normal tablets, 14400s for sys catalog). YB's default is much shorter (30min vs Kudu's
  24h). Both are adjustable at runtime. YB fires based on a tserver-monotonic clock
  (`MonoTime`), Kudu uses wall clock (`GetCurrentTimeMicros()`); monotonic is safer
  across NTP adjustments but the difference is minor at 86400s granularity.

- **GAP (G1, same as above)**: When this backstop fires, history floor is not released.

- **GAP (G2)**: No integration test for this backstop exists in Kudu.

### 3c. Time Floor (--cdc_wal_retention_secs)

**Implementation** (`tablet_replica.cc:994-998`, `log.cc:1005-1009`):
```
// In GetRetentionIndexes():
ret.cdc_wal_retention_deadline_micros =
    GetCurrentTimeMicros() - FLAGS_cdc_wal_retention_secs * 1000000L;

// In GetPrefixSizeToGC():
if (retention_indexes.cdc_wal_retention_deadline_micros > 0 &&
    segment->footer().has_close_timestamp_micros() &&
    segment->footer().close_timestamp_micros() >=
        retention_indexes.cdc_wal_retention_deadline_micros) {
  break;
}
```

**Verdict: Correct and segment-granularity accurate.**

- Applied per-segment against the segment's footer `close_timestamp_micros`, so it is WAL
  boundary-aligned (never splits a segment mid-write).

- The time floor is NOT applied when `skip_cdc_clamp` fires (disk/age release), because
  `cdc_wal_retention_deadline_micros` stays at 0 in that branch. This is correct: if the
  barrier is force-released, there is no CDC time floor to enforce either.

- YB equivalent: `log.cc:1617-1634` `ApplyTimeRetentionPolicy()` using `SegmentAgedOutOfTimeRetention()`.
  Semantically identical. Kudu's approach reads the same close timestamp from the segment footer.

---

## 4. What Was Checked and Found Correct

- **Barrier durability across restart**: `tablet_metadata.cc:1060-1082` persists
  `cdc_min_retained_op_index_` and `cdc_history_safe_time_micros_` in the superblock via
  `SetCDCRetentionBarrier()`, flushed on every barrier change (`cdc_service.cc:2262-2265`).
  `GetRetentionIndexes()` reads from the superblock (`tablet_replica.cc:909`), so the first GC
  after restart honors the barrier without waiting for the master's next maintenance pass.

- **Barrier durability across leader change**: The superblock is per-tablet across all peers.
  Every replica receives the barrier RPC (master fans to all), so even a follower that becomes
  leader immediately has the correct persisted barrier.

- **Barrier ordering / last-writer-wins**: `cdc_service.cc:2182-2190` gates every incoming
  RPC against `barrier_last_seq_[tablet_id]`; a SET with seq < current is discarded. The map
  is not erased on release so a stale SET cannot re-anchor after a RELEASE. On tserver restart
  the map clears (in-memory), but both sides recover: the new master's first maintenance pass
  sends a correctly-stamped barrier; age-ceiling guards the gap.

- **Checkpoint monotonicity**: `catalog_manager.cc:8702-8705` stores
  `max(existing_op_index, new_op_index)` at the master level, preventing a lagging-leader
  report from moving the checkpoint backward and inflating the retention scope.

- **Stream expiry / non-advancing staleness**: `catalog_manager.cc:8811-8834` checks both
  `last_active_time_micros` (consumer idle) and `last_checkpoint_advance_time_micros` (consumer
  polling but not progressing) independently. A non-advancing consumer stops pinning retention
  after `--cdc_max_staleness_ms`, even if it keeps polling (which refreshes `last_active` but
  not `last_advance`).

- **Tablet DELETE / tombstone handling**: `ts_tablet_manager.cc:1204` calls
  `cdc->ReleaseAnchorsForTablet(tablet_id)` before deleting a tablet, releasing both the
  retention anchor and all per-stream consumer anchors. `catalog_manager.cc:8863-8883`
  identifies `is_deleted()` tablets in `gone_tablets` and excludes them from the barrier-set
  step, so deleted tablets are never re-barriered.

- **Orphaned barriers (stream deleted, RPC lost)**: The `cdc_max_wal_retention_secs` age
  ceiling fires on the tserver after 86400s even with no master contact. For the history floor,
  the master's delete path sends `history_safe_time_micros=0` in the release RPC; if that RPC
  is lost, the tserver age ceiling does NOT clear the history floor (G1 above), but the WAL
  floor is eventually released.

- **Mass-release throttle**: `catalog_manager.cc:8945-8969` caps releases at
  `--cdc_max_barrier_releases_per_run` per maintenance pass to avoid flooding the master's
  outbound RPC path; deferred releases are retried next pass with correct seq.

- **Disk valve path correctness**: `fs_manager.h:283-285` `GetTabletWalDir()` returns
  `<wals_root>/<tablet_id>`, which is the correct filesystem for the WAL segments. The
  `statvfs`-equivalent call on the directory is cheaper than YB's per-segment calls.

- **History floor correctly clamps AHM**: `tablet.cc:1560-1566` applies
  `cdc_history_floor_` as an upper bound on the Ancient History Mark in
  `GetTabletAncientHistoryMark()`, preventing UNDO GC past the CDC consumer's read frontier.

- **Flag validator**: `catalog_manager.cc:715-726` `ValidateCdcMaxStaleness()` enforces that
  `cdc_max_staleness_ms > cdc_bg_scan_interval_ms` so the staleness guard can always fire.
