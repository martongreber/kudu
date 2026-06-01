# CDC Producer GetChanges Path: Production-Readiness Analysis

Analysis date: 2026-08-28
Reviewer: automated deep-dive (claude-sonnet-4-6)
Branch: cdc (Kudu) vs. YB main (xcluster_producer.cc, cdc_service.cc)

---

## 1. Summary of Biggest Gaps

The Kudu port's GetChanges producer path is architecturally sound and well-structured. The most
significant production gap is that the CHANGE-mode WAL read loop (`ReadReplicatesInRange` called from
`ReadChanges`) has no deadline parameter and no per-iteration deadline check: on a cold disk or under
heavy I/O contention a single GetChanges call can hold an RPC worker thread past the client deadline,
wasting the work and potentially starving the CDC service pool. The closely related second gap is that
the transaction-span escalation loop (`while(true)` in `ReadChanges`) also has no deadline budget; a
large transaction requiring several doublings (8 MB -> 16 MB -> ... -> 512 MB) issues multiple full WAL
reads from disk with no bail-out. Third, CHANGE-mode WAL reads allocate raw `ReplicateMsg*` objects
(and the decoded `txn_buffers` for multi-write transactions) with zero MemTracker involvement: the
`scan_mem_tracker_` / `TryAcquireScanSlot` guards cover only snapshot and FULL scans, so a burst of
CHANGE-mode consumers can silently consume large amounts of heap. Fourth, Kudu CDC bypasses the log
cache entirely (going straight to `log->reader()`) while YB xCluster reads benefit from the in-memory
Raft log LRU, increasing disk I/O per call for recently-active tablets. Finally, the server-produced
`have_more_records` field is dead code: `CDCTabletPoller::PollOnce` ignores it and uses
`resp.records_size() > 0` for backoff decisions, so the signal from the server is never acted on.

---

## 2. Findings Table

| # | Gap | Severity | YB anchor (file:line + mechanism) | Kudu status (file:line) | Why prod-shaping | Kudu-idiomatic sketch | New or dup |
|---|-----|----------|----------------------------------|------------------------|------------------|-----------------------|-----------|
| G1 | **No deadline in WAL read loop** | P2 | `yb/consensus/log_reader.cc:450` per-entry `CoarseMonoClock::Now() >= deadline` check; same in `log_cache.cc:437`. Both the log cache AND the log reader check deadline on every entry iteration. | `kudu/consensus/log_reader.cc:280` `ReadReplicatesInRange` has no deadline parameter; `ReadChanges` passes `apply_deadline` only to `ReconstructBeforeAfterImages` (FULL mode). CHANGE-mode WAL read runs to completion regardless of how close the RPC deadline is. | RPC worker thread blocked past client deadline; on slow disk or cold page cache with max_bytes/entry_size iterations this can take seconds, starving the CDC service worker pool | Add `MonoTime deadline` param to `ReadReplicatesInRange`; check `MonoTime::Now() >= deadline` at top of the for-loop; return partial batch (already supported by the `replicates_tmp` partial-result pattern); caller treats partial as normal truncation | **New** |
| G2 | **Transaction-span escalation loop has no deadline** | P2 | YB does not have an analogous in-WAL-scan escalation (intents/VWAL design). No direct anchor. | `kudu/cdc/cdc_service.cc:1431-1485` `while(true)` escalation loop doubles `effective_max_bytes` (8 MB -> 512 MB) and re-reads from disk on each iteration with no time budget check. On a slow disk a 512-MB scan can run for many seconds. | A single large transaction causes multiple full WAL re-reads without deadline enforcement; RPC thread is monopolized for unbounded time | Before each `ReadReplicatesInRange` call inside the loop check `MonoTime::Now() >= deadline`; if exceeded, return a retryable error (e.g. `TimedOut` -> `SERVER_TOO_BUSY`) so the consumer retries when I/O conditions improve | **New** |
| G3 | **No MemTracker for CHANGE-mode WAL read allocations** | P2 | `yb/cdc/xcluster_producer.cc:362-365` `ScopedTrackedConsumption(context.mem_tracker, read_ops.read_from_disk_size)` tracks WAL bytes read from disk against a per-stream child of the Tablet MemTracker. Also `yb/consensus/log_util.cc:989-997` `read_wal_mem_tracker_->TryConsume(estimated)` gates segment reads under `ObeyMemoryLimit::kTrue`, returning `Status::Busy` on OOM which propagates as `have_more_messages=true` rather than a crash. | `kudu/cdc/cdc_service.cc:834-873` `TryAcquireScanSlot` / `scan_mem_tracker_` guards are acquired only for snapshot and FULL scans (lines 1050, 1123). CHANGE-mode WAL streaming (the hot path) has no `TryAcquireScanSlot` call and no `scan_mem_tracker_` reservation. The `replicates` vector allocated by `ReadReplicatesInRange` is invisible to the memory subsystem. | At 9 concurrent CHANGE-mode calls (10 threads, 10% free ratio) each reading 8 MB, 72 MB of untracked heap is consumed. No back-pressure mechanism prevents exceeding the server memory budget. | Include CHANGE-mode calls in `scan_mem_tracker_` or create a separate `cdc_wal_reads` child tracker; reserve `max_bytes` before the read and release in `ScopedCleanup`; add a flag `--cdc_wal_read_mem_limit_bytes` for a soft cap (0 = unlimited, matching current behavior) | **New** |
| G4 | **`have_more_records` is computed but never consumed** | P3 | YB's `HaveMoreMessages` is returned from `ReadReplicatedMessagesForXCluster` and used by the poller to determine whether to back off. | `kudu/cdc/cdc_service.cc:1494-1496` server sets `resp.have_more_records` correctly. `kudu/cdc/cdc_consumer.cc:443-451` `PollOnce` sets `got_records = resp.records_size() > 0` and never reads `resp.have_more_records()`. The server signal is dead code. | In the edge case where the byte cap truncates a batch but all read records are buffered under an open transaction (records_size==0 yet more WAL data is available), the consumer backs off unnecessarily instead of polling again | Wire `resp.have_more_records()` into `PollOnce`: if true and `got_records` is false, reset backoff and poll immediately without sleeping | **New** |
| G5 | **CDC reads bypass the log cache (direct disk reads)** | P3 | `yb/consensus/consensus_queue.cc:800-801` `ReadReplicatedMessagesForXCluster` calls `ReadFromLogCacheForXRepl` which reads from the in-memory Raft log LRU cache first; disk only on cache miss. Multiple streams on the same tablet share cache hits. | `kudu/cdc/cdc_service.cc:1398-1435` `log->reader()` obtained directly; all `ReadReplicatesInRange` calls go to the `LogReader` (disk) unconditionally, bypassing the in-memory consensus log cache that Raft replication fills. | Under steady-state streaming (consumer near real-time), recently-committed WAL entries are in the OS page cache and the disk read is cheap. But under multiple CDC streams on the same tablet, or after leader election, direct reads amplify disk I/O vs. cache-assisted reads. | Not trivially fixable (Kudu's log cache is private to `PeerMessageQueue`); a pragmatic alternative is a CDC-local read-ahead buffer shared across concurrent streams on the same tablet. Lower priority given OS page cache helps significantly. | **New** |

---

## 3. Pressure-Test on "DONE" Backlog Items

### P0-1: TRANSACTION_TOO_LARGE fail-loud (bounded-WAL correctness)

**Status: Mostly YB-grade, with one deficiency.**

The wedge-detection and escalation logic at `kudu/cdc/cdc_service.cc:1431-1485` is sound. The
`OldestOpenTxnFirstIndex` helper correctly identifies the pinned index, the `wedged` predicate guards
against false wedge classification (requires both `truncated_by_cap` AND `open_first == from_op_index + 1`),
and `TRANSACTION_TOO_LARGE` is surfaced loudly rather than stalling silently.

**Deficiency (ties to G2 above):** The escalation `while(true)` loop calls `ReadReplicatesInRange`
(a blocking disk read) on each doubling iteration with no deadline budget. For a 512-MB transaction
ceiling and starting cap of 8 MB, this can issue up to 6 full disk reads (8 -> 16 -> 32 -> 64 -> 128 ->
256 -> 512 MB read attempts) from a cold WAL. YB does not have this escalation pattern (its intents
model eliminates the need), so there is no direct YB anchor to compare against, but the fix is
straightforward: check `MonoTime::Now() >= deadline` before each iteration and return `TimedOut` (
retryable `SERVER_TOO_BUSY`) if the deadline would be exceeded. Without this, a single large
transaction can monopolize an RPC worker thread for tens of seconds.

### P1-1: op-index lag + bootstrap-required gauges

**Status: YB-grade.**

`cdc_stream_ops_behind`, `cdc_stream_bootstrap_required` per-session and `cdc_max_ops_behind`,
`cdc_bootstrap_required_streams` server-level function gauges are correct and properly wired.
`last_known_min_replicate_index` is updated at `cdc_service.cc:1769` only on successful reads,
which is conservative but not wrong (a consumer stuck with WAL_EXPIRED stops updating the gauge,
which gives a stale but pessimistic view - acceptable for monitoring).

### P1-4: per-error-code + admission-shed counters

**Status: YB-grade.**

`SetCDCError` at `cdc_service.cc:665-679` correctly routes to per-code counters.
The out-of-range guard (`idx < 1 || idx >= size -> UNKNOWN_ERROR`) is defensive.
`scans_rejected_concurrency_`, `scans_rejected_memory_`, `scans_rejected_worker_pool_` are
incremented at the right decision points (TryAcquireScanSlot and the free-ratio check).
One micro-concern: the comment at `cdc_service.cc:348-349` says "index matches CDCErrorPB::Code (1-12)"
but the array is indexed by enum value via a `std::array<>` of size matching the max code + 1.
If a new error code is added beyond the current max, the guard falls back to UNKNOWN_ERROR. This is
fine but should be documented as "extend error_code_counters_ when adding new CDCErrorPB::Code values."

### P1-2: WAL/history retained-bytes gauges

Not directly touching the GetChanges read path; no additional findings.

### P1-3: master maintenance-loop observability

Not directly touching the GetChanges read path; no additional findings.

---

## 4. Things Checked and Found FINE

The following areas were examined against the YB xCluster path and found to be correctly or
adequately implemented in Kudu:

- **Leader check + term capture before read**: `cdc_service.cc:1332-1340` captures `leader_term`
  and checks `consensus->role() == LEADER` before reading. Post-read recheck at `1794-1801` checks
  both role AND term (`CurrentTerm() != leader_term`). This is belt-and-suspenders vs. YB's term-only
  check (equivalent in Raft, since a new leader always bumps the term) but harmless.

- **TABLET_NOT_RUNNING vs. TABLET_NOT_LEADER disambiguation**: `cdc_service.cc:1313-1321` uses
  `replica->CheckRunning()` to distinguish a bootstrapping/crashed replica from a non-leader, returning
  `TABLET_NOT_RUNNING` (retryable, correct error code) before the leader check. YB achieves the same
  distinction via `tablet_peer->IsLeaderAndReady()` at `yb/cdc/cdc_service.cc:1706-1710`.

- **from_op_index validation**: `cdc_service.cc:1002-1009` rejects negative from_op_index early
  before any WAL access. YB's equivalent is implicit in its OpId semantics.

- **Schema version tracking across multiple ALTERs in a batch (E9 fix)**: `cdc_service.cc:1544-1556`
  correctly derives the base schema version from the first ALTER's `new_version - 1` (not from
  `tablet_metadata()->schema_version()` which may lag apply). Multiple ALTERs in the same batch are
  handled correctly by the in-loop `running_schema_version = record.new_schema_version()` update at
  line 1723. This is more explicit than YB's equivalent (which relies on docdb encoding).

- **WAL_EXPIRED vs. STREAM_EXPIRED disambiguation**: `cdc_service.cc:1447-1450` correctly
  classifies the GC'd case based on session idle time (`StreamIdleExpired`). The reactive-only
  approach (checked at miss time, never proactively) is appropriate.

- **Transaction committed-only semantics**: `txn_buffers` / `txn_first_index` / `open_min` pinning
  at `cdc_service.cc:1569-1756` correctly suppresses uncommitted and aborted transactions, emits
  BEGIN/COMMIT wrappers at the FINALIZE_COMMIT op, and pins the checkpoint at `open_min - 1`.

- **FULL-mode before/after image deadline**: `apply_deadline` is correctly derived from the
  client deadline with the safe ratio reservation and capped by `cdc_full_apply_wait_timeout_ms`
  (`cdc_service.cc:1133-1146`). The deadline is passed through to `ReconstructBeforeAfterImages`.

- **Snapshot consistency (E4, E10)**: `snapshot_start_locks_` serializes concurrent start requests;
  `SNAPSHOT_SESSION_LOST` is returned for resume-key with no active session; the server's
  `resume_key` (not the client-supplied key) is used as the single source of truth for scan position.

- **Byte cap on snapshot pages vs. WAL streaming**: `cdc_snapshot_max_bytes_per_response` provides
  a smaller cap for snapshot pages, reducing iterator competition with user scans.

- **Admission control for GetChanges (RPC worker reservation)**: `get_changes_inflight_` counted
  against `cdc_get_changes_free_rpc_ratio * rpc_num_service_threads` cap at `cdc_service.cc:958-980`.
  The always-at-least-1 guard prevents a high free ratio from denying all service with few threads.

- **Stream-not-found early reject**: `GetOrFetchStreamConfig` single-flighted per stream ID
  (`GetStreamConfigFetchLock`), with NotFound evicting the cache entry and surfacing `STREAM_NOT_FOUND`
  rather than CHANGE-mode data. Stale config served on master blip (not on authoritative NotFound).

- **Checkpoint durable persist throttle**: `cdc_checkpoint_persist_interval_ms` limits master RPCs
  to one per interval per session; anchor is still advanced on every Checkpoint call so GC is
  unaffected. The at-least-once re-read window on a crash is bounded by the interval.

- **have_more_records computation (server side)**: `cdc_service.cc:1494-1496` correctly signals
  truncation vs. caught-up. The subtle exception (records deferred by an open transaction are NOT
  counted as "more") is intentional and correctly commented (line 1492-1493).

- **Single oversized WAL entry (non-txn)**: `log_reader.cc:336-338` `replicates_tmp.empty()`
  guard ensures the first op is always included even if it exceeds `max_bytes_to_read`. This is
  correct -- you need at least one op to make progress.

- **`need_schema_info` DDL prepend**: Idempotent DDL record using `from_op_index` (no checkpoint
  advance) at `cdc_service.cc:1347-1356`. Correctly uses `schema_version` from metadata.

- **WAL anchor update before read**: Anchor is updated at `cdc_service.cc:1394` before
  `ReadReplicatesInRange` to prevent GC of the segment being read. Correct ordering.

- **Post-read metrics update (sent lag, min replicate index)**: Atomic updates at
  `cdc_service.cc:1761-1784` using relaxed loads/stores are safe (monotone increases and the
  data is for monitoring only).
