# Kudu CDC vs YugabyteDB: Deep Production-Grade Gap Synthesis

> Cross-subsystem synthesis of a fresh, code-anchored comparison of the Kudu CDC
> port (branch `cdc`) against YugabyteDB's CDC implementation (`~/yugabyte-db`,
> xCluster async-replication path). Seven parallel deep-dives, one per subsystem;
> per-subsystem detail lives in `01_*.md` .. `07_*.md`. Every claim there is
> `file:line`-anchored in both trees.
>
> Purpose: go past the existing `production_readiness.md` scorecard (which is
> accurate for what it covers) and surface the production-grade GATES and SAFETY
> mechanisms it did not catch -- especially data-safety gaps and cross-service
> resource containment. Date: 2026-08-28.

---

## Headline

The feature set is right and the previously-tracked P0-1 (bounded-WAL) and P1
observability work is real. But this pass found **five new data-safety or
node-stability gaps that the scorecard missed**, and it re-frames one item the
scorecard had dismissed as cosmetic (`DELETING` state) into the single
highest-leverage P0. The gaps cluster into four themes:

1. **Stream lifecycle is not crash-safe** (create + delete boundaries).
2. **CDC is not resource-isolated from the rest of the tserver** (it can starve
   Raft and tip the node into OOM) -- distinct from "CDC fills the disk," which
   P0-1 already addressed.
3. **The existing P0-1 disk valve is half-open** (releases WAL but not history).
4. **The producer/consumer error contract is not machine-readable**, and
   observability still lacks byte-throughput and time-to-expiry.

The good news, code-verified: DR-006 (snapshot leader-change) and DR-007
(non-first checkpoint) are **SAFE** as designed; the P0-1 backstops are
structurally sound; and several Kudu metrics are actually *more* complete than
YB's. This is a hardening pass, not a redesign.

---

## Theme 1 -- Stream lifecycle is not crash-safe (the `DELETING`-state cluster)

The scorecard listed "unused `DELETING` stream state" as a P3 cleanup. It is not
cosmetic: it is the missing **two-phase delete**, and its absence is the root
cause of a P0 permanent-retention leak plus three secondary leaks. YB marks the
stream `DELETING` in sys-catalog first, then lets *any* leader's background task
finish cleanup idempotently -- so no RPC race and no failover window exists.

| ID | Sev | Gap | Source |
|----|-----|-----|--------|
| L1 | **P0** | `DeleteCDCStream` removes the sys-catalog row *before* firing best-effort barrier-RELEASE RPCs. Master crash in between => new master has empty (never-persisted) `cdc_barriered_tablets_`, sends no RELEASE; tablet reloads its **superblock** barrier on restart and pins WAL + MVCC history **permanently**. | 07-G1 (`catalog_manager.cc:8500,8534-8543`; `tablet_metadata.cc:407-409`) |
| L2 | P1 | Table drop while a stream is active leaves the stream `ACTIVE` in sys-catalog forever (ghost stream in `ListCDCStreams`); `DeleteTable` has zero CDC interaction. | 07-G3 (`catalog_manager.cc:2990-3089`) |
| L3 | P1 | Partial `DeleteCDCStream` + failover orphans per-tablet checkpoint rows (`LoadCDCTabletCheckpoints` reloads them; owning stream is gone) -> unbounded catalog/memory leak. | 07-G4 (`catalog_manager.cc:8500,8506-8511,8351`) |
| L4 | P3 | `barrier_seq` SET/RELEASE collision in the same wall-clock microsecond can re-pin a just-released barrier. | 07-G5 (`cdc_service.cc:2184`) |

**Highest-leverage fix in the whole report:** implement YB's two-phase
`DELETING` lifecycle (mark deleting in sys-catalog; idempotent background reap of
barriers + checkpoint rows by whichever master is leader). It closes L1, L3, L4
at once and gives L2 a target state to transition into.

Related creation-side boundary gaps (same theme, different end):

| ID | Sev | Gap | Source |
|----|-----|-----|--------|
| L5 | **P0** | `CreateCDCStream` pushes **no initial barrier**. Until the first `PersistCheckpoint` lands, every replica has `cdc_min_retained_op_index = -1`; a leader crash in that window lets Raft GC discard ops the consumer still needs -> `WAL_EXPIRED` despite a stored checkpoint. (All *later* checkpoints are safe -- CF-3.) | 03-CF-1 (`catalog_manager.cc:8429-8432`; `cdc_service.cc:1243,1250`) |
| L6 | P1 | `CreateCDCStream` validates only `table_ids_size()!=0` -- not table existence / not-deleting / visible. Streams can be created on dropped or nonexistent tables. | 07-G2 (`catalog_manager.cc:8408-8443`) |

---

## Theme 2 -- CDC is not resource-isolated from the rest of the tserver

P0-1 stopped CDC from filling the *disk*. This theme is the other half: under
load, CDC can starve *Raft* and exhaust *memory*, taking the node down for
reasons unrelated to retention. YB isolates xCluster with a dedicated RPC queue
and a MemTracker hierarchy that propagates the server-wide limit.

| ID | Sev | Gap | Source |
|----|-----|-----|--------|
| R1 | **P0** | CDC registers into the **shared** global RPC queue (`rpc_service_queue_length=50`) used by consensus/tablet services. A burst of CDC consumers fills it and begins rejecting **Raft consensus RPCs** before any CDC admission code runs. YB gives xCluster its own `xcluster_svc_queue_length=5000`. | 05-G1 |
| R2 | P1 | CDC scan admission (`TryAcquireScanSlot`) never checks `process_memory::SoftLimitExceeded()` (the tablet read path does). CDC can be admitted up to 256 MiB while the server is already shedding user reads for memory pressure -> can tip into OOM. | 05-G4 (`cdc_service.cc`; cf. `tablet_service.cc:1706`) |
| R3 | P2 | The **hot CHANGE-mode read path** builds the `replicates` vector with **no MemTracker** at all; the `cdc_scans` tracker only guards snapshot/FULL scans. YB tracks WAL-read bytes and gates under memory pressure (`Status::Busy` instead of OOM). | 01-G3 (`cdc_service.cc:1050,1123`; YB `xcluster_producer.cc:362-365`) |
| R4 | P2 | No **record-count** cap on a response -- only an 8 MiB byte cap. Narrow rows => tens of thousands of tiny `CDCRecordPB` per response, ballooning consumer allocation. YB caps at `cdc_max_stream_intent_records=1680`. | 05-G2 |
| R5 | P2 | The CHANGE-mode WAL read loop (`ReadReplicatesInRange`) has **no deadline**; YB checks the clock per entry and returns partial. Cold-disk scan can monopolize an RPC worker past the client deadline. | 01-G1 |
| R6 | P2 | The txn-span **escalation loop** (8->512 MiB doubling) has no deadline budget -> unbounded RPC-thread occupation on slow storage. | 01-G2 (`cdc_service.cc:1431-1485`) |
| R7 | P2 | Decoded `CDCRecordPB` heap is charged as the *WAL-window* size, not actual decoded size (FULL + before-image can exceed 5x). Post-emit true-up would fix. (Confirms scorecard P2-1.) | 05-G5 |
| R8 | P3 | CDC reads bypass the Raft `LogCache`, doing more disk I/O than YB after elections / with multiple streams per tablet. | 01-G5 |

R1 is the sleeper P0: it converts a CDC-consumer burst into a cluster-wide
consensus availability event.

---

## Theme 3 -- The existing P0-1 disk valve is half-open

| ID | Sev | Gap | Source |
|----|-----|-----|--------|
| V1 | P1 (degrades a P0) | When the disk-pressure valve or age ceiling fires, `GetRetentionIndexes()` sets `skip_cdc_clamp` (unblocks WAL GC) but never releases `cdc_history_floor_`. UNDO/history compaction stays blocked during the exact disk-full event the valve exists to relieve. YB keeps **separate** WAL vs history staleness clocks. Fix: also `SetCDCHistoryFloor(Timestamp(0))` when the valve fires, or add `--cdc_history_max_age_secs`. | 02-G1 (`tablet_replica.cc:932-999`; `tablet.cc:1564-1566`) |
| V2 | P1 | **Neither** P0-1 backstop (`--cdc_stop_retaining_min_disk_mb`, `--cdc_max_wal_retention_secs`) has any integration test. YB has dedicated tests that inject the condition and assert segments release. Untested safety valves fail silently. | 02-G2 |
| V3 | P2 | No per-tablet "why is the barrier still pinned" diagnostic (dead master vs non-advancing consumer vs stale checkpoint) without VLOG 4. YB emits a structured retention-factor string. | 02-G3 |
| V4 | P1 (data-safety, conditional) | Compound failure: `PersistCheckpoint` is best-effort + silent on failure; consumer gets clean SUCCESS; after a master outage > `--cdc_max_staleness_ms` (4h) the maintenance loop classifies the stream stale on the last *persisted* advance time and releases the barrier -> silently GCs WAL an actively-advancing consumer still needs. | 03-CF-2 (`cdc_service.cc:2363`; `catalog_manager.cc:8824-8834`) |

---

## Theme 4 -- Error contract + observability

Error contract (correctness for **all** consumers, not just the in-tree one):

| ID | Sev | Gap | Source |
|----|-----|-----|--------|
| E1 | P1 | `CDCErrorPB` has no machine-readable `is_retryable` / `needs_resnapshot`. Every external consumer must reverse-engineer the retry vs re-bootstrap decision from opaque codes. YB makes it transparent at the RPC layer. | 06-F05 |
| E2 | P1 | No `SCHEMA_VERSION_MISMATCH` code. A consumer attaching after an ALTER gets no signal to re-fetch schema; in-tree consumer silently falls back to hex. | 06-F06 |

Observability (extends the already-strong metric set):

| ID | Sev | Gap | Source |
|----|-----|-----|--------|
| O1 | P1 | No **byte-throughput** metric (`cdc_bytes_sent`). Only record counts -> can't see large-row storms / bandwidth saturation. | 04-#1 |
| O2 | P1 | No **time-to-expiry** gauge. Only time-since-last-poll; can't set a simple "expires in < 30m" alert. | 04-#2 |
| O3 | P2 | No GetChanges **response-size histogram** (p50/p99) and no consumer-**applied** (end-to-end) lag (needs consumer feedback). | 04-#3,#4 |

Consumer resilience (in-tree consumer is a reference => operability, not
correctness -- weighted accordingly):

| ID | Sev | Gap | Source |
|----|-----|-----|--------|
| C1 | P2 | No supervised restart / exponential backoff / auto-resnapshot; poller `return`s on terminal error. (Confirms scorecard P3-1; YB `XClusterPoller` backs off to ~65s and re-bootstraps.) | 06-F01 |
| C2 | P2 | Consumer **ignores `have_more_records`** -> backs off even when the server has data ready (needless lag). Cross-validated by two independent agents. | 06-F03 / 01-G4 |
| C3 | P3 | No jitter in backoff -> thundering herd on a recovering tserver. | 06-F02 |

Cleanups confirmed (scorecard P3-3): two hardcoded 10s master RPC timeouts
(`cdc_service.cc:2354,2507`; 05-G3); active-time reported to master up to 5 min
stale vs YB's 15s (03-CF-5); dead `NOT_AUTHORIZED` / `max_bytes_per_response`.

---

## Re-prioritized backlog (new + re-graded items only)

`production_readiness.md`'s P0-1..P1-4 remain valid and largely DONE. This is the
delta this pass adds.

### P0 -- data-safety / node-stability in normal fleets
- **L1** two-phase `DELETING` delete (permanent WAL+history leak on failover).
- **L5** initial barrier at `CreateCDCStream` (first-checkpoint WAL race).
- **R1** dedicated CDC RPC service queue (CDC burst starves Raft).

### P1 -- operate safely / avoid conditional data-safety
- **V1** release history floor when the disk/age valve fires (half-open valve).
- **V4** don't release a stream on staleness while persist has been *failing*
  (or distinguish "not advancing" from "cannot persist").
- **V2** integration tests for both P0-1 backstops.
- **R2** honor server soft-memory limit in CDC admission.
- **L6** validate table existence/state in `CreateCDCStream`; **L2** auto-mark
  streams `DELETING` on table drop; **L3** reap orphaned checkpoint rows.
- **E1** `is_retryable`/`needs_resnapshot` fields; **E2** `SCHEMA_VERSION_MISMATCH`.
- **O1** `cdc_bytes_sent`; **O2** `cdc_stream_time_to_expiry_micros`.

### P2 -- bounded-resource hardening
- **R3** MemTracker on the hot CHANGE-mode read path; **R4** record-count cap;
  **R5/R6** deadline in read + escalation loops; **R7** decoded-heap true-up.
- **O3** response-size histogram + applied-lag.
- **C1** consumer supervised restart/auto-resnapshot; **C2** consume
  `have_more_records`.

### P3 -- polish
- **L4** `barrier_seq` collision (subsumed by L1); **R8** log-cache reuse;
  **C3** backoff jitter; hardcoded master timeouts -> flag; active-time interval;
  dead-code removal.

---

## Verified SAFE / already-strong (do not spend effort here)
- DR-007 for all non-first checkpoints: superblock barrier at N-X is the real
  guarantor; at-least-once re-read window bounded by the 15s persist interval
  (03-CF-3, SAFE).
- DR-006 snapshot on leader change: `SNAPSHOT_SESSION_LOST` returned
  unconditionally; server-authoritative resume key; streaming-start op-index
  persisted bypassing the rate limiter (03-CF-4, SAFE).
- P0-1 backstops structurally correct: right WAL dir, restart-safe init
  (`cdc_barrier_prev_op_index_=-2`), monotonic guard (02 pressure-test).
- Several Kudu metrics exceed YB's (`cdc_stream_bootstrap_required` fires
  earlier; `cdc_stream_ops_behind` is a real gauge vs YB's web-UI-only
  diagnostic) (04 pressure-test).
- P1-1..P1-4 confirmed YB-grade (01, 04 pressure-tests).

---

## Detail index
- `01_producer_getchanges.md` -- read loop, deadlines, memory, log cache.
- `02_retention_barrier.md` -- P0-1 backstop pressure-test, history-floor gap.
- `03_checkpoint_state.md` -- DR-006/DR-007 crash walkthroughs, create/staleness races.
- `04_metrics_observability.md` -- full YB xrepl metric inventory diff.
- `05_admission_flags.md` -- full gflag inventory diff, RPC/memory isolation.
- `06_consumer_error_contract.md` -- poller resilience, error-contract completeness.
- `07_master_lifecycle.md` -- create/delete/drop races, `DELETING` two-phase.
