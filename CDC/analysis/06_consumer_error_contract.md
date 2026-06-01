# CDC Consumer Resilience and Error Contract Analysis

**Branch:** cdc  
**Date:** 2026-08-28  
**Subsystem:** consumer/poller resilience + error taxonomy / consumer contract

---

## 1. Summary

The Kudu CDC in-tree consumer (CDCTabletPoller / CDCConsumer) is structurally sound and
handles the common transient failure modes (leader failover, server overload, snapshot
expiry detection). The main gaps fall into two buckets:

**Operability / resilience (in-tree consumer):**
- No supervised restart or auto-resnapshot after WAL/HISTORY/STREAM_EXPIRED: pollers stop
  permanently and the only recovery path is operator intervention. Confirmed P3-1.
- No jitter in the exponential backoff: 30+ pollers hitting a failed server in lockstep.
- `have_more_records` response signal is produced by the server but not consumed by the
  poll loop, causing unnecessary idle backoff on high-throughput streams.
- Checkpoint on leader-change: if the leader changes inside the checkpoint window,
  MaybeCheckpoint silently skips (uninitialized leader_ guard), leaving durable progress
  lagging behind delivered progress.

**Error contract (correctness for ALL consumers):**
- No explicit retryable/fatal signal in CDCErrorPB: external consumers must re-implement
  ClassifyCdcError or risk retrying fatal errors forever, or abandoning retryable ones.
- No SCHEMA_VERSION_MISMATCH code: a consumer attaching mid-stream after an ALTER
  receives no machine-readable signal to re-fetch the schema; it silently decodes against
  a stale layout.
- need_schema_info flag has no server-side enforcement: the server cannot tell a consumer
  it attached with a stale schema.

Hardcoded 10-second master RPC timeouts in the tserver-side checkpoint and config-fetch
paths remain as P3-3 cleanups (confirmed).

---

## 2. Findings Table

| # | Gap | Severity | YB anchor | Kudu status | Why prod-shaping | Kudu sketch | New/Dup |
|---|-----|----------|-----------|-------------|------------------|-------------|---------|
| F-01 | No supervised poller restart + auto-resnapshot after expiry | P2 | xcluster_poller.cc:603-606 (apply_failures capped at replication_failure_delay_exponent, re-bootstrap via IsBootstrapRequired) | cdc_consumer.cc:276-281 (kResnapshot/kFatal both call return, no restart) | Tablets drained by WAL GC or HISTORY GC go permanently dark; entire table lag grows unbounded without operator action | CDCConsumer::Start() loop: after pollers_.emplace_back, wrap in a supervisor that watches needs_resnapshot and re-creates the poller with do_snapshot=true | DUPLICATE P3-1 |
| F-02 | No jitter in exponential backoff | P2 | xcluster_poller.cc:318-320 (1 << poll_failures_, no jitter); xcluster_poller.h:187 (poll_failures_ uint32) | cdc_consumer.cc:210-218 (GrowBackoff: x2, no jitter); min 200ms, max 2s | On a 30-tablet table all pollers hitting the same failed tserver synchronize retries at 2s intervals, creating a thundering herd that delays recovery | Add jitter: backoff_ = base + random(0, base) before capping at max_poll_backoff | NEW |
| F-03 | have_more_records not consumed by poll loop | P2 | xcluster_poller.cc:317-318 (poll_failures_ increment on any error) -- YB has no equivalent flag; it re-polls immediately on non-zero record count | cdc_consumer.cc:443-453, 260-274 (got_records = resp.records_size() > 0; does NOT check resp.have_more_records()) | Server sets have_more_records=true when byte budget was hit mid-WAL; not checking it means the consumer imposes idle backoff even when records are immediately available, degrading throughput under sustained write load | In PollOnce, after setting *got_records = true, also set a local bool from resp.have_more_records() and propagate to Run(); skip SleepFor when true | NEW |
| F-04 | Durable checkpoint silently skips on leader change | P2 | xcluster_poller.cc: output_client ApplyChanges writes checkpoint to cdc_state atomically with apply; no silent-skip path | cdc_consumer.cc:484-486 (MaybeCheckpoint: if (!leader_.Initialized()) { return; }) | If leader changes inside the 10-second checkpoint window, delivered-but-not-checkpointed records accumulate. On crash, consumer reprocesses more work than expected. For long-running streams this is a WAL anchor leakage risk | In MaybeCheckpoint, if leader_ is uninitialized after ResolveLeader fails, retry up to N times with backoff before giving up, and log a warning | NEW |
| F-05 | Error contract lacks explicit retryable/fatal classification | P1 (correctness) | xcluster_rpc.cc:296-329 (GetChangesRpc::response_error maps CDC codes to TabletServerErrorPB: TABLET_NOT_FOUND -> NOT_THE_LEADER or TABLET_NOT_FOUND; LEADER_NOT_READY -> LEADER_NOT_READY_TO_SERVE; TabletInvoker handles the rest transparently); cdc_error.h: StatusErrorCodeImpl | cdc.proto:33-88 (12 codes, no is_retryable field); cdc_consumer.cc:351-395 (ClassifyCdcError is internal, not published) | External consumers that implement GetChanges have no machine-readable way to know whether a code means "back off and retry" vs "stop and re-bootstrap" vs "stop and page operator". They must hard-code a switch identical to ClassifyCdcError or produce incorrect behavior on novel error codes | Add optional bool is_retryable = 3 to CDCErrorPB. Populate it in SetCDCError helpers. Emit true for TABLET_NOT_LEADER/TABLET_NOT_RUNNING/SERVER_TOO_BUSY/NOT_AUTHORIZED, false for WAL_EXPIRED/HISTORY_EXPIRED/STREAM_EXPIRED/TRANSACTION_TOO_LARGE/STREAM_NOT_FOUND, resnapshot-flagged for WAL/HISTORY/STREAM_EXPIRED | NEW |
| F-06 | No SCHEMA_VERSION_MISMATCH error code | P1 (correctness) | YB: xcluster_poller.cc:239-250 (UpdateSchemaVersions pushed via consumer heartbeat); ProcessGetChangesResponseError:541-555 (AUTO_FLAGS_CONFIG_VERSION_MISMATCH triggers version negotiation) | cdc.proto:33-88 (12 codes, none for schema staleness); cdc_consumer.cc:796-834 (DecodeRecord silently decodes against potentially stale schema; column not found falls back to hex) | A consumer that attaches mid-stream after an ALTER, forgets need_schema_info=true, and does not process DDL records in-band will silently decode new columns as hex blobs. There is no server-side error to prompt a schema refresh. | Add CDCErrorPB::SCHEMA_VERSION_MISMATCH = 13. In ReadChanges, if req.schema_version() is set and is less than the tablet's current schema_version, return this error. Consumers can then re-issue with need_schema_info=true | NEW |
| F-07 | NOT_AUTHORIZED path effectively untriggered by default | P3 | n/a (YB uses different authz model) | cdc_service.cc:307-313 (cdc_enforce_access_control default false); cdc_consumer.cc:370-376 (NOT_AUTHORIZED handler force-refreshes token then retries) | With cdc_enforce_access_control=false (the default), the server never returns NOT_AUTHORIZED, so the consumer token-refresh path is never exercised. If an operator enables the flag in production, this untested path is the only defence against auth-token expiry | Integration test: create a stream with access control enabled, let the authz token expire (mock the verifier), verify the consumer survives | DUPLICATE P3-3 (dead path note) |
| F-08 | DELETING stream state defined but never persisted | P3 | n/a | cdc.proto:140-143 (DELETING = 1); catalog_manager.cc:8619 ("Delete is single-phase today, so DELETING is not yet persisted; this guard is forward-looking") | The GetCDCStreamInfo check (catalog_manager.cc:8621) that rejects non-ACTIVE streams is dead for the DELETING branch: no code path sets the state. If a two-phase delete is ever implemented without also updating the CDCTabletPoller, pollers will silently stop receiving STREAM_NOT_FOUND and instead hang until WAL expires | Note is already in the code comment. No action required until two-phase delete is implemented. | DUPLICATE P3-3 (unused state note) |
| F-09 | Hardcoded 10-second master RPC timeouts in tserver | P3 | n/a | cdc_service.cc:2354 (UpdateCDCCheckpoint to master), cdc_service.cc:2507 (GetCDCStreamInfo from master) | In cloud environments with >10s master latency (leader election, GC pause) checkpoint RPCs silently fail; stream lag metrics go stale without surfacing the cause | Replace with a gflag: DEFINE_int32(cdc_master_rpc_timeout_ms, 10000, ...) and use it in both callsites | DUPLICATE P3-3 (hardcoded 10s timeout) |
| F-10 | Poll backoff max too small relative to YB; no idle-count threshold | P3 | xcluster_poller.cc:47-55 (async_replication_idle_delay_ms=100ms, max_idle_wait=3 polls before full idle delay; replication_failure_delay_exponent=16 -> max ~65s) | cdc_consumer.h:133-134 (min 200ms, max 2s); cdc_consumer.cc:267-270 (any empty batch triggers GrowBackoff; no idle count threshold) | On a quiet stream the poller ramps to max 2s quickly. On a busy stream recovering from a transient failure, 2s cap means the consumer is pounding a recovering server 500 times/minute across all pollers. YB's 65s cap is more durable. | Raise default max_poll_backoff to 30s; add an async_replication_max_idle_wait equivalent (idle counter before applying max delay) to distinguish "stream is quiet" from "failure backoff" | NEW |

---

## 3. ERROR CONTRACT Completeness (Correctness for All Consumers)

This section covers correctness gaps that affect every consumer implementation, not just the
in-tree reference consumer.

### 3.1 Kudu 12-code taxonomy vs YB 14-code taxonomy

**Kudu CDCErrorPB codes (cdc.proto:34-88):**
```
UNKNOWN_ERROR=1, STREAM_NOT_FOUND=2, TABLET_NOT_FOUND=3, TABLET_NOT_LEADER=4,
WAL_EXPIRED=5, TABLET_NOT_RUNNING=6, HISTORY_EXPIRED=7, STREAM_EXPIRED=8,
NOT_AUTHORIZED=9, SERVER_TOO_BUSY=10, SNAPSHOT_SESSION_LOST=11, TRANSACTION_TOO_LARGE=12
```

**YB CDCErrorPB codes (yb/cdc/cdc_service.proto:83-103):**
```
UNKNOWN_ERROR=1, TABLET_NOT_FOUND=2, TABLE_NOT_FOUND=3, SUBSCRIBER_NOT_FOUND=4,
CHECKPOINT_TOO_OLD=5, TABLET_NOT_RUNNING=6, NOT_LEADER=7(deprecated), NOT_RUNNING=8,
INTERNAL_ERROR=9, INVALID_REQUEST=10, LEADER_NOT_READY=11, TABLET_SPLIT=12,
OPERATION_DISALLOWED=13, AUTO_FLAGS_CONFIG_VERSION_MISMATCH=14
```

### 3.2 Conditions Kudu can express that YB cannot
These are Kudu's intentional improvements over YB's design:
- WAL_EXPIRED (5): distinct from CHECKPOINT_TOO_OLD; WAL segment was GC'd
- HISTORY_EXPIRED (7): MVCC history barrier missed; before-image reconstruction fails
- STREAM_EXPIRED (8): stream idle beyond cdc_stream_expiry_ms
- SNAPSHOT_SESSION_LOST (11): in-memory snapshot session discarded on leader change
- TRANSACTION_TOO_LARGE (12): operator must raise cdc_max_transaction_span_bytes

The WAL_EXPIRED / STREAM_EXPIRED / HISTORY_EXPIRED three-way split is better than YB's
single CHECKPOINT_TOO_OLD because it lets the consumer (and operator) distinguish:
"GC happened" (WAL_EXPIRED) from "stream went idle" (STREAM_EXPIRED) from "history barrier
was not set in time" (HISTORY_EXPIRED).

### 3.3 Conditions YB can express that Kudu cannot (gaps)

**Gap A: SCHEMA_VERSION_MISMATCH (missing, severity P1)**

YB pushes schema version maps to each poller via `XClusterConsumer::UpdateSchemaVersions`
(xcluster_consumer.cc; xcluster_poller.cc:239-250) and raises
`AUTO_FLAGS_CONFIG_VERSION_MISMATCH` on auto-flag drift. Kudu has no equivalent server-side
code to signal "you are decoding against the wrong schema version".

A consumer that:
  (a) attaches after an ALTER without setting need_schema_info=true, AND
  (b) does not process DDL op records in-band

will receive records with schema_version N+1 but decode them against schema version N.
The consumer silently falls back to hex for unknown column names
(cdc_consumer.cc:86-92, DecodeColumn) without any server-side error.

The server knows the current schema_version per tablet. If a request carries
req.schema_version and it does not match the current tablet schema_version, the server
should return CDCErrorPB::SCHEMA_VERSION_MISMATCH so external consumers can re-issue
with need_schema_info=true.

**Gap B: No explicit retryable vs fatal classification (missing, severity P1)**

Every consumer that calls GetChanges must implement a switch over all 12 error codes to
decide: retry with backoff / resolve leader and retry / resnapshot / fatal. This
classification is currently internal to CDCTabletPoller::ClassifyCdcError
(cdc_consumer.cc:351-395) and is not part of the published contract.

Adding `optional bool is_retryable = 3` and `optional bool needs_resnapshot = 4` to
CDCErrorPB would let any consumer make the correct decision without re-implementing the
taxonomy. YB achieves this differently: the TabletInvoker in xcluster_rpc.cc:277-329
handles leader/not-found retries transparently, and CHECKPOINT_TOO_OLD sets
REPLICATION_MISSING_OP_ID (xcluster_poller.cc:558-560) which triggers external
re-bootstrap via the consumer registry. Kudu has no equivalent signalling path to the
consumer supervisor.

**Gap C: TABLET_SPLIT (YB code 12, not applicable to current Kudu)**

YB emits TABLET_SPLIT when a consumer's checkpoint points at a tablet that was split
into children. Kudu does not perform online tablet splits, so this gap is not currently
material. If Kudu ever adds online tablet splitting, a corresponding error code and
poller-level handling (discover split children, re-register pollers) will be needed.

### 3.4 Contract documentation gap

The CDCErrorPB comment block in cdc.proto (lines 33-88) is the primary specification for
all consumers. While the comments for each code are precise, there is no machine-readable
or documented classification of which codes are:
  - retry-with-backoff (TABLET_NOT_LEADER, TABLET_NOT_RUNNING, SERVER_TOO_BUSY, NOT_AUTHORIZED)
  - resolve-leader-and-retry (TABLET_NOT_LEADER, TABLET_NOT_FOUND -- these two currently
    collapse into the same kRetry + leader reset in ClassifyCdcError:355-367)
  - resnapshot-required (WAL_EXPIRED, HISTORY_EXPIRED, STREAM_EXPIRED)
  - fatal-operator-action (TRANSACTION_TOO_LARGE, SNAPSHOT_SESSION_LOST, STREAM_NOT_FOUND,
    UNKNOWN_ERROR by default)

A comment block enumerating this classification inside the CDCErrorPB definition, plus
the is_retryable / needs_resnapshot fields (Gap B above), would make the contract
self-describing for external consumers.

---

## 4. What Is Fine

- **Leader failover handling**: ClassifyCdcError correctly resets leader_ to uninitialized
  on TABLET_NOT_LEADER/TABLET_NOT_FOUND/TABLET_NOT_RUNNING, triggering ResolveLeaderWithRetry
  on the next PollOnce call (cdc_consumer.cc:355-367, 400-403). The consumer does not die
  on leader elections.

- **Snapshot protocol resilience**: SNAPSHOT_SESSION_LOST is correctly mapped to kRetry
  (via the default branch, code 11) -- wait, actually checking ClassifyCdcError: code 11
  falls into the `default:` branch which returns kFatal (cdc_consumer.cc:388-394).
  The correct behavior is to restart the snapshot, not to fatal. This is F-01 territory
  (no auto-resnapshot). For now, the snapshot phase re-issues with force=false on any
  non-kRetry CDCError from the snapshot loop (cdc_consumer.cc:317-323). Actually
  SNAPSHOT_SESSION_LOST in the snapshot phase hits ClassifyCdcError which returns kFatal,
  and the snapshot loop returns StatusFromPB(resp.error().status()), causing RunSnapshotPhase
  to fail and the poller to stop. This is correct-ish (the operator must restart, and the
  restart re-issues is_snapshot_start=true), but it is subsumed by F-01.

- **WAL/HISTORY/STREAM disambiguation**: The three expiry codes correctly set
  needs_resnapshot=true and return kResnapshot, recorded in progress for introspection.

- **At-least-once checkpoint semantics**: from_op_ (read cursor) advances on each successful
  GetChanges, but the durable checkpoint (Checkpoint() RPC) persists periodically. On
  restart, Start() re-reads tablet_checkpoints from the server (cdc_consumer.cc:577-578),
  so re-delivery covers at most one checkpoint_interval worth of records. Correct.

- **Authz token refresh on NOT_AUTHORIZED**: when returned, the consumer force-fetches a
  fresh token and retries (cdc_consumer.cc:372-376). Correct for secured deployments.

- **DDL schema evolution in-band**: DecodeAndDeliver applies schema updates from DDL
  records in-order (cdc_consumer.cc:753-763), ensuring subsequent row records in the same
  batch decode against the updated schema. Unknown columns fall back to hex rather than
  failing hard (cdc_consumer.cc:86-92).

- **max_bytes_per_response as per-request override**: CDCConsumer::Options.max_bytes_per_response
  (cdc_consumer.h:138) IS live; it overrides the per-stream default in every GetChanges
  request (cdc_consumer.cc:299-301, 424-425, 686-688). Default 0 = defer to stream config.
  Not dead code. The backlog description of this as "dead" appears to be stale.

- **Checkpoint interval is configurable**: CDCConsumer::Options.checkpoint_interval defaults
  to 10s (cdc_consumer.h:130) and is passed by the caller, not hardcoded inside the poller.
  The 10s hardcoded values in cdc_service.cc are for the server-side master RPCs (P3-3),
  not the per-tablet checkpoint interval.

- **SERVER_TOO_BUSY backoff**: correctly returns kRetry with backoff growth, protecting
  the server from immediate retry storms (cdc_consumer.cc:365-369).

---

## File / Line Reference Index

| Symbol | File | Lines |
|--------|------|-------|
| CDCTabletPoller::ClassifyCdcError | kudu/cdc/cdc_consumer.cc | 351-395 |
| CDCTabletPoller::Run poll loop | kudu/cdc/cdc_consumer.cc | 240-285 |
| CDCTabletPoller::GrowBackoff / ResetBackoff | kudu/cdc/cdc_consumer.cc | 210-218 |
| CDCTabletPoller::PollOnce | kudu/cdc/cdc_consumer.cc | 398-465 |
| CDCTabletPoller::MaybeCheckpoint | kudu/cdc/cdc_consumer.cc | 467-515 |
| CDCConsumer::Options.have_more_records (absent) | kudu/cdc/cdc_consumer.h | 125-139 |
| CDCErrorPB 12-code enum | kudu/cdc/cdc.proto | 33-88 |
| GetChangesResponsePB.have_more_records | kudu/cdc/cdc.proto | 332 |
| SysCDCStreamEntryPB.DELETING forward comment | kudu/master/catalog_manager.cc | 8619 |
| Hardcoded 10s master RPC timeout (checkpoint) | kudu/cdc/cdc_service.cc | 2354 |
| Hardcoded 10s master RPC timeout (config fetch) | kudu/cdc/cdc_service.cc | 2507 |
| XClusterPoller exponential backoff | yb/tserver/xcluster_poller.cc | 311-322 |
| XClusterPoller replication_failure_delay_exponent | yb/tserver/xcluster_poller.cc | 54-56 |
| XClusterPoller IncrementPollFailures | yb/tserver/xcluster_poller.cc | 855-860 |
| YB CDC error code enum | yb/cdc/cdc_service.proto | 83-103 |
| YB GetChangesRpc error->TabletServerError mapping | yb/cdc/xcluster_rpc.cc | 296-329 |
| YB schema version update in poller | yb/tserver/xcluster_poller.cc | 239-250 |
| YB CHECKPOINT_TOO_OLD -> REPLICATION_MISSING_OP_ID | yb/tserver/xcluster_poller.cc | 558-560 |
