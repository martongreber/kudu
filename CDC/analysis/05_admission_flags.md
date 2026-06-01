# 05 Admission Control, Resource Bounds, and Flag Surface
# YugabyteDB CDC vs Apache Kudu CDC port -- gap analysis

Anchored to: YB branch as found in ~/yugabyte-db; Kudu branch `cdc`.
Date: 2026-08-28

---

## 1. Summary

Kudu's three-layer admission control (RPC-pool reservation, heavy-scan concurrency cap,
heap budget) is correct in design and covers the hot path.  Five material production-
safety gaps remain:

| # | Gap | Severity |
|---|-----|----------|
| G1 | CDC service shares the 50-slot global RPC queue; no `cdc_svc_queue_length` flag | HIGH |
| G2 | No record-count cap (analog to YB `cdc_max_stream_intent_records`); only byte cap | HIGH |
| G3 | Two hardcoded 10-second master RPC timeouts -- not a flag, not observable | MEDIUM |
| G4 | Server-wide soft-memory pressure not consulted before CDC scan admission | MEDIUM |
| G5 (P2-1) | Decoded CDCRecordPB heap not tracked; pre-reservation proxies WAL bytes only | MEDIUM |

Known-backlog items P2-2 (fleet caps), P0-1 (disk/time/stale backstops) are confirmed
open/done as expected and not resurveyed here.  The send-rate limiter (DR-001) and all
CDCSDK/virtual-WAL/JSON/intents flags are out of scope and omitted.

---

## 2. Full Flag Inventory

### 2a. YB CDC flags (production-relevant xCluster path only; CDCSDK-only flags noted)

| YB flag | Default | Purpose | YB file:line | Kudu analog or MISSING | Prod-shaping? severity | Status |
|---------|---------|---------|--------------|----------------------|----------------------|--------|
| `cdc_read_rpc_timeout_ms` | 30000 ms | Timeout for GetChanges RPC calls from consumer | common_flags.cc:317 | MISSING (hardcoded 10s in 2 places) | HIGH -- untunable under slow masters | New G3 |
| `cdc_write_rpc_timeout_ms` | 30000 ms | Timeout for outbound CDC write RPCs | cdc_service.cc:104 | MISSING (no Kudu write path in scope) | N/A for Kudu producer-only model | N/A |
| `cdc_state_checkpoint_update_interval_ms` | 15000 ms | Frequency of CDC state table checkpoint flush | cdc_service.cc:110 | `cdc_checkpoint_persist_interval_ms` 15s (cdc_service.cc:268) | FINE | Dup/fine |
| `update_min_cdc_indices_interval_secs` | 60 s | Frequency of UpdatePeersAndMetrics background pass | cdc_service.cc:113 | `cdc_bg_scan_interval_ms` 60000ms (catalog_manager.cc:166, master-side) | FINE -- different component | Fine |
| `update_metrics_interval_ms` | 15000 ms | CDC metrics refresh interval | cdc_service.cc:120 | no direct analog (metrics update fused into checkpoint loop) | Low | Fine |
| `enable_cdc_client_tablet_caching` | false | Cache tablet metadata in CDC client | cdc_service.cc:126 | `cdc_stream_config_cache_ttl_ms` 5min (cdc_service.cc:298) | Low | Dup/fine |
| `enable_collect_cdc_metrics` | true | Enable CDC metrics collection | cdc_service.cc:129 | no explicit toggle (always on) | Low | Fine |
| `cdc_read_safe_deadline_ratio` | 0.10 | Fraction of RPC deadline budget reserved for response serialization | cdc_service.cc:132 | `cdc_read_safe_deadline_ratio` 0.10 (cdc_service.cc:136) | FINE | Dup/fine |
| `cdc_get_changes_free_rpc_ratio` | 0.10 | Fraction of RPC worker threads kept free from GetChanges | cdc_service.cc:136 | `cdc_get_changes_free_rpc_ratio` 0.10 (cdc_service.cc:160) | FINE | Dup/fine |
| `xcluster_get_changes_max_send_rate_mbps` | 100 MB/s | Per-tserver xCluster throughput rate limiter (RocksDB RateLimiter) | cdc_service.cc:164 | MISSING (DR-001 descoped) | Descoped | Out of scope |
| `xcluster_checkpoint_max_staleness_secs` | 300 s | Max age before xCluster checkpoint is considered stale | cdc_service.cc:186 | `cdc_max_staleness_ms` 4h (catalog_manager.cc:184) | FINE -- Kudu's is more generous; both valid | Fine |
| `cdc_max_virtual_wal_per_tserver` | 5 | Max CDCSDK virtual WAL instances per tserver | cdc_service.cc:218 | CDCSDK-only; N/A | N/A | Descoped |
| `xcluster_svc_queue_length` | 5000 | Dedicated RPC service-pool queue depth for xCluster/CDC | tserver/tablet_server.cc:218 | MISSING (shares global `rpc_service_queue_length`=50) | HIGH -- queue starvation risk | New G1 |
| `cdc_max_stream_intent_records` | 1680 | Max WAL intent records per GetChanges batch (count cap) | docdb/docdb.cc:85 | MISSING -- Kudu has byte cap only | HIGH -- unbounded record cardinality | New G2 |
| `cdc_wal_retention_time_secs` | 28800 s | Global WAL retention floor for CDC-enabled tablets | consensus/log.cc:252 | `cdc_wal_retention_secs` 8h (tablet_replica.cc:87) | FINE | Dup/fine |
| `cdc_intent_retention_ms` | 28800000 ms | Intents DB retention for CDC (intents path) | consensus/log.cc:244 | CDCSDK/intents only; N/A | N/A | Descoped |
| `cdc_checkpoint_opid_interval_ms` | 60000 ms | Min opid advancement interval before checkpoint state write | consensus/consensus_queue.cc:103 | no analog (Kudu checkpoints at a configurable wall-clock interval) | Low | Fine |
| `cdc_stream_records_threshold_size_bytes` | 4 MB | Response byte threshold in xCluster/CDCSDK producer loop | cdcsdk_producer.cc:76 | `cdc_max_bytes_per_response` 8 MB (cdc_service.cc:90) | FINE -- Kudu's cap is per-response not per-producer-loop iteration, but both bound response size | Dup/fine |
| `cdc_snapshot_records_threshold_size_bytes` | 4 MB | Byte threshold for snapshot GetChanges responses | cdcsdk_producer.cc:80 | `cdc_snapshot_max_bytes_per_response` 8 MB (cdc_service.cc:220) | FINE | Dup/fine |
| `cdc_resolve_intent_lag_threshold_ms` | 300000 ms | CDCSDK intents lag threshold (CDCSDK only) | cdcsdk_producer.cc:91 | CDCSDK-only; N/A | N/A | Descoped |
| `cdcsdk_max_consistent_records` | 500 | Max records per consistent CDCSDK batch (virtual WAL, count cap) | cdcsdk_virtual_wal.cc:58 | CDCSDK-only; N/A | N/A | Descoped |
| `cdcsdk_vwal_getchanges_resp_max_size_bytes` | 4 MB | Max byte size for virtual-WAL consistent response | cdcsdk_virtual_wal.cc:65 | CDCSDK-only; N/A | N/A | Descoped |
| `cdc_enable_implicit_checkpointing` | false | Enable implicit checkpoint advancement | cdc_service.cc:212 | no analog (Kudu checkpoints are explicit) | Low | N/A |
| `cdc_enable_local_rpc_in_virtual_wal` | true | Route virtual-WAL GetChanges calls via local RPC | cdc_service.cc:215 | CDCSDK-only; N/A | N/A | Descoped |
| `cdc_state_table_num_tablets` | 0 | Tablet count for CDC state table | cdc_state_table.cc:39 | no analog (Kudu uses master catalog, not a separate table) | Arch difference | N/A |

### 2b. Kudu CDC flags (all DEFINE_ in cdc scope)

| Kudu flag | Default | Purpose | Kudu file:line | YB analog or MISSING | Notes |
|-----------|---------|---------|---------------|----------------------|-------|
| `cdc_max_bytes_per_response` | 8 MiB | Max serialized bytes per GetChanges response | cdc_service.cc:90 | `cdc_stream_records_threshold_size_bytes` 4MB | Kudu caps serialized proto bytes; YB caps WAL bytes read per producer iteration |
| `cdc_max_transaction_span_bytes` | 512 MiB | Max WAL span to read for one open transaction | cdc_service.cc:102 | no direct analog | Kudu-specific anti-wedge mechanism |
| `cdc_full_apply_wait_timeout_ms` | 30000 ms | Max wait for ops to be applied before FULL-mode read | cdc_service.cc:118 | no direct analog (YB FULL mode absent) | Kudu-specific |
| `cdc_snapshot_wait_timeout_ms` | 30000 ms | Max wait for snapshot establishment | cdc_service.cc:126 | no direct analog | Kudu-specific |
| `cdc_read_safe_deadline_ratio` | 0.10 | Fraction of client deadline reserved for response build | cdc_service.cc:136 | `cdc_read_safe_deadline_ratio` 0.10 (YB cdc_service.cc:132) | Dup/fine |
| `cdc_get_changes_free_rpc_ratio` | 0.10 | Fraction of RPC workers kept free from CDC GetChanges | cdc_service.cc:160 | `cdc_get_changes_free_rpc_ratio` 0.10 (YB cdc_service.cc:136) | Dup/fine |
| `cdc_snapshot_max_bytes_per_response` | 8 MiB | Max bytes per snapshot GetChanges page | cdc_service.cc:220 | `cdc_snapshot_records_threshold_size_bytes` 4MB | Dup/fine |
| `cdc_max_concurrent_scans` | 8 | Max concurrent heavy-scan (FULL/snapshot) GetChanges calls | cdc_service.cc:231 | no direct per-tserver scan concurrency cap | Kudu-specific; YB's semaphore is in CDCServiceImpl ctor |
| `cdc_scan_mem_limit_bytes` | 256 MiB | Heap budget for concurrent CDC scans (cdc_scans MemTracker) | cdc_service.cc:245 | no direct analog | Kudu-specific; enforced at reservation time |
| `cdc_active_time_report_interval_ms` | 300000 ms | Interval for reporting stream activity to master | cdc_service.cc:260 | `update_min_cdc_indices_interval_secs` 60s | Similar purpose; fine |
| `cdc_checkpoint_persist_interval_ms` | 15000 ms | Interval for persisting checkpoint to master | cdc_service.cc:268 | `cdc_state_checkpoint_update_interval_ms` 15s | Dup/fine |
| `cdc_stream_idle_expiry_ms` | 8h | Inactivity window before stream is considered idle/expired | cdc_service.cc:281 | `cdc_stream_expiry_ms` 8h (master catalog_manager.cc:174) | Dup/fine |
| `cdc_stream_config_cache_ttl_ms` | 5 min | TTL for cached stream config fetched from master | cdc_service.cc:298 | `enable_cdc_client_tablet_caching` (bool) | Fine |
| `cdc_enforce_access_control` | false | Require SCAN privilege for GetChanges | cdc_service.cc:307 | no analog | Kudu-specific ACL |
| `cdc_wal_retention_secs` | 8h | WAL retention floor for CDC-enabled tablets | tablet_replica.cc:87 | `cdc_wal_retention_time_secs` 8h | Dup/fine |
| `cdc_stop_retaining_min_disk_mb` | 100 MB | Force-release WAL barrier if disk below threshold | tablet_replica.cc:107 | no analog (P0-1 backstop) | DONE P0-1 |
| `cdc_max_wal_retention_secs` | 86400 s | Dead-master backstop: max WAL retention staleness | tablet_replica.cc:117 | no analog | DONE P0-1 |
| `cdc_bg_scan_interval_ms` | 60000 ms | Master CDC maintenance loop interval | master/catalog_manager.cc:166 | `update_min_cdc_indices_interval_secs` 60s | Dup/fine |
| `cdc_stream_expiry_ms` | 8h | Master-side stream inactivity expiry | master/catalog_manager.cc:174 | no YB master analog (YB handles in cdc_service) | Fine |
| `cdc_max_staleness_ms` | 4h | Max checkpoint-progress staleness before barrier release | master/catalog_manager.cc:184 | `xcluster_checkpoint_max_staleness_secs` 300s | Kudu more permissive; both valid |
| `cdc_max_barrier_releases_per_run` | 1000 | Max barrier-release RPCs per master maintenance pass | master/catalog_manager.cc:198 | no analog | Kudu-specific; fine |

---

## 3. Resource-Protection Gaps

### G1: No dedicated CDC service-pool queue (HIGH)

**Problem.** Kudu registers CDCServiceImpl with `ServerBase::RegisterService()` which
wraps every service in one `ServicePool` using `options_.service_queue_length`
(`rpc_service_queue_length`, default 50) and `options_.num_service_threads`
(`rpc_num_service_threads`, default 10).  All services -- TabletService, ConsensusService,
AdminService, TabletCopyService, and CDCService -- share these 10 threads and the
50-slot queue.

YB gives xCluster its own pool:
```
// tserver/tablet_server.cc:808
RETURN_NOT_OK(RegisterService(FLAGS_xcluster_svc_queue_length, cdc_service_));
// FLAGS_xcluster_svc_queue_length default = 5000
```

**Impact.** A burst of CDC consumers (common during catch-up) can fill the 50-slot queue.
New inbound calls -- including Raft consensus votes and heartbeats -- are then rejected
with SERVICE_TOO_BUSY.  The `cdc_get_changes_free_rpc_ratio` guard only protects against
worker-thread saturation, not queue-depth saturation.  Queue rejection happens before any
admission-control logic runs.

**Kudu-idiomatic fix.** Add a flag and register CDC with its own pool:
```cpp
// tserver/tablet_server.cc
DEFINE_NON_RUNTIME_int32(cdc_svc_queue_length, 500,
    "RPC service queue depth for the CDC service. CDC calls are long-tail "
    "and can arrive in bursts; a dedicated queue prevents CDC-induced "
    "queue saturation from rejecting Raft consensus RPCs.");
TAG_FLAG(cdc_svc_queue_length, advanced);

// in TabletServer::Init():
RETURN_NOT_OK(RegisterService(FLAGS_cdc_svc_queue_length, std::move(cdc_service)));
```

`RegisterService` in Kudu's `ServerBase` does not currently accept a queue-length
argument; it forwards to `RpcServer::RegisterService` which reads from options.  The
simplest path is to add an overload:
```cpp
// server/server_base.h / .cc
Status RegisterService(int queue_length, unique_ptr<rpc::ServiceIf> rpc_impl);
```
and wire it through `RpcServer`.

---

### G2: No record-count cap on GetChanges response (HIGH)

**Problem.** Kudu bounds response size in bytes (`cdc_max_bytes_per_response`, 8 MiB)
but does not cap the number of CDCRecordPB records per response.  For narrow rows (e.g.
single-int primary key, single-column update) a single 8 MiB WAL window can produce tens
of thousands of tiny records.  On the consumer side, deserializing a protobuf with 50 k
sub-messages allocates far more than 8 MiB.

YB's analog (`cdc_max_stream_intent_records`, 1680, docdb/docdb.cc:85 / :398-435) is
a COUNT cap applied in the intent-record scan loop, separate from the byte threshold:
```cpp
const auto max_records = FLAGS_cdc_max_stream_intent_records;
// ...
if (cur_records >= max_records) { break; }
```

**Kudu-idiomatic fix.**  Add a record-count cap and enforce it in both `ReadChanges`
and `ReadSnapshot`:
```cpp
// cdc_service.cc
DEFINE_int32(cdc_max_records_per_response, 4096,
    "Maximum number of CDCRecordPB records returned in a single GetChanges "
    "response, regardless of byte size.  Bounds consumer-side deserialization "
    "cost for narrow-row tables.  0 = unlimited.");
TAG_FLAG(cdc_max_records_per_response, advanced);
TAG_FLAG(cdc_max_records_per_response, runtime);
```

In `ReadChanges` (around the record-emission loop) and `ReadSnapshot` (around the
`resp->add_records()` block at ~cdc_service.cc:2050):
```cpp
const int32_t rec_cap = FLAGS_cdc_max_records_per_response;
// inside emit loop:
if (rec_cap > 0 && resp->records_size() >= rec_cap) {
    truncated = true;
    break;
}
```

---

### G3: Hardcoded 10-second master RPC timeouts (MEDIUM)

**Problem.** Two places in CDCServiceImpl call master RPCs with a hard-coded 10-second
timeout:

- `CDCServiceImpl::PersistCheckpoint` (cdc_service.cc:2354):
  `proxy.UpdateCDCCheckpoint(req, &resp, &rpc)` with `rpc.set_timeout(FromSeconds(10))`
- `CDCServiceImpl::GetOrFetchStreamConfig` (cdc_service.cc:2507):
  `proxy.GetCDCStreamInfo(req, &resp, &rpc)` with `rpc.set_timeout(FromSeconds(10))`

YB makes the equivalent configurable:
```
// common_flags.cc:317
DEFINE_NON_RUNTIME_int32(cdc_read_rpc_timeout_ms, 30 * 1000, ...);
// cdc_service.cc:2193 uses FLAGS_cdc_read_rpc_timeout_ms
```

**Impact.** Under a GC pause, network hiccup, or leader election on the master, both
calls time out after exactly 10 seconds with no operator knob to adjust.  Operators
cannot distinguish a slow master from a correctly-rejected call.

**Kudu-idiomatic fix.**
```cpp
// cdc_service.cc
DEFINE_int32(cdc_master_rpc_timeout_ms, 30 * 1000,
    "Timeout in milliseconds for master RPCs made by the CDC service "
    "(UpdateCDCCheckpoint and GetCDCStreamInfo).  Increase under high master "
    "load or cross-datacenter deployments.");
TAG_FLAG(cdc_master_rpc_timeout_ms, advanced);
TAG_FLAG(cdc_master_rpc_timeout_ms, runtime);
```

Replace the two hard-coded calls:
```cpp
rpc.set_timeout(MonoDelta::FromMilliseconds(FLAGS_cdc_master_rpc_timeout_ms));
```

---

### G4: Server soft-memory pressure not consulted at CDC scan admission (MEDIUM)

**Problem.** `TryAcquireScanSlot` (cdc_service.cc:834) enforces:
1. concurrency cap (`cdc_max_concurrent_scans`)
2. CDC-local budget (`cdc_scan_mem_limit_bytes` vs `cdc_scans` MemTracker)

Neither check asks whether the *whole server* is under memory pressure.
`process_memory::SoftLimitExceeded()` is called in the tablet service read path
(tablet_service.cc:1706) before user scans, but the CDC path bypasses that gate.

In YB, the per-tablet MemTracker hierarchy naturally propagates: the CDC tracker hangs
under the tablet tracker (`cdc_service.cc:684: tablet_ptr->mem_tracker()`), which hangs
under the server root, so a server-wide hard limit kills the CDC tracker's Consume() call
and triggers server-wide back-pressure.  Kudu's `cdc_scans` tracker hangs directly under
`server_->mem_tracker()` (cdc_service.cc:604-605) but has no hard limit (`-1`) and the
`cdc_scan_mem_limit_bytes` cap is checked only against `cdc_scans` consumption, not
against overall server pressure.

**Impact.** CDC scans continue admitting up to `cdc_scan_mem_limit_bytes` (default
256 MiB) even when the server is already above its soft-memory-limit threshold.  This can
tip the server into OOM while the tablet service is already shedding user reads.

**Kudu-idiomatic fix.**  In `TryAcquireScanSlot`, add a server-pressure check before the
CDC-local budget check:
```cpp
// After concurrency check, before mem-budget check:
double capacity_pct;
if (process_memory::SoftLimitExceeded(&capacity_pct)) {
    active_scans_.fetch_sub(1, std::memory_order_acq_rel);
    return Status::ServiceUnavailable(Substitute(
        "server memory soft limit exceeded ($0%); CDC scan deferred", capacity_pct));
}
```

---

### G5 (P2-1): Decoded CDCRecordPB heap not tracked (MEDIUM)

**Problem.** `TryAcquireScanSlot(reserve_bytes)` pre-reserves `max_bytes` (the WAL scan
window or snapshot page size) against the `cdc_scans` MemTracker.  This is a PROXY for
actual heap use, not actual tracking.  Decoded `CDCRecordPB` objects (built by
`PopulateReadRecord` / the WAL decode path) may be substantially larger than the raw WAL
bytes read -- for FULL streams with before-image reconstruction, the ratio can exceed 5x.
No `ScopedTrackedConsumption` or equivalent is applied to the response proto accumulator.

YB's xcluster_producer uses `ScopedTrackedConsumption` for the actual WAL read bytes
(`xcluster_producer.cc:362-364`):
```cpp
consumption = ScopedTrackedConsumption(context.mem_tracker, read_ops.read_from_disk_size);
```
This still does not track decoded proto heap, but it is RAII and exact on the raw bytes.
Kudu's model pre-reserves and then releases on the same `max_bytes` estimate regardless
of whether the actual read was smaller.

**Status.** Confirmed open.  This is the P2-1 backlog item.  A full fix requires
tracking the serialized response proto size (resp->ByteSizeLong() after the emit loop)
or adding a `ScopedTrackedConsumption`-style wrapper in ReadChanges / ReadSnapshot.

**Sketch.**
```cpp
// After the emit loop in ReadChanges/ReadSnapshot, before responding:
// Replace the pre-reservation with an exact post-facto reservation:
// 1. At TryAcquireScanSlot: reserve max_bytes (existing behavior, unchanged).
// 2. After response built: compute actual_bytes = resp->ByteSizeLong().
//    If actual_bytes < reserve_bytes: Release(reserve_bytes - actual_bytes).
//    This trues-up the tracker without adding a second Consume path.
```
This keeps the current lock-free admit path and only adds a correction on the release side.

---

### G6 (P2-2): No fleet-level stream or barriered-tablet cap (MEDIUM, known)

**Problem.** Neither Kudu nor YB has a per-tserver cap on active CDC streams or
barriered tablets.  The Kudu `cdc_max_streams` / `cdc_max_barriered_tablets_per_server`
flags do not exist.  A misconfigured deployment can create O(tablets * streams) retention
barriers with no bound.

**Status.** Confirmed open as P2-2.

**Sketch.**
```cpp
// master/catalog_manager.cc (enforced at CreateCDCStream and at
// barrier-set time in the maintenance loop)
DEFINE_int32(cdc_max_streams, 0,
    "Maximum number of active CDC streams per cluster.  0 = unlimited.");
DEFINE_int32(cdc_max_barriered_tablets_per_server, 0,
    "Maximum number of tablets with an active CDC retention barrier "
    "on a single tserver.  0 = unlimited.");
```

---

## 4. What is FINE

The following areas were audited and require no action:

| Area | Kudu | YB | Verdict |
|------|------|-----|---------|
| RPC-worker reservation | `cdc_get_changes_free_rpc_ratio`=0.10, lock-free atomic, floor=1 (cdc_service.cc:960-975) | `cdc_get_changes_free_rpc_ratio`=0.10, blocking Semaphore (cdc_service.cc:1626-1629) | Both server-global; not per-tablet, not per-stream. Kudu's atomic is correct; YB's Semaphore provides blocking rather than shedding. Kudu's shed-and-retry is appropriate for the Kudu consumer model. FINE. |
| Heavy-scan concurrency cap | `cdc_max_concurrent_scans`=8, lock-free atomic (cdc_service.cc:834-848) | No direct per-tserver scan semaphore (YB scans run inline) | Kudu-specific; FINE. |
| Heap budget | `cdc_scan_mem_limit_bytes`=256 MiB, `cdc_scans` MemTracker child of server root (cdc_service.cc:604) | Per-tablet MemTracker hierarchy | Both bound CDC heap; Kudu's is a single flat cap vs YB's hierarchical. Kudu's approach is simpler and sufficient with G4 fixed. |
| Response byte cap | `cdc_max_bytes_per_response`=8 MiB (cdc_service.cc:90) | `cdc_stream_records_threshold_size_bytes`=4 MB | Kudu's cap is larger but applied per-response; YB's is applied per-iteration in producer loop. Both bound response size. G2 adds the missing count cap. |
| WAL retention floor | `cdc_wal_retention_secs`=8h (tablet_replica.cc:87) | `cdc_wal_retention_time_secs`=8h | Identical semantics and defaults. FINE. |
| Dead-master WAL backstops | `cdc_stop_retaining_min_disk_mb`, `cdc_max_wal_retention_secs` (tablet_replica.cc:107, 117) | no analog | P0-1 DONE in Kudu; YB has no equivalent. Kudu is AHEAD here. |
| Checkpoint-progress staleness | `cdc_max_staleness_ms`=4h (catalog_manager.cc:184) | `xcluster_checkpoint_max_staleness_secs`=300s | Both protect against stalled consumers pinning WAL. Kudu's window is 48x longer; that may be intentional for a write-once analytics use case. Acceptable. |
| Safe-deadline ratio | `cdc_read_safe_deadline_ratio`=0.10 (cdc_service.cc:136) | `cdc_read_safe_deadline_ratio`=0.10 | Identical. FINE. |
| Transaction-span anti-wedge | `cdc_max_transaction_span_bytes`=512 MiB (cdc_service.cc:102) | no analog | Kudu-specific; FINE. |
| Stream-config cache | `cdc_stream_config_cache_ttl_ms`=5 min (cdc_service.cc:298) | `enable_cdc_client_tablet_caching` | Analogous; FINE. |
| Access control gate | `cdc_enforce_access_control` (cdc_service.cc:307) | no analog | Kudu ACL; FINE. |
| Barrier-release throttle | `cdc_max_barrier_releases_per_run`=1000 (catalog_manager.cc:198) | no analog | Kudu-specific; FINE. |
