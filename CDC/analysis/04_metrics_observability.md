# CDC Metrics & Observability: YugabyteDB vs Apache Kudu Port

**Date:** 2026-08-28
**Branch:** cdc
**Scope:** xrepl/xCluster producer-side metrics only. Consumer-side YB metrics
(xcluster_consumer_*) are N/A for Kudu's current producer-only implementation.
CDCSDK-logical-only metrics (replication slots, virtual WAL) are flagged where
they overlap with Kudu's per-tablet model.

---

## 1. Summary

YB defines **25 named metrics** in xrepl_metrics.cc and xcluster_consumer.cc
relevant to the CDC/xCluster producer path. Of these:

- **7 map cleanly** to already-added Kudu metrics (see "Already Added" list).
- **3 are CDCSDK-slot-only** (cdcsdk_flush_lag, cdcsdk-specific entity metrics
  on the `cdcsdk` entity) or **consumer-only** (xcluster_consumer_*) with no
  meaning in Kudu's producer model.
- **15 are missing** from Kudu, of which **4 are production-shaping** (HIGH/MEDIUM-HIGH
  severity), **5 are MEDIUM**, and **6 are LOW**.

Critical gaps:

1. **Byte throughput (HIGH)**: Kudu has record counts but zero byte-throughput
   metrics. No way to detect bandwidth issues, large-row storms, or throughput
   regressions.
2. **Stream expiry remaining (HIGH)**: cdcsdk_expiry_time_ms gives a direct
   "stream expires in N ms" gauge. Kudu's cdc_stream_active_age_micros shows
   how long since last poll but requires the operator to mentally subtract from
   the expiry threshold to derive the urgency.
3. **Committed/consumer-applied lag (MEDIUM-HIGH)**: async_replication_committed_lag_micros
   is the end-to-end lag as seen by the consuming application. Kudu only measures
   the producer-sent lag, not whether the consumer has applied the records.
4. **RPC payload histogram (MEDIUM)**: rpc_payload_bytes_responded is an
   event_stats histogram exposing p50/p99 payload sizes per GetChanges. Kudu has
   no equivalent.

Additionally, Kudu's already-added metrics are **correct** in what they measure.
Two subtle semantic differences are noted in section 4.

---

## 2. Full Inventory Table

Legend:
  YB file:line references the definition in xrepl_metrics.cc unless stated.
  "MAPPED" = Kudu has an equivalent metric (may differ in entity or granularity).
  "MISSING" = no analog defined in Kudu.
  "N/A" = not applicable to Kudu's per-tablet producer model.
  Severity: HIGH / MED-HIGH / MED / LOW / N/A

### 2a. XClusterTabletMetrics (entity: xcluster, per-tablet per-stream)

| YB metric name | Type | Entity | What it measures | YB file:line | Kudu analog | Prod-shaping? | Status |
|---|---|---|---|---|---|---|---|
| rpc_payload_bytes_responded | event_stats (histogram) | xcluster | Payload bytes of GetChanges responses (only non-heartbeat). Histogram gives p50/p99 per stream per tablet. | xrepl_metrics.cc:40 | MISSING | MED: detects large-row traffic, BW saturation | MISSING |
| rpc_heartbeats_responded | counter | xcluster | GetChanges responses with empty payload (no new records) | xrepl_metrics.cc:44 | MISSING | LOW: ratio with rpc_payload helps diagnose idle-poll vs data-delivering polls | MISSING |
| last_read_opid_term | gauge_int64 | xcluster | Term component of last read OpId | xrepl_metrics.cc:48 | MISSING | LOW: term changes track leader election; not directly actionable | MISSING |
| last_read_opid_index | gauge_int64 | xcluster | Index of last producer op read by consumer | xrepl_metrics.cc:54 | MISSING (derived: cdc_stream_ops_behind = last_readable - checkpoint) | LOW: redundant with ops_behind | MISSING |
| last_checkpoint_opid_index | gauge_int64 | xcluster | Consumer's last checkpointed op index | xrepl_metrics.cc:60 | MISSING (used internally in cdc_stream_ops_behind computation) | LOW: redundant with ops_behind | MISSING |
| last_read_hybridtime | gauge_uint64 | xcluster | HybridTime of last record read | xrepl_metrics.cc:67 | MISSING (HybridTime not used in Kudu) | LOW: Kudu uses physical time only | N/A (hybrid-clock specific) |
| last_read_physicaltime | gauge_uint64 | xcluster | Physical timestamp (us) of last record sent to consumer | xrepl_metrics.cc:72 | MISSING as direct metric (internally: CDCStreamTabletState::last_sent_record_phys_micros, cdc_service.h:123) | MED: raw timestamp is useful for debugging; lag is derived from it but the timestamp itself is not exposed | MISSING |
| last_checkpoint_physicaltime | gauge_uint64 | xcluster | Physical time of last consumer-committed op | xrepl_metrics.cc:77 | MISSING | MED: consumer-committed time; basis for committed lag | MISSING |
| last_readable_opid_index | gauge_int64 | xcluster | Last op index that GetChanges COULD read (producer head) | xrepl_metrics.cc:83 | MISSING (used internally as last_known_up_to_op_index, cdc_service.h:128) | LOW: redundant with ops_behind | MISSING |
| async_replication_sent_lag_micros | gauge_int64 | xcluster | now - physical_time_of_last_record_sent (producer sent lag) | xrepl_metrics.cc:88 | cdc_stream_sent_lag_micros (per-stream) + cdc_max_sent_lag_micros (server) | HIGH: primary lag signal -- MAPPED | Already added |
| async_replication_committed_lag_micros | gauge_int64 | xcluster | now - physical_time_of_last_record_applied_on_consumer (end-to-end lag; sourced from cdc_state.last_replication_time) | xrepl_metrics.cc:94 | MISSING | MED-HIGH: end-to-end lag requires consumer feedback; without it operators cannot distinguish "sent but consumer not applying" from "consumer keeping up" | MISSING |
| is_bootstrap_required | gauge_bool | xcluster | Whether consumer has fallen off the WAL and must re-bootstrap | xrepl_metrics.cc:99 | cdc_stream_bootstrap_required (per-stream) + cdc_bootstrap_required_streams (server count) | HIGH -- MAPPED | Already added |
| last_getchanges_time | gauge_uint64 | xcluster | Epoch-us of last GetChanges received | xrepl_metrics.cc:104 | MISSING as direct metric (drives cdc_stream_active_age_micros internally) | LOW: age metric is more useful | MISSING |
| time_since_last_getchanges | gauge_int64 | xcluster | Microseconds since last GetChanges | xrepl_metrics.cc:110 | cdc_stream_active_age_micros (per-stream) + cdc_max_active_age_micros (server) -- note semantic diff in sec 4 | HIGH -- MAPPED (with note) | Already added |
| last_caughtup_physicaltime | gauge_uint64 | xcluster | Physical time at which consumer last was fully caught up (set on heartbeat/empty response) | xrepl_metrics.cc:117 | MISSING | MED: detects intermittent lag vs sustained lag; a stream can have high sent_lag but still occasionally catch up | MISSING |

### 2b. CDCSDKTabletMetrics (entity: cdcsdk, per-tablet)
Note: cdcsdk_sent_lag_micros and cdcsdk_change_event_count exist in CDCSDK but
have meaningful per-tablet equivalents in Kudu's xCluster-style model.
cdcsdk_flush_lag is replication-slot specific and does NOT apply.

| YB metric name | Type | Entity | What it measures | YB file:line | Kudu analog | Prod-shaping? | Status |
|---|---|---|---|---|---|---|---|
| cdcsdk_sent_lag_micros | gauge_int64 | cdcsdk | Per-tablet sent lag (same semantics as async_replication_sent_lag_micros but on cdcsdk entity) | xrepl_metrics.cc:124 | cdc_stream_sent_lag_micros -- MAPPED (different entity name, same meaning) | HIGH -- MAPPED | Already added |
| cdcsdk_traffic_sent | counter | cdcsdk | Cumulative bytes sent (proto wire size of all records) | xrepl_metrics.cc:129 | MISSING | HIGH: ONLY byte-throughput metric in YB CDC; Kudu has zero byte metrics; cannot detect large-row spikes or bandwidth ceiling | MISSING |
| cdcsdk_change_event_count | counter | cdcsdk | Cumulative change events sent (per tablet-stream) | xrepl_metrics.cc:133 | cdc_records_produced (server-level only) -- PARTIAL MATCH (Kudu lacks per-stream granularity) | MED: server-level exists; per-stream granularity missing | MAPPED (server level only) |
| cdcsdk_expiry_time_ms | gauge_uint64 | cdcsdk | Remaining stream expiry window in ms (=expiry_threshold - time_since_last_poll); decrements toward 0 as stream goes idle | xrepl_metrics.cc:136 | MISSING | HIGH: direct "stream expires in N ms" gauge, actionable for paging alerts; Kudu has cdc_stream_active_age_micros (how long idle) but not the remaining budget | MISSING |
| cdcsdk_last_sent_physicaltime | gauge_uint64 | cdcsdk | Raw physical timestamp (us) of last record sent | xrepl_metrics.cc:141 | MISSING as direct metric (internal: last_sent_record_phys_micros) | MED: useful for debugging; lag is derived from it | MISSING |
| cdcsdk_flush_lag | gauge_uint64 | cdcsdk | WAL-flush lag for replication slot restart point | xrepl_metrics.cc:146 | N/A: replication-slot / logical-WAL concept; no equivalent in Kudu per-tablet model | N/A | N/A |

### 2c. CDCServerMetrics (entity: server)

| YB metric name | Type | Entity | What it measures | YB file:line | Kudu analog | Prod-shaping? | Status |
|---|---|---|---|---|---|---|---|
| cdc_rpc_proxy_count | counter | server | GetChanges requests that required proxy forwarding (not-leader case) | xrepl_metrics.cc:152 | MISSING | LOW: useful for diagnosing routing storm when many requests hit non-leaders; not critical | MISSING |

### 2d. xcluster_consumer.cc metrics (entity: server, consumer side)
Note: These apply to the XCluster CONSUMER (replication target tserver). Kudu
currently implements only the producer side. Mark as N/A unless Kudu grows a
consumer.

| YB metric name | Type | Entity | What it measures | YB file:line | Kudu analog | Prod-shaping? | Status |
|---|---|---|---|---|---|---|---|
| xcluster_consumer_replication_error_count | counter | server | Schema mismatch / missing-op-id errors requiring user fix | xcluster_consumer.cc:58 | N/A (consumer-side) | N/A | N/A |
| xcluster_consumer_apply_failure_count | counter | server | Failures calling GetChanges on source cluster | xcluster_consumer.cc:63 | N/A (consumer-side) | N/A | N/A |
| xcluster_consumer_poll_failure_count | counter | server | Failures applying changes to target tablet | xcluster_consumer.cc:68 | N/A (consumer-side) | N/A | N/A |

### 2e. Auto-generated RPC latency metric (YB framework)
The comment at xrepl_metrics.h:61 documents this metric:
  "For rpc_latency and rpcs_responded_count, use
   handler_latency_yb_cdc_CDCService_GetChanges"

This is auto-generated by the YB RPC framework. In Kudu, the equivalent
auto-generated metric is:
  handler_latency_kudu_cdc_CDCService_GetChanges
This is generated by the Kudu RPC framework for every registered service method
and IS present in Kudu. No action needed.

### 2f. StreamTabletStats (YB internal, NOT metric entities)
xrepl_stream_stats.h/cc tracks avg_throughput_kbps, mbs_sent, records_sent,
avg_poll_delay_ms, avg_get_changes_latency_ms in a circular buffer per stream.
These are NOT METRIC_DEFINE metrics. They are exposed via yb-admin/web UI
diagnostics only, not as Prometheus/JSON metrics. No formal metric analog is
required in Kudu, though the concepts (especially avg_get_changes_latency_ms
and avg_throughput_kbps) are worth noting as potential additions.

---

## 3. Shortlist of genuinely-missing production-shaping metrics with
   Kudu-idiomatic sketches

### 3.1 Byte throughput counter (HIGH severity)
**Gap:** No byte metric exists anywhere in Kudu CDC. cdcsdk_traffic_sent in YB
counts cumulative wire bytes of all records sent. Without this, operators cannot
detect:
  - Large-row storms (records count is low but bytes are high)
  - BW ceiling approaching
  - Regression in compression/encoding efficiency

**Kudu sketch (two metrics, server-level):**

In cdc_service.cc, add two server-level metrics:

  METRIC_DEFINE_counter(
      server, cdc_bytes_sent, "CDC Bytes Sent",
      kudu::MetricUnit::kBytes,
      "Cumulative payload bytes returned to CDC consumers across all GetChanges "
      "calls on this tablet server (proto wire size of all records emitted). "
      "Together with cdc_records_produced this gives an average record size.",
      kudu::MetricLevel::kInfo);

  METRIC_DEFINE_counter(
      server, cdc_snapshot_bytes_sent, "CDC Snapshot Bytes Sent",
      kudu::MetricUnit::kBytes,
      "Cumulative payload bytes from FULL-mode snapshot GetChanges responses. "
      "Broken out from cdc_bytes_sent to distinguish snapshot traffic from "
      "incremental change traffic.",
      kudu::MetricLevel::kInfo);

Increment cdc_bytes_sent after building each GetChanges response:
  // In GetChangesForTablet, after IncrementBy on records_produced_:
  bytes_sent_->IncrementBy(resp->ByteSizeLong());

Per-stream granularity can be added later on the cdc_stream entity if needed.

### 3.2 Stream time-to-expiry gauge (HIGH severity)
**Gap:** cdcsdk_expiry_time_ms gives "remaining_expiry_window_ms" directly.
Kudu's cdc_stream_active_age_micros gives "time_since_last_poll_us" which
requires mental arithmetic against the expiry threshold. Operators cannot set a
simple alert threshold like "alert if stream expires in < 30 min."

**Kudu sketch (per-stream entity):**

In cdc_service.cc, add to cdc_stream entity:

  METRIC_DEFINE_gauge_int64(
      cdc_stream, cdc_stream_time_to_expiry_micros,
      "CDC Stream Time to Expiry",
      kudu::MetricUnit::kMicroseconds,
      "For this CDC (stream, tablet) session, the remaining time in microseconds "
      "before the stream expires due to inactivity. Computed as "
      "(expiry_threshold - time_since_last_poll), clamped to 0. A value of 0 "
      "means the stream has already expired or is about to expire. Requires "
      "--cdc_stream_expiry_ms to be set.",
      kudu::MetricLevel::kWarn);

In SetupSessionMetrics, register as FunctionGauge:

  METRIC_cdc_stream_time_to_expiry_micros.InstantiateFunctionGauge(
      state->metric_entity,
      [st]() -> int64_t {
        const int64_t expiry_us =
            static_cast<int64_t>(FLAGS_cdc_stream_idle_expiry_ms) * 1000;
        if (expiry_us <= 0) return INT64_MAX; // expiry disabled
        const int64_t last_active =
            st->last_active_time_micros.load(std::memory_order_relaxed);
        if (last_active == 0) return expiry_us;
        const int64_t idle_us = GetCurrentTimeMicros() - last_active;
        const int64_t remaining = expiry_us - idle_us;
        return remaining > 0 ? remaining : 0;
      })
      ->AutoDetach(&state->metric_detacher);

Note: cdc_max_active_age_micros on the server entity implicitly encodes the
worst-case remaining budget, but exposing this directly as a per-stream countdown
makes alerting far simpler.

### 3.3 Committed lag / consumer-applied lag (MEDIUM-HIGH severity)
**Gap:** async_replication_committed_lag_micros in YB is the end-to-end lag:
  now - (physical_time_of_last_record_confirmed_applied_on_consumer)
This is sourced from cdc_state.last_replication_time, which is populated by the
consumer cluster when it applies ops. Kudu has no consumer feedback mechanism.

**Assessment:** This metric cannot be added without architectural changes. Kudu
currently has no "consumer acknowledges applied" path; the Checkpoint RPC carries
the consumer's WAL position but not the physical timestamp of the last applied
record. To add this properly, the Checkpoint RPC would need to carry
'last_applied_phystime' and CDCStreamTabletState would need to track it.

This is P2 scope work. For now, documenting as known gap. Alert workaround:
pair cdc_stream_sent_lag_micros (sent) with the consumer-side processing
monitoring (outside Kudu).

### 3.4 RPC payload size histogram (MEDIUM severity)
**Gap:** rpc_payload_bytes_responded is event_stats (histogram) exposing p50/p99
payload sizes per GetChanges call. Useful to detect if p99 responses are hitting
the --cdc_max_bytes_per_response ceiling (which can cause consumer-side memory
spikes on burst traffic).

**Kudu sketch:**

  METRIC_DEFINE_histogram(
      server, cdc_get_changes_response_bytes,
      "CDC GetChanges Response Bytes",
      kudu::MetricUnit::kBytes,
      "Histogram of proto payload sizes returned by GetChanges (only responses "
      "containing at least one record). Percentiles at p50/p95/p99 reveal whether "
      "responses are consistently near the --cdc_max_bytes_per_response ceiling.",
      kudu::MetricLevel::kInfo,
      65536, 1);

Increment in GetChangesForTablet after the record loop, only when
resp->records_size() > 0:
  get_changes_response_bytes_->Increment(resp->ByteSizeLong());

### 3.5 Heartbeat-only response counter (LOW severity)
**Gap:** rpc_heartbeats_responded counts GetChanges calls that returned no
records (fully caught up, or leader switch probe). The ratio
  heartbeats / (heartbeats + payload_responses)
reveals whether consumers are receiving any data at all or just idling.

**Kudu sketch:**

  METRIC_DEFINE_counter(
      server, cdc_heartbeat_responses, "CDC Heartbeat Responses",
      kudu::MetricUnit::kRequests,
      "Number of GetChanges responses containing zero records (consumer was "
      "fully caught up or no new ops existed). The ratio of this counter to "
      "cdc_get_changes_requests reveals the fraction of polls that delivered data.",
      kudu::MetricLevel::kInfo);

Increment when records_produced == 0 in the GetChanges handler.

---

## 4. Pressure-test: do Kudu's added metrics measure the right thing?

### 4.1 cdc_stream_sent_lag_micros / cdc_max_sent_lag_micros
vs. async_replication_sent_lag_micros

YB implementation (cdc_service.cc:4859):
  tablet_metric->async_replication_sent_lag_micros->set_value(
      std::max<int64_t>(0, GetCurrentTimeMicros() - last_replicated_micros + 1));
where last_replicated_micros = physical time of last record polled
  (tablet_metric->last_read_physicaltime->value() before the update).

Kudu implementation (SetupSessionMetrics, cdc_service.cc:781-791):
  FunctionGauge: now - last_sent_record_phys_micros (atomic, set per GetChanges)

VERDICT: Semantically identical. Both compute (now - timestamp_of_last_sent_record).
The mechanism differs: YB updates the gauge on a periodic UpdateLagMetrics task
(potentially stale by up to the task period), while Kudu recomputes on every
scrape (always fresh). Kudu's approach is more accurate. No correction needed.

Minor note: YB adds +1 to avoid returning 0 (so a freshly-polled tablet shows
lag = 1us not 0). Kudu clamps to 0, which is slightly more intuitive. Not a
behavioral difference that matters operationally.

### 4.2 cdc_stream_active_age_micros / cdc_max_active_age_micros
vs. time_since_last_getchanges

YB time_since_last_getchanges (xrepl_metrics.cc:110): resets only on GetChanges.
Updated in UpdateLagMetrics (cdc_service.cc:2502):
  tablet_metric->time_since_last_getchanges->set_value(
      GetCurrentTimeMicros() - last_getchanges_time);
where last_getchanges_time is set by UpdateTabletMetrics on GetChanges.

Kudu cdc_stream_active_age_micros (cdc_service.cc:793-804):
  FunctionGauge: now - last_active_time_micros
where last_active_time_micros is updated on BOTH GetChanges AND Checkpoint RPCs
(cdc_service.cc:2388 and cdc_service.cc:1223).

VERDICT: Subtle semantic difference. Kudu resets on Checkpoint activity; YB
resets only on GetChanges. In Kudu's two-RPC model (GetChanges + Checkpoint
are separate RPCs), a consumer that checkpoints without polling will show lower
cdc_stream_active_age_micros than the equivalent YB metric. This is acceptable
behavior -- a checkpointing consumer IS active -- but operators relying on this
metric to detect a stalled consumer should be aware that infrequent GetChanges
combined with frequent Checkpoints will keep active_age low even if no data is
being consumed. No code change needed, but consider adding a documentation
comment noting this semantic difference.

### 4.3 cdc_stream_bootstrap_required / cdc_bootstrap_required_streams
vs. is_bootstrap_required

YB is_bootstrap_required: set_value(status.IsNotFound()) in cdc_service.cc:1998,
i.e., fires AFTER the WAL entry is actually missing (reactive).

Kudu cdc_stream_bootstrap_required: fires when checkpoint_op_index <
last_known_min_replicate_index (the minimum retained index in the log reader).
This is PREDICTIVE -- it fires as soon as the consumer's checkpoint approaches
the WAL window edge, before a WAL_EXPIRED error actually occurs.

VERDICT: Kudu's implementation is MORE CONSERVATIVE (better for production).
It gives operators earlier warning that a stream is at risk. YB's version requires
that a GetChanges call with the stale checkpoint actually be made before the flag
flips. No correction needed; Kudu behavior is preferable.

### 4.4 cdc_stream_ops_behind / cdc_max_ops_behind

No direct YB Prometheus metric equivalent (YB tracks this only in
StreamTabletStatsHistory as sent_index vs latest_index, exposed only via web UI).
Kudu's cdc_stream_ops_behind is a proper per-stream Prometheus gauge -- it is
MORE complete observability than what YB offers for this signal. Good addition.

### 4.5 cdc_wal_retained_bytes

No YB equivalent. The Kudu-specific addition measures the delta between
GC-able bytes under the pure Raft floor and under the CDC-clamped floor. This
is the definitive "disk cost of CDC" signal and directly addresses the P0
disk-exhaustion concern. YB has no equivalent per-tablet metric for CDC's
contribution to WAL retention. The implementation correctly uses two
GetGCableDataSize calls in the FunctionGauge callback (acceptable for a
scrape-time callback). No correction needed.

### 4.6 cdc_history_floor_age_micros

No YB equivalent. A Kudu-specific addition for MVCC history floor tracking.
The age rises as the FULL-mode stream fails to advance its history floor (e.g.,
consumer lagging). This is the right signal for detecting MVCC retention leaks
that will eventually force aggressive GC. No correction needed.

### 4.7 cdc_checkpoint_persists vs. cdc_checkpoint_requests (ratio)

No YB equivalent. Kudu adds this to expose the throttle effectiveness
(--cdc_checkpoint_persist_interval_ms). The ratio persists/requests near 1.0
indicates the throttle is ineffective. This is a Kudu-specific improvement with
no YB analog. Good addition.

---

## 5. Entity coverage summary

| Entity | YB | Kudu |
|---|---|---|
| Per-tablet per-stream (xcluster / cdc_stream) | 15 metrics (xcluster entity) | 4 metrics (cdc_stream entity): sent_lag, active_age, ops_behind, bootstrap_required |
| Per-tablet (tablet entity) | 0 | 3: cdc_barrier_forced_releases, cdc_wal_retained_bytes, cdc_history_floor_age_micros |
| Server (tserver) | 1 (cdc_rpc_proxy_count) + 3 consumer-side | 14: requests, records, checkpoint, errors (x12+aggregate+3 rejection), max_sent_lag, max_active_age, active_streams, max_ops_behind, bootstrap_required_streams |
| Master | 0 | 6: barrier_releases_total, barrier_releases_deferred, barriered_tablet_count, maintenance_last_run_micros, maintenance_last_run_duration_micros, maintenance_runs |
