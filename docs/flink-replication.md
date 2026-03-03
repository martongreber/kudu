# Apache Kudu Flink-Based Replication

This document describes how to use the Apache Flink-based replication job to continuously replicate
data from one Apache Kudu cluster to another.

## Overview

The Kudu replication job is an Apache Flink streaming application that continuously copies data
from a **source** Kudu table to a **sink** Kudu table. It uses Kudu's
[diff scan](https://kudu.apache.org/docs/transaction_semantics.html) capability to efficiently
detect and replicate only the rows that changed between two points in time, rather than scanning
the full table on every cycle.

The replication mechanism works as follows:

1. On first startup, the job performs a full snapshot scan of the source table and records the
   snapshot timestamp `t0`.
2. On each subsequent discovery cycle, it performs a diff scan over the interval
   `[t_previous, t_now]`, which returns all inserted, updated, and deleted rows in that window.
3. Deleted rows are identified via Kudu's `IS_DELETED` virtual column and replicated as delete
   operations on the sink.
4. All writes to the sink use upsert-ignore semantics, making replication idempotent and
   compatible with at-least-once Flink checkpointing.

Flink checkpoints are taken regularly to enable fault-tolerant recovery. On restart from a
checkpoint, any splits that were in-flight at the time of the checkpoint are re-processed.
Because sink writes are idempotent, re-processing is safe.

## Prerequisites

- A running Apache Flink cluster (session or per-job mode).
- A source Kudu cluster with the table to replicate.
- A sink Kudu cluster with the destination table already created, or `job.createTable=true`
  to have the job create it automatically.
- A shared filesystem (e.g. HDFS, S3) accessible by all Flink TaskManagers for storing
  Flink checkpoints.
- The replication job JAR (e.g. `kudu-replication-<version>-shaded.jar`).

### Source cluster configuration

Kudu retains MVCC history for diff scans for a configurable duration controlled by the
`--tablet_history_max_age_sec` tablet server flag (default: 7 days). If the replication job is
stopped for longer than this retention period, the next diff scan may fail because the required
history has been garbage collected. Ensure this value is set to at least as long as the maximum
expected downtime of the replication job.

Note: the master also accepts this flag but overrides its default to 5 minutes, since the master
only ever reads the latest snapshot from the system catalog and does not need a long history.
The tserver default of 7 days is what governs user table data.

The flag can also be overridden per-table via the `history_max_age_sec` extra config.

## Security

### Kerberos authentication

The Kudu Java client negotiates Kerberos authentication automatically using the ambient Kerberos
ticket cache (via GSSAPI/JAAS). There are no explicit keytab or principal flags in the replication
job's own configuration — authentication is handled entirely at the environment level.

On YARN, the standard approach is:

1. Ensure a valid Kerberos ticket exists for the submitting user on the node where `flink run` is
   executed (`kinit -kt /path/to/user.keytab user@REALM`).
2. YARN propagates Hadoop delegation tokens into the JobManager and TaskManager containers
   automatically. The Kudu client inside each container picks up the ticket from the token cache.

If you need to provide credentials explicitly (e.g. for long-running jobs where ticket renewal
cannot be guaranteed by the environment), use Flink's built-in keytab support via `-D` flags at
submission time:

```bash
flink run-application -t yarn-application \
  -Dsecurity.kerberos.login.keytab=/path/to/user.keytab \
  -Dsecurity.kerberos.login.principal=user@REALM \
  -Dclassloader.parent-first-patterns.additional=org.apache.kudu \
  -c org.apache.kudu.replication.ReplicationJob \
  kudu-replication-<version>-shaded.jar \
  ...
```

Flink will automatically renew the keytab-based TGT at regular intervals
(`security.kerberos.relogin.period`, default: 1 minute) and ship the keytab to YARN nodes
(`yarn.security.kerberos.ship-local-keytab`, default: true).

### Authorization (Ranger)

If the Kudu cluster has Apache Ranger integration enabled, the user submitting the replication
job must have the following Ranger privileges:

| Resource | Required privilege |
|----------|--------------------|
| Source table | `select`, `metadata` |
| Sink database | `create` (only if `job.createTable=true`) |
| Sink table | `all` |

### Protecting the sink cluster from accidental writes

When using the sink cluster as a DR target, it is strongly recommended to restrict write access
on the sink cluster so that only the replication job's service user can write to replicated
tables. This prevents accidental data manipulation on the DR side that would cause divergence
between source and sink and invalidate the replication state.

The recommended Ranger policy setup on the sink cluster is:

| Principal | Resource | Privilege | Rationale |
|-----------|----------|-----------|-----------|
| Replication service user | Sink tables | `all` | Required for the replication job to upsert and delete rows |
| All other users | Sink tables | `select`, `metadata` (read-only) | Allows querying the DR cluster without risk of writes |
| All other users | Sink tables | *(no insert/update/delete)* | Explicitly deny or omit write privileges |

> **Note:** Ranger deny policies can be used to explicitly block write operations (insert, update,
> delete) on the sink tables for all users except the replication service user, even if those
> users have broad database-level grants elsewhere. This is the safest approach for a DR cluster.

### Classloader isolation

When running on a secured cluster, always pass the following `-D` flag to avoid Kerberos
classloader conflicts between the Kudu client and Flink's child-first classloader:

```
-Dclassloader.parent-first-patterns.additional=org.apache.kudu
```

This ensures Kudu classes are loaded by the same parent classloader as Hadoop security classes
(`org.apache.hadoop.*` is already in Flink's default parent-first list), preventing
`ClassCastException` errors during Kerberos negotiation.

## Starting the Replication Job

The replication job is started by submitting the shaded JAR to a Flink cluster using the
standard `flink run` command. All configuration is passed as `--key value` command-line
arguments.

### Minimal example

```bash
flink run-application -t yarn-application \
  -Dclassloader.parent-first-patterns.additional=org.apache.kudu \
  -c org.apache.kudu.replication.ReplicationJob \
  kudu-replication-<version>-shaded.jar \
  --job.sourceMasterAddresses source-master1:7051,source-master2:7051 \
  --job.sinkMasterAddresses   sink-master1:7051,sink-master2:7051 \
  --job.tableName             my_table \
  --job.checkpointsDirectory  hdfs:///kudu-replication/checkpoints/my_table
```

### Resuming from a checkpoint

To resume from a specific Flink checkpoint or savepoint after a failure or planned restart:

```bash
flink run-application -t yarn-application \
  -s hdfs:///kudu-replication/checkpoints/my_table/<checkpoint-id> \
  -Dclassloader.parent-first-patterns.additional=org.apache.kudu \
  -c org.apache.kudu.replication.ReplicationJob \
  kudu-replication-<version>-shaded.jar \
  --job.sourceMasterAddresses source-master1:7051,source-master2:7051 \
  --job.sinkMasterAddresses   sink-master1:7051,sink-master2:7051 \
  --job.tableName             my_table \
  --job.checkpointsDirectory  hdfs:///kudu-replication/checkpoints/my_table
```

Checkpoints are retained on cancellation (`RETAIN_ON_CANCELLATION`), so the last checkpoint
directory is always available for recovery.

## Configuration Reference

Configuration parameters are grouped into three namespaces: `job.*`, `reader.*`, and `writer.*`.

### Job parameters

These parameters control the overall replication behavior.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `job.sourceMasterAddresses` | Yes | — | Comma-separated list of source Kudu master addresses (e.g. `host1:7051,host2:7051`). |
| `job.sinkMasterAddresses` | Yes | — | Comma-separated list of sink Kudu master addresses. |
| `job.tableName` | Yes | — | Name of the source Kudu table to replicate. |
| `job.checkpointsDirectory` | Yes | — | Filesystem path where Flink stores checkpoint data (e.g. `hdfs:///path/to/checkpoints`). |
| `job.discoveryIntervalSeconds` | No | `600` | How often (in seconds) the job polls for new changes via a diff scan. |
| `job.checkpointingIntervalMillis` | No | `60000` | Interval (in milliseconds) at which Flink takes checkpoints. Must be strictly less than `job.discoveryIntervalSeconds * 1000`. |
| `job.createTable` | No | `false` | If `true`, automatically create the sink table if it does not exist, replicating the full partition schema from the source. |
| `job.tableSuffix` | No | `""` | Suffix appended to the sink table name. Useful for testing replication to a table with a different name on the same cluster. |
| `job.restoreOwner` | No | `false` | If `true` and `job.createTable=true`, copy the table owner from the source to the sink table. |

> **Note:** `job.checkpointingIntervalMillis` must be strictly less than
> `job.discoveryIntervalSeconds * 1000`. This ensures the `lastEndTimestamp` advances before
> the next discovery cycle begins. The job will fail to start if this constraint is violated.

### Reader parameters

These parameters control how data is read from the source Kudu cluster.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `reader.batchSizeBytes` | No | `20971520` (20 MiB) | Maximum number of bytes fetched in a single scan batch. |
| `reader.splitSizeBytes` | No | Kudu default | Target size in bytes for each scan split when parallelizing input. |
| `reader.scanRequestTimeout` | No | `30000` ms | Timeout in milliseconds for individual scan RPCs. |
| `reader.prefetching` | No | `false` | Whether to enable prefetching of data blocks from the scanner. |
| `reader.keepAlivePeriodMs` | No | `15000` ms | Period in milliseconds after which an idle scanner sends a keep-alive to the server. |
| `reader.replicaSelection` | No | `CLOSEST_REPLICA` | Replica selection strategy. Valid values: `CLOSEST_REPLICA`, `LEADER_ONLY`. |

### Writer parameters

These parameters control how data is written to the sink Kudu cluster.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `writer.flushMode` | No | `AUTO_FLUSH_BACKGROUND` | Kudu session flush mode. Valid values: `AUTO_FLUSH_BACKGROUND`, `AUTO_FLUSH_SYNC`, `MANUAL_FLUSH`. |
| `writer.operationTimeout` | No | `30000` ms | Timeout in milliseconds for individual write operations. |
| `writer.maxBufferSize` | No | `1000` | Maximum number of operations buffered in the Kudu write session. |
| `writer.flushInterval` | No | `1000` ms | Interval in milliseconds at which buffered operations are flushed automatically. |

## Supported Data Types

The following Kudu column types are supported for replication:

| Kudu Type | Notes |
|-----------|-------|
| `STRING` | |
| `BOOL` | |
| `INT8` | |
| `INT16` | |
| `INT32` | |
| `INT64` | |
| `FLOAT` | |
| `DOUBLE` | |
| `DECIMAL(precision, scale)` | Precision and scale are preserved. |
| `UNIXTIME_MICROS` | Timestamps with microsecond precision. |
| `BINARY` | |

> **Limitation:** The `ARRAY` column type is **not supported**. Tables containing array columns
> cannot be replicated.

## Flink Metrics

The replication job exposes the following custom metrics via the Flink metrics system. These
metrics are registered on the source enumerator's metric group and are available through any
configured Flink metrics reporter (e.g. JMX, Prometheus).

| Metric Name | Type | Description |
|-------------|------|-------------|
| `lastEndTimestamp` | Gauge (Long) | The end timestamp (epoch milliseconds) of the most recently completed diff scan. Monotonically increasing. Use this to verify that replication is advancing. |
| `pendingCount` | Gauge (Integer) | Number of scan splits currently assigned to readers but not yet fully processed. |
| `unassignedCount` | Gauge (Integer) | Number of scan splits waiting to be assigned to a reader. |
| `pendingRemovalCount` | Gauge (Integer) | Number of completed splits deferred for removal until the next Flink checkpoint completes. |

In steady state (between discovery cycles), `pendingCount`, `unassignedCount`, and
`pendingRemovalCount` should all be zero. The `lastEndTimestamp` metric is the primary indicator
of replication health; if it stops advancing, the replication job has stalled.

## Monitoring

### Setting up Prometheus

#### Flink

Add the following to `conf/config.yaml` on the Flink cluster to enable Prometheus metrics
reporting:

```yaml
metrics:
  reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
  reporter.prom.port: 9250-9260
```

The port range `9250-9260` covers one port per TaskManager instance.

#### Kudu tserver metrics and table-level relabeling

Kudu's Prometheus endpoint (`/metrics_prometheus`) embeds the tablet ID directly in each metric
name (e.g. `kudu_tablet_<tablet-id>_rows_upserted`). This means queries must reference
individual tablet IDs rather than table names, and must be updated whenever tablets are added or
removed.

To make metrics queryable by table name two additional components are required:

1. **Metric relabeling** in the Prometheus scrape config — normalizes metric names by extracting
   the tablet ID into a proper label.
2. **`json_exporter`** — reads Kudu's JSON metrics endpoint (which does include `table_name`)
   and exposes a `kudu_tablet_info` metric that maps each `tablet_id` to its `table_name`.

**Install `json_exporter`:**

Download the latest release from the
[prometheus-community/json_exporter](https://github.com/prometheus-community/json_exporter/releases)
releases page and place the binary on a host reachable by Prometheus.

Create `kudu_tablet_info.yml`:

```yaml
modules:
  default:
    metrics:
      - name: kudu_tablet
        type: object
        path: "{[?(@.type=='tablet')]}"
        labels:
          tablet_id: "{.id}"
          table_name: "{.attributes.table_name}"
          table_id: "{.attributes.table_id}"
        values:
          info: "1"
```

Start `json_exporter`:

```bash
./json_exporter --config.file=kudu_tablet_info.yml --web.listen-address=:7979
```

#### Full `prometheus.yml` scrape config

```yaml
scrape_configs:
  - job_name: "replication"
    static_configs:
      - targets: ["<flink-host>:9250", "<flink-host>:9251"]
        labels:
          app: "replication"

  - job_name: "kudu"
    metrics_path: "/metrics_prometheus"
    static_configs:
      - targets: ["<tserver-host1>:9871", "<tserver-host2>:9871"]
        labels:
          app: "kudu"
    metric_relabel_configs:
      # Extract tablet_id from the metric name into a label
      - source_labels: [__name__]
        regex: 'kudu_tablet_([a-f0-9]{32})_(.*)'
        target_label: tablet_id
        replacement: '$1'
      # Normalize the metric name by stripping the embedded tablet_id
      - source_labels: [__name__]
        regex: 'kudu_tablet_([a-f0-9]{32})_(.*)'
        target_label: __name__
        replacement: 'kudu_tablet_$2'

  - job_name: "kudu_tablet_info"
    metrics_path: /probe
    params:
      module: [default]
    static_configs:
      - targets:
          - "http://<tserver-host1>:9871/metrics?types=tablet"
          - "http://<tserver-host2>:9871/metrics?types=tablet"
    relabel_configs:
      - source_labels: [__address__]
        target_label: __param_target
      - source_labels: [__param_target]
        target_label: instance
      - target_label: __address__
        replacement: "<json-exporter-host>:7979"
```

> **Note:** To enable hot-reloading without restarting Prometheus, start it with the
> `--web.enable-lifecycle` flag and reload with
> `curl -X POST http://localhost:9090/-/reload`.

### Grafana dashboard

The following PromQL queries form a minimal monitoring dashboard for the replication job.

#### Source table write activity (rows/min by operation type)

Shows the rate of each write operation type on the source table. Use this to verify the source
table is receiving data and to detect unexpected operation patterns.

```promql
sum by (__name__) (
  (
    rate(kudu_tablet_rows_inserted[1m])
    or rate(kudu_tablet_rows_updated[1m])
    or rate(kudu_tablet_rows_upserted[1m])
    or rate(kudu_tablet_rows_deleted[1m])
  )
  * on(tablet_id) group_left(table_name)
  (max by (tablet_id, table_name, table_id) (kudu_tablet_info{table_name="<table-name>"}))
) * 60
```

Set the Grafana legend to `{{__name__}}` for per-operation labeling.

> **Note:** `rate()` is preferred over `increase()` here because it is resilient to counter
> resets caused by tserver restarts or tablet leadership changes.

#### Replication lag (minutes)

Shows how many minutes have elapsed since the enumerator last completed a diff scan cycle. In a
healthy replication job this value should stay close to `job.discoveryIntervalSeconds / 60`.

```promql
(time() - (flink_jobmanager_job_operator_coordinator_enumerator_lastEndTimestamp / 1000)) / 60
```

#### Enumerator split state

Shows the number of splits in each state. In steady state all three should be zero between
discovery cycles. Non-zero values indicate an active discovery cycle or a stalled reader.

```promql
label_replace(flink_jobmanager_job_operator_coordinator_enumerator_pendingCount, "split_state", "pending", "", "")
or
label_replace(flink_jobmanager_job_operator_coordinator_enumerator_pendingRemovalCount, "split_state", "pendingRemoval", "", "")
or
label_replace(flink_jobmanager_job_operator_coordinator_enumerator_unassignedCount, "split_state", "unassigned", "", "")
```

Set the Grafana legend to `{{split_state}}`.

> **Note:** During an active discovery cycle, `unassignedCount` spikes up to the number of
> tablets at cycle start and drains to zero as splits are assigned to readers.
> `pendingRemovalCount` rises briefly after processing and drops to zero at the next Flink
> checkpoint. This is normal. Sustained non-zero values that do not drain between cycles
> indicate a stalled reader.

> **Note:** On first startup, before the initial snapshot scan completes, `lastEndTimestamp`
> is `0`. The replication lag query will therefore return a very large value until the first
> full scan finishes. This is expected and not an error.

#### Live row count (coarse sanity check)

`kudu_tablet_live_row_count` can be used to compare the approximate row count between source
and sink tables as a coarse consistency check:

```promql
sum(
  kudu_tablet_live_row_count
  * on(tablet_id) group_left(table_name)
  (max by (tablet_id, table_name, table_id) (kudu_tablet_info{table_name=~"<table-name>|<table-name><suffix>"}))
) by (table_name)
```

The `table_name=~` regex filter restricts the result to the source and sink table pair,
excluding any unrelated tables on the same tserver. Replace `<suffix>` with the value of
`job.tableSuffix` used when starting the replication job (e.g. `_backup`). Both tables appear
as separate lines on the same graph — set the legend to `{{table_name}}`.

> **Limitation:** Row counts hide the effect of upserts. If two rows with the same key are
> written to the source but the sink already has that key, the sink count does not change even
> though the row content was updated. Row count equality is a necessary but not sufficient
> condition for data consistency.

### Multiple tables

Each replicated table requires its own replication job. When monitoring multiple tables,
update the `table_name` filter in all Grafana queries to match the table being monitored.
The `kudu_tablet_info` mapping metric covers all tables on the tserver automatically — no
changes to the `json_exporter` or Prometheus config are needed when new tables are added.

> **Note on `json_exporter` metric naming:** `json_exporter` automatically appends `_info` to
> object-type metric names. The config entry `name: kudu_tablet` therefore produces a metric
> named `kudu_tablet_info` in Prometheus. This is expected behaviour.

## Schema Change Handling

> **Warning:** The schema change procedure depends on monitoring to verify its preconditions.
> Without a working Prometheus setup, there is no reliable way to determine when it is safe to
> apply DDL. Applying a schema change with writes still in flight risks producing rows with
> mismatched schemas on the sink.

The replication job does not automatically detect or apply schema changes. If the source table
schema changes (e.g. a column is added), you must perform the change manually on both the source
and sink tables, and restart the replication job from a savepoint.

The key requirement before applying any DDL is that **no writes are in flight on the source
table** at the moment the schema change is applied. Applying DDL while writes are active risks
replication producing rows with mismatched schemas on the sink.

### Monitoring write activity on the source table

Use the write activity query from the [Grafana dashboard](#grafana-dashboard) section to verify
that no new rows are being written to the source table. When the query returns 0, the source
table is idle and it is safe to proceed with the DDL.

### Monitoring replication lag

To verify that replication has caught up before stopping the job for a schema change, use the
replication lag query from the [Grafana dashboard](#grafana-dashboard) section. The lag should
be close to zero before proceeding.

### Procedure for adding a column

1. Verify replication lag is near zero using the PromQL query above, and confirm no writes are
   active on the source table.
2. Create a savepoint to gracefully stop the replication job:
   ```bash
   flink stop -p hdfs:///kudu-replication/savepoints/my_table <job-id>
   ```
3. Add the new column to the source Kudu table:
   ```bash
   kudu table add_column <source-master-addresses> my_table new_column INT32 \
     --default 0
   ```
4. Add the same column with the same definition to the sink Kudu table:
   ```bash
   kudu table add_column <sink-master-addresses> my_table new_column INT32 \
     --default 0
   ```
5. Restart the replication job from the savepoint:
   ```bash
   flink run-application -t yarn-application \
     -s hdfs:///kudu-replication/savepoints/my_table/<savepoint-id> \
     -Dclassloader.parent-first-patterns.additional=org.apache.kudu \
     -c org.apache.kudu.replication.ReplicationJob \
     kudu-replication-<version>-shaded.jar \
     --job.sourceMasterAddresses source-master1:7051 \
     --job.sinkMasterAddresses   sink-master1:7051 \
     --job.tableName             my_table \
     --job.checkpointsDirectory  hdfs:///kudu-replication/checkpoints/my_table
   ```

> **Note:** The new column must have a default value so that rows inserted before the column was
> added can be read without error during the next diff scan.

Other schema change types (dropping a column, renaming a column, changing a column type) are not
currently tested in the context of replication. These operations may require additional care and
should be validated in a non-production environment before being applied.

## Resource Configuration

Resource sizing for a replication job has three independent dimensions: the Kudu source cluster,
the Kudu Java client used inside the replication job, and the Flink cluster itself.

### Kudu source cluster (tserver)

A longer `--tablet_history_max_age_sec` window means Kudu tablets accumulate more UNDO delta data
before it is eligible for garbage collection. The two tserver flags directly relevant to this are:

| Flag | Default | Description |
|------|---------|-------------|
| `--maintenance_manager_num_threads` | `1` | Number of threads for all background maintenance ops (flush, compaction, UNDO GC). With a large history window and many tablets, the single default thread can fall behind on GC. The [recommended ratio](troubleshooting.adoc) is 1 thread per 3 data directories (e.g. 4 threads for 12 data directories). |

### Replication job (Kudu Java client)

The `reader.*` and `writer.*` parameters described in the [Configuration Reference](#configuration-reference)
are the interface for tuning the Kudu Java client embedded in the replication job.

The most impactful parameters for throughput and memory use are:

| Parameter | Default | Why it matters |
|-----------|---------|----------------|
| `reader.batchSizeBytes` | `20971520` (20 MiB) | Controls how much data each scan RPC fetches. Increase for large tables on fast networks; decrease to reduce per-reader heap usage. |
| `reader.splitSizeBytes` | Kudu default | Smaller values produce more, finer-grained splits, increasing parallelism at the cost of more scan RPCs. |
| `writer.maxBufferSize` | `1000` | Operations buffered client-side before flushing. Increase for higher write throughput; each buffered op consumes heap. |
| `writer.flushInterval` | `1000` ms | How often buffered writes are flushed. Lower values reduce latency; higher values improve batching efficiency. |

### Flink cluster

The replication job is a simple two-operator pipeline (source → sink) with no intermediate
transformations. Resource requirements are proportional to the configured parallelism.

**Deployment mode.** The job runs on YARN in one of two modes:

- **Application mode** (`flink run-application -t yarn-application`) — recommended since Flink 1.15.
- **Per-job mode** (`flink run -t yarn-per-job`) — deprecated since Flink 1.15 but still supported.

In both modes, YARN spawns **dedicated containers** for the JobManager and TaskManagers of this
job. The `-D` memory flags below set the size of those YARN containers and apply at submission
time. If the YARN cluster is shared with other workloads (e.g. Spark, Impala, or other Flink
jobs), ensure that sufficient YARN resources (memory and vcores) are available for the replication
job's containers in addition to the existing cluster load.

**Parallelism** is set at submission time with the `-p` flag. Each parallel instance runs one
`KuduSource` reader and one `KuduSink` writer. Multiple parallel readers reduce wall-clock time
per discovery cycle: the enumerator distributes splits across all registered readers and only
starts the next diff scan once every split from the current cycle is fully processed.

The effective upper bound on useful parallelism is the number of scan tokens Kudu produces for
the table, which by default equals the number of tablets. Setting `-p` higher than the tablet
count does not help, as the extra reader instances will sit idle waiting for splits. To increase
the number of splits — and thus raise the useful parallelism ceiling — reduce
`reader.splitSizeBytes` so that Kudu sub-divides large tablets into multiple tokens.

**TaskManager memory.** The right size depends on row width, parallelism, and slot count, so
start with Flink's defaults and adjust based on observed usage. The main memory drivers per
TaskManager are the scan batch buffer (`reader.batchSizeBytes`, default 20 MiB, multiplied by
the number of slots per TaskManager) and the write buffer (`writer.maxBufferSize × average row
size` per slot). If TaskManagers are OOM-killed by YARN, increase
`taskmanager.memory.process.size` or reduce `reader.batchSizeBytes`.

**JobManager memory.** The enumerator's footprint is small regardless of table size or
parallelism. The Flink default is sufficient in almost all cases.

Pass settings as `-D` flags on the submission command:

```bash
flink run-application -t yarn-application \
  -p 4 \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -Dclassloader.parent-first-patterns.additional=org.apache.kudu \
  -c org.apache.kudu.replication.ReplicationJob \
  kudu-replication-<version>-shaded.jar \
  --job.sourceMasterAddresses source-master1:7051 \
  --job.sinkMasterAddresses   sink-master1:7051 \
  --job.tableName             my_table \
  --job.checkpointsDirectory  hdfs:///kudu-replication/checkpoints/my_table
```
