# YugabyteDB CDC Architecture

> Research notes for the Kudu CDC design project.

## Overview

YugabyteDB implements CDC as **XREPL** (external replication) with two major modes:

| Mode | Purpose | Consumer |
|------|---------|----------|
| **CDCSDK** | Logical CDC — ordered row-level events | Debezium, PostgreSQL walsender |
| **xCluster** | Cross-cluster replication (WAL format) | `XClusterPoller` on target cluster |

Both modes share a single `CDCServiceImpl` RPC service running on every tablet server,
but use different producer code paths and output formats.

---

## Architecture Diagram

```
Master (catalog)
├── XReplCatalogManager
│   ├── CreateCDCStream / DeleteCDCStream (master_replication.proto)
│   ├── CDCStreamInfo + SysCDCStreamEntryPB in sys.catalog
│   └── PopulateCDCStateTable() → cdc_state YCQL table
│
Tablet Server (per node)
├── CDCServiceImpl (RPC service)
│   ├── GetChanges (per-tablet poll)
│   ├── GetConsistentChanges (Virtual WAL, multi-tablet)
│   ├── UpdateCdcReplicatedIndex (retention barriers)
│   └── checkpoint / bootstrap / lag APIs
│
│   Producers:
│   ├── GetChangesForCDCSDK()  ← WAL + DocDB intents + snapshot
│   └── GetChangesForXCluster() ← Raft WAL (CDCRecordFormat::WAL)
│
│   CDCSDKVirtualWAL → merges per-tablet GetChanges for walsender
│
└── Consumers (all pull-based)
    ├── Debezium connector (polls GetChanges)
    ├── PostgreSQL walsender (polls GetConsistentChanges)
    └── XClusterConsumer (polls GetChanges on source)
```

---

## Key Source Files

### CDC Core (`src/yb/cdc/`)

| File | Role |
|------|------|
| `cdc_service.h` / `cdc_service.cc` | Main RPC service: `GetChanges`, checkpoints, Virtual WAL |
| `cdc_service.proto` | Service + message definitions |
| `cdc_producer.h` | Producer API: `GetChangesForCDCSDK`, `GetChangesForXCluster` |
| `cdcsdk_producer.cc` | CDCSDK: WAL read, intent streaming, snapshot, row encoding |
| `xcluster_producer.cc` | xCluster: read replicated WAL ops |
| `cdcsdk_virtual_wal.h` / `cdcsdk_virtual_wal.cc` | Multi-tablet consistent changes for walsender |
| `cdc_state_table.h` / `cdc_state_table.cc` | `cdc_state` YCQL table access (durable checkpoints) |
| `xrepl_stream_metadata.h` / `xrepl_stream_metadata.cc` | In-memory stream metadata cache |

### Master / Catalog

| File | Role |
|------|------|
| `master/xrepl_catalog_manager.cc` | `CreateCDCStream`, `PopulateCDCStateTable` |
| `master/catalog_entity_info.proto` | `SysCDCStreamEntryPB`, `CDCStreamOptionsPB` |
| `master/master_replication.proto` | Master-facing CDC stream RPCs |

### WAL / Tablet Integration

| File | Role |
|------|------|
| `consensus/consensus_queue.cc` | `ReadReplicatedMessagesForCDC`, `ReadReplicatedMessagesForConsistentCDC` |
| `consensus/log.h` / `log.cc` | `cdc_min_replicated_index` for log GC |
| `tablet/tablet.cc` | `GetIntentsForCDC` |
| `docdb/docdb.cc` | `GetIntentsBatchForCDC` |
| `tablet/transaction_participant.cc` | CDC retention barriers, intent cleanup coordination |

### xCluster Consumer (target side)

| File | Role |
|------|------|
| `tserver/xcluster_consumer.h` / `xcluster_consumer.cc` | Manages pollers per replication group |
| `tserver/xcluster_poller.h` / `xcluster_poller.cc` | Per (stream, tablet) GetChanges poll loop |

---

## Why CDCSDK Needs Both WAL + Intents

YugabyteDB uses DocDB (RocksDB-based) as its storage engine. Committed writes land in RocksDB
directly, while transactional writes are stored as **intents** until commit. The Raft WAL
carries transaction metadata (BEGIN/COMMIT) and DDL, but **not the actual row data**.

Therefore CDCSDK must:
1. Read the WAL for transaction boundaries and DDL
2. Read DocDB intents for the actual row-level INSERT/UPDATE/DELETE data

This dual-source approach is the primary architectural complexity in YugabyteDB's CDC.

**Kudu does not have this problem**: the full `WriteRequestPB` (including all row operations)
is embedded in every Raft `ReplicateMsg`. The WAL is a complete logical change log.

---

## Data Flow: Write → CDC Event (CDCSDK)

```
1. SQL/DML → TabletPeer::SubmitWrite / consensus replicate
2. ReplicateMsg appended to Raft WAL (OpId term.index, hybrid_time)
3. Apply to DocDB:
   - Committed writes → RocksDB
   - Transactional writes → intents until commit
4. On GetChanges (CDCSDK):
   a. Read WAL from checkpoint via ReadReplicatedMessagesForConsistentCDC
      → BEGIN/COMMIT/APPLY txn records, DDL in WAL
   b. Read intents via GetIntentsForCDC for row-level INSERT/UPDATE/DELETE
   c. Resolve consistent safe time (leader safe time, running txns)
   d. Encode CDCSDKProtoRecordPB (RowMessage + cdc_sdk_op_id)
   e. Update checkpoint in response + optionally cdc_state
5. Consumer receives batch, commits offset/LSN, calls UpdateCdcReplicatedIndex
```

---

## Stream Lifecycle

1. Client calls `CreateCDCStream` on master (`master_replication.proto`)
2. `XReplCatalogManager` persists `SysCDCStreamEntryPB` (state INITIATED → ACTIVE)
3. `PopulateCDCStateTable` inserts per-tablet rows with initial checkpoints
4. Consumer discovers tablets via `ListTablets`, polls `GetChanges` on each tablet leader
5. `DeleteCDCStream` cleans up catalog + `cdc_state` entries

---

## Checkpointing

| Scope | Mechanism |
|-------|----------|
| Per-tablet checkpoint | `cdc_state` YCQL table: key=(tablet_id, stream_id), value=OpId |
| Replication slot LSNs | `confirmed_flush_lsn`, `restart_lsn` for PG walsender |
| In-memory | `TabletCheckpoint` per stream for log GC decisions |
| Log retention | `cdc_min_replicated_index_` on `Log` prevents WAL GC |
| Propagation | `UpdateCdcReplicatedIndex` RPC sets index on all peers |

---

## Key Protobuf Messages (`cdc_service.proto`)

```proto
enum CDCRecordType { CHANGE, PG_FULL, PG_DEFAULT, ... }
enum CDCRecordFormat { JSON, WAL, PROTO }
enum CDCRequestSource { XCLUSTER, CDCSDK }
enum CDCCheckpointType { IMPLICIT, EXPLICIT }

message GetChangesRequestPB { stream_id, tablet_id, from_checkpoint, ... }
message GetChangesResponsePB { records, checkpoint, ... }
message CDCSDKProtoRecordPB { RowMessage, CDCSDKOpIdPB }
message RowMessage { op (INSERT/UPDATE/DELETE/BEGIN/COMMIT/DDL), new_tuple, old_tuple, ... }
```

---

## Consumer Models

| Consumer | Poll Mechanism | Delivery |
|----------|---------------|---------|
| Debezium | `GetChanges` per tablet | Pull |
| PG walsender | `GetConsistentChanges` via Virtual WAL | Pull |
| xCluster | `XClusterPoller::GetChanges` on source | Pull |

All are **pull-based**. No server-initiated push exists.

---

## Log Retention Mechanism

```cpp
// In log.cc: GetSegmentsToGCUnlocked()
// Uses cdc_min_replicated_index_ to protect segments needed by CDC
Log::cdc_min_replicated_index_

// Set via:
CDCServiceImpl::UpdateCdcReplicatedIndex(tablet_id, min_index)
→ tablet_peer->set_cdc_min_replicated_index(min_index)
→ log->set_cdc_min_replicated_index(min_index)
```

---

## Practical Reading Order for Deep Dives

1. `cdc_service.proto` — contract definitions
2. `cdc_service.cc` — `GetChanges` dispatch (~line 1605)
3. `cdcsdk_producer.cc` — `GetChangesForCDCSDK`, `GetConsistentWALRecords`
4. `cdcsdk_virtual_wal.cc` — walsender path
5. `xcluster_producer.cc` — 2DC path
6. `xrepl_catalog_manager.cc` — stream creation
7. `consensus_queue.cc` — WAL read primitives
