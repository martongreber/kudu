# Kudu Architecture for CDC

> Research notes describing Kudu's relevant internals for implementing CDC.

## Overview

Kudu has **no CDC implementation** today. This document maps the existing infrastructure
that makes CDC feasible without major restructuring.

---

## Write Path Summary

```
Client Write RPC
  → TabletServiceImpl::Write           (src/kudu/tserver/tablet_service.cc)
  → TabletReplica::SubmitWrite         (src/kudu/tablet/tablet_replica.cc)
  → WriteOp + OpDriver (LEADER)
  → Prepare: decode RowOperationsPB, acquire locks
  → Start: assign hybrid-logical timestamp (MVCC)
  → RaftConsensus::Replicate
      → WAL REPLICATE entry (full WriteRequestPB)  ← CDC source
  → Majority committed
  → Apply: mutate MemRowSet / delta stores
  → WAL COMMIT entry (TxResultPB)
  → Finalize: make MVCC visible, respond to client
```

**Key insight**: `WriteOp::NewReplicateMsg` copies the entire `WriteRequestPB` into the
Raft message. The WAL contains every logical mutation with its schema version and timestamp.
This is simpler than YugabyteDB, which must cross-reference WAL + DocDB intents.

---

## WAL Structure

### On-disk format (from `log.proto`)

```
LogSegmentHeaderPB
  tablet_id        - identifies the tablet
  sequence_number  - monotonic segment counter
  schema           - SchemaPB at segment creation time
  schema_version   - uint32

[repeated LogEntryBatchPB]
  [repeated LogEntryPB]
    type = REPLICATE | COMMIT | FLUSH_MARKER
    replicate = ReplicateMsg  (for REPLICATE entries)
    commit    = CommitMsg     (for COMMIT entries)

LogSegmentFooterPB
  num_entries
  min_replicate_index, max_replicate_index
  close_timestamp_micros
```

### ReplicateMsg fields (CDC-relevant) (`consensus.proto`)

```proto
message ReplicateMsg {
  required OpId id = 1;               // (term, index) — ordering key
  required fixed64 timestamp = 2;     // hybrid logical clock
  required OperationType op_type = 3; // WRITE_OP, ALTER_SCHEMA_OP, etc.
  optional WriteRequestPB write_request = 4;          // full row operations
  optional AlterSchemaRequestPB alter_schema_request = 5;
  optional ChangeConfigRecordPB change_config_record = 6;
  optional ParticipantOpPB participant_request = 7;   // txn participant ops
}
```

### Operation types

| Type | CDC relevance |
|------|-------------|
| `WRITE_OP` | All INSERT/UPDATE/DELETE/UPSERT — primary CDC source |
| `ALTER_SCHEMA_OP` | Schema change events — must be emitted as DDL records |
| `PARTICIPANT_OP` | Transaction BEGIN/COMMIT/ABORT boundaries |
| `CHANGE_CONFIG_OP` | Raft membership — skip for CDC |
| `NO_OP` | Leadership no-op — skip for CDC |

---

## Row Operations (`row_operations.proto`)

```proto
message RowOperationsPB {
  enum Type {
    INSERT = 1; UPDATE = 2; DELETE = 3; UPSERT = 5;
    INSERT_IGNORE = 10; UPDATE_IGNORE = 11;
    DELETE_IGNORE = 12; UPSERT_IGNORE = 13;
    // partition/split types: not relevant to CDC
  }
  optional bytes rows = 2;          // packed row data
  optional bytes indirect_data = 3; // string/binary payloads
}
```

Encoding:
- One byte op type
- Column isset bitmap (one bit per schema column)
- Null bitmap (one bit per nullable column, if any nullable)
- Column data in schema order, for set and non-null columns
- String/binary data offsets into `indirect_data`

---

## Existing WAL Access Infrastructure

### `LogReader` (`src/kudu/consensus/log_reader.h`)

```cpp
// Read a range of replicates by log index
Status ReadReplicatesInRange(
    int64_t starting_at,
    int64_t up_to,
    int64_t max_bytes_to_read,
    std::vector<consensus::ReplicateMsg*>* replicates) const;

// Look up OpId for a log index
Status LookupOpId(int64_t op_index, consensus::OpId* op_id) const;
```

`LogReader` uses `LogIndex` (OpId → file offset) for efficient seeks into WAL segments.

### `LogCache` (`src/kudu/consensus/log_cache.h`)

In-memory write-through cache of recent `ReplicateMsg` entries. CDC can read hot
recent entries from the cache without disk I/O. Falls back to `LogReader` for older entries.

### `LogIndex` (`src/kudu/consensus/log_index.h`)

Maps log index → `{segment_sequence_number, offset_in_segment}`. Required for
`ReadReplicatesInRange`. Shared between `Log` and `LogReader`.

---

## Log Retention: `LogAnchorRegistry`

(`src/kudu/consensus/log_anchor_registry.h`)

Callers register anchors to prevent WAL segment GC:

```cpp
class LogAnchorRegistry {
  // Pin WAL at log_index and above
  void Register(int64_t log_index, const std::string& owner, LogAnchor* anchor);

  // Advance the anchor (e.g. as consumer checkpoint moves forward)
  Status RegisterOrUpdate(int64_t log_index, const std::string& owner, LogAnchor* anchor);

  // Release when stream is deleted or caught up
  Status Unregister(LogAnchor* anchor);
};
```

`TabletReplica::RunLogGC` calls `log_anchor_registry_->GetEarliestRegisteredLogIndex()`
to determine the minimum safe GC point. CDC must register an anchor per (stream, tablet)
at the stream's checkpoint index.

---

## Tablet Storage Model

- **MemRowSet**: in-memory B-tree for new inserts; updates stored as mutation chains
- **DiskRowSet**: columnar `cfile` blocks for base data
- **Delta stores**: `deltamemstore`, `deltafile` for updates/deletes to existing rows
- **MVCC**: `MvccSnapshot` determines visible rows at a read timestamp
- **Compaction**: merges rowsets and deltas; history can be GC'd

**Important**: The storage layer is **not** append-only. Only the WAL provides an ordered,
append-only record of changes. CDC must read from the WAL, not from storage.

---

## Schema Change Handling

Schema changes are `ALTER_SCHEMA_OP` entries in the WAL:
- Contains `AlterSchemaRequestPB` with the new `SchemaPB` and `schema_version`
- Log segment headers record the schema at segment creation time
- Mid-segment schema changes are possible (known limitation for tools: KUDU-515)

CDC must:
1. Track the `schema_version` associated with each REPLICATE entry
2. Emit a DDL event when `ALTER_SCHEMA_OP` is encountered
3. Use the correct schema for decoding subsequent `WRITE_OP` entries

---

## Transactions

Kudu has multi-tablet transactions:
- `txn_id` field in `WriteRequestPB`
- `PARTICIPANT_OP` in WAL for transaction state machine (BEGIN/COMMIT/ABORT)
- Transaction status tablet manages global state

For CDC phase 1: emit changes only after observing COMMIT for a given `txn_id`.
Non-transactional writes (most common case) are directly visible after REPLICATE+COMMIT.

---

## Master Server Role

The master manages:
- Table/tablet metadata in sys catalog
- Tablet placement and replica assignment
- Schema versions via `AlterTable`
- Tablet leadership (via heartbeats)

For CDC, the master will additionally manage:
- Stream registry (`SysCDCStreamEntryPB` in sys catalog)
- Per-tablet checkpoint persistence (initially in master memory / sys catalog)
- Stream lifecycle RPCs (`CreateCDCStream`, `DeleteCDCStream`, `ListCDCStreams`)

---

## Replica Types: NON_VOTER / LEARNER

`RaftPeerPB` member types:
- `VOTER`: participates in elections and majority quorum
- `NON_VOTER` / `LEARNER`: receives WAL replication but doesn't vote

A NON_VOTER replica can serve CDC `GetChanges` requests without impacting leader
throughput or majority quorum. This is a Phase 2 optimization.

---

## Key Files for CDC Implementation

| Component | Key Files |
|----------|-----------|
| WAL reading | `src/kudu/consensus/log_reader.{h,cc}`, `log_cache.{h,cc}`, `log_index.{h,cc}` |
| WAL retention | `src/kudu/consensus/log_anchor_registry.{h,cc}` |
| Row decoding | `src/kudu/common/row_operations.{h,cc}`, `src/kudu/tools/tool_action_common.cc` |
| Write path | `src/kudu/tablet/ops/write_op.{h,cc}`, `op_driver.{h,cc}` |
| TabletReplica | `src/kudu/tablet/tablet_replica.{h,cc}` |
| TabletServer | `src/kudu/tserver/tablet_server.{h,cc}` |
| Master catalog | `src/kudu/master/catalog_manager.{h,cc}`, `master.proto` |
| Proto definitions | `src/kudu/consensus/consensus.proto`, `log.proto`, `tserver/tserver.proto` |

---

## WAL Dump Reference

`src/kudu/tools/tool_action_common.cc` contains `PrintDecoded()` — a reference
implementation that reads WAL segments and decodes `WRITE_OP` / `ALTER_SCHEMA_OP`
entries. The CDC row decoder can reuse this logic directly.
