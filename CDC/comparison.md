# YugabyteDB vs Kudu: CDC Architecture Comparison

> Side-by-side analysis to guide the Kudu CDC design.

> STATUS (2026-08-03): this is a design-era document. Several "Phase 1 / omit /
> proposed / Gap" notes below have since been implemented (before-images via
> FULL mode, durable checkpoint, server-driven snapshot, stream expiry). For the
> current state see `dev_docs/YB_KUDU_CDC_COMPARISON.md` and `gaps.md`; the
> individual stale cells are corrected inline.

---

## High-Level Comparison

| Aspect | YugabyteDB | Kudu | Notes |
|--------|-----------|------|-------|
| **WAL content** | Txn metadata + intent pointers | Full `WriteRequestPB` with all row ops | Kudu is simpler |
| **Row data source** | WAL + DocDB intents (dual source) | WAL only | Kudu advantage |
| **Before-images** | Available via intents | Reconstructed from MVCC/UNDO in FULL mode | Implemented (2026-08-03) |
| **Schema changes** | DDL in WAL | `ALTER_SCHEMA_OP` in WAL | Same pattern |
| **Transactions** | Distributed intents | `PARTICIPANT_OP` + txn status tablet | Different mechanism, same effect |
| **Checkpoint store** | `cdc_state` YCQL table | Master sys catalog (implemented) | Different implementation |
| **Log retention** | `cdc_min_replicated_index` on `Log` | `LogAnchorRegistry` (exists today) | Different API, same concept |
| **Consumer model** | Pull (GetChanges RPC) | Pull (GetChanges RPC) | Same pattern |
| **Replica isolation** | Leader handles CDC | Leader + optional NON_VOTER | Kudu has option |
| **Multi-tablet consistency** | CDCSDKVirtualWAL | Not needed initially | Simpler with single consumer |
| **Stream format** | PROTO (RowMessage) or WAL | Custom CDCRecordPB (new) | Design choice |
| **Master role** | `XReplCatalogManager` | `CatalogManager` extension | Same concept |

---

## Why Kudu CDC Is Architecturally Simpler

### The YugabyteDB complexity

In YugabyteDB, the Raft WAL carries **transaction-level metadata** but not row data:
- BEGIN, COMMIT, APPLY entries in WAL
- Actual row-level changes live in DocDB intents (RocksDB pending writes)
- `GetChangesForCDCSDK` must read both sources and merge them

This dual-source architecture requires:
- Intent resolution (`GetIntentsForCDC` + `GetIntentsBatchForCDC`)
- Consistent safe time computation across tablets
- Virtual WAL to merge multi-tablet streams for walsender

### The Kudu advantage

Kudu's `WriteOp::NewReplicateMsg` copies the entire `WriteRequestPB` — including all
`RowOperationsPB` — into the Raft REPLICATE entry:

```cpp
// src/kudu/tablet/ops/write_op.cc
void WriteOp::NewReplicateMsg(unique_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(consensus::OperationType::WRITE_OP);
  auto* write_req = (*replicate_msg)->mutable_write_request();
  write_req->CopyFrom(*state()->request());  // full client request embedded
}
```

A CDC reader only needs to tail the WAL and decode `WriteRequestPB.row_operations`.
No cross-referencing with a separate intent store is required.

---

## Methodology Applicability Analysis

| YugabyteDB pattern | Applicable to Kudu? | Notes |
|-------------------|--------------------|----|
| CDCServiceImpl as RPC service on tablet server | ✅ Yes | Register `CDCService` with `TabletServer` |
| Pull-based `GetChanges` RPC | ✅ Yes | Identical pattern |
| Per-tablet stream with OpId checkpoint | ✅ Yes | Kudu's `(term, index)` maps directly |
| Master manages stream lifecycle | ✅ Yes | Extend `CatalogManager` |
| Log retention via anchors | ✅ Yes | `LogAnchorRegistry` exists today |
| WAL reading via log reader | ✅ Yes | `LogReader::ReadReplicatesInRange` |
| Schema change events | ✅ Yes | `ALTER_SCHEMA_OP` in WAL |
| Transaction boundary events | ✅ Yes | `PARTICIPANT_OP` in WAL |
| Read from WAL + intents | ❌ Not needed | Kudu WAL contains everything |
| Virtual WAL for multi-tablet ordering | 🔄 Later | Phase 2 |
| NON_VOTER replica for CDC offload | 🔄 Later | Phase 2 |
| Before-images for UPDATE | Done | FULL mode reconstructs from MVCC/UNDO history (not the WAL) |

---

## Key Differences Requiring Adaptation

### 1. Checkpoint storage

- **YugabyteDB**: `cdc_state` YCQL table (dedicated distributed table)
- **Kudu plan**: Master sys catalog initially (`SysCDCStreamEntryPB`), with per-tablet
  checkpoint persisted on `Checkpoint` RPC

Rationale: Kudu's master sys catalog already provides durable metadata. A dedicated
table (like `cdc_state`) would be cleaner at scale but adds complexity for a first implementation.

### 2. Row decoding

- **YugabyteDB**: Reads DocDB format (RocksDB key/value encoding)
- **Kudu plan**: Decode `RowOperationsPB` using existing `Schema` object

Kudu's row encoding is documented in `row_operations.proto` and already decoded by
`src/kudu/tools/tool_action_common.cc` for `wal dump`. The CDC decoder can reuse this.

### 3. Before-images for UPDATE

- **YugabyteDB**: Available because intents carry full before/after state
- **Kudu**: the WAL has only after-values (the write request)

IMPLEMENTED (2026-08-03): rather than the Phase 2 write-path option originally
sketched here, FULL mode reconstructs before/after images at read time from the
tablet's MVCC/UNDO history at the op timestamp (`ReconstructBeforeAfterImages`),
protected by a CDC history floor so the needed versions are not compacted. CHANGE
mode still emits only changed columns. The original design note follows.

Original plan: for Phase 1, omit before-images (update events carry only changed
columns). Phase 2 option: read before-image from storage at apply time in
`OpDriver::Finalize`, but this adds latency to the write path.

### 4. Multi-tablet consistency

- **YugabyteDB**: `CDCSDKVirtualWAL` merges per-tablet streams into a globally ordered
  virtual log with consistent safe time
- **Kudu plan**: Phase 1 exposes per-tablet streams; consumers handle cross-tablet ordering

Kudu's `KuduClient` already handles tablet routing. A consumer can poll all tablet leaders
independently and merge by HLC timestamp for soft ordering.

---

## Recommended Implementation Strategy

Follow the YugabyteDB CDCSDK design pattern with these Kudu-specific adaptations:

1. **Simpler producer**: WAL-only, no intent cross-referencing
2. **Same RPC pattern**: `GetChanges(stream_id, tablet_id, from_op_id) → [CDCRecordPB]`
3. **Same retention pattern**: Anchor WAL at consumer's checkpoint index
4. **Same lifecycle pattern**: Master creates/deletes/lists streams
5. **Phased delivery**: Per-tablet streams first, virtual WAL merge in Phase 2
