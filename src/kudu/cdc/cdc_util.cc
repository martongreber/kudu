// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/cdc/cdc_util.h"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_bool(cdc_inject_full_reconstruction_failure, false,
            "If true, CDC FULL-mode before/after image reconstruction fails with "
            "an injected error as soon as a batch needs reconstruction. For tests "
            "only: exercises the path where reconstruction fails for a reason "
            "other than history GC or a timeout.");
TAG_FLAG(cdc_inject_full_reconstruction_failure, hidden);
TAG_FLAG(cdc_inject_full_reconstruction_failure, unsafe);
TAG_FLAG(cdc_inject_full_reconstruction_failure, runtime);

using kudu::consensus::ALTER_SCHEMA_OP;
using kudu::consensus::NO_OP;
using kudu::consensus::PARTICIPANT_OP;
using kudu::consensus::ReplicateMsg;
using kudu::consensus::WRITE_OP;
using kudu::tablet::MvccSnapshot;
using kudu::tablet::RowIteratorOptions;
using kudu::tablet::Tablet;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cdc {

namespace {

// Map a RowOperationsPB::Type to a CDCOpTypePB.
// Returns false for non-DML types (split rows, range bounds).
bool MapOpType(RowOperationsPB::Type src, CDCOpTypePB* dst) {
  switch (src) {
    case RowOperationsPB::INSERT:
    case RowOperationsPB::INSERT_IGNORE:
      *dst = CDCOpTypePB::INSERT;
      return true;
    case RowOperationsPB::UPDATE:
    case RowOperationsPB::UPDATE_IGNORE:
      *dst = CDCOpTypePB::UPDATE;
      return true;
    case RowOperationsPB::DELETE:
    case RowOperationsPB::DELETE_IGNORE:
      *dst = CDCOpTypePB::DELETE;
      return true;
    case RowOperationsPB::UPSERT:
    case RowOperationsPB::UPSERT_IGNORE:
      *dst = CDCOpTypePB::UPSERT;
      return true;
    default:
      return false;
  }
}

// Serialize a single typed cell value from raw Kudu on-wire format into 'out'.
// For variable-length types (STRING, BINARY), the value is a Slice pointing
// into indirect_data; we copy the bytes into 'out' directly.
// For fixed-width types, we copy 'type_size' bytes.
void SerializeCellValue(const TypeInfo* type_info,
                        const uint8_t* cell_ptr,
                        string* out) {
  if (type_info->physical_type() == BINARY) {
    // Variable-length: cell_ptr points to a Slice struct.
    const Slice* slice = reinterpret_cast<const Slice*>(cell_ptr);
    out->assign(reinterpret_cast<const char*>(slice->data()), slice->size());
  } else {
    out->assign(reinterpret_cast<const char*>(cell_ptr),
                type_info->size());
  }
}

// Populate CDCColumnValuePBs for an INSERT/UPSERT row (ContiguousRow layout).
// All columns that are set in 'isset_bitmap' are emitted.
Status PopulateInsertColumns(const Schema& schema,
                             const DecodedRowOperation& op,
                             CDCRecordPB* record) {
  const uint8_t* row_data = op.row_data;
  const uint8_t* isset_bitmap = op.isset_bitmap;

  for (int col_idx = 0; col_idx < schema.num_columns(); ++col_idx) {
    if (!BitmapTest(isset_bitmap, col_idx)) {
      continue;  // column not set by this operation
    }

    const ColumnSchema& col = schema.column(col_idx);
    CDCColumnValuePB* cv = record->add_changes();
    cv->set_column_id(schema.column_id(col_idx));
    cv->set_column_name(col.name());

    bool is_null = col.is_nullable() &&
        ContiguousRowHelper::is_null(schema, row_data, col_idx);
    cv->set_is_null(is_null);

    if (!is_null) {
      const uint8_t* cell_ptr =
          ContiguousRowHelper::cell_ptr(schema, row_data, col_idx);
      string value;
      SerializeCellValue(col.type_info(), cell_ptr, &value);
      cv->set_value(std::move(value));
    }
  }
  return Status::OK();
}

// Populate CDCColumnValuePBs for an UPDATE/DELETE row (RowChangeList + key).
// For DELETE: only primary key columns from row_data are emitted.
// For UPDATE: primary key columns from row_data + changed columns from changelist.
Status PopulateUpdateDeleteColumns(const Schema& schema,
                                   const DecodedRowOperation& op,
                                   CDCRecordPB* record) {
  // Primary key columns always come from row_data.
  for (int col_idx = 0; col_idx < schema.num_key_columns(); ++col_idx) {
    const ColumnSchema& col = schema.column(col_idx);
    CDCColumnValuePB* cv = record->add_changes();
    cv->set_column_id(schema.column_id(col_idx));
    cv->set_column_name(col.name());

    bool is_null = col.is_nullable() &&
        ContiguousRowHelper::is_null(schema, op.row_data, col_idx);
    cv->set_is_null(is_null);
    if (!is_null) {
      const uint8_t* cell_ptr =
          ContiguousRowHelper::cell_ptr(schema, op.row_data, col_idx);
      string value;
      SerializeCellValue(col.type_info(), cell_ptr, &value);
      cv->set_value(std::move(value));
    }
  }

  if (op.type == RowOperationsPB::DELETE ||
      op.type == RowOperationsPB::DELETE_IGNORE) {
    return Status::OK();  // DELETE: only key columns needed
  }

  // UPDATE: decode the changelist for non-key columns.
  RowChangeListDecoder decoder(op.changelist);
  RETURN_NOT_OK(decoder.Init());

  while (decoder.HasNext()) {
    RowChangeListDecoder::DecodedUpdate update;
    RETURN_NOT_OK(decoder.DecodeNext(&update));

    int col_idx = -1;
    const void* value_ptr = nullptr;
    RETURN_NOT_OK(update.Validate(schema, &col_idx, &value_ptr));
    if (col_idx < 0) {
      continue;  // column not in this schema version; skip
    }

    const ColumnSchema& col = schema.column(col_idx);
    CDCColumnValuePB* cv = record->add_changes();
    cv->set_column_id(schema.column_id(col_idx));
    cv->set_column_name(col.name());
    cv->set_is_null(update.null);

    if (!update.null && value_ptr != nullptr) {
      string value;
      SerializeCellValue(col.type_info(),
                         reinterpret_cast<const uint8_t*>(value_ptr),
                         &value);
      cv->set_value(std::move(value));
    }
  }
  return Status::OK();
}

// Decode a single WriteRequestPB row at position 'row_idx' into 'record'.
// All decoded rows from the RowOperationsPB are iterated; only the one at
// 'row_idx' is written into 'record'.
Status DecodeWriteOpRowInternal(const Schema& tablet_schema,
                                const RowOperationsPB& row_ops,
                                int target_row_idx,
                                CDCRecordPB* record) {
  Arena arena(256);
  RowOperationsPBDecoder decoder(&row_ops, &tablet_schema, &tablet_schema, &arena);

  vector<DecodedRowOperation> ops;
  RETURN_NOT_OK(decoder.DecodeOperations<WRITE_OPS>(&ops));

  if (target_row_idx >= static_cast<int>(ops.size())) {
    return Status::InvalidArgument("row_idx exceeds number of rows in WriteRequestPB");
  }

  const DecodedRowOperation& op = ops[target_row_idx];

  CDCOpTypePB cdc_op;
  if (!MapOpType(op.type, &cdc_op)) {
    return Status::Aborted("non-DML row operation type");
  }
  record->set_op_type(cdc_op);

  if (op.type == RowOperationsPB::INSERT ||
      op.type == RowOperationsPB::INSERT_IGNORE ||
      op.type == RowOperationsPB::UPSERT ||
      op.type == RowOperationsPB::UPSERT_IGNORE) {
    RETURN_NOT_OK(PopulateInsertColumns(tablet_schema, op, record));
  } else {
    RETURN_NOT_OK(PopulateUpdateDeleteColumns(tablet_schema, op, record));
  }

  return Status::OK();
}

// Emit all columns of a materialized (scanned) row into 'out'.
void PopulateColumnsFromScannedRow(
    const Schema& schema,
    const RowBlockRow& row,
    google::protobuf::RepeatedPtrField<CDCColumnValuePB>* out) {
  out->Clear();
  for (int col_idx = 0; col_idx < schema.num_columns(); ++col_idx) {
    const ColumnSchema& col = schema.column(col_idx);
    CDCColumnValuePB* cv = out->Add();
    cv->set_column_id(schema.column_id(col_idx));
    cv->set_column_name(col.name());
    bool is_null = col.is_nullable() && row.is_null(col_idx);
    cv->set_is_null(is_null);
    if (!is_null) {
      string value;
      SerializeCellValue(col.type_info(), row.cell_ptr(col_idx), &value);
      cv->set_value(std::move(value));
    }
  }
}

// Build the encoded primary-key byte string for a materialized (scanned) row.
// Key columns are the first num_key_columns() columns of 'schema'.
string EncodeScannedRowKey(const Schema& schema,
                           const RowBlockRow& row,
                           Arena* arena) {
  EncodedKeyBuilder builder(&schema, arena);
  for (int i = 0; i < schema.num_key_columns(); ++i) {
    builder.AddColumnKey(row.cell_ptr(i));
  }
  EncodedKey* key = builder.BuildEncodedKey();
  return key->encoded_key().ToString();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

Status DecodeWriteOpAllRows(const ReplicateMsg& replicate,
                            int32_t schema_version,
                            vector<CDCRecordPB>* records) {
  DCHECK(records);

  if (!replicate.has_write_request()) {
    return Status::Corruption("WRITE_OP missing write_request");
  }
  const auto& req = replicate.write_request();

  if (!req.has_schema()) {
    return Status::Corruption("WRITE_OP WriteRequestPB missing schema");
  }
  Schema schema;
  RETURN_NOT_OK(SchemaFromPB(req.schema(), &schema));

  // The RowOperationsPBDecoder requires:
  //   - If client_schema == tablet_schema (same pointer): schema must have column IDs.
  //   - If different pointers: client_schema must NOT have column IDs.
  // The WAL stores the client schema (without IDs in real writes, with IDs in tests).
  // We always add IDs if missing, then pass the same schema as both.
  Schema tablet_schema = schema.has_column_ids()
      ? schema
      : SchemaBuilder(schema).Build();

  if (!req.has_row_operations() || req.row_operations().rows().empty()) {
    return Status::Aborted("WRITE_OP has no row operations");
  }

  // Decode all rows.
  Arena arena(256);
  RowOperationsPBDecoder decoder(&req.row_operations(),
                                 &tablet_schema, &tablet_schema, &arena);
  vector<DecodedRowOperation> ops;
  RETURN_NOT_OK(decoder.DecodeOperations<WRITE_OPS>(&ops));

  for (int i = 0; i < static_cast<int>(ops.size()); ++i) {
    const DecodedRowOperation& op = ops[i];

    CDCOpTypePB cdc_op;
    if (!MapOpType(op.type, &cdc_op)) {
      continue;  // skip non-DML (e.g. split row ops)
    }

    CDCRecordPB record;
    record.set_op_index(replicate.id().index());
    record.set_op_term(replicate.id().term());
    record.set_timestamp(replicate.timestamp());
    record.set_schema_version(schema_version);
    record.set_op_type(cdc_op);

    if (req.has_txn_id()) {
      record.set_txn_id(std::to_string(req.txn_id()));
    }

    if (op.type == RowOperationsPB::INSERT ||
        op.type == RowOperationsPB::INSERT_IGNORE ||
        op.type == RowOperationsPB::UPSERT ||
        op.type == RowOperationsPB::UPSERT_IGNORE) {
      RETURN_NOT_OK(PopulateInsertColumns(tablet_schema, op, &record));
    } else {
      RETURN_NOT_OK(PopulateUpdateDeleteColumns(tablet_schema, op, &record));
    }

    records->emplace_back(std::move(record));
  }

  return Status::OK();
}

Status DecodeNonWriteReplicateMsg(const ReplicateMsg& replicate,
                                  CDCRecordPB* record) {
  DCHECK(record);

  record->set_op_index(replicate.id().index());
  record->set_op_term(replicate.id().term());
  record->set_timestamp(replicate.timestamp());

  switch (replicate.op_type()) {
    case WRITE_OP:
      return Status::Aborted("use DecodeWriteOpAllRows for WRITE_OP");

    case ALTER_SCHEMA_OP: {
      if (!replicate.has_alter_schema_request()) {
        return Status::Corruption("ALTER_SCHEMA_OP missing alter_schema_request");
      }
      const auto& req = replicate.alter_schema_request();
      record->set_op_type(CDCOpTypePB::DDL);
      // 'schema_version' is the version in effect *before* this op (matching its
      // meaning on every other record type), and 'new_schema_version' is the
      // version this ALTER establishes. req.schema_version() is the new version,
      // so the pre-op version is one less. (An ALTER always advances the version,
      // so req.schema_version() >= 1; guard against underflow regardless.)
      const uint32_t new_version = req.schema_version();
      record->set_schema_version(new_version > 0 ? new_version - 1 : 0);
      record->mutable_new_schema()->CopyFrom(req.schema());
      record->set_new_schema_version(new_version);
      return Status::OK();
    }

    case PARTICIPANT_OP: {
      if (!replicate.has_participant_request()) {
        return Status::Corruption("PARTICIPANT_OP missing participant_request");
      }
      const auto& req = replicate.participant_request();
      if (!req.has_op()) {
        return Status::Aborted("PARTICIPANT_OP has no op");
      }
      const auto& op = req.op();
      switch (op.type()) {
        case tserver::ParticipantOpPB::BEGIN_TXN:
          record->set_op_type(CDCOpTypePB::BEGIN);
          break;
        case tserver::ParticipantOpPB::FINALIZE_COMMIT:
          record->set_op_type(CDCOpTypePB::COMMIT);
          break;
        case tserver::ParticipantOpPB::ABORT_TXN:
          record->set_op_type(CDCOpTypePB::ABORT);
          break;
        default:
          return Status::Aborted("PARTICIPANT_OP subtype not relevant for CDC");
      }
      if (op.has_txn_id()) {
        record->set_txn_id(std::to_string(op.txn_id()));
      }
      return Status::OK();
    }

    case NO_OP:
    default:
      return Status::Aborted("op type not relevant for CDC");
  }
}

Status DecodeWriteOpRow(const Schema& schema,
                        const RowOperationsPB& row_ops,
                        int row_idx,
                        CDCRecordPB* record) {
  return DecodeWriteOpRowInternal(schema, row_ops, row_idx, record);
}

Status ReconstructBeforeAfterImages(Tablet* tablet,
                                    consensus::TimeManager* time_manager,
                                    const ReplicateMsg& replicate,
                                    uint64_t effective_timestamp,
                                    const MonoTime& deadline,
                                    vector<CDCRecordPB>* records) {
  DCHECK(tablet);
  DCHECK(time_manager);
  DCHECK(records);
  if (records->empty()) {
    return Status::OK();
  }

  // Does anything in this batch need image reconstruction? INSERT-only batches
  // (all after-image already decoded, no before-image) need no MVCC reads.
  bool needs_reconstruction = false;
  for (const auto& r : *records) {
    if (r.op_type() == CDCOpTypePB::UPDATE ||
        r.op_type() == CDCOpTypePB::DELETE ||
        r.op_type() == CDCOpTypePB::UPSERT) {
      needs_reconstruction = true;
      break;
    }
  }
  if (!needs_reconstruction) {
    return Status::OK();
  }

  if (PREDICT_FALSE(FLAGS_cdc_inject_full_reconstruction_failure)) {
    return Status::IOError("injected FULL reconstruction failure");
  }

  // A committed op is readable from the WAL BEFORE the apply pool applies it to
  // the MemRowSet. Wait until every op with timestamp <= effective_timestamp is
  // safe and applied before scanning, so the after-image scan at
  // MvccSnapshot(effective_timestamp + 1) observes the mutation being
  // reconstructed rather than a stale/empty row. Bounded by 'deadline'. This
  // mirrors the snapshot bootstrap path (CDCServiceImpl::ReadSnapshot) and is the
  // fix for the after-image apply-race that is most acute right after a leader
  // change. Waiting at effective_timestamp + 1 covers both the before-image scan
  // (at effective_timestamp) and the after-image scan (at effective_timestamp + 1).
  const Timestamp wait_ts(effective_timestamp + 1);
  RETURN_NOT_OK(time_manager->WaitUntilSafe(wait_ts, deadline));
  tablet::MvccSnapshot unused;
  RETURN_NOT_OK(tablet->mvcc_manager()->WaitForSnapshotWithAllApplied(
      wait_ts, &unused, deadline));

  if (!replicate.has_write_request() ||
      !replicate.write_request().has_schema()) {
    return Status::Corruption("WRITE_OP missing write_request/schema");
  }
  const auto& req = replicate.write_request();

  // The before-image is read as of just before 'effective_timestamp'. If the
  // MVCC history that far back has already been GC'd, we cannot reconstruct it
  // and must not scan (a time-travel scan below the GC point silently returns
  // the current row as the "before" image -- wrong data with no error).
  //
  // Two guards, both required:
  //  1. The *current* ancient history mark: history below it is reclaimable now.
  //  2. The history-GC water mark: the highest AHM ever actually applied. This
  //     catches the case the current AHM misses -- the CDC floor is re-pinned to
  //     each batch's minimum op timestamp before reconstruction, which can lower
  //     the current AHM back below a point where an earlier GC (run while no CDC
  //     floor protected it, e.g. a stream that lapsed then replayed old ops)
  //     already removed the UNDO history. Without guard 2 that replay passes the
  //     current-AHM check and emits a stale before-image. The water mark is
  //     monotonic, so a legitimately protected read (floor held at GC time, so
  //     GC ran at the clamped-low AHM) keeps the mark low and is not affected.
  Timestamp before_ts(effective_timestamp);
  Timestamp ancient_history_mark;
  if (tablet->GetTabletAncientHistoryMark(&ancient_history_mark) &&
      before_ts < ancient_history_mark) {
    return Status::Incomplete(Substitute(
        "CDC before-image history has been garbage-collected (read ts $0 < "
        "ancient history mark $1)",
        before_ts.ToString(), ancient_history_mark.ToString()));
  }
  Timestamp gc_water_mark = tablet->cdc_history_gc_water_mark();
  if (gc_water_mark.value() > 0 && before_ts < gc_water_mark) {
    return Status::Incomplete(Substitute(
        "CDC before-image history has been garbage-collected (read ts $0 < "
        "history GC water mark $1)",
        before_ts.ToString(), gc_water_mark.ToString()));
  }

  // Project all columns. Pass a column-id-free projection; the iterator maps it
  // onto the tablet's column IDs internally. Since we project every column in
  // schema order, the resulting row layout matches the tablet schema, so the
  // full tablet schema (with IDs) is used to serialize the images.
  const SchemaPtr tablet_schema = tablet->schema();
  Schema scan_projection = tablet_schema->CopyWithoutColumnIds();

  // Re-decode the write op rows so we can recover each row's primary key from
  // its contiguous row buffer. The decoded ops align 1:1 (in order) with the
  // DML records produced by DecodeWriteOpAllRows.
  Schema wal_schema;
  RETURN_NOT_OK(SchemaFromPB(req.schema(), &wal_schema));
  Schema write_schema = wal_schema.has_column_ids()
      ? wal_schema
      : SchemaBuilder(wal_schema).Build();

  Arena arena(1024);
  RowOperationsPBDecoder decoder(&req.row_operations(),
                                 &write_schema, &write_schema, &arena);
  vector<DecodedRowOperation> ops;
  RETURN_NOT_OK(decoder.DecodeOperations<WRITE_OPS>(&ops));

  // Build encoded PK per record and the overall [min, max] scan range.
  // 'record_keys[i]' is the encoded PK string of records->at(i).
  //
  // A single write batch can contain more than one operation on the same
  // primary key (e.g. two UPSERTs, or an INSERT followed by an UPDATE); the
  // tablet applies them sequentially and all of them reach the WAL as separate
  // ops. So one key can map to multiple record indices -- 'key_to_records' maps
  // a key to *every* record that targets it, and the scan below fans its match
  // to all of them. Mapping to a single index would leave the earlier records
  // unmatched, misclassifying an UPSERT to a pre-existing key as an INSERT.
  vector<string> record_keys(records->size());
  unordered_map<string, vector<int>> key_to_records;
  EncodedKey* min_key = nullptr;
  EncodedKey* max_key = nullptr;

  int rec_idx = 0;
  for (const auto& op : ops) {
    CDCOpTypePB cdc_op;
    if (!MapOpType(op.type, &cdc_op)) {
      continue;  // non-DML op; skipped by DecodeWriteOpAllRows too
    }
    if (rec_idx >= static_cast<int>(records->size())) {
      break;
    }
    ConstContiguousRow row(&write_schema, op.row_data);
    EncodedKey* key = EncodedKey::FromContiguousRow(row, &arena);
    string key_str = key->encoded_key().ToString();
    record_keys[rec_idx] = key_str;
    key_to_records[key_str].push_back(rec_idx);
    if (min_key == nullptr || key->encoded_key() < min_key->encoded_key()) {
      min_key = key;
    }
    if (max_key == nullptr || key->encoded_key() > max_key->encoded_key()) {
      max_key = key;
    }
    ++rec_idx;
  }
  if (min_key == nullptr) {
    return Status::OK();  // no DML rows to reconstruct
  }

  // Exclusive upper bound = max_key + 1 (best effort; if the key is already the
  // maximum, fall back to an unbounded upper end).
  EncodedKey* upper_excl = max_key;
  bool have_upper = EncodedKey::IncrementEncodedKey(scan_projection, &upper_excl, &arena).ok();

  // Helper that runs one scan at 'snap' and applies 'apply' to each matched
  // (record index, scanned row) pair.
  auto run_scan = [&](const MvccSnapshot& snap,
                      const std::function<void(int, const RowBlockRow&)>& apply)
      -> Status {
    RowIteratorOptions opts;
    opts.projection = &scan_projection;
    opts.snap_to_include = snap;
    opts.order = ORDERED;
    opts.include_deleted_rows = false;
    unique_ptr<RowwiseIterator> iter;
    RETURN_NOT_OK(tablet->NewRowIterator(std::move(opts), &iter));

    ScanSpec spec;
    spec.SetLowerBoundKey(min_key);
    if (have_upper) {
      spec.SetExclusiveUpperBoundKey(upper_excl);
    }
    RETURN_NOT_OK(iter->Init(&spec));

    const Schema& iter_schema = iter->schema();
    RowBlockMemory mem(1024);
    RowBlock block(&iter_schema, 256, &mem);
    Arena key_arena(256);
    while (iter->HasNext()) {
      RETURN_NOT_OK(iter->NextBlock(&block));
      for (size_t i = 0; i < block.nrows(); ++i) {
        if (!block.selection_vector()->IsRowSelected(i)) {
          continue;
        }
        RowBlockRow row = block.row(i);
        key_arena.Reset();
        string key_str = EncodeScannedRowKey(iter_schema, row, &key_arena);
        auto it = key_to_records.find(key_str);
        if (it != key_to_records.end()) {
          // Fan the single scanned row to every record targeting this key, so
          // multiple ops on the same PK in one batch are all matched.
          for (int idx : it->second) {
            apply(idx, row);
          }
        }
      }
    }
    return Status::OK();
  };

  // Remember the original op type of each record (before any UPSERT
  // reclassification) so the after-image scan can decide which records to fill.
  vector<CDCOpTypePB> original_op(records->size());
  for (size_t i = 0; i < records->size(); ++i) {
    original_op[i] = (*records)[i].op_type();
  }

  // Before-image scan at MvccSnapshot(effective_timestamp): populate old_changes
  // for UPDATE/DELETE/UPSERT records. Track which matched so UPSERTs with no
  // pre-existing row can be reclassified to INSERT.
  vector<bool> matched_before(records->size(), false);
  RETURN_NOT_OK(run_scan(
      MvccSnapshot(before_ts),
      [&](int idx, const RowBlockRow& row) {
        CDCOpTypePB op = original_op[idx];
        if (op == CDCOpTypePB::UPDATE ||
            op == CDCOpTypePB::DELETE ||
            op == CDCOpTypePB::UPSERT) {
          matched_before[idx] = true;
          PopulateColumnsFromScannedRow(
              *tablet_schema, row, (*records)[idx].mutable_old_changes());
        }
      }));

  // Reclassify UPSERTs: matched -> UPDATE, unmatched (new row) -> INSERT.
  for (size_t i = 0; i < records->size(); ++i) {
    if (original_op[i] == CDCOpTypePB::UPSERT) {
      (*records)[i].set_op_type(
          matched_before[i] ? CDCOpTypePB::UPDATE : CDCOpTypePB::INSERT);
    }
  }

  // After-image scan at MvccSnapshot(effective_timestamp + 1): replace 'changes'
  // with the complete row for UPDATE/UPSERT records. DELETE keeps key-only
  // 'changes'; original INSERTs keep their decoded after-image.
  RETURN_NOT_OK(run_scan(
      MvccSnapshot(Timestamp(effective_timestamp + 1)),
      [&](int idx, const RowBlockRow& row) {
        CDCOpTypePB op = original_op[idx];
        if (op == CDCOpTypePB::UPDATE || op == CDCOpTypePB::UPSERT) {
          PopulateColumnsFromScannedRow(
              *tablet_schema, row, (*records)[idx].mutable_changes());
        }
      }));

  return Status::OK();
}

Status PopulateReadRecord(const Schema& schema,
                          const RowBlockRow& row,
                          CDCRecordPB* record) {
  PopulateColumnsFromScannedRow(schema, row, record->mutable_changes());
  return Status::OK();
}

std::string SerializeSnapshotKey(const Schema& schema,
                                 const RowBlockRow& row,
                                 Arena* arena) {
  return EncodeScannedRowKey(schema, row, arena);
}

} // namespace cdc
} // namespace kudu
