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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
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
#include "kudu/gutil/port.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
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

DEFINE_bool(cdc_inject_full_reconstruction_timeout, false,
            "If true, CDC FULL-mode before/after image reconstruction times out "
            "as soon as a batch needs reconstruction, as when the ops it waits "
            "for are not applied in time after a leader change. For tests only.");
TAG_FLAG(cdc_inject_full_reconstruction_timeout, hidden);
TAG_FLAG(cdc_inject_full_reconstruction_timeout, unsafe);
TAG_FLAG(cdc_inject_full_reconstruction_timeout, runtime);

DEFINE_int64(cdc_inject_full_reconstruction_timeout_at_op_index, -1,
             "If not negative, CDC FULL-mode before/after image reconstruction "
             "of the WRITE_OP at this op index times out, as when the read "
             "deadline expires. For tests only.");
TAG_FLAG(cdc_inject_full_reconstruction_timeout_at_op_index, hidden);
TAG_FLAG(cdc_inject_full_reconstruction_timeout_at_op_index, unsafe);
TAG_FLAG(cdc_inject_full_reconstruction_timeout_at_op_index, runtime);

using kudu::consensus::ALTER_SCHEMA_OP;
using kudu::consensus::NO_OP;
using kudu::consensus::PARTICIPANT_OP;
using kudu::consensus::ReplicateMsg;
using kudu::consensus::WRITE_OP;
using kudu::tablet::MvccSnapshot;
using kudu::tablet::OperationResultPB;
using kudu::tablet::RowIteratorOptions;
using kudu::tablet::Tablet;
using kudu::tablet::TxResultPB;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cdc {

namespace {

// See SetReconstructImagesHookForTests().
simple_spinlock reconstruct_images_hook_lock;
std::function<void()>* reconstruct_images_hook = nullptr;

void RunReconstructImagesHookForTests() {
  std::function<void()> hook;
  {
    std::lock_guard l(reconstruct_images_hook_lock);
    if (PREDICT_TRUE(reconstruct_images_hook == nullptr)) {
      return;
    }
    hook = *reconstruct_images_hook;
  }
  hook();
}

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
// An INSERT emits every column of the row, including those the tablet filled
// with their defaults; an UPSERT only the columns it set, as it applies only
// those to an existing row.
Status PopulateInsertColumns(const Schema& schema,
                             const DecodedRowOperation& op,
                             CDCRecordPB* record) {
  const uint8_t* row_data = op.row_data;
  const bool all_columns = op.type == RowOperationsPB::INSERT ||
                           op.type == RowOperationsPB::INSERT_IGNORE;

  for (int col_idx = 0; col_idx < schema.num_columns(); ++col_idx) {
    if (!all_columns && !BitmapTest(op.isset_bitmap, col_idx)) {
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

// Decodes the row operations of the WRITE_OP request 'req' into 'ops', the way
// the tablet did: the request's (client) schema is mapped by column name onto
// 'schema', the tablet's schema the op was applied with, whose column ids,
// defaults and attributes (such as immutability) apply. 'schema' must have
// column ids and outlive 'ops'. The leader records its auto-incrementing counter
// in the REPLICATE so that replicas assign the same auto_incrementing_id values;
// the decoder is seeded with a copy of it so it regenerates exactly those ids.
Status DecodeWriteRequest(const tserver::WriteRequestPB& req,
                          const Schema& schema,
                          Arena* arena,
                          vector<DecodedRowOperation>* ops) {
  DCHECK(schema.has_column_ids());
  Schema client_schema;
  RETURN_NOT_OK(SchemaFromPB(req.schema(), &client_schema));
  if (client_schema.has_column_ids()) {
    // The tablet refuses such a write, but REPLICATEs built by tests have them.
    client_schema = client_schema.CopyWithoutColumnIds();
  }

  int64_t auto_incrementing_counter = -1;
  if (schema.has_auto_incrementing()) {
    if (!req.has_auto_incrementing_column()) {
      return Status::Corruption(
          "WRITE_OP on a table with an auto-incrementing column is missing the "
          "auto-incrementing counter");
    }
    auto_incrementing_counter =
        req.auto_incrementing_column().auto_incrementing_counter();
  }
  RowOperationsPBDecoder decoder(&req.row_operations(), &client_schema, &schema, arena);
  return decoder.DecodeOperations<WRITE_OPS>(ops, &auto_incrementing_counter);
}

// Returns Corruption unless 'result', an op's outcome from its COMMIT entry,
// has one element per decoded row of the op.
Status CheckResultMatchesRows(const TxResultPB& result, size_t num_rows) {
  if (PREDICT_FALSE(static_cast<size_t>(result.ops_size()) != num_rows)) {
    return Status::Corruption(Substitute(
        "WRITE_OP outcome has $0 row results for $1 rows", result.ops_size(), num_rows));
  }
  return Status::OK();
}

// Sets '*produces' to whether the decoded row 'op' produces a CDC record, and
// if so, 'cdc_op' to its CDC type. 'op_result' is the row's outcome from the
// op's COMMIT entry. Only rows the tablet changed produce a record: a failed
// row has a failed_status, and a row that changed nothing (an ignored *_IGNORE
// row, or an UPSERT of only the key of an existing row) has no mutated store.
// The outcome survives a restart: tablet bootstrap rewrites COMMIT entries with
// the original failed_status and mutated stores. Returns Corruption if the
// tablet applied a row that the CDC decode rejected, because its record cannot
// be reproduced. DecodeWriteOpAllRows and ReconstructBeforeAfterImages both use
// this, so their records stay aligned.
Status RowProducesRecord(const DecodedRowOperation& op,
                         const OperationResultPB& op_result,
                         bool* produces,
                         CDCOpTypePB* cdc_op) {
  *produces = false;
  if (!MapOpType(op.type, cdc_op) || op_result.has_failed_status() ||
      op_result.mutated_stores().empty()) {
    return Status::OK();
  }
  if (PREDICT_FALSE(!op.result.ok())) {
    return op.result.CloneAndPrepend("row applied by the tablet cannot be decoded");
  }
  *produces = true;
  return Status::OK();
}

// Returns true if the INSERT/UPSERT row 'op' sets an immutable column.
bool SetsImmutableColumn(const Schema& schema, const DecodedRowOperation& op) {
  for (int col_idx = 0; col_idx < schema.num_columns(); ++col_idx) {
    if (schema.column(col_idx).is_immutable() && BitmapTest(op.isset_bitmap, col_idx)) {
      return true;
    }
  }
  return false;
}

// Sets 'cv' to the value of column 'col', whose id is 'col_id': NULL if
// 'is_null', else the cell at 'cell'.
void SetColumnValue(const ColumnSchema& col,
                    ColumnId col_id,
                    bool is_null,
                    const void* cell,
                    CDCColumnValuePB* cv) {
  cv->Clear();
  cv->set_column_id(col_id);
  cv->set_column_name(col.name());
  cv->set_is_null(is_null);
  if (!is_null) {
    string value;
    SerializeCellValue(col.type_info(), static_cast<const uint8_t*>(cell), &value);
    cv->set_value(std::move(value));
  }
}

// Emit all columns of a materialized (scanned) row into 'out'.
void PopulateColumnsFromScannedRow(
    const Schema& schema,
    const RowBlockRow& row,
    google::protobuf::RepeatedPtrField<CDCColumnValuePB>* out) {
  out->Clear();
  for (int col_idx = 0; col_idx < schema.num_columns(); ++col_idx) {
    const ColumnSchema& col = schema.column(col_idx);
    SetColumnValue(col, schema.column_id(col_idx), col.is_nullable() && row.is_null(col_idx),
                   row.cell_ptr(col_idx), out->Add());
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

// The complete image of a row in FULL mode: one value for each column of the
// image schema, in its order.
typedef google::protobuf::RepeatedPtrField<CDCColumnValuePB> RowImage;

// Returns true if 'a' and 'b' are the same row image.
bool SameImage(const RowImage& a, const RowImage& b) {
  return std::equal(a.begin(), a.end(), b.begin(), b.end(),
                    [](const CDCColumnValuePB& x, const CDCColumnValuePB& y) {
                      return x.column_id() == y.column_id() && x.is_null() == y.is_null() &&
                             x.value() == y.value();
                    });
}

// Sets 'image' to the row that the INSERT, or UPSERT of a new row, 'op' wrote.
// 'op' was decoded with 'op_schema'. A column that was added since has its
// read default, as when the row is read.
void ImageFromInsert(const Schema& image_schema,
                     const Schema& op_schema,
                     const DecodedRowOperation& op,
                     RowImage* image) {
  image->Clear();
  ConstContiguousRow row(&op_schema, op.row_data);
  for (int i = 0; i < image_schema.num_columns(); ++i) {
    const ColumnSchema& col = image_schema.column(i);
    const ColumnId col_id = image_schema.column_id(i);
    const int op_idx = op_schema.find_column_by_id(col_id);
    if (op_idx == Schema::kColumnNotFound) {
      SetColumnValue(col, col_id, col.read_default_value() == nullptr,
                     col.read_default_value(), image->Add());
    } else {
      SetColumnValue(col, col_id,
                     op_schema.column(op_idx).is_nullable() && row.is_null(op_idx),
                     row.cell_ptr(op_idx), image->Add());
    }
  }
}

// Applies the UPSERT 'op' of an existing row, decoded with 'op_schema', to the
// row's 'image', as the tablet does: it sets the non-key columns the operation
// set, except that an UPSERT_IGNORE leaves the immutable ones unchanged (an
// UPSERT that sets one fails instead).
void ApplyUpsertToImage(const Schema& image_schema,
                        const Schema& op_schema,
                        const DecodedRowOperation& op,
                        RowImage* image) {
  ConstContiguousRow row(&op_schema, op.row_data);
  for (int op_idx = op_schema.num_key_columns(); op_idx < op_schema.num_columns(); ++op_idx) {
    const ColumnSchema& op_col = op_schema.column(op_idx);
    if (!BitmapTest(op.isset_bitmap, op_idx) || op_col.is_immutable()) {
      continue;
    }
    const int idx = image_schema.find_column_by_id(op_schema.column_id(op_idx));
    if (idx == Schema::kColumnNotFound) {
      continue;  // dropped since
    }
    SetColumnValue(image_schema.column(idx), image_schema.column_id(idx),
                   op_col.is_nullable() && row.is_null(op_idx), row.cell_ptr(op_idx),
                   image->Mutable(idx));
  }
}

// Applies the changes 'changelist' of an UPDATE to the row's 'image'.
Status ApplyChangeListToImage(const Schema& image_schema,
                              const RowChangeList& changelist,
                              RowImage* image) {
  RowChangeListDecoder decoder(changelist);
  RETURN_NOT_OK(decoder.Init());
  while (decoder.HasNext()) {
    RowChangeListDecoder::DecodedUpdate update;
    RETURN_NOT_OK(decoder.DecodeNext(&update));
    int idx = -1;
    const void* value = nullptr;
    RETURN_NOT_OK(update.Validate(image_schema, &idx, &value));
    if (idx < 0) {
      continue;  // dropped since
    }
    SetColumnValue(image_schema.column(idx), image_schema.column_id(idx), update.null, value,
                   image->Mutable(idx));
  }
  return Status::OK();
}

// Reads the row with the primary key 'key' (encoded as 'key_str') as of 'snap'
// into 'image', in the columns of 'image_schema', and sets 'found' to whether
// it exists. 'projection' is 'image_schema' without column ids. Adds the
// number of rows read to 'rows_read'. Returns ServiceUnavailable if the
// tablet's schema is no longer 'image_schema'.
Status LookupRow(Tablet* tablet,
                 const Schema& image_schema,
                 const Schema& projection,
                 const MvccSnapshot& snap,
                 EncodedKey* key,
                 const string& key_str,
                 Arena* arena,
                 bool* found,
                 RowImage* image,
                 int64_t* rows_read) {
  *found = false;
  RowIteratorOptions opts;
  opts.projection = &projection;
  opts.snap_to_include = snap;
  opts.include_deleted_rows = false;
  // An iterator that cannot be made or initialized because a column of
  // 'projection' was dropped since 'image_schema' was read.
  const auto schema_changed = [&](const Status& s) {
    if (s.IsInvalidArgument() && *tablet->schema() != image_schema) {
      return Status::ServiceUnavailable(
          "the tablet schema changed while CDC was reading row images; retry", s.ToString());
    }
    return s;
  };
  unique_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(schema_changed(tablet->NewRowIterator(std::move(opts), &iter)));

  // Read only the key's range. A key that cannot be incremented is the largest
  // possible one, so nothing follows it.
  ScanSpec spec;
  spec.SetLowerBoundKey(key);
  EncodedKey* upper = key;
  if (EncodedKey::IncrementEncodedKey(image_schema, &upper, arena).ok()) {
    spec.SetExclusiveUpperBoundKey(upper);
  }
  RETURN_NOT_OK(schema_changed(iter->Init(&spec)));

  const Schema& iter_schema = iter->schema();
  bool same_schema = iter_schema.num_columns() == image_schema.num_columns();
  for (int i = 0; same_schema && i < image_schema.num_columns(); ++i) {
    same_schema = iter_schema.column_id(i) == image_schema.column_id(i);
  }
  if (PREDICT_FALSE(!same_schema)) {
    return Status::ServiceUnavailable(
        "the tablet schema changed while CDC was reading row images; retry");
  }

  RowBlockMemory mem(1024);
  RowBlock block(&iter_schema, 16, &mem);
  Arena key_arena(256);
  while (iter->HasNext()) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    for (size_t i = 0; i < block.nrows(); ++i) {
      if (!block.selection_vector()->IsRowSelected(i)) {
        continue;
      }
      ++*rows_read;
      RowBlockRow row = block.row(i);
      key_arena.Reset();
      if (EncodeScannedRowKey(iter_schema, row, &key_arena) == key_str) {
        *found = true;
        PopulateColumnsFromScannedRow(image_schema, row, image);
      }
    }
  }
  return Status::OK();
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

Status DecodeWriteOpAllRows(const ReplicateMsg& replicate,
                            const TxResultPB& result,
                            int32_t schema_version,
                            const Schema& schema,
                            vector<CDCRecordPB>* records) {
  DCHECK(records);

  if (!replicate.has_write_request()) {
    return Status::Corruption("WRITE_OP missing write_request");
  }
  const auto& req = replicate.write_request();

  if (!req.has_schema()) {
    return Status::Corruption("WRITE_OP WriteRequestPB missing schema");
  }
  if (!req.has_row_operations() || req.row_operations().rows().empty()) {
    return Status::Aborted("WRITE_OP has no row operations");
  }

  // Decode all rows.
  Arena arena(256);
  vector<DecodedRowOperation> ops;
  RETURN_NOT_OK(DecodeWriteRequest(req, schema, &arena, &ops));
  RETURN_NOT_OK(CheckResultMatchesRows(result, ops.size()));

  for (int i = 0; i < result.ops_size(); ++i) {
    const DecodedRowOperation& op = ops[i];
    bool produces;
    CDCOpTypePB cdc_op;
    RETURN_NOT_OK_PREPEND(RowProducesRecord(op, result.ops(i), &produces, &cdc_op),
                          Substitute("row $0", i));
    if (!produces) {
      continue;
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
      RETURN_NOT_OK(PopulateInsertColumns(schema, op, &record));
      // On an existing row the tablet applies an UPSERT_IGNORE as an update
      // that skips immutable columns. Whether the row existed is not known
      // here, so tell the sink not to overwrite them.
      if (op.type == RowOperationsPB::UPSERT_IGNORE &&
          SetsImmutableColumn(schema, op)) {
        record.set_keep_immutable_columns_on_update(true);
      }
    } else {
      RETURN_NOT_OK(PopulateUpdateDeleteColumns(schema, op, &record));
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
      // version this ALTER establishes.
      const uint32_t new_version = req.schema_version();
      record->set_schema_version(PriorSchemaVersion(req));
      record->mutable_new_schema()->CopyFrom(req.schema());
      record->set_new_schema_version(new_version);
      if (req.has_new_table_name()) {
        record->set_new_table_name(req.new_table_name());
      }
      if (req.has_new_extra_config()) {
        *record->mutable_new_extra_config() = req.new_extra_config();
      }
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

uint32_t PriorSchemaVersion(const tserver::AlterSchemaRequestPB& req) {
  if (req.has_prior_schema_version()) {
    return req.prior_schema_version();
  }
  // An ALTER always advances the version, so req.schema_version() >= 1; guard
  // against underflow regardless.
  return req.schema_version() > 0 ? req.schema_version() - 1 : 0;
}

Status ReconstructBeforeAfterImages(Tablet* tablet,
                                    consensus::TimeManager* time_manager,
                                    const ReplicateMsg& replicate,
                                    const TxResultPB& result,
                                    const Schema& op_schema,
                                    uint64_t effective_timestamp,
                                    const MonoTime& deadline,
                                    vector<CDCRecordPB>* records,
                                    int64_t* rows_read) {
  DCHECK(tablet);
  DCHECK(time_manager);
  DCHECK(records);
  DCHECK(rows_read);
  if (records->empty()) {
    return Status::OK();
  }

  // An op of only INSERTs needs no reads: there is no before-image, and its
  // records already carry the complete rows, in the columns of the op's schema.
  bool needs_reconstruction = false;
  for (const auto& r : *records) {
    if (r.op_type() != CDCOpTypePB::INSERT) {
      needs_reconstruction = true;
      break;
    }
  }
  if (!needs_reconstruction) {
    for (auto& r : *records) {
      r.set_image_schema_version(r.schema_version());
    }
    return Status::OK();
  }

  if (PREDICT_FALSE(FLAGS_cdc_inject_full_reconstruction_failure)) {
    return Status::IOError("injected FULL reconstruction failure");
  }
  if (PREDICT_FALSE(FLAGS_cdc_inject_full_reconstruction_timeout ||
                    FLAGS_cdc_inject_full_reconstruction_timeout_at_op_index ==
                        replicate.id().index())) {
    return Status::TimedOut("injected FULL reconstruction timeout");
  }

  // A committed op is readable from the WAL BEFORE the apply pool applies it to
  // the MemRowSet. Wait until every op with timestamp <= effective_timestamp is
  // safe and applied before reading, so the reads at
  // MvccSnapshot(effective_timestamp + 1) observe the mutation being
  // reconstructed rather than a stale/empty row. Bounded by 'deadline'. This
  // mirrors the snapshot bootstrap path (CDCServiceImpl::ReadSnapshot) and is
  // the fix for the after-image apply-race that is most acute right after a
  // leader change. Waiting at effective_timestamp + 1 covers both the
  // before-image reads (at effective_timestamp) and the after-image reads (at
  // effective_timestamp + 1).
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
  // and must not read (a time-travel read below the GC point silently returns
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
  //
  // Both guards are checked again after each read of the row images. The floor
  // that protects this batch can be cleared while the images are read (a CDC
  // release valve opening, or a master release), and a GC can then remove the
  // history the reads need: the reads would see a newer row version than the
  // one at 'before_ts', and nothing else would tell. A GC raises the water mark
  // before it removes any history, and a read that sees the removal is ordered
  // after it, so the recheck after the read catches it.
  const Timestamp before_ts(effective_timestamp);
  const auto check_history = [&]() {
    Timestamp ancient_history_mark;
    if (tablet->GetTabletAncientHistoryMark(&ancient_history_mark) &&
        before_ts < ancient_history_mark) {
      return Status::Incomplete(Substitute(
          "CDC before-image history has been garbage-collected (read ts $0 < "
          "ancient history mark $1)",
          before_ts.ToString(), ancient_history_mark.ToString()));
    }
    const Timestamp gc_water_mark = tablet->cdc_history_gc_water_mark();
    if (gc_water_mark.value() > 0 && before_ts < gc_water_mark) {
      return Status::Incomplete(Substitute(
          "CDC before-image history has been garbage-collected (read ts $0 < "
          "history GC water mark $1)",
          before_ts.ToString(), gc_water_mark.ToString()));
    }
    return Status::OK();
  };
  RETURN_NOT_OK(check_history());
  RunReconstructImagesHookForTests();

  // The images are read with the tablet's current schema, so they carry its
  // columns: a column dropped since the op is missing, and one added since has
  // its default.
  SchemaPtr image_schema;
  uint32_t image_schema_version;
  tablet->metadata()->GetSchemaAndVersion(&image_schema, &image_schema_version);
  const Schema projection = image_schema->CopyWithoutColumnIds();

  // Re-decode the write op rows. The decoded ops that produce a record align 1:1
  // (in order) with the records produced by DecodeWriteOpAllRows.
  Arena arena(1024);
  vector<DecodedRowOperation> ops;
  RETURN_NOT_OK(DecodeWriteRequest(req, op_schema, &arena, &ops));
  RETURN_NOT_OK(CheckResultMatchesRows(result, ops.size()));

  // The row of each record, and the distinct primary keys of the rows. One op
  // can change the same row more than once (e.g. two UPSERTs, or an INSERT and
  // then an UPDATE): the tablet applies them in order.
  vector<const DecodedRowOperation*> record_ops;
  vector<size_t> record_keys;
  vector<EncodedKey*> keys;
  vector<string> key_strs;
  unordered_map<string, size_t> key_indexes;
  record_ops.reserve(records->size());
  record_keys.reserve(records->size());
  for (int i = 0; i < result.ops_size(); ++i) {
    const DecodedRowOperation& op = ops[i];
    bool produces;
    CDCOpTypePB cdc_op;
    RETURN_NOT_OK(RowProducesRecord(op, result.ops(i), &produces, &cdc_op));
    if (!produces) {
      continue;  // skipped by DecodeWriteOpAllRows too
    }
    EncodedKey* key =
        EncodedKey::FromContiguousRow(ConstContiguousRow(&op_schema, op.row_data), &arena);
    string key_str = key->encoded_key().ToString();
    const auto emplaced = key_indexes.emplace(key_str, keys.size());
    if (emplaced.second) {
      keys.push_back(key);
      key_strs.emplace_back(std::move(key_str));
    }
    record_ops.push_back(&op);
    record_keys.push_back(emplaced.first->second);
  }
  if (PREDICT_FALSE(record_ops.size() != records->size())) {
    return Status::Corruption(Substitute(
        "WRITE_OP has $0 rows that produce a record, but $1 records", record_ops.size(),
        records->size()));
  }

  // Reads the row of each key as of 'snap' into 'rows' (nullopt if there is
  // none), one key at a time: the op's keys can be anywhere in the tablet.
  const auto read_rows = [&](const MvccSnapshot& snap, vector<std::optional<RowImage>>* rows) {
    rows->assign(keys.size(), std::nullopt);
    for (size_t k = 0; k < keys.size(); ++k) {
      if (deadline.Initialized() && MonoTime::Now() >= deadline) {
        return Status::TimedOut("deadline expired while reading CDC row images");
      }
      bool found;
      RowImage image;
      RETURN_NOT_OK(LookupRow(tablet, *image_schema, projection, snap, keys[k], key_strs[k],
                              &arena, &found, &image, rows_read));
      if (found) {
        (*rows)[k] = std::move(image);
      }
    }
    return Status::OK();
  };

  // Starting from each row as of right before the op, apply the op's changes
  // in order, the way the tablet did: each record's before-image is the row
  // before its change, and its after-image the row after it.
  vector<std::optional<RowImage>> rows;
  RETURN_NOT_OK(read_rows(MvccSnapshot(before_ts), &rows));
  // Before the consistency checks below, which a read below a GC point can
  // also fail.
  RETURN_NOT_OK(check_history());
  const auto inconsistent = [&](size_t i, const char* what) {
    return Status::Corruption(Substitute(
        "CDC cannot reconstruct the row images of op index $0, row $1: $2",
        replicate.id().index(), i, what));
  };
  for (size_t i = 0; i < records->size(); ++i) {
    CDCRecordPB& record = (*records)[i];
    const DecodedRowOperation& op = *record_ops[i];
    std::optional<RowImage>& row = rows[record_keys[i]];
    record.clear_old_changes();
    if (row) {
      *record.mutable_old_changes() = *row;
    }
    switch (op.type) {
      case RowOperationsPB::INSERT:
      case RowOperationsPB::INSERT_IGNORE:
        if (row) {
          return inconsistent(i, "an inserted row already existed");
        }
        row.emplace();
        ImageFromInsert(*image_schema, op_schema, op, &*row);
        break;
      case RowOperationsPB::UPSERT:
      case RowOperationsPB::UPSERT_IGNORE:
        // An UPSERT of an existing row is an UPDATE, and its after-image is the
        // row as stored, so a sink needs no hint about immutable columns.
        record.clear_keep_immutable_columns_on_update();
        if (row) {
          record.set_op_type(CDCOpTypePB::UPDATE);
          ApplyUpsertToImage(*image_schema, op_schema, op, &*row);
        } else {
          record.set_op_type(CDCOpTypePB::INSERT);
          row.emplace();
          ImageFromInsert(*image_schema, op_schema, op, &*row);
        }
        break;
      case RowOperationsPB::UPDATE:
      case RowOperationsPB::UPDATE_IGNORE:
        if (!row) {
          return inconsistent(i, "an updated row did not exist");
        }
        RETURN_NOT_OK(ApplyChangeListToImage(*image_schema, op.changelist, &*row));
        break;
      case RowOperationsPB::DELETE:
      case RowOperationsPB::DELETE_IGNORE:
        if (!row) {
          return inconsistent(i, "a deleted row did not exist");
        }
        row.reset();
        break;
      default:
        return inconsistent(i, "unexpected row operation type");
    }
    // A DELETE keeps its key-only after-image.
    if (row) {
      *record.mutable_changes() = *row;
    }
    record.set_image_schema_version(image_schema_version);
  }

  // The row of each key after the op must be what the op left.
  vector<std::optional<RowImage>> after_rows;
  RETURN_NOT_OK(read_rows(MvccSnapshot(Timestamp(effective_timestamp + 1)), &after_rows));
  RETURN_NOT_OK(check_history());
  for (size_t k = 0; k < keys.size(); ++k) {
    if (rows[k].has_value() != after_rows[k].has_value() ||
        (rows[k] && !SameImage(*rows[k], *after_rows[k]))) {
      return Status::Corruption(Substitute(
          "CDC reconstructed a row image of op index $0 that differs from the row the "
          "tablet stored", replicate.id().index()));
    }
  }
  return Status::OK();
}

void SetReconstructImagesHookForTests(std::function<void()> hook) {
  std::unique_ptr<std::function<void()>> new_hook;
  if (hook) {
    new_hook = std::make_unique<std::function<void()>>(std::move(hook));
  }
  std::unique_ptr<std::function<void()>> old_hook;
  {
    std::lock_guard l(reconstruct_images_hook_lock);
    old_hook.reset(reconstruct_images_hook);
    reconstruct_images_hook = new_hook.release();
  }
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
