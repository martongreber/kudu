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

#include "kudu/integration-tests/cdc_mirror.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/common/array_cell_view.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/int128.h"
#include "kudu/util/slice.h"

using kudu::cdc::CDCDecodedColumn;
using kudu::cdc::CDCOpTypePB;
using kudu::cdc::CDCRecordBatch;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

namespace {

void AppendKeyPart(const string& raw, string* key) {
  const uint32_t len = raw.size();
  key->append(reinterpret_cast<const char*>(&len), sizeof(len));
  key->append(raw);
}

// Returns the elements of 'cell', a serialized array of type 'ti', as one
// string: for each element, a byte that says whether it is NULL, then for a
// non-NULL element its value (with its length, if variable). The same array
// can be serialized in more than one way, so arrays are compared in this form.
string ArrayElements(const TypeInfo& ti, const Slice& cell) {
  const TypeInfo* elem = GetArrayElementTypeInfo(ti);
  ArrayCellMetadataView view(cell.data(), cell.size());
  if (elem == nullptr || !view.Init().ok()) {
    return "corrupt:" + cell.ToString();
  }
  const uint8_t* data = view.data_as(elem->type());
  string out;
  for (size_t i = 0; i < view.elem_num(); i++) {
    const bool valid = !view.has_nulls() || BitmapTest(view.not_null_bitmap(), i);
    out.push_back(valid ? 1 : 0);
    if (!valid) continue;
    if (elem->physical_type() == BINARY) {
      AppendKeyPart(reinterpret_cast<const Slice*>(data)[i].ToString(), &out);
    } else {
      out.append(reinterpret_cast<const char*>(data) + i * elem->size(), elem->size());
    }
  }
  return out;
}

// The value of 'c' as the mirror keeps it.
string CellValue(const CDCDecodedColumn& c) {
  if (c.type_info != nullptr && c.type_info->is_array()) {
    return ArrayElements(*c.type_info, Slice(c.raw_value));
  }
  return c.raw_value;
}

// Merges the values of 'cols' into 'row'.
void MergeInto(const vector<CDCDecodedColumn>& cols, CDCMirror::Row* row) {
  for (const auto& c : cols) {
    if (c.is_null) {
      (*row)[c.column_id] = std::nullopt;
    } else {
      (*row)[c.column_id] = CellValue(c);
    }
  }
}

CDCMirror::Row ToRow(const vector<CDCDecodedColumn>& cols) {
  CDCMirror::Row row;
  MergeInto(cols, &row);
  return row;
}

string RowToString(const CDCMirror::Row& row) {
  string out = "{";
  for (const auto& [id, value] : row) {
    if (out.size() > 1) out += ", ";
    out += Substitute("$0: $1", id, value ? strings::b2a_hex(*value) : "NULL");
  }
  return out + "}";
}

template <class T>
string RawOf(T v) {
  return string(reinterpret_cast<const char*>(&v), sizeof(v));
}

// Returns the raw value of column 'idx' of 'row', whose type is 'col''s.
Status RawCell(const KuduScanBatch::RowPtr& row, int idx, const ColumnSchema& col,
               string* raw) {
  switch (col.type_info()->type()) {
    case BOOL: {
      bool v;
      RETURN_NOT_OK(row.GetBool(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case INT8: {
      int8_t v;
      RETURN_NOT_OK(row.GetInt8(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case INT16: {
      int16_t v;
      RETURN_NOT_OK(row.GetInt16(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case INT32: {
      int32_t v;
      RETURN_NOT_OK(row.GetInt32(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case INT64: {
      int64_t v;
      RETURN_NOT_OK(row.GetInt64(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case UNIXTIME_MICROS: {
      int64_t v;
      RETURN_NOT_OK(row.GetUnixTimeMicros(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case DATE: {
      int32_t v;
      RETURN_NOT_OK(row.GetDate(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case FLOAT: {
      float v;
      RETURN_NOT_OK(row.GetFloat(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case DOUBLE: {
      double v;
      RETURN_NOT_OK(row.GetDouble(idx, &v));
      *raw = RawOf(v);
      return Status::OK();
    }
    case DECIMAL32:
    case DECIMAL64:
    case DECIMAL128: {
      int128_t v;
      RETURN_NOT_OK(row.GetUnscaledDecimal(idx, &v));
      if (col.type_info()->type() == DECIMAL32) {
        *raw = RawOf(static_cast<int32_t>(v));
      } else if (col.type_info()->type() == DECIMAL64) {
        *raw = RawOf(static_cast<int64_t>(v));
      } else {
        *raw = RawOf(v);
      }
      return Status::OK();
    }
    case STRING:
    case BINARY:
    case VARCHAR: {
      Slice v;
      if (col.type_info()->type() == STRING) {
        RETURN_NOT_OK(row.GetString(idx, &v));
      } else if (col.type_info()->type() == BINARY) {
        RETURN_NOT_OK(row.GetBinary(idx, &v));
      } else {
        RETURN_NOT_OK(row.GetVarchar(idx, &v));
      }
      *raw = v.ToString();
      return Status::OK();
    }
    case NESTED:
      if (col.type_info()->is_array()) {
        *raw = ArrayElements(*col.type_info(), *reinterpret_cast<const Slice*>(row.cell(idx)));
        return Status::OK();
      }
      return Status::NotSupported("unsupported column type", col.ToString());
    default:
      return Status::NotSupported("unsupported column type", col.ToString());
  }
}

} // anonymous namespace

CDCMirror::CDCMirror(vector<int32_t> key_column_ids, bool full_images)
    : key_column_ids_(std::move(key_column_ids)),
      full_images_(full_images) {
}

template <class Columns>
bool CDCMirror::EncodeKey(const Columns& values, string* key) {
  key->clear();
  for (int32_t id : key_column_ids_) {
    const CDCDecodedColumn* found = nullptr;
    for (const auto& c : values) {
      if (c.column_id == id) {
        found = &c;
        break;
      }
    }
    if (found == nullptr || found->is_null) {
      AddViolationUnlocked(Substitute("record without key column $0", id));
      return false;
    }
    AppendKeyPart(found->raw_value, key);
  }
  return true;
}

void CDCMirror::AddViolationUnlocked(string msg) {
  LOG(WARNING) << "CDC mirror violation: " << msg;
  violations_.emplace_back(std::move(msg));
}

void CDCMirror::Apply(const CDCRecordBatch& batch) {
  std::lock_guard<std::mutex> l(lock_);
  const int64_t delivered =
      FindWithDefault(delivered_checkpoints_, batch.tablet_id, -1);
  uint64_t* snapshot_ts = &LookupOrInsert(&snapshot_timestamps_, batch.tablet_id, 0);
  bool in_txn = false;
  int txn_rows = 0;
  for (const auto& r : batch.records) {
    if (r.op_type == CDCOpTypePB::READ) {
      *snapshot_ts = std::max(*snapshot_ts, r.timestamp);
    }
    const uint64_t ts = r.has_commit_timestamp ? r.commit_timestamp : r.timestamp;
    const bool first_time = r.op_index > delivered && ts > *snapshot_ts;
    const string where = Substitute("tablet $0 op $1.$2", batch.tablet_id, r.op_term,
                                    r.op_index);
    switch (r.op_type) {
      case CDCOpTypePB::BEGIN:
        in_txn = true;
        txn_rows = 0;
        continue;
      case CDCOpTypePB::COMMIT:
        if (check_empty_txns_ && first_time && in_txn && txn_rows == 0) {
          AddViolationUnlocked(Substitute("$0: transaction without rows", where));
        }
        in_txn = false;
        continue;
      case CDCOpTypePB::ABORT:
        in_txn = false;
        continue;
      case CDCOpTypePB::DDL:
        continue;
      default:
        break;
    }
    txn_rows++;
    num_row_records_++;
    string key;
    if (!EncodeKey(r.after, &key)) {
      continue;
    }
    // A row moves to another tablet when its range partition is dropped and
    // added again; the old tablet's records can come after the new one's.
    RowWriter* last_writer = FindOrNull(row_writers_, key);
    if (last_writer && last_writer->tablet_id != batch.tablet_id &&
        ts < last_writer->timestamp) {
      continue;
    }
    Row* row = FindOrNull(rows_, key);
    const bool check = first_time && r.op_type != CDCOpTypePB::READ;
    const string desc = Substitute("$0 ($1 of key $2)", where,
                                   cdc::CDCOpTypePB_Name(r.op_type), strings::b2a_hex(key));
    if (check && full_images_ && !r.before.empty()) {
      // The before-image is the row as stored before the change.
      num_before_images_checked_++;
      const Row before = ToRow(r.before);
      if (row == nullptr) {
        AddViolationUnlocked(Substitute("$0: before-image $1 of a row that does not exist",
                                        desc, RowToString(before)));
      } else {
        for (const auto& c : r.before) {
          // A column that the row has no value for (it was added after the
          // row was written) cannot be checked.
          const std::optional<string>* mine = FindOrNull(*row, c.column_id);
          if (mine == nullptr) continue;
          const bool same = c.is_null ? !mine->has_value()
                                      : (mine->has_value() && **mine == CellValue(c));
          if (!same) {
            AddViolationUnlocked(Substitute("$0: before-image $1 differs from row $2", desc,
                                            RowToString(before), RowToString(*row)));
            break;
          }
        }
      }
    }
    switch (r.op_type) {
      case CDCOpTypePB::READ:
        rows_[key] = ToRow(r.after);
        break;
      case CDCOpTypePB::INSERT:
        if (check && row != nullptr) {
          AddViolationUnlocked(Substitute("$0: the row exists: $1", desc, RowToString(*row)));
        }
        rows_[key] = ToRow(r.after);
        break;
      case CDCOpTypePB::UPSERT:
        // Only CHANGE streams send UPSERT records, with the columns that the
        // operation set. Immutable columns are not supported here.
        if (r.keep_immutable_columns_on_update) {
          AddViolationUnlocked(Substitute("$0: immutable columns are not supported", desc));
        }
        MergeInto(r.after, &rows_[key]);
        break;
      case CDCOpTypePB::UPDATE:
        if (check && row == nullptr) {
          AddViolationUnlocked(Substitute("$0: the row does not exist", desc));
        }
        if (check && full_images_ && r.before.empty()) {
          AddViolationUnlocked(Substitute("$0: no before-image", desc));
        }
        if (full_images_) {
          rows_[key] = ToRow(r.after);
        } else {
          MergeInto(r.after, &rows_[key]);
        }
        break;
      case CDCOpTypePB::DELETE:
        if (check && row == nullptr) {
          AddViolationUnlocked(Substitute("$0: the row does not exist", desc));
        }
        if (check && full_images_ && r.before.empty()) {
          AddViolationUnlocked(Substitute("$0: no before-image", desc));
        }
        rows_.erase(key);
        // Keep the writer: a deleted row is a row too.
        row_writers_[key] = RowWriter{ batch.tablet_id, ts };
        continue;
      default:
        AddViolationUnlocked(Substitute("$0: unexpected record", desc));
        continue;
    }
    row_writers_[key] = RowWriter{ batch.tablet_id, ts };
  }
  if (batch.checkpoint_op_index > delivered) {
    delivered_checkpoints_[batch.tablet_id] = batch.checkpoint_op_index;
  }
}

void CDCMirror::RemoveTabletRows(const string& tablet_id) {
  std::lock_guard<std::mutex> l(lock_);
  int removed = 0;
  for (const auto& [key, writer] : row_writers_) {
    if (writer.tablet_id == tablet_id) {
      removed += rows_.erase(key);
    }
  }
  LOG(INFO) << Substitute("CDC mirror: removed the $0 rows of tablet $1", removed, tablet_id);
}

CDCMirror::Rows CDCMirror::rows() const {
  std::lock_guard<std::mutex> l(lock_);
  return rows_;
}

vector<string> CDCMirror::violations() const {
  std::lock_guard<std::mutex> l(lock_);
  return violations_;
}

int64_t CDCMirror::num_row_records() const {
  std::lock_guard<std::mutex> l(lock_);
  return num_row_records_;
}

int64_t CDCMirror::num_before_images_checked() const {
  std::lock_guard<std::mutex> l(lock_);
  return num_before_images_checked_;
}

Status CDCMirror::ScanTable(client::KuduTable* table, const Schema& schema,
                            const vector<int32_t>& key_column_ids, Rows* rows) {
  rows->clear();
  KuduScanner scanner(table);
  RETURN_NOT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  RETURN_NOT_OK(scanner.SetFaultTolerant());
  RETURN_NOT_OK(scanner.SetTimeoutMillis(60 * 1000));
  RETURN_NOT_OK(scanner.Open());
  const auto& client_schema = table->schema();
  // The schema of each projected column, and its id.
  vector<const ColumnSchema*> cols;
  vector<int32_t> ids;
  for (size_t i = 0; i < client_schema.num_columns(); i++) {
    const string name = client_schema.Column(i).name();
    const int idx = schema.find_column(name);
    if (idx == Schema::kColumnNotFound) {
      return Status::IllegalState("column not in the schema", name);
    }
    cols.push_back(&schema.column(idx));
    ids.push_back(schema.column_id(idx));
  }
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (const auto& r : batch) {
      Row row;
      for (size_t i = 0; i < cols.size(); i++) {
        if (r.IsNull(i)) {
          row[ids[i]] = std::nullopt;
          continue;
        }
        string raw;
        RETURN_NOT_OK(RawCell(r, i, *cols[i], &raw));
        row[ids[i]] = std::move(raw);
      }
      string key;
      for (int32_t id : key_column_ids) {
        const std::optional<string>* v = FindOrNull(row, id);
        if (v == nullptr || !v->has_value()) {
          return Status::IllegalState(Substitute("scanned row without key column $0", id));
        }
        AppendKeyPart(**v, &key);
      }
      (*rows)[key] = std::move(row);
    }
  }
  return Status::OK();
}

CDCMirror::Rows CDCMirror::Project(const Rows& rows, const Schema& schema) {
  // The value of each column for a row that has none.
  std::map<int32_t, std::optional<string>> defaults;
  for (size_t i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    std::optional<string> value;
    if (col.has_read_default()) {
      const void* d = col.read_default_value();
      if (col.type_info()->physical_type() == BINARY) {
        value = reinterpret_cast<const Slice*>(d)->ToString();
      } else {
        value = string(reinterpret_cast<const char*>(d), col.type_info()->size());
      }
    }
    defaults.emplace(schema.column_id(i), std::move(value));
  }
  Rows out;
  for (const auto& [key, row] : rows) {
    Row& projected = out[key];
    for (const auto& [id, value] : defaults) {
      const std::optional<string>* mine = FindOrNull(row, id);
      projected[id] = mine ? *mine : value;
    }
  }
  return out;
}

Status CDCMirror::CompareWithTable(client::KuduTable* table, const Schema& schema,
                                   string* diff) const {
  Rows scanned;
  RETURN_NOT_OK(ScanTable(table, schema, key_column_ids_, &scanned));
  *diff = Diff(Project(scanned, schema), Project(rows(), schema));
  return Status::OK();
}

string CDCMirror::Diff(const Rows& expected, const Rows& actual, int max_diffs) {
  string out;
  int n = 0;
  int total = 0;
  auto add = [&](const string& line) {
    if (n++ < max_diffs) out += line + "\n";
  };
  auto e = expected.begin();
  auto a = actual.begin();
  while (e != expected.end() || a != actual.end()) {
    if (a == actual.end() || (e != expected.end() && e->first < a->first)) {
      add(Substitute("missing row $0: $1", strings::b2a_hex(e->first), RowToString(e->second)));
      ++e;
      total++;
    } else if (e == expected.end() || a->first < e->first) {
      add(Substitute("extra row $0: $1", strings::b2a_hex(a->first), RowToString(a->second)));
      ++a;
      total++;
    } else {
      if (e->second != a->second) {
        add(Substitute("row $0: expected $1, got $2", strings::b2a_hex(e->first),
                       RowToString(e->second), RowToString(a->second)));
        total++;
      }
      ++e;
      ++a;
    }
  }
  if (total > max_diffs) {
    out += Substitute("... $0 differences in all ($1 expected rows, $2 actual rows)\n",
                      total, expected.size(), actual.size());
  }
  return out;
}

} // namespace itest
} // namespace kudu
