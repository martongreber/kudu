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
#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class Schema;

namespace cdc {
struct CDCRecordBatch;
} // namespace cdc

namespace client {
class KuduTable;
} // namespace client

namespace itest {

// The reference sink of the CDC end-to-end tests: an in-memory copy of a
// table, built by applying the records of a CDC stream. A row is keyed by its
// encoded primary key and holds the values it got by column id, as the raw
// bytes the stream carried, so the copy can be compared with a scan of the
// table (see CompareWithTable()) across renamed, added and dropped columns. A
// column that a row got no value for (it was added to the table after the
// row was written) has the column's read default, as if the sink applied the
// DDL record that added the column.
//
// Besides building the copy, the mirror checks each record that it applies
// for the first time against it, and records a violation for:
//  - an INSERT of a row that exists, or an UPDATE or DELETE of one that does
//    not;
//  - on a FULL stream, a before-image that differs from the row in the copy,
//    or an UPDATE or DELETE without one;
//  - optionally, a transaction delivered without rows (BEGIN then COMMIT).
//
// The records of a tablet are applied in the order they come. A row whose
// range partition was dropped and added again is in two tablets, whose records
// come in no particular order: of those, the latest write wins.
//
// A record is applied for the first time unless its tablet already delivered
// a batch whose checkpoint is at or past the record's op index (a consumer
// that resumes from its durable checkpoint delivers the records after it
// again), or the record is at or below the timestamp of the tablet's snapshot
// (a write that commits while the snapshot is established can be in both the
// snapshot and the stream). Applying such records again leaves the copy as it
// was, since every record sets values rather than changing them. The
// snapshot's timestamp is that of its READ records, so the duplicates of a
// tablet whose snapshot had no rows are not recognized.
class CDCMirror {
 public:
  // The values of a row: column id -> raw cell value
  // (CDCDecodedColumn::raw_value), or std::nullopt for NULL.
  typedef std::map<int32_t, std::optional<std::string>> Row;
  // Rows by encoded primary key.
  typedef std::map<std::string, Row> Rows;

  // 'key_column_ids' are the ids of the table's primary key columns, in key
  // order. 'full_images' is whether the stream is a FULL stream.
  CDCMirror(std::vector<int32_t> key_column_ids, bool full_images);

  // Whether to record a violation for a transaction without rows. A
  // transaction whose rows are all in a snapshot has none, so this is for
  // streams that take no snapshot.
  void set_check_empty_transactions(bool check) { check_empty_txns_ = check; }

  // Applies 'batch'. Thread-safe.
  void Apply(const cdc::CDCRecordBatch& batch);

  // Removes the rows written last by tablet 'tablet_id': its range partition
  // was dropped. Thread-safe.
  void RemoveTabletRows(const std::string& tablet_id);

  // A copy of the rows. Thread-safe.
  Rows rows() const;

  // The violations found so far. Thread-safe.
  std::vector<std::string> violations() const;

  // The number of row records applied, and the number of before-images that
  // were checked against the copy.
  int64_t num_row_records() const;
  int64_t num_before_images_checked() const;

  // Scans 'table' at a snapshot, and sets 'diff' to the differences between
  // the scan and the copy, or to an empty string if there are none. 'schema'
  // is the table's schema, with column ids (for example
  // cdc::CDCTableMetadata::schema), and must have every column of 'table'.
  // Thread-safe.
  Status CompareWithTable(client::KuduTable* table, const Schema& schema,
                          std::string* diff) const;

  // Scans 'table' at a snapshot into 'rows', keyed like a mirror of it. See
  // CompareWithTable() for 'schema'.
  static Status ScanTable(client::KuduTable* table, const Schema& schema,
                          const std::vector<int32_t>& key_column_ids, Rows* rows);

  // Returns 'rows' with a value for exactly the columns of 'schema': that of
  // the row, or the column's read default (or NULL) if the row has none.
  static Rows Project(const Rows& rows, const Schema& schema);

  // Describes up to 'max_diffs' differences between 'expected' and 'actual',
  // or returns an empty string if they are equal.
  static std::string Diff(const Rows& expected, const Rows& actual, int max_diffs = 10);

 private:
  // Returns false and records a violation if 'values' do not have every key
  // column.
  template <class Columns>
  bool EncodeKey(const Columns& values, std::string* key);

  void AddViolationUnlocked(std::string msg);

  const std::vector<int32_t> key_column_ids_;
  const bool full_images_;
  bool check_empty_txns_ = false;

  mutable std::mutex lock_;
  Rows rows_;
  // The tablet that wrote a row last, and the timestamp of the write (the
  // commit timestamp of a transaction).
  struct RowWriter {
    std::string tablet_id;
    uint64_t timestamp;
  };
  // The last writer of each row, including the deleted ones.
  std::map<std::string, RowWriter> row_writers_;
  // The highest checkpoint delivered by each tablet.
  std::map<std::string, int64_t> delivered_checkpoints_;
  // The timestamp of each tablet's last snapshot.
  std::map<std::string, uint64_t> snapshot_timestamps_;
  std::vector<std::string> violations_;
  int64_t num_row_records_ = 0;
  int64_t num_before_images_checked_ = 0;

  DISALLOW_COPY_AND_ASSIGN(CDCMirror);
};

} // namespace itest
} // namespace kudu
