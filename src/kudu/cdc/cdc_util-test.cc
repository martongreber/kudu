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
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(max_cell_size_bytes);

using kudu::consensus::ReplicateMsg;
using std::string;
using std::vector;

namespace kudu {
namespace cdc {

namespace {

// Sets 'out' to the INT32 value of column 'name' in the after-image of 'r'.
// Returns false if the column is absent, null, or not 4 bytes wide.
bool GetInt32Col(const CDCRecordPB& r, const string& name, int32_t* out) {
  for (const auto& c : r.changes()) {
    if (c.column_name() == name) {
      if (c.is_null() || c.value().size() != sizeof(*out)) {
        return false;
      }
      memcpy(out, c.value().data(), sizeof(*out));
      return true;
    }
  }
  return false;
}

// The outcome of a row, as recorded in its op's COMMIT entry.
enum RowOutcome {
  kApplied,
  kFailed,
  // The row changed nothing, e.g. an INSERT_IGNORE of an existing key.
  kIgnored,
};

// Returns the outcome of an op whose rows had the given outcomes.
tablet::TxResultPB Outcomes(const vector<RowOutcome>& outcomes) {
  tablet::TxResultPB result;
  for (RowOutcome outcome : outcomes) {
    auto* op = result.add_ops();
    switch (outcome) {
      case kApplied:
        op->add_mutated_stores()->set_mrs_id(0);
        break;
      case kFailed:
        StatusToPB(Status::AlreadyPresent("key already present"), op->mutable_failed_status());
        break;
      case kIgnored:
        break;
    }
  }
  return result;
}

// Returns the outcome of an op whose 'num_rows' rows were all applied.
tablet::TxResultPB AllApplied(int num_rows) {
  return Outcomes(vector<RowOutcome>(num_rows, kApplied));
}

} // anonymous namespace

class CDCUtilTest : public KuduTest {
 protected:
  CDCUtilTest()
      : schema_(SchemaBuilder(GetSimpleTestSchema()).Build()) {}

  // Build a WRITE_OP ReplicateMsg with the given rows.
  // Each row is (op_type, key, int_val, string_val).
  // If string_val is nullptr, the nullable column is not set.
  ReplicateMsg BuildWriteReplicateMsg(
      int64_t op_index, int64_t op_term, uint64_t timestamp,
      const vector<std::tuple<RowOperationsPB::Type, int32_t, int32_t, const char*>>& rows) {
    ReplicateMsg msg;
    msg.set_op_type(consensus::WRITE_OP);
    msg.mutable_id()->set_index(op_index);
    msg.mutable_id()->set_term(op_term);
    msg.set_timestamp(timestamp);

    auto* req = msg.mutable_write_request();
    EXPECT_OK(SchemaToPB(schema_, req->mutable_schema()));
    req->set_tablet_id("test-tablet");

    for (const auto& [op_type, key, int_val, str_val] : rows) {
      AddTestRowWithNullableStringToPB(op_type, schema_, key, int_val, str_val,
                                       req->mutable_row_operations());
    }
    return msg;
  }

  // Build a WRITE_OP with a single DELETE (key-only).
  ReplicateMsg BuildDeleteReplicateMsg(int64_t op_index, int64_t op_term,
                                       uint64_t timestamp, int32_t key) {
    ReplicateMsg msg;
    msg.set_op_type(consensus::WRITE_OP);
    msg.mutable_id()->set_index(op_index);
    msg.mutable_id()->set_term(op_term);
    msg.set_timestamp(timestamp);

    auto* req = msg.mutable_write_request();
    EXPECT_OK(SchemaToPB(schema_, req->mutable_schema()));
    req->set_tablet_id("test-tablet");

    AddTestKeyToPB(RowOperationsPB::DELETE, schema_, key,
                   req->mutable_row_operations());
    return msg;
  }

  // Build an ALTER_SCHEMA_OP ReplicateMsg.
  ReplicateMsg BuildAlterSchemaReplicateMsg(int64_t op_index, int64_t op_term,
                                            uint64_t timestamp,
                                            uint32_t new_schema_version) {
    ReplicateMsg msg;
    msg.set_op_type(consensus::ALTER_SCHEMA_OP);
    msg.mutable_id()->set_index(op_index);
    msg.mutable_id()->set_term(op_term);
    msg.set_timestamp(timestamp);

    auto* req = msg.mutable_alter_schema_request();
    req->set_tablet_id("test-tablet");
    req->set_schema_version(new_schema_version);

    // Build a new schema (original + one extra column).
    Schema new_schema({ ColumnSchema("key", INT32),
                        ColumnSchema("int_val", INT32),
                        ColumnSchema("string_val", STRING, ColumnSchema::NULLABLE),
                        ColumnSchema("new_col", INT64, ColumnSchema::NULLABLE) },
                      1);
    EXPECT_OK(SchemaToPB(new_schema, req->mutable_schema()));
    return msg;
  }

  // Build a PARTICIPANT_OP ReplicateMsg.
  ReplicateMsg BuildParticipantReplicateMsg(
      int64_t op_index, int64_t op_term, uint64_t timestamp,
      tserver::ParticipantOpPB::ParticipantOpType type, int64_t txn_id) {
    ReplicateMsg msg;
    msg.set_op_type(consensus::PARTICIPANT_OP);
    msg.mutable_id()->set_index(op_index);
    msg.mutable_id()->set_term(op_term);
    msg.set_timestamp(timestamp);

    auto* req = msg.mutable_participant_request();
    req->set_tablet_id("test-tablet");
    auto* op = req->mutable_op();
    op->set_type(type);
    op->set_txn_id(txn_id);
    return msg;
  }

  // Build a NO_OP ReplicateMsg.
  ReplicateMsg BuildNoOpReplicateMsg(int64_t op_index, int64_t op_term,
                                     uint64_t timestamp) {
    ReplicateMsg msg;
    msg.set_op_type(consensus::NO_OP);
    msg.mutable_id()->set_index(op_index);
    msg.mutable_id()->set_term(op_term);
    msg.set_timestamp(timestamp);
    return msg;
  }

  Schema schema_;
};

// ---------------------------------------------------------------------------
// DecodeWriteOpAllRows tests
// ---------------------------------------------------------------------------

TEST_F(CDCUtilTest, InsertSingleRow) {
  auto msg = BuildWriteReplicateMsg(10, 1, 1000,
      {{RowOperationsPB::INSERT, 42, 99, "hello"}});

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(1), /*schema_version=*/3, schema_, &records));
  ASSERT_EQ(1, records.size());

  const auto& r = records[0];
  EXPECT_EQ(CDCOpTypePB::INSERT, r.op_type());
  EXPECT_EQ(10, r.op_index());
  EXPECT_EQ(1, r.op_term());
  EXPECT_EQ(1000, r.timestamp());
  EXPECT_EQ(3, r.schema_version());

  // 3 columns: key, int_val, string_val
  ASSERT_EQ(3, r.changes_size());

  // key column
  EXPECT_EQ("key", r.changes(0).column_name());
  EXPECT_FALSE(r.changes(0).is_null());
  int32_t key_val;
  ASSERT_EQ(sizeof(key_val), r.changes(0).value().size());
  memcpy(&key_val, r.changes(0).value().data(), sizeof(key_val));
  EXPECT_EQ(42, key_val);

  // int_val column
  EXPECT_EQ("int_val", r.changes(1).column_name());
  EXPECT_FALSE(r.changes(1).is_null());
  int32_t int_val;
  memcpy(&int_val, r.changes(1).value().data(), sizeof(int_val));
  EXPECT_EQ(99, int_val);

  // string_val column
  EXPECT_EQ("string_val", r.changes(2).column_name());
  EXPECT_FALSE(r.changes(2).is_null());
  EXPECT_EQ("hello", r.changes(2).value());
}

TEST_F(CDCUtilTest, DeleteSingleRow) {
  auto msg = BuildDeleteReplicateMsg(5, 2, 2000, 7);

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(1), /*schema_version=*/1, schema_, &records));
  ASSERT_EQ(1, records.size());

  const auto& r = records[0];
  EXPECT_EQ(CDCOpTypePB::DELETE, r.op_type());
  EXPECT_EQ(5, r.op_index());
  EXPECT_EQ(2, r.op_term());

  // DELETE: only key column
  ASSERT_EQ(1, r.changes_size());
  EXPECT_EQ("key", r.changes(0).column_name());
  int32_t key_val;
  memcpy(&key_val, r.changes(0).value().data(), sizeof(key_val));
  EXPECT_EQ(7, key_val);
}

TEST_F(CDCUtilTest, UpsertSingleRow) {
  auto msg = BuildWriteReplicateMsg(8, 1, 3000,
      {{RowOperationsPB::UPSERT, 10, 20, "upserted"}});

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(1), /*schema_version=*/0, schema_, &records));
  ASSERT_EQ(1, records.size());

  EXPECT_EQ(CDCOpTypePB::UPSERT, records[0].op_type());
  ASSERT_EQ(3, records[0].changes_size());
  EXPECT_EQ("upserted", records[0].changes(2).value());
}

TEST_F(CDCUtilTest, MultiRowWrite) {
  auto msg = BuildWriteReplicateMsg(15, 3, 5000, {
      {RowOperationsPB::INSERT, 1, 100, "first"},
      {RowOperationsPB::INSERT, 2, 200, "second"},
      {RowOperationsPB::INSERT, 3, 300, "third"},
  });

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(3), /*schema_version=*/2, schema_, &records));
  ASSERT_EQ(3, records.size());

  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(CDCOpTypePB::INSERT, records[i].op_type());
    EXPECT_EQ(15, records[i].op_index());
    EXPECT_EQ(3, records[i].op_term());
    EXPECT_EQ(2, records[i].schema_version());

    int32_t key_val;
    memcpy(&key_val, records[i].changes(0).value().data(), sizeof(key_val));
    EXPECT_EQ(i + 1, key_val);
  }

  EXPECT_EQ("first", records[0].changes(2).value());
  EXPECT_EQ("second", records[1].changes(2).value());
  EXPECT_EQ("third", records[2].changes(2).value());
}

TEST_F(CDCUtilTest, NullableColumnIsNull) {
  // Pass nullptr for string_val to leave it unset (null).
  auto msg = BuildWriteReplicateMsg(1, 1, 100,
      {{RowOperationsPB::INSERT, 5, 50, nullptr}});

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(1), /*schema_version=*/0, schema_, &records));
  ASSERT_EQ(1, records.size());

  // An INSERT carries the complete row as the tablet stored it: string_val is
  // not set in the partial row and has no default, so it is NULL.
  ASSERT_EQ(3, records[0].changes_size());
  EXPECT_EQ("key", records[0].changes(0).column_name());
  EXPECT_EQ("int_val", records[0].changes(1).column_name());
  EXPECT_EQ("string_val", records[0].changes(2).column_name());
  EXPECT_TRUE(records[0].changes(2).is_null());
  EXPECT_FALSE(records[0].changes(2).has_value());
}

// Only the rows the tablet applied produce records: a failed row (e.g. a
// duplicate INSERT) or a row that changed nothing (e.g. an ignored
// INSERT_IGNORE) produces none, although the REPLICATE carries all of them.
TEST_F(CDCUtilTest, FailedAndIgnoredRowsProduceNoRecord) {
  auto msg = BuildWriteReplicateMsg(1, 1, 100, {
      {RowOperationsPB::INSERT, 1, 1, "failed"},
      {RowOperationsPB::INSERT_IGNORE, 2, 2, "ignored"},
      {RowOperationsPB::INSERT_IGNORE, 3, 3, "applied"},
      {RowOperationsPB::UPDATE_IGNORE, 4, 4, "ignored"},
      {RowOperationsPB::UPDATE, 5, 5, "applied"},
      {RowOperationsPB::UPSERT, 6, 6, "failed"},
  });
  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(
      msg, Outcomes({ kFailed, kIgnored, kApplied, kIgnored, kApplied, kFailed }),
      /*schema_version=*/0, schema_, &records));
  ASSERT_EQ(2, records.size());
  int32_t key = -1;
  EXPECT_EQ(CDCOpTypePB::INSERT, records[0].op_type());
  ASSERT_TRUE(GetInt32Col(records[0], "key", &key));
  EXPECT_EQ(3, key);
  EXPECT_EQ(CDCOpTypePB::UPDATE, records[1].op_type());
  ASSERT_TRUE(GetInt32Col(records[1], "key", &key));
  EXPECT_EQ(5, key);
}

// An op whose outcome does not match its rows cannot be streamed, nor can a
// row that the tablet applied but CDC fails to decode.
TEST_F(CDCUtilTest, OutcomeMismatchIsCorruption) {
  auto msg = BuildWriteReplicateMsg(1, 1, 100, {
      {RowOperationsPB::INSERT, 1, 1, "a"},
      {RowOperationsPB::INSERT, 2, 2, "b"},
  });
  vector<CDCRecordPB> records;
  Status s = DecodeWriteOpAllRows(msg, AllApplied(1), /*schema_version=*/0, schema_, &records);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
  s = DecodeWriteOpAllRows(msg, tablet::TxResultPB(), /*schema_version=*/0, schema_, &records);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
  EXPECT_TRUE(records.empty());

  // An UPDATE with no non-key column does not decode.
  msg = BuildWriteReplicateMsg(2, 1, 100, {});
  AddTestKeyToPB(RowOperationsPB::UPDATE, schema_, 1,
                 msg.mutable_write_request()->mutable_row_operations());
  s = DecodeWriteOpAllRows(msg, AllApplied(1), /*schema_version=*/0, schema_, &records);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_TRUE(records.empty());
}

// On an existing row the tablet applies an UPSERT_IGNORE as an update that
// skips immutable columns. Whether the row existed is not known from the WAL,
// so a record that sets an immutable column tells the sink not to overwrite it.
TEST_F(CDCUtilTest, UpsertIgnoreOfImmutableColumnIsFlagged) {
  const Schema schema({ ColumnSchema("key", INT32),
                        ColumnSchemaBuilder()
                            .name("imm")
                            .type(INT32)
                            .nullable(true)
                            .immutable(true),
                        ColumnSchema("v", INT32, ColumnSchema::NULLABLE) },
                      1);
  const Schema tablet_schema = SchemaBuilder(schema).Build();
  ReplicateMsg msg;
  msg.set_op_type(consensus::WRITE_OP);
  msg.mutable_id()->set_index(3);
  msg.mutable_id()->set_term(1);
  msg.set_timestamp(100);
  auto* req = msg.mutable_write_request();
  ASSERT_OK(SchemaToPB(schema, req->mutable_schema()));
  req->set_tablet_id("test-tablet");
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  struct Row {
    RowOperationsPB::Type type;
    bool set_imm;
    bool flagged;
  };
  const vector<Row> rows = {
      { RowOperationsPB::UPSERT_IGNORE, true, true },
      { RowOperationsPB::UPSERT_IGNORE, false, false },
      { RowOperationsPB::UPSERT, true, false },
      { RowOperationsPB::INSERT, true, false },
  };
  for (size_t i = 0; i < rows.size(); ++i) {
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("key", i));
    ASSERT_OK(row.SetInt32("v", i));
    if (rows[i].set_imm) {
      ASSERT_OK(row.SetInt32("imm", i));
    }
    enc.Add(rows[i].type, row);
  }

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(rows.size()), /*schema_version=*/0,
                                 tablet_schema, &records));
  ASSERT_EQ(rows.size(), records.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    SCOPED_TRACE(records[i].DebugString());
    EXPECT_EQ(rows[i].flagged, records[i].keep_immutable_columns_on_update());
    int32_t v = -1;
    EXPECT_EQ(rows[i].set_imm, GetInt32Col(records[i], "imm", &v));
  }
}

TEST_F(CDCUtilTest, SchemaVersionPassedThrough) {
  auto msg = BuildWriteReplicateMsg(1, 1, 100,
      {{RowOperationsPB::INSERT, 1, 1, "test"}});

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(1), /*schema_version=*/7, schema_, &records));
  ASSERT_EQ(1, records.size());
  EXPECT_EQ(7, records[0].schema_version());
}

TEST_F(CDCUtilTest, WriteOpMissingSchema) {
  ReplicateMsg msg;
  msg.set_op_type(consensus::WRITE_OP);
  msg.mutable_id()->set_index(1);
  msg.mutable_id()->set_term(1);
  msg.set_timestamp(100);
  msg.mutable_write_request()->set_tablet_id("test-tablet");
  // No schema set.

  vector<CDCRecordPB> records;
  Status s = DecodeWriteOpAllRows(msg, AllApplied(0), 0, schema_, &records);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(CDCUtilTest, WriteOpNoRowOperations) {
  ReplicateMsg msg;
  msg.set_op_type(consensus::WRITE_OP);
  msg.mutable_id()->set_index(1);
  msg.mutable_id()->set_term(1);
  msg.set_timestamp(100);
  auto* req = msg.mutable_write_request();
  req->set_tablet_id("test-tablet");
  ASSERT_OK(SchemaToPB(schema_, req->mutable_schema()));
  // No row_operations set.

  vector<CDCRecordPB> records;
  Status s = DecodeWriteOpAllRows(msg, AllApplied(0), 0, schema_, &records);
  EXPECT_TRUE(s.IsAborted()) << s.ToString();
}

// A non-unique primary key table has a hidden auto_incrementing_id key column
// that INSERTs leave unset. The leader records its counter in the REPLICATE so
// replicas reproduce the same ids; CDC must seed its decode from it too.
TEST_F(CDCUtilTest, NonUniquePk_InsertDecodesAutoIncrementId) {
  const Schema schema({ ColumnSchema("c0", INT32),
                        ColumnSchemaBuilder()
                            .name(Schema::GetAutoIncrementingColumnName())
                            .auto_incrementing(true),
                        ColumnSchema("c1", INT32, ColumnSchema::NULLABLE) },
                      2);
  const Schema tablet_schema = SchemaBuilder(schema).Build();
  ReplicateMsg msg;
  msg.set_op_type(consensus::WRITE_OP);
  msg.mutable_id()->set_index(12);
  msg.mutable_id()->set_term(1);
  msg.set_timestamp(100);
  auto* req = msg.mutable_write_request();
  ASSERT_OK(SchemaToPB(schema, req->mutable_schema()));
  req->set_tablet_id("test-tablet");
  req->mutable_auto_incrementing_column()->set_auto_incrementing_counter(41);
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  for (int i = 0; i < 3; ++i) {
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("c0", 7));
    ASSERT_OK(row.SetInt32("c1", i));
    enc.Add(RowOperationsPB::INSERT, row);
  }
  // A DELETE in the same op carries the id explicitly and must not consume it.
  {
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("c0", 7));
    ASSERT_OK(row.SetInt64(Schema::GetAutoIncrementingColumnName(), 5));
    enc.Add(RowOperationsPB::DELETE, row);
  }

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(4), /*schema_version=*/0,
                                 tablet_schema, &records));
  ASSERT_EQ(4, records.size());
  const auto id_of = [](const CDCRecordPB& r) {
    for (const auto& c : r.changes()) {
      if (c.column_name() == Schema::GetAutoIncrementingColumnName()) {
        int64_t v;
        CHECK_EQ(sizeof(v), c.value().size());
        memcpy(&v, c.value().data(), sizeof(v));
        return v;
      }
    }
    return int64_t{-1};
  };
  for (int i = 0; i < 3; ++i) {
    EXPECT_EQ(CDCOpTypePB::INSERT, records[i].op_type());
    EXPECT_EQ(42 + i, id_of(records[i]));
  }
  EXPECT_EQ(CDCOpTypePB::DELETE, records[3].op_type());
  EXPECT_EQ(5, id_of(records[3]));

  // Without the replicated counter the ids cannot be reproduced: fail rather
  // than emit rows with missing or invented ids.
  req->clear_auto_incrementing_column();
  records.clear();
  Status s = DecodeWriteOpAllRows(msg, AllApplied(4), /*schema_version=*/0, tablet_schema,
                                  &records);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
  EXPECT_TRUE(records.empty());
}

// A row that the tablet rejected while decoding (here: an UPDATE with no
// non-key column, and an INSERT with an oversized cell) was never applied. It
// must be skipped on its own, without failing the whole op or dropping the
// valid sibling rows.
TEST_F(CDCUtilTest, RowRejectedAtDecodeIsSkipped) {
  FLAGS_max_cell_size_bytes = 16;
  const string big(64, 'x');
  ReplicateMsg msg = BuildWriteReplicateMsg(3, 1, 100,
      {{RowOperationsPB::UPDATE, 1, 10, "a"}});
  auto* ops = msg.mutable_write_request()->mutable_row_operations();
  AddTestKeyToPB(RowOperationsPB::UPDATE, schema_, 2, ops);
  AddTestRowToPB(RowOperationsPB::INSERT, schema_, 3, 30, big, ops);
  AddTestRowToPB(RowOperationsPB::UPDATE, schema_, 4, 40, "b", ops);

  vector<CDCRecordPB> records;
  // The tablet rejects the rows the same way.
  ASSERT_OK(DecodeWriteOpAllRows(msg, Outcomes({ kApplied, kFailed, kFailed, kApplied }),
                                 /*schema_version=*/0, schema_, &records));
  ASSERT_EQ(2, records.size());
  int32_t key = -1;
  EXPECT_EQ(CDCOpTypePB::UPDATE, records[0].op_type());
  ASSERT_TRUE(GetInt32Col(records[0], "key", &key));
  EXPECT_EQ(1, key);
  EXPECT_EQ(CDCOpTypePB::UPDATE, records[1].op_type());
  ASSERT_TRUE(GetInt32Col(records[1], "key", &key));
  EXPECT_EQ(4, key);
}

// The Java client sends a column's default value only as 'read_default_value'
// in the write request's schema. The tablet fills an unset column with the
// write default of its own schema, so an INSERT or UPSERT that leaves such a
// NOT NULL column unset must not be rejected when decoding, and the INSERT
// carries the default.
TEST_F(CDCUtilTest, ReadDefaultOnlyColumnIsNotRejected) {
  const int32_t kDefault = 7;
  const Schema schema({ ColumnSchema("key", INT32),
                        ColumnSchemaBuilder()
                            .name("d")
                            .type(INT32)
                            .read_default(&kDefault)
                            .write_default(&kDefault) },
                      1);
  const Schema tablet_schema = SchemaBuilder(schema).Build();
  ReplicateMsg msg;
  msg.set_op_type(consensus::WRITE_OP);
  msg.mutable_id()->set_index(14);
  msg.mutable_id()->set_term(1);
  msg.set_timestamp(100);
  auto* req = msg.mutable_write_request();
  ASSERT_OK(SchemaToPB(schema, req->mutable_schema(),
                       SCHEMA_PB_WITHOUT_IDS | SCHEMA_PB_WITHOUT_WRITE_DEFAULT));
  ASSERT_TRUE(req->schema().columns(1).has_read_default_value());
  ASSERT_FALSE(req->schema().columns(1).has_write_default_value());
  req->set_tablet_id("test-tablet");
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  for (const auto& [type, key] : vector<std::pair<RowOperationsPB::Type, int32_t>>{
           { RowOperationsPB::INSERT, 1 }, { RowOperationsPB::UPSERT, 2 } }) {
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("key", key));
    enc.Add(type, row);
  }

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, AllApplied(2), /*schema_version=*/0,
                                 tablet_schema, &records));
  ASSERT_EQ(2, records.size());
  EXPECT_EQ(CDCOpTypePB::INSERT, records[0].op_type());
  EXPECT_EQ(CDCOpTypePB::UPSERT, records[1].op_type());
  for (int i = 0; i < 2; ++i) {
    SCOPED_TRACE(records[i].DebugString());
    int32_t v = -1;
    ASSERT_TRUE(GetInt32Col(records[i], "key", &v));
    EXPECT_EQ(i + 1, v);
  }
  // An INSERT carries the complete row; an UPSERT only the columns it set.
  int32_t v = -1;
  ASSERT_TRUE(GetInt32Col(records[0], "d", &v));
  EXPECT_EQ(kDefault, v);
  EXPECT_FALSE(GetInt32Col(records[1], "d", &v));
}

// ---------------------------------------------------------------------------
// DecodeNonWriteReplicateMsg tests
// ---------------------------------------------------------------------------

TEST_F(CDCUtilTest, AlterSchemaOp) {
  auto msg = BuildAlterSchemaReplicateMsg(20, 2, 9000, /*new_schema_version=*/5);

  CDCRecordPB record;
  ASSERT_OK(DecodeNonWriteReplicateMsg(msg, &record));

  EXPECT_EQ(CDCOpTypePB::DDL, record.op_type());
  EXPECT_EQ(20, record.op_index());
  EXPECT_EQ(2, record.op_term());
  EXPECT_EQ(9000, record.timestamp());
  // schema_version is the pre-op version (the new version minus one), while
  // new_schema_version is the version the ALTER establishes.
  EXPECT_EQ(4, record.schema_version());
  EXPECT_EQ(5, record.new_schema_version());
  EXPECT_TRUE(record.has_new_schema());
  EXPECT_EQ(4, record.new_schema().columns_size());
}

// A rename, or a change of the extra configuration, leaves the schema as it
// is: the DDL record carries the table's new name and extra configuration, so
// a sink can tell it from a no-op.
TEST_F(CDCUtilTest, AlterSchemaOp_CarriesNewTableName) {
  auto msg = BuildAlterSchemaReplicateMsg(22, 2, 9200, /*new_schema_version=*/5);
  auto* req = msg.mutable_alter_schema_request();
  req->set_new_table_name("renamed_table");
  req->mutable_new_extra_config()->set_history_max_age_sec(3600);

  CDCRecordPB record;
  ASSERT_OK(DecodeNonWriteReplicateMsg(msg, &record));
  EXPECT_EQ(CDCOpTypePB::DDL, record.op_type());
  EXPECT_EQ("renamed_table", record.new_table_name());
  ASSERT_TRUE(record.has_new_extra_config());
  EXPECT_EQ(3600, record.new_extra_config().history_max_age_sec());

  // An op without them, such as one written by an older server, has neither.
  auto old_msg = BuildAlterSchemaReplicateMsg(23, 2, 9300, /*new_schema_version=*/6);
  CDCRecordPB old_record;
  ASSERT_OK(DecodeNonWriteReplicateMsg(old_msg, &old_record));
  EXPECT_FALSE(old_record.has_new_table_name());
  EXPECT_FALSE(old_record.has_new_extra_config());
}

// An ALTER whose new schema version is the minimum possible value must not
// underflow the pre-op version computation.
TEST_F(CDCUtilTest, AlterSchemaOpSchemaVersionZeroDoesNotUnderflow) {
  auto msg = BuildAlterSchemaReplicateMsg(21, 2, 9100, /*new_schema_version=*/0);

  CDCRecordPB record;
  ASSERT_OK(DecodeNonWriteReplicateMsg(msg, &record));

  EXPECT_EQ(CDCOpTypePB::DDL, record.op_type());
  EXPECT_EQ(0, record.schema_version());
  EXPECT_EQ(0, record.new_schema_version());
}

TEST_F(CDCUtilTest, ParticipantOp_Begin) {
  auto msg = BuildParticipantReplicateMsg(
      30, 1, 4000, tserver::ParticipantOpPB::BEGIN_TXN, 42);

  CDCRecordPB record;
  ASSERT_OK(DecodeNonWriteReplicateMsg(msg, &record));

  EXPECT_EQ(CDCOpTypePB::BEGIN, record.op_type());
  EXPECT_EQ(30, record.op_index());
  EXPECT_EQ("42", record.txn_id());
}

TEST_F(CDCUtilTest, ParticipantOp_Commit) {
  auto msg = BuildParticipantReplicateMsg(
      31, 1, 4001, tserver::ParticipantOpPB::FINALIZE_COMMIT, 42);

  CDCRecordPB record;
  ASSERT_OK(DecodeNonWriteReplicateMsg(msg, &record));

  EXPECT_EQ(CDCOpTypePB::COMMIT, record.op_type());
  EXPECT_EQ("42", record.txn_id());
}

TEST_F(CDCUtilTest, ParticipantOp_Abort) {
  auto msg = BuildParticipantReplicateMsg(
      32, 1, 4002, tserver::ParticipantOpPB::ABORT_TXN, 99);

  CDCRecordPB record;
  ASSERT_OK(DecodeNonWriteReplicateMsg(msg, &record));

  EXPECT_EQ(CDCOpTypePB::ABORT, record.op_type());
  EXPECT_EQ("99", record.txn_id());
}

TEST_F(CDCUtilTest, ParticipantOp_BeginCommitSkipped) {
  auto msg = BuildParticipantReplicateMsg(
      33, 1, 4003, tserver::ParticipantOpPB::BEGIN_COMMIT, 10);

  CDCRecordPB record;
  Status s = DecodeNonWriteReplicateMsg(msg, &record);
  EXPECT_TRUE(s.IsAborted()) << s.ToString();
}

TEST_F(CDCUtilTest, NoOpSkipped) {
  auto msg = BuildNoOpReplicateMsg(1, 1, 100);

  CDCRecordPB record;
  Status s = DecodeNonWriteReplicateMsg(msg, &record);
  EXPECT_TRUE(s.IsAborted()) << s.ToString();
}

TEST_F(CDCUtilTest, WriteOpReturnsAbortedForNonWrite) {
  auto msg = BuildWriteReplicateMsg(1, 1, 100,
      {{RowOperationsPB::INSERT, 1, 1, "x"}});

  CDCRecordPB record;
  Status s = DecodeNonWriteReplicateMsg(msg, &record);
  EXPECT_TRUE(s.IsAborted()) << s.ToString();
}

} // namespace cdc
} // namespace kudu
