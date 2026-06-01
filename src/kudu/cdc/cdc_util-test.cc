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
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::consensus::ReplicateMsg;
using std::string;
using std::vector;

namespace kudu {
namespace cdc {

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
  ASSERT_OK(DecodeWriteOpAllRows(msg, /*schema_version=*/3, &records));
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
  ASSERT_OK(DecodeWriteOpAllRows(msg, /*schema_version=*/1, &records));
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
  ASSERT_OK(DecodeWriteOpAllRows(msg, /*schema_version=*/0, &records));
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
  ASSERT_OK(DecodeWriteOpAllRows(msg, /*schema_version=*/2, &records));
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
  ASSERT_OK(DecodeWriteOpAllRows(msg, /*schema_version=*/0, &records));
  ASSERT_EQ(1, records.size());

  // Only key and int_val should be present (2 columns set).
  // string_val is not set in the partial row, so it won't appear in isset bitmap.
  ASSERT_EQ(2, records[0].changes_size());
  EXPECT_EQ("key", records[0].changes(0).column_name());
  EXPECT_EQ("int_val", records[0].changes(1).column_name());
}

TEST_F(CDCUtilTest, InsertIgnoreVariant) {
  auto msg = BuildWriteReplicateMsg(1, 1, 100,
      {{RowOperationsPB::INSERT_IGNORE, 1, 1, "ignored"}});

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, /*schema_version=*/0, &records));
  ASSERT_EQ(1, records.size());
  EXPECT_EQ(CDCOpTypePB::INSERT, records[0].op_type());
}

TEST_F(CDCUtilTest, SchemaVersionPassedThrough) {
  auto msg = BuildWriteReplicateMsg(1, 1, 100,
      {{RowOperationsPB::INSERT, 1, 1, "test"}});

  vector<CDCRecordPB> records;
  ASSERT_OK(DecodeWriteOpAllRows(msg, /*schema_version=*/7, &records));
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
  Status s = DecodeWriteOpAllRows(msg, 0, &records);
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
  Status s = DecodeWriteOpAllRows(msg, 0, &records);
  EXPECT_TRUE(s.IsAborted()) << s.ToString();
}

// ---------------------------------------------------------------------------
// DecodeWriteOpRow tests (single-row API)
// ---------------------------------------------------------------------------

TEST_F(CDCUtilTest, DecodeWriteOpRow_OutOfBounds) {
  ReplicateMsg msg = BuildWriteReplicateMsg(1, 1, 100,
      {{RowOperationsPB::INSERT, 1, 1, "x"}});

  CDCRecordPB record;
  Status s = DecodeWriteOpRow(schema_, msg.write_request().row_operations(),
                              /*row_idx=*/1, &record);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
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
