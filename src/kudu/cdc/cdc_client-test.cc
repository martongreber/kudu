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

#include "kudu/cdc/cdc_consumer.h"

#include <cstdint>
#include <cstring>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_service.h"
#include "kudu/cdc/cdc_util.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/util/int128.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

// C1 flag declared here so tests can inspect/override it.
DECLARE_int32(cdc_max_resnapshot_attempts);

using std::string;
using std::vector;

namespace kudu {
namespace cdc {

namespace {
string Int32Bytes(int32_t v) {
  string s;
  s.resize(sizeof(v));
  memcpy(&s[0], &v, sizeof(v));
  return s;
}

Schema MakeKeyValSchema() {
  Schema schema;
  CHECK_OK(schema.Reset({ColumnSchema("key", INT32), ColumnSchema("val", STRING)},
                        /*key_columns=*/1));
  return schema;
}

// A history with MakeKeyValSchema() (without column ids) as version 0.
void MakeKeyValHistory(CDCSchemaHistory* history) {
  history->AddVersion(0, MakeKeyValSchema());
}

CDCColumnValuePB* AddCol(CDCRecordPB* rec, const string& name, const string& value) {
  CDCColumnValuePB* cv = rec->add_changes();
  cv->set_column_name(name);
  cv->set_value(value);
  cv->set_is_null(false);
  return cv;
}
}  // anonymous namespace

class CDCClientTest : public KuduTest {};

TEST_F(CDCClientTest, DecodeInsertRecord) {
  CDCSchemaHistory schemas;
  MakeKeyValHistory(&schemas);

  CDCRecordPB rec;
  rec.set_op_type(INSERT);
  rec.set_op_index(5);
  rec.set_op_term(2);
  rec.set_timestamp(1234567);
  rec.set_schema_version(0);
  AddCol(&rec, "key", Int32Bytes(42));
  AddCol(&rec, "val", "acme");

  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "tablet-A", rec, &out));

  EXPECT_EQ(INSERT, out.op_type);
  EXPECT_EQ(5, out.op_index);
  EXPECT_EQ(2, out.op_term);
  EXPECT_EQ("tablet-A", out.tablet_id);
  ASSERT_EQ(2, out.after.size());
  EXPECT_EQ("key", out.after[0].name);
  EXPECT_FALSE(out.after[0].is_null);
  EXPECT_EQ("42", out.after[0].value);
  EXPECT_EQ("val", out.after[1].name);
  EXPECT_EQ("\"acme\"", out.after[1].value);
  EXPECT_TRUE(out.before.empty());
}

TEST_F(CDCClientTest, DecodeDeleteRecordKeyOnly) {
  CDCSchemaHistory schemas;
  MakeKeyValHistory(&schemas);

  CDCRecordPB rec;
  rec.set_op_type(DELETE);
  rec.set_op_index(9);
  AddCol(&rec, "key", Int32Bytes(1001));

  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec, &out));
  EXPECT_EQ(DELETE, out.op_type);
  ASSERT_EQ(1, out.after.size());
  EXPECT_EQ("key", out.after[0].name);
  EXPECT_EQ("1001", out.after[0].value);
}

TEST_F(CDCClientTest, DecodeFullImageWithBeforeAndNull) {
  CDCSchemaHistory schemas;
  MakeKeyValHistory(&schemas);

  CDCRecordPB rec;
  rec.set_op_type(UPDATE);
  rec.set_op_index(11);
  // After-image.
  AddCol(&rec, "key", Int32Bytes(7));
  AddCol(&rec, "val", "shipped");
  // Before-image (old_changes): val was NULL.
  {
    CDCColumnValuePB* cv = rec.add_old_changes();
    cv->set_column_name("key");
    cv->set_value(Int32Bytes(7));
    cv->set_is_null(false);
  }
  {
    CDCColumnValuePB* cv = rec.add_old_changes();
    cv->set_column_name("val");
    cv->set_is_null(true);
  }

  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec, &out));
  ASSERT_EQ(2, out.after.size());
  EXPECT_EQ("\"shipped\"", out.after[1].value);
  ASSERT_EQ(2, out.before.size());
  EXPECT_EQ("key", out.before[0].name);
  EXPECT_EQ("7", out.before[0].value);
  EXPECT_TRUE(out.before[1].is_null);
  EXPECT_TRUE(out.before[1].value.empty());
}

TEST_F(CDCClientTest, DecodeTransactionalAndDdlMetadata) {
  CDCSchemaHistory schemas;
  MakeKeyValHistory(&schemas);

  CDCRecordPB begin;
  begin.set_op_type(BEGIN);
  begin.set_op_index(3);
  begin.set_commit_timestamp(99);
  begin.set_txn_id("txn-1");
  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", begin, &out));
  EXPECT_EQ(BEGIN, out.op_type);
  EXPECT_TRUE(out.has_commit_timestamp);
  EXPECT_EQ(99, out.commit_timestamp);
  EXPECT_TRUE(out.has_txn_id);
  EXPECT_EQ("txn-1", out.txn_id);

  CDCRecordPB ddl;
  ddl.set_op_type(DDL);
  ddl.set_op_index(4);
  ddl.set_new_schema_version(2);
  CDCDecodedRecord ddl_out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", ddl, &ddl_out));
  EXPECT_EQ(DDL, ddl_out.op_type);
  EXPECT_TRUE(ddl_out.has_new_schema);
  EXPECT_EQ(2, ddl_out.new_schema_version);
}

// A value of a column that is in no known schema is delivered undecoded, with
// its bytes. Before, it was shown as a hex string, as if it were decoded.
TEST_F(CDCClientTest, DecodeUnknownColumnIsNotDecoded) {
  CDCSchemaHistory schemas;
  MakeKeyValHistory(&schemas);

  CDCRecordPB rec;
  rec.set_op_type(INSERT);
  rec.set_op_index(1);
  AddCol(&rec, "key", Int32Bytes(1));
  AddCol(&rec, "ghost", "rawbytes");

  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec, &out));
  ASSERT_EQ(2, out.after.size());
  EXPECT_TRUE(out.after[0].decode_ok);
  EXPECT_EQ("1", out.after[0].value);
  EXPECT_EQ("ghost", out.after[1].name);
  EXPECT_FALSE(out.after[1].decode_ok);
  EXPECT_EQ(nullptr, out.after[1].type_info);
  EXPECT_TRUE(out.after[1].value.empty());
  EXPECT_EQ("rawbytes", out.after[1].raw_value);

  // The same with a column id, and a value of the wrong size for its type.
  CDCRecordPB rec2;
  rec2.set_op_type(INSERT);
  AddCol(&rec2, "key", "abc");
  CDCDecodedRecord out2;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec2, &out2));
  ASSERT_EQ(1, out2.after.size());
  EXPECT_FALSE(out2.after[0].decode_ok);
  EXPECT_EQ("abc", out2.after[0].raw_value);
}

// The values of a record decode by column id with the schema of its version,
// whatever the newest schema is: across a RENAME, and across a DROP and ADD of
// a column with the same name and another type. A record without column ids
// decodes by name with the schema of its version.
TEST_F(CDCClientTest, DecodeBySchemaVersionAndColumnId) {
  SchemaBuilder b;
  ASSERT_OK(b.AddKeyColumn("key", INT32));
  ASSERT_OK(b.AddNullableColumn("x", INT64));
  const Schema v0 = b.Build();
  ASSERT_OK(b.RenameColumn("x", "y"));
  const Schema v1 = b.Build();
  ASSERT_OK(b.RemoveColumn("y"));
  ASSERT_OK(b.AddNullableColumn("y", DOUBLE));
  const Schema v2 = b.Build();
  const int32_t old_id = v0.column_id(1);
  const int32_t new_id = v2.column_id(1);
  ASSERT_NE(old_id, new_id);

  CDCSchemaHistory schemas;
  schemas.AddVersion(2, v2);
  schemas.AddVersion(0, v0);
  // A version that is added again keeps its first schema.
  schemas.AddVersion(2, v0);
  EXPECT_EQ(new_id, schemas.max_column_id());

  int64_t i = 42;
  string i_bytes(reinterpret_cast<const char*>(&i), sizeof(i));
  double d = 1.5;
  string d_bytes(reinterpret_cast<const char*>(&d), sizeof(d));

  // A version-0 row written before the RENAME, decoded by id.
  CDCRecordPB rec;
  rec.set_op_type(INSERT);
  rec.set_schema_version(0);
  AddCol(&rec, "x", i_bytes)->set_column_id(old_id);
  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec, &out));
  ASSERT_EQ(1, out.after.size());
  EXPECT_EQ("x", out.after[0].name);
  EXPECT_EQ(old_id, out.after[0].column_id);
  EXPECT_TRUE(out.after[0].decode_ok);
  EXPECT_EQ(INT64, out.after[0].type_info->type());
  EXPECT_EQ("42", out.after[0].value);

  // A version-1 row (a version the consumer did not learn) after the RENAME.
  rec.set_schema_version(1);
  rec.mutable_changes(0)->set_column_name("y");
  CDCDecodedRecord out1;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec, &out1));
  ASSERT_EQ(1, out1.after.size());
  EXPECT_EQ("y", out1.after[0].name);
  EXPECT_EQ(INT64, out1.after[0].type_info->type());
  EXPECT_EQ("42", out1.after[0].value);

  // A version-2 row of the new column.
  CDCRecordPB rec2;
  rec2.set_op_type(INSERT);
  rec2.set_schema_version(2);
  AddCol(&rec2, "y", d_bytes)->set_column_id(new_id);
  CDCDecodedRecord out2;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec2, &out2));
  EXPECT_EQ(DOUBLE, out2.after[0].type_info->type());
  EXPECT_EQ("1.5", out2.after[0].value);

  // Without column ids: by name, with the schema of the record's version.
  for (const auto& [version, type, bytes] :
       { std::make_tuple(0, INT64, i_bytes), std::make_tuple(2, DOUBLE, d_bytes) }) {
    SCOPED_TRACE(version);
    CDCRecordPB rec3;
    rec3.set_op_type(INSERT);
    rec3.set_schema_version(version);
    AddCol(&rec3, version == 0 ? "x" : "y", bytes);
    CDCDecodedRecord out3;
    ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec3, &out3));
    ASSERT_TRUE(out3.after[0].decode_ok);
    EXPECT_EQ(type, out3.after[0].type_info->type());
  }

  // The images of a FULL record decode with 'image_schema_version'.
  CDCRecordPB full;
  full.set_op_type(UPDATE);
  full.set_schema_version(0);
  full.set_image_schema_version(2);
  AddCol(&full, "y", d_bytes);
  CDCDecodedRecord full_out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", full, &full_out));
  ASSERT_TRUE(full_out.after[0].decode_ok);
  EXPECT_EQ("1.5", full_out.after[0].value);
}

// A DECIMAL value is shown with its column's scale. Before, the scale was lost
// ("1234_D32").
TEST_F(CDCClientTest, DecodeDecimalPreservesScale) {
  Schema schema;
  ASSERT_OK(schema.Reset({ ColumnSchema("key", INT32),
                           ColumnSchemaBuilder()
                               .name("d32")
                               .type(DECIMAL32)
                               .nullable(true)
                               .type_attributes(ColumnTypeAttributes(9, 2)),
                           ColumnSchemaBuilder()
                               .name("d64")
                               .type(DECIMAL64)
                               .nullable(true)
                               .type_attributes(ColumnTypeAttributes(18, 4)),
                           ColumnSchemaBuilder()
                               .name("d128")
                               .type(DECIMAL128)
                               .nullable(true)
                               .type_attributes(ColumnTypeAttributes(38, 10)) },
                         /*key_columns=*/1));
  CDCSchemaHistory schemas;
  schemas.AddVersion(0, schema);
  const auto bytes = [](const auto& v) {
    return string(reinterpret_cast<const char*>(&v), sizeof(v));
  };
  const struct {
    int32_t d32;
    int64_t d64;
    int128_t d128;
    const char* s32;
    const char* s64;
    const char* s128;
  } kCases[] = {
    { 1234, -5, 1, "12.34", "-0.0005", "0.0000000001" },
    { 0, 0, 0, "0.00", "0.0000", "0.0000000000" },
    { -100, 123456789, -12345678901, "-1.00", "12345.6789", "-1.2345678901" },
  };
  for (const auto& c : kCases) {
    SCOPED_TRACE(c.s32);
    CDCRecordPB rec;
    rec.set_op_type(INSERT);
    AddCol(&rec, "key", Int32Bytes(1));
    AddCol(&rec, "d32", bytes(c.d32));
    AddCol(&rec, "d64", bytes(c.d64));
    AddCol(&rec, "d128", bytes(c.d128));
    CDCDecodedRecord out;
    ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec, &out));
    ASSERT_EQ(4, out.after.size());
    EXPECT_EQ(c.s32, out.after[1].value);
    EXPECT_EQ(c.s64, out.after[2].value);
    EXPECT_EQ(c.s128, out.after[3].value);
    EXPECT_EQ(2, out.after[1].type_attributes.scale);
    EXPECT_EQ(9, out.after[1].type_attributes.precision);
    EXPECT_EQ(DECIMAL128, out.after[3].type_info->type());
  }
}

// Every column type round-trips: the tablet server's serialization of a row
// (PopulateReadRecord) decodes to the column's type and attributes, its raw
// bytes are the cell's, and a NULL of each type decodes as a NULL of it.
TEST_F(CDCClientTest, DecodeAllTypes_RoundTripRawBytes) {
  SchemaBuilder b;
  ASSERT_OK(b.AddKeyColumn("key", INT32));
  const auto add = [&](const string& name, DataType type,
                       ColumnTypeAttributes attrs = ColumnTypeAttributes()) {
    return b.AddColumn(ColumnSchemaBuilder()
                           .name(name)
                           .type(type)
                           .nullable(true)
                           .type_attributes(attrs));
  };
  ASSERT_OK(add("i8", INT8));
  ASSERT_OK(add("i16", INT16));
  ASSERT_OK(add("i32", INT32));
  ASSERT_OK(add("i64", INT64));
  ASSERT_OK(add("b", BOOL));
  ASSERT_OK(add("f", FLOAT));
  ASSERT_OK(add("d", DOUBLE));
  ASSERT_OK(add("s", STRING));
  ASSERT_OK(add("bin", BINARY));
  ASSERT_OK(add("ts", UNIXTIME_MICROS));
  ASSERT_OK(add("dec32", DECIMAL32, ColumnTypeAttributes(9, 3)));
  ASSERT_OK(add("dec64", DECIMAL64, ColumnTypeAttributes(18, 1)));
  ASSERT_OK(add("dec128", DECIMAL128, ColumnTypeAttributes(38, 5)));
  ASSERT_OK(add("vc", VARCHAR, ColumnTypeAttributes(10)));
  ASSERT_OK(add("date", DATE));
  const Schema schema = b.Build();

  const int8_t i8 = -8;
  const int16_t i16 = -16000;
  const int32_t i32 = 123456;
  const int64_t i64 = -1234567890123;
  const bool bo = true;
  const float f = 0.25;
  const double d = -2.5;
  const Slice str("a\"b\n");
  const string bin_bytes("\x00\x01\xff", 3);
  const Slice bin(bin_bytes);
  const int64_t ts = 1700000000123456;
  const int32_t dec32 = -12345;
  const int64_t dec64 = 987;
  const int128_t dec128 = 100000;
  const Slice vc("varchar");
  const int32_t date = 19000;
  const vector<std::pair<const void*, size_t>> cells = {
    { &i32, sizeof(i32) },  // key
    { &i8, sizeof(i8) }, { &i16, sizeof(i16) }, { &i32, sizeof(i32) },
    { &i64, sizeof(i64) }, { &bo, sizeof(bo) }, { &f, sizeof(f) }, { &d, sizeof(d) },
    { &str, sizeof(str) }, { &bin, sizeof(bin) }, { &ts, sizeof(ts) },
    { &dec32, sizeof(dec32) }, { &dec64, sizeof(dec64) }, { &dec128, sizeof(dec128) },
    { &vc, sizeof(vc) }, { &date, sizeof(date) },
  };
  ASSERT_EQ(schema.num_columns(), cells.size());
  const vector<string> expected_values = {
    "123456", "-8", "-16000", "123456", "-1234567890123", "true", "0.25", "-2.5",
    "\"a\\\"b\\n\"", "\"\\x00\\x01\\xff\"", "2023-11-14T22:13:20.123456Z", "-12.345",
    "98.7", "1.00000", "\"varchar\"", "2022-01-08",
  };
  ASSERT_EQ(schema.num_columns(), expected_values.size());

  CDCSchemaHistory schemas;
  schemas.AddVersion(0, schema);
  RowBlockMemory mem;
  for (bool nulls : { false, true }) {
    SCOPED_TRACE(nulls);
    RowBlock block(&schema, 1, &mem);
    RowBlockRow row = block.row(0);
    for (int i = 0; i < schema.num_columns(); i++) {
      memcpy(row.mutable_cell_ptr(i), cells[i].first, cells[i].second);
      if (schema.column(i).is_nullable()) {
        row.cell(i).set_null(nulls);
      }
    }
    CDCRecordPB rec;
    rec.set_op_type(READ);
    ASSERT_OK(PopulateReadRecord(schema, row, &rec));
    CDCDecodedRecord out;
    ASSERT_OK(CDCConsumer::DecodeRecord(schemas, "t", rec, &out));
    ASSERT_EQ(schema.num_columns(), out.after.size());
    for (int i = 0; i < schema.num_columns(); i++) {
      const ColumnSchema& col = schema.column(i);
      const CDCDecodedColumn& c = out.after[i];
      SCOPED_TRACE(col.name());
      EXPECT_EQ(col.name(), c.name);
      EXPECT_EQ(schema.column_id(i), c.column_id);
      ASSERT_TRUE(c.decode_ok);
      EXPECT_EQ(col.type_info(), c.type_info);
      EXPECT_TRUE(c.type_attributes.EqualsForType(col.type_attributes(),
                                                  col.type_info()->type()));
      if (nulls && col.is_nullable()) {
        EXPECT_TRUE(c.is_null);
        EXPECT_TRUE(c.raw_value.empty());
        EXPECT_TRUE(c.value.empty());
        continue;
      }
      EXPECT_FALSE(c.is_null);
      string expected_raw;
      if (col.type_info()->physical_type() == BINARY) {
        expected_raw = static_cast<const Slice*>(cells[i].first)->ToString();
      } else {
        expected_raw.assign(static_cast<const char*>(cells[i].first), cells[i].second);
      }
      EXPECT_EQ(expected_raw, c.raw_value);
      EXPECT_EQ(expected_values[i], c.value);
    }
  }
}

TEST_F(CDCClientTest, CreateClientRejectsEmptyMasters) {
  CDCClient::Options opts;  // no masters
  std::unique_ptr<CDCClient> client;
  Status s = CDCClient::Create(opts, &client);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

// ---------------------------------------------------------------------------
// T14 C3: backoff jitter unit tests
// ---------------------------------------------------------------------------

// Verify CDCConsumer::ComputeJitteredBackoff bounds and cap properties
// using deterministic jitter_frac values (no Random involved).
TEST_F(CDCClientTest, Backoff_JitterBoundsAndCap) {
  using kudu::MonoDelta;
  MonoDelta cur = MonoDelta::FromMilliseconds(200);
  MonoDelta max = MonoDelta::FromSeconds(2);

  // jitter_frac = 0.0: minimum value, exactly 2 * cur (no jitter).
  // Proves the formula does not underflow below the doubled base.
  MonoDelta r0 = CDCConsumer::ComputeJitteredBackoff(cur, max, 0.0);
  EXPECT_EQ(MonoDelta::FromMilliseconds(400), r0);

  // jitter_frac = 0.5: midpoint, 2.5 * cur.
  // Proves jitter is applied proportionally.
  MonoDelta r05 = CDCConsumer::ComputeJitteredBackoff(cur, max, 0.5);
  EXPECT_EQ(MonoDelta::FromMilliseconds(500), r05);

  // jitter_frac just below 1.0: value approaches 3 * cur before cap.
  // Proves the upper bound of the jitter range is correct.
  MonoDelta r09 = CDCConsumer::ComputeJitteredBackoff(cur, max, 0.9);
  EXPECT_GE(r09, MonoDelta::FromMilliseconds(400));
  EXPECT_LE(r09, max);

  // Cap: 2 * (1s) + 0.5 * (1s) = 2.5s > max(2s) -> must be clamped to max.
  // Proves the max cap is enforced.
  MonoDelta rcap = CDCConsumer::ComputeJitteredBackoff(
      MonoDelta::FromSeconds(1), max, 0.5);
  EXPECT_EQ(max, rcap);

  // Different jitter values produce different results (non-uniform test).
  // Proves the jitter is actually applied and not ignored.
  EXPECT_NE(CDCConsumer::ComputeJitteredBackoff(cur, max, 0.0),
            CDCConsumer::ComputeJitteredBackoff(cur, max, 0.5));
}

// An automatic re-snapshot does not tell the application which rows were
// deleted while the stream's position was lost, so it is off by default: the
// poller stops in kNeedsResnapshot instead (see cdc_client-itest).
TEST_F(CDCClientTest, Resnapshot_FlagDefaultIsOff) {
  EXPECT_EQ(0, FLAGS_cdc_max_resnapshot_attempts);
}

// The consumer acts on every error code as the server's published contract
// says: with the wire fields the server sets, and, for an older server that
// does not set them, by the code alone. A transient failure must never stop
// a poller, and an expired position must never be retried in place.
TEST_F(CDCClientTest, ClassifyCdcError_MatchesServerContract) {
  const google::protobuf::EnumDescriptor* codes = CDCErrorPB::Code_descriptor();
  for (int i = 0; i < codes->value_count(); i++) {
    const auto code = static_cast<CDCErrorPB::Code>(codes->value(i)->number());
    SCOPED_TRACE(CDCErrorPB::Code_Name(code));
    bool is_retryable;
    bool needs_resnapshot;
    ClassifyCDCErrorCode(code, &is_retryable, &needs_resnapshot);
    ASSERT_FALSE(is_retryable && needs_resnapshot);
    const CDCConsumer::ErrorAction expected =
        is_retryable ? CDCConsumer::ErrorAction::kRetry
                     : (needs_resnapshot ? CDCConsumer::ErrorAction::kResnapshot
                                         : CDCConsumer::ErrorAction::kFatal);
    CDCErrorPB err;
    err.set_code(code);
    EXPECT_EQ(expected, CDCConsumer::ClassifyError(err)) << "without wire fields";
    err.set_is_retryable(is_retryable);
    err.set_needs_resnapshot(needs_resnapshot);
    EXPECT_EQ(expected, CDCConsumer::ClassifyError(err)) << "with wire fields";
  }
  // The codes whose classification matters most.
  bool is_retryable;
  bool needs_resnapshot;
  ClassifyCDCErrorCode(CDCErrorPB::SCHEMA_VERSION_MISMATCH, &is_retryable, &needs_resnapshot);
  EXPECT_TRUE(is_retryable);
  ClassifyCDCErrorCode(CDCErrorPB::SNAPSHOT_SESSION_LOST, &is_retryable, &needs_resnapshot);
  EXPECT_TRUE(needs_resnapshot);
  // Not sent: an authorization failure is an RPC error. Not retryable, as
  // documented.
  ClassifyCDCErrorCode(CDCErrorPB::NOT_AUTHORIZED, &is_retryable, &needs_resnapshot);
  EXPECT_FALSE(is_retryable);
}

// The wire fields win over the code, so that a consumer acts correctly on a
// code a newer server reclassified (or one it does not know).
TEST_F(CDCClientTest, Classify_UsesWireRetryFields) {
  CDCErrorPB err;
  err.set_code(CDCErrorPB::UNKNOWN_ERROR);
  err.set_is_retryable(true);
  err.set_needs_resnapshot(false);
  EXPECT_EQ(CDCConsumer::ErrorAction::kRetry, CDCConsumer::ClassifyError(err));
  err.set_is_retryable(false);
  err.set_needs_resnapshot(true);
  EXPECT_EQ(CDCConsumer::ErrorAction::kResnapshot, CDCConsumer::ClassifyError(err));
  err.set_code(CDCErrorPB::TABLET_NOT_LEADER);
  err.set_is_retryable(false);
  err.set_needs_resnapshot(false);
  EXPECT_EQ(CDCConsumer::ErrorAction::kFatal, CDCConsumer::ClassifyError(err));
}

// A response with an error code the consumer does not know, as a newer server
// may send, still parses, and the consumer acts on its wire fields. Before,
// the code was a required field: the unknown value made the whole response
// unparsable, so the consumer lost the retry hints too.
TEST_F(CDCClientTest, Classify_UnknownCodeUsesWireRetryFields) {
  constexpr int kUnknownCode = 1000;
  ASSERT_FALSE(CDCErrorPB::Code_IsValid(kUnknownCode));
  for (bool retryable : { true, false }) {
    SCOPED_TRACE(retryable);
    GetChangesResponsePB sent;
    CDCErrorPB* err = sent.mutable_error();
    StatusToPB(Status::IllegalState("a newer error"), err->mutable_status());
    err->set_is_retryable(retryable);
    err->set_needs_resnapshot(!retryable);
    // How the code of a newer server's enum value is encoded.
    err->mutable_unknown_fields()->AddVarint(CDCErrorPB::kCodeFieldNumber, kUnknownCode);

    GetChangesResponsePB received;
    ASSERT_TRUE(received.ParseFromString(sent.SerializePartialAsString()));
    ASSERT_TRUE(received.has_error());
    EXPECT_FALSE(received.error().has_code());
    EXPECT_EQ(retryable ? CDCConsumer::ErrorAction::kRetry
                        : CDCConsumer::ErrorAction::kResnapshot,
              CDCConsumer::ClassifyError(received.error()));
  }
}

}  // namespace cdc
}  // namespace kudu
