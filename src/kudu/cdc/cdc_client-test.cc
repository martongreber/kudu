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

#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;

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
  const Schema schema = MakeKeyValSchema();

  CDCRecordPB rec;
  rec.set_op_type(INSERT);
  rec.set_op_index(5);
  rec.set_op_term(2);
  rec.set_timestamp(1234567);
  rec.set_schema_version(0);
  AddCol(&rec, "key", Int32Bytes(42));
  AddCol(&rec, "val", "acme");

  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schema, "tablet-A", rec, &out));

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
  const Schema schema = MakeKeyValSchema();

  CDCRecordPB rec;
  rec.set_op_type(DELETE);
  rec.set_op_index(9);
  AddCol(&rec, "key", Int32Bytes(1001));

  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schema, "t", rec, &out));
  EXPECT_EQ(DELETE, out.op_type);
  ASSERT_EQ(1, out.after.size());
  EXPECT_EQ("key", out.after[0].name);
  EXPECT_EQ("1001", out.after[0].value);
}

TEST_F(CDCClientTest, DecodeFullImageWithBeforeAndNull) {
  const Schema schema = MakeKeyValSchema();

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
  ASSERT_OK(CDCConsumer::DecodeRecord(schema, "t", rec, &out));
  ASSERT_EQ(2, out.after.size());
  EXPECT_EQ("\"shipped\"", out.after[1].value);
  ASSERT_EQ(2, out.before.size());
  EXPECT_EQ("key", out.before[0].name);
  EXPECT_EQ("7", out.before[0].value);
  EXPECT_TRUE(out.before[1].is_null);
  EXPECT_TRUE(out.before[1].value.empty());
}

TEST_F(CDCClientTest, DecodeTransactionalAndDdlMetadata) {
  const Schema schema = MakeKeyValSchema();

  CDCRecordPB begin;
  begin.set_op_type(BEGIN);
  begin.set_op_index(3);
  begin.set_commit_timestamp(99);
  begin.set_txn_id("txn-1");
  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schema, "t", begin, &out));
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
  ASSERT_OK(CDCConsumer::DecodeRecord(schema, "t", ddl, &ddl_out));
  EXPECT_EQ(DDL, ddl_out.op_type);
  EXPECT_TRUE(ddl_out.has_new_schema);
  EXPECT_EQ(2, ddl_out.new_schema_version);
}

TEST_F(CDCClientTest, DecodeUnknownColumnFallsBackToDebugString) {
  const Schema schema = MakeKeyValSchema();

  CDCRecordPB rec;
  rec.set_op_type(INSERT);
  rec.set_op_index(1);
  // A column not present in the schema: should not fail, should fall back.
  AddCol(&rec, "ghost", "rawbytes");

  CDCDecodedRecord out;
  ASSERT_OK(CDCConsumer::DecodeRecord(schema, "t", rec, &out));
  ASSERT_EQ(1, out.after.size());
  EXPECT_EQ("ghost", out.after[0].name);
  EXPECT_FALSE(out.after[0].value.empty());
}

TEST_F(CDCClientTest, CreateClientRejectsEmptyMasters) {
  CDCClient::Options opts;  // no masters
  std::unique_ptr<CDCClient> client;
  Status s = CDCClient::Create(opts, &client);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

}  // namespace cdc
}  // namespace kudu
