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

// Tests of the 'kudu cdc' command line tool.

#include <fcntl.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.proxy.h"
#include "kudu/cdc/cdc_service.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_bool(cdc_enabled);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_dropped_tablet_retention_secs);
DECLARE_int32(cdc_max_records_per_response);
DECLARE_int32(log_min_segments_to_retain);
DECLARE_int64(cdc_active_time_report_interval_ms);
DECLARE_int64(cdc_ephemeral_stream_expiry_ms);
DECLARE_int64(cdc_snapshot_cursor_persist_interval_ms);

using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::master::CDCStreamInfoPB;
using kudu::master::GetCDCStreamInfoRequestPB;
using kudu::master::GetCDCStreamInfoResponsePB;
using kudu::master::ListCDCStreamsRequestPB;
using kudu::master::ListCDCStreamsResponsePB;
using kudu::master::MasterServiceProxy;
using kudu::master::SysCDCStreamEntryPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using kudu::tablet::TabletReplica;
using kudu::tools::GetKuduToolAbsolutePath;
using kudu::tools::RunKuduTool;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {

class CDCToolITest : public MiniClusterITestBase {
 protected:
  static constexpr const char* const kTableName = "cdc_tool_table";

  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_cdc_enabled = true;
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
  }

  void StartClusterAndCreateTable() {
    NO_FATALS(StartCluster(/*num_tablet_servers=*/1));
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(b.Build(&schema));
    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(kTableName)
              .schema(&schema)
              .set_range_partition_columns({ "key" })
              .num_replicas(1)
              .Create());
    master_addr_ = cluster_->mini_master()->bound_rpc_addr().ToString();

    MessengerBuilder bld("CDCToolITest");
    ASSERT_OK(bld.Build(&messenger_));
    master_proxy_.reset(new MasterServiceProxy(
        messenger_, cluster_->mini_master()->bound_rpc_addr(),
        cluster_->mini_master()->bound_rpc_addr().host()));
    const auto& ts_addr = cluster_->mini_tablet_server(0)->bound_rpc_addr();
    cdc_proxy_.reset(new cdc::CDCServiceProxy(messenger_, ts_addr, ts_addr.host()));
    const vector<string> tablet_ids = cluster_->mini_tablet_server(0)->ListTablets();
    ASSERT_EQ(1, tablet_ids.size());
    tablet_id_ = tablet_ids[0];
  }

  // Reads the first page of the stream's snapshot, or with 'resume_key' the
  // next one, like the stream's consumer does.
  Status ReadSnapshotPage(const string& stream_id, const string& resume_key,
                          cdc::GetChangesResponsePB* resp) {
    cdc::GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id_);
    req.set_is_snapshot_start(resume_key.empty());
    if (!resume_key.empty()) {
      req.set_snapshot_resume_key(resume_key);
    }
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(cdc_proxy_->GetChanges(req, resp, &rpc));
    return resp->has_error() ? StatusFromPB(resp->error().status()) : Status::OK();
  }

  // The stream's durable state on the master.
  Status GetStreamInfo(const string& stream_id, CDCStreamInfoPB* info) {
    GetCDCStreamInfoRequestPB req;
    req.set_stream_id(stream_id);
    GetCDCStreamInfoResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(master_proxy_->GetCDCStreamInfo(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    *info = resp.stream();
    return Status::OK();
  }

  // Inserts 'count' rows with keys from 'start' in a single write, so a
  // consumer gets them as one batch.
  void InsertRowsInOneWrite(int start, int count) {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int i = start; i < start + count; ++i) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i));
      ASSERT_OK(session->Apply(insert.release()));
    }
    ASSERT_OK(session->Flush());
  }

  // Runs 'kudu cdc <args>' against the cluster's master.
  Status RunCdcTool(const vector<string>& args, string* out, string* err = nullptr) {
    // The master address follows the action: "cdc consume <master>" or
    // "cdc stream <action> <master>".
    vector<string> full = { "cdc" };
    full.insert(full.end(), args.begin(), args.end());
    full.insert(full.begin() + (args[0] == "consume" ? 2 : 3), master_addr_);
    string ignored_err;
    return RunKuduTool(full, out, err ? err : &ignored_err);
  }

  // The keys of the INSERT records the tool printed in 'out'.
  static vector<int> PrintedInsertKeys(const string& out) {
    vector<int> keys;
    const vector<string> lines = Split(out, "\n", strings::SkipEmpty());
    for (const auto& l : lines) {
      if (l.find("] INSERT ") == string::npos) {
        continue;
      }
      const size_t p = l.find("key=");
      CHECK_NE(string::npos, p) << l;
      keys.push_back(std::stoi(l.substr(p + 4)));
    }
    return keys;
  }

  static string StreamIdFromCreateOutput(string out) {
    StripWhiteSpace(&out);
    return out;
  }

  // Returns the streams the master knows of, including the ones being deleted.
  Status ListAllStreams(ListCDCStreamsResponsePB* resp) {
    ListCDCStreamsRequestPB req;
    req.set_include_deleting(true);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(master_proxy_->ListCDCStreams(req, resp, &rpc));
    return resp->has_error() ? StatusFromPB(resp->error().status()) : Status::OK();
  }

  string master_addr_;
  string tablet_id_;
  shared_ptr<Messenger> messenger_;
  unique_ptr<MasterServiceProxy> master_proxy_;
  unique_ptr<cdc::CDCServiceProxy> cdc_proxy_;
};

// create, list, describe and delete, and the errors for bad input.
TEST_F(CDCToolITest, StreamLifecycle) {
  // No maintenance pass after the first one, so a deleted stream stays
  // DELETING (unreaped) for the rest of the test.
  FLAGS_cdc_bg_scan_interval_ms = 3600 * 1000;
  NO_FATALS(StartClusterAndCreateTable());

  string out;
  ASSERT_OK(RunCdcTool({ "stream", "create", kTableName, "--record_type=full" }, &out));
  const string stream_id = StreamIdFromCreateOutput(out);
  ASSERT_FALSE(stream_id.empty());

  ASSERT_OK(RunCdcTool({ "stream", "list" }, &out));
  ASSERT_STR_CONTAINS(out, stream_id);
  ASSERT_STR_CONTAINS(out, "active");

  ASSERT_OK(RunCdcTool({ "stream", "describe", stream_id }, &out));
  ASSERT_STR_CONTAINS(out, "record_type:   full");
  ASSERT_STR_CONTAINS(out, "state:         active");
  ASSERT_STR_CONTAINS(out, "ephemeral:     false");
  ASSERT_STR_NOT_CONTAINS(out, "created:       -");
  // The per-tablet table: the stream's single tablet is live.
  ASSERT_STR_CONTAINS(out, "last_advance");
  ASSERT_STR_CONTAINS(out, "live");

  ASSERT_OK(RunCdcTool({ "stream", "describe", stream_id, "--format=json" }, &out));
  ASSERT_STR_CONTAINS(out, "\"state\": \"active\"");
  ASSERT_STR_CONTAINS(out, "\"status\": \"live\"");

  ASSERT_OK(RunCdcTool({ "stream", "delete", stream_id }, &out));
  ASSERT_STR_CONTAINS(out, "Deleted stream " + stream_id);

  // A stream being deleted is listed only on request, and can be described.
  ASSERT_OK(RunCdcTool({ "stream", "list" }, &out));
  ASSERT_STR_NOT_CONTAINS(out, stream_id);
  ASSERT_OK(RunCdcTool({ "stream", "list", "--include_deleting" }, &out));
  ASSERT_STR_CONTAINS(out, stream_id);
  ASSERT_STR_CONTAINS(out, "deleting");
  ASSERT_OK(RunCdcTool({ "stream", "describe", stream_id }, &out));
  ASSERT_STR_CONTAINS(out, "state:         deleting");

  // Bad input.
  string err;
  Status s = RunCdcTool({ "stream", "describe", "no-such-stream" }, &out, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(err, "not found");
  s = RunCdcTool({ "stream", "create", kTableName, "--record_type=bogus" }, &out, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(err, "unknown record type");
  s = RunCdcTool({ "stream", "create", "no_such_table" }, &out, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(err, "could not resolve table");
  s = RunCdcTool({ "consume", kTableName, "--from=bogus" }, &out, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(err, "unknown start point");
}

// Stopping at --max_records in the middle of a batch must not checkpoint the
// records of the batch that were not printed: the next consume of the stream
// prints them.
TEST_F(CDCToolITest, ConsumeMaxRecordsDoesNotSkip) {
  NO_FATALS(StartClusterAndCreateTable());
  string out;
  ASSERT_OK(RunCdcTool({ "stream", "create", kTableName }, &out));
  const string stream_id = StreamIdFromCreateOutput(out);
  constexpr int kNumRows = 5;
  NO_FATALS(InsertRowsInOneWrite(0, kNumRows));

  const string stream_flag = "--stream=" + stream_id;
  ASSERT_OK(RunCdcTool({ "consume", kTableName, stream_flag, "--checkpoint",
                         "--from=earliest", "--max_records=2" }, &out));
  ASSERT_EQ(vector<int>({ 0, 1 }), PrintedInsertKeys(out)) << out;
  // The final checkpoint the tool sent before it exited does not cover the
  // records it did not print, so the consumer is still behind the tablet.
  ASSERT_GT(cluster_->mini_tablet_server(0)->server()->cdc_service()->MaxOpsBehind(), 0);

  // The second run resumes from the durable checkpoint if there is one (else
  // from the earliest WAL, like the first run). Had the first run checkpointed
  // the whole batch, this run would find nothing to print and never reach
  // --max_records.
  ASSERT_OK(RunCdcTool({ "consume", kTableName, stream_flag, "--checkpoint",
                         "--from=earliest", Substitute("--max_records=$0", kNumRows) },
                       &out));
  ASSERT_EQ(vector<int>({ 0, 1, 2, 3, 4 }), PrintedInsertKeys(out)) << out;
}

// With --format=json, a column value is a JSON value of the column's type.
// Before, every value was a string with Kudu's debug rendering of it: "1" for
// an INT32, a quoted and escaped STRING, a DECIMAL without its scale.
TEST_F(CDCToolITest, ConsumeJsonValuesAreTyped) {
  NO_FATALS(StartClusterAndCreateTable());
  constexpr const char* const kTypedTable = "cdc_tool_typed_table";
  {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("s")->Type(client::KuduColumnSchema::STRING);
    b.AddColumn("dec")->Type(client::KuduColumnSchema::DECIMAL)->Precision(9)->Scale(2);
    b.AddColumn("bin")->Type(client::KuduColumnSchema::BINARY);
    b.AddColumn("d")->Type(client::KuduColumnSchema::DOUBLE);
    b.AddColumn("b")->Type(client::KuduColumnSchema::BOOL);
    b.AddColumn("n")->Type(client::KuduColumnSchema::INT64);
    ASSERT_OK(b.Build(&schema));
    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(kTypedTable)
              .schema(&schema)
              .set_range_partition_columns({ "key" })
              .num_replicas(1)
              .Create());
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTypedTable, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(insert->mutable_row()->SetString("s", "a\"b"));
    ASSERT_OK(insert->mutable_row()->SetUnscaledDecimal("dec", 1234));
    ASSERT_OK(insert->mutable_row()->SetBinary("bin", Slice("\x01\x02", 2)));
    ASSERT_OK(insert->mutable_row()->SetDouble("d", -2.5));
    ASSERT_OK(insert->mutable_row()->SetBool("b", true));
    ASSERT_OK(insert->mutable_row()->SetNull("n"));
    ASSERT_OK(session->Apply(insert.release()));
  }
  string out;
  ASSERT_OK(RunCdcTool({ "consume", kTypedTable, "--from=earliest", "--max_records=1",
                         "--format=json" }, &out));
  const vector<string> expected_columns = {
    R"j({"name":"key","type":"int32","is_null":false,"value":1})j",
    R"j({"name":"s","type":"string","is_null":false,"value":"a\"b"})j",
    R"j({"name":"dec","type":"decimal(9, 2)","is_null":false,"value":"12.34"})j",
    R"j({"name":"bin","type":"binary","is_null":false,"value":"AQI="})j",
    R"j({"name":"d","type":"double","is_null":false,"value":-2.5})j",
    R"j({"name":"b","type":"bool","is_null":false,"value":true})j",
    R"j({"name":"n","type":"int64","is_null":true})j",
  };
  for (const auto& col : expected_columns) {
    ASSERT_STR_CONTAINS(out, col);
  }
}

// Consuming an existing stream leaves its checkpoint where it is unless
// --checkpoint is given, so that looking at a stream does not make its
// consumer skip changes. Before, the tool always checkpointed.
TEST_F(CDCToolITest, ConsumeExistingStreamDoesNotAdvanceCheckpoint) {
  NO_FATALS(StartClusterAndCreateTable());
  string out;
  ASSERT_OK(RunCdcTool({ "stream", "create", kTableName }, &out));
  const string stream_id = StreamIdFromCreateOutput(out);
  NO_FATALS(InsertRowsInOneWrite(0, 5));
  const auto checkpoint = [&](int64_t* op_index) {
    ListCDCStreamsResponsePB resp;
    ASSERT_OK(ListAllStreams(&resp));
    ASSERT_EQ(1, resp.streams_size());
    ASSERT_EQ(1, resp.streams(0).tablet_checkpoints_size());
    *op_index = resp.streams(0).tablet_checkpoints().begin()->second;
  };

  const string stream_flag = "--stream=" + stream_id;
  ASSERT_OK(RunCdcTool({ "consume", kTableName, stream_flag, "--from=earliest",
                         "--max_records=5" }, &out));
  ASSERT_EQ(vector<int>({ 0, 1, 2, 3, 4 }), PrintedInsertKeys(out)) << out;
  // Give a checkpoint the time to reach the master.
  SleepFor(MonoDelta::FromSeconds(2));
  int64_t op_index;
  NO_FATALS(checkpoint(&op_index));
  ASSERT_EQ(0, op_index);

  ASSERT_OK(RunCdcTool({ "consume", kTableName, stream_flag, "--checkpoint",
                         "--from=earliest", "--max_records=5" }, &out));
  ASSERT_EVENTUALLY([&] {
    int64_t op_index;
    NO_FATALS(checkpoint(&op_index));
    ASSERT_GT(op_index, 0);
  });
}

// Without --checkpoint, the tool does not take the snapshot of an existing
// stream: the snapshot's progress is durable stream state, like the checkpoint.
// Before, it took the snapshot, and the stream's consumer could then skip it.
TEST_F(CDCToolITest, ConsumeExistingStreamTakesNoSnapshot) {
  NO_FATALS(StartClusterAndCreateTable());
  string out;
  ASSERT_OK(RunCdcTool({ "stream", "create", kTableName,
                         "--snapshot_mode=initial_and_continue" }, &out));
  const string stream_id = StreamIdFromCreateOutput(out);
  NO_FATALS(InsertRowsInOneWrite(0, 5));

  string err;
  Status s = RunCdcTool({ "consume", kTableName, "--stream=" + stream_id,
                          "--from=snapshot", "--max_records=5" }, &out, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString() << ": " << out;
  ASSERT_STR_CONTAINS(err, "requires --checkpoint");
  ASSERT_STR_NOT_CONTAINS(out, "] READ ");
  CDCStreamInfoPB info;
  ASSERT_OK(GetStreamInfo(stream_id, &info));
  EXPECT_EQ(0, info.tablet_snapshot_cursors_size()) << info.ShortDebugString();
  for (const auto& e : info.tablet_checkpoints()) {
    EXPECT_EQ(0, e.second) << info.ShortDebugString();
  }
}

// Without --checkpoint, the tool does not continue the snapshot that the
// stream's consumer has in progress, whatever --from says. Before, it took the
// snapshot over, and moved its durable position past rows that the stream's
// consumer never got.
TEST_F(CDCToolITest, ConsumeExistingStreamLeavesSnapshotInProgress) {
  FLAGS_cdc_max_records_per_response = 2;
  FLAGS_cdc_snapshot_cursor_persist_interval_ms = 0;
  NO_FATALS(StartClusterAndCreateTable());
  string out;
  ASSERT_OK(RunCdcTool({ "stream", "create", kTableName,
                         "--snapshot_mode=initial_and_continue" }, &out));
  const string stream_id = StreamIdFromCreateOutput(out);
  NO_FATALS(InsertRowsInOneWrite(0, 6));

  // The stream's consumer read the first page of its snapshot.
  cdc::GetChangesResponsePB p1;
  ASSERT_OK(ReadSnapshotPage(stream_id, "", &p1));
  ASSERT_FALSE(p1.snapshot_done());
  master::CDCTabletSnapshotCursorPB cursor;
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfoPB info;
    ASSERT_OK(GetStreamInfo(stream_id, &info));
    ASSERT_EQ(1, info.tablet_snapshot_cursors_size());
    cursor = info.tablet_snapshot_cursors(0);
  });

  string err;
  Status s = RunCdcTool({ "consume", kTableName, "--stream=" + stream_id,
                          "--max_records=3" }, &out, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString() << ": " << out;
  ASSERT_STR_CONTAINS(err, "has a snapshot in progress");
  ASSERT_STR_NOT_CONTAINS(out, "] READ ");
  CDCStreamInfoPB info;
  ASSERT_OK(GetStreamInfo(stream_id, &info));
  ASSERT_EQ(1, info.tablet_snapshot_cursors_size()) << info.ShortDebugString();
  EXPECT_EQ(cursor.ShortDebugString(), info.tablet_snapshot_cursors(0).ShortDebugString());

  // The stream's consumer continues its snapshot.
  cdc::GetChangesResponsePB p2;
  ASSERT_OK(ReadSnapshotPage(stream_id, p1.snapshot_resume_key(), &p2));
  EXPECT_EQ(2, p2.records_size());
}

// The tool fails once consuming stopped on every tablet, here because the
// stream was deleted. Before, it kept running with no tablet consumed.
TEST_F(CDCToolITest, ConsumeFailsWhenEveryTabletStops) {
  FLAGS_cdc_bg_scan_interval_ms = 200;
  NO_FATALS(StartClusterAndCreateTable());
  string out;
  ASSERT_OK(RunCdcTool({ "stream", "create", kTableName }, &out));
  const string stream_id = StreamIdFromCreateOutput(out);
  NO_FATALS(InsertRowsInOneWrite(0, 1));

  Subprocess consume({ GetKuduToolAbsolutePath(), "cdc", "consume", master_addr_,
                       kTableName, "--stream=" + stream_id, "--from=earliest" });
  consume.ShareParentStdout(false);
  consume.ShareParentStderr(false);
  ASSERT_OK(consume.Start());
  auto kill = MakeScopedCleanup([&]() { WARN_NOT_OK(consume.Kill(SIGKILL), "kill failed"); });
  // Wait until the tool reads the tablet.
  ASSERT_EVENTUALLY([&] {
    ASSERT_GT(cluster_->mini_tablet_server(0)->server()->cdc_service()->ActiveStreamCount(), 0);
  });
  ASSERT_OK(RunCdcTool({ "stream", "delete", stream_id }, &out));

  int wait_status = 0;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(consume.WaitNoBlock(&wait_status));
  });
  kill.cancel();
  int exit_status = 0;
  string info;
  ASSERT_OK(consume.GetExitStatus(&exit_status, &info));
  ASSERT_NE(0, exit_status) << info;
}

// The tool reports a dropped range partition whose changes the stream did not
// read before the master deleted its tablet, and fails.
TEST_F(CDCToolITest, ConsumeFailsOnDroppedPartitionWithLostChanges) {
  FLAGS_catalog_manager_bg_task_wait_ms = 100;
  FLAGS_cdc_bg_scan_interval_ms = 100;
  FLAGS_cdc_dropped_tablet_retention_secs = 1;
  NO_FATALS(StartClusterAndCreateTable());
  string out;
  ASSERT_OK(RunCdcTool({ "stream", "create", kTableName }, &out));
  const string stream_id = StreamIdFromCreateOutput(out);
  NO_FATALS(InsertRowsInOneWrite(0, 5));
  // Replace the table's only range partition, which is unbounded.
  {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    alterer->DropRangePartition(table->schema().NewRow(), table->schema().NewRow());
    ASSERT_OK(alterer->Alter());
    alterer.reset(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
    unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
    ASSERT_OK(lb->SetInt32("key", 1000));
    ASSERT_OK(ub->SetInt32("key", 2000));
    alterer->AddRangePartition(lb.release(), ub.release());
    ASSERT_OK(alterer->Alter());
  }
  ASSERT_EVENTUALLY([&] {
    CDCStreamInfoPB info;
    ASSERT_OK(GetStreamInfo(stream_id, &info));
    ASSERT_EQ(1, info.dropped_tablets_size());
    ASSERT_EQ(master::CDCStreamDroppedTabletPB::TAIL_LOST, info.dropped_tablets(0).state());
  });

  // The tool goes on consuming the new partition, and reports the drop while
  // it runs.
  Subprocess consume({ GetKuduToolAbsolutePath(), "cdc", "consume", master_addr_,
                       kTableName, "--stream=" + stream_id, "--from=earliest" });
  consume.ShareParentStdout(false);
  consume.ShareParentStderr(false);
  ASSERT_OK(consume.Start());
  auto kill = MakeScopedCleanup([&]() { WARN_NOT_OK(consume.Kill(SIGINT), "kill failed"); });
  const int err_fd = consume.from_child_stderr_fd();
  ASSERT_EQ(0, fcntl(err_fd, F_SETFL, fcntl(err_fd, F_GETFL) | O_NONBLOCK));
  string err;
  ASSERT_EVENTUALLY([&] {
    char buf[4096];
    ssize_t n;
    while ((n = read(err_fd, buf, sizeof(buf))) > 0) {
      err.append(buf, n);
    }
    ASSERT_STR_CONTAINS(err, "was dropped, and changes not consumed are lost");
  });
  ASSERT_OK(consume.Kill(SIGINT));
  ASSERT_OK(consume.Wait());
  kill.cancel();
  int exit_status = 0;
  string info;
  ASSERT_OK(consume.GetExitStatus(&exit_status, &info));
  ASSERT_NE(0, exit_status) << info;
}

// The tool exits when every range partition of the table was dropped, also
// once the pollers of the dropped tablets are removed.
TEST_F(CDCToolITest, ConsumeExitsWhenEveryPartitionIsDropped) {
  FLAGS_catalog_manager_bg_task_wait_ms = 100;
  FLAGS_cdc_bg_scan_interval_ms = 100;
  NO_FATALS(StartClusterAndCreateTable());
  string out;
  ASSERT_OK(RunCdcTool({ "stream", "create", kTableName }, &out));
  const string stream_id = StreamIdFromCreateOutput(out);
  NO_FATALS(InsertRowsInOneWrite(0, 5));

  // Rescan often, so that the poller of the dropped tablet is removed before
  // the tool sees that it stopped.
  Subprocess consume({ GetKuduToolAbsolutePath(), "cdc", "consume", master_addr_,
                       kTableName, "--stream=" + stream_id, "--from=earliest",
                       "--cdc_tablet_rescan_interval_ms=10" });
  consume.ShareParentStdout(false);
  consume.ShareParentStderr(false);
  ASSERT_OK(consume.Start());
  auto kill = MakeScopedCleanup([&]() { WARN_NOT_OK(consume.Kill(SIGINT), "kill failed"); });
  const int out_fd = consume.from_child_stdout_fd();
  ASSERT_EQ(0, fcntl(out_fd, F_SETFL, fcntl(out_fd, F_GETFL) | O_NONBLOCK));
  string records;
  ASSERT_EVENTUALLY([&] {
    char buf[4096];
    ssize_t n;
    while ((n = read(out_fd, buf, sizeof(buf))) > 0) {
      records.append(buf, n);
    }
    const vector<string> lines = Split(records, "\n", strings::SkipEmpty());
    ASSERT_EQ(5, lines.size()) << records;
  });

  // Drop the table's only range partition, which is unbounded.
  {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    unique_ptr<client::KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    alterer->DropRangePartition(table->schema().NewRow(), table->schema().NewRow());
    ASSERT_OK(alterer->Alter());
  }
  const MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  Status s;
  while ((s = consume.WaitNoBlock()).IsTimedOut() && MonoTime::Now() < deadline) {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  ASSERT_FALSE(s.IsTimedOut()) << "the tool did not exit after every range partition "
                                  "was dropped";
  ASSERT_OK(s);
  kill.cancel();
  int exit_status = 0;
  string info;
  ASSERT_OK(consume.GetExitStatus(&exit_status, &info));
  ASSERT_EQ(0, exit_status) << info;
}

// The tool fails if consuming stopped on any tablet, even when it goes on to
// consume the others. Here the first tablet's WAL was garbage-collected before
// the stream was created. Before, the tool exited with success when
// interrupted.
TEST_F(CDCToolITest, ConsumeFailsWhenSomeTabletStops) {
  FLAGS_log_min_segments_to_retain = 1;
  NO_FATALS(StartClusterAndCreateTable());
  constexpr const char* const kTwoTabletTable = "cdc_tool_two_tablets";
  {
    client::KuduSchema schema;
    client::KuduSchemaBuilder b;
    b.AddColumn("key")->Type(client::KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(client::KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(b.Build(&schema));
    unique_ptr<KuduPartialRow> split(schema.NewRow());
    ASSERT_OK(split->SetInt32("key", 100));
    unique_ptr<client::KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(kTwoTabletTable)
              .schema(&schema)
              .set_range_partition_columns({ "key" })
              .add_range_partition_split(split.release())
              .num_replicas(1)
              .Create());
  }
  // Write to the first tablet, then garbage-collect its first ops.
  client::sp::shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTwoTabletTable, &table));
  client::sp::shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  const auto insert = [&](int key) {
    unique_ptr<KuduInsert> ins(table->NewInsert());
    ASSERT_OK(ins->mutable_row()->SetInt32("key", key));
    ASSERT_OK(ins->mutable_row()->SetInt32("val", key));
    ASSERT_OK(session->Apply(ins.release()));
  };
  NO_FATALS(insert(0));
  vector<scoped_refptr<TabletReplica>> replicas;
  cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletReplicas(&replicas);
  scoped_refptr<TabletReplica> first;
  for (const auto& r : replicas) {
    uint64_t rows = 0;
    if (r->tablet_metadata()->table_name() == kTwoTabletTable &&
        r->tablet()->CountRows(&rows).ok() && rows > 0) {
      first = r;
    }
  }
  ASSERT_NE(nullptr, first.get());
  ASSERT_OK(first->log()->WaitUntilAllFlushed());
  ASSERT_OK(first->log()->AllocateSegmentAndRollOverForTests());
  NO_FATALS(insert(1));
  ASSERT_OK(first->log()->WaitUntilAllFlushed());
  ASSERT_OK(first->log()->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(first->tablet()->Flush());
  first->RunLogGC();
  ASSERT_GT(first->log()->reader()->GetMinReplicateIndex(), 1);

  Subprocess consume({ GetKuduToolAbsolutePath(), "cdc", "consume", master_addr_,
                       kTwoTabletTable, "--from=earliest" });
  consume.ShareParentStdout(false);
  consume.ShareParentStderr(false);
  ASSERT_OK(consume.Start());
  auto kill = MakeScopedCleanup([&]() { WARN_NOT_OK(consume.Kill(SIGKILL), "kill failed"); });
  // Give the first tablet's poller the time to find its position expired; it
  // does so on its first read.
  SleepFor(MonoDelta::FromSeconds(3));
  ASSERT_OK(consume.Kill(SIGINT));
  ASSERT_OK(consume.Wait());
  kill.cancel();
  int exit_status = 0;
  string info;
  ASSERT_OK(consume.GetExitStatus(&exit_status, &info));
  ASSERT_NE(0, exit_status) << info;
}

// A temporary stream is ephemeral: when the tool is killed before it can
// delete the stream, the master deletes it once it has been idle long enough,
// so it does not pin the WAL until --cdc_stream_expiry_ms.
TEST_F(CDCToolITest, ConsumeKilledLeavesNoStream) {
  FLAGS_cdc_bg_scan_interval_ms = 200;
  FLAGS_cdc_active_time_report_interval_ms = 200;
  FLAGS_cdc_ephemeral_stream_expiry_ms = 3000;
  NO_FATALS(StartClusterAndCreateTable());
  NO_FATALS(InsertRowsInOneWrite(0, 1));

  Subprocess consume({ GetKuduToolAbsolutePath(), "cdc", "consume", master_addr_,
                       kTableName, "--from=earliest" });
  consume.ShareParentStdout(false);
  consume.ShareParentStderr(false);
  ASSERT_OK(consume.Start());
  string stream_id;
  ASSERT_EVENTUALLY([&] {
    ListCDCStreamsResponsePB resp;
    ASSERT_OK(ListAllStreams(&resp));
    ASSERT_EQ(1, resp.streams_size());
    ASSERT_TRUE(resp.streams(0).ephemeral());
    ASSERT_EQ(SysCDCStreamEntryPB::ACTIVE, resp.streams(0).state());
    stream_id = resp.streams(0).stream_id();
  });
  // While the tool runs, its stream stays.
  SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_cdc_ephemeral_stream_expiry_ms));
  {
    ListCDCStreamsResponsePB resp;
    ASSERT_OK(ListAllStreams(&resp));
    ASSERT_EQ(1, resp.streams_size());
    ASSERT_EQ(SysCDCStreamEntryPB::ACTIVE, resp.streams(0).state());
  }

  ASSERT_OK(consume.KillAndWait(SIGKILL));
  ASSERT_EVENTUALLY([&] {
    ListCDCStreamsResponsePB resp;
    ASSERT_OK(ListAllStreams(&resp));
    ASSERT_EQ(0, resp.streams_size()) << resp.ShortDebugString();
  });
}

} // namespace kudu
