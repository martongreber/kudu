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

// Integration test for the MCP server's tools/call dispatch. Drives the
// JSON-RPC serve loop in-process against a live ExternalMiniCluster, exercising
// the master_addresses injection path (the addresses are supplied only via
// FLAGS_master_addresses, never in the tool arguments) and asserting that the
// captured cout output of each read action is returned in the tool result.

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <istream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"

// The serve-level master addresses, defined in the master library and read by
// the MCP tools/call injection path (tool_action_mcp.cc).
DECLARE_string(master_addresses);
// The write gate (tool_action_mcp.cc). Set true (save/restore) so GATED tools
// like table_add_range_partition enter the registry and are dispatchable.
DECLARE_bool(allow_writes);
// Running a tool Action in-process calls kudu::ValidateFlags(), which runs the
// clock guardrail. KuduTest defaults --time_source to system_unsync, so the
// guardrail requires --unlock_unsafe_flags to be set.
DECLARE_bool(unlock_unsafe_flags);

using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableCreator;
using std::istringstream;
using std::ostringstream;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

// Defined in tool_action_mcp.cc. Declared here so the JSON-RPC loop can be
// driven directly against in-memory streams.
Status RunMcpServeLoop(std::istream& in, std::ostream& out);

// The child-process outcome mapping, defined at namespace scope in
// tool_action_mcp.cc. Redeclared here (identical definition; ODR-compatible)
// so the crash / timeout / exit-code mapping can be unit tested deterministically
// without spawning a real crashing binary.
struct ToolOutcome {
  string text;
  bool is_error;
};
ToolOutcome InterpretChildOutcome(int wait_status, bool timed_out,
                                  int timeout_seconds, const string& out,
                                  const string& err);

namespace {

// The parsed MCP tool result from a 'tools/call' JSON-RPC response.
struct ToolResult {
  bool is_error;
  string text;  // content[0].text
};

// Runs a single 'tools/call' request through the serve loop and parses the tool
// result. 'arguments_json' is the raw JSON object for params.arguments.
ToolResult CallTool(const string& tool_name, const string& arguments_json) {
  const string req = Substitute(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"$0\",\"arguments\":$1}}\n",
      tool_name, arguments_json);
  istringstream in(req);
  ostringstream out;
  CHECK_OK(RunMcpServeLoop(in, out));

  // Exactly one response line; parse it (which also asserts valid JSON).
  string line;
  {
    istringstream iss(out.str());
    CHECK(std::getline(iss, line)) << "no response line for " << tool_name;
    string extra;
    CHECK(!std::getline(iss, extra))
        << "more than one response line for " << tool_name;
  }

  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  ToolResult tr;
  tr.is_error = false;
  if (result->HasMember("isError")) {
    CHECK_OK(reader.ExtractBool(result, "isError", &tr.is_error));
  }
  vector<const rapidjson::Value*> content;
  CHECK_OK(reader.ExtractObjectArray(result, "content", &content));
  CHECK(!content.empty()) << line;
  CHECK_OK(reader.ExtractString(content[0], "text", &tr.text));
  return tr;
}

// Writes all of 'data' to 'fd', retrying short writes and EINTR. Used to feed
// newline-delimited JSON-RPC requests into a spawned server's stdin.
Status WriteFull(int fd, const string& data) {
  size_t written = 0;
  while (written < data.size()) {
    const ssize_t n = ::write(fd, data.data() + written, data.size() - written);
    if (n < 0) {
      if (errno == EINTR) continue;
      return Status::IOError("write to child stdin failed", std::strerror(errno));
    }
    written += n;
  }
  return Status::OK();
}

// Reads from 'fd' until EOF, appending everything to '*out'. Used to drain the
// spawned server's stdout after its stdin has been closed (so it exits).
Status ReadUntilEof(int fd, string* out) {
  char buf[4096];
  while (true) {
    const ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) continue;
      return Status::IOError("read from child stdout failed", std::strerror(errno));
    }
    if (n == 0) break;  // EOF: the child closed stdout (i.e. exited).
    out->append(buf, n);
  }
  return Status::OK();
}

// Splits newline-delimited server output into individual JSON-RPC lines,
// dropping the trailing empty element from the final newline.
vector<string> SplitResponseLines(const string& out) {
  vector<string> lines;
  istringstream iss(out);
  string line;
  while (std::getline(iss, line)) {
    if (!line.empty()) lines.push_back(line);
  }
  return lines;
}

// Extracts the JSON-RPC error code from a response 'line'. CHECK-fails if the
// line is not an error envelope.
int ErrorCode(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "error", &err));
  int32_t code = 0;
  CHECK_OK(reader.ExtractInt32(err, "code", &code));
  return code;
}

// Extracts the error 'message' string from an error response 'line'.
string ErrorMessage(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "error", &err));
  string message;
  CHECK_OK(reader.ExtractString(err, "message", &message));
  return message;
}

// Parses a 'tools/call' success response 'line' into its MCP tool result.
ToolResult ParseToolResult(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  ToolResult tr;
  tr.is_error = false;
  if (result->HasMember("isError")) {
    CHECK_OK(reader.ExtractBool(result, "isError", &tr.is_error));
  }
  vector<const rapidjson::Value*> content;
  CHECK_OK(reader.ExtractObjectArray(result, "content", &content));
  CHECK(!content.empty()) << line;
  CHECK_OK(reader.ExtractString(content[0], "text", &tr.text));
  return tr;
}

// Collects the "name" of every tool in a 'tools/list' response 'line'.
vector<string> ToolNames(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  CHECK_OK(reader.ExtractObjectArray(result, "tools", &tools));
  vector<string> names;
  for (const auto* t : tools) {
    string n;
    CHECK_OK(reader.ExtractString(t, "name", &n));
    names.push_back(n);
  }
  return names;
}

bool ContainsName(const vector<string>& v, const string& s) {
  return std::find(v.begin(), v.end(), s) != v.end();
}

} // anonymous namespace

class ToolActionMcpITest : public ExternalMiniClusterITestBase {
 protected:
  void SetUp() override {
    ExternalMiniClusterITestBase::SetUp();
    // Tool actions run in-process here and call kudu::ValidateFlags(); satisfy
    // the clock guardrail given KuduTest's system_unsync default time source.
    // KuduTest's FlagSaver restores this after the test.
    FLAGS_unlock_unsafe_flags = true;
  }

  // Creates a RANGE-partitioned table on a single int32 primary-key column
  // "key" with one initial range partition [0, 100). Uses the client API
  // directly (not TestWorkload, whose tables are hash-partitioned) so the gated
  // write has a real range partitioning to extend.
  void CreateRangePartitionedTable(const string& table_name) {
    KuduSchema schema;
    {
      KuduSchemaBuilder b;
      b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull();
      b.SetPrimaryKey({ "key" });
      ASSERT_OK(b.Build(&schema));
    }
    unique_ptr<KuduPartialRow> lower(schema.NewRow());
    ASSERT_OK(lower->SetInt32("key", 0));
    unique_ptr<KuduPartialRow> upper(schema.NewRow());
    ASSERT_OK(upper->SetInt32("key", 100));

    unique_ptr<KuduTableCreator> tc(client_->NewTableCreator());
    ASSERT_OK(tc->table_name(table_name)
                  .schema(&schema)
                  .set_range_partition_columns({ "key" })
                  .add_range_partition(lower.release(), upper.release())
                  .num_replicas(1)
                  .Create());
  }
};

// Happy path: against a live cluster, tools/call runs read-only actions and
// returns their captured cout output. The master
// addresses are provided ONLY through FLAGS_master_addresses (the injection
// path) -- they are never passed in the tool arguments.
TEST_F(ToolActionMcpITest, ReadOnlyToolsCallReturnsCapturedOutput) {
  NO_FATALS(StartCluster({}, {}, /*num_tablet_servers=*/1));

  // Create a table with a known name so table_list / table_describe have
  // something to report.
  const string kTableName = "mcp_itest_table";
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  // Inject the cluster's master address via the serve-level flag; do NOT pass
  // master_addresses in any tool's arguments.
  const string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  const string saved_master_addresses = FLAGS_master_addresses;
  FLAGS_master_addresses = master_addr;
  SCOPED_CLEANUP({ FLAGS_master_addresses = saved_master_addresses; });

  // master_status: a MasterActionBuilder action -> requires the singular
  // 'master_address', injected from the first configured master address. Its
  // output is the server status proto debug string.
  {
    const ToolResult tr = CallTool("master_status", "{}");
    ASSERT_FALSE(tr.is_error) << tr.text;
    // Stable ServerStatusPB debug-string fields.
    EXPECT_NE(string::npos, tr.text.find("permanent_uuid")) << tr.text;
    EXPECT_NE(string::npos, tr.text.find("bound_rpc_addresses")) << tr.text;
  }

  // table_list: a ClusterActionBuilder action -> requires the plural
  // 'master_addresses', injected from FLAGS_master_addresses. The created table
  // must appear.
  {
    const ToolResult tr = CallTool("table_list", "{}");
    ASSERT_FALSE(tr.is_error) << tr.text;
    EXPECT_NE(string::npos, tr.text.find(kTableName))
        << "table_list output missing the table name: " << tr.text;
  }

  // table_describe: takes the model-supplied 'table_name' plus the injected
  // master addresses. Output must mention the table and its schema.
  {
    const ToolResult tr = CallTool(
        "table_describe", Substitute("{\"table_name\":\"$0\"}", kTableName));
    ASSERT_FALSE(tr.is_error) << tr.text;
    EXPECT_NE(string::npos, tr.text.find(kTableName))
        << "table_describe output missing the table name: " << tr.text;
    // TestWorkload's default schema has an integer key column named "key".
    EXPECT_NE(string::npos, tr.text.find("key"))
        << "table_describe output missing schema columns: " << tr.text;
  }
}

// A model-supplied caller may still override the injected master addresses by
// passing them explicitly; the explicit value wins over FLAGS_master_addresses.
TEST_F(ToolActionMcpITest, ExplicitMasterAddressesOverrideInjection) {
  NO_FATALS(StartCluster({}, {}, /*num_tablet_servers=*/1));

  const string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  // Leave FLAGS_master_addresses unset to prove the explicit argument is used.
  const string saved_master_addresses = FLAGS_master_addresses;
  FLAGS_master_addresses = "";
  SCOPED_CLEANUP({ FLAGS_master_addresses = saved_master_addresses; });

  const ToolResult tr = CallTool(
      "table_list", Substitute("{\"master_addresses\":\"$0\"}", master_addr));
  ASSERT_FALSE(tr.is_error) << tr.text;
}

// Happy path: with --allow-writes, a real
// table_add_range_partition executes and the new range is observably created on
// the cluster. Observation is robust: re-adding the identical range via a second
// tools/call must now FAIL (already present / overlapping), which it could only
// do if the first add actually persisted.
TEST_F(ToolActionMcpITest, GatedAddRangePartitionExecutesAndIsObservable) {
  NO_FATALS(StartCluster({}, {}, /*num_tablet_servers=*/1));

  const string kTableName = "mcp_m5_add_range";
  NO_FATALS(CreateRangePartitionedTable(kTableName));

  // Inject the master address via the serve-level flag; open the write gate.
  const string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  const string saved_master_addresses = FLAGS_master_addresses;
  FLAGS_master_addresses = master_addr;
  const bool saved_allow_writes = FLAGS_allow_writes;
  FLAGS_allow_writes = true;
  SCOPED_CLEANUP({
    FLAGS_master_addresses = saved_master_addresses;
    FLAGS_allow_writes = saved_allow_writes;
  });

  // Add a brand-new range [100, 200): succeeds.
  const string add_args = Substitute(
      "{\"table_name\":\"$0\",\"table_range_lower_bound\":\"[100]\","
      "\"table_range_upper_bound\":\"[200]\"}", kTableName);
  {
    const ToolResult tr = CallTool("table_add_range_partition", add_args);
    ASSERT_FALSE(tr.is_error) << "add of a new range must succeed: " << tr.text;
  }

  // Re-adding the identical range now fails -- proving the first add persisted
  // the partition on the cluster.
  {
    const ToolResult tr = CallTool("table_add_range_partition", add_args);
    ASSERT_TRUE(tr.is_error)
        << "re-adding an existing range must fail (partition was created): "
        << tr.text;
  }
}

// Dry-run leaves the cluster unchanged: a dry_run:true
// table_add_range_partition returns the literal command with isError:false and
// makes ZERO mutations. Proof: a subsequent REAL add of that same range
// SUCCEEDS -- which it could not if the dry-run had created it.
TEST_F(ToolActionMcpITest, DryRunAddRangePartitionMakesNoClusterChange) {
  NO_FATALS(StartCluster({}, {}, /*num_tablet_servers=*/1));

  const string kTableName = "mcp_m5_dryrun_range";
  NO_FATALS(CreateRangePartitionedTable(kTableName));

  const string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  const string saved_master_addresses = FLAGS_master_addresses;
  FLAGS_master_addresses = master_addr;
  const bool saved_allow_writes = FLAGS_allow_writes;
  FLAGS_allow_writes = true;
  SCOPED_CLEANUP({
    FLAGS_master_addresses = saved_master_addresses;
    FLAGS_allow_writes = saved_allow_writes;
  });

  // Dry-run add of range [200, 300): returns the literal command, no mutation.
  {
    const string dry_args = Substitute(
        "{\"table_name\":\"$0\",\"table_range_lower_bound\":\"[200]\","
        "\"table_range_upper_bound\":\"[300]\",\"dry_run\":true}", kTableName);
    const ToolResult tr = CallTool("table_add_range_partition", dry_args);
    ASSERT_FALSE(tr.is_error) << tr.text;
    ASSERT_STR_CONTAINS(tr.text, "kudu table add_range_partition");
  }

  // The real add of the SAME range now succeeds: the dry-run created nothing.
  {
    const string real_args = Substitute(
        "{\"table_name\":\"$0\",\"table_range_lower_bound\":\"[200]\","
        "\"table_range_upper_bound\":\"[300]\"}", kTableName);
    const ToolResult tr = CallTool("table_add_range_partition", real_args);
    ASSERT_FALSE(tr.is_error)
        << "real add after dry-run must succeed (dry-run must not have created "
           "the range): " << tr.text;
  }
}

// ---------------------------------------------------------------------------
// Subprocess-driven end-to-end test.
// ---------------------------------------------------------------------------

// Spawns a REAL 'kudu mcp serve' subprocess (the production binary, not the
// in-process loop), speaks newline-delimited JSON-RPC over its stdin/stdout
// pipes against a live cluster, then closes stdin so the server sees EOF and
// exits cleanly. This is the full-stack exercise the in-process itests above
// cannot give: process spawn, stdio framing, the disposition-filtered tool
// surface, a real read executed end to end, a gated rejection, parser survival
// across a malformed line, and a clean exit-status-0 shutdown on EOF.
//
// All requests are written up front and stdin is then closed; the server
// processes them in order (single-threaded), flushing one response line each,
// and exits at EOF. Requests are tiny, so the write completes without blocking
// and there is no reader/writer deadlock; responses are then drained to EOF.
TEST_F(ToolActionMcpITest, SubprocessServeEndToEnd) {
  NO_FATALS(StartCluster({}, {}, /*num_tablet_servers=*/1));

  // A table so the read (case 3) has an observable name in its output.
  const string kTableName = "mcp_e2e_table";
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  const string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  // Launch the server WITHOUT --allow-writes: GATED tools must stay hidden and
  // a gated call must be rejected. master_addresses is injected via the serve
  // flag, never through tool arguments.
  Subprocess server({
      GetKuduToolAbsolutePath(),
      "mcp",
      "serve",
      Substitute("--master_addresses=$0", master_addr),
  });
  server.ShareParentStdin(false);
  server.ShareParentStdout(false);
  ASSERT_OK(server.Start());

  int in_fd = server.ReleaseChildStdinFd();
  int out_fd = server.ReleaseChildStdoutFd();
  SCOPED_CLEANUP({
    // Defensive: if an assertion aborts the test before we close these, make
    // sure the child is not left blocked on a read forever (each is set to -1
    // once closed on the happy path, so this never double-closes).
    if (in_fd >= 0) ::close(in_fd);
    if (out_fd >= 0) ::close(out_fd);
  });

  // Case 1: initialize. Case 2: tools/list (surface present, gated absent).
  // Case 3: read (table_list). Case 4: gated call rejected. Malformed line ->
  // parse error. Final tools/list proves the server survived the bad line.
  const string requests = Substitute(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
      "\"params\":{\"protocolVersion\":\"2025-06-18\"}}\n"
      "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n"
      "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"table_list\",\"arguments\":{}}}\n"
      "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"table_add_range_partition\",\"arguments\":"
      "{\"table_name\":\"$0\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[1]\"}}}\n"
      "this is not valid json\n"
      "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"tools/list\"}\n",
      kTableName);
  ASSERT_OK(WriteFull(in_fd, requests));

  // Close stdin so the server sees EOF and shuts down after the last request.
  ASSERT_EQ(0, ::close(in_fd));
  in_fd = -1;  // Prevent the SCOPED_CLEANUP from double-closing.

  string output;
  ASSERT_OK(ReadUntilEof(out_fd, &output));
  ASSERT_EQ(0, ::close(out_fd));
  out_fd = -1;

  // The server exited on its own after EOF; it must exit cleanly.
  ASSERT_OK(server.Wait());
  int exit_status = -1;
  ASSERT_OK(server.GetExitStatus(&exit_status));
  ASSERT_EQ(0, exit_status) << "server did not exit 0 after EOF";

  const vector<string> lines = SplitResponseLines(output);
  ASSERT_EQ(6, lines.size()) << "unexpected response framing:\n" << output;

  // Case 1: initialize handshake.
  {
    JsonReader reader(lines[0]);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* result = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
    string protocol_version;
    ASSERT_OK(reader.ExtractString(result, "protocolVersion", &protocol_version));
    ASSERT_FALSE(protocol_version.empty());
    const rapidjson::Value* server_info = nullptr;
    ASSERT_OK(reader.ExtractObject(result, "serverInfo", &server_info));
    string name;
    ASSERT_OK(reader.ExtractString(server_info, "name", &name));
    ASSERT_EQ("kudu-mcp", name);
    const rapidjson::Value* capabilities = nullptr;
    ASSERT_OK(reader.ExtractObject(result, "capabilities", &capabilities));
    const rapidjson::Value* tools = nullptr;
    ASSERT_OK(reader.ExtractObject(capabilities, "tools", &tools));
  }

  // Case 2: tools/list surfaces the SURFACE read and hides the GATED write
  // (server launched without --allow-writes).
  {
    const vector<string> names = ToolNames(lines[1]);
    EXPECT_TRUE(ContainsName(names, "table_list")) << lines[1];
    EXPECT_FALSE(ContainsName(names, "table_add_range_partition")) << lines[1];
  }

  // Case 3: the read ran end to end against the cluster and returned the table.
  {
    const ToolResult tr = ParseToolResult(lines[2]);
    ASSERT_FALSE(tr.is_error) << tr.text;
    EXPECT_NE(string::npos, tr.text.find(kTableName))
        << "table_list output missing the table name: " << tr.text;
  }

  // Case 4: the gated call is rejected with a JSON-RPC error that names the
  // write gate (not merely hidden).
  {
    ASSERT_EQ(-32602, ErrorCode(lines[3])) << lines[3];
    EXPECT_NE(string::npos, ErrorMessage(lines[3]).find("--allow-writes"))
        << "gated rejection must name --allow-writes: " << lines[3];
  }

  // Malformed line: a -32700 parse error.
  {
    ASSERT_EQ(-32700, ErrorCode(lines[4])) << lines[4];
  }

  // Survival: the request after the malformed line is still answered cleanly.
  {
    JsonReader reader(lines[5]);
    ASSERT_OK(reader.Init());
    int32_t id = 0;
    ASSERT_OK(reader.ExtractInt32(reader.root(), "id", &id));
    ASSERT_EQ(6, id) << lines[5];
    ASSERT_TRUE(reader.root()->HasMember("result")) << lines[5];
  }
}

// ---------------------------------------------------------------------------
// Child-outcome mapping (deterministic, no live cluster needed).
// ---------------------------------------------------------------------------

// InterpretChildOutcome is the sole place a finished child tool process becomes
// an MCP tool result, so it is the crash boundary's decision function. Cover its
// four cases directly -- a clean exit, a non-zero exit, death by signal (the
// CHECK / LOG(FATAL) abort class that used to take the whole server down in
// process), and a timeout -- with hand-built wait statuses, so the signal and
// timeout paths are exercised without needing a binary that actually crashes.
TEST_F(ToolActionMcpITest, InterpretChildOutcomeMapsExitSignalAndTimeout) {
  // Traditional Unix wait-status layout (Linux and macOS): the low 7 bits hold
  // the terminating signal (0 for a normal exit), bit 0x80 marks a core dump,
  // and bits 8..15 hold the exit code. Each case asserts the W* macro
  // precondition first, so a platform whose layout differed would fail loudly
  // here rather than silently mis-test the mapping.
  auto make_exited = [](int code) { return (code & 0xff) << 8; };
  auto make_signaled = [](int sig, bool core) {
    return (sig & 0x7f) | (core ? 0x80 : 0);
  };

  // Clean exit (code 0): not an error; the child's stdout IS the result text,
  // and stderr is ignored.
  {
    const int status = make_exited(0);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(0, WEXITSTATUS(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*out=*/"the tool output", /*err=*/"some progress on stderr");
    EXPECT_FALSE(o.is_error);
    EXPECT_EQ("the tool output", o.text);
  }

  // Non-zero exit: an error whose text names the exit code and carries the
  // child's stderr (the action's own diagnostics).
  {
    const int status = make_exited(1);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(1, WEXITSTATUS(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*out=*/"", /*err=*/"table not found");
    EXPECT_TRUE(o.is_error);
    EXPECT_NE(string::npos, o.text.find("code 1")) << o.text;
    EXPECT_NE(string::npos, o.text.find("table not found")) << o.text;
  }

  // Non-zero exit with empty stderr: the error text falls back to stdout.
  {
    const int status = make_exited(2);
    ASSERT_TRUE(WIFEXITED(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*out=*/"printed to stdout before failing", /*err=*/"");
    EXPECT_TRUE(o.is_error);
    EXPECT_NE(string::npos, o.text.find("printed to stdout before failing"))
        << o.text;
  }

  // Death by signal (SIGABRT: the CHECK / LOG(FATAL) / abort class): an error
  // that names the signal. This is exactly the outcome that, in process, would
  // have terminated the long-lived server.
  {
    const int status = make_signaled(SIGABRT, /*core=*/false);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(SIGABRT, WTERMSIG(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*out=*/"", /*err=*/"Check failed: ...");
    EXPECT_TRUE(o.is_error);
    EXPECT_NE(string::npos, o.text.find(Substitute("signal $0", SIGABRT)))
        << o.text;
  }

  // Death by signal with a core dump: the text notes the core dump.
  {
    const int status = make_signaled(SIGSEGV, /*core=*/true);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(SIGSEGV, WTERMSIG(status));
    ASSERT_TRUE(WCOREDUMP(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*out=*/"", /*err=*/"");
    EXPECT_TRUE(o.is_error);
    EXPECT_NE(string::npos, o.text.find("core dumped")) << o.text;
  }

  // Timeout: an error naming the deadline; 'timed_out' wins over whatever the
  // (post-SIGKILL) wait status happens to be.
  {
    const int status = make_signaled(SIGKILL, /*core=*/false);
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/true, /*timeout_seconds=*/42,
        /*out=*/"partial", /*err=*/"");
    EXPECT_TRUE(o.is_error);
    EXPECT_NE(string::npos, o.text.find("42 seconds")) << o.text;
  }
}

// End-to-end crash isolation: a REAL 'kudu mcp serve' subprocess must survive a
// tool call that fails -- and, crucially, repeated calls that in the old
// in-process design tripped the non-idempotent global-state crash
// ('txn list --included_states=*' aborted the whole server on the SECOND call,
// KUDU issue this change fixes). Each tools/call now runs in a fresh child, so
// the validator runs once into an empty global every time and the server can
// never be taken down by an action. We send two such calls back to back, then a
// known-good read, and assert the server answered ALL THREE and exited cleanly
// on EOF.
TEST_F(ToolActionMcpITest, SubprocessServeSurvivesRepeatedFailingToolCalls) {
  NO_FATALS(StartCluster({}, {}, /*num_tablet_servers=*/1));

  const string kTableName = "mcp_isolation_table";
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableName);
  workload.set_num_replicas(1);
  workload.Setup();

  const string master_addr = cluster_->master()->bound_rpc_addr().ToString();

  Subprocess server({
      GetKuduToolAbsolutePath(),
      "mcp",
      "serve",
      Substitute("--master_addresses=$0", master_addr),
  });
  server.ShareParentStdin(false);
  server.ShareParentStdout(false);
  ASSERT_OK(server.Start());

  int in_fd = server.ReleaseChildStdinFd();
  int out_fd = server.ReleaseChildStdoutFd();
  SCOPED_CLEANUP({
    if (in_fd >= 0) ::close(in_fd);
    if (out_fd >= 0) ::close(out_fd);
  });

  // Two txn_list calls with included_states="*" (the historical crash trigger),
  // then a valid table_list. txn_list is SURFACE, so no --allow-writes needed.
  // No txn status table exists on this cluster, so each txn_list is expected to
  // come back isError=true -- what matters is that BOTH are answered (the server
  // did not abort on the second) and the following table_list still works.
  const string requests =
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"txn_list\",\"arguments\":{\"included_states\":\"*\"}}}\n"
      "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"txn_list\",\"arguments\":{\"included_states\":\"*\"}}}\n"
      "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"table_list\",\"arguments\":{}}}\n";
  ASSERT_OK(WriteFull(in_fd, requests));

  ASSERT_EQ(0, ::close(in_fd));
  in_fd = -1;

  string output;
  ASSERT_OK(ReadUntilEof(out_fd, &output));
  ASSERT_EQ(0, ::close(out_fd));
  out_fd = -1;

  // The server exited on its own after EOF -- and cleanly (exit 0), NOT on a
  // signal. A crash would show up as a non-zero / signal exit and missing lines.
  ASSERT_OK(server.Wait());
  int exit_status = -1;
  ASSERT_OK(server.GetExitStatus(&exit_status));
  ASSERT_EQ(0, exit_status) << "serve process did not exit cleanly after EOF; "
                               "a tool call may have taken it down";

  const vector<string> lines = SplitResponseLines(output);
  ASSERT_EQ(3, lines.size())
      << "server did not answer all three requests (it may have crashed on the "
         "second failing call):\n" << output;

  // Both txn_list calls were answered (the point of the test: the second did not
  // abort the server). Their isError value is not what matters here.
  for (int i = 0; i < 2; i++) {
    JsonReader reader(lines[i]);
    ASSERT_OK(reader.Init());
    ASSERT_TRUE(reader.root()->HasMember("result")) << lines[i];
  }

  // The valid read after the two failing calls still works end to end.
  {
    const ToolResult tr = ParseToolResult(lines[2]);
    ASSERT_FALSE(tr.is_error) << tr.text;
    EXPECT_NE(string::npos, tr.text.find(kTableName))
        << "table_list output missing the table name: " << tr.text;
  }
}

// Regression: a positional argument value that begins with '-' must reach the
// child 'kudu' as literal data, NOT be reinterpreted as a child flag. The child's
// gflags parser (ParseCommandLineNonHelpFlags, remove_flags=true) would otherwise
// consume a value like "--version" or "--help" from any argv position; BuildToolArgv
// guards against that with a "--" end-of-flags sentinel before the positionals.
// Without the sentinel, "--version" made the child print its version and exit 0,
// i.e. the wrong action ran and the call falsely reported success.
TEST_F(ToolActionMcpITest, FlagLikePositionalIsTreatedAsData) {
  NO_FATALS(StartCluster({}, {}, /*num_tablet_servers=*/1));

  const string master_addr = cluster_->master()->bound_rpc_addr().ToString();
  const string saved_master_addresses = FLAGS_master_addresses;
  FLAGS_master_addresses = master_addr;
  SCOPED_CLEANUP({ FLAGS_master_addresses = saved_master_addresses; });

  // Each flag-like table name must be handled as a (nonexistent) table name: the
  // action actually runs and fails with a "table does not exist" error naming the
  // literal value -- proving the value was not swallowed as a child flag.
  for (const char* flag_like : { "--version", "--help", "--flagfile=/etc/passwd" }) {
    const ToolResult tr = CallTool(
        "table_describe", Substitute("{\"table_name\":\"$0\"}", flag_like));
    ASSERT_TRUE(tr.is_error)
        << "flag-like table name '" << flag_like << "' was not treated as data: "
        << tr.text;
    EXPECT_NE(string::npos, tr.text.find("does not exist"))
        << "expected a 'table does not exist' error for '" << flag_like
        << "', got: " << tr.text;
    EXPECT_NE(string::npos, tr.text.find(flag_like))
        << "error should echo the literal table name '" << flag_like
        << "', got: " << tr.text;
    // The child version banner must never appear: that would mean "--version" was
    // parsed as a flag and the describe action never ran.
    EXPECT_EQ(string::npos, tr.text.find("build type"))
        << "child interpreted '" << flag_like << "' as a flag: " << tr.text;
  }
}

} // namespace tools
} // namespace kudu
