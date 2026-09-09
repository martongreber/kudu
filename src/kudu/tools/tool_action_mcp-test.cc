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

#include <algorithm>
#include <cstdint>
#include <istream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/mcp_disposition.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

// The write gate, defined in tool_action_mcp.cc. Toggled in-process to exercise
// SURFACE-only vs SURFACE+GATED tool listings.
DECLARE_bool(allow_writes);

// The serve-level master addresses, injected into tools/call dispatch. Set
// in-process to exercise the injection path (and its "not configured" error).
DECLARE_string(master_addresses);

using std::istringstream;
using std::ostringstream;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

// Defined in tool_action_mcp.cc. Declared here so the JSON-RPC loop can be
// exercised directly against in-memory streams.
Status RunMcpServeLoop(std::istream& in, std::ostream& out);

namespace {

// Splits 'out' into individual response lines, dropping the trailing empty
// element that a final newline produces.
vector<string> ResponseLines(const string& out) {
  vector<string> lines;
  istringstream iss(out);
  string line;
  while (std::getline(iss, line)) {
    lines.push_back(line);
  }
  return lines;
}

// Runs the serve loop over 'input' and returns the response lines.
vector<string> RunAndCollect(const string& input) {
  istringstream in(input);
  ostringstream out;
  CHECK(RunMcpServeLoop(in, out).ok());
  return ResponseLines(out.str());
}

// Asserts that 'line' is a well-formed JSON-RPC 2.0 envelope: it parses, is an
// object, carries "jsonrpc":"2.0", and does not contain both 'result' and
// 'error'. On success, sets '*reader' up so callers can inspect it further.
void AssertValidEnvelope(const string& line, JsonReader* reader) {
  ASSERT_OK(reader->Init());
  const rapidjson::Value* root = reader->root();
  ASSERT_TRUE(root->IsObject()) << "response is not a JSON object: " << line;

  string jsonrpc;
  ASSERT_OK(reader->ExtractString(root, "jsonrpc", &jsonrpc));
  ASSERT_EQ("2.0", jsonrpc);

  const bool has_result = root->HasMember("result");
  const bool has_error = root->HasMember("error");
  ASSERT_FALSE(has_result && has_error)
      << "response carries both result and error: " << line;
  ASSERT_TRUE(has_result || has_error)
      << "response carries neither result nor error: " << line;
}

// Runs a single 'tools/list' request through the loop and returns the one
// response line. 'allow_writes' toggles FLAGS_allow_writes around the call
// (saved and restored) to exercise the write gate in-process.
string RunToolsList(bool allow_writes) {
  const bool saved = FLAGS_allow_writes;
  FLAGS_allow_writes = allow_writes;
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n");
  FLAGS_allow_writes = saved;
  CHECK_EQ(1, lines.size());
  return lines[0];
}

// Collects the "name" of every tool in a 'tools/list' response line.
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

bool Contains(const vector<string>& v, const string& s) {
  return std::find(v.begin(), v.end(), s) != v.end();
}

// Extracts the integer error code from a JSON-RPC error response line.
int ErrorCode(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "error", &err));
  int32_t code = 0;
  CHECK_OK(reader.ExtractInt32(err, "code", &code));
  return code;
}

// The parsed MCP tool result from a successful 'tools/call' JSON-RPC response.
struct ToolResult {
  bool is_error;
  string text;  // content[0].text
};

// Parses a 'tools/call' success response line into its MCP tool result. Fails
// (via CHECK) if the line is not a well-formed result carrying content[0].text;
// this doubles as the R3 assertion that the protocol line is valid JSON.
ToolResult ParseToolResult(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  ToolResult tr;
  tr.is_error = false;
  // isError is present on M4 results (false on success, true on tool error).
  if (result->HasMember("isError")) {
    CHECK_OK(reader.ExtractBool(result, "isError", &tr.is_error));
  }
  vector<const rapidjson::Value*> content;
  CHECK_OK(reader.ExtractObjectArray(result, "content", &content));
  CHECK(!content.empty()) << "tool result has empty content: " << line;
  string type;
  CHECK_OK(reader.ExtractString(content[0], "type", &type));
  CHECK_EQ("text", type) << line;
  CHECK_OK(reader.ExtractString(content[0], "text", &tr.text));
  return tr;
}

// Runs a single 'tools/call' request through the loop and returns the one
// response line. 'arguments_json' is the raw JSON object for params.arguments.
string RunToolsCall(const string& tool_name, const string& arguments_json) {
  const string req = Substitute(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"$0\",\"arguments\":$1}}\n",
      tool_name, arguments_json);
  vector<string> lines = RunAndCollect(req);
  CHECK_EQ(1, lines.size()) << "expected exactly one response line";
  return lines[0];
}

// RAII helper: sets FLAGS_master_addresses for the duration of a test and
// restores the prior value on destruction, so injection-path tests do not leak
// state into other tests.
class ScopedMasterAddresses {
 public:
  explicit ScopedMasterAddresses(const string& value) : saved_(FLAGS_master_addresses) {
    FLAGS_master_addresses = value;
  }
  ~ScopedMasterAddresses() { FLAGS_master_addresses = saved_; }
 private:
  const string saved_;
};

// RAII helper: toggles FLAGS_allow_writes for the duration of a test (so the
// serve loop surfaces GATED tools and the dispatch path admits them) and
// restores the prior value on destruction.
class ScopedAllowWrites {
 public:
  explicit ScopedAllowWrites(bool value) : saved_(FLAGS_allow_writes) {
    FLAGS_allow_writes = value;
  }
  ~ScopedAllowWrites() { FLAGS_allow_writes = saved_; }
 private:
  const bool saved_;
};

// Recursively walks the action tree rooted at 'mode' (whose full mode chain
// from the root is 'chain', with 'mode' as its last element), invoking 'visit'
// with (chain-to-parent, action) for every leaf action. Mirrors the traversal
// the registry builder uses, so the test sees exactly the action set the server
// classifies.
template <typename Fn>
void VisitActions(Mode* mode, vector<Mode*>* chain, const Fn& visit) {
  chain->push_back(mode);
  for (const auto& action : mode->actions()) {
    visit(*chain, action.get());
  }
  for (const auto& submode : mode->modes()) {
    VisitActions(submode.get(), chain, visit);
  }
  chain->pop_back();
}

// The MCP tool name the server would assign to 'action' given its 'chain'.
// Recomputed here from the exported DispositionCommandPath() (drop the root
// name; join with spaces) with spaces mapped to underscores -- identical to
// McpToolName() in tool_action_mcp.cc, without widening that function's
// visibility.
string McpToolNameForTest(const vector<Mode*>& chain, const Action* action) {
  string name = DispositionCommandPath(chain, action);
  std::replace(name.begin(), name.end(), ' ', '_');
  return name;
}

} // anonymous namespace

// Happy path: 'initialize' returns protocolVersion, serverInfo, and a tools
// capability, and echoes the request id.
TEST(ToolActionMcpTest, InitializeReturnsHandshake) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
      "\"params\":{\"protocolVersion\":\"2025-06-18\"}}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  const rapidjson::Value* root = reader.root();

  // id echoed back as the number it was sent as.
  int32_t id = 0;
  ASSERT_OK(reader.ExtractInt32(root, "id", &id));
  ASSERT_EQ(1, id);

  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(root, "result", &result));

  string protocol_version;
  ASSERT_OK(reader.ExtractString(result, "protocolVersion", &protocol_version));
  ASSERT_EQ("2025-06-18", protocol_version);

  // serverInfo.name / serverInfo.version present.
  const rapidjson::Value* server_info = nullptr;
  ASSERT_OK(reader.ExtractObject(result, "serverInfo", &server_info));
  string name;
  ASSERT_OK(reader.ExtractString(server_info, "name", &name));
  ASSERT_EQ("kudu-mcp", name);
  string version;
  ASSERT_OK(reader.ExtractString(server_info, "version", &version));
  ASSERT_FALSE(version.empty());

  // capabilities.tools present (an object).
  const rapidjson::Value* capabilities = nullptr;
  ASSERT_OK(reader.ExtractObject(result, "capabilities", &capabilities));
  const rapidjson::Value* tools = nullptr;
  ASSERT_OK(reader.ExtractObject(capabilities, "tools", &tools));
  // v1 declares no resources capability.
  ASSERT_FALSE(capabilities->HasMember("resources"));
}

// When the client requests an unsupported protocolVersion, the server replies
// with its own default supported version rather than echoing the request.
TEST(ToolActionMcpTest, InitializeFallsBackToServerVersionWhenUnsupported) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"initialize\","
      "\"params\":{\"protocolVersion\":\"1999-01-01\"}}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  string protocol_version;
  ASSERT_OK(reader.ExtractString(result, "protocolVersion", &protocol_version));
  ASSERT_NE("1999-01-01", protocol_version);
  ASSERT_FALSE(protocol_version.empty());
}

// A string id is echoed back exactly as a string.
TEST(ToolActionMcpTest, StringIdIsEchoedExactly) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":\"abc-123\",\"method\":\"initialize\","
      "\"params\":{}}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  string id;
  ASSERT_OK(reader.ExtractString(reader.root(), "id", &id));
  ASSERT_EQ("abc-123", id);
}

// A numeric id is echoed back as a number (not stringified).
TEST(ToolActionMcpTest, NumericIdIsEchoedAsNumber) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/list\"}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  // Must extract as an int, proving it was written as a JSON number.
  int32_t id = 0;
  ASSERT_OK(reader.ExtractInt32(reader.root(), "id", &id));
  ASSERT_EQ(42, id);
}

// Happy path (M3): 'tools/list' now reflects the action tree into MCP tool
// objects, filtered by the disposition table. Without --allow-writes the list
// is non-empty and SURFACE-only. Every tool is well-formed: a non-empty name
// and an inputSchema object whose type is "object".
TEST(ToolActionMcpTest, ToolsListReflectsSurfaceToolsByDefault) {
  const string line = RunToolsList(/*allow_writes=*/false);

  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));
  ASSERT_FALSE(tools.empty());

  for (const auto* t : tools) {
    string name;
    ASSERT_OK(reader.ExtractString(t, "name", &name));
    ASSERT_FALSE(name.empty()) << "tool with empty name in: " << line;
    const rapidjson::Value* schema = nullptr;
    ASSERT_OK(reader.ExtractObject(t, "inputSchema", &schema));
    string type;
    ASSERT_OK(reader.ExtractString(schema, "type", &type));
    ASSERT_EQ("object", type) << "tool '" << name << "' inputSchema.type";
  }

  // A representative SURFACE read is present; a GATED write is not (no gate).
  const vector<string> names = ToolNames(line);
  EXPECT_TRUE(Contains(names, "table_describe")) << line;
  EXPECT_TRUE(Contains(names, "master_status")) << line;
  EXPECT_FALSE(Contains(names, "table_delete")) << line;
}

// Happy path (M3): a known SURFACE read action emits a valid JSON Schema:
// - inputSchema.type == "object";
// - its required positional arg is a string property listed in "required";
// - an optional bool flag maps to a "boolean" property NOT in "required".
TEST(ToolActionMcpTest, SurfaceToolInputSchemaIsWellFormed) {
  const string line = RunToolsList(/*allow_writes=*/false);

  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));

  const rapidjson::Value* describe = nullptr;
  for (const auto* t : tools) {
    string n;
    ASSERT_OK(reader.ExtractString(t, "name", &n));
    if (n == "table_describe") {
      describe = t;
      break;
    }
  }
  ASSERT_NE(nullptr, describe) << "table_describe not surfaced: " << line;

  const rapidjson::Value* schema = nullptr;
  ASSERT_OK(reader.ExtractObject(describe, "inputSchema", &schema));
  string type;
  ASSERT_OK(reader.ExtractString(schema, "type", &type));
  ASSERT_EQ("object", type);

  const rapidjson::Value* props = nullptr;
  ASSERT_OK(reader.ExtractObject(schema, "properties", &props));

  // Required arg "table_name" is a string property.
  ASSERT_TRUE(props->HasMember("table_name")) << line;
  const rapidjson::Value* table_name = nullptr;
  ASSERT_OK(reader.ExtractObject(props, "table_name", &table_name));
  string table_name_type;
  ASSERT_OK(reader.ExtractString(table_name, "type", &table_name_type));
  ASSERT_EQ("string", table_name_type);

  // "table_name" is listed in required[].
  vector<const rapidjson::Value*> required;
  ASSERT_OK(reader.ExtractObjectArray(schema, "required", &required));
  bool found_required = false;
  for (const auto* r : required) {
    ASSERT_TRUE(r->IsString());
    if (string(r->GetString()) == "table_name") {
      found_required = true;
    }
  }
  ASSERT_TRUE(found_required) << "table_name not in required[]: " << line;

  // Optional bool flag "show_attributes" maps to a boolean property and is NOT
  // required.
  ASSERT_TRUE(props->HasMember("show_attributes")) << line;
  const rapidjson::Value* show_attributes = nullptr;
  ASSERT_OK(reader.ExtractObject(props, "show_attributes", &show_attributes));
  string show_attributes_type;
  ASSERT_OK(reader.ExtractString(show_attributes, "type", &show_attributes_type));
  ASSERT_EQ("boolean", show_attributes_type);
  for (const auto* r : required) {
    ASSERT_NE("show_attributes", string(r->GetString()));
  }
}

// Happy path (M3): an optional integer gflag maps to an "integer" JSON Schema
// type (proving the gflag-type -> JSON-type mapping beyond bool). 'table scan'
// is SURFACE and declares the int32 flag 'scan_batch_size'.
TEST(ToolActionMcpTest, OptionalIntegerFlagMapsToIntegerType) {
  const string line = RunToolsList(/*allow_writes=*/false);

  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));

  const rapidjson::Value* scan = nullptr;
  for (const auto* t : tools) {
    string n;
    ASSERT_OK(reader.ExtractString(t, "name", &n));
    if (n == "table_scan") {
      scan = t;
      break;
    }
  }
  ASSERT_NE(nullptr, scan) << "table_scan not surfaced: " << line;

  const rapidjson::Value* schema = nullptr;
  ASSERT_OK(reader.ExtractObject(scan, "inputSchema", &schema));
  const rapidjson::Value* props = nullptr;
  ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
  ASSERT_TRUE(props->HasMember("scan_batch_size")) << line;
  const rapidjson::Value* batch = nullptr;
  ASSERT_OK(reader.ExtractObject(props, "scan_batch_size", &batch));
  string batch_type;
  ASSERT_OK(reader.ExtractString(batch, "type", &batch_type));
  ASSERT_EQ("integer", batch_type);
}

// Happy path (M3): flipping FLAGS_allow_writes surfaces GATED tools, and a
// GATED tool carries the not-read-only / destructive annotations.
TEST(ToolActionMcpTest, GatedToolsAppearOnlyWithAllowWritesAndAreTagged) {
  // Absent when the gate is closed.
  EXPECT_FALSE(Contains(ToolNames(RunToolsList(false)), "table_delete"));

  // Present when the gate is open.
  const string line = RunToolsList(/*allow_writes=*/true);
  const vector<string> names = ToolNames(line);
  ASSERT_TRUE(Contains(names, "table_delete")) << line;
  // SURFACE tools are still present alongside the GATED ones.
  EXPECT_TRUE(Contains(names, "table_describe")) << line;

  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));

  const rapidjson::Value* del = nullptr;
  for (const auto* t : tools) {
    string n;
    ASSERT_OK(reader.ExtractString(t, "name", &n));
    if (n == "table_delete") {
      del = t;
      break;
    }
  }
  ASSERT_NE(nullptr, del);

  const rapidjson::Value* annotations = nullptr;
  ASSERT_OK(reader.ExtractObject(del, "annotations", &annotations));
  bool read_only = true;
  ASSERT_OK(reader.ExtractBool(annotations, "readOnlyHint", &read_only));
  ASSERT_FALSE(read_only) << "GATED tool must not be read-only: " << line;
  bool destructive = false;
  ASSERT_OK(reader.ExtractBool(annotations, "destructiveHint", &destructive));
  ASSERT_TRUE(destructive) << "GATED tool must be destructive-hinted: " << line;
}

// Bad path (M3): REJECT (cluster_rebalance) and EXCLUDE (pbc_edit) tools NEVER
// appear, in either the default mode or with --allow-writes.
TEST(ToolActionMcpTest, RejectAndExcludeToolsNeverAppear) {
  for (bool allow_writes : {false, true}) {
    const vector<string> names = ToolNames(RunToolsList(allow_writes));
    EXPECT_FALSE(Contains(names, "cluster_rebalance"))
        << "REJECT tool leaked (allow_writes=" << allow_writes << ")";
    EXPECT_FALSE(Contains(names, "pbc_edit"))
        << "EXCLUDE tool leaked (allow_writes=" << allow_writes << ")";
    // The MCP server's own blocking 'serve' action (REJECT) must not appear.
    EXPECT_FALSE(Contains(names, "mcp_serve"))
        << "mcp_serve leaked (allow_writes=" << allow_writes << ")";
  }
}

// Bad path (M3): the server-level control flag 'allow_writes' must never leak
// into any tool's inputSchema.properties, in either mode. Setting it per-call
// would let the model open the write gate itself.
TEST(ToolActionMcpTest, ControlFlagsNeverLeakIntoAnyInputSchema) {
  for (bool allow_writes : {false, true}) {
    const string line = RunToolsList(allow_writes);
    JsonReader reader(line);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* result = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
    vector<const rapidjson::Value*> tools;
    ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));
    for (const auto* t : tools) {
      string name;
      ASSERT_OK(reader.ExtractString(t, "name", &name));
      const rapidjson::Value* schema = nullptr;
      ASSERT_OK(reader.ExtractObject(t, "inputSchema", &schema));
      const rapidjson::Value* props = nullptr;
      ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
      EXPECT_FALSE(props->HasMember("allow_writes"))
          << "tool '" << name << "' leaks allow_writes (allow_writes="
          << allow_writes << ")";
    }
  }
}

// Bad path: an unknown method yields a JSON-RPC -32601 (method not found).
TEST(ToolActionMcpTest, UnknownMethodReturnsMethodNotFound) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"no/such/method\"}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  ASSERT_EQ(-32601, ErrorCode(lines[0]));
  // The id is still echoed on the error response.
  int32_t id = 0;
  ASSERT_OK(reader.ExtractInt32(reader.root(), "id", &id));
  ASSERT_EQ(3, id);
}

// Bad path: a malformed JSON line yields a -32700 parse error, and the loop
// continues to serve the subsequent valid line.
TEST(ToolActionMcpTest, MalformedJsonReturnsParseErrorAndLoopContinues) {
  vector<string> lines = RunAndCollect(
      "not json at all\n"
      "{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"tools/list\"}\n");
  ASSERT_EQ(2, lines.size());

  // First line: parse error with a null id.
  JsonReader err(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &err));
  ASSERT_EQ(-32700, ErrorCode(lines[0]));

  // Second line: the following valid request is still served.
  JsonReader ok(lines[1]);
  NO_FATALS(AssertValidEnvelope(lines[1], &ok));
  ASSERT_TRUE(ok.root()->HasMember("result"));
}

// Bad path: a 'notifications/initialized' line produces NO output line.
TEST(ToolActionMcpTest, InitializedNotificationProducesNoOutput) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n");
  ASSERT_TRUE(lines.empty());
}

// A JSON-RPC notification (a request without an id) produces no response even
// for a non-"notifications/*" method.
TEST(ToolActionMcpTest, RequestWithoutIdProducesNoOutput) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\"}\n");
  ASSERT_TRUE(lines.empty());
}

// Bad path: a request that carries an id but no 'method' -> -32600 (invalid
// request).
TEST(ToolActionMcpTest, RequestWithoutMethodReturnsInvalidRequest) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":5}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  ASSERT_EQ(-32600, ErrorCode(lines[0]));
}

// Blank lines are ignored while surrounding requests are answered in order.
TEST(ToolActionMcpTest, BlankLinesIgnoredAndRequestsAnsweredInOrder) {
  vector<string> lines = RunAndCollect(
      "\n"
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n"
      "\n"
      "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n");
  ASSERT_EQ(2, lines.size());

  JsonReader r0(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &r0));
  int32_t id0 = 0;
  ASSERT_OK(r0.ExtractInt32(r0.root(), "id", &id0));
  ASSERT_EQ(1, id0);

  JsonReader r1(lines[1]);
  NO_FATALS(AssertValidEnvelope(lines[1], &r1));
  int32_t id1 = 0;
  ASSERT_OK(r1.ExtractInt32(r1.root(), "id", &id1));
  ASSERT_EQ(2, id1);
}

// Bad path: valid JSON that is not an object (a bare array/number) -> -32600.
TEST(ToolActionMcpTest, NonObjectJsonReturnsInvalidRequest) {
  vector<string> lines = RunAndCollect("[1,2,3]\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  ASSERT_EQ(-32600, ErrorCode(lines[0]));
}

// Empty stdin: the loop returns OK and writes nothing.
TEST(ToolActionMcpTest, EmptyInputProducesNoResponseAndExitsCleanly) {
  vector<string> lines = RunAndCollect("");
  ASSERT_TRUE(lines.empty());
}

// ---------------------------------------------------------------------------
// M4: tools/call
// ---------------------------------------------------------------------------

// Bad path (M4): calling an unknown tool name yields a JSON-RPC -32602 (invalid
// params), not a tool result.
TEST(ToolActionMcpTest, ToolsCallUnknownToolReturnsInvalidParams) {
  const string line = RunToolsCall("no_such_tool", "{}");
  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_TRUE(reader.root()->HasMember("error")) << line;
  ASSERT_EQ(-32602, ErrorCode(line));
}

// Bad path (M4): a GATED tool is absent from the registry without
// --allow-writes, so calling it is rejected server-side as an unknown tool
// (M5 defense-in-depth, free here).
TEST(ToolActionMcpTest, ToolsCallGatedToolWithoutAllowWritesIsRejected) {
  ASSERT_FALSE(FLAGS_allow_writes);
  const string line = RunToolsCall("table_delete", "{\"table_name\":\"t\"}");
  ASSERT_EQ(-32602, ErrorCode(line)) << line;
}

// Bad path (M4): a call missing a non-injected required argument yields -32602
// naming the missing argument. 'tserver_status' requires 'tserver_address',
// which is model-supplied (not injected).
TEST(ToolActionMcpTest, ToolsCallMissingRequiredArgReturnsInvalidParams) {
  const string line = RunToolsCall("tserver_status", "{}");
  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_EQ(-32602, ErrorCode(line));

  const rapidjson::Value* err = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
  string message;
  ASSERT_OK(reader.ExtractString(err, "message", &message));
  ASSERT_NE(string::npos, message.find("tserver_address"))
      << "error must name the missing argument: " << message;
}

// Schema scrub (M4): the injected master-address positionals must NEVER appear
// in any tool's inputSchema (properties or required[]), in either write mode.
// The model must not need to supply the cluster address.
TEST(ToolActionMcpTest, MasterAddressesNeverInAnyInputSchema) {
  for (bool allow_writes : {false, true}) {
    const string line = RunToolsList(allow_writes);
    JsonReader reader(line);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* result = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
    vector<const rapidjson::Value*> tools;
    ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));
    for (const auto* t : tools) {
      string name;
      ASSERT_OK(reader.ExtractString(t, "name", &name));
      const rapidjson::Value* schema = nullptr;
      ASSERT_OK(reader.ExtractObject(t, "inputSchema", &schema));
      const rapidjson::Value* props = nullptr;
      ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
      EXPECT_FALSE(props->HasMember("master_addresses"))
          << "tool '" << name << "' leaks master_addresses";
      EXPECT_FALSE(props->HasMember("master_address"))
          << "tool '" << name << "' leaks master_address";
      vector<const rapidjson::Value*> required;
      ASSERT_OK(reader.ExtractObjectArray(schema, "required", &required));
      for (const auto* r : required) {
        ASSERT_TRUE(r->IsString());
        const string rname = r->GetString();
        EXPECT_NE("master_addresses", rname) << name;
        EXPECT_NE("master_address", rname) << name;
      }
    }
  }
}

// Injection error path (M4): omitting master_addresses when FLAGS_master_addresses
// is unset yields a tool result (isError=true) telling the operator to launch
// with --master_addresses -- NOT a JSON-RPC protocol error.
TEST(ToolActionMcpTest, ToolsCallWithoutConfiguredMasterAddressesReturnsToolError) {
  ScopedMasterAddresses guard("");
  const string line = RunToolsCall("master_status", "{}");
  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_TRUE(reader.root()->HasMember("result")) << line;

  const ToolResult tr = ParseToolResult(line);
  ASSERT_TRUE(tr.is_error) << line;
  ASSERT_NE(string::npos, tr.text.find("--master_addresses"))
      << "message must tell the operator to launch with --master_addresses: "
      << tr.text;
}

// Status->isError mapping (M4): a read action invoked against an unreachable
// cluster returns a non-OK Status, which surfaces as a well-formed JSON-RPC
// result with isError=true (a tool-execution error, not a protocol error). This
// exercises the mapping without needing a live cluster. A short RPC timeout
// keeps the connection retries bounded.
TEST(ToolActionMcpTest, ToolsCallReadAgainstUnreachableClusterMapsToIsError) {
  ScopedMasterAddresses guard("127.0.0.1:1");
  const string line = RunToolsCall(
      "master_status",
      "{\"timeout_ms\":1000,\"negotiation_timeout_ms\":500}");

  // Well-formed JSON-RPC result envelope (not an error envelope).
  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_TRUE(reader.root()->HasMember("result")) << line;

  const ToolResult tr = ParseToolResult(line);
  ASSERT_TRUE(tr.is_error) << "unreachable cluster must map to isError: " << line;
  ASSERT_FALSE(tr.text.empty()) << "isError result must carry the Status text";
}

// R2 regression (M4): after a tools/call that overrides optional gflags, every
// touched FLAGS_* is restored to its prior value -- INCLUDING when Run()
// returned an error (the scope guard restores on the error path too). We force
// the error path with an unreachable cluster.
TEST(ToolActionMcpTest, R2OptionalFlagsRestoredEvenOnErrorPath) {
  ScopedMasterAddresses guard("127.0.0.1:1");

  // Capture prior values via gflags (no DECLARE needed, works for any flag).
  string prior_show_attributes;
  string prior_timeout_ms;
  ASSERT_TRUE(google::GetCommandLineOption("show_attributes", &prior_show_attributes));
  ASSERT_TRUE(google::GetCommandLineOption("timeout_ms", &prior_timeout_ms));

  // table_describe requires only 'table_name' (master_addresses is injected).
  // Override show_attributes (bool) and timeout_ms (int) so the call touches
  // two flags of different types; the short timeout also bounds the retries.
  const string line = RunToolsCall(
      "table_describe",
      "{\"table_name\":\"no_such_table\",\"show_attributes\":true,"
      "\"timeout_ms\":1000,\"negotiation_timeout_ms\":500}");

  // The call failed (unreachable cluster) -> isError.
  const ToolResult tr = ParseToolResult(line);
  ASSERT_TRUE(tr.is_error) << line;

  // Both overridden flags are restored to their prior values.
  string after_show_attributes;
  string after_timeout_ms;
  ASSERT_TRUE(google::GetCommandLineOption("show_attributes", &after_show_attributes));
  ASSERT_TRUE(google::GetCommandLineOption("timeout_ms", &after_timeout_ms));
  EXPECT_EQ(prior_show_attributes, after_show_attributes)
      << "show_attributes not restored after an error-path tools/call";
  EXPECT_EQ(prior_timeout_ms, after_timeout_ms)
      << "timeout_ms not restored after an error-path tools/call";
}

// R3 regression (M4): the action's output goes into the tool result content,
// never interleaved into the JSON-RPC line framing. The protocol response is
// exactly one line and is valid, parseable JSON; the action's failure text
// lands inside content[0].text, and a following request is still answered
// cleanly (proving std::cout was restored between calls).
TEST(ToolActionMcpTest, R3ActionOutputStaysInContentAndFramingIsClean) {
  ScopedMasterAddresses guard("127.0.0.1:1");

  // Drive two requests back to back through one loop invocation.
  const string input = Substitute(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"master_status\","
      "\"arguments\":{\"timeout_ms\":1000,\"negotiation_timeout_ms\":500}}}\n"
      "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n");
  vector<string> lines = RunAndCollect(input);

  // Exactly two response lines -- nothing the action printed leaked as extra
  // lines onto the protocol channel.
  ASSERT_EQ(2, lines.size()) << "action output leaked onto the protocol channel";

  // First line: a valid tool result whose content carries the failure text.
  JsonReader r0(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &r0));
  const ToolResult tr = ParseToolResult(lines[0]);
  ASSERT_TRUE(tr.is_error) << lines[0];
  ASSERT_FALSE(tr.text.empty());

  // Second line: the following tools/list is still served cleanly (cout was
  // restored), and is itself valid JSON.
  JsonReader r1(lines[1]);
  NO_FATALS(AssertValidEnvelope(lines[1], &r1));
  ASSERT_TRUE(r1.root()->HasMember("result")) << lines[1];
}

// ---------------------------------------------------------------------------
// M5: write gating (dry-run + server-side gated rejection)
// ---------------------------------------------------------------------------

// Schema (M5): a GATED tool's inputSchema carries the synthetic 'dry_run'
// boolean property, and it is NOT listed in required[]. A SURFACE tool's schema
// carries no 'dry_run' property at all (dry-run is meaningful only for writes).
TEST(ToolActionMcpTest, GatedToolSchemaCarriesDryRunPropertyNotRequired) {
  const string line = RunToolsList(/*allow_writes=*/true);
  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));

  const rapidjson::Value* add_range = nullptr;
  const rapidjson::Value* describe = nullptr;
  for (const auto* t : tools) {
    string n;
    ASSERT_OK(reader.ExtractString(t, "name", &n));
    if (n == "table_add_range_partition") {
      add_range = t;
    } else if (n == "table_describe") {
      describe = t;
    }
  }
  ASSERT_NE(nullptr, add_range) << "GATED table_add_range_partition missing: " << line;
  ASSERT_NE(nullptr, describe) << "SURFACE table_describe missing: " << line;

  // GATED tool: dry_run is a boolean property.
  const rapidjson::Value* schema = nullptr;
  ASSERT_OK(reader.ExtractObject(add_range, "inputSchema", &schema));
  const rapidjson::Value* props = nullptr;
  ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
  ASSERT_TRUE(props->HasMember("dry_run")) << line;
  const rapidjson::Value* dry_run = nullptr;
  ASSERT_OK(reader.ExtractObject(props, "dry_run", &dry_run));
  string dr_type;
  ASSERT_OK(reader.ExtractString(dry_run, "type", &dr_type));
  ASSERT_EQ("boolean", dr_type);

  // ...but never a required argument.
  vector<const rapidjson::Value*> required;
  ASSERT_OK(reader.ExtractObjectArray(schema, "required", &required));
  for (const auto* r : required) {
    ASSERT_TRUE(r->IsString());
    ASSERT_NE("dry_run", string(r->GetString())) << "dry_run must not be required: " << line;
  }

  // SURFACE tool: no dry_run property at all.
  const rapidjson::Value* sschema = nullptr;
  ASSERT_OK(reader.ExtractObject(describe, "inputSchema", &sschema));
  const rapidjson::Value* sprops = nullptr;
  ASSERT_OK(reader.ExtractObject(sschema, "properties", &sprops));
  ASSERT_FALSE(sprops->HasMember("dry_run"))
      << "SURFACE tool must not carry dry_run: " << line;
}

// Dry-run happy path (M5): a GATED tools/call with dry_run:true and valid args
// returns isError:false and content text that is EXACTLY the literal command
// that would run -- including the injected master addresses at their positional
// slot, positionals in declared order, and shell-quoting of bracketed bounds.
// No cluster is contacted (Run() is never called).
TEST(ToolActionMcpTest, DryRunGatedCallReturnsExactLiteralCommand) {
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");

  const string line = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"my_range_tbl\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\",\"dry_run\":true}");

  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_TRUE(reader.root()->HasMember("result")) << line;
  const ToolResult tr = ParseToolResult(line);
  ASSERT_FALSE(tr.is_error) << tr.text;
  ASSERT_EQ(
      "kudu table add_range_partition master-1:7051 my_range_tbl '[0]' '[100]'",
      tr.text);
}

// Dry-run bad path (M5): args are validated even in dry-run, so a missing
// required argument still yields -32602 naming it (never a silent success).
TEST(ToolActionMcpTest, DryRunWithMissingRequiredArgStillReturnsInvalidParams) {
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");

  // Missing table_range_upper_bound.
  const string line = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"my_range_tbl\",\"table_range_lower_bound\":\"[0]\","
      "\"dry_run\":true}");
  ASSERT_EQ(-32602, ErrorCode(line)) << line;

  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
  string message;
  ASSERT_OK(reader.ExtractString(err, "message", &message));
  ASSERT_NE(string::npos, message.find("table_range_upper_bound"))
      << "error must name the missing argument: " << message;
}

// Gating bad path (M5): without --allow-writes a GATED tool is absent from
// tools/list AND calling it is rejected server-side with a DISTINCT message
// that names --allow-writes -- clearly different from the "unknown tool"
// message a genuinely unknown name still gets.
TEST(ToolActionMcpTest, GatedToolWithoutAllowWritesRejectedWithDistinctMessage) {
  ASSERT_FALSE(FLAGS_allow_writes);

  // Absent from the listing.
  EXPECT_FALSE(Contains(ToolNames(RunToolsList(/*allow_writes=*/false)),
                        "table_add_range_partition"));

  // Calling it anyway: rejected, and the message names the write gate.
  const string gated = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"t\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\"}");
  ASSERT_EQ(-32602, ErrorCode(gated)) << gated;
  {
    JsonReader reader(gated);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* err = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
    string message;
    ASSERT_OK(reader.ExtractString(err, "message", &message));
    ASSERT_NE(string::npos, message.find("--allow-writes"))
        << "gated rejection must name --allow-writes: " << message;
  }

  // A genuinely unknown tool still gets the "unknown tool" message, not the
  // gated one.
  const string unknown = RunToolsCall("no_such_tool", "{}");
  ASSERT_EQ(-32602, ErrorCode(unknown)) << unknown;
  {
    JsonReader reader(unknown);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* err = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
    string message;
    ASSERT_OK(reader.ExtractString(err, "message", &message));
    ASSERT_NE(string::npos, message.find("unknown tool"))
        << "unknown-tool rejection must say 'unknown tool': " << message;
    ASSERT_EQ(string::npos, message.find("--allow-writes"))
        << "unknown-tool rejection must NOT mention --allow-writes: " << message;
  }
}

// R2 confirm (M5): a dry-run call renders any supplied optional flag in the
// reconstructed command but does NOT apply it to process-global gflag state --
// dry-run must leak no flag state (nor call Run()).
TEST(ToolActionMcpTest, DryRunLeavesOptionalFlagStateUnchanged) {
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");

  string prior_lower_bound_type;
  ASSERT_TRUE(
      google::GetCommandLineOption("lower_bound_type", &prior_lower_bound_type));

  const string line = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"my_range_tbl\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\","
      "\"lower_bound_type\":\"EXCLUSIVE_BOUND\",\"dry_run\":true}");

  const ToolResult tr = ParseToolResult(line);
  ASSERT_FALSE(tr.is_error) << tr.text;
  // The optional flag is echoed into the reconstructed command...
  ASSERT_NE(string::npos, tr.text.find("--lower_bound_type=EXCLUSIVE_BOUND"))
      << "dry-run command missing the supplied optional flag: " << tr.text;

  // ...but the process-global gflag value is untouched.
  string after_lower_bound_type;
  ASSERT_TRUE(
      google::GetCommandLineOption("lower_bound_type", &after_lower_bound_type));
  ASSERT_EQ(prior_lower_bound_type, after_lower_bound_type)
      << "dry-run must not mutate gflag state";
}

// ---------------------------------------------------------------------------
// M6 R1: no REJECT / EXCLUDE action is ever exposed (exhaustive tree walk)
// ---------------------------------------------------------------------------

// The RejectAndExcludeToolsNeverAppear test above checks three hand-picked
// names. This is the exhaustive form required by M6: walk the REAL action tree,
// and for EVERY action classified REJECT or EXCLUDE assert its MCP tool name is
// absent from tools/list in BOTH write modes. A newly added blocking or
// interactive action that is mistakenly surfaced fails here without needing a
// bespoke per-name assertion.
TEST(ToolActionMcpTest, NoRejectOrExcludeActionIsEverExposed) {
  unique_ptr<Mode> root = BuildRootMode("kudu");

  // The set of names the server actually surfaces, in each write mode.
  const unordered_set<string> surfaced_closed = [&] {
    const vector<string> n = ToolNames(RunToolsList(/*allow_writes=*/false));
    return unordered_set<string>(n.begin(), n.end());
  }();
  const unordered_set<string> surfaced_open = [&] {
    const vector<string> n = ToolNames(RunToolsList(/*allow_writes=*/true));
    return unordered_set<string>(n.begin(), n.end());
  }();

  int reject_or_exclude_seen = 0;
  vector<Mode*> chain;
  VisitActions(root.get(), &chain, [&](const vector<Mode*>& c, const Action* a) {
    const DispositionInfo info = DispositionFor(c, a);
    ASSERT_TRUE(info.classified)
        << "unclassified action leaked into the tree: "
        << DispositionCommandPath(c, a);
    if (info.disposition != Disposition::REJECT &&
        info.disposition != Disposition::EXCLUDE) {
      return;
    }
    ++reject_or_exclude_seen;
    const string tool = McpToolNameForTest(c, a);
    EXPECT_EQ(0, surfaced_closed.count(tool))
        << DispositionToString(info.disposition) << " action '" << tool
        << "' is surfaced without --allow-writes";
    EXPECT_EQ(0, surfaced_open.count(tool))
        << DispositionToString(info.disposition) << " action '" << tool
        << "' is surfaced with --allow-writes";
  });

  // Sanity: the tree really did contain REJECT/EXCLUDE actions, so the walk
  // above was not vacuously green.
  ASSERT_GT(reject_or_exclude_seen, 0)
      << "expected the action tree to contain REJECT/EXCLUDE actions";
}

// R1 dispatch side: calling a REJECT (cluster_rebalance) or an EXCLUDE
// (pbc_edit) tool by name is refused server-side -- it is not in the registry
// in either write mode, so tools/call returns -32602 (unknown tool), never a
// tool result that would run it.
TEST(ToolActionMcpTest, ToolsCallRejectOrExcludeToolIsRefused) {
  for (bool allow_writes : {false, true}) {
    ScopedAllowWrites allow(allow_writes);
    for (const char* tool : {"cluster_rebalance", "pbc_edit"}) {
      const string line = RunToolsCall(tool, "{}");
      ASSERT_EQ(-32602, ErrorCode(line))
          << tool << " (allow_writes=" << allow_writes << "): " << line;
      JsonReader reader(line);
      ASSERT_OK(reader.Init());
      const rapidjson::Value* err = nullptr;
      ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
      string message;
      ASSERT_OK(reader.ExtractString(err, "message", &message));
      // A never-surfaced tool is rejected as unknown, NOT with the write-gate
      // message (which is reserved for GATED tools hidden by the closed gate).
      EXPECT_NE(string::npos, message.find("unknown tool"))
          << tool << ": " << message;
      EXPECT_EQ(string::npos, message.find("--allow-writes"))
          << tool << " must not be treated as a gated tool: " << message;
    }
  }
}

// ---------------------------------------------------------------------------
// M6 R4: no surfaced tool exposes an unsafe/experimental optional parameter
// ---------------------------------------------------------------------------

// Mechanized R4 (process-fatal) audit. Action::Run() calls kudu::ValidateFlags(),
// which can exit(1) if an unsafe- or experimental-tagged gflag is set without
// the matching --unlock flag (CheckFlagsAllowed). A surfaced tool is safe from
// that path ONLY if none of its model-settable optional parameters map to such
// a flag. This walks every SURFACE/GATED action and asserts none of its optional
// parameters is tagged 'unsafe' or 'experimental'. If this ever fails, a newly
// surfaced tool could let model input trip a process-fatal flag check -- fix the
// disposition (or the tool), do not weaken this assertion.
TEST(ToolActionMcpTest, NoSurfacedToolExposesUnsafeOrExperimentalFlag) {
  unique_ptr<Mode> root = BuildRootMode("kudu");

  int checked = 0;
  vector<Mode*> chain;
  VisitActions(root.get(), &chain, [&](const vector<Mode*>& c, const Action* a) {
    const DispositionInfo info = DispositionFor(c, a);
    ASSERT_TRUE(info.classified);
    if (info.disposition != Disposition::SURFACE &&
        info.disposition != Disposition::GATED) {
      return;
    }
    ++checked;
    const string tool = McpToolNameForTest(c, a);
    for (const auto& flag : a->args().optional) {
      unordered_set<string> tags;
      GetFlagTags(flag.name, &tags);
      EXPECT_EQ(0, tags.count("unsafe"))
          << "surfaced tool '" << tool << "' exposes unsafe flag '"
          << flag.name << "'";
      EXPECT_EQ(0, tags.count("experimental"))
          << "surfaced tool '" << tool << "' exposes experimental flag '"
          << flag.name << "'";
    }
  });

  ASSERT_GT(checked, 0) << "expected surfaced actions to audit";
}

} // namespace tools
} // namespace kudu
