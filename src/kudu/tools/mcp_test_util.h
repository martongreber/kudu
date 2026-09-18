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

// Client-side JSON-RPC helpers shared by the MCP server's unit test
// (tool_action_mcp-test.cc) and integration test (tool_action_mcp-itest.cc).
//
// These build requests and parse responses using JsonReader only -- they share
// no code with the server's own response writer (tool_action_mcp.cc), so the
// tests remain an independent oracle of the wire format: a bug in the server's
// framing cannot be masked by a helper that mirrors it.

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action_mcp.h"  // RunMcpServeLoop, kJsonRpc* error codes
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

// The parsed MCP tool result from a 'tools/call' JSON-RPC response. This
// deliberately mirrors (rather than reuses) the server's ToolOutcome in
// tool_action_mcp.cc: that is the value before JSON serialization, this is the
// value parsed back off the wire. Keeping the test's parse-side type
// independent of the server's type is what makes these helpers an independent
// oracle of the wire format (see the file comment above), so a framing bug in
// the server cannot be masked by a helper that reuses the server's own type.
struct ToolResult {
  bool is_error;
  std::string text;  // content[0].text
};

// Builds a newline-terminated 'tools/call' JSON-RPC request line. 'arguments_json'
// is the raw JSON object for params.arguments (e.g. R"({"table_name":"t"})").
inline std::string MakeToolsCallRequest(const std::string& tool_name,
                                        const std::string& arguments_json) {
  return strings::Substitute(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"$0\",\"arguments\":$1}}\n",
      tool_name, arguments_json);
}

// Splits newline-delimited server output into individual JSON-RPC lines,
// dropping empty lines (the server emits exactly one response line per request
// and nothing for notifications, so interior blanks never carry content).
inline std::vector<std::string> SplitResponseLines(const std::string& out) {
  std::vector<std::string> lines;
  std::istringstream iss(out);
  std::string line;
  while (std::getline(iss, line)) {
    if (!line.empty()) lines.push_back(line);
  }
  return lines;
}

// Collects the "name" of every tool in a 'tools/list' response 'line'.
inline std::vector<std::string> ToolNames(const std::string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  std::vector<const rapidjson::Value*> tools;
  CHECK_OK(reader.ExtractObjectArray(result, "tools", &tools));
  std::vector<std::string> names;
  for (const auto* t : tools) {
    std::string n;
    CHECK_OK(reader.ExtractString(t, "name", &n));
    names.push_back(n);
  }
  return names;
}

// Extracts the integer error code from a JSON-RPC error response 'line'.
// CHECK-fails if the line is not an error envelope.
inline int ErrorCode(const std::string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "error", &err));
  int32_t code = 0;
  CHECK_OK(reader.ExtractInt32(err, "code", &code));
  return code;
}

// Extracts the error 'message' string from a JSON-RPC error response 'line'.
inline std::string ErrorMessage(const std::string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "error", &err));
  std::string message;
  CHECK_OK(reader.ExtractString(err, "message", &message));
  return message;
}

// Parses a 'tools/call' success response 'line' into its MCP tool result. Fails
// (via CHECK) if the line is not a well-formed result carrying a text
// content[0]; this doubles as the assertion that the protocol line is valid JSON.
inline ToolResult ParseToolResult(const std::string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  ToolResult tr;
  tr.is_error = false;
  // isError is present on tools/call results (false on success, true on error).
  if (result->HasMember("isError")) {
    CHECK_OK(reader.ExtractBool(result, "isError", &tr.is_error));
  }
  std::vector<const rapidjson::Value*> content;
  CHECK_OK(reader.ExtractObjectArray(result, "content", &content));
  CHECK(!content.empty()) << "tool result has empty content: " << line;
  std::string type;
  CHECK_OK(reader.ExtractString(content[0], "type", &type));
  CHECK_EQ("text", type) << line;
  CHECK_OK(reader.ExtractString(content[0], "text", &tr.text));
  return tr;
}

// True if 'v' contains 's'.
inline bool Contains(const std::vector<std::string>& v, const std::string& s) {
  return std::find(v.begin(), v.end(), s) != v.end();
}

} // namespace tools
} // namespace kudu
