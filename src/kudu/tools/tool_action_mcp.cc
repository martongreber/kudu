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
#include <cctype>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <streambuf>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tools/mcp_disposition.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/status.h"
#include "kudu/util/version_info.h"

// This flag is declared here in M0 as part of the write-gating surface described
// in the PRD (D4). It is registered on the 'serve' action but is NOT enforced
// yet -- enforcement lands in M5. It exists now so the flag plumbing is stable
// for later milestones.
DEFINE_bool(allow_writes, false,
            "Whether to expose mutating (write) tools over the MCP protocol. "
            "When false, only read-only tools are surfaced. Write gating is not "
            "enforced yet; this flag is reserved for a later milestone.");

// The master addresses the serve session connects to. This is the serve-level
// source of connection info: the operator registers the server once with
// 'kudu mcp serve --master_addresses=...', and tools/call injects this value
// for any action that needs it, so the addresses never have to appear in the
// conversation (PRD section 3). Defined in the master library (master_options.cc)
// and linked into the 'kudu' binary; declared here so 'serve' can register it as
// an optional parameter and tools/call can read it.
DECLARE_string(master_addresses);

using std::ostringstream;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// ----------------------------------------------------------------------------
// MCP / JSON-RPC constants
// ----------------------------------------------------------------------------

// serverInfo.name reported in the initialize handshake.
const char* const kServerName = "kudu-mcp";

// The MCP protocol version this server implements by default. This is what we
// report when the client does not request a specific (supported) version. Uses
// the dated MCP revision string format.
const char* const kDefaultProtocolVersion = "2025-06-18";

// JSON-RPC 2.0 standard error codes (see the JSON-RPC 2.0 spec, section 5.1).
constexpr int kJsonRpcParseError = -32700;
constexpr int kJsonRpcInvalidRequest = -32600;
constexpr int kJsonRpcMethodNotFound = -32601;
constexpr int kJsonRpcInvalidParams = -32602;

// Returns true if 'version' is an MCP protocol revision this server can speak.
// The client's requested version is echoed back only when it is in this set;
// otherwise the server replies with kDefaultProtocolVersion and the client is
// expected to adapt or disconnect.
bool IsSupportedProtocolVersion(const string& version) {
  return version == "2024-11-05" ||
         version == "2025-03-26" ||
         version == "2025-06-18";
}

// ----------------------------------------------------------------------------
// Response envelope construction (JsonWriter)
// ----------------------------------------------------------------------------

// Writes a JSON-RPC 'id' value, preserving the type exactly as the client sent
// it (integer or string are the only types JSON-RPC permits, but we tolerate
// the full numeric range). A null or absent id is written as JSON null, which
// is what the spec mandates for error responses whose id could not be
// determined. 'id' may be nullptr.
void WriteJsonRpcId(JsonWriter* jw, const rapidjson::Value* id) {
  if (id == nullptr || id->IsNull()) {
    jw->Null();
  } else if (id->IsInt()) {
    jw->Int(id->GetInt());
  } else if (id->IsInt64()) {
    jw->Int64(id->GetInt64());
  } else if (id->IsUint()) {
    jw->Uint(id->GetUint());
  } else if (id->IsUint64()) {
    jw->Uint64(id->GetUint64());
  } else if (id->IsDouble()) {
    jw->Double(id->GetDouble());
  } else if (id->IsString()) {
    jw->String(id->GetString());
  } else {
    // Bool/object/array are not valid JSON-RPC ids. Echo null to stay
    // well-formed rather than propagating a nonsensical id.
    jw->Null();
  }
}

// Builds a JSON-RPC error response line:
//   {"jsonrpc":"2.0","id":<id>,"error":{"code":<code>,"message":<message>}}
// A response carries EITHER 'result' OR 'error', never both.
string BuildErrorResponse(const rapidjson::Value* id,
                          int code,
                          const string& message) {
  ostringstream ss;
  JsonWriter jw(&ss, JsonWriter::COMPACT);
  jw.StartObject();
  jw.String("jsonrpc");
  jw.String("2.0");
  jw.String("id");
  WriteJsonRpcId(&jw, id);
  jw.String("error");
  jw.StartObject();
  jw.String("code");
  jw.Int(code);
  jw.String("message");
  jw.String(message);
  jw.EndObject();
  jw.EndObject();
  return ss.str();
}

// Builds the JSON-RPC response to an 'initialize' request. The result reports
// the negotiated protocol version, the server's declared capabilities (tools
// only, per PRD S4 -- no resources), and serverInfo.
string BuildInitializeResponse(const rapidjson::Value* id,
                               const string& protocol_version) {
  ostringstream ss;
  JsonWriter jw(&ss, JsonWriter::COMPACT);
  jw.StartObject();
  jw.String("jsonrpc");
  jw.String("2.0");
  jw.String("id");
  WriteJsonRpcId(&jw, id);
  jw.String("result");
  jw.StartObject();
  {
    jw.String("protocolVersion");
    jw.String(protocol_version);

    // Capabilities: declare only the 'tools' capability. Its value is an empty
    // object (no sub-capabilities such as listChanged in v1).
    jw.String("capabilities");
    jw.StartObject();
    jw.String("tools");
    jw.StartObject();
    jw.EndObject();
    jw.EndObject();

    jw.String("serverInfo");
    jw.StartObject();
    jw.String("name");
    jw.String(kServerName);
    jw.String("version");
    jw.String(VersionInfo::GetShortVersionInfo());
    jw.EndObject();
  }
  jw.EndObject();
  jw.EndObject();
  return ss.str();
}

// ----------------------------------------------------------------------------
// Tool exposure registry + reflection into MCP tool objects (M3)
// ----------------------------------------------------------------------------

// Server-level control flags that are configured once at launch, never per
// tool call. They must NEVER be surfaced as a tool input property: the model
// cannot be allowed to flip the write gate or the dry-run switch on a per-call
// basis. This scrubs 'allow_writes' and any dry-run flag (matched by name so a
// dry-run flag introduced in a later milestone is covered defensively even if
// an action erroneously declares it as an optional parameter).
bool IsReservedControlFlag(const string& flag_name) {
  return flag_name == "allow_writes" ||
         flag_name.find("dry_run") != string::npos;
}

// Connection arguments that the server injects from its serve-level
// FLAGS_master_addresses rather than requiring the model to supply per call.
// These are the master-address positionals: the plural 'master_addresses'
// (cluster / table actions) and the singular 'master_address' (master actions).
// They are OMITTED from every tool's inputSchema (the model must not need to
// know the cluster address -- that is the PRD UX in section 3) and are filled in
// at dispatch time. This is deliberately distinct from IsReservedControlFlag:
// the tserver-address positional ('tserver_address') is NOT injected, because a
// cluster has many tservers and the agent selects one (e.g. from ksck output).
bool IsInjectedConnectionArg(const string& arg_name) {
  return arg_name == kMasterAddressesArg || arg_name == kMasterAddressArg;
}

// One exposed action and everything tools/list (M3) needs to describe it and
// tools/call (M4) will need to invoke it. Built by BuildMcpToolRegistry() by
// walking the action tree once; the tree must outlive the registry because
// 'chain' and 'action' point into it.
struct McpToolEntry {
  // The flattened tool name, e.g. "table_describe" (see McpToolName()).
  string tool_name;
  // The mode chain from the root to the action's parent (chain.front() is the
  // root), exactly as the disposition lookup and BuildHelpXML walk expect it.
  vector<Mode*> chain;
  const Action* action;
  DispositionInfo disposition;
};

// Computes the MCP tool name for an action: its full command path with the path
// separators (spaces) turned into underscores. E.g. a chain {root, "table"}
// with action "describe" -> "table_describe"; {root, "tserver", "quiesce"} with
// action "status" -> "tserver_quiesce_status". Mode and action names may
// themselves contain underscores (e.g. "set_limit", "authz_cache"), so the
// joined name is NOT reversible by splitting on '_'; callers resolve a name back
// to an action via the registry, never by string-splitting. This function is
// the single source of truth so tools/list (M3) and tools/call (M4) agree.
string McpToolName(const vector<Mode*>& chain, const Action* action) {
  string name = DispositionCommandPath(chain, action);
  std::replace(name.begin(), name.end(), ' ', '_');
  return name;
}

// Recursively walks 'mode' (whose full chain from the root is 'chain', with
// 'mode' as its last element), appending an McpToolEntry for every EXPOSED
// action. Exposure follows the PRD: SURFACE is always exposed; GATED only when
// 'allow_writes'; REJECT / EXCLUDE / unclassified are never exposed.
void CollectMcpTools(const vector<Mode*>& chain,
                     const Mode* mode,
                     bool allow_writes,
                     vector<McpToolEntry>* out) {
  for (const auto& action : mode->actions()) {
    const DispositionInfo info = DispositionFor(chain, action.get());
    // The startup coverage invariant guarantees every action is classified;
    // stay defensive and never surface an action we cannot classify.
    if (!info.classified) {
      continue;
    }
    bool expose = false;
    switch (info.disposition) {
      case Disposition::SURFACE:
        expose = true;
        break;
      case Disposition::GATED:
        expose = allow_writes;
        break;
      case Disposition::REJECT:
      case Disposition::EXCLUDE:
        expose = false;
        break;
    }
    if (!expose) {
      continue;
    }
    McpToolEntry entry;
    entry.tool_name = McpToolName(chain, action.get());
    entry.chain = chain;
    entry.action = action.get();
    entry.disposition = info;
    out->emplace_back(std::move(entry));
  }
  for (const auto& submode : mode->modes()) {
    vector<Mode*> child_chain(chain);
    child_chain.push_back(submode.get());
    CollectMcpTools(child_chain, submode.get(), allow_writes, out);
  }
}

// Builds the exposure registry for the tree rooted at 'root': one record per
// action that should be visible as an MCP tool given 'allow_writes'. This is
// what tools/list iterates now and what tools/call (M4) will resolve a tool
// name against. The returned entries borrow from 'root', which must outlive
// them.
vector<McpToolEntry> BuildMcpToolRegistry(const Mode* root, bool allow_writes) {
  vector<McpToolEntry> tools;
  vector<Mode*> root_chain = { const_cast<Mode*>(root) };
  for (const auto& mode : root->modes()) {
    vector<Mode*> child_chain(root_chain);
    child_chain.push_back(mode.get());
    CollectMcpTools(child_chain, mode.get(), allow_writes, &tools);
  }
  return tools;
}

// Maps a gflag type string (as reported by google::CommandLineFlagInfo::type)
// to a JSON Schema type: "bool" -> boolean; the integer families -> integer;
// "double" -> number; everything else -> string (the safe fallback for any
// gflag type we do not explicitly recognize, e.g. "string" and "uint64" edge
// cases). Never returns null.
const char* JsonSchemaTypeForGflag(const string& gflag_type) {
  if (gflag_type == "bool") {
    return "boolean";
  }
  if (gflag_type == "int32" || gflag_type == "int64" ||
      gflag_type == "uint32" || gflag_type == "uint64") {
    return "integer";
  }
  if (gflag_type == "double") {
    return "number";
  }
  return "string";
}

// Writes a JSON Schema "default" value for an optional flag, typed to match the
// property's JSON type where possible. gflags reports every default as a
// string; this converts it to a JSON boolean/number when the flag type calls
// for one and the string parses cleanly, falling back to the raw string
// otherwise so the schema is always well-formed.
void WriteFlagDefault(JsonWriter* jw,
                      const string& gflag_type,
                      const string& default_value) {
  if (gflag_type == "bool") {
    jw->Bool(default_value == "true");
    return;
  }
  if (gflag_type == "int32" || gflag_type == "int64" ||
      gflag_type == "uint32" || gflag_type == "uint64") {
    int64_t v = 0;
    if (safe_strto64(default_value, &v)) {
      jw->Int64(v);
      return;
    }
  } else if (gflag_type == "double") {
    double d = 0;
    if (safe_strtod(default_value.c_str(), &d)) {
      jw->Double(d);
      return;
    }
  }
  jw->String(default_value);
}

// Writes a single MCP tool object for 'entry' into the open array 'jw'. Mirrors
// Action::BuildHelpXML's argument handling: required args and the variadic arg
// become JSON Schema properties (and are listed in "required"), optional flags
// become typed properties keyed off the gflag type.
void WriteMcpToolObject(JsonWriter* jw, const McpToolEntry& entry) {
  const Action* action = entry.action;
  const ActionArgsDescriptor& args = action->args();

  jw->StartObject();

  jw->String("name");
  jw->String(entry.tool_name);

  // Description: the action description, plus its extra description if any, plus
  // a node-local locality note for tagged SURFACE tools (PRD section 6).
  string description = action->description();
  if (action->extra_description()) {
    description += " ";
    description += *action->extra_description();
  }
  if (entry.disposition.disposition == Disposition::SURFACE &&
      entry.disposition.node_local) {
    description += " Note: this reflects only the local node this server runs "
                   "on, not the whole cluster.";
  }
  jw->String("description");
  jw->String(description);

  // inputSchema: a JSON Schema object.
  jw->String("inputSchema");
  jw->StartObject();
  {
    jw->String("type");
    jw->String("object");

    jw->String("properties");
    jw->StartObject();
    {
      // Required positional args -> string properties. Injected connection args
      // (the master-address positionals) are omitted: the server fills them in
      // from its serve-level FLAGS_master_addresses, so the model neither sees
      // nor supplies them.
      for (const auto& r : args.required) {
        if (IsInjectedConnectionArg(r.name)) {
          continue;
        }
        jw->String(r.name);
        jw->StartObject();
        jw->String("type");
        jw->String("string");
        jw->String("description");
        jw->String(r.description);
        jw->EndObject();
      }
      // The variadic arg -> an array-of-strings property (required variadic).
      if (args.variadic) {
        const ActionArgsDescriptor::Arg& v = *args.variadic;
        jw->String(v.name);
        jw->StartObject();
        jw->String("type");
        jw->String("array");
        jw->String("items");
        jw->StartObject();
        jw->String("type");
        jw->String("string");
        jw->EndObject();
        jw->String("description");
        jw->String(v.description);
        jw->EndObject();
      }
      // Optional flags -> typed properties, type read from gflags exactly as
      // the XML walk reads it. Control flags are never surfaced.
      for (const auto& o : args.optional) {
        if (IsReservedControlFlag(o.name)) {
          continue;
        }
        google::CommandLineFlagInfo gflag_info =
            google::GetCommandLineFlagInfoOrDie(o.name.c_str());
        // The action may override the gflag's description / default.
        const string description_str =
            o.description.value_or(gflag_info.description);
        const string default_str =
            o.default_value.value_or(gflag_info.default_value);

        jw->String(o.name);
        jw->StartObject();
        jw->String("type");
        jw->String(JsonSchemaTypeForGflag(gflag_info.type));
        jw->String("description");
        jw->String(description_str);
        jw->String("default");
        WriteFlagDefault(jw, gflag_info.type, default_str);
        jw->EndObject();
      }
      // GATED tools carry a synthetic 'dry_run' control property (M5). It is an
      // MCP-level switch, NOT a gflag: when set true, tools/call reconstructs and
      // returns the exact command that would run instead of executing it. It is
      // surfaced only on GATED tools (a write is the only thing worth previewing)
      // and, being reserved by IsReservedControlFlag, can never collide with a
      // real action flag. It is intentionally absent from required[].
      if (entry.disposition.disposition == Disposition::GATED) {
        jw->String("dry_run");
        jw->StartObject();
        jw->String("type");
        jw->String("boolean");
        jw->String("description");
        jw->String("If true, do not execute; return the exact kudu command that "
                   "would run, making no changes.");
        jw->String("default");
        jw->Bool(false);
        jw->EndObject();
      }
    }
    jw->EndObject(); // properties

    // required[]: required args and the (required) variadic arg. Optional flags
    // are never required.
    jw->String("required");
    jw->StartArray();
    for (const auto& r : args.required) {
      if (IsInjectedConnectionArg(r.name)) {
        continue;
      }
      jw->String(r.name);
    }
    if (args.variadic) {
      jw->String(args.variadic->name);
    }
    jw->EndArray();
  }
  jw->EndObject(); // inputSchema

  // annotations: hint the host about read-only vs destructive behavior so it
  // can decide whether to prompt. Full confirm/dry-run enforcement is M5; here
  // we only tag. GATED tools are mutating (not read-only) and potentially
  // destructive; SURFACE tools are read-only.
  jw->String("annotations");
  jw->StartObject();
  if (entry.disposition.disposition == Disposition::GATED) {
    jw->String("readOnlyHint");
    jw->Bool(false);
    jw->String("destructiveHint");
    jw->Bool(true);
  } else {
    jw->String("readOnlyHint");
    jw->Bool(true);
  }
  jw->EndObject();

  jw->EndObject(); // tool
}

// Builds the JSON-RPC response to a 'tools/list' request by emitting one MCP
// tool object per entry in the pre-built exposure registry.
string BuildToolsListResponse(const rapidjson::Value* id,
                              const vector<McpToolEntry>& tools) {
  ostringstream ss;
  JsonWriter jw(&ss, JsonWriter::COMPACT);
  jw.StartObject();
  jw.String("jsonrpc");
  jw.String("2.0");
  jw.String("id");
  WriteJsonRpcId(&jw, id);
  jw.String("result");
  jw.StartObject();
  jw.String("tools");
  jw.StartArray();
  for (const auto& entry : tools) {
    WriteMcpToolObject(&jw, entry);
  }
  jw.EndArray();
  jw.EndObject();
  jw.EndObject();
  return ss.str();
}

// Builds the action tree the serve session reflects into MCP tools. Delegates
// to the shared BuildRootMode() so the top-level mode list has exactly one
// source of truth (see BuildRootMode()'s definition below and its declaration
// in tool_action.h). The root mode's name is irrelevant to reflection: it is
// dropped from every command path (DispositionCommandPath) and tool name
// (McpToolName).
unique_ptr<Mode> BuildMcpRootMode() {
  return BuildRootMode("kudu");
}

// Determines the protocol version to report in the initialize response. If the
// client's params carry a supported protocolVersion, it is echoed back;
// otherwise the server's default supported version is returned.
string NegotiateProtocolVersion(const JsonReader& reader,
                                const rapidjson::Value* root) {
  const rapidjson::Value* params = nullptr;
  if (reader.ExtractObject(root, "params", &params).ok()) {
    string requested;
    if (reader.ExtractString(params, "protocolVersion", &requested).ok() &&
        !requested.empty() && IsSupportedProtocolVersion(requested)) {
      return requested;
    }
  }
  return kDefaultProtocolVersion;
}

// ----------------------------------------------------------------------------
// tools/call: dispatch a read-only action and capture its text output (M4)
// ----------------------------------------------------------------------------

// Converts a scalar JSON value to the string form the CLI's positional and
// gflag parsing expects. Strings pass through; numbers and booleans are
// stringified as gflags would render them. Returns false for a non-scalar
// (object / array / null), which cannot be a positional arg or a flag value.
bool JsonScalarToString(const rapidjson::Value& v, string* out) {
  if (v.IsString()) {
    *out = v.GetString();
    return true;
  }
  if (v.IsBool()) {
    *out = v.GetBool() ? "true" : "false";
    return true;
  }
  if (v.IsInt()) {
    *out = SimpleItoa(v.GetInt());
    return true;
  }
  if (v.IsInt64()) {
    *out = SimpleItoa(v.GetInt64());
    return true;
  }
  if (v.IsUint()) {
    *out = SimpleItoa(v.GetUint());
    return true;
  }
  if (v.IsUint64()) {
    *out = SimpleItoa(v.GetUint64());
    return true;
  }
  if (v.IsDouble()) {
    *out = SimpleDtoa(v.GetDouble());
    return true;
  }
  return false;
}

// RAII guard honoring R2: optional flags are process-global gflags, so a
// tools/call that overrides them must restore the prior values afterward --
// including on any early-return / error path. SetAndSave() records the current
// value once and applies the new one; the destructor restores every saved flag.
// The serve loop is single-threaded, so no other call observes the transient
// state.
class ScopedFlagSaver {
 public:
  ScopedFlagSaver() = default;

  ~ScopedFlagSaver() {
    for (auto it = saved_.rbegin(); it != saved_.rend(); ++it) {
      google::SetCommandLineOption(it->first.c_str(), it->second.c_str());
    }
  }

  // Saves the current value of 'flag' and sets it to 'value'. Returns false if
  // the flag is unknown or the set failed (in which case any recorded prior
  // value is still restored on destruction, which is a no-op).
  bool SetAndSave(const string& flag, const string& value) {
    string current;
    if (!google::GetCommandLineOption(flag.c_str(), &current)) {
      return false;
    }
    saved_.emplace_back(flag, current);
    return !google::SetCommandLineOption(flag.c_str(), value.c_str()).empty();
  }

 private:
  vector<pair<string, string>> saved_;
  DISALLOW_COPY_AND_ASSIGN(ScopedFlagSaver);
};

// RAII guard honoring R3/S4: swaps std::cout's streambuf to a caller-provided
// buffer for the guard's lifetime and restores the original on destruction, so
// the action's cout output is captured and never leaks onto the protocol
// channel. Restoration happens even if Run() returns early. glog stays on
// stderr and is unaffected.
class ScopedCoutRedirect {
 public:
  explicit ScopedCoutRedirect(std::streambuf* to)
      : original_(std::cout.rdbuf(to)) {}

  ~ScopedCoutRedirect() { std::cout.rdbuf(original_); }

 private:
  std::streambuf* const original_;
  DISALLOW_COPY_AND_ASSIGN(ScopedCoutRedirect);
};

// Computes the value to inject for a connection argument the caller did not
// supply, from the serve-level FLAGS_master_addresses. The plural
// 'master_addresses' takes the full flag; the singular 'master_address' takes
// the first configured address (a single-master action wants one address).
// Returns "" if no addresses were configured at launch.
string InjectedConnectionValue(const string& arg_name) {
  const string& all = FLAGS_master_addresses;
  if (all.empty()) {
    return "";
  }
  if (arg_name == kMasterAddressArg) {
    return all.substr(0, all.find(','));
  }
  return all;
}

// Builds a successful JSON-RPC 'tools/call' response carrying an MCP tool
// result: content is a single text block, and 'isError' distinguishes a normal
// result from a tool-execution error (a non-OK action Status). Note this is a
// SUCCESSFUL JSON-RPC result even when is_error is true -- the tool ran and
// reported a failure, which is not a protocol-level error.
string BuildToolResultResponse(const rapidjson::Value* id,
                               const string& text,
                               bool is_error) {
  ostringstream ss;
  JsonWriter jw(&ss, JsonWriter::COMPACT);
  jw.StartObject();
  jw.String("jsonrpc");
  jw.String("2.0");
  jw.String("id");
  WriteJsonRpcId(&jw, id);
  jw.String("result");
  jw.StartObject();
  {
    jw.String("content");
    jw.StartArray();
    {
      jw.StartObject();
      jw.String("type");
      jw.String("text");
      jw.String("text");
      jw.String(text);
      jw.EndObject();
    }
    jw.EndArray();
    jw.String("isError");
    jw.Bool(is_error);
  }
  jw.EndObject();
  jw.EndObject();
  return ss.str();
}

// Finds a member of the 'arguments' object by name, or nullptr if 'arguments'
// is absent / not an object / lacks the member.
const rapidjson::Value* FindArgument(const rapidjson::Value* arguments,
                                     const string& key) {
  if (arguments == nullptr || !arguments->IsObject()) {
    return nullptr;
  }
  auto it = arguments->FindMember(key.c_str());
  if (it == arguments->MemberEnd()) {
    return nullptr;
  }
  return &it->value;
}

// Returns 'raw' unchanged if it is made up entirely of characters a POSIX shell
// treats literally; otherwise wraps it in single quotes (escaping any embedded
// single quote as the usual '\'' sequence) so the reconstructed dry-run command
// line is copy-pasteable into a shell without reinterpretation. An empty string
// becomes '' so it survives as a distinct (empty) argument.
string ShellQuote(const string& raw) {
  if (!raw.empty()) {
    bool safe = true;
    for (const char c : raw) {
      const bool ok = std::isalnum(static_cast<unsigned char>(c)) ||
                      c == '_' || c == '-' || c == '.' || c == '/' ||
                      c == ':' || c == ',' || c == '=' || c == '@' ||
                      c == '+' || c == '%';
      if (!ok) {
        safe = false;
        break;
      }
    }
    if (safe) {
      return raw;
    }
  }
  string out = "'";
  for (const char c : raw) {
    if (c == '\'') {
      out += "'\\''";
    } else {
      out += c;
    }
  }
  out += "'";
  return out;
}

// Reconstructs the faithful, copy-pasteable command line a GATED tools/call
// would execute, for the dry-run path (M5): the model sees exactly what would
// run instead of it running. Format:
//   kudu <chain-names...> <action-name> <positionals-in-declared-order> \
//        [--opt=value ...]
// Positional order matches args().required declaration order (which is why the
// injected master-address value is emitted at its real slot -- it is part of the
// command), followed by any variadic values, then the optional flags that were
// supplied, in declaration order. Each argument is shell-quoted as needed.
string BuildDryRunCommand(const McpToolEntry& entry,
                          const unordered_map<string, string>& required_args,
                          const vector<string>& variadic_args,
                          const vector<pair<string, string>>& optional_args) {
  const ActionArgsDescriptor& args = entry.action->args();
  ostringstream cmd;
  // DispositionCommandPath drops the root and joins mode names + action with
  // spaces, e.g. "table add_range_partition".
  cmd << "kudu " << DispositionCommandPath(entry.chain, entry.action);
  for (const auto& r : args.required) {
    const string* value = FindOrNull(required_args, r.name);
    // Every required arg (injected ones included) was marshalled before this
    // point, so it is present; stay defensive against a future refactor.
    if (value != nullptr) {
      cmd << " " << ShellQuote(*value);
    }
  }
  for (const auto& v : variadic_args) {
    cmd << " " << ShellQuote(v);
  }
  for (const auto& o : optional_args) {
    cmd << " --" << o.first << "=" << ShellQuote(o.second);
  }
  return cmd.str();
}

// Handles a 'tools/call' request. Resolves the tool name against the pre-built
// (already write-gated) registry, marshals the JSON arguments into the
// required/variadic/optional shapes Action::Run() expects, runs the action with
// cout captured (R3) and optional flags saved/restored (R2), and returns the
// captured text as an MCP tool result. Protocol-level problems (bad params,
// unknown tool) return a JSON-RPC error; an action-level failure returns a
// successful result carrying isError=true.
string HandleToolsCall(const JsonReader& reader,
                       const rapidjson::Value* req,
                       const rapidjson::Value* id,
                       const vector<McpToolEntry>& tools,
                       const unordered_set<string>& gated_tool_names) {
  const rapidjson::Value* params = nullptr;
  if (!reader.ExtractObject(req, "params", &params).ok()) {
    return BuildErrorResponse(id, kJsonRpcInvalidParams,
                              "Invalid params: missing 'params' object");
  }

  string name;
  if (!reader.ExtractString(params, "name", &name).ok()) {
    return BuildErrorResponse(id, kJsonRpcInvalidParams,
                              "Invalid params: missing tool 'name'");
  }

  // Resolve the name against the active (write-gated) registry. A GATED tool is
  // absent from it when the server was launched without --allow-writes.
  const McpToolEntry* entry = nullptr;
  for (const auto& e : tools) {
    if (e.tool_name == name) {
      entry = &e;
      break;
    }
  }
  if (entry == nullptr) {
    // Defense in depth (M5): a GATED tool is not merely hidden without
    // --allow-writes, it is explicitly rejected here -- and with an honest
    // message distinguishing "the write gate is closed" from "no such tool".
    // 'gated_tool_names' is the FULL (ungated) set of GATED tool names, so a
    // name that misses the active registry but is a known GATED tool means the
    // server simply was not started with --allow-writes.
    if (ContainsKey(gated_tool_names, name)) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: tool '$0' is a gated (mutating) tool and "
                     "is not enabled; start the server with --allow-writes to "
                     "use it", name));
    }
    return BuildErrorResponse(
        id, kJsonRpcInvalidParams,
        Substitute("Invalid params: unknown tool '$0'", name));
  }

  // 'arguments' is optional; absent (or an explicit null) means no arguments.
  const rapidjson::Value* arguments = nullptr;
  if (params->HasMember("arguments")) {
    const rapidjson::Value& a = (*params)["arguments"];
    if (a.IsObject()) {
      arguments = &a;
    } else if (!a.IsNull()) {
      return BuildErrorResponse(id, kJsonRpcInvalidParams,
                                "Invalid params: 'arguments' must be an object");
    }
  }

  // Dry-run (M5) is a synthetic MCP-level control, honored ONLY for GATED tools
  // (for a SURFACE tool it is a harmless unknown argument -- ignored, and never
  // advertised in the schema). When set, we still validate/marshal every
  // argument (so malformed args are still reported as -32602, never a partial
  // mutation) but return the reconstructed command instead of calling Run().
  // 'dry_run' is a control key, not an action argument: it is consumed here and
  // excluded from flag marshalling exactly as the reserved/injected args are.
  bool dry_run = false;
  if (entry->disposition.disposition == Disposition::GATED) {
    const rapidjson::Value* dr = FindArgument(arguments, "dry_run");
    if (dr != nullptr && dr->IsBool()) {
      dry_run = dr->GetBool();
    }
  }

  const ActionArgsDescriptor& args = entry->action->args();
  unordered_map<string, string> required_args;
  vector<string> variadic_args;

  // Required positional args, in declaration order (mirrors MarshalArgs).
  for (const auto& r : args.required) {
    const rapidjson::Value* v = FindArgument(arguments, r.name);
    if (IsInjectedConnectionArg(r.name)) {
      // Injected connection arg: use the caller-supplied value if present,
      // otherwise inject the serve-level FLAGS_master_addresses.
      string value;
      if (v != nullptr) {
        if (!JsonScalarToString(*v, &value)) {
          return BuildErrorResponse(
              id, kJsonRpcInvalidParams,
              Substitute("Invalid params: '$0' must be a string", r.name));
        }
      } else {
        value = InjectedConnectionValue(r.name);
      }
      if (value.empty()) {
        return BuildToolResultResponse(
            id,
            "No master addresses are configured. Launch the server with "
            "'kudu mcp serve --master_addresses=<addr>[,<addr>...]'.",
            /*is_error=*/true);
      }
      required_args[r.name] = std::move(value);
      continue;
    }
    if (v == nullptr) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: missing required argument '$0'", r.name));
    }
    string value;
    if (!JsonScalarToString(*v, &value)) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: argument '$0' must be a scalar", r.name));
    }
    required_args[r.name] = std::move(value);
  }

  // Variadic arg (at most one, at the end): a non-empty JSON array of scalars.
  if (args.variadic) {
    const string& vname = args.variadic->name;
    const rapidjson::Value* v = FindArgument(arguments, vname);
    if (v == nullptr || !v->IsArray() || v->Empty()) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: variadic argument '$0' must be a "
                     "non-empty array", vname));
    }
    for (const auto& elem : v->GetArray()) {
      string value;
      if (!JsonScalarToString(elem, &value)) {
        return BuildErrorResponse(
            id, kJsonRpcInvalidParams,
            Substitute("Invalid params: elements of '$0' must be scalars",
                       vname));
      }
      variadic_args.emplace_back(std::move(value));
    }
  }

  // Dry-run: the args are fully validated above, so any malformed argument has
  // already produced a -32602. Reconstruct the faithful command line and return
  // it WITHOUT calling Run() and WITHOUT touching any gflag: collect the
  // supplied optional flags (validating their scalar-ness, same as the execute
  // path) purely to render them, never applying them to process-global state.
  if (dry_run) {
    vector<pair<string, string>> optional_supplied;
    for (const auto& o : args.optional) {
      if (IsReservedControlFlag(o.name) || IsInjectedConnectionArg(o.name)) {
        continue;
      }
      const rapidjson::Value* v = FindArgument(arguments, o.name);
      if (v == nullptr) {
        continue;
      }
      string value;
      if (!JsonScalarToString(*v, &value)) {
        return BuildErrorResponse(
            id, kJsonRpcInvalidParams,
            Substitute("Invalid params: optional flag '$0' must be a scalar",
                       o.name));
      }
      optional_supplied.emplace_back(o.name, std::move(value));
    }
    const string command =
        BuildDryRunCommand(*entry, required_args, variadic_args, optional_supplied);
    return BuildToolResultResponse(id, command, /*is_error=*/false);
  }

  // R2: apply optional flags the action declares that appear in 'arguments',
  // saving and (on scope exit) restoring each. Reserved control flags and
  // injected connection args are never taken from per-call arguments.
  ScopedFlagSaver flag_saver;
  for (const auto& o : args.optional) {
    if (IsReservedControlFlag(o.name) || IsInjectedConnectionArg(o.name)) {
      continue;
    }
    const rapidjson::Value* v = FindArgument(arguments, o.name);
    if (v == nullptr) {
      continue;
    }
    string value;
    if (!JsonScalarToString(*v, &value)) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: optional flag '$0' must be a scalar",
                     o.name));
    }
    if (!flag_saver.SetAndSave(o.name, value)) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: could not set flag '$0'", o.name));
    }
  }

  // R3/S4: capture cout around Run() so the action's text output is returned in
  // the tool result rather than leaking onto the protocol channel. The redirect
  // and the flag saver both restore on scope exit, error path included.
  //
  // R4 (process-fatal audit): a surfaced (SURFACE/GATED) action returns its
  // failures as a non-OK Status rather than terminating the process, so a bad
  // model-supplied argument yields an MCP tool error (isError=true) and the
  // serve loop survives to answer the next request (see the survival assertion
  // in tool_action_mcp-itest.cc). One residual, framework-level exit path
  // remains and is out of scope here: Action::Run() calls kudu::ValidateFlags()
  // (tool_action.cc), which can exit(1) if an unsafe/experimental gflag is set
  // without the matching --unlock flag (CheckFlagsAllowed) or if a custom flag
  // validator fails (RunCustomValidators). This is not reachable from model
  // input under a normal operator config: no SURFACE/GATED tool exposes an
  // unsafe/experimental optional parameter (mechanically asserted by the
  // flag-tag guard test in tool_action_mcp-test.cc), and the serve process must
  // itself be started with any --unlock flags its own environment needs. We
  // deliberately do not wrap or fork Run() here: that shared flag-validation
  // framework is used identically by the CLI and must not be forked for MCP.
  ostringstream captured;
  Status run_status;
  {
    ScopedCoutRedirect redirect(captured.rdbuf());
    run_status = entry->action->Run(entry->chain, required_args, variadic_args);
  }

  if (!run_status.ok()) {
    // A non-OK Status is an MCP tool-execution error: a successful JSON-RPC
    // result carrying isError=true and the Status text (plus any partial output
    // the action managed to print before failing).
    string text = run_status.ToString();
    const string partial = captured.str();
    if (!partial.empty()) {
      text += "\n";
      text += partial;
    }
    return BuildToolResultResponse(id, text, /*is_error=*/true);
  }
  return BuildToolResultResponse(id, captured.str(), /*is_error=*/false);
}

} // anonymous namespace

// The single source of truth for the top-level CLI action tree (declared in
// tool_action.h). tool_main.cc's RootMode(), the MCP server's
// BuildMcpRootMode(), and the disposition-coverage test all delegate here so a
// newly added top-level mode is wired in exactly one place.
//
// Defined in this translation unit rather than tool_action.cc on purpose:
// tool_action.cc is compiled into the low-level kudu_tools_util library, and
// referencing every Build*Mode() factory from there would inject undefined
// symbols into every kudu_tools_util consumer (e.g. ksck-test) that does not
// link the tool_action_*.cc factories. This file already lives in the CLI
// sources (and KUDU_CLI_TOOL_SRCS_NO_MAIN) and already references every factory,
// so it is the natural, link-safe home.
unique_ptr<Mode> BuildRootMode(const string& name) {
  return ModeBuilder(name)
      .Description("Kudu Command Line Tools")
      .AddMode(BuildClusterMode())
      .AddMode(BuildDiagnoseMode())
      .AddMode(BuildFsMode())
      .AddMode(BuildHmsMode())
      .AddMode(BuildLocalReplicaMode())
      .AddMode(BuildMasterMode())
      .AddMode(BuildMcpMode())
      .AddMode(BuildPbcMode())
      .AddMode(BuildPerfMode())
      .AddMode(BuildRemoteReplicaMode())
      .AddMode(BuildTableMode())
      .AddMode(BuildTabletMode())
#if defined(KUDU_CLI_TEST_TOOL_ENABLED)
      .AddMode(BuildTestMode())
#endif
      .AddMode(BuildTxnMode())
      .AddMode(BuildTServerMode())
      .AddMode(BuildWalMode())
      .Build();
}

// Runs the MCP serve loop over the provided streams: a single-threaded
// JSON-RPC 2.0 dispatcher implementing the MCP lifecycle (M1).
//
// The loop reads newline-delimited JSON from 'in'. For each non-blank line it:
//   - parses the line with JsonReader; on parse failure it emits a JSON-RPC
//     parse-error response (-32700) with a null id and continues;
//   - distinguishes requests (which carry an 'id') from notifications (no 'id',
//     or any "notifications/*" method): notifications receive no response;
//   - dispatches requests by method:
//       * "initialize"  -> protocolVersion / capabilities.tools / serverInfo;
//       * "tools/list"  -> an empty tools array (populated in M3);
//       * unknown method -> JSON-RPC -32601 (method not found);
//       * a request with no method -> JSON-RPC -32600 (invalid request).
//
// Every response carries "jsonrpc":"2.0", echoes the request id exactly as
// sent, and contains EITHER "result" OR "error", never both. Blank lines are
// ignored. The loop terminates on EOF and returns Status::OK(). It is
// intentionally single-threaded; stdout carries the protocol only (glog goes to
// stderr), so 'out' must not be interleaved with other writers.
//
// "tools/call" (M4) resolves the tool name against the registry, marshals the
// JSON arguments, runs the action with cout captured (R3) and any optional
// flags saved/restored (R2), and returns the captured text as an MCP tool
// result (or a tool error carrying the action's non-OK Status). See
// HandleToolsCall().
//
// Factored out of the action runner so it can be unit tested against in-memory
// streams.
//
// Write gating and dry-run for GATED tools (M5) are enforced in the dispatch
// path (HandleToolsCall): the active registry is write-gated, so a GATED tool is
// unreachable without --allow-writes; the full (ungated) set of GATED tool names
// is threaded through so a gated call without --allow-writes is REJECTED with an
// honest message rather than merely hidden; GATED tools carry the destructive
// confirm annotation and a synthetic 'dry_run' schema property; and a dry-run
// call returns the reconstructed literal command instead of calling Run().
Status RunMcpServeLoop(std::istream& in, std::ostream& out) {
  // Build the action tree once for this serve session and reflect it into the
  // exposure registry. The tree must outlive the registry (its entries borrow
  // from it), so both live for the whole loop.
  //
  // The startup invariant: fail loudly and immediately if any reachable CLI
  // action is not classified in the disposition table (i.e. a new action was
  // added without a disposition entry), rather than silently mis-handling it.
  unique_ptr<Mode> root = BuildMcpRootMode();
  ValidateDispositionCoverageOrDie(root.get());
  const vector<McpToolEntry> tools =
      BuildMcpToolRegistry(root.get(), FLAGS_allow_writes);

  // The FULL (ungated) set of GATED tool names, computed once. It lets
  // HandleToolsCall tell "this tool exists but the write gate is closed" apart
  // from "no such tool" when a name misses the active (write-gated) registry --
  // the defense-in-depth server-side rejection M5 wants. When --allow-writes is
  // set the active registry already contains every GATED tool, so this set is
  // only ever consulted on the closed-gate path.
  unordered_set<string> gated_tool_names;
  for (const auto& e : BuildMcpToolRegistry(root.get(), /*allow_writes=*/true)) {
    if (e.disposition.disposition == Disposition::GATED) {
      gated_tool_names.insert(e.tool_name);
    }
  }

  string line;
  while (std::getline(in, line)) {
    // Ignore blank / whitespace-only lines: nothing to parse, nothing to
    // answer. This keeps the M0 behavior where blank framing lines are skipped.
    if (line.find_first_not_of(" \t\r\n") == string::npos) {
      continue;
    }

    JsonReader reader(line);
    Status s = reader.Init();
    if (!s.ok()) {
      // Malformed JSON must not take down a long-lived server. Reply with a
      // JSON-RPC parse error (null id, since we could not read one) and carry
      // on with the next line.
      LOG(WARNING) << "mcp: parse error on JSON-RPC line: " << s.ToString();
      out << BuildErrorResponse(/*id=*/nullptr, kJsonRpcParseError, "Parse error")
          << std::endl;
      continue;
    }

    const rapidjson::Value* root = reader.root();
    if (!root->IsObject()) {
      // Valid JSON, but not a JSON-RPC request object (e.g. a bare number or an
      // array). We cannot recover an id, so reply with a null-id invalid
      // request. Batch requests (arrays) are not supported in v1.
      out << BuildErrorResponse(/*id=*/nullptr, kJsonRpcInvalidRequest, "Invalid Request")
          << std::endl;
      continue;
    }

    // The presence of an 'id' member is what distinguishes a request (expects a
    // response) from a notification (never answered). A null id still counts as
    // present per JSON-RPC.
    const bool has_id = root->HasMember("id");
    const rapidjson::Value* id = has_id ? &(*root)["id"] : nullptr;

    // Extract the method. It must be a string to be usable.
    const bool has_method = root->HasMember("method") && (*root)["method"].IsString();
    const string method = has_method ? (*root)["method"].GetString() : string();

    // MCP notifications (method "notifications/*") are one-way: never answered,
    // regardless of whether an id was (incorrectly) supplied.
    if (has_method && HasPrefixString(method, "notifications/")) {
      LOG(INFO) << "mcp: received notification '" << method << "' (no reply)";
      continue;
    }

    // A request without an id is a JSON-RPC notification: produce no output.
    if (!has_id) {
      if (has_method) {
        LOG(INFO) << "mcp: received notification '" << method << "' (no reply)";
      }
      continue;
    }

    // From here on we have a request that requires a response.
    if (!has_method) {
      out << BuildErrorResponse(id, kJsonRpcInvalidRequest,
                                "Invalid Request: missing 'method'")
          << std::endl;
      continue;
    }

    LOG(INFO) << "mcp: received request method '" << method << "'";

    if (method == "initialize") {
      out << BuildInitializeResponse(id, NegotiateProtocolVersion(reader, root))
          << std::endl;
    } else if (method == "tools/list") {
      out << BuildToolsListResponse(id, tools) << std::endl;
    } else if (method == "tools/call") {
      out << HandleToolsCall(reader, root, id, tools, gated_tool_names)
          << std::endl;
    } else {
      out << BuildErrorResponse(id, kJsonRpcMethodNotFound,
                                Substitute("Method not found: $0", method))
          << std::endl;
    }
  }

  // getline stopped: either EOF or a stream error. Both terminate the server
  // cleanly.
  return Status::OK();
}

namespace {

Status RunMcpServe(const RunnerContext& /*context*/) {
  return RunMcpServeLoop(std::cin, std::cout);
}

} // anonymous namespace

std::unique_ptr<Mode> BuildMcpMode() {
  std::unique_ptr<Action> serve =
      ActionBuilder("serve", &RunMcpServe)
      .Description("Serve Kudu admin actions as MCP tools over stdio")
      .McpDisposition(Disposition::REJECT)
      .ExtraDescription("Reads newline-delimited JSON-RPC requests from stdin "
                        "and writes responses to stdout, one line per request, "
                        "until EOF. Intended to be launched by an MCP host.")
      .AddOptionalParameter("allow_writes")
      // The serve-level master addresses. Injected into tools/call dispatch for
      // actions that need them, so the addresses never appear in the
      // conversation (PRD section 3).
      .AddOptionalParameter("master_addresses")
      .Build();

  return ModeBuilder("mcp")
      .Description("Operate as a Model Context Protocol (MCP) server")
      .AddAction(std::move(serve))
      .Build();
}

} // namespace tools
} // namespace kudu
