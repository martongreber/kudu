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
#include <atomic>
#include <csignal>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DECLARE_int64(timeout_ms);
DECLARE_string(format);

DEFINE_string(record_type, "change",
              "Record type for a new stream. One of: change (after-image only) "
              "or full (before- and after-image).");
DEFINE_string(snapshot_mode, "never",
              "Snapshot mode for a new stream. One of: never, "
              "initial_and_continue, or initial_only.");
DEFINE_string(from, "now",
              "Where a tablet without a durable checkpoint should start when "
              "consuming. One of: now (tail the current end), earliest (oldest "
              "retained WAL), or snapshot (emit a consistent snapshot first, "
              "then follow live changes).");
DEFINE_string(stream, "",
              "Consume an existing (durable) stream with this id instead of "
              "creating a temporary one. The stream must cover the named table.");
DEFINE_int64(max_records, 0,
             "Stop consuming after delivering this many records. 0 means run "
             "until interrupted (Ctrl-C).");
DEFINE_string(stream_table_id, "",
              "If non-empty, only list streams covering this table id.");

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCDecodedColumn;
using kudu::cdc::CDCDecodedRecord;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamInfo;
using kudu::cdc::CDCStreamOptions;
using kudu::cdc::CDCTableMetadata;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

const char* const kStreamIdArg = "stream_id";

// Set by the SIGINT/SIGTERM handler to request a graceful shutdown of a
// running 'consume'.
std::atomic<bool> g_interrupted(false);

void HandleSignal(int /*signum*/) {
  g_interrupted.store(true);
}

// ---- enum <-> string mapping helpers ----------------------------------

Status ParseRecordType(const string& s,
                       master::CDCStreamConfigPB::RecordType* out) {
  string lower;
  ToLowerCase(s, &lower);
  if (lower == "change") {
    *out = master::CDCStreamConfigPB::CHANGE;
  } else if (lower == "full") {
    *out = master::CDCStreamConfigPB::FULL;
  } else {
    return Status::InvalidArgument(
        Substitute("unknown record type '$0' (expected 'change' or 'full')", s));
  }
  return Status::OK();
}

const char* RecordTypeToString(master::CDCStreamConfigPB::RecordType t) {
  switch (t) {
    case master::CDCStreamConfigPB::CHANGE: return "change";
    case master::CDCStreamConfigPB::FULL:   return "full";
    default:                                return "unknown";
  }
}

Status ParseSnapshotMode(const string& s,
                         master::CDCStreamConfigPB::SnapshotMode* out) {
  string lower;
  ToLowerCase(s, &lower);
  if (lower == "never") {
    *out = master::CDCStreamConfigPB::NEVER;
  } else if (lower == "initial_and_continue") {
    *out = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  } else if (lower == "initial_only") {
    *out = master::CDCStreamConfigPB::INITIAL_ONLY;
  } else {
    return Status::InvalidArgument(Substitute(
        "unknown snapshot mode '$0' (expected 'never', 'initial_and_continue', "
        "or 'initial_only')", s));
  }
  return Status::OK();
}

const char* SnapshotModeToString(master::CDCStreamConfigPB::SnapshotMode m) {
  switch (m) {
    case master::CDCStreamConfigPB::NEVER:                return "never";
    case master::CDCStreamConfigPB::INITIAL_AND_CONTINUE: return "initial_and_continue";
    case master::CDCStreamConfigPB::INITIAL_ONLY:         return "initial_only";
    default:                                              return "unknown";
  }
}

Status ParseStartMode(const string& s, CDCConsumer::StartMode* out) {
  string lower;
  ToLowerCase(s, &lower);
  if (lower == "now") {
    *out = CDCConsumer::kNow;
  } else if (lower == "earliest") {
    *out = CDCConsumer::kEarliest;
  } else if (lower == "snapshot") {
    *out = CDCConsumer::kSnapshot;
  } else {
    return Status::InvalidArgument(Substitute(
        "unknown start point '$0' (expected 'now', 'earliest', or 'snapshot')", s));
  }
  return Status::OK();
}

const char* OpTypeToString(cdc::CDCOpTypePB op) {
  switch (op) {
    case cdc::INSERT: return "INSERT";
    case cdc::UPDATE: return "UPDATE";
    case cdc::DELETE: return "DELETE";
    case cdc::UPSERT: return "UPSERT";
    case cdc::DDL:    return "DDL";
    case cdc::BEGIN:  return "BEGIN";
    case cdc::COMMIT: return "COMMIT";
    case cdc::ABORT:  return "ABORT";
    case cdc::READ:   return "READ";
    default:          return "UNKNOWN";
  }
}

// Renders a HybridTime as a UTC wall-clock string with microsecond precision.
// The physical component occupies all but the low 12 (logical) bits.
string FormatHybridTime(uint64_t ht) {
  if (ht == 0) {
    return "-";
  }
  const uint64_t micros = ht >> 12;
  const time_t secs = static_cast<time_t>(micros / 1000000);
  const int usec = static_cast<int>(micros % 1000000);
  struct tm tm_utc;
  gmtime_r(&secs, &tm_utc);
  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_utc);
  return StringPrintf("%s.%06dZ", buf, usec);
}

string ColumnsToString(const vector<CDCDecodedColumn>& cols) {
  string out;
  for (int i = 0; i < static_cast<int>(cols.size()); i++) {
    if (i > 0) {
      out += ", ";
    }
    out += cols[i].name;
    out += "=";
    out += cols[i].is_null ? "NULL" : cols[i].value;
  }
  return out;
}

// ---- record printers --------------------------------------------------

void PrintRecordPretty(const CDCDecodedRecord& r, std::ostream& out) {
  out << "[" << FormatHybridTime(r.timestamp) << "] "
      << OpTypeToString(r.op_type)
      << " tablet=" << r.tablet_id
      << " op_index=" << r.op_index;
  if (r.has_txn_id) {
    out << " txn_id=" << r.txn_id;
  }
  if (!r.after.empty()) {
    out << "  {" << ColumnsToString(r.after) << "}";
  }
  if (!r.before.empty()) {
    out << "  before={" << ColumnsToString(r.before) << "}";
  }
  if (r.has_new_schema) {
    out << "  new_schema_version=" << r.new_schema_version;
  }
  out << "\n";
}

void WriteColumnsJson(JsonWriter* w, const vector<CDCDecodedColumn>& cols) {
  w->StartArray();
  for (const auto& c : cols) {
    w->StartObject();
    w->String("name");
    w->String(c.name);
    w->String("is_null");
    w->Bool(c.is_null);
    if (!c.is_null) {
      w->String("value");
      w->String(c.value);
    }
    w->EndObject();
  }
  w->EndArray();
}

void PrintRecordJson(const CDCDecodedRecord& r, std::ostream& out) {
  std::ostringstream ss;
  JsonWriter w(&ss, JsonWriter::COMPACT);
  w.StartObject();
  w.String("op_type");
  w.String(OpTypeToString(r.op_type));
  w.String("tablet_id");
  w.String(r.tablet_id);
  w.String("op_index");
  w.Int64(r.op_index);
  w.String("op_term");
  w.Int64(r.op_term);
  w.String("timestamp");
  w.String(FormatHybridTime(r.timestamp));
  w.String("schema_version");
  w.Int64(r.schema_version);
  if (r.has_commit_timestamp) {
    w.String("commit_timestamp");
    w.String(FormatHybridTime(r.commit_timestamp));
  }
  if (r.has_txn_id) {
    w.String("txn_id");
    w.String(r.txn_id);
  }
  w.String("after");
  WriteColumnsJson(&w, r.after);
  if (!r.before.empty()) {
    w.String("before");
    WriteColumnsJson(&w, r.before);
  }
  if (r.has_new_schema) {
    w.String("new_schema_version");
    w.Int64(r.new_schema_version);
  }
  w.EndObject();
  out << ss.str() << "\n";
}

Status BuildClient(const RunnerContext& context, unique_ptr<CDCClient>* client) {
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));
  CDCClient::Options opts;
  opts.master_addresses = std::move(master_addresses);
  opts.rpc_timeout = MonoDelta::FromMilliseconds(FLAGS_timeout_ms);
  return CDCClient::Create(std::move(opts), client);
}

// ---- actions ----------------------------------------------------------

Status CreateStreamAction(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);

  CDCStreamOptions opts;
  RETURN_NOT_OK(ParseRecordType(FLAGS_record_type, &opts.record_type));
  RETURN_NOT_OK(ParseSnapshotMode(FLAGS_snapshot_mode, &opts.snapshot_mode));

  unique_ptr<CDCClient> client;
  RETURN_NOT_OK(BuildClient(context, &client));

  string stream_id;
  RETURN_NOT_OK(client->CreateStream(table_name, opts, &stream_id));
  std::cout << stream_id << std::endl;
  return Status::OK();
}

Status ListStreamsAction(const RunnerContext& context) {
  unique_ptr<CDCClient> client;
  RETURN_NOT_OK(BuildClient(context, &client));

  vector<CDCStreamInfo> streams;
  RETURN_NOT_OK(client->ListStreams(FLAGS_stream_table_id, &streams));

  DataTable table({ "stream_id", "table_ids", "record_type",
                    "snapshot_mode", "tablets" });
  for (const auto& s : streams) {
    table.AddRow({ s.stream_id,
                   JoinStrings(s.table_ids, ","),
                   RecordTypeToString(s.record_type),
                   SnapshotModeToString(s.snapshot_mode),
                   SimpleItoa(static_cast<int64_t>(s.tablet_checkpoints.size())) });
  }
  return table.PrintTo(std::cout);
}

Status DescribeStreamAction(const RunnerContext& context) {
  const string& stream_id = FindOrDie(context.required_args, kStreamIdArg);

  unique_ptr<CDCClient> client;
  RETURN_NOT_OK(BuildClient(context, &client));

  CDCStreamInfo info;
  RETURN_NOT_OK(client->GetStreamInfo(stream_id, &info));

  if (iequals(FLAGS_format, "json")) {
    std::ostringstream ss;
    JsonWriter w(&ss, JsonWriter::PRETTY);
    w.StartObject();
    w.String("stream_id");
    w.String(info.stream_id);
    w.String("record_type");
    w.String(RecordTypeToString(info.record_type));
    w.String("snapshot_mode");
    w.String(SnapshotModeToString(info.snapshot_mode));
    w.String("table_ids");
    w.StartArray();
    for (const auto& t : info.table_ids) {
      w.String(t);
    }
    w.EndArray();
    w.String("tablet_checkpoints");
    w.StartObject();
    for (const auto& e : info.tablet_checkpoints) {
      w.String(e.first);
      w.Int64(e.second);
    }
    w.EndObject();
    w.EndObject();
    std::cout << ss.str() << std::endl;
    return Status::OK();
  }

  std::cout << "stream_id:     " << info.stream_id << "\n";
  std::cout << "record_type:   " << RecordTypeToString(info.record_type) << "\n";
  std::cout << "snapshot_mode: " << SnapshotModeToString(info.snapshot_mode) << "\n";
  std::cout << "table_ids:     " << JoinStrings(info.table_ids, ",") << "\n";
  std::cout << "checkpoints:   " << info.tablet_checkpoints.size() << " tablet(s)\n";
  if (!info.tablet_checkpoints.empty()) {
    DataTable table({ "tablet_id", "checkpoint_op_index" });
    for (const auto& e : info.tablet_checkpoints) {
      table.AddRow({ e.first, SimpleItoa(e.second) });
    }
    RETURN_NOT_OK(table.PrintTo(std::cout));
  }
  return Status::OK();
}

Status DeleteStreamAction(const RunnerContext& context) {
  const string& stream_id = FindOrDie(context.required_args, kStreamIdArg);

  unique_ptr<CDCClient> client;
  RETURN_NOT_OK(BuildClient(context, &client));

  RETURN_NOT_OK(client->DeleteStream(stream_id));
  std::cout << "Deleted stream " << stream_id << std::endl;
  return Status::OK();
}

Status ConsumeAction(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const bool json = iequals(FLAGS_format, "json");

  CDCConsumer::StartMode start_mode;
  RETURN_NOT_OK(ParseStartMode(FLAGS_from, &start_mode));

  unique_ptr<CDCClient> client;
  RETURN_NOT_OK(BuildClient(context, &client));

  // Resolve the table so we can validate an explicit stream and (if needed)
  // create a temporary one bound to the table's id.
  CDCTableMetadata md;
  RETURN_NOT_OK_PREPEND(client->GetTableMetadata(table_name, /*by_id=*/false, &md),
                        Substitute("could not resolve table '$0'", table_name));

  // Determine the stream to consume: an explicit durable one, or a temporary
  // stream we create here and delete on exit.
  string stream_id = FLAGS_stream;
  bool ephemeral = stream_id.empty();
  if (ephemeral) {
    CDCStreamOptions opts;
    RETURN_NOT_OK(ParseRecordType(FLAGS_record_type, &opts.record_type));
    // A snapshot start requires a stream configured to produce one.
    if (start_mode == CDCConsumer::kSnapshot) {
      opts.snapshot_mode = master::CDCStreamConfigPB::INITIAL_AND_CONTINUE;
    }
    RETURN_NOT_OK_PREPEND(client->CreateStream(table_name, opts, &stream_id),
                          "could not create temporary stream");
    std::cerr << "Created temporary stream " << stream_id
              << " (will be deleted on exit)" << std::endl;
  } else {
    // Validate that the named stream covers this table.
    CDCStreamInfo info;
    RETURN_NOT_OK_PREPEND(client->GetStreamInfo(stream_id, &info),
                          Substitute("could not describe stream '$0'", stream_id));
    if (std::find(info.table_ids.begin(), info.table_ids.end(), md.table_id) ==
        info.table_ids.end()) {
      return Status::InvalidArgument(Substitute(
          "stream '$0' does not cover table '$1' (id $2)",
          stream_id, table_name, md.table_id));
    }
  }

  // Ensure a temporary stream is cleaned up regardless of how we exit.
  auto cleanup = MakeScopedCleanup([&]() {
    if (ephemeral && !stream_id.empty()) {
      Status s = client->DeleteStream(stream_id);
      if (s.ok()) {
        std::cerr << "Deleted temporary stream " << stream_id << std::endl;
      } else {
        std::cerr << "Warning: could not delete temporary stream "
                  << stream_id << ": " << s.ToString() << std::endl;
      }
    }
  });

  CDCConsumer::Options copts;
  copts.stream_id = stream_id;
  copts.start_mode = start_mode;

  unique_ptr<CDCConsumer> consumer;
  RETURN_NOT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));

  // Shared record-count / printing state. The callback runs concurrently on
  // one thread per tablet, so serialize output with a mutex.
  std::mutex print_lock;
  std::atomic<int64_t> delivered(0);
  const int64_t max_records = FLAGS_max_records;

  RETURN_NOT_OK(consumer->Start([&](const CDCRecordBatch& batch) -> Status {
    std::lock_guard<std::mutex> l(print_lock);
    for (const auto& r : batch.records) {
      if (json) {
        PrintRecordJson(r, std::cout);
      } else {
        PrintRecordPretty(r, std::cout);
      }
      const int64_t n = delivered.fetch_add(1) + 1;
      if (max_records > 0 && n >= max_records) {
        g_interrupted.store(true);
        break;
      }
    }
    std::cout.flush();
    return Status::OK();
  }));

  // Install signal handlers so Ctrl-C triggers a graceful, checkpointing stop.
  g_interrupted.store(false);
  signal(SIGINT, &HandleSignal);
  signal(SIGTERM, &HandleSignal);

  while (!g_interrupted.load()) {
    SleepFor(MonoDelta::FromMilliseconds(200));
  }

  // Best-effort final checkpoint before shutting down, then stop pollers.
  WARN_NOT_OK(consumer->Flush(), "final checkpoint failed");
  consumer->Stop();

  signal(SIGINT, SIG_DFL);
  signal(SIGTERM, SIG_DFL);

  std::cerr << "Consumed " << delivered.load() << " record(s)." << std::endl;
  return Status::OK();
}

}  // anonymous namespace

unique_ptr<Mode> BuildCdcMode() {
  unique_ptr<Action> create =
      ClusterActionBuilder("create", &CreateStreamAction)
      .Description("Create a new CDC stream on a table")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to capture" })
      .AddOptionalParameter("record_type")
      .AddOptionalParameter("snapshot_mode")
      .Build();

  unique_ptr<Action> list =
      ClusterActionBuilder("list", &ListStreamsAction)
      .Description("List CDC streams")
      .AddOptionalParameter("stream_table_id")
      .AddOptionalParameter("format")
      .Build();

  unique_ptr<Action> describe =
      ClusterActionBuilder("describe", &DescribeStreamAction)
      .Description("Show details of a CDC stream")
      .AddRequiredParameter({ kStreamIdArg, "Id of the stream to describe" })
      .AddOptionalParameter("format")
      .Build();

  unique_ptr<Action> del =
      ClusterActionBuilder("delete", &DeleteStreamAction)
      .Description("Delete a CDC stream")
      .AddRequiredParameter({ kStreamIdArg, "Id of the stream to delete" })
      .Build();

  unique_ptr<Mode> stream =
      ModeBuilder("stream")
      .Description("Manage CDC streams")
      .AddAction(std::move(create))
      .AddAction(std::move(list))
      .AddAction(std::move(describe))
      .AddAction(std::move(del))
      .Build();

  unique_ptr<Action> consume =
      ClusterActionBuilder("consume", &ConsumeAction)
      .Description("Consume and print change records for a table")
      .ExtraDescription(
          "Follows a table's changes and prints each record. Without --stream, "
          "a temporary stream is created and deleted on exit; pass --stream to "
          "consume (and durably checkpoint) an existing stream that covers the "
          "table. Runs until interrupted (Ctrl-C) unless --max_records is set.")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to consume" })
      .AddOptionalParameter("stream")
      .AddOptionalParameter("from")
      .AddOptionalParameter("record_type")
      .AddOptionalParameter("max_records")
      .AddOptionalParameter("format")
      .Build();

  return ModeBuilder("cdc")
      .Description("Operate on Kudu Change Data Capture (CDC) streams")
      .AddMode(std::move(stream))
      .AddAction(std::move(consume))
      .Build();
}

}  // namespace tools
}  // namespace kudu
