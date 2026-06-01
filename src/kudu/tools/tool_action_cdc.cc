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
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
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
DEFINE_bool(checkpoint, false,
            "With --stream, durably checkpoint the consumed position, so that "
            "the next consumer of the stream starts after it. Without it the "
            "tool leaves the stream's checkpoint and snapshot where they are, "
            "so that it does not make the stream's other consumer skip changes, "
            "and it cannot take or continue a snapshot of the stream.");
DEFINE_int64(max_records, 0,
             "Stop consuming after delivering this many records. 0 means run "
             "until interrupted (Ctrl-C). With --checkpoint, the checkpoint "
             "does not move past a record that was not printed, so the next "
             "consume of the stream starts with it (and may print a few records "
             "again).");
DEFINE_string(stream_table_id, "",
              "If non-empty, only list streams covering this table id.");
DEFINE_bool(include_deleting, false,
            "Also list the streams that are being deleted, whose retention "
            "barriers may not have been released yet.");

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCDecodedColumn;
using kudu::cdc::CDCDecodedRecord;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamInfo;
using kudu::cdc::CDCStreamOptions;
using kudu::cdc::CDCTableMetadata;
using kudu::cdc::CDCTabletProgress;
using std::set;
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

// Renders microseconds since the Unix epoch as a UTC wall-clock string with
// microsecond precision.
string FormatMicros(uint64_t micros) {
  if (micros == 0) {
    return "-";
  }
  const time_t secs = static_cast<time_t>(micros / 1000000);
  const int usec = static_cast<int>(micros % 1000000);
  struct tm tm_utc;
  gmtime_r(&secs, &tm_utc);
  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_utc);
  return StringPrintf("%s.%06dZ", buf, usec);
}

// Renders a HybridTime as a UTC wall-clock string with microsecond precision.
// The physical component occupies all but the low 12 (logical) bits.
string FormatHybridTime(uint64_t ht) {
  return FormatMicros(ht >> 12);
}

const char* StreamStateToString(master::SysCDCStreamEntryPB::State state) {
  switch (state) {
    case master::SysCDCStreamEntryPB::ACTIVE:   return "active";
    case master::SysCDCStreamEntryPB::DELETING: return "deleting";
    case master::SysCDCStreamEntryPB::CREATING: return "creating";
    default:                                    return "unknown";
  }
}

// Whether a tablet's checkpoint still pins the tablet's WAL and history.
const char* TabletStatusToString(const master::CDCTabletStatusPB& t) {
  if (t.expired()) {
    return "expired";
  }
  if (t.stale()) {
    return "stale";
  }
  return "live";
}

string ColumnsToString(const vector<CDCDecodedColumn>& cols) {
  string out;
  for (int i = 0; i < static_cast<int>(cols.size()); i++) {
    if (i > 0) {
      out += ", ";
    }
    out += cols[i].name;
    out += "=";
    if (cols[i].is_null) {
      out += "NULL";
    } else if (!cols[i].decode_ok) {
      out += "<undecoded " + Slice(cols[i].raw_value).ToDebugString() + ">";
    } else {
      out += cols[i].value;
    }
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

// Returns the fixed-size value 'c' as a T.
template<typename T>
T RawValueAs(const CDCDecodedColumn& c) {
  T v;
  DCHECK_EQ(sizeof(v), c.raw_value.size());
  memcpy(&v, c.raw_value.data(), sizeof(v));
  return v;
}

// Writes the value of 'c', which is decoded and not null, as a JSON value of
// its type: a number for the integer, floating point, UNIXTIME_MICROS
// (microseconds since the epoch) and DATE (days since the epoch) types, a
// boolean for BOOL, and a string for the others: STRING and VARCHAR as they
// are, BINARY in base64, a DECIMAL with its scale, an array as Kudu renders it.
// A non-finite floating point number is a string too ("nan", "inf", "-inf").
void WriteValueJson(JsonWriter* w, const CDCDecodedColumn& c) {
  const auto write_double = [&](double d) {
    if (std::isfinite(d)) {
      w->Double(d);
    } else {
      w->String(std::isnan(d) ? "nan" : (d > 0 ? "inf" : "-inf"));
    }
  };
  switch (c.type_info->type()) {
    case BOOL: w->Bool(RawValueAs<uint8_t>(c) != 0); break;
    case INT8: w->Int64(RawValueAs<int8_t>(c)); break;
    case INT16: w->Int64(RawValueAs<int16_t>(c)); break;
    case INT32:
    case DATE: w->Int64(RawValueAs<int32_t>(c)); break;
    case INT64:
    case UNIXTIME_MICROS: w->Int64(RawValueAs<int64_t>(c)); break;
    case UINT8: w->Uint64(RawValueAs<uint8_t>(c)); break;
    case UINT16: w->Uint64(RawValueAs<uint16_t>(c)); break;
    case UINT32: w->Uint64(RawValueAs<uint32_t>(c)); break;
    case UINT64: w->Uint64(RawValueAs<uint64_t>(c)); break;
    case FLOAT: write_double(RawValueAs<float>(c)); break;
    case DOUBLE: write_double(RawValueAs<double>(c)); break;
    case STRING:
    case VARCHAR: w->String(c.raw_value); break;
    case BINARY: {
      string b64;
      strings::Base64Escape(c.raw_value, &b64);
      w->String(b64);
      break;
    }
    default:
      // DECIMAL (with its scale) and arrays.
      w->String(c.value);
      break;
  }
}

// Writes 'cols' as a JSON array of objects with the name, type, and typed
// value of each column (see WriteValueJson()). A value whose type the consumer
// does not know has "decode_ok": false and its bytes in base64 as "raw".
void WriteColumnsJson(JsonWriter* w, const vector<CDCDecodedColumn>& cols) {
  w->StartArray();
  for (const auto& c : cols) {
    w->StartObject();
    w->String("name");
    w->String(c.name);
    if (c.decode_ok) {
      w->String("type");
      w->String(c.type_info->name() +
                c.type_attributes.ToStringForType(c.type_info->type()));
    }
    w->String("is_null");
    w->Bool(c.is_null);
    if (!c.decode_ok) {
      w->String("decode_ok");
      w->Bool(false);
      if (!c.is_null) {
        string b64;
        strings::Base64Escape(c.raw_value, &b64);
        w->String("raw");
        w->String(b64);
      }
    } else if (!c.is_null) {
      w->String("value");
      WriteValueJson(w, c);
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
  RETURN_NOT_OK(client->ListStreams(FLAGS_stream_table_id, &streams,
                                    FLAGS_include_deleting));

  DataTable table({ "stream_id", "table_ids", "record_type", "snapshot_mode",
                    "state", "ephemeral", "created", "tablets" });
  for (const auto& s : streams) {
    table.AddRow({ s.stream_id,
                   JoinStrings(s.table_ids, ","),
                   RecordTypeToString(s.record_type),
                   SnapshotModeToString(s.snapshot_mode),
                   StreamStateToString(s.state),
                   s.ephemeral ? "true" : "false",
                   FormatMicros(s.creation_time_micros),
                   SimpleItoa(static_cast<int64_t>(s.tablet_checkpoints.size())) });
  }
  return table.PrintTo(std::cout);
}

Status DescribeStreamAction(const RunnerContext& context) {
  const string& stream_id = FindOrDie(context.required_args, kStreamIdArg);

  unique_ptr<CDCClient> client;
  RETURN_NOT_OK(BuildClient(context, &client));

  CDCStreamInfo info;
  RETURN_NOT_OK(client->GetStreamInfo(stream_id, &info, /*include_details=*/true));

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
    w.String("state");
    w.String(StreamStateToString(info.state));
    w.String("ephemeral");
    w.Bool(info.ephemeral);
    w.String("created");
    w.String(FormatMicros(info.creation_time_micros));
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
    w.String("tablets");
    w.StartArray();
    for (const auto& t : info.tablet_status) {
      w.StartObject();
      w.String("tablet_id");
      w.String(t.tablet_id());
      if (t.has_op_index()) {
        w.String("checkpoint_op_index");
        w.Int64(t.op_index());
      }
      w.String("last_active");
      w.String(FormatMicros(t.last_active_time_micros()));
      w.String("last_advance");
      w.String(FormatMicros(t.last_checkpoint_advance_time_micros()));
      w.String("status");
      w.String(TabletStatusToString(t));
      w.String("snapshot_active");
      w.Bool(t.snapshot_active());
      w.EndObject();
    }
    w.EndArray();
    w.EndObject();
    std::cout << ss.str() << std::endl;
    return Status::OK();
  }

  std::cout << "stream_id:     " << info.stream_id << "\n";
  std::cout << "record_type:   " << RecordTypeToString(info.record_type) << "\n";
  std::cout << "snapshot_mode: " << SnapshotModeToString(info.snapshot_mode) << "\n";
  std::cout << "state:         " << StreamStateToString(info.state) << "\n";
  std::cout << "ephemeral:     " << (info.ephemeral ? "true" : "false") << "\n";
  std::cout << "created:       " << FormatMicros(info.creation_time_micros) << "\n";
  std::cout << "table_ids:     " << JoinStrings(info.table_ids, ",") << "\n";
  std::cout << "checkpoints:   " << info.tablet_checkpoints.size() << " tablet(s)\n";
  if (!info.tablet_status.empty()) {
    // 'last_advance' is when the checkpoint last moved forward (or the consumer
    // reported being caught up); an expired or stale tablet no longer pins its
    // WAL, so a consumer that comes back must bootstrap again.
    DataTable table({ "tablet_id", "checkpoint_op_index", "last_active",
                      "last_advance", "status", "snapshot_active" });
    for (const auto& t : info.tablet_status) {
      table.AddRow({ t.tablet_id(),
                     t.has_op_index() ? SimpleItoa(t.op_index()) : "-",
                     FormatMicros(t.last_active_time_micros()),
                     FormatMicros(t.last_checkpoint_advance_time_micros()),
                     TabletStatusToString(t),
                     t.snapshot_active() ? "true" : "false" });
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

  // Install the signal handlers first, so that a Ctrl-C at any point below
  // stops gracefully and the temporary stream (if any) is deleted.
  g_interrupted.store(false);
  signal(SIGINT, &HandleSignal);
  signal(SIGTERM, &HandleSignal);
  SCOPED_CLEANUP({
    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);
  });

  unique_ptr<CDCClient> client;
  RETURN_NOT_OK(BuildClient(context, &client));

  // Resolve the table so we can validate an explicit stream and (if needed)
  // create a temporary one bound to the table's id.
  CDCTableMetadata md;
  RETURN_NOT_OK_PREPEND(client->GetTableMetadata(table_name, /*by_id=*/false, &md),
                        Substitute("could not resolve table '$0'", table_name));

  // Determine the stream to consume: an explicit durable one, or a temporary
  // stream we create here and delete on exit. The temporary stream is created
  // ephemeral, so the master deletes it if this process dies without deleting
  // it (SIGKILL, a lost session, ...) once it has been idle for
  // --cdc_ephemeral_stream_expiry_ms (on the masters).
  string stream_id = FLAGS_stream;
  bool ephemeral = stream_id.empty();
  // Snapshot progress is durable stream state, like the checkpoint.
  if (!ephemeral && !FLAGS_checkpoint && start_mode == CDCConsumer::kSnapshot) {
    return Status::InvalidArgument("--from=snapshot with --stream requires --checkpoint");
  }
  if (ephemeral) {
    CDCStreamOptions opts;
    opts.ephemeral = true;
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
  // The temporary stream is the tool's own: checkpoint it, so that it does not
  // retain the WAL from where it started.
  copts.checkpoint_enabled = ephemeral || FLAGS_checkpoint;

  // Shared record-count / printing state. The callback runs concurrently on
  // one thread per tablet, so serialize output with a mutex.
  std::mutex print_lock;
  // The dropped range partitions whose changes were not all consumed.
  // Guarded by 'print_lock'.
  vector<string> lost_partitions;
  // The number of tablets whose range partition was dropped.
  std::atomic<int> num_dropped(0);
  // Reported when it happens: the poller of a dropped tablet is removed soon
  // after.
  copts.on_tablet_stopped = [&](const CDCTabletProgress& p) {
    if (p.state != cdc::CDCTabletState::kDropped) {
      return;
    }
    num_dropped.fetch_add(1);
    std::lock_guard<std::mutex> l(print_lock);
    std::cerr << "Tablet " << p.tablet_id << ": range partition "
              << p.dropped_partition_description << " was dropped"
              << (p.dropped_tail_lost ? ", and changes not consumed are lost" : "")
              << std::endl;
    if (p.dropped_tail_lost) {
      lost_partitions.emplace_back(p.dropped_partition_description);
    }
  };

  unique_ptr<CDCConsumer> consumer;
  RETURN_NOT_OK(CDCConsumer::Create(client.get(), std::move(copts), &consumer));
  std::atomic<int64_t> delivered(0);
  std::atomic<bool> reached_max_records(false);
  const int64_t max_records = FLAGS_max_records;
  // The tablets the callback stopped at --max_records. Guarded by 'print_lock'.
  set<string> stopped_at_max_records;

  RETURN_NOT_OK(consumer->Start([&](const CDCRecordBatch& batch) -> Status {
    std::lock_guard<std::mutex> l(print_lock);
    for (const auto& r : batch.records) {
      if (max_records > 0 && delivered.load() >= max_records) {
        // A batch is checkpointed only if the callback succeeds. Fail this one,
        // which stops the tablet's poller without checkpointing past the
        // records that were not printed, so a later consume of the stream
        // delivers them (with the printed part of the batch again).
        reached_max_records.store(true);
        g_interrupted.store(true);
        stopped_at_max_records.insert(batch.tablet_id);
        std::cout.flush();
        return Status::Aborted("reached --max_records");
      }
      if (json) {
        PrintRecordJson(r, std::cout);
      } else {
        PrintRecordPretty(r, std::cout);
      }
      delivered.fetch_add(1);
    }
    if (max_records > 0 && delivered.load() >= max_records) {
      reached_max_records.store(true);
      g_interrupted.store(true);
    }
    std::cout.flush();
    return Status::OK();
  }));

  // Run until interrupted, or until every tablet's poller stopped by itself:
  // on an error, because its range partition was dropped, or after the
  // snapshot of an INITIAL_ONLY stream. The pollers of dropped tablets are
  // removed, so no poller is left if every range partition was dropped.
  bool all_stopped = false;
  while (!g_interrupted.load() && !all_stopped) {
    SleepFor(MonoDelta::FromMilliseconds(200));
    vector<CDCTabletProgress> progress;
    consumer->GetProgress(&progress);
    all_stopped = (!progress.empty() || num_dropped.load() > 0) &&
        !reached_max_records.load() &&
        std::all_of(progress.begin(), progress.end(),
                    [](const CDCTabletProgress& p) { return cdc::IsFinished(p.state); });
  }

  // Fail if consuming stopped on any tablet, other than at --max_records.
  Status result;
  {
    vector<CDCTabletProgress> progress;
    consumer->GetProgress(&progress);
    std::lock_guard<std::mutex> l(print_lock);
    for (const auto& p : progress) {
      if (!cdc::IsTerminal(p.state) || ContainsKey(stopped_at_max_records, p.tablet_id)) {
        continue;
      }
      std::cerr << "Tablet " << p.tablet_id << " stopped ("
                << cdc::CDCTabletStateToString(p.state) << "): "
                << p.terminal_error.ToString() << std::endl;
      if (result.ok()) {
        result = Status::RuntimeError(
            all_stopped ? string("consuming stopped on every tablet")
                        : Substitute("consuming stopped on tablet $0", p.tablet_id),
            p.terminal_error.ToString());
      }
    }
  }

  // Best-effort final checkpoint before shutting down, then stop pollers.
  WARN_NOT_OK(consumer->Flush(), "final checkpoint failed");
  consumer->Stop();
  if (result.ok() && !lost_partitions.empty()) {
    result = Status::RuntimeError("the changes of dropped range partitions were lost",
                                  JoinStrings(lost_partitions, ", "));
  }

  std::cerr << "Consumed " << delivered.load() << " record(s)." << std::endl;
  return result;
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
      .AddOptionalParameter("include_deleting")
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
          "a temporary stream is created and deleted on exit (or by the master, "
          "once idle, if the tool is killed); pass --stream to "
          "consume an existing stream that covers the table, and --checkpoint "
          "to also move its checkpoint (and to take or continue its snapshot). "
          "Runs until interrupted (Ctrl-C) unless --max_records is set, until "
          "consuming stopped on every tablet with an error, and fails if "
          "consuming stopped on any tablet with an error. With --format=json, "
          "each record is a line with a JSON object, whose column values are "
          "JSON values of the column's type: numbers for integers, floating "
          "point numbers, UNIXTIME_MICROS (microseconds since the epoch) and "
          "DATE (days since the epoch), booleans for BOOL, and strings for "
          "STRING, VARCHAR, BINARY (base64) and DECIMAL.")
      .AddRequiredParameter({ kTableNameArg, "Name of the table to consume" })
      .AddOptionalParameter("stream")
      .AddOptionalParameter("checkpoint")
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
