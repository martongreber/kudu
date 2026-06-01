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

// End-to-end tests of CDCConsumer on a cluster with three tablet servers and
// RF 3: the consumer's copy of the table (a CDCMirror) must equal the table
// after leader changes, tablet server restarts and consumer restarts, for
// every column type, across schema changes, and across range partitions that
// are added, dropped and added again.

#include <atomic>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cdc_mirror.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/int128.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_bool(cdc_enabled);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_max_records_per_response);
DECLARE_int32(cdc_tablet_rescan_interval_ms);
DECLARE_int64(cdc_checkpoint_persist_interval_ms);

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamOptions;
using kudu::cdc::CDCTableMetadata;
using kudu::cdc::CDCTabletProgress;
using kudu::cdc::CDCTabletState;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::KuduWriteOperation;
using kudu::consensus::RaftPeerPB;
using kudu::itest::CDCMirror;
using kudu::master::CDCStreamConfigPB;
using kudu::tablet::TabletReplica;
using std::atomic;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {
constexpr const char* kTableName = "cdc_consumer_itest";
constexpr int kRangeWidth = 1000;
const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
} // anonymous namespace

class CDCConsumerITest : public MiniClusterITestBase {
 protected:
  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_cdc_enabled = true;
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
    FLAGS_catalog_manager_bg_task_wait_ms = 100;
    FLAGS_cdc_bg_scan_interval_ms = 100;
    FLAGS_cdc_tablet_rescan_interval_ms = 500;
    FLAGS_cdc_checkpoint_persist_interval_ms = 100;
  }

  void TearDown() override {
    StopWriter();
    if (consumer_) {
      consumer_->Stop();
      consumer_.reset();
    }
    cdc_client_.reset();
    MiniClusterITestBase::TearDown();
  }

  // Starts a cluster of three tablet servers, and creates the table with
  // 'schema', RF 3 and a range partition [i * kRangeWidth, (i + 1) *
  // kRangeWidth) for each i < 'num_ranges'.
  void StartClusterAndCreateTable(const KuduSchema& schema, int num_ranges) {
    NO_FATALS(StartCluster(3));
    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name(kTableName)
        .schema(&schema)
        .set_range_partition_columns({ "key" })
        .num_replicas(3);
    for (int i = 0; i < num_ranges; i++) {
      unique_ptr<KuduPartialRow> lb(schema.NewRow());
      unique_ptr<KuduPartialRow> ub(schema.NewRow());
      ASSERT_OK(lb->SetInt32("key", i * kRangeWidth));
      ASSERT_OK(ub->SetInt32("key", (i + 1) * kRangeWidth));
      creator->add_range_partition(lb.release(), ub.release());
    }
    ASSERT_OK(creator->Create());
    CDCClient::Options opts;
    opts.master_addresses.push_back(cluster_->mini_master()->bound_rpc_addr().ToString());
    opts.rpc_timeout = kTimeout;
    ASSERT_OK(CDCClient::Create(std::move(opts), &cdc_client_));
  }

  static KuduSchema KeyValSchema() {
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(KuduColumnSchema::INT32);
    CHECK_OK(b.Build(&schema));
    return schema;
  }

  void CreateStream(CDCStreamConfigPB::RecordType record_type,
                    CDCStreamConfigPB::SnapshotMode snapshot_mode) {
    CDCStreamOptions opts;
    opts.record_type = record_type;
    opts.snapshot_mode = snapshot_mode;
    ASSERT_OK(cdc_client_->CreateStream(kTableName, opts, &stream_id_));
    CDCTableMetadata md;
    ASSERT_OK(cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md));
    mirror_.reset(new CDCMirror({ md.schema.column_id(0) },
                                record_type == CDCStreamConfigPB::FULL));
  }

  // Starts a consumer of the stream that applies its records to 'mirror_'
  // and checks that every value was decoded.
  void StartConsumer(CDCConsumer::StartMode start_mode) {
    CDCConsumer::Options opts;
    opts.stream_id = stream_id_;
    opts.start_mode = start_mode;
    opts.checkpoint_interval = MonoDelta::FromMilliseconds(100);
    opts.min_poll_backoff = MonoDelta::FromMilliseconds(20);
    opts.max_poll_backoff = MonoDelta::FromMilliseconds(200);
    opts.on_tablet_stopped = [this](const CDCTabletProgress& p) {
      if (p.state == CDCTabletState::kDropped && !p.dropped_tail_lost) {
        mirror_->RemoveTabletRows(p.tablet_id);
        return;
      }
      AddError(Substitute("tablet $0 stopped in $1: $2", p.tablet_id,
                          cdc::CDCTabletStateToString(p.state), p.terminal_error.ToString()));
    };
    ASSERT_OK(CDCConsumer::Create(cdc_client_.get(), std::move(opts), &consumer_));
    ASSERT_OK(consumer_->Start([this](const CDCRecordBatch& batch) {
      for (const auto& r : batch.records) {
        for (const auto* cols : { &r.after, &r.before }) {
          for (const auto& c : *cols) {
            if (check_decoded_ && !c.decode_ok && !c.is_null) {
              AddError(Substitute("column $0 of a record of tablet $1 was not decoded",
                                  c.name, batch.tablet_id));
            }
          }
        }
      }
      mirror_->Apply(batch);
      return Status::OK();
    }));
  }

  void RestartConsumer(CDCConsumer::StartMode start_mode) {
    WARN_NOT_OK(consumer_->Flush(), "flush failed");
    consumer_->Stop();
    consumer_.reset();
    NO_FATALS(StartConsumer(start_mode));
  }

  void AddError(string error) {
    LOG(WARNING) << error;
    std::lock_guard<std::mutex> l(lock_);
    errors_.emplace_back(std::move(error));
  }

  // The consumer's copy of the table equals the table, and nothing went wrong.
  void CheckCopy() {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    ASSERT_EVENTUALLY([&] {
      CDCTableMetadata md;
      ASSERT_OK(cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md));
      string diff;
      ASSERT_OK(mirror_->CompareWithTable(table.get(), md.schema, &diff));
      ASSERT_TRUE(diff.empty()) << diff << ProgressToString();
    });
    EXPECT_TRUE(mirror_->violations().empty()) << JoinStrings(mirror_->violations(), "\n");
    std::lock_guard<std::mutex> l(lock_);
    EXPECT_TRUE(errors_.empty()) << JoinStrings(errors_, "\n");
  }

  // The consumer's progress on each tablet, for failure messages.
  string ProgressToString() const {
    if (!consumer_) return "";
    vector<CDCTabletProgress> progress;
    consumer_->GetProgress(&progress);
    string out = "consumer progress:\n";
    for (const auto& p : progress) {
      out += Substitute("  tablet $0: $1, delivered $2, checkpointed $3, $4 errors, last: $5\n",
                        p.tablet_id, cdc::CDCTabletStateToString(p.state),
                        p.last_delivered_op_index, p.last_checkpointed_op_index, p.num_errors,
                        p.last_error.ToString());
    }
    return out;
  }

  // Writes random rows of the key-val table with keys in [0, 'key_space')
  // until StopWriter().
  void StartWriter(int key_space) {
    writer_ = thread([this, key_space] {
      Random rng(SeedRandom());
      while (!stop_writer_) {
        client::sp::shared_ptr<KuduTable> table;
        if (!client_->OpenTable(kTableName, &table).ok()) continue;
        client::sp::shared_ptr<KuduSession> session = client_->NewSession();
        session->SetTimeoutMillis(kTimeout.ToMilliseconds());
        CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
        for (int i = 0; i < 1 + rng.Uniform(20); i++) {
          KuduWriteOperation* op;
          bool set_val = true;
          switch (rng.Uniform(6)) {
            case 0: op = table->NewInsert(); break;
            case 1: op = table->NewInsertIgnore(); break;
            case 2: op = table->NewUpsert(); break;
            case 3: op = table->NewUpdateIgnore(); break;
            case 4: op = table->NewUpdate(); break;
            default: op = table->NewDeleteIgnore(); set_val = false; break;
          }
          CHECK_OK(op->mutable_row()->SetInt32("key", rng.Uniform(key_space)));
          if (set_val) {
            if (rng.OneIn(5)) {
              CHECK_OK(op->mutable_row()->SetNull("val"));
            } else {
              CHECK_OK(op->mutable_row()->SetInt32("val", rng.Next()));
            }
          }
          CHECK_OK(session->Apply(op));
        }
        // Rows fail (an INSERT of an existing key), and writes time out while
        // a tablet server is down.
        WARN_NOT_OK(session->Flush(), "write failed");
        num_batches_++;
      }
    });
  }

  void StopWriter() {
    stop_writer_ = true;
    if (writer_.joinable()) {
      writer_.join();
    }
    stop_writer_ = false;
  }

  void WaitForWrites(int n) {
    const int64_t target = num_batches_ + n;
    ASSERT_EVENTUALLY([&] { ASSERT_GE(num_batches_, target); });
  }

  // Moves the leadership of every tablet of the table away from its leader.
  void ChangeLeaders() {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      vector<scoped_refptr<TabletReplica>> replicas;
      cluster_->mini_tablet_server(i)->server()->tablet_manager()->GetTabletReplicas(
          &replicas);
      for (const auto& r : replicas) {
        auto consensus = r->shared_consensus();
        if (r->tablet_metadata()->table_name() == kTableName && consensus &&
            consensus->role() == RaftPeerPB::LEADER) {
          consensus::LeaderStepDownResponsePB resp;
          WARN_NOT_OK(consensus->StepDown(&resp), "step-down failed");
        }
      }
    }
  }

  unique_ptr<CDCClient> cdc_client_;
  unique_ptr<CDCConsumer> consumer_;
  unique_ptr<CDCMirror> mirror_;
  string stream_id_;
  // Whether every value of a record must be decoded.
  bool check_decoded_ = true;

  thread writer_;
  atomic<bool> stop_writer_{false};
  atomic<int64_t> num_batches_{0};

  std::mutex lock_;
  vector<string> errors_;
};

// The consumer's copy equals the table after leader changes, a tablet server
// restart and consumer restarts, for each record type and start mode, with
// failing and IGNORE operations in the mix.
class CDCConsumerFailuresITest
    : public CDCConsumerITest,
      public ::testing::WithParamInterface<
          std::tuple<CDCStreamConfigPB::RecordType, CDCConsumer::StartMode>> {
};

INSTANTIATE_TEST_SUITE_P(
    Params, CDCConsumerFailuresITest,
    ::testing::Combine(::testing::Values(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL),
                       ::testing::Values(CDCConsumer::kSnapshot, CDCConsumer::kEarliest)));

TEST_P(CDCConsumerFailuresITest, CopyEqualsTable) {
  const auto [record_type, start_mode] = GetParam();
  NO_FATALS(StartClusterAndCreateTable(KeyValSchema(), /*num_ranges=*/3));
  // Rows for the snapshot, or for the start of the WAL.
  StartWriter(3 * kRangeWidth);
  NO_FATALS(WaitForWrites(20));
  NO_FATALS(CreateStream(record_type, start_mode == CDCConsumer::kSnapshot
                                          ? CDCStreamConfigPB::INITIAL_AND_CONTINUE
                                          : CDCStreamConfigPB::NEVER));
  // With kEarliest, the consumer reads the WAL from the table's first op,
  // which is still there.
  NO_FATALS(StartConsumer(start_mode));
  NO_FATALS(WaitForWrites(20));
  NO_FATALS(ChangeLeaders());
  NO_FATALS(WaitForWrites(20));
  NO_FATALS(RestartConsumer(start_mode));
  NO_FATALS(WaitForWrites(20));

  LOG(INFO) << "restarting tablet server 1";
  cluster_->mini_tablet_server(1)->Shutdown();
  NO_FATALS(WaitForWrites(10));
  ASSERT_OK(cluster_->mini_tablet_server(1)->Restart());
  NO_FATALS(WaitForWrites(20));
  NO_FATALS(ChangeLeaders());
  NO_FATALS(RestartConsumer(start_mode));
  NO_FATALS(WaitForWrites(20));
  StopWriter();
  NO_FATALS(CheckCopy());
}

// Every column type, with edge values and NULLs, in the snapshot and in the
// stream: the consumer's copy has the bytes the table has.
class CDCConsumerAllTypesITest
    : public CDCConsumerITest,
      public ::testing::WithParamInterface<CDCStreamConfigPB::RecordType> {
};

INSTANTIATE_TEST_SUITE_P(RecordTypes, CDCConsumerAllTypesITest,
                         ::testing::Values(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL));

TEST_P(CDCConsumerAllTypesITest, AllColumnTypes) {
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("b")->Type(KuduColumnSchema::BOOL);
  b.AddColumn("i8")->Type(KuduColumnSchema::INT8);
  b.AddColumn("i16")->Type(KuduColumnSchema::INT16);
  b.AddColumn("i32")->Type(KuduColumnSchema::INT32);
  b.AddColumn("i64")->Type(KuduColumnSchema::INT64);
  b.AddColumn("ts")->Type(KuduColumnSchema::UNIXTIME_MICROS);
  b.AddColumn("dt")->Type(KuduColumnSchema::DATE);
  b.AddColumn("f")->Type(KuduColumnSchema::FLOAT);
  b.AddColumn("d")->Type(KuduColumnSchema::DOUBLE);
  b.AddColumn("dec32")->Type(KuduColumnSchema::DECIMAL)->Precision(9)->Scale(3);
  b.AddColumn("dec64")->Type(KuduColumnSchema::DECIMAL)->Precision(18)->Scale(0);
  b.AddColumn("dec128")->Type(KuduColumnSchema::DECIMAL)->Precision(38)->Scale(38);
  b.AddColumn("s")->Type(KuduColumnSchema::STRING);
  b.AddColumn("vc")->Type(KuduColumnSchema::VARCHAR)->Length(5);
  b.AddColumn("bin")->Type(KuduColumnSchema::BINARY);
  // A column with a default that the writes leave unset.
  b.AddColumn("def")->Type(KuduColumnSchema::INT64)->NotNull()
      ->Default(KuduValue::FromInt(-42));
  ASSERT_OK(b.Build(&schema));
  NO_FATALS(StartClusterAndCreateTable(schema, /*num_ranges=*/1));

  // Sets the columns of 'row' to the 'n'th set of edge values. Set 0 is NULL.
  const auto set_values = [](int n, KuduPartialRow* row) {
    if (n == 0) {
      for (const char* c : { "b", "i8", "i16", "i32", "i64", "ts", "dt", "f", "d", "dec32",
                             "dec64", "dec128", "s", "vc", "bin" }) {
        CHECK_OK(row->SetNull(c));
      }
      return;
    }
    const bool lo = n % 2 == 1;
    // The largest unscaled value of a DECIMAL(38, s): 10^38 - 1.
    const int128_t max38 = kMaxUnscaledDecimal128;
    CHECK_OK(row->SetBool("b", lo));
    CHECK_OK(row->SetInt8("i8", lo ? std::numeric_limits<int8_t>::min()
                                   : std::numeric_limits<int8_t>::max()));
    CHECK_OK(row->SetInt16("i16", lo ? std::numeric_limits<int16_t>::min()
                                     : std::numeric_limits<int16_t>::max()));
    CHECK_OK(row->SetInt32("i32", lo ? std::numeric_limits<int32_t>::min()
                                     : std::numeric_limits<int32_t>::max()));
    CHECK_OK(row->SetInt64("i64", lo ? std::numeric_limits<int64_t>::min()
                                     : std::numeric_limits<int64_t>::max()));
    CHECK_OK(row->SetUnixTimeMicros("ts", lo ? std::numeric_limits<int64_t>::min() : n));
    // The bounds of Kudu's DATE range.
    CHECK_OK(row->SetDate("dt", lo ? -719162 : 2932896));
    CHECK_OK(row->SetFloat("f", lo ? -std::numeric_limits<float>::infinity()
                                   : std::numeric_limits<float>::denorm_min()));
    CHECK_OK(row->SetDouble("d", lo ? std::numeric_limits<double>::lowest() : -0.0));
    CHECK_OK(row->SetUnscaledDecimal("dec32", lo ? -999999999 : 999999999));
    CHECK_OK(row->SetUnscaledDecimal("dec64", lo ? -999999999999999999LL
                                                 : 999999999999999999LL));
    CHECK_OK(row->SetUnscaledDecimal("dec128", lo ? -max38 : max38));
    // A large cell, below --max_cell_size_bytes.
    CHECK_OK(row->SetStringCopy("s", lo ? string() : string(60000, 'x')));
    CHECK_OK(row->SetVarchar("vc", lo ? string() : string("abcde")));
    CHECK_OK(row->SetBinaryCopy("bin", lo ? string("\0\1\0", 3) : string(256, '\xff')));
  };
  const auto write = [&](bool insert, int first, int count, int values) {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int k = first; k < first + count; k++) {
      KuduWriteOperation* op =
          insert ? static_cast<KuduWriteOperation*>(table->NewInsert()) : table->NewUpdate();
      ASSERT_OK(op->mutable_row()->SetInt32("key", k));
      set_values((k + values) % 3, op->mutable_row());
      ASSERT_OK(session->Apply(op));
    }
    ASSERT_OK(session->Flush());
  };

  NO_FATALS(write(/*insert=*/true, 0, 6, 0));
  NO_FATALS(CreateStream(GetParam(), CDCStreamConfigPB::INITIAL_AND_CONTINUE));
  NO_FATALS(StartConsumer(CDCConsumer::kSnapshot));
  NO_FATALS(write(/*insert=*/true, 6, 6, 0));
  NO_FATALS(write(/*insert=*/false, 0, 12, 1));
  NO_FATALS(CheckCopy());
  NO_FATALS(write(/*insert=*/false, 0, 12, 2));
  {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int k = 0; k < 12; k += 3) {
      KuduWriteOperation* op = table->NewDelete();
      ASSERT_OK(op->mutable_row()->SetInt32("key", k));
      ASSERT_OK(session->Apply(op));
    }
    ASSERT_OK(session->Flush());
  }
  NO_FATALS(CheckCopy());
  if (GetParam() == CDCStreamConfigPB::FULL) {
    EXPECT_GT(mirror_->num_before_images_checked(), 0);
  }
}

// Array columns, NULL, empty, with NULL elements and with values, in the
// snapshot and in the stream: the consumer's copy has the arrays the table has.
TEST_P(CDCConsumerAllTypesITest, ArrayColumns) {
  const auto add_array = [](KuduSchemaBuilder* b, const string& name,
                            KuduColumnSchema::DataType elem_type) {
    return b->AddColumn(name)->NestedType(KuduColumnSchema::KuduNestedTypeDescriptor(
        KuduColumnSchema::KuduArrayTypeDescriptor(elem_type)));
  };
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  add_array(&b, "a_b", KuduColumnSchema::BOOL);
  add_array(&b, "a_i32", KuduColumnSchema::INT32);
  add_array(&b, "a_i64", KuduColumnSchema::INT64);
  add_array(&b, "a_d", KuduColumnSchema::DOUBLE);
  add_array(&b, "a_dec", KuduColumnSchema::DECIMAL)->Precision(18)->Scale(2);
  add_array(&b, "a_s", KuduColumnSchema::STRING);
  add_array(&b, "a_bin", KuduColumnSchema::BINARY);
  ASSERT_OK(b.Build(&schema));
  NO_FATALS(StartClusterAndCreateTable(schema, /*num_ranges=*/1));

  // Sets the columns of 'row' to the 'n'th set of arrays: NULL, empty, with
  // NULL elements, or with values.
  const auto set_values = [](int n, KuduPartialRow* row) {
    static const char* const kColumns[] = { "a_b", "a_i32", "a_i64", "a_d", "a_dec", "a_s",
                                            "a_bin" };
    switch (n) {
      case 0:
        for (const char* c : kColumns) {
          CHECK_OK(row->SetNull(c));
        }
        return;
      case 1:
        CHECK_OK(row->SetArrayBool("a_b", {}, {}));
        CHECK_OK(row->SetArrayInt32("a_i32", {}, {}));
        CHECK_OK(row->SetArrayInt64("a_i64", {}, {}));
        CHECK_OK(row->SetArrayDouble("a_d", {}, {}));
        CHECK_OK(row->SetArrayUnscaledDecimal("a_dec", vector<int64_t>(), {}));
        CHECK_OK(row->SetArrayString("a_s", {}, {}));
        CHECK_OK(row->SetArrayBinary("a_bin", {}, {}));
        return;
      case 2: {
        const vector<bool> validity = { false, true, false };
        CHECK_OK(row->SetArrayBool("a_b", { false, true, false }, validity));
        CHECK_OK(row->SetArrayInt32("a_i32", { 0, -7, 0 }, validity));
        CHECK_OK(row->SetArrayInt64("a_i64", { 0, std::numeric_limits<int64_t>::min(), 0 },
                                    validity));
        CHECK_OK(row->SetArrayDouble("a_d", { 0, -0.0, 0 }, validity));
        CHECK_OK(row->SetArrayUnscaledDecimal("a_dec", vector<int64_t>{ 0, -12345, 0 },
                                              validity));
        CHECK_OK(row->SetArrayString("a_s", { Slice(), Slice("x"), Slice() }, validity));
        CHECK_OK(row->SetArrayBinary("a_bin", { Slice(), Slice("\0\1", 2), Slice() },
                                     validity));
        return;
      }
      default: {
        static const string kBig(4096, 'y');
        CHECK_OK(row->SetArrayBool("a_b", { true, false }, {}));
        CHECK_OK(row->SetArrayInt32("a_i32", { std::numeric_limits<int32_t>::max(), 1, 2 },
                                    {}));
        CHECK_OK(row->SetArrayInt64("a_i64", { std::numeric_limits<int64_t>::max() }, {}));
        CHECK_OK(row->SetArrayDouble("a_d", { std::numeric_limits<double>::lowest(), 1.5 },
                                     {}));
        CHECK_OK(row->SetArrayUnscaledDecimal(
            "a_dec", vector<int64_t>{ 999999999999999999LL, -999999999999999999LL },
            { true, true }));
        CHECK_OK(row->SetArrayString("a_s", { Slice(""), Slice(kBig), Slice("z") }, {}));
        CHECK_OK(row->SetArrayBinary("a_bin", { Slice("\xff", 1) }, {}));
        return;
      }
    }
  };
  const auto write = [&](bool insert, int first, int count, int values) {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int k = first; k < first + count; k++) {
      KuduWriteOperation* op =
          insert ? static_cast<KuduWriteOperation*>(table->NewInsert()) : table->NewUpdate();
      ASSERT_OK(op->mutable_row()->SetInt32("key", k));
      set_values((k + values) % 4, op->mutable_row());
      ASSERT_OK(session->Apply(op));
    }
    ASSERT_OK(session->Flush());
  };

  NO_FATALS(write(/*insert=*/true, 0, 8, 0));
  NO_FATALS(CreateStream(GetParam(), CDCStreamConfigPB::INITIAL_AND_CONTINUE));
  NO_FATALS(StartConsumer(CDCConsumer::kSnapshot));
  NO_FATALS(write(/*insert=*/true, 8, 8, 0));
  NO_FATALS(write(/*insert=*/false, 0, 16, 1));
  NO_FATALS(CheckCopy());
  // The before-images of these updates come from rowsets that were flushed.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    vector<scoped_refptr<TabletReplica>> replicas;
    cluster_->mini_tablet_server(i)->server()->tablet_manager()->GetTabletReplicas(&replicas);
    for (const auto& r : replicas) {
      ASSERT_OK(r->tablet()->Flush());
    }
  }
  NO_FATALS(write(/*insert=*/false, 0, 16, 2));
  {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int k = 0; k < 16; k += 3) {
      KuduWriteOperation* op = table->NewDelete();
      ASSERT_OK(op->mutable_row()->SetInt32("key", k));
      ASSERT_OK(session->Apply(op));
    }
    ASSERT_OK(session->Flush());
  }
  NO_FATALS(CheckCopy());
  if (GetParam() == CDCStreamConfigPB::FULL) {
    EXPECT_GT(mirror_->num_before_images_checked(), 0);
  }
}

// The kinds of schema change of CDCConsumerSchemaITest.
enum class AlterKind {
  kAddNullable,
  kAddWithDefault,
  kDrop,
  kRename,
  kDropAndAddAgain,
  kChangeDefault,
};

const char* AlterKindToString(AlterKind k) {
  switch (k) {
    case AlterKind::kAddNullable: return "AddNullable";
    case AlterKind::kAddWithDefault: return "AddWithDefault";
    case AlterKind::kDrop: return "Drop";
    case AlterKind::kRename: return "Rename";
    case AlterKind::kDropAndAddAgain: return "DropAndAddAgain";
    case AlterKind::kChangeDefault: return "ChangeDefault";
  }
  return "?";
}

// A schema change of a column 'c' while the table is written and consumed:
// the consumer's copy equals the table, with the values of the rows written
// before, during and after the change.
class CDCConsumerSchemaITest
    : public CDCConsumerITest,
      public ::testing::WithParamInterface<
          std::tuple<AlterKind, CDCStreamConfigPB::RecordType, int>> {
 protected:
  // Writes rows until StopWriter(): inserts of new keys, and updates and
  // deletes of the written keys, setting 'c' (by its current name) if the
  // table has it.
  void StartSchemaWriter() {
    writer_ = thread([this] {
      Random rng(SeedRandom());
      int next_key = 0;
      while (!stop_writer_) {
        client::sp::shared_ptr<KuduTable> table;
        if (!client_->OpenTable(kTableName, &table).ok()) continue;
        string c;
        for (int i = 0; i < table->schema().num_columns(); i++) {
          const string name = table->schema().Column(i).name();
          if (name[0] == 'c') c = name;
        }
        client::sp::shared_ptr<KuduSession> session = client_->NewSession();
        session->SetTimeoutMillis(kTimeout.ToMilliseconds());
        CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
        for (int i = 0; i < 10; i++) {
          KuduWriteOperation* op;
          int key;
          const int kind = next_key == 0 ? 0 : rng.Uniform(4);
          if (kind < 2) {
            op = table->NewInsert();
            key = next_key++ % (num_ranges_ * kRangeWidth);
          } else if (kind == 2) {
            op = table->NewUpdate();
            key = rng.Uniform(next_key) % (num_ranges_ * kRangeWidth);
          } else {
            op = table->NewDeleteIgnore();
            key = rng.Uniform(next_key) % (num_ranges_ * kRangeWidth);
          }
          CHECK_OK(op->mutable_row()->SetInt32("key", key));
          if (kind < 3) {
            CHECK_OK(op->mutable_row()->SetInt32("val", rng.Next()));
            // Leave 'c' unset in some inserts, so they get its default.
            if (!c.empty() && !rng.OneIn(3)) {
              CHECK_OK(op->mutable_row()->SetInt32(c, rng.Next()));
            }
          }
          CHECK_OK(session->Apply(op));
        }
        // Writes with the old schema fail during the change.
        WARN_NOT_OK(session->Flush(), "write failed");
        num_batches_++;
      }
    });
  }

  int num_ranges_ = 1;
};

INSTANTIATE_TEST_SUITE_P(
    Params, CDCConsumerSchemaITest,
    ::testing::Combine(::testing::Values(AlterKind::kAddNullable, AlterKind::kAddWithDefault,
                                         AlterKind::kDrop, AlterKind::kRename,
                                         AlterKind::kDropAndAddAgain,
                                         AlterKind::kChangeDefault),
                       ::testing::Values(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::FULL),
                       ::testing::Values(1, 3)),
    [](const ::testing::TestParamInfo<CDCConsumerSchemaITest::ParamType>& info) {
      return Substitute("$0_$1_$2Tablets", AlterKindToString(std::get<0>(info.param)),
                        CDCStreamConfigPB::RecordType_Name(std::get<1>(info.param)),
                        std::get<2>(info.param));
    });

TEST_P(CDCConsumerSchemaITest, CopyEqualsTableAcrossAlter) {
  const auto [kind, record_type, num_ranges] = GetParam();
  num_ranges_ = num_ranges;
  // The image of a FULL record can have a column that was added after the
  // consumer learned the table's schema, whose value is then delivered raw
  // (see CDCDecodedColumn::decode_ok).
  check_decoded_ = record_type != CDCStreamConfigPB::FULL;
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
  b.AddColumn("val")->Type(KuduColumnSchema::INT32);
  const bool has_c = kind != AlterKind::kAddNullable && kind != AlterKind::kAddWithDefault;
  if (has_c) {
    b.AddColumn("c")->Type(KuduColumnSchema::INT32)->Default(KuduValue::FromInt(7));
  }
  ASSERT_OK(b.Build(&schema));
  NO_FATALS(StartClusterAndCreateTable(schema, num_ranges));
  NO_FATALS(CreateStream(record_type, CDCStreamConfigPB::NEVER));
  NO_FATALS(StartConsumer(CDCConsumer::kEarliest));
  StartSchemaWriter();
  NO_FATALS(WaitForWrites(10));

  unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
  switch (kind) {
    case AlterKind::kAddNullable:
      alterer->AddColumn("c")->Type(KuduColumnSchema::INT32);
      break;
    case AlterKind::kAddWithDefault:
      alterer->AddColumn("c")->Type(KuduColumnSchema::INT32)->NotNull()
          ->Default(KuduValue::FromInt(99));
      break;
    case AlterKind::kDrop:
    case AlterKind::kDropAndAddAgain:
      alterer->DropColumn("c");
      break;
    case AlterKind::kRename:
      alterer->AlterColumn("c")->RenameTo("c2");
      break;
    case AlterKind::kChangeDefault:
      alterer->AlterColumn("c")->Default(KuduValue::FromInt(8));
      break;
  }
  ASSERT_OK(alterer->Alter());
  NO_FATALS(WaitForWrites(10));
  if (kind == AlterKind::kDropAndAddAgain) {
    // A new column with the same name.
    unique_ptr<KuduTableAlterer> again(client_->NewTableAlterer(kTableName));
    again->AddColumn("c")->Type(KuduColumnSchema::INT32)->Default(KuduValue::FromInt(5));
    ASSERT_OK(again->Alter());
    NO_FATALS(WaitForWrites(10));
  }
  StopWriter();
  NO_FATALS(CheckCopy());
}

// Range partitions that are added, dropped with their rows, and added again,
// for each start mode, with a consumer that runs throughout or that is
// stopped while the partitions change.
class CDCConsumerPartitionITest
    : public CDCConsumerITest,
      public ::testing::WithParamInterface<std::tuple<CDCConsumer::StartMode, bool>> {
 protected:
  void AlterRange(bool add, int lower) {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
    unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
    ASSERT_OK(lb->SetInt32("key", lower));
    ASSERT_OK(ub->SetInt32("key", lower + kRangeWidth));
    if (add) {
      alterer->AddRangePartition(lb.release(), ub.release());
    } else {
      alterer->DropRangePartition(lb.release(), ub.release());
    }
    ASSERT_OK(alterer->Alter());
  }

  void Upsert(int first, int count) {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int k = first; k < first + count; k++) {
      KuduWriteOperation* op = table->NewUpsert();
      ASSERT_OK(op->mutable_row()->SetInt32("key", k));
      ASSERT_OK(op->mutable_row()->SetInt32("val", k + num_batches_));
      ASSERT_OK(session->Apply(op));
    }
    ASSERT_OK(session->Flush());
    num_batches_++;
  }
};

INSTANTIATE_TEST_SUITE_P(
    Params, CDCConsumerPartitionITest,
    ::testing::Combine(::testing::Values(CDCConsumer::kNow, CDCConsumer::kEarliest,
                                         CDCConsumer::kSnapshot),
                       ::testing::Bool()));

TEST_P(CDCConsumerPartitionITest, CopyEqualsTableAcrossRangeChanges) {
  const auto [start_mode, restart] = GetParam();
  NO_FATALS(StartClusterAndCreateTable(KeyValSchema(), /*num_ranges=*/1));
  NO_FATALS(CreateStream(CDCStreamConfigPB::CHANGE,
                         start_mode == CDCConsumer::kSnapshot
                             ? CDCStreamConfigPB::INITIAL_AND_CONTINUE
                             : CDCStreamConfigPB::NEVER));
  NO_FATALS(StartConsumer(start_mode));
  NO_FATALS(Upsert(0, 10));
  NO_FATALS(CheckCopy());
  if (restart) {
    // A kNow consumer that finds no durable checkpoint starts at the tail, so
    // wait until the master has the checkpoints (Flush() does not).
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(consumer_->Flush());
      vector<CDCTabletProgress> progress;
      consumer_->GetProgress(&progress);
      cdc::CDCStreamInfo info;
      ASSERT_OK(cdc_client_->GetStreamInfo(stream_id_, &info));
      for (const auto& p : progress) {
        ASSERT_GE(FindWithDefault(info.tablet_checkpoints, p.tablet_id, -1),
                  p.last_checkpointed_op_index) << p.tablet_id;
      }
    });
    consumer_->Stop();
    consumer_.reset();
  }
  NO_FATALS(AlterRange(/*add=*/true, kRangeWidth));
  NO_FATALS(Upsert(kRangeWidth, 10));
  NO_FATALS(AlterRange(/*add=*/false, kRangeWidth));
  NO_FATALS(AlterRange(/*add=*/true, kRangeWidth));
  NO_FATALS(Upsert(kRangeWidth + 5, 10));
  NO_FATALS(Upsert(0, 5));
  if (restart) {
    NO_FATALS(StartConsumer(start_mode));
  }
  NO_FATALS(CheckCopy());
}

// A response without records that says that more records follow (the read
// window had only failed writes) is followed by the next read right away,
// without the idle backoff.
TEST_F(CDCConsumerITest, HaveMoreRecordsSkipsBackoff) {
  NO_FATALS(StartClusterAndCreateTable(KeyValSchema(), /*num_ranges=*/1));
  NO_FATALS(CreateStream(CDCStreamConfigPB::CHANGE, CDCStreamConfigPB::NEVER));
  client::sp::shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  client::sp::shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
  {
    KuduWriteOperation* op = table->NewInsert();
    ASSERT_OK(op->mutable_row()->SetInt32("key", 0));
    ASSERT_OK(session->Apply(op));
  }
  // Ops whose rows all fail: none produces a record.
  constexpr int kFailedOps = 30;
  for (int i = 0; i < kFailedOps; i++) {
    KuduWriteOperation* op = table->NewInsert();
    ASSERT_OK(op->mutable_row()->SetInt32("key", 0));
    ASSERT_FALSE(session->Apply(op).ok());
  }
  {
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    KuduWriteOperation* op = table->NewInsert();
    ASSERT_OK(op->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(session->Apply(op));
  }
  // One op per response, and an idle backoff that would make the failed ops
  // take minutes.
  CDCConsumer::Options opts;
  opts.stream_id = stream_id_;
  opts.start_mode = CDCConsumer::kEarliest;
  opts.min_poll_backoff = MonoDelta::FromSeconds(5);
  opts.max_poll_backoff = MonoDelta::FromSeconds(5);
  opts.max_bytes_per_response = 1;
  ASSERT_OK(CDCConsumer::Create(cdc_client_.get(), std::move(opts), &consumer_));
  ASSERT_OK(consumer_->Start([this](const CDCRecordBatch& batch) {
    mirror_->Apply(batch);
    return Status::OK();
  }));
  const MonoTime start = MonoTime::Now();
  ASSERT_EVENTUALLY([&] { ASSERT_EQ(2, mirror_->rows().size()); });
  EXPECT_LT(MonoTime::Now() - start, MonoDelta::FromSeconds(20));
}

} // namespace kudu
