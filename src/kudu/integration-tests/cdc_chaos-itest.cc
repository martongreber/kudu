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

// A randomized workload, chaos and oracle test of CDC: writers apply a random
// mix of operations (including failing and IGNORE operations, transactions
// that commit or abort, and batches larger than a GetChanges response) to a
// table with every scalar column type, while the table's schema and range
// partitions change, and tablet servers and masters are killed, paused and
// made to step down. A CDCConsumer replays the table's CDC stream into an
// in-memory copy (CDCMirror), and is itself restarted from its durable
// checkpoint now and then. At the end, the copy must equal a scan of the
// table, FULL before-images must have matched the copy, and once the stream
// is deleted no replica in a tablet's Raft config retains WAL for it.
//
// The test runs for --cdc_chaos_duration_secs (by default 20 seconds, or 60
// seconds with KUDU_ALLOW_SLOW_TESTS; a nightly run should use more) and logs
// its random seed; rerun with
// --test_random_seed to replay a seed's choices (the interleaving with the
// cluster's threads is not reproducible).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cdc_mirror.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/int128.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(cdc_tablet_rescan_interval_ms);

DEFINE_int32(cdc_chaos_duration_secs, 0,
             "How long the workload and the chaos run, in seconds. 0 means 20 "
             "seconds, or 60 seconds if slow tests are allowed.");

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_gauge_int64(cdc_wal_retained_bytes);

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamOptions;
using kudu::cdc::CDCTableMetadata;
using kudu::cdc::CDCTabletProgress;
using kudu::cdc::CDCTabletState;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduTransaction;
using kudu::client::KuduWriteOperation;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::itest::CDCMirror;
using kudu::itest::MiniClusterFsInspector;
using kudu::master::CDCStreamConfigPB;
using std::atomic;
using std::map;
using std::set;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {

constexpr const char* kTableName = "cdc_chaos";
constexpr int kNumMasters = 3;
constexpr int kNumTabletServers = 5;
constexpr int kNumWriters = 3;
// Non-transactional writers use the keys [0, kKeySpace), split into two range
// partitions, so that they collide.
constexpr int64_t kKeySpace = 2000;
// Each transaction inserts new keys from [kTxnKeyBase, kTxnKeyBase + kRange):
// a transactional row is never inserted by another writer.
constexpr int64_t kTxnKeyBase = 1000000;
// The range partitions added and dropped by the test start here, each
// kExtraRangeWidth wide.
constexpr int64_t kExtraRangeBase = 3000000;
constexpr int64_t kExtraRangeWidth = 1000;
constexpr int64_t kRange = 1000000;
// A GetChanges response has at most this many records, so that many ops and
// transactions are larger than one response.
constexpr int kMaxRecordsPerResponse = 16;
const MonoDelta kTimeout = MonoDelta::FromSeconds(60);

// Counts of what the workload and the chaos did, for the log.
struct Stats {
  atomic<int64_t> ops{0};
  atomic<int64_t> row_errors{0};
  atomic<int64_t> flush_errors{0};
  atomic<int64_t> txns_committed{0};
  atomic<int64_t> txns_aborted{0};
  atomic<int64_t> txns_failed{0};
  atomic<int64_t> ddls{0};
  atomic<int64_t> chaos_events{0};
  atomic<int64_t> consumer_restarts{0};
};

} // anonymous namespace

class CDCChaosITest : public KuduTest,
                      public ::testing::WithParamInterface<CDCStreamConfigPB::RecordType> {
 protected:
  void SetUp() override {
    KuduTest::SetUp();
    // Discover the added range partitions quickly.
    FLAGS_cdc_tablet_rescan_interval_ms = 1000;

    ExternalMiniClusterOptions opts;
    opts.num_masters = kNumMasters;
    opts.num_tablet_servers = kNumTabletServers;
    opts.extra_master_flags = {
      "--cdc_enabled=true",
      "--cdc_bg_scan_interval_ms=500",
      "--catalog_manager_bg_task_wait_ms=200",
      "--txn_manager_enabled=true",
      "--txn_manager_lazily_initialized=false",
      "--txn_manager_status_table_num_replicas=3",
    };
    opts.extra_tserver_flags = {
      "--enable_txn_system_client_init=true",
      Substitute("--cdc_max_records_per_response=$0", kMaxRecordsPerResponse),
      "--cdc_checkpoint_persist_interval_ms=500",
      "--cdc_active_time_report_interval_ms=1000",
      "--cdc_checkpoint_flush_interval_ms=200",
      // Small WAL segments that are rolled and GCed often, so the CDC
      // retention barrier is what keeps the stream's WAL.
      "--log_segment_size_mb=1",
      "--log_min_segments_to_retain=1",
      "--flush_threshold_mb=1",
      "--flush_threshold_secs=2",
      "--maintenance_manager_polling_interval_ms=100",
      // Re-replicate a replica that is down for a few seconds, so tablets are
      // copied while the stream is consumed.
      "--follower_unavailable_considered_failed_sec=4",
      "--consensus_rpc_timeout_ms=3000",
    };
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
    int leader_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
    ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy(leader_idx),
                                           cluster_->messenger(), &ts_map_));
    NO_FATALS(CreateTable());
  }

  void TearDown() override {
    if (cluster_) {
      // A failed test may have left servers paused.
      for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
        if (cluster_->tablet_server(i)->IsProcessAlive()) {
          WARN_NOT_OK(cluster_->tablet_server(i)->Resume(), "could not resume tserver");
        }
      }
      for (int i = 0; i < cluster_->num_masters(); i++) {
        if (cluster_->master(i)->IsProcessAlive()) {
          WARN_NOT_OK(cluster_->master(i)->Resume(), "could not resume master");
        }
      }
    }
    if (consumer_) {
      consumer_->Stop();
      consumer_.reset();
    }
    cdc_client_.reset();
    STLDeleteValues(&ts_map_);
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  void CreateTable() {
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT64)->NotNull()->PrimaryKey();
    b.AddColumn("b")->Type(KuduColumnSchema::BOOL);
    b.AddColumn("i8")->Type(KuduColumnSchema::INT8);
    b.AddColumn("i16")->Type(KuduColumnSchema::INT16);
    b.AddColumn("i32")->Type(KuduColumnSchema::INT32);
    b.AddColumn("i64")->Type(KuduColumnSchema::INT64);
    b.AddColumn("ts")->Type(KuduColumnSchema::UNIXTIME_MICROS);
    b.AddColumn("dt")->Type(KuduColumnSchema::DATE);
    b.AddColumn("f")->Type(KuduColumnSchema::FLOAT);
    b.AddColumn("d")->Type(KuduColumnSchema::DOUBLE);
    b.AddColumn("dec32")->Type(KuduColumnSchema::DECIMAL)->Precision(9)->Scale(2);
    b.AddColumn("dec64")->Type(KuduColumnSchema::DECIMAL)->Precision(18)->Scale(6);
    b.AddColumn("dec128")->Type(KuduColumnSchema::DECIMAL)->Precision(38)->Scale(10);
    b.AddColumn("s")->Type(KuduColumnSchema::STRING);
    b.AddColumn("vc")->Type(KuduColumnSchema::VARCHAR)->Length(16);
    b.AddColumn("bin")->Type(KuduColumnSchema::BINARY);
    ASSERT_OK(b.Build(&schema));

    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    creator->table_name(kTableName)
        .schema(&schema)
        .add_hash_partitions({ "key" }, 2)
        .set_range_partition_columns({ "key" })
        .num_replicas(3);
    for (const auto& [lower, upper] : vector<std::pair<int64_t, int64_t>>{
             { 0, kKeySpace / 2 }, { kKeySpace / 2, kKeySpace },
             { kTxnKeyBase, kTxnKeyBase + kRange } }) {
      unique_ptr<KuduPartialRow> lb(schema.NewRow());
      unique_ptr<KuduPartialRow> ub(schema.NewRow());
      ASSERT_OK(lb->SetInt64("key", lower));
      ASSERT_OK(ub->SetInt64("key", upper));
      creator->add_range_partition(lb.release(), ub.release());
    }
    ASSERT_OK(creator->timeout(kTimeout).Create());
  }

  // Returns 'hi' * 10^17 + 'lo', which for |hi| < 10^18 and |lo| < 10 fits a
  // DECIMAL(38). Not instrumented: the int128 overflow check calls
  // __muloti4, which the UBSAN builds do not link.
  ATTRIBUTE_NO_SANITIZE_INTEGER
  static int128_t Decimal128FromParts(int64_t hi, int64_t lo) {
    return static_cast<int128_t>(hi) * 100000000000000000LL + lo;
  }

  // Sets column 'idx' of 'row' (of 'schema') to a random value of its type,
  // or to NULL.
  static void SetRandomValue(const KuduSchema& schema, int idx, Random* rng,
                             KuduPartialRow* row) {
    if (rng->OneIn(5)) {
      CHECK_OK(row->SetNull(idx));
      return;
    }
    const KuduColumnSchema col = schema.Column(idx);
    const int64_t r = static_cast<int64_t>(rng->Next64());
    switch (col.type()) {
      case KuduColumnSchema::BOOL:
        CHECK_OK(row->SetBool(idx, rng->OneIn(2)));
        break;
      case KuduColumnSchema::INT8:
        CHECK_OK(row->SetInt8(idx, static_cast<int8_t>(r)));
        break;
      case KuduColumnSchema::INT16:
        CHECK_OK(row->SetInt16(idx, static_cast<int16_t>(r)));
        break;
      case KuduColumnSchema::INT32:
        CHECK_OK(row->SetInt32(idx, static_cast<int32_t>(r)));
        break;
      case KuduColumnSchema::INT64:
        CHECK_OK(row->SetInt64(idx, r));
        break;
      case KuduColumnSchema::UNIXTIME_MICROS:
        CHECK_OK(row->SetUnixTimeMicros(idx, r));
        break;
      case KuduColumnSchema::DATE:
        // Kudu's valid DATE range is about [-719162, 2932896] days.
        CHECK_OK(row->SetDate(idx, static_cast<int32_t>(rng->Uniform(2932896))));
        break;
      case KuduColumnSchema::FLOAT:
        CHECK_OK(row->SetFloat(idx, static_cast<float>(r % 1000000) / 7));
        break;
      case KuduColumnSchema::DOUBLE:
        CHECK_OK(row->SetDouble(idx, static_cast<double>(r) / 3));
        break;
      case KuduColumnSchema::DECIMAL: {
        int128_t v;
        const int precision = col.type_attributes().precision();
        if (precision <= 9) {
          v = r % 1000000000;
        } else if (precision <= 18) {
          v = r % 1000000000000000000LL;
        } else {
          v = Decimal128FromParts(r % 1000000000000000000LL, r % 7);
        }
        CHECK_OK(row->SetUnscaledDecimal(idx, v));
        break;
      }
      case KuduColumnSchema::STRING: {
        // Now and then a large cell.
        const int len = rng->OneIn(50) ? 4000 + rng->Uniform(4000) : rng->Uniform(40);
        CHECK_OK(row->SetStringCopy(idx, RandomString(len, rng)));
        break;
      }
      case KuduColumnSchema::VARCHAR:
        CHECK_OK(row->SetVarchar(idx, RandomString(rng->Uniform(17), rng)));
        break;
      case KuduColumnSchema::BINARY: {
        string v(rng->Uniform(30), '\0');
        for (auto& c : v) {
          c = static_cast<char>(rng->Next());
        }
        CHECK_OK(row->SetBinaryCopy(idx, v));
        break;
      }
      default:
        LOG(FATAL) << "unexpected column type " << col.type();
    }
  }

  static string RandomString(int len, Random* rng) {
    string s(len, 'a');
    for (auto& c : s) {
      c = static_cast<char>('a' + rng->Uniform(26));
    }
    return s;
  }

  // Sets the non-key columns of 'row' to random values: all of them, or with
  // 'some', a random non-empty subset.
  static void SetRandomValues(const KuduSchema& schema, bool some, Random* rng,
                              KuduPartialRow* row) {
    bool any = false;
    for (int i = 1; i < schema.num_columns(); i++) {
      if (some && !rng->OneIn(3)) continue;
      SetRandomValue(schema, i, rng, row);
      any = true;
    }
    if (!any) {
      SetRandomValue(schema, 1 + rng->Uniform(schema.num_columns() - 1), rng, row);
    }
  }

  // A random key for a non-transactional write: mostly from the colliding key
  // space, sometimes a committed transactional row or a row of an added
  // range partition.
  int64_t RandomKey(Random* rng) {
    if (rng->OneIn(10)) {
      std::lock_guard<std::mutex> l(lock_);
      if (!txn_keys_.empty()) {
        return txn_keys_[rng->Uniform(txn_keys_.size())];
      }
    }
    if (rng->OneIn(5)) {
      std::lock_guard<std::mutex> l(lock_);
      if (!extra_ranges_.empty()) {
        auto it = extra_ranges_.begin();
        std::advance(it, rng->Uniform(extra_ranges_.size()));
        return *it + rng->Uniform(50);
      }
    }
    return rng->Uniform(kKeySpace);
  }

  // Returns a new random write operation of 'table'.
  KuduWriteOperation* RandomOp(KuduTable* table, Random* rng) {
    const KuduSchema& schema = table->schema();
    const int64_t key = RandomKey(rng);
    KuduWriteOperation* op;
    bool full_row = false;
    bool some_values = false;
    switch (rng->Uniform(20)) {
      case 0: case 1: case 2: case 3: case 4:
        op = table->NewInsert();
        full_row = true;
        break;
      case 5: case 6:
        op = table->NewInsertIgnore();
        full_row = true;
        break;
      case 7: case 8: case 9:
        op = table->NewUpsert();
        some_values = true;
        break;
      case 10:
        op = table->NewUpsertIgnore();
        some_values = true;
        break;
      case 11: case 12: case 13: case 14:
        op = table->NewUpdate();
        some_values = true;
        break;
      case 15:
        op = table->NewUpdateIgnore();
        some_values = true;
        break;
      case 16: case 17:
        op = table->NewDelete();
        break;
      default:
        op = table->NewDeleteIgnore();
        break;
    }
    CHECK_OK(op->mutable_row()->SetInt64("key", key));
    if (full_row || some_values) {
      SetRandomValues(schema, some_values, rng, op->mutable_row());
    }
    return op;
  }

  Status OpenTable(client::sp::shared_ptr<KuduTable>* table) {
    return client_->OpenTable(kTableName, table);
  }

  // Flushes 'session' and counts its errors, which are expected: ops that
  // fail (an INSERT of an existing row, a write to a dropped partition),
  // timeouts under chaos, and so on.
  void FlushAndCountErrors(KuduSession* session) {
    const Status s = session->Flush();
    if (!s.ok()) {
      stats_.flush_errors++;
    }
    vector<KuduError*> errors;
    ElementDeleter d(&errors);
    session->GetPendingErrors(&errors, nullptr);
    stats_.row_errors += errors.size();
  }

  void WriterThread(int idx) {
    Random rng(seed_ + idx);
    client::sp::shared_ptr<KuduTable> table;
    int batches = 0;
    while (!stop_writers_) {
      if (!table || batches++ % 20 == 0) {
        if (!OpenTable(&table).ok()) {
          SleepFor(MonoDelta::FromMilliseconds(100));
          continue;
        }
      }
      client::sp::shared_ptr<KuduSession> session = client_->NewSession();
      session->SetTimeoutMillis(10000);
      CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
      // Now and then a batch larger than a GetChanges response.
      const int n = rng.OneIn(20) ? 50 + rng.Uniform(100) : 1 + rng.Uniform(20);
      for (int i = 0; i < n; i++) {
        WARN_NOT_OK(session->Apply(RandomOp(table.get(), &rng)), "apply failed");
        stats_.ops++;
      }
      FlushAndCountErrors(session.get());
    }
  }

  // Inserts new rows into the transactional key range in transactions that
  // span tablets, and commits or rolls back each of them.
  void TxnWriterThread() {
    Random rng(seed_ + 100);
    int64_t next_key = kTxnKeyBase;
    while (!stop_writers_) {
      // Let a column DDL in between two transactions.
      SleepFor(MonoDelta::FromMilliseconds(20));
      std::lock_guard<std::timed_mutex> txn_lock(no_txn_lock_);
      client::sp::shared_ptr<KuduTable> table;
      if (!OpenTable(&table).ok()) {
        SleepFor(MonoDelta::FromMilliseconds(100));
        continue;
      }
      client::sp::shared_ptr<KuduTransaction> txn;
      if (!client_->NewTransaction(&txn).ok()) {
        stats_.txns_failed++;
        SleepFor(MonoDelta::FromMilliseconds(100));
        continue;
      }
      client::sp::shared_ptr<KuduSession> session;
      CHECK_OK(txn->CreateSession(&session));
      session->SetTimeoutMillis(10000);
      CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
      vector<int64_t> keys;
      const int n = 1 + rng.Uniform(rng.OneIn(5) ? 60 : 10);
      for (int i = 0; i < n; i++) {
        // Spread the rows over both hash buckets; now and then insert a key
        // of the transaction again, which an INSERT_IGNORE skips.
        const bool dup = !keys.empty() && rng.OneIn(8);
        const int64_t key = dup ? keys[rng.Uniform(keys.size())] : next_key++;
        KuduWriteOperation* op = dup ? static_cast<KuduWriteOperation*>(table->NewInsertIgnore())
                                     : table->NewInsert();
        CHECK_OK(op->mutable_row()->SetInt64("key", key));
        SetRandomValues(table->schema(), /*some=*/false, &rng, op->mutable_row());
        WARN_NOT_OK(session->Apply(op), "apply failed");
        if (!dup) keys.push_back(key);
        stats_.ops++;
      }
      FlushAndCountErrors(session.get());
      if (rng.OneIn(3)) {
        WARN_NOT_OK(txn->Rollback(), "rollback failed");
        stats_.txns_aborted++;
        continue;
      }
      if (txn->Commit().ok()) {
        stats_.txns_committed++;
        std::lock_guard<std::mutex> l(lock_);
        txn_keys_.insert(txn_keys_.end(), keys.begin(), keys.end());
      } else {
        // A transaction that is not rolled back holds its partition locks
        // until it is aborted for lack of keepalive.
        WARN_NOT_OK(txn->Rollback(), "rollback failed");
        stats_.txns_failed++;
      }
    }
  }

  // Alters the table's schema or range partitions at random.
  Status RandomDdl(Random* rng) {
    client::sp::shared_ptr<KuduTable> table;
    RETURN_NOT_OK(OpenTable(&table));
    unique_ptr<KuduTableAlterer> alterer(client_->NewTableAlterer(kTableName));
    alterer->timeout(kTimeout);
    vector<string> extra_cols;
    for (int i = 0; i < table->schema().num_columns(); i++) {
      const string name = table->schema().Column(i).name();
      if (name[0] == 'x') extra_cols.push_back(name);
    }
    const int action = rng->Uniform(5);
    // A column DDL waits for the open transaction, see 'no_txn_lock_'.
    std::unique_lock<std::timed_mutex> txn_lock(no_txn_lock_, std::defer_lock);
    if (action >= 2 && !txn_lock.try_lock_for(std::chrono::seconds(5))) {
      return Status::OK();
    }
    std::unique_lock<std::mutex> l(lock_);
    switch (action) {
      case 0:
      case 1: {
        if (extra_ranges_.size() >= 3) {
          // Drop a range partition.
          auto it = extra_ranges_.begin();
          std::advance(it, rng->Uniform(extra_ranges_.size()));
          const int64_t lower = *it;
          extra_ranges_.erase(it);
          l.unlock();
          unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
          unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
          RETURN_NOT_OK(lb->SetInt64("key", lower));
          RETURN_NOT_OK(ub->SetInt64("key", lower + kExtraRangeWidth));
          alterer->DropRangePartition(lb.release(), ub.release());
          LOG(INFO) << "CHAOS: dropping range partition at " << lower;
        } else {
          // Add a range partition, or add again one that was dropped.
          const int64_t lower =
              kExtraRangeBase + kExtraRangeWidth * static_cast<int64_t>(rng->Uniform(6));
          if (ContainsKey(extra_ranges_, lower)) return Status::OK();
          l.unlock();
          unique_ptr<KuduPartialRow> lb(table->schema().NewRow());
          unique_ptr<KuduPartialRow> ub(table->schema().NewRow());
          RETURN_NOT_OK(lb->SetInt64("key", lower));
          RETURN_NOT_OK(ub->SetInt64("key", lower + kExtraRangeWidth));
          alterer->AddRangePartition(lb.release(), ub.release());
          LOG(INFO) << "CHAOS: adding range partition at " << lower;
          RETURN_NOT_OK(alterer->Alter());
          l.lock();
          extra_ranges_.insert(lower);
          return Status::OK();
        }
        break;
      }
      case 2:
        if (extra_cols.size() < 3) {
          const string name = Substitute("x$0", next_column_++);
          l.unlock();
          alterer->AddColumn(name)->Type(KuduColumnSchema::INT32);
          LOG(INFO) << "CHAOS: adding column " << name;
          break;
        }
        [[fallthrough]];
      case 3:
        if (!extra_cols.empty()) {
          l.unlock();
          const string& name = extra_cols[rng->Uniform(extra_cols.size())];
          alterer->DropColumn(name);
          LOG(INFO) << "CHAOS: dropping column " << name;
          break;
        }
        return Status::OK();
      default:
        if (!extra_cols.empty()) {
          const string to = Substitute("x$0", next_column_++);
          l.unlock();
          const string& from = extra_cols[rng->Uniform(extra_cols.size())];
          alterer->AlterColumn(from)->RenameTo(to);
          LOG(INFO) << "CHAOS: renaming column " << from << " to " << to;
          break;
        }
        return Status::OK();
    }
    stats_.ddls++;
    return alterer->Alter();
  }

  // Sets 'ids' to the ids of the table's tablets.
  Status TabletIds(vector<string>* ids) {
    CDCTableMetadata md;
    RETURN_NOT_OK(cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md));
    vector<cdc::CDCTabletInfo> tablets;
    RETURN_NOT_OK(cdc_client_->GetTabletLocations(md.table_id, &tablets));
    ids->clear();
    for (const auto& t : tablets) {
      ids->push_back(t.tablet_id);
    }
    return Status::OK();
  }

  Status StepDownTabletLeader(Random* rng) {
    vector<string> tablets;
    RETURN_NOT_OK(TabletIds(&tablets));
    const string& tablet_id = tablets[rng->Uniform(tablets.size())];
    itest::TServerDetails* leader;
    RETURN_NOT_OK(itest::FindTabletLeader(ts_map_, tablet_id, MonoDelta::FromSeconds(5),
                                          &leader));
    LOG(INFO) << "CHAOS: leader of " << tablet_id << " on " << leader->uuid() << " steps down";
    return itest::LeaderStepDown(leader, tablet_id, MonoDelta::FromSeconds(5));
  }

  Status StepDownLeaderMaster() {
    int idx;
    RETURN_NOT_OK(cluster_->GetLeaderMasterIndex(&idx));
    LOG(INFO) << "CHAOS: leader master " << idx << " steps down";
    consensus::ConsensusServiceProxy proxy(cluster_->messenger(),
                                           cluster_->master(idx)->bound_rpc_addr(),
                                           cluster_->master(idx)->bound_rpc_hostport().host());
    consensus::LeaderStepDownRequestPB req;
    req.set_dest_uuid(cluster_->master(idx)->uuid());
    req.set_tablet_id(master::SysCatalogTable::kSysCatalogTabletId);
    req.set_mode(consensus::LeaderStepDownMode::GRACEFUL);
    consensus::LeaderStepDownResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(5));
    return proxy.LeaderStepDown(req, &resp, &rpc);
  }

  // Waits for a random time of 200 to 'max_ms' + 200 milliseconds.
  static void ChaosPause(Random* rng, int max_ms) {
    SleepFor(MonoDelta::FromMilliseconds(200 + rng->Uniform(max_ms)));
  }

  // Disrupts the cluster, the consumer or the table at random, one thing at a
  // time, until 'deadline'. At most one tablet server is down at a time, so
  // every tablet keeps a majority. Runs on the test's main thread, which
  // ExternalMiniCluster requires to start servers.
  void RunChaos(const MonoTime& deadline) {
    Random rng(seed_ + 200);
    while (MonoTime::Now() < deadline) {
      ChaosPause(&rng, 1500);
      stats_.chaos_events++;
      const int ts_idx = rng.Uniform(kNumTabletServers);
      const int m_idx = rng.Uniform(kNumMasters);
      const int action = rng.Uniform(9);
      // See 'no_txn_lock_'.
      std::unique_lock<std::timed_mutex> txn_lock(no_txn_lock_, std::defer_lock);
      if (action <= 1) {
        if (!txn_lock.try_lock_for(std::chrono::seconds(5))) continue;
        // Let the last commit be finalized.
        SleepFor(MonoDelta::FromSeconds(1));
      }
      switch (action) {
        case 0: {
          LOG(INFO) << "CHAOS: killing tablet server " << ts_idx;
          cluster_->tablet_server(ts_idx)->Shutdown();
          // Long enough, now and then, for the replicas to be re-replicated.
          ChaosPause(&rng, rng.OneIn(3) ? 8000 : 3000);
          LOG(INFO) << "CHAOS: restarting tablet server " << ts_idx;
          CHECK_OK(cluster_->tablet_server(ts_idx)->Restart());
          break;
        }
        case 1:
          LOG(INFO) << "CHAOS: pausing tablet server " << ts_idx;
          CHECK_OK(cluster_->tablet_server(ts_idx)->Pause());
          ChaosPause(&rng, 4000);
          LOG(INFO) << "CHAOS: resuming tablet server " << ts_idx;
          CHECK_OK(cluster_->tablet_server(ts_idx)->Resume());
          break;
        case 2:
        case 3:
          WARN_NOT_OK(StepDownTabletLeader(&rng), "tablet leader step-down failed");
          break;
        case 4:
          WARN_NOT_OK(StepDownLeaderMaster(), "master leader step-down failed");
          break;
        case 5: {
          if (rng.OneIn(2)) {
            LOG(INFO) << "CHAOS: restarting master " << m_idx;
            cluster_->master(m_idx)->Shutdown();
            ChaosPause(&rng, 2000);
            CHECK_OK(cluster_->master(m_idx)->Restart());
          } else {
            LOG(INFO) << "CHAOS: pausing master " << m_idx;
            CHECK_OK(cluster_->master(m_idx)->Pause());
            ChaosPause(&rng, 3000);
            CHECK_OK(cluster_->master(m_idx)->Resume());
          }
          break;
        }
        case 6:
          RestartConsumer(/*flush=*/rng.OneIn(2));
          break;
        default:
          WARN_NOT_OK(RandomDdl(&rng), "DDL failed");
          break;
      }
    }
  }

  Status StartConsumer() {
    CDCConsumer::Options opts;
    opts.stream_id = stream_id_;
    opts.start_mode = CDCConsumer::kSnapshot;
    opts.checkpoint_interval = MonoDelta::FromMilliseconds(500);
    opts.min_poll_backoff = MonoDelta::FromMilliseconds(20);
    opts.max_poll_backoff = MonoDelta::FromMilliseconds(300);
    opts.on_tablet_stopped = [this](const CDCTabletProgress& p) {
      if (p.state == CDCTabletState::kDropped) {
        // The tablet's partition was dropped: so are the rows it wrote.
        if (p.dropped_tail_lost) {
          std::lock_guard<std::mutex> l(lock_);
          consumer_errors_.push_back("lost the tail of dropped tablet " + p.tablet_id);
        }
        mirror_->RemoveTabletRows(p.tablet_id);
        return;
      }
      std::lock_guard<std::mutex> l(lock_);
      consumer_errors_.push_back(Substitute("tablet $0 stopped in $1: $2", p.tablet_id,
                                            cdc::CDCTabletStateToString(p.state),
                                            p.terminal_error.ToString()));
    };
    RETURN_NOT_OK(CDCConsumer::Create(cdc_client_.get(), std::move(opts), &consumer_));
    return consumer_->Start([this](const CDCRecordBatch& batch) {
      mirror_->Apply(batch);
      return Status::OK();
    });
  }

  // Stops the consumer and starts a new one, which resumes from the stream's
  // durable checkpoints.
  void RestartConsumer(bool flush) {
    std::lock_guard<std::mutex> l(consumer_lock_);
    LOG(INFO) << "CHAOS: restarting the consumer" << (flush ? " after a flush" : "");
    if (flush) {
      WARN_NOT_OK(consumer_->Flush(), "flush failed");
    }
    consumer_->Stop();
    consumer_.reset();
    stats_.consumer_restarts++;
    // The masters may be down for a while.
    Status s;
    for (int i = 0; i < 100; i++) {
      s = StartConsumer();
      if (s.ok()) return;
      LOG(WARNING) << "could not start the consumer: " << s.ToString();
      consumer_.reset();
      SleepFor(MonoDelta::FromMilliseconds(200));
    }
    CHECK_OK(s);
  }

  // Undoes what the chaos thread left: resumes every paused server and
  // starts every server that is down.
  void HealCluster() {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->tablet_server(i);
      if (ts->IsShutdown()) {
        ASSERT_OK(ts->Restart());
      } else {
        ASSERT_OK(ts->Resume());
      }
    }
    for (int i = 0; i < cluster_->num_masters(); i++) {
      auto* m = cluster_->master(i);
      if (m->IsShutdown()) {
        ASSERT_OK(m->Restart());
      } else {
        ASSERT_OK(m->Resume());
      }
    }
  }

  // Checks that no replica of the table's tablets retains WAL or history for
  // CDC. Only the members of a tablet's committed Raft config are checked: the
  // master no longer updates the barrier of a replica that was evicted, which
  // keeps it until the master deletes the replica or the barrier lease expires.
  void CheckNoRetention(const string& table_id) {
    MiniClusterFsInspector inspector(cluster_.get());
    // The READY replicas of the table, by tablet id: the indexes of their
    // tablet servers.
    map<string, vector<int>> ready_replicas;
    // The latest committed Raft config of each tablet, from any replica.
    map<string, consensus::RaftConfigPB> configs;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      for (const auto& tablet_id : inspector.ListTabletsOnTS(i)) {
        tablet::TabletSuperBlockPB sb;
        ASSERT_OK(inspector.ReadTabletSuperBlockOnTS(i, tablet_id, &sb));
        if (sb.table_id() != table_id) {
          continue;
        }
        consensus::ConsensusMetadataPB cmeta;
        if (inspector.ReadConsensusMetadataOnTS(i, tablet_id, &cmeta).ok()) {
          auto& config = configs[tablet_id];
          if (!config.has_opid_index() ||
              cmeta.committed_config().opid_index() > config.opid_index()) {
            config = cmeta.committed_config();
          }
        }
        if (sb.tablet_data_state() == tablet::TABLET_DATA_READY) {
          ready_replicas[tablet_id].push_back(i);
        }
      }
    }
    int replicas = 0;
    for (const auto& [tablet_id, ts_indexes] : ready_replicas) {
      for (int i : ts_indexes) {
        const string& uuid = cluster_->tablet_server(i)->uuid();
        const auto& peers = configs[tablet_id].peers();
        if (std::none_of(peers.begin(), peers.end(),
                         [&](const consensus::RaftPeerPB& p) {
                           return p.permanent_uuid() == uuid;
                         })) {
          LOG(INFO) << Substitute("not checking replica of tablet $0 on tablet server $1: "
                                  "it is not in the committed Raft config", tablet_id, i);
          continue;
        }
        tablet::TabletSuperBlockPB sb;
        ASSERT_OK(inspector.ReadTabletSuperBlockOnTS(i, tablet_id, &sb));
        replicas++;
        SCOPED_TRACE(Substitute("tablet $0 on tablet server $1", tablet_id, i));
        // An absent barrier means none.
        ASSERT_FALSE(sb.has_cdc_min_retained_op_index() && sb.cdc_min_retained_op_index() >= 0)
            << "the WAL barrier was not released";
        ASSERT_LE(sb.cdc_history_safe_time_micros(), 0)
            << "the history floor was not released";
        int64_t retained = 0;
        ASSERT_OK(itest::GetInt64Metric(cluster_->tablet_server(i)->bound_http_hostport(),
                                        &METRIC_ENTITY_tablet, tablet_id.c_str(),
                                        &METRIC_cdc_wal_retained_bytes, "value",
                                        &retained));
        ASSERT_EQ(0, retained);
      }
    }
    ASSERT_GE(replicas, 3);
  }

  unique_ptr<ExternalMiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;
  itest::TabletServerMap ts_map_;
  unique_ptr<CDCClient> cdc_client_;
  string stream_id_;
  unique_ptr<CDCMirror> mirror_;

  // Guards 'consumer_' against concurrent restarts.
  std::mutex consumer_lock_;
  unique_ptr<CDCConsumer> consumer_;

  int seed_ = 0;
  atomic<bool> stop_writers_{false};
  Stats stats_;

  // Held by the transactional writer for the lifetime of each transaction.
  // Holding it keeps transactions out of two kinds of chaos that crash tablet
  // servers for reasons unrelated to CDC:
  //  - a column DDL: a write of an open transaction with a column that was
  //    added after the transaction began fails a CHECK in MemRowSet::Insert();
  //  - a tablet server that is down while a commit is finalized: the retried
  //    finalization fails a DCHECK in CommitTasks::FinalizeCommitAsync().
  std::timed_mutex no_txn_lock_;

  // Guards the members below.
  std::mutex lock_;
  vector<int64_t> txn_keys_;
  set<int64_t> extra_ranges_;
  int next_column_ = 0;
  vector<string> consumer_errors_;
};

INSTANTIATE_TEST_SUITE_P(RecordTypes, CDCChaosITest,
                         ::testing::Values(CDCStreamConfigPB::CHANGE,
                                           CDCStreamConfigPB::FULL),
                         [](const ::testing::TestParamInfo<CDCStreamConfigPB::RecordType>& i) {
                           return CDCStreamConfigPB::RecordType_Name(i.param);
                         });

TEST_P(CDCChaosITest, RandomWorkloadWithChaosMatchesTable) {
  seed_ = SeedRandom();
  LOG(INFO) << "random seed: " << seed_ << " (replay with --test_random_seed=" << seed_ << ")";
  const bool full = GetParam() == CDCStreamConfigPB::FULL;
  int duration_secs = FLAGS_cdc_chaos_duration_secs;
  if (duration_secs == 0) {
    duration_secs = AllowSlowTests() ? 60 : 20;
  }

  CDCClient::Options copts;
  for (int i = 0; i < cluster_->num_masters(); i++) {
    copts.master_addresses.push_back(cluster_->master(i)->bound_rpc_hostport().ToString());
  }
  copts.rpc_timeout = MonoDelta::FromSeconds(10);
  ASSERT_OK(CDCClient::Create(std::move(copts), &cdc_client_));

  CDCTableMetadata md;
  ASSERT_OK(cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md));
  vector<int32_t> key_ids;
  for (int i = 0; i < md.schema.num_key_columns(); i++) {
    key_ids.push_back(md.schema.column_id(i));
  }
  mirror_.reset(new CDCMirror(key_ids, full));

  // Rows for the snapshot.
  {
    Random rng(seed_ + 300);
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(OpenTable(&table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int i = 0; i < 500; i++) {
      KuduWriteOperation* op = table->NewUpsert();
      ASSERT_OK(op->mutable_row()->SetInt64("key", rng.Uniform(kKeySpace)));
      SetRandomValues(table->schema(), /*some=*/false, &rng, op->mutable_row());
      ASSERT_OK(session->Apply(op));
    }
    ASSERT_OK(session->Flush());
  }

  CDCStreamOptions sopts;
  sopts.record_type = GetParam();
  sopts.snapshot_mode = CDCStreamConfigPB::INITIAL_AND_CONTINUE;
  ASSERT_OK(cdc_client_->CreateStream(kTableName, sopts, &stream_id_));
  {
    std::lock_guard<std::mutex> l(consumer_lock_);
    ASSERT_OK(StartConsumer());
  }

  vector<thread> writers;
  for (int i = 0; i < kNumWriters; i++) {
    writers.emplace_back([this, i] { WriterThread(i); });
  }
  writers.emplace_back([this] { TxnWriterThread(); });
  RunChaos(MonoTime::Now() + MonoDelta::FromSeconds(duration_secs));
  NO_FATALS(HealCluster());
  stop_writers_ = true;
  for (auto& t : writers) {
    t.join();
  }
  LOG(INFO) << Substitute(
      "workload: $0 ops, $1 row errors, $2 flush errors; transactions: $3 committed, "
      "$4 aborted, $5 failed; $6 DDLs; $7 chaos events, $8 consumer restarts",
      stats_.ops.load(), stats_.row_errors.load(), stats_.flush_errors.load(),
      stats_.txns_committed.load(), stats_.txns_aborted.load(), stats_.txns_failed.load(),
      stats_.ddls.load(), stats_.chaos_events.load(), stats_.consumer_restarts.load());

  // The copy catches up with the table.
  client::sp::shared_ptr<KuduTable> table;
  AssertEventually([&] {
    ASSERT_OK(OpenTable(&table));
    CDCTableMetadata md;
    ASSERT_OK(cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md));
    string diff;
    ASSERT_OK(mirror_->CompareWithTable(table.get(), md.schema, &diff));
    ASSERT_TRUE(diff.empty()) << diff;
  }, MonoDelta::FromSeconds(120));
  NO_PENDING_FATALS();
  LOG(INFO) << Substitute("the copy has $0 rows; $1 row records applied, $2 before-images "
                          "checked", mirror_->rows().size(), mirror_->num_row_records(),
                          mirror_->num_before_images_checked());

  const vector<string> violations = mirror_->violations();
  EXPECT_TRUE(violations.empty()) << JoinStrings(violations, "\n");
  {
    std::lock_guard<std::mutex> l(lock_);
    EXPECT_TRUE(consumer_errors_.empty()) << JoinStrings(consumer_errors_, "\n");
  }
  ASSERT_GT(stats_.txns_committed, 0);
  if (full) {
    ASSERT_GT(mirror_->num_before_images_checked(), 0);
  }

  // Once the stream is deleted, no replica retains anything for it.
  {
    std::lock_guard<std::mutex> l(consumer_lock_);
    consumer_->Stop();
    consumer_.reset();
  }
  ASSERT_OK(cdc_client_->DeleteStream(stream_id_));
  AssertEventually([&] {
    NO_FATALS(CheckNoRetention(table->id()));
  }, MonoDelta::FromSeconds(60));
  NO_PENDING_FATALS();
}

} // namespace kudu
