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

// Tests of CDC across the life cycle of tablet replicas, with a consumer that
// keeps consuming: a replica that is re-replicated by a tablet copy, a replica
// that is moved to another tablet server, and the leadership moving to a
// copied replica. The consumer's copy of the table must equal the table.
// Also a randomized stress of these with streams that come and go, meant to
// run under TSAN as well.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/cdc/cdc_service.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cdc_mirror.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_bool(catalog_manager_evict_excess_replicas);
DECLARE_bool(cdc_enabled);
DECLARE_int32(catalog_manager_bg_task_wait_ms);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_tablet_rescan_interval_ms);
DECLARE_int32(follower_unavailable_considered_failed_sec);
DECLARE_int64(cdc_checkpoint_persist_interval_ms);

DEFINE_int32(cdc_lifecycle_stress_secs, 0,
             "How long CDCReplicaLifecycleITest.Stress runs, in seconds. 0 means 10 "
             "seconds, or 30 seconds if slow tests are allowed.");

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamOptions;
using kudu::cdc::CDCTableMetadata;
using kudu::cdc::CDCTabletProgress;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduWriteOperation;
using kudu::consensus::RaftPeerPB;
using kudu::itest::CDCMirror;
using kudu::itest::TServerDetails;
using kudu::tablet::TabletReplica;
using std::atomic;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {
constexpr const char* kTableName = "cdc_replica_lifecycle";
constexpr int kNumTabletServers = 4;
const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
} // anonymous namespace

class CDCReplicaLifecycleITest : public MiniClusterITestBase {
 protected:
  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_cdc_enabled = true;
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
    FLAGS_catalog_manager_bg_task_wait_ms = 100;
    FLAGS_cdc_bg_scan_interval_ms = 100;
    FLAGS_cdc_tablet_rescan_interval_ms = 1000;
    FLAGS_cdc_checkpoint_persist_interval_ms = 200;
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

  // Starts the cluster, creates a single-tablet table with RF 3 (one tablet
  // server has no replica) and a stream, and starts a consumer of it.
  void SetUpClusterAndConsumer() {
    NO_FATALS(StartCluster(kNumTabletServers));
    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(KuduColumnSchema::INT32);
    ASSERT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(kTableName)
              .schema(&schema)
              .set_range_partition_columns({ "key" })
              .num_replicas(3)
              .Create());

    CDCClient::Options opts;
    opts.master_addresses.push_back(cluster_->mini_master()->bound_rpc_addr().ToString());
    opts.rpc_timeout = kTimeout;
    ASSERT_OK(CDCClient::Create(std::move(opts), &cdc_client_));
    ASSERT_OK(cdc_client_->CreateStream(kTableName, CDCStreamOptions(), &stream_id_));
    CDCTableMetadata md;
    ASSERT_OK(cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md));
    schema_ = md.schema;
    mirror_.reset(new CDCMirror({ schema_.column_id(0) }, /*full_images=*/false));
    mirror_->set_check_empty_transactions(true);
    vector<cdc::CDCTabletInfo> tablets;
    ASSERT_OK(cdc_client_->GetTabletLocations(md.table_id, &tablets));
    ASSERT_EQ(1, tablets.size());
    tablet_id_ = tablets[0].tablet_id;
    NO_FATALS(StartConsumer());
  }

  void StartConsumer() {
    CDCConsumer::Options opts;
    opts.stream_id = stream_id_;
    opts.start_mode = CDCConsumer::kEarliest;
    opts.checkpoint_interval = MonoDelta::FromMilliseconds(200);
    opts.min_poll_backoff = MonoDelta::FromMilliseconds(20);
    opts.max_poll_backoff = MonoDelta::FromMilliseconds(200);
    opts.on_tablet_stopped = [this](const CDCTabletProgress& p) {
      std::lock_guard<std::mutex> l(lock_);
      consumer_errors_.push_back(Substitute("tablet $0 stopped in $1: $2", p.tablet_id,
                                            cdc::CDCTabletStateToString(p.state),
                                            p.terminal_error.ToString()));
    };
    ASSERT_OK(CDCConsumer::Create(cdc_client_.get(), std::move(opts), &consumer_));
    ASSERT_OK(consumer_->Start([this](const CDCRecordBatch& batch) {
      mirror_->Apply(batch);
      return Status::OK();
    }));
  }

  // Upserts random rows until 'stop_writer_'.
  void StartWriter() {
    writer_ = thread([this] {
      Random rng(SeedRandom());
      client::sp::shared_ptr<KuduTable> table;
      CHECK_OK(client_->OpenTable(kTableName, &table));
      while (!stop_writer_) {
        client::sp::shared_ptr<KuduSession> session = client_->NewSession();
        session->SetTimeoutMillis(kTimeout.ToMilliseconds());
        CHECK_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
        for (int i = 0; i < 20; i++) {
          const bool del = rng.OneIn(5);
          KuduWriteOperation* op = del ? static_cast<KuduWriteOperation*>(
              table->NewDeleteIgnore()) : table->NewUpsert();
          CHECK_OK(op->mutable_row()->SetInt32("key", rng.Uniform(500)));
          if (!del) {
            CHECK_OK(op->mutable_row()->SetInt32("val", rng.Next()));
          }
          CHECK_OK(session->Apply(op));
        }
        // A write may time out while a replica moves.
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
  }

  // Waits for the writer to write 'n' more batches.
  void WaitForWrites(int n) {
    const int64_t target = num_batches_ + n;
    ASSERT_EVENTUALLY([&] { ASSERT_GE(num_batches_, target); });
  }

  // The consumer's copy of the table equals the table, and the consumer saw
  // nothing wrong.
  void CheckConsumerCopy() {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    ASSERT_EVENTUALLY([&] {
      string diff;
      ASSERT_OK(mirror_->CompareWithTable(table.get(), schema_, &diff));
      ASSERT_TRUE(diff.empty()) << diff;
    });
    EXPECT_TRUE(mirror_->violations().empty()) << JoinStrings(mirror_->violations(), "\n");
    std::lock_guard<std::mutex> l(lock_);
    EXPECT_TRUE(consumer_errors_.empty()) << JoinStrings(consumer_errors_, "\n");
  }

  // The replica of the tablet on tablet server 'idx', or null.
  scoped_refptr<TabletReplica> Replica(int idx) {
    scoped_refptr<TabletReplica> replica;
    auto* ts = cluster_->mini_tablet_server(idx);
    if (!ts->is_started() ||
        !ts->server()->tablet_manager()->GetTabletReplica(tablet_id_, &replica).ok()) {
      return nullptr;
    }
    return replica;
  }

  // The index of the tablet server whose replica of the tablet is in 'role'
  // (the leader, a follower or, with RaftPeerPB::UNKNOWN_ROLE, none), or -1.
  int FindServer(RaftPeerPB::Role role) {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      if (!cluster_->mini_tablet_server(i)->is_started()) continue;
      scoped_refptr<TabletReplica> r = Replica(i);
      const bool hosts = r && r->tablet_metadata()->tablet_data_state() ==
                                  tablet::TABLET_DATA_READY;
      if (role == RaftPeerPB::UNKNOWN_ROLE) {
        if (!hosts) return i;
        continue;
      }
      if (hosts && r->shared_consensus() && r->shared_consensus()->role() == role) {
        return i;
      }
    }
    return -1;
  }

  TServerDetails* Details(int idx) {
    return ts_map_[cluster_->mini_tablet_server(idx)->uuid()];
  }

  // Adds a replica of the tablet on tablet server 'idx', as a non-voter that
  // the leader promotes once it caught up, as 'kudu tablet change_config
  // move_replica' does.
  Status AddReplica(int leader_idx, int idx) {
    consensus::RaftPeerAttrsPB attrs;
    attrs.set_promote(true);
    return itest::AddServer(Details(leader_idx), tablet_id_, Details(idx),
                            RaftPeerPB::NON_VOTER, kTimeout, attrs);
  }

  // Moves the leadership of the tablet to the replica on tablet server 'idx'.
  void MoveLeaderTo(int idx) {
    ASSERT_EVENTUALLY([&] {
      const int leader_idx = FindServer(RaftPeerPB::LEADER);
      ASSERT_GE(leader_idx, 0);
      if (leader_idx != idx) {
        WARN_NOT_OK(itest::LeaderStepDown(Details(leader_idx), tablet_id_, kTimeout,
                                          nullptr, cluster_->mini_tablet_server(idx)->uuid()),
                    "step-down failed");
      }
      ASSERT_EQ(idx, FindServer(RaftPeerPB::LEADER));
    });
  }

  // Waits until the replica on tablet server 'idx' is a running voter that
  // copied the tablet, and checks that it has the stream's retention barrier.
  void WaitForCopiedReplica(int idx) {
    AssertEventually([&] {
      scoped_refptr<TabletReplica> r = Replica(idx);
      ASSERT_TRUE(r);
      ASSERT_EQ(tablet::RUNNING, r->state());
      ASSERT_EQ(tablet::TABLET_DATA_READY, r->tablet_metadata()->tablet_data_state());
      // A promoted non-voter is a follower.
      const auto role = r->shared_consensus()->role();
      ASSERT_TRUE(role == RaftPeerPB::FOLLOWER || role == RaftPeerPB::LEADER)
          << RaftPeerPB::Role_Name(role);
      ASSERT_GE(r->tablet_metadata()->cdc_min_retained_op_index(), 0)
          << "the copied replica has no retention barrier";
    }, MonoDelta::FromSeconds(60));
    NO_PENDING_FATALS();
  }

  unique_ptr<CDCClient> cdc_client_;
  unique_ptr<CDCConsumer> consumer_;
  unique_ptr<CDCMirror> mirror_;
  string stream_id_;
  string tablet_id_;
  Schema schema_;

  thread writer_;
  atomic<bool> stop_writer_{false};
  atomic<int64_t> num_batches_{0};

  std::mutex lock_;
  vector<string> consumer_errors_;
};

// A follower that fails is replaced by a copy of the tablet on the spare
// tablet server. The copy has the stream's retention barrier, and once it
// leads, the consumer reads from it without a gap.
TEST_F(CDCReplicaLifecycleITest, ReReplicatedReplicaInheritsBarrierAndServesConsumer) {
  FLAGS_follower_unavailable_considered_failed_sec = 2;
  NO_FATALS(SetUpClusterAndConsumer());
  StartWriter();
  NO_FATALS(WaitForWrites(10));

  const int spare = FindServer(RaftPeerPB::UNKNOWN_ROLE);
  ASSERT_GE(spare, 0);
  int failed = -1;
  ASSERT_EVENTUALLY([&] {
    failed = FindServer(RaftPeerPB::FOLLOWER);
    ASSERT_GE(failed, 0);
  });
  LOG(INFO) << "shutting down the follower on tablet server " << failed;
  cluster_->mini_tablet_server(failed)->Shutdown();
  NO_FATALS(WaitForCopiedReplica(spare));
  NO_FATALS(MoveLeaderTo(spare));
  NO_FATALS(WaitForWrites(10));
  StopWriter();
  NO_FATALS(CheckConsumerCopy());
  ASSERT_OK(cluster_->mini_tablet_server(failed)->Restart());
}

// The leader's replica is moved to the spare tablet server while the stream
// is consumed: a new voter copies the tablet, takes over the leadership, and
// the old leader's replica is removed. The removed replica is tombstoned and
// keeps no CDC session.
TEST_F(CDCReplicaLifecycleITest, MoveLeaderReplicaDuringConsumption) {
  // The test removes the moved replica itself.
  FLAGS_catalog_manager_evict_excess_replicas = false;
  NO_FATALS(SetUpClusterAndConsumer());
  StartWriter();
  NO_FATALS(WaitForWrites(10));

  const int spare = FindServer(RaftPeerPB::UNKNOWN_ROLE);
  ASSERT_GE(spare, 0);
  int old_leader = -1;
  ASSERT_EVENTUALLY([&] {
    old_leader = FindServer(RaftPeerPB::LEADER);
    ASSERT_GE(old_leader, 0);
  });
  ASSERT_EVENTUALLY([&] {
    ASSERT_TRUE(cluster_->mini_tablet_server(old_leader)->server()->cdc_service()
                    ->HasSessionForTests(stream_id_, tablet_id_));
  });
  ASSERT_OK(AddReplica(old_leader, spare));
  NO_FATALS(WaitForCopiedReplica(spare));
  NO_FATALS(MoveLeaderTo(spare));
  // The new leader accepts a config change once it committed an op.
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(itest::RemoveServer(Details(spare), tablet_id_, Details(old_leader), kTimeout));
  });
  ASSERT_EVENTUALLY([&] {
    scoped_refptr<TabletReplica> removed = Replica(old_leader);
    ASSERT_TRUE(!removed || removed->tablet_metadata()->tablet_data_state() ==
                                tablet::TABLET_DATA_TOMBSTONED);
    ASSERT_FALSE(cluster_->mini_tablet_server(old_leader)->server()->cdc_service()
                     ->HasSessionForTests(stream_id_, tablet_id_));
  });
  NO_FATALS(WaitForWrites(10));
  StopWriter();
  NO_FATALS(CheckConsumerCopy());
}

// Randomized stress of the replica life cycle with CDC: while a consumer
// consumes the table, the leadership moves, replicas are moved to other
// tablet servers, and other streams are created, polled and deleted. Meant to
// run under TSAN too.
TEST_F(CDCReplicaLifecycleITest, Stress) {
  int duration_secs = FLAGS_cdc_lifecycle_stress_secs;
  if (duration_secs == 0) {
    duration_secs = AllowSlowTests() ? 30 : 10;
  }
  // The test removes the moved replicas itself.
  FLAGS_catalog_manager_evict_excess_replicas = false;
  NO_FATALS(SetUpClusterAndConsumer());
  StartWriter();

  // Streams that come and go, each polled from the leader a few times.
  atomic<bool> stop(false);
  thread churn([&] {
    Random rng(SeedRandom());
    while (!stop) {
      string stream_id;
      if (!cdc_client_->CreateStream(kTableName, CDCStreamOptions(), &stream_id).ok()) {
        continue;
      }
      vector<cdc::CDCTabletInfo> tablets;
      CDCTableMetadata md;
      if (cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md).ok() &&
          cdc_client_->GetTabletLocations(md.table_id, &tablets).ok() && !tablets.empty()) {
        int64_t from = 0;
        for (int i = 0; i < 1 + rng.Uniform(5); i++) {
          cdc::GetChangesRequestPB req;
          req.set_stream_id(stream_id);
          req.set_tablet_id(tablet_id_);
          req.set_from_op_index(from);
          cdc::GetChangesResponsePB resp;
          if (!cdc_client_->GetChanges(tablets[0].leader, req, &resp).ok() ||
              resp.has_error()) {
            break;
          }
          from = std::max(from, resp.checkpoint_op_index());
          cdc::CheckpointRequestPB creq;
          creq.set_stream_id(stream_id);
          creq.set_tablet_id(tablet_id_);
          creq.set_op_index(from);
          cdc::CheckpointResponsePB cresp;
          WARN_NOT_OK(cdc_client_->Checkpoint(tablets[0].leader, creq, &cresp),
                      "checkpoint failed");
        }
      }
      WARN_NOT_OK(cdc_client_->DeleteStream(stream_id), "could not delete a stream");
    }
  });
  auto join_churn = MakeScopedCleanup([&] {
    stop = true;
    churn.join();
  });

  Random rng(SeedRandom());
  const MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(duration_secs);
  while (MonoTime::Now() < deadline) {
    SleepFor(MonoDelta::FromMilliseconds(200 + rng.Uniform(500)));
    const int leader = FindServer(RaftPeerPB::LEADER);
    if (leader < 0) continue;
    if (rng.OneIn(2)) {
      // Move the leadership to a random follower.
      const int follower = FindServer(RaftPeerPB::FOLLOWER);
      if (follower >= 0) {
        WARN_NOT_OK(itest::LeaderStepDown(Details(leader), tablet_id_, kTimeout, nullptr,
                                          cluster_->mini_tablet_server(follower)->uuid()),
                    "step-down failed");
      }
      continue;
    }
    // Move a follower's replica to the tablet server without one.
    const int spare = FindServer(RaftPeerPB::UNKNOWN_ROLE);
    const int follower = FindServer(RaftPeerPB::FOLLOWER);
    if (spare < 0 || follower < 0) continue;
    LOG(INFO) << Substitute("moving the replica on tablet server $0 to $1", follower, spare);
    if (!AddReplica(leader, spare).ok()) {
      continue;
    }
    NO_FATALS(WaitForCopiedReplica(spare));
    for (int i = 0; i < 10; i++) {
      const int cur_leader = FindServer(RaftPeerPB::LEADER);
      if (cur_leader >= 0 && cur_leader != follower &&
          itest::RemoveServer(Details(cur_leader), tablet_id_, Details(follower),
                              kTimeout).ok()) {
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(200));
    }
  }
  join_churn.run();
  StopWriter();
  NO_FATALS(CheckConsumerCopy());
}

} // namespace kudu
