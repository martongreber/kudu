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

// Tests of CDC across leader master changes, on a cluster with three masters:
// the stream state that one leader persisted is used by the next one, and the
// control plane operations started by one leader are completed by the next.

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
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
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cdc_mirror.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(cdc_tablet_rescan_interval_ms);

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamInfo;
using kudu::cdc::CDCStreamOptions;
using kudu::cdc::CDCTableMetadata;
using kudu::cdc::CDCTabletProgress;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduWriteOperation;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::itest::CDCMirror;
using kudu::itest::MiniClusterFsInspector;
using kudu::master::CDCStreamConfigPB;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {
constexpr const char* kTableName = "cdc_master_failover";
constexpr int kNumTablets = 3;
const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
// Checkpoints by tablet id.
typedef std::map<string, int64_t> CheckpointMap;
} // anonymous namespace

class CDCMasterFailoverITest : public KuduTest {
 protected:
  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_cdc_tablet_rescan_interval_ms = 1000;
  }

  void StartCluster(vector<string> extra_master_flags = {}) {
    ExternalMiniClusterOptions opts;
    opts.num_masters = 3;
    opts.num_tablet_servers = 3;
    opts.extra_master_flags = {
      "--cdc_enabled=true",
      "--cdc_bg_scan_interval_ms=200",
      "--catalog_manager_bg_task_wait_ms=100",
    };
    for (auto& f : extra_master_flags) {
      opts.extra_master_flags.emplace_back(std::move(f));
    }
    opts.extra_tserver_flags = {
      "--cdc_checkpoint_persist_interval_ms=0",
      "--cdc_active_time_report_interval_ms=500",
      "--cdc_checkpoint_flush_interval_ms=100",
    };
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(KuduColumnSchema::INT32);
    ASSERT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(kTableName)
              .schema(&schema)
              .add_hash_partitions({ "key" }, kNumTablets)
              .num_replicas(3)
              .Create());
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    table_id_ = table->id();

    CDCClient::Options copts;
    for (int i = 0; i < cluster_->num_masters(); i++) {
      copts.master_addresses.push_back(cluster_->master(i)->bound_rpc_hostport().ToString());
    }
    copts.rpc_timeout = kTimeout;
    ASSERT_OK(CDCClient::Create(std::move(copts), &cdc_client_));
  }

  void TearDown() override {
    if (consumer_) {
      consumer_->Stop();
      consumer_.reset();
    }
    cdc_client_.reset();
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  // Upserts the keys [start, start + count) modulo 1000.
  Status TryWriteRows(int start, int count) {
    client::sp::shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    session->SetTimeoutMillis(kTimeout.ToMilliseconds());
    RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int i = start; i < start + count; i++) {
      KuduWriteOperation* op = table->NewUpsert();
      RETURN_NOT_OK(op->mutable_row()->SetInt32("key", i % 1000));
      RETURN_NOT_OK(op->mutable_row()->SetInt32("val", i));
      RETURN_NOT_OK(session->Apply(op));
    }
    return session->Flush();
  }

  void WriteRows(int start, int count) {
    ASSERT_OK(TryWriteRows(start, count));
  }

  // Makes the leader master step down, and waits for another master to lead.
  void StepDownLeaderMaster() {
    int old_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&old_idx));
    ASSERT_EVENTUALLY([&] {
      int idx;
      ASSERT_OK(cluster_->GetLeaderMasterIndex(&idx));
      if (idx == old_idx) {
        consensus::ConsensusServiceProxy proxy(
            cluster_->messenger(), cluster_->master(idx)->bound_rpc_addr(),
            cluster_->master(idx)->bound_rpc_hostport().host());
        consensus::LeaderStepDownRequestPB req;
        req.set_dest_uuid(cluster_->master(idx)->uuid());
        req.set_tablet_id(master::SysCatalogTable::kSysCatalogTabletId);
        req.set_mode(consensus::LeaderStepDownMode::GRACEFUL);
        consensus::LeaderStepDownResponsePB resp;
        rpc::RpcController rpc;
        rpc.set_timeout(MonoDelta::FromSeconds(5));
        ASSERT_OK(proxy.LeaderStepDown(req, &resp, &rpc));
        ASSERT_OK(cluster_->GetLeaderMasterIndex(&idx));
      }
      ASSERT_NE(old_idx, idx);
    });
  }

  // Kills the leader master and returns its index.
  int KillLeaderMaster() {
    int idx = -1;
    CHECK_OK(cluster_->GetLeaderMasterIndex(&idx));
    LOG(INFO) << "killing leader master " << idx;
    cluster_->master(idx)->Shutdown();
    return idx;
  }

  // Checks every replica of the table's tablets: with 'retained', that it has
  // a CDC retention barrier, and otherwise that it has none.
  void CheckBarriers(bool retained) {
    MiniClusterFsInspector inspector(cluster_.get());
    int replicas = 0;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      for (const auto& tablet_id : inspector.ListTabletsOnTS(i)) {
        tablet::TabletSuperBlockPB sb;
        ASSERT_OK(inspector.ReadTabletSuperBlockOnTS(i, tablet_id, &sb));
        if (sb.table_id() != table_id_ || sb.tablet_data_state() != tablet::TABLET_DATA_READY) {
          continue;
        }
        replicas++;
        SCOPED_TRACE(Substitute("tablet $0 on tablet server $1", tablet_id, i));
        // An absent barrier means none.
        const bool has_barrier =
            sb.has_cdc_min_retained_op_index() && sb.cdc_min_retained_op_index() >= 0;
        ASSERT_EQ(retained, has_barrier) << sb.cdc_min_retained_op_index();
      }
    }
    ASSERT_EQ(kNumTablets * 3, replicas);
  }

  Status StartConsumer(const string& stream_id, CDCMirror* mirror) {
    CDCConsumer::Options opts;
    opts.stream_id = stream_id;
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
    RETURN_NOT_OK(CDCConsumer::Create(cdc_client_.get(), std::move(opts), &consumer_));
    return consumer_->Start([mirror](const CDCRecordBatch& batch) {
      mirror->Apply(batch);
      return Status::OK();
    });
  }

  // Sets 'cps' to the stream's durable checkpoints, as the leader master has
  // them.
  Status Checkpoints(const string& stream_id, CheckpointMap* cps) {
    CDCStreamInfo info;
    RETURN_NOT_OK(cdc_client_->GetStreamInfo(stream_id, &info));
    *cps = std::move(info.tablet_checkpoints);
    return Status::OK();
  }

  unique_ptr<ExternalMiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;
  unique_ptr<CDCClient> cdc_client_;
  unique_ptr<CDCConsumer> consumer_;
  string table_id_;

  std::mutex lock_;
  vector<string> consumer_errors_;
};

// A CDCClient call made while the masters elect a new leader waits for the new
// leader, rather than failing as soon as every master said it is not the
// leader.
TEST_F(CDCMasterFailoverITest, ClientCallDuringLeaderElectionSucceeds) {
  NO_FATALS(StartCluster());
  string stream_id;
  ASSERT_OK(cdc_client_->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  for (int i = 0; i < 3; i++) {
    SCOPED_TRACE(i);
    const int killed = KillLeaderMaster();
    CDCStreamInfo info;
    ASSERT_OK(cdc_client_->GetStreamInfo(stream_id, &info));
    ASSERT_EQ(stream_id, info.stream_id);
    ASSERT_OK(cluster_->master(killed)->Restart());
  }
}

// The checkpoints that consumers made through one leader master are the
// stream's checkpoints on the next one, and the next one takes new ones.
TEST_F(CDCMasterFailoverITest, CheckpointsSurviveLeaderMasterChange) {
  NO_FATALS(StartCluster());
  string stream_id;
  ASSERT_OK(cdc_client_->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  CDCTableMetadata md;
  ASSERT_OK(cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md));
  CDCMirror mirror({ md.schema.column_id(0) }, /*full_images=*/false);
  ASSERT_OK(StartConsumer(stream_id, &mirror));

  CheckpointMap last;
  for (int round = 0; round < 3; round++) {
    SCOPED_TRACE(round);
    NO_FATALS(WriteRows(round * 100, 100));
    // Every tablet's checkpoint advances on the leader master.
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(consumer_->Flush());
      CheckpointMap cps;
      ASSERT_OK(Checkpoints(stream_id, &cps));
      ASSERT_EQ(kNumTablets, cps.size());
      for (const auto& e : cps) {
        ASSERT_GT(e.second, FindWithDefault(last, e.first, 0)) << e.first;
      }
      last = cps;
    });
    if (round % 2 == 0) {
      NO_FATALS(StepDownLeaderMaster());
    } else {
      const int killed = KillLeaderMaster();
      ASSERT_OK(cluster_->master(killed)->Restart());
    }
    // The new leader has the checkpoints the previous one persisted.
    ASSERT_EVENTUALLY([&] {
      CheckpointMap cps;
      ASSERT_OK(Checkpoints(stream_id, &cps));
      for (const auto& e : cps) {
        ASSERT_GE(e.second, last[e.first]) << e.first;
      }
    });
  }
  // And the consumer, which kept running, has every row.
  client::sp::shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_EVENTUALLY([&] {
    string diff;
    ASSERT_OK(mirror.CompareWithTable(table.get(), md.schema, &diff));
    ASSERT_TRUE(diff.empty()) << diff;
  });
  EXPECT_TRUE(mirror.violations().empty()) << JoinStrings(mirror.violations(), "\n");
  std::lock_guard<std::mutex> l(lock_);
  EXPECT_TRUE(consumer_errors_.empty()) << JoinStrings(consumer_errors_, "\n");
}

// A stream deletion that the leader master accepted is completed by the next
// leader if the first one dies before it released the stream's retention.
TEST_F(CDCMasterFailoverITest, DeleteStreamCompletesAcrossLeaderMasterChange) {
  NO_FATALS(StartCluster());
  string stream_id;
  ASSERT_OK(cdc_client_->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  NO_FATALS(WriteRows(0, 100));
  ASSERT_EVENTUALLY([&] { NO_FATALS(CheckBarriers(/*retained=*/true)); });

  // Pause the tablet servers so that the leader master cannot release the
  // barriers before it dies.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(cluster_->tablet_server(i)->Pause());
  }
  ASSERT_OK(cdc_client_->DeleteStream(stream_id));
  const int killed = KillLeaderMaster();
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(cluster_->tablet_server(i)->Resume());
  }

  // The new leader releases the barriers and finishes the deletion.
  ASSERT_EVENTUALLY([&] { NO_FATALS(CheckBarriers(/*retained=*/false)); });
  ASSERT_EVENTUALLY([&] {
    vector<CDCStreamInfo> streams;
    ASSERT_OK(cdc_client_->ListStreams("", &streams, /*include_deleting=*/true));
    ASSERT_TRUE(streams.empty()) << streams[0].stream_id;
  });
  ASSERT_OK(cluster_->master(killed)->Restart());
  // The restarted master does not bring the stream back either.
  NO_FATALS(StepDownLeaderMaster());
  vector<CDCStreamInfo> streams;
  ASSERT_OK(cdc_client_->ListStreams("", &streams, /*include_deleting=*/true));
  ASSERT_TRUE(streams.empty());
}

// Streams created while the leader master changes are either created whole,
// with a checkpoint and a retention barrier for every tablet, or not at all.
TEST_F(CDCMasterFailoverITest, CreateStreamDuringLeaderMasterChangesIsAtomic) {
  NO_FATALS(StartCluster({ "--cdc_max_streams=1000", "--cdc_max_streams_per_table=1000" }));
  std::atomic<bool> stop(false);
  std::set<string> created;
  int failed = 0;
  thread creator([&] {
    while (!stop) {
      string stream_id;
      CDCStreamOptions opts;
      const Status s = cdc_client_->CreateStream(kTableName, opts, &stream_id);
      if (s.ok()) {
        created.insert(stream_id);
      } else {
        LOG(INFO) << "CreateStream failed: " << s.ToString();
        failed++;
      }
      SleepFor(MonoDelta::FromMilliseconds(20));
    }
  });
  auto join_creator = MakeScopedCleanup([&] {
    stop = true;
    creator.join();
  });
  for (int i = 0; i < 4; i++) {
    SleepFor(MonoDelta::FromMilliseconds(300));
    if (i % 2 == 0) {
      NO_FATALS(StepDownLeaderMaster());
    } else {
      const int killed = KillLeaderMaster();
      SleepFor(MonoDelta::FromMilliseconds(500));
      ASSERT_OK(cluster_->master(killed)->Restart());
    }
  }
  join_creator.run();
  LOG(INFO) << Substitute("$0 streams created, $1 creations failed", created.size(), failed);
  ASSERT_FALSE(created.empty());

  // Every stream that exists is whole. A stream whose creation the client saw
  // fail may exist (its response was lost), but then it is whole too.
  ASSERT_EVENTUALLY([&] {
    vector<CDCStreamInfo> streams;
    ASSERT_OK(cdc_client_->ListStreams("", &streams, /*include_deleting=*/true));
    std::set<string> listed;
    for (const auto& s : streams) {
      listed.insert(s.stream_id);
      ASSERT_EQ(master::SysCDCStreamEntryPB::ACTIVE, s.state) << s.stream_id;
      CDCStreamInfo info;
      ASSERT_OK(cdc_client_->GetStreamInfo(s.stream_id, &info));
      ASSERT_EQ(kNumTablets, info.tablet_checkpoints.size()) << s.stream_id;
    }
    for (const auto& id : created) {
      ASSERT_TRUE(ContainsKey(listed, id)) << "stream " << id << " was lost";
    }
    ASSERT_LE(listed.size(), created.size() + failed);
  });
  NO_FATALS(CheckBarriers(/*retained=*/true));
}

// A stream that expires is released by the leader master that takes over
// from the one that tracked its activity.
TEST_F(CDCMasterFailoverITest, ExpiryReleaseSurvivesLeaderMasterChange) {
  NO_FATALS(StartCluster({ "--cdc_stream_expiry_ms=6000" }));
  string stream_id;
  ASSERT_OK(cdc_client_->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  NO_FATALS(WriteRows(0, 10));
  ASSERT_EVENTUALLY([&] { NO_FATALS(CheckBarriers(/*retained=*/true)); });
  // The stream is not consumed. Change the leader before it expires.
  const int killed = KillLeaderMaster();
  AssertEventually([&] {
    NO_FATALS(CheckBarriers(/*retained=*/false));
  }, MonoDelta::FromSeconds(60));
  NO_PENDING_FATALS();
  ASSERT_OK(cluster_->master(killed)->Restart());
}

// A consumer that runs while the leader master changes again and again, and
// that is restarted from its durable checkpoints, gets every change.
TEST_F(CDCMasterFailoverITest, ConsumerAcrossLeaderMasterChanges) {
  NO_FATALS(StartCluster());
  string stream_id;
  ASSERT_OK(cdc_client_->CreateStream(kTableName, CDCStreamOptions(), &stream_id));
  CDCTableMetadata md;
  ASSERT_OK(cdc_client_->GetTableMetadata(kTableName, /*by_id=*/false, &md));
  CDCMirror mirror({ md.schema.column_id(0) }, /*full_images=*/false);
  mirror.set_check_empty_transactions(true);
  // The consumer applies to 'mirror': stop it before 'mirror' goes away.
  SCOPED_CLEANUP({
    if (consumer_) {
      consumer_->Stop();
      consumer_.reset();
    }
  });
  ASSERT_OK(StartConsumer(stream_id, &mirror));

  std::atomic<bool> stop(false);
  thread writer([&] {
    for (int i = 0; !stop; i += 50) {
      // A write may fail while the masters change.
      WARN_NOT_OK(TryWriteRows(i, 50), "write failed");
    }
  });
  auto join_writer = MakeScopedCleanup([&] {
    stop = true;
    writer.join();
  });
  for (int i = 0; i < 6; i++) {
    SleepFor(MonoDelta::FromMilliseconds(500));
    switch (i % 3) {
      case 0:
        NO_FATALS(StepDownLeaderMaster());
        break;
      case 1: {
        const int killed = KillLeaderMaster();
        SleepFor(MonoDelta::FromMilliseconds(500));
        ASSERT_OK(cluster_->master(killed)->Restart());
        break;
      }
      default:
        WARN_NOT_OK(consumer_->Flush(), "flush failed");
        consumer_->Stop();
        consumer_.reset();
        ASSERT_OK(StartConsumer(stream_id, &mirror));
        break;
    }
  }
  join_writer.run();

  client::sp::shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(kTableName, &table));
  ASSERT_EVENTUALLY([&] {
    string diff;
    ASSERT_OK(mirror.CompareWithTable(table.get(), md.schema, &diff));
    ASSERT_TRUE(diff.empty()) << diff;
  });
  EXPECT_TRUE(mirror.violations().empty()) << JoinStrings(mirror.violations(), "\n");
  std::lock_guard<std::mutex> l(lock_);
  EXPECT_TRUE(consumer_errors_.empty()) << JoinStrings(consumer_errors_, "\n");
}

} // namespace kudu
