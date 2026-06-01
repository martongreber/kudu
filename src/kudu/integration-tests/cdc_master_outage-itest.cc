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

// Tests that the CDC data path on a tablet server does not wait on the
// masters: a paused (black-holed) master must not delay GetChanges, Checkpoint
// or snapshot pages, and checkpoints reach the master once it is reachable.

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc.proxy.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_UpdateCDCCheckpoint);
METRIC_DECLARE_histogram(handler_latency_kudu_master_MasterService_UpdateCDCCheckpoints);

using kudu::cdc::CDCErrorPB;
using kudu::cdc::CDCServiceProxy;
using kudu::cdc::CheckpointRequestPB;
using kudu::cdc::CheckpointResponsePB;
using kudu::cdc::GetChangesRequestPB;
using kudu::cdc::GetChangesResponsePB;
using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::itest::GetInt64Metric;
using kudu::master::CDCStreamConfigPB;
using kudu::rpc::RpcController;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {
constexpr const char* kTableName = "cdc_master_outage_table";
constexpr int kNumTablets = 4;
// Per-attempt timeout of a tablet server's master RPCs. A call that waited on a
// paused master would take at least this long.
constexpr int kMasterRpcTimeoutMs = 3000;
// Upper bound (in milliseconds) for a CDC call that does not wait on a master.
constexpr int64_t kFastCallMs = 1500;
const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
} // anonymous namespace

class CDCMasterOutageITest : public KuduTest {
 protected:
  void SetUp() override {
    KuduTest::SetUp();
    ExternalMiniClusterOptions opts;
    opts.num_masters = 3;
    opts.num_tablet_servers = 1;
    opts.extra_master_flags = { "--cdc_enabled=true" };
    opts.extra_tserver_flags = {
      Substitute("--cdc_master_rpc_timeout_ms=$0", kMasterRpcTimeoutMs),
      // Report activity, persist checkpoints and persist snapshot cursors on
      // every call, so every call has a master write to (not) wait on.
      "--cdc_active_time_report_interval_ms=0",
      "--cdc_checkpoint_persist_interval_ms=0",
      "--cdc_snapshot_cursor_persist_interval_ms=0",
      "--cdc_checkpoint_flush_interval_ms=100",
    };
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));

    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("val")->Type(KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(b.Build(&schema));
    unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
    ASSERT_OK(creator->table_name(kTableName)
              .schema(&schema)
              .add_hash_partitions({ "key" }, kNumTablets)
              .num_replicas(1)
              .Create());
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    table_id_ = table->id();
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    ASSERT_OK(KuduScanTokenBuilder(table.get()).Build(&tokens));
    for (const auto* t : tokens) {
      tablet_ids_.emplace_back(t->tablet().id());
    }
    ASSERT_EQ(kNumTablets, tablet_ids_.size());
    ResetCDCProxy();
  }

  void TearDown() override {
    if (cluster_) {
      // A failed test may have left masters paused.
      for (int i = 0; i < cluster_->num_masters(); ++i) {
        WARN_NOT_OK(cluster_->master(i)->Resume(), "could not resume master");
      }
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  void ResetCDCProxy() {
    const auto& addr = cluster_->tablet_server(0)->bound_rpc_addr();
    cdc_proxy_.reset(new CDCServiceProxy(cluster_->messenger(), addr, addr.host()));
  }

  void InsertRows(int start, int count) {
    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    client::sp::shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int i = start; i < start + count; ++i) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i));
      ASSERT_OK(session->Apply(insert.release()));
    }
    ASSERT_OK(session->Flush());
  }

  // Runs 'call' against the leader master, retrying while the leadership
  // moves.
  template<class Fn>
  void CallLeaderMaster(const Fn& call) {
    ASSERT_EVENTUALLY([&] {
      int leader_idx;
      ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
      ASSERT_OK(call(cluster_->master_proxy(leader_idx).get()));
    });
  }

  void CreateStream(CDCStreamConfigPB::SnapshotMode snapshot_mode, string* stream_id) {
    NO_FATALS(CallLeaderMaster([&](master::MasterServiceProxy* proxy) {
      master::CreateCDCStreamRequestPB req;
      master::CreateCDCStreamResponsePB resp;
      req.add_table_ids(table_id_);
      req.mutable_config()->set_snapshot_mode(snapshot_mode);
      RpcController rpc;
      rpc.set_timeout(kTimeout);
      RETURN_NOT_OK(proxy->CreateCDCStream(req, &resp, &rpc));
      if (resp.has_error()) return StatusFromPB(resp.error().status());
      *stream_id = resp.stream_id();
      return Status::OK();
    }));
  }

  void GetStreamInfo(const string& stream_id, master::GetCDCStreamInfoResponsePB* resp) {
    NO_FATALS(CallLeaderMaster([&](master::MasterServiceProxy* proxy) {
      master::GetCDCStreamInfoRequestPB req;
      req.set_stream_id(stream_id);
      resp->Clear();
      RpcController rpc;
      rpc.set_timeout(kTimeout);
      RETURN_NOT_OK(proxy->GetCDCStreamInfo(req, resp, &rpc));
      if (resp->has_error()) return StatusFromPB(resp->error().status());
      return Status::OK();
    }));
  }

  // Issues a GetChanges for 'tablet_id' and returns how long it took.
  MonoDelta TimedGetChanges(const string& stream_id, const string& tablet_id,
                            GetChangesResponsePB* resp, Status* rpc_status,
                            MonoDelta timeout = kTimeout) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_from_op_index(0);
    RpcController rpc;
    rpc.set_timeout(timeout);
    const MonoTime start = MonoTime::Now();
    *rpc_status = cdc_proxy_->GetChanges(req, resp, &rpc);
    return MonoTime::Now() - start;
  }

  MonoDelta TimedCheckpoint(const string& stream_id, const string& tablet_id,
                            int64_t op_index) {
    CheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);
    CheckpointResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(kTimeout);
    const MonoTime start = MonoTime::Now();
    CHECK_OK(cdc_proxy_->Checkpoint(req, &resp, &rpc));
    CHECK(!resp.has_error()) << resp.error().ShortDebugString();
    return MonoTime::Now() - start;
  }

  // Issues a snapshot page and returns how long it took.
  MonoDelta TimedSnapshotPage(const string& stream_id, const string& resume_key,
                              GetChangesResponsePB* resp, Status* rpc_status,
                              MonoDelta timeout = kTimeout) {
    GetChangesRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_ids_[0]);
    if (resume_key.empty()) {
      req.set_is_snapshot_start(true);
    } else {
      req.set_snapshot_resume_key(resume_key);
    }
    req.set_max_bytes(1);  // one row per page
    RpcController rpc;
    rpc.set_timeout(timeout);
    const MonoTime start = MonoTime::Now();
    *rpc_status = cdc_proxy_->GetChanges(req, resp, &rpc);
    return MonoTime::Now() - start;
  }

  void PauseAllMasters() {
    for (int i = 0; i < cluster_->num_masters(); ++i) {
      ASSERT_OK(cluster_->master(i)->Pause());
    }
  }

  void ResumeAllMasters() {
    for (int i = 0; i < cluster_->num_masters(); ++i) {
      ASSERT_OK(cluster_->master(i)->Resume());
    }
  }

  // Returns the number of checkpoint RPCs master 'idx' has handled.
  int64_t CheckpointRpcsHandled(int idx) {
    int64_t total = 0;
    for (const auto* proto :
         { &METRIC_handler_latency_kudu_master_MasterService_UpdateCDCCheckpoint,
           &METRIC_handler_latency_kudu_master_MasterService_UpdateCDCCheckpoints }) {
      int64_t v = 0;
      CHECK_OK(GetInt64Metric(cluster_->master(idx)->bound_http_hostport(),
                              &METRIC_ENTITY_server, "kudu.master", proto,
                              "total_count", &v));
      total += v;
    }
    return total;
  }

  unique_ptr<ExternalMiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;
  string table_id_;
  vector<string> tablet_ids_;
  unique_ptr<CDCServiceProxy> cdc_proxy_;
};

// With master 0 black-holed, GetChanges (which reports activity) and Checkpoint
// (which persists the checkpoint) return without waiting on it, and the
// checkpoint still reaches the master through the other masters.
TEST_F(CDCMasterOutageITest, GetChangesLatencyUnaffectedByPausedMaster) {
  string stream_id;
  NO_FATALS(CreateStream(CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 100));
  const string& tablet_id = tablet_ids_[0];

  // Warm the tablet server's stream-config cache.
  GetChangesResponsePB resp;
  Status s;
  TimedGetChanges(stream_id, tablet_id, &resp, &s);
  ASSERT_OK(s);
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  const int64_t checkpoint = resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 0);

  ASSERT_OK(cluster_->master(0)->Pause());
  for (int i = 0; i < 5; ++i) {
    resp.Clear();
    const MonoDelta get_changes = TimedGetChanges(stream_id, tablet_id, &resp, &s);
    ASSERT_OK(s);
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
    EXPECT_LT(get_changes.ToMilliseconds(), kFastCallMs) << "GetChanges waited on a paused master";
    EXPECT_LT(TimedCheckpoint(stream_id, tablet_id, checkpoint).ToMilliseconds(), kFastCallMs)
        << "Checkpoint waited on a paused master";
  }

  // The checkpoint is durable on the (possibly new) leader master.
  ASSERT_EVENTUALLY([&] {
    master::GetCDCStreamInfoResponsePB info;
    NO_FATALS(GetStreamInfo(stream_id, &info));
    const auto& cps = info.stream().tablet_checkpoints();
    ASSERT_EQ(1, cps.count(tablet_id));
    ASSERT_EQ(checkpoint, cps.at(tablet_id));
  });
  ASSERT_OK(cluster_->master(0)->Resume());
}

// Checkpoint persists do not hold CDC service threads: with every master
// paused, a burst of Checkpoint calls (more than there are service threads)
// does not keep a following GetChanges from being served promptly.
TEST_F(CDCMasterOutageITest, CheckpointPersistsDoNotStarveGetChanges) {
  string stream_id;
  NO_FATALS(CreateStream(CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 100));
  for (const auto& tablet_id : tablet_ids_) {
    GetChangesResponsePB resp;
    Status s;
    TimedGetChanges(stream_id, tablet_id, &resp, &s);
    ASSERT_OK(s);
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  }

  NO_FATALS(PauseAllMasters());
  // Twice the default --rpc_num_service_threads.
  constexpr int kNumCheckpoints = 40;
  CountDownLatch done(kNumCheckpoints);
  vector<unique_ptr<CheckpointRequestPB>> reqs;
  vector<unique_ptr<CheckpointResponsePB>> resps;
  vector<unique_ptr<RpcController>> rpcs;
  for (int i = 0; i < kNumCheckpoints; ++i) {
    reqs.emplace_back(new CheckpointRequestPB);
    reqs.back()->set_stream_id(stream_id);
    reqs.back()->set_tablet_id(tablet_ids_[i % kNumTablets]);
    reqs.back()->set_op_index(1);
    resps.emplace_back(new CheckpointResponsePB);
    rpcs.emplace_back(new RpcController);
    rpcs.back()->set_timeout(kTimeout);
    cdc_proxy_->CheckpointAsync(*reqs.back(), resps.back().get(), rpcs.back().get(),
                                [&done]() { done.CountDown(); });
  }
  done.Wait();
  for (const auto& rpc : rpcs) {
    ASSERT_OK(rpc->status());
  }

  GetChangesResponsePB resp;
  Status s;
  const MonoDelta elapsed = TimedGetChanges(stream_id, tablet_ids_[0], &resp, &s,
                                            MonoDelta::FromSeconds(5));
  ASSERT_OK(s);
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  EXPECT_LT(elapsed.ToMilliseconds(), kFastCallMs);
  NO_FATALS(ResumeAllMasters());
}

// Snapshot pages do not wait on a paused master while they hold a scan slot:
// the start and the intermediate pages queue their cursor persist, and the
// final page, which persists synchronously, gives up within the client's
// deadline. The completion, once the client acknowledged the final page,
// reaches the master once it is back. A start needs the master's durable
// snapshot cursor (to continue an unfinished snapshot rather than take a new
// one): without the master it fails within its deadline as retryable.
TEST_F(CDCMasterOutageITest, SnapshotPageNotBlockedOnMaster) {
  string stream_id;
  NO_FATALS(CreateStream(CDCStreamConfigPB::INITIAL_AND_CONTINUE, &stream_id));
  NO_FATALS(InsertRows(0, 200));
  // Warm the tablet server's stream-config cache.
  {
    GetChangesResponsePB resp;
    Status s;
    TimedGetChanges(stream_id, tablet_ids_[0], &resp, &s);
    ASSERT_OK(s);
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  }

  NO_FATALS(PauseAllMasters());
  {
    GetChangesResponsePB resp;
    Status s;
    const MonoDelta timeout = MonoDelta::FromSeconds(5);
    const MonoDelta elapsed = TimedSnapshotPage(stream_id, "", &resp, &s, timeout);
    ASSERT_OK(s);
    ASSERT_TRUE(resp.has_error()) << resp.ShortDebugString();
    EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code())
        << resp.error().ShortDebugString();
    EXPECT_TRUE(resp.error().is_retryable());
    EXPECT_EQ(0, resp.records_size());
    EXPECT_LT(elapsed.ToMilliseconds(), timeout.ToMilliseconds());
  }
  // The start, with the masters up. The pages after it do not need them.
  NO_FATALS(ResumeAllMasters());
  string resume_key;
  {
    GetChangesResponsePB resp;
    Status s;
    TimedSnapshotPage(stream_id, "", &resp, &s);
    ASSERT_OK(s);
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
    ASSERT_FALSE(resp.snapshot_done());
    resume_key = resp.snapshot_resume_key();
    ASSERT_FALSE(resume_key.empty());
  }
  NO_FATALS(PauseAllMasters());
  int pages = 1;
  bool done = false;
  int64_t streaming_start = 0;
  while (!done) {
    GetChangesResponsePB resp;
    Status s;
    const MonoDelta timeout = MonoDelta::FromSeconds(5);
    const MonoDelta elapsed = TimedSnapshotPage(stream_id, resume_key, &resp, &s, timeout);
    SCOPED_TRACE(Substitute("page $0", pages));
    ASSERT_OK(s);
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
    done = resp.snapshot_done();
    if (done) {
      streaming_start = resp.snapshot_streaming_start_op_index();
      EXPECT_LT(elapsed.ToMilliseconds(), timeout.ToMilliseconds());
    } else {
      EXPECT_LT(elapsed.ToMilliseconds(), kFastCallMs) << "snapshot page " << pages
                                    << " waited on a paused master";
      ASSERT_FALSE(resp.snapshot_resume_key().empty());
      resume_key = resp.snapshot_resume_key();
    }
    ++pages;
    ASSERT_LT(pages, 1000);
  }
  ASSERT_GT(pages, 2);
  ASSERT_GT(streaming_start, 0);
  // The client acknowledges the last page by checkpointing at the streaming
  // start point, which does not wait on the master either.
  EXPECT_LT(TimedCheckpoint(stream_id, tablet_ids_[0], streaming_start).ToMilliseconds(),
            kFastCallMs);
  NO_FATALS(ResumeAllMasters());

  // The completed snapshot reaches the master: the checkpoint is at the
  // streaming start point, and the cursor is no longer offered for a resume.
  ASSERT_EVENTUALLY([&] {
    master::GetCDCStreamInfoResponsePB info;
    NO_FATALS(GetStreamInfo(stream_id, &info));
    const auto& cps = info.stream().tablet_checkpoints();
    ASSERT_EQ(1, cps.count(tablet_ids_[0]));
    ASSERT_EQ(streaming_start, cps.at(tablet_ids_[0]));
    for (const auto& cur : info.stream().tablet_snapshot_cursors()) {
      ASSERT_NE(tablet_ids_[0], cur.tablet_id()) << "the snapshot is still active";
    }
  });
}

// With a cold stream-config cache and every master paused, concurrent
// GetChanges calls for the tablets of one stream all respond within their
// deadline instead of queueing one master fetch after another, and a failed
// fetch is not retried by every following call.
TEST_F(CDCMasterOutageITest, ColdConfigCacheAllMastersPaused) {
  string stream_id;
  NO_FATALS(CreateStream(CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 100));

  // Restart the tablet server to empty its config cache.
  cluster_->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());
  ASSERT_OK(cluster_->WaitForTabletsRunning(cluster_->tablet_server(0), kNumTablets,
                                            kTimeout));
  ResetCDCProxy();
  NO_FATALS(PauseAllMasters());

  const MonoDelta kRpcTimeout = MonoDelta::FromSeconds(5);
  const auto get_changes_all_tablets = [&](vector<MonoDelta>* elapsed) {
    vector<Status> statuses(kNumTablets);
    elapsed->assign(kNumTablets, MonoDelta());
    vector<thread> threads;
    for (int i = 0; i < kNumTablets; ++i) {
      threads.emplace_back([&, i]() {
        GetChangesResponsePB resp;
        (*elapsed)[i] = TimedGetChanges(stream_id, tablet_ids_[i], &resp, &statuses[i],
                                        kRpcTimeout);
        if (statuses[i].ok() && resp.has_error()) {
          // A call that could not get the config in time is shed as retryable.
          if (resp.error().code() != CDCErrorPB::SERVER_TOO_BUSY) {
            statuses[i] = StatusFromPB(resp.error().status());
          }
        }
      });
    }
    for (auto& t : threads) {
      t.join();
    }
    for (int i = 0; i < kNumTablets; ++i) {
      SCOPED_TRACE(Substitute("tablet $0", i));
      ASSERT_OK(statuses[i]);
    }
  };

  vector<MonoDelta> elapsed;
  NO_FATALS(get_changes_all_tablets(&elapsed));
  // The failed fetch is remembered for a while: the next calls do not wait on
  // the paused masters again.
  NO_FATALS(get_changes_all_tablets(&elapsed));
  for (int i = 0; i < kNumTablets; ++i) {
    EXPECT_LT(elapsed[i].ToMilliseconds(), kFastCallMs) << "tablet " << i;
  }
  NO_FATALS(ResumeAllMasters());
}

// A config fetch that failed only because its caller's deadline ran out does
// not let the next calls go on without the config: they are shed until the
// config can be fetched, so a NEVER stream never serves a snapshot.
TEST_F(CDCMasterOutageITest, FailedConfigFetchNotServedWithoutConfig) {
  string stream_id;
  NO_FATALS(CreateStream(CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 100));

  // With all masters up, delay the config fetch past the deadline of the first
  // call, which is the first call for the stream on this tablet server.
  auto* ts = cluster_->tablet_server(0);
  ASSERT_OK(cluster_->SetFlag(ts, "cdc_inject_latency_before_stream_config_fetch_ms",
                              "2000"));
  GetChangesResponsePB resp;
  Status s;
  TimedGetChanges(stream_id, tablet_ids_[0], &resp, &s, MonoDelta::FromMilliseconds(500));
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_OK(cluster_->SetFlag(ts, "cdc_inject_latency_before_stream_config_fetch_ms", "0"));

  // This call waits for the first call's fetch, and finds it failed.
  resp.Clear();
  TimedSnapshotPage(stream_id, "", &resp, &s);
  ASSERT_OK(s);
  ASSERT_TRUE(resp.has_error());
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code())
      << resp.error().ShortDebugString();
  EXPECT_EQ(0, resp.records_size());

  // Once the failure is forgotten, the config is fetched and the snapshot is
  // refused.
  ASSERT_EVENTUALLY([&] {
    resp.Clear();
    TimedSnapshotPage(stream_id, "", &resp, &s);
    ASSERT_OK(s);
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(CDCErrorPB::UNKNOWN_ERROR, resp.error().code())
        << resp.error().ShortDebugString();
  });
  EXPECT_STR_CONTAINS(resp.error().status().message(), "snapshot_mode=NEVER");
  EXPECT_EQ(0, resp.records_size());
}

// The call whose own config fetch was cut short by its deadline is not served
// without the config either. With master 0 paused and a cold config cache, the
// first fetch attempt goes to master 0 and runs into the deadline before a
// healthy master is tried: the call is shed rather than serving a snapshot of
// a NEVER stream.
TEST_F(CDCMasterOutageITest, FetchCutByOwnDeadlineNotServedWithoutConfig) {
  string stream_id;
  NO_FATALS(CreateStream(CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 100));

  // Restart the tablet server to empty its config cache and to make it try
  // master 0 first.
  cluster_->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());
  ASSERT_OK(cluster_->WaitForTabletsRunning(cluster_->tablet_server(0), kNumTablets,
                                            kTimeout));
  ResetCDCProxy();
  ASSERT_OK(cluster_->master(0)->Pause());

  // The client timeout is no longer than the per-attempt master timeout, so
  // the attempt on master 0 uses up the call's budget.
  GetChangesResponsePB resp;
  Status s;
  TimedSnapshotPage(stream_id, "", &resp, &s,
                    MonoDelta::FromMilliseconds(kMasterRpcTimeoutMs));
  ASSERT_OK(s);
  ASSERT_TRUE(resp.has_error()) << "served without the config: "
                                << resp.records_size() << " records";
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code())
      << resp.error().ShortDebugString();
  EXPECT_EQ(0, resp.records_size());

  // The tablet server moves on to the next master and, once the failure is
  // forgotten, gets the config from it and refuses the snapshot.
  ASSERT_EVENTUALLY([&] {
    resp.Clear();
    TimedSnapshotPage(stream_id, "", &resp, &s);
    ASSERT_OK(s);
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(CDCErrorPB::UNKNOWN_ERROR, resp.error().code())
        << resp.error().ShortDebugString();
  });
  EXPECT_STR_CONTAINS(resp.error().status().message(), "snapshot_mode=NEVER");
  EXPECT_EQ(0, resp.records_size());
  ASSERT_OK(cluster_->master(0)->Resume());
}

// Same as above with all masters healthy: the call's deadline passes before
// its config fetch reaches any master.
TEST_F(CDCMasterOutageITest, FetchPastOwnDeadlineNotServedWithoutConfig) {
  string stream_id;
  NO_FATALS(CreateStream(CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 100));

  // Reserve half of the client budget, and delay the fetch past the other
  // half, so the call still has time to answer before its client deadline.
  auto* ts = cluster_->tablet_server(0);
  ASSERT_OK(cluster_->SetFlag(ts, "cdc_read_safe_deadline_ratio", "0.5"));
  ASSERT_OK(cluster_->SetFlag(ts, "cdc_inject_latency_before_stream_config_fetch_ms",
                              "2500"));
  GetChangesResponsePB resp;
  Status s;
  TimedSnapshotPage(stream_id, "", &resp, &s, MonoDelta::FromMilliseconds(4000));
  ASSERT_OK(s);
  ASSERT_TRUE(resp.has_error()) << "served without the config: "
                                << resp.records_size() << " records";
  EXPECT_EQ(CDCErrorPB::SERVER_TOO_BUSY, resp.error().code())
      << resp.error().ShortDebugString();
  EXPECT_EQ(0, resp.records_size());
}

// After it found the leader master once, a tablet server sends its checkpoint
// persists to that master only, instead of trying the masters in order.
TEST_F(CDCMasterOutageITest, PersistCheckpointUsesCachedLeader) {
  // Move the leadership off master 0, the first master in the tablet server's
  // list.
  int leader_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  if (leader_idx == 0) {
    cluster_->master(0)->Shutdown();
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
      ASSERT_NE(0, leader_idx);
    });
    ASSERT_OK(cluster_->master(0)->Restart());
  }

  string stream_id;
  NO_FATALS(CreateStream(CDCStreamConfigPB::NEVER, &stream_id));
  NO_FATALS(InsertRows(0, 100));
  const string& tablet_id = tablet_ids_[0];
  GetChangesResponsePB resp;
  Status s;
  TimedGetChanges(stream_id, tablet_id, &resp, &s);
  ASSERT_OK(s);
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  const int64_t checkpoint = resp.checkpoint_op_index();
  ASSERT_GT(checkpoint, 1);

  // Lets the tablet server find the leader.
  const auto checkpoint_and_wait = [&](int64_t op_index) {
    TimedCheckpoint(stream_id, tablet_id, op_index);
    ASSERT_EVENTUALLY([&] {
      master::GetCDCStreamInfoResponsePB info;
      NO_FATALS(GetStreamInfo(stream_id, &info));
      ASSERT_EQ(op_index, info.stream().tablet_checkpoints().at(tablet_id));
    });
  };
  NO_FATALS(checkpoint_and_wait(1));
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  vector<int64_t> before(cluster_->num_masters());
  for (int i = 0; i < cluster_->num_masters(); ++i) {
    before[i] = CheckpointRpcsHandled(i);
  }
  NO_FATALS(checkpoint_and_wait(checkpoint));
  int leader_now;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_now));
  ASSERT_EQ(leader_idx, leader_now) << "the leadership moved during the test";
  for (int i = 0; i < cluster_->num_masters(); ++i) {
    if (i == leader_idx) {
      EXPECT_GT(CheckpointRpcsHandled(i), before[i]);
    } else {
      EXPECT_EQ(before[i], CheckpointRpcsHandled(i))
          << "master " << i << " is not the leader";
    }
  }
}

} // namespace kudu
