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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/walltime.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/server/monitored_task.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_create_stream_fail_checkpoint_idx);
DECLARE_int64(cdc_max_staleness_ms);
DECLARE_int64(cdc_stream_expiry_ms);

using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::map;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace master {

// Collects every per-(stream, tablet) CDC checkpoint row from the sys catalog so
// tests can assert on the row store directly (rather than only through the RPC
// surface). Used to prove rows are actually created, migrated, and deleted.
class CountingCheckpointVisitor : public CDCTabletCheckpointVisitor {
 public:
  Status Visit(const string& /*entry_id*/,
               const SysCDCTabletCheckpointEntryPB& metadata) override {
    rows.push_back(metadata);
    return Status::OK();
  }
  std::vector<SysCDCTabletCheckpointEntryPB> rows;
};

// Collects every CDC stream row from the sys catalog so tests can assert on the
// stream-row store directly -- in particular that the two-phase delete marks a
// row DELETING before the reap removes it, and that the reap eventually removes
// it entirely.
class CountingStreamVisitor : public CDCStreamVisitor {
 public:
  Status Visit(const string& stream_id,
               const SysCDCStreamEntryPB& metadata) override {
    streams[stream_id] = metadata;
    return Status::OK();
  }
  map<string, SysCDCStreamEntryPB> streams;
};

// A minimal MonitoredTask whose state can be set freely. Used by the DR-010 F-2
// guard test to inject a "pending" RELEASE task and advance it to terminal state
// without needing real tablet servers.
class MockMonitoredTask : public MonitoredTask {
 public:
  explicit MockMonitoredTask(State initial = kStateRunning)
      : state_(initial) {}
  void Abort() override { state_.store(kStateAborted); }
  State state() const override { return static_cast<State>(state_.load()); }
  void set_state(State s) { state_.store(s); }
  std::string type_name() const override { return "MockRelease"; }
  std::string description() const override { return "MockMonitoredTask for F-2 test"; }
  MonoTime start_timestamp() const override { return MonoTime(); }
  MonoTime completion_timestamp() const override { return MonoTime(); }
 private:
  std::atomic<int> state_;
};

class CDCManagerTest : public KuduTest {
 protected:
  void SetUp() override {
    KuduTest::SetUp();
    FLAGS_catalog_manager_check_ts_count_for_create_table = false;
    // The background CDC maintenance task fires once shortly after a master
    // becomes leader and then waits this interval. Push the interval far out so
    // that single early pass is the only automatic one during a test; the reap
    // tests then drive maintenance deterministically via RunMaintenance().
    FLAGS_cdc_bg_scan_interval_ms = 3600 * 1000;

    mini_master_.reset(new MiniMaster(GetTestPath("Master"), HostPort("127.0.0.1", 0)));
    ASSERT_OK(mini_master_->Start());
    master_ = mini_master_->master();
    ASSERT_OK(master_->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
        MonoDelta::FromSeconds(90)));

    MessengerBuilder bld("Client");
    ASSERT_OK(bld.Build(&client_messenger_));
    proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                        mini_master_->bound_rpc_addr().host()));
  }

  void TearDown() override {
    mini_master_->Shutdown();
    KuduTest::TearDown();
  }

  // Creates a real user table with 'num_tablets' range-partitioned tablets and
  // returns its generated table id plus the ids of its tablets. CreateCDCStream
  // now validates the table exists and is running (L6) and pushes an initial
  // retention barrier over the table's tablets (L5), so streams in these tests
  // must be created over a real table rather than a synthetic id. No tablet
  // servers are running, so the tablets stay unassigned (no consensus config);
  // that is fine here -- the barrier fan-out simply no-ops on such tablets while
  // the durable sys-catalog rows this suite asserts on are still written.
  Status CreateTable(const string& name,
                     int num_tablets,
                     string* table_id,
                     vector<string>* tablet_ids) {
    CreateTableRequestPB req;
    CreateTableResponsePB resp;
    req.set_name(name);
    Schema schema = GetSimpleTestSchema();
    RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
    // num_tablets-1 split points on the INT32 "key" column yield num_tablets
    // range partitions.
    if (num_tablets > 1) {
      RowOperationsPBEncoder enc(req.mutable_split_rows_range_bounds());
      for (int i = 1; i < num_tablets; ++i) {
        KuduPartialRow row(&schema);
        RETURN_NOT_OK(row.SetInt32("key", i * 1000));
        enc.Add(RowOperationsPB::SPLIT_ROW, row);
      }
    }

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->CreateTable(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    *table_id = resp.table_id();

    // Read back the tablet ids directly from the catalog (the table is RUNNING
    // synchronously once its tablet metadata is written, even with no tservers).
    scoped_refptr<TableInfo> table;
    {
      CatalogManager::ScopedLeaderSharedLock l(
          mini_master_->master()->catalog_manager());
      RETURN_NOT_OK(l.first_failed_status());
      RETURN_NOT_OK(mini_master_->master()->catalog_manager()->GetTableInfo(
          *table_id, &table));
    }
    if (!table) {
      return Status::NotFound("table not found after create");
    }
    vector<scoped_refptr<TabletInfo>> tablets;
    table->GetAllTablets(&tablets);
    tablet_ids->clear();
    for (const auto& t : tablets) {
      tablet_ids->emplace_back(t->id());
    }
    return Status::OK();
  }

  // Convenience: create a single-tablet table and a CDC stream over it, returning
  // the table id, the (single) tablet id, and the stream id.
  Status CreateTableAndStream(const string& name,
                              string* table_id,
                              string* tablet_id,
                              string* stream_id) {
    vector<string> tablet_ids;
    RETURN_NOT_OK(CreateTable(name, /*num_tablets=*/1, table_id, &tablet_ids));
    if (tablet_ids.size() != 1) {
      return Status::IllegalState("expected exactly one tablet");
    }
    *tablet_id = tablet_ids[0];
    return CreateStream(*table_id, stream_id);
  }

  Status CreateStream(const string& table_id, string* stream_id) {
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.add_table_ids(table_id);

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->CreateCDCStream(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    *stream_id = resp.stream_id();
    return Status::OK();
  }

  Status DeleteStream(const string& stream_id) {
    DeleteCDCStreamRequestPB req;
    DeleteCDCStreamResponsePB resp;
    req.set_stream_id(stream_id);

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->DeleteCDCStream(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  // Hard-deletes (drops) a table by name. With reserve_seconds omitted and the
  // default --default_deleted_table_reserve_seconds=0, SoftDeleteTableRpc routes
  // to the terminal DeleteTable path (state -> REMOVED), not a recallable soft
  // delete.
  Status DropTable(const string& name) {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    req.mutable_table()->set_table_name(name);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->DeleteTable(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  // Soft-deletes a table by name, reserving it for 'reserve_seconds' (recallable).
  Status SoftDeleteTable(const string& name, uint32_t reserve_seconds) {
    DeleteTableRequestPB req;
    DeleteTableResponsePB resp;
    req.mutable_table()->set_table_name(name);
    req.set_reserve_seconds(reserve_seconds);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->DeleteTable(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  // Recalls a soft-deleted table by id. The table resumes its previous name
  // (no rename requested), transitioning back to RUNNING so it is usable again.
  Status RecallTable(const string& table_id) {
    RecallDeletedTableRequestPB req;
    RecallDeletedTableResponsePB resp;
    req.mutable_table()->set_table_id(table_id);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->RecallDeletedTable(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  Status ListStreams(const string& table_id, ListCDCStreamsResponsePB* resp) {
    ListCDCStreamsRequestPB req;
    if (!table_id.empty()) {
      req.set_table_id(table_id);
    }

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->ListCDCStreams(req, resp, &rpc));
    if (resp->has_error()) {
      return StatusFromPB(resp->error().status());
    }
    return Status::OK();
  }

  Status GetStreamInfo(const string& stream_id, GetCDCStreamInfoResponsePB* resp) {
    GetCDCStreamInfoRequestPB req;
    req.set_stream_id(stream_id);

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->GetCDCStreamInfo(req, resp, &rpc));
    if (resp->has_error()) {
      return StatusFromPB(resp->error().status());
    }
    return Status::OK();
  }

  Status UpdateCheckpoint(const string& stream_id,
                          const string& tablet_id,
                          int64_t op_index) {
    UpdateCDCCheckpointRequestPB req;
    UpdateCDCCheckpointResponsePB resp;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->UpdateCDCCheckpoint(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  // Reads all per-(stream, tablet) checkpoint rows directly from the sys catalog.
  // Always resolves the master via mini_master_ so it stays valid after restarts.
  Status VisitCheckpointRows(std::vector<SysCDCTabletCheckpointEntryPB>* rows) {
    CountingCheckpointVisitor visitor;
    RETURN_NOT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                      ->VisitCDCTabletCheckpoints(&visitor));
    *rows = std::move(visitor.rows);
    return Status::OK();
  }

  // Number of checkpoint rows belonging to 'stream_id' (empty = all streams).
  Status CountCheckpointRows(const string& stream_id, int* count) {
    std::vector<SysCDCTabletCheckpointEntryPB> rows;
    RETURN_NOT_OK(VisitCheckpointRows(&rows));
    int n = 0;
    for (const auto& r : rows) {
      if (stream_id.empty() || r.stream_id() == stream_id) {
        ++n;
      }
    }
    *count = n;
    return Status::OK();
  }

  // Reads all CDC stream rows directly from the sys catalog, keyed by stream_id.
  // Always resolves the master via mini_master_ so it stays valid after restarts.
  Status VisitStreamRows(map<string, SysCDCStreamEntryPB>* streams) {
    CountingStreamVisitor visitor;
    RETURN_NOT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                      ->VisitCDCStreams(&visitor));
    *streams = std::move(visitor.streams);
    return Status::OK();
  }

  // Number of stream rows for 'stream_id' currently in the sys catalog (0 or 1).
  // Kept comma-free at the call site so it can be used inside ASSERT_EVENTUALLY.
  Status CountStreamRows(const string& stream_id, int* count) {
    map<string, SysCDCStreamEntryPB> streams;
    RETURN_NOT_OK(VisitStreamRows(&streams));
    *count = static_cast<int>(streams.count(stream_id));
    return Status::OK();
  }

  // Runs one CDC maintenance pass (which begins with the two-phase-delete reap)
  // on the current leader master directly, as the background task would.
  void RunMaintenance() {
    mini_master_->master()->catalog_manager()->RunCDCStreamMaintenance();
  }

  // Runs only phase 2 (the two-phase-delete reap), skipping the rest of the
  // maintenance pass. Tests that assert a surviving stream keeps its checkpoint
  // rows must use this: a full pass prunes any tablet absent from tablet_map_ as
  // a dropped partition, and this fixture creates streams over tablet ids that
  // have no backing TabletInfo, so a full pass would delete every checkpoint row.
  void RunReap() {
    mini_master_->master()->catalog_manager()->ReapDeletedCDCStreams();
  }

  // Runs only the dropped-table scan (the L2 backstop), skipping the reap and the
  // rest of the maintenance pass. Lets tests isolate the ACTIVE -> DELETING
  // marking decision from reap / barrier-recompute / dropped-partition pruning.
  void RunMarkDeletingForDroppedTables() {
    mini_master_->master()->catalog_manager()->MarkDeletingStreamsForDroppedTables();
  }

  shared_ptr<Messenger> client_messenger_;
  unique_ptr<MiniMaster> mini_master_;
  Master* master_;
  unique_ptr<MasterServiceProxy> proxy_;
};

TEST_F(CDCManagerTest, CreateStream_Success) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("test-table-1", &table_id, &tablet_id, &stream_id));
  EXPECT_FALSE(stream_id.empty());
}

// L6: CreateCDCStream must reject a table that does not exist rather than
// persisting a stream (and retention barriers) that can never make progress.
TEST_F(CDCManagerTest, CreateStream_RejectsMissingTable) {
  string stream_id;
  Status s = CreateStream("no-such-table", &stream_id);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();

  // Nothing must have been persisted for the rejected stream.
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  EXPECT_TRUE(streams.empty());
}

// L6: a soft-deleted table is on its way out; a new CDC stream over it must be
// rejected (InvalidArgument) rather than pinning WAL on tablets being removed.
TEST_F(CDCManagerTest, CreateStream_RejectsSoftDeletedTable) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("soft-doomed", /*num_tablets=*/1, &table_id, &tablet_ids));

  // Soft-delete the table (reserve it rather than removing it outright). The
  // soft-delete path (MoveToSoftDeletedContainer) looks the table up by name in
  // normalized_table_names_map_, so the request must carry the table name, not
  // just the id.
  DeleteTableRequestPB req;
  DeleteTableResponsePB resp;
  req.mutable_table()->set_table_name("soft-doomed");
  req.set_reserve_seconds(3600);
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(proxy_->DeleteTable(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().status().message();

  string stream_id;
  Status s = CreateStream(table_id, &stream_id);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

// L6: a hard-deleted table (REMOVED state) stays in table_ids_map_ for up to
// metadata_for_deleted_table_and_tablet_reserved_secs (default 1h) before the
// background GC removes its entry. During that window, CreateCDCStream must
// detect is_deleted() == true and return NotFound rather than persisting a
// stream over a gone table.
TEST_F(CDCManagerTest, CreateStream_RejectsRemovedTable) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("gone-table", /*num_tablets=*/1, &table_id, &tablet_ids));

  // Hard-delete the table (no reserve_seconds -> default -> routes to the
  // terminal DeleteTable path: state -> REMOVED). The table id stays in
  // table_ids_map_ until the cleanup GC runs, so the is_deleted() check is
  // what catches this case.
  ASSERT_OK(DropTable("gone-table"));

  string stream_id;
  Status s = CreateStream(table_id, &stream_id);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();

  // Nothing must have been persisted for the rejected stream.
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  EXPECT_TRUE(streams.empty());
}

TEST_F(CDCManagerTest, CreateStream_PersistsSysCatalog) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("test-table-1", &table_id, &tablet_id, &stream_id));
  ASSERT_FALSE(stream_id.empty());

  // Restart the master and verify the stream survives.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));

  // Recreate the proxy after restart.
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));

  GetCDCStreamInfoResponsePB resp;
  ASSERT_OK(GetStreamInfo(stream_id, &resp));
  ASSERT_TRUE(resp.has_stream());
  EXPECT_EQ(stream_id, resp.stream().stream_id());
  ASSERT_EQ(1, resp.stream().table_ids_size());
  EXPECT_EQ(table_id, resp.stream().table_ids(0));
}

// L5: creating a stream must establish an initial retention barrier for each of
// the table's tablets -- a durable per-tablet checkpoint row at op_index 0 --
// so WAL is pinned from stream creation until the consumer's first checkpoint,
// closing the create-to-first-checkpoint GC race. The row must survive failover
// (it is what makes a new leader keep pinning WAL) and its activity/advance
// timestamps must be set so an unused stream can still auto-expire.
TEST_F(CDCManagerTest, CreateStream_PushesInitialBarrier) {
  const int kNumTablets = 3;
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("barrier-table", kNumTablets, &table_id, &tablet_ids));
  ASSERT_EQ(kNumTablets, tablet_ids.size());

  string stream_id;
  ASSERT_OK(CreateStream(table_id, &stream_id));

  // One initial checkpoint row per tablet, each at op_index 0 with activity and
  // advance timestamps stamped.
  vector<SysCDCTabletCheckpointEntryPB> rows;
  ASSERT_OK(VisitCheckpointRows(&rows));
  int matched = 0;
  for (const auto& r : rows) {
    if (r.stream_id() != stream_id) {
      continue;
    }
    ++matched;
    ASSERT_TRUE(r.has_op_index());
    EXPECT_EQ(0, r.op_index());
    EXPECT_GT(r.last_active_time_micros(), 0);
    EXPECT_GT(r.last_checkpoint_advance_time_micros(), 0);
  }
  EXPECT_EQ(kNumTablets, matched);

  // The barrier rows are durable: a new leader reloads them and keeps owning the
  // barrier without having seen the CreateCDCStream RPC.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));

  int rows_after = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows_after));
  EXPECT_EQ(kNumTablets, rows_after);
}

// L5 + L1: the initial barrier rows are reclaimed by the two-phase-delete reap,
// exactly like consumer-created rows -- creating them at stream creation does
// not introduce a leak.
TEST_F(CDCManagerTest, CreateStream_InitialBarrierReapedOnDelete) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("reaped-table", &table_id, &tablet_id, &stream_id));

  int rows = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  ASSERT_EQ(1, rows);

  ASSERT_OK(DeleteStream(stream_id));
  RunMaintenance();

  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(0, rows);
}

TEST_F(CDCManagerTest, ListStreams_Empty) {
  ListCDCStreamsResponsePB resp;
  ASSERT_OK(ListStreams("", &resp));
  EXPECT_EQ(0, resp.streams_size());
}

TEST_F(CDCManagerTest, ListStreams_FilterByTable) {
  string table_a, tablet_a, stream_id_1;
  string table_b, tablet_b, stream_id_2;
  ASSERT_OK(CreateTableAndStream("table-A", &table_a, &tablet_a, &stream_id_1));
  ASSERT_OK(CreateTableAndStream("table-B", &table_b, &tablet_b, &stream_id_2));

  // List all -- should get both.
  ListCDCStreamsResponsePB resp_all;
  ASSERT_OK(ListStreams("", &resp_all));
  EXPECT_EQ(2, resp_all.streams_size());

  // Filter by table-A -- should get only stream_id_1.
  ListCDCStreamsResponsePB resp_a;
  ASSERT_OK(ListStreams(table_a, &resp_a));
  ASSERT_EQ(1, resp_a.streams_size());
  EXPECT_EQ(stream_id_1, resp_a.streams(0).stream_id());

  // Filter by table-B -- should get only stream_id_2.
  ListCDCStreamsResponsePB resp_b;
  ASSERT_OK(ListStreams(table_b, &resp_b));
  ASSERT_EQ(1, resp_b.streams_size());
  EXPECT_EQ(stream_id_2, resp_b.streams(0).stream_id());
}

TEST_F(CDCManagerTest, GetStreamInfo_Success) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("my-table", &table_id, &tablet_id, &stream_id));

  GetCDCStreamInfoResponsePB resp;
  ASSERT_OK(GetStreamInfo(stream_id, &resp));
  ASSERT_TRUE(resp.has_stream());
  EXPECT_EQ(stream_id, resp.stream().stream_id());
  ASSERT_EQ(1, resp.stream().table_ids_size());
  EXPECT_EQ(table_id, resp.stream().table_ids(0));
}

TEST_F(CDCManagerTest, GetStreamInfo_NotFound) {
  GetCDCStreamInfoResponsePB resp;
  Status s = GetStreamInfo("nonexistent-stream-id", &resp);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(CDCManagerTest, DeleteStream_Success) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-1", &table_id, &tablet_id, &stream_id));

  // Delete it. This is phase 1 of the two-phase delete: the stream is marked
  // DELETING and immediately stops being served.
  ASSERT_OK(DeleteStream(stream_id));

  // Verify it's gone from the serving surface (GetCDCStreamInfo hides non-ACTIVE
  // streams), even though the reap has not yet removed its row.
  GetCDCStreamInfoResponsePB resp;
  Status s = GetStreamInfo(stream_id, &resp);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(CDCManagerTest, DeleteStream_NotFound) {
  Status s = DeleteStream("nonexistent-stream-id");
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

// Regression test for E3: DeleteCDCStream must persist the removal to
// sys_catalog before dropping the in-memory entry, so a deleted stream does not
// resurrect (and re-pin WAL/MVCC anchors on its tablets) when the master reloads
// its catalog. Verifies the durable removal actually took effect by restarting
// the master after a successful delete.
TEST_F(CDCManagerTest, DeleteStream_PersistsAcrossRestart) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-1", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(DeleteStream(stream_id));

  // Restart the master so the catalog is reloaded from sys_catalog.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));

  // The stream must stay gone -- it must not reload from sys_catalog.
  GetCDCStreamInfoResponsePB resp;
  Status s = GetStreamInfo(stream_id, &resp);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(CDCManagerTest, UpdateCheckpoint_Success) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-1", &table_id, &tablet_id, &stream_id));

  // The stream starts with one initial (op_index 0) row for the table's tablet.
  // Advancing that tablet's checkpoint updates the same row in place.
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 42));

  // Verify checkpoint is stored.
  GetCDCStreamInfoResponsePB resp;
  ASSERT_OK(GetStreamInfo(stream_id, &resp));
  ASSERT_TRUE(resp.has_stream());
  ASSERT_EQ(1, resp.stream().tablet_checkpoints_size());
  auto it = resp.stream().tablet_checkpoints().find(tablet_id);
  ASSERT_NE(it, resp.stream().tablet_checkpoints().end());
  EXPECT_EQ(42, it->second);
}

TEST_F(CDCManagerTest, UpdateCheckpoint_StreamNotFound) {
  Status s = UpdateCheckpoint("nonexistent", "tablet-1", 10);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

// E7: the persisted checkpoint must be monotonic. A new leader whose local WAL
// anchor lags can report a lower op_index than one already stored; the master
// must keep the higher value (store max) rather than moving the durable
// checkpoint backward and retaining more WAL than necessary.
TEST_F(CDCManagerTest, UpdateCheckpoint_IsMonotonic) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-1", &table_id, &tablet_id, &stream_id));

  // Advance to a high checkpoint (from the initial op_index 0).
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 100));

  auto stored = [&](int64_t* out) -> Status {
    GetCDCStreamInfoResponsePB resp;
    RETURN_NOT_OK(GetStreamInfo(stream_id, &resp));
    auto it = resp.stream().tablet_checkpoints().find(tablet_id);
    if (it == resp.stream().tablet_checkpoints().end()) {
      return Status::NotFound("no checkpoint for tablet");
    }
    *out = it->second;
    return Status::OK();
  };

  int64_t got = -1;
  ASSERT_OK(stored(&got));
  EXPECT_EQ(100, got);

  // A lagging leader reports a lower op_index: it must be ignored, not stored.
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 50));
  ASSERT_OK(stored(&got));
  EXPECT_EQ(100, got) << "checkpoint moved backward";

  // Re-reporting the same index is a no-op for the value.
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 100));
  ASSERT_OK(stored(&got));
  EXPECT_EQ(100, got);

  // A genuinely higher index still advances.
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 150));
  ASSERT_OK(stored(&got));
  EXPECT_EQ(150, got);
}

TEST_F(CDCManagerTest, UpdateCheckpoint_PersistsAcrossRestart) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-1", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 100));

  // Restart master.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));

  // Verify checkpoint survived restart.
  GetCDCStreamInfoResponsePB resp;
  ASSERT_OK(GetStreamInfo(stream_id, &resp));
  ASSERT_TRUE(resp.has_stream());
  auto it = resp.stream().tablet_checkpoints().find(tablet_id);
  ASSERT_NE(it, resp.stream().tablet_checkpoints().end());
  EXPECT_EQ(100, it->second);
}

// Lever 2: each (stream, tablet) checkpoint lives in its own sys-catalog row with
// its own lock, so advancing one tablet's checkpoint neither disturbs another's
// stored value nor collapses them into a single blob. Verifies the rows are truly
// independent and that a stream with N checkpointed tablets has exactly N rows.
TEST_F(CDCManagerTest, PerTabletRowsAreIndependent) {
  const int kNumTablets = 3;
  string table_id;
  vector<string> t;
  ASSERT_OK(CreateTable("table-1", kNumTablets, &table_id, &t));
  ASSERT_EQ(kNumTablets, t.size());

  string stream_id;
  ASSERT_OK(CreateStream(table_id, &stream_id));

  // Creating the stream establishes one initial (op_index 0) row per tablet.
  int rows = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(kNumTablets, rows);

  // Advance the three tablets to distinct indexes.
  ASSERT_OK(UpdateCheckpoint(stream_id, t[0], 10));
  ASSERT_OK(UpdateCheckpoint(stream_id, t[1], 20));
  ASSERT_OK(UpdateCheckpoint(stream_id, t[2], 30));

  // Still one row per tablet (advancing upserts, never adds).
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(kNumTablets, rows);

  // Advancing one tablet leaves the others' stored values untouched.
  ASSERT_OK(UpdateCheckpoint(stream_id, t[1], 99));

  GetCDCStreamInfoResponsePB resp;
  ASSERT_OK(GetStreamInfo(stream_id, &resp));
  const auto& cps = resp.stream().tablet_checkpoints();
  ASSERT_EQ(kNumTablets, cps.size());
  ASSERT_NE(cps.find(t[0]), cps.end());
  ASSERT_NE(cps.find(t[1]), cps.end());
  ASSERT_NE(cps.find(t[2]), cps.end());
  EXPECT_EQ(10, cps.at(t[0]));
  EXPECT_EQ(99, cps.at(t[1]));
  EXPECT_EQ(30, cps.at(t[2]));

  // Still exactly three rows -- advancing an existing tablet upserts, never adds.
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(kNumTablets, rows);
}

// Deleting a stream must delete its per-tablet checkpoint rows too -- otherwise
// orphaned rows would leak in the sys catalog and (on reload) re-pin WAL/MVCC
// anchors on tablets whose stream no longer exists.
TEST_F(CDCManagerTest, DeleteStreamRemovesCheckpointRows) {
  string table_id;
  vector<string> t;
  ASSERT_OK(CreateTable("table-1", /*num_tablets=*/2, &table_id, &t));
  ASSERT_EQ(2, t.size());
  string stream_id;
  ASSERT_OK(CreateStream(table_id, &stream_id));
  ASSERT_OK(UpdateCheckpoint(stream_id, t[0], 10));
  ASSERT_OK(UpdateCheckpoint(stream_id, t[1], 20));

  int rows = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  ASSERT_EQ(2, rows);

  ASSERT_OK(DeleteStream(stream_id));

  // Two-phase delete: DeleteCDCStream only marks the stream DELETING. Its
  // checkpoint rows are removed by the reap in the next maintenance pass.
  RunMaintenance();

  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(0, rows);

  // And must stay gone across a restart (durable removal, no resurrection).
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));

  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(0, rows);
}

// Two-phase delete, phase 1: DeleteCDCStream persists state=DELETING and returns.
// The stream row and its checkpoint rows survive until the reap runs, but the
// stream is immediately hidden from the Get/List serving surface. Phase 2 (the
// reap in RunCDCStreamMaintenance) then removes both the checkpoint rows and the
// stream row.
TEST_F(CDCManagerTest, DeleteStream_MarksDeletingThenReaps) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-1", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 10));

  ASSERT_OK(DeleteStream(stream_id));

  // Phase 1: the stream row is still present, now marked DELETING, and its
  // checkpoint row has not yet been removed.
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_EQ(1, streams.count(stream_id));
  EXPECT_EQ(SysCDCStreamEntryPB::DELETING, streams[stream_id].state());
  int rows = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(1, rows);

  // Yet it is already hidden from consumers: GetCDCStreamInfo and ListCDCStreams
  // must not surface a stream that is being torn down.
  GetCDCStreamInfoResponsePB ginfo;
  EXPECT_TRUE(GetStreamInfo(stream_id, &ginfo).IsNotFound());
  ListCDCStreamsResponsePB list;
  ASSERT_OK(ListStreams("", &list));
  EXPECT_EQ(0, list.streams_size());

  // Phase 2: the reap removes the checkpoint rows and then the stream row.
  RunMaintenance();
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(0, rows);
  ASSERT_OK(VisitStreamRows(&streams));
  EXPECT_EQ(0, streams.count(stream_id));
}

// A repeated delete is idempotent: the second call sees the stream already
// DELETING and returns OK (not NotFound), so a client retry after a lost response
// does not fail.
TEST_F(CDCManagerTest, DeleteStream_IsIdempotent) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-1", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(DeleteStream(stream_id));
  ASSERT_OK(DeleteStream(stream_id));
}

// L1 (the linchpin): the two-phase delete is crash-safe. The DELETING marker is
// durable, so even when the master fails over *before* the reap has run, the new
// leader finds the marker and completes the cleanup -- no permanently leaked
// checkpoint rows (and, in a real cluster, no permanently pinned retention
// barrier). This is exactly the window the old single-phase delete could not
// recover from.
TEST_F(CDCManagerTest, DeleteStream_ReapCompletesAfterFailover) {
  string table_id;
  vector<string> t;
  ASSERT_OK(CreateTable("table-1", /*num_tablets=*/2, &table_id, &t));
  ASSERT_EQ(2, t.size());
  string stream_id;
  ASSERT_OK(CreateStream(table_id, &stream_id));
  ASSERT_OK(UpdateCheckpoint(stream_id, t[0], 10));
  ASSERT_OK(UpdateCheckpoint(stream_id, t[1], 20));

  // Phase 1 only: mark DELETING. The durable marker must be in place before we
  // simulate the failover.
  ASSERT_OK(DeleteStream(stream_id));
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_EQ(1, streams.count(stream_id));
  ASSERT_EQ(SysCDCStreamEntryPB::DELETING, streams[stream_id].state());

  // Simulate a failover before any reap by restarting the master.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));

  // The new leader completes the cleanup (its own maintenance pass, driven here
  // deterministically). Both the checkpoint rows and the stream row go away.
  ASSERT_EVENTUALLY([&]() {
    RunMaintenance();
    int rows = -1;
    ASSERT_OK(CountCheckpointRows(stream_id, &rows));
    ASSERT_EQ(0, rows);
    int stream_rows = -1;
    ASSERT_OK(CountStreamRows(stream_id, &stream_rows));
    ASSERT_EQ(0, stream_rows);
  });
}

// L3: orphaned checkpoint rows -- rows whose owning stream row is gone -- are
// reaped. This is the state a partial single-phase delete plus failover used to
// strand forever: LoadCDCTabletCheckpoints reloads the rows with no owning stream
// in cdc_stream_map_, and nothing ever removed them. The reap now collects them.
TEST_F(CDCManagerTest, Reap_OrphanedCheckpointRows) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-1", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 10));

  // Manufacture the orphaned-row state: remove ONLY the stream row from the sys
  // catalog, leaving the checkpoint row behind. (In production this is what a
  // crash mid-way through the old single-phase delete left behind.) The in-memory
  // maps are untouched, so the row is still owned in memory until a reload.
  ASSERT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                ->RemoveCDCStream(stream_id));
  int rows = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  ASSERT_EQ(1, rows);

  // Reload the catalog: the checkpoint row loads back with no owning stream row.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));

  // The reap treats a checkpoint row with no stream row as an implicit deletion
  // and removes it. Without the reap this row would leak forever.
  ASSERT_EVENTUALLY([&]() {
    RunMaintenance();
    int n = -1;
    ASSERT_OK(CountCheckpointRows(stream_id, &n));
    ASSERT_EQ(0, n);
  });
}

// Reap idempotency across partial completions (no restart). If a prior reap
// pass removed some checkpoint rows from the sys catalog but then failed to
// finish (e.g. a transient write error on another row), the in-memory
// cdc_tablet_checkpoint_map_ still references the already-deleted rows. The
// next reap pass must tolerate re-deleting those rows without erroring out.
// RemoveCDCTabletCheckpoint uses DELETE_IGNORE for exactly this reason; this
// test verifies that guarantee holds: after manually pre-removing one
// checkpoint row (simulating a partial prior pass) and then calling the reap,
// the stream is fully cleaned up on the first reap call.
TEST_F(CDCManagerTest, Reap_IdempotentAcrossPartialCompletion) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("table-1", /*num_tablets=*/2, &table_id, &tablet_ids));
  ASSERT_EQ(2, tablet_ids.size());

  string stream_id;
  ASSERT_OK(CreateStream(table_id, &stream_id));
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_ids[0], 10));
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_ids[1], 20));

  // Mark the stream DELETING (phase 1).
  ASSERT_OK(DeleteStream(stream_id));
  {
    map<string, SysCDCStreamEntryPB> s;
    ASSERT_OK(VisitStreamRows(&s));
    ASSERT_EQ(1, s.count(stream_id));
    ASSERT_EQ(SysCDCStreamEntryPB::DELETING, s[stream_id].state());
  }

  // Simulate a partial prior reap: remove tablet_ids[0]'s checkpoint row
  // directly from the sys catalog -- but leave the in-memory map untouched,
  // exactly as a prior partial pass would have. On the NEXT reap call the
  // in-memory snapshot still includes tablet_ids[0], so the reap sends a
  // DELETE_IGNORE for it (already gone) and must not fail.
  ASSERT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                ->RemoveCDCTabletCheckpoint(stream_id, tablet_ids[0]));
  {
    int rows = -1;
    ASSERT_OK(CountCheckpointRows(stream_id, &rows));
    // Only tablet_ids[1]'s row survives in the sys catalog.
    ASSERT_EQ(1, rows);
  }

  // Single reap pass: must complete fully despite tablet_ids[0]'s row being
  // absent from the sys catalog (the DELETE_IGNORE in RemoveCDCTabletCheckpoint
  // makes the already-removed row a no-op, not an error). ASSERT_EVENTUALLY
  // is used to tolerate the async RELEASE RPC tasks.
  ASSERT_EVENTUALLY([&]() {
    RunReap();
    int rows = -1;
    ASSERT_OK(CountCheckpointRows(stream_id, &rows));
    ASSERT_EQ(0, rows);
    int stream_rows = -1;
    ASSERT_OK(CountStreamRows(stream_id, &stream_rows));
    ASSERT_EQ(0, stream_rows);
  });
}

// DR-010 F-2: ReapDeletedCDCStreams must defer stream-row removal while
// in-flight RELEASE tasks are pending, and must proceed once they reach a
// terminal state. Without this guard, a master crash after RemoveCDCStream
// (step 3) but before the async RELEASE RPC lands could leave the tablet
// pinning WAL/MVCC history permanently (no surviving DELETING marker to drive
// a re-send on the new leader).
//
// The test injects a mock "running" task via inject_pending_release_task_for_tests
// (simulating an in-flight RELEASE) before the first reap. The stream row must
// survive that pass, and must be removed once the task is advanced to terminal.
TEST_F(CDCManagerTest, Reap_F2Guard_DeferredUntilTasksTerminal) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("table-guard", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(UpdateCheckpoint(stream_id, tablet_id, 5));
  ASSERT_OK(DeleteStream(stream_id));

  // Verify phase 1 left the stream DELETING.
  {
    map<string, SysCDCStreamEntryPB> s;
    ASSERT_OK(VisitStreamRows(&s));
    ASSERT_EQ(1, s.count(stream_id));
    ASSERT_EQ(SysCDCStreamEntryPB::DELETING, s[stream_id].state());
  }

  // Inject a mock task in kStateRunning into pending_release_tasks_ for this
  // stream. This simulates an in-flight RELEASE RPC dispatched by a previous
  // reap pass that has not yet been acknowledged by the replica.
  scoped_refptr<MockMonitoredTask> mock_task(new MockMonitoredTask(MonitoredTask::kStateRunning));
  mini_master_->master()->catalog_manager()->inject_pending_release_task_for_tests(
      stream_id, mock_task);

  // First reap: task is still in kStateRunning. Stream row must NOT be removed.
  RunReap();
  {
    int stream_rows = -1;
    ASSERT_OK(CountStreamRows(stream_id, &stream_rows));
    EXPECT_EQ(1, stream_rows)
        << "stream row must not be removed while RELEASE task is still running";
  }

  // Advance task to kStateComplete (RELEASE confirmed on replica).
  mock_task->set_state(MonitoredTask::kStateComplete);

  // Second reap: task is terminal, reap must fully clean up the stream.
  RunReap();
  {
    int stream_rows = -1;
    ASSERT_OK(CountStreamRows(stream_id, &stream_rows));
    EXPECT_EQ(0, stream_rows)
        << "stream row must be removed once RELEASE task is terminal";
    int rows = -1;
    ASSERT_OK(CountCheckpointRows(stream_id, &rows));
    EXPECT_EQ(0, rows);
  }
}

// DR-011: CreateCDCStream mid-fanout checkpoint write failure must mark the
// partially-created stream DELETING (not leave it ACTIVE) so the two-phase reap
// collects it on the next maintenance pass rather than waiting for the 4h
// staleness window. A 2-tablet table ensures we can inject a failure mid-fanout
// (after tablet[0]'s row succeeds, before tablet[1]'s).
TEST_F(CDCManagerTest, CreateStream_PartialCheckpointFanoutMarksDeleting) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("table-dr011", /*num_tablets=*/2, &table_id, &tablet_ids));
  ASSERT_EQ(2, tablet_ids.size());

  // Inject a failure on the second (index 1) checkpoint write.
  FLAGS_cdc_create_stream_fail_checkpoint_idx = 1;
  string stream_id;
  Status s = CreateStream(table_id, &stream_id);
  FLAGS_cdc_create_stream_fail_checkpoint_idx = -1;  // disarm

  // CreateCDCStream must have returned an error.
  ASSERT_FALSE(s.ok()) << "expected error from injected checkpoint failure";

  // The stream_id from the response is empty on error; recover it from the
  // sys catalog (there should be exactly one stream row, now DELETING).
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_EQ(1, streams.size()) << "expected exactly one stream row";
  const auto& [sid, entry] = *streams.begin();
  EXPECT_EQ(SysCDCStreamEntryPB::DELETING, entry.state())
      << "partially-created stream must be DELETING, not ACTIVE";

  // Exactly one checkpoint row (tablet[0] succeeded; tablet[1] never written).
  {
    int rows = -1;
    ASSERT_OK(CountCheckpointRows(sid, &rows));
    EXPECT_EQ(1, rows);
  }

  // A single reap pass must fully remove both the checkpoint row and the stream
  // row (no tservers => no in-flight RELEASE tasks => single pass).
  RunReap();
  {
    int rows = -1;
    ASSERT_OK(CountCheckpointRows(sid, &rows));
    EXPECT_EQ(0, rows);
    ASSERT_OK(VisitStreamRows(&streams));
    EXPECT_EQ(0, streams.count(sid));
  }
}

// A tablet still referenced by a surviving stream must NOT have its checkpoint
// row removed when a different stream sharing that tablet is deleted: the reap
// releases only the deleted stream's own rows, never a live stream's.
TEST_F(CDCManagerTest, Reap_LeavesSurvivingStreamRows) {
  string table_id, shared_tablet;
  {
    vector<string> t;
    ASSERT_OK(CreateTable("table-1", /*num_tablets=*/1, &table_id, &t));
    ASSERT_EQ(1, t.size());
    shared_tablet = t[0];
  }
  // Two streams over the same table, hence over the same (single) tablet.
  string doomed, survivor;
  ASSERT_OK(CreateStream(table_id, &doomed));
  ASSERT_OK(CreateStream(table_id, &survivor));
  ASSERT_OK(UpdateCheckpoint(doomed, shared_tablet, 10));
  ASSERT_OK(UpdateCheckpoint(survivor, shared_tablet, 20));

  ASSERT_OK(DeleteStream(doomed));
  // Drive only the reap (not a full maintenance pass) so the assertion isolates
  // reap behavior -- that it touches only the deleted stream's own rows -- from
  // the rest of a maintenance pass's barrier recomputation.
  RunReap();

  // The doomed stream and its row are gone...
  int rows = -1;
  ASSERT_OK(CountCheckpointRows(doomed, &rows));
  EXPECT_EQ(0, rows);
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  EXPECT_EQ(0, streams.count(doomed));

  // ...but the surviving stream, its row, and its checkpoint are untouched.
  ASSERT_EQ(1, streams.count(survivor));
  EXPECT_EQ(SysCDCStreamEntryPB::ACTIVE, streams[survivor].state());
  ASSERT_OK(CountCheckpointRows(survivor, &rows));
  EXPECT_EQ(1, rows);
  GetCDCStreamInfoResponsePB info;
  ASSERT_OK(GetStreamInfo(survivor, &info));
  auto it = info.stream().tablet_checkpoints().find(shared_tablet);
  ASSERT_NE(it, info.stream().tablet_checkpoints().end());
  EXPECT_EQ(20, it->second);
}

// L2: dropping (hard-deleting) a table must condemn every CDC stream that
// references it. DeleteTable marks such streams DELETING eagerly, before any
// maintenance pass runs; the stream is then immediately hidden from consumers.
TEST_F(CDCManagerTest, DropTable_MarksReferencingStreamDeleting) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("doomed-table", &table_id, &tablet_id, &stream_id));

  ASSERT_OK(DropTable("doomed-table"));

  // Eagerly marked DELETING by DeleteTable -- no maintenance pass needed. The
  // stream row is still present (the reap has not run yet) but in DELETING state.
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_EQ(1, streams.count(stream_id));
  EXPECT_EQ(SysCDCStreamEntryPB::DELETING, streams[stream_id].state());

  // And already hidden from consumers.
  GetCDCStreamInfoResponsePB info;
  EXPECT_TRUE(GetStreamInfo(stream_id, &info).IsNotFound());
  ListCDCStreamsResponsePB list;
  ASSERT_OK(ListStreams("", &list));
  EXPECT_EQ(0, list.streams_size());
}

// L2 + L1: after a table drop condemns its stream, the ordinary two-phase reap
// removes the stream row and all its checkpoint rows -- no leak.
TEST_F(CDCManagerTest, DropTable_StreamReapedByMaintenance) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("doomed-table", &table_id, &tablet_id, &stream_id));
  int rows = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  ASSERT_EQ(1, rows);

  ASSERT_OK(DropTable("doomed-table"));

  // A maintenance pass marks (already done eagerly) then reaps. Driven
  // deterministically here, with ASSERT_EVENTUALLY to tolerate the retrying
  // RELEASE RPC tasks.
  ASSERT_EVENTUALLY([&]() {
    RunMaintenance();
    int n = -1;
    ASSERT_OK(CountCheckpointRows(stream_id, &n));
    ASSERT_EQ(0, n);
    int stream_rows = -1;
    ASSERT_OK(CountStreamRows(stream_id, &stream_rows));
    ASSERT_EQ(0, stream_rows);
  });
}

// Recall-safety: a soft-deleted (recallable) table is NOT a drop. Its streams
// must stay ACTIVE so that a recall leaves them intact. The dropped-table scan
// (run directly here, and as the maintenance backstop) must leave them alone.
TEST_F(CDCManagerTest, SoftDeleteTable_LeavesStreamActive) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("recallable-table", &table_id, &tablet_id, &stream_id));

  ASSERT_OK(SoftDeleteTable("recallable-table", /*reserve_seconds=*/3600));

  // Neither the eager path (soft delete never sets REMOVED) nor the backstop
  // scan condemns the stream.
  RunMarkDeletingForDroppedTables();
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_EQ(1, streams.count(stream_id));
  EXPECT_EQ(SysCDCStreamEntryPB::ACTIVE, streams[stream_id].state());

  // Still visible to consumers.
  GetCDCStreamInfoResponsePB info;
  ASSERT_OK(GetStreamInfo(stream_id, &info));
  EXPECT_EQ(stream_id, info.stream().stream_id());
}

// Recall-safety end-to-end: soft-deleting then recalling a table must leave the
// CDC stream ACTIVE throughout. The stream is kept during the soft-delete window
// and the recall puts the table back to RUNNING. No code path modifies the stream
// during recall, so this verifies the complete soft-delete -> recall cycle.
TEST_F(CDCManagerTest, SoftDeleteThenRecall_KeepsStreamActive) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("recalled-table", &table_id, &tablet_id, &stream_id));

  // Soft-delete (recallable).
  ASSERT_OK(SoftDeleteTable("recalled-table", /*reserve_seconds=*/3600));

  // Stream must stay ACTIVE during the soft-delete window: neither the eager
  // hook (not called on the soft-delete path) nor the backstop scan should
  // condemn a stream whose table is merely soft-deleted.
  RunMarkDeletingForDroppedTables();
  {
    map<string, SysCDCStreamEntryPB> streams;
    ASSERT_OK(VisitStreamRows(&streams));
    ASSERT_EQ(1, streams.count(stream_id));
    EXPECT_EQ(SysCDCStreamEntryPB::ACTIVE, streams[stream_id].state());
  }

  // Recall the table back to RUNNING.
  ASSERT_OK(RecallTable(table_id));

  // Stream must still be ACTIVE after recall: a recallable table is never a
  // drop, so the stream must not have been condemned at any point.
  RunMarkDeletingForDroppedTables();
  {
    map<string, SysCDCStreamEntryPB> streams;
    ASSERT_OK(VisitStreamRows(&streams));
    ASSERT_EQ(1, streams.count(stream_id));
    EXPECT_EQ(SysCDCStreamEntryPB::ACTIVE, streams[stream_id].state());
  }

  // Stream is still visible to consumers after the table's lifecycle round-trip.
  GetCDCStreamInfoResponsePB info;
  ASSERT_OK(GetStreamInfo(stream_id, &info));
  EXPECT_EQ(stream_id, info.stream().stream_id());
}

// No false positives: dropping an unrelated table must not condemn a stream that
// does not reference it.
TEST_F(CDCManagerTest, DropUnrelatedTable_LeavesStreamActive) {
  string table_a, tablet_a, stream_id;
  ASSERT_OK(CreateTableAndStream("table-A", &table_a, &tablet_a, &stream_id));

  string table_b;
  vector<string> tablets_b;
  ASSERT_OK(CreateTable("table-B", /*num_tablets=*/1, &table_b, &tablets_b));

  ASSERT_OK(DropTable("table-B"));
  RunMarkDeletingForDroppedTables();

  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_EQ(1, streams.count(stream_id));
  EXPECT_EQ(SysCDCStreamEntryPB::ACTIVE, streams[stream_id].state());
}

// The backstop is a durable, failover-safe safety net: even if the eager mark in
// DeleteTable never happened (or a ghost ACTIVE stream over a since-dropped table
// predates this logic), a maintenance pass condemns the stream. Reproduced here
// by forcing the persisted stream row back to ACTIVE after a drop, reloading the
// catalog, then running the scan.
TEST_F(CDCManagerTest, DropTable_BackstopMarksDeletingAfterReload) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("ghost-table", &table_id, &tablet_id, &stream_id));

  // Read the (now-DELETING, post-drop) stream row is not what we want yet: drop
  // first, then rewrite the row back to ACTIVE to simulate a stream that the
  // eager path failed to condemn.
  ASSERT_OK(DropTable("ghost-table"));
  {
    map<string, SysCDCStreamEntryPB> streams;
    ASSERT_OK(VisitStreamRows(&streams));
    ASSERT_EQ(1, streams.count(stream_id));
    SysCDCStreamEntryPB pb = streams[stream_id];
    pb.set_state(SysCDCStreamEntryPB::ACTIVE);
    ASSERT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                  ->WriteCDCStream(stream_id, pb));
  }

  // Reload the catalog so the in-memory maps reflect the forced-ACTIVE row.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));

  // The backstop re-condemns it (the referenced table is gone / REMOVED), and the
  // reap finishes cleanup.
  ASSERT_EVENTUALLY([&]() {
    RunMaintenance();
    int stream_rows = -1;
    ASSERT_OK(CountStreamRows(stream_id, &stream_rows));
    ASSERT_EQ(0, stream_rows);
  });
}

// V4/CF-2: the max-staleness guard releases a tablet's retention barrier once its
// durable checkpoint stops advancing for --cdc_max_staleness_ms. Measured naively
// from last_checkpoint_advance_time_micros alone, that clock keeps ticking through
// a master outage: a master that recovers after being down longer than the window
// would, on its very first maintenance pass, declare every not-recently-advanced
// stream stale and drop its barrier -- punishing consumers for the master's own
// downtime. The fix measures staleness from max(last_advance, leader_ready) so an
// outage the consumer did not cause does not count against it; only after the new
// leader has itself been up longer than the window does a genuinely stuck consumer
// get released. This test doctors a checkpoint's last-advance far into the past,
// reloads it, and drives one maintenance pass under each leader-ready placement.
TEST_F(CDCManagerTest, StalenessGuardGracePeriodAfterLeaderReady) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("stale-grace", &table_id, &tablet_id, &stream_id));

  // Doctor the tablet's checkpoint row: keep it pinning WAL (op_index set) and
  // recently active (so idle-expiry never fires), but push its last durable
  // advance far into the past so the staleness window is exceeded by a wide
  // margin regardless of when the pass runs.
  {
    vector<SysCDCTabletCheckpointEntryPB> rows;
    ASSERT_OK(VisitCheckpointRows(&rows));
    SysCDCTabletCheckpointEntryPB pb;
    bool found = false;
    for (const auto& r : rows) {
      if (r.stream_id() == stream_id && r.tablet_id() == tablet_id) {
        pb = r;
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found) << "no checkpoint row for the created stream/tablet";
    pb.set_op_index(0);
    pb.set_last_active_time_micros(GetCurrentTimeMicros());
    // Ancient: essentially the Unix epoch, so (now - advance) dwarfs any window.
    pb.set_last_checkpoint_advance_time_micros(1000);
    ASSERT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                  ->WriteCDCTabletCheckpoint(stream_id, tablet_id, pb));
  }

  // Reload so the in-memory checkpoint map (which maintenance reads) reflects the
  // doctored advance time. The restart also re-stamps the real leader-ready clock,
  // which the test overrides below to make each pass deterministic.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));
  CatalogManager* cm = mini_master_->master()->catalog_manager();

  // Enable the staleness guard (> the huge bg-scan interval, satisfying the flag
  // group validator) and disable idle-expiry so it is the only release path.
  FLAGS_cdc_max_staleness_ms = 2 * 3600 * 1000; // 2h > bg_scan (1h)
  FLAGS_cdc_stream_expiry_ms = 0;               // disable idle-expiry

  const int64_t now = GetCurrentTimeMicros();

  // Grace case (the fix): the leader became ready just now, so the doctored
  // ancient advance is inside the grace window (effective_advance = leader_ready).
  // The barrier must be retained.
  cm->set_cdc_leader_ready_micros_for_tests(now);
  RunMaintenance();
  EXPECT_EQ(1, cm->cdc_barriered_tablet_count())
      << "staleness guard must not release a barrier within the post-leader-ready "
         "grace window (outage is not the consumer's fault)";

  // Control: the leader has been ready since the ancient advance itself, so the
  // grace window has fully elapsed and the genuinely-stuck consumer is released.
  cm->set_cdc_leader_ready_micros_for_tests(1000);
  RunMaintenance();
  EXPECT_EQ(0, cm->cdc_barriered_tablet_count())
      << "once the leader has been up longer than the staleness window, a "
         "non-advancing checkpoint must still release its barrier";
}

// V4/CF-2 re-stamp: each new leadership acquisition must re-stamp
// cdc_leader_ready_micros_ so a second (or later) failover re-opens a fresh
// grace window rather than inheriting an ancient floor from a previous term
// or from the initial zero value. The test does two consecutive restarts:
// after the first it forces leader_ready to ancient and asserts the control
// case (release fires); after the second restart the real stamp (now) is used,
// and the grace window must be fresh -- the ancient checkpoint is still in the
// row, but effective_advance = max(ancient, now) = now, so no release.
TEST_F(CDCManagerTest, StalenessGuardGraceFloorReStampedOnEachLeaderTerm) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("restamp-table", &table_id, &tablet_id, &stream_id));

  // Push the tablet's last-advance far into the past and persist it.
  {
    vector<SysCDCTabletCheckpointEntryPB> rows;
    ASSERT_OK(VisitCheckpointRows(&rows));
    SysCDCTabletCheckpointEntryPB pb;
    bool found = false;
    for (const auto& r : rows) {
      if (r.stream_id() == stream_id && r.tablet_id() == tablet_id) {
        pb = r;
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found) << "no checkpoint row for the created stream/tablet";
    pb.set_op_index(0);
    pb.set_last_active_time_micros(GetCurrentTimeMicros());
    pb.set_last_checkpoint_advance_time_micros(1000); // essentially the epoch
    ASSERT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                  ->WriteCDCTabletCheckpoint(stream_id, tablet_id, pb));
  }

  FLAGS_cdc_max_staleness_ms = 2 * 3600 * 1000; // 2h > bg_scan (1h)
  FLAGS_cdc_stream_expiry_ms = 0;               // disable idle-expiry

  // First leadership: restart, reload the doctored row, then simulate
  // a long-uptime leader (ancient floor) -- the control case must release.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));
  {
    CatalogManager* cm1 = mini_master_->master()->catalog_manager();
    cm1->set_cdc_leader_ready_micros_for_tests(1000); // ancient: grace expired
    RunMaintenance();
    EXPECT_EQ(0, cm1->cdc_barriered_tablet_count())
        << "first term, ancient floor: stale barrier must be released";
  }

  // Second leadership: restart again. PrepareForLeadershipTask re-stamps
  // cdc_leader_ready_micros_ to the current time. The grace window must now
  // be fresh even though the ancient 1000 was left behind in the previous term.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));
  {
    CatalogManager* cm2 = mini_master_->master()->catalog_manager();
    // Do NOT override leader_ready -- use the real stamp from PrepareForLeadershipTask.
    // effective_advance = max(1000, now) = now -> (now - now) = 0 < 2h -> not stale.
    RunMaintenance();
    EXPECT_EQ(1, cm2->cdc_barriered_tablet_count())
        << "second term, real re-stamp: PrepareForLeadershipTask must have stamped "
           "cdc_leader_ready_micros_ to now, so the ancient advance is inside the "
           "grace window and the barrier must be retained";
  }
}

// CF-2/DR-018 fix: the staleness guard must NOT release the barrier when the
// consumer is actively advancing but sys-catalog writes are failing.
//
// Background: the staleness guard fires when last_checkpoint_advance_time_micros
// (durable, in sys-catalog) has not moved for --cdc_max_staleness_ms. If
// PersistCheckpoint calls succeed at the tserver but the master-side sys-catalog
// write fails (transient I/O error, disk pressure, etc.), last_advance stays
// stale even though the consumer is making real progress. Without this fix the
// maintenance loop would release the retention barrier and silently discard WAL
// the consumer still needs -- the consumer never learns of the data loss because
// the Checkpoint RPC already returned SUCCESS.
//
// The fix adds last_checkpoint_advance_attempt_micros_ (in-memory, per row) as
// a third term in effective_advance alongside last_advance (durable) and
// leader_ready_micros (DR-015). UpdateCDCCheckpoint stamps it whenever the
// incoming op_index strictly advances, BEFORE the sys-catalog write, so it
// is always current for an advancing consumer regardless of write success.
//
// Non-vacuous: with this test, reverting the fix (removing advance_attempt
// from the effective_advance computation) causes case 1 to fail -- the
// maintenance loop releases the barrier even though the consumer is advancing.
TEST_F(CDCManagerTest, StalenessGuardAdvanceAttemptSuppressesRelease) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("cf2-table", &table_id, &tablet_id, &stream_id));

  // Doctor the checkpoint row: set op_index (so the tablet pins WAL), keep
  // last_active recent (so idle-expiry never fires), but push last_advance to
  // the Unix epoch so the staleness window is blown wide open.
  {
    vector<SysCDCTabletCheckpointEntryPB> rows;
    ASSERT_OK(VisitCheckpointRows(&rows));
    SysCDCTabletCheckpointEntryPB pb;
    bool found = false;
    for (const auto& r : rows) {
      if (r.stream_id() == stream_id && r.tablet_id() == tablet_id) {
        pb = r;
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found) << "no checkpoint row for the created stream/tablet";
    pb.set_op_index(0);
    pb.set_last_active_time_micros(GetCurrentTimeMicros());
    pb.set_last_checkpoint_advance_time_micros(1000); // essentially the epoch
    ASSERT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                  ->WriteCDCTabletCheckpoint(stream_id, tablet_id, pb));
  }

  // Reload so the in-memory checkpoint map reflects the doctored row.
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));
  CatalogManager* cm = mini_master_->master()->catalog_manager();

  FLAGS_cdc_max_staleness_ms = 2 * 3600 * 1000; // 2h
  FLAGS_cdc_stream_expiry_ms = 0;                // disable idle-expiry

  // Disable DR-015's grace floor so it does not mask the CF-2 fix: force the
  // leader-ready time to be ancient (same epoch value as the doctored advance),
  // so effective_advance would be (epoch, epoch, 0) = epoch if the fix is absent.
  cm->set_cdc_leader_ready_micros_for_tests(1000);

  // Without any advance attempt the row looks like a genuinely stuck consumer:
  // both last_advance and advance_attempt are ancient (0 = never set after
  // restart). The barrier must be released (control case).
  // advance_attempt_ is 0 after restart (in-memory field). leader_ready is 1000.
  RunMaintenance();
  EXPECT_EQ(0, cm->cdc_barriered_tablet_count())
      << "with ancient last_advance, ancient leader_ready, and no advance "
         "attempt, the genuinely-stuck consumer must still be released";

  // Reset: restore the checkpoint row (so the stream is active again with a
  // barrier) and reload. We need a fresh row to re-pin the barrier.
  {
    vector<SysCDCTabletCheckpointEntryPB> rows;
    ASSERT_OK(VisitCheckpointRows(&rows));
    SysCDCTabletCheckpointEntryPB pb;
    bool found = false;
    for (const auto& r : rows) {
      if (r.stream_id() == stream_id && r.tablet_id() == tablet_id) {
        pb = r;
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found) << "no checkpoint row for the second phase";
    pb.set_op_index(0);
    pb.set_last_active_time_micros(GetCurrentTimeMicros());
    pb.set_last_checkpoint_advance_time_micros(1000); // still ancient
    ASSERT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                  ->WriteCDCTabletCheckpoint(stream_id, tablet_id, pb));
  }
  mini_master_->Shutdown();
  ASSERT_OK(mini_master_->Restart());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                      mini_master_->bound_rpc_addr().host()));
  cm = mini_master_->master()->catalog_manager();
  cm->set_cdc_leader_ready_micros_for_tests(1000); // ancient: DR-015 grace expired

  // Case 1 (the fix): simulate a consumer that IS advancing but whose sys-catalog
  // writes are failing. UpdateCDCCheckpoint stamps last_checkpoint_advance_attempt_micros_
  // before the write; our test accessor simulates that stamp directly. The durable
  // last_advance is still ancient (epoch), but the in-memory attempt time is now.
  // The barrier must be HELD.
  const int64_t now = GetCurrentTimeMicros();
  cm->set_last_checkpoint_advance_attempt_micros_for_tests(stream_id, tablet_id, now);
  RunMaintenance();
  EXPECT_EQ(1, cm->cdc_barriered_tablet_count())
      << "CF-2 fix: a current advance attempt must suppress the staleness "
         "release even when the durable last_advance and leader_ready are both "
         "ancient (simulates: consumer advancing, sys-catalog writes failing)";

  // Case 2 (the fix must not block genuinely-stuck consumers): now make the
  // advance attempt also ancient. The consumer has stopped advancing entirely;
  // all three terms of effective_advance are stale. The barrier must be released.
  cm->set_last_checkpoint_advance_attempt_micros_for_tests(stream_id, tablet_id, 1000);
  RunMaintenance();
  EXPECT_EQ(0, cm->cdc_barriered_tablet_count())
      << "CF-2 fix must not block release of a genuinely stuck consumer: "
         "when advance attempt is also ancient, staleness guard must fire";
}

} // namespace master
} // namespace kudu
