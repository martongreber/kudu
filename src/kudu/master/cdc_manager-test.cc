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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
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
#include "kudu/rpc/user_credentials.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(catalog_manager_check_ts_count_for_create_table);
DECLARE_bool(cdc_enabled);
DECLARE_double(sys_catalog_fail_during_write);
DECLARE_int32(cdc_bg_scan_interval_ms);
DECLARE_int32(cdc_create_stream_fail_checkpoint_idx);
DECLARE_int32(cdc_max_streams);
DECLARE_int32(cdc_max_streams_per_table);
DECLARE_int64(cdc_max_staleness_ms);
DECLARE_int64(cdc_stream_expiry_ms);
DECLARE_int64(cdc_ephemeral_stream_expiry_ms);
DECLARE_bool(cdc_skip_eager_partition_register_for_tests);
DECLARE_int32(tablet_creation_timeout_ms);
DECLARE_int32(unresponsive_ts_rpc_timeout_ms);
DECLARE_int32(master_ts_rpc_timeout_ms);

METRIC_DECLARE_counter(cdc_maintenance_runs);
METRIC_DECLARE_counter(cdc_tablet_checkpoint_expirations);
METRIC_DECLARE_gauge_int64(cdc_max_checkpoint_age_micros);
METRIC_DECLARE_gauge_int64(cdc_streams_active);
METRIC_DECLARE_gauge_int64(cdc_tablet_checkpoints_expired);

using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::rpc::RpcController;
using std::map;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

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
    FLAGS_cdc_enabled = true;
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

  Status CreateStream(const string& table_id, string* stream_id,
                      CDCStreamConfigPB::RecordType record_type =
                          CDCStreamConfigPB::CHANGE) {
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.add_table_ids(table_id);
    if (record_type != CDCStreamConfigPB::CHANGE) {
      req.mutable_config()->set_record_type(record_type);
    }

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

  // Runs only ReconcileNewPartitionsForCDCStreams, the maintenance-loop self-heal
  // path that registers tablets added by ADD_RANGE_PARTITION.
  void RunReconcile() {
    mini_master_->master()->catalog_manager()->ReconcileNewPartitionsForCDCStreams();
  }

  // Creates a table with a single explicit bounded range partition [0, 1000).
  // Unlike CreateTable (which covers all of INT32 with split rows), this table
  // has a gap at [1000, ...) so AddRangePartition can add [1000, 2000) later.
  Status CreateTableBounded(const string& name,
                             string* table_id,
                             vector<string>* tablet_ids) {
    CreateTableRequestPB req;
    CreateTableResponsePB resp;
    req.set_name(name);
    Schema schema = GetSimpleTestSchema();
    RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
    // Explicit range bounds [0, 1000) leave [1000, ...) empty for ADD_RANGE_PARTITION.
    {
      KuduPartialRow lb(&schema), ub(&schema);
      RETURN_NOT_OK(lb.SetInt32("key", 0));
      RETURN_NOT_OK(ub.SetInt32("key", 1000));
      RowOperationsPBEncoder enc(req.mutable_split_rows_range_bounds());
      enc.Add(RowOperationsPB::RANGE_LOWER_BOUND, lb);
      enc.Add(RowOperationsPB::RANGE_UPPER_BOUND, ub);
    }

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    // Explicit RANGE_LOWER_BOUND / RANGE_UPPER_BOUND ops in split_rows_range_bounds
    // require the RANGE_PARTITION_BOUNDS server feature.
    rpc.RequireServerFeature(MasterFeatures::RANGE_PARTITION_BOUNDS);
    RETURN_NOT_OK(proxy_->CreateTable(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    *table_id = resp.table_id();

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

  // Calls AlterTable ADD_RANGE_PARTITION to add [lower, upper) to 'table_id'.
  // Returns the tablet ids of ALL tablets after the alter (including pre-existing).
  Status AddRangePartition(const string& table_id,
                           int lower_bound, int upper_bound,
                           vector<string>* all_tablet_ids) {
    AlterTableRequestPB req;
    AlterTableResponsePB resp;
    req.mutable_table()->set_table_id(table_id);

    AlterTableRequestPB::Step* step = req.add_alter_schema_steps();
    step->set_type(AlterTableRequestPB::ADD_RANGE_PARTITION);
    Schema schema = GetSimpleTestSchema();
    KuduPartialRow lb(&schema), ub(&schema);
    RETURN_NOT_OK(lb.SetInt32("key", lower_bound));
    RETURN_NOT_OK(ub.SetInt32("key", upper_bound));
    RowOperationsPBEncoder enc(
        step->mutable_add_range_partition()->mutable_range_bounds());
    enc.Add(RowOperationsPB::RANGE_LOWER_BOUND, lb);
    enc.Add(RowOperationsPB::RANGE_UPPER_BOUND, ub);
    // Schema field: master needs this to decode the range bounds.
    RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));

    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    // ADD_RANGE_PARTITION requires the ADD_DROP_RANGE_PARTITIONS feature flag
    // (mirroring kudu::client which sets this for AlterTable with partitions).
    rpc.RequireServerFeature(MasterFeatures::ADD_DROP_RANGE_PARTITIONS);
    RETURN_NOT_OK(proxy_->AlterTable(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }

    // Read back all tablet ids.
    scoped_refptr<TableInfo> table;
    {
      CatalogManager::ScopedLeaderSharedLock l(
          mini_master_->master()->catalog_manager());
      RETURN_NOT_OK(l.first_failed_status());
      RETURN_NOT_OK(mini_master_->master()->catalog_manager()->GetTableInfo(
          table_id, &table));
    }
    if (!table) {
      return Status::NotFound("table not found after alter");
    }
    vector<scoped_refptr<TabletInfo>> tablets;
    table->GetAllTablets(&tablets);
    all_tablet_ids->clear();
    for (const auto& t : tablets) {
      all_tablet_ids->emplace_back(t->id());
    }
    return Status::OK();
  }

  // Restarts the master and waits until it is the ready leader and its first
  // automatic CDC maintenance pass is done, so a test can drive later passes
  // deterministically without racing the background task.
  void RestartMaster() {
    mini_master_->Shutdown();
    ASSERT_OK(mini_master_->Restart());
    ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
        MonoDelta::FromSeconds(90)));
    master_ = mini_master_->master();
    proxy_.reset(new MasterServiceProxy(client_messenger_, mini_master_->bound_rpc_addr(),
                                        mini_master_->bound_rpc_addr().host()));
    NO_FATALS(WaitForFirstMaintenancePass());
  }

  void WaitForFirstMaintenancePass() {
    scoped_refptr<Counter> runs =
        METRIC_cdc_maintenance_runs.Instantiate(mini_master_->master()->metric_entity());
    ASSERT_EVENTUALLY([&] {
      ASSERT_GE(runs->value(), 1);
    });
  }

  // Rewrites the sys-catalog checkpoint row of (stream_id, tablet_id) with
  // 'mutate' applied. The in-memory copy is not updated: restart the master to
  // load the doctored row.
  void DoctorCheckpointRow(const string& stream_id,
                           const string& tablet_id,
                           const std::function<void(SysCDCTabletCheckpointEntryPB*)>& mutate) {
    vector<SysCDCTabletCheckpointEntryPB> rows;
    ASSERT_OK(VisitCheckpointRows(&rows));
    for (auto& r : rows) {
      if (r.stream_id() == stream_id && r.tablet_id() == tablet_id) {
        mutate(&r);
        ASSERT_OK(mini_master_->master()->catalog_manager()->sys_catalog()
                      ->WriteCDCTabletCheckpoint(stream_id, tablet_id, r));
        return;
      }
    }
    FAIL() << "no checkpoint row for " << stream_id << "/" << tablet_id;
  }

  // Returns the checkpoint row of (stream_id, tablet_id) from the sys catalog.
  void GetCheckpointRow(const string& stream_id,
                        const string& tablet_id,
                        SysCDCTabletCheckpointEntryPB* row) {
    vector<SysCDCTabletCheckpointEntryPB> rows;
    ASSERT_OK(VisitCheckpointRows(&rows));
    for (const auto& r : rows) {
      if (r.stream_id() == stream_id && r.tablet_id() == tablet_id) {
        *row = r;
        return;
      }
    }
    FAIL() << "no checkpoint row for " << stream_id << "/" << tablet_id;
  }

  Status SendUpdateCheckpoint(const UpdateCDCCheckpointRequestPB& req) {
    UpdateCDCCheckpointResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy_->UpdateCDCCheckpoint(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  Status GetTabletInfo(const string& table_id,
                       const string& tablet_id,
                       scoped_refptr<TabletInfo>* tablet) {
    scoped_refptr<TableInfo> table;
    {
      CatalogManager::ScopedLeaderSharedLock l(mini_master_->master()->catalog_manager());
      RETURN_NOT_OK(l.first_failed_status());
      RETURN_NOT_OK(mini_master_->master()->catalog_manager()->GetTableInfo(table_id, &table));
    }
    if (!table) {
      return Status::NotFound("table not found");
    }
    vector<scoped_refptr<TabletInfo>> tablets;
    table->GetAllTablets(&tablets);
    for (auto& t : tablets) {
      if (t->id() == tablet_id) {
        *tablet = std::move(t);
        return Status::OK();
      }
    }
    return Status::NotFound("tablet not found");
  }

  // Gives 'tablet_id' (in memory only) a committed config whose single replica
  // is a tablet server the master does not know, or (with an empty 'ts_uuid')
  // removes the config. The barrier tasks the master then sends to the replica
  // keep retrying until --unresponsive_ts_rpc_timeout_ms and then fail, as for
  // a tablet server that is down.
  void SetFakeReplica(const string& table_id, const string& tablet_id,
                      const string& ts_uuid) {
    scoped_refptr<TabletInfo> tablet;
    ASSERT_OK(GetTabletInfo(table_id, tablet_id, &tablet));
    TabletMetadataLock l(tablet.get(), LockMode::WRITE);
    auto* pb = &l.mutable_data()->pb;
    if (ts_uuid.empty()) {
      pb->clear_consensus_state();
    } else {
      auto* cstate = pb->mutable_consensus_state();
      cstate->set_current_term(1);
      cstate->mutable_committed_config()->clear_peers();
      auto* peer = cstate->mutable_committed_config()->add_peers();
      peer->set_permanent_uuid(ts_uuid);
      peer->set_member_type(consensus::RaftPeerPB::VOTER);
    }
    l.Commit();
  }

  // Returns the CDC barrier tasks of 'table_id' that are still running.
  void GetRunningBarrierTasks(const string& table_id,
                              vector<scoped_refptr<MonitoredTask>>* out) {
    scoped_refptr<TableInfo> table;
    {
      CatalogManager::ScopedLeaderSharedLock l(mini_master_->master()->catalog_manager());
      ASSERT_OK(l.first_failed_status());
      ASSERT_OK(mini_master_->master()->catalog_manager()->GetTableInfo(table_id, &table));
    }
    ASSERT_NE(nullptr, table.get());
    vector<scoped_refptr<MonitoredTask>> tasks;
    table->GetTaskList(&tasks);
    out->clear();
    for (auto& t : tasks) {
      if (t->type_name() == "UpdateCDCRetentionBarrier" &&
          t->state() == MonitoredTask::kStateRunning) {
        out->emplace_back(std::move(t));
      }
    }
  }

  int NumRunningBarrierTasks(const string& table_id) {
    vector<scoped_refptr<MonitoredTask>> tasks;
    GetRunningBarrierTasks(table_id, &tasks);
    return static_cast<int>(tasks.size());
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

// A stream covers exactly one table: dropping one table of a multi-table
// stream would delete the whole stream, and consumers follow only one table.
TEST_F(CDCManagerTest, CreateStream_MultiTableRejected) {
  string table_a, table_b;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("multi-a", /*num_tablets=*/1, &table_a, &tablet_ids));
  ASSERT_OK(CreateTable("multi-b", /*num_tablets=*/1, &table_b, &tablet_ids));

  CreateCDCStreamRequestPB req;
  CreateCDCStreamResponsePB resp;
  req.add_table_ids(table_a);
  req.add_table_ids(table_b);
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(proxy_->CreateCDCStream(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  const Status s = StatusFromPB(resp.error().status());
  EXPECT_TRUE(s.IsNotSupported()) << s.ToString();
  EXPECT_FALSE(resp.has_stream_id());

  // Nothing was persisted for the rejected stream.
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  EXPECT_TRUE(streams.empty());
}

// A stream configuration with an option this master does not know, from a
// newer client, is rejected: the master would store the stream without
// applying the option. Before, such a stream was created, and an unknown
// record type was served as CHANGE.
TEST_F(CDCManagerTest, CreateStream_UnknownConfigOptionRejected) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("unknown-option", /*num_tablets=*/1, &table_id, &tablet_ids));

  // A field this master does not know, and a record type it does not know.
  for (int field : { 100, static_cast<int>(CDCStreamConfigPB::kRecordTypeFieldNumber) }) {
    SCOPED_TRACE(field);
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.add_table_ids(table_id);
    req.mutable_config()->mutable_unknown_fields()->AddVarint(field, 100);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->CreateCDCStream(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    const Status s = StatusFromPB(resp.error().status());
    EXPECT_TRUE(s.IsNotSupported()) << s.ToString();
    EXPECT_FALSE(resp.has_stream_id());
  }
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

// UpdateCDCCheckpoints applies the updates for all rows of a batch with one
// sys-catalog write, applies the updates for one row in order, and skips the
// updates for an unknown stream without failing the batch.
TEST_F(CDCManagerTest, CheckpointBatch_AppliesAllRowsInOneWrite) {
  const int kNumTablets = 5;
  string table_id;
  vector<string> t;
  ASSERT_OK(CreateTable("table-1", kNumTablets, &table_id, &t));
  ASSERT_EQ(kNumTablets, t.size());
  string stream_id;
  ASSERT_OK(CreateStream(table_id, &stream_id));

  UpdateCDCCheckpointsRequestPB req;
  for (int i = 0; i < kNumTablets; ++i) {
    auto* u = req.add_updates();
    u->set_stream_id(stream_id);
    u->set_tablet_id(t[i]);
    u->set_op_index(10 * (i + 1));
  }
  // A second update for the first tablet, applied after the first one.
  auto* again = req.add_updates();
  again->set_stream_id(stream_id);
  again->set_tablet_id(t[0]);
  again->set_op_index(77);
  // An update for a stream that does not exist is skipped.
  auto* unknown = req.add_updates();
  unknown->set_stream_id("no-such-stream");
  unknown->set_tablet_id(t[1]);
  unknown->set_op_index(1000);

  const auto& consensus = mini_master_->master()->catalog_manager()->sys_catalog()
      ->tablet_replica()->consensus();
  const auto committed_index = [&]() {
    return consensus->GetLastOpId(consensus::COMMITTED_OPID)->index();
  };
  const int64_t index_before = committed_index();
  UpdateCDCCheckpointsResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(proxy_->UpdateCDCCheckpoints(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  EXPECT_EQ(1, committed_index() - index_before)
      << "all rows of the batch must be written in one sys-catalog write";

  GetCDCStreamInfoResponsePB info;
  ASSERT_OK(GetStreamInfo(stream_id, &info));
  const auto& cps = info.stream().tablet_checkpoints();
  ASSERT_EQ(kNumTablets, cps.size());
  EXPECT_EQ(77, cps.at(t[0]));
  for (int i = 1; i < kNumTablets; ++i) {
    EXPECT_EQ(10 * (i + 1), cps.at(t[i]));
  }
  vector<SysCDCTabletCheckpointEntryPB> rows;
  ASSERT_OK(VisitCheckpointRows(&rows));
  for (const auto& r : rows) {
    EXPECT_EQ(stream_id, r.stream_id()) << "no row for the unknown stream";
  }
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

// DR-011: after a CreateCDCStream mid-fanout checkpoint write failure, the
// partially-created stream must stay CREATING (never ACTIVE) so the two-phase
// reap collects it on the next maintenance pass rather than waiting for the 4h
// staleness window. A 2-tablet table ensures we can inject a failure mid-fanout
// (after tablet[0]'s row succeeds, before tablet[1]'s).
TEST_F(CDCManagerTest, CreateStream_PartialCheckpointFanoutIsReaped) {
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
  // sys catalog (there should be exactly one stream row, still CREATING).
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_EQ(1, streams.size()) << "expected exactly one stream row";
  const auto& [sid, entry] = *streams.begin();
  EXPECT_EQ(SysCDCStreamEntryPB::CREATING, entry.state())
      << "partially-created stream must stay CREATING, not become ACTIVE";
  ListCDCStreamsResponsePB list;
  ASSERT_OK(ListStreams("", &list));
  EXPECT_EQ(0, list.streams_size());

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

// --- ADD_RANGE_PARTITION / CDC reconcile tests ---

// ReconcileNewPartitionsForCDCStreams must write a checkpoint row for a tablet
// that exists in the table but has no entry in cdc_tablet_checkpoint_map_.
// This simulates a master crash between ADD_RANGE_PARTITION and the CDC row
// write (the crash-recovery self-heal path).
TEST_F(CDCManagerTest, ReconcileAddsCheckpointRowForNewPartition) {
  // Create a table with one bounded partition [0, 1000) and a CDC stream.
  string table_id, stream_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTableBounded("reconcile-table", &table_id, &tablet_ids));
  ASSERT_EQ(1, tablet_ids.size());
  ASSERT_OK(CreateStream(table_id, &stream_id));

  // Verify one checkpoint row after stream creation.
  int count = 0;
  ASSERT_OK(CountCheckpointRows(stream_id, &count));
  ASSERT_EQ(1, count);

  // Add a second range partition [1000, 2000). The injection flag suppresses
  // the eager RegisterNewTabletsWithCDCStreams call so the new tablet is NOT
  // registered -- simulating a crash between the sys_catalog write and the CDC
  // row write.
  FLAGS_cdc_skip_eager_partition_register_for_tests = true;
  vector<string> all_ids;
  ASSERT_OK(AddRangePartition(table_id, 1000, 2000, &all_ids));
  FLAGS_cdc_skip_eager_partition_register_for_tests = false;
  ASSERT_EQ(2, all_ids.size());

  // After AlterTable with the flag, still only 1 checkpoint row (eager skipped).
  ASSERT_OK(CountCheckpointRows(stream_id, &count));
  ASSERT_EQ(1, count);

  // RunReconcile must discover the new tablet and write a checkpoint row for it.
  RunReconcile();

  ASSERT_OK(CountCheckpointRows(stream_id, &count));
  ASSERT_EQ(2, count) << "reconcile must write a checkpoint row for the new partition's tablet";

  // Verify op_index=0 was written for the new tablet.
  // Identify the new tablet (the one not in the original tablet_ids list).
  string new_tablet_id;
  for (const auto& id : all_ids) {
    if (id != tablet_ids[0]) {
      new_tablet_id = id;
      break;
    }
  }
  ASSERT_FALSE(new_tablet_id.empty());
  vector<SysCDCTabletCheckpointEntryPB> rows;
  ASSERT_OK(VisitCheckpointRows(&rows));
  bool found_new = false;
  for (const auto& r : rows) {
    if (r.stream_id() == stream_id && r.tablet_id() == new_tablet_id) {
      EXPECT_EQ(0, r.op_index())
          << "new partition checkpoint must start at op_index=0";
      found_new = true;
      break;
    }
  }
  EXPECT_TRUE(found_new) << "no checkpoint row for the new partition's tablet";
}

// A history floor of 0 means "no floor", not "retain all history". The rows of
// a FULL stream therefore start with a floor at the creation time, both at
// stream creation and for a partition added later (eager register and the
// reconcile self-heal), so the history its first before-images need is
// retained until the consumer reports a real position. CHANGE rows get none.
TEST_F(CDCManagerTest, FullStreamRowsStartWithCreationHistoryFloor) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTableBounded("floor-table", &table_id, &tablet_ids));
  ASSERT_EQ(1, tablet_ids.size());

  const int64_t before = GetCurrentTimeMicros();
  string full_id;
  string change_id;
  ASSERT_OK(CreateStream(table_id, &full_id, CDCStreamConfigPB::FULL));
  ASSERT_OK(CreateStream(table_id, &change_id, CDCStreamConfigPB::CHANGE));
  // One partition registered eagerly, one by the reconcile pass.
  vector<string> all_ids;
  ASSERT_OK(AddRangePartition(table_id, 1000, 2000, &all_ids));
  FLAGS_cdc_skip_eager_partition_register_for_tests = true;
  ASSERT_OK(AddRangePartition(table_id, 2000, 3000, &all_ids));
  FLAGS_cdc_skip_eager_partition_register_for_tests = false;
  RunReconcile();
  const int64_t after = GetCurrentTimeMicros();
  ASSERT_EQ(3, all_ids.size());

  vector<SysCDCTabletCheckpointEntryPB> rows;
  ASSERT_OK(VisitCheckpointRows(&rows));
  int full_rows = 0;
  int change_rows = 0;
  for (const auto& r : rows) {
    if (r.stream_id() == full_id) {
      ++full_rows;
      EXPECT_GE(r.history_safe_time_micros(), before) << r.ShortDebugString();
      EXPECT_LE(r.history_safe_time_micros(), after) << r.ShortDebugString();
    } else if (r.stream_id() == change_id) {
      ++change_rows;
      EXPECT_FALSE(r.has_history_safe_time_micros()) << r.ShortDebugString();
    }
  }
  EXPECT_EQ(3, full_rows);
  EXPECT_EQ(3, change_rows);
}

// ReconcileNewPartitionsForCDCStreams is idempotent: calling it twice when all
// tablets are already registered must not duplicate rows or change row count.
TEST_F(CDCManagerTest, ReconcileIdempotent) {
  string table_id, stream_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTableBounded("idempotent-table", &table_id, &tablet_ids));
  ASSERT_OK(CreateStream(table_id, &stream_id));

  int count_before = 0;
  ASSERT_OK(CountCheckpointRows(stream_id, &count_before));

  // Running reconcile when all tablets are already registered must be a no-op.
  RunReconcile();
  RunReconcile();

  int count_after = 0;
  ASSERT_OK(CountCheckpointRows(stream_id, &count_after));
  EXPECT_EQ(count_before, count_after)
      << "reconcile must not add duplicate rows when all tablets are known";
}

// ReconcileNewPartitionsForCDCStreams must not write checkpoint rows for
// streams in DELETING state (they are being torn down).
TEST_F(CDCManagerTest, ReconcileSkipsDeletingStream) {
  string table_id, stream_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTableBounded("deleting-table", &table_id, &tablet_ids));
  ASSERT_OK(CreateStream(table_id, &stream_id));

  // Mark the stream DELETING (simulates a DeleteCDCStream phase-1 commit).
  ASSERT_OK(DeleteStream(stream_id));

  // Add a second partition. Flag off so we control the write count.
  FLAGS_cdc_skip_eager_partition_register_for_tests = true;
  vector<string> all_ids;
  ASSERT_OK(AddRangePartition(table_id, 1000, 2000, &all_ids));
  FLAGS_cdc_skip_eager_partition_register_for_tests = false;

  // Reap the DELETING stream first (which removes the original checkpoint rows).
  RunReap();

  // Reconcile must NOT write a new row for the deleted stream's new tablet.
  RunReconcile();

  int count = 0;
  ASSERT_OK(CountCheckpointRows(stream_id, &count));
  EXPECT_EQ(0, count)
      << "reconcile must not register tablets for a DELETING/reaped stream";
}

// EagerRegisterOnAddRangePartition: the eager AlterTable hook must write
// checkpoint rows for new tablets BEFORE RunCDCStreamMaintenance runs.
TEST_F(CDCManagerTest, EagerRegisterOnAddRangePartition) {
  string table_id, stream_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTableBounded("eager-table", &table_id, &tablet_ids));
  ASSERT_EQ(1, tablet_ids.size());
  ASSERT_OK(CreateStream(table_id, &stream_id));

  // Confirm baseline: 1 checkpoint row before alter.
  int count = 0;
  ASSERT_OK(CountCheckpointRows(stream_id, &count));
  ASSERT_EQ(1, count);

  // Add a second partition. Eager hook is NOT suppressed, so
  // RegisterNewTabletsWithCDCStreams must write the row synchronously.
  vector<string> all_ids;
  ASSERT_OK(AddRangePartition(table_id, 1000, 2000, &all_ids));
  ASSERT_EQ(2, all_ids.size());

  // Without running maintenance, the row must already exist.
  ASSERT_OK(CountCheckpointRows(stream_id, &count));
  ASSERT_EQ(2, count)
      << "eager hook must write checkpoint row for new partition without "
         "waiting for the maintenance loop";

  // Verify the new tablet's row has op_index=0.
  string new_tablet_id;
  for (const auto& id : all_ids) {
    if (id != tablet_ids[0]) {
      new_tablet_id = id;
      break;
    }
  }
  ASSERT_FALSE(new_tablet_id.empty());
  vector<SysCDCTabletCheckpointEntryPB> rows;
  ASSERT_OK(VisitCheckpointRows(&rows));
  bool found_new = false;
  for (const auto& r : rows) {
    if (r.stream_id() == stream_id && r.tablet_id() == new_tablet_id) {
      EXPECT_EQ(0, r.op_index());
      found_new = true;
      break;
    }
  }
  EXPECT_TRUE(found_new) << "no checkpoint row written by eager hook for new partition";

  // Running maintenance afterward must be idempotent (still 2 rows, not 3+).
  RunMaintenance();
  ASSERT_OK(CountCheckpointRows(stream_id, &count));
  EXPECT_EQ(2, count)
      << "maintenance loop must not add duplicate rows when eager hook ran first";
}

// --- Barrier release reconciliation and stream state ---

// A stream that expires while no leader runs a maintenance pass (here: across a
// master restart) must still have its barrier released by the new leader. The
// new leader does not know which barriers the previous one set, so it seeds its
// release set from the checkpoint rows. Without that, the expired row is just
// skipped and no RELEASE is ever sent.
TEST_F(CDCManagerTest, NewLeaderReleasesBarrierOfStreamExpiredDuringFailover) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("expire-failover", &table_id, &tablet_id, &stream_id));
  // The stream went idle long before the restart.
  NO_FATALS(DoctorCheckpointRow(stream_id, tablet_id,
                                [](SysCDCTabletCheckpointEntryPB* pb) {
    pb->set_last_active_time_micros(1000);
  }));
  NO_FATALS(RestartMaster());
  CatalogManager* cm = mini_master_->master()->catalog_manager();

  // The leader has been up longer than the expiry window (no grace).
  cm->set_cdc_leader_ready_micros_for_tests(1000);
  RunMaintenance();
  // The counter is per catalog manager, so it counts the releases this leader
  // sent, including in its first (automatic) pass.
  EXPECT_EQ(1, cm->cdc_barrier_releases_total())
      << "the new leader must release the barrier of a stream that expired "
         "before it became leader";
  EXPECT_EQ(0, cm->cdc_barriered_tablet_count());
}

// A master outage longer than the expiry window must not expire every stream
// on the new leader's first pass: the consumers could not report while no
// master was up. Until the leader has been ready for a full window, a tablet
// pinned only by such rows is held: no RELEASE (which would drop a live
// consumer's barrier) and no SET (which would re-pin the WAL of a dead stream
// whose barrier was already released).
TEST_F(CDCManagerTest, ExpiryGraceAfterLeaderReady) {
  FLAGS_tablet_creation_timeout_ms = 3600 * 1000;
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("expire-grace", &table_id, &tablet_id, &stream_id));
  NO_FATALS(DoctorCheckpointRow(stream_id, tablet_id,
                                [](SysCDCTabletCheckpointEntryPB* pb) {
    pb->set_last_active_time_micros(1000);
  }));
  NO_FATALS(RestartMaster());
  CatalogManager* cm = mini_master_->master()->catalog_manager();
  NO_FATALS(SetFakeReplica(table_id, tablet_id, "fake-ts"));

  // The leader just became ready: the idle row is in its grace window.
  cm->set_cdc_leader_ready_micros_for_tests(GetCurrentTimeMicros());
  const int64_t releases_before = cm->cdc_barrier_releases_total();
  RunMaintenance();
  EXPECT_EQ(0, cm->cdc_barrier_releases_total() - releases_before)
      << "a stream idle across a master outage must not be released right "
         "after the new leader became ready";
  EXPECT_EQ(1, cm->cdc_barriered_tablet_count());
  EXPECT_EQ(0, NumRunningBarrierTasks(table_id))
      << "a held tablet must get neither a SET nor a RELEASE";

  // Once the leader has been ready for longer than the window, the stream is
  // expired and its barrier is released.
  cm->set_cdc_leader_ready_micros_for_tests(1000);
  RunMaintenance();
  EXPECT_EQ(1, cm->cdc_barrier_releases_total() - releases_before);
  EXPECT_EQ(1, NumRunningBarrierTasks(table_id));
}

// A consumer that keeps reporting must not be expired just because the master
// cannot persist its last-active time.
TEST_F(CDCManagerTest, ExpiryHeldWhileActivityCannotBePersisted) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("expire-attempt", &table_id, &tablet_id, &stream_id));
  NO_FATALS(DoctorCheckpointRow(stream_id, tablet_id,
                                [](SysCDCTabletCheckpointEntryPB* pb) {
    pb->set_last_active_time_micros(1000);
  }));
  NO_FATALS(RestartMaster());
  CatalogManager* cm = mini_master_->master()->catalog_manager();
  cm->set_cdc_leader_ready_micros_for_tests(1000);

  // An activity heartbeat arrives, but its sys-catalog write fails.
  UpdateCDCCheckpointRequestPB req;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_op_index(0);
  req.set_refresh_active_time_only(true);
  FLAGS_sys_catalog_fail_during_write = 1.0;
  Status s = SendUpdateCheckpoint(req);
  FLAGS_sys_catalog_fail_during_write = 0.0;
  ASSERT_FALSE(s.ok());

  const int64_t releases_before = cm->cdc_barrier_releases_total();
  RunMaintenance();
  EXPECT_EQ(0, cm->cdc_barrier_releases_total() - releases_before);
  EXPECT_EQ(1, cm->cdc_barriered_tablet_count())
      << "a consumer that reports activity must keep its barrier even if the "
         "report could not be persisted";
}

// A release whose task fails (the replica was down for longer than
// --unresponsive_ts_rpc_timeout_ms) must be sent again rather than forgotten.
TEST_F(CDCManagerTest, ReleaseRedispatchedAfterTaskFailure) {
  FLAGS_tablet_creation_timeout_ms = 3600 * 1000;
  FLAGS_unresponsive_ts_rpc_timeout_ms = 1000;
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("release-retry", &table_id, &tablet_id, &stream_id));
  NO_FATALS(SetFakeReplica(table_id, tablet_id, "down-ts"));
  CatalogManager* cm = mini_master_->master()->catalog_manager();

  // Expire the stream: the next pass releases the eagerly pushed barrier. Take
  // the baseline first: the background pass may be the one that releases it.
  const int64_t releases_before = cm->cdc_barrier_releases_total();
  FLAGS_cdc_stream_expiry_ms = 1;
  SleepFor(MonoDelta::FromMilliseconds(5));
  RunMaintenance();
  ASSERT_EQ(1, cm->cdc_barrier_releases_total() - releases_before);
  ASSERT_EQ(1, NumRunningBarrierTasks(table_id));

  // While the release task is in flight it is neither re-sent nor forgotten.
  RunMaintenance();
  EXPECT_EQ(1, cm->cdc_barrier_releases_total() - releases_before);
  EXPECT_EQ(1, cm->cdc_barriered_tablet_count())
      << "an unconfirmed release must stay tracked";

  // The task gives up; the next pass must send the release again.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, NumRunningBarrierTasks(table_id));
  });
  RunMaintenance();
  EXPECT_EQ(2, cm->cdc_barrier_releases_total() - releases_before)
      << "a failed release must be re-sent";
  EXPECT_EQ(1, NumRunningBarrierTasks(table_id));
  EXPECT_EQ(1, cm->cdc_barriered_tablet_count());

  // The dead replica leaves the config. The in-flight task fails, the next
  // release has no replica to contact, and the tablet is forgotten.
  NO_FATALS(SetFakeReplica(table_id, tablet_id, ""));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, NumRunningBarrierTasks(table_id));
  });
  RunMaintenance();
  EXPECT_EQ(3, cm->cdc_barrier_releases_total() - releases_before);
  EXPECT_EQ(0, cm->cdc_barriered_tablet_count());
  RunMaintenance();
  EXPECT_EQ(3, cm->cdc_barrier_releases_total() - releases_before);
}

// The reap of a deleted stream must not count an aborted or failed RELEASE as
// delivered: it dispatches the release again and keeps the rows (and so the
// DELETING marker that drives the retry) until every RELEASE completed.
TEST_F(CDCManagerTest, Reap_AbortedReleaseTaskIsRedispatched) {
  FLAGS_tablet_creation_timeout_ms = 3600 * 1000;
  FLAGS_unresponsive_ts_rpc_timeout_ms = 1000;
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("reap-abort", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(DeleteStream(stream_id));
  NO_FATALS(SetFakeReplica(table_id, tablet_id, "down-ts"));

  // Pass A ran on an earlier term and its task was aborted, as a leadership
  // change does to every outstanding task.
  scoped_refptr<MockMonitoredTask> aborted(
      new MockMonitoredTask(MonitoredTask::kStateAborted));
  mini_master_->master()->catalog_manager()->inject_pending_release_task_for_tests(
      stream_id, aborted);
  RunReap();
  int rows = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(1, rows) << "an aborted RELEASE must not be counted as delivered";
  EXPECT_EQ(1, NumRunningBarrierTasks(table_id)) << "the RELEASE must be re-sent";

  // The re-sent RELEASE fails too (the replica is still down): sent again.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, NumRunningBarrierTasks(table_id));
  });
  RunReap();
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(1, rows) << "a failed RELEASE must not be counted as delivered";
  EXPECT_EQ(1, NumRunningBarrierTasks(table_id));

  // Once the replica is out of the config there is nobody left to release and
  // the reap completes.
  NO_FATALS(SetFakeReplica(table_id, tablet_id, ""));
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, NumRunningBarrierTasks(table_id));
  });
  RunReap();
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(0, rows);
  int stream_rows = -1;
  ASSERT_OK(CountStreamRows(stream_id, &stream_rows));
  EXPECT_EQ(0, stream_rows);
}

// Same-process re-election (M1 -> M2 -> M1): the leadership change aborts the
// reap's RELEASE tasks. The new term must dispatch them again instead of
// reading the aborted handles as delivered.
TEST_F(CDCManagerTest, Reap_ReleaseRedispatchedAfterReElection) {
  FLAGS_tablet_creation_timeout_ms = 3600 * 1000;
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("reap-reelect", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(DeleteStream(stream_id));
  NO_FATALS(SetFakeReplica(table_id, tablet_id, "down-ts"));
  RunReap();
  vector<scoped_refptr<MonitoredTask>> tasks;
  NO_FATALS(GetRunningBarrierTasks(table_id, &tasks));
  ASSERT_EQ(1, tasks.size());

  CatalogManager* cm = mini_master_->master()->catalog_manager();
  ASSERT_OK(cm->sys_catalog()->tablet_replica()->consensus()->EmulateElectionForTests());
  ASSERT_OK(mini_master_->master()->WaitUntilCatalogManagerIsLeaderAndReadyForTests(
      MonoDelta::FromSeconds(90)));
  ASSERT_EQ(MonitoredTask::kStateAborted, tasks[0]->state());
  // Reloading the catalog dropped the in-memory replica; put it back.
  NO_FATALS(SetFakeReplica(table_id, tablet_id, "down-ts"));

  RunReap();
  int rows = -1;
  ASSERT_OK(CountCheckpointRows(stream_id, &rows));
  EXPECT_EQ(1, rows) << "a RELEASE aborted by a leadership change must not be "
                        "counted as delivered";
  EXPECT_EQ(1, NumRunningBarrierTasks(table_id));
}

// A checkpoint or heartbeat for a stream that is no longer ACTIVE must be
// rejected. Otherwise a consumer still polling a just-deleted stream writes a
// row that the reap may already have passed, and the row outlives the stream.
TEST_F(CDCManagerTest, UpdateCheckpoint_RejectedForDeletingStream) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("ckpt-deleting", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(DeleteStream(stream_id));

  Status s = UpdateCheckpoint(stream_id, tablet_id, 5);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
  s = UpdateCheckpoint(stream_id, "some-other-tablet", 5);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();

  vector<SysCDCTabletCheckpointEntryPB> rows;
  ASSERT_OK(VisitCheckpointRows(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(tablet_id, rows[0].tablet_id());
  EXPECT_EQ(0, rows[0].op_index()) << "the DELETING stream's row must not change";

  RunReap();
  ASSERT_OK(VisitCheckpointRows(&rows));
  EXPECT_TRUE(rows.empty());
}

// Within one snapshot generation the snapshot only goes from active to done: a
// delayed page write that lands after the completion write must not offer the
// completed snapshot for resume again.
TEST_F(CDCManagerTest, SnapshotCursor_ActiveIsMonotonicWithinGeneration) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("snap-monotonic", &table_id, &tablet_id, &stream_id));
  const auto persist = [&](const string& key, bool active) {
    UpdateCDCCheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(7);
    req.set_snapshot_snap_ts(100);
    req.set_snapshot_resume_key(key);
    req.set_snapshot_active(active);
    return SendUpdateCheckpoint(req);
  };
  ASSERT_OK(persist("a", true));
  ASSERT_OK(persist("b", false));  // the snapshot completed
  ASSERT_OK(persist("a", true));   // a delayed earlier page

  GetCDCStreamInfoResponsePB resp;
  ASSERT_OK(GetStreamInfo(stream_id, &resp));
  EXPECT_EQ(0, resp.stream().tablet_snapshot_cursors_size())
      << "a completed snapshot must not be re-activated";
  SysCDCTabletCheckpointEntryPB row;
  NO_FATALS(GetCheckpointRow(stream_id, tablet_id, &row));
  EXPECT_FALSE(row.snapshot_active());
  EXPECT_EQ("b", row.snapshot_resume_key());
}

// While a tablet's snapshot is active, its op_index is the point the WAL is
// streamed from once the snapshot completes. GetCDCStreamInfo and
// ListCDCStreams must not report it as the tablet's checkpoint: a consumer
// that restarted mid-snapshot would resume streaming from it and skip the rest
// of the snapshot. The tablet is reported by its snapshot cursor instead.
TEST_F(CDCManagerTest, SnapshotCursor_ActiveSnapshotHidesCheckpoint) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("snap-hides-ckpt", &table_id, &tablet_id, &stream_id));
  const auto persist = [&](bool active) {
    UpdateCDCCheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(7);
    req.set_snapshot_snap_ts(100);
    req.set_snapshot_resume_key("a");
    req.set_snapshot_active(active);
    return SendUpdateCheckpoint(req);
  };
  ASSERT_OK(persist(/*active=*/true));
  {
    GetCDCStreamInfoResponsePB resp;
    ASSERT_OK(GetStreamInfo(stream_id, &resp));
    EXPECT_EQ(0, resp.stream().tablet_checkpoints().count(tablet_id))
        << "the streaming start of an active snapshot was reported as a checkpoint";
    ASSERT_EQ(1, resp.stream().tablet_snapshot_cursors_size());
    const auto& cur = resp.stream().tablet_snapshot_cursors(0);
    EXPECT_EQ(tablet_id, cur.tablet_id());
    EXPECT_TRUE(cur.snapshot_active());
    EXPECT_EQ(7, cur.streaming_start_op_index());
    ListCDCStreamsResponsePB list;
    ASSERT_OK(ListStreams(table_id, &list));
    ASSERT_EQ(1, list.streams_size());
    EXPECT_EQ(0, list.streams(0).tablet_checkpoints().count(tablet_id));
  }

  // Once the snapshot completes, the checkpoint is the streaming start point.
  ASSERT_OK(persist(/*active=*/false));
  {
    GetCDCStreamInfoResponsePB resp;
    ASSERT_OK(GetStreamInfo(stream_id, &resp));
    ASSERT_EQ(1, resp.stream().tablet_checkpoints().count(tablet_id));
    EXPECT_EQ(7, resp.stream().tablet_checkpoints().at(tablet_id));
    EXPECT_EQ(0, resp.stream().tablet_snapshot_cursors_size());
    ListCDCStreamsResponsePB list;
    ASSERT_OK(ListStreams(table_id, &list));
    ASSERT_EQ(1, list.streams_size());
    ASSERT_EQ(1, list.streams(0).tablet_checkpoints().count(tablet_id));
    EXPECT_EQ(7, list.streams(0).tablet_checkpoints().at(tablet_id));
  }
}

// A consumer that checkpoints at or past the streaming start point of an
// active snapshot got its last page and streams the WAL: the snapshot is
// complete, even though the tablet server that served the last page (which
// would have reported it) is no longer the leader. A checkpoint below it
// leaves the snapshot in progress.
TEST_F(CDCManagerTest, SnapshotCursor_CheckpointAtStreamingStartCompletesSnapshot) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("snap-ckpt-completes", &table_id, &tablet_id, &stream_id));
  {
    UpdateCDCCheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(7);
    req.set_snapshot_snap_ts(100);
    req.set_snapshot_resume_key("a");
    req.set_snapshot_active(true);
    ASSERT_OK(SendUpdateCheckpoint(req));
  }
  const auto checkpoint = [&](int64_t op_index) {
    UpdateCDCCheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);
    return SendUpdateCheckpoint(req);
  };
  // A consumer in the middle of the snapshot checkpoints its initial position.
  ASSERT_OK(checkpoint(0));
  {
    GetCDCStreamInfoResponsePB resp;
    ASSERT_OK(GetStreamInfo(stream_id, &resp));
    EXPECT_EQ(0, resp.stream().tablet_checkpoints().count(tablet_id));
    ASSERT_EQ(1, resp.stream().tablet_snapshot_cursors_size());
  }
  ASSERT_OK(checkpoint(7));
  {
    GetCDCStreamInfoResponsePB resp;
    ASSERT_OK(GetStreamInfo(stream_id, &resp));
    EXPECT_EQ(0, resp.stream().tablet_snapshot_cursors_size());
    ASSERT_EQ(1, resp.stream().tablet_checkpoints().count(tablet_id));
    EXPECT_EQ(7, resp.stream().tablet_checkpoints().at(tablet_id));
  }
}

// A checkpoint of 0 does not complete a snapshot whose streaming start point
// is 0 (reachable through the transaction fallback of the establish): it is
// the position of a consumer that has not streamed yet. Before, it completed
// the snapshot, and a consumer that restarted then streamed from 0 instead of
// taking the rest of the snapshot.
TEST_F(CDCManagerTest, SnapshotCursor_CheckpointZeroDoesNotCompleteSnapshotAtZero) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("snap-ckpt-zero", &table_id, &tablet_id, &stream_id));
  {
    UpdateCDCCheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(0);
    req.set_snapshot_snap_ts(100);
    req.set_snapshot_resume_key("a");
    req.set_snapshot_active(true);
    ASSERT_OK(SendUpdateCheckpoint(req));
  }
  const auto checkpoint = [&](int64_t op_index) {
    UpdateCDCCheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(op_index);
    return SendUpdateCheckpoint(req);
  };
  ASSERT_OK(checkpoint(0));
  {
    GetCDCStreamInfoResponsePB resp;
    ASSERT_OK(GetStreamInfo(stream_id, &resp));
    EXPECT_EQ(0, resp.stream().tablet_checkpoints().count(tablet_id));
    ASSERT_EQ(1, resp.stream().tablet_snapshot_cursors_size())
        << "a checkpoint of 0 completed the snapshot";
    EXPECT_TRUE(resp.stream().tablet_snapshot_cursors(0).snapshot_active());
  }
  ASSERT_OK(checkpoint(1));
  {
    GetCDCStreamInfoResponsePB resp;
    ASSERT_OK(GetStreamInfo(stream_id, &resp));
    EXPECT_EQ(0, resp.stream().tablet_snapshot_cursors_size());
    ASSERT_EQ(1, resp.stream().tablet_checkpoints().count(tablet_id));
    EXPECT_EQ(1, resp.stream().tablet_checkpoints().at(tablet_id));
  }
}

// The staleness guard must not release a healthy consumer of an idle tablet:
// its checkpoint cannot advance because there is nothing to read. A report
// whose up_to_op_index the stored checkpoint has reached counts as progress;
// one the checkpoint is behind does not.
TEST_F(CDCManagerTest, Staleness_IdleCaughtUpTabletNotReleased) {
  string table_id, stream_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("stale-idle", /*num_tablets=*/2, &table_id, &tablet_ids));
  ASSERT_EQ(2, tablet_ids.size());
  ASSERT_OK(CreateStream(table_id, &stream_id));
  for (const auto& tablet_id : tablet_ids) {
    NO_FATALS(DoctorCheckpointRow(stream_id, tablet_id,
                                  [](SysCDCTabletCheckpointEntryPB* pb) {
      pb->set_op_index(5);
      pb->set_last_active_time_micros(GetCurrentTimeMicros());
      pb->set_last_checkpoint_advance_time_micros(1000);
    }));
  }
  NO_FATALS(RestartMaster());
  CatalogManager* cm = mini_master_->master()->catalog_manager();
  FLAGS_cdc_max_staleness_ms = 2 * 3600 * 1000;
  FLAGS_cdc_stream_expiry_ms = 0;
  cm->set_cdc_leader_ready_micros_for_tests(1000);

  const auto heartbeat = [&](const string& tablet_id, int64_t up_to) {
    UpdateCDCCheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(0);
    req.set_refresh_active_time_only(true);
    req.set_up_to_op_index(up_to);
    return SendUpdateCheckpoint(req);
  };
  ASSERT_OK(heartbeat(tablet_ids[0], 5));  // caught up
  ASSERT_OK(heartbeat(tablet_ids[1], 9));  // 4 ops behind

  RunMaintenance();
  EXPECT_EQ(1, cm->cdc_barriered_tablet_count())
      << "only the caught-up tablet may keep its barrier";
  SysCDCTabletCheckpointEntryPB row;
  NO_FATALS(GetCheckpointRow(stream_id, tablet_ids[0], &row));
  EXPECT_GT(row.last_checkpoint_advance_time_micros(), 1000);
  NO_FATALS(GetCheckpointRow(stream_id, tablet_ids[1], &row));
  EXPECT_EQ(1000, row.last_checkpoint_advance_time_micros());
}

// During a snapshot op_index stays at the streaming start. A snapshot cursor
// that moves forward is progress for the staleness guard; one re-sent at the
// same key is not.
TEST_F(CDCManagerTest, Staleness_SnapshotCursorProgressCountsAsAdvance) {
  string table_id, stream_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("stale-snap", /*num_tablets=*/2, &table_id, &tablet_ids));
  ASSERT_EQ(2, tablet_ids.size());
  ASSERT_OK(CreateStream(table_id, &stream_id));
  for (const auto& tablet_id : tablet_ids) {
    NO_FATALS(DoctorCheckpointRow(stream_id, tablet_id,
                                  [](SysCDCTabletCheckpointEntryPB* pb) {
      pb->set_op_index(7);
      pb->set_snapshot_snap_ts(100);
      pb->set_snapshot_resume_key("k1");
      pb->set_snapshot_active(true);
      pb->set_last_active_time_micros(GetCurrentTimeMicros());
      pb->set_last_checkpoint_advance_time_micros(1000);
    }));
  }
  NO_FATALS(RestartMaster());
  CatalogManager* cm = mini_master_->master()->catalog_manager();
  FLAGS_cdc_max_staleness_ms = 2 * 3600 * 1000;
  FLAGS_cdc_stream_expiry_ms = 0;
  cm->set_cdc_leader_ready_micros_for_tests(1000);

  const auto persist_cursor = [&](const string& tablet_id, const string& key) {
    UpdateCDCCheckpointRequestPB req;
    req.set_stream_id(stream_id);
    req.set_tablet_id(tablet_id);
    req.set_op_index(7);
    req.set_snapshot_snap_ts(100);
    req.set_snapshot_resume_key(key);
    req.set_snapshot_active(true);
    return SendUpdateCheckpoint(req);
  };
  ASSERT_OK(persist_cursor(tablet_ids[0], "k2"));  // the scan advanced
  ASSERT_OK(persist_cursor(tablet_ids[1], "k1"));  // the scan is stuck

  RunMaintenance();
  EXPECT_EQ(1, cm->cdc_barriered_tablet_count())
      << "only the tablet whose snapshot cursor advanced may keep its barrier";
  SysCDCTabletCheckpointEntryPB row;
  NO_FATALS(GetCheckpointRow(stream_id, tablet_ids[0], &row));
  EXPECT_TRUE(row.snapshot_active());
  NO_FATALS(GetCheckpointRow(stream_id, tablet_ids[1], &row));
  EXPECT_TRUE(row.snapshot_active())
      << "releasing a stale snapshot's retention must not complete the snapshot";
}

// A snapshot whose consumer went away (stale: its cursor stopped advancing;
// expired: no activity) loses its retention, but it stays unfinished: the
// master keeps the cursor, reports the tablet only by it, and never reports
// the streaming start point that op_index holds as the tablet's checkpoint.
// Before, the maintenance pass cleared snapshot_active on such a row, and a
// returning consumer streamed the WAL from the streaming start point, skipping
// the rows of the snapshot it had not read.
TEST_F(CDCManagerTest, Hygiene_AbandonedSnapshotStaysUnfinished) {
  string table_id, stream_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("abandoned-snap", /*num_tablets=*/2, &table_id, &tablet_ids));
  ASSERT_EQ(2, tablet_ids.size());
  ASSERT_OK(CreateStream(table_id, &stream_id));
  const string& stale_tablet = tablet_ids[0];
  const string& expired_tablet = tablet_ids[1];
  const int64_t now = GetCurrentTimeMicros();
  for (const auto& tablet_id : tablet_ids) {
    const bool stale = tablet_id == stale_tablet;
    NO_FATALS(DoctorCheckpointRow(stream_id, tablet_id,
                                  [&](SysCDCTabletCheckpointEntryPB* pb) {
      pb->set_op_index(7);
      pb->set_history_safe_time_micros(50);
      pb->set_snapshot_snap_ts(100);
      pb->set_snapshot_resume_key("k1");
      pb->set_snapshot_active(true);
      // The stale row's consumer polls without progress; the expired row's
      // consumer is gone.
      pb->set_last_active_time_micros(stale ? now : 1000);
      pb->set_last_checkpoint_advance_time_micros(stale ? 1000 : now);
    }));
  }
  NO_FATALS(RestartMaster());
  CatalogManager* cm = mini_master_->master()->catalog_manager();
  FLAGS_cdc_max_staleness_ms = 2 * 3600 * 1000;
  FLAGS_cdc_stream_expiry_ms = 2 * 3600 * 1000;
  cm->set_cdc_leader_ready_micros_for_tests(1000);

  // Two passes: a write of the first one must be visible to the second.
  RunMaintenance();
  RunMaintenance();
  EXPECT_EQ(0, cm->cdc_barriered_tablet_count())
      << "an abandoned snapshot must not keep its retention";
  for (const auto& tablet_id : tablet_ids) {
    SCOPED_TRACE(tablet_id);
    SysCDCTabletCheckpointEntryPB row;
    NO_FATALS(GetCheckpointRow(stream_id, tablet_id, &row));
    EXPECT_TRUE(row.snapshot_active());
    EXPECT_EQ(7, row.op_index());
    EXPECT_EQ(100, row.snapshot_snap_ts());
    EXPECT_EQ("k1", row.snapshot_resume_key());
  }

  GetCDCStreamInfoRequestPB req;
  req.set_stream_id(stream_id);
  req.set_include_tablet_status(true);
  GetCDCStreamInfoResponsePB resp;
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(proxy_->GetCDCStreamInfo(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  EXPECT_EQ(0, resp.stream().tablet_checkpoints_size())
      << "the streaming start point of an unfinished snapshot was reported as a "
      << "checkpoint: " << resp.stream().ShortDebugString();
  ASSERT_EQ(2, resp.stream().tablet_snapshot_cursors_size());
  for (const auto& cur : resp.stream().tablet_snapshot_cursors()) {
    EXPECT_TRUE(cur.snapshot_active());
    EXPECT_EQ(100, cur.snapshot_snap_ts());
    EXPECT_EQ(7, cur.streaming_start_op_index());
  }
  ASSERT_EQ(2, resp.stream().tablet_status_size());
  for (const auto& st : resp.stream().tablet_status()) {
    SCOPED_TRACE(st.tablet_id());
    EXPECT_TRUE(st.snapshot_active());
    EXPECT_EQ(st.tablet_id() == stale_tablet, st.stale());
    EXPECT_EQ(st.tablet_id() == expired_tablet, st.expired());
  }
}

// A leader that loses leadership (or crashes) in the middle of CreateCDCStream
// must not leave an ACTIVE stream with only some of its checkpoint rows: the
// client never got its id, yet it would pin WAL from op 0 on the written
// tablets. The stream stays CREATING until every row is durable, and the next
// leader reaps it.
TEST_F(CDCManagerTest, CreateStream_LeadershipLossMidFanoutLeavesNoActiveZombie) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("create-zombie", /*num_tablets=*/3, &table_id, &tablet_ids));
  ASSERT_EQ(3, tablet_ids.size());

  // The write of the third row fails, as it does once leadership is lost.
  FLAGS_cdc_create_stream_fail_checkpoint_idx = 2;
  string stream_id;
  ASSERT_FALSE(CreateStream(table_id, &stream_id).ok());
  FLAGS_cdc_create_stream_fail_checkpoint_idx = -1;

  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_EQ(1, streams.size());
  const string sid = streams.begin()->first;
  EXPECT_EQ(SysCDCStreamEntryPB::CREATING, streams.begin()->second.state())
      << "a partially created stream must never be durably ACTIVE";
  int rows = -1;
  ASSERT_OK(CountCheckpointRows(sid, &rows));
  EXPECT_EQ(2, rows);

  // A new leader loads the stream (this master is restarted before its own
  // maintenance pass could reap it). It must not serve it, and must reap it.
  NO_FATALS(RestartMaster());
  ListCDCStreamsResponsePB list;
  ASSERT_OK(ListStreams("", &list));
  EXPECT_EQ(0, list.streams_size());
  GetCDCStreamInfoResponsePB info;
  EXPECT_TRUE(GetStreamInfo(sid, &info).IsNotFound());
  EXPECT_TRUE(UpdateCheckpoint(sid, tablet_ids[0], 5).IsNotFound());

  RunMaintenance();
  ASSERT_OK(CountCheckpointRows(sid, &rows));
  EXPECT_EQ(0, rows);
  int stream_rows = -1;
  ASSERT_OK(CountStreamRows(sid, &stream_rows));
  EXPECT_EQ(0, stream_rows);
}

// While a replica is unreachable, each maintenance pass must not add another
// barrier task for it: at most one task per (tablet, replica) is in flight.
// Before, every pass added a task that retried for
// --unresponsive_ts_rpc_timeout_ms (1 hour by default).
TEST_F(CDCManagerTest, BarrierTasksToDeadTserverAreBounded) {
  FLAGS_tablet_creation_timeout_ms = 3600 * 1000;
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("dead-ts-bounded", &table_id, &tablet_id, &stream_id));
  NO_FATALS(SetFakeReplica(table_id, tablet_id, "down-ts"));
  for (int i = 0; i < 5; ++i) {
    RunMaintenance();
  }
  ASSERT_EQ(1, NumRunningBarrierTasks(table_id));
}

// A barrier task gives up after about two maintenance passes (the next pass
// sends the barrier again), not after --unresponsive_ts_rpc_timeout_ms.
TEST_F(CDCManagerTest, BarrierTaskDeadlineFollowsScanInterval) {
  FLAGS_tablet_creation_timeout_ms = 3600 * 1000;
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("dead-ts-deadline", &table_id, &tablet_id, &stream_id));
  NO_FATALS(SetFakeReplica(table_id, tablet_id, "down-ts"));
  // A deadline of max(2 x 1 s, 0.5 s) = 2 s.
  FLAGS_master_ts_rpc_timeout_ms = 500;
  FLAGS_cdc_bg_scan_interval_ms = 1000;
  RunMaintenance();
  ASSERT_EQ(1, NumRunningBarrierTasks(table_id));
  // Stop the background passes, so only the task sent above is checked.
  FLAGS_cdc_bg_scan_interval_ms = 3600 * 1000;
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, NumRunningBarrierTasks(table_id));
  });
}

// Reads the master's CDC gauge 'proto'.
int64_t MasterCDCGauge(Master* master, const GaugePrototype<int64_t>& proto) {
  scoped_refptr<Metric> m = master->metric_entity()->FindOrNull(proto);
  CHECK(m) << proto.name();
  return down_cast<FunctionGauge<int64_t>*>(m.get())->value();
}

// An expired checkpoint is logged and counted once, when it expires, and is
// reported as expired to operators.
TEST_F(CDCManagerTest, ExpiredCheckpointLoggedAndCountedOnce) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("expire-log", &table_id, &tablet_id, &stream_id));
  NO_FATALS(DoctorCheckpointRow(stream_id, tablet_id,
                                [](SysCDCTabletCheckpointEntryPB* pb) {
    pb->set_last_active_time_micros(1000);
  }));
  NO_FATALS(RestartMaster());
  CatalogManager* cm = mini_master_->master()->catalog_manager();
  // The first pass after the restart saw the row in its expiry grace.
  ASSERT_EQ(0, cm->cdc_tablet_checkpoint_expirations());
  cm->set_cdc_leader_ready_micros_for_tests(1000);

  StringVectorSink sink;
  ScopedRegisterSink reg(&sink);
  RunMaintenance();
  RunMaintenance();
  int num_logged = 0;
  for (const auto& msg : sink.logged_msgs()) {
    if (msg.find(Substitute("CDC stream $0 tablet $1 expired", stream_id, tablet_id)) !=
        string::npos) {
      ++num_logged;
    }
  }
  EXPECT_EQ(1, num_logged);
  EXPECT_EQ(1, cm->cdc_tablet_checkpoint_expirations());
  scoped_refptr<Counter> expirations = METRIC_cdc_tablet_checkpoint_expirations.Instantiate(
      mini_master_->master()->metric_entity());
  EXPECT_EQ(1, expirations->value());
  EXPECT_EQ(1, MasterCDCGauge(mini_master_->master(), METRIC_cdc_tablet_checkpoints_expired));

  GetCDCStreamInfoRequestPB req;
  req.set_stream_id(stream_id);
  req.set_include_tablet_status(true);
  GetCDCStreamInfoResponsePB resp;
  RpcController rpc;
  ASSERT_OK(proxy_->GetCDCStreamInfo(req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
  ASSERT_EQ(1, resp.stream().tablet_status_size());
  const auto& status = resp.stream().tablet_status(0);
  EXPECT_EQ(tablet_id, status.tablet_id());
  EXPECT_TRUE(status.expired());
  EXPECT_FALSE(status.stale());
  EXPECT_EQ(1000, status.last_active_time_micros());
  EXPECT_EQ(SysCDCStreamEntryPB::ACTIVE, resp.stream().state());
  EXPECT_GT(resp.stream().creation_time_micros(), 0);
  EXPECT_EQ(FLAGS_cdc_stream_expiry_ms, resp.stream_expiry_ms());
}

// The master exports how long ago the oldest checkpoint advanced, which also
// covers a consumer that stopped polling, and the number of streams by state.
TEST_F(CDCManagerTest, MasterExportsCheckpointAgeAndStreamCounts) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("ckpt-age", &table_id, &tablet_id, &stream_id));
  string other_stream_id;
  ASSERT_OK(CreateStream(table_id, &other_stream_id));
  ASSERT_OK(DeleteStream(other_stream_id));
  const int64_t kAgeMicros = 3600LL * 1000 * 1000;
  NO_FATALS(DoctorCheckpointRow(stream_id, tablet_id,
                                [&](SysCDCTabletCheckpointEntryPB* pb) {
    pb->set_last_checkpoint_advance_time_micros(GetCurrentTimeMicros() - kAgeMicros);
  }));
  NO_FATALS(RestartMaster());
  RunMaintenance();
  // The deleted stream is not counted as active.
  Master* master = mini_master_->master();
  EXPECT_GE(MasterCDCGauge(master, METRIC_cdc_max_checkpoint_age_micros), kAgeMicros);
  EXPECT_EQ(1, MasterCDCGauge(master, METRIC_cdc_streams_active));
}

// Streams being deleted are hidden from the default listing and description,
// but shown to an operator who asks for them.
TEST_F(CDCManagerTest, ListAndDescribeIncludeDeleting) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("list-deleting", &table_id, &tablet_id, &stream_id));
  ASSERT_OK(DeleteStream(stream_id));

  ListCDCStreamsResponsePB list_resp;
  ASSERT_OK(ListStreams(table_id, &list_resp));
  EXPECT_EQ(0, list_resp.streams_size());

  ListCDCStreamsRequestPB list_req;
  list_req.set_table_id(table_id);
  list_req.set_include_deleting(true);
  list_resp.Clear();
  {
    RpcController rpc;
    ASSERT_OK(proxy_->ListCDCStreams(list_req, &list_resp, &rpc));
  }
  ASSERT_FALSE(list_resp.has_error()) << list_resp.error().DebugString();
  ASSERT_EQ(1, list_resp.streams_size());
  EXPECT_EQ(stream_id, list_resp.streams(0).stream_id());
  EXPECT_EQ(SysCDCStreamEntryPB::DELETING, list_resp.streams(0).state());

  GetCDCStreamInfoResponsePB info_resp;
  EXPECT_TRUE(GetStreamInfo(stream_id, &info_resp).IsNotFound());
  GetCDCStreamInfoRequestPB info_req;
  info_req.set_stream_id(stream_id);
  info_req.set_include_deleting(true);
  info_resp.Clear();
  {
    RpcController rpc;
    ASSERT_OK(proxy_->GetCDCStreamInfo(info_req, &info_resp, &rpc));
  }
  ASSERT_FALSE(info_resp.has_error()) << info_resp.error().DebugString();
  EXPECT_EQ(SysCDCStreamEntryPB::DELETING, info_resp.stream().state());
}

// An ephemeral stream whose consumer went away is deleted once all its tablets
// have been idle for --cdc_ephemeral_stream_expiry_ms; a regular stream idle
// for as long is kept.
TEST_F(CDCManagerTest, IdleEphemeralStreamIsDeleted) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("ephemeral", /*num_tablets=*/1, &table_id, &tablet_ids));
  string ephemeral_id;
  {
    CreateCDCStreamRequestPB req;
    req.add_table_ids(table_id);
    req.set_ephemeral(true);
    CreateCDCStreamResponsePB resp;
    RpcController rpc;
    ASSERT_OK(proxy_->CreateCDCStream(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().DebugString();
    ephemeral_id = resp.stream_id();
  }
  string regular_id;
  ASSERT_OK(CreateStream(table_id, &regular_id));
  GetCDCStreamInfoResponsePB info;
  ASSERT_OK(GetStreamInfo(ephemeral_id, &info));
  EXPECT_TRUE(info.stream().ephemeral());
  EXPECT_EQ(FLAGS_cdc_ephemeral_stream_expiry_ms, info.stream_expiry_ms());

  // Idle for twice the ephemeral expiry, well within the regular one.
  const int64_t last_active =
      GetCurrentTimeMicros() - 2 * FLAGS_cdc_ephemeral_stream_expiry_ms * 1000;
  for (const auto& id : { ephemeral_id, regular_id }) {
    NO_FATALS(DoctorCheckpointRow(id, tablet_ids[0],
                                  [&](SysCDCTabletCheckpointEntryPB* pb) {
      pb->set_last_active_time_micros(last_active);
    }));
  }
  NO_FATALS(RestartMaster());
  mini_master_->master()->catalog_manager()->set_cdc_leader_ready_micros_for_tests(1000);
  RunMaintenance();

  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  ASSERT_TRUE(ContainsKey(streams, ephemeral_id));
  EXPECT_EQ(SysCDCStreamEntryPB::DELETING, streams[ephemeral_id].state());
  ASSERT_TRUE(ContainsKey(streams, regular_id));
  EXPECT_EQ(SysCDCStreamEntryPB::ACTIVE, streams[regular_id].state());
  EXPECT_FALSE(streams[regular_id].ephemeral());
}

// The master's CDC flags reject values outside their documented ranges.
TEST_F(CDCManagerTest, FlagValidatorsRejectBadValues) {
  EXPECT_TRUE(google::SetCommandLineOption("cdc_bg_scan_interval_ms", "0").empty());
  for (const char* flag : { "cdc_stream_expiry_ms", "cdc_max_staleness_ms",
                            "cdc_max_barrier_releases_per_run",
                            "cdc_barrier_refresh_interval_ms",
                            "cdc_ephemeral_stream_expiry_ms",
                            "cdc_max_streams", "cdc_max_streams_per_table" }) {
    SCOPED_TRACE(flag);
    EXPECT_TRUE(google::SetCommandLineOption(flag, "-1").empty());
  }
}

// Streams can be created only with --cdc_enabled. The master still advertises
// the CDC feature without it, so that existing streams can still be described
// and deleted by clients that require the feature.
TEST_F(CDCManagerTest, CreateStream_RejectedWhenDisabled) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("disabled-table", &table_id, &tablet_id, &stream_id));

  FLAGS_cdc_enabled = false;
  string other_stream_id;
  Status s = CreateStream(table_id, &other_stream_id);
  EXPECT_TRUE(s.IsNotSupported()) << s.ToString();
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  EXPECT_EQ(1, streams.size());
  {
    ConnectToMasterRequestPB req;
    ConnectToMasterResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    rpc.RequireServerFeature(MasterFeatures::CDC);
    ASSERT_OK(proxy_->ConnectToMaster(req, &resp, &rpc));
  }

  GetCDCStreamInfoResponsePB info;
  ASSERT_OK(GetStreamInfo(stream_id, &info));
  ASSERT_OK(DeleteStream(stream_id));
}

// --cdc_max_streams and --cdc_max_streams_per_table bound the number of active
// streams. A create over the limit writes nothing, and a deleted stream no
// longer counts.
TEST_F(CDCManagerTest, StreamQuotaEnforced) {
  FLAGS_cdc_max_streams = 3;
  FLAGS_cdc_max_streams_per_table = 2;
  vector<string> table_ids(3);
  for (size_t i = 0; i < table_ids.size(); ++i) {
    vector<string> tablet_ids;
    ASSERT_OK(CreateTable(Substitute("quota-table-$0", i), 1, &table_ids[i], &tablet_ids));
  }
  vector<string> stream_ids(3);
  ASSERT_OK(CreateStream(table_ids[0], &stream_ids[0]));
  ASSERT_OK(CreateStream(table_ids[0], &stream_ids[1]));
  string rejected_id;
  Status s = CreateStream(table_ids[0], &rejected_id);
  EXPECT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  EXPECT_STR_CONTAINS(s.ToString(), "--cdc_max_streams_per_table");
  ASSERT_OK(CreateStream(table_ids[1], &stream_ids[2]));
  s = CreateStream(table_ids[2], &rejected_id);
  EXPECT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  EXPECT_STR_CONTAINS(s.ToString(), "--cdc_max_streams");

  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  EXPECT_EQ(3, streams.size());
  vector<SysCDCTabletCheckpointEntryPB> rows;
  ASSERT_OK(VisitCheckpointRows(&rows));
  EXPECT_EQ(3, rows.size());

  ASSERT_OK(DeleteStream(stream_ids[0]));
  ASSERT_OK(CreateStream(table_ids[0], &stream_ids[0]));
}

// A stream records the user that created it. A user other than the service
// user and the superusers is not given the snapshot resume keys, which hold
// primary key values, but still sees which tablets are in their snapshot.
TEST_F(CDCManagerTest, GetStreamInfo_NoResumeKeyForNonServiceUser) {
  string table_id, tablet_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("owner-table", 1, &table_id, &tablet_ids));
  tablet_id = tablet_ids[0];

  unique_ptr<MasterServiceProxy> user_proxy(new MasterServiceProxy(
      client_messenger_, mini_master_->bound_rpc_addr(),
      mini_master_->bound_rpc_addr().host()));
  rpc::UserCredentials creds;
  creds.set_real_user("cdc-test-user");
  user_proxy->set_user_credentials(creds);
  string stream_id;
  {
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.add_table_ids(table_id);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(user_proxy->CreateCDCStream(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
    stream_id = resp.stream_id();
  }
  map<string, SysCDCStreamEntryPB> streams;
  ASSERT_OK(VisitStreamRows(&streams));
  EXPECT_EQ("cdc-test-user", streams[stream_id].owner());

  UpdateCDCCheckpointRequestPB req;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablet_id);
  req.set_op_index(7);
  req.set_snapshot_snap_ts(100);
  req.set_snapshot_resume_key("secret-pk");
  req.set_snapshot_active(true);
  ASSERT_OK(SendUpdateCheckpoint(req));

  // The service user (this test's process user) gets the resume key.
  GetCDCStreamInfoResponsePB resp;
  ASSERT_OK(GetStreamInfo(stream_id, &resp));
  ASSERT_EQ(1, resp.stream().tablet_snapshot_cursors_size());
  EXPECT_EQ("secret-pk", resp.stream().tablet_snapshot_cursors(0).snapshot_resume_key());
  EXPECT_EQ("cdc-test-user", resp.stream().owner());

  // Another user does not.
  GetCDCStreamInfoRequestPB info_req;
  info_req.set_stream_id(stream_id);
  resp.Clear();
  RpcController rpc;
  rpc.set_timeout(MonoDelta::FromSeconds(10));
  ASSERT_OK(user_proxy->GetCDCStreamInfo(info_req, &resp, &rpc));
  ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  ASSERT_EQ(1, resp.stream().tablet_snapshot_cursors_size());
  const auto& cursor = resp.stream().tablet_snapshot_cursors(0);
  EXPECT_EQ(tablet_id, cursor.tablet_id());
  EXPECT_TRUE(cursor.snapshot_active());
  EXPECT_FALSE(cursor.has_snapshot_resume_key());
}

// A table that was dropped cannot be authorized against. A stream over it is
// shown to, and may be deleted by, only its owner and the trusted users: under
// another user's request, the stream could otherwise give up changes it has
// not delivered yet.
TEST_F(CDCManagerTest, StreamOfDroppedTable_OnlyForOwner) {
  string table_id;
  vector<string> tablet_ids;
  ASSERT_OK(CreateTable("dropped-table", 1, &table_id, &tablet_ids));
  const auto proxy_as = [&](const string& user) {
    unique_ptr<MasterServiceProxy> proxy(new MasterServiceProxy(
        client_messenger_, mini_master_->bound_rpc_addr(),
        mini_master_->bound_rpc_addr().host()));
    rpc::UserCredentials creds;
    creds.set_real_user(user);
    proxy->set_user_credentials(creds);
    return proxy;
  };
  unique_ptr<MasterServiceProxy> owner_proxy = proxy_as("cdc-owner");
  unique_ptr<MasterServiceProxy> other_proxy = proxy_as("cdc-other-user");
  string stream_id;
  {
    CreateCDCStreamRequestPB req;
    CreateCDCStreamResponsePB resp;
    req.add_table_ids(table_id);
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(owner_proxy->CreateCDCStream(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
    stream_id = resp.stream_id();
  }
  ASSERT_OK(DropTable("dropped-table"));

  const auto list = [&](MasterServiceProxy* proxy, int* num_streams) {
    ListCDCStreamsRequestPB req;
    req.set_include_deleting(true);
    ListCDCStreamsResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy->ListCDCStreams(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    *num_streams = resp.streams_size();
    return Status::OK();
  };
  const auto describe = [&](MasterServiceProxy* proxy) {
    GetCDCStreamInfoRequestPB req;
    req.set_stream_id(stream_id);
    req.set_include_deleting(true);
    GetCDCStreamInfoResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy->GetCDCStreamInfo(req, &resp, &rpc));
    return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
  };
  const auto remove = [&](MasterServiceProxy* proxy) {
    DeleteCDCStreamRequestPB req;
    req.set_stream_id(stream_id);
    DeleteCDCStreamResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    RETURN_NOT_OK(proxy->DeleteCDCStream(req, &resp, &rpc));
    return resp.has_error() ? StatusFromPB(resp.error().status()) : Status::OK();
  };

  int num_streams = -1;
  ASSERT_OK(list(other_proxy.get(), &num_streams));
  EXPECT_EQ(0, num_streams);
  Status s = describe(other_proxy.get());
  EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();
  s = remove(other_proxy.get());
  EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();

  ASSERT_OK(list(owner_proxy.get(), &num_streams));
  EXPECT_EQ(1, num_streams);
  ASSERT_OK(describe(owner_proxy.get()));
  ASSERT_OK(remove(owner_proxy.get()));
  ASSERT_OK(list(proxy_.get(), &num_streams));
  EXPECT_EQ(1, num_streams);
}

// A checkpoint for a tablet of another table, or with a negative op index,
// would set or release the retention barrier of that tablet: the master
// rejects it and writes no row, alone or in a batch.
TEST_F(CDCManagerTest, UpdateCheckpoint_ForeignTabletOrNegativeIndexRejected) {
  string table_id, tablet_id, stream_id;
  ASSERT_OK(CreateTableAndStream("own-table", &table_id, &tablet_id, &stream_id));
  string other_table_id;
  vector<string> other_tablet_ids;
  ASSERT_OK(CreateTable("other-table", 1, &other_table_id, &other_tablet_ids));
  const string& other_tablet_id = other_tablet_ids[0];

  Status s = UpdateCheckpoint(stream_id, other_tablet_id, 5);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = UpdateCheckpoint(stream_id, "no-such-tablet", 5);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = UpdateCheckpoint(stream_id, tablet_id, -1);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();

  // In a batch, the valid update is applied and the others are dropped.
  {
    UpdateCDCCheckpointsRequestPB req;
    for (const auto& t : { std::make_pair(other_tablet_id, int64_t{9}),
                           std::make_pair(tablet_id, int64_t{-1}),
                           std::make_pair(tablet_id, int64_t{3}) }) {
      auto* u = req.add_updates();
      u->set_stream_id(stream_id);
      u->set_tablet_id(t.first);
      u->set_op_index(t.second);
    }
    UpdateCDCCheckpointsResponsePB resp;
    RpcController rpc;
    rpc.set_timeout(MonoDelta::FromSeconds(10));
    ASSERT_OK(proxy_->UpdateCDCCheckpoints(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
  }

  vector<SysCDCTabletCheckpointEntryPB> rows;
  ASSERT_OK(VisitCheckpointRows(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(tablet_id, rows[0].tablet_id());
  EXPECT_EQ(3, rows[0].op_index());
}

} // namespace master
} // namespace kudu
