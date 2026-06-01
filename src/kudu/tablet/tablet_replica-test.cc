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

#include "kudu/tablet/tablet_replica.h"

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_reader.h"  // IWYU pragma: keep
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/lock_manager.h"
#include "kudu/tablet/ops/alter_schema_op.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/op_driver.h"  // IWYU pragma: keep
#include "kudu/tablet/ops/op_tracker.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/tablet_replica_mm_ops.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/array_view.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_maintenance_manager);
DECLARE_int32(flush_threshold_mb);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_int64(cdc_stop_retaining_min_disk_mb);
DECLARE_int64(cdc_max_wal_retention_secs);
DECLARE_int64(cdc_barrier_lease_secs);
DECLARE_int64(cdc_max_wal_retention_bytes);
DECLARE_int64(disk_reserved_bytes_free_for_testing);
DECLARE_int64(fs_wal_dir_reserved_bytes);

METRIC_DECLARE_entity(tablet);

METRIC_DECLARE_gauge_bool(cdc_barrier_valve_open);
METRIC_DECLARE_gauge_int64(cdc_wal_retained_bytes);
METRIC_DECLARE_gauge_uint64(live_row_count);
METRIC_DECLARE_histogram(alter_schema_duration);

using kudu::clock::HybridClock;
using kudu::consensus::CommitMsg;
using kudu::consensus::ConsensusBootstrapInfo;
using kudu::consensus::OpId;
using kudu::consensus::RECEIVED_OPID;
using kudu::consensus::RaftConsensus;
using kudu::log::Log;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::tserver::AlterSchemaRequestPB;
using kudu::tserver::AlterSchemaResponsePB;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;

namespace kudu {

namespace tablet {

static Schema GetTestSchema() {
  return Schema({ ColumnSchema("key", INT32) }, 1);
}

// Sets the CDC retention barrier of 'meta' like an unsequenced master update
// without a refresh time, so the barrier lease does not apply. Returns true
// iff the barrier changed.
static bool SetCDCBarrier(TabletMetadata* meta, int64_t op_index,
                          uint64_t history_safe_time_micros) {
  return meta->ApplyCDCRetentionBarrierUpdate(op_index, history_safe_time_micros,
                                              /*barrier_seq=*/0, /*now_micros=*/0,
                                              /*refresh_flush_interval_micros=*/1);
}

class TabletReplicaTest : public TabletReplicaTestBase {
 public:
  TabletReplicaTest()
      : TabletReplicaTestBase(GetTestSchema()),
        insert_counter_(0),
        delete_counter_(0) {
  }

 protected:
  // Generate monotonic sequence of key column integers.
  Status GenerateSequentialInsertRequest(const Schema& schema,
                                         WriteRequestPB* write_req) {
    write_req->set_tablet_id(tablet()->tablet_id());
    RETURN_NOT_OK(SchemaToPB(schema, write_req->mutable_schema()));

    KuduPartialRow row(&schema);
    for (int i = 0; i < schema.num_columns(); i++) {
      RETURN_NOT_OK(row.SetInt32(i, insert_counter_++));
    }

    RowOperationsPBEncoder enc(write_req->mutable_row_operations());
    enc.Add(RowOperationsPB::INSERT, row);
    return Status::OK();
  }

  // Generate monotonic sequence of deletions, starting with 0.
  // Will assert if you try to delete more rows than you inserted.
  Status GenerateSequentialDeleteRequest(WriteRequestPB* write_req) {
    CHECK_LT(delete_counter_, insert_counter_);
    Schema schema(GetTestSchema());
    write_req->set_tablet_id(tablet()->tablet_id());
    CHECK_OK(SchemaToPB(schema, write_req->mutable_schema()));

    KuduPartialRow row(&schema);
    CHECK_OK(row.SetInt32("key", delete_counter_++));

    RowOperationsPBEncoder enc(write_req->mutable_row_operations());
    enc.Add(RowOperationsPB::DELETE, row);
    return Status::OK();
  }

  Status UpdateSchema(const SchemaPB& schema, int schema_version) {
    AlterSchemaRequestPB alter;
    alter.set_dest_uuid(tablet()->metadata()->fs_manager()->uuid());
    alter.set_tablet_id(tablet()->tablet_id());
    alter.set_schema_version(schema_version);
    *alter.mutable_schema() = schema;
    return ExecuteAlter(tablet_replica_.get(), alter);
  }

  Status ExecuteAlter(TabletReplica* replica, const AlterSchemaRequestPB& req) {
    unique_ptr<AlterSchemaResponsePB> resp(new AlterSchemaResponsePB());
    unique_ptr<AlterSchemaOpState> op_state(
        new AlterSchemaOpState(replica, &req, resp.get()));
    CountDownLatch rpc_latch(1);
    op_state->set_completion_callback(unique_ptr<OpCompletionCallback>(
          new LatchOpCompletionCallback<AlterSchemaResponsePB>(&rpc_latch, resp.get())));
    RETURN_NOT_OK(replica->SubmitAlterSchema(std::move(op_state)));
    rpc_latch.Wait();
    CHECK(!resp->has_error())
        << "\nReq:\n" << SecureDebugString(req) << "Resp:\n" << SecureDebugString(*resp);
    return Status::OK();
  }

  static Status RollLog(TabletReplica* replica) {
    RETURN_NOT_OK(replica->log_->WaitUntilAllFlushed());
    return replica->log_->AllocateSegmentAndRollOverForTests();
  }

  Status ExecuteWriteAndRollLog(TabletReplica* tablet_replica, const WriteRequestPB& req) {
    RETURN_NOT_OK(ExecuteWrite(tablet_replica, req));

    // Roll the log after each write.
    // Usually the append thread does the roll and no additional sync is required. However in
    // this test the thread that is appending is not the same thread that is rolling the log
    // so we must make sure the Log's queue is flushed before we roll or we might have a race
    // between the appender thread and the thread executing the test.
    CHECK_OK(RollLog(tablet_replica));
    return Status::OK();
  }

  // Execute insert requests and roll log after each one.
  Status ExecuteInsertsAndRollLogs(int num_inserts) {
    for (int i = 0; i < num_inserts; i++) {
      WriteRequestPB req;
      RETURN_NOT_OK(GenerateSequentialInsertRequest(GetTestSchema(), &req));
      RETURN_NOT_OK(ExecuteWriteAndRollLog(tablet_replica_.get(), req));
    }
    return Status::OK();
  }

  // Execute delete requests and roll log after each one.
  Status ExecuteDeletesAndRollLogs(int num_deletes) {
    for (int i = 0; i < num_deletes; i++) {
      WriteRequestPB req;
      RETURN_NOT_OK(GenerateSequentialDeleteRequest(&req));
      RETURN_NOT_OK(ExecuteWriteAndRollLog(tablet_replica_.get(), req));
    }
    return Status::OK();
  }

  // Assert that there are no log anchors held on the tablet replica.
  //
  // NOTE: when an op finishes and notifies the completion callback, it still is
  // registered with the op tracker for a very short time before being
  // destructed. So, this should always be called with an ASSERT_EVENTUALLY wrapper.
  void AssertNoLogAnchors() {
    // Make sure that there are no registered anchors in the registry
    ASSERT_EQ(0, tablet_replica()->log_anchor_registry()->GetAnchorCountForTests());
  }

  // Assert that the Log GC() anchor is earlier than the latest OpId in the Log.
  void AssertLogAnchorEarlierThanLogLatest() {
    log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
    std::optional<OpId> last_log_opid = tablet_replica_->consensus()->GetLastOpId(RECEIVED_OPID);
    ASSERT_TRUE(last_log_opid);
    ASSERT_LT(retention.for_durability, last_log_opid->index())
      << "Expected valid log anchor, got earliest opid: " << retention.for_durability
      << " (expected any value earlier than last log id: " << SecureShortDebugString(*last_log_opid)
      << ")";
  }

  // We disable automatic log GC. Don't leak those changes.
  google::FlagSaver flag_saver_;

  int32_t insert_counter_;
  int32_t delete_counter_;
};

// A Op that waits on the apply_continue latch inside of Apply().
class DelayedApplyOp : public WriteOp {
 public:
  DelayedApplyOp(CountDownLatch* apply_started,
                 CountDownLatch* apply_continue,
                 unique_ptr<WriteOpState> state)
      : WriteOp(std::move(state), consensus::LEADER),
        apply_started_(DCHECK_NOTNULL(apply_started)),
        apply_continue_(DCHECK_NOTNULL(apply_continue)) {
  }

  Status Apply(CommitMsg** commit_msg) override {
    apply_started_->CountDown();
    LOG(INFO) << "Delaying apply...";
    apply_continue_->Wait();
    LOG(INFO) << "Apply proceeding";
    return WriteOp::Apply(commit_msg);
  }

 private:
  CountDownLatch* apply_started_;
  CountDownLatch* apply_continue_;
  DISALLOW_COPY_AND_ASSIGN(DelayedApplyOp);
};

TEST_F(TabletReplicaTest, TestAlterSchemaMetric) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  const int orig_schema_version = tablet()->metadata()->schema_version();

  // Get the metric.
  auto alter_schema_duration =
    tablet_replica_->tablet()->metrics()->alter_schema_duration;
  const auto before_cnt = alter_schema_duration->TotalCount();

  // Add a new column.
  SchemaBuilder builder(*tablet()->metadata()->schema());
  ASSERT_OK(builder.AddColumn("new_col", INT32));
  SchemaPB new_schema;
  ASSERT_OK(SchemaToPB(builder.Build(), &new_schema));
  ASSERT_OK(UpdateSchema(new_schema, orig_schema_version + 1));

  ASSERT_EQ(before_cnt + 1, alter_schema_duration->TotalCount());
}

// Ensure that Log::GC() doesn't delete logs when the MRS has an anchor.
TEST_F(TabletReplicaTest, TestMRSAnchorPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log();
  int32_t num_gced;

  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  log::SegmentSequence segments;
  log->reader()->GetSegmentsSnapshot(&segments);

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(4, segments.size());

  NO_FATALS(AssertLogAnchorEarlierThanLogLatest());
  ASSERT_GT(tablet_replica_->log_anchor_registry()->GetAnchorCountForTests(), 0);

  // Ensure nothing gets deleted.
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(0, num_gced) << "earliest needed: " << retention.for_durability;

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // The first two segments should be deleted.
  // The last is anchored due to the commit in the last segment being the last
  // OpId in the log.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(2, num_gced) << "earliest needed: " << retention.for_durability;
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());
}

// Ensure that Log::GC() doesn't delete logs when the DMS has an anchor.
TEST_F(TabletReplicaTest, TestDMSAnchorPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log();
  shared_ptr<RaftConsensus> consensus = tablet_replica_->shared_consensus();
  int32_t num_gced;

  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  log::SegmentSequence segments;
  log->reader()->GetSegmentsSnapshot(&segments);

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(2));
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(3, segments.size());

  // Flush MRS & GC log so the next mutation goes into a DMS.
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  // We will only GC 1, and have 1 left because the earliest needed OpId falls
  // back to the latest OpId written to the Log if no anchors are set.
  ASSERT_EQ(1, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());

  std::optional<OpId> id = consensus->GetLastOpId(consensus::RECEIVED_OPID);
  ASSERT_TRUE(id);
  LOG(INFO) << "Before: " << *id;

  // We currently have no anchors and the last operation in the log is 0.3
  // Before the below was ExecuteDeletesAndRollLogs(1) but that was breaking
  // what I think is a wrong assertion.
  // I.e. since 0.4 is the last operation that we know is in memory 0.4 is the
  // last anchor we expect _and_ it's the last op in the log.
  // Only if we apply two operations is the last anchored operation and the
  // last operation in the log different.

  // Execute a mutation.
  ASSERT_OK(ExecuteDeletesAndRollLogs(2));
  NO_FATALS(AssertLogAnchorEarlierThanLogLatest());
  ASSERT_GT(tablet_replica_->log_anchor_registry()->GetAnchorCountForTests(), 0);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(4, segments.size());

  // Execute another couple inserts, but Flush it so it doesn't anchor.
  ASSERT_OK(ExecuteInsertsAndRollLogs(2));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(6, segments.size());

  // Ensure the delta and last insert remain in the logs, anchored by the delta.
  // Note that this will allow GC of the 2nd insert done above.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(1, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(5, segments.size());

  // Flush DMS to release the anchor.
  ASSERT_OK(tablet_replica_->tablet()->FlushBiggestDMSForTests());

  // Verify no anchors after Flush().
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // We should only hang onto one segment due to no anchors.
  // The last log OpId is the commit in the last segment, so it only anchors
  // that segment, not the previous, because it's not the first OpId in the
  // segment.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(3, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());
}

// Ensure that Log::GC() doesn't compact logs with OpIds of active ops.
TEST_F(TabletReplicaTest, TestActiveOpPreventsLogGC) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Log* log = tablet_replica_->log();
  int32_t num_gced;

  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  log::SegmentSequence segments;
  log->reader()->GetSegmentsSnapshot(&segments);

  ASSERT_EQ(1, segments.size());
  ASSERT_OK(ExecuteInsertsAndRollLogs(4));
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(5, segments.size());

  // Flush MRS as needed to ensure that we don't have OpId anchors in the MRS.
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(tablet_replica_->tablet()->Flush());

  // Verify no anchors after Flush().
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // Now create a long-lived op that hangs during Apply().
  // Allow other ops to go through. Logs should be populated, but the
  // long-lived op should prevent the log from being deleted since it
  // is in-flight.
  CountDownLatch rpc_latch(1);
  CountDownLatch apply_started(1);
  CountDownLatch apply_continue(1);
  unique_ptr<WriteRequestPB> req(new WriteRequestPB());
  unique_ptr<WriteResponsePB> resp(new WriteResponsePB());
  {
    // Long-running mutation.
    ASSERT_OK(GenerateSequentialDeleteRequest(req.get()));
    unique_ptr<WriteOpState> op_state(new WriteOpState(tablet_replica_.get(),
                                                       req.get(),
                                                       nullptr, // No RequestIdPB
                                                       resp.get()));

    op_state->set_completion_callback(unique_ptr<OpCompletionCallback>(
        new LatchOpCompletionCallback<WriteResponsePB>(&rpc_latch, resp.get())));

    unique_ptr<DelayedApplyOp> op(
        new DelayedApplyOp(&apply_started,
                           &apply_continue,
                           std::move(op_state)));

    shared_ptr<OpDriver> driver;
    ASSERT_OK(tablet_replica_->NewLeaderOpDriver(std::move(op),
                                                 &driver,
                                                 MonoTime::Max()));
    driver->ExecuteAsync();
    apply_started.Wait();
    ASSERT_TRUE(driver->GetOpId().IsInitialized())
      << "By the time an op is applied, it should have an Opid";
    // The apply will hang until we CountDown() the continue latch.
    // Now, roll the log. Below, we execute a few more insertions with rolling.
    ASSERT_OK(log->AllocateSegmentAndRollOverForTests());
  }

  ASSERT_EQ(1, tablet_replica_->op_tracker_.GetNumPendingForTests());
  // The log anchor is currently equal to the latest OpId written to the Log
  // because we are delaying the Commit message with the CountDownLatch.

  // GC the first four segments created by the inserts.
  log::RetentionIndexes retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(4, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());

  // We use mutations here, since an MRS Flush() quiesces the tablet, and we
  // want to ensure the only thing "anchoring" is the OpTracker.
  ASSERT_OK(ExecuteDeletesAndRollLogs(3));
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(5, segments.size());
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(tablet_replica_->tablet()->FlushBiggestDMSForTests());

  ASSERT_EVENTUALLY([&]{
      AssertNoLogAnchors();
      ASSERT_EQ(1, tablet_replica_->op_tracker_.GetNumPendingForTests());
    });

  NO_FATALS(AssertLogAnchorEarlierThanLogLatest());

  // Try to GC(), nothing should be deleted due to the in-flight op.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(0, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(5, segments.size());

  // Now we release the op and wait for everything to complete.
  // We fully quiesce and flush, which should release all anchors.
  ASSERT_EQ(1, tablet_replica_->op_tracker_.GetNumPendingForTests());
  apply_continue.CountDown();
  rpc_latch.Wait();
  tablet_replica_->op_tracker_.WaitForAllToFinish();
  ASSERT_EQ(0, tablet_replica_->op_tracker_.GetNumPendingForTests());
  ASSERT_OK(tablet_replica_->tablet()->FlushBiggestDMSForTests());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // All should be deleted except the two last segments.
  retention = tablet_replica_->GetRetentionIndexes();
  ASSERT_OK(log->GC(retention, &num_gced));
  ASSERT_EQ(3, num_gced);
  log->reader()->GetSegmentsSnapshot(&segments);
  ASSERT_EQ(2, segments.size());
}

// A tablet whose range partition was dropped, and which the master keeps for
// CDC, rejects writes once it is marked, and registers no op for them.
TEST_F(TabletReplicaTest, TestDroppedTabletRejectsWrites) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  WriteRequestPB req;
  ASSERT_OK(GenerateSequentialInsertRequest(GetTestSchema(), &req));
  ASSERT_OK(ExecuteWrite(tablet_replica_.get(), req));
  // ExecuteWrite() returns once the write completed, which can be before its op
  // left the tracker.
  tablet_replica_->op_tracker()->WaitForAllToFinish();
  ASSERT_EQ(0, tablet_replica_->op_tracker()->GetNumPendingForTests());

  ASSERT_FALSE(tablet_replica_->rejects_writes_of_dropped_tablet());
  ASSERT_TRUE(tablet_replica_->RejectWritesOfDroppedTablet());
  ASSERT_FALSE(tablet_replica_->RejectWritesOfDroppedTablet());
  ASSERT_TRUE(tablet_replica_->rejects_writes_of_dropped_tablet());
  WriteRequestPB rejected_req;
  ASSERT_OK(GenerateSequentialInsertRequest(GetTestSchema(), &rejected_req));
  Status s = ExecuteWrite(tablet_replica_.get(), rejected_req);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_EQ(0, tablet_replica_->op_tracker()->GetNumPendingForTests());
  uint64_t num_rows = 0;
  ASSERT_OK(tablet_replica_->tablet()->CountLiveRows(&num_rows));
  ASSERT_EQ(1, num_rows);
}

TEST_F(TabletReplicaTest, TestGCEmptyLog) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplica(info));
  // We don't wait on consensus on purpose.
  tablet_replica_->RunLogGC();
}

// When the CDC disk-pressure valve fires, GetRetentionIndexes() must release the
// in-memory MVCC/UNDO history floor in addition to the WAL clamp -- otherwise the
// valve is only half-open: WAL GC resumes but compaction/UNDO GC stays pinned to
// the stale floor during the exact disk-full event the valve exists to relieve.
TEST_F(TabletReplicaTest, TestCDCValveReleasesHistoryFloor) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  Tablet* tablet = tablet_replica_->tablet();

  // Simulate an active FULL/snapshot CDC stream: a persisted retention barrier
  // (op_index >= 0 arms the CDC path in GetRetentionIndexes) plus a live history
  // floor the master last pushed, and a later floor held by a local session.
  const uint64_t kHistoryMicros = 1234567890ULL;
  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            kHistoryMicros));
  tablet->SetCDCHistoryFloorHolder(Tablet::kCDCMasterHistoryFloorHolder,
                                   HybridClock::TimestampFromMicroseconds(kHistoryMicros));
  tablet->SetCDCHistoryFloorHolder(
      "session", HybridClock::TimestampFromMicroseconds(kHistoryMicros + 1));
  ASSERT_EQ(HybridClock::TimestampFromMicroseconds(kHistoryMicros),
            tablet->cdc_history_floor());

  // Valve closed: with the disk valve disabled and the age ceiling not tripped,
  // GetRetentionIndexes() must leave the floor intact.
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  FLAGS_cdc_max_wal_retention_secs = 0;
  (void)tablet_replica_->GetRetentionIndexes();
  ASSERT_EQ(HybridClock::TimestampFromMicroseconds(kHistoryMicros),
            tablet->cdc_history_floor())
      << "history floor must survive when no valve fires";

  // Valve open (disk pressure): a threshold larger than any real free space
  // forces the disk-pressure release. The history floor must now be cleared.
  FLAGS_cdc_stop_retaining_min_disk_mb = std::numeric_limits<int64_t>::max();
  (void)tablet_replica_->GetRetentionIndexes();
  ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor())
      << "disk-pressure valve must release the MVCC history floor";
  // The persisted floor is released too, so a restart does not re-pin it. The
  // op-index barrier itself stays: the valve only skips its clamp.
  ASSERT_EQ(0, tablet_replica_->tablet_metadata()->cdc_history_safe_time_micros());
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());
}

// The tablet server's CDC retention flags reject negative values, which would
// silently disable a valve, and retention times so large that converting them
// to microseconds or nanoseconds overflows. gflags validators run on runtime
// changes as well as at startup.
TEST_F(TabletReplicaTest, TestCDCRetentionFlagValidators) {
  for (const char* flag : { "cdc_wal_retention_secs", "cdc_stop_retaining_min_disk_mb",
                            "cdc_max_wal_retention_secs", "cdc_max_wal_retention_bytes",
                            "cdc_barrier_lease_secs" }) {
    SCOPED_TRACE(flag);
    EXPECT_TRUE(google::SetCommandLineOption(flag, "-1").empty());
    EXPECT_FALSE(google::SetCommandLineOption(flag, "0").empty());
  }
  // 100 years is accepted, more is not.
  constexpr int64_t kHundredYearsSecs = 100LL * 365 * 24 * 3600;
  for (const char* flag : { "cdc_max_wal_retention_secs", "cdc_barrier_lease_secs" }) {
    SCOPED_TRACE(flag);
    EXPECT_FALSE(google::SetCommandLineOption(
        flag, std::to_string(kHundredYearsSecs).c_str()).empty());
    EXPECT_TRUE(google::SetCommandLineOption(
        flag, std::to_string(kHundredYearsSecs + 1).c_str()).empty());
    EXPECT_TRUE(google::SetCommandLineOption(
        flag, std::to_string(std::numeric_limits<int64_t>::max()).c_str()).empty());
  }
}

// The CDC history floor is the minimum over its holders: a holder can raise or
// remove only its own floor, never another holder's.
TEST_F(TabletReplicaTest, TestCDCHistoryFloorIsMinimumOverHolders) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  Tablet* tablet = tablet_replica_->tablet();
  const Timestamp t1(1000);
  const Timestamp t2(2000);
  const Timestamp t3(3000);

  ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor());
  tablet->SetCDCHistoryFloorHolder(Tablet::kCDCMasterHistoryFloorHolder, t2);
  tablet->SetCDCHistoryFloorHolder("a", t1);
  tablet->SetCDCHistoryFloorHolder("b", t3);
  ASSERT_EQ(t1, tablet->cdc_history_floor());

  // Raising one holder does not raise the floor past another holder.
  tablet->SetCDCHistoryFloorHolder("a", t3);
  ASSERT_EQ(t2, tablet->cdc_history_floor());
  // Removing a holder (Timestamp(0)) leaves the others.
  tablet->SetCDCHistoryFloorHolder(Tablet::kCDCMasterHistoryFloorHolder, Timestamp(0));
  ASSERT_EQ(t3, tablet->cdc_history_floor());
  tablet->SetCDCHistoryFloorHolder("a", t1);
  ASSERT_EQ(t1, tablet->cdc_history_floor());

  // The ancient history mark is clamped to the floor.
  Timestamp ahm;
  ASSERT_TRUE(tablet->GetTabletAncientHistoryMark(&ahm));
  ASSERT_EQ(t1, ahm);

  tablet->ClearCDCHistoryFloorHolders();
  ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor());
}

// RaiseCDCHistoryFloorHolder() only ever unpins history: it raises or removes
// the floor of a holder, and never pins a floor for a holder that holds none
// (e.g. one the retention valve cleared).
TEST_F(TabletReplicaTest, TestCDCRaiseHistoryFloorHolderNeverPins) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  Tablet* tablet = tablet_replica_->tablet();
  const Timestamp t1(1000);
  const Timestamp t2(2000);
  const Timestamp t3(3000);

  tablet->RaiseCDCHistoryFloorHolder("a", t1);
  ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor());

  tablet->SetCDCHistoryFloorHolder("a", t2);
  tablet->RaiseCDCHistoryFloorHolder("a", t1);
  ASSERT_EQ(t2, tablet->cdc_history_floor());
  tablet->RaiseCDCHistoryFloorHolder("a", t3);
  ASSERT_EQ(t3, tablet->cdc_history_floor());
  tablet->RaiseCDCHistoryFloorHolder("a", Timestamp(0));
  ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor());
  tablet->RaiseCDCHistoryFloorHolder("a", t2);
  ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor());
}

// The history half of the disk-pressure valve also applies to a tablet
// without a retention barrier: the CDC history floors that remain there (the
// pin of a snapshot session) are released on low disk space too.
TEST_F(TabletReplicaTest, TestCDCDiskPressureReleasesHistoryFloorWithoutBarrier) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  Tablet* tablet = tablet_replica_->tablet();
  ASSERT_LT(tablet_replica_->tablet_metadata()->cdc_min_retained_op_index(), 0);
  const Timestamp kSnapTs = HybridClock::TimestampFromMicroseconds(1234567890ULL);
  tablet->SetCDCHistoryFloorHolder("session", kSnapTs);

  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  (void)tablet_replica_->GetRetentionIndexes();
  ASSERT_EQ(kSnapTs, tablet->cdc_history_floor());

  FLAGS_cdc_stop_retaining_min_disk_mb = std::numeric_limits<int64_t>::max();
  (void)tablet_replica_->GetRetentionIndexes();
  ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor());
  ASSERT_LT(tablet_replica_->tablet_metadata()->cdc_min_retained_op_index(), 0);
}

// The barrier-age ceiling (--cdc_max_wal_retention_secs) must also release the
// in-memory MVCC/UNDO history floor when it fires, not just the WAL clamp.
// Both valve conditions converge in the same skip_cdc_clamp block in
// GetRetentionIndexes(), so releasing one without the other is a latent bug.
// The age ceiling fires only when the barrier is actually pinning WAL
// (cdc_min_op_index < ret.for_durability), so we first do inserts to advance
// the Raft durability floor above the pinned barrier index.
TEST_F(TabletReplicaTest, TestCDCAgeCeilingValveReleasesHistoryFloor) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  // Build WAL segments and flush so the Raft durability floor advances well
  // above index 1; the age-ceiling pre-condition (barrier < Raft floor)
  // requires this.
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  Tablet* tablet = tablet_replica_->tablet();

  // Simulate an active FULL/snapshot CDC stream: barrier at index 1 plus a
  // live history floor.
  const uint64_t kHistoryMicros = 1234567890ULL;
  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            kHistoryMicros));
  tablet->SetCDCHistoryFloorHolder(Tablet::kCDCMasterHistoryFloorHolder,
                                   HybridClock::TimestampFromMicroseconds(kHistoryMicros));
  ASSERT_NE(Timestamp(0), tablet->cdc_history_floor());

  // Disable the disk valve so the age ceiling is the only active release path.
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  const int64_t kMaxRetainSecs = 3600;
  FLAGS_cdc_max_wal_retention_secs = kMaxRetainSecs;

  // The barrier was just set, so it is young: the floor must stay intact.
  (void)tablet_replica_->GetRetentionIndexes();
  ASSERT_EQ(HybridClock::TimestampFromMicroseconds(kHistoryMicros),
            tablet->cdc_history_floor())
      << "history floor must survive while the barrier is young";

  // Backdate the barrier's last refresh well past the ceiling. The age valve
  // must fire and clear the history floor to the no-floor sentinel.
  tablet_replica_->tablet_metadata()->set_cdc_barrier_refresh_monotime_for_tests(
      MonoTime::Now() - MonoDelta::FromSeconds(kMaxRetainSecs + 60));
  (void)tablet_replica_->GetRetentionIndexes();
  ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor())
      << "age-ceiling valve must release the MVCC history floor";
}

// V2/G2: end-to-end coverage of the disk-pressure backstop
// (--cdc_stop_retaining_min_disk_mb). With a CDC retention barrier pinning an
// early WAL index, the clamp normally holds segments the Raft floor would
// otherwise let go. When free space drops below the threshold the valve must
// (a) bump the cdc_barrier_forced_releases counter and (b) release the clamp so
// for_durability reverts to the true Raft floor and Log GC can reclaim the WAL.
TEST_F(TabletReplicaTest, TestCDCDiskPressureValveReleasesWAL) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  Log* log = tablet_replica_->log();

  // Build several WAL segments and flush the MRS so no MRS/DMS anchor holds the
  // early segments -- the CDC barrier is then the only thing pinning them.
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // Pin WAL from index 1 (below the true Raft durability floor, which now sits
  // near the last op after the flush). CHANGE stream: no history floor.
  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            /*history_safe_time_micros=*/0));

  // Valve closed: disk check disabled, age ceiling disabled. The clamp holds, so
  // for_durability is pinned at the barrier and the counter does not move.
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  FLAGS_cdc_max_wal_retention_secs = 0;
  const int64_t releases_before = tablet_replica_->cdc_barrier_forced_releases_for_tests();
  const int64_t fd_clamped = tablet_replica_->GetRetentionIndexes().for_durability;
  ASSERT_EQ(releases_before, tablet_replica_->cdc_barrier_forced_releases_for_tests())
      << "counter must not move while no valve fires";
  ASSERT_EQ(fd_clamped, 1)
      << "barrier must clamp for_durability to exactly the pinned index";

  // Valve open: a threshold larger than any real free space forces the disk
  // release. The counter increments and the clamp is skipped, so for_durability
  // reverts above the barrier.
  FLAGS_cdc_stop_retaining_min_disk_mb = std::numeric_limits<int64_t>::max();
  const log::RetentionIndexes released = tablet_replica_->GetRetentionIndexes();
  ASSERT_EQ(releases_before + 1, tablet_replica_->cdc_barrier_forced_releases_for_tests())
      << "disk-pressure valve must increment cdc_barrier_forced_releases";
  ASSERT_GT(released.for_durability, fd_clamped)
      << "disk-pressure valve must release the WAL clamp (for_durability reverts "
         "to the true Raft floor)";

  // And GC actually proceeds now that the clamp is released.
  int32_t num_gced = 0;
  ASSERT_OK(log->GC(released, &num_gced));
  ASSERT_GT(num_gced, 0) << "released barrier must let Log GC reclaim WAL segments";
}

// V2/G2: end-to-end coverage of the barrier-age ceiling
// (--cdc_max_wal_retention_secs), the dead-master backstop. If the master stops
// refreshing the barrier for longer than the ceiling, the tserver must release
// it on its own: bump cdc_barrier_forced_releases and skip the clamp. The last
// refresh is backdated via a test hook so the test is deterministic (no sleep).
TEST_F(TabletReplicaTest, TestCDCAgeCeilingValveReleasesWAL) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  Log* log = tablet_replica_->log();

  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            /*history_safe_time_micros=*/0));

  // Disable the disk valve so the age ceiling is the only release path.
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  const int64_t kMaxRetainSecs = 3600;
  FLAGS_cdc_max_wal_retention_secs = kMaxRetainSecs;

  // The barrier was just set: it clamps for_durability, and the counter must
  // not move (age not yet exceeded).
  const int64_t releases_before = tablet_replica_->cdc_barrier_forced_releases_for_tests();
  const int64_t fd_clamped = tablet_replica_->GetRetentionIndexes().for_durability;
  ASSERT_EQ(releases_before, tablet_replica_->cdc_barrier_forced_releases_for_tests())
      << "age ceiling must not fire for a young barrier";
  ASSERT_EQ(fd_clamped, 1)
      << "barrier must clamp for_durability to exactly the pinned index";

  // Backdate the barrier's last refresh well past the ceiling and re-evaluate:
  // the age valve fires.
  tablet_replica_->tablet_metadata()->set_cdc_barrier_refresh_monotime_for_tests(
      MonoTime::Now() - MonoDelta::FromSeconds(kMaxRetainSecs + 60));
  const log::RetentionIndexes released = tablet_replica_->GetRetentionIndexes();
  ASSERT_EQ(releases_before + 1, tablet_replica_->cdc_barrier_forced_releases_for_tests())
      << "age ceiling must increment cdc_barrier_forced_releases once exceeded";
  ASSERT_GT(released.for_durability, fd_clamped)
      << "age ceiling must release the WAL clamp (for_durability reverts to the "
         "true Raft floor)";

  int32_t num_gced = 0;
  ASSERT_OK(log->GC(released, &num_gced));
  ASSERT_GT(num_gced, 0) << "released barrier must let Log GC reclaim WAL segments";
}

// A barrier that the master stopped refreshing is released once its lease
// (--cdc_barrier_lease_secs) has run out, as after a master downgrade that
// left the barrier behind. The refresh time is persisted, so a restart does not
// extend the lease. The age ceiling cannot do this: a restart re-stamps its
// in-memory clock, so a tablet server restarted more often than that ceiling
// would retain the WAL forever.
TEST_F(TabletReplicaTest, TestCDCBarrierLeaseSurvivesRestart) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  FLAGS_cdc_max_wal_retention_secs = 3600;
  const int64_t kLeaseSecs = 3600;
  FLAGS_cdc_barrier_lease_secs = kLeaseSecs;

  // The master last refreshed the barrier at index 1 longer ago than the lease.
  const int64_t kStaleRefresh = GetCurrentTimeMicros() - (kLeaseSecs + 60) * 1000000LL;
  ASSERT_TRUE(tablet_replica_->tablet_metadata()->ApplyCDCRetentionBarrierUpdate(
      /*op_index=*/1, /*history_safe_time_micros=*/0, /*barrier_seq=*/5, kStaleRefresh,
      /*refresh_flush_interval_micros=*/1));
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());

  ASSERT_OK(RestartReplica());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });
  ASSERT_EQ(1, tablet_replica_->tablet_metadata()->cdc_min_retained_op_index());
  ASSERT_EQ(5, tablet_replica_->tablet_metadata()->cdc_barrier_seq());
  ASSERT_EQ(kStaleRefresh, tablet_replica_->tablet_metadata()->cdc_barrier_refresh_micros());

  const int64_t releases_before = tablet_replica_->cdc_barrier_forced_releases_for_tests();
  const log::RetentionIndexes released = tablet_replica_->GetRetentionIndexes();
  ASSERT_GT(released.for_durability, 1)
      << "the expired lease must release the WAL clamp after a restart";
  ASSERT_EQ(releases_before + 1, tablet_replica_->cdc_barrier_forced_releases_for_tests());

  // A master refresh renews the lease, and the barrier clamps again.
  ASSERT_TRUE(tablet_replica_->tablet_metadata()->ApplyCDCRetentionBarrierUpdate(
      1, 0, 6, GetCurrentTimeMicros(), kLeaseSecs * 1000000LL / 8));
  ASSERT_EQ(1, tablet_replica_->GetRetentionIndexes().for_durability);
}

// A master release of the CDC retention barrier that reaches a replica while it
// bootstraps updates only the tablet metadata, since the replica has not
// published its tablet yet. The tablet, opened before the release, holds the
// master's history floor of the superblock: the replica must apply the
// released floor when it starts, or the floor would pin the tablet's history
// until the next restart. A barrier set in that window must be applied too.
TEST_F(TabletReplicaTest, TestCDCBarrierUpdateDuringBootstrapReachesTablet) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  // No retention valve may release the floor in this test.
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  FLAGS_cdc_max_wal_retention_secs = 0;
  const uint64_t kFloorMicros = GetCurrentTimeMicros() - 3600 * 1000000LL;
  const Timestamp kFloor = HybridClock::TimestampFromMicroseconds(kFloorMicros);
  ASSERT_TRUE(tablet_replica_->tablet_metadata()->ApplyCDCRetentionBarrierUpdate(
      /*op_index=*/1, kFloorMicros, /*barrier_seq=*/5, GetCurrentTimeMicros(),
      /*refresh_flush_interval_micros=*/1));
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());

  // The release lands between the bootstrap, which opens the tablet with the
  // persisted floor, and the start of the replica, as when
  // CDCServiceImpl::SetRetentionBarrier() finds no published tablet.
  ASSERT_OK(RestartReplica(/*reset_tablet=*/false, [&](const shared_ptr<Tablet>& tablet) {
    ASSERT_EQ(nullptr, tablet_replica_->shared_tablet());
    ASSERT_EQ(kFloor, tablet->cdc_history_floor());
    ASSERT_TRUE(tablet_replica_->tablet_metadata()->ApplyCDCRetentionBarrierUpdate(
        /*op_index=*/-1, /*history_safe_time_micros=*/0, /*barrier_seq=*/6,
        GetCurrentTimeMicros(), /*refresh_flush_interval_micros=*/1));
  }));
  ASSERT_EQ(Timestamp(0), tablet_replica_->tablet()->cdc_history_floor())
      << "a barrier release during the bootstrap must reach the tablet";

  // The same for a barrier set during the bootstrap.
  const uint64_t kNewFloorMicros = kFloorMicros + 1000000;
  ASSERT_OK(RestartReplica(/*reset_tablet=*/false, [&](const shared_ptr<Tablet>& tablet) {
    ASSERT_EQ(Timestamp(0), tablet->cdc_history_floor());
    ASSERT_TRUE(tablet_replica_->tablet_metadata()->ApplyCDCRetentionBarrierUpdate(
        /*op_index=*/1, kNewFloorMicros, /*barrier_seq=*/7, GetCurrentTimeMicros(),
        /*refresh_flush_interval_micros=*/1));
  }));
  ASSERT_EQ(HybridClock::TimestampFromMicroseconds(kNewFloorMicros),
            tablet_replica_->tablet()->cdc_history_floor())
      << "a barrier set during the bootstrap must reach the tablet";
}

// Negative case: NEITHER valve must fire under normal conditions (healthy disk,
// young barrier). A spurious release would send a WAL_EXPIRED response to a
// live CDC consumer, forcing an unnecessary re-snapshot. This test exercises the
// actual comparison logic of each valve with the valves enabled at their
// production defaults, not disabled -- verifying the condition evaluates
// correctly, not just that the code is skipped when the flag is zero.
//   (a) disk-pressure: flag = 100 MB (default), actual WAL dir free space >>
//       the WAL reservation + 100 MB on any viable build machine -- the valve
//       must not release.
//   (b) age-ceiling: flag = 3600 s (default), barrier just observed (age ~ 0 s
//       << 3600 s) -- age_secs > max_retain_secs must evaluate to false.
TEST_F(TabletReplicaTest, TestCDCValveNoSpuriousRelease) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });

  // Pin WAL from index 1 with no history floor (CHANGE stream).
  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            /*history_safe_time_micros=*/0));

  // Both valves enabled at production defaults. On a healthy machine neither
  // condition (low disk / old barrier) is met.
  FLAGS_cdc_stop_retaining_min_disk_mb = 100;
  FLAGS_cdc_max_wal_retention_secs = 3600;

  const int64_t releases_before = tablet_replica_->cdc_barrier_forced_releases_for_tests();

  // The barrier was just set, so its age is effectively 0 s.
  const log::RetentionIndexes ret = tablet_replica_->GetRetentionIndexes();

  ASSERT_EQ(releases_before, tablet_replica_->cdc_barrier_forced_releases_for_tests())
      << "no valve must fire on healthy disk (> 100 MB free) with a freshly "
         "observed barrier (age ~ 0 s, ceiling = 3600 s)";
  ASSERT_EQ(ret.for_durability, 1)
      << "CDC clamp must remain active: for_durability must equal the barrier "
         "index when no valve fires";
}

// The disk-pressure valve fires while the free space is still above the WAL
// reservation (--fs_wal_dir_reserved_bytes), which WAL segment allocation
// enforces: below the reservation the next allocation fails, so a valve that
// only fires there comes too late.
TEST_F(TabletReplicaTest, TestCDCDiskValveFiresAboveWalReservation) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });
  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            /*history_safe_time_micros=*/0));
  FLAGS_cdc_max_wal_retention_secs = 0;
  FLAGS_cdc_barrier_lease_secs = 0;
  FLAGS_cdc_stop_retaining_min_disk_mb = 100;
  constexpr int64_t kMiB = 1024 * 1024;
  constexpr int64_t kReserved = 10 * 1024 * kMiB;
  FLAGS_fs_wal_dir_reserved_bytes = kReserved;

  // Plenty of space above the reservation: the barrier clamps.
  FLAGS_disk_reserved_bytes_free_for_testing = kReserved + 1024 * kMiB;
  ASSERT_EQ(1, tablet_replica_->GetRetentionIndexes().for_durability);
  ASSERT_FALSE(tablet_replica_->cdc_retention_valve_open());

  // Far more than 100 MB free in absolute terms, but less than 100 MB above
  // the reservation: the valve must open, before the WAL runs out of space.
  FLAGS_disk_reserved_bytes_free_for_testing = kReserved + 50 * kMiB;
  ASSERT_GT(tablet_replica_->GetRetentionIndexes().for_durability, 1)
      << "the disk valve must fire within --cdc_stop_retaining_min_disk_mb of the "
         "WAL reservation";
  ASSERT_TRUE(tablet_replica_->cdc_retention_valve_open());
}

// The age ceiling measures the time since the master last refreshed the
// barrier, not since its op index last changed: the barrier of an idle tablet
// whose consumer is caught up does not move, but the master keeps refreshing
// it, and that barrier must keep retaining.
TEST_F(TabletReplicaTest, TestCDCAgeValveNoFireOnIdleRefreshedBarrier) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  FLAGS_cdc_barrier_lease_secs = 0;
  FLAGS_cdc_max_wal_retention_secs = 1;
  const int64_t releases_before = tablet_replica_->cdc_barrier_forced_releases_for_tests();

  // The master refreshes the unchanged barrier several times per ceiling, for
  // more than two ceilings.
  const MonoTime deadline = MonoTime::Now() + MonoDelta::FromMilliseconds(2500);
  while (MonoTime::Now() < deadline) {
    SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                  /*history_safe_time_micros=*/0);
    ASSERT_EQ(1, tablet_replica_->GetRetentionIndexes().for_durability);
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                /*history_safe_time_micros=*/0);
  ASSERT_EQ(1, tablet_replica_->GetRetentionIndexes().for_durability)
      << "the age valve fired for a barrier the master keeps refreshing";
  ASSERT_EQ(releases_before, tablet_replica_->cdc_barrier_forced_releases_for_tests());
}

// The forced-release counter counts the times the valve opens, not the
// evaluations while it is open: the maintenance manager evaluates it several
// times a second through the GC stats. While the valve is open, the retained
// bytes gauge reports 0, matching what log GC does.
TEST_F(TabletReplicaTest, TestCDCForcedReleaseCounterCountsTransitions) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });
  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            /*history_safe_time_micros=*/0));
  FLAGS_cdc_max_wal_retention_secs = 0;
  FLAGS_cdc_barrier_lease_secs = 0;
  // The gauges the replica registered on its tablet's metric entity.
  const auto& entity = tablet_replica_->tablet()->GetMetricEntity();
  const scoped_refptr<Metric> retained_bytes_metric =
      entity->FindOrNull(METRIC_cdc_wal_retained_bytes);
  ASSERT_NE(nullptr, retained_bytes_metric.get());
  const auto* retained_bytes =
      down_cast<FunctionGauge<int64_t>*>(retained_bytes_metric.get());
  const scoped_refptr<Metric> valve_open_metric =
      entity->FindOrNull(METRIC_cdc_barrier_valve_open);
  ASSERT_NE(nullptr, valve_open_metric.get());
  const auto* valve_open = down_cast<FunctionGauge<bool>*>(valve_open_metric.get());

  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  ASSERT_EQ(1, tablet_replica_->GetRetentionIndexes().for_durability);
  ASSERT_GT(retained_bytes->value(), 0);
  ASSERT_FALSE(valve_open->value());
  const int64_t releases_before = tablet_replica_->cdc_barrier_forced_releases_for_tests();

  FLAGS_cdc_stop_retaining_min_disk_mb = std::numeric_limits<int64_t>::max();
  for (int i = 0; i < 5; i++) {
    int64_t gcable = 0;
    ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable));
    ASSERT_GT(gcable, 0);
  }
  ASSERT_EQ(releases_before + 1, tablet_replica_->cdc_barrier_forced_releases_for_tests());
  ASSERT_TRUE(valve_open->value());
  ASSERT_EQ(0, retained_bytes->value());

  // Closing and reopening the valve counts once more.
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  ASSERT_EQ(1, tablet_replica_->GetRetentionIndexes().for_durability);
  ASSERT_FALSE(valve_open->value());
  FLAGS_cdc_stop_retaining_min_disk_mb = std::numeric_limits<int64_t>::max();
  for (int i = 0; i < 3; i++) {
    ASSERT_GT(tablet_replica_->GetRetentionIndexes().for_durability, 1);
  }
  ASSERT_EQ(releases_before + 2, tablet_replica_->cdc_barrier_forced_releases_for_tests());
}

// --cdc_max_wal_retention_bytes bounds the WAL a CDC barrier may retain.
TEST_F(TabletReplicaTest, TestCDCByteCeilingValveReleasesWAL) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });
  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            /*history_safe_time_micros=*/0));
  FLAGS_cdc_stop_retaining_min_disk_mb = 0;
  FLAGS_cdc_max_wal_retention_secs = 0;
  FLAGS_cdc_barrier_lease_secs = 0;

  FLAGS_cdc_max_wal_retention_bytes = std::numeric_limits<int64_t>::max();
  ASSERT_EQ(1, tablet_replica_->GetRetentionIndexes().for_durability);

  FLAGS_cdc_max_wal_retention_bytes = 1;
  const log::RetentionIndexes released = tablet_replica_->GetRetentionIndexes();
  ASSERT_GT(released.for_durability, 1);
  int32_t num_gced = 0;
  ASSERT_OK(tablet_replica_->log()->GC(released, &num_gced));
  ASSERT_GT(num_gced, 0);
}

// A stopped replica's metric entity can still be scraped until it is retired,
// but by then the log is closed. The cdc_wal_retained_bytes gauge must report
// 0 instead of reading the closed log.
TEST_F(TabletReplicaTest, TestCDCRetainedBytesGaugeSafeAfterStop) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  ASSERT_OK(ExecuteInsertsAndRollLogs(3));
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_EVENTUALLY([&]{ AssertNoLogAnchors(); });
  ASSERT_TRUE(SetCDCBarrier(tablet_replica_->tablet_metadata().get(), /*op_index=*/1,
                            /*history_safe_time_micros=*/0));

  // The gauge is already instantiated by the replica, so the function given
  // here is ignored and the replica's own callback is returned.
  auto retained_bytes = METRIC_cdc_wal_retained_bytes.InstantiateFunctionGauge(
      tablet_replica_->tablet()->GetMetricEntity(), []() { return int64_t{-1}; });
  ASSERT_GT(retained_bytes->value(), 0);

  tablet_replica_->Shutdown();
  ASSERT_EQ(0, retained_bytes->value());
}

TEST_F(TabletReplicaTest, TestFlushOpsPerfImprovements) {
  FLAGS_flush_threshold_mb = 64;

  MaintenanceOpStats stats;

  // Just on the threshold and not enough time has passed for a time-based flush,
  // we'll expect improvement equal to '1'.
  stats.set_ram_anchored(64 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 1);
  ASSERT_EQ(1.0, stats.perf_improvement());
  stats.Clear();

  // Below the threshold and enough time has passed, we'll have a low improvement.
  stats.set_ram_anchored(2 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 3 * 60 * 1000);
  ASSERT_LT(0.01, stats.perf_improvement());
  ASSERT_GT(0.1, stats.perf_improvement());
  stats.Clear();

  // Over the threshold, we expect improvement equal to the excess MB.
  stats.set_ram_anchored(128 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 1);
  ASSERT_NEAR(stats.perf_improvement(), 64, 0.01);
  stats.Clear();

  // Below the threshold but have been there a long time, closing in to 1.0.
  stats.set_ram_anchored(1);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 60 * 50 * 1000);
  ASSERT_LT(0.7, stats.perf_improvement());
  ASSERT_GT(1.0, stats.perf_improvement());
  stats.Clear();

  // Approaching threshold, enough time has passed but haven't been there a long time,
  // closing in to 1.0.
  stats.set_ram_anchored(63 * 1024 * 1024);
  FlushOpPerfImprovementPolicy::SetPerfImprovementForFlush(&stats, 3 * 60 * 1000);
  ASSERT_LT(0.9, stats.perf_improvement());
  ASSERT_GT(1.0, stats.perf_improvement());
  stats.Clear();
}

// Test that the schema of a tablet will be rolled forward upon replaying an
// alter schema request.
TEST_F(TabletReplicaTest, TestRollLogSegmentSchemaOnAlter) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  SchemaPB orig_schema_pb;
  ASSERT_OK(SchemaToPB(SchemaBuilder(*tablet()->metadata()->schema()).Build(), &orig_schema_pb));
  const int orig_schema_version = tablet()->metadata()->schema_version();

  // Add a new column.
  SchemaBuilder builder(*tablet()->metadata()->schema());
  ASSERT_OK(builder.AddColumn("new_col", INT32));
  Schema new_client_schema = builder.BuildWithoutIds();
  SchemaPB new_schema;
  ASSERT_OK(SchemaToPB(builder.Build(), &new_schema));
  ASSERT_OK(UpdateSchema(new_schema, orig_schema_version + 1));

  const auto write = [&] {
    unique_ptr<WriteRequestPB> req(new WriteRequestPB());
    ASSERT_OK(GenerateSequentialInsertRequest(new_client_schema, req.get()));
    ASSERT_OK(ExecuteWrite(tablet_replica_.get(), *req));
  };
  // Upon restarting, our log segment header schema should have "new_col".
  NO_FATALS(write());
  ASSERT_OK(RestartReplica());

  // Get rid of the alter in the WALs.
  NO_FATALS(write());
  ASSERT_OK(RollLog(tablet_replica_.get()));
  NO_FATALS(write());
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  tablet_replica_->RunLogGC();

  // Now write some more and restart. If our segment header schema previously
  // didn't have "new_col", bootstrapping would fail, complaining about a
  // mismatch between the segment header schema and the write request schema.
  NO_FATALS(write());
  ASSERT_OK(RestartReplica());
}

// Regression test for KUDU-2690, wherein a alter schema request that failed
// (e.g. because of an invalid schema) would roll forward the log segment
// header schema, causing a failure or crash upon bootstrapping.
TEST_F(TabletReplicaTest, Kudu2690Test) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  SchemaPB orig_schema_pb;
  ASSERT_OK(SchemaToPB(SchemaBuilder(*tablet()->metadata()->schema()).Build(), &orig_schema_pb));
  const int orig_schema_version = tablet()->metadata()->schema_version();

  // First things first, add a new column.
  SchemaBuilder builder(*tablet()->metadata()->schema());
  ASSERT_OK(builder.AddColumn("new_col", INT32));
  Schema new_client_schema = builder.BuildWithoutIds();
  SchemaPB new_schema;
  ASSERT_OK(SchemaToPB(builder.Build(), &new_schema));
  ASSERT_OK(UpdateSchema(new_schema, orig_schema_version + 1));

  // Try to update the schema to an older version. Before the fix for
  // KUDU-2690, this would revert the schema in the next log segment header
  // upon rolling the log below.
  ASSERT_OK(UpdateSchema(orig_schema_pb, orig_schema_version));

  // Roll onto a new segment so we can begin filling a new segment. This allows
  // us to GC the first segment.
  ASSERT_OK(RollLog(tablet_replica_.get()));
  {
    unique_ptr<WriteRequestPB> req(new WriteRequestPB());
    ASSERT_OK(GenerateSequentialInsertRequest(new_client_schema, req.get()));
    ASSERT_OK(ExecuteWrite(tablet_replica_.get(), *req));
  }
  tablet_replica_->RunLogGC();

  // Before KUDU-2960 was fixed, bootstrapping would fail, complaining that the
  // write requests contained a column that was not in the log segment header's
  // schema.
  ASSERT_OK(RestartReplica());
}

TEST_F(TabletReplicaTest, TestLiveRowCountMetric) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  // We don't care what the function is, since the metric is already instantiated.
  auto live_row_count = METRIC_live_row_count.InstantiateFunctionGauge(
      tablet_replica_->tablet()->GetMetricEntity(), [](){ return 0; });
  ASSERT_EQ(0, live_row_count->value());

  // Insert some rows.
  Random rand(SeedRandom());
  const int kNumInsert = rand.Next() % 100 + 1;
  ASSERT_OK(ExecuteInsertsAndRollLogs(kNumInsert));
  ASSERT_EQ(kNumInsert, live_row_count->value());

  // Delete some rows.
  const int kNumDelete = rand.Next() % kNumInsert;
  ASSERT_OK(ExecuteDeletesAndRollLogs(kNumDelete));
  ASSERT_EQ(kNumInsert - kNumDelete, live_row_count->value());
}

TEST_F(TabletReplicaTest, TestRestartAfterGCDeletedRowsets) {
  FLAGS_enable_maintenance_manager = false;
  FLAGS_tablet_history_max_age_sec = 1;
  const int kNumRows = 10;
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  auto* tablet = tablet_replica_->tablet();
  // Metrics are already registered so pass a dummy lambda.
  auto live_row_count = METRIC_live_row_count.InstantiateFunctionGauge(
      tablet->GetMetricEntity(), [] () { return 0; });

  // Insert some rows and flush so we get a DRS, and then delete them so we
  // have an ancient, fully deleted DRS.
  ASSERT_OK(ExecuteInsertsAndRollLogs(kNumRows));
  ASSERT_OK(tablet->Flush());
  ASSERT_OK(ExecuteDeletesAndRollLogs(kNumRows));
  ASSERT_EQ(1, tablet->num_rowsets());
  ASSERT_EQ(0, live_row_count->value());
  SleepFor(MonoDelta::FromSeconds(FLAGS_tablet_history_max_age_sec));

  // Insert some fresh rows so we can validate that we don't GC everything.
  ASSERT_OK(ExecuteInsertsAndRollLogs(kNumRows));
  ASSERT_OK(tablet->Flush());
  ASSERT_EQ(2, tablet->num_rowsets());
  ASSERT_EQ(kNumRows, live_row_count->value());

  // Now GC what we can. The first rowset should be gone.
  ASSERT_OK(tablet->DeleteAncientDeletedRowsets());
  ASSERT_EQ(1, tablet->num_rowsets());
  ASSERT_EQ(kNumRows, live_row_count->value());
  ASSERT_OK(ExecuteDeletesAndRollLogs(kNumRows));
  ASSERT_EQ(0, live_row_count->value());

  // Restart and ensure we can rebuild our DMS okay.
  ASSERT_OK(RestartReplica());
  tablet = tablet_replica_->tablet();
  ASSERT_EQ(1, tablet->num_rowsets());
  live_row_count = METRIC_live_row_count.InstantiateFunctionGauge(
      tablet->GetMetricEntity(), [] () { return 0; });
  ASSERT_EQ(0, live_row_count->value());

  // Now do that again but with deltafiles.
  ASSERT_OK(tablet->FlushBiggestDMSForTests());
  ASSERT_OK(RestartReplica());
  tablet = tablet_replica_->tablet();
  ASSERT_EQ(1, tablet->num_rowsets());

  // Wait for our deleted rowset to become ancient. Since we just started up,
  // we shouldn't have read any delta stats, so running the GC won't pick up
  // our deleted DRS.
  SleepFor(MonoDelta::FromSeconds(FLAGS_tablet_history_max_age_sec));
  ASSERT_OK(tablet->DeleteAncientDeletedRowsets());
  ASSERT_EQ(1, tablet->num_rowsets());
}

// This is a trivial test scenario to check how row locking works in case of
// concurrent attempts to lock the same row with relatively long waiting times.
// The thread attempting to acquire the row lock for long times should be able
// to acquire the lock eventually and log about its attempts to acquire the log.
// The logging part isn't covered by any special assertions, though.
// An alternative place to add this scenario could be lock_manager-test.cc, but
// for proper logging a real WriteOpState backed by a tablet is necessary.
TEST_F(TabletReplicaTest, RowLocksLongWaitAndLogging) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr const char* const kKey = "key";
  constexpr int32_t kValue = 0;

  const Schema schema(GetTestSchema());

  Slice key[]{kKey};
  unique_ptr<WriteRequestPB> req(new WriteRequestPB);
  req->set_tablet_id(tablet()->tablet_id());
  CHECK_OK(SchemaToPB(schema, req->mutable_schema()));
  KuduPartialRow row(&schema);
  CHECK_OK(row.SetInt32(kKey, kValue));
  {
    RowOperationsPBEncoder enc(req->mutable_row_operations());
    enc.Add(RowOperationsPB::DELETE, row);
  }
  unique_ptr<WriteResponsePB> resp(new WriteResponsePB);
  LockManager lock_manager;

  thread t0([&]{
    unique_ptr<WriteOpState> op_state(new WriteOpState(
        tablet_replica_.get(), req.get(), nullptr, resp.get()));
    ScopedRowLock row_lock(
        &lock_manager, op_state.get(), key, LockManager::LOCK_EXCLUSIVE);
    CHECK(row_lock.acquired());
    // Pause for a while when the other thread tries to acquire the lock,
    // so the other thread logs about its attempts to acquire the row lock.
    SleepFor(MonoDelta::FromMilliseconds(3000));
  });

  thread t1([&]{
    // Let the other thread acquire the lock first.
    SleepFor(MonoDelta::FromMilliseconds(500));
    unique_ptr<WriteOpState> op_state(new WriteOpState(
        tablet_replica_.get(), req.get(), nullptr, resp.get()));
    ScopedRowLock row_lock(
        &lock_manager, op_state.get(), key, LockManager::LOCK_EXCLUSIVE);
    CHECK(row_lock.acquired());
  });

  t0.join();
  t1.join();
}

// Test the replication duration metric works.
TEST_F(TabletReplicaTest, TestReplicationDurationMetric) {
  ConsensusBootstrapInfo info;
  ASSERT_OK(StartReplicaAndWaitUntilLeader(info));

  // The metric should be zero at the beginning.
  ASSERT_EQ(0, tablet_replica_->tablet()->metrics()->replication_duration->TotalCount());

  auto req = std::make_unique<WriteRequestPB>();
  ASSERT_OK(GenerateSequentialInsertRequest(GetTestSchema(), req.get()));
  ASSERT_OK(ExecuteWrite(tablet_replica_.get(), *req));

  // The metric should be non-zero after the write completes.
  ASSERT_EVENTUALLY([&]{
    ASSERT_EQ(1, tablet_replica_->tablet()->metrics()->replication_duration->TotalCount());
  });
}

} // namespace tablet
} // namespace kudu
