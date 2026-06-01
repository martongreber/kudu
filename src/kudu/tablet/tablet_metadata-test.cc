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

#include "kudu/tablet/tablet_metadata.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(enable_tablet_orphaned_block_deletion);
DECLARE_double(env_inject_eio);
DECLARE_string(env_inject_eio_globs);

DEFINE_int64(test_row_set_count, 1000, "");
DEFINE_int64(test_block_count_per_rs, 1000, "");

using kudu::fs::BlockManager;
using kudu::fs::CreateBlockOptions;
using kudu::fs::ReadableBlock;
using kudu::fs::WritableBlock;
using kudu::log::LogAnchorRegistry;
using kudu::log::MinLogIndexAnchorer;
using std::map;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

namespace kudu {
namespace tablet {

class TestTabletMetadata : public KuduTabletTest {
 public:
  TestTabletMetadata()
      : KuduTabletTest(GetSimpleTestSchema()) {
  }

  void SetUp() override {
    KuduTabletTest::SetUp();
    writer_.reset(new LocalTabletWriter(harness_->tablet().get(),
                                        &client_schema_));
  }

  void BuildPartialRow(int key, int intval, const char* strval,
                       unique_ptr<KuduPartialRow>* row);

  // Creates a block on disk using the block manager, writes some data to it, and returns
  // its BlockId. The block is committed to the block manager's in-memory index and
  // can be added to the tablet metadata's orphaned-blocks set for deletion testing.
  BlockId CreateRealBlock();

  // Sets the CDC retention barrier of 'meta' like an unsequenced master update
  // without a refresh time. Returns true iff the barrier changed.
  static bool SetCDCBarrier(TabletMetadata* meta, int64_t op_index,
                            uint64_t history_safe_time_micros) {
    return meta->ApplyCDCRetentionBarrierUpdate(op_index, history_safe_time_micros,
                                                /*barrier_seq=*/0, /*now_micros=*/0,
                                                /*refresh_flush_interval_micros=*/1);
  }

 protected:
  unique_ptr<LocalTabletWriter> writer_;
};

void TestTabletMetadata::BuildPartialRow(int key, int intval, const char* strval,
                                         unique_ptr<KuduPartialRow>* row) {
  row->reset(new KuduPartialRow(&client_schema_));
  CHECK_OK((*row)->SetInt32(0, key));
  CHECK_OK((*row)->SetInt32(1, intval));
  CHECK_OK((*row)->SetStringCopy(2, strval));
}

BlockId TestTabletMetadata::CreateRealBlock() {
  scoped_refptr<BlockManager> bm = harness_->fs_manager()->block_manager();
  const string& tablet_id = harness_->tablet()->metadata()->tablet_id();
  unique_ptr<WritableBlock> block;
  CHECK_OK(bm->CreateBlock(CreateBlockOptions{tablet_id}, &block));
  BlockId block_id = block->id();
  CHECK_OK(block->Append(Slice("orphaned block test data")));
  CHECK_OK(block->Close());
  return block_id;
}

// Test that loading & storing the superblock results in an equivalent file.
TEST_F(TestTabletMetadata, TestLoadFromSuperBlock) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  // Write some data to the tablet and flush.
  unique_ptr<KuduPartialRow> row;
  BuildPartialRow(0, 0, "foo", &row);
  ASSERT_OK(writer_->Insert(*row));
  ASSERT_OK(harness_->tablet()->Flush());

  // Create one more rowset. Write and flush.
  BuildPartialRow(1, 1, "bar", &row);
  ASSERT_OK(writer_->Insert(*row));
  ASSERT_OK(harness_->tablet()->Flush());

  // Dump the superblock to a PB. Save the PB to the side.
  TabletSuperBlockPB superblock_pb_0;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb_0));

  // Alter table's extra configuration properties.
  TableExtraConfigPB extra_config;
  extra_config.set_history_max_age_sec(7200);
  NO_FATALS(AlterSchema(*harness_->tablet()->schema(), std::make_optional(extra_config)));

  // Shut down the tablet.
  harness_->tablet()->Shutdown();

  // Dump the superblock to a PB. Save the PB to the side.
  TabletSuperBlockPB superblock_pb_1;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb_1));

  // Check for AlterSchema.
  ASSERT_FALSE(superblock_pb_0.has_extra_config());
  ASSERT_TRUE(superblock_pb_1.has_extra_config());
  ASSERT_TRUE(superblock_pb_1.extra_config().has_history_max_age_sec());
  ASSERT_EQ(7200, superblock_pb_1.extra_config().history_max_age_sec());

  // Test TabletMetadata::ReplaceSuperBlock
  {
    // Load the superblock PB back into the TabletMetadata.
    ASSERT_OK(meta->ReplaceSuperBlock(superblock_pb_1));

    // Dump the tablet metadata to a superblock PB again, and save it.
    TabletSuperBlockPB superblock_pb_2;
    ASSERT_OK(meta->ToSuperBlock(&superblock_pb_2));

    // Compare the 2 dumped superblock PBs.
    ASSERT_EQ(superblock_pb_1.SerializeAsString(),
              superblock_pb_2.SerializeAsString())
                  << pb_util::SecureDebugString(superblock_pb_1)
                  << pb_util::SecureDebugString(superblock_pb_2);
  }

  // Test TabletMetadata::Load
  {
    // Load the TabletMetadata from disk.
    scoped_refptr<TabletMetadata> new_meta;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   &new_meta));

    // Dump the tablet metadata to a superblock PB again, and save it.
    TabletSuperBlockPB superblock_pb_2;
    ASSERT_OK(new_meta->ToSuperBlock(&superblock_pb_2));

    // Compare the 2 dumped superblock PBs.
    ASSERT_EQ(superblock_pb_1.SerializeAsString(),
              superblock_pb_2.SerializeAsString())
                  << pb_util::SecureDebugString(superblock_pb_1)
                  << pb_util::SecureDebugString(superblock_pb_2);
  }

  LOG(INFO) << "Superblocks match:\n"
            << pb_util::SecureDebugString(superblock_pb_1);
}

TEST_F(TestTabletMetadata, TestOnDiskSize) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  // The tablet metadata was flushed on creation.
  int64_t initial_size = meta->on_disk_size();
  ASSERT_GT(initial_size, 0);

  // Write some data to the tablet and flush.
  unique_ptr<KuduPartialRow> row;
  BuildPartialRow(0, 0, "foo", &row);
  ASSERT_OK(writer_->Insert(*row));
  ASSERT_OK(harness_->tablet()->Flush());

  // The tablet metadata grows after flushing a new rowset.
  int64_t middle_size = meta->on_disk_size();
  ASSERT_GT(middle_size, initial_size);

  // Create another rowset.
  // The on-disk size shouldn't change until after flush.
  BuildPartialRow(1, 1, "bar", &row);
  ASSERT_OK(writer_->Insert(*row));
  ASSERT_EQ(middle_size, meta->on_disk_size());
  ASSERT_OK(harness_->tablet()->Flush());
  int64_t final_size = meta->on_disk_size();
  ASSERT_GT(final_size, middle_size);

  // Shut down the tablet.
  harness_->tablet()->Shutdown();

  // The tablet metadata is a container file holding the superblock PB,
  // so the on_disk_size should be at least as big.
  TabletSuperBlockPB superblock_pb;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb));
  ASSERT_GE(final_size, superblock_pb.ByteSizeLong());
}

// The CDC retention barrier (op-index floor + FULL-mode history safe-time) must
// survive a restart: it is persisted in the superblock and reloaded at bootstrap so
// WAL/history retention does not reset to zero on restart or leader change.
TEST_F(TestTabletMetadata, TestCDCRetentionBarrierRoundTrip) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  // A freshly created tablet has no barrier.
  ASSERT_EQ(-1, meta->cdc_min_retained_op_index());
  ASSERT_EQ(0, meta->cdc_history_safe_time_micros());

  // Setting a barrier reports a change; setting the identical values does not.
  const int64_t kOpIndex = 4242;
  const uint64_t kHistoryMicros = 1234567890ULL;
  ASSERT_TRUE(SetCDCBarrier(meta, kOpIndex, kHistoryMicros));
  ASSERT_FALSE(SetCDCBarrier(meta, kOpIndex, kHistoryMicros));
  ASSERT_EQ(kOpIndex, meta->cdc_min_retained_op_index());
  ASSERT_EQ(kHistoryMicros, meta->cdc_history_safe_time_micros());
  ASSERT_OK(meta->Flush());

  // Reload from disk and confirm both values were restored.
  {
    scoped_refptr<TabletMetadata> reloaded;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   &reloaded));
    ASSERT_EQ(kOpIndex, reloaded->cdc_min_retained_op_index());
    ASSERT_EQ(kHistoryMicros, reloaded->cdc_history_safe_time_micros());
  }

  // Releasing the barrier (negative index, zero history) clears it, and the release
  // is itself durable: a reload reads back the "no barrier" sentinels, not the
  // previously-persisted values.
  ASSERT_TRUE(SetCDCBarrier(meta, -1, 0));
  ASSERT_EQ(-1, meta->cdc_min_retained_op_index());
  ASSERT_EQ(0, meta->cdc_history_safe_time_micros());
  ASSERT_OK(meta->Flush());
  {
    scoped_refptr<TabletMetadata> reloaded;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   &reloaded));
    ASSERT_EQ(-1, reloaded->cdc_min_retained_op_index());
    ASSERT_EQ(0, reloaded->cdc_history_safe_time_micros());
  }

  // Any negative index normalizes to the -1 sentinel: re-arm a real barrier, then
  // release with an arbitrary negative index and confirm it lands on -1.
  ASSERT_TRUE(SetCDCBarrier(meta, kOpIndex, 0));
  ASSERT_TRUE(SetCDCBarrier(meta, -99, 0));
  ASSERT_EQ(-1, meta->cdc_min_retained_op_index());
}

// The CDC history-GC water mark is persisted in the superblock, so a restart or
// a tablet copy (which ships the superblock) keeps it. It only moves forward,
// and clearing the history safe-time (the CDC valve) does not touch it.
TEST_F(TestTabletMetadata, TestCDCHistoryGcWaterMarkRoundTrip) {
  TabletMetadata* meta = harness_->tablet()->metadata();
  ASSERT_EQ(0, meta->cdc_history_gc_water_mark());

  const uint64_t kWaterMark = 0x123456789ULL;
  meta->AdvanceCDCHistoryGcWaterMark(kWaterMark);
  meta->AdvanceCDCHistoryGcWaterMark(kWaterMark - 1);
  ASSERT_EQ(kWaterMark, meta->cdc_history_gc_water_mark());
  ASSERT_TRUE(SetCDCBarrier(meta, 7, 1234));
  ASSERT_OK(meta->Flush());
  {
    scoped_refptr<TabletMetadata> reloaded;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   &reloaded));
    ASSERT_EQ(kWaterMark, reloaded->cdc_history_gc_water_mark());
  }

  // Clearing the history safe-time keeps the op-index barrier and the water
  // mark; the clear is durable once flushed.
  ASSERT_TRUE(meta->ClearCDCHistorySafeTime());
  ASSERT_FALSE(meta->ClearCDCHistorySafeTime());
  ASSERT_OK(meta->Flush());
  {
    scoped_refptr<TabletMetadata> reloaded;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   &reloaded));
    ASSERT_EQ(7, reloaded->cdc_min_retained_op_index());
    ASSERT_EQ(0, reloaded->cdc_history_safe_time_micros());
    ASSERT_EQ(kWaterMark, reloaded->cdc_history_gc_water_mark());
  }
}

// A binary that tracks the CDC history-GC water mark writes it also when it is
// 0, so that a superblock without it (written by a binary that did not track
// it) reads back as unknown. An unknown mark stays unknown across a flush, so
// the tablet still seeds it when it opens, and is known once seeded.
TEST_F(TestTabletMetadata, TestCDCHistoryGcWaterMarkUnknownUntilSeeded) {
  TabletMetadata* meta = harness_->tablet()->metadata();
  ASSERT_TRUE(meta->has_cdc_history_gc_water_mark());
  ASSERT_EQ(0, meta->cdc_history_gc_water_mark());
  ASSERT_OK(meta->Flush());
  TabletSuperBlockPB sb;
  ASSERT_OK(meta->ReadSuperBlockFromDisk(&sb));
  ASSERT_TRUE(sb.has_cdc_history_gc_water_mark());
  ASSERT_EQ(0, sb.cdc_history_gc_water_mark());

  // Load a superblock without the mark, as an older binary wrote it.
  harness_->tablet()->Shutdown();
  sb.clear_cdc_history_gc_water_mark();
  ASSERT_OK(meta->ReplaceSuperBlock(sb));
  ASSERT_FALSE(meta->has_cdc_history_gc_water_mark());
  ASSERT_EQ(0, meta->cdc_history_gc_water_mark());

  // A GC does not make the mark known, and an unknown mark is not written.
  const uint64_t kGcMark = 0x1000;
  meta->AdvanceCDCHistoryGcWaterMark(kGcMark);
  ASSERT_FALSE(meta->has_cdc_history_gc_water_mark());
  ASSERT_OK(meta->Flush());
  ASSERT_OK(meta->ReadSuperBlockFromDisk(&sb));
  ASSERT_FALSE(sb.has_cdc_history_gc_water_mark());
  {
    scoped_refptr<TabletMetadata> reloaded;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   &reloaded));
    ASSERT_FALSE(reloaded->has_cdc_history_gc_water_mark());
  }

  // Only the first seed applies, and it never lowers the mark.
  const uint64_t kSeed = 0x2000;
  ASSERT_TRUE(meta->SeedCDCHistoryGcWaterMark(kSeed));
  ASSERT_FALSE(meta->SeedCDCHistoryGcWaterMark(kSeed + 1));
  ASSERT_TRUE(meta->has_cdc_history_gc_water_mark());
  ASSERT_EQ(kSeed, meta->cdc_history_gc_water_mark());
  ASSERT_OK(meta->Flush());
  {
    scoped_refptr<TabletMetadata> reloaded;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   &reloaded));
    ASSERT_TRUE(reloaded->has_cdc_history_gc_water_mark());
    ASSERT_EQ(kSeed, reloaded->cdc_history_gc_water_mark());
    ASSERT_FALSE(reloaded->SeedCDCHistoryGcWaterMark(kSeed + 1));
  }
}

// The mark of a dropped tablet kept for CDC survives a restart, so that the
// replica keeps rejecting writes.
TEST_F(TestTabletMetadata, TestCDCTabletDroppedRoundTrip) {
  TabletMetadata* meta = harness_->tablet()->metadata();
  ASSERT_FALSE(meta->cdc_tablet_dropped());
  ASSERT_TRUE(meta->SetCDCTabletDropped());
  ASSERT_FALSE(meta->SetCDCTabletDropped());
  ASSERT_TRUE(meta->cdc_tablet_dropped());
  ASSERT_OK(meta->Flush());
  scoped_refptr<TabletMetadata> reloaded;
  ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                 harness_->tablet()->tablet_id(),
                                 &reloaded));
  ASSERT_TRUE(reloaded->cdc_tablet_dropped());
}

// The schemas that an ALTER replaced are kept for the ops applied with them,
// across a restart, until the ops are no longer in the WAL.
TEST_F(TestTabletMetadata, TestSchemaHistory) {
  TabletMetadata* meta = harness_->tablet()->metadata();
  const SchemaPtr v0 = meta->schema();
  ASSERT_EQ(0, meta->schema_version());
  SchemaBuilder builder(*v0);
  ASSERT_OK(builder.AddNullableColumn("c1", INT32));
  const SchemaPtr v1 = std::make_shared<Schema>(builder.Build());
  ASSERT_OK(builder.AddNullableColumn("c2", INT32));
  const SchemaPtr v3 = std::make_shared<Schema>(builder.Build());
  // ALTERs at op indexes 10 (to version 1) and 20 (to version 3).
  meta->SetSchemaFromAlter(v1, 1, 10);
  meta->SetSchemaFromAlter(v3, 3, 20);

  const auto check = [](const TabletMetadata& m, int64_t op_index, bool known,
                        uint32_t expected_version, const Schema* expected_schema) {
    SCOPED_TRACE(op_index);
    SchemaPtr schema;
    uint32_t version;
    ASSERT_EQ(known, m.GetSchemaForOpIndex(op_index, &schema, &version));
    if (known) {
      EXPECT_EQ(expected_version, version);
      EXPECT_EQ(*expected_schema, *schema);
    }
  };
  const auto verify = [&](const TabletMetadata& m) {
    NO_FATALS(check(m, 1, true, 0, v0.get()));
    NO_FATALS(check(m, 10, true, 0, v0.get()));
    NO_FATALS(check(m, 11, true, 1, v1.get()));
    NO_FATALS(check(m, 20, true, 1, v1.get()));
    NO_FATALS(check(m, 21, true, 3, v3.get()));
    ASSERT_NE(nullptr, m.GetSchemaForVersion(0));
    EXPECT_EQ(*v1, *m.GetSchemaForVersion(1));
    EXPECT_EQ(*v3, *m.GetSchemaForVersion(3));
    EXPECT_EQ(nullptr, m.GetSchemaForVersion(2));
  };
  NO_FATALS(verify(*meta));
  ASSERT_OK(meta->Flush());
  {
    scoped_refptr<TabletMetadata> reloaded;
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(), &reloaded));
    NO_FATALS(verify(*reloaded.get()));
  }

  // Once the WAL starts after op 10, the schema of version 0 is not needed.
  meta->TrimSchemaHistory(11);
  NO_FATALS(check(*meta, 10, false, 0, nullptr));
  NO_FATALS(check(*meta, 11, true, 1, v1.get()));
  EXPECT_EQ(nullptr, meta->GetSchemaForVersion(0));
  EXPECT_NE(nullptr, meta->GetSchemaForVersion(1));
}

// A master barrier update persists its sequence, so the replica's sequence
// gate holds across a restart, also after the barrier was released. The
// refresh time of a set barrier is persisted too, but an unchanged barrier
// asks for a flush only once the refresh time moved by the flush interval.
TEST_F(TestTabletMetadata, TestCDCBarrierSeqAndRefreshRoundTrip) {
  TabletMetadata* meta = harness_->tablet()->metadata();
  ASSERT_EQ(0, meta->cdc_barrier_seq());
  ASSERT_EQ(0, meta->cdc_barrier_refresh_micros());
  const auto reload = [&](scoped_refptr<TabletMetadata>* reloaded) {
    ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                   harness_->tablet()->tablet_id(),
                                   reloaded));
  };

  constexpr int64_t kInterval = 1000;
  constexpr int64_t kT0 = 1000000;
  ASSERT_TRUE(meta->ApplyCDCRetentionBarrierUpdate(7, 0, /*barrier_seq=*/10, kT0, kInterval));
  ASSERT_EQ(10, meta->cdc_barrier_seq());
  ASSERT_EQ(kT0, meta->cdc_barrier_refresh_micros());
  ASSERT_OK(meta->Flush());

  // An unchanged barrier refreshed within the interval needs no flush, but the
  // new sequence and refresh time are kept in memory.
  ASSERT_FALSE(meta->ApplyCDCRetentionBarrierUpdate(7, 0, 11, kT0 + kInterval - 1, kInterval));
  ASSERT_EQ(11, meta->cdc_barrier_seq());
  ASSERT_EQ(kT0 + kInterval - 1, meta->cdc_barrier_refresh_micros());
  {
    scoped_refptr<TabletMetadata> reloaded;
    NO_FATALS(reload(&reloaded));
    ASSERT_EQ(7, reloaded->cdc_min_retained_op_index());
    ASSERT_EQ(10, reloaded->cdc_barrier_seq());
    ASSERT_EQ(kT0, reloaded->cdc_barrier_refresh_micros());
  }
  // Once the refresh time moved by the interval since the last flush, the
  // update asks for one.
  ASSERT_TRUE(meta->ApplyCDCRetentionBarrierUpdate(7, 0, 12, kT0 + kInterval, kInterval));
  ASSERT_OK(meta->Flush());
  {
    scoped_refptr<TabletMetadata> reloaded;
    NO_FATALS(reload(&reloaded));
    ASSERT_EQ(12, reloaded->cdc_barrier_seq());
    ASSERT_EQ(kT0 + kInterval, reloaded->cdc_barrier_refresh_micros());
  }

  // A release keeps the sequence and drops the refresh time.
  ASSERT_TRUE(meta->ApplyCDCRetentionBarrierUpdate(-1, 0, 13, kT0 + 2 * kInterval, kInterval));
  ASSERT_EQ(0, meta->cdc_barrier_refresh_micros());
  ASSERT_OK(meta->Flush());
  {
    scoped_refptr<TabletMetadata> reloaded;
    NO_FATALS(reload(&reloaded));
    ASSERT_EQ(-1, reloaded->cdc_min_retained_op_index());
    ASSERT_EQ(13, reloaded->cdc_barrier_seq());
    ASSERT_EQ(0, reloaded->cdc_barrier_refresh_micros());
  }

  // An unchanged release asks for a flush when it changes the sequence: a
  // released barrier is not refreshed, so nothing else would persist it.
  ASSERT_TRUE(meta->ApplyCDCRetentionBarrierUpdate(-1, 0, 14, kT0 + 2 * kInterval, kInterval));
  ASSERT_OK(meta->Flush());
  ASSERT_FALSE(meta->ApplyCDCRetentionBarrierUpdate(-1, 0, 14, kT0 + 2 * kInterval, kInterval));
  {
    scoped_refptr<TabletMetadata> reloaded;
    NO_FATALS(reload(&reloaded));
    ASSERT_EQ(14, reloaded->cdc_barrier_seq());
  }

  // An unsequenced update (0) leaves the sequence alone.
  ASSERT_TRUE(meta->ApplyCDCRetentionBarrierUpdate(3, 0, 0, kT0, kInterval));
  ASSERT_EQ(14, meta->cdc_barrier_seq());
  ASSERT_OK(meta->Flush());

  // A sequence that went backwards asks for a flush even if the barrier is
  // unchanged, and it is recorded with a new time.
  const MonoTime before_backwards = meta->cdc_barrier_seq_time();
  ASSERT_TRUE(meta->ApplyCDCRetentionBarrierUpdate(3, 0, 5, kT0, kInterval));
  ASSERT_EQ(5, meta->cdc_barrier_seq());
  ASSERT_GE(meta->cdc_barrier_seq_time(), before_backwards);
  ASSERT_OK(meta->Flush());
  {
    scoped_refptr<TabletMetadata> reloaded;
    NO_FATALS(reload(&reloaded));
    ASSERT_EQ(5, reloaded->cdc_barrier_seq());
  }
}

TEST_F(TestTabletMetadata, BenchmarkCollectBlockIds) {
  auto tablet_meta = harness_->tablet()->metadata();
  RowSetMetadataVector rs_metas;
  for (int i = 0; i < FLAGS_test_row_set_count; ++i) {
    unique_ptr<RowSetMetadata> meta;
    ASSERT_OK(RowSetMetadata::CreateNew(tablet_meta, i, &meta));

    map<ColumnId, BlockId> block_by_column;
    for (int j = 0; j < FLAGS_test_block_count_per_rs; ++j) {
      block_by_column[ColumnId(j)] = BlockId(FLAGS_test_block_count_per_rs * i + j);
    }
    meta->SetColumnDataBlocks(block_by_column);
    rs_metas.emplace_back(shared_ptr<RowSetMetadata>(meta.release()));
  }
  ASSERT_OK(tablet_meta->UpdateAndFlush(RowSetMetadataIds(),
                                        rs_metas,
                                        TabletMetadata::kNoMrsFlushed));
  for (int i = 0; i < 10; ++i) {
    BlockIdContainer block_ids;
    LOG_TIMING(INFO, "collecting BlockIds") {
      block_ids = tablet_meta->CollectBlockIds();
    }
    LOG(INFO) << "block_ids size: " << block_ids.size();
  }
}

TEST_F(TestTabletMetadata, GetMaxLiveBlockId) {
  auto* tablet_meta = harness_->tablet()->metadata();
  RowSetMetadataVector rs_metas;
  for (int i = 0; i < FLAGS_test_row_set_count; ++i) {
    unique_ptr<RowSetMetadata> meta;
    ASSERT_OK(RowSetMetadata::CreateNew(tablet_meta, i, &meta));

    map<ColumnId, BlockId> block_by_column;
    for (int j = 0; j < FLAGS_test_block_count_per_rs; ++j) {
      block_by_column[ColumnId(j)] = BlockId(FLAGS_test_block_count_per_rs * i + j);
    }
    meta->SetColumnDataBlocks(block_by_column);
    rs_metas.emplace_back(shared_ptr<RowSetMetadata>(meta.release()));
  }
  ASSERT_OK(tablet_meta->UpdateAndFlush(RowSetMetadataIds(), rs_metas,
                                        TabletMetadata::kNoMrsFlushed));

  BlockIdContainer block_ids = tablet_meta->CollectBlockIds();
  BlockId max_block_id = tablet_meta->GetMaxLiveBlockId();
  ASSERT_FALSE(max_block_id.IsNull());
  const auto expected_max_block_id = std::max_element(block_ids.begin(), block_ids.end());
  ASSERT_EQ(*expected_max_block_id, max_block_id);
}

// FindTxnMetadata() looks up a single transaction: it returns the same
// metadata object as the full map, and nullptr for an unknown transaction.
TEST_F(TestTabletMetadata, TestFindTxnMetadata) {
  scoped_refptr<LogAnchorRegistry> registry(new LogAnchorRegistry);
  TabletMetadata* meta = harness_->tablet()->metadata();
  constexpr int64_t kTxnId = 7;
  ASSERT_EQ(nullptr, meta->FindTxnMetadata(kTxnId));
  meta->AddTxnMetadata(kTxnId, unique_ptr<MinLogIndexAnchorer>(
      new MinLogIndexAnchorer(registry.get(), "txn")));
  const scoped_refptr<TxnMetadata> txn_meta = meta->FindTxnMetadata(kTxnId);
  ASSERT_NE(nullptr, txn_meta);
  ASSERT_EQ(FindOrDie(meta->GetTxnMetadata(), kTxnId).get(), txn_meta.get());
  ASSERT_EQ(nullptr, meta->FindTxnMetadata(kTxnId + 1));
}

TEST_F(TestTabletMetadata, TestTxnMetadata) {
  constexpr const char* kOwner = "txn";
  const Timestamp kDummyTimestamp = Timestamp(1337);
  scoped_refptr<LogAnchorRegistry> registry(new LogAnchorRegistry);
  TabletMetadata* meta = harness_->tablet()->metadata();
  const auto make_anchor = [&] () {
    return unique_ptr<MinLogIndexAnchorer>(new MinLogIndexAnchorer(registry.get(), kOwner));
  };
  int64_t kCommittedTxnId = 1;
  int64_t kAbortedTxnId = 2;
  int64_t kInFlightTxnId = 3;
  meta->AddTxnMetadata(kCommittedTxnId, make_anchor());
  meta->BeginCommitTransaction(kCommittedTxnId, kDummyTimestamp, make_anchor());
  meta->AddCommitTimestamp(kCommittedTxnId, kDummyTimestamp, make_anchor());
  ASSERT_EQ(1, meta->GetTxnMetadata().size());

  meta->AddTxnMetadata(kAbortedTxnId, make_anchor());
  meta->BeginCommitTransaction(kAbortedTxnId, kDummyTimestamp, make_anchor());
  meta->AbortTransaction(kAbortedTxnId, make_anchor());
  ASSERT_EQ(2, meta->GetTxnMetadata().size());

  meta->AddTxnMetadata(kInFlightTxnId, make_anchor());
  meta->BeginCommitTransaction(kInFlightTxnId, kDummyTimestamp, make_anchor());
  ASSERT_EQ(3, meta->GetTxnMetadata().size());

  // Validate the transactions' fields.
  const auto validate_txn_metas = [&] (TabletMetadata* meta) {
    auto txn_metas = meta->GetTxnMetadata();
    ASSERT_EQ(3, txn_metas.size());
    ASSERT_TRUE(meta->HasTxnMetadata(kCommittedTxnId));
    ASSERT_TRUE(meta->HasTxnMetadata(kAbortedTxnId));
    ASSERT_TRUE(meta->HasTxnMetadata(kInFlightTxnId));

    const auto& committed_txn = FindOrDie(txn_metas, kCommittedTxnId);
    ASSERT_FALSE(committed_txn->aborted());
    ASSERT_TRUE(committed_txn->commit_mvcc_op_timestamp().has_value());
    ASSERT_EQ(kDummyTimestamp, *committed_txn->commit_mvcc_op_timestamp());
    ASSERT_TRUE(committed_txn->commit_timestamp().has_value());
    ASSERT_EQ(kDummyTimestamp, *committed_txn->commit_timestamp());

    const auto& aborted_txn = FindOrDie(txn_metas, kAbortedTxnId);
    ASSERT_TRUE(aborted_txn->aborted());
    ASSERT_FALSE(aborted_txn->commit_timestamp().has_value());
    ASSERT_TRUE(aborted_txn->commit_mvcc_op_timestamp().has_value());
    ASSERT_EQ(kDummyTimestamp, *aborted_txn->commit_mvcc_op_timestamp());

    const auto& in_flight_txn = FindOrDie(txn_metas, kInFlightTxnId);
    ASSERT_FALSE(in_flight_txn->aborted());
    ASSERT_FALSE(in_flight_txn->commit_timestamp().has_value());
    ASSERT_TRUE(in_flight_txn->commit_mvcc_op_timestamp().has_value());
    ASSERT_EQ(kDummyTimestamp, *in_flight_txn->commit_mvcc_op_timestamp());

    unordered_set<int64_t> in_flight_txn_ids;
    unordered_set<int64_t> terminal_txn_ids;
    meta->GetTxnIds(&in_flight_txn_ids, &terminal_txn_ids);
    ASSERT_EQ(1, in_flight_txn_ids.size());
    ASSERT_EQ(2, terminal_txn_ids.size());
    ASSERT_TRUE(ContainsKey(in_flight_txn_ids, kInFlightTxnId));
    ASSERT_TRUE(ContainsKey(terminal_txn_ids, kAbortedTxnId));
    ASSERT_TRUE(ContainsKey(terminal_txn_ids, kCommittedTxnId));
  };
  NO_FATALS(validate_txn_metas(meta));

  // Validate the superblock fields are set.
  TabletSuperBlockPB superblock_pb;
  ASSERT_OK(meta->ToSuperBlock(&superblock_pb));
  ASSERT_EQ(3, superblock_pb.txn_metadata_size());

  // Flush and reload the metadata.
  ASSERT_OK(meta->Flush());
  scoped_refptr<TabletMetadata> new_meta;
  ASSERT_OK(TabletMetadata::Load(harness_->fs_manager(),
                                  harness_->tablet()->tablet_id(),
                                  &new_meta));
  NO_FATALS(validate_txn_metas(new_meta.get()));
}

// Verify the happy path i.e., orphaned blocks are successfully deleted during a
// metadata flush, and the tablet metrics are updated to reflect the cleanup.
TEST_F(TestTabletMetadata, TestOrphanBlockCleanupSuccess) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  // Create a real block on disk and register it as orphaned.
  BlockId block_id = CreateRealBlock();
  meta->AddOrphanedBlocks({block_id});

  // Flush the tablet (the MemRowSet is empty, so HandleEmptyCompactionOrFlush
  // is taken, which still runs the full metadata flush and orphan cleanup path).
  StringVectorSink sink;
  {
    ScopedRegisterSink reg(&sink);
    ASSERT_OK(harness_->tablet()->Flush());
  }

  // Metrics must reflect one successful cleanup and zero failures.
  auto* metrics = harness_->tablet()->metrics();
  ASSERT_EQ(1, metrics->orphaned_blocks_cleaned->value());
  ASSERT_EQ(0, metrics->orphaned_block_cleanup_failures->value());

  // No error or warning about orphaned-block cleanup should have been logged.
  for (const string& s : sink.logged_msgs()) {
    ASSERT_STR_NOT_MATCHES(s, ": failed cleaning up.*removed block");
    ASSERT_STR_NOT_MATCHES(s, "Not deleting.*orphaned block");
  }

  // The block must no longer be accessible via the block manager.
  unique_ptr<ReadableBlock> rblock;
  Status s = harness_->fs_manager()->block_manager()->OpenBlock(block_id, &rblock);
  ASSERT_TRUE(s.IsNotFound()) << "Expected block to be deleted: " << s.ToString();
}

// Verify that when deletion of orphaned block is disabled via the runtime flag, a
// warning is logged, the block is not deleted, and no failure metrics are updated.
TEST_F(TestTabletMetadata, TestOrphanBlockDeletionDisabled) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  BlockId block_id = CreateRealBlock();
  meta->AddOrphanedBlocks({block_id});

  StringVectorSink sink;
  {
    // Disable orphaned block deletion for the duration of this scope.
    google::FlagSaver flag_saver;
    FLAGS_enable_tablet_orphaned_block_deletion = false;

    ScopedRegisterSink reg(&sink);
    ASSERT_OK(harness_->tablet()->Flush());
  }

  // A WARNING must have been emitted explaining that deletion was skipped.
  ASSERT_STRINGS_ANY_MATCH(sink.logged_msgs(),
      "Not deleting.*orphaned block.*enable_tablet_orphaned_block_deletion");

  // Metrics must be all-zero: no deletion was attempted.
  auto* metrics = harness_->tablet()->metrics();
  ASSERT_EQ(0, metrics->orphaned_blocks_cleaned->value());
  ASSERT_EQ(0, metrics->orphaned_block_cleanup_failures->value());

  // The block must still be accessible on disk since deletion was suppressed.
  unique_ptr<ReadableBlock> rblock;
  ASSERT_OK(harness_->fs_manager()->block_manager()->OpenBlock(block_id, &rblock));
}

// Verify the failure path: when a block deletion encounters an I/O error
// (simulated by injecting EIO on the data directory), an WARNING is logged
// with the "kudu fs check --repair" hint and the failure metric is bumped.
// Even though the physical deletion failed, the block is removed from
// orphaned_blocks_ so it does not accumulate in the superblock across restarts.
// The leaked disk space can be reclaimed with 'kudu fs check --repair'.
TEST_F(TestTabletMetadata, TestOrphanBlockDeletionFailed) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  BlockId block_id = CreateRealBlock();
  meta->AddOrphanedBlocks({block_id});

  // Identify the data directory so that we can scope EIO injection to it,
  // leaving the tablet-metadata directory (superblock write path) unaffected.
  const vector<string> data_dirs = harness_->fs_manager()->GetDataRootDirs();
  ASSERT_FALSE(data_dirs.empty());

  StringVectorSink sink;
  {
    // Inject EIO on every file operation under the data directory. The
    // superblock write goes to tablet-meta which is a different directory and
    // therefore unaffected. Also, Tablet::Flush() still returns OK overall.
    google::FlagSaver flag_saver;
    FLAGS_env_inject_eio_globs = data_dirs[0] + "/*";
    FLAGS_env_inject_eio = 1.0;

    ScopedRegisterSink reg(&sink);
    // Flush succeeds overall; orphan deletion failure is non-fatal.
    ASSERT_OK(harness_->tablet()->Flush());
  }

  // An warning log must report the failed cleanup and suggest the repair tool.
  ASSERT_STRINGS_ANY_MATCH(sink.logged_msgs(),
      ": failed cleaning up.*removed block");
  ASSERT_STRINGS_ANY_MATCH(sink.logged_msgs(),
      "kudu fs check --repair");

  // The failure counter must be incremented; cleaned must remain zero.
  auto* metrics = harness_->tablet()->metrics();
  ASSERT_EQ(0, metrics->orphaned_blocks_cleaned->value());
  ASSERT_EQ(1, metrics->orphaned_block_cleanup_failures->value());

  // Even though physical deletion failed, the block must have been removed
  // from orphaned_blocks_ so the superblock is written clean. This prevents
  // the entry from accumulating across restarts. The block remains on disk
  // as leaked space, recoverable via 'kudu fs check --repair'.
  TabletSuperBlockPB pb;
  ASSERT_OK(meta->ToSuperBlock(&pb));
  ASSERT_EQ(0, pb.orphaned_blocks_size())
      << "Block must be removed from orphaned set even on deletion failure";
}

// Verify that a block whose physical deletion fails due to an I/O error is
// not retained in orphaned_blocks_ for retry. After the flush the superblock
// is written clean (zero orphaned entries) and a subsequent flush performs no
// additional cleanup work. The leaked block remains on disk and is recoverable
// via 'kudu fs check --repair'.
TEST_F(TestTabletMetadata, TestOrphanBlockNotRetainedAfterFailure) {
  TabletMetadata* meta = harness_->tablet()->metadata();

  BlockId block_id = CreateRealBlock();
  meta->AddOrphanedBlocks({block_id});

  const vector<string> data_dirs = harness_->fs_manager()->GetDataRootDirs();
  ASSERT_FALSE(data_dirs.empty());

  StringVectorSink sink;

  // First flush: deletion fails due to injected EIO.
  {
    ScopedRegisterSink reg(&sink);
    google::FlagSaver flag_saver;
    FLAGS_env_inject_eio_globs = data_dirs[0] + "/*";
    FLAGS_env_inject_eio = 1.0;
    // Flush still returns OK; orphan deletion failure is non-fatal.
    ASSERT_OK(harness_->tablet()->Flush());
  }

  ASSERT_STRINGS_ANY_MATCH(sink.logged_msgs(),
      ": failed cleaning up.*removed block");

  auto* metrics = harness_->tablet()->metrics();
  ASSERT_EQ(0, metrics->orphaned_blocks_cleaned->value());
  ASSERT_EQ(1, metrics->orphaned_block_cleanup_failures->value());

  // The block must have been removed from orphaned_blocks_ even though its
  // physical deletion failed. The superblock must already be clean.
  TabletSuperBlockPB pb;
  ASSERT_OK(meta->ToSuperBlock(&pb));
  ASSERT_EQ(0, pb.orphaned_blocks_size())
      << "Block must be removed from orphaned set even on deletion failure";

  // Second flush: no I/O error, but there is nothing left to retry.
  sink.logged_msgs().clear();
  {
    ScopedRegisterSink reg(&sink);
    ASSERT_OK(harness_->tablet()->Flush());
  }

  // No orphan-related messages should appear — there was nothing to clean up.
  for (const string& s : sink.logged_msgs()) {
    ASSERT_STR_NOT_MATCHES(s, ": failed cleaning up.*removed block");
    ASSERT_STR_NOT_MATCHES(s, "Not deleting.*orphaned block");
  }

  // Counters must be unchanged: no additional cleanup was attempted.
  ASSERT_EQ(0, metrics->orphaned_blocks_cleaned->value());
  ASSERT_EQ(1, metrics->orphaned_block_cleanup_failures->value());
}

} // namespace tablet
} // namespace kudu
