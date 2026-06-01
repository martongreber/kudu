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
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/fs/block_id.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {

class BlockIdPB;
class FsManager;
class Timestamp;

namespace log {
class MinLogIndexAnchorer;
} // namespace log

namespace tablet {

class RowSetMetadata;
class TxnMetadata;

enum TxnState : int8_t;

typedef std::vector<std::shared_ptr<RowSetMetadata>> RowSetMetadataVector;
typedef std::unordered_set<int64_t> RowSetMetadataIds;

// A transaction ID whose MRS is being flushed.
// TODO(awong): If we begin supporting flushing before committing, we should
// extend this to include the MRS ID so we can define some
// 'last_durable_mrs_id' per transaction.
typedef int64_t TxnInfoBeingFlushed;

constexpr int64_t kNoDurableMemStore = -1;

// Manages the "blocks tracking" for the specified tablet.
//
// TabletMetadata is owned by the Tablet. As new blocks are written to store
// the tablet's data, the Tablet calls Flush() to persist the block list
// on disk.
//
// At startup, the TSTabletManager will load a TabletMetadata for each
// super block found in the tablets/ directory, and then instantiate
// tablets from this data.
class TabletMetadata : public RefCountedThreadSafe<TabletMetadata> {
 public:
  // Create metadata for a new tablet. This assumes that the given superblock
  // has not been written before, and writes out the initial superblock with
  // the provided parameters.
  static Status CreateNew(FsManager* fs_manager,
                          const std::string& tablet_id,
                          const std::string& table_name,
                          const std::string& table_id,
                          const Schema& schema,
                          const PartitionSchema& partition_schema,
                          const Partition& partition,
                          const TabletDataState& initial_tablet_data_state,
                          std::optional<consensus::OpId> tombstone_last_logged_opid,
                          bool supports_live_row_count,
                          std::optional<TableExtraConfigPB> extra_config,
                          std::optional<std::string> dimension_label,
                          std::optional<TableTypePB> table_type,
                          scoped_refptr<TabletMetadata>* metadata);

  // Load existing metadata from disk.
  static Status Load(FsManager* fs_manager,
                     const std::string& tablet_id,
                     scoped_refptr<TabletMetadata>* metadata);

  // Try to load an existing tablet. If it does not exist, create it.
  // If it already existed, verifies that the schema of the tablet matches the
  // provided 'schema'.
  //
  // This is mostly useful for tests which instantiate tablets directly.
  static Status LoadOrCreate(FsManager* fs_manager,
                             const std::string& tablet_id,
                             const std::string& table_name,
                             const std::string& table_id,
                             const Schema& schema,
                             const PartitionSchema& partition_schema,
                             const Partition& partition,
                             const TabletDataState& initial_tablet_data_state,
                             std::optional<consensus::OpId> tombstone_last_logged_opid,
                             std::optional<TableExtraConfigPB> extra_config,
                             std::optional<std::string> dimension_label,
                             std::optional<TableTypePB> table_type,
                             scoped_refptr<TabletMetadata>* metadata);

  static std::vector<BlockIdPB> CollectBlockIdPBs(
      const TabletSuperBlockPB& superblock);

  BlockIdContainer CollectBlockIds() const;

  BlockId GetMaxLiveBlockId() const;

  const std::string& tablet_id() const {
    DCHECK_NE(state_, kNotLoadedYet);
    return tablet_id_;
  }

  // Returns the partition of the tablet.
  const Partition& partition() const {
    return partition_;
  }

  const std::string& table_id() const {
    DCHECK_NE(state_, kNotLoadedYet);
    return table_id_;
  }

  const std::optional<TableTypePB>& table_type() const;

  std::string table_name() const;

  uint32_t schema_version() const;

  void SetSchema(const SchemaPtr& schema, uint32_t version);

  // Sets the schema to 'schema' at 'version', established by the ALTER_SCHEMA
  // op at index 'op_index'. The replaced schema is kept in the schema history
  // for the ops that were applied with it (see GetSchemaForOpIndex()), until
  // TrimSchemaHistory() drops it or the history is full. Does NOT flush.
  void SetSchemaFromAlter(const SchemaPtr& schema, uint32_t version, int64_t op_index);

  // Drops the schema history entries that only ops below 'min_op_index' were
  // applied with. Does NOT flush.
  void TrimSchemaHistory(int64_t min_op_index);

  // Sets 'schema' and 'version' to the schema the tablet applied (or will
  // apply) the op at index 'op_index' with, assuming that every ALTER_SCHEMA op
  // below 'op_index' has been applied. Returns false if that schema is not
  // known: it was dropped from the schema history, or was replaced while the
  // history was not kept (before an upgrade).
  bool GetSchemaForOpIndex(int64_t op_index, SchemaPtr* schema, uint32_t* version) const;

  // Returns the current schema if its version is 'version', else the schema
  // with that version in the schema history, or nullptr if there is none.
  SchemaPtr GetSchemaForVersion(uint32_t version) const;

  void SetTableName(const std::string& table_name);

  void SetExtraConfig(TableExtraConfigPB extra_config);

  // Return a scoped_refptr to the current schema.
  // This pointer will be valid until the TabletMetadata is destructed,
  // even if the schema is changed.
  const SchemaPtr schema() const {
    std::lock_guard l(data_lock_);
    return schema_;
  }

  // Sets 'schema' and 'version' to the current schema and its version.
  void GetSchemaAndVersion(SchemaPtr* schema, uint32_t* version) const {
    std::lock_guard l(data_lock_);
    *schema = schema_;
    *version = schema_version_;
  }

  // Returns the partition schema of the tablet's table.
  const PartitionSchema& partition_schema() const {
    return partition_schema_;
  }

  // Set / get the tablet copy / tablet data state.
  // If set to TABLET_DATA_READY, also clears 'tombstone_last_logged_opid_'.
  void set_tablet_data_state(TabletDataState state);
  TabletDataState tablet_data_state() const;

  // Increments flush pin count by one: if flush pin count > 0,
  // metadata will _not_ be flushed to disk during Flush().
  void PinFlush();

  // Decrements flush pin count by one: if flush pin count is zero,
  // metadata will be flushed to disk during the next call to Flush()
  // or -- if Flush() had been called after a call to PinFlush() but
  // before this method was called -- Flush() will be called inside
  // this method.
  Status UnPinFlush();

  // Stats produced by a orphaned block cleanup pass that runs at the end of each Flush() call.
  struct OrphanBlockCleanupStats {
    // Number of orphaned blocks successfully deleted from disk.
    int64_t blocks_cleaned = 0;
    // Number of orphaned blocks that could not be deleted (e.g. I/O error).
    // These are removed from the superblock regardless (see DeleteOrphanedBlocks),
    // so any unreclaimed disk space must be recovered via 'kudu fs check --repair'.
    int64_t blocks_failed = 0;
  };

  // Flushes the tablet superblock to disk and then attempts to delete any
  // orphaned blocks recorded in the superblock. If 'orphan_stats' is non-null
  // it is populated with the outcome of the cleanup attempt.
  Status Flush(OrphanBlockCleanupStats* orphan_stats = nullptr);

  // Updates the metadata in the following ways:
  // 1. Adds rowsets from 'to_add'.
  // 2. Removes rowsets from 'to_remove'.
  // 3. Adds orphaned blocks from 'to_remove'.
  // 4. Updates the last durable MRS ID from 'last_durable_mrs_id',
  //    assuming it's not kNoMrsFlushed.
  // 5. If 'orphan_stats' is non-null it receives the orphaned block cleanup
  //    outcome (see Flush()).
  static const int64_t kNoMrsFlushed = -1;
  Status UpdateAndFlush(const RowSetMetadataIds& to_remove,
                        const RowSetMetadataVector& to_add,
                        int64_t last_durable_mrs_id,
                        const std::vector<TxnInfoBeingFlushed>& txns_being_flushed = {},
                        OrphanBlockCleanupStats* orphan_stats = nullptr);

  // Adds the blocks referenced by 'block_ids' to 'orphaned_blocks_'.
  //
  // This set will be written to the on-disk metadata in any subsequent
  // flushes.
  //
  // Blocks are removed from this set after they are successfully deleted
  // in a call to DeleteOrphanedBlocks().
  void AddOrphanedBlocks(const BlockIdContainer& block_ids);

  // Mark the superblock to be in state 'delete_type', sync it to disk, and
  // then delete all of the rowsets in this tablet.
  // The metadata (superblock) is not deleted. For that, call DeleteSuperBlock().
  //
  // 'delete_type' must be one of TABLET_DATA_DELETED, TABLET_DATA_TOMBSTONED,
  // or TABLET_DATA_COPYING.
  //
  // 'last_logged_opid' should be set to the last opid in the log, if any is known.
  // If 'last_logged_opid' is not set, then the current value of
  // last_logged_opid is not modified. This is important for roll-forward of
  // partially-tombstoned tablets during crash recovery.
  //
  // Returns only once all data has been removed.
  //
  // Note: this will always update the in-memory state, but upon failure,
  // may not update the on-disk state.
  Status DeleteTabletData(TabletDataState delete_type,
                          const std::optional<consensus::OpId>& last_logged_opid);

  // Return true if this metadata references no blocks (either live or orphaned) and is
  // already marked as tombstoned. If this is the case, then calling DeleteTabletData
  // would be a no-op.
  bool IsTombstonedWithNoBlocks() const;

  // Permanently deletes the superblock from the disk.
  // DeleteTabletData() must first be called and the tablet data state must be
  // TABLET_DATA_DELETED.
  // Returns Status::InvalidArgument if the list of orphaned blocks is not empty.
  // Returns Status::IllegalState if the tablet data state is not TABLET_DATA_DELETED.
  Status DeleteSuperBlock();

  // Create a new RowSetMetadata for this tablet.
  // Does not add the new rowset to the list of rowsets. Use one of the Update()
  // calls to do so.
  Status CreateRowSet(std::shared_ptr<RowSetMetadata>* rowset);

  // Records the fact that the given transaction ID has been started, adopting
  // the anchor until the metadata is flushed. The transaction must not already
  // have metadata associated with it.
  void AddTxnMetadata(int64_t txn_id, std::unique_ptr<log::MinLogIndexAnchorer> log_anchor);

  // Records the fact that the given transaction has started committing, with
  // the given timestamp as the end of its open window, adopting the anchor
  // until the metadata is flushed. The transaction must already have metadata
  // associated with it.
  void BeginCommitTransaction(int64_t txn_id, Timestamp mvcc_op_timestamp,
                              std::unique_ptr<log::MinLogIndexAnchorer> log_anchor);

  // Records the fact that the transaction was committed at the given
  // timestamp, adopting the anchor until the metadata is flushed. The
  // transaction must already have metadata associated with it.
  //
  // TODO(awong): implement a way to remove timstamps once all relevant
  // transactional mutations have been merged with the rest of the tablet.
  void AddCommitTimestamp(int64_t txn_id, Timestamp commit_timestamp,
                          std::unique_ptr<log::MinLogIndexAnchorer> log_anchor);

  // Adds an abort bit to the transaction metadata, adopting the anchor until
  // the metadata is flushed. The transaction must already have metadata
  // associated with it.
  void AbortTransaction(int64_t txn_id, std::unique_ptr<log::MinLogIndexAnchorer> log_anchor);

  // Returns whether a given transaction has metadata, and if requested, what
  // state the transaction is in. If set, 'timestamp' is set to the begin
  // commit timestamp or finalized commit timestamp, depending on whether
  // 'state' is kCommitInProgress or kCommitted respectively.
  bool HasTxnMetadata(int64_t txn_id, TxnState* state = nullptr,
                      Timestamp* timestamp = nullptr);

  // Populates 'pb' with metadata for the given transaction, returning false if
  // the transaction doesn't exist and true otherwise.
  bool GetTxnMetadataPB(int64_t txn_id, TxnMetadataPB* pb) const;

  // Returns the metadata of the given transaction, or nullptr if there is none.
  // Unlike GetTxnMetadata(), does not copy the map of all the transactions.
  scoped_refptr<TxnMetadata> FindTxnMetadata(int64_t txn_id) const;

  // Returns the transaction IDs that were persisted as being in-flight,
  // terminal (committed or aborted), and having un-flushed MRSs.
  void GetTxnIds(std::unordered_set<int64_t>* in_flight_txn_ids,
                 std::unordered_set<int64_t>* terminal_txn_ids = nullptr,
                 std::unordered_set<int64_t>* txn_ids_with_mrs = nullptr);

  const RowSetMetadataVector& rowsets() const { return rowsets_; }

  const FsManager* fs_manager() const { return fs_manager_; }
  FsManager* fs_manager() { return fs_manager_; }

  int64_t last_durable_mrs_id() const { return last_durable_mrs_id_; }

  void SetLastDurableMrsIdForTests(int64_t mrs_id) { last_durable_mrs_id_ = mrs_id; }

  void SetPreFlushCallback(StatusClosure callback);

  // Return the last-logged opid of a tombstoned tablet, if known.
  std::optional<consensus::OpId> tombstone_last_logged_opid() const;

  // Returns the table's extra configuration properties.
  std::optional<TableExtraConfigPB> extra_config() const;

  // Returns the table's dimension label.
  std::optional<std::string> dimension_label() const;

  // The lowest Raft op index that must remain in the WAL for an active CDC stream
  // on this tablet, or -1 if there is no active barrier. Persisted in the superblock
  // and honored by TabletReplica::GetRetentionIndexes() so WAL retention survives a
  // restart or leader change. Thread-safe.
  int64_t cdc_min_retained_op_index() const;

  // The oldest MVCC timestamp (physical microseconds) an active FULL-mode/snapshot
  // CDC stream still needs retained in UNDO history, or 0 if there is no floor.
  // Restored into Tablet::cdc_history_floor_ at bootstrap. Thread-safe.
  uint64_t cdc_history_safe_time_micros() const;

  // The master sequence of the last CDC retention barrier update applied on
  // this replica (see CDCServiceImpl::SetRetentionBarrier()), or 0 if none.
  // Persisted. Thread-safe.
  int64_t cdc_barrier_seq() const;

  // Monotonic time at which cdc_barrier_seq() was last set by
  // ApplyCDCRetentionBarrierUpdate(), or at which this metadata was loaded.
  // Not persisted. Thread-safe.
  MonoTime cdc_barrier_seq_time() const;

  // Wall-clock time (microseconds) of the last master update that set or
  // refreshed the CDC retention barrier, or 0 if no barrier is set or the time
  // is not known. After a restart this is the persisted value, which may lag
  // the last refresh by up to the flush interval given to
  // ApplyCDCRetentionBarrierUpdate(). Thread-safe.
  int64_t cdc_barrier_refresh_micros() const;

  // Monotonic time of the last master update that set or refreshed the CDC
  // retention barrier, or at which this metadata was loaded. Drives the
  // barrier-age valve (--cdc_max_wal_retention_secs). Not persisted.
  // Thread-safe.
  MonoTime cdc_barrier_refresh_monotime() const;

  void set_cdc_barrier_refresh_monotime_for_tests(MonoTime t) {
    std::lock_guard l(data_lock_);
    cdc_barrier_refresh_monotime_ = t;
  }

  // Applies a master barrier update: sets the persisted CDC retention barrier
  // (a negative 'op_index' or a 0 'history_safe_time_micros' clears the
  // respective component), records 'barrier_seq' if it is positive, and, if
  // the barrier is set, records 'now_micros' (and the current monotonic
  // time) as its refresh time. Does NOT flush. Returns true iff the caller
  // should flush: a barrier value changed, a release changed the sequence, the
  // sequence went backwards, or the refresh time moved by at least
  // 'refresh_flush_interval_micros' since the last update that returned true.
  // Otherwise the sequence is persisted by the next flush. Thread-safe.
  bool ApplyCDCRetentionBarrierUpdate(int64_t op_index,
                                      uint64_t history_safe_time_micros,
                                      int64_t barrier_seq,
                                      int64_t now_micros,
                                      int64_t refresh_flush_interval_micros);

  // Clears the persisted CDC history safe-time, keeping the op-index barrier.
  // Does NOT flush; the next superblock flush persists it. Returns true iff the
  // value changed. Thread-safe.
  bool ClearCDCHistorySafeTime();

  // Whether the tablet's range partition was dropped and the master keeps the
  // tablet only for CDC streams to read the rest of its changes. Thread-safe,
  // and takes no lock.
  bool cdc_tablet_dropped() const;

  // Marks the tablet as dropped (see cdc_tablet_dropped()). Returns true if it
  // was not marked before, in which case the caller must flush the metadata to
  // persist it.
  bool SetCDCTabletDropped();

  // The CDC history-GC water mark (a raw Timestamp value, see
  // Tablet::cdc_history_gc_water_mark()), or 0 if no history GC is known.
  // Thread-safe.
  uint64_t cdc_history_gc_water_mark() const;

  // Whether the CDC history-GC water mark is known. It is known for a tablet
  // created by this binary, and for one loaded from a superblock that records
  // it. It is not known for a superblock written by a binary that did not track
  // it (an upgrade, a downgrade and re-upgrade, or a tablet copy from such a
  // binary): that binary may have GC'd UNDO history at any point up to its last
  // run. An unknown mark is not persisted, so it stays unknown until
  // SeedCDCHistoryGcWaterMark(). Thread-safe.
  bool has_cdc_history_gc_water_mark() const;

  // If the CDC history-GC water mark is not known, raises it to 'water_mark'
  // and marks it known, and returns true. Otherwise does nothing and returns
  // false. Does NOT flush. Thread-safe.
  bool SeedCDCHistoryGcWaterMark(uint64_t water_mark);

  // Raises the CDC history-GC water mark to 'water_mark' if it is higher. Does
  // NOT flush: the superblock flush that publishes the rowsets of the GC that
  // raised the mark also persists it. Does not make an unknown mark known.
  // Thread-safe.
  void AdvanceCDCHistoryGcWaterMark(uint64_t water_mark);

  // Loads the currently-flushed superblock from disk into the given protobuf.
  Status ReadSuperBlockFromDisk(TabletSuperBlockPB* superblock) const;

  // Sets *super_block to the serialized form of the current metadata.
  Status ToSuperBlock(TabletSuperBlockPB* super_block) const;

  // Replace the superblock (used for bootstrap) both in memory and on disk.
  Status ReplaceSuperBlock(const TabletSuperBlockPB &pb);

  // Update the in-memory state of metadata to that of the given superblock PB.
  Status LoadFromSuperBlock(const TabletSuperBlockPB& superblock);

  int64_t on_disk_size() const {
    return on_disk_size_.load(std::memory_order_relaxed);
  }

  bool supports_live_row_count() const {
    return supports_live_row_count_;
  }

  // Return standard "T xxx P yyy" log prefix.
  const std::string& LogPrefix() const {
    return log_prefix_;
  }

  // ==========================================================================
  // Stuff used by the tests
  // ==========================================================================
  const RowSetMetadata* GetRowSetForTests(int64_t id) const;
  RowSetMetadata* GetRowSetForTests(int64_t id);

  std::unordered_map<int64_t, scoped_refptr<TxnMetadata>> GetTxnMetadata() const;

  int flush_count_for_tests() const {
    return flush_count_for_tests_;
  }

  // For testing only.
  void set_supports_live_row_count_for_tests(bool supports_live_row_count) {
    supports_live_row_count_ = supports_live_row_count;
  }

 private:
  friend class MetadataTest;
  friend class RefCountedThreadSafe<TabletMetadata>;

  // Compile time assert that no one deletes TabletMetadata objects.
  ~TabletMetadata();

  // Constructor for creating a new tablet.
  //
  // TODO(todd): get rid of this many-arg constructor in favor of just passing
  // in a SuperBlock, which already contains all of these fields.
  TabletMetadata(FsManager* fs_manager, std::string tablet_id,
                 std::string table_name, std::string table_id,
                 const Schema& schema, PartitionSchema partition_schema,
                 Partition partition,
                 const TabletDataState& tablet_data_state,
                 std::optional<consensus::OpId> tombstone_last_logged_opid,
                 bool supports_live_row_count,
                 std::optional<TableExtraConfigPB> extra_config,
                 std::optional<std::string> dimension_label,
                 std::optional<TableTypePB> table_type);

  // Constructor for loading an existing tablet.
  TabletMetadata(FsManager* fs_manager, std::string tablet_id);

  void SwapSchemaUnlocked(SchemaPtr schema, uint32_t version, SchemaPtr* old_schema);

  Status LoadFromDisk();

  // Updates the cached on-disk size of the tablet superblock.
  Status UpdateOnDiskSize();

  // Fully replace superblock.
  // Requires 'flush_lock_'.
  Status ReplaceSuperBlockUnlocked(const TabletSuperBlockPB& pb);

  // Requires 'data_lock_'.
  Status UpdateUnlocked(const RowSetMetadataIds& to_remove,
                        const RowSetMetadataVector& to_add,
                        int64_t last_durable_mrs_id,
                        const std::vector<TxnInfoBeingFlushed>& txns_being_flushed);

  // Requires 'data_lock_'.
  Status ToSuperBlockUnlocked(TabletSuperBlockPB* super_block,
                              const RowSetMetadataVector& rowsets) const;

  // Requires 'data_lock_'.
  void AddOrphanedBlocksUnlocked(const BlockIdContainer& block_ids);

  // Deletes the provided 'blocks' on disk.
  //
  // All blocks that are successfully deleted are removed from the
  // 'orphaned_blocks_' set.
  //
  // Failures are logged, but are not fatal.
  OrphanBlockCleanupStats DeleteOrphanedBlocks(const BlockIdContainer& blocks);

  enum State {
    kNotLoadedYet,
    kNotWrittenYet,
    kInitialized
  };
  State state_;

  // Lock protecting the underlying data.
  mutable simple_spinlock data_lock_;

  // Lock protecting flushing the data to disk.
  // If taken together with 'data_lock_', must be acquired first.
  Mutex flush_lock_;

  const std::string tablet_id_;
  std::string table_id_;

  Partition partition_;

  FsManager* const fs_manager_;
  const std::string log_prefix_;
  RowSetMetadataVector rowsets_;

  base::subtle::Atomic64 next_rowset_idx_;

  int64_t last_durable_mrs_id_;

  // CDC retention barrier, persisted in the superblock. Protected by 'data_lock_'.
  // 'cdc_min_retained_op_index_' is -1 when no barrier is set; the history safe-time
  // is 0 when no FULL-mode history floor is set.
  int64_t cdc_min_retained_op_index_;
  uint64_t cdc_history_safe_time_micros_;

  // See cdc_tablet_dropped(). Persisted in the superblock. Atomic, so that
  // every write can check it without taking 'data_lock_'.
  std::atomic<bool> cdc_tablet_dropped_;

  // CDC history-GC water mark, persisted in the superblock while known (see
  // has_cdc_history_gc_water_mark()). Monotonic. Protected by 'data_lock_'.
  uint64_t cdc_history_gc_water_mark_;
  bool cdc_history_gc_water_mark_known_;

  // Sequence of the last applied CDC barrier update, and the last (and last
  // flush-requesting) refresh time of the barrier, in wall-clock microseconds.
  // The first two are persisted in the superblock. Protected by 'data_lock_'.
  int64_t cdc_barrier_seq_;
  int64_t cdc_barrier_refresh_micros_;
  int64_t cdc_barrier_refresh_flushed_micros_;
  // See cdc_barrier_seq_time() and cdc_barrier_refresh_monotime(). Protected
  // by 'data_lock_'.
  MonoTime cdc_barrier_seq_time_;
  MonoTime cdc_barrier_refresh_monotime_;

  // Log anchors that are waiting on a metadata flush to be unanchored.
  std::vector<std::unique_ptr<log::MinLogIndexAnchorer>> anchors_needing_flush_;

  // Commit timestamps for transactions whose mutations have not yet been
  // merged with main state.
  std::unordered_map<int64_t, scoped_refptr<TxnMetadata>> txn_metadata_by_txn_id_;;

  // The current schema version.
  SchemaPtr schema_;
  uint32_t schema_version_;
  // The index of the ALTER_SCHEMA op that established 'schema_', or 0.
  int64_t schema_op_index_;

  // A schema the tablet had before 'schema_', see SchemaHistoryEntryPB.
  struct SchemaHistoryEntry {
    uint32_t version;
    SchemaPtr schema;
    int64_t start_op_index;
    int64_t end_op_index;
  };
  // In ascending version order. Protected by 'data_lock_'.
  std::vector<SchemaHistoryEntry> schema_history_;
  std::string table_name_;
  PartitionSchema partition_schema_;

  // Protected by 'data_lock_'.
  BlockIdSet orphaned_blocks_;

  // The current state of tablet copy for the tablet.
  TabletDataState tablet_data_state_;

  // Record of the last opid logged by the tablet before it was last
  // tombstoned. Has no meaning for non-tombstoned tablets.
  // Protected by 'data_lock_'.
  std::optional<consensus::OpId> tombstone_last_logged_opid_;

  // Table extra config.
  std::optional<TableExtraConfigPB> extra_config_;

  // Tablet's dimension label.
  std::optional<std::string> dimension_label_;

  // The table type of the table this tablet belongs to.
  std::optional<TableTypePB> table_type_;

  // If this counter is > 0 then Flush() will not write any data to
  // disk.
  int32_t num_flush_pins_;

  // Set if Flush() is called when num_flush_pins_ is > 0; if true,
  // then next UnPinFlush will call Flush() again to ensure the
  // metadata is persisted.
  bool needs_flush_;

  // The number of times metadata has been flushed to disk
  int flush_count_for_tests_;

  // A callback that, if set, is called before this metadata is flushed
  // to disk. Protected by the 'flush_lock_'.
  StatusClosure pre_flush_callback_;

  // The on-disk size of the tablet metadata, as of the last successful
  // call to Flush() or LoadFromDisk().
  std::atomic<int64_t> on_disk_size_;

  // The tablet supports live row counting if true.
  bool supports_live_row_count_;

  DISALLOW_COPY_AND_ASSIGN(TabletMetadata);
};

} // namespace tablet
} // namespace kudu
