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

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {

class Arena;
class MonoTime;
class RowBlockRow;
class Schema;

namespace consensus {
class ReplicateMsg;
class TimeManager;
} // namespace consensus

namespace tablet {
class Tablet;
class TxResultPB;
} // namespace tablet

namespace tserver {
class AlterSchemaRequestPB;
} // namespace tserver

namespace cdc {

class CDCColumnValuePB;
class CDCRecordPB;

// Decode all rows from a WRITE_OP ReplicateMsg into CDCRecordPB records.
//
// Each DML row in the WriteRequestPB that the tablet applied produces one
// CDCRecordPB with op_index, op_term, timestamp, and schema_version set from
// the ReplicateMsg metadata. 'schema' is the tablet's schema that the op was
// applied with, and 'schema_version' its version. The rows are decoded the way
// the tablet decoded them: the request's (client) schema is mapped onto
// 'schema' by column name, and 'schema' provides the column ids, the defaults
// of the columns the request does not set, and the column attributes. 'result'
// is the op's outcome from its COMMIT entry, with one element per row: a row
// that failed (e.g. a duplicate INSERT, or an UPDATE of a missing key) or that
// changed nothing (e.g. an ignored INSERT_IGNORE) produces no record. For a
// table with an auto-incrementing column the rows are decoded with the counter
// recorded in the ReplicateMsg, so INSERTs carry the auto_incrementing_id values
// the tablet assigned.
//
// Returns non-OK if the op as a whole cannot be decoded, including if 'result'
// does not match the rows or a row the tablet applied cannot be decoded.
// Returns Status::Aborted() if the write has no row operations.
Status DecodeWriteOpAllRows(const consensus::ReplicateMsg& replicate,
                            const tablet::TxResultPB& result,
                            int32_t schema_version,
                            const Schema& schema,
                            std::vector<CDCRecordPB>* records);

// Returns the tablet's schema version right before the ALTER_SCHEMA op with
// request 'req': the one the leader recorded in the op, or, for an op written
// by an older server, the version before the one the op establishes.
uint32_t PriorSchemaVersion(const tserver::AlterSchemaRequestPB& req);

// Decode an ALTER_SCHEMA_OP or PARTICIPANT_OP ReplicateMsg into a CDCRecordPB.
//
// For ALTER_SCHEMA_OP: produces a DDL record with new_schema and
// new_schema_version, and the prior version (see PriorSchemaVersion()) as
// schema_version.
// For PARTICIPANT_OP: produces BEGIN, COMMIT, or ABORT records.
// For WRITE_OP: returns Status::Aborted() (use DecodeWriteOpAllRows instead).
// For other op types (NO_OP, CHANGE_CONFIG_OP): returns Status::Aborted().
Status DecodeNonWriteReplicateMsg(const consensus::ReplicateMsg& replicate,
                                  CDCRecordPB* record);

// For FULL-mode streams: reconstruct the before-image and full after-image of
// each row touched by a single WRITE_OP ReplicateMsg, reading directly from the
// tablet's MVCC storage.
//
// 'records' are the CDCRecordPBs already decoded from 'replicate' and its
// outcome 'result' by DecodeWriteOpAllRows with 'op_schema' (in WAL order, one
// per applied DML row). If they are all INSERTs, they already carry the
// complete rows, and only 'image_schema_version' is set (to 'schema_version').
// Otherwise, each row the op changes is read as of right before the op, and
// the op's changes are applied to it in order, so that several changes of the
// same row in one op each get their own images. On success:
//   - UPDATE/DELETE (and UPSERT that resolved to an update): 'old_changes' is
//     populated with the complete row state immediately before the change.
//   - INSERT/UPDATE (and UPSERT): 'changes' is replaced with the complete row
//     after the change.
//   - INSERT and UPSERT-that-created-a-row: 'old_changes' stays empty; a UPSERT
//     with no pre-existing row is reclassified to INSERT, and one with a
//     pre-existing row to UPDATE.
//   - A reclassified UPSERT never has 'keep_immutable_columns_on_update' set.
//   - DELETE: 'changes' is left as key-columns-only.
//   - 'image_schema_version' is the version of the tablet's current schema,
//     whose columns the images carry.
// Each row is read with a lookup of its primary key, and read again as of
// right after the op to check that the reconstructed row is the one the tablet
// stored (Corruption if not). 'rows_read' is increased by the number of rows
// that the reads returned.
//
// 'effective_timestamp' is the MVCC pivot: the before-image is read at
// MvccSnapshot(effective_timestamp) and the after-image at
// MvccSnapshot(effective_timestamp + 1). For non-transactional writes this is
// replicate.timestamp(); for transactional writes the caller passes the
// transaction's commit timestamp (writes become visible at commit).
//
// An op is Raft-committed (and readable from the WAL) BEFORE the apply pool
// applies it to the MemRowSet, so before scanning this waits, bounded by
// 'deadline', until every op with timestamp <= effective_timestamp is both safe
// ('time_manager') and applied (the tablet's MvccManager). Without that wait a
// freshly-elected leader with committed-but-not-yet-applied entries could emit a
// wrong/empty after-image. Mirrors the snapshot bootstrap path.
//
// Returns an Incomplete status (mapped by the caller to HISTORY_EXPIRED) if the
// MVCC history needed for the before-image has been garbage-collected, a
// TimedOut status if the apply wait or the reads exceed 'deadline', or a
// ServiceUnavailable status if the tablet's schema changed during the reads.
Status ReconstructBeforeAfterImages(tablet::Tablet* tablet,
                                    consensus::TimeManager* time_manager,
                                    const consensus::ReplicateMsg& replicate,
                                    const tablet::TxResultPB& result,
                                    const Schema& op_schema,
                                    uint64_t effective_timestamp,
                                    const MonoTime& deadline,
                                    std::vector<CDCRecordPB>* records,
                                    int64_t* rows_read);

// For tests only: sets a hook that ReconstructBeforeAfterImages() runs after
// its history pre-check and before it reads the row images, so a test can GC
// the history in between. An empty function removes the hook. Thread-safe.
void SetReconstructImagesHookForTests(std::function<void()> hook);

// Emit all columns of a materialized row 'row' (from a snapshot/read scan) into
// 'record->changes()'. 'schema' is the scan projection schema.
Status PopulateReadRecord(const Schema& schema,
                          const RowBlockRow& row,
                          CDCRecordPB* record);

// Encode the primary key columns of a materialized row 'row' into a byte string
// usable as a CDC snapshot resume key (and as a scan lower bound after being
// decoded + incremented). Allocates scratch from 'arena'.
std::string SerializeSnapshotKey(const Schema& schema,
                                 const RowBlockRow& row,
                                 Arena* arena);

} // namespace cdc
} // namespace kudu
