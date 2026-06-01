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
#include <string>
#include <vector>

#include "kudu/common/row_operations.pb.h"
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
} // namespace tablet

namespace cdc {

class CDCColumnValuePB;
class CDCRecordPB;

// Decode all rows from a WRITE_OP ReplicateMsg into CDCRecordPB records.
//
// Each row in the WriteRequestPB produces one CDCRecordPB with op_index,
// op_term, timestamp, and schema_version set from the ReplicateMsg metadata.
// 'schema_version' is the version from the containing WAL segment header.
//
// Returns non-OK on decoding errors.
// Returns Status::Aborted() if the write has no row operations.
Status DecodeWriteOpAllRows(const consensus::ReplicateMsg& replicate,
                            int32_t schema_version,
                            std::vector<CDCRecordPB>* records);

// Decode an ALTER_SCHEMA_OP or PARTICIPANT_OP ReplicateMsg into a CDCRecordPB.
//
// For ALTER_SCHEMA_OP: produces a DDL record with new_schema and schema_version.
// For PARTICIPANT_OP: produces BEGIN, COMMIT, or ABORT records.
// For WRITE_OP: returns Status::Aborted() (use DecodeWriteOpAllRows instead).
// For other op types (NO_OP, CHANGE_CONFIG_OP): returns Status::Aborted().
Status DecodeNonWriteReplicateMsg(const consensus::ReplicateMsg& replicate,
                                  CDCRecordPB* record);

// Decode one row from a WriteRequestPB and populate 'record'.
// 'schema' must correspond to the schema_version in the WriteRequestPB.
// 'row_idx' is the zero-based index of the row within the RowOperationsPB.
//
// Exposed for testing and for callers that need fine-grained row access.
Status DecodeWriteOpRow(const Schema& schema,
                        const kudu::RowOperationsPB& row_ops,
                        int row_idx,
                        CDCRecordPB* record);

// For FULL-mode streams: reconstruct the before-image and full after-image of
// each row touched by a single WRITE_OP ReplicateMsg, reading directly from the
// tablet's MVCC storage.
//
// 'records' are the CDCRecordPBs already decoded from 'replicate' by
// DecodeWriteOpAllRows (in WAL order, one per DML row). On success:
//   - UPDATE/DELETE (and UPSERT that resolved to an update): 'old_changes' is
//     populated with the complete row state immediately before the op.
//   - UPDATE (and UPSERT that resolved to an update): 'changes' is replaced with
//     the complete row after the op.
//   - INSERT and UPSERT-that-created-a-row: 'old_changes' stays empty; a UPSERT
//     with no pre-existing row is reclassified to INSERT.
//   - DELETE: 'changes' is left as key-columns-only.
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
// Returns a NotFound/RuntimeError status (mapped by the caller to
// HISTORY_EXPIRED) if the MVCC history needed for the before-image has been
// garbage-collected, or a TimedOut status if the apply wait exceeds 'deadline'.
Status ReconstructBeforeAfterImages(tablet::Tablet* tablet,
                                    consensus::TimeManager* time_manager,
                                    const consensus::ReplicateMsg& replicate,
                                    uint64_t effective_timestamp,
                                    const MonoTime& deadline,
                                    std::vector<CDCRecordPB>* records);

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
