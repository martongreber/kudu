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

#include "kudu/consensus/pending_rounds.h"

#include <iterator>
#include <ostream>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread_restrictions.h"

DEFINE_bool(raft_allow_committed_pending_index_gap, false,
            "Allow a gap between last committed and first pending operation "
            "in the queue when starting Raft consensus. For testing only!");
TAG_FLAG(raft_allow_committed_pending_index_gap, runtime);
TAG_FLAG(raft_allow_committed_pending_index_gap, unsafe);

using kudu::pb_util::SecureShortDebugString;
using std::string;
using strings::Substitute;

namespace kudu {
namespace consensus {

//------------------------------------------------------------
// PendingRounds
//------------------------------------------------------------

PendingRounds::PendingRounds(const string& log_prefix,
                             TimeManager* time_manager)
    : log_prefix_(log_prefix),
      last_committed_op_id_(MinimumOpId()),
      time_manager_(time_manager) {}

Status PendingRounds::CancelPendingOps() {
  ThreadRestrictions::AssertWaitAllowed();
  if (pending_ops_.empty()) {
    return Status::OK();
  }

  LOG_WITH_PREFIX(INFO) << "Trying to abort " << pending_ops_.size()
                        << " pending ops.";
  // Abort ops in reverse index order. See KUDU-1678.
  static const auto kAbortedStatus = Status::Aborted("Op aborted");
  for (auto op = pending_ops_.crbegin(); op != pending_ops_.crend(); ++op) {
    const scoped_refptr<ConsensusRound>& round = op->second;
    // We cancel only ops whose applies have not yet been triggered.
    LOG_WITH_PREFIX(INFO) << "Aborting op as it isn't in flight: "
                          << SecureShortDebugString(*round->replicate_msg());
    round->NotifyReplicationFinished(kAbortedStatus);
  }
  return Status::OK();
}

void PendingRounds::AbortOpsAfter(int64_t index) {
  LOG_WITH_PREFIX(INFO) << "Aborting all ops after (but not including) "
                        << index;

  DCHECK_GE(index, 0);
  OpId new_preceding;

  auto iter = pending_ops_.lower_bound(index);

  // Either the new preceding id is in the pendings set or it must be equal to the
  // committed index since we can't truncate already committed operations.
  if (iter != pending_ops_.end() && iter->first == index) {
    new_preceding = iter->second->replicate_msg()->id();
    ++iter;
  } else {
    CHECK_EQ(index, last_committed_op_id_.index());
    new_preceding = last_committed_op_id_;
  }

  static const auto kAbortedStatus = Status::Aborted("Op aborted by new leader");
  for (; iter != pending_ops_.end();) {
    const scoped_refptr<ConsensusRound>& round = iter->second;
    const auto op_type = round->replicate_msg()->op_type();
    LOG_WITH_PREFIX(INFO)
        << "Aborting uncommitted " << OperationType_Name(op_type)
        << " operation due to leader change: " << round->replicate_msg()->id();

    round->NotifyReplicationFinished(kAbortedStatus);
    // Erase the entry from pendings.
    iter = pending_ops_.erase(iter);
  }
}

Status PendingRounds::AddPendingOperation(const scoped_refptr<ConsensusRound>& round) {
  InsertOrDie(&pending_ops_, round->replicate_msg()->id().index(), round);
  return Status::OK();
}

scoped_refptr<ConsensusRound> PendingRounds::GetPendingOpByIndexOrNull(int64_t index) {
  return FindPtrOrNull(pending_ops_, index);
}

bool PendingRounds::IsOpCommittedOrPending(const OpId& op_id, bool* term_mismatch) {
  *term_mismatch = false;

  if (op_id.index() <= GetCommittedIndex()) {
    return true;
  }

  scoped_refptr<ConsensusRound> round = GetPendingOpByIndexOrNull(op_id.index());
  if (!round) {
    return false;
  }

  if (round->id().term() != op_id.term()) {
    *term_mismatch = true;
    return false;
  }
  return true;
}

const OpId& PendingRounds::GetLastPendingOpOpId() const {
  return pending_ops_.empty()
      ? MinimumOpId() : pending_ops_.crbegin()->second->id();
}

Status PendingRounds::AdvanceCommittedIndex(int64_t committed_index) {
  // If we already committed up to (or past) 'id' return.
  // This can happen in the case that multiple UpdateConsensus() calls end
  // up in the RPC queue at the same time, and then might get interleaved out
  // of order.
  if (last_committed_op_id_.index() >= committed_index) {
    VLOG_WITH_PREFIX(1)
      << "Already marked ops through " << last_committed_op_id_ << " as committed. "
      << "Now trying to mark " << committed_index << " which would be a no-op.";
    return Status::OK();
  }

  if (pending_ops_.empty()) {
    LOG(ERROR) << "Advancing commit index to " << committed_index
               << " from " << last_committed_op_id_
               << " we have no pending ops"
               << GetStackTrace();
    VLOG_WITH_PREFIX(1) << "No ops to mark as committed up to: "
                        << committed_index;
    return Status::OK();
  }

  // Stop at the operation after the last one we must commit.
  const auto end_iter = pending_ops_.upper_bound(committed_index);

  // Start at the operation after the last committed one.
  auto iter = pending_ops_.upper_bound(last_committed_op_id_.index());
  DCHECK(iter != pending_ops_.end());

  VLOG_WITH_PREFIX(1) << "Last triggered apply was: "
                      <<  last_committed_op_id_
                      << " Starting to apply from log index: " << iter->first;

  while (iter != end_iter) {
    scoped_refptr<ConsensusRound> round = iter->second; // Make a copy.
    DCHECK(round);
    const OpId& current_id = round->id();

    if (PREDICT_TRUE(last_committed_op_id_ != MinimumOpId())) {
      CHECK_OK(CheckOpInSequence(last_committed_op_id_, current_id));
    }

    iter = pending_ops_.erase(iter);
    last_committed_op_id_ = round->id();
    time_manager_->AdvanceSafeTimeWithMessage(*round->replicate_msg());
    round->NotifyReplicationFinished(Status::OK());
  }

  return Status::OK();
}

Status PendingRounds::SetInitialCommittedOpId(const OpId& committed_op) {
  CHECK_EQ(last_committed_op_id_.index(), 0);
  if (!pending_ops_.empty()) {
    int64_t first_pending_index = pending_ops_.begin()->first;
    if (committed_op.index() < first_pending_index) {
      if (PREDICT_TRUE(!FLAGS_raft_allow_committed_pending_index_gap) &&
          committed_op.index() != first_pending_index - 1) {
        return Status::Corruption(Substitute(
            "pending operations should start at first operation "
            "after the committed operation (committed=$0, first pending=$1)",
            OpIdToString(committed_op), first_pending_index));
      }
      last_committed_op_id_ = committed_op;
    }

    RETURN_NOT_OK(AdvanceCommittedIndex(committed_op.index()));
    CHECK_EQ(SecureShortDebugString(last_committed_op_id_),
             SecureShortDebugString(committed_op));

  } else {
    last_committed_op_id_ = committed_op;
  }
  return Status::OK();
}

Status PendingRounds::CheckOpInSequence(const OpId& previous, const OpId& current) {
  if (current.term() < previous.term()) {
    return Status::Corruption(Substitute("New operation's term is not >= than the previous "
        "op's term. Current: $0. Previous: $1", OpIdToString(current), OpIdToString(previous)));
  }
  if (current.index() != previous.index() + 1) {
    return Status::Corruption(Substitute("New operation's index does not follow the previous"
        " op's index. Current: $0. Previous: $1", OpIdToString(current), OpIdToString(previous)));
  }
  return Status::OK();
}

int64_t PendingRounds::GetCommittedIndex() const {
  return last_committed_op_id_.index();
}

int64_t PendingRounds::GetTermWithLastCommittedOp() const {
  return last_committed_op_id_.term();
}

int PendingRounds::GetNumPendingOps() const {
  return pending_ops_.size();
}

}  // namespace consensus
}  // namespace kudu
