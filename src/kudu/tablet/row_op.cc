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

#include "kudu/tablet/row_op.h"

#include <memory>
#include <utility>

#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using kudu::pb_util::SecureDebugString;
using std::unique_ptr;

namespace kudu {

namespace tablet {

RowOp::RowOp(DecodedRowOperation op)
    : decoded_op(std::move(op)) {
  if (!decoded_op.result.ok()) {
    SetFailed(decoded_op.result);
  }
}

void RowOp::SetFailed(const Status& s) {
  DCHECK(!result) << SecureDebugString(*result);
  result.reset(new OperationResultPB());
  StatusToPB(s, result->mutable_failed_status());
}

void RowOp::SetInsertSucceeded(int mrs_id) {
  DCHECK(!result) << SecureDebugString(*result);
  result.reset(new OperationResultPB());
  result->add_mutated_stores()->set_mrs_id(mrs_id);
}

void RowOp::SetErrorIgnored() {
  DCHECK(!result) << SecureDebugString(*result);
  result.reset(new OperationResultPB());
  error_ignored = true;
}

void RowOp::SetMutateSucceeded(unique_ptr<OperationResultPB> result) {
  DCHECK(!this->result) << SecureDebugString(*result);
  this->result = std::move(result);
}

std::string RowOp::ToString(const Schema& schema) const {
  return decoded_op.ToString(schema);
}

void RowOp::SetSkippedResult(const OperationResultPB& result) {
  DCHECK(result.skip_on_replay());
  this->result.reset(new OperationResultPB(result));
}

} // namespace tablet
} // namespace kudu
