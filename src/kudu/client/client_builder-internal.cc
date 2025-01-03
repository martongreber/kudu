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

#include "kudu/client/client_builder-internal.h"

#include <gflags/gflags_declare.h>

#include "kudu/client/replica_controller-internal.h"

DECLARE_int64(rpc_max_message_size);

namespace kudu {

namespace client {

KuduClientBuilder::Data::Data()
    : default_admin_operation_timeout_(MonoDelta::FromSeconds(30)),
      default_rpc_timeout_(MonoDelta::FromSeconds(10)),
      replica_visibility_(internal::ReplicaController::Visibility::VOTERS),
      rpc_max_message_size_(FLAGS_rpc_max_message_size),
      require_authentication_(false),
      encryption_policy_(EncryptionPolicy::OPTIONAL) {
  }

  KuduClientBuilder::Data::~Data() {}

}  // namespace client
} // namespace kudu
