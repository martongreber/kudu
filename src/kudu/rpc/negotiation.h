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

#include <iosfwd>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/macros.h"
#include "kudu/security/security_flags.h"

namespace kudu {

class MonoTime;

namespace rpc {

class Connection;

enum class AuthenticationType {
  INVALID,
  SASL,
  TOKEN,
  CERTIFICATE,
  JWT,
};
const char* AuthenticationTypeToString(AuthenticationType t);

std::ostream& operator<<(std::ostream& o, AuthenticationType authentication_type);

class Negotiation {
 public:

  // Perform negotiation for a connection (either server or client)
  static void RunNegotiation(const scoped_refptr<Connection>& conn,
                             security::RpcAuthentication authentication,
                             security::RpcEncryption encryption,
                             bool loopback_encryption,
                             MonoTime deadline);
 private:
  DISALLOW_IMPLICIT_CONSTRUCTORS(Negotiation);
};

} // namespace rpc
} // namespace kudu
