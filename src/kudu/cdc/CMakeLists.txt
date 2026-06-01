# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#########################################
# cdc_proto
# Plain protobuf (no KRPC) — contains the wire messages and the
# CDCService service definition.  The KRPC stubs are generated below.
#########################################

KRPC_GENERATE(
  CDC_KRPC_SRCS CDC_KRPC_HDRS CDC_KRPC_TGTS
  SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..
  BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../..
  PROTO_FILES cdc.proto)
set(CDC_KRPC_LIBS
  krpc
  kudu_common_proto
  protobuf
  rpc_header_proto
  token_proto
  wire_protocol_proto)
ADD_EXPORTABLE_LIBRARY(cdc_proto
  SRCS ${CDC_KRPC_SRCS}
  DEPS ${CDC_KRPC_LIBS}
  NONLINK_DEPS ${CDC_KRPC_TGTS})

#########################################
# cdc
# Implementation: CDCServiceImpl + row decoder utilities.
#########################################

set(CDC_SRCS
  cdc_service.cc
  cdc_util.cc
)

add_library(cdc ${CDC_SRCS})
target_link_libraries(cdc
  cdc_proto
  consensus
  consensus_proto
  gflags
  glog
  kudu_common
  kudu_common_proto
  log
  log_proto
  master_proto
  rpc_header_proto
  server_process
  tablet
  tserver_proto
  wire_protocol_proto)

#########################################
# cdc_client
# Client-side CDC library: stream management, tablet/leader discovery, and the
# per-tablet consumer machinery. Deliberately does NOT link the server-side
# 'cdc' library (which pulls in tablet/consensus/log); it drives the generated
# proxies directly.
#########################################

set(CDC_CLIENT_SRCS
  cdc_client.cc
  cdc_consumer.cc
)

add_library(cdc_client ${CDC_CLIENT_SRCS})
target_link_libraries(cdc_client
  cdc_proto
  consensus_proto
  gflags
  glog
  krpc
  kudu_common
  kudu_common_proto
  master_proto
  security
  token_proto
  wire_protocol_proto)

#########################################
# Unit tests
#########################################

if (NOT NO_TESTS)
  SET_KUDU_TEST_LINK_LIBS(
    cdc
    cdc_client
    cdc_proto
    kudu_test_util
    tserver_test_util)
  ADD_KUDU_TEST(cdc_util-test)
  ADD_KUDU_TEST(cdc_service-test)
  ADD_KUDU_TEST(cdc_client-test)
endif()
