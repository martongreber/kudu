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

// Authorization of CDC on a cluster with Kerberos, Ranger and fine-grained
// access control on the tablet servers: the master's stream RPCs and the
// tablet servers' data path, for users with different privileges.

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/cdc/cdc.pb.h"
#include "kudu/cdc/cdc_client.h"
#include "kudu/cdc/cdc_consumer.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/ranger/mini_ranger.h"
#include "kudu/ranger/ranger.pb.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cdc::CDCClient;
using kudu::cdc::CDCConsumer;
using kudu::cdc::CDCRecordBatch;
using kudu::cdc::CDCStreamInfo;
using kudu::cdc::CDCStreamOptions;
using kudu::cdc::CDCTableMetadata;
using kudu::cdc::CDCTabletInfo;
using kudu::cdc::CDCTabletProgress;
using kudu::cdc::CDCTabletState;
using kudu::cdc::CheckpointRequestPB;
using kudu::cdc::CheckpointResponsePB;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::master::RefreshAuthzCacheRequestPB;
using kudu::master::RefreshAuthzCacheResponsePB;
using kudu::ranger::ActionPB;
using kudu::ranger::AuthorizationPolicy;
using kudu::ranger::PolicyItem;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::RpcController;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {

constexpr const char* kDb = "db";
constexpr const char* kTable = "db.table";
constexpr const char* kOtherTable = "db.other";

// A superuser and a trusted user: the external mini cluster sets
// --superuser_acl and --trusted_user_acl to it. The service user is not a
// superuser then.
constexpr const char* kAdminUser = "test-admin";
// ALL on every table of the database.
constexpr const char* kAllUser = "all-user";
// METADATA, SELECT and ALTER on 'table': may create a stream over it.
constexpr const char* kStreamUser = "stream-user";
// METADATA and SELECT on 'table', but not ALTER.
constexpr const char* kSelectUser = "select-user";
// METADATA on 'table', and SELECT on its key column only.
constexpr const char* kColumnUser = "column-user";
// No privileges.
constexpr const char* kNoPrivilegeUser = "no-privilege-user";

const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

} // anonymous namespace

class CDCAuthzITest : public ExternalMiniClusterITestBase {
 protected:
  void SetUp() override {
    NO_FATALS(ExternalMiniClusterITestBase::SetUp());
    ExternalMiniClusterOptions opts;
    opts.enable_kerberos = true;
    opts.enable_ranger = true;
    const string user_acl = Substitute("--user_acl=$0,$1,$2,$3,$4", kAllUser, kStreamUser,
                                       kSelectUser, kColumnUser, kNoPrivilegeUser);
    opts.extra_master_flags = { "--cdc_enabled=true", user_acl };
    opts.extra_tserver_flags = { "--tserver_enforce_access_control=true", user_acl };
    NO_FATALS(StartClusterWithOpts(std::move(opts)));

    for (const char* user : { kAdminUser, kAllUser, kStreamUser, kSelectUser, kColumnUser,
                              kNoPrivilegeUser }) {
      ASSERT_OK(cluster_->kdc()->CreateUserPrincipal(user));
    }
    NO_FATALS(Grant(kAllUser, "*", {}, { ActionPB::ALL }));
    NO_FATALS(Grant(kStreamUser, "table", {},
                    { ActionPB::METADATA, ActionPB::SELECT, ActionPB::ALTER }));
    NO_FATALS(Grant(kSelectUser, "table", {}, { ActionPB::METADATA, ActionPB::SELECT }));
    NO_FATALS(Grant(kColumnUser, "table", {}, { ActionPB::METADATA }));
    NO_FATALS(Grant(kColumnUser, "table", { "key" }, { ActionPB::SELECT }));
    NO_FATALS(RefreshAuthzPolicies());

    ASSERT_OK(cluster_->kdc()->Kinit(kAdminUser));
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
    for (const char* table : { kTable, kOtherTable }) {
      KuduSchema schema;
      KuduSchemaBuilder b;
      b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
      b.AddColumn("val")->Type(KuduColumnSchema::INT32)->NotNull();
      ASSERT_OK(b.Build(&schema));
      unique_ptr<KuduTableCreator> creator(client_->NewTableCreator());
      ASSERT_OK(creator->table_name(table)
                .schema(&schema)
                .set_range_partition_columns({ "key" })
                .num_replicas(1)
                .Create());
    }
  }

  // Grants 'actions' on 'table' of the database (on its 'columns' only, if
  // given) to 'user'.
  void Grant(const string& user, const string& table, vector<string> columns,
             vector<ActionPB> actions) {
    AuthorizationPolicy policy;
    policy.databases = { kDb };
    policy.tables = { table };
    policy.columns = columns.empty() ? vector<string>{ "*" } : std::move(columns);
    policy.items.emplace_back(PolicyItem({ user }, std::move(actions), false));
    ASSERT_OK(cluster_->ranger()->AddPolicy(std::move(policy)));
  }

  void RefreshAuthzPolicies() {
    ASSERT_EVENTUALLY([&] {
      RefreshAuthzCacheRequestPB req;
      RefreshAuthzCacheResponsePB resp;
      RpcController rpc;
      rpc.set_timeout(MonoDelta::FromSeconds(10));
      ASSERT_OK(cluster_->master_proxy()->RefreshAuthzCache(req, &resp, &rpc));
      ASSERT_FALSE(resp.has_error()) << resp.error().ShortDebugString();
    });
  }

  // Logs in as 'user' and returns a CDC client that connects as 'user'.
  unique_ptr<CDCClient> ClientAs(const string& user) {
    CHECK_OK(cluster_->kdc()->Kinit(user));
    CDCClient::Options opts;
    opts.master_addresses.emplace_back(cluster_->master()->bound_rpc_addr().ToString());
    opts.rpc_timeout = kTimeout;
    unique_ptr<CDCClient> client;
    CHECK_OK(CDCClient::Create(std::move(opts), &client));
    return client;
  }

  Status CreateStreamAs(const string& user, const string& table, string* stream_id) {
    return ClientAs(user)->CreateStream(table, CDCStreamOptions(), stream_id);
  }

  void InsertRows(int start, int count) {
    shared_ptr<KuduTable> table;
    ASSERT_OK(client_->OpenTable(kTable, &table));
    shared_ptr<KuduSession> session = client_->NewSession();
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    for (int i = start; i < start + count; ++i) {
      KuduInsert* insert = table->NewInsert();
      ASSERT_OK(insert->mutable_row()->SetInt32("key", i));
      ASSERT_OK(insert->mutable_row()->SetInt32("val", i));
      ASSERT_OK(session->Apply(insert));
    }
  }

  static CDCTabletProgress Progress(const CDCConsumer& consumer) {
    vector<CDCTabletProgress> progress;
    consumer.GetProgress(&progress);
    CHECK_EQ(1, progress.size());
    return progress[0];
  }
};

// Creating a stream requires SELECT on all columns plus ALTER on the table, or
// ALL. The stream records its creator as its owner.
TEST_F(CDCAuthzITest, CreateStreamRequiresSelectAndAlter) {
  string stream_id;
  for (const char* user : { kSelectUser, kColumnUser, kNoPrivilegeUser }) {
    SCOPED_TRACE(user);
    Status s = CreateStreamAs(user, kTable, &stream_id);
    EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }
  vector<CDCStreamInfo> streams;
  ASSERT_OK(ClientAs(kAdminUser)->ListStreams("", &streams));
  EXPECT_TRUE(streams.empty());

  for (const char* user : { kStreamUser, kAllUser, kAdminUser }) {
    SCOPED_TRACE(user);
    ASSERT_OK(CreateStreamAs(user, kTable, &stream_id));
  }
  // The stream user has no privilege on the other table.
  Status s = CreateStreamAs(kStreamUser, kOtherTable, &stream_id);
  EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();

  // At the stream quota, an unauthorized user is still told that it is not
  // authorized, rather than that the quota is reached.
  ASSERT_OK(cluster_->SetFlag(cluster_->master(), "cdc_max_streams", "3"));
  s = CreateStreamAs(kSelectUser, kTable, &stream_id);
  EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();
  s = CreateStreamAs(kStreamUser, kTable, &stream_id);
  EXPECT_TRUE(s.IsServiceUnavailable()) << s.ToString();
}

// A stream may be deleted by its owner, by a user with ALL on its table, and by
// a superuser, but not by another user.
TEST_F(CDCAuthzITest, DeleteStreamOfAnotherUser) {
  vector<string> stream_ids(3);
  for (auto& stream_id : stream_ids) {
    ASSERT_OK(CreateStreamAs(kStreamUser, kTable, &stream_id));
  }
  for (const char* user : { kSelectUser, kColumnUser, kNoPrivilegeUser }) {
    SCOPED_TRACE(user);
    Status s = ClientAs(user)->DeleteStream(stream_ids[0]);
    EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }
  ASSERT_OK(ClientAs(kStreamUser)->DeleteStream(stream_ids[0]));
  ASSERT_OK(ClientAs(kAllUser)->DeleteStream(stream_ids[1]));
  ASSERT_OK(ClientAs(kAdminUser)->DeleteStream(stream_ids[2]));
}

// A user sees only the streams of the tables it may see the metadata of, and
// is not given the snapshot resume keys.
TEST_F(CDCAuthzITest, ListAndDescribeFilteredByPrivilege) {
  string stream_id;
  string other_stream_id;
  ASSERT_OK(CreateStreamAs(kAllUser, kTable, &stream_id));
  ASSERT_OK(CreateStreamAs(kAllUser, kOtherTable, &other_stream_id));

  vector<CDCStreamInfo> streams;
  ASSERT_OK(ClientAs(kAllUser)->ListStreams("", &streams));
  EXPECT_EQ(2, streams.size());
  for (const char* user : { kStreamUser, kColumnUser }) {
    SCOPED_TRACE(user);
    unique_ptr<CDCClient> client = ClientAs(user);
    ASSERT_OK(client->ListStreams("", &streams));
    ASSERT_EQ(1, streams.size());
    EXPECT_EQ(stream_id, streams[0].stream_id);
    CDCStreamInfo info;
    ASSERT_OK(client->GetStreamInfo(stream_id, &info));
    Status s = client->GetStreamInfo(other_stream_id, &info);
    EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }
  unique_ptr<CDCClient> client = ClientAs(kNoPrivilegeUser);
  ASSERT_OK(client->ListStreams("", &streams));
  EXPECT_TRUE(streams.empty());
  CDCStreamInfo info;
  Status s = client->GetStreamInfo(stream_id, &info);
  EXPECT_TRUE(s.IsNotAuthorized()) << s.ToString();
}

// The tablet servers serve the changes of a table only to a user that may
// SELECT all its columns: a user that may read some columns only stops with a
// fatal error rather than retrying forever, and is not given any record.
TEST_F(CDCAuthzITest, DataPathRequiresSelectOnAllColumns) {
  string stream_id;
  ASSERT_OK(CreateStreamAs(kAdminUser, kTable, &stream_id));
  NO_FATALS(InsertRows(0, 5));

  const auto consume = [&](const string& user, std::mutex* lock, int* num_records,
                           unique_ptr<CDCConsumer>* consumer,
                           unique_ptr<CDCClient>* client) {
    *client = ClientAs(user);
    CDCConsumer::Options opts;
    opts.stream_id = stream_id;
    opts.start_mode = CDCConsumer::kEarliest;
    ASSERT_OK(CDCConsumer::Create(client->get(), std::move(opts), consumer));
    ASSERT_OK((*consumer)->Start([lock, num_records](const CDCRecordBatch& batch) {
      std::lock_guard<std::mutex> l(*lock);
      *num_records += batch.records.size();
      return Status::OK();
    }));
  };

  std::mutex lock;
  {
    int num_records = 0;
    unique_ptr<CDCClient> client;
    unique_ptr<CDCConsumer> consumer;
    NO_FATALS(consume(kSelectUser, &lock, &num_records, &consumer, &client));
    ASSERT_EVENTUALLY([&] {
      std::lock_guard<std::mutex> l(lock);
      ASSERT_EQ(5, num_records);
    });
    consumer->Stop();
  }

  int num_records = 0;
  unique_ptr<CDCClient> client;
  unique_ptr<CDCConsumer> consumer;
  NO_FATALS(consume(kColumnUser, &lock, &num_records, &consumer, &client));
  ASSERT_EVENTUALLY([&] {
    const CDCTabletProgress p = Progress(*consumer);
    ASSERT_EQ(CDCTabletState::kFailed, p.state) << p.last_error.ToString();
    ASSERT_TRUE(p.terminal_error.IsRemoteError()) << p.terminal_error.ToString();
  });
  consumer->Stop();
  {
    std::lock_guard<std::mutex> l(lock);
    EXPECT_EQ(0, num_records);
  }

  // Neither are its checkpoints accepted.
  CDCTableMetadata md;
  ASSERT_OK(client->GetTableMetadata(kTable, /*by_id=*/false, &md));
  ASSERT_TRUE(md.has_authz_token);
  vector<CDCTabletInfo> tablets;
  ASSERT_OK(client->GetTabletLocations(md.table_id, &tablets));
  ASSERT_EQ(1, tablets.size());
  CheckpointRequestPB req;
  req.set_stream_id(stream_id);
  req.set_tablet_id(tablets[0].tablet_id);
  req.set_op_index(1);
  *req.mutable_authz_token() = md.authz_token;
  CheckpointResponsePB resp;
  ErrorStatusPB::RpcErrorCodePB rpc_error;
  Status s = client->Checkpoint(tablets[0].leader, req, &resp, &rpc_error);
  EXPECT_TRUE(s.IsRemoteError()) << s.ToString();
  EXPECT_EQ(ErrorStatusPB::FATAL_UNAUTHORIZED, rpc_error);
}

} // namespace kudu
