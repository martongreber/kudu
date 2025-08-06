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

#include "kudu/mini-cluster/external_mini_cluster.h"

#include <iosfwd>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h> // IWYU pragma: keep
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h" // IWYU pragma: keep
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/thrift/client.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/mini_prometheus.h"

using std::set;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cluster {

enum class Kerberos {
  DISABLED,
  ENABLED,
};

enum class HiveMetastore {
  DISABLED,
  ENABLED,
};

enum BuiltInNtp {
  DISABLED = 0,
  ENABLED_SINGLE_SERVER = 1,
  ENABLED_MULTIPLE_SERVERS = 5,
};

// Beautifies test output if a test scenario fails.
std::ostream& operator<<(std::ostream& o, Kerberos opt) {
  switch (opt) {
    case Kerberos::ENABLED:
      return o << "Kerberos::ENABLED";
    case Kerberos::DISABLED:
      return o << "Kerberos::DISABLED";
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, HiveMetastore opt) {
  switch (opt) {
    case HiveMetastore::ENABLED:
      return o << "HiveMetastore::ENABLED";
    case HiveMetastore::DISABLED:
      return o << "HiveMetastore::DISABLED";
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, BuiltInNtp opt) {
  switch (opt) {
    case BuiltInNtp::DISABLED:
      return o << "BuiltInNtp::DISABLED";
    case BuiltInNtp::ENABLED_SINGLE_SERVER:
      return o << "BuiltInNtp::ENABLED_SINGLE_SERVER";
    case BuiltInNtp::ENABLED_MULTIPLE_SERVERS:
      return o << "BuiltInNtp::ENABLED_MULTIPLE_SERVERS";
  }
  return o;
}

class ExternalMiniClusterTest :
    public KuduTest,
#if !defined(NO_CHRONY)
    public testing::WithParamInterface<tuple<Kerberos, HiveMetastore, BuiltInNtp>>
#else
    public testing::WithParamInterface<tuple<Kerberos, HiveMetastore>>
#endif
{
};

INSTANTIATE_TEST_SUITE_P(,
    ExternalMiniClusterTest,
    ::testing::Combine(
        ::testing::Values(Kerberos::DISABLED, Kerberos::ENABLED),
        ::testing::Values(HiveMetastore::DISABLED, HiveMetastore::ENABLED)
#if !defined(NO_CHRONY)
        ,
        ::testing::Values(BuiltInNtp::DISABLED,
                          BuiltInNtp::ENABLED_SINGLE_SERVER,
                          BuiltInNtp::ENABLED_MULTIPLE_SERVERS)
#endif // #if !defined(NO_CHRONY) ...
                          ));

void SmokeTestKerberizedCluster(ExternalMiniClusterOptions opts) {
  ASSERT_TRUE(opts.enable_kerberos);
  int num_tservers = opts.num_tablet_servers;

  ExternalMiniCluster cluster(std::move(opts));
  ASSERT_OK(cluster.Start());

  // Sleep long enough to ensure that the tserver's ticket would have expired
  // if not for the renewal thread doing its thing.
  SleepFor(MonoDelta::FromSeconds(16));

  // Re-kinit for the client, since the client's ticket would have expired as well
  // since the renewal thread doesn't run for the test client.
  ASSERT_OK(cluster.kdc()->Kinit("test-admin"));

  // Restart the master, and make sure the tserver is still able to reconnect and
  // authenticate.
  cluster.master(0)->Shutdown();
  ASSERT_OK(cluster.master(0)->Restart());
  // Ensure that all of the tablet servers can register with the masters.
  ASSERT_OK(cluster.WaitForTabletServerCount(num_tservers, MonoDelta::FromSeconds(30)));
  cluster.Shutdown();
}

void SmokeExternalMiniCluster(const ExternalMiniClusterOptions& opts,
                              ExternalMiniCluster* cluster,
                              vector<HostPort>* master_rpc_addresses) {
  CHECK(cluster);
  CHECK(master_rpc_addresses);
  master_rpc_addresses->clear();

  // Verify each of the masters.
  for (int i = 0; i < opts.num_masters; i++) {
    SCOPED_TRACE(i);
    ExternalMaster* master = CHECK_NOTNULL(cluster->master(i));
    HostPort master_endpoint = master->bound_rpc_hostport();
    string expected_prefix = Substitute("$0:", cluster->GetBindIpForMaster(i));
    if (cluster->bind_mode() == BindMode::UNIQUE_LOOPBACK) {
      EXPECT_NE(expected_prefix, "127.0.0.1:") << "Should bind to unique per-server hosts";
    }
    EXPECT_TRUE(HasPrefixString(master_endpoint.ToString(), expected_prefix))
        << master_endpoint.ToString();

    HostPort master_http = master->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(master_http.ToString(), expected_prefix)) << master_http.ToString();
    master_rpc_addresses->emplace_back(std::move(master_endpoint));
  }

  // Verify each of the tablet servers.
  for (int i = 0; i < opts.num_tablet_servers; i++) {
    SCOPED_TRACE(i);
    ExternalTabletServer* ts = CHECK_NOTNULL(cluster->tablet_server(i));
    HostPort ts_rpc = ts->bound_rpc_hostport();
    string expected_prefix = Substitute("$0:", cluster->GetBindIpForTabletServer(i));
    if (cluster->bind_mode() == BindMode::UNIQUE_LOOPBACK) {
      EXPECT_NE(expected_prefix, "127.0.0.1:") << "Should bind to unique per-server hosts";
    }
    EXPECT_TRUE(HasPrefixString(ts_rpc.ToString(), expected_prefix)) << ts_rpc.ToString();

    HostPort ts_http = ts->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(ts_http.ToString(), expected_prefix)) << ts_http.ToString();
  }

  // Ensure that all of the tablet servers can register with the masters.
  ASSERT_OK(cluster->WaitForTabletServerCount(opts.num_tablet_servers, MonoDelta::FromSeconds(30)));

  // Restart a master and a tablet server. Make sure they come back up with the same ports.
  ExternalMaster* master = cluster->master(0);
  HostPort master_rpc = master->bound_rpc_hostport();
  HostPort master_http = master->bound_http_hostport();

  master->Shutdown();
  ASSERT_OK(master->Restart());

  ASSERT_EQ(master_rpc.ToString(), master->bound_rpc_hostport().ToString());
  ASSERT_EQ(master_http.ToString(), master->bound_http_hostport().ToString());

  ExternalTabletServer* ts = cluster->tablet_server(0);

  HostPort ts_rpc = ts->bound_rpc_hostport();
  HostPort ts_http = ts->bound_http_hostport();

  ts->Shutdown();
  ASSERT_OK(ts->Restart());

  ASSERT_EQ(ts_rpc.ToString(), ts->bound_rpc_hostport().ToString());
  ASSERT_EQ(ts_http.ToString(), ts->bound_http_hostport().ToString());

  // Verify that the HMS is reachable.
  if (opts.hms_mode == HmsMode::ENABLE_HIVE_METASTORE) {
    thrift::ClientOptions hms_client_opts;
    hms_client_opts.enable_kerberos = opts.enable_kerberos;
    hms_client_opts.service_principal = "hive";
    hms::HmsClient hms_client(cluster->hms()->address(), hms_client_opts);
    ASSERT_OK(hms_client.Start());
    vector<string> tables;
    ASSERT_OK(hms_client.GetTableNames("default", &tables));
    ASSERT_TRUE(tables.empty()) << "tables: " << tables;
  }

  // Verify that, in a Kerberized cluster, if we drop our Kerberos environment,
  // we can't make RPCs to a server.
  if (opts.enable_kerberos) {
    ASSERT_OK(cluster->kdc()->Kdestroy());
    Status s = cluster->SetFlag(ts, "foo", "bar");
    // The error differs depending on the version of Kerberos, so we match
    // either message.
    ASSERT_STR_CONTAINS(s.ToString(),
                        "server requires authentication, "
                        "but client does not have Kerberos credentials available");
  }

  // ExternalTabletServer::Restart() still returns OK, even if the tablet server crashed.
  ts->Shutdown();
  ts->mutable_flags()->push_back("--fault_before_start=1.0");
  ASSERT_OK(ts->Restart());
  ASSERT_FALSE(ts->IsProcessAlive());
  // Since the process should have already crashed, waiting for an injected crash with no
  // timeout should still return OK.
  ASSERT_OK(ts->WaitForInjectedCrash(MonoDelta::FromSeconds(0)));
  ts->mutable_flags()->pop_back();
  ts->Shutdown();
  ASSERT_OK(ts->Restart());
  ASSERT_TRUE(ts->IsProcessAlive());
}

TEST_F(ExternalMiniClusterTest, TestKerberosReacquire) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ExternalMiniClusterOptions opts;
  opts.enable_kerberos = true;
  // Set the kerberos ticket lifetime as 15 seconds to force ticket reacquisition every 15 seconds.
  // Note that we do not renew tickets but always acquire a new one.
  opts.mini_kdc_options.ticket_lifetime = "15s";
  opts.num_tablet_servers = 1;

  NO_FATALS(SmokeTestKerberizedCluster(std::move(opts)));
}

TEST_P(ExternalMiniClusterTest, TestBasicOperation) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ExternalMiniClusterOptions opts;
  const auto& param = GetParam();
  opts.enable_kerberos = std::get<0>(param) == Kerberos::ENABLED;
  if (std::get<1>(param) == HiveMetastore::ENABLED) {
    opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  }
#if !defined(NO_CHRONY)
  opts.num_ntp_servers = std::get<2>(param);
#endif

  opts.num_masters = 3;
  opts.num_tablet_servers = 3;

  unique_ptr<ExternalMiniCluster> cluster(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());
  vector<HostPort> master_rpc_addresses;
  NO_FATALS(SmokeExternalMiniCluster(opts, cluster.get(), &master_rpc_addresses));

  // Destroy the cluster object, create a new one with the same options,
  // and run the same scenario again at already existing data directory
  // structure.
  // This is to make sure that:
  //   * the cluster's components are shutdown upon the destruction
  //     of the object
  //   * configuration files and other persistent data for cluster components
  //     are either reused or rewritten/recreated in consistent manner
  // The only cluster options to preserve is the masters' RPC addresses from
  // the prior run.
  opts.master_rpc_addresses = master_rpc_addresses;
  cluster.reset(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());
  NO_FATALS(SmokeExternalMiniCluster(opts, cluster.get(), &master_rpc_addresses));
  ASSERT_EQ(opts.master_rpc_addresses, master_rpc_addresses);

  // Shutdown the cluster explicitly. This is not strictly necessary since
  // the cluster will be shutdown upon the call of ExternalMiniCluster's
  // destructor, but this is done in the context of testing. This is to verify
  // that ExternalMiniCluster object destructor works as expected in the case
  // if the cluster has already been shutdown.
  cluster->Shutdown();
}

TEST_P(ExternalMiniClusterTest, TestAddMaster) {
  ExternalMiniClusterOptions opts;
  const auto& param = GetParam();
  opts.enable_kerberos = std::get<0>(param) == Kerberos::ENABLED;
  if (std::get<1>(param) == HiveMetastore::ENABLED) {
    opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  }
#if !defined(NO_CHRONY)
  opts.num_ntp_servers = std::get<2>(param);
#endif

  opts.num_masters = 3;
  opts.num_tablet_servers = 1;

  unique_ptr<ExternalMiniCluster> cluster(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());

  // Add a master and wait for it to start up and get reported to.
  ASSERT_OK(cluster->AddMaster());
  ASSERT_OK(cluster->master(opts.num_masters)->WaitForCatalogManager());
  cluster->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster->tablet_server(0)->Restart());
  ASSERT_OK(cluster->WaitForTabletServerCount(
      opts.num_tablet_servers, MonoDelta::FromSeconds(30), /*master_idx*/opts.num_masters));

  // Smoke the cluster using an updated 'opts' that expects the new number of
  // masters.
  opts.num_masters++;
  vector<HostPort> master_rpc_addresses;
  NO_FATALS(SmokeExternalMiniCluster(opts, cluster.get(), &master_rpc_addresses));

  // Shutdown the cluster explicitly on top of the one in the cluster's
  // destructor to test repeated calls to Shutdown().
  cluster->Shutdown();
}

TEST_P(ExternalMiniClusterTest, TestRestApiSpnegoConnectionThroughCurl) {
  ExternalMiniClusterOptions opts;
  opts.enable_kerberos = true;
  opts.enable_rest_api = true;
  ExternalMiniCluster cluster(std::move(opts));
  ASSERT_OK(cluster.Start());

  {
    EasyCurl curl;
    faststring buf;
    Status s = curl.FetchURL(
        Substitute("$0/api/v1/tables", cluster.master(0)->bound_http_hostport().ToString()), &buf);
    ASSERT_STR_CONTAINS(s.ToString(), "HTTP 401");
  }

  ASSERT_OK(cluster.kdc()->Kinit("test-admin"));
  {
    EasyCurl curl;
    faststring buf;
    curl.set_auth(CurlAuthType::SPNEGO);
    ASSERT_OK(curl.FetchURL(
        Substitute("$0/api/v1/tables", cluster.master(0)->bound_http_hostport().ToString()), &buf));
  }
}

TEST_F(ExternalMiniClusterTest, TestPrometheusIntegration) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ExternalMiniClusterOptions opts;
  opts.enable_prometheus = true;
  opts.num_masters = 2;
  opts.num_tablet_servers = 3;

  ExternalMiniCluster cluster(opts);
  ASSERT_OK(cluster.Start());

  // Verify Prometheus is running
  ASSERT_NE(nullptr, cluster.prometheus());
  ASSERT_TRUE(cluster.prometheus()->IsRunning());
  
  LOG(INFO) << "Prometheus is running on: " << cluster.prometheus()->web_url();
  
  // Wait a bit for Prometheus to fully start up and discover targets
  SleepFor(MonoDelta::FromSeconds(8));

  EasyCurl curl;
  faststring buf;
  
  // First verify that our metrics endpoints are accessible
  LOG(INFO) << "Verifying that Kudu metrics endpoints are accessible...";
  for (int i = 0; i < cluster.num_masters(); i++) {
    const string metrics_url = Substitute("http://$0/metrics", 
                                         cluster.master(i)->bound_http_hostport().ToString());
    buf.clear();
    Status s = curl.FetchURL(metrics_url, &buf);
    LOG(INFO) << "Master " << i << " metrics at " << metrics_url << ": " 
              << (s.ok() ? "OK" : s.ToString()) << " (" << buf.size() << " bytes)";
    ASSERT_OK(s);
  }
  
  for (int i = 0; i < cluster.num_tablet_servers(); i++) {
    const string metrics_url = Substitute("http://$0/metrics", 
                                         cluster.tablet_server(i)->bound_http_hostport().ToString());
    buf.clear();
    Status s = curl.FetchURL(metrics_url, &buf);
    LOG(INFO) << "Tablet server " << i << " metrics at " << metrics_url << ": " 
              << (s.ok() ? "OK" : s.ToString()) << " (" << buf.size() << " bytes)";
    ASSERT_OK(s);
  }
  
  // Test Prometheus API - verify that our Kudu services are registered as targets
  const string targets_url = Substitute("$0/api/v1/targets", cluster.prometheus()->web_url());
  ASSERT_OK(curl.FetchURL(targets_url, &buf));
  const string targets_response = buf.ToString();
  
  LOG(INFO) << "Prometheus targets API response length: " << targets_response.size();
  LOG(INFO) << "Full Prometheus targets response: " << targets_response;
  
  // Parse the JSON response
  JsonReader reader(targets_response);
  ASSERT_OK(reader.Init());
  
  // Verify the response status is success
  string status;
  ASSERT_OK(reader.ExtractString(reader.root(), "status", &status));
  ASSERT_EQ("success", status);
  
  // Extract the data.activeTargets array
  const rapidjson::Value* data;
  ASSERT_OK(reader.ExtractObject(reader.root(), "data", &data));
  
  vector<const rapidjson::Value*> active_targets;
  ASSERT_OK(reader.ExtractObjectArray(data, "activeTargets", &active_targets));
  
  // Build expected target set (all masters and tablet servers)
  std::set<string> expected_targets;
  for (int i = 0; i < cluster.num_masters(); i++) {
    expected_targets.insert(cluster.master(i)->bound_http_hostport().ToString());
  }
  for (int i = 0; i < cluster.num_tablet_servers(); i++) {
    expected_targets.insert(cluster.tablet_server(i)->bound_http_hostport().ToString());
  }
  
  LOG(INFO) << "Expected " << expected_targets.size() << " targets, found " 
            << active_targets.size() << " active targets in Prometheus";
  
  // Verify each target in the Prometheus response
  std::set<string> found_targets;
  for (const auto* target : active_targets) {
    // Extract the discoveredLabels.__address__ field which contains the target address
    const rapidjson::Value* discovered_labels;
    ASSERT_OK(reader.ExtractObject(target, "discoveredLabels", &discovered_labels));
    
    string target_address;
    ASSERT_OK(reader.ExtractString(discovered_labels, "__address__", &target_address));
    
    found_targets.insert(target_address);
    
    // Extract health status
    string health;
    ASSERT_OK(reader.ExtractString(target, "health", &health));
    
    LOG(INFO) << "Found Prometheus target: " << target_address << " with health: " << health;
    
    // Verify this target is one of our expected Kudu services
    ASSERT_TRUE(expected_targets.count(target_address) > 0) 
        << "Unexpected target found in Prometheus: " << target_address;
  }
  
  // Verify we found all expected targets
  ASSERT_EQ(expected_targets.size(), found_targets.size()) 
      << "Expected targets: " << JoinStrings(vector<string>(expected_targets.begin(), expected_targets.end()), ", ") 
      << ", Found targets: " << JoinStrings(vector<string>(found_targets.begin(), found_targets.end()), ", ");
  
  for (const string& expected_target : expected_targets) {
    ASSERT_TRUE(found_targets.count(expected_target) > 0) 
        << "Expected target not found in Prometheus: " << expected_target;
  }
  
  // Also verify that the metrics endpoints are accessible from our side
  for (const string& target : expected_targets) {
    const string metrics_url = Substitute("http://$0/metrics", target);
    buf.clear();
    ASSERT_OK(curl.FetchURL(metrics_url, &buf));
    ASSERT_GT(buf.size(), 0) << "Metrics endpoint not accessible: " << metrics_url;
    
    // Verify it contains some expected Kudu metrics
    const string metrics_content = buf.ToString();
    ASSERT_STR_CONTAINS(metrics_content, "kudu_") << "No Kudu metrics found at: " << metrics_url;
  }

  LOG(INFO) << "Prometheus integration test completed successfully";
}

} // namespace cluster
} // namespace kudu
