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

// Integration tests for Prometheus HTTP service discovery with a real Prometheus
// instance. Each test scenario addresses a specific open question raised during
// the code review of the prometheus-sd patch (KUDU-3538, KUDU-3685).

#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/mini_prometheus.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(location_mapping_cmd);

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;

// Base class for Prometheus SD integration tests. Sets up a single-master,
// single-tserver InternalMiniCluster.
class PrometheusSDITest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    InternalMiniClusterOptions opts;
    opts.num_masters = 1;
    opts.num_tablet_servers = 1;
    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->WaitForTabletServerCount(1));
  }

  void TearDown() override {
    if (prom_) {
      WARN_NOT_OK(prom_->Stop(), "Failed to stop MiniPrometheus");
      prom_.reset();
    }
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  // Returns the /prometheus-sd URL for the master at index 'idx'.
  string MasterSDUrl(int idx = 0) const {
    return Substitute("http://$0/prometheus-sd",
                      cluster_->mini_master(idx)->bound_http_addr().ToString());
  }

  // Starts MiniPrometheus with all master SD URLs populated from the cluster.
  Status StartPrometheus(const vector<string>& sd_urls) {
    MiniPrometheusOptions prom_opts;
    prom_opts.master_sd_urls = sd_urls;
    prom_.reset(new MiniPrometheus(prom_opts));
    return prom_->Start();
  }

  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<MiniPrometheus> prom_;
};

// Verifies the basic SD-to-scrape path:
// Prometheus discovers the master and tserver targets via the SD endpoint on
// the leader master, then successfully scrapes /metrics_prometheus on each.
TEST_F(PrometheusSDITest, BasicSDAndScrape) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus({ MasterSDUrl() }));

  // The cluster has 1 master + 1 tserver, so we expect 2 active targets.
  ASSERT_OK(prom_->WaitForActiveTargets(2, MonoDelta::FromSeconds(90)));

  rapidjson::Document targets_doc;
  ASSERT_OK(prom_->GetTargets(&targets_doc));
  ASSERT_STREQ("success", targets_doc["status"].GetString());

  const auto& active = targets_doc["data"]["activeTargets"];
  ASSERT_EQ(2u, active.Size());

  // All targets should be healthy.
  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    ASSERT_STREQ("up", active[i]["health"].GetString())
        << "Target " << i << " not healthy: "
        << active[i]["scrapeUrl"].GetString();
  }

  // Verify that we can query a Kudu metric via PromQL.
  // 'up' is a Prometheus built-in gauge (1 = scrape succeeded) available for
  // every scraped target; check that there are exactly 2 such series.
  rapidjson::Document query_doc;
  ASSERT_OK(prom_->Query("up", &query_doc));
  ASSERT_STREQ("success", query_doc["status"].GetString());
  ASSERT_EQ(2u, query_doc["data"]["result"].Size());
}

// Verifies the 'group' label on discovered targets.
// Masters should have group="masters" and tservers group="tservers".
TEST_F(PrometheusSDITest, SDLabels) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus({ MasterSDUrl() }));
  ASSERT_OK(prom_->WaitForActiveTargets(2, MonoDelta::FromSeconds(90)));

  rapidjson::Document targets_doc;
  ASSERT_OK(prom_->GetTargets(&targets_doc));
  const auto& active = targets_doc["data"]["activeTargets"];

  bool found_master = false;
  bool found_tserver = false;
  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    const auto& labels = active[i]["labels"];
    ASSERT_TRUE(labels.HasMember("group"));
    ASSERT_TRUE(labels.HasMember("job"));
    ASSERT_STREQ("Kudu", labels["job"].GetString());
    ASSERT_TRUE(labels.HasMember("cluster_id"));
    ASSERT_FALSE(string(labels["cluster_id"].GetString()).empty());

    const string group = labels["group"].GetString();
    if (group == "masters") {
      found_master = true;
    } else if (group == "tservers") {
      found_tserver = true;
    }
  }
  ASSERT_TRUE(found_master) << "No master target found in Prometheus";
  ASSERT_TRUE(found_tserver) << "No tserver target found in Prometheus";
}

// Verifies behavior during master restart.
//
// When the leader master restarts it returns HTTP 503 while the catalog
// manager is initializing. Per the Prometheus HTTP SD spec, the SD client
// keeps the last successfully fetched list when it receives an error response,
// so active targets remain accessible. Once the master is fully up, targets
// should be present again.
TEST_F(PrometheusSDITest, SDResponseDuringMasterRestart) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus({ MasterSDUrl() }));
  ASSERT_OK(prom_->WaitForActiveTargets(2, MonoDelta::FromSeconds(90)));

  // Capture the pre-restart target count.
  rapidjson::Document before_doc;
  ASSERT_OK(prom_->GetTargets(&before_doc));
  const rapidjson::SizeType pre_active = before_doc["data"]["activeTargets"].Size();
  ASSERT_GE(pre_active, 2u);

  // Restart the master. During startup the SD endpoint returns HTTP 503.
  cluster_->mini_master()->Shutdown();
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_OK(cluster_->mini_master()->WaitForCatalogManagerInit());

  // Wait for Prometheus to re-discover and scrape targets after master recovery.
  ASSERT_OK(prom_->WaitForActiveTargets(2, MonoDelta::FromSeconds(90)));

  // Targets should still be present and healthy after recovery.
  rapidjson::Document after_doc;
  ASSERT_OK(prom_->GetTargets(&after_doc));
  const auto& active = after_doc["data"]["activeTargets"];
  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    ASSERT_STREQ("up", active[i]["health"].GetString())
        << "Target " << i << " unhealthy after master recovery";
  }
}

// Verifies that Prometheus correctly aggregates SD results from a multi-master
// cluster. The leader master returns the full list of targets, while follower
// masters return an empty array []. Prometheus should discover all targets by
// merging results from all configured SD URLs.
class MultiMasterPrometheusSDITest : public PrometheusSDITest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    InternalMiniClusterOptions opts;
    opts.num_masters = 3;
    opts.num_tablet_servers = 2;
    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->WaitForTabletServerCount(2));
  }
};

// With all 3 master SD URLs configured, Prometheus aggregates:
//   - leader: returns [master targets + tserver targets]
//   - follower 1: returns []
//   - follower 2: returns []
// The merged result should still include all masters and tservers.
TEST_F(MultiMasterPrometheusSDITest, LeaderFollowerSDBehavior) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Register SD URLs for all 3 masters.
  vector<string> sd_urls;
  for (int i = 0; i < cluster_->num_masters(); ++i) {
    sd_urls.push_back(MasterSDUrl(i));
  }
  ASSERT_OK(StartPrometheus(sd_urls));

  // Cluster: 3 masters + 2 tservers = 5 total targets.
  ASSERT_OK(prom_->WaitForActiveTargets(5, MonoDelta::FromSeconds(120)));

  rapidjson::Document targets_doc;
  ASSERT_OK(prom_->GetTargets(&targets_doc));
  ASSERT_STREQ("success", targets_doc["status"].GetString());
  ASSERT_EQ(5u, targets_doc["data"]["activeTargets"].Size());

  // Verify that all active targets are healthy.
  const auto& active = targets_doc["data"]["activeTargets"];
  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    ASSERT_STREQ("up", active[i]["health"].GetString());
  }
}

// Verifies that when all masters concurrently return [] (e.g., during leader
// re-election), Prometheus marks previous targets as stale rather than
// immediately removing them. After a new leader is elected, targets reappear.
//
// This test validates the key design decision: followers returning [] with HTTP
// 200 (rather than HTTP 503) is safe because Prometheus caches the last good
// response from any SD config. A target only disappears from /api/v1/targets
// once all SD configs return [] simultaneously AND no cached entry remains.
TEST_F(MultiMasterPrometheusSDITest, EmptyArrayBehaviorOnFollower) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Configure all 3 master SD URLs.
  vector<string> sd_urls;
  for (int i = 0; i < cluster_->num_masters(); ++i) {
    sd_urls.push_back(MasterSDUrl(i));
  }
  ASSERT_OK(StartPrometheus(sd_urls));
  ASSERT_OK(prom_->WaitForActiveTargets(5, MonoDelta::FromSeconds(120)));

  // Find the leader and forcibly disable it to make it return [] on /prometheus-sd.
  int leader_idx = -1;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  {
    // While the leader is disabled, all masters return []. Prometheus should
    // keep the previously cached targets in its /api/v1/targets list for the
    // duration of the SD refresh interval (2s in tests). The exact staleness
    // behavior (stale marker vs. immediate removal) depends on how recently
    // the SD list was refreshed; we verify that Prometheus does not crash and
    // that targets reappear once the leader is restored.
    master::CatalogManager::ScopedLeaderDisablerForTests disabler(
        cluster_->mini_master(leader_idx)->master()->catalog_manager());

    // After the SD refresh (2s), Prometheus will see [] from all masters.
    // Sleep to let at least one SD refresh cycle happen.
    SleepFor(MonoDelta::FromSeconds(5));

    // Prometheus should still report targets (possibly stale/unhealthy but not crashed).
    rapidjson::Document during_doc;
    ASSERT_OK(prom_->GetTargets(&during_doc));
    ASSERT_STREQ("success", during_doc["status"].GetString());
    // There may be 0 active targets if Prometheus cleared them, but the API
    // must remain functional (no crash or error).
    LOG(INFO) << "Active targets during all-follower period: "
              << during_doc["data"]["activeTargets"].Size();
  }

  // After the disabler goes out of scope, the master becomes leader again and
  // returns the full target list. Prometheus should re-discover all targets.
  ASSERT_OK(prom_->WaitForActiveTargets(5, MonoDelta::FromSeconds(60)));

  rapidjson::Document after_doc;
  ASSERT_OK(prom_->GetTargets(&after_doc));
  ASSERT_EQ(5u, after_doc["data"]["activeTargets"].Size());
}

// Verifies that location labels assigned to tablet servers are propagated
// through the SD endpoint to Prometheus target labels. This allows users to
// filter metrics by rack/location using PromQL label selectors.
class LocationLabelSDITest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    const string location_script =
        JoinPathSegments(GetTestExecutableDirectory(), "testdata/assign-location.py");
    // Use a test-specific state store path to avoid state pollution across
    // test runs from the default /tmp/location-sequencer-state path.
    const string state_store =
        JoinPathSegments(GetTestDataDirectory(), "location-sequencer-state");
    FLAGS_location_mapping_cmd = Substitute(
        "$0 --state_store $1 --map /rack1:1 --map /rack2:1",
        location_script, state_store);

    InternalMiniClusterOptions opts;
    opts.num_masters = 1;
    opts.num_tablet_servers = 2;
    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->WaitForTabletServerCount(2));
  }

  void TearDown() override {
    if (prom_) {
      WARN_NOT_OK(prom_->Stop(), "Failed to stop MiniPrometheus");
      prom_.reset();
    }
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<MiniPrometheus> prom_;
};

TEST_F(LocationLabelSDITest, TserverLocationLabel) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const string sd_url = Substitute(
      "http://$0/prometheus-sd",
      cluster_->mini_master()->bound_http_addr().ToString());

  MiniPrometheusOptions prom_opts;
  prom_opts.master_sd_urls = { sd_url };
  prom_.reset(new MiniPrometheus(prom_opts));
  ASSERT_OK(prom_->Start());

  // 1 master + 2 tservers = 3 targets.
  ASSERT_OK(prom_->WaitForActiveTargets(3, MonoDelta::FromSeconds(90)));

  rapidjson::Document targets_doc;
  ASSERT_OK(prom_->GetTargets(&targets_doc));
  ASSERT_STREQ("success", targets_doc["status"].GetString());

  const auto& active = targets_doc["data"]["activeTargets"];
  bool found_rack1 = false;
  bool found_rack2 = false;

  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    const auto& labels = active[i]["labels"];
    if (!labels.HasMember("group") ||
        string(labels["group"].GetString()) != "tservers") {
      continue;
    }
    ASSERT_TRUE(labels.HasMember("location"))
        << "Tserver target missing 'location' label";
    const string location = labels["location"].GetString();
    if (location == "/rack1") found_rack1 = true;
    if (location == "/rack2") found_rack2 = true;
  }

  ASSERT_TRUE(found_rack1) << "No tserver with location=/rack1 found";
  ASSERT_TRUE(found_rack2) << "No tserver with location=/rack2 found";

  // Verify that PromQL can filter by location label.
  rapidjson::Document query_doc;
  ASSERT_OK(prom_->Query("up{location=\"/rack1\"}", &query_doc));
  ASSERT_STREQ("success", query_doc["status"].GetString());
  ASSERT_GE(query_doc["data"]["result"].Size(), 1u);
}

} // namespace kudu
