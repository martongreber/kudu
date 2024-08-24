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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/util/mini_prometheus.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
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

// Scrape and SD-refresh interval used by all MiniPrometheus instances in this file.
constexpr int kScrapeIntervalSecs = 2;

// Number of scrape cycles to poll when checking that a target is active.
// 5 cycles gives enough slack for scrape timing jitter on loaded machines.
constexpr int kActiveTargetPollCycles = 5;

// Range window (in scrape cycles) used for min_over_time PromQL checks.
// 10 cycles is large enough to catch transient scrape failures while keeping
// the window short enough that the query returns quickly after recovery.
constexpr int kMinOverTimeWindowCycles = 10;

// Timeout for waiting for Prometheus to discover and scrape targets in a
// single-master cluster.
const MonoDelta kTargetWaitTimeout = MonoDelta::FromSeconds(90);

// Longer timeout for multi-master clusters where SD aggregation across 3
// masters and scraping more targets takes more time.
const MonoDelta kMultiMasterTargetWaitTimeout = MonoDelta::FromSeconds(120);

// Base class for Prometheus SD integration tests. The default SetUp creates a
// single-master, single-tserver cluster; subclasses override SetUp to change
// the topology while inheriting TearDown, MasterSDUrl, and StartPrometheus.
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
  string MasterSDUrl(int idx = 0) const {
    return Substitute("http://$0/prometheus-sd",
                      cluster_->mini_master(idx)->bound_http_addr().ToString());
  }

  int TotalTargets() const {
    return cluster_->num_masters() + cluster_->num_tablet_servers();
  }

  Status StartPrometheus(const vector<string>& sd_urls) {
    MiniPrometheusOptions prom_opts;
    prom_opts.master_sd_urls = sd_urls;
    prom_opts.scrape_interval = Substitute("$0s", kScrapeIntervalSecs);
    prom_opts.sd_refresh_interval = Substitute("$0s", kScrapeIntervalSecs);
    prom_.reset(new MiniPrometheus(prom_opts));
    return prom_->Start();
  }

  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<MiniPrometheus> prom_;
};

// Verifies the end-to-end SD-to-scrape path: SD discovery produces the
// expected number of active targets, all report health="up", and PromQL
// returns one result per target.
TEST_F(PrometheusSDITest, BasicSDAndScrape) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus({ MasterSDUrl() }));

  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kTargetWaitTimeout));

  rapidjson::Document targets_doc;
  ASSERT_OK(prom_->GetTargets(&targets_doc));
  ASSERT_STREQ("success", targets_doc["status"].GetString());

  const auto& active = targets_doc["data"]["activeTargets"];
  ASSERT_EQ(static_cast<rapidjson::SizeType>(TotalTargets()), active.Size());

  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    ASSERT_STREQ("up", active[i]["health"].GetString())
        << "Target " << i << " not healthy: "
        << active[i]["scrapeUrl"].GetString();
  }

  rapidjson::Document query_doc;
  ASSERT_OK(prom_->Query("up", &query_doc));
  ASSERT_STREQ("success", query_doc["status"].GetString());
  ASSERT_EQ(static_cast<rapidjson::SizeType>(TotalTargets()),
            query_doc["data"]["result"].Size());
}

// Verifies the 'group' label on discovered targets.
// Masters should have group="masters" and tservers group="tservers".
TEST_F(PrometheusSDITest, SDLabels) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus({ MasterSDUrl() }));
  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kTargetWaitTimeout));

  rapidjson::Document targets_doc;
  ASSERT_OK(prom_->GetTargets(&targets_doc));
  const auto& active = targets_doc["data"]["activeTargets"];

  bool found_master = false;
  bool found_tserver = false;
  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    const auto& labels = active[i]["labels"];
    ASSERT_TRUE(labels.HasMember("group"));
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

// While the master is down, the SD endpoint is unreachable (connection
// refused). Per the Prometheus HTTP SD spec, any SD error causes Prometheus to
// retain the last target list, so tserver scraping continues uninterrupted.
TEST_F(PrometheusSDITest, TserverMetricContinuityDuringMasterRestart) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus({ MasterSDUrl() }));
  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kTargetWaitTimeout));

  cluster_->mini_master()->Shutdown();

  // Core assertion: while the master is down the tserver target must remain
  // active in Prometheus. Per the HTTP SD spec, any SD error causes Prometheus
  // to retain its cached target list rather than clearing it.
  AssertEventually([&]() {
    rapidjson::Document doc;
    ASSERT_OK(prom_->GetTargets(&doc));
    bool found = false;
    const auto& active = doc["data"]["activeTargets"];
    for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
      if (!active[i]["labels"].HasMember("group")) continue;
      if (string(active[i]["labels"]["group"].GetString()) != "tservers") continue;
      if (string(active[i]["health"].GetString()) == "up") {
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found) << "Tserver target was removed from Prometheus during master restart";
  }, MonoDelta::FromSeconds(kActiveTargetPollCycles * kScrapeIntervalSecs));
  NO_PENDING_FATALS();

  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_OK(cluster_->mini_master()->WaitForCatalogManagerInit());
  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kTargetWaitTimeout));

  // After recovery verify that tserver scrape health never dropped to 0 in
  // the most recent kMinOverTimeWindowCycles-cycle window (up=1 at every
  // sample means no scrape failure). Poll until the window is fully populated,
  // allowing up to kActiveTargetPollCycles extra cycles of slack.
  AssertEventually([&]() {
    rapidjson::Document doc;
    ASSERT_OK(prom_->Query(
        Substitute("min_over_time(up{group=\"tservers\"}[$0s])",
                   kMinOverTimeWindowCycles * kScrapeIntervalSecs),
        &doc));
    ASSERT_STREQ("success", doc["status"].GetString());
    const auto& results = doc["data"]["result"];
    ASSERT_GE(results.Size(), 1u) << "No tserver data in window yet";
    const double min_val = std::stod(results[0]["value"][1].GetString());
    ASSERT_EQ(1.0, min_val) << "Tserver scrape health dropped to 0 after master recovery";
  }, MonoDelta::FromSeconds(
      (kMinOverTimeWindowCycles + kActiveTargetPollCycles) * kScrapeIntervalSecs));
  NO_PENDING_FATALS();
}

class MultiMasterPrometheusSDITest : public PrometheusSDITest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    InternalMiniClusterOptions opts;
    opts.num_masters = 3;
    opts.num_tablet_servers = 1;
    cluster_.reset(new InternalMiniCluster(env_, opts));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->WaitForTabletServerCount(1));
  }

 protected:
  Status StartPrometheusWithAllMasters() {
    vector<string> sd_urls;
    for (int i = 0; i < cluster_->num_masters(); ++i) {
      sd_urls.push_back(MasterSDUrl(i));
    }
    return StartPrometheus(sd_urls);
  }
};

// With all 3 master SD URLs configured, Prometheus aggregates:
//   - leader: returns [master targets + tserver targets]
//   - follower 1: returns []
//   - follower 2: returns []
// The merged result should still include all masters and tservers.
TEST_F(MultiMasterPrometheusSDITest, LeaderFollowerSDBehavior) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheusWithAllMasters());

  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kMultiMasterTargetWaitTimeout));

  rapidjson::Document targets_doc;
  ASSERT_OK(prom_->GetTargets(&targets_doc));
  ASSERT_STREQ("success", targets_doc["status"].GetString());
  ASSERT_EQ(static_cast<rapidjson::SizeType>(TotalTargets()),
            targets_doc["data"]["activeTargets"].Size());

  const auto& active = targets_doc["data"]["activeTargets"];
  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    ASSERT_STREQ("up", active[i]["health"].GetString());
  }
}

// Verifies that when all masters concurrently return [] (e.g., during leader
// re-election), Prometheus temporarily loses the targets (since 200+[] is a
// valid empty response, not an error - unlike HTTP 503 which retains the cache).
// After a new leader is elected, targets reappear and all previously collected
// samples remain intact in the TSDB — the gap is a write gap, not a deletion.
TEST_F(MultiMasterPrometheusSDITest, EmptyArrayBehaviorOnFollower) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheusWithAllMasters());
  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kMultiMasterTargetWaitTimeout));

  // Wait for Prometheus to write at least one healthy (up=1) tserver sample,
  // then derive pre_gap_time from that sample's own timestamp.
  double pre_gap_time = 0.0;
  AssertEventually([&]() {
    rapidjson::Document doc;
    ASSERT_OK(prom_->Query("up{group=\"tservers\"}", &doc));
    ASSERT_STREQ("success", doc["status"].GetString());
    const auto& results = doc["data"]["result"];
    ASSERT_EQ(1u, results.Size())
        << "Prometheus has not yet written a tserver sample";
    // value[0] is the Unix timestamp as a JSON float (e.g. 1745345123.456);
    // value[1] is the metric value string (e.g. "1").
    ASSERT_STREQ("1", results[0]["value"][1].GetString())
        << "Most recent tserver sample is not yet up=1; retrying";
    pre_gap_time = results[0]["value"][0].GetDouble();
  }, MonoDelta::FromSeconds(kActiveTargetPollCycles * kScrapeIntervalSecs));
  NO_PENDING_FATALS();

  int leader_idx = -1;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_idx));
  {
    // While the leader is disabled all masters return HTTP 200 + []. Unlike
    // HTTP 503, Prometheus treats this as a valid empty response, so active
    // targets may drop to 0 after one SD refresh cycle. That is expected and
    // accepted: we verify the API stays functional (no crash), not continuity.
    master::CatalogManager::ScopedLeaderDisablerForTests disabler(
        cluster_->mini_master(leader_idx)->master()->catalog_manager());

    SleepFor(MonoDelta::FromSeconds(5));

    rapidjson::Document during_doc;
    ASSERT_OK(prom_->GetTargets(&during_doc));
    ASSERT_STREQ("success", during_doc["status"].GetString());
  }

  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), MonoDelta::FromSeconds(60)));

  rapidjson::Document after_doc;
  ASSERT_OK(prom_->GetTargets(&after_doc));
  const auto& active = after_doc["data"]["activeTargets"];
  ASSERT_EQ(static_cast<rapidjson::SizeType>(TotalTargets()), active.Size());
  for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
    ASSERT_STREQ("up", active[i]["health"].GetString())
        << "Target " << i << " not healthy after leader recovery";
  }

  // Verify that historical data collected before the gap was not erased from
  // the TSDB. The PromQL '@' modifier queries as-of a past timestamp; if
  // Prometheus had deleted samples the result set would be empty.
  AssertEventually([&]() {
    rapidjson::Document doc;
    ASSERT_OK(prom_->Query(
        Substitute("up{group=\"tservers\"} @ $0", pre_gap_time),
        &doc));
    ASSERT_STREQ("success", doc["status"].GetString());
    const auto& results = doc["data"]["result"];
    ASSERT_EQ(1u, results.Size())
        << "Historical tserver data missing after gap — TSDB was unexpectedly cleared";
    ASSERT_STREQ("1", results[0]["value"][1].GetString())
        << "Historical tserver sample does not show up=1";
  }, MonoDelta::FromSeconds(kActiveTargetPollCycles * kScrapeIntervalSecs));
  NO_PENDING_FATALS();
}

// Verifies that location labels assigned to tablet servers are propagated
// through the SD endpoint to Prometheus target labels. This allows users to
// filter metrics by rack/location using PromQL label selectors.
class LocationLabelSDITest : public PrometheusSDITest {
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
};

TEST_F(LocationLabelSDITest, TserverLocationLabel) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus({ MasterSDUrl() }));

  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kTargetWaitTimeout));

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

  rapidjson::Document query_doc;
  ASSERT_OK(prom_->Query("up{location=\"/rack1\"}", &query_doc));
  ASSERT_STREQ("success", query_doc["status"].GetString());
  ASSERT_GE(query_doc["data"]["result"].Size(), 1u);
}

} // namespace kudu
