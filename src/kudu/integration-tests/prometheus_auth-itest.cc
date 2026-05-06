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

#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/util/mini_prometheus.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(webserver_prometheus_token);

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

using cluster::InternalMiniCluster;
using cluster::InternalMiniClusterOptions;

constexpr const char* kToken = "test-prometheus-bearer-token";
constexpr int kScrapeIntervalSecs = 2;
const MonoDelta kScrapeTimeout = MonoDelta::FromSeconds(120);

vector<string> StaticTargets(const InternalMiniCluster& cluster) {
  vector<string> targets;
  for (int i = 0; i < cluster.num_masters(); ++i) {
    targets.push_back(cluster.mini_master(i)->bound_http_addr().ToString());
  }
  for (int i = 0; i < cluster.num_tablet_servers(); ++i) {
    targets.push_back(cluster.mini_tablet_server(i)->bound_http_addr().ToString());
  }
  return targets;
}

Status StartPrometheus(const vector<string>& targets,
                       const string& bearer_token,
                       unique_ptr<MiniPrometheus>* prom) {
  MiniPrometheusOptions opts;
  opts.static_targets = targets;
  opts.bearer_token = bearer_token;
  opts.scrape_interval = Substitute("$0s", kScrapeIntervalSecs);
  prom->reset(new MiniPrometheus(opts));
  return (*prom)->Start();
}


class PrometheusAuthITest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    // Configure all webservers in the cluster to require a bearer token for
    // the Prometheus endpoints.
    FLAGS_webserver_prometheus_token = kToken;

    InternalMiniClusterOptions opts;
    opts.num_masters = 3;
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
  int TotalTargets() const {
    return cluster_->num_masters() + cluster_->num_tablet_servers();
  }

  // Asserts that exactly TotalTargets() active targets exist and all report
  // the given 'health' state ("up" or "down").
  void AssertAllTargetsHealth(const string& health) {
    rapidjson::Document doc;
    ASSERT_OK(prom_->GetTargets(&doc));
    const auto& active = doc["data"]["activeTargets"];
    ASSERT_EQ(static_cast<rapidjson::SizeType>(TotalTargets()), active.Size());
    for (rapidjson::SizeType i = 0; i < active.Size(); ++i) {
      ASSERT_STREQ(health.c_str(), active[i]["health"].GetString())
          << "Target " << i << " has unexpected health: "
          << active[i]["scrapeUrl"].GetString();
    }
  }

  unique_ptr<InternalMiniCluster> cluster_;
  unique_ptr<MiniPrometheus> prom_;
};

// When Prometheus is configured with the correct bearer token, all targets
// are scraped successfully.
TEST_F(PrometheusAuthITest, TestCorrectTokenScrapeSucceeds) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus(StaticTargets(*cluster_), kToken, &prom_));
  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kScrapeTimeout));
  NO_FATALS(AssertAllTargetsHealth("up"));
}

// When Prometheus is configured with the wrong bearer token, all targets
// report health="down" due to HTTP 401 responses from the Kudu webservers.
TEST_F(PrometheusAuthITest, TestWrongTokenScrapeFails) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus(StaticTargets(*cluster_), "wrong-token", &prom_));
  ASSERT_OK(prom_->WaitForTargetHealth(TotalTargets(), "down", kScrapeTimeout));
  NO_FATALS(AssertAllTargetsHealth("down"));
}

// When Prometheus has no bearer token in its scrape config, all targets
// report health="down" because the Kudu webservers reject unauthenticated
// requests to the Prometheus endpoints with HTTP 401.
TEST_F(PrometheusAuthITest, TestMissingTokenScrapeFails) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheus(StaticTargets(*cluster_), /*bearer_token=*/"", &prom_));
  ASSERT_OK(prom_->WaitForTargetHealth(TotalTargets(), "down", kScrapeTimeout));
  NO_FATALS(AssertAllTargetsHealth("down"));
}

// Verifies that when both the SD endpoint and the scrape endpoint require a
// bearer token, Prometheus can discover and scrape targets successfully when
// the correct token is supplied for both. Unlike PrometheusAuthITest (which
// uses static targets to keep scrape auth isolated), this fixture exercises
// the full SD -> scrape path with auth end-to-end.
class PrometheusAuthSdITest : public PrometheusAuthITest {
 protected:
  vector<string> AllMasterSDUrls() const {
    vector<string> urls;
    for (int i = 0; i < cluster_->num_masters(); ++i) {
      urls.push_back(Substitute("http://$0/prometheus-sd",
                                cluster_->mini_master(i)->bound_http_addr().ToString()));
    }
    return urls;
  }

  Status StartPrometheusViaSd(const string& bearer_token) {
    MiniPrometheusOptions opts;
    opts.master_sd_urls = AllMasterSDUrls();
    opts.bearer_token = bearer_token;
    opts.scrape_interval = Substitute("$0s", kScrapeIntervalSecs);
    opts.sd_refresh_interval = Substitute("$0s", kScrapeIntervalSecs);
    prom_.reset(new MiniPrometheus(opts));
    return prom_->Start();
  }
};

// With the correct bearer token configured for both SD and scrape, Prometheus
// discovers all targets via the master SD endpoint and scrapes them successfully.
TEST_F(PrometheusAuthSdITest, TestCorrectTokenSdAndScrapeSucceed) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ASSERT_OK(StartPrometheusViaSd(kToken));
  ASSERT_OK(prom_->WaitForActiveTargets(TotalTargets(), kScrapeTimeout));
  NO_FATALS(AssertAllTargetsHealth("up"));
}

// When the SD token is wrong or missing, Prometheus cannot authenticate against
// the /prometheus-sd endpoint (HTTP 401) and never discovers any targets.
// Unlike the scrape-auth negative tests (which use static targets), we assert
// that activeTargets remains empty after several SD refresh cycles — there are
// no targets to mark as "down" because discovery itself never succeeded.
void AssertNoTargetsDiscovered(MiniPrometheus* prom) {
  // Give Prometheus several refresh cycles to try (and fail) SD discovery,
  // then confirm no targets were discovered.
  SleepFor(MonoDelta::FromSeconds(kScrapeIntervalSecs * 3));
  rapidjson::Document doc;
  ASSERT_OK(prom->GetTargets(&doc));
  ASSERT_EQ(0, doc["data"]["activeTargets"].Size())
      << "Expected no active targets when SD authentication fails";
}

TEST_F(PrometheusAuthSdITest, TestWrongTokenSdFails) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ASSERT_OK(StartPrometheusViaSd("wrong-token"));
  NO_FATALS(AssertNoTargetsDiscovered(prom_.get()));
}

TEST_F(PrometheusAuthSdITest, TestMissingTokenSdFails) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ASSERT_OK(StartPrometheusViaSd(/*bearer_token=*/""));
  NO_FATALS(AssertNoTargetsDiscovered(prom_.get()));
}

} // namespace kudu
