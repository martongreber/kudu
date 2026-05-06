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
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/mini_prometheus.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

using cluster::ExternalMiniCluster;
using cluster::ExternalMiniClusterOptions;

constexpr const char* kToken = "test-prometheus-bearer-token";
constexpr int kScrapeIntervalSecs = 2;
const MonoDelta kScrapeTimeout = MonoDelta::FromSeconds(120);

// Base fixture for Prometheus authentication integration tests.
// Starts an ExternalMiniCluster with Kerberos enabled (which automatically
// sets --webserver_require_spnego=true on every daemon) and a TLS-enabled
// webserver. No Prometheus bearer token is configured by default.
class PrometheusSpnegoITest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    // Generate one self-signed TLS cert shared by all daemons.
    // Prometheus is configured with insecure_skip_verify=true because the
    // ExternalMiniCluster binds each daemon to a dynamically-chosen loopback
    // alias (e.g. 127.x.x.x) that does not match the SAN in the test cert,
    // making hostname-based validation impossible. Encryption is still active;
    // only the certificate identity check is skipped, mirroring the pattern
    // used by webserver-crawl-itest.cc (curl.set_verify_peer(false)).
    string cert_file, pk_file, pw;
    ASSERT_OK(security::CreateTestSSLCertWithEncryptedKey(
        GetTestDataDirectory(), &cert_file, &pk_file, &pw));

    ExternalMiniClusterOptions opts;
    opts.enable_kerberos = true;
    opts.num_masters = 3;
    opts.num_tablet_servers = 1;

    vector<string> extra_flags = {
        Substitute("--webserver_certificate_file=$0", cert_file),
        Substitute("--webserver_private_key_file=$0", pk_file),
        Substitute("--webserver_private_key_password_cmd=echo $0", pw),
    };
    AddExtraFlags(&extra_flags);
    opts.extra_master_flags  = extra_flags;
    opts.extra_tserver_flags = extra_flags;

    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
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
  virtual void AddExtraFlags(vector<string>* /*flags*/) {}

  int TotalDaemons() const {
    return cluster_->num_masters() + cluster_->num_tablet_servers();
  }

  vector<string> StaticTargets() const {
    vector<string> targets;
    for (int i = 0; i < cluster_->num_masters(); ++i) {
      targets.push_back(cluster_->master(i)->bound_http_hostport().ToString());
    }
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      targets.push_back(cluster_->tablet_server(i)->bound_http_hostport().ToString());
    }
    return targets;
  }

  vector<string> MasterSDUrls() const {
    vector<string> urls;
    for (int i = 0; i < cluster_->num_masters(); ++i) {
      urls.push_back(Substitute("https://$0/prometheus-sd",
                                cluster_->master(i)->bound_http_hostport().ToString()));
    }
    return urls;
  }

  MiniPrometheusOptions BasePromOpts() const {
    MiniPrometheusOptions opts;
    opts.scheme = "https";
    opts.skip_tls_verify = true;
    opts.scrape_interval = Substitute("$0s", kScrapeIntervalSecs);
    opts.sd_refresh_interval = Substitute("$0s", kScrapeIntervalSecs);
    return opts;
  }

  // Waits several SD refresh cycles then asserts no active targets exist.
  void AssertNoTargetsDiscovered() {
    SleepFor(MonoDelta::FromSeconds(kScrapeIntervalSecs * 3));
    rapidjson::Document doc;
    ASSERT_OK(prom_->GetTargets(&doc));
    ASSERT_EQ(0, doc["data"]["activeTargets"].Size())
        << "Expected no active targets when SD authentication fails";
  }

  unique_ptr<ExternalMiniCluster> cluster_;
  unique_ptr<MiniPrometheus> prom_;
};

// With SPNEGO required cluster-wide but no bearer token configured, Prometheus
// cannot authenticate against the scrape endpoint (gets 401 Negotiate) and all
// static targets report health="down".
TEST_F(PrometheusSpnegoITest, TestScrapeFailsWithSpnegoOnly) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  MiniPrometheusOptions opts = BasePromOpts();
  opts.static_targets = StaticTargets();
  prom_.reset(new MiniPrometheus(opts));
  ASSERT_OK(prom_->Start());

  ASSERT_OK(prom_->WaitForTargetHealth(TotalDaemons(), "down", kScrapeTimeout));
}

// With SPNEGO required cluster-wide but no bearer token configured, Prometheus
// cannot authenticate against the SD endpoint (gets 401 Negotiate) and
// discovers no targets at all.
TEST_F(PrometheusSpnegoITest, TestSdDiscoveryFailsWithSpnegoOnly) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  MiniPrometheusOptions opts = BasePromOpts();
  opts.master_sd_urls = MasterSDUrls();
  prom_.reset(new MiniPrometheus(opts));
  ASSERT_OK(prom_->Start());

  NO_FATALS(AssertNoTargetsDiscovered());
}

// Extends PrometheusSpnegoITest by also configuring a Prometheus bearer token
// on every daemon.
class PrometheusTokenITest : public PrometheusSpnegoITest {
 protected:
  void AddExtraFlags(vector<string>* flags) override {
    flags->push_back(Substitute("--webserver_prometheus_token_cmd=echo $0", kToken));
  }
};

// When Prometheus is configured with the correct bearer token, all targets
// are scraped successfully despite SPNEGO being required cluster-wide.
TEST_F(PrometheusTokenITest, TestCorrectTokenScrapeSucceeds) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  MiniPrometheusOptions opts = BasePromOpts();
  opts.static_targets = StaticTargets();
  opts.bearer_token = kToken;
  prom_.reset(new MiniPrometheus(opts));
  ASSERT_OK(prom_->Start());

  ASSERT_OK(prom_->WaitForActiveTargets(TotalDaemons(), kScrapeTimeout));
}

// When Prometheus is configured with the wrong bearer token, all targets
// report health="down" due to HTTP 401 responses from the Kudu webservers.
TEST_F(PrometheusTokenITest, TestWrongTokenScrapeFails) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  MiniPrometheusOptions opts = BasePromOpts();
  opts.static_targets = StaticTargets();
  opts.bearer_token = "wrong-token";
  prom_.reset(new MiniPrometheus(opts));
  ASSERT_OK(prom_->Start());

  ASSERT_OK(prom_->WaitForTargetHealth(TotalDaemons(), "down", kScrapeTimeout));
}

// When Prometheus sends no bearer token, all targets report health="down"
// because the Kudu webservers reject the unauthenticated request with HTTP 401.
TEST_F(PrometheusTokenITest, TestMissingTokenScrapeFails) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  MiniPrometheusOptions opts = BasePromOpts();
  opts.static_targets = StaticTargets();
  // No bearer_token set.
  prom_.reset(new MiniPrometheus(opts));
  ASSERT_OK(prom_->Start());

  ASSERT_OK(prom_->WaitForTargetHealth(TotalDaemons(), "down", kScrapeTimeout));
}

// With the correct bearer token configured for both SD and scrape, Prometheus
// discovers all targets via the master SD endpoint and scrapes them
// successfully, exercising the full SD -> scrape path with auth end-to-end.
TEST_F(PrometheusTokenITest, TestCorrectTokenSdAndScrapeSucceed) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  MiniPrometheusOptions opts = BasePromOpts();
  opts.master_sd_urls = MasterSDUrls();
  opts.bearer_token = kToken;
  prom_.reset(new MiniPrometheus(opts));
  ASSERT_OK(prom_->Start());

  ASSERT_OK(prom_->WaitForActiveTargets(TotalDaemons(), kScrapeTimeout));
}

// When the SD token is wrong, Prometheus cannot authenticate against the
// /prometheus-sd endpoint (HTTP 401) and never discovers any targets.
TEST_F(PrometheusTokenITest, TestWrongTokenSdFails) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  MiniPrometheusOptions opts = BasePromOpts();
  opts.master_sd_urls = MasterSDUrls();
  opts.bearer_token = "wrong-token";
  prom_.reset(new MiniPrometheus(opts));
  ASSERT_OK(prom_->Start());

  NO_FATALS(AssertNoTargetsDiscovered());
}

// When no SD token is supplied, Prometheus cannot authenticate against the
// /prometheus-sd endpoint (HTTP 401) and never discovers any targets.
TEST_F(PrometheusTokenITest, TestMissingTokenSdFails) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  MiniPrometheusOptions opts = BasePromOpts();
  opts.master_sd_urls = MasterSDUrls();
  // No bearer_token set.
  prom_.reset(new MiniPrometheus(opts));
  ASSERT_OK(prom_->Start());

  NO_FATALS(AssertNoTargetsDiscovered());
}

} // namespace kudu
