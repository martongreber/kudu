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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <rapidjson/document.h>

#include "kudu/gutil/port.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class Subprocess;

// Configuration options for MiniPrometheus.
struct MiniPrometheusOptions {
  // List of Kudu master HTTP SD URLs. Each entry becomes one http_sd_configs
  // entry in the generated prometheus.yml, e.g.:
  //   "http://127.0.0.1:8765/prometheus-sd"
  // In a multi-master setup, list all master SD URLs so Prometheus aggregates
  // results from all of them; the leader returns targets and followers return [].
  std::vector<std::string> master_sd_urls;

  // List of static scrape targets in "host:port" form, e.g. "127.0.0.1:8765".
  // Each entry becomes a target under static_configs in the generated
  // prometheus.yml. Use this instead of (or in addition to) master_sd_urls
  // when you want to scrape a fixed set of endpoints without HTTP SD.
  std::vector<std::string> static_targets;

  // HTTP path used to scrape metrics on discovered targets.
  std::string metrics_path = "/metrics_prometheus";

  // How frequently Prometheus scrapes discovered targets.
  std::string scrape_interval = "1s";

  // How frequently Prometheus refreshes the HTTP SD target lists.
  std::string sd_refresh_interval = "1s";
};

// A wrapper around a Prometheus server subprocess for use in integration tests.
//
// MiniPrometheus starts a Prometheus process with a generated prometheus.yml
// that configures HTTP service discovery against Kudu master(s). It exposes a
// minimal HTTP API surface needed to verify that service discovery and metric
// scraping work correctly end-to-end.
//
// The Prometheus binary is expected to be installed at the path discovered by
// FindHomeDir("prometheus", bin_dir, ...) — i.e. a "prometheus-home" symlink
// in the test binary directory pointing to the thirdparty Prometheus directory.
//
// Typical usage:
//   MiniPrometheusOptions opts;
//   opts.master_sd_urls = { "http://127.0.0.1:8765/prometheus-sd" };
//   MiniPrometheus prom(opts);
//   ASSERT_OK(prom.Start());
//   ASSERT_OK(prom.WaitForActiveTargets(2, MonoDelta::FromSeconds(60)));
//   rapidjson::Document doc;
//   ASSERT_OK(prom.GetTargets(&doc));
//   ASSERT_OK(prom.Stop());
class MiniPrometheus {
 public:
  explicit MiniPrometheus(MiniPrometheusOptions options);
  ~MiniPrometheus();

  // Starts the Prometheus subprocess. Writes a prometheus.yml config file into
  // a temporary directory, then spawns Prometheus. Blocks until Prometheus is
  // listening on its HTTP port (up to 60 seconds).
  Status Start() WARN_UNUSED_RESULT;

  // Stops the Prometheus subprocess.
  Status Stop() WARN_UNUSED_RESULT;

  // Queries the Prometheus HTTP API at /api/v1/targets and populates 'doc'
  // with the parsed JSON response. The caller can inspect doc["data"]["activeTargets"]
  // and doc["data"]["droppedTargets"] to verify discovery results.
  Status GetTargets(rapidjson::Document* doc) WARN_UNUSED_RESULT;

  // Blocks until Prometheus has at least 'count' active (health=up) targets,
  // polling every second. Returns a timeout Status if 'timeout' expires.
  Status WaitForActiveTargets(int count, MonoDelta timeout) WARN_UNUSED_RESULT;

  // Issues a PromQL instant query via /api/v1/query and populates 'doc' with
  // the parsed JSON response (envelope with "status", "data.resultType",
  // "data.result").
  Status Query(const std::string& promql,
               rapidjson::Document* doc) WARN_UNUSED_RESULT;

  // Returns the TCP port Prometheus is listening on. Only valid after Start().
  uint16_t port() const { return port_; }

  // Returns the base URL for this Prometheus instance, e.g. "http://127.0.0.1:<port>".
  std::string url() const;

 private:
  // Writes the prometheus.yml config file into data_root_.
  Status WriteConfig() const WARN_UNUSED_RESULT;

  // Fetches the given path (relative to the Prometheus base URL) and parses
  // the JSON response body into 'doc'.
  Status FetchJson(const std::string& path,
                   rapidjson::Document* doc) WARN_UNUSED_RESULT;

  const MiniPrometheusOptions options_;

  std::string data_root_;
  std::string prometheus_home_;

  std::unique_ptr<Subprocess> process_;

  uint16_t port_ = 0;
};

} // namespace kudu
