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

#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class Subprocess;

// Basic wrapper around Prometheus binary for integration testing.
class MiniPrometheus {
 public:
  MiniPrometheus();
  ~MiniPrometheus();

  // Start the Prometheus server
  Status Start() WARN_UNUSED_RESULT;

  // Start the Prometheus server with static scrape targets
  Status Start(const std::vector<std::string>& target_urls) WARN_UNUSED_RESULT;

  // Stop the Prometheus server
  Status Stop() WARN_UNUSED_RESULT;

  // Check if Prometheus is running
  bool IsRunning() const;

  // Get the web port that Prometheus is listening on
  uint16_t web_port() const { return web_port_; }

  // Get the web URL for Prometheus
  std::string web_url() const;

  // Generate a static scrape configuration for the given target URLs
  static std::string GenerateScrapeConfig(const std::vector<std::string>& target_urls);

 private:
  // Find an available port for the Prometheus web interface
  Status FindAvailablePort();

  std::unique_ptr<Subprocess> process_;
  std::string config_file_path_;
  uint16_t web_port_;
  
  DISALLOW_COPY_AND_ASSIGN(MiniPrometheus);
};

} // namespace kudu 