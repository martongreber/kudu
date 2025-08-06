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

#include "kudu/util/mini_prometheus.h"

#include <gtest/gtest.h>

#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

class MiniPrometheusTest : public KuduTest {};

TEST_F(MiniPrometheusTest, TestBasicStartStop) {
  MiniPrometheus prometheus;

  // Test that we can start and stop without errors
  ASSERT_OK(prometheus.Start());
  ASSERT_TRUE(prometheus.IsRunning());

  ASSERT_OK(prometheus.Stop());
  ASSERT_FALSE(prometheus.IsRunning());
}

TEST_F(MiniPrometheusTest, TestScrapeConfigGeneration) {
  // Test the static scrape config generation
  std::vector<std::string> targets = {
    "localhost:8080",
    "localhost:8081",
    "192.168.1.100:9090"
  };
  
  std::string config = MiniPrometheus::GenerateScrapeConfig(targets);
  
  // Verify the config contains our targets
  ASSERT_STR_CONTAINS(config, "localhost:8080");
  ASSERT_STR_CONTAINS(config, "localhost:8081");
  ASSERT_STR_CONTAINS(config, "192.168.1.100:9090");
  ASSERT_STR_CONTAINS(config, "job_name: 'kudu-test'");
  ASSERT_STR_CONTAINS(config, "scrape_interval: 15s");
}

TEST_F(MiniPrometheusTest, TestStartWithScrapeTargets) {
  MiniPrometheus prometheus;
  
  std::vector<std::string> targets = {
    "localhost:8080"
  };

  // Test that we can start with scrape targets
  ASSERT_OK(prometheus.Start(targets));
  ASSERT_TRUE(prometheus.IsRunning());

  ASSERT_OK(prometheus.Stop());
  ASSERT_FALSE(prometheus.IsRunning());
}

} // namespace kudu 