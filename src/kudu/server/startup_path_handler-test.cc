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

#include "kudu/server/startup_path_handler.h"

#include <atomic>
#include <memory>

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/server/webserver.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/metrics.h"
#include "kudu/util/test_util.h"
#include "kudu/util/timer.h"
#include "kudu/util/web_callback_registry.h"

namespace kudu {
namespace server {

class StartupPathHandlerTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    metric_registry_.reset(new MetricRegistry());
    metric_entity_ =
        METRIC_ENTITY_server.Instantiate(metric_registry_.get(), "test");
    handler_.reset(new StartupPathHandler(metric_entity_));
    handler_->set_is_tablet_server(true);
    handler_->set_is_using_lbm(true);
  }

 protected:
  // Helper: trigger the handler and return the "read_filesystem_status" value
  // from the rendered JSON output.
  int GetReadFilesystemStatus() {
    Webserver::WebRequest req;
    Webserver::WebResponse resp;
    handler_->Startup(req, &resp);
    return resp.output.value()["read_filesystem_status"].GetInt();
  }

  // Helper: trigger the handler and return the "read_data_directories_status"
  // value from the rendered JSON output.
  int GetReadDataDirectoriesStatus() {
    Webserver::WebRequest req;
    Webserver::WebResponse resp;
    handler_->Startup(req, &resp);
    return resp.output.value()["read_data_directories_status"].GetInt();
  }

  std::unique_ptr<MetricRegistry> metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  std::unique_ptr<StartupPathHandler> handler_;
};

// Parent step Timer has not been started yet: progress should be 0%.
TEST_F(StartupPathHandlerTest, ReadFilesystemNotStarted) {
  ASSERT_EQ(0, GetReadFilesystemStatus());
}

// Parent step has been started but no sub-step has made progress: 0%.
TEST_F(StartupPathHandlerTest, ReadFilesystemStartedNoProgress) {
  handler_->read_filesystem_progress()->Start();
  ASSERT_EQ(0, GetReadFilesystemStatus());
}

// Parent step finished: unconditionally 100% regardless of sub-step state.
TEST_F(StartupPathHandlerTest, ReadFilesystemStopped) {
  handler_->read_filesystem_progress()->Start();
  handler_->read_filesystem_progress()->Stop();
  ASSERT_EQ(100, GetReadFilesystemStatus());
}

// While the parent step is running and data directories are being processed,
// the returned status should reflect the log-block-container progress and
// be strictly less than 100 (we do not let the parent reach 100 until the
// parent Timer itself is stopped).
TEST_F(StartupPathHandlerTest, ReadFilesystemShowsIntermediateProgressLbm) {
  handler_->read_filesystem_progress()->Start();
  handler_->read_instance_metadata_files_progress()->Start();
  handler_->read_instance_metadata_files_progress()->Stop();
  handler_->read_data_directories_progress()->Start();

  handler_->containers_total()->store(100);
  handler_->containers_processed()->store(50);

  const int status = GetReadFilesystemStatus();
  // Instance metadata is 5% of the aggregate and data directories 95%; with
  // containers at 50% we expect: 5 + 0.95 * 50 = 52 (integer division).
  ASSERT_EQ(52, status);
}

// The aggregate must never spuriously hit 100 while the parent Timer still
// runs, even if all sub-step counters are momentarily at their maximum.
TEST_F(StartupPathHandlerTest, ReadFilesystemClampedUnder100WhileRunning) {
  handler_->read_filesystem_progress()->Start();
  handler_->read_instance_metadata_files_progress()->Start();
  handler_->read_instance_metadata_files_progress()->Stop();
  handler_->read_data_directories_progress()->Start();

  // Over-report processed containers to simulate a race window.
  handler_->containers_total()->store(100);
  handler_->containers_processed()->store(150);

  ASSERT_EQ(99, GetReadFilesystemStatus());

  handler_->read_filesystem_progress()->Stop();
  ASSERT_EQ(100, GetReadFilesystemStatus());
}

// When using the (non-default) file block manager, the handler has no
// per-container signal; the parent step falls back to a coarse estimate based
// solely on the instance-metadata sub-step.
TEST_F(StartupPathHandlerTest, ReadFilesystemFileBlockManager) {
  handler_->set_is_using_lbm(false);
  handler_->read_filesystem_progress()->Start();
  ASSERT_EQ(0, GetReadFilesystemStatus());

  handler_->read_instance_metadata_files_progress()->Start();
  handler_->read_instance_metadata_files_progress()->Stop();
  handler_->read_data_directories_progress()->Start();
  // Only the 5% instance-metadata weight has elapsed.
  ASSERT_EQ(5, GetReadFilesystemStatus());

  handler_->read_data_directories_progress()->Stop();
  // Both sub-steps are stopped: aggregate is 100 but the parent Timer has
  // not been stopped yet, so clamp to 99.
  ASSERT_EQ(99, GetReadFilesystemStatus());

  handler_->read_filesystem_progress()->Stop();
  ASSERT_EQ(100, GetReadFilesystemStatus());
}

// The 'read_data_directories_status' percentage must be clamped to [0, 100]
// even if 'containers_processed' briefly races ahead of 'containers_total'
// while multiple data directories concurrently accumulate their counts. It
// must also avoid signed-integer overflow on the intermediate 'processed*100'
// multiplication for very large counters.
TEST_F(StartupPathHandlerTest, ReadDataDirectoriesStatusClamped) {
  // Race window: processed temporarily exceeds total.
  handler_->containers_total()->store(100);
  handler_->containers_processed()->store(150);
  ASSERT_EQ(100, GetReadDataDirectoriesStatus());

  // Very large counters: processed*100 would overflow int32; verify we do
  // not hit UB and still return a sane value in [0, 100].
  handler_->containers_total()->store(100000000);      // 1e8
  handler_->containers_processed()->store(50000000);   // 5e7
  ASSERT_EQ(50, GetReadDataDirectoriesStatus());
}

} // namespace server
} // namespace kudu
