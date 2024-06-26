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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h" // IWYU pragma: keep
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

DEFINE_int32(client_threads, 16,
             "Number of client threads. For the synchronous benchmark, each thread has "
             "a single outstanding synchronous request at a time. For the async "
             "benchmark, this determines the number of client reactors.");

DEFINE_int32(async_call_concurrency, 60,
             "Number of concurrent requests that will be outstanding at a time for the "
             "async benchmark. The requests are multiplexed across the number of "
             "reactors specified by the 'client_threads' flag.");

DEFINE_int32(worker_threads, 1,
             "Number of server worker threads");

DEFINE_int32(acceptor_threads, 1,
             "Number of threads in the messenger's acceptor pool");

DEFINE_int32(server_reactors, 4,
             "Number of server reactor threads");

DEFINE_bool(enable_encryption, false, "Whether to enable TLS encryption for rpc-bench");

DEFINE_int32(run_seconds, 1, "Seconds to run the test");

DECLARE_bool(rpc_encrypt_loopback_connections);
DECLARE_bool(rpc_suppress_negotiation_trace);

METRIC_DECLARE_histogram(acceptor_dispatch_times);
METRIC_DECLARE_histogram(reactor_load_percent);
METRIC_DECLARE_histogram(reactor_active_latency_us);

namespace kudu {
namespace rpc {

class RpcBench : public RpcTestBase {
 public:
  RpcBench()
      : should_run_(true),
        stop_(0) {
  }

  void SetUp() override {
    RpcTestBase::SetUp();
    OverrideFlagForSlowTests("run_seconds", "10");

    n_worker_threads_ = FLAGS_worker_threads;
    n_server_reactor_threads_ = FLAGS_server_reactors;

    // Set up server.
    FLAGS_rpc_encrypt_loopback_connections = FLAGS_enable_encryption;
    ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr_, FLAGS_enable_encryption));
  }

  void SummarizePerf(CpuTimes elapsed, int total_reqs, bool sync) {
    float reqs_per_second = static_cast<float>(total_reqs / elapsed.wall_seconds());
    float user_cpu_micros_per_req = static_cast<float>(elapsed.user / 1000.0 / total_reqs);
    float sys_cpu_micros_per_req = static_cast<float>(elapsed.system / 1000.0 / total_reqs);
    float csw_per_req = static_cast<float>(elapsed.context_switches) / total_reqs;

    HdrHistogram reactor_load(*METRIC_reactor_load_percent.Instantiate(
        server_messenger_->metric_entity())->histogram());
    HdrHistogram reactor_latency(*METRIC_reactor_active_latency_us.Instantiate(
        server_messenger_->metric_entity())->histogram());

    LOG(INFO) << "Mode:            " << (sync ? "Sync" : "Async");
    if (sync) {
      LOG(INFO) << "Client threads:   " << FLAGS_client_threads;
    } else {
      LOG(INFO) << "Client reactors:  " << FLAGS_client_threads;
      LOG(INFO) << "Call concurrency: " << FLAGS_async_call_concurrency;
    }

    LOG(INFO) << "Worker threads:   " << FLAGS_worker_threads;
    LOG(INFO) << "Server reactors:  " << FLAGS_server_reactors;
    LOG(INFO) << "Encryption:       " << FLAGS_enable_encryption;
    LOG(INFO) << "----------------------------------";
    LOG(INFO) << "Reqs/sec:         " << reqs_per_second;
    LOG(INFO) << "User CPU per req: " << user_cpu_micros_per_req << "us";
    LOG(INFO) << "Sys CPU per req:  " << sys_cpu_micros_per_req << "us";
    LOG(INFO) << "Ctx Sw. per req:  " << csw_per_req;
    LOG(INFO) << "Server reactor load histogram";
    reactor_load.DumpHumanReadable(&LOG(INFO));
    LOG(INFO) << "Server reactor latency histogram";
    reactor_latency.DumpHumanReadable(&LOG(INFO));
  }

 protected:
  friend class ClientThread;
  friend class ClientAsyncWorkload;

  Sockaddr server_addr_;
  atomic<bool> should_run_;
  CountDownLatch stop_;
};

class ClientThread {
 public:
  explicit ClientThread(RpcBench *bench)
    : bench_(bench),
      request_count_(0) {
  }

  void Start() {
    thread_.reset(new thread(&ClientThread::Run, this));
  }

  void Join() {
    thread_->join();
  }

  void Run() {
    shared_ptr<Messenger> client_messenger;
    CHECK_OK(bench_->CreateMessenger("Client", &client_messenger));

    CalculatorServiceProxy p(client_messenger, bench_->server_addr_, "localhost");

    AddRequestPB req;
    AddResponsePB resp;
    while (bench_->should_run_) {
      req.set_x(request_count_);
      req.set_y(request_count_);
      RpcController controller;
      controller.set_timeout(MonoDelta::FromSeconds(10));
      CHECK_OK(p.Add(req, &resp, &controller));
      CHECK_EQ(req.x() + req.y(), resp.result());
      request_count_++;
    }
  }

  unique_ptr<thread> thread_;
  RpcBench *bench_;
  int request_count_;
};


// Test making successful RPC calls.
TEST_F(RpcBench, BenchmarkCalls) {
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();

  vector<unique_ptr<ClientThread>> threads;
  for (int i = 0; i < FLAGS_client_threads; i++) {
    threads.emplace_back(new ClientThread(this));
    threads.back()->Start();
  }

  SleepFor(MonoDelta::FromSeconds(FLAGS_run_seconds));
  should_run_ = false;

  int total_reqs = 0;

  for (auto& thr : threads) {
    thr->Join();
    total_reqs += thr->request_count_;
  }
  sw.stop();

  SummarizePerf(sw.elapsed(), total_reqs, true);
}

class ClientAsyncWorkload {
 public:
  ClientAsyncWorkload(RpcBench *bench, shared_ptr<Messenger> messenger)
    : bench_(bench),
      messenger_(std::move(messenger)),
      request_count_(0) {
    controller_.set_timeout(MonoDelta::FromSeconds(10));
    proxy_.reset(new CalculatorServiceProxy(messenger_, bench_->server_addr_, "localhost"));
  }

  void CallOneRpc() {
    if (request_count_ > 0) {
      CHECK_OK(controller_.status());
      CHECK_EQ(req_.x() + req_.y(), resp_.result());
    }
    if (!bench_->should_run_) {
      bench_->stop_.CountDown();
      return;
    }
    controller_.Reset();
    req_.set_x(request_count_);
    req_.set_y(request_count_);
    request_count_++;
    proxy_->AddAsync(req_,
                     &resp_,
                     &controller_,
                     [this]() { this->CallOneRpc(); });
  }

  void Start() {
    CallOneRpc();
  }

  RpcBench *bench_;
  shared_ptr<Messenger> messenger_;
  unique_ptr<CalculatorServiceProxy> proxy_;
  uint32_t request_count_;
  RpcController controller_;
  AddRequestPB req_;
  AddResponsePB resp_;
};

TEST_F(RpcBench, BenchmarkCallsAsync) {
  int threads = FLAGS_client_threads;
  int concurrency = FLAGS_async_call_concurrency;

  vector<shared_ptr<Messenger>> messengers;
  for (int i = 0; i < threads; i++) {
    shared_ptr<Messenger> m;
    ASSERT_OK(CreateMessenger("Client", &m));
    messengers.emplace_back(std::move(m));
  }

  vector<unique_ptr<ClientAsyncWorkload>> workloads;
  for (int i = 0; i < concurrency; i++) {
    workloads.emplace_back(
        new ClientAsyncWorkload(this, messengers[i % threads]));
  }

  stop_.Reset(concurrency);

  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();

  for (int i = 0; i < concurrency; i++) {
    workloads[i]->Start();
  }

  SleepFor(MonoDelta::FromSeconds(FLAGS_run_seconds));
  should_run_ = false;

  sw.stop();

  stop_.Wait();
  int total_reqs = 0;
  for (int i = 0; i < concurrency; i++) {
    total_reqs += workloads[i]->request_count_;
  }

  SummarizePerf(sw.elapsed(), total_reqs, false);
}

class RpcAcceptorBench : public RpcTestBase {
 protected:
  RpcAcceptorBench()
      : should_run_(true),
        server_addr_(Sockaddr::Wildcard()) {
    // Instantiate as many acceptor pool's threads as requested.
    n_acceptor_pool_threads_ = FLAGS_acceptor_threads;

    // This test connects to the RPC socket and immediately closes the
    // connection once it has been established. However, the server side passes
    // the accepted connection to the connection negotiation handlers.
    // To make the server side to be able to accept as many connections as
    // possible, make sure the acceptor isn't not going to receive the
    // Status::ServiceUnavailable status because the negotiation thread pool is
    // at capacity and not able to accept one more connection negotiation task.
    // The connection negotiations tasks are very fast because they end up with
    // an error when trying to operate on a connection that has been already
    // closed at the client side.
    n_negotiation_threads_ = 2 * FLAGS_client_threads;

    // Suppress connection negotiation tracing to avoid flooding the output
    // with the traces of failed RPC connection negotiation attempts. In this
    // test, all the opened TCP connections are closed immediately by the
    // client side.
    FLAGS_rpc_suppress_negotiation_trace = true;
  }

  void SetUp() override {
    NO_FATALS(RpcTestBase::SetUp());
    ASSERT_OK(StartTestServer(&server_addr_));
  }

  atomic<bool> should_run_;
  Sockaddr server_addr_;
};

TEST_F(RpcAcceptorBench, MeasureAcceptorDispatchTimes) {
  // It's enough to have just one client thread to verify that the acceptor
  // dispatch times metric works as expected, so let's set this minimum viable
  // configuration as the default one. The option of changing the default
  // setting for --client_threads for all the test scenarios in this file
  // doesn't look attractive since other scenarios rely on client-side
  // concurrency to provide some meaningful test coverage.
  ASSERT_NE("", SetCommandLineOptionWithMode("client_threads",
                                             "1",
                                             gflags::SET_FLAGS_DEFAULT));
  const size_t threads_num = FLAGS_client_threads;

  thread threads[threads_num];
  Status status[threads_num];

  for (auto i = 0; i < threads_num; ++i) {
    auto* my_status = &status[i];
    threads[i] = thread([this, my_status]() {
      while (should_run_) {
        Socket socket;
        if (auto s = socket.Init(server_addr_.family(), /*flags=*/0);
            PREDICT_FALSE(!s.ok())) {
          *my_status = s;
          return;
        }
        if (auto s = socket.SetLinger(true); PREDICT_FALSE(!s.ok())) {
          *my_status = s;
          return;
        }
        if (auto s = socket.Connect(server_addr_); PREDICT_FALSE(!s.ok())) {
          *my_status = s;
          return;
        }
      }
    });
  }

  SleepFor(MonoDelta::FromSeconds(FLAGS_run_seconds));
  should_run_ = false;

  for (auto& t : threads) {
    t.join();
  }

  for (auto i = 0; i < threads_num; ++i) {
    SCOPED_TRACE(Substitute("thread idx $0", i));
    ASSERT_OK(status[i]);
  }

  scoped_refptr<Histogram> t =
      METRIC_acceptor_dispatch_times.Instantiate(server_messenger_->metric_entity());
  LOG(INFO) << Substitute("Dispatched $0 connection requests in $1 seconds",
                          t->TotalCount(), FLAGS_run_seconds);
  LOG(INFO) << Substitute(
      "Request dispatching time (us): min $0 max $1 average $2",
      t->MinValueForTests(), t->MaxValueForTests(), t->MeanValueForTests());
}

} // namespace rpc
} // namespace kudu

