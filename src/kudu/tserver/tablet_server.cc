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

#include "kudu/tserver/tablet_server.h"

#include <functional>
#include <memory>
#include <ostream>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/cfile/block_cache.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/service_if.h"
#include "kudu/server/rpc_server.h"
#include "kudu/server/startup_path_handler.h"
#include "kudu/transactions/txn_system_client.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_copy_service.h"
#include "kudu/tserver/tablet_service.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver_path_handlers.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace kudu {
class Timer;
} // namespace kudu

using kudu::fs::ErrorHandlerType;
using kudu::rpc::ServiceIf;
using kudu::transactions::TxnSystemClientInitializer;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tserver {

TabletServer::TabletServer(const TabletServerOptions& opts)
    : KuduServer("TabletServer", opts, "kudu.tabletserver"),
      state_(kStopped),
      quiescing_(false),
      fail_heartbeats_for_tests_(false),
      opts_(opts),
      tablet_manager_(new TSTabletManager(this)),
      scanner_manager_(new ScannerManager(metric_entity())),
      path_handlers_(new TabletServerPathHandlers(this)) {
}

TabletServer::~TabletServer() {
  ShutdownImpl();
}

Status TabletServer::Init() {
  CHECK_EQ(kStopped, state_);
  startup_path_handler_->set_is_tablet_server(true);
  cfile::BlockCache::GetSingleton()->StartInstrumentation(metric_entity());

  UnorderedHostPortSet master_addrs;
  for (auto addr : opts_.master_addresses) {
    master_addrs.emplace(std::move(addr));
  }
  // If we deduplicated some masters addresses, log something about it.
  if (master_addrs.size() < opts_.master_addresses.size()) {
    vector<HostPort> addr_list;
    for (const auto& addr : master_addrs) {
      addr_list.emplace_back(addr);
    }
    LOG(INFO) << "deduplicated master addresses: "
              << HostPort::ToCommaSeparatedString(addr_list);
  }
  // Validate that the passed master address actually resolves.
  // We don't validate that we can connect at this point -- it should
  // be allowed to start the TS and the master in whichever order --
  // our heartbeat thread will loop until successfully connecting.
  for (const auto& addr : master_addrs) {
    RETURN_NOT_OK_PREPEND(dns_resolver()->ResolveAddresses(addr, nullptr),
        strings::Substitute("couldn't resolve master service address '$0'",
                            addr.ToString()));
  }

  RETURN_NOT_OK(KuduServer::Init());

  maintenance_manager_ = std::make_shared<MaintenanceManager>(
      MaintenanceManager::kDefaultOptions, fs_manager_->uuid(), metric_entity());

  client_initializer_.reset(new TxnSystemClientInitializer);
  RETURN_NOT_OK(client_initializer_->Init(messenger_, opts_.master_addresses));

  heartbeater_.reset(new Heartbeater(std::move(master_addrs), this));

  Timer* start_tablets = startup_path_handler_->start_tablets_progress();
  std::atomic<int>* tablets_processed = startup_path_handler_->tablets_processed();
  std::atomic<int>* total_tablets = startup_path_handler_->tablets_total();
  RETURN_NOT_OK_PREPEND(tablet_manager_->Init(start_tablets, tablets_processed,
                                              total_tablets),
                        "Could not init Tablet Manager");
  RETURN_NOT_OK_PREPEND(scanner_manager_->StartCollectAndRemovalThread(),
      "Could not start slow scans collect and expired Scanner removal thread");

  state_ = kInitialized;
  return Status::OK();
}

Status TabletServer::WaitInited() {
  return tablet_manager_->WaitForAllBootstrapsToFinish();
}

Status TabletServer::Start() {
  CHECK_EQ(kInitialized, state_);

  fs_manager_->SetErrorNotificationCb(
      ErrorHandlerType::DISK_ERROR, [this](const string& uuid, const string& tenant_id) {
        this->tablet_manager_->FailTabletsInDataDir(uuid, tenant_id);
      });
  fs_manager_->SetErrorNotificationCb(
      ErrorHandlerType::CFILE_CORRUPTION, [this](const string& uuid,
                                                 const string& /* tenant_id */) {
        this->tablet_manager_->FailTabletAndScheduleShutdown(uuid);
      });
  fs_manager_->SetErrorNotificationCb(
      ErrorHandlerType::KUDU_2233_CORRUPTION, [this](const string& uuid,
                                                     const string& /* tenant_id */) {
        this->tablet_manager_->FailTabletAndScheduleShutdown(uuid);
      });
  unique_ptr<ServiceIf> ts_service(new TabletServiceImpl(this));
  unique_ptr<ServiceIf> admin_service(new TabletServiceAdminImpl(this));
  unique_ptr<ServiceIf> consensus_service(new ConsensusServiceImpl(this, tablet_manager_.get()));
  unique_ptr<ServiceIf> tablet_copy_service(new TabletCopyServiceImpl(
      this, tablet_manager_.get()));

  RETURN_NOT_OK(RegisterService(std::move(ts_service)));
  RETURN_NOT_OK(RegisterService(std::move(admin_service)));
  RETURN_NOT_OK(RegisterService(std::move(consensus_service)));
  RETURN_NOT_OK(RegisterService(std::move(tablet_copy_service)));
  RETURN_NOT_OK(KuduServer::Start());

  if (web_server_) {
    RETURN_NOT_OK(path_handlers_->Register(web_server_.get()));
  }

  RETURN_NOT_OK(heartbeater_->Start());
  RETURN_NOT_OK(maintenance_manager_->Start());

  google::FlushLogFiles(google::INFO); // Flush the startup messages.

  state_ = kRunning;
  return Status::OK();
}

void TabletServer::ShutdownImpl() {
  if (kInitialized == state_ || kRunning == state_) {
    const string name = rpc_server_->ToString();
    LOG(INFO) << "TabletServer@" << name << " shutting down...";

    // 1. Stop accepting new RPCs.
    UnregisterAllServices();

    // 2. Shut down the tserver's subsystems.
    maintenance_manager_->Shutdown();
    WARN_NOT_OK(heartbeater_->Stop(), "Failed to stop TS Heartbeat thread");
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::KUDU_2233_CORRUPTION);
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::CFILE_CORRUPTION);
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::DISK_ERROR);
    tablet_manager_->Shutdown();

    client_initializer_->Shutdown();

    // 3. Shut down generic subsystems.
    KuduServer::Shutdown();
    LOG(INFO) << "TabletServer@" << name << " shutdown complete.";
  }
  state_ = kStopped;
}

} // namespace tserver
} // namespace kudu
