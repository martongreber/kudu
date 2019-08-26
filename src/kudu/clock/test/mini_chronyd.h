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
#include <ctime>
#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;
class RWFile;
class Subprocess;

namespace clock {

// Options to run MiniChronyd with.
struct MiniChronydOptions {
  // There might be multiple mini_chronyd run by the same test.
  //
  // Default: 0
  size_t index = 0;

  // Directory under which to store all chronyd-related data.
  //
  // Default: "", which auto-generates a unique path for this chronyd.
  // The default may only be used from a gtest unit test.
  std::string data_root;

  // IP address or path Unix domain local socket file used to listen to command
  // packets (issued by chronyc).
  // This directly maps to chronyd's configuration property with the same name.
  //
  // Default: "", which auto-generates a unique path to Unix domain socket for
  // this chronyd. The default may only be used from a gtest unit test.
  std::string bindcmdaddress;

  // IP address to bind the NTP server to listen and respond to client requests.
  // This directly maps to chronyd's configuration property with the same name.
  //
  // Default: 127.0.0.1
  std::string bindaddress = "127.0.0.1";

  // Port of the NTP server to listen to and serve requests from clients.
  // This directly maps to chronyd's configuration property with the same name.
  //
  // Default: 10123 (10000 + the default NTP port).
  uint16_t port = 10123;

  // File to store PID of the chronyd process.
  // This directly maps to chronyd's configuration property with the same name.
  //
  // Default: "", which auto-generates a unique pid file path for this chronyd.
  // The default may only be used from a gtest unit test.
  std::string pidfile;

  // Returns a string representation of the options suitable for debug printing.
  std::string ToString() const;
};

// MiniChronyd is a wrapper around chronyd NTP daemon running in server-only
// mode (i.e. it doesn't drive the system clock), allowing manual setting of the
// reference true time. MiniChronyd is used in tests as a reference NTP servers
// for the built-in NTP client.
class MiniChronyd {
 public:
  // Structure to represent relevant information from output by
  // 'chronyc serverstats'.
  struct ServerStats {
    int64_t cmd_packets_received;
    int64_t ntp_packets_received;
  };

  // Check that NTP servers with the specified endpoints are seen as a good
  // enough synchronisation source by the reference NTP client (chronyd itself).
  // The client will wait for no more than the specified timeout in seconds
  // for the set reference servers to become a good NTP source.
  // This method returns Status::OK() if the servers look like a good source
  // for clock synchronisation via NTP, even if the offset of the client's clock
  // from the reference clock provided by NTP server(s) is huge.
  static Status CheckNtpSource(const std::vector<HostPort>& servers,
                               int timeout_sec = 3)
      WARN_UNUSED_RESULT;

  // Create a new MiniChronyd with the provided options, or with defaults
  // if the 'options' argument is omitted.
  explicit MiniChronyd(MiniChronydOptions options = {});

  ~MiniChronyd();

  // Return the options the underlying chronyd has been started with.
  const MiniChronydOptions& options() const;

  // Get the PID of the chronyd process.
  pid_t pid() const;

  // Start the mini chronyd in server-only mode.
  Status Start() WARN_UNUSED_RESULT;

  // Stop the mini chronyd.
  Status Stop() WARN_UNUSED_RESULT;

  // Get NTP server statistics as output by 'chronyc serverstats'.
  Status GetServerStats(ServerStats* stats) const;

  // Manually set the reference time for the underlying chronyd
  // with the precision of 1 second. The input is number of seconds
  // from the beginning of the Epoch.
  Status SetTime(time_t time) WARN_UNUSED_RESULT;

 private:
  friend class MiniChronydTest;

  // Find absolute path to chronyc (chrony's CLI tool),
  // storing the result path in 'path' output parameter.
  static Status GetChronycPath(std::string* path);

  // Find absolute path to chronyd (chrony NTP implementation),
  // storing the result path in 'path' output parameter.
  static Status GetChronydPath(std::string* path);

  // Get absolute path to an executable from the chrony bundle.
  static Status GetPath(const std::string& path_suffix, std::string* abs_path);

  // Correct the ownership of the target path to be compliant with chrony's
  // security constraints.
  static Status CorrectOwnership(const std::string& path);

  // A shortcut to options_.bindcmdaddress: returns the command address
  // for the underlying chronyd, by default that's the absolute path
  // to a Unix socket.
  std::string cmdaddress() const { return options_.bindcmdaddress; }

  // Return absolute path to chronyd's configuration file.
  std::string config_file_path() const;

  // Create a chrony.conf file with server-only mode settings and other options
  // corresponding to MiniChronydOptions in the data root of Kudu mini cluster.
  Status CreateConf() WARN_UNUSED_RESULT;

  // Run chronyc command with arguments as specified by 'args', targeting this
  // chronyd instance.
  Status RunChronyCmd(const std::vector<std::string>& args,
                      std::string* out_stdout = nullptr,
                      std::string* out_stderr = nullptr) const WARN_UNUSED_RESULT;

  MiniChronydOptions options_;
  std::unique_ptr<RWFile> cmd_socket_;
  std::unique_ptr<Subprocess> process_;
};

} // namespace clock
} // namespace kudu
