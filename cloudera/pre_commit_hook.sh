#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Build and test Kudu. This script must be run as a user with passwordless sudo
# in an Ubuntu environment compatible with the dist test infrastructure.
set -ex
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.."  && pwd )"

# Install the prerequisite libraries, if they are not installed.
# CentOS/RHEL
if [[ -f "/usr/bin/yum" ]]; then
  sudo yum update -y
  num_attempts=5
  for i in $(seq 1 $num_attempts); do
    if sudo yum install -y autoconf automake curl ccache cyrus-sasl-devel cyrus-sasl-gssapi \
        cyrus-sasl-plain flex gcc gcc-c++ gdb git java-1.8.0-openjdk-devel \
        krb5-server krb5-workstation libtool lsof make maven ninja-build nscd ntp openssl-devel \
        patch pkgconfig python python3-dev python-virtualenv redhat-lsb-core rsync unzip vim-common which ; then
      break;
    fi
    echo "Failed to yum install required packages after $i attempt(s)"
    if [ $i -eq $num_attempts ]; then
      echo "Retries exhausted!"
      exit ${EXECUTION_SETUP_FAILURE}
    fi
    echo Sleeping and retrying...
    sleep 60
  done
  export JAVA8_HOME=/usr/lib/jvm/java-1.8.0-openjdk/
# Ubuntu/Debian
elif [[ -f "/usr/bin/apt-get" ]]; then
  sudo add-apt-repository ppa:openjdk-r/ppa
  sudo apt-get update
  num_attempts=5
  # Some packages, by default, prompt the user for input; avoid this by setting
  # DEBIAN_FRONTEND and the -y configuration.
  for i in $(seq 1 $num_attempts); do
    if sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
        autoconf automake curl ccache flex g++ gcc gdb git krb5-admin-server \
        krb5-kdc krb5-user libkrb5-dev libsasl2-dev libsasl2-modules libsasl2-modules-gssapi-mit \
        libssl-dev libtool lsb-release lsof make maven ninja-build nscd ntp openjdk-8-jdk openssl \
        patch pkg-config python python3-dev python-virtualenv rsync unzip vim-common ; then
      break;
    fi
    echo "Failed to apt-get install required packages after $i attempt(s)"
    if [ $i -eq $num_attempts ]; then
      echo "Retries exhausted!"
      exit ${EXECUTION_SETUP_FAILURE}
    fi
    echo Sleeping and retrying...
    sleep 60
  done
  export JAVA8_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
else
  echo "Unsupported OS"
  exit 1
fi

# Make sure /usr/sbin and /sbin is on the path to ensure lsof is found on CentOS.
export PATH=$PATH:/usr/sbin:/sbin

# Pull the requirements to run via dist test.
pushd $ROOT_DIR
rm -rf isolate-bin
mkdir isolate-bin
curl -fLSs http://cloudera-thirdparty-libs.s3.amazonaws.com/isolate \
    --output isolate-bin/isolate --retry 5
chmod 755 isolate-bin/isolate
rm -rf dist_test
git clone git://github.com/cloudera/dist_test.git
popd

export ENABLE_DIST_TEST=1
export DIST_TEST_HOME=$ROOT_DIR/dist_test
export DIST_TEST_MASTER=http://dist-test.cloudera.org/
export DIST_TEST_USER='kudu-jobs'
export DIST_TEST_PASSWORD='Beo3shei'

export KUDU_FLAKY_TEST_ATTEMPTS=3
export TEST_RESULT_SERVER=dist-test.cloudera.org:8080

export PATH=$ROOT_DIR/isolate-bin:$PATH
# Pre-commits are always run on 4 CPU core YCloud instance.
export PARALLEL=4

(cd "$ROOT_DIR" && build-support/jenkins/build-and-test.sh)
