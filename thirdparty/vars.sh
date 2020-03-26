#!/bin/bash
#
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

if [ -z "$TP_DIR" ]; then
   echo "TP_DIR variable not set, check your scripts"
   exit 1
fi

TP_SOURCE_DIR="$TP_DIR/src"
TP_BUILD_DIR="$TP_DIR/build"

# This URL corresponds to the CloudFront Distribution for the S3
# bucket cloudera-thirdparty-libs which is directly accessible at
# http://cloudera-thirdparty-libs.s3.amazonaws.com/
CLOUDFRONT_URL_PREFIX=https://d3dr9sfxru4sde.cloudfront.net

# Third party dependency downloading URL, default to the CloudFront
# Distribution URL.
DEPENDENCY_URL=${DEPENDENCY_URL:-$CLOUDFRONT_URL_PREFIX}

PREFIX_COMMON=$TP_DIR/installed/common
PREFIX_DEPS=$TP_DIR/installed/uninstrumented
PREFIX_DEPS_TSAN=$TP_DIR/installed/tsan

GFLAGS_VERSION=2.2.0
GFLAGS_NAME=gflags-$GFLAGS_VERSION
GFLAGS_SOURCE=$TP_SOURCE_DIR/$GFLAGS_NAME

GLOG_VERSION=0.3.5
GLOG_NAME=glog-$GLOG_VERSION
GLOG_SOURCE=$TP_SOURCE_DIR/$GLOG_NAME

GMOCK_VERSION=1.8.0
GMOCK_NAME=googletest-release-$GMOCK_VERSION
GMOCK_SOURCE=$TP_SOURCE_DIR/$GMOCK_NAME

GPERFTOOLS_VERSION=2.6.90
GPERFTOOLS_NAME=gperftools-$GPERFTOOLS_VERSION
GPERFTOOLS_SOURCE=$TP_SOURCE_DIR/$GPERFTOOLS_NAME

PROTOBUF_VERSION=3.4.1
PROTOBUF_NAME=protobuf-$PROTOBUF_VERSION
PROTOBUF_SOURCE=$TP_SOURCE_DIR/$PROTOBUF_NAME

# Note: CMake gets patched on SLES12SP0. When changing the CMake version, please check if
# cmake-issue-15873-dont-use-select.patch needs to be updated.
CMAKE_VERSION=3.16.4
CMAKE_NAME=cmake-$CMAKE_VERSION
CMAKE_SOURCE=$TP_SOURCE_DIR/$CMAKE_NAME

SNAPPY_VERSION=1.1.4
SNAPPY_NAME=snappy-$SNAPPY_VERSION
SNAPPY_SOURCE=$TP_SOURCE_DIR/$SNAPPY_NAME

LZ4_VERSION=1.9.1
LZ4_NAME=lz4-$LZ4_VERSION
LZ4_SOURCE=$TP_SOURCE_DIR/$LZ4_NAME

# from https://github.com/kiyo-masui/bitshuffle
# Hash of git: 55f9b4caec73fa21d13947cacea1295926781440
BITSHUFFLE_VERSION=55f9b4c
BITSHUFFLE_NAME=bitshuffle-$BITSHUFFLE_VERSION
BITSHUFFLE_SOURCE=$TP_SOURCE_DIR/$BITSHUFFLE_NAME

ZLIB_VERSION=1.2.8
ZLIB_NAME=zlib-$ZLIB_VERSION
ZLIB_SOURCE=$TP_SOURCE_DIR/$ZLIB_NAME

LIBEV_VERSION=4.20
LIBEV_NAME=libev-$LIBEV_VERSION
LIBEV_SOURCE=$TP_SOURCE_DIR/$LIBEV_NAME

RAPIDJSON_VERSION=1.1.0
RAPIDJSON_NAME=rapidjson-$RAPIDJSON_VERSION
RAPIDJSON_SOURCE=$TP_SOURCE_DIR/$RAPIDJSON_NAME

# Hash of the squeasel git revision to use.
# (from http://github.com/cloudera/squeasel)
#
# To re-build this tarball use the following in the squeasel repo:
#  export NAME=squeasel-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SQUEASEL_VERSION=030ccce87359d892e22fb368c5fc5b75d9a2a5f7
SQUEASEL_NAME=squeasel-$SQUEASEL_VERSION
SQUEASEL_SOURCE=$TP_SOURCE_DIR/$SQUEASEL_NAME

# Hash of the mustache git revision to use.
# (from https://github.com/henryr/cpp-mustache)
#
# To re-build this tarball use the following in the mustache repo:
#  export NAME=mustache-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
MUSTACHE_VERSION=b290952d8eb93d085214d8c8c9eab8559df9f606
MUSTACHE_NAME=mustache-$MUSTACHE_VERSION
MUSTACHE_SOURCE=$TP_SOURCE_DIR/$MUSTACHE_NAME

# git revision of google style guide:
# https://github.com/google/styleguide
# git archive --prefix=google-styleguide-$(git rev-parse HEAD)/ -o /tmp/google-styleguide-$(git rev-parse HEAD).tgz HEAD
GSG_VERSION=7a179d1ac2e08a5cc1622bec900d1e0452776713
GSG_NAME=google-styleguide-$GSG_VERSION
GSG_SOURCE=$TP_SOURCE_DIR/$GSG_NAME

GCOVR_VERSION=3.0
GCOVR_NAME=gcovr-$GCOVR_VERSION
GCOVR_SOURCE=$TP_SOURCE_DIR/$GCOVR_NAME

CURL_VERSION=7.68.0
CURL_NAME=curl-$CURL_VERSION
CURL_SOURCE=$TP_SOURCE_DIR/$CURL_NAME

# Hash of the crcutil git revision to use.
# (from http://github.com/cloudera/crcutil)
#
# To re-build this tarball use the following in the crcutil repo:
#  export NAME=crcutil-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
CRCUTIL_VERSION=81f8a60f67190ff1e0c9f2f6e5a07f650671a646
CRCUTIL_NAME=crcutil-$CRCUTIL_VERSION
CRCUTIL_SOURCE=$TP_SOURCE_DIR/$CRCUTIL_NAME

LIBUNWIND_VERSION=1.3.1
LIBUNWIND_NAME=libunwind-$LIBUNWIND_VERSION
LIBUNWIND_SOURCE=$TP_SOURCE_DIR/$LIBUNWIND_NAME

# See package-llvm.sh for details on the LLVM tarball.
LLVM_VERSION=9.0.0
LLVM_NAME=llvm-$LLVM_VERSION.src
LLVM_SOURCE=$TP_SOURCE_DIR/$LLVM_NAME

# The include-what-you-use is built along with LLVM in its source tree.
IWYU_VERSION=0.13

# Python is required to build LLVM 3.6+ because it uses
# llvm/utils/llvm-build/llvmbuild script. It is only built and installed if
# the system Python version is less than 2.7.
PYTHON_VERSION=2.7.13
PYTHON_NAME=python-$PYTHON_VERSION
PYTHON_SOURCE=$TP_SOURCE_DIR/$PYTHON_NAME

# Our trace-viewer repository is separate since it's quite large and
# shouldn't change frequently. We upload the built artifacts (HTML/JS)
# when we need to roll to a new revision.
#
# The source can be found in the 'kudu' branch of https://github.com/cloudera/catapult
# and built with "tracing/kudu-build.sh" included within the repository.
TRACE_VIEWER_VERSION=21d76f8350fea2da2aa25cb6fd512703497d0c11
TRACE_VIEWER_NAME=kudu-trace-viewer-$TRACE_VIEWER_VERSION
TRACE_VIEWER_SOURCE=$TP_SOURCE_DIR/$TRACE_VIEWER_NAME

BOOST_VERSION=1_61_0
BOOST_NAME=boost_$BOOST_VERSION
BOOST_SOURCE=$TP_SOURCE_DIR/$BOOST_NAME

OPENSSL_WORKAROUND_DIR="$TP_DIR/installed/openssl-el6-workaround"

# The breakpad source artifact is created using the script found in
# scripts/make-breakpad-src-archive.sh
BREAKPAD_VERSION=9eac2058b70615519b2c4d8c6bdbfca1bd079e39
BREAKPAD_NAME=breakpad-$BREAKPAD_VERSION
BREAKPAD_SOURCE=$TP_SOURCE_DIR/$BREAKPAD_NAME

# Hash of the sparsehash-c11 git revision to use.
# (from http://github.com/sparsehash/sparsehash-c11)
#
# To re-build this tarball use the following in the sparsehash-c11 repo:
#  export NAME=sparsehash-c11-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
SPARSEHASH_VERSION=cf0bffaa456f23bc4174462a789b90f8b6f5f42f
SPARSEHASH_NAME=sparsehash-c11-$SPARSEHASH_VERSION
SPARSEHASH_SOURCE=$TP_SOURCE_DIR/$SPARSEHASH_NAME

SPARSEPP_VERSION=1.22
SPARSEPP_NAME=sparsepp-$SPARSEPP_VERSION
SPARSEPP_SOURCE=$TP_SOURCE_DIR/$SPARSEPP_NAME

THRIFT_VERSION=0.11.0
THRIFT_NAME=thrift-$THRIFT_VERSION
THRIFT_SOURCE=$TP_SOURCE_DIR/$THRIFT_NAME

BISON_VERSION=3.0.5
BISON_NAME=bison-$BISON_VERSION
BISON_SOURCE=$TP_SOURCE_DIR/$BISON_NAME

# Note: The Hive release binary tarball is stripped of unnecessary jars before
# being uploaded. See thirdparty/package-hive.sh for details.
HIVE_VERSION=3.1.1
HIVE_NAME=hive-$HIVE_VERSION
HIVE_SOURCE=$TP_SOURCE_DIR/$HIVE_NAME

# Note: The Hadoop release tarball is stripped of unnecessary jars before being
# uploaded. See thirdparty/package-hadoop.sh for details.
HADOOP_VERSION=3.2.0
HADOOP_NAME=hadoop-$HADOOP_VERSION
HADOOP_SOURCE=$TP_SOURCE_DIR/$HADOOP_NAME

# TODO(dan): bump to a release version once SENTRY-2371, SENTRY-2440, SENTRY-2471
# and SENTRY-2522 are published. The SHA below is the current head of the master branch.
# Note: Sentry releases source code only. To build the binary tarball, use `dist`
# maven profile. For example, `mvn clean install -Pdist`. After a successful build,
# the tarball will be available under sentry-dist/target.
SENTRY_VERSION=b71a78ed960702536b35e1f048dc40dfc79992d4
SENTRY_NAME=sentry-$SENTRY_VERSION
SENTRY_SOURCE=$TP_SOURCE_DIR/$SENTRY_NAME

# If opting to use the CDH ecosystem, pull the Hadoop ecosystem components
# (e.g. Hadoop, Hive, Sentry) from the most recently published
# 'impala-minicluster-tarballs' package.
if [[ "$KUDU_USE_CDH_ECOSYSTEM" -ne 0 ]]; then
  # This URL corresponds to Cloudera's CDH distribution.
  CAULDRON_URL_PREFIX='http://cloudera-build-us-west-1.vpc.cloudera.com/s3/build'

  # Get the CDH version from 'version.txt' and use it to query BuildDB for the
  # latest Global Build Number (GBN) of that version.
  VERSION_FILE="$TP_DIR/../version.txt"
  if [[ -f $VERSION_FILE ]]; then
    # Transform the full version number from version.txt into a CDH version
    # useable by BuildDb queries. E.g.:
    #   1.8.0-cdh6.x-SNAPSHOT --> 6.x
    #   1.6.0-cdh6.0.x-SNAPSHOT --> 6.0.x
    FULL_VERSION=$(cat $VERSION_FILE | cut -d'-' -f2)
    if [[ $FULL_VERSION == cdh* ]]; then
      CDH_VERSION=${FULL_VERSION#"cdh"}
    fi
  fi
  if [[ -z "$CDH_VERSION" ]]; then
    echo "Error: failed to fetch CDH version number"
    exit 1
  fi

  # Fetch the latest GBN for given version.
  BUILDDB_QUERY='http://builddb.infra.cloudera.com/query?product=cdh;tag=impala-minicluster-tarballs,official;version='"${CDH_VERSION}"
  KUDU_CDH_GBN=$(curl -L --fail --retry 3 -s -S "${BUILDDB_QUERY}")
  if [[ -z "$KUDU_CDH_GBN" ]]; then
    echo "Error: failed to fetch CDH global build number"
    exit 1
  fi

  CDH_TARBALL_URL="${CAULDRON_URL_PREFIX}/${KUDU_CDH_GBN}/impala-minicluster-tarballs"
  TARBALL_FILES=$(curl -L --fail --retry 3 -s -S "${CDH_TARBALL_URL}/index.txt")
  # Pull out the artifacts of interest from the list of tarballs, e.g.:
  #   hadoop-3.0.0-cdh6.x-1041528.tar.gz
  #   hbase-2.1.0-cdh6.x-1041528.tar.gz
  #   hive-2.1.1-cdh6.x-1041528.tar.gz
  #   kudu-1.10.0-cdh6.x-1041528-debian8.tar.gz
  #   kudu-1.10.0-cdh6.x-1041528-redhat6.tar.gz
  #   kudu-1.10.0-cdh6.x-1041528-redhat7.tar.gz
  #   kudu-1.10.0-cdh6.x-1041528-sles12.tar.gz
  #   kudu-1.10.0-cdh6.x-1041528-ubuntu1604.tar.gz
  #   kudu-1.10.0-cdh6.x-1041528-ubuntu1804.tar.gz
  #   sentry-2.1.0-cdh6.x-1041528.tar.gz
  while read line; do
    if [[ $line == hive* ]]; then
      HIVE_NUMBER=$(echo $line | cut -d'-' -f2)
    fi
    if [[ $line == sentry* ]]; then
      SENTRY_NUMBER=$(echo $line | cut -d'-' -f2)
    fi
    if [[ $line == hadoop* ]]; then
      HADOOP_NUMBER=$(echo $line | cut -d'-' -f2)
    fi
  done <<< "$TARBALL_FILES"

  if [[ -z "$HIVE_NUMBER" ]] || [[ -z "$SENTRY_NUMBER" ]] || [[ -z "$HADOOP_NUMBER" ]]; then
    echo "Error: failed to fetch version numbers"
    echo "Hive: ${HIVE_NUMBER}, Sentry: ${SENTRY_NUMBER}, Hadoop: ${HADOOP_NUMBER}"
    echo "Files list: ${TARBALL_FILES}"
    exit 1
  fi

  HIVE_VERSION="${HIVE_NUMBER}-cdh${CDH_VERSION}"
  HIVE_NAME="hive-${HIVE_VERSION}-${KUDU_CDH_GBN}"
  HIVE_SOURCE="$TP_SOURCE_DIR/$HIVE_NAME"

  HADOOP_VERSION="${HADOOP_NUMBER}-cdh${CDH_VERSION}"
  HADOOP_NAME="hadoop-${HADOOP_VERSION}-${KUDU_CDH_GBN}"
  HADOOP_SOURCE="$TP_SOURCE_DIR/$HADOOP_NAME"

  SENTRY_VERSION="${SENTRY_NUMBER}-cdh${CDH_VERSION}"
  SENTRY_NAME="sentry-${SENTRY_VERSION}-${KUDU_CDH_GBN}"
  SENTRY_SOURCE="$TP_SOURCE_DIR/$SENTRY_NAME"
fi

YAML_VERSION=0.6.2
YAML_NAME=yaml-cpp-yaml-cpp-$YAML_VERSION
YAML_SOURCE=$TP_SOURCE_DIR/$YAML_NAME

CHRONY_VERSION=3.5
CHRONY_NAME=chrony-$CHRONY_VERSION
CHRONY_SOURCE=$TP_SOURCE_DIR/$CHRONY_NAME

# Hash of the gumbo-parser git revision to use.
# (from https://github.com/google/gumbo-parser)
#
# To re-build this tarball use the following in the sparsepp repo:
#  export NAME=gumbo-parser-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
GUMBO_PARSER_VERSION=aa91b27b02c0c80c482e24348a457ed7c3c088e0
GUMBO_PARSER_NAME=gumbo-parser-$GUMBO_PARSER_VERSION
GUMBO_PARSER_SOURCE=$TP_SOURCE_DIR/$GUMBO_PARSER_NAME

# Hash of the gumbo-query git revision to use.
# (from https://github.com/lazytiger/gumbo-query)
#
# To re-build this tarball use the following in the sparsepp repo:
#  export NAME=gumbo-query-$(git rev-parse HEAD)
#  git archive HEAD --prefix=$NAME/ -o /tmp/$NAME.tar.gz
#  s3cmd put -P /tmp/$NAME.tar.gz s3://cloudera-thirdparty-libs/$NAME.tar.gz
GUMBO_QUERY_VERSION=c9f10880b645afccf4fbcd11d2f62a7c01222d2e
GUMBO_QUERY_NAME=gumbo-query-$GUMBO_QUERY_VERSION
GUMBO_QUERY_SOURCE=$TP_SOURCE_DIR/$GUMBO_QUERY_NAME

POSTGRES_VERSION=12.2
POSTGRES_NAME=postgresql-$POSTGRES_VERSION
POSTGRES_SOURCE=$TP_SOURCE_DIR/$POSTGRES_NAME

POSTGRES_JDBC_VERSION=42.2.10
POSTGRES_JDBC_NAME=postgresql-$POSTGRES_JDBC_VERSION
POSTGRES_JDBC_SOURCE=$TP_SOURCE_DIR/$POSTGRES_JDBC_NAME

# If you need to rebuild the tarball for a specific hash instead of a release,
# run the following commands:
# mvn versions:set -DnewVersion=$(git rev-parse HEAD)
# mvn versions:update-child-modules
# mvn package
RANGER_VERSION=f37f5407eee8d2627a4306a25938b151f8e2ba31
RANGER_NAME=ranger-$RANGER_VERSION-admin
RANGER_SOURCE=$TP_SOURCE_DIR/$RANGER_NAME
