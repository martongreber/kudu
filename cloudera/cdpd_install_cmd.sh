#!/usr/bin/env bash
##########################################################
# Script called during the the `[[install_cmd]]` section
# of the components.ini file in the CDPD build.
##########################################################

set -ex

VERSION=$(cat version.txt)
GIT_HASH=$(git rev-parse HEAD)

function do_make_build() {
  local BUILD_TYPE=$1
  mkdir -p build/${BUILD_TYPE}
  pushd build/${BUILD_TYPE}
  rm -rf CMakeCache.txt CMakeFiles/
  ../../build-support/enable_devtoolset.sh \
    ../../thirdparty/installed/common/bin/cmake \
    -DKUDU_LINK=static -DNO_TESTS=1 -DKUDU_GIT_HASH="$GIT_HASH" \
    -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ../..
  make -j$(nproc)
  make -j$(nproc) install DESTDIR=$(pwd)/client
  popd

  # Show the ccache stats.
  ccache -s || true
}

# This script is typically invoked from one of several different Jenkins jobs.
# The jobs share a common ccache but each operates in its own workspace:
#
#   /grid/0/jenkins/workspace/Job_A/...
#   /grid/0/jenkins/workspace/Job_B/...
#
# By default, ccache hashes the absolute path of each file, which means there
# won't be any cross-job cache hits (due to the differing workspace paths). If
# we provide a base_dir, ccache will rewrite file paths to be relative to that
# base_dir, enabling far more cache hits across jobs.
export CCACHE_BASEDIR="$(pwd)"

# By default, ccache includes the current working directory (CWD) in a
# compilation's hash. This is used to distinguish between two compilations when
# compiling with -g. Kudu always uses -g, which means differences in the CWD
# (such as between two Jenkins workspaces) will lead to cache misses.
#
# Let's disable this feature to improve our cache hit rate. The downside is
# potentially incorrect CWDs in the debug info of our object files, but that
# should only minimally impact Kudu's source-based debugability.
export CCACHE_NOHASHDIR=1

# The CDP build prepopulates the ccache directory. Zero ccache statistics to ignore
# this irrelevent past.
ccache -z || true
ccache -p || true

# Build thirdparty.
build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh

# Build a release and fastdebug build.
do_make_build release
do_make_build fastdebug

# Build the java modules.
pushd java
./gradlew install -PskipSigning=true
popd

# Copy the published artifacts to a versioned staging directory.
mkdir -p ../kudu-${VERSION}/java ../kudu-${VERSION}/thirdparty
cp -rpf build ../kudu-${VERSION}/
cp -rpf java/*/build/libs/*.jar ../kudu-${VERSION}/java/
cp -rpf www ../kudu-${VERSION}/
cp -pf NOTICE.txt LICENSE.txt ../kudu-${VERSION}/
cp -pf thirdparty/LICENSE.txt ../kudu-${VERSION}/thirdparty/

# Tar the published artifacts and remove the staging directory.
# Note: The final file should match the definition in the `[[artifacts]]`
# section of the components.ini file in the CDPD build.
tar --exclude-vcs -czf kudu-${VERSION}.tar.gz ../kudu-${VERSION}
rm -rf ../kudu-${VERSION}
