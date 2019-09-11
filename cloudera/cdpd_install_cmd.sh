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
}

# Show the ccache stats.
ccache -s || true

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
