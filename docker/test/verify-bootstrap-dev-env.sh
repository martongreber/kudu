#!/bin/bash
##########################################################
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
#
# Auxiliary script to verify that bootstrap-dev-env.sh
# installs development tools correctly on all supported OS images.
#
# Usage:
#   ./docker/test/verify-bootstrap-dev-env.sh [image1 image2 ...]
#
# When no images are specified all supported base OS images
# from docker-build.py are tested.
#
##########################################################

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOOTSTRAP_SCRIPT="$SCRIPT_DIR/../bootstrap-dev-env.sh"

# The full list of supported bases mirrors docker-build.py's --bases choices.
DEFAULT_IMAGES=(
  "rockylinux:8"
  "rockylinux:9"
  "ubuntu:bionic"
  "ubuntu:focal"
  "ubuntu:jammy"
  "opensuse/leap:15"
)

if [[ $# -gt 0 ]]; then
  IMAGES=("$@")
else
  IMAGES=("${DEFAULT_IMAGES[@]}")
fi

PASS=0
FAIL=0
declare -a RESULTS

for IMAGE in "${IMAGES[@]}"; do
  echo ""
  echo "=========================================="
  echo "Testing: $IMAGE"
  echo "=========================================="

  if docker run --rm \
    -v "$BOOTSTRAP_SCRIPT:/bootstrap-dev-env.sh:ro" \
    "$IMAGE" \
    bash -xe -c "
      /bootstrap-dev-env.sh
      GCC_VERSION=\$(gcc --version | head -1)
      echo \"gcc: \$GCC_VERSION\"
      GXX_VERSION=\$(g++ --version | head -1)
      echo \"g++: \$GXX_VERSION\"
      CMAKE_VERSION=\$(cmake --version | head -1)
      echo \"cmake: \$CMAKE_VERSION\"
      MAKE_VERSION=\$(make --version | head -1)
      echo \"make: \$MAKE_VERSION\"
      GIT_VERSION=\$(git --version)
      echo \"git: \$GIT_VERSION\"
      NINJA_VERSION=\$(ninja --version)
      echo \"ninja: \$NINJA_VERSION\"
    "; then
    echo ">>> PASS: $IMAGE"
    RESULTS+=("PASS: $IMAGE")
    ((PASS++))
  else
    echo ">>> FAIL: $IMAGE"
    RESULTS+=("FAIL: $IMAGE")
    ((FAIL++))
  fi
done

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
for RESULT in "${RESULTS[@]}"; do
  echo "  $RESULT"
done
echo ""
echo "Passed: $PASS / $((PASS + FAIL))"

if [[ "$FAIL" -gt 0 ]]; then
  exit 1
fi
