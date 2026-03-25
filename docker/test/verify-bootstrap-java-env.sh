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
# Auxiliary script to verify that bootstrap-java-env.sh
# installs Java 17 correctly on all supported OS images.
#
# Usage:
#   ./docker/verify-bootstrap-java-env.sh [image1 image2 ...]
#
# When no images are specified all supported base OS images
# from docker-build.py are tested.
#
##########################################################

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOOTSTRAP_SCRIPT="$SCRIPT_DIR/../bootstrap-java-env.sh"

# The full list of supported bases mirrors docker-build.py's --bases choices.
DEFAULT_IMAGES=(
  "centos:7"
  "centos:8"
  "ubuntu:bionic"
  "ubuntu:focal"
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
    -v "$BOOTSTRAP_SCRIPT:/bootstrap-java-env.sh:ro" \
    "$IMAGE" \
    bash -xe -c "
      /bootstrap-java-env.sh
      JAVA_VERSION=\$(java -version 2>&1 | head -1)
      echo \"Installed: \$JAVA_VERSION\"
      echo \"\$JAVA_VERSION\" | grep -q '^openjdk version \"17\.' || {
        echo 'ERROR: Expected Java 17 but got: '\"\$JAVA_VERSION\"
        exit 1
      }
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
