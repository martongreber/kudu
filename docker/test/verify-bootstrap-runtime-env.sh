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
# Auxiliary script to verify that bootstrap-runtime-env.sh
# installs runtime dependencies correctly on all supported OS images.
#
# Usage:
#   ./docker/test/verify-bootstrap-runtime-env.sh [image1 image2 ...]
#
# When no images are specified all supported base OS images
# from docker-build.py are tested.
#
##########################################################

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOOTSTRAP_SCRIPT="$SCRIPT_DIR/../bootstrap-runtime-env.sh"

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
    -v "$BOOTSTRAP_SCRIPT:/bootstrap-runtime-env.sh:ro" \
    "$IMAGE" \
    bash -xe -c "
      /bootstrap-runtime-env.sh
      OPENSSL_VERSION=\$(openssl version)
      echo \"Installed: \$OPENSSL_VERSION\"
      echo \"\$OPENSSL_VERSION\" | grep -qi '^openssl' || {
        echo 'ERROR: openssl not found or not working'
        exit 1
      }
      which kinit || {
        echo 'ERROR: kinit (kerberos client) not found in PATH'
        exit 1
      }
      echo \"kinit found at: \$(which kinit)\"
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
