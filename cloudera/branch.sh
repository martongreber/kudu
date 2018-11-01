#!/bin/bash
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
# branch.sh bumps the cdh suffix in the kudu project and stages
# the changes. More about how this script is run here:
#   http://github.mtv.cloudera.com/CDH/cdh/blob/cdh6.x/README_cauldron.md#branching-and-branch-names
set -exu

# Update cdh suffix in version.txt.
# Note that version.txt may optionally include a -SNAPSHOT suffix.
PREV_FULL_VERSION=$(cat version.txt)
perl -077 -i -pe "s/cdh$CDH_START_MAVEN_VERSION/cdh$CDH_NEW_MAVEN_VERSION/" version.txt

# Update cm versions in the csd service descriptors.
perl -077 -i -pe "s/$CM_START_VERSION/$CM_NEW_VERSION/" java/kudu-csd*/src/descriptor/service.sdl

# Update the cdh and cm versions for the Java modules from within the `java` directory:
FULL_VERSION=$(cat version.txt)
(
cd java
# Update gradle.properties
perl -077 -i -pe "s/cdhversion\s*=\s*$CDH_START_MAVEN_VERSION/cdhversion=$CDH_NEW_MAVEN_VERSION/; \
                  s/cmVersion\s*=\s*$CM_START_VERSION/cmVersion=$CM_NEW_VERSION/" gradle.properties
)

# Assert something changed and print the diff.
! git diff --exit-code
# Stage changes
git add -u
