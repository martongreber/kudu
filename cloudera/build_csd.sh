#!/usr/bin/env bash
##########################################################
# Script called during the the `[[install_cmd]]` section
# of the components.ini file in the CDPD build that
# builds the Kudu CSDs.
##########################################################

set -ex

CSD_BUILD_DIR=${1:-"./build/csds"}
VERSION=$(cat version.txt)

pushd java

# First, build the CSDs.
CSD_TASKS=$(find kudu-csd* -type d -maxdepth 0 -exec echo {}:assemble \; | tr '\n' ' ')
GRADLE_ARGS="--console=plain --no-daemon"
echo "Running the following CSD tasks: $CSD_TASKS"
./gradlew -PbuildCSD -PskipSigning $GRADLE_ARGS $CSD_TASKS

# Next, install them into the CSD repo.
mkdir -p ${CSD_BUILD_DIR}
for JAR in kudu-csd*/build/libs/*.jar; do
  if [[ "$JAR" =~ "tests" ]]; then
    # Ignore test JARs.
    continue
  fi

  # Need to override packaging from 'pom' to 'jar' because Maven!
  mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file \
    -Dfile="$JAR" \
    -DlocalRepositoryPath="${CSD_BUILD_DIR}" \
    -Dpackaging=jar \
    -DpomFile="$(dirname $JAR)/../pom.xml"
done
popd

# Tar the published CSD jars.
# Note: The final file should match the definition in the `[[artifacts]]`
# section of the components.ini file in the CDPD build.
tar -czf kudu-csd-${VERSION}.tar.gz -C ${CSD_BUILD_DIR} ./