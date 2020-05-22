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

function log {
  echo "$(date): $*"
}

# Print args to stderr and exits with failure.
function die() {
  echo "$@" 1>&2
  exit 1
}

# Check for the required enviorment variables.
function check_env_variables() {
  for variable_name in "$@"
  do
    if [[ -v ${variable_name} ]]
    then
      log "${variable_name}: ${!variable_name}"
    else
      die "${variable_name} shall be defined"
    fi
  done
}

# Export the new enviorment variables.
function write_env_variables() {
  SCRIPT_NAME="${CONF_DIR}/scripts/ranger_vars.sh"
  echo "#!/usr/bin/env bash" > "${SCRIPT_NAME}"

  for variable_name in "$@"
  do
    if [[ -v ${variable_name} ]]
    then
      printf "export %s=%s\n" "${variable_name}" "${!variable_name}" >> "${SCRIPT_NAME}"
    fi
  done

}

# Create the sanitized Ranger Kudu service name.
function create_sanitize_service_name() {
  # Generate the service name if needed. Otherwise use the supplied name.
  if [[ "${service}" == "{{GENERATED_RANGER_SERVICE_NAME}}" ]]
  then
    service=${RANGER_REPO_NAME}
  fi

  # Sanitize the service name.
  service=${service//@([^[:alnum:]])/_}

  # Assign to the sanitized variable for futher use.
  export SANITIZED_RANGER_KUDU_SERVICE_NAME=${service}
}

function connect_ranger() {
  counter=1
  counter_max=5

  until curl -k --output /dev/null --silent --head "${RANGER_REST_URL}" || (( counter > counter_max ))
  do
    echo "waiting to connect to ${RANGER_REST_URL} ..."
    sleep 5
    (( counter++ ))
  done

  if (( counter > counter_max ))
  then
    die "Ranger is unreachable"
  fi
}

# Perform REST call to the Ranger service.
function ranger_service_rest() {
  API_URL_PATH=${1}
  HTTP_METHOD=${2}
  INPUT=${3:+--data @${3}}

  connect_ranger
  REQUEST_URL=""
  if [[ "$RANGER_REST_URL" == */ ]]
  then
    REQUEST_URL=${RANGER_REST_URL}${API_URL_PATH}
  else
    REQUEST_URL=${RANGER_REST_URL}/${API_URL_PATH}
  fi
  read -r http_code < <(curl --output /dev/null -s -k -w "%{http_code}" --negotiate -u :  -X "${HTTP_METHOD}" -H "Content-Type:application/json" ${INPUT} "${REQUEST_URL}")
  log "result" "${HTTP_METHOD} ${API_URL_PATH}" "${http_code}"
}

# for debugging
set -x
shopt -s extglob

log "Host: $(hostname)"
log "Pwd: $(pwd)"

# Check whether Ranger service is available.
if [[ -z "${RANGER_SERVICE}" || "${RANGER_SERVICE}" == "none" ]]
# Otherwise, die gracefully.
then
  log "No Ranger service defined, nothing to do"
  exit 0
fi

# Kinit if Kerberos is enabled. Check for 'Enable Security' flag
# as it indicates Kerberos is required.
if [[ ${ENABLE_SECURITY} == "true" ]]
then
  kinit -k -t "${CONF_DIR}/kudu.keytab" "${kudu_principal}"
  log "kerberos tickets" $(klist)
# Otherwise if Kerberos is not available, die gracefully.
else
  log "Kerberos not enabled, won't connect to Ranger"
  exit 0
fi

check_env_variables CONF_DIR RANGER_REST_URL kudu_principal CLUSTER_NAME RANGER_REPO_NAME

# Process the command line arguments.
while getopts "c:s:" arg
do
  case $arg in
    c)
      command=${OPTARG}
      ;;
    s)
      service=${OPTARG}
      ;;
    *)
      command=create
  esac
done

# SANITIZED_RANGER_KUDU_SERVICE_NAME contains the sanitized service name after this call.
create_sanitize_service_name
write_env_variables SANITIZED_RANGER_KUDU_SERVICE_NAME

case $command in
  # Check for existence of the service. And then create the service, if does not exist.
  (create)
    ranger_service_rest "service/public/v2/api/service/name/${SANITIZED_RANGER_KUDU_SERVICE_NAME}" GET
    if [[ "${http_code}" == "404" ]]
    then
      ranger_service_rest "service/public/v2/api/service" POST <(envsubst < "$CONF_DIR"/aux/repo_create.json)
      if [[ "${http_code}" == "200" ]]
      then
        exit 0;
      else
        die "API call failed for creating ranger repo: ${SANITIZED_RANGER_KUDU_SERVICE_NAME}"
      fi
    else
      die "API call failed for getting ranger repo: ${SANITIZED_RANGER_KUDU_SERVICE_NAME}"
    fi
    ;;

  (*)
    die "Don't understand [$command]"
    ;;
esac
