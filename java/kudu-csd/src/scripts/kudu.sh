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

set -x

# Import some common CM functions, like acquire_kerberos_tgt().
. $COMMON_SCRIPT

# Time marker for both stderr and stdout
date 1>&2

# Preference order:
# 1. KUDU_HOME (set by kudu_env.sh in the KUDU parcel).
# 2. CDH_KUDU_HOME (set by cdh_env.sh in the CDH parcel).
# 3. Hardcoded default value (where the Cloudera packages install Kudu).
DEFAULT_KUDU_HOME=/usr/lib/kudu
export KUDU_HOME=${KUDU_HOME:-$CDH_KUDU_HOME}
export KUDU_HOME=${KUDU_HOME:-$DEFAULT_KUDU_HOME}

CMD=$1
shift 1

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Reads a line in the format "$host:$key=$value", setting those variables.
function readconf {
  local conf
  IFS=':' read host conf <<< "$1"
  IFS='=' read key value <<< "$conf"
}

function tserver_enter_maintenance {
  # Get the UUID from the local FS.
  ts_uuid=$(kudu fs dump uuid --flagfile=$GFLAG_FILE)

  # Set maintenance mode for the given UUID.
  exec kudu tserver state enter_maintenance "$1" "$ts_uuid"
}

function tserver_exit_maintenance {
  # Get the UUID from the local FS.
  ts_uuid=$(kudu fs dump uuid --flagfile=$GFLAG_FILE)

  # Exit maintenance mode for the given UUID.
  exec kudu tserver state exit_maintenance "$1" "$ts_uuid"
}

function run_until_success {
  timeout_sec="$1"
  retry_interval_sec="$2"
  cmd="$3"
  start_time=$(date +%s)
  deadline=$((start_time + timeout_sec))
  while [[ $deadline -gt $(date +%s) ]]; do
    $cmd
    if [[ $? -eq 0 ]]; then
      exit 0;
    fi
    echo "Sleeping for $retry_interval_sec seconds..."
    sleep $retry_interval_sec
  done
  echo "Timed out: $cmd did not succeed within $timeout_sec seconds..."
  exit 1
}

# Runs the rebalancer tool, if it's available.
function run_rebalancer_tool {
  # Heuristic to determine if the rebalance tool is available.
  if ! kudu cluster 2>&1 > /dev/null | grep -q "rebalance"; then
    echo "'kudu cluster rebalance' command not available with this version of Kudu"
    exit 1
  fi

  exec kudu cluster rebalance "$1" \
    --max_moves_per_server="$RB_MAX_MOVES_PER_SERVER" \
    --max_run_time_sec="$RB_MAX_RUN_TIME_SEC" \
    --max_staleness_interval_sec="$RB_MAX_STALENESS_INTERVAL_SEC"
}

log "KUDU_HOME: $KUDU_HOME"
log "CONF_DIR: $CONF_DIR"
log "CMD: $CMD"

# Make sure we've got the main gflagfile.
GFLAG_FILE="$CONF_DIR/gflagfile"
if [ ! -r "$GFLAG_FILE" ]; then
  log "Could not find $GFLAG_FILE, exiting"
  exit 1
fi

# Make sure we've got a file describing the master config.
MASTER_FILE="$CONF_DIR/master.properties"
if [ ! -r "$MASTER_FILE" ]; then
  log "Could not find $MASTER_FILE, exiting"
  exit 1
fi

# Parse the master config.
MASTER_IPS=
for line in $(cat "$MASTER_FILE")
do
  readconf "$line"
  case $key in
    server.address)
      # Fall back to the host only if there's no defined value.
      if [ -n "$value" ]; then
        actual_value="$value"
      else
        actual_value="$host"
      fi

      # Append to comma-separated MASTER_IPS.
      if [ -n "$MASTER_IPS" ]; then
        MASTER_IPS="${MASTER_IPS},"
      fi
      MASTER_IPS="${MASTER_IPS}${actual_value}"
      ;;
  esac
done
log "Found master(s) on $MASTER_IPS"

# Enable core dumping if requested.
if [ "$ENABLE_CORE_DUMP" == "true" ]; then
  # The core dump directory should already exist.
  if [ -z "$CORE_DUMP_DIRECTORY" -o ! -d "$CORE_DUMP_DIRECTORY" ]; then
    log "Could not find core dump directory $CORE_DUMP_DIRECTORY, exiting"
    exit 1
  fi
  # It should also be writable.
  if [ ! -w "$CORE_DUMP_DIRECTORY" ]; then
    log "Core dump directory $CORE_DUMP_DIRECTORY is not writable, exiting"
    exit 1
  fi

  ulimit -c unlimited
  cd "$CORE_DUMP_DIRECTORY"
  STATUS=$?
  if [ $STATUS != 0 ]; then
    log "Could not change to core dump directory to $CORE_DUMP_DIRECTORY, exiting"
    exit $STATUS
  fi
fi

KUDU_ARGS=

if [ "$ENABLE_SECURITY" == "true" ]; then
  # Ideally the value of the enable_security parameter would dictate whether CM
  # emits these parameters to the flagfile in the first place. However, that
  # isn't possible, so we emit them on the command line here instead.
  #
  # CM guarantees [1] this keytab filename.
  #
  # 1. https://github.com/cloudera/cm_ext/wiki/Service-Descriptor-Language-Reference#kerberosprincipals
  KUDU_ARGS="$KUDU_ARGS \
             --rpc_authentication=required \
             --rpc_encryption=required \
             --webserver_require_spnego=true \
             --keytab_file=$CONF_DIR/kudu.keytab"
fi

# If Ranger service is selected as dependency, add Ranger Kudu plugin specific parameters
if [[ -n "${RANGER_SERVICE}" && "${RANGER_SERVICE}" != "none" ]]; then
  # Emit the parameter to the gflagfile.
  KUDU_ARGS="$KUDU_ARGS \
             --ranger_config_path=$CONF_DIR \
             --trusted_user_acl=impala,hive,kudu"

  # TODO(Hao): remove once ranger related gflag is no longer experimental.
  KUDU_ARGS="$KUDU_ARGS --unlock_experimental_flags=true"

  # Populate the required field for 'ranger-kudu-security.xml'
  RANGER_KUDU_PLUGIN_SSL_FILE="${CONF_DIR}/ranger-kudu-policymgr-ssl.xml"
  perl -pi -e "s#\{\{RANGER_KUDU_PLUGIN_SSL_FILE}}#${RANGER_KUDU_PLUGIN_SSL_FILE}#g" "${CONF_DIR}"/ranger-kudu-security.xml

  # Populate the required fields for 'ranger-kudu-policymgr-ssl.xml'. Disable printing
  # the commands as they are sensitive fields.
  set +x
  if [ -n "${KUDU_TRUSTSTORE_LOCATION}" ] && [ -n "${KUDU_TRUSTORE_PASSWORD}" ]; then
    perl -pi -e "s#\{\{RANGER_PLUGIN_TRUSTSTORE}}#${KUDU_TRUSTSTORE_LOCATION}#g" "${CONF_DIR}"/ranger-kudu-policymgr-ssl.xml
    RANGER_PLUGIN_TRUSTSTORE_CRED_FILE="jceks://file${CONF_DIR}/rangerpluginssl.jceks"

    # Use jars from Ranger admin package to generate the trsustore credential file.
    RANGER_ADMIN_CRED_LIB="${PARCELS_ROOT}/${PARCEL_DIRNAMES}/lib/ranger-admin/cred/lib/"
    export JAVA_HOME=${JAVA_HOME};"${JAVA_HOME}"/bin/java -cp "${RANGER_ADMIN_CRED_LIB}/*" org.apache.ranger.credentialapi.buildks create sslTrustStore -value "${KUDU_TRUSTORE_PASSWORD}" -provider "${RANGER_PLUGIN_TRUSTSTORE_CRED_FILE}"
    perl -pi -e "s#\{\{RANGER_PLUGIN_TRUSTSTORE_CRED_FILE}}#${RANGER_PLUGIN_TRUSTSTORE_CRED_FILE}#g" "${CONF_DIR}"/ranger-kudu-policymgr-ssl.xml
  else
    perl -pi -e "s#\{\{RANGER_PLUGIN_TRUSTSTORE}}##g" ${CONF_DIR}/ranger-kudu-policymgr-ssl.xml
    perl -pi -e "s#\{\{RANGER_PLUGIN_TRUSTSTORE_CRED_FILE}}##g" ${CONF_DIR}/ranger-kudu-policymgr-ssl.xml
  fi
  set -x

  # Populate the required fields for 'ranger-kudu-audit.xml'
  KEYTAB_FILE="${CONF_DIR}/kudu.keytab"
  perl -pi -e "s#\{\{KEYTAB_FILE}}#${KEYTAB_FILE}#g" ${CONF_DIR}/ranger-kudu-audit.xml
  if [[ "${RANGER_KUDU_HDFS_AUDIT_DIR}" == *"{ranger_base_audit_url}"* ]]; then
    RANGER_KUDU_HDFS_AUDIT_PATH=$(echo ${RANGER_KUDU_HDFS_AUDIT_DIR} | sed -e "s/\${ranger_base_audit_url}//g")
    RANGER_KUDU_HDFS_AUDIT_PATH="${RANGER_AUDIT_BASE_PATH}${RANGER_KUDU_HDFS_AUDIT_PATH}"
  else
    RANGER_KUDU_HDFS_AUDIT_PATH=${RANGER_KUDU_HDFS_AUDIT_DIR}
  fi
  sed -i "s#{{RANGER_KUDU_HDFS_AUDIT_PATH}}#${RANGER_KUDU_HDFS_AUDIT_PATH}#g" "${CONF_DIR}"/ranger-kudu-audit.xml

  cp -f ${CONF_DIR}/hadoop-conf/core-site.xml ${CONF_DIR}/

  # Optionally create the Kudu service in Ranger.
  "${CONF_DIR}"/scripts/ranger_init.sh -c create -s "${RANGER_KUDU_SERVICE_NAME}"

  # Slurp up sanitized Ranger Kudu service name. And use it to interpolate the non-sanitized one.
  . "${CONF_DIR}/scripts/ranger_vars.sh"
  perl -pi -e "s#\Q${RANGER_KUDU_SERVICE_NAME}\E#${SANITIZED_RANGER_KUDU_SERVICE_NAME}#g" ${CONF_DIR}/ranger-kudu-security.xml
fi

if [ "$CMD" = "master" ]; then
  # Only pass --master_addresses if there's more than one master.
  #
  # Need to use [[ ]] for regex support.
  if [[ "$MASTER_IPS" =~ , ]]; then
    KUDU_ARGS="$KUDU_ARGS --master_addresses=$MASTER_IPS"
  fi

  # Add the location mapping command.
  # This is hardcoded but can be overridden by specifying the flag
  # --location_mapping_cmd in the master gflagfile safety valve.
  TOPOLOGY_SCRIPT="$CONF_DIR/topology.py"
  KUDU_ARGS="$KUDU_ARGS --location_mapping_cmd=$TOPOLOGY_SCRIPT"

  exec "$KUDU_HOME/sbin/kudu-master" \
    $KUDU_ARGS \
    --flagfile="$GFLAG_FILE"
elif [ "$CMD" = "tserver" ]; then
  KUDU_ARGS="$KUDU_ARGS --tserver_master_addrs=$MASTER_IPS"
  exec "$KUDU_HOME/sbin/kudu-tserver" \
    $KUDU_ARGS \
    --flagfile="$GFLAG_FILE"
else
  # If we're not running a server, we need to ensure we have Kerberos
  # credentials cached. This will point our KRB5CCNAME at a file specific to
  # this process and kinit.
  # NOTE: $kudu_principal is populated by CM when security is enabled, and this
  # command will no-op if it is empty.
  acquire_kerberos_tgt kudu.keytab "$kudu_principal"
fi

if [ "$CMD" = "diagnostics-master" ]; then
  LOG_DIR=$1
  shift 1
  python scripts/gather_diagnostics.py "master" "$LOG_DIR" "$PWD" "$MASTER_IPS"
elif [ "$CMD" = "diagnostics-tserver" ]; then
  LOG_DIR=$1
  shift 1
  python scripts/gather_diagnostics.py "tserver" "$LOG_DIR" "$PWD" "$MASTER_IPS"
elif [ "$CMD" = "ksck" ]; then
  exec kudu cluster ksck "$MASTER_IPS"
elif [ "$CMD" = "rebalance_tool" ]; then
  run_rebalancer_tool "$MASTER_IPS"
elif [ "$CMD" = "wait_until_healthy" ]; then
  run_until_success $RR_BATCH_TIME_LIMIT_SEC $RR_HEALTH_CHECK_INTERVAL_SEC \
      "kudu cluster ksck $MASTER_IPS"
elif [ "$CMD" = "tserver_enter_maintenance" ]; then
  tserver_enter_maintenance "$MASTER_IPS"
elif [ "$CMD" = "tserver_exit_maintenance" ]; then
  tserver_exit_maintenance "$MASTER_IPS"
elif [ "$CMD" = "tserver_quiesce" ]; then
  run_until_success $TS_QUIESCING_TIME_LIMIT_SEC $TS_QUIESCING_RETRY_INTERVAL_SEC \
      "kudu tserver quiesce start $TS_QUIESCING_HOST --error_if_not_fully_quiesced"
elif [ "$CMD" = "tserver_stop_quiescing" ]; then
  exec kudu tserver quiesce stop $TS_QUIESCING_HOST
else
  log "Unknown command: $CMD"
  exit 2
fi
