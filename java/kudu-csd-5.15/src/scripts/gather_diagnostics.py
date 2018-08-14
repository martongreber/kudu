#!/usr/bin/env python

"""
This script collects information for inclusion in a Cloudera Manager support
bundle. The steps are:

1. Collect all information to the staging directory.
2. Redact.
3. Compress the redacted information into a .tar.gz.
4. Check that the result does not exceed CM's per-role size limit for service diagnostics.
5. Create a symlink pointing to the .tar.gz from a specially-named file in the CM
   runner's process directory.

The diagnostics are stored in a bundle directory inside the log directory. If there is a fatal
error gathering diagnostics, the bundle directory is deleted. If diagnostics gathering
succeeds, any staging information is deleted and only the compressed archive remains.

The following diagnostics are gathered:
- ksck, on each master
- the diagnostics logs, from most recent to least recent limited by size

"""

import argparse
import json
import os.path
import re
import shutil
import subprocess
import uuid

# Role types.
MASTER, TSERVER = "master", "tserver"

# The maximum size in bytes of additional bundle data permitted by CM, post compression.
MAX_BUNDLE_SIZE = 10 * 1024 * 1024 # 10 MiB
EXCEEDS_MAX_SIZE_MSG = "The Kudu diagnostics were deleted: they exceeded the maximum size (%d/%d)"

# The maximum size in bytes that the diagnostics logs are allowed to consume.
# This must be smaller than MAX_BUNDLE_SIZE.
MAX_DIAG_LOGS_SIZE = 9 * 1024 * 1024 # 9 MiB
EXCEEDS_MAX_DIAG_LOGS_SIZE_MSG = ("One or more Kudu diagnostics logs were omitted because the "
                                  "estimated size of the logs exceeded the limit (%d/%d)")
assert(MAX_DIAG_LOGS_SIZE < MAX_BUNDLE_SIZE)

# The estimated compression ratio for diagnostics log. This is based off measuring the
# compression ratios of several multi-megabyte diagnostics logs created during an insert
# workload. The empirical ratio was 96%; the estimate used is conservative.
APPROX_COMP_RATIO = 0.90

def gather_diagnostics_logs(log_dir, staging_dir):
    # Sort the directory listing in reverse order. Since diagnostics logs are named with their
    # creation datetime like <common prefix>.yyyymmdd-hhmmss..., we will process them from
    # newest to oldest. Therefore, we take logs until we've met our size quota, knowing we're
    # taking the most interesting (= newest) logs.
    diag_logs_size = 0
    for diag_log in (f for f in sorted(os.listdir(log_dir), reverse=True) if "diagnostics" in f):
        src_diag_path = os.path.join(log_dir, diag_log)
        if not os.path.isfile(src_diag_path):
            continue

        # Kudu guarantees gzipped logs have a .gz suffix.
        # We could be fancier and examine the magic bytes or something, but this should work.
        gzipped = diag_log.endswith('.gz')

        # Estimate how much logs will contribute to the final .tgz, to stay below the limit.
        sz = os.path.getsize(src_diag_path)
        diag_logs_size += sz if gzipped else (1 - APPROX_COMP_RATIO) * sz

        if diag_logs_size > MAX_DIAG_LOGS_SIZE:
            print EXCEEDS_MAX_DIAG_LOGS_SIZE_MSG % (diag_logs_size, MAX_DIAG_LOGS_SIZE)
            break

        dst_diag_path = os.path.join(staging_dir, diag_log)
        shutil.copy2(src_diag_path, dst_diag_path)

def do_ksck(staging_dir, master_addrs):
    with open(os.path.join(staging_dir, "ksck"), 'w') as ksck_out:
        subprocess.call(["kudu", "cluster", "ksck", master_addrs],
                        stdout=ksck_out,
                        stderr=subprocess.STDOUT)

"""
  Redact the ksck output in 'staging_dir' by replacing regex matches according
  to the rules in redaction-rules.json, which can be found in the process directory.

  redaction-rules.json looks like:

  {
    "rules": [
      {
        "description": "Redact passwords from json files",
        "caseSensitive": false,
        "trigger": "password",
        "search": "\"password\"[ ]*:[ ]*\"[^\"]+\"",
        "replace": "\"password\": \"BUNDLE-REDACTED\""
      },
      ...
    ]
  }
"""
# TODO(wdberkeley): Until OPSAPS-45182 is fixed, redaction-rules.json will just
#  be empty, so this code won't do much.
# TODO(wdberkeley): Redact the diagnostics logs according to the redaction rules.
#                   It'll be a pain, we'll have to ungzip, redact, re-gzip :(.
def redact(staging_dir):
    rulefilename = "redaction-rules.json"
    if not os.path.exists(rulefilename) or os.path.getsize(rulefilename) == 0:
        print("No %s found: skipping redaction" % rulefilename)
        return
    with open(rulefilename) as rulefile:
        rulejson = json.load(rulefile)
    if "rules" not in rulejson:
        print("No 'rules' array in %s: skipping redaction" % rulefilename)
        return
    rules = rulejson["rules"]
    if len(rules) == 0:
        print("No redaction rules in %s: skipping redaction" % rulefilename)
        return
    ksck_file = os.path.join(staging_dir, "ksck")
    with open(ksck_file, 'r') as ksck_out:
        ksck = ksck_out.read()
    for rule in rules:
        flags = 0 if rule["caseSensitive"] == "true" else re.IGNORECASE
        pattern = re.compile(rule["search"], flags)
        ksck = re.sub(pattern, rule["replace"], ksck)
    with open(ksck_file, 'w') as ksck_out:
        ksck_out.write(ksck)

def compress_and_link(staging_dir, diag_dir, process_dir):
    tarball = os.path.join(diag_dir, "diagnostics.tar.gz")
    subprocess.check_call(["tar", "zcvf", tarball, "-C", staging_dir, "."])

    # If the diagnostics are too large, replace them with an error message.
    diag_size = os.path.getsize(tarball)
    if diag_size > MAX_BUNDLE_SIZE:
        msg = EXCEEDS_MAX_SIZE_MSG % (diag_size, MAX_BUNDLE_SIZE)
        print msg
        with open(os.path.join(process_dir, "error"), 'w') as error_out:
            error_out.write("%s\n" % msg)
        tarball = os.path.join(process_dir, "error.tar.gz")
        subprocess.check_call(["tar", "zcvf", tarball, "-C", process_dir, "error"])
        shutil.rmtree(diag_dir)

    subprocess.check_call(["ln", "-s", tarball, os.path.join(process_dir, "output.gz")])

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("role_type", choices=[MASTER, TSERVER], help="the role type")
    parser.add_argument("log_dir", help="the Kudu role's log directory")
    parser.add_argument("process_dir", help="the script runner's process directory")
    parser.add_argument("master_addrs", help="comma-separated list of master addresses")
    args = parser.parse_args()
    return args.role_type, args.log_dir, args.process_dir, args.master_addrs

def main():
    role_type, log_dir, process_dir, master_addrs = setup_args()

    # Add a UUID to the bundle dir because if two Kudu role instances share
    # a log dir, their bundles shouldn't clobber each other.
    bundle_dir = os.path.join(log_dir, "kudu-%s-bundle-%s" % (role_type, uuid.uuid1()))
    staging_dir = os.path.join(bundle_dir, "staging")
    if os.path.exists(bundle_dir):
        shutil.rmtree(bundle_dir)
    os.makedirs(staging_dir, 0755)

    try:
        gather_diagnostics_logs(log_dir, staging_dir)
        if role_type == MASTER:
            do_ksck(staging_dir, master_addrs)
            redact(staging_dir)
        compress_and_link(staging_dir, bundle_dir, process_dir)
    finally:
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)

if __name__ == "__main__":
    main()
