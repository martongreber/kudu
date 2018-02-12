#!/usr/bin/env python

"""
This script collects information for inclusion in a Cloudera Manager support
bundle. The steps are:

1. Collect all information to the staging directory.
2. Redact (TODO).
3. Compress the redacted information into a .tar.gz.
4. Check that the result does not exceed CM's per-role size limit for service diagnostics.
5. Create a symlink pointing to the .tar.gz from a specially-named file in the CM
   runner's process directory.

The diagnostics are stored in a diagnostics directory inside the log directory. If there is a fatal
error gathering diagnostics, the diagnostics directory is deleted. If diagnostics gathering
succeeds, any staging information is deleted and only the diagnostics compressed archive remains.

Right now, the only diagnostic gathered is ksck.
"""

import argparse
import json
import os.path
import re
import shutil
import subprocess
import uuid

# The maximum size in bytes of additional diagnostics permitted by CM.
MAX_DIAG_SIZE = 10 * 1024 * 1024 # 10 MiB
EXCEEDS_MAX_SIZE_MSG = "The Kudu diagnostics were deleted: they exceeded the maximum size (%d/%d)"

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
    if diag_size > MAX_DIAG_SIZE:
        msg = EXCEEDS_MAX_SIZE_MSG % (diag_size, MAX_DIAG_SIZE)
        print msg
        with open(os.path.join(process_dir, "error"), 'w') as error_out:
            error_out.write("%s\n" % msg)
        tarball = os.path.join(process_dir, "error.tar.gz")
        subprocess.check_call(["tar", "zcvf", tarball, "-C", process_dir, "error"])
        shutil.rmtree(diag_dir)

    subprocess.check_call(["ln", "-s", tarball, os.path.join(process_dir, "output.gz")])

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_dir", help="the Kudu role's log directory")
    parser.add_argument("process_dir", help="the script runner's process directory")
    parser.add_argument("master_addrs", help="comma-separated list of master addresses")
    args = parser.parse_args()
    return args.log_dir, args.process_dir, args.master_addrs

def main():
    log_dir, process_dir, master_addrs = setup_args()

    # Add a UUID to the diagnostics dir because if two Kudu role instances share
    # a log dir, their diagnostics shouldn't clobber each other.
    # TODO(wdberkeley): Would be better to have info identifying the role instance.
    diag_dir = os.path.join(log_dir, "kudu-diagnostics-%s" % uuid.uuid1())
    staging_dir = os.path.join(diag_dir, "staging")
    if os.path.exists(diag_dir):
        shutil.rmtree(diag_dir)
    os.makedirs(staging_dir, 0755)

    try:
        do_ksck(staging_dir, master_addrs)
        redact(staging_dir)
        compress_and_link(staging_dir, diag_dir, process_dir)
    except:
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
        raise

if __name__ == "__main__":
    main()
