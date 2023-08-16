#!/usr/bin/env python3

#
# This is an example of how you can parse through the job history files.
#
# This script will return a JSON string (pipe it to `jq` to make it pretty),
#   where the keys are the user IDs and the values are the number of jobs they've run
#   over the entire history of the job history files.
#
# There's a LOT of data in production, so expect this will take a while to run.
#

import os
import argparse
import pathlib
import json
import sys

parser = argparse.ArgumentParser(description='Process job history files from T1')
parser.add_argument('job_datapath', help="Path to the `data` directory of the job history files")
args = parser.parse_args()

# This should work with relative or absolute paths
job_datapath = pathlib.Path(args.job_datapath)
if not job_datapath.exists():
    print(f"Path does not exist: {job_datapath}")
    exit(1)

users = {}

for filename in os.listdir(job_datapath):
    # job_datapath looks like:  ./data  or  /gpfs/system/slurm/job_data
    # filename looks like:      2020-01-01.json
    # filepath would be:        /gpfs/system/slurm/job_data/2020-01-01.json
    filepath = job_datapath / filename

    # print to stderr for progress so we can pipe stdout to `jq`
    print("Processing {}".format(filepath), file=sys.stderr) 

    # load in the data from the file
    with open(filepath, 'r') as f:
        day_data = json.load(f)

    for job in day_data["jobs"]:
        if job.get("user_id", None) is not None:
            if not job["user_id"] in users:
                users[job["user_id"]] = 0
            users[job["user_id"]] += 1

# print to stdout in JSON format so we can pipe to `jq` or other scripts
print(json.dumps(users))
