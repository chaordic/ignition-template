#!/bin/bash

# This is a example of "run once" job setup using job-runner.
# For a continuous job, you should create a system service with start, restart, stop, etc

IGNITION_HOME=/mnt/ignition
RESULTS_DIR=$IGNITION_HOME/results

mkdir -p $RESULTS_DIR
# Here we save key files on S3 and access them through IAM, but alternatively they can be hardcoded on this script
export AWS_SECRET_ACCESS_KEY=$(s3cat s3://internal_keyring/ignition_access_key)
export AWS_ACCESS_KEY_ID=$(s3cat s3://internal_keyring/ignition_key_id)
export LANG=C # Reset locale to a neutral state. All jobs must be able to run with this locale
export LC_ALL=C

# Here our root is $IGNITION_HOME/latest
nohup $IGNITION_HOME/latest/scripts/job_runner.py mysetup1 $RESULTS_DIR --disable-vpc --security-group=mysetup1-cluster >& $IGNITION_HOME/mysetup1_job_runner.log &

