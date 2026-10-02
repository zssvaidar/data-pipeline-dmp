#!/usr/bin/env bash
# Run the nightly batch now instead of waiting for the 02:00 UTC schedule:
# crawler -> (trigger) Glue ETL job -> (EventBridge) activation Lambda.
# Send orders first and wait for the Firehose buffer to flush (FirehoseBufferSeconds).
set -euo pipefail

STACK="${STACK_NAME:-dmp}"
export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

output() {
  aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
CRAWLER="$(output CrawlerName)"
JOB="$(output EtlJobName)"
BUCKET="$(output DataLakeBucket)"

wait_for() { # description, command printing a state, terminal states...
  local what=$1 cmd=$2; shift 2
  local state
  while :; do
    state="$(eval "$cmd")"
    echo "  $what: $state"
    for s in "$@"; do [[ "$state" == "$s" ]] && return 0; done
    sleep 15
  done
}

latest_run() {
  aws glue get-job-runs --job-name "$JOB" --max-items 1 --query "JobRuns[0].Id" --output text
}
previous_run="$(latest_run)"

echo "starting crawler $CRAWLER"
aws glue start-crawler --name "$CRAWLER"
wait_for "crawler" "aws glue get-crawler --name '$CRAWLER' --query Crawler.State --output text" READY
last="$(aws glue get-crawler --name "$CRAWLER" --query Crawler.LastCrawl.Status --output text)"
[[ "$last" == SUCCEEDED ]] || { echo "crawler finished with $last"; exit 1; }

echo "waiting for the trigger to start $JOB"
run_id="$(latest_run)"
while [[ "$run_id" == "$previous_run" ]]; do
  sleep 10
  run_id="$(latest_run)"
done
wait_for "job $run_id" "aws glue get-job-run --job-name '$JOB' --run-id '$run_id' --query JobRun.JobRunState --output text" \
  SUCCEEDED FAILED TIMEOUT STOPPED ERROR

echo "activation Lambda runs on job success; segment output:"
key="activation/high_value_customers/dt=$(date -u +%F)/segment.json"
for _ in $(seq 1 20); do
  if aws s3 cp "s3://$BUCKET/$key" - 2>/dev/null; then exit 0; fi
  sleep 15
done
echo "no segment at s3://$BUCKET/$key yet; check the ActivateFunction logs"
exit 1
