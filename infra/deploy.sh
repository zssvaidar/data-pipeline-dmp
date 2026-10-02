#!/usr/bin/env bash
# Build and deploy the stack, then upload the Glue job scripts.
# Usage: STACK_NAME=dmp AWS_REGION=us-east-1 ./deploy.sh [extra sam parameter overrides]
set -euo pipefail
cd "$(dirname "$0")"

STACK="${STACK_NAME:-dmp}"
export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

sam build --template template.yaml
sam deploy \
  --stack-name "$STACK" \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --no-confirm-changeset \
  --no-fail-on-empty-changeset \
  ${*:+--parameter-overrides "$@"}

output() {
  aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
BUCKET="$(output DataLakeBucket)"

# The Glue job reads its script and the shared transform from the lake bucket.
aws s3 cp ../etl/jobs/glue_order_events.py "s3://$BUCKET/_glue/jobs/glue_order_events.py"
aws s3 cp ../etl/jobs/order_events_etl.py "s3://$BUCKET/_glue/jobs/order_events_etl.py"

aws cloudformation describe-stacks --stack-name "$STACK" --query "Stacks[0].Outputs" --output table
