#!/usr/bin/env bash
# Delete the stack and ALL its data (the lake bucket is emptied first).
set -euo pipefail
cd "$(dirname "$0")"

STACK="${STACK_NAME:-dmp}"
export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

BUCKET="$(aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='DataLakeBucket'].OutputValue" --output text)"
read -r -p "Delete stack '$STACK' and everything in s3://$BUCKET? [y/N] " ok
[[ "$ok" == y || "$ok" == Y ]] || exit 1

aws s3 rm "s3://$BUCKET" --recursive
sam delete --stack-name "$STACK" --no-prompts
