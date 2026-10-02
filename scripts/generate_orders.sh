#!/usr/bin/env bash
# Send sample orders to the API. A few "whale" customers spend enough to
# land in the high-value segment; everyone else places small orders.
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
COUNT="${1:-200}"

post() {
  curl -fsS -o /dev/null -X POST "$API_URL/orders" \
    -H 'Content-Type: application/json' \
    -d "{\"customer_id\":\"$1\",\"amount\":$2}"
}

for i in $(seq 1 "$COUNT"); do
  if (( i % 10 == 0 )); then
    post "whale-$(( RANDOM % 3 + 1 ))" "$(( RANDOM % 3000 + 2000 )).$(( RANDOM % 100 ))"
  else
    post "cust-$(( RANDOM % 50 + 1 ))" "$(( RANDOM % 200 + 5 )).$(( RANDOM % 100 ))"
  fi
done
echo "sent $COUNT orders to $API_URL"
