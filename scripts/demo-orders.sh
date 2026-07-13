#!/usr/bin/env bash
# Demo: install the typed placeOrder/topCustomers WASM pair and seed sample
# orders through GraphQL. Run `cargo run -p fluent-server -- <dir>` (or
# `cargo run -p fluent-graphql -- <dir>` for the GraphQL plane alone) first.
#
#   scripts/demo-orders.sh [endpoint]     default http://127.0.0.1:8317/graphql
set -euo pipefail

URL="${1:-http://127.0.0.1:8317/graphql}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WASM_DIR="$ROOT/guests/target/wasm32-unknown-unknown/release"

gql() { # gql <query> [variables-json]
  local q v body
  q=$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1")
  v="${2:-}"
  [ -n "$v" ] || v='{}'
  body=$(mktemp)
  printf '{"query":%s,"variables":%s}' "$q" "$v" > "$body"
  curl -sS "$URL" -H 'content-type: application/json' --data-binary @"$body"
  rm -f "$body"
}

check() { # check <label> <response-json>
  echo "$2" | python3 -c '
import json, sys
label = sys.argv[1]
r = json.load(sys.stdin)
errs = r.get("errors")
if errs:
    sys.stderr.write(label + ": ERROR " + errs[0]["message"] + "\n")
    sys.exit(1)
print(label + ": " + json.dumps(r["data"]))' "$1"
}

echo "== building demo guests (wasm32) =="
# only pin RUSTC when rustup exists: an empty RUSTC breaks cargo, and a
# non-rustup toolchain may already carry the wasm32 target
if RUSTC_PATH="$(rustup which rustc 2>/dev/null)" && [ -n "$RUSTC_PATH" ]; then
  RUSTC="$RUSTC_PATH" cargo build --manifest-path "$ROOT/guests/Cargo.toml" \
    --target wasm32-unknown-unknown --release --target-dir "$ROOT/guests/target"
else
  cargo build --manifest-path "$ROOT/guests/Cargo.toml" \
    --target wasm32-unknown-unknown --release --target-dir "$ROOT/guests/target"
fi

echo "== installing typed modules =="
PO=$(base64 < "$WASM_DIR/place_order.wasm" | tr -d '\n')
TC=$(base64 < "$WASM_DIR/top_customers.wasm" | tr -d '\n')
VARS=$(printf '{"po":{"base64":"%s"},"tc":{"base64":"%s"}}' "$PO" "$TC")
check install "$(gql '
  mutation I($po: BytesInput!, $tc: BytesInput!) {
    placeOrder: installModule(name: "placeOrder", wasm: $po) { typed schemaError }
    topCustomers: installModule(name: "topCustomers", wasm: $tc) { typed schemaError }
  }' "$VARS")"

echo "== seeding orders =="
seed() { # seed <customer> <cents> [note]
  local query
  query=$(printf 'mutation { placeOrder(customer: "%s", amountCents: "%s"%s) { id customerOrders customerTotalCents } }' \
    "$1" "$2" "$([ $# -ge 3 ] && printf ', note: "%s"' "$3")")
  check "order $1/$2" "$(gql "$query")"
}
seed acme    12900 "annual plan"
seed acme     4900
seed acme     9900 "expansion seats"
seed zenith  25000 "enterprise pilot"
seed zenith   1500
seed nimbus   7900
seed nimbus   7900 "renewal"
seed orbit      99 "trial upgrade"

echo "== top customers =="
check report "$(gql '{
  topCustomers(limit: 5) { customer orders totalCents avgCents }
  bigSpenders: topCustomers(minTotalCents: "20000") { customer totalCents }
  stats { visibleSeqno }
}')"

echo
echo "Open GraphiQL and try it yourself: ${URL%/graphql}/"
echo '  mutation { placeOrder(customer: "you", amountCents: "4200") { id customerTotalCents } }'
echo '  query { topCustomers(limit: 3) { customer totalCents avgCents } }'
