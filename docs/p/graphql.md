<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#graphql -->

# GraphQL API

> `POST /graphql` for the primary, `POST /graphql/<instanceId>` for a fork. A `GET` serves GraphiQL; a WebSocket upgrade with the graphql-ws subprotocol serves subscriptions.

## Encoding

Keys and values are raw bytes. Inputs take exactly one of `{text}`, `{base64}` or `{hex}` (`BytesInput`, a `@oneOf` input). Outputs expose `text` (null if not UTF-8), `base64`, `hex` and `len` (`Bytes`).

`U64` is a string-encoded 64-bit unsigned scalar used for seqnos, timestamps and byte totals. Inputs also accept numbers. `Json` is opaque passthrough, used by typed modules only.

## Query

| Field | Notes |
|---|---|
| `get(key: BytesInput!): Bytes` | null when absent |
| `scan(lo, hi, prefix, after, reverse, limit): ScanPage` | `[lo, hi)` or `prefix`; `limit` defaults to 100 and tops out at 10000; `ScanPage { pairs { key value } hasMore nextAfter }`; pass `nextAfter` back as `after` |
| `wasm(module: String!, input: BytesInput): Bytes` | a generic query module call |
| `wasmOnce(wasm: BytesInput!, input: BytesInput): Bytes` | a one-shot query, binary or WAT |
| `modules: [Module!]` | `{ name size typed schemaError }`, current state |
| `stats: Stats` | the `DbStats` fields in camelCase |
| `forks: [Fork!]` | `{ name instanceId createdUnixMs lastSeqno path }` |
| `pins: [Pin!]` | `{ name seqno createdUnixMs }`, oldest first |
| `triggers: [Trigger!]` | `{ name module lo hi mode pending lastError }` |
| `snapshotSeqno: U64` | the seqno this operation reads at |
| `seqno: U64!` | the current visible seqno, not snapshot-bound; pass it to `fork(at:)` to cut "now" deterministically |
| `<module>(...)` | every installed typed `kind: "query"` module |

Every read field of one query operation runs at one pinned snapshot. `stats`, `modules`, `forks`, `pins`, `triggers` and `seqno` report current state.

## Mutation

| Field | Notes |
|---|---|
| `put(key, value)`, `delete(key)` |  |
| `writeBatch(ops: [WriteOp!]!): Int` | `WriteOp` is `@oneOf { put: {key value} \| delete: BytesInput }`; atomic; returns the number of ops applied |
| `wasmExecute(module, input)`, `wasmExecuteOnce(wasm, input)` | executor calls; the one-shot accepts WAT |
| `installModule(name, wasm): Module` | binary (`base64`) or WAT (`text`); hot-swaps the schema |
| `uninstallModule(name)` |  |
| `createTrigger(name, module, lo, hi)`, `deleteTrigger(name)` |  |
| `reloadSchema` | re-describes everything; the resync path after out-of-band installs |
| `fork(name, at: U64): Fork` | omit `at` for the head; returns the new `instanceId` |
| `deleteFork(name)` | refused while in use |
| `pin(name): Pin`, `unpin(name)` |  |
| `syncWal` | a durability barrier, the companion to `--sync periodic` |
| `flush`, `compactAll`, `gcVlog` |  |
| `<module>(...)` | every installed typed `kind: "execute"` module |

Mutation fields run serially in document order, each as an independent atomic write, and executor fields each run their own transaction. A document is never one transaction.

## Subscription

```
subscription {                      # raw plane: no module needed
  changes(lo: {text: "orders/"}, hi: {text: "orders0"}) {
    kind seqno commitSeqno key { text } value { text }
    query { snapshotSeqno get(key: {text: "orders/count"}) { text } }
  }
}
subscription {                      # typed plane: a module with a `feed` descriptor
  orderFeed { kind seqno commitSeqno key { text } event { seqno op id record elided } }
}
```

- `kind` is `ATTACHED`, `PUT` or `DELETE`. The stream opens with one `ATTACHED` marker with no key, value or event. Its `seqno` is the attach boundary: everything at or below it is readable through the marker's `query`, and everything above arrives on the stream. Gap-free, with no overlap.
- Every item carries `query: Query!`, the full Query root pinned at the item's `commitSeqno`, which is the exact state in which the op became visible. The ops of one atomic commit share a `commitSeqno`.
- Typed feeds deliver puts only, so feed GC deletes are invisible. `event` is the written value validated against the declared event type.
- A consumer that falls behind `sub_queue_bytes` is cut off with a "lagged" error. Re-subscribe and re-scan from the new boundary. Items hold snapshots, so consume promptly. A server restart ends every subscription; nothing about them is persisted.
- The idiom: history is a `scan` of the feed range, the latest value is a `get`, and live is a subscription. A disconnected client misses nothing durable as long as the module materializes its feed.

## Errors

Engine failures map to `errors[].extensions.code`: `IO`, `CORRUPTION`, `INVALID_ARGUMENT`, `CONFLICT` (retries exhausted), `CLOSED`, `BACKGROUND`, `WASM`, `GUEST_FAILED` (with `guestExitCode`, `guestOutputBase64`, and `guestOutputText` when the output is UTF-8), `PROVENANCE_MISMATCH`, `GONE`, `JOURNAL_GAP` and `OUTPUT_SCHEMA_VIOLATION` (a typed output mismatch, carrying `committed: true` for executors). Documents are capped at depth 32 and complexity 5000.

Root fields are always outer-nullable, so a failure yields `field: null` plus an `errors` entry rather than a spec-invalid response.

## Instances

`fork(name:) { instanceId }` returns the address of the new branch, and `/graphql/<instanceId>` serves it with the same full surface: its own modules, triggers, schema and forks. Instances open lazily on the first request and close when idle (`fork-idle-ttl-secs`) or when evicted past `fork-max-open`. Forks nest up to 8 deep under one primary. An unknown id is a 404. The id is routing, not authorization.

## Demo

```
cargo run -p fluent-server -- ./data
scripts/demo-orders.sh [endpoint]     # builds the guests, installs placeOrder + topCustomers, seeds, ranks
```

```
mutation { placeOrder(customer: "you", amountCents: "4200") { id customerTotalCents } }
query    { topCustomers(limit: 3) { customer orders totalCents avgCents } }
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `GraphQL API` in *Reference*
