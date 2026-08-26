<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Extending with WASM · Human version: https://orthory.github.io/fluent31/#wasm-typed -->

# Typed GraphQL fields

> A module that exports `describe` becomes its own root field the moment it is installed. No schema file, no resolver, no server restart.

`kind: "query"` lands the field on `Query`, `kind: "execute"` on `Mutation`, and a `feed` declaration on `Subscription`. The schema is rebuilt and hot-swapped on every install and uninstall, at server start, and on `mutation { reloadSchema }` — so the API changes with the code that backs it.

## The descriptor

```
fluent_guest::fluent_describe!(r#"{
  "kind": "execute",
  "description": "docs for the root field",
  "args": [{"name": "customer", "type": "String!"},
           {"name": "amountCents", "type": "U64!"},
           {"name": "note", "type": "String"}],
  "types": [{"name": "PlacedOrder", "fields": [
    {"name": "id", "type": "U64!"},
    {"name": "customerTotalCents", "type": "U64!"}]}],
  "output": "PlacedOrder!",
  "feed": {"prefix": "feed/", "event": "OrderFeedEntry!"}
}"#);
```

It is a static string, evaluated by running the export with an empty input, so it cannot depend on the data. Read one back with `db.describe_module("name")`, or with `db.describe_wasm(&bytes)` to inspect a module before installing it; both return `None` when the module does not export `describe`.

## The type grammar

- Scalars are `String`, `Int` (32-bit), `Float`, `Boolean`, `U64` (a string on the wire, with numbers accepted on input) and `Json` (opaque). At most one list level.
- `args` reference scalars only. `output` and the fields of `types` may also reference types declared in the same descriptor.
- `!` marks non-null, on args, on fields and on the output.
- Limits: 32 types, 64 fields per type, 16 args, and a 64 KiB descriptor.

## Shape rules

- At least one of `kind` and `feed` must be present.
- `output` is required with `kind` and rejected without it; `args` require `kind`.
- A trigger-only module declares just `feed` plus `types` — no `kind`, no `output`.
- The feed's `event` type must be one of the declared `types`.
- Every declaration must be backed by its export: `"query"` by `query`, `"execute"` by `execute`, `feed` by `on_apply`. Otherwise the install is rejected.

## How arguments reach the module

With `args`, the entry receives one JSON object holding every declared argument, with omitted optional ones as `null` and `U64` as a number. Without `args`, the field takes an optional `input: BytesInput` and the entry receives raw bytes.

Only the GraphQL layer builds that object. Through the shell or `db.execute`, a typed module receives whatever bytes you hand it — so call it with the same JSON object yourself:

```
exec placeOrder {"customer":"acme","amountCents":1250,"note":null}
```

## How output is validated

The output is parsed as JSON and checked against `output`. Undeclared keys are dropped. Missing declared fields become `null`, which is an error if the field is `!`. A violation surfaces as `OUTPUT_SCHEMA_VIOLATION`, and for an executor it carries `committed: true` — the transaction has already committed and only the response failed to typecheck. Never blind-retry that one.

## Naming

**The field is named exactly what you install the module as**; the crate name is irrelevant. The reference modules are installed under camelCase names — `scripts/demo-orders.sh` installs `place_order.wasm` as `placeOrder` and `top_customers.wasm` as `topCustomers`, and those are the field names.

The name must be a valid GraphQL name and must not shadow a built-in root field. Type names must not be reserved and must not collide with another module's, so prefix yours: `PlacedOrder`, not `Order`.

## Install and confirm

```
mutation($w: BytesInput!) {
  installModule(name: "placeOrder", wasm: $w) { name typed schemaError }
}
```

The server's `installModule` runs `describe` and rejects a descriptor that does not hold up. The engine's `install_module` — Rust and the shell — checks only the exports, so a module installed that way with a bad descriptor ends up degraded: still callable through the generic byte fields, but with no typed field. `modules { name typed schemaError }` says why, and `reloadSchema` is the resync after an out-of-band install.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Typed GraphQL fields` in *Extending with WASM*
