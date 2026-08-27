<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Tutorial · Human version: https://orthory.github.io/fluent31/#first-trigger -->

# Your first trigger

> Bind a module to a key range and the engine invokes it after every committed write into that range. Derived data stops being the writer's problem.

This step uses a module the repository ships, `guests/customer_index`, which maintains a secondary index over order records:

```
orders/<id, 8 digits>         the record: JSON with a "customer" field
idx/customer/<name>/<id>      the index entry
idx/order/<id>                a back-pointer: what this record was last indexed as
```

## Install and bind

```
fluent31> install customer_index guests/target/wasm32-unknown-unknown/release/customer_index.wasm
fluent31> mktrig customerIndex customer_index orders/ orders0
fluent31> triggers
   customerIndex  customer_index  [orders/, orders0)  keys  pending 0
```

The mode was not chosen — it was detected. The module exports `on_touch`, so the trigger runs in keys mode: it will be handed the keys that were touched and asked to reconcile them.

## Write an order

As an ordinary put. Nothing about this write knows an index exists:

```
fluent31> put orders/00000001 {"customer":"acme"}
fluent31> scan idx/ idx0
   1) "idx/customer/acme/00000001" => ""
   2) "idx/order/00000001" => "acme"
```

Looking up one customer's orders is now a prefix scan of `idx/customer/acme/`. The index is ordinary keys — scannable, forkable and replicable like everything else.

## Change it

```
fluent31> put orders/00000001 {"customer":"globex"}
fluent31> scan idx/ idx0
   1) "idx/customer/globex/00000001" => ""
   2) "idx/order/00000001" => "globex"
```

The stale `acme` entry is gone. A keys-mode event carries the key and nothing else — no old value — so the module found what to remove through its own back-pointer. That is the shape every keys-mode consumer takes: read current state, make the derived state match it, and converge no matter how the events arrive. Deleting the order removes both keys the same way.

## Three things to carry forward

- **It is asynchronous.** Derived state trails the base data by the backlog. If a scan looks stale, the drain has not happened yet; `triggers` reports `pending` and the last error per trigger.
- **Registration does not backfill.** Keys already in the range when the trigger was created fire no events. Existing data is indexed by scanning it deliberately, or by re-writing the range.
- **Trigger writes never fire triggers.** The index entries above generated no events of their own, so cascades cannot loop.

What the engine guarantees in exchange is worth stating plainly: the event was committed in the same atomic batch as the write that caused it, and the module's writes commit together with consuming that event. Derived state built this way cannot drift, even across a crash.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Your first trigger` in *Tutorial*
