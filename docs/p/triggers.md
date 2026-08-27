<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#triggers -->

# Triggers

> Bind a module to a key range and the engine invokes it after every committed write into the range: indexes, views, feeds, cascades.

## Registering

```
db.create_trigger("name", "module", Some(b"orders/"), Some(b"orders0"))?;   // Rust
```

```
mktrig NAME MODULE [LO|-] [HI|-]            # shell; - = open end
```

```
mutation { createTrigger(name: "idx", module: "customer_index",
                         lo: {text: "orders/"}, hi: {text: "orders0"}) }
query { triggers { name module lo { text } hi { text } mode pending lastError } }
mutation { deleteTrigger(name: "idx") }     # discards pending events
```

`None` bounds mean an open end. The module must already be installed. Names follow the module-name rules and must be unique. `lo >= hi`, or a bound longer than `max_key_size`, is rejected. One module may back many triggers.

The mode is detected from the module's exports at registration and fixed for the trigger's life: `on_apply` present means changes mode; otherwise `on_touch` means keys mode; neither is rejected. Replacing the module's bytes later does not change the mode. A changes-mode trigger whose module lost `on_apply` fails its drains loudly (`lastError`) and holds its events.

> **No backfill.** Keys already in the range fire no events. To index existing data, have the module scan on demand (as `guests/dynamic_index` does when a spec key is written) or re-put the range with a one-shot executor.

Each trigger has its own queue, and one runner thread drains them independently. There is no ordering between triggers, and overlapping ranges each get their own copy of an event.

## Keys mode (on_touch)

The input is the touched keys, up to `trigger_batch` per invocation. No values, no op kind, no order. Re-touches of one key coalesce into one pending event while a backlog exists.

**The contract:** an event means "reconcile this key". Read the key at your snapshot. If it is present, upsert your derived state; if it is absent, remove it. Written this way the module converges under replay, coalescing and reordering. Updates and deletes need your own back-pointer (say `idx/order/<id>` pointing at the customer) to find the stale entry, because the event carries no old value. `guests/customer_index` is the reference.

## Changes mode (on_apply)

The input is the ordered list of committed changes, one per op, up to `trigger_batch` per invocation. Each carries `seqno` (the op's own seqno, assigned at commit, unique and strictly increasing across the feed), the kind (put, delete, or put with the value elided), the key, and the value inline up to `trigger_inline_value` (64 KiB). Above that, `value` is `None`, and you read the key knowing the read is current state, possibly newer than the change.

**The contract:** one event per op, in commit order, never coalesced. A key written three times yields three changes. Filter in code; the range is only the coarse cut. Derive output keys from the seqno (`feed/<seqno zero-padded>`) so that replays overwrite instead of duplicating. Old values are still your job. A hot key grows the backlog where keys mode would coalesce it. The references are `guests/order_feed`, `guests/live_stats`, `guests/dynamic_index` and `guests/cascade_delete`.

## Delivery guarantees

- **Durable capture.** Events commit in the same atomic batch as the write that caused them. A write that survives a crash fires its trigger after recovery; one that doesn't, doesn't.
- **At-least-once invocation, exactly-once effects.** Consumed events are deleted inside the module's own transaction. A crash or a conflict re-runs the whole attempt, and your writes and the events' consumption are inseparable.
- **No stacking.** Writes made by a trigger invocation never generate events, for any trigger. No chains, no loops.
- **Asynchronous.** Derived state trails the base data by the backlog. Nothing waits for a trigger. Watch `pending` to see how far behind it is.
- **Failure holds, never drops.** A failing module (a guest error, a missing module, conflict exhaustion) leaves the batch queued. The runner backs off per trigger, starting at 100 ms and doubling up to a 6.4 s ceiling. `list_triggers` and `triggers { pending lastError }` show the depth and the reason. Fix the module by reinstalling it and the backlog drains.
- **Batch bounds.** A drain hands the module at most `trigger_batch` events and never more than `max_wasm_input` bytes. Inlined values are clamped so that every event fits.
- Trigger definitions and queues live in the reserved keyspace, so they are versioned, recovered and forked with everything else. A store rebuilt from the journal has neither, so recreate the triggers.

Every writer fires triggers: plain puts, batches, transactions, executors, one-shot executors, and every network surface. Trigger invocations themselves, value-log GC relocations, and writes made while `wasm_enabled = false` do not.

## Waiting for a drain

There is no synchronous "run the triggers now". Poll `list_triggers()` until `pending == 0` for the triggers you care about and `last_error` is `None`. This is the reference loop (`crates/fluent31/examples/util/mod.rs::drain`):

```
let deadline = Instant::now() + Duration::from_secs(30);
loop {
    let triggers = db.list_triggers()?;
    if let Some(err) = triggers.iter().find_map(|t| t.last_error.clone()) {
        panic!("trigger failed: {err}");          // a failing module never clears itself
    }
    if triggers.iter().all(|t| t.pending == 0) { break; }
    assert!(Instant::now() < deadline, "triggers did not drain in 30s");
    std::thread::sleep(Duration::from_millis(10));
}
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Triggers` in *Reference*
