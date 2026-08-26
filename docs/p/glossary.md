<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#glossary -->

# Glossary

> The words the docs lean on, in one place.

|  |  |
|---|---|
| **changes mode** | the trigger mode that delivers every committed op, in order, to `on_apply` |
| **commit seqno** | the last seqno of an atomic commit; the state in which its ops became visible |
| **cut** | the seqno a fork captures |
| **edge cache** | a replica scoped to a key range |
| **elided** | a changes-mode event whose value exceeded `trigger_inline_value` and arrives key-only |
| **executor** | a module invoked through `execute`, inside a transaction |
| **feed** | a descriptor declaration that makes a changes-mode module's output range a typed subscription |
| **fork** | a named, hard-linked, complete copy of the database at a cut |
| **instance** | a database directory, primary or fork, as addressed by a server; identified by its instance id |
| **join point** | the replication listener that replicas attach to |
| **keys mode** | the trigger mode that delivers coalesced touched keys to `on_touch` |
| **lineage** | a store and the forks and restores descending from it, linked by instance ids |
| **module** | a WASM binary installed in the database |
| **one-shot** | invoking module bytes without installing them |
| **pin** | a durable, named, store-wide GC hold at a seqno |
| **querier** | a module invoked through `query`, read-only at a snapshot |
| **reserved keyspace** | keys starting with `0x00`. Engine state, invisible to users |
| **seqno** | the sequence number of an op, and also the address of a state |
| **store name, identity** | an operator name that maps to a deterministic instance id; required for replication |
| **trigger** | a binding of a module to a key range, invoked after commits into the range |
| **value log, vlog** | append-only files holding values at or above `value_threshold`. The tree holds pointers |
| **WAT** | the WebAssembly text format, accepted wherever module bytes are |
| **watermark** | the oldest registered snapshot; the GC boundary |

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Glossary` in *Reference*
