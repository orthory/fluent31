<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Reference · Human version: https://orthory.github.io/fluent31/#testing -->

# Testing

> One workspace suite, plus fault injection, endurance and benches.

```
cargo test --workspace                              # engine model tests, group commit, wasm, graphql,
                                                    # server e2e, replication e2e, durability suites
cargo test -p fluent31 --features fault-injection   # fsync failure / ENOSPC / read-fault paths
cargo test --test backup_and_soak -- --ignored      # endurance soak
cargo check -p fluent31 --no-default-features       # the engine without the WASM layer
cargo run --release -p fluent31 --example bench     # throughput probe
cargo run --release -p fluent31 --example gc_bench -- [threads] [always|never] [ops-per-thread] [txn]
```

## Suites worth knowing by name

`engine` (a randomized model test against a `BTreeMap` with interleaved flush, compaction, GC and reopen), `crash_recovery` (a SIGKILLed child), `fault_injection`, `corruption_fuzz`, `journal_rebuild`, `durability_modes`, `group_commit`, `fork_stress` (forks under concurrent writers, flush, compaction and GC), `trigger_changes`, `trigger_robustness`, `wasm` and `wasm_sandbox`. `fluent-graphql/tests/graphql.rs` has WAT fixtures for modules, including `describe`; `fluent-server/tests/server.rs` and `fluent-replication/tests/replication.rs` are the end-to-end suites.

To test your own modules, look at the GraphQL suite's WAT fixtures for minimal modules. For executors, spawn N concurrent calls and assert no lost updates. Restart the server and assert the typed field reappears.

## Under Docker

```
docker run --security-opt seccomp=unconfined -v $PWD:/src -w /src rust:1 \
  sh -c "rustup target add wasm32-unknown-unknown && cargo test --workspace"
```

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Testing` in *Reference*
