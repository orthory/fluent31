<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Start here · Human version: https://orthory.github.io/fluent31/#installation -->

# Install

> Stable Rust, one optional wasm target, two cargo features.

You need stable Rust (2021 edition). For modules, add the wasm target:

```
rustup target add wasm32-unknown-unknown
```

Build and test the workspace:

```
cargo build --workspace --release
cargo test --workspace
```

The example modules live in a separate workspace under `guests/` and only build for wasm32:

```
cargo build --manifest-path guests/Cargo.toml --target wasm32-unknown-unknown --release
# artifacts: guests/target/wasm32-unknown-unknown/release/<name>.wasm
```

> **Note** If your `cargo` is not rustup's, point it at rustup's rustc so the wasm32 standard library is found: `RUSTC="$(rustup which rustc)" cargo build …`

## Cargo features

| Feature | Default | Effect |
|---|---|---|
| `wasm` | on | The WASM layer (wasmtime). `--no-default-features` builds the pure storage engine; module and trigger APIs do not exist. |
| `fault-injection` | off | A test seam that exposes the IO traits and `Db::open_with_io`. Never enable it in production. |

## Platforms

Linux, where io_uring is probed automatically with a fallback to portable IO, and macOS with portable IO. Docker's default seccomp profile blocks io_uring, so run with `--security-opt seccomp=unconfined` or set `io_backend = Std`.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Install` in *Start here*
