<!-- Generated from docs/index.html by scripts/build-agent-docs.py. Do not edit. -->

<!-- Section: Tutorial · Human version: https://orthory.github.io/fluent31/#first-module -->

# Your first module

> Install code into the database and call it by name. This is the feature the rest of the engine is arranged around.

Modules build for WebAssembly, so add the target once:

```
rustup target add wasm32-unknown-unknown
```

## The crate

Guest modules live in their own workspace under `guests/`, because they only build for wasm32. Create `guests/count/Cargo.toml` and add `"count"` to the `members` list in `guests/Cargo.toml`:

<!-- guests/count/Cargo.toml -->
```
[package]
name = "count"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
fluent-guest = { path = "../../crates/fluent-guest" }
```

<!-- guests/count/src/lib.rs -->
```
use fluent_guest::Fail;

#[fluent_guest::query]
fn count(prefix: Vec<u8>) -> Result<String, Fail> {
    if prefix.is_empty() {
        return Err(Fail::new(2, "empty prefix"));
    }
    let n = fluent_guest::scan_prefix(&prefix)
        .map_err(|_| Fail::new(3, "scan failed"))?
        .count();
    Ok(n.to_string())
}
```

The attribute is what makes this a module: it exports an entry named `query`, and **the export is the role**. A `query` runs read-only against one pinned snapshot — a write from inside it returns `EROFS`. The function itself may not be called `query`, since that is the name the macro generates. Distinct `Fail` codes are the convention: the caller can tell one failure from another.

## Build and install

```
cargo build --manifest-path guests/Cargo.toml --target wasm32-unknown-unknown --release
# guests/target/wasm32-unknown-unknown/release/count.wasm
```

```
$ cargo run -p fluent-cli -- ./data
fluent31> install count guests/target/wasm32-unknown-unknown/release/count.wasm
fluent31> query count user/
"4"
```

The scan ran inside the database. Nothing but the answer crossed a boundary, and the whole count came from one consistent state — which is what makes it different from looping over a range from the outside.

## Make it an API

A module that describes itself becomes a typed GraphQL field. Add the descriptor and rebuild:

```
fluent_guest::fluent_describe!(r#"{
  "kind": "query",
  "description": "Number of keys under a prefix.",
  "output": "String!"
}"#);
```

Install it through the server this time — `installModule` runs `describe` and rejects a descriptor that does not hold up:

```
mutation Install($w: BytesInput!) {
  installModule(name: "countKeys", wasm: $w) { typed schemaError }
}
# variables: {"w": {"base64": "<base64 of count.wasm>"}}
# -> { "typed": true, "schemaError": null }
```

The schema was rebuilt and hot-swapped while the server kept running. Reload GraphiQL and the field is there, documented:

```
query { countKeys(input: {text: "user/"}) }
```

The field is named after *the name you installed under*, not the crate — `countKeys`, not `count`. A descriptor that declares `args` gets typed arguments instead of the raw `input`, and one that declares an `output` object gets a selectable result type.

One last thing worth knowing before moving on: module bytes are stored in the database as ordinary versioned keys. Your code is written durably with the data, recovered with it, copied into every fork of it, and readable at a past sequence number alongside it.

---

fluent31 docs · [index](../llms.txt) · [all pages](https://orthory.github.io/fluent31/llms.txt) · this page is `Your first module` in *Tutorial*
