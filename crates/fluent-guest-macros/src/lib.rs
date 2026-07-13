//! Attribute macros that turn a plain typed function into a fluent31 WASM
//! entry point. Deliberately dependency-free: the expansion only needs the
//! function's NAME — all typing (input decoding, output encoding, error →
//! exit code) happens in `fluent_guest::__entry`, where the compiler checks
//! the annotated signature against the `FromInput`/`IntoOutput` traits.
//!
//! The export name IS the module's role (fluentabi v2):
//!
//! ```ignore
//! #[fluent_guest::query]                // exports `query` (read-only)
//! fn top_customers(input: Vec<u8>) -> Result<String, fluent_guest::Fail> { ... }
//!
//! #[fluent_guest::execute]              // exports `execute` (transactional)
//! fn place_order(input: Vec<u8>) -> Result<String, fluent_guest::Fail> { ... }
//!
//! #[fluent_guest::on_touch]             // exports `on_touch`
//! fn index(keys: Vec<Vec<u8>>) -> Result<(), fluent_guest::Fail> { ... }
//!
//! #[fluent_guest::on_apply]             // exports `on_apply`
//! fn feed(changes: Vec<fluent_guest::Change>) -> Result<(), fluent_guest::Fail> { ... }
//! ```

use proc_macro::{TokenStream, TokenTree};

/// Export the annotated `fn name(input: T) -> Result<O, Fail>` as the
/// module's read-only `query` entry point (`Db::query`, GraphQL `wasm`, or
/// the module's own typed Query field).
#[proc_macro_attribute]
pub fn query(attr: TokenStream, item: TokenStream) -> TokenStream {
    entry_attribute(attr, item, "query", "query")
}

/// Export the annotated `fn name(input: T) -> Result<O, Fail>` as the
/// module's transactional `execute` entry point (`Db::execute`, GraphQL
/// `wasmExecute`, or the module's own typed Mutation field).
#[proc_macro_attribute]
pub fn execute(attr: TokenStream, item: TokenStream) -> TokenStream {
    entry_attribute(attr, item, "execute", "execute")
}

/// Export the annotated `fn name(keys: Vec<Vec<u8>>) -> Result<O, Fail>`
/// as the module's `on_touch` entry point — the keys-mode trigger hook
/// receiving the coalesced touched keys.
#[proc_macro_attribute]
pub fn on_touch(attr: TokenStream, item: TokenStream) -> TokenStream {
    entry_attribute(attr, item, "on_touch", "on_touch")
}

/// Export the annotated `fn name(changes: Vec<Change>) -> Result<O, Fail>`
/// as the module's `on_apply` entry point — the changes-mode trigger hook
/// receiving the ordered list of committed changes.
#[proc_macro_attribute]
pub fn on_apply(attr: TokenStream, item: TokenStream) -> TokenStream {
    entry_attribute(attr, item, "on_apply", "on_apply")
}

fn entry_attribute(
    attr: TokenStream,
    item: TokenStream,
    macro_name: &str,
    export: &str,
) -> TokenStream {
    if !attr.is_empty() {
        return compile_error(
            &format!("#[fluent_guest::{macro_name}] takes no arguments"),
            item,
        );
    }
    let Some(name) = fn_name(&item) else {
        return compile_error(
            &format!(
                "#[fluent_guest::{macro_name}] must annotate a function: \
                 fn name(input: T) -> Result<O, fluent_guest::Fail>"
            ),
            item,
        );
    };
    if name == export {
        return compile_error(
            &format!("the annotated function must not be named `{export}` — the macro itself exports that symbol"),
            item,
        );
    }
    let wrapper: TokenStream = format!(
        "#[no_mangle] pub extern \"C\" fn {export}() -> i32 {{ ::fluent_guest::__entry({name}) }}"
    )
    .parse()
    .expect("generated wrapper parses");
    // re-emit the original item unchanged (spans intact), then the wrapper
    let mut out = item;
    out.extend(wrapper);
    out
}

/// The identifier following the first top-level `fn` keyword.
fn fn_name(item: &TokenStream) -> Option<String> {
    let mut saw_fn = false;
    for tt in item.clone() {
        if let TokenTree::Ident(id) = tt {
            let s = id.to_string();
            if saw_fn {
                return Some(s);
            }
            if s == "fn" {
                saw_fn = true;
            }
        }
    }
    None
}

fn compile_error(msg: &str, item: TokenStream) -> TokenStream {
    let mut out: TokenStream = format!("compile_error!({msg:?});").parse().unwrap();
    // keep the item in the output so unrelated errors don't cascade
    out.extend(item);
    out
}
