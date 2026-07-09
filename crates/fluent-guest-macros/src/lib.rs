//! Attribute macros that turn a plain typed function into a fluent31 WASM
//! entry point. Deliberately dependency-free: the expansion only needs the
//! function's NAME — all typing (input decoding, output encoding, error →
//! exit code) happens in `fluent_guest::__entry`, where the compiler checks
//! the annotated signature against the `FromInput`/`IntoOutput` traits.
//!
//! ```ignore
//! #[fluent_guest::main]                 // exports `run`
//! fn top_customers(input: Vec<u8>) -> Result<String, fluent_guest::Fail> { ... }
//!
//! #[fluent_guest::on_apply]             // exports `on_apply`
//! fn feed(changes: Vec<fluent_guest::Change>) -> Result<(), fluent_guest::Fail> { ... }
//! ```

use proc_macro::{TokenStream, TokenTree};

/// Export the annotated `fn name(input: T) -> Result<O, Fail>` as the
/// module's `run` entry point (query and executor invocations).
#[proc_macro_attribute]
pub fn main(attr: TokenStream, item: TokenStream) -> TokenStream {
    entry_attribute(attr, item, "main", "run")
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
