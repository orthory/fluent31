//! Shared plumbing for the runnable examples: build the guest workspace
//! for wasm32 (same recipe as the test suites), wait for trigger queues to
//! drain, and small print helpers. Not an example itself — each example
//! pulls it in via `#[path = "util/mod.rs"] mod util;`.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Once;
use std::time::{Duration, Instant};

use fluent31::Db;

fn workspace_root() -> PathBuf {
    // crates/fluent31 -> workspace root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Build the guest crates once per process and read the named artifact.
/// The wasm32 std lives in the rustup toolchain, so point cargo at
/// rustup's rustc when available.
pub fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILD: Once = Once::new();
    let root = workspace_root();
    BUILD.call_once(|| {
        let mut cmd = std::process::Command::new("cargo");
        if let Ok(out) = std::process::Command::new("rustup")
            .args(["which", "rustc"])
            .output()
        {
            if out.status.success() {
                let rustc = String::from_utf8_lossy(&out.stdout).trim().to_string();
                cmd.env("RUSTC", rustc);
            }
        }
        let status = cmd
            .args([
                "build",
                "--manifest-path",
                root.join("guests/Cargo.toml").to_str().unwrap(),
                "--target",
                "wasm32-unknown-unknown",
                "--release",
                "--target-dir",
                root.join("guests/target").to_str().unwrap(),
            ])
            .env_remove("CARGO_TARGET_DIR")
            .status()
            .expect("cargo build for guests");
        assert!(status.success(), "guest build failed");
    });
    std::fs::read(
        root.join("guests/target/wasm32-unknown-unknown/release")
            .join(format!("{name}.wasm")),
    )
    .expect("guest artifact")
}

/// Wait until every registered trigger queue is empty; a drain error is an
/// example bug — surface it instead of spinning forever.
pub fn drain(db: &Db) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let triggers = db.list_triggers().expect("list_triggers");
        if let Some(err) = triggers.iter().find_map(|t| t.last_error.clone()) {
            panic!("trigger failed: {err}");
        }
        if triggers.iter().all(|t| t.pending == 0) {
            return;
        }
        assert!(Instant::now() < deadline, "triggers did not drain in 30s");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// All keys under a prefix, as lossy strings, in order.
pub fn keys(db: &Db, prefix: &str) -> Vec<String> {
    let mut hi = prefix.as_bytes().to_vec();
    *hi.last_mut().unwrap() += 1;
    db.iter(Some(prefix.as_bytes()), Some(&hi), false)
        .expect("iter")
        .map(|kv| String::from_utf8_lossy(&kv.expect("kv").0).into_owned())
        .collect()
}

/// All (key, value) pairs under a prefix, as lossy strings, in order.
pub fn pairs(db: &Db, prefix: &str) -> Vec<(String, String)> {
    let mut hi = prefix.as_bytes().to_vec();
    *hi.last_mut().unwrap() += 1;
    db.iter(Some(prefix.as_bytes()), Some(&hi), false)
        .expect("iter")
        .map(|kv| {
            let (k, v) = kv.expect("kv");
            (
                String::from_utf8_lossy(&k).into_owned(),
                String::from_utf8_lossy(&v).into_owned(),
            )
        })
        .collect()
}

/// Print every key under a prefix (or note emptiness), with a blank line.
pub fn show(db: &Db, prefix: &str) {
    let found = keys(db, prefix);
    if found.is_empty() {
        println!("   ({prefix}* is empty)\n");
        return;
    }
    for k in found {
        println!("   {k}");
    }
    println!();
}

pub fn put(db: &Db, k: &str, v: &str) {
    db.put(k.as_bytes().to_vec(), v.as_bytes().to_vec()).expect("put");
}
