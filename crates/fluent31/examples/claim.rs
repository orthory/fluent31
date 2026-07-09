//! Atomic unique-claim under real concurrency — the schema-free UNIQUE
//! constraint, enforced by the `guests/claim` executor through the
//! engine's OCC loop: eight racers claim the same username at once,
//! exactly one wins, everyone else gets a clean attributable failure, and
//! re-claiming your own name stays idempotent.
//!
//! ```sh
//! cargo run -p fluent31 --example claim
//! ```

#[path = "util/mod.rs"]
mod util;

use std::sync::{Arc, Barrier};

use fluent31::{Db, Error, Options, SyncMode};
use util::guest_wasm;

const RACERS: usize = 8;

fn main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Arc::new(
        Db::open(
            dir.path(),
            Options {
                sync: SyncMode::Never,
                ..Options::default()
            },
        )
        .expect("open"),
    );
    db.install_module("claim", &guest_wasm("claim")).expect("install");

    println!("== {RACERS} racers claim username \"neo\" concurrently");
    let barrier = Arc::new(Barrier::new(RACERS));
    let handles: Vec<_> = (0..RACERS)
        .map(|i| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let input = format!(r#"{{"username":"neo","owner":"racer-{i}"}}"#);
                barrier.wait();
                (i, db.execute("claim", input.as_bytes()))
            })
        })
        .collect();

    let mut winner: Option<usize> = None;
    let mut losses = 0;
    for h in handles {
        match h.join().expect("join") {
            (i, Ok(out)) => {
                println!("   racer-{i} WON: {}", String::from_utf8_lossy(&out));
                assert!(winner.replace(i).is_none(), "two winners — OCC failed us");
            }
            (i, Err(Error::GuestFailed { code: 1, output })) => {
                losses += 1;
                if losses == 1 {
                    println!("   racer-{i} lost: {}", String::from_utf8_lossy(&output));
                }
            }
            (i, Err(e)) => panic!("racer-{i} unexpected error: {e}"),
        }
    }
    let winner = winner.expect("someone must win");
    println!("   ({losses} racers rejected with exit code 1)\n");
    assert_eq!(losses, RACERS - 1);
    let holder = db.get(b"uname/neo").expect("get").expect("claimed");
    assert_eq!(holder, format!("racer-{winner}").into_bytes());
    println!("== the store agrees: uname/neo -> {}", String::from_utf8_lossy(&holder));

    println!("== the winner re-claims: idempotent success, not an error");
    let again = format!(r#"{{"username":"neo","owner":"racer-{winner}"}}"#);
    let out = db.execute("claim", again.as_bytes()).expect("re-claim");
    println!("   {}", String::from_utf8_lossy(&out));
    assert!(String::from_utf8_lossy(&out).contains(r#""already":true"#));

    println!("== a different name is free for anyone");
    let out = db
        .execute("claim", br#"{"username":"trinity","owner":"racer-3"}"#)
        .expect("fresh claim");
    println!("   {}", String::from_utf8_lossy(&out));
    println!("done: one name, one owner, no lost updates.");
}
