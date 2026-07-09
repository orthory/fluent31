//! Test-support binary for the hard-crash recovery suite (`tests/crash_recovery.rs`).
//! NOT part of the library API.
//!
//! Opens a store under the requested sync mode and writes monotonically
//! increasing, zero-padded keys per thread (`crash/<t>/<i>`), reporting each
//! thread's acked count to stdout. The parent lets it make progress, then
//! SIGKILLs it mid-write and reopens the store to verify recovery: under
//! `Always` every acked write survives (the printed counts are a durable lower
//! bound); under all modes the surviving keys form a gapless prefix per thread
//! and the store reopens clean.
//!
//! Args: `<dir> [always|periodic|never] [threads]`.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fluent31::{Db, Options, SyncMode};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).expect("usage: crash_writer <dir> [mode] [threads]").clone();
    let mode = args.get(2).map(|s| s.as_str()).unwrap_or("always");
    let threads: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);

    let sync = match mode {
        "always" => SyncMode::Always,
        "periodic" => SyncMode::Periodic {
            every: Duration::from_millis(20),
        },
        "never" => SyncMode::Never,
        other => panic!("unknown sync mode {other}"),
    };

    let opts = Options {
        sync,
        // small memtable so flushes happen — recovery must span SSTs + WAL
        memtable_size: 64 << 10,
        value_threshold: 128,
        ..Options::default()
    };
    let db = Arc::new(Db::open(&dir, opts).expect("open"));

    let counters: Arc<Vec<AtomicU64>> = Arc::new((0..threads).map(|_| AtomicU64::new(0)).collect());
    for t in 0..threads {
        let db = db.clone();
        let counters = counters.clone();
        std::thread::spawn(move || {
            let mut i = 0u64;
            loop {
                let key = format!("crash/{t}/{i:010}").into_bytes();
                let val = format!("v-{t}-{i}-{}", "p".repeat(50)).into_bytes();
                // Under Always, Ok means fsynced-before-ack, so the counter is a
                // durable watermark. The parent only trusts counts it has seen.
                if db.put(key, val).is_ok() {
                    counters[t].store(i + 1, Ordering::Relaxed);
                    i += 1;
                }
            }
        });
    }

    // Reporter: emit per-thread acked counts until the parent kills us.
    let stdout = std::io::stdout();
    loop {
        std::thread::sleep(Duration::from_millis(3));
        let line = counters
            .iter()
            .map(|c| c.load(Ordering::Relaxed).to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut h = stdout.lock();
        if writeln!(h, "{line}").is_err() {
            // pipe closed (parent stopped reading before the kill) — keep the
            // writer threads going so there is in-flight work at kill time
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = h.flush();
    }
}
