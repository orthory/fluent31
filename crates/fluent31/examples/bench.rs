//! Quick-and-dirty throughput probe (not a rigorous benchmark).
use fluent31::{Db, Options, SyncMode};
use std::time::Instant;

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = Options::default();
    opts.sync = SyncMode::Never;
    let db = Db::open(dir.path(), opts).unwrap();

    const N: u32 = 200_000;
    let t = Instant::now();
    for i in 0..N {
        db.put(format!("key{i:08}").into_bytes(), format!("value-{i}").into_bytes())
            .unwrap();
    }
    let d = t.elapsed();
    println!("put   small x{N}: {:>8.0} ops/s", N as f64 / d.as_secs_f64());

    let t = Instant::now();
    for i in (0..N).step_by(7) {
        assert!(db.get(format!("key{i:08}").as_bytes()).unwrap().is_some());
    }
    let d = t.elapsed();
    println!("get   hot   x{}: {:>8.0} ops/s", N / 7, (N / 7) as f64 / d.as_secs_f64());

    db.flush().unwrap();
    db.compact_all().unwrap();
    let t = Instant::now();
    for i in (0..N).step_by(7) {
        assert!(db.get(format!("key{i:08}").as_bytes()).unwrap().is_some());
    }
    let d = t.elapsed();
    println!("get   cold  x{}: {:>8.0} ops/s (post-compaction, cache-warm blocks)", N / 7, (N / 7) as f64 / d.as_secs_f64());

    let t = Instant::now();
    let n = db.iter(None, None, false).unwrap().count();
    let d = t.elapsed();
    println!("scan  full  x{n}: {:>8.0} entries/s", n as f64 / d.as_secs_f64());

    // vlog-resident values
    let t = Instant::now();
    const M: u32 = 20_000;
    for i in 0..M {
        db.put(format!("blob{i:08}").into_bytes(), vec![7u8; 8192]).unwrap();
    }
    let d = t.elapsed();
    println!("put   8KiB  x{M}: {:>8.0} ops/s ({:.0} MiB/s)", M as f64 / d.as_secs_f64(), (M as u64 * 8192) as f64 / (1 << 20) as f64 / d.as_secs_f64());
    let t = Instant::now();
    let n = db.iter(Some(b"blob"), Some(b"bloc"), false).unwrap().count();
    let d = t.elapsed();
    println!("scan  8KiB  x{n}: {:>8.0} entries/s ({:.0} MiB/s, batched vlog reads)", n as f64 / d.as_secs_f64(), (n as u64 * 8192) as f64 / (1 << 20) as f64 / d.as_secs_f64());
}
