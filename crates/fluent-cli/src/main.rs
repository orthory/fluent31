//! Interactive dev shell for fluent31 — redis-cli style, every command
//! reports its wall-clock latency.
//!
//! Usage: fluent-cli <db-dir> [--std|--uring] [--nosync] [--sync-every <ms>]
//!        fluent-cli journal-rebuild <journal-dir> <dest-dir>
//!
//! `journal-rebuild` is a one-shot mode (no shell): reconstruct a fresh
//! store at <dest-dir> from a mutation journal (fluent31::journal).
//!
//! Byte arguments accept plain UTF-8 or `hex:DEADBEEF`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use fluent31::{Db, Error, IoBackend, Options, Snapshot, SyncMode, Txn};

const USAGE: &str = "\
usage: fluent-cli <db-dir> [--std|--uring] [--nosync] [--sync-every <ms>]
       fluent-cli journal-rebuild <journal-dir> <dest-dir>";

fn parse_bytes(tok: &str) -> Result<Vec<u8>, String> {
    if let Some(hex) = tok.strip_prefix("hex:") {
        if hex.len() % 2 != 0 {
            return Err("odd hex length".into());
        }
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|e| e.to_string()))
            .collect()
    } else {
        Ok(tok.as_bytes().to_vec())
    }
}

fn fmt_bytes(v: &[u8]) -> String {
    const MAX: usize = 160;
    let printable = v
        .iter()
        .all(|&b| (0x20..0x7f).contains(&b) || b == b'\n' || b == b'\t');
    let body = if printable && !v.is_empty() {
        format!("\"{}\"", String::from_utf8_lossy(&v[..v.len().min(MAX)]))
    } else {
        let hexed: String = v
            .iter()
            .take(MAX / 2)
            .map(|b| format!("{b:02x}"))
            .collect();
        format!("hex:{hexed}")
    };
    if v.len() > MAX {
        format!("{body}… ({} bytes)", v.len())
    } else {
        body
    }
}

fn fmt_latency(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us >= 1000.0 {
        format!("({:.2} ms)", us / 1000.0)
    } else {
        format!("({us:.1} µs)")
    }
}

struct Shell {
    db: Db,
    txn: Option<Txn>,
    snaps: HashMap<u32, Snapshot>,
    next_snap: u32,
}

const HELP: &str = "\
kv        get K | put K V | del K | scan [LO|-] [HI|-] [--rev] [--limit N] | count [LO] [HI]
txn       begin | tget K | tput K V | tdel K | tlock K (get_for_update) | commit | abort
snapshots snap | snaps | sget ID K | snapdrop ID
wasm      install NAME FILE.wasm | modules | uninstall NAME
          query NAME [INPUT] | exec NAME [INPUT]
forks     fork NAME [AT] | forks | delfork NAME
pins      pin NAME | pins | unpin NAME (pinned seqnos stay fork-able)
          seqno (current visible seqno — the AT address of \"now\")
triggers  mktrig NAME MODULE [LO|-] [HI|-] | deltrig NAME | triggers
admin     flush | compact | gc | stats | help | exit
bytes     plain utf-8 or hex:DEADBEEF";

impl Shell {
    fn dispatch(&mut self, tokens: &[String]) -> Result<String, String> {
        let arg = |i: usize| -> Result<Vec<u8>, String> {
            tokens
                .get(i)
                .ok_or_else(|| "missing argument".to_string())
                .and_then(|t| parse_bytes(t))
        };
        let err = |e: Error| e.to_string();
        match tokens[0].as_str() {
            "help" => Ok(HELP.to_string()),
            "get" => {
                let v = self.db.get(&arg(1)?).map_err(err)?;
                Ok(v.map(|v| fmt_bytes(&v)).unwrap_or_else(|| "(nil)".into()))
            }
            "put" => {
                self.db.put(arg(1)?, arg(2)?).map_err(err)?;
                Ok("OK".into())
            }
            "del" => {
                self.db.delete(arg(1)?).map_err(err)?;
                Ok("OK".into())
            }
            "scan" | "count" => {
                let counting = tokens[0] == "count";
                let mut lo = None;
                let mut hi = None;
                let mut rev = false;
                let mut limit = if counting { usize::MAX } else { 50usize };
                let mut pos = 0;
                let mut i = 1;
                while i < tokens.len() {
                    match tokens[i].as_str() {
                        "--rev" => rev = true,
                        "--limit" => {
                            i += 1;
                            limit = tokens
                                .get(i)
                                .and_then(|t| t.parse().ok())
                                .ok_or("bad --limit")?;
                        }
                        "-" => pos += 1,
                        t => {
                            let b = parse_bytes(t)?;
                            if pos == 0 {
                                lo = Some(b);
                            } else {
                                hi = Some(b);
                            }
                            pos += 1;
                        }
                    }
                    i += 1;
                }
                let it = self
                    .db
                    .iter(lo.as_deref(), hi.as_deref(), rev)
                    .map_err(err)?;
                let mut out = String::new();
                let mut n = 0usize;
                for kv in it {
                    let (k, v) = kv.map_err(err)?;
                    n += 1;
                    if !counting {
                        out.push_str(&format!(
                            "{:>4}) {} => {}\n",
                            n,
                            fmt_bytes(&k),
                            fmt_bytes(&v)
                        ));
                    }
                    if n >= limit {
                        if !counting {
                            out.push_str("     …(limit reached, use --limit)\n");
                        }
                        break;
                    }
                }
                if counting {
                    Ok(format!("{n}"))
                } else if n == 0 {
                    Ok("(empty range)".into())
                } else {
                    Ok(out.trim_end().to_string())
                }
            }
            "begin" => {
                if self.txn.is_some() {
                    return Err("transaction already open (commit/abort first)".into());
                }
                self.txn = Some(self.db.begin());
                Ok("txn open".into())
            }
            "tget" | "tput" | "tdel" | "tlock" => {
                let t = self.txn.as_mut().ok_or("no open transaction (begin)")?;
                match tokens[0].as_str() {
                    "tget" => {
                        let v = t.get(&arg(1)?).map_err(err)?;
                        Ok(v.map(|v| fmt_bytes(&v)).unwrap_or_else(|| "(nil)".into()))
                    }
                    "tlock" => {
                        let v = t.get_for_update(&arg(1)?).map_err(err)?;
                        Ok(v.map(|v| fmt_bytes(&v)).unwrap_or_else(|| "(nil)".into()))
                    }
                    "tput" => {
                        t.put(arg(1)?, arg(2)?).map_err(err)?;
                        Ok("buffered".into())
                    }
                    _ => {
                        t.delete(arg(1)?).map_err(err)?;
                        Ok("buffered".into())
                    }
                }
            }
            "commit" => {
                let t = self.txn.take().ok_or("no open transaction")?;
                match t.commit() {
                    Ok(()) => Ok("committed".into()),
                    Err(Error::Conflict) => {
                        Err("CONFLICT (first committer wins) — txn rolled back".into())
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            "abort" => {
                self.txn.take().ok_or("no open transaction")?.rollback();
                Ok("rolled back".into())
            }
            "snap" => {
                let id = self.next_snap;
                self.next_snap += 1;
                self.snaps.insert(id, self.db.snapshot());
                Ok(format!("snapshot {id}"))
            }
            "snaps" => {
                let mut ids: Vec<_> = self.snaps.keys().collect();
                ids.sort();
                Ok(format!("{ids:?}"))
            }
            "sget" => {
                let id: u32 = tokens.get(1).and_then(|t| t.parse().ok()).ok_or("bad id")?;
                let snap = self.snaps.get(&id).ok_or("unknown snapshot")?;
                let v = self.db.get_at(&arg(2)?, snap).map_err(err)?;
                Ok(v.map(|v| fmt_bytes(&v)).unwrap_or_else(|| "(nil)".into()))
            }
            "snapdrop" => {
                let id: u32 = tokens.get(1).and_then(|t| t.parse().ok()).ok_or("bad id")?;
                self.snaps.remove(&id).ok_or("unknown snapshot")?;
                Ok("dropped".into())
            }
            "install" => {
                let name = tokens.get(1).ok_or("missing name")?;
                let path = tokens.get(2).ok_or("missing wasm file")?;
                let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
                self.db.install_module(name, &bytes).map_err(err)?;
                Ok(format!("installed {name} ({} bytes)", bytes.len()))
            }
            "uninstall" => {
                self.db
                    .uninstall_module(tokens.get(1).ok_or("missing name")?)
                    .map_err(err)?;
                Ok("uninstalled".into())
            }
            "modules" => {
                let mods = self.db.list_modules().map_err(err)?;
                if mods.is_empty() {
                    return Ok("(none)".into());
                }
                Ok(mods
                    .iter()
                    .map(|m| format!("{} ({} bytes)", m.name, m.size))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "query" | "exec" => {
                let name = tokens.get(1).ok_or("missing module name")?;
                let input = tokens
                    .get(2)
                    .map(|t| parse_bytes(t))
                    .transpose()?
                    .unwrap_or_default();
                let res = if tokens[0] == "query" {
                    self.db.query(name, &input)
                } else {
                    self.db.execute(name, &input)
                };
                match res {
                    Ok(out) => Ok(if out.is_empty() {
                        "OK (no output)".into()
                    } else {
                        fmt_bytes(&out)
                    }),
                    Err(Error::GuestFailed { code, output }) => Err(format!(
                        "guest exited with code {code}{}",
                        if output.is_empty() {
                            String::new()
                        } else {
                            format!(", output {}", fmt_bytes(&output))
                        }
                    )),
                    Err(e) => Err(e.to_string()),
                }
            }
            "mktrig" => {
                let name = tokens.get(1).ok_or("missing trigger name")?;
                let module = tokens.get(2).ok_or("missing module name")?;
                // `-` = unbounded, mirroring scan's convention
                let bound = |i: usize| -> Result<Option<Vec<u8>>, String> {
                    match tokens.get(i) {
                        None => Ok(None),
                        Some(t) if t == "-" => Ok(None),
                        Some(t) => parse_bytes(t).map(Some),
                    }
                };
                let (lo, hi) = (bound(3)?, bound(4)?);
                let mode = self
                    .db
                    .create_trigger(name, module, lo.as_deref(), hi.as_deref())
                    .map_err(err)?;
                Ok(format!("trigger {name} -> {module} mode={}", mode.as_str()))
            }
            "deltrig" => {
                self.db
                    .delete_trigger(tokens.get(1).ok_or("missing trigger name")?)
                    .map_err(err)?;
                Ok("deleted".into())
            }
            "triggers" => {
                let trigs = self.db.list_triggers().map_err(err)?;
                if trigs.is_empty() {
                    return Ok("(none)".into());
                }
                Ok(trigs
                    .iter()
                    .map(|t| {
                        let range = |b: &[u8]| {
                            if b.is_empty() {
                                "-".to_string()
                            } else {
                                fmt_bytes(b)
                            }
                        };
                        let mut line = format!(
                            "{} -> {} [{}, {}) mode={} pending={}",
                            t.name,
                            t.module,
                            range(&t.lo),
                            range(&t.hi),
                            t.mode.as_str(),
                            t.pending
                        );
                        if let Some(e) = &t.last_error {
                            line.push_str(&format!(" last_error={e:?}"));
                        }
                        line
                    })
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "fork" => {
                let name = tokens.get(1).ok_or("missing name")?;
                let info = match tokens.get(2) {
                    Some(at) => {
                        let at: u64 = at.parse().map_err(|_| "AT must be a seqno")?;
                        self.db.fork_at(name, at).map_err(err)?
                    }
                    None => self.db.fork(name).map_err(err)?,
                };
                Ok(format!(
                    "fork {} @ seq {} (instance {}) -> {}",
                    info.name,
                    info.last_seqno,
                    info.instance_id,
                    info.path.display()
                ))
            }
            "forks" => {
                let forks = self.db.list_forks().map_err(err)?;
                if forks.is_empty() {
                    return Ok("(none)".into());
                }
                Ok(forks
                    .iter()
                    .map(|c| {
                        format!(
                            "{} @ seq {} (instance {}, {} ms epoch)",
                            c.name, c.last_seqno, c.instance_id, c.created_unix_ms
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "delfork" => {
                self.db
                    .delete_fork(tokens.get(1).ok_or("missing name")?)
                    .map_err(err)?;
                Ok("deleted".into())
            }
            "pin" => {
                let info = self
                    .db
                    .pin(tokens.get(1).ok_or("missing name")?)
                    .map_err(err)?;
                Ok(format!("pin {} @ seq {}", info.name, info.seqno))
            }
            "pins" => {
                let pins = self.db.pins();
                if pins.is_empty() {
                    return Ok("(none)".into());
                }
                Ok(pins
                    .iter()
                    .map(|p| {
                        format!(
                            "{} @ seq {} ({} ms epoch)",
                            p.name, p.seqno, p.created_unix_ms
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "unpin" => {
                self.db
                    .unpin(tokens.get(1).ok_or("missing name")?)
                    .map_err(err)?;
                Ok("unpinned".into())
            }
            "seqno" => Ok(self.db.seqno().to_string()),
            "flush" => {
                self.db.flush().map_err(err)?;
                Ok("flushed".into())
            }
            "compact" => {
                self.db.compact_all().map_err(err)?;
                Ok("compacted".into())
            }
            "gc" => match self.db.gc_vlog().map_err(err)? {
                Some(id) => Ok(format!("retired vlog file {id}")),
                None => Ok("no vlog file above the gc ratio".into()),
            },
            "stats" => {
                let s = self.db.stats();
                let mut out = format!(
                    "backend        {}\nvisible seqno  {}\nmemtable       {} bytes (+{} frozen)\n",
                    s.backend, s.visible_seqno, s.memtable_bytes, s.immutable_memtables
                );
                for (i, (runs, files, bytes)) in s.levels.iter().enumerate() {
                    if *runs > 0 {
                        out.push_str(&format!(
                            "level {i}        {runs} runs, {files} files, {bytes} bytes\n"
                        ));
                    }
                }
                out.push_str(&format!(
                    "vlog           {} live files, {} retired pending, {} discardable bytes\n",
                    s.vlog_files, s.vlog_retired, s.discard_bytes
                ));
                let total = s.cache_hits + s.cache_misses;
                let rate = if total > 0 {
                    s.cache_hits as f64 / total as f64 * 100.0
                } else {
                    0.0
                };
                out.push_str(&format!(
                    "block cache    {} hits / {} misses ({rate:.1}%)\n",
                    s.cache_hits, s.cache_misses
                ));
                out.push_str(&format!(
                    "group commit   {} batches in {} groups, {} wal syncs",
                    s.commit_batches, s.commit_groups, s.wal_syncs
                ));
                Ok(out)
            }
            other => Err(format!("unknown command {other:?} (try help)")),
        }
    }
}

/// One-shot `journal-rebuild <journal-dir> <dest-dir>`: reconstruct a
/// fresh store from a mutation journal and print what it held.
fn journal_rebuild(jrn: &str, dest: &str) -> ! {
    // bulk replay — rebuild() ends on its own explicit sync_wal barrier,
    // so per-op fsyncs during the replay would buy nothing but time
    let opts = Options {
        sync: SyncMode::Never,
        ..Options::default()
    };
    let t = Instant::now();
    match fluent31::journal::rebuild(jrn, dest, opts) {
        Ok(r) => {
            println!("rebuilt {dest} from {jrn}  {}", fmt_latency(t.elapsed()));
            println!("source instance  {}", r.source_instance);
            println!("base keys        {}", r.base_keys);
            println!("deltas applied   {}", r.deltas_applied);
            println!("last seqno       {}", r.last_seqno);
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("journal-rebuild failed: {e}");
            std::process::exit(1);
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|a| a == "journal-rebuild") {
        let [jrn, dest] = &args[1..] else {
            eprintln!("{USAGE}");
            std::process::exit(2);
        };
        journal_rebuild(jrn, dest);
    }
    let Some(dir) = args.first().filter(|a| !a.starts_with("--")) else {
        eprintln!("{USAGE}");
        std::process::exit(2);
    };
    let mut opts = Options::default();
    let mut it = args[1..].iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--std" => opts.io_backend = IoBackend::Std,
            "--uring" => opts.io_backend = IoBackend::Uring,
            "--nosync" => opts.sync = SyncMode::Never,
            "--sync-every" => {
                let ms = it
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|ms| *ms > 0)
                    .unwrap_or_else(|| {
                        eprintln!("--sync-every needs a positive millisecond value");
                        std::process::exit(2);
                    });
                opts.sync = SyncMode::Periodic {
                    every: std::time::Duration::from_millis(ms),
                };
            }
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
    }

    let t = Instant::now();
    let db = match Db::open(dir, opts) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("open failed: {e}");
            std::process::exit(1);
        }
    };
    println!(
        "fluent31 shell — {} — opened in {} — `help` for commands",
        dir,
        fmt_latency(t.elapsed())
    );
    println!("io backend: {}", db.stats().backend);

    let mut shell = Shell {
        db,
        txn: None,
        snaps: HashMap::new(),
        next_snap: 0,
    };
    let mut rl = rustyline::DefaultEditor::new().expect("readline");
    loop {
        let prompt = if shell.txn.is_some() {
            "fluent31(txn)> "
        } else {
            "fluent31> "
        };
        match rl.readline(prompt) {
            Ok(line) => {
                let tokens: Vec<String> =
                    line.split_whitespace().map(|s| s.to_string()).collect();
                if tokens.is_empty() {
                    continue;
                }
                if tokens[0] == "exit" || tokens[0] == "quit" {
                    break;
                }
                let _ = rl.add_history_entry(&line);
                let t = Instant::now();
                let result = shell.dispatch(&tokens);
                let lat = fmt_latency(t.elapsed());
                match result {
                    Ok(msg) => println!("{msg}  {lat}"),
                    Err(msg) => println!("(error) {msg}  {lat}"),
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => continue,
            Err(_) => break,
        }
    }
}
