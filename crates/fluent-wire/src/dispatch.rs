//! Opcode → engine dispatch. Every engine call hops to the blocking pool
//! behind the shared gates; payload codecs are defined in WIRE.md.

use bytes::{BufMut, BytesMut};
use fluent31::WriteBatch;

use crate::proto::*;
use crate::server::WireServer;

pub(crate) async fn handle(srv: &WireServer, opcode: u8, payload: &[u8]) -> (u8, Vec<u8>) {
    match run(srv, opcode, payload).await {
        Ok((st, body)) => (st, body),
        Err(HandleErr::Bad(msg)) => (ST_BAD_FRAME, msg.into_bytes()),
        Err(HandleErr::Engine(e)) => {
            let st = status_for(&e);
            let body = match e {
                fluent31::Error::GuestFailed { code, output } => {
                    let mut b = Vec::with_capacity(4 + output.len());
                    b.extend_from_slice(&code.to_le_bytes());
                    b.extend_from_slice(&output);
                    b
                }
                other => other.to_string().into_bytes(),
            };
            (st, body)
        }
    }
}

enum HandleErr {
    Bad(String),
    Engine(fluent31::Error),
}

impl From<String> for HandleErr {
    fn from(s: String) -> Self {
        HandleErr::Bad(s)
    }
}

impl From<fluent31::Error> for HandleErr {
    fn from(e: fluent31::Error) -> Self {
        HandleErr::Engine(e)
    }
}

async fn read_call<T, F>(srv: &WireServer, f: F) -> Result<T, HandleErr>
where
    F: FnOnce() -> fluent31::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let _p = srv.gate.read.clone().acquire_owned().await.expect("open");
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| HandleErr::Bad(format!("engine worker failed: {e}")))?
        .map_err(HandleErr::Engine)
}

async fn write_call<T, F>(srv: &WireServer, f: F) -> Result<T, HandleErr>
where
    F: FnOnce() -> fluent31::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let _p = srv.gate.write.clone().acquire_owned().await.expect("open");
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| HandleErr::Bad(format!("engine worker failed: {e}")))?
        .map_err(HandleErr::Engine)
}

async fn run(srv: &WireServer, opcode: u8, payload: &[u8]) -> Result<(u8, Vec<u8>), HandleErr> {
    let db = srv.db.clone();
    match opcode {
        OP_HELLO => {
            let mut out = Vec::new();
            out.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
            out.extend_from_slice(b"fluent31");
            Ok((ST_OK, out))
        }

        OP_GET => {
            let key = payload.to_vec();
            match read_call(srv, move || db.get(&key)).await? {
                Some(v) => Ok((ST_OK, v)),
                None => Ok((ST_NOT_FOUND, Vec::new())),
            }
        }

        OP_PUT => {
            let mut rd = Rd(payload);
            let key = rd.blob()?.to_vec();
            let value = rd.rest().to_vec();
            write_call(srv, move || db.put(key, value)).await?;
            Ok((ST_OK, Vec::new()))
        }

        OP_DEL => {
            let key = payload.to_vec();
            write_call(srv, move || db.delete(key)).await?;
            Ok((ST_OK, Vec::new()))
        }

        OP_BATCH => {
            let mut rd = Rd(payload);
            let count = rd.u32()?;
            let mut batch = WriteBatch::new();
            for _ in 0..count {
                match rd.u8()? {
                    0 => {
                        let k = rd.blob()?.to_vec();
                        let v = rd.blob()?.to_vec();
                        batch.put(k, v);
                    }
                    1 => batch.delete(rd.blob()?.to_vec()),
                    k => return Err(format!("unknown batch op kind {k}").into()),
                }
            }
            rd.done()?;
            let n = batch.len() as u32;
            write_call(srv, move || db.write(batch)).await?;
            Ok((ST_OK, n.to_le_bytes().to_vec()))
        }

        OP_SCAN => {
            let mut rd = Rd(payload);
            let flags = rd.u8()?;
            let reverse = flags & 1 == 1;
            let lo = rd.opt_blob()?.map(<[u8]>::to_vec);
            let hi = rd.opt_blob()?.map(<[u8]>::to_vec);
            let after = rd.opt_blob()?.map(<[u8]>::to_vec);
            let limit = rd.u32()? as usize;
            rd.done()?;
            if limit == 0 || limit > 100_000 {
                return Err(format!("limit {limit} out of 1..=100000").into());
            }
            // `after` restarts strictly past the cursor in iteration order
            // (same clamping as the GraphQL scan)
            let (lo, hi) = match after {
                None => (lo, hi),
                Some(a) if reverse => {
                    let hi = match hi {
                        Some(h) if h < a => Some(h),
                        _ => Some(a),
                    };
                    (lo, hi)
                }
                Some(a) => {
                    let mut succ = a;
                    succ.push(0);
                    let lo = match lo {
                        Some(l) if l > succ => Some(l),
                        _ => Some(succ),
                    };
                    (lo, hi)
                }
            };
            let (pairs, has_more) = read_call(srv, move || {
                let it = db.iter(lo.as_deref(), hi.as_deref(), reverse)?;
                let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                let mut has_more = false;
                for item in it {
                    let (k, v) = item?;
                    if pairs.len() == limit {
                        has_more = true;
                        break;
                    }
                    pairs.push((k, v));
                }
                Ok((pairs, has_more))
            })
            .await?;
            let mut out = BytesMut::new();
            out.put_u32_le(pairs.len() as u32);
            for (k, v) in &pairs {
                put_blob(&mut out, k);
                put_blob(&mut out, v);
            }
            out.put_u8(has_more as u8);
            match (has_more, pairs.last()) {
                (true, Some((k, _))) => {
                    out.put_u8(1);
                    put_blob(&mut out, k);
                }
                _ => out.put_u8(0),
            }
            Ok((ST_OK, out.to_vec()))
        }

        OP_QUERY => {
            let mut rd = Rd(payload);
            let name = String::from_utf8(rd.blob()?.to_vec())
                .map_err(|_| "module name is not UTF-8".to_string())?;
            let input = rd.rest().to_vec();
            let out = read_call(srv, move || db.query(&name, &input)).await?;
            Ok((ST_OK, out))
        }

        OP_EXEC => {
            let mut rd = Rd(payload);
            let name = String::from_utf8(rd.blob()?.to_vec())
                .map_err(|_| "module name is not UTF-8".to_string())?;
            let input = rd.rest().to_vec();
            let out = write_call(srv, move || db.execute(&name, &input)).await?;
            Ok((ST_OK, out))
        }

        OP_SYNC_WAL => {
            write_call(srv, move || db.sync_wal()).await?;
            Ok((ST_OK, Vec::new()))
        }

        OP_STATS => {
            // human-readable, format-unstable by design (WIRE.md)
            let s = read_call(srv, move || Ok(db.stats())).await?;
            Ok((ST_OK, format!("{s:#?}").into_bytes()))
        }

        other => Err(format!("unknown opcode {other:#04x}").into()),
    }
}
