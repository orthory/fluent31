//! Query root: reads (direct and WASM) at one MVCC snapshot per operation,
//! plus admin listings.
//!
//! Every field is nullable so a per-field failure stays representable in
//! the response (`errors` entry + null field) without spec-invalid
//! partial objects — sibling fields keep their data.

use async_graphql::{Context, Object, Result};

use crate::bytes::{Bytes, BytesInput};
use crate::types::{Checkpoint, Module, Pair, ScanPage, Stats, U64};
use crate::{blocking_read, db, snap};

const DEFAULT_SCAN_LIMIT: i32 = 100;
const MAX_SCAN_LIMIT: i32 = 10_000;

/// Smallest key strictly greater than every key with this prefix, or None
/// when the prefix is all 0xFF (the range is unbounded above).
fn prefix_end(p: &[u8]) -> Option<Vec<u8>> {
    let mut end = p.to_vec();
    while let Some(last) = end.last_mut() {
        if *last == 0xFF {
            end.pop();
        } else {
            *last += 1;
            return Some(end);
        }
    }
    None
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Point lookup at this operation's snapshot. Null when the key is
    /// absent.
    async fn get(&self, ctx: &Context<'_>, key: BytesInput) -> Result<Option<Bytes>> {
        let key = key.into_bytes()?;
        let db = db(ctx)?;
        let snap = snap(ctx)?;
        Ok(blocking_read(ctx, move || db.get_at(&key, &snap))
            .await?
            .map(Bytes))
    }

    /// Range scan over `[lo, hi)` — or over a key `prefix` — at this
    /// operation's snapshot, forward or reverse (default forward),
    /// paginated with `limit` (default 100, max 10000) plus the returned
    /// `nextAfter` cursor.
    async fn scan(
        &self,
        ctx: &Context<'_>,
        lo: Option<BytesInput>,
        hi: Option<BytesInput>,
        prefix: Option<BytesInput>,
        after: Option<BytesInput>,
        reverse: Option<bool>,
        limit: Option<i32>,
    ) -> Result<Option<ScanPage>> {
        let reverse = reverse.unwrap_or(false);
        let limit = limit.unwrap_or(DEFAULT_SCAN_LIMIT);
        if !(1..=MAX_SCAN_LIMIT).contains(&limit) {
            return Err(format!("limit must be in 1..={MAX_SCAN_LIMIT}").into());
        }
        if prefix.is_some() && (lo.is_some() || hi.is_some()) {
            return Err("prefix cannot be combined with lo/hi".into());
        }
        let (mut lo, mut hi) = match prefix {
            Some(p) => {
                let p = p.into_bytes()?;
                let end = prefix_end(&p);
                (Some(p), end)
            }
            None => (
                lo.map(BytesInput::into_bytes).transpose()?,
                hi.map(BytesInput::into_bytes).transpose()?,
            ),
        };
        // `after` restarts strictly past the cursor in iteration order.
        if let Some(a) = after.map(BytesInput::into_bytes).transpose()? {
            if reverse {
                hi = Some(match hi {
                    Some(h) if h < a => h,
                    _ => a,
                });
            } else {
                let mut succ = a;
                succ.push(0);
                lo = Some(match lo {
                    Some(l) if l > succ => l,
                    _ => succ,
                });
            }
        }
        let db = db(ctx)?;
        let snap = snap(ctx)?;
        let take = limit as usize;
        let (pairs, has_more) = blocking_read(ctx, move || {
            let it = db.iter_at(lo.as_deref(), hi.as_deref(), reverse, &snap)?;
            let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut has_more = false;
            for item in it {
                let (k, v) = item?;
                if pairs.len() == take {
                    has_more = true;
                    break;
                }
                pairs.push((k, v));
            }
            Ok((pairs, has_more))
        })
        .await?;
        let next_after = if has_more {
            pairs.last().map(|(k, _)| Bytes(k.clone()))
        } else {
            None
        };
        Ok(Some(ScanPage {
            pairs: pairs
                .into_iter()
                .map(|(k, v)| Pair {
                    key: Bytes(k),
                    value: Bytes(v),
                })
                .collect(),
            has_more,
            next_after,
        }))
    }

    /// Run a registered read-only WASM query module at this operation's
    /// snapshot. `input` is handed to the guest verbatim; the guest's output
    /// bytes come back. A non-zero guest exit surfaces as a GUEST_FAILED
    /// error carrying the exit code and output.
    async fn wasm(
        &self,
        ctx: &Context<'_>,
        module: String,
        input: Option<BytesInput>,
    ) -> Result<Option<Bytes>> {
        let input = input.map(BytesInput::into_bytes).transpose()?.unwrap_or_default();
        let db = db(ctx)?;
        let snap = snap(ctx)?;
        Ok(Some(Bytes(
            blocking_read(ctx, move || db.query_at(&module, &input, &snap)).await?,
        )))
    }

    /// Installed WASM modules.
    async fn modules(&self, ctx: &Context<'_>) -> Result<Option<Vec<Module>>> {
        let db = db(ctx)?;
        Ok(Some(
            blocking_read(ctx, move || db.list_modules())
                .await?
                .into_iter()
                .map(Into::into)
                .collect(),
        ))
    }

    /// Engine statistics (not snapshot-bound).
    async fn stats(&self, ctx: &Context<'_>) -> Result<Option<Stats>> {
        let db = db(ctx)?;
        Ok(Some(blocking_read(ctx, move || Ok(db.stats())).await?.into()))
    }

    /// Point-in-time checkpoint archives.
    async fn checkpoints(&self, ctx: &Context<'_>) -> Result<Option<Vec<Checkpoint>>> {
        let db = db(ctx)?;
        Ok(Some(
            blocking_read(ctx, move || db.list_checkpoints())
                .await?
                .into_iter()
                .map(Into::into)
                .collect(),
        ))
    }

    /// The MVCC sequence number this query operation reads at.
    async fn snapshot_seqno(&self, ctx: &Context<'_>) -> Result<Option<U64>> {
        Ok(Some(U64(snap(ctx)?.seqno())))
    }
}

#[cfg(test)]
mod tests {
    use super::prefix_end;

    #[test]
    fn plain_prefix_increments_last_byte() {
        assert_eq!(prefix_end(b"scan/"), Some(b"scan0".to_vec()));
        assert_eq!(prefix_end(&[0xab, 0x01]), Some(vec![0xab, 0x02]));
    }

    #[test]
    fn trailing_ff_carries_into_earlier_byte() {
        assert_eq!(prefix_end(&[0xab, 0xff]), Some(vec![0xac]));
        assert_eq!(prefix_end(&[0xab, 0xff, 0xff]), Some(vec![0xac]));
        assert_eq!(prefix_end(&[0x01, 0xfe, 0xff]), Some(vec![0x01, 0xff]));
    }

    #[test]
    fn all_ff_prefix_is_unbounded_above() {
        assert_eq!(prefix_end(&[0xff]), None);
        assert_eq!(prefix_end(&[0xff, 0xff, 0xff]), None);
        // empty prefix scans everything; hi stays unbounded
        assert_eq!(prefix_end(b""), None);
    }
}
