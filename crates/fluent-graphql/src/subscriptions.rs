//! GraphQL subscriptions — the live plane over [`fluent31::Db::subscribe`].
//!
//! Two surfaces share one delivery pipeline:
//!
//! 1. **`subscription { changes(lo:, hi:) }`** — the raw post-commit change
//!    stream of a key range: every committed put/delete, in seqno order.
//!    "Listen to everything, filter later."
//! 2. **Typed module feeds** — a module whose descriptor declares a `feed`
//!    (see `descriptor.rs`) becomes its own Subscription field: the values
//!    its `on_apply` writes under the feed prefix are delivered as typed
//!    events (puts only — feed GC deletes are not events).
//!
//! Payload semantics, both surfaces:
//!
//! - The first item is an `ATTACHED` marker pinned at the subscribe
//!   boundary: scan through its `query` for history at-or-below the
//!   boundary, take everything above from the stream — gap-free attach
//!   with zero overlap.
//! - Every item carries `query: Query!` — the full Query root re-entered at
//!   a snapshot pinned to the item's **exact commit boundary**
//!   (`StreamEntry::commit_seqno`): the one transactionally-consistent
//!   state in which the event became visible, never a torn mid-commit view
//!   and never "whenever the server got around to it". The worker holds a
//!   rolling snapshot at each delivered batch's tail so the next batch's
//!   commit seqnos are always still registrable (the GC watermark can
//!   never outrun them).
//! - A consumer that falls behind the engine-side buffer is cut loose
//!   (never stalling writers): the stream ends with a "lagged" error and
//!   the client re-attaches and re-scans.
//!
//! Each active subscription runs one dedicated forwarder thread that
//! drains the engine subscription and feeds a bounded channel; dropping
//! the GraphQL stream closes the channel and the thread exits on its next
//! poll tick.

use std::sync::Arc;
use std::time::Duration;

use async_graphql::dynamic::{
    Enum, EnumItem, Field, FieldFuture, FieldValue, InputValue, Object, SubscriptionField,
    SubscriptionFieldFuture, TypeRef,
};
use async_graphql::futures_util::stream::{self, Stream, StreamExt};
use async_graphql::{Error, Name, Value};
use fluent31::{Db, Snapshot, StreamEvent, ValueKind};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::builtins::{opt_arg_bytes, prefix_end};
use crate::descriptor::{FeedSpec, ModuleSchema};
use crate::modules::type_ref_nullable_outer;
use crate::schema::{manager, BytesVal, SnapAt};

/// How often the forwarder wakes to notice a dropped consumer.
const POLL: Duration = Duration::from_millis(500);
/// In-flight items between the forwarder and the GraphQL stream. Small on
/// purpose: each item holds a registered snapshot (a GC pin), and real
/// buffering lives in the engine-side subscription queue.
const CHANNEL_ITEMS: usize = 16;

// ---------------------------------------------------------------------------
// items
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubItemKind {
    Attached,
    Put,
    Delete,
}

impl SubItemKind {
    fn as_str(self) -> &'static str {
        match self {
            SubItemKind::Attached => "ATTACHED",
            SubItemKind::Put => "PUT",
            SubItemKind::Delete => "DELETE",
        }
    }
}

/// One delivered subscription item: an attach marker or a committed op,
/// plus the snapshot pinned at its exact commit boundary.
struct SubItem {
    seqno: u64,
    commit_seqno: u64,
    kind: SubItemKind,
    /// None for the attach marker.
    key: Option<Vec<u8>>,
    /// None for the attach marker and for deletes.
    value: Option<Vec<u8>>,
    snap: Arc<Snapshot>,
}

// ---------------------------------------------------------------------------
// delivery pipeline (engine subscription → bounded channel → stream)
// ---------------------------------------------------------------------------

/// Spawn the forwarder thread and return the item stream (items become
/// `FieldValue`s at the resolver site, where the payload lifetime is
/// known). `puts_only` drops deletes (feed surfaces: feed GC is not an
/// event).
fn subscribe_stream(
    db: Arc<Db>,
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
    puts_only: bool,
) -> Result<impl Stream<Item = Result<SubItem, String>> + Send, Error> {
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_ITEMS);
    std::thread::Builder::new()
        .name("fluent-gql-sub".into())
        .spawn(move || pump(db, lo, hi, puts_only, tx))
        .map_err(|e| Error::new(format!("subscription worker spawn failed: {e}")))?;
    Ok(stream::unfold(
        rx,
        |mut rx: Receiver<Result<SubItem, String>>| async move {
            let item = rx.recv().await?;
            Some((item, rx))
        },
    ))
}

/// Forwarder thread body: drain the engine subscription, pin each item's
/// commit-boundary snapshot, push into the channel. Any failure (or a
/// dropped consumer) ends the thread; failures are delivered as the
/// stream's final error item.
fn pump(
    db: Arc<Db>,
    lo: Vec<u8>,
    hi: Option<Vec<u8>>,
    puts_only: bool,
    tx: Sender<Result<SubItem, String>>,
) {
    let fail = |msg: String| {
        let _ = tx.blocking_send(Err(msg));
    };

    let mut sub = match db.subscribe(&lo, hi.as_deref()) {
        Ok(sub) => sub,
        Err(e) => return fail(format!("subscribe failed: {e}")),
    };

    // The attach marker pins the exact subscribe boundary: the engine
    // subscription itself still holds its pin at `start` here, so this
    // registration cannot lose the GC race.
    let start = sub.start_seqno();
    let head = match db.snapshot_at(start) {
        Ok(snap) => Arc::new(snap),
        Err(e) => return fail(format!("attach snapshot failed: {e}")),
    };
    let attached = SubItem {
        seqno: start,
        commit_seqno: start,
        kind: SubItemKind::Attached,
        key: None,
        value: None,
        snap: head.clone(),
    };
    let Ok(()) = tx.blocking_send(Ok(attached)) else { return };

    // Rolling GC hold: `hold` stays registered at-or-below every seqno the
    // next delivered batch can carry, so the per-commit `snapshot_at`
    // below can never be outrun by the GC watermark. Assigning a new hold
    // registers it before the old one drops.
    let mut hold = head;
    loop {
        let event = match sub.recv_timeout(POLL) {
            Ok(event) => event,
            Err(e) => return fail(format!("subscription stream failed: {e}")),
        };
        let Some(event) = event else {
            // idle tick — only useful to notice a departed consumer
            if tx.is_closed() {
                return;
            }
            continue;
        };
        let entries = match event {
            StreamEvent::Batch(entries) => entries,
            StreamEvent::Lagged => {
                return fail(
                    "subscription lagged past its buffer and was cut off; \
                     re-attach and re-scan from the new ATTACHED boundary"
                        .into(),
                )
            }
        };
        let Some(tail) = entries.last().map(|e| e.commit_seqno) else {
            continue;
        };

        // one snapshot per commit: consecutive entries share commit_seqno
        let mut commit: Option<(u64, Arc<Snapshot>)> = None;
        for entry in entries {
            if puts_only && entry.kind == ValueKind::Delete {
                continue;
            }
            let snap = match &commit {
                Some((at, snap)) if *at == entry.commit_seqno => snap.clone(),
                _ => match db.snapshot_at(entry.commit_seqno) {
                    Ok(snap) => {
                        let snap = Arc::new(snap);
                        commit = Some((entry.commit_seqno, snap.clone()));
                        snap
                    }
                    Err(e) => return fail(format!("commit snapshot failed: {e}")),
                },
            };
            let item = SubItem {
                seqno: entry.seqno,
                commit_seqno: entry.commit_seqno,
                kind: match entry.kind {
                    ValueKind::Put => SubItemKind::Put,
                    ValueKind::Delete => SubItemKind::Delete,
                },
                key: Some(entry.key),
                value: entry.value,
                snap,
            };
            let Ok(()) = tx.blocking_send(Ok(item)) else { return };
        }

        let next_hold = match commit {
            Some((at, snap)) if at == tail => snap,
            // every tail entry was filtered out (or shared no snapshot):
            // still roll the hold so the invariant survives quiet ranges
            _ => match db.snapshot_at(tail) {
                Ok(snap) => Arc::new(snap),
                Err(e) => return fail(format!("hold snapshot failed: {e}")),
            },
        };
        // register-new-then-drop-old, so the watermark can never leap both
        drop(std::mem::replace(&mut hold, next_hold));
    }
}

// ---------------------------------------------------------------------------
// payload types
// ---------------------------------------------------------------------------

pub(crate) fn change_kind_enum() -> Enum {
    Enum::new("ChangeKind")
        .description("What a subscription item is.")
        .item(EnumItem::new("ATTACHED").description(
            "The stream's first item: no op, just the subscribe boundary. \
             Everything at-or-below its seqno is scannable through `query`; \
             everything above arrives on the stream — gap-free, no overlap.",
        ))
        .item(EnumItem::new("PUT").description("A committed put."))
        .item(EnumItem::new("DELETE").description("A committed delete."))
}

/// Downcast helper for the payload field resolvers.
macro_rules! item_field {
    ($name:literal, $ty:expr, |$it:ident| $body:expr) => {
        Field::new($name, $ty, |ctx| {
            FieldFuture::new(async move {
                let $it = ctx.parent_value.try_downcast_ref::<SubItem>()?;
                Ok($body)
            })
        })
    };
}

/// The fields every subscription payload shares (`ChangeEvent` adds
/// `value`, feed payloads add `event`).
fn item_object(obj: Object) -> Object {
    obj.field(
        item_field!("seqno", TypeRef::named_nn("U64"), |it| Some(
            FieldValue::value(Value::String(it.seqno.to_string()))
        ))
        .description(
            "The op's commit seqno — unique, strictly increasing. For \
             ATTACHED: the subscribe boundary.",
        ),
    )
    .field(
        item_field!("commitSeqno", TypeRef::named_nn("U64"), |it| Some(
            FieldValue::value(Value::String(it.commit_seqno.to_string()))
        ))
        .description(
            "The last seqno of the atomic commit this op belonged to — the \
             exact read boundary `query` is pinned at. Ops of one commit \
             (e.g. a writeBatch or one on_apply drain) share it.",
        ),
    )
    .field(item_field!("kind", TypeRef::named_nn("ChangeKind"), |it| Some(
        FieldValue::value(Value::Enum(Name::new(it.kind.as_str())))
    )))
    .field(
        item_field!("key", TypeRef::named("Bytes"), |it| it
            .key
            .clone()
            .map(|k| FieldValue::owned_any(BytesVal(k))))
        .description("The written key; null for ATTACHED."),
    )
    .field(
        item_field!("query", TypeRef::named_nn("Query"), |it| Some(
            FieldValue::owned_any(SnapAt(it.snap.clone()))
        ))
        .description(
            "The full Query root, re-entered at a snapshot pinned to this \
             item's exact commit boundary (commitSeqno): reads are \
             transactionally consistent with the event — never a torn \
             mid-commit view, never newer state. Admin fields (stats, \
             modules, …) stay current-state, as everywhere.",
        ),
    )
}

pub(crate) fn change_event_object() -> Object {
    item_object(Object::new("ChangeEvent"))
        .description("One item of the raw `changes` subscription.")
        .field(
            item_field!("value", TypeRef::named("Bytes"), |it| it
                .value
                .clone()
                .map(|v| FieldValue::owned_any(BytesVal(v))))
            .description("The written value; null for DELETE and ATTACHED."),
        )
}

// ---------------------------------------------------------------------------
// subscription fields
// ---------------------------------------------------------------------------

/// The built-in raw plane: `changes(lo:, hi:)`.
pub(crate) fn register(subscription: async_graphql::dynamic::Subscription) -> async_graphql::dynamic::Subscription {
    subscription.field(
        SubscriptionField::new("changes", TypeRef::named_nn("ChangeEvent"), |ctx| {
            SubscriptionFieldFuture::new(async move {
                let lo = opt_arg_bytes(&ctx, "lo")?.unwrap_or_default();
                let hi = opt_arg_bytes(&ctx, "hi")?;
                let mgr = manager(&ctx)?;
                let items = subscribe_stream(mgr.db.clone(), lo, hi, false)?;
                Ok(items.map(|item| item.map(FieldValue::owned_any).map_err(Error::new)))
            })
        })
        .argument(InputValue::new("lo", TypeRef::named("BytesInput")))
        .argument(InputValue::new("hi", TypeRef::named("BytesInput")))
        .description(
            "Every committed write in [lo, hi) (omit either bound for an open \
             end), post-commit, in seqno order — listen to everything, filter \
             later. Starts with an ATTACHED boundary marker; ends with an \
             error when the consumer lags past the engine-side buffer \
             (re-attach and re-scan). Each item's `query` reads at the \
             item's exact commit boundary.",
        ),
    )
}

/// A feed-declaring module's Subscription field plus its generated payload
/// object type.
pub(crate) fn feed_field(
    name: &str,
    schema: Arc<ModuleSchema>,
    feed: FeedSpec,
) -> (SubscriptionField, Object) {
    let event_schema = schema.clone();
    let event_ty = feed.event.clone();
    let payload = item_object(Object::new(&feed.payload_type))
        .description(format!(
            "One event of module {name}'s feed (values written under prefix \
             {:?}).",
            String::from_utf8_lossy(&feed.prefix)
        ))
        .field(
            Field::new(
                "event",
                type_ref_nullable_outer(&feed.event),
                move |ctx| {
                    let schema = event_schema.clone();
                    let ty = event_ty.clone();
                    FieldFuture::new(async move {
                        let it = ctx.parent_value.try_downcast_ref::<SubItem>()?;
                        // feed streams are puts-only, so a missing value is
                        // exactly the attach marker
                        let Some(value) = &it.value else { return Ok(None) };
                        let value = crate::descriptor::normalize_output(&schema, &ty, value)
                            .map_err(|e| {
                                Error::new(format!(
                                    "module {} wrote a feed value violating its declared \
                                     event type: {e}",
                                    schema.module
                                ))
                            })?;
                        if value == Value::Null {
                            return Ok(None);
                        }
                        Ok(Some(FieldValue::value(value)))
                    })
                },
            )
            .description("The typed event; null for ATTACHED."),
        );

    let prefix = feed.prefix.clone();
    let mut field = SubscriptionField::new(
        name.to_string(),
        TypeRef::named_nn(&feed.payload_type),
        move |ctx| {
            let prefix = prefix.clone();
            SubscriptionFieldFuture::new(async move {
                let mgr = manager(&ctx)?;
                let hi = prefix_end(&prefix);
                let items = subscribe_stream(mgr.db.clone(), prefix, hi, true)?;
                Ok(items.map(|item| item.map(FieldValue::owned_any).map_err(Error::new)))
            })
        },
    );
    field = match &schema.description {
        Some(d) => field.description(d),
        None => field.description(format!(
            "Live typed feed of module {name}: one item per value its \
             on_apply writes under {:?} (puts only), starting with an \
             ATTACHED boundary marker.",
            String::from_utf8_lossy(&feed.prefix)
        )),
    };
    (field, payload)
}
