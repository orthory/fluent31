//! Mutation root: direct writes, transactional WASM executors, and
//! module/checkpoint/maintenance admin. Mutation fields run serially in
//! document order (GraphQL spec), each against the then-current state.
//!
//! Every field is nullable so a per-field failure stays representable in
//! the response (`errors` entry + null field) without spec-invalid partial
//! objects — earlier fields' committed effects stay visible in `data`.

use async_graphql::{Context, InputObject, Object, OneofObject, Result};
use fluent31::WriteBatch;

use crate::bytes::{Bytes, BytesInput};
use crate::types::{Checkpoint, GcResult, Module, U64};
use crate::{blocking_write, db};

/// One entry of a `writeBatch`.
#[derive(OneofObject)]
pub enum WriteOp {
    /// Insert or overwrite a key.
    Put(PutOp),
    /// Delete a key (succeeds whether or not it existed).
    Delete(BytesInput),
}

#[derive(InputObject)]
pub struct PutOp {
    pub key: BytesInput,
    pub value: BytesInput,
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Insert or overwrite one key.
    async fn put(
        &self,
        ctx: &Context<'_>,
        key: BytesInput,
        value: BytesInput,
    ) -> Result<Option<bool>> {
        let (key, value) = (key.into_bytes()?, value.into_bytes()?);
        let db = db(ctx)?;
        blocking_write(ctx, move || db.put(key, value)).await?;
        Ok(Some(true))
    }

    /// Delete one key (succeeds whether or not it existed).
    async fn delete(&self, ctx: &Context<'_>, key: BytesInput) -> Result<Option<bool>> {
        let key = key.into_bytes()?;
        let db = db(ctx)?;
        blocking_write(ctx, move || db.delete(key)).await?;
        Ok(Some(true))
    }

    /// Apply puts/deletes atomically — all-or-nothing in the WAL and
    /// memtable, one contiguous sequence-number range. Returns the number
    /// of operations applied.
    async fn write_batch(&self, ctx: &Context<'_>, ops: Vec<WriteOp>) -> Result<Option<i32>> {
        let mut batch = WriteBatch::new();
        for op in ops {
            match op {
                WriteOp::Put(p) => batch.put(p.key.into_bytes()?, p.value.into_bytes()?),
                WriteOp::Delete(k) => batch.delete(k.into_bytes()?),
            }
        }
        let n = i32::try_from(batch.len()).unwrap_or(i32::MAX);
        let db = db(ctx)?;
        blocking_write(ctx, move || db.write(batch)).await?;
        Ok(Some(n))
    }

    /// Run a registered WASM executor inside a transaction: commits when
    /// the guest exits 0, rolls back otherwise, retries automatically on
    /// write conflicts. Returns the guest's output bytes.
    async fn wasm_execute(
        &self,
        ctx: &Context<'_>,
        module: String,
        input: Option<BytesInput>,
    ) -> Result<Option<Bytes>> {
        let input = input.map(BytesInput::into_bytes).transpose()?.unwrap_or_default();
        let db = db(ctx)?;
        Ok(Some(Bytes(
            blocking_write(ctx, move || db.execute(&module, &input)).await?,
        )))
    }

    /// Install (or replace) a WASM module under `name`. Accepts a binary
    /// module (use `base64`) or WAT text (use `text`).
    async fn install_module(
        &self,
        ctx: &Context<'_>,
        name: String,
        wasm: BytesInput,
    ) -> Result<Option<Module>> {
        let bytes = wasm.into_bytes()?;
        let size = i32::try_from(bytes.len()).unwrap_or(i32::MAX);
        let db = db(ctx)?;
        let stored = name.clone();
        blocking_write(ctx, move || db.install_module(&stored, &bytes)).await?;
        Ok(Some(Module { name, size }))
    }

    /// Uninstall a WASM module.
    async fn uninstall_module(&self, ctx: &Context<'_>, name: String) -> Result<Option<bool>> {
        let db = db(ctx)?;
        blocking_write(ctx, move || db.uninstall_module(&name)).await?;
        Ok(Some(true))
    }

    /// Create a named point-in-time checkpoint archive.
    async fn checkpoint(&self, ctx: &Context<'_>, name: String) -> Result<Option<Checkpoint>> {
        let db = db(ctx)?;
        Ok(Some(
            blocking_write(ctx, move || db.checkpoint(&name)).await?.into(),
        ))
    }

    /// Delete a checkpoint archive.
    async fn delete_checkpoint(&self, ctx: &Context<'_>, name: String) -> Result<Option<bool>> {
        let db = db(ctx)?;
        blocking_write(ctx, move || db.delete_checkpoint(&name)).await?;
        Ok(Some(true))
    }

    /// Freeze the active memtable and wait until everything is in tables.
    async fn flush(&self, ctx: &Context<'_>) -> Result<Option<bool>> {
        let db = db(ctx)?;
        blocking_write(ctx, move || db.flush()).await?;
        Ok(Some(true))
    }

    /// Run compaction until no trigger fires.
    async fn compact_all(&self, ctx: &Context<'_>) -> Result<Option<bool>> {
        let db = db(ctx)?;
        blocking_write(ctx, move || db.compact_all()).await?;
        Ok(Some(true))
    }

    /// One value-log GC pass.
    async fn gc_vlog(&self, ctx: &Context<'_>) -> Result<Option<GcResult>> {
        let db = db(ctx)?;
        Ok(Some(GcResult {
            retired: blocking_write(ctx, move || db.gc_vlog()).await?.map(U64),
        }))
    }
}
