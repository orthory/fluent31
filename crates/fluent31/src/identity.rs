//! Store identity & lineage: a deterministic 128-bit instance id answering
//! "which database lifetime am I talking to?".
//!
//! Replication copies bytes between machines and names them by
//! `(file id, offset)` — coordinates unique only within one store lifetime.
//! The instance id is the outer qualifier: minted at creation, re-minted at
//! every point where history can diverge (first read-write open of a
//! checkpoint archive, `restore_to`), compared by pure equality. It is
//! *derived*, not random — `H(name)` for a root store, `H(parent ‖ cut ‖
//! name)` for a fork — so the chain is reproducible from lineage metadata
//! and uniqueness is an operator contract (fleet-unique store names), like
//! hostnames. Normal restarts and crash recovery keep the id: the engine
//! never reuses a `(file id, offset)` within one lifetime (file ids are
//! monotonic; recovery never appends to a pre-crash vlog file).

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::types::SeqNo;

pub const INSTANCE_ID_LEN: usize = 16;

/// 128-bit store-lifetime identifier (truncated SHA-256 of the lineage).
pub type InstanceId = [u8; INSTANCE_ID_LEN];

/// The identity a store carries in its manifest once named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreIdentity {
    /// Operator-chosen, fleet-unique store name (uniqueness contract).
    pub name: String,
    pub instance_id: InstanceId,
    /// Fork lineage: `(parent instance, cut seqno)` when this store began
    /// as a checkpoint fork or restore; `None` for a root store.
    pub parent: Option<(InstanceId, SeqNo)>,
}

impl StoreIdentity {
    pub fn root(name: &str) -> StoreIdentity {
        StoreIdentity {
            name: name.to_string(),
            instance_id: derive_root(name),
            parent: None,
        }
    }

    pub fn instance_hex(&self) -> String {
        hex(&self.instance_id)
    }
}

/// A fork recorded by checkpoint/restore but not yet minted. The first
/// read-write open consumes it: derives the child id and persists the new
/// identity, so one archive can fork in place at most once (and each
/// `restore_to` copy names its own fork).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingFork {
    pub parent_instance_id: InstanceId,
    pub cut_seqno: SeqNo,
    /// The child's store name, fixed at checkpoint/restore time.
    pub name: String,
}

impl PendingFork {
    /// Derive the child identity this fork mints.
    pub fn mint(&self) -> StoreIdentity {
        StoreIdentity {
            name: self.name.clone(),
            instance_id: derive_fork(&self.parent_instance_id, self.cut_seqno, &self.name),
            parent: Some((self.parent_instance_id, self.cut_seqno)),
        }
    }
}

/// Domain-separated, length-prefixed truncated SHA-256.
fn h16(domain: &[u8], parts: &[&[u8]]) -> InstanceId {
    let mut h = Sha256::new();
    h.update((domain.len() as u64).to_le_bytes());
    h.update(domain);
    for p in parts {
        h.update((p.len() as u64).to_le_bytes());
        h.update(p);
    }
    let digest = h.finalize();
    digest[..INSTANCE_ID_LEN].try_into().unwrap()
}

pub fn derive_root(name: &str) -> InstanceId {
    h16(b"fluent31.identity.v1", &[name.as_bytes()])
}

pub fn derive_fork(parent: &InstanceId, cut_seqno: SeqNo, name: &str) -> InstanceId {
    h16(
        b"fluent31.fork.v1",
        &[parent, &cut_seqno.to_le_bytes(), name.as_bytes()],
    )
}

pub fn hex(id: &InstanceId) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// Store names share the checkpoint-name charset: they become fork names
/// for archives (directory names) and travel in replication handshakes.
pub fn validate_store_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
        && !name.starts_with('.');
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "invalid store name {name:?} (use [A-Za-z0-9._-], max 64 chars, no leading dot)"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_deterministic_and_name_sensitive() {
        assert_eq!(derive_root("prod"), derive_root("prod"));
        assert_ne!(derive_root("prod"), derive_root("prod2"));
        let id = StoreIdentity::root("prod");
        assert_eq!(id.instance_id, derive_root("prod"));
        assert_eq!(id.instance_hex().len(), 32);
        assert!(id.parent.is_none());
    }

    #[test]
    fn fork_depends_on_every_input() {
        let p1 = derive_root("a");
        let p2 = derive_root("b");
        let f = derive_fork(&p1, 100, "edge");
        assert_eq!(f, derive_fork(&p1, 100, "edge"));
        assert_ne!(f, derive_fork(&p2, 100, "edge"));
        assert_ne!(f, derive_fork(&p1, 101, "edge"));
        assert_ne!(f, derive_fork(&p1, 100, "edge2"));
        // fork and root derivations never collide (domain separation)
        assert_ne!(derive_fork(&p1, 0, "a"), derive_root("a"));
    }

    #[test]
    fn pending_fork_mints_child_with_lineage() {
        let parent = StoreIdentity::root("main");
        let pf = PendingFork {
            parent_instance_id: parent.instance_id,
            cut_seqno: 42,
            name: "nightly".into(),
        };
        let child = pf.mint();
        assert_eq!(child.name, "nightly");
        assert_eq!(child.parent, Some((parent.instance_id, 42)));
        assert_eq!(
            child.instance_id,
            derive_fork(&parent.instance_id, 42, "nightly")
        );
        // the chain is verifiable: recompute from lineage
        let (pid, cut) = child.parent.unwrap();
        assert_eq!(child.instance_id, derive_fork(&pid, cut, &child.name));
    }

    #[test]
    fn store_name_validation() {
        assert!(validate_store_name("prod-eu.1").is_ok());
        assert!(validate_store_name("").is_err());
        assert!(validate_store_name(".hidden").is_err());
        assert!(validate_store_name("has space").is_err());
        assert!(validate_store_name(&"x".repeat(65)).is_err());
    }
}
