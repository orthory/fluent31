//! Sharded LRU block cache keyed by `(file_id, block_offset)`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

type Key = (u64, u64);

const NIL: usize = usize::MAX;
const SHARDS: usize = 16;

struct Node {
    key: Key,
    val: Arc<Vec<u8>>,
    prev: usize,
    next: usize,
}

struct Shard {
    map: HashMap<Key, usize>,
    nodes: Vec<Node>,
    free: Vec<usize>,
    head: usize, // most recently used
    tail: usize, // least recently used
    bytes: usize,
    cap: usize,
}

impl Shard {
    fn new(cap: usize) -> Self {
        Shard {
            map: HashMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            bytes: 0,
            cap,
        }
    }

    fn unlink(&mut self, i: usize) {
        let (p, n) = (self.nodes[i].prev, self.nodes[i].next);
        if p != NIL {
            self.nodes[p].next = n;
        } else {
            self.head = n;
        }
        if n != NIL {
            self.nodes[n].prev = p;
        } else {
            self.tail = p;
        }
        self.nodes[i].prev = NIL;
        self.nodes[i].next = NIL;
    }

    fn push_front(&mut self, i: usize) {
        self.nodes[i].prev = NIL;
        self.nodes[i].next = self.head;
        if self.head != NIL {
            self.nodes[self.head].prev = i;
        }
        self.head = i;
        if self.tail == NIL {
            self.tail = i;
        }
    }

    fn get(&mut self, key: &Key) -> Option<Arc<Vec<u8>>> {
        let i = *self.map.get(key)?;
        self.unlink(i);
        self.push_front(i);
        Some(self.nodes[i].val.clone())
    }

    fn insert(&mut self, key: Key, val: Arc<Vec<u8>>) {
        if let Some(&i) = self.map.get(&key) {
            self.bytes = self.bytes - self.nodes[i].val.len() + val.len();
            self.nodes[i].val = val;
            self.unlink(i);
            self.push_front(i);
        } else {
            let i = if let Some(i) = self.free.pop() {
                self.nodes[i] = Node {
                    key,
                    val,
                    prev: NIL,
                    next: NIL,
                };
                i
            } else {
                self.nodes.push(Node {
                    key,
                    val,
                    prev: NIL,
                    next: NIL,
                });
                self.nodes.len() - 1
            };
            self.bytes += self.nodes[i].val.len();
            self.map.insert(key, i);
            self.push_front(i);
        }
        while self.bytes > self.cap && self.tail != NIL {
            let victim = self.tail;
            // never evict the entry we just touched
            if victim == self.head {
                break;
            }
            self.bytes -= self.nodes[victim].val.len();
            self.map.remove(&self.nodes[victim].key);
            self.unlink(victim);
            self.nodes[victim].val = Arc::new(Vec::new());
            self.free.push(victim);
        }
    }
}

pub struct BlockCache {
    shards: Vec<Mutex<Shard>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let per = (capacity / SHARDS).max(1024);
        BlockCache {
            shards: (0..SHARDS).map(|_| Mutex::new(Shard::new(per))).collect(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn shard(&self, key: &Key) -> &Mutex<Shard> {
        let h = key.0.wrapping_mul(0x9e3779b97f4a7c15) ^ key.1;
        &self.shards[(h as usize) % SHARDS]
    }

    pub fn get(&self, file: u64, off: u64) -> Option<Arc<Vec<u8>>> {
        let key = (file, off);
        let got = self.shard(&key).lock().get(&key);
        match &got {
            Some(_) => self.hits.fetch_add(1, Ordering::Relaxed),
            None => self.misses.fetch_add(1, Ordering::Relaxed),
        };
        got
    }

    pub fn insert(&self, file: u64, off: u64, val: Arc<Vec<u8>>) {
        let key = (file, off);
        self.shard(&key).lock().insert(key, val);
    }

    pub fn hit_rate(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_get_insert_evict() {
        let c = BlockCache::new(SHARDS * 4096);
        for i in 0..1000u64 {
            c.insert(0, i, Arc::new(vec![0u8; 512]));
        }
        // capacity per shard is 4096 bytes -> ~8 entries per shard survive
        let survivors = (0..1000u64).filter(|&i| c.get(0, i).is_some()).count();
        assert!(survivors > 0 && survivors < 1000, "survivors={survivors}");
    }

    #[test]
    fn lru_prefers_recent() {
        let c = BlockCache::new(SHARDS * 4096);
        // fill one shard's worth via same file id, offsets hashed across shards;
        // use a single key repeatedly to ensure it stays
        c.insert(1, 42, Arc::new(vec![1u8; 100]));
        for i in 0..200u64 {
            c.insert(2, i, Arc::new(vec![0u8; 512]));
            let _ = c.get(1, 42); // keep it hot
        }
        assert!(c.get(1, 42).is_some());
    }
}
