//! A sharded LRU cache with byte charges. The LSM engine keeps table blocks
//! in it and the B+ tree keeps pages.

use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::{Arc, Mutex};

const SHARDS: usize = 16;

/// A small multiplicative hasher (the FxHash mixing step used by rustc) for
/// the integer keys the engines cache.
#[derive(Default)]
pub struct FxHasher(u64);

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut b = [0u8; 8];
            b[..chunk.len()].copy_from_slice(chunk);
            self.write_u64(u64::from_le_bytes(b));
        }
    }

    fn write_u64(&mut self, v: u64) {
        self.0 = (self.0.rotate_left(5) ^ v).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type FxBuild = BuildHasherDefault<FxHasher>;

struct Entry<V> {
    value: Arc<V>,
    charge: usize,
    tick: u64,
}

struct Shard<K, V> {
    map: HashMap<K, Entry<V>, FxBuild>,
    order: BTreeMap<u64, K>,
    tick: u64,
    used: usize,
    capacity: usize,
}

pub struct LruCache<K, V> {
    shards: Box<[Mutex<Shard<K, V>>]>,
    capacity: usize,
}

impl<K: Hash + Eq + Clone, V> LruCache<K, V> {
    /// A cache holding up to `capacity` bytes of charges. Zero disables it.
    pub fn new(capacity: usize) -> Self {
        let per = capacity.div_ceil(SHARDS);
        let shards = (0..SHARDS)
            .map(|_| {
                Mutex::new(Shard {
                    map: HashMap::default(),
                    order: BTreeMap::new(),
                    tick: 0,
                    used: 0,
                    capacity: per,
                })
            })
            .collect();
        LruCache { shards, capacity }
    }

    fn shard(&self, k: &K) -> &Mutex<Shard<K, V>> {
        let mut h = FxHasher::default();
        k.hash(&mut h);
        let idx = (h.finish().wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 60) as usize % SHARDS;
        &self.shards[idx]
    }

    pub fn get(&self, k: &K) -> Option<Arc<V>> {
        if self.capacity == 0 {
            return None;
        }
        let mut s = self.shard(k).lock().unwrap();
        s.tick += 1;
        let tick = s.tick;
        let e = s.map.get_mut(k)?;
        let old = std::mem::replace(&mut e.tick, tick);
        let v = e.value.clone();
        if let Some(key) = s.order.remove(&old) {
            s.order.insert(tick, key);
        }
        Some(v)
    }

    pub fn insert(&self, k: K, v: Arc<V>, charge: usize) {
        if self.capacity == 0 || charge > self.capacity / SHARDS {
            return;
        }
        let mut s = self.shard(&k).lock().unwrap();
        s.tick += 1;
        let tick = s.tick;
        if let Some(old) = s.map.insert(
            k.clone(),
            Entry {
                value: v,
                charge,
                tick,
            },
        ) {
            s.order.remove(&old.tick);
            s.used -= old.charge;
        }
        s.order.insert(tick, k);
        s.used += charge;
        while s.used > s.capacity {
            let Some((_, victim)) = s.order.pop_first() else {
                break;
            };
            if let Some(e) = s.map.remove(&victim) {
                s.used -= e.charge;
            }
        }
    }

    pub fn remove(&self, k: &K) {
        if self.capacity == 0 {
            return;
        }
        let mut s = self.shard(k).lock().unwrap();
        if let Some(e) = s.map.remove(k) {
            s.order.remove(&e.tick);
            s.used -= e.charge;
        }
    }

    /// Bytes currently charged.
    pub fn usage(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().used).sum()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recently_used() {
        let c: LruCache<u64, u64> = LruCache::new(SHARDS * 100);
        for i in 0..1000u64 {
            c.insert(i, Arc::new(i), 10);
        }
        assert!(c.usage() <= SHARDS * 100);
        assert_eq!(c.get(&999).as_deref(), Some(&999));
        assert!(c.get(&0).is_none());
        c.remove(&999);
        assert!(c.get(&999).is_none());
    }

    #[test]
    fn zero_capacity_disables() {
        let c: LruCache<u64, u64> = LruCache::new(0);
        c.insert(1, Arc::new(1), 1);
        assert!(c.get(&1).is_none());
    }
}
