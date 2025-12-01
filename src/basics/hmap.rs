use std::collections::HashMap;
use std::hash::Hash;
use std::num::Wrapping;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const NUM_SHARDS: usize = 64;

/// Fast, deterministic shard selection without external crates
pub trait Shardable {
    fn shard(&self) -> usize;
}

impl Shardable for u16 {
    #[inline]
    fn shard(&self) -> usize {
        // Simple but good-enough hash (same constant used in fxhash)
        let k = Wrapping(*self as usize);
        let h = k * Wrapping(0x517cc1b727220a95u128 as usize);
        (h.0) & (NUM_SHARDS - 1)
    }
}

impl Shardable for u32 {
    #[inline]
    fn shard(&self) -> usize {
        let k = Wrapping(*self as usize);
        let h = k * Wrapping(0x517cc1b727220a95u128 as usize);
        (h.0) & (NUM_SHARDS - 1)
    }
}

/// Async-safe sharded HashMap using tokio::sync::RwLock
pub struct ShardedRwLockMap<K, V> {
    shards: Box<[RwLock<HashMap<K, V>>; NUM_SHARDS]>,
}

impl<K, V> Default for ShardedRwLockMap<K, V>
where
    K: Eq + Hash + Shardable + Copy,
    V: Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ShardedRwLockMap<K, V>
where
    K: Eq + Hash + Shardable + Copy,
    V: Send + Sync,
{
    pub fn new() -> Self {
        // Helper to create an array of RwLock<HashMap<..>>
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
        }
        // SAFETY: we just created exactly NUM_SHARDS elements
        let shards: [_; NUM_SHARDS] = shards.try_into().unwrap_or_else(|_| unreachable!());
        Self {
            shards: Box::new(shards),
        }
    }

    #[inline]
    fn shard_for(&self, key: &K) -> usize {
        key.shard()
    }

    /// Get a read guard to the shard containing the key (if the key exists)
    pub async fn get(&self, key: K) -> Option<RwLockReadGuard<'_, HashMap<K, V>>> {
        let shard = &self.shards[self.shard_for(&key)];
        let lock = shard.read().await;
        if lock.contains_key(&key) {
            Some(lock)
        } else {
            None
        }
    }

    /// Get a write guard to the shard containing the key (if the key exists)
    pub async fn get_mut(&self, key: K) -> Option<RwLockWriteGuard<'_, HashMap<K, V>>> {
        let shard = &self.shards[self.shard_for(&key)];
        let lock = shard.write().await;
        if lock.contains_key(&key) {
            Some(lock)
        } else {
            None
        }
    }

    /// Get or create an entry (very common pattern for accounts)
    pub async fn entry(&self, key: K) -> RwLockWriteGuard<'_, HashMap<K, V>>
    where
        V: Default,
    {
        let shard = &self.shards[self.shard_for(&key)];
        let mut lock = shard.write().await;
        lock.entry(key).or_default();
        lock
    }

    /// Insert a value
    pub async fn insert(&self, key: K, value: V) -> Option<V> {
        let shard = &self.shards[self.shard_for(&key)];
        let mut lock = shard.write().await;
        lock.insert(key, value)
    }

    /// Remove a value
    pub async fn remove(&self, key: K) -> Option<V> {
        let shard = &self.shards[self.shard_for(&key)];
        let mut lock = shard.write().await;
        lock.remove(&key)
    }

    /// Check if key exists
    pub async fn contains_key(&self, key: K) -> bool {
        let shard = &self.shards[self.shard_for(&key)];
        let lock = shard.read().await;
        lock.contains_key(&key)
    }

    /// Iterate over all entries — useful for CSV output
    pub async fn iter(&self) -> ShardedIter<'_, K, V> {
        ShardedIter {
            map: self,
            shard_idx: 0,
        }
    }

    /// Returns the total number of entries in the map.
    ///
    /// This function works by summing the lengths of all shards in the map.
    /// It is an O(n) operation where n is the number of shard in the map.
    pub async fn len(&self) -> usize {
        let mut total = 0;
        for shard in self.shards.iter() {
            total += shard.read().await.len();
        }
        total
    }

    /// Check if all shards are empty
    ///
    /// Iterate over all shards and check if they are empty.
    /// If any shard is not empty, return false.
    /// If all shard are empty, return true.
    pub async fn is_empty(&self) -> bool {
        for shard in self.shards.iter() {
            if !shard.read().await.is_empty() {
                return false;
            }
        }
        true
    }
}

/// Async iterator over all key-value pairs
pub struct ShardedIter<'a, K, V> {
    map: &'a ShardedRwLockMap<K, V>,
    shard_idx: usize,
}

impl<'a, K, V> ShardedIter<'a, K, V>
where
    K: Eq + Hash + Shardable + Copy + 'a,
    V: 'a,
{
    pub async fn next(&mut self) -> Option<(K, RwLockReadGuard<'a, HashMap<K, V>>)> {
        loop {
            if self.shard_idx >= NUM_SHARDS {
                return None;
            }

            let shard = &self.map.shards[self.shard_idx];
            let read_guard = shard.read().await;

            if !read_guard.is_empty() {
                // Return the first key and the guard that protects the whole shard
                // Caller must not hold this guard longer than needed
                let key = *read_guard.keys().next()?;
                self.shard_idx += 1; // move to next shard on next call
                return Some((key, read_guard));
            }

            self.shard_idx += 1;
        }
    }
}
