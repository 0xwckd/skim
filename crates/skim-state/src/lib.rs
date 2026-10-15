//! Per-key MVCC state store driven by EIP-7928 block access lists (PROJECT.md §5, §6).
//!
//! - **Versions:** each tracked key keeps `(block number, value)` versions along the
//!   *selected* chain. A snapshot of block `n` reads the newest version at or below `n`.
//! - **Unknown is not zero:** a key with no version at or below `n` reads as
//!   [`StateValue::Unknown`].
//! - **BAL index:** recent blocks' normalized writes are kept, so a value fetched at block `P`
//!   (a SNAP base) is committed together with every later write to that key ([`Store::insert_base`]).
//!   A base never reaches backwards: it is valid for `P` and its descendants only.
//! - **Reorgs:** [`Store::rollback_to`] truncates versions and chain entries above the common
//!   ancestor. There is no separate undo journal.
//! - **Single writer:** all mutations go through `&self` methods under one lock. Readers take
//!   immutable [`Snapshot`]s. A snapshot whose block left the selected chain reports
//!   [`StateError::Reorged`] instead of reading another branch's data.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use parking_lot::RwLock;
pub use skim_bal::{BlockWrites, StateKey};
use tokio::sync::broadcast;

/// A block on the selected execution chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub state_root: B256,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateValue {
    Known(U256),
    Unknown,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum StateError {
    #[error("block {0} is not the child of the head {1}")]
    NotChild(B256, B256),
    #[error("block {number} ({hash}) is no longer on the selected chain")]
    Reorged { number: u64, hash: B256 },
    #[error("block {0} is outside the retained window")]
    OutOfWindow(u64),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreSizes {
    pub chain: usize,
    pub recent_bals: usize,
    pub keys: usize,
    pub versions: usize,
    pub watched_keys: usize,
    pub temp_keys: usize,
    pub watched_addresses: usize,
    pub code: usize,
    pub code_bytes: usize,
}

/// How long a fetched value is kept current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    /// Subscribed: kept current from every BAL until unsubscribed.
    Subscribed,
    /// On-demand: kept current for this many blocks past the head, then evicted with its
    /// versions (it reads as Unknown again and is refetched when needed).
    Ttl(u64),
}

/// Emitted after each applied block.
#[derive(Debug, Clone)]
pub struct StateReady {
    pub block: BlockRef,
    /// Tracked keys this block wrote.
    pub changed_keys: Arc<Vec<StateKey>>,
    /// Untracked addresses the block touched (prefetch candidates, §7.3).
    pub touched: usize,
}

#[derive(Default)]
struct Inner {
    chain: BTreeMap<u64, BlockRef>,
    versions: HashMap<StateKey, Vec<(u64, U256)>>,
    watched_addresses: HashSet<Address>,
    watched_keys: HashSet<StateKey>,
    /// On-demand keys and the block after which they expire (tracked until then).
    temp: HashMap<StateKey, u64>,
    recent: BTreeMap<u64, Arc<BlockWrites>>,
    code: HashMap<B256, Bytes>,
}

impl Inner {
    fn head(&self) -> Option<&BlockRef> {
        self.chain.values().next_back()
    }

    fn tracked(&self, k: &StateKey) -> bool {
        let a = match k {
            StateKey::Balance(a) | StateKey::Nonce(a) | StateKey::CodeHash(a) | StateKey::Storage(a, _) => a,
        };
        self.watched_addresses.contains(a) || self.watched_keys.contains(k) || self.temp.contains_key(k)
    }

    fn tracked_by_address(&self, k: &StateKey) -> bool {
        let a = match k {
            StateKey::Balance(a) | StateKey::Nonce(a) | StateKey::CodeHash(a) | StateKey::Storage(a, _) => a,
        };
        self.watched_addresses.contains(a)
    }

    fn put(&mut self, key: StateKey, number: u64, value: U256) {
        let v = self.versions.entry(key).or_default();
        match v.binary_search_by_key(&number, |(n, _)| *n) {
            Ok(i) => v[i].1 = value,
            Err(i) => v.insert(i, (number, value)),
        }
    }
}

pub struct Store {
    inner: RwLock<Inner>,
    window: u64,
    events: broadcast::Sender<StateReady>,
}

impl Store {
    /// `window`: blocks of history (chain entries, BAL index, versions) kept for reads, lazy
    /// fills and reorgs.
    pub fn new(window: u64) -> Arc<Self> {
        Arc::new(Self { inner: RwLock::default(), window, events: broadcast::channel(1024).0 })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StateReady> {
        self.events.subscribe()
    }

    /// Every BAL write to this address is recorded from now on.
    pub fn watch_address(&self, a: Address) {
        self.inner.write().watched_addresses.insert(a);
    }

    pub fn watch_key(&self, k: StateKey) {
        let mut st = self.inner.write();
        st.temp.remove(&k);
        st.watched_keys.insert(k);
    }

    /// Stops keeping `k` current (unless covered by a watched address) and drops its versions.
    pub fn unwatch_key(&self, k: &StateKey) {
        let mut st = self.inner.write();
        st.watched_keys.remove(k);
        st.temp.remove(k);
        if !st.tracked_by_address(k) {
            st.versions.remove(k);
        }
    }

    /// Extends on-demand keys that were just used (`cache_ttl_blocks` from the head).
    pub fn touch_ttl(&self, keys: &[StateKey], n: u64) {
        let mut st = self.inner.write();
        let Some(head) = st.head().map(|h| h.number) else { return };
        for k in keys {
            if let Some(e) = st.temp.get_mut(k) {
                *e = (*e).max(head + n);
            }
        }
    }

    pub fn is_subscribed(&self, k: &StateKey) -> bool {
        let st = self.inner.read();
        st.watched_keys.contains(k) || st.tracked_by_address(k)
    }

    pub fn head(&self) -> Option<BlockRef> {
        self.inner.read().head().copied()
    }

    pub fn block(&self, number: u64) -> Option<BlockRef> {
        self.inner.read().chain.get(&number).copied()
    }

    /// Applies a block whose BAL commitment has already been verified. The first block anchors
    /// the chain; later ones must extend the head.
    pub fn apply_block(&self, block: BlockRef, writes: Arc<BlockWrites>) -> Result<StateReady, StateError> {
        let ready = {
            let mut st = self.inner.write();
            if let Some(h) = st.head() {
                if block.parent_hash != h.hash || block.number != h.number + 1 {
                    return Err(StateError::NotChild(block.hash, h.hash));
                }
            }
            let mut changed = Vec::new();
            for &(k, v) in &writes.writes {
                if st.tracked(&k) {
                    st.put(k, block.number, v);
                    changed.push(k);
                }
            }
            for (h, c) in &writes.code {
                st.code.insert(*h, c.clone());
            }
            let touched = writes.touched.iter().filter(|a| !st.watched_addresses.contains(*a)).count();
            st.chain.insert(block.number, block);
            st.recent.insert(block.number, writes);
            self.gc(&mut st);
            StateReady { block, changed_keys: Arc::new(changed), touched }
        };
        let _ = self.events.send(ready.clone());
        Ok(ready)
    }

    /// Drops chain entries, BAL index and versions above `number` (reorg to that ancestor).
    pub fn rollback_to(&self, number: u64) {
        let mut st = self.inner.write();
        st.chain.retain(|n, _| *n <= number);
        st.recent.retain(|n, _| *n <= number);
        for v in st.versions.values_mut() {
            v.retain(|(n, _)| *n <= number);
        }
    }

    /// Commits a value fetched at `at` (e.g. via SNAP at `at.state_root`), then replays later
    /// indexed writes to `key`, so a stale fetch never hides a newer BAL write. Tracks the key.
    pub fn insert_base(&self, at: &BlockRef, key: StateKey, value: U256) -> Result<(), StateError> {
        self.insert_base_with(at, key, value, Retention::Subscribed)
    }

    /// [`Self::insert_base`] with an explicit retention. A TTL never downgrades a key that is
    /// already subscribed.
    pub fn insert_base_with(&self, at: &BlockRef, key: StateKey, value: U256, retention: Retention) -> Result<(), StateError> {
        let mut st = self.inner.write();
        if st.chain.get(&at.number).map(|b| b.hash) != Some(at.hash) {
            return Err(StateError::Reorged { number: at.number, hash: at.hash });
        }
        match retention {
            Retention::Subscribed => {
                st.temp.remove(&key);
                st.watched_keys.insert(key);
            }
            Retention::Ttl(n) => {
                let subscribed = st.watched_keys.contains(&key) || st.tracked_by_address(&key);
                if !subscribed {
                    let head = st.head().map_or(at.number, |h| h.number);
                    let e = st.temp.entry(key).or_insert(0);
                    *e = (*e).max(head + n);
                }
            }
        }
        // Keep an existing version at `at` (a BAL write at that block is authoritative).
        let has_at = st.versions.get(&key).is_some_and(|v| v.iter().any(|(n, _)| *n == at.number));
        if !has_at {
            st.put(key, at.number, value);
        }
        let later: Vec<(u64, U256)> = st
            .recent
            .range(at.number + 1..)
            .filter_map(|(n, w)| w.get(&key).map(|v| (*n, v)))
            .collect();
        for (n, v) in later {
            st.put(key, n, v);
        }
        Ok(())
    }

    /// Sizes of the store's structures (for growth monitoring).
    pub fn sizes(&self) -> StoreSizes {
        let st = self.inner.read();
        StoreSizes {
            chain: st.chain.len(),
            recent_bals: st.recent.len(),
            keys: st.versions.len(),
            versions: st.versions.values().map(Vec::len).sum(),
            watched_keys: st.watched_keys.len(),
            temp_keys: st.temp.len(),
            watched_addresses: st.watched_addresses.len(),
            code: st.code.len(),
            code_bytes: st.code.values().map(|c| c.len()).sum(),
        }
    }

    pub fn code(&self, hash: &B256) -> Option<Bytes> {
        self.inner.read().code.get(hash).cloned()
    }

    pub fn insert_code(&self, code: Bytes) -> B256 {
        let h = keccak256(&code);
        self.inner.write().code.insert(h, code);
        h
    }

    /// Snapshot of `block` (must be on the selected chain).
    pub fn snapshot(self: &Arc<Self>, block: &BlockRef) -> Result<Snapshot, StateError> {
        let st = self.inner.read();
        match st.chain.get(&block.number) {
            Some(b) if b.hash == block.hash => Ok(Snapshot { store: self.clone(), block: *block }),
            Some(_) => Err(StateError::Reorged { number: block.number, hash: block.hash }),
            None => Err(StateError::OutOfWindow(block.number)),
        }
    }

    /// Tracked keys that are known at `block`.
    pub fn known_keys(&self, block: &BlockRef) -> Vec<(StateKey, U256)> {
        let st = self.inner.read();
        st.versions
            .iter()
            .filter_map(|(k, v)| {
                let i = v.partition_point(|(n, _)| *n <= block.number);
                (i > 0).then(|| (*k, v[i - 1].1))
            })
            .collect()
    }

    fn gc(&self, st: &mut Inner) {
        let Some(head) = st.head().map(|h| h.number) else { return };
        // Expire on-demand keys: stop tracking and forget their versions.
        let expired: Vec<StateKey> = st.temp.iter().filter(|(_, e)| **e < head).map(|(k, _)| *k).collect();
        for k in expired {
            st.temp.remove(&k);
            st.versions.remove(&k);
        }
        let Some(cut) = head.checked_sub(self.window) else { return };
        st.chain.retain(|n, _| *n > cut);
        st.recent.retain(|n, _| *n > cut);
        for v in st.versions.values_mut() {
            // Collapse everything at or below the cut into one base version.
            let i = v.partition_point(|(n, _)| *n <= cut);
            if i > 1 {
                v.drain(..i - 1);
            }
        }
    }
}

/// An immutable view of one block's post-state.
#[derive(Clone)]
pub struct Snapshot {
    store: Arc<Store>,
    block: BlockRef,
}

impl Snapshot {
    pub fn block(&self) -> &BlockRef {
        &self.block
    }

    pub fn code(&self, hash: &B256) -> Option<Bytes> {
        self.store.code(hash)
    }

    pub fn get(&self, key: &StateKey) -> Result<StateValue, StateError> {
        let st = self.store.inner.read();
        if st.chain.get(&self.block.number).map(|b| b.hash) != Some(self.block.hash) {
            return Err(StateError::Reorged { number: self.block.number, hash: self.block.hash });
        }
        Ok(match st.versions.get(key) {
            Some(v) => {
                let i = v.partition_point(|(n, _)| *n <= self.block.number);
                if i == 0 { StateValue::Unknown } else { StateValue::Known(v[i - 1].1) }
            }
            None => StateValue::Unknown,
        })
    }
}

#[cfg(test)]
mod tests;
