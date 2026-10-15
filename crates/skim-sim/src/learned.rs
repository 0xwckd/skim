//! Learned access sets (PROJECT.md §7.2 "learned-set hygiene").
//!
//! A learned set is a prediction, not a cache:
//! - **Exact input:** the last access set seen for the same calldata. Repeated identical calls
//!   (the same quote every block) fetch exactly what they touched last time.
//! - **New input of a known shape:** only *core* keys, i.e. those seen in at least `min_freq`
//!   of the shape's recent runs, capped at `cap` by frequency. Input-specific keys (extra tick
//!   words for a large swap) come in later rounds instead of poisoning every first wave.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

use alloy_primitives::{keccak256, Address, Bytes, B256, U256};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AccessKey {
    Account(Address),
    Storage(Address, U256),
}

pub type Shape = (Address, [u8; 4]);

pub struct Learned {
    window: usize,
    min_freq: f64,
    cap: usize,
    exact: HashMap<B256, Vec<AccessKey>>,
    /// Insertion order of `exact`, oldest first: bounded at `EXACT_MAX` entries. Every new
    /// calldata (e.g. a random quote) adds an entry, so without a bound this grows forever.
    exact_order: VecDeque<B256>,
    recent: HashMap<Shape, VecDeque<Vec<AccessKey>>>,
}

pub const EXACT_MAX: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Exact,
    Core,
}

impl Learned {
    pub fn new(window: usize, min_freq: f64, cap: usize) -> Self {
        Self { window, min_freq, cap, exact: HashMap::new(), exact_order: VecDeque::new(), recent: HashMap::new() }
    }

    fn shape(to: Address, data: &Bytes) -> Shape {
        let mut sel = [0u8; 4];
        sel.copy_from_slice(&data[..4.min(data.len())]);
        (to, sel)
    }

    pub fn record(&mut self, to: Address, data: &Bytes, access: Vec<AccessKey>) {
        let runs = self.recent.entry(Self::shape(to, data)).or_default();
        runs.push_back(access.clone());
        while runs.len() > self.window {
            runs.pop_front();
        }
        let id = keccak256([to.as_slice(), data].concat());
        if self.exact.insert(id, access).is_none() {
            self.exact_order.push_back(id);
            while self.exact_order.len() > EXACT_MAX {
                if let Some(old) = self.exact_order.pop_front() {
                    self.exact.remove(&old);
                }
            }
        }
    }

    /// (exact entries, keys held by exact entries, shapes, keys held by shape windows)
    pub fn sizes(&self) -> (usize, usize, usize, usize) {
        (
            self.exact.len(),
            self.exact.values().map(Vec::len).sum(),
            self.recent.len(),
            self.recent.values().flat_map(|q| q.iter()).map(Vec::len).sum(),
        )
    }

    pub fn predict(&self, to: Address, data: &Bytes) -> Option<(Source, Vec<AccessKey>)> {
        if let Some(keys) = self.exact.get(&keccak256([to.as_slice(), data].concat())) {
            return Some((Source::Exact, keys.clone()));
        }
        let runs = self.recent.get(&Self::shape(to, data))?;
        let mut freq: HashMap<AccessKey, usize> = HashMap::new();
        for run in runs {
            for k in run {
                *freq.entry(*k).or_default() += 1;
            }
        }
        let need = ((runs.len() as f64) * self.min_freq).ceil().max(1.0) as usize;
        let mut core: Vec<_> = freq.into_iter().filter(|(_, n)| *n >= need).collect();
        core.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        core.truncate(self.cap);
        Some((Source::Core, core.into_iter().map(|(k, _)| k).collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(i: u64) -> AccessKey {
        AccessKey::Storage(Address::ZERO, U256::from(i))
    }

    #[test]
    fn exact_beats_core_and_core_drops_outliers() {
        let to = Address::repeat_byte(1);
        let small = Bytes::from(vec![0xc6, 0xa5, 0x02, 0x6a, 1]);
        let big = Bytes::from(vec![0xc6, 0xa5, 0x02, 0x6a, 2]);
        let new = Bytes::from(vec![0xc6, 0xa5, 0x02, 0x6a, 3]);
        let mut l = Learned::new(8, 0.5, 256);
        l.record(to, &small, vec![st(1), st(2)]);
        l.record(to, &small, vec![st(1), st(2)]);
        // One big input touching many extra keys must not poison new inputs.
        l.record(to, &big, (1..200).map(st).collect());

        let (src, keys) = l.predict(to, &small).unwrap();
        assert_eq!(src, Source::Exact);
        assert_eq!(keys, vec![st(1), st(2)]);

        let (src, keys) = l.predict(to, &new).unwrap();
        assert_eq!(src, Source::Core);
        assert_eq!(keys, vec![st(1), st(2)]); // seen in 3/3; the 197 big-only keys in 1/3

        assert!(l.predict(Address::repeat_byte(2), &new).is_none());
    }

    #[test]
    fn exact_entries_are_bounded() {
        let mut l = Learned::new(8, 0.5, 256);
        for i in 0..(EXACT_MAX as u32 + 100) {
            l.record(Address::ZERO, &Bytes::from(i.to_be_bytes().to_vec()), vec![st(1)]);
        }
        assert_eq!(l.sizes().0, EXACT_MAX);
        // The oldest inputs were evicted, the newest kept.
        assert!(l.predict(Address::ZERO, &Bytes::from(0u32.to_be_bytes().to_vec())).is_some_and(|(s, _)| s == Source::Core));
        let last = (EXACT_MAX as u32 + 99).to_be_bytes().to_vec();
        assert!(l.predict(Address::ZERO, &Bytes::from(last)).is_some_and(|(s, _)| s == Source::Exact));
    }

    #[test]
    fn cap_limits_core() {
        let to = Address::ZERO;
        let d = Bytes::from(vec![1, 2, 3, 4]);
        let mut l = Learned::new(4, 0.5, 10);
        l.record(to, &d, (0..100).map(st).collect());
        let d2 = Bytes::from(vec![1, 2, 3, 4, 9]);
        assert_eq!(l.predict(to, &d2).unwrap().1.len(), 10);
    }
}
