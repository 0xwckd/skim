//! Learned access sets (PROJECT.md §7.2 "learned-set hygiene").
//!
//! A learned set is a prediction, not a cache. Three tiers, most specific first:
//! - **Exact input:** the last access set seen for the same calldata. Repeated identical calls
//!   (the same quote every block) fetch exactly what they touched last time.
//! - **Pattern:** inputs that differ only in "how much". Each argument word is classified:
//!   addresses and small integers (fee tier, direction, tick spacing, offsets) stay exact,
//!   anything else (amounts, price limits) is reduced to a magnitude bucket. So
//!   `quote(WETH→USDC, 0.3%, 1.2 ETH)` and `quote(WETH→USDC, 0.3%, 1.7 ETH)` share a pattern
//!   (same pool, direction and order of magnitude, so about the same tick range), while
//!   `USDC→WETH` or `100 ETH` do not. The prediction is the union of the pattern's recent runs.
//! - **Shape** (`to` + selector): only *core* keys, i.e. those seen in at least `min_freq` of
//!   the shape's recent runs, capped at `cap` by frequency. Input-specific keys (extra tick
//!   words for a large swap) come in later rounds instead of poisoning every first wave.
//!
//! Everything is bounded: entries per tier (least recently used evicted), and a global key
//! budget across tiers, evicting exact inputs first, then patterns, then shapes.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hash;

use alloy_primitives::{keccak256, Address, Bytes, B256, U256};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AccessKey {
    Account(Address),
    Storage(Address, U256),
}

pub type Shape = (Address, [u8; 4]);

/// Approximate memory per stored key (key + hash-map/vec overhead), for the byte estimate.
pub const KEY_BYTES: usize = 64;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub exact: usize,
    pub patterns: usize,
    pub shapes: usize,
    /// Total keys held across all tiers.
    pub keys: usize,
    /// Runs kept per pattern.
    pub pattern_window: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self { exact: 4096, patterns: 1024, shapes: 256, keys: 500_000, pattern_window: 4 }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct Sizes {
    pub exact: usize,
    pub exact_keys: usize,
    pub patterns: usize,
    pub pattern_keys: usize,
    pub shapes: usize,
    pub shape_keys: usize,
    pub keys: usize,
    pub bytes: usize,
    pub evicted: u64,
}

/// A map with least-recently-used order.
struct Lru<K, V> {
    map: HashMap<K, (V, u64)>,
    order: BTreeMap<u64, K>,
    tick: u64,
}

impl<K: Hash + Eq + Clone, V> Lru<K, V> {
    fn new() -> Self {
        Self { map: HashMap::new(), order: BTreeMap::new(), tick: 0 }
    }
    fn len(&self) -> usize {
        self.map.len()
    }
    fn touch(&mut self, k: &K) {
        if let Some((_, t)) = self.map.get_mut(k) {
            self.order.remove(t);
            self.tick += 1;
            *t = self.tick;
            self.order.insert(self.tick, k.clone());
        }
    }
    fn get(&self, k: &K) -> Option<&V> {
        self.map.get(k).map(|(v, _)| v)
    }
    /// Entry for `k` (inserted with `f` if absent), marked most recently used.
    fn entry(&mut self, k: K, f: impl FnOnce() -> V) -> &mut V {
        self.tick += 1;
        let t = self.tick;
        match self.map.get_mut(&k) {
            Some(e) => {
                self.order.remove(&e.1);
                e.1 = t;
            }
            None => {
                self.map.insert(k.clone(), (f(), t));
            }
        }
        self.order.insert(t, k.clone());
        &mut self.map.get_mut(&k).expect("present").0
    }
    fn pop_oldest(&mut self) -> Option<V> {
        let (_, k) = self.order.pop_first()?;
        self.map.remove(&k).map(|(v, _)| v)
    }
    fn values(&self) -> impl Iterator<Item = &V> {
        self.map.values().map(|(v, _)| v)
    }
}

pub struct Learned {
    window: usize,
    min_freq: f64,
    cap: usize,
    limits: Limits,
    exact: Lru<B256, Vec<AccessKey>>,
    patterns: Lru<B256, VecDeque<Vec<AccessKey>>>,
    shapes: Lru<Shape, VecDeque<Vec<AccessKey>>>,
    keys: usize,
    evicted: u64,
}

/// Kept for compatibility: the default bound on exact entries.
pub const EXACT_MAX: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Exact,
    Pattern,
    Core,
}

/// Class of one ABI argument word.
fn class(w: &[u8]) -> [u8; 33] {
    let v = U256::from_be_slice(w);
    let mut out = [0u8; 33];
    let exact = v < U256::from(1u32 << 24) // small ints: fee, direction, spacing, offsets, lengths
        || (v >= U256::from(1u8) << 128usize && v < U256::from(1u8) << 160usize) // addresses
        || v >= U256::MAX - (U256::from(1u8) << 24usize); // small negative ints (ticks)
    if exact {
        out[0] = 0;
        out[1..].copy_from_slice(w);
    } else {
        // Magnitude bucket: a factor of 4 per bucket.
        out[0] = 1;
        out[1] = (v.bit_len() / 2) as u8;
    }
    out
}

/// Pattern id: `to`, selector and each argument word's class. `None` for calls without
/// arguments (exact already covers them).
pub fn pattern(to: Address, data: &Bytes) -> Option<B256> {
    if data.len() < 4 + 32 {
        return None;
    }
    let mut buf = Vec::with_capacity(24 + data.len() / 32 * 33 + 8);
    buf.extend_from_slice(to.as_slice());
    buf.extend_from_slice(&data[..4]);
    let args = &data[4..];
    for w in args.chunks(32) {
        if w.len() == 32 {
            buf.extend_from_slice(&class(w));
        } else {
            buf.extend_from_slice(w); // trailing bytes: exact
        }
    }
    Some(keccak256(buf))
}

impl Learned {
    pub fn new(window: usize, min_freq: f64, cap: usize) -> Self {
        Self::with_limits(window, min_freq, cap, Limits::default())
    }

    pub fn with_limits(window: usize, min_freq: f64, cap: usize, limits: Limits) -> Self {
        Self {
            window,
            min_freq,
            cap,
            limits,
            exact: Lru::new(),
            patterns: Lru::new(),
            shapes: Lru::new(),
            keys: 0,
            evicted: 0,
        }
    }

    fn shape(to: Address, data: &Bytes) -> Shape {
        let mut sel = [0u8; 4];
        let n = 4.min(data.len());
        sel[..n].copy_from_slice(&data[..n]);
        (to, sel)
    }

    fn push_run(q: &mut VecDeque<Vec<AccessKey>>, run: Vec<AccessKey>, window: usize) -> isize {
        let mut delta = run.len() as isize;
        q.push_back(run);
        while q.len() > window {
            delta -= q.pop_front().map_or(0, |r| r.len()) as isize;
        }
        delta
    }

    fn add(&mut self, delta: isize) {
        self.keys = (self.keys as isize + delta).max(0) as usize;
    }

    pub fn record(&mut self, to: Address, data: &Bytes, access: Vec<AccessKey>) {
        let (window, pw) = (self.window, self.limits.pattern_window);
        let d = Self::push_run(self.shapes.entry(Self::shape(to, data), VecDeque::new), access.clone(), window);
        self.add(d);
        if let Some(p) = pattern(to, data) {
            let d = Self::push_run(self.patterns.entry(p, VecDeque::new), access.clone(), pw);
            self.add(d);
        }
        let n = access.len() as isize;
        let old = std::mem::replace(self.exact.entry(keccak256([to.as_slice(), data].concat()), Vec::new), access);
        self.add(n - old.len() as isize);
        self.enforce();
    }

    fn enforce(&mut self) {
        let runs = |q: VecDeque<Vec<AccessKey>>| q.iter().map(Vec::len).sum::<usize>();
        while self.exact.len() > self.limits.exact {
            let n = self.exact.pop_oldest().map_or(0, |v| v.len());
            self.add(-(n as isize));
            self.evicted += 1;
        }
        while self.patterns.len() > self.limits.patterns {
            let n = self.patterns.pop_oldest().map_or(0, runs);
            self.add(-(n as isize));
            self.evicted += 1;
        }
        while self.shapes.len() > self.limits.shapes {
            let n = self.shapes.pop_oldest().map_or(0, runs);
            self.add(-(n as isize));
            self.evicted += 1;
        }
        while self.keys > self.limits.keys {
            let n = if let Some(v) = self.exact.pop_oldest() {
                v.len()
            } else if let Some(q) = self.patterns.pop_oldest() {
                runs(q)
            } else if let Some(q) = self.shapes.pop_oldest() {
                runs(q)
            } else {
                break;
            };
            self.add(-(n as isize));
            self.evicted += 1;
        }
    }

    pub fn sizes(&self) -> Sizes {
        let runs = |q: &VecDeque<Vec<AccessKey>>| q.iter().map(Vec::len).sum::<usize>();
        let s = Sizes {
            exact: self.exact.len(),
            exact_keys: self.exact.values().map(Vec::len).sum(),
            patterns: self.patterns.len(),
            pattern_keys: self.patterns.values().map(runs).sum(),
            shapes: self.shapes.len(),
            shape_keys: self.shapes.values().map(runs).sum(),
            keys: self.keys,
            bytes: self.keys * KEY_BYTES,
            evicted: self.evicted,
        };
        debug_assert_eq!(s.keys, s.exact_keys + s.pattern_keys + s.shape_keys);
        s
    }

    fn by_freq<'a>(runs: impl Iterator<Item = &'a Vec<AccessKey>>, need: usize, cap: usize) -> Vec<AccessKey> {
        let mut freq: HashMap<AccessKey, usize> = HashMap::new();
        for run in runs {
            for k in run.iter().collect::<HashSet<_>>() {
                *freq.entry(*k).or_default() += 1;
            }
        }
        let mut core: Vec<_> = freq.into_iter().filter(|(_, n)| *n >= need).collect();
        core.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        core.truncate(cap);
        core.into_iter().map(|(k, _)| k).collect()
    }

    /// Prediction for an input. Marks the entries it used as recently used.
    pub fn predict_touch(&mut self, to: Address, data: &Bytes) -> Option<(Source, Vec<AccessKey>)> {
        let id = keccak256([to.as_slice(), data].concat());
        self.exact.touch(&id);
        if let Some(p) = pattern(to, data) {
            self.patterns.touch(&p);
        }
        self.shapes.touch(&Self::shape(to, data));
        self.predict(to, data)
    }

    pub fn predict(&self, to: Address, data: &Bytes) -> Option<(Source, Vec<AccessKey>)> {
        if let Some(keys) = self.exact.get(&keccak256([to.as_slice(), data].concat())) {
            return Some((Source::Exact, keys.clone()));
        }
        if let Some(runs) = pattern(to, data).and_then(|p| self.patterns.get(&p)) {
            // Union of similar inputs' runs, most frequent first.
            return Some((Source::Pattern, Self::by_freq(runs.iter(), 1, self.cap)));
        }
        let runs = self.shapes.get(&Self::shape(to, data))?;
        let need = ((runs.len() as f64) * self.min_freq).ceil().max(1.0) as usize;
        Some((Source::Core, Self::by_freq(runs.iter(), need, self.cap)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(i: u64) -> AccessKey {
        AccessKey::Storage(Address::ZERO, U256::from(i))
    }

    fn word(v: U256) -> [u8; 32] {
        v.to_be_bytes()
    }

    /// quoteExactInputSingle-like calldata: (tokenIn, tokenOut, amount, fee, limit).
    fn quote(tin: u8, tout: u8, amount: u128, fee: u32) -> Bytes {
        let mut d = vec![0xc6, 0xa5, 0x02, 0x6a];
        d.extend(word(U256::from_be_slice(Address::repeat_byte(tin).as_slice())));
        d.extend(word(U256::from_be_slice(Address::repeat_byte(tout).as_slice())));
        d.extend(word(U256::from(amount)));
        d.extend(word(U256::from(fee)));
        d.extend(word(U256::ZERO));
        d.into()
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
    fn patterns_generalize_amounts_but_not_pools_or_direction() {
        let to = Address::repeat_byte(9);
        let e18 = 10u128.pow(18);
        let mut l = Learned::new(8, 0.5, 256);
        // A large WETH->USDC swap reaches far ticks (keys 100..140)...
        l.record(to, &quote(1, 2, 50 * e18, 3000), (100..140).map(st).collect());
        // ...a small one only the near ones.
        l.record(to, &quote(1, 2, e18 + 1, 3000), vec![st(1), st(2)]);

        // Same pool and direction, similar size: the small input's set, not the far ticks.
        let (src, keys) = l.predict(to, &quote(1, 2, e18 + 12345, 3000)).unwrap();
        assert_eq!(src, Source::Pattern);
        assert_eq!(keys, vec![st(1), st(2)]);
        // Similar size to the large swap: its far ticks.
        let (src, keys) = l.predict(to, &quote(1, 2, 51 * e18, 3000)).unwrap();
        assert_eq!(src, Source::Pattern);
        assert_eq!(keys.len(), 40);
        // Other direction / other fee tier: no pattern, shape core only.
        assert_eq!(l.predict(to, &quote(2, 1, e18, 3000)).unwrap().0, Source::Core);
        assert_eq!(l.predict(to, &quote(1, 2, e18, 500)).unwrap().0, Source::Core);
        // Address and small-int words are exact, amounts bucketed.
        assert_ne!(pattern(to, &quote(1, 2, e18, 3000)), pattern(to, &quote(1, 3, e18, 3000)));
        assert_eq!(pattern(to, &quote(1, 2, e18, 3000)), pattern(to, &quote(1, 2, e18 + e18 / 2, 3000)));
        assert_ne!(pattern(to, &quote(1, 2, e18, 3000)), pattern(to, &quote(1, 2, 100 * e18, 3000)));
    }

    #[test]
    fn exact_entries_are_bounded() {
        let input = |i: u32| Bytes::from([[1, 2, 3, 4], i.to_be_bytes()].concat());
        let mut l = Learned::new(8, 0.5, 256);
        for i in 0..(EXACT_MAX as u32 + 100) {
            l.record(Address::ZERO, &input(i), vec![st(1)]);
        }
        assert_eq!(l.sizes().exact, EXACT_MAX);
        // The oldest inputs were evicted, the newest kept.
        assert!(l.predict(Address::ZERO, &input(0)).is_some_and(|(s, _)| s == Source::Core));
        assert!(l.predict(Address::ZERO, &input(EXACT_MAX as u32 + 99)).is_some_and(|(s, _)| s == Source::Exact));
    }

    #[test]
    fn global_key_budget_and_lru() {
        let limits = Limits { exact: 1000, patterns: 1000, shapes: 2, keys: 1000, pattern_window: 4 };
        let mut l = Learned::with_limits(8, 0.5, 256, limits);
        let to = Address::repeat_byte(3);
        // Random-ish inputs with 50 keys each: without the budget this is 3 * 50 * 200 keys.
        for i in 0..200u128 {
            l.record(to, &quote(1, 2, 1000 + i * 7919, 3000), (i as u64 * 50..i as u64 * 50 + 50).map(st).collect());
            let s = l.sizes();
            assert!(s.keys <= 1000, "{s:?}");
            assert_eq!(s.keys, s.exact_keys + s.pattern_keys + s.shape_keys);
        }
        assert!(l.sizes().evicted > 0);
        assert_eq!(l.sizes().bytes, l.sizes().keys * KEY_BYTES);

        // Shapes: LRU, so a recently predicted shape survives a new one.
        let (a, b, c) = (Address::repeat_byte(4), Address::repeat_byte(5), Address::repeat_byte(6));
        let d = Bytes::from(vec![1, 2, 3, 4]);
        let mut l = Learned::with_limits(8, 0.5, 256, Limits { shapes: 2, ..Limits::default() });
        l.record(a, &d, vec![st(1)]);
        l.record(b, &d, vec![st(2)]);
        l.predict_touch(a, &d);
        l.record(c, &d, vec![st(3)]);
        assert!(l.shapes.get(&(a, [1, 2, 3, 4])).is_some());
        assert!(l.shapes.get(&(b, [1, 2, 3, 4])).is_none());
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
