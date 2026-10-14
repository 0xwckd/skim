//! Adaptive SNAP scheduler (PROJECT.md §6.3.1, §7.2).
//!
//! The scheduler, not a peer session, owns every request:
//! - **Dedup:** one job per `(root, key)`. Concurrent waiters share the result.
//! - **Placement:** the least-loaded fast peer, with a per-peer in-flight cap.
//! - **Recovery:** on a disconnect, timeout or "root unavailable", the job is re-queued for
//!   another peer. Nothing is lost when a session drops.
//! - **Hedging:** a job that is slow on one peer gets one duplicate on another. The first
//!   answer wins and later answers are ignored.
//! - **Batching:** queued code hashes go out together in one `GetByteCodes`.
//!
//! Network access sits behind [`Transport`], so the policy is testable without peers.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use parking_lot::Mutex;
use tokio::sync::{oneshot, Notify};

pub mod el;

pub type PeerId = alloy_primitives::B512;

/// What a lookup asks for. Account and storage keys are pinned to a state root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Key {
    Account { root: B256, address: Address },
    Storage { root: B256, address: Address, slot: U256 },
    Code(B256),
}

impl Key {
    fn root(&self) -> Option<B256> {
        match *self {
            Key::Account { root, .. } | Key::Storage { root, .. } => Some(root),
            Key::Code(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountState {
    pub nonce: u64,
    pub balance: U256,
    pub storage_root: B256,
    pub code_hash: B256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `None` = absent at that root (trusted unless proofs are verified, §8).
    Account(Option<AccountState>),
    Storage(U256),
    Code(Bytes),
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum FetchError {
    #[error("gave up after {0} attempts")]
    Exhausted(u32),
    #[error("deadline exceeded")]
    Deadline,
}

/// A peer's answer to a point lookup.
#[derive(Debug, Clone)]
pub enum AccountAnswer {
    /// The peer can't serve the root (empty response without proof).
    Unavailable,
    Found(Option<AccountState>),
}

#[derive(Debug, Clone)]
pub enum SlotAnswer {
    /// Empty without proof: either the root is unavailable at this peer or the storage trie is
    /// empty. Resolved by asking more peers.
    EmptyNoProof,
    Found(U256),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum TransportError {
    #[error("timeout")]
    Timeout,
    #[error("peer gone")]
    Disconnected,
    #[error("{0}")]
    Other(String),
}

pub trait Transport: Clone + Send + Sync + 'static {
    /// Peers that can currently serve SNAP requests.
    fn snap_peers(&self) -> Vec<PeerId>;
    fn account(
        &self,
        peer: PeerId,
        root: B256,
        address: Address,
    ) -> impl Future<Output = Result<AccountAnswer, TransportError>> + Send;
    fn storage(
        &self,
        peer: PeerId,
        root: B256,
        address: Address,
        slot: U256,
    ) -> impl Future<Output = Result<SlotAnswer, TransportError>> + Send;
    /// Returns whatever subset of `hashes` the peer had.
    fn codes(
        &self,
        peer: PeerId,
        hashes: Vec<B256>,
    ) -> impl Future<Output = Result<Vec<Bytes>, TransportError>> + Send;
}

#[derive(Debug, Clone)]
pub struct Config {
    pub per_peer_inflight: usize,
    pub max_attempts: u32,
    /// Send one duplicate to another peer if no answer after this long.
    pub hedge_after: Duration,
    pub deadline: Duration,
    pub code_batch: usize,
    /// Empty-without-proof answers from this many distinct peers resolve a slot as zero.
    pub empty_quorum: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            per_peer_inflight: 8,
            max_attempts: 8,
            hedge_after: Duration::from_millis(1200),
            deadline: Duration::from_secs(15),
            code_batch: 32,
            empty_quorum: 2,
        }
    }
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct Stats {
    pub jobs: u64,
    pub dedup_hits: u64,
    pub requests: u64,
    pub ok: u64,
    pub root_unavailable: u64,
    pub errors: u64,
    pub disconnects: u64,
    pub requeued: u64,
    pub hedges: u64,
    pub late_answers_ignored: u64,
    pub failed: u64,
    pub code_batches: u64,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct PeerStats {
    pub ok: u64,
    pub errors: u64,
    pub ewma_ms: f64,
    pub inflight: usize,
}

struct Job {
    waiters: Vec<oneshot::Sender<Result<Value, FetchError>>>,
    attempts: u32,
    inflight: u32,
    tried: HashSet<PeerId>,
    empty_answers: HashSet<PeerId>,
    created: Instant,
    last_sent: Option<Instant>,
    hedged: bool,
    queued: bool,
}

#[derive(Default)]
struct Peer {
    inflight: usize,
    ewma_ms: Option<f64>,
    ok: u64,
    errors: u64,
    /// Roots this peer couldn't serve, and when. Retried after `ROOT_RETRY`: at the head a peer
    /// may simply not have executed the block yet.
    unavailable_roots: HashMap<B256, Instant>,
}

const ROOT_RETRY: Duration = Duration::from_millis(400);

impl Peer {
    fn score(&self) -> f64 {
        // Unmeasured peers look average-fast so they get tried.
        self.ewma_ms.unwrap_or(300.0) * (1.0 + self.inflight as f64)
    }
}

#[derive(Default)]
struct State {
    jobs: HashMap<Key, Job>,
    queue: VecDeque<Key>,
    peers: HashMap<PeerId, Peer>,
    stats: Stats,
}

pub struct Scheduler<T: Transport> {
    inner: Arc<Inner<T>>,
}

impl<T: Transport> Clone for Scheduler<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

struct Inner<T> {
    transport: T,
    cfg: Config,
    state: Mutex<State>,
    wake: Notify,
}

impl<T: Transport> Scheduler<T> {
    /// Spawns the dispatcher. Must be called inside a Tokio runtime.
    pub fn new(transport: T, cfg: Config) -> Self {
        let inner = Arc::new(Inner { transport, cfg, state: Mutex::default(), wake: Notify::new() });
        let weak = Arc::downgrade(&inner);
        tokio::spawn(async move {
            loop {
                // Holds a strong ref for at most one tick, so dropping the scheduler stops this.
                let Some(inner) = weak.upgrade() else { return };
                Inner::dispatch(&inner);
                let _ = tokio::time::timeout(Duration::from_millis(25), inner.wake.notified()).await;
            }
        });
        Self { inner }
    }

    /// Fetches a wave of keys concurrently. Results are in input order.
    pub async fn fetch(&self, keys: &[Key]) -> Vec<Result<Value, FetchError>> {
        let rxs: Vec<_> = {
            let mut st = self.inner.state.lock();
            keys.iter().map(|k| self.inner.enqueue(&mut st, *k)).collect()
        };
        self.inner.wake.notify_one();
        futures::future::join_all(rxs.into_iter().map(|rx| async move {
            rx.await.unwrap_or(Err(FetchError::Deadline))
        }))
        .await
    }

    pub fn stats(&self) -> Stats {
        self.inner.state.lock().stats.clone()
    }

    pub fn peer_stats(&self) -> HashMap<PeerId, PeerStats> {
        self.inner
            .state
            .lock()
            .peers
            .iter()
            .map(|(id, p)| {
                (*id, PeerStats { ok: p.ok, errors: p.errors, ewma_ms: p.ewma_ms.unwrap_or(0.0), inflight: p.inflight })
            })
            .collect()
    }
}

impl<T: Transport> Inner<T> {
    fn enqueue(&self, st: &mut State, key: Key) -> oneshot::Receiver<Result<Value, FetchError>> {
        let (tx, rx) = oneshot::channel();
        if let Some(job) = st.jobs.get_mut(&key) {
            job.waiters.push(tx);
            st.stats.dedup_hits += 1;
            return rx;
        }
        st.jobs.insert(
            key,
            Job {
                waiters: vec![tx],
                attempts: 0,
                inflight: 0,
                tried: HashSet::new(),
                empty_answers: HashSet::new(),
                created: Instant::now(),
                last_sent: None,
                hedged: false,
                queued: true,
            },
        );
        st.queue.push_back(key);
        st.stats.jobs += 1;
        rx
    }

    fn finish(st: &mut State, key: &Key, result: Result<Value, FetchError>) {
        if let Some(job) = st.jobs.remove(key) {
            if result.is_err() {
                st.stats.failed += 1;
            }
            for w in job.waiters {
                let _ = w.send(result.clone());
            }
        }
    }

    fn requeue(st: &mut State, key: Key) {
        if let Some(job) = st.jobs.get_mut(&key) {
            if !job.queued && job.inflight == 0 {
                job.queued = true;
                st.queue.push_front(key);
                st.stats.requeued += 1;
            }
        }
    }

    fn pick_peer(&self, st: &State, key: &Key, job: &Job) -> Option<PeerId> {
        let root = key.root();
        let eligible = |(id, p): &(&PeerId, &Peer)| {
            p.inflight < self.cfg.per_peer_inflight
                && root.is_none_or(|r| p.unavailable_roots.get(&r).is_none_or(|t| t.elapsed() > ROOT_RETRY))
                && !job.empty_answers.contains(*id)
        };
        let best = |untried_only: bool| {
            st.peers
                .iter()
                .filter(eligible)
                .filter(|(id, _)| !untried_only || !job.tried.contains(*id))
                .min_by(|a, b| a.1.score().total_cmp(&b.1.score()))
                .map(|(id, _)| *id)
        };
        best(true).or_else(|| best(false))
    }

    fn dispatch(self: &Arc<Self>) {
        let mut st = self.state.lock();

        // Sync the peer table. In-flight requests to vanished peers fail and get re-queued.
        let live: HashSet<PeerId> = self.transport.snap_peers().into_iter().collect();
        st.peers.retain(|id, _| live.contains(id));
        for id in live {
            st.peers.entry(id).or_default();
        }

        // Deadlines and hedges.
        let now = Instant::now();
        let mut expired = Vec::new();
        let mut hedge = Vec::new();
        for (k, j) in &st.jobs {
            if now.duration_since(j.created) > self.cfg.deadline {
                expired.push(*k);
            } else if j.inflight == 1
                && !j.hedged
                && !j.queued
                && j.last_sent.is_some_and(|t| now.duration_since(t) > self.cfg.hedge_after)
            {
                hedge.push(*k);
            }
        }
        for k in expired {
            Self::finish(&mut st, &k, Err(FetchError::Deadline));
        }
        for k in hedge {
            let j = st.jobs.get_mut(&k).unwrap();
            j.hedged = true;
            j.queued = true;
            st.queue.push_front(k);
            st.stats.hedges += 1;
        }

        // Place queued jobs.
        let mut deferred = VecDeque::new();
        while let Some(key) = st.queue.pop_front() {
            let Some(job) = st.jobs.get(&key) else { continue };
            let Some(peer) = self.pick_peer(&st, &key, job) else {
                deferred.push_back(key);
                continue;
            };
            let mut keys = vec![key];
            if matches!(key, Key::Code(_)) {
                // Batch further queued code hashes onto the same request.
                let mut rest = VecDeque::new();
                while let Some(k) = st.queue.pop_front() {
                    if matches!(k, Key::Code(_)) && keys.len() < self.cfg.code_batch && st.jobs.contains_key(&k) {
                        keys.push(k);
                    } else {
                        rest.push_back(k);
                    }
                }
                st.queue = rest;
                st.stats.code_batches += 1;
            }
            for k in &keys {
                let j = st.jobs.get_mut(k).unwrap();
                j.queued = false;
                j.inflight += 1;
                j.attempts += 1;
                j.tried.insert(peer);
                j.last_sent = Some(Instant::now());
            }
            st.peers.get_mut(&peer).unwrap().inflight += 1;
            st.stats.requests += 1;
            let this = self.clone();
            tokio::spawn(async move { this.run(peer, keys).await });
        }
        st.queue = deferred;
    }

    async fn run(self: Arc<Self>, peer: PeerId, keys: Vec<Key>) {
        let t = &self.transport;
        let start = Instant::now();
        enum Outcome {
            Values(Vec<(Key, Value)>),
            Unavailable,
            Empty,
            Err(TransportError),
        }
        let outcome = match keys[0] {
            Key::Account { root, address } => match t.account(peer, root, address).await {
                Ok(AccountAnswer::Found(a)) => Outcome::Values(vec![(keys[0], Value::Account(a))]),
                Ok(AccountAnswer::Unavailable) => Outcome::Unavailable,
                Err(e) => Outcome::Err(e),
            },
            Key::Storage { root, address, slot } => match t.storage(peer, root, address, slot).await {
                Ok(SlotAnswer::Found(v)) => Outcome::Values(vec![(keys[0], Value::Storage(v))]),
                Ok(SlotAnswer::EmptyNoProof) => Outcome::Empty,
                Err(e) => Outcome::Err(e),
            },
            Key::Code(_) => {
                let hashes: Vec<B256> =
                    keys.iter().map(|k| if let Key::Code(h) = k { *h } else { unreachable!() }).collect();
                match t.codes(peer, hashes).await {
                    Ok(codes) => Outcome::Values(
                        codes.into_iter().map(|c| (Key::Code(keccak256(&c)), Value::Code(c))).collect(),
                    ),
                    Err(e) => Outcome::Err(e),
                }
            }
        };
        let elapsed_ms = start.elapsed().as_secs_f64() * 1e3;

        let mut st = self.state.lock();
        for k in &keys {
            if let Some(j) = st.jobs.get_mut(k) {
                j.inflight = j.inflight.saturating_sub(1);
            }
        }
        let gone = !st.peers.contains_key(&peer);
        if let Some(p) = st.peers.get_mut(&peer) {
            p.inflight = p.inflight.saturating_sub(1);
        }

        match outcome {
            Outcome::Values(values) => {
                st.stats.ok += 1;
                if let Some(p) = st.peers.get_mut(&peer) {
                    p.ok += 1;
                    p.ewma_ms = Some(p.ewma_ms.map_or(elapsed_ms, |e| 0.8 * e + 0.2 * elapsed_ms));
                }
                let mut answered = HashSet::new();
                for (k, v) in values {
                    if keys.contains(&k) {
                        answered.insert(k);
                        if st.jobs.contains_key(&k) {
                            Self::finish(&mut st, &k, Ok(v));
                        } else {
                            st.stats.late_answers_ignored += 1;
                        }
                    }
                }
                // Code hashes this peer didn't have go back to the queue.
                for k in keys.iter().filter(|k| !answered.contains(k)) {
                    Self::retry_or_fail(&self.cfg, &mut st, *k);
                }
            }
            Outcome::Unavailable => {
                st.stats.root_unavailable += 1;
                if let (Some(p), Some(root)) = (st.peers.get_mut(&peer), keys[0].root()) {
                    if p.unavailable_roots.len() > 256 {
                        p.unavailable_roots.retain(|_, t| t.elapsed() < Duration::from_secs(60));
                    }
                    p.unavailable_roots.insert(root, Instant::now());
                }
                Self::retry_or_fail(&self.cfg, &mut st, keys[0]);
            }
            Outcome::Empty => {
                let k = keys[0];
                let quorum = self.cfg.empty_quorum.min(st.peers.len().max(1) as u32);
                let n = st.jobs.get_mut(&k).map(|j| {
                    j.empty_answers.insert(peer);
                    j.empty_answers.len() as u32
                });
                match n {
                    Some(n) if n >= quorum => Self::finish(&mut st, &k, Ok(Value::Storage(U256::ZERO))),
                    Some(_) => Self::retry_or_fail(&self.cfg, &mut st, k),
                    None => st.stats.late_answers_ignored += 1,
                }
            }
            Outcome::Err(e) => {
                if gone || matches!(e, TransportError::Disconnected) {
                    st.stats.disconnects += 1;
                } else {
                    st.stats.errors += 1;
                }
                if let Some(p) = st.peers.get_mut(&peer) {
                    p.errors += 1;
                    // Penalize slow/failing peers so placement drifts away from them.
                    p.ewma_ms = Some(p.ewma_ms.unwrap_or(300.0) * 1.5);
                }
                for k in &keys {
                    Self::retry_or_fail(&self.cfg, &mut st, *k);
                }
            }
        }
        drop(st);
        self.wake.notify_one();
    }

    fn retry_or_fail(cfg: &Config, st: &mut State, key: Key) {
        let Some(job) = st.jobs.get(&key) else { return };
        if job.inflight > 0 {
            return; // a hedge is still out
        }
        if job.attempts >= cfg.max_attempts {
            let n = job.attempts;
            Self::finish(st, &key, Err(FetchError::Exhausted(n)));
        } else {
            Self::requeue(st, key);
        }
    }
}

#[cfg(test)]
mod tests;
