//! Simulation over skim's MVCC snapshots (PROJECT.md §7).
//!
//! [`Simulator::call`] runs one input at a pinned block:
//! 1. **predict** the state it reads ([`predictor`]: learned sets + protocol-aware sources)
//! 2. **fetch** the unknown part in one wave (parent root + BAL replay, [`prefetch::resolve`])
//! 3. **execute** on the snapshot (revm, [`db::SnapDb`])
//! 4. **on a miss** (in `Resolve` mode): expand, fetch, re-run; record the access set
//!
//! In [`Mode::WarmOnly`] it never touches the network: missing state is an error, not a zero.

pub mod db;
pub mod env;
pub mod learned;
pub mod predictor;
pub mod prefetch;
pub mod subscription;

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes, U256};
use parking_lot::{Mutex, RwLock};
use revm::{
    Context, ExecuteEvm, MainBuilder, MainContext,
    context::{TxEnv, result::ExecutionResult},
    database::WrapDatabaseRef,
    primitives::TxKind,
};
use skim_bal::StateKey;
use skim_snap::{Scheduler, el::ElTransport};
use skim_state::{BlockRef, Retention, Snapshot, StateError, StateValue, Store};
use skim_snap::Priority;

pub use db::Access;
pub use prefetch::Resolve;

/// The part of a call the predictor keys on.
#[derive(Debug, Clone)]
pub struct CallInput {
    pub to: Address,
    pub data: Bytes,
}

#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// Never fetch; missing state is [`SimError::StateUnavailable`].
    WarmOnly,
    /// Predict, fetch and re-run up to `max_rounds` miss rounds.
    Resolve { max_rounds: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum SimError {
    #[error("state unavailable locally ({0} keys) in warm-only mode")]
    StateUnavailable(usize),
    #[error("state still incomplete after {0} fetch rounds")]
    Incomplete(usize),
    #[error(transparent)]
    State(#[from] StateError),
    #[error("no header for block {0}")]
    NoHeader(u64),
    #[error("state not archived at block {block}: {keys} key(s) uncovered (archived history starts where each key was first known)")]
    NotArchived { block: u64, keys: usize },
    #[error("evm: {0}")]
    Evm(String),
}

#[derive(Debug)]
pub struct CallOutcome {
    pub result: ExecutionResult,
    pub access: Access,
    /// The predictor's pre-execution fetch, if any keys were unknown.
    pub predicted: Option<Resolve>,
    /// Miss rounds after execution.
    pub rounds: Vec<Resolve>,
    /// Total local execution time over all runs.
    pub exec_us: f64,
}

pub struct Simulator {
    pub store: Arc<Store>,
    pub sched: Scheduler<ElTransport>,
    pub predictor: Mutex<predictor::Predictor>,
    headers: RwLock<BTreeMap<u64, Header>>,
    /// Resolve misses at the parent root (peers may not have executed the head yet).
    pub miss_at_parent: bool,
    /// Retention for state fetched on demand (unsubscribed): `Ttl(n)` keeps it current for n
    /// blocks, then evicts it. `Subscribed` keeps everything ever read (pre-M3 behaviour).
    pub on_demand: RwLock<Retention>,
}

impl Simulator {
    pub fn new(store: Arc<Store>, sched: Scheduler<ElTransport>, predictor: predictor::Predictor) -> Arc<Self> {
        Arc::new(Self {
            store,
            sched,
            predictor: Mutex::new(predictor),
            headers: RwLock::default(),
            miss_at_parent: true,
            on_demand: RwLock::new(Retention::Ttl(4)),
        })
    }

    /// Keeps the header of an applied block (execution env, BLOCKHASH). Pruned with the store
    /// window.
    pub fn insert_header(&self, h: Header) {
        let mut hs = self.headers.write();
        hs.insert(h.number, h);
        if let Some(head) = hs.keys().next_back().copied() {
            while hs.keys().next().is_some_and(|n| *n + 512 < head) {
                hs.pop_first();
            }
        }
    }

    pub fn header(&self, b: &BlockRef) -> Option<Header> {
        if let Some(h) = self.headers.read().get(&b.number).filter(|h| h.hash_slow() == b.hash).cloned() {
            return Some(h);
        }
        let (_, rlp) = self.store.archived_block(b.number)?;
        let h = <Header as alloy_rlp::Decodable>::decode(&mut rlp.as_slice()).ok()?;
        (h.hash_slow() == b.hash).then_some(h)
    }

    fn fetch_at(&self, b: &BlockRef) -> BlockRef {
        if self.miss_at_parent { self.store.block(b.number.saturating_sub(1)).unwrap_or(*b) } else { *b }
    }

    /// One local execution (CPU only).
    pub fn execute(&self, snap: &Snapshot, header: &Header, tx: TxEnv) -> Result<(ExecutionResult, Access), SimError> {
        let store = self.store.clone();
        let block_hash = move |n: u64| store.block_hash(n);
        let db = db::SnapDb { snap, block_hash: &block_hash, access: Default::default() };
        let res = {
            let mut evm = Context::mainnet()
                .with_db(WrapDatabaseRef(&db))
                .with_block(env::block_env(header))
                .with_cfg(env::call_cfg())
                .build_mainnet();
            evm.transact(tx).map(|r| r.result).map_err(|e| SimError::Evm(format!("{e:?}")))
        };
        let mut access = db.access.into_inner().unwrap();
        access.dedup();
        res.map(|r| (r, access))
    }

    /// Runs `tx` on the post-state of `b`; fetched state gets the on-demand retention.
    pub async fn call(&self, b: &BlockRef, tx: TxEnv, mode: Mode) -> Result<CallOutcome, SimError> {
        let retention = *self.on_demand.read();
        self.call_retained(b, tx, mode, retention).await
    }

    /// [`Self::call`] committing fetched state with `retention` (subscriptions use `Subscribed`).
    pub async fn call_retained(&self, b: &BlockRef, tx: TxEnv, mode: Mode, retention: Retention) -> Result<CallOutcome, SimError> {
        let snap = self.store.snapshot(b)?;
        let header = self.header(b).ok_or(SimError::NoHeader(b.number))?;
        let at = self.fetch_at(b);
        // Historical (archived) blocks: old roots aren't served by SNAP peers, so never fetch.
        let mode = if snap.is_archived() { Mode::WarmOnly } else { mode };
        let input = match tx.kind {
            TxKind::Call(to) => Some(CallInput { to, data: tx.data.clone() }),
            TxKind::Create => None,
        };
        let mut predicted = None;
        if let (Mode::Resolve { .. }, Some(input)) = (mode, &input) {
            let (keys, _) = self.predictor.lock().predict(input, &snap);
            let (accts, slots) = unknown_split(&snap, &keys);
            if !accts.is_empty() || !slots.is_empty() {
                predicted = Some(
                    prefetch::resolve_retained(&self.store, &self.sched, &at, &accts, &slots, &[], Priority::High, retention).await,
                );
            }
        }
        let mut rounds = Vec::new();
        let mut exec_us = 0.0;
        loop {
            let t = Instant::now();
            let (result, access) = self.execute(&snap, &header, tx.clone())?;
            exec_us += t.elapsed().as_secs_f64() * 1e6;
            if !access.has_misses() {
                if let Some(input) = &input {
                    self.predictor.lock().record(input, &access.accounts, &access.storage);
                }
                if let Retention::Ttl(n) = retention {
                    // Recently used on-demand keys stay cached for another TTL.
                    let mut keys: Vec<StateKey> = access.storage.iter().map(|(a, s)| StateKey::Storage(*a, *s)).collect();
                    for a in &access.accounts {
                        keys.extend([StateKey::Balance(*a), StateKey::Nonce(*a), StateKey::CodeHash(*a)]);
                    }
                    self.store.touch_ttl(&keys, n);
                }
                return Ok(CallOutcome { result, access, predicted, rounds, exec_us });
            }
            let max = match mode {
                Mode::WarmOnly if snap.is_archived() => {
                    return Err(SimError::NotArchived {
                        block: b.number,
                        keys: access.miss_accounts.len() + access.miss_storage.len() + access.miss_code.len(),
                    });
                }
                Mode::WarmOnly => {
                    return Err(SimError::StateUnavailable(
                        access.miss_accounts.len() + access.miss_storage.len() + access.miss_code.len(),
                    ));
                }
                Mode::Resolve { max_rounds } => max_rounds,
            };
            if rounds.len() >= max {
                return Err(SimError::Incomplete(rounds.len()));
            }
            let mut slots = access.miss_storage.clone();
            let extra = self.predictor.lock().expand(&snap, &slots);
            slots.extend(unknown_split(&snap, &extra).1);
            slots.sort();
            slots.dedup();
            rounds.push(
                prefetch::resolve_retained(
                    &self.store,
                    &self.sched,
                    &at,
                    &access.miss_accounts,
                    &slots,
                    &access.miss_code,
                    Priority::High,
                    retention,
                )
                .await,
            );
        }
    }

    /// A single state value at `b`, fetched over SNAP if unknown.
    pub async fn read(&self, b: &BlockRef, key: StateKey) -> Result<U256, SimError> {
        let snap = self.store.snapshot(b)?;
        if let StateValue::Known(v) = snap.get(&key)? {
            return Ok(v);
        }
        if snap.is_archived() {
            return Err(SimError::NotArchived { block: b.number, keys: 1 });
        }
        let at = self.fetch_at(b);
        let ret = *self.on_demand.read();
        let r = match key {
            StateKey::Storage(a, s) => {
                prefetch::resolve_retained(&self.store, &self.sched, &at, &[], &[(a, s)], &[], Priority::High, ret).await
            }
            StateKey::Balance(a) | StateKey::Nonce(a) | StateKey::CodeHash(a) => {
                prefetch::resolve_retained(&self.store, &self.sched, &at, &[a], &[], &[], Priority::High, ret).await
            }
        };
        match snap.get(&key)? {
            StateValue::Known(v) => Ok(v),
            StateValue::Unknown => Err(SimError::Incomplete(r.failed.max(1))),
        }
    }

    /// Account code at `b` (empty for EOAs and absent accounts).
    pub async fn code(&self, b: &BlockRef, a: Address) -> Result<Bytes, SimError> {
        let h = B256::from(self.read(b, StateKey::CodeHash(a)).await?);
        if h == revm::primitives::KECCAK_EMPTY {
            return Ok(Bytes::new());
        }
        if let Some(c) = self.store.code(&h) {
            return Ok(c);
        }
        prefetch::resolve(&self.store, &self.sched, &self.fetch_at(b), &[], &[], &[h]).await;
        self.store.code(&h).ok_or(SimError::Incomplete(1))
    }
}

/// Splits keys into the accounts and slots still unknown at `snap`.
pub fn unknown_split(snap: &Snapshot, keys: &[StateKey]) -> (Vec<Address>, Vec<(Address, U256)>) {
    let mut accts = Vec::new();
    let mut slots = Vec::new();
    for k in keys {
        if !matches!(snap.get(k), Ok(StateValue::Unknown)) {
            continue;
        }
        match *k {
            StateKey::Storage(a, s) => slots.push((a, s)),
            StateKey::Balance(a) | StateKey::Nonce(a) | StateKey::CodeHash(a) => {
                if !accts.contains(&a) {
                    accts.push(a);
                }
            }
        }
    }
    (accts, slots)
}
