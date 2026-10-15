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
use skim_state::{BlockRef, Snapshot, StateError, StateValue, Store};

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
}

impl Simulator {
    pub fn new(store: Arc<Store>, sched: Scheduler<ElTransport>, predictor: predictor::Predictor) -> Arc<Self> {
        Arc::new(Self {
            store,
            sched,
            predictor: Mutex::new(predictor),
            headers: RwLock::default(),
            miss_at_parent: true,
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
        self.headers.read().get(&b.number).filter(|h| h.hash_slow() == b.hash).cloned()
    }

    fn fetch_at(&self, b: &BlockRef) -> BlockRef {
        if self.miss_at_parent { self.store.block(b.number.saturating_sub(1)).unwrap_or(*b) } else { *b }
    }

    /// One local execution (CPU only).
    pub fn execute(&self, snap: &Snapshot, header: &Header, tx: TxEnv) -> Result<(ExecutionResult, Access), SimError> {
        let store = self.store.clone();
        let block_hash = move |n: u64| store.block(n).map(|b| b.hash);
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

    /// Runs `tx` on the post-state of `b`.
    pub async fn call(&self, b: &BlockRef, tx: TxEnv, mode: Mode) -> Result<CallOutcome, SimError> {
        let snap = self.store.snapshot(b)?;
        let header = self.header(b).ok_or(SimError::NoHeader(b.number))?;
        let at = self.fetch_at(b);
        let input = match tx.kind {
            TxKind::Call(to) => Some(CallInput { to, data: tx.data.clone() }),
            TxKind::Create => None,
        };
        let mut predicted = None;
        if let (Mode::Resolve { .. }, Some(input)) = (mode, &input) {
            let (keys, _) = self.predictor.lock().predict(input, &snap);
            let (accts, slots) = unknown_split(&snap, &keys);
            if !accts.is_empty() || !slots.is_empty() {
                predicted = Some(prefetch::resolve(&self.store, &self.sched, &at, &accts, &slots, &[]).await);
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
                return Ok(CallOutcome { result, access, predicted, rounds, exec_us });
            }
            let max = match mode {
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
                prefetch::resolve(&self.store, &self.sched, &at, &access.miss_accounts, &slots, &access.miss_code).await,
            );
        }
    }

    /// A single state value at `b`, fetched over SNAP if unknown.
    pub async fn read(&self, b: &BlockRef, key: StateKey) -> Result<U256, SimError> {
        let snap = self.store.snapshot(b)?;
        if let StateValue::Known(v) = snap.get(&key)? {
            return Ok(v);
        }
        let at = self.fetch_at(b);
        let r = match key {
            StateKey::Storage(a, s) => prefetch::resolve(&self.store, &self.sched, &at, &[], &[(a, s)], &[]).await,
            StateKey::Balance(a) | StateKey::Nonce(a) | StateKey::CodeHash(a) => {
                prefetch::resolve(&self.store, &self.sched, &at, &[a], &[], &[]).await
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
