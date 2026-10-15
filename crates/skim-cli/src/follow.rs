//! M1 (PROJECT.md §6, §10): the BAL state follower, measured end to end.
//!
//! For every block it records:
//! - `env_ms`: envelope (with BAL) received, ms after slot start (CL gossip)
//! - `applied_ms`: commitment verified and BAL applied to the MVCC store (state ready)
//! - `sim_ms`: watched `eth_call`s completed locally against the new snapshot
//! - `infura_ready_ms`: when Infura first answers `eth_call` at that block hash, polled every
//!   100 ms on a keep-alive client
//! - `infura_call_ms`: one keep-alive Infura `eth_call` once ready (fair warm comparison)
//!
//! Every local output is compared with Infura's at the same block hash.
//!
//! Bootstrap: at the first verified block, each watched call is resolved once over SNAP. Its
//! access set and values become MVCC bases. From then on, BAL writes keep those keys current
//! and simulations run without network. Misses fall back to the SNAP scheduler at that block's
//! root and are counted. Gaps in gossip are filled from EL peers (headers + eth/71 BALs).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes, U256};
use revm::{
    Context, DatabaseRef, ExecuteEvm, MainBuilder, MainContext,
    bytecode::Bytecode,
    context::{CfgEnv, TxEnv},
    database::WrapDatabaseRef,
    primitives::{KECCAK_EMPTY, hardfork::SpecId},
    state::AccountInfo,
};
use skim_bal::{BlockWrites, StateKey};
use skim_cl::{ClEvent, ClNode, NodeConfig, ssz::Envelope, verify};
use skim_el::ElNet;
use skim_snap::{Scheduler, el::ElTransport};
use skim_state::{BlockRef, Snapshot, StateValue, Store};

use skim_sim::prefetch;

use crate::{
    probe_call::{Call, Misses, Sparse, block_env, calls, cold_run, infura_url},
    stats::Samples,
};


#[derive(clap::Args)]
pub struct Args {
    /// Blocks to follow after bootstrap.
    #[arg(long, default_value_t = 100)]
    blocks: u64,
    /// CL libp2p/discv5 port (QUIC on port+1).
    #[arg(long, default_value_t = 9120)]
    cl_port: u16,
    /// EL RLPx/discovery port.
    #[arg(long, default_value_t = 30390)]
    el_port: u16,
    /// Watched calls (substring match on probe-call names; default: all).
    #[arg(long)]
    calls: Vec<String>,
    /// Skip Infura comparison.
    #[arg(long)]
    no_infura: bool,
    #[arg(long, default_value = "data/follow.jsonl")]
    out: String,
    /// Root to resolve misses at: `parent` (fetch at H-1, replay BAL(H)) or `head`.
    #[arg(long, default_value = "parent")]
    miss_root: String,
    /// V3 prefetch: bitmap words on each side of the current word (0 disables).
    #[arg(long, default_value_t = 2)]
    v3_words: i32,
    /// V3 pools to prefetch for (default: Sepolia WETH/USDC 0.05% and 0.3%).
    #[arg(long, value_delimiter = ',', default_value = "0x3289680dD4d6C10bb19b899729cda5eEF58AEfF1,0x6Ce0896eAE6D4BD668fDe41BB784548fb8F59b50")]
    v3_pools: Vec<Address>,
    /// Random new-input V3 quotes per block (no learned set applies to them).
    #[arg(long, default_value_t = 0)]
    random_quotes: usize,
    /// Run the State Access Predictor before each call (learned sets + V3 input-aware range)
    /// and expand V3 bitmap misses.
    #[arg(long)]
    predictor: bool,
    /// Background V3 window follows the predictor's observed per-pool reach.
    #[arg(long)]
    adaptive_window: bool,
    /// Background prefetch budget: keys per pool per block (closest to the current tick first).
    #[arg(long, default_value_t = 256)]
    prefetch_budget: usize,
}

fn unix_ms(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_secs_f64() * 1e3
}

fn slot_ms(ts: u64) -> f64 {
    ts as f64 * 1e3
}

fn block_ref(h: &Header) -> BlockRef {
    BlockRef { number: h.number, hash: h.hash_slow(), parent_hash: h.parent_hash, state_root: h.state_root, timestamp: h.timestamp }
}

// ---------- revm over an MVCC snapshot ----------

#[derive(Default)]
struct SnapMisses {
    accounts: Vec<Address>,
    storage: Vec<(Address, U256)>,
    code: Vec<B256>,
    /// Every account/slot read (hit or miss): the call's access set.
    touched_accounts: Vec<Address>,
    touched_storage: Vec<(Address, U256)>,
}

struct SnapDb<'a> {
    snap: &'a Snapshot,
    misses: StdMutex<SnapMisses>,
}

impl DatabaseRef for SnapDb<'_> {
    type Error = std::convert::Infallible;

    fn basic_ref(&self, a: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.misses.lock().unwrap().touched_accounts.push(a);
        let g = |k| self.snap.get(&k).unwrap_or(StateValue::Unknown);
        match (g(StateKey::Balance(a)), g(StateKey::Nonce(a)), g(StateKey::CodeHash(a))) {
            (StateValue::Known(balance), StateValue::Known(nonce), StateValue::Known(ch)) => {
                let code_hash = B256::from(ch);
                Ok(Some(AccountInfo { balance, nonce: nonce.to(), code_hash, code: None, ..Default::default() }))
            }
            _ => {
                self.misses.lock().unwrap().accounts.push(a);
                Ok(None)
            }
        }
    }

    fn code_by_hash_ref(&self, h: B256) -> Result<Bytecode, Self::Error> {
        if h == KECCAK_EMPTY {
            return Ok(Bytecode::default());
        }
        match self.snap.code(&h) {
            Some(c) => Ok(Bytecode::new_raw(c)),
            None => {
                self.misses.lock().unwrap().code.push(h);
                Ok(Bytecode::default())
            }
        }
    }

    fn storage_ref(&self, a: Address, slot: U256) -> Result<U256, Self::Error> {
        self.misses.lock().unwrap().touched_storage.push((a, slot));
        match self.snap.get(&StateKey::Storage(a, slot)).unwrap_or(StateValue::Unknown) {
            StateValue::Known(v) => Ok(v),
            StateValue::Unknown => {
                self.misses.lock().unwrap().storage.push((a, slot));
                Ok(U256::ZERO)
            }
        }
    }

    fn block_hash_ref(&self, _n: u64) -> Result<B256, Self::Error> {
        Ok(B256::ZERO)
    }
}

fn simulate(snap: &Snapshot, h: &Header, call: &Call) -> (Option<Bytes>, SnapMisses) {
    let db = SnapDb { snap, misses: Default::default() };
    let mut cfg = CfgEnv::new_with_spec(SpecId::AMSTERDAM);
    cfg.chain_id = 11155111;
    cfg.disable_nonce_check = true;
    cfg.disable_balance_check = true;
    cfg.disable_base_fee = true;
    cfg.disable_eip3607 = true;
    cfg.disable_block_gas_limit = true;
    cfg.disable_fee_charge = true;
    let out = {
        let mut evm = Context::mainnet().with_db(WrapDatabaseRef(&db)).with_block(block_env(h)).with_cfg(cfg).build_mainnet();
        let tx = TxEnv::builder()
            .caller(Address::ZERO)
            .kind(revm::primitives::TxKind::Call(call.to))
            .data(call.data.clone())
            .gas_limit(30_000_000)
            .chain_id(Some(11155111))
            .build()
            .unwrap();
        evm.transact(tx).ok().map(|r| r.result.into_output().unwrap_or_default())
    };
    (out, db.misses.into_inner().unwrap())
}

/// Commits a resolved sparse state (fetched at `at`) into the store as MVCC bases.
fn commit_sparse(store: &Store, at: &BlockRef, st: &Sparse, access: &Misses) {
    for a in &access.accounts {
        let info = st.accounts.get(a).cloned().flatten();
        let (b, n, c) = match info {
            Some(i) => (i.balance, U256::from(i.nonce), U256::from_be_bytes(i.code_hash.0)),
            None => (U256::ZERO, U256::ZERO, U256::from_be_bytes(KECCAK_EMPTY.0)),
        };
        let _ = store.insert_base(at, StateKey::Balance(*a), b);
        let _ = store.insert_base(at, StateKey::Nonce(*a), n);
        let _ = store.insert_base(at, StateKey::CodeHash(*a), c);
    }
    for (a, s) in &access.storage {
        let v = st.storage.get(&(*a, *s)).copied().unwrap_or_default();
        let _ = store.insert_base(at, StateKey::Storage(*a, *s), v);
    }
    for c in st.code.values() {
        store.insert_code(c.original_bytes());
    }
}

// ---------- Infura ----------

async fn infura_call(client: &reqwest::Client, url: &str, hash: B256, call: &Call) -> anyhow::Result<Bytes> {
    let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": call.to, "data": call.data}, {"blockHash": hash}]});
    let v: serde_json::Value = client.post(url).json(&body).send().await?.json().await?;
    let r = v["result"].as_str().ok_or_else(|| anyhow::anyhow!("{}", v["error"]))?;
    Ok(alloy_primitives::hex::decode(r)?.into())
}

// ---------- driver ----------

#[derive(Default)]
struct Stats {
    blocks: u64,
    from_gossip: u64,
    from_el_fill: u64,
    reorgs: u64,
    env: Samples,
    applied: Samples,
    apply_us: Samples,
    sim: Samples,
    sim_local_us: Samples,
    infura_ready: Samples,
    infura_answer: Samples,
    infura_call: Samples,
    lead_vs_ready: Samples,
    lead_vs_answer: Samples,
    infura_polled: u64,
    misses_resolved: u64,
    miss_events: u64,
    miss_wall: Samples,
    miss_queue: Samples,
    miss_service: Samples,
    miss_attempts_max: u32,
    miss_unavailable: u64,
    calls_run: u64,
    calls_with_miss: u64,
    prefetch_rounds: u64,
    predict_waves: u64,
    predicted_keys: u64,
    predicted_unknown: u64,
    predict_keys: u64,
    predict_wall: Samples,
    prefetch_keys: u64,
    prefetch_wall: Samples,
    match_ok: u64,
    match_fail: u64,
}

struct Ctx {
    store: Arc<Store>,
    net: ElNet,
    sched: Scheduler<ElTransport>,
    calls: Vec<Call>,
    stats: StdMutex<Stats>,
    out: StdMutex<std::io::BufWriter<std::fs::File>>,
    infura: Option<(reqwest::Client, String)>,
    /// Infura readiness per block number, polled independently of skim (see `spawn_poller`).
    pollers: StdMutex<HashMap<u64, tokio::task::JoinHandle<Option<Poll>>>>,
    miss_at_parent: bool,
    v3_words: i32,
    v3_pools: StdMutex<Vec<prefetch::V3Pool>>,
    random_quotes: usize,
    predictor: Option<StdMutex<skim_sim::predictor::Predictor>>,
    adaptive_window: bool,
    prefetch_budget: usize,
}

/// First successful Infura `eth_call` at a block number: when that request was sent (an upper
/// bound on when Infura could answer) and when its answer arrived (when an Infura user has it).
#[derive(Clone, Copy, Debug)]
struct Poll {
    first_ok_sent_ms: f64,
    first_ok_recv_ms: f64,
    last_fail_sent_ms: Option<f64>,
    polls: u32,
}

const POLL_START_MS: f64 = 300.0;
const POLL_EVERY: Duration = Duration::from_millis(50);

/// Polls Infura for block `number` (slot timestamp `ts`) from `ts + 300 ms`, every 50 ms,
/// independent of when skim sees the block.
fn spawn_poller(ctx: &Arc<Ctx>, number: u64, ts: u64) {
    let Some((client, url)) = ctx.infura.clone() else { return };
    let call = Call { name: ctx.calls[0].name.clone(), to: ctx.calls[0].to, data: ctx.calls[0].data.clone() };
    let mut map = ctx.pollers.lock().unwrap();
    if map.contains_key(&number) {
        return;
    }
    let h = tokio::spawn(async move {
        let start = slot_ms(ts) + POLL_START_MS;
        let now = unix_ms(SystemTime::now());
        if start > now {
            tokio::time::sleep(Duration::from_secs_f64((start - now) / 1e3)).await;
        }
        let (mut last_fail, mut polls) = (None, 0);
        let give_up = Instant::now() + Duration::from_secs(30);
        let mut tick = tokio::time::interval(POLL_EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while Instant::now() < give_up {
            tick.tick().await;
            polls += 1;
            let sent = unix_ms(SystemTime::now());
            let body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                "params": [{"to": call.to, "data": call.data}, format!("0x{number:x}")]});
            let ok = match client.post(&url).json(&body).send().await {
                Ok(r) => r.json::<serde_json::Value>().await.is_ok_and(|v| v["result"].is_string()),
                Err(_) => false,
            };
            if ok {
                return Some(Poll { first_ok_sent_ms: sent, first_ok_recv_ms: unix_ms(SystemTime::now()), last_fail_sent_ms: last_fail, polls });
            }
            last_fail = Some(sent);
        }
        None
    });
    map.insert(number, h);
}

impl Ctx {
    fn log(&self, v: serde_json::Value) {
        use std::io::Write;
        let mut o = self.out.lock().unwrap();
        let _ = serde_json::to_writer(&mut *o, &v);
        let _ = o.write_all(b"\n");
        let _ = o.flush();
    }
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let infura = if args.no_infura { None } else { Some((reqwest::Client::new(), infura_url()?)) };
    let net = ElNet::start_sepolia(args.el_port, Vec::new(), false).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let sched = Scheduler::new(ElTransport::new(net.clone()), skim_snap::Config::default());
    let (_cl, mut rx) = ClNode::start(NodeConfig::sepolia(args.cl_port)).await?;
    let all = calls();
    let calls: Vec<Call> = if args.calls.is_empty() {
        all
    } else {
        all.into_iter().filter(|c| args.calls.iter().any(|f| c.name.contains(f.as_str()))).collect()
    };
    let ctx = Arc::new(Ctx {
        store: Store::new(256),
        net,
        sched,
        calls,
        stats: Default::default(),
        out: StdMutex::new(std::io::BufWriter::new(std::fs::File::create(&args.out)?)),
        infura,
        pollers: Default::default(),
        miss_at_parent: args.miss_root == "parent",
        v3_words: args.v3_words,
        v3_pools: Default::default(),
        random_quotes: args.random_quotes,
        adaptive_window: args.adaptive_window,
        prefetch_budget: args.prefetch_budget,
        predictor: args.predictor.then(|| {
            StdMutex::new(skim_sim::predictor::Predictor {
                learned: skim_sim::learned::Learned::new(8, 0.5, 256),
                v3: Vec::new(),
                base_words: args.v3_words.max(1),
                reach: Default::default(),
            })
        }),
    });
    for a in args.v3_pools.iter().chain([&crate::probe_call::WETH_ADDR, &crate::probe_call::USDC_ADDR]) {
        ctx.store.watch_address(*a);
    }
    let pools = args.v3_pools.clone();
    tracing::info!(calls = ctx.calls.len(), "follow: waiting for envelopes and SNAP peers");
    let chain = crate::chain::Chain {
        store: ctx.store.clone(),
        net: ctx.net.clone(),
        on_header: Box::new(|_| {}),
        lookback: 32,
    };

    // 0 = not started, 1 = running, 2 = done. Blocks are applied from the first envelope on, so
    // the BAL index covers the bootstrap block; bootstrap starts once SNAP peers exist.
    let boot = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let mut headers: HashMap<B256, Header> = HashMap::new();
    let mut followed = 0u64;
    while let Some(ev) = rx.recv().await {
        let ClEvent::Gossip { topic, data, received, .. } = ev else { continue };
        if topic != "execution_payload" {
            continue;
        }
        let recv_ms = unix_ms(received);
        let Ok(env) = Envelope::parse(&data) else { continue };
        let v = verify::verify_payload(&env);
        if !v.ok {
            tracing::warn!(number = env.payload.block_number, "payload hash mismatch; ignored");
            continue;
        }
        let t_apply = Instant::now();
        let Ok(bal) = skim_bal::decode(env.payload.block_access_list) else { continue };
        let writes = Arc::new(BlockWrites::from_bal(&bal));
        let h = v.header;
        let b = block_ref(&h);
        headers.insert(b.hash, h.clone());

        // Place the block on the selected chain (fork switch / EL reconcile in `chain`).
        match chain.place(h.clone(), writes).await {
            crate::chain::Placed::Applied { reorged_to, filled, .. } => {
                let mut st = ctx.stats.lock().unwrap();
                st.reorgs += reorged_to.is_some() as u64;
                st.from_el_fill += filled;
                if reorged_to.is_some() || filled > 0 {
                    tracing::warn!(number = b.number, ?reorged_to, filled, "reconciled with EL chain");
                }
            }
            crate::chain::Placed::Reanchored(_) => {
                tracing::warn!(number = b.number, "re-anchored: state reset, bootstrapping again");
                boot.store(0, std::sync::atomic::Ordering::SeqCst);
            }
            crate::chain::Placed::Duplicate => continue,
            crate::chain::Placed::Deferred(why) => {
                tracing::debug!(number = b.number, "deferred: {why}");
                continue;
            }
        }
        let apply_us = t_apply.elapsed().as_secs_f64() * 1e6;
        let applied_ms = unix_ms(SystemTime::now());

        use std::sync::atomic::Ordering::SeqCst;
        match boot.load(SeqCst) {
            0 => {
                let snap_peers = ctx.net.peers().iter().filter(|(_, p)| p.snap1 || p.snap2).count();
                if snap_peers >= 2 {
                    boot.store(1, SeqCst);
                    let (ctx2, h2, boot2, pools2) = (ctx.clone(), h.clone(), boot.clone(), pools.clone());
                    tokio::spawn(async move {
                        bootstrap(&ctx2, &h2, &b).await;
                        detect_pools(&ctx2, &h2, &b, &pools2).await;
                        boot2.store(2, SeqCst);
                    });
                } else {
                    tracing::info!(number = b.number, snap_peers, "applied (waiting for SNAP peers to bootstrap)");
                }
                continue;
            }
            1 => continue,
            _ => {}
        }
        followed += 1;
        spawn_poller(&ctx, b.number + 1, b.timestamp + 12);
        spawn_prefetch(&ctx, b);
        {
            let mut s = ctx.stats.lock().unwrap();
            s.blocks += 1;
            s.from_gossip += 1;
            s.env.push_ms(recv_ms - slot_ms(h.timestamp));
            s.applied.push_ms(applied_ms - slot_ms(h.timestamp));
            s.apply_us.push_ms(apply_us);
        }
        let ctx2 = ctx.clone();
        let h2 = h.clone();
        tokio::spawn(async move { measure_block(ctx2, h2, b, recv_ms, applied_ms).await });
        if followed >= args.blocks {
            tokio::time::sleep(Duration::from_secs(20)).await;
            break;
        }
    }
    report(&ctx);
    Ok(())
}

async fn bootstrap(ctx: &Ctx, h: &Header, b: &BlockRef) {
    for call in &ctx.calls {
        let mut st = Sparse::default();
        match cold_run(&ctx.sched, h, call, &mut st, None).await {
            Ok(c) => {
                commit_sparse(&ctx.store, b, &st, &c.access);
                tracing::info!(call = %call.name, keys = c.access.accounts.len() + c.access.storage.len(),
                    rounds = c.rounds.len(), "bootstrapped");
            }
            Err(e) => tracing::warn!(call = %call.name, "bootstrap failed: {e}"),
        }
    }
    let head = ctx.store.head().map(|h| h.number);
    tracing::info!(at = b.number, ?head, "bootstrap complete; following");
}

/// Splits predicted keys into the accounts and slots that are still unknown at `snap`.
fn unknown_split(snap: &Snapshot, keys: &[StateKey]) -> (Vec<Address>, Vec<(Address, U256)>) {
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

/// Reads each pool's `tickSpacing()` from the bootstrapped state (resolving it if needed).
async fn detect_pools(ctx: &Arc<Ctx>, h: &Header, b: &BlockRef, pools: &[Address]) {
    let sel = Bytes::from_static(&[0xd0, 0xc9, 0x3a, 0x7c]); // tickSpacing()
    for &address in pools {
        let call = Call { name: "tickSpacing".into(), to: address, data: sel.clone() };
        for _ in 0..4 {
            let Ok(snap) = ctx.store.snapshot(b) else { break };
            let (out, m) = simulate(&snap, h, &call);
            if m.accounts.is_empty() && m.storage.is_empty() && m.code.is_empty() {
                match out.as_ref().and_then(prefetch::decode_spacing) {
                    Some(spacing) => {
                        let pool = prefetch::V3Pool { address, spacing };
                        ctx.v3_pools.lock().unwrap().push(pool);
                        let word = |sel: [u8; 4]| -> Option<U256> {
                            let c = Call { name: "pool".into(), to: address, data: Bytes::copy_from_slice(&sel) };
                            let (o, m) = simulate(&snap, h, &c);
                            (m.accounts.is_empty() && m.storage.is_empty()).then_some(())?;
                            o.filter(|o| o.len() == 32).map(|o| U256::from_be_slice(&o))
                        };
                        let fee = word([0xdd, 0xca, 0x3f, 0x43]);
                        let t0 = word([0x0d, 0xfe, 0x16, 0x81]);
                        let t1 = word([0xd2, 0x12, 0x20, 0xa7]);
                        tracing::info!(%address, spacing, ?fee, "V3 pool registered for prefetch");
                        if let (Some(p), Some(fee), Some(t0), Some(t1)) = (&ctx.predictor, fee, t0, t1) {
                            p.lock().unwrap().v3.push(skim_sim::predictor::V3Meta {
                                pool,
                                fee: fee.to(),
                                token0: Address::from_word(t0.into()),
                                token1: Address::from_word(t1.into()),
                            });
                        }
                    }
                    None => tracing::warn!(%address, "tickSpacing() failed"),
                }
                break;
            }
            prefetch::resolve(&ctx.store, &ctx.sched, b, &m.accounts, &m.storage, &m.code).await;
        }
    }
}

/// Per-block V3 refresh, off the simulation path.
fn spawn_prefetch(ctx: &Arc<Ctx>, b: BlockRef) {
    if ctx.v3_words <= 0 {
        return;
    }
    let pools = ctx.v3_pools.lock().unwrap().clone();
    let ctx = ctx.clone();
    tokio::spawn(async move {
        let at = if ctx.miss_at_parent { ctx.store.block(b.number - 1).unwrap_or(b) } else { b };
        for pool in pools {
            let window = match &ctx.predictor {
                Some(p) if ctx.adaptive_window => p.lock().unwrap().window(&pool.address),
                _ => (-ctx.v3_words, ctx.v3_words),
            };
            let rounds = prefetch::refresh_pool(&ctx.store, &ctx.sched, &b, &at, &pool, window, ctx.prefetch_budget).await;
            let mut s = ctx.stats.lock().unwrap();
            for r in rounds {
                s.prefetch_rounds += 1;
                s.prefetch_keys += r.keys as u64;
                s.prefetch_wall.push_ms(r.wall_ms);
            }
        }
    });
}

async fn measure_block(ctx: Arc<Ctx>, h: Header, b: BlockRef, recv_ms: f64, applied_ms: f64) {
    let Ok(snap) = ctx.store.snapshot(&b) else { return };
    let at = if ctx.miss_at_parent { ctx.store.block(b.number - 1).unwrap_or(b) } else { b };
    let mut calls: Vec<Call> = ctx.calls.iter().map(|c| Call { name: c.name.clone(), to: c.to, data: c.data.clone() }).collect();
    calls.extend(crate::probe_call::random_quotes(b.number, ctx.random_quotes));
    let mut outputs = Vec::new();
    let mut local_us = 0.0;
    let mut resolved = 0u64;
    let mut miss_log = Vec::new();
    for call in &calls {
        // State Access Predictor: fetch the predicted-but-unknown keys in one wave first.
        if let Some(p) = &ctx.predictor {
            let input = skim_sim::CallInput { to: call.to, data: call.data.clone() };
            let (keys, pred) = p.lock().unwrap().predict(&input, &snap);
            let (accts, slots) = unknown_split(&snap, &keys);
            {
                let unknown = keys
                    .iter()
                    .filter(|k| matches!(snap.get(k), Ok(StateValue::Unknown)))
                    .count();
                let mut s = ctx.stats.lock().unwrap();
                s.predicted_keys += keys.len() as u64;
                s.predicted_unknown += unknown as u64;
            }
            if !accts.is_empty() || !slots.is_empty() {
                let r = prefetch::resolve(&ctx.store, &ctx.sched, &at, &accts, &slots, &[]).await;
                let mut s = ctx.stats.lock().unwrap();
                s.predict_waves += 1;
                s.predict_keys += r.keys as u64;
                s.predict_wall.push_ms(r.wall_ms);
                drop(s);
                miss_log.push(serde_json::json!({"call": call.name, "round": "predict", "prediction": pred, "resolve": r}));
            }
        }
        let t = Instant::now();
        let (mut out, mut m) = simulate(&snap, &h, call);
        local_us += t.elapsed().as_secs_f64() * 1e6;
        let mut had_miss = false;
        // Misses: fetch at `at` (parent root by default), commit as bases (replaying this
        // block's BAL), re-run. Bounded.
        for round in 0..8 {
            if m.accounts.is_empty() && m.storage.is_empty() && m.code.is_empty() {
                break;
            }
            had_miss = true;
            m.accounts.sort();
            m.accounts.dedup();
            m.storage.sort();
            m.storage.dedup();
            m.code.sort();
            m.code.dedup();
            if let Some(p) = &ctx.predictor {
                let extra = p.lock().unwrap().expand(&snap, &m.storage);
                let (_, more) = unknown_split(&snap, &extra);
                m.storage.extend(more);
                m.storage.sort();
                m.storage.dedup();
            }
            resolved += (m.accounts.len() + m.storage.len()) as u64;
            let r = prefetch::resolve(&ctx.store, &ctx.sched, &at, &m.accounts, &m.storage, &m.code).await;
            let t2 = Instant::now();
            (out, m) = simulate(&snap, &h, call);
            let rerun_us = t2.elapsed().as_secs_f64() * 1e6;
            {
                let mut s = ctx.stats.lock().unwrap();
                s.miss_events += 1;
                s.miss_wall.push_ms(r.wall_ms);
                s.miss_queue.push_ms(r.queue_ms_max);
                s.miss_service.push_ms(r.service_ms_max);
                s.miss_attempts_max = s.miss_attempts_max.max(r.attempts_max);
                s.miss_unavailable += r.unavailable as u64;
            }
            miss_log.push(serde_json::json!({"call": call.name, "round": round, "rerun_us": rerun_us, "resolve": r}));
        }
        {
            let mut s = ctx.stats.lock().unwrap();
            s.calls_run += 1;
            s.calls_with_miss += had_miss as u64;
        }
        if let Some(p) = &ctx.predictor {
            let mut ta = m.touched_accounts.clone();
            let mut ts = m.touched_storage.clone();
            ta.sort();
            ta.dedup();
            ts.sort();
            ts.dedup();
            p.lock().unwrap().record(&skim_sim::CallInput { to: call.to, data: call.data.clone() }, &ta, &ts);
        }
        outputs.push(out);
    }
    let sim_ms = unix_ms(SystemTime::now());

    // Infura readiness from the independent poller (if one was started for this block), then
    // one warm keep-alive call per watched call for correctness and warm latency.
    let poll = {
        let h = ctx.pollers.lock().unwrap().remove(&b.number);
        match h {
            Some(h) => h.await.ok().flatten(),
            None => None,
        }
    };
    let (mut warm_ms, mut matches, mut mismatches) = (None, 0, 0);
    if let Some((client, url)) = &ctx.infura {
        for (call, ours) in calls.iter().zip(&outputs) {
            let t = Instant::now();
            match infura_call(client, url, b.hash, call).await {
                Ok(theirs) => {
                    warm_ms.get_or_insert(t.elapsed().as_secs_f64() * 1e3);
                    if ours.as_ref() == Some(&theirs) { matches += 1 } else {
                        mismatches += 1;
                        tracing::warn!(call = %call.name, number = b.number, "OUTPUT MISMATCH");
                    }
                }
                Err(e) => tracing::debug!("infura: {e}"),
            }
        }
    }

    let slot0 = slot_ms(h.timestamp);
    {
        let mut s = ctx.stats.lock().unwrap();
        s.sim.push_ms(sim_ms - slot0);
        s.sim_local_us.push_ms(local_us);
        s.misses_resolved += resolved;
        s.match_ok += matches;
        s.match_fail += mismatches;
        if let Some(p) = poll {
            s.infura_polled += 1;
            s.infura_ready.push_ms(p.first_ok_sent_ms - slot0);
            s.infura_answer.push_ms(p.first_ok_recv_ms - slot0);
            s.lead_vs_ready.push_ms(p.first_ok_sent_ms - sim_ms);
            s.lead_vs_answer.push_ms(p.first_ok_recv_ms - sim_ms);
        }
        if let Some(w) = warm_ms {
            s.infura_call.push_ms(w);
        }
    }
    tracing::info!(number = b.number, env_ms = (recv_ms - slot0) as i64, applied_ms = (applied_ms - slot0) as i64,
        sim_ms = (sim_ms - slot0) as i64,
        infura_ready_le_ms = poll.map(|p| (p.first_ok_sent_ms - slot0) as i64),
        infura_answer_ms = poll.map(|p| (p.first_ok_recv_ms - slot0) as i64),
        local_us = local_us as u64, resolved, matches, mismatches, "block");
    ctx.log(serde_json::json!({"number": b.number, "env_ms": recv_ms - slot0, "applied_ms": applied_ms - slot0,
        "sim_ms": sim_ms - slot0,
        "infura_ready_le_ms": poll.map(|p| p.first_ok_sent_ms - slot0),
        "infura_ready_gt_ms": poll.and_then(|p| p.last_fail_sent_ms).map(|t| t - slot0),
        "infura_answer_ms": poll.map(|p| p.first_ok_recv_ms - slot0),
        "infura_polls": poll.map(|p| p.polls), "infura_call_ms": warm_ms,
        "local_us": local_us, "resolved": resolved, "misses": miss_log, "matches": matches, "mismatches": mismatches}));
}

fn report(ctx: &Ctx) {
    let s = ctx.stats.lock().unwrap();
    let summary = serde_json::json!({
        "blocks": s.blocks, "from_gossip": s.from_gossip, "from_el_fill": s.from_el_fill, "reorgs": s.reorgs,
        "envelope_after_slot": s.env.json(),
        "state_ready_after_slot": s.applied.json(),
        "bal_apply_us": s.apply_us.json(),
        "sims_done_after_slot": s.sim.json(),
        "local_sim_all_calls_us": s.sim_local_us.json(),
        "infura_ready_upper_bound_after_slot": s.infura_ready.json(),
        "infura_user_has_answer_after_slot": s.infura_answer.json(),
        "skim_lead_vs_infura_ready_bound": s.lead_vs_ready.json(),
        "skim_lead_vs_infura_user_answer": s.lead_vs_answer.json(),
        "blocks_with_infura_poll": s.infura_polled,
        "infura_warm_call": s.infura_call.json(),
        "misses_resolved": s.misses_resolved,
        "calls_run": s.calls_run, "calls_with_miss": s.calls_with_miss,
        "miss_events": s.miss_events, "miss_wall": s.miss_wall.json(), "miss_queue_max": s.miss_queue.json(),
        "miss_service_max": s.miss_service.json(), "miss_attempts_max": s.miss_attempts_max,
        "miss_root_unavailable": s.miss_unavailable,
        "predicted_keys": s.predicted_keys, "predicted_unknown_at_call": s.predicted_unknown,
        "prefetch_hit_rate": if s.predicted_keys > 0 { 1.0 - s.predicted_unknown as f64 / s.predicted_keys as f64 } else { 0.0 },
        "predict_waves": s.predict_waves, "predict_keys": s.predict_keys, "predict_wall": s.predict_wall.json(),
        "prefetch_rounds": s.prefetch_rounds, "prefetch_keys": s.prefetch_keys, "prefetch_wall": s.prefetch_wall.json(),
        "outputs_match": s.match_ok, "outputs_mismatch": s.match_fail,
        "scheduler": ctx.sched.stats(),
    });
    println!("{}", serde_json::to_string_pretty(&summary).unwrap());
}
