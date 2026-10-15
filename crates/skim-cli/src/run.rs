//! `skim run`: follow the chain from CL payload gossip and serve JSON-RPC on top of it.
//!
//! - Chain: each verified `execution_payload` envelope is applied to the MVCC store (BAL), and
//!   its header kept for the execution environment. Gaps are filled from EL peers (headers +
//!   eth/71 BALs). A one-block fork switches branches; deeper ones wait for the EL path.
//! - State: nothing is watched up front. Every key a request reads is fetched once over SNAP
//!   (parent root + BAL replay) and from then on stays current through BAL writes.

use std::{net::SocketAddr, sync::Arc};

use skim_bal::BlockWrites;
use skim_cl::{ClEvent, ClNode, NodeConfig, ssz::Envelope, verify};
use skim_el::ElNet;
use skim_sim::{Simulator, learned::Learned, predictor::Predictor};
use skim_snap::{Scheduler, el::ElTransport};
use skim_state::Store;

use crate::{
    chain::{Chain, Placed},
    rpc::{Rpc, serve},
};

#[derive(clap::Args)]
pub struct Args {
    /// JSON-RPC listen address.
    #[arg(long, default_value = "127.0.0.1:8545")]
    rpc: SocketAddr,
    #[arg(long, default_value_t = 9130)]
    cl_port: u16,
    #[arg(long, default_value_t = 30388)]
    el_port: u16,
    /// Blocks of history kept for reads and reorgs.
    #[arg(long, default_value_t = 256)]
    window: u64,
    /// Never use these peers (hex node ids) for SNAP state, e.g. a node being benchmarked
    /// against skim on the same host. Repeatable.
    #[arg(long = "exclude-snap-peer")]
    exclude_snap_peer: Vec<alloy_primitives::B512>,
    /// skim.yaml: state subscriptions, on-demand cache TTL, limits (PROJECT.md §7.5).
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    /// On-disk archive for subscriptions with `retention: { mode: archive }` (M4).
    #[arg(long)]
    archive: Option<std::path::PathBuf>,
}

#[derive(serde::Deserialize, Default)]
struct Config {
    #[serde(default)]
    state: StateConfig,
}

#[derive(serde::Deserialize, Default)]
struct StateConfig {
    #[serde(default)]
    subscriptions: Vec<skim_sim::subscription::SubscriptionSpec>,
    #[serde(default)]
    on_demand: OnDemand,
    #[serde(default)]
    limits: Limits,
}

#[derive(serde::Deserialize)]
struct OnDemand {
    #[serde(default = "yes")]
    enabled: bool,
    #[serde(default = "four")]
    cache_ttl_blocks: u64,
}

impl Default for OnDemand {
    fn default() -> Self {
        Self { enabled: true, cache_ttl_blocks: 4 }
    }
}

#[derive(serde::Deserialize)]
struct Limits {
    #[serde(default = "hundred_k")]
    max_subscribed_keys: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self { max_subscribed_keys: 100_000 }
    }
}

fn yes() -> bool {
    true
}
fn four() -> u64 {
    4
}
fn hundred_k() -> usize {
    100_000
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let net = ElNet::start_sepolia(args.el_port, Vec::new(), false).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let transport = ElTransport::new(net.clone());
    for p in &args.exclude_snap_peer {
        transport.exclude(*p);
    }
    let rej = transport.rejections.clone();
    tokio::spawn(async move {
        use std::sync::atomic::Ordering::Relaxed;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            tracing::info!(no_proof = rej.no_proof.load(Relaxed), bad_proof = rej.bad_proof.load(Relaxed),
                contradiction = rej.contradiction.load(Relaxed), root_unavailable = rej.root_unavailable.load(Relaxed),
                "SNAP answers rejected by verification");
        }
    });
    let sched = Scheduler::new(transport, skim_snap::Config::default());
    let store = match &args.archive {
        Some(p) => {
            let ar = skim_state::archive::Archive::open(p)?;
            tracing::info!(path = %p.display(), last_block = ?ar.last_block()?, segments = ?ar.segments()?,
                archived_keys = ar.archived_keys()?.len(), "archive opened");
            Store::with_archive(args.window, ar)?
        }
        None => Store::new(args.window),
    };
    let sim = Simulator::new(
        store.clone(),
        sched,
        Predictor { learned: Learned::new(8, 0.5, 256), v3: vec![], base_words: 2, reach: Default::default() },
    );
    let cfg: Config = match &args.config {
        Some(p) => serde_yaml_ng::from_str(&std::fs::read_to_string(p)?)?,
        None => Config::default(),
    };
    if !cfg.state.on_demand.enabled {
        tracing::warn!("on_demand.enabled=false: unsubscribed state is still fetched for correctness, with a 1-block TTL");
    }
    *sim.on_demand.write() = skim_state::Retention::Ttl(if cfg.state.on_demand.enabled { cfg.state.on_demand.cache_ttl_blocks } else { 1 });
    let subs = Arc::new(skim_sim::subscription::Subscriptions::new(cfg.state.subscriptions, cfg.state.limits.max_subscribed_keys)?);
    tracing::info!(subscriptions = subs.status().len(), declared_keys = subs.total_keys(), "state subscriptions loaded");
    let (_cl, mut rx) = ClNode::start(NodeConfig::sepolia(args.cl_port)).await?;
    tokio::spawn(serve(Arc::new(Rpc { sim: sim.clone(), subs: Some(subs.clone()) }), args.rpc));
    let maintaining = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sim2 = sim.clone();
    let store2 = store.clone();
    let chain = Chain {
        store: store.clone(),
        net: net.clone(),
        on_header: Box::new(move |h| {
            sim2.insert_header(h.clone());
            store2.note_header(h.hash_slow(), alloy_rlp::encode(h));
        }),
        lookback: 32,
    };
    let mut archive_gap_checked = false;
    tracing::info!("following Sepolia from CL gossip; RPC answers once the first block is applied");

    while let Some(ev) = rx.recv().await {
        let ClEvent::Gossip { topic, data, .. } = ev else { continue };
        if topic != "execution_payload" {
            continue;
        }
        let Ok(env) = Envelope::parse(&data) else { continue };
        let v = verify::verify_payload(&env);
        if !v.ok {
            tracing::warn!(number = env.payload.block_number, "payload hash mismatch; ignored");
            continue;
        }
        let Ok(bal) = skim_bal::decode(env.payload.block_access_list) else { continue };
        let number = v.header.number;
        // After a restart, fill the archive from its last block up to this one, so archived
        // history stays continuous (eth/71 BALs are retained for ~2 weeks).
        if !archive_gap_checked {
            archive_gap_checked = true;
            if let Some(ar) = store.archive() {
                if let Some(last) = ar.last_block()? {
                    if number > last + 1 {
                        match fill_archive_gap(&store, &net, last + 1, number - 1).await {
                            Ok(n) => tracing::info!(from = last + 1, to = number - 1, blocks = n, "archive gap filled from eth/71"),
                            Err(e) => tracing::warn!(from = last + 1, to = number - 1,
                                "archive gap not filled ({e}); history before {number} stays uncovered for keys written in the gap"),
                        }
                    }
                }
            }
        }
        match chain.place(v.header, Arc::new(BlockWrites::from_bal(&bal))).await {
            Placed::Applied { ready, reorged_to, filled } => {
                tracing::info!(number, changed = ready.changed_keys.len(), ?reorged_to, filled, "state ready");
                // Keep subscriptions current and their readiness fresh (one pass at a time).
                if !maintaining.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    let (sim, subs, flag, b) = (sim.clone(), subs.clone(), maintaining.clone(), ready.block);
                    tokio::spawn(async move {
                        subs.maintain(&sim, &b).await;
                        flag.store(false, std::sync::atomic::Ordering::SeqCst);
                    });
                }
            }
            Placed::Reanchored(_) => tracing::warn!(number, "re-anchored"),
            Placed::Duplicate => {}
            Placed::Deferred(why) => tracing::debug!(number, "deferred: {why}"),
        }
    }
    Ok(())
}

/// Writes blocks `from..=to` (headers + archived keys' BAL writes) straight into the archive.
async fn fill_archive_gap(store: &skim_state::Store, net: &ElNet, from: u64, to: u64) -> anyhow::Result<u64> {
    use reth_eth_wire_types::BlockHashOrNumber;
    let ar = store.archive().ok_or_else(|| anyhow::anyhow!("no archive"))?;
    let mut prev = ar.hash(from - 1)?;
    let mut next = from;
    let peers: Vec<_> = net.peers().into_iter().filter(|(_, p)| p.eth_version >= 71).map(|(id, _)| id).collect();
    'outer: while next <= to {
        let count = (to - next + 1).min(64);
        for &peer in &peers {
            let Ok(hs) = net.headers(peer, BlockHashOrNumber::Number(next), count).await.0 else { continue };
            if hs.is_empty() || hs[0].number != next {
                continue;
            }
            let hashes: Vec<alloy_primitives::B256> = hs.iter().map(|h| h.hash_slow()).collect();
            let Ok(bals) = net.bals(peer, hashes.clone()).await.0 else { continue };
            for ((h, hash), raw) in hs.iter().zip(&hashes).zip(bals.0) {
                if prev.is_some_and(|p| p != h.parent_hash) {
                    anyhow::bail!("archive chain does not link at {}", h.number);
                }
                let raw = raw.ok_or_else(|| anyhow::anyhow!("BAL {} unavailable", h.number))?;
                let bal = skim_bal::decode_verified(&raw, h.block_access_list_hash.unwrap_or_default())?;
                let w = BlockWrites::from_bal(&bal);
                let aw: Vec<_> = w.writes.iter().filter(|(k, _)| store.is_archived(k)).copied().collect();
                let rlp = alloy_rlp::encode(h);
                ar.write_block(&skim_state::archive::ArchiveBlock { number: h.number, hash: *hash, header_rlp: &rlp, writes: &aw })?;
                for (ch, c) in &w.code {
                    ar.put_code(*ch, c)?;
                }
                prev = Some(*hash);
                next = h.number + 1;
            }
            continue 'outer;
        }
        anyhow::bail!("no EL peer served headers+BALs from {next}");
    }
    Ok(to + 1 - from)
}
