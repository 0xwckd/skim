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
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let net = ElNet::start_sepolia(args.el_port, Vec::new(), false).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let sched = Scheduler::new(ElTransport::new(net.clone()), skim_snap::Config::default());
    let store = Store::new(args.window);
    let sim = Simulator::new(
        store.clone(),
        sched,
        Predictor { learned: Learned::new(8, 0.5, 256), v3: vec![], base_words: 2, reach: Default::default() },
    );
    let (_cl, mut rx) = ClNode::start(NodeConfig::sepolia(args.cl_port)).await?;
    tokio::spawn(serve(Arc::new(Rpc { sim: sim.clone() }), args.rpc));
    let sim2 = sim.clone();
    let chain = Chain { store: store.clone(), net: net.clone(), on_header: Box::new(move |h| sim2.insert_header(h.clone())), lookback: 32 };
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
        match chain.place(v.header, Arc::new(BlockWrites::from_bal(&bal))).await {
            Placed::Applied { ready, reorged_to, filled } => {
                tracing::info!(number, changed = ready.changed_keys.len(), ?reorged_to, filled, "state ready")
            }
            Placed::Reanchored(_) => tracing::warn!(number, "re-anchored"),
            Placed::Duplicate => {}
            Placed::Deferred(why) => tracing::debug!(number, "deferred: {why}"),
        }
    }
    Ok(())
}
