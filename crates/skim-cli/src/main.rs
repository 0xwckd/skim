//! `skim` command line. For now: M0 probes (PROJECT.md §10).

mod follow;
mod learned;
mod predictor;
mod prefetch;
mod probe_cl;
mod probe_call;
mod probe_el;
mod stats;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "skim", about = "P2P-backed Ethereum simulation client")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// M0 Spike B: Sepolia EL peers, eth/71 BALs, snap/2 point lookups, root-servable lag.
    ProbeEl(probe_el::Args),
    /// M0 Spike C: cold-call SNAP rounds and latency vs Infura eth_call at the same block.
    ProbeCall(probe_call::Args),
    /// M1: follow the chain from CL gossip BALs into the MVCC store; time block -> state ready
    /// -> eth_call done vs when Infura can answer the same block.
    Follow(follow::Args),
    /// Peer-count sweep: one large learned wave fetched with 1/2/4/all peers per block.
    ProbeWave(probe_call::WaveArgs),
    /// M0 Spike A: Sepolia CL gossip listener; execution-payload envelope timing and BAL checks.
    ProbeCl(probe_cl::Args),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,reth_network=warn,discv5=warn,libp2p_gossipsub=warn".into()),
        )
        .init();
    match Cli::parse().cmd {
        Cmd::ProbeEl(args) => probe_el::run(args).await,
        Cmd::ProbeCall(args) => probe_call::run(args).await,
        Cmd::Follow(args) => follow::run(args).await,
        Cmd::ProbeWave(args) => probe_call::run_wave(args).await,
        Cmd::ProbeCl(args) => probe_cl::run(args).await,
    }
}
