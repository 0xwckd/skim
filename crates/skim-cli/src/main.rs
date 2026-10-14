//! `skim` command line. For now: M0 probes (PROJECT.md §10).

mod learned;
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
    /// Peer-count sweep: one large learned wave fetched with 1/2/4/all peers per block.
    ProbeWave(probe_call::WaveArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,reth_network=warn,discv5=warn".into()),
        )
        .init();
    match Cli::parse().cmd {
        Cmd::ProbeEl(args) => probe_el::run(args).await,
        Cmd::ProbeCall(args) => probe_call::run(args).await,
        Cmd::ProbeWave(args) => probe_call::run_wave(args).await,
    }
}
