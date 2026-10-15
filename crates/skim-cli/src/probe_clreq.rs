//! `skim probe-cl-req`: asks consensus peers for recent payload envelopes and a light-client
//! bootstrap, printing each peer's answer (diagnostics for the CL req/resp paths).

use skim_cl::{ClNode, NodeConfig};

#[derive(clap::Args)]
pub struct Args {
    #[arg(long, default_value_t = 9230)]
    cl_port: u16,
    /// Beacon block root for the bootstrap request (default: none).
    #[arg(long)]
    root: Option<alloy_primitives::B256>,
    #[arg(long, default_value_t = 12)]
    attempts: usize,
    /// Save received envelopes (raw SSZ) here, e.g. as fuzzing seeds.
    #[arg(long)]
    dump: Option<std::path::PathBuf>,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let (cl, _rx) = ClNode::start(NodeConfig::sepolia(args.cl_port)).await?;
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
    let slot = (now - skim_cl::node::SEPOLIA_GENESIS_TIME) / 12;
    for i in 0..args.attempts {
        let start = slot - 40;
        match cl.envelopes_by_range(start, 8).await {
            Ok(v) => {
                println!("envelopes {start}..+8 attempt {i}: {} chunks, sizes {:?}", v.len(), v.iter().map(Vec::len).collect::<Vec<_>>());
                if let Some(d) = &args.dump {
                    std::fs::create_dir_all(d)?;
                    for (j, e) in v.iter().enumerate() {
                        std::fs::write(d.join(format!("envelope-{start}-{j}.ssz")), e)?;
                    }
                }
            }
            Err(e) => println!("envelopes {start}..+8 attempt {i}: {e}"),
        }
        if let Some(r) = args.root {
            match cl.light_client_bootstrap(r.0).await {
                Ok(b) => println!("bootstrap attempt {i}: {} bytes (expected {})", b.len(), skim_cl::light::BOOTSTRAP_LEN),
                Err(e) => println!("bootstrap attempt {i}: {e}"),
            }
        }
    }
    Ok(())
}
