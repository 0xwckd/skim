//! P3 baseline: drive a full node's JSON-RPC with the same per-block workload `skim follow` runs
//! (the 7 fixed calls + the same seeded random V3 quotes), so resource use can be compared at equal
//! work. Resource sampling (RSS/CPU/disk/network) is done externally.

use std::time::{Duration, Instant};

use crate::{
    probe_call::{calls, random_quotes},
    stats::Samples,
};

#[derive(clap::Args)]
pub struct Args {
    /// The node's JSON-RPC URL.
    #[arg(long, default_value = "http://127.0.0.1:8545")]
    rpc: String,
    /// Blocks to run.
    #[arg(long, default_value_t = 300)]
    blocks: u64,
    #[arg(long, default_value_t = 8)]
    random_quotes: usize,
    #[arg(long, default_value = "data/bench-node.jsonl")]
    out: String,
}

async fn rpc(client: &reqwest::Client, url: &str, method: &str, params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let v: serde_json::Value = client
        .post(url)
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await?
        .json()
        .await?;
    Ok(v)
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    use std::io::Write;
    let client = reqwest::Client::new();
    let mut out = std::io::BufWriter::new(std::fs::File::create(&args.out)?);
    let mut lat = Samples::default();
    let (mut ok, mut err) = (0u64, 0u64);
    let mut last = 0u64;
    let mut done = 0u64;
    while done < args.blocks {
        let n = rpc(&client, &args.rpc, "eth_blockNumber", serde_json::json!([])).await?;
        let n = u64::from_str_radix(n["result"].as_str().unwrap_or("0x0").trim_start_matches("0x"), 16)?;
        if n <= last {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        last = n;
        done += 1;
        let tag = format!("{n:#x}");
        let mut all = calls();
        all.extend(random_quotes(n, args.random_quotes));
        let t_block = Instant::now();
        for c in &all {
            let t = Instant::now();
            let r = rpc(&client, &args.rpc, "eth_call", serde_json::json!([{"to": c.to, "data": c.data}, tag])).await;
            let ms = t.elapsed().as_secs_f64() * 1e3;
            lat.push_ms(ms);
            match r {
                Ok(v) if v.get("result").is_some() || v.get("error").is_some_and(|e| e["code"] == 3) => ok += 1,
                _ => err += 1,
            }
        }
        let _ = writeln!(out, "{}", serde_json::json!({"number": n, "calls": all.len(), "block_ms": t_block.elapsed().as_secs_f64() * 1e3}));
        let _ = out.flush();
        if done % 25 == 0 {
            tracing::info!(done, n, %ok, %err, "eth_call {}", lat.summary());
        }
    }
    println!("{}", serde_json::json!({"blocks": done, "calls_ok": ok, "calls_err": err, "eth_call": lat.json()}));
    Ok(())
}
