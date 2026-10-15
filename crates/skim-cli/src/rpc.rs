//! JSON-RPC adapter (PROJECT.md §7.4), built only on `skim-sim`'s public API.
//!
//! HTTP POST, single or batch. Errors follow EIP-1474:
//! - `-32002` resource unavailable: block outside the window, or state peers couldn't serve
//! - `-32004` method not supported: out of scope for a simulation client
//! - `3` execution reverted, with the revert data
//!
//! Block tags: `latest` is the **state-ready** head. A block number inside the retained window
//! and EIP-1898 `{blockHash}` / `{blockNumber}` are accepted. `pending`, `safe`, `finalized` and
//! `earliest` are not tracked yet and answer `-32002`. Within one batch, `latest` is pinned to
//! one block.

use std::sync::Arc;

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types_eth::{Block, BlockId, BlockNumberOrTag, BlockTransactions, TransactionRequest};
use axum::{Json, Router, extract::State, routing::post};
use revm::{
    context::{TxEnv, result::ExecutionResult},
    primitives::TxKind,
};
use serde_json::{Value, json};
use skim_bal::StateKey;
use skim_sim::{Mode, SimError, Simulator};
use skim_state::BlockRef;

pub const CLIENT_VERSION: &str = concat!("skim/", env!("CARGO_PKG_VERSION"), " (P2P-backed simulation; unverified state)");

#[derive(Debug)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), data: None }
    }
    fn unavailable(m: impl Into<String>) -> Self {
        Self::new(-32002, m)
    }
    fn unsupported(method: &str) -> Self {
        Self::new(-32004, format!("{method} is not supported: skim is a simulation client"))
    }
    fn params(m: impl Into<String>) -> Self {
        Self::new(-32602, m)
    }
}

impl From<SimError> for RpcError {
    fn from(e: SimError) -> Self {
        match e {
            SimError::Evm(m) => Self::new(-32000, m),
            other => Self::unavailable(other.to_string()),
        }
    }
}

type R = Result<Value, RpcError>;

const OUT_OF_SCOPE: &[&str] = &[
    "eth_sendRawTransaction",
    "eth_sendTransaction",
    "eth_getLogs",
    "eth_getTransactionReceipt",
    "eth_getTransactionByHash",
    "eth_newFilter",
    "eth_getFilterChanges",
    "eth_subscribe",
];

fn hex_u64(n: u64) -> Value {
    Value::String(format!("{n:#x}"))
}

fn param<T: serde::de::DeserializeOwned>(params: &Value, i: usize) -> Result<Option<T>, RpcError> {
    match params.get(i) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => serde_json::from_value(v.clone()).map(Some).map_err(|e| RpcError::params(format!("param {i}: {e}"))),
    }
}

fn need<T: serde::de::DeserializeOwned>(params: &Value, i: usize) -> Result<T, RpcError> {
    param(params, i)?.ok_or_else(|| RpcError::params(format!("missing param {i}")))
}

pub struct Rpc {
    pub sim: Arc<Simulator>,
    pub subs: Option<Arc<skim_sim::subscription::Subscriptions>>,
}

impl Rpc {
    fn resolve_block(&self, id: Option<BlockId>, latest: &Option<BlockRef>) -> Result<BlockRef, RpcError> {
        let store = &self.sim.store;
        let latest = || latest.ok_or_else(|| RpcError::unavailable("not following the chain yet"));
        match id.unwrap_or(BlockId::Number(BlockNumberOrTag::Latest)) {
            BlockId::Number(BlockNumberOrTag::Latest) => latest(),
            BlockId::Number(BlockNumberOrTag::Number(n)) => {
                store.block(n).ok_or_else(|| RpcError::unavailable(format!("block {n} is outside the retained window")))
            }
            BlockId::Number(BlockNumberOrTag::Pending) => Err(RpcError::new(-32004, "pending state is not tracked")),
            BlockId::Number(t) => Err(RpcError::unavailable(format!("block tag {t} is not tracked yet"))),
            BlockId::Hash(h) => {
                let head = latest()?;
                (head.number.saturating_sub(512)..=head.number)
                    .rev()
                    .filter_map(|n| store.block(n))
                    .find(|b| b.hash == h.block_hash)
                    .ok_or_else(|| RpcError::unavailable(format!("block {} is not on the followed chain window", h.block_hash)))
            }
        }
    }

    fn header(&self, b: &BlockRef) -> Result<Header, RpcError> {
        self.sim.header(b).ok_or_else(|| RpcError::unavailable(format!("no header for block {}", b.number)))
    }

    fn tx_env(&self, req: TransactionRequest, header: &Header) -> TxEnv {
        let gas = req.gas.unwrap_or(header.gas_limit).min(header.gas_limit.max(30_000_000));
        let data = req.input.input().cloned().unwrap_or_default();
        TxEnv::builder()
            .caller(req.from.unwrap_or_default())
            .kind(req.to.unwrap_or(TxKind::Create))
            .data(data)
            .value(req.value.unwrap_or_default())
            .gas_limit(gas)
            .gas_price(0)
            .build()
            .unwrap_or_default()
    }

    fn exec_result(r: &ExecutionResult) -> R {
        match r {
            ExecutionResult::Success { output, .. } => Ok(json!(Bytes::copy_from_slice(output.data()))),
            ExecutionResult::Revert { output, .. } => {
                Err(RpcError { code: 3, message: "execution reverted".into(), data: Some(json!(output)) })
            }
            ExecutionResult::Halt { reason, .. } => Err(RpcError::new(-32000, format!("execution halted: {reason:?}"))),
        }
    }

    pub async fn dispatch(&self, method: &str, params: &Value, latest: &Option<BlockRef>) -> R {
        let sim = &self.sim;
        match method {
            "web3_clientVersion" => Ok(json!(CLIENT_VERSION)),
            "eth_chainId" => Ok(hex_u64(skim_sim::env::SEPOLIA_CHAIN_ID)),
            "net_version" => Ok(json!(skim_sim::env::SEPOLIA_CHAIN_ID.to_string())),
            "net_listening" => Ok(json!(true)),
            "eth_syncing" => Ok(json!(latest.is_none())),
            "eth_blockNumber" => Ok(hex_u64(self.resolve_block(None, latest)?.number)),
            "eth_getBlockByNumber" | "eth_getBlockByHash" => {
                let id: BlockId = if method == "eth_getBlockByHash" {
                    BlockId::from(need::<B256>(params, 0)?)
                } else {
                    BlockId::Number(need::<BlockNumberOrTag>(params, 0)?)
                };
                let b = match self.resolve_block(Some(id), latest) {
                    Ok(b) => b,
                    Err(e) if e.code == -32002 => return Ok(Value::Null),
                    Err(e) => return Err(e),
                };
                let h = self.header(&b)?;
                let block: Block<alloy_rpc_types_eth::Transaction> = Block {
                    header: alloy_rpc_types_eth::Header::new(h),
                    uncles: vec![],
                    // Transactions are not retained (simulation client): an empty list.
                    transactions: BlockTransactions::Hashes(vec![]),
                    withdrawals: None,
                };
                Ok(serde_json::to_value(block).unwrap())
            }
            "eth_getBalance" | "eth_getTransactionCount" | "eth_getCode" => {
                let a: Address = need(params, 0)?;
                let b = self.resolve_block(param(params, 1)?, latest)?;
                match method {
                    "eth_getBalance" => Ok(json!(sim.read(&b, StateKey::Balance(a)).await?)),
                    "eth_getTransactionCount" => Ok(json!(sim.read(&b, StateKey::Nonce(a)).await?)),
                    _ => Ok(json!(sim.code(&b, a).await?)),
                }
            }
            "eth_getStorageAt" => {
                let a: Address = need(params, 0)?;
                let slot: U256 = need(params, 1)?;
                let b = self.resolve_block(param(params, 2)?, latest)?;
                let v = sim.read(&b, StateKey::Storage(a, slot)).await?;
                Ok(json!(B256::from(v)))
            }
            "eth_call" => {
                if params.get(2).is_some_and(|v| !v.is_null()) {
                    return Err(RpcError::params("state overrides are not supported yet"));
                }
                let req: TransactionRequest = need(params, 0)?;
                let b = self.resolve_block(param(params, 1)?, latest)?;
                let header = self.header(&b)?;
                let out = sim.call(&b, self.tx_env(req, &header), Mode::Resolve { max_rounds: 8 }).await?;
                Self::exec_result(&out.result)
            }
            "eth_estimateGas" => {
                let req: TransactionRequest = need(params, 0)?;
                let b = self.resolve_block(param(params, 1)?, latest)?;
                let header = self.header(&b)?;
                let cap = req.gas.unwrap_or(header.gas_limit);
                let mut tx = self.tx_env(req, &header);
                tx.gas_limit = cap;
                let first = sim.call(&b, tx.clone(), Mode::Resolve { max_rounds: 8 }).await?;
                Self::exec_result(&first.result)?;
                // State is now local: binary-search the lowest gas limit that still succeeds.
                let (mut lo, mut hi) = (first.result.tx_gas_used().saturating_sub(1), cap);
                while lo + 1 < hi {
                    let mid = lo + (hi - lo) / 2;
                    tx.gas_limit = mid;
                    let ok = matches!(sim.call(&b, tx.clone(), Mode::WarmOnly).await, Ok(o) if o.result.is_success());
                    if ok { hi = mid } else { lo = mid }
                }
                Ok(hex_u64(hi))
            }
            "eth_gasPrice" | "eth_maxPriorityFeePerGas" => {
                let b = self.resolve_block(None, latest)?;
                let base = self.header(&b)?.base_fee_per_gas.unwrap_or_default() as u128;
                let tip = 1_000_000_000u128;
                Ok(json!(U256::from(if method == "eth_gasPrice" { base + tip } else { tip })))
            }
            "skim_subscriptionStatus" => Ok(serde_json::to_value(self.subs.as_ref().map(|s| s.status()).unwrap_or_default()).unwrap()),
            m if OUT_OF_SCOPE.contains(&m) => Err(RpcError::unsupported(m)),
            m => Err(RpcError::new(-32601, format!("method {m} not found"))),
        }
    }

    async fn one(&self, req: &Value, latest: &Option<BlockRef>) -> Value {
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let Some(method) = req.get("method").and_then(Value::as_str) else {
            return json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32600, "message": "invalid request"}});
        };
        let params = req.get("params").cloned().unwrap_or(json!([]));
        let t = std::time::Instant::now();
        let res = self.dispatch(method, &params, latest).await;
        tracing::debug!(method, ms = t.elapsed().as_secs_f64() * 1e3, ok = res.is_ok(), "rpc");
        match res {
            Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
            Err(e) => {
                let mut err = json!({"code": e.code, "message": e.message});
                if let Some(d) = e.data {
                    err["data"] = d;
                }
                json!({"jsonrpc": "2.0", "id": id, "error": err})
            }
        }
    }
}

async fn handle(State(rpc): State<Arc<Rpc>>, Json(body): Json<Value>) -> Json<Value> {
    let latest = rpc.sim.store.head();
    match body {
        Value::Array(reqs) => {
            let mut out = Vec::with_capacity(reqs.len());
            for r in &reqs {
                out.push(rpc.one(r, &latest).await);
            }
            Json(Value::Array(out))
        }
        req => Json(rpc.one(&req, &latest).await),
    }
}

pub async fn serve(rpc: Arc<Rpc>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let app = Router::new().route("/", post(handle)).with_state(rpc);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "JSON-RPC listening");
    axum::serve(listener, app).await?;
    Ok(())
}
