//! JSON-RPC adapter (PROJECT.md §7.4), built only on `skim-sim`'s public API.
//!
//! HTTP POST, single or batch. Errors follow EIP-1474:
//! - `-32002` resource unavailable: block outside the window, or state peers couldn't serve
//! - `-32004` method not supported: out of scope for a simulation client
//! - `3` execution reverted, with the revert data
//!
//! Block tags: `latest` is the **state-ready** head. A block number inside the retained window
//! and EIP-1898 `{blockHash}` / `{blockNumber}` are accepted. `pending` reads the latest state,
//! except that `eth_getTransactionCount` counts transactions sent through skim and not yet
//! included. `safe`, `finalized` and `earliest` are not tracked yet and answer `-32002`. Within
//! one batch, `latest` is pinned to one block.
//!
//! Transactions, receipts and logs come from [`crate::chaindata`].

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

use crate::chaindata::{ChainData, DataError};

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
    "eth_sendTransaction", // skim holds no keys: sign locally, then eth_sendRawTransaction
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
    pub chain: Option<Arc<ChainData>>,
    pub light: Option<Arc<crate::light::Light>>,
    /// `latest` = highest block the sync committee signed off on.
    pub require_verified: bool,
}

impl From<DataError> for RpcError {
    fn from(e: DataError) -> Self {
        match e {
            DataError::Unavailable(m) => Self::unavailable(m),
            DataError::Invalid(m) => Self::new(-32000, m),
            DataError::RangeTooLarge(n) => Self::new(-32005, format!("block range too large: at most {n} blocks per eth_getLogs")),
        }
    }
}

impl Rpc {
    fn resolve_block(&self, id: Option<BlockId>, latest: &Option<BlockRef>) -> Result<BlockRef, RpcError> {
        let store = &self.sim.store;
        let latest = || latest.ok_or_else(|| RpcError::unavailable("not following the chain yet"));
        match id.unwrap_or(BlockId::Number(BlockNumberOrTag::Latest)) {
            BlockId::Number(BlockNumberOrTag::Latest) => latest(),
            BlockId::Number(BlockNumberOrTag::Number(n)) => store
                .block(n)
                .or_else(|| store.archived_block(n).map(|(b, _)| b))
                .ok_or_else(|| RpcError::unavailable(format!("block {n} is outside the retained window and not archived"))),
            // Pending state = latest state (skim keeps no mempool); see eth_getTransactionCount.
            BlockId::Number(BlockNumberOrTag::Pending) => latest(),
            BlockId::Number(t @ (BlockNumberOrTag::Safe | BlockNumberOrTag::Finalized)) => {
                // `safe`: signed by a sync-committee supermajority; `finalized`: finalized
                // beacon block's execution block. Both from the light client.
                let st = self.light.as_ref().map(|l| l.status()).unwrap_or_default();
                let n = if t == BlockNumberOrTag::Safe { st.verified_block } else { st.finalized_block };
                let n = n.ok_or_else(|| RpcError::unavailable(format!("no {t} block verified yet")))?;
                store
                    .block(n)
                    .or_else(|| store.archived_block(n).map(|(b, _)| b))
                    .ok_or_else(|| RpcError::unavailable(format!("{t} block {n} is outside the retained window")))
            }
            BlockId::Number(t) => Err(RpcError::unavailable(format!("block tag {t} is not tracked yet"))),
            BlockId::Hash(h) => {
                let head = latest()?;
                (head.number.saturating_sub(512)..=head.number)
                    .rev()
                    .filter_map(|n| store.block(n))
                    .find(|b| b.hash == h.block_hash)
                    .or_else(|| store.archived_number(&h.block_hash).and_then(|n| store.archived_block(n)).map(|(b, _)| b))
                    .ok_or_else(|| RpcError::unavailable(format!("block {} is not in the window or the archive", h.block_hash)))
            }
        }
    }

    fn chain(&self) -> Result<&Arc<ChainData>, RpcError> {
        self.chain.as_ref().ok_or_else(|| RpcError::unavailable("chain data is not available in this mode"))
    }

    fn header(&self, b: &BlockRef) -> Result<std::sync::Arc<Header>, RpcError> {
        self.sim.header_arc(b).ok_or_else(|| RpcError::unavailable(format!("no header for block {}", b.number)))
    }

    /// Builds the execution for a call request. Every field that changes execution is honoured
    /// (fees, access list, EIP-7702 authorizations) or the request is rejected: nothing is
    /// silently dropped.
    fn tx_env(&self, req: TransactionRequest, header: &Header) -> Result<TxEnv, RpcError> {
        if req.blob_versioned_hashes.as_ref().is_some_and(|v| !v.is_empty()) || req.max_fee_per_blob_gas.is_some() || req.sidecar.is_some() {
            return Err(RpcError::params("blob transaction fields are not supported in calls"));
        }
        if req.gas_price.is_some() && (req.max_fee_per_gas.is_some() || req.max_priority_fee_per_gas.is_some()) {
            return Err(RpcError::params("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified"));
        }
        let gas = req.gas.unwrap_or(header.gas_limit).min(header.gas_limit.max(30_000_000));
        let data = req.input.input().cloned().unwrap_or_default();
        let mut b = TxEnv::builder()
            .caller(req.from.unwrap_or_default())
            .kind(req.to.unwrap_or(TxKind::Create))
            .data(data)
            .value(req.value.unwrap_or_default())
            .gas_limit(gas)
            .chain_id(Some(skim_sim::env::SEPOLIA_CHAIN_ID));
        // Fees: as nodes do for eth_call, 0 unless given (GASPRICE then sees the given price).
        b = match (req.gas_price, req.max_fee_per_gas, req.max_priority_fee_per_gas) {
            (Some(p), _, _) => b.gas_price(p),
            (None, None, None) => b.gas_price(0),
            (None, max, prio) => {
                let max = max.unwrap_or(0);
                b.gas_price(max).gas_priority_fee(Some(prio.unwrap_or(0).min(max)))
            }
        };
        if let Some(al) = req.access_list {
            b = b.access_list(al);
        }
        if let Some(auth) = req.authorization_list {
            if !auth.is_empty() {
                b = b.authorization_list_signed(auth);
            }
        }
        b.build().map_err(|e| RpcError::params(format!("invalid call: {e:?}")))
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
                    header: alloy_rpc_types_eth::Header::new((*h).clone()),
                    uncles: vec![],
                    // Hashes when skim indexed the block's transactions (gossip or a fetched body).
                    transactions: BlockTransactions::Hashes(
                        self.chain.as_ref().and_then(|c| c.block_tx_hashes(b.number, b.hash)).map(|v| v.to_vec()).unwrap_or_default(),
                    ),
                    withdrawals: None,
                };
                Ok(serde_json::to_value(block).unwrap())
            }
            "eth_getBalance" | "eth_getTransactionCount" | "eth_getCode" => {
                let a: Address = need(params, 0)?;
                let b = self.resolve_block(param(params, 1)?, latest)?;
                match method {
                    "eth_getBalance" => Ok(json!(sim.read(&b, StateKey::Balance(a)).await?)),
                    "eth_getTransactionCount" => {
                        let n: u64 = sim.read(&b, StateKey::Nonce(a)).await?.saturating_to();
                        let pending = matches!(param::<BlockId>(params, 1)?, Some(BlockId::Number(BlockNumberOrTag::Pending)));
                        Ok(hex_u64(match (&self.chain, pending) {
                            (Some(c), true) => c.pending_nonce(a, n),
                            _ => n,
                        }))
                    }
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
                let out = sim.call(&b, self.tx_env(req, &header)?, Mode::Resolve { max_rounds: 8 }).await?;
                tracing::debug!(number = b.number, predicted_keys = out.predicted.as_ref().map_or(0, |p| p.keys),
                    predicted_ms = out.predicted.as_ref().map_or(0.0, |p| p.wall_ms), rounds = out.rounds.len(),
                    round_keys = out.rounds.iter().map(|r| r.keys).sum::<usize>(), "eth_call fetches");
                Self::exec_result(&out.result)
            }
            "eth_estimateGas" => {
                let req: TransactionRequest = need(params, 0)?;
                let b = self.resolve_block(param(params, 1)?, latest)?;
                let header = self.header(&b)?;
                let cap = req.gas.unwrap_or(header.gas_limit);
                let mut tx = self.tx_env(req, &header)?;
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
            "eth_sendRawTransaction" => {
                let chain = self.chain()?;
                let raw: Bytes = need(params, 0)?;
                let (tx, from) = ChainData::decode_raw(&raw)?;
                // The early errors a node's pool would give, from verified state at the head.
                use alloy_consensus::Transaction as _;
                let b = self.resolve_block(None, latest)?;
                let nonce: u64 = sim.read(&b, StateKey::Nonce(from)).await?.saturating_to();
                if tx.nonce() < nonce {
                    return Err(RpcError::new(-32000, format!("nonce too low: next nonce {nonce}, tx nonce {}", tx.nonce())));
                }
                let balance = sim.read(&b, StateKey::Balance(from)).await?;
                let cost = U256::from(tx.gas_limit()) * U256::from(tx.max_fee_per_gas()) + tx.value();
                if balance < cost {
                    return Err(RpcError::new(-32000, format!("insufficient funds for gas * price + value: balance {balance}, cost {cost}")));
                }
                let (hash, peers) = chain.send(tx, from);
                if peers == 0 {
                    tracing::warn!(tx = %hash, "no EL peers connected; will re-broadcast");
                }
                Ok(json!(hash))
            }
            "eth_getTransactionByHash" => Ok(self.chain()?.transaction(need(params, 0)?).await?),
            "eth_getTransactionReceipt" => Ok(self.chain()?.receipt(need(params, 0)?).await?),
            "eth_getLogs" => {
                let chain = self.chain()?;
                let filter: alloy_rpc_types_eth::Filter = need(params, 0)?;
                let head = self.resolve_block(None, latest)?.number;
                let (from, to) = match filter.block_option {
                    alloy_rpc_types_eth::FilterBlockOption::AtBlockHash(h) => {
                        let n = chain.number_of(&h).ok_or_else(|| RpcError::unavailable(format!("block {h} is not known")))?;
                        (n, n)
                    }
                    _ => ChainData::range(&filter, head)?,
                };
                Ok(chain.logs(&filter, from, to).await?)
            }
            "skim_status" => {
                let z = sim.store.sizes();
                let peers = sim.sched.transport().net.peers();
                Ok(json!({
                    "head": sim.store.head().map(|h| h.number),
                    "store": {
                        "chain": z.chain, "recent_bals": z.recent_bals, "keys": z.keys, "versions": z.versions,
                        "subscribed_keys": z.watched_keys, "on_demand_keys": z.temp_keys,
                        "code": z.code, "code_bytes": z.code_bytes, "admissions_refused": z.admissions_refused,
                        "on_demand_evicted": z.on_demand_evicted, "recent_bal_bytes": z.recent_bal_bytes,
                    },
                    "light_client": self.light.as_ref().map(|l| l.status()),
                    "el_peers": peers.len(),
                    "snap_peers": peers.iter().filter(|(_, p)| p.snap1 || p.snap2).count(),
                }))
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
    let latest = if rpc.require_verified {
        rpc.light.as_ref().and_then(|l| l.status().verified_block).and_then(|n| rpc.sim.store.block(n))
    } else {
        rpc.sim.store.head()
    };
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
