//! Upstream-backed Nitro reads. Cached values are pinned to block hashes; this
//! module does not execute transactions or verify the upstream's state proofs.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result as AnyResult, anyhow};
use reqwest::{Client, Url};
use serde_json::{Value, json};

const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Head {
    pub number: u64,
    pub hash: String,
    pub block: Value,
}

#[derive(Clone, Debug)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }

    fn upstream(message: impl Into<String>) -> Self {
        Self {
            code: -32098,
            message: message.into(),
            data: None,
        }
    }

    fn unavailable() -> Self {
        Self {
            code: -32001,
            message: "Requested block is unavailable".into(),
            data: None,
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (RPC {})", self.message, self.code)
    }
}

impl std::error::Error for RpcError {}

struct CacheEntry {
    value: Value,
    bytes: usize,
}

struct Cache {
    entries: HashMap<String, CacheEntry>,
    order: VecDeque<String>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl Cache {
    fn get(&self, key: &str) -> Option<Value> {
        self.entries.get(key).map(|entry| entry.value.clone())
    }

    fn insert(&mut self, key: String, value: Value) {
        let bytes = key.len().saturating_add(value.to_string().len());
        if self.max_entries == 0 || bytes > self.max_bytes || self.entries.contains_key(&key) {
            return;
        }
        while self.entries.len() >= self.max_entries
            || self.bytes.saturating_add(bytes) > self.max_bytes
        {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(old.bytes);
            }
        }
        self.bytes += bytes;
        self.order.push_back(key.clone());
        self.entries.insert(key, CacheEntry { value, bytes });
    }
}

pub struct Backend {
    client: Client,
    upstream: Url,
    chain_id: u64,
    next_id: AtomicU64,
    head: RwLock<Option<Head>>,
    cache: Mutex<Cache>,
    upstream_requests: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl Backend {
    pub async fn connect(
        upstream: &str,
        expected_chain_id: u64,
        max_cache_entries: usize,
        max_cache_bytes: usize,
    ) -> AnyResult<Self> {
        // Do not include a provider URL (which can contain API keys) in errors.
        let upstream = Url::parse(upstream).map_err(|_| anyhow!("Invalid upstream RPC URL"))?;
        if !matches!(upstream.scheme(), "http" | "https") || upstream.host_str().is_none() {
            return Err(anyhow!(
                "Upstream RPC URL must use HTTP or HTTPS and include a host"
            ));
        }
        if !upstream.username().is_empty()
            || upstream.password().is_some()
            || upstream.fragment().is_some()
        {
            return Err(anyhow!(
                "Upstream RPC URL must not contain credentials or a fragment"
            ));
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("skim-robinhood/0.1.0")
            .build()
            .map_err(|_| anyhow!("Unable to initialize upstream HTTP client"))?;
        let backend = Self {
            client,
            upstream,
            chain_id: expected_chain_id,
            next_id: AtomicU64::new(1),
            head: RwLock::new(None),
            cache: Mutex::new(Cache {
                entries: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                max_entries: max_cache_entries,
                max_bytes: max_cache_bytes,
            }),
            upstream_requests: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        };
        let actual = backend
            .rpc("eth_chainId", json!([]))
            .await
            .context("Upstream chain handshake failed")?;
        let actual = quantity_u64(&actual)
            .ok_or_else(|| anyhow!("Upstream returned an invalid chain ID"))?;
        if actual != expected_chain_id {
            return Err(anyhow!(
                "Upstream chain ID mismatch: expected {expected_chain_id}, received {actual}"
            ));
        }
        backend
            .refresh_head()
            .await
            .context("Unable to read upstream head")?;
        Ok(backend)
    }

    pub async fn refresh_head(&self) -> Result<Head, RpcError> {
        let block = self
            .rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let head = parse_head(block)?;
        *self
            .head
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(head.clone());
        Ok(head)
    }

    pub fn head(&self) -> Option<Head> {
        self.head
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn status(&self) -> Value {
        let cache = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let head = self.head();
        json!({
            "mode": "upstream-backed-nitro",
            "chainId": format!("0x{:x}", self.chain_id),
            "upstreamTrusted": true,
            "proofsVerified": false,
            "localExecution": false,
            "head": head.map(|head| json!({"number": format!("0x{:x}", head.number), "hash": head.hash})),
            "cache": {"entries": cache.entries.len(), "bytes": cache.bytes, "maxEntries": cache.max_entries, "maxBytes": cache.max_bytes,
                "hits": self.cache_hits.load(Ordering::Relaxed), "misses": self.cache_misses.load(Ordering::Relaxed)},
            "upstreamRequests": self.upstream_requests.load(Ordering::Relaxed),
        })
    }

    pub async fn dispatch(
        &self,
        method: &str,
        params: Value,
        head: &Head,
    ) -> Result<Value, RpcError> {
        let args = params
            .as_array()
            .ok_or_else(|| RpcError::invalid("Parameters must be a JSON array"))?;
        match method {
            "eth_chainId" | "net_version" | "eth_blockNumber" | "web3_clientVersion"
            | "net_listening" | "skim_status" => {
                if !args.is_empty() {
                    return Err(RpcError::invalid("This method takes no parameters"));
                }
                return Ok(match method {
                    "eth_chainId" => json!(format!("0x{:x}", self.chain_id)),
                    "net_version" => json!(self.chain_id.to_string()),
                    "eth_blockNumber" => json!(format!("0x{:x}", head.number)),
                    "web3_clientVersion" => json!("skim-robinhood/0.1.0/upstream-backed-nitro"),
                    "net_listening" => json!(true),
                    _ => self.status(),
                });
            }
            _ => {}
        }

        if method == "eth_getBlockByNumber" && args.first() == Some(&json!("latest")) {
            if args.len() != 2 || !args[1].is_boolean() {
                return Err(RpcError::invalid(
                    "Block queries require a selector and a boolean transaction flag",
                ));
            }
            if args[1] == false {
                return Ok(head.block.clone());
            }
            let block = self
                .rpc("eth_getBlockByHash", json!([head.hash, true]))
                .await?;
            let resolved = parse_head(block)?;
            if resolved.hash != head.hash || resolved.number != head.number {
                return Err(RpcError::upstream("Upstream returned a different block"));
            }
            return Ok(resolved.block);
        }

        if matches!(method, "eth_estimateGas" | "eth_createAccessList") {
            if !(1..=3).contains(&args.len())
                || !args[0].is_object()
                || args.get(2).is_some_and(|value| !value.is_object())
            {
                return Err(RpcError::invalid(
                    "Gas estimation requires a transaction object, optional block selector, and optional state override object",
                ));
            }
            let selector = args.get(1).cloned().unwrap_or_else(|| json!("latest"));
            if selector == "pending" {
                return self.rpc(method, params).await;
            }
            let (pinned, require_canonical) = self.resolve_block(&selector, head).await?;
            let mut pinned_args = args.clone();
            if pinned_args.len() == 1 {
                pinned_args.push(Value::Null);
            }
            pinned_args[1] =
                json!({"blockHash": pinned.hash, "requireCanonical": require_canonical});
            // Nitro computes these every time: its fee estimate can change even
            // when the L2 execution state is pinned to the same block.
            return self.rpc(method, Value::Array(pinned_args)).await;
        }

        if let Some(block_index) = cached_block_index(method) {
            validate_cached_params(method, args)?;
            let selector = args
                .get(block_index)
                .cloned()
                .unwrap_or_else(|| json!("latest"));
            if selector == "pending" {
                // Preserve pending semantics, including an omitted/explicit block parameter.
                return self.rpc(method, params).await;
            }
            let (pinned, require_canonical) = self.resolve_block(&selector, head).await?;
            let mut pinned_args = args.clone();
            while pinned_args.len() <= block_index {
                pinned_args.push(Value::Null);
            }
            pinned_args[block_index] =
                json!({"blockHash": pinned.hash, "requireCanonical": require_canonical});
            let pinned_params = Value::Array(pinned_args);
            let key = format!("{method}:{}", pinned_params);
            let cached = self
                .cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&key);
            if let Some(value) = cached {
                // A canonical hash can become orphaned. EIP-1898 requires rejecting it
                // even when its immutable state remains available in our cache.
                if require_canonical {
                    self.ensure_canonical(&pinned).await?;
                }
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(value);
            }
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            let value = self.rpc(method, pinned_params).await?;
            if !valid_cached_result(method, &value)
                || (method == "eth_getProof" && !proof_matches_request(args, &value))
            {
                return Err(RpcError::upstream(
                    "Upstream returned an invalid state result",
                ));
            }
            self.cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(key, value.clone());
            return Ok(value);
        }

        if read_only_passthrough(method) {
            return self.rpc(method, params).await;
        }
        Err(RpcError {
            code: -32601,
            message: "Method is not supported by this read-only RPC".into(),
            data: None,
        })
    }

    async fn resolve_block(&self, selector: &Value, head: &Head) -> Result<(Head, bool), RpcError> {
        if let Some(tag) = selector.as_str() {
            if tag == "latest" {
                return Ok((head.clone(), true));
            }
            if matches!(tag, "earliest" | "safe" | "finalized") || quantity_u64(selector).is_some()
            {
                let block = self
                    .rpc("eth_getBlockByNumber", json!([tag, false]))
                    .await?;
                let resolved = parse_head(block)?;
                if quantity_u64(selector).is_some_and(|number| number != resolved.number)
                    || (tag == "earliest" && resolved.number != 0)
                {
                    return Err(RpcError::upstream(
                        "Upstream returned a different block number",
                    ));
                }
                return Ok((resolved, true));
            }
            return Err(RpcError::invalid("Invalid block tag or number"));
        }
        let object = selector
            .as_object()
            .ok_or_else(|| RpcError::invalid("Invalid block selector"))?;
        if let Some(hash) = object.get("blockHash") {
            if object
                .keys()
                .any(|key| key != "blockHash" && key != "requireCanonical")
                || !is_hex_data(hash, Some(32))
            {
                return Err(RpcError::invalid("Invalid block hash selector"));
            }
            let require_canonical = match object.get("requireCanonical") {
                None => false,
                Some(Value::Bool(value)) => *value,
                _ => return Err(RpcError::invalid("requireCanonical must be a boolean")),
            };
            let block = self.rpc("eth_getBlockByHash", json!([hash, false])).await?;
            let pinned = parse_head(block)?;
            if !pinned
                .hash
                .eq_ignore_ascii_case(hash.as_str().unwrap_or_default())
            {
                return Err(RpcError::upstream(
                    "Upstream returned a different block hash",
                ));
            }
            return Ok((pinned, require_canonical));
        }
        if object.len() == 1
            && let Some(number) = object.get("blockNumber")
            && let Some(expected_number) = quantity_u64(number)
        {
            let block = self
                .rpc("eth_getBlockByNumber", json!([number, false]))
                .await?;
            let resolved = parse_head(block)?;
            if resolved.number != expected_number {
                return Err(RpcError::upstream(
                    "Upstream returned a different block number",
                ));
            }
            return Ok((resolved, true));
        }
        Err(RpcError::invalid("Invalid EIP-1898 block selector"))
    }

    async fn ensure_canonical(&self, pinned: &Head) -> Result<(), RpcError> {
        let canonical = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{:x}", pinned.number), false]),
            )
            .await?;
        let canonical = parse_head(canonical)?;
        if canonical.hash != pinned.hash {
            return Err(RpcError {
                code: -32000,
                message: "Requested block is not canonical".into(),
                data: None,
            });
        }
        Ok(())
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.upstream_requests.fetch_add(1, Ordering::Relaxed);
        let mut response = self
            .client
            .post(self.upstream.clone())
            .json(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    RpcError::upstream("Upstream RPC request timed out")
                } else {
                    RpcError::upstream("Upstream RPC transport failed")
                }
            })?;
        if !response.status().is_success() {
            return Err(RpcError::upstream(format!(
                "Upstream RPC returned HTTP {}",
                response.status().as_u16()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err(RpcError::upstream("Upstream RPC response exceeds 8 MiB"));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| RpcError::upstream("Unable to read upstream RPC response"))?
        {
            if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(RpcError::upstream("Upstream RPC response exceeds 8 MiB"));
            }
            body.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&body)
            .map_err(|_| RpcError::upstream("Upstream returned invalid JSON"))?;
        let object = value
            .as_object()
            .ok_or_else(|| RpcError::upstream("Upstream returned an invalid JSON-RPC envelope"))?;
        if object.get("jsonrpc") != Some(&json!("2.0"))
            || object.get("id") != Some(&json!(id))
            || object.contains_key("result") == object.contains_key("error")
        {
            return Err(RpcError::upstream(
                "Upstream returned an invalid JSON-RPC envelope",
            ));
        }
        if let Some(error) = object.get("error") {
            let code = error
                .get("code")
                .and_then(Value::as_i64)
                .ok_or_else(|| RpcError::upstream("Upstream returned an invalid RPC error"))?;
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(|| RpcError::upstream("Upstream returned an invalid RPC error"))?;
            return Err(RpcError {
                code,
                message: message.replace(self.upstream.as_str(), "[upstream]"),
                data: error.get("data").cloned(),
            });
        }
        Ok(object["result"].clone())
    }
}

fn parse_head(block: Value) -> Result<Head, RpcError> {
    if block.is_null() {
        return Err(RpcError::unavailable());
    }
    let number = block
        .get("number")
        .and_then(quantity_u64)
        .ok_or_else(|| RpcError::upstream("Upstream returned an invalid block number"))?;
    let hash = block
        .get("hash")
        .filter(|hash| is_hex_data(hash, Some(32)))
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::upstream("Upstream returned an invalid block hash"))?
        .to_ascii_lowercase();
    Ok(Head {
        number,
        hash,
        block,
    })
}

fn quantity_u64(value: &Value) -> Option<u64> {
    let text = value.as_str()?;
    if !is_quantity(value) {
        return None;
    }
    u64::from_str_radix(&text[2..], 16).ok()
}

fn is_quantity(value: &Value) -> bool {
    let Some(text) = value.as_str().and_then(|text| text.strip_prefix("0x")) else {
        return false;
    };
    !text.is_empty()
        && text.len() <= 64
        && (text.len() == 1 || !text.starts_with('0'))
        && text.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_hex_data(value: &Value, bytes: Option<usize>) -> bool {
    let Some(text) = value.as_str().and_then(|text| text.strip_prefix("0x")) else {
        return false;
    };
    text.len() % 2 == 0
        && bytes.is_none_or(|bytes| text.len() == bytes * 2)
        && text.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn cached_block_index(method: &str) -> Option<usize> {
    match method {
        "eth_call" | "eth_getBalance" | "eth_getCode" | "eth_getTransactionCount" => Some(1),
        "eth_getStorageAt" | "eth_getProof" => Some(2),
        _ => None,
    }
}

fn validate_cached_params(method: &str, args: &[Value]) -> Result<(), RpcError> {
    let (min, max) = match method {
        "eth_call" => (1, 4),
        "eth_getStorageAt" | "eth_getProof" => (2, 3),
        _ => (1, 2),
    };
    if !(min..=max).contains(&args.len()) {
        return Err(RpcError::invalid("Invalid number of parameters"));
    }
    if method == "eth_call" {
        if !args[0].is_object()
            || args.get(2).is_some_and(|value| !value.is_object())
            || args.get(3).is_some_and(|value| !value.is_object())
        {
            return Err(RpcError::invalid(
                "Call and override parameters must be objects",
            ));
        }
    } else if !is_hex_data(&args[0], Some(20)) {
        return Err(RpcError::invalid(
            "Address must contain 20 hexadecimal bytes",
        ));
    }
    if method == "eth_getStorageAt" && !is_quantity(&args[1]) && !is_hex_data(&args[1], Some(32)) {
        return Err(RpcError::invalid(
            "Storage position must be a hexadecimal quantity or 32 bytes",
        ));
    }
    if method == "eth_getProof"
        && !args[1].as_array().is_some_and(|keys| {
            keys.iter()
                .all(|key| is_hex_data(key, Some(32)) || is_quantity(key))
        })
    {
        return Err(RpcError::invalid(
            "Proof storage keys must be hexadecimal quantities or 32 bytes",
        ));
    }
    Ok(())
}

fn valid_cached_result(method: &str, value: &Value) -> bool {
    match method {
        "eth_call" | "eth_getCode" => is_hex_data(value, None),
        "eth_getStorageAt" => is_hex_data(value, Some(32)),
        "eth_getBalance" | "eth_getTransactionCount" => is_quantity(value),
        "eth_getProof" => {
            value.is_object()
                && value
                    .get("address")
                    .is_some_and(|value| is_hex_data(value, Some(20)))
                && value.get("balance").is_some_and(is_quantity)
                && value.get("nonce").is_some_and(is_quantity)
                && value
                    .get("codeHash")
                    .is_some_and(|value| is_hex_data(value, Some(32)))
                && value
                    .get("storageHash")
                    .is_some_and(|value| is_hex_data(value, Some(32)))
                && value
                    .get("accountProof")
                    .and_then(Value::as_array)
                    .is_some_and(|proof| proof.iter().all(|node| is_hex_data(node, None)))
                && value
                    .get("storageProof")
                    .and_then(Value::as_array)
                    .is_some_and(|proof| {
                        proof.iter().all(|item| {
                            item.get("key")
                                .is_some_and(|key| is_quantity(key) || is_hex_data(key, Some(32)))
                                && item.get("value").is_some_and(is_quantity)
                                && item.get("proof").and_then(Value::as_array).is_some_and(
                                    |nodes| nodes.iter().all(|node| is_hex_data(node, None)),
                                )
                        })
                    })
        }
        _ => false,
    }
}

fn normalized_storage_key(value: &Value) -> Option<String> {
    if !is_quantity(value) && !is_hex_data(value, Some(32)) {
        return None;
    }
    let normalized = value.as_str()?.strip_prefix("0x")?.trim_start_matches('0');
    Some(if normalized.is_empty() {
        "0".into()
    } else {
        normalized.to_ascii_lowercase()
    })
}

fn proof_matches_request(args: &[Value], value: &Value) -> bool {
    let Some(expected_address) = args.first().and_then(Value::as_str) else {
        return false;
    };
    let Some(actual_address) = value.get("address").and_then(Value::as_str) else {
        return false;
    };
    if !expected_address.eq_ignore_ascii_case(actual_address) {
        return false;
    }
    let Some(expected_keys) = args.get(1).and_then(Value::as_array) else {
        return false;
    };
    let Some(actual_keys) = value.get("storageProof").and_then(Value::as_array) else {
        return false;
    };
    let expected = expected_keys
        .iter()
        .map(normalized_storage_key)
        .collect::<Option<std::collections::BTreeSet<_>>>();
    let actual = actual_keys
        .iter()
        .map(|proof| proof.get("key").and_then(normalized_storage_key))
        .collect::<Option<std::collections::BTreeSet<_>>>();
    matches!((expected, actual), (Some(expected), Some(actual)) if expected == actual)
}

fn read_only_passthrough(method: &str) -> bool {
    matches!(
        method,
        "eth_gasPrice"
            | "eth_maxPriorityFeePerGas"
            | "eth_feeHistory"
            | "eth_blobBaseFee"
            | "eth_getLogs"
            | "eth_getBlockByNumber"
            | "eth_getBlockByHash"
            | "eth_getBlockTransactionCountByNumber"
            | "eth_getBlockTransactionCountByHash"
            | "eth_getBlockReceipts"
            | "eth_getTransactionByHash"
            | "eth_getTransactionByBlockHashAndIndex"
            | "eth_getTransactionByBlockNumberAndIndex"
            | "eth_getTransactionReceipt"
            | "eth_getRawTransactionByHash"
            | "eth_getUncleCountByBlockHash"
            | "eth_getUncleCountByBlockNumber"
            | "eth_getUncleByBlockHashAndIndex"
            | "eth_getUncleByBlockNumberAndIndex"
            | "eth_syncing"
            | "net_peerCount"
            | "arb_getL1Confirmations"
            | "arb_findBatchContainingBlock"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    const ADDRESS: &str = "0x0000000000000000000000000000000000000064";

    struct MockState {
        chain: u64,
        latest: AtomicU64,
        safe: AtomicU64,
        finalized: AtomicU64,
        fork: AtomicU64,
        revert: AtomicBool,
        response_mode: AtomicU8,
        requests: Mutex<Vec<Value>>,
    }

    impl MockState {
        fn hash(&self, number: u64) -> String {
            format!(
                "0x{:064x}",
                number + self.fork.load(Ordering::Relaxed) * 1_000_000
            )
        }

        fn block(&self, number: u64, hash: Option<String>, full: bool) -> Value {
            json!({"number": format!("0x{number:x}"), "hash": hash.unwrap_or_else(|| self.hash(number)),
                "transactions": if full { json!([{"hash": format!("0x{:064x}", 900)}]) } else { json!([format!("0x{:064x}", 900)]) }})
        }

        fn answer(&self, request: &Value) -> Value {
            self.requests.lock().unwrap().push(request.clone());
            let method = request["method"].as_str().unwrap();
            let args = request["params"].as_array().unwrap();
            let result = match method {
                "eth_chainId" => json!(format!("0x{:x}", self.chain)),
                "eth_getBlockByNumber" => {
                    let number = match args[0].as_str().unwrap() {
                        "latest" => self.latest.load(Ordering::Relaxed),
                        "safe" => self.safe.load(Ordering::Relaxed),
                        "finalized" => self.finalized.load(Ordering::Relaxed),
                        "earliest" => 0,
                        _ => quantity_u64(&args[0]).unwrap(),
                    };
                    self.block(number, None, args[1] == true)
                }
                "eth_getBlockByHash" => {
                    let encoded = u64::from_str_radix(&args[0].as_str().unwrap()[2..], 16).unwrap();
                    self.block(
                        encoded % 1_000_000,
                        Some(args[0].as_str().unwrap().to_string()),
                        args[1] == true,
                    )
                }
                "eth_estimateGas" | "eth_createAccessList" => {
                    if self.revert.load(Ordering::Relaxed) {
                        return json!({"jsonrpc": "2.0", "id": request["id"], "error": {"code": 3, "message": "execution reverted", "data": "0xdeadbeef"}});
                    }
                    json!({"method": method, "params": args})
                }
                "eth_call"
                | "eth_getBalance"
                | "eth_getCode"
                | "eth_getTransactionCount"
                | "eth_getStorageAt"
                | "eth_getProof" => {
                    if self.revert.load(Ordering::Relaxed) {
                        return json!({"jsonrpc": "2.0", "id": request["id"], "error": {"code": 3, "message": "execution reverted", "data": "0xdeadbeef"}});
                    }
                    if self.response_mode.load(Ordering::Relaxed) == 1 {
                        Value::Null
                    } else if self.response_mode.load(Ordering::Relaxed) == 3 {
                        json!(format!("0x{}", "a".repeat(MAX_RESPONSE_BYTES)))
                    } else {
                        let index = cached_block_index(method).unwrap();
                        let selector = &args[index];
                        let encoded = if selector == "pending" {
                            12345
                        } else {
                            u64::from_str_radix(&selector["blockHash"].as_str().unwrap()[2..], 16)
                                .unwrap()
                        };
                        if selector.get("requireCanonical") == Some(&json!(true))
                            && selector["blockHash"] != self.hash(encoded % 1_000_000)
                        {
                            return json!({"jsonrpc":"2.0", "id":request["id"], "error":{"code":-32000,"message":"Requested block is not canonical"}});
                        }
                        match method {
                            "eth_getBalance" | "eth_getTransactionCount" => {
                                json!(format!("0x{encoded:x}"))
                            }
                            "eth_getProof" => {
                                let mode = self.response_mode.load(Ordering::Relaxed);
                                let address = if mode == 4 {
                                    json!("0x0000000000000000000000000000000000000001")
                                } else {
                                    args[0].clone()
                                };
                                let keys = if mode == 5 {
                                    json!(["0x2"])
                                } else {
                                    args[1].clone()
                                };
                                let proofs: Vec<_> = keys.as_array().unwrap().iter().map(|key| {
                                    json!({"key":format!("0x{:0>64}", &key.as_str().unwrap()[2..]), "value":"0x0", "proof":["0x01"]})
                                }).collect();
                                json!({"address": address, "balance": "0x0", "nonce": "0x0", "codeHash": format!("0x{:064x}", 0), "storageHash": format!("0x{:064x}", 0), "accountProof": ["0x01"], "storageProof": proofs})
                            }
                            _ => json!(format!("0x{encoded:064x}")),
                        }
                    }
                }
                _ => json!({"method": method, "params": args}),
            };
            json!({"jsonrpc":"2.0", "id": if self.response_mode.load(Ordering::Relaxed) == 2 { json!(0) } else { request["id"].clone() }, "result": result})
        }

        fn requests_for(&self, method: &str) -> Vec<Value> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request["method"] == method)
                .cloned()
                .collect()
        }
    }

    struct Mock {
        url: String,
        state: Arc<MockState>,
        task: JoinHandle<()>,
    }

    impl Mock {
        async fn start(chain: u64) -> Self {
            let state = Arc::new(MockState {
                chain,
                latest: AtomicU64::new(10),
                safe: AtomicU64::new(8),
                finalized: AtomicU64::new(7),
                fork: AtomicU64::new(0),
                revert: AtomicBool::new(false),
                response_mode: AtomicU8::new(0),
                requests: Mutex::new(Vec::new()),
            });
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let shared = state.clone();
            let task = tokio::spawn(async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    let state = shared.clone();
                    tokio::spawn(async move {
                        let mut bytes = Vec::new();
                        let body_start = loop {
                            let mut chunk = [0; 4096];
                            let count = socket.read(&mut chunk).await.unwrap();
                            if count == 0 {
                                return;
                            }
                            bytes.extend_from_slice(&chunk[..count]);
                            if let Some(end) =
                                bytes.windows(4).position(|window| window == b"\r\n\r\n")
                            {
                                break end + 4;
                            }
                        };
                        let headers = String::from_utf8_lossy(&bytes[..body_start]);
                        let length: usize = headers
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                            .unwrap()
                            .1
                            .trim()
                            .parse()
                            .unwrap();
                        while bytes.len() < body_start + length {
                            let mut chunk = [0; 4096];
                            let count = socket.read(&mut chunk).await.unwrap();
                            if count == 0 {
                                return;
                            }
                            bytes.extend_from_slice(&chunk[..count]);
                        }
                        let request: Value =
                            serde_json::from_slice(&bytes[body_start..body_start + length])
                                .unwrap();
                        let body = state.answer(&request).to_string();
                        let headers = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            body.len()
                        );
                        if socket.write_all(headers.as_bytes()).await.is_ok() {
                            let _ = socket.write_all(body.as_bytes()).await;
                        }
                    });
                }
            });
            Self { url, state, task }
        }

        async fn connect(&self) -> Backend {
            Backend::connect(&self.url, self.state.chain, 100, 1024 * 1024)
                .await
                .unwrap()
        }
    }

    impl Drop for Mock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn checks_chain_and_sanitizes_url_errors() {
        let mock = Mock::start(4663).await;
        let error = Backend::connect(&mock.url, 46630, 10, 1024)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("expected 46630, received 4663"));
        assert_eq!(mock.state.requests_for("eth_getBlockByNumber").len(), 0);
        for url in [
            "file:///tmp/secret",
            "http://user:secret@localhost/secret",
            "not a url",
        ] {
            let error = Backend::connect(url, 4663, 10, 1024)
                .await
                .err()
                .unwrap()
                .to_string();
            assert!(!error.contains("secret"));
        }
    }

    #[tokio::test]
    async fn caches_pinned_calls_and_keeps_overrides_and_batch_head() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        let params = json!([{"to":ADDRESS,"data":"0x1234"}, "latest", {ADDRESS:{"balance":"0x10"}}, {"time":"0x1"}]);
        let first = backend
            .dispatch("eth_call", params.clone(), &head)
            .await
            .unwrap();
        mock.state.latest.store(11, Ordering::Relaxed);
        assert_eq!(
            first,
            backend
                .dispatch("eth_call", params.clone(), &head)
                .await
                .unwrap()
        );
        assert_eq!(mock.state.requests_for("eth_call").len(), 1);
        let request = &mock.state.requests_for("eth_call")[0];
        assert_eq!(
            request["params"][1],
            json!({"blockHash":head.hash,"requireCanonical":true})
        );
        assert_eq!(request["params"][2], params[2]);
        assert_eq!(request["params"][3], params[3]);
        assert_eq!(
            backend
                .dispatch("eth_blockNumber", json!([]), &head)
                .await
                .unwrap(),
            "0xa"
        );
        for full in [false, true] {
            let block = backend
                .dispatch("eth_getBlockByNumber", json!(["latest", full]), &head)
                .await
                .unwrap();
            assert_eq!(block["hash"], head.hash);
            assert_eq!(block["transactions"][0].is_object(), full);
        }
        let next = backend.refresh_head().await.unwrap();
        assert_ne!(
            first,
            backend.dispatch("eth_call", params, &next).await.unwrap()
        );
        assert_eq!(backend.status()["cache"]["hits"], 1);
    }

    #[tokio::test]
    async fn safe_and_finalized_are_resolved_again_as_they_advance() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        for (tag, height) in [
            ("safe", &mock.state.safe),
            ("finalized", &mock.state.finalized),
        ] {
            let params = json!([ADDRESS, tag]);
            let first = backend
                .dispatch("eth_getBalance", params.clone(), &head)
                .await
                .unwrap();
            height.fetch_add(1, Ordering::Relaxed);
            let second = backend
                .dispatch("eth_getBalance", params, &head)
                .await
                .unwrap();
            assert_ne!(first, second);
            assert_eq!(
                mock.state
                    .requests_for("eth_getBlockByNumber")
                    .iter()
                    .filter(|request| request["params"][0] == tag)
                    .count(),
                2
            );
        }
    }

    #[tokio::test]
    async fn pending_calls_and_nonce_reads_bypass_the_cache() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        for (method, params) in [
            ("eth_call", json!([{"to":ADDRESS},"pending"])),
            ("eth_getTransactionCount", json!([ADDRESS, "pending"])),
        ] {
            for _ in 0..2 {
                backend
                    .dispatch(method, params.clone(), &head)
                    .await
                    .unwrap();
            }
            let requests = mock.state.requests_for(method);
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0]["params"], params);
        }
        assert_eq!(backend.status()["cache"]["entries"], 0);
    }

    #[tokio::test]
    async fn native_estimates_pin_snapshot_and_preserve_overrides_without_caching() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        let pinned_selector = json!({"blockHash":head.hash,"requireCanonical":true});
        backend
            .dispatch("eth_call", json!([{}]), &head)
            .await
            .unwrap();
        mock.state.latest.store(11, Ordering::Relaxed);
        for method in ["eth_estimateGas", "eth_createAccessList"] {
            for _ in 0..2 {
                backend
                    .dispatch(method, json!([{"to":ADDRESS}]), &head)
                    .await
                    .unwrap();
            }
            let overrides = json!({ADDRESS:{"balance":"0x10"}});
            backend
                .dispatch(method, json!([{"to":ADDRESS}, "latest", overrides]), &head)
                .await
                .unwrap();
            let requests = mock.state.requests_for(method);
            assert_eq!(requests.len(), 3);
            for request in &requests {
                assert_eq!(request["params"][1], pinned_selector);
            }
            assert_eq!(requests[2]["params"][2], overrides);
            let pending = json!([{"to":ADDRESS}, "pending", overrides]);
            backend
                .dispatch(method, pending.clone(), &head)
                .await
                .unwrap();
            assert_eq!(mock.state.requests_for(method)[3]["params"], pending);
        }
        // The only entry is the separately cached eth_call.
        assert_eq!(backend.status()["cache"]["entries"], 1);
        mock.state.revert.store(true, Ordering::Relaxed);
        for method in ["eth_estimateGas", "eth_createAccessList"] {
            let error = backend
                .dispatch(method, json!([{}]), &head)
                .await
                .unwrap_err();
            assert_eq!(error.code, 3);
            assert_eq!(error.message, "execution reverted");
            assert_eq!(error.data, Some(json!("0xdeadbeef")));
        }
    }

    #[tokio::test]
    async fn native_estimate_params_are_checked_and_listening_is_local() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        for method in ["eth_estimateGas", "eth_createAccessList"] {
            for params in [
                json!([]),
                json!(["bad"]),
                json!([{}, "latest", []]),
                json!([{}, "latest", {}, {}]),
            ] {
                assert_eq!(
                    backend
                        .dispatch(method, params, &head)
                        .await
                        .unwrap_err()
                        .code,
                    -32602
                );
            }
            assert!(mock.state.requests_for(method).is_empty());
        }
        assert_eq!(
            backend
                .dispatch("net_listening", json!([]), &head)
                .await
                .unwrap(),
            true
        );
        assert_eq!(
            backend
                .dispatch("net_listening", json!([true]), &head)
                .await
                .unwrap_err()
                .code,
            -32602
        );
        assert!(mock.state.requests_for("net_listening").is_empty());
    }

    #[tokio::test]
    async fn upstream_reverts_keep_error_data_and_are_never_cached() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        mock.state.revert.store(true, Ordering::Relaxed);
        for _ in 0..2 {
            let error = backend
                .dispatch("eth_call", json!([{}]), &head)
                .await
                .unwrap_err();
            assert_eq!(error.code, 3);
            assert_eq!(error.message, "execution reverted");
            assert_eq!(error.data, Some(json!("0xdeadbeef")));
        }
        assert_eq!(mock.state.requests_for("eth_call").len(), 2);
        assert_eq!(backend.status()["cache"]["entries"], 0);
    }

    #[tokio::test]
    async fn reorgs_isolate_hashes_and_reject_canonical_orphan_cache_hits() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let old = backend.head().unwrap();
        let params = json!([{}]);
        let first = backend
            .dispatch("eth_call", params.clone(), &old)
            .await
            .unwrap();
        mock.state.fork.store(1, Ordering::Relaxed);
        assert_eq!(
            backend
                .dispatch("eth_call", params.clone(), &old)
                .await
                .unwrap_err()
                .code,
            -32000
        );
        let current = backend.refresh_head().await.unwrap();
        assert_eq!(old.number, current.number);
        assert_ne!(old.hash, current.hash);
        assert_ne!(
            first,
            backend
                .dispatch("eth_call", params, &current)
                .await
                .unwrap()
        );
        let orphan_params = json!([{}, {"blockHash": old.hash, "requireCanonical":false}]);
        let orphan = backend
            .dispatch("eth_call", orphan_params.clone(), &current)
            .await
            .unwrap();
        assert_eq!(first, orphan);
        assert_eq!(
            orphan,
            backend
                .dispatch("eth_call", orphan_params, &current)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn rejects_malformed_results_envelopes_and_oversized_responses() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        for mode in [1, 2, 3] {
            mock.state.response_mode.store(mode, Ordering::Relaxed);
            let error = backend
                .dispatch("eth_call", json!([{}]), &head)
                .await
                .unwrap_err();
            assert_eq!(error.code, -32098);
        }
        assert_eq!(backend.status()["cache"]["entries"], 0);
    }

    #[tokio::test]
    async fn bounds_cache_entries_and_bytes() {
        let mock = Mock::start(4663).await;
        let backend = Backend::connect(&mock.url, 4663, 1, 1024).await.unwrap();
        let head = backend.head().unwrap();
        for data in ["0x01", "0x02", "0x01"] {
            backend
                .dispatch("eth_call", json!([{"data":data}]), &head)
                .await
                .unwrap();
        }
        assert_eq!(mock.state.requests_for("eth_call").len(), 3);
        assert_eq!(backend.status()["cache"]["entries"], 1);
        assert!(backend.status()["cache"]["bytes"].as_u64().unwrap() <= 1024);
        let disabled = Backend::connect(&mock.url, 4663, 10, 1).await.unwrap();
        disabled
            .dispatch("eth_call", json!([{}]), &head)
            .await
            .unwrap();
        assert_eq!(disabled.status()["cache"]["entries"], 0);
    }

    #[tokio::test]
    async fn validates_params_supports_proof_reads_and_blocks_writes() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        for (method, params) in [
            ("eth_call", json!(["bad"])),
            ("eth_getBalance", json!(["0x12"])),
            ("eth_getStorageAt", json!([ADDRESS, "0x00"])),
            ("eth_getProof", json!([ADDRESS, "bad"])),
            (
                "eth_getBalance",
                json!([ADDRESS, {"blockHash":head.hash,"requireCanonical":"yes"}]),
            ),
        ] {
            assert_eq!(
                backend
                    .dispatch(method, params, &head)
                    .await
                    .unwrap_err()
                    .code,
                -32602
            );
        }
        for method in [
            "eth_sendRawTransaction",
            "eth_sendTransaction",
            "personal_unlockAccount",
            "admin_peers",
            "debug_traceCall",
            "eth_newFilter",
        ] {
            assert_eq!(
                backend
                    .dispatch(method, json!([]), &head)
                    .await
                    .unwrap_err()
                    .code,
                -32601
            );
            assert!(mock.state.requests_for(method).is_empty());
        }
        for _ in 0..2 {
            let proof = backend
                .dispatch("eth_getProof", json!([ADDRESS, []]), &head)
                .await
                .unwrap();
            assert_eq!(proof["address"], ADDRESS);
        }
        assert_eq!(mock.state.requests_for("eth_getProof").len(), 1);
    }

    #[tokio::test]
    async fn proof_results_must_match_requested_address_and_storage_keys() {
        let mock = Mock::start(4663).await;
        let backend = mock.connect().await;
        let head = backend.head().unwrap();
        let params = json!([ADDRESS, ["0x0", "0x1"]]);
        for mode in [4, 5] {
            mock.state.response_mode.store(mode, Ordering::Relaxed);
            assert_eq!(
                backend
                    .dispatch("eth_getProof", params.clone(), &head)
                    .await
                    .unwrap_err()
                    .code,
                -32098
            );
            assert_eq!(backend.status()["cache"]["entries"], 0);
        }
        mock.state.response_mode.store(0, Ordering::Relaxed);
        // Nitro can return full-width keys for the requested quantity keys.
        let proof = backend
            .dispatch("eth_getProof", params.clone(), &head)
            .await
            .unwrap();
        assert_eq!(proof["storageProof"][0]["key"], format!("0x{:064x}", 0));
        backend
            .dispatch("eth_getProof", params, &head)
            .await
            .unwrap();
        assert_eq!(mock.state.requests_for("eth_getProof").len(), 3);
        assert_eq!(backend.status()["cache"]["entries"], 1);
        assert_eq!(backend.status()["proofsVerified"], false);
    }
}
