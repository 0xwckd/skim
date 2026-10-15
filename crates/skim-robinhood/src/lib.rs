//! Robinhood Chain uses Nitro, not Ethereum's SNAP/BAL protocols. This backend caches
//! explicitly pinned RPC reads and executes calls on Nitro. It does not verify proofs,
//! execute locally, follow consensus, or maintain a complete per-block state archive.

pub mod backend;
pub mod watch;

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};

use backend::{Backend, Head, RpcError};
use watch::{Config, Watchlist};

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    pub fn chain_id(self) -> u64 {
        match self {
            Self::Mainnet => 4663,
            Self::Testnet => 46630,
        }
    }

    pub fn upstream(self) -> &'static str {
        match self {
            Self::Mainnet => "https://rpc.mainnet.chain.robinhood.com",
            Self::Testnet => "https://rpc.testnet.chain.robinhood.com",
        }
    }

    fn from_config(name: &str) -> anyhow::Result<Self> {
        match name {
            "robinhood-mainnet" | "mainnet" => Ok(Self::Mainnet),
            "robinhood-testnet" | "testnet" => Ok(Self::Testnet),
            _ => anyhow::bail!(
                "unsupported network {name:?}; use robinhood-mainnet or robinhood-testnet"
            ),
        }
    }
}

#[derive(clap::Args)]
pub struct Args {
    /// Robinhood network. Defaults to mainnet; overrides the config file.
    #[arg(long, value_enum)]
    pub network: Option<Network>,
    /// Nitro HTTP JSON-RPC endpoint. Defaults to the network's public RPC.
    #[arg(long)]
    pub upstream: Option<String>,
    /// Watchlist YAML. See examples/robinhood-chain/.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Local HTTP JSON-RPC listen address.
    #[arg(long, default_value = "127.0.0.1:8545")]
    pub rpc: SocketAddr,
    /// Poll and warm watchlist at this interval; intervening blocks can be skipped.
    #[arg(long, default_value_t = 1000)]
    pub poll_ms: u64,
    /// Maximum cached immutable reads (oldest entries are evicted).
    #[arg(long, default_value_t = 10000)]
    pub cache_entries: usize,
    /// Maximum approximate encoded cache size in bytes.
    #[arg(long, default_value_t = 67108864)]
    pub cache_bytes: usize,
}

struct Server {
    backend: Backend,
    watchlist: Watchlist,
}

const MAX_BATCH_REPLY_BYTES: usize = 16 * 1024 * 1024;

pub async fn run(args: Args) -> anyhow::Result<()> {
    ensure!(
        (100..=60_000).contains(&args.poll_ms),
        "poll-ms must be between 100 and 60000"
    );
    ensure!(
        args.cache_entries > 0 && args.cache_bytes > 0,
        "cache bounds must be nonzero"
    );
    let config: Config = match args.config {
        Some(path) => serde_yaml_ng::from_str(
            &std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?,
        )
        .with_context(|| format!("parse {}", path.display()))?,
        None => Config::default(),
    };
    let network = match args.network {
        Some(network) => network,
        None => config
            .network
            .as_deref()
            .map(Network::from_config)
            .transpose()?
            .unwrap_or(Network::Mainnet),
    };
    let upstream = args
        .upstream
        .or_else(|| config.upstream.clone())
        .unwrap_or_else(|| network.upstream().into());
    let watchlist = Watchlist::new(config)?;
    let backend = Backend::connect(
        &upstream,
        network.chain_id(),
        args.cache_entries,
        args.cache_bytes,
    )
    .await?;
    let head = backend.head().context("initial head unavailable")?;
    let server = Arc::new(Server { backend, watchlist });
    server.watchlist.warm(&server.backend, &head).await;
    let listener = tokio::net::TcpListener::bind(args.rpc)
        .await
        .context("bind Robinhood JSON-RPC listener")?;
    tracing::info!(chain_id = network.chain_id(), block = head.number, address = %listener.local_addr()?,
        "Robinhood RPC cache ready; calls execute on upstream Nitro; state and consensus are trusted");
    let poller = {
        let server = server.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(args.poll_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if server.watchlist.is_empty() {
                    continue;
                }
                match server.backend.refresh_head().await {
                    Ok(head) => server.watchlist.warm(&server.backend, &head).await,
                    Err(error) => {
                        tracing::warn!(code = error.code, "Robinhood watchlist head refresh failed")
                    }
                }
            }
        })
    };
    let result = axum::serve(listener, router(server))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    poller.abort();
    result.context("serve Robinhood JSON-RPC")
}

fn router(server: Arc<Server>) -> Router {
    Router::new()
        .route("/", post(handle))
        .route("/health", get(health))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(server)
}

async fn health(State(server): State<Arc<Server>>) -> Response {
    match server.backend.refresh_head().await {
        Ok(head) => (StatusCode::OK, axum::Json(json!({"ok":true,"chainId":server.backend.status()["chainId"],"blockNumber":format!("{:#x}",head.number),"blockHash":head.hash}))).into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({"ok":false,"error":"upstream head unavailable"}))).into_response(),
    }
}

fn error(id: Value, code: i64, message: impl Into<String>, data: Option<Value>) -> Value {
    let mut error = json!({"code":code,"message":message.into()});
    if let Some(data) = data {
        error["data"] = data;
    }
    json!({"jsonrpc":"2.0","id":id,"error":error})
}

fn valid_request(value: &Value) -> bool {
    value.is_object()
        && value.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
        && value.get("method").and_then(Value::as_str).is_some()
        && value
            .get("id")
            .is_none_or(|id| id.is_null() || id.is_string() || id.is_number())
        && value.get("params").is_none_or(Value::is_array)
}

fn local_method(method: &str) -> bool {
    matches!(
        method,
        "skim_status"
            | "skim_subscriptionStatus"
            | "web3_clientVersion"
            | "eth_chainId"
            | "net_version"
            | "net_listening"
    )
}

async fn execute(server: &Server, request: &Value, head: &Result<Head, RpcError>) -> Option<Value> {
    if !valid_request(request) {
        return Some(error(
            Value::Null,
            -32600,
            "invalid JSON-RPC 2.0 request (positional params required)",
            None,
        ));
    }
    let method = request["method"].as_str().expect("validated method");
    let params = request.get("params").cloned().unwrap_or_else(|| json!([]));
    let result = if matches!(method, "skim_status" | "skim_subscriptionStatus")
        && !params.as_array().is_some_and(Vec::is_empty)
    {
        Err(RpcError {
            code: -32602,
            message: "This method takes no parameters".into(),
            data: None,
        })
    } else if method == "skim_status" {
        let mut status = server.backend.status();
        status["subscriptions"] = server.watchlist.status();
        status["mode"] = json!("robinhood-nitro-rpc-cache");
        status["upstreamRequired"] = json!(true);
        status["localExecution"] = json!(false);
        status["proofsVerified"] = json!(false);
        status["consensusVerified"] = json!(false);
        status["watchlistMode"] = json!("sampled-head-refresh");
        Ok(status)
    } else if method == "skim_subscriptionStatus" {
        Ok(server.watchlist.status())
    } else {
        // Local identity remains available during an upstream outage in a mixed batch.
        let fallback = server.backend.head();
        let selected_head = if local_method(method) {
            fallback.as_ref().ok_or_else(|| RpcError {
                code: -32002,
                message: "head unavailable".into(),
                data: None,
            })
        } else {
            head.as_ref().map_err(Clone::clone)
        };
        match selected_head {
            Ok(head) => server.backend.dispatch(method, params, head).await,
            Err(e) => Err(e),
        }
    };
    request.get("id").map(|id| match result {
        Ok(value) => json!({"jsonrpc":"2.0","id":id,"result":value}),
        Err(e) => error(id.clone(), e.code, e.message, e.data),
    })
}

async fn handle(State(server): State<Arc<Server>>, body: Bytes) -> Response {
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return axum::Json(error(Value::Null, -32700, "parse error", None)).into_response();
        }
    };
    let is_batch = payload.is_array();
    let requests = match &payload {
        Value::Array(requests) if requests.is_empty() || requests.len() > 100 => {
            return axum::Json(error(
                Value::Null,
                -32600,
                "batch must contain 1 to 100 requests",
                None,
            ))
            .into_response();
        }
        Value::Array(requests) => requests.as_slice(),
        _ => std::slice::from_ref(&payload),
    };
    // Capture one live head for the entire batch. A failed refresh fails reads instead of
    // returning old cached state under the moving `latest` tag.
    let needs_refresh = requests
        .iter()
        .any(|r| valid_request(r) && !local_method(r["method"].as_str().unwrap()));
    let head = if needs_refresh {
        server.backend.refresh_head().await
    } else {
        server.backend.head().ok_or_else(|| RpcError {
            code: -32002,
            message: "head unavailable".into(),
            data: None,
        })
    };
    let mut replies = Vec::new();
    let mut reply_bytes = 0usize;
    for request in requests {
        if let Some(mut reply) = execute(&server, request, &head).await {
            let bytes = reply.to_string().len();
            if reply_bytes.saturating_add(bytes) > MAX_BATCH_REPLY_BYTES {
                reply = error(
                    request.get("id").cloned().unwrap_or(Value::Null),
                    -32005,
                    "batch response exceeds 16 MiB; request fewer or smaller reads",
                    None,
                );
            }
            reply_bytes = reply_bytes.saturating_add(reply.to_string().len());
            replies.push(reply);
        }
    }
    if replies.is_empty() {
        return StatusCode::NO_CONTENT.into_response();
    }
    axum::Json(if is_batch {
        Value::Array(replies)
    } else {
        replies.remove(0)
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mock_block(number: u64) -> Value {
        json!({"number":format!("{number:#x}"),"hash":format!("0x{number:064x}"),"transactions":[]})
    }

    async fn mock_rpc(
        State(next): State<Arc<AtomicU64>>,
        axum::Json(request): axum::Json<Value>,
    ) -> axum::Json<Value> {
        let params = &request["params"];
        let result = match request["method"].as_str().unwrap() {
            "eth_chainId" => json!("0x1237"),
            "eth_getBlockByNumber" => {
                let number = if params[0] == "latest" {
                    next.fetch_add(1, Ordering::Relaxed)
                } else {
                    u64::from_str_radix(params[0].as_str().unwrap().trim_start_matches("0x"), 16)
                        .unwrap()
                };
                mock_block(number)
            }
            "eth_getBlockByHash" => {
                let hash = params[0].as_str().unwrap().trim_start_matches("0x");
                mock_block(u64::from_str_radix(hash.trim_start_matches('0'), 16).unwrap())
            }
            "eth_call" => {
                let hash = params[1]["blockHash"].as_str().expect("hash-pinned call");
                json!(hash)
            }
            "eth_getCode" => json!(format!("0x{}", "ab".repeat(3 * 1024 * 1024))),
            other => panic!("unexpected mock method {other}"),
        };
        axum::Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
    }

    async fn test_server() -> (
        String,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_url = format!("http://{}", upstream_listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move {
            axum::serve(
                upstream_listener,
                Router::new()
                    .route("/", post(mock_rpc))
                    .with_state(Arc::new(AtomicU64::new(1))),
            )
            .await
            .unwrap();
        });
        let backend = Backend::connect(&upstream_url, 4663, 100, 1024 * 1024)
            .await
            .unwrap();
        let server = Arc::new(Server {
            backend,
            watchlist: Watchlist::new(Config::default()).unwrap(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let local = tokio::spawn(async move {
            axum::serve(listener, router(server)).await.unwrap();
        });
        (url, local, upstream)
    }

    #[tokio::test]
    async fn http_batch_pins_one_head_and_keeps_identity_available_during_outage() {
        let (url, local, upstream) = test_server().await;
        let client = reqwest::Client::new();
        let batch = json!([
            {"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
            {"jsonrpc":"2.0","id":2,"method":"eth_getBlockByNumber","params":["latest",false]},
            {"jsonrpc":"2.0","id":3,"method":"eth_call","params":[{},"latest"]},
            {"jsonrpc":"2.0","id":4,"method":"eth_chainId","params":[]}
        ]);
        let response: Value = client
            .post(&url)
            .json(&batch)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response[0]["result"], "0x2");
        assert_eq!(response[1]["result"]["number"], "0x2");
        assert_eq!(response[2]["result"], format!("0x{:064x}", 2));
        assert_eq!(response[3]["result"], "0x1237");
        upstream.abort();
        let _ = upstream.await;
        let offline: Value = client
            .post(&url)
            .json(&batch)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(offline[0].get("error").is_some());
        assert!(offline[2].get("error").is_some());
        assert_eq!(offline[3]["result"], "0x1237");
        assert_eq!(
            client
                .get(format!("{url}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        local.abort();
    }

    #[tokio::test]
    async fn http_handles_invalid_batches_parse_errors_and_notifications() {
        let (url, local, upstream) = test_server().await;
        let client = reqwest::Client::new();
        let malformed: Value = client
            .post(&url)
            .body("{")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(malformed["error"]["code"], -32700);
        let empty: Value = client
            .post(&url)
            .json(&json!([]))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(empty["error"]["code"], -32600);
        let notification = json!({"jsonrpc":"2.0","method":"eth_chainId","params":[]});
        assert_eq!(
            client
                .post(&url)
                .json(&notification)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        let mixed: Value = client
            .post(&url)
            .json(&json!([
                notification,
                {"jsonrpc":"2.0","id":"status","method":"skim_status","params":[]},
                {"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]},
                {"jsonrpc":"2.0","id":2,"method":"skim_subscriptionStatus","params":["wrong"]}
            ]))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(mixed.as_array().unwrap().len(), 3);
        assert_eq!(mixed[0]["result"]["localExecution"], false);
        assert_eq!(mixed[0]["result"]["proofsVerified"], false);
        assert_eq!(mixed[1]["error"]["code"], -32601);
        assert_eq!(mixed[2]["error"]["code"], -32602);
        local.abort();
        upstream.abort();
    }

    #[tokio::test]
    async fn http_bounds_aggregate_batch_replies() {
        let (url, local, upstream) = test_server().await;
        let address = "0x0000000000000000000000000000000000000001";
        let requests: Vec<_> = (1..=3).map(|id| json!({"jsonrpc":"2.0","id":id,"method":"eth_getCode","params":[address,"latest"]})).collect();
        let response: Value = reqwest::Client::new()
            .post(&url)
            .json(&requests)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(response[0].get("result").is_some());
        assert!(response[1].get("result").is_some());
        assert_eq!(response[2]["error"]["code"], -32005);
        assert!(response.to_string().len() < MAX_BATCH_REPLY_BYTES);
        local.abort();
        upstream.abort();
    }

    #[test]
    fn validates_json_rpc_envelopes() {
        assert!(valid_request(
            &json!({"jsonrpc":"2.0","method":"eth_chainId","id":1})
        ));
        assert!(valid_request(
            &json!({"jsonrpc":"2.0","method":"eth_chainId"})
        ));
        for request in [
            json!(null),
            json!([]),
            json!({"method":"eth_call"}),
            json!({"jsonrpc":"2.0","method":"eth_call","params":{}}),
            json!({"jsonrpc":"2.0","method":"eth_call","id":true}),
        ] {
            assert!(!valid_request(&request));
        }
    }

    #[test]
    fn configured_networks_are_distinct() {
        assert_eq!(Network::Mainnet.chain_id(), 4663);
        assert_eq!(Network::Testnet.chain_id(), 46630);
        assert!(Network::from_config("sepolia").is_err());
        assert_ne!(Network::Mainnet.upstream(), Network::Testnet.upstream());
    }
}
