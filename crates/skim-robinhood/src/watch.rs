//! Sampled RPC watchlists for Robinhood Chain.
//!
//! Queries are fetched again at each selected head and pinned to that head by the backend.
//! This does not consume block access lists, learn state access, or retain an every-block archive;
//! a polling interval can skip intermediate blocks. Readiness applies only to the reported block.

use std::{collections::HashSet, io::Write, sync::RwLock};

use anyhow::{Context, Result, bail, ensure};
use futures::{StreamExt, stream};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::backend::{Backend, Head, RpcError};

/// Bounds the amount of upstream work one selected block can trigger.
const MAX_READ_QUERIES: usize = 1_000;
const MAX_QUERY_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_SNAPSHOT_BYTES: usize = 8 * 1024 * 1024;
const WARM_CONCURRENCY: usize = 16;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub network: Option<String>,
    pub upstream: Option<String>,
    #[serde(default)]
    pub state: StateConfig,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub name: String,
    pub address: Option<String>,
    #[serde(default)]
    pub account: bool,
    #[serde(default, deserialize_with = "deserialize_storage")]
    pub storage: Vec<String>,
    #[serde(default)]
    pub calls: Vec<Value>,
    #[serde(default = "enabled")]
    pub on_block: bool,
}

fn enabled() -> bool {
    true
}

fn deserialize_storage<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Array(slots) => slots
            .into_iter()
            .map(|slot| match slot {
                Value::String(slot) => Ok(slot),
                _ => Err(serde::de::Error::custom(
                    "storage slots must be quoted hexadecimal strings",
                )),
            })
            .collect(),
        Value::String(value) if value == "all" => Err(serde::de::Error::custom(
            "storage: all is unavailable in sampled RPC mode; list explicit hexadecimal slots",
        )),
        _ => Err(serde::de::Error::custom(
            "storage must be a list of explicit hexadecimal slots",
        )),
    }
}

struct Snapshot {
    head_hash: Option<String>,
    subscriptions: Vec<Value>,
}

pub struct Watchlist {
    subscriptions: Vec<Subscription>,
    snapshot: RwLock<Snapshot>,
    /// Serialize warming passes without holding the snapshot lock across network requests.
    warming: Mutex<()>,
}

#[derive(Clone, Debug)]
struct Query {
    method: &'static str,
    params: Value,
}

struct CompletedQuery {
    index: usize,
    query: Query,
    response: Result<Value, RpcError>,
}

struct SnapshotBudget {
    used: usize,
    reserved_errors: usize,
    limit: usize,
}

impl SnapshotBudget {
    /// Reserve enough space to retain a useful error for every unfinished query, even if an
    /// earlier query consumes nearly the entire result budget.
    fn admit(&mut self, mut completed: CompletedQuery) -> CompletedQuery {
        let error_size = entry_size(
            completed.index,
            &completed.query,
            &Err(snapshot_limit_error()),
        ) + 1;
        self.reserved_errors = self.reserved_errors.saturating_sub(error_size);
        let result_size = entry_size(completed.index, &completed.query, &completed.response) + 1;
        if self
            .used
            .saturating_add(self.reserved_errors)
            .saturating_add(result_size)
            > self.limit
        {
            completed.response = Err(snapshot_limit_error());
            self.used += error_size;
        } else {
            self.used += result_size;
        }
        completed
    }
}

fn snapshot_limit_error() -> RpcError {
    RpcError {
        code: -32005,
        message: "watchlist snapshot exceeds the 8 MiB size limit".into(),
        data: None,
    }
}

/// Count encoded JSON without allocating another copy of a possibly large result.
fn encoded_size<T: Serialize + ?Sized>(value: &T) -> usize {
    #[derive(Default)]
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter::default();
    if serde_json::to_writer(&mut counter, value).is_err() {
        usize::MAX
    } else {
        counter.0
    }
}

fn entry_size(index: usize, query: &Query, response: &Result<Value, RpcError>) -> usize {
    #[derive(Serialize)]
    struct Success<'a> {
        index: usize,
        method: &'a str,
        params: &'a Value,
        result: &'a Value,
    }
    #[derive(Serialize)]
    struct Failure<'a> {
        index: usize,
        method: &'a str,
        params: &'a Value,
        code: i64,
        message: &'a str,
        data: &'a Option<Value>,
    }
    match response {
        Ok(result) => encoded_size(&Success {
            index,
            method: query.method,
            params: &query.params,
            result,
        }),
        Err(error) => encoded_size(&Failure {
            index,
            method: query.method,
            params: &query.params,
            code: error.code,
            message: &error.message,
            data: &error.data,
        }),
    }
}

impl Watchlist {
    pub fn new(mut config: Config) -> Result<Self> {
        ensure!(
            config.state.subscriptions.len() <= MAX_READ_QUERIES,
            "watchlist allows at most {MAX_READ_QUERIES} subscriptions"
        );
        ensure!(
            encoded_size(&config.state) <= MAX_QUERY_CONFIG_BYTES,
            "watchlist query configuration exceeds the 1 MiB size limit"
        );
        let mut names = HashSet::new();
        let mut query_count = 0usize;
        for subscription in &mut config.state.subscriptions {
            ensure!(
                !subscription.name.trim().is_empty(),
                "subscription name must not be empty"
            );
            ensure!(
                names.insert(subscription.name.clone()),
                "duplicate subscription name {:?}",
                subscription.name
            );
            validate_subscription(subscription)
                .with_context(|| format!("subscription {:?}", subscription.name))?;
            query_count += queries(subscription).len();
            ensure!(
                query_count <= MAX_READ_QUERIES,
                "watchlist has {query_count} read queries; sampled RPC mode allows at most {MAX_READ_QUERIES}",
            );
        }
        let subscriptions = config.state.subscriptions;
        let snapshot = Snapshot {
            head_hash: None,
            subscriptions: subscriptions
                .iter()
                .map(|subscription| subscription_status(subscription, None, Vec::new()))
                .collect(),
        };
        Ok(Self {
            subscriptions,
            snapshot: RwLock::new(snapshot),
            warming: Mutex::new(()),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    /// Refresh active reads at each selected head, retrying incomplete snapshots at the same hash.
    /// Errors are retained beside successful reads; readers see the preceding complete snapshot
    /// until this pass finishes. A successful snapshot is skipped until the head hash changes.
    pub async fn warm(&self, backend: &Backend, head: &Head) {
        let _warming = self.warming.lock().await;
        {
            let snapshot = self
                .snapshot
                .read()
                .unwrap_or_else(|error| error.into_inner());
            if snapshot.head_hash.as_deref() == Some(head.hash.as_str())
                && snapshot
                    .subscriptions
                    .iter()
                    .all(|subscription| subscription["ready"] == true)
            {
                return;
            }
        }

        let work: Vec<_> = self
            .subscriptions
            .iter()
            .enumerate()
            .flat_map(|(subscription_index, subscription)| {
                queries(subscription)
                    .into_iter()
                    .enumerate()
                    .map(move |(query_index, query)| (subscription_index, query_index, query))
            })
            .collect();
        let initial_status: Vec<_> = self
            .subscriptions
            .iter()
            .map(|subscription| subscription_status(subscription, Some(head), Vec::new()))
            .collect();
        let reserved_errors = work
            .iter()
            .map(|(_, index, query)| entry_size(*index, query, &Err(snapshot_limit_error())) + 1)
            .sum();
        let mut budget = SnapshotBudget {
            used: encoded_size(&initial_status),
            reserved_errors,
            limit: MAX_SNAPSHOT_BYTES,
        };
        let mut completed = stream::iter(work.into_iter().map(
            |(subscription_index, query_index, query)| async move {
                let response = backend
                    .dispatch(query.method, query.params.clone(), head)
                    .await;
                (
                    subscription_index,
                    CompletedQuery {
                        index: query_index,
                        query,
                        response,
                    },
                )
            },
        ))
        .buffer_unordered(WARM_CONCURRENCY);
        let mut by_subscription: Vec<Vec<CompletedQuery>> =
            (0..self.subscriptions.len()).map(|_| Vec::new()).collect();
        while let Some((subscription_index, query)) = completed.next().await {
            by_subscription[subscription_index].push(budget.admit(query));
        }
        for completed in &mut by_subscription {
            completed.sort_by_key(|query| query.index);
        }
        let subscriptions = self
            .subscriptions
            .iter()
            .zip(by_subscription)
            .map(|(subscription, completed)| {
                subscription_status(subscription, Some(head), completed)
            })
            .collect();
        *self
            .snapshot
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Snapshot {
            head_hash: Some(head.hash.clone()),
            subscriptions,
        };
    }

    pub fn status(&self) -> Value {
        Value::Array(
            self.snapshot
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .subscriptions
                .clone(),
        )
    }
}

fn validate_subscription(subscription: &mut Subscription) -> Result<()> {
    if let Some(address) = &mut subscription.address {
        validate_address(address)?;
        address.make_ascii_lowercase();
    }
    ensure!(
        !(subscription.account || !subscription.storage.is_empty())
            || subscription.address.is_some(),
        "account and storage reads require an address",
    );
    for slot in &mut subscription.storage {
        validate_quantity(slot).with_context(|| format!("invalid storage slot {slot:?}"))?;
        *slot = normalize_quantity(slot);
    }
    for (index, call) in subscription.calls.iter_mut().enumerate() {
        validate_call(call).with_context(|| format!("invalid call {index}"))?;
    }
    Ok(())
}

fn validate_address(address: &str) -> Result<()> {
    ensure!(
        hex_body(address).is_some_and(|body| body.len() == 40),
        "address must be 0x followed by exactly 40 hexadecimal digits"
    );
    Ok(())
}

fn hex_body(value: &str) -> Option<&str> {
    value
        .strip_prefix("0x")
        .filter(|body| body.as_bytes().iter().all(u8::is_ascii_hexdigit))
}

fn validate_quantity(value: &str) -> Result<()> {
    ensure!(
        hex_body(value).is_some_and(|body| !body.is_empty() && body.len() <= 64),
        "expected 0x followed by 1 to 64 hexadecimal digits",
    );
    Ok(())
}

fn normalize_quantity(value: &str) -> String {
    let body = value[2..].trim_start_matches('0');
    if body.is_empty() {
        "0x0".into()
    } else {
        format!("0x{}", body.to_ascii_lowercase())
    }
}

fn validate_bytes(value: &str, length: Option<usize>) -> Result<()> {
    ensure!(
        hex_body(value).is_some_and(
            |body| body.len() % 2 == 0 && length.is_none_or(|length| body.len() == length * 2)
        ),
        "expected 0x-prefixed hexadecimal bytes{}",
        length
            .map(|length| format!(" of length {length}"))
            .unwrap_or_default(),
    );
    Ok(())
}

fn string(value: &Value) -> Result<&str> {
    value
        .as_str()
        .context("expected a quoted hexadecimal string")
}

/// Keep calls as JSON transaction objects so ordinary RPC transaction fields survive unchanged,
/// while refusing silently ignored misspellings and unsupported state/block override envelopes.
fn validate_call(call: &mut Value) -> Result<()> {
    let fields = call
        .as_object_mut()
        .context("call must be a transaction object")?;
    for (name, value) in fields.iter_mut() {
        match name.as_str() {
            "from" | "to" => {
                let address = string(value)?;
                validate_address(address).with_context(|| name.clone())?;
                *value = Value::String(address.to_ascii_lowercase());
            }
            "data" | "input" => {
                validate_bytes(string(value)?, None).with_context(|| name.clone())?;
            }
            "value"
            | "gas"
            | "gasPrice"
            | "maxFeePerGas"
            | "maxPriorityFeePerGas"
            | "maxFeePerBlobGas"
            | "nonce"
            | "chainId"
            | "type" => {
                let quantity = string(value)?;
                validate_quantity(quantity).with_context(|| name.clone())?;
                *value = Value::String(normalize_quantity(quantity));
            }
            "accessList" => validate_access_list(value)?,
            "blobVersionedHashes" => {
                for hash in value
                    .as_array()
                    .context("blobVersionedHashes must be a list")?
                {
                    validate_bytes(string(hash)?, Some(32)).context("blobVersionedHashes")?;
                }
            }
            "authorizationList" => validate_authorizations(value)?,
            _ => bail!("unsupported transaction field {name:?}"),
        }
    }
    if let (Some(data), Some(input)) = (fields.get("data"), fields.get("input")) {
        ensure!(
            string(data)?.eq_ignore_ascii_case(string(input)?),
            "data and input must match when both are supplied"
        );
    }
    ensure!(
        !(fields.contains_key("gasPrice")
            && (fields.contains_key("maxFeePerGas")
                || fields.contains_key("maxPriorityFeePerGas"))),
        "gasPrice cannot be combined with maxFeePerGas or maxPriorityFeePerGas",
    );
    Ok(())
}

fn validate_access_list(value: &Value) -> Result<()> {
    for entry in value.as_array().context("accessList must be a list")? {
        let fields = entry
            .as_object()
            .context("accessList entries must be objects")?;
        ensure!(
            fields.len() == 2
                && fields.contains_key("address")
                && fields.contains_key("storageKeys"),
            "accessList entries require only address and storageKeys"
        );
        validate_address(string(&fields["address"])?)?;
        for slot in fields["storageKeys"]
            .as_array()
            .context("accessList storageKeys must be a list")?
        {
            validate_bytes(string(slot)?, Some(32)).context("accessList storageKeys")?;
        }
    }
    Ok(())
}

fn validate_authorizations(value: &Value) -> Result<()> {
    const FIELDS: [&str; 6] = ["chainId", "address", "nonce", "yParity", "r", "s"];
    for authorization in value
        .as_array()
        .context("authorizationList must be a list")?
    {
        let fields = authorization
            .as_object()
            .context("authorizationList entries must be objects")?;
        ensure!(
            fields.len() == FIELDS.len() && FIELDS.iter().all(|field| fields.contains_key(*field)),
            "authorizationList entries require only chainId, address, nonce, yParity, r and s"
        );
        validate_address(string(&fields["address"])?)?;
        for field in ["chainId", "nonce", "yParity", "r", "s"] {
            validate_quantity(string(&fields[field])?)
                .with_context(|| format!("authorizationList {field}"))?;
        }
        ensure!(
            matches!(
                normalize_quantity(string(&fields["yParity"])?).as_str(),
                "0x0" | "0x1"
            ),
            "authorizationList yParity must be 0x0 or 0x1"
        );
    }
    Ok(())
}

fn queries(subscription: &Subscription) -> Vec<Query> {
    let mut queries = Vec::new();
    if let Some(address) = &subscription.address {
        if subscription.account {
            for method in ["eth_getBalance", "eth_getTransactionCount", "eth_getCode"] {
                queries.push(Query {
                    method,
                    params: json!([address, "latest"]),
                });
            }
        }
        for slot in &subscription.storage {
            queries.push(Query {
                method: "eth_getStorageAt",
                params: json!([address, slot, "latest"]),
            });
        }
    }
    if subscription.on_block {
        for call in &subscription.calls {
            queries.push(Query {
                method: "eth_call",
                params: json!([call, "latest"]),
            });
        }
    }
    queries
}

fn subscription_status(
    subscription: &Subscription,
    head: Option<&Head>,
    completed: Vec<CompletedQuery>,
) -> Value {
    let query_count = queries(subscription).len();
    let complete = completed.len() == query_count;
    let mut results = Vec::new();
    let mut errors = Vec::new();
    for CompletedQuery {
        index,
        query,
        response,
    } in completed
    {
        match response {
            Ok(result) => results.push(json!({ "index": index, "method": query.method, "params": query.params, "result": result })),
            Err(error) => errors.push(json!({
                "index": index,
                "method": query.method,
                "params": query.params,
                "code": error.code,
                "message": error.message,
                "data": error.data,
            })),
        }
    }
    json!({
        "name": subscription.name,
        "ready": head.is_some() && complete && errors.is_empty(),
        "blockNumber": head.map(|head| format!("{:#x}", head.number)),
        "blockHash": head.map(|head| &head.hash),
        "sampled": true,
        "onBlock": subscription.on_block,
        "queryCount": query_count,
        "results": results,
        "errors": errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADDRESS: &str = "0x0000000000000000000000000000000000000001";

    fn config(subscription: Value) -> Config {
        serde_json::from_value(json!({ "state": { "subscriptions": [subscription] } })).unwrap()
    }

    #[test]
    fn parses_familiar_shape_and_defaults_to_on_block() {
        let config = config(json!({
            "name": "pool",
            "address": ADDRESS,
            "account": true,
            "storage": ["0x0", "0x000A"],
            "calls": [{ "to": ADDRESS, "data": "0x12345678", "gas": "0x001000" }],
        }));
        let watchlist = Watchlist::new(config).unwrap();
        assert!(!watchlist.is_empty());
        let subscription = &watchlist.subscriptions[0];
        assert!(subscription.on_block);
        assert_eq!(subscription.storage, ["0x0", "0xa"]);
        assert_eq!(subscription.calls[0]["gas"], "0x1000");
        assert_eq!(queries(subscription).len(), 6);
        assert_eq!(watchlist.status()[0]["ready"], false);
    }

    #[test]
    fn rejects_original_features_that_rpc_mode_cannot_provide() {
        for value in [
            json!({ "state": { "on_demand": { "enabled": true } } }),
            json!({ "predictors": {} }),
            json!({ "archive": "archive.db" }),
            json!({ "state": { "subscriptions": [{ "name": "pool", "address": ADDRESS, "retention": { "mode": "archive" } }] } }),
            json!({ "state": { "subscriptions": [{ "name": "pool", "address": ADDRESS, "storage": "all" }] } }),
            json!({ "state": { "subscriptions": [{ "name": "pool", "address": ADDRESS, "max_slots": 10 }] } }),
        ] {
            assert!(serde_json::from_value::<Config>(value).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_names_and_empty_names() {
        let duplicate_config: Config =
            serde_json::from_value(json!({ "state": { "subscriptions": [
            { "name": "pool", "address": ADDRESS }, { "name": "pool", "address": ADDRESS },
        ] } }))
            .unwrap();
        assert!(
            Watchlist::new(duplicate_config)
                .err()
                .unwrap()
                .to_string()
                .contains("duplicate")
        );
        assert!(Watchlist::new(config(json!({ "name": "  " }))).is_err());
    }

    #[test]
    fn validates_addresses_slots_and_transaction_fields() {
        for subscription in [
            json!({ "name": "pool", "account": true }),
            json!({ "name": "pool", "storage": ["0x1"] }),
            json!({ "name": "pool", "address": "0x1234" }),
            json!({ "name": "pool", "address": ADDRESS, "storage": ["0x"] }),
            json!({ "name": "pool", "address": ADDRESS, "storage": [format!("0x{}", "1".repeat(65))] }),
            json!({ "name": "pool", "calls": [{ "to": ADDRESS, "data": "0x1" }] }),
            json!({ "name": "pool", "calls": [{ "to": ADDRESS, "gasLimit": "0x1000" }] }),
            json!({ "name": "pool", "calls": [{ "to": ADDRESS, "value": 12 }] }),
            json!({ "name": "pool", "calls": [{ "to": ADDRESS, "data": "0x12", "input": "0x34" }] }),
            json!({ "name": "pool", "calls": [{ "to": ADDRESS, "gasPrice": "0x1", "maxFeePerGas": "0x2" }] }),
            json!({ "name": "pool", "calls": [{ "to": ADDRESS, "stateOverride": {} }] }),
        ] {
            assert!(Watchlist::new(config(subscription)).is_err());
        }
    }

    #[test]
    fn accepts_supported_transaction_fields_and_checks_nested_fields() {
        let mut call = json!({
            "from": ADDRESS, "to": ADDRESS, "input": "0x", "value": "0x0",
            "gas": "0x10000", "nonce": "0x0", "chainId": "0x71b55", "type": "0x2",
            "maxFeePerGas": "0x10", "maxPriorityFeePerGas": "0x1",
            "accessList": [{ "address": ADDRESS, "storageKeys": [format!("0x{}", "0".repeat(64))] }],
        });
        validate_call(&mut call).unwrap();
        call["accessList"][0]["storageKeys"][0] = json!("0x0");
        assert!(validate_call(&mut call).is_err());
        let mut authorization = json!({ "authorizationList": [{
            "chainId": "0x0", "address": ADDRESS, "nonce": "0x0", "yParity": "0x1", "r": "0x1", "s": "0x1",
        }] });
        validate_call(&mut authorization).unwrap();
        authorization["authorizationList"][0]["yParity"] = json!("0x2");
        assert!(validate_call(&mut authorization).is_err());
    }

    #[test]
    fn caps_total_read_queries_and_honors_disabled_calls() {
        let mut subscription =
            json!({ "name": "pool", "address": ADDRESS, "storage": vec!["0x0"; MAX_READ_QUERIES] });
        assert!(Watchlist::new(config(subscription.clone())).is_ok());
        subscription["account"] = json!(true);
        assert!(Watchlist::new(config(subscription)).is_err());
        let watchlist = Watchlist::new(config(
            json!({ "name": "disabled", "calls": [{ "to": ADDRESS }], "on_block": false }),
        ))
        .unwrap();
        assert!(queries(&watchlist.subscriptions[0]).is_empty());
    }

    #[test]
    fn bounds_retained_configuration_even_for_disabled_calls() {
        let oversized = config(json!({
            "name": "disabled", "on_block": false,
            "calls": [{ "to": ADDRESS, "data": format!("0x{}", "ab".repeat(MAX_QUERY_CONFIG_BYTES / 2)) }],
        }));
        assert!(
            Watchlist::new(oversized)
                .err()
                .unwrap()
                .to_string()
                .contains("1 MiB")
        );
        let excessive_subscriptions: Config = serde_json::from_value(json!({ "state": { "subscriptions":
            (0..=MAX_READ_QUERIES).map(|index| json!({ "name": format!("empty-{index}") })).collect::<Vec<_>>()
        } })).unwrap();
        assert!(Watchlist::new(excessive_subscriptions).is_err());
    }

    #[test]
    fn snapshot_budget_keeps_limit_errors_and_later_small_results() {
        let watchlist = Watchlist::new(config(
            json!({ "name": "account", "address": ADDRESS, "account": true }),
        ))
        .unwrap();
        let subscription = &watchlist.subscriptions[0];
        let queries = queries(subscription);
        let head = Head {
            number: 42,
            hash: format!("0x{}", "a".repeat(64)),
            block: json!({}),
        };
        let fixed = encoded_size(&vec![subscription_status(
            subscription,
            Some(&head),
            Vec::new(),
        )]);
        let reserved_errors = queries
            .iter()
            .enumerate()
            .map(|(index, query)| entry_size(index, query, &Err(snapshot_limit_error())) + 1)
            .sum::<usize>();
        let limit = fixed + reserved_errors + 20;
        let mut budget = SnapshotBudget {
            used: fixed,
            reserved_errors,
            limit,
        };
        let responses = [
            Ok(json!("x".repeat(limit * 2))),
            Ok(json!("0x0")),
            Err(RpcError {
                code: -32000,
                message: "upstream error".into(),
                data: Some(json!("x".repeat(limit * 2))),
            }),
        ];
        let completed = queries
            .into_iter()
            .zip(responses)
            .enumerate()
            .map(|(index, (query, response))| {
                budget.admit(CompletedQuery {
                    index,
                    query,
                    response,
                })
            })
            .collect();
        let status = subscription_status(subscription, Some(&head), completed);
        assert!(encoded_size(&vec![status.clone()]) <= limit);
        assert!(budget.used <= limit);
        assert_eq!(budget.reserved_errors, 0);
        assert_eq!(status["results"].as_array().unwrap().len(), 1);
        assert_eq!(status["results"][0]["index"], 1);
        assert_eq!(status["errors"].as_array().unwrap().len(), 2);
        assert!(
            status["errors"]
                .as_array()
                .unwrap()
                .iter()
                .all(|error| error["code"] == -32005)
        );
        assert_eq!(status["ready"], false);
    }

    #[test]
    fn errors_are_kept_with_successful_results_and_never_report_ready() {
        let watchlist = Watchlist::new(config(
            json!({ "name": "pool", "address": ADDRESS, "storage": ["0x0", "0x1"] }),
        ))
        .unwrap();
        let subscription = &watchlist.subscriptions[0];
        let queries = queries(subscription);
        let head = Head {
            number: 42,
            hash: format!("0x{}", "a".repeat(64)),
            block: json!({}),
        };
        let status = subscription_status(
            subscription,
            Some(&head),
            vec![
                CompletedQuery {
                    index: 0,
                    query: queries[0].clone(),
                    response: Ok(json!("0x1234")),
                },
                CompletedQuery {
                    index: 1,
                    query: queries[1].clone(),
                    response: Err(RpcError {
                        code: -32000,
                        message: "upstream unavailable".into(),
                        data: Some(json!({ "retry": true })),
                    }),
                },
            ],
        );
        assert_eq!(status["ready"], false);
        assert_eq!(status["blockNumber"], "0x2a");
        assert_eq!(status["blockHash"], head.hash);
        assert_eq!(status["results"][0]["result"], "0x1234");
        assert_eq!(status["errors"][0]["index"], 1);
        assert_eq!(status["errors"][0]["code"], -32000);
        assert_eq!(status["errors"][0]["data"]["retry"], true);
        assert_eq!(
            subscription_status(subscription, Some(&head), Vec::new())["ready"],
            false
        );
    }

    #[test]
    fn bundled_network_examples_are_valid_watchlists() {
        for yaml in [
            include_str!("../../../examples/robinhood-chain/mainnet.yaml"),
            include_str!("../../../examples/robinhood-chain/testnet.yaml"),
        ] {
            let config = serde_yaml_ng::from_str::<Config>(yaml).unwrap();
            let watchlist = Watchlist::new(config).unwrap();
            assert!(!watchlist.is_empty());
        }
    }

    #[tokio::test]
    async fn warming_pins_reads_retries_errors_and_skips_successful_heads() {
        use axum::{Json, Router, routing::post};
        use std::sync::{Arc, Mutex};

        let reads = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded = reads.clone();
        let hash = format!("0x{}", "a".repeat(64));
        let block = json!({ "number": "0x2a", "hash": hash });
        let handler_block = block.clone();
        let router = Router::new().route("/", post(move |Json(request): Json<Value>| {
            let reads = recorded.clone();
            let block = handler_block.clone();
            async move {
                let result = match request["method"].as_str().unwrap() {
                    "eth_chainId" => json!("0x1237"),
                    "eth_getBlockByNumber" => block,
                    "eth_getStorageAt" => {
                        let mut reads = reads.lock().unwrap();
                        reads.push(request["params"].clone());
                        if reads.len() == 1 {
                            return Json(json!({ "jsonrpc": "2.0", "id": request["id"], "error": { "code": -32000, "message": "temporary failure" } }));
                        }
                        json!(format!("0x{}", "0".repeat(64)))
                    }
                    method => panic!("unexpected method {method}"),
                };
                Json(json!({ "jsonrpc": "2.0", "id": request["id"], "result": result }))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let backend = Backend::connect(&upstream, 4663, 100, 1024 * 1024)
            .await
            .unwrap();
        let head = backend.head().unwrap();
        let watchlist = Watchlist::new(config(
            json!({ "name": "pool", "address": ADDRESS, "storage": ["0x0"] }),
        ))
        .unwrap();

        watchlist.warm(&backend, &head).await;
        assert_eq!(watchlist.status()[0]["ready"], false);
        assert_eq!(watchlist.status()[0]["errors"][0]["code"], -32000);
        watchlist.warm(&backend, &head).await;
        assert_eq!(watchlist.status()[0]["ready"], true);
        assert!(
            watchlist.status()[0]["errors"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        watchlist.warm(&backend, &head).await;
        assert_eq!(reads.lock().unwrap().len(), 2);
        let next = Head {
            number: head.number + 1,
            hash: format!("0x{}", "b".repeat(64)),
            block,
        };
        watchlist.warm(&backend, &next).await;
        assert_eq!(watchlist.status()[0]["blockHash"], next.hash);
        let reads = reads.lock().unwrap();
        assert_eq!(reads.len(), 3);
        assert_eq!(
            reads[0][2],
            json!({ "blockHash": head.hash, "requireCanonical": true })
        );
        assert_eq!(
            reads[2][2],
            json!({ "blockHash": next.hash, "requireCanonical": true })
        );
        server.abort();
    }
}
