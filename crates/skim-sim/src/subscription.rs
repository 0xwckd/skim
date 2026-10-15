//! Subscriptions (PROJECT.md §7.5): the state an application declares is kept current and
//! **ready**; everything else is on demand.
//!
//! - **Account:** balance, nonce and code hash.
//! - **Storage list:** explicit slots.
//! - **Learned:** whatever the calls in `calls` read (their access sets, re-learned every block),
//!   also used by protocol predictors.
//! - **Simulation:** run fixed call inputs every block. Their access sets stay subscribed, so the
//!   calls stay warm; with `on_block` the latest results are kept for readers.
//!
//! **Ready** means the head's BAL is applied and every required key of the subscription is known
//! at the head. Readiness is reported per subscription and never assumed.

use std::collections::{BTreeMap, HashSet};

use alloy_primitives::{Address, Bytes, U256};
use parking_lot::RwLock;
use revm::{context::TxEnv, primitives::TxKind};
use serde::{Deserialize, Serialize};
use skim_bal::StateKey;
use skim_state::{BlockRef, Retention, StateValue};

use crate::{Mode, Simulator, prefetch};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum StorageSpec {
    Slots(Vec<U256>),
    /// `"all"`: the whole storage trie. Not supported yet (needs SNAP range download).
    All(String),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct CallSpec {
    pub to: Address,
    pub data: Bytes,
    #[serde(default)]
    pub from: Option<Address>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SubscriptionSpec {
    pub name: String,
    #[serde(default)]
    pub address: Option<Address>,
    #[serde(default)]
    pub account: bool,
    #[serde(default)]
    pub storage: Option<StorageSpec>,
    /// Simulation inputs whose access sets are learned and kept warm.
    #[serde(default)]
    pub calls: Vec<CallSpec>,
    /// Re-run `calls` every block and keep the latest results.
    #[serde(default)]
    pub on_block: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Status {
    pub name: String,
    pub ready: bool,
    /// Block the status refers to.
    pub block: Option<u64>,
    pub required_keys: usize,
    pub missing_keys: usize,
    pub truncated: bool,
    pub error: Option<String>,
    /// Latest results of `calls` (hex output, or an error), when `on_block` is set.
    pub results: Vec<String>,
}

pub struct Subscriptions {
    specs: Vec<SubscriptionSpec>,
    max_keys: usize,
    /// Required keys per subscription (declared + learned).
    required: RwLock<BTreeMap<String, HashSet<StateKey>>>,
    status: RwLock<BTreeMap<String, Status>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("subscription {0}: storage \"all\" is not supported yet")]
    StorageAll(String),
    #[error("subscription {0}: storage/account needs an address")]
    NoAddress(String),
}

fn account_keys(a: Address) -> [StateKey; 3] {
    [StateKey::Balance(a), StateKey::Nonce(a), StateKey::CodeHash(a)]
}

impl Subscriptions {
    pub fn new(specs: Vec<SubscriptionSpec>, max_keys: usize) -> Result<Self, ConfigError> {
        let mut required = BTreeMap::new();
        for s in &specs {
            let mut keys = HashSet::new();
            if (s.account || s.storage.is_some()) && s.address.is_none() {
                return Err(ConfigError::NoAddress(s.name.clone()));
            }
            if let Some(a) = s.address {
                if s.account {
                    keys.extend(account_keys(a));
                }
                match &s.storage {
                    Some(StorageSpec::All(_)) => return Err(ConfigError::StorageAll(s.name.clone())),
                    Some(StorageSpec::Slots(v)) => keys.extend(v.iter().map(|slot| StateKey::Storage(a, *slot))),
                    None => {}
                }
            }
            required.insert(s.name.clone(), keys);
        }
        let status = specs.iter().map(|s| (s.name.clone(), Status { name: s.name.clone(), ..Default::default() })).collect();
        Ok(Self { specs, max_keys, required: RwLock::new(required), status: RwLock::new(status) })
    }

    pub fn total_keys(&self) -> usize {
        self.required.read().values().map(HashSet::len).sum()
    }

    pub fn status(&self) -> Vec<Status> {
        self.status.read().values().cloned().collect()
    }

    /// Per block: subscribe declared keys (fetching unknown ones), run/learn the calls,
    /// then compute readiness at `head`.
    pub async fn maintain(&self, sim: &Simulator, head: &BlockRef) {
        // 1. Declared keys: subscribe and fetch whatever is unknown (one wave, parent root).
        let declared: Vec<StateKey> = self.required.read().values().flat_map(|k| k.iter().copied()).collect();
        for k in &declared {
            sim.store.watch_key(*k);
        }
        if let Ok(snap) = sim.store.snapshot(head) {
            let (accts, slots) = crate::unknown_split(&snap, &declared);
            if !accts.is_empty() || !slots.is_empty() {
                let at = if sim.miss_at_parent { sim.store.block(head.number.saturating_sub(1)).unwrap_or(*head) } else { *head };
                prefetch::resolve_retained(&sim.store, &sim.sched, &at, &accts, &slots, &[], skim_snap::Priority::High, Retention::Subscribed)
                    .await;
            }
        }

        // 2. Calls: run with subscribed retention; their access sets become required keys.
        for spec in self.specs.iter().filter(|s| !s.calls.is_empty()) {
            let mut results = Vec::new();
            let mut learned = HashSet::new();
            for c in &spec.calls {
                let Some(header) = sim.header(head) else { break };
                let tx = TxEnv::builder()
                    .caller(c.from.unwrap_or_default())
                    .kind(TxKind::Call(c.to))
                    .data(c.data.clone())
                    .gas_limit(header.gas_limit)
                    .gas_price(0)
                    .build()
                    .unwrap_or_default();
                match sim.call_retained(head, tx, Mode::Resolve { max_rounds: 8 }, Retention::Subscribed).await {
                    Ok(o) => {
                        for a in &o.access.accounts {
                            learned.extend(account_keys(*a));
                        }
                        learned.extend(o.access.storage.iter().map(|(a, s)| StateKey::Storage(*a, *s)));
                        results.push(match &o.result {
                            revm::context::result::ExecutionResult::Success { output, .. } => format!("{}", output.data()),
                            other => format!("error: {other:?}"),
                        });
                    }
                    Err(e) => results.push(format!("error: {e}")),
                }
            }
            let mut req = self.required.write();
            let declared_len = req.get(&spec.name).map_or(0, |k| k.len());
            let room = self.max_keys.saturating_sub(req.values().map(HashSet::len).sum::<usize>() - declared_len);
            let entry = req.entry(spec.name.clone()).or_default();
            let mut truncated = false;
            for k in learned {
                if entry.len() >= room && !entry.contains(&k) {
                    truncated = true;
                    continue;
                }
                entry.insert(k);
            }
            drop(req);
            let mut st = self.status.write();
            let s = st.entry(spec.name.clone()).or_default();
            s.truncated = truncated;
            if spec.on_block {
                s.results = results;
            }
        }

        // 3. Readiness at the head.
        let Ok(snap) = sim.store.snapshot(head) else { return };
        let req = self.required.read();
        let mut st = self.status.write();
        for (name, keys) in req.iter() {
            let missing = keys.iter().filter(|k| !matches!(snap.get(k), Ok(StateValue::Known(_)))).count();
            let s = st.entry(name.clone()).or_default();
            s.name = name.clone();
            s.block = Some(head.number);
            s.required_keys = keys.len();
            s.missing_keys = missing;
            s.ready = missing == 0 && sim.store.head().is_some_and(|h| h.hash == head.hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_yaml_and_rejects_unsupported() {
        let y = r#"
- name: weth
  address: "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"
  account: true
  storage: ["0x3", "0x4"]
- name: quoter
  calls:
    - to: "0xEd1f6473345F45b75F8179591dd5bA1888cf2FB3"
      data: "0xc6a5026a"
  on_block: true
"#;
        let specs: Vec<SubscriptionSpec> = serde_yaml_ng::from_str(y).unwrap();
        let subs = Subscriptions::new(specs, 1000).unwrap();
        assert_eq!(subs.total_keys(), 5); // 3 account keys + 2 slots; calls learn later
        let bad: Vec<SubscriptionSpec> = serde_yaml_ng::from_str("- name: x\n  address: \"0x0000000000000000000000000000000000000001\"\n  storage: all\n").unwrap();
        assert!(matches!(Subscriptions::new(bad, 10), Err(ConfigError::StorageAll(_))));
        let noaddr: Vec<SubscriptionSpec> = serde_yaml_ng::from_str("- name: y\n  account: true\n").unwrap();
        assert!(matches!(Subscriptions::new(noaddr, 10), Err(ConfigError::NoAddress(_))));
    }
}
