//! Consensus verification for `skim run`: which execution blocks a supermajority of the sync
//! committee has signed off on (`safe`), and which are finalized (`finalized`).
//!
//! Trust starts from a checkpoint block root: `--checkpoint`, else the last finalized root
//! this node verified itself (saved in the data dir), else a checkpoint-sync URL. From there,
//! every update must carry a valid sync-committee signature; committees roll forward only via
//! finalized headers those committees signed.

use std::sync::Arc;

use alloy_primitives::B256;
use parking_lot::{Mutex, RwLock};
use skim_cl::{ClNode, light};
use skim_state::{Store, persist::Persist};

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct LightStatus {
    /// Sync committee period skim holds (None until bootstrapped).
    pub period: Option<u64>,
    /// Highest execution block a supermajority of the sync committee vouched for.
    pub verified_block: Option<u64>,
    pub verified_hash: Option<B256>,
    /// Execution block of the latest finalized beacon block skim verified.
    pub finalized_block: Option<u64>,
    pub finalized_hash: Option<B256>,
    pub updates_verified: u64,
    pub updates_rejected: u64,
    pub last_error: Option<String>,
    pub trusted_from: Option<String>,
}

pub struct Light {
    committee: Mutex<Option<light::Committee>>,
    status: RwLock<LightStatus>,
    cl: ClNode,
    persist: Option<Arc<Persist>>,
    bootstrapping: std::sync::atomic::AtomicBool,
}

const FINALIZED_ROOT_META: &str = "lc_finalized_root";

impl Light {
    pub fn new(cl: ClNode, persist: Option<Arc<Persist>>) -> Arc<Self> {
        Arc::new(Self {
            committee: Mutex::new(None),
            status: Default::default(),
            cl,
            persist,
            bootstrapping: Default::default(),
        })
    }

    pub fn status(&self) -> LightStatus {
        self.status.read().clone()
    }

    /// Picks the trusted root and bootstraps in the background, retrying across peers.
    pub fn start(self: &Arc<Self>, checkpoint: Option<B256>, checkpoint_url: String) {
        let me = self.clone();
        tokio::spawn(async move {
            let saved = me.persist.as_ref().and_then(|p| p.get_meta(FINALIZED_ROOT_META).ok().flatten()).filter(|v| v.len() == 32);
            let (root, from) = match (checkpoint, saved) {
                (Some(r), _) => (r, "--checkpoint".to_string()),
                (None, Some(v)) => (B256::from_slice(&v), "finalized root verified in a previous run".to_string()),
                (None, None) => match fetch_checkpoint(&checkpoint_url).await {
                    Ok(r) => (r, format!("checkpoint sync URL {checkpoint_url}")),
                    Err(e) => {
                        me.status.write().last_error = Some(format!("no checkpoint: {e}"));
                        tracing::warn!("light client: no checkpoint root ({e}); consensus verification is off");
                        return;
                    }
                },
            };
            me.status.write().trusted_from = Some(from.clone());
            tracing::info!(%root, %from, "light client: bootstrapping");
            me.bootstrap(root).await;
        });
    }

    async fn bootstrap(&self, root: B256) {
        if self.bootstrapping.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        for attempt in 0..120 {
            match self.cl.light_client_bootstrap(root.0).await.and_then(|b| light::bootstrap(&b, root).map_err(Into::into)) {
                Ok((header, committee)) => {
                    tracing::info!(slot = header.beacon.slot, period = committee.period, "light client: sync committee verified");
                    self.status.write().period = Some(committee.period);
                    *self.committee.lock() = Some(committee);
                    break;
                }
                Err(e) => {
                    if attempt % 10 == 0 {
                        tracing::debug!("light client bootstrap: {e}");
                    }
                    self.status.write().last_error = Some(format!("bootstrap: {e}"));
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                }
            }
        }
        self.bootstrapping.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// A gossiped light-client update. Verified against the committee; results are stored as
    /// execution block hashes and resolved to numbers by [`Self::on_block`].
    pub async fn on_gossip(self: &Arc<Self>, topic: &str, data: &[u8], store: &Store) {
        let fv = skim_cl::node::SEPOLIA_GLOAS_VERSION;
        tracing::debug!(topic, len = data.len(), "light client update received");
        let res = {
            let c = self.committee.lock();
            let Some(c) = c.as_ref() else { return };
            match topic {
                "light_client_optimistic_update" => light::optimistic_update(c, data, fv).map(|(a, _)| (a, None)),
                "light_client_finality_update" => light::finality_update(c, data, fv).map(|(a, f)| (a, Some(f))),
                _ => return,
            }
        };
        match res {
            Ok((attested, finalized)) => {
                {
                    let mut st = self.status.write();
                    st.updates_verified += 1;
                    st.verified_hash = Some(attested.execution_block_hash);
                    if let Some(f) = &finalized {
                        st.finalized_hash = Some(f.execution_block_hash);
                    }
                }
                if let Some(f) = finalized {
                    let root = B256::from(f.beacon.root());
                    if let Some(p) = &self.persist {
                        let _ = p.set_meta(FINALIZED_ROOT_META, root.as_slice());
                    }
                    // A finalized header in a newer period carries the next committee.
                    let held = self.committee.lock().as_ref().map(|c| c.period);
                    if held.is_some_and(|p| f.beacon.slot / light::SLOTS_PER_PERIOD > p) {
                        let me = self.clone();
                        tokio::spawn(async move { me.bootstrap(root).await });
                    }
                }
                self.on_block(store);
            }
            Err(e) => {
                let mut st = self.status.write();
                st.updates_rejected += 1;
                st.last_error = Some(e.to_string());
                tracing::debug!(topic, "light client update rejected: {e}");
            }
        }
    }

    /// Resolves verified hashes to block numbers in the store's chain (blocks may arrive after
    /// the update that vouches for them).
    pub fn on_block(&self, store: &Store) {
        let (v, f) = {
            let st = self.status.read();
            (st.verified_hash, st.finalized_hash)
        };
        let vn = v.and_then(|h| store.number_of_hash(&h));
        let fnum = f.and_then(|h| store.number_of_hash(&h));
        let mut st = self.status.write();
        if let Some(n) = vn {
            st.verified_block = Some(st.verified_block.map_or(n, |o| o.max(n)));
        }
        if let Some(n) = fnum {
            st.finalized_block = Some(st.finalized_block.map_or(n, |o| o.max(n)));
        }
    }
}

async fn fetch_checkpoint(url: &str) -> anyhow::Result<B256> {
    let v: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/eth/v1/beacon/blocks/finalized/root", url.trim_end_matches('/')))
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await?
        .json()
        .await?;
    let r = v["data"]["root"].as_str().ok_or_else(|| anyhow::anyhow!("no data.root"))?;
    Ok(r.trim_start_matches("0x").parse()?)
}
