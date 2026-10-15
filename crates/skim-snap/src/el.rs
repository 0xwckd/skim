//! [`Transport`] over skim's EL network (reth sessions, snap/2).

use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use alloy_rlp::Decodable;
use skim_el::{ElNet, ReqError};

use crate::{AccountAnswer, AccountState, PeerId, SlotAnswer, Transport, TransportError};

#[derive(Clone)]
pub struct ElTransport {
    pub net: ElNet,
    /// Peers the scheduler must not use (experiments, operator policy).
    pub excluded: std::sync::Arc<parking_lot::RwLock<std::collections::HashSet<PeerId>>>,
}

impl ElTransport {
    pub fn new(net: ElNet) -> Self {
        Self { net, excluded: Default::default() }
    }

    pub fn exclude(&self, peer: PeerId) {
        self.excluded.write().insert(peer);
    }
}

fn map_err(e: ReqError) -> TransportError {
    match e {
        ReqError::Timeout => TransportError::Timeout,
        // reth drops the response channel when the session closes.
        ReqError::Peer(s) if s.contains("dropped") => TransportError::Disconnected,
        other => TransportError::Other(other.to_string()),
    }
}

impl Transport for ElTransport {
    fn snap_peers(&self) -> Vec<PeerId> {
        let ex = self.excluded.read();
        self.net.peers().into_iter().filter(|(id, p)| (p.snap2 || p.snap1) && !ex.contains(id)).map(|(id, _)| id).collect()
    }

    async fn account(&self, peer: PeerId, root: B256, address: Address) -> Result<AccountAnswer, TransportError> {
        let h = keccak256(address);
        let m = self.net.account_range(peer, root, h, 512).await.0.map_err(map_err)?;
        if m.accounts.is_empty() && m.proof.is_empty() {
            return Ok(AccountAnswer::Unavailable);
        }
        Ok(AccountAnswer::Found(m.accounts.first().filter(|d| d.hash == h).map(|d| {
            let t = d.trie_account();
            AccountState { nonce: t.nonce, balance: t.balance, storage_root: t.storage_root, code_hash: t.code_hash }
        })))
    }

    async fn storage(&self, peer: PeerId, root: B256, address: Address, slot: U256) -> Result<SlotAnswer, TransportError> {
        let sh = keccak256(slot.to_be_bytes::<32>());
        let m = self.net.storage_range(peer, root, keccak256(address), sh, 512).await.0.map_err(map_err)?;
        let first = m.slots.first().and_then(|s| s.first());
        if first.is_none() && m.proof.is_empty() {
            return Ok(SlotAnswer::EmptyNoProof);
        }
        Ok(SlotAnswer::Found(
            first.filter(|d| d.hash == sh).and_then(|d| U256::decode(&mut d.data.as_ref()).ok()).unwrap_or_default(),
        ))
    }

    async fn codes(&self, peer: PeerId, hashes: Vec<B256>) -> Result<Vec<Bytes>, TransportError> {
        Ok(self.net.byte_codes(peer, hashes, 2 << 20).await.0.map_err(map_err)?.codes)
    }
}
