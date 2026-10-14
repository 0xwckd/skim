//! EIP-7928 block access lists for skim: commitment checks on raw wire bytes, decoding, and
//! normalization of one block's BAL into final per-key writes (PROJECT.md §3.1, §5.2).
//!
//! Types come from `alloy-eip7928`; this crate only adds what the state follower needs.

use alloy_eip7928::{AccountChanges, BlockAccessList};
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use alloy_rlp::Decodable;

pub use alloy_eip7928;

/// RLP of an empty list: a valid, empty BAL (distinct from `0x80`, "unavailable", on eth/71).
pub const EMPTY_BAL_RLP: u8 = 0xc0;

#[derive(Debug, thiserror::Error)]
pub enum BalError {
    #[error("BAL commitment mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: B256, actual: B256 },
    #[error("BAL RLP decode failed: {0}")]
    Decode(#[from] alloy_rlp::Error),
    #[error("BAL has {0} trailing bytes")]
    TrailingBytes(usize),
}

/// Checks `keccak256(raw) == expected` and decodes. The commitment covers the exact bytes on the
/// wire, so it is checked before decoding and never by re-encoding.
pub fn decode_verified(raw: &[u8], expected: B256) -> Result<BlockAccessList, BalError> {
    let actual = keccak256(raw);
    if actual != expected {
        return Err(BalError::HashMismatch { expected, actual });
    }
    decode(raw)
}

/// Decodes without a commitment check (for raw bytes already verified elsewhere).
pub fn decode(raw: &[u8]) -> Result<BlockAccessList, BalError> {
    let mut buf = raw;
    let bal = <Vec<AccountChanges>>::decode(&mut buf)?;
    if !buf.is_empty() {
        return Err(BalError::TrailingBytes(buf.len()));
    }
    Ok(bal)
}

/// One state key, as tracked by the per-key MVCC store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StateKey {
    Balance(Address),
    Nonce(Address),
    CodeHash(Address),
    Storage(Address, U256),
}

/// The final post-block value of every key a block wrote, plus what it only read.
#[derive(Debug, Default, Clone)]
pub struct BlockWrites {
    /// Final value per written key (last change by `block_access_index`).
    pub writes: Vec<(StateKey, U256)>,
    /// New code by hash, for keys in `writes` of kind `CodeHash` (value = hash as U256).
    pub code: Vec<(B256, Bytes)>,
    /// Slots read but not written. They carry no values and never make a key known.
    pub storage_reads: Vec<(Address, U256)>,
    /// Every address in the BAL, including access-only entries. Access says nothing about
    /// existence (§5.4).
    pub touched: Vec<Address>,
}

impl BlockWrites {
    pub fn from_bal(bal: &[AccountChanges]) -> Self {
        let mut out = Self::default();
        for acc in bal {
            let a = acc.address;
            out.touched.push(a);
            if let Some(b) = acc.balance_post_state() {
                out.writes.push((StateKey::Balance(a), b));
            }
            if let Some(n) = acc.nonce_post_state() {
                out.writes.push((StateKey::Nonce(a), U256::from(n)));
            }
            if let Some((hash, code)) = acc.code_post_state_with_hash() {
                out.writes.push((StateKey::CodeHash(a), hash.into()));
                out.code.push((hash, code.clone()));
            }
            for (slot, value) in acc.storage_post_states() {
                out.writes.push((StateKey::Storage(a, slot), value));
            }
            out.storage_reads.extend(acc.storage_reads().iter().map(|&s| (a, s)));
        }
        out
    }

    pub fn get(&self, key: &StateKey) -> Option<U256> {
        self.writes.iter().find(|(k, _)| k == key).map(|&(_, v)| v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_eip7928::{BalanceChange, BlockAccessIndex, SlotChanges, StorageChange};

    fn sample() -> BlockAccessList {
        let a = Address::repeat_byte(0xaa);
        vec![
            AccountChanges::new(a)
                .with_storage_change(
                    SlotChanges::new(U256::from(1), Vec::new())
                        .with_change(StorageChange::new(BlockAccessIndex(1), U256::from(10)))
                        .with_change(StorageChange::new(BlockAccessIndex(3), U256::from(30))),
                )
                .with_storage_read(U256::from(7))
                .with_balance_change(BalanceChange::new(BlockAccessIndex(2), U256::from(5))),
            AccountChanges::new(Address::repeat_byte(0xbb)),
        ]
    }

    #[test]
    fn verify_and_normalize() {
        let bal = sample();
        let raw = alloy_rlp::encode(&bal);
        let decoded = decode_verified(&raw, keccak256(&raw)).unwrap();
        assert_eq!(decoded, bal);

        let w = BlockWrites::from_bal(&decoded);
        let a = Address::repeat_byte(0xaa);
        assert_eq!(w.get(&StateKey::Storage(a, U256::from(1))), Some(U256::from(30)));
        assert_eq!(w.get(&StateKey::Balance(a)), Some(U256::from(5)));
        assert_eq!(w.get(&StateKey::Storage(a, U256::from(7))), None);
        assert_eq!(w.storage_reads, vec![(a, U256::from(7))]);
        assert_eq!(w.touched.len(), 2);
    }

    #[test]
    fn rejects_bad_commitment_and_trailing_bytes() {
        let raw = alloy_rlp::encode(sample());
        assert!(matches!(decode_verified(&raw, B256::ZERO), Err(BalError::HashMismatch { .. })));
        let mut long = raw.clone();
        long.push(0);
        assert!(matches!(decode(&long), Err(BalError::TrailingBytes(1))));
    }

    #[test]
    fn empty_bal() {
        assert!(decode(&[EMPTY_BAL_RLP]).unwrap().is_empty());
    }
}
