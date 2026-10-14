//! Ties a gossiped payload's BAL to its block hash (PROJECT.md §3.2): rebuild the Amsterdam EL
//! header from the payload fields with `block_access_list_hash = keccak256(raw BAL)` and check
//! `keccak256(rlp(header)) == payload.block_hash`.

use alloy_consensus::{
    EMPTY_OMMER_ROOT_HASH, Header,
    private::alloy_eips::eip4895::Withdrawal as ElWithdrawal,
    proofs::{calculate_withdrawals_root, ordered_trie_root_with_encoder},
};
use alloy_primitives::{B64, B256, Bytes, U256, keccak256};

use crate::ssz::Envelope;

#[derive(Debug, Clone)]
pub struct Verified {
    /// Rebuilt header hash; equals the payload's `block_hash` when `ok`.
    pub computed_hash: B256,
    pub ok: bool,
    pub bal_hash: B256,
    pub header: Header,
}

/// Rebuilds the EL header for the envelope's payload.
pub fn rebuild_header(env: &Envelope<'_>) -> Header {
    let p = &env.payload;
    let withdrawals: Vec<ElWithdrawal> = p
        .withdrawals
        .iter()
        .map(|w| ElWithdrawal {
            index: w.index,
            validator_index: w.validator_index,
            address: w.address,
            amount: w.amount_gwei,
        })
        .collect();
    let base_fee: u64 = p.base_fee_per_gas.try_into().unwrap_or(u64::MAX);
    Header {
        parent_hash: p.parent_hash,
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: p.fee_recipient,
        state_root: p.state_root,
        transactions_root: ordered_trie_root_with_encoder(&p.transactions, |tx: &&[u8], buf| {
            buf.extend_from_slice(tx)
        }),
        receipts_root: p.receipts_root,
        logs_bloom: p.logs_bloom,
        difficulty: U256::ZERO,
        number: p.block_number,
        gas_limit: p.gas_limit,
        gas_used: p.gas_used,
        timestamp: p.timestamp,
        extra_data: Bytes::copy_from_slice(p.extra_data),
        mix_hash: p.prev_randao,
        nonce: B64::ZERO,
        base_fee_per_gas: Some(base_fee),
        withdrawals_root: Some(calculate_withdrawals_root(&withdrawals)),
        blob_gas_used: Some(p.blob_gas_used),
        excess_blob_gas: Some(p.excess_blob_gas),
        parent_beacon_block_root: Some(env.parent_beacon_block_root),
        requests_hash: Some(env.execution_requests.requests_hash()),
        block_access_list_hash: Some(keccak256(p.block_access_list)),
        slot_number: Some(p.slot_number),
    }
}

/// Checks the payload's block hash against the rebuilt header. On success the raw BAL bytes are
/// committed to by `payload.block_hash` (and, via the bid, by the proposer-signed block).
pub fn verify_payload(env: &Envelope<'_>) -> Verified {
    let header = rebuild_header(env);
    let computed_hash = header.hash_slow();
    Verified {
        ok: computed_hash == env.payload.block_hash,
        computed_hash,
        bal_hash: header.block_access_list_hash.unwrap_or_default(),
        header,
    }
}
