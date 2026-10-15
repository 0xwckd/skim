//! Merkle proofs from SNAP peers: root (32) ++ key (32) ++ nodes, each node prefixed by a
//! 2-byte length. Must answer Ok/Err without panicking.
#![no_main]
use alloy_primitives::{B256, Bytes};

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.len() < 64 {
        return;
    }
    let root = B256::from_slice(&data[..32]);
    let key = B256::from_slice(&data[32..64]);
    let mut nodes = Vec::new();
    let mut rest = &data[64..];
    while rest.len() >= 2 {
        let n = (u16::from_be_bytes([rest[0], rest[1]]) as usize).min(rest.len() - 2);
        nodes.push(Bytes::copy_from_slice(&rest[2..2 + n]));
        rest = &rest[2 + n..];
    }
    let _ = skim_snap::proof::lookup(root, key, &nodes);
    // The leaves path used for proof-less small tries.
    let leaves = nodes.iter().map(|n| (alloy_primitives::keccak256(n), n.clone())).collect();
    let _ = skim_snap::proof::root_of(leaves);  // must be Some or None, never a panic
});
