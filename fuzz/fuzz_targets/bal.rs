//! EIP-7928 block access lists from gossip or eth/71 peers.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if let Ok(bal) = skim_bal::decode(data) {
        let _ = skim_bal::BlockWrites::from_bal(&bal);
    }
    let _ = skim_bal::decode_verified(data, alloy_primitives::keccak256(data));
});
