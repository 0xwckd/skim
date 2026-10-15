//! Gossiped / requested `SignedExecutionPayloadEnvelope`: parse, rebuild + hash the header,
//! decode the BAL. Must never panic on any input.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if let Ok(env) = skim_cl::ssz::Envelope::parse(data) {
        let _ = skim_cl::verify::verify_payload(&env);
        if let Ok(bal) = skim_bal::decode(env.payload.block_access_list) {
            let _ = skim_bal::BlockWrites::from_bal(&bal);
        }
    }
});
