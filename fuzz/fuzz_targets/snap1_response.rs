//! snap/1 responses from Geth peers, decoded by skim's own satellite protocol.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = skim_el::fuzz_snap1_response(data);
});
