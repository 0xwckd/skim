//! Gossiped `SignedBeaconBlock` (skim reads slot, roots and the bid from it).
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = skim_cl::ssz::BlockInfo::parse(data);
});
