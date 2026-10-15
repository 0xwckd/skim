//! eth2 req/resp framing from peers: chunked responses with context bytes, snappy payloads,
//! Status and MetaData.
#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let _ = skim_cl::rpc::split_context_chunks(data);
    let _ = skim_cl::rpc::decode_payload(data);
    let _ = skim_cl::rpc::decode_payload_len(data, 1 << 20);
    let _ = skim_cl::rpc::Status::decode(data);
    let _ = skim_cl::rpc::MetaData::decode(data);
});
