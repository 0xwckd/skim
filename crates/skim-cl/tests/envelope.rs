//! Real Sepolia gossip captured by `skim probe-cl --capture-dir` (slot 11356920, EL block
//! 11916226). Expected values were cross-checked against a local Nethermind
//! (`eth_getBlockByNumber`: hash, stateRoot, blockAccessListHash, slotNumber, tx count) and the
//! message ids against Python `hashlib` over the same bytes.

use alloy_primitives::{B256, b256};
use skim_cl::{
    gossip,
    ssz::{BlockInfo, Envelope},
    verify,
};

const ENVELOPE: &[u8] = include_bytes!("fixtures/sepolia-11356920.envelope.ssz");
const BLOCK: &[u8] = include_bytes!("fixtures/sepolia-11356920.block.ssz");
const BLOCK_HASH: B256 = b256!("72ca458b6f4e8d971c9bf65e6de5908b55f31d0b49bf91d8f7feca5254feb99b");

#[test]
fn parses_envelope_fields() {
    let env = Envelope::parse(ENVELOPE).unwrap();
    let p = &env.payload;
    assert_eq!(p.slot_number, 11356920);
    assert_eq!(p.block_number, 11916226);
    assert_eq!(p.block_hash, BLOCK_HASH);
    assert_eq!(p.parent_hash, b256!("74947c7a81606f07c354b78bd32474a5433d04eaa73b996af5716bf458a58c3f"));
    assert_eq!(p.state_root, b256!("b5754930b07618e2a46d8bf3364d19da0b975ada1b4f4ef5cd5fa2a533ec8164"));
    assert_eq!(p.timestamp, 1655733600 + 11356920 * 12);
    assert_eq!(p.transactions.len(), 107);
    assert_eq!(p.withdrawals.len(), 16);
    assert_eq!(p.block_access_list.len(), 35827);
    assert_eq!(env.builder_index, 0);
    assert_eq!(
        env.beacon_block_root,
        b256!("50e30991c9bd70d6046c604137a7e1d2dabe9156863c427b64cd3f4e3e72d9df")
    );
}

#[test]
fn rebuilt_header_matches_block_hash_and_bal_decodes() {
    let env = Envelope::parse(ENVELOPE).unwrap();
    let v = verify::verify_payload(&env);
    assert!(v.ok, "computed {} expected {}", v.computed_hash, env.payload.block_hash);
    assert_eq!(v.bal_hash, b256!("b0f26ce3d5fa85b3e922d370ef938465bc50375ce197410e2886f8127260e2c6"));
    // The commitment check and decode the state follower uses.
    let bal = skim_bal::decode_verified(env.payload.block_access_list, v.bal_hash).unwrap();
    assert_eq!(bal.len(), 284);
}

#[test]
fn tampered_bal_breaks_block_hash() {
    let mut raw = ENVELOPE.to_vec();
    let env = Envelope::parse(ENVELOPE).unwrap();
    // Flip the last byte of the BAL (located via the parsed slice, not a hard-coded offset).
    let off = env.payload.block_access_list.as_ptr() as usize - ENVELOPE.as_ptr() as usize;
    raw[off + env.payload.block_access_list.len() - 1] ^= 1;
    let env = Envelope::parse(&raw).unwrap();
    assert!(!verify::verify_payload(&env).ok);
}

#[test]
fn parses_block_and_bid_links_to_envelope() {
    let b = BlockInfo::parse(BLOCK).unwrap();
    assert_eq!(b.slot, 11356920);
    assert_eq!(b.proposer_index, 830);
    let bid = b.bid.unwrap();
    assert_eq!(bid.block_hash, BLOCK_HASH);
    assert_eq!(bid.slot, 11356920);
    assert_eq!(bid.builder_index, 0);
    let env = Envelope::parse(ENVELOPE).unwrap();
    assert_eq!(bid.parent_block_hash, env.payload.parent_hash);
    assert_eq!(bid.parent_block_root, env.parent_beacon_block_root);
}

#[test]
fn rejects_truncated_envelope() {
    assert!(Envelope::parse(&ENVELOPE[..ENVELOPE.len() / 2]).is_err());
    assert!(Envelope::parse(&ENVELOPE[..50]).is_err());
}

#[test]
fn message_ids() {
    let topic_env = gossip::topic([0x66, 0x9e, 0x6c, 0x11], "execution_payload");
    assert_eq!(topic_env, "/eth2/669e6c11/execution_payload/ssz_snappy");
    let topic_blk = gossip::topic([0x66, 0x9e, 0x6c, 0x11], "beacon_block");

    // Gossip carries snappy *block* format; the id is over the decompressed bytes.
    let wire = snap::raw::Encoder::new().compress_vec(ENVELOPE).unwrap();
    assert_eq!(gossip::decompress(&wire).unwrap(), ENVELOPE);
    assert_eq!(hex(&gossip::message_id_raw(&topic_env, &wire)), "63e188f687a72cbf0c81336e5eae9fbc1f5a2f1b");
    let wire = snap::raw::Encoder::new().compress_vec(BLOCK).unwrap();
    assert_eq!(hex(&gossip::message_id_raw(&topic_blk, &wire)), "edb5388854aca8ffeb50330a2a113c86bc8fede3");
    // Invalid snappy: invalid-snappy domain over the raw bytes.
    assert_eq!(
        hex(&gossip::message_id_raw(&topic_blk, &[0xff, 0xff, 0xff])),
        "3abd2b589e9efdcdff26012d857813e51b814fca"
    );
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
