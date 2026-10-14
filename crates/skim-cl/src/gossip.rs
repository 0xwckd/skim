//! eth2 gossipsub specifics: topic names, snappy (block format) payloads, and message ids
//! (consensus-specs phase0/altair p2p-interface: `message-id`).

use libp2p::gossipsub::{self, DataTransform, MessageId, RawMessage, TopicHash};
use sha2::{Digest, Sha256};

/// `MESSAGE_DOMAIN_VALID_SNAPPY`.
pub const DOMAIN_VALID_SNAPPY: [u8; 4] = [1, 0, 0, 0];
/// `MESSAGE_DOMAIN_INVALID_SNAPPY`.
pub const DOMAIN_INVALID_SNAPPY: [u8; 4] = [0, 0, 0, 0];
/// `MAX_PAYLOAD_SIZE` (Fulu+: 10 MiB), the bound on both compressed and uncompressed size.
pub const MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;

pub fn topic(fork_digest: [u8; 4], name: &str) -> String {
    format!("/eth2/{}/{name}/ssz_snappy", hex::encode(fork_digest))
}

/// Altair+ message id: `SHA256(domain ++ uint64_le(len(topic)) ++ topic ++ data)[:20]`, where
/// `data` is the snappy-decompressed payload (valid domain) or the raw payload (invalid domain).
pub fn message_id(topic: &str, data: &[u8], valid_snappy: bool) -> [u8; 20] {
    let mut h = Sha256::new();
    h.update(if valid_snappy { DOMAIN_VALID_SNAPPY } else { DOMAIN_INVALID_SNAPPY });
    h.update((topic.len() as u64).to_le_bytes());
    h.update(topic.as_bytes());
    h.update(data);
    let mut out = [0u8; 20];
    out.copy_from_slice(&h.finalize()[..20]);
    out
}

/// Message id for a raw gossip payload as received (decompresses if possible).
pub fn message_id_raw(topic: &str, raw: &[u8]) -> [u8; 20] {
    match decompress(raw) {
        Ok(d) => message_id(topic, &d, true),
        Err(_) => message_id(topic, raw, false),
    }
}

/// Snappy block-format decompression with the eth2 size bound checked before allocating.
pub fn decompress(raw: &[u8]) -> std::io::Result<Vec<u8>> {
    let len = snap::raw::decompress_len(raw)?;
    if len > MAX_PAYLOAD_SIZE {
        return Err(std::io::Error::other("decompressed gossip payload too large"));
    }
    snap::raw::Decoder::new().decompress_vec(raw).map_err(std::io::Error::other)
}

/// Decompresses inbound messages so that the message-id function and the application both see
/// SSZ bytes; gossipsub keeps the raw form for forwarding. Messages that fail to decompress are
/// dropped as invalid (as Lighthouse does).
#[derive(Clone, Default)]
pub struct SnappyTransform;

impl DataTransform for SnappyTransform {
    fn inbound_transform(&self, raw: RawMessage) -> std::io::Result<gossipsub::Message> {
        Ok(gossipsub::Message {
            source: raw.source,
            data: decompress(&raw.data)?,
            sequence_number: raw.sequence_number,
            topic: raw.topic,
        })
    }

    fn outbound_transform(&self, _topic: &TopicHash, data: Vec<u8>) -> std::io::Result<Vec<u8>> {
        snap::raw::Encoder::new().compress_vec(&data).map_err(std::io::Error::other)
    }
}

/// `message_id_fn` for gossipsub: the message has already been decompressed by the transform.
pub fn gossipsub_message_id(m: &gossipsub::Message) -> MessageId {
    MessageId::from(message_id(m.topic.as_str(), &m.data, true).to_vec())
}

mod hex {
    pub fn encode(b: [u8; 4]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }
}
