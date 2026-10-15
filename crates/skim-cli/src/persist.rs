//! Restart persistence for `skim run` (see `skim_state::persist`).
//!
//! - **Node key:** generated once and reused, so peers recognise a restarted node.
//! - **Peers:** good EL peers are saved every minute and re-dialed on start.
//! - **State snapshot:** subscribed keys' values at a block, saved every 4 blocks and on
//!   shutdown. On start it is verified and replayed to the head before use:
//!   headers from the snapshot block to the head must link by hash (both ends included), each
//!   block's BAL must match its header, and the writes are replayed in order. If anything
//!   doesn't check out, the snapshot is discarded and state is fetched as without it.

use std::{sync::Arc, time::Instant};

use alloy_primitives::B256;
use skim_el::{ElNet, Peer};
use skim_state::{BlockRef, BlockWrites, Retention, Store, persist::{Persist, Saved}};

/// Longest gap a snapshot is replayed across (each block costs a header and a BAL download).
const MAX_REPLAY: u64 = 8192;

pub fn node_key(p: &Persist) -> anyhow::Result<[u8; 32]> {
    if let Some(k) = p.get_meta("node_key")?
        && let Ok(k) = <[u8; 32]>::try_from(k.as_slice())
    {
        return Ok(k);
    }
    let mut k = [0u8; 32];
    std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut k)?;
    p.set_meta("node_key", &k)?;
    Ok(k)
}

pub fn load_peers(p: &Persist) -> Vec<(Peer, std::net::SocketAddr)> {
    let Ok(Some(raw)) = p.get_meta("peers") else { return Vec::new() };
    String::from_utf8_lossy(&raw)
        .lines()
        .filter_map(|l| {
            let (id, addr) = l.split_once('@')?;
            Some((id.parse().ok()?, addr.parse().ok()?))
        })
        .collect()
}

pub fn spawn_peer_saver(p: Arc<Persist>, net: ElNet) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let peers = net.good_peers();
            if peers.is_empty() {
                continue;
            }
            let text: String = peers.iter().map(|(id, a)| format!("{id}@{a}\n")).collect();
            if let Err(e) = p.set_meta("peers", text.as_bytes()) {
                tracing::warn!("saving peers: {e}");
            }
        }
    });
}

/// Saves the subscribed state at `head` (off the async runtime).
pub fn save_now(p: &Arc<Persist>, store: &Store, head: &BlockRef) {
    let (values, code) = store.subscribed_at(head);
    if values.is_empty() {
        return;
    }
    let (p, number, hash) = (p.clone(), head.number, head.hash);
    let t = std::thread::spawn(move || p.save(number, hash, &values, &code));
    // Shutdown needs the write finished; per-block saves are small and quick too.
    match t.join() {
        Ok(Err(e)) => tracing::warn!("saving restart snapshot: {e}"),
        Err(_) => tracing::warn!("saving restart snapshot: writer panicked"),
        Ok(Ok(())) => {}
    }
}

/// Verifies the snapshot against the chain up to `head` and installs it. Never partially:
/// either every check passes and all keys are inserted, or nothing is.
pub async fn restore(store: Arc<Store>, net: ElNet, saved: Saved, head: BlockRef) {
    let t = Instant::now();
    match restore_inner(&store, &net, &saved, &head).await {
        Ok(n) => tracing::info!(keys = n, from = saved.number, to = head.number, ms = t.elapsed().as_millis() as u64,
            "restart snapshot verified and replayed to the head"),
        Err(e) => tracing::warn!(from = saved.number, "restart snapshot discarded ({e}); fetching state as usual"),
    }
}

async fn restore_inner(store: &Store, net: &ElNet, saved: &Saved, head: &BlockRef) -> anyhow::Result<usize> {
    use reth_eth_wire_types::BlockHashOrNumber;
    if saved.number >= head.number {
        // The snapshot is at or after the first block we applied: check it's on this chain.
        let b = store.block(saved.number).ok_or_else(|| anyhow::anyhow!("block {} not in the window", saved.number))?;
        anyhow::ensure!(b.hash == saved.hash, "snapshot block {} is not on the current chain", saved.number);
        return install(store, &b, &saved.values, &saved.code);
    }
    let gap = head.number - saved.number;
    anyhow::ensure!(gap <= MAX_REPLAY, "snapshot is {gap} blocks old (max {MAX_REPLAY})");
    // EL peers come up after CL gossip; give them time.
    let deadline = Instant::now() + std::time::Duration::from_secs(300);
    let mut writes: Vec<BlockWrites> = Vec::with_capacity(gap as usize);
    let mut prev = saved.hash;
    let mut next = saved.number + 1;
    while next <= head.number {
        anyhow::ensure!(Instant::now() < deadline, "no EL peer served headers + BALs from {next}");
        let peers: Vec<Peer> = net.peers().into_iter().filter(|(_, p)| p.eth_version >= 71).map(|(id, _)| id).collect();
        let mut progressed = false;
        for peer in peers {
            let count = (head.number - next + 1).min(64);
            let Ok(hs) = net.headers(peer, BlockHashOrNumber::Number(next), count).await.0 else { continue };
            if hs.is_empty() || hs[0].number != next {
                continue;
            }
            let hashes: Vec<B256> = hs.iter().map(|h| h.hash_slow()).collect();
            // Chain link: each header's parent is the previous hash (the snapshot block first).
            if hs.iter().zip(std::iter::once(prev).chain(hashes.iter().copied())).any(|(h, p)| h.parent_hash != p) {
                continue;
            }
            let Ok(bals) = net.bals(peer, hashes.clone()).await.0 else { continue };
            let mut ok = 0;
            for (h, raw) in hs.iter().zip(bals.0) {
                let Some(raw) = raw else { break };
                let Ok(bal) = skim_bal::decode_verified(&raw, h.block_access_list_hash.unwrap_or_default()) else { break };
                writes.push(BlockWrites::from_bal(&bal));
                ok += 1;
            }
            if ok == 0 {
                continue;
            }
            prev = hashes[ok - 1];
            next += ok as u64;
            progressed = true;
            break;
        }
        if !progressed {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }
    anyhow::ensure!(prev == head.hash, "replayed chain ends at {prev}, not the head {}", head.hash);
    let (values, code) = skim_state::persist::replay(saved, &writes);
    install(store, head, &values, &code)
}

fn install(store: &Store, at: &BlockRef, values: &[(skim_bal::StateKey, alloy_primitives::U256)], code: &[(B256, alloy_primitives::Bytes)]) -> anyhow::Result<usize> {
    for (_, c) in code {
        store.insert_code(c.clone());
    }
    let mut n = 0;
    for (k, v) in values {
        // `at` must still be in the window; insert_base replays any later blocks itself.
        store.insert_base_with(at, *k, *v, Retention::Subscribed)?;
        n += 1;
    }
    Ok(n)
}
