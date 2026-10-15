use super::*;

const A: Address = Address::repeat_byte(0xaa);

fn blk(n: u64, branch: u8) -> BlockRef {
    let h = |n: u64, b: u8| {
        let mut x = [0u8; 32];
        x[..8].copy_from_slice(&n.to_be_bytes());
        x[31] = b;
        B256::from(x)
    };
    BlockRef { number: n, hash: h(n, branch), parent_hash: h(n.wrapping_sub(1), if n > 10 { branch } else { 0 }), state_root: B256::ZERO, timestamp: n * 12 }
}

fn slot(i: u64) -> StateKey {
    StateKey::Storage(A, U256::from(i))
}

fn writes(w: &[(StateKey, u64)]) -> Arc<BlockWrites> {
    Arc::new(BlockWrites {
        writes: w.iter().map(|(k, v)| (*k, U256::from(*v))).collect(),
        touched: vec![A],
        ..Default::default()
    })
}

fn known(s: &Snapshot, k: StateKey) -> Option<u64> {
    match s.get(&k).unwrap() {
        StateValue::Known(v) => Some(v.to()),
        StateValue::Unknown => None,
    }
}

#[test]
fn follows_writes_and_unknown_is_not_zero() {
    let s = Store::new(64);
    s.watch_address(A);
    s.apply_block(blk(1, 0), writes(&[])).unwrap();
    let r = s.apply_block(blk(2, 0), writes(&[(slot(1), 10)])).unwrap();
    assert_eq!(*r.changed_keys, vec![slot(1)]);
    s.apply_block(blk(3, 0), writes(&[(slot(1), 30)])).unwrap();

    assert_eq!(known(&s.snapshot(&blk(1, 0)).unwrap(), slot(1)), None);
    assert_eq!(known(&s.snapshot(&blk(2, 0)).unwrap(), slot(1)), Some(10));
    assert_eq!(known(&s.snapshot(&blk(3, 0)).unwrap(), slot(1)), Some(30));
    assert_eq!(known(&s.snapshot(&blk(3, 0)).unwrap(), slot(2)), None);
}

#[test]
fn rejects_non_child() {
    let s = Store::new(64);
    s.apply_block(blk(1, 0), writes(&[])).unwrap();
    assert!(matches!(s.apply_block(blk(3, 0), writes(&[])), Err(StateError::NotChild(..))));
}

#[test]
fn untracked_writes_are_ignored() {
    let s = Store::new(64);
    s.apply_block(blk(1, 0), writes(&[(slot(1), 5)])).unwrap();
    assert_eq!(known(&s.snapshot(&blk(1, 0)).unwrap(), slot(1)), None);
}

#[test]
fn base_insert_replays_later_writes_and_never_reaches_back() {
    let s = Store::new(64);
    s.apply_block(blk(1, 0), writes(&[])).unwrap();
    s.apply_block(blk(2, 0), writes(&[])).unwrap();
    s.apply_block(blk(3, 0), writes(&[(slot(7), 50)])).unwrap(); // untracked yet: indexed only
    s.apply_block(blk(4, 0), writes(&[])).unwrap();

    // A SNAP fetch at block 2 returns 20 (arriving after block 3 wrote 50).
    s.insert_base(&blk(2, 0), slot(7), U256::from(20)).unwrap();

    assert_eq!(known(&s.snapshot(&blk(1, 0)).unwrap(), slot(7)), None);
    assert_eq!(known(&s.snapshot(&blk(2, 0)).unwrap(), slot(7)), Some(20));
    assert_eq!(known(&s.snapshot(&blk(3, 0)).unwrap(), slot(7)), Some(50));
    assert_eq!(known(&s.snapshot(&blk(4, 0)).unwrap(), slot(7)), Some(50));

    // Tracked now: new writes are recorded.
    s.apply_block(blk(5, 0), writes(&[(slot(7), 60)])).unwrap();
    assert_eq!(known(&s.snapshot(&blk(5, 0)).unwrap(), slot(7)), Some(60));
}

#[test]
fn bal_write_at_base_block_wins() {
    let s = Store::new(64);
    s.watch_address(A);
    s.apply_block(blk(1, 0), writes(&[(slot(1), 9)])).unwrap();
    s.insert_base(&blk(1, 0), slot(1), U256::from(1)).unwrap(); // stale fetch
    assert_eq!(known(&s.snapshot(&blk(1, 0)).unwrap(), slot(1)), Some(9));
}

#[test]
fn reorg_rolls_back_and_invalidates_snapshots() {
    let s = Store::new(64);
    s.watch_address(A);
    for n in 10..=12 {
        s.apply_block(blk(n, 0), writes(&[(slot(1), n)])).unwrap();
    }
    let old = s.snapshot(&blk(12, 0)).unwrap();
    s.rollback_to(11);
    assert!(matches!(old.get(&slot(1)), Err(StateError::Reorged { .. })));
    let mut b = blk(12, 1);
    b.parent_hash = blk(11, 0).hash;
    s.apply_block(b, writes(&[(slot(1), 99)])).unwrap();
    assert_eq!(known(&s.snapshot(&b).unwrap(), slot(1)), Some(99));
    assert_eq!(known(&s.snapshot(&blk(11, 0)).unwrap(), slot(1)), Some(11));
}

#[test]
fn gc_collapses_old_versions_into_a_base() {
    let s = Store::new(4);
    s.watch_address(A);
    for n in 1..=20 {
        s.apply_block(blk(n, 0), writes(&[(slot(1), n)])).unwrap();
    }
    assert!(matches!(s.snapshot(&blk(10, 0)), Err(StateError::OutOfWindow(10))));
    assert_eq!(known(&s.snapshot(&blk(20, 0)).unwrap(), slot(1)), Some(20));
    assert_eq!(known(&s.snapshot(&blk(17, 0)).unwrap(), slot(1)), Some(17));
    assert!(s.inner.read().versions[&slot(1)].len() <= 5);
}

#[tokio::test]
async fn emits_state_ready() {
    let s = Store::new(64);
    s.watch_address(A);
    let mut rx = s.subscribe();
    s.apply_block(blk(1, 0), writes(&[(slot(3), 1)])).unwrap();
    let ev = rx.recv().await.unwrap();
    assert_eq!(ev.block.number, 1);
    assert_eq!(*ev.changed_keys, vec![slot(3)]);
}

#[test]
fn on_demand_keys_stay_current_during_ttl_then_expire() {
    let s = Store::new(64);
    s.apply_block(blk(1, 0), writes(&[])).unwrap();
    s.insert_base_with(&blk(1, 0), slot(4), U256::from(1), Retention::Ttl(2)).unwrap();
    // Within the TTL, BAL writes keep it current (no stale reuse).
    s.apply_block(blk(2, 0), writes(&[(slot(4), 2)])).unwrap();
    assert_eq!(known(&s.snapshot(&blk(2, 0)).unwrap(), slot(4)), Some(2));
    s.apply_block(blk(3, 0), writes(&[])).unwrap();
    assert_eq!(known(&s.snapshot(&blk(3, 0)).unwrap(), slot(4)), Some(2));
    // Past head+TTL it is evicted entirely: Unknown again, not stale.
    s.apply_block(blk(4, 0), writes(&[])).unwrap();
    assert_eq!(known(&s.snapshot(&blk(4, 0)).unwrap(), slot(4)), None);
    assert_eq!(known(&s.snapshot(&blk(2, 0)).unwrap(), slot(4)), None);
    // And no longer tracked: a later write is ignored.
    s.apply_block(blk(5, 0), writes(&[(slot(4), 5)])).unwrap();
    assert_eq!(known(&s.snapshot(&blk(5, 0)).unwrap(), slot(4)), None);
    assert_eq!(s.sizes().temp_keys, 0);
}

#[test]
fn ttl_never_downgrades_a_subscription_and_promotion_sticks() {
    let s = Store::new(64);
    s.apply_block(blk(1, 0), writes(&[])).unwrap();
    s.watch_key(slot(1));
    s.insert_base_with(&blk(1, 0), slot(1), U256::from(7), Retention::Ttl(1)).unwrap();
    s.insert_base_with(&blk(1, 0), slot(2), U256::from(8), Retention::Ttl(1)).unwrap();
    s.watch_key(slot(2)); // promote
    for n in 2..=6 {
        s.apply_block(blk(n, 0), writes(&[])).unwrap();
    }
    let snap = s.snapshot(&blk(6, 0)).unwrap();
    assert_eq!(known(&snap, slot(1)), Some(7));
    assert_eq!(known(&snap, slot(2)), Some(8));
    assert!(s.is_subscribed(&slot(2)));
}

#[test]
fn archived_keys_answer_blocks_outside_the_window() {
    let d = tempfile::tempdir().unwrap();
    let ar = archive::Archive::open(d.path().join("a.redb")).unwrap();
    let s = Store::with_archive(4, ar).unwrap();
    s.apply_block(blk(1, 0), writes(&[])).unwrap();
    s.insert_base(&blk(1, 0), slot(1), U256::from(100)).unwrap();
    s.insert_base(&blk(1, 0), slot(2), U256::from(200)).unwrap();
    s.archive_key(slot(1)); // slot 2 is subscribed but not archived
    for n in 2..=20 {
        let w: Vec<(StateKey, u64)> = if n % 5 == 0 { vec![(slot(1), n), (slot(2), n)] } else { vec![] };
        s.apply_block(blk(n, 0), writes(&w)).unwrap();
    }
    // Block 3 is far outside the 4-block window: served from the archive.
    let old = s.snapshot(&blk(3, 0)).unwrap();
    assert!(old.is_archived());
    assert_eq!(known(&old, slot(1)), Some(100));
    assert_eq!(known(&old, slot(2)), None); // not archived: never guessed
    assert_eq!(known(&s.snapshot(&blk(12, 0)).unwrap(), slot(1)), Some(10));
    assert_eq!(known(&s.snapshot(&blk(19, 0)).unwrap(), slot(1)), Some(15));
    assert_eq!(known(&s.snapshot(&blk(20, 0)).unwrap(), slot(1)), Some(20));
    assert_eq!(s.archive_errors(), 0);
    assert_eq!(s.block_hash(3), Some(blk(3, 0).hash));
}

#[test]
fn archive_follows_reorgs() {
    let d = tempfile::tempdir().unwrap();
    let s = Store::with_archive(64, archive::Archive::open(d.path().join("a.redb")).unwrap()).unwrap();
    s.apply_block(blk(10, 0), writes(&[])).unwrap();
    s.insert_base(&blk(10, 0), slot(1), U256::from(1)).unwrap();
    s.archive_key(slot(1));
    s.apply_block(blk(11, 0), writes(&[(slot(1), 11)])).unwrap();
    s.apply_block(blk(12, 0), writes(&[(slot(1), 12)])).unwrap();
    s.rollback_to(11);
    let mut b = blk(12, 1);
    b.parent_hash = blk(11, 0).hash;
    s.apply_block(b, writes(&[(slot(1), 99)])).unwrap();
    let ar = s.archive().unwrap();
    assert_eq!(ar.get(&slot(1), 12).unwrap(), archive::ArchiveRead::Value(U256::from(99)));
    assert_eq!(ar.get(&slot(1), 11).unwrap(), archive::ArchiveRead::Value(U256::from(11)));
}

#[test]
fn code_is_cached_only_for_tracked_accounts() {
    let s = Store::new(64);
    let b = Address::repeat_byte(0xbb);
    s.watch_key(StateKey::CodeHash(A));
    let (ca, cb) = (Bytes::from_static(b"code-a"), Bytes::from_static(b"code-b"));
    let w = BlockWrites {
        writes: vec![
            (StateKey::CodeHash(A), U256::from_be_bytes(keccak256(&ca).0)),
            (StateKey::CodeHash(b), U256::from_be_bytes(keccak256(&cb).0)),
        ],
        code: vec![(keccak256(&ca), ca.clone()), (keccak256(&cb), cb)],
        ..Default::default()
    };
    s.apply_block(blk(1, 0), Arc::new(w)).unwrap();
    assert_eq!(s.code(&keccak256(&ca)), Some(ca));
    assert_eq!(s.sizes().code, 1); // the untracked deployment isn't cached
}

#[test]
fn blockhash_ring_outlives_a_short_window() {
    // Review finding: with the window holding the head, H-256 was gone after GC, and an unknown
    // hash read as zero. The ring keeps every hash BLOCKHASH can ask for.
    let s = Store::new(16);
    for n in 1..=300 {
        s.apply_block(blk(n, 0), writes(&[])).unwrap();
    }
    assert_eq!(s.block_hash(300 - 256), Some(blk(300 - 256, 0).hash));
    assert_eq!(s.block_hash(299), Some(blk(299, 0).hash));
    assert_eq!(s.block_hash(300 - 258), None); // out of BLOCKHASH range: unknown, not zero
}

#[test]
fn ancestor_hashes_are_accepted_only_when_hash_linked() {
    let s = Store::new(16);
    for n in 100..=105 {
        s.apply_block(blk(n, 0), writes(&[])).unwrap();
    }
    // The ring knows 99 (parent of 100) but nothing older.
    assert_eq!(s.oldest_known_hash(), Some((99, blk(99, 0).hash)));
    assert_eq!(s.block_hash(98), None);
    let h = |n: u64| blk(n, 0).hash;
    let forged = B256::repeat_byte(0xee);
    let chain = [(99, h(99), h(98)), (98, h(98), h(97)), (97, forged, h(96)), (96, h(96), h(95))];
    assert_eq!(s.insert_ancestor_hashes(&chain), 1); // 98 links; 97 doesn't, so the walk stops
    assert_eq!(s.block_hash(98), Some(h(98)));
    assert_eq!(s.block_hash(97), None);
    assert_eq!(s.block_hash(96), None);
}

#[test]
fn subscribed_key_limit_is_enforced_at_admission() {
    // Review finding: the limit only trimmed bookkeeping; fetched keys were already subscribed.
    let s = Store::new(64);
    s.set_max_subscribed(2);
    s.apply_block(blk(1, 0), writes(&[])).unwrap();
    assert!(s.watch_key(slot(1)));
    s.insert_base(&blk(1, 0), slot(2), U256::from(2)).unwrap(); // Subscribed: admitted (2/2)
    assert!(!s.watch_key(slot(3))); // over the limit: refused
    s.insert_base(&blk(1, 0), slot(4), U256::from(4)).unwrap(); // Subscribed fetch: kept on demand
    assert!(s.is_subscribed(&slot(2)));
    assert!(!s.is_subscribed(&slot(3)));
    assert!(!s.is_subscribed(&slot(4)));
    assert_eq!(s.sizes().watched_keys, 2);
    assert_eq!(s.sizes().admissions_refused, 2);
    assert!(s.watch_key(slot(1))); // already subscribed: always fine
    // The refused key is still correct while cached, then expires instead of growing the store.
    assert_eq!(known(&s.snapshot(&blk(1, 0)).unwrap(), slot(4)), Some(4));
    for n in 2..=10 {
        s.apply_block(blk(n, 0), writes(&[])).unwrap();
    }
    assert_eq!(s.sizes().temp_keys, 0);
}

#[test]
fn bytecode_is_dropped_with_its_last_account() {
    let s = Store::new(64);
    s.apply_block(blk(1, 0), writes(&[])).unwrap();
    let c = Bytes::from_static(b"some-code");
    let h = s.insert_code(c.clone());
    s.insert_base_with(&blk(1, 0), StateKey::CodeHash(A), U256::from_be_bytes(h.0), Retention::Ttl(2)).unwrap();
    s.apply_block(blk(2, 0), writes(&[])).unwrap();
    assert_eq!(s.code(&h), Some(c));
    for n in 3..=6 {
        s.apply_block(blk(n, 0), writes(&[])).unwrap();
    }
    assert_eq!(s.code(&h), None);
    assert_eq!(s.sizes().code, 0);
}
