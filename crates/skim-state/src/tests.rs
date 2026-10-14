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
