use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Behav {
    Ok { ms: u64 },
    /// Session drops on the first request; the peer leaves the pool.
    Disconnect,
    Unavailable,
    Empty,
    Fail,
}

#[derive(Clone)]
struct Mock {
    peers: Arc<Mutex<Vec<PeerId>>>,
    behav: Arc<HashMap<PeerId, Behav>>,
    requests: Arc<AtomicUsize>,
    inflight: Arc<Mutex<HashMap<PeerId, (usize, usize)>>>, // (now, max)
}

fn pid(n: u8) -> PeerId {
    PeerId::repeat_byte(n)
}

impl Mock {
    fn new(peers: &[(u8, Behav)]) -> Self {
        Self {
            peers: Arc::new(Mutex::new(peers.iter().map(|(n, _)| pid(*n)).collect())),
            behav: Arc::new(peers.iter().map(|(n, b)| (pid(*n), *b)).collect()),
            requests: Default::default(),
            inflight: Default::default(),
        }
    }

    async fn serve<R>(&self, peer: PeerId, ok: impl FnOnce() -> R, unavailable: R, empty: R) -> Result<R, TransportError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        {
            let mut m = self.inflight.lock();
            let e = m.entry(peer).or_default();
            e.0 += 1;
            e.1 = e.1.max(e.0);
        }
        let r = match self.behav[&peer] {
            Behav::Ok { ms } => {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(ok())
            }
            Behav::Disconnect => {
                self.peers.lock().retain(|p| *p != peer);
                Err(TransportError::Disconnected)
            }
            Behav::Unavailable => Ok(unavailable),
            Behav::Empty => Ok(empty),
            Behav::Fail => Err(TransportError::Timeout),
        };
        self.inflight.lock().get_mut(&peer).unwrap().0 -= 1;
        r
    }
}

fn acct() -> AccountState {
    AccountState { nonce: 1, balance: U256::from(7), storage_root: B256::ZERO, code_hash: B256::ZERO }
}

impl Transport for Mock {
    fn snap_peers(&self) -> Vec<PeerId> {
        self.peers.lock().clone()
    }
    async fn account(&self, peer: PeerId, _: B256, _: Address) -> Result<AccountAnswer, TransportError> {
        self.serve(peer, || AccountAnswer::Found(Some(acct())), AccountAnswer::Unavailable, AccountAnswer::Unavailable).await
    }
    async fn storage(&self, peer: PeerId, _: B256, _: Address, slot: U256) -> Result<SlotAnswer, TransportError> {
        self.serve(peer, || SlotAnswer::Found(slot + U256::from(1)), SlotAnswer::EmptyNoProof, SlotAnswer::EmptyNoProof).await
    }
    async fn codes(&self, peer: PeerId, hashes: Vec<B256>) -> Result<Vec<Bytes>, TransportError> {
        // Code whose keccak is the requested hash can't be faked; serve the preimages we know.
        let have: Vec<Bytes> = hashes
            .iter()
            .filter_map(|h| CODES.iter().find(|c| keccak256(c) == *h))
            .map(|c| Bytes::from_static(c))
            .collect();
        let (none, none2): (Vec<Bytes>, Vec<Bytes>) = (Vec::new(), Vec::new());
        let r = self.serve(peer, move || have, none, none2).await;
        r
    }
}

static CODES: [&[u8]; 10] = [b"c0", b"c1", b"c2", b"c3", b"c4", b"c5", b"c6", b"c7", b"c8", b"c9"];

fn slot(i: u64) -> Key {
    Key::Storage { root: B256::ZERO, address: Address::ZERO, slot: U256::from(i) }
}

fn cfg() -> Config {
    Config { hedge_after: Duration::from_secs(10), deadline: Duration::from_secs(5), ..Default::default() }
}

#[tokio::test]
async fn dedups_concurrent_waiters() {
    let m = Mock::new(&[(1, Behav::Ok { ms: 30 })]);
    let s = Scheduler::new(m.clone(), cfg());
    let k = [slot(1)];
    let (a, b) = tokio::join!(s.fetch(&k), s.fetch(&k));
    assert_eq!(a[0], Ok(Value::Storage(U256::from(2))));
    assert_eq!(a, b);
    assert_eq!(m.requests.load(Ordering::SeqCst), 1);
    assert_eq!(s.stats().dedup_hits, 1);
}

#[tokio::test]
async fn disconnect_requeues_to_other_peer() {
    let m = Mock::new(&[(1, Behav::Disconnect), (2, Behav::Ok { ms: 5 })]);
    let s = Scheduler::new(m, cfg());
    let keys: Vec<_> = (0..20).map(slot).collect();
    let r = s.fetch(&keys).await;
    assert!(r.iter().all(|x| x.is_ok()), "{r:?}");
    assert_eq!(s.stats().failed, 0);
    assert!(s.stats().disconnects >= 1);
}

#[tokio::test]
async fn respects_per_peer_inflight_cap() {
    let m = Mock::new(&[(1, Behav::Ok { ms: 20 }), (2, Behav::Ok { ms: 20 })]);
    let s = Scheduler::new(m.clone(), Config { per_peer_inflight: 4, ..cfg() });
    let keys: Vec<_> = (0..64).map(slot).collect();
    assert!(s.fetch(&keys).await.iter().all(|x| x.is_ok()));
    for (_, (_, max)) in m.inflight.lock().iter() {
        assert!(*max <= 4, "max in flight {max}");
    }
}

#[tokio::test]
async fn unavailable_root_moves_to_other_peer() {
    let m = Mock::new(&[(1, Behav::Unavailable), (2, Behav::Ok { ms: 5 })]);
    let s = Scheduler::new(m, cfg());
    let k = Key::Account { root: B256::ZERO, address: Address::ZERO };
    assert_eq!(s.fetch(&[k]).await[0], Ok(Value::Account(Some(acct()))));
}

#[tokio::test]
async fn batches_code_hashes() {
    let m = Mock::new(&[(1, Behav::Ok { ms: 5 })]);
    let s = Scheduler::new(m.clone(), cfg());
    let keys: Vec<_> = CODES.iter().map(|c| Key::Code(keccak256(c))).collect();
    let r = s.fetch(&keys).await;
    assert!(r.iter().all(|x| matches!(x, Ok(Value::Code(_)))), "{r:?}");
    assert_eq!(m.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn hedges_slow_peer() {
    let m = Mock::new(&[(1, Behav::Ok { ms: 2000 }), (2, Behav::Ok { ms: 5 })]);
    // Peer 1 gets the first request (both unmeasured, tie broken arbitrarily), so force it:
    // with a single key one of them is picked; either way the call must finish quickly.
    let s = Scheduler::new(m, Config { hedge_after: Duration::from_millis(100), ..cfg() });
    let t = Instant::now();
    assert!(s.fetch(&[slot(3)]).await[0].is_ok());
    assert!(t.elapsed() < Duration::from_millis(1000), "took {:?}", t.elapsed());
}

#[tokio::test]
async fn empty_quorum_resolves_zero() {
    let m = Mock::new(&[(1, Behav::Empty), (2, Behav::Empty)]);
    let s = Scheduler::new(m, cfg());
    assert_eq!(s.fetch(&[slot(9)]).await[0], Ok(Value::Storage(U256::ZERO)));
}

#[tokio::test]
async fn exhausts_when_every_peer_fails() {
    let m = Mock::new(&[(1, Behav::Fail), (2, Behav::Fail)]);
    let s = Scheduler::new(m, Config { max_attempts: 3, ..cfg() });
    assert_eq!(s.fetch(&[slot(1)]).await[0], Err(FetchError::Exhausted(3)));
}

#[tokio::test]
async fn waits_for_peers_then_deadline() {
    let m = Mock::new(&[]);
    let s = Scheduler::new(m, Config { deadline: Duration::from_millis(200), ..cfg() });
    assert_eq!(s.fetch(&[slot(1)]).await[0], Err(FetchError::Deadline));
}

#[tokio::test]
async fn queueing_for_capacity_is_not_a_deadline() {
    // 40 keys, one peer, 2 in flight, 50 ms each: ~1 s of queueing against a 300 ms deadline.
    let m = Mock::new(&[(1, Behav::Ok { ms: 50 })]);
    let s = Scheduler::new(m, Config { per_peer_inflight: 2, deadline: Duration::from_millis(300), ..cfg() });
    let keys: Vec<_> = (0..40).map(slot).collect();
    assert!(s.fetch(&keys).await.iter().all(|x| x.is_ok()));
}
