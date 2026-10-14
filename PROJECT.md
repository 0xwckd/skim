# skim

**A P2P-backed Ethereum simulation client.**
skim follows recent execution state directly from the Ethereum P2P network. It runs EVM simulations locally through a native Rust API and a standard JSON-RPC endpoint. It needs no full node and no RPC provider.

| | |
|---|---|
| Status | Design v0.4, M0 in progress |
| Target | Ethereum Sepolia, Glamsterdam (active since 2026-10-06 13:53:36 UTC) |
| Language | Rust |
| Core dependencies | Reth networking crates (EL P2P), a CL gossip stack (TBD in M0), REVM |
| Date | 2026-10-14 |

---

## 1. Thesis

A full node gives fast local simulation. The cost is executing and storing the whole chain, even when an application only touches a small slice of state.

skim keeps only the state its users need and stays current by applying EIP-7928 Block Access Lists (BALs) instead of re-executing blocks.

**What it is:** local simulation with much lower memory, disk, CPU and operational cost than a full node, for workloads with a bounded, predictable working set.

**What it is not:** a faster alternative to an in-process Reth/REVM setup. Warm simulation speed should be comparable, not better.

### Latency targets by cache state

| State | Target |
|---|---|
| **Warm**: every key known locally | Close to a full node's in-process execution; zero network requests |
| **Partially cold**: a few keys missing, shallow dependency chain | Competitive with a hosted RPC provider (Infura) `eth_call` |
| **Fully cold**: new contracts, no peers yet, or deep dependency chains | Recover as fast as possible; no performance advantage promised |

Conceptual cold-call model (not measured):

```
T_skim  ≈ Σ_{i=1..k} T_snap_round(i) + (k+1)·T_revm
T_infura ≈ T_rpc_rtt + T_server_exec
```

`k` is the number of **sequential** fetch rounds. Independent misses collapse into one round (§7.2). `k` grows only along data dependencies: an address or slot key derived from a value not yet known. Examples:
- a proxy's implementation slot leading to that implementation's code
- an account's code hash leading to its code
- a pool address read from storage leading to that pool's slots

With `k = 1` and a nearby, responsive SNAP peer, skim can beat a hosted RPC. With `k ≈ 5`, it likely loses. M0 measures where the crossover sits.

**One narrow performance hypothesis, to be measured rather than promised:** under ePBS, a BAL-based client that receives execution-payload gossip might have usable post-state *before* a conventional node finishes executing the block.

### Three properties, kept distinct

- **Provider independence.** At runtime, every byte comes from Ethereum P2P. Reference RPC nodes are allowed only in test and benchmark harnesses.
- **Lightweight execution.** skim does not store or execute the whole chain.
- **Interface compatibility.** It offers a native Rust API, plus a JSON-RPC adapter for simulation methods so existing `cast`, viem, ethers and Foundry code works unchanged.

## 2. Scope

### Target workloads
- RFQ quote engines, intent solvers, DEX pool simulation, protocol-specific simulators: any repeated simulation over a known set of contracts and slots.
- Foundry fork mode (`anvil --fork-url`, `forge test --fork-url`) and `cast call` against recent blocks.
- Resource-constrained deployments that need local EVM execution without full-node infrastructure.

### Simulation only
skim is a simulation client. It does not:
- send transactions (`eth_sendRawTransaction`), run a TxPool, or track `pending` state
- serve receipts, logs or transaction lookups (`eth_getLogs`, `eth_getTransactionReceipt`, `eth_getTransactionByHash`)
- serve archive queries outside the retention window
- validate the chain trustlessly (see §10)
- reconstruct the full state trie or re-execute blocks

Simulated logs, which REVM produces for each simulation, *are* returned in simulation results.

### Non-goals for the initial release
- Beating full nodes on raw warm simulation speed
- Workloads with unbounded or unpredictable state access
- Acting as a security-critical wallet backend

## 3. Protocol basis

### 3.1 EIP-7928: Block-Level Access Lists
Each account entry carries `storage_changes`, `storage_reads`, `balance_changes`, `nonce_changes` and `code_changes`.
- Change values are **absolute post-values**, indexed by `block_access_index`:
  - `0`: pre-execution system calls
  - `1..n`: transactions
  - `n+1`: post-execution calls and withdrawals
- `storage_reads` carries keys only, with no values.
- The header commits to the BAL as `block_access_list_hash = keccak256(rlp(BAL))`.
- The BAL is **not** in the block body. It is a field of `ExecutionPayload` (Engine API `ExecutionPayloadV4`).
- EL nodes must retain BALs for at least the weak-subjectivity period (3533 epochs).

Consequences for skim:
- An absolute write to a watched key that skim doesn't know yet makes it known immediately, with no SNAP request.
- Read-only entries never make a key known.
- A storage write that leaves the value unchanged is recorded as a read.
- For an account created and destroyed in the same transaction, nonce and code changes are omitted and its storage accesses appear as reads. These must not be treated as persistent writes.

### 3.2 Gloas ePBS execution-payload gossip (preferred fast path)
- The Gloas P2P spec defines an `execution_payload` gossip topic carrying `SignedExecutionPayloadEnvelope`. Its `ExecutionPayload` includes the full `block_access_list`. EIP-7928's pruning allowance applies to long-term storage, not to live gossip.
- So one message delivers the block hash, state root, transactions and BAL.
- **Verifying a gossiped payload** (there is no header on this path):
  1. Rebuild the execution header from the payload fields, with `block_access_list_hash = keccak256(rlp(BAL))`.
  2. Check that `keccak256(rlp(header)) == payload.block_hash`.
  3. Check that `payload.block_hash` equals the bid's `block_hash` committed in the beacon block.

  This ties the BAL to a builder-signed bid inside a proposer-signed block, even before skim verifies any signatures.
- **Measured (Lighthouse, 1,348 slots):** envelopes arrive P50 764 ms / P95 2.7 s after slot start. That is about 0.9 s earlier than skim's EL path (header poll + eth/71 BAL) and about 0.3–0.5 s before public EL nodes serve the header.
- **ePBS separates three stages:** block commitment, payload reveal and payload status (full or empty). A received payload is only a *candidate* until the chain tracker selects it.
- Joining CL gossip requires discv5 with eth2 ENRs (correct fork digest), libp2p gossipsub, and Status/Metadata/Ping request-response. A listen-only node may get scored down or dropped. M0 measures this.

### 3.3 eth/71 (EIP-8159): BAL retrieval over EL P2P
- Messages: `GetBlockAccessLists` (0x12) / `BlockAccessLists` (0x13), by block hash.
- An unavailable BAL comes back as `0x80`, the RLP empty string. A valid empty BAL is `0xc0`.
- Unavailable may just mean the peer pruned it, so retry other peers before giving up.
- Uses: fallback when gossip is missing, catch-up after a disconnect, and reorgs.

### 3.4 snap/2 (EIP-8189): state retrieval
- skim uses messages 0x00–0x05: `GetAccountRange`, `GetStorageRanges`, `GetByteCodes`.
- snap/2 removes `GetTrieNodes`/`TrieNodes` and adds `GetBlockAccessLists` (0x08) / `BlockAccessLists` (0x09).
- Peers are *expected* (by serving convention, not a guarantee) to serve roughly the last 128 state roots.
- Point lookups are narrow range queries over `keccak256(address)` and `keccak256(slot)`. Storage values come RLP-encoded.
- skim must tell these responses apart:
  - **present**
  - **absent**: proven by a proof, or accepted on trust when proof verification is off
  - **root unavailable**: an empty response *without* a proof
  - **request failure**
  - **malformed**
- Nodes should keep BALs for orphaned blocks too, which helps reorg recovery.

## 4. Architecture

```
        Rust API (crate `skim`)  ◄── primary interface
            │           \
            │        JSON-RPC adapter (localhost:8545, HTTP + WS, batch)
            │
   Snapshot / Overlay / Resolver ── REVM workers
            │
     Per-key MVCC state store ◄── State synchronizer (single writer)
            ▲                          ▲            ▲
            │                    BAL processor   SNAP resolver
            │                          ▲            ▲
      Chain tracker ◄──────────────────┤            │
        ▲        ▲                     │            │
   CL gossip   EL eth/71 (fallback) ───┘       snap/2 peers
   (Gloas payload envelopes + BALs)
```

### Components

| Crate | Responsibility |
|---|---|
| `skim-bal` | EIP-7928 types, RLP decode, commitment checks, normalization into per-key writes |
| `skim-el` | EL devp2p via Reth crates: peers, eth/71 BAL fetch, header fetch, snap/2 client |
| `skim-cl` | CL gossip listener: discv5/ENR, gossipsub, Gloas SSZ containers, payload-envelope extraction |
| `skim-chain` | Header/beacon DAG, candidate vs selected head, payload status, finalized checkpoint, 256-hash BLOCKHASH window |
| `skim-state` | Per-key MVCC store, snapshots, GC, the state synchronizer (single writer) |
| `skim-snap` | SNAP resolver: dedup, batching, peer scoring, budgets, root selection plus local BAL replay |
| `skim-sim` | REVM integration: `DatabaseRef` over snapshots, overlays, miss collection, environments |
| `skim` | Public Rust API (re-exports) |
| `skim-rpc` | JSON-RPC adapter, built only on the public API |
| `skim-cli` | `skim` binary: `run`, `status`, `watch`, `call`, `bench`, and M0 probes |

**Reuse:** `../reth_less` (evmsim) already provides block-pinned snapshots, `Fork` overlays (call, apply, bundle, branch, overrides), discovery runs and replay harnesses on revm 43. That code is RPC-backed. `skim-sim` should reuse or extract its overlay and execution layer over a skim-backed `DatabaseRef`, rather than rewrite it.

## 5. State model

### 5.1 Per-key MVCC
```rust
enum StateKey {
    Existence(Address),
    Balance(Address),
    Nonce(Address),
    CodeHash(Address),
    Storage(Address, U256),
}
enum StateValue<T> { Known(T), Unknown }
struct Version { block: BlockHash, value: StateValue<..> }
```
- `Known(0)` and `Unknown` are different. **Unknown is never read as zero or empty.** A simulation that reads an unknown key fails with `StateUnavailable`.
- Bytecode is content-addressed by code hash and shared everywhere. Code from a BAL is hashed and checked.
- **Lookup:** the newest version on the target block's ancestry. A value from another branch is never visible.
- **Base inserts don't reach backwards.** A value fetched at block H is valid for H and its descendants only. A lookup at H-1 still returns `Unknown`.
- **Rollback** comes free from branch-aware lookup. There is no separate undo journal.
- **GC:** keep versions above the finalized checkpoint plus anything pinned by live snapshots. Collapse older history into a single base entry per key.

### 5.2 Recent BALs indexed by key
The synchronizer keeps about 128 blocks of decoded BALs in memory, indexed by `StateKey`. This one mechanism covers lazy fills, concurrent fetches and replay:
- **Committing a value fetched at root P** = insert the base at P, then replay that key's writes from P+1 to head from the index.
- New BALs that arrive while a fetch is in flight are already in the index, so nothing is lost and no "pending key" registration is needed.
- A stale SNAP response can never overwrite a BAL-derived version.

### 5.3 Snapshot identity and views
- Snapshots are keyed by execution `block_hash`. The header provides number, parent, state root and environment.
- A snapshot is an immutable, cheap-to-clone (`Arc`) view pinned to a block hash and a *resolved-state generation*.
- Lazy fills create a new generation. Running simulations never see values change underneath them.
- Simulation writes live only in per-execution copy-on-write overlays.

### 5.4 Account existence
Existence is derived only from evidence:
- A balance change on an account previously known absent means it now exists.
- A contract creation means it exists and its code and nonce are known.
- An access with no changes says nothing about existence.

When the evidence is ambiguous, existence stays `Unknown`.

## 6. Synchronization

### 6.1 Bootstrap
1. Join EL and CL peers and learn the head.
2. Fetch the last 256 execution headers (BLOCKHASH window).
3. Start following BALs (§6.2).
4. Resolve the watchlist through the SNAP resolver (§6.3).
5. Publish snapshots once the head is followed. "Ready" is tracked per key, not globally.

### 6.2 Following
For each selected execution block:
1. Get the payload and BAL from CL gossip. Fall back to eth/71 by hash.
2. Verify the commitment (§3.2 on the gossip path; header hash on the EL path).
3. Index the BAL by key (§5.2).
4. Append versions for tracked keys and initialize unknown tracked keys from absolute writes.
5. Advance the selected head.
6. Emit `StateReady { block, changed_keys }`.

**Reorgs:**
1. Find the common ancestor and switch the selected branch.
2. Fetch any missing BALs for the new branch, from gossip, eth/71 or snap/2.
3. Apply them in order and publish.

Rebootstrap is needed only when the BALs for the new branch can't be obtained.

### 6.3 Resolving missing state
The default path covers both direct lookup and pivot replay:
1. Pick the newest ancestor root P ≤ target H that some peer will serve. At the very head, peers may not have executed H yet, so P is often H-1.
2. Fetch the key at `stateRoot(P)`.
3. Commit the base at P, replay indexed BAL writes from P+1 onward, and publish a new generation.

If no servable root is covered by the BAL index, fail explicitly. Never use a peer's "current" value as if it belonged to H.

Resolver properties:
- deduplicated per (key, root)
- batched
- per-request timeouts, with backoff and peer rotation
- request budgets per call and globally
- peer latency and availability tracked

### 6.3.1 State provider pool (pinned SNAP peers)
State is fetched from a small pool of **pinned peers**, not from whichever peer is available. Public discovery is used only to find candidates and to re-score them periodically.

```
discovery (discv4/discv5) ──► candidate scoring ──► pinned pool
                                  (periodic)         ├── primary
                                                     ├── backup A
                                                     └── backup B
cache miss ──► primary ──(slow / root unavailable)──► backup ──► MVCC commit ──► retry sim
```

- **Persistent connections.** Keep 3–5 pinned snap/2 peers connected (reth `trusted_nodes`), so the handshake cost is paid once rather than per request. Pinning applies only to state fetches. BAL and header traffic can still use any peer, unless `connect_trusted_nodes_only` is set for a closed deployment.
- **Scoring.** Each peer gets a score from:
  - RTT
  - SNAP account and storage P50/P95
  - success rate
  - root-servable lag: how quickly it serves new roots
  - session stability

  The best peer is the primary. Scores are exponentially decayed, so a degraded primary is demoted automatically.
- **Direct lookup at the target root**, with fallback:
  - If the primary is slow (adaptive hedge timeout, roughly its P95), the request is hedged to a backup.
  - If the primary hasn't executed H yet (`root unavailable`), the request goes to a backup or falls back to H-1 plus a local BAL(H) replay (§6.3).
- **Re-scoring.** Discovery keeps running in the background. Every few minutes, candidates are probed with a few cheap requests. A candidate that beats the worst pinned peer replaces it.
- **Retention (measured, `docs/m0-scheduler.md`).** With a genesis status, public snap/2 peers evicted us within 22–51 s. Two changes fixed most of it:
  - **Advertising the real head** (Status + `BlockRangeUpdate`) got about 25× more SNAP service.
  - **A snap/1 client** reaches Geth peers.

  Together they gave 4 SNAP peers that stayed connected for the whole 17-minute run. Both are always on.
- **Scheduler (`skim-snap`).** The scheduler owns every request:
  - per-peer in-flight cap
  - re-queue on disconnect, timeout or unavailable root
  - one hedge per job
  - code batching
  - `(root, key)` dedup

  Placement by EWMA latency × load drifts toward the fastest peer. In one run, a 71 ms Geth peer served 53% of requests.
- **Limits.** Pinning a public peer guarantees nothing. It may rate-limit us, disconnect us, or prune state. If the pool can't meet the targets in §1, the next step is to run a dedicated snap/2 peer. The protocol path is the same; only the trust and operating cost change.

### 6.4 Head semantics
- `latest` is the **state-ready** head, not the first-seen gossip head.
- `finalized` comes from CL finality information.
- `safe` maps to `finalized` until there's a better source. This is documented.

## 7. Simulation

### 7.1 Native Rust API (primary)
```rust
let client = Client::start(Config::sepolia()).await?;
client.watch(Watch::account(pool).slots([SLOT0, LIQUIDITY])).await?;

let mut ready = client.state_ready();                 // Stream<StateReady>
while let Some(ev) = ready.next().await {
    if !ev.changed_keys.intersects(&my_keys) { continue; }
    let snap = client.snapshot(ev.block_hash)?;        // immutable, pins MVCC versions
    let env  = BlockEnv::next_block(&snap)             // base/blob fee + slot timestamp derived
        .coinbase(builder);                            // remaining unknowns explicit

    let quotes = snap.simulate_many(&env, &calls, Mode::WarmOnly);

    let mut ov = snap.overlay(&env);                   // CoW, single owner
    ov.apply(&approve)?;
    let (mut a, mut b) = (ov.fork(), ov.fork());       // O(1) branching
    let (ra, rb) = (a.simulate(&swap_a)?, b.simulate(&swap_b)?);
}

match snap.simulate(&env, &tx, Mode::WarmOnly) {
    Err(SimError::StateUnavailable { keys }) => {
        client.resolve(snap.block_hash(), &keys, Budget::ms(500)).await?;
    }
    r => { r?; }
}
```

Rules:
- `simulate*` is synchronous and CPU-only. `resolve`, `watch` and streams are async. EVM work never runs on the Tokio runtime.
- **Modes:**
  - `WarmOnly` never touches the network.
  - `Resolve { budget, deadline }` uses miss collection (§7.2) and is restarted outside the EVM.
- `SimOutcome` contains:
  - status and gas
  - return or revert data
  - simulated logs
  - state diff and access set
  - **the resolved `BlockEnv`**, so every assumption is visible
- Custom tracing goes through `simulate_with(&env, &tx, &mut impl Inspector)`.
- `client.learn(&outcome)` adds a run's access set to the watchlist.

### 7.2 Miss collection and cold-round minimization
REVM's `Database` is synchronous, so network I/O never happens inside it. A `Resolve`-mode call works like this:
1. Run a **discovery pass**: an unknown key reads as a placeholder and is recorded. Results from this pass are never returned.
2. Fetch every predicted key in one parallel round (sources below).
3. Re-run for real. Repeat up to a bounded number of rounds. Then return a result, or `StateUnavailable { keys }`.

**Prediction sources**, merged into each round:
- keys recorded by the discovery pass
- access sets previously recorded for the same call shape (target + selector)
- a caller-supplied access list (API `hints`, or the `accessList` field in RPC requests)
- BAL `storage_reads` and writes for the target contracts in recent blocks
- for proxies (EIP-1967 and similar), the implementation slot together with the implementation's code

**Round-level tactics:**
- In one round, send `GetAccountRange` and `GetStorageRanges` in parallel. Storage requests need only `keccak256(address)` and the root, not the account record.
- Cached code hashes skip `GetByteCodes`.
- Hedge slow requests to a second peer after an adaptive timeout, to cut P95.
- Keep warm connections to the lowest-RTT SNAP peers.

**Divergence control:** placeholder values can steer a discovery pass down paths the real execution never takes (Spike C: a multihop quote requested 807 keys). Each round is capped, and fetching happens in waves that stop early once the access set has converged.

**Width:** a round's keys are spread across the pinned peers, with per-peer in-flight limits. Spike C showed dozens of parallel requests to a single peer get dropped.

**Learned-set hygiene:** a learned access set is a prediction, not a cache. A plain union over inputs over-fetches: one 100-WETH quote put 1,094 tick keys into the shape's set, and every small quote then took about 7.5 s. Each learned set needs:
- frequency weighting: keys seen in at least x% of recent runs
- a size cap on the first wave
- optionally, input bucketing (e.g. by amount magnitude)

Misses beyond the predicted set are fetched in later rounds.

**Measured policy (`docs/m0-round2.md`):**
- exact-input set first
- otherwise core keys (≥ 50% of the shape's last 8 runs, cap 256)

Result: repeated inputs run at k=1, and new inputs of a known shape at k=2.

**Throughput model:** a wave's speed is about Σ (in-flight_i / RTT_i) over serving peers (measured: 42 keys/s with one ~190 ms peer; 169 keys/s once a ~47 ms peer joined). Per-peer concurrency should adapt (AIMD) so that fast peers carry more of the load.

**Dedicated peer:** serving costs 0.36 ms per lookup (local Nethermind). With a co-located snap peer, even k=8 cold calls finish in under 10 ms, so cold fetch fits on the live path. On public peers, cold fetch remains a k=1 game.

**Sharing:** everything fetched joins the shared MVCC store. A second simulation touching the same contracts runs warm. Values learned from BAL writes are known without any SNAP request.

### 7.3 Execution environment
- **Post-state of H, historical context:** H's own environment.
- **Next block:**
  - derived: EIP-1559 base fee, blob base fee, and timestamp (`genesis + slot × 12` for the target slot)
  - supplied by policy or options, never silently guessed: coinbase, prevrandao, gas limit (±1/1024)
- REVM must run the Glamsterdam execution rules for Sepolia.

### 7.4 JSON-RPC adapter
Built only on the public API. Runs over HTTP and WebSocket and supports batch requests. Error codes follow EIP-1474: `-32002` resource unavailable, `-32004` method not supported.

| Method | Notes | Priority |
|---|---|---|
| `eth_chainId`, `net_version`, `eth_blockNumber`, `eth_syncing`, `web3_clientVersion` | `blockNumber` = state-ready head | P0 |
| `eth_getBalance`, `eth_getCode`, `eth_getStorageAt`, `eth_getTransactionCount` | `-32002` on unavailable, never a fabricated 0 | P0 |
| `eth_getBlockByNumber` / `ByHash` | headers; full txs within the gossip retention window | P0 |
| `eth_call` with state and block overrides | core | P0 |
| `eth_estimateGas` | binary search on REVM | P0 |
| `eth_simulateV1` | multi-call/multi-block | P1 |
| `eth_createAccessList` | access set from REVM | P1 |
| `debug_traceCall` | REVM inspectors | P1 |
| `eth_subscribe("newHeads")` | from state-ready events | P1 |
| `eth_gasPrice`, `eth_maxPriorityFeePerGas`, `eth_feeHistory` | headers plus gossip payload txs | P2 |
| `eth_getProof` | only for roots fetched directly with proofs | P2 |
| `eth_sendRawTransaction`, `eth_getLogs`, `eth_getTransactionReceipt`, `eth_getTransactionByHash` | out of scope | `-32004` |

Further rules:
- Block tags `latest`, `safe` and `finalized` and EIP-1898 `{blockHash}` are supported. `pending` maps to `-32004`.
- Within one batch, `latest` is pinned to a single snapshot.
- Use `alloy-rpc-types` and `jsonrpsee` so the wire format matches existing clients.

## 8. Trust model
**Checked (cheap):**
- header parent links
- BAL commitments
- payload block hash against the bid in the beacon block
- code hashes
- version ancestry
- chain and fork identifiers

**Not checked in the initial release:**
- PoS fork choice and finality
- execution correctness
- full state roots
- SNAP Merkle proofs (proofs are kept for later verification)

**Planned optional modes:**
- **Verified State:** verify single-key SNAP proofs. This is cheap, and it's a candidate to become the default once RPC users connect signing tools.
- **Verified Consensus:** light-client authentication of selected heads.

`web3_clientVersion` and the docs state the unverified status.

## 9. Benchmarks
The primary baseline is **a local Reth node with in-process REVM**, with its version pinned and BAL-parallel execution on/off recorded. A remote RPC simulator is a secondary baseline.

| Metric | Purpose |
|---|---|
| `payload_gossip_lag_ms` | slot start → envelope received |
| `el_header_poll_lag_ms` | same block via EL polling (comparison) |
| `state_ready_lag_ms` | payload received → snapshot published |
| `bal_apply_us` | apply cost per block |
| `snap_root_servable_lag_ms` | gossip arrival → peers serve that root |
| `snap_request_p50/p95_ms`, `snap_success_rate` | resolver reliability |
| `cl_peer_retention` | how long listen-only CL connections survive |
| `peer_rtt_ms` | raw network distance, to separate it from serving time |
| `snap_account_p50/p95_ms`, `snap_storage_p50/p95_ms`, `snap_code_p50/p95_ms` | cost of one round, per message type |
| `sequential_rounds_per_cold_call` (k) | dependency depth of cold calls |
| `keys_per_round` | how much each round collapses |
| `simulate_warm_p95_us`, `simulate_partial_cold_p95_ms`, `simulate_full_cold_p95_ms` | the three latency targets (§1) |
| `infura_eth_call_p50/p95_ms` | hosted-RPC baseline for the same call and block |
| `cache_hit_rate`, `simulation_restarts` | working-set fit |
| `rss_bytes`, `disk_bytes`, `cpu_pct`, `net_bytes` | the actual thesis |

Every comparison uses identical hardware, inputs, fork rules and target block state. Benchmark at least one predictable workload and one unpredictable workload.

## 10. Plan

### M0: network feasibility (go/no-go)
- **Spike A, CL payload gossip:**
  - join Sepolia CL gossip
  - decode `SignedExecutionPayloadEnvelope` and extract and verify the BAL (§3.2)
  - measure arrival lag versus EL header polling, peer retention, and completeness, including withheld and empty payloads
- **Spike B, EL eth/71 + snap/2:**
  - connect to multiple peers and fetch BALs by hash
  - fetch one account and specific slots at recent roots
  - measure latency, success rate and disconnects at several request rates
  - measure `snap_root_servable_lag_ms`
- **Spike B2, pinned pool:** take the 3–5 best peers from Spike B's per-peer results, pin them, and measure the following over persistent connections:
  - storage-lookup P50/P95
  - root-servable lag
  - disconnect/throttle rate over hours
- **Spike C, cold-call economics:**
  - for a fixed set of Sepolia calls, measure the P50/P95 of one SNAP request per message type, and `k` per call. Calls cover a plain ERC-20 read, a proxied token, a Uniswap-style quote, and a multi-hop route.
  - compare against Infura `eth_call` with the same calldata and block, measured from the same machine
  - output: the crossover of `k` and round latency at which skim beats a hosted RPC, i.e. whether the design can be *faster* than RPC under some conditions or is only *cheaper*

**Go if** both paths are reliable enough for continuous partial-state maintenance.

### M1: BAL follower + first interface
- Payload/header tracking, BAL decode and verification, gap recovery over eth/71, sequential application.
- A Rust test that follows, snapshots and reads a slot.
- A minimal JSON-RPC (`chainId`, `blockNumber`, `getBalance`, `getStorageAt`, `getCode`) so `cast` works against `localhost:8545`.
- **Differential harness:** the same request goes to a reference node and the JSON responses are compared. This becomes every milestone's regression suite.
- Target: 100 consecutive Sepolia blocks with no gaps.

### M2: per-key MVCC
Branch-aware versions, the BAL key index, lazy-fill replay, the single-writer commit path, GC, the `StateReady { changed_keys }` stream, and simulated reorgs.

### M3: SNAP resolver
Root selection, batching, dedup, budgets, peer scoring, and the absent/unavailable distinction.

### M4: simulation
- `skim-sim` over MVCC, reusing evmsim's overlay layer.
- `WarmOnly` and `Resolve` modes with miss collection.
- Environments and BLOCKHASH.
- `eth_call` and `eth_estimateGas`, with matching results on selected Sepolia transactions.

### M5: benchmarks
Measure against a local Reth node, covering both latency and resource cost (§9).

## 11. Risks
- **Working-set predictability.** This is the main risk, more than REVM speed. Generic RPC usage makes cold misses common. Whether a cold call beats a hosted RPC depends on `k` and SNAP round latency (§1), which Spike C measures.
- **Peer behavior.** SNAP throttling, and CL scoring of listen-only nodes.
- **ePBS gossip.** Real arrival lag and completeness are unproven. Reth may adopt BAL-parallel execution and shrink the hypothesized window.
- **Concurrency.** Lazy fills, BALs and reorgs all mutate state. The single-writer commit path is the mitigation.
- **Trust.** Unverified state can produce wrong results, which limits who should use the initial release.
- **Protocol churn.** Reth crate APIs, eth/71, snap/2, Gloas types and REVM Amsterdam support are all moving. Pin revisions and keep live Sepolia interop tests.

## 12. Prior art
- **Portal Network (Trin, etc.):** P2P-served JSON-RPC over a separate DHT. Its state network was the hardest part. skim instead uses existing SNAP peers plus BALs.
- **Helios:** a verifying light client that still depends on an untrusted RPC for data.
- **evmsim (`../reth_less`):** an RPC-backed local simulation cache. skim's simulation layer builds on it.

## References
- EIP-7928 Block-Level Access Lists: https://eips.ethereum.org/EIPS/eip-7928
- EIP-8159 eth/71 BAL exchange: https://eips.ethereum.org/EIPS/eip-8159
- EIP-8189 snap/2: https://eips.ethereum.org/EIPS/eip-8189
- EIP-7732 ePBS: https://eips.ethereum.org/EIPS/eip-7732
- EIP-1474 JSON-RPC error codes: https://eips.ethereum.org/EIPS/eip-1474
- Gloas consensus specs (beacon chain, p2p): https://github.com/ethereum/consensus-specs
- devp2p eth/snap: https://github.com/ethereum/devp2p
- Reth: https://github.com/paradigmxyz/reth · REVM: https://github.com/bluealloy/revm
