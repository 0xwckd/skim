# P0: cold-miss tail on new inputs (miss root, randomized quotes, V3 prefetch)

Runs on 2026-10-16 (KST): 60 blocks per configuration, same home connection, public SNAP peers only. The local node was stopped for most of the runs. Raw data is in `data/p0-{A,B,C}.*`; the analysis script is `docs/tools/p0_analyze.py`.

**Workload, per block (900 calls per run):**
- the 7 fixed calls from M1
- **8 randomized V3 single-hop quotes:**
  - random direction
  - fee tier 0.05% (spacing 10) or 0.3% (spacing 60)
  - log-uniform amount, 0.0001–100 WETH or 0.001–100k USDC

The randomized quotes are inputs no learned access set has seen (480 per run). Every output is compared with Infura `eth_call` at the same block hash.

## Correctness bug found and fixed first
The first run of A produced **28 output mismatches vs Infura**. The scheduler had resolved an "empty storage, no proof" SNAP answer as zero, when two peers (or the only peer) said so. At the head, that answer usually means "this peer hasn't executed the block yet", not "the slot is empty". The wrong base value then persisted into later blocks.

**Fix (`3950f01`):** an empty storage answer is resolved as zero only if the same peer reports the account as absent or its storage root as the empty trie. Otherwise the job is root-unavailable for that peer and is retried. Regression test: `empty_answer_from_unserving_peer_is_never_zero`. All three runs below use the fix: **2,700 / 2,700 outputs match Infura**.

## Results

| | A: miss at head root | B: miss at parent root + BAL replay | C: B + V3 prefetch (±2 words) |
|---|---|---|---|
| Outputs match Infura | 900/900 | 900/900 | 900/900 |
| Calls that hit any miss | 12 (1.3%) | 13 (1.4%) | **3 (0.33%)** |
| Miss rounds per such call | 1×9, 2×2, 4×1 | 1×7, 2×3, 3×3 | 1×1, 2×1, 3×1 |
| One miss round, wall P50 / P95 / max (ms) | 1,020 / 2,493 / 3,358 | **312 / 642 / 1,043** | 410 / 1,334 / 1,334 |
| ├ queue wait (max per round) P95 | 2,187 | 278 | 1,324 |
| ├ SNAP service (max per round) P50 / P95 | 612 / 1,276 | **311 / 630** | 268 / 533 |
| └ root-unavailable answers per round P95 / max | 11 / 27 | **0 / 0** | 0 / 0 |
| Max attempts for any key | 3 | **1** | 1 |
| Total miss cost of one call, max (ms) | **9,007** | 1,506 | 2,393 |
| Re-run of the call after a round | ~6.5 ms | ~4.1 ms | ~5.5 ms |
| Prefetch cost | – | – | 4 rounds, 1,389 keys in the whole run |

### What the breakdown shows
- **The multi-second tail was waiting for the head root, not network RTT.** At gossip time, SNAP peers haven't executed block H yet:
  - Fetching at root(H) gets "root unavailable" answers (up to 27 per round) plus queueing behind the retries.
  - Fetching at root(H−1) and replaying BAL(H) from the index removes them completely (0 unavailable, 1 attempt). A miss round becomes about one RTT (P50 312 ms).
- **Prefetch cuts the *number* of misses by about 4×** (13 → 3 calls). Initialized ticks and bitmap words around the current tick stay known, and BAL writes keep them current, so after the first refresh the prefetcher is nearly idle (1,389 keys in total).
- **The remaining misses are all large swaps (53–71 WETH) on the 0.05% pool** (spacing 10, so one bitmap word covers about 29% of price). They run past ±2 words, and the unknown words read as empty during discovery. The swap then walks word by word, giving 2–3 rounds of 124–319 keys each.

## Next (from the data)
1. **Input-aware range:** estimate how far a swap can move the price from `amount` and the current liquidity, and prefetch the bitmap words up to that point.
2. **Miss expansion:** when a miss is a bitmap word outside the window, fetch the whole contiguous word range from the current word to the missed one in one wave, instead of discovering it word by word.
3. Fold the learned set, the V3 prediction and miss expansion into one **State Access Predictor** (the user's proposal): input → predicted keys → async prefetch → MVCC → revm.

## P3: removing the top-serving peer (`probe-call --soak --drop-top-after 50`)
- **Setup:** 120 blocks of steady-state hinted calls on public peers (7 calls per block, each one fetch wave).
- **Intervention:** at iteration 50 the scheduler stopped using the peer that had served the most requests (19,714, EWMA 105 ms).
- **Data:** `data/drop-top.*`.

The SNAP pool grew from 5 to 22 peers during the run. Windows with a similar pool size are compared:

| Window | SNAP peers | Runs | Wall P50 / P95 / P99 / max (ms) |
|---|---|---|---|
| Iterations 30–49 (top peer in use) | 12–14 | 140 | 307 / 760 / 985 / 1,007 |
| Iterations 51–70 (top peer excluded) | 13–19 | 140 | 319 / 754 / 937 / 955 |

- **Completion:** 833 / 833 hinted runs completed, all at k=1, before and after the drop.
- **Load:** the next peer took about 10k requests and the rest spread across the pool.
- **Latency:** no measurable change (P50 +12 ms, P99 −48 ms).

**Limitation:** the pool was large (12–22 SNAP peers). The earlier soak sometimes had only 2–4 SNAP peers. Losing the top peer in a pool that small would likely raise the tail, which this run doesn't test.
