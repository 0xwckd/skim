# M4: selective state archiving (first results)

Run on 2026-10-15 (KST): `skim run --window 16 --archive … --config data/m4/skim.archive-test.yaml` on public Sepolia peers. Two archived subscriptions:
- a Uniswap V3 pool (account + slots 0, 1, 2, 4)
- a QuoterV2 call, whose learned access set (39 keys) is archived

The 16-block in-memory window forces every historical read below to come from the on-disk archive. The reference is Infura (archive) at the same block numbers. Tool: `docs/tools/archive_check.py`.

| Check | Result |
|---|---|
| Subscriptions ready | both ready a few blocks after start (7 and 39 required keys) |
| Historical `eth_getStorageAt` / `eth_getBalance` / **historical `eth_call` (V3 quote)** over 18 blocks, 40–35 blocks behind the head | **90 / 90 match** |
| Slots 834–851 (right after the subscription) | 24 / 24 match |
| A slot never read or archived (its on-chain value is 0) | `-32002` "state not archived" (not 0) |
| A block before the subscription started | `-32002` (outside window, not archived) |
| **Restart continuity** | stopped 60 s and then 30 s; the gaps (915–919, 946–949) were healed from eth/71 BALs once EL peers connected, and the segments merged |
| Reads inside the healed gaps (slot0, liquidity, historical quote) | **51 / 51 match** |

## What the first restart taught us
On the first restart, the gap fill ran once, at the first gossiped block. At that moment no EL peer was connected yet, because CL gossip arrives before EL sessions. The archive correctly started a new segment and left 915–919 uncovered: a refusal, not a guess.

The fix:
- `Archive::fill_gap` fills the space *between* two segments, but only if the filled blocks link by hash to both sides (the previous segment's end hash, and the next segment's first parent hash).
- A background healer retries every 15 s while BALs are retained (~2 weeks).

## Semantics, as implemented
- A version is stored only when a BAL changes a key, plus one base per key when it becomes known. Nothing is stored for blocks where it doesn't change.
- A read at block N answers only if N lies in a segment of consecutively processed blocks, the key's coverage started at or before N, and the newest version at or before N lies in that segment. Anything else is `NotCovered`, which RPC returns as `-32002`.
- Reorgs truncate exactly what orphaned blocks wrote (a per-block journal).
- Historical `eth_call` uses the archived header and BLOCKHASH, and never fetches over SNAP (old roots aren't served). If any key the call reads is uncovered, it returns `-32002` with the count.
