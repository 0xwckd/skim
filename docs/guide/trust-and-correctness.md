# Trust and correctness

skim is built so that it can be **slow or unavailable, but not wrong** about the state it serves, within the trust model below.

## What skim verifies
| Data | Check |
|---|---|
| Block access list | The EL header is rebuilt from the gossiped payload (including `keccak(BAL)`) and must hash to the payload's `block_hash`. |
| Gap-filled blocks | Headers must link by parent hash. Each BAL must match its header's `block_access_list_hash`. |
| State fetched over SNAP | Every account is Merkle-proven against the block's `stateRoot`, and every slot against that account's `storageRoot`. Zero is accepted only with a proof of absence. |
| Contract code | `keccak(code) == codeHash` |
| Receipts | Rebuild `receiptsRoot` |
| Bodies (older transactions) | Rebuild `transactionsRoot` |
| Headers before skim started | Hash-linked to a known header |

## What skim does not verify (yet)
- **Consensus.** skim trusts that the gossiped head is the canonical chain. Proof-of-stake fork choice, finality and BLS signatures are not checked, as in an optimistic light client. A consensus light-client sync is on the roadmap.
- **Execution of the block itself.** skim takes the BAL's post-values as given, because they are committed by the block hash. A block that is valid by hash but whose BAL disagrees with its own execution would be invalid under consensus, and is out of scope here.

## Rules that hold everywhere
1. **Unknown is never zero.** A value is used only if a BAL wrote it or a proof established it.
2. **Fetch at the parent, replay to the head.** At gossip time most peers haven't executed block N yet. skim reads at N−1's root and applies BAL(N) itself.
3. **Reorgs roll back to the common ancestor only** (in memory and in the archive), then replay.
4. **Errors instead of guesses.** If a read can't be proven, the RPC answers `-32002`.

## How it was tested
Every claim above was measured on Sepolia against Infura at the same block hashes. See [`docs/experiments`](../experiments):

| Experiment | Result |
|---|---|
| 1-hour soak | 1,753 calls, 0 mismatches |
| 2-hour subscription soak | 0 disagreements |
| Randomized, never-seen V3 quotes | 2,700, all identical |
| Verified-state runs | 866 calls, 0 mismatches |
| Archive | 90/90 and 51/51 historical reads; backfill 102 identical, 3 refused, 0 wrong |
| Receipts and logs | 32/32 receipts and transactions; every `eth_getLogs` filter identical over 16 and 1,024 blocks |

A bug found by these checks (an empty SNAP answer treated as zero) is why rule 1 exists.
