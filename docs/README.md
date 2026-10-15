# skim documentation

## Guide
1. [Getting started](guide/getting-started.md): build, run, first call, subscriptions
2. [How skim works](guide/how-it-works.md): BALs, verified fetches, subscriptions, prediction
3. [Configuration reference](guide/configuration.md): `skim.yaml` and `skim run` flags
4. [JSON-RPC reference](guide/json-rpc.md): methods, block tags, errors
5. [Archiving history for your state](guide/archive.md): `retention: archive`, backfill
6. [Trust and correctness](guide/trust-and-correctness.md): what's verified, what isn't
7. [Troubleshooting](guide/troubleshooting.md)

**Examples:** [Uniswap V3, Uniswap V4 and sending transactions with viem](../examples)

**Design:** [PROJECT.md](../PROJECT.md)

## Experiment logs
Every result in the README comes from one of these runs, on Sepolia over public peers.

| Doc | What was measured |
|---|---|
| [M0 spike A](experiments/m0-spike-a.md) | CL payload gossip: receiving `execution_payload` envelopes with BALs |
| [M0 spike B](experiments/m0-spike-b.md) | EL peers: eth/71 BALs, snap/2 point lookups, peer census |
| [M0 spike C](experiments/m0-spike-c.md) | cold-call SNAP rounds vs Infura `eth_call` |
| [M0 round 2](experiments/m0-round2.md) | learned access sets, k rounds per call |
| [M0 scheduler](experiments/m0-scheduler.md) | adaptive request scheduler, peer retention |
| [M0 soak](experiments/m0-soak.md) | 298k SNAP jobs, 0 failed |
| [M1 follower](experiments/m1-follower.md) | BAL application latency, state-ready time |
| [M2 miss tail](experiments/m2-p0-miss-tail.md) | prefetch, the predictor, priorities, peer removal |
| [M2 verified state](experiments/m2-verified-state.md) | Merkle-proof verification of every SNAP value |
| [M3 2-hour soak](experiments/m3-soak-2h.md) | subscriptions vs on-demand, 0 disagreements |
| [M4 archive](experiments/m4-archive.md) | selective archive, restart healing, BAL backfill |
| [M5 predictors and chain data](experiments/m5-predictors-and-chain-data.md) | learned patterns, V3 predictor in `skim run`, transactions/receipts/logs |

Tools used by these runs live in [`experiments/tools`](experiments/tools).
