# Configuration reference

skim reads one YAML file (`skim run --config skim.yaml`) plus command-line flags. Every field is optional, and with no config skim serves everything on demand.

## `skim.yaml`
```yaml
network: sepolia                      # the only network today

state:
  subscriptions:                      # your app's state: always kept ready
    - name: <unique name>
      address: "0x…"                  # needed for account/storage
      account: true                   # balance, nonce, code hash (and the code)
      storage: ["0x0", "0x4"]         # explicit slots (hex strings)
      calls:                          # simulations whose access sets stay ready
        - to: "0x…"
          data: "0x…"                 # full calldata
          from: "0x…"                 # optional caller (default 0x0)
      on_block: false                 # re-run `calls` every block, results in skim_subscriptionStatus
      retention: { mode: archive }    # keep every version on disk (needs --archive)

  on_demand:
    enabled: true                     # false: still fetched for correctness, kept 1 block only
    cache_ttl_blocks: 4               # unsubscribed state stays current this many blocks after use

  limits:
    max_subscribed_keys: 100000       # cap on declared + learned keys; excess is reported as `truncated`

predictors:
  uniswap_v3:                         # input-aware prefetch for QuoterV2.quoteExactInputSingle
    pools: ["0x…", "0x…"]             # V3 pools (fee/tokens/tickSpacing are read on start)
    words: 2                          # tick-bitmap words kept warm on each side (grows with observed reach)
    prefetch_budget: 256              # background keys per pool per block (low priority)
```

### Subscriptions in detail
| Field | Effect |
|---|---|
| `account: true` | Subscribes balance, nonce and code hash. Code is cached for tracked accounts. |
| `storage: [slots]` | Subscribes exactly these slots. Use `cast index` / `cast storage` to find mapping slots, or subscribe a call instead. |
| `storage: all` | **Not supported yet:** rejected at startup. |
| `calls` | Each call runs every block (local, ~ms). Every key it reads becomes a required key of the subscription, so the call itself and any call reading the same state stay warm. |
| `on_block: true` | Keeps each call's latest output (hex) or error in `skim_subscriptionStatus.results`. |
| `retention: { mode: archive }` | Stores every BAL-written version of this subscription's keys (including learned keys) in the `--archive` file, so historical `eth_getStorageAt` / `eth_call` work for them. See [archive](archive.md). |

**Readiness** (`skim_subscriptionStatus`): `ready: true` means the head block's BAL is applied **and** every required key is known at the head. `missing_keys` counts the rest. `truncated: true` means `max_subscribed_keys` cut the learned set.

### Choosing between slots and calls
- **You know the layout** (an ERC-20 balance, a V3 `slot0`): list slots. It's the cheapest option.
- **You know the call your app makes** (a quote, `getReserves`, a lens contract): subscribe the call. This is the right default for anything non-trivial, including Uniswap V4, whose pools live inside one `PoolManager`.

## `skim run` flags
| Flag | Default | Meaning |
|---|---|---|
| `--config <path>` | none | the YAML above |
| `--rpc <addr>` | `127.0.0.1:8545` | JSON-RPC listen address (HTTP; single and batch requests) |
| `--cl-port <port>` | `9130` | consensus-layer libp2p TCP + discv5 UDP; QUIC on port+1 |
| `--el-port <port>` | `30388` | execution-layer RLPx TCP + discovery UDP |
| `--window <blocks>` | `256` | in-memory history for reads at recent blocks and reorgs |
| `--archive <path>` | off | on-disk archive (`redb`) for `retention: archive` subscriptions |
| `--archive-backfill <blocks>` | `0` | extend archived history backwards from retained BALs (~2 weeks) |
| `--max-logs-range <blocks>` | `2048` | largest range one `eth_getLogs` may scan |
| `--exclude-snap-peer <node-id>` | none | never use this peer for state (e.g. a node you benchmark against); repeatable |
| `--no-learned-patterns` | off | disable the similar-input learned tier (for A/B measurements) |

**Logging:** set `RUST_LOG`, e.g. `RUST_LOG=info,skim::rpc=debug`. That logs every RPC with its latency, and each `eth_call`'s fetch waves.

## Sizing
- **Memory:** tracked keys, their versions inside `--window`, and contract code. Measured: ~120 MB RSS for a few pools, tokens and quotes, following Sepolia.
- **Network:** applying blocks costs nothing extra (gossip carries the BAL). Fetches happen only for state skim hasn't seen.
- **Disk:** none, unless `--archive`.
