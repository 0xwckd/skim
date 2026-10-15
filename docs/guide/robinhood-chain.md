# Robinhood Chain

This fork supports Robinhood Chain mainnet (`4663`) and testnet (`46630`). Robinhood
uses Arbitrum Nitro. Ethereum's SNAP peers, EIP-7928 block access lists and Gloas gossip
are unavailable here, and vanilla revm does not implement ArbOS, Stylus, or Arbitrum's
precompiles and L1 block context. Changing the Ethereum chain ID would produce an
incorrect client.

The Robinhood backend therefore provides a bounded, in-memory **RPC read cache and
watchlist warmer**, with Nitro as its execution engine. It requires a public/provider
RPC or your own Nitro node. It does not provide upstream-independent local execution.

## Run

Rust is pinned by `rust-toolchain.toml`. From the repository root:

```bash
# Build only the Robinhood backend, without compiling the Ethereum P2P client.
cargo build --release -p skim-robinhood
./target/release/skim-robinhood --config examples/robinhood-chain/mainnet.yaml

# Testnet:
./target/release/skim-robinhood --config examples/robinhood-chain/testnet.yaml

# Use your own Nitro node:
./target/release/skim-robinhood --network mainnet --upstream http://127.0.0.1:8547

# Full skim CLI also exposes the same backend:
cargo build --release -p skim-cli
./target/release/skim robinhood --network mainnet
```

The server binds `127.0.0.1:8545` by default. Change it with `--rpc`. A startup handshake
checks the upstream chain ID and refuses a different network. `--network` and `--upstream`
override the corresponding config values. Default network is mainnet.

| Network | RPC | Explorer |
|---|---|---|
| Mainnet, 4663 | `https://rpc.mainnet.chain.robinhood.com` | <https://robinhoodchain.blockscout.com> |
| Testnet, 46630 | `https://rpc.testnet.chain.robinhood.com` | <https://explorer.testnet.chain.robinhood.com> |

Both use ETH for gas. See [Robinhood's official connection settings](https://docs.robinhood.com/chain/connecting/).

## Read semantics

- Every HTTP request/batch containing chain reads captures a fresh upstream head. All
  cached `latest` state reads in that batch use the same block hash, through EIP-1898. A failed head refresh returns an
  error instead of serving old values as `latest`.
- State reads and `eth_call` results are cached by method, complete parameters, and
  immutable block hash. State overrides remain part of the call and its cache key.
- Explicit block numbers, hash selectors, `earliest`, `safe` and `finalized` are resolved
  against the upstream. Moving tags are resolved anew. Canonical cache reads are checked
  against the current canonical chain; a reorg cannot reuse a value from another hash.
- `pending` requests bypass the cache. Errors, including revert data, pass through and
  are not cached. Missing state is never converted to zero.
- `eth_estimateGas` and `eth_createAccessList` pin their execution block without caching
  the result. Nitro computes estimates each time so ArbOS and L1 fee calculations are
  preserved. Log, fee-history and other passthrough methods retain upstream semantics.

Cache limits default to 10,000 entries and approximately 64 MiB of encoded entries.
Set `--cache-entries` and `--cache-bytes` to change them. Entries disappear on restart.
The byte limit counts serialized keys and values; allocator overhead is additional.
Canonical checks and head resolution still make upstream requests on cache hits; this
avoids repeating expensive calls but does not eliminate upstream traffic.

## Watchlists

See [mainnet.yaml](../../examples/robinhood-chain/mainnet.yaml) and
[testnet.yaml](../../examples/robinhood-chain/testnet.yaml). An account watch warms
balance, nonce and code; explicit slots warm storage reads; call watches warm exact
transaction inputs. `skim_subscriptionStatus` returns the selected block hash, results,
errors, and readiness for each subscription.

Watchlists sample the head every `--poll-ms` milliseconds (default 1,000). They may skip
intervening Nitro blocks. `ready` describes the reported snapshot, not an independently
verified latest or finalized state. Unknown configuration fields, `storage: all`, archive
retention, and Ethereum predictors are rejected instead of being silently ignored.
Watchlists allow at most 1,000 subscriptions and 1,000 active reads, with a 1 MiB
configuration budget and an 8 MiB serialized snapshot budget. A result that exceeds the
remaining snapshot budget becomes a `-32005` error; that subscription remains unready.

The public endpoints are rate limited. Large watchlists or history queries should use
your own node or a provider with sufficient capacity and historical state retention.

## RPC and trust boundary

HTTP JSON-RPC supports individual requests, notifications, and batches of up to 100
requests. The body limit is 1 MiB. `/health` checks live upstream head availability.
An aggregate batch response limit of 16 MiB prevents retaining arbitrarily large replies;
reads that exceed the remaining budget receive `-32005`.
There is no WebSocket subscription interface in Robinhood mode.

Cached methods: `eth_call`, `eth_getBalance`, `eth_getCode`, `eth_getStorageAt`,
`eth_getTransactionCount`, `eth_getProof`. Other supported block/transaction/log/fee reads
and Nitro gas estimates pass through. Wallet, transaction submission, admin, debug and
unsupported methods are refused. Sign and submit transactions through your chosen
wallet or a separate Nitro endpoint.

`skim_status` reports cache statistics and the backend's limitations. RPC headers, state,
proofs and call results are trusted from the upstream. `eth_getProof` is exposed but its
Merkle proof is **not verified** by this mode. This is not a trustless light client, a
consensus follower, a node replacement, or a selective archive. The original Ethereum
backend's performance figures and trust guarantees do not describe this mode.

## Verify

```bash
cargo test -p skim-robinhood
python3 examples/robinhood-chain/smoke.py

# Against a local testnet instance:
python3 examples/robinhood-chain/smoke.py --network testnet
```

The live script compares local and upstream results at one exact block hash, including
ArbSys (Nitro-native) calls, code, balances, nonce and storage. It checks cache reuse,
batch head consistency, the reported trust flags, and refusal of transaction submission.
Recorded results: [mainnet/testnet validation](../experiments/robinhood-validation.md).

Architecture references: [Arbitrum node FAQ](https://docs.arbitrum.io/run-a-node/faq),
[Nitro execution](https://developer.arbitrum.io/how-arbitrum-works/reference/geth),
[Robinhood execution differences](https://docs.robinhood.com/chain/differences-from-ethereum/),
[Robinhood full-node guide](https://docs.robinhood.com/chain/run-a-full-node/).
