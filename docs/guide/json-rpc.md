# JSON-RPC reference

skim serves standard Ethereum JSON-RPC over HTTP and WebSocket on the same address (`--rpc`, default `127.0.0.1:8545`; `ws://` for WebSocket), with single and batch requests. Unmodified viem, ethers, web3.py and Foundry (`cast`, `forge script --rpc-url`) work against it.

## Methods
| Method | Notes |
|---|---|
| `eth_call` | Local revm. Unknown state is fetched (verified) first. Honours `gasPrice` / `maxFeePerGas` / `maxPriorityFeePerGas` (0 if omitted, as nodes do), `accessList`, `authorizationList` (EIP-7702) and **state overrides** (`balance`, `nonce`, `code`, `state`, `stateDiff`). Blob fields are rejected (`-32602`), never ignored. |
| `eth_simulateV1` | Sequences of calls across simulated blocks (each call sees earlier calls' writes), with per-block `blockOverrides` (number, time, gas limit, fee recipient, base fee, prevrandao) and `stateOverrides`. Per call: `status`, `returnData`, `gasUsed`, `logs`, `error`. Not yet: `validation: true`, `traceTransfers`. |
| `debug_traceCall` | geth tracers: default (struct logs), `callTracer`, `prestateTracer`, `noopTracer`, with `stateOverrides`. Not yet: `blockOverrides`, other tracers. |
| `eth_estimateGas` | Binary search on local execution, once the first run made the state local. |
| `eth_getBalance`, `eth_getCode`, `eth_getStorageAt` | Tracked: local. Untracked: one verified fetch. |
| `eth_getTransactionCount` | `pending` adds transactions sent through this skim and not yet included (gap-free nonces only). |
| `eth_sendRawTransaction` | Checks chain id, signature, `nonce too low`, and `insufficient funds` against verified head state. Then gossips to all connected EL peers, re-broadcasting every 12 s until included (gives up after 30 min). Blob transactions are rejected. |
| `eth_getTransactionByHash` | Transactions sent through skim (pending or included), and any transaction in the last 2,048 indexed blocks. |
| `eth_getTransactionReceipt` | `null` until included. Then built from the block's receipts, fetched from peers and checked against `receiptsRoot`. |
| `eth_getLogs` | Address / topics / `fromBlock`–`toBlock` / `blockHash`. Range ≤ `--max-logs-range` (2,048). Only blocks whose bloom (and, for followed blocks, BAL) can match have their receipts fetched; receipts are verified. |
| `eth_getBlockByNumber`, `eth_getBlockByHash` | Header fields, plus transaction **hashes** when skim indexed the block. Full transaction objects: not yet. |
| `eth_blockNumber` | The latest **state-ready** block. |
| `eth_chainId`, `net_version`, `net_listening`, `eth_syncing`, `web3_clientVersion` | |
| `eth_gasPrice`, `eth_maxPriorityFeePerGas` | Head base fee + 1 gwei tip, and the 1 gwei tip. |
| `skim_subscriptionStatus` | Per subscription: `ready`, `block`, `required_keys`, `missing_keys`, `truncated`, `calls_ok`, `error`, `results` (for `on_block` calls). |
| `skim_status` | Head, real store sizes (subscribed / on-demand keys, bytecode, refused admissions), peer counts, light-client status. |

## WebSocket subscriptions
Every method works over WebSocket too. In addition, `eth_subscribe` / `eth_unsubscribe` support:

| Subscription | Pushes |
|---|---|
| `newHeads` | the header of each block whose state is ready |
| `logs` (with an `address` / `topics` filter) | each matching log of each new block, one notification per log |
| `skim_subscriptions` | after each block: every subscription's `ready`, `missing_keys`, `calls_ok`, `error` and latest `on_block` `results` |

viem's `watch*` helpers use these automatically with `webSocket(...)`.

## Block parameters
| Tag | Meaning |
|---|---|
| `latest` (or omitted) | The latest state-ready block. Pinned for a whole batch request. |
| a number, or `{blockNumber}` / `{blockHash}` (EIP-1898) | Inside the in-memory window: any block, for keys tracked at that block. Older: archived subscriptions only ([archive](archive.md)). |
| `pending` | The latest state (skim keeps no mempool), except for `eth_getTransactionCount`. |
| `safe` | The highest execution block a 2/3 supermajority of the sync committee signed off on (light client). `-32002` until verified. |
| `finalized` | The execution block of the latest finalized beacon block skim verified. `-32002` until verified. |
| `earliest` | Not tracked: `-32002`. |

**Light-client status:** consensus clients on Sepolia don't serve Gloas light-client data yet, so `safe` and `finalized` currently answer `-32002`. `skim_status.light_client` shows the state.

## Errors
| Code | When |
|---|---|
| `-32002` | Resource unavailable: no peer could serve the state, receipts or headers right now, or the block is outside the window and not archived. **skim never substitutes a guess**, so retrying later is safe. |
| `-32004` | Not supported: `eth_sendTransaction` (skim holds no keys; sign in your client), filters and subscriptions. |
| `-32005` | `eth_getLogs` range larger than `--max-logs-range`. |
| `-32000` | Transaction rejected (`nonce too low`, `insufficient funds…`, invalid signature, wrong chain id), or EVM halt. |
| `3` | Execution reverted. `data` carries the revert bytes, so viem/ethers decode custom errors as usual. |
| `-32602` / `-32601` | Bad params / unknown method. |

## Not yet
`eth_createAccessList`, `eth_feeHistory`, `eth_getProof`, `debug_traceTransaction`, full transactions in blocks, `newPendingTransactions`.

## Compatibility notes
- **Checked against a reference:** state overrides, `eth_simulateV1` and the three tracers were checked against Nethermind at the same block. All were identical (`docs/experiments/tools/sim_trace_check.py`).
- **Calls without `gas`:** skim uses the block gas limit; nodes differ here (Nethermind: 100M).
- **EIP-8037 gas breakdown in `callTracer`:** the field names aren't standardized across clients yet. skim says `executionGasUsed`, Nethermind `regularGasUsed`.
- **Fee recipient in `prestateTracer`:** it is included, as geth and Nethermind do, even though skim's calls charge no fees.
