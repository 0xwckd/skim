# JSON-RPC reference

skim serves standard Ethereum JSON-RPC over HTTP (`--rpc`, default `127.0.0.1:8545`), with single and batch requests. Unmodified viem, ethers, web3.py and Foundry (`cast`, `forge script --rpc-url`) work against it.

## Methods
| Method | Notes |
|---|---|
| `eth_call` | Local revm. Unknown state is fetched (verified) first. State overrides: not yet (`-32602`). |
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
| `skim_subscriptionStatus` | Per subscription: `ready`, `block`, `required_keys`, `missing_keys`, `truncated`, `results` (for `on_block` calls). |

## Block parameters
| Tag | Meaning |
|---|---|
| `latest` (or omitted) | The latest state-ready block. Pinned for a whole batch request. |
| a number, or `{blockNumber}` / `{blockHash}` (EIP-1898) | Inside the in-memory window: any block, for keys tracked at that block. Older: archived subscriptions only ([archive](archive.md)). |
| `pending` | The latest state (skim keeps no mempool), except for `eth_getTransactionCount`. |
| `safe`, `finalized`, `earliest` | Not tracked yet: `-32002`. |

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
`eth_simulateV1`, `eth_createAccessList`, `debug_traceCall`, `eth_feeHistory`, `eth_subscribe` (WebSocket), `eth_getProof`, state overrides, full transactions in blocks.
