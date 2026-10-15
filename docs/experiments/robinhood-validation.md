# Robinhood backend validation — 2026-10-16

Validated locally with Rust 1.97.0 on macOS arm64 against the official public mainnet
and testnet RPCs. These are functional checks, not a performance or uptime benchmark.

- `cargo test --locked --workspace --lib`: all library tests passed. The final targeted
  Robinhood run passed 28 tests, including the final response-budget and proof-shape fixes.
- `cargo test --locked -p skim-cli`: all four CLI unit tests passed.
- `cargo clippy --locked -p skim-robinhood --all-targets -- -D warnings`: passed.
- `cargo fmt -p skim-robinhood --check`: passed.
- `cargo check --locked -p skim-cli`: passed.
- `cargo build --locked --release -p skim-robinhood`: passed.

The live smoke script was run against the release executable on both networks:

| Check | Mainnet | Testnet |
|---|---|---|
| Chain ID | 4663 | 46630 |
| Selected block | `0x557a0b6` | `0x81e0970` |
| Selected hash | `0x6740b64c090ed2731f6f63d69fce1b4497dcc8bb0718a8f931b563d7d9cb2efb` | `0x3cd301eb2eff25f35c329a1c593df0f913632f01a2c0417e27f4e828aa41f5d9` |
| Exact-hash reads matched | 11/11 | 7/7 |
| Repeat reads hit cache | 11/11 | 7/7 |
| Latest batch block/call consistent | Yes | Yes |
| Transaction submission refused | Yes | Yes |

Read comparisons cover ArbSys chain ID and L2 block number, balance, code, nonce,
storage, and raw account/storage proofs. Mainnet additionally covers official WETH
symbol, decimals, code, and storage. All three mainnet watchlists reported ready without
errors during the run. A separate live probe confirmed hash-pinned gas estimation;
public access-list creation returns the upstream's method-unavailable error.

Mock tests exercise same-height reorgs and canonical orphan rejection, safe/finalized
tag movement, pending bypass, preserved call overrides and revert data, chain mismatch,
upstream outages, malformed/oversized responses, write-method refusal, memory budgets,
watch retries, and HTTP batch/notification behavior.

This mode trusts the upstream. Matching its answers does not establish independent
consensus or proof verification. No upstream-independent local execution or selective
archiving is implemented. See the [Robinhood guide](../guide/robinhood-chain.md).
