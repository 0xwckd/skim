# skim

A P2P-backed Ethereum simulation client. skim follows a sparse slice of recent execution state
straight from Ethereum P2P (Gloas payload gossip, eth/71, snap/2), keeps it current by applying
EIP-7928 Block Access Lists instead of re-executing blocks, and runs simulations locally through
a Rust API and a JSON-RPC adapter (`eth_call`, `eth_estimateGas`, ...). No full node, no RPC provider.

Status: design v0.4, M0 (network feasibility) in progress. Target: Sepolia / Glamsterdam.

See [PROJECT.md](PROJECT.md) for the design, scope, trust model and plan.
