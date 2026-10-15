#!/usr/bin/env python3
"""Compare Robinhood skim and Nitro at a single immutable block. No Python packages needed."""

import argparse
import json
import subprocess


def post(url, payload):
    result = subprocess.run(
        ["curl", "--fail-with-body", "--silent", "--show-error", "--max-time", "30",
         "--header", "content-type: application/json", "--data-binary", "@-", url],
        input=json.dumps(payload), text=True, capture_output=True, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"RPC transport failed (curl exit {result.returncode})")
    return json.loads(result.stdout)


def request(method, params, request_id=1):
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}


def rpc(url, method, params):
    reply = post(url, request(method, params))
    if "error" in reply:
        raise RuntimeError(f"{method}: {reply['error']}")
    return reply["result"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", choices=["mainnet", "testnet"], default="mainnet")
    parser.add_argument("--local", default="http://127.0.0.1:8545")
    parser.add_argument("--upstream")
    args = parser.parse_args()
    upstream = args.upstream or f"https://rpc.{args.network}.chain.robinhood.com"
    chain_id = 4663 if args.network == "mainnet" else 46630
    assert int(rpc(args.local, "eth_chainId", []), 16) == chain_id
    assert int(rpc(upstream, "eth_chainId", []), 16) == chain_id
    block = rpc(upstream, "eth_getBlockByNumber", ["latest", False])
    pinned = {"blockHash": block["hash"], "requireCanonical": True}
    arbsys = "0x0000000000000000000000000000000000000064"
    checks = [
        ("eth_call", [{"to": arbsys, "data": "0xd127f54a"}, pinned]),
        ("eth_call", [{"to": arbsys, "data": "0xa3b1b31d"}, pinned]),
        ("eth_getBalance", [arbsys, pinned]),
        ("eth_getCode", [arbsys, pinned]),
        ("eth_getTransactionCount", [arbsys, pinned]),
        ("eth_getStorageAt", [arbsys, "0x0", pinned]),
        ("eth_getProof", [arbsys, ["0x0"], pinned]),
    ]
    if args.network == "mainnet":
        weth = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"
        checks += [
            ("eth_call", [{"to": weth, "data": "0x95d89b41"}, pinned]),
            ("eth_call", [{"to": weth, "data": "0x313ce567"}, pinned]),
            ("eth_getStorageAt", [weth, "0x0", pinned]),
            ("eth_getCode", [weth, pinned]),
        ]
    # A batch exercises pinning and also reduces public RPC HTTP overhead.
    batch = [request(method, params, index + 1) for index, (method, params) in enumerate(checks)]
    reference = {reply["id"]: reply for reply in post(upstream, batch)}
    local = {reply["id"]: reply for reply in post(args.local, batch)}
    for index, (method, _) in enumerate(checks, 1):
        assert "result" in reference[index], f"reference {method}: {reference[index]}"
        assert local[index] == reference[index], f"{method} mismatch: {local[index]} != {reference[index]}"
    assert int(local[1]["result"], 16) == chain_id
    assert int(local[2]["result"], 16) == int(block["number"], 16)
    before = rpc(args.local, "skim_status", [])
    repeated = {reply["id"]: reply for reply in post(args.local, batch)}
    assert repeated == local
    after = rpc(args.local, "skim_status", [])
    assert after["cache"]["hits"] >= before["cache"]["hits"] + len(checks)
    assert after["upstreamRequired"] is True
    assert after["localExecution"] is False
    assert after["proofsVerified"] is False
    assert after["consensusVerified"] is False
    latest_batch = post(args.local, [
        request("eth_blockNumber", [], 1),
        request("eth_getBlockByNumber", ["latest", False], 2),
        request("eth_call", [{"to": arbsys, "data": "0xa3b1b31d"}, "latest"], 3),
    ])
    latest = {reply["id"]: reply["result"] for reply in latest_batch}
    assert int(latest[1], 16) == int(latest[2]["number"], 16) == int(latest[3], 16)
    refused = post(args.local, request("eth_sendRawTransaction", ["0x00"]))
    assert refused.get("error", {}).get("code") in [-32601, -32004]
    watches = rpc(args.local, "skim_subscriptionStatus", [])
    print(json.dumps({
        "network": args.network, "chainId": chain_id, "block": block["number"],
        "blockHash": block["hash"], "matchedReads": len(checks),
        "cacheHitsAdded": after["cache"]["hits"] - before["cache"]["hits"],
        "latestBatchConsistent": True, "transactionSubmissionRefused": True,
        "watchlists": len(watches),
    }, indent=2))


if __name__ == "__main__":
    main()
