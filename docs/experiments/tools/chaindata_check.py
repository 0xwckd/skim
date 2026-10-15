"""Check skim's transactions, receipts and logs against a reference node.

usage: python3 -I chaindata_check.py <skim-url> <reference-url> [blocks]

- eth_getTransactionReceipt / eth_getTransactionByHash for transactions of recent blocks
- eth_getLogs: address filter, topic-only filter, address+topic, blockHash, and an older range
Fields are compared exactly, except fields only one side returns.
"""
import json, sys, urllib.request

SKIM, REF = sys.argv[1], sys.argv[2]
BLOCKS = int(sys.argv[3]) if len(sys.argv) > 3 else 32
WETH = "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"
POOL = "0x6Ce0896eAE6D4BD668fDe41BB784548fb8F59b50"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SWAP = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


def rpc(url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json", "user-agent": "skim-check"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)


def same(a, b, path=""):
    """Differences between a and b on keys both have."""
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        for k in a.keys() & b.keys():
            out += same(a[k], b[k], f"{path}.{k}")
        return out
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return [f"{path}: len {len(a)} != {len(b)}"]
        return [d for x, y in zip(a, b) for d in same(x, y, path)]
    if isinstance(a, str) and isinstance(b, str):
        return [] if a.lower() == b.lower() else [f"{path}: {a} != {b}"]
    return [] if a == b else [f"{path}: {a} != {b}"]


head = int(rpc(SKIM, "eth_blockNumber", [])["result"], 16)
ok = bad = unavailable = 0

# Receipts and transactions: a few per block.
for n in range(head - 3, head + 1):
    blk = rpc(REF, "eth_getBlockByNumber", [hex(n), False])["result"]
    for h in blk["transactions"][:: max(1, len(blk["transactions"]) // 4)][:4]:
        for m in ("eth_getTransactionReceipt", "eth_getTransactionByHash"):
            s, r = rpc(SKIM, m, [h]), rpc(REF, m, [h])
            if "error" in s:
                unavailable += 1
                print(f"  {m} {h}: {s['error']}")
                continue
            d = same(s["result"], r["result"])
            ok += not d
            bad += bool(d)
            if d:
                print(f"  BAD {m} {h}: {d[:4]}")
print(f"receipts/transactions: {ok} match, {bad} differ, {unavailable} unavailable")

cases = [
    ("WETH Transfer", {"address": WETH, "topics": [TRANSFER]}, BLOCKS),
    ("pool (any event)", {"address": POOL}, BLOCKS),
    ("Swap topic, any address", {"topics": [SWAP]}, BLOCKS),
    ("two addresses", {"address": [WETH, POOL]}, BLOCKS),
]
for name, f, span in cases:
    f = dict(f, fromBlock=hex(head - span + 1), toBlock=hex(head))
    s, r = rpc(SKIM, "eth_getLogs", [f]), rpc(REF, "eth_getLogs", [f])
    if "error" in s:
        print(f"getLogs {name}: skim error {s['error']}")
        continue
    d = same(s["result"], r["result"])
    print(f"getLogs {name} ({span} blocks): skim {len(s['result'])} logs, ref {len(r['result'])}, {'MATCH' if not d else d[:4]}")

bh = rpc(REF, "eth_getBlockByNumber", [hex(head - 1), False])["result"]["hash"]
s, r = rpc(SKIM, "eth_getLogs", [{"blockHash": bh}]), rpc(REF, "eth_getLogs", [{"blockHash": bh}])
print(f"getLogs blockHash: skim {len(s.get('result', []))} ref {len(r['result'])} {s.get('error') or ('MATCH' if not same(s['result'], r['result']) else 'DIFF')}")
