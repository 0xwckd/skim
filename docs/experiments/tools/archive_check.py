"""M4 check: historical reads from skim's selective archive vs a reference archive node.

usage: python3 -I archive_check.py <skim-url> <reference-url> <first-archived-block>

Queries blocks older than skim's in-memory window (so they are served from the archive):
  - archived slots / balance            -> must equal the reference
  - historical eth_call (archived quote) -> must equal the reference
  - an unarchived slot                   -> must be -32002 (never a guess)
  - a block before the subscription      -> must be -32002
"""
import json, sys, urllib.request

SKIM, REF, FIRST = sys.argv[1], sys.argv[2], int(sys.argv[3])
POOL = "0x6Ce0896eAE6D4BD668fDe41BB784548fb8F59b50"
QUOTER = "0xEd1f6473345F45b75F8179591dd5bA1888cf2FB3"
QUOTE = ("0xc6a5026a000000000000000000000000fff9976782d46cc05630d1f6ebab18b2324d6b14"
         "0000000000000000000000001c7d4b196cb0c7b01d743fbc6116a902379c7238"
         "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
         "0000000000000000000000000000000000000000000000000000000000000bb8"
         "0000000000000000000000000000000000000000000000000000000000000000")


def rpc(url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json", "user-agent": "skim-archive-check"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


head = int(rpc(SKIM, "eth_blockNumber", [])["result"], 16)
old_end = head - 40  # well outside a small --window
blocks = sorted(set(list(range(FIRST, old_end, max(1, (old_end - FIRST) // 12))) + [old_end]))
ok = bad = 0
for n in blocks:
    tag = hex(n)
    cases = [
        ("slot0", "eth_getStorageAt", [POOL, "0x0", tag]),
        ("liquidity", "eth_getStorageAt", [POOL, "0x4", tag]),
        ("feeGrowth0", "eth_getStorageAt", [POOL, "0x1", tag]),
        ("balance", "eth_getBalance", [POOL, tag]),
        ("quote 1 WETH", "eth_call", [{"to": QUOTER, "data": QUOTE}, tag]),
    ]
    for name, m, p in cases:
        s, r = rpc(SKIM, m, p), rpc(REF, m, p)
        good = s.get("result") is not None and s.get("result") == r.get("result")
        ok += good
        bad += not good
        if not good:
            print(f"  BAD block {n} {name}: skim={s.get('result', s.get('error'))} ref={r.get('result', r.get('error'))}")
    print(f"block {n}: checked")

# Must refuse, not guess.
unarch = rpc(SKIM, "eth_getStorageAt", [POOL, "0x777", hex(old_end)])        # never read/archived (value is 0 on chain)
before = rpc(SKIM, "eth_getStorageAt", [POOL, "0x0", hex(FIRST - 5)])       # before the subscription
print(f"\nhistorical reads matching the reference: {ok}/{ok + bad}")
print("unarchived slot  ->", unarch.get("error", unarch.get("result")))
print("before coverage  ->", before.get("error", before.get("result")))
