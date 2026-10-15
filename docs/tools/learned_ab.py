"""A/B: the same random Uniswap V3 quotes, every block, against two skim instances.

usage: python3 -I learned_ab.py <url-A> <url-B> [blocks] [quotes-per-block]

Prints call latency per instance. Fetch rounds are in each instance's debug log
("eth_call fetches").
"""
import json, random, sys, threading, time, urllib.request

A, B = sys.argv[1], sys.argv[2]
BLOCKS = int(sys.argv[3]) if len(sys.argv) > 3 else 40
K = int(sys.argv[4]) if len(sys.argv) > 4 else 8
QUOTER = "0xEd1f6473345F45b75F8179591dd5bA1888cf2FB3"
WETH, USDC = "fff9976782d46cc05630d1f6ebab18b2324d6b14", "1c7d4b196cb0c7b01d743fbc6116a902379c7238"


def rpc(url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)


def quotes(seed, n):
    rnd = random.Random(seed)
    out = []
    for _ in range(n):
        weth_in = rnd.random() < 0.5
        fee = rnd.choice([500, 3000, 10000])
        amt = int(10 ** (14 + 6 * rnd.random())) if weth_in else int(10 ** (3 + 8 * rnd.random()))
        tin, tout = (WETH, USDC) if weth_in else (USDC, WETH)
        data = "0xc6a5026a" + tin.rjust(64, "0") + tout.rjust(64, "0") + f"{amt:x}".rjust(64, "0") + f"{fee:x}".rjust(64, "0") + "0" * 64
        out.append({"to": QUOTER, "data": data})
    return out


lat = {A: [], B: []}
errs = {A: 0, B: 0}


def run(url, calls, n):
    for c in calls:
        t = time.time()
        r = rpc(url, "eth_call", [c, hex(n)])
        lat[url].append((time.time() - t) * 1e3)
        if "error" in r and r["error"].get("code") != 3:
            errs[url] += 1


last = None
done = 0
while done < BLOCKS:
    n = int(rpc(A, "eth_blockNumber", [])["result"], 16)
    nb = int(rpc(B, "eth_blockNumber", [])["result"], 16)
    n = min(n, nb)
    if n == last:
        time.sleep(1)
        continue
    last = n
    calls = quotes(n, K)
    ts = [threading.Thread(target=run, args=(u, calls, n)) for u in (A, B)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    done += 1


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p / 100))]


for name, u in (("A", A), ("B", B)):
    v = lat[u]
    print(f"{name} {u}: n={len(v)} p50={pct(v, 50):.1f} p90={pct(v, 90):.1f} p99={pct(v, 99):.1f} max={max(v):.0f} ms, "
          f">100ms: {sum(x > 100 for x in v)}, errors: {errs[u]}")
