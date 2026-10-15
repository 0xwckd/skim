"""Warm-path latency of a skim (or any) endpoint: sequential calls, then concurrent throughput.

usage: python3 -I latency_bench.py <url> [calls-per-kind]

Kinds (all at `latest`, after one warm-up call each):
  quote_sub   1 WETH -> USDC quote (subscribed in examples/uniswap-v3/skim.yaml)
  quote_amts  quotes for 10 fixed amounts, cycled (state local after warm-up)
  balanceOf   WETH.balanceOf(pool)
  storage     eth_getStorageAt(pool, slot0)
Client and server are on one machine; HTTP keep-alive is used.
"""
import http.client, json, sys, threading, time, urllib.parse

URL = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 500
u = urllib.parse.urlparse(URL)
QUOTER = "0xEd1f6473345F45b75F8179591dd5bA1888cf2FB3"
POOL = "0x6Ce0896eAE6D4BD668fDe41BB784548fb8F59b50"
WETH, USDC = "fff9976782d46cc05630d1f6ebab18b2324d6b14", "1c7d4b196cb0c7b01d743fbc6116a902379c7238"


def quote(amount):
    return {"to": QUOTER, "data": "0xc6a5026a" + WETH.rjust(64, "0") + USDC.rjust(64, "0") + f"{amount:x}".rjust(64, "0")
            + f"{3000:x}".rjust(64, "0") + "0" * 64}


KINDS = {
    "quote_sub": [("eth_call", [quote(10**18), "latest"])],
    "quote_amts": [("eth_call", [quote(int(10**17 * (1 + i * 0.7))), "latest"]) for i in range(10)],
    "balanceOf": [("eth_call", [{"to": "0x" + WETH, "data": "0x70a08231" + POOL[2:].lower().rjust(64, "0")}, "latest"])],
    "storage": [("eth_getStorageAt", [POOL, "0x0", "latest"])],
}


class Client:
    def __init__(self):
        self.c = http.client.HTTPConnection(u.hostname, u.port, timeout=120)

    def call(self, m, p):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": m, "params": p})
        self.c.request("POST", u.path or "/", body, {"content-type": "application/json"})
        r = json.loads(self.c.getresponse().read())
        return "error" not in r


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p / 100))]


c = Client()
out = {}
for kind, reqs in KINDS.items():
    for m, p in reqs:  # warm-up: make every key local
        for _ in range(3):
            c.call(m, p)
    lat, errs = [], 0
    for i in range(N):
        m, p = reqs[i % len(reqs)]
        t = time.perf_counter()
        ok = c.call(m, p)
        lat.append((time.perf_counter() - t) * 1e3)
        errs += not ok
    out[kind] = {"p50": round(pct(lat, 50), 3), "p90": round(pct(lat, 90), 3), "p99": round(pct(lat, 99), 3), "errors": errs}

for conc in (1, 10, 50):
    done = [0]
    stop = time.time() + 5
    def worker():
        cl = Client()
        while time.time() < stop:
            cl.call(*KINDS["quote_amts"][done[0] % 10])
            done[0] += 1
    ts = [threading.Thread(target=worker) for _ in range(conc)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    out[f"rps_conc{conc}"] = round(done[0] / 5)
print(json.dumps(out))
