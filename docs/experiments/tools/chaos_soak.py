"""Long-run correctness soak: skim must answer right or refuse, under restarts and key-set churn.

usage: REF_URL=<reference node> [SKIM_EXTRA_ARGS="--trusted-peer enode://…"] python3 -I chaos_soak.py <skim-binary> <out-dir> [minutes]

Every block, the same requests go to skim and to the reference at the same block hash:
  - random Uniswap V3 quotes (cold misses, prediction)
  - subscribed and unsubscribed storage slots
  - BLOCKHASH(n - k) for random k in 1..256 (contract-creation probe)
  - GASPRICE with an explicit gasPrice (request fields must be honoured)
  - archived slots at older blocks (the in-memory window is short, so these come from disk)
Each answer is MATCH, REFUSED (skim returned an error, e.g. -32002), or WRONG (skim returned a
value the reference contradicts). WRONG is the failure this soak exists to catch.

Chaos, every 6-12 minutes: SIGKILL + restart, graceful restart, or restart with the *other*
config (key-set change). The archive file persists across all of them.

Resources: skim_status every block; subscribed keys must stay <= max_subscribed_keys, and RSS
under RSS_LIMIT_MB if set. Exit status is nonzero on any WRONG answer or limit breach.

`--selftest` checks the classification rules (reverts and halts are outcomes to compare).
Reorgs are not injected here (live network); they're covered by deterministic store tests.
"""
import json, os, random, signal, subprocess, sys, time, urllib.request

if __name__ == "__main__" and len(sys.argv) > 1 and sys.argv[1] == "--selftest":
    pass  # handled below, after the helpers are defined
else:
    BIN, OUT = sys.argv[1], sys.argv[2]
MINUTES = float(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[1] != "--selftest" else 120
REF = os.environ.get("REF_URL", "")
PORT = 8547
SKIM = f"http://127.0.0.1:{PORT}"
MAX_KEYS = 60
RSS_LIMIT_MB = float(os.environ.get("RSS_LIMIT_MB", "0"))  # 0 = no limit
if sys.argv[1] != "--selftest":
    os.makedirs(OUT, exist_ok=True)

QUOTER = "0xEd1f6473345F45b75F8179591dd5bA1888cf2FB3"
POOL = "0x6Ce0896eAE6D4BD668fDe41BB784548fb8F59b50"
WETH, USDC = "fff9976782d46cc05630d1f6ebab18b2324d6b14", "1c7d4b196cb0c7b01d743fbc6116a902379c7238"
QUOTE_1 = ("0xc6a5026a" + WETH.rjust(64, "0") + USDC.rjust(64, "0") + f"{10**18:x}".rjust(64, "0")
           + f"{3000:x}".rjust(64, "0") + "0" * 64)

CONFIGS = {
    "A": f"""
state:
  subscriptions:
    - name: pool
      address: "{POOL}"
      account: true
      storage: ["0x0", "0x1", "0x2", "0x4"]
      retention: {{ mode: archive }}
    - name: quote
      calls: [{{ to: "{QUOTER}", data: "{QUOTE_1}" }}]
      on_block: true
      retention: {{ mode: archive }}
  limits: {{ max_subscribed_keys: {MAX_KEYS} }}
predictors:
  uniswap_v3:
    pools: ["{POOL}"]
    prefetch_budget: 16
""",
    "B": f"""
state:
  subscriptions:
    - name: pool
      address: "{POOL}"
      account: true
      storage: ["0x0", "0x1", "0x2", "0x4"]
      retention: {{ mode: archive }}
    - name: weth
      address: "0x{WETH}"
      account: true
  limits: {{ max_subscribed_keys: {MAX_KEYS} }}
""",
}


def rpc(url, method, params, timeout=60):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json", "user-agent": "skim-chaos"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:  # connection refused during restarts, timeouts
        return {"error": {"code": "transport", "message": str(e)}}


def initcode_returning(op_hex):
    """Creation code that executes `op_hex` (leaving one word) and returns it."""
    return "0x" + op_hex + "5f52" + "6020" + "5ff3"  # PUSH0 MSTORE PUSH1 32 PUSH0 RETURN


def blockhash_probe(n):
    return {"data": initcode_returning("67" + f"{n:016x}" + "40")}  # PUSH8 n BLOCKHASH


def random_quote(rnd):
    weth_in = rnd.random() < 0.5
    amt = int(10 ** (14 + 5 * rnd.random())) if weth_in else int(10 ** (3 + 7 * rnd.random()))
    tin, tout = (WETH, USDC) if weth_in else (USDC, WETH)
    return {"to": QUOTER, "data": "0xc6a5026a" + tin.rjust(64, "0") + tout.rjust(64, "0")
            + f"{amt:x}".rjust(64, "0") + f"{3000:x}".rjust(64, "0") + "0" * 64}


class Skim:
    def __init__(self):
        self.proc = None
        self.cfg = "A"
        self.archive = os.path.join(OUT, "chaos.redb")

    def start(self, cfg):
        self.cfg = cfg
        path = os.path.join(OUT, f"skim-{cfg}.yaml")
        with open(path, "w") as f:
            f.write(CONFIGS[cfg])
        log = open(os.path.join(OUT, "skim.log"), "ab")
        self.proc = subprocess.Popen(
            [BIN, "run", "--rpc", f"127.0.0.1:{PORT}", "--cl-port", "9170", "--el-port", "30409", "--window", "32",
             "--config", path, "--archive", self.archive] + os.environ.get("SKIM_EXTRA_ARGS", "").split(),
            stdout=log, stderr=log, env=dict(os.environ, RUST_LOG="info"))

    def stop(self, sig):
        if self.proc:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


HALT_WORDS = ("out of gas", "execution", "invalid opcode", "stack", "halt", "gas required exceeds")


def outcome(resp, side):
    """Execution outcome of a JSON-RPC response:
    ("ok", result) | ("revert", data) | ("halt",) | ("unavailable", why).
    Reverts and EVM halts are results to compare, not failures; only transport and resource
    errors (or, for skim, its -32002 refusals) are "unavailable"."""
    e = resp.get("error")
    if e is None:
        return ("ok", resp.get("result"))
    code, msg = e.get("code"), str(e.get("message", "")).lower()
    if code == 3:
        return ("revert", e.get("data"))
    if code == -32000 and any(w in msg for w in HALT_WORDS):
        return ("halt",)
    return ("unavailable", f"{side} {code} {msg[:60]}")


def classify(s, r):
    ro, so = outcome(r, "ref"), outcome(s, "skim")
    if ro[0] == "unavailable":
        return "ref_unavailable"
    if so[0] == "unavailable":
        return "refused"
    return "match" if so == ro else "WRONG"


def selftest():
    ok = {"result": "0x01"}
    rev = lambda d: {"error": {"code": 3, "message": "execution reverted", "data": d}}
    cases = [
        (ok, rev("0xaa"), "WRONG"),                                   # skim succeeded, ref reverted
        (rev("0xbb"), rev("0xaa"), "WRONG"),                          # both reverted, different data
        (rev("0xaa"), rev("0xaa"), "match"),                          # same revert
        (ok, {"result": "0x02"}, "WRONG"),
        (ok, ok, "match"),
        ({"error": {"code": -32002, "message": "unavailable"}}, ok, "refused"),
        (ok, {"error": {"code": "transport", "message": "timeout"}}, "ref_unavailable"),
        (ok, {"error": {"code": -32000, "message": "header not found"}}, "ref_unavailable"),
        ({"error": {"code": -32000, "message": "execution halted: OutOfGas"}}, {"error": {"code": -32000, "message": "out of gas"}}, "match"),
        (ok, {"error": {"code": -32000, "message": "out of gas"}}, "WRONG"),  # ref halted, skim didn't
    ]
    bad = [(s, r, want, classify(s, r)) for s, r, want in cases if classify(s, r) != want]
    for b in bad:
        print("SELFTEST FAIL", b)
    print(f"selftest: {len(cases) - len(bad)}/{len(cases)} ok")
    sys.exit(1 if bad else 0)


if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
    selftest()


counts = {"match": 0, "refused": 0, "WRONG": 0, "ref_unavailable": 0}
by_kind = {}
wrong_log = open(os.path.join(OUT, "wrong.jsonl"), "a")
status_log = open(os.path.join(OUT, "status.jsonl"), "a")
events = open(os.path.join(OUT, "events.jsonl"), "a")
skim = Skim()
skim.start("A")
t_end = time.time() + MINUTES * 60
next_chaos = time.time() + random.uniform(6, 12) * 60
last = None
first_block = None
over_limit = 0
rss_max = 0.0
rnd = random.Random(7)

while time.time() < t_end:
    if time.time() >= next_chaos:
        kind = rnd.choice(["sigkill", "graceful", "switch-config"])
        cfg = ("B" if skim.cfg == "A" else "A") if kind == "switch-config" else skim.cfg
        skim.stop(signal.SIGKILL if kind == "sigkill" else signal.SIGTERM)
        events.write(json.dumps({"t": time.time(), "event": kind, "config": cfg}) + "\n"); events.flush()
        print(f"chaos: {kind} -> config {cfg}", flush=True)
        time.sleep(rnd.uniform(5, 40))
        skim.start(cfg)
        next_chaos = time.time() + rnd.uniform(6, 12) * 60
    head = rpc(SKIM, "eth_getBlockByNumber", ["latest", False], timeout=5).get("result")
    if not head or head["hash"] == last:
        time.sleep(1)
        continue
    last = head["hash"]
    n = int(head["number"], 16)
    first_block = first_block or n
    tag = {"blockHash": head["hash"]}
    reqs = [("quote", "eth_call", [random_quote(rnd), tag]) for _ in range(3)]
    reqs += [("quote_sub", "eth_call", [{"to": QUOTER, "data": QUOTE_1}, tag])]
    reqs += [("slot_sub", "eth_getStorageAt", [POOL, hex(s), tag]) for s in (0, 4)]
    reqs += [("slot_cold", "eth_getStorageAt", [POOL, hex(rnd.randrange(5, 12)), tag])]
    reqs += [("blockhash", "eth_call", [blockhash_probe(n - rnd.randrange(1, 257)), tag])]
    reqs += [("gasprice", "eth_call", [{"data": initcode_returning("3a"), "gas": "0x4c4b40", "gasPrice": hex(rnd.randrange(1, 10**10))}, tag])]
    if n - first_block > 40:
        old = rnd.randrange(first_block, n - 35)
        reqs += [("archived", "eth_getStorageAt", [POOL, "0x0", hex(old)])]
    for kind, m, p in reqs:
        s = rpc(SKIM, m, p)
        if s.get("error", {}).get("code") == "transport":
            continue
        r = rpc(REF, m, p)
        c = classify(s, r)
        counts[c] += 1
        by_kind.setdefault(kind, {}).setdefault(c, 0)
        by_kind[kind][c] += 1
        if c == "WRONG":
            wrong_log.write(json.dumps({"block": n, "kind": kind, "method": m, "params": p, "skim": s, "ref": r}) + "\n")
            wrong_log.flush()
    st = rpc(SKIM, "skim_status", [], timeout=5).get("result")
    sub = rpc(SKIM, "skim_subscriptionStatus", [], timeout=5).get("result")
    rss = subprocess.run(["ps", "-o", "rss=", "-p", str(skim.proc.pid)], capture_output=True, text=True).stdout.strip()
    rss_max = max(rss_max, int(rss or 0) / 1024)
    if st and st["store"]["subscribed_keys"] > MAX_KEYS:
        over_limit += 1
    status_log.write(json.dumps({"t": time.time(), "block": n, "config": skim.cfg, "rss_mb": int(rss or 0) / 1024,
                                 "status": st, "subs": sub}) + "\n")
    status_log.flush()
    print(f"block {n} [{skim.cfg}] {counts} rss={int(rss or 0)//1024}MB subscribed={st and st['store']['subscribed_keys']}", flush=True)

skim.stop(signal.SIGTERM)
failures = []
if counts["WRONG"]:
    failures.append(f"{counts['WRONG']} wrong answers (see wrong.jsonl)")
if over_limit:
    failures.append(f"subscribed keys over the limit in {over_limit} samples")
if RSS_LIMIT_MB and rss_max > RSS_LIMIT_MB:
    failures.append(f"RSS {rss_max:.0f} MB over the {RSS_LIMIT_MB:.0f} MB limit")
summary = {"minutes": MINUTES, "counts": counts, "by_kind": by_kind, "subscribed_over_limit_samples": over_limit,
           "rss_max_mb": rss_max, "verdict": "FAIL" if failures else "PASS", "failures": failures}
json.dump(summary, open(os.path.join(OUT, "summary.json"), "w"), indent=2)
print(json.dumps(summary, indent=2))
sys.exit(1 if failures else 0)
