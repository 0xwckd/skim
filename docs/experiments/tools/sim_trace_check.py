"""Compare skim's state overrides, eth_simulateV1 and debug_traceCall with a reference node.

usage: python3 -I sim_trace_check.py <skim-url> <reference-url>

Everything runs at the same block number on both. Results are compared field by field;
differences are printed. The reference must support eth_simulateV1 and debug_traceCall
(e.g. Nethermind with the Debug namespace enabled).
"""
import json, sys, urllib.request

SKIM, REF = sys.argv[1], sys.argv[2]
WETH = "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"
QUOTER = "0xEd1f6473345F45b75F8179591dd5bA1888cf2FB3"
X = "0x00000000000000000000000000000000000a11ce"
Y = "0x0000000000000000000000000000000000000b0b"
QUOTE = ("0xc6a5026a" + "fff9976782d46cc05630d1f6ebab18b2324d6b14".rjust(64, "0") + "1c7d4b196cb0c7b01d743fbc6116a902379c7238".rjust(64, "0")
         + f"{10**18:x}".rjust(64, "0") + f"{3000:x}".rjust(64, "0") + "0" * 64)


def rpc(url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)


def word(a):
    return a[2:].lower().rjust(64, "0")


def balance_of(a):
    return "0x70a08231" + word(a)


def keccak(hexdata):
    return rpc(SKIM, "web3_sha3", [hexdata])["result"] if False else rpc(REF, "web3_sha3", [hexdata])["result"]


def diff(a, b, path=""):
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        for k in sorted(a.keys() | b.keys()):
            if k not in a or k not in b:
                out.append(f"{path}.{k}: only in {'skim' if k in a else 'ref'}")
            else:
                out += diff(a[k], b[k], f"{path}.{k}")
        return out
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return [f"{path}: len {len(a)} vs {len(b)}"]
        return [d for i, (x, y) in enumerate(zip(a, b)) for d in diff(x, y, f"{path}[{i}]")]
    if isinstance(a, str) and isinstance(b, str):
        return [] if a.lower() == b.lower() else [f"{path}: {a[:66]} vs {b[:66]}"]
    return [] if a == b else [f"{path}: {a} vs {b}"]


n = int(rpc(SKIM, "eth_blockNumber", [])["result"], 16) - 1
tag = hex(n)
results = []


def check(name, method, params, pick=lambda r: r, ignore=()):
    s, r = rpc(SKIM, method, params), rpc(REF, method, params)
    if "error" in s or "error" in r:
        results.append((name, False, [f"skim: {s.get('error')}", f"ref: {r.get('error')}"]))
        return
    a, b = pick(s["result"]), pick(r["result"])
    d = [x for x in diff(a, b) if not any(i in x for i in ignore)]
    results.append((name, not d, d[:6]))


# WETH balances live in mapping slot 3: keccak(addr . 3)
slot_x = keccak("0x" + word(X) + "3".rjust(64, "0"))
five_eth = "0x" + f"{5 * 10**18:x}".rjust(64, "0")
check("eth_call + stateDiff override", "eth_call",
      [{"to": WETH, "data": balance_of(X)}, tag, {WETH: {"stateDiff": {slot_x: five_eth}}}])
check("eth_call + code override", "eth_call",
      [{"to": Y, "data": "0x"}, tag, {Y: {"code": "0x602a5f5260205ff3"}}])  # returns 42

sim = {"blockStateCalls": [
    {"stateOverrides": {X: {"balance": hex(10 * 10**18)}},
     "calls": [{"from": X, "to": WETH, "value": hex(10**18), "data": "0xd0e30db0"},
               {"from": X, "to": WETH, "data": balance_of(X)}]},
    {"calls": [{"from": X, "to": WETH, "data": "0xa9059cbb" + word(Y) + f"{5 * 10**17:x}".rjust(64, "0")},
               {"from": X, "to": WETH, "data": balance_of(Y)},
               {"from": X, "to": WETH, "data": balance_of(X)}]},
]}
check("eth_simulateV1 (2 blocks, deposit + transfer)", "eth_simulateV1", [sim, tag],
      pick=lambda r: [[{k: c.get(k) for k in ("status", "returnData", "gasUsed", "logs")} for c in b["calls"]] for b in r],
      ignore=("blockHash", "transactionHash", "blockTimestamp", "maxUsedGas"))
# Explicit gas: nodes differ in the default gas for calls without one (skim: block gas limit).
# EIP-8037 gas-breakdown field names aren't standardized yet (executionGasUsed / regularGasUsed).
check("debug_traceCall callTracer (V3 quote)", "debug_traceCall", [{"to": QUOTER, "data": QUOTE, "gas": "0x989680"}, tag, {"tracer": "callTracer"}],
      ignore=("executionGasUsed", "regularGasUsed"))
check("debug_traceCall prestateTracer (V3 quote)", "debug_traceCall", [{"to": QUOTER, "data": QUOTE, "gas": "0x989680"}, tag, {"tracer": "prestateTracer"}])
check("debug_traceCall default (WETH totalSupply)", "debug_traceCall", [{"to": WETH, "data": "0x18160ddd"}, tag, {}],
      pick=lambda r: {"gas": r.get("gas"), "failed": r.get("failed"), "returnValue": r.get("returnValue"),
                      "steps": len(r.get("structLogs", [])), "ops": [s["op"] for s in r.get("structLogs", [])]})

for name, ok, d in results:
    print(f"{'MATCH' if ok else 'DIFF '} {name}")
    for x in d:
        print("      ", x)
print(f"\n{sum(ok for _, ok, _ in results)}/{len(results)} identical at block {n}")
