"""Compare skim probe-cl per-slot arrival with Lighthouse log arrival for the same slots.

usage: python3 -I compare.py <probe-cl.jsonl> <lighthouse.log> [utc_offset_hours=9]
"""
import json, re, sys
from datetime import datetime, timezone, timedelta

jsonl, lhlog = sys.argv[1], sys.argv[2]
off = timedelta(hours=float(sys.argv[3]) if len(sys.argv) > 3 else 9)
GENESIS = 1655733600

def pct(v, p):
    v = sorted(v)
    return v[round(p / 100 * (len(v) - 1))] if v else None

skim = {"envelope": {}, "block": {}}
sessions = []
for line in open(jsonl):
    d = json.loads(line)
    if d["ev"] in ("envelope", "block"):
        skim[d["ev"]].setdefault(d["slot"], d["lag_ms"])
    elif d["ev"] == "session":
        sessions.append(d)

lh = {"envelope": {}, "block": {}}
pat = re.compile(r"^(\w{3} \d{2} \d{2}:\d{2}:\d{2}\.\d{3}) INFO\s+New (envelope|block) received\s+slot: (\d+)")
for line in open(lhlog, errors="replace"):
    m = pat.match(line)
    if not m:
        continue
    t = datetime.strptime("2026 " + m.group(1), "%Y %b %d %H:%M:%S.%f").replace(tzinfo=timezone(off))
    slot = int(m.group(3))
    lag = t.timestamp() * 1e3 - (GENESIS + slot * 12) * 1e3
    lh[m.group(2)].setdefault(slot, lag)

out = {}
for kind in ("envelope", "block"):
    common = sorted(set(skim[kind]) & set(lh[kind]))
    s = [skim[kind][x] for x in common]
    l = [lh[kind][x] for x in common]
    diff = [a - b for a, b in zip(s, l)]
    out[kind] = {
        "skim_all": {"n": len(skim[kind]), "p50": pct(list(skim[kind].values()), 50), "p95": pct(list(skim[kind].values()), 95)},
        "matching_slots": len(common),
        "skim": {"p50": pct(s, 50), "p95": pct(s, 95)},
        "lighthouse": {"p50": pct(l, 50), "p95": pct(l, 95)},
        "skim_minus_lh": {"p50": pct(diff, 50), "p95": pct(diff, 95), "min": min(diff, default=None), "max": max(diff, default=None),
                          "skim_earlier_frac": (sum(1 for x in diff if x < 0) / len(diff)) if diff else None},
    }
    if skim[kind]:
        lo, hi = min(skim[kind]), max(skim[kind])
        out[kind]["lh_slots_in_window_missing_in_skim"] = sorted(set(x for x in lh[kind] if lo <= x <= hi) - set(skim[kind]))

lens = [s["secs"] for s in sessions if s.get("got_status")]
out["sessions_with_status"] = {"n": len(lens), "p50_s": pct(lens, 50), "p95_s": pct(lens, 95), "max_s": max(lens, default=None),
                                "over_60s": sum(1 for x in lens if x > 60), "over_300s": sum(1 for x in lens if x > 300)}
print(json.dumps(out, indent=1, default=lambda x: round(x, 1)))
