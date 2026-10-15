"""Summarize a `skim follow` JSONL (P0 runs): misses, rounds per call, breakdown, tails."""
import json, sys, collections
def pct(v, p):
    if not v: return None
    v = sorted(v); return round(v[min(len(v)-1, int(round(p/100*(len(v)-1))))], 1)
for path in sys.argv[1:]:
    rows = [json.loads(l) for l in open(path)]
    rounds_per_call = collections.Counter(); calls_with_miss = 0; calls = 0
    wall, queue, service, unavail, attempts, rerun = [], [], [], [], [], []
    per_call_block_ms = []
    for r in rows:
        by_call = collections.defaultdict(list)
        for m in r['misses']:
            if m['round'] == 'predict': continue
            by_call[m['call']].append(m)
            res = m['resolve']
            wall.append(res['wall_ms']); queue.append(res['queue_ms_max']); service.append(res['service_ms_max'])
            unavail.append(res['unavailable']); attempts.append(res['attempts_max']); rerun.append(m['rerun_us'])
        for c, ms in by_call.items():
            rounds_per_call[len(ms)] += 1
            per_call_block_ms.append(sum(m['resolve']['wall_ms'] for m in ms))
    n_blocks = len(rows)
    mism = sum(r['mismatches'] for r in rows); match = sum(r['matches'] for r in rows)
    sim = [r['sim_ms'] for r in rows]; env = [r['env_ms'] for r in rows]
    print(f"== {path}: {n_blocks} blocks, outputs match {match} / mismatch {mism}")
    print(f"  calls with a miss: {sum(rounds_per_call.values())}  (miss rounds per such call: {dict(sorted(rounds_per_call.items()))})")
    print(f"  miss rounds: {len(wall)}  wall p50/p95/p99/max {pct(wall,50)}/{pct(wall,95)}/{pct(wall,99)}/{max(wall) if wall else None} ms")
    print(f"    queue max p50/p95/p99 {pct(queue,50)}/{pct(queue,95)}/{pct(queue,99)}  service max p50/p95/p99 {pct(service,50)}/{pct(service,95)}/{pct(service,99)}")
    print(f"    root-unavailable answers per round p50/p95/max {pct(unavail,50)}/{pct(unavail,95)}/{max(unavail) if unavail else None}  attempts max {max(attempts) if attempts else None}  re-run p50 {pct(rerun,50)} us")
    print(f"  per-call total miss cost p50/p95/p99/max {pct(per_call_block_ms,50)}/{pct(per_call_block_ms,95)}/{pct(per_call_block_ms,99)}/{max(per_call_block_ms) if per_call_block_ms else None} ms")
    print(f"  block: env p50 {pct(env,50)}  all-calls-done p50/p95/p99 {pct(sim,50)}/{pct(sim,95)}/{pct(sim,99)} ms after slot")
