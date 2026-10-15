"""python3 assets/concept.py assets/concept.svg [freeze-at-seconds]. Generates: a 32 s looping explainer (CSS animations only, so GitHub plays it inline)."""
import sys
T = 32.0
css = []
n = 0

def pct(t):
    return f"{max(0.0, min(100.0, t / T * 100)):.3f}%"

def anim(frames, ease="ease-in-out"):
    """frames: [(t_seconds, 'css decls')]; returns a class name."""
    global n
    n += 1
    name = f"a{n}"
    ks = " ".join(f"{pct(t)}{{{d}}}" for t, d in frames)
    css.append(f"@keyframes {name}{{{ks}}} .{name}{{animation:{name} {T}s {ease} infinite;}}")
    return name

def show(t0, t1, fade=0.5, extra=""):
    """Visible between t0 and t1 (with fades)."""
    f = [(0, "opacity:0"), (t0, "opacity:0"), (t0 + fade, "opacity:1"), (t1 - fade, "opacity:1"), (t1, "opacity:0"), (T, "opacity:0")]
    return anim(f)

def rise(t0, t1, dy=14):
    f = [(0, f"opacity:0;transform:translateY({dy}px)"), (t0, f"opacity:0;transform:translateY({dy}px)"),
         (t0 + 0.6, "opacity:1;transform:translateY(0)"), (t1 - 0.4, "opacity:1;transform:translateY(0)"),
         (t1, "opacity:0;transform:translateY(0)"), (T, "opacity:0")]
    return anim(f)

def move(t0, t1, x0, x1, y0=0, y1=0, vis_from=None, vis_to=None):
    """A packet travelling from (x0,y0) to (x1,y1) between t0 and t1, hidden otherwise."""
    f = [(0, f"opacity:0;transform:translate({x0}px,{y0}px)"), (t0 - 0.01, f"opacity:0;transform:translate({x0}px,{y0}px)"),
         (t0, f"opacity:1;transform:translate({x0}px,{y0}px)"), (t1, f"opacity:1;transform:translate({x1}px,{y1}px)"),
         (t1 + 0.15, f"opacity:0;transform:translate({x1}px,{y1}px)"), (T, f"opacity:0;transform:translate({x1}px,{y1}px)")]
    return anim(f, "linear")

W, H = 1280, 720
body = []
add = body.append

# ---------- Scene 1: today (0 - 8 s) ----------
S1 = show(0.2, 8.0)
add(f'<g class="{S1}">')
add(f'<text x="640" y="92" class="h1" text-anchor="middle">Today, your app borrows someone else\'s node</text>')
def box(x, y, w, h, title, sub, cls="box", tcls="bt", scls="bs"):
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" class="{cls}"/>')
    add(f'<text x="{x+w/2}" y="{y+h/2-4}" class="{tcls}" text-anchor="middle">{title}</text>')
    if sub:
        add(f'<text x="{x+w/2}" y="{y+h/2+24}" class="{scls}" text-anchor="middle">{sub}</text>')
box(80, 250, 250, 120, "Your app", "viem · ethers · foundry")
box(515, 250, 250, 120, "RPC provider", "subscription plan")
box(950, 250, 250, 120, "Their full node", "the whole chain")
add('<line x1="330" y1="310" x2="515" y2="310" class="wire"/><line x1="765" y1="310" x2="950" y2="310" class="wire"/>')
for i in range(3):
    t = 1.2 + i * 1.6
    add(f'<circle r="7" class="pk {move(t, t+1.0, 340, 505, 310, 310)}"/>')
    add(f'<circle r="7" class="pk2 {move(t+1.0, t+1.9, 505, 340, 310, 310)}"/>')
tags = [("API keys", 1.6), ("rate limits", 2.4), ("$ per request", 3.2), ("outages you can't fix", 4.0), ("trust their answers", 4.8)]
for i, (tg, t) in enumerate(tags):
    x = 640 + (i - 2) * 205
    add(f'<g class="{rise(t, 8.0)}"><rect x="{x-95}" y="430" width="190" height="44" rx="22" class="tag"/>'
        f'<text x="{x}" y="459" class="tt" text-anchor="middle">{tg}</text></g>')
add(f'<text x="640" y="560" class="cap {rise(5.6, 8.0)}" text-anchor="middle">…and you can\'t check what they send you.</text>')
add('</g>')

# ---------- Scene 2: skim (8 - 16.5 s) ----------
S2 = show(8.2, 16.5)
add(f'<g class="{S2}">')
add('<text x="640" y="92" class="h1" text-anchor="middle">skim: your own peer on the Ethereum network</text>')
box(70, 270, 230, 110, "Your app", "same code, new URL")
add('<rect x="430" y="235" width="300" height="180" rx="20" class="skim"/>')
add('<text x="580" y="305" class="bt" text-anchor="middle" style="font-size:34px">skim</text>')
add('<text x="580" y="340" class="bs" text-anchor="middle">localhost:8545 · JSON-RPC</text>')
add('<text x="580" y="375" class="bs" text-anchor="middle">only your app\'s state</text>')
add('<line x1="300" y1="325" x2="430" y2="325" class="wire"/>')
for i in range(3):
    t = 9.6 + i * 1.9
    add(f'<circle r="7" class="pk {move(t, t+0.6, 310, 420, 325, 325)}"/>')
    add(f'<circle r="7" class="pk2 {move(t+0.65, t+1.2, 420, 310, 325, 325)}"/>')
# P2P mesh
nodes = [(960, 180), (1110, 150), (1210, 260), (1040, 300), (930, 420), (1090, 450), (1200, 400), (1010, 560), (1170, 560)]
edges = [(0,1),(1,2),(0,3),(1,3),(2,3),(3,4),(3,5),(2,6),(5,6),(4,5),(4,7),(5,7),(5,8),(6,8),(7,8)]
for a, b in edges:
    add(f'<line x1="{nodes[a][0]}" y1="{nodes[a][1]}" x2="{nodes[b][0]}" y2="{nodes[b][1]}" class="mesh"/>')
for i, (x, y) in enumerate(nodes):
    add(f'<circle cx="{x}" cy="{y}" r="15" class="node {anim([(0,"opacity:.55"),(8.5+i*0.25,"opacity:.55"),(8.8+i*0.25,"opacity:1"),(9.4+i*0.25,"opacity:.55"),(T,"opacity:.55")])}"/>')
add('<text x="1070" y="640" class="bs" text-anchor="middle">Ethereum P2P · no provider, no API key</text>')
add('<line x1="730" y1="290" x2="930" y2="230" class="wire2"/><line x1="730" y1="360" x2="930" y2="420" class="wire2"/>')
# packets from mesh to skim with labels
for i in range(3):
    t = 10.2 + i * 1.8
    add(f'<rect width="22" height="14" rx="4" class="blk {move(t, t+1.1, 920, 735, 225, 284)}"/>')
    add(f'<rect width="22" height="14" rx="4" class="prf {move(t+0.8, t+1.9, 920, 735, 414, 352)}"/>')
add(f'<g class="{rise(10.0, 16.5)}"><text x="830" y="210" class="lab" text-anchor="middle">every block + its access list</text>'
    '<text x="830" y="232" class="lab2" text-anchor="middle">consensus-layer gossip</text></g>')
add(f'<g class="{rise(11.0, 16.5)}"><text x="830" y="470" class="lab" text-anchor="middle">missing state + Merkle proof</text>'
    '<text x="830" y="492" class="lab2" text-anchor="middle">snap peers, on demand</text></g>')
add(f'<text x="560" y="610" class="cap {rise(12.5, 16.5)}" text-anchor="middle">Blocks say exactly what changed: no re-execution, no full sync.</text>')
add('</g>')

# ---------- Scene 3: verification (16.5 - 25.5 s) ----------
S3 = show(16.7, 25.5)
add(f'<g class="{S3}">')
add('<text x="640" y="92" class="h1" text-anchor="middle">Every value is proven against the block</text>')
add('<rect x="510" y="160" width="260" height="330" rx="18" class="gate"/>')
add('<text x="640" y="200" class="bt" text-anchor="middle" style="font-size:22px">verify</text>')
add('<line x1="120" y1="325" x2="510" y2="325" class="wire"/><line x1="770" y1="325" x2="1160" y2="325" class="wire"/>')
add('<text x="140" y="300" class="lab2">from any peer</text><text x="1150" y="300" class="lab2" text-anchor="end">into skim\'s state</text>')
checks = [("block access list", "keccak(rebuilt header) = block hash", 17.4),
          ("account · slot", "Merkle proof → stateRoot", 19.0),
          ("receipts · logs", "→ receiptsRoot", 20.6)]
for i, (a, b, t) in enumerate(checks):
    y = 560 + i * 44
    add(f'<rect width="26" height="18" rx="4" class="prf {move(t, t+0.8, 130, 640, 316, 316)}"/>')
    add(f'<rect width="26" height="18" rx="4" class="prf {move(t+0.8, t+1.5, 640, 1130, 316, 316)}"/>')
    add(f'<g class="{rise(t+0.7, 25.5, 8)}"><text x="380" y="{y}" class="ok">✓</text><text x="420" y="{y}" class="ckA">{a}</text>'
        f'<text x="640" y="{y}" class="ckB">{b}</text></g>')
# forged packet
add(f'<rect width="26" height="18" rx="4" class="bad {move(22.4, 23.2, 130, 560, 316, 316)}"/>')
add(f'<g class="{anim([(0,"opacity:0"),(23.2,"opacity:0"),(23.35,"opacity:1"),(25.3,"opacity:1"),(25.5,"opacity:0"),(T,"opacity:0")])}">'
    '<text x="640" y="352" class="x" text-anchor="middle">✕</text>'
    '<text x="640" y="455" class="bad2" text-anchor="middle">forged value</text><text x="640" y="477" class="bad3" text-anchor="middle">rejected · ask another peer</text></g>')
add(f'<text x="640" y="135" class="lab {rise(17.0, 25.5, 6)}" text-anchor="middle">unproven is never served · unknown is never zero</text>')
add(f'<text x="640" y="700" class="lab2 {rise(21.5, 25.5, 6)}" text-anchor="middle">(the chain head itself is trusted from gossip today; sync-committee light-client checks are next)</text>')
add('</g>')

# ---------- Scene 4: result (25.5 - 32 s) ----------
S4 = show(25.7, 31.9)
add(f'<g class="{S4}">')
add('<text x="640" y="110" class="h1" text-anchor="middle">Sync (and archive) only what you need</text>')
stats = [("~90 MB", "memory", 26.2), ("0 bytes", "on disk", 26.7), ("~3 ms", "eth_call, local", 27.2), ("= Infura", "byte for byte", 27.7)]
for i, (big, small, t) in enumerate(stats):
    x = 220 + i * 280
    add(f'<g class="{rise(t, 31.9)}"><rect x="{x-120}" y="200" width="240" height="170" rx="18" class="card"/>'
        f'<text x="{x}" y="285" class="big" text-anchor="middle">{big}</text><text x="{x}" y="325" class="bs" text-anchor="middle">{small}</text></g>')
add(f'<text x="640" y="450" class="cap {rise(28.4, 31.9)}" text-anchor="middle">vs. a synced Sepolia full node: 337 GB on disk</text>')
add(f'<text x="640" y="540" class="tag2 {rise(29.2, 31.9)}" text-anchor="middle">No more RPC subscriptions. P2P, with verification.</text>')
add(f'<text x="640" y="610" class="url {rise(29.8, 31.9)}" text-anchor="middle">github.com/leekt/skim</text>')
add('</g>')

style = """
.h1{font:700 40px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:#fff}
.bt{font:700 26px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:#fff}
.bs,.lab2{font:400 17px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:#9AA7C7}
.lab{font:600 19px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:#DCE6FF}
.cap{font:500 24px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:#C3D4FF}
.tt{font:600 17px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:#FFB4C0}
.ckA{font:700 20px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:#fff}
.ckB{font:400 20px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;fill:#8FF7E4}
.ok{font:800 24px ui-sans-serif,-apple-system,sans-serif;fill:#2EE6C5}
.x{font:800 64px ui-sans-serif,-apple-system,sans-serif;fill:#FF5C7A}
.bad2{font:700 20px ui-sans-serif,-apple-system,sans-serif;fill:#FF5C7A}
.bad3{font:400 17px ui-sans-serif,-apple-system,sans-serif;fill:#FFB4C0}
.big{font:800 46px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:url(#arc)}
.tag2{font:800 34px ui-sans-serif,-apple-system,'Segoe UI',Helvetica,Arial,sans-serif;fill:url(#arc)}
.url{font:500 22px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;fill:#9AA7C7}
.box{fill:#141D40;stroke:#3A4A80;stroke-width:2}
.skim{fill:#16204A;stroke:url(#arc);stroke-width:3}
.gate{fill:#101836;stroke:#2EE6C5;stroke-width:2;stroke-dasharray:6 6}
.card{fill:#121A3A;stroke:#2B3A70;stroke-width:1.5}
.tag{fill:#2A1530;stroke:#FF5C7A;stroke-width:1.5}
.wire{stroke:#3A4A80;stroke-width:3}
.wire2{stroke:#3A4A80;stroke-width:2;stroke-dasharray:6 7}
.mesh{stroke:#2B3A70;stroke-width:2}
.node{fill:#3FA8FF}
.pk{fill:#7C5CFF}.pk2{fill:#3FA8FF}
.blk{fill:#7C5CFF}.prf{fill:#2EE6C5}.bad{fill:#FF5C7A}
"""
svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" aria-label="skim concept: no RPC provider, your own verified peer on the Ethereum P2P network">
<defs>
<linearGradient id="bg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#111939"/><stop offset="1" stop-color="#070A16"/></linearGradient>
<linearGradient id="arc" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#7C5CFF"/><stop offset="0.55" stop-color="#3FA8FF"/><stop offset="1" stop-color="#2EE6C5"/></linearGradient>
</defs>
<style>{style}{chr(10).join(css)}__EXTRA__</style>
<rect width="{W}" height="{H}" fill="url(#bg)"/>
{chr(10).join(body)}
</svg>
'''
extra = ""
if len(sys.argv) > 2:  # debug: freeze at time t
    extra = f"*{{animation-play-state:paused!important;animation-delay:-{sys.argv[2]}s!important}}"
open(sys.argv[1], "w").write(svg.replace("__EXTRA__", extra))
