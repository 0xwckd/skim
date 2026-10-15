# Showcase recording: `vhs assets/demo.tape` from the repo root, with skim already running
# on :8545 (examples/uniswap-v3 + uniswap-v4 subscriptions) and its log in data/ex/demo.log.
Output assets/demo.gif
Output assets/demo.mp4

Set Shell "bash"
Set FontSize 16
Set Width 1280
Set Height 560
Set Padding 22
Set Theme "Catppuccin Mocha"
Set TypingSpeed 28ms
Set WindowBar Colorful
Set CursorBlink false

Hide
Type "cd examples && export PS1='\[\e[38;5;141m\]❯\[\e[0m\] ' BLOCKS=2 && sed 's/\x1b\[[0-9;]*m//g' ../data/ex/demo.log | grep 'state ready' | sed -E 's/^([0-9T:-]{19})[^ ]* +INFO skim::run: /\1  /' > skim.log && clear" Enter
Sleep 500ms
Show

Type "# skim follows Sepolia from P2P gossip: no full node, no RPC provider." Enter
Sleep 600ms
Type "tail -n 3 skim.log" Enter
Sleep 2.2s
Type "cast block-number --rpc-url localhost:8545" Enter
Sleep 1.8s
Type "# Uniswap V3 through plain viem: pool state, quotes for any size, a quote per block, logs" Enter
Sleep 400ms
Type "npm run -s v3" Enter
Wait+Screen@60s /ready=true block=\d+ keys=5/
Sleep 3s
Type "clear" Enter
Type "# Uniswap V4: StateView, V4Quoter both ways, PoolManager logs filtered by pool id" Enter
Sleep 400ms
Type "npm run -s v4" Enter
Wait+Screen@60s /Swap events for this pool/
Sleep 3s
Type `ps -o rss= -p $(pgrep -f 'skim run') | awk '{print "skim:", int($1/1024), "MB of RAM, 0 bytes of disk"}'` Enter
Sleep 1.5s
Type "# Not every app needs a full node. Sync (and archive) only what you need." Enter
Sleep 3.5s
Hide
Type "trash skim.log" Enter
