// Live pushes from skim over WebSocket, with plain viem.
//
//   skim run --config examples/uniswap-v3/skim.yaml        # serves ws:// and http:// on :8545
//   cd examples && npm run ws
//
// Three kinds of subscription:
//   newHeads            each block whose state is ready (viem: watchBlocks / watchBlockNumber)
//   logs                matching logs per block (viem: watchEvent / watchContractEvent)
//   skim_subscriptions  after every block: each subscription's readiness and its latest
//                       on_block call results (skim-specific; via transport.subscribe)
import { createPublicClient, decodeFunctionResult, parseAbi, parseAbiItem, webSocket } from 'viem'
import { sepolia } from 'viem/chains'

const client = createPublicClient({
  chain: sepolia,
  transport: webSocket(process.env.SKIM_WS ?? 'ws://127.0.0.1:8545'),
})

client.watchBlockNumber({ onBlockNumber: (n) => console.log(`block ${n} ready`) })

client.watchEvent({
  event: parseAbiItem('event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)'),
  onLogs: (logs) => logs.forEach((l) => console.log(`swap in ${l.address} at tick ${l.args.tick}`)),
})

// The 1 WETH quote subscribed with on_block in uniswap-v3/skim.yaml, pushed every block.
const quoterAbi = parseAbi([
  'function quoteExactInputSingle((address,address,uint256,uint24,uint160)) returns (uint256 amountOut, uint160, uint32, uint256)',
])
await client.transport.subscribe({
  params: ['skim_subscriptions'],
  onData: ({ result }) => {
    for (const s of result.subscriptions) {
      if (!s.results.length || s.results[0].startsWith('error')) {
        console.log(`  ${s.name}: ready=${s.ready}`)
        continue
      }
      const [out] = decodeFunctionResult({ abi: quoterAbi, functionName: 'quoteExactInputSingle', data: s.results[0] })
      console.log(`  ${s.name}: ready=${s.ready}, 1 WETH -> ${Number(out) / 1e6} USDC`)
    }
  },
})
