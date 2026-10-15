// Uniswap V3 quotes and pool state through skim, with plain viem.
//
//   skim run --config examples/uniswap-v3/skim.yaml      # in another terminal
//   cd examples && npm install && npm run v3
//
// Nothing here is skim-specific except the URL: skim speaks Ethereum JSON-RPC.
import { createPublicClient, formatUnits, http, parseAbi, parseAbiItem, parseEther } from 'viem'
import { sepolia } from 'viem/chains'

const client = createPublicClient({
  chain: sepolia,
  transport: http(process.env.SKIM_RPC ?? 'http://127.0.0.1:8545'),
})

const QUOTER = '0xEd1f6473345F45b75F8179591dd5bA1888cf2FB3' // QuoterV2
const POOL = '0x6Ce0896eAE6D4BD668fDe41BB784548fb8F59b50' // WETH/USDC 0.3%
const WETH = '0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14'
const USDC = '0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238'

const poolAbi = parseAbi([
  'function slot0() view returns (uint160 sqrtPriceX96, int24 tick, uint16, uint16, uint16, uint8, bool)',
  'function liquidity() view returns (uint128)',
])
const quoterAbi = parseAbi([
  'function quoteExactInputSingle((address tokenIn, address tokenOut, uint256 amountIn, uint24 fee, uint160 sqrtPriceLimitX96) params) returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)',
])
const swapEvent = parseAbiItem(
  'event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)',
)

const ms = (t0) => `${(performance.now() - t0).toFixed(1)} ms`

async function quote(amountIn, fee = 3000, blockNumber) {
  const { result } = await client.simulateContract({
    address: QUOTER,
    abi: quoterAbi,
    functionName: 'quoteExactInputSingle',
    args: [{ tokenIn: WETH, tokenOut: USDC, amountIn, fee, sqrtPriceLimitX96: 0n }],
    blockNumber,
  })
  return result
}

// 1. Subscribed pool state: answered from skim's MVCC store, no network.
let t0 = performance.now()
const [[sqrtPriceX96, tick], liquidity] = await Promise.all([
  client.readContract({ address: POOL, abi: poolAbi, functionName: 'slot0' }),
  client.readContract({ address: POOL, abi: poolAbi, functionName: 'liquidity' }),
])
console.log(`pool  tick ${tick}  liquidity ${liquidity}  sqrtPriceX96 ${sqrtPriceX96}  (${ms(t0)})`)

// 2. Quotes. 1 WETH is subscribed (warm). Other sizes are new inputs: the V3 predictor fetches
//    the tick range each can reach in one wave the first time, then they are local too.
for (const eth of ['1', '0.37', '12.5']) {
  t0 = performance.now()
  const [out, , crossed] = await quote(parseEther(eth))
  console.log(`quote ${eth.padStart(5)} WETH -> ${formatUnits(out, 6).padStart(14)} USDC  ticks crossed ${crossed}  (${ms(t0)})`)
}

// 3. Every new block: re-quote at exactly that block.
const blocks = Number(process.env.BLOCKS ?? 3)
let seen = 0
await new Promise((resolve) => {
  const stop = client.watchBlockNumber({
    pollingInterval: 500,
    onBlockNumber: async (n) => {
      t0 = performance.now()
      const [out] = await quote(parseEther('1'), 3000, n)
      console.log(`block ${n}: 1 WETH -> ${formatUnits(out, 6)} USDC  (${ms(t0)})`)
      if (++seen >= blocks) {
        stop()
        resolve()
      }
    },
  })
})

// 4. Swap events of all three WETH/USDC pools over the last 500 blocks. skim checks each
//    block's logsBloom (and its BAL, for blocks it followed) and fetches receipts only where a
//    pool can have logged, verified against the block's receiptsRoot.
const POOLS = [POOL, '0x3289680dD4d6C10bb19b899729cda5eEF58AEfF1', '0x6418EEC70f50913ff0d756B48d32Ce7C02b47C47']
const head = await client.getBlockNumber()
t0 = performance.now()
const logs = await client.getLogs({ address: POOLS, event: swapEvent, fromBlock: head - 499n, toBlock: head })
console.log(`${logs.length} Swap events in the last 500 blocks (${ms(t0)})`)
for (const l of logs.slice(-3)) {
  console.log(`  block ${l.blockNumber} pool ${l.address.slice(0, 8)}…  amount0 ${l.args.amount0}  amount1 ${l.args.amount1}`)
}

// 5. skim's own view: is the subscribed state ready, and the latest on_block result.
const status = await client.request({ method: 'skim_subscriptionStatus', params: [] })
for (const s of status) console.log(`subscription ${s.name}: ready=${s.ready} block=${s.block} keys=${s.required_keys}`)
