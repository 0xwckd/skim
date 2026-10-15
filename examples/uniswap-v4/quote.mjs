// Uniswap V4 pool state, quotes and swap events through skim, with plain viem.
//
//   skim run --config examples/uniswap-v4/skim.yaml      # in another terminal
//   cd examples && npm install && npm run v4
import { createPublicClient, formatEther, formatUnits, http, parseAbi, parseAbiItem, parseEther } from 'viem'
import { sepolia } from 'viem/chains'

const client = createPublicClient({
  chain: sepolia,
  transport: http(process.env.SKIM_RPC ?? 'http://127.0.0.1:8545'),
})

const POOL_MANAGER = '0xE03A1074c86CFeDd5C142C4F04F1a1536e203543'
const STATE_VIEW = '0xE1Dd9c3fA50EDB962E442f60DfBc432e24537E4C'
const V4_QUOTER = '0x61B3f2011A92d183C7dbaDBdA940a7555Ccf9227'

// Native ETH / USDC, fee 20 (0.002%), tickSpacing 1, no hooks.
const poolKey = {
  currency0: '0x0000000000000000000000000000000000000000',
  currency1: '0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238',
  fee: 20,
  tickSpacing: 1,
  hooks: '0x0000000000000000000000000000000000000000',
}
const POOL_ID = '0xc743656d27fde4e2d5895e878557aaa56dd48c8656d25e9db35ba10b1fe3d824' // keccak256(abi.encode(poolKey))

const stateViewAbi = parseAbi([
  'function getSlot0(bytes32 poolId) view returns (uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)',
  'function getLiquidity(bytes32 poolId) view returns (uint128 liquidity)',
])
const quoterAbi = parseAbi([
  'function quoteExactInputSingle(((address currency0, address currency1, uint24 fee, int24 tickSpacing, address hooks) poolKey, bool zeroForOne, uint128 exactAmount, bytes hookData) params) returns (uint256 amountOut, uint256 gasEstimate)',
])
const swapEvent = parseAbiItem(
  'event Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)',
)

const ms = (t0) => `${(performance.now() - t0).toFixed(1)} ms`

// 1. Pool state via StateView (it reads PoolManager storage with extsload).
let t0 = performance.now()
const [[sqrtPriceX96, tick, , lpFee], liquidity] = await Promise.all([
  client.readContract({ address: STATE_VIEW, abi: stateViewAbi, functionName: 'getSlot0', args: [POOL_ID] }),
  client.readContract({ address: STATE_VIEW, abi: stateViewAbi, functionName: 'getLiquidity', args: [POOL_ID] }),
])
console.log(`pool  tick ${tick}  lpFee ${lpFee}  liquidity ${liquidity}  sqrtPriceX96 ${sqrtPriceX96}  (${ms(t0)})`)

// 2. Quotes, both directions. 0.001 ETH -> USDC is subscribed (warm); the others are new inputs.
async function quote(zeroForOne, exactAmount) {
  const { result } = await client.simulateContract({
    address: V4_QUOTER,
    abi: quoterAbi,
    functionName: 'quoteExactInputSingle',
    args: [{ poolKey, zeroForOne, exactAmount, hookData: '0x' }],
  })
  return result[0]
}
for (const eth of ['0.001', '0.0042']) {
  t0 = performance.now()
  const out = await quote(true, parseEther(eth))
  console.log(`quote ${eth} ETH -> ${formatUnits(out, 6)} USDC  (${ms(t0)})`)
}
t0 = performance.now()
const back = await quote(false, 5_000_000n) // 5 USDC
console.log(`quote 5 USDC -> ${formatEther(back)} ETH  (${ms(t0)})`)

// 3. This pool's Swap events over the last 2,000 blocks. PoolManager emits for every V4 pool,
//    so filter by the indexed pool id: only blocks whose bloom has this id are fetched.
const head = await client.getBlockNumber()
t0 = performance.now()
const logs = await client.getLogs({
  address: POOL_MANAGER,
  event: swapEvent,
  args: { id: POOL_ID },
  fromBlock: head - 1999n,
  toBlock: head,
})
console.log(`${logs.length} Swap events for this pool in the last 2,000 blocks (${ms(t0)})`)
for (const l of logs.slice(-3)) {
  console.log(`  block ${l.blockNumber}  amount0 ${l.args.amount0}  amount1 ${l.args.amount1}  tick ${l.args.tick}`)
}
