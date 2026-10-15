// Send a transaction through skim and wait for its receipt, with a viem wallet client.
//
//   PRIVATE_KEY=0x… node send-transaction/send.mjs [to] [amount-in-ether]
//
// skim holds no keys: viem signs locally and calls eth_sendRawTransaction. skim checks the
// transaction against verified head state (chain id, nonce, balance), gossips it straight to
// its Ethereum peers, re-broadcasts it until a block includes it, and serves the receipt from
// the block's peers, checked against the header's receiptsRoot.
import { createPublicClient, createWalletClient, formatEther, http, parseEther } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import { sepolia } from 'viem/chains'

const transport = http(process.env.SKIM_RPC ?? 'http://127.0.0.1:8545')
const account = privateKeyToAccount(process.env.PRIVATE_KEY)
const wallet = createWalletClient({ account, chain: sepolia, transport })
const client = createPublicClient({ chain: sepolia, transport })

const to = process.argv[2] ?? account.address // default: a 0-value self-transfer
const value = parseEther(process.argv[3] ?? '0')

console.log(`from ${account.address}  balance ${formatEther(await client.getBalance({ address: account.address }))} ETH`)
const hash = await wallet.sendTransaction({ to, value }) // nonce ('pending'), gas, fees: all from skim
console.log(`sent ${hash}`)

const t0 = performance.now()
const receipt = await client.waitForTransactionReceipt({ hash, pollingInterval: 1_000 })
console.log(
  `included in block ${receipt.blockNumber} (index ${receipt.transactionIndex}), status ${receipt.status}, ` +
    `gas used ${receipt.gasUsed}, after ${((performance.now() - t0) / 1000).toFixed(1)} s`,
)
