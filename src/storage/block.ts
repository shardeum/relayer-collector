import { Address, bigIntToHex, bytesToHex } from '@ethereumjs/util'
import { config } from '../config'
import * as db from './sqlite3storage'
import { Block as EthBlock } from '@ethereumjs/block'
import { Common, Hardfork } from '@ethereumjs/common'
import { Cycle, DbBlock } from '../types'
import { getLatestBlock } from '../cache/LatestBlockCache'
import { blockQueryDelayInMillis } from '../utils/block'
import { queryTransactionsByBlock } from './transaction'
import * as RLP from '@ethereumjs/rlp'
import { BaseTrie } from 'merkle-patricia-tree'

const evmCommon = new Common({ chain: 'mainnet', hardfork: Hardfork.Istanbul, eips: [3855] })

export type ShardeumBlockOverride = EthBlock & { number?: string; hash?: string }

export async function insertBlock(block: DbBlock): Promise<void> {
  try {
    const fields = Object.keys(block).join(', ')
    const placeholders = Object.keys(block).fill('?').join(', ')
    const values = db.extractValues(block)
    const sql = 'INSERT OR REPLACE INTO blocks (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    /*prettier-ignore*/ if (config.verbose) console.log('block: Successfully inserted block', block.number, block.hash)
  } catch (e) {
    console.log(e)
    /*prettier-ignore*/ console.log('block: Unable to insert block or it is already stored in to database', block.number, block.hash)
  }
}

export async function bulkInsertBlocks(blocks: DbBlock[]): Promise<void> {
  try {
    const fields = Object.keys(blocks[0]).join(', ')
    const placeholders = Object.keys(blocks[0]).fill('?').join(', ')
    const values = db.extractValuesFromArray(blocks)
    let sql = 'INSERT OR REPLACE INTO blocks (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < blocks.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    /*prettier-ignore*/ console.log('block: Successfully bulk inserted blocks', blocks.length)
  } catch (e) {
    console.log(e)
    /*prettier-ignore*/ console.log('block: Unable to bulk insert blocks', blocks.length)
  }
}

export async function upsertBlocksForCycle(cycle: Cycle): Promise<void> {
  await upsertBlocksForCycleCore(cycle.counter, cycle.cycleRecord.start)
}

export async function upsertBlocksForCycleCore(
  cycleCounter: number,
  startTimeInSeconds: number
): Promise<void> {
  /*prettier-ignore*/ console.log(`block: Creating blocks for cycle ${cycleCounter} with start timestamp ${startTimeInSeconds}`)
  const numBlocksPerCycle =
    config.blockIndexing.cycleDurationInSeconds / config.blockIndexing.blockProductionRate
  let firstBlockNumberForCycle = 0
  for (let i = 0; i < numBlocksPerCycle; i++) {
    const blockNumber = Math.floor(
      config.blockIndexing.initBlockNumber + cycleCounter * numBlocksPerCycle + i
    )
    if (i === 0) {
      firstBlockNumberForCycle = blockNumber
    }
    const newBlockTimestampInSecond =
      startTimeInSeconds +
      (blockNumber - config.blockIndexing.initBlockNumber - firstBlockNumberForCycle) *
      config.blockIndexing.blockProductionRate
    const newBlockTimestamp = newBlockTimestampInSecond * 1000
    const block = createNewBlock(blockNumber, newBlockTimestamp)
    /*prettier-ignore*/ if (config.verbose) console.log(`Block number: ${block.header.number}, timestamp: ${block.header.timestamp}, hash: ${bytesToHex(block.header.hash())}`)
    try {
      const readableBlock = await convertToReadableBlock(blockNumber, newBlockTimestamp)
      // console.log("before insertBlock to DB readableBlock.hash.toString()", readableBlock.hash.toString())
      await insertBlock({
        number: Number(block.header.number),
        numberHex: '0x' + block.header.number.toString(16),
        hash: readableBlock.hash.toString(), //bytesToHex(block.header.hash()),
        timestamp: newBlockTimestamp,
        cycle: cycleCounter,
        readableBlock: JSON.stringify(readableBlock),
      })
    } catch (e) {
      /*prettier-ignore*/ console.log(`block: Unable to create block ${blockNumber} for cycle ${cycleCounter}`, e)
    }
  }
  /*prettier-ignore*/ if (config.verbose) console.log(`block: Successfully created ${numBlocksPerCycle} blocks for cycle ${cycleCounter}`)
}

async function queryBlockByNumberWithoutDelay(blockNumber: number): Promise<DbBlock | null> {
  try {
    const sql = 'SELECT * FROM blocks WHERE number = ?'
    const values = [blockNumber]
    const block: DbBlock = await db.get(sql, values)
    return block
  } catch (e) {
    return null
  }
}

export async function queryBlockByNumber(blockNumber: number): Promise<DbBlock | null> {
  /*prettier-ignore*/ if (config.verbose) console.log('block: Querying block by number', blockNumber)
  try {
    const sql = 'SELECT * FROM blocks WHERE number = ?'
    const values = [blockNumber]
    const block: DbBlock = await db.get(sql, values)
    if (block && block.timestamp > Date.now() - blockQueryDelayInMillis()) {
      return null
    }
    return block
  } catch (e) {
    /*prettier-ignore*/ console.log('block: Unable to query block', blockNumber, e)
    return null
  }
}

export async function queryBlockByTag(tag: 'earliest' | 'latest'): Promise<DbBlock | null> {
  try {
    if (tag === 'earliest') {
      const block: DbBlock = await db.get(`SELECT * FROM blocks WHERE number = 0`)
      return block
    }
    const block: DbBlock = await getLatestBlock()
    return block
  } catch (e) {
    console.error('Error occurred while querying block by tag:', e)
    return null
  }
}

export async function queryBlockByHash(blockHash: string): Promise<DbBlock | null> {
  /*prettier-ignore*/ if (config.verbose) console.log('block: Querying block by hash', blockHash)
  try {
    const sql = 'SELECT * FROM blocks WHERE hash = ?'
    const values = [blockHash]
    const block: DbBlock = await db.get(sql, values)
    if (block && block.timestamp > Date.now() - blockQueryDelayInMillis()) {
      return null
    }
    return block
  } catch (e) {
    /*prettier-ignore*/ console.log('block: Unable to query block', blockHash, e)
    return null
  }
}

export async function upsertBlocksForCycles(cycles: Cycle[]): Promise<void> {
  /*prettier-ignore*/ if (config.verbose) console.log(`block: Creating blocks for ${cycles.length} cycles`)
  for (const cycle of cycles) {
    await upsertBlocksForCycle(cycle)
  }
}

export function createNewBlock(blockNumber: number, timestamp: number): EthBlock {
  const timestampInSecond = timestamp ? Math.round(timestamp / 1000) : Math.round(Date.now() / 1000)
  const blockData = {
    header: { number: blockNumber, timestamp: timestampInSecond },
    transactions: [],
    uncleHeaders: [],
  }
  const block = EthBlock.fromBlockData(blockData, { common: evmCommon })
  return block
}

async function convertToReadableBlock(
  blockNumber: number,
  timestamp: number
): Promise<ShardeumBlockOverride> {
  const timestampInSecond = timestamp ? Math.round(timestamp / 1000) : Math.round(Date.now() / 1000)
  const defaultBlock = {
    difficulty: '0x4ea3f27bc',
    extraData: '0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32',
    gasLimit: '0x4a817c800',
    gasUsed: '0x0',
    hash: '0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae',
    logsBloom:
      '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000',
    miner: '0xbb7b8287f3f0a933474a79eae42cbca977791171',
    mixHash: '0x4fffe9ae21f1c9e15207b1f472d5bbdd68c9595d461666602f2be20daf5e7843',
    nonce: '0x689056015818adbe',
    number: '0',
    parentHash: '0x0000000000000000000000000000000000000000000000000000000000000000',
    receiptsRoot: '0x0000000000000000000000000000000000000000000000000000000000000000',
    sha3Uncles: '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
    size: '0x220',
    stateRoot: '0x0000000000000000000000000000000000000000000000000000000000000000',
    timestamp: '0x55ba467c',
    totalDifficulty: '0x78ed983323d',
    transactions: [],
    transactionsRoot: '0x0000000000000000000000000000000000000000000000000000000000000000',
    uncles: [],
  }
  const correctParentHash =
    blockNumber == 0 ? defaultBlock.parentHash : (await queryBlockByNumberWithoutDelay(blockNumber - 1)).hash
  const rawTransactions = await queryTransactionsByBlock(blockNumber, null)
  const transactions = rawTransactions.map((tx) => tx.txHash)
  const txRoot = await calculateTransactionRoot(transactions)
  const headerObject = {
    parentHash: correctParentHash,
    number: '0x' + blockNumber.toString(16),
    timestamp: timestampInSecond,
    uncleHash: defaultBlock.sha3Uncles,
    coinbase: Address.fromString(defaultBlock.miner),
    stateRoot: defaultBlock.stateRoot,
    transactionsTrie: txRoot,
    receiptTrie: defaultBlock.receiptsRoot,
    logsBloom: defaultBlock.logsBloom,
    difficulty: defaultBlock.difficulty,
    gasLimit: defaultBlock.gasLimit,
    gasUsed: defaultBlock.gasUsed,
    extraData: defaultBlock.extraData,
    mixHash: defaultBlock.mixHash,
    nonce: defaultBlock.nonce,
  }
  // console.log("headerObject", headerObject)
  const blockData = {
    header: headerObject,
    transactions: [],
    uncleHeaders: [],
  }
  console.log("blockData", blockData)
  const block = EthBlock.fromBlockData(blockData, { common: evmCommon, skipConsensusFormatValidation: true })
  defaultBlock.parentHash = bytesToHex(block.header.parentHash)
  defaultBlock.number = bigIntToHex(block.header.number)
  defaultBlock.timestamp = bigIntToHex(block.header.timestamp)
  defaultBlock.hash = bytesToHex(block.header.hash())
  defaultBlock.transactions = transactions
  defaultBlock.transactionsRoot = txRoot
  // console.log("HASH OF DEFAULT ", defaultBlock.hash)
  return defaultBlock as unknown as ShardeumBlockOverride
}

async function calculateTransactionRoot(txns: string[]): Promise<string> {
  const trie = new BaseTrie()
  for (let i = 0; i < txns.length; i++) {
    // i now is also the transactionIndex of transaction in the block
    // eslint-disable-next-line security/detect-object-injection
    const currTxnHash = txns[i]
    const path = Buffer.from(RLP.encode(i))
    await trie.put(path, Buffer.from(currTxnHash))
  }
  const evaluatedTxnRoot = '0x' + trie.root.toString('hex')
  return evaluatedTxnRoot
}

export async function queryBlockCount(): Promise<number> {
  let blocks: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    const sql = `SELECT COUNT(*) FROM blocks`
    blocks = await db.get(sql, [])
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Block count', blocks)

  return blocks['COUNT(*)'] || 0
}

export async function queryLatestBlocks(count: number): Promise<DbBlock[]> {
  try {
    const sql = `SELECT * FROM blocks ORDER BY number DESC LIMIT ${count}`
    const blocks: DbBlock[] = await db.all(sql)
    if (config.verbose) console.log('block latest', blocks)
    return blocks
  } catch (e) {
    console.log(e)
  }
  return []
}
