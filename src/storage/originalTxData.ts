import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import { config } from '../config/index'
import {
  InternalTXType,
  TransactionType,
  OriginalTxData,
  OriginalTxData2,
  OriginalTxDataInterface,
  TransactionSearchType,
} from '../types'
import { getTransactionObj, isStakingEVMTx, getStakeTxBlobFromEVMTx } from '../utils/decodeEVMRawTx'
import { bytesToHex } from '@ethereumjs/util'
import { Utils as StringUtils } from '@shardus/types'

type DbOriginalTxData = OriginalTxData & {
  originalTxData: string
  sign: string
}

enum OriginalTxDataType {
  OriginalTxData = 'originalTxsData',
  OriginalTxData2 = 'originalTxsData2',
}

export const originalTxsMap: Map<string, number> = new Map()

export async function insertOriginalTxData(
  originalTxData: OriginalTxData | OriginalTxData2,
  tableName: OriginalTxDataType
): Promise<void> {
  try {
    const fields = Object.keys(originalTxData).join(', ')
    const placeholders = Object.keys(originalTxData).fill('?').join(', ')
    const values = extractValues(originalTxData)
    const sql = `INSERT OR REPLACE INTO ${tableName} (` + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    if (config.verbose) console.log(`Successfully inserted ${tableName}`, originalTxData.txId)
  } catch (e) {
    console.log(e)
    console.log(`Unable to insert ${tableName} or it is already stored in to database`, originalTxData)
  }
}

export async function bulkInsertOriginalTxsData(
  originalTxsData: OriginalTxData[] | OriginalTxData2[],
  tableName: OriginalTxDataType
): Promise<void> {
  try {
    const fields = Object.keys(originalTxsData[0]).join(', ')
    const placeholders = Object.keys(originalTxsData[0]).fill('?').join(', ')
    const values = extractValuesFromArray(originalTxsData)
    let sql = `INSERT OR REPLACE INTO ${tableName} (` + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < originalTxsData.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    console.log(`Successfully bulk inserted ${tableName}`, originalTxsData.length)
  } catch (e) {
    console.log(e)
    console.log(`Unable to bulk insert ${tableName}`, originalTxsData.length)
    throw e // check with Achal/Jai
  }
}

export async function processOriginalTxData(
  originalTxsData: OriginalTxData[],
  saveOnlyNewData = false
): Promise<void> {
  if (originalTxsData && originalTxsData.length <= 0) return
  const bucketSize = 1000
  let combineOriginalTxsData: OriginalTxData[] = []
  let combineOriginalTxsData2: OriginalTxData2[] = []

  for (const originalTxData of originalTxsData) {
    const { txId, timestamp } = originalTxData
    if (originalTxsMap.has(txId) && originalTxsMap.get(txId) === timestamp) continue
    originalTxsMap.set(txId, timestamp)
    /* prettier-ignore */ if (config.verbose) console.log('originalTxData', originalTxData)
    if (saveOnlyNewData) {
      const originalTxDataExist = await queryOriginalTxDataByTxId(txId)
      if (originalTxDataExist) continue
    }
    combineOriginalTxsData.push(originalTxData)
    if (combineOriginalTxsData.length >= bucketSize) {
      await bulkInsertOriginalTxsData(combineOriginalTxsData, OriginalTxDataType.OriginalTxData)
      combineOriginalTxsData = []
    }
    if (!config.processData.indexOriginalTxData) continue
    try {
      if (originalTxData.originalTxData.tx.raw) {
        // EVM Tx
        const txObj = getTransactionObj(originalTxData.originalTxData.tx)
        /* prettier-ignore */ if (config.verbose) console.log('txObj', txObj)
        if (txObj) {
          let transactionType = TransactionType.Receipt
          if (isStakingEVMTx(txObj)) {
            const internalTxData = getStakeTxBlobFromEVMTx(txObj) as { internalTXType: InternalTXType }
            /* prettier-ignore */ if (config.verbose) console.log('internalTxData', internalTxData)

            if (internalTxData) {
              if (internalTxData.internalTXType === InternalTXType.Stake) {
                transactionType = TransactionType.StakeReceipt
              } else if (internalTxData.internalTXType === InternalTXType.Unstake) {
                transactionType = TransactionType.UnstakeReceipt
              } else console.log('Unknown staking evm tx type', internalTxData)
            }
          }
          combineOriginalTxsData2.push({
            txId: originalTxData.txId,
            timestamp: originalTxData.timestamp,
            cycle: originalTxData.cycle,
            txHash: bytesToHex(txObj.hash()),
            transactionType,
          })
        } else {
          console.log('Unable to get txObj from EVM raw tx', originalTxData.originalTxData.tx.raw)
        }
      } else {
        combineOriginalTxsData2.push({
          txId: originalTxData.txId,
          timestamp: originalTxData.timestamp,
          cycle: originalTxData.cycle,
          txHash: '0x' + originalTxData.txId,
          transactionType: TransactionType.InternalTxReceipt,
        })
      }
    } catch (e) {
      console.log('Error in processing original Tx data', originalTxData.txId, e)
    }
    if (combineOriginalTxsData2.length >= bucketSize) {
      await bulkInsertOriginalTxsData(combineOriginalTxsData2, OriginalTxDataType.OriginalTxData2)
      combineOriginalTxsData2 = []
    }
  }
  if (combineOriginalTxsData.length > 0)
    await bulkInsertOriginalTxsData(combineOriginalTxsData, OriginalTxDataType.OriginalTxData)
  if (combineOriginalTxsData2.length > 0)
    await bulkInsertOriginalTxsData(combineOriginalTxsData2, OriginalTxDataType.OriginalTxData2)
}

export async function queryOriginalTxDataCount(
  txType?: TransactionSearchType,
  afterTimestamp?: number,
  startCycle?: number,
  endCycle?: number
): Promise<number> {
  let originalTxsData: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    let sql = `SELECT COUNT(*) FROM originalTxsData`
    const values: unknown[] = []
    if (startCycle && endCycle) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    if (afterTimestamp) {
      if (startCycle && endCycle) sql += ` AND timestamp>?`
      else sql += ` WHERE timestamp>?`
      values.push(afterTimestamp)
    }
    if (txType) {
      sql = sql.replace('originalTxsData', 'originalTxsData2')
      if ((startCycle && endCycle) || afterTimestamp) sql += ` AND`
      else sql += ` WHERE`
      if (txType === TransactionSearchType.AllExceptInternalTx) {
        sql += ` transactionType!=?`
        values.push(TransactionType.InternalTxReceipt)
      } else if (
        txType === TransactionSearchType.Receipt ||
        txType === TransactionSearchType.NodeRewardReceipt ||
        txType === TransactionSearchType.StakeReceipt ||
        txType === TransactionSearchType.UnstakeReceipt ||
        txType === TransactionSearchType.InternalTxReceipt
      ) {
        const ty =
          txType === TransactionSearchType.Receipt
            ? TransactionType.Receipt
            : txType === TransactionSearchType.NodeRewardReceipt
            ? TransactionType.NodeRewardReceipt
            : txType === TransactionSearchType.StakeReceipt
            ? TransactionType.StakeReceipt
            : txType === TransactionSearchType.UnstakeReceipt
            ? TransactionType.UnstakeReceipt
            : TransactionType.InternalTxReceipt
        sql += ` transactionType=?`
        values.push(ty)
      }
    }
    originalTxsData = await db.get(sql, values)
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('OriginalTxData count', originalTxsData)
  return originalTxsData['COUNT(*)'] || 0
}

export async function queryOriginalTxsData(
  skip = 0,
  limit = 10,
  txType?: TransactionSearchType,
  afterTimestamp?: number,
  startCycle?: number,
  endCycle?: number
): Promise<OriginalTxDataInterface[]> {
  let originalTxsData: DbOriginalTxData[] = []
  try {
    let sql = `SELECT * FROM originalTxsData`
    const sqlSuffix = ` ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
    const values: unknown[] = []
    if (startCycle && endCycle) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    if (afterTimestamp) {
      if (startCycle && endCycle) sql += ` AND timestamp>?`
      else sql += ` WHERE timestamp>?`
      values.push(afterTimestamp)
    }
    if (txType) {
      sql = sql.replace('originalTxsData', 'originalTxsData2')
      if ((startCycle && endCycle) || afterTimestamp) sql += ` AND`
      else sql += ` WHERE`
      if (txType === TransactionSearchType.AllExceptInternalTx) {
        sql += ` transactionType!=?`
        values.push(TransactionType.InternalTxReceipt)
      } else if (
        txType === TransactionSearchType.Receipt ||
        txType === TransactionSearchType.NodeRewardReceipt ||
        txType === TransactionSearchType.StakeReceipt ||
        txType === TransactionSearchType.UnstakeReceipt ||
        txType === TransactionSearchType.InternalTxReceipt
      ) {
        const ty =
          txType === TransactionSearchType.Receipt
            ? TransactionType.Receipt
            : txType === TransactionSearchType.NodeRewardReceipt
            ? TransactionType.NodeRewardReceipt
            : txType === TransactionSearchType.StakeReceipt
            ? TransactionType.StakeReceipt
            : txType === TransactionSearchType.UnstakeReceipt
            ? TransactionType.UnstakeReceipt
            : TransactionType.InternalTxReceipt
        sql += ` transactionType=?`
        values.push(ty)
      }
    }
    sql += sqlSuffix
    originalTxsData = await db.all(sql, values)
    for (const originalTxData of originalTxsData) {
      if (txType) {
        const sql = `SELECT * FROM originalTxsData WHERE txId=?`
        const originalTxDataById: DbOriginalTxData = await db.get(sql, [originalTxData.txId])
        originalTxData.originalTxData = originalTxDataById.originalTxData
        originalTxData.sign = originalTxDataById.sign
      }
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = StringUtils.safeJsonParse(originalTxData.originalTxData)
      if (originalTxData.sign) originalTxData.sign = StringUtils.safeJsonParse(originalTxData.sign)
    }
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('OriginalTxData originalTxsData', originalTxsData)
  return originalTxsData as unknown as OriginalTxDataInterface[]
}

export async function queryOriginalTxDataByTxId(txId: string): Promise<OriginalTxDataInterface | null> {
  try {
    const sql = `SELECT * FROM originalTxsData WHERE txId=?`
    const originalTxData: DbOriginalTxData = await db.get(sql, [txId])
    if (originalTxData) {
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = StringUtils.safeJsonParse(originalTxData.originalTxData)
      if (originalTxData.sign) originalTxData.sign = StringUtils.safeJsonParse(originalTxData.sign)
    }
    if (config.verbose) console.log('OriginalTxData txId', originalTxData)
    return originalTxData as unknown as OriginalTxDataInterface
  } catch (e) {
    console.log(e)
  }
  return null
}

export async function queryOriginalTxDataByTxHash(txHash: string): Promise<OriginalTxDataInterface | null> {
  try {
    const sql = `SELECT * FROM originalTxsData2 WHERE txHash=?`
    const originalTxData: DbOriginalTxData = await db.get(sql, [txHash])
    if (originalTxData) {
      const sql = `SELECT * FROM originalTxsData WHERE txId=?`
      const originalTxDataById: DbOriginalTxData = await db.get(sql, [originalTxData.txId])
      originalTxData.originalTxData = originalTxDataById.originalTxData
      originalTxData.sign = originalTxDataById.sign
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = StringUtils.safeJsonParse(originalTxData.originalTxData)
      if (originalTxData.sign) originalTxData.sign = StringUtils.safeJsonParse(originalTxData.sign)
    }
    if (config.verbose) console.log('OriginalTxData txHash', originalTxData)
    return originalTxData as unknown as OriginalTxDataInterface
  } catch (e) {
    console.log(e)
  }
  return null
}

export async function queryOriginalTxDataCountByCycles(
  start: number,
  end: number
): Promise<{ originalTxsData: number; cycle: number }[]> {
  let originalTxsData: { cycle: number; 'COUNT(*)': number }[] = []
  try {
    const sql = `SELECT cycle, COUNT(*) FROM originalTxsData GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
    originalTxsData = await db.all(sql, [start, end])
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('OriginalTxData count by cycles', originalTxsData)

  return originalTxsData.map((originalTxData) => {
    return {
      originalTxsData: originalTxData['COUNT(*)'],
      cycle: originalTxData.cycle,
    }
  })
}

export function cleanOldOriginalTxsMap(timestamp: number): void {
  for (const [key, value] of originalTxsMap) {
    if (value < timestamp) {
      originalTxsMap.delete(key)
    }
  }
  if (config.verbose) console.log('Clean Old OriginalTxs map!', timestamp, originalTxsMap)
}
