/* eslint-disable no-empty */
import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import { config } from '../config/index'
import { isArray } from 'lodash'
import { Utils as StringUtils } from '@shardus/types'

export interface Log<L = object> {
  cycle: number
  timestamp: number
  txHash: string
  blockNumber: number
  blockHash: string
  contractAddress: string
  log: L
  topic0: string
  topic1?: string
  topic2?: string
  topic3?: string
}

export interface LogQueryRequest {
  address?: string
  topics?: unknown[]
  fromBlock?: number
  toBlock?: number
  blockHash?: string
}

type DbLog = Log & {
  log: string
}

export async function insertLog(log: Log): Promise<void> {
  try {
    const fields = Object.keys(log).join(', ')
    const placeholders = Object.keys(log).fill('?').join(', ')
    const values = extractValues(log)
    const sql = 'INSERT OR REPLACE INTO logs (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    if (config.verbose) console.log('Successfully inserted Log', log.txHash, log.contractAddress)
  } catch (e) {
    console.log(e)
    console.log(
      'Unable to insert Log or it is already stored in to database',
      log.txHash,
      log.contractAddress
    )
  }
}

export async function bulkInsertLogs(logs: Log[]): Promise<void> {
  try {
    const fields = Object.keys(logs[0]).join(', ')
    const placeholders = Object.keys(logs[0]).fill('?').join(', ')
    const values = extractValuesFromArray(logs)
    let sql = 'INSERT OR REPLACE INTO logs (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < logs.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    console.log('Successfully bulk inserted Logs', logs.length)
  } catch (e) {
    console.log(e)
    console.log('Unable to bulk insert Logs', logs.length)
  }
}

function buildLogQueryString(
  request: LogQueryRequest,
  countOnly: boolean,
  type: string
): { sql: string; values: unknown[] } {
  let sql
  const queryParams = []
  const values = []
  if (countOnly) {
    sql = 'SELECT COUNT(txHash) FROM logs '
    if (type === 'txs') sql = 'SELECT COUNT(DISTINCT(txHash)) FROM logs '
  } else {
    sql = 'SELECT * FROM logs '
  }
  const fromBlock = request.fromBlock
  const toBlock = request.toBlock
  if (fromBlock && toBlock) {
    queryParams.push(`blockNumber BETWEEN ? AND ?`)
    values.push(fromBlock, toBlock)
  } else if (request.blockHash) {
    queryParams.push(`blockHash=?`)
    values.push(request.blockHash)
  }
  if (request.address) {
    queryParams.push(`contractAddress=?`)
    values.push(request.address)
  }

  const createTopicQuery = (topicIndex: number, topicValue: unknown): void => {
    const hexPattern = /^0x[a-fA-F0-9]{64}$/
    if (Array.isArray(topicValue)) {
      const validHexValues = topicValue.filter((value) => typeof value === 'string' && hexPattern.test(value))
      if (validHexValues.length > 0) {
        const query = `topic${topicIndex} IN (${validHexValues.map(() => '?').join(',')})`
        queryParams.push(query)
        values.push(...validHexValues)
      }
    } else if (typeof topicValue === 'string' && hexPattern.test(topicValue)) {
      queryParams.push(`topic${topicIndex}=?`)
      values.push(topicValue)
    }
  }
  // Handling topics array
  if (Array.isArray(request.topics)) {
    request.topics.forEach((topic, index) => createTopicQuery(index, topic))
  }
  sql = `${sql}${queryParams.length > 0 ? ` WHERE ${queryParams.join(' AND ')}` : ''}`
  return { sql, values }
}

export async function queryLogCount(
  contractAddress?: string,
  topics?: unknown[],
  fromBlock?: number,
  toBlock?: number,
  blockHash?: string,
  type = undefined
): Promise<number> {
  let logs: { 'COUNT(txHash)': number } | { 'COUNT(DISTINCT(txHash))': number } = { 'COUNT(txHash)': 0 }
  try {
    const { sql, values: inputs } = buildLogQueryString(
      {
        address: contractAddress,
        topics,
        fromBlock,
        toBlock,
        blockHash,
      },
      true,
      type
    )
    if (config.verbose) console.log(sql, inputs)
    logs = await db.get(sql, inputs)
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Log count', logs)

  if (logs && type === 'txs') return logs['COUNT(DISTINCT(txHash))']
  else if (logs) return logs['COUNT(txHash)']
  else return 0
}

export async function queryLogs(
  skip = 0,
  limit = 10,
  contractAddress?: string,
  topics?: unknown[],
  fromBlock?: number,
  toBlock?: number,
  type?: string
): Promise<Log[]> {
  let logs: DbLog[] = []
  try {
    const { sql, values: inputs } = buildLogQueryString(
      {
        address: contractAddress,
        topics,
        fromBlock,
        toBlock,
      },
      false,
      type
    )
    let sqlQueryExtension = ` ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
    if (type === 'txs') {
      sqlQueryExtension = ` GROUP BY txHash` + sqlQueryExtension
    }
    if (config.verbose) console.log(sql, inputs)
    logs = await db.all(sql + sqlQueryExtension, inputs)
    if (logs.length > 0) {
      logs.forEach((log: DbLog) => {
        if (log.log) (log as Log).log = StringUtils.safeJsonParse(log.log)
      })
    }
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Log logs', logs)
  return logs
}

export async function queryLogCountBetweenCycles(
  startCycleNumber: number,
  endCycleNumber: number
): Promise<number> {
  let logs: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    const sql = `SELECT COUNT(*) FROM logs WHERE cycle BETWEEN ? AND ?`
    logs = await db.get(sql, [startCycleNumber, endCycleNumber])
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) {
    console.log('Log count between cycle', logs)
  }

  return logs['COUNT(*)'] || 0
}

export async function queryLogsBetweenCycles(
  skip = 0,
  limit = 10000,
  startCycleNumber: number,
  endCycleNumber: number
): Promise<Log[]> {
  let logs: DbLog[] = []
  try {
    const sql = `SELECT * FROM logs WHERE cycle BETWEEN ? AND ? ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
    logs = await db.all(sql, [startCycleNumber, endCycleNumber])
    if (logs.length > 0) {
      logs.forEach((log: DbLog) => {
        if (log.log) (log as Log).log = StringUtils.safeJsonParse(log.log)
      })
    }
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) {
    console.log('Log logs between cycles', logs ? logs.length : logs, 'skip', skip)
  }

  return logs
}

export type LogFilter = {
  address: string[]
  topics: string[]
  fromBlock: string
  toBlock: string
  blockHash?: string
}

export async function queryLogsByFilter(logFilter: LogFilter, limit = 5000): Promise<Log[]> {
  let logs: DbLog[] = []
  const queryParams = []

  function createSqlFromEvmLogFilter(filter: LogFilter): string {
    const { fromBlock, toBlock, address, topics, blockHash } = filter

    let sql = `SELECT log FROM logs WHERE 1 = 1`

    if (isArray(address) && address.length > 0) {
      sql += ` AND contractAddress IN (${address.map(() => `?`).join(',')})`
      for (const addr of address) {
        queryParams.push(addr.toLowerCase())
      }
    }

    if (blockHash) {
      sql += ` AND blockHash = ?`
      queryParams.push(blockHash.toLowerCase())
    } else {
      if (fromBlock == 'latest') {
        sql += ` AND blockNumber >= (
                        SELECT MAX(blockNumber)
                        FROM logs
                  )`
      }
      if (fromBlock == 'earliest') {
        // genesis block
        sql += ` AND blockNumber >= 0`
      }
      if (fromBlock && fromBlock !== 'latest' && fromBlock !== 'earliest') {
        sql += ` AND blockNumber >= ?`
        queryParams.push(Number(fromBlock))
      }

      if (toBlock == 'latest') {
        sql += ` AND blockNumber <= (
                        SELECT MAX(blockNumber)
                        FROM logs
                  )`
      }
      if (toBlock == 'earliest') {
        // genesis block
        sql += ` AND blockNumber <= 0`
      }
      if (toBlock && toBlock !== 'latest' && toBlock !== 'earliest') {
        sql += ` AND blockNumber <= ?`
        queryParams.push(Number(toBlock))
      }
    }

    if (topics[0]) {
      sql += ` AND topic0 = ?`
      queryParams.push(topics[0].toLowerCase())
    }
    if (topics[1]) {
      sql += ` AND topic1 = ?`
      queryParams.push(topics[1].toLowerCase())
    }
    if (topics[2]) {
      sql += ` AND topic2 = ?`
      queryParams.push(topics[2].toLowerCase())
    }
    if (topics[3]) {
      sql += ` AND topic3 = ?`
      queryParams.push(topics[3].toLowerCase())
    }
    sql += ` ORDER BY blockNumber ASC LIMIT ?;`
    queryParams.push(limit)

    if (config.verbose) console.log(`queryLogsByFilter: Query: `, sql, queryParams)
    return sql
  }
  const sql = createSqlFromEvmLogFilter(logFilter)
  logs = await db.all(sql, queryParams)
  if (logs.length > 0) {
    logs.forEach((log: DbLog) => {
      if (log.log) (log as Log).log = StringUtils.safeJsonParse(log.log)
    })
  }

  return logs
}
