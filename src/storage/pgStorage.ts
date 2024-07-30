import { Client, types } from 'pg'
import { config } from '../config'
import { Utils as StringUtils } from '@shardus/types'

types.setTypeParser(20, Number);

const pgDefaultDBClient = new Client({ connectionString: config.pgDefaultDBConnectionString })
const pgShardeumIndexerDBClient = new Client({ connectionString: config.pgShardeumIndexerDBConnectionString })

const COLUMN_EXCHANGER = ['accountId', 'accountType', 'afterStateHash', 'amountSpent', 'amountSpent_decimal', 'appReceiptData', 'appliedReceipt', 'beforeStateAccounts', 'beforeStateHash', 'blockHash', 'blockNumber', 'contractAddress', 'contractInfo', 'contractType', 'cycleMarker', 'cycleRecord', 'ethAddress', 'executionShardKey', 'globalModification', 'internalTx', 'isGlobal', 'isInternalTx', 'numberHex', 'originalTxData', 'penalty', 'readableBlock', 'receiptId', 'tokenEvent', 'tokenFrom', 'tokenOperator', 'tokenTo', 'tokenType', 'tokenValue', 'totalStakeAmount', 'totalUnstakeAmount', 'transactionFee', 'transactionHash', 'transactionType', 'txFrom', 'txHash', 'txId', 'txTo', 'wrappedEVMAccount']

export type DbName = 'default' | 'shardeumIndexer'

export interface DbOptions {
  enableShardeumIndexer: boolean
}

export async function init(config: { enableShardeumIndexer: boolean }): Promise<void> {
  await pgDefaultDBClient.connect()
  pgDefaultDBClient.on('error', (err) => {
    console.error('default PG DB encountered an error', err.stack)
  })
  console.log('PG Database initialized.')

  if (config.enableShardeumIndexer) {
    await pgShardeumIndexerDBClient.connect()
    pgShardeumIndexerDBClient.on('error', (err) => {
      console.error('shardeumIndexer PG DB encountered an error', err.stack)
    })
    console.log('PG Shardeum indexer database initialized.')
  }
}

function getClient(dbName: DbName): Client {
  return pgDefaultDBClient
}

export async function runCreate(createStatement: string, dbName: DbName = 'default'): Promise<void> {
  await run(createStatement, [], dbName, false)
}

function fixSQLCasing(sql: string) {
  for (let index = 0; index < COLUMN_EXCHANGER.length; index++) {
    const word = COLUMN_EXCHANGER[index]
    sql = sql.replace(new RegExp(`(?<!")${word}(?!")`, "g"), `"${word}"`);
  }

  return sql
}

export async function run(
  sql: string,
  params: unknown[] | object = [],
  dbName: DbName = 'default',
  enforceCase = true
): Promise<{ rowCount: any }> {
  return new Promise((resolve, reject) => {
    if (enforceCase) sql = fixSQLCasing(sql)

    getClient(dbName).query({ text: sql, values: params }).then((res) => {
      resolve({ rowCount: res.rowCount })
    }).catch((err) => {
      console.log('Error running pg run sql: ' + sql)
      console.log(err)
      reject(err)
    })
  })
}


export async function get<T>(
  sql: string,
  params: unknown[] | object = [],
  dbName: DbName = 'default'
): Promise<T> {
  sql = fixSQLCasing(sql)

  return new Promise((resolve, reject) => {
    getClient(dbName).query({ text: sql, values: params }).then((res) => {
      resolve(res.rows?.[0])
    }).catch((err) => {
      console.log('Error running pg run sql: ' + sql)
      console.log(err)
      reject(err)
    })
  })
}


export async function all<T>(
  sql: string,
  params: unknown[] | object = [],
  dbName: DbName = 'default'
): Promise<T[]> {
  sql = fixSQLCasing(sql)

  return new Promise((resolve, reject) => {
    getClient(dbName).query({ text: sql, values: params }).then((res) => {
      resolve(res.rows)
    }).catch((err) => {
      console.log('Error running pg run sql: ' + sql)
      console.log(err)
      reject(err)
    })
  })
}

/**
 * Closes the Database and Indexer Connections Gracefully
 */
export async function close(): Promise<void> {
  try {
    console.log('Terminating PG Database/Indexer Connections...')
    await new Promise<void>((resolve, reject) => {
      pgDefaultDBClient.end().catch((err) => {
        if (err) {
          console.error('Error closing PG Database Connection.')
          reject(err)
        } else {
          console.log('PG Database connection closed.')
          resolve()
        }
      })
    })

    if (config.enableShardeumIndexer && pgShardeumIndexerDBClient) {
      await new Promise<void>((resolve, reject) => {
        pgShardeumIndexerDBClient.end().catch((err) => {
          if (err) {
            console.error('Error closing PG Indexer Connection.')
            reject(err)
          } else {
            console.log('Shardeum Indexer PG Database Connection closed.')
            resolve()
          }
        })
      })
    }
  } catch (err) {
    console.error('Error thrown in db close() function: ')
    console.error(err)
  }
}

export function extractValues(object: object): string[] {
  try {
    const inputs: string[] = []
    for (let value of Object.values(object)) {
      if (typeof value === 'object') value = StringUtils.safeStringify(value)
      inputs.push(value)
    }
    return inputs
  } catch (e) {
    console.log(e)
  }

  return []
}

export function extractValuesFromArray(arr: object[]): string[] {
  try {
    const inputs: string[] = []
    for (const object of arr) {
      for (let value of Object.values(object)) {
        if (typeof value === 'object') value = StringUtils.safeStringify(value)
        inputs.push(value)
      }
    }
    return inputs
  } catch (e) {
    console.log(e)
  }

  return []
}
