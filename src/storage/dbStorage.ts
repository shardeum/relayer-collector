import * as pgDb from './pgStorage'
import * as sqliteDb from './sqlite3storage'
import { config } from '../config'

export type DbName = 'default' | 'shardeumIndexer'

const isPGEnabled = config.postgresEnabled

export async function init(): Promise<void> {
  if (isPGEnabled) {
    await pgDb.init({
      enableShardeumIndexer: config.enableShardeumIndexer
    })
  } else {
    await sqliteDb.init({
      defaultDbSqlitePath: 'db.sqlite3',
      enableShardeumIndexer: config.enableShardeumIndexer,
      shardeumIndexerSqlitePath: config.shardeumIndexerSqlitePath,
    })
  }
}

export async function runCreate(createStatement: string, dbName: DbName = 'default'): Promise<void> {
  if (isPGEnabled) {
    await pgDb.runCreate(createStatement, dbName)
  } else {
    await sqliteDb.runCreate(createStatement, dbName)
  }
}

export async function run(
  sql: string,
  params: unknown[] | object = [],
  dbName: DbName = 'default',
  enforceCase = true
): Promise<{ rowCount: any } | { id: number }> {
  let res = isPGEnabled ? { rowCount: 0 } : { id: 0 }
  if (isPGEnabled) {
    res = await pgDb.run(sql, params, dbName, enforceCase)
  } else {
    res = await sqliteDb.run(sql, params, dbName)
  }
  return res
}

export async function get<T>(
  sql: string,
  params: unknown[] | object = [],
  dbName: DbName = 'default'
): Promise<T> {
  const res = isPGEnabled ? (await pgDb.get<T>(sql, params, dbName)) : (await sqliteDb.get<T>(sql, params, dbName))
  return res
}


export async function all<T>(
  sql: string,
  params: unknown[] | object = [],
  dbName: DbName = 'default'
): Promise<T[]> {
  const res = isPGEnabled ? (await pgDb.all<T>(sql, params, dbName)) : (await sqliteDb.all<T>(sql, params, dbName))
  return res
}

/**
 * Closes the Database and Indexer Connections Gracefully
 */
export async function close(): Promise<void> {
  if (isPGEnabled) {
    await pgDb.close()
  } else {
    await sqliteDb.close()
  }
}

export function extractValues(object: object): string[] {
  if (isPGEnabled) {
    return pgDb.extractValues(object)
  }
  return sqliteDb.extractValues(object)
}

export function extractValuesFromArray(arr: object[]): string[] {
  if (isPGEnabled) {
    return pgDb.extractValuesFromArray(arr)
  }
  return sqliteDb.extractValuesFromArray(arr)
}
