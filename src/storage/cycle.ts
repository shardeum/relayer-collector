import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import { Cycle } from '../types'
import { config } from '../config/index'
import { isBlockIndexingEnabled } from '.'
import { upsertBlocksForCycle, upsertBlocksForCycles } from './block'
import { cleanOldReceiptsMap } from './receipt'
import { cleanOldOriginalTxsMap } from './originalTxData'
import { Utils as StringUtils } from '@shardus/types'

export let Collection: unknown

type DbCycle = Cycle & {
  cycleRecord: string
}

export function isCycle(obj: Cycle): obj is Cycle {
  return (obj as Cycle).cycleRecord !== undefined && (obj as Cycle).cycleMarker !== undefined
}

export async function insertCycle(cycle: Cycle): Promise<void> {
  try {
    const fields = Object.keys(cycle).join(', ')
    const placeholders = Object.keys(cycle).fill('?').join(', ')
    const values = extractValues(cycle)
    const sql = 'INSERT OR REPLACE INTO cycles (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    if (config.verbose)
      console.log('Successfully inserted Cycle', cycle.cycleRecord.counter, cycle.cycleMarker)
    if (isBlockIndexingEnabled()) await upsertBlocksForCycle(cycle)
  } catch (e) {
    console.log(e)
    console.log(
      'Unable to insert cycle or it is already stored in to database',
      cycle.cycleRecord.counter,
      cycle.cycleMarker
    )
  }
}

export async function bulkInsertCycles(cycles: Cycle[]): Promise<void> {
  try {
    const fields = Object.keys(cycles[0]).join(', ')
    const placeholders = Object.keys(cycles[0]).fill('?').join(', ')
    const values = extractValuesFromArray(cycles)
    let sql = 'INSERT OR REPLACE INTO cycles (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < cycles.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    console.log('Successfully bulk inserted Cycles', cycles.length)
    if (isBlockIndexingEnabled()) await upsertBlocksForCycles(cycles)
  } catch (e) {
    console.log(e)
    console.log('Unable to bulk insert Cycles', cycles.length)
  }
}

export async function updateCycle(marker: string, cycle: Cycle): Promise<void> {
  try {
    const sql = `UPDATE cycles SET counter = $counter, cycleRecord = $cycleRecord WHERE cycleMarker = $marker `
    await db.run(sql, {
      $counter: cycle.counter,
      $cycleRecord: cycle.cycleRecord && StringUtils.safeStringify(cycle.cycleRecord),
      $marker: marker,
    })
    if (config.verbose) console.log('Updated cycle for counter', cycle.cycleRecord.counter, cycle.cycleMarker)
    if (isBlockIndexingEnabled()) await upsertBlocksForCycle(cycle)
  } catch (e) {
    console.log(e)
    console.log('Unable to update Cycle', cycle.cycleMarker)
  }
}

export async function insertOrUpdateCycle(cycle: Cycle): Promise<void> {
  if (cycle && cycle.cycleRecord && cycle.cycleMarker) {
    const cycleInfo: Cycle = {
      counter: cycle.cycleRecord.counter,
      cycleRecord: cycle.cycleRecord,
      cycleMarker: cycle.cycleMarker,
    }
    const cycleExist = await queryCycleByMarker(cycle.cycleMarker)
    if (config.verbose) console.log('cycleExist', cycleExist)
    if (cycleExist) {
      if (StringUtils.safeStringify(cycleInfo) !== StringUtils.safeStringify(cycleExist))
        await updateCycle(cycleInfo.cycleMarker, cycleInfo)
    } else {
      await insertCycle(cycleInfo)
      // Clean up receipts map that are older than 5 minutes
      const CLEAN_UP_TIMESTMAP_MS = Date.now() - 5 * 60 * 1000
      cleanOldReceiptsMap(CLEAN_UP_TIMESTMAP_MS)
      cleanOldOriginalTxsMap(CLEAN_UP_TIMESTMAP_MS)
    }
  } else {
    console.log('No cycleRecord or cycleMarker in cycle,', cycle)
  }
}

export async function queryLatestCycleRecords(count: number): Promise<Cycle[]> {
  try {
    const sql = `SELECT * FROM cycles ORDER BY counter DESC LIMIT ${count}`
    const cycleRecords: DbCycle[] = await db.all(sql)
    if (cycleRecords.length > 0) {
      cycleRecords.forEach((cycleRecord: DbCycle) => {
        if (cycleRecord.cycleRecord)
          cycleRecord.cycleRecord = StringUtils.safeJsonParse(cycleRecord.cycleRecord)
      })
    }
    if (config.verbose) console.log('cycle latest', cycleRecords)
    return cycleRecords as unknown as Cycle[]
  } catch (e) {
    console.log(e)
  }

  return []
}

export async function queryCycleRecordsBetween(start: number, end: number): Promise<Cycle[]> {
  try {
    const sql = `SELECT * FROM cycles WHERE counter BETWEEN ? AND ? ORDER BY counter DESC`
    const cycles: DbCycle[] = await db.all(sql, [start, end])
    if (cycles.length > 0) {
      cycles.forEach((cycleRecord: DbCycle) => {
        if (cycleRecord.cycleRecord)
          cycleRecord.cycleRecord = StringUtils.safeJsonParse(cycleRecord.cycleRecord)
      })
    }
    if (config.verbose) console.log('cycle between', cycles)
    return cycles as unknown as Cycle[]
  } catch (e) {
    console.log(e)
  }
  return []
}

export async function queryCycleByMarker(marker: string): Promise<Cycle | null> {
  try {
    const sql = `SELECT * FROM cycles WHERE cycleMarker=? LIMIT 1`
    const cycleRecord: DbCycle = await db.get(sql, [marker])
    if (cycleRecord) {
      if (cycleRecord.cycleRecord)
        cycleRecord.cycleRecord = StringUtils.safeJsonParse(cycleRecord.cycleRecord)
    }
    if (config.verbose) console.log('cycle marker', cycleRecord)
    return cycleRecord as unknown as Cycle
  } catch (e) {
    console.log(e)
  }

  return null
}

export async function queryCycleByCounter(counter: number): Promise<Cycle | null> {
  try {
    const sql = `SELECT * FROM cycles WHERE counter=? LIMIT 1`
    const cycleRecord: DbCycle = await db.get(sql, [counter])
    if (cycleRecord) {
      if (cycleRecord.cycleRecord)
        cycleRecord.cycleRecord = StringUtils.safeJsonParse(cycleRecord.cycleRecord)
    }
    if (config.verbose) console.log('cycle counter', cycleRecord)
    return cycleRecord as unknown as Cycle
  } catch (e) {
    console.log(e)
  }

  return null
}

export async function queryCycleCount(): Promise<number> {
  let cycles: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    const sql = `SELECT COUNT(*) FROM cycles`
    cycles = await db.get(sql, [])
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Cycle count', cycles)

  return cycles['COUNT(*)'] || 0
}
