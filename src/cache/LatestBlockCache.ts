import { config } from '../config'
import { blockQueryDelayInMillis } from '../utils/block'
import * as db from '../storage/sqlite3storage'
import * as pgDb from '../storage/pgStorage'
import { DbBlock } from '../types'
import { sleep } from '../utils'

let latestBlock: DbBlock | null = null
let lastCacheUpdateTimestamp = 0
let getLatestBlockFromDB_running = false

async function getLatestBlockFromDB(): Promise<DbBlock> {
  const before = Date.now()

  const delayInMillis = blockQueryDelayInMillis()
  const sql = `SELECT * FROM (SELECT *${config.postgresEnabled ? ', readableBlock::TEXT' : ''} FROM blocks ORDER BY number DESC LIMIT 100) AS subquery WHERE timestamp <= ${Date.now() - delayInMillis}`

  //Shouldn't the highest block number always be the latest block?
  //This query seems much faster for the same thing
  // A:  nope, some reason this only updates once only 10 cycles or so.. even if they come to us out 
  // of order I would think the sort on primary key would still work... oh... it could be because
  // earlier blocks are sent.   I think maybe this also needs the timestamp check,
  // or we get 10 and then sort through and find one that works 
  
  //const sql = `SELECT * FROM blocks ORDER BY number DESC LIMIT 1`

  const block: DbBlock = config.postgresEnabled
    ? await pgDb.get(sql)
    : await db.get(sql)

  const elapsed = Date.now() - before
  console.log('SLOW QUERY?  getLatestBlockFromDB   ', elapsed, 'ms' , block.number)

  return block
}

async function updateLatestBlockCacheIfNeeded(): Promise<void> {
  const now = Date.now()
  if (latestBlock === null || now - lastCacheUpdateTimestamp >= config.blockCache.cacheUpdateIntervalInMillis) {
    try{
      if(getLatestBlockFromDB_running === true) {
        while (getLatestBlockFromDB_running === true) {
          await sleep(200)
        }
        console.log('SLOW QUERY?  getLatestBlockFromDB skipped', latestBlock.number )
        return //other process updated latestBlock
      }
      getLatestBlockFromDB_running = true
      latestBlock = await getLatestBlockFromDB()
      lastCacheUpdateTimestamp = now
      getLatestBlockFromDB_running = false
    } finally {
      getLatestBlockFromDB_running = false
    }
  }
}
export function registerCache(): void {
  if (config.blockCache.enabled) {
    setInterval(updateLatestBlockCacheIfNeeded, config.blockCache.cacheUpdateIntervalInMillis)
  }
}

export async function getLatestBlock(): Promise<DbBlock> {
  if (config.blockCache.enabled) {
    await updateLatestBlockCacheIfNeeded()
    return latestBlock
  }
  const block: DbBlock = await getLatestBlockFromDB()
  return block
}
