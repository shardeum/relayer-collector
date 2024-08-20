import { config } from '../config/index'
import { Account, AccountEntry } from '../types'
import * as db from './sqlite3storage'
import * as pgDb from './pgStorage'
import { Utils as StringUtils } from '@shardus/types'

export async function insertAccountEntry(account: Account): Promise<void> {
  try {
    const accountEntry: AccountEntry = {
      accountId: account.accountId,
      timestamp: account.timestamp as number,
      data: account.account,
    }
    const fields = Object.keys(accountEntry).join(', ')
    const values = db.extractValues(accountEntry)
    if (config.postgresEnabled) {
      const placeholders = Object.keys(accountEntry).map((key, ind) => `\$${ind + 1}`).join(', ')
      const replacement = Object.keys(accountEntry).map((key) => `${key} = EXCLUDED.${key}`).join(', ')
      const sql = 'INSERT INTO accountsEntry (' + fields + ') VALUES (' + placeholders + ') ON CONFLICT(accountId) DO UPDATE SET ' + replacement
      await pgDb.run(sql, values, 'shardeumIndexer')
    } else {
      const placeholders = Object.keys(accountEntry).fill('?').join(', ')
      const sql = 'INSERT OR REPLACE INTO accountsEntry (' + fields + ') VALUES (' + placeholders + ')'
      await db.run(sql, values, 'shardeumIndexer')
    }
    if (config.verbose)
      console.log(
        'ShardeumIndexer: Successfully inserted AccountEntry',
        account.ethAddress || account.accountId
      )
  } catch (e) {
    console.log(e)
    console.log(
      'ShardeumIndexer: Unable to insert AccountEntry or it is already stored in to database',
      account.accountId
    )
  }
}

export async function bulkInsertAccountEntries(accounts: Account[]): Promise<void> {
  try {
    const accountEntries: AccountEntry[] = []
    for (const account of accounts) {
      const accountEntry: AccountEntry = {
        accountId: account.accountId,
        timestamp: account.timestamp as number,
        data: account.account,
      }
      accountEntries.push(accountEntry)
    }

    const fields = Object.keys(accountEntries[0]).join(', ')
    const values = db.extractValuesFromArray(accountEntries)

    if (config.postgresEnabled) {
      const placeholders = Object.keys(accountEntries[0]).map((_, i) => `$${i + 1}`).join(', ')

      let sql = `INSERT INTO accountsEntry (${fields}) VALUES `

      sql += accountEntries.map((_, i) => {
        const currentPlaceholders = Object.keys(accountEntries[0])
          .map((_, j) => `$${i * Object.keys(accountEntries[0]).length + j + 1}`)
          .join(', ')
        return `(${currentPlaceholders})`
      }).join(", ")

      sql = `${sql} ON CONFLICT(accountId) DO UPDATE SET ${fields.split(', ').map(field => `${field} = EXCLUDED.${field}`).join(', ')}`
      await pgDb.run(sql, values, 'shardeumIndexer')
    }
    else {
      const placeholders = Object.keys(accountEntries[0]).fill('?').join(', ')

      let sql = 'INSERT OR REPLACE INTO accountsEntry (' + fields + ') VALUES (' + placeholders + ')'
      for (let i = 1; i < accountEntries.length; i++) {
        sql = sql + ', (' + placeholders + ')'
      }
      await db.run(sql, values, 'shardeumIndexer')
    }
    console.log('ShardeumIndexer: Successfully bulk inserted AccountEntries', accountEntries.length)
  } catch (e) {
    console.log(e)
    console.log('ShardeumIndexer: Unable to bulk insert AccountEntries', accounts.length)
  }
}

export async function updateAccountEntry(_accountId: string, account: Partial<Account>): Promise<void> {
  try {
    if (config.postgresEnabled) {
      const sql = `
        UPDATE accountsEntry
        SET timestamp = $1, data = $2
        WHERE accountId = $3
      `
      const values = [
        account.timestamp,
        account.account && StringUtils.safeStringify(account.account),
        account.accountId
      ]

      await pgDb.run(sql, values, 'shardeumIndexer')
    }
    else {
      const sql = `UPDATE accountsEntry SET timestamp = $timestamp, data = $account WHERE accountId = $accountId `
      await db.run(
        sql,
        {
          $timestamp: account.timestamp,
          $account: account.account && StringUtils.safeStringify(account.account),
          $accountId: account.accountId,
        },
        'shardeumIndexer'
      )
    }
    if (config.verbose)
      console.log(
        'ShardeumIndexer: Successfully updated AccountEntry',
        account.ethAddress || account.accountId
      )
  } catch (e) {
    console.log(e)
    console.log('ShardeumIndexer: Unable to update AccountEntry', account)
  }
}

export async function queryAccountEntryCount(): Promise<number> {
  let accountEntries: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    const sql = `SELECT COUNT(*) as "COUNT(*)" FROM accountsEntry`
    accountEntries = config.postgresEnabled
      ? await pgDb.get(sql, [], 'shardeumIndexer')
      : await db.get(sql, [], 'shardeumIndexer')
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('AccountEntry count', accountEntries)
  return accountEntries['COUNT(*)'] || 0
}
