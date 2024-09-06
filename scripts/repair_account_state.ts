import dotenv from 'dotenv'
dotenv.config()

import * as Crypto from '../src/utils/crypto'
import * as Storage from '../src/storage'
import * as AccountDB from '../src/storage/account'
import * as DataSync from '../src/class/DataSync'
import { config, overrideDefaultConfig } from '../src/config'
import { Account, AccountCopy, AccountSearchType, AccountType } from '../src/types'
import { bytesToHex } from '@ethereumjs/util'
import { getContractInfo } from '../src/class/TxDecoder'

const patchAccountData = false

const start = async (): Promise<void> => {
  overrideDefaultConfig(process.env, process.argv)
  // Set crypto hash keys from config
  Crypto.setCryptoHashKey(config.hashKey)
  await Storage.initializeDB()
  Storage.addExitListeners()

  let response = await DataSync.queryFromDistributor(DataSync.DataType.TOTALDATA, {})
  if (!response || !response.data || response.data.totalAccounts < 0 || response.data.totalCycles < 0) {
    console.error('Failed to get data from distributor')
    return
  }
  const totalAccounts = response.data.totalAccounts
  const existingAccountsCount = await AccountDB.queryAccountCount(AccountSearchType.All)
  console.log('Total Accounts', totalAccounts, 'Existing Accounts', existingAccountsCount)
  if (existingAccountsCount > totalAccounts) {
    console.error('Existing accounts count is greater than total accounts')
  }

  const limit = 1000
  for (let i = 0; i < totalAccounts; i += limit) {
    const end = i + limit > totalAccounts ? totalAccounts : i + limit
    console.log('Downloading accounts', i, end)
    response = await DataSync.queryFromDistributor(DataSync.DataType.ACCOUNT, { start: i, end })
    if (!response || !response.data || !response.data.accounts) {
      console.error('Failed to get accounts from distributor', i, end)
      continue
    }
    const accounts = response.data.accounts as AccountCopy[]
    console.log('Accounts', accounts.length)
    for (const account of accounts) {
      const existingAccount = await AccountDB.queryAccountByAccountId(account.accountId)
      if (!existingAccount) {
        console.log('Account not found', account.accountId)
        if (patchAccountData) {
          await saveAccount(account)
        }
        continue
      }
      if (existingAccount.hash !== account.hash || existingAccount.timestamp !== account.timestamp) {
        console.log('Account found but hash or timestamp mismatch', account.accountId)
        if (patchAccountData) {
          await saveAccount(account)
        }
      }
    }
  }

  await Storage.closeDatabase()
  console.log('Finish verifying accounts state data!')
}

const saveAccount = async (account: AccountCopy): Promise<void> => {
  const accountType = account.data.accountType as AccountType
  const accObj = {
    accountId: account.accountId,
    cycle: account.cycleNumber,
    timestamp: account.timestamp,
    account: account.data,
    hash: account.hash,
    accountType,
    isGlobal: account.isGlobal,
  } as Account
  if (
    accountType === AccountType.Account ||
    accountType === AccountType.ContractStorage ||
    accountType === AccountType.ContractCode
  ) {
    accObj.ethAddress = account.data.ethAddress.toLowerCase()
    if (
      config.processData.decodeContractInfo &&
      accountType === AccountType.Account &&
      'account' in accObj.account &&
      bytesToHex(Uint8Array.from(Object.values(accObj.account.account?.codeHash || []))) !== AccountDB.EOA_CodeHash
    ) {
      const accountExist = await AccountDB.queryAccountByAccountId(accObj.accountId)
      if (config.verbose) console.log('accountExist', accountExist)
      if (!accountExist) {
        const { contractInfo, contractType } = await getContractInfo(accObj.ethAddress)
        accObj.contractInfo = contractInfo
        accObj.contractType = contractType
        await AccountDB.insertAccount(accObj)
      } else {
        if (accountExist.timestamp < accObj.timestamp) {
          await AccountDB.updateAccount(accObj.accountId, accObj)
        }
      }
    }
  } else if (
    accountType === AccountType.NetworkAccount ||
    accountType === AccountType.DevAccount ||
    accountType === AccountType.NodeAccount ||
    accountType === AccountType.NodeAccount2
  ) {
    accObj.ethAddress = account.accountId // Adding accountId as ethAddess for these account types for now; since we need ethAddress for mysql index
  }
  const accountExist = await AccountDB.queryAccountByAccountId(accObj.accountId)
  if (config.verbose) console.log('accountExist', accountExist)
  if (!accountExist) {
    await AccountDB.insertAccount(accObj)
  } else {
    if (accountExist.timestamp < accObj.timestamp) {
      await AccountDB.updateAccount(accObj.accountId, accObj)
    }
  }
}

start()
