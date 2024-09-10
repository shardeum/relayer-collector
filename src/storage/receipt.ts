import { config } from '../config'
import * as AccountDB from './account'
import * as TransactionDB from './transaction'
import * as AccountHistoryStateDB from './accountHistoryState'
import { Utils as StringUtils } from '@shardus/types'
import {
  AccountType,
  TokenTx,
  Transaction,
  TransactionType,
  WrappedAccount,
  WrappedEVMAccount,
  Receipt,
  ContractInfo,
  Account,
  Token,
} from '../types'
import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import { decodeTx, getContractInfo, ZERO_ETH_ADDRESS } from '../class/TxDecoder'
import { bytesToHex } from '@ethereumjs/util'
import { forwardReceiptData } from '../log_subscription/CollectorSocketconnection'

type DbReceipt = Receipt & {
  tx: string
  beforeStates: string
  afterStates: string
  appReceiptData: string | null
  signedReceipt: string
}

export const receiptsMap: Map<string, number> = new Map()

export async function insertReceipt(receipt: Receipt): Promise<void> {
  try {
    const fields = Object.keys(receipt).join(', ')
    const placeholders = Object.keys(receipt).fill('?').join(', ')
    const values = extractValues(receipt)
    const sql = 'INSERT OR REPLACE INTO receipts (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    if (config.verbose) console.log('Successfully inserted Receipt', receipt.receiptId)
  } catch (e) {
    console.log(e)
    console.log('Unable to insert Receipt or it is already stored in to database', receipt.receiptId)
  }
}

export async function bulkInsertReceipts(receipts: Receipt[]): Promise<void> {
  try {
    const fields = Object.keys(receipts[0]).join(', ')
    const placeholders = Object.keys(receipts[0]).fill('?').join(', ')
    const values = extractValuesFromArray(receipts)
    let sql = 'INSERT OR REPLACE INTO receipts (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < receipts.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    console.log('Successfully bulk inserted receipts', receipts.length)
  } catch (e) {
    console.log(e)
    console.log('Unable to bulk insert receipts', receipts.length)
  }
}

export async function processReceiptData(receipts: Receipt[], saveOnlyNewData = false): Promise<void> {
  if (receipts && receipts.length <= 0) return
  const bucketSize = 1000
  let combineReceipts: Receipt[] = []
  let combineAccounts1: Account[] = []
  let combineTransactions: Transaction[] = []
  let combineTokenTransactions: TokenTx[] = [] // For TransactionType (Internal ,ERC20, ERC721)
  let combineTokenTransactions2: TokenTx[] = [] // For TransactionType (ERC1155)
  let combineTokens: Token[] = [] // For Tokens owned by an address
  let accountHistoryStateList: AccountHistoryStateDB.AccountHistoryState[] = []
  for (const receiptObj of receipts) {
    const { afterStates, cycle, appReceiptData, tx, timestamp, signedReceipt } = receiptObj
    if (receiptsMap.has(tx.txId) && receiptsMap.get(tx.txId) === timestamp) {
      continue
    }
    let modifiedReceiptObj = {
      ...receiptObj,
      beforeStates: config.storeReceiptBeforeStates ? receiptObj.beforeStates : [],
    }
    if (saveOnlyNewData) {
      const receiptExist = await queryReceiptByReceiptId(tx.txId)
      if (!receiptExist) combineReceipts.push(modifiedReceiptObj as unknown as Receipt)
    } else combineReceipts.push(modifiedReceiptObj as unknown as Receipt)
    let txReceipt: WrappedAccount = appReceiptData
    receiptsMap.set(tx.txId, tx.timestamp)

    // Forward receipt data to LogServer
    if (config.enableCollectorSocketServer) await forwardReceiptData([receiptObj])
    // Receipts size can be big, better to save per 100
    if (combineReceipts.length >= 100) {
      await bulkInsertReceipts(combineReceipts)
      combineReceipts = []
    }
    if (!config.processData.indexReceipt) continue
    const storageKeyValueMap = {}
    for (const account of afterStates) {
      const accountType = account.data.accountType as AccountType
      const accObj = {
        accountId: account.accountId,
        cycle: cycle,
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
          bytesToHex(Uint8Array.from(Object.values(accObj.account.account.codeHash))) !==
            AccountDB.EOA_CodeHash
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
          continue
        }
        if (
          config.processData.decodeTokenTransfer &&
          accountType === AccountType.ContractStorage &&
          'key' in accObj.account
        ) {
          storageKeyValueMap[accObj.account.key + accObj.ethAddress] = accObj.account
        }
      } else if (
        accountType === AccountType.NetworkAccount ||
        accountType === AccountType.DevAccount ||
        accountType === AccountType.NodeAccount ||
        accountType === AccountType.NodeAccount2
      ) {
        accObj.ethAddress = account.accountId // Adding accountId as ethAddess for these account types for now; since we need ethAddress for mysql index
      }
      const index = combineAccounts1.findIndex((a) => {
        return a.accountId === accObj.accountId
      })
      if (index > -1) {
        // eslint-disable-next-line security/detect-object-injection
        const accountExist = combineAccounts1[index]
        if (accountExist.timestamp < accObj.timestamp) {
          combineAccounts1.splice(index, 1)
          combineAccounts1.push(accObj)
        }
      } else {
        const accountExist = await AccountDB.queryAccountByAccountId(accObj.accountId)
        if (config.verbose) console.log('accountExist', accountExist)
        if (!accountExist) {
          combineAccounts1.push(accObj)
        } else {
          if (accountExist.timestamp < accObj.timestamp) {
            await AccountDB.updateAccount(accObj.accountId, accObj)
          }
        }
      }
      if (
        accountType === AccountType.Receipt ||
        accountType === AccountType.NodeRewardReceipt ||
        accountType === AccountType.StakeReceipt ||
        accountType === AccountType.UnstakeReceipt ||
        accountType === AccountType.InternalTxReceipt
      )
        txReceipt = account
    }
    let blockNumber
    let blockHash
    if (txReceipt) {
      // if (txReceipt.data.accountType !== AccountType.InternalTxReceipt) {
      const transactionType: TransactionType =
        txReceipt.data.accountType === AccountType.Receipt
          ? TransactionType.Receipt
          : txReceipt.data.accountType === AccountType.NodeRewardReceipt
          ? TransactionType.NodeRewardReceipt
          : txReceipt.data.accountType === AccountType.StakeReceipt
          ? TransactionType.StakeReceipt
          : txReceipt.data.accountType === AccountType.UnstakeReceipt
          ? TransactionType.UnstakeReceipt
          : txReceipt.data.accountType === AccountType.InternalTxReceipt
          ? TransactionType.InternalTxReceipt
          : (-1 as TransactionType)
      blockHash = txReceipt.data?.readableReceipt?.blockHash
      if (!blockHash) console.error(`Transaction ${tx.txId} has no blockHash`)
      blockNumber = parseInt(txReceipt.data?.readableReceipt?.blockNumber)
      if (transactionType !== (-1 as TransactionType)) {
        const txObj: Transaction = {
          txId: tx.txId,
          cycle: cycle,
          blockNumber,
          blockHash,
          timestamp: tx.timestamp,
          wrappedEVMAccount: txReceipt.data,
          transactionType,
          txHash: txReceipt.data.ethAddress,
          txFrom: txReceipt.data.readableReceipt.from,
          txTo: txReceipt.data.readableReceipt.to
            ? txReceipt.data.readableReceipt.to
            : txReceipt.data.readableReceipt.contractAddress,
          originalTxData: tx.originalTxData || {},
        }
        if (txReceipt.data.readableReceipt.stakeInfo) {
          txObj.nominee = txReceipt.data.readableReceipt.stakeInfo.nominee
        }
        let newTx = true
        const transactionExist = await TransactionDB.queryTransactionByTxId(tx.txId)
        if (config.verbose) console.log('transactionExist', transactionExist)
        if (!transactionExist) {
          if (txObj.nominee) await TransactionDB.insertTransaction(txObj)
          else combineTransactions.push(txObj)
        } else {
          if (transactionExist.timestamp < txObj.timestamp) {
            await TransactionDB.insertTransaction(txObj)
          }
          newTx = false
        }
        const { txs, accs, tokens } = await decodeTx(txObj, storageKeyValueMap, newTx)
        for (const acc of accs) {
          if (acc === ZERO_ETH_ADDRESS) continue
          if (!combineAccounts1.some((a) => a.ethAddress === acc)) {
            const addressToCreate = acc
            const accountExist = await AccountDB.queryAccountByAccountId(
              addressToCreate.slice(2).toLowerCase() + '0'.repeat(24) //Search by Shardus address
            )
            if (config.verbose) console.log('addressToCreate', addressToCreate, accountExist)
            if (!accountExist) {
              // Although this account is not created yet in Shardeum, we created it as a dummy account, so that we can show the account info on the explorer
              const accObj = {
                accountId: addressToCreate.slice(2).toLowerCase() + '0'.repeat(24),
                cycle: txObj.cycle,
                timestamp: txObj.timestamp,
                ethAddress: addressToCreate,
                account: {
                  nonce: '0',
                  balance: '0',
                } as WrappedEVMAccount,
                hash: 'Ox',
                accountType: AccountType.Account,
                isGlobal: false,
              }
              combineAccounts1.push(accObj)
            }
          }
        }
        for (const tx of txs) {
          let accountExist: Account | null = null
          if (tx.tokenType !== TransactionType.EVM_Internal)
            accountExist = await AccountDB.queryAccountByAccountId(
              tx.contractAddress.slice(2).toLowerCase() + '0'.repeat(24) //Search by Shardus address
            )
          let contractInfo = {} as ContractInfo
          if (accountExist && accountExist.contractInfo) {
            contractInfo = accountExist.contractInfo
          }
          if ('amountSpent' in txObj.wrappedEVMAccount) {
            const obj: TokenTx = {
              ...tx,
              txId: txObj.txId,
              txHash: txObj.txHash,
              cycle: txObj.cycle,
              timestamp: txObj.timestamp,
              transactionFee: txObj.wrappedEVMAccount.amountSpent, // Maybe provide with actual token transfer cost
              contractInfo,
            }
            if (tx.tokenType === TransactionType.ERC_1155) {
              combineTokenTransactions2.push(obj)
            } else {
              combineTokenTransactions.push(obj)
            }
          }
        }
        combineTokens = [...combineTokens, ...tokens]
      }
    }
    if (config.saveAccountHistoryState) {
      if (
        receiptObj.globalModification === false &&
        signedReceipt &&
        blockNumber &&
        blockHash &&
        signedReceipt.proposal.accountIDs.length > 0
      ) {
        for (let i = 0; i < signedReceipt.proposal.accountIDs.length; i++) {
          const accountHistoryState = {
            accountId: signedReceipt.proposal.accountIDs[i],
            beforeStateHash: signedReceipt.proposal.beforeStateHashes[i],
            afterStateHash: signedReceipt.proposal.afterStateHashes[i],
            timestamp,
            blockNumber,
            blockHash,
            receiptId: tx.txId,
          }
          accountHistoryStateList.push(accountHistoryState)
        }
      } else {
        if (receiptObj.globalModification === true) {
          console.log(`Receipt ${tx.txId} with timestamp ${timestamp} has globalModification as true`)
        }
        if (receiptObj.globalModification === false && !signedReceipt) {
          console.error(`Receipt ${tx.txId} with timestamp ${timestamp} has no signedReceipt`)
        }
        if (!blockNumber || !blockHash) {
          console.error(`Receipt ${tx.txId} with timestamp ${timestamp} has no blockNumber or blockHash`)
        }
      }
    }
    if (combineAccounts1.length >= bucketSize) {
      await AccountDB.bulkInsertAccounts(combineAccounts1)
      combineAccounts1 = []
    }
    if (combineTransactions.length >= bucketSize) {
      await TransactionDB.bulkInsertTransactions(combineTransactions)
      combineTransactions = []
    }
    if (combineTokenTransactions.length >= bucketSize) {
      await TransactionDB.bulkInsertTokenTransactions(combineTokenTransactions)
      combineTokenTransactions = []
    }
    if (combineTokenTransactions2.length >= bucketSize) {
      await TransactionDB.bulkInsertTokenTransactions(combineTokenTransactions2)
      combineTokenTransactions2 = []
    }
    if (combineTokens.length >= bucketSize) {
      await AccountDB.bulkInsertTokens(combineTokens)
      combineTokens = []
    }
    if (accountHistoryStateList.length > bucketSize) {
      await AccountHistoryStateDB.bulkInsertAccountHistoryStates(accountHistoryStateList)
      accountHistoryStateList = []
    }
  }
  if (combineReceipts.length > 0) await bulkInsertReceipts(combineReceipts)
  if (combineAccounts1.length > 0) await AccountDB.bulkInsertAccounts(combineAccounts1)
  if (combineTransactions.length > 0) await TransactionDB.bulkInsertTransactions(combineTransactions)
  if (combineTokenTransactions.length > 0)
    await TransactionDB.bulkInsertTokenTransactions(combineTokenTransactions)
  if (combineTokenTransactions2.length > 0)
    await TransactionDB.bulkInsertTokenTransactions(combineTokenTransactions2)
  if (combineTokens.length > 0) await AccountDB.bulkInsertTokens(combineTokens)
  if (accountHistoryStateList.length > 0)
    await AccountHistoryStateDB.bulkInsertAccountHistoryStates(accountHistoryStateList)
}

export async function queryReceiptByReceiptId(receiptId: string): Promise<Receipt | null> {
  try {
    const sql = `SELECT * FROM receipts WHERE receiptId=?`
    const receipt: DbReceipt = await db.get(sql, [receiptId])
    if (receipt) deserializeDbReceipt(receipt)
    if (config.verbose) console.log('Receipt receiptId', receipt)
    return receipt as Receipt
  } catch (e) {
    console.log(e)
  }

  return null
}

export async function queryLatestReceipts(count: number): Promise<Receipt[]> {
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle DESC, timestamp DESC LIMIT ${count}`
    const receipts: DbReceipt[] = await db.all(sql)

    receipts.forEach((receipt: DbReceipt) => deserializeDbReceipt(receipt))

    if (config.verbose) console.log('Receipt latest', receipts)
    return receipts
  } catch (e) {
    console.log(e)
  }

  return []
}

export async function queryReceipts(skip = 0, limit = 10000): Promise<Receipt[]> {
  let receipts: DbReceipt[] = []
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    receipts = await db.all(sql)
    receipts.forEach((receipt: DbReceipt) => deserializeDbReceipt(receipt))
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Receipt receipts', receipts ? receipts.length : receipts, 'skip', skip)

  return receipts
}

export async function queryReceiptCount(): Promise<number> {
  let receipts: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    const sql = `SELECT COUNT(*) FROM receipts`
    receipts = await db.get(sql, [])
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Receipt count', receipts)

  return receipts['COUNT(*)'] || 0
}

export async function queryReceiptCountByCycles(
  start: number,
  end: number
): Promise<{ receipts: number; cycle: number }[]> {
  let receipts: { cycle: number; 'COUNT(*)': number }[] = []
  try {
    const sql = `SELECT cycle, COUNT(*) FROM receipts GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
    receipts = await db.all(sql, [start, end])
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Receipt count by cycles', receipts)

  return receipts.map((receipt) => {
    return {
      receipts: receipt['COUNT(*)'],
      cycle: receipt.cycle,
    }
  })
}

export async function queryReceiptsBetweenCycles(
  skip = 0,
  limit = 10,
  start: number,
  end: number
): Promise<Receipt[]> {
  let receipts: DbReceipt[] = []
  try {
    const sql = `SELECT * FROM receipts WHERE cycle BETWEEN ? and ? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    receipts = await db.all(sql, [start, end])
    receipts.forEach((receipt: DbReceipt) => deserializeDbReceipt(receipt))
  } catch (e) {
    console.log(e)
  }

  if (config.verbose) console.log('Receipt receipts between cycles', receipts)
  return receipts
}

export async function queryReceiptCountBetweenCycles(start: number, end: number): Promise<number> {
  let receipts: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    const sql = `SELECT COUNT(*) FROM receipts WHERE cycle BETWEEN ? and ?`
    receipts = await db.get(sql, [start, end])
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Receipt receipts count between cycles', receipts)

  return receipts['COUNT(*)'] || 0
}

function deserializeDbReceipt(receipt: DbReceipt): void {
  receipt.tx &&= StringUtils.safeJsonParse(receipt.tx)
  receipt.beforeStates &&= StringUtils.safeJsonParse(receipt.beforeStates)
  receipt.afterStates &&= StringUtils.safeJsonParse(receipt.afterStates)
  receipt.appReceiptData &&= StringUtils.safeJsonParse(receipt.appReceiptData)
  receipt.signedReceipt &&= StringUtils.safeJsonParse(receipt.signedReceipt)

  // globalModification is stored as 0 or 1 in the database, convert it to boolean
  receipt.globalModification = (receipt.globalModification as unknown as number) === 1
}

export function cleanOldReceiptsMap(timestamp: number): void {
  for (const [key, value] of receiptsMap) {
    if (value < timestamp) receiptsMap.delete(key)
  }
  if (config.verbose) console.log('Clean Old Receipts Map', timestamp, receiptsMap)
}
