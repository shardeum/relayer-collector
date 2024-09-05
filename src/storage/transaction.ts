import * as db from './sqlite3storage'
import * as pgDb from './pgStorage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import * as analyticsUtil from '../utils/analytics'
import { config } from '../config/index'
import { Utils as StringUtils } from '@shardus/types'
import {
  AccountType,
  Transaction,
  TokenTx,
  Account,
  Token,
  TransactionType,
  TransactionSearchType,
  WrappedEVMAccount,
  WrappedDataReceipt,
  InternalTXType,
  ContractInfo,
} from '../types'
import Web3 from 'web3'
import * as AccountDB from './account'
import { decodeTx, ZERO_ETH_ADDRESS } from '../class/TxDecoder'

export { type Transaction } from '../types'

export const ERC20_METHOD_DIC = {
  '0xa9059cbb': 'transfer',
  '0xa978501e': 'transferFrom',
}

type DbTransaction = Transaction & {
  wrappedEVMAccount: string
  originalTxData: string
}

type DbTokenTx = TokenTx & {
  contractInfo: string
}

export async function insertTransaction(transaction: Transaction): Promise<void> {
  try {
    const fields = Object.keys(transaction).join(', ')
    const values = extractValues(transaction)

    if (config.postgresEnabled) {
      const tTransaction = analyticsUtil.transformTransaction(transaction)

      const fields = Object.keys(tTransaction).map((field) => `"${field}"`).join(', ')
      const values = extractValues(tTransaction)

      let sql = `INSERT INTO transactions (${fields}) VALUES `
      sql += `(${Object.keys(tTransaction).map((_, i) => `$${i + 1}`).join(', ')})`
      sql += ` ON CONFLICT(txId, txHash) DO UPDATE SET ${fields.split(', ').map(field => `${field} = EXCLUDED.${field}`).join(', ')}`
      await pgDb.run(sql, values, 'default')
    } else {
      const placeholders = Object.keys(transaction).fill('?').join(', ')
      const sql = 'INSERT OR REPLACE INTO transactions (' + fields + ') VALUES (' + placeholders + ')'
      await db.run(sql, values, 'default')
    }
    if (config.verbose) console.log('Successfully inserted Transaction', transaction.txId, transaction.txHash)
  } catch (e) {
    console.log(e)
    console.log('Unable to insert Transaction or it is already stored in to database', transaction.txId)
  }
}

export async function bulkInsertTransactions(transactions: Transaction[]): Promise<void> {
  try {
    const fields = Object.keys(transactions[0]).join(', ')
    const values = extractValuesFromArray(transactions)

    if (config.postgresEnabled) {
      const tTransactions = transactions.map((transaction) => analyticsUtil.transformTransaction(transaction))

      const fields = Object.keys(tTransactions[0]).map((field) => `"${field}"`).join(', ')

      const values = extractValuesFromArray(tTransactions)

      let sql = `INSERT INTO transactions (${fields}) VALUES `
      sql += tTransactions.map((_, i) => {
        const currentPlaceholders = Object.keys(tTransactions[0])
          .map((_, j) => `$${i * Object.keys(tTransactions[0]).length + j + 1}`)
          .join(', ')
        return `(${currentPlaceholders})`
      }).join(", ")

      sql += ` ON CONFLICT(txId, txHash) DO UPDATE SET ${fields.split(', ').map(field => `${field} = EXCLUDED.${field}`).join(', ')}`
      await pgDb.run(sql, values, 'default')
    } else {
      const placeholders = Object.keys(transactions[0]).fill('?').join(', ')
      let sql = 'INSERT OR REPLACE INTO transactions (' + fields + ') VALUES (' + placeholders + ')'
      for (let i = 1; i < transactions.length; i++) {
        sql += ', (' + placeholders + ')'
      }
      await db.run(sql, values, 'default')
    }
    console.log('Successfully bulk inserted transactions', transactions.length)
  } catch (e) {
    console.log(e)
    console.log('Unable to bulk insert transactions', transactions.length)
  }
}

export async function updateTransaction(_txId: string, transaction: Partial<Transaction>): Promise<void> {
  try {
    if (config.postgresEnabled) {
      const sql = `UPDATE transactions SET cycle = $1, wrappedEVMAccount = $2, txHash = $3 WHERE txId = $4`
      const values = [
        transaction.cycle,
        transaction.wrappedEVMAccount && StringUtils.safeStringify(transaction.wrappedEVMAccount),
        transaction.txHash,
        transaction.txId
      ]
      await pgDb.run(sql, values, 'default')
    } else {
      const sql = `UPDATE transactions SET cycle = $cycle, wrappedEVMAccount = $wrappedEVMAccount, txHash = $txHash WHERE txId = $txId`
      await db.run(sql, {
        $cycle: transaction.cycle,
        $wrappedEVMAccount: transaction.wrappedEVMAccount && StringUtils.safeStringify(transaction.wrappedEVMAccount),
        $txHash: transaction.txHash,
        $txId: transaction.txId,
      })
    }
    if (config.verbose) console.log('Successfully Updated Transaction', transaction.txId, transaction.txHash)
  } catch (e) {
    /* prettier-ignore */ if (config.verbose) console.log(e);
    console.log('Unable to update Transaction', transaction.txId, transaction.txHash)
  }
}

export async function insertTokenTransaction(tokenTx: TokenTx): Promise<void> {
  try {
    const fields = Object.keys(tokenTx).join(', ')
    const values = extractValues(tokenTx)

    if (config.postgresEnabled) {
      let sql = `INSERT INTO tokenTxs (${fields}) VALUES `
      sql += `(${Object.keys(tokenTx).map((_, i) => `$${i + 1}`).join(', ')})`
      sql += ` ON CONFLICT(txId, txHash) DO UPDATE SET ${fields.split(', ').map(field => `${field} = EXCLUDED.${field}`).join(', ')}`
      await pgDb.run(sql, values, 'default')
    } else {
      const placeholders = Object.keys(tokenTx).fill('?').join(', ')
      const sql = 'INSERT OR REPLACE INTO tokenTxs (' + fields + ') VALUES (' + placeholders + ')'
      await db.run(sql, values)
    }
    if (config.verbose) console.log('Successfully inserted Token Transaction', tokenTx.txHash)
  } catch (e) {
    console.log(e)
    console.log('Unable to insert Token Transaction or it is already stored in to database', tokenTx.txHash)
  }
}

export async function bulkInsertTokenTransactions(tokenTxs: TokenTx[]): Promise<void> {
  try {
    const fields = Object.keys(tokenTxs[0]).join(', ')
    const values = extractValuesFromArray(tokenTxs)

    if (config.postgresEnabled) {
      let sql = `INSERT INTO tokenTxs (${fields}) VALUES `
      sql += tokenTxs.map((_, i) => {
        const currentPlaceholders = Object.keys(tokenTxs[0])
          .map((_, j) => `$${i * Object.keys(tokenTxs[0]).length + j + 1}`)
          .join(', ')
        return `(${currentPlaceholders})`
      }).join(", ")

      sql += ` ON CONFLICT(txId, txHash) DO UPDATE SET ${fields.split(', ').map(field => `${field} = EXCLUDED.${field}`).join(', ')}`
      await pgDb.run(sql, values, 'default')
    } else {
      const placeholders = Object.keys(tokenTxs[0]).fill('?').join(', ')
      let sql = 'INSERT OR REPLACE INTO tokenTxs (' + fields + ') VALUES (' + placeholders + ')'
      for (let i = 1; i < tokenTxs.length; i++) {
        sql += ', (' + placeholders + ')'
      }
      await db.run(sql, values)
    }
    console.log('Successfully inserted token transactions', tokenTxs.length)
  } catch (e) {
    console.log(e)
    console.log('Unable to bulk insert token transactions', tokenTxs.length)
  }
}

interface RawTransaction {
  accountId: string
  cycleNumber: number
  data: WrappedEVMAccount
  originalTxData: {
    duration: number
    internalTXType: InternalTXType
    isInternalTx: boolean
    nominator: string
    nominee: string
    sign: {
      owner: string
      sig: string
    }
    timestamp: number
  }
  result: { txIdShort: string; txResult: string }
  sign: {
    owner: string
    sig: string
  }
  timestamp: number
  txId: string
}

function isReceiptData(obj?: WrappedEVMAccount | null): obj is WrappedEVMAccount & WrappedDataReceipt {
  const accountType = obj?.accountType
  return (
    accountType === AccountType.Receipt ||
    accountType === AccountType.NodeRewardReceipt ||
    accountType === AccountType.StakeReceipt ||
    accountType === AccountType.UnstakeReceipt ||
    accountType === AccountType.InternalTxReceipt
  )
}

export async function processTransactionData(transactions: RawTransaction[]): Promise<void> {
  console.log('transactions size', transactions.length)
  if (transactions && transactions.length <= 0) return
  const bucketSize = 1000
  const combineAccounts: Account[] = []
  const existingAccounts: string[] = [] // To save perf on querying from the db again and again, save the existing account that is queried once in memory
  let combineTransactions: Transaction[] = []
  let combineTokenTransactions: TokenTx[] = [] // For TransactionType (Internal ,ERC20, ERC721)
  let combineTokenTransactions2: TokenTx[] = [] // For TransactionType (ERC1155)
  let combineTokens: Token[] = [] // For Tokens owned by an address
  for (const transaction of transactions) {
    if (isReceiptData(transaction.data)) {
      const txObj: Transaction = {
        txId: transaction.data?.txId,
        cycle: transaction.cycleNumber,
        blockNumber: parseInt(transaction.data.readableReceipt.blockNumber),
        blockHash: transaction.data.readableReceipt.blockHash,
        timestamp: transaction.timestamp,
        wrappedEVMAccount: transaction.data,
        transactionType:
          transaction.data.accountType === AccountType.Receipt
            ? TransactionType.Receipt
            : transaction.data.accountType === AccountType.NodeRewardReceipt
              ? TransactionType.NodeRewardReceipt
              : transaction.data.accountType === AccountType.StakeReceipt
                ? TransactionType.StakeReceipt
                : transaction.data.accountType === AccountType.UnstakeReceipt
                  ? TransactionType.UnstakeReceipt
                  : TransactionType.InternalTxReceipt,
        txHash: transaction.data.ethAddress,
        txFrom: transaction.data.readableReceipt.from,
        txTo: transaction.data.readableReceipt.to
          ? transaction.data.readableReceipt.to
          : transaction.data.readableReceipt.contractAddress,
        originalTxData: {},
      }

      const { txs, accs, tokens } = await decodeTx(txObj)
      for (const acc of accs) {
        if (acc === ZERO_ETH_ADDRESS) continue
        const index = combineAccounts.findIndex(
          (a) => a.accountId === acc.slice(2).toLowerCase() + '0'.repeat(24)
        )
        if (index > -1) {
          // eslint-disable-next-line security/detect-object-injection
          const accountExist = combineAccounts[index]
          accountExist.timestamp = txObj.timestamp
          combineAccounts.splice(index, 1)
          combineAccounts.push(accountExist)
        } else {
          const addressToCreate = acc
          // To save performance on querying from the db again and again, save it in memory
          if (existingAccounts.includes(addressToCreate)) {
            continue
          }
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
            combineAccounts.push(accObj)
          } else {
            existingAccounts.push(addressToCreate)
          }
        }
      }
      for (const tx of txs) {
        const accountExist = await AccountDB.queryAccountByAccountId(
          tx.contractAddress.slice(2).toLowerCase() + '0'.repeat(24) //Search by Shardus address
        )
        let contractInfo = {} as ContractInfo
        if (accountExist && accountExist.contractInfo) {
          contractInfo = accountExist.contractInfo
        }
        // wrapped data must be a receipt here. this type guard ensures that
        if ('readableReceipt' in txObj.wrappedEVMAccount) {
          const obj: TokenTx = {
            ...tx,
            txId: txObj.txId,
            txHash: txObj.txHash,
            cycle: txObj.cycle,
            timestamp: txObj.timestamp,
            transactionFee: txObj.wrappedEVMAccount.readableReceipt.gasUsed ?? '0', // Maybe provide with actual token transfer cost
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
      combineTransactions.push(txObj)
    }
    if (combineTransactions.length >= bucketSize) {
      await bulkInsertTransactions(combineTransactions)
      combineTransactions = []
    }
    if (combineTokenTransactions.length >= bucketSize) {
      await bulkInsertTokenTransactions(combineTokenTransactions)
      combineTokenTransactions = []
    }
    if (combineTokenTransactions2.length >= bucketSize) {
      await bulkInsertTokenTransactions(combineTokenTransactions2)
      combineTokenTransactions2 = []
    }
    if (combineTokens.length >= bucketSize) {
      await AccountDB.bulkInsertTokens(combineTokens)
      combineTokens = []
    }
  }
  if (combineAccounts.length > 0) {
    let limit = bucketSize
    let j = limit
    let accountsToSave: Account[]
    for (let i = 0; i < combineAccounts.length; i = j) {
      accountsToSave = combineAccounts.slice(i, limit)
      await AccountDB.bulkInsertAccounts(accountsToSave)
      j = limit
      limit += bucketSize
    }
  }
  if (combineTransactions.length > 0) await bulkInsertTransactions(combineTransactions)
  if (combineTokenTransactions.length > 0) await bulkInsertTokenTransactions(combineTokenTransactions)
  if (combineTokenTransactions2.length > 0) await bulkInsertTokenTransactions(combineTokenTransactions2)
  if (combineTokens.length > 0) await AccountDB.bulkInsertTokens(combineTokens)
}

export const getWeb3 = function (): Promise<Web3> {
  return new Promise((resolve, reject) => {
    try {
      const web3 = new Web3(new Web3.providers.HttpProvider(`${config.rpcUrl}`))
      resolve(web3)
    } catch (e) {
      console.error(e)
      reject('Cannot get web3 instance')
    }
  })
}

export async function queryTransactionCount(
  address?: string,
  txType?: TransactionSearchType,
  filterAddress?: string
): Promise<number> {
  let transactions: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    if (address) {
      if (!txType) {
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE txFrom=$1 OR txTo=$2 OR nominee=$3`
          : `SELECT COUNT(*) FROM transactions WHERE txFrom=? OR txTo=? OR nominee=?`
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, [address, address, address])
          : await db.get(sql, [address, address, address])
      } else if (txType === TransactionSearchType.AllExceptInternalTx) {
        const ty = TransactionType.InternalTxReceipt
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE transactionType!=$1 AND (txFrom=$2 OR txTo=$3 OR nominee=$4)`
          : `SELECT COUNT(*) FROM transactions WHERE transactionType!=? AND (txFrom=? OR txTo=? OR nominee=?)`
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, [ty, address, address, address])
          : await db.get(sql, [ty, address, address, address])
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
        let sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE transactionType=$1 AND (txFrom=$2 OR txTo=$3 OR nominee=$4)`
          : `SELECT COUNT(*) FROM transactions WHERE transactionType=? AND (txFrom=? OR txTo=? OR nominee=?)`

        if (txType === TransactionSearchType.InternalTxReceipt) {
          sql = config.postgresEnabled
            ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE (transactionType!=$1 AND transactionType!=$2 AND transactionType!=$3) AND (txFrom=$4 OR txTo=$5 OR nominee=$6)`
            : `SELECT COUNT(*) FROM transactions WHERE (transactionType!=? AND transactionType!=? AND transactionType!=?) AND (txFrom=? OR txTo=? OR nominee=?)`
          const values = [TransactionType.Receipt, TransactionType.StakeReceipt,
          TransactionType.UnstakeReceipt, address, address, address]
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, values)
            : await db.get(sql, values)
        } else {
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, [ty, address, address, address])
            : await db.get(sql, [ty, address, address, address])
        }
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE (tokenFrom=$1 OR tokenTo=$2 OR tokenOperator=$3) AND tokenType=$4`
          : `SELECT COUNT(*) FROM tokenTxs WHERE (tokenFrom=? OR tokenTo=? OR tokenOperator=?) AND tokenType=?`

        transactions = config.postgresEnabled
          ? await pgDb.get(sql, [address, address, address, ty])
          : await db.get(sql, [address, address, address, ty])
      } else if (txType === TransactionSearchType.TokenTransfer) {
        if (filterAddress) {
          const sql = config.postgresEnabled
            ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE contractAddress=$1 AND (tokenFrom=$2 OR tokenTo=$3 OR tokenOperator=$4) AND NOT tokenType=$5`
            : `SELECT COUNT(*) FROM tokenTxs WHERE contractAddress=? AND (tokenFrom=? OR tokenTo=? OR tokenOperator=?) AND NOT tokenType=?`
          const values = [
            address,
            filterAddress,
            filterAddress,
            filterAddress,
            TransactionType.EVM_Internal,
          ]
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, values)
            : await db.get(sql, values)
        } else {
          const sql = config.postgresEnabled
            ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE contractAddress=$1 AND NOT tokenType=$2`
            : `SELECT COUNT(*) FROM tokenTxs WHERE contractAddress=? AND NOT tokenType=?`
          const values = [address, TransactionType.EVM_Internal]
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, values)
            : await db.get(sql, values)
        }
      }
    } else if (txType || txType === TransactionSearchType.All) {
      if (txType === TransactionSearchType.All) {
        const sql = `SELECT COUNT(*) as "COUNT(*)" FROM transactions`
        transactions = config.postgresEnabled
          ? await pgDb.get(sql)
          : await db.get(sql)
      } else if (txType === TransactionSearchType.AllExceptInternalTx) {
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE (transactionType=$1 OR transactionType=$2 OR transactionType=$3)`
          : `SELECT COUNT(*) FROM transactions WHERE (transactionType=? OR transactionType=? OR transactionType=?)`
        const values = [
          TransactionType.Receipt,
          TransactionType.StakeReceipt,
          TransactionType.UnstakeReceipt,
        ]
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, values)
          : await db.get(sql, values)
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
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE transactionType=$1`
          : `SELECT COUNT(*) FROM transactions WHERE transactionType=?`
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, [ty])
          : await db.get(sql, [ty])
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE tokenType=$1`
          : `SELECT COUNT(*) FROM tokenTxs WHERE tokenType=?`
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, [ty])
          : await db.get(sql, [ty])
      }
    } else {
      const sql = `SELECT COUNT(*) as "COUNT(*)" FROM transactions`
      transactions = config.postgresEnabled
        ? await pgDb.get(sql)
        : await db.get(sql)
    }
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('transactions count', transactions)

  return transactions['COUNT(*)'] || 0
}

export async function queryTransactions(
  skip = 0,
  limit = 10,
  address?: string,
  txType?: TransactionSearchType,
  filterAddress?: string
): Promise<(DbTransaction | DbTokenTx)[]> {
  let transactions: (DbTransaction | DbTokenTx)[] = []
  try {
    if (address) {
      if (!txType) {
        const sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE (txFrom=$1 OR txTo=$2 OR nominee=$3) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, [address, address, address])
          : await db.all(sql, [address, address, address])
      } else if (txType === TransactionSearchType.AllExceptInternalTx) {
        // const sql = `SELECT * FROM transactions WHERE (transactionType=? OR transactionType=? OR transactionType=?) AND (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
        // transactions = await db.all(sql, [
        //   TransactionType.Receipt,
        //   TransactionType.StakeReceipt,
        //   TransactionType.UnstakeReceipt,
        //   address,
        //   address,
        //   address,
        // ])
        const sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE transactionType!=$1 AND (txFrom=$2 OR txTo=$3 OR nominee=$4) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE transactionType!=? AND (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, [TransactionType.InternalTxReceipt, address, address, address])
          : await db.all(sql, [TransactionType.InternalTxReceipt, address, address, address])
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
        let sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE transactionType=$1 AND (txFrom=$2 OR txTo=$3 OR nominee=$4) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE transactionType=? AND (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
        if (txType === TransactionSearchType.InternalTxReceipt) {
          sql = config.postgresEnabled
            ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE (transactionType!=$1 AND transactionType!=$2 AND transactionType!=$3) AND (txFrom=$4 OR txTo=$5 OR nominee=$6) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
            : `SELECT * FROM transactions WHERE (transactionType!=? AND transactionType!=? AND transactionType!=?) AND (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          const values = [
            TransactionType.Receipt,
            TransactionType.StakeReceipt,
            TransactionType.UnstakeReceipt,
            address,
            address,
            address,
          ]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        } else {
          const values = [ty, address, address, address]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        }
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        const sql = config.postgresEnabled
          ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE (tokenFrom=$1 OR tokenTo=$2 OR tokenOperator=$3) AND tokenType=$4 ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM tokenTxs WHERE (tokenFrom=? OR tokenTo=? OR tokenOperator=?) AND tokenType=? ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`

        transactions = config.postgresEnabled
          ? await pgDb.all(sql, [address, address, address, ty])
          : await db.all(sql, [address, address, address, ty])
      } else if (txType === TransactionSearchType.TokenTransfer) {
        if (filterAddress) {
          const sql = config.postgresEnabled
            ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE contractAddress=$1 AND (tokenFrom=$2 OR tokenTo=$3 OR tokenOperator=$4) AND NOT (tokenType=$5) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
            : `SELECT * FROM tokenTxs WHERE contractAddress=? AND (tokenFrom=? OR tokenTo=? OR tokenOperator=?) AND NOT (tokenType=?) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          const values = [
            address,
            filterAddress,
            filterAddress,
            filterAddress,
            TransactionType.EVM_Internal,
          ]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        } else {
          const sql = config.postgresEnabled
            ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE contractAddress=$1 AND NOT (tokenType=$2) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
            : `SELECT * FROM tokenTxs WHERE contractAddress=? AND NOT (tokenType=?) ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, [address, TransactionType.EVM_Internal])
            : await db.all(sql, [address, TransactionType.EVM_Internal])
        }
      }
    } else if (txType) {
      if (txType === TransactionSearchType.AllExceptInternalTx) {
        const ty = TransactionType.InternalTxReceipt
        const sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE transactionType!=$1 ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE transactionType!=? ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, [ty])
          : await db.all(sql, [ty])
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
        let sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE transactionType=$1 ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE transactionType=? ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
        if (txType === TransactionSearchType.InternalTxReceipt) {
          sql = config.postgresEnabled
            ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE transactionType!=$1 AND transactionType!=$2 AND transactionType!=$3 ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
            : `SELECT * FROM transactions WHERE transactionType!=? AND transactionType!=? AND transactionType!=? ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          const values = [
            TransactionType.Receipt,
            TransactionType.StakeReceipt,
            TransactionType.UnstakeReceipt,
          ]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        } else {
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, [ty])
            : await db.all(sql, [ty])
        }
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        const sql = config.postgresEnabled
          ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE tokenType=$1 ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM tokenTxs WHERE tokenType=? ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`

        transactions = config.postgresEnabled
          ? await pgDb.all(sql, [ty])
          : await db.all(sql, [ty])
      }
    } else {
      const sql = `SELECT *${config.postgresEnabled ? ', wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp"' : ''} FROM transactions ORDER BY cycle DESC, timestamp DESC LIMIT ${limit} OFFSET ${skip}`
      transactions = config.postgresEnabled
        ? await pgDb.all(sql)
        : await db.all(sql)
    }

    if (transactions.length > 0) {
      transactions.forEach((transaction: DbTransaction | DbTokenTx) => {
        if ('transactionType' in transaction && transaction.transactionType)
          deserializeDbTransaction(transaction)
        else if ('tokenType' in transaction && transaction.tokenType) deserializeDbToken(transaction)
      })
    }

    if (config.verbose) console.log('transactions', transactions)
  } catch (e) {
    console.log(e)
  }

  return transactions
}

export async function queryTransactionByTxId(txId: string, detail = false): Promise<Transaction | null> {
  try {
    const sql = config.postgresEnabled
      ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE txId=$1`
      : `SELECT * FROM transactions WHERE txId=?`
    const transaction: DbTransaction = config.postgresEnabled
      ? await pgDb.get(sql, [txId])
      : await db.get(sql, [txId])
    if (transaction) {
      deserializeDbTransaction(transaction)
    }
    if (detail) {
      const sql = config.postgresEnabled
        ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE txId=$1`
        : `SELECT * FROM tokenTxs WHERE txId=?`
      const tokenTxs: DbTokenTx[] = config.postgresEnabled
        ? await pgDb.all(sql, [txId])
        : await db.all(sql, [txId])
      if (tokenTxs.length > 0) {
        tokenTxs.forEach((tokenTx: DbTokenTx) => deserializeDbToken(tokenTx))
        transaction.tokenTxs = tokenTxs
      }
    }
    if (config.verbose) console.log('transaction txId', transaction)
    return transaction
  } catch (e) {
    console.log(e)
  }
  return null
}

export async function queryTransactionByHash(txHash: string, detail = false): Promise<Transaction[] | null> {
  try {
    const sql = `SELECT *${config.postgresEnabled ? ', wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp"' : ''} FROM transactions WHERE txHash=? ORDER BY cycle DESC, timestamp DESC`
    const transactions: DbTransaction[] = await db.all(sql, [txHash])
    if (transactions.length > 0) {
      for (const transaction of transactions) {
        deserializeDbTransaction(transaction)
        if (detail) {
          const sql = `SELECT *${config.postgresEnabled ? ', contractInfo::TEXT' : ''} FROM tokenTxs WHERE txId=? ORDER BY cycle DESC, timestamp DESC`
          const tokenTxs: DbTokenTx[] = await db.all(sql, [transaction.txId])
          if (tokenTxs.length > 0) {
            tokenTxs.forEach((tokenTx: DbTokenTx) => deserializeDbToken(tokenTx))
            transaction.tokenTxs = tokenTxs
          }
        }
      }
    }
    if (config.verbose) console.log('transaction hash', transactions)
    return transactions
  } catch (e) {
    console.log(e)
  }
  return null
}

export async function queryTransactionsForCycle(cycleNumber: number): Promise<Transaction[]> {
  let transactions: DbTransaction[] = []
  try {
    const sql = config.postgresEnabled
      ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle=$1 ORDER BY timestamp ASC`
      : `SELECT * FROM transactions WHERE cycle=? ORDER BY timestamp ASC`
    transactions = config.postgresEnabled
      ? await pgDb.all(sql, [cycleNumber])
      : await db.all(sql, [cycleNumber])
    if (transactions.length > 0) {
      transactions.forEach((transaction: DbTransaction) => deserializeDbTransaction(transaction))
    }
    if (config.verbose) console.log('transactions for cycle', cycleNumber, transactions)
  } catch (e) {
    console.log('exception when querying transactions for cycle', cycleNumber, e)
  }
  return transactions
}

export async function queryTransactionsBetweenCycles(
  skip = 0,
  limit = 10,
  start: number,
  end: number,
  address?: string,
  txType?: TransactionSearchType,
  filterAddress?: string
): Promise<(DbTransaction | DbTokenTx)[]> {
  let transactions: (DbTransaction | DbTokenTx)[] = []
  try {
    if (address) {
      if (!txType || TransactionSearchType.All) {
        const sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle BETWEEN $1 AND $2 AND (txFrom=$3 OR txTo=$4 OR nominee=$5) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE cycle BETWEEN ? AND ? AND (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        const values = [start, end, address, address, address]
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, values)
          : await db.all(sql, values)
      } else if (txType === TransactionSearchType.AllExceptInternalTx) {
        const ty = TransactionType.InternalTxReceipt
        const sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle BETWEEN $1 AND $2 AND transactionType!=$3 AND (txFrom=$4 OR txTo=$5 OR nominee=$6) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE cycle BETWEEN ? AND ? AND transactionType!=? AND (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        const values = [start, end, ty, address, address, address]
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, values)
          : await db.all(sql, values)
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
        let sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle BETWEEN $1 AND $2 AND transactionType=$3 AND (txFrom=$4 OR txTo=$5 OR nominee=$6) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE cycle BETWEEN ? AND ? AND transactionType=? AND (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        if (txType === TransactionSearchType.InternalTxReceipt) {
          sql = config.postgresEnabled
            ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle BETWEEN $1 AND $2 AND (transactionType!=$3 AND transactionType!=$4 AND transactionType!=$5) AND (txFrom=$6 OR txTo=$7 OR nominee=$8) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
            : `SELECT * FROM transactions WHERE cycle BETWEEN ? AND ? AND (transactionType!=? AND transactionType!=? AND transactionType!=?) AND (txFrom=? OR txTo=? OR nominee=?) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          const values = [
            start,
            end,
            TransactionType.Receipt,
            TransactionType.StakeReceipt,
            TransactionType.UnstakeReceipt,
            address,
            address,
            address,
          ]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        } else {
          const values = [start, end, ty, address, address, address]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        }
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        const sql = config.postgresEnabled
          ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE cycle BETWEEN $1 AND $2 AND (tokenFrom=$3 OR tokenTo=$4 OR tokenOperator=$5) AND tokenType=$6 ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM tokenTxs WHERE cycle BETWEEN ? AND ? AND (tokenFrom=? OR tokenTo=? OR tokenOperator=?) AND tokenType=? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        const values = [start, end, address, address, address, ty]
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, values)
          : await db.all(sql, values)
      } else if (txType === TransactionSearchType.TokenTransfer) {
        if (filterAddress) {
          const sql = config.postgresEnabled
            ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE cycle BETWEEN $1 AND $2 AND contractAddress=$3 AND (tokenFrom=$4 OR tokenTo=$5 OR tokenOperator=$6) AND NOT (tokenType=$7) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
            : `SELECT * FROM tokenTxs WHERE cycle BETWEEN ? AND ? AND contractAddress=? AND (tokenFrom=? OR tokenTo=? OR tokenOperator=?) AND NOT (tokenType=?) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          const values = [start, end, address, filterAddress, filterAddress, filterAddress, TransactionType.EVM_Internal]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        } else {
          const sql = config.postgresEnabled
            ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE cycle BETWEEN $1 AND $2 AND contractAddress=$3 AND NOT (tokenType=$4) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
            : `SELECT * FROM tokenTxs WHERE cycle BETWEEN ? AND ? AND contractAddress=? AND NOT (tokenType=?) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          const values = [start, end, address, TransactionType.EVM_Internal]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        }
      }
    } else if (txType) {
      if (txType === TransactionSearchType.AllExceptInternalTx) {
        // const ty = TransactionType.InternalTxReceipt
        // const sql = `SELECT * FROM transactions WHERE cycle BETWEEN ? and ? AND transactionType!=? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        // transactions = await db.all(sql, [start, end, ty])

        // This seems to be faster than the above query
        const sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle BETWEEN $1 AND $2 AND (transactionType=$3 OR transactionType=$4 OR transactionType=$5) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE cycle BETWEEN ? AND ? AND (transactionType=? OR transactionType=? OR transactionType=?) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        const values = [
          start,
          end,
          TransactionType.Receipt,
          TransactionType.StakeReceipt,
          TransactionType.UnstakeReceipt,
        ]
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, values)
          : await db.all(sql, values)
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
        let sql = config.postgresEnabled
          ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle BETWEEN $1 AND $2 AND transactionType=$3 ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM transactions WHERE cycle BETWEEN ? AND ? AND transactionType=? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        if (txType === TransactionSearchType.InternalTxReceipt) {
          sql = config.postgresEnabled
            ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle BETWEEN $1 AND $2 AND (transactionType!=$3 AND transactionType!=$4 AND transactionType!=$5) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
            : `SELECT * FROM transactions WHERE cycle BETWEEN ? AND ? AND (transactionType!=? AND transactionType!=? AND transactionType!=?) ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          const values = [
            start,
            end,
            TransactionType.Receipt,
            TransactionType.StakeReceipt,
            TransactionType.UnstakeReceipt,
          ]
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, values)
            : await db.all(sql, values)
        } else {
          transactions = config.postgresEnabled
            ? await pgDb.all(sql, [start, end, ty])
            : await db.all(sql, [start, end, ty])
        }
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        const sql = config.postgresEnabled
          ? `SELECT *, contractInfo::TEXT FROM tokenTxs WHERE cycle BETWEEN $1 AND $2 AND tokenType=$3 ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
          : `SELECT * FROM tokenTxs WHERE cycle BETWEEN ? AND ? AND tokenType=? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, [start, end, ty])
          : await db.all(sql, [start, end, ty])
      }
    } else {
      const sql = config.postgresEnabled
        ? `SELECT *, wrappedEVMAccount::TEXT, originalTxData::TEX, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp" FROM transactions WHERE cycle BETWEEN $1 AND $2 ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        : `SELECT * FROM transactions WHERE cycle BETWEEN ? AND ? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
      transactions = config.postgresEnabled
        ? await pgDb.all(sql, [start, end])
        : await db.all(sql, [start, end])
    }
    if (transactions.length > 0) {
      transactions.forEach((transaction: DbTransaction | DbTokenTx) => {
        if ('transactionType' in transaction && transaction.transactionType)
          deserializeDbTransaction(transaction)
        else if ('tokenType' in transaction && transaction.tokenType) deserializeDbToken(transaction)
      })
    }
  } catch (e) {
    console.log(e)
  }

  if (config.verbose) console.log('transactions betweeen cycles', transactions)
  return transactions
}

export async function queryTransactionCountBetweenCycles(
  start: number,
  end: number,
  address?: string,
  txType?: TransactionSearchType,
  filterAddress?: string
): Promise<number> {
  let transactions: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  try {
    if (address) {
      if (!txType) {
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2 AND (txFrom=$3 OR txTo=$4 OR nominee=$5)`
          : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ? AND (txFrom=? OR txTo=? OR nominee=?)`
        const values = [start, end, address, address, address]
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, values)
          : await db.get(sql, values)
      } else if (txType === TransactionSearchType.AllExceptInternalTx) {
        const ty = TransactionType.InternalTxReceipt
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2 AND transactionType!=$3 AND (txFrom=$4 OR txTo=$5 OR nominee=$6)`
          : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ? AND transactionType!=? AND (txFrom=? OR txTo=? OR nominee=?)`
        const values = [start, end, ty, address, address, address]
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, values)
          : await db.get(sql, values)
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

        let sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2 AND transactionType=$3 AND (txFrom=$4 OR txTo=$5 OR nominee=$6)`
          : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ? AND transactionType=? AND (txFrom=? OR txTo=? OR nominee=?)`

        if (txType === TransactionSearchType.InternalTxReceipt) {
          sql = config.postgresEnabled
            ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2 AND (transactionType!=$3 AND transactionType!=$4 AND transactionType!=$5) AND (txFrom=$6 OR txTo=$7 OR nominee=$8)`
            : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ? AND (transactionType!=? AND transactionType!=? AND transactionType!=?) AND (txFrom=? OR txTo=? OR nominee=?)`

          const values = [start, end, TransactionType.Receipt, TransactionType.StakeReceipt,
            TransactionType.UnstakeReceipt, address, address, address]

          transactions = config.postgresEnabled
            ? await pgDb.get(sql, values)
            : await db.get(sql, values)
        } else {
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, [start, end, ty, address, address, address])
            : await db.get(sql, [start, end, ty, address, address, address])
        }
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155

        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE cycle BETWEEN $1 and $2 AND (tokenFrom=$3 OR tokenTo=$4 OR tokenOperator=$5) AND tokenType=$6`
          : `SELECT COUNT(*) FROM tokenTxs WHERE cycle BETWEEN ? and ? AND (tokenFrom=? OR tokenTo=? OR tokenOperator=?) AND tokenType=?`
        const values = [start, end, address, address, address, ty]
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, values)
          : await db.get(sql, values)
      } else if (txType === TransactionSearchType.TokenTransfer) {
        if (filterAddress) {
          const sql = config.postgresEnabled
            ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE cycle BETWEEN $1 and $2 AND contractAddress=$3 AND (tokenFrom=$4 OR tokenTo=$5 OR tokenOperator=$6) AND NOT tokenType=$7`
            : `SELECT COUNT(*) FROM tokenTxs WHERE cycle BETWEEN ? and ? AND contractAddress=? AND (tokenFrom=? OR tokenTo=? OR tokenOperator=?) AND NOT tokenType=?`
          const values = [start, end, address, filterAddress, filterAddress,
            filterAddress, TransactionType.EVM_Internal]
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, values)
            : await db.get(sql, values)
        } else {
          const sql = config.postgresEnabled
            ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE cycle BETWEEN $1 and $2 AND contractAddress=$3 AND NOT tokenType=$4`
            : `SELECT COUNT(*) FROM tokenTxs WHERE cycle BETWEEN ? and ? AND contractAddress=? AND NOT tokenType=?`
          const values = [start, end, address, TransactionType.EVM_Internal]
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, values)
            : await db.get(sql, values)
        }
      }
    } else if (txType || txType === TransactionSearchType.All) {
      if (txType === TransactionSearchType.All) {
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2`
          : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ?`
        const values = [start, end]
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, values)
          : await db.get(sql, values)
      } else if (txType === TransactionSearchType.AllExceptInternalTx) {
        // const ty = TransactionType.InternalTxReceipt
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2 AND (transactionType=$3 OR transactionType=$4 OR transactionType=$5)`
          : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ? AND (transactionType=? OR transactionType=? OR transactionType=?)`
        const values = [start, end, TransactionType.Receipt, TransactionType.StakeReceipt, TransactionType.UnstakeReceipt]
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, values)
          : await db.get(sql, values)
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

        let sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2 AND transactionType=$3`
          : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ? AND transactionType=?`
        if (txType === TransactionSearchType.InternalTxReceipt) {
          // This is taking too long to respond
          // transactions = await db.get(sql, [start, end, ty])
          sql = config.postgresEnabled
            ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2`
            : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ?`
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, [start, end])
            : await db.get(sql, [start, end])
          const totalTxs = transactions['COUNT(*)'] || 0

          sql = config.postgresEnabled
            ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2 AND (transactionType=$3 OR transactionType=$4 OR transactionType=$5)`
            : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ? AND (transactionType=? OR transactionType=? OR transactionType=?)`
          const values = [start, end, TransactionType.Receipt,
            TransactionType.StakeReceipt, TransactionType.UnstakeReceipt]
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, values)
            : await db.get(sql, values)
          const totalTxsExceptInternalTx = transactions['COUNT(*)'] || 0
          transactions['COUNT(*)'] = totalTxs - totalTxsExceptInternalTx
        } else {
          transactions = config.postgresEnabled
            ? await pgDb.get(sql, [start, end, ty])
            : await db.get(sql, [start, end, ty])
        }
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        const sql = config.postgresEnabled
          ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE cycle BETWEEN $1 and $2 AND tokenType=$3`
          : `SELECT COUNT(*) FROM tokenTxs WHERE cycle BETWEEN ? and ? AND tokenType=?`
        const values = [start, end, ty]
        transactions = config.postgresEnabled
          ? await pgDb.get(sql, values)
          : await db.get(sql, values)
      }
    } else {
      const sql = config.postgresEnabled
        ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE cycle BETWEEN $1 and $2`
        : `SELECT COUNT(*) FROM transactions WHERE cycle BETWEEN ? and ?`
      const values = [start, end]
      transactions = config.postgresEnabled
        ? await pgDb.get(sql, values)
        : await db.get(sql, values)
    }
  } catch (e) {
    console.log(e)
  }

  if (config.verbose) console.log('transactions count between cycles', transactions)

  return transactions['COUNT(*)'] || 0
}

export async function queryTransactionCountByCycles(
  start: number,
  end: number,
  txType?: TransactionSearchType
): Promise<{ cycle: number; transactions: number }[]> {
  let transactions: { cycle: number; 'COUNT(*)': number }[] = []
  try {
    if (
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
      let sql = config.postgresEnabled
        ? `SELECT cycle, COUNT(*) as "COUNT(*)" FROM transactions WHERE transactionType=$1 GROUP BY cycle HAVING cycle BETWEEN $2 AND $3 ORDER BY cycle ASC`
        : `SELECT cycle, COUNT(*) as "COUNT(*)" FROM transactions WHERE transactionType=? GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
      if (txType === TransactionSearchType.InternalTxReceipt) {
        sql = config.postgresEnabled
          ? `SELECT cycle, COUNT(*) as "COUNT(*)" FROM transactions WHERE transactionType!=$1 AND transactionType!=$2 AND transactionType!=$3 GROUP BY cycle HAVING cycle BETWEEN $4 AND $5 ORDER BY cycle ASC`
          : `SELECT cycle, COUNT(*) as "COUNT(*)" FROM transactions WHERE transactionType!=? AND transactionType!=? AND transactionType!=? GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
        const values = [
          TransactionType.Receipt,
          TransactionType.StakeReceipt,
          TransactionType.UnstakeReceipt,
          start,
          end,
        ]
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, values)
          : await db.all(sql, values)
      } else {
        transactions = config.postgresEnabled
          ? await pgDb.all(sql, [ty, start, end])
          : await db.all(sql, [ty, start, end])
      }
    } else {
      const sql = config.postgresEnabled
        ? `SELECT cycle, COUNT(*) as "COUNT(*)" FROM transactions GROUP BY cycle HAVING cycle BETWEEN $1 AND $2 ORDER BY cycle ASC`
        : `SELECT cycle, COUNT(*) as "COUNT(*)" FROM transactions GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
      transactions = config.postgresEnabled
        ? await pgDb.all(sql, [start, end])
        : await db.all(sql, [start, end])
    }
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('Transaction count by cycles', transactions)

  return transactions.map((receipt) => {
    return {
      cycle: receipt.cycle,
      transactions: receipt['COUNT(*)'],
    }
  })
}

export async function queryTransactionCountByTimestamp(
  beforeTimestamp: number,
  afterTimestamp: number,
  address?: string,
  txType?: TransactionSearchType,
  filterAddress?: string
): Promise<number> {
  const beforeTimeString = (new Date(beforeTimestamp)).toISOString()
  const afterTimeString = (new Date(afterTimestamp)).toISOString()
  let transactions: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  let sql = config.postgresEnabled
    ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE `
    : `SELECT COUNT(*) FROM transactions WHERE `
  if (txType) {
    if (
      txType === TransactionSearchType.EVM_Internal ||
      txType === TransactionSearchType.ERC_20 ||
      txType === TransactionSearchType.ERC_721 ||
      txType === TransactionSearchType.ERC_1155 ||
      txType === TransactionSearchType.TokenTransfer
    )
      sql = config.postgresEnabled
        ? `SELECT COUNT(*) as "COUNT(*)" FROM tokenTxs WHERE `
        : `SELECT COUNT(*) FROM tokenTxs WHERE `
  }
  const values: (string | number)[] = []
  if (afterTimestamp > 0) {
    const currentPlaceholder: number = values.length + 1
    sql += `timestamp>${config.postgresEnabled ? '$' + (currentPlaceholder) : '?'} `
    values.push(afterTimeString)
  }
  if (beforeTimestamp > 0) {
    if (afterTimestamp > 0) {
      sql += `AND timestamp<${config.postgresEnabled ? '$' + (values.length + 1) : '?'} `
    } else {
      sql += `timestamp<${config.postgresEnabled ? '$' + (values.length + 1) : '?'} `
    }
    values.push(beforeTimeString)
  }
  try {
    if (address) {
      if (!txType) {
        sql += `AND (txFrom=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} OR txTo=${config.postgresEnabled ? '$' + (values.length + 2) : '?'} OR nominee=${config.postgresEnabled ? '$' + (values.length + 3) : '?'}) `
        values.push(address, address, address)
      } else if (txType === TransactionSearchType.AllExceptInternalTx) {
        const ty = TransactionType.InternalTxReceipt
        sql += `AND (txFrom=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} OR txTo=${config.postgresEnabled ? '$' + (values.length + 2) : '?'} OR nominee=${config.postgresEnabled ? '$' + (values.length + 3) : '?'}) AND transactionType!=${config.postgresEnabled ? '$' + (values.length + 4) : '?'} `
        values.push(address, address, address, ty)
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
        sql += `AND (txFrom=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} OR txTo=${config.postgresEnabled ? '$' + (values.length + 2) : '?'} OR nominee=${config.postgresEnabled ? '$' + (values.length + 3) : '?'}) AND transactionType=${config.postgresEnabled ? '$' + (values.length + 4) : '?'} `
        values.push(address, address, address, ty)
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        sql += `AND (tokenFrom=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} OR tokenTo=${config.postgresEnabled ? '$' + (values.length + 2) : '?'} OR tokenOperator=${config.postgresEnabled ? '$' + (values.length + 3) : '?'}) AND tokenType=${config.postgresEnabled ? '$' + (values.length + 4) : '?'} `
        values.push(address, address, address, ty)
      } else if (txType === TransactionSearchType.TokenTransfer) {
        if (filterAddress) {
          sql += `AND contractAddress=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} AND (tokenFrom=${config.postgresEnabled ? '$' + (values.length + 2) : '?'} OR tokenTo=${config.postgresEnabled ? '$' + (values.length + 3) : '?'} OR tokenOperator=${config.postgresEnabled ? '$' + (values.length + 4) : '?'}) AND NOT tokenType=${config.postgresEnabled ? '$' + (values.length + 5) : '?'} `
          values.push(address, filterAddress, filterAddress, filterAddress, TransactionType.EVM_Internal)
        } else {
          sql += `AND contractAddress=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} AND NOT tokenType=${config.postgresEnabled ? '$' + (values.length + 2) : '?'} `
          values.push(address, TransactionType.EVM_Internal)
        }
      }
    } else if (txType) {
      if (txType === TransactionSearchType.AllExceptInternalTx) {
        const ty = TransactionType.InternalTxReceipt
        sql += `AND transactionType!=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} `
        values.push(ty)
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
        sql += `AND transactionType=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} `
        values.push(ty)
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        sql += `AND tokenType=${config.postgresEnabled ? '$' + (values.length + 1) : '?'} `
        values.push(ty)
      }
    }
    transactions = config.postgresEnabled
      ? await pgDb.get(sql, values)
      : await db.get(sql, values)
  } catch (e) {
    console.log(e)
  }

  if (config.verbose) console.log('transactions count by timestamp', transactions)

  return transactions['COUNT(*)'] || 0
}

export async function queryTransactionsByTimestamp(
  skip = 0,
  limit = 10,
  beforeTimestamp: number,
  afterTimestamp: number,
  address?: string,
  txType?: TransactionSearchType,
  filterAddress?: string
): Promise<(DbTransaction | DbTokenTx)[]> {
  const beforeTimeString = (new Date(beforeTimestamp)).toISOString()
  const afterTimeString = (new Date(afterTimestamp)).toISOString()
  let transactions: (DbTransaction | DbTokenTx)[] = []
  let sql = `SELECT *${config.postgresEnabled ? ', wrappedEVMAccount::TEXT, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp"' : ''} FROM transactions WHERE `
  if (txType) {
    if (
      txType === TransactionSearchType.EVM_Internal ||
      txType === TransactionSearchType.ERC_20 ||
      txType === TransactionSearchType.ERC_721 ||
      txType === TransactionSearchType.ERC_1155 ||
      txType === TransactionSearchType.TokenTransfer
    )
      sql = `SELECT *${config.postgresEnabled ? ', contractInfo::TEXT' : ''} FROM tokenTxs WHERE `
  }
  const values: (string | number)[] = []
  let sqlSuffix = ''
  if (afterTimestamp > 0) {
    const valuePlaceholder: number = values.length + 1

    sql += ` timestamp${config.postgresEnabled ? `>$${valuePlaceholder}` : '>?'} `
    sqlSuffix = ` ORDER BY timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    values.push(afterTimeString)
  }
  if (beforeTimestamp > 0) {
    const valuePlaceholder: number = values.length + 1
    if (afterTimestamp > 0) {
      sql += ` AND timestamp${config.postgresEnabled ? `<$${valuePlaceholder}` : '<?'} `
    } else {
      sql += ` timestamp${config.postgresEnabled ? `<$${valuePlaceholder}` : '<?'} `
      sqlSuffix = ` ORDER BY timestamp DESC LIMIT ${limit} OFFSET ${skip}`
    }
    values.push(beforeTimeString)
  }
  try {
    if (address) {
      if (!txType || TransactionSearchType.All) {
        const valuePlaceholder: number = values.length

        sql += ` AND (txFrom${config.postgresEnabled ? `=$${valuePlaceholder + 1}` : '=?'} OR txTo${config.postgresEnabled ? `=$${valuePlaceholder + 2}` : '=?'} OR nominee${config.postgresEnabled ? `=$${valuePlaceholder + 3}` : '=?'})`
        values.push(address, address, address)
      } else if (txType === TransactionSearchType.AllExceptInternalTx) {
        const ty = TransactionType.InternalTxReceipt
        sql += ` AND (txFrom${config.postgresEnabled ? `=$${values.length + 1}` : '=?'} OR txTo${config.postgresEnabled ? `=$${values.length + 2}` : '=?'} OR nominee${config.postgresEnabled ? `=$${values.length + 3}` : '=?'}) AND transactionType${config.postgresEnabled ? `!=$${values.length + 4}` : '!=?'}`
        values.push(address, address, address, ty)
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
        sql += ` AND (txFrom${config.postgresEnabled ? `=$${values.length + 1}` : '=?'} OR txTo${config.postgresEnabled ? `=$${values.length + 2}` : '=?'} OR nominee${config.postgresEnabled ? `=$${values.length + 3}` : '=?'}) AND transactionType${config.postgresEnabled ? `=$${values.length + 4}` : '=?'}`
        values.push(address, address, address, ty)
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        sql += ` AND (tokenFrom${config.postgresEnabled ? `=$${values.length + 1}` + (values.length + 1) : '=?'} OR tokenTo${config.postgresEnabled ? `=$${values.length + 2}` : '=?'} OR tokenOperator${config.postgresEnabled ? `=$${values.length + 3}` : '=?'}) AND tokenType${config.postgresEnabled ? `=$${values.length + 4}` : '=?'}`
        values.push(address, address, address, ty)
      } else if (txType === TransactionSearchType.TokenTransfer) {
        if (filterAddress) {
          sql += ` AND contractAddress${config.postgresEnabled ? `=$${values.length + 1}` : '=?'} AND (tokenFrom${config.postgresEnabled ? `=$${values.length + 2}` : '=?'} OR tokenTo${config.postgresEnabled ? `=$${values.length + 3}` : '=?'} OR tokenOperator${config.postgresEnabled ? `=$${values.length + 4}` : '=?'}) AND NOT (tokenType${config.postgresEnabled ? `=$${values.length + 5}` : '=?'})`
          values.push(address, filterAddress, filterAddress, filterAddress, TransactionType.EVM_Internal)
        } else {
          sql += ` AND contractAddress=${config.postgresEnabled ? `=$${values.length + 1}` : '=?'} AND NOT (tokenType${config.postgresEnabled ? `=$${values.length + 2}` : '=?'})`
          values.push(address, TransactionType.EVM_Internal)
        }
      }
    } else if (txType) {
      if (txType === TransactionSearchType.AllExceptInternalTx) {
        const ty = TransactionType.InternalTxReceipt
        sql += ` AND transactionType${config.postgresEnabled ? `!=$${values.length + 1}` : '!=?'}`
        values.push(ty)
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
        sql += ` AND transactionType${config.postgresEnabled ? `=$${values.length + 1}` : '=?'}`
        values.push(ty)
      } else if (
        txType === TransactionSearchType.EVM_Internal ||
        txType === TransactionSearchType.ERC_20 ||
        txType === TransactionSearchType.ERC_721 ||
        txType === TransactionSearchType.ERC_1155
      ) {
        const ty =
          txType === TransactionSearchType.EVM_Internal
            ? TransactionType.EVM_Internal
            : txType === TransactionSearchType.ERC_20
              ? TransactionType.ERC_20
              : txType === TransactionSearchType.ERC_721
                ? TransactionType.ERC_721
                : TransactionType.ERC_1155
        sql += ` AND tokenType${config.postgresEnabled ? `=$${values.length + 1}` : '=?'}`
        values.push(ty)
      }
    }
    sql += sqlSuffix
    transactions = config.postgresEnabled
      ? await pgDb.all(sql)
      : await db.all(sql);
    if (transactions.length > 0) {
      transactions.forEach((transaction: DbTransaction | DbTokenTx) => {
        if ('transactionType' in transaction) deserializeDbTransaction(transaction)
        else if ('tokenType' in transaction) deserializeDbToken(transaction)
      })
    }
  } catch (e) {
    console.log(e)
  }

  if (config.verbose) console.log('transactions by timestamp', transactions)
  return transactions
}

// transactionCount with txType = Receipt, StakeReceipt, UnstakeReceipt
export async function queryTransactionCountByBlock(blockNumber: number, blockHash: string): Promise<number> {
  let transactions: { 'COUNT(*)': number } = { 'COUNT(*)': 0 }
  let sql = config.postgresEnabled
    ? `SELECT COUNT(*) as "COUNT(*)" FROM transactions WHERE transactionType IN ($1, $2, $3) `
    : `SELECT COUNT(*) FROM transactions WHERE transactionType IN (?,?,?) `
  const values: (string | number)[] = [
    TransactionType.Receipt,
    TransactionType.StakeReceipt,
    TransactionType.UnstakeReceipt,
  ]
  if (blockNumber > 0) {
    sql += config.postgresEnabled ? `AND blockNumber=$4 ` : `AND blockNumber=? `
    values.push(blockNumber)
  } else if (blockHash) {
    sql += config.postgresEnabled ? `AND blockHash=$4 ` : `AND blockHash=? `
    values.push(blockHash)
  }
  try {
    transactions = config.postgresEnabled
      ? await pgDb.get(sql, values)
      : await db.get(sql, values)
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('transactions count by block', transactions)
  return transactions['COUNT(*)'] || 0
}

// transactions with txType = Receipt, StakeReceipt, UnstakeReceipt
export async function queryTransactionsByBlock(
  blockNumber: number,
  blockHash: string
): Promise<DbTransaction[]> {
  let transactions: DbTransaction[] = []
  let sql = `SELECT *${config.postgresEnabled ? ', wrappedEVMAccount::TEX, originalTxData::TEXT, (extract(epoch from "timestamp")*1000)::bigint AS "timestamp"' : ''} FROM transactions WHERE transactionType IN ${config.postgresEnabled ? '($1, $2, $3)' : '(?,?,?)'}`
  const values: (string | number)[] = [
    TransactionType.Receipt,
    TransactionType.StakeReceipt,
    TransactionType.UnstakeReceipt,
  ]
  if (blockNumber >= 0) {
    sql += `AND blockNumber${config.postgresEnabled ? `=$${values.length + 1}` : '=?'} `
    values.push(blockNumber)
  } else if (blockHash) {
    sql += `AND blockHash${config.postgresEnabled ? `=$${values.length + 1}` : '=?'} `
    values.push(blockHash)
  }
  sql += `ORDER BY timestamp ASC;`
  try {
    transactions = config.postgresEnabled
      ? await pgDb.all(sql, values)
      : await db.all(sql, values)
    if (transactions.length > 0) {
      transactions.forEach((transaction: DbTransaction) => deserializeDbTransaction(transaction))
    }
  } catch (e) {
    console.log(e)
  }
  if (config.verbose) console.log('transactions by block', transactions)
  return transactions
}

export async function queryTokenTxByTxId(txId: string): Promise<DbTokenTx[] | []> {
  try {
    const sql = `SELECT *${config.postgresEnabled ? ', contractInfo::TEXT' : ''} FROM tokenTxs WHERE txId${config.postgresEnabled ? '=$1' : '=?'}`
    const tokenTxs: DbTokenTx[] = config.postgresEnabled
      ? await pgDb.all(sql, [txId])
      : await db.all(sql, [txId])
    if (config.verbose) console.log('tokenTxs txId', tokenTxs)
    return tokenTxs
  } catch (e) {
    console.log(e)
  }
  return []
}

function deserializeDbTransaction(transaction: DbTransaction): void {
  transaction.wrappedEVMAccount = StringUtils.safeJsonParse(transaction.wrappedEVMAccount)
  transaction.originalTxData = StringUtils.safeJsonParse(transaction.originalTxData)
}

function deserializeDbToken(transaction: DbTokenTx): void {
  transaction.contractInfo = StringUtils.safeJsonParse(transaction.contractInfo)
}
