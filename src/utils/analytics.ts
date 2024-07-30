import { type Transaction } from '../storage/transaction'
import * as pgDb from '../storage/pgStorage'
import { bigIntToHex } from '@ethereumjs/util'
import BN from 'bn.js'
import web3 from 'web3'


export declare type HexString = string;
export declare type Bytes = Uint8Array | HexString;
export declare type Numbers = number | bigint | string | HexString;

type EtherUnits = 'noether' | 'wei' | 'kwei' | 'Kwei' | 'babbage' | 'femtoether' | 'mwei' | 'Mwei' | 'lovelace' | 'picoether' | 'gwei' | 'Gwei' | 'shannon' | 'nanoether' | 'nano' | 'szabo' | 'microether' | 'micro' | 'finney' | 'milliether' | 'milli' | 'ether' | 'kether' | 'grand' | 'mether' | 'gether' | 'tether'

export const fromWeiNoTrailingComma = (number: Numbers, unit: EtherUnits): string => {
  const result = web3.utils.fromWei(number, unit)
  return result === '0.' ? '0' : result
}

export const calculateFullValue = (value: string | BN): string => {
  try {
    return fromWeiNoTrailingComma(value.toString(), 'ether')
  } catch (e) {
    return 'error in calculating Value'
  }
}

export enum AnalyticsTableNames {
  ACCOUNTS = 'analyticsAccounts',
  CYCLES = 'analyticsCycles',
  TRANSACTIONS = 'analyticsTransactions'
}

const transformTransaction = (tx: Transaction): any => {
  return {
    txId: tx.txId,
    cycle: tx.cycle,
    timestamp: tx.timestamp,
    blockNumber: tx.blockNumber,
    blockHash: tx.blockHash,
    txFrom: tx.txFrom,
    txTo: tx.txTo,
    nominee: tx.nominee,
    txHash: tx.txHash,
    transactionType: tx.transactionType,
    contractAddress: tx.wrappedEVMAccount['readableReceipt']?.['contractAddress'],
    data: tx.wrappedEVMAccount['readableReceipt']?.['data'].slice(0, 65534),
    from: tx.wrappedEVMAccount['readableReceipt']?.['from'],
    nonce: tx.wrappedEVMAccount['readableReceipt']?.['nonce'],
    status: tx.wrappedEVMAccount['readableReceipt']?.['status'],
    to: tx.wrappedEVMAccount['readableReceipt']?.['to'],
    transactionHash: tx.wrappedEVMAccount['readableReceipt']?.['transactionHash'],
    value: tx.wrappedEVMAccount['readableReceipt']?.['value'],
    isInternalTx: tx.wrappedEVMAccount['readableReceipt']?.['isInternalTx'],
    internalTx: tx.wrappedEVMAccount['readableReceipt']?.['internalTx'],
    accountType: tx.wrappedEVMAccount?.['accountType'],
    amountSpent: tx.wrappedEVMAccount?.['amountSpent'],
    ethAddress: tx.wrappedEVMAccount?.['ethAddress'],
    hash: tx.wrappedEVMAccount?.['hash'],
    penalty:
      tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['penalty'] &&
      bigIntToHex(tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['penalty']).slice('0x'.length),
    reward:
      tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['reward'] &&
      bigIntToHex(tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['reward']),
    stake:
      tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['stake'] &&
      bigIntToHex(tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['stake']),
    totalUnstakeAmount:
      tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['totalUnstakeAmount'] &&
      bigIntToHex(tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['totalUnstakeAmount']),
    totalStakeAmount:
      tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['totalStakeAmount'] &&
      bigIntToHex(tx.wrappedEVMAccount['readableReceipt']?.['stakeInfo']?.['totalStakeAmount']),
    value_decimal:
      tx.wrappedEVMAccount['readableReceipt']?.['value'] &&
      calculateFullValue(tx.wrappedEVMAccount['readableReceipt']?.['value']),
    amountSpent_decimal:
      tx.wrappedEVMAccount?.['amountSpent'] && calculateFullValue(tx.wrappedEVMAccount?.['amountSpent']),
  }
}

export async function upsertTransaction(transaction: Transaction): Promise<void> {
  const transformedTxn = transformTransaction(transaction);

  const fields = Object.keys(transformedTxn)
  const cols = fields.map((field) => `${field}`).join(', ')
  const updateStrategyOnConflict = fields.map((field) => `${field} = EXCLUDED.${field}`).join(', ')
  const values = Object.values(transformedTxn)

  const placeholders = fields.map((_, ind) => `$${ind + 1}`).join(',')

  let sql = `INSERT INTO ${AnalyticsTableNames.TRANSACTIONS}(${cols}) VALUES (${placeholders}) ON CONFLICT ${updateStrategyOnConflict}`
  await pgDb.run(sql, values)
}

export async function bulkUpsertTransactions(transactions: Transaction[]): Promise<void> {
  if ((typeof transactions === 'undefined') || (transactions.length == 0)) {
    return;
  }
  const firstTransformedTxn = transformTransaction(transactions[0])
  const fields = Object.keys(firstTransformedTxn)
  const cols = fields.map((field) => `${field}`).join(', ')
  const updateStrategyOnConflict = fields.map((field) => `${field} = EXCLUDED.${field}`).join(', ')

  const transformedTransactionValues = transactions.map((transaction) => transformTransaction(transaction))
  const placeholders = transformedTransactionValues.map((txn, i) => {
    const currentPlaceholders = Object.keys(txn)
      .map((_, j) => `$${i * Object.keys(txn).length + j + 1}`)
      .join(', ')
    return `(${currentPlaceholders})`
  }).join(", ")

  const values = pgDb.extractValuesFromArray(transformedTransactionValues)
  const sql = `INSERT INTO ${AnalyticsTableNames.TRANSACTIONS} (${cols}) VALUES ${placeholders} ON CONFLICT ${updateStrategyOnConflict}`
  await pgDb.run(sql, values)
}








