import { TransactionFactory, Transaction, TransactionType } from '@ethereumjs/tx'
import { bytesToHex, toAscii, toBytes } from '@ethereumjs/util'
import { RawTxData } from '../types'
import { config } from '../config'
import { Utils as StringUtils } from '@shardus/types'

export type TransactionObj =
  | Transaction[TransactionType.Legacy]
  | Transaction[TransactionType.AccessListEIP2930]
import { TransactionType as TransactionType2, OriginalTxDataInterface } from '../types'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function getTransactionObj(tx: RawTxData): TransactionObj {
  if (!tx.raw) throw Error('tx has no raw field')
  let transactionObj
  const serializedInput = toBytes(tx.raw)
  try {
    transactionObj = TransactionFactory.fromSerializedData<TransactionType.Legacy>(serializedInput)
  } catch (e) {
    /* prettier-ignore */ if (config.verbose) console.log('Unable to get legacy transaction obj', e)
  }
  if (!transactionObj) {
    try {
      transactionObj =
        TransactionFactory.fromSerializedData<TransactionType.AccessListEIP2930>(serializedInput)
    } catch (e) {
      /* prettier-ignore */ if (config.verbose) console.log('Unable to get transaction obj', e)
    }
  }

  if (transactionObj) {
    return transactionObj
  } else {
    throw Error('tx obj fail')
  }
}

const stakeTargetAddress = '0x0000000000000000000000000000000000000001' //dev-relaunch required to change this '0x0000000000000000000000000000000000010000',

export function isStakingEVMTx(
  transaction: Transaction[TransactionType.Legacy] | Transaction[TransactionType.AccessListEIP2930]
): boolean {
  if (transaction.to && transaction.to.toString() === stakeTargetAddress) return true
  return false
}

export type StakeTxBlobFromEVMTx =
  | Transaction[TransactionType.Legacy]
  | Transaction[TransactionType.AccessListEIP2930]

export function getStakeTxBlobFromEVMTx(transaction: StakeTxBlobFromEVMTx): unknown {
  try {
    const stakeTxString = toAscii(bytesToHex(transaction.data))
    return StringUtils.safeJsonParse(stakeTxString)
  } catch (e) {
    console.log('Unable to get stakeTxBlobFromEVMTx', e)
  }
}

export function decodeEVMRawTxData(originalTxData: OriginalTxDataInterface): void {
  if (originalTxData.originalTxData.tx.raw) {
    // EVM Tx
    const txObj = getTransactionObj(originalTxData.originalTxData.tx)
    // Custom readableReceipt for originalTxsData
    if (txObj) {
      const readableReceipt = {
        from: txObj.getSenderAddress().toString(),
        to: txObj.to ? txObj.to.toString() : null,
        nonce: txObj.nonce.toString(16),
        value: txObj.value.toString(16),
        data: '0x' + txObj.data.toString(),
      }
      if (
        originalTxData.transactionType === TransactionType2.StakeReceipt ||
        originalTxData.transactionType === TransactionType2.UnstakeReceipt
      ) {
        const internalTxData = getStakeTxBlobFromEVMTx(txObj)
        readableReceipt['internalTxData'] = internalTxData
      }
      originalTxData.originalTxData = { ...originalTxData.originalTxData, readableReceipt }
    }
  }
}
