import { type Transaction } from '../storage/transaction'
import { type Cycle } from '../storage/cycle'
import * as pgDb from '../storage/pgStorage'
import { bigIntToHex } from '@ethereumjs/util'
import BN from 'bn.js'
import web3 from 'web3'
import { JoinRequest, JoinedConsensor } from '@shardus/types/build/src/p2p/JoinTypes'
import { safeJsonParse } from '@shardus/types/build/src/utils/functions/stringify'

const NETWORK_VERSION = '1.11.0'


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


const idStates = ['startedSyncing', 'finishedSyncing', 'activated', 'removed', 'apoptosized']
const pubKeyStates = ['standbyAdd', 'standbyRefresh', 'standbyRemove', 'joinedConsensors']

export type CycleRecordRow = {
  version: string
  eventName: string
  cycleMarker: string
  counter: number
  timestampEpoch: number
  publicKey?: string
  id?: string
  externalIp?: string
  externalPort?: number
}

export const transformCycleRecords = (cycle: Cycle) => {
  const allAnalyticsCycleRecords: CycleRecordRow[] = []

  if (typeof cycle.cycleRecord === "string") {
    cycle.cycleRecord = safeJsonParse(cycle.cycleRecord)
  }

  Object.keys(cycle.cycleRecord).forEach((key) => {
    const value = cycle.cycleRecord[key]

    if (
      Array.isArray(value) &&
      !key.toLowerCase().includes('archivers') &&
      (idStates.includes(key) || pubKeyStates.includes(key))
    ) {
      if (key == 'joinedConsensors') {
        value.forEach((item: JoinedConsensor) => {
          allAnalyticsCycleRecords.push({
            version: NETWORK_VERSION,
            eventName: key,
            cycleMarker: cycle.cycleMarker,
            counter: cycle.counter,
            timestampEpoch: cycle.cycleRecord.start,
            publicKey: item.publicKey,
            id: item.id,
          })
        })
      } else if (key == 'standbyAdd') {
        value.forEach((item: JoinRequest) => {
          allAnalyticsCycleRecords.push({
            version: NETWORK_VERSION,
            eventName: key,
            cycleMarker: cycle.cycleMarker,
            counter: cycle.counter,
            timestampEpoch: cycle.cycleRecord.start,
            publicKey: item.nodeInfo.address,
            externalIp: item.nodeInfo.externalIp,
            externalPort: item.nodeInfo.externalPort,
          })
        })
      } else if (idStates.includes(key)) {
        value.forEach((item: string) => {
          allAnalyticsCycleRecords.push({
            version: NETWORK_VERSION,
            eventName: key,
            cycleMarker: cycle.cycleMarker,
            counter: cycle.counter,
            timestampEpoch: cycle.cycleRecord.start,
            id: item,
          })
        })
      } else if (pubKeyStates.includes(key)) {
        value.forEach((item: string) => {
          allAnalyticsCycleRecords.push({
            version: NETWORK_VERSION,
            eventName: key,
            cycleMarker: cycle.cycleMarker,
            counter: cycle.counter,
            timestampEpoch: cycle.cycleRecord.start,
            publicKey: item,
          })
        })
      }
    }
  })

  return allAnalyticsCycleRecords
}

export const transformTransaction = (tx: Transaction) => {
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
    version: NETWORK_VERSION,
    wrappedEVMAccount: tx.wrappedEVMAccount
  }
}
