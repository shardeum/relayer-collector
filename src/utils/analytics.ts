import { type Transaction } from '../storage/transaction'
import { type Cycle } from '../storage/cycle'
import * as db from '../storage/dbStorage'
import { bigIntToHex } from '@ethereumjs/util'
import BN from 'bn.js'
import web3 from 'web3'
import { JoinRequest, JoinedConsensor } from '@shardus/types/build/src/p2p/JoinTypes'
import { safeJsonParse } from '@shardus/types/build/src/utils/functions/stringify'
import { TransactionType, InternalTXType } from '../types'

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


const idStates = ['startedSyncing', 'finishedSyncing', 'activated', 'removed', 'apoptosized', 'lostAfterSelection', 'lostSyncing']
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
  key: string
}

export const transformCycle = async (cycle: Cycle) => {
  if (typeof cycle.cycleRecord === "string") {
    cycle.cycleRecord = safeJsonParse(cycle.cycleRecord)
  }

  const cycleRecordStartISOString = (new Date(cycle.cycleRecord.start * 1000)).toISOString()
  if (cycle.cycleRecord.mode == "shutdown") {
    let sql = `UPDATE analyticsCycle
      SET
          "leftTime" = $1,
          "activeEndCycle" = $2
      WHERE
          "leftTime" IS NULL AND
          "activeEndCycle" IS NULL AND
          "activeStartCycle" IS NOT NULL;
      `
    const values = [cycleRecordStartISOString, cycle.counter]
    await db.run(sql, values)


    sql = `UPDATE analyticsCycle
      SET
          "leftTime" = $1
      WHERE
          "leftTime" IS NULL AND
          "activeEndCycle" IS NULL AND
          "activeStartCycle" IS NULL;
      `

    await db.run(sql, [cycleRecordStartISOString])
    return
  }

  for (let i = 0; i < Object.keys(cycle.cycleRecord).length; i++) {
    const key = Object.keys(cycle.cycleRecord)[i];
    const value = cycle.cycleRecord[key]
    let sql: string = ""

    if (
      Array.isArray(value) &&
      !key.toLowerCase().includes('archivers') &&
      (idStates.includes(key) || pubKeyStates.includes(key))
    ) {
      switch (key) {
        case "standbyAdd":
          const fields = ["nominator", "publicKey", "joinedTime", "nodeVersion"].map((e) => `"${e}"`).join(", ")

          sql = `INSERT INTO analyticsCycle (${fields})
          VALUES ($1, $2, $3)
        `

          for (let index = 0; index < value.length; index++) {
            const item: JoinRequest = value[index];
            const values = []

            values.push(item?.appJoinData?.stakeCert?.nominator ?? null)
            values.push(item.nodeInfo.address)
            values.push(cycleRecordStartISOString)
            values.push(item?.appJoinData?.version)

            await db.run(sql, values)
          }

          break;

        case "lostAfterSelection":
        case "lostSyncing":
          sql = `UPDATE analyticsCycle
            SET "leftTime" = $1
            WHERE "nodeId" = $2
            AND "leftTime" IS NULL;`

          for (let index = 0; index < value.length; index++) {
            const item: string = value[index];

            const values = []
            values.push(cycleRecordStartISOString)
            values.push(item)

            await db.run(sql, values)
          }
          break;
        case "standbyRemove":

          sql = `UPDATE analyticsCycle
            SET "leftTime" = $1
            WHERE "publicKey" = $2
            AND "leftTime" IS NULL;`

          for (let index = 0; index < value.length; index++) {
            const item: string = value[index];

            const values = []
            values.push(cycleRecordStartISOString)
            values.push(item)

            await db.run(sql, values)
          }
          break;

        case "joinedConsensors":
          sql = `UPDATE analyticsCycle
            SET "nodeId" = $1
            WHERE "publicKey" = $2
            AND "nodeId" IS NULL
            AND "leftTime" IS NULL;`

          for (let index = 0; index < value.length; index++) {
            const item: JoinedConsensor = value[index];

            const values = []
            values.push(item.id)
            values.push(item.publicKey)

            await db.run(sql, values)
          }
          break;

        case "activated":
          sql = `UPDATE analyticsCycle
            SET "activeStartCycle" = $1
            WHERE "nodeId" = $2
            AND "leftTime" IS NULL;`

          for (let index = 0; index < value.length; index++) {
            const item: string = value[index];

            const values = []
            values.push(cycle.counter)
            values.push(item)

            await db.run(sql, values)
          }
          break;
        case "apoptosized":
        case "removed":
          sql = `UPDATE analyticsCycle
            SET "activeEndCycle" = $1,
                "leftTime" = $2
            WHERE "nodeId" = $3
            AND "leftTime" IS NULL;`
          for (let index = 0; index < value.length; index++) {
            const item: string = value[index];

            const values = []
            values.push(cycle.counter)
            values.push(cycleRecordStartISOString)
            values.push(item)

            await db.run(sql, values)
          }
          break;
        default:
          break;
      }
    }
  }
}


export const transformTransaction = (tx: Transaction) => {
  const internalTX = tx.wrappedEVMAccount['readableReceipt']?.['internalTx']
  let internalTXType = undefined

  if (internalTX) {
    internalTXType = internalTX?.['internalTXType']
  } else {
    if (tx.transactionType === TransactionType.StakeReceipt) {
      internalTXType = InternalTXType.Stake
    } else if (tx.transactionType === TransactionType.UnstakeReceipt) {
      internalTXType = InternalTXType.Unstake
    } else {
      internalTXType = undefined
    }
  }

  return {
    txId: tx.txId,
    cycle: tx.cycle,
    timestamp: (new Date(tx.timestamp)).toISOString(),
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
    rewardAmount:
      tx.wrappedEVMAccount['readableReceipt']?.['rewardAmount'] &&
      bigIntToHex(tx.wrappedEVMAccount['readableReceipt']?.['rewardAmount']),
    penaltyAmount:
      tx.wrappedEVMAccount['readableReceipt']?.['penaltyAmount'] &&
      bigIntToHex(tx.wrappedEVMAccount['readableReceipt']?.['penaltyAmount']),
    violationType:
      tx.wrappedEVMAccount['readableReceipt']?.['internalTX']?.['violationType'],
    internalTXType: internalTXType,
    wrappedEVMAccount: tx.wrappedEVMAccount
  }
}
