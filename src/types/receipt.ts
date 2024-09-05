import { Signature } from '@shardus/crypto-utils'
import { Log } from './transaction'
import { AccountCopy } from './account'

export type Proposal = {
  applied: boolean
  cant_preApply: boolean
  accountIDs: string[]
  beforeStateHashes: string[]
  afterStateHashes: string[]
  appReceiptDataHash: string
  txid: string
}

export type Vote = {
  proposalHash: string
  sign?: Signature
}

interface AccountsCopy {
  accountId: string
  cycleNumber: number
  data: any
  timestamp: number
  hash: string
  isGlobal: boolean
}

export type SignedReceipt = {
  proposal: Proposal
  proposalHash: string // Redundant, may go
  signaturePack: Signature[]
  voteOffsets: number[]
  sign?: Signature
}
/**
 * ArchiverReceipt is the full data (shardusReceipt + appReceiptData + accounts ) of a tx that is sent to the archiver
 */
export interface ArchiverReceipt {
  tx: {
    originalTxData: object
    txId: string
    timestamp: number
  }
  cycle: number
  signedReceipt: SignedReceipt
  afterStates?: AccountsCopy[]
  beforeStates?: AccountsCopy[]
  appReceiptData: any
  executionShardKey: string
  globalModification: boolean
}

export type AppliedVote = {
  txid: string
  transaction_result: boolean
  account_id: string[]
  //if we add hash state before then we could prove a dishonest apply vote
  //have to consider software version
  account_state_hash_after: string[]
  account_state_hash_before: string[]
  cant_apply: boolean // indicates that the preapply could not give a pass or fail
  node_id: string // record the node that is making this vote.. todo could look this up from the sig later
  sign: Signature
  // hash of app data
  app_data_hash: string
}

/**
 * a space efficent version of the receipt
 *
 * use TellSignedVoteHash to send just signatures of the vote hash (votes must have a deterministic sort now)
 * never have to send or request votes individually, should be able to rely on existing receipt send/request
 * for nodes that match what is required.
 */
export type AppliedReceipt2 = {
  txid: string
  result: boolean
  //single copy of vote
  appliedVote: AppliedVote
  confirmOrChallenge: ConfirmOrChallengeMessage
  //all signatures for this vote
  signatures: [Signature] //Could have all signatures or best N.  (lowest signature value?)
  // hash of app data
  app_data_hash: string
}

export type ConfirmOrChallengeMessage = {
  message: string
  nodeId: string
  appliedVote: AppliedVote
  sign: Signature
}
export interface Receipt extends ArchiverReceipt {
  receiptId: string
  timestamp: number
  applyTimestamp: number
}

export interface ReadableReceipt {
  status?: boolean | string | number
  transactionHash: string
  transactionIndex: string
  blockNumber: string
  nonce: string
  blockHash: string
  cumulativeGasUsed: string
  gasUsed: string
  logs: Log[]
  logBloom: string
  contractAddress: string | null
  from: string
  to: string | null
  value: string
  data: string
  stakeInfo?: {
    nominee?: string
    stakeAmount?: string
    totalStakeAmount?: string
    totalUnstakeAmount?: string
    stake?: string
    reward?: string
    penalty?: string
  }
}
