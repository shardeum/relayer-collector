import { Account as EVMAccount } from '@ethereumjs/util'
import { TxReceipt } from '@ethereumjs/vm/dist/types'
import { ReadableReceipt } from './receipt'
import { ERC20ContractDetail, NetworkParameters } from '.'
import { ContractInfo, TokenTx, TransactionType } from './transaction'

/** Same as type AccountsCopy in the shardus core */
export type AccountCopy = {
  accountId: string
  data: any // Todo: Create a type of different accounts
  timestamp: number
  hash: string
  cycleNumber: number
  isGlobal: boolean
}

type hexNumber = {
  value: string;
  dataType: string;
}

export interface Account {
  accountId: string
  cycle: number
  timestamp: number | string
  ethAddress: string
  account: WrappedEVMAccount
  hash: string
  accountType: AccountType
  isGlobal: boolean
  contractType?: ContractType
  contractInfo?: ContractInfo
}

// AccountEntry: is the model used by shardeum-indexer
export interface AccountEntry {
  accountId: string
  timestamp: number
  data: WrappedEVMAccount
}

export interface Token {
  ethAddress: string
  contractAddress: string
  tokenValue: string
  tokenType: TransactionType
}

export enum ContractType {
  GENERIC,
  ERC_20,
  ERC_721,
  ERC_1155,
}

export enum AccountType {
  Account, //  EOA or CA
  ContractStorage, // Contract storage key value pair
  ContractCode, // Contract code bytes
  Receipt, //This holds logs for a TX
  Debug,
  NetworkAccount,
  NodeAccount,
  NodeRewardReceipt,
  DevAccount,
  NodeAccount2,
  StakeReceipt,
  UnstakeReceipt,
  InternalTxReceipt,
}

export enum AccountSearchType {
  All, // All Accounts Type
  EOA,
  CA,
  GENERIC, // Generic Contract Type
  ERC_20,
  ERC_721,
  ERC_1155,
  Receipt, // EVM Receipt
  StakeReceipt,
  UnstakeReceipt,
  NetworkAccount,
  NodeAccount,
  NodeRewardReceipt,
  NodeAccount2,
  ContractStorage,
  ContractCode,
  InternalTxReceipt,
}

export interface BaseAccount {
  accountType: AccountType
}

interface BaseWrappedEVMAccount extends BaseAccount {
  /** account address in EVM space. can have different meanings depending on account type */
  ethAddress: string

  /** account hash */
  hash: string

  /** account timestamp. last time a TX changed it */
  timestamp: number
}

/**
 * Still working out the details here.
 * This has become a variant data type now that can hold an EVM account or a key value pair from CA storage
 * I think that is the shortest path for now to get syncing and repair functionality working
 *
 * Long term I am not certain if we will be able to hold these in memory.  They may have to be a temporary thing
 * that is held in memory for awhile but eventually cleared.  This would mean that we have to be able to pull these
 * from disk again, and that could be a bit tricky.
 */
export type WrappedEVMAccount = BaseWrappedEVMAccount &
  (WrappedDataReceipt | WrappedDataAccount | WrappedDataContractStorage | WrappedDataContractCode) & { account?: EVMAccount }

/** Variant data: account */
export interface WrappedDataAccount {
  accountType: AccountType.Account
  account: EVMAccount
}

/** Variant data: contract storage */
export interface WrappedDataContractStorage {
  accountType: AccountType.ContractStorage

  /** EVM CA storage key */
  key: string

  /** EVM buffer value if this is of type CA_KVP */
  value: Uint8Array
}

/** Variant data: contract code related and addresses */
export interface WrappedDataContractCode {
  accountType: AccountType.ContractCode

  codeByte: Uint8Array
  codeHash: Uint8Array
  contractAddress: string
}

/** Variant data: receipt related */
export interface WrappedDataReceipt {
  accountType:
  | AccountType.Receipt
  | AccountType.NodeRewardReceipt
  | AccountType.StakeReceipt
  | AccountType.UnstakeReceipt
  | AccountType.InternalTxReceipt

  /** For debug tx */
  balance: string
  amountSpent: string
  contractInfo: ERC20ContractDetail
  nonce: string
  readableReceipt: ReadableReceipt
  receipt: TxReceipt
  tokenTx: TokenTx
  txFrom: string
  txId: string
}

export interface NodeAccount extends BaseAccount {
  id: string
  balance: number
  nodeRewardTime: number
  hash: string
  timestamp: number
}

/** Unrelated to `WrappedEVMAccount`,  */
export interface WrappedAccount {
  accountId: string
  data: WrappedDataReceipt & WrappedEVMAccount
  timestamp: number
}

export interface NetworkAccount extends BaseAccount {
  id: string
  current: NetworkParameters
  next: NetworkParameters | object
  hash: string
  timestamp: number
}
