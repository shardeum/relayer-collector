import WebSocket from 'ws'
import { config } from '../config'
import * as db from './sqlite3storage'

export const isShardeumIndexerEnabled = (): boolean => {
  return config.enableShardeumIndexer
}

export const isBlockIndexingEnabled = (): boolean => {
  return config.blockIndexing.enabled
}

export const initializeDB = async (): Promise<void> => {
  await db.init({
    defaultDbSqlitePath: config.dbPath,
    enableShardeumIndexer: config.enableShardeumIndexer,
    shardeumIndexerSqlitePath: config.shardeumIndexerSqlitePath,
  })
  await db.runCreate(
    'CREATE TABLE if not exists `cycles` (`cycleMarker` TEXT NOT NULL UNIQUE PRIMARY KEY, `counter` NUMBER NOT NULL, `cycleRecord` JSON NOT NULL)'
  )
  // await db.runCreate('Drop INDEX if exists `cycles_idx`');
  await db.runCreate('CREATE INDEX if not exists `cycles_idx` ON `cycles` (`counter` DESC)')
  await db.runCreate(
    'CREATE TABLE if not exists `accounts` (`accountId` TEXT NOT NULL UNIQUE PRIMARY KEY, `cycle` NUMBER NOT NULL, `timestamp` BIGINT NOT NULL, `ethAddress` TEXT NOT NULL, `account` JSON NOT NULL, `accountType` INTEGER NOT NULL, `hash` TEXT NOT NULL, `isGlobal` BOOLEAN NOT NULL, `contractInfo` JSON, `contractType` INTEGER)'
  )
  if (isShardeumIndexerEnabled()) {
    console.log('ShardeumIndexer: Enabled, creating tables and indexes for ShardeumIndexer')
    await db.runCreate(
      'CREATE TABLE if not exists `accountsEntry` (`accountId` TEXT NOT NULL UNIQUE PRIMARY KEY, `timestamp` BIGINT NOT NULL, `data` TEXT NOT NULL)',
      'shardeumIndexer'
    )
  }

  if (isBlockIndexingEnabled()) {
    console.log('BlockIndexing: Enabled, creating tables and indexes for BlockIndexing')
    await db.runCreate(
      'CREATE TABLE if not exists `blocks` (`number` NUMBER NOT NULL UNIQUE PRIMARY KEY, `numberHex` TEXT NOT NULL, `hash` TEXT NOT NULL, `timestamp` BIGINT NOT NULL, `cycle` NUMBER NOT NULL, `readableBlock` JSON NOT NULL)'
    )
    await db.runCreate(`CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks (hash)`)
    await db.runCreate(`CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks (timestamp)`)
  }

  // await db.runCreate('Drop INDEX if exists `accounts_idx`');
  await db.runCreate(
    'CREATE INDEX if not exists `accounts_idx` ON `accounts` (`cycle` DESC, `timestamp` DESC, `accountType` ASC, `ethAddress`, `contractInfo`, `contractType` ASC)'
  )
  await db.runCreate(
    'CREATE TABLE if not exists `transactions` (`txId` TEXT NOT NULL, `cycle` NUMBER NOT NULL, `timestamp` BIGINT NOT NULL, `blockNumber` NUMBER NOT NULL, `blockHash` TEXT NOT NULL, `wrappedEVMAccount` JSON NOT NULL, `txFrom` TEXT NOT NULL, `txTo` TEXT NOT NULL, `nominee` TEXT, `txHash` TEXT NOT NULL, `transactionType` INTEGER NOT NULL, `originalTxData` JSON, PRIMARY KEY (`txId`, `txHash`))'
  )
  // await db.runCreate('Drop INDEX if exists `transactions_hash_id`');
  await db.runCreate('CREATE INDEX if not exists `transactions_hash_id` ON `transactions` (`txHash`, `txId`)')
  await db.runCreate('CREATE INDEX if not exists `transactions_txType` ON `transactions` (`transactionType`)')
  await db.runCreate('CREATE INDEX if not exists `transactions_txFrom` ON `transactions` (`txFrom`)')
  await db.runCreate('CREATE INDEX if not exists `transactions_txTo` ON `transactions` (`txTo`)')
  await db.runCreate('CREATE INDEX if not exists `transactions_nominee` ON `transactions` (`nominee`)')
  await db.runCreate(
    'CREATE INDEX if not exists `transactions_cycle_timestamp` ON `transactions` (`cycle` DESC, `timestamp` DESC)'
  )
  await db.runCreate('CREATE INDEX if not exists `transactions_blockHash` ON `transactions` (`blockHash`)')
  await db.runCreate(
    'CREATE INDEX if not exists `transactions_blockNumber` ON `transactions` (`blockNumber`)'
  )
  await db.runCreate(
    'CREATE TABLE if not exists `tokenTxs` (`txId` TEXT NOT NULL, `txHash` TEXT NOT NULL, `cycle` NUMBER NOT NULL, `timestamp` BIGINT NOT NULL, `contractAddress` TEXT NOT NULL, `contractInfo` JSON, `tokenFrom` TEXT NOT NULL, `tokenTo` TEXT NOT NULL, `tokenValue` TEXT NOT NULL, `tokenType` INTEGER NOT NULL, `tokenEvent` TEXT NOT NULL, `tokenOperator` TEXT, `transactionFee` TEXT NOT NULL, FOREIGN KEY (`txId`, `txHash`) REFERENCES transactions(`txId`, `txHash`))'
  )
  // await db.runCreate('Drop INDEX if exists `tokenTxs_idx`');
  await db.runCreate(
    'CREATE INDEX if not exists `tokenTxs_idx` ON `tokenTxs` (`cycle` DESC, `timestamp` DESC, `txId`, `txHash`, `contractAddress`, `tokenFrom`, `tokenTo`, `tokenType`, `tokenOperator`)'
  )
  await db.runCreate(
    'CREATE TABLE if not exists `tokens` (`ethAddress` TEXT NOT NULL, `contractAddress` TEXT NOT NULL, `tokenType` INTEGER NOT NULL, `tokenValue` TEXT NOT NULL, PRIMARY KEY (`ethAddress`, `contractAddress`))'
  )
  // await db.runCreate('Drop INDEX if exists `tokens_idx`');
  await db.runCreate(
    'CREATE INDEX if not exists `tokens_idx` ON `tokens` (`ethAddress`, `contractAddress`, `tokenType`, `tokenValue` DESC)'
  )
  await db.runCreate(
    'CREATE TABLE if not exists `logs` (`_id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `txHash` TEXT NOT NULL, `cycle` NUMBER NOT NULL, `timestamp` BIGINT NOT NULL, `blockNumber` NUMBER NOT NULL, `blockHash` TEXT NOT NULL, `contractAddress` TEXT NOT NULL,' +
      ' `log` JSON NOT NULL, `topic0` TEXT NOT NULL, `topic1` TEXT, `topic2` TEXT, `topic3` TEXT)'
  )
  await db.runCreate(
    'CREATE INDEX IF NOT EXISTS `logs_cycle_timestamp` ON `logs` (`cycle` DESC, `timestamp` DESC)'
  )
  await db.runCreate('CREATE INDEX IF NOT EXISTS `logs_contractAddress` ON `logs` (`contractAddress`)')
  await db.runCreate('CREATE INDEX IF NOT EXISTS `logs_blockHash` ON `logs` (`blockHash`)')
  await db.runCreate('CREATE INDEX IF NOT EXISTS `logs_blockNumber` ON `logs` (`blockNumber` DESC)')
  await db.runCreate(
    'CREATE INDEX IF NOT EXISTS `logs_topic` ON `logs` (`topic0`, `topic1`, `topic2`, `topic3`)'
  )

  await db.runCreate(
    'CREATE TABLE if not exists `receipts` (`receiptId` TEXT NOT NULL UNIQUE PRIMARY KEY, `tx` JSON NOT NULL, `cycle` NUMBER NOT NULL, `applyTimestamp` BIGINT NOT NULL, `timestamp` BIGINT NOT NULL, `signedReceipt` JSON NOT NULL, `afterStates` JSON, `beforeStates` JSON, `appReceiptData` JSON, `executionShardKey` TEXT NOT NULL, `globalModification` BOOLEAN NOT NULL)'
  )
  // await db.runCreate('Drop INDEX if exists `receipts_idx`');
  await db.runCreate('CREATE INDEX if not exists `receipts_idx` ON `receipts` (`cycle` ASC, `timestamp` ASC)')
  // Main originalTxData
  await db.runCreate(
    'CREATE TABLE if not exists `originalTxsData` (`txId` TEXT NOT NULL, `timestamp` BIGINT NOT NULL, `cycle` NUMBER NOT NULL, `originalTxData` JSON NOT NULL, PRIMARY KEY (`txId`, `timestamp`))'
  )
  // await db.runCreate('Drop INDEX if exists `originalTxData_idx`');
  await db.runCreate(
    'CREATE INDEX if not exists `originalTxsData_idx` ON `originalTxsData` (`cycle` ASC, `timestamp` ASC, `txId`)'
  )
  // Mapped OriginalTxData with txHash and transactionType
  await db.runCreate(
    'CREATE TABLE if not exists `originalTxsData2` (`txId` TEXT NOT NULL, `txHash` TEXT NOT NULL, `timestamp` BIGINT NOT NULL, `cycle` NUMBER NOT NULL,  `transactionType` INTEGER NOT NULL, PRIMARY KEY (`txId`, `timestamp`))'
  )
  // await db.runCreate('Drop INDEX if exists `originalTxData2_idx`');
  await db.runCreate(
    'CREATE INDEX if not exists `originalTxsData2_idx` ON `originalTxsData2` (`txHash`, `txId`, `cycle` DESC, `timestamp` DESC, `transactionType`)'
  )
  await db.runCreate(
    'CREATE TABLE if not exists `accountHistoryState` (`accountId` TEXT NOT NULL, `beforeStateHash` TEXT NOT NULL, `afterStateHash` TEXT NOT NULL, `blockNumber` NUMBER NOT NULL, `blockHash` TEXT NOT NULL, `timestamp` BIGINT NOT NULL, `receiptId` TEXT NOT NULL, PRIMARY KEY (`accountId`, `timestamp`))'
  )
  await db.runCreate(
    'CREATE INDEX if not exists `accountHistoryState_idx` ON `accountHistoryState` (`accountId`, `blockHash`, `blockNumber` DESC, `timestamp` DESC)'
  )
}

export const closeDatabase = async (): Promise<void> => {
  await db.close()
}

export const addExitListeners = (ws?: WebSocket) => {
  process.on('SIGINT', async () => {
    console.log('Exiting on SIGINT')
    if (ws) ws.close()
    await closeDatabase()
    process.exit(0)
  })
  process.on('SIGTERM', async () => {
    console.log('Exiting on SIGTERM')
    if (ws) ws.close()
    await closeDatabase()
    process.exit(0)
  })
}
