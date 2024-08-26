import * as Storage from '../src/storage'
import * as ReceiptDB from '../src/storage/receipt'
import * as AccountHistoryStateDB from '../src/storage/accountHistoryState'

const start = async (): Promise<void> => {
  await Storage.initializeDB()
  Storage.addExitListeners()

  const receiptsCount = await ReceiptDB.queryReceiptCount()
  console.log('receiptsCount', receiptsCount)
  const limit = 100
  const bucketSize = 1000
  for (let i = 0; i < receiptsCount; i += limit) {
    console.log(i, i + limit)
    const receipts = await ReceiptDB.queryReceipts(i, limit)
    let accountHistoryStateList: AccountHistoryStateDB.AccountHistoryState[] = []
    for (const receipt of receipts) {
      const { signedReceipt, appReceiptData, globalModification, receiptId } = receipt
      const blockHash = appReceiptData.data?.readableReceipt?.blockHash
      if (!blockHash) {
        console.error(`Receipt ${receiptId} has no blockHash`)
        continue
      }
      const blockNumber = parseInt(appReceiptData.data?.readableReceipt?.blockNumber)
      if (globalModification === false && signedReceipt && signedReceipt.proposal.accountIDs.length > 0) {
        for (let i = 0; i < receipt.afterStates!.length; i++) {
          const accountHistoryState: AccountHistoryStateDB.AccountHistoryState = {
            accountId: receipt.afterStates!.at(i)!.accountId,
            beforeStateHash: receipt.beforeStates!.at(i)!.hash,
            afterStateHash: receipt.afterStates!.at(i)!.hash,
            timestamp: receipt.timestamp,
            blockNumber,
            blockHash,
            receiptId,
          }
          accountHistoryStateList.push(accountHistoryState)
        }
      } else {
        if (globalModification === true) {
          console.log(`Receipt ${receiptId} has globalModification as true`)
        }
        if (globalModification === false && !signedReceipt) {
          console.error(`Receipt ${receiptId} has no signedReceipt`)
        }
      }
      if (accountHistoryStateList.length >= bucketSize) {
        await AccountHistoryStateDB.bulkInsertAccountHistoryStates(accountHistoryStateList)
        accountHistoryStateList = []
      }
    }
    if (accountHistoryStateList.length > 0) {
      await AccountHistoryStateDB.bulkInsertAccountHistoryStates(accountHistoryStateList)
      accountHistoryStateList = []
    }
  }
  const accountHistoryStateCount = await AccountHistoryStateDB.queryAccountHistoryStateCount()
  console.log('accountHistoryStateCount', accountHistoryStateCount)
  await Storage.closeDatabase()
}

start()
