import RMQReceiptsConsumer from '../../../src/collectors/rmq_receipts'
import * as crypto from '@shardus/crypto-utils'
import {
  Channel,
  Connection,
  ConsumeMessage,
  ConsumeMessageFields,
  MessageProperties,
  connect,
} from 'amqplib'
import { ReceiptLogWriter } from '../../../src/class/DataLogWriter'
import { config } from '../../../src/config'
import { processReceiptData } from '../../../src/storage/receipt'

// jest.mock('../../../src/class/validateData')
jest.mock('amqplib')
jest.mock('../../../src/class/DataLogWriter', () => ({
  ReceiptLogWriter: {
    writeToLog: jest.fn(),
  },
}))

jest.mock('@shardus/crypto-utils', () => ({
  verifyObj: jest.fn(),
}))

jest.mock('../../../src/storage/receipt', () => ({
  processReceiptData: jest.fn(),
}))

describe('RMQReceiptsConsumer', () => {
  const queueName = 'testQueue'
  let receiptsConsumer: RMQReceiptsConsumer
  let mockChannel: jest.Mocked<Channel>
  let mockConnection: jest.Mocked<Connection>

  beforeEach(() => {
    process.env.RMQ_RECEIPTS_QUEUE_NAME = queueName
    receiptsConsumer = new RMQReceiptsConsumer()

    mockChannel = {
      assertQueue: jest.fn().mockResolvedValue(undefined),
      consume: jest.fn(),
      on: jest.fn(),
      prefetch: jest.fn(),
      ack: jest.fn(),
      nack: jest.fn(),
    } as unknown as jest.Mocked<Channel>

    mockConnection = {
      createChannel: jest.fn().mockResolvedValue(mockChannel),
      on: jest.fn(),
    } as unknown as jest.Mocked<Connection>
    ;(connect as jest.Mock).mockResolvedValue(mockConnection)

    config.distributorInfo.publicKey = '758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3'
  })

  afterEach(() => {
    jest.clearAllMocks()
  })

  it('[receipts consumer] should throw an error if queue name is not provided', () => {
    delete process.env.RMQ_RECEIPTS_QUEUE_NAME
    expect(() => new RMQReceiptsConsumer()).toThrow(
      '[RMQReceiptsConsumer]: please provide queue name for consumer'
    )
  })

  it('[receipts consumer] should fail invalid object', async () => {
    const msgStr = '{"key": "value"}'
    ;(ReceiptLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await receiptsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const ReceiptLogWriterSpy = jest.spyOn(ReceiptLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(ReceiptLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[receipts consumer] should fail type validation', async () => {
    const msgStr =
      '{"receipt":{"accounts":[{"accountId":"1000000000000000000000000000000000000000000000000000000000000001","data":{"accountType":5,"current":{"activeVersion":"1.12.1","archiver":{"activeVersion":"3.4.23","latestVersion":"3.4.23","minVersion":"3.4.23"},"certCycleDuration":30,"description":"These are the initial network parameters Shardeum started with","latestVersion":"1.12.1","maintenanceFee":0,"maintenanceInterval":86400000,"minVersion":"1.12.1","nodePenaltyUsd":{"dataType":"bi","value":"8ac7230489e80000"},"nodeRewardAmountUsd":{"dataType":"bi","value":"de0b6b3a7640000"},"nodeRewardInterval":3600000,"slashing":{"enableLeftNetworkEarly":false,"enableNodeRefuted":false,"enableSyncTimeout":false},"stabilityScaleDiv":1000,"stabilityScaleMul":1000,"stakeRequiredUsd":{"dataType":"bi","value":"8ac7230489e80000"},"title":"Initial parameters","txPause":false},"hash":"07793fc4a134404607c7e79dc547876894ee2a483adb0873cca892fbce6d6aee","id":"1000000000000000000000000000000000000000000000000000000000000001","listOfChanges":[],"next":{},"timestamp":1723499245280},"hash":"07793fc4a134404607c7e79dc547876894ee2a483adb0873cca892fbce6d6aee","isGlobal":true,"timestamp":1723499245280}],"appReceiptData":{"accountId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","data":{"accountType":12,"amountSpent":"0x0","ethAddress":"0x7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","hash":"40ff7577cf58f300715fe23e9e75e331a9a8872d4a3ee6850ad069f90d13a9e8","readableReceipt":{"blockHash":"0xa682e3a7af49c54e4f5bfcaa92253ac27f74cc3fbf8927b3801c1dff6e5cc17e","blockNumber":"0x21","contractAddress":null,"cumulativeGasUsed":"0x0","data":"0x0","from":"1000000000000000000000000000000000000000000000000000000000000001","gasRefund":"0x0","gasUsed":"0x0","internalTx":{"internalTXType":1,"isInternalTx":true,"network":"1000000000000000000000000000000000000000000000000000000000000001","sign":null,"timestamp":1723499245280},"isInternalTx":true,"logs":[],"logsBloom":"","nonce":"0x0","status":1,"to":"1000000000000000000000000000000000000000000000000000000000000001","transactionHash":"0x7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","transactionIndex":"0x1","value":"0x0"},"receipt":null,"timestamp":1723499245280,"txFrom":"1000000000000000000000000000000000000000000000000000000000000001","txId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1"},"stateId":"40ff7577cf58f300715fe23e9e75e331a9a8872d4a3ee6850ad069f90d13a9e8","timestamp":1723499245280},"appliedReceipt":{},"beforeStateAccounts":[],"cycle":2,"executionShardKey":"","globalModification":true,"receiptId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","timestamp":1723499245280,"tx":{"originalTxData":{"tx":{"internalTXType":1,"isInternalTx":true,"network":"1000000000000000000000000000000000000000000000000000000000000001","timestamp":1723499245280}},"timestamp":1723499245280,"txId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1"}}}'
    ;(ReceiptLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await receiptsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const ReceiptLogWriterSpy = jest.spyOn(ReceiptLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(ReceiptLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[receipts consumer] should fail data owner validation', async () => {
    const msgStr =
      '{"receipt":{"accounts":[{"accountId":"1000000000000000000000000000000000000000000000000000000000000001","data":{"accountType":5,"current":{"activeVersion":"1.12.1","archiver":{"activeVersion":"3.4.23","latestVersion":"3.4.23","minVersion":"3.4.23"},"certCycleDuration":30,"description":"These are the initial network parameters Shardeum started with","latestVersion":"1.12.1","maintenanceFee":0,"maintenanceInterval":86400000,"minVersion":"1.12.1","nodePenaltyUsd":{"dataType":"bi","value":"8ac7230489e80000"},"nodeRewardAmountUsd":{"dataType":"bi","value":"de0b6b3a7640000"},"nodeRewardInterval":3600000,"slashing":{"enableLeftNetworkEarly":false,"enableNodeRefuted":false,"enableSyncTimeout":false},"stabilityScaleDiv":1000,"stabilityScaleMul":1000,"stakeRequiredUsd":{"dataType":"bi","value":"8ac7230489e80000"},"title":"Initial parameters","txPause":false},"hash":"07793fc4a134404607c7e79dc547876894ee2a483adb0873cca892fbce6d6aee","id":"1000000000000000000000000000000000000000000000000000000000000001","listOfChanges":[],"next":{},"timestamp":1723499245280},"hash":"07793fc4a134404607c7e79dc547876894ee2a483adb0873cca892fbce6d6aee","isGlobal":true,"timestamp":1723499245280}],"appReceiptData":{"accountId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","data":{"accountType":12,"amountSpent":"0x0","ethAddress":"0x7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","hash":"40ff7577cf58f300715fe23e9e75e331a9a8872d4a3ee6850ad069f90d13a9e8","readableReceipt":{"blockHash":"0xa682e3a7af49c54e4f5bfcaa92253ac27f74cc3fbf8927b3801c1dff6e5cc17e","blockNumber":"0x21","contractAddress":null,"cumulativeGasUsed":"0x0","data":"0x0","from":"1000000000000000000000000000000000000000000000000000000000000001","gasRefund":"0x0","gasUsed":"0x0","internalTx":{"internalTXType":1,"isInternalTx":true,"network":"1000000000000000000000000000000000000000000000000000000000000001","sign":null,"timestamp":1723499245280},"isInternalTx":true,"logs":[],"logsBloom":"","nonce":"0x0","status":1,"to":"1000000000000000000000000000000000000000000000000000000000000001","transactionHash":"0x7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","transactionIndex":"0x1","value":"0x0"},"receipt":null,"timestamp":1723499245280,"txFrom":"1000000000000000000000000000000000000000000000000000000000000001","txId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1"},"stateId":"40ff7577cf58f300715fe23e9e75e331a9a8872d4a3ee6850ad069f90d13a9e8","timestamp":1723499245280},"appliedReceipt":{},"beforeStateAccounts":[],"cycle":2,"executionShardKey":"","globalModification":true,"receiptId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","timestamp":1723499245280,"tx":{"originalTxData":{"tx":{"internalTXType":1,"isInternalTx":true,"network":"1000000000000000000000000000000000000000000000000000000000000001","timestamp":1723499245280}},"timestamp":1723499245280,"txId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1"}},"sign":{"sig":"00ca2202714fa5381643631b5c517386d4a1c2f6bf5476442cc592be302e2d27164a800de07ce04199f9b3802bbaf8a27f8e2fca21c1efa2a130e4a219a1ba04276bb9eee4346764a195156f72860816f1f526ea1c8badd6d889d76989fe63ad"}}'
    ;(ReceiptLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await receiptsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const ReceiptLogWriterSpy = jest.spyOn(ReceiptLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(ReceiptLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[receipts consumer] should fail data required field validation', async () => {
    const msgStr =
      '{"receipt1":{"accounts":[{"accountId":"1000000000000000000000000000000000000000000000000000000000000001","data":{"accountType":5,"current":{"activeVersion":"1.12.1","archiver":{"activeVersion":"3.4.23","latestVersion":"3.4.23","minVersion":"3.4.23"},"certCycleDuration":30,"description":"These are the initial network parameters Shardeum started with","latestVersion":"1.12.1","maintenanceFee":0,"maintenanceInterval":86400000,"minVersion":"1.12.1","nodePenaltyUsd":{"dataType":"bi","value":"8ac7230489e80000"},"nodeRewardAmountUsd":{"dataType":"bi","value":"de0b6b3a7640000"},"nodeRewardInterval":3600000,"slashing":{"enableLeftNetworkEarly":false,"enableNodeRefuted":false,"enableSyncTimeout":false},"stabilityScaleDiv":1000,"stabilityScaleMul":1000,"stakeRequiredUsd":{"dataType":"bi","value":"8ac7230489e80000"},"title":"Initial parameters","txPause":false},"hash":"07793fc4a134404607c7e79dc547876894ee2a483adb0873cca892fbce6d6aee","id":"1000000000000000000000000000000000000000000000000000000000000001","listOfChanges":[],"next":{},"timestamp":1723499245280},"hash":"07793fc4a134404607c7e79dc547876894ee2a483adb0873cca892fbce6d6aee","isGlobal":true,"timestamp":1723499245280}],"appReceiptData":{"accountId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","data":{"accountType":12,"amountSpent":"0x0","ethAddress":"0x7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","hash":"40ff7577cf58f300715fe23e9e75e331a9a8872d4a3ee6850ad069f90d13a9e8","readableReceipt":{"blockHash":"0xa682e3a7af49c54e4f5bfcaa92253ac27f74cc3fbf8927b3801c1dff6e5cc17e","blockNumber":"0x21","contractAddress":null,"cumulativeGasUsed":"0x0","data":"0x0","from":"1000000000000000000000000000000000000000000000000000000000000001","gasRefund":"0x0","gasUsed":"0x0","internalTx":{"internalTXType":1,"isInternalTx":true,"network":"1000000000000000000000000000000000000000000000000000000000000001","sign":null,"timestamp":1723499245280},"isInternalTx":true,"logs":[],"logsBloom":"","nonce":"0x0","status":1,"to":"1000000000000000000000000000000000000000000000000000000000000001","transactionHash":"0x7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","transactionIndex":"0x1","value":"0x0"},"receipt":null,"timestamp":1723499245280,"txFrom":"1000000000000000000000000000000000000000000000000000000000000001","txId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1"},"stateId":"40ff7577cf58f300715fe23e9e75e331a9a8872d4a3ee6850ad069f90d13a9e8","timestamp":1723499245280},"appliedReceipt":{},"beforeStateAccounts":[],"cycle":2,"executionShardKey":"","globalModification":true,"receiptId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","timestamp":1723499245280,"tx":{"originalTxData":{"tx":{"internalTXType":1,"isInternalTx":true,"network":"1000000000000000000000000000000000000000000000000000000000000001","timestamp":1723499245280}},"timestamp":1723499245280,"txId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1"}},"sign":{"owner":"758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3","sig":"00ca2202714fa5381643631b5c517386d4a1c2f6bf5476442cc592be302e2d27164a800de07ce04199f9b3802bbaf8a27f8e2fca21c1efa2a130e4a219a1ba04276bb9eee4346764a195156f72860816f1f526ea1c8badd6d889d76989fe63ad"}}'
    ;(ReceiptLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await receiptsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const ReceiptLogWriterSpy = jest.spyOn(ReceiptLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(ReceiptLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[receipts consumer] should successfully process message', async () => {
    const msgStr =
      '{"receipt":{"accounts":[{"accountId":"1000000000000000000000000000000000000000000000000000000000000001","data":{"accountType":5,"current":{"activeVersion":"1.12.1","archiver":{"activeVersion":"3.4.23","latestVersion":"3.4.23","minVersion":"3.4.23"},"certCycleDuration":30,"description":"These are the initial network parameters Shardeum started with","latestVersion":"1.12.1","maintenanceFee":0,"maintenanceInterval":86400000,"minVersion":"1.12.1","nodePenaltyUsd":{"dataType":"bi","value":"8ac7230489e80000"},"nodeRewardAmountUsd":{"dataType":"bi","value":"de0b6b3a7640000"},"nodeRewardInterval":3600000,"slashing":{"enableLeftNetworkEarly":false,"enableNodeRefuted":false,"enableSyncTimeout":false},"stabilityScaleDiv":1000,"stabilityScaleMul":1000,"stakeRequiredUsd":{"dataType":"bi","value":"8ac7230489e80000"},"title":"Initial parameters","txPause":false},"hash":"07793fc4a134404607c7e79dc547876894ee2a483adb0873cca892fbce6d6aee","id":"1000000000000000000000000000000000000000000000000000000000000001","listOfChanges":[],"next":{},"timestamp":1723499245280},"hash":"07793fc4a134404607c7e79dc547876894ee2a483adb0873cca892fbce6d6aee","isGlobal":true,"timestamp":1723499245280}],"appReceiptData":{"accountId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","data":{"accountType":12,"amountSpent":"0x0","ethAddress":"0x7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","hash":"40ff7577cf58f300715fe23e9e75e331a9a8872d4a3ee6850ad069f90d13a9e8","readableReceipt":{"blockHash":"0xa682e3a7af49c54e4f5bfcaa92253ac27f74cc3fbf8927b3801c1dff6e5cc17e","blockNumber":"0x21","contractAddress":null,"cumulativeGasUsed":"0x0","data":"0x0","from":"1000000000000000000000000000000000000000000000000000000000000001","gasRefund":"0x0","gasUsed":"0x0","internalTx":{"internalTXType":1,"isInternalTx":true,"network":"1000000000000000000000000000000000000000000000000000000000000001","sign":null,"timestamp":1723499245280},"isInternalTx":true,"logs":[],"logsBloom":"","nonce":"0x0","status":1,"to":"1000000000000000000000000000000000000000000000000000000000000001","transactionHash":"0x7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","transactionIndex":"0x1","value":"0x0"},"receipt":null,"timestamp":1723499245280,"txFrom":"1000000000000000000000000000000000000000000000000000000000000001","txId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1"},"stateId":"40ff7577cf58f300715fe23e9e75e331a9a8872d4a3ee6850ad069f90d13a9e8","timestamp":1723499245280},"appliedReceipt":{},"beforeStateAccounts":[],"cycle":2,"executionShardKey":"","globalModification":true,"receiptId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1","timestamp":1723499245280,"tx":{"originalTxData":{"tx":{"internalTXType":1,"isInternalTx":true,"network":"1000000000000000000000000000000000000000000000000000000000000001","timestamp":1723499245280}},"timestamp":1723499245280,"txId":"7bdd7e1ce477480ca2e3cd69c03beab4a5b55b8833123cd37f8d3fe8171fcdd1"}},"sign":{"owner":"758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3","sig":"00ca2202714fa5381643631b5c517386d4a1c2f6bf5476442cc592be302e2d27164a800de07ce04199f9b3802bbaf8a27f8e2fca21c1efa2a130e4a219a1ba04276bb9eee4346764a195156f72860816f1f526ea1c8badd6d889d76989fe63ad"}}'
    ;(ReceiptLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    ;(processReceiptData as jest.Mock).mockResolvedValue(undefined)

    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await receiptsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const ReceiptLogWriterSpy = jest.spyOn(ReceiptLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(ReceiptLogWriterSpy).toHaveBeenCalledTimes(1)
  })
})
