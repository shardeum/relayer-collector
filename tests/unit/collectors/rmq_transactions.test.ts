import RMQOriginalTxsConsumer from '../../../src/collectors/rmq_original_txs'
import * as crypto from '@shardus/crypto-utils'
import {
  Channel,
  Connection,
  ConsumeMessage,
  ConsumeMessageFields,
  MessageProperties,
  connect,
} from 'amqplib'
import { OriginalTxDataLogWriter } from '../../../src/class/DataLogWriter'
import { config } from '../../../src/config'
import { processOriginalTxData } from '../../../src/storage/originalTxData'

// jest.mock('../../../src/class/validateData')
jest.mock('amqplib')
jest.mock('../../../src/class/DataLogWriter', () => ({
  OriginalTxDataLogWriter: {
    writeToLog: jest.fn(),
  },
}))

jest.mock('@shardus/crypto-utils', () => ({
  verifyObj: jest.fn(),
}))

jest.mock('../../../src/storage/originalTxData', () => ({
  processOriginalTxData: jest.fn(),
}))

describe('RMQTransactionsConsumer', () => {
  const queueName = 'testQueue'
  let transactionsConsumer: RMQOriginalTxsConsumer
  let mockChannel: jest.Mocked<Channel>
  let mockConnection: jest.Mocked<Connection>

  beforeEach(() => {
    process.env.RMQ_ORIGINAL_TXS_QUEUE_NAME = queueName
    transactionsConsumer = new RMQOriginalTxsConsumer()

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

  it('[originalTxs consumer] should throw an error if queue name is not provided', () => {
    delete process.env.RMQ_ORIGINAL_TXS_QUEUE_NAME
    expect(() => new RMQOriginalTxsConsumer()).toThrow(
      '[RMQOriginalTxsConsumer]: please provide queue name for consumer'
    )
  })

  it('[originalTxs consumer] should fail invalid object', async () => {
    const msgStr = '{"key": "value"}'
    ;(OriginalTxDataLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await transactionsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const OriginalTxDataLogWriterSpy = jest.spyOn(OriginalTxDataLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(OriginalTxDataLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[originalTxs consumer] should fail type validation', async () => {
    const msgStr =
      '{"originalTx":{"cycle":36,"originalTxData":{"timestampReceipt":{"cycleCounter":34,"cycleMarker":"9416e4c46564e702c4bd4ca94d2589d17b27888dd24ebd19cd0534eb511bf7f3","sign":{"owner":"30af7d3a20c4acb519279b39949393e8baec8da8504dde4c2f679847b0c563fe","sig":"48e1443de5c1d9fd11bdca55a931282eeae6640b1acf1d81ca94bcb0e37510cff1810270a0736f111e5bd184a5ceb962da4c74dc98d7e8bb947a6494a48de80d16053145c7fecfd33113e1e527236ab195129c0be2b50b7d16776a0038e763fc"},"timestamp":1723501204322,"txId":"463e530012229a265ae26f5f2f42daa12d92b15d1f5bd0cd07a124af5dee6be6"},"tx":{"raw":"0xf86f80853f84fc75168252089478ce67671c4052001babbfeb9fbeaa6475ead1f38916fc8af869f9c9000080823f48a00febfee5dd8743c68f94f70d0ff3557e749ee6f77baa3b50ba32e67332901731a0741d838bc912346c22a2bbc4f889ffc1d659c3dbaddff2679e83e085835a43e8","timestamp":1723501202689}},"timestamp":1723501204322,"txId":"463e530012229a265ae26f5f2f42daa12d92b15d1f5bd0cd07a124af5dee6be6"}}'
    ;(OriginalTxDataLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await transactionsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const OriginalTxDataLogWriterSpy = jest.spyOn(OriginalTxDataLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(OriginalTxDataLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[originalTxs consumer] should fail data owner validation', async () => {
    const msgStr =
      '{"originalTx":{"cycle":36,"originalTxData":{"timestampReceipt":{"cycleCounter":34,"cycleMarker":"9416e4c46564e702c4bd4ca94d2589d17b27888dd24ebd19cd0534eb511bf7f3","sign":{"owner":"30af7d3a20c4acb519279b39949393e8baec8da8504dde4c2f679847b0c563fe","sig":"48e1443de5c1d9fd11bdca55a931282eeae6640b1acf1d81ca94bcb0e37510cff1810270a0736f111e5bd184a5ceb962da4c74dc98d7e8bb947a6494a48de80d16053145c7fecfd33113e1e527236ab195129c0be2b50b7d16776a0038e763fc"},"timestamp":1723501204322,"txId":"463e530012229a265ae26f5f2f42daa12d92b15d1f5bd0cd07a124af5dee6be6"},"tx":{"raw":"0xf86f80853f84fc75168252089478ce67671c4052001babbfeb9fbeaa6475ead1f38916fc8af869f9c9000080823f48a00febfee5dd8743c68f94f70d0ff3557e749ee6f77baa3b50ba32e67332901731a0741d838bc912346c22a2bbc4f889ffc1d659c3dbaddff2679e83e085835a43e8","timestamp":1723501202689}},"timestamp":1723501204322,"txId":"463e530012229a265ae26f5f2f42daa12d92b15d1f5bd0cd07a124af5dee6be6"},"sign":{"owner":"758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de4","sig":"f32c2bc280104048ba9150bbdcd84426c2ced5ea27caadc5f07576eb34d278895ad7b38e7023ecb953f25048fa0820ad0abb8316844ae0d7985af7148716f900fe9764caec19cab9c98e3db925e4292c59d889b2e022433a928f2fc4ae94b9f5"}}'
    ;(OriginalTxDataLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await transactionsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const OriginalTxDataLogWriterSpy = jest.spyOn(OriginalTxDataLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(OriginalTxDataLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[originalTxs consumer] should fail data required field validation', async () => {
    const msgStr =
      '{"originalTx1":{"cycle":36,"originalTxData":{"timestampReceipt":{"cycleCounter":34,"cycleMarker":"9416e4c46564e702c4bd4ca94d2589d17b27888dd24ebd19cd0534eb511bf7f3","sign":{"owner":"30af7d3a20c4acb519279b39949393e8baec8da8504dde4c2f679847b0c563fe","sig":"48e1443de5c1d9fd11bdca55a931282eeae6640b1acf1d81ca94bcb0e37510cff1810270a0736f111e5bd184a5ceb962da4c74dc98d7e8bb947a6494a48de80d16053145c7fecfd33113e1e527236ab195129c0be2b50b7d16776a0038e763fc"},"timestamp":1723501204322,"txId":"463e530012229a265ae26f5f2f42daa12d92b15d1f5bd0cd07a124af5dee6be6"},"tx":{"raw":"0xf86f80853f84fc75168252089478ce67671c4052001babbfeb9fbeaa6475ead1f38916fc8af869f9c9000080823f48a00febfee5dd8743c68f94f70d0ff3557e749ee6f77baa3b50ba32e67332901731a0741d838bc912346c22a2bbc4f889ffc1d659c3dbaddff2679e83e085835a43e8","timestamp":1723501202689}},"timestamp":1723501204322,"txId":"463e530012229a265ae26f5f2f42daa12d92b15d1f5bd0cd07a124af5dee6be6"},"sign":{"owner":"758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3","sig":"f32c2bc280104048ba9150bbdcd84426c2ced5ea27caadc5f07576eb34d278895ad7b38e7023ecb953f25048fa0820ad0abb8316844ae0d7985af7148716f900fe9764caec19cab9c98e3db925e4292c59d889b2e022433a928f2fc4ae94b9f5"}}'
    ;(OriginalTxDataLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await transactionsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const OriginalTxDataLogWriterSpy = jest.spyOn(OriginalTxDataLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(OriginalTxDataLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[originalTxs consumer] should successfully process message', async () => {
    const msgStr =
      '{"originalTx":{"cycle":36,"originalTxData":{"timestampReceipt":{"cycleCounter":34,"cycleMarker":"9416e4c46564e702c4bd4ca94d2589d17b27888dd24ebd19cd0534eb511bf7f3","sign":{"owner":"30af7d3a20c4acb519279b39949393e8baec8da8504dde4c2f679847b0c563fe","sig":"48e1443de5c1d9fd11bdca55a931282eeae6640b1acf1d81ca94bcb0e37510cff1810270a0736f111e5bd184a5ceb962da4c74dc98d7e8bb947a6494a48de80d16053145c7fecfd33113e1e527236ab195129c0be2b50b7d16776a0038e763fc"},"timestamp":1723501204322,"txId":"463e530012229a265ae26f5f2f42daa12d92b15d1f5bd0cd07a124af5dee6be6"},"tx":{"raw":"0xf86f80853f84fc75168252089478ce67671c4052001babbfeb9fbeaa6475ead1f38916fc8af869f9c9000080823f48a00febfee5dd8743c68f94f70d0ff3557e749ee6f77baa3b50ba32e67332901731a0741d838bc912346c22a2bbc4f889ffc1d659c3dbaddff2679e83e085835a43e8","timestamp":1723501202689}},"timestamp":1723501204322,"txId":"463e530012229a265ae26f5f2f42daa12d92b15d1f5bd0cd07a124af5dee6be6"},"sign":{"owner":"758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3","sig":"f32c2bc280104048ba9150bbdcd84426c2ced5ea27caadc5f07576eb34d278895ad7b38e7023ecb953f25048fa0820ad0abb8316844ae0d7985af7148716f900fe9764caec19cab9c98e3db925e4292c59d889b2e022433a928f2fc4ae94b9f5"}}'
    ;(OriginalTxDataLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    ;(processOriginalTxData as jest.Mock).mockResolvedValue(undefined)

    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await transactionsConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const OriginalTxDataLogWriterSpy = jest.spyOn(OriginalTxDataLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(OriginalTxDataLogWriterSpy).toHaveBeenCalledTimes(1)
  })
})
