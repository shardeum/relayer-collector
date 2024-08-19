import RMQCyclesConsumer from '../../../src/collectors/rmq_cycles'
import * as crypto from '@shardus/crypto-utils'
import {
  Channel,
  Connection,
  ConsumeMessage,
  ConsumeMessageFields,
  MessageProperties,
  connect,
} from 'amqplib'
import { CycleLogWriter } from '../../../src/class/DataLogWriter'
import { config } from '../../../src/config'
import { insertOrUpdateCycle } from '../../../src/storage/cycle'
import { upsertBlocksForCycleCore } from '../../../src/storage/block'

// jest.mock('../../../src/class/validateData')
jest.mock('amqplib')
jest.mock('../../../src/class/DataLogWriter', () => ({
  CycleLogWriter: {
    writeToLog: jest.fn(),
  },
}))

jest.mock('@shardus/crypto-utils', () => ({
  verifyObj: jest.fn(),
}))

jest.mock('../../../src/storage/cycle', () => ({
  insertOrUpdateCycle: jest.fn(),
}))

jest.mock('../../../src/storage/block', () => ({
  upsertBlocksForCycleCore: jest.fn(),
}))

describe('RMQCyclesConsumer', () => {
  const queueName = 'testQueue'
  let cyclesConsumer: RMQCyclesConsumer
  let mockChannel: jest.Mocked<Channel>
  let mockConnection: jest.Mocked<Connection>

  beforeEach(() => {
    process.env.RMQ_CYCLES_QUEUE_NAME = queueName
    cyclesConsumer = new RMQCyclesConsumer()

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

  it('[cycle consumer] should throw an error if queue name is not provided', () => {
    delete process.env.RMQ_CYCLES_QUEUE_NAME
    expect(() => new RMQCyclesConsumer()).toThrow(
      '[RMQCyclesConsumer]: please provide queue name for consumer'
    )
  })

  it('[cycle consumer] should fail invalid object', async () => {
    const msgStr = '{"key": "value"}'
    ;(CycleLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    ;(insertOrUpdateCycle as jest.Mock).mockResolvedValue(undefined)
    ;(upsertBlocksForCycleCore as jest.Mock).mockResolvedValue(undefined)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await cyclesConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const CycleLogWriterSpy = jest.spyOn(CycleLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(CycleLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[cycle consumer] should fail type validation', async () => {
    const msgStr =
      '{"cycle":{"counter":2074,"cycleMarker":"f658eb98868a365d7842f22b42e65c8653b9adfe426109633c838dfaddcef9a5","cycleRecord":{"activated":[],"activatedPublicKeys":[],"active":60,"apoptosized":[],"appRemoved":[],"archiverListHash":"033550061aebdbac90dd4f93bc5ca29e3607fb42365c47e196f7217ebe79f799","counter":2074,"desired":72,"duration":60,"expired":1,"finishedSyncing":[],"joined":[],"joinedArchivers":[],"joinedConsensors":[],"leavingArchivers":[],"lost":[],"lostAfterSelection":[],"lostArchivers":[],"lostSyncing":[],"marker":"f658eb98868a365d7842f22b42e65c8653b9adfe426109633c838dfaddcef9a5","maxSyncTime":1200,"mode":"processing","networkConfigHash":"fe27e1957570368bb85d6ca34d5ed6da2c403f25bd6487854a344b44f3b491f2","networkId":"3ed18546ac12348331264f51a92334c0f893af803042fe78cb455c77b6a58536","nodeListHash":"5ab1d2cfd65f9b9a7547adb8d58062f284d32ea46eb324c1684275d90c2bc2d0","previous":"038f0514aed48994efb4d6b2aef6a4c24f718b0a148c6db74e802f60d5be66ad","random":0,"refreshedArchivers":[],"refreshedConsensors":[],"refuted":[],"refutedArchivers":[],"removed":[],"removedArchivers":[],"returned":[],"standby":0,"standbyAdd":[],"standbyNodeListHash":"78c7d4bfca718a92b57a31832c1c8460f43dee960b5f4cf4bbdae3bcce2deb6d","standbyRefresh":[],"standbyRemove":[],"start":1723623490,"startedSyncing":[],"syncing":0,"target":71.99999999999994}},"sign":{"sig":"47cd84baca578611f29a9d2cfb713f825daeb3a14128966c74436658bcf8f8247a95626c849ef2c275aedf1a6a4773222f6c17e1acd128a24172f9760e6e1201de5a115d7b5365499d5c55a19756e8d9f9392699d2888ac57ca4bbaaf5968e27"}}'
    ;(CycleLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    ;(insertOrUpdateCycle as jest.Mock).mockResolvedValue(undefined)
    ;(upsertBlocksForCycleCore as jest.Mock).mockResolvedValue(undefined)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await cyclesConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const CycleLogWriterSpy = jest.spyOn(CycleLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(CycleLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[cycle consumer] should fail data owner validation', async () => {
    const msgStr =
      '{"cycle":{"counter":2074,"cycleMarker":"f658eb98868a365d7842f22b42e65c8653b9adfe426109633c838dfaddcef9a5","cycleRecord":{"activated":[],"activatedPublicKeys":[],"active":60,"apoptosized":[],"appRemoved":[],"archiverListHash":"033550061aebdbac90dd4f93bc5ca29e3607fb42365c47e196f7217ebe79f799","counter":2074,"desired":72,"duration":60,"expired":1,"finishedSyncing":[],"joined":[],"joinedArchivers":[],"joinedConsensors":[],"leavingArchivers":[],"lost":[],"lostAfterSelection":[],"lostArchivers":[],"lostSyncing":[],"marker":"f658eb98868a365d7842f22b42e65c8653b9adfe426109633c838dfaddcef9a5","maxSyncTime":1200,"mode":"processing","networkConfigHash":"fe27e1957570368bb85d6ca34d5ed6da2c403f25bd6487854a344b44f3b491f2","networkId":"3ed18546ac12348331264f51a92334c0f893af803042fe78cb455c77b6a58536","nodeListHash":"5ab1d2cfd65f9b9a7547adb8d58062f284d32ea46eb324c1684275d90c2bc2d0","previous":"038f0514aed48994efb4d6b2aef6a4c24f718b0a148c6db74e802f60d5be66ad","random":0,"refreshedArchivers":[],"refreshedConsensors":[],"refuted":[],"refutedArchivers":[],"removed":[],"removedArchivers":[],"returned":[],"standby":0,"standbyAdd":[],"standbyNodeListHash":"78c7d4bfca718a92b57a31832c1c8460f43dee960b5f4cf4bbdae3bcce2deb6d","standbyRefresh":[],"standbyRemove":[],"start":1723623490,"startedSyncing":[],"syncing":0,"target":71.99999999999994}},"sign":{"owner":"758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de4","sig":"47cd84baca578611f29a9d2cfb713f825daeb3a14128966c74436658bcf8f8247a95626c849ef2c275aedf1a6a4773222f6c17e1acd128a24172f9760e6e1201de5a115d7b5365499d5c55a19756e8d9f9392699d2888ac57ca4bbaaf5968e27"}}'
    ;(CycleLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    ;(insertOrUpdateCycle as jest.Mock).mockResolvedValue(undefined)
    ;(upsertBlocksForCycleCore as jest.Mock).mockResolvedValue(undefined)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await cyclesConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const CycleLogWriterSpy = jest.spyOn(CycleLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(CycleLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[cycle consumer] should fail data required field validation', async () => {
    const msgStr =
      '{"cycle1":{"counter":2074,"cycleMarker":"f658eb98868a365d7842f22b42e65c8653b9adfe426109633c838dfaddcef9a5","cycleRecord":{"activated":[],"activatedPublicKeys":[],"active":60,"apoptosized":[],"appRemoved":[],"archiverListHash":"033550061aebdbac90dd4f93bc5ca29e3607fb42365c47e196f7217ebe79f799","counter":2074,"desired":72,"duration":60,"expired":1,"finishedSyncing":[],"joined":[],"joinedArchivers":[],"joinedConsensors":[],"leavingArchivers":[],"lost":[],"lostAfterSelection":[],"lostArchivers":[],"lostSyncing":[],"marker":"f658eb98868a365d7842f22b42e65c8653b9adfe426109633c838dfaddcef9a5","maxSyncTime":1200,"mode":"processing","networkConfigHash":"fe27e1957570368bb85d6ca34d5ed6da2c403f25bd6487854a344b44f3b491f2","networkId":"3ed18546ac12348331264f51a92334c0f893af803042fe78cb455c77b6a58536","nodeListHash":"5ab1d2cfd65f9b9a7547adb8d58062f284d32ea46eb324c1684275d90c2bc2d0","previous":"038f0514aed48994efb4d6b2aef6a4c24f718b0a148c6db74e802f60d5be66ad","random":0,"refreshedArchivers":[],"refreshedConsensors":[],"refuted":[],"refutedArchivers":[],"removed":[],"removedArchivers":[],"returned":[],"standby":0,"standbyAdd":[],"standbyNodeListHash":"78c7d4bfca718a92b57a31832c1c8460f43dee960b5f4cf4bbdae3bcce2deb6d","standbyRefresh":[],"standbyRemove":[],"start":1723623490,"startedSyncing":[],"syncing":0,"target":71.99999999999994}},"sign":{"owner":"758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3","sig":"47cd84baca578611f29a9d2cfb713f825daeb3a14128966c74436658bcf8f8247a95626c849ef2c275aedf1a6a4773222f6c17e1acd128a24172f9760e6e1201de5a115d7b5365499d5c55a19756e8d9f9392699d2888ac57ca4bbaaf5968e27"}}'
    ;(CycleLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    ;(insertOrUpdateCycle as jest.Mock).mockResolvedValue(undefined)
    ;(upsertBlocksForCycleCore as jest.Mock).mockResolvedValue(undefined)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await cyclesConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const CycleLogWriterSpy = jest.spyOn(CycleLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(CycleLogWriterSpy).toHaveBeenCalledTimes(0)
  })

  it('[cycle consumer] should successfully process message', async () => {
    const msgStr =
      '{"cycle":{"counter":2074,"cycleMarker":"f658eb98868a365d7842f22b42e65c8653b9adfe426109633c838dfaddcef9a5","cycleRecord":{"activated":[],"activatedPublicKeys":[],"active":60,"apoptosized":[],"appRemoved":[],"archiverListHash":"033550061aebdbac90dd4f93bc5ca29e3607fb42365c47e196f7217ebe79f799","counter":2074,"desired":72,"duration":60,"expired":1,"finishedSyncing":[],"joined":[],"joinedArchivers":[],"joinedConsensors":[],"leavingArchivers":[],"lost":[],"lostAfterSelection":[],"lostArchivers":[],"lostSyncing":[],"marker":"f658eb98868a365d7842f22b42e65c8653b9adfe426109633c838dfaddcef9a5","maxSyncTime":1200,"mode":"processing","networkConfigHash":"fe27e1957570368bb85d6ca34d5ed6da2c403f25bd6487854a344b44f3b491f2","networkId":"3ed18546ac12348331264f51a92334c0f893af803042fe78cb455c77b6a58536","nodeListHash":"5ab1d2cfd65f9b9a7547adb8d58062f284d32ea46eb324c1684275d90c2bc2d0","previous":"038f0514aed48994efb4d6b2aef6a4c24f718b0a148c6db74e802f60d5be66ad","random":0,"refreshedArchivers":[],"refreshedConsensors":[],"refuted":[],"refutedArchivers":[],"removed":[],"removedArchivers":[],"returned":[],"standby":0,"standbyAdd":[],"standbyNodeListHash":"78c7d4bfca718a92b57a31832c1c8460f43dee960b5f4cf4bbdae3bcce2deb6d","standbyRefresh":[],"standbyRemove":[],"start":1723623490,"startedSyncing":[],"syncing":0,"target":71.99999999999994}},"sign":{"owner":"758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3","sig":"47cd84baca578611f29a9d2cfb713f825daeb3a14128966c74436658bcf8f8247a95626c849ef2c275aedf1a6a4773222f6c17e1acd128a24172f9760e6e1201de5a115d7b5365499d5c55a19756e8d9f9392699d2888ac57ca4bbaaf5968e27"}}'
    ;(CycleLogWriter.writeToLog as jest.Mock).mockResolvedValue(undefined)
    ;(crypto.verifyObj as jest.Mock).mockReturnValue(true)
    ;(insertOrUpdateCycle as jest.Mock).mockResolvedValue(undefined)
    ;(upsertBlocksForCycleCore as jest.Mock).mockResolvedValue(undefined)
    const mockMessage: ConsumeMessage = {
      content: Buffer.from(msgStr),
      fields: {} as ConsumeMessageFields,
      properties: {} as MessageProperties,
    }
    await cyclesConsumer.start()

    const consumeCallback = mockChannel.consume.mock.calls[0][1]
    const CycleLogWriterSpy = jest.spyOn(CycleLogWriter, 'writeToLog')

    consumeCallback(mockMessage)

    expect(mockChannel.consume).toHaveBeenCalledTimes(1)
    expect(CycleLogWriterSpy).toHaveBeenCalledTimes(1)
  })
})
