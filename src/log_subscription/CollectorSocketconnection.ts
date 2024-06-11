import { Server, Socket } from 'socket.io'
import { Data } from '../class/validateData'
import { config as CONFIG } from '../config'
import { Receipt } from '../types'
import { Utils as StringUtils } from '@shardus/types'

// constants
const ConnectionEvent = 'connection'
const DisconnectionEvent = 'disconnect'
const ErrorEvent = 'error'

export const CycleDataWsEvent = '/data/cycle'
export const ReceiptDataWsEvent = '/data/receipt'

const registeredLogServers = new Map<string, Socket>()

export const setupCollectorSocketServer = (): void => {
  const socketServer = new Server()

  socketServer.on(ConnectionEvent, (socket) => {
    console.log(`New LogServer registered ${socket.id}`)
    registeredLogServers.set(socket.id, socket)
    socket.on(DisconnectionEvent, () => {
      console.log(`LogServer ${socket.id} disconnected`)
      registeredLogServers.delete(socket.id)
    })
    socket.on(ErrorEvent, (err) => {
      console.log(`LogServer ${socket.id} error: ${err}. Disconnecting...`)
      registeredLogServers.delete(socket.id)
      socket.disconnect()
    })
  })

  socketServer.listen(Number(CONFIG.port.collector))
  console.log(`LogServer sender listening on port ${CONFIG.port.collector}`)
}

export const forwardCycleData = async (data: Data): Promise<void> => {
  for (const socket of registeredLogServers.values()) {
    socket.emit(CycleDataWsEvent, StringUtils.safeStringify(data))
  }
  console.log(`Forwarded cycle data to ${registeredLogServers.size} LogServers`)
}

export const forwardReceiptData = async (data: Receipt[]): Promise<void> => {
  for (const socket of registeredLogServers.values()) {
    socket.emit(ReceiptDataWsEvent, StringUtils.safeStringify(data))
  }
  /* prettier-ignore */ if (CONFIG.verbose) console.log(`Forwarded receipt data to ${registeredLogServers.size} LogServers`)
}
