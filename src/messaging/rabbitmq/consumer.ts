import * as amqp from 'amqplib'

export default class RMQConsumer {
  name: string // can be used as identifier
  queue: string
  prefetch: number
  processFn: (msg: string) => Promise<boolean>

  conn: amqp.Connection | null = null
  channel: amqp.Channel | null = null
  isConnected: boolean
  isConnClosing: boolean

  constructor(name: string, queue: string, prefetch = 10, callback: (msg: string) => Promise<boolean>) {
    this.name = name
    this.queue = queue
    this.prefetch = prefetch
    this.isConnected = false
    this.isConnClosing = false
    this.processFn = callback
  }

  public async consume(): Promise<void> {
    try {
      if (!this.isConnected || this.conn === null) {
        this.conn = await this.connect()
      }

      this.conn.on('error', this.handleConnectionError)
      this.conn.on('close', this.handleConnectionClose)

      this.channel = await this.conn.createChannel()
      this.channel.prefetch(this.prefetch)
      console.log(`[Consumer ${this.name}]: Started listening to queue: ${this.queue}`)
      this.channel.consume(this.queue, async (msg) => {
        if (msg) {
          console.log(`[Consumer ${this.name}]: Received message`)
          try {
            const success = await this.processFn(msg.content.toString())
            if (success === true) {
              this.channel.ack(msg)
              console.log(`[Consumer ${this.name}]: Successfully processed message`)
            } else {
              this.channel.nack(msg, false, true)
            }
          } catch (e) {
            console.error(
              `Consumer [${
                this.name
              }]: Error while processing message: ${e}\nMessage: ${msg.content.toString()}`
            )
            this.channel.nack(msg, false, true)
          }
        }
      })
    } catch (e) {
      console.error(`[Consumer ${this.name}]: Unexpected error occurred, retyring connection. Err: ${e}`)
      throw e
    }
  }

  private async connect(): Promise<amqp.Connection> {
    return await amqp.connect({
      protocol: process.env.RMQ_PROTOCOL || 'amqp',
      hostname: process.env.RMQ_HOST,
      port: process.env.RMQ_PORT ? parseInt(process.env.RMQ_PORT) : 5672,
      username: process.env.RMQ_USER,
      password: process.env.RMQ_PASS,
    })
  }

  public async cleanUp(): Promise<void> {
    this.isConnClosing = true
    if (this.channel != null) {
      await this.channel.close()
      console.log(`[Consumer ${this.name}]: Closed channel successfully`)
    }
    if (this.conn !== null) {
      await this.conn.close()
      console.log(`[Consumer ${this.name}]: Closed connection successfully`)
    }
  }

  private async handleConnectionError(error: unknown): Promise<void> {
    console.error(`[Consumer ${this.name}]: Connection error: ${error}`)
    this.isConnected = false
  }

  private handleConnectionClose = async (): Promise<void> => {
    if (this.isConnClosing === true) {
      // this is triggered internally, possibly on SIGTERM/SIGINT; so no need to retry
      return Promise.resolve()
    }

    this.isConnected = false
    return new Promise<void>((resolve) => {
      this.retryConnection()
      resolve()
    })
  }

  private retryConnection(): void {
    let attempt = 0
    if (!this.isConnected) {
      const interval = setInterval(async () => {
        attempt++
        console.log(`[retryConnection ${this.name}]: (Attempt ${attempt}) intitiated connection retry...`)
        try {
          await this.consume()
          console.log(`[retryConnection ${this.name}]: (Attempt ${attempt}) successfully connected...`)
          this.isConnected = true
          clearInterval(interval)
        } catch (e) {
          console.log(`[retryConnection ${this.name}]: (Attempt ${attempt}) unsuccessul. Err: ${e}`)
        }
      }, 5000) // Wait 5 seconds before retrying
    }
  }
}
