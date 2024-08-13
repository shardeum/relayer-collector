import * as amqp from 'amqplib'
import RMQConsumer from '../messaging/rabbitmq/consumer'
import { validateData } from '../class/validateData'
import { Utils as StringUtils } from '@shardus/types'

export default class RMQReceiptsConsumer {
  consumer: RMQConsumer
  conn: amqp.Connection
  channel: amqp.Channel

  constructor() {
    const queueName = process.env.RMQ_RECEIPTS_QUEUE_NAME
    if (queueName === null || queueName === undefined || queueName.trim() === '') {
      throw new Error('[RMQReceiptsConsumer]: please provide queue name for consumer')
    }
    this.consumer = new RMQConsumer('Receipts', queueName, 100, this.processMessage.bind(this))
  }

  public async start(): Promise<void> {
    await this.consumer.consume()
  }

  public async cleanUp(): Promise<void> {
    await this.consumer.cleanUp()
  }

  private async processMessage(msgStr: string): Promise<boolean> {
    try {
      const success = await validateData(StringUtils.safeJsonParse(msgStr))
      console.log(`receipts#processMessage: ${success}`)
      return success
    } catch (e) {
      console.error(`[RMQReceiptsConsumer]: Error while processing message: ${e}`)
      return false
    }
  }
}
