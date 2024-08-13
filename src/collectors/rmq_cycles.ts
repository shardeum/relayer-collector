import * as amqp from 'amqplib'
import RMQConsumer from '../messaging/rabbitmq/consumer'
import { validateData } from '../class/validateData'
import { Utils as StringUtils } from '@shardus/types'

export default class RMQCyclesConsumer {
  consumer: RMQConsumer
  conn: amqp.Connection
  channel: amqp.Channel

  constructor() {
    const queueName = process.env.RMQ_CYCLES_QUEUE_NAME
    if (queueName === null || queueName === undefined || queueName.trim() === '') {
      throw new Error('[RMQCyclesConsumer]: please provide queue name for consumer')
    }
    this.consumer = new RMQConsumer('Cycles', queueName, 20, this.processMessage.bind(this))
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
      console.log(`cycles#processMessage: ${success}`)
      return success
    } catch (e) {
      console.error(`[RMQCyclesConsumer]: Error while processing message: ${e}`)
      return false
    }
  }
}
