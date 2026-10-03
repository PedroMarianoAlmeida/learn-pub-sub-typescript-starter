import amqp from "amqplib"; import type { Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel();
  await ch.assertExchange(exchange, "direct", { durable: true });
  const isTransient = queueType === SimpleQueueType.Transient;
  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: isTransient,
    exclusive: isTransient,
  });

  await ch.bindQueue(queue.queue, exchange, key);

  return [ch, queue];
}
