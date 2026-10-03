import amqp from "amqplib"; import type { Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  exchangeType: "direct" | "topic" = "direct",
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel();
  await ch.assertExchange(exchange, exchangeType, { durable: true });
  const isTransient = queueType === SimpleQueueType.Transient;
  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: isTransient,
    exclusive: isTransient,
  });

  await ch.bindQueue(queue.queue, exchange, key);

  return [ch, queue];
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType,
  exchangeType: "direct" | "topic" = "direct",
): Promise<void> {
  const [ch, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    exchangeType,
  );

  await ch.consume(queue.queue, (msg: amqp.ConsumeMessage | null) => {
    if (msg === null) {
      return;
    }

    const data = JSON.parse(msg.content.toString()) as T;
    switch (handler(data)) {
      case AckType.Ack:
        console.log("Acking message");
        ch.ack(msg);
        break;
      case AckType.NackRequeue:
        console.log("Nacking message and requeuing it");
        ch.nack(msg, false, true);
        break;
      case AckType.NackDiscard:
        console.log("Nacking message and discarding it");
        ch.nack(msg, false, false);
        break;
    }
  });
}
