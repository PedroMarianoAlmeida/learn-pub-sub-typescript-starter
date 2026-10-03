import amqp from "amqplib";
import type { Channel } from "amqplib";
import { decode } from "@msgpack/msgpack";

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
  queueArguments: Record<string, unknown> = {
    "x-dead-letter-exchange": "peril_dlx",
  },
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel();
  await ch.assertExchange(exchange, exchangeType, { durable: true });
  const isTransient = queueType === SimpleQueueType.Transient;
  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: isTransient,
    exclusive: isTransient,
    arguments: queueArguments,
  });

  await ch.bindQueue(queue.queue, exchange, key);

  return [ch, queue];
}

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
  exchangeType: "direct" | "topic" = "direct",
  queueArguments?: Record<string, unknown>,
): Promise<void> {
  const [ch, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    exchangeType,
    queueArguments,
  );

  await ch.prefetch(10);

  await ch.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (msg === null) {
      return;
    }

    const data = deserializer(msg.content);
    switch (await handler(data)) {
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

export function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  exchangeType: "direct" | "topic" = "direct",
  queueArguments?: Record<string, unknown>,
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data) => JSON.parse(data.toString()) as T,
    exchangeType,
    queueArguments,
  );
}

export function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  exchangeType: "direct" | "topic" = "direct",
  queueArguments?: Record<string, unknown>,
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data) => decode(data) as T,
    exchangeType,
    queueArguments,
  );
}
