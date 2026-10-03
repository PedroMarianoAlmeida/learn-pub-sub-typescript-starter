import amqp from "amqplib";
import { decode } from "@msgpack/msgpack";
export var SimpleQueueType;
(function (SimpleQueueType) {
    SimpleQueueType[SimpleQueueType["Durable"] = 0] = "Durable";
    SimpleQueueType[SimpleQueueType["Transient"] = 1] = "Transient";
})(SimpleQueueType || (SimpleQueueType = {}));
export var AckType;
(function (AckType) {
    AckType[AckType["Ack"] = 0] = "Ack";
    AckType[AckType["NackRequeue"] = 1] = "NackRequeue";
    AckType[AckType["NackDiscard"] = 2] = "NackDiscard";
})(AckType || (AckType = {}));
export async function declareAndBind(conn, exchange, queueName, key, queueType, exchangeType = "direct", queueArguments = {
    "x-dead-letter-exchange": "peril_dlx",
}) {
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
export async function subscribe(conn, exchange, queueName, key, queueType, handler, deserializer, exchangeType = "direct", queueArguments) {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType, exchangeType, queueArguments);
    await ch.consume(queue.queue, async (msg) => {
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
export function subscribeJSON(conn, exchange, queueName, key, queueType, handler, exchangeType = "direct", queueArguments) {
    return subscribe(conn, exchange, queueName, key, queueType, handler, (data) => JSON.parse(data.toString()), exchangeType, queueArguments);
}
export function subscribeMsgPack(conn, exchange, queueName, key, queueType, handler, exchangeType = "direct", queueArguments) {
    return subscribe(conn, exchange, queueName, key, queueType, handler, (data) => decode(data), exchangeType, queueArguments);
}
