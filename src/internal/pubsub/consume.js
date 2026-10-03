import amqp from "amqplib";
export var SimpleQueueType;
(function (SimpleQueueType) {
    SimpleQueueType[SimpleQueueType["Durable"] = 0] = "Durable";
    SimpleQueueType[SimpleQueueType["Transient"] = 1] = "Transient";
})(SimpleQueueType || (SimpleQueueType = {}));
export async function declareAndBind(conn, exchange, queueName, key, queueType, exchangeType = "direct") {
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
export async function subscribeJSON(conn, exchange, queueName, key, queueType, handler, exchangeType = "direct") {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType, exchangeType);
    await ch.consume(queue.queue, (msg) => {
        if (msg === null) {
            return;
        }
        const data = JSON.parse(msg.content.toString());
        handler(data);
        ch.ack(msg);
    });
}
