import { encode } from "@msgpack/msgpack";
export function publishJSON(ch, exchange, routingKey, value) {
    const content = Buffer.from(JSON.stringify(value));
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, content, { contentType: "application/json" }, (err) => {
            if (err) {
                reject(err);
                return;
            }
            resolve();
        });
    });
}
export function publishMsgPack(ch, exchange, routingKey, value) {
    const content = Buffer.from(encode(value));
    return new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, content, { contentType: "application/x-msgpack" }, (err) => {
            if (err) {
                reject(err);
                return;
            }
            resolve();
        });
    });
}
