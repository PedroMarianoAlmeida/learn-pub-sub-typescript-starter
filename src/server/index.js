import amqp from "amqplib";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, } from "../internal/routing/routing.js";
async function main() {
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(ExchangePerilDirect, "direct", { durable: true });
    await ch.assertExchange(ExchangePerilTopic, "topic", { durable: true });
    const gameLogsQueue = await ch.assertQueue(GameLogSlug, { durable: true });
    await ch.bindQueue(gameLogsQueue.queue, ExchangePerilTopic, `${GameLogSlug}.*`);
    printServerHelp();
    serverLoop: while (true) {
        const words = await getInput();
        if (words.length === 0) {
            continue;
        }
        switch (words[0]) {
            case "pause": {
                console.log("Sending pause message.");
                const state = { isPaused: true };
                await publishJSON(ch, ExchangePerilDirect, PauseKey, state);
                break;
            }
            case "resume": {
                console.log("Sending resume message.");
                const state = { isPaused: false };
                await publishJSON(ch, ExchangePerilDirect, PauseKey, state);
                break;
            }
            case "quit":
                console.log("Exiting.");
                break serverLoop;
            default:
                console.log("I don't understand that command.");
        }
    }
    await conn.close();
}
main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});
