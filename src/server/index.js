import amqp from "amqplib";
import { writeLog } from "../internal/gamelogic/logs.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { AckType, SimpleQueueType, subscribeMsgPack, } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, } from "../internal/routing/routing.js";
async function main() {
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(ExchangePerilDirect, "direct", { durable: true });
    await ch.assertExchange(ExchangePerilTopic, "topic", { durable: true });
    await subscribeMsgPack(conn, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable, async (gameLog) => {
        await writeLog(gameLog);
        process.stdout.write("> ");
        return AckType.Ack;
    }, "topic", {});
    // Used to run the server from a non-interactive source, like the multiserver.sh file
    if (!process.stdin.isTTY) {
        console.log("Non-interactive mode: skipping command input.");
        return;
    }
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
