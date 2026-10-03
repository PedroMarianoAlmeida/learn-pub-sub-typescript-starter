import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, getMaliciousLog, printClientHelp, printQuit, } from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { subscribeJSON, SimpleQueueType, } from "../internal/pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix, } from "../internal/routing/routing.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
export function publishGameLog(ch, username, message) {
    const gameLog = {
        username,
        message,
        currentTime: new Date(),
    };
    return publishMsgPack(ch, ExchangePerilTopic, `${GameLogSlug}.${username}`, gameLog);
}
async function main() {
    console.log("Starting Peril client...");
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    const username = await clientWelcome();
    const gameState = new GameState(username);
    const publisher = await conn.createConfirmChannel();
    await subscribeJSON(conn, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, `${ArmyMovesPrefix}.*`, SimpleQueueType.Transient, handlerMove(gameState, publisher), "topic");
    await subscribeJSON(conn, ExchangePerilTopic, `${WarRecognitionsPrefix}.${username}`, `${WarRecognitionsPrefix}.${username}`, SimpleQueueType.Durable, handlerWar(gameState, publisher, publishGameLog), "topic");
    await subscribeJSON(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState));
    clientLoop: while (true) {
        const words = await getInput();
        if (words.length === 0) {
            continue;
        }
        switch (words[0]) {
            case "spawn":
                try {
                    commandSpawn(gameState, words);
                }
                catch (err) {
                    console.log(err.message);
                }
                break;
            case "move":
                try {
                    const move = commandMove(gameState, words);
                    await publishJSON(publisher, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, move);
                    console.log("Move published successfully.");
                }
                catch (err) {
                    console.log(err.message);
                }
                break;
            case "status":
                await commandStatus(gameState);
                break;
            case "help":
                printClientHelp();
                break;
            case "spam":
                try {
                    if (!words[1]) {
                        throw new Error("usage: spam <count>");
                    }
                    const count = Number.parseInt(words[1], 10);
                    if (Number.isNaN(count) || count < 0) {
                        throw new Error("count must be a non-negative integer");
                    }
                    for (let i = 0; i < count; i += 1) {
                        await publishGameLog(publisher, username, getMaliciousLog());
                    }
                }
                catch (err) {
                    console.log(err.message);
                }
                break;
            case "quit":
                printQuit();
                break clientLoop;
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
