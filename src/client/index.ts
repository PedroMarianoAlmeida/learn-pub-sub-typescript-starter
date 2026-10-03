import amqp from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import {
  declareAndBind,
  SimpleQueueType,
} from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const username = await clientWelcome();

  const [ch, queue] = await declareAndBind(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
  );
  const gameState = new GameState(username);

  await ch.consume(queue.queue, (msg) => {
    if (msg === null) {
      return;
    }

    const state = JSON.parse(msg.content.toString()) as PlayingState;
    handlePause(gameState, state);
    ch.ack(msg);
  });

  clientLoop: while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }

    switch (words[0]) {
      case "spawn":
        try {
          commandSpawn(gameState, words);
        } catch (err) {
          console.log((err as Error).message);
        }
        break;
      case "move":
        try {
          commandMove(gameState, words);
        } catch (err) {
          console.log((err as Error).message);
        }
        break;
      case "status":
        await commandStatus(gameState);
        break;
      case "help":
        printClientHelp();
        break;
      case "spam":
        console.log("Spamming not allowed yet!");
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
