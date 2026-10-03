import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
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
  const gameState = new GameState(username);

  const [ch, queue] = await declareAndBind(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
  );

  await ch.consume(queue.queue, (msg) => {
    if (msg === null) {
      return;
    }

    const state = JSON.parse(msg.content.toString()) as PlayingState;
    handlePause(gameState, state);
    ch.ack(msg);
  });

  await new Promise<void>((resolve) => {
    process.once("SIGINT", resolve);
  });

  await conn.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
