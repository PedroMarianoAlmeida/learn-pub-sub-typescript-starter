import amqp from "amqplib";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const ch = await conn.createConfirmChannel();
  const state: PlayingState = { isPaused: true };

  await publishJSON(ch, ExchangePerilDirect, PauseKey, state);
  await conn.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
