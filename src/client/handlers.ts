import type { ConfirmChannel } from "amqplib";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import type {
  ArmyMove,
  RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(
  gs: GameState,
  publisher: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move) => {
    const outcome = handleMove(gs, move);

    switch (outcome) {
      case MoveOutcome.Safe:
        process.stdout.write("> ");
        return AckType.Ack;
      case MoveOutcome.MakeWar: {
        const rw: RecognitionOfWar = {
          attacker: move.player,
          defender: gs.getPlayerSnap(),
        };
        try {
          await publishJSON(
            publisher,
            ExchangePerilTopic,
            `${WarRecognitionsPrefix}.${rw.attacker.username}`,
            rw,
          );
          return AckType.Ack;
        } catch {
          return AckType.NackRequeue;
        } finally {
          process.stdout.write("> ");
        }
      }
      case MoveOutcome.SamePlayer:
      default:
        process.stdout.write("> ");
        return AckType.NackDiscard;
    }
  };
}

export function handlerWar(
  gs: GameState,
  publisher: ConfirmChannel,
  publishGameLog: (
    ch: ConfirmChannel,
    username: string,
    message: string,
  ) => Promise<void>,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw) => {
    const outcome = handleWar(gs, rw);
    process.stdout.write("> ");

    switch (outcome.result) {
      case WarOutcome.NotInvolved:
        return AckType.NackDiscard;
      case WarOutcome.NoUnits:
        return AckType.NackDiscard;
      case WarOutcome.OpponentWon:
      case WarOutcome.YouWon:
        try {
          await publishGameLog(
            publisher,
            rw.attacker.username,
            `${outcome.winner} won a war against ${outcome.loser}`,
          );
          return AckType.Ack;
        } catch {
          return AckType.NackRequeue;
        }
      case WarOutcome.Draw:
        try {
          await publishGameLog(
            publisher,
            rw.attacker.username,
            `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`,
          );
          return AckType.Ack;
        } catch {
          return AckType.NackRequeue;
        }
      default:
        console.error("Unexpected war outcome:", outcome);
        return AckType.NackDiscard;
    }
  };
}
