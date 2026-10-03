import { handlePause } from "../internal/gamelogic/pause.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => void {
  return (ps) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
  };
}
