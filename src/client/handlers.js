import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove } from "../internal/gamelogic/move.js";
export function handlerPause(gs) {
    return (ps) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
    };
}
export function handlerMove(gs) {
    return (move) => {
        handleMove(gs, move);
        process.stdout.write("> ");
    };
}
