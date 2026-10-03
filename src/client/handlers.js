import { handlePause } from "../internal/gamelogic/pause.js";
export function handlerPause(gs) {
    return (ps) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
    };
}
