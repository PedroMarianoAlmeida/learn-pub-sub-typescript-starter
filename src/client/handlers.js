import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { AckType } from "../internal/pubsub/consume.js";
export function handlerPause(gs) {
    return (ps) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
}
export function handlerMove(gs) {
    return (move) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ");
        switch (outcome) {
            case MoveOutcome.Safe:
            case MoveOutcome.MakeWar:
                return AckType.Ack;
            case MoveOutcome.SamePlayer:
            default:
                return AckType.NackDiscard;
        }
    };
}
