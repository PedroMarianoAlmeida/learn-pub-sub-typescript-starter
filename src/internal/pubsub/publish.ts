import type {ConfirmChannel} from "amqplib"

function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void>;

publishJSON("1", "2", "3", JSON.stringify({value: "Test"}))
