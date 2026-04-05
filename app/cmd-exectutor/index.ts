import { handlers } from "../handler";
import type { CommandContext, TRespData } from "../types";

export function executeCommand(
  message: TRespData,
  ctx: CommandContext,
  isMulti: boolean,
  cmdQueue: (() => void)[],
) {
  if (!Array.isArray(message) || message.length === 0) {
    return ctx.socket.write(`-ERR protocol error\r\n`);
  }
  const [commandRaw, ...args] = message;
  if (typeof commandRaw !== "string") {
    return ctx.socket.write(`-ERR invalid command\r\n`);
  }
  const command = commandRaw.toUpperCase();
  const handler = handlers[command];
  if (!handler) {
    return ctx.socket.write(`-ERR unknown command '${command}'\r\n`);
  }
  if (isMulti) {
    cmdQueue.push(() => handler(args, ctx));
    return ctx.socket.write("+QUEUED\r\n");
  } else {
    handler(args, ctx);
  }
}
