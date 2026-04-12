import { SUBSCRIBE_MODE_APPLICABLE_CMDS, WRITE_CMDS } from "../constants";
import RespEncoder from "../encoder/RespEncoder";
import RedisError from "../error";
import { handlers } from "../handler";
import { sendToReplica } from "../main";
import type { ICommandContext, TRespData } from "../types";

export async function executeCommand(message: TRespData, ctx: ICommandContext) {
  if (ctx.myMaster && ctx.isFromMaster && message && Array.isArray(message)) {
    message = message.map((item: any) => item.value as string);
  }
  if (!Array.isArray(message) || message.length === 0) {
    return ctx.socket.write(`-ERR protocol error\r\n`);
  }
  const [commandRaw, ...args] = message;
  if (typeof commandRaw !== "string") {
    return ctx.socket.write(`-ERR invalid command\r\n`);
  }
  const command = commandRaw.toUpperCase();
  const isWriteCMD = WRITE_CMDS.includes(command);
  if (isWriteCMD && ctx.myMaster && !ctx.isFromMaster) {
    return ctx.socket.write(
      `-ERREADONLY You can't write against a read only replica.\r\n`,
    );
  }
  if (isWriteCMD && !ctx.myMaster && !ctx.isFromMaster) {
    sendToReplica(message, ctx);
  }
  const handler = handlers[command];
  if (!handler) {
    return ctx.socket.write(`-ERR unknown command '${command}'\r\n`);
  }
  if (
    ctx.isMulti &&
    commandRaw.toUpperCase() !== "EXEC" &&
    commandRaw.toUpperCase() !== "DISCARD"
  ) {
    ctx.cmdQueue?.push({ handler, args });
    return ctx.socket.write("+QUEUED\r\n");
  }
  if (
    ctx.isSubscribeMode &&
    !SUBSCRIBE_MODE_APPLICABLE_CMDS.includes(commandRaw.toUpperCase())
  ) {
    return ctx.socket.write(
      `-ERR Can't execute '${commandRaw}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n`,
    );
  }
  try {
    const result = await handler(args, ctx);
    if (result === undefined) return;
    const shouldReply =
      !ctx.isFromMaster ||
      (ctx.isFromMaster && command === "REPLCONF" && args[0] === "GETACK");
    if (shouldReply) {
      ctx.socket.write(RespEncoder.encode(result as TRespData));
    }
  } catch (err: any) {
    const message = err instanceof Error ? err.message : "unknown error";
    if (!ctx.isFromMaster) {
      if (err instanceof RedisError) {
        ctx.socket.write(`-${err.code} ${err.message}\r\n`);
      } else {
        ctx.socket.write(`-ERR ${message}\r\n`);
      }
    }
  }
}
