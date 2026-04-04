import type { CommandHandler } from "../types";

export function isStrictNumber(str: string) {
  if (typeof str !== "string" || str.trim() === "") return false;
  return !Number.isNaN(Number(str));
}

export const safeHandler = (handler: CommandHandler): CommandHandler => {
  return (args, ctx) => {
    try {
      const result = handler(args, ctx);
      if (result === undefined) {
        ctx.socket.write(`-ERR internal handler error\r\n`);
      }
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : "unknown error";
      ctx.socket.write(`-ERR ${message}\r\n`);
    }
  };
};
