import type { CommandHandler, TBlocked, TRespData } from "../types";

export function isStrictNumber(str: string) {
  if (typeof str !== "string" || str.trim() === "") return false;
  return !Number.isNaN(Number(str));
}

export const safeHandler = (handler: CommandHandler): CommandHandler => {
  return async (args, ctx) => {
    try {
      await handler(args, ctx);
    } catch (err) {
      const message = err instanceof Error ? err.message : "unknown error";
      ctx.socket.write(`-ERR ${message}\r\n`);
    }
  };
};

export const wakeBlockedClients = (
  key: string,
  cache: Map<string, TRespData>,
  blocked: Map<string, Array<TBlocked>>,
) => {
  const queue = blocked.get(key);
  if (!queue || queue.length === 0) return;
  const list = cache.get(key);
  if (!Array.isArray(list) || list.length === 0) return;
  while (queue.length > 0 && list.length > 0) {
    const client = queue.shift();
    const element = list.shift();
    client?.unblock(key, element || null);
  }
  if (queue.length === 0) {
    blocked.delete(key);
  }
};
