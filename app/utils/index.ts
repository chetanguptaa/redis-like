import type {
  TCommandHandler,
  TBlocked,
  TRespData,
  TSimpleString,
} from "../types";

export function isStrictNumber(str: string) {
  if (typeof str !== "string" || str.trim() === "") return false;
  return !Number.isNaN(Number(str));
}

export const safeHandler = (handler: TCommandHandler): TCommandHandler => {
  return async (args, ctx) => {
    try {
      return await handler(args, ctx);
    } catch (err) {
      throw err;
    }
  };
};

export const wakeBlockedListClients = (
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
    client?.unblock(key, element ?? null);
  }
  if (queue.length === 0) {
    blocked.delete(key);
  }
};

export const wakeBlockedStreamsClients = (
  key: string,
  blocked: Map<string, Array<TBlocked>>,
) => {
  const clients = blocked.get(key);
  if (!clients || clients.length === 0) return;
  for (const client of clients) {
    client?.unblock();
  }
};

export const simpleString = (value: string): TSimpleString => ({
  __simple: true,
  value,
});
