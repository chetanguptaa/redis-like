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

export const encodeGeohash = (lat: number, lon: number): number => {
  const latNorm = (lat + 90) / 180;
  const lonNorm = (lon + 180) / 360;
  let hash = 0;
  for (let i = 25; i >= 0; i--) {
    const latBit = Math.floor(latNorm * (1 << (i + 1))) & 1;
    const lonBit = Math.floor(lonNorm * (1 << (i + 1))) & 1;
    hash = hash * 4 + lonBit * 2 + latBit;
  }
  return hash;
};

export const decodeGeohash = (score: number): { lat: number; lon: number } => {
  let latNorm = 0,
    lonNorm = 0;
  for (let i = 25; i >= 0; i--) {
    lonNorm += ((score >> (2 * i + 1)) & 1) / (1 << (25 - i + 1));
    latNorm += ((score >> (2 * i)) & 1) / (1 << (25 - i + 1));
  }
  return {
    lat: latNorm * 180 - 90,
    lon: lonNorm * 360 - 180,
  };
};
