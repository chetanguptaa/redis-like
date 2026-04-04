import { SET_OPTIONS } from "../constants";
import Stream, { type TEntry } from "../data-structures/Stream";
import RespEncoder from "../encoder/RespEncoder";
import type { CommandHandler, TRespData } from "../types";
import { isStrictNumber, safeHandler, wakeBlockedClients } from "../utils";

export const rawHandlers: Record<string, CommandHandler> = {
  ECHO: (args, { socket }) => {
    if (args.length < 1) {
      return socket.write(`-ERR wrong number of arguments for 'echo'\r\n`);
    }
    socket.write(RespEncoder.encode(args[0]));
  },

  PING: (_args, { socket }) => {
    socket.write("+PONG\r\n");
  },

  SET: (args, { socket, cache }) => {
    if (args.length < 2) {
      return socket.write(`-ERR wrong number of arguments for 'set'\r\n`);
    }
    const [key, value, option, ttlRaw] = args;
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    let ttl: number | undefined;
    if (option && ttlRaw) {
      if (typeof ttlRaw === "string" && !isStrictNumber(ttlRaw)) {
        return socket.write(`-ERR invalid expire time\r\n`);
      }
      if (option === SET_OPTIONS.EX) {
        ttl = Number(ttlRaw) * 1000;
      } else if (option === SET_OPTIONS.PX) {
        ttl = Number(ttlRaw);
      }
    }
    cache.set(key, value);
    if (ttl) {
      setTimeout(() => cache.delete(key), ttl);
    }
    socket.write("+OK\r\n");
  },

  GET: (args, { socket, cache }) => {
    if (args.length < 1) {
      return socket.write(`-ERR wrong number of arguments for 'get'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    const value = cache.get(key) ?? null;
    socket.write(RespEncoder.encode(value));
  },

  RPUSH: (args, { socket, cache, blocked }) => {
    if (args.length < 2) {
      return socket.write(`-ERR wrong number of arguments for 'rpush'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    let value = cache.get(key) ?? null;
    if (value && !Array.isArray(value)) {
      return socket.write(
        `WRONGTYPE Operation against a key holding the wrong kind of value`,
      );
    }
    value = Array.isArray(value) ? value : [];
    for (let i = 1; i < args.length; i++) {
      value.push(args[i]);
    }
    cache.set(key, value);
    socket.write(RespEncoder.encode(value.length));
    wakeBlockedClients(key, cache, blocked);
  },

  LRANGE: (args, { socket, cache }) => {
    if (args.length !== 3) {
      return socket.write(`-ERR wrong number of arguments for 'lrange'\r\n`);
    }
    const [key, startArg, stopArg] = args;
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    const value = cache.get(key);
    if (value == null) {
      return socket.write(RespEncoder.encode([]));
    }
    if (!Array.isArray(value)) {
      return socket.write(
        `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
      );
    }
    if (typeof startArg === "string" && typeof stopArg === "string") {
      if (!isStrictNumber(startArg) || !isStrictNumber(stopArg)) {
        return socket.write(`-ERR value is not an integer or out of range\r\n`);
      }
      let start = parseInt(startArg, 10);
      let stop = parseInt(stopArg, 10);
      const len = value.length;
      if (start < 0) start = len + start;
      if (stop < 0) stop = len + stop;
      start = Math.max(start, 0);
      stop = Math.min(stop, len - 1);
      if (start > stop || start >= len) {
        return socket.write(RespEncoder.encode([]));
      }
      const result = value.slice(start, stop + 1);
      return socket.write(RespEncoder.encode(result));
    }
  },

  LPUSH: (args, { socket, cache, blocked }) => {
    if (args.length < 2) {
      return socket.write(`-ERR wrong number of arguments for 'lpush'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    let value = cache.get(key) ?? null;
    if (value && !Array.isArray(value)) {
      return socket.write(
        `WRONGTYPE Operation against a key holding the wrong kind of value`,
      );
    }
    value = Array.isArray(value) ? value : [];
    for (let i = 1; i < args.length; i++) {
      value.unshift(args[i]);
    }
    cache.set(key, value);
    socket.write(RespEncoder.encode(value.length));
    wakeBlockedClients(key, cache, blocked);
  },

  LLEN: (args, { socket, cache }) => {
    if (args.length < 1) {
      return socket.write(`-ERR wrong number of arguments for 'llen'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    let value = cache.get(key) ?? null;
    if (value && !Array.isArray(value)) {
      return socket.write(
        `WRONGTYPE Operation against a key holding the wrong kind of value`,
      );
    }
    value = Array.isArray(value) ? value : [];
    socket.write(RespEncoder.encode(value.length));
  },

  LPOP: (args, { socket, cache }) => {
    if (args.length < 1) {
      return socket.write(`-ERR wrong number of arguments for 'llen'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    let value = cache.get(key) ?? null;
    if (value && !Array.isArray(value)) {
      return socket.write(
        `WRONGTYPE Operation against a key holding the wrong kind of value`,
      );
    }
    value = Array.isArray(value) ? value : [];
    if (value.length === 0) {
      return socket.write(RespEncoder.encode(null));
    }
    if (args.length === 1) {
      return socket.write(RespEncoder.encode(value.shift() || null));
    }
    if (args.length === 2) {
      let amount = args[1];
      if (typeof amount === "string") {
        if (!isStrictNumber(amount)) {
          return socket.write(
            `-ERR value is not an integer or out of range\r\n`,
          );
        }
        amount = parseInt(amount);
        if (amount === 0) {
          return socket.write(RespEncoder.encode([]));
        }
        const output: TRespData[] = [];
        while (amount !== 0) {
          output.push(value.shift() || null);
          amount--;
        }
        return socket.write(RespEncoder.encode(output));
      }
    }
  },

  BLPOP: (args, { socket, cache, blocked }) => {
    if (args.length < 2) {
      return socket.write(`-ERR wrong number of arguments for 'blpop'\r\n`);
    }
    const timeout = parseFloat(String(args[args.length - 1]));
    const keys = args.slice(0, -1);
    if (isNaN(timeout)) {
      return socket.write(`-ERR timeout is not a number\r\n`);
    }
    for (const key of keys) {
      if (typeof key === "string") {
        let value = cache.get(key) ?? null;
        if (value && !Array.isArray(value)) {
          return socket.write(
            `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
          );
        }
        if (Array.isArray(value) && value.length > 0) {
          const element = value.shift();
          return socket.write(RespEncoder.encode([key, element || null]));
        }
      }
    }
    let resolved = false;
    const unblock = (key: string, element: TRespData) => {
      if (resolved) return;
      resolved = true;
      if (timer) clearTimeout(timer);
      socket.write(RespEncoder.encode([key, element]));
    };
    for (const key of keys) {
      if (typeof key === "string") {
        if (!blocked.has(key)) blocked.set(key, []);
        blocked.get(key)?.push({ socket, unblock });
      }
    }
    let timer: NodeJS.Timeout | null = null;
    if (timeout > 0) {
      timer = setTimeout(() => {
        if (resolved) return;
        resolved = true;
        socket.write(`*-1\r\n`);
      }, timeout * 1000);
    }
  },

  TYPE: (args, { socket, cache }) => {
    if (args.length < 1) {
      return socket.write(`-ERR wrong number of arguments for 'type'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    let value = cache.get(key) ?? null;
    if (!value) {
      return socket.write(`+none\r\n`);
    }
    if (value instanceof Stream) {
      return socket.write(`+stream\r\n`);
    }
    return socket.write(`+string\r\n`);
  },

  XADD: (args, { socket, cache }) => {
    if (args.length < 3) {
      return socket.write(`-ERR wrong number of arguments for 'xadd'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    let value = cache.get(key) ?? null;
    if (!value) {
      value = new Stream();
    }
    if (value instanceof Stream) {
      const id = args[1];
      const kVS = args.slice(2, args.length);
      if (typeof id === "string") {
        const lastEntry = value.entries[value.entries.length - 1];
        if (lastEntry) {
          const [lastTimestamp, lastSeq] = lastEntry.id.split("-");
          const [newTimestamp, newSeq] = id.split("-");
          const lastTimestampNum = Number(lastTimestamp);
          const lastSeqNum = Number(lastSeq);
          const newTimestampNum = Number(newTimestamp);
          const newSeqNum = Number(newSeq);
          if (lastTimestampNum > newTimestampNum) {
            return socket.write(
              `-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n`,
            );
          } else if (
            lastTimestampNum === newTimestampNum &&
            lastSeqNum >= newSeqNum
          ) {
            return socket.write(
              `-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n`,
            );
          }
        }
        const obj: TEntry = { id };
        for (let i = 0; i < kVS.length; i += 2) {
          const k = kVS[i] as string;
          const v = kVS[i + 1] as string;
          obj[k] = v;
        }
        value.entries.push(obj);
      }
      cache.set(key, value);
      return socket.write(RespEncoder.encode(id));
    } else {
      return socket.write(
        `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
      );
    }
  },
};

export const handlers: Record<string, CommandHandler> = Object.fromEntries(
  Object.entries(rawHandlers).map(([cmd, handler]) => [
    cmd,
    safeHandler(handler),
  ]),
);
