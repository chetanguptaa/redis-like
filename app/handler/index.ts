import { SET_OPTIONS } from "../constants";
import Stream, { type TEntry } from "../data-structures/Stream";
import RespEncoder from "../encoder/RespEncoder";
import type { CommandHandler, TRespData } from "../types";
import {
  isStrictNumber,
  safeHandler,
  wakeBlockedListClients,
  wakeBlockedStreamsClients,
} from "../utils";

export const rawHandlers: Record<string, CommandHandler> = {
  ECHO: (args, { socket }) => {
    if (args.length < 1) {
      return socket.write(`-ERR wrong number of arguments for 'echo'\r\n`);
    }
    return socket.write(RespEncoder.encode(args[0]));
  },

  PING: (_args, { socket }) => {
    return socket.write("+PONG\r\n");
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
    return socket.write("+OK\r\n");
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
    return socket.write(RespEncoder.encode(value));
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
    wakeBlockedListClients(key, cache, blocked);
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
    wakeBlockedListClients(key, cache, blocked);
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
    return;
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
    const unblock = (key?: string, element?: TRespData) => {
      if (resolved) return;
      resolved = true;
      if (timer) clearTimeout(timer);
      if (key && element) {
        socket.write(RespEncoder.encode([key, element]));
      }
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

  XADD: (args, { socket, cache, blocked }) => {
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
      let id = args[1];
      const kVS = args.slice(2, args.length);
      if (typeof id === "string") {
        if (id === "*") {
          const timestamp = Date.now();
          id = `${timestamp}` + "-" + "*";
        }
        const lastEntry = value.entries[value.entries.length - 1];
        if (lastEntry) {
          const [lastTimestamp, lastSeq] = lastEntry.id.split("-");
          const lastTimestampNum = Number(lastTimestamp);
          const lastSeqNum = Number(lastSeq);
          const [newTimestamp, newSeq] = id.split("-");
          const newTimestampNum = Number(newTimestamp);
          if (newSeq !== "*") {
            const newSeqNum = Number(newSeq);
            if (newTimestampNum === 0 && newSeqNum === 0) {
              return socket.write(
                `-ERR The ID specified in XADD must be greater than 0-0\r\n`,
              );
            }
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
          } else {
            if (lastTimestampNum === newTimestampNum) {
              id = id.split("-")[0] + "-" + (lastSeqNum + 1).toString();
            } else {
              if (id.split("-")[1] === "*") {
                id = id.split("-")[0] + "-" + (0).toString();
              }
            }
          }
        } else {
          const [timestamp, seq] = id.split("-");
          if (timestamp === "*" || seq === "*") {
            if (Number(timestamp) === 0) {
              id = id.split("-")[0] + "-" + "1";
            } else {
              id = id.split("-")[0] + "-" + "0";
            }
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
      socket.write(RespEncoder.encode(id));
      wakeBlockedStreamsClients(key, blocked);
      return;
    } else {
      return socket.write(
        `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
      );
    }
  },

  XRANGE: (args, { socket, cache }) => {
    if (args.length < 3) {
      return socket.write(`-ERR wrong number of arguments for 'xrange'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    const value = cache.get(key) ?? null;
    if (!value || !(value instanceof Stream)) {
      return socket.write(
        `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
      );
    }
    const left = args[1];
    const right = args[2];
    if (typeof left === "string" && typeof right === "string") {
      const entries = value.entries;
      const lastEntry = entries[entries.length - 1];
      const lastSeq = Number(lastEntry.id.split("-")[1]);
      let [leftTS, leftSeq] = left.split("-").map(Number);
      let [rightTS, rightSeq] = right.split("-").map(Number);
      if (!leftSeq) leftSeq = 0;
      if (!rightSeq) rightSeq = lastSeq;
      const output: TRespData[] = [];
      for (let i = 0; i < entries.length; i++) {
        const e = entries[i];
        const [eTS, eSeq] = e.id.split("-").map(Number);
        if (eTS < leftTS || (eTS === leftTS && eSeq < leftSeq)) {
          continue;
        }
        if (eTS > rightTS || (eTS === rightTS && eSeq > rightSeq)) {
          continue;
        }
        const fieldArray: TRespData[] = [];
        for (const [field, val] of Object.entries(e)) {
          if (field === "id") continue;
          fieldArray.push(field, val);
        }
        output.push([e.id, fieldArray]);
      }
      return socket.write(RespEncoder.encode(output));
    }
    return socket.write(`-ERR invalid range arguments\r\n`);
  },

  XREAD: (args, { socket, cache, blocked }) => {
    if (args.length < 3) {
      return socket.write(`-ERR wrong number of arguments for 'xread'\r\n`);
    }
    let i = 0;
    let timeout: number | null = null;
    if (String(args[i])?.toUpperCase() === "BLOCK") {
      timeout = parseFloat(String(args[i + 1]));
      if (isNaN(timeout)) {
        return socket.write(`-ERR timeout is not a number\r\n`);
      }
      i += 2;
    }
    if (String(args[i])?.toUpperCase() !== "STREAMS") {
      return socket.write(`-ERR syntax error\r\n`);
    }
    i++;
    const remaining = args.length - i;
    if (remaining % 2 !== 0) {
      return socket.write(`-ERR syntax error\r\n`);
    }
    const half = remaining / 2;
    const keys = args.slice(i, i + half);
    const ids = args.slice(i + half);
    const resolvedIds = ids.map((id, idx) => {
      if (id !== "$") return id;
      const key = keys[idx];
      if (typeof key === "string") {
        const value = cache.get(key);
        if (
          !value ||
          !(value instanceof Stream) ||
          value.entries.length === 0
        ) {
          return "0-0";
        }
        return value.entries[value.entries.length - 1].id;
      }
    });
    const readStreams = () => {
      const result: TRespData[] = [];
      for (let k = 0; k < keys.length; k++) {
        const key = keys[k];
        const id = resolvedIds[k];
        if (typeof key !== "string" || typeof id !== "string") {
          return null;
        }
        const value = cache.get(key);
        if (value && !(value instanceof Stream)) {
          return "WRONGTYPE";
        }
        if (!value || !(value instanceof Stream)) continue;
        let ts: number, seq: number;
        if (id === "$") {
          if (!value.entries.length) {
            ts = 0;
            seq = 0;
          } else {
            const lastId = value.entries[value.entries.length - 1].id;
            [ts, seq] = lastId.split("-").map(Number);
          }
        } else {
          [ts, seq] = id.split("-").map(Number);
          if (!seq) seq = 0;
        }
        if (!seq) seq = 0;
        const entriesOut: TRespData[] = [];
        for (const e of value.entries) {
          const [eTS, eSeq] = e.id.split("-").map(Number);
          if (eTS < ts || (eTS === ts && eSeq <= seq)) continue;
          const fieldArr: TRespData[] = [];
          for (const [f, v] of Object.entries(e)) {
            if (f === "id") continue;
            fieldArr.push(f, v);
          }
          entriesOut.push([e.id, fieldArr]);
        }
        if (entriesOut.length > 0) {
          result.push([key, entriesOut]);
        }
      }
      return result;
    };
    const immediate = readStreams();
    if (immediate === "WRONGTYPE") {
      return socket.write(
        `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`,
      );
    }
    if (immediate && immediate.length > 0) {
      return socket.write(RespEncoder.encode(immediate));
    }
    if (timeout === null) {
      return socket.write(`*-1\r\n`);
    }
    let resolved = false;
    const unblock = () => {
      if (resolved) return;
      const res = readStreams();
      if (res && res.length > 0) {
        resolved = true;
        for (const k of keys) {
          if (typeof k === "string") {
            const arr = blocked.get(k);
            if (!arr) continue;
            blocked.set(
              k,
              arr.filter((c) => c.socket !== socket),
            );
          }
        }
        if (timer) clearTimeout(timer);
        return socket.write(RespEncoder.encode(res));
      }
    };
    for (const key of keys) {
      if (typeof key !== "string") continue;
      if (!blocked.has(key)) blocked.set(key, []);
      blocked.get(key)?.push({ socket, unblock });
    }
    let timer: NodeJS.Timeout | null = null;
    if (timeout > 0) {
      timer = setTimeout(() => {
        if (resolved) return;
        resolved = true;
        socket.write(`*-1\r\n`);
      }, timeout);
    }
  },

  INCR: (args, { socket, cache }) => {
    if (args.length < 1) {
      return socket.write(`-ERR wrong number of arguments for 'incr'\r\n`);
    }
    const key = args[0];
    if (typeof key !== "string") {
      return socket.write(`-ERR invalid key\r\n`);
    }
    const val = cache.get(key);
    if (val && typeof val === "string" && !isStrictNumber(val)) {
      return socket.write(`-ERR value is not an integer or out of range\r\n`);
    }
    let newVal = 1;
    if (val) {
      newVal = Number(val) + 1;
    }
    cache.set(key, newVal.toString());
    return socket.write(RespEncoder.encode(newVal));
  },
};

export const handlers: Record<string, CommandHandler> = Object.fromEntries(
  Object.entries(rawHandlers).map(([cmd, handler]) => [
    cmd,
    safeHandler(handler),
  ]),
);
