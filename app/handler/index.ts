import { readFileSync } from "node:fs";
import { SET_OPTIONS } from "../constants";
import Stream, { type TEntry } from "../data-structures/Stream";
import RespEncoder from "../encoder/RespEncoder";
import RedisError from "../error";
import type { TCommandHandler, TRespData } from "../types";
import {
  isStrictNumber,
  safeHandler,
  simpleString,
  wakeBlockedListClients,
  wakeBlockedStreamsClients,
} from "../utils";

export const rawHandlers: Record<string, TCommandHandler> = {
  ECHO: (args) => {
    if (args.length < 1) {
      throw new Error("wrong number of arguments for 'echo'");
    }
    return args[0];
  },

  PING: (_args) => {
    return simpleString("PONG");
  },

  SET: (args, { cache }) => {
    if (args.length < 2) {
      throw new Error("wrong number of arguments for 'set'");
    }
    const [key, value, option, ttlRaw] = args;
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    let ttl: number | undefined;
    if (option && ttlRaw) {
      if (typeof ttlRaw === "string" && !isStrictNumber(ttlRaw)) {
        throw new Error("invalid expire time");
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
    return simpleString("OK");
  },

  GET: (args, { cache }) => {
    if (args.length < 1) {
      throw new Error("wrong number of arguments for 'get'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    return cache.get(key) ?? null;
  },

  RPUSH: (args, { cache, blocked }) => {
    if (args.length < 2) {
      throw new Error("wrong number of arguments for 'rpush'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    let value = cache.get(key) ?? null;
    if (value && !Array.isArray(value)) {
      throw new RedisError(
        "WRONGTYPE",
        "Operation against a key holding the wrong kind of value",
      );
    }
    value = Array.isArray(value) ? value : [];
    for (let i = 1; i < args.length; i++) {
      value.push(args[i]);
    }
    cache.set(key, value);
    const output = value.length;
    wakeBlockedListClients(key, cache, blocked);
    return output;
  },

  LRANGE: (args, { cache }) => {
    if (args.length !== 3) {
      throw new Error("wrong number of arguments for 'lrange'");
    }
    const [key, startArg, stopArg] = args;
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    const value = cache.get(key);
    if (value == null) {
      return [];
    }
    if (!Array.isArray(value)) {
      throw new RedisError(
        "WRONGTYPE",
        "Operation against a key holding the wrong kind of value",
      );
    }
    if (typeof startArg === "string" && typeof stopArg === "string") {
      if (!isStrictNumber(startArg) || !isStrictNumber(stopArg)) {
        throw new Error("value is not an integer or out of range");
      }
      let start = parseInt(startArg, 10);
      let stop = parseInt(stopArg, 10);
      const len = value.length;
      if (start < 0) start = len + start;
      if (stop < 0) stop = len + stop;
      start = Math.max(start, 0);
      stop = Math.min(stop, len - 1);
      if (start > stop || start >= len) {
        return [];
      }
      const result = value.slice(start, stop + 1);
      return result;
    }
  },

  LPUSH: (args, { cache, blocked }) => {
    if (args.length < 2) {
      throw new Error("wrong number of arguments for 'lpush'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    let value = cache.get(key) ?? null;
    if (value && !Array.isArray(value)) {
      throw new RedisError(
        "WRONGTYPE",
        "Operation against a key holding the wrong kind of value",
      );
    }
    value = Array.isArray(value) ? value : [];
    for (let i = 1; i < args.length; i++) {
      value.unshift(args[i]);
    }
    cache.set(key, value);
    const output = value.length;
    wakeBlockedListClients(key, cache, blocked);
    return output;
  },

  LLEN: (args, { cache }) => {
    if (args.length < 1) {
      throw new Error("wrong number of arguments for 'llen'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    let value = cache.get(key) ?? null;
    if (value && !Array.isArray(value)) {
      throw new RedisError(
        "WRONGTYPE",
        "Operation against a key holding the wrong kind of value",
      );
    }
    value = Array.isArray(value) ? value : [];
    return value.length;
  },

  LPOP: (args, { cache }) => {
    if (args.length < 1) {
      throw new Error("wrong number of arguments for 'llop'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    let value = cache.get(key) ?? null;
    if (value && !Array.isArray(value)) {
      throw new RedisError(
        "WRONGTYPE",
        "Operation against a key holding the wrong kind of value",
      );
    }
    value = Array.isArray(value) ? value : [];
    if (value.length === 0) {
      return null;
    }
    if (args.length === 1) {
      return value.shift() || null;
    }
    if (args.length === 2) {
      let amount = args[1];
      if (typeof amount === "string") {
        if (!isStrictNumber(amount)) {
          throw new Error("value is not an integer or out of range");
        }
        amount = parseInt(amount);
        if (amount === 0) {
          return [];
        }
        const output: TRespData[] = [];
        while (amount !== 0) {
          output.push(value.shift() || null);
          amount--;
        }
        return output;
      }
    }
  },

  BLPOP: (args, { socket, cache, blocked }) => {
    if (args.length < 2) {
      throw new Error("wrong number of arguments for 'blpop'");
    }
    const timeout = parseFloat(String(args[args.length - 1]));
    const keys = args.slice(0, -1);
    if (isNaN(timeout)) {
      throw new Error("timeout is not a number");
    }
    for (const key of keys) {
      if (typeof key === "string") {
        let value = cache.get(key) ?? null;
        if (value && !Array.isArray(value)) {
          throw new RedisError(
            "WRONGTYPE",
            "Operation against a key holding the wrong kind of value",
          );
        }
        if (Array.isArray(value) && value.length > 0) {
          const element = value.shift();
          return [key, element ?? null];
        }
      }
    }
    let resolved = false;
    let timer: NodeJS.Timeout | null = null;
    const unblock = (key?: string, element?: TRespData) => {
      if (resolved) return;
      resolved = true;
      if (timer) clearTimeout(timer);
      if (key !== undefined && element !== undefined) {
        socket.write(RespEncoder.encode([key, element]));
      } else {
        socket.write("*-1\r\n");
      }
    };
    for (const key of keys) {
      if (typeof key === "string") {
        if (!blocked.has(key)) blocked.set(key, []);
        blocked.get(key)?.push({ socket, unblock });
      }
    }
    if (timeout > 0) {
      timer = setTimeout(() => {
        unblock();
      }, timeout * 1000);
    }
    return;
  },

  TYPE: (args, { cache }) => {
    if (args.length < 1) {
      throw new Error("wrong number of arguments for 'type'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    let value = cache.get(key) ?? null;
    if (!value) {
      return simpleString("none");
    }
    if (value instanceof Stream) {
      return simpleString("stream");
    }
    return simpleString("string");
  },

  XADD: (args, { cache, blocked }) => {
    if (args.length < 3) {
      throw new Error("wrong number of arguments for 'xadd'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
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
              throw new Error(
                "The ID specified in XADD must be greater than 0-0",
              );
            }
            if (lastTimestampNum > newTimestampNum) {
              throw new Error(
                "The ID specified in XADD is equal or smaller than the target stream top item",
              );
            } else if (
              lastTimestampNum === newTimestampNum &&
              lastSeqNum >= newSeqNum
            ) {
              throw new Error(
                "The ID specified in XADD is equal or smaller than the target stream top item",
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
      wakeBlockedStreamsClients(key, blocked);
      return id;
    } else {
      throw new RedisError(
        "WRONGTYPE",
        "Operation against a key holding the wrong kind of value",
      );
    }
  },

  XRANGE: (args, { socket, cache }) => {
    if (args.length < 3) {
      throw new Error("wrong number of arguments for 'xrange'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    const value = cache.get(key) ?? null;
    if (!value || !(value instanceof Stream)) {
      throw new RedisError(
        "WRONGTYPE",
        "Operation against a key holding the wrong kind of value",
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
      return output;
    }
    throw new Error("invalid range arguments");
  },

  XREAD: (args, { socket, cache, blocked }) => {
    if (args.length < 3) {
      throw new Error("wrong number of arguments for 'xread'");
    }
    let i = 0;
    let timeout: number | null = null;
    if (String(args[i])?.toUpperCase() === "BLOCK") {
      timeout = parseFloat(String(args[i + 1]));
      if (isNaN(timeout)) {
        throw new Error("timeout is not a number");
      }
      i += 2;
    }
    if (String(args[i])?.toUpperCase() !== "STREAMS") {
      throw new Error("syntax error");
    }
    i++;
    const remaining = args.length - i;
    if (remaining % 2 !== 0) {
      throw new Error("syntax error");
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
          throw new RedisError(
            "WRONGTYPE",
            "Operation against a key holding the wrong kind of value",
          );
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
    if (immediate && immediate.length > 0) {
      return immediate;
    }
    if (timeout === null) {
      return {
        type: "null-array",
      };
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
        socket.write(RespEncoder.encode(res));
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
        socket.write("*-1\r\n");
      }, timeout);
    }
  },

  INCR: (args, { cache }) => {
    if (args.length < 1) {
      throw new Error("wrong number of arguments for 'incr'");
    }
    const key = args[0];
    if (typeof key !== "string") {
      throw new Error("invalid key");
    }
    const val = cache.get(key);
    if (val && typeof val === "string" && !isStrictNumber(val)) {
      throw new Error("value is not an integer or out of range");
    }
    let newVal = 1;
    if (val) {
      newVal = Number(val) + 1;
    }
    cache.set(key, newVal.toString());
    return newVal;
  },

  MULTI: (args, { setIsMulti }) => {
    if (args.length > 0) {
      throw new Error("wrong number of arguments for 'multi'");
    }
    if (setIsMulti) {
      setIsMulti(true);
    }
    return simpleString("OK");
  },

  EXEC: async (args, ctx) => {
    if (args.length > 0) {
      throw new Error("wrong number of arguments for 'exec'");
    }
    if (!ctx.isMulti) {
      throw new Error("EXEC without MULTI");
    }
    if (ctx.setIsMulti) {
      ctx.setIsMulti(false);
    }
    const output: TRespData[] = [];
    if (ctx.cmdQueue) {
      for (const { handler, args } of ctx.cmdQueue) {
        try {
          const res = await handler(args, ctx);
          output.push(res ?? null);
        } catch (err) {
          const message = err instanceof Error ? err.message : "unknown error";
          output.push(new RedisError("ERR", message));
        }
      }
    }

    return output;
  },

  DISCARD: (args, { isMulti, setIsMulti, cmdQueue }) => {
    if (args.length > 0) {
      throw new Error("wrong number of arguments for 'exec'");
    }
    if (!isMulti) {
      throw new Error("DISCARD without MULTI");
    }
    if (setIsMulti) {
      setIsMulti(false);
    }
    if (cmdQueue) {
      cmdQueue = [];
    }
    return simpleString("OK");
  },

  INFO: (args, { myMaster, replicationId, replicationOffset }) => {
    if (args.length === 0) {
      let output = `# Replication\r\nrole:${myMaster ? "slave" : "master"}\r\n`;
      if (!myMaster) {
        output = output.concat(
          `master_replid:${replicationId}\r\nmaster_repl_offset:${replicationOffset}\r\n`,
        );
      }
      return output;
    }
    if (
      args.length === 1 &&
      typeof args[0] === "string" &&
      args[0].toUpperCase() === "REPLICATION"
    ) {
      let output = `# Replication\r\nrole:${myMaster ? "slave" : "master"}\r\n`;
      if (!myMaster) {
        output = output.concat(
          `master_replid:${replicationId}\r\nmaster_repl_offset:${replicationOffset}\r\n`,
        );
      }
      return output;
    }
    throw new Error("unsupported INFO section");
  },

  REPLCONF: (
    args,
    {
      socket,
      myMaster,
      mySlaves,
      masterReplicationOffset,
      masterOffsetBeforeCommand,
    },
  ) => {
    if (!args.length || (args.length === 1 && args[0] === "listening-port")) {
      throw new Error("wrong number of arguments for 'replconf'");
    }
    if (args[0] === "listening-port") {
      const replicaPort = args[1];
      const replicaId = `${socket.remoteAddress}:${replicaPort}`;
      if (!mySlaves?.has(replicaId)) {
        mySlaves?.set(replicaId, socket);
      }
    }
    if (
      myMaster &&
      args[0] === "GETACK" &&
      args.length === 2 &&
      masterReplicationOffset !== null &&
      masterReplicationOffset !== undefined
    ) {
      const geTackArg = args[1];
      if (geTackArg === "*") {
        const offset = masterOffsetBeforeCommand;
        return ["REPLCONF", "ACK", offset?.toString() || 0];
      }
    } else if (!myMaster && args[0] === "GETACK") {
      throw new Error("NOT SUPPORTED");
    } else if (args[0] === "ACK") {
      (socket as any).replOffset = Number(args[1]);
      return undefined;
    }
    return simpleString("OK");
  },

  PSYNC: (_args, { socket, replicationId, myMaster }) => {
    if (!myMaster) {
      socket.write(`+FULLRESYNC ${replicationId} 0\r\n`);
      const emptyRdb = readFileSync("empty.rdb");
      socket.write(`$${emptyRdb.length}\r\n`);
      socket.write(emptyRdb);
      return;
    }
    throw new Error("unsupported PSYNC section for slave");
  },

  WAIT: (args, { myMaster, mySlaves, replicationOffset }) => {
    if (myMaster) {
      throw new Error("unsupported WAIT for slave");
    }
    if (args.length !== 2) {
      throw new Error("wrong number of arguments for 'wait'");
    }
    const numReplicasTarget = Number(args[0]);
    const timeout = Number(args[1]);
    if (
      replicationOffset === 0 ||
      !mySlaves ||
      mySlaves.size === 0 ||
      numReplicasTarget === 0
    ) {
      return mySlaves?.size || 0;
    }
    let ackedCount = 0;
    for (const slaveSocket of mySlaves.values()) {
      if (((slaveSocket as any).replOffset || 0) >= (replicationOffset || 0)) {
        ackedCount++;
      }
    }
    if (ackedCount >= numReplicasTarget) {
      return ackedCount;
    }
    const getackCmd = RespEncoder.encode(["REPLCONF", "GETACK", "*"]);
    for (const slaveSocket of mySlaves.values()) {
      slaveSocket.write(getackCmd);
    }
    return new Promise((resolve) => {
      const targetOffset = replicationOffset || 0;
      let timer: NodeJS.Timeout | null = null;
      const check = () => {
        let count = 0;
        for (const slaveSocket of mySlaves.values()) {
          if (((slaveSocket as any).replOffset || 0) >= targetOffset) {
            count++;
          }
        }
        if (count >= numReplicasTarget) {
          if (timer) clearTimeout(timer);
          clearInterval(interval);
          resolve(count);
        }
      };
      const interval = setInterval(check, 10);
      if (timeout > 0) {
        timer = setTimeout(() => {
          clearInterval(interval);
          let count = 0;
          for (const slaveSocket of mySlaves.values()) {
            if (((slaveSocket as any).replOffset || 0) >= targetOffset) {
              count++;
            }
          }
          resolve(count);
        }, timeout);
      }
    });
  },

  CONFIG: (args, { dir, dbFileName }) => {
    if (args.length < 2) {
      throw new Error("wrong number of arguments for 'CONFIG'");
    }
    const arg1 = args[0];
    const configParam = args[1];

    if (arg1 === "GET") {
      if (configParam === "dir") {
        return ["dir", dir];
      } else if (configParam === "dbfilename") {
        return ["dbfilename", dbFileName];
      }
    }
    throw new Error("unsupported CONFIG section");
  },

  KEYS: async (args, { cache }) => {
    if (args.length !== 1) {
      throw new Error("wrong number of arguments for 'keys'");
    }
    const pattern = args[0];
    if (typeof pattern === "string") {
      if (pattern === "*") return [...cache.keys()];
      const regex = new RegExp(
        "^" +
          pattern
            .replace(/[.+^${}()|[\]\\]/g, "\\$&")
            .replace(/\*/g, ".*")
            .replace(/\?/g, ".") +
          "$",
      );
      return [...cache.keys()].filter((key) => regex.test(key));
    }
    throw new Error("unsupported keys section");
  },

  SUBSCRIBE: (args, { subscribedChannels }) => {
    if (args.length !== 1) {
      throw new Error("wrong number of arguments for 'subscribe'");
    }
    const channel = args[0];
    if (typeof channel === "string" && subscribedChannels) {
      subscribedChannels.push(channel);
      return ["subscribe", channel, subscribedChannels.length];
    }
    throw new Error("unsupported subscribe section");
  },
};

export const handlers: Record<string, TCommandHandler> = Object.fromEntries(
  Object.entries(rawHandlers).map(([cmd, handler]) => [
    cmd,
    safeHandler(handler),
  ]),
);
