import { readFileSync } from "node:fs";
import { SET_OPTIONS } from "../constants";
import Stream, { type TEntry } from "../data-structures/Stream";
import RespEncoder from "../encoder/RespEncoder";
import RedisError from "../error";
import type { TCommandHandler, TGeoEntry, TRespData, TZSet } from "../types";
import {
  decodeGeohash,
  encodeGeohash,
  geohashGetDistance,
  isBigIntString,
  isStrictNumber,
  safeHandler,
  simpleString,
  wakeBlockedListClients,
  wakeBlockedStreamsClients,
} from "../utils";
import { MinHeap } from "../data-structures/MinHeap";
import { createHash } from "node:crypto";

export const rawHandlers: Record<string, TCommandHandler> = {
  ECHO: (args) => {
    if (args.length < 1) {
      throw new Error("wrong number of arguments for 'echo'");
    }
    return args[0];
  },

  PING: (_args, { isSubscribeMode }) => {
    if (isSubscribeMode) {
      return ["pong", ""];
    }
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

  SUBSCRIBE: (
    args,
    {
      subscribedChannels,
      setIsSubscribeMode,
      channelsToSubscribersMap,
      socket,
    },
  ) => {
    if (args.length !== 1) {
      throw new Error("wrong number of arguments for 'subscribe'");
    }
    const channel = args[0];
    if (
      typeof channel === "string" &&
      subscribedChannels &&
      setIsSubscribeMode &&
      channelsToSubscribersMap
    ) {
      setIsSubscribeMode(true);
      if (!subscribedChannels.includes(channel)) {
        subscribedChannels.push(channel);
      }
      if (channelsToSubscribersMap.has(channel)) {
        const subscribers = channelsToSubscribersMap.get(channel);
        if (subscribers) {
          if (!subscribers.includes(socket)) {
            subscribers.push(socket);
            channelsToSubscribersMap.set(channel, subscribers);
          }
        }
      } else {
        channelsToSubscribersMap.set(channel, [socket]);
      }
      return ["subscribe", channel, subscribedChannels.length];
    }
    throw new Error("unsupported subscribe section");
  },

  PUBLISH: (args, { subscribedChannels, channelsToSubscribersMap, socket }) => {
    if (args.length !== 2) {
      throw new Error("wrong number of arguments for 'publish'");
    }
    const channel = args[0];
    const content = args[1];
    if (
      typeof channel === "string" &&
      subscribedChannels &&
      channelsToSubscribersMap
    ) {
      if (channelsToSubscribersMap.has(channel)) {
        const subscribers = channelsToSubscribersMap.get(channel);
        if (subscribers) {
          const publishedContent = RespEncoder.encode([
            "message",
            channel,
            content,
          ]);
          for (const subscriberSocket of subscribers.values()) {
            subscriberSocket.write(publishedContent);
          }
        }
        return channelsToSubscribersMap.get(channel)?.length;
      }
    }
    throw new Error("unsupported publish section");
  },

  UNSUBSCRIBE: (
    args,
    { subscribedChannels, channelsToSubscribersMap, socket },
  ) => {
    if (args.length !== 1) {
      throw new Error("wrong number of arguments for 'unsubscribe'");
    }
    const channel = args[0];
    if (
      typeof channel === "string" &&
      subscribedChannels &&
      channelsToSubscribersMap
    ) {
      subscribedChannels.splice(
        0,
        subscribedChannels.length,
        ...subscribedChannels.filter((sc) => sc !== channel),
      );
      const subscribers = channelsToSubscribersMap.get(channel);
      if (subscribers) {
        const updatedSubscribers = subscribers.filter((s) => s !== socket);
        if (updatedSubscribers.length > 0) {
          channelsToSubscribersMap.set(channel, updatedSubscribers);
        } else {
          channelsToSubscribersMap.delete(channel); // optional cleanup
        }
      }
      return ["unsubscribe", channel, subscribedChannels.length];
    }
    throw new Error("unsupported unsubscribe section");
  },

  ZADD: (args, { zCache, geoCache }) => {
    if (args.length !== 3) {
      throw new Error("wrong number of arguments for 'zadd'");
    }
    const key = args[0] as string;
    let score = args[1];
    const value = args[2] as string;
    if ((!zCache && !geoCache) || typeof score !== "string") {
      throw new Error("unsupported zadd section");
    }
    if (isBigIntString(score as string) && geoCache) {
      let heap = geoCache.get(key);
      if (!heap) {
        heap = new MinHeap<TGeoEntry>();
        geoCache.set(key, heap);
      }
      const existingIndex = heap.findByField("member", value);
      score = BigInt(score);
      if (existingIndex !== -1) {
        const { latitude, longitude } = decodeGeohash(score);
        heap.updateScore(existingIndex, score);
        (heap.get(existingIndex) as TGeoEntry).lat = latitude;
        (heap.get(existingIndex) as TGeoEntry).lon = longitude;
        return 0;
      } else {
        const { latitude, longitude } = decodeGeohash(score);
        heap.insert({ member: value, lat: longitude, lon: latitude, score });
        return 1;
      }
    }
    if (!zCache) throw new Error("unsupported zadd section");
    let heap = zCache.get(key);
    if (!heap) {
      heap = new MinHeap();
      zCache.set(key, heap);
    }
    const existingIndex = heap.findByField("value", value);
    if (existingIndex !== -1) {
      heap.updateScore(existingIndex, Number(score));
      return 0;
    } else {
      heap.insert({ value, score: Number(score) });
      return 1;
    }
  },

  ZRANK: (args, { zCache }) => {
    if (args.length !== 2) {
      throw new Error("wrong number of arguments for 'zrank'");
    }
    const key = args[0] as string;
    const value = args[1] as string;
    if (!zCache) throw new Error("unsupported zrank section");
    const heap = zCache.get(key);
    if (!heap || heap.size() === 0) return null;
    const sorted = heap.toSortedArray();
    const rank = sorted.findIndex((item) => item.value === value);
    return rank === -1 ? null : rank;
  },

  ZRANGE: (args, { zCache, geoCache }) => {
    if (args.length < 3) {
      throw new Error("wrong number of arguments for 'zrange'");
    }
    const key = args[0] as string;
    let start = Number(args[1]);
    let stop = Number(args[2]);
    const withScores = args[4]?.toString().toUpperCase() === "WITHSCORES";
    if (!zCache && !geoCache) throw new Error("unsupported zrange section");
    let heap: MinHeap<TZSet> | MinHeap<TGeoEntry> | undefined =
      zCache?.get(key);
    let getName: (item: any) => string = (item) => item.value;
    if (!heap && geoCache) {
      heap = geoCache.get(key);
      getName = (item) => item.member;
    }
    if (!heap || heap.size() === 0) return [];
    const sorted = heap.toSortedArray();
    const len = sorted.length;
    if (start < 0) start = Math.max(0, len + start);
    if (stop < 0) stop = len + stop;
    if (start > stop || start >= len) return [];
    stop = Math.min(stop, len - 1);
    const slice = sorted.slice(start, stop + 1);
    if (withScores) {
      return slice.flatMap((item) => [getName(item), String(item.score)]);
    }
    return slice.map((item) => getName(item));
  },

  ZCARD: (args, { zCache }) => {
    if (args.length !== 1) {
      throw new Error("wrong number of arguments for 'zcard'");
    }
    const key = args[0] as string;
    if (!zCache) throw new Error("unsupported zcard section");
    const heap = zCache.get(key);
    if (!heap) return 0;
    return heap.size();
  },

  ZSCORE: (args, { zCache, geoCache }) => {
    if (args.length !== 2) {
      throw new Error("wrong number of arguments for 'zscore'");
    }
    const key = args[0] as string;
    const value = args[1] as string;
    if (!zCache && !geoCache) throw new Error("unsupported zscore section");
    let index = -1;
    let heap: MinHeap<TZSet> | MinHeap<TGeoEntry> | undefined =
      zCache?.get(key);
    if (heap) {
      index = heap.findByField("value", value);
    } else if (geoCache) {
      heap = geoCache.get(key);
      if (heap) index = heap.findByField("member", value);
    }
    if (!heap || heap.size() === 0) return null;
    return index === -1 ? null : String(heap.getScore(index));
  },

  ZREM: (args, { zCache }) => {
    if (args.length !== 2) {
      throw new Error("wrong number of arguments for 'zrem'");
    }
    const key = args[0] as string;
    const value = args[1] as string;
    if (!zCache) throw new Error("unsupported zrem section");
    const heap = zCache.get(key);
    if (!heap || heap.size() === 0) return 0;
    const index = heap.findByField("value", value);
    if (index === -1) return 0;
    heap.remove(index);
    return 1;
  },

  GEOADD: (args, { geoCache }) => {
    if (args.length < 4 || (args.length - 1) % 3 !== 0) {
      throw new Error("wrong number of arguments for 'geoadd'");
    }
    const key = args[0] as string;
    if (!geoCache) throw new Error("unsupported geoadd section");
    let heap = geoCache.get(key);
    if (!heap) {
      heap = new MinHeap<TGeoEntry>();
      geoCache.set(key, heap);
    }
    let added = 0;
    for (let i = 1; i < args.length; i += 3) {
      const lon = Number(args[i]);
      const lat = Number(args[i + 1]);
      const member = args[i + 2] as string;
      if (lat < -85.05112878 || lat > 85.05112878) {
        throw new Error(`invalid latitude ${lat}`);
      }
      if (lon < -180 || lon > 180) {
        throw new Error(`invalid longitude ${lon}`);
      }
      const score = encodeGeohash(lat, lon);
      const existingIndex = heap.findByField("member", member);
      if (existingIndex !== -1) {
        heap.updateScore(existingIndex, score);
        (heap.get(existingIndex) as TGeoEntry).lat = lat;
        (heap.get(existingIndex) as TGeoEntry).lon = lon;
      } else {
        heap.insert({ member, lat, lon, score });
        added++;
      }
    }
    return added;
  },

  GEOPOS: (args, { geoCache }) => {
    if (args.length < 2) {
      throw new Error("wrong number of arguments for 'geopos'");
    }
    if (!geoCache) throw new Error("unsupported geopos section");
    const key: string = args[0] as string;
    const places = args.slice(1);
    const response: TRespData = [];
    const heap = geoCache.get(key);
    if (!heap) {
      for (let i = 0; i < places.length; i++) {
        response.push({
          type: "null-array",
        });
      }
    } else {
      for (const place of places) {
        let arr: TRespData = {
          type: "null-array",
        };
        const idx = heap.findByField("member", place as string);
        if (idx !== -1) {
          const { lat, lon } = heap.get(idx);
          arr = [];
          arr.push(lat.toString());
          arr.push(lon.toString());
        }
        response.push(arr);
      }
    }
    return response;
  },

  GEODIST: (args, { geoCache }) => {
    if (args.length !== 3) {
      throw new Error("wrong number of arguments for 'geodist'");
    }
    if (!geoCache) throw new Error("unsupported geodist section");
    const key: string = args[0] as string;
    const place1 = args[1];
    const place2 = args[2];
    const heap = geoCache.get(key);
    if (!heap) return null;
    const idx1 = heap.findByField("member", place1 as string);
    const idx2 = heap.findByField("member", place2 as string);
    if (idx1 === -1 || idx2 === -1) return null;
    const { score: score1 } = heap.get(idx1);
    const { score: score2 } = heap.get(idx2);
    const { latitude: lat1, longitude: long1 } = decodeGeohash(score1);
    const { latitude: lat2, longitude: long2 } = decodeGeohash(score2);
    const distance = geohashGetDistance(long1, lat1, long2, lat2);
    return distance.toString();
  },

  GEOSEARCH: (args, { geoCache }) => {
    if (args.length < 6) {
      throw new Error("wrong number of arguments for 'geosearch'");
    }
    if (!geoCache) throw new Error("unsupported geosearch section");
    const key = args[0] as string;
    const heap = geoCache.get(key);
    if (!heap) return [];
    let lon: number;
    let lat: number;
    let idx = 1;
    if (args[idx] === "FROMLONLAT") {
      lon = Number(args[idx + 1]);
      lat = Number(args[idx + 2]);
      idx += 3;
    } else {
      throw new Error("unsupported GEOSEARCH option");
    }
    let radius: number;
    if (args[idx] === "BYRADIUS") {
      radius = Number(args[idx + 1]);
      const unit = args[idx + 2] as string;
      if (unit === "km") radius *= 1000;
      else if (unit === "mi") radius *= 1609.344;
      else if (unit === "ft") radius *= 0.3048;
      idx += 3;
    } else {
      throw new Error("unsupported GEOSEARCH option");
    }
    const results: string[] = [];
    for (let i = 0; i < heap.size(); i++) {
      const entry = heap.get(i) as TGeoEntry;
      const { latitude: memberLat, longitude: memberLon } = decodeGeohash(
        entry.score,
      );
      const distance = geohashGetDistance(lon, lat, memberLon, memberLat);
      if (distance <= radius) {
        results.push(entry.member);
      }
    }
    return results;
  },

  ACL: async (args, { users }) => {
    if (args.length < 1) {
      throw new Error("wrong number of arguments for 'acl'");
    }
    const arg = args[0];
    if (typeof arg === "string") {
      if (arg === "WHOAMI") {
        return "default";
      }
      if (arg === "GETUSER") {
        const username = args[1] as string;
        if (!users) throw new Error("unsupported acl section");
        if (!users.has(username)) return null;
        const userPasswords = users.get(username);
        const flags = [];
        const passwords = [];
        if (!userPasswords?.length) {
          flags.push("nopass");
        } else {
          passwords.push(...userPasswords);
        }
        return ["flags", flags, "passwords", passwords];
      }
      if (arg === "SETUSER") {
        if (!users) throw new Error("unsupported acl section");
        const username = args[1] as string;
        const value = args[2] as string;
        if (value.startsWith(">")) {
          const password = value.slice(1);
          const encoder = new TextEncoder();
          const data = encoder.encode(password);
          const hashBuffer = await crypto.subtle.digest("SHA-256", data);
          const hashArray = Array.from(new Uint8Array(hashBuffer));
          const hashHex = hashArray
            .map((b) => b.toString(16).padStart(2, "0"))
            .join("");
          users.set(username, [...(users.get(username) || []), hashHex]);
          return simpleString("OK");
        }
        if (value === "nopass" || value === "on" || value === "off") {
          if (!users.has(username)) {
            users.set(username, []);
          }
          return simpleString("OK");
        }
      }
    }
    throw new Error("Unsupported acl section");
  },

  AUTH: async (args, { users, setIsAuthenticated, setCurrentUser }) => {
    if (args.length < 2) {
      throw new Error("wrong number of arguments for 'auth'");
    }
    if (!users) throw new Error("unsupported auth section");
    const username = args[0] as string;
    const password = args[1] as string;
    const storedPasswords = users.get(username);
    if (!storedPasswords || storedPasswords.length === 0) {
      throw new RedisError(
        "WRONGPASS",
        "invalid username-password pair or user is disabled.",
      );
    }
    const hashHex = createHash("sha256").update(password).digest("hex");
    for (let i = 0; i < storedPasswords.length; i++) {
      const storedHash = storedPasswords[i];
      if (hashHex === storedHash) {
        if (setCurrentUser && setIsAuthenticated) {
          setCurrentUser(username);
          setIsAuthenticated(true);
        }
        return simpleString("OK");
      }
    }
    throw new RedisError(
      "WRONGPASS",
      "invalid username-password pair or user is disabled.",
    );
  },
};

export const handlers: Record<string, TCommandHandler> = Object.fromEntries(
  Object.entries(rawHandlers).map(([cmd, handler]) => [
    cmd,
    safeHandler(handler),
  ]),
);
