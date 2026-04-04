import { SET_OPTIONS } from "../constants";
import RespEncoder from "../encoder/RespEncoder";
import type { CommandHandler, TRespData } from "../types";
import { isStrictNumber } from "../utils";

export const handlers: Record<string, CommandHandler> = {
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

  RPUSH: (args, { socket, cache }) => {
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
    value.push(args[1]);
    cache.set(key, value);
    socket.write(RespEncoder.encode(value.length));
  },
};
