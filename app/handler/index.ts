import RespEncoder from "../encoder/RespEncoder";
import { SUPPORTED_SUB_COMMANDS } from "../constants";
import type { CommandHandler } from "../types";

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
      if (typeof ttlRaw !== "number") {
        return socket.write(`-ERR invalid expire time\r\n`);
      }
      if (option === SUPPORTED_SUB_COMMANDS.EX) {
        ttl = ttlRaw * 1000;
      } else if (option === SUPPORTED_SUB_COMMANDS.PX) {
        ttl = ttlRaw;
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
};
