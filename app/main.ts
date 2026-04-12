import * as net from "net";
import { parseArgs } from "node:util";
import RespParser from "./parser/RespParser";
import type {
  ICommandContext,
  TBlocked,
  TCMDQueueElem,
  TGeoEntry,
  TRespData,
  TStage,
  TZSet,
} from "./types";
import { executeCommand } from "./cmd-exectutor";
import RespEncoder from "./encoder/RespEncoder";
import RespDecoder from "./decoder/RespDecoder";
import path from "node:path";
import * as fs from "fs";
import type { MinHeap } from "./data-structures/MinHeap";

const { values } = parseArgs({
  options: {
    port: { type: "string" },
    replicaof: { type: "string" },
    dir: { type: "string" },
    dbfilename: { type: "string" },
  },
});

export function connectToMaster(
  myMaster: string,
  port: number,
  server: RedisServer,
) {
  const [masterHost, masterPort] = myMaster.split(" ");
  const socket = net.connect(Number(masterPort), masterHost);
  let stage: TStage = "PING";
  let buffer: Buffer<ArrayBufferLike> = Buffer.alloc(0);
  let rdbBytesExpected = 0;
  let rdbBytesReceived = 0;
  socket.on("connect", () => {
    socket.write(RespEncoder.encode(["PING"]));
  });
  socket.on("data", (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);
    while (true) {
      if (stage === "RDB") {
        const remaining = rdbBytesExpected - rdbBytesReceived;
        if (buffer.length < remaining) {
          rdbBytesReceived += buffer.length;
          buffer = Buffer.alloc(0);
          return;
        }
        const rdbChunk = buffer.subarray(0, remaining);
        buffer = buffer.subarray(remaining);
        rdbBytesReceived += rdbChunk.length;
        console.log("RDB received complete");
        stage = "STREAM";
        continue;
      }
      if (stage === "FULLRESYNC") {
        const idx = buffer.indexOf("\r\n");
        if (idx === -1) return;
        const header = buffer.subarray(0, idx).toString();
        if (!header.startsWith("$")) {
          throw new Error("Expected RDB bulk length");
        }
        rdbBytesExpected = Number(header.slice(1));
        rdbBytesReceived = 0;
        buffer = buffer.subarray(idx + 2);
        stage = "RDB";
        continue;
      }
      const beforeLength = buffer.length;
      const result = RespDecoder.tryDecode(buffer);
      if (!result) break;
      const { value, rest } = result;
      const consumed = beforeLength - rest.length;
      if (server.masterReplicationOffset === null) {
        server.masterReplicationOffset = 0;
      }
      server.masterReplicationOffset += consumed;
      const offsetBefore = (server.masterReplicationOffset || 0) - consumed;
      buffer = rest;
      handleResponse(value, offsetBefore);
    }
  });

  function handleResponse(response: any, offsetBefore: number | null) {
    if (response?.type === "simple") {
      const value = response.value;
      if (value === "PONG" && stage === "PING") {
        stage = "REPLCONF1";
        socket.write(
          RespEncoder.encode(["REPLCONF", "listening-port", port.toString()]),
        );
      } else if (value === "OK" && stage === "REPLCONF1") {
        stage = "REPLCONF2";
        socket.write(RespEncoder.encode(["REPLCONF", "capa", "psync2"]));
      } else if (value === "OK" && stage === "REPLCONF2") {
        stage = "PSYNC";
        socket.write(RespEncoder.encode(["PSYNC", "?", "-1"]));
      } else if (value.startsWith("FULLRESYNC")) {
        const [, replId, offset] = value.split(" ");
        server.masterReplicationId = replId;
        server.masterReplicationOffset = Number(offset);
        stage = "FULLRESYNC";
      }
    } else if (response?.type === "array") {
      if (stage === "STREAM") {
        executeCommand(response.value, {
          socket,
          cache: server.cache,
          blocked: server.blocked,
          myMaster: server.myMaster,
          replicationId: server.replicationId,
          replicationOffset: server.replicationOffset,
          port: server.redisPort,
          isFromMaster: true,
          masterOffsetBeforeCommand: offsetBefore,
          masterReplicationOffset: server.masterReplicationOffset,
          dbFileName: server.dbFileName,
          dir: server.dir,
        });
      }
    }
  }
}

export function sendToReplica(message: TRespData, ctx: ICommandContext) {
  if (!ctx.mySlaves || ctx.mySlaves.size === 0) return;
  const encoded = RespEncoder.encode(message);
  for (const [id, socket] of ctx.mySlaves.entries()) {
    if (socket.destroyed) {
      ctx.mySlaves.delete(id);
      continue;
    }
    try {
      socket.write(encoded);
    } catch (err) {
      console.error(`Failed to write to replica ${id}:`, err);
      socket.destroy();
      ctx.mySlaves.delete(id);
    }
  }
  if (ctx.replicationOffset !== undefined && ctx.replicationOffset !== null) {
    ctx.replicationOffset += Buffer.byteLength(encoded);
  }
}

class RedisServer {
  public redisPort: number | null = null;
  public server: net.Server;
  public cache = new Map<string, TRespData>();
  public zCache = new Map<string, MinHeap<TZSet>>();
  public blocked = new Map<string, Array<TBlocked>>();
  public replicationId: string | null = null;
  public replicationOffset: number | null = null;
  public masterReplicationId: string | null = null;
  public masterReplicationOffset: number | null = null;
  public myMaster: string | null = null;
  public mySlaves = new Map<string, net.Socket>();
  public dir: string | null = null;
  public dbFileName: string | null = null;
  public channelsToSubscribersMap = new Map<string, net.Socket[]>();
  public geoCache = new Map<string, MinHeap<TGeoEntry>>();

  constructor(
    private port: number = 6379,
    replicaOf: string | null = null,
    dir: string | null = null,
    dbFileName: string | null = null,
  ) {
    this.redisPort = port;
    this.server = net.createServer(this.handleConnection.bind(this));
    this.dir = dir;
    this.dbFileName = dbFileName;
    if (dir !== null && dbFileName !== null) {
      const filePath = path.join(dir, dbFileName);
      if (fs.existsSync(filePath)) {
        const db = fs.readFileSync(filePath);
        let cursor = 0;

        // Helper: read a length-encoded integer, returns [value, newCursor]
        const readLength = (pos: number): [number, number] => {
          const first = db[pos];
          const type = (first & 0xc0) >> 6;
          if (type === 0) return [first & 0x3f, pos + 1]; // 6-bit length
          if (type === 1) return [((first & 0x3f) << 8) | db[pos + 1], pos + 2]; // 14-bit
          if (type === 2) return [db.readUInt32BE(pos + 1), pos + 5]; // 32-bit
          // type === 3: special encoding — return encoding kind, caller handles it
          return [-(first & 0x3f), pos + 1];
        };

        // Helper: read a length-prefixed string
        const readString = (pos: number): [string, number] => {
          const [len, next] = readLength(pos);
          if (len === 0) return [db.slice(next, next + len).toString(), next]; // empty
          if (len < 0) {
            // Special integer encodings: 0=int8, 1=int16, 2=int32
            if (len === -0)
              return [db.slice(next, next + 1).toString(), next + 1];
            if (len === -1) return [String(db.readInt8(next)), next + 1];
            if (len === -2) return [String(db.readInt16LE(next)), next + 2];
            if (len === -3) return [String(db.readInt32LE(next)), next + 4];
            return ["", next];
          }
          return [db.slice(next, next + len).toString(), next + len];
        };

        // Skip to the DB selector (0xFE)
        while (cursor < db.length && db[cursor] !== 0xfe) cursor++;
        if (cursor >= db.length) return;

        cursor++; // skip 0xFE
        cursor++; // skip db index byte

        // Expect 0xFB (resize db)
        if (db[cursor] === 0xfb) {
          cursor++;
          [, cursor] = readLength(cursor); // hash table size
          [, cursor] = readLength(cursor); // expiry hash table size
        }

        // Read key-value pairs until 0xFF (EOF) or next 0xFE (next DB)
        while (
          cursor < db.length &&
          db[cursor] !== 0xff &&
          db[cursor] !== 0xfe
        ) {
          let expiryMs: number | null = null;

          // Check for expiry
          if (db[cursor] === 0xfc) {
            // Expiry in milliseconds (8 bytes, little-endian)
            cursor++;
            expiryMs = Number(db.readBigUInt64LE(cursor));
            cursor += 8;
          } else if (db[cursor] === 0xfd) {
            // Expiry in seconds (4 bytes, little-endian)
            cursor++;
            expiryMs = db.readUInt32LE(cursor) * 1000;
            cursor += 4;
          }

          const valueType = db[cursor]; // 0x00 = string, others = list/set/etc.
          cursor++;

          const [key, afterKey] = readString(cursor);
          cursor = afterKey;
          const [value, afterVal] = readString(cursor);
          cursor = afterVal;

          // Skip already-expired keys
          if (expiryMs !== null && expiryMs < Date.now()) continue;

          // Only handle string type (0x00) for now
          if (valueType === 0x00) {
            this.cache.set(key, value as TRespData);
            // Optionally store expiry: you'd need an expiry map for this
          }
        }
      }
    }
    if (replicaOf) {
      this.myMaster = replicaOf;
      connectToMaster(replicaOf, port, this);
    } else {
      this.replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
      this.replicationOffset = 0;
    }
  }

  start() {
    this.server.listen(this.port, "127.0.0.1", () => {
      console.log(`Server running on 127.0.0.1:${this.port}`);
    });
    this.server.on("error", (err) => {
      console.error("Server error:", err);
    });
  }

  private handleConnection(socket: net.Socket) {
    let isMulti = false;
    const cmdQueue: TCMDQueueElem[] = [];
    const subscribedChannels: string[] = [];
    let isSubscribeMode: boolean = false;
    const parser = new RespParser();
    const self = this;
    socket.on("data", (chunk) => {
      const messages = parser.push(chunk.toString());
      for (const msg of messages) {
        executeCommand(msg, {
          socket,
          cache: this.cache,
          blocked: this.blocked,
          isMulti,
          setIsMulti: (value: boolean) => {
            isMulti = value;
          },
          cmdQueue,
          myMaster: this.myMaster,
          replicationId: this.replicationId,
          masterReplicationId: this.masterReplicationId,
          masterReplicationOffset: this.masterReplicationOffset,
          mySlaves: this.mySlaves,
          port: this.redisPort,
          isFromMaster: false,
          dir: this.dir,
          dbFileName: this.dbFileName,
          subscribedChannels,
          isSubscribeMode,
          channelsToSubscribersMap: this.channelsToSubscribersMap,
          zCache: this.zCache,
          geoCache: this.geoCache,
          setIsSubscribeMode: (value: boolean) => {
            isSubscribeMode = value;
          },

          get replicationOffset() {
            return self.replicationOffset;
          },
          set replicationOffset(val) {
            self.replicationOffset = val;
          },
        });
      }
    });
    socket.on("error", (err) => {
      console.error("Socket error:", err.message);
      socket.destroy();
    });
  }
}

new RedisServer(
  Number(values.port) || 6379,
  values.replicaof,
  values.dir,
  values.dbfilename,
).start();
