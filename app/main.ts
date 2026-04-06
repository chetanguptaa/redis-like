import * as net from "net";
import { parseArgs } from "node:util";
import RespParser from "./parser/RespParser";
import type {
  ICommandContext,
  TBlocked,
  TCMDQueueElem,
  TRespData,
  TStage,
} from "./types";
import { executeCommand } from "./cmd-exectutor";
import RespEncoder from "./encoder/RespEncoder";
import RespDecoder from "./decoder/RespDecoder";

const { values } = parseArgs({
  options: {
    port: { type: "string" },
    replicaof: { type: "string" },
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
      const result = RespDecoder.tryDecode(buffer);
      if (!result) break;
      const { value, rest } = result;
      buffer = rest;
      handleResponse(value);
    }
  });

  function handleResponse(response: any) {
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
  if (ctx.replicationOffset) {
    ctx.replicationOffset += Buffer.byteLength(encoded);
  }
}

class RedisServer {
  public redisPort: number | null = null;
  public server: net.Server;
  public cache = new Map<string, TRespData>();
  public blocked = new Map<string, Array<TBlocked>>();
  public replicationId: string | null = null;
  public replicationOffset: number | null = null;
  public masterReplicationId: string | null = null;
  public masterReplicationOffset: number | null = null;
  public myMaster: string | null = null;
  public mySlaves = new Map<string, net.Socket>();

  constructor(
    private port: number = 6379,
    replicaOf: string | null = null,
  ) {
    this.redisPort = port;
    this.server = net.createServer(this.handleConnection.bind(this));
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
    const parser = new RespParser();
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
          replicationOffset: this.replicationOffset,
          mySlaves: this.mySlaves,
          port: this.redisPort,
        });
      }
    });
    socket.on("error", (err) => {
      console.error("Socket error:", err.message);
      socket.destroy();
    });
  }
}

new RedisServer(Number(values.port) || 6379, values.replicaof).start();
