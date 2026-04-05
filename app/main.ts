import * as net from "net";
import { parseArgs } from "node:util";
import RespParser from "./parser/RespParser";
import type { TBlocked, TCMDQueueElem, TRespData } from "./types";
import { executeCommand } from "./cmd-exectutor";
import RespEncoder from "./encoder/RespEncoder";
import RespDecoder from "./decoder/RespDecoder";

const { values } = parseArgs({
  options: {
    port: { type: "string" },
    replicaof: { type: "string" },
  },
});

type Stage =
  | "PING"
  | "REPLCONF1"
  | "REPLCONF2"
  | "PSYNC"
  | "FULLRESYNC"
  | "RDB"
  | "STREAM";

export function connectToMaster(replicaOf: string, port: number) {
  const [masterHost, masterPort] = replicaOf.split(" ");
  const socket = net.connect(Number(masterPort), masterHost);
  let stage: Stage = "PING";
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
      const result = RespDecoder.tryDecode(buffer);
      if (!result) return;
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
        stage = "FULLRESYNC";
      }
    } else if (response?.type === "bulk") {
      if (stage === "FULLRESYNC") {
        rdbBytesExpected = response.length;
        rdbBytesReceived = 0;
        stage = "RDB";
      }
    } else if (response?.type === "array") {
      if (stage === "STREAM") {
      }
    }
  }
}

class RedisServer {
  private server: net.Server;
  private cache = new Map<string, TRespData>();
  private blocked = new Map<string, Array<TBlocked>>();
  private replicaOf: string | null = null;
  private replicationId: string | null = null;
  private replicationOffset: number | null = null;

  constructor(
    private port: number = 6379,
    replicaOf: string | null = null,
  ) {
    this.server = net.createServer(this.handleConnection.bind(this));
    if (replicaOf) {
      this.replicaOf = replicaOf;
      connectToMaster(replicaOf, port);
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
          replicaOf: this.replicaOf,
          replicationId: this.replicationId,
          replicationOffset: this.replicationOffset,
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
