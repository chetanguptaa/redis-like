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
      const [masterHost, masterPort] = replicaOf.split(" ");
      const toMasterConnection = net.connect(Number(masterPort), masterHost);
      toMasterConnection.write(RespEncoder.encode(["PING"]));
      let stage: "PING" | "REPLCONF1" | "REPLCONF2" | "PSYNC" = "PING";
      toMasterConnection.on("data", (chunk) => {
        const response = RespDecoder.decode(chunk.toString());
        if (
          typeof response === "object" &&
          response !== null &&
          "__simple" in response
        ) {
          const value = response.value;
          if (value === "PONG" && stage === "PING") {
            stage = "REPLCONF1";
            toMasterConnection.write(
              RespEncoder.encode([
                "REPLCONF",
                "listening-port",
                port.toString(),
              ]),
            );
            toMasterConnection.write(
              RespEncoder.encode(["REPLCONF", "capa", "psync2"]),
            );
          } else if (value === "OK" && stage === "REPLCONF1") {
            stage = "REPLCONF2";
          } else if (value === "OK" && stage === "REPLCONF2") {
            stage = "PSYNC";
            toMasterConnection.write(RespEncoder.encode(["PSYNC", "?", "-1"]));
          }
        }
      });
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
