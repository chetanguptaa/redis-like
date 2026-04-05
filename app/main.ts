import * as net from "net";
import { parseArgs } from "node:util";
import RespParser from "./parser/RespParser";
import type { TBlocked, TCMDQueueElem, TRespData } from "./types";
import { executeCommand } from "./cmd-exectutor";

const { values } = parseArgs({
  options: {
    port: { type: "string" },
  },
});

class RedisServer {
  private server: net.Server;
  private cache = new Map<string, TRespData>();
  private blocked = new Map<string, Array<TBlocked>>();

  constructor(private port: number = 6379) {
    this.server = net.createServer(this.handleConnection.bind(this));
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
        });
      }
    });
    socket.on("error", (err) => {
      console.error("Socket error:", err.message);
      socket.destroy();
    });
  }
}

new RedisServer(Number(values.port) || 6379).start();
