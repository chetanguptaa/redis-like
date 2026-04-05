import * as net from "net";
import RespParser from "./parser/RespParser";
import type { TBlocked, TRespData } from "./types";
import { executeCommand } from "./cmd-exectutor";

class RedisServer {
  private server: net.Server;
  private cache = new Map<string, TRespData>();
  private blocked = new Map<string, Array<TBlocked>>();
  private isMulti = false;
  private cmdQueue: (() => void)[] = [];

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

  private setIsMulti = (value: boolean) => {
    this.isMulti = value;
  };

  private handleConnection(socket: net.Socket) {
    const parser = new RespParser();
    socket.on("data", (chunk) => {
      try {
        const messages = parser.push(chunk.toString());
        for (const msg of messages) {
          executeCommand(
            msg,
            {
              socket,
              cache: this.cache,
              blocked: this.blocked,
              isMulti: this.isMulti,
              setIsMulti: this.setIsMulti,
            },
            this.isMulti,
            this.cmdQueue,
          );
        }
      } catch (err) {
        this.handleError(err, socket);
      }
    });
    socket.on("error", (err) => {
      console.error("Socket error:", err.message);
      socket.destroy();
    });
  }

  private handleError(err: unknown, socket: net.Socket) {
    const message = err instanceof Error ? err.message : "Unknown server error";
    console.error("Error:", message);
    socket.write(`-ERR ${message}\r\n`);
  }
}

new RedisServer().start();
