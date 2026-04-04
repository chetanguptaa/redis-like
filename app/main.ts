import * as net from "net";
import RespParser from "./parser/RespParser";
import RespEncoder from "./encoder/RespEncoder";
import { SUPPORTED_COMMANDS, SUPPORTED_SUB_COMMANDS } from "./constants";
import ExpiryMap from "./data-structures/ExpiryMap";
import type { TRespData } from "./types";

type CommandHandler = (args: any[], socket: net.Socket) => void;

class RedisServer {
  private server: net.Server;
  private cache = new ExpiryMap<string, TRespData>();

  constructor(private port: number = 6379) {
    this.server = net.createServer(this.handleConnection.bind(this));
  }

  public start() {
    this.server.listen(this.port, "127.0.0.1", () => {
      console.log(`Server listening on 127.0.0.1:${this.port}`);
    });
    this.server.on("error", (err) => {
      console.error("Server error:", err);
    });
  }

  private handleConnection(socket: net.Socket) {
    console.log("Client connected:", socket.remoteAddress);
    const parser = new RespParser();
    socket.on("data", (chunk: Buffer) => {
      try {
        const messages = parser.push(chunk.toString());
        for (const message of messages) {
          this.handleMessage(message, socket);
        }
      } catch (err) {
        this.handleError(err, socket);
      }
    });
    socket.on("error", (err) => {
      console.error("Socket error:", err.message);
      socket.destroy();
    });
    socket.on("close", () => {
      console.log("Client disconnected");
    });
  }

  private handleMessage(message: any, socket: net.Socket) {
    if (!Array.isArray(message) || message.length === 0) {
      return this.writeError(socket, "Protocol error: expected array");
    }
    const [commandRaw, ...args] = message;
    if (typeof commandRaw !== "string") {
      return this.writeError(socket, "Invalid command");
    }
    const command = commandRaw.toUpperCase();
    const handler = this.getHandler(command);
    if (!handler) {
      return this.writeError(socket, `ERR unknown command '${command}'`);
    }
    try {
      handler(args, socket);
    } catch (err) {
      this.handleError(err, socket);
    }
  }

  private getHandler(command: string): CommandHandler | null {
    const handlers: Record<string, CommandHandler> = {
      [SUPPORTED_COMMANDS.ECHO]: (args, socket) => {
        if (args.length < 1) {
          return this.writeError(
            socket,
            "wrong number of arguments for 'echo'",
          );
        }
        socket.write(RespEncoder.encode(args[0]));
      },
      [SUPPORTED_COMMANDS.PING]: (_args, socket) => {
        socket.write("+PONG\r\n");
      },
      [SUPPORTED_COMMANDS.SET]: (args, socket) => {
        if (args.length < 2) {
          return this.writeError(socket, "wrong number of arguments for 'set'");
        }
        if (args.length === 4) {
          const timeoutVersion = args[2];
          const time = args[3];
          if (timeoutVersion === SUPPORTED_SUB_COMMANDS.EX) {
            this.cache.set(args[0], args[1], time * 1000);
          }
          if (timeoutVersion === SUPPORTED_SUB_COMMANDS.PX) {
            this.cache.set(args[0], args[1], time);
          }
        } else {
          this.cache.set(args[0], args[1]);
        }
        socket.write("+OK\r\n");
      },
      [SUPPORTED_COMMANDS.GET]: (args, socket) => {
        if (args.length < 1) {
          return this.writeError(socket, "wrong number of arguments for 'set'");
        }
        const value = this.cache.get(args[0]);
        socket.write(RespEncoder.encode(value || null));
      },
    };
    return handlers[command] || null;
  }

  private writeError(socket: net.Socket, message: string) {
    socket.write(`-ERR ${message}\r\n`);
  }

  private handleError(err: unknown, socket: net.Socket) {
    const message = err instanceof Error ? err.message : "Unknown server error";
    console.error("Processing error:", message);
    this.writeError(socket, message);
  }
}

const server = new RedisServer(6379);
server.start();
