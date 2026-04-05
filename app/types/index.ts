import * as net from "net";
import type Stream from "../data-structures/Stream";

export type RespPrimitive = string | number | Stream | null;
export type TRespData = RespPrimitive | TRespData[];

export type CommandHandler = (args: TRespData[], ctx: CommandContext) => void;

export type TBlocked = {
  socket: net.Socket;
  unblock: (key?: string, element?: TRespData) => void;
};

export interface CommandContext {
  socket: net.Socket;
  cache: Map<string, TRespData>;
  blocked: Map<string, Array<TBlocked>>;
}
