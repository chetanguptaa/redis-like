import * as net from "net";
import type Stream from "../data-structures/Stream";
import type RedisError from "../error";

export type RespPrimitive =
  | string
  | number
  | Stream
  | TSimpleString
  | TRespNull
  | Error
  | RedisError
  | null;

export type TRespData = RespPrimitive | TRespData[];

export type TRespNull = { type: "null-array" } | { type: "null-bulk" };

export type CommandHandler = (args: TRespData[], ctx: CommandContext) => void;

export type TSimpleString = { __simple: true; value: string };

export type TBlocked = {
  socket: net.Socket;
  unblock: (key?: string, element?: TRespData) => void;
};

export type TCMDQueueElem = { handler: Function; args: TRespData[] };

export interface CommandContext {
  socket: net.Socket;
  cache: Map<string, TRespData>;
  blocked: Map<string, Array<TBlocked>>;
  isMulti: boolean;
  setIsMulti: (value: boolean) => void;
  cmdQueue: TCMDQueueElem[];
  replicaOf: string | null;
  replicationId: string | null;
  replicationOffset: number | null;
}
