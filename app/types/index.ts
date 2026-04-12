import * as net from "net";
import type Stream from "../data-structures/Stream";
import type RedisError from "../error";
import type { MinHeap } from "../data-structures/MinHeap";

export type TRespPrimitive =
  | string
  | number
  | bigint
  | Stream
  | TSimpleString
  | TRespNull
  | Error
  | RedisError
  | null;

export type TRespData = TRespPrimitive | TRespData[];

export type TRespNull = { type: "null-array" } | { type: "null-bulk" };

export type TCommandHandler = (args: TRespData[], ctx: ICommandContext) => void;

export type TSimpleString = { __simple: true; value: string };

export type TZSet = {
  value: string;
  score: number;
};

export type TGeoEntry = {
  member: string;
  lat: number;
  lon: number;
  score: bigint;
};

export type TBlocked = {
  socket: net.Socket;
  unblock: (key?: string, element?: TRespData) => void;
};

export type TCMDQueueElem = { handler: Function; args: TRespData[] };

export type TStage =
  | "PING"
  | "REPLCONF1"
  | "REPLCONF2"
  | "PSYNC"
  | "FULLRESYNC"
  | "RDB"
  | "STREAM";

export interface ICommandContext {
  socket: net.Socket;
  cache: Map<string, TRespData>;
  blocked: Map<string, Array<TBlocked>>;
  isMulti?: boolean;
  setIsMulti?: (value: boolean) => void;
  cmdQueue?: TCMDQueueElem[];
  mySlaves?: Map<string, net.Socket>;
  channelsToSubscribersMap?: Map<string, net.Socket[]>;
  replicationId?: string | null;
  replicationOffset?: number | null;
  myMaster?: string | null;
  masterReplicationId?: string | null;
  masterReplicationOffset?: number | null;
  port: number | null;
  isFromMaster: boolean;
  masterOffsetBeforeCommand?: number | null;
  dir: string | null;
  dbFileName: string | null;
  subscribedChannels?: string[];
  isSubscribeMode?: boolean;
  setIsSubscribeMode?: (value: boolean) => void;
  zCache?: Map<string, MinHeap<TZSet>>;
  geoCache?: Map<string, MinHeap<TGeoEntry>>;
}
