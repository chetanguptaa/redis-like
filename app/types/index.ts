export type RespPrimitive = string | number | null;
export type TRespData = RespPrimitive | TRespData[];

export type CommandHandler = (args: TRespData[], ctx: CommandContext) => void;

export interface CommandContext {
  socket: import("net").Socket;
  cache: Map<string, TRespData>;
}
