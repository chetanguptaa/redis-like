export const SET_OPTIONS = {
  PX: "PX",
  EX: "EX",
} as const;

export type TSetOption = keyof typeof SET_OPTIONS;

export const WRITE_CMDS = [
  // "PING", ////// Pointer: Special command that master uses to tell the repica they are still alive
  "SET",
  "DEL",
  "RPUSH",
  "LPUSH",
  "LLOP",
  "BLPOP",
  "XADD",
  "INCR",
];

export const SUBSCRIBE_MODE_APPLICABLE_CMDS = [
  "SUBSCRIBE",
  "UNSUBSCRIBE",
  "PSUBSCRIBE",
  "PUNSUBSCRIBE",
  "PING",
  "QUIT",
];
