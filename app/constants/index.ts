export const SET_OPTIONS = {
  PX: "PX",
  EX: "EX",
} as const;

export type TSetOption = keyof typeof SET_OPTIONS;

export const WRITE_CMDS = [
  "SET",
  "DEL",
  "RPUSH",
  "LPUSH",
  "LLOP",
  "BLPOP",
  "XADD",
  "INCR",
];
