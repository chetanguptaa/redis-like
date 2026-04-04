export const SUPPORTED_COMMANDS = {
  ECHO: "ECHO",
  PING: "PING",
  SET: "SET",
  GET: "GET",
  RPUSH: "RPUSH",
  LRANGE: "LRANGE",
} as const;

export type TSupportedCommand =
  (typeof SUPPORTED_COMMANDS)[keyof typeof SUPPORTED_COMMANDS];

export const SET_OPTIONS = {
  PX: "PX",
  EX: "EX",
} as const;

export type TSetOption = keyof typeof SET_OPTIONS;
