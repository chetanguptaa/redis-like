export const SET_OPTIONS = {
  PX: "PX",
  EX: "EX",
} as const;

export type TSetOption = keyof typeof SET_OPTIONS;
