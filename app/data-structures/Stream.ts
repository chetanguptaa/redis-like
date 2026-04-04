import type { TRespData } from "../types";

export type TEntry = {
  id: string;
  [key: string]: TRespData;
};

class Stream {
  public entries: Array<TEntry>;

  constructor() {
    this.entries = [];
  }
}

export default Stream;
