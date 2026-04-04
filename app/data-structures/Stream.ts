import type { TRespData } from "../types";

class Stream {
  public entries: Array<TRespData>;
  public lastTimestamp: number;
  public lastSeq: number;

  constructor() {
    this.entries = [];
    this.lastTimestamp = 0;
    this.lastSeq = 0;
  }
}

export default Stream;
