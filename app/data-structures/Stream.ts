export type TEntry = {
  id: string;
  [key: string]: any;
};

class Stream {
  public entries: Array<TEntry>;

  constructor() {
    this.entries = [];
  }
}

export default Stream;
