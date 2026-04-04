import type { TRespData } from "../types";

class RespParser {
  private buffer: string = "";
  private offset: number = 0;

  public push(chunk: string): TRespData[] {
    this.buffer += chunk;
    const results: TRespData[] = [];
    while (true) {
      try {
        const value = this.readValue();
        if (value === undefined) break;
        results.push(value);
      } catch {
        break;
      }
    }

    this.buffer = this.buffer.slice(this.offset);
    this.offset = 0;
    return results;
  }

  private readValue(): TRespData | undefined {
    if (this.offset >= this.buffer.length) return undefined;
    const type = this.buffer[this.offset++];
    switch (type) {
      case "+":
        return this.readSimpleString();
      case "-":
        return this.readError();
      case ":":
        return this.readInteger();
      case "$":
        return this.readBulkString();
      case "*":
        return this.readArray();
      default:
        throw new Error(`Unknown RESP type: ${type}`);
    }
  }

  private readLine(): string {
    const end = this.buffer.indexOf("\r\n", this.offset);
    if (end === -1) throw new Error("Incomplete");
    const line = this.buffer.slice(this.offset, end);
    this.offset = end + 2;
    return line;
  }

  private readSimpleString(): string {
    return this.readLine();
  }

  private readError(): never {
    throw new Error(this.readLine());
  }

  private readInteger(): number {
    return parseInt(this.readLine(), 10);
  }

  private readBulkString(): string | null {
    const length = parseInt(this.readLine(), 10);
    if (length === -1) return null;
    if (this.offset + length + 2 > this.buffer.length) {
      throw new Error("Incomplete");
    }
    const str = this.buffer.slice(this.offset, this.offset + length);
    this.offset += length + 2;
    return str;
  }

  private readArray(): TRespData {
    const count = parseInt(this.readLine(), 10);
    if (count === -1) return null;
    const result: TRespData[] = [];
    for (let i = 0; i < count; i++) {
      const val = this.readValue();
      if (val === undefined) throw new Error("Incomplete");
      result.push(val);
    }
    return result;
  }
}

export default RespParser;
