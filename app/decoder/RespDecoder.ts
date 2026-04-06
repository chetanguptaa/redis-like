import RedisError from "../error";
import type { TRespData, TSimpleString } from "../types";

class RespDecoder {
  private offset = 0;
  constructor(private input: string) {}

  static decode(input: string): TRespData {
    const decoder = new RespDecoder(input);
    return decoder.parse();
  }

  static tryDecode(buffer: Buffer): { value: any; rest: Buffer } | null {
    if (buffer.length === 0) return null;
    const prefix = String.fromCharCode(buffer[0]);
    if (prefix === "+") {
      const end = buffer.indexOf("\r\n");
      if (end === -1) return null;
      const value = buffer.subarray(1, end).toString();
      return {
        value: { type: "simple", value },
        rest: buffer.subarray(end + 2),
      };
    }
    if (prefix === "$") {
      const end = buffer.indexOf("\r\n");
      if (end === -1) return null;
      const len = parseInt(buffer.subarray(1, end).toString(), 10);
      if (len === -1) {
        return {
          value: { type: "bulk", value: null },
          rest: buffer.subarray(end + 2),
        };
      }
      const total = end + 2 + len + 2;
      if (buffer.length < total) return null;
      const value = buffer.subarray(end + 2, end + 2 + len).toString();
      return {
        value: { type: "bulk", value },
        rest: buffer.subarray(end + 2 + len + 2),
      };
    }
    if (prefix === "*") {
      const end = buffer.indexOf("\r\n");
      if (end === -1) return null;
      const count = parseInt(buffer.subarray(1, end).toString(), 10);
      let offset = end + 2;
      const items = [];
      for (let i = 0; i < count; i++) {
        const sub = RespDecoder.tryDecode(buffer.subarray(offset));
        if (!sub) return null;
        items.push(sub.value);
        offset = buffer.length - sub.rest.length;
      }
      return {
        value: { type: "array", value: items },
        rest: buffer.subarray(offset),
      };
    }

    return null;
  }

  private parse(): TRespData {
    const prefix = this.input[this.offset++];
    switch (prefix) {
      case "+":
        return this.parseSimpleString();
      case "-":
        return this.parseError();
      case ":":
        return this.parseInteger();
      case "$":
        return this.parseBulkString();
      case "*":
        return this.parseArray();
      default:
        throw new Error(`Unknown RESP prefix: ${prefix}`);
    }
  }

  private readLine(): string {
    const end = this.input.indexOf("\r\n", this.offset);
    if (end === -1) throw new Error("Invalid RESP: missing CRLF");
    const line = this.input.slice(this.offset, end);
    this.offset = end + 2;
    return line;
  }

  private parseSimpleString(): TSimpleString {
    const value = this.readLine();
    return { __simple: true, value };
  }

  private parseError(): RedisError {
    const line = this.readLine();
    const [code, ...rest] = line.split(" ");
    const message = rest.join(" ");
    return new RedisError(message, code);
  }

  private parseInteger(): number {
    const line = this.readLine();
    return parseInt(line, 10);
  }

  private parseBulkString(): string | null {
    const length = parseInt(this.readLine(), 10);
    if (length === -1) {
      return null;
    }
    const value = this.input.slice(this.offset, this.offset + length);
    this.offset += length + 2;
    return value;
  }

  private parseArray(): TRespData[] | { type: "null-array" } {
    const length = parseInt(this.readLine(), 10);
    if (length === -1) {
      return { type: "null-array" };
    }
    const result: TRespData[] = [];
    for (let i = 0; i < length; i++) {
      result.push(this.parse());
    }
    return result;
  }
}

export default RespDecoder;
