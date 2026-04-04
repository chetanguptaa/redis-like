import type { TRespData } from "../types";

class RespEncoder {
  static encode(data: TRespData): string {
    if (data === null) {
      return "$-1\r\n";
    }
    if (typeof data === "string") {
      return `$${data.length}\r\n${data}\r\n`;
    }
    if (typeof data === "number") {
      return `:${data}\r\n`;
    }
    if (Array.isArray(data)) {
      let result = `*${data.length}\r\n`;
      for (const item of data) {
        result += this.encode(item);
      }
      return result;
    }
    throw new Error("Unsupported type");
  }
  static encodeCommand(args: string[]): string {
    return this.encode(args);
  }
}

export default RespEncoder;
