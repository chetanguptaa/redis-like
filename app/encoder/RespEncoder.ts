import type { TRespData, TSimpleString } from "../types";

class RespEncoder {
  static encode(data: TRespData): string {
    if (
      typeof data === "object" &&
      data !== null &&
      "type" in data &&
      data.type === "null-array"
    ) {
      return "*-1\r\n";
    }
    if (
      typeof data === "object" &&
      data !== null &&
      "type" in data &&
      data.type === "null-bulk"
    ) {
      return "$-1\r\n";
    }
    if (data === null) {
      return "$-1\r\n";
    }
    if (typeof data === "object" && data !== null && "__simple" in data) {
      return `+${(data as TSimpleString).value}\r\n`;
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
