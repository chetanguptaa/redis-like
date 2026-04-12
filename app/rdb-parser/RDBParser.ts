type RDBValue = string;

interface RDBObject {
  key: string;
  value: RDBValue;
}

class RDBParser {
  private buf: Buffer;
  private offset: number;

  constructor(buffer: Buffer) {
    this.buf = buffer;
    this.offset = 0;
  }

  private read(n: number): Buffer {
    const slice = this.buf.subarray(this.offset, this.offset + n);
    this.offset += n;
    return slice;
  }

  private readByte(): number {
    return this.buf[this.offset++];
  }

  private readString(len: number): string {
    return this.read(len).toString("utf-8");
  }

  /**
   * Redis length encoding
   */
  private readLength(): number {
    const first = this.readByte();
    const type = (first & 0b11000000) >> 6;

    if (type === 0) {
      // 6-bit
      return first & 0b00111111;
    }

    if (type === 1) {
      // 14-bit
      const next = this.readByte();
      return ((first & 0b00111111) << 8) | next;
    }

    if (type === 2) {
      // 32-bit
      const val = this.buf.readUInt32BE(this.offset);
      this.offset += 4;
      return val;
    }

    // type === 3 → special encoding (not handled)
    throw new Error("Special encoding not supported");
  }

  private parseHeader(): string {
    const header = this.read(9).toString("ascii");

    if (!header.startsWith("REDIS")) {
      throw new Error("Invalid RDB file header");
    }

    return header;
  }

  private parseKeyValue(): RDBObject | null {
    const type = this.readByte();

    if (type === 0xff) {
      return null; // EOF
    }

    if (type !== 0) {
      throw new Error(`Unsupported value type: ${type}`);
    }

    const keyLen = this.readLength();
    const key = this.readString(keyLen);

    const valLen = this.readLength();
    const value = this.readString(valLen);

    return { key, value };
  }

  public parse(): Record<string, RDBValue> {
    const result: Record<string, RDBValue> = {};

    this.parseHeader();

    while (this.offset < this.buf.length) {
      const kv = this.parseKeyValue();
      if (!kv) break;
      result[kv.key] = kv.value;
    }

    return result;
  }
}

export default RDBParser;
