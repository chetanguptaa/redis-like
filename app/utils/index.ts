import type {
  TCommandHandler,
  TBlocked,
  TRespData,
  TSimpleString,
} from "../types";

export function isStrictNumber(str: string) {
  if (typeof str !== "string" || str.trim() === "") return false;
  return !Number.isNaN(Number(str));
}

export const safeHandler = (handler: TCommandHandler): TCommandHandler => {
  return async (args, ctx) => {
    try {
      return await handler(args, ctx);
    } catch (err) {
      throw err;
    }
  };
};

export const wakeBlockedListClients = (
  key: string,
  cache: Map<string, TRespData>,
  blocked: Map<string, Array<TBlocked>>,
) => {
  const queue = blocked.get(key);
  if (!queue || queue.length === 0) return;
  const list = cache.get(key);
  if (!Array.isArray(list) || list.length === 0) return;
  while (queue.length > 0 && list.length > 0) {
    const client = queue.shift();
    const element = list.shift();
    client?.unblock(key, element ?? null);
  }
  if (queue.length === 0) {
    blocked.delete(key);
  }
};

export const wakeBlockedStreamsClients = (
  key: string,
  blocked: Map<string, Array<TBlocked>>,
) => {
  const clients = blocked.get(key);
  if (!clients || clients.length === 0) return;
  for (const client of clients) {
    client?.unblock();
  }
};

export const simpleString = (value: string): TSimpleString => ({
  __simple: true,
  value,
});

const ENCODE_MIN_LATITUDE = -85.05112878;
const ENCODE_MAX_LATITUDE = 85.05112878;
const ENCODE_MIN_LONGITUDE = -180.0;
const ENCODE_MAX_LONGITUDE = 180.0;

const ENCODE_LATITUDE_RANGE = ENCODE_MAX_LATITUDE - ENCODE_MIN_LATITUDE;
const ENCODE_LONGITUDE_RANGE = ENCODE_MAX_LONGITUDE - ENCODE_MIN_LONGITUDE;

function spread32BitsTo64Bits(v: number): bigint {
  let result = BigInt(v) & 0xffffffffn;
  result = (result | (result << 16n)) & 0x0000ffff0000ffffn;
  result = (result | (result << 8n)) & 0x00ff00ff00ff00ffn;
  result = (result | (result << 4n)) & 0x0f0f0f0f0f0f0f0fn;
  result = (result | (result << 2n)) & 0x3333333333333333n;
  result = (result | (result << 1n)) & 0x5555555555555555n;
  return result;
}

function interleaveBits(x: number, y: number): bigint {
  const xSpread = spread32BitsTo64Bits(x);
  const ySpread = spread32BitsTo64Bits(y);
  const yShifted = ySpread << 1n;
  return xSpread | yShifted;
}

export const encodeGeohash = (latitude: number, longitude: number): bigint => {
  const normalizedLatitude =
    (Math.pow(2, 26) * (latitude - ENCODE_MIN_LATITUDE)) /
    ENCODE_LATITUDE_RANGE;
  const normalizedLongitude =
    (Math.pow(2, 26) * (longitude - ENCODE_MIN_LONGITUDE)) /
    ENCODE_LONGITUDE_RANGE;
  const latInt = Math.floor(normalizedLatitude);
  const lonInt = Math.floor(normalizedLongitude);
  return interleaveBits(latInt, lonInt);
};

const DECODE_MIN_LATITUDE = -85.05112878;
const DECODE_MAX_LATITUDE = 85.05112878;
const DECODE_MIN_LONGITUDE = -180.0;
const DECODE_MAX_LONGITUDE = 180.0;

const DECODE_LATITUDE_RANGE = DECODE_MAX_LATITUDE - DECODE_MIN_LATITUDE;
const DECODE_LONGITUDE_RANGE = DECODE_MAX_LONGITUDE - DECODE_MIN_LONGITUDE;

class DecodeCoordinates {
  constructor(
    public latitude: number,
    public longitude: number,
  ) {}
}

function decodeCompactInt64ToInt32(v: bigint): number {
  v = v & 0x5555555555555555n;
  v = (v | (v >> 1n)) & 0x3333333333333333n;
  v = (v | (v >> 2n)) & 0x0f0f0f0f0f0f0f0fn;
  v = (v | (v >> 4n)) & 0x00ff00ff00ff00ffn;
  v = (v | (v >> 8n)) & 0x0000ffff0000ffffn;
  v = (v | (v >> 16n)) & 0x00000000ffffffffn;
  return Number(v);
}

function decodeConvertGridNumbersToCoordinates(
  gridLatitudeNumber: number,
  gridLongitudeNumber: number,
): DecodeCoordinates {
  const gridLatitudeMin =
    DECODE_MIN_LATITUDE +
    DECODE_LATITUDE_RANGE * ((gridLatitudeNumber * 1.0) / Math.pow(2, 26));
  const gridLatitudeMax =
    DECODE_MIN_LATITUDE +
    DECODE_LATITUDE_RANGE *
      (((gridLatitudeNumber + 1) * 1.0) / Math.pow(2, 26));
  const gridLongitudeMin =
    DECODE_MIN_LONGITUDE +
    DECODE_LONGITUDE_RANGE * ((gridLongitudeNumber * 1.0) / Math.pow(2, 26));
  const gridLongitudeMax =
    DECODE_MIN_LONGITUDE +
    DECODE_LONGITUDE_RANGE *
      (((gridLongitudeNumber + 1) * 1.0) / Math.pow(2, 26));
  const latitude = (gridLatitudeMin + gridLatitudeMax) / 2;
  const longitude = (gridLongitudeMin + gridLongitudeMax) / 2;
  return new DecodeCoordinates(latitude, longitude);
}

export const decodeGeohash = (geoCode: bigint): DecodeCoordinates => {
  const y = geoCode >> 1n;
  const x = geoCode;
  const gridLatitudeNumber = decodeCompactInt64ToInt32(x);
  const gridLongitudeNumber = decodeCompactInt64ToInt32(y);
  return decodeConvertGridNumbersToCoordinates(
    gridLatitudeNumber,
    gridLongitudeNumber,
  );
};

export const isBigIntString = (val: string): boolean => {
  try {
    BigInt(val);
    return true;
  } catch (e) {
    return false;
  }
};
