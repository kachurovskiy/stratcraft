import { types as pgTypes } from 'pg';

const OID_INT8 = 20;
const OID_NUMERIC = 1700;
const OID_DATE = 1082;

let configured = false;

function parseSafeInt8(value: string): number {
  const parsed = BigInt(value);
  if (parsed > BigInt(Number.MAX_SAFE_INTEGER) || parsed < BigInt(Number.MIN_SAFE_INTEGER)) {
    throw new Error(`int8 value ${value} is outside JS safe integer range`);
  }
  return Number(parsed);
}

function parseFiniteNumber(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`numeric value "${value}" is not a finite number`);
  }
  return parsed;
}

export function configurePgTypeParsers(): void {
  if (configured) {
    return;
  }
  configured = true;

  pgTypes.setTypeParser(OID_INT8, parseSafeInt8);
  pgTypes.setTypeParser(OID_NUMERIC, parseFiniteNumber);

  // pg parses DATE into a local-time Date object by default, which can shift the day when converting to ISO.
  // Preserve DATE columns as plain YYYY-MM-DD strings instead.
  pgTypes.setTypeParser(OID_DATE, (value) => value);
}

