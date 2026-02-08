import { randomInt } from 'crypto';

const RANGES: Array<[number, number]> = [
  [0x1F300, 0x1F5FF],
  [0x1F600, 0x1F64F],
  [0x1F680, 0x1F6FF],
  [0x1F700, 0x1F77F],
  [0x1F780, 0x1F7FF],
  [0x1F800, 0x1F8FF],
  [0x1F900, 0x1F9FF],
  [0x1FA00, 0x1FA6F],
  [0x1FA70, 0x1FAFF],
  [0x2600, 0x26FF],
  [0x2700, 0x27BF],
];

const EXTENDED_PICTOGRAPHIC_REGEX = (() => {
  try {
    return new RegExp('\\p{Extended_Pictographic}', 'u');
  } catch {
    return null;
  }
})();

const randomIntInclusive = (min: number, max: number): number => randomInt(min, max + 1);

export const randomEmoji = (maxTries = 50): string => {
  const tries = Number.isFinite(maxTries) && maxTries > 0 ? Math.floor(maxTries) : 50;

  for (let i = 0; i < tries; i++) {
    const [min, max] = RANGES[randomInt(0, RANGES.length)] ?? [];
    if (typeof min !== 'number' || typeof max !== 'number') {
      continue;
    }

    const codepoint = randomIntInclusive(min, max);
    const ch = String.fromCodePoint(codepoint);

    if (EXTENDED_PICTOGRAPHIC_REGEX?.test(ch) ?? true) {
      return ch;
    }
  }

  return '✨';
};

