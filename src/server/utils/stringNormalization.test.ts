import {
  normalizeNullableUppercaseString,
  normalizeOptionalUppercaseString,
  normalizeUppercaseString,
  normalizeUppercaseStrings
} from './stringNormalization';

describe('stringNormalization', () => {
  it('normalizes strings to trimmed uppercase text', () => {
    expect(normalizeUppercaseString(' spy ')).toBe('SPY');
    expect(normalizeUppercaseString(null)).toBe('');
  });

  it('returns null or undefined for empty optional values', () => {
    expect(normalizeNullableUppercaseString('   ')).toBeNull();
    expect(normalizeNullableUppercaseString(' qqq ')).toBe('QQQ');
    expect(normalizeOptionalUppercaseString(undefined)).toBeUndefined();
    expect(normalizeOptionalUppercaseString(' dia ')).toBe('DIA');
  });

  it('normalizes iterables and drops empty entries', () => {
    expect(normalizeUppercaseStrings([' spy ', '', 'qqq', '   ', 'dia'])).toEqual(['SPY', 'QQQ', 'DIA']);
  });
});
