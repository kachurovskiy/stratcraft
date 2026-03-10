import { viewHelpers } from './helpers';

describe('viewHelpers.formatPercentAsPercent', () => {
  it('formats finite percent values with two decimals', () => {
    expect(viewHelpers.formatPercentAsPercent(12.3456)).toBe('12.35%');
  });

  it('returns N/A for null and non-finite values', () => {
    expect(viewHelpers.formatPercentAsPercent(null)).toBe('N/A');
    expect(viewHelpers.formatPercentAsPercent(Number.NaN)).toBe('N/A');
    expect(viewHelpers.formatPercentAsPercent(Number.POSITIVE_INFINITY)).toBe('N/A');
  });
});
