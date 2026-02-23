import { DEFAULT_BACKTEST_INITIAL_CAPITAL } from '../constants';

const normalizeInitialCapital = (rawValue: unknown, fallback: number): number => {
  const parsed = typeof rawValue === 'number'
    ? rawValue
    : typeof rawValue === 'string'
      ? Number(rawValue.trim())
      : Number.NaN;

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
};

export const resolveBacktestInitialCapitalSetting = (rawValue: string | null): number =>
  normalizeInitialCapital(rawValue, DEFAULT_BACKTEST_INITIAL_CAPITAL);

export const resolveStrategyInitialCapital = (
  strategy: { accountId?: string | null; parameters?: Record<string, unknown> | null },
  backtestInitialCapital: number
): number => {
  if (strategy?.accountId) {
    return normalizeInitialCapital(strategy.parameters?.initialCapital, backtestInitialCapital);
  }

  return backtestInitialCapital;
};
