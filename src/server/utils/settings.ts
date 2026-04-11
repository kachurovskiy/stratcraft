import type { SettingsValue } from '../database/types';

export type SettingsRepo = {
  value: Pick<SettingsValue, 'paramScoring' | 'templateScoring'>;
};

export const normalizeNumber = (
  value: number,
  fallback: number,
  options: { min?: number; max?: number; integer?: boolean } = {}
): number => {
  if (!Number.isFinite(value)) {
    return fallback;
  }
  if (options.integer && !Number.isInteger(value)) {
    return fallback;
  }
  if (options.min !== undefined && value < options.min) {
    return fallback;
  }
  if (options.max !== undefined && value > options.max) {
    return fallback;
  }
  return value;
};
