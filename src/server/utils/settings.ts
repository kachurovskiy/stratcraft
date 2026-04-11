import type { SettingKey } from '../constants';

export type SettingsRepo = {
  getSettingsByKeys: (settingKeys: SettingKey[]) => Promise<Record<string, string | null>>;
};

type NumberSettingMapping<T> = { settingKey: SettingKey; field: keyof T };

export const parseOptionalNumberSetting = (rawValue: string | null): number | null => {
  if (typeof rawValue !== 'string') {
    return null;
  }
  const trimmed = rawValue.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
};

export const buildNumberSettingOverrides = <T extends Record<string, number>>(
  settingsMap: Record<string, string | null>,
  mapping: NumberSettingMapping<T>[]
): Partial<T> => {
  const overrides: Partial<Record<keyof T, number>> = {};
  mapping.forEach(({ settingKey, field }) => {
    const parsed = parseOptionalNumberSetting(settingsMap[settingKey]);
    if (parsed === null) {
      return;
    }
    overrides[field] = parsed;
  });
  return overrides as Partial<T>;
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

export const loadNumberSettingOverrides = async <T extends Record<string, number>>(
  settingsRepo: SettingsRepo | undefined,
  settingKeys: SettingKey[],
  mapping: NumberSettingMapping<T>[]
): Promise<Partial<T>> => {
  if (!settingsRepo) {
    return {};
  }
  const settingsMap = await settingsRepo.getSettingsByKeys(settingKeys);
  return buildNumberSettingOverrides(settingsMap, mapping);
};
