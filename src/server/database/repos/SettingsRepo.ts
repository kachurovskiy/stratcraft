import type { PoolClient, QueryResultRow } from 'pg';
import { isSensitiveSettingKey, type SettingKey } from '../../constants';
import { decryptValue, encryptValue } from '../../utils/encryption';
import { DbClient } from '../core/DbClient';

const SETTINGS_CACHE_TTL_MS = 1_000;

type CachedSetting = { value: string | null; loadedAt: number };

type SettingValueRow = QueryResultRow & { value: string | null };
type SettingRow = QueryResultRow & { setting_key: string; value: string | null };

export class SettingsRepo {
  private cache = new Map<string, CachedSetting>();

  constructor(private readonly db: DbClient) {}

  private readFromCache(settingKey: string): string | null | undefined {
    const cached = this.cache.get(settingKey);
    if (!cached) {
      return undefined;
    }
    if (Date.now() - cached.loadedAt > SETTINGS_CACHE_TTL_MS) {
      this.cache.delete(settingKey);
      return undefined;
    }
    return cached.value;
  }

  private cacheValue(settingKey: string, value: string | null): void {
    this.cache.set(settingKey, { value, loadedAt: Date.now() });
  }

  async getSettingValue(settingKey: string): Promise<string | null> {
    if (!settingKey || typeof settingKey !== 'string') {
      throw new Error('settingKey is required for getSettingValue');
    }

    const cached = this.readFromCache(settingKey);
    if (cached !== undefined) {
      return cached;
    }

    const row = await this.db.get<SettingValueRow>(
      'SELECT value FROM settings WHERE setting_key = ?',
      [settingKey]
    );
    const rawValue = typeof row?.value === 'string' ? row.value : null;
    const value = rawValue && isSensitiveSettingKey(settingKey) ? decryptValue(rawValue) : rawValue;
    this.cacheValue(settingKey, value ?? null);
    return value ?? null;
  }

  async getRequiredSettingValue(settingKey: string): Promise<string> {
    const value = await this.getSettingValue(settingKey);
    if (typeof value === 'string' && value.trim().length > 0) {
      return value;
    }
    throw new Error(`Required setting "${settingKey}" is missing or empty.`);
  }

  async getSettingArray(settingKey: string): Promise<string[]> {
    const rawValue = await this.getSettingValue(settingKey);
    return this.parseSettingArrayValue(rawValue);
  }

  async getSettingsByKeys(settingKeys: SettingKey[]): Promise<Record<string, string | null>> {
    if (!Array.isArray(settingKeys) || settingKeys.length === 0) {
      return {};
    }

    const placeholders = settingKeys.map(() => '?').join(',');
    const rows = await this.db.all<SettingRow>(
      `SELECT setting_key, value FROM settings WHERE setting_key IN (${placeholders})`,
      settingKeys
    );

    const result: Record<string, string | null> = {};
    for (const key of settingKeys) {
      result[key] = null;
    }

    for (const row of rows) {
      const key = typeof row.setting_key === 'string' ? row.setting_key : String(row.setting_key ?? '');
      if (!key) {
        continue;
      }
      const rawValue =
        typeof row.value === 'string'
          ? row.value
          : row.value === null || row.value === undefined
            ? null
            : String(row.value);
      const value = rawValue && isSensitiveSettingKey(key) ? decryptValue(rawValue) : rawValue;
      result[key] = value ?? null;
    }

    return result;
  }

  async upsertSettings(settings: Record<string, string>): Promise<void> {
    const entries = Object.entries(settings ?? {}).filter(
      ([key]) => typeof key === 'string' && key.trim().length > 0
    );
    if (entries.length === 0) {
      return;
    }

    await this.db.withTransaction(async (client: PoolClient) => {
      for (const [key, rawValue] of entries) {
        const value = typeof rawValue === 'string' ? rawValue : String(rawValue ?? '');
        const storedValue = isSensitiveSettingKey(key) && value.length > 0 ? encryptValue(value) : value;
        await this.db.run(
          `
            INSERT INTO settings (setting_key, value, updated_at)
            VALUES (?, ?, NOW())
            ON CONFLICT (setting_key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
          `,
          [key, storedValue],
          client
        );
      }
    });

    for (const [key, rawValue] of entries) {
      const value = typeof rawValue === 'string' ? rawValue : String(rawValue ?? '');
      this.cacheValue(key, value);
    }
  }

  private parseSettingArrayValue(rawValue: string | null): string[] {
    if (!rawValue || rawValue.trim().length === 0) {
      return [];
    }
    const trimmed = rawValue.trim();
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        const normalized = parsed
          .filter((entry) => typeof entry === 'string')
          .map((entry) => entry.trim().toUpperCase())
          .filter((entry) => entry.length > 0);
        return Array.from(new Set(normalized));
      }
    } catch {
      // Fall back to delimiter parsing below.
    }

    const split = trimmed.split(/[,\s]+/g);
    const normalized = split
      .map((entry) => entry.trim().toUpperCase())
      .filter((entry) => entry.length > 0);
    return Array.from(new Set(normalized));
  }
}

