import { Database } from '../database/Database';
import { SETTING_KEYS } from '../constants';

const LOCAL_DOMAIN_PREFIXES = ['localhost', '127.0.0.1', '[::1]'];
const DOMAIN_REGEX = /^[A-Za-z0-9.-]+$/;

export const isLocalDomain = (value: string): boolean => {
  const lowered = value.toLowerCase();
  return LOCAL_DOMAIN_PREFIXES.some(prefix => lowered.startsWith(prefix));
};

export const normalizeDomain = (value?: string | null): string | null => {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
  }
  if (trimmed.includes('://') || trimmed.includes('/') || trimmed.includes('?') || trimmed.includes('#')) {
    return null;
  }
  if (trimmed.includes(':')) {
    return null;
  }
  if (!DOMAIN_REGEX.test(trimmed)) {
    return null;
  }
  return trimmed;
};

export const resolveSiteName = async (db: Database): Promise<string> => {
  return String(await db.settings.getSettingValue(SETTING_KEYS.SITE_NAME));
};

export const resolveAppDomain = async (db?: Database): Promise<string | null> => {
  const envValue = normalizeDomain(process.env.DOMAIN);
  if (envValue) {
    return envValue;
  }
  if (!db) {
    return null;
  }
  const settingValue = await db.settings.getSettingValue(SETTING_KEYS.DOMAIN);
  return normalizeDomain(settingValue);
};

export const resolveAppBaseUrl = async (db?: Database): Promise<string | null> => {
  const domain = await resolveAppDomain(db);
  if (!domain) {
    return null;
  }
  const scheme = isLocalDomain(domain) ? 'http' : 'https';
  return `${scheme}://${domain}`;
};

export const resolveNoreplyFromEmail = async (db?: Database): Promise<string | null> => {
  const domain = await resolveAppDomain(db);
  if (!domain) {
    return null;
  }
  return `noreply@${domain}`;
};

export const resolveFromEmail = async (db?: Database): Promise<string | null> => {
  return await resolveNoreplyFromEmail(db);
};
