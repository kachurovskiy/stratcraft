import { Database } from '../database/Database';
import { isLocalDomain, normalizeDomain } from './domain';

export { isLocalDomain, normalizeDomain } from './domain';

export const resolveSiteName = async (db: Database): Promise<string> => {
  return db.settings.value.app.siteName;
};

export const resolveAppDomain = async (db?: Database): Promise<string | null> => {
  const envValue = normalizeDomain(process.env.DOMAIN);
  if (envValue) {
    return envValue;
  }
  if (!db) {
    return null;
  }
  return db.settings.value.app.domain || null;
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
