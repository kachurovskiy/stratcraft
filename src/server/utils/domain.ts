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
