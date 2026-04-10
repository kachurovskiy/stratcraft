const normalizeTrimmedUppercase = (value: unknown): string => {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim().toUpperCase();
};

export const normalizeUppercaseString = normalizeTrimmedUppercase;

export const normalizeNullableUppercaseString = (value: unknown): string | null => {
  const normalized = normalizeTrimmedUppercase(value);
  return normalized.length > 0 ? normalized : null;
};

export const normalizeOptionalUppercaseString = (value: unknown): string | undefined => {
  const normalized = normalizeTrimmedUppercase(value);
  return normalized.length > 0 ? normalized : undefined;
};

export const normalizeUppercaseStrings = (values: Iterable<unknown>): string[] => {
  const normalized: string[] = [];
  for (const value of values) {
    const entry = normalizeTrimmedUppercase(value);
    if (entry.length > 0) {
      normalized.push(entry);
    }
  }
  return normalized;
};
