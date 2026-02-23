import { SETTING_KEYS } from '../constants';
import { Database } from '../database/Database';

export const DEFAULT_FOOTER_DISCLAIMER_HTML =
  'Not financial advice. Most retail traders lose money. Use at your own risk.';

const escapeHtml = (value: string): string => (
  value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
);

const ALLOWED_INLINE_TAGS = new Set(['a', 'b', 'br', 'code', 'em', 'i', 'small', 'strong']);

const normalizeHref = (hrefValue: string): string | null => {
  const trimmed = hrefValue.trim();
  if (!trimmed) {
    return null;
  }

  if (trimmed.startsWith('/') || trimmed.startsWith('#')) {
    return trimmed;
  }

  const lowered = trimmed.toLowerCase();
  if (
    lowered.startsWith('http://') ||
    lowered.startsWith('https://') ||
    lowered.startsWith('mailto:')
  ) {
    return trimmed;
  }

  return null;
};

const extractSafeHref = (rawAttributes: string): string | null => {
  const hrefMatch = rawAttributes.match(
    /\bhref\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+))/i
  );
  if (!hrefMatch) {
    return null;
  }
  const hrefCandidate = hrefMatch[1] ?? hrefMatch[2] ?? hrefMatch[3] ?? '';
  return normalizeHref(hrefCandidate);
};

export const sanitizeFooterDisclaimerHtml = (rawValue: string | null | undefined): string => {
  const input = typeof rawValue === 'string' ? rawValue.trim() : '';
  if (!input) {
    return DEFAULT_FOOTER_DISCLAIMER_HTML;
  }

  const withoutUnsafeBlocks = input.replace(
    /<\s*(script|style)\b[^>]*>[\s\S]*?<\s*\/\s*\1\s*>/gi,
    ''
  );
  const tagPattern = /<\/?([a-zA-Z0-9]+)([^>]*)>/g;
  let output = '';
  let cursor = 0;
  let match: RegExpExecArray | null = null;
  const openTagStack: string[] = [];

  while ((match = tagPattern.exec(withoutUnsafeBlocks)) !== null) {
    const [fullTag, rawTagName, rawAttributes] = match;
    const tagStart = match.index;
    if (tagStart > cursor) {
      output += escapeHtml(withoutUnsafeBlocks.slice(cursor, tagStart));
    }

    const tagName = rawTagName.toLowerCase();
    if (!ALLOWED_INLINE_TAGS.has(tagName)) {
      cursor = tagStart + fullTag.length;
      continue;
    }

    const isClosingTag = fullTag.startsWith('</');
    if (tagName === 'br') {
      if (!isClosingTag) {
        output += '<br>';
      }
      cursor = tagStart + fullTag.length;
      continue;
    }

    if (isClosingTag) {
      const topTag = openTagStack[openTagStack.length - 1];
      if (topTag === tagName) {
        output += `</${tagName}>`;
        openTagStack.pop();
      }
      cursor = tagStart + fullTag.length;
      continue;
    }

    if (tagName === 'a') {
      const safeHref = extractSafeHref(rawAttributes);
      if (safeHref) {
        output += `<a href="${escapeHtml(safeHref)}" rel="noopener noreferrer">`;
        openTagStack.push(tagName);
      }
      cursor = tagStart + fullTag.length;
      continue;
    }

    output += `<${tagName}>`;
    openTagStack.push(tagName);
    cursor = tagStart + fullTag.length;
  }

  if (cursor < withoutUnsafeBlocks.length) {
    output += escapeHtml(withoutUnsafeBlocks.slice(cursor));
  }

  while (openTagStack.length > 0) {
    const tagName = openTagStack.pop();
    if (tagName) {
      output += `</${tagName}>`;
    }
  }

  return output.trim().length > 0 ? output : DEFAULT_FOOTER_DISCLAIMER_HTML;
};

export const resolveFooterDisclaimerHtml = async (db: Database): Promise<string> => {
  const rawValue = await db.settings.getSettingValue(SETTING_KEYS.FOOTER_DISCLAIMER_HTML);
  return sanitizeFooterDisclaimerHtml(rawValue);
};
