import {
  DEFAULT_FOOTER_DISCLAIMER_HTML,
  sanitizeFooterDisclaimerHtml
} from './footerDisclaimer';

describe('sanitizeFooterDisclaimerHtml', () => {
  it('returns default text when value is missing', () => {
    expect(sanitizeFooterDisclaimerHtml(null)).toBe(DEFAULT_FOOTER_DISCLAIMER_HTML);
    expect(sanitizeFooterDisclaimerHtml('   ')).toBe(DEFAULT_FOOTER_DISCLAIMER_HTML);
  });

  it('allows basic formatting tags and safe links', () => {
    const sanitized = sanitizeFooterDisclaimerHtml(
      'Not <strong>financial advice</strong>. <a href="https://example.com" onclick="alert(1)">Learn more</a>'
    );

    expect(sanitized).toBe(
      'Not <strong>financial advice</strong>. <a href="https://example.com" rel="noopener noreferrer">Learn more</a>'
    );
  });

  it('strips unsafe link protocols', () => {
    const sanitized = sanitizeFooterDisclaimerHtml(
      'Avoid <a href="javascript:alert(1)">bad links</a>'
    );

    expect(sanitized).toBe('Avoid bad links');
  });

  it('removes script/style blocks and falls back to default when output is empty', () => {
    expect(sanitizeFooterDisclaimerHtml('<script>alert(1)</script>')).toBe(
      DEFAULT_FOOTER_DISCLAIMER_HTML
    );
    expect(sanitizeFooterDisclaimerHtml('<style>body{color:red}</style>')).toBe(
      DEFAULT_FOOTER_DISCLAIMER_HTML
    );
  });

  it('escapes text around unsupported markup', () => {
    const sanitized = sanitizeFooterDisclaimerHtml('<u>Important</u> & more');
    expect(sanitized).toBe('Important &amp; more');
  });
});
