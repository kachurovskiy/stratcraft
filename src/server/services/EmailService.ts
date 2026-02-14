import { Resend } from 'resend';
import { Database } from '../database/Database';
import { normalizeMtlsAccessCertPassword, SETTING_KEYS } from '../constants';
import { LoggingService } from './LoggingService';
import { isLocalDomain, resolveAppBaseUrl, resolveAppDomain, resolveFromEmail, resolveSiteName } from '../utils/appUrl';
import {
  calculateEstimatedCashImpact,
  calculateOrderSizeStats,
  type CashImpactSummary,
  type OrderSizeStats
} from '../utils/dispatchSummaryCalculations';
import { MtlsLockdownService } from './MtlsLockdownService';

const escapeHtml = (value: string): string => (
  value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
);

export class ResendApiKeyMissingError extends Error {
  readonly code = 'RESEND_API_KEY_MISSING';

  constructor(message?: string) {
    super(message ?? 'Resend API key is not configured. Set it in Admin Settings -> Email.');
    this.name = 'ResendApiKeyMissingError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class AppDomainMissingError extends Error {
  readonly code = 'APP_DOMAIN_MISSING';

  constructor(message?: string) {
    super(message ?? 'App domain is not configured. Set it in Admin Settings -> App.');
    this.name = 'AppDomainMissingError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export interface EmailOptions {
  to: string;
  subject: string;
  html: string;
  attachments?: Array<{
    content?: string | Buffer;
    filename?: string | false;
    path?: string;
    contentType?: string;
    contentId?: string;
  }>;
}

export interface OperationDispatchSummaryPayload {
  operations: Array<{
    accountName: string;
    accountProvider: string;
    accountEnvironment: string;
    ticker: string;
    operationType: string;
    quantity: number | null;
    price: number | null;
    orderType?: 'market' | 'limit' | null;
    status: string;
    statusReason?: string;
  }>;
}


export class EmailService {
  private static readonly USD_FORMATTER = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  private fromEmailOverride: string | null = null;
  private loggingService: LoggingService;
  private db: Database;
  private mtlsLockdownService: MtlsLockdownService | null;

  constructor(loggingService: LoggingService, db: Database, mtlsLockdownService?: MtlsLockdownService) {
    this.loggingService = loggingService;
    this.db = db;
    this.mtlsLockdownService = mtlsLockdownService ?? null;
  }

  async sendOTP(email: string, otpCode: string): Promise<void> {
    const siteName = await resolveSiteName(this.db);
    const escapedSiteName = escapeHtml(siteName);
    const subject = `Your ${siteName} Access Code`;
    const html = `
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #333;">Welcome to ${escapedSiteName}</h2>
        <p>Your one-time access code is:</p>
        <div style="background-color: #f5f5f5; padding: 20px; text-align: center; margin: 20px 0;">
          <h1 style="color: #007bff; font-size: 32px; margin: 0; letter-spacing: 4px;">${otpCode}</h1>
        </div>
        <p>This code will expire in 10 minutes.</p>
        <p>If you didn't request this code, please ignore this email.</p>
        <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
      </div>
    `;

    const domain = await resolveAppDomain(this.db);
    if (!domain) {
      throw new AppDomainMissingError();
    }
    const apiKey = await this.db.settings.getSettingValue(SETTING_KEYS.RESEND_API_KEY);
    const normalizedKey = apiKey?.trim() ?? '';
    if (!normalizedKey) {
      throw new ResendApiKeyMissingError();
    }
    const fromEmail = this.fromEmailOverride ?? (await resolveFromEmail(this.db)) ?? `noreply@${domain}`;
    await this.sendEmailWithKey(fromEmail, normalizedKey, { to: email, subject, html });
  }

  async sendInvitation(
    email: string,
    inviteLink: string,
    expiresAt: Date,
    inviteDays: number
  ): Promise<void> {
    const context = await this.resolveSendContext();
    if (!context) {
      return;
    }

    const escapedSiteName = escapeHtml(context.siteName);
    const accessGate = await this.getClientCertificateAccessGate();
    const subject = accessGate?.certificateBundle
      ? `You are invited to ${context.siteName} (access certificate attached)`
      : `You are invited to ${context.siteName}`;
    const expiresLabel = expiresAt.toLocaleString();
    const accessGateSnippet = accessGate?.certificateBundle
      ? `
        <div style="background:#fff6e5;border:1px solid #ffd699;border-radius:6px;padding:12px 14px;margin:16px 0;">
          <div style="font-weight:600;margin-bottom:6px;">Client certificate required</div>
          <div style="color:#333;">
            This ${escapedSiteName} server is protected by an nginx client certificate (mTLS) gate.
            Before importing, remove any previously installed ${escapedSiteName} access certificate from your certificate store
            (Windows: open <strong>"Manage user certificates"</strong> / <code>certmgr.msc</code>).
            Then download the attached <code>stratcraft-access.p12</code> (password: <code>${escapeHtml(accessGate.p12Password)}</code>) and import it into your browser/OS certificate store.
            After import, fully restart your browser (or reboot your PC), then return to this email and click <strong>Accept Invitation</strong>.
          </div>
        </div>
      `
      : '';
    const html = `
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #333;">You're invited to ${escapedSiteName}</h2>
        <p>An administrator has invited you to ${escapedSiteName}.</p>
        ${accessGateSnippet}
        <p>Use the button below to sign in. This link can be used once and expires in ${inviteDays} day${inviteDays === 1 ? '' : 's'}.</p>
        <div style="text-align: center; margin: 30px 0;">
          <a href="${inviteLink}" style="background-color: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; display: inline-block;">
            Accept Invitation
          </a>
        </div>
        ${accessGate?.certificateBundle ? `<p style="margin: 0 0 8px 0;">After import, open: <a href="${context.baseUrl}">${context.baseUrl}</a></p>` : ''}
        <p style="color: #555; font-size: 13px;">This link expires on ${expiresLabel}.</p>
        <p style="color: #555; font-size: 13px;">If you did not expect this invitation, you can ignore this email.</p>
        <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
      </div>
    `;

    await this.sendEmailWithKey(context.fromEmail, context.apiKey, {
      to: email,
      subject,
      html,
      attachments: accessGate?.certificateBundle
        ? [{
          filename: 'stratcraft-access.p12',
          content: accessGate.certificateBundle,
          contentType: 'application/x-pkcs12'
        }]
        : undefined
    });
  }

  async sendOperationDispatchSummary(email: string, summary: OperationDispatchSummaryPayload): Promise<void> {
    if (!Array.isArray(summary.operations) || summary.operations.length === 0) {
      return;
    }

    const context = await this.resolveSendContext();
    if (!context) {
      return;
    }

    const escapedSiteName = escapeHtml(context.siteName);
    const sentCount = summary.operations.filter(op => op.status === 'sent').length;
    const failedCount = summary.operations.filter(op => op.status === 'failed').length;
    const skippedCount = summary.operations.filter(op => op.status === 'skipped').length;
    const subjectParts = [`${context.siteName} order dispatch`];
    subjectParts.push(`${sentCount} sent`);
    if (failedCount > 0) {
      subjectParts.push(`${failedCount} failed`);
    }
    if (skippedCount > 0) {
      subjectParts.push(`${skippedCount} skipped`);
    }
    const subject = subjectParts.join(' | ');

    let cashImpactSummary: CashImpactSummary | null = null;
    try {
      cashImpactSummary = await calculateEstimatedCashImpact(summary.operations, {
        candlesRepo: this.db.candles
      });
    } catch (error) {
      this.loggingService.warn('system', 'Failed to calculate cash impact summary', {
        error: error instanceof Error ? error.message : String(error)
      });
    }
    const cashImpactSnippet = this.renderCashImpactSummary(cashImpactSummary);
    const orderSizeStats = calculateOrderSizeStats(summary.operations);
    const orderSizeStatsSnippet = this.renderOrderSizeStats(orderSizeStats);

    const failedOperations = summary.operations.filter(op => op.status === 'failed');
    const skippedOperations = summary.operations.filter(op => op.status === 'skipped');
    const otherOperations = summary.operations.filter(
      op => op.status !== 'failed' && op.status !== 'skipped'
    );
    const orderedOperations = [...failedOperations, ...skippedOperations, ...otherOperations];

    type AccountGroup = {
      accountName: string;
      accountProvider: string;
      accountEnvironment: string;
      operations: OperationDispatchSummaryPayload['operations'];
    };

    const accountGroups = orderedOperations.reduce<Map<string, AccountGroup>>((groups, op) => {
      const key = `${op.accountName}|||${op.accountProvider}|||${op.accountEnvironment}`;
      if (!groups.has(key)) {
        groups.set(key, {
          accountName: op.accountName,
          accountProvider: op.accountProvider,
          accountEnvironment: op.accountEnvironment,
          operations: [],
        });
      }
      const group = groups.get(key)!;
      group.operations.push(op);
      return groups;
    }, new Map());

    const accountSections = Array.from(accountGroups.values())
      .map(group => {
        const rows = group.operations
          .map(op => {
            const quantity = typeof op.quantity === 'number' ? op.quantity : '--';
            const price = typeof op.price === 'number' ? `$${op.price.toFixed(2)}` : '--';
            const statusLabel = op.status === 'sent'
              ? 'Sent'
              : op.status === 'failed'
                ? 'Failed'
                : op.status === 'skipped'
                  ? 'Skipped'
                  : op.status;
            const note = op.statusReason
              ? op.statusReason
              : op.status === 'sent'
                ? 'Submitted'
                : op.status === 'skipped'
                  ? 'No action taken'
                  : 'Review activity log';
            return `
              <tr style="white-space:nowrap;">
                <td style="padding:10px 8px;border-bottom:1px solid #eee;">${op.ticker}</td>
                <td style="padding:10px 8px;border-bottom:1px solid #eee;">${op.operationType}</td>
                <td style="padding:10px 8px;border-bottom:1px solid #eee;text-align:right;">${quantity}</td>
                <td style="padding:10px 8px;border-bottom:1px solid #eee;text-align:right;">${price}</td>
                <td style="padding:10px 8px;border-bottom:1px solid #eee;">
                  <span style="font-weight:600;">${statusLabel}</span>
                  <span style="color:#666;margin-left:8px;">${note}</span>
                </td>
              </tr>
            `;
          })
          .join('');

        return `
          <section style="margin-top:24px;">
            <h2 style="color:#333;margin:0 0 4px 0;">${group.accountName}</h2>
            <p style="margin:0 0 12px 0;color:#666;">${group.accountProvider} &middot; ${group.accountEnvironment}</p>
            <div style="overflow-x:auto;">
              <table style="border-collapse:collapse;font-size:13px;min-width:0;width:auto;display:inline-table;">
                <thead>
                  <tr style="background:#f5f5f5;">
                    <th style="text-align:left;padding:10px 8px;">Ticker</th>
                    <th style="text-align:left;padding:10px 8px;">Type</th>
                    <th style="text-align:right;padding:10px 8px;">Qty</th>
                    <th style="text-align:right;padding:10px 8px;">Price</th>
                    <th style="text-align:left;padding:10px 8px;">Status</th>
                  </tr>
                </thead>
                <tbody>${rows}</tbody>
              </table>
            </div>
          </section>
        `;
      })
      .join('');

    const manageAccountsUrl = `${context.baseUrl}/dashboard`;

    const html = `
      <div style="font-family: Arial, sans-serif;width:100%;max-width:100%;margin:0;">
        <div style="background:#f0f4ff;border:1px solid #c9d8ff;border-radius:6px;padding:12px 16px;display:inline-flex;flex-wrap:wrap;gap:12px;align-items:center;max-width:100%;">
          <a href="${manageAccountsUrl}" style="color:#0b63ce;font-weight:600;text-decoration:none;">Manage accounts</a>
          &nbsp;|&nbsp;
          <a href="https://app.alpaca.markets/account/orders" style="color:#0b63ce;font-weight:600;text-decoration:none;">View or cancel broker orders</a>
        </div>
        <p style="margin-top:24px;">We attempted to submit ${summary.operations.length} operation${summary.operations.length === 1 ? '' : 's'} on your behalf.</p>
        ${orderSizeStatsSnippet}
        ${cashImpactSnippet}
        <ul style="padding-left: 18px; color: #555;">
          <li><strong>${sentCount}</strong> sent successfully</li>
          <li><strong>${failedCount}</strong> failed</li>
          ${skippedCount > 0 ? `<li><strong>${skippedCount}</strong> skipped (no broker action required)</li>` : ''}
        </ul>
        ${accountSections}
        <p style="color:#666;font-size:12px;margin-top:20px;">
          You can review additional details inside ${escapedSiteName} and take action directly in your broker account if something looks off.
        </p>
      </div>
    `;

    await this.sendEmailWithKey(context.fromEmail, context.apiKey, { to: email, subject, html });
  }

  async sendAdhocEmail(options: EmailOptions): Promise<boolean> {
    const context = await this.resolveSendContext();
    if (!context) {
      return false;
    }
    await this.sendEmailWithKey(context.fromEmail, context.apiKey, options);
    return true;
  }

  async sendAdhocEmailRequired(options: EmailOptions): Promise<void> {
    const context = await this.resolveSendContextRequired();
    await this.sendEmailWithKey(context.fromEmail, context.apiKey, options);
  }

  async sendClientCertificateLockdownEnabledEmailToAllUsers(params: {
    certificateBundle: Buffer;
  }): Promise<{ sent: number; adminCount: number; }> {
    const context = await this.resolveSendContextRequired();
    const rawPassword = await this.db.settings.getSettingValue(SETTING_KEYS.MTLS_ACCESS_CERT_PASSWORD);
    const p12Password = normalizeMtlsAccessCertPassword(rawPassword);

    const allUsers = await this.db.users.listUsers('ASC');
    const uniqueByEmail = new Map<string, { email: string; isAdmin: boolean }>();

    for (const user of allUsers) {
      const email = typeof user.email === 'string' ? user.email.trim() : '';
      if (!email) continue;
      const key = email.toLowerCase();

      const isAdmin = user.role === 'admin';
      const existing = uniqueByEmail.get(key);
      if (!existing) {
        uniqueByEmail.set(key, { email, isAdmin });
      } else if (isAdmin) {
        existing.isAdmin = true;
      }
    }

    const recipients = Array.from(uniqueByEmail.values());
    const adminCount = recipients.filter(recipient => recipient.isAdmin).length;
    if (recipients.length === 0) {
      return { sent: 0, adminCount: 0 };
    }

    const attachment = {
      filename: 'stratcraft-access.p12',
      content: params.certificateBundle,
      contentType: 'application/x-pkcs12'
    };

    const results = await Promise.allSettled(
      recipients.map(async recipient => {
        const message = this.renderClientCertificateLockdownEnabledEmail({
          baseUrl: context.baseUrl,
          isAdmin: recipient.isAdmin,
          p12Password,
          siteName: context.siteName
        });

        await this.sendEmailWithKey(context.fromEmail, context.apiKey, {
          to: recipient.email,
          subject: message.subject,
          html: message.html,
          attachments: [attachment]
        });
      })
    );

    const failed = results.filter(result => result.status === 'rejected');
    if (failed.length > 0) {
      const firstFailure = failed[0];
      const errorMessage = firstFailure.status === 'rejected'
        ? firstFailure.reason instanceof Error
          ? firstFailure.reason.message
          : String(firstFailure.reason)
        : 'Unknown email error';
      throw new Error(`Failed to email ${failed.length} user(s). First error: ${errorMessage}`);
    }

    return { sent: recipients.length, adminCount };
  }

  private async sendEmailWithKey(fromEmail: string, apiKey: string, options: EmailOptions): Promise<void> {
    let subject = options.subject;
    try {
      const rawEmoji = await this.db.settings.getSettingValue(SETTING_KEYS.EMAIL_SECURITY_EMOJI);
      const emoji = typeof rawEmoji === 'string' ? rawEmoji.trim() : '';
      const emojiPrefix = emoji.length > 0 ? `${emoji} ` : '';
      subject = emojiPrefix && !options.subject.startsWith(emojiPrefix)
        ? `${emojiPrefix} ${options.subject}`
        : options.subject;

      const resend = new Resend(apiKey);
      const result = await resend.emails.send({
        from: fromEmail,
        to: options.to,
        subject,
        html: options.html,
        attachments: options.attachments,
      });

      if (result.error) {
        this.loggingService.error('system', 'Failed to send email', {
          to: options.to,
          subject,
          error: result.error.message
        });
        throw new Error(`Failed to send email: ${result.error.message}`);
      }

      this.loggingService.info('system', 'Email sent successfully', {
        to: options.to,
        subject,
        email_id: result.data?.id
      });
    } catch (error) {
      this.loggingService.error('system', 'Email service error', {
        to: options.to,
        subject,
        error: error instanceof Error ? error.message : String(error)
      });
      throw error;
    }
  }

  private async getClientCertificateAccessGate(): Promise<{ certificateBundle: Buffer; p12Password: string; } | null> {
    const mtlsService = this.mtlsLockdownService;
    if (!mtlsService) {
      return null;
    }

    let state: { supported: boolean; lockdownEnabled: boolean; bundleAvailable: boolean } | null = null;
    try {
      state = await mtlsService.getLockdownState();
    } catch {
      state = null;
    }

    if (!state?.supported || !state.lockdownEnabled) {
      return null;
    }

    if (!state.bundleAvailable) {
      throw new Error(
        'Client certificate lockdown is enabled but the access certificate bundle is missing. Generate it in Admin -> Users -> Server Access Lockdown.'
      );
    }

    const rawPassword = await this.db.settings.getSettingValue(SETTING_KEYS.MTLS_ACCESS_CERT_PASSWORD);
    const p12Password = normalizeMtlsAccessCertPassword(rawPassword);
    return { certificateBundle: await mtlsService.readClientCertificateBundle(), p12Password };
  }

  private renderClientCertificateLockdownEnabledEmail(params: {
    baseUrl: string;
    isAdmin: boolean;
    p12Password: string;
    siteName: string;
  }): { subject: string; html: string; } {
    const attachmentName = 'stratcraft-access.p12';
    const subject = `${params.siteName} access certificate (required)`;

    const adminSnippet = params.isAdmin
      ? `
          <hr style="margin: 24px 0; border: none; border-top: 1px solid #eee;">
          <h3 style="margin: 0 0 8px 0;">Admin: emergency disable</h3>
          <p style="margin: 0 0 12px 0;">
            If you enabled lockdown before importing the certificate (or a user is locked out), you can disable it via SSH:
          </p>
          <pre style="background:#f5f5f5;padding:12px;border-radius:6px;overflow:auto;"><code>ssh root@YOUR_SERVER_IP
sudo /usr/local/bin/stratcraft-mtls disable
sudo nginx -t &amp;&amp; sudo systemctl reload nginx</code></pre>
          <p style="color:#666;font-size:12px;margin:12px 0 0 0;">
            If the helper is missing, set <code>ssl_verify_client off;</code> in <code>/etc/nginx/stratcraft-mtls.conf</code> and reload nginx.
          </p>
        `
      : '';

    const html = `
      <div style="font-family: Arial, sans-serif; max-width: 720px; margin: 0 auto;">
        <h2 style="color:#333;margin:0 0 12px 0;">${escapeHtml(params.siteName)} server access lockdown enabled</h2>
        <p style="margin: 0 0 12px 0;">
          ${escapeHtml(params.siteName)} is now protected by an nginx <strong>client certificate</strong> (mTLS) gate. This is <strong>not</strong> used for sign-in;
          it only controls whether your browser can connect to the server.
        </p>

        <div style="background:#fff6e5;border:1px solid #ffd699;border-radius:6px;padding:12px 14px;margin:16px 0;">
          <strong>Attachment:</strong> <code>${attachmentName}</code> (password: <code>${escapeHtml(params.p12Password)}</code>). Treat it like a password.
        </div>

        <h3 style="margin: 18px 0 8px 0;">How to use</h3>
        <ol style="margin:0;padding-left:18px;color:#333;">
          <li>Download the attached <code>${attachmentName}</code>.</li>
          <li>Before import, remove any previously installed ${escapeHtml(params.siteName)} access certificate (Windows: open <strong>"Manage user certificates"</strong> / <code>certmgr.msc</code>).</li>
          <li>Import it into your OS / browser certificate store (PKCS#12 / Personal certificate).</li>
          <li>Fully restart your browser (or reboot your PC).</li>
          <li>Open <a href="${params.baseUrl}">${params.baseUrl}</a>. If prompted, select the access certificate.</li>
        </ol>
        ${adminSnippet}
        <p style="color:#666;font-size:12px;margin-top:20px;">
          If you believe you received this email in error, contact your administrator.
        </p>
      </div>
    `;

    return { subject, html };
  }

  private async resolveSendContext(): Promise<{ apiKey: string; fromEmail: string; baseUrl: string; siteName: string; } | null> {
    const domain = await resolveAppDomain(this.db);
    if (!domain) {
      this.loggingService.warn('system', 'Skipping email send; app domain not configured', {});
      return null;
    }
    const apiKey = await this.db.settings.getSettingValue(SETTING_KEYS.RESEND_API_KEY);
    const normalizedKey = apiKey?.trim() ?? '';
    if (!normalizedKey) {
      this.loggingService.warn('system', 'Skipping email send; Resend API key missing', {});
      return null;
    }
    const baseUrl = await resolveAppBaseUrl(this.db);
    if (!baseUrl) {
      this.loggingService.warn('system', 'Skipping email send; base URL unavailable', {});
      return null;
    }
    const siteName = await resolveSiteName(this.db);
    const fromEmail = this.fromEmailOverride ?? (await resolveFromEmail(this.db)) ?? `noreply@${domain}`;
    return {
      apiKey: normalizedKey,
      fromEmail,
      baseUrl,
      siteName
    };
  }

  private async resolveSendContextRequired(): Promise<{ apiKey: string; fromEmail: string; baseUrl: string; siteName: string; }> {
    const domain = await resolveAppDomain(this.db);
    if (!domain) {
      throw new AppDomainMissingError();
    }

    const apiKey = await this.db.settings.getSettingValue(SETTING_KEYS.RESEND_API_KEY);
    const normalizedKey = apiKey?.trim() ?? '';
    if (!normalizedKey) {
      throw new ResendApiKeyMissingError();
    }

    const scheme = isLocalDomain(domain) ? 'http' : 'https';
    const baseUrl = `${scheme}://${domain}`;
    const siteName = await resolveSiteName(this.db);
    const fromEmail = this.fromEmailOverride ?? (await resolveFromEmail(this.db)) ?? `noreply@${domain}`;

    return {
      apiKey: normalizedKey,
      fromEmail,
      baseUrl,
      siteName
    };
  }

  private renderCashImpactSummary(summary: CashImpactSummary | null): string {
    const baseStyle = 'margin:12px 0;color:#1f3b64;';
    if (!summary) {
      return `<p style="${baseStyle}">Estimated cash impact unavailable for these orders.</p>`;
    }

    if (summary.considered === 0) {
      return `<p style="${baseStyle}">Estimated cash impact unavailable &mdash; missing pricing for ${summary.missingPricing} of ${summary.eligible} eligible order${summary.missingPricing === 1 ? '' : 's'}.</p>`;
    }

    const formattedImpact = EmailService.USD_FORMATTER.format(summary.impact);
    const hasLimitOrders = summary.limitOrders > 0;
    const directionLabel = summary.impact > 0
      ? hasLimitOrders
        ? 'Estimated cash increase with limit fill weighting.'
        : 'Cash increases if all orders fill.'
      : summary.impact < 0
        ? hasLimitOrders
          ? 'Estimated cash decrease with limit fill weighting.'
          : 'Cash decreases if all orders fill.'
        : hasLimitOrders
          ? 'Estimated net cash change with limit fill weighting.'
          : 'No net cash change if all orders fill.';
    const emphasisColor = summary.impact > 0 ? '#1f7a1f' : summary.impact < 0 ? '#c0392b' : '#1f3b64';
    const missingText = summary.missingPricing > 0
      ? `<span style="color:#6c757d;margin-left:8px;">${summary.missingPricing} of ${summary.eligible} order${summary.missingPricing === 1 ? '' : 's'} missing price data.</span>`
      : '';
    const limitText = hasLimitOrders
      ? `<span style="color:#6c757d;margin-left:8px;">${summary.limitAdjusted} of ${summary.limitOrders} limit order${summary.limitOrders === 1 ? '' : 's'} adjusted for expected fills${summary.limitMissing > 0 ? `; ${summary.limitMissing} missing ticker data.` : '.'}</span>`
      : '';

    return `<p style="${baseStyle}"><strong style="color:${emphasisColor};">${formattedImpact}</strong> <span style="color:${emphasisColor};">${directionLabel}</span>${limitText}${missingText}</p>`;
  }

  private renderOrderSizeStats(stats: OrderSizeStats | null): string {
    const baseStyle = 'margin:12px 0;color:#1f3b64;';
    if (!stats) {
      return `<p style="${baseStyle}">Order size summary unavailable &mdash; missing price or quantity data.</p>`;
    }
    const min = EmailService.USD_FORMATTER.format(stats.min);
    const avg = EmailService.USD_FORMATTER.format(stats.avg);
    const max = EmailService.USD_FORMATTER.format(stats.max);
    return `<p style="${baseStyle}"><strong>Order size summary:</strong> min ${min}, avg ${avg}, max ${max}</p>`;
  }

}
