import { X509Certificate } from 'crypto';
import fs from 'fs';
import path from 'path';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { normalizeMtlsAccessCertPassword, SETTING_KEYS } from '../constants';
import type { Database } from '../database/Database';
import type { LoggingService } from './LoggingService';
import { resolveSiteName } from '../utils/appUrl';

const execFileAsync = promisify(execFile);

const DEFAULT_HELPER_PATH = '/usr/local/bin/stratcraft-mtls';
const DEFAULT_NGINX_MTLS_CONF_PATH = '/etc/nginx/stratcraft-mtls.conf';
const DEFAULT_COMMAND_TIMEOUT_MS = 120000;
const DEFAULT_CERT_LABEL = 'Site';
const MAX_CERT_LABEL_LENGTH = 48;

export type ClientCertLockdownState = {
  supported: boolean;
  helperAvailable: boolean;
  controlsEnabled: boolean;
  lockdownEnabled: boolean;
  bundleAvailable: boolean;
};

export type MtlsCertificateRotationEmailer = {
  sendClientCertificateLockdownEnabledEmailToAllUsers(params: {
    certificateBundle: Buffer;
  }): Promise<{ sent: number; adminCount: number; }>;
};

export class MtlsAccessBundleEmailError extends Error {
  readonly details: string;
  readonly rollbackSucceeded: boolean;
  readonly rollbackError?: string;
  readonly helperPath: string;

  constructor(details: string, rollbackSucceeded: boolean, helperPath: string, rollbackError?: string) {
    super(`Failed to email access certificate: ${details}`);
    this.name = 'MtlsAccessBundleEmailError';
    this.details = details;
    this.rollbackSucceeded = rollbackSucceeded;
    this.rollbackError = rollbackError;
    this.helperPath = helperPath;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

function describeCommandError(error: unknown, fallback: string): string {
  const stderr = typeof (error as any)?.stderr === 'string' ? (error as any).stderr.trim() : '';
  const stdout = typeof (error as any)?.stdout === 'string' ? (error as any).stdout.trim() : '';
  if (stderr) return `${fallback}: ${stderr}`;
  if (stdout) return `${fallback}: ${stdout}`;
  if (error instanceof Error && error.message) return `${fallback}: ${error.message}`;
  return fallback;
}

async function runCommand(
  command: string,
  args: string[],
  options?: { cwd?: string; timeoutMs?: number; }
): Promise<{ stdout: string; stderr: string; }> {
  return execFileAsync(command, args, {
    timeout: options?.timeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS,
    maxBuffer: 10 * 1024 * 1024,
    cwd: options?.cwd
  });
}

export class MtlsLockdownService {
  readonly mtlsDir: string;
  readonly caCertPath: string;
  readonly caKeyPath: string;
  readonly clientKeyPath: string;
  readonly clientCsrPath: string;
  readonly clientCertPath: string;
  readonly clientExtPath: string;
  readonly clientP12Path: string;
  readonly helperPath: string;
  readonly nginxConfPath: string;
  private readonly commandTimeoutMs: number;

  constructor(options?: {
    mtlsDir?: string;
    helperPath?: string;
    nginxConfPath?: string;
    commandTimeoutMs?: number;
  }) {
    this.mtlsDir =
      options?.mtlsDir ?? process.env.ADMIN_MTLS_DIR ?? path.join(process.cwd(), '.mtls');
    this.caCertPath = path.join(this.mtlsDir, 'ca.crt');
    this.caKeyPath = path.join(this.mtlsDir, 'ca.key');
    this.clientKeyPath = path.join(this.mtlsDir, 'client.key');
    this.clientCsrPath = path.join(this.mtlsDir, 'client.csr');
    this.clientCertPath = path.join(this.mtlsDir, 'client.crt');
    this.clientExtPath = path.join(this.mtlsDir, 'client.ext');
    this.clientP12Path = path.join(this.mtlsDir, 'stratcraft-access.p12');
    this.helperPath = options?.helperPath ?? DEFAULT_HELPER_PATH;
    this.nginxConfPath = options?.nginxConfPath ?? DEFAULT_NGINX_MTLS_CONF_PATH;
    this.commandTimeoutMs = options?.commandTimeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;
  }

  isSupported(): boolean {
    return process.platform === 'linux';
  }

  isClientCertificateBundleAvailable(): boolean {
    return fs.existsSync(this.clientP12Path);
  }

  isAccessBundleAvailable(): boolean {
    return fs.existsSync(this.clientP12Path) && fs.existsSync(this.caCertPath);
  }

  async readClientCertificateBundle(): Promise<Buffer> {
    return fs.promises.readFile(this.clientP12Path);
  }

  async getClientCertificateExpiry(): Promise<Date | null> {
    try {
      const pem = await fs.promises.readFile(this.clientCertPath, 'utf8');
      const certificate = new X509Certificate(pem);
      const expiry = new Date(certificate.validTo);
      if (Number.isNaN(expiry.getTime())) {
        return null;
      }
      return expiry;
    } catch {
      return null;
    }
  }

  async generateClientCertificateBundleFromDatabase(db: Database): Promise<void> {
    const rawPassword = await db.settings.getSettingValue(SETTING_KEYS.MTLS_ACCESS_CERT_PASSWORD);
    const p12Password = normalizeMtlsAccessCertPassword(rawPassword);
    await this.generateClientCertificateBundle(p12Password, await resolveSiteName(db));
  }

  async emailAccessBundleToAllUsersOrRollback(options: {
    emailService: MtlsCertificateRotationEmailer;
  }): Promise<{ sent: number; adminCount: number; }> {
    try {
      const certificateBundle = await this.readClientCertificateBundle();
      return await options.emailService.sendClientCertificateLockdownEnabledEmailToAllUsers({
        certificateBundle
      });
    } catch (error) {
      const details = error instanceof Error ? error.message : String(error);
      let rollbackSucceeded = false;
      let rollbackError: string | undefined;
      try {
        await this.disableLockdown();
        rollbackSucceeded = true;
      } catch (rollback) {
        rollbackError = rollback instanceof Error ? rollback.message : String(rollback);
      }
      throw new MtlsAccessBundleEmailError(details, rollbackSucceeded, this.helperPath, rollbackError);
    }
  }

  async generateClientCertificateBundle(p12Password: string, siteName: string): Promise<void> {
    if (!this.isSupported()) {
      throw new Error('Client certificate issuance is only supported on Linux deployments.');
    }

    const normalizedPassword = typeof p12Password === 'string' ? p12Password.trim() : '';
    if (normalizedPassword.length === 0) {
      throw new Error('Client certificate bundle export password must not be empty.');
    }

    const certLabel = MtlsLockdownService.normalizeCertificateLabel(siteName);
    const caCommonName = `${certLabel} Client CA`;
    const accessCommonName = `${certLabel} Access`;

    await this.ensureMtlsDir();

    await fs.promises.writeFile(
      this.clientExtPath,
      [
        'basicConstraints=critical,CA:FALSE',
        'keyUsage=critical,digitalSignature,keyEncipherment',
        'extendedKeyUsage=clientAuth',
        'subjectKeyIdentifier=hash',
        'authorityKeyIdentifier=keyid,issuer'
      ].join('\n') + '\n',
      { mode: 0o600 }
    );

    await runCommand('openssl', ['genrsa', '-out', this.caKeyPath, '4096'], {
      cwd: this.mtlsDir,
      timeoutMs: this.commandTimeoutMs
    });
    await runCommand(
      'openssl',
      [
        'req',
        '-x509',
        '-new',
        '-nodes',
        '-key',
        this.caKeyPath,
        '-sha256',
        '-days',
        '3650',
        '-subj',
        `/CN=${caCommonName}`,
        '-addext',
        'basicConstraints=critical,CA:true',
        '-addext',
        'keyUsage=critical,keyCertSign,cRLSign',
        '-out',
        this.caCertPath
      ],
      { cwd: this.mtlsDir, timeoutMs: this.commandTimeoutMs }
    );

    await runCommand('openssl', ['genrsa', '-out', this.clientKeyPath, '2048'], {
      cwd: this.mtlsDir,
      timeoutMs: this.commandTimeoutMs
    });
    await runCommand(
      'openssl',
      [
        'req',
        '-new',
        '-key',
        this.clientKeyPath,
        '-out',
        this.clientCsrPath,
        '-subj',
        `/CN=${accessCommonName}`
      ],
      { cwd: this.mtlsDir, timeoutMs: this.commandTimeoutMs }
    );

    await runCommand(
      'openssl',
      [
        'x509',
        '-req',
        '-in',
        this.clientCsrPath,
        '-CA',
        this.caCertPath,
        '-CAkey',
        this.caKeyPath,
        '-CAcreateserial',
        '-out',
        this.clientCertPath,
        '-days',
        '825',
        '-sha256',
        '-extfile',
        this.clientExtPath
      ],
      { cwd: this.mtlsDir, timeoutMs: this.commandTimeoutMs }
    );

    await runCommand(
      'openssl',
      [
        'pkcs12',
        '-export',
        '-out',
        this.clientP12Path,
        '-inkey',
        this.clientKeyPath,
        '-in',
        this.clientCertPath,
        '-certfile',
        this.caCertPath,
        '-name',
        accessCommonName,
        '-passout',
        `pass:${normalizedPassword}`
      ],
      { cwd: this.mtlsDir, timeoutMs: this.commandTimeoutMs }
    );

    await Promise.all([
      fs.promises.chmod(this.caKeyPath, 0o600).catch(() => undefined),
      fs.promises.chmod(this.clientKeyPath, 0o600).catch(() => undefined),
      fs.promises.chmod(this.clientP12Path, 0o600).catch(() => undefined),
      fs.promises.chmod(this.caCertPath, 0o644).catch(() => undefined),
      fs.promises.chmod(this.clientCertPath, 0o644).catch(() => undefined)
    ]);
  }

  async getLockdownState(): Promise<ClientCertLockdownState> {
    const supported = this.isSupported();
    if (!supported) {
      return {
        supported,
        helperAvailable: false,
        controlsEnabled: false,
        lockdownEnabled: false,
        bundleAvailable: false
      };
    }

    const helperAvailable = fs.existsSync(this.helperPath);
    const bundleAvailable = this.isAccessBundleAvailable();

    let lockdownEnabled = false;
    try {
      if (fs.existsSync(this.nginxConfPath)) {
        const contents = fs.readFileSync(this.nginxConfPath, 'utf8');
        lockdownEnabled = contents.includes('ssl_verify_client on;');
      }
    } catch {
      // ignore read failures
    }

    let controlsEnabled = false;
    if (helperAvailable) {
      try {
        await this.runHelperCommand('status');
        controlsEnabled = true;
      } catch {
        controlsEnabled = false;
      }
    }

    return {
      supported,
      helperAvailable,
      controlsEnabled,
      lockdownEnabled,
      bundleAvailable
    };
  }

  async enableLockdown(): Promise<void> {
    await this.runHelperCommand('enable');
  }

  async disableLockdown(): Promise<void> {
    await this.runHelperCommand('disable');
  }

  async handleExpiredClientCertificateOnStartup(options: {
    loggingService: LoggingService;
    db: Database;
    emailService: MtlsCertificateRotationEmailer;
  }): Promise<void> {
    const { loggingService, db, emailService } = options;
    let state: ClientCertLockdownState;
    try {
      state = await this.getLockdownState();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      loggingService.warn('system', 'Failed to check mTLS lockdown state on startup', {
        error: message
      });
      return;
    }

    if (!state.supported || !state.lockdownEnabled || !state.helperAvailable) {
      return;
    }

    if (!state.controlsEnabled) {
      loggingService.warn('system', 'mTLS helper is unavailable for automatic certificate rotation', {
        helperPath: this.helperPath
      });
      return;
    }

    const expiry = await this.getClientCertificateExpiry();
    if (!expiry) {
      loggingService.warn('system', 'Unable to determine mTLS client certificate expiry on startup', {
        clientCertPath: this.clientCertPath
      });
      return;
    }

    const now = new Date();
    if (expiry.getTime() > now.getTime()) {
      return;
    }

    loggingService.warn('system', 'Expired mTLS client certificate detected; rotating', {
      expiredAt: expiry.toISOString()
    });

    try {
      await this.generateClientCertificateBundleFromDatabase(db);
      await this.enableLockdown();

      const { sent, adminCount } = await this.emailAccessBundleToAllUsersOrRollback({
        emailService
      });

      loggingService.info('system', 'Rotated expired mTLS certificate and emailed users', {
        sent,
        adminCount
      });
    } catch (error) {
      if (error instanceof MtlsAccessBundleEmailError) {
        loggingService.error('system', 'Failed to rotate expired mTLS certificate', {
          error: error.details
        });
        if (error.rollbackSucceeded) {
          loggingService.warn('system', 'Lockdown disabled after failed mTLS certificate rotation email');
        } else if (error.rollbackError) {
          loggingService.error('system', 'Failed to disable lockdown after rotation failure', {
            error: error.rollbackError
          });
        }
        return;
      }

      const message = error instanceof Error ? error.message : String(error);
      loggingService.error('system', 'Failed to rotate expired mTLS certificate', {
        error: message
      });

      try {
        await this.disableLockdown();
        loggingService.warn('system', 'Lockdown disabled after failed mTLS certificate rotation email');
      } catch (rollbackError) {
        const rollbackMessage = rollbackError instanceof Error ? rollbackError.message : String(rollbackError);
        loggingService.error('system', 'Failed to disable lockdown after rotation failure', {
          error: rollbackMessage
        });
      }
    }
  }

  private static normalizeCertificateLabel(siteName?: string): string {
    const normalized = (siteName ?? '')
      .replace(/[^A-Za-z0-9 ._-]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    if (normalized.length === 0) {
      return DEFAULT_CERT_LABEL;
    }
    return normalized.slice(0, MAX_CERT_LABEL_LENGTH).trim();
  }

  private async ensureMtlsDir(): Promise<void> {
    await fs.promises.mkdir(this.mtlsDir, { recursive: true, mode: 0o700 });
    try {
      await fs.promises.chmod(this.mtlsDir, 0o700);
    } catch {
      // ignore chmod failures on some filesystems
    }
  }

  private async runHelperCommand(subcommand: 'status' | 'enable' | 'disable'): Promise<void> {
    if (!this.isSupported()) {
      throw new Error('Client certificate lockdown is only supported on Linux deployments.');
    }

    if (!fs.existsSync(this.helperPath)) {
      throw new Error(
        'nginx client-cert helper is missing. Re-run deploy.sh update or follow the manual nginx instructions.'
      );
    }

    try {
      await runCommand('sudo', ['-n', this.helperPath, subcommand], {
        timeoutMs: this.commandTimeoutMs
      });
    } catch (error) {
      const actionLabel =
        subcommand === 'status' ? 'check status of' : `${subcommand} nginx`;
      throw new Error(describeCommandError(error, `Failed to ${actionLabel} client certificate lockdown`));
    }
  }
}
