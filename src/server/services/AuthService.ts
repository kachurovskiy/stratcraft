import { Database } from '../database/Database';
import type { RequestQuotaAction } from '../database/types';
import { AppDomainMissingError, EmailService, ResendApiKeyMissingError } from './EmailService';
import { LoggingService } from './LoggingService';
import * as crypto from 'crypto';

type QuotaConfig = {
  windowMs: number;
  maxAttempts: number;
};

const OTP_EXPIRATION_MS = 10 * 60 * 1000;
const OTP_DIGITS = 6;
const OTP_QUOTA_WINDOW_MS = 24 * 60 * 60 * 1000;

const OTP_SEND_EMAIL_QUOTA: QuotaConfig = {
  windowMs: OTP_QUOTA_WINDOW_MS,
  maxAttempts: 5
};

const OTP_SEND_IP_QUOTA: QuotaConfig = {
  windowMs: OTP_QUOTA_WINDOW_MS,
  maxAttempts: 10
};

const OTP_VERIFY_EMAIL_QUOTA: QuotaConfig = {
  windowMs: OTP_QUOTA_WINDOW_MS,
  maxAttempts: 5
};

const OTP_VERIFY_IP_QUOTA: QuotaConfig = {
  windowMs: OTP_QUOTA_WINDOW_MS,
  maxAttempts: 10
};

const SAFE_EMAIL_LOCAL_REGEX = /^[A-Za-z0-9._%+-]+$/;
const SAFE_EMAIL_DOMAIN_REGEX = /^[A-Za-z0-9.-]+$/;
const HASHED_TOKEN_PREFIX = 'sha256:';

export interface User {
  id: number;
  email: string;
  role: 'admin' | 'user';
  createdAt: Date;
  updatedAt: Date;
}

export interface AuthSession {
  id: number;
  userId: number;
  email: string;
  role: 'admin' | 'user';
}

export class RequestQuotaError extends Error {
  retryAfterMs: number | null;

  constructor(message: string, retryAfterMs: number | null = null) {
    super(message);
    this.retryAfterMs = retryAfterMs;
    this.name = 'RequestQuotaError';
  }
}

export class AuthService {
  private db: Database;
  private emailService: EmailService;
  private loggingService: LoggingService;

  constructor(db: Database, loggingService: LoggingService) {
    this.db = db;
    this.loggingService = loggingService;
    this.emailService = new EmailService(loggingService, db);
  }

  async generateOTP(): Promise<string> {
    // Generate a 6-digit OTP
    const value = crypto.randomInt(0, 10 ** OTP_DIGITS);
    return value.toString().padStart(OTP_DIGITS, '0');
  }

  async sendOTP(
    email: string,
    ipAddress?: string
  ): Promise<{ emailSent: boolean; fallbackOtp?: string }> {
    const trimmedEmail = email.trim();
    await this.enforceOtpQuota('send_otp', trimmedEmail, ipAddress, OTP_SEND_EMAIL_QUOTA, OTP_SEND_IP_QUOTA);
    const otpCode = await this.generateOTP();
    const expiresAt = new Date(Date.now() + OTP_EXPIRATION_MS); // 10 minutes from now

    if (await this.getUserByEmail(trimmedEmail)) {
      await this.db.users.updateUserOtp(trimmedEmail, this.hashWithPrefix(otpCode), expiresAt);
    } else if (await this.db.users.getUserCount() === 0) {
      await this.db.users.createUserWithOtp({
        email: trimmedEmail,
        role: 'admin',
        otpCode: this.hashWithPrefix(otpCode),
        otpExpiresAt: expiresAt
      });
    } else {
      return { emailSent: false };
    }

    try {
      await this.emailService.sendOTP(trimmedEmail, otpCode);
      return { emailSent: true };
    } catch (error) {
      if (
        await this.db.accounts.getLiveAccountCount() === 0
        && (error instanceof ResendApiKeyMissingError || error instanceof AppDomainMissingError)
      ) {
        this.loggingService.warn('auth', 'OTP email failed; exposing OTP for bootstrap login', {
          email: trimmedEmail,
          error: error instanceof Error ? error.message : String(error)
        });
        return { emailSent: false, fallbackOtp: otpCode };
      }
      throw error;
    }
  }

  private generateInviteToken(): string {
    return crypto.randomBytes(32).toString('hex');
  }

  private hashToken(token: string): string {
    return crypto.createHash('sha256').update(token).digest('hex');
  }

  private hashWithPrefix(value: string): string {
    return `${HASHED_TOKEN_PREFIX}${this.hashToken(value)}`;
  }

  private generateSessionTokenValue(): string {
    return crypto.randomBytes(32).toString('hex');
  }

  private resolveDeviceType(userAgent: string): string {
    const ua = userAgent.toLowerCase();
    if (ua.includes('iphone')) return 'iPhone';
    if (ua.includes('ipad')) return 'iPad';
    if (ua.includes('ipod')) return 'iPod';

    const androidMatch = userAgent.match(/Android [\d.]+; ([^;)\[]+)/i);
    if (androidMatch && androidMatch[1]) {
      const model = androidMatch[1].replace(/Build\/.*$/i, '').trim();
      if (model.length > 0) {
        return model;
      }
    }

    const isTablet = ua.includes('tablet')
      || ua.includes('kindle')
      || ua.includes('silk')
      || ua.includes('playbook')
      || (ua.includes('android') && !ua.includes('mobile'));
    if (isTablet) {
      return 'tablet';
    }
    const isMobile = ua.includes('mobi')
      || ua.includes('iphone')
      || ua.includes('ipod')
      || ua.includes('android')
      || ua.includes('windows phone');
    if (isMobile) {
      return 'mobile';
    }
    return 'desktop';
  }

  async inviteUser(
    email: string,
    inviteDays: number,
    inviteLinkBase: string
  ): Promise<{ inviteLink: string; expiresAt: Date }> {
    const trimmedEmail = email.trim();
    if (!this.isReasonableSafeEmail(trimmedEmail)) {
      throw new Error('Invalid email address');
    }

    const validDays = Number.isFinite(inviteDays) && inviteDays > 0 ? Math.trunc(inviteDays) : 7;
    const expiresAt = new Date(Date.now() + validDays * 24 * 60 * 60 * 1000);
    const token = this.generateInviteToken();
    const tokenHash = this.hashToken(token);

    const existingUser = await this.getUserByEmail(trimmedEmail);
    if (existingUser) {
      await this.db.users.updateUserInvite(trimmedEmail, tokenHash, expiresAt);
      this.loggingService.info('auth', 'Invite created for existing user', {
        email: trimmedEmail,
        user_id: existingUser.id
      });
    } else {
      await this.db.users.createUserWithInvite({
        email: trimmedEmail,
        role: 'user',
        inviteTokenHash: tokenHash,
        inviteExpiresAt: expiresAt
      });
      this.loggingService.info('auth', 'Invite created for new user', { email: trimmedEmail });
    }

    const normalizedBase = inviteLinkBase.endsWith('/')
      ? inviteLinkBase.slice(0, -1)
      : inviteLinkBase;
    const inviteLink = `${normalizedBase}/auth/invite?token=${encodeURIComponent(token)}`;

    await this.emailService.sendInvitation(trimmedEmail, inviteLink, expiresAt, validDays);

    return { inviteLink, expiresAt };
  }

  private isReasonableSafeEmail(email: string): boolean {
    if (!email) {
      return false;
    }
    const trimmed = email.trim();
    if (trimmed.length === 0 || trimmed.length > 254) {
      return false;
    }
    const parts = trimmed.split('@');
    if (parts.length !== 2) {
      return false;
    }
    const [local, domain] = parts;
    if (!local || !domain) {
      return false;
    }
    if (local.length > 64 || domain.length > 253) {
      return false;
    }
    if (local.startsWith('.') || local.endsWith('.') || local.includes('..')) {
      return false;
    }
    if (domain.startsWith('.') || domain.endsWith('.') || domain.includes('..')) {
      return false;
    }
    if (!SAFE_EMAIL_LOCAL_REGEX.test(local)) {
      return false;
    }
    if (!SAFE_EMAIL_DOMAIN_REGEX.test(domain)) {
      return false;
    }
    if (!domain.includes('.')) {
      return false;
    }
    const labels = domain.split('.');
    for (const label of labels) {
      if (!label || label.length > 63) {
        return false;
      }
      if (label.startsWith('-') || label.endsWith('-')) {
        return false;
      }
    }
    return true;
  }

  async verifyInviteToken(token: string): Promise<AuthSession | null> {
    if (!token || typeof token !== 'string') {
      return null;
    }

    const tokenHash = this.hashToken(token);
    const user = await this.db.users.getUserByInviteTokenHash(tokenHash);

    if (!user) {
      this.loggingService.warn('auth', 'Invalid invite token used', { token_hash: tokenHash });
      return null;
    }

    if (user.invite_used_at) {
      this.loggingService.warn('auth', 'Invite token already used', { email: user.email, user_id: user.id });
      return null;
    }

    if (!user.invite_expires_at || new Date() > user.invite_expires_at) {
      this.loggingService.warn('auth', 'Expired invite token used', { email: user.email, user_id: user.id });
      return null;
    }

    await this.db.users.recordInviteLogin(user.id);

    this.loggingService.info('auth', 'User authenticated via invite link', {
      email: user.email,
      user_id: user.id,
      role: user.role
    });

    return {
      id: user.id,
      userId: user.id,
      email: user.email,
      role: user.role as User['role']
    };
  }

  async verifyOTP(email: string, otpCode: string, ipAddress?: string): Promise<AuthSession | null> {
    const trimmedEmail = email.trim();
    await this.enforceOtpQuota('verify_otp', trimmedEmail, ipAddress, OTP_VERIFY_EMAIL_QUOTA, OTP_VERIFY_IP_QUOTA);
    const user = await this.getUserByEmail(trimmedEmail);
    const normalizedOtp = otpCode.trim();

    if (!user || !user.otpCode) {
      this.loggingService.warn('auth', 'Invalid OTP attempt', { email: trimmedEmail });
      return null;
    }

    const hashedOtp = this.hashWithPrefix(normalizedOtp);
    const otpMatches = user.otpCode.startsWith(HASHED_TOKEN_PREFIX)
      ? user.otpCode === hashedOtp
      : user.otpCode === normalizedOtp;

    if (!otpMatches) {
      this.loggingService.warn('auth', 'Invalid OTP attempt', { email: trimmedEmail });
      return null;
    }

    // Check if OTP is expired
    if (!user.otpExpiresAt || new Date() > user.otpExpiresAt) {
      this.loggingService.warn('auth', 'Expired OTP attempt', { email: trimmedEmail });
      return null;
    }

    // Clear OTP and update last login
    await this.db.users.recordSuccessfulLogin(trimmedEmail);
    await this.db.users.clearRequestQuota('verify_otp', 'email', trimmedEmail);

    this.loggingService.info('auth', 'User successfully authenticated', {
      email: trimmedEmail,
      user_id: user.id,
      role: user.role
    });

    return {
      id: user.id,
      userId: user.id,
      email: user.email,
      role: user.role
    };
  }

  private async enforceOtpQuota(
    action: RequestQuotaAction,
    email: string,
    ipAddress: string | undefined,
    emailQuota: QuotaConfig,
    ipQuota: QuotaConfig
  ): Promise<void> {
    const emailIdentifier = typeof email === 'string' ? email.trim() : '';
    const ipIdentifier = typeof ipAddress === 'string' ? ipAddress.trim() : '';
    const checks = [
      this.db.users.checkRequestQuota({
        action,
        identifierType: 'email',
        identifier: emailIdentifier,
        windowMs: emailQuota.windowMs,
        maxAttempts: emailQuota.maxAttempts
      })
    ];

    if (ipIdentifier) {
      checks.push(
        this.db.users.checkRequestQuota({
          action,
          identifierType: 'ip',
          identifier: ipIdentifier,
          windowMs: ipQuota.windowMs,
          maxAttempts: ipQuota.maxAttempts
        })
      );
    }

    const results = await Promise.all(checks);
    const blocked = results.find(result => !result.allowed);
    if (blocked && !blocked.allowed) {
      throw new RequestQuotaError('Too many attempts. Please wait and try again.', blocked.retryAfterMs);
    }
  }

  async getUserByEmail(email: string): Promise<(User & { otpCode?: string; otpExpiresAt?: Date }) | null> {
    const result = await this.db.users.getUserByEmailRow(email);

    if (!result) return null;

    return {
      id: result.id,
      email: result.email,
      role: result.role as User['role'],
      createdAt: result.created_at,
      updatedAt: result.updated_at,
      otpCode: result.otp_code ?? undefined,
      otpExpiresAt: result.otp_expires_at ?? undefined
    };
  }

  async getUserById(id: number): Promise<User | null> {
    const result = await this.db.users.getUserByIdRow(id);

    if (!result) return null;

    return {
      id: result.id,
      email: result.email,
      role: result.role as User['role'],
      createdAt: result.created_at,
      updatedAt: result.updated_at
    };
  }

  async getAllUsers(): Promise<User[]> {
    const results = await this.db.users.listUsers();

    return results.map(result => ({
      id: result.id,
      email: result.email,
      role: result.role as User['role'],
      createdAt: result.created_at,
      updatedAt: result.updated_at
    }));
  }

  async getAdminUsers(): Promise<User[]> {
    const results = await this.db.users.listUsersByRole('admin', 'ASC');

    return results.map(result => ({
      id: result.id,
      email: result.email,
      role: result.role as User['role'],
      createdAt: result.created_at,
      updatedAt: result.updated_at
    }));
  }

  async deleteUser(userId: number): Promise<void> {
    // First check if this is the last admin user
    const adminUsers = await this.getAdminUsers();
    const userToDelete = await this.getUserById(userId);

    if (!userToDelete) {
      throw new Error('User not found');
    }

    if (userToDelete.role === 'admin' && adminUsers.length === 1) {
      throw new Error('Cannot delete the last admin user');
    }

    // Delete the user
    const deleted = await this.db.users.deleteUserById(userId);

    if (deleted === 0) {
      throw new Error('User not found or already deleted');
    }

    // Log the deletion
    await this.loggingService.log('auth', 'info', `User deleted: ${userToDelete.email}`, {
      deletedUserId: userId,
      deletedUserEmail: userToDelete.email,
      deletedUserRole: userToDelete.role
    });
  }

  async promoteToAdmin(userId: number): Promise<void> {
    const user = await this.getUserById(userId);
    if (!user) {
      throw new Error('User not found');
    }

    if (user.role === 'admin') {
      // Nothing to do
      return;
    }

    await this.db.users.updateUserRole(userId, 'admin');

    await this.loggingService.log('auth', 'info', `User promoted to admin: ${user.email}`, {
      promotedUserId: userId,
      promotedUserEmail: user.email
    });
  }

  async generateSessionToken(
    session: AuthSession,
    expiresAt: Date,
    ipAddress: string,
    userAgent: string,
  ): Promise<string> {
    const token = this.generateSessionTokenValue();
    const deviceType = this.resolveDeviceType(userAgent);
    await this.db.users.createUserSession(session.userId, this.hashWithPrefix(token), expiresAt, ipAddress, deviceType);
    return token;
  }

  async verifySessionToken(token: string): Promise<AuthSession | null> {
    if (!token || typeof token !== 'string') {
      return null;
    }

    try {
      const hashedToken = this.hashWithPrefix(token);
      const user = await this.db.users.getUserBySessionToken(hashedToken);
      if (!user) {
        return null;
      }

      void this.db.users.updateUserSessionLastSeen(hashedToken).catch((error) => {
        this.loggingService.warn('auth', 'Failed to update session last seen time', {
          session_token_suffix: token.slice(-6),
          error: error instanceof Error ? error.message : String(error)
        });
      });

      return {
        id: user.id,
        userId: user.id,
        email: user.email,
        role: user.role as User['role']
      };
    } catch (error) {
      return null;
    }
  }

  async clearSessionToken(token: string): Promise<void> {
    if (!token || typeof token !== 'string') {
      return;
    }
    const hashedToken = this.hashWithPrefix(token);
    await this.db.users.deleteUserSessionByToken(hashedToken);
  }

  async clearAllSessionsForUser(userId: number): Promise<number> {
    if (!Number.isFinite(userId)) {
      return 0;
    }
    return this.db.users.deleteUserSessionsByUserId(userId);
  }
}
