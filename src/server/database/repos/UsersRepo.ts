import type { PoolClient, QueryResultRow } from 'pg';
import { DbClient } from '../core/DbClient';
import type {
  RawUserRow,
  RequestQuotaAction,
  RequestQuotaCheckResult,
  RequestQuotaIdentifierType,
  UserSessionRecord
} from '../types';
import type { StrategiesRepo } from './StrategiesRepo';

type UserRow = QueryResultRow & RawUserRow;

type UserSessionRow = QueryResultRow & {
  id: number;
  user_id: number;
  created_at: Date;
  expires_at: Date;
  last_seen_at: Date;
  created_ip: string | null;
  device_type: string;
};

type CountRow = QueryResultRow & { count: number };

type RequestQuotaRow = QueryResultRow & {
  action: string;
  identifier_type: string;
  identifier: string;
  window_started_at: Date;
  attempt_count: number;
  last_attempt_at: Date;
};

type IdRow = QueryResultRow & { id: string };

export class UsersRepo {
  constructor(
    private readonly db: DbClient,
    private readonly strategiesRepo: StrategiesRepo
  ) {}

  private mapUserSessionRow(row: UserSessionRow): UserSessionRecord {
    const createdIp =
      typeof row.created_ip === 'string' && row.created_ip.trim().length > 0 ? row.created_ip : null;
    const deviceType = row.device_type.trim().length > 0 ? row.device_type : 'unknown';

    return {
      id: row.id,
      userId: row.user_id,
      createdAt: row.created_at,
      expiresAt: row.expires_at,
      lastSeenAt: row.last_seen_at,
      createdIp,
      deviceType
    };
  }

  async updateUserOtp(email: string, otpCode: string, expiresAt: Date): Promise<void> {
    await this.db.run(
      `UPDATE users
       SET otp_code = ?,
             otp_expires_at = ?,
             updated_at = CURRENT_TIMESTAMP
       WHERE email = ?`,
      [otpCode, expiresAt, email]
    );
  }

  async createUserWithOtp(input: { email: string; role: string; otpCode: string; otpExpiresAt: Date }): Promise<void> {
    await this.db.run(
      `INSERT INTO users (email, role, otp_code, otp_expires_at, created_at, updated_at)
       VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      [input.email, input.role, input.otpCode, input.otpExpiresAt]
    );
  }

  async checkRequestQuota(input: {
    action: RequestQuotaAction;
    identifierType: RequestQuotaIdentifierType;
    identifier: string;
    windowMs: number;
    maxAttempts: number;
  }): Promise<RequestQuotaCheckResult> {
    const identifier = typeof input.identifier === 'string' ? input.identifier.trim() : '';
    const fallback: RequestQuotaCheckResult = {
      allowed: true,
      retryAfterMs: null,
      remaining: input.maxAttempts,
      limit: input.maxAttempts,
      windowMs: input.windowMs
    };
    if (!identifier) {
      return fallback;
    }

    const now = new Date();
    return this.db.withTransaction(async (client) => {
      const row = await this.db.get<RequestQuotaRow>(
        `SELECT
            action,
            identifier_type,
            identifier,
            window_started_at,
            attempt_count,
            last_attempt_at
         FROM request_quotas
         WHERE action = ?
           AND identifier_type = ?
           AND identifier = ?
         FOR UPDATE`,
        [input.action, input.identifierType, identifier],
        client
      );

      let windowStartedAt = row?.window_started_at ?? now;
      let attemptCount = row?.attempt_count ?? 0;
      if (!row || now.getTime() - windowStartedAt.getTime() >= input.windowMs) {
        windowStartedAt = now;
        attemptCount = 0;
      }

      attemptCount += 1;
      const allowed = attemptCount <= input.maxAttempts;

      if (row) {
        await this.db.run(
          `UPDATE request_quotas
             SET window_started_at = ?,
                 attempt_count = ?,
                 last_attempt_at = ?,
                 updated_at = CURRENT_TIMESTAMP
           WHERE action = ?
             AND identifier_type = ?
             AND identifier = ?`,
          [windowStartedAt, attemptCount, now, input.action, input.identifierType, identifier],
          client
        );
      } else {
        await this.db.run(
          `INSERT INTO request_quotas (
             action,
             identifier_type,
             identifier,
             window_started_at,
             attempt_count,
             last_attempt_at,
             created_at,
             updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          [input.action, input.identifierType, identifier, windowStartedAt, attemptCount, now],
          client
        );
      }

      const windowEndMs = windowStartedAt.getTime() + input.windowMs;
      const retryAfterMs = allowed ? null : Math.max(0, windowEndMs - now.getTime());

      return {
        allowed,
        retryAfterMs,
        remaining: allowed ? Math.max(0, input.maxAttempts - attemptCount) : 0,
        limit: input.maxAttempts,
        windowMs: input.windowMs
      };
    });
  }

  async clearRequestQuota(
    action: RequestQuotaAction,
    identifierType: RequestQuotaIdentifierType,
    identifier: string
  ): Promise<void> {
    const trimmed = typeof identifier === 'string' ? identifier.trim() : '';
    if (!trimmed) {
      return;
    }
    await this.db.run(
      `DELETE FROM request_quotas
        WHERE action = ?
          AND identifier_type = ?
          AND identifier = ?`,
      [action, identifierType, trimmed]
    );
  }

  async createUserWithInvite(input: {
    email: string;
    role: string;
    inviteTokenHash: string;
    inviteExpiresAt: Date;
  }): Promise<void> {
    await this.db.run(
      `INSERT INTO users (email, role, invite_token_hash, invite_expires_at, created_at, updated_at)
       VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      [input.email, input.role, input.inviteTokenHash, input.inviteExpiresAt]
    );
  }

  async updateUserInvite(email: string, inviteTokenHash: string, inviteExpiresAt: Date): Promise<void> {
    await this.db.run(
      `UPDATE users
       SET invite_token_hash = ?,
             invite_expires_at = ?,
             invite_used_at = NULL,
             updated_at = CURRENT_TIMESTAMP
       WHERE email = ?`,
      [inviteTokenHash, inviteExpiresAt, email]
    );
  }

  async recordSuccessfulLogin(email: string): Promise<void> {
    await this.db.run(
      `UPDATE users
         SET otp_code = NULL,
             otp_expires_at = NULL,
             invite_token_hash = NULL,
             invite_expires_at = NULL,
             updated_at = CURRENT_TIMESTAMP
       WHERE email = ?`,
      [email]
    );
  }

  async recordInviteLogin(userId: number): Promise<void> {
    await this.db.run(
      `UPDATE users
         SET invite_token_hash = NULL,
             invite_expires_at = NULL,
             invite_used_at = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [userId]
    );
  }

  async createUserSession(
    userId: number,
    sessionToken: string,
    expiresAt: Date,
    createdIp: string | null,
    deviceType: string
  ): Promise<void> {
    await this.db.run(
      `INSERT INTO user_sessions (user_id, session_token, expires_at, last_seen_at, created_ip, device_type)
       VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)`,
      [userId, sessionToken, expiresAt, createdIp, deviceType]
    );
  }

  async deleteUserSessionByToken(sessionToken: string): Promise<void> {
    const trimmed = typeof sessionToken === 'string' ? sessionToken.trim() : '';
    if (!trimmed) {
      return;
    }
    await this.db.run(
      `DELETE FROM user_sessions
       WHERE session_token = ?`,
      [trimmed]
    );
  }

  async deleteUserSessionById(sessionId: number): Promise<number> {
    const result = await this.db.run(
      `DELETE FROM user_sessions
       WHERE id = ?`,
      [sessionId]
    );
    return result.changes || 0;
  }

  async deleteUserSessionsByUserId(userId: number): Promise<number> {
    const result = await this.db.run(
      `DELETE FROM user_sessions
       WHERE user_id = ?`,
      [userId]
    );
    return result.changes || 0;
  }

  async updateUserSessionLastSeen(sessionToken: string, minIntervalMinutes = 5): Promise<void> {
    const safeIntervalMinutes =
      Number.isFinite(minIntervalMinutes) && minIntervalMinutes > 0 ? Math.round(minIntervalMinutes) : 5;
    await this.db.run(
      `UPDATE user_sessions
         SET last_seen_at = CURRENT_TIMESTAMP
       WHERE session_token = ?
         AND (
           last_seen_at IS NULL
           OR last_seen_at < (CURRENT_TIMESTAMP - INTERVAL '${safeIntervalMinutes} minutes')
         )`,
      [sessionToken]
    );
  }

  async deleteExpiredUserSessions(): Promise<number> {
    const result = await this.db.run(
      `DELETE FROM user_sessions
       WHERE expires_at <= CURRENT_TIMESTAMP`
    );
    return result.changes || 0;
  }

  async getUserByEmailRow(email: string): Promise<RawUserRow | null> {
    const row = await this.db.get<UserRow>('SELECT * FROM users WHERE email = ?', [email]);
    return row ?? null;
  }

  async getUserByInviteTokenHash(inviteTokenHash: string): Promise<RawUserRow | null> {
    const row = await this.db.get<UserRow>('SELECT * FROM users WHERE invite_token_hash = ?', [inviteTokenHash]);
    return row ?? null;
  }

  async getUserBySessionToken(sessionToken: string): Promise<RawUserRow | null> {
    const trimmed = typeof sessionToken === 'string' ? sessionToken.trim() : '';
    if (!trimmed) {
      return null;
    }

    const row = await this.db.get<UserRow>(
      `SELECT u.*
       FROM user_sessions s
       INNER JOIN users u ON u.id = s.user_id
       WHERE s.session_token = ?
         AND s.expires_at > CURRENT_TIMESTAMP
       LIMIT 1`,
      [trimmed]
    );

    if (!row) {
      await this.db.run(
        `DELETE FROM user_sessions
         WHERE session_token = ?
           AND expires_at <= CURRENT_TIMESTAMP`,
        [trimmed]
      );
      return null;
    }

    return row;
  }

  async listActiveUserSessions(): Promise<UserSessionRecord[]> {
    await this.deleteExpiredUserSessions();
    const rows = await this.db.all<UserSessionRow>(
      `SELECT id, user_id, created_at, expires_at, last_seen_at, created_ip, device_type
       FROM user_sessions
       WHERE expires_at > CURRENT_TIMESTAMP
       ORDER BY created_at DESC`
    );
    return rows.map((row) => this.mapUserSessionRow(row));
  }

  async getUserByIdRow(id: number): Promise<RawUserRow | null> {
    const row = await this.db.get<UserRow>('SELECT * FROM users WHERE id = ?', [id]);
    return row ?? null;
  }

  async listUsers(order: 'ASC' | 'DESC' = 'DESC'): Promise<RawUserRow[]> {
    const rows = await this.db.all<UserRow>(`SELECT * FROM users ORDER BY created_at ${order}`);
    return rows;
  }

  async listUsersByRole(role: string, order: 'ASC' | 'DESC' = 'ASC'): Promise<RawUserRow[]> {
    const rows = await this.db.all<UserRow>(`SELECT * FROM users WHERE role = ? ORDER BY created_at ${order}`, [role]);
    return rows;
  }

  async getUserCount(): Promise<number> {
    const row = await this.db.get<CountRow>('SELECT COUNT(*) as count FROM users');
    return row?.count ?? 0;
  }

  async deleteUserById(userId: number): Promise<number> {
    return await this.db.withTransaction(async (client) => {
      const strategyRows = await this.db.all<IdRow>('SELECT id FROM strategies WHERE user_id = ?', [userId], client);
      const strategyIds = Array.from(
        new Set(
          strategyRows
            .map((row) => row.id.trim())
            .filter((id) => id.length > 0)
        )
      );

      if (strategyIds.length > 0) {
        await this.strategiesRepo.deleteStrategyRelatedData(strategyIds, client);
        const placeholders = strategyIds.map(() => '?').join(', ');
        await this.db.run(`DELETE FROM strategies WHERE id IN (${placeholders})`, strategyIds, client);
      }

      await this.db.run('DELETE FROM trades WHERE user_id = ?', [userId], client);
      await this.db.run('DELETE FROM signals WHERE user_id = ?', [userId], client);

      const accountRows = await this.db.all<IdRow>('SELECT id FROM accounts WHERE user_id = ?', [userId], client);
      const accountIds = Array.from(
        new Set(
          accountRows
            .map((row) => row.id.trim())
            .filter((id) => id.length > 0)
        )
      );

      if (accountIds.length > 0) {
        const accountPlaceholders = accountIds.map(() => '?').join(', ');
        await this.db.run(
          `DELETE FROM account_operations WHERE account_id IN (${accountPlaceholders})`,
          accountIds,
          client
        );
        await this.db.run(
          `UPDATE strategies
             SET account_id = NULL,
                 updated_at = CURRENT_TIMESTAMP
           WHERE account_id IN (${accountPlaceholders})`,
          accountIds,
          client
        );
        await this.db.run(`DELETE FROM accounts WHERE id IN (${accountPlaceholders})`, accountIds, client);
      }

      const result = await this.db.run('DELETE FROM users WHERE id = ?', [userId], client);
      return result.changes ?? 0;
    });
  }

  async updateUserRole(userId: number, role: string): Promise<void> {
    await this.db.run(
      `UPDATE users
         SET role = ?,
             updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [role, userId]
    );
  }
}
