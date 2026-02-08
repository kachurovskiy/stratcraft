import type { PoolClient, QueryResultRow } from 'pg';
import type { CreateTradingAccountInput, TradingAccount } from '../../../shared/types/Account';
import { decryptValue, encryptValue } from '../../utils/encryption';
import { DbClient } from '../core/DbClient';
import { toInteger } from '../core/valueParsers';
import { StrategiesRepo } from './StrategiesRepo';

type AccountRow = QueryResultRow & {
  id: string;
  user_id: number;
  name: string;
  provider: string;
  environment: string;
  excluded_tickers: string;
  excluded_keywords: string;
  api_key: string;
  api_secret: string;
  created_at: Date;
  updated_at: Date;
};

type IdRow = QueryResultRow & {
  id: string;
};

type CountRow = QueryResultRow & {
  count: number;
};

export class AccountsRepo {
  constructor(
    private readonly db: DbClient,
    private readonly strategiesRepo: StrategiesRepo
  ) {}

  private parseExcludedTickers(value?: string | null): string[] {
    if (typeof value !== 'string' || value.trim().length === 0) {
      return [];
    }
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) {
        return Array.from(
          new Set(
            parsed
              .filter((entry) => typeof entry === 'string')
              .map((entry) => entry.toUpperCase().trim())
              .filter((entry) => entry.length > 0)
          )
        );
      }
      return [];
    } catch {
      return [];
    }
  }

  private parseExcludedKeywords(value?: string | null): string[] {
    if (typeof value !== 'string' || value.trim().length === 0) {
      return [];
    }
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) {
        return Array.from(
          new Set(
            parsed
              .filter((entry) => typeof entry === 'string')
              .map((entry) => entry.toLowerCase().trim())
              .filter((entry) => entry.length > 0)
          )
        );
      }
      return [];
    } catch {
      return [];
    }
  }

  private mapAccountRow(row: AccountRow): TradingAccount {
    return {
      id: row.id,
      userId: toInteger(row.user_id, 0),
      name: row.name,
      provider: row.provider,
      environment: row.environment,
      excludedTickers: this.parseExcludedTickers(row.excluded_tickers),
      excludedKeywords: this.parseExcludedKeywords(row.excluded_keywords),
      apiKey: row.api_key ? decryptValue(row.api_key) : '',
      apiSecret: row.api_secret ? decryptValue(row.api_secret) : '',
      createdAt: row.created_at,
      updatedAt: row.updated_at
    };
  }

  async createAccount(input: CreateTradingAccountInput): Promise<void> {
    await this.db.run(
      `INSERT INTO accounts (id, user_id, name, provider, environment, excluded_tickers, excluded_keywords, api_key, api_secret, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      [
        input.id,
        input.userId,
        input.name,
        input.provider,
        input.environment,
        JSON.stringify(input.excludedTickers ?? []),
        JSON.stringify(input.excludedKeywords ?? []),
        encryptValue(input.apiKey),
        encryptValue(input.apiSecret)
      ]
    );
  }

  async getAccountsForUser(userId: number): Promise<TradingAccount[]> {
    const rows = await this.db.all<AccountRow>(
      `SELECT id, user_id, name, provider, environment, excluded_tickers, excluded_keywords, api_key, api_secret, created_at, updated_at
       FROM accounts
       WHERE user_id = ?
       ORDER BY created_at DESC`,
      [userId]
    );
    return rows.map((row) => this.mapAccountRow(row));
  }

  async getLiveAccountCount(): Promise<number> {
    const row = await this.db.get<CountRow>(`SELECT COUNT(*) as count FROM accounts WHERE environment = 'live'`);
    return row?.count ?? 0;
  }

  async getAccountById(id: string, userId: number): Promise<TradingAccount | null> {
    const row = await this.db.get<AccountRow>(
      `SELECT id, user_id, name, provider, environment, excluded_tickers, excluded_keywords, api_key, api_secret, created_at, updated_at
       FROM accounts
       WHERE id = ? AND user_id = ?`,
      [id, userId]
    );
    if (!row) {
      return null;
    }
    return this.mapAccountRow(row);
  }

  async getAccountsByIds(accountIds: string[]): Promise<TradingAccount[]> {
    if (!Array.isArray(accountIds) || accountIds.length === 0) {
      return [];
    }
    const uniqueIds = Array.from(
      new Set(accountIds.filter((value) => typeof value === 'string' && value.trim().length > 0))
    );
    if (uniqueIds.length === 0) {
      return [];
    }
    const placeholders = uniqueIds.map(() => '?').join(', ');
    const rows = await this.db.all<AccountRow>(
      `SELECT id, user_id, name, provider, environment, excluded_tickers, excluded_keywords, api_key, api_secret, created_at, updated_at
       FROM accounts
       WHERE id IN (${placeholders})
       ORDER BY created_at DESC`,
      uniqueIds
    );
    return rows.map((row) => this.mapAccountRow(row));
  }

  async updateAccountExcludedTickers(id: string, userId: number, excludedTickers: string[]): Promise<boolean> {
    const result = await this.db.run(
      `UPDATE accounts
       SET excluded_tickers = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ? AND user_id = ?`,
      [JSON.stringify(excludedTickers ?? []), id, userId]
    );
    return (result?.changes ?? 0) > 0;
  }

  async updateAccountExcludedKeywords(id: string, userId: number, excludedKeywords: string[]): Promise<boolean> {
    const result = await this.db.run(
      `UPDATE accounts
       SET excluded_keywords = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ? AND user_id = ?`,
      [JSON.stringify(excludedKeywords ?? []), id, userId]
    );
    return (result?.changes ?? 0) > 0;
  }

  async deleteAccount(id: string, userId: number): Promise<boolean> {
    return await this.db.withTransaction(async (client) => {
      const account = await this.db.get<IdRow>(
        'SELECT id FROM accounts WHERE id = ? AND user_id = ?',
        [id, userId],
        client
      );

      if (!account) {
        return false;
      }

      const strategyRows = await this.db.all<IdRow>(
        'SELECT id FROM strategies WHERE account_id = ? AND user_id = ?',
        [id, userId],
        client
      );
      const strategyIds = Array.from(
        new Set(
          strategyRows
            .map((row) => (typeof row?.id === 'string' ? row.id.trim() : ''))
            .filter((strategyId) => strategyId.length > 0)
        )
      );

      if (strategyIds.length > 0) {
        await this.strategiesRepo.deleteStrategyRelatedData(strategyIds, client);
        const placeholders = strategyIds.map(() => '?').join(', ');
        await this.db.run(`DELETE FROM strategies WHERE id IN (${placeholders})`, strategyIds, client);
      }

      await this.db.run('DELETE FROM account_operations WHERE account_id = ?', [id], client);

      const result = await this.db.run('DELETE FROM accounts WHERE id = ? AND user_id = ?', [id, userId], client);
      return (result?.changes ?? 0) > 0;
    });
  }

  async clearAllAccounts(): Promise<{
    accountsDeleted: number;
    accountOperationsDeleted: number;
    strategiesUpdated: number;
  }> {
    try {
      return await this.db.withTransaction(async (client: PoolClient) => {
        const accountOperationsResult = await this.db.run('DELETE FROM account_operations', [], client);
        const strategiesResult = await this.db.run(
          `UPDATE strategies
           SET account_id = NULL,
               updated_at = CURRENT_TIMESTAMP
           WHERE account_id IS NOT NULL`,
          [],
          client
        );
        const accountsResult = await this.db.run('DELETE FROM accounts', [], client);

        return {
          accountsDeleted: accountsResult.changes || 0,
          accountOperationsDeleted: accountOperationsResult.changes || 0,
          strategiesUpdated: strategiesResult.changes || 0
        };
      });
    } catch (error) {
      console.error('Failed to clear all accounts:', error);
      throw error;
    }
  }
}
