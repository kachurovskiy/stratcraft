import type { QueryResultRow } from 'pg';
import type {
  AccountOperation,
  AccountOperationStatus,
  AccountOperationType
} from '../../../shared/types/StrategyTemplate';
import type { TradingAccount } from '../../../shared/types/Account';
import type { AccountOperationDispatchCandidate } from '../types';
import { decryptValue } from '../../utils/encryption';
import { DbClient, type QueryValue } from '../core/DbClient';
import { toInteger, toNullableBoolean, toNullableInteger, toNullableNumber, trimToNull } from '../core/valueParsers';
import { TradesRepo } from './TradesRepo';

type AccountOperationSortField = 'triggeredAt' | 'statusUpdatedAt' | 'createdAt';

type AccountOperationQueryOptions = {
  since?: Date;
  until?: Date;
  limit?: number;
  offset?: number;
  order?: 'asc' | 'desc';
  sortBy?: AccountOperationSortField;
  ticker?: string;
  operationTypes?: AccountOperationType[];
  textFilter?: string;
};

type AccountOperationRow = QueryResultRow & {
  id: string;
  account_id: string;
  strategy_id: string;
  trade_id: string;
  ticker: string;
  operation_type: AccountOperationType;
  quantity: number | null;
  price: number | null;
  stop_loss: number | null;
  previous_stop_loss: number | null;
  triggered_at: Date;
  status: AccountOperationStatus;
  status_reason: string | null;
  status_updated_at: Date | null;
  attempt_count: number;
  last_attempt_at: Date | null;
  reason: string | null;
  order_id: string | null;
  last_payload: string | null;
  order_type: string | null;
  discount_applied: boolean | null;
  signal_confidence: number | null;
  account_cash_at_plan: number | null;
  days_held: number | null;
  created_at: Date;
  updated_at: Date;
  trade_entry_order_id?: string | null;
  trade_stop_order_id?: string | null;
  trade_exit_order_id?: string | null;
  entry_order_id?: string | null;
  stop_order_id?: string | null;
  exit_order_id?: string | null;
};

type CountRow = QueryResultRow & { count: number };

type StatusCountRow = QueryResultRow & { status: string; count: number };

type PendingDispatchRow = AccountOperationRow & {
  account_user_id: number;
  account_name: string;
  account_provider: string;
  account_environment: string;
  account_excluded_tickers: string;
  account_excluded_keywords: string;
  account_api_key: string;
  account_api_secret: string;
  account_created_at: Date;
  account_updated_at: Date;
  strategy_name: string;
  operation_user_id: number;
  user_email: string | null;
};

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

export class AccountOperationsRepo {
  constructor(
    private readonly db: DbClient,
    private readonly trades: TradesRepo
  ) {}

  private normalizeOperationOrderType(value: unknown): 'market' | 'limit' | null {
    if (typeof value !== 'string') {
      return null;
    }
    const normalized = value.trim().toLowerCase();
    return normalized === 'market' || normalized === 'limit' ? normalized : null;
  }

  private serializeAccountOperationPayload(payload?: Record<string, unknown> | null): string | null {
    if (!payload || typeof payload !== 'object') {
      return null;
    }
    try {
      return JSON.stringify(payload);
    } catch {
      return null;
    }
  }

  private parseAccountOperationPayload(raw: unknown): Record<string, unknown> | null {
    if (typeof raw !== 'string' || raw.trim().length === 0) {
      return null;
    }
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
    } catch {
      return null;
    }
  }

  private parseExcludedTickers(value?: string | null): string[] {
    if (typeof value !== 'string' || value.trim().length === 0) {
      return [];
    }
    try {
      const parsed = JSON.parse(value);
      if (!Array.isArray(parsed)) {
        return [];
      }
      return Array.from(
        new Set(parsed.filter((entry) => typeof entry === 'string').map((entry) => entry.trim()).filter(Boolean))
      );
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
      if (!Array.isArray(parsed)) {
        return [];
      }
      return Array.from(
        new Set(
          parsed
            .filter((entry) => typeof entry === 'string')
            .map((entry) => entry.toLowerCase().trim())
            .filter((entry) => entry.length > 0)
        )
      );
    } catch {
      return [];
    }
  }

  private mapAccountRow(row: AccountRow): TradingAccount {
    const rawApiKey = typeof row.api_key === 'string' ? row.api_key : '';
    const rawApiSecret = typeof row.api_secret === 'string' ? row.api_secret : '';
    return {
      id: row.id,
      userId: toInteger(row.user_id, 0),
      name: row.name,
      provider: row.provider,
      environment: row.environment,
      excludedTickers: this.parseExcludedTickers(row.excluded_tickers),
      excludedKeywords: this.parseExcludedKeywords(row.excluded_keywords),
      apiKey: rawApiKey ? decryptValue(rawApiKey) : '',
      apiSecret: rawApiSecret ? decryptValue(rawApiSecret) : '',
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at)
    };
  }

  private mapAccountOperation(row: AccountOperationRow): AccountOperation {
    const tradeId = typeof row.trade_id === 'string' ? row.trade_id : '';
    const reason = typeof row.reason === 'string' && row.reason.trim().length > 0 ? row.reason : null;
    const orderId = trimToNull(row.order_id);
    const orderType = this.normalizeOperationOrderType(row.order_type);
    const discountApplied = toNullableBoolean(row.discount_applied);
    const signalConfidence = toNullableNumber(row.signal_confidence);
    const accountCashAtPlan = toNullableNumber(row.account_cash_at_plan);
    const daysHeld = toNullableInteger(row.days_held);
    const lastPayload = this.parseAccountOperationPayload(row.last_payload);

    return {
      id: row.id,
      accountId: row.account_id,
      strategyId: row.strategy_id,
      tradeId,
      ticker: row.ticker,
      operationType: row.operation_type as AccountOperationType,
      quantity: row.quantity ?? null,
      price: row.price ?? null,
      stopLoss: row.stop_loss ?? null,
      previousStopLoss: row.previous_stop_loss ?? null,
      triggeredAt: new Date(row.triggered_at),
      status: row.status as AccountOperationStatus,
      statusReason: row.status_reason ?? null,
      statusUpdatedAt: row.status_updated_at ? new Date(row.status_updated_at) : new Date(row.created_at),
      attemptCount: Math.max(0, toInteger(row.attempt_count, 0)),
      lastAttemptAt: row.last_attempt_at ? new Date(row.last_attempt_at) : null,
      reason,
      orderId,
      lastPayload,
      orderType,
      discountApplied,
      signalConfidence,
      accountCashAtPlan,
      daysHeld,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      entryOrderId: trimToNull(row.trade_entry_order_id ?? row.entry_order_id),
      stopOrderId: trimToNull(row.trade_stop_order_id ?? row.stop_order_id),
      exitOrderId: trimToNull(row.trade_exit_order_id ?? row.exit_order_id)
    };
  }

  private getAccountOperationOrderColumn(sortBy?: AccountOperationSortField): string {
    switch (sortBy) {
      case 'statusUpdatedAt':
        return 'status_updated_at';
      case 'createdAt':
        return 'created_at';
      default:
        return 'triggered_at';
    }
  }

  private buildAccountOperationsFilter(
    baseCondition: string,
    baseParams: QueryValue[],
    statuses?: AccountOperationStatus[],
    options?: AccountOperationQueryOptions
  ): { whereClause: string; params: QueryValue[] } {
    const whereParts: string[] = [baseCondition];
    const params: QueryValue[] = [...baseParams];

    if (statuses && statuses.length > 0) {
      whereParts.push(`status IN (${statuses.map(() => '?').join(', ')})`);
      params.push(...statuses);
    }

    const since = options?.since instanceof Date && !Number.isNaN(options.since.getTime()) ? options.since : undefined;
    if (since) {
      whereParts.push('triggered_at >= ?');
      params.push(since.toISOString());
    }

    const until = options?.until instanceof Date && !Number.isNaN(options.until.getTime()) ? options.until : undefined;
    if (until) {
      whereParts.push('triggered_at <= ?');
      params.push(until.toISOString());
    }

    const ticker = typeof options?.ticker === 'string' ? options.ticker.trim().toUpperCase() : '';
    if (ticker.length > 0) {
      whereParts.push('UPPER(ticker) = ?');
      params.push(ticker);
    }

    if (Array.isArray(options?.operationTypes) && options.operationTypes.length > 0) {
      const validTypes = options.operationTypes.filter(
        (type): type is AccountOperationType =>
          type === 'open_position' || type === 'close_position' || type === 'update_stop_loss'
      );
      if (validTypes.length > 0) {
        whereParts.push(`operation_type IN (${validTypes.map(() => '?').join(', ')})`);
        params.push(...validTypes);
      }
    }

    const textFilter = typeof options?.textFilter === 'string' ? options.textFilter.trim() : '';
    if (textFilter.length > 0) {
      const pattern = `%${textFilter}%`;
      whereParts.push(
        `(ticker ILIKE ? OR trade_id ILIKE ? OR order_id ILIKE ? OR status_reason ILIKE ? OR COALESCE(reason, '') ILIKE ?)`
      );
      params.push(pattern, pattern, pattern, pattern, pattern);
    }

    return {
      whereClause: whereParts.join(' AND '),
      params
    };
  }

  private async countAccountOperations(
    baseCondition: string,
    baseParams: QueryValue[],
    statuses?: AccountOperationStatus[],
    options?: AccountOperationQueryOptions
  ): Promise<number> {
    const { whereClause, params } = this.buildAccountOperationsFilter(baseCondition, baseParams, statuses, options);
    const row = await this.db.get<CountRow>(
      `SELECT COUNT(*) AS count
       FROM account_operations
       WHERE ${whereClause}`,
      params
    );
    return Math.max(0, toInteger(row?.count, 0));
  }

  private async getAccountOperationStatusCounts(
    baseCondition: string,
    baseParams: QueryValue[],
    options?: AccountOperationQueryOptions
  ): Promise<Record<AccountOperationStatus, number>> {
    const { whereClause, params } = this.buildAccountOperationsFilter(baseCondition, baseParams, undefined, options);
    const rows = await this.db.all<StatusCountRow>(
      `SELECT status, COUNT(*) AS count
       FROM account_operations
       WHERE ${whereClause}
       GROUP BY status`,
      params
    );

    const counts: Record<AccountOperationStatus, number> = {
      pending: 0,
      sent: 0,
      skipped: 0,
      failed: 0
    };

    rows.forEach((row) => {
      const status = typeof row.status === 'string' ? (row.status as AccountOperationStatus) : undefined;
      if (!status || counts[status] === undefined) {
        return;
      }
      counts[status] = Math.max(0, toInteger(row.count, 0));
    });

    return counts;
  }

  private async fetchAccountOperations(
    baseCondition: string,
    baseParams: QueryValue[],
    statuses?: AccountOperationStatus[],
    options?: AccountOperationQueryOptions
  ): Promise<AccountOperation[]> {
    const { whereClause, params } = this.buildAccountOperationsFilter(baseCondition, baseParams, statuses, options);
    const orderDirection = options?.order === 'desc' ? 'DESC' : 'ASC';
    const orderColumn = this.getAccountOperationOrderColumn(options?.sortBy);

    let limitClause = '';
    let limitValue: number | undefined;
    if (options?.limit !== undefined) {
      const parsedLimit = Math.floor(Number(options.limit));
      if (Number.isFinite(parsedLimit) && parsedLimit > 0) {
        limitValue = parsedLimit;
        limitClause = ' LIMIT ?';
      }
    }

    let offsetClause = '';
    let offsetValue: number | undefined;
    if (options?.offset !== undefined) {
      const parsedOffset = Math.floor(Number(options.offset));
      if (Number.isFinite(parsedOffset) && parsedOffset > 0) {
        offsetValue = parsedOffset;
        offsetClause = ' OFFSET ?';
      }
    }

    const queryParams = [...params];
    if (limitValue !== undefined) {
      queryParams.push(limitValue);
    }
    if (offsetValue !== undefined) {
      queryParams.push(offsetValue);
    }

    const rows = await this.db.all<AccountOperationRow>(
      `SELECT id, account_id, strategy_id, trade_id, ticker, operation_type,
              quantity, price, stop_loss, previous_stop_loss, triggered_at, status, status_reason, status_updated_at,
              attempt_count, last_attempt_at, reason, order_id, order_type, discount_applied,
              signal_confidence, account_cash_at_plan, days_held, created_at, updated_at
       FROM account_operations
       WHERE ${whereClause}
       ORDER BY ${orderColumn} ${orderDirection}${limitClause}${offsetClause}`,
      queryParams
    );

    return rows.map((row) => this.mapAccountOperation(row));
  }

  async getAccountOperations(
    accountId: string,
    statuses?: AccountOperationStatus[],
    options?: AccountOperationQueryOptions
  ): Promise<AccountOperation[]> {
    return this.fetchAccountOperations('account_id = ?', [accountId], statuses, options);
  }

  async getAccountOperationsForAccounts(
    accountIds: string[],
    statuses?: AccountOperationStatus[],
    options?: AccountOperationQueryOptions
  ): Promise<AccountOperation[]> {
    if (!Array.isArray(accountIds) || accountIds.length === 0) {
      return [];
    }
    const placeholders = accountIds.map(() => '?').join(', ');
    return this.fetchAccountOperations(`account_id IN (${placeholders})`, accountIds, statuses, options);
  }

  async getAccountOperationsForStrategy(
    strategyId: string,
    statuses?: AccountOperationStatus[],
    options?: AccountOperationQueryOptions
  ): Promise<AccountOperation[]> {
    return this.fetchAccountOperations('strategy_id = ?', [strategyId], statuses, options);
  }

  async getAccountOperationsForTrade(
    tradeId: string,
    statuses?: AccountOperationStatus[],
    options?: AccountOperationQueryOptions
  ): Promise<AccountOperation[]> {
    return this.fetchAccountOperations('trade_id = ?', [tradeId], statuses, options);
  }

  async getAccountOperationById(operationId: string): Promise<AccountOperation | null> {
    const row = await this.db.get<AccountOperationRow>(
      `SELECT id, account_id, strategy_id, trade_id, ticker, operation_type,
              quantity, price, stop_loss, previous_stop_loss, triggered_at, status, status_reason, status_updated_at,
              attempt_count, last_attempt_at, reason, order_id, last_payload, order_type, discount_applied,
              signal_confidence, account_cash_at_plan, days_held, created_at, updated_at
       FROM account_operations
       WHERE id = ?`,
      [operationId]
    );
    return row ? this.mapAccountOperation(row) : null;
  }

  async getAccountOperationsByIds(operationIds: string[]): Promise<AccountOperation[]> {
    if (!Array.isArray(operationIds) || operationIds.length === 0) {
      return [];
    }
    const placeholders = operationIds.map(() => '?').join(', ');
    return this.fetchAccountOperations(`id IN (${placeholders})`, operationIds);
  }

  async countAccountOperationsForStrategy(
    strategyId: string,
    statuses?: AccountOperationStatus[],
    options?: AccountOperationQueryOptions
  ): Promise<number> {
    return this.countAccountOperations('strategy_id = ?', [strategyId], statuses, options);
  }

  async getAccountOperationStatusCountsForStrategy(
    strategyId: string,
    options?: AccountOperationQueryOptions
  ): Promise<Record<AccountOperationStatus, number>> {
    return this.getAccountOperationStatusCounts('strategy_id = ?', [strategyId], options);
  }

  async getPendingAccountOperationsForDispatch(): Promise<AccountOperationDispatchCandidate[]> {
    const rows = await this.db.all<PendingDispatchRow>(
      `SELECT
         ao.*,
         t.entry_order_id as trade_entry_order_id,
         t.stop_order_id as trade_stop_order_id,
         t.exit_order_id as trade_exit_order_id,
         a.id as account_id,
         a.user_id as account_user_id,
         a.name as account_name,
         a.provider as account_provider,
         a.environment as account_environment,
         a.excluded_tickers as account_excluded_tickers,
         a.excluded_keywords as account_excluded_keywords,
         a.api_key as account_api_key,
         a.api_secret as account_api_secret,
         a.created_at as account_created_at,
         a.updated_at as account_updated_at,
         s.name as strategy_name,
         COALESCE(s.user_id, a.user_id) as operation_user_id,
         u.email as user_email
       FROM account_operations ao
       INNER JOIN accounts a ON ao.account_id = a.id
       INNER JOIN strategies s ON ao.strategy_id = s.id
       LEFT JOIN trades t ON t.id = ao.trade_id
       LEFT JOIN users u ON u.id = COALESCE(s.user_id, a.user_id)
       WHERE ao.status = 'pending'
       ORDER BY ao.triggered_at ASC`
    );

    return rows.map((row) => {
      const accountRow: AccountRow = {
        id: row.account_id,
        user_id: row.account_user_id,
        name: row.account_name,
        provider: row.account_provider,
        environment: row.account_environment,
        excluded_tickers: row.account_excluded_tickers,
        excluded_keywords: row.account_excluded_keywords,
        api_key: row.account_api_key,
        api_secret: row.account_api_secret,
        created_at: row.account_created_at,
        updated_at: row.account_updated_at
      };

      return {
        operation: this.mapAccountOperation(row),
        account: this.mapAccountRow(accountRow),
        strategyName: row.strategy_name,
        userId:
          row.operation_user_id !== null && row.operation_user_id !== undefined
            ? toNullableInteger(row.operation_user_id)
            : null,
        userEmail: typeof row.user_email === 'string' ? row.user_email : null
      };
    });
  }

  async recordAccountOperationAttempt(
    operation: Pick<AccountOperation, 'id' | 'tradeId' | 'operationType'>,
    status: AccountOperationStatus,
    reason?: string,
    orderId?: string | null,
    payload?: Record<string, unknown> | null
  ): Promise<void> {
    const normalizedOrderId = trimToNull(orderId);
    const serializedPayload = this.serializeAccountOperationPayload(payload);
    await this.db.run(
      `UPDATE account_operations
          SET status = ?,
              status_reason = ?,
              order_id = COALESCE(?::text, order_id),
              last_payload = COALESCE(?::text, last_payload),
              status_updated_at = CURRENT_TIMESTAMP,
              attempt_count = attempt_count + 1,
              last_attempt_at = CURRENT_TIMESTAMP,
              updated_at = CURRENT_TIMESTAMP
        WHERE id = ?`,
      [status, reason ?? null, normalizedOrderId, serializedPayload, operation.id]
    );

    await this.trades.updateTradeOrderIdForOperation(operation, normalizedOrderId);
  }

  async clearAllAccountOperations(): Promise<number> {
    try {
      const result = await this.db.run('DELETE FROM account_operations');
      return result.changes || 0;
    } catch (error) {
      console.error('Failed to clear all account operations:', error);
      throw error;
    }
  }
}
