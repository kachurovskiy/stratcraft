import type { QueryResultRow } from 'pg';
import type {
  AccountOperation,
  AccountOperationType,
  Trade,
  TradeCancellationSource,
  TradeChange
} from '../../types/StrategyTemplate';
import { normalizeUppercaseString } from '../../utils/stringNormalization';
import { DbClient, type QueryValue } from '../core/DbClient';
import { toNullableInteger, toNullableNumber, trimToNull } from '../core/valueParsers';
import type { EntryFillGapHistogram, TradeTickerStats, TradeVolumeSegmentStats } from '../types';

type TradeStatus = 'pending' | 'active' | 'closed' | 'cancelled';

type TradeRow = QueryResultRow & {
  id: string;
  strategy_id: string;
  backtest_result_id: string | null;
  ticker: string;
  quantity: number;
  price: number;
  date: string;
  status: string;
  cancellation_source: string | null;
  pnl: number | null;
  fee: number;
  exit_price: number | null;
  exit_date: string | null;
  stop_loss: number | null;
  stop_loss_triggered: boolean | null;
  entry_order_id: string | null;
  entry_cancel_after?: Date | null;
  stop_order_id: string | null;
  exit_order_id: string | null;
  entry_order_status: string | null;
  entry_order_status_updated_at: string | Date | null;
  stop_order_status: string | null;
  stop_order_status_updated_at: string | Date | null;
  exit_order_status: string | null;
  exit_order_status_updated_at: string | Date | null;
  changes: unknown;
  created_at: Date;
  user_id?: number | null;
};

type TradeWithStrategyRow = TradeRow & {
  account_id?: string | null;
  strategy_name?: string | null;
};

type CountRow = QueryResultRow & { total: number };
type StatusCountRow = QueryResultRow & { status: string | null; count: number };
type EntryFillGapBucketRow = QueryResultRow & {
  bucket: number | null;
  count: number;
  total_pnl: number | null;
  sum_pnl_percent: number | null;
  pnl_percent_count: number | null;
};

export class TradesRepo {
  constructor(private readonly db: DbClient) {}

  private areNumericValuesEqual(a: number | null | undefined, b: number | null | undefined): boolean {
    if (a === null || a === undefined) {
      return b === null || b === undefined;
    }
    if (b === null || b === undefined) {
      return false;
    }
    return Math.abs(a - b) <= 1e-6;
  }

  private parseTradeChanges(raw: unknown): TradeChange[] {
    if (raw === null || raw === undefined) {
      return [];
    }

    let jsonText: string;
    if (typeof raw === 'string') {
      jsonText = raw;
    } else if (raw instanceof Buffer) {
      jsonText = raw.toString('utf8');
    } else {
      jsonText = String(raw);
    }

    try {
      const parsed = JSON.parse(jsonText);
      if (!Array.isArray(parsed)) {
        return [];
      }

      return parsed
        .map((item: unknown): TradeChange | null => {
          const record = item as Record<string, unknown> | null;
          if (!record) {
            return null;
          }

          const changedAtValue = record.changedAt ?? record.changed_at ?? record.date;
          const changedAt = changedAtValue ? new Date(String(changedAtValue)) : null;
          if (!changedAt || Number.isNaN(changedAt.getTime())) {
            return null;
          }

          const oldValue: unknown = record.oldValue ?? record.old_value ?? null;
          const newValue: unknown = record.newValue ?? record.new_value ?? null;

          return {
            field: typeof record.field === 'string' ? record.field : 'unknown',
            oldValue,
            newValue,
            changedAt
          } satisfies TradeChange;
        })
        .filter((item): item is TradeChange => item !== null);
    } catch (error) {
      console.warn('Failed to parse trade changes JSON', error);
      return [];
    }
  }

  private mapTradeRow(row: TradeRow): Trade {
    const status =
      row.status === 'pending' || row.status === 'active' || row.status === 'closed' || row.status === 'cancelled'
        ? row.status
        : 'pending';

    const entryOrderStatus =
      typeof row.entry_order_status === 'string' && row.entry_order_status.trim().length > 0
        ? row.entry_order_status.trim().toLowerCase()
        : undefined;
    const stopOrderStatus =
      typeof row.stop_order_status === 'string' && row.stop_order_status.trim().length > 0
        ? row.stop_order_status.trim().toLowerCase()
        : undefined;
    const exitOrderStatus =
      typeof row.exit_order_status === 'string' && row.exit_order_status.trim().length > 0
        ? row.exit_order_status.trim().toLowerCase()
        : undefined;

    const entryOrderStatusUpdatedAt =
      row.entry_order_status_updated_at ? new Date(row.entry_order_status_updated_at) : undefined;
    const stopOrderStatusUpdatedAt =
      row.stop_order_status_updated_at ? new Date(row.stop_order_status_updated_at) : undefined;
    const exitOrderStatusUpdatedAt =
      row.exit_order_status_updated_at ? new Date(row.exit_order_status_updated_at) : undefined;

    const cancellationSourceRaw =
      typeof row.cancellation_source === 'string' ? row.cancellation_source.trim().toLowerCase() : '';
    const cancellationSource: TradeCancellationSource | undefined =
      cancellationSourceRaw === 'expiry' || cancellationSourceRaw === 'exchange'
        ? (cancellationSourceRaw as TradeCancellationSource)
        : undefined;

    return {
      id: row.id,
      strategyId: row.strategy_id,
      userId: toNullableInteger(row.user_id),
      backtestResultId: row.backtest_result_id ?? undefined,
      ticker: row.ticker,
      quantity: row.quantity,
      price: row.price,
      date: new Date(row.date),
      status,
      cancellationSource,
      pnl: row.pnl ?? undefined,
      fee: row.fee ?? null,
      exitPrice: row.exit_price ?? undefined,
      exitDate: row.exit_date ? new Date(row.exit_date) : undefined,
      stopLoss: row.stop_loss ?? undefined,
      stopLossTriggered: row.stop_loss_triggered ?? undefined,
      entryOrderId: trimToNull(row.entry_order_id),
      entryCancelAfter: row.entry_cancel_after ? new Date(row.entry_cancel_after) : undefined,
      stopOrderId: trimToNull(row.stop_order_id),
      exitOrderId: trimToNull(row.exit_order_id),
      entryOrderStatus,
      entryOrderStatusUpdatedAt,
      stopOrderStatus,
      stopOrderStatusUpdatedAt,
      exitOrderStatus,
      exitOrderStatusUpdatedAt,
      createdAt: new Date(row.created_at),
      changes: this.parseTradeChanges(row.changes)
    };
  }

  private getTradeOrderIdColumnForOperation(operationType: AccountOperationType | undefined): string | null {
    switch (operationType) {
      case 'open_position':
        return 'entry_order_id';
      case 'update_stop_loss':
        return 'stop_order_id';
      case 'close_position':
        return 'exit_order_id';
      default:
        return null;
    }
  }

  async updateTradeOrderIdForOperation(
    operation: Pick<AccountOperation, 'tradeId' | 'operationType'>,
    orderId?: string | null
  ): Promise<void> {
    const normalizedOrderId = trimToNull(orderId);
    if (!normalizedOrderId) {
      return;
    }
    const tradeColumn = this.getTradeOrderIdColumnForOperation(operation.operationType);
    const tradeId = typeof operation.tradeId === 'string' ? operation.tradeId.trim() : '';
    if (tradeColumn && tradeId.length > 0) {
      await this.db.run(
        `UPDATE trades
            SET ${tradeColumn} = ?
          WHERE id = ?`,
        [normalizedOrderId, tradeId]
      );
    }
  }

  async updateTradeEntryCancelAfter(
    tradeId: string | null | undefined,
    cancelAfter: Date | null | undefined
  ): Promise<void> {
    const normalizedTradeId = typeof tradeId === 'string' ? tradeId.trim() : '';
    if (!normalizedTradeId) {
      return;
    }
    const timestamp = cancelAfter instanceof Date && !Number.isNaN(cancelAfter.getTime()) ? cancelAfter : null;
    await this.db.run(
      `UPDATE trades
          SET entry_cancel_after = ?
        WHERE id = ?`,
      [timestamp ? timestamp.toISOString() : null, normalizedTradeId]
    );
  }

  async updateTradeStopOrderId(
    tradeId: string | null | undefined,
    stopOrderId: string | null | undefined
  ): Promise<void> {
    const normalizedTradeId = typeof tradeId === 'string' ? tradeId.trim() : '';
    if (!normalizedTradeId) {
      return;
    }
    const normalizedOrderId = trimToNull(stopOrderId);
    if (!normalizedOrderId) {
      return;
    }
    await this.db.run(
      `UPDATE trades
          SET stop_order_id = ?
        WHERE id = ?`,
      [normalizedOrderId, normalizedTradeId]
    );
  }

  async ensureLiveTradeForOperation(operation: AccountOperation, userId: number | null): Promise<void> {
    const tradeId = typeof operation.tradeId === 'string' ? operation.tradeId.trim() : '';
    if (!tradeId) {
      return;
    }
    const ticker = normalizeUppercaseString(operation.ticker);
    if (!ticker) {
      return;
    }
    const quantityValue =
      typeof operation.quantity === 'number' && Number.isFinite(operation.quantity) ? Math.trunc(operation.quantity) : null;
    const priceValue = typeof operation.price === 'number' && Number.isFinite(operation.price) ? operation.price : 0;
    if (quantityValue === null || quantityValue === 0) {
      return;
    }
    const createdAt =
      operation.createdAt instanceof Date && !Number.isNaN(operation.createdAt.getTime())
        ? operation.createdAt
        : null;
    const tradeDate = createdAt ?? new Date();
    await this.db.run(
      `INSERT INTO trades (id, strategy_id, user_id, ticker, quantity, price, date, status, stop_loss)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
         ON CONFLICT (id) DO UPDATE
           SET date = GREATEST(trades.date, EXCLUDED.date)`,
      [
        tradeId,
        operation.strategyId,
        userId ?? null,
        ticker,
        quantityValue,
        priceValue,
        tradeDate,
        operation.stopLoss ?? null
      ]
    );
  }

  async updateTradeStopLossFromOperation(
    operation: Pick<AccountOperation, 'tradeId' | 'stopLoss' | 'previousStopLoss'>
  ): Promise<void> {
    const tradeId = typeof operation.tradeId === 'string' ? operation.tradeId.trim() : '';
    if (!tradeId) {
      return;
    }
    const nextStop = typeof operation.stopLoss === 'number' && Number.isFinite(operation.stopLoss) ? operation.stopLoss : null;
    const row = await this.db.get<QueryResultRow & { stop_loss: unknown; changes: unknown }>(
      `SELECT stop_loss, changes
         FROM trades
        WHERE id = ?`,
      [tradeId]
    );
    if (!row) {
      return;
    }
    const currentStop = toNullableNumber(row.stop_loss);
    const previousStop =
      typeof operation.previousStopLoss === 'number' && Number.isFinite(operation.previousStopLoss)
        ? operation.previousStopLoss
        : currentStop;

    const hasChange = !this.areNumericValuesEqual(currentStop, nextStop);
    if (!hasChange) {
      return;
    }

    const changes = this.parseTradeChanges(row.changes);
    changes.push({
      field: 'stopLoss',
      oldValue: previousStop ?? currentStop ?? null,
      newValue: nextStop,
      changedAt: new Date()
    });
    await this.db.run(
      `UPDATE trades
          SET stop_loss = ?,
              changes = ?
        WHERE id = ?`,
      [nextStop, JSON.stringify(changes), tradeId]
    );
  }

  async getTrades(
    strategyId: string | undefined,
    daysBack: number | undefined,
    ticker: string | undefined,
    limit: number | undefined,
    status: string | undefined,
    backtestResultId: string | undefined,
    userId: number
  ): Promise<Trade[]> {
    let sql = `
      SELECT t.id, t.strategy_id, t.backtest_result_id, t.ticker, t.quantity, t.price, t.date, t.status,
             t.cancellation_source,
             t.pnl, t.fee, t.exit_price, t.exit_date, t.stop_loss, t.stop_loss_triggered,
             t.entry_order_id, t.entry_cancel_after, t.stop_order_id, t.exit_order_id, t.entry_order_status, t.entry_order_status_updated_at, t.stop_order_status, t.stop_order_status_updated_at, t.exit_order_status, t.exit_order_status_updated_at, t.changes, t.created_at,
             COALESCE(t.user_id, s.user_id) as user_id
      FROM trades t
      LEFT JOIN strategies s ON s.id = t.strategy_id
    `;
    const params: QueryValue[] = [];
    const conditions: string[] = [];

    if (strategyId) {
      conditions.push('t.strategy_id = ?');
      params.push(strategyId);
    }

    if (ticker) {
      conditions.push('t.ticker = ?');
      params.push(ticker);
    }

    if (daysBack) {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - daysBack);
      conditions.push('t.date >= ?');
      params.push(cutoffDate.toISOString());
    }

    if (status) {
      conditions.push('t.status = ?');
      params.push(status);
    }

    if (backtestResultId) {
      conditions.push('t.backtest_result_id = ?');
      params.push(backtestResultId);
    }

    conditions.push('(COALESCE(t.user_id, s.user_id) = ? OR COALESCE(t.user_id, s.user_id) IS NULL)');
    params.push(userId);

    if (conditions.length > 0) {
      sql += ' WHERE ' + conditions.join(' AND ');
    }

    sql += ' ORDER BY t.date DESC, UPPER(t.ticker) ASC, t.id ASC';

    if (limit) {
      sql += ' LIMIT ?';
      params.push(limit);
    }

    const rows = await this.db.all<TradeRow>(sql, params);
    return rows.map((row) => this.mapTradeRow(row));
  }

  private buildBacktestTradeFilters(
    backtestResultId: string,
    userId: number,
    options: { status?: TradeStatus; ticker?: string; startDate?: Date } = {}
  ) {
    const conditions: string[] = ['t.backtest_result_id = ?'];
    const params: QueryValue[] = [backtestResultId];

    if (options.status) {
      conditions.push('t.status = ?');
      params.push(options.status);
    }

    if (options.ticker) {
      conditions.push('UPPER(t.ticker) LIKE ?');
      params.push(`%${options.ticker.toUpperCase()}%`);
    }

    if (options.startDate) {
      conditions.push('COALESCE(t.exit_date, t.date) >= ?');
      params.push(options.startDate.toISOString());
    }

    conditions.push('(COALESCE(t.user_id, s.user_id) = ? OR COALESCE(t.user_id, s.user_id) IS NULL)');
    params.push(userId);

    return { conditions, params };
  }

  private buildLiveTradeFilters(
    strategyId: string,
    userId: number,
    options: { status?: TradeStatus; ticker?: string } = {}
  ) {
    const conditions: string[] = [
      't.strategy_id = ?',
      't.entry_order_id IS NOT NULL',
      's.account_id IS NOT NULL',
      '(COALESCE(t.user_id, s.user_id) = ? OR COALESCE(t.user_id, s.user_id) IS NULL)'
    ];
    const params: QueryValue[] = [strategyId, userId];
    if (options.status) {
      conditions.push('t.status = ?');
      params.push(options.status);
    }
    if (options.ticker) {
      const normalizedTicker = normalizeUppercaseString(options.ticker);
      if (normalizedTicker.length > 0) {
        conditions.push('UPPER(t.ticker) LIKE ?');
        params.push(`%${normalizedTicker}%`);
      }
    }
    return { conditions, params };
  }

  async countLiveTradesForStrategy(
    strategyId: string,
    userId: number,
    options?: { status?: TradeStatus; ticker?: string }
  ): Promise<number> {
    const { conditions, params } = this.buildLiveTradeFilters(strategyId, userId, options ?? {});
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const row = await this.db.get<CountRow>(
      `
      SELECT COUNT(*) AS total
      FROM trades t
      LEFT JOIN strategies s ON s.id = t.strategy_id
      ${whereClause}
    `,
      params
    );
    const totalValue = row?.total ?? 0;
    return typeof totalValue === 'number' ? totalValue : Number(totalValue) || 0;
  }

  async getLiveTradeStatusCounts(strategyId: string, userId: number): Promise<Record<string, number>> {
    const { conditions, params } = this.buildLiveTradeFilters(strategyId, userId);
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const rows = await this.db.all<StatusCountRow>(
      `
      SELECT t.status as status, COUNT(*) as count
      FROM trades t
      LEFT JOIN strategies s ON s.id = t.strategy_id
      ${whereClause}
      GROUP BY t.status
    `,
      params
    );
    const counts: Record<string, number> = {};
    let total = 0;
    for (const row of rows) {
      const statusKey = typeof row.status === 'string' && row.status.length > 0 ? row.status : 'unknown';
      const value = typeof row.count === 'number' ? row.count : Number(row.count) || 0;
      counts[statusKey] = value;
      total += value;
    }
    counts.total = total;
    return counts;
  }

  async getLiveTradesForStrategy(
    strategyId: string,
    userId: number,
    options: {
      status?: TradeStatus;
      ticker?: string;
      limit: number;
      offset: number;
      sortBy?: 'date' | 'createdAt' | 'pnl' | 'ticker';
      sortDirection?: 'asc' | 'desc';
    }
  ): Promise<Trade[]> {
    const limit = Math.max(1, Math.min(options.limit, 1000));
    const offset = Math.max(0, options.offset);
    const sortColumnMap: Record<string, string> = {
      date: 't.date',
      createdAt: 't.created_at',
      pnl: 't.pnl',
      ticker: 't.ticker'
    };
    const sortColumn = sortColumnMap[options.sortBy ?? 'date'] ?? 't.date';
    const sortDirection = options.sortDirection === 'asc' ? 'ASC' : 'DESC';
    const { conditions, params } = this.buildLiveTradeFilters(strategyId, userId, {
      status: options.status,
      ticker: options.ticker
    });
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const rows = await this.db.all<TradeRow>(
      `
      SELECT t.id, t.strategy_id, t.backtest_result_id, t.ticker, t.quantity, t.price, t.date, t.status,
             t.cancellation_source,
             t.pnl, t.fee, t.exit_price, t.exit_date, t.stop_loss, t.stop_loss_triggered,
             t.entry_order_id, t.entry_cancel_after, t.stop_order_id, t.exit_order_id, t.entry_order_status, t.entry_order_status_updated_at, t.stop_order_status, t.stop_order_status_updated_at, t.exit_order_status, t.exit_order_status_updated_at, t.changes, t.created_at,
             COALESCE(t.user_id, s.user_id) as user_id
      FROM trades t
      LEFT JOIN strategies s ON s.id = t.strategy_id
      ${whereClause}
      ORDER BY ${sortColumn} ${sortDirection}, UPPER(t.ticker) ASC, t.id ASC
      LIMIT ?
      OFFSET ?
    `,
      [...params, limit, offset]
    );
    return rows.map((row) => this.mapTradeRow(row));
  }

  async getLiveTradesForAccounts(
    accountIds: string[],
    userId: number
  ): Promise<
    Array<{
      accountId: string;
      strategyId: string;
      strategyName: string | null;
      trade: Trade;
    }>
  > {
    if (!Array.isArray(accountIds) || accountIds.length === 0) {
      return [];
    }
    const placeholders = accountIds.map(() => '?').join(', ');
    const rows = await this.db.all<TradeWithStrategyRow>(
      `
      SELECT t.id, t.strategy_id, t.backtest_result_id, t.ticker, t.quantity, t.price, t.date, t.status,
             t.cancellation_source,
             t.pnl, t.fee, t.exit_price, t.exit_date, t.stop_loss, t.stop_loss_triggered,
             t.entry_order_id, t.entry_cancel_after, t.stop_order_id, t.exit_order_id, t.entry_order_status, t.entry_order_status_updated_at, t.stop_order_status, t.stop_order_status_updated_at, t.exit_order_status, t.exit_order_status_updated_at, t.changes, t.created_at,
             COALESCE(t.user_id, s.user_id) as user_id,
             s.account_id,
             s.name as strategy_name
      FROM trades t
      INNER JOIN strategies s ON s.id = t.strategy_id
      WHERE s.account_id IN (${placeholders})
        AND t.entry_order_id IS NOT NULL
        AND t.status IN ('pending', 'active')
        AND (COALESCE(t.user_id, s.user_id) = ? OR COALESCE(t.user_id, s.user_id) IS NULL)
      ORDER BY t.date DESC, UPPER(t.ticker) ASC, t.created_at DESC, t.id DESC
    `,
      [...accountIds, userId]
    );
    const results: Array<{
      accountId: string;
      strategyId: string;
      strategyName: string | null;
      trade: Trade;
    }> = [];
    for (const row of rows) {
      const accountId = typeof row.account_id === 'string' ? row.account_id : null;
      if (!accountId) {
        continue;
      }
      results.push({
        accountId,
        strategyId: row.strategy_id,
        strategyName: typeof row.strategy_name === 'string' ? row.strategy_name : null,
        trade: this.mapTradeRow(row)
      });
    }
    return results;
  }

  async countBacktestTrades(
    backtestResultId: string,
    userId: number,
    options: { status?: TradeStatus; ticker?: string; startDate?: Date } = {}
  ): Promise<number> {
    const { conditions, params } = this.buildBacktestTradeFilters(backtestResultId, userId, options);
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const row = await this.db.get<CountRow>(
      `
      SELECT COUNT(*) AS total
      FROM trades t
      LEFT JOIN strategies s ON s.id = t.strategy_id
      ${whereClause}
    `,
      params
    );
    const totalValue = row?.total ?? 0;
    return typeof totalValue === 'number' ? totalValue : Number(totalValue) || 0;
  }

  async getBacktestTrades(
    backtestResultId: string,
    userId: number,
    options: {
      status?: TradeStatus;
      ticker?: string;
      limit: number;
      offset: number;
      sortBy?: 'date' | 'createdAt' | 'pnl' | 'ticker';
      sortDirection?: 'asc' | 'desc';
    }
  ): Promise<Trade[]> {
    const limit = Math.max(1, Math.min(options.limit, 1000));
    const offset = Math.max(0, options.offset);
    const sortColumnMap: Record<string, string> = {
      date: 't.date',
      createdAt: 't.created_at',
      pnl: 't.pnl',
      ticker: 't.ticker'
    };
    const sortColumn = sortColumnMap[options.sortBy ?? 'date'] ?? 't.date';
    const sortDirection = options.sortDirection === 'asc' ? 'ASC' : 'DESC';
    const { conditions, params } = this.buildBacktestTradeFilters(backtestResultId, userId, {
      status: options.status,
      ticker: options.ticker
    });
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const rows = await this.db.all<TradeRow>(
      `
      SELECT t.id, t.strategy_id, t.backtest_result_id, t.ticker, t.quantity, t.price, t.date, t.status,
             t.cancellation_source,
             t.pnl, t.fee, t.exit_price, t.exit_date, t.stop_loss, t.stop_loss_triggered,
             t.entry_order_id, t.entry_cancel_after, t.stop_order_id, t.exit_order_id, t.entry_order_status, t.entry_order_status_updated_at, t.stop_order_status, t.stop_order_status_updated_at, t.exit_order_status, t.exit_order_status_updated_at, t.changes, t.created_at,
             COALESCE(t.user_id, s.user_id) as user_id
      FROM trades t
      LEFT JOIN strategies s ON s.id = t.strategy_id
      ${whereClause}
      ORDER BY ${sortColumn} ${sortDirection}, UPPER(t.ticker) ASC, t.id ASC
      LIMIT ?
      OFFSET ?
    `,
      [...params, limit, offset]
    );
    return rows.map((row) => this.mapTradeRow(row));
  }

  async getBacktestTradesForDate(
    backtestResultId: string,
    userId: number,
    targetDate: Date
  ): Promise<Trade[]> {
    const { conditions, params } = this.buildBacktestTradeFilters(backtestResultId, userId);
    const dateString = targetDate.toISOString().split('T')[0];

    conditions.push(
      `(
        DATE(t.date) = ?
        OR (t.exit_date IS NOT NULL AND DATE(t.exit_date) = ?)
        OR (DATE(t.date) <= ? AND (t.exit_date IS NULL OR DATE(t.exit_date) >= ?))
      )`
    );
    params.push(dateString, dateString, dateString, dateString);

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const rows = await this.db.all<TradeRow>(
      `
      SELECT t.id, t.strategy_id, t.backtest_result_id, t.ticker, t.quantity, t.price, t.date, t.status,
             t.cancellation_source,
             t.pnl, t.fee, t.exit_price, t.exit_date, t.stop_loss, t.stop_loss_triggered,
             t.entry_order_id, t.entry_cancel_after, t.stop_order_id, t.exit_order_id, t.entry_order_status, t.entry_order_status_updated_at, t.stop_order_status, t.stop_order_status_updated_at, t.exit_order_status, t.exit_order_status_updated_at, t.changes, t.created_at,
             COALESCE(t.user_id, s.user_id) as user_id
      FROM trades t
      LEFT JOIN strategies s ON s.id = t.strategy_id
      ${whereClause}
      ORDER BY t.date ASC, UPPER(t.ticker) ASC, t.id ASC
    `,
      params
    );
    return rows.map((row) => this.mapTradeRow(row));
  }

  async getBacktestTradeStatusCounts(backtestResultId: string, userId: number): Promise<Record<string, number>> {
    const { conditions, params } = this.buildBacktestTradeFilters(backtestResultId, userId);
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const rows = await this.db.all<StatusCountRow>(
      `
      SELECT t.status as status, COUNT(*) as count
      FROM trades t
      LEFT JOIN strategies s ON s.id = t.strategy_id
      ${whereClause}
      GROUP BY t.status
    `,
      params
    );

    const counts: Record<string, number> = {};
    let total = 0;
    for (const row of rows) {
      const statusKey = typeof row.status === 'string' && row.status.length > 0 ? row.status : 'unknown';
      const value = typeof row.count === 'number' ? row.count : Number(row.count) || 0;
      counts[statusKey] = value;
      total += value;
    }

    counts.total = total;
    return counts;
  }

  async getTrade(tradeId: string, userId: number): Promise<Trade | null> {
    const sql = `
      SELECT t.id, t.strategy_id, t.backtest_result_id, t.ticker, t.quantity, t.price, t.date, t.status,
             t.cancellation_source,
             t.pnl, t.fee, t.exit_price, t.exit_date, t.stop_loss, t.stop_loss_triggered,
             t.entry_order_id, t.entry_cancel_after, t.stop_order_id, t.exit_order_id, t.entry_order_status, t.entry_order_status_updated_at, t.stop_order_status, t.stop_order_status_updated_at, t.exit_order_status, t.exit_order_status_updated_at, t.changes, t.created_at, t.user_id as user_id
      FROM trades t
      WHERE t.id = ? AND (t.user_id = ? OR t.user_id IS NULL)
    `;
    const row = await this.db.get<TradeRow>(sql, [tradeId, userId]);

    if (!row) {
      return null;
    }

    return this.mapTradeRow(row);
  }

  private async getTradesByPnlPercentOrder(
    strategyId: string,
    userId: number,
    limit: number = 10,
    backtestResultId: string | undefined,
    order: 'ASC' | 'DESC'
  ): Promise<Trade[]> {
    const effectiveLimit = Math.max(1, Math.floor(limit ?? 10));
    const sortDirection = order === 'ASC' ? 'ASC' : 'DESC';
    const filters: string[] = [
      't.strategy_id = ?',
      '(t.user_id = ? OR t.user_id IS NULL)',
      `t.status IN ('active', 'closed')`,
      't.pnl IS NOT NULL',
      '(t.quantity * t.price) != 0'
    ];
    const params: QueryValue[] = [strategyId, userId];

    if (backtestResultId) {
      filters.push('t.backtest_result_id = ?');
      params.push(backtestResultId);
    }

    const sql = `
      SELECT
        t.*,
        (t.pnl / ABS(t.quantity * t.price)) AS pnl_percent
      FROM trades t
      WHERE ${filters.join(' AND ')}
      ORDER BY pnl_percent ${sortDirection}, UPPER(t.ticker) ASC, t.id ASC
      LIMIT ?
    `;

    const rows = await this.db.all<TradeRow>(sql, [...params, effectiveLimit]);
    return rows.map((row) => this.mapTradeRow(row));
  }

  async getBestTradesByPnlPercent(
    strategyId: string,
    userId: number,
    limit: number = 10,
    backtestResultId?: string
  ): Promise<Trade[]> {
    return this.getTradesByPnlPercentOrder(strategyId, userId, limit, backtestResultId, 'DESC');
  }

  async getWorstTradesByPnlPercent(
    strategyId: string,
    userId: number,
    limit: number = 10,
    backtestResultId?: string
  ): Promise<Trade[]> {
    return this.getTradesByPnlPercentOrder(strategyId, userId, limit, backtestResultId, 'ASC');
  }

  async getTickerTradeStatsForBacktest(
    backtestResultId: string,
    userId: number,
    options: { startDate?: Date } = {}
  ): Promise<TradeTickerStats[]> {
    const { conditions, params } = this.buildBacktestTradeFilters(backtestResultId, userId, {
      startDate: options.startDate
    });
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const rows = await this.db.all<QueryResultRow>(
      `
        WITH filtered_trades AS (
          SELECT
            UPPER(t.ticker) AS ticker,
            COALESCE(t.pnl, 0) AS pnl,
            ABS(COALESCE(t.quantity, 0) * COALESCE(t.price, 0)) AS trade_value,
            t.date AS entry_date,
            t.exit_date AS exit_date
          FROM trades t
          LEFT JOIN strategies s ON s.id = t.strategy_id
          ${whereClause}
        ),
        enriched_trades AS (
          SELECT
            ticker,
            pnl,
            trade_value,
            CASE WHEN trade_value > 0 THEN (pnl / trade_value) * 100 ELSE NULL END AS pnl_percent,
            CASE WHEN trade_value > 0 AND pnl > 0 THEN (pnl / trade_value) * 100 ELSE NULL END AS win_pnl_percent,
            CASE WHEN trade_value > 0 AND pnl < 0 THEN ABS((pnl / trade_value) * 100) ELSE NULL END AS loss_pnl_percent,
            CASE WHEN pnl > 0 THEN 1 ELSE 0 END AS is_win,
            CASE WHEN pnl < 0 THEN 1 ELSE 0 END AS is_loss,
            CASE WHEN pnl = 0 THEN 1 ELSE 0 END AS is_neutral,
            GREATEST(
              0,
              COALESCE(EXTRACT(EPOCH FROM COALESCE(exit_date, CURRENT_TIMESTAMP) - entry_date) / 86400.0, 0)
            ) AS duration_days,
            CASE
              WHEN pnl > 0 THEN GREATEST(
                0,
                COALESCE(EXTRACT(EPOCH FROM COALESCE(exit_date, CURRENT_TIMESTAMP) - entry_date) / 86400.0, 0)
              )
              ELSE NULL
            END AS win_duration_days,
            CASE
              WHEN pnl < 0 THEN GREATEST(
                0,
                COALESCE(EXTRACT(EPOCH FROM COALESCE(exit_date, CURRENT_TIMESTAMP) - entry_date) / 86400.0, 0)
              )
              ELSE NULL
            END AS loss_duration_days
          FROM filtered_trades
        )
        SELECT
          ticker,
          COUNT(*) as total_trades,
          SUM(is_win) as profitable_trades,
          SUM(is_loss) as unprofitable_trades,
          SUM(is_neutral) as neutral_trades,
          SUM(pnl) as net_pnl,
          SUM(ABS(pnl)) as abs_net_pnl,
          SUM(trade_value) as total_buy_cost,
          SUM(CASE WHEN pnl_percent IS NOT NULL THEN pnl_percent ELSE 0 END) as sum_pnl_percent,
          SUM(CASE WHEN pnl_percent IS NOT NULL THEN 1 ELSE 0 END) as pnl_percent_count,
          SUM(CASE WHEN win_pnl_percent IS NOT NULL THEN win_pnl_percent ELSE 0 END) as win_sum_percent,
          SUM(CASE WHEN win_pnl_percent IS NOT NULL THEN 1 ELSE 0 END) as win_count,
          SUM(CASE WHEN loss_pnl_percent IS NOT NULL THEN loss_pnl_percent ELSE 0 END) as loss_sum_percent,
          SUM(CASE WHEN loss_pnl_percent IS NOT NULL THEN 1 ELSE 0 END) as loss_count,
          SUM(CASE WHEN win_duration_days IS NOT NULL THEN win_duration_days ELSE 0 END) as win_duration_sum,
          SUM(CASE WHEN win_duration_days IS NOT NULL THEN 1 ELSE 0 END) as win_duration_count,
          SUM(CASE WHEN loss_duration_days IS NOT NULL THEN loss_duration_days ELSE 0 END) as loss_duration_sum,
          SUM(CASE WHEN loss_duration_days IS NOT NULL THEN 1 ELSE 0 END) as loss_duration_count
        FROM enriched_trades
        GROUP BY ticker
        ORDER BY net_pnl DESC
      `,
      params
    );

    return rows.map((row) => ({
      ticker: typeof row.ticker === 'string' ? row.ticker : 'UNKNOWN',
      totalTrades: Number(row.total_trades) || 0,
      profitableTrades: Number(row.profitable_trades) || 0,
      unprofitableTrades: Number(row.unprofitable_trades) || 0,
      neutralTrades: Number(row.neutral_trades) || 0,
      netPnl: Number(row.net_pnl) || 0,
      absNetPnl: Number(row.abs_net_pnl) || 0,
      totalBuyCost: Number(row.total_buy_cost) || 0,
      sumPnlPercent: Number(row.sum_pnl_percent) || 0,
      pnlPercentCount: Number(row.pnl_percent_count) || 0,
      winSumPercent: Number(row.win_sum_percent) || 0,
      winCount: Number(row.win_count) || 0,
      lossSumPercent: Number(row.loss_sum_percent) || 0,
      lossCount: Number(row.loss_count) || 0,
      winDurationSum: Number(row.win_duration_sum) || 0,
      winDurationCount: Number(row.win_duration_count) || 0,
      lossDurationSum: Number(row.loss_duration_sum) || 0,
      lossDurationCount: Number(row.loss_duration_count) || 0
    }));
  }

  async getTradeVolumeSegmentStatsForBacktest(
    backtestResultId: string,
    userId: number
  ): Promise<TradeVolumeSegmentStats[]> {
    const { conditions, params } = this.buildBacktestTradeFilters(backtestResultId, userId);
    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const rows = await this.db.all<QueryResultRow>(
      `
        WITH filtered_trades AS (
          SELECT
            UPPER(t.ticker) AS ticker,
            t.date::date AS trade_date,
            COALESCE(t.pnl, 0) AS pnl,
            ABS(COALESCE(t.quantity, 0) * COALESCE(t.price, 0)) AS trade_value
          FROM trades t
          LEFT JOIN strategies s ON s.id = t.strategy_id
          ${whereClause}
        ),
        trade_with_volume AS (
          SELECT
            ft.*,
            (c.close * c.volume_shares) AS volume_usd
          FROM filtered_trades ft
          LEFT JOIN candles c
            ON c.ticker = ft.ticker AND c.date = ft.trade_date
        )
        SELECT
          CASE
            WHEN volume_usd < 1000000 THEN '<1M'
            WHEN volume_usd >= 1000000 AND volume_usd < 10000000 THEN '1-10M'
            WHEN volume_usd >= 10000000 AND volume_usd < 100000000 THEN '10-100M'
            WHEN volume_usd >= 100000000 AND volume_usd < 1000000000 THEN '100-1000M'
            WHEN volume_usd >= 1000000000 AND volume_usd < 10000000000 THEN '1B-10B'
            ELSE '10B+'
          END AS bucket_label,
          CASE
            WHEN volume_usd < 1000000 THEN 1
            WHEN volume_usd >= 1000000 AND volume_usd < 10000000 THEN 2
            WHEN volume_usd >= 10000000 AND volume_usd < 100000000 THEN 3
            WHEN volume_usd >= 100000000 AND volume_usd < 1000000000 THEN 4
            WHEN volume_usd >= 1000000000 AND volume_usd < 10000000000 THEN 5
            ELSE 6
          END AS bucket_order,
          CASE
            WHEN volume_usd < 1000000 THEN 0
            WHEN volume_usd >= 1000000 AND volume_usd < 10000000 THEN 1000000
            WHEN volume_usd >= 10000000 AND volume_usd < 100000000 THEN 10000000
            WHEN volume_usd >= 100000000 AND volume_usd < 1000000000 THEN 100000000
            WHEN volume_usd >= 1000000000 AND volume_usd < 10000000000 THEN 1000000000
            ELSE 10000000000
          END AS min_volume_usd,
          CASE
            WHEN volume_usd < 1000000 THEN 1000000
            WHEN volume_usd >= 1000000 AND volume_usd < 10000000 THEN 10000000
            WHEN volume_usd >= 10000000 AND volume_usd < 100000000 THEN 100000000
            WHEN volume_usd >= 100000000 AND volume_usd < 1000000000 THEN 1000000000
            WHEN volume_usd >= 1000000000 AND volume_usd < 10000000000 THEN 10000000000
            ELSE NULL
          END AS max_volume_usd,
          COUNT(*) AS trade_count,
          SUM(pnl) AS total_pnl,
          SUM(trade_value) AS total_buy_cost
        FROM trade_with_volume
        WHERE volume_usd IS NOT NULL
          AND volume_usd > 0
          AND trade_value > 0
        GROUP BY bucket_label, bucket_order, min_volume_usd, max_volume_usd
        ORDER BY bucket_order ASC
      `,
      params
    );

    return rows
      .map((row) => ({
        bucketLabel: typeof row.bucket_label === 'string' ? row.bucket_label : 'Unknown',
        bucketOrder: Number(row.bucket_order) || 0,
        tradeCount: Number(row.trade_count) || 0,
        totalPnl: Number(row.total_pnl) || 0,
        totalBuyCost: Number(row.total_buy_cost) || 0,
        minVolumeUsd: Number(row.min_volume_usd) || 0,
        maxVolumeUsd: row.max_volume_usd === null ? null : Number(row.max_volume_usd) || 0
      }))
      .filter((row) => row.tradeCount > 0 && Number.isFinite(row.bucketOrder));
  }

  async getEntryFillGapHistogramForBacktest(
    backtestResultId: string,
    userId: number,
    options: { minPercent?: number; maxPercent?: number; bucketCount?: number } = {}
  ): Promise<EntryFillGapHistogram> {
    const requestedMin = Number(options.minPercent);
    const requestedMax = Number(options.maxPercent);
    const requestedBuckets = Number(options.bucketCount);
    const minPercent = Number.isFinite(requestedMin) ? requestedMin : -10;
    const maxPercent = Number.isFinite(requestedMax) ? requestedMax : 10;
    const bucketCount = Number.isFinite(requestedBuckets) ? Math.max(1, Math.floor(requestedBuckets)) : 20;
    const lowerBound = Math.min(minPercent, maxPercent);
    const upperBound = Math.max(minPercent, maxPercent);

    const rows = await this.db.all<EntryFillGapBucketRow>(
      `
        WITH filtered_trades AS (
          SELECT t.ticker, t.price, t.date, t.quantity, t.pnl
          FROM trades t
          LEFT JOIN strategies s ON s.id = t.strategy_id
          WHERE t.backtest_result_id = ?
            AND t.quantity != 0
            AND (COALESCE(t.user_id, s.user_id) = ? OR COALESCE(t.user_id, s.user_id) IS NULL)
        ),
        trade_with_close AS (
          SELECT
            ft.ticker,
            ft.price,
            ft.date,
            ft.quantity,
            ft.pnl,
            (
              SELECT c.close
              FROM candles c
              WHERE c.ticker = ft.ticker
                AND c.date < ft.date
              ORDER BY c.date DESC
              LIMIT 1
            ) AS last_close
          FROM filtered_trades ft
        ),
        gap_values AS (
          SELECT
            ((price - last_close) / NULLIF(last_close, 0)) * 100 AS gap_percent,
            pnl,
            ABS(quantity * price) AS trade_value
          FROM trade_with_close
          WHERE last_close IS NOT NULL
        )
        SELECT width_bucket(gap_percent, ?, ?, ?) AS bucket,
               COUNT(*) AS count,
               SUM(COALESCE(pnl, 0)) AS total_pnl,
               SUM(CASE WHEN trade_value > 0 AND pnl IS NOT NULL THEN (pnl / trade_value) * 100 ELSE 0 END)
                 AS sum_pnl_percent,
               SUM(CASE WHEN trade_value > 0 AND pnl IS NOT NULL THEN 1 ELSE 0 END)
                 AS pnl_percent_count
        FROM gap_values
        GROUP BY bucket
        ORDER BY bucket ASC
      `,
      [backtestResultId, userId, lowerBound, upperBound, bucketCount]
    );

    const buckets = rows
      .map((row) => {
        if (row.bucket === null || row.bucket === undefined) {
          return null;
        }
        const bucketValue = Number(row.bucket);
        if (!Number.isFinite(bucketValue)) {
          return null;
        }
        return {
          bucket: bucketValue,
          count: Number(row.count) || 0,
          totalPnl: Number(row.total_pnl) || 0,
          sumPnlPercent: Number(row.sum_pnl_percent) || 0,
          pnlPercentCount: Number(row.pnl_percent_count) || 0
        };
      })
      .filter(
        (row): row is { bucket: number; count: number; totalPnl: number; sumPnlPercent: number; pnlPercentCount: number } =>
          row !== null
      );

    return {
      minPercent: lowerBound,
      maxPercent: upperBound,
      bucketCount,
      buckets
    };
  }
}
