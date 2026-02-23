import type { PoolClient, QueryResultRow } from 'pg';
import type { Strategy } from '../../../shared/types/StrategyTemplate';
import { DbClient } from '../core/DbClient';
import { toNullableInteger, toNullableNumber } from '../core/valueParsers';
import type { InsertStrategyInput } from '../types';

type StrategyRow = QueryResultRow & {
  id: string;
  name: string;
  user_id: number | null;
  account_id: string | null;
  template_id: string;
  parameters: string;
  status: string;
  created_at: Date;
  updated_at: Date;
  backtest_start_date: Date | null;
  last_backtest_duration_minutes: number | null;
};

type StrategyIdTemplateRow = QueryResultRow & {
  id: string;
  template_id: string;
};

export class StrategiesRepo {
  constructor(private readonly db: DbClient) {}

  private toStrategyStatus(value: unknown): Strategy['status'] {
    return value === 'inactive' || value === 'error' ? value : 'active';
  }

  private mapStrategyRow(row: StrategyRow): Strategy {
    return {
      id: row.id,
      name: row.name,
      userId: toNullableInteger(row.user_id),
      templateId: row.template_id,
      accountId: row.account_id ?? null,
      parameters: JSON.parse(row.parameters) as Strategy['parameters'],
      status: this.toStrategyStatus(row.status),
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      backtestStartDate: row.backtest_start_date ? new Date(row.backtest_start_date) : null,
      lastBacktestDurationMinutes: toNullableNumber(row.last_backtest_duration_minutes) ?? undefined,
      performance: undefined
    };
  }

  async getStrategiesByIdLike(pattern: string): Promise<Array<{ id: string; templateId: string }>> {
    const likePattern = pattern.includes('%') ? pattern : `${pattern}%`;
    const rows = await this.db.all<StrategyIdTemplateRow>('SELECT id, template_id FROM strategies WHERE id LIKE ?', [
      likePattern
    ]);
    return rows.map((row) => ({ id: row.id, templateId: row.template_id }));
  }

  async updateStrategyBacktestDuration(strategyId: string, durationMinutes: number): Promise<void> {
    await this.db.run(
      `UPDATE strategies
       SET last_backtest_duration_minutes = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [durationMinutes, strategyId]
    );
  }

  async updateStrategyStatus(strategyId: string, status: string): Promise<void> {
    await this.db.run(
      `UPDATE strategies
       SET status = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [status, strategyId]
    );
  }

  async updateStrategyName(strategyId: string, name: string): Promise<void> {
    await this.db.run(
      `UPDATE strategies
       SET name = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [name, strategyId]
    );
  }

  async getStrategies(userId: number): Promise<Strategy[]> {
    const rows = await this.db.all<StrategyRow>(
      `
        SELECT s.id, s.name, s.user_id, s.account_id, s.template_id, s.parameters, s.status, s.created_at, s.updated_at,
               s.backtest_start_date, s.last_backtest_duration_minutes
        FROM strategies s
        WHERE (s.user_id = ? OR s.user_id IS NULL)
        ORDER BY s.created_at DESC
      `,
      [userId]
    );

    return rows.map((row) => this.mapStrategyRow(row));
  }

  async getStrategiesByTemplate(templateId: string, userId: number): Promise<Strategy[]> {
    const rows = await this.db.all<StrategyRow>(
      `
        SELECT s.id, s.name, s.user_id, s.account_id, s.template_id, s.parameters, s.status, s.created_at, s.updated_at,
               s.backtest_start_date, s.last_backtest_duration_minutes
        FROM strategies s
        WHERE s.template_id = ?
          AND (s.user_id = ? OR s.user_id IS NULL)
        ORDER BY s.created_at DESC
      `,
      [templateId, userId]
    );

    return rows.map((row) => this.mapStrategyRow(row));
  }

  async getStrategy(id: string, userId: number): Promise<Strategy | null> {
    const row = await this.db.get<StrategyRow>(
      `
        SELECT s.id, s.name, s.user_id, s.account_id, s.template_id, s.parameters, s.status, s.created_at, s.updated_at,
               s.backtest_start_date, s.last_backtest_duration_minutes
        FROM strategies s
        WHERE s.id = ?
          AND (s.user_id = ? OR s.user_id IS NULL)
      `,
      [id, userId]
    );

    if (!row) {
      return null;
    }

    return this.mapStrategyRow(row);
  }

  async insertStrategy(strategy: InsertStrategyInput): Promise<void> {
    const status = strategy.status ?? 'active';
    const userId = strategy.userId ?? null;
    const createdAt = strategy.createdAt ?? new Date();
    const updatedAt = strategy.updatedAt ?? new Date();
    const backtestStartDate = strategy.backtestStartDate ?? null;
    const accountId = strategy.accountId ?? null;

    await this.db.run(
      `
        INSERT INTO strategies (id, name, user_id, account_id, template_id, parameters, backtest_start_date, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        strategy.id,
        strategy.name,
        userId,
        accountId,
        strategy.templateId,
        JSON.stringify(strategy.parameters),
        backtestStartDate ? backtestStartDate.toISOString() : null,
        status,
        createdAt,
        updatedAt
      ]
    );
  }

  async createStrategy(
    strategy: Omit<Strategy, 'id' | 'createdAt' | 'updatedAt'> & { backtestStartDate?: Date | null }
  ): Promise<string> {
    const id = `strategy_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    await this.insertStrategy({
      id,
      name: strategy.name,
      templateId: strategy.templateId,
      parameters: strategy.parameters,
      status: strategy.status,
      userId: strategy.userId ?? null,
      backtestStartDate: strategy.backtestStartDate ?? null,
      accountId: strategy.accountId ?? null
    });

    return id;
  }

  async deleteStrategyRelatedData(
    strategyIds: string[],
    client?: PoolClient
  ): Promise<{
    accountOperationsDeleted: number;
    tradesDeleted: number;
    backtestResultsDeleted: number;
  }> {
    const normalizedIds = Array.from(
      new Set(
        (strategyIds || [])
          .map((strategyId) => (typeof strategyId === 'string' ? strategyId.trim() : ''))
          .filter((strategyId) => strategyId.length > 0)
      )
    );

    if (normalizedIds.length === 0) {
      return {
        accountOperationsDeleted: 0,
        tradesDeleted: 0,
        backtestResultsDeleted: 0
      };
    }

    const placeholders = normalizedIds.map(() => '?').join(', ');
    const accountOperationsResult = await this.db.run(
      `DELETE FROM account_operations WHERE strategy_id IN (${placeholders})`,
      normalizedIds,
      client
    );
    const tradesResult = await this.db.run(`DELETE FROM trades WHERE strategy_id IN (${placeholders})`, normalizedIds, client);
    const backtestResultsResult = await this.db.run(
      `DELETE FROM backtest_results WHERE strategy_id IN (${placeholders})`,
      normalizedIds,
      client
    );
    await this.db.run(`DELETE FROM signals WHERE strategy_id IN (${placeholders})`, normalizedIds, client);

    return {
      accountOperationsDeleted: accountOperationsResult.changes || 0,
      tradesDeleted: tradesResult.changes || 0,
      backtestResultsDeleted: backtestResultsResult.changes || 0
    };
  }

  async clearStrategyBacktestResults(
    strategyId: string,
    preserveAccountTrades: boolean
  ): Promise<{
    accountOperationsDeleted: number;
    tradesDeleted: number;
    backtestResultsDeleted: number;
  }> {
    try {
      return await this.db.withTransaction(async (client) => {
        let result: { accountOperationsDeleted: number; tradesDeleted: number; backtestResultsDeleted: number };

        if (!preserveAccountTrades) {
          result = await this.deleteStrategyRelatedData([strategyId], client);
        } else {
          const nonAccountTradesResult = await this.db.run(
            `DELETE FROM trades t
              WHERE t.strategy_id = ?
                AND t.entry_order_id IS NULL`,
            [strategyId],
            client
          );
          await this.db.run(
            `UPDATE trades
               SET backtest_result_id = NULL
             WHERE strategy_id = ?
               AND entry_order_id IS NOT NULL
               AND backtest_result_id IS NOT NULL`,
            [strategyId],
            client
          );
          const backtestResultsResult = await this.db.run('DELETE FROM backtest_results WHERE strategy_id = ?', [strategyId], client);
          await this.db.run('DELETE FROM signals WHERE strategy_id = ?', [strategyId], client);

          result = {
            accountOperationsDeleted: 0,
            tradesDeleted: nonAccountTradesResult.changes || 0,
            backtestResultsDeleted: backtestResultsResult.changes || 0
          };
        }

        await this.db.run(
          'DELETE FROM account_signal_skips WHERE strategy_id = ? AND source = ?',
          [strategyId, 'backtest'],
          client
        );

        return result;
      });
    } catch (error) {
      console.error(`Failed to clear backtest results for strategy ${strategyId}:`, error);
      throw error;
    }
  }

  async deleteStrategy(strategyId: string): Promise<{
    strategyDeleted: boolean;
    accountOperationsDeleted: number;
    tradesDeleted: number;
    backtestResultsDeleted: number;
  }> {
    try {
      const cleanupResult = await this.clearStrategyBacktestResults(strategyId, false);

      const strategyResult = await this.db.run('DELETE FROM strategies WHERE id = ?', [strategyId]);
      const strategyDeleted = (strategyResult.changes || 0) > 0;

      return {
        strategyDeleted,
        accountOperationsDeleted: cleanupResult.accountOperationsDeleted,
        tradesDeleted: cleanupResult.tradesDeleted,
        backtestResultsDeleted: cleanupResult.backtestResultsDeleted
      };
    } catch (error) {
      console.error(`Failed to delete strategy ${strategyId}:`, error);
      throw error;
    }
  }
}
