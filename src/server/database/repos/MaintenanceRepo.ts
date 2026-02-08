import type { PoolClient, QueryResultRow } from 'pg';
import { DbClient } from '../core/DbClient';
import { toInteger } from '../core/valueParsers';
import { TickersRepo } from './TickersRepo';

type TableRowEstimateRow = QueryResultRow & {
  table_name: string;
  row_estimate: number;
};

export class MaintenanceRepo {
  constructor(
    private readonly db: DbClient,
    private readonly tickersRepo: TickersRepo
  ) {}

  async clearDatabase(): Promise<void> {
    try {
      await this.db.run('DELETE FROM trades');
      await this.db.run('DELETE FROM strategies');
      await this.db.run('DELETE FROM candles');
      await this.db.run('DELETE FROM tickers');
      await this.db.run('DELETE FROM backtest_results');
      await this.db.run('DELETE FROM templates');
      await this.db.run('DELETE FROM signals');
    } catch (error) {
      console.error('Failed to clear database:', error);
      throw error;
    }
  }

  async clearAllBacktestResults(): Promise<{ backtestTradesDeleted: number; backtestResultsDeleted: number }> {
    try {
      const tradesResult = await this.db.run(
        `DELETE FROM trades t
          WHERE t.entry_order_id IS NULL`
      );
      const backtestTradesDeleted = tradesResult.changes || 0;

      const backtestResultsResult = await this.db.run('DELETE FROM backtest_results');
      const backtestResultsDeleted = backtestResultsResult.changes || 0;

      return {
        backtestTradesDeleted,
        backtestResultsDeleted
      };
    } catch (error) {
      console.error('Failed to clear backtest results:', error);
      throw error;
    }
  }

  async clearAllTrades(): Promise<{ tradesDeleted: number; backtestResultsDeleted: number }> {
    try {
      const tradesResult = await this.db.run('DELETE FROM trades');

      const backtestResultsResult = await this.db.run('DELETE FROM backtest_results');
      const backtestResultsDeleted = backtestResultsResult.changes || 0;

      return {
        tradesDeleted: tradesResult.changes || 0,
        backtestResultsDeleted
      };
    } catch (error) {
      console.error('Failed to clear trades:', error);
      throw error;
    }
  }

  async clearAllStrategiesAndTrades(): Promise<{ strategiesDeleted: number; tradesDeleted: number; backtestResultsDeleted: number }> {
    try {
      const tradesResult = await this.db.run('DELETE FROM trades');
      const tradesDeleted = tradesResult.changes || 0;

      const backtestResultsResult = await this.db.run('DELETE FROM backtest_results');
      const backtestResultsDeleted = backtestResultsResult.changes || 0;

      const strategiesResult = await this.db.run('DELETE FROM strategies');
      const strategiesDeleted = strategiesResult.changes || 0;

      return {
        strategiesDeleted,
        tradesDeleted,
        backtestResultsDeleted
      };
    } catch (error) {
      console.error('Failed to clear all strategies and trades:', error);
      throw error;
    }
  }

  async clearAllTradingData(): Promise<{
    strategiesDeleted: number;
    tradesDeleted: number;
    templatesDeleted: number;
    backtestResultsDeleted: number;
  }> {
    try {
      const tradesResult = await this.db.run('DELETE FROM trades');
      const tradesDeleted = tradesResult.changes || 0;

      const backtestResultsResult = await this.db.run('DELETE FROM backtest_results');
      const backtestResultsDeleted = backtestResultsResult.changes || 0;

      const strategiesResult = await this.db.run('DELETE FROM strategies');
      const strategiesDeleted = strategiesResult.changes || 0;

      const templatesResult = await this.db.run('DELETE FROM templates');
      const templatesDeleted = templatesResult.changes || 0;

      await this.db.run('DELETE FROM signals');
      await this.db.run('DELETE FROM lightgbm_models');

      return {
        strategiesDeleted,
        tradesDeleted,
        templatesDeleted,
        backtestResultsDeleted
      };
    } catch (error) {
      console.error('Failed to clear all trading data:', error);
      throw error;
    }
  }

  async clearAllDataExceptUsers(): Promise<{
    truncatedTables: string[];
    rowEstimates: Record<string, number>;
    totalRowEstimate: number;
  }> {
    const tablesToTruncate = [
      'account_operations',
      'accounts',
      'backtest_cache',
      'backtest_results',
      'candles',
      'lightgbm_models',
      'remote_optimizer_jobs',
      'signals',
      'strategies',
      'system_logs',
      'templates',
      'tickers',
      'trades'
    ];

    return this.db.withTransaction(async (client: PoolClient) => {
      const statsRows = await this.db.all<TableRowEstimateRow>(
        `
          SELECT relname AS table_name, COALESCE(n_live_tup, 0) AS row_estimate
          FROM pg_stat_user_tables
          WHERE schemaname = current_schema()
            AND relname = ANY(?::text[])
        `,
        [tablesToTruncate],
        client
      );

      const rowEstimateMap = new Map<string, number>();
      for (const row of statsRows) {
        rowEstimateMap.set(row.table_name, toInteger(row.row_estimate, 0));
      }

      const rowEstimates: Record<string, number> = {};
      for (const tableName of tablesToTruncate) {
        rowEstimates[tableName] = rowEstimateMap.get(tableName) ?? 0;
      }
      const totalRowEstimate = tablesToTruncate.reduce((sum, table) => sum + (rowEstimates[table] ?? 0), 0);

      const quotedTables = tablesToTruncate.map((table) => `"${table}"`).join(', ');
      await this.db.exec(`TRUNCATE TABLE ${quotedTables} RESTART IDENTITY CASCADE`, client);
      this.tickersRepo.invalidateCache();

      return {
        truncatedTables: tablesToTruncate,
        rowEstimates,
        totalRowEstimate
      };
    });
  }
}
