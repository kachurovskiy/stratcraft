import type { QueryResultRow } from 'pg';
import { DbClient } from '../core/DbClient';
import { toInteger, toNumber } from '../core/valueParsers';

type TableStatsRow = QueryResultRow & {
  schemaname: string;
  table_name: string;
  row_estimate: number;
  total_bytes: number;
  table_bytes: number;
  index_bytes: number;
};

type CountRow = QueryResultRow & {
  count: number;
};

type TableStats = {
  schema: string;
  tableName: string;
  rowEstimate: number;
  totalBytes: number;
  tableBytes: number;
  indexBytes: number;
};

type AdminEntityCounts = {
  templates: number;
  strategies: number;
  backtests: number;
  trades: number;
  accounts: number;
  accountOperations: number;
  signals: number;
  tickers: number;
  candles: number;
};

export class AdminRepo {
  constructor(private readonly db: DbClient) {}

  async getDatabaseTableStats(): Promise<TableStats[]> {
    try {
      const rows = await this.db.all<TableStatsRow>(`
        SELECT
          schemaname,
          relname AS table_name,
          n_live_tup AS row_estimate,
          pg_total_relation_size(relid) AS total_bytes,
          pg_relation_size(relid) AS table_bytes,
          pg_indexes_size(relid) AS index_bytes
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(relid) DESC
      `);

      return rows.map((row) => ({
        schema: row.schemaname,
        tableName: row.table_name,
        rowEstimate: Math.max(0, toInteger(row.row_estimate, 0)),
        totalBytes: Math.max(0, toNumber(row.total_bytes, 0)),
        tableBytes: Math.max(0, toNumber(row.table_bytes, 0)),
        indexBytes: Math.max(0, toNumber(row.index_bytes, 0))
      }));
    } catch (error) {
      console.error('Error fetching database table stats:', error);
      return [];
    }
  }

  async getAdminEntityCounts(): Promise<AdminEntityCounts> {
    const createEmptyCounts = (): AdminEntityCounts => ({
      templates: 0,
      strategies: 0,
      backtests: 0,
      trades: 0,
      accounts: 0,
      accountOperations: 0,
      signals: 0,
      tickers: 0,
      candles: 0
    });

    const entityQueries: Array<{ key: keyof AdminEntityCounts; sql: string }> = [
      { key: 'templates', sql: 'SELECT COUNT(*) AS count FROM templates' },
      { key: 'strategies', sql: 'SELECT COUNT(*) AS count FROM strategies' },
      { key: 'backtests', sql: 'SELECT COUNT(*) AS count FROM backtest_results' },
      { key: 'trades', sql: 'SELECT COUNT(*) AS count FROM trades' },
      { key: 'accounts', sql: 'SELECT COUNT(*) AS count FROM accounts' },
      { key: 'accountOperations', sql: 'SELECT COUNT(*) AS count FROM account_operations' },
      { key: 'signals', sql: 'SELECT COUNT(*) AS count FROM signals' },
      { key: 'tickers', sql: 'SELECT COUNT(*) AS count FROM tickers' },
      { key: 'candles', sql: 'SELECT COUNT(*) AS count FROM candles' }
    ];

    try {
      const rows = await Promise.all(entityQueries.map(({ sql }) => this.db.get<CountRow>(sql)));
      const counts = createEmptyCounts();
      rows.forEach((row, index) => {
        const { key } = entityQueries[index];
        counts[key] = toNumber(row?.count, 0);
      });
      return counts;
    } catch (error) {
      console.error('Error fetching admin entity counts:', error);
      return createEmptyCounts();
    }
  }
}
