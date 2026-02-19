import { DbClient } from '../core/DbClient';
import type { AccountSignalSkipRow } from '../types';

export class AccountSignalSkipsRepo {
  constructor(private readonly db: DbClient) {}

  async getLatestSignalSkipDateForStrategy(
    strategyId: string,
    sources?: string[]
  ): Promise<string | null> {
    const params: Array<string | string[]> = [strategyId];
    let sql = `
      SELECT MAX(signal_date) AS latest_date
        FROM account_signal_skips
       WHERE strategy_id = ?
    `;

    if (sources && sources.length > 0) {
      sql += ' AND source = ANY(?::text[])';
      params.push(sources);
    }

    const row = await this.db.get<{ latest_date?: string | null }>(sql, params);
    return row?.latest_date ?? null;
  }

  async getAccountSignalSkipsForStrategyInRange(
    strategyId: string,
    startDate: Date,
    endDate: Date,
    sources?: string[]
  ): Promise<AccountSignalSkipRow[]> {
    const start = startDate.toISOString().slice(0, 10);
    const end = endDate.toISOString().slice(0, 10);
    const params: Array<string | string[]> = [strategyId, start, end];
    let sql = `
      SELECT id,
             strategy_id,
             account_id,
             ticker,
             signal_date,
             action,
             source,
             reason,
             details,
             created_at
        FROM account_signal_skips
       WHERE strategy_id = ?
         AND signal_date >= ?
         AND signal_date <= ?
    `;

    if (sources && sources.length > 0) {
      sql += ' AND source = ANY(?::text[])';
      params.push(sources);
    }

    sql += ' ORDER BY signal_date DESC, created_at DESC';

    return this.db.all<AccountSignalSkipRow>(sql, params);
  }
}
