import type { QueryResultRow } from 'pg';
import { DbClient, type QueryValue } from '../core/DbClient';
import { toNumber } from '../core/valueParsers';

type SignalRow = QueryResultRow & {
  date: string;
  ticker: string;
  strategy_id: string;
  action: 'buy' | 'sell';
  confidence: number | null;
};

type SignalWithStrategyRow = SignalRow & {
  strategy_name: string | null;
};

type SignalLineCountRow = QueryResultRow & {
  date: string;
  action: string;
  count: number;
};

type SignalConfidenceMaxRow = QueryResultRow & {
  day: string;
  action: string;
  max: number;
};

type SignalSummaryRow = QueryResultRow & {
  count: number;
  first_date: string | null;
  last_date: string | null;
};

type SignalMostRecentRow = QueryResultRow & {
  last_date: string | null;
};

type CountRow = QueryResultRow & { count: number };

export class SignalsRepo {
  constructor(private readonly db: DbClient) {}

  async deleteSignalsForDate(date: Date): Promise<number> {
    const dateStr = date.toISOString().split('T')[0];
    const res = await this.db.run('DELETE FROM signals WHERE date = ?', [dateStr]);
    return res.changes || 0;
  }

  async getSignalsForTicker(
    ticker: string,
    userId: number,
    startDate?: Date,
    endDate?: Date
  ): Promise<Array<{ date: Date; ticker: string; strategyId: string; action: 'buy' | 'sell'; confidence: number | null }>> {
    const params: QueryValue[] = [ticker];
    const conditions: string[] = ['sig.ticker = ?'];
    if (startDate) {
      const s = new Date(startDate);
      s.setUTCHours(0, 0, 0, 0);
      params.push(s.toISOString().split('T')[0]);
      conditions.push('sig.date >= ?');
    }
    if (endDate) {
      const e = new Date(endDate);
      e.setUTCHours(0, 0, 0, 0);
      params.push(e.toISOString().split('T')[0]);
      conditions.push('sig.date <= ?');
    }

    conditions.push('(sig.user_id = ? OR sig.user_id IS NULL)');
    params.push(userId);

    const rows = await this.db.all<SignalRow>(
      `SELECT sig.date, sig.ticker, sig.strategy_id, sig.action, sig.confidence
       FROM signals sig
       WHERE ${conditions.join(' AND ')}
       ORDER BY sig.date ASC`,
      params
    );
    return rows.map((row) => ({
      date: new Date(row.date),
      ticker: row.ticker,
      strategyId: row.strategy_id,
      action: row.action,
      confidence: row.confidence ?? null,
    }));
  }

  async getSignalsForStrategy(
    strategyId: string,
    maxSignalDays: number = 7
  ): Promise<Array<{ date: Date; ticker: string; strategyId: string; action: 'buy' | 'sell'; confidence: number | null }>> {
    const boundedDays = Number.isFinite(maxSignalDays) && maxSignalDays > 0 ? Math.floor(maxSignalDays) : 7;
    const rows = await this.db.all<SignalRow>(
      `WITH recent_dates AS (
         SELECT DISTINCT date
         FROM signals
         WHERE strategy_id = ?
         ORDER BY date DESC
         LIMIT ?
       )
       SELECT sig.date, sig.ticker, sig.strategy_id, sig.action, sig.confidence
       FROM signals sig
       INNER JOIN recent_dates rd ON rd.date = sig.date
       WHERE sig.strategy_id = ?
       ORDER BY sig.date DESC, sig.ticker ASC`,
      [strategyId, boundedDays, strategyId]
    );
    return rows.map((row) => ({
      date: new Date(row.date),
      ticker: row.ticker,
      strategyId: row.strategy_id,
      action: row.action,
      confidence: row.confidence ?? null,
    }));
  }

  async getSignalLineCountsByDay(
    strategyId: string,
    lookbackYears: number = 3
  ): Promise<Array<{ isoDate: string; buyCount: number; sellCount: number }>> {
    const boundedYears = Number.isFinite(lookbackYears) && lookbackYears > 0 ? lookbackYears : 3;
    const cutoffDate = new Date();
    cutoffDate.setUTCFullYear(cutoffDate.getUTCFullYear() - boundedYears);
    cutoffDate.setUTCHours(0, 0, 0, 0);
    const cutoffIso = cutoffDate.toISOString().split('T')[0];

    const rows = await this.db.all<SignalLineCountRow>(
      `SELECT date, action, COUNT(*) AS count
       FROM signals
       WHERE strategy_id = ?
         AND date >= ?
       GROUP BY date, action
       ORDER BY date ASC`,
      [strategyId, cutoffIso]
    );

    const dailyMap = new Map<string, { buyCount: number; sellCount: number }>();

    for (const row of rows) {
      if (!row || !row.date) {
        continue;
      }
      const isoDate = typeof row.date === 'string' ? row.date : new Date(row.date).toISOString().split('T')[0];
      if (!dailyMap.has(isoDate)) {
        dailyMap.set(isoDate, { buyCount: 0, sellCount: 0 });
      }

      const entry = dailyMap.get(isoDate)!;
      const rawCount = typeof row.count === 'number' ? row.count : Number(row.count) || 0;
      const normalizedAction = typeof row.action === 'string' ? row.action.toLowerCase() : '';
      if (rawCount <= 0) {
        continue;
      }

      if (normalizedAction === 'sell') {
        entry.sellCount += rawCount;
      } else if (normalizedAction === 'buy') {
        entry.buyCount += rawCount;
      }
    }

    return Array.from(dailyMap.entries())
      .map(([isoDate, counts]) => ({
        isoDate,
        buyCount: counts.buyCount,
        sellCount: counts.sellCount
      }))
      .filter((entry) => entry.buyCount > 0 || entry.sellCount > 0)
      .sort((a, b) => a.isoDate.localeCompare(b.isoDate));
  }

  async getSignalConfidenceMaxByDay(
    strategyId: string,
    lookbackDays: number = 365
  ): Promise<Array<{ isoDate: string; buyMax: number | null; sellMax: number | null }>> {
    const boundedDays = Number.isFinite(lookbackDays) && lookbackDays > 0 ? lookbackDays : 365;
    const cutoffDate = new Date();
    cutoffDate.setUTCDate(cutoffDate.getUTCDate() - boundedDays);
    cutoffDate.setUTCHours(0, 0, 0, 0);
    const cutoffIso = cutoffDate.toISOString().split('T')[0];

    const rows = await this.db.all<SignalConfidenceMaxRow>(
      `SELECT date AS day,
         action,
         MAX(confidence) AS max
       FROM signals
       WHERE strategy_id = ?
         AND date >= ?
         AND confidence IS NOT NULL
         AND action IN ('buy', 'sell')
       GROUP BY day, action
       ORDER BY day ASC`,
      [strategyId, cutoffIso]
    );

    const dayMap = new Map<string, { isoDate: string; buyMax: number | null; sellMax: number | null }>();

    for (const row of rows) {
      const isoDate = typeof row.day === 'string'
        ? row.day
        : new Date(row.day).toISOString().split('T')[0];
      if (!isoDate) {
        continue;
      }
      if (!dayMap.has(isoDate)) {
        dayMap.set(isoDate, { isoDate, buyMax: null, sellMax: null });
      }
      const entry = dayMap.get(isoDate)!;
      const action = typeof row.action === 'string' ? row.action.toLowerCase() : '';
      const max = Number(row.max);
      const normalizedMax = Number.isFinite(max) ? max : null;

      if (action === 'buy') {
        entry.buyMax = normalizedMax;
      } else if (action === 'sell') {
        entry.sellMax = normalizedMax;
      }
    }

    return Array.from(dayMap.values())
      .sort((a, b) => a.isoDate.localeCompare(b.isoDate));
  }

  async getSignals(
    userId: number,
    daysBack: number = 7
  ): Promise<Array<{ date: Date; ticker: string; strategyId: string; action: 'buy' | 'sell'; confidence: number | null }>> {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - daysBack);
    const params: QueryValue[] = [cutoff.toISOString().split('T')[0]];
    const conditions: string[] = ['sig.date >= ?'];

    conditions.push('(sig.user_id = ? OR sig.user_id IS NULL)');
    params.push(userId);

    const rows = await this.db.all<SignalRow>(
      `SELECT sig.date, sig.ticker, sig.strategy_id, sig.action, sig.confidence
       FROM signals sig
       WHERE ${conditions.join(' AND ')}
       ORDER BY sig.date DESC, sig.ticker ASC`,
      params
    );
    return rows.map((row) => ({
      date: new Date(row.date),
      ticker: row.ticker,
      strategyId: row.strategy_id,
      action: row.action,
      confidence: row.confidence ?? null,
    }));
  }

  async getSignalSummary(strategyId?: string): Promise<{ count: number; firstDate: Date | null; lastDate: Date | null }> {
    const whereClause = strategyId ? 'WHERE strategy_id = ?' : '';
    const params: QueryValue[] = strategyId ? [strategyId] : [];
    const row = await this.db.get<SignalSummaryRow>(
      `SELECT
         COUNT(*) AS count,
         MIN(date) AS first_date,
         MAX(date) AS last_date
       FROM signals
       ${whereClause}`,
      params
    );

    const count = toNumber(row?.count, 0);
    const firstDate = row?.first_date ? new Date(row.first_date) : null;
    const lastDate = row?.last_date ? new Date(row.last_date) : null;

    return { count, firstDate, lastDate };
  }

  async getMostRecentSignalDate(userId: number): Promise<Date | null> {
    const row = await this.db.get<SignalMostRecentRow>(
      `SELECT MAX(date) AS last_date
       FROM signals
       WHERE (user_id = ? OR user_id IS NULL)`,
      [userId]
    );

    return row?.last_date ? new Date(row.last_date) : null;
  }

  async getTopSignalsForDate(
    targetDate: Date,
    userId: number,
    limit: number = 8
  ): Promise<Array<{ date: Date; ticker: string; strategyId: string; strategyName: string | null; action: 'buy' | 'sell'; confidence: number | null }>> {
    const isoDate = targetDate.toISOString().split('T')[0];
    const boundedLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 50) : 8;
    const rows = await this.db.all<SignalWithStrategyRow>(
      `SELECT sig.date, sig.ticker, sig.strategy_id, sig.action, sig.confidence, strat.name AS strategy_name
       FROM signals sig
       LEFT JOIN strategies strat ON strat.id = sig.strategy_id
       WHERE sig.date = ?
         AND (sig.user_id = ? OR sig.user_id IS NULL)
       ORDER BY sig.confidence DESC NULLS LAST, sig.ticker ASC
       LIMIT ?`,
      [isoDate, userId, boundedLimit]
    );

    return rows.map((row) => ({
      date: new Date(row.date),
      ticker: row.ticker,
      strategyId: row.strategy_id,
      strategyName: row.strategy_name ?? null,
      action: row.action,
      confidence: row.confidence ?? null
    }));
  }

  async clearAllSignals(): Promise<number> {
    try {
      const existingCount = await this.getSignalCount();
      if (existingCount === 0) {
        return 0;
      }

      await this.db.run('DELETE FROM signals');
      return existingCount;
    } catch (error) {
      console.error('Failed to clear all signals:', error);
      throw error;
    }
  }

  private async getSignalCount(): Promise<number> {
    const result = await this.db.get<CountRow>('SELECT COUNT(*) as count FROM signals');
    return toNumber(result?.count, 0);
  }
}
