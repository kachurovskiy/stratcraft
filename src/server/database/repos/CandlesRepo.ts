import type { QueryResultRow } from 'pg';
import type { Candle } from '../../../shared/types/StrategyTemplate';
import { DbClient, type QueryValue } from '../core/DbClient';
import { toInteger, toNullableInteger, toNumber } from '../core/valueParsers';

type CandleRow = QueryResultRow & {
  ticker: string;
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  unadjusted_close: number | null;
  volume_shares: number;
};

type MaxDateRow = QueryResultRow & {
  ticker: string;
  max_date: string | null;
};

type DateRangeRow = QueryResultRow & {
  min_date: string | null;
  max_date: string | null;
};

type DateRow = QueryResultRow & {
  date: string;
};

type WeeklyCandleRow = QueryResultRow & {
  ticker: string;
  week_start: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume_shares: number | null;
};

type MonthlyCandleRow = QueryResultRow & {
  ticker: string;
  month_start: string;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume_shares: number | null;
};

type LatestDateRow = QueryResultRow & {
  latest_date: string | null;
};

type SymbolRow = QueryResultRow & {
  symbol: string;
};

type CloseRow = QueryResultRow & { close: number | null };

export class CandlesRepo {
  constructor(private readonly db: DbClient) {}

  private mapCandleRow(row: CandleRow): Candle {
    return {
      ticker: row.ticker,
      date: new Date(row.date),
      open: toNumber(row.open),
      high: toNumber(row.high),
      low: toNumber(row.low),
      close: toNumber(row.close),
      unadjustedClose: toNumber(row.unadjusted_close ?? row.close),
      volumeShares: toInteger(row.volume_shares)
    };
  }

  async getLastCandle(ticker: string): Promise<Candle | null> {
    const row = await this.db.get<CandleRow>(
      `
        SELECT ticker, date, open, high, low, close, unadjusted_close, volume_shares
        FROM candles
        WHERE ticker = ?
        ORDER BY date DESC
        LIMIT 1
      `,
      [ticker]
    );

    if (!row) {
      return null;
    }

    return this.mapCandleRow(row);
  }

  async getLastCandleDates(tickers: string[]): Promise<Record<string, Date>> {
    if (tickers.length === 0) {
      return {};
    }

    const placeholders = tickers.map(() => '?').join(',');
    const sql = `
      SELECT ticker, MAX(date) as max_date
      FROM candles
      WHERE ticker IN (${placeholders})
      GROUP BY ticker
    `;
    const rows = await this.db.all<MaxDateRow>(sql, tickers);
    const result: Record<string, Date> = {};

    for (const row of rows) {
      if (row.ticker && row.max_date) {
        result[row.ticker] = new Date(row.max_date);
      }
    }

    return result;
  }

  async getCandles(tickers: string[], startDate?: Date, endDate?: Date): Promise<Record<string, Candle[]>> {
    if (tickers.length === 0) {
      return {};
    }

    const placeholders = tickers.map(() => '?').join(',');
    let sql = `
      SELECT ticker, date, open, high, low, close, unadjusted_close, volume_shares
      FROM candles
      WHERE ticker IN (${placeholders})
    `;
    const params: QueryValue[] = [...tickers];

    if (startDate) {
      sql += ' AND date >= ?';
      params.push(startDate.toISOString().split('T')[0]);
    }

    if (endDate) {
      sql += ' AND date <= ?';
      params.push(endDate.toISOString().split('T')[0]);
    }

    sql += ' ORDER BY ticker, date ASC';

    const rows = await this.db.all<CandleRow>(sql, params);

    const result: Record<string, Candle[]> = {};
    for (const ticker of tickers) {
      result[ticker] = [];
    }

    for (const row of rows) {
      if (!result[row.ticker]) {
        result[row.ticker] = [];
      }
      result[row.ticker].push(this.mapCandleRow(row));
    }

    return result;
  }

  async getDateRangeForTickers(tickers: string[]): Promise<{ startDate: Date | null; endDate: Date | null }> {
    if (tickers.length === 0) {
      return { startDate: null, endDate: null };
    }

    const placeholders = tickers.map(() => '?').join(',');
    const sql = `
      SELECT MIN(date) as min_date, MAX(date) as max_date
      FROM candles
      WHERE ticker IN (${placeholders})
    `;
    const row = await this.db.get<DateRangeRow>(sql, tickers);

    return {
      startDate: row?.min_date ? new Date(row.min_date) : null,
      endDate: row?.max_date ? new Date(row.max_date) : null
    };
  }

  async getUniqueDatesForTickers(tickers: string[]): Promise<Date[]> {
    if (tickers.length === 0) {
      return [];
    }

    const placeholders = tickers.map(() => '?').join(',');
    const sql = `
      SELECT DISTINCT date
      FROM candles
      WHERE ticker IN (${placeholders})
      ORDER BY date ASC
    `;

    const rows = await this.db.all<DateRow>(sql, tickers);
    return rows.map((row) => new Date(row.date));
  }

  async getWeeklyCandles(ticker: string, startDate?: Date, endDate?: Date): Promise<Candle[]> {
    const params: QueryValue[] = [ticker];
    let dateConditions = '';

    if (startDate) {
      dateConditions += ' AND date >= ?';
      params.push(startDate.toISOString().split('T')[0]);
    }

    if (endDate) {
      dateConditions += ' AND date <= ?';
      params.push(endDate.toISOString().split('T')[0]);
    }

    const sql = `
      WITH ordered AS (
        SELECT
          ticker,
          date_trunc('week', date::timestamp)::date AS week_start,
          date,
          open,
          high,
          low,
          close,
          volume_shares,
          ROW_NUMBER() OVER (PARTITION BY ticker, date_trunc('week', date::timestamp) ORDER BY date ASC) AS rn_asc,
          ROW_NUMBER() OVER (PARTITION BY ticker, date_trunc('week', date::timestamp) ORDER BY date DESC) AS rn_desc
        FROM candles
        WHERE ticker = ?
        ${dateConditions}
      )
      SELECT
        ticker,
        week_start,
        MAX(CASE WHEN rn_asc = 1 THEN open END) AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        MAX(CASE WHEN rn_desc = 1 THEN close END) AS close,
        SUM(volume_shares) AS volume_shares
      FROM ordered
      GROUP BY ticker, week_start
      ORDER BY week_start ASC
    `;

    const rows = await this.db.all<WeeklyCandleRow>(sql, params);

    return rows.map((row) => ({
      ticker: row.ticker,
      date: new Date(row.week_start),
      open: toNumber(row.open),
      high: toNumber(row.high),
      low: toNumber(row.low),
      close: toNumber(row.close),
      unadjustedClose: toNumber(row.close),
      volumeShares: toInteger(row.volume_shares)
    }));
  }

  async getMonthlyCandles(ticker: string, startDate?: Date, endDate?: Date): Promise<Candle[]> {
    const params: QueryValue[] = [ticker];
    let dateConditions = '';

    if (startDate) {
      dateConditions += ' AND date >= ?';
      params.push(startDate.toISOString().split('T')[0]);
    }

    if (endDate) {
      dateConditions += ' AND date <= ?';
      params.push(endDate.toISOString().split('T')[0]);
    }

    const sql = `
      WITH ordered AS (
        SELECT
          ticker,
          date_trunc('month', date::timestamp)::date AS month_start,
          date,
          open,
          high,
          low,
          close,
          volume_shares,
          ROW_NUMBER() OVER (PARTITION BY ticker, date_trunc('month', date::timestamp) ORDER BY date ASC) AS rn_asc,
          ROW_NUMBER() OVER (PARTITION BY ticker, date_trunc('month', date::timestamp) ORDER BY date DESC) AS rn_desc
        FROM candles
        WHERE ticker = ?
        ${dateConditions}
      )
      SELECT
        ticker,
        month_start,
        MAX(CASE WHEN rn_asc = 1 THEN open END) AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        MAX(CASE WHEN rn_desc = 1 THEN close END) AS close,
        SUM(volume_shares) AS volume_shares
      FROM ordered
      GROUP BY ticker, month_start
      ORDER BY month_start ASC
    `;

    const rows = await this.db.all<MonthlyCandleRow>(sql, params);

    return rows.map((row) => ({
      ticker: row.ticker,
      date: new Date(row.month_start),
      open: toNumber(row.open),
      high: toNumber(row.high),
      low: toNumber(row.low),
      close: toNumber(row.close),
      unadjustedClose: toNumber(row.close),
      volumeShares: toInteger(row.volume_shares)
    }));
  }

  async upsertCandlesForTicker(symbol: string, candles: Candle[]): Promise<void> {
    if (candles.length === 0) {
      return;
    }

    const normalizedSymbol = symbol.trim().toUpperCase();

    await this.db.withTransaction(async (client) => {
      for (const candle of candles) {
        const volumeShares = toNullableInteger(candle.volumeShares);
        const normalizedVolumeShares = volumeShares === null ? null : Math.max(0, volumeShares);

        await this.db.run(
          `
            INSERT INTO candles (ticker, date, open, high, low, close, unadjusted_close, volume_shares)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, 0))
            ON CONFLICT (ticker, date) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                unadjusted_close = EXCLUDED.unadjusted_close,
                volume_shares = COALESCE(?, candles.volume_shares)
          `,
          [
            normalizedSymbol,
            candle.date.toISOString().split('T')[0],
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.unadjustedClose ?? candle.close,
            normalizedVolumeShares,
            normalizedVolumeShares
          ],
          client
        );
      }
    });
  }

  async replaceCandlesForTicker(symbol: string, candles: Candle[]): Promise<void> {
    if (candles.length === 0) {
      return;
    }

    const normalizedSymbol = symbol.trim().toUpperCase();

    await this.db.withTransaction(async (client) => {
      await this.db.run('DELETE FROM candles WHERE ticker = ?', [normalizedSymbol], client);

      for (const candle of candles) {
        const volumeShares = toNullableInteger(candle.volumeShares);
        const normalizedVolumeShares = volumeShares === null ? null : Math.max(0, volumeShares);

        await this.db.run(
          `
            INSERT INTO candles (ticker, date, open, high, low, close, unadjusted_close, volume_shares)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, 0))
            ON CONFLICT (ticker, date) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                unadjusted_close = EXCLUDED.unadjusted_close,
                volume_shares = COALESCE(?, candles.volume_shares)
          `,
          [
            normalizedSymbol,
            candle.date.toISOString().split('T')[0],
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.unadjustedClose ?? candle.close,
            normalizedVolumeShares,
            normalizedVolumeShares
          ],
          client
        );
      }

      await this.db.run(
        `
          UPDATE tickers
          SET last_updated = NULL,
              max_fluctuation_ratio = NULL
          WHERE symbol = ?
        `,
        [normalizedSymbol],
        client
      );
    });
  }

  async getLatestCandleDate(ticker: string): Promise<Date | null> {
    const row = await this.db.get<LatestDateRow>(
      `
        SELECT MAX(date) as latest_date
        FROM candles
        WHERE ticker = ?
      `,
      [ticker]
    );

    return row?.latest_date ? new Date(row.latest_date) : null;
  }

  async getLatestGlobalCandleDate(): Promise<Date | null> {
    const row = await this.db.get<LatestDateRow>(
      `
        SELECT MAX(date) as latest_date
        FROM candles
      `
    );

    return row?.latest_date ? new Date(row.latest_date) : null;
  }

  async getTickersWithNoCandles(): Promise<string[]> {
    const rows = await this.db.all<SymbolRow>(
      `
        SELECT t.symbol
        FROM tickers t
        LEFT JOIN candles c ON t.symbol = c.ticker
        WHERE c.ticker IS NULL
        ORDER BY t.volume_usd DESC
      `
    );

    return rows.map((row) => row.symbol);
  }

  async getLatestPrice(ticker: string): Promise<number> {
    try {
      const row = await this.db.get<CloseRow>(
        `
          SELECT close
          FROM candles
          WHERE ticker = ?
          ORDER BY date DESC
          LIMIT 1
        `,
        [ticker]
      );
      return row ? toNumber(row.close) : 0;
    } catch (error) {
      console.error(`Error getting latest price for ${ticker}:`, error);
      return 0;
    }
  }

  async clearCandlesForTicker(symbol: string): Promise<number> {
    try {
      const normalizedSymbol = symbol.trim().toUpperCase();
      const result = await this.db.run('DELETE FROM candles WHERE ticker = ?', [normalizedSymbol]);
      const deleted = result.changes || 0;

      await this.db.run(
        `
          UPDATE tickers
          SET last_updated = NULL,
              max_fluctuation_ratio = NULL
          WHERE symbol = ?
        `,
        [normalizedSymbol]
      );

      return deleted;
    } catch (error) {
      console.error(`Failed to clear candles for ${symbol}:`, error);
      throw error;
    }
  }

  async clearAllCandles(): Promise<number> {
    try {
      const result = await this.db.run('DELETE FROM candles');
      return result.changes || 0;
    } catch (error) {
      console.error('Failed to clear all candles:', error);
      throw error;
    }
  }
}
