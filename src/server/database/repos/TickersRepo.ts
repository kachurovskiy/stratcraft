import type { PoolClient, QueryResultRow } from 'pg';
import type { TickerInfo } from '../../../shared/types/StrategyTemplate';
import { DbClient, type QueryValue } from '../core/DbClient';
import { toInteger, toNullableNumber } from '../core/valueParsers';
import type { TickerAssetRecord, TickerAssetType, TickerWithCandleStats } from '../types';

type TickerBaseRow = QueryResultRow & {
  symbol: string;
  name: string | null;
  tradable: boolean;
  shortable: boolean;
  easy_to_borrow: boolean;
  asset_type: string | null;
  expense_ratio: number | null;
  market_cap: number | null;
  volume_usd: number | null;
  max_fluctuation_ratio: number | null;
  last_updated: Date | null;
  training: boolean;
};

type TickerStatsRow = TickerBaseRow & {
  candle_count: number;
  last_candle_date: string | null;
  first_candle_date: string | null;
  min_low: number | null;
  max_high: number | null;
  min_close: number | null;
  max_close: number | null;
  min_usd_volume: number | null;
  max_usd_volume: number | null;
  latest_close: number | null;
  latest_volume_shares: number | null;
  latest_usd_volume: number | null;
};

type TickerRow = TickerBaseRow;

type TradeCountRow = QueryResultRow & {
  ticker: string;
  trade_count: number;
};

type CountRow = QueryResultRow & { count: number };

type CloseRow = QueryResultRow & { close: number | null };

export class TickersRepo {
  private tickerCache: TickerWithCandleStats[] | null = null;
  private tickerCachePromise: Promise<TickerWithCandleStats[]> | null = null;
  private tickerCacheGeneration = 0;

  constructor(private readonly db: DbClient) {}

  invalidateCache(): void {
    this.tickerCache = null;
    this.tickerCachePromise = null;
    this.tickerCacheGeneration += 1;
  }

  private async loadTickersFromDatabase(): Promise<TickerWithCandleStats[]> {
    const rows = await this.db.all<TickerStatsRow>(
      `
        SELECT
          t.symbol,
          t.name,
          t.tradable,
          t.shortable,
          t.easy_to_borrow,
          t.asset_type,
          t.expense_ratio,
          t.market_cap,
          t.volume_usd,
          t.max_fluctuation_ratio,
          t.last_updated,
          t.training,
          COUNT(c.ticker) as candle_count,
          MAX(c.date) as last_candle_date,
          MIN(c.date) as first_candle_date,
          MIN(c.low) as min_low,
          MAX(c.high) as max_high,
          MIN(c.close) as min_close,
          MAX(c.close) as max_close,
          MIN(c.close * c.volume_shares) as min_usd_volume,
          MAX(c.close * c.volume_shares) as max_usd_volume,
          latest.latest_close,
          latest.latest_volume_shares,
          (latest.latest_close * latest.latest_volume_shares) as latest_usd_volume
        FROM tickers t
        LEFT JOIN candles c ON t.symbol = c.ticker
        LEFT JOIN LATERAL (
          SELECT
            c2.date as latest_date,
            c2.close as latest_close,
            c2.volume_shares as latest_volume_shares
          FROM candles c2
          WHERE c2.ticker = t.symbol
          ORDER BY c2.date DESC
          LIMIT 1
        ) latest ON TRUE
        GROUP BY t.symbol, t.name, t.tradable, t.shortable, t.easy_to_borrow, t.asset_type, t.expense_ratio, t.market_cap, t.volume_usd, t.max_fluctuation_ratio, t.last_updated, t.training, latest.latest_close, latest.latest_volume_shares
        ORDER BY t.volume_usd DESC
      `
    );

    return rows.map((row) => {
      const marketCap = toNullableNumber(row.market_cap);
      const volumeUsd = toNullableNumber(row.volume_usd);
      const maxFluctuationRatio = toNullableNumber(row.max_fluctuation_ratio);
      const candleCount = toNullableNumber(row.candle_count) ?? 0;
      const minLow = toNullableNumber(row.min_low);
      const maxHigh = toNullableNumber(row.max_high);
      const minClose = toNullableNumber(row.min_close);
      const maxClose = toNullableNumber(row.max_close);
      const minUsdVolume = toNullableNumber(row.min_usd_volume);
      const maxUsdVolume = toNullableNumber(row.max_usd_volume);
      const latestClose = toNullableNumber(row.latest_close);
      const latestVolumeShares = toNullableNumber(row.latest_volume_shares);
      const latestUsdVolume = toNullableNumber(row.latest_usd_volume);

      return {
        symbol: row.symbol,
        name: row.name,
        tradable: Boolean(row.tradable),
        shortable: Boolean(row.shortable),
        easyToBorrow: Boolean(row.easy_to_borrow),
        assetType: typeof row.asset_type === 'string' ? (row.asset_type as TickerAssetType) : null,
        expenseRatio: toNullableNumber(row.expense_ratio),
        marketCap: marketCap === null ? undefined : marketCap,
        volumeUsd: volumeUsd === null ? undefined : volumeUsd,
        maxFluctuationRatio: maxFluctuationRatio === null ? undefined : maxFluctuationRatio,
        lastUpdated: row.last_updated ? new Date(row.last_updated) : undefined,
        training: Boolean(row.training),
        candleCount,
        lastCandleDate: row.last_candle_date ? new Date(row.last_candle_date) : null,
        firstCandleDate: row.first_candle_date ? new Date(row.first_candle_date) : null,
        minLow,
        maxHigh,
        minClose,
        maxClose,
        minUsdVolume,
        maxUsdVolume,
        latestClose,
        latestVolumeShares,
        latestUsdVolume
      } satisfies TickerWithCandleStats;
    });
  }

  async getTickers(): Promise<TickerWithCandleStats[]> {
    if (this.tickerCache) {
      return this.tickerCache;
    }

    if (!this.tickerCachePromise) {
      const fetchGeneration = this.tickerCacheGeneration;
      const fetchPromise = this.loadTickersFromDatabase()
        .then((results) => {
          if (fetchGeneration === this.tickerCacheGeneration) {
            this.tickerCache = results;
          }
          return results;
        })
        .finally(() => {
          if (this.tickerCachePromise === fetchPromise) {
            this.tickerCachePromise = null;
          }
        });
      this.tickerCachePromise = fetchPromise;
    }

    return this.tickerCachePromise;
  }

  async getTradeCountsByTicker(): Promise<Record<string, number>> {
    const rows = await this.db.all<TradeCountRow>(
      `
        SELECT ticker, COUNT(*) as trade_count
        FROM trades
        GROUP BY ticker
      `
    );

    const tradeCounts: Record<string, number> = {};
    rows.forEach((row) => {
      tradeCounts[row.ticker] = toInteger(row.trade_count, 0);
    });

    return tradeCounts;
  }

  async getTickersWithTradeCounts(): Promise<(TickerWithCandleStats & { tradeCount: number })[]> {
    const tickersWithCandles = await this.getTickers();
    const tradeCounts = await this.getTradeCountsByTicker();
    return tickersWithCandles.map((ticker) => ({
      ...ticker,
      tradeCount: tradeCounts[ticker.symbol] || 0
    }));
  }

  async getTicker(symbol: string): Promise<TickerInfo | null> {
    const row = await this.db.get<TickerRow>(
      `
        SELECT symbol, name, tradable, shortable, easy_to_borrow, asset_type, expense_ratio, market_cap, volume_usd, max_fluctuation_ratio, last_updated, training
        FROM tickers
        WHERE symbol = ?
      `,
      [symbol]
    );

    if (!row) {
      return null;
    }

    const expenseRatio = toNullableNumber(row.expense_ratio);

    return {
      symbol: row.symbol,
      name: row.name,
      tradable: Boolean(row.tradable),
      shortable: Boolean(row.shortable),
      easyToBorrow: Boolean(row.easy_to_borrow),
      assetType: typeof row.asset_type === 'string' ? (row.asset_type as TickerAssetType) : null,
      expenseRatio,
      marketCap: row.market_cap === null ? undefined : toNullableNumber(row.market_cap) ?? undefined,
      volumeUsd: row.volume_usd === null ? undefined : toNullableNumber(row.volume_usd) ?? undefined,
      maxFluctuationRatio:
        row.max_fluctuation_ratio === null ? undefined : toNullableNumber(row.max_fluctuation_ratio) ?? undefined,
      lastUpdated: row.last_updated ? new Date(row.last_updated) : undefined,
      training: Boolean(row.training)
    };
  }

  async upsertTicker(ticker: TickerInfo): Promise<void> {
    await this.db.run(
      `
        INSERT INTO tickers (symbol, market_cap, volume_usd, max_fluctuation_ratio, last_updated)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (symbol) DO UPDATE
        SET market_cap = EXCLUDED.market_cap,
            volume_usd = EXCLUDED.volume_usd,
            max_fluctuation_ratio = EXCLUDED.max_fluctuation_ratio,
            last_updated = EXCLUDED.last_updated
      `,
      [
        ticker.symbol,
        ticker.marketCap,
        ticker.volumeUsd,
        ticker.maxFluctuationRatio,
        ticker.lastUpdated || new Date()
      ]
    );
    this.invalidateCache();
  }

  async getTickerCount(): Promise<number> {
    const row = await this.db.get<CountRow>('SELECT COUNT(*) as count FROM tickers');
    return Math.max(0, toInteger(row?.count, 0));
  }

  async calculateMaxFluctuationRatio(symbol: string): Promise<number> {
    try {
      const rows = await this.db.all<CloseRow>(
        `
          SELECT close
          FROM candles
          WHERE ticker = ?
          ORDER BY date ASC
        `,
        [symbol]
      );

      if (rows.length < 2) {
        return 0;
      }

      let maxFluctuation = 0;
      for (let i = 1; i < rows.length; i += 1) {
        const prevClose = toNullableNumber(rows[i - 1].close) ?? 0;
        const currentClose = toNullableNumber(rows[i].close) ?? 0;
        if (!prevClose) {
          continue;
        }
        const fluctuation = Math.abs(currentClose - prevClose) / prevClose;
        if (Number.isFinite(fluctuation)) {
          maxFluctuation = Math.max(maxFluctuation, fluctuation);
        }
      }

      return maxFluctuation;
    } catch (error) {
      console.error(`Error calculating max fluctuation ratio for ${symbol}:`, error);
      return 0;
    }
  }

  async updateMaxFluctuationRatio(symbol: string): Promise<void> {
    try {
      const maxFluctuation = await this.calculateMaxFluctuationRatio(symbol);

      await this.db.run(
        `
          UPDATE tickers
          SET max_fluctuation_ratio = ?, last_updated = ?
          WHERE symbol = ?
        `,
        [maxFluctuation, new Date(), symbol]
      );

      this.invalidateCache();
    } catch (error) {
      console.error(`Failed to update max fluctuation ratio for ${symbol}:`, error);
      throw error;
    }
  }

  async deleteTicker(symbol: string): Promise<void> {
    try {
      await this.db.run('DELETE FROM candles WHERE ticker = ?', [symbol]);
      await this.db.run('DELETE FROM trades WHERE ticker = ?', [symbol]);
      await this.db.run('DELETE FROM signals WHERE ticker = ?', [symbol]);
      await this.db.run('DELETE FROM tickers WHERE symbol = ?', [symbol]);
      this.invalidateCache();
    } catch (error) {
      console.error(`Failed to delete ticker ${symbol}:`, error);
      throw error;
    }
  }

  async bulkInsertTickers(tickers: string[], options?: { training?: boolean }): Promise<void> {
    const normalized = tickers
      .map((symbol) => symbol.trim().toUpperCase())
      .filter((symbol) => symbol.length > 0);

    if (normalized.length === 0) {
      return;
    }

    const now = new Date();
    const trainingFlag = options?.training ?? true;
    const valuePlaceholders = normalized.map(() => '(?, ?, ?)').join(', ');
    const params: QueryValue[] = [];
    for (const symbol of normalized) {
      params.push(symbol, now, trainingFlag);
    }

    await this.db.run(
      `
        INSERT INTO tickers (symbol, last_updated, training)
        VALUES ${valuePlaceholders}
        ON CONFLICT (symbol) DO NOTHING
      `,
      params
    );

    this.invalidateCache();
  }

  async bulkDeleteTickers(tickers: string[]): Promise<void> {
    if (tickers.length === 0) {
      return;
    }

    for (const symbol of tickers) {
      await this.deleteTicker(symbol.trim().toUpperCase());
    }
  }

  async syncTickersFromAssets(assets: TickerAssetRecord[]): Promise<{ upserted: number; disabled: number }> {
    if (!assets.length) {
      return { upserted: 0, disabled: 0 };
    }

    return this.db.withTransaction(async (client) => {
      const chunkSize = 500;
      let upserted = 0;

      for (let i = 0; i < assets.length; i += chunkSize) {
        const chunk = assets.slice(i, i + chunkSize);
        if (chunk.length === 0) {
          continue;
        }

        const placeholders: string[] = [];
        const params: QueryValue[] = [];

        for (const asset of chunk) {
          placeholders.push('(?, ?, ?, ?, ?, ?, ?, ?)');
          params.push(
            asset.symbol.trim().toUpperCase(),
            asset.name,
            asset.tradable,
            asset.shortable,
            asset.easyToBorrow,
            asset.assetType,
            asset.expenseRatio,
            asset.training
          );
        }

        const result = await this.db.run(
          `
            INSERT INTO tickers (symbol, name, tradable, shortable, easy_to_borrow, asset_type, expense_ratio, training)
            VALUES ${placeholders.join(', ')}
            ON CONFLICT (symbol) DO UPDATE
            SET name = EXCLUDED.name,
                tradable = EXCLUDED.tradable,
                shortable = EXCLUDED.shortable,
                easy_to_borrow = EXCLUDED.easy_to_borrow,
                asset_type = EXCLUDED.asset_type,
                expense_ratio = EXCLUDED.expense_ratio,
                training = EXCLUDED.training
          `,
          params,
          client
        );

        upserted += result.rowCount;
      }

      const symbols = assets.map((asset) => asset.symbol.trim().toUpperCase());
      let disabled = 0;
      if (symbols.length > 0) {
        const disableResult = await this.db.run(
          `
            UPDATE tickers
            SET tradable = FALSE,
                shortable = FALSE,
                easy_to_borrow = FALSE
            WHERE NOT (symbol = ANY(?))
          `,
          [symbols],
          client
        );
        disabled = disableResult.rowCount;
      }

      this.invalidateCache();

      return { upserted, disabled };
    });
  }

  async clearAllTickers(): Promise<{
    tickersDeleted: number;
    candlesDeleted: number;
    tradesDeleted: number;
    signalsDeleted: number;
  }> {
    try {
      return await this.db.withTransaction(async (client) => {
        await this.db.exec('LOCK TABLE candles, trades, signals, tickers IN ACCESS EXCLUSIVE MODE', client);

        const [tickerCountRow, candleCountRow, tradeCountRow, signalCountRow] = await Promise.all([
          this.db.get<CountRow>('SELECT COUNT(*) AS count FROM tickers', [], client),
          this.db.get<CountRow>('SELECT COUNT(*) AS count FROM candles', [], client),
          this.db.get<CountRow>('SELECT COUNT(*) AS count FROM trades', [], client),
          this.db.get<CountRow>('SELECT COUNT(*) AS count FROM signals', [], client)
        ]);

        await this.db.exec('TRUNCATE TABLE candles, trades, signals, tickers RESTART IDENTITY CASCADE', client);

        this.invalidateCache();

        return {
          tickersDeleted: toInteger(tickerCountRow?.count, 0),
          candlesDeleted: toInteger(candleCountRow?.count, 0),
          tradesDeleted: toInteger(tradeCountRow?.count, 0),
          signalsDeleted: toInteger(signalCountRow?.count, 0)
        };
      });
    } catch (error) {
      console.error('Failed to clear all tickers:', error);
      throw error;
    }
  }

  async deleteTickersByIds(tickerSymbols: string[], client: PoolClient): Promise<number> {
    if (tickerSymbols.length === 0) {
      return 0;
    }
    const placeholders = tickerSymbols.map(() => '?').join(', ');
    const result = await this.db.run(`DELETE FROM tickers WHERE symbol IN (${placeholders})`, tickerSymbols, client);
    this.invalidateCache();
    return result.rowCount;
  }
}
