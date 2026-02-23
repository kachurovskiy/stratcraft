import { Database } from '../database/Database';
import { Candle, TickerInfo } from '../../shared/types/StrategyTemplate';
import { LoggingService } from '../services/LoggingService';
import { SETTING_KEYS } from '../constants';
import { CandleSource } from './candleSources/CandleSource';
import { AlpacaCandleSource } from './candleSources/AlpacaCandleSource';
import { EodhdCandleSource } from './candleSources/EodhdCandleSource';
import { TiingoCandleSource } from './candleSources/TiingoCandleSource';

export class CandleClient {
  private db: Database;
  private loggingService: LoggingService;
  private candleSource: CandleSource | null;
  private candleSourceName: string | null;
  private tickersWithNoData: Set<string> = new Set();
  private candleMismatchThreshold = 0;

  constructor(database: Database, loggingService: LoggingService) {
    this.db = database;
    this.loggingService = loggingService;
    this.candleSource = null;
    this.candleSourceName = null;
  }

  private async getHistoricalData(
    symbol: string,
    startDate: Date,
    endDate: Date
  ): Promise<Candle[]> {
    const candleSource = await this.getCandleSource();
    const { candles, noData } = await candleSource.getHistoricalCandles(symbol, startDate, endDate);
    if (noData) {
      this.tickersWithNoData.add(symbol);
    }
    return candles;
  }

  async updateTickerData(symbol: string, includeToday = false): Promise<Candle[]> {
    const endDate = this.getEndDate(includeToday);

    const latestDate = await this.db.candles.getLatestCandleDate(symbol);
    const normalizedLatest = latestDate ? this.startOfDay(new Date(latestDate)) : null;
    if (normalizedLatest) {

      if (normalizedLatest >= endDate) {
        return [];
      }

      const missingStart = new Date(normalizedLatest);
      missingStart.setDate(missingStart.getDate() + 1);
      if (this.areAllWeekendDays(missingStart, endDate)) {
        return [];
      }
    }

    const fullHistoryStart = this.subtractYears(endDate, 11);

    if (!normalizedLatest) {
      return this.reloadFullHistory(symbol, fullHistoryStart, endDate);
    }

    const candles = await this.getHistoricalData(symbol, normalizedLatest, endDate);
    if (candles.length === 0) {
      return [];
    }

    const existingLastCandle = await this.db.candles.getLastCandle(symbol);
    const fetchedLastCandle = this.findCandleByDate(candles, normalizedLatest);
    this.candleMismatchThreshold = Number(await this.db.settings.getSettingValue(SETTING_KEYS.CANDLE_MISMATCH_THRESHOLD));
    if (
      existingLastCandle &&
      fetchedLastCandle &&
      this.formatDate(existingLastCandle.date) === this.formatDate(normalizedLatest) &&
      this.hasSignificantCandleMismatch(existingLastCandle, fetchedLastCandle)
    ) {
      this.loggingService.warn('candle-job', 'Candle mismatch detected; reloading full history', {
        symbol,
        date: this.formatDate(normalizedLatest)
      });

      return this.reloadFullHistory(symbol, fullHistoryStart, endDate);
    }

    await this.db.candles.upsertCandlesForTicker(symbol, candles);
    this.db.tickers.invalidateCache();
    await this.updateTickerVolumeFromLastCandle(symbol);
    return candles;
  }

  private findCandleByDate(candles: Candle[], date: Date): Candle | null {
    const target = this.formatDate(date);
    return candles.find(candle => this.formatDate(candle.date) === target) ?? null;
  }

  private async reloadFullHistory(symbol: string, startDate: Date, endDate: Date): Promise<Candle[]> {
    const candles = await this.getHistoricalData(symbol, startDate, endDate);
    if (candles.length === 0) {
      return [];
    }

    await this.db.candles.replaceCandlesForTicker(symbol, candles);
    this.db.tickers.invalidateCache();
    await this.updateTickerVolumeFromLastCandle(symbol);
    return candles;
  }

  private hasSignificantCandleMismatch(existing: Candle, incoming: Candle): boolean {
    return (
      this.exceedsRelativeThreshold(existing.open, incoming.open) ||
      this.exceedsRelativeThreshold(existing.high, incoming.high) ||
      this.exceedsRelativeThreshold(existing.low, incoming.low) ||
      this.exceedsRelativeThreshold(existing.close, incoming.close) ||
      this.exceedsRelativeThreshold(existing.volumeShares, incoming.volumeShares)
    );
  }

  private exceedsRelativeThreshold(existing: number, incoming: number): boolean {
    if (!Number.isFinite(existing) || !Number.isFinite(incoming)) {
      return false;
    }

    if (existing === 0) {
      return incoming !== 0;
    }

    return Math.abs(incoming - existing) / Math.abs(existing) > this.candleMismatchThreshold;
  }

  private startOfDay(date: Date): Date {
    const normalized = new Date(date);
    normalized.setHours(0, 0, 0, 0);
    return normalized;
  }

  private formatDate(date: Date): string {
    return this.startOfDay(date).toISOString().split('T')[0];
  }

  private subtractYears(date: Date, years: number): Date {
    const copy = new Date(date);
    copy.setFullYear(copy.getFullYear() - years);
    return copy;
  }

  private getEndDate(includeToday: boolean): Date {
    const endDate = new Date();
    if (!includeToday) {
      endDate.setDate(endDate.getDate() - 1); // Don't include today (market might not be closed)
    }
    return this.startOfDay(endDate);
  }

  private areAllWeekendDays(startDate: Date, endDate: Date): boolean {
    const normalizedStart = this.startOfDay(startDate);
    const normalizedEnd = this.startOfDay(endDate);

    const [start, end] =
      normalizedStart <= normalizedEnd
        ? [normalizedStart, normalizedEnd]
        : [normalizedEnd, normalizedStart];

    for (let current = new Date(start); current <= end; current.setDate(current.getDate() + 1)) {
      if (!this.isWeekend(current)) {
        return false;
      }
    }

    return true;
  }

  private isWeekend(date: Date): boolean {
    const day = date.getDay();
    return day === 0 || day === 6;
  }

  private async updateTickerVolumeFromLastCandle(symbol: string): Promise<void> {
    try {
      // Get the latest candle for this ticker
      const latestCandle = await this.db.candles.getLastCandle(symbol);

      if (latestCandle) {
        const volumeUsd = latestCandle.close * latestCandle.volumeShares;

        // Get existing ticker info and update volumeUsd
        const existingTicker = await this.db.tickers.getTicker(symbol);
        if (existingTicker) {
          const updatedTicker: TickerInfo = {
            ...existingTicker,
            volumeUsd: volumeUsd,
            lastUpdated: new Date()
          };
          await this.db.tickers.upsertTicker(updatedTicker);
        }
      }

      // Update the max fluctuation ratio after inserting new candles
      await this.db.tickers.updateMaxFluctuationRatio(symbol);
    } catch (error) {
      console.warn(`Failed to update volumeUsd and fluctuation ratio for ${symbol}:`, error);
    }
  }

  public drainNoDataTickers(): string[] {
    const tickers = Array.from(this.tickersWithNoData);
    this.tickersWithNoData.clear();
    return tickers;
  }

  public getCandleSourceName(): string {
    return this.candleSource?.getSourceName() ?? 'EODHD';
  }

  private async getCandleSource(): Promise<CandleSource> {
    const providerRaw = await this.db.settings.getSettingValue(SETTING_KEYS.CANDLE_DATA_PROVIDER);
    const normalizedProvider = typeof providerRaw === 'string' ? providerRaw.trim().toUpperCase() : '';
    const provider =
      normalizedProvider === 'TIINGO'
        ? 'TIINGO'
        : normalizedProvider === 'ALPACA'
          ? 'ALPACA'
          : 'EODHD';

    if (this.candleSource && this.candleSourceName === provider) {
      return this.candleSource;
    }

    if (provider === 'ALPACA') {
      this.candleSource = new AlpacaCandleSource(this.db, this.loggingService);
    } else if (provider === 'TIINGO') {
      this.candleSource = new TiingoCandleSource(this.db, this.loggingService);
    } else {
      if (
        normalizedProvider &&
        normalizedProvider !== 'EODHD' &&
        normalizedProvider !== 'TIINGO' &&
        normalizedProvider !== 'ALPACA'
      ) {
        this.loggingService.warn('candle-job', 'Unknown candle data provider; defaulting to EODHD', {
          provider: normalizedProvider
        });
      }
      this.candleSource = new EodhdCandleSource(this.db, this.loggingService);
    }

    this.candleSourceName = provider;
    return this.candleSource;
  }
}
