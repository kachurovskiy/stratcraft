import { Database } from '../database/Database';
import { Candle, TickerInfo } from '../types/StrategyTemplate';
import { LoggingService } from '../services/LoggingService';
import { SETTING_KEYS } from '../constants';
import { parseOptionalNumberSetting } from '../utils/settings';
import { normalizeUppercaseString } from '../utils/stringNormalization';
import { CandleSource } from './candleSources/CandleSource';
import { AlpacaCandleSource } from './candleSources/AlpacaCandleSource';
import { EodhdCandleSource } from './candleSources/EodhdCandleSource';
import { TiingoCandleSource } from './candleSources/TiingoCandleSource';

const PRICE_EPSILON = 1e-6;

type CandleDisableSettings = {
  tradeEntryPriceMin: number;
  tradeEntryPriceMax: number;
  minimumDollarVolumeForEntry: number;
  minimumDollarVolumeLookback: number;
  minFluctuationRatio: number;
  maxFluctuationRatio: number;
};

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
    endDate: Date,
    abortSignal?: AbortSignal
  ): Promise<Candle[]> {
    const candleSource = await this.getCandleSource();
    const { candles, noData } = await candleSource.getHistoricalCandles(symbol, startDate, endDate, abortSignal);
    if (noData) {
      this.tickersWithNoData.add(symbol);
    }
    return this.removeWeekendCandles(candles);
  }

  async updateTickerData(symbol: string, includeToday = false, abortSignal?: AbortSignal): Promise<Candle[]> {
    if (abortSignal?.aborted) {
      throw new Error('Candle synchronization cancelled');
    }
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
      return this.reloadFullHistory(symbol, fullHistoryStart, endDate, abortSignal);
    }

    const candles = await this.getHistoricalData(symbol, normalizedLatest, endDate, abortSignal);
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

      return this.reloadFullHistory(symbol, fullHistoryStart, endDate, abortSignal);
    }

    const candlesWithDisabled = await this.applyDisabledFlags(symbol, candles, true);
    await this.db.candles.upsertCandlesForTicker(symbol, candlesWithDisabled);
    this.db.tickers.invalidateCache();
    await this.updateTickerVolumeFromLastCandle(symbol);
    return candlesWithDisabled;
  }

  private findCandleByDate(candles: Candle[], date: Date): Candle | null {
    const target = this.formatDate(date);
    return candles.find(candle => this.formatDate(candle.date) === target) ?? null;
  }

  private async reloadFullHistory(
    symbol: string,
    startDate: Date,
    endDate: Date,
    abortSignal?: AbortSignal
  ): Promise<Candle[]> {
    const candles = await this.getHistoricalData(symbol, startDate, endDate, abortSignal);
    if (candles.length === 0) {
      return [];
    }

    const candlesWithDisabled = await this.applyDisabledFlags(symbol, candles, false);
    await this.db.candles.replaceCandlesForTicker(symbol, candlesWithDisabled);
    this.db.tickers.invalidateCache();
    await this.updateTickerVolumeFromLastCandle(symbol);
    return candlesWithDisabled;
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

  private removeWeekendCandles(candles: Candle[]): Candle[] {
    return candles.filter(candle => !this.isWeekend(candle.date));
  }

  private async applyDisabledFlags(symbol: string, candles: Candle[], useExistingHistory: boolean): Promise<Candle[]> {
    if (candles.length === 0) {
      return candles;
    }

    const sorted = [...candles].sort((a, b) => a.date.getTime() - b.date.getTime());
    const settings = await this.loadCandleDisableSettings();
    const maxWindow = Math.max(0, settings.minimumDollarVolumeLookback - 1);
    const historyLimit = useExistingHistory ? Math.max(1, maxWindow + 1) : 0;
    const firstCandleDate = sorted[0].date;
    const [history, maxFluctuationBefore] = await Promise.all([
      historyLimit > 0 ? this.db.candles.getRecentCandles(symbol, historyLimit) : Promise.resolve([]),
      useExistingHistory ? this.db.candles.getMaxFluctuationBeforeDate(symbol, firstCandleDate) : Promise.resolve(0)
    ]);
    let maxFluctuation = Number.isFinite(maxFluctuationBefore) ? maxFluctuationBefore : 0;

    const firstCandleTime = firstCandleDate.getTime();
    const filteredHistory = history.filter((candle) => candle.date.getTime() < firstCandleTime);
    let lastClose = filteredHistory.length > 0 ? filteredHistory[filteredHistory.length - 1].close : null;
    if (lastClose === null && useExistingHistory) {
      const lastCandle = await this.db.candles.getLastCandleBeforeDate(symbol, firstCandleDate);
      lastClose = lastCandle?.close ?? null;
    }
    const volumeWindow = filteredHistory.map((candle) => this.calculateDollarVolume(candle));
    if (volumeWindow.length > maxWindow) {
      volumeWindow.splice(0, volumeWindow.length - maxWindow);
    }

    const results: Candle[] = [];
    for (const candle of sorted) {
      const price = this.resolveUnadjustedPrice(candle);
      const maxFluctuationBefore = maxFluctuation;
      const volumeUsd = this.calculateDollarVolume(candle);
      let maxFluctuationAfter = maxFluctuationBefore;

      if (lastClose !== null && Number.isFinite(lastClose) && Number.isFinite(candle.close) && lastClose !== 0) {
        const ratio = Math.abs(candle.close - lastClose) / Math.abs(lastClose);
        if (Number.isFinite(ratio)) {
          maxFluctuationAfter = Math.max(maxFluctuationBefore, ratio);
        }
      }

      let disabled: string | null = null;
      if (!this.isFluctuationWithinBounds(maxFluctuationAfter, settings)) {
        disabled = 'f';
      } else if (!this.isPriceWithinBounds(price, settings)) {
        disabled = 'p';
      } else if (!this.hasMinimumDollarVolume(volumeWindow, volumeUsd, settings)) {
        disabled = 'v';
      }

      results.push({ ...candle, disabled });

      if (settings.minimumDollarVolumeLookback > 0) {
        volumeWindow.push(volumeUsd);
        if (volumeWindow.length > maxWindow) {
          volumeWindow.splice(0, volumeWindow.length - maxWindow);
        }
      }

      maxFluctuation = maxFluctuationAfter;
      lastClose = candle.close;
    }

    return results;
  }

  private async loadCandleDisableSettings(): Promise<CandleDisableSettings> {
    const settings = await this.db.settings.getSettingsByKeys([
      SETTING_KEYS.TRADE_ENTRY_PRICE_MIN,
      SETTING_KEYS.TRADE_ENTRY_PRICE_MAX,
      SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_FOR_ENTRY,
      SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_LOOKBACK,
      SETTING_KEYS.MIN_TICKER_FLUCTUATION_RATIO,
      SETTING_KEYS.MAX_TICKER_FLUCTUATION_RATIO
    ]);

    const parseSetting = (settingKey: string, fallback: number) =>
      parseOptionalNumberSetting(settings[settingKey]) ?? fallback;

    const tradeEntryPriceMin = Math.max(0, parseSetting(SETTING_KEYS.TRADE_ENTRY_PRICE_MIN, 0));
    let tradeEntryPriceMax = parseSetting(SETTING_KEYS.TRADE_ENTRY_PRICE_MAX, Number.POSITIVE_INFINITY);
    if (!Number.isFinite(tradeEntryPriceMax) || tradeEntryPriceMax <= 0) {
      tradeEntryPriceMax = Number.POSITIVE_INFINITY;
    }
    if (tradeEntryPriceMax < tradeEntryPriceMin) {
      tradeEntryPriceMax = tradeEntryPriceMin;
    }

    const minimumDollarVolumeForEntry = Math.max(0, parseSetting(SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_FOR_ENTRY, 0));
    const minimumDollarVolumeLookback = Math.max(
      0,
      Math.floor(parseSetting(SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_LOOKBACK, 0))
    );

    const minFluctuationRatio = Math.max(0, parseSetting(SETTING_KEYS.MIN_TICKER_FLUCTUATION_RATIO, 0));
    let maxFluctuationRatio = parseSetting(SETTING_KEYS.MAX_TICKER_FLUCTUATION_RATIO, Number.POSITIVE_INFINITY);
    if (!Number.isFinite(maxFluctuationRatio) || maxFluctuationRatio <= 0) {
      maxFluctuationRatio = Number.POSITIVE_INFINITY;
    }
    if (maxFluctuationRatio < minFluctuationRatio) {
      maxFluctuationRatio = minFluctuationRatio;
    }

    return {
      tradeEntryPriceMin,
      tradeEntryPriceMax,
      minimumDollarVolumeForEntry,
      minimumDollarVolumeLookback,
      minFluctuationRatio,
      maxFluctuationRatio
    };
  }

  private resolveUnadjustedPrice(candle: Candle): number {
    const price = candle.unadjustedClose ?? candle.close;
    return Number.isFinite(price) ? price : Number.NaN;
  }

  private isPriceWithinBounds(price: number, settings: CandleDisableSettings): boolean {
    if (!Number.isFinite(price)) {
      return false;
    }
    return price >= settings.tradeEntryPriceMin && price <= settings.tradeEntryPriceMax;
  }

  private calculateDollarVolume(candle: Candle): number {
    const high = Number.isFinite(candle.high) ? candle.high : 0;
    const volume = Number.isFinite(candle.volumeShares) ? candle.volumeShares : 0;
    const usdVolume = high * volume;
    return Number.isFinite(usdVolume) ? usdVolume : 0;
  }

  private hasMinimumDollarVolume(
    volumeWindow: number[],
    currentUsdVolume: number,
    settings: CandleDisableSettings
  ): boolean {
    if (settings.minimumDollarVolumeForEntry <= 0 || settings.minimumDollarVolumeLookback <= 0) {
      return true;
    }
    if (volumeWindow.length + 1 < settings.minimumDollarVolumeLookback) {
      return false;
    }
    const startIndex = volumeWindow.length + 1 - settings.minimumDollarVolumeLookback;
    for (let idx = Math.max(0, startIndex); idx < volumeWindow.length; idx += 1) {
      if (volumeWindow[idx] + PRICE_EPSILON < settings.minimumDollarVolumeForEntry) {
        return false;
      }
    }
    return currentUsdVolume + PRICE_EPSILON >= settings.minimumDollarVolumeForEntry;
  }

  private isFluctuationWithinBounds(maxFluctuation: number, settings: CandleDisableSettings): boolean {
    if (!Number.isFinite(maxFluctuation)) {
      return false;
    }
    return maxFluctuation >= settings.minFluctuationRatio && maxFluctuation <= settings.maxFluctuationRatio;
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
    const normalizedProvider = normalizeUppercaseString(providerRaw);
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
