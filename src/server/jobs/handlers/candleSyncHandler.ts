import { createHash } from 'crypto';
import { JobHandler, JobHandlerContext } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';
import type { TickerAssetRecord, TickerAssetType } from '../../database/types';
import { SETTING_KEYS } from '../../constants';

const CANDLE_SOURCE = 'candle-job';

type LeveragedExpenseRatios = Record<2 | 3 | 5, number>;

type CandleSyncSettings = {
  maxConcurrentUpdates: number;
  etfBaseExpenseRatio: number;
  inverseEtfExpenseRatio: number;
  commodityTrustExpenseRatio: number;
  bondEtfExpenseRatio: number;
  incomeEtfExpenseRatio: number;
  leveragedExpenseRatios: LeveragedExpenseRatios;
  trainingAllocationRatio: number;
  matchingRatioThreshold: number;
};

function parseRequiredNumberSetting(
  settingKey: string,
  rawValue: string,
  options: { min?: number; max?: number; integer?: boolean } = {}
): number {
  const value = Number(rawValue);
  if (!Number.isFinite(value)) {
    throw new Error(`Setting "${settingKey}" must be a valid number.`);
  }
  if (options.integer && !Number.isInteger(value)) {
    throw new Error(`Setting "${settingKey}" must be an integer.`);
  }
  if (options.min !== undefined && value < options.min) {
    throw new Error(`Setting "${settingKey}" must be at least ${options.min}.`);
  }
  if (options.max !== undefined && value > options.max) {
    throw new Error(`Setting "${settingKey}" must be at most ${options.max}.`);
  }
  return value;
}

async function loadCandleSyncSettings(db: JobHandlerDependencies['db']): Promise<CandleSyncSettings> {
  const [
    maxConcurrentRaw,
    etfBaseRaw,
    inverseEtfRaw,
    commodityTrustRaw,
    bondEtfRaw,
    incomeEtfRaw,
    leveraged2xRaw,
    leveraged3xRaw,
    leveraged5xRaw,
    trainingAllocationRaw,
    matchingRatioRaw
  ] = await Promise.all([
    db.settings.getRequiredSettingValue(SETTING_KEYS.CANDLE_SYNC_MAX_CONCURRENT_UPDATES),
    db.settings.getRequiredSettingValue(SETTING_KEYS.ETF_BASE_EXPENSE_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.INVERSE_ETF_EXPENSE_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.COMMODITY_TRUST_EXPENSE_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.BOND_ETF_EXPENSE_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.INCOME_ETF_EXPENSE_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.LEVERAGED_2X_EXPENSE_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.LEVERAGED_3X_EXPENSE_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.LEVERAGED_5X_EXPENSE_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.TRAINING_ALLOCATION_RATIO),
    db.settings.getRequiredSettingValue(SETTING_KEYS.CANDLE_SYNC_MATCHING_RATIO_THRESHOLD)
  ]);

  return {
    maxConcurrentUpdates: parseRequiredNumberSetting(
      SETTING_KEYS.CANDLE_SYNC_MAX_CONCURRENT_UPDATES,
      maxConcurrentRaw,
      { min: 1, integer: true }
    ),
    etfBaseExpenseRatio: parseRequiredNumberSetting(SETTING_KEYS.ETF_BASE_EXPENSE_RATIO, etfBaseRaw, { min: 0 }),
    inverseEtfExpenseRatio: parseRequiredNumberSetting(SETTING_KEYS.INVERSE_ETF_EXPENSE_RATIO, inverseEtfRaw, { min: 0 }),
    commodityTrustExpenseRatio: parseRequiredNumberSetting(SETTING_KEYS.COMMODITY_TRUST_EXPENSE_RATIO, commodityTrustRaw, { min: 0 }),
    bondEtfExpenseRatio: parseRequiredNumberSetting(SETTING_KEYS.BOND_ETF_EXPENSE_RATIO, bondEtfRaw, { min: 0 }),
    incomeEtfExpenseRatio: parseRequiredNumberSetting(SETTING_KEYS.INCOME_ETF_EXPENSE_RATIO, incomeEtfRaw, { min: 0 }),
    leveragedExpenseRatios: {
      2: parseRequiredNumberSetting(SETTING_KEYS.LEVERAGED_2X_EXPENSE_RATIO, leveraged2xRaw, { min: 0 }),
      3: parseRequiredNumberSetting(SETTING_KEYS.LEVERAGED_3X_EXPENSE_RATIO, leveraged3xRaw, { min: 0 }),
      5: parseRequiredNumberSetting(SETTING_KEYS.LEVERAGED_5X_EXPENSE_RATIO, leveraged5xRaw, { min: 0 })
    },
    trainingAllocationRatio: parseRequiredNumberSetting(SETTING_KEYS.TRAINING_ALLOCATION_RATIO, trainingAllocationRaw, {
      min: 0,
      max: 1
    }),
    matchingRatioThreshold: parseRequiredNumberSetting(
      SETTING_KEYS.CANDLE_SYNC_MATCHING_RATIO_THRESHOLD,
      matchingRatioRaw,
      { min: 0, max: 1 }
    )
  };
}

function normalizeAssetName(name?: string | null): string {
  if (typeof name !== 'string') {
    return '';
  }
  return name.trim().toUpperCase();
}

type NumericLeverageDetection = { multiplier: 2 | 3 | 5; isInverse: boolean };

function detectNumericLeverage(normalizedName: string): NumericLeverageDetection | null {
  if (!normalizedName) {
    return null;
  }
  const match = normalizedName.match(/([+-]?)([235])\s*X\b/);
  if (match) {
    const sign = match[1] || '';
    const value = Number(match[2]);
    if (value === 2 || value === 3 || value === 5) {
      return {
        multiplier: value as 2 | 3 | 5,
        isInverse: sign.trim() === '-'
      };
    }
  }
  return null;
}

function isEtfLike(normalizedName: string): boolean {
  if (!normalizedName) {
    return false;
  }
  if (
    normalizedName.includes('ETF') ||
    normalizedName.includes('ETN') ||
    normalizedName.includes('EXCHANGE TRADED')
  ) {
    return true;
  }
  const hasFund = normalizedName.includes(' FUND');
  const hasIndex = normalizedName.includes(' INDEX');
  const hasTrust = normalizedName.includes(' TRUST');
  const hasSeries = normalizedName.includes(' SERIES');
  if (hasTrust && (hasSeries || normalizedName.includes(' ETF'))) {
    return true;
  }
  if (hasFund && hasIndex) {
    return true;
  }
  return false;
}

function detectTextualLeverage(normalizedName: string): 2 | 3 | 5 | null {
  if (!normalizedName) {
    return null;
  }
  if (normalizedName.includes('ULTRAPRO')) {
    return 3;
  }
  if (normalizedName.includes('PROSHARES') && normalizedName.includes('ULTRA')) {
    return 2;
  }
  if (normalizedName.includes('TRIPLE')) {
    return 3;
  }
  return null;
}

function toLeverageAssetType(multiplier: 2 | 3 | 5): TickerAssetType {
  switch (multiplier) {
    case 2:
      return 'leveraged_2x';
    case 3:
      return 'leveraged_3x';
    case 5:
      return 'leveraged_5x';
    default:
      return 'equity';
  }
}

function toInverseLeverageAssetType(multiplier: 2 | 3 | 5): TickerAssetType {
  switch (multiplier) {
    case 2:
      return 'inverse_leveraged_2x';
    case 3:
      return 'inverse_leveraged_3x';
    case 5:
      return 'inverse_leveraged_5x';
    default:
      return 'inverse_etf';
  }
}

const METAL_KEYWORDS = ['GOLD', 'SILVER', 'PLATINUM', 'PALLADIUM', 'PRECIOUS', 'METALS', 'COPPER'];
const BOND_KEYWORDS = ['BOND', 'TREASURY', 'MUNICIPAL', 'MUNI', 'CORPORATE', 'FIXED INCOME', 'AGGREGATE'];
const INCOME_ETF_KEYWORDS = [
  'BUYWRITE',
  'BUY-WRITE',
  'COVERED CALL',
  'COVEREDCALL',
  'ENHANCED INCOME',
  'PREMIUM INCOME',
  'INCOME BUILDER',
  'INCOME ETF',
  'YIELD ENHANCED'
];

function hasInverseKeywords(normalizedName: string): boolean {
  if (!normalizedName) {
    return false;
  }
  if (normalizedName.includes('INVERSE') || normalizedName.includes(' BEAR') || normalizedName.includes('ULTRASHORT')) {
    return true;
  }
  const standaloneShort = /\bSHORT\b/.test(normalizedName);
  if (standaloneShort) {
    if (/\bSHORT[-\s]+TERM\b/.test(normalizedName) || /\bSHORT[-\s]+DURATION\b/.test(normalizedName)) {
      return false;
    }
    return true;
  }
  if (normalizedName.includes('-1X') || normalizedName.includes('(-1X')) {
    return true;
  }
  return false;
}

function isCommodityTrustName(normalizedName: string): boolean {
  if (!normalizedName.includes('TRUST')) {
    return false;
  }
  if (normalizedName.includes('PHYSICAL')) {
    return true;
  }
  return METAL_KEYWORDS.some((keyword) => normalizedName.includes(keyword));
}

function isBondEtfName(normalizedName: string): boolean {
  return BOND_KEYWORDS.some((keyword) => normalizedName.includes(keyword));
}

function isIncomeEtfName(normalizedName: string): boolean {
  return INCOME_ETF_KEYWORDS.some((keyword) => normalizedName.includes(keyword));
}

function classifyAssetFromName(
  name: string | null | undefined,
  settings: CandleSyncSettings
): { assetType: TickerAssetType; expenseRatio: number | null } {
  const normalized = normalizeAssetName(name);
  const inverseKeywords = hasInverseKeywords(normalized);
  const numericLeverage = detectNumericLeverage(normalized);
  if (numericLeverage) {
    const assetType = inverseKeywords || numericLeverage.isInverse
      ? toInverseLeverageAssetType(numericLeverage.multiplier)
      : toLeverageAssetType(numericLeverage.multiplier);
    return {
      assetType,
      expenseRatio: settings.leveragedExpenseRatios[numericLeverage.multiplier]
    };
  }

  if (isCommodityTrustName(normalized)) {
    return {
      assetType: 'commodity_trust',
      expenseRatio: settings.commodityTrustExpenseRatio
    };
  }

  if (isEtfLike(normalized)) {
    const textualLeverage = detectTextualLeverage(normalized);
    if (textualLeverage) {
      return {
        assetType: inverseKeywords ? toInverseLeverageAssetType(textualLeverage) : toLeverageAssetType(textualLeverage),
        expenseRatio: settings.leveragedExpenseRatios[textualLeverage]
      };
    }
    if (inverseKeywords) {
      return {
        assetType: 'inverse_etf',
        expenseRatio: settings.inverseEtfExpenseRatio
      };
    }
    if (isBondEtfName(normalized)) {
      return {
        assetType: 'bond_etf',
        expenseRatio: settings.bondEtfExpenseRatio
      };
    }
    if (isIncomeEtfName(normalized)) {
      return {
        assetType: 'income_etf',
        expenseRatio: settings.incomeEtfExpenseRatio
      };
    }
    return {
      assetType: 'etf',
      expenseRatio: settings.etfBaseExpenseRatio
    };
  }

  return {
    assetType: 'equity',
    expenseRatio: null
  };
}

export function createCandleSyncHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    const [alwaysValidationTickersRaw, candleSyncSettings, autoDailyCandleSyncRaw] = await Promise.all([
      deps.db.settings.getSettingArray(SETTING_KEYS.ALWAYS_VALIDATION_TICKERS),
      loadCandleSyncSettings(deps.db),
      deps.db.settings.getSettingValue(SETTING_KEYS.AUTO_DAILY_CANDLE_SYNC_ENABLED),
    ]);
    const alwaysValidationTickers = new Set(alwaysValidationTickersRaw);
    const autoDailyCandleSyncEnabled = autoDailyCandleSyncRaw === 'true';
    const marketClock = await resolveMarketClock(ctx, deps);
    if (marketClock.isOpen) {
      const hasExistingCandles = !!(await deps.db.candles.getLatestGlobalCandleDate());
      if (!hasExistingCandles) {
        ctx.loggingService.info(CANDLE_SOURCE, 'Market open detected but no candles exist; continuing sync', {
          ...logMetadata,
          marketClockSource: marketClock.source,
          timestamp: marketClock.timestamp?.toISOString() ?? null,
          nextOpen: marketClock.nextOpen?.toISOString() ?? null,
          nextClose: marketClock.nextClose?.toISOString() ?? null
        });
      } else {
        ctx.loggingService.info(CANDLE_SOURCE, 'Skipping candle sync while market is open', {
          ...logMetadata,
          marketClockSource: marketClock.source,
          timestamp: marketClock.timestamp?.toISOString() ?? null,
          nextOpen: marketClock.nextOpen?.toISOString() ?? null,
          nextClose: marketClock.nextClose?.toISOString() ?? null
        });
        await scheduleNext(deps, ctx, autoDailyCandleSyncEnabled, logMetadata);
        return { message: 'Candle sync skipped while market is open' };
      }
    }

    let tickers = await deps.db.tickers.getTickers();
    if (!tickers.length) {
      const seeded = await refreshTickersFromAlpaca(
        ctx,
        deps,
        'database-empty',
        alwaysValidationTickers,
        candleSyncSettings
      );
      if (seeded) {
        tickers = await deps.db.tickers.getTickers();
      }
    }

    if (!tickers.length) {
      ctx.loggingService.warn(CANDLE_SOURCE, 'No tickers available for candle sync after Alpaca refresh', logMetadata);
      await scheduleNext(deps, ctx, autoDailyCandleSyncEnabled, logMetadata);
      return {
        message: 'No tickers found for synchronization'
      };
    }

    let symbols = buildSymbolList(tickers);
    let totalTickers = symbols.length;
    const updatedTickers = new Set<string>();

    ctx.loggingService.info(CANDLE_SOURCE, 'Checking SPY for new candles', logMetadata);
    const spyCandles = await deps.candleClient.updateTickerData('SPY', true);
    let latestSpyDate = spyCandles.length > 0 ? spyCandles[spyCandles.length - 1].date : null;

    if (spyCandles.length > 0) {
      updatedTickers.add('SPY');
      ctx.loggingService.info(CANDLE_SOURCE, `Loaded ${spyCandles.length} new SPY candles`, {
        ...logMetadata,
        newCandles: spyCandles.length
      });
    }

    if (!latestSpyDate) {
      const lastSpyCandle = await deps.db.candles.getLastCandle('SPY');
      if (!lastSpyCandle?.date) {
        throw new Error('Unable to determine reference SPY candle date');
      }
      latestSpyDate = lastSpyCandle.date;
    }

    if (spyCandles.length > 0) {
      const refreshed = await refreshTickersFromAlpaca(
        ctx,
        deps,
        'full-sync',
        alwaysValidationTickers,
        candleSyncSettings
      );
      if (refreshed) {
        tickers = await deps.db.tickers.getTickers();
        symbols = buildSymbolList(tickers);
        totalTickers = symbols.length;
      }
    }

    const tickersToRefresh = await determineTickersToRefresh(
      ctx,
      deps,
      symbols,
      latestSpyDate,
      spyCandles.length > 0,
      candleSyncSettings.matchingRatioThreshold
    );

    const errors: string[] = [];
    let nextTickerIndex = 0;
    const workerCount = Math.min(candleSyncSettings.maxConcurrentUpdates, tickersToRefresh.length);
    const processNextTicker = async (): Promise<void> => {
      while (true) {
        if (ctx.abortSignal.aborted) {
          throw new Error('Candle synchronization cancelled');
        }

        const ticker = tickersToRefresh[nextTickerIndex++];
        if (nextTickerIndex % 500 === 0) {
          ctx.loggingService.info(
            CANDLE_SOURCE,
            `Candle sync progress: ${nextTickerIndex}/${tickersToRefresh.length}`,
            logMetadata
          );
        }
        if (ticker === undefined) {
          return;
        }

        try {
          const candles = await deps.candleClient.updateTickerData(ticker, true);
          if (candles.length > 0) {
            updatedTickers.add(ticker);
          }
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          errors.push(`${ticker}: ${message}`);
          ctx.loggingService.error(CANDLE_SOURCE, `Failed to update ${ticker}`, {
            ...logMetadata,
            error: message
          });
        }
      }
    };
    if (workerCount > 0) {
      await Promise.all(Array.from({ length: workerCount }, () => processNextTicker()));
    }

    const noDataTickers = deps.candleClient.drainNoDataTickers();
    if (noDataTickers.length > 0) {
      const sourceName = deps.candleClient.getCandleSourceName();
      ctx.loggingService.info(CANDLE_SOURCE, `No ${sourceName} data returned for ${noDataTickers.length} tickers`, {
        ...logMetadata,
        tickers: noDataTickers
      });
    }

    await scheduleNext(deps, ctx, autoDailyCandleSyncEnabled, logMetadata);

    return {
      message: `Updated ${updatedTickers.size} tickers`,
      meta: {
        totalTickers,
        updatedTickers: updatedTickers.size,
        tickersToRefresh: tickersToRefresh.length,
        errorCount: errors.length
      }
    };
  };
}

async function refreshTickersFromAlpaca(
  ctx: JobHandlerContext,
  deps: JobHandlerDependencies,
  reason: 'database-empty' | 'full-sync',
  alwaysValidationTickers: Set<string>,
  candleSyncSettings: CandleSyncSettings
): Promise<boolean> {
  try {
    ctx.loggingService.info(CANDLE_SOURCE, `Refreshing tickers from Alpaca (${reason})`, {
      jobId: ctx.job.id,
      reason
    });
    const assets = await deps.alpacaAssetService.fetchActiveEquityAssets();
    if (!assets.length) {
      ctx.loggingService.warn(CANDLE_SOURCE, 'Alpaca asset list was empty', { jobId: ctx.job.id, reason });
      return false;
    }

    const payload: TickerAssetRecord[] = assets.map(asset => ({
      symbol: asset.symbol,
      name: asset.name,
      tradable: asset.tradable,
      shortable: asset.shortable,
      easyToBorrow: asset.easyToBorrow,
      ...classifyAssetFromName(asset.name, candleSyncSettings),
      training: isTrainingTicker(asset.symbol, alwaysValidationTickers, candleSyncSettings.trainingAllocationRatio)
    }));

    const result = await deps.db.tickers.syncTickersFromAssets(payload);
    ctx.loggingService.info(CANDLE_SOURCE, 'Synced Alpaca tickers', {
      jobId: ctx.job.id,
      reason,
      assets: assets.length,
      upserted: result.upserted,
      disabled: result.disabled
    });
    return true;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    ctx.loggingService.error(CANDLE_SOURCE, 'Failed to refresh tickers from Alpaca', {
      jobId: ctx.job.id,
      reason,
      error: message
    });
    return false;
  }
}

function buildSymbolList(tickers: { symbol: string }[]): string[] {
  const symbols = Array.from(new Set(tickers.map(t => t.symbol)));
  if (!symbols.includes('SPY')) {
    symbols.unshift('SPY');
  }
  return symbols;
}

async function determineTickersToRefresh(
  ctx: JobHandlerContext,
  deps: JobHandlerDependencies,
  symbols: string[],
  latestSpyDate: Date,
  spyHadNewCandles: boolean,
  matchingRatioThreshold: number
): Promise<string[]> {
  if (spyHadNewCandles) {
    ctx.loggingService.info(CANDLE_SOURCE, 'SPY had new candles, refreshing all tickers', { jobId: ctx.job.id });
    return symbols.filter(symbol => symbol !== 'SPY');
  }

  const lastDates = await deps.db.candles.getLastCandleDates(symbols);
  const spyDateKey = toDateKey(latestSpyDate);
  const missingTickers = symbols.filter(symbol => {
    if (symbol === 'SPY') return false;
    const tickerDate = lastDates[symbol];
    return !tickerDate || toDateKey(tickerDate) !== spyDateKey;
  });

  const matchingRatio = symbols.length > 0 ? 1 - (missingTickers.length / symbols.length) : 1;
  const matchingPercent = Math.round(matchingRatio * 10000) / 100;

  if (matchingRatio >= matchingRatioThreshold) {
    ctx.loggingService.info(
      CANDLE_SOURCE,
      `Skipping refresh, ${matchingPercent}% tickers aligned with SPY`,
      { jobId: ctx.job.id }
    );
    return [];
  }

  ctx.loggingService.info(CANDLE_SOURCE, `Refreshing ${missingTickers.length} tickers missing latest SPY candle`, {
    jobId: ctx.job.id,
    matchingPercent
  });
  return missingTickers;
}

async function scheduleNext(deps: JobHandlerDependencies, ctx: JobHandlerContext, autoDailyCandleSyncEnabled: boolean, logMetadata: { jobId: string }): Promise<void> {
  ctx.loggingService.info(CANDLE_SOURCE, 'Refreshing market data snapshot after candle update pass', logMetadata);
  await deps.engineCli.run('export-market-data', [], ctx.abortSignal, logMetadata);

  const hasPendingSignalJob = ctx.scheduler.hasPendingJob(job => job.type === 'generate-signals');
  if (!hasPendingSignalJob) {
    ctx.scheduler.scheduleJob('generate-signals', {
      description: 'Triggered by candle synchronization update'
    });
  }

  if (!autoDailyCandleSyncEnabled) {
    ctx.loggingService.info(CANDLE_SOURCE, 'Automatic daily candle sync is disabled; skipping schedule', {
      jobId: ctx.job.id
    });
    return;
  }

  const nextMidnight = getNextMidnightUtc();
  const alreadyScheduled = ctx.scheduler.hasPendingJob(job =>
    job.type === 'candle-sync' &&
    Math.abs(job.scheduledFor.getTime() - nextMidnight.getTime()) < 60 * 1000
  );

  if (!alreadyScheduled) {
    ctx.scheduler.scheduleJob('candle-sync', {
      startAt: nextMidnight,
      description: 'Daily midnight candle sync pass',
      metadata: { trigger: 'daily' }
    });
  }
}

function getNextMidnightUtc(): Date {
  const now = new Date();
  const next = new Date(now);
  next.setUTCHours(23, 59, 0, 0);

  const timeUntilNext = next.getTime() - now.getTime();
  if (timeUntilNext < 60 * 60 * 1000) {
    next.setUTCDate(next.getUTCDate() + 1);
  }

  return next;
}

type MarketClockStatus = {
  isOpen: boolean;
  source: 'alpaca' | 'alpaca-unavailable';
  timestamp?: Date;
  nextOpen?: Date;
  nextClose?: Date;
};

async function resolveMarketClock(
  ctx: JobHandlerContext,
  deps: JobHandlerDependencies
): Promise<MarketClockStatus> {
  try {
    const clock = await deps.alpacaAssetService.fetchMarketClock(ctx.abortSignal);
    return {
      isOpen: clock.isOpen,
      source: 'alpaca',
      timestamp: clock.timestamp,
      nextOpen: clock.nextOpen,
      nextClose: clock.nextClose
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    ctx.loggingService.warn(CANDLE_SOURCE, 'Failed to fetch Alpaca market clock; continuing without open/close info', {
      jobId: ctx.job.id,
      error: message
    });
    return {
      isOpen: false,
      source: 'alpaca-unavailable'
    };
  }
}

function toDateKey(date: Date): string {
  return date.toISOString().split('T')[0];
}

function isTrainingTicker(
  symbol: string,
  alwaysValidationTickers: Set<string>,
  trainingAllocationRatio: number
): boolean {
  const normalized = symbol.trim().toUpperCase();
  if (alwaysValidationTickers.has(normalized)) return false;
  const hash = createHash('sha256').update(normalized).digest();
  const value = hash.readUInt32BE(0) / 0xffffffff;
  return value < trainingAllocationRatio;
}
