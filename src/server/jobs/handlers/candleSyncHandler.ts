import { JobHandler, JobHandlerContext } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';
import type { TickerAssetRecord } from '../../database/types';
import { SETTING_KEYS } from '../../constants';
import { deploymentService } from '../../services/DeploymentService';
import {
  type AssetClassificationSettings,
  classifyAssetFromName,
  isTrainingTicker
} from '../../utils/assetClassification';
import { toDateKey } from '../../utils/date';
import { parseRequiredNumberSetting } from '../../utils/settings';

const CANDLE_SOURCE = 'candle-job';
const SERVER_UPDATE_SOURCE = 'system';

type CandleSyncSettings = AssetClassificationSettings & {
  maxConcurrentUpdates: number;
  trainingAllocationRatio: number;
  matchingRatioThreshold: number;
};

function loadCandleSyncSettings(db: JobHandlerDependencies['db']): CandleSyncSettings {
  const { candleSync, expenseRatios, tickerRules } = db.settings.value;

  return {
    maxConcurrentUpdates: parseRequiredNumberSetting(
      SETTING_KEYS.CANDLE_SYNC_MAX_CONCURRENT_UPDATES,
      String(candleSync.maxConcurrentUpdates),
      { min: 1, integer: true }
    ),
    etfBaseExpenseRatio: parseRequiredNumberSetting(
      SETTING_KEYS.ETF_BASE_EXPENSE_RATIO,
      String(expenseRatios.etfBaseExpenseRatio),
      { min: 0 }
    ),
    inverseEtfExpenseRatio: parseRequiredNumberSetting(
      SETTING_KEYS.INVERSE_ETF_EXPENSE_RATIO,
      String(expenseRatios.inverseEtfExpenseRatio),
      { min: 0 }
    ),
    commodityTrustExpenseRatio: parseRequiredNumberSetting(
      SETTING_KEYS.COMMODITY_TRUST_EXPENSE_RATIO,
      String(expenseRatios.commodityTrustExpenseRatio),
      { min: 0 }
    ),
    bondEtfExpenseRatio: parseRequiredNumberSetting(
      SETTING_KEYS.BOND_ETF_EXPENSE_RATIO,
      String(expenseRatios.bondEtfExpenseRatio),
      { min: 0 }
    ),
    incomeEtfExpenseRatio: parseRequiredNumberSetting(
      SETTING_KEYS.INCOME_ETF_EXPENSE_RATIO,
      String(expenseRatios.incomeEtfExpenseRatio),
      { min: 0 }
    ),
    leveragedExpenseRatios: {
      2: parseRequiredNumberSetting(
        SETTING_KEYS.LEVERAGED_2X_EXPENSE_RATIO,
        String(expenseRatios.leveraged2xExpenseRatio),
        { min: 0 }
      ),
      3: parseRequiredNumberSetting(
        SETTING_KEYS.LEVERAGED_3X_EXPENSE_RATIO,
        String(expenseRatios.leveraged3xExpenseRatio),
        { min: 0 }
      ),
      5: parseRequiredNumberSetting(
        SETTING_KEYS.LEVERAGED_5X_EXPENSE_RATIO,
        String(expenseRatios.leveraged5xExpenseRatio),
        { min: 0 }
      )
    },
    trainingAllocationRatio: parseRequiredNumberSetting(
      SETTING_KEYS.TRAINING_ALLOCATION_RATIO,
      String(tickerRules.trainingAllocationRatio),
      { min: 0, max: 1 }
    ),
    matchingRatioThreshold: parseRequiredNumberSetting(
      SETTING_KEYS.CANDLE_SYNC_MATCHING_RATIO_THRESHOLD,
      String(candleSync.matchingRatioThreshold),
      { min: 0, max: 1 }
    )
  };
}

export function createCandleSyncHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    const settingsValue = deps.db.settings.value;
    const candleSyncSettings = loadCandleSyncSettings(deps.db);
    const alwaysValidationTickers = new Set(settingsValue.tickerRules.alwaysValidationTickers);
    const autoDailyCandleSyncEnabled = settingsValue.candleSync.autoDailyCandleSyncEnabled;
    const autoDailyServerUpdateEnabled = settingsValue.candleSync.autoDailyServerUpdateEnabled;
    const ignoredTickers = new Set(settingsValue.tickerRules.ignoredTickers);
    if (autoDailyServerUpdateEnabled) {
      const updateTriggered = await triggerServerUpdateIfBehind(ctx, logMetadata);
      if (updateTriggered) {
        return { message: 'Server update triggered instead of candle sync' };
      }
    }
    const filterIgnoredTickers = <T extends { symbol: string }>(items: T[]) =>
      ignoredTickers.size === 0 ? items : items.filter(item => !ignoredTickers.has(item.symbol));
    const loadFilteredTickers = async () => filterIgnoredTickers(await deps.db.tickers.getTickers());
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

    let tickers = await loadFilteredTickers();
    if (!tickers.length) {
      const seeded = await refreshTickersFromAlpaca(
        ctx,
        deps,
        'database-empty',
        alwaysValidationTickers,
        candleSyncSettings
      );
      if (seeded) {
        tickers = await loadFilteredTickers();
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
    const spyCandles = await deps.candleClient.updateTickerData('SPY', true, ctx.abortSignal);
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
        tickers = await loadFilteredTickers();
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
          const candles = await deps.candleClient.updateTickerData(ticker, true, ctx.abortSignal);
          if (candles.length > 0) {
            updatedTickers.add(ticker);
          }
        } catch (error) {
          if (ctx.abortSignal.aborted) {
            throw error;
          }
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

async function scheduleNext(
  deps: JobHandlerDependencies,
  ctx: JobHandlerContext,
  autoDailyCandleSyncEnabled: boolean,
  logMetadata: { jobId: string }
): Promise<void> {
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

  const nextDailyRunAt = getNextDailyCandleSyncUtc();
  const alreadyScheduled = ctx.scheduler.hasPendingJob(job =>
    job.type === 'candle-sync' &&
    Math.abs(job.scheduledFor.getTime() - nextDailyRunAt.getTime()) < 60 * 1000
  );

  if (!alreadyScheduled) {
    ctx.scheduler.scheduleJob('candle-sync', {
      startAt: nextDailyRunAt,
      description: 'Daily 2am London candle sync pass',
      metadata: { trigger: 'daily' }
    });
  }
}

async function triggerServerUpdateIfBehind(
  ctx: JobHandlerContext,
  logMetadata: { jobId: string }
): Promise<boolean> {
  const status = await deploymentService.getUpstreamBehindCount();
  if (status.error) {
    ctx.loggingService.warn(SERVER_UPDATE_SOURCE, 'Unable to check upstream commits for auto-update', {
      ...logMetadata,
      error: status.error
    });
    return false;
  }

  if (status.behindCount <= 0) {
    return false;
  }

  try {
    await deploymentService.triggerServerUpdate();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    ctx.loggingService.error(SERVER_UPDATE_SOURCE, 'Failed to trigger auto server update', {
      ...logMetadata,
      behindCount: status.behindCount,
      error: message
    });
    throw error;
  }

  ctx.loggingService.warn(SERVER_UPDATE_SOURCE, 'Auto server update triggered before candle sync', {
    ...logMetadata,
    behindCount: status.behindCount
  });
  return true;
}

function getNextDailyCandleSyncUtc(): Date {
  const now = new Date();
  const next = new Date(now);
  next.setUTCHours(3, 59, 0, 0);
  next.setUTCDate(next.getUTCDate() + 1);

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
