import { JobHandler, JobHandlerContext } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';
import { toDateKey } from '../../utils/date';

const CANDLE_SOURCE = 'candle-job';

export function createCandleSyncHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    const { candleSync, tickerRules } = deps.db.settings.value;
    const ignoredTickers = new Set(tickerRules.ignoredTickers);
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
        await finishCandleSync(deps, ctx, logMetadata);
        return { message: 'Candle sync skipped while market is open' };
      }
    }

    const tickers = await loadFilteredTickers();

    if (!tickers.length) {
      ctx.loggingService.warn(CANDLE_SOURCE, 'No tickers available for candle sync', logMetadata);
      await finishCandleSync(deps, ctx, logMetadata);
      return {
        message: 'No tickers found for synchronization'
      };
    }

    const symbols = buildSymbolList(tickers);
    const totalTickers = symbols.length;
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

    const tickersToRefresh = await determineTickersToRefresh(
      ctx,
      deps,
      symbols,
      latestSpyDate,
      spyCandles.length > 0,
      candleSync.matchingRatioThreshold
    );

    const errors: string[] = [];
    let nextTickerIndex = 0;
    const workerCount = Math.min(candleSync.maxConcurrentUpdates, tickersToRefresh.length);
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

    await finishCandleSync(deps, ctx, logMetadata);

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

async function finishCandleSync(
  deps: JobHandlerDependencies,
  ctx: JobHandlerContext,
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
