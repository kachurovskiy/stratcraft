import { JobHandler, JobHandlerContext } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';
import type { TickerAssetRecord } from '../../database/types';
import { deploymentService } from '../../services/DeploymentService';
import { classifyAssetFromName, isTrainingTicker } from '../../utils/assetClassification';

const TICKER_SOURCE = 'ticker-job';
const SERVER_UPDATE_SOURCE = 'system';

type TickerSyncSummary = {
  assets: number;
  upserted: number;
  disabled: number;
};

export function createTickerSyncHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    const { autoDailyCandleSyncEnabled, autoDailyServerUpdateEnabled } = deps.db.settings.value.candleSync;

    if (autoDailyServerUpdateEnabled) {
      const updateTriggered = await triggerServerUpdateIfBehind(ctx, logMetadata);
      if (updateTriggered) {
        return { message: 'Server update triggered instead of ticker sync' };
      }
    }

    const syncSummary = await syncTickersFromAlpaca(ctx, deps);
    if (ctx.abortSignal.aborted) {
      throw new Error('Ticker synchronization cancelled');
    }

    const candleSyncScheduled = scheduleCandleSync(ctx, logMetadata);
    scheduleNextDailyTickerSync(ctx, autoDailyCandleSyncEnabled, logMetadata);

    return {
      message: 'Ticker sync completed',
      meta: {
        ...(syncSummary ?? {}),
        candleSyncScheduled
      }
    };
  };
}

async function syncTickersFromAlpaca(
  ctx: JobHandlerContext,
  deps: JobHandlerDependencies
): Promise<TickerSyncSummary | null> {
  const { expenseRatios, tickerRules } = deps.db.settings.value;
  const alwaysValidationTickers = new Set(tickerRules.alwaysValidationTickers);

  try {
    ctx.loggingService.info(TICKER_SOURCE, 'Refreshing tickers from Alpaca', {
      jobId: ctx.job.id
    });
    const assets = await deps.alpacaAssetService.fetchActiveEquityAssets();
    if (!assets.length) {
      ctx.loggingService.warn(TICKER_SOURCE, 'Alpaca asset list was empty', { jobId: ctx.job.id });
      return {
        assets: 0,
        upserted: 0,
        disabled: 0
      };
    }

    const payload: TickerAssetRecord[] = assets.map(asset => ({
      symbol: asset.symbol,
      name: asset.name,
      tradable: asset.tradable,
      shortable: asset.shortable,
      easyToBorrow: asset.easyToBorrow,
      ...classifyAssetFromName(asset.name, {
        etfBaseExpenseRatio: expenseRatios.etfBaseExpenseRatio,
        inverseEtfExpenseRatio: expenseRatios.inverseEtfExpenseRatio,
        commodityTrustExpenseRatio: expenseRatios.commodityTrustExpenseRatio,
        bondEtfExpenseRatio: expenseRatios.bondEtfExpenseRatio,
        incomeEtfExpenseRatio: expenseRatios.incomeEtfExpenseRatio,
        leveragedExpenseRatios: {
          2: expenseRatios.leveraged2xExpenseRatio,
          3: expenseRatios.leveraged3xExpenseRatio,
          5: expenseRatios.leveraged5xExpenseRatio
        }
      }),
      training: isTrainingTicker(asset.symbol, alwaysValidationTickers, tickerRules.trainingAllocationRatio)
    }));

    const result = await deps.db.tickers.syncTickersFromAssets(payload);
    ctx.loggingService.info(TICKER_SOURCE, 'Synced Alpaca tickers', {
      jobId: ctx.job.id,
      assets: assets.length,
      upserted: result.upserted,
      disabled: result.disabled
    });

    return {
      assets: assets.length,
      upserted: result.upserted,
      disabled: result.disabled
    };
  } catch (error) {
    if (ctx.abortSignal.aborted) {
      throw error;
    }

    const message = error instanceof Error ? error.message : String(error);
    ctx.loggingService.error(TICKER_SOURCE, 'Failed to refresh tickers from Alpaca', {
      jobId: ctx.job.id,
      error: message
    });
    return null;
  }
}

function scheduleCandleSync(
  ctx: JobHandlerContext,
  logMetadata: { jobId: string }
): boolean {
  const hasPendingCandleSync = ctx.scheduler.hasPendingJob(job => job.type === 'candle-sync');
  if (hasPendingCandleSync) {
    ctx.loggingService.info(TICKER_SOURCE, 'Candle sync already pending; skipping schedule', logMetadata);
    return false;
  }

  ctx.scheduler.scheduleJob('candle-sync', {
    description: 'Triggered by ticker synchronization update'
  });
  return true;
}

function scheduleNextDailyTickerSync(
  ctx: JobHandlerContext,
  autoDailyCandleSyncEnabled: boolean,
  logMetadata: { jobId: string }
): void {
  if (!autoDailyCandleSyncEnabled) {
    ctx.loggingService.info(TICKER_SOURCE, 'Automatic daily ticker sync is disabled; skipping schedule', logMetadata);
    return;
  }

  const nextDailyRunAt = getNextDailyTickerSyncUtc();
  const alreadyScheduled = ctx.scheduler.hasPendingJob(job =>
    job.type === 'ticker-sync' &&
    Math.abs(job.scheduledFor.getTime() - nextDailyRunAt.getTime()) < 60 * 1000
  );

  if (!alreadyScheduled) {
    ctx.scheduler.scheduleJob('ticker-sync', {
      startAt: nextDailyRunAt,
      description: 'Daily 2am London ticker sync pass',
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

  ctx.loggingService.warn(SERVER_UPDATE_SOURCE, 'Auto server update triggered before ticker sync', {
    ...logMetadata,
    behindCount: status.behindCount
  });
  return true;
}

function getNextDailyTickerSyncUtc(): Date {
  const now = new Date();
  const next = new Date(now);
  next.setUTCHours(3, 59, 0, 0);
  next.setUTCDate(next.getUTCDate() + 1);

  return next;
}
