import { formatDate } from '../../api/candleSources/candleSourceUtils';
import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const CORPORATE_ACTION_SOURCE = 'corporate-actions-job';

export function createCorporateActionSyncHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(CORPORATE_ACTION_SOURCE, 'Running corporate actions sync', logMetadata);

    const scope = await deps.db.trades.getAccountTradeCorporateActionScope();
    if (!scope.firstTradeDate || scope.tickers.length === 0) {
      ctx.loggingService.info(CORPORATE_ACTION_SOURCE, 'No account-tied trades found for corporate action sync', logMetadata);
      const candleSyncScheduled = scheduleCandleSync(ctx, logMetadata);
      return {
        message: 'No account-tied trades found',
        meta: {
          tickers: 0,
          fetched: 0,
          upserted: 0,
          candleSyncScheduled
        }
      };
    }

    const latestKnownProcessDate = await deps.db.corporateActions.getLatestProcessDate();
    const startDate =
      latestKnownProcessDate && latestKnownProcessDate.getTime() > scope.firstTradeDate.getTime()
        ? latestKnownProcessDate
        : scope.firstTradeDate;
    const endDate = new Date();

    try {
      const actions = await deps.alpacaAssetService.fetchCorporateActions({
        symbols: scope.tickers,
        startDate,
        endDate,
        abortSignal: ctx.abortSignal
      });

      if (ctx.abortSignal.aborted) {
        throw new Error('Corporate action sync cancelled');
      }

      const result = await deps.db.corporateActions.upsertCorporateActions(actions);
      const candleSyncScheduled = scheduleCandleSync(ctx, logMetadata);

      ctx.loggingService.info(CORPORATE_ACTION_SOURCE, 'Corporate actions sync completed', {
        ...logMetadata,
        tickers: scope.tickers.length,
        fetched: actions.length,
        upserted: result.upserted,
        candleSyncScheduled,
        startDate: formatDate(startDate),
        endDate: formatDate(endDate),
        latestKnownProcessDate: latestKnownProcessDate ? formatDate(latestKnownProcessDate) : null
      });

      return {
        message: actions.length > 0 ? 'Synchronized corporate actions' : 'No corporate actions found',
        meta: {
          tickers: scope.tickers.length,
          fetched: actions.length,
          upserted: result.upserted,
          candleSyncScheduled,
          startDate: formatDate(startDate),
          endDate: formatDate(endDate)
        }
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      ctx.loggingService.error(CORPORATE_ACTION_SOURCE, 'Corporate actions sync failed', {
        ...logMetadata,
        tickers: scope.tickers.length,
        startDate: formatDate(startDate),
        endDate: formatDate(endDate),
        error: message
      });
      throw error;
    }
  };
}

function scheduleCandleSync(
  ctx: Parameters<JobHandler>[0],
  logMetadata: { jobId: string }
): boolean {
  const hasPendingCandleSync = ctx.scheduler.hasPendingJob(job => job.type === 'candle-sync');
  if (hasPendingCandleSync) {
    ctx.loggingService.info(CORPORATE_ACTION_SOURCE, 'Candle sync already pending; skipping schedule', logMetadata);
    return false;
  }

  ctx.scheduler.scheduleJob('candle-sync', {
    description: 'Triggered by corporate actions synchronization update'
  });
  return true;
}
