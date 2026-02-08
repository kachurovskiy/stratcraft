import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const MARKET_DATA_SOURCE = 'market-data-job';

export function createMarketDataSnapshotHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(MARKET_DATA_SOURCE, 'Refreshing market data snapshot', logMetadata);

    if (!deps.engineCli.isCompiled()) {
      ctx.loggingService.warn(
        MARKET_DATA_SOURCE,
        'Engine binary missing; compiling before snapshot refresh',
        logMetadata
      );
      await deps.engineCli.compile(ctx.abortSignal, logMetadata);
    }

    await deps.engineCli.run('export-market-data', [], ctx.abortSignal, logMetadata);
    ctx.loggingService.info(MARKET_DATA_SOURCE, 'Market data snapshot refresh completed', logMetadata);

    return {
      message: 'Market data snapshot refreshed'
    };
  };
}
