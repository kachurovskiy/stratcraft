import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const ENGINE_COMPILE_SOURCE = 'engine-compile-job';

export function createEngineCompileHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(ENGINE_COMPILE_SOURCE, 'Starting engine compilation job', logMetadata);
    await deps.engineCli.compile(ctx.abortSignal, logMetadata);
    ctx.loggingService.info(ENGINE_COMPILE_SOURCE, 'Engine compilation completed', logMetadata);

    const hasQueuedTickerJob = ctx.scheduler.hasPendingJob(
      job => job.type === 'ticker-sync' && job.status === 'queued'
    );

    if (!hasQueuedTickerJob) {
      ctx.scheduler.scheduleJob('ticker-sync', {
        description: 'Initial ticker synchronization after engine compile'
      });
    }

    return {
      message: 'Engine compiled successfully'
    };
  };
}
