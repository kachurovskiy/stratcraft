import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const ENGINE_COMPILE_SOURCE = 'engine-compile-job';

export function createEngineCompileHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(ENGINE_COMPILE_SOURCE, 'Starting engine compilation job', logMetadata);
    await deps.engineCli.compile(ctx.abortSignal, logMetadata);
    ctx.loggingService.info(ENGINE_COMPILE_SOURCE, 'Engine compilation completed', logMetadata);

    const hasQueuedCandleJob = ctx.scheduler.hasPendingJob(
      job => job.type === 'candle-sync' && job.status === 'queued'
    );

    if (!hasQueuedCandleJob) {
      ctx.scheduler.scheduleJob('candle-sync', {
        description: 'Initial candle synchronization after engine compile'
      });
    }

    return {
      message: 'Engine compiled successfully'
    };
  };
}
