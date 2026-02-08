import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const SIGNAL_SOURCE = 'signal-job';

export function createSignalHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(SIGNAL_SOURCE, 'Running signal generation job', logMetadata);
    await deps.engineCli.run('generate-signals', [], ctx.abortSignal, logMetadata);

    const hasQueuedSignals = ctx.scheduler.hasPendingJob(job => job.type === 'generate-signals' && job.status === 'queued');
    const hasQueuedBacktest = ctx.scheduler.hasPendingJob(job => job.type === 'reconcile-trades');
    if (!hasQueuedSignals && !hasQueuedBacktest) {
      ctx.scheduler.scheduleJob('reconcile-trades', {
        description: 'Triggered by signal generation'
      });
    }

    return {
      message: 'Signals generated successfully'
    };
  };
}
