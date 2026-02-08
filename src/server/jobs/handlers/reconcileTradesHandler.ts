import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const RECONCILE_SOURCE = 'reconcile-trades-job';

export function createReconcileTradesHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(RECONCILE_SOURCE, 'Running reconcile-trades job', logMetadata);
    await deps.engineCli.run('reconcile-trades', [], ctx.abortSignal, logMetadata);

    const skipPlanOperations = Boolean(ctx.job.metadata?.skipPlanOperations);
    if (!skipPlanOperations) {
      const hasPlanJob = ctx.scheduler.hasPendingJob(job => job.type === 'backtest-active');
      if (!hasPlanJob) {
        ctx.scheduler.scheduleJob('backtest-active', {
          description: 'Triggered after trade reconciliation'
        });
      }
    }

    ctx.loggingService.info(RECONCILE_SOURCE, 'Reconcile trades completed', logMetadata);
    return {
      message: 'Reconciled trades'
    };
  };
}
