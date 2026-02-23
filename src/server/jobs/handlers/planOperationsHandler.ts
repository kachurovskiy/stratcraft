import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const PLAN_SOURCE = 'plan-operations-job';

export function createPlanOperationsHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(PLAN_SOURCE, 'Running plan-operations job', logMetadata);
    await deps.engineCli.run('plan-operations', [], ctx.abortSignal, logMetadata);
    const hasDispatchJob = ctx.scheduler.hasPendingJob(job => job.type === 'dispatch-operations');
    if (!hasDispatchJob) {
      ctx.scheduler.scheduleJob('dispatch-operations', {
        description: 'Auto-dispatch newly planned operations'
      });
    }
    ctx.loggingService.info(PLAN_SOURCE, 'Plan operations completed', logMetadata);
    return {
      message: 'Plan operations finished'
    };
  };
}
