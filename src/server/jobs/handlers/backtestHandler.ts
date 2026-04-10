import { BacktestScope } from '../../types/StrategyTemplate';
import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const BACKTEST_SOURCE = 'backtest-job';
const BACKTEST_SCOPES: BacktestScope[] = ['validation', 'training'];

export function createBacktestHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(BACKTEST_SOURCE, 'Running backtest-active job', logMetadata);

    const parsedMonths = deps.db.settings.value.engine.backtestActiveMonths;
    if (parsedMonths.length === 0) {
      const message = 'BACKTEST_ACTIVE_MONTHS is required but empty.';
      ctx.loggingService.error(BACKTEST_SOURCE, message, logMetadata);
      throw new Error(message);
    }

    for (const scope of BACKTEST_SCOPES) {
      ctx.loggingService.info(
        BACKTEST_SOURCE,
        `Running ${scope} backtest windows: ${parsedMonths.join(', ')}`,
        logMetadata
      );
      await deps.engineCli.run(
        'backtest-active',
        ['--scope', scope, parsedMonths.join(',')],
        ctx.abortSignal,
        logMetadata
      );
    }

    ctx.loggingService.info(
      BACKTEST_SOURCE,
      'Running account-linked backtests with all tickers',
      logMetadata
    );
    await deps.engineCli.run('backtest-accounts', [], ctx.abortSignal, logMetadata);

    const hasReconcileJob = ctx.scheduler.hasPendingJob(job => job.type === 'plan-operations');
    if (!hasReconcileJob) {
      ctx.scheduler.scheduleJob('plan-operations', {
        description: 'Triggered by backtest results'
      });
    }

    return {
      message: 'Backtest active strategies completed'
    };
  };
}
