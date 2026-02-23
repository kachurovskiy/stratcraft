import { BacktestScope } from '../../../shared/types/StrategyTemplate';
import { SETTING_KEYS } from '../../constants';
import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const BACKTEST_SOURCE = 'backtest-job';
const BACKTEST_SCOPES: BacktestScope[] = ['validation', 'training'];

const parseBacktestMonthsSetting = (rawValue: string | null): number[] => {
  return rawValue
    ? Array.from(new Set(rawValue.replace(/[\[\]]/g, '').split(/[,\s]+/g)
        .map((entry) => Math.trunc(Number(entry)))
        .filter((entry) => Number.isFinite(entry) && entry > 0)))
    : [];
};

export function createBacktestHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(BACKTEST_SOURCE, 'Running backtest-active job', logMetadata);

    const rawMonthsSetting = await deps.db.settings.getSettingValue(SETTING_KEYS.BACKTEST_ACTIVE_MONTHS);
    const parsedMonths = parseBacktestMonthsSetting(rawMonthsSetting);
    if (parsedMonths.length === 0) {
      const message = rawMonthsSetting && rawMonthsSetting.trim().length > 0
        ? `Invalid BACKTEST_ACTIVE_MONTHS value "${rawMonthsSetting}".`
        : 'BACKTEST_ACTIVE_MONTHS is required but empty.';
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
