import { BacktestScope } from '../../types/StrategyTemplate';
import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const START_TIMING_SOURCE = 'start-timing-backtest-job';
const DEFAULT_WEEKS = 52;
const MAX_WEEKS = 260;

const normalizeScope = (value: unknown): Exclude<BacktestScope, 'live'> => {
  if (value === 'training' || value === 'all') {
    return value;
  }
  return 'validation';
};

const normalizePositiveInteger = (value: unknown, fallback: number, max: number): number => {
  const parsed = typeof value === 'string' ? Number.parseInt(value, 10) : Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.min(max, Math.floor(parsed));
};

export function createStartTimingBacktestHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const metadata = ctx.job.metadata ?? {};
    const strategyId = typeof metadata.strategyId === 'string' ? metadata.strategyId.trim() : '';
    if (!strategyId) {
      throw new Error('Start timing backtest job requires strategyId metadata.');
    }

    const scope = normalizeScope(metadata.scope);
    const weeks = normalizePositiveInteger(metadata.weeks, DEFAULT_WEEKS, MAX_WEEKS);
    const logMetadata = {
      jobId: ctx.job.id,
      strategyId,
      scope,
      weeks
    };

    ctx.loggingService.info(
      START_TIMING_SOURCE,
      'Running horizon-limited start timing samples',
      logMetadata
    );

    await deps.engineCli.run(
      'backtest-start-timing',
      [
        '--strategy-id',
        strategyId,
        '--scope',
        scope,
        '--weeks',
        String(weeks)
      ],
      ctx.abortSignal,
      logMetadata
    );

    return {
      message: `Start timing samples refreshed for ${strategyId}`,
      meta: {
        strategyId,
        scope,
        weeks
      }
    };
  };
}
