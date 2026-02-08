import { JobHandler, JobType } from './JobScheduler';
import { JobHandlerDependencies } from './types';
import { createEngineCompileHandler } from './handlers/engineCompileHandler';
import { createCandleSyncHandler } from './handlers/candleSyncHandler';
import { createMarketDataSnapshotHandler } from './handlers/marketDataSnapshotHandler';
import { createSignalHandler } from './handlers/signalHandler';
import { createBacktestHandler } from './handlers/backtestHandler';
import { createPlanOperationsHandler } from './handlers/planOperationsHandler';
import { createDispatchOperationsHandler } from './handlers/dispatchOperationsHandler';
import { createReconcileTradesHandler } from './handlers/reconcileTradesHandler';
import { createOptimizeHandler } from './handlers/optimizeHandler';
import { createTrainLightgbmHandler } from './handlers/trainLightgbmHandler';

type HandlerMap = Record<JobType, JobHandler>;

export function createJobHandlers(deps: JobHandlerDependencies): HandlerMap {
  return {
    'engine-compile': createEngineCompileHandler(deps),
    'candle-sync': createCandleSyncHandler(deps),
    'export-market-data': createMarketDataSnapshotHandler(deps),
    'generate-signals': createSignalHandler(deps),
    'backtest-active': createBacktestHandler(deps),
    'reconcile-trades': createReconcileTradesHandler(deps),
    'plan-operations': createPlanOperationsHandler(deps),
    'dispatch-operations': createDispatchOperationsHandler(deps),
    'optimize': createOptimizeHandler(deps),
    'train-lightgbm': createTrainLightgbmHandler(deps)
  };
}
