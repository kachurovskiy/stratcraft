import { createOptimizeHandler } from './optimizeHandler';

function createHarness() {
  const loggingService = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  };
  const abortSignal = new AbortController().signal;
  const template = {
    id: 'adx_breakout',
    name: 'ADX Breakout'
  };
  const deps = {
    db: {
      templates: {
        getTemplateNeedingLocalOptimization: jest.fn()
          .mockResolvedValueOnce(template)
          .mockResolvedValueOnce(null),
        getAllTemplateIds: jest.fn().mockResolvedValue([template.id])
      },
      remoteOptimizerJobs: {
        hasActiveRemoteOptimizerJob: jest.fn().mockResolvedValue(false)
      },
      strategies: {
        deleteStrategy: jest.fn()
      },
      backtestCache: {
        getBacktestCacheTemplateCounts: jest.fn().mockResolvedValue([{ templateId: template.id, count: 1 }])
      },
      settings: {
        value: {
          optimizer: {
            optimizerExploreEnabled: true
          }
        }
      }
    },
    engineCli: {
      run: jest.fn().mockResolvedValue(undefined),
      forceTerminateActiveProcess: jest.fn()
    },
    strategyRegistry: {
      refreshOutdatedDefaultStrategies: jest.fn().mockResolvedValue({
        checked: 1,
        refreshed: 1,
        unchanged: 0,
        failures: []
      })
    }
  } as any;
  const ctx = {
    job: {
      id: 'job-1',
      type: 'optimize',
      status: 'running',
      scheduledFor: new Date('2025-01-02T14:00:00.000Z'),
      createdAt: new Date('2025-01-02T14:00:00.000Z'),
      attempts: 1,
      maxRetries: 5
    },
    abortSignal,
    loggingService: loggingService as any,
    scheduler: {} as any
  } as any;

  return { ctx, deps, template };
}

describe('createOptimizeHandler', () => {
  test('checks default strategies after verification and balance', async () => {
    const { ctx, deps } = createHarness();

    const handler = createOptimizeHandler(deps);
    const result = await handler(ctx);

    expect(deps.engineCli.run.mock.calls.map((call: unknown[]) => call[0])).toEqual([
      'optimize',
      'verify',
      'balance',
      'explore'
    ]);
    expect(deps.strategyRegistry.refreshOutdatedDefaultStrategies).toHaveBeenCalledTimes(1);

    const balanceCallOrder = deps.engineCli.run.mock.invocationCallOrder[2];
    expect(deps.strategyRegistry.refreshOutdatedDefaultStrategies.mock.invocationCallOrder[0]).toBeGreaterThan(balanceCallOrder);
    expect(result?.meta).toMatchObject({
      optimized: 1,
      verified: 1,
      balanced: 1,
      defaultRefreshAttempted: 1,
      defaultRefreshed: 1
    });
  });

  test('still checks default strategies after verification failure', async () => {
    const { ctx, deps, template } = createHarness();
    deps.engineCli.run.mockImplementation((command: string) => {
      if (command === 'verify') {
        return Promise.reject(new Error('verify failed'));
      }
      return Promise.resolve();
    });

    const handler = createOptimizeHandler(deps);
    await expect(handler(ctx)).rejects.toThrow(
      `Optimize pass incomplete: verification failed (1): ${template.id}`
    );

    expect(deps.strategyRegistry.refreshOutdatedDefaultStrategies).toHaveBeenCalledTimes(1);
  });

  test('fails incomplete balance pass so the scheduler can retry', async () => {
    const { ctx, deps, template } = createHarness();
    deps.engineCli.run.mockImplementation((command: string) => {
      if (command === 'balance') {
        return Promise.reject(new Error('connection closed'));
      }
      return Promise.resolve();
    });

    const handler = createOptimizeHandler(deps);
    await expect(handler(ctx)).rejects.toThrow(
      `Optimize pass incomplete: balance failed (1): ${template.id}`
    );

    expect(deps.strategyRegistry.refreshOutdatedDefaultStrategies).toHaveBeenCalledTimes(1);
  });

  test('skips explore runs when optimizer explore is disabled', async () => {
    const { ctx, deps } = createHarness();
    deps.db.settings.value.optimizer.optimizerExploreEnabled = false;

    const handler = createOptimizeHandler(deps);
    const result = await handler(ctx);

    expect(deps.engineCli.run.mock.calls.map((call: unknown[]) => call[0])).toEqual([
      'optimize',
      'verify',
      'balance'
    ]);
    expect(deps.db.backtestCache.getBacktestCacheTemplateCounts).not.toHaveBeenCalled();
    expect(result?.meta).toMatchObject({
      exploreEnabled: false,
      exploreAttempted: 0,
      explored: 0,
      exploreFailures: []
    });
    expect(result?.message).toContain('Explore disabled');
  });
});
