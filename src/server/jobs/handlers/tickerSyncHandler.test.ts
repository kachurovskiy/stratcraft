import { createTickerSyncHandler } from './tickerSyncHandler';

type HarnessOptions = {
  autoDailyCandleSyncEnabled?: boolean;
  corporateActionsSyncPending?: boolean;
};

function createHarness(options: HarnessOptions = {}) {
  const loggingService = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  };
  const scheduler = {
    hasPendingJob: jest.fn().mockImplementation((predicate: (job: any) => boolean) => {
      if (options.corporateActionsSyncPending) {
        return predicate({
          id: 'corporate-actions-job-1',
          type: 'corporate-actions-sync',
          status: 'queued',
          scheduledFor: new Date('2025-01-02T14:05:00.000Z'),
          createdAt: new Date('2025-01-02T14:00:00.000Z'),
          attempts: 0,
          maxRetries: 5
        });
      }

      return false;
    }),
    scheduleJob: jest.fn()
  };
  const abortSignal = new AbortController().signal;
  const assets = [
    {
      symbol: 'AAA',
      name: 'Alpha Asset Inc.',
      tradable: true,
      shortable: true,
      easyToBorrow: true
    }
  ];
  const deps = {
    db: {
      settings: {
        value: {
          candleSync: {
            autoDailyCandleSyncEnabled: options.autoDailyCandleSyncEnabled ?? false,
            autoDailyServerUpdateEnabled: false
          },
          expenseRatios: {
            etfBaseExpenseRatio: 0.001,
            inverseEtfExpenseRatio: 0.009,
            commodityTrustExpenseRatio: 0.004,
            bondEtfExpenseRatio: 0.001,
            incomeEtfExpenseRatio: 0.007,
            leveraged2xExpenseRatio: 0.009,
            leveraged3xExpenseRatio: 0.0095,
            leveraged5xExpenseRatio: 0.015
          },
          tickerRules: {
            alwaysValidationTickers: [],
            trainingAllocationRatio: 0.65
          }
        }
      },
      tickers: {
        syncTickersFromAssets: jest.fn().mockResolvedValue({ upserted: assets.length, disabled: 0 })
      }
    },
    alpacaAssetService: {
      fetchActiveEquityAssets: jest.fn().mockResolvedValue(assets)
    }
  } as any;
  const ctx = {
    job: {
      id: 'job-1',
      type: 'ticker-sync',
      status: 'running',
      scheduledFor: new Date('2025-01-02T14:00:00.000Z'),
      createdAt: new Date('2025-01-02T14:00:00.000Z'),
      attempts: 1,
      maxRetries: 5
    },
    abortSignal,
    loggingService: loggingService as any,
    scheduler: scheduler as any
  } as any;

  return {
    ctx,
    deps,
    scheduler
  };
}

describe('createTickerSyncHandler', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  test('syncs tickers and starts corporate actions sync when finished', async () => {
    const { ctx, deps, scheduler } = createHarness();

    const handler = createTickerSyncHandler(deps);
    const result = await handler(ctx);

    expect(deps.alpacaAssetService.fetchActiveEquityAssets).toHaveBeenCalledTimes(1);
    expect(deps.db.tickers.syncTickersFromAssets).toHaveBeenCalledTimes(1);
    expect(scheduler.scheduleJob).toHaveBeenCalledTimes(1);
    expect(scheduler.scheduleJob).toHaveBeenCalledWith('corporate-actions-sync', {
      description: 'Triggered by ticker synchronization update'
    });
    expect(result).toEqual({
      message: 'Ticker sync completed',
      meta: {
        assets: 1,
        upserted: 1,
        disabled: 0,
        corporateActionsSyncScheduled: true
      }
    });
  });

  test('does not queue a duplicate corporate actions sync when one is already pending', async () => {
    const { ctx, deps, scheduler } = createHarness({ corporateActionsSyncPending: true });

    const handler = createTickerSyncHandler(deps);
    const result = await handler(ctx);

    expect(deps.alpacaAssetService.fetchActiveEquityAssets).toHaveBeenCalledTimes(1);
    expect(deps.db.tickers.syncTickersFromAssets).toHaveBeenCalledTimes(1);
    expect(scheduler.scheduleJob).not.toHaveBeenCalled();
    expect(result).toEqual({
      message: 'Ticker sync completed',
      meta: {
        assets: 1,
        upserted: 1,
        disabled: 0,
        corporateActionsSyncScheduled: false
      }
    });
  });

  test('keeps Saturday ticker sync scheduled for Friday market data', async () => {
    jest.useFakeTimers().setSystemTime(new Date('2025-01-03T04:05:00.000Z'));
    const { ctx, deps, scheduler } = createHarness({ autoDailyCandleSyncEnabled: true });

    const handler = createTickerSyncHandler(deps);
    await handler(ctx);

    expect(scheduler.scheduleJob).toHaveBeenCalledWith('ticker-sync', {
      startAt: new Date('2025-01-04T03:59:00.000Z'),
      description: 'Post-close ticker sync pass',
      metadata: { trigger: 'daily' }
    });
  });

  test('skips Sunday and Monday early morning ticker sync scheduling', async () => {
    jest.useFakeTimers().setSystemTime(new Date('2025-01-04T04:05:00.000Z'));
    const { ctx, deps, scheduler } = createHarness({ autoDailyCandleSyncEnabled: true });

    const handler = createTickerSyncHandler(deps);
    await handler(ctx);

    expect(scheduler.scheduleJob).toHaveBeenCalledWith('ticker-sync', {
      startAt: new Date('2025-01-07T03:59:00.000Z'),
      description: 'Post-close ticker sync pass',
      metadata: { trigger: 'daily' }
    });
  });
});
