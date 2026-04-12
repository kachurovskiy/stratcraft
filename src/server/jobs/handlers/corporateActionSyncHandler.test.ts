import { createCorporateActionSyncHandler } from './corporateActionSyncHandler';

type HarnessOptions = {
  firstTradeDate?: Date | null;
  tickers?: string[];
  latestProcessDate?: Date | null;
  candleSyncPending?: boolean;
};

function createHarness(options: HarnessOptions = {}) {
  const loggingService = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  };
  const scheduler = {
    hasPendingJob: jest.fn().mockImplementation((predicate: (job: any) => boolean) => {
      if (options.candleSyncPending) {
        return predicate({
          id: 'candle-job-1',
          type: 'candle-sync',
          status: 'queued',
          scheduledFor: new Date('2026-04-01T12:05:00.000Z'),
          createdAt: new Date('2026-04-01T12:00:00.000Z'),
          attempts: 0,
          maxRetries: 5
        });
      }

      return false;
    }),
    scheduleJob: jest.fn()
  };
  const abortSignal = new AbortController().signal;
  const actions = [
    {
      id: '5c60d6d0-b10e-4f72-b6bc-41f3067e1b96',
      actionType: 'cash_dividend',
      primarySymbol: 'AAPL',
      relatedSymbols: [],
      processDate: new Date('2026-03-20T00:00:00.000Z'),
      exDate: new Date('2026-03-20T00:00:00.000Z'),
      payload: {
        id: '5c60d6d0-b10e-4f72-b6bc-41f3067e1b96',
        symbol: 'AAPL'
      }
    }
  ];
  const deps = {
    db: {
      trades: {
        getAccountTradeCorporateActionScope: jest.fn().mockResolvedValue({
          firstTradeDate: options.firstTradeDate ?? new Date('2026-01-05T00:00:00.000Z'),
          tickers: options.tickers ?? ['AAPL', 'MSFT']
        })
      },
      corporateActions: {
        getLatestProcessDate: jest.fn().mockResolvedValue(options.latestProcessDate ?? null),
        upsertCorporateActions: jest.fn().mockResolvedValue({ upserted: actions.length })
      }
    },
    alpacaAssetService: {
      fetchCorporateActions: jest.fn().mockResolvedValue(actions)
    }
  } as any;
  const ctx = {
    job: {
      id: 'job-1',
      type: 'corporate-actions-sync',
      status: 'running',
      scheduledFor: new Date('2026-04-01T12:00:00.000Z'),
      createdAt: new Date('2026-04-01T12:00:00.000Z'),
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

describe('createCorporateActionSyncHandler', () => {
  test('skips sync when no account-tied trades exist and still starts candle sync', async () => {
    const { ctx, deps, scheduler } = createHarness({
      firstTradeDate: null,
      tickers: []
    });

    const handler = createCorporateActionSyncHandler(deps);
    const result = await handler(ctx);

    expect(deps.alpacaAssetService.fetchCorporateActions).not.toHaveBeenCalled();
    expect(deps.db.corporateActions.upsertCorporateActions).not.toHaveBeenCalled();
    expect(scheduler.scheduleJob).toHaveBeenCalledWith('candle-sync', {
      description: 'Triggered by corporate actions synchronization update'
    });
    expect(result).toEqual({
      message: 'No account-tied trades found',
      meta: {
        tickers: 0,
        fetched: 0,
        upserted: 0,
        candleSyncScheduled: true
      }
    });
  });

  test('uses the first account trade date when no prior corporate actions exist and starts candle sync', async () => {
    const firstTradeDate = new Date('2026-01-05T00:00:00.000Z');
    const { ctx, deps, scheduler } = createHarness({
      firstTradeDate,
      latestProcessDate: null
    });

    const handler = createCorporateActionSyncHandler(deps);
    const result = await handler(ctx);

    expect(deps.alpacaAssetService.fetchCorporateActions).toHaveBeenCalledWith({
      symbols: ['AAPL', 'MSFT'],
      startDate: firstTradeDate,
      endDate: expect.any(Date),
      abortSignal: ctx.abortSignal
    });
    expect(deps.db.corporateActions.upsertCorporateActions).toHaveBeenCalledTimes(1);
    expect(scheduler.scheduleJob).toHaveBeenCalledWith('candle-sync', {
      description: 'Triggered by corporate actions synchronization update'
    });
    expect(result).toMatchObject({
      message: 'Synchronized corporate actions',
      meta: {
        tickers: 2,
        fetched: 1,
        upserted: 1,
        candleSyncScheduled: true,
        startDate: '2026-01-05'
      }
    });
  });

  test('does not queue a duplicate candle sync when one is already pending', async () => {
    const { ctx, deps, scheduler } = createHarness({
      candleSyncPending: true
    });

    const handler = createCorporateActionSyncHandler(deps);
    const result = await handler(ctx);

    expect(scheduler.scheduleJob).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      message: 'Synchronized corporate actions',
      meta: {
        candleSyncScheduled: false
      }
    });
  });

  test('uses the latest known process date when it is later than the first trade', async () => {
    const firstTradeDate = new Date('2026-01-05T00:00:00.000Z');
    const latestProcessDate = new Date('2026-03-10T00:00:00.000Z');
    const { ctx, deps } = createHarness({
      firstTradeDate,
      latestProcessDate
    });

    const handler = createCorporateActionSyncHandler(deps);
    await handler(ctx);

    expect(deps.alpacaAssetService.fetchCorporateActions).toHaveBeenCalledWith({
      symbols: ['AAPL', 'MSFT'],
      startDate: latestProcessDate,
      endDate: expect.any(Date),
      abortSignal: ctx.abortSignal
    });
  });
});
