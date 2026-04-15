import type { Candle } from '../../types/StrategyTemplate';
import { createCandleSyncHandler } from './candleSyncHandler';

type HarnessOptions = {
  marketOpen?: boolean;
  hasExistingCandles?: boolean;
  tickers?: Array<{ symbol: string }>;
  tickerUpdates?: Record<string, Candle[]>;
  abortController?: AbortController;
  updateTickerDataImpl?: (symbol: string, includeToday?: boolean, abortSignal?: AbortSignal) => Promise<Candle[]>;
};

function createHarness(options: HarnessOptions = {}) {
  const loggingService = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  };
  const scheduler = {
    hasPendingJob: jest.fn().mockReturnValue(false),
    scheduleJob: jest.fn()
  };
  const abortController = options.abortController ?? new AbortController();
  const abortSignal = abortController.signal;
  const assets = [
    {
      symbol: 'AAA',
      name: 'Alpha Asset Inc.',
      tradable: true,
      shortable: true,
      easyToBorrow: true
    }
  ];
  const candleClient = {
    updateTickerData: jest.fn(async (symbol: string, includeToday?: boolean, signal?: AbortSignal) => {
      if (options.updateTickerDataImpl) {
        return options.updateTickerDataImpl(symbol, includeToday, signal);
      }
      return options.tickerUpdates?.[symbol] ?? [];
    }),
    drainNoDataTickers: jest.fn(() => []),
    getCandleSourceName: jest.fn(() => 'alpaca')
  };
  const engineCli = {
    run: jest.fn().mockResolvedValue(undefined)
  };
  const alpacaAssetService = {
    fetchActiveEquityAssets: jest.fn().mockResolvedValue(assets),
    fetchMarketClock: jest.fn().mockResolvedValue({
      isOpen: options.marketOpen ?? false,
      timestamp: new Date('2025-01-02T14:00:00.000Z'),
      nextOpen: new Date('2025-01-03T14:30:00.000Z'),
      nextClose: new Date('2025-01-02T21:00:00.000Z')
    })
  };
  const deps = {
    db: {
      settings: {
        value: {
          candleSync: {
            maxConcurrentUpdates: 2,
            matchingRatioThreshold: 0.98,
            autoDailyCandleSyncEnabled: false,
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
            trainingAllocationRatio: 0.65,
            ignoredTickers: []
          }
        }
      },
      tickers: {
        getTickers: jest.fn().mockResolvedValue(options.tickers ?? [{ symbol: 'AAA' }]),
        syncTickersFromAssets: jest.fn().mockResolvedValue({ upserted: assets.length, disabled: 0 })
      },
      candles: {
        getLatestGlobalCandleDate: jest
          .fn()
          .mockResolvedValue(options.hasExistingCandles === false ? null : new Date('2025-01-01T00:00:00.000Z')),
        getLastCandle: jest.fn().mockResolvedValue({ date: new Date('2025-01-01T00:00:00.000Z') }),
        getLastCandleDates: jest.fn().mockResolvedValue({})
      }
    },
    candleClient,
    engineCli,
    emailService: {},
    accountDataService: {},
    alpacaAssetService,
    strategyRegistry: {}
  } as any;
  const ctx = {
    job: {
      id: 'job-1',
      type: 'candle-sync',
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
    scheduler,
    engineCli,
    candleClient,
    alpacaAssetService,
    abortController
  };
}

describe('createCandleSyncHandler', () => {
  test('skips candle refresh during market hours without touching ticker sync', async () => {
    const { ctx, deps, candleClient, alpacaAssetService, engineCli } = createHarness({
      marketOpen: true,
      hasExistingCandles: true
    });

    const handler = createCandleSyncHandler(deps);
    const result = await handler(ctx);

    expect(alpacaAssetService.fetchActiveEquityAssets).not.toHaveBeenCalled();
    expect(deps.db.tickers.syncTickersFromAssets).not.toHaveBeenCalled();
    expect(candleClient.updateTickerData).not.toHaveBeenCalled();
    expect(engineCli.run).toHaveBeenCalledWith('export-market-data', [], ctx.abortSignal, { jobId: 'job-1' });
    expect(result).toEqual({ message: 'Candle sync skipped while market is open' });
  });

  test('refreshes tickers only once when SPY has new candles', async () => {
    const spyCandle: Candle = {
      ticker: 'SPY',
      date: new Date('2025-01-02T00:00:00.000Z'),
      open: 100,
      high: 101,
      low: 99,
      close: 100,
      volumeShares: 1_000
    };
    const aaaCandle: Candle = {
      ticker: 'AAA',
      date: new Date('2025-01-02T00:00:00.000Z'),
      open: 10,
      high: 11,
      low: 9,
      close: 10,
      volumeShares: 500
    };
    const { ctx, deps, candleClient, alpacaAssetService } = createHarness({
      tickerUpdates: {
        SPY: [spyCandle],
        AAA: [aaaCandle]
      }
    });

    const handler = createCandleSyncHandler(deps);
    const result = await handler(ctx);

    expect(alpacaAssetService.fetchActiveEquityAssets).not.toHaveBeenCalled();
    expect(deps.db.tickers.syncTickersFromAssets).not.toHaveBeenCalled();
    expect(candleClient.updateTickerData).toHaveBeenCalledTimes(2);
    expect(candleClient.updateTickerData).toHaveBeenNthCalledWith(1, 'SPY', true, ctx.abortSignal);
    expect(candleClient.updateTickerData).toHaveBeenNthCalledWith(2, 'AAA', true, ctx.abortSignal);
    expect(result).toEqual({
      message: 'Updated 2 tickers',
      meta: {
        totalTickers: 2,
        updatedTickers: 2,
        tickersToRefresh: 1,
        errorCount: 0
      }
    });
  });

  test('waits for in-flight ticker updates to settle before cancellation returns', async () => {
    const abortController = new AbortController();
    const spyCandle: Candle = {
      ticker: 'SPY',
      date: new Date('2025-01-02T00:00:00.000Z'),
      open: 100,
      high: 101,
      low: 99,
      close: 100,
      volumeShares: 1_000
    };
    const bbbCandle: Candle = {
      ticker: 'BBB',
      date: new Date('2025-01-02T00:00:00.000Z'),
      open: 20,
      high: 21,
      low: 19,
      close: 20,
      volumeShares: 750
    };

    let releaseBbbUpdate!: () => void;
    let resolveBothWorkersStarted: (() => void) | null = null;
    const bothWorkersStarted = new Promise<void>(resolve => {
      resolveBothWorkersStarted = resolve;
    });
    const startedWorkers = new Set<string>();
    const markWorkerStarted = (symbol: string) => {
      startedWorkers.add(symbol);
      if (startedWorkers.has('AAA') && startedWorkers.has('BBB')) {
        resolveBothWorkersStarted?.();
      }
    };

    const { ctx, deps, engineCli } = createHarness({
      abortController,
      tickers: [{ symbol: 'AAA' }, { symbol: 'BBB' }],
      updateTickerDataImpl: async (symbol: string, _includeToday, signal) => {
        if (symbol === 'SPY') {
          return [spyCandle];
        }

        if (symbol === 'AAA') {
          markWorkerStarted(symbol);
          if (startedWorkers.has('BBB')) {
            abortController.abort();
          }
          return new Promise<Candle[]>((_, reject) => {
            signal?.addEventListener(
              'abort',
              () => reject(new Error('Candle synchronization cancelled')),
              { once: true }
            );
          });
        }

        if (symbol === 'BBB') {
          markWorkerStarted(symbol);
          abortController.abort();
          return new Promise<Candle[]>(resolve => {
            releaseBbbUpdate = () => resolve([bbbCandle]);
          });
        }

        return [];
      }
    });

    const handler = createCandleSyncHandler(deps);
    let settled = false;
    const handlerPromise = handler(ctx).finally(() => {
      settled = true;
    });

    await bothWorkersStarted;
    await Promise.resolve();

    expect(settled).toBe(false);
    expect(engineCli.run).not.toHaveBeenCalled();

    releaseBbbUpdate();

    await expect(handlerPromise).rejects.toThrow('Candle synchronization cancelled');
    expect(engineCli.run).not.toHaveBeenCalled();
  });
});
