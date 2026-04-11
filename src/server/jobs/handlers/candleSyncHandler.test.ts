import type { Candle } from '../../types/StrategyTemplate';
import { createCandleSyncHandler } from './candleSyncHandler';

type HarnessOptions = {
  marketOpen?: boolean;
  hasExistingCandles?: boolean;
  tickers?: Array<{ symbol: string }>;
  tickerUpdates?: Record<string, Candle[]>;
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
  const candleClient = {
    updateTickerData: jest.fn(async (symbol: string) => options.tickerUpdates?.[symbol] ?? []),
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
            trainingAllocationRatio: 0.7,
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
    alpacaAssetService
  };
}

describe('createCandleSyncHandler', () => {
  test('refreshes tickers even when candle sync is skipped for an open market', async () => {
    const { ctx, deps, candleClient, alpacaAssetService, engineCli } = createHarness({
      marketOpen: true,
      hasExistingCandles: true
    });

    const handler = createCandleSyncHandler(deps);
    const result = await handler(ctx);

    expect(alpacaAssetService.fetchActiveEquityAssets).toHaveBeenCalledTimes(1);
    expect(deps.db.tickers.syncTickersFromAssets).toHaveBeenCalledTimes(1);
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

    expect(alpacaAssetService.fetchActiveEquityAssets).toHaveBeenCalledTimes(1);
    expect(deps.db.tickers.syncTickersFromAssets).toHaveBeenCalledTimes(1);
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
});
