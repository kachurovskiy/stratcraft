import type { BacktestResultRecord, AccountSignalSkipRow } from '../database/types';
import type { BacktestScope, Trade } from '../types/StrategyTemplate';
import { buildBacktestComparisonView } from './backtestComparison';

const STRATEGY_ID = 'strategy-1';
const USER_ID = 1;

const utcDate = (dateKey: string): Date => new Date(`${dateKey}T00:00:00.000Z`);
const dateKey = (date: Date): string => date.toISOString().slice(0, 10);

const createTrade = (overrides: Partial<Trade> = {}): Trade => ({
  id: 'trade-1',
  strategyId: STRATEGY_ID,
  userId: USER_ID,
  ticker: 'CETX',
  quantity: 100,
  price: 1,
  date: utcDate('2026-05-26'),
  status: 'active',
  createdAt: utcDate('2026-05-26'),
  changes: [],
  ...overrides
});

const createBacktest = (id: string, tickerScope: BacktestScope): BacktestResultRecord => ({
  id,
  strategyId: STRATEGY_ID,
  startDate: utcDate('2026-05-22'),
  endDate: utcDate('2026-05-28'),
  periodDays: 6,
  periodMonths: 0,
  initialCapital: 100000,
  finalPortfolioValue: 100000,
  performance: null,
  dailySnapshots: [],
  tickers: ['CETX'],
  tickerScope,
  createdAt: utcDate('2026-05-29')
});

const createSkip = (overrides: Partial<AccountSignalSkipRow> = {}): AccountSignalSkipRow => ({
  id: 1,
  strategy_id: STRATEGY_ID,
  account_id: null,
  ticker: 'CETX',
  signal_date: '2026-05-22',
  action: 'buy',
  source: 'backtest',
  reason: 'discount_not_reached',
  details: null,
  created_at: utcDate('2026-05-22'),
  ...overrides
});

describe('buildBacktestComparisonView', () => {
  test('matches Friday signal skips to Tuesday entries after a holiday weekend', async () => {
    const liveTrade = createTrade();
    const getSkips = jest.fn().mockResolvedValue([createSkip()]);
    const getTickersBySymbols = jest.fn().mockResolvedValue([]);
    const db = {
      trades: {
        getTrades: jest.fn()
          .mockResolvedValueOnce([])
          .mockResolvedValueOnce([liveTrade])
      },
      strategies: {
        getStrategy: jest.fn().mockResolvedValue({ parameters: {} })
      },
      accountSignalSkips: {
        getAccountSignalSkipsForStrategyInRange: getSkips
      },
      tickers: {
        getTickersBySymbols
      },
      settings: {
        value: {
          engine: {
            tradeSlippageRate: 0,
            limitBuyPenetrationRatio: 0
          }
        }
      }
    } as any;

    const view = await buildBacktestComparisonView({
      db,
      strategyId: STRATEGY_ID,
      userId: USER_ID,
      backtests: [
        createBacktest('engine-backtest', 'training'),
        createBacktest('live-backtest', 'live')
      ],
      isEligible: true
    });

    expect(getSkips).toHaveBeenCalledTimes(1);
    expect(getTickersBySymbols).toHaveBeenCalledWith(['CETX']);
    expect(view.hasComparisonData).toBe(true);
    expect(dateKey(getSkips.mock.calls[0][1])).toBe('2026-05-22');
    expect(view.sampleDays).toHaveLength(1);
    expect(view.sampleDays[0].rows[0].engine.reasons[0]).toMatchObject({
      label: 'Limit price not reached',
      detail: 'Skipped in Engine backtest | Candle low stayed above the limit price.'
    });
  });

  test('normalizes non-finite comparison settings', async () => {
    const db = {
      trades: {
        getTrades: jest.fn()
          .mockResolvedValueOnce([])
          .mockResolvedValueOnce([])
      },
      strategies: {
        getStrategy: jest.fn().mockResolvedValue({ parameters: {} })
      },
      accountSignalSkips: {
        getAccountSignalSkipsForStrategyInRange: jest.fn()
      },
      tickers: {
        getTickersBySymbols: jest.fn()
      },
      settings: {
        value: {
          engine: {
            tradeSlippageRate: Number.NaN,
            limitBuyPenetrationRatio: Number.POSITIVE_INFINITY
          }
        }
      }
    } as any;

    const view = await buildBacktestComparisonView({
      db,
      strategyId: STRATEGY_ID,
      userId: USER_ID,
      backtests: [
        createBacktest('engine-backtest', 'training'),
        createBacktest('live-backtest', 'live')
      ],
      isEligible: true
    });

    expect(view.hasComparisonData).toBe(true);
    expect(view.slippage?.setting).toBeNull();
    expect(view.penetration?.setting).toBeNull();
  });
});
