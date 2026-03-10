import type { Database } from '../database/Database';
import type { BacktestResultRecord, AccountSignalSkipRow } from '../database/types';
import type { Trade } from '../../shared/types/StrategyTemplate';
import { buildBacktestComparisonView } from './backtestComparison';

const buildBacktest = (
  id: string,
  tickerScope: BacktestResultRecord['tickerScope']
): BacktestResultRecord => ({
  id,
  strategyId: 'strategy-1',
  startDate: new Date('2026-03-06T00:00:00Z'),
  endDate: new Date('2026-03-09T00:00:00Z'),
  periodDays: 3,
  periodMonths: 0,
  initialCapital: 100000,
  finalPortfolioValue: 100100,
  performance: null,
  dailySnapshots: [],
  tickers: ['BANL'],
  tickerScope,
  createdAt: new Date('2026-03-10T00:00:00Z')
});

const buildTrade = (overrides: Partial<Trade> = {}): Trade => ({
  id: 'trade-1',
  strategyId: 'strategy-1',
  userId: 1,
  backtestResultId: 'engine-backtest',
  ticker: 'BANL',
  quantity: 1167,
  price: 0.63,
  date: new Date('2026-03-09T00:00:00Z'),
  status: 'active',
  createdAt: new Date('2026-03-10T00:00:00Z'),
  changes: [],
  ...overrides
});

describe('buildBacktestComparisonView', () => {
  it('matches operation-planning skips across weekend gaps', async () => {
    const engineBacktest = buildBacktest('engine-backtest', 'all');
    const liveBacktest = buildBacktest('live-backtest', 'live');
    const engineTrade = buildTrade();
    const skips: AccountSignalSkipRow[] = [
      {
        id: 1,
        strategy_id: 'strategy-1',
        account_id: 'account-1',
        ticker: 'BANL',
        signal_date: '2026-03-06',
        action: 'buy',
        source: 'plan_operations',
        reason: 'operation_requested',
        details: 'buy_signal_sync',
        created_at: new Date('2026-03-08T04:33:00Z')
      }
    ];

    const db = {
      settings: {
        getSettingValue: jest.fn().mockResolvedValue('0.01')
      },
      trades: {
        getTrades: jest.fn().mockImplementation(async (_strategyId, _daysBack, _ticker, _limit, _status, backtestId) => {
          if (backtestId === engineBacktest.id) {
            return [engineTrade];
          }
          return [];
        })
      },
      accountSignalSkips: {
        getAccountSignalSkipsForStrategyInRange: jest.fn().mockResolvedValue(skips)
      },
      tickers: {
        getTicker: jest.fn().mockResolvedValue({ expenseRatio: null })
      }
    } as unknown as Database;

    const comparison = await buildBacktestComparisonView({
      db,
      strategyId: 'strategy-1',
      userId: 1,
      backtests: [engineBacktest, liveBacktest],
      isEligible: true
    });

    expect(comparison.sampleDays).toHaveLength(1);
    expect(comparison.sampleDays[0]?.rows).toHaveLength(1);

    const liveReasons = comparison.sampleDays[0]?.rows[0]?.live.reasons ?? [];
    expect(liveReasons).toHaveLength(1);
    expect(liveReasons[0]?.label).toBe('operation_requested');
    expect(liveReasons[0]?.detail).toContain('Skipped in Operation planning');
    expect(liveReasons[0]?.detail).toContain('buy_signal_sync');
  });
});
