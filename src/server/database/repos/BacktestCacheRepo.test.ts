import type { QueryValue } from '../core/DbClient';
import { DbClient } from '../core/DbClient';
import { createDefaultSettingsValue } from '../../settings/defaults';
import {
  BacktestCacheRepo,
  DEFAULT_BACKTEST_CACHE_PARAMETER_DIFFERENCE_THRESHOLD
} from './BacktestCacheRepo';

type CacheRowInput = {
  id: string;
  parameters: Record<string, unknown>;
  sharpeRatio?: number;
  calmarRatio?: number;
  totalReturn?: number;
};

const createSettings = () => {
  const value = createDefaultSettingsValue();
  value.paramScoring.minTrades = 0;
  value.paramScoring.stabilityGamma = 0;
  return { value } as any;
};

const createCacheRow = ({
  id,
  parameters,
  sharpeRatio = 1,
  calmarRatio = 1,
  totalReturn = 0.1
}: CacheRowInput) => ({
  id,
  template_id: 'template-a',
  parameters: JSON.stringify(parameters),
  sharpe_ratio: sharpeRatio,
  calmar_ratio: calmarRatio,
  total_return: totalReturn,
  cagr: totalReturn,
  max_drawdown: 0,
  max_drawdown_ratio: 0,
  win_rate: 0.5,
  total_trades: 50
});

describe('BacktestCacheRepo', () => {
  test('pruneBacktestCacheByBestParams deletes entries at or above the parameter difference threshold', async () => {
    const formatter = new DbClient({} as any);
    const rows = [
      createCacheRow({
        id: 'best',
        parameters: { a: 1, b: 1, c: 1, d: 1, e: 1 },
        sharpeRatio: 10,
        calmarRatio: 10,
        totalReturn: 1
      }),
      createCacheRow({
        id: 'close',
        parameters: { a: 2, b: 1, c: 2, d: 1, e: 1 }
      }),
      createCacheRow({
        id: 'far',
        parameters: { a: 2, b: 2, c: 2, d: 2, e: 1 }
      }),
      createCacheRow({
        id: 'missing',
        parameters: { a: 1 }
      })
    ];
    const runCalls: Array<{ sql: string; params: QueryValue[] }> = [];
    const db = {
      all: async (sql: string, params: QueryValue[] = []) => {
        formatter.formatQuery(sql, params);
        return rows;
      },
      run: async (sql: string, params: QueryValue[] = []) => {
        formatter.formatQuery(sql, params);
        runCalls.push({ sql, params });
        return { rowCount: (params[1] as string[]).length, changes: (params[1] as string[]).length };
      }
    } as any;

    const repo = new BacktestCacheRepo(db, createSettings());

    const result = await repo.pruneBacktestCacheByBestParams('template-a', 3);

    expect(result).toEqual({
      deleted: 2,
      scanned: 4,
      threshold: 3,
      bestFound: true
    });
    expect(runCalls).toHaveLength(1);
    expect(runCalls[0].sql).toContain('DELETE FROM backtest_cache');
    expect(runCalls[0].params).toEqual(['template-a', ['far', 'missing']]);
  });

  test('pruneBacktestCacheByBestParams defaults to five parameter differences', async () => {
    const formatter = new DbClient({} as any);
    const rows = [
      createCacheRow({
        id: 'best',
        parameters: { a: 1, b: 1, c: 1, d: 1, e: 1, f: 1 },
        sharpeRatio: 10,
        calmarRatio: 10,
        totalReturn: 1
      }),
      createCacheRow({
        id: 'four-different',
        parameters: { a: 2, b: 2, c: 2, d: 2, e: 1, f: 1 }
      }),
      createCacheRow({
        id: 'five-different',
        parameters: { a: 2, b: 2, c: 2, d: 2, e: 2, f: 1 }
      })
    ];
    const runCalls: Array<{ sql: string; params: QueryValue[] }> = [];
    const db = {
      all: async (sql: string, params: QueryValue[] = []) => {
        formatter.formatQuery(sql, params);
        return rows;
      },
      run: async (sql: string, params: QueryValue[] = []) => {
        formatter.formatQuery(sql, params);
        runCalls.push({ sql, params });
        return { rowCount: (params[1] as string[]).length, changes: (params[1] as string[]).length };
      }
    } as any;

    const repo = new BacktestCacheRepo(db, createSettings());

    const result = await repo.pruneBacktestCacheByBestParams('template-a');

    expect(result.threshold).toBe(DEFAULT_BACKTEST_CACHE_PARAMETER_DIFFERENCE_THRESHOLD);
    expect(result.deleted).toBe(1);
    expect(runCalls[0].params).toEqual(['template-a', ['five-different']]);
  });
});
