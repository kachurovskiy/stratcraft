import type { QueryValue } from '../core/DbClient';
import { DbClient } from '../core/DbClient';
import type { Candle } from '../../../shared/types/StrategyTemplate';
import { CandlesRepo } from './CandlesRepo';

describe('CandlesRepo', () => {
  test('upsertCandlesForTicker tolerates missing volume_shares', async () => {
    const formatter = new DbClient({} as any);
    const runCalls: Array<{ sql: string; params: QueryValue[] }> = [];

    const db = {
      withTransaction: async (callback: (client: unknown) => Promise<void>) => {
        await callback({});
      },
      run: async (sql: string, params: QueryValue[] = [], _executor?: unknown) => {
        formatter.formatQuery(sql, params);
        runCalls.push({ sql, params });
        return { rowCount: 1, changes: 1 };
      }
    } as any;

    const repo = new CandlesRepo(db);
    const candle = {
      ticker: 'BBCB',
      date: new Date('2026-02-05'),
      open: 1,
      high: 2,
      low: 0.5,
      close: 1.5,
      unadjustedClose: 1.5,
      volumeShares: undefined as unknown as number
    } satisfies Candle;

    await repo.upsertCandlesForTicker('BBCB', [candle]);

    expect(runCalls).toHaveLength(1);
    expect(runCalls[0].sql).toContain('COALESCE(?::bigint, 0)');
    expect(runCalls[0].sql).toContain('volume_shares = COALESCE(?::bigint, candles.volume_shares)');
    expect(runCalls[0].params).toHaveLength(9);
    expect(runCalls[0].params[7]).toBeNull();
    expect(runCalls[0].params[8]).toBeNull();
  });

  test('replaceCandlesForTicker tolerates missing volume_shares', async () => {
    const formatter = new DbClient({} as any);
    const runCalls: Array<{ sql: string; params: QueryValue[] }> = [];

    const db = {
      withTransaction: async (callback: (client: unknown) => Promise<void>) => {
        await callback({});
      },
      run: async (sql: string, params: QueryValue[] = [], _executor?: unknown) => {
        formatter.formatQuery(sql, params);
        runCalls.push({ sql, params });
        return { rowCount: 1, changes: 1 };
      }
    } as any;

    const repo = new CandlesRepo(db);
    const candle = {
      ticker: 'TAGS',
      date: new Date('2026-02-05'),
      open: 1,
      high: 2,
      low: 0.5,
      close: 1.5,
      unadjustedClose: 1.5,
      volumeShares: undefined as unknown as number
    } satisfies Candle;

    await repo.replaceCandlesForTicker('TAGS', [candle]);

    const insertCall = runCalls.find((call) => call.sql.includes('INSERT INTO candles'));
    expect(insertCall).toBeDefined();
    expect(insertCall!.sql).toContain('COALESCE(?::bigint, 0)');
    expect(insertCall!.sql).toContain('volume_shares = COALESCE(?::bigint, candles.volume_shares)');
    expect(insertCall!.params).toHaveLength(9);
    expect(insertCall!.params[7]).toBeNull();
    expect(insertCall!.params[8]).toBeNull();
  });
});
