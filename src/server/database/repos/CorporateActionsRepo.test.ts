import type { QueryValue } from '../core/DbClient';
import { DbClient } from '../core/DbClient';
import type { CorporateActionRecord } from '../types';
import { CorporateActionsRepo } from './CorporateActionsRepo';

describe('CorporateActionsRepo', () => {
  test('upsertCorporateActions includes payload placeholders for batch inserts', async () => {
    const formatter = new DbClient({} as any);
    const runCalls: Array<{ sql: string; params: QueryValue[] }> = [];

    const db = {
      withTransaction: async <T>(callback: (client: unknown) => Promise<T>) => {
        return callback({});
      },
      run: async (sql: string, params: QueryValue[] = [], _executor?: unknown) => {
        formatter.formatQuery(sql, params);
        runCalls.push({ sql, params });
        return { rowCount: 2, changes: 2 };
      }
    } as any;

    const repo = new CorporateActionsRepo(db);
    const actions: CorporateActionRecord[] = [
      {
        id: '4c1e1d2e-0c1b-4a9a-92d8-fc5d5a6bcb11',
        actionType: 'name_change',
        primarySymbol: 'FB',
        relatedSymbols: ['META'],
        processDate: new Date('2026-04-11'),
        effectiveDate: new Date('2026-04-10'),
        exDate: null,
        recordDate: null,
        payableDate: null,
        payload: { oldSymbol: 'FB', newSymbol: 'META' }
      },
      {
        id: '6d5f08e6-e4ea-4dbe-9d18-4f8dd4f55a5d',
        actionType: 'cash_dividend',
        primarySymbol: 'META',
        relatedSymbols: [],
        processDate: new Date('2026-04-11'),
        effectiveDate: null,
        exDate: new Date('2026-04-09'),
        recordDate: new Date('2026-04-10'),
        payableDate: new Date('2026-04-12'),
        payload: { cash: 0.5 }
      }
    ];

    await expect(repo.upsertCorporateActions(actions)).resolves.toEqual({ upserted: 2 });

    expect(runCalls).toHaveLength(1);
    expect(runCalls[0].sql).toContain(
      '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)'
    );
    expect(runCalls[0].params).toHaveLength(20);
    expect(runCalls[0].params[9]).toEqual({ oldSymbol: 'FB', newSymbol: 'META' });
    expect(runCalls[0].params[19]).toEqual({ cash: 0.5 });
  });
});
