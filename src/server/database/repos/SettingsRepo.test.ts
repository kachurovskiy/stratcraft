import type { QueryValue } from '../core/DbClient';
import { SettingsRepo } from './SettingsRepo';
import { SETTING_KEYS } from '../../constants';

describe('SettingsRepo', () => {
  test('initialize hydrates the typed value snapshot', async () => {
    const db = {
      all: jest.fn().mockResolvedValue([
        { setting_key: SETTING_KEYS.SITE_NAME, value: 'CustomCraft' },
        { setting_key: SETTING_KEYS.TRADINGVIEW_CHARTS_ENABLED, value: 'false' },
        { setting_key: SETTING_KEYS.CANDLE_DATA_PROVIDER, value: 'alpaca' },
        { setting_key: SETTING_KEYS.CANDLE_SYNC_MAX_CONCURRENT_UPDATES, value: '8' },
        { setting_key: SETTING_KEYS.ALWAYS_VALIDATION_TICKERS, value: 'spy, qqq, tlt' },
        { setting_key: SETTING_KEYS.BACKTEST_ACTIVE_MONTHS, value: '[1, 6, 12]' },
        { setting_key: SETTING_KEYS.OPTIMIZATION_OBJECTIVE, value: 'cagr' },
        { setting_key: SETTING_KEYS.LOCAL_OPTIMIZATION_STEP_MULTIPLIERS, value: '-2,-1,1,2' }
      ]),
      get: jest.fn(),
      run: jest.fn(),
      withTransaction: jest.fn()
    } as any;

    const repo = new SettingsRepo(db);

    await repo.initialize();

    expect(repo.value.app.siteName).toBe('CustomCraft');
    expect(repo.value.app.tradingViewChartsEnabled).toBe(false);
    expect(repo.value.dataProvider.candleDataProvider).toBe('ALPACA');
    expect(repo.value.candleSync.maxConcurrentUpdates).toBe(8);
    expect(repo.value.tickerRules.alwaysValidationTickers).toEqual(['SPY', 'QQQ', 'TLT']);
    expect(repo.value.engine.backtestActiveMonths).toEqual([1, 6, 12]);
    expect(repo.value.optimizer.optimizationObjective).toBe('CAGR');
    expect(repo.value.optimizer.localOptimizationStepMultipliers).toEqual([-2, -1, 1, 2]);
    expect(repo.value.userAccess.sessionCookieValidDays).toBe(30);
  });

  test('upsertSettings updates the typed value snapshot after writes', async () => {
    const runCalls: Array<{ sql: string; params: QueryValue[] }> = [];
    const db = {
      all: jest.fn().mockResolvedValue([]),
      get: jest.fn(),
      withTransaction: async (callback: (client: unknown) => Promise<void>) => {
        await callback({});
      },
      run: async (sql: string, params: QueryValue[] = []) => {
        runCalls.push({ sql, params });
        return { rowCount: 1, changes: 1 };
      }
    } as any;

    const repo = new SettingsRepo(db);
    await repo.initialize();

    await repo.upsertSettings({
      [SETTING_KEYS.AUTO_DAILY_CANDLE_SYNC_ENABLED]: 'false',
      [SETTING_KEYS.IGNORED_TICKERS]: 'spy, qqq',
      [SETTING_KEYS.BACKTEST_ACTIVE_MONTHS]: '3,12'
    });

    expect(runCalls).toHaveLength(3);
    expect(repo.value.candleSync.autoDailyCandleSyncEnabled).toBe(false);
    expect(repo.value.tickerRules.ignoredTickers).toEqual(['SPY', 'QQQ']);
    expect(repo.value.engine.backtestActiveMonths).toEqual([3, 12]);
  });
});
