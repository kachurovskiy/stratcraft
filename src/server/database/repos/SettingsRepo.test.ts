import type { QueryValue } from '../core/DbClient';
import { SettingsRepo } from './SettingsRepo';
import { SETTING_KEYS } from '../../constants';

describe('SettingsRepo', () => {
  test('initialize hydrates the typed value snapshot', async () => {
    const db = {
      all: jest.fn().mockResolvedValue([
        { setting_key: SETTING_KEYS.SITE_NAME, value: '  CustomCraft  ' },
        { setting_key: SETTING_KEYS.TRADINGVIEW_CHARTS_ENABLED, value: 'false' },
        { setting_key: SETTING_KEYS.CANDLE_DATA_PROVIDER, value: 'alpaca' },
        { setting_key: SETTING_KEYS.CANDLE_SYNC_MAX_CONCURRENT_UPDATES, value: '8' },
        { setting_key: SETTING_KEYS.ALWAYS_VALIDATION_TICKERS, value: 'spy, qqq, tlt' },
        { setting_key: SETTING_KEYS.BACKTEST_ACTIVE_MONTHS, value: '[1, 6, 12]' },
        { setting_key: SETTING_KEYS.PARAM_SCORE_STABILITY_GAMMA, value: '3' },
        { setting_key: SETTING_KEYS.OPTIMIZATION_OBJECTIVE, value: 'cagr' },
        { setting_key: SETTING_KEYS.LOCAL_OPTIMIZATION_STEP_MULTIPLIERS, value: '-2,-1,1,2' },
        { setting_key: SETTING_KEYS.HETZNER_SSH_KEY_NAME, value: '  hetzner-node  ' }
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
    expect(repo.value.paramScoring.stabilityGamma).toBe(3);
    expect(repo.value.optimizer.optimizationObjective).toBe('CAGR');
    expect(repo.value.optimizer.localOptimizationStepMultipliers).toEqual([-2, -1, 1, 2]);
    expect(repo.value.optimizer.hetznerSshKeyName).toBe('hetzner-node');
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
      [SETTING_KEYS.BACKTEST_ACTIVE_MONTHS]: '3,12',
      [SETTING_KEYS.SITE_NAME]: '  StratCraft Pro  '
    });

    expect(runCalls).toHaveLength(4);
    expect(repo.value.candleSync.autoDailyCandleSyncEnabled).toBe(false);
    expect(repo.value.tickerRules.ignoredTickers).toEqual(['SPY', 'QQQ']);
    expect(repo.value.engine.backtestActiveMonths).toEqual([3, 12]);
    expect(repo.value.app.siteName).toBe('StratCraft Pro');
    expect(runCalls[3]?.params[1]).toBe('StratCraft Pro');
  });

  test('normalizes invalid raw settings into safe typed values', async () => {
    const db = {
      all: jest.fn().mockResolvedValue([
        { setting_key: SETTING_KEYS.SITE_NAME, value: '   ' },
        { setting_key: SETTING_KEYS.DOMAIN, value: 'https://example.com/app' },
        { setting_key: SETTING_KEYS.ALPACA_PAPER_URL, value: 'not-a-url' },
        { setting_key: SETTING_KEYS.TRADE_ENTRY_PRICE_MIN, value: '-5' },
        { setting_key: SETTING_KEYS.TRADE_ENTRY_PRICE_MAX, value: '0' },
        { setting_key: SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_LOOKBACK, value: '2.9' },
        { setting_key: SETTING_KEYS.MAX_TICKER_FLUCTUATION_RATIO, value: '-1' },
        { setting_key: SETTING_KEYS.BACKTEST_ACTIVE_MONTHS, value: '[0, -1, \"bad\"]' },
        { setting_key: SETTING_KEYS.TRAINING_ALLOCATION_RATIO, value: '1.8' },
        { setting_key: SETTING_KEYS.CANDLE_SYNC_MATCHING_RATIO_THRESHOLD, value: '-4' },
        { setting_key: SETTING_KEYS.LIGHTGBM_TRAINING_START_DATE, value: '2024-99-99' },
        { setting_key: SETTING_KEYS.HETZNER_SERVER_TYPE, value: '   ' },
        { setting_key: SETTING_KEYS.INVITE_LINK_VALID_DAYS, value: '0' },
        { setting_key: SETTING_KEYS.SESSION_COOKIE_VALID_DAYS, value: '3.9' },
        { setting_key: SETTING_KEYS.MTLS_ACCESS_CERT_PASSWORD, value: '   ' }
      ]),
      get: jest.fn(),
      run: jest.fn(),
      withTransaction: jest.fn()
    } as any;

    const repo = new SettingsRepo(db);

    await repo.initialize();

    expect(repo.value.app.siteName).toBe('StratCraft');
    expect(repo.value.app.domain).toBe('');
    expect(repo.value.alpaca.paperUrl).toBe('https://paper-api.alpaca.markets/v2');
    expect(repo.value.engine.tradeEntryPriceMin).toBe(0);
    expect(repo.value.engine.tradeEntryPriceMax).toBe(Number.POSITIVE_INFINITY);
    expect(repo.value.engine.minimumDollarVolumeLookback).toBe(2);
    expect(repo.value.engine.maxTickerFluctuationRatio).toBe(Number.POSITIVE_INFINITY);
    expect(repo.value.engine.backtestActiveMonths).toEqual([1, 3, 6, 12, 24, 36, 48, 60, 120]);
    expect(repo.value.tickerRules.trainingAllocationRatio).toBe(1);
    expect(repo.value.candleSync.matchingRatioThreshold).toBe(0);
    expect(repo.value.optimizer.lightgbmTrainingStartDate).toBe('2021-01-01');
    expect(repo.value.optimizer.hetznerServerType).toBe('cpx62');
    expect(repo.value.userAccess.inviteLinkValidDays).toBe(7);
    expect(repo.value.userAccess.sessionCookieValidDays).toBe(3);
    expect(repo.value.userAccess.mtlsAccessCertPassword).toBe('stratcraft');
  });
});
