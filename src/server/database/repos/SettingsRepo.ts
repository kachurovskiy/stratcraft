import type { PoolClient, QueryResultRow } from 'pg';
import {
  DEFAULT_AUTO_OPTIMIZATION_DELAY_SECONDS,
  DEFAULT_BACKTEST_INITIAL_CAPITAL,
  DEFAULT_LIQUIDATION_DEVIATION_BAND_PERCENT,
  DEFAULT_LIQUIDATION_DISCOUNT_PERCENT,
  DEFAULT_MARKET_ORDER_PRICE_CAP_RATIO,
  DEFAULT_MTLS_ACCESS_CERT_PASSWORD,
  isSensitiveSettingKey,
  SETTING_KEY_LIST,
  SETTING_KEYS,
  type SettingKey
} from '../../constants';
import { DEFAULT_FOOTER_DISCLAIMER_HTML } from '../../utils/footerDisclaimer';
import { decryptValue, encryptValue } from '../../utils/encryption';
import { normalizeUppercaseString, normalizeUppercaseStrings } from '../../utils/stringNormalization';
import { DbClient } from '../core/DbClient';
import type { SettingsCandleDataProvider, SettingsOptimizationObjective, SettingsValue } from '../types';

const SETTINGS_CACHE_TTL_MS = 1_000;
const KNOWN_SETTING_KEYS = new Set<SettingKey>(SETTING_KEY_LIST);
const DEFAULT_CANDLE_DATA_PROVIDER: SettingsCandleDataProvider = 'TIINGO';
const DEFAULT_OPTIMIZATION_OBJECTIVE: SettingsOptimizationObjective = 'SHARPE';

type CachedSetting = { value: string | null; loadedAt: number };

type SettingValueRow = QueryResultRow & { value: string | null };
type SettingRow = QueryResultRow & { setting_key: string; value: string | null };

const createEmptySettingsRecord = (): Record<SettingKey, string | null> => {
  const settings = {} as Record<SettingKey, string | null>;
  for (const settingKey of SETTING_KEY_LIST) {
    settings[settingKey] = null;
  }
  return settings;
};

const createDefaultSettingsValue = (): SettingsValue => ({
  app: {
    siteName: 'StratCraft',
    domain: '',
    footerDisclaimerHtml: DEFAULT_FOOTER_DISCLAIMER_HTML,
    tradingViewChartsEnabled: true
  },
  dataProvider: {
    candleDataProvider: DEFAULT_CANDLE_DATA_PROVIDER
  },
  alpaca: {
    paperUrl: 'https://paper-api.alpaca.markets/v2',
    liveUrl: 'https://api.alpaca.markets/v2',
    dataBaseUrl: 'https://data.alpaca.markets/v2',
    apiKey: '',
    apiSecret: '',
    dataRateLimitWaitSeconds: 60,
    marketOrderPriceCapRatio: DEFAULT_MARKET_ORDER_PRICE_CAP_RATIO,
    accountLiquidationDiscountPercent: DEFAULT_LIQUIDATION_DISCOUNT_PERCENT,
    accountLiquidationDeviationBandPercent: DEFAULT_LIQUIDATION_DEVIATION_BAND_PERCENT
  },
  eodhd: {
    baseUrl: 'https://eodhd.com/api/eod',
    apiToken: '',
    rateLimitWaitSeconds: 60
  },
  tiingo: {
    baseUrl: 'https://api.tiingo.com/tiingo/daily',
    apiToken: '',
    rateLimitWaitSeconds: 60
  },
  candleSync: {
    candleMismatchThreshold: 0.01,
    maxConcurrentUpdates: 5,
    matchingRatioThreshold: 0.98,
    autoDailyCandleSyncEnabled: true,
    autoDailyServerUpdateEnabled: false
  },
  expenseRatios: {
    etfBaseExpenseRatio: 0.0008,
    inverseEtfExpenseRatio: 0.009,
    commodityTrustExpenseRatio: 0.004,
    bondEtfExpenseRatio: 0.001,
    incomeEtfExpenseRatio: 0.007,
    leveraged2xExpenseRatio: 0.009,
    leveraged3xExpenseRatio: 0.0095,
    leveraged5xExpenseRatio: 0.015
  },
  engine: {
    tradeCloseFeeRate: 0.0005,
    tradeSlippageRate: 0.02,
    limitBuyPenetrationRatio: 0.05,
    shortBorrowFeeAnnualRate: 0.003,
    tradeEntryPriceMin: 0.1,
    tradeEntryPriceMax: 1000,
    minimumDollarVolumeForEntry: 150_000,
    minimumDollarVolumeLookback: 5,
    minTickerFluctuationRatio: 0.03,
    maxTickerFluctuationRatio: 10,
    backtestActiveMonths: [1, 3, 6, 12, 24, 36, 48, 60, 120],
    backtestInitialCapital: DEFAULT_BACKTEST_INITIAL_CAPITAL
  },
  tickerRules: {
    ignoredTickers: [],
    alwaysValidationTickers: ['SPY', 'QQQ'],
    trainingAllocationRatio: 0.7
  },
  optimizer: {
    autoOptimizationEnabled: true,
    autoOptimizationDelaySeconds: DEFAULT_AUTO_OPTIMIZATION_DELAY_SECONDS,
    allowShortSellingOptimizationEnabled: false,
    lightgbmTrainingStartDate: '2021-01-01',
    lightgbmTrainingEndDate: '2024-12-31',
    optimizerTrainingStartDate: '2021-01-01',
    optimizerTrainingEndDate: '2024-12-31',
    verifyWindowStartDate: '2025-01-01',
    verifyWindowEndDate: '2025-12-31',
    balanceWindowStartDate: '2023-01-01',
    balanceWindowEndDate: '2025-12-31',
    localOptimizationVersion: 9,
    localOptimizationMultiStartSeeds: 0,
    optimizationObjective: DEFAULT_OPTIMIZATION_OBJECTIVE,
    hetznerApiToken: '',
    hetznerServerType: 'cpx62',
    hetznerServerLocation: 'hel1',
    hetznerSshKeyName: 'hetzner-node',
    hetznerPublicKey: '',
    hetznerPrivateKey: '',
    localOptimizationStepMultipliers: [-4, -3, -2, -1, 1, 2, 3, 4],
    localOptimizationMaxUnadjustedPriceValues: [3, 5, 7, 10, 15, 20],
    maxAllowedDrawdownRatio: 0.3,
    backtestApiSecret: ''
  },
  paramScoring: {
    minTrades: 20,
    drawdownLambda: 3.5,
    neighborThreshold: 0.15,
    coreScoreQuantile: 0.6,
    pairwiseNeighborLimit: 1500
  },
  templateScoring: {
    returnScale: 0.2,
    validationNegativePenaltyStrength: 2,
    drawdownLambda: 2.5,
    tradeTarget: 200,
    tradeWeight: 0.25,
    recencyHalfLifeDays: 365,
    verifySharpeScale: 2,
    verifyCalmarScale: 2,
    verifyCagrScale: 0.25,
    verifyCagrNegativeScale: 0.1,
    verifyDrawdownLambda: 2.5,
    verifyMinMultiplier: 0.8,
    verifyMaxMultiplier: 1.2
  },
  userAccess: {
    inviteLinkValidDays: 7,
    sessionCookieValidDays: 30,
    mtlsAccessCertPassword: DEFAULT_MTLS_ACCESS_CERT_PASSWORD
  },
  email: {
    emailSecurityEmoji: '',
    resendApiKey: ''
  }
});

export class SettingsRepo {
  private cache = new Map<string, CachedSetting>();
  private rawSettings = createEmptySettingsRecord();
  private valueInitialized = false;

  readonly value: SettingsValue = createDefaultSettingsValue();

  constructor(private readonly db: DbClient) {}

  async initialize(): Promise<void> {
    if (this.valueInitialized) {
      return;
    }

    await this.getSettingsByKeys(SETTING_KEY_LIST);
    this.valueInitialized = true;
  }

  private readFromCache(settingKey: string): string | null | undefined {
    const cached = this.cache.get(settingKey);
    if (!cached) {
      return undefined;
    }
    if (Date.now() - cached.loadedAt > SETTINGS_CACHE_TTL_MS) {
      this.cache.delete(settingKey);
      return undefined;
    }
    return cached.value;
  }

  private cacheValue(settingKey: string, value: string | null): void {
    this.cache.set(settingKey, { value, loadedAt: Date.now() });
  }

  private isKnownSettingKey(settingKey: string): settingKey is SettingKey {
    return KNOWN_SETTING_KEYS.has(settingKey as SettingKey);
  }

  private applyKnownSettings(settings: Partial<Record<SettingKey, string | null>>): void {
    let changed = false;
    for (const [settingKey, value] of Object.entries(settings)) {
      if (!this.isKnownSettingKey(settingKey)) {
        continue;
      }
      if (this.rawSettings[settingKey] === value) {
        continue;
      }
      this.rawSettings[settingKey] = value ?? null;
      changed = true;
    }

    if (changed) {
      this.rebuildValue();
    }
  }

  private rebuildValue(): void {
    const next = this.buildValue();
    Object.assign(this.value.app, next.app);
    Object.assign(this.value.dataProvider, next.dataProvider);
    Object.assign(this.value.alpaca, next.alpaca);
    Object.assign(this.value.eodhd, next.eodhd);
    Object.assign(this.value.tiingo, next.tiingo);
    Object.assign(this.value.candleSync, next.candleSync);
    Object.assign(this.value.expenseRatios, next.expenseRatios);
    Object.assign(this.value.engine, next.engine);
    Object.assign(this.value.tickerRules, next.tickerRules);
    Object.assign(this.value.optimizer, next.optimizer);
    Object.assign(this.value.paramScoring, next.paramScoring);
    Object.assign(this.value.templateScoring, next.templateScoring);
    Object.assign(this.value.userAccess, next.userAccess);
    Object.assign(this.value.email, next.email);
  }

  private buildValue(): SettingsValue {
    const defaults = createDefaultSettingsValue();

    return {
      app: this.buildAppValue(defaults),
      dataProvider: this.buildDataProviderValue(),
      alpaca: this.buildAlpacaValue(defaults),
      eodhd: this.buildEodhdValue(defaults),
      tiingo: this.buildTiingoValue(defaults),
      candleSync: this.buildCandleSyncValue(defaults),
      expenseRatios: this.buildExpenseRatiosValue(defaults),
      engine: this.buildEngineValue(defaults),
      tickerRules: this.buildTickerRulesValue(defaults),
      optimizer: this.buildOptimizerValue(defaults),
      paramScoring: this.buildParamScoringValue(defaults),
      templateScoring: this.buildTemplateScoringValue(defaults),
      userAccess: this.buildUserAccessValue(defaults),
      email: this.buildEmailValue(defaults)
    };
  }

  private buildAppValue(defaults: SettingsValue): SettingsValue['app'] {
    return {
      siteName: this.parseString(this.rawSettings[SETTING_KEYS.SITE_NAME], defaults.app.siteName),
      domain: this.parseString(this.rawSettings[SETTING_KEYS.DOMAIN], defaults.app.domain),
      footerDisclaimerHtml: this.parseString(
        this.rawSettings[SETTING_KEYS.FOOTER_DISCLAIMER_HTML],
        defaults.app.footerDisclaimerHtml
      ),
      tradingViewChartsEnabled: this.parseBoolean(
        this.rawSettings[SETTING_KEYS.TRADINGVIEW_CHARTS_ENABLED],
        defaults.app.tradingViewChartsEnabled
      )
    };
  }

  private buildDataProviderValue(): SettingsValue['dataProvider'] {
    return {
      candleDataProvider: this.parseCandleDataProvider(this.rawSettings[SETTING_KEYS.CANDLE_DATA_PROVIDER])
    };
  }

  private buildAlpacaValue(defaults: SettingsValue): SettingsValue['alpaca'] {
    return {
      paperUrl: this.parseString(this.rawSettings[SETTING_KEYS.ALPACA_PAPER_URL], defaults.alpaca.paperUrl),
      liveUrl: this.parseString(this.rawSettings[SETTING_KEYS.ALPACA_LIVE_URL], defaults.alpaca.liveUrl),
      dataBaseUrl: this.parseString(this.rawSettings[SETTING_KEYS.ALPACA_DATA_BASE_URL], defaults.alpaca.dataBaseUrl),
      apiKey: this.parseString(this.rawSettings[SETTING_KEYS.ALPACA_API_KEY], defaults.alpaca.apiKey),
      apiSecret: this.parseString(this.rawSettings[SETTING_KEYS.ALPACA_API_SECRET], defaults.alpaca.apiSecret),
      dataRateLimitWaitSeconds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.ALPACA_DATA_RATE_LIMIT_WAIT_SECONDS],
        defaults.alpaca.dataRateLimitWaitSeconds
      ),
      marketOrderPriceCapRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MARKET_ORDER_PRICE_CAP_RATIO],
        defaults.alpaca.marketOrderPriceCapRatio
      ),
      accountLiquidationDiscountPercent: this.parseNumber(
        this.rawSettings[SETTING_KEYS.ACCOUNT_LIQUIDATION_DISCOUNT_PERCENT],
        defaults.alpaca.accountLiquidationDiscountPercent
      ),
      accountLiquidationDeviationBandPercent: this.parseNumber(
        this.rawSettings[SETTING_KEYS.ACCOUNT_LIQUIDATION_DEVIATION_BAND_PERCENT],
        defaults.alpaca.accountLiquidationDeviationBandPercent
      )
    };
  }

  private buildEodhdValue(defaults: SettingsValue): SettingsValue['eodhd'] {
    return {
      baseUrl: this.parseString(this.rawSettings[SETTING_KEYS.EODHD_BASE_URL], defaults.eodhd.baseUrl),
      apiToken: this.parseString(this.rawSettings[SETTING_KEYS.EODHD_API_TOKEN], defaults.eodhd.apiToken),
      rateLimitWaitSeconds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.EODHD_RATE_LIMIT_WAIT_SECONDS],
        defaults.eodhd.rateLimitWaitSeconds
      )
    };
  }

  private buildTiingoValue(defaults: SettingsValue): SettingsValue['tiingo'] {
    return {
      baseUrl: this.parseString(this.rawSettings[SETTING_KEYS.TIINGO_BASE_URL], defaults.tiingo.baseUrl),
      apiToken: this.parseString(this.rawSettings[SETTING_KEYS.TIINGO_API_TOKEN], defaults.tiingo.apiToken),
      rateLimitWaitSeconds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TIINGO_RATE_LIMIT_WAIT_SECONDS],
        defaults.tiingo.rateLimitWaitSeconds
      )
    };
  }

  private buildCandleSyncValue(defaults: SettingsValue): SettingsValue['candleSync'] {
    return {
      candleMismatchThreshold: this.parseNumber(
        this.rawSettings[SETTING_KEYS.CANDLE_MISMATCH_THRESHOLD],
        defaults.candleSync.candleMismatchThreshold
      ),
      maxConcurrentUpdates: this.parseNumber(
        this.rawSettings[SETTING_KEYS.CANDLE_SYNC_MAX_CONCURRENT_UPDATES],
        defaults.candleSync.maxConcurrentUpdates
      ),
      matchingRatioThreshold: this.parseNumber(
        this.rawSettings[SETTING_KEYS.CANDLE_SYNC_MATCHING_RATIO_THRESHOLD],
        defaults.candleSync.matchingRatioThreshold
      ),
      autoDailyCandleSyncEnabled: this.parseBoolean(
        this.rawSettings[SETTING_KEYS.AUTO_DAILY_CANDLE_SYNC_ENABLED],
        defaults.candleSync.autoDailyCandleSyncEnabled
      ),
      autoDailyServerUpdateEnabled: this.parseBoolean(
        this.rawSettings[SETTING_KEYS.AUTO_DAILY_SERVER_UPDATE_ENABLED],
        defaults.candleSync.autoDailyServerUpdateEnabled
      )
    };
  }

  private buildExpenseRatiosValue(defaults: SettingsValue): SettingsValue['expenseRatios'] {
    return {
      etfBaseExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.ETF_BASE_EXPENSE_RATIO],
        defaults.expenseRatios.etfBaseExpenseRatio
      ),
      inverseEtfExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.INVERSE_ETF_EXPENSE_RATIO],
        defaults.expenseRatios.inverseEtfExpenseRatio
      ),
      commodityTrustExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.COMMODITY_TRUST_EXPENSE_RATIO],
        defaults.expenseRatios.commodityTrustExpenseRatio
      ),
      bondEtfExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.BOND_ETF_EXPENSE_RATIO],
        defaults.expenseRatios.bondEtfExpenseRatio
      ),
      incomeEtfExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.INCOME_ETF_EXPENSE_RATIO],
        defaults.expenseRatios.incomeEtfExpenseRatio
      ),
      leveraged2xExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LEVERAGED_2X_EXPENSE_RATIO],
        defaults.expenseRatios.leveraged2xExpenseRatio
      ),
      leveraged3xExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LEVERAGED_3X_EXPENSE_RATIO],
        defaults.expenseRatios.leveraged3xExpenseRatio
      ),
      leveraged5xExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LEVERAGED_5X_EXPENSE_RATIO],
        defaults.expenseRatios.leveraged5xExpenseRatio
      )
    };
  }

  private buildEngineValue(defaults: SettingsValue): SettingsValue['engine'] {
    return {
      tradeCloseFeeRate: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TRADE_CLOSE_FEE_RATE],
        defaults.engine.tradeCloseFeeRate
      ),
      tradeSlippageRate: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TRADE_SLIPPAGE_RATE],
        defaults.engine.tradeSlippageRate
      ),
      limitBuyPenetrationRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LIMIT_BUY_PENETRATION_RATIO],
        defaults.engine.limitBuyPenetrationRatio
      ),
      shortBorrowFeeAnnualRate: this.parseNumber(
        this.rawSettings[SETTING_KEYS.SHORT_BORROW_FEE_ANNUAL_RATE],
        defaults.engine.shortBorrowFeeAnnualRate
      ),
      tradeEntryPriceMin: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TRADE_ENTRY_PRICE_MIN],
        defaults.engine.tradeEntryPriceMin
      ),
      tradeEntryPriceMax: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TRADE_ENTRY_PRICE_MAX],
        defaults.engine.tradeEntryPriceMax
      ),
      minimumDollarVolumeForEntry: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_FOR_ENTRY],
        defaults.engine.minimumDollarVolumeForEntry
      ),
      minimumDollarVolumeLookback: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_LOOKBACK],
        defaults.engine.minimumDollarVolumeLookback
      ),
      minTickerFluctuationRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MIN_TICKER_FLUCTUATION_RATIO],
        defaults.engine.minTickerFluctuationRatio
      ),
      maxTickerFluctuationRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MAX_TICKER_FLUCTUATION_RATIO],
        defaults.engine.maxTickerFluctuationRatio
      ),
      backtestActiveMonths: this.parseNumberArray(
        this.rawSettings[SETTING_KEYS.BACKTEST_ACTIVE_MONTHS],
        defaults.engine.backtestActiveMonths,
        { integer: true, min: 1 }
      ),
      backtestInitialCapital: this.parseNumber(
        this.rawSettings[SETTING_KEYS.BACKTEST_INITIAL_CAPITAL],
        defaults.engine.backtestInitialCapital
      )
    };
  }

  private buildTickerRulesValue(defaults: SettingsValue): SettingsValue['tickerRules'] {
    return {
      ignoredTickers: this.parseStringArray(
        this.rawSettings[SETTING_KEYS.IGNORED_TICKERS],
        defaults.tickerRules.ignoredTickers
      ),
      alwaysValidationTickers: this.parseStringArray(
        this.rawSettings[SETTING_KEYS.ALWAYS_VALIDATION_TICKERS],
        defaults.tickerRules.alwaysValidationTickers
      ),
      trainingAllocationRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TRAINING_ALLOCATION_RATIO],
        defaults.tickerRules.trainingAllocationRatio
      )
    };
  }

  private buildOptimizerValue(defaults: SettingsValue): SettingsValue['optimizer'] {
    return {
      autoOptimizationEnabled: this.parseBoolean(
        this.rawSettings[SETTING_KEYS.AUTO_OPTIMIZATION_ENABLED],
        defaults.optimizer.autoOptimizationEnabled
      ),
      autoOptimizationDelaySeconds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.AUTO_OPTIMIZATION_DELAY_SECONDS],
        defaults.optimizer.autoOptimizationDelaySeconds
      ),
      allowShortSellingOptimizationEnabled: this.parseBoolean(
        this.rawSettings[SETTING_KEYS.ALLOW_SHORT_SELLING_OPTIMIZATION_ENABLED],
        defaults.optimizer.allowShortSellingOptimizationEnabled
      ),
      lightgbmTrainingStartDate: this.parseString(
        this.rawSettings[SETTING_KEYS.LIGHTGBM_TRAINING_START_DATE],
        defaults.optimizer.lightgbmTrainingStartDate
      ),
      lightgbmTrainingEndDate: this.parseString(
        this.rawSettings[SETTING_KEYS.LIGHTGBM_TRAINING_END_DATE],
        defaults.optimizer.lightgbmTrainingEndDate
      ),
      optimizerTrainingStartDate: this.parseString(
        this.rawSettings[SETTING_KEYS.OPTIMIZER_TRAINING_START_DATE],
        defaults.optimizer.optimizerTrainingStartDate
      ),
      optimizerTrainingEndDate: this.parseString(
        this.rawSettings[SETTING_KEYS.OPTIMIZER_TRAINING_END_DATE],
        defaults.optimizer.optimizerTrainingEndDate
      ),
      verifyWindowStartDate: this.parseString(
        this.rawSettings[SETTING_KEYS.VERIFY_WINDOW_START_DATE],
        defaults.optimizer.verifyWindowStartDate
      ),
      verifyWindowEndDate: this.parseString(
        this.rawSettings[SETTING_KEYS.VERIFY_WINDOW_END_DATE],
        defaults.optimizer.verifyWindowEndDate
      ),
      balanceWindowStartDate: this.parseString(
        this.rawSettings[SETTING_KEYS.BALANCE_WINDOW_START_DATE],
        defaults.optimizer.balanceWindowStartDate
      ),
      balanceWindowEndDate: this.parseString(
        this.rawSettings[SETTING_KEYS.BALANCE_WINDOW_END_DATE],
        defaults.optimizer.balanceWindowEndDate
      ),
      localOptimizationVersion: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LOCAL_OPTIMIZATION_VERSION],
        defaults.optimizer.localOptimizationVersion
      ),
      localOptimizationMultiStartSeeds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LOCAL_OPTIMIZATION_MULTI_START_SEEDS],
        defaults.optimizer.localOptimizationMultiStartSeeds
      ),
      optimizationObjective: this.parseOptimizationObjective(
        this.rawSettings[SETTING_KEYS.OPTIMIZATION_OBJECTIVE]
      ),
      hetznerApiToken: this.parseString(
        this.rawSettings[SETTING_KEYS.HETZNER_API_TOKEN],
        defaults.optimizer.hetznerApiToken
      ),
      hetznerServerType: this.parseString(
        this.rawSettings[SETTING_KEYS.HETZNER_SERVER_TYPE],
        defaults.optimizer.hetznerServerType
      ),
      hetznerServerLocation: this.parseString(
        this.rawSettings[SETTING_KEYS.HETZNER_SERVER_LOCATION],
        defaults.optimizer.hetznerServerLocation
      ),
      hetznerSshKeyName: this.parseString(
        this.rawSettings[SETTING_KEYS.HETZNER_SSH_KEY_NAME],
        defaults.optimizer.hetznerSshKeyName
      ),
      hetznerPublicKey: this.parseString(
        this.rawSettings[SETTING_KEYS.HETZNER_PUBLIC_KEY],
        defaults.optimizer.hetznerPublicKey
      ),
      hetznerPrivateKey: this.parseString(
        this.rawSettings[SETTING_KEYS.HETZNER_PRIVATE_KEY],
        defaults.optimizer.hetznerPrivateKey
      ),
      localOptimizationStepMultipliers: this.parseNumberArray(
        this.rawSettings[SETTING_KEYS.LOCAL_OPTIMIZATION_STEP_MULTIPLIERS],
        defaults.optimizer.localOptimizationStepMultipliers,
        { integer: true }
      ),
      localOptimizationMaxUnadjustedPriceValues: this.parseNumberArray(
        this.rawSettings[SETTING_KEYS.LOCAL_OPTIMIZATION_MAX_UNADJUSTED_PRICE_VALUES],
        defaults.optimizer.localOptimizationMaxUnadjustedPriceValues,
        { integer: true, min: 1 }
      ),
      maxAllowedDrawdownRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MAX_ALLOWED_DRAWDOWN_RATIO],
        defaults.optimizer.maxAllowedDrawdownRatio
      ),
      backtestApiSecret: this.parseString(
        this.rawSettings[SETTING_KEYS.BACKTEST_API_SECRET],
        defaults.optimizer.backtestApiSecret
      )
    };
  }

  private buildParamScoringValue(defaults: SettingsValue): SettingsValue['paramScoring'] {
    return {
      minTrades: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_MIN_TRADES],
        defaults.paramScoring.minTrades
      ),
      drawdownLambda: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_DRAWDOWN_LAMBDA],
        defaults.paramScoring.drawdownLambda
      ),
      neighborThreshold: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_NEIGHBOR_THRESHOLD],
        defaults.paramScoring.neighborThreshold
      ),
      coreScoreQuantile: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_CORE_SCORE_QUANTILE],
        defaults.paramScoring.coreScoreQuantile
      ),
      pairwiseNeighborLimit: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_PAIRWISE_NEIGHBOR_LIMIT],
        defaults.paramScoring.pairwiseNeighborLimit
      )
    };
  }

  private buildTemplateScoringValue(defaults: SettingsValue): SettingsValue['templateScoring'] {
    return {
      returnScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_RETURN_SCALE],
        defaults.templateScoring.returnScale
      ),
      validationNegativePenaltyStrength: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VALIDATION_NEGATIVE_PENALTY_STRENGTH],
        defaults.templateScoring.validationNegativePenaltyStrength
      ),
      drawdownLambda: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_DRAWDOWN_LAMBDA],
        defaults.templateScoring.drawdownLambda
      ),
      tradeTarget: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_TRADE_TARGET],
        defaults.templateScoring.tradeTarget
      ),
      tradeWeight: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_TRADE_WEIGHT],
        defaults.templateScoring.tradeWeight
      ),
      recencyHalfLifeDays: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_RECENCY_HALF_LIFE_DAYS],
        defaults.templateScoring.recencyHalfLifeDays
      ),
      verifySharpeScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_SHARPE_SCALE],
        defaults.templateScoring.verifySharpeScale
      ),
      verifyCalmarScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CALMAR_SCALE],
        defaults.templateScoring.verifyCalmarScale
      ),
      verifyCagrScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_SCALE],
        defaults.templateScoring.verifyCagrScale
      ),
      verifyCagrNegativeScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_NEG_SCALE],
        defaults.templateScoring.verifyCagrNegativeScale
      ),
      verifyDrawdownLambda: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_DRAWDOWN_LAMBDA],
        defaults.templateScoring.verifyDrawdownLambda
      ),
      verifyMinMultiplier: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER],
        defaults.templateScoring.verifyMinMultiplier
      ),
      verifyMaxMultiplier: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MAX_MULTIPLIER],
        defaults.templateScoring.verifyMaxMultiplier
      )
    };
  }

  private buildUserAccessValue(defaults: SettingsValue): SettingsValue['userAccess'] {
    return {
      inviteLinkValidDays: this.parseNumber(
        this.rawSettings[SETTING_KEYS.INVITE_LINK_VALID_DAYS],
        defaults.userAccess.inviteLinkValidDays
      ),
      sessionCookieValidDays: this.parseNumber(
        this.rawSettings[SETTING_KEYS.SESSION_COOKIE_VALID_DAYS],
        defaults.userAccess.sessionCookieValidDays
      ),
      mtlsAccessCertPassword: this.parseString(
        this.rawSettings[SETTING_KEYS.MTLS_ACCESS_CERT_PASSWORD],
        defaults.userAccess.mtlsAccessCertPassword
      )
    };
  }

  private buildEmailValue(defaults: SettingsValue): SettingsValue['email'] {
    return {
      emailSecurityEmoji: this.parseString(
        this.rawSettings[SETTING_KEYS.EMAIL_SECURITY_EMOJI],
        defaults.email.emailSecurityEmoji
      ),
      resendApiKey: this.parseString(this.rawSettings[SETTING_KEYS.RESEND_API_KEY], defaults.email.resendApiKey)
    };
  }

  private parseString(rawValue: string | null | undefined, fallback: string): string {
    return typeof rawValue === 'string' ? rawValue : fallback;
  }

  private parseBoolean(rawValue: string | null | undefined, fallback: boolean): boolean {
    if (typeof rawValue !== 'string') {
      return fallback;
    }
    return rawValue.trim().toLowerCase() === 'true';
  }

  private parseNumber(rawValue: string | null | undefined, fallback: number): number {
    if (typeof rawValue !== 'string') {
      return fallback;
    }
    const trimmed = rawValue.trim();
    if (!trimmed) {
      return fallback;
    }
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  private parseStringArray(rawValue: string | null | undefined, fallback: string[]): string[] {
    if (rawValue === null || rawValue === undefined) {
      return [...fallback];
    }
    return this.parseSettingArrayValue(rawValue);
  }

  private parseNumberArray(
    rawValue: string | null | undefined,
    fallback: number[],
    options: { integer?: boolean; min?: number; max?: number } = {}
  ): number[] {
    if (rawValue === null || rawValue === undefined) {
      return [...fallback];
    }

    const trimmed = rawValue.trim();
    if (!trimmed) {
      return [];
    }

    const entries = this.parseRawArrayEntries(trimmed);
    const values: number[] = [];
    const seen = new Set<number>();

    for (const entry of entries) {
      const parsed = typeof entry === 'number' ? entry : Number(String(entry).trim());
      if (!Number.isFinite(parsed)) {
        continue;
      }
      if (options.integer && !Number.isInteger(parsed)) {
        continue;
      }
      if (options.min !== undefined && parsed < options.min) {
        continue;
      }
      if (options.max !== undefined && parsed > options.max) {
        continue;
      }
      if (seen.has(parsed)) {
        continue;
      }
      seen.add(parsed);
      values.push(parsed);
    }

    return values.length > 0 ? values : [];
  }

  private parseRawArrayEntries(rawValue: string): unknown[] {
    try {
      const parsed = JSON.parse(rawValue);
      if (Array.isArray(parsed)) {
        return parsed;
      }
    } catch {
      // Fall back to delimiter parsing below.
    }

    return rawValue.split(/[,\s]+/g).filter(entry => entry.length > 0);
  }

  private parseCandleDataProvider(rawValue: string | null | undefined): SettingsCandleDataProvider {
    const normalized = normalizeUppercaseString(rawValue);
    if (normalized === 'ALPACA' || normalized === 'EODHD' || normalized === 'TIINGO') {
      return normalized;
    }
    return DEFAULT_CANDLE_DATA_PROVIDER;
  }

  private parseOptimizationObjective(rawValue: string | null | undefined): SettingsOptimizationObjective {
    const normalized = normalizeUppercaseString(rawValue);
    if (normalized === 'CAGR' || normalized === 'SHARPE') {
      return normalized;
    }
    return DEFAULT_OPTIMIZATION_OBJECTIVE;
  }

  async getSettingValue(settingKey: string): Promise<string | null> {
    if (!settingKey || typeof settingKey !== 'string') {
      throw new Error('settingKey is required for getSettingValue');
    }

    const cached = this.readFromCache(settingKey);
    if (cached !== undefined) {
      return cached;
    }

    const row = await this.db.get<SettingValueRow>(
      'SELECT value FROM settings WHERE setting_key = ?',
      [settingKey]
    );
    const rawValue = typeof row?.value === 'string' ? row.value : null;
    const value = rawValue && isSensitiveSettingKey(settingKey) ? decryptValue(rawValue) : rawValue;
    this.cacheValue(settingKey, value ?? null);
    if (this.isKnownSettingKey(settingKey)) {
      this.applyKnownSettings({ [settingKey]: value ?? null });
    }
    return value ?? null;
  }

  async getRequiredSettingValue(settingKey: string): Promise<string> {
    const value = await this.getSettingValue(settingKey);
    if (typeof value === 'string' && value.trim().length > 0) {
      return value;
    }
    throw new Error(`Required setting "${settingKey}" is missing or empty.`);
  }

  async getSettingArray(settingKey: string): Promise<string[]> {
    const rawValue = await this.getSettingValue(settingKey);
    return this.parseSettingArrayValue(rawValue);
  }

  async getSettingsByKeys(settingKeys: SettingKey[]): Promise<Record<string, string | null>> {
    if (!Array.isArray(settingKeys) || settingKeys.length === 0) {
      return {};
    }

    const placeholders = settingKeys.map(() => '?').join(',');
    const rows = await this.db.all<SettingRow>(
      `SELECT setting_key, value FROM settings WHERE setting_key IN (${placeholders})`,
      settingKeys
    );

    const result: Record<string, string | null> = {};
    const knownSettings: Partial<Record<SettingKey, string | null>> = {};

    for (const key of settingKeys) {
      result[key] = null;
      knownSettings[key] = null;
    }

    for (const row of rows) {
      const key = typeof row.setting_key === 'string' ? row.setting_key : String(row.setting_key ?? '');
      if (!key) {
        continue;
      }
      const rawValue =
        typeof row.value === 'string'
          ? row.value
          : row.value === null || row.value === undefined
            ? null
            : String(row.value);
      const value = rawValue && isSensitiveSettingKey(key) ? decryptValue(rawValue) : rawValue;
      result[key] = value ?? null;
      if (this.isKnownSettingKey(key)) {
        knownSettings[key] = value ?? null;
      }
    }

    this.applyKnownSettings(knownSettings);
    return result;
  }

  async upsertSettings(settings: Record<string, string>): Promise<void> {
    const entries = Object.entries(settings ?? {}).filter(
      ([key]) => typeof key === 'string' && key.trim().length > 0
    );
    if (entries.length === 0) {
      return;
    }

    await this.db.withTransaction(async (client: PoolClient) => {
      for (const [key, rawValue] of entries) {
        const value = typeof rawValue === 'string' ? rawValue : String(rawValue ?? '');
        const storedValue = isSensitiveSettingKey(key) && value.length > 0 ? encryptValue(value) : value;
        await this.db.run(
          `
            INSERT INTO settings (setting_key, value, updated_at)
            VALUES (?, ?, NOW())
            ON CONFLICT (setting_key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
          `,
          [key, storedValue],
          client
        );
      }
    });

    const knownSettings: Partial<Record<SettingKey, string | null>> = {};
    for (const [key, rawValue] of entries) {
      const value = typeof rawValue === 'string' ? rawValue : String(rawValue ?? '');
      this.cacheValue(key, value);
      if (this.isKnownSettingKey(key)) {
        knownSettings[key] = value;
      }
    }

    this.applyKnownSettings(knownSettings);
  }

  private parseSettingArrayValue(rawValue: string | null): string[] {
    if (!rawValue || rawValue.trim().length === 0) {
      return [];
    }
    const trimmed = rawValue.trim();
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        const normalized = normalizeUppercaseStrings(
          parsed.filter((entry): entry is string => typeof entry === 'string')
        );
        return Array.from(new Set(normalized));
      }
    } catch {
      // Fall back to delimiter parsing below.
    }

    const split = trimmed.split(/[,\s]+/g);
    const normalized = normalizeUppercaseStrings(split);
    return Array.from(new Set(normalized));
  }
}
