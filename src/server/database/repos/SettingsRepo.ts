import type { PoolClient, QueryResultRow } from 'pg';
import {
  isSensitiveSettingKey,
  SETTING_KEY_LIST,
  SETTING_KEYS,
  type SettingKey
} from '../../constants';
import {
  createDefaultSettingsValue,
  DEFAULT_CANDLE_DATA_PROVIDER,
  DEFAULT_OPTIMIZATION_OBJECTIVE,
  DEFAULT_OPTIMIZATION_OBJECTIVE_2
} from '../../settings/defaults';
import { normalizeDomain } from '../../utils/domain';
import { decryptValue, encryptValue } from '../../utils/encryption';
import { normalizeUppercaseString, normalizeUppercaseStrings } from '../../utils/stringNormalization';
import { DbClient } from '../core/DbClient';
import type { SettingsCandleDataProvider, SettingsOptimizationObjective, SettingsValue } from '../types';

const KNOWN_SETTING_KEYS = new Set<SettingKey>(SETTING_KEY_LIST);
const ISO_DATE_REGEX = /^(\d{4})-(\d{2})-(\d{2})$/;

type SettingRow = QueryResultRow & { setting_key: string; value: string | null };
type NumberParseOptions = { integer?: boolean; min?: number; max?: number; clamp?: boolean };

const createEmptySettingsRecord = (): Record<SettingKey, string | null> => {
  const settings = {} as Record<SettingKey, string | null>;
  for (const settingKey of SETTING_KEY_LIST) {
    settings[settingKey] = null;
  }
  return settings;
};

export class SettingsRepo {
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
      siteName: this.parseNonEmptyString(this.rawSettings[SETTING_KEYS.SITE_NAME], defaults.app.siteName),
      domain: this.parseDomain(this.rawSettings[SETTING_KEYS.DOMAIN]),
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
    const marketOrderPriceCapRatio = this.parseNumber(
      this.rawSettings[SETTING_KEYS.MARKET_ORDER_PRICE_CAP_RATIO],
      defaults.alpaca.marketOrderPriceCapRatio,
      { min: 0 }
    );

    return {
      paperUrl: this.parseHttpUrl(this.rawSettings[SETTING_KEYS.ALPACA_PAPER_URL], defaults.alpaca.paperUrl),
      liveUrl: this.parseHttpUrl(this.rawSettings[SETTING_KEYS.ALPACA_LIVE_URL], defaults.alpaca.liveUrl),
      dataBaseUrl: this.parseHttpUrl(this.rawSettings[SETTING_KEYS.ALPACA_DATA_BASE_URL], defaults.alpaca.dataBaseUrl),
      apiKey: this.parseString(this.rawSettings[SETTING_KEYS.ALPACA_API_KEY], defaults.alpaca.apiKey),
      apiSecret: this.parseString(this.rawSettings[SETTING_KEYS.ALPACA_API_SECRET], defaults.alpaca.apiSecret),
      dataRateLimitWaitSeconds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.ALPACA_DATA_RATE_LIMIT_WAIT_SECONDS],
        defaults.alpaca.dataRateLimitWaitSeconds,
        { min: 1, integer: true }
      ),
      marketOrderPriceCapRatio,
      accountLiquidationDiscountPercent: this.parseNumber(
        this.rawSettings[SETTING_KEYS.ACCOUNT_LIQUIDATION_DISCOUNT_PERCENT],
        defaults.alpaca.accountLiquidationDiscountPercent,
        { min: 0, clamp: true }
      ),
      accountLiquidationDeviationBandPercent: this.parseNumber(
        this.rawSettings[SETTING_KEYS.ACCOUNT_LIQUIDATION_DEVIATION_BAND_PERCENT],
        defaults.alpaca.accountLiquidationDeviationBandPercent,
        { min: 0, clamp: true }
      )
    };
  }

  private buildEodhdValue(defaults: SettingsValue): SettingsValue['eodhd'] {
    return {
      baseUrl: this.parseHttpUrl(this.rawSettings[SETTING_KEYS.EODHD_BASE_URL], defaults.eodhd.baseUrl),
      apiToken: this.parseString(this.rawSettings[SETTING_KEYS.EODHD_API_TOKEN], defaults.eodhd.apiToken),
      rateLimitWaitSeconds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.EODHD_RATE_LIMIT_WAIT_SECONDS],
        defaults.eodhd.rateLimitWaitSeconds,
        { min: 1, integer: true }
      )
    };
  }

  private buildTiingoValue(defaults: SettingsValue): SettingsValue['tiingo'] {
    return {
      baseUrl: this.parseHttpUrl(this.rawSettings[SETTING_KEYS.TIINGO_BASE_URL], defaults.tiingo.baseUrl),
      apiToken: this.parseString(this.rawSettings[SETTING_KEYS.TIINGO_API_TOKEN], defaults.tiingo.apiToken),
      rateLimitWaitSeconds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TIINGO_RATE_LIMIT_WAIT_SECONDS],
        defaults.tiingo.rateLimitWaitSeconds,
        { min: 1, integer: true }
      )
    };
  }

  private buildCandleSyncValue(defaults: SettingsValue): SettingsValue['candleSync'] {
    return {
      candleMismatchThreshold: this.parseNumber(
        this.rawSettings[SETTING_KEYS.CANDLE_MISMATCH_THRESHOLD],
        defaults.candleSync.candleMismatchThreshold,
        { min: 0, clamp: true }
      ),
      maxConcurrentUpdates: this.parseNumber(
        this.rawSettings[SETTING_KEYS.CANDLE_SYNC_MAX_CONCURRENT_UPDATES],
        defaults.candleSync.maxConcurrentUpdates,
        { min: 1, integer: true }
      ),
      matchingRatioThreshold: this.parseNumber(
        this.rawSettings[SETTING_KEYS.CANDLE_SYNC_MATCHING_RATIO_THRESHOLD],
        defaults.candleSync.matchingRatioThreshold,
        { min: 0, max: 1, clamp: true }
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
        defaults.expenseRatios.etfBaseExpenseRatio,
        { min: 0, clamp: true }
      ),
      inverseEtfExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.INVERSE_ETF_EXPENSE_RATIO],
        defaults.expenseRatios.inverseEtfExpenseRatio,
        { min: 0, clamp: true }
      ),
      commodityTrustExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.COMMODITY_TRUST_EXPENSE_RATIO],
        defaults.expenseRatios.commodityTrustExpenseRatio,
        { min: 0, clamp: true }
      ),
      bondEtfExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.BOND_ETF_EXPENSE_RATIO],
        defaults.expenseRatios.bondEtfExpenseRatio,
        { min: 0, clamp: true }
      ),
      incomeEtfExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.INCOME_ETF_EXPENSE_RATIO],
        defaults.expenseRatios.incomeEtfExpenseRatio,
        { min: 0, clamp: true }
      ),
      leveraged2xExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LEVERAGED_2X_EXPENSE_RATIO],
        defaults.expenseRatios.leveraged2xExpenseRatio,
        { min: 0, clamp: true }
      ),
      leveraged3xExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LEVERAGED_3X_EXPENSE_RATIO],
        defaults.expenseRatios.leveraged3xExpenseRatio,
        { min: 0, clamp: true }
      ),
      leveraged5xExpenseRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LEVERAGED_5X_EXPENSE_RATIO],
        defaults.expenseRatios.leveraged5xExpenseRatio,
        { min: 0, clamp: true }
      )
    };
  }

  private buildEngineValue(defaults: SettingsValue): SettingsValue['engine'] {
    const tradeEntryPriceMin = this.parseNumber(
      this.rawSettings[SETTING_KEYS.TRADE_ENTRY_PRICE_MIN],
      defaults.engine.tradeEntryPriceMin,
      { min: 0, clamp: true }
    );
    const minTickerFluctuationRatio = this.parseNumber(
      this.rawSettings[SETTING_KEYS.MIN_TICKER_FLUCTUATION_RATIO],
      defaults.engine.minTickerFluctuationRatio,
      { min: 0, clamp: true }
    );

    return {
      tradeCloseFeeRate: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TRADE_CLOSE_FEE_RATE],
        defaults.engine.tradeCloseFeeRate,
        { min: 0, clamp: true }
      ),
      tradeSlippageRate: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TRADE_SLIPPAGE_RATE],
        defaults.engine.tradeSlippageRate,
        { min: 0, clamp: true }
      ),
      limitBuyPenetrationRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LIMIT_BUY_PENETRATION_RATIO],
        defaults.engine.limitBuyPenetrationRatio,
        { min: 0, clamp: true }
      ),
      shortBorrowFeeAnnualRate: this.parseNumber(
        this.rawSettings[SETTING_KEYS.SHORT_BORROW_FEE_ANNUAL_RATE],
        defaults.engine.shortBorrowFeeAnnualRate,
        { min: 0, clamp: true }
      ),
      tradeEntryPriceMin,
      tradeEntryPriceMax: this.parseUpperBound(
        this.rawSettings[SETTING_KEYS.TRADE_ENTRY_PRICE_MAX],
        defaults.engine.tradeEntryPriceMax,
        tradeEntryPriceMin
      ),
      minimumDollarVolumeForEntry: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_FOR_ENTRY],
        defaults.engine.minimumDollarVolumeForEntry,
        { min: 0, clamp: true }
      ),
      minimumDollarVolumeLookback: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_LOOKBACK],
        defaults.engine.minimumDollarVolumeLookback,
        { min: 0, integer: true, clamp: true }
      ),
      minTickerFluctuationRatio,
      maxTickerFluctuationRatio: this.parseUpperBound(
        this.rawSettings[SETTING_KEYS.MAX_TICKER_FLUCTUATION_RATIO],
        defaults.engine.maxTickerFluctuationRatio,
        minTickerFluctuationRatio
      ),
      backtestActiveMonths: this.parseNumberArray(
        this.rawSettings[SETTING_KEYS.BACKTEST_ACTIVE_MONTHS],
        defaults.engine.backtestActiveMonths,
        { integer: true, min: 1, fallbackOnEmpty: true }
      ),
      backtestInitialCapital: this.parseNumber(
        this.rawSettings[SETTING_KEYS.BACKTEST_INITIAL_CAPITAL],
        defaults.engine.backtestInitialCapital,
        { min: 1 }
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
        defaults.tickerRules.trainingAllocationRatio,
        { min: 0, max: 1, clamp: true }
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
        defaults.optimizer.autoOptimizationDelaySeconds,
        { min: 0, clamp: true }
      ),
      allowShortSellingOptimizationEnabled: this.parseBoolean(
        this.rawSettings[SETTING_KEYS.ALLOW_SHORT_SELLING_OPTIMIZATION_ENABLED],
        defaults.optimizer.allowShortSellingOptimizationEnabled
      ),
      lightgbmTrainingStartDate: this.parseIsoDate(
        this.rawSettings[SETTING_KEYS.LIGHTGBM_TRAINING_START_DATE],
        defaults.optimizer.lightgbmTrainingStartDate
      ),
      lightgbmTrainingEndDate: this.parseIsoDate(
        this.rawSettings[SETTING_KEYS.LIGHTGBM_TRAINING_END_DATE],
        defaults.optimizer.lightgbmTrainingEndDate
      ),
      optimizerTrainingStartDate: this.parseIsoDate(
        this.rawSettings[SETTING_KEYS.OPTIMIZER_TRAINING_START_DATE],
        defaults.optimizer.optimizerTrainingStartDate
      ),
      optimizerTrainingEndDate: this.parseIsoDate(
        this.rawSettings[SETTING_KEYS.OPTIMIZER_TRAINING_END_DATE],
        defaults.optimizer.optimizerTrainingEndDate
      ),
      verifyWindowStartDate: this.parseIsoDate(
        this.rawSettings[SETTING_KEYS.VERIFY_WINDOW_START_DATE],
        defaults.optimizer.verifyWindowStartDate
      ),
      verifyWindowEndDate: this.parseIsoDate(
        this.rawSettings[SETTING_KEYS.VERIFY_WINDOW_END_DATE],
        defaults.optimizer.verifyWindowEndDate
      ),
      balanceWindowStartDate: this.parseIsoDate(
        this.rawSettings[SETTING_KEYS.BALANCE_WINDOW_START_DATE],
        defaults.optimizer.balanceWindowStartDate
      ),
      balanceWindowEndDate: this.parseIsoDate(
        this.rawSettings[SETTING_KEYS.BALANCE_WINDOW_END_DATE],
        defaults.optimizer.balanceWindowEndDate
      ),
      localOptimizationVersion: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LOCAL_OPTIMIZATION_VERSION],
        defaults.optimizer.localOptimizationVersion,
        { min: 0, integer: true, clamp: true }
      ),
      localOptimizationMultiStartSeeds: this.parseNumber(
        this.rawSettings[SETTING_KEYS.LOCAL_OPTIMIZATION_MULTI_START_SEEDS],
        defaults.optimizer.localOptimizationMultiStartSeeds,
        { min: 0, integer: true, clamp: true }
      ),
      optimizationObjective: this.parseOptimizationObjective(
        this.rawSettings[SETTING_KEYS.OPTIMIZATION_OBJECTIVE],
        DEFAULT_OPTIMIZATION_OBJECTIVE
      ),
      optimizationObjective2: this.parseOptimizationObjective(
        this.rawSettings[SETTING_KEYS.OPTIMIZATION_OBJECTIVE_2],
        DEFAULT_OPTIMIZATION_OBJECTIVE_2
      ),
      hetznerApiToken: this.parseString(
        this.rawSettings[SETTING_KEYS.HETZNER_API_TOKEN],
        defaults.optimizer.hetznerApiToken
      ),
      hetznerServerType: this.parseNonEmptyString(
        this.rawSettings[SETTING_KEYS.HETZNER_SERVER_TYPE],
        defaults.optimizer.hetznerServerType
      ),
      hetznerServerLocation: this.parseNonEmptyString(
        this.rawSettings[SETTING_KEYS.HETZNER_SERVER_LOCATION],
        defaults.optimizer.hetznerServerLocation
      ),
      hetznerSshKeyName: this.parseNonEmptyString(
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
        { integer: true, fallbackOnEmpty: true }
      ),
      localOptimizationMaxUnadjustedPriceValues: this.parseNumberArray(
        this.rawSettings[SETTING_KEYS.LOCAL_OPTIMIZATION_MAX_UNADJUSTED_PRICE_VALUES],
        defaults.optimizer.localOptimizationMaxUnadjustedPriceValues,
        { integer: true, min: 1, fallbackOnEmpty: true }
      ),
      maxAllowedDrawdownRatio: this.parseNumber(
        this.rawSettings[SETTING_KEYS.MAX_ALLOWED_DRAWDOWN_RATIO],
        defaults.optimizer.maxAllowedDrawdownRatio,
        { min: 0, clamp: true }
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
        defaults.paramScoring.minTrades,
        { min: 0, integer: true, clamp: true }
      ),
      drawdownLambda: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_DRAWDOWN_LAMBDA],
        defaults.paramScoring.drawdownLambda,
        { min: 0, clamp: true }
      ),
      neighborThreshold: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_NEIGHBOR_THRESHOLD],
        defaults.paramScoring.neighborThreshold,
        { min: 0, clamp: true }
      ),
      pairwiseNeighborLimit: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_PAIRWISE_NEIGHBOR_LIMIT],
        defaults.paramScoring.pairwiseNeighborLimit,
        { min: 1, integer: true }
      ),
      stabilityGamma: this.parseNumber(
        this.rawSettings[SETTING_KEYS.PARAM_SCORE_STABILITY_GAMMA],
        defaults.paramScoring.stabilityGamma,
        { min: 0, clamp: true }
      )
    };
  }

  private buildTemplateScoringValue(defaults: SettingsValue): SettingsValue['templateScoring'] {
    const verifyMinMultiplier = this.parseNumber(
      this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER],
      defaults.templateScoring.verifyMinMultiplier,
      { min: 0, clamp: true }
    );

    return {
      returnScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_RETURN_SCALE],
        defaults.templateScoring.returnScale,
        { min: 1e-6 }
      ),
      validationNegativePenaltyStrength: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VALIDATION_NEGATIVE_PENALTY_STRENGTH],
        defaults.templateScoring.validationNegativePenaltyStrength,
        { min: 0, clamp: true }
      ),
      drawdownLambda: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_DRAWDOWN_LAMBDA],
        defaults.templateScoring.drawdownLambda,
        { min: 0, clamp: true }
      ),
      tradeTarget: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_TRADE_TARGET],
        defaults.templateScoring.tradeTarget,
        { min: 1 }
      ),
      tradeWeight: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_TRADE_WEIGHT],
        defaults.templateScoring.tradeWeight,
        { min: 0, max: 1, clamp: true }
      ),
      recencyHalfLifeDays: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_RECENCY_HALF_LIFE_DAYS],
        defaults.templateScoring.recencyHalfLifeDays,
        { min: 1 }
      ),
      verifySharpeScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_SHARPE_SCALE],
        defaults.templateScoring.verifySharpeScale,
        { min: 1e-6 }
      ),
      verifyCalmarScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CALMAR_SCALE],
        defaults.templateScoring.verifyCalmarScale,
        { min: 1e-6 }
      ),
      verifyCagrScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_SCALE],
        defaults.templateScoring.verifyCagrScale,
        { min: 1e-6 }
      ),
      verifyCagrNegativeScale: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_NEG_SCALE],
        defaults.templateScoring.verifyCagrNegativeScale,
        { min: 1e-6 }
      ),
      verifyDrawdownLambda: this.parseNumber(
        this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_DRAWDOWN_LAMBDA],
        defaults.templateScoring.verifyDrawdownLambda,
        { min: 0, clamp: true }
      ),
      verifyMinMultiplier,
      verifyMaxMultiplier: Math.max(
        verifyMinMultiplier,
        this.parseNumber(
          this.rawSettings[SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MAX_MULTIPLIER],
          defaults.templateScoring.verifyMaxMultiplier,
          { min: 0, clamp: true }
        )
      )
    };
  }

  private buildUserAccessValue(defaults: SettingsValue): SettingsValue['userAccess'] {
    return {
      inviteLinkValidDays: this.parseNumber(
        this.rawSettings[SETTING_KEYS.INVITE_LINK_VALID_DAYS],
        defaults.userAccess.inviteLinkValidDays,
        { min: 1, integer: true }
      ),
      sessionCookieValidDays: this.parseNumber(
        this.rawSettings[SETTING_KEYS.SESSION_COOKIE_VALID_DAYS],
        defaults.userAccess.sessionCookieValidDays,
        { min: 1, integer: true }
      ),
      mtlsAccessCertPassword: this.parseNonEmptyString(
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
    return typeof rawValue === 'string' ? rawValue.trim() : fallback;
  }

  private parseNonEmptyString(rawValue: string | null | undefined, fallback: string): string {
    const parsed = this.parseString(rawValue, fallback);
    return parsed.length > 0 ? parsed : fallback;
  }

  private parseDomain(rawValue: string | null | undefined): string {
    return normalizeDomain(this.parseString(rawValue, '')) ?? '';
  }

  private parseHttpUrl(rawValue: string | null | undefined, fallback: string): string {
    const parsed = this.parseString(rawValue, '');
    if (!parsed) {
      return fallback;
    }

    try {
      const url = new URL(parsed);
      if (url.protocol === 'http:' || url.protocol === 'https:') {
        return parsed;
      }
    } catch {
      // Fall back to the configured default below.
    }

    return fallback;
  }

  private parseBoolean(rawValue: string | null | undefined, fallback: boolean): boolean {
    if (typeof rawValue !== 'string') {
      return fallback;
    }
    return rawValue.trim().toLowerCase() === 'true';
  }

  private parseNumber(
    rawValue: string | null | undefined,
    fallback: number,
    options: NumberParseOptions = {}
  ): number {
    const parsed = this.parseRawNumber(rawValue);
    if (parsed === null) {
      return fallback;
    }

    const normalized = options.integer ? Math.trunc(parsed) : parsed;

    if (options.clamp) {
      let clamped = normalized;
      if (options.min !== undefined) {
        clamped = Math.max(options.min, clamped);
      }
      if (options.max !== undefined) {
        clamped = Math.min(options.max, clamped);
      }
      return clamped;
    }

    if (options.min !== undefined && normalized < options.min) {
      return fallback;
    }
    if (options.max !== undefined && normalized > options.max) {
      return fallback;
    }

    return normalized;
  }

  private parseUpperBound(rawValue: string | null | undefined, fallback: number, minimum: number): number {
    const parsed = this.parseRawNumber(rawValue);
    if (parsed === null) {
      return Math.max(fallback, minimum);
    }
    if (parsed <= 0) {
      return Number.POSITIVE_INFINITY;
    }
    return Math.max(parsed, minimum);
  }

  private parseIsoDate(rawValue: string | null | undefined, fallback: string): string {
    const parsed = this.parseString(rawValue, '');
    if (!parsed) {
      return fallback;
    }

    const match = ISO_DATE_REGEX.exec(parsed);
    if (!match) {
      return fallback;
    }

    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const utcDate = new Date(Date.UTC(year, month - 1, day));
    if (
      Number.isNaN(utcDate.getTime()) ||
      utcDate.getUTCFullYear() !== year ||
      utcDate.getUTCMonth() !== month - 1 ||
      utcDate.getUTCDate() !== day
    ) {
      return fallback;
    }

    return parsed;
  }

  private parseRawNumber(rawValue: string | null | undefined): number | null {
    if (typeof rawValue !== 'string') {
      return null;
    }
    const trimmed = rawValue.trim();
    if (!trimmed) {
      return null;
    }
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
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
    options: { integer?: boolean; min?: number; max?: number; fallbackOnEmpty?: boolean } = {}
  ): number[] {
    if (rawValue === null || rawValue === undefined) {
      return [...fallback];
    }

    const trimmed = rawValue.trim();
    if (!trimmed) {
      return options.fallbackOnEmpty ? [...fallback] : [];
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

    if (values.length > 0) {
      return values;
    }

    return options.fallbackOnEmpty ? [...fallback] : [];
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

  private parseOptimizationObjective(
    rawValue: string | null | undefined,
    fallback: SettingsOptimizationObjective
  ): SettingsOptimizationObjective {
    const normalized = normalizeUppercaseString(rawValue);
    if (normalized === 'CAGR' || normalized === 'SHARPE') {
      return normalized;
    }
    return fallback;
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
      const value = rawValue && isSensitiveSettingKey(key) ? decryptValue(rawValue).trim() : rawValue?.trim() ?? null;
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
        const value = (typeof rawValue === 'string' ? rawValue : String(rawValue ?? '')).trim();
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
      const value = (typeof rawValue === 'string' ? rawValue : String(rawValue ?? '')).trim();
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
