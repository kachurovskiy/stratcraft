import {
  DEFAULT_AUTO_OPTIMIZATION_DELAY_SECONDS,
  DEFAULT_BACKTEST_INITIAL_CAPITAL,
  DEFAULT_LIQUIDATION_DEVIATION_BAND_PERCENT,
  DEFAULT_LIQUIDATION_DISCOUNT_PERCENT,
  DEFAULT_MARKET_ORDER_PRICE_CAP_RATIO,
  DEFAULT_MTLS_ACCESS_CERT_PASSWORD
} from '../constants';
import type { SettingsCandleDataProvider, SettingsOptimizationObjective, SettingsValue } from '../database/types';
import { DEFAULT_FOOTER_DISCLAIMER_HTML } from '../utils/footerDisclaimer';

export const DEFAULT_CANDLE_DATA_PROVIDER: SettingsCandleDataProvider = 'TIINGO';
export const DEFAULT_OPTIMIZATION_OBJECTIVE: SettingsOptimizationObjective = 'SHARPE';

export const createDefaultSettingsValue = (): SettingsValue => ({
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
