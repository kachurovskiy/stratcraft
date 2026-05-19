import type { SettingsCandleDataProvider, SettingsOptimizationObjective, SettingsValue } from '../database/types';
import { DEFAULT_FOOTER_DISCLAIMER_HTML } from '../utils/footerDisclaimer';

export const DEFAULT_CANDLE_DATA_PROVIDER: SettingsCandleDataProvider = 'TIINGO';
export const DEFAULT_OPTIMIZATION_OBJECTIVE: SettingsOptimizationObjective = 'SHARPE';
export const DEFAULT_OPTIMIZATION_OBJECTIVE_2: SettingsOptimizationObjective = 'CAGR';

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
    marketOrderPriceCapRatio: 0.08,
    accountLiquidationDiscountPercent: 0,
    accountLiquidationDeviationBandPercent: 10
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
    backtestInitialCapital: 100000
  },
  tickerRules: {
    ignoredTickers: [],
    alwaysValidationTickers: ['SPY', 'QQQ'],
    trainingAllocationRatio: 0.65
  },
  optimizer: {
    autoOptimizationEnabled: true,
    autoOptimizationDelaySeconds: 300,
    allowShortSellingOptimizationEnabled: false,
    optimizerExploreEnabled: true,
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
    optimizationObjective2: DEFAULT_OPTIMIZATION_OBJECTIVE_2,
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
    pairwiseNeighborLimit: 1500,
    stabilityGamma: 2
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
    mtlsAccessCertPassword: 'stratcraft'
  },
  email: {
    emailSecurityEmoji: '',
    resendApiKey: ''
  }
});
