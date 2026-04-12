import type { BacktestCacheRow } from '../scoring/paramScore';
import type { RemoteOptimizationStatus } from '../types/RemoteOptimization';
import type {
  AccountOperation,
  BacktestScope,
  Strategy,
  StrategyPerformance,
  TickerInfo,
  Trade
} from '../types/StrategyTemplate';
import type { TradingAccount } from '../types/Account';

export type RawUserRow = {
  id: number;
  email: string;
  role: string;
  created_at: Date;
  updated_at: Date;
  otp_code: string | null;
  otp_expires_at: Date | null;
  invite_token_hash: string | null;
  invite_expires_at: Date | null;
  invite_used_at: Date | null;
};

export type UserSessionRecord = {
  id: number;
  userId: number;
  createdAt: Date;
  expiresAt: Date;
  lastSeenAt: Date;
  createdIp: string | null;
  deviceType: string;
};

export type RequestQuotaAction = 'send_otp' | 'verify_otp';
export type RequestQuotaIdentifierType = 'email' | 'ip';

export type RequestQuotaCheckResult = {
  allowed: boolean;
  retryAfterMs: number | null;
  remaining: number;
  limit: number;
  windowMs: number;
};

export type SettingsCandleDataProvider = 'ALPACA' | 'EODHD' | 'TIINGO';
export type SettingsOptimizationObjective = 'CAGR' | 'SHARPE';

export interface AppSettingsValue {
  siteName: string;
  domain: string;
  footerDisclaimerHtml: string;
  tradingViewChartsEnabled: boolean;
}

export interface DataProviderSettingsValue {
  candleDataProvider: SettingsCandleDataProvider;
}

export interface AlpacaSettingsValue {
  paperUrl: string;
  liveUrl: string;
  dataBaseUrl: string;
  apiKey: string;
  apiSecret: string;
  dataRateLimitWaitSeconds: number;
  marketOrderPriceCapRatio: number;
  accountLiquidationDiscountPercent: number;
  accountLiquidationDeviationBandPercent: number;
}

export interface EodhdSettingsValue {
  baseUrl: string;
  apiToken: string;
  rateLimitWaitSeconds: number;
}

export interface TiingoSettingsValue {
  baseUrl: string;
  apiToken: string;
  rateLimitWaitSeconds: number;
}

export interface CandleSyncSettingsValue {
  candleMismatchThreshold: number;
  maxConcurrentUpdates: number;
  matchingRatioThreshold: number;
  autoDailyCandleSyncEnabled: boolean;
  autoDailyServerUpdateEnabled: boolean;
}

export interface ExpenseRatioSettingsValue {
  etfBaseExpenseRatio: number;
  inverseEtfExpenseRatio: number;
  commodityTrustExpenseRatio: number;
  bondEtfExpenseRatio: number;
  incomeEtfExpenseRatio: number;
  leveraged2xExpenseRatio: number;
  leveraged3xExpenseRatio: number;
  leveraged5xExpenseRatio: number;
}

export interface EngineSettingsValue {
  tradeCloseFeeRate: number;
  tradeSlippageRate: number;
  limitBuyPenetrationRatio: number;
  shortBorrowFeeAnnualRate: number;
  tradeEntryPriceMin: number;
  tradeEntryPriceMax: number;
  minimumDollarVolumeForEntry: number;
  minimumDollarVolumeLookback: number;
  minTickerFluctuationRatio: number;
  maxTickerFluctuationRatio: number;
  backtestActiveMonths: number[];
  backtestInitialCapital: number;
}

export interface TickerRulesSettingsValue {
  ignoredTickers: string[];
  alwaysValidationTickers: string[];
  trainingAllocationRatio: number;
}

export interface OptimizerSettingsValue {
  autoOptimizationEnabled: boolean;
  autoOptimizationDelaySeconds: number;
  allowShortSellingOptimizationEnabled: boolean;
  lightgbmTrainingStartDate: string;
  lightgbmTrainingEndDate: string;
  optimizerTrainingStartDate: string;
  optimizerTrainingEndDate: string;
  verifyWindowStartDate: string;
  verifyWindowEndDate: string;
  balanceWindowStartDate: string;
  balanceWindowEndDate: string;
  localOptimizationVersion: number;
  localOptimizationMultiStartSeeds: number;
  optimizationObjective: SettingsOptimizationObjective;
  hetznerApiToken: string;
  hetznerServerType: string;
  hetznerServerLocation: string;
  hetznerSshKeyName: string;
  hetznerPublicKey: string;
  hetznerPrivateKey: string;
  localOptimizationStepMultipliers: number[];
  localOptimizationMaxUnadjustedPriceValues: number[];
  maxAllowedDrawdownRatio: number;
  backtestApiSecret: string;
}

export interface ParamScoringSettingsValue {
  minTrades: number;
  drawdownLambda: number;
  neighborThreshold: number;
  coreScoreQuantile: number;
  pairwiseNeighborLimit: number;
  stabilityGamma: number;
}

export interface TemplateScoringSettingsValue {
  returnScale: number;
  validationNegativePenaltyStrength: number;
  drawdownLambda: number;
  tradeTarget: number;
  tradeWeight: number;
  recencyHalfLifeDays: number;
  verifySharpeScale: number;
  verifyCalmarScale: number;
  verifyCagrScale: number;
  verifyCagrNegativeScale: number;
  verifyDrawdownLambda: number;
  verifyMinMultiplier: number;
  verifyMaxMultiplier: number;
}

export interface UserAccessSettingsValue {
  inviteLinkValidDays: number;
  sessionCookieValidDays: number;
  mtlsAccessCertPassword: string;
}

export interface EmailSettingsValue {
  emailSecurityEmoji: string;
  resendApiKey: string;
}

export interface SettingsValue {
  app: AppSettingsValue;
  dataProvider: DataProviderSettingsValue;
  alpaca: AlpacaSettingsValue;
  eodhd: EodhdSettingsValue;
  tiingo: TiingoSettingsValue;
  candleSync: CandleSyncSettingsValue;
  expenseRatios: ExpenseRatioSettingsValue;
  engine: EngineSettingsValue;
  tickerRules: TickerRulesSettingsValue;
  optimizer: OptimizerSettingsValue;
  paramScoring: ParamScoringSettingsValue;
  templateScoring: TemplateScoringSettingsValue;
  userAccess: UserAccessSettingsValue;
  email: EmailSettingsValue;
}

export type SystemLogInsertInput = {
  source: string;
  level: string;
  message: string;
  metadata?: Record<string, unknown> | null;
  createdAt?: Date;
};

export type SystemLogQueryOptions = {
  source?: string;
  level?: string;
  limit?: number;
  offset?: number;
};

export type SystemLogRow = {
  id: number;
  source: string;
  level: string;
  message: string;
  metadata: string | null;
  created_at: Date;
};

export type AccountSignalSkipRow = {
  id: number;
  strategy_id: string;
  account_id: string | null;
  ticker: string;
  signal_date: string;
  action: string;
  source: string;
  reason: string;
  details: string | null;
  created_at: Date;
};

export interface TickerBacktestPerformanceRow {
  strategyId: string;
  strategyName: string;
  periodMonths: number;
  avgReturnPercent: number | null;
  tradeCount: number;
  backtestResultId: string;
  backtestCreatedAt: Date | null;
}

export interface TradeTickerStats {
  ticker: string;
  totalTrades: number;
  profitableTrades: number;
  unprofitableTrades: number;
  neutralTrades: number;
  netPnl: number;
  absNetPnl: number;
  totalBuyCost: number;
  sumPnlPercent: number;
  pnlPercentCount: number;
  winSumPercent: number;
  winCount: number;
  lossSumPercent: number;
  lossCount: number;
  winDurationSum: number;
  winDurationCount: number;
  lossDurationSum: number;
  lossDurationCount: number;
}

export interface TradeVolumeSegmentStats {
  bucketLabel: string;
  bucketOrder: number;
  tradeCount: number;
  totalPnl: number;
  totalBuyCost: number;
  minVolumeUsd: number;
  maxVolumeUsd: number | null;
}

export interface EntryFillGapBucketStats {
  bucket: number;
  count: number;
  totalPnl: number;
  sumPnlPercent: number;
  pnlPercentCount: number;
}

export interface EntryFillGapHistogram {
  minPercent: number;
  maxPercent: number;
  bucketCount: number;
  buckets: EntryFillGapBucketStats[];
}

export type InsertStrategyInput = {
  id: string;
  name: string;
  templateId: string;
  parameters: Strategy['parameters'];
  status?: Strategy['status'];
  userId?: number | null;
  accountId?: string | null;
  createdAt?: Date;
  updatedAt?: Date;
  backtestStartDate?: Date | null;
};

export type BacktestCachePerformancePoint = {
  id: string;
  templateId: string;
  sharpeRatio: number | null;
  calmarRatio: number | null;
  verifySharpeRatio: number | null;
  verifyCalmarRatio: number | null;
  cagr: number | null;
  verifyCagr: number | null;
  maxDrawdownRatio: number | null;
  verifyMaxDrawdownRatio: number | null;
  balanceTrainingSharpeRatio: number | null;
  balanceTrainingCalmarRatio: number | null;
  balanceTrainingCagr: number | null;
  balanceTrainingMaxDrawdownRatio: number | null;
  balanceValidationSharpeRatio: number | null;
  balanceValidationCalmarRatio: number | null;
  balanceValidationCagr: number | null;
  balanceValidationMaxDrawdownRatio: number | null;
  totalTrades: number | null;
  tickerCount: number | null;
  createdAt: Date | null;
};

export type BacktestDailySnapshot = {
  date: Date;
  cash: number;
  positionsValue: number;
  concurrentTrades?: number;
  missedTradesDueToCash?: number;
};

export type BacktestResultRecord = {
  id: string;
  strategyId: string;
  startDate: Date;
  endDate: Date;
  periodDays: number;
  periodMonths: number;
  initialCapital: number;
  finalPortfolioValue: number;
  performance: Partial<StrategyPerformance> | null;
  dailySnapshots: BacktestDailySnapshot[];
  tickers: string[];
  tickerScope: BacktestScope;
  strategyState?: unknown;
  createdAt: Date;
  trades?: Trade[];
};

export type BacktestCacheLookupResult = {
  sharpeRatio: number;
  calmarRatio: number;
  totalReturn: number;
  cagr: number;
  maxDrawdown: number;
  maxDrawdownRatio: number;
  verifySharpeRatio: number | null;
  verifyCalmarRatio: number | null;
  verifyCagr: number | null;
  verifyMaxDrawdownRatio: number | null;
  balanceTrainingSharpeRatio: number | null;
  balanceTrainingCalmarRatio: number | null;
  balanceTrainingCagr: number | null;
  balanceTrainingMaxDrawdownRatio: number | null;
  balanceValidationSharpeRatio: number | null;
  balanceValidationCalmarRatio: number | null;
  balanceValidationCagr: number | null;
  balanceValidationMaxDrawdownRatio: number | null;
  winRate: number;
  totalTrades: number;
  tickerCount: number;
  startDate: string;
  endDate: string;
  periodDays: number | null;
  periodMonths: number | null;
  parameters: Record<string, unknown>;
};

export type BacktestCacheResultRow = BacktestCacheRow & {
  id: string;
  template_id: string;
  startDate: Date | null;
  endDate: Date | null;
  createdAt: Date | null;
  periodDays: number;
  periodMonths: number;
};

export type BacktestCacheHistoryRow = BacktestCacheRow & {
  id: string;
  template_id: string;
  ticker_count: number;
  startDate: Date;
  endDate: Date;
  duration_minutes: number | null;
  tool: string | null;
  period_months: number | null;
  period_days: number | null;
  createdAt: Date;
};

export type TickerWithCandleStats = TickerInfo & {
  candleCount: number;
  lastCandleDate: Date | null;
  firstCandleDate: Date | null;
  minLow: number | null;
  maxHigh: number | null;
  minClose: number | null;
  maxClose: number | null;
  minUsdVolume: number | null;
  maxUsdVolume: number | null;
  latestClose: number | null;
  latestVolumeShares: number | null;
  latestUsdVolume: number | null;
};

export type TickerAssetType =
  | 'equity'
  | 'etf'
  | 'leveraged_2x'
  | 'leveraged_3x'
  | 'leveraged_5x'
  | 'inverse_etf'
  | 'inverse_leveraged_2x'
  | 'inverse_leveraged_3x'
  | 'inverse_leveraged_5x'
  | 'commodity_trust'
  | 'bond_etf'
  | 'income_etf';

export interface TickerAssetRecord {
  symbol: string;
  name: string | null;
  tradable: boolean;
  shortable: boolean;
  easyToBorrow: boolean;
  assetType: TickerAssetType | null;
  expenseRatio: number | null;
  training: boolean;
}

export type CorporateActionType =
  | 'reverse_split'
  | 'forward_split'
  | 'unit_split'
  | 'cash_dividend'
  | 'stock_dividend'
  | 'spin_off'
  | 'cash_merger'
  | 'stock_merger'
  | 'stock_and_cash_merger'
  | 'redemption'
  | 'name_change'
  | 'worthless_removal'
  | 'rights_distribution';

export interface CorporateActionRecord {
  id: string;
  actionType: CorporateActionType;
  primarySymbol: string;
  relatedSymbols: string[];
  processDate: Date;
  effectiveDate?: Date | null;
  exDate?: Date | null;
  recordDate?: Date | null;
  payableDate?: Date | null;
  payload: Record<string, unknown>;
}

export interface AccountOperationDispatchCandidate {
  operation: AccountOperation;
  account: TradingAccount;
  strategyName: string;
  userId: number | null;
  userEmail: string | null;
}

export interface RemoteOptimizerJobEntity {
  id: string;
  templateId: string;
  templateName: string;
  status: RemoteOptimizationStatus;
  createdAt: Date;
  startedAt?: Date;
  finishedAt?: Date;
  hetznerServerId?: number | null;
  remoteServerIp?: string | null;
}

export type LightgbmModelSource = 'manual' | 'training';

export interface LightgbmDatasetStatsSummary {
  rowCount: number;
  featureCount: number;
  startDate: string | null;
  endDate: string | null;
  labelCounts: Record<string, number>;
}

export interface LightgbmValidationMetricsSummary {
  topK: number;
  positiveRate: number | null;
  positives: number | null;
  totalRows: number | null;
  dayCount: number | null;
  precisionAtK: number | null;
  hitRateAtK: number | null;
  ndcgAtK: number | null;
  avgMaxMultiple: number | null;
}

export interface LightgbmModelRecord {
  id: string;
  name: string;
  treeText: string;
  source: LightgbmModelSource;
  numIterations: number | null;
  learningRate: number | null;
  numLeaves: number | null;
  maxDepth: number | null;
  minDataInLeaf: number | null;
  minGainToSplit: number | null;
  lambdaL1: number | null;
  lambdaL2: number | null;
  featureFraction: number | null;
  baggingFraction: number | null;
  baggingFreq: number | null;
  earlyStoppingRound: number | null;
  trainDatasetStats: LightgbmDatasetStatsSummary | null;
  validationDatasetStats: LightgbmDatasetStatsSummary | null;
  validationMetrics: LightgbmValidationMetricsSummary | null;
  engineStdout: string | null;
  engineStderr: string | null;
  trainedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
  isActive: boolean;
}

export type LightgbmModelCreateInput = Pick<LightgbmModelRecord, 'id' | 'name' | 'treeText' | 'source'> &
  Partial<
    Pick<
      LightgbmModelRecord,
      | 'numIterations'
      | 'learningRate'
      | 'numLeaves'
      | 'maxDepth'
      | 'minDataInLeaf'
      | 'minGainToSplit'
      | 'lambdaL1'
      | 'lambdaL2'
      | 'featureFraction'
      | 'baggingFraction'
      | 'baggingFreq'
      | 'earlyStoppingRound'
      | 'trainDatasetStats'
      | 'validationDatasetStats'
      | 'validationMetrics'
      | 'engineStdout'
      | 'engineStderr'
      | 'trainedAt'
    >
  >;
