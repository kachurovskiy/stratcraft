export type AccountOperationType =
  | 'open_position'
  | 'close_position'
  | 'update_stop_loss';

export type AccountOperationStatus = 'pending' | 'sent' | 'skipped' | 'failed';

export interface AccountOperation {
  id: string;
  accountId: string;
  strategyId: string;
  tradeId: string;
  ticker: string;
  operationType: AccountOperationType;
  quantity?: number | null;
  price?: number | null;
  stopLoss?: number | null;
  previousStopLoss?: number | null;
  triggeredAt: Date;
  status: AccountOperationStatus;
  statusReason?: string | null;
  statusUpdatedAt: Date;
  attemptCount: number;
  lastAttemptAt?: Date | null;
  reason?: string | null;
  orderId?: string | null;
  lastPayload?: Record<string, unknown> | null;
  entryOrderId?: string | null;
  stopOrderId?: string | null;
  exitOrderId?: string | null;
  orderType?: 'market' | 'limit' | null;
  discountApplied?: boolean | null;
  signalConfidence?: number | null;
  accountCashAtPlan?: number | null;
  daysHeld?: number | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface StrategyParameter {
  name: string;
  type: 'number' | 'string';
  label: string;
  description: string;
  required: boolean;
  default?: string | number;
  min?: number;
  max?: number;
  step?: number;
}

export interface StrategyTemplate {
  id: string;
  name: string;
  description: string;
  category: string;
  author: string;
  version: string;
  enabled?: boolean;
  localOptimizationVersion?: number;
  parameters: StrategyParameter[];
  exampleUsage?: string;
}

export interface Strategy {
  id: string;
  name: string;
  templateId: string;
  userId: number | null;
  accountId?: string | null;
  backtestStartDate?: Date | null;
  parameters: Record<string, string | number | boolean>;
  status: 'active' | 'inactive' | 'error';
  createdAt: Date;
  updatedAt: Date;
  // Duration in minutes for the most recent backtest run (if known)
  lastBacktestDurationMinutes?: number;
  performance?: StrategyPerformance;
  backtestHistory?: BacktestResult[];
  accountOperations?: AccountOperation[];
}

export interface StrategyPerformance {
  totalTrades: number;
  winningTrades: number;
  losingTrades: number;
  winRate: number;
  totalReturn: number;
  cagr: number;
  sharpeRatio: number;
  calmarRatio: number;
  maxDrawdown: number;
  maxDrawdownPercent: number; // in percentage
  avgTradeReturn: number;
  bestTrade: number;
  worstTrade: number;
  totalTickers: number;
  medianTradeDuration: number; // in days
  medianTradePnl: number; // in dollars
  medianTradePnlPercent: number; // in percentage
  medianConcurrentTrades: number;
  avgTradeDuration: number; // in days
  avgTradePnl: number; // average PNL per trade
  avgTradePnlPercent: number; // average PNL per trade in percentage
  avgConcurrentTrades: number; // average concurrent trades
  avgLosingPnl: number; // average PNL for losing trades
  avgLosingPnlPercent: number; // average PNL for losing trades in percentage
  avgWinningPnl: number; // average PNL for winning trades
  avgWinningPnlPercent: number; // average PNL for winning trades in percentage
  backtestCompletionReason?: string; // Reason why backtest finished (e.g., 'last candle processed', 'early stop triggered', 'timeout')
  backtestId?: string;
  lastUpdated: Date;
  backtestStartDate?: Date;
  backtestEndDate?: Date;
  tickerScope?: BacktestScope;
}

export type BacktestScope = 'training' | 'validation' | 'all' | 'live';

export interface BacktestDataPoint {
  date: Date;
  portfolioValue: number;
  cash: number;
  positionsValue: number;
  concurrentTrades: number;
  missedTradesDueToCash: number;
}

export interface StrategyStateSnapshot {
  templateId: string;
  data: unknown;
}

export interface BacktestResult {
  id: string;
  strategyId: string;
  startDate: Date;
  endDate: Date;
  initialCapital: number;
  finalPortfolioValue: number;
  performance: StrategyPerformance;
  dailySnapshots: BacktestDataPoint[];
  trades: Trade[];
  tickers: string[]; // Store the list of tickers considered in this backtest
  tickerScope?: BacktestScope;
  strategyState?: StrategyStateSnapshot;
  createdAt: Date;
}

export interface TradeChange {
  field: string;
  oldValue: unknown;
  newValue: unknown;
  changedAt: Date;
}

export interface Trade {
  id: string;
  strategyId: string;
  userId: number | null;
  backtestResultId?: string;
  ticker: string;
  quantity: number;
  price: number;
  date: Date;
  status: 'pending' | 'active' | 'closed' | 'cancelled';
  pnl?: number;
  fee?: number | null;
  exitPrice?: number;
  exitDate?: Date;
  stopLoss?: number;
  stopLossTriggered?: boolean;
  entryOrderId?: string | null;
  entryCancelAfter?: Date | null;
  stopOrderId?: string | null;
  exitOrderId?: string | null;
  createdAt: Date;
  changes: TradeChange[];
}

export interface Candle {
  ticker: string;
  date: Date;
  open: number;
  high: number;
  low: number;
  close: number;
  unadjustedClose?: number;
  volumeShares: number;
}

export interface TickerInfo {
  symbol: string;
  name?: string | null;
  tradable: boolean;
  shortable: boolean;
  easyToBorrow: boolean;
  assetType?:
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
    | 'income_etf'
    | null;
  expenseRatio?: number | null;
  marketCap?: number;
  volumeUsd?: number;
  maxFluctuationRatio?: number;
  lastUpdated?: Date;
  training: boolean;
  candleCount?: number;
  firstCandleDate?: Date | null;
  lastCandleDate?: Date | null;
  minLow?: number | null;
  maxHigh?: number | null;
  minClose?: number | null;
  maxClose?: number | null;
  minUsdVolume?: number | null;
  maxUsdVolume?: number | null;
  latestClose?: number | null;
  latestVolumeShares?: number | null;
  latestUsdVolume?: number | null;
  tradeCount?: number;
}
