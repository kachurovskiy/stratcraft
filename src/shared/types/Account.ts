export type AccountEnvironment = 'paper' | 'live' | string;

export interface TradingAccount {
  id: string;
  userId: number;
  name: string;
  provider: string;
  environment: AccountEnvironment;
  excludedTickers: string[];
  excludedKeywords: string[];
  apiKey: string;
  apiSecret: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface CreateTradingAccountInput {
  id: string;
  userId: number;
  name: string;
  provider: string;
  environment: AccountEnvironment;
  excludedTickers: string[];
  excludedKeywords: string[];
  apiKey: string;
  apiSecret: string;
}

export type AccountSnapshotStatus = 'ready' | 'error' | 'unsupported';

export interface AccountSnapshot {
  accountId: string;
  provider: string;
  environment: AccountEnvironment;
  balance: number | null;
  cash: number | null;
  longMarketValue: number | null;
  shortMarketValue: number | null;
  equity: number | null;
  liquidationValue: number | null;
  openTrades: number | null;
  openLongPositions: number | null;
  openShortPositions: number | null;
  openOrders: number | null;
  openBuyOrders: number | null;
  openSellOrders: number | null;
  currency: string | null;
  fetchedAt: Date;
  status: AccountSnapshotStatus;
  source: string;
  message?: string;
}

export type AccountPortfolioHistoryPoint = {
  timestamp: number;
  equity: number | null;
};

export interface AccountPortfolioHistory {
  currency: string | null;
  timeframe: string | null;
  baseValue: number | null;
  baseValueAsOf: string | null;
  startAt: string | null;
  endAt: string | null;
  points: AccountPortfolioHistoryPoint[];
}

export interface AccountPortfolioHistoryRequest {
  period?: string;
  timeframe?: string;
  intradayReporting?: 'market_hours' | 'extended_hours' | 'continuous';
  start?: string;
  end?: string;
}

export type AccountPositionSide = 'long' | 'short';

export interface AccountPosition {
  ticker: string;
  side: AccountPositionSide;
  quantity: number;
  marketValue: number | null;
  averageEntryPrice: number | null;
  costBasis: number | null;
  unrealizedPnl: number | null;
}
