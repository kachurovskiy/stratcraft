import { Request, Response } from 'express';
import { Database } from '../../server/database/Database';
import { StrategyRegistry } from '../../server/strategies/registry';
import { AccountDataService } from '../../server/services/AccountDataService';

// Extended Express Request interface with custom properties
export interface StratCraftRequest extends Request {
  db: Database;
  strategyRegistry: StrategyRegistry;
  accountDataService: AccountDataService;
}

// Extended Express Response interface (for future use if needed)
export interface StratCraftResponse extends Response {}

// Common query parameter types
export interface TickerQueryParams {
  search?: string;
  sort?: 'volumeUsd' | 'symbol' | 'maxFluctuationRatio' | 'candleCount' | 'lastCandleDate' | 'tradeCount';
  sortDirection?: 'asc' | 'desc';
  page?: string;
}

// Route parameter types
export interface TickerParams extends Record<string, string> {
  symbol: string;
}

export interface StrategyParams extends Record<string, string> {
  id: string;
}

export interface TradeParams extends Record<string, string> {
  id: string;
}

export interface AccountParams extends Record<string, string> {
  id: string;
}

export interface StrategyIdParams extends Record<string, string> {
  strategyId: string;
}

export interface TemplateParams extends Record<string, string> {
  templateId: string;
}

export interface BacktestParams extends Record<string, string> {
  backtestId: string;
}
