import axios, { AxiosError } from 'axios';
import {
  AccountPortfolioHistory,
  AccountPortfolioHistoryRequest,
  AccountPosition,
  AccountSnapshot,
  AccountSnapshotStatus,
  TradingAccount
} from '../../shared/types/Account';
import { AccountOperation, AccountOperationStatus } from '../../shared/types/StrategyTemplate';
import { LoggingService, LogSource } from './LoggingService';
import { AlpacaAccountConnector } from './AlpacaAccountConnector';
import { Database } from '../database/Database';

export type DispatchResult = {
  status: AccountOperationStatus;
  reason?: string;
  orderId?: string | null;
  stopOrderId?: string | null;
  payload?: Record<string, unknown> | null;
  cancelAfter?: Date | string | null;
};

export interface AccountConnector {
  supports(provider: string): boolean;
  fetchSnapshot(account: TradingAccount): Promise<AccountSnapshot>;
  fetchPositions?(account: TradingAccount): Promise<AccountPosition[]>;
  fetchPortfolioHistory?(
    account: TradingAccount,
    options?: AccountPortfolioHistoryRequest
  ): Promise<AccountPortfolioHistory>;
  dispatchOperation?(
    account: TradingAccount,
    operation: AccountOperation,
    abortSignal: AbortSignal
  ): Promise<DispatchResult>;
}

const INTERNAL_SNAPSHOT_SOURCE = 'accounts-service';
const ACCOUNT_SERVICE_LOG_SOURCE: LogSource = 'account-data';

export class AccountDataService {
  private connectors: AccountConnector[];

  private readonly alpacaSkipReasonPatterns = ['not tradable', 'not active', 'trading halt'];

  constructor(
    private loggingService: LoggingService,
    private db: Database
  ) {
    this.connectors = [
      new AlpacaAccountConnector(this.loggingService, this.db)
    ];
  }

  async fetchSnapshots(accounts: TradingAccount[]): Promise<Record<string, AccountSnapshot>> {
    const results = await Promise.all(
      accounts.map(async (account) => {
        const snapshot = await this.fetchSnapshot(account);
        return [account.id, snapshot] as const;
      })
    );
    return Object.fromEntries(results);
  }

  async fetchPortfolioHistory(
    account: TradingAccount,
    options?: AccountPortfolioHistoryRequest
  ): Promise<AccountPortfolioHistory> {
    const connector = this.connectors.find((c) => c.supports(account.provider));
    if (!connector || !connector.fetchPortfolioHistory) {
      throw new Error(`Portfolio history unsupported for provider ${account.provider}`);
    }
    try {
      return await connector.fetchPortfolioHistory(account, options);
    } catch (error) {
      const message = this.extractErrorMessage(error);
      this.loggingService.warn(ACCOUNT_SERVICE_LOG_SOURCE, 'Account portfolio history fetch failed', {
        provider: account.provider,
        accountId: account.id,
        message
      });
      throw new Error(message || 'Unable to load portfolio history');
    }
  }

  async fetchOpenPositions(account: TradingAccount): Promise<AccountPosition[]> {
    const connector = this.connectors.find((c) => c.supports(account.provider));
    if (!connector || !connector.fetchPositions) {
      return [];
    }
    try {
      return await connector.fetchPositions(account);
    } catch (error) {
      const message = this.extractErrorMessage(error);
      this.loggingService.warn(ACCOUNT_SERVICE_LOG_SOURCE, 'Account positions fetch failed', {
        provider: account.provider,
        accountId: account.id,
        message
      });
      return [];
    }
  }

  async dispatchOperation(
    account: TradingAccount,
    operation: AccountOperation,
    abortSignal: AbortSignal
  ): Promise<DispatchResult> {
    const connector = this.connectors.find((c) => c.supports(account.provider));
    if (!connector || !connector.dispatchOperation) {
      return {
        status: 'failed',
        reason: `provider_${(account.provider || 'unknown').toLowerCase()}_unsupported`
      };
    }

    try {
      const result = await connector.dispatchOperation(account, operation, abortSignal);
      return this.normalizeDispatchResult(account, result);
    } catch (error) {
      const failedResult: DispatchResult = {
        status: 'failed',
        reason: this.extractErrorMessage(error),
        payload: this.extractDispatchPayload(error)
      };
      return this.normalizeDispatchResult(account, failedResult);
    }
  }

  private async fetchSnapshot(account: TradingAccount): Promise<AccountSnapshot> {
    const connector = this.connectors.find((c) => c.supports(account.provider));
    if (!connector) {
      return this.buildSnapshot(account, 'unsupported', 'Provider not yet supported', INTERNAL_SNAPSHOT_SOURCE);
    }

    try {
      return await connector.fetchSnapshot(account);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      this.loggingService.warn('system', 'Account snapshot fetch failed', {
        provider: account.provider,
        accountId: account.id,
        message
      });
      return this.buildSnapshot(account, 'error', message, connector.constructor.name);
    }
  }

  private buildSnapshot(
    account: TradingAccount,
    status: AccountSnapshotStatus,
    message: string,
    source: string
  ): AccountSnapshot {
    return {
      accountId: account.id,
      provider: account.provider,
      environment: account.environment,
      balance: null,
      cash: null,
      longMarketValue: null,
      shortMarketValue: null,
      equity: null,
      liquidationValue: null,
      openTrades: null,
      openLongPositions: null,
      openShortPositions: null,
      openOrders: null,
      openBuyOrders: null,
      openSellOrders: null,
      currency: null,
      fetchedAt: new Date(),
      status,
      source: source || INTERNAL_SNAPSHOT_SOURCE,
      message
    };
  }

  private extractErrorMessage(error: unknown): string {
    if (axios.isAxiosError(error)) {
      const axiosError = error as AxiosError;
      const data = axiosError.response?.data as any;
      if (data) {
        if (typeof data === 'string') {
          return data;
        }
        if (typeof data.error === 'string') {
          return data.error;
        }
        if (typeof data.message === 'string') {
          return data.message;
        }
      }
      if (axiosError.message) {
        return axiosError.message;
      }
    }
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  }

  private normalizeDispatchResult(account: TradingAccount, result: DispatchResult): DispatchResult {
    if (
      result.status === 'failed' &&
      typeof result.reason === 'string' &&
      this.isAlpacaProvider(account.provider) &&
      this.shouldSkipDueToAlpacaRestrictions(result.reason)
    ) {
      return {
        ...result,
        status: 'skipped'
      };
    }
    return result;
  }

  private isAlpacaProvider(provider?: string | null): boolean {
    return typeof provider === 'string' && provider.trim().toLowerCase() === 'alpaca';
  }

  private shouldSkipDueToAlpacaRestrictions(reason: string): boolean {
    const normalized = reason.toLowerCase();
    return this.alpacaSkipReasonPatterns.some((phrase) => normalized.includes(phrase));
  }

  private extractDispatchPayload(error: unknown): Record<string, unknown> | null {
    if (error && typeof error === 'object' && error !== null) {
      const payload = (error as { dispatchPayload?: unknown }).dispatchPayload;
      if (payload && typeof payload === 'object') {
        return payload as Record<string, unknown>;
      }
    }
    return null;
  }
}
