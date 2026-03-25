import axios from 'axios';
import {
  AccountEnvironment,
  AccountPosition,
  AccountSnapshot,
  TradingAccount
} from '../../shared/types/Account';
import { AccountOperation } from '../../shared/types/StrategyTemplate';
import { DEFAULT_MARKET_ORDER_PRICE_CAP_RATIO, SETTING_KEYS } from '../constants';
import { Database } from '../database/Database';
import { LoggingService } from './LoggingService';
import type { AccountConnector, DispatchResult, LiquidationRequest, LiquidationResult } from './AccountDataService';

type AlpacaOrder = {
  id?: string;
  order_id?: string;
  client_order_id?: string;
  symbol?: string;
  qty?: string;
  side?: string;
  type?: string;
  stop_price?: string;
  status?: string;
  order_class?: string;
  [key: string]: any;
};

export class AlpacaAccountConnector implements AccountConnector {
  private readonly requestTimeout = 5000;
  private readonly orderRequestTimeout = 10000;
  private readonly defaultPageSize = 500;
  private readonly maxPaginationPages = 100;

  constructor(
    private loggingService: LoggingService,
    private db: Database
  ) {}

  supports(provider: string): boolean {
    return provider.trim().toLowerCase() === 'alpaca';
  }

  async fetchSnapshot(account: TradingAccount): Promise<AccountSnapshot> {
    const baseUrl = await this.getBaseUrl(account.environment);
    const headers = this.buildHeaders(account);

    const accountResponse = await axios.get(`${baseUrl}/account`, {
      headers,
      timeout: this.requestTimeout
    });

    let openTrades: number | null = null;
    let openLongPositions: number | null = null;
    let openShortPositions: number | null = null;
    let openOrders: number | null = null;
    let openBuyOrders: number | null = null;
    let openSellOrders: number | null = null;
    try {
      const { total, long, short, truncated } = await this.fetchPositionCounts(baseUrl, headers);
      openTrades = total;
      openLongPositions = long;
      openShortPositions = short;
      if (truncated) {
        this.loggingService.warn('system', 'Alpaca positions truncated due to pagination limit', {
          provider: account.provider,
          accountId: account.id,
          counted: total
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to fetch positions';
      this.loggingService.warn('system', 'Alpaca positions fetch failed', {
        provider: account.provider,
        accountId: account.id,
        message
      });
    }

    try {
      const { total, buy, sell, truncated } = await this.fetchOpenOrderCounts(baseUrl, headers);
      openOrders = total;
      openBuyOrders = buy;
      openSellOrders = sell;
      if (truncated) {
        this.loggingService.warn('system', 'Alpaca open orders truncated due to pagination limit', {
          provider: account.provider,
          accountId: account.id,
          counted: total
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to fetch orders';
      this.loggingService.warn('system', 'Alpaca open orders fetch failed', {
        provider: account.provider,
        accountId: account.id,
        message
      });
    }

    const accountData = accountResponse.data ?? {};
    const cash = this.toNumber(accountData?.cash);
    const longMarketValue = this.toNumber(accountData?.long_market_value);
    const shortMarketValue = this.toNumber(accountData?.short_market_value);
    const equity = this.toNumber(accountData?.equity);
    const liquidationValue = this.toNumber(accountData?.equity);
    const balance = cash;
    const currency = this.toCurrency(accountData?.currency);

    return {
      accountId: account.id,
      provider: account.provider,
      environment: account.environment,
      balance,
      cash,
      longMarketValue,
      shortMarketValue,
      equity,
      liquidationValue,
      openTrades,
      openLongPositions,
      openShortPositions,
      openOrders,
      openBuyOrders,
      openSellOrders,
      currency,
      fetchedAt: new Date(),
      status: 'ready',
      source: 'alpaca'
    };
  }

  async fetchPositions(account: TradingAccount): Promise<AccountPosition[]> {
    const baseUrl = await this.getBaseUrl(account.environment);
    const headers = this.buildHeaders(account);
    const positions: AccountPosition[] = [];
    const { truncated } = await this.collectPaginatedResource(
      baseUrl,
      headers,
      '/positions',
      {
        dataKeys: ['positions', 'data'],
        onItems: (items) => {
          for (const item of items) {
            if (!item || typeof item !== 'object') {
              continue;
            }
            const ticker = typeof item.symbol === 'string' ? item.symbol.trim().toUpperCase() : null;
            if (!ticker) {
              continue;
            }
            const rawSide = typeof item.side === 'string' ? item.side.trim().toLowerCase() : null;
            const side: AccountPosition['side'] = rawSide === 'short' ? 'short' : 'long';
            const quantity = this.toNumber(item.qty);
            positions.push({
              ticker,
              side,
              quantity: quantity !== null ? Math.abs(quantity) : 0,
              marketValue: this.toNumber(item.market_value),
              averageEntryPrice: this.toNumber(item.avg_entry_price),
              costBasis: this.toNumber(item.cost_basis),
              unrealizedPnl: this.toNumber(item.unrealized_pl)
            });
          }
        }
      }
    );
    if (truncated) {
      this.loggingService.warn('system', 'Alpaca positions truncated while fetching details', {
        provider: account.provider,
        accountId: account.id,
        counted: positions.length
      });
    }
    return positions;
  }

  async dispatchOperation(
    account: TradingAccount,
    operation: AccountOperation,
    abortSignal: AbortSignal
  ): Promise<DispatchResult> {
    const baseUrl = await this.getBaseUrl(account.environment);
    const headers = this.buildHeaders(account);
    const ticker = this.normalizeTicker(operation.ticker);
    if (!ticker) {
      throw new Error('missing_ticker');
    }

    if (operation.operationType === 'close_position' || operation.operationType === 'update_stop_loss') {
      const positionExists = await this.hasOpenPosition(baseUrl, headers, ticker, abortSignal);
      if (!positionExists) {
        const reason =
          operation.operationType === 'close_position'
            ? `${ticker} position not found on Alpaca`
            : `${ticker} position not found on Alpaca for stop update`;
        return {
          status: 'skipped',
          reason
        };
      }
    }

    if (operation.operationType === 'close_position') {
      await this.cancelExistingStopLossOrder(
        baseUrl,
        headers,
        account,
        operation,
        ticker,
        abortSignal
      );
    }

    const marketOrderPriceCapRatio = await this.resolveMarketOrderPriceCapRatio();
    const payload = this.buildAlpacaOrderPayload(operation, ticker, marketOrderPriceCapRatio);

    if (operation.operationType === 'update_stop_loss') {
      return this.replaceStopLossOrder(
        baseUrl,
        headers,
        account,
        operation,
        ticker,
        payload,
        abortSignal
      );
    }

    let response;
    try {
      response = await axios.post(
        `${baseUrl}/orders`,
        payload,
        {
          headers,
          timeout: this.orderRequestTimeout,
          signal: abortSignal
        }
      );
    } catch (error) {
      this.attachDispatchPayload(error, payload);
      throw error;
    }

    // Wait 300ms to stay under rate limits
    await new Promise((resolve) => setTimeout(resolve, 300));

    const orderId = response.data?.id ?? response.data?.order_id ?? response.data?.client_order_id;
    const stopOrderId =
      operation.operationType === 'open_position'
        ? this.extractStopLossOrderId(response.data)
        : null;
    let cancelAfter: Date | null = null;
    if (operation.operationType === 'open_position') {
      cancelAfter = await this.fetchNextMarketClose(baseUrl, headers, account, abortSignal);
    }

    return {
      status: 'sent',
      reason: orderId ? `Order ${orderId}` : undefined,
      orderId: orderId ?? null,
      stopOrderId,
      payload,
      cancelAfter
    };
  }

  async liquidatePositions(
    account: TradingAccount,
    request: LiquidationRequest,
    abortSignal?: AbortSignal
  ): Promise<LiquidationResult> {
    const baseUrl = await this.getBaseUrl(account.environment);
    const headers = this.buildHeaders(account);
    const dryRun = Boolean(request.dryRun);
    const cancelledOrders = dryRun ? null : await this.cancelAllOpenOrders(baseUrl, headers, abortSignal);

    const positions = await this.fetchPositions(account);
    const totalPositions = positions.length;
    let submittedOrders = 0;
    let plannedOrders = 0;
    let skippedDeviationPositions = 0;
    let skippedMissingPricePositions = 0;
    let skippedMissingReferencePositions = 0;
    let failedOrders = 0;
    let expectedLiquidationValue = 0;
    const orders: LiquidationResult['orders'] = [];

    const dataBaseUrl = await this.getMarketDataBaseUrl();
    const dataHeaders = this.buildHeaders(account);
    const discountPercent = Number.isFinite(request.discountPercent) ? request.discountPercent : 0;
    const discountRatio = Math.max(0, discountPercent) / 100;
    const deviationPercent = Number.isFinite(request.deviationBandPercent) ? request.deviationBandPercent! : 0;
    const deviationRatio = Math.max(0, deviationPercent) / 100;
    const lastCloseByTicker = await this.db.candles.getLastCloseByTickerOnOrBeforeDate(
      positions.map(position => position.ticker),
      new Date()
    );

    for (const position of positions) {
      const side: 'buy' | 'sell' = position.side === 'short' ? 'buy' : 'sell';
      const quantity = this.normalizeQuantity(position.quantity);
      if (quantity === null) {
        failedOrders += 1;
        orders.push({
          ticker: position.ticker,
          side,
          quantity: position.quantity ?? 0,
          latestPrice: null,
          lastClose: lastCloseByTicker[position.ticker] ?? null,
          limitPrice: null,
          status: 'failed',
          statusReason: 'Invalid quantity'
        });
        continue;
      }

      let latestPrice: number | null = null;
      try {
        latestPrice = await this.fetchLatestTradePrice(dataBaseUrl, dataHeaders, position.ticker, abortSignal);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.loggingService.warn('system', 'Alpaca latest price fetch failed', {
          provider: account.provider,
          accountId: account.id,
          ticker: position.ticker,
          message
        });
      }

      if (!latestPrice || !Number.isFinite(latestPrice)) {
        skippedMissingPricePositions += 1;
        orders.push({
          ticker: position.ticker,
          side,
          quantity,
          latestPrice: null,
          lastClose: lastCloseByTicker[position.ticker] ?? null,
          limitPrice: null,
          status: 'skipped',
          statusReason: 'Missing latest price'
        });
        continue;
      }

      const lastCloseRaw = lastCloseByTicker[position.ticker];
      const lastCloseValue =
        typeof lastCloseRaw === 'number' && Number.isFinite(lastCloseRaw) ? lastCloseRaw : null;
      if (lastCloseValue === null || lastCloseValue <= 0) {
        skippedMissingReferencePositions += 1;
        orders.push({
          ticker: position.ticker,
          side,
          quantity,
          latestPrice,
          lastClose: lastCloseValue,
          limitPrice: null,
          status: 'skipped',
          statusReason: 'Missing last close reference'
        });
        continue;
      }
      const lastClose = lastCloseValue;

      if (deviationRatio > 0) {
        if (position.side === 'short') {
          if (latestPrice > lastClose * (1 + deviationRatio)) {
            skippedDeviationPositions += 1;
            orders.push({
              ticker: position.ticker,
              side,
              quantity,
              latestPrice,
              lastClose,
              limitPrice: null,
              status: 'skipped',
              statusReason: 'Latest price above deviation band'
            });
            continue;
          }
        } else {
          if (latestPrice < lastClose * (1 - deviationRatio)) {
            skippedDeviationPositions += 1;
            orders.push({
              ticker: position.ticker,
              side,
              quantity,
              latestPrice,
              lastClose,
              limitPrice: null,
              status: 'skipped',
              statusReason: 'Latest price below deviation band'
            });
            continue;
          }
        }
      }

      const limitPriceTarget =
        position.side === 'short'
          ? latestPrice * (1 + discountRatio)
          : latestPrice * (1 - discountRatio);
      if (!Number.isFinite(limitPriceTarget) || limitPriceTarget <= 0) {
        failedOrders += 1;
        orders.push({
          ticker: position.ticker,
          side,
          quantity,
          latestPrice,
          lastClose,
          limitPrice: null,
          status: 'failed',
          statusReason: 'Invalid limit price'
        });
        continue;
      }

      let payload: Record<string, any>;
      let limitPrice: number | null = null;
      try {
        payload = this.buildLimitOrderPayload(position.ticker, quantity, limitPriceTarget, side);
        limitPrice = payload.limit_price ?? limitPriceTarget;
      } catch (error) {
        failedOrders += 1;
        orders.push({
          ticker: position.ticker,
          side,
          quantity,
          latestPrice,
          lastClose,
          limitPrice: null,
          status: 'failed',
          statusReason: 'Failed to build order'
        });
        continue;
      }
      if (dryRun) {
        plannedOrders += 1;
        orders.push({
          ticker: position.ticker,
          side,
          quantity,
          latestPrice,
          lastClose,
          limitPrice,
          status: 'dry-run',
          statusReason: 'Dry run'
        });
      } else {
        try {
          await axios.post(
            `${baseUrl}/orders`,
            payload,
            {
              headers,
              timeout: this.orderRequestTimeout,
              signal: abortSignal
            }
          );
          submittedOrders += 1;
          orders.push({
            ticker: position.ticker,
            side,
            quantity,
            latestPrice,
            lastClose,
            limitPrice,
            status: 'submitted',
            statusReason: 'Submitted'
          });
        } catch (error) {
          this.attachDispatchPayload(error, payload);
          failedOrders += 1;
          orders.push({
            ticker: position.ticker,
            side,
            quantity,
            latestPrice,
            lastClose,
            limitPrice,
            status: 'failed',
            statusReason: 'Submit failed'
          });
          continue;
        }

        await new Promise((resolve) => setTimeout(resolve, 300));
      }

      if (limitPrice !== null) {
        const signedValue = (position.side === 'short' ? -1 : 1) * limitPrice * quantity;
        expectedLiquidationValue += signedValue;
      }
    }

    return {
      cancelledOrders,
      totalPositions,
      submittedOrders,
      plannedOrders,
      skippedDeviationPositions,
      skippedMissingPricePositions,
      skippedMissingReferencePositions,
      failedOrders,
      expectedLiquidationValue,
      dryRun,
      orders
    };
  }

  private async getBaseUrl(environment: AccountEnvironment): Promise<string> {
    const normalized = typeof environment === 'string' ? environment.trim().toLowerCase() : '';
    const isLive = normalized === 'live';
    const settingKey = isLive ? SETTING_KEYS.ALPACA_LIVE_URL : SETTING_KEYS.ALPACA_PAPER_URL;
    const configured = await this.db.settings.getRequiredSettingValue(settingKey);
    return configured.trim();
  }

  private async resolveMarketOrderPriceCapRatio(): Promise<number> {
    const rawValue = await this.db.settings.getSettingValue(SETTING_KEYS.MARKET_ORDER_PRICE_CAP_RATIO);
    const trimmed = typeof rawValue === 'string' ? rawValue.trim() : '';
    if (!trimmed) {
      return DEFAULT_MARKET_ORDER_PRICE_CAP_RATIO;
    }
    const parsed = Number(trimmed);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return parsed;
    }
    return DEFAULT_MARKET_ORDER_PRICE_CAP_RATIO;
  }

  private buildHeaders(account: TradingAccount) {
    return {
      'APCA-API-KEY-ID': account.apiKey,
      'APCA-API-SECRET-KEY': account.apiSecret
    };
  }

  private async getMarketDataBaseUrl(): Promise<string> {
    const baseUrl = await this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_DATA_BASE_URL);
    const trimmedBase = baseUrl.replace(/\/+$/, '');
    if (trimmedBase.endsWith('/v2/stocks')) {
      return trimmedBase;
    }
    if (trimmedBase.endsWith('/v2')) {
      return `${trimmedBase}/stocks`;
    }
    return `${trimmedBase}/v2/stocks`;
  }

  private async fetchLatestTradePrice(
    baseUrl: string,
    headers: Record<string, string>,
    ticker: string,
    abortSignal?: AbortSignal
  ): Promise<number | null> {
    const response = await axios.get(
      `${baseUrl}/trades/latest`,
      {
        headers,
        params: { symbols: ticker },
        timeout: this.requestTimeout,
        signal: abortSignal
      }
    );
    const data = response.data ?? {};
    const trade = data.trades?.[ticker];
    const price = this.toNumber(trade?.p);
    return price !== null && price > 0 ? price : null;
  }

  private buildLimitOrderPayload(
    ticker: string,
    quantity: number,
    limitPrice: number,
    side: 'buy' | 'sell'
  ): Record<string, any> {
    if (!Number.isFinite(quantity) || quantity <= 0) {
      throw new Error('invalid_quantity');
    }
    if (!Number.isFinite(limitPrice) || limitPrice <= 0) {
      throw new Error('invalid_price');
    }

    return {
      symbol: ticker,
      qty: Math.abs(quantity).toString(),
      side,
      type: 'limit',
      limit_price: this.normalizeOrderPrice(limitPrice),
      time_in_force: 'day',
      // Alpaca: extended_hours only valid for limit + day orders.
      extended_hours: true
    };
  }

  private normalizeTicker(value?: string | null): string | null {
    if (typeof value !== 'string') {
      return null;
    }
    const normalized = value.trim().toUpperCase();
    return normalized.length > 0 ? normalized : null;
  }

  private parseIsoTimestamp(value: unknown): Date | null {
    if (typeof value !== 'string') {
      return null;
    }
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  private async hasOpenPosition(
    baseUrl: string,
    headers: Record<string, string>,
    ticker: string,
    abortSignal?: AbortSignal
  ): Promise<boolean> {
    try {
      await axios.get(`${baseUrl}/positions/${ticker}`, {
        headers,
        timeout: this.requestTimeout,
        signal: abortSignal
      });
      return true;
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return false;
      }
      throw error;
    }
  }

  private toNumber(value: any): number | null {
    if (value === undefined || value === null) {
      return null;
    }
    const asNumber = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(asNumber) ? asNumber : null;
  }

  private toCurrency(value: any): string | null {
    if (!value) {
      return 'USD';
    }
    return String(value).toUpperCase();
  }

  private buildAlpacaOrderPayload(
    operation: AccountOperation,
    ticker: string,
    marketOrderPriceCapRatio: number
  ) {
    const quantity = operation.quantity ?? 0;
    if (!Number.isFinite(quantity) || quantity <= 0) {
      throw new Error('invalid_quantity');
    }

    const payload: Record<string, any> = {
      symbol: ticker,
      qty: Math.abs(quantity).toString(),
      time_in_force: 'gtc',
      extended_hours: false
    };

    switch (operation.operationType) {
      case 'open_position': {
        payload.side = 'buy';
        const metadataOrderType = this.getOperationOrderType(operation);
        const limitPrice =
          metadataOrderType === 'limit' && this.isValidOrderPrice(operation.price)
            ? operation.price!
            : this.resolveMarketOrderLimitPrice(operation, 'buy', marketOrderPriceCapRatio);
        if (limitPrice !== null) {
          payload.type = 'limit';
          payload.limit_price = this.normalizeOrderPrice(limitPrice);
        } else {
          payload.type = 'market';
        }
        const stopLossPrice = this.extractStopLossPrice(operation.stopLoss);
        if (stopLossPrice !== null) {
          payload.order_class = 'oto';
          payload.stop_loss = {
            stop_price: stopLossPrice
          };
        }
        break;
      }
      case 'close_position': {
        payload.side = 'sell';
        const metadataOrderType = this.getOperationOrderType(operation);
        const limitPrice =
          metadataOrderType === 'limit' && this.isValidOrderPrice(operation.price)
            ? operation.price!
            : this.resolveMarketOrderLimitPrice(operation, 'sell', marketOrderPriceCapRatio);
        if (limitPrice !== null) {
          payload.type = 'limit';
          payload.limit_price = this.normalizeOrderPrice(limitPrice);
        } else {
          payload.type = 'market';
        }
        break;
      }
      case 'update_stop_loss': {
        const stopPrice = operation.stopLoss ?? operation.price;
        if (!stopPrice || !Number.isFinite(stopPrice)) {
          throw new Error('missing_stop_price');
        }
        payload.side = 'sell';
        payload.type = 'stop';
        payload.stop_price = this.normalizeOrderPrice(stopPrice);
        break;
      }
      default:
        throw new Error(`unsupported_operation_type_${operation.operationType}`);
    }

    return payload;
  }

  private async fetchNextMarketClose(
    baseUrl: string,
    headers: Record<string, string>,
    account: TradingAccount,
    abortSignal: AbortSignal
  ): Promise<Date | null> {
    try {
      const response = await axios.get(`${baseUrl}/clock`, {
        headers,
        timeout: this.requestTimeout,
        signal: abortSignal
      });
      const nextCloseRaw = response.data?.next_close ?? response.data?.nextClose;
      const nextClose = this.parseIsoTimestamp(nextCloseRaw);
      if (nextClose) {
        return nextClose;
      }
      this.loggingService.warn('system', 'Alpaca clock response missing next close', {
        provider: account.provider,
        accountId: account.id,
        raw: nextCloseRaw ?? null
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      this.loggingService.warn('system', 'Failed to fetch Alpaca market clock for cancel-after', {
        provider: account.provider,
        accountId: account.id,
        message
      });
    }
    return null;
  }

  private async cancelExistingStopLossOrder(
    baseUrl: string,
    headers: Record<string, string>,
    account: TradingAccount,
    operation: AccountOperation,
    ticker: string,
    abortSignal: AbortSignal
  ): Promise<void> {
    const desiredSide =
      typeof operation.quantity === 'number' && operation.quantity < 0 ? 'buy' : 'sell';
    let orderId = this.normalizeOrderId(operation.stopOrderId);
    const quantity = this.normalizeQuantity(operation.quantity);

    if (!orderId && quantity !== null) {
      const fallbackOrder = await this.findOpenStopLossOrder(
        baseUrl,
        headers,
        ticker,
        quantity,
        desiredSide,
        null,
        abortSignal
      );
      orderId = this.extractOrderId(fallbackOrder);
      if (!orderId) {
        this.loggingService.info('system', 'No existing stop loss order found before close', {
          provider: account.provider,
          accountId: account.id,
          ticker,
          quantity
        });
      }
    }

    if (!orderId) {
      return;
    }

    try {
      await axios.delete(`${baseUrl}/orders/${orderId}`, {
        headers,
        timeout: this.orderRequestTimeout,
        signal: abortSignal
      });
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        this.loggingService.warn('system', 'Stop loss order already absent before close', {
          provider: account.provider,
          accountId: account.id,
          ticker,
          orderId
        });
        return;
      }
      this.attachDispatchPayload(error, { cancelOrderId: orderId });
      throw error;
    }

    await new Promise((resolve) => setTimeout(resolve, 300));
  }

  private async replaceStopLossOrder(
    baseUrl: string,
    headers: Record<string, string>,
    account: TradingAccount,
    operation: AccountOperation,
    ticker: string,
    payload: Record<string, any>,
    abortSignal: AbortSignal
  ): Promise<DispatchResult> {
    const quantity = operation.quantity ?? 0;
    if (!Number.isFinite(quantity) || quantity === 0) {
      throw new Error('invalid_quantity');
    }

    const previousStopPrice = this.extractStopLossPrice(operation.previousStopLoss);
    const payloadStopPrice = payload.stop_price;
    if (payloadStopPrice === undefined) {
      throw new Error('missing_stop_price');
    }

    const desiredSide =
      typeof payload.side === 'string'
        ? payload.side.trim().toLowerCase()
        : quantity < 0
          ? 'buy'
          : 'sell';

    const existingOrder = await this.findOpenStopLossOrder(
      baseUrl,
      headers,
      ticker,
      Math.abs(quantity),
      desiredSide,
      previousStopPrice,
      abortSignal
    );

    if (!existingOrder) {
      return this.submitStopLossOrder(
        baseUrl,
        headers,
        payload,
        abortSignal
      );
    }

    const orderId = existingOrder.id ?? existingOrder.order_id;
    if (!orderId) {
      this.loggingService.warn('system', 'Alpaca stop loss order missing ID for update', {
        provider: account.provider,
        accountId: account.id,
        ticker,
        quantity,
        previousStopPrice
      });
      return {
        status: 'skipped',
        reason: 'Stop loss order id unavailable for update'
      };
    }

    const patchPayload: Record<string, any> = {
      stop_price: payloadStopPrice,
      time_in_force: 'gtc'
    };

    let response;
    try {
      response = await axios.patch(
        `${baseUrl}/orders/${orderId}`,
        patchPayload,
        {
          headers,
          timeout: this.orderRequestTimeout,
          signal: abortSignal
        }
      );
    } catch (error) {
      this.attachDispatchPayload(error, patchPayload);
      throw error;
    }

    await new Promise((resolve) => setTimeout(resolve, 300));

    const responseOrderId = response.data?.id ?? response.data?.order_id ?? response.data?.client_order_id ?? orderId;

    return {
      status: 'sent',
      reason: responseOrderId ? `Order ${responseOrderId}` : undefined,
      orderId: responseOrderId ?? null,
      payload: patchPayload
    };
  }

  private async submitStopLossOrder(
    baseUrl: string,
    headers: Record<string, string>,
    payload: Record<string, any>,
    abortSignal: AbortSignal
  ): Promise<DispatchResult> {
    let response;
    try {
      response = await axios.post(
        `${baseUrl}/orders`,
        payload,
        {
          headers,
          timeout: this.orderRequestTimeout,
          signal: abortSignal
        }
      );
    } catch (error) {
      this.attachDispatchPayload(error, payload);
      throw error;
    }

    await new Promise((resolve) => setTimeout(resolve, 300));

    const orderId =
      response.data?.id ??
      response.data?.order_id ??
      response.data?.client_order_id ??
      null;

    return {
      status: 'sent',
      reason: orderId ? `Order ${orderId}` : undefined,
      orderId,
      payload
    };
  }

  private async findOpenStopLossOrder(
    baseUrl: string,
    headers: Record<string, string>,
    ticker: string,
    quantity: number,
    side: string,
    previousStopPrice: number | null,
    abortSignal: AbortSignal
  ): Promise<AlpacaOrder | null> {
    const normalizedTicker = ticker.toUpperCase();
    const desiredSide = typeof side === 'string' && side.trim().toLowerCase() === 'buy' ? 'buy' : 'sell';

    const params: Record<string, string | number | boolean> = {
      status: 'open',
      limit: 500,
      nested: false,
      symbols: normalizedTicker
    };
    if (desiredSide) {
      params.side = desiredSide;
    }

    const response = await axios.get(`${baseUrl}/orders`, {
      headers,
      timeout: this.requestTimeout,
      signal: abortSignal,
      params
    });

    if (!Array.isArray(response.data)) {
      return null;
    }

    for (const rawOrder of response.data as AlpacaOrder[]) {
      if (!rawOrder || typeof rawOrder !== 'object') {
        continue;
      }

      const symbol = typeof rawOrder.symbol === 'string' ? rawOrder.symbol.trim().toUpperCase() : null;
      if (symbol !== normalizedTicker) {
        continue;
      }

      const type = typeof rawOrder.type === 'string' ? rawOrder.type.trim().toLowerCase() : null;
      if (type !== 'stop' && type !== 'stop_limit') {
        continue;
      }

      const orderSide = typeof rawOrder.side === 'string' ? rawOrder.side.trim().toLowerCase() : null;
      if (orderSide && orderSide !== desiredSide) {
        continue;
      }

      const parsedQty = this.toNumber(rawOrder.qty);
      if (parsedQty === null || !this.areQuantitiesClose(Math.abs(parsedQty), quantity)) {
        continue;
      }

      if (previousStopPrice !== null) {
        const orderStopPrice = this.extractOrderStopPrice(rawOrder);
        if (orderStopPrice === null || !this.arePricesClose(orderStopPrice, previousStopPrice)) {
          continue;
        }
      }

      return rawOrder;
    }

    return null;
  }

  private extractOrderStopPrice(order: AlpacaOrder): number | null {
    const rawValue = order.stop_price ?? order.stopPrice;
    const parsedValue = this.toNumber(rawValue);
    if (parsedValue === null) {
      return null;
    }
    try {
      return this.normalizeOrderPrice(parsedValue);
    } catch {
      return null;
    }
  }

  private extractStopLossOrderId(order: any): string | null {
    if (!order || typeof order !== 'object') {
      return null;
    }
    const legs = Array.isArray(order.legs) ? order.legs : null;
    if (!legs) {
      return null;
    }

    for (const leg of legs) {
      if (!leg || typeof leg !== 'object') {
        continue;
      }
      const typeRaw = typeof leg.type === 'string' ? leg.type.trim().toLowerCase() : null;
      const orderTypeRaw =
        typeof leg.order_type === 'string' ? leg.order_type.trim().toLowerCase() : null;
      const orderType = typeRaw ?? orderTypeRaw;
      const hasStopPrice = leg.stop_price !== undefined || leg.stopPrice !== undefined;
      if (orderType === 'stop' || orderType === 'stop_limit' || orderType === 'trailing_stop' || hasStopPrice) {
        const candidate =
          leg.id ?? leg.order_id ?? leg.client_order_id ?? leg.orderId ?? leg.clientOrderId;
        const normalized = this.normalizeOrderId(candidate);
        if (normalized) {
          return normalized;
        }
      }
    }

    return null;
  }

  private areQuantitiesClose(a: number, b: number): boolean {
    return Math.abs(a - b) <= 1e-6;
  }

  private arePricesClose(a: number, b: number): boolean {
    const tolerance = Math.abs(a) >= 1 || Math.abs(b) >= 1 ? 0.01 : 0.0001;
    return Math.abs(a - b) <= tolerance;
  }

  private getOperationOrderType(operation: AccountOperation): 'limit' | 'market' | null {
    if (typeof operation.orderType === 'string') {
      const normalized = operation.orderType.trim().toLowerCase();
      if (normalized === 'limit' || normalized === 'market') {
        return normalized;
      }
    }
    return null;
  }

  private normalizeOrderPrice(value: number): number {
    if (!Number.isFinite(value)) {
      throw new Error('invalid_price');
    }
    const decimals = Math.abs(value) >= 1 ? 2 : 4;
    return Number(value.toFixed(decimals));
  }

  private isValidOrderPrice(value?: number | null): value is number {
    return typeof value === 'number' && Number.isFinite(value) && value > 0;
  }

  private resolveMarketOrderLimitPrice(
    operation: AccountOperation,
    side: 'buy' | 'sell',
    capRatio: number
  ): number | null {
    if (!Number.isFinite(capRatio) || capRatio <= 0) {
      return null;
    }
    const referencePrice = this.isValidOrderPrice(operation.price) ? operation.price : null;
    if (referencePrice === null) {
      return null;
    }
    const multiplier = side === 'buy' ? 1 + capRatio : 1 - capRatio;
    if (!Number.isFinite(multiplier) || multiplier <= 0) {
      return null;
    }
    const limitPrice = referencePrice * multiplier;
    if (!Number.isFinite(limitPrice) || limitPrice <= 0) {
      return null;
    }
    return limitPrice;
  }

  private normalizeQuantity(value?: number | null): number | null {
    if (typeof value !== 'number') {
      return null;
    }
    const absolute = Math.abs(value);
    return Number.isFinite(absolute) && absolute > 0 ? absolute : null;
  }

  private normalizeOrderId(value: unknown): string | null {
    if (typeof value !== 'string') {
      return null;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }

  private extractStopLossPrice(value?: number | null): number | null {
    if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
      return null;
    }
    return this.normalizeOrderPrice(value);
  }

  private async cancelAllOpenOrders(
    baseUrl: string,
    headers: Record<string, string>,
    abortSignal?: AbortSignal
  ): Promise<number | null> {
    const response = await axios.delete(`${baseUrl}/orders`, {
      headers,
      timeout: this.orderRequestTimeout,
      signal: abortSignal
    });
    const data = response.data;
    if (Array.isArray(data)) {
      return data.length;
    }
    if (Array.isArray(data?.orders)) {
      return data.orders.length;
    }
    return null;
  }

  private async fetchPositionCounts(
    baseUrl: string,
    headers: Record<string, string>
  ): Promise<{ total: number | null; long: number | null; short: number | null; truncated: boolean }> {
    let longCount = 0;
    let shortCount = 0;
    let sawItems = false;
    const result = await this.collectPaginatedResource(
      baseUrl,
      headers,
      '/positions',
      {
        dataKeys: ['positions', 'data'],
        onItems: (items) => {
          sawItems = true;
          for (const item of items) {
            const side = typeof item?.side === 'string' ? item.side.trim().toLowerCase() : null;
            if (side === 'long') {
              longCount += 1;
            } else if (side === 'short') {
              shortCount += 1;
            }
          }
        }
      }
    );

    return {
      total: result.count,
      long: sawItems ? longCount : null,
      short: sawItems ? shortCount : null,
      truncated: result.truncated
    };
  }

  private async fetchOpenOrderCounts(
    baseUrl: string,
    headers: Record<string, string>
  ): Promise<{ total: number | null; buy: number | null; sell: number | null; truncated: boolean }> {
    let buyCount = 0;
    let sellCount = 0;
    let total = 0;
    let pages = 0;
    let truncated = false;
    let afterOrderId: string | null = null;
    let sawItems = false;

    while (true) {
      pages += 1;
      const params: Record<string, any> = {
        status: 'open',
        nested: false,
        direction: 'asc',
        limit: this.defaultPageSize
      };
      if (afterOrderId) {
        params.after_order_id = afterOrderId;
      }

      const response = await axios.get(`${baseUrl}/orders`, {
        headers,
        timeout: this.requestTimeout,
        params
      });

      const orders = Array.isArray(response.data)
        ? response.data
        : Array.isArray(response.data?.orders)
          ? response.data.orders
          : [];

      if (!orders.length) {
        break;
      }

      sawItems = true;
      for (const order of orders) {
        total += 1;
        const side = typeof order?.side === 'string' ? order.side.trim().toLowerCase() : null;
        if (side === 'buy') {
          buyCount += 1;
        } else if (side === 'sell') {
          sellCount += 1;
        }
      }

      const lastOrderId = this.extractOrderId(orders[orders.length - 1]);
      if (!lastOrderId) {
        truncated = orders.length === this.defaultPageSize;
        break;
      }
      afterOrderId = lastOrderId;

      if (orders.length < this.defaultPageSize) {
        break;
      }

      if (pages >= this.maxPaginationPages) {
        truncated = true;
        break;
      }
    }

    return {
      total: sawItems ? total : 0,
      buy: sawItems ? buyCount : 0,
      sell: sawItems ? sellCount : 0,
      truncated
    };
  }

  private extractOrderId(order: any): string | null {
    if (typeof order?.id === 'string' && order.id) {
      return order.id;
    }
    if (typeof order?.order_id === 'string' && order.order_id) {
      return order.order_id;
    }
    if (typeof order?.client_order_id === 'string' && order.client_order_id) {
      return order.client_order_id;
    }
    return null;
  }

  private async collectPaginatedResource(
    baseUrl: string,
    headers: Record<string, string>,
    path: string,
    options?: {
      params?: Record<string, any>;
      dataKeys?: string[];
      pageSize?: number;
      maxPages?: number;
      onItems?: (items: any[]) => void;
    }
  ): Promise<{ count: number | null; truncated: boolean }> {
    const baseParams = { ...(options?.params ?? {}) };
    const dataKeys = options?.dataKeys ?? ['data', 'items'];
    const maxPages = options?.maxPages ?? this.maxPaginationPages;
    const pageSize = options?.pageSize;

    let pageToken: string | null = null;
    let pages = 0;
    let count: number | null = 0;
    let sawPayload = false;
    let truncated = false;

    while (true) {
      pages += 1;
      const params: Record<string, any> = { ...baseParams };
      if (typeof pageSize === 'number' && Number.isFinite(pageSize) && pageSize > 0) {
        params.limit = pageSize;
        params.page_size = pageSize;
      }
      if (pageToken) {
        params.page_token = pageToken;
      }

      const response = await axios.get(`${baseUrl}${path}`, {
        headers,
        timeout: this.requestTimeout,
        params: Object.keys(params).length ? params : undefined
      });

      const pageItems = this.extractArrayFromResponse(response.data, dataKeys);
      if (!pageItems) {
        if (!sawPayload) {
          if (Array.isArray(response.data)) {
            count = response.data.length;
          } else {
            const fallback = this.toNumber(response.data?.count ?? response.data?.total);
            count = fallback;
          }
        }
        break;
      }

      sawPayload = true;
      options?.onItems?.(pageItems);
      count = (count ?? 0) + pageItems.length;

      const nextToken = this.extractNextPageToken(response.data, response.headers);
      if (!nextToken) {
        break;
      }
      pageToken = nextToken;

      if (pages >= maxPages) {
        truncated = true;
        break;
      }
    }

    if (!sawPayload && count === null) {
      return { count: null, truncated: false };
    }

    return { count, truncated };
  }

  private extractArrayFromResponse(data: any, dataKeys: string[]): any[] | null {
    if (Array.isArray(data)) {
      return data;
    }
    if (data && typeof data === 'object') {
      for (const key of dataKeys) {
        if (Array.isArray(data[key])) {
          return data[key];
        }
      }
    }
    return null;
  }

  private extractNextPageToken(body: any, headers: any): string | null {
    const candidateBodyKeys = ['next_page_token', 'nextPageToken', 'page_token', 'pageToken'];
    for (const key of candidateBodyKeys) {
      const value = body?.[key];
      if (typeof value === 'string' && value.trim().length > 0) {
        return value.trim();
      }
    }
    const candidateHeaderKeys = ['x-next-page-token', 'next-page-token', 'x-page-token'];
    for (const key of candidateHeaderKeys) {
      if (headers && key in headers) {
        const headerValue = headers[key];
        if (typeof headerValue === 'string' && headerValue.trim().length > 0) {
          return headerValue.trim();
        }
      }
    }
    return null;
  }

  private attachDispatchPayload(error: unknown, payload: Record<string, any>): void {
    if (!payload || typeof payload !== 'object') {
      return;
    }
    if (error && typeof error === 'object' && error !== null) {
      try {
        Object.defineProperty(error, 'dispatchPayload', {
          value: payload,
          configurable: true,
          enumerable: false,
          writable: true
        });
      } catch {
        (error as Record<string, any>).dispatchPayload = payload;
      }
    }
  }
}
