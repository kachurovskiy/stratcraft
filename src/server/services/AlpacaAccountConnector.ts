import axios from 'axios';
import {
  AccountEnvironment,
  AccountPortfolioHistory,
  AccountPortfolioHistoryPoint,
  AccountPortfolioHistoryRequest,
  AccountPosition,
  AccountSnapshot,
  TradingAccount
} from '../../shared/types/Account';
import { AccountOperation } from '../../shared/types/StrategyTemplate';
import { SETTING_KEYS } from '../constants';
import { Database } from '../database/Database';
import { LoggingService } from './LoggingService';
import type { AccountConnector, DispatchResult } from './AccountDataService';

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

  async fetchPortfolioHistory(
    account: TradingAccount,
    options?: AccountPortfolioHistoryRequest
  ): Promise<AccountPortfolioHistory> {
    const baseUrl = await this.getBaseUrl(account.environment);
    const headers = this.buildHeaders(account);
    const params = this.buildPortfolioHistoryParams(options);
    const response = await axios.get(`${baseUrl}/account/portfolio/history`, {
      headers,
      timeout: this.requestTimeout,
      params
    });
    return this.normalizePortfolioHistoryResponse(response.data);
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

  private buildPortfolioHistoryParams(
    options?: AccountPortfolioHistoryRequest
  ): Record<string, string> {
    const params: Record<string, string> = {};
    const normalizedStart = this.normalizeIsoTimestamp(options?.start);
    const normalizedEnd = this.normalizeIsoTimestamp(options?.end);
    if (normalizedStart) {
      params.start = normalizedStart;
    }
    if (normalizedEnd) {
      params.end = normalizedEnd;
    }
    const normalizedTimeframe = this.normalizeHistoryTimeframe(options?.timeframe);
    if (normalizedTimeframe) {
      params.timeframe = normalizedTimeframe;
    }
    const intraday = this.normalizeIntradayReporting(options?.intradayReporting, normalizedTimeframe);
    if (intraday) {
      params.intraday_reporting = intraday;
    }
    const pnlReset = this.normalizePnlReset(options?.pnlReset, normalizedTimeframe);
    if (pnlReset) {
      params.pnl_reset = pnlReset;
    }
    const normalizedPeriod = this.normalizeHistoryPeriod(options?.period);
    const hasStart = Boolean(normalizedStart);
    const hasEnd = Boolean(normalizedEnd);
    if (!normalizedStart && !normalizedEnd) {
      params.period = normalizedPeriod ?? '1M';
    } else if (
      normalizedPeriod &&
      ((hasStart && !hasEnd) || (!hasStart && hasEnd))
    ) {
      params.period = normalizedPeriod;
    }
    return params;
  }

  private normalizePortfolioHistoryResponse(data: any): AccountPortfolioHistory {
    const timestamps = Array.isArray(data?.timestamp) ? data.timestamp : [];
    const equityValues = Array.isArray(data?.equity) ? data.equity : [];
    const profitLossValues = Array.isArray(data?.profit_loss) ? data.profit_loss : [];
    const profitLossPctValues = Array.isArray(data?.profit_loss_pct) ? data.profit_loss_pct : [];
    const count = Math.max(timestamps.length, equityValues.length, profitLossValues.length, profitLossPctValues.length);
    const points: AccountPortfolioHistoryPoint[] = [];

    for (let i = 0; i < count; i++) {
      const tsSeconds = this.toNumber(timestamps[i]);
      if (tsSeconds === null) {
        continue;
      }
      points.push({
        timestamp: Math.round(tsSeconds * 1000),
        equity: this.toNumber(equityValues[i]),
        profitLoss: this.toNumber(profitLossValues[i]),
        profitLossPct: this.toNumber(profitLossPctValues[i])
      });
    }

    points.sort((a, b) => a.timestamp - b.timestamp);

    return {
      currency: this.toCurrency(data?.currency ?? data?.base_currency),
      timeframe: typeof data?.timeframe === 'string' ? data.timeframe : null,
      baseValue: this.toNumber(data?.base_value),
      baseValueAsOf: typeof data?.base_value_asof === 'string' ? data.base_value_asof : null,
      startAt: points.length > 0 ? new Date(points[0].timestamp).toISOString() : null,
      endAt: points.length > 0 ? new Date(points[points.length - 1].timestamp).toISOString() : null,
      points
    };
  }

  private normalizeIsoTimestamp(value?: string | null): string | null {
    if (typeof value !== 'string' || value.trim().length === 0) {
      return null;
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return null;
    }
    return date.toISOString();
  }

  private normalizeHistoryPeriod(value?: string | null): string | null {
    if (typeof value !== 'string') {
      return null;
    }
    const normalized = value.trim().toUpperCase();
    const match = normalized.match(/^(\d{1,3})([DWMAY])$/);
    if (!match) {
      return null;
    }
    const quantity = match[1];
    const unit = match[2] === 'Y' ? 'A' : match[2];
    return `${quantity}${unit}`;
  }

  private normalizeHistoryTimeframe(value?: string | null): string | null {
    const allowed: Record<string, string> = {
      '1MIN': '1Min',
      '5MIN': '5Min',
      '15MIN': '15Min',
      '1H': '1H',
      '1D': '1D'
    };
    if (typeof value === 'string') {
      const normalized = value.trim().toUpperCase();
      if (allowed[normalized]) {
        return allowed[normalized];
      }
    }
    return null;
  }

  private normalizeIntradayReporting(
    value: string | undefined,
    timeframe: string | null
  ): string | null {
    if (!timeframe || timeframe === '1D') {
      return null;
    }
    const allowed = new Set(['market_hours', 'extended_hours', 'continuous']);
    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (allowed.has(normalized)) {
        return normalized;
      }
    }
    return 'extended_hours';
  }

  private normalizePnlReset(
    value: string | undefined,
    timeframe: string | null
  ): string | null {
    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (normalized === 'per_day' || normalized === 'no_reset') {
        return normalized;
      }
    }
    if (timeframe && timeframe !== '1D') {
      return 'no_reset';
    }
    return null;
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

    const payload = this.buildAlpacaOrderPayload(operation, ticker);

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

  private async getBaseUrl(environment: AccountEnvironment): Promise<string> {
    const normalized = typeof environment === 'string' ? environment.trim().toLowerCase() : '';
    const isLive = normalized === 'live';
    const settingKey = isLive ? SETTING_KEYS.ALPACA_LIVE_URL : SETTING_KEYS.ALPACA_PAPER_URL;
    const configured = await this.db.settings.getRequiredSettingValue(settingKey);
    return configured.trim();
  }

  private buildHeaders(account: TradingAccount) {
    return {
      'APCA-API-KEY-ID': account.apiKey,
      'APCA-API-SECRET-KEY': account.apiSecret
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

  private buildAlpacaOrderPayload(operation: AccountOperation, ticker: string) {
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
        if (metadataOrderType === 'limit' && operation.price) {
          payload.type = 'limit';
          payload.limit_price = this.normalizeOrderPrice(operation.price);
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
        if (metadataOrderType === 'limit' && operation.price) {
          payload.type = 'limit';
          payload.limit_price = this.normalizeOrderPrice(operation.price);
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
