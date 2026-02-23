import axios from 'axios';
import { Database } from '../../database/Database';
import { LoggingService } from '../../services/LoggingService';
import { SETTING_KEYS } from '../../constants';
import { CandleSource, CandleSourceResult } from './CandleSource';
import { RequestParams, formatDate, requestWithRetry, sanitizeParams } from './candleSourceUtils';

type AlpacaBar = {
  t: string;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
  n?: number;
  vw?: number;
};

type AlpacaBarsResponse = {
  bars?: AlpacaBar[] | null;
  symbol?: string;
  next_page_token?: string | null;
};

const ALPACA_PAGE_LIMIT = 10000;

export class AlpacaCandleSource implements CandleSource {
  private db: Database;
  private loggingService: LoggingService;
  private omitEndUntil: number | null = null;

  constructor(database: Database, loggingService: LoggingService) {
    this.db = database;
    this.loggingService = loggingService;
  }

  getSourceName(): string {
    return 'ALPACA';
  }

  async getHistoricalCandles(
    symbol: string,
    startDate: Date,
    endDate: Date
  ): Promise<CandleSourceResult> {
    const baseUrl = await this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_DATA_BASE_URL);
    const trimmedBase = baseUrl.replace(/\/+$/, '');
    const baseWithStocks = trimmedBase.endsWith('/v2/stocks')
      ? trimmedBase
      : trimmedBase.endsWith('/v2')
        ? `${trimmedBase}/stocks`
        : `${trimmedBase}/v2/stocks`;
    const url = `${baseWithStocks}/${encodeURIComponent(symbol)}/bars`;

    const baseParams: RequestParams = {
      timeframe: '1Day',
      start: formatDate(startDate),
      adjustment: 'all',
      limit: ALPACA_PAGE_LIMIT
    };

    const bars: AlpacaBar[] = [];
    let nextPage: string | undefined;

    do {
      const params: RequestParams = {
        ...baseParams,
        ...(this.shouldOmitEndParam() ? {} : { end: formatDate(endDate) }),
        page_token: nextPage
      };

      const { data, noData } = await this.makeRequest<AlpacaBarsResponse>(url, params, symbol);
      if (noData) {
        return { candles: [], noData: true };
      }

      if (data.bars !== undefined && data.bars !== null && !Array.isArray(data.bars)) {
        this.loggingService.error('candle-job', 'Alpaca API returned unexpected payload', {
          url,
          params: sanitizeParams(params),
          response: data
        });
        return { candles: [], noData: false };
      }

      if (Array.isArray(data.bars)) {
        bars.push(...data.bars);
      }

      nextPage = data.next_page_token ?? undefined;
    } while (nextPage);

    bars.sort((a, b) => new Date(a.t).getTime() - new Date(b.t).getTime());

    const candles = bars.map(bar => ({
      ticker: symbol,
      date: new Date(bar.t),
      open: bar.o,
      high: bar.h,
      low: bar.l,
      close: bar.c,
      volumeShares: bar.v
    }));

    return {
      candles,
      noData: false
    };
  }

  private async makeRequest<T>(
    url: string,
    params: RequestParams = {},
    symbol?: string
  ): Promise<{ data: T; noData: boolean }> {
    const apiKey = await this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_API_KEY);
    const apiSecret = await this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_API_SECRET);
    const headers = {
      'Content-Type': 'application/json',
      'APCA-API-KEY-ID': apiKey,
      'APCA-API-SECRET-KEY': apiSecret
    };

    const requestWithParams = (requestParams: RequestParams) => requestWithRetry<T>(this.db, this.loggingService, {
      url,
      request: () => axios.get(url, {
        headers,
        params: requestParams,
        timeout: 30000
      }),
      logParams: requestParams,
      symbol,
      sourceLabel: 'Alpaca',
      waitSecondsSettingKey: SETTING_KEYS.ALPACA_DATA_RATE_LIMIT_WAIT_SECONDS
    });

    try {
      return await requestWithParams(params);
    } catch (error: any) {
      const status = error?.response?.status;
      if (status === 403 && params.end) {
        this.markOmitEndParam();
        const fallbackParams: RequestParams = { ...params };
        delete fallbackParams.end;
        return requestWithParams(fallbackParams);
      }
      throw error;
    }
  }

  private shouldOmitEndParam(): boolean {
    return this.omitEndUntil !== null && Date.now() < this.omitEndUntil;
  }

  private markOmitEndParam(): void {
    this.omitEndUntil = Date.now() + 60 * 60 * 1000;
  }
}
