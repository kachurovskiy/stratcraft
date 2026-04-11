import axios from 'axios';
import { Database } from '../../database/Database';
import { LoggingService } from '../../services/LoggingService';
import { CandleSource, CandleSourceResult } from './CandleSource';
import { RequestParams, formatDate, requestWithRetry, sanitizeParams } from './candleSourceUtils';

export interface TiingoPrice {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  adjOpen?: number;
  adjHigh?: number;
  adjLow?: number;
  adjClose?: number;
  adjVolume?: number;
  splitFactor?: number;
  divCash?: number;
}

export class TiingoCandleSource implements CandleSource {
  private db: Database;
  private loggingService: LoggingService;

  constructor(database: Database, loggingService: LoggingService) {
    this.db = database;
    this.loggingService = loggingService;
  }

  getSourceName(): string {
    return 'TIINGO';
  }

  async getHistoricalCandles(
    symbol: string,
    startDate: Date,
    endDate: Date,
    abortSignal?: AbortSignal
  ): Promise<CandleSourceResult> {
    const tiingoSettings = this.db.settings.value.tiingo;
    const baseUrl = tiingoSettings.baseUrl;
    if (!baseUrl) throw new Error('TIINGO_BASE_URL is missing or empty.');
    const trimmedBase = baseUrl.replace(/\/+$/, '');
    const url = `${trimmedBase}/${encodeURIComponent(symbol)}/prices`;
    const params: RequestParams = {
      startDate: formatDate(startDate),
      endDate: formatDate(endDate)
    };

    const { data, noData } = await this.makeRequest<TiingoPrice[]>(url, params, symbol, abortSignal);
    if (noData) {
      return { candles: [], noData: true };
    }

    if (!Array.isArray(data)) {
      const waitSeconds = tiingoSettings.rateLimitWaitSeconds;
      this.loggingService.warn('candle-job', `Tiingo returned non-array payload; pausing requests for ${waitSeconds} seconds`, {
        url,
        params: sanitizeParams(params, ['token']),
        response: JSON.stringify(data).substring(0, 200),
      });
      await new Promise(r => setTimeout(r, waitSeconds * 1000));
      return { candles: [], noData: false };
    }

    const sorted = [...data].sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
    const candles = sorted.map(item => {
      const useAdjusted =
        Number.isFinite(item.adjOpen) &&
        Number.isFinite(item.adjHigh) &&
        Number.isFinite(item.adjLow) &&
        Number.isFinite(item.adjClose);

      const open = useAdjusted ? item.adjOpen! : item.open;
      const high = useAdjusted ? item.adjHigh! : item.high;
      const low = useAdjusted ? item.adjLow! : item.low;
      const close = useAdjusted ? item.adjClose! : item.close;
      const volumeShares = useAdjusted && Number.isFinite(item.adjVolume) ? item.adjVolume! : item.volume;

      return {
        ticker: symbol,
        date: new Date(item.date),
        open,
        high,
        low,
        close,
        unadjustedClose: item.close,
        volumeShares
      };
    });

    return {
      candles,
      noData: false
    };
  }

  private async makeRequest<T>(
    url: string,
    params: RequestParams = {},
    symbol?: string,
    abortSignal?: AbortSignal
  ): Promise<{ data: T; noData: boolean }> {
    const apiToken = this.db.settings.value.tiingo.apiToken;
    if (!apiToken) throw new Error('TIINGO_API_TOKEN is missing or empty.');
    const requestParams: RequestParams = {
      token: apiToken,
      ...params
    };
    const waitSeconds = this.db.settings.value.tiingo.rateLimitWaitSeconds;

    return requestWithRetry<T>(this.loggingService, {
      url,
      request: () => axios.get(url, {
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Token ${apiToken}`
        },
        params: requestParams,
        timeout: 30000,
        signal: abortSignal
      }),
      logParams: requestParams,
      symbol,
      sourceLabel: 'Tiingo',
      waitSeconds,
      redactKeys: ['token'],
      abortSignal
    });
  }
}
