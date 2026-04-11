import axios from 'axios';
import { Database } from '../../database/Database';
import { LoggingService } from '../../services/LoggingService';
import { CandleSource, CandleSourceResult } from './CandleSource';
import { RequestParams, formatDate, requestWithRetry } from './candleSourceUtils';

export interface EodhdResponse {
  date: string;
  close: number;
  high: number;
  low: number;
  open: number;
  volume: number;
  adjusted_close?: number;
}

export class EodhdCandleSource implements CandleSource {
  private db: Database;
  private loggingService: LoggingService;

  constructor(database: Database, loggingService: LoggingService) {
    this.db = database;
    this.loggingService = loggingService;
  }

  getSourceName(): string {
    return 'EODHD';
  }

  async getHistoricalCandles(
    symbol: string,
    startDate: Date,
    endDate: Date,
    abortSignal?: AbortSignal
  ): Promise<CandleSourceResult> {
    const baseUrl = this.db.settings.value.eodhd.baseUrl;
    if (!baseUrl) throw new Error('EODHD_BASE_URL is missing or empty.');
    const url = `${baseUrl}/${encodeURIComponent(symbol)}`;
    const params: RequestParams = {
      period: 'd',
      from: formatDate(startDate),
      to: formatDate(endDate)
    };

    const { data, noData } = await this.makeRequest<EodhdResponse[]>(url, params, symbol, abortSignal);

    const candles = data.map(item => {
      const close = item.adjusted_close ?? item.close;
      let { open, high, low } = item;
      let volumeShares = item.volume;

      if (item.adjusted_close && item.close && item.close !== 0) {
        const adjustment = item.adjusted_close / item.close;
        if (Number.isFinite(adjustment) && adjustment > 0) {
          // Keep candles split/dividend adjusted whenever EODHD provides the adjusted close
          open *= adjustment;
          high *= adjustment;
          low *= adjustment;
          volumeShares = Math.round(item.volume / adjustment);
        }
      }

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
      noData
    };
  }

  private async makeRequest<T>(
    url: string,
    params: RequestParams = {},
    symbol?: string,
    abortSignal?: AbortSignal
  ): Promise<{ data: T; noData: boolean }> {
    const apiToken = this.db.settings.value.eodhd.apiToken;
    if (!apiToken) throw new Error('EODHD_API_TOKEN is missing or empty.');
    const requestParams: RequestParams = {
      api_token: apiToken,
      fmt: 'json',
      ...params
    };
    const waitSeconds = this.db.settings.value.eodhd.rateLimitWaitSeconds;

    return requestWithRetry<T>(this.loggingService, {
      url,
      request: () => axios.get(url, {
        headers: {
          'Content-Type': 'application/json'
        },
        params: requestParams,
        timeout: 30000,
        signal: abortSignal
      }),
      logParams: requestParams,
      symbol,
      sourceLabel: 'EODHD',
      waitSeconds,
      redactKeys: ['api_token'],
      abortSignal
    });
  }
}
