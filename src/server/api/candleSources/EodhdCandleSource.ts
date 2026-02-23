import axios from 'axios';
import { Database } from '../../database/Database';
import { LoggingService } from '../../services/LoggingService';
import { SETTING_KEYS } from '../../constants';
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
    endDate: Date
  ): Promise<CandleSourceResult> {
    const baseUrl = await this.db.settings.getRequiredSettingValue(SETTING_KEYS.EODHD_BASE_URL);
    const url = `${baseUrl}/${encodeURIComponent(symbol)}`;
    const params: RequestParams = {
      period: 'd',
      from: formatDate(startDate),
      to: formatDate(endDate)
    };

    const { data, noData } = await this.makeRequest<EodhdResponse[]>(url, params, symbol);

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
    symbol?: string
  ): Promise<{ data: T; noData: boolean }> {
    const apiToken = await this.db.settings.getRequiredSettingValue(SETTING_KEYS.EODHD_API_TOKEN);
    const requestParams: RequestParams = {
      api_token: apiToken,
      fmt: 'json',
      ...params
    };

    return requestWithRetry<T>(this.db, this.loggingService, {
      url,
      request: () => axios.get(url, {
        headers: {
          'Content-Type': 'application/json'
        },
        params: requestParams,
        timeout: 30000
      }),
      logParams: requestParams,
      symbol,
      sourceLabel: 'EODHD',
      waitSecondsSettingKey: SETTING_KEYS.EODHD_RATE_LIMIT_WAIT_SECONDS,
      redactKeys: ['api_token']
    });
  }
}
