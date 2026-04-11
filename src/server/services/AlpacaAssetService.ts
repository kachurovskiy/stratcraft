import axios from 'axios';
import { Database } from '../database/Database';
import { LoggingService, LogSource } from './LoggingService';
import { normalizeUppercaseString } from '../utils/stringNormalization';

const ASSET_SOURCE: LogSource = 'candle-job';

export interface AlpacaAssetSummary {
  symbol: string;
  name: string | null;
  tradable: boolean;
  shortable: boolean;
  easyToBorrow: boolean;
}

export interface AlpacaMarketClock {
  timestamp: Date;
  isOpen: boolean;
  nextOpen: Date;
  nextClose: Date;
}

interface AlpacaAssetResponse {
  symbol: string;
  status: string;
  class: string;
  name?: string | null;
  tradable: boolean;
  shortable: boolean;
  easy_to_borrow: boolean;
}

interface AlpacaClockResponse {
  timestamp: string;
  is_open: boolean;
  next_open: string;
  next_close: string;
}

export class AlpacaAssetService {
  constructor(
    private readonly loggingService: LoggingService,
    private readonly db: Database
  ) {}

  async fetchMarketClock(abortSignal?: AbortSignal): Promise<AlpacaMarketClock> {
    try {
      const alpacaSettings = this.db.settings.value.alpaca;
      const baseUrl = alpacaSettings.paperUrl;
      if (!baseUrl) throw new Error('ALPACA_PAPER_URL is missing or empty.');
      const apiKey = alpacaSettings.apiKey;
      if (!apiKey) throw new Error('ALPACA_API_KEY is missing or empty.');
      const apiSecret = alpacaSettings.apiSecret;
      if (!apiSecret) throw new Error('ALPACA_API_SECRET is missing or empty.');

      const url = `${baseUrl.replace(/\/+$/, '')}/clock`;
      const response = await axios.get<AlpacaClockResponse>(url, {
        headers: {
          'APCA-API-KEY-ID': apiKey,
          'APCA-API-SECRET-KEY': apiSecret
        },
        timeout: 20000,
        signal: abortSignal
      });

      const parseTimestamp = (value: unknown, field: string): Date => {
        if (typeof value !== 'string') {
          throw new Error(`Alpaca clock response missing ${field}.`);
        }
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) {
          throw new Error(`Alpaca clock response had invalid ${field}.`);
        }
        return parsed;
      };

      const data = response.data;
      const isOpenRaw = (data as any)?.is_open ?? (data as any)?.isOpen;
      const nextOpenRaw = (data as any)?.next_open ?? (data as any)?.nextOpen;
      const nextCloseRaw = (data as any)?.next_close ?? (data as any)?.nextClose;

      return {
        timestamp: parseTimestamp((data as any)?.timestamp, 'timestamp'),
        isOpen: typeof isOpenRaw === 'boolean' ? isOpenRaw : Boolean(isOpenRaw),
        nextOpen: parseTimestamp(nextOpenRaw, 'next_open'),
        nextClose: parseTimestamp(nextCloseRaw, 'next_close')
      };
    } catch (error: any) {
      const message = error?.message ?? 'Unknown error';
      this.loggingService.warn(ASSET_SOURCE, 'Failed to fetch Alpaca market clock', {
        error: message
      });
      throw error;
    }
  }

  async fetchActiveEquityAssets(): Promise<AlpacaAssetSummary[]> {
    try {
      const alpacaSettings = this.db.settings.value.alpaca;
      const baseUrl = alpacaSettings.paperUrl;
      if (!baseUrl) throw new Error('ALPACA_PAPER_URL is missing or empty.');
      const apiKey = alpacaSettings.apiKey;
      if (!apiKey) throw new Error('ALPACA_API_KEY is missing or empty.');
      const apiSecret = alpacaSettings.apiSecret;
      if (!apiSecret) throw new Error('ALPACA_API_SECRET is missing or empty.');
      const ignoredTickers = this.db.settings.value.tickerRules.ignoredTickers;
      const url = `${baseUrl.replace(/\/+$/, '')}/assets`;
      const ignoredSet = new Set(ignoredTickers);
      const response = await axios.get<AlpacaAssetResponse[]>(url, {
        headers: {
          'APCA-API-KEY-ID': apiKey,
          'APCA-API-SECRET-KEY': apiSecret
        },
        params: {
          status: 'active',
          asset_class: 'us_equity'
        },
        timeout: 20000
      });

      const deduped = new Map<string, AlpacaAssetSummary>();
      const symbolPattern = /^[A-Z]+$/;
      for (const asset of response.data) {
        const symbol = normalizeUppercaseString(asset.symbol);
        if (!symbol || !symbolPattern.test(symbol)) continue;
        if (ignoredSet.has(symbol)) continue;
        if (asset.class && asset.class.toLowerCase() !== 'us_equity') continue;
        deduped.set(symbol, {
          symbol,
          name: asset.name?.trim() || null,
          tradable: Boolean(asset.tradable),
          shortable: Boolean(asset.shortable),
          easyToBorrow: Boolean(asset.easy_to_borrow)
        });
      }

      return Array.from(deduped.values());
    } catch (error: any) {
      const message = error?.message ?? 'Unknown error';
      this.loggingService.error(ASSET_SOURCE, 'Failed to fetch Alpaca assets', {
        error: message
      });
      throw error;
    }
  }
}
