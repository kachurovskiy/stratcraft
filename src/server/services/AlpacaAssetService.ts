import axios from 'axios';
import { Database } from '../database/Database';
import { LoggingService, LogSource } from './LoggingService';
import { SETTING_KEYS } from '../constants';

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
      const [baseUrl, apiKey, apiSecret] = await Promise.all([
        this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_PAPER_URL),
        this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_API_KEY),
        this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_API_SECRET)
      ]);

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
      const [
        baseUrl,
        apiKey,
        apiSecret,
        ignoredTickers
      ] = await Promise.all([
        this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_PAPER_URL),
        this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_API_KEY),
        this.db.settings.getRequiredSettingValue(SETTING_KEYS.ALPACA_API_SECRET),
        this.db.settings.getSettingArray(SETTING_KEYS.IGNORED_TICKERS)
      ]);
      const url = `${baseUrl}/assets`;
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
        const symbol = asset.symbol?.trim().toUpperCase();
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
