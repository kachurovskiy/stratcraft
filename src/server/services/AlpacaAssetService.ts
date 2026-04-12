import axios from 'axios';
import { formatDate } from '../api/candleSources/candleSourceUtils';
import { Database } from '../database/Database';
import type { CorporateActionRecord, CorporateActionType } from '../database/types';
import { LoggingService, LogSource } from './LoggingService';
import { normalizeUppercaseString } from '../utils/stringNormalization';

const ASSET_SOURCE: LogSource = 'candle-job';
const CORPORATE_ACTION_SOURCE: LogSource = 'corporate-actions-job';
const CORPORATE_ACTION_PAGE_LIMIT = 1000;
const CORPORATE_ACTION_SYMBOL_BATCH_SIZE = 200;

const CORPORATE_ACTION_RESPONSE_KEYS = {
  reverse_split: 'reverse_splits',
  forward_split: 'forward_splits',
  unit_split: 'unit_splits',
  cash_dividend: 'cash_dividends',
  stock_dividend: 'stock_dividends',
  spin_off: 'spin_offs',
  cash_merger: 'cash_mergers',
  stock_merger: 'stock_mergers',
  stock_and_cash_merger: 'stock_and_cash_mergers',
  redemption: 'redemptions',
  name_change: 'name_changes',
  worthless_removal: 'worthless_removals',
  rights_distribution: 'rights_distributions'
} as const satisfies Record<CorporateActionType, string>;

const CORPORATE_ACTION_SYMBOL_FIELDS = [
  'symbol',
  'old_symbol',
  'new_symbol',
  'source_symbol',
  'acquirer_symbol',
  'acquiree_symbol',
  'alternate_symbol'
] as const;

const CORPORATE_ACTION_PRIMARY_SYMBOL_FIELDS = [
  'symbol',
  'old_symbol',
  'source_symbol',
  'acquiree_symbol',
  'new_symbol',
  'acquirer_symbol',
  'alternate_symbol'
] as const;

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

type AlpacaCorporateActionPayload = Record<string, unknown> & {
  id?: string;
  process_date?: string;
  effective_date?: string;
  ex_date?: string;
  record_date?: string;
  payable_date?: string;
};

type CorporateActionResponseKey = (typeof CORPORATE_ACTION_RESPONSE_KEYS)[CorporateActionType];

interface AlpacaCorporateActionsResponse {
  corporate_actions?: Partial<Record<CorporateActionResponseKey, AlpacaCorporateActionPayload[]>> | null;
  next_page_token?: string | null;
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

  async fetchCorporateActions(options: {
    symbols: string[];
    startDate: Date;
    endDate?: Date;
    abortSignal?: AbortSignal;
  }): Promise<CorporateActionRecord[]> {
    const normalizedSymbols = Array.from(
      new Set(options.symbols.map((symbol) => normalizeUppercaseString(symbol)).filter((symbol) => symbol.length > 0))
    );
    if (normalizedSymbols.length === 0) {
      return [];
    }

    const alpacaSettings = this.db.settings.value.alpaca;
    const baseUrl = alpacaSettings.dataBaseUrl;
    if (!baseUrl) throw new Error('ALPACA_DATA_BASE_URL is missing or empty.');
    const apiKey = alpacaSettings.apiKey;
    if (!apiKey) throw new Error('ALPACA_API_KEY is missing or empty.');
    const apiSecret = alpacaSettings.apiSecret;
    if (!apiSecret) throw new Error('ALPACA_API_SECRET is missing or empty.');

    const url = new URL('/v1/corporate-actions', baseUrl).toString();
    const actionsById = new Map<string, CorporateActionRecord>();

    for (let i = 0; i < normalizedSymbols.length; i += CORPORATE_ACTION_SYMBOL_BATCH_SIZE) {
      const symbolBatch = normalizedSymbols.slice(i, i + CORPORATE_ACTION_SYMBOL_BATCH_SIZE);
      let pageToken: string | undefined;

      do {
        const params: Record<string, string | number | undefined> = {
          symbols: symbolBatch.join(','),
          start: formatDate(options.startDate),
          end: options.endDate ? formatDate(options.endDate) : undefined,
          limit: CORPORATE_ACTION_PAGE_LIMIT,
          sort: 'asc',
          page_token: pageToken
        };
        const data = await this.fetchDataRequest<AlpacaCorporateActionsResponse>(url, params, options.abortSignal);
        const corporateActions = data.corporate_actions;

        if (corporateActions && typeof corporateActions === 'object') {
          for (const [actionType, responseKey] of Object.entries(CORPORATE_ACTION_RESPONSE_KEYS) as Array<
            [CorporateActionType, CorporateActionResponseKey]
          >) {
            const items = corporateActions[responseKey];
            if (!Array.isArray(items)) {
              continue;
            }
            for (const item of items) {
              const action = this.mapCorporateAction(actionType, item);
              if (action) {
                actionsById.set(action.id, action);
              }
            }
          }
        }

        pageToken =
          typeof data.next_page_token === 'string' && data.next_page_token.trim().length > 0
            ? data.next_page_token.trim()
            : undefined;
      } while (pageToken);
    }

    return Array.from(actionsById.values()).sort((left, right) => {
      const dateDiff = left.processDate.getTime() - right.processDate.getTime();
      if (dateDiff !== 0) {
        return dateDiff;
      }
      return left.id.localeCompare(right.id);
    });
  }

  private async fetchDataRequest<T>(
    url: string,
    params: Record<string, string | number | undefined>,
    abortSignal?: AbortSignal
  ): Promise<T> {
    const apiKey = this.db.settings.value.alpaca.apiKey;
    if (!apiKey) throw new Error('ALPACA_API_KEY is missing or empty.');
    const apiSecret = this.db.settings.value.alpaca.apiSecret;
    if (!apiSecret) throw new Error('ALPACA_API_SECRET is missing or empty.');
    const waitSeconds = this.db.settings.value.alpaca.dataRateLimitWaitSeconds;

    try {
      const response = await axios.get<T>(url, {
        headers: {
          'APCA-API-KEY-ID': apiKey,
          'APCA-API-SECRET-KEY': apiSecret,
          accept: 'application/json'
        },
        params,
        timeout: 30000,
        signal: abortSignal
      });
      return response.data;
    } catch (error: any) {
      if (abortSignal?.aborted || error?.code === 'ERR_CANCELED' || error?.name === 'CanceledError') {
        throw new Error('Request cancelled');
      }

      const status = error?.response?.status;
      if (status === 429) {
        this.loggingService.warn(CORPORATE_ACTION_SOURCE, 'Alpaca corporate actions rate limit exceeded, retrying', {
          url,
          params,
          waitSeconds
        });
        await new Promise((resolve) => setTimeout(resolve, waitSeconds * 1000));
        return this.fetchDataRequest<T>(url, params, abortSignal);
      }

      const message = error?.message ?? 'Unknown error';
      this.loggingService.error(CORPORATE_ACTION_SOURCE, 'Failed to fetch Alpaca corporate actions', {
        url,
        params,
        status,
        error: message
      });
      throw error;
    }
  }

  private mapCorporateAction(
    actionType: CorporateActionType,
    payload: AlpacaCorporateActionPayload
  ): CorporateActionRecord | null {
    const id = typeof payload.id === 'string' ? payload.id.trim() : '';
    if (!id) {
      return null;
    }

    const processDate = this.parseDateValue(payload.process_date);
    if (!processDate) {
      return null;
    }

    const primarySymbol = this.resolveCorporateActionPrimarySymbol(payload);
    if (!primarySymbol) {
      return null;
    }

    const relatedSymbols = this.resolveCorporateActionSymbols(payload).filter((symbol) => symbol !== primarySymbol);

    return {
      id,
      actionType,
      primarySymbol,
      relatedSymbols,
      processDate,
      effectiveDate: this.parseDateValue(payload.effective_date),
      exDate: this.parseDateValue(payload.ex_date),
      recordDate: this.parseDateValue(payload.record_date),
      payableDate: this.parseDateValue(payload.payable_date),
      payload: { ...payload }
    };
  }

  private resolveCorporateActionPrimarySymbol(payload: AlpacaCorporateActionPayload): string {
    for (const field of CORPORATE_ACTION_PRIMARY_SYMBOL_FIELDS) {
      const symbol = normalizeUppercaseString(payload[field]);
      if (symbol.length > 0) {
        return symbol;
      }
    }
    return '';
  }

  private resolveCorporateActionSymbols(payload: AlpacaCorporateActionPayload): string[] {
    const symbols: string[] = [];
    for (const field of CORPORATE_ACTION_SYMBOL_FIELDS) {
      const symbol = normalizeUppercaseString(payload[field]);
      if (symbol.length > 0) {
        symbols.push(symbol);
      }
    }
    return Array.from(new Set(symbols));
  }

  private parseDateValue(value: unknown): Date | null {
    if (typeof value !== 'string' || value.trim().length === 0) {
      return null;
    }
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }
}
