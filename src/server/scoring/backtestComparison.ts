import type { Database } from '../database/Database';
import type { AccountSignalSkipRow, BacktestResultRecord } from '../database/types';
import type { BacktestScope, Candle, Trade } from '../../shared/types/StrategyTemplate';
import { SETTING_KEYS } from '../constants';
import { formatSignalSkipReason } from '../utils/skipReasonFormatting';
import { resolveEntryOrderCancellationReason } from '../utils/tradeOrderStatus';

type BacktestComparisonSummary = {
  label: string;
  scopeLabel: string;
  scopeBadge: string;
  periodLabel: string;
  startDate: Date;
  endDate: Date;
  createdAt: Date;
};

type BacktestComparisonSlippage = {
  hasData: boolean;
  setting: number | null;
  impliedAvg: number | null;
  impliedAvgAbs: number | null;
  gap: number | null;
  gapClass: string;
  matchedEntries: number;
};

type BacktestComparisonPenetration = {
  hasData: boolean;
  setting: number | null;
  impliedAvg: number | null;
  gap: number | null;
  gapClass: string;
  matchedEntries: number;
};

type BacktestComparisonExpenseRatio = {
  hasData: boolean;
  engineAvg: number | null;
  liveAvg: number | null;
  gap: number | null;
  gapClass: string;
  engineNotional: number;
  liveNotional: number;
};

type TradeTickerLink = {
  id: string;
  ticker: string;
  tradeUrl: string;
  badgeClass: string;
  label: string;
  detail: string | null;
};

type TradeEntrySampleCell = {
  trades: TradeTickerLink[];
  reasons: TradeDifferenceReason[];
};

type EntryPriceGapSummary = {
  label: string;
  className: string;
};

type TradeEntrySampleRow = {
  ticker: string;
  engine: TradeEntrySampleCell;
  live: TradeEntrySampleCell;
  quantityNote: string | null;
  entryPriceGap: EntryPriceGapSummary | null;
};

type TradeEntrySampleDay = {
  date: Date;
  engineCount: number;
  liveCount: number;
  rows: TradeEntrySampleRow[];
};

export type BacktestComparisonView = {
  isEligible: boolean;
  hasEngine: boolean;
  hasLive: boolean;
  notice?: string;
  engine?: BacktestComparisonSummary;
  live?: BacktestComparisonSummary;
  slippage?: BacktestComparisonSlippage;
  penetration?: BacktestComparisonPenetration;
  expenseRatio?: BacktestComparisonExpenseRatio;
  sampleDays: TradeEntrySampleDay[];
};

const SLIPPAGE_DEFAULT = 0.003;
const PENETRATION_DEFAULT = 0.005;
const SAMPLE_DAY_LIMIT = 5;
const ENTRY_STATUS = new Set<Trade['status']>(['active', 'closed']);
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const SIGNAL_SKIP_LOOKBACK_DAYS = 3;

export const BACKTEST_SCOPE_META: Record<BacktestScope, { label: string; badge: string }> = {
  validation: { label: 'Validation tickers', badge: 'bg-warning text-dark' },
  training: { label: 'Training tickers', badge: 'bg-secondary' },
  all: { label: 'All tickers', badge: 'bg-info text-dark' },
  live: { label: 'Live backtest', badge: 'bg-success' }
};

export const normalizeBacktestScope = (value: unknown): BacktestScope => {
  if (value === 'validation' || value === 'training' || value === 'all' || value === 'live') {
    return value;
  }
  return 'training';
};

type EntryAggregate = {
  notional: number;
  quantity: number;
};

type EntryAggregation = {
  entriesByKey: Map<string, EntryAggregate>;
};

const toDateKey = (value: Date): string => value.toISOString().slice(0, 10);

const isEntryTrade = (trade: Trade): boolean => ENTRY_STATUS.has(trade.status);
const isCancelledTrade = (trade: Trade): boolean => trade.status === 'cancelled';

const formatTradePrice = (value: number): string | null => {
  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }
  const formatted = value.toFixed(4);
  return formatted.replace(/\.?0+$/, '');
};

const formatPercentValue = (value: number): string | null => {
  if (!Number.isFinite(value)) {
    return null;
  }
  const formatted = value.toFixed(2);
  return formatted.replace(/\.?0+$/, '');
};

const formatSignedPercentOneDecimal = (value: number): string | null => {
  if (!Number.isFinite(value)) {
    return null;
  }
  const formatted = value.toFixed(1);
  const sign = value > 0 ? '+' : '';
  return `${sign}${formatted}%`;
};

const parseNumericValue = (value: unknown): number | null => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const selectSizingPrice = (candle: Candle | null | undefined): number | null => {
  if (!candle) {
    return null;
  }
  if (Number.isFinite(candle.close) && candle.close > 0) {
    return candle.close;
  }
  if (Number.isFinite(candle.unadjustedClose ?? NaN) && (candle.unadjustedClose ?? 0) > 0) {
    return candle.unadjustedClose ?? null;
  }
  return null;
};

const formatLowLimitDetail = (low: number | null, limit: number | null): string | null => {
  const lowValue = typeof low === 'number' ? low : null;
  const limitValue = typeof limit === 'number' ? limit : null;
  const lowLabel = lowValue !== null ? formatTradePrice(lowValue) : null;
  const limitLabel = limitValue !== null ? formatTradePrice(limitValue) : null;

  if (lowLabel && limitLabel && lowValue !== null && limitValue !== null) {
    const diff = limitValue - lowValue;
    if (Math.abs(diff) < 1e-8) {
      return `Low ${lowLabel} (at limit ${limitLabel})`;
    }
    const diffLabel = formatTradePrice(Math.abs(diff));
    const percentValue = limitValue > 0 ? (Math.abs(diff) / limitValue) * 100 : null;
    const percentLabel = percentValue !== null ? formatPercentValue(percentValue) : null;
    if (diffLabel) {
      const relation = diff > 0 ? 'below' : 'above';
      const percentSuffix = percentLabel ? `, ${percentLabel}%` : '';
      return `Low ${lowLabel} (${diffLabel} ${relation} limit ${limitLabel}${percentSuffix})`;
    }
    return `Low ${lowLabel} | Limit ${limitLabel}`;
  }

  if (lowLabel) {
    return `Low ${lowLabel}`;
  }

  if (limitLabel) {
    return `Limit ${limitLabel}`;
  }

  return null;
};

const formatCancelledTradeDetail = (trade: Trade, candleLow: number | null): string | null => {
  const detailParts: string[] = [];
  const limit = Number.isFinite(trade.price) ? trade.price : null;
  const lowLimitDetail = formatLowLimitDetail(candleLow, limit);
  if (lowLimitDetail) {
    detailParts.push(lowLimitDetail);
  }
  const reason = resolveEntryOrderCancellationReason(
    {
      cancellationSource: trade.cancellationSource ?? null,
      entryCancelAfter: trade.entryCancelAfter ?? null,
      entryOrderStatus: trade.entryOrderStatus ?? null,
      entryOrderStatusUpdatedAt: trade.entryOrderStatusUpdatedAt ?? null
    },
    {
      includeEntryStatus: true,
      defaultToAutoCancel: true
    }
  );
  if (reason) {
    detailParts.push(reason);
  }
  return detailParts.length > 0 ? detailParts.join(' | ') : null;
};

const formatPeriodLabel = (periodMonths: number | null, periodDays: number | null): string => {
  if (periodMonths && periodMonths > 0) {
    if (periodMonths >= 12) {
      const years = Math.floor(periodMonths / 12);
      const remainingMonths = periodMonths % 12;
      return remainingMonths > 0 ? `${years}y ${remainingMonths}m` : `${years}y`;
    }
    return `${periodMonths}m`;
  }
  if (periodDays && periodDays > 0) {
    return `${periodDays}d`;
  }
  return 'N/A';
};

const buildSummary = (backtest: BacktestResultRecord, label: string): BacktestComparisonSummary => {
  const tickerScope = normalizeBacktestScope(backtest.tickerScope);
  const scopeMeta = BACKTEST_SCOPE_META[tickerScope];
  const periodMonths = Number.isFinite(backtest.periodMonths) ? backtest.periodMonths : null;
  const periodDays = Number.isFinite(backtest.periodDays) ? backtest.periodDays : null;
  return {
    label,
    scopeLabel: scopeMeta.label,
    scopeBadge: scopeMeta.badge,
    periodLabel: formatPeriodLabel(periodMonths, periodDays),
    startDate: backtest.startDate,
    endDate: backtest.endDate,
    createdAt: backtest.createdAt
  };
};

const buildEntryAggregation = (trades: Trade[]): EntryAggregation => {
  const entriesByKey = new Map<string, EntryAggregate>();

  for (const trade of trades) {
    if (!isEntryTrade(trade)) {
      continue;
    }
    const notional = Math.abs(trade.quantity * trade.price);
    if (!Number.isFinite(notional) || notional <= 0) {
      continue;
    }
    const dateKey = toDateKey(trade.date);
    const entryKey = `${dateKey}|${trade.ticker}`;
    const aggregate = entriesByKey.get(entryKey) ?? { notional: 0, quantity: 0 };
    aggregate.notional += notional;
    aggregate.quantity += Math.abs(trade.quantity);
    entriesByKey.set(entryKey, aggregate);
  }

  return { entriesByKey };
};

type TradeBucket = {
  engine: Trade[];
  live: Trade[];
};

type ExclusiveTradeSets = {
  engineOnlyIds: Set<string>;
  liveOnlyIds: Set<string>;
};

type TradeDifferenceReason = {
  label: string;
  detail: string | null;
  badge: string;
};

const SKIP_SOURCE_LABELS: Record<string, string> = {
  backtest: 'Engine backtest',
  plan_operations: 'Operation planning'
};

const buildTradesByDate = (trades: Trade[]): Map<string, Trade[]> => {
  const tradesByDate = new Map<string, Trade[]>();

  for (const trade of trades) {
    const dateKey = toDateKey(trade.date);
    const entries = tradesByDate.get(dateKey);
    if (entries) {
      entries.push(trade);
    } else {
      tradesByDate.set(dateKey, [trade]);
    }
  }

  return tradesByDate;
};

const sortTradesForMatch = (a: Trade, b: Trade): number => {
  const createdDiff = a.createdAt.getTime() - b.createdAt.getTime();
  if (createdDiff !== 0) {
    return createdDiff;
  }
  return a.id.localeCompare(b.id);
};

const sortTradesForDisplay = (a: Trade, b: Trade): number => {
  const tickerDiff = a.ticker.localeCompare(b.ticker);
  if (tickerDiff !== 0) {
    return tickerDiff;
  }
  return a.id.localeCompare(b.id);
};

const buildTradeBuckets = (engineTrades: Trade[], liveTrades: Trade[]): Map<string, TradeBucket> => {
  const buckets = new Map<string, TradeBucket>();

  const addTrade = (trade: Trade, side: 'engine' | 'live') => {
    const entryKey = `${toDateKey(trade.date)}|${trade.ticker}`;
    const bucket = buckets.get(entryKey) ?? { engine: [], live: [] };
    bucket[side].push(trade);
    buckets.set(entryKey, bucket);
  };

  engineTrades.forEach(trade => addTrade(trade, 'engine'));
  liveTrades.forEach(trade => addTrade(trade, 'live'));

  return buckets;
};

const groupTradeLinksByTicker = (tradeLinks: TradeTickerLink[]): Map<string, TradeTickerLink[]> => {
  const index = new Map<string, TradeTickerLink[]>();

  for (const trade of tradeLinks) {
    const bucket = index.get(trade.ticker);
    if (bucket) {
      bucket.push(trade);
    } else {
      index.set(trade.ticker, [trade]);
    }
  }

  return index;
};

const groupTradesByTicker = (trades: Trade[]): Map<string, Trade[]> => {
  const index = new Map<string, Trade[]>();

  for (const trade of trades) {
    const bucket = index.get(trade.ticker);
    if (bucket) {
      bucket.push(trade);
    } else {
      index.set(trade.ticker, [trade]);
    }
  }

  return index;
};

const sumTradeQuantity = (trades: Trade[]): number => {
  let total = 0;
  for (const trade of trades) {
    const quantity = Math.abs(trade.quantity);
    if (Number.isFinite(quantity) && quantity > 0) {
      total += quantity;
    }
  }
  return total;
};

const computeAverageEntryPrice = (trades: Trade[]): number | null => {
  let notional = 0;
  let quantity = 0;
  for (const trade of trades) {
    const tradeQuantity = Math.abs(trade.quantity);
    if (!Number.isFinite(tradeQuantity) || tradeQuantity <= 0) {
      continue;
    }
    const tradeNotional = Math.abs(trade.quantity * trade.price);
    if (!Number.isFinite(tradeNotional) || tradeNotional <= 0) {
      continue;
    }
    notional += tradeNotional;
    quantity += tradeQuantity;
  }
  if (notional <= 0 || quantity <= 0) {
    return null;
  }
  return notional / quantity;
};

const computeEntryPriceGap = (engineTrades: Trade[], liveTrades: Trade[]): EntryPriceGapSummary | null => {
  if (engineTrades.length === 0 || liveTrades.length === 0) {
    return null;
  }
  const engineAvg = computeAverageEntryPrice(engineTrades);
  const liveAvg = computeAverageEntryPrice(liveTrades);
  if (engineAvg === null || liveAvg === null || engineAvg <= 0) {
    return null;
  }
  const gapPercent = ((liveAvg - engineAvg) / engineAvg) * 100;
  if (!Number.isFinite(gapPercent) || Math.abs(gapPercent) <= 0.1) {
    return null;
  }
  const label = formatSignedPercentOneDecimal(gapPercent);
  if (!label) {
    return null;
  }
  return {
    label,
    className: gapPercent < 0 ? 'text-success' : 'text-danger'
  };
};

const formatQuantityDifferenceNote = (engineTrades: Trade[], liveTrades: Trade[]): string | null => {
  if (engineTrades.length === 0 || liveTrades.length === 0) {
    return null;
  }
  const engineQuantity = sumTradeQuantity(engineTrades);
  const liveQuantity = sumTradeQuantity(liveTrades);
  if (engineQuantity <= 0 || liveQuantity <= 0) {
    return null;
  }
  const diff = engineQuantity - liveQuantity;
  if (Math.abs(diff) < 1e-8) {
    return null;
  }
  const largerLabel = diff > 0 ? 'Engine' : 'Live';
  const largerQuantity = diff > 0 ? engineQuantity : liveQuantity;
  const smallerQuantity = diff > 0 ? liveQuantity : engineQuantity;
  if (smallerQuantity <= 0) {
    return null;
  }
  const percentDiff = ((largerQuantity - smallerQuantity) / smallerQuantity) * 100;
  const percentLabel = formatPercentValue(percentDiff);
  if (!percentLabel) {
    return null;
  }
  return `${largerLabel} larger by ${percentLabel}%`;
};

const buildExclusiveReasonIndex = (
  trades: Trade[],
  exclusiveIds: Set<string>,
  reasonByTradeId: Map<string, TradeDifferenceReason>
): Map<string, TradeDifferenceReason[]> => {
  const index = new Map<string, TradeDifferenceReason[]>();

  for (const trade of trades) {
    if (!exclusiveIds.has(trade.id)) {
      continue;
    }
    const reason = reasonByTradeId.get(trade.id);
    if (!reason) {
      continue;
    }
    const bucket = index.get(trade.ticker);
    if (bucket) {
      bucket.push(reason);
    } else {
      index.set(trade.ticker, [reason]);
    }
  }

  return index;
};

const buildExclusiveTradeSets = (buckets: Map<string, TradeBucket>): ExclusiveTradeSets => {
  const engineOnlyIds = new Set<string>();
  const liveOnlyIds = new Set<string>();

  for (const [entryKey, bucket] of buckets.entries()) {
    const engineSorted = [...bucket.engine].sort(sortTradesForMatch);
    const liveSorted = [...bucket.live].sort(sortTradesForMatch);
    const matchedCount = Math.min(engineSorted.length, liveSorted.length);

    for (const trade of engineSorted.slice(matchedCount)) {
      engineOnlyIds.add(trade.id);
    }

    for (const trade of liveSorted.slice(matchedCount)) {
      liveOnlyIds.add(trade.id);
    }
  }

  return { engineOnlyIds, liveOnlyIds };
};

const buildSampleDateKeys = (
  engineByDate: Map<string, Trade[]>,
  liveByDate: Map<string, Trade[]>
): string[] => {
  const dateKeys = new Set<string>([...engineByDate.keys(), ...liveByDate.keys()]);
  return Array.from(dateKeys)
    .sort((a, b) => b.localeCompare(a))
    .slice(0, SAMPLE_DAY_LIMIT);
};

const buildTradeLink = (
  trade: Trade,
  isExclusive: boolean,
  side: 'engine' | 'live'
): TradeTickerLink => {
  return {
    id: trade.id,
    ticker: trade.ticker,
    tradeUrl: `/trades/${trade.id}`,
    badgeClass: isExclusive
      ? side === 'engine'
        ? 'bg-danger'
        : 'bg-success'
      : 'bg-light text-dark',
    label: 'Trade',
    detail: null
  };
};

const buildCancelledTradeLink = (trade: Trade, candleLowByKey: Map<string, number>): TradeTickerLink => {
  const lowKey = `${trade.ticker}|${toDateKey(trade.date)}`;
  const candleLow = candleLowByKey.get(lowKey) ?? null;
  return {
    id: trade.id,
    ticker: trade.ticker,
    tradeUrl: `/trades/${trade.id}`,
    badgeClass: 'bg-secondary',
    label: 'Cancelled',
    detail: formatCancelledTradeDetail(trade, candleLow)
  };
};

const buildSampleDays = (
  engineByDate: Map<string, Trade[]>,
  liveByDate: Map<string, Trade[]>,
  engineCancelledByDate: Map<string, Trade[]>,
  liveCancelledByDate: Map<string, Trade[]>,
  cancelledLowByKey: Map<string, number>,
  engineOnlyIds: Set<string>,
  liveOnlyIds: Set<string>,
  dateKeys: string[],
  reasonByTradeId: Map<string, TradeDifferenceReason>
): TradeEntrySampleDay[] => {
  const sampleDays: TradeEntrySampleDay[] = [];

  for (const dateKey of dateKeys) {
    const engineTrades = (engineByDate.get(dateKey) ?? []).sort(sortTradesForDisplay);
    const liveTrades = (liveByDate.get(dateKey) ?? []).sort(sortTradesForDisplay);
    const engineCancelled = (engineCancelledByDate.get(dateKey) ?? []).sort(sortTradesForDisplay);
    const liveCancelled = (liveCancelledByDate.get(dateKey) ?? []).sort(sortTradesForDisplay);
    if (engineTrades.length === 0 && liveTrades.length === 0) {
      continue;
    }

    const engineTradeLinks = engineTrades.map(trade =>
      buildTradeLink(trade, engineOnlyIds.has(trade.id), 'engine')
    );
    const liveTradeLinks = liveTrades.map(trade =>
      buildTradeLink(trade, liveOnlyIds.has(trade.id), 'live')
    );
    const engineCancelledLinks = engineCancelled.map(trade => buildCancelledTradeLink(trade, cancelledLowByKey));
    const liveCancelledLinks = liveCancelled.map(trade => buildCancelledTradeLink(trade, cancelledLowByKey));
    const engineTradeLinksByTicker = groupTradeLinksByTicker(engineTradeLinks);
    const liveTradeLinksByTicker = groupTradeLinksByTicker(liveTradeLinks);
    const engineCancelledByTicker = groupTradeLinksByTicker(engineCancelledLinks);
    const liveCancelledByTicker = groupTradeLinksByTicker(liveCancelledLinks);
    const engineTradesByTicker = groupTradesByTicker(engineTrades);
    const liveTradesByTicker = groupTradesByTicker(liveTrades);
    const engineReasonsByTicker = buildExclusiveReasonIndex(engineTrades, engineOnlyIds, reasonByTradeId);
    const liveReasonsByTicker = buildExclusiveReasonIndex(liveTrades, liveOnlyIds, reasonByTradeId);
    const tickers = Array.from(
      new Set<string>([...engineTradeLinksByTicker.keys(), ...liveTradeLinksByTicker.keys()])
    ).sort((a, b) => a.localeCompare(b));
    const rows: TradeEntrySampleRow[] = tickers.map(ticker => {
      const engineEntryTrades = engineTradeLinksByTicker.get(ticker) ?? [];
      const liveEntryTrades = liveTradeLinksByTicker.get(ticker) ?? [];
      const engineCancelledTrades = engineCancelledByTicker.get(ticker) ?? [];
      const liveCancelledTrades = liveCancelledByTicker.get(ticker) ?? [];

      return {
        ticker,
        engine: {
          trades: engineEntryTrades.length > 0 ? engineEntryTrades : engineCancelledTrades,
          reasons: liveReasonsByTicker.get(ticker) ?? []
        },
        live: {
          trades: liveEntryTrades.length > 0 ? liveEntryTrades : liveCancelledTrades,
          reasons: engineReasonsByTicker.get(ticker) ?? []
        },
        quantityNote: formatQuantityDifferenceNote(
          engineTradesByTicker.get(ticker) ?? [],
          liveTradesByTicker.get(ticker) ?? []
        ),
        entryPriceGap: computeEntryPriceGap(
          engineTradesByTicker.get(ticker) ?? [],
          liveTradesByTicker.get(ticker) ?? []
        )
      };
    });

    sampleDays.push({
      date: new Date(`${dateKey}T00:00:00Z`),
      engineCount: engineTrades.length,
      liveCount: liveTrades.length,
      rows
    });
  }

  return sampleDays;
};

const buildSnapshotMap = (
  snapshots: BacktestResultRecord['dailySnapshots']
): Map<string, BacktestResultRecord['dailySnapshots'][number]> => {
  const map = new Map<string, BacktestResultRecord['dailySnapshots'][number]>();
  for (const snapshot of snapshots) {
    map.set(toDateKey(snapshot.date), snapshot);
  }
  return map;
};

type CandleIndex = {
  candles: Candle[];
  indexByDate: Map<string, number>;
};

const buildCandleIndex = (candlesByTicker: Record<string, Candle[]>): Map<string, CandleIndex> => {
  const index = new Map<string, CandleIndex>();
  for (const [ticker, candles] of Object.entries(candlesByTicker)) {
    const indexByDate = new Map<string, number>();
    candles.forEach((candle, idx) => {
      indexByDate.set(toDateKey(candle.date), idx);
    });
    index.set(ticker, { candles, indexByDate });
  }
  return index;
};

const buildSignalSkipIndex = (skips: AccountSignalSkipRow[]): Map<string, AccountSignalSkipRow[]> => {
  const index = new Map<string, AccountSignalSkipRow[]>();
  for (const skip of skips) {
    const action = typeof skip.action === 'string' ? skip.action.toLowerCase() : '';
    const source = typeof skip.source === 'string' ? skip.source.toLowerCase() : '';
    const dateKey = typeof skip.signal_date === 'string' ? skip.signal_date : toDateKey(skip.signal_date);
    const key = `${skip.ticker}|${action}|${source}|${dateKey}`;
    const bucket = index.get(key) ?? [];
    bucket.push(skip);
    index.set(key, bucket);
  }

  for (const [key, bucket] of index.entries()) {
    bucket.sort((a, b) => {
      const aDate = a.created_at instanceof Date ? a.created_at : new Date(a.created_at);
      const bDate = b.created_at instanceof Date ? b.created_at : new Date(b.created_at);
      return bDate.getTime() - aDate.getTime();
    });
    index.set(key, bucket);
  }

  return index;
};

const buildSignalSkipDateKeys = (tradeDate: Date): string[] => {
  const keys = [toDateKey(tradeDate)];
  const maxOffsetDays = tradeDate.getUTCDay() === 1 ? SIGNAL_SKIP_LOOKBACK_DAYS : 1;
  for (let offset = 1; offset <= maxOffsetDays; offset += 1) {
    keys.push(toDateKey(new Date(tradeDate.getTime() - ONE_DAY_MS * offset)));
  }
  return keys;
};

const matchSignalSkip = (
  trade: Trade,
  source: string,
  index: Map<string, AccountSignalSkipRow[]>
): AccountSignalSkipRow | null => {
  const action = trade.quantity < 0 ? 'sell' : 'buy';
  const dateKeys = buildSignalSkipDateKeys(trade.date);
  for (const dateKey of dateKeys) {
    const key = `${trade.ticker}|${action}|${source}|${dateKey}`;
    const matches = index.get(key);
    if (matches && matches.length > 0) {
      return matches[0];
    }
  }
  return null;
};

const buildSignalSkipReason = (skip: AccountSignalSkipRow, source: string): TradeDifferenceReason => {
  const sourceLabel = SKIP_SOURCE_LABELS[source] ?? source;
  const { label, detail } = formatSignalSkipReason(skip.reason, skip.details);
  const normalizedReason = typeof skip.reason === 'string' ? skip.reason.trim().toLowerCase() : '';
  const prefix = normalizedReason === 'operation_requested' ? 'Planned in' : 'Skipped in';
  const detailParts = [`${prefix} ${sourceLabel}`];
  if (detail) {
    detailParts.push(detail);
  }

  return {
    label,
    detail: detailParts.join(' | '),
    badge: 'bg-secondary'
  };
};

const buildExclusiveTradeReason = ({
  trade,
  otherBacktest,
  otherBacktestLabel,
  otherTickersTraded,
  otherSnapshotsByDate,
  skipIndex,
  skipSource
}: {
  trade: Trade;
  otherBacktest: BacktestResultRecord;
  otherBacktestLabel: string;
  otherTickersTraded: Set<string>;
  otherSnapshotsByDate: Map<string, BacktestResultRecord['dailySnapshots'][number]>;
  skipIndex: Map<string, AccountSignalSkipRow[]>;
  skipSource: string;
}): TradeDifferenceReason => {
  const tradeDate = trade.date;
  const tradeTime = tradeDate.getTime();
  if (tradeTime < otherBacktest.startDate.getTime() || tradeTime > otherBacktest.endDate.getTime()) {
    return {
      label: `Outside ${otherBacktestLabel} range`,
      detail: `${toDateKey(otherBacktest.startDate)} to ${toDateKey(otherBacktest.endDate)}`,
      badge: 'bg-warning text-dark'
    };
  }

  const matchedSkip = matchSignalSkip(trade, skipSource, skipIndex);
  if (matchedSkip) {
    return buildSignalSkipReason(matchedSkip, skipSource);
  }

  const snapshot = otherSnapshotsByDate.get(toDateKey(tradeDate));
  if (snapshot && typeof snapshot.missedTradesDueToCash === 'number' && snapshot.missedTradesDueToCash > 0) {
    const missedTrades = Math.round(snapshot.missedTradesDueToCash);
    const missedLabel = missedTrades === 1 ? '1 trade missed' : `${missedTrades} trades missed`;
    return {
      label: `Cash constrained on ${otherBacktestLabel}`,
      detail: missedLabel,
      badge: 'bg-danger'
    };
  }

  if (!otherTickersTraded.has(trade.ticker)) {
    return {
      label: `Not in ${otherBacktestLabel}`,
      detail: 'No entries recorded',
      badge: 'bg-secondary'
    };
  }

  return {
    label: 'No obvious driver found',
    detail: null,
    badge: 'bg-light text-dark'
  };
};

const buildExclusiveTradeReasonMap = ({
  engineTrades,
  liveTrades,
  engineOnlyIds,
  liveOnlyIds,
  engineBacktest,
  liveBacktest,
  skipIndex
}: {
  engineTrades: Trade[];
  liveTrades: Trade[];
  engineOnlyIds: Set<string>;
  liveOnlyIds: Set<string>;
  engineBacktest: BacktestResultRecord;
  liveBacktest: BacktestResultRecord;
  skipIndex: Map<string, AccountSignalSkipRow[]>;
}): Map<string, TradeDifferenceReason> => {
  const engineTickersTraded = new Set(engineTrades.map(trade => trade.ticker));
  const liveTickersTraded = new Set(liveTrades.map(trade => trade.ticker));
  const engineSnapshotsByDate = buildSnapshotMap(engineBacktest.dailySnapshots);
  const liveSnapshotsByDate = buildSnapshotMap(liveBacktest.dailySnapshots);
  const reasonByTradeId = new Map<string, TradeDifferenceReason>();

  const addReason = (trade: Trade, side: 'engine' | 'live') => {
    const isEngine = side === 'engine';
    const reason = buildExclusiveTradeReason({
      trade,
      otherBacktest: isEngine ? liveBacktest : engineBacktest,
      otherBacktestLabel: isEngine ? 'live trades backtest' : 'engine backtest',
      otherTickersTraded: isEngine ? liveTickersTraded : engineTickersTraded,
      otherSnapshotsByDate: isEngine ? liveSnapshotsByDate : engineSnapshotsByDate,
      skipIndex,
      skipSource: isEngine ? 'plan_operations' : 'backtest'
    });
    reasonByTradeId.set(trade.id, reason);
  };

  engineTrades
    .filter(trade => engineOnlyIds.has(trade.id))
    .forEach(trade => addReason(trade, 'engine'));
  liveTrades
    .filter(trade => liveOnlyIds.has(trade.id))
    .forEach(trade => addReason(trade, 'live'));

  return reasonByTradeId;
};

const buildPenetrationFallback = (setting: number | null): BacktestComparisonPenetration => ({
  hasData: false,
  setting,
  impliedAvg: null,
  gap: null,
  gapClass: 'text-muted',
  matchedEntries: 0
});

const computePenetration = ({
  trades,
  candlesByTicker,
  buyDiscountRatio,
  setting
}: {
  trades: Trade[];
  candlesByTicker: Record<string, Candle[]>;
  buyDiscountRatio: number | null;
  setting: number | null;
}): BacktestComparisonPenetration => {
  if (!buyDiscountRatio || buyDiscountRatio <= 0) {
    return buildPenetrationFallback(setting);
  }

  const candleIndex = buildCandleIndex(candlesByTicker);
  let weightedSum = 0;
  let weightTotal = 0;
  let matched = 0;

  for (const trade of trades) {
    if (trade.quantity <= 0) {
      continue;
    }
    const notional = Math.abs(trade.quantity * trade.price);
    if (!Number.isFinite(notional) || notional <= 0) {
      continue;
    }
    const tickerIndex = candleIndex.get(trade.ticker);
    if (!tickerIndex) {
      continue;
    }
    const tradeKey = toDateKey(trade.date);
    const candlePosition = tickerIndex.indexByDate.get(tradeKey);
    if (candlePosition === undefined || candlePosition <= 0) {
      continue;
    }
    const executionCandle = tickerIndex.candles[candlePosition];
    const signalCandle = tickerIndex.candles[candlePosition - 1];
    const planningClose = selectSizingPrice(signalCandle);
    if (planningClose === null || !Number.isFinite(planningClose)) {
      continue;
    }
    const limitPrice = planningClose * (1 - buyDiscountRatio);
    if (!Number.isFinite(limitPrice) || limitPrice <= 0) {
      continue;
    }
    const low = executionCandle.low;
    if (!Number.isFinite(low)) {
      continue;
    }
    const penetrationRatio = (limitPrice - low) / limitPrice;
    if (!Number.isFinite(penetrationRatio) || penetrationRatio < 0) {
      continue;
    }
    weightedSum += penetrationRatio * notional;
    weightTotal += notional;
    matched += 1;
  }

  const hasData = matched > 0 && weightTotal > 0;
  const impliedAvg = hasData ? weightedSum / weightTotal : null;
  const gap = impliedAvg !== null && setting !== null ? impliedAvg - setting : null;
  const gapClass = gap === null
    ? 'text-muted'
    : gap > 0
      ? 'text-danger'
      : gap < 0
        ? 'text-success'
        : 'text-muted';

  return {
    hasData,
    setting,
    impliedAvg,
    gap,
    gapClass,
    matchedEntries: matched
  };
};

const computeSlippage = (
  engineByKey: Map<string, EntryAggregate>,
  liveByKey: Map<string, EntryAggregate>,
  setting: number | null
): BacktestComparisonSlippage => {
  let weightedSum = 0;
  let weightedAbsSum = 0;
  let weightTotal = 0;
  let matched = 0;

  for (const [key, engineAgg] of engineByKey.entries()) {
    const liveAgg = liveByKey.get(key);
    if (!liveAgg) {
      continue;
    }
    const engineQuantity = engineAgg.quantity;
    const liveQuantity = liveAgg.quantity;
    if (!engineQuantity || !liveQuantity) {
      continue;
    }
    const engineAvgPrice = engineAgg.notional / engineQuantity;
    const liveAvgPrice = liveAgg.notional / liveQuantity;
    if (!Number.isFinite(engineAvgPrice) || engineAvgPrice <= 0 || !Number.isFinite(liveAvgPrice)) {
      continue;
    }
    const slippage = (liveAvgPrice - engineAvgPrice) / engineAvgPrice;
    const weight = engineAgg.notional;
    weightedSum += slippage * weight;
    weightedAbsSum += Math.abs(slippage) * weight;
    weightTotal += weight;
    matched += 1;
  }

  const hasData = matched > 0 && weightTotal > 0;
  const impliedAvg = hasData ? weightedSum / weightTotal : null;
  const impliedAvgAbs = hasData ? weightedAbsSum / weightTotal : null;
  const gap = impliedAvgAbs !== null && setting !== null ? impliedAvgAbs - setting : null;
  const gapClass = gap === null
    ? 'text-muted'
    : gap > 0
      ? 'text-danger'
      : gap < 0
        ? 'text-success'
        : 'text-muted';

  return {
    hasData,
    setting,
    impliedAvg,
    impliedAvgAbs,
    gap,
    gapClass,
    matchedEntries: matched
  };
};

const computeExpenseRatioAverage = (
  trades: Trade[],
  expenseMap: Map<string, number | null>
): { avg: number | null; notional: number } => {
  let weightedSum = 0;
  let totalNotional = 0;
  for (const trade of trades) {
    if (!isEntryTrade(trade)) {
      continue;
    }
    const notional = Math.abs(trade.quantity * trade.price);
    if (!Number.isFinite(notional) || notional <= 0) {
      continue;
    }
    const ratio = expenseMap.get(trade.ticker);
    const ratioValue = typeof ratio === 'number' && Number.isFinite(ratio) ? ratio : 0;
    weightedSum += ratioValue * notional;
    totalNotional += notional;
  }

  if (!totalNotional) {
    return { avg: null, notional: 0 };
  }

  return {
    avg: weightedSum / totalNotional,
    notional: totalNotional
  };
};

const loadExpenseRatioMap = async (db: Database, tickers: string[]): Promise<Map<string, number | null>> => {
  const unique = Array.from(new Set(tickers));
  const results = await Promise.all(unique.map((ticker) => db.tickers.getTicker(ticker)));
  const expenseMap = new Map<string, number | null>();
  unique.forEach((ticker, index) => {
    const row = results[index];
    const ratio = row?.expenseRatio;
    expenseMap.set(ticker, typeof ratio === 'number' && Number.isFinite(ratio) ? ratio : null);
  });
  return expenseMap;
};

const parseSettingNumber = (raw: string | null, fallback: number | null): number | null => {
  if (typeof raw === 'string' && raw.trim().length > 0) {
    const parsed = Number(raw);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
};

const pickLatestBacktest = (
  backtests: BacktestResultRecord[],
  scopes: BacktestScope[]
): BacktestResultRecord | null => {
  for (const scope of scopes) {
    const candidates = backtests
      .filter((backtest) => normalizeBacktestScope(backtest.tickerScope) === scope)
      .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
    if (candidates.length > 0) {
      return candidates[0];
    }
  }
  return null;
};

export const buildBacktestComparisonView = async ({
  db,
  strategyId,
  userId,
  backtests,
  isEligible
}: {
  db: Database;
  strategyId: string;
  userId: number;
  backtests: BacktestResultRecord[];
  isEligible: boolean;
}): Promise<BacktestComparisonView> => {
  if (!isEligible) {
    return {
      isEligible: false,
      hasEngine: false,
      hasLive: false,
      sampleDays: []
    };
  }

  const engineBacktest = pickLatestBacktest(backtests, ['training', 'validation', 'all']);
  const liveBacktest = pickLatestBacktest(backtests, ['live']);
  const hasEngine = Boolean(engineBacktest);
  const hasLive = Boolean(liveBacktest);

  if (!hasEngine || !hasLive) {
    const missing = [];
    if (!hasEngine) missing.push('engine backtest');
    if (!hasLive) missing.push('live trades backtest');
    return {
      isEligible: true,
      hasEngine,
      hasLive,
      notice: missing.length > 0
        ? `Need ${missing.join(' and ')} results to compare entries.`
        : 'Need live and engine backtests to compare entries.',
      sampleDays: []
    };
  }

  const [slippageRaw, penetrationRaw, engineTradesRaw, liveTradesRaw, strategy] = await Promise.all([
    db.settings.getSettingValue(SETTING_KEYS.TRADE_SLIPPAGE_RATE),
    db.settings.getSettingValue(SETTING_KEYS.LIMIT_BUY_PENETRATION_RATIO),
    db.trades.getTrades(strategyId, undefined, undefined, undefined, undefined, engineBacktest!.id, userId),
    db.trades.getTrades(strategyId, undefined, undefined, undefined, undefined, liveBacktest!.id, userId),
    db.strategies.getStrategy(strategyId, userId)
  ]);

  const slippageSetting = parseSettingNumber(slippageRaw, SLIPPAGE_DEFAULT);
  const penetrationSetting = parseSettingNumber(penetrationRaw, PENETRATION_DEFAULT);
  const engineTrades = engineTradesRaw.filter(isEntryTrade);
  const liveTrades = liveTradesRaw.filter(isEntryTrade);
  const engineCancelledTrades = engineTradesRaw.filter(isCancelledTrade);
  const liveCancelledTrades = liveTradesRaw.filter(isCancelledTrade);
  const buyDiscountRatio = parseNumericValue(strategy?.parameters?.buyDiscountRatio ?? null);

  const engineAggregation = buildEntryAggregation(engineTrades);
  const liveAggregation = buildEntryAggregation(liveTrades);
  const tradeBuckets = buildTradeBuckets(engineTrades, liveTrades);
  const { engineOnlyIds, liveOnlyIds } = buildExclusiveTradeSets(tradeBuckets);
  const engineTradesByDate = buildTradesByDate(engineTrades);
  const liveTradesByDate = buildTradesByDate(liveTrades);
  const engineCancelledByDate = buildTradesByDate(engineCancelledTrades);
  const liveCancelledByDate = buildTradesByDate(liveCancelledTrades);
  const sampleDateKeys = buildSampleDateKeys(engineTradesByDate, liveTradesByDate);
  const sampleDateSet = new Set(sampleDateKeys);
  const cancelledLowByKey = new Map<string, number>();
  const cancelledTrades = [...engineCancelledTrades, ...liveCancelledTrades].filter(trade =>
    sampleDateSet.has(toDateKey(trade.date))
  );
  if (cancelledTrades.length > 0) {
    const tickers = Array.from(new Set(cancelledTrades.map(trade => trade.ticker)));
    const timestamps = cancelledTrades.map(trade => trade.date.getTime());
    const start = new Date(Math.min(...timestamps));
    const end = new Date(Math.max(...timestamps));
    const candlesByTicker = await db.candles.getCandles(tickers, start, end);
    for (const [ticker, candles] of Object.entries(candlesByTicker)) {
      for (const candle of candles) {
        if (Number.isFinite(candle.low)) {
          const key = `${ticker}|${toDateKey(candle.date)}`;
          cancelledLowByKey.set(key, candle.low);
        }
      }
    }
  }
  const exclusiveCandidates = [
    ...engineTrades.filter(trade => engineOnlyIds.has(trade.id)),
    ...liveTrades.filter(trade => liveOnlyIds.has(trade.id))
  ];
  let skipIndex = new Map<string, AccountSignalSkipRow[]>();
  if (exclusiveCandidates.length > 0) {
    const timestamps = exclusiveCandidates.map(trade => trade.date.getTime());
    const minTime = Math.min(...timestamps);
    const maxTime = Math.max(...timestamps);
    const start = new Date(minTime - ONE_DAY_MS * SIGNAL_SKIP_LOOKBACK_DAYS);
    const end = new Date(maxTime + ONE_DAY_MS);
    const skips = await db.accountSignalSkips.getAccountSignalSkipsForStrategyInRange(strategyId, start, end, [
      'backtest',
      'plan_operations'
    ]);
    skipIndex = buildSignalSkipIndex(skips);
  }

  let reasonByTradeId = new Map<string, TradeDifferenceReason>();
  if (exclusiveCandidates.length > 0) {
    reasonByTradeId = buildExclusiveTradeReasonMap({
      engineTrades,
      liveTrades,
      engineOnlyIds,
      liveOnlyIds,
      engineBacktest: engineBacktest!,
      liveBacktest: liveBacktest!,
      skipIndex
    });
  }
  const sampleDays = buildSampleDays(
    engineTradesByDate,
    liveTradesByDate,
    engineCancelledByDate,
    liveCancelledByDate,
    cancelledLowByKey,
    engineOnlyIds,
    liveOnlyIds,
    sampleDateKeys,
    reasonByTradeId
  );
  const slippage = computeSlippage(engineAggregation.entriesByKey, liveAggregation.entriesByKey, slippageSetting);
  let penetration = buildPenetrationFallback(penetrationSetting);
  if (buyDiscountRatio !== null && buyDiscountRatio > 0) {
    const liveLongTrades = liveTrades.filter((trade) => trade.quantity > 0);
    if (liveLongTrades.length > 0) {
      const tradeTimes = liveLongTrades.map((trade) => trade.date.getTime());
      const start = new Date(Math.min(...tradeTimes) - ONE_DAY_MS * 10);
      const end = new Date(Math.max(...tradeTimes));
      const tickers = Array.from(new Set(liveLongTrades.map((trade) => trade.ticker)));
      const candlesByTicker = await db.candles.getCandles(tickers, start, end);
      penetration = computePenetration({
        trades: liveLongTrades,
        candlesByTicker,
        buyDiscountRatio,
        setting: penetrationSetting
      });
    }
  }

  const tickers = [
    ...engineTrades.map((trade) => trade.ticker),
    ...liveTrades.map((trade) => trade.ticker)
  ];
  const expenseMap = await loadExpenseRatioMap(db, tickers);
  const engineExpense = computeExpenseRatioAverage(engineTrades, expenseMap);
  const liveExpense = computeExpenseRatioAverage(liveTrades, expenseMap);
  const expenseGap =
    engineExpense.avg !== null && liveExpense.avg !== null ? liveExpense.avg - engineExpense.avg : null;
  const expenseGapClass = expenseGap === null
    ? 'text-muted'
    : expenseGap > 0
      ? 'text-danger'
      : expenseGap < 0
        ? 'text-success'
        : 'text-muted';

  return {
    isEligible: true,
    hasEngine,
    hasLive,
    engine: buildSummary(engineBacktest!, 'Engine backtest'),
    live: buildSummary(liveBacktest!, 'Live trades backtest'),
    slippage,
    penetration,
    expenseRatio: {
      hasData: engineExpense.avg !== null || liveExpense.avg !== null,
      engineAvg: engineExpense.avg,
      liveAvg: liveExpense.avg,
      gap: expenseGap,
      gapClass: expenseGapClass,
      engineNotional: engineExpense.notional,
      liveNotional: liveExpense.notional
    },
    sampleDays
  };
};
