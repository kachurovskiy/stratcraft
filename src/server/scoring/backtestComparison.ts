import type { Database } from '../database/Database';
import type { BacktestResultRecord } from '../database/types';
import type { BacktestScope, Trade } from '../../shared/types/StrategyTemplate';
import { SETTING_KEYS } from '../constants';

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
  isExclusive: boolean;
};

type TradeEntrySampleDay = {
  date: Date;
  engineCount: number;
  liveCount: number;
  engineTrades: TradeTickerLink[];
  liveTrades: TradeTickerLink[];
};

type TradeDifferenceSample = {
  id: string;
  ticker: string;
  tradeUrl: string;
  date: Date;
  quantity: number;
  price: number;
  sideLabel: string;
  sideBadge: string;
  reasonLabel: string;
  reasonDetail: string | null;
  reasonBadge: string;
};

export type BacktestComparisonView = {
  isEligible: boolean;
  hasEngine: boolean;
  hasLive: boolean;
  notice?: string;
  engine?: BacktestComparisonSummary;
  live?: BacktestComparisonSummary;
  slippage?: BacktestComparisonSlippage;
  expenseRatio?: BacktestComparisonExpenseRatio;
  sampleDays: TradeEntrySampleDay[];
  sampleTrades: TradeDifferenceSample[];
};

const SLIPPAGE_DEFAULT = 0.003;
const SAMPLE_DAY_LIMIT = 5;
const SAMPLE_TRADE_LIMIT = 10;
const ENTRY_STATUS = new Set<Trade['status']>(['active', 'closed']);

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
  differenceDates: Set<string>;
};

type TradeDifferenceReason = {
  label: string;
  detail: string | null;
  badge: string;
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

const buildExclusiveTradeSets = (buckets: Map<string, TradeBucket>): ExclusiveTradeSets => {
  const engineOnlyIds = new Set<string>();
  const liveOnlyIds = new Set<string>();
  const differenceDates = new Set<string>();

  for (const [entryKey, bucket] of buckets.entries()) {
    const engineSorted = [...bucket.engine].sort(sortTradesForMatch);
    const liveSorted = [...bucket.live].sort(sortTradesForMatch);
    const matchedCount = Math.min(engineSorted.length, liveSorted.length);

    if (engineSorted.length !== liveSorted.length) {
      const [dateKey] = entryKey.split('|');
      differenceDates.add(dateKey);
    }

    for (const trade of engineSorted.slice(matchedCount)) {
      engineOnlyIds.add(trade.id);
    }

    for (const trade of liveSorted.slice(matchedCount)) {
      liveOnlyIds.add(trade.id);
    }
  }

  return { engineOnlyIds, liveOnlyIds, differenceDates };
};

const buildTradeLink = (trade: Trade, isExclusive: boolean, side: 'engine' | 'live'): TradeTickerLink => ({
  id: trade.id,
  ticker: trade.ticker,
  tradeUrl: `/trades/${trade.id}`,
  isExclusive,
  badgeClass: isExclusive
    ? side === 'engine'
      ? 'bg-danger'
      : 'bg-success'
    : 'bg-light text-dark'
});

const buildSampleDays = (
  engineByDate: Map<string, Trade[]>,
  liveByDate: Map<string, Trade[]>,
  engineOnlyIds: Set<string>,
  liveOnlyIds: Set<string>,
  differenceDates: Set<string>
): TradeEntrySampleDay[] => {
  const dateKeys = Array.from(differenceDates).sort();
  const sampleDays: TradeEntrySampleDay[] = [];

  for (const dateKey of dateKeys) {
    const engineTrades = (engineByDate.get(dateKey) ?? []).sort(sortTradesForDisplay);
    const liveTrades = (liveByDate.get(dateKey) ?? []).sort(sortTradesForDisplay);
    if (engineTrades.length === 0 && liveTrades.length === 0) {
      continue;
    }

    sampleDays.push({
      date: new Date(`${dateKey}T00:00:00Z`),
      engineCount: engineTrades.length,
      liveCount: liveTrades.length,
      engineTrades: engineTrades.map(trade =>
        buildTradeLink(trade, engineOnlyIds.has(trade.id), 'engine')
      ),
      liveTrades: liveTrades.map(trade =>
        buildTradeLink(trade, liveOnlyIds.has(trade.id), 'live')
      )
    });

    if (sampleDays.length >= SAMPLE_DAY_LIMIT) {
      break;
    }
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

const buildExclusiveTradeReason = ({
  trade,
  otherBacktest,
  otherBacktestLabel,
  otherScopeLabel,
  otherTickersInScope,
  otherTickersTraded,
  otherSnapshotsByDate
}: {
  trade: Trade;
  otherBacktest: BacktestResultRecord;
  otherBacktestLabel: string;
  otherScopeLabel: string;
  otherTickersInScope: Set<string>;
  otherTickersTraded: Set<string>;
  otherSnapshotsByDate: Map<string, BacktestResultRecord['dailySnapshots'][number]>;
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

  if (otherTickersInScope.size > 0 && !otherTickersInScope.has(trade.ticker)) {
    return {
      label: `Ticker not in ${otherBacktestLabel} scope`,
      detail: otherScopeLabel,
      badge: 'bg-info text-dark'
    };
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
      label: `Ticker never traded in ${otherBacktestLabel}`,
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

const buildExclusiveTradeSamples = ({
  engineTrades,
  liveTrades,
  engineOnlyIds,
  liveOnlyIds,
  engineBacktest,
  liveBacktest
}: {
  engineTrades: Trade[];
  liveTrades: Trade[];
  engineOnlyIds: Set<string>;
  liveOnlyIds: Set<string>;
  engineBacktest: BacktestResultRecord;
  liveBacktest: BacktestResultRecord;
}): TradeDifferenceSample[] => {
  const engineScopeLabel = BACKTEST_SCOPE_META[normalizeBacktestScope(engineBacktest.tickerScope)].label;
  const liveScopeLabel = BACKTEST_SCOPE_META[normalizeBacktestScope(liveBacktest.tickerScope)].label;
  const engineTickersInScope = new Set(engineBacktest.tickers);
  const liveTickersInScope = new Set(liveBacktest.tickers);
  const engineTickersTraded = new Set(engineTrades.map(trade => trade.ticker));
  const liveTickersTraded = new Set(liveTrades.map(trade => trade.ticker));
  const engineSnapshotsByDate = buildSnapshotMap(engineBacktest.dailySnapshots);
  const liveSnapshotsByDate = buildSnapshotMap(liveBacktest.dailySnapshots);

  const candidates: Array<{ trade: Trade; side: 'engine' | 'live' }> = [
    ...engineTrades
      .filter(trade => engineOnlyIds.has(trade.id))
      .map(trade => ({ trade, side: 'engine' as const })),
    ...liveTrades
      .filter(trade => liveOnlyIds.has(trade.id))
      .map(trade => ({ trade, side: 'live' as const }))
  ];

  const sortedCandidates = candidates.sort((a, b) => {
    const dateDiff = b.trade.date.getTime() - a.trade.date.getTime();
    if (dateDiff !== 0) {
      return dateDiff;
    }
    const tickerDiff = a.trade.ticker.localeCompare(b.trade.ticker);
    if (tickerDiff !== 0) {
      return tickerDiff;
    }
    return a.trade.id.localeCompare(b.trade.id);
  });

  return sortedCandidates.slice(0, SAMPLE_TRADE_LIMIT).map(({ trade, side }) => {
    const isEngine = side === 'engine';
    const otherBacktest = isEngine ? liveBacktest : engineBacktest;
    const otherBacktestLabel = isEngine ? 'live trades backtest' : 'engine backtest';
    const otherScopeLabel = isEngine ? liveScopeLabel : engineScopeLabel;
    const otherTickersInScope = isEngine ? liveTickersInScope : engineTickersInScope;
    const otherTickersTraded = isEngine ? liveTickersTraded : engineTickersTraded;
    const otherSnapshotsByDate = isEngine ? liveSnapshotsByDate : engineSnapshotsByDate;
    const reason = buildExclusiveTradeReason({
      trade,
      otherBacktest,
      otherBacktestLabel,
      otherScopeLabel,
      otherTickersInScope,
      otherTickersTraded,
      otherSnapshotsByDate
    });

    return {
      id: trade.id,
      ticker: trade.ticker,
      tradeUrl: `/trades/${trade.id}`,
      date: trade.date,
      quantity: trade.quantity,
      price: trade.price,
      sideLabel: isEngine ? 'Engine only' : 'Live only',
      sideBadge: isEngine ? 'bg-danger' : 'bg-success',
      reasonLabel: reason.label,
      reasonDetail: reason.detail,
      reasonBadge: reason.badge
    };
  });
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
      sampleDays: [],
      sampleTrades: []
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
      sampleDays: [],
      sampleTrades: []
    };
  }

  const [slippageRaw, engineTradesRaw, liveTradesRaw] = await Promise.all([
    db.settings.getSettingValue(SETTING_KEYS.TRADE_SLIPPAGE_RATE),
    db.trades.getTrades(strategyId, undefined, undefined, undefined, undefined, engineBacktest!.id, userId),
    db.trades.getTrades(strategyId, undefined, undefined, undefined, undefined, liveBacktest!.id, userId)
  ]);

  const slippageSetting = parseSettingNumber(slippageRaw, SLIPPAGE_DEFAULT);
  const engineTrades = engineTradesRaw.filter(isEntryTrade);
  const liveTrades = liveTradesRaw.filter(isEntryTrade);

  const engineAggregation = buildEntryAggregation(engineTrades);
  const liveAggregation = buildEntryAggregation(liveTrades);
  const tradeBuckets = buildTradeBuckets(engineTrades, liveTrades);
  const { engineOnlyIds, liveOnlyIds, differenceDates } = buildExclusiveTradeSets(tradeBuckets);
  const engineTradesByDate = buildTradesByDate(engineTrades);
  const liveTradesByDate = buildTradesByDate(liveTrades);
  const sampleDays = buildSampleDays(
    engineTradesByDate,
    liveTradesByDate,
    engineOnlyIds,
    liveOnlyIds,
    differenceDates
  );
  const sampleTrades = buildExclusiveTradeSamples({
    engineTrades,
    liveTrades,
    engineOnlyIds,
    liveOnlyIds,
    engineBacktest: engineBacktest!,
    liveBacktest: liveBacktest!
  });
  const slippage = computeSlippage(engineAggregation.entriesByKey, liveAggregation.entriesByKey, slippageSetting);

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
    expenseRatio: {
      hasData: engineExpense.avg !== null || liveExpense.avg !== null,
      engineAvg: engineExpense.avg,
      liveAvg: liveExpense.avg,
      gap: expenseGap,
      gapClass: expenseGapClass,
      engineNotional: engineExpense.notional,
      liveNotional: liveExpense.notional
    },
    sampleDays,
    sampleTrades
  };
};
