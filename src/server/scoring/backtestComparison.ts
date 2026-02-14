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

type TradeEntryDifferenceDay = {
  date: Date;
  engineCount: number;
  liveCount: number;
  engineOnlyDisplay: string;
  engineOnlyExtra: number;
  liveOnlyDisplay: string;
  liveOnlyExtra: number;
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
  sampleDays: TradeEntryDifferenceDay[];
};

const SLIPPAGE_DEFAULT = 0.003;
const SAMPLE_DAY_LIMIT = 5;
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
  entriesByDate: Map<string, Set<string>>;
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
  const entriesByDate = new Map<string, Set<string>>();
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
    const dateSet = entriesByDate.get(dateKey);
    if (dateSet) {
      dateSet.add(trade.ticker);
    } else {
      entriesByDate.set(dateKey, new Set([trade.ticker]));
    }

    const entryKey = `${dateKey}|${trade.ticker}`;
    const aggregate = entriesByKey.get(entryKey) ?? { notional: 0, quantity: 0 };
    aggregate.notional += notional;
    aggregate.quantity += Math.abs(trade.quantity);
    entriesByKey.set(entryKey, aggregate);
  }

  return { entriesByDate, entriesByKey };
};

const buildTickerDisplay = (tickers: string[], limit = 8): { display: string; extra: number } => {
  if (!tickers.length) {
    return { display: 'None', extra: 0 };
  }
  const sorted = [...tickers].sort((a, b) => a.localeCompare(b));
  const slice = sorted.slice(0, limit);
  const extra = Math.max(0, sorted.length - slice.length);
  return { display: slice.join(', '), extra };
};

const buildSampleDays = (
  engineByDate: Map<string, Set<string>>,
  liveByDate: Map<string, Set<string>>
): TradeEntryDifferenceDay[] => {
  const dateKeys = Array.from(new Set([...engineByDate.keys(), ...liveByDate.keys()])).sort();
  const sampleDays: TradeEntryDifferenceDay[] = [];

  for (const dateKey of dateKeys) {
    const engineSet = engineByDate.get(dateKey) ?? new Set<string>();
    const liveSet = liveByDate.get(dateKey) ?? new Set<string>();
    const engineOnly = Array.from(engineSet).filter((ticker) => !liveSet.has(ticker));
    const liveOnly = Array.from(liveSet).filter((ticker) => !engineSet.has(ticker));
    if (!engineOnly.length && !liveOnly.length) {
      continue;
    }

    const engineDisplay = buildTickerDisplay(engineOnly);
    const liveDisplay = buildTickerDisplay(liveOnly);
    sampleDays.push({
      date: new Date(`${dateKey}T00:00:00Z`),
      engineCount: engineSet.size,
      liveCount: liveSet.size,
      engineOnlyDisplay: engineDisplay.display,
      engineOnlyExtra: engineDisplay.extra,
      liveOnlyDisplay: liveDisplay.display,
      liveOnlyExtra: liveDisplay.extra
    });

    if (sampleDays.length >= SAMPLE_DAY_LIMIT) {
      break;
    }
  }

  return sampleDays;
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
  const sampleDays = buildSampleDays(engineAggregation.entriesByDate, liveAggregation.entriesByDate);
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
    sampleDays
  };
};
