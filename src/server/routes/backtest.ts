import express, { NextFunction, Request, Response } from 'express';
import { BacktestParams } from '../types/Express';
import { Trade } from '../types/StrategyTemplate';
import type {
  BacktestDailySnapshot,
  EntryFillGapHistogram,
  TickerAssetRecord,
  TradeTickerStats
} from '../database/types';
import {
  buildBenchmarkDataFromSnapshots,
  buildCashPercentageDataFromSnapshots,
  buildDailyReturnDistribution,
  buildDrawdownDataFromPortfolio,
  buildPortfolioValueDataFromSnapshots,
  type PortfolioValuePoint
} from '../utils/backtestCharts';
import { BACKTEST_SCOPE_META, normalizeBacktestScope } from '../scoring/backtestComparison';
import { buildParameterContexts } from '../utils/parameters';
import { normalizeUppercaseString } from '../utils/stringNormalization';
import { getReqUserId } from './utils';

const router = express.Router();

const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

type EntryFillGapChartData = {
  labels: string[];
  counts: number[];
  total: number;
  minPercent: number;
  maxPercent: number;
  bucketSize: number;
  bucketCount: number;
};

type EntryFillGapPnlChartData = {
  labels: string[];
  totalPnl: number[];
  avgProfitPercent: Array<number | null>;
  totalTrades: number;
  minPercent: number;
  maxPercent: number;
  bucketSize: number;
  bucketCount: number;
};

type EntryFillGapBucketAggregate = {
  count: number;
  totalPnl: number;
  sumPnlPercent: number;
  pnlPercentCount: number;
};

type EntryFillGapBucketSeries = {
  labels: string[];
  buckets: EntryFillGapBucketAggregate[];
  minPercent: number;
  maxPercent: number;
  bucketSize: number;
  bucketCount: number;
};

const formatPercentLabel = (value: number, decimals: number): string => {
  if (!Number.isFinite(value)) {
    return '0%';
  }
  const rounded = Object.is(value, -0) ? 0 : value;
  const fixed = rounded.toFixed(decimals);
  const trimmed = fixed.replace(/(\.\d*?[1-9])0+$/u, '$1').replace(/\.0+$/u, '');
  return `${trimmed}%`;
};

const buildEntryFillGapBucketSeries = (histogram: EntryFillGapHistogram | null): EntryFillGapBucketSeries | null => {
  if (!histogram || histogram.bucketCount <= 0) {
    return null;
  }
  const { minPercent, maxPercent, bucketCount } = histogram;
  const bucketSize = (maxPercent - minPercent) / bucketCount;
  if (!Number.isFinite(bucketSize) || bucketSize <= 0) {
    return null;
  }

  const buckets: EntryFillGapBucketAggregate[] = Array.from({ length: bucketCount + 2 }, () => ({
    count: 0,
    totalPnl: 0,
    sumPnlPercent: 0,
    pnlPercentCount: 0
  }));
  for (const entry of histogram.buckets) {
    const index = Math.max(0, Math.min(bucketCount + 1, Math.floor(entry.bucket)));
    const bucket = buckets[index];
    bucket.count += entry.count;
    bucket.totalPnl += entry.totalPnl;
    bucket.sumPnlPercent += entry.sumPnlPercent;
    bucket.pnlPercentCount += entry.pnlPercentCount;
  }

  const decimals = bucketSize >= 1 ? 0 : bucketSize >= 0.1 ? 1 : 2;
  const labels: string[] = [];

  labels.push(`< ${formatPercentLabel(minPercent, decimals)}`);
  for (let i = 1; i <= bucketCount; i += 1) {
    const start = minPercent + (i - 1) * bucketSize;
    const end = start + bucketSize;
    labels.push(`${formatPercentLabel(start, decimals)} to ${formatPercentLabel(end, decimals)}`);
  }
  labels.push(`> ${formatPercentLabel(maxPercent, decimals)}`);

  return {
    labels,
    buckets,
    minPercent,
    maxPercent,
    bucketSize,
    bucketCount
  };
};

const buildEntryFillGapChartData = (histogram: EntryFillGapHistogram | null): EntryFillGapChartData | null => {
  const series = buildEntryFillGapBucketSeries(histogram);
  if (!series) {
    return null;
  }

  const counts = series.buckets.map((bucket) => bucket.count);
  const total = counts.reduce((sum, value) => sum + value, 0);
  if (total === 0) {
    return null;
  }

  return {
    labels: series.labels,
    counts,
    total,
    minPercent: series.minPercent,
    maxPercent: series.maxPercent,
    bucketSize: series.bucketSize,
    bucketCount: series.bucketCount
  };
};

const buildEntryFillGapPnlChartData = (histogram: EntryFillGapHistogram | null): EntryFillGapPnlChartData | null => {
  const series = buildEntryFillGapBucketSeries(histogram);
  if (!series) {
    return null;
  }

  const totalTrades = series.buckets.reduce((sum, bucket) => sum + bucket.count, 0);
  if (totalTrades === 0) {
    return null;
  }

  return {
    labels: series.labels,
    totalPnl: series.buckets.map((bucket) => bucket.totalPnl),
    avgProfitPercent: series.buckets.map((bucket) =>
      bucket.pnlPercentCount > 0 ? bucket.sumPnlPercent / bucket.pnlPercentCount : null
    ),
    totalTrades,
    minPercent: series.minPercent,
    maxPercent: series.maxPercent,
    bucketSize: series.bucketSize,
    bucketCount: series.bucketCount
  };
};

type ExposureBucketKey =
  | 'stocks'
  | 'etfs'
  | 'inverseStocks'
  | 'inverseEtfs'
  | 'leveraged2x'
  | 'leveraged3x'
  | 'commodities'
  | 'realEstate'
  | 'cash'
  | 'other';

type ExposureBucketView = {
  key: ExposureBucketKey;
  label: string;
  value: number;
  percentOfPortfolio?: number;
};

type ExposureSummaryView = {
  available: boolean;
  asOfDate: Date | null;
  totalValue?: number;
  activeTradeCount: number;
  rows: ExposureBucketView[];
  largestPositionPercent?: number;
};

type ExposureDb = {
  tickers: {
    getTickersBySymbols(symbols: string[]): Promise<TickerAssetRecord[]>;
  };
  candles: {
    getLastCloseByTickerOnOrBeforeDate(tickers: string[], endDate: Date): Promise<Record<string, number | null>>;
  };
};

const EXPOSURE_BUCKETS: Array<{ key: ExposureBucketKey; label: string }> = [
  { key: 'stocks', label: 'Stocks' },
  { key: 'etfs', label: 'ETFs' },
  { key: 'inverseStocks', label: 'Inverse Stocks' },
  { key: 'inverseEtfs', label: 'Inverse ETFs' },
  { key: 'leveraged2x', label: '2x Leveraged' },
  { key: 'leveraged3x', label: '3x Leveraged' },
  { key: 'commodities', label: 'Commodities' },
  { key: 'realEstate', label: 'Real Estate' },
  { key: 'cash', label: 'Cash' },
  { key: 'other', label: 'Other' }
];

const REAL_ESTATE_NAME_PATTERN = /\b(REIT|REAL ESTATE|REALTY)\b/i;

const getLatestDailySnapshot = (snapshots: BacktestDailySnapshot[]): BacktestDailySnapshot | null => {
  if (!Array.isArray(snapshots) || snapshots.length === 0) {
    return null;
  }

  let latest: BacktestDailySnapshot | null = null;
  let latestTimestamp = -Infinity;

  for (const snapshot of snapshots) {
    const date = snapshot.date instanceof Date ? snapshot.date : new Date(snapshot.date);
    const timestamp = date.getTime();
    if (!Number.isFinite(timestamp)) {
      continue;
    }
    if (!latest || timestamp >= latestTimestamp) {
      latest = snapshot;
      latestTimestamp = timestamp;
    }
  }

  return latest;
};

const resolveExposureBucket = (
  assetType: string | null,
  isShort: boolean,
  name?: string | null
): ExposureBucketKey => {
  if (name && REAL_ESTATE_NAME_PATTERN.test(name)) {
    return 'realEstate';
  }

  switch (assetType) {
    case 'equity':
      return isShort ? 'inverseStocks' : 'stocks';
    case 'etf':
    case 'bond_etf':
    case 'income_etf':
      return isShort ? 'inverseEtfs' : 'etfs';
    case 'leveraged_2x':
      return isShort ? 'inverseEtfs' : 'leveraged2x';
    case 'leveraged_3x':
      return isShort ? 'inverseEtfs' : 'leveraged3x';
    case 'leveraged_5x':
      return isShort ? 'inverseEtfs' : 'other';
    case 'inverse_etf':
    case 'inverse_leveraged_2x':
    case 'inverse_leveraged_3x':
    case 'inverse_leveraged_5x':
      return 'inverseEtfs';
    case 'commodity_trust':
      return 'commodities';
    default:
      return isShort ? 'inverseStocks' : 'other';
  }
};

const buildExposureSummary = async ({
  activeTrades,
  dailySnapshots,
  backtestEndDate,
  db
}: {
  activeTrades: Trade[];
  dailySnapshots: BacktestDailySnapshot[];
  backtestEndDate: Date | null;
  db: ExposureDb;
}): Promise<ExposureSummaryView> => {
  const latestSnapshot = getLatestDailySnapshot(dailySnapshots);
  const snapshotDate = latestSnapshot ? new Date(latestSnapshot.date) : null;
  const asOfDate = snapshotDate ?? backtestEndDate ?? null;

  const cashValue =
    latestSnapshot && Number.isFinite(latestSnapshot.cash) ? Number(latestSnapshot.cash) : undefined;
  const positionsValue =
    latestSnapshot && Number.isFinite(latestSnapshot.positionsValue)
      ? Number(latestSnapshot.positionsValue)
      : undefined;
  const totalValue =
    cashValue !== undefined && positionsValue !== undefined && Number.isFinite(cashValue + positionsValue)
      ? cashValue + positionsValue
      : undefined;

  const bucketTotals: Record<ExposureBucketKey, number> = {
    stocks: 0,
    etfs: 0,
    inverseStocks: 0,
    inverseEtfs: 0,
    leveraged2x: 0,
    leveraged3x: 0,
    commodities: 0,
    realEstate: 0,
    cash: 0,
    other: 0
  };

  if (cashValue !== undefined) {
    bucketTotals.cash = cashValue;
  }

  const activeTradeCount = Array.isArray(activeTrades) ? activeTrades.length : 0;
  const tradeTickers = Array.isArray(activeTrades)
    ? Array.from(
        new Set(
          activeTrades
            .map((trade) => normalizeUppercaseString(trade.ticker))
            .filter((ticker) => ticker.length > 0)
        )
      )
    : [];

  let tickerAssets: TickerAssetRecord[] = [];
  let lastCloseByTicker: Record<string, number | null> = {};

  if (tradeTickers.length > 0) {
    tickerAssets = await db.tickers.getTickersBySymbols(tradeTickers);
    if (asOfDate) {
      lastCloseByTicker = await db.candles.getLastCloseByTickerOnOrBeforeDate(tradeTickers, asOfDate);
    }
  }

  const tickerLookup = new Map(tickerAssets.map((asset) => [normalizeUppercaseString(asset.symbol), asset]));

  let largestPositionValue = 0;

  for (const trade of activeTrades) {
    const ticker = normalizeUppercaseString(trade.ticker);
    if (!ticker) {
      continue;
    }
    const asset = tickerLookup.get(ticker);
    const quantity = Number(trade.quantity);
    if (!Number.isFinite(quantity) || quantity === 0) {
      continue;
    }
    const isShort = quantity < 0;
    const latestClose = lastCloseByTicker[ticker];
    const latestCloseValue =
      typeof latestClose === 'number' && Number.isFinite(latestClose) ? latestClose : undefined;
    const fallbackPrice = Number(trade.price);
    const price =
      latestCloseValue !== undefined
        ? latestCloseValue
        : Number.isFinite(fallbackPrice)
          ? fallbackPrice
          : undefined;
    if (price === undefined || price <= 0) {
      continue;
    }
    const value = Math.abs(quantity * price);
    if (!Number.isFinite(value) || value <= 0) {
      continue;
    }

    const bucket = resolveExposureBucket(asset?.assetType ?? null, isShort, asset?.name ?? null);
    bucketTotals[bucket] += value;
    largestPositionValue = Math.max(largestPositionValue, value);
  }

  const rows = EXPOSURE_BUCKETS.map((bucket) => ({
    key: bucket.key,
    label: bucket.label,
    value: bucketTotals[bucket.key] ?? 0,
    percentOfPortfolio:
      totalValue && totalValue > 0 ? (bucketTotals[bucket.key] ?? 0) / totalValue : undefined
  }));

  const largestPositionPercent =
    totalValue && totalValue > 0 && largestPositionValue > 0 ? largestPositionValue / totalValue : undefined;

  const available = Boolean(latestSnapshot || activeTradeCount > 0);

  return {
    available,
    asOfDate,
    totalValue,
    activeTradeCount,
    rows,
    largestPositionPercent
  };
};

type PortfolioDayMove = {
  date: string;
  change: number;
  changePercent: number;
  value: number;
  activeTrades: number;
  missedTradesDueToCash: number;
};

function buildPortfolioDayMovers(
  portfolioPoints: PortfolioValuePoint[],
  limit: number = 10
): { best: PortfolioDayMove[]; worst: PortfolioDayMove[] } {
  const normalizedLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 10;
  if (!Array.isArray(portfolioPoints) || portfolioPoints.length < 2) {
    return { best: [], worst: [] };
  }

  const normalizedPoints = portfolioPoints
    .map(point => {
      const timestamp = new Date(point.date).getTime();
      return {
        date: point.date,
        value: Number(point.value),
        activeTrades: Number(point.activeTrades) || 0,
        missedTradesDueToCash: Number(point.missedTradesDueToCash) || 0,
        timestamp
      };
    })
    .filter(point => Number.isFinite(point.timestamp) && Number.isFinite(point.value))
    .sort((a, b) => a.timestamp - b.timestamp);

  if (normalizedPoints.length < 2) {
    return { best: [], worst: [] };
  }

  const dayMoves: PortfolioDayMove[] = [];

  for (let i = 1; i < normalizedPoints.length; i += 1) {
    const previous = normalizedPoints[i - 1];
    const current = normalizedPoints[i];
    const change = current.value - previous.value;
    const changePercent = previous.value !== 0 ? (change / previous.value) * 100 : 0;

    dayMoves.push({
      date: current.date,
      change,
      changePercent,
      value: current.value,
      activeTrades: current.activeTrades,
      missedTradesDueToCash: current.missedTradesDueToCash
    });
  }

  const bestDays = dayMoves
    .filter(move => move.change > 0)
    .sort((a, b) => {
      if (b.changePercent !== a.changePercent) {
        return b.changePercent - a.changePercent;
      }
      if (b.change !== a.change) {
        return b.change - a.change;
      }
      return new Date(b.date).getTime() - new Date(a.date).getTime();
    })
    .slice(0, normalizedLimit);

  const worstDays = dayMoves
    .filter(move => move.change < 0)
    .sort((a, b) => {
      if (a.changePercent !== b.changePercent) {
        return a.changePercent - b.changePercent;
      }
      if (a.change !== b.change) {
        return a.change - b.change;
      }
      return new Date(a.date).getTime() - new Date(b.date).getTime();
    })
    .slice(0, normalizedLimit);

  return { best: bestDays, worst: worstDays };
}

// Backtest page for a specific backtest result
router.get<BacktestParams>('/backtests/:backtestId', requireAuth, async (req, res) => {
  try {
    const { backtestId } = req.params;
    const userId = getReqUserId(req);
    const targetBacktest = await req.db.backtestResults.getBacktestResultById(backtestId, userId);

    if (!targetBacktest) {
      return res.status(404).render('pages/error', {
        title: 'Backtest Not Found',
        error: `Backtest ${backtestId} not found`
      });
    }

    const strategy = await req.db.strategies.getStrategy(targetBacktest.strategyId, userId);

    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${targetBacktest.strategyId} not found`
      });
    }

    const template = req.strategyRegistry.getTemplate(strategy.templateId);
    const { parameterSummaries, extraParameters } = buildParameterContexts(template, strategy.parameters);

    const bestTrades = await req.db.trades.getBestTradesByPnlPercent(strategy.id, userId, 20, targetBacktest.id);
    const worstTrades = await req.db.trades.getWorstTradesByPnlPercent(strategy.id, userId, 20, targetBacktest.id);

    const entryFillGapHistogram = await req.db.trades.getEntryFillGapHistogramForBacktest(
      targetBacktest.id,
      userId
    );
    const entryFillGapChartData = buildEntryFillGapChartData(entryFillGapHistogram);
    const entryFillGapPnlChartData = buildEntryFillGapPnlChartData(entryFillGapHistogram);

    const tradeTickerStatsAll: TradeTickerStats[] = await req.db.trades.getTickerTradeStatsForBacktest(targetBacktest.id, userId);
    const tradeVolumeSegments = await req.db.trades.getTradeVolumeSegmentStatsForBacktest(targetBacktest.id, userId);
    const volumeSegmentStats = tradeVolumeSegments
      .map((segment) => {
        const totalBuyCost = Math.abs(segment.totalBuyCost);
        const avgReturnPercent = totalBuyCost > 0 ? (segment.totalPnl / totalBuyCost) * 100 : 0;
        return {
          bucketLabel: segment.bucketLabel,
          bucketOrder: segment.bucketOrder,
          minVolumeUsd: segment.minVolumeUsd,
          maxVolumeUsd: segment.maxVolumeUsd,
          tradeCount: segment.tradeCount,
          totalPnl: segment.totalPnl,
          totalBuyCost,
          avgReturnPercent
        };
      })
      .filter((segment) => Number.isFinite(segment.minVolumeUsd) && Number.isFinite(segment.bucketOrder))
      .sort((a, b) => a.bucketOrder - b.bucketOrder);
    const tradesPageUrl = `/backtests/${targetBacktest.id}/trades`;
    const tradeDateInsightsUrl = `/backtests/${targetBacktest.id}/trades/date-insights`;

    const tickerScope = normalizeBacktestScope(targetBacktest.tickerScope);
    const scopeMeta = BACKTEST_SCOPE_META[tickerScope];
    const periodMonthsValue = Number(targetBacktest.periodMonths);
    const periodMonths = Number.isFinite(periodMonthsValue) ? periodMonthsValue : null;

    const basePerformance = await req.db.backtestResults.getStoredStrategyPerformance(strategy.id, {
      periodMonths: periodMonths ?? undefined,
      tickerScope
    });
    const performance = {
      ...basePerformance,
      ...(targetBacktest.performance ?? {})
    };
    performance.backtestStartDate = targetBacktest.startDate ?? performance.backtestStartDate;
    performance.backtestEndDate = targetBacktest.endDate ?? performance.backtestEndDate;
    performance.lastUpdated = targetBacktest.createdAt ?? performance.lastUpdated;
    performance.tickerScope = tickerScope;
    performance.backtestId = targetBacktest.id ?? performance.backtestId;

    const dailySnapshots = Array.isArray(targetBacktest.dailySnapshots) ? targetBacktest.dailySnapshots : [];
    const backtestInitialCapital = Number(targetBacktest.initialCapital);
    const portfolioValueData = buildPortfolioValueDataFromSnapshots(dailySnapshots);
    const { best: bestPortfolioDays, worst: worstPortfolioDays } = buildPortfolioDayMovers(
      portfolioValueData,
      10
    );
    const benchmarkData = await buildBenchmarkDataFromSnapshots(
      req.db,
      dailySnapshots,
      backtestInitialCapital
    );
    const drawdownData = buildDrawdownDataFromPortfolio(portfolioValueData);
    const dailyReturnDistributionData = buildDailyReturnDistribution(portfolioValueData);
    const cashPercentageData = buildCashPercentageDataFromSnapshots(dailySnapshots);
    const activeTrades = await req.db.trades.getTrades(
      strategy.id,
      undefined,
      undefined,
      undefined,
      'active',
      targetBacktest.id,
      userId
    );
    const exposureSummary = await buildExposureSummary({
      activeTrades,
      dailySnapshots,
      backtestEndDate: targetBacktest.endDate ?? null,
      db: req.db
    });

    const { success, error } = req.query;
    res.render('pages/backtest', {
      title: `${strategy.name} - Strategy Details`,
      page: 'dashboard',
      user: req.user,
      strategy: {
        ...strategy,
        performance
      },
      template,
      bestTrades,
      worstTrades,
      entryFillGapChartData,
      entryFillGapPnlChartData,
      portfolioValueData,
      bestPortfolioDays,
      worstPortfolioDays,
      benchmarkData,
      drawdownData,
      dailyReturnDistributionData,
      tradeTickerStats: tradeTickerStatsAll,
      volumeSegmentStats,
      tradesPageUrl,
      tradeDateInsightsUrl,
      cashPercentageData,
      exposureSummary,
      backtestInitialCapital,
      parameterSummaries,
      parameterSummariesCount: parameterSummaries.length,
      extraParameters,
      extraParameterCount: extraParameters.length,
      success,
      error,
      selectedBacktestScopeLabel: scopeMeta.label,
      selectedBacktestScopeBadgeVariant: scopeMeta.badge
    });
  } catch (error) {
    console.error('Error rendering strategy detail for specific backtest period:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load strategy details'
    });
  }
});

export default router;
