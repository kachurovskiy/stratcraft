import express, { NextFunction, Request, Response } from 'express';
import { AccountSnapshot } from '../../shared/types/Account';
import { BacktestScope, Strategy, TickerInfo } from '../../shared/types/StrategyTemplate';
import { getReqUserId, getCurrentUrl, formatBacktestPeriodLabel } from './utils';

const router = express.Router();

const DEFAULT_SIGNAL_LOOKBACK_DAYS = 7;

const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

const getSnapshotBadgeMeta = (snapshot?: AccountSnapshot) => {
  if (!snapshot) {
    return { label: 'Pending', variant: 'secondary' };
  }
  switch (snapshot.status) {
    case 'ready':
      return { label: 'Live', variant: 'success' };
    case 'unsupported':
      return { label: 'Manual', variant: 'warning' };
    default:
      return { label: 'Error', variant: 'danger' };
  }
};

router.get('/', requireAuth, async (req: Request, res: Response) => {
  try {
    const availableBacktestPeriods = await req.db.backtestResults.getAvailableBacktestPeriods();
    const {
      selectedKey: selectedBacktestPeriod,
      selectedMonths: selectedBacktestPeriodMonths
    } = normalizeBacktestPeriod(req, availableBacktestPeriods);

    const userId = getReqUserId(req);

    const [
      strategiesWithPerformance,
      rawAccounts
    ] = await Promise.all([
      fetchStrategiesWithPerformance(req, userId, selectedBacktestPeriodMonths ?? undefined, 'validation'),
      req.db.accounts.getAccountsForUser(userId)
    ]);

    const strategies = strategiesWithPerformance.filter((strategy: Strategy) => {
      const performance = strategy.performance;
      const isValidationScope = performance?.tickerScope === 'validation';
      const hasBacktest = Boolean(performance?.backtestStartDate);
      return isValidationScope && hasBacktest;
    });
    const hasStrategiesAwaitingBacktests = strategiesWithPerformance.length > 0 && strategies.length === 0;

    const strategyIds = strategies.map((strategy: Strategy) => strategy.id);
    let strategySharpeMedianMap: Record<string, number | null> = {};
    let strategyCalmarMedianMap: Record<string, number | null> = {};
    let strategyCagrByPeriod: Record<string, { periodMonths: number; cagr: number }[]> = {};
    if (strategyIds.length > 0) {
      [
        strategySharpeMedianMap,
        strategyCalmarMedianMap,
        strategyCagrByPeriod
      ] = await Promise.all([
        req.db.backtestResults.getStrategySharpeMedians(strategyIds),
        req.db.backtestResults.getStrategyCalmarMedians(strategyIds),
        req.db.backtestResults.getStrategyCagrByPeriod(strategyIds, 'validation')
      ]);
    }

    const backtestPeriodOptions = buildBacktestPeriodOptions(req, availableBacktestPeriods, selectedBacktestPeriod);

    const snapshotMap: Record<string, AccountSnapshot> = await req.accountDataService.fetchSnapshots(rawAccounts);

    const accounts = rawAccounts.map((account: any) => {
      const snapshot = snapshotMap[account.id];
      const excludedTickers: string[] = Array.isArray(account.excludedTickers) ? account.excludedTickers : [];
      return {
        id: account.id,
        name: account.name,
        provider: account.provider,
        environment: account.environment,
        createdAt: account.createdAt,
        snapshot,
        snapshotBadge: getSnapshotBadgeMeta(snapshot),
        snapshotMessage: snapshot?.message ?? null,
        excludedTickers,
        excludedTickerCount: excludedTickers.length
      };
    });

    const getCagr = (strategy: Strategy) => {
      if (!strategy.performance || typeof strategy.performance.cagr !== 'number') {
        return Number.NEGATIVE_INFINITY;
      }
      return strategy.performance.cagr;
    };

    const sortedStrategies = [...strategies].sort((a: Strategy, b: Strategy) => {
      return getCagr(b) - getCagr(a);
    });

    const cagrChartData = buildCagrChartData(sortedStrategies, availableBacktestPeriods, strategyCagrByPeriod);

    const annotateStrategy = (strategy: Strategy): Strategy & {
      medianSharpeRatio: number | null;
      medianCalmarRatio: number | null;
    } => ({
      ...strategy,
      medianSharpeRatio: strategySharpeMedianMap[strategy.id] ?? null,
      medianCalmarRatio: strategyCalmarMedianMap[strategy.id] ?? null
    });

    res.render('pages/dashboard', {
      title: 'Dashboard',
      page: 'dashboard',
      user: req.user,
      strategies: sortedStrategies.map(annotateStrategy),
      accounts,
      backtestPeriodOptions,
      selectedBacktestPeriod,
      selectedBacktestPeriodMonths,
      hasStrategiesAwaitingBacktests,
      cagrChartData
    });
  } catch (error) {
    console.error('Error rendering dashboard:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load dashboard'
    });
  }
});

router.get('/signals', requireAuth, async (req: Request, res: Response) => {
  try {
    const userId = getReqUserId(req);
    const lookbackDays = DEFAULT_SIGNAL_LOOKBACK_DAYS;

    const [
      strategiesForSignals,
      tickers,
      signals
    ] = await Promise.all([
      fetchStrategiesWithPerformance(req, userId, 120, 'validation'),
      req.db.tickers.getTickers(),
      req.db.signals.getSignals(userId, lookbackDays)
    ]);

    const groupedSignals = groupSignalsByDayAndTicker(
      signals,
      strategiesForSignals,
      tickers,
      lookbackDays
    );

    res.render('pages/signals', {
      title: 'Recent Signals',
      page: 'signals',
      user: req.user,
      groupedSignals,
      lookbackDays,
      currentUrl: getCurrentUrl(req)
    });
  } catch (error) {
    console.error('Error rendering signals:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load recent signals'
    });
  }
});

function normalizeBacktestPeriod(
  req: Request,
  availablePeriods: number[]
): { selectedKey: string | null; selectedMonths: number | null } {
  if (availablePeriods.length === 0) {
    return { selectedKey: null, selectedMonths: null };
  }

  const raw = Array.isArray(req.query.backtestPeriod) ? req.query.backtestPeriod[0] : req.query.backtestPeriod;
  const parsed = typeof raw === 'string' ? parseInt(raw, 10) : NaN;
  const validMonths = Number.isFinite(parsed) && availablePeriods.includes(parsed) ? parsed : availablePeriods[0];
  const selectedKey = String(validMonths);
  return { selectedKey, selectedMonths: validMonths };
}

async function fetchStrategiesWithPerformance(
  req: Request,
  userId: number,
  periodMonths?: number,
  scope?: BacktestScope
): Promise<Strategy[]> {
  const strategies = await req.db.strategies.getStrategies(userId);

  return Promise.all(
    strategies.map(async (strategy) => {
      const performance = await req.db.backtestResults.getStoredStrategyPerformance(strategy.id, {
        periodMonths,
        tickerScope: scope
      });

      return {
        ...strategy,
        performance
      };
    })
  );
}

function buildBacktestPeriodOptions(req: Request, availablePeriods: number[], selectedKey: string | null) {
  if (availablePeriods.length === 0) {
    return [];
  }
  const baseParams = new URLSearchParams();
  Object.entries(req.query).forEach(([key, value]) => {
    if (key === 'backtestPeriod') return;
    if (Array.isArray(value)) {
      value.forEach(v => {
        if (v !== undefined && v !== null) {
          baseParams.append(key, String(v));
        }
      });
    } else if (value !== undefined && value !== null) {
      baseParams.append(key, String(value));
    }
  });

  return availablePeriods.map(periodMonths => {
    const params = new URLSearchParams(baseParams.toString());
    params.set('backtestPeriod', String(periodMonths));
    const queryString = params.toString();
    return {
      key: String(periodMonths),
      label: formatBacktestPeriodLabel(periodMonths),
      href: queryString ? `/dashboard?${queryString}` : '/dashboard',
      active: selectedKey === String(periodMonths)
    };
  });
}

function buildCagrChartData(
  strategies: Strategy[],
  availablePeriods: number[],
  cagrByStrategy: Record<string, { periodMonths: number; cagr: number }[]>
): {
  periods: number[];
  labels: string[];
  series: Array<{ id: string; name: string; values: Array<number | null> }>;
  hasData: boolean;
} {
  const skipMonths = new Set<number>([1, 3, 6]);
  const periodSet = new Set<number>();
  const normalizedPeriods = Array.isArray(availablePeriods) ? availablePeriods : [];
  normalizedPeriods.forEach(period => {
    const numeric = Number(period);
    if (Number.isFinite(numeric) && numeric > 0 && !skipMonths.has(numeric)) {
      periodSet.add(numeric);
    }
  });
  Object.values(cagrByStrategy || {}).forEach(entries => {
    entries.forEach(entry => {
      const numeric = Number(entry.periodMonths);
      if (Number.isFinite(numeric) && numeric > 0 && !skipMonths.has(numeric)) {
        periodSet.add(numeric);
      }
    });
  });

  const sortedPeriods = Array.from(periodSet).sort((a, b) => b - a);
  const labels = sortedPeriods.map(period => formatBacktestPeriodLabel(period));

  const series = strategies
    .map(strategy => {
      const entries = cagrByStrategy[strategy.id] ?? [];
      if (!Array.isArray(entries) || entries.length === 0) {
        return null;
      }

      const valueMap = new Map<number, number>();
      for (const entry of entries) {
        const periodMonths = Number(entry.periodMonths);
        const cagrValue = typeof entry.cagr === 'number' ? entry.cagr : Number(entry.cagr);
        if (Number.isFinite(periodMonths) && Number.isFinite(cagrValue)) {
          valueMap.set(periodMonths, cagrValue);
        }
      }

      const values = sortedPeriods.map(period => (valueMap.has(period) ? valueMap.get(period)! : null));
      const hasValues = values.some(value => value !== null);
      if (!hasValues) {
        return null;
      }

      return {
        id: strategy.id,
        name: strategy.name,
        values
      };
    })
    .filter((item): item is { id: string; name: string; values: Array<number | null> } => Boolean(item));

  return {
    periods: sortedPeriods,
    labels,
    series,
    hasData: series.length > 0 && sortedPeriods.length > 0
  };
}

function groupSignalsByDayAndTicker(
  signals: Array<{ date: Date; ticker: string; strategyId: string; action: 'buy' | 'sell'; confidence: number | null }>,
  strategies: Strategy[],
  tickerInfos: TickerInfo[] = [],
  lookbackWindowDays: number = DEFAULT_SIGNAL_LOOKBACK_DAYS,
) {
  const grouped: { [date: string]: { [ticker: string]: {
    ticker: string;
    date: Date;
    strategies: { id: string; name: string; label: string; action: 'buy' | 'sell'; confidence?: number | null; sharpe: number }[];
  } } } = {};

  const strategyNameMap = new Map<string, string>();
  const strategySharpeMap = new Map<string, number>();
  const tickerVolumeMap = new Map<string, number>();
  strategies.forEach(s => {
    strategyNameMap.set(s.id, s.name);
    strategySharpeMap.set(s.id, s.performance?.sharpeRatio ?? 0);
  });
  tickerInfos.forEach(info => {
    if (typeof info.volumeUsd === 'number') {
      tickerVolumeMap.set(info.symbol.toUpperCase(), info.volumeUsd);
    }
  });

  const sharpeFilterThreshold = 0.8;

  const today = new Date();
  for (let i = 0; i < lookbackWindowDays; i++) {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    const key = d.toISOString().split('T')[0];
    grouped[key] = {};
  }

  for (const sig of signals) {
    const dateKey = sig.date.toISOString().split('T')[0];
    if (!grouped[dateKey]) grouped[dateKey] = {};
    if (!grouped[dateKey][sig.ticker]) {
      grouped[dateKey][sig.ticker] = {
        ticker: sig.ticker,
        date: sig.date,
        strategies: []
      };
    }
    const name = strategyNameMap.get(sig.strategyId) || 'Unknown Strategy';
    const label = name.slice(0, 2).toUpperCase();
    const sharpe = strategySharpeMap.get(sig.strategyId) ?? 0;
    if (sharpe < sharpeFilterThreshold) {
      continue;
    }
    grouped[dateKey][sig.ticker].strategies.push({
      id: sig.strategyId,
      name,
      label,
      action: sig.action,
      confidence: sig.confidence,
      sharpe
    });
  }

  const highSharpeThreshold = 1.0;

  const result = Object.keys(grouped)
    .sort((a, b) => b.localeCompare(a))
    .map(dateKey => {
      const tickers = Object.keys(grouped[dateKey]).map(ticker => {
        const base = grouped[dateKey][ticker];
        const strategiesSorted = base.strategies.sort((x, y) => x.label.localeCompare(y.label));
        const volumeUsd = tickerVolumeMap.get(base.ticker.toUpperCase());

        const buyCount = strategiesSorted.filter(s => s.action === 'buy').length;
        const sellCount = strategiesSorted.filter(s => s.action === 'sell').length;
        const buySharpeSum = strategiesSorted.filter(s => s.action === 'buy').reduce((sum, s) => sum + (s.sharpe || 0), 0);
        const sellSharpeSum = strategiesSorted.filter(s => s.action === 'sell').reduce((sum, s) => sum + (s.sharpe || 0), 0);
        const highSharpeBuySum = strategiesSorted
          .filter(s => s.action === 'buy' && (s.sharpe || 0) >= highSharpeThreshold)
          .reduce((sum, s) => sum + (s.sharpe || 0), 0);
        const highSharpeSellSum = strategiesSorted
          .filter(s => s.action === 'sell' && (s.sharpe || 0) >= highSharpeThreshold)
          .reduce((sum, s) => sum + (s.sharpe || 0), 0);
        const voteScore = buySharpeSum - sellSharpeSum;
        const consensus: 'buy' | 'sell' | 'mixed' = voteScore > 0 ? 'buy' : voteScore < 0 ? 'sell' : 'mixed';
        const highSharpeConsensus: 'buy' | 'sell' | 'none' =
          highSharpeBuySum > highSharpeSellSum ? 'buy'
            : highSharpeSellSum > highSharpeBuySum ? 'sell'
              : 'none';

        return {
          ...base,
          strategies: strategiesSorted,
          volumeUsd,
          metrics: {
            buyCount,
            sellCount,
            buySharpeSum,
            sellSharpeSum,
            highSharpeBuySum,
            highSharpeSellSum,
            voteScore,
            consensus,
            highSharpeConsensus,
            totalSharpe: buySharpeSum + sellSharpeSum,
            activityCount: strategiesSorted.length,
            balance: Math.abs(buyCount - sellCount),
            highSharpeTotal: highSharpeBuySum + highSharpeSellSum
          }
        };
      });
      const tickersFiltered = tickers.filter(t => t.metrics.activityCount > 0);

      const tickersSorted = tickersFiltered.sort((a, b) => {
        const scoreDiff = Math.abs(b.metrics.voteScore) - Math.abs(a.metrics.voteScore);
        if (scoreDiff !== 0) return scoreDiff;
        const volumeDiff = (b.volumeUsd ?? 0) - (a.volumeUsd ?? 0);
        if (volumeDiff !== 0) return volumeDiff;
        return a.ticker.localeCompare(b.ticker);
      });

      const topBuys = tickersSorted
        .filter(t => t.metrics.consensus === 'buy')
        .sort((a, b) => {
          const scoreDiff = b.metrics.voteScore - a.metrics.voteScore;
          if (scoreDiff !== 0) return scoreDiff;
          const volumeDiff = (b.volumeUsd ?? 0) - (a.volumeUsd ?? 0);
          if (volumeDiff !== 0) return volumeDiff;
          return a.ticker.localeCompare(b.ticker);
        });
      const topSells = tickersSorted
        .filter(t => t.metrics.consensus === 'sell')
        .sort((a, b) => {
          const scoreDiff = a.metrics.voteScore - b.metrics.voteScore;
          if (scoreDiff !== 0) return scoreDiff;
          const volumeDiff = (b.volumeUsd ?? 0) - (a.volumeUsd ?? 0);
          if (volumeDiff !== 0) return volumeDiff;
          return a.ticker.localeCompare(b.ticker);
        });

      const summary = {
        totalTickers: tickersSorted.length,
        buyMajority: topBuys.length,
        sellMajority: topSells.length,
        mixed: tickersSorted.filter(t => t.metrics.consensus === 'mixed').length
      };

      const LIMIT = 100;
      const topBuysLimited = topBuys.slice(0, LIMIT);
      const topSellsLimited = topSells.slice(0, LIMIT);
      const topMixed = tickersSorted
        .filter(t => t.metrics.consensus === 'mixed')
        .sort((a, b) => {
          const bHigh = b.metrics.highSharpeTotal || 0;
          const aHigh = a.metrics.highSharpeTotal || 0;
          if (bHigh !== aHigh) return bHigh - aHigh;
          const bTot = b.metrics.totalSharpe || 0;
          const aTot = a.metrics.totalSharpe || 0;
          if (bTot !== aTot) return bTot - aTot;
          if (b.metrics.activityCount !== a.metrics.activityCount) return b.metrics.activityCount - a.metrics.activityCount;
          return a.metrics.balance - b.metrics.balance;
        });
      const topMixedLimited = topMixed.slice(0, LIMIT);

      return {
        date: dateKey,
        dateObj: new Date(dateKey),
        tickers: tickersSorted,
        topBuys,
        topSells,
        topBuysLimited,
        topSellsLimited,
        topMixed,
        topMixedLimited,
        summary
      };
    })
    .filter(day => day.tickers.length > 0);

  return result;
}

export default router;
