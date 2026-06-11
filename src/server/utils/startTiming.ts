import type { BenchmarkData, BenchmarkSeriesPoint, PortfolioValuePoint } from './backtestCharts';

export const START_TIMING_WINDOWS = [
  { key: '1w', label: '1W', tradingDays: 5, sidewaysThresholdPercent: 1 },
  { key: '1m', label: '1M', tradingDays: 21, sidewaysThresholdPercent: 2 },
  { key: '3m', label: '3M', tradingDays: 63, sidewaysThresholdPercent: 4 }
] as const;

const BENCHMARKS = [
  { key: 'spy', symbol: 'SPY' },
  { key: 'qqq', symbol: 'QQQ' }
] as const;

const MARKET_STATES = [
  { key: 'down', label: 'Down', badge: 'bg-danger' },
  { key: 'sideways', label: 'Sideways', badge: 'bg-secondary' },
  { key: 'up', label: 'Up', badge: 'bg-success' }
] as const;

type StartTimingWindow = (typeof START_TIMING_WINDOWS)[number];
type StartTimingWindowKey = StartTimingWindow['key'];
type BenchmarkConfig = (typeof BENCHMARKS)[number];
type BenchmarkReturnKey = BenchmarkConfig['key'];
type BenchmarkSymbol = BenchmarkConfig['symbol'];
type MarketState = (typeof MARKET_STATES)[number];
type MarketStateKey = MarketState['key'];

type SeriesPoint = {
  date: string;
  value: number;
};

type ReturnPair = {
  date: string;
  strategy: number;
  benchmark: number;
};

export type StartTimingSensitivityReturn = {
  strategy: number | null;
  spy: number | null;
  qqq: number | null;
};

export type StartTimingSensitivityPoint = {
  date: string;
  returns: Record<StartTimingWindowKey, StartTimingSensitivityReturn>;
};

export type StartTimingSensitivitySummary = {
  key: StartTimingWindowKey;
  label: string;
  tradingDays: number;
  sampleCount: number;
  strategyMedianReturnPercent: number | null;
  spyMedianReturnPercent: number | null;
  qqqMedianReturnPercent: number | null;
  strategyPositiveRate: number | null;
  outperformSpyRate: number | null;
};

export type StartTimingMarketCorrelation = {
  benchmark: BenchmarkSymbol;
  windowKey: StartTimingWindowKey;
  windowLabel: string;
  sampleCount: number;
  correlation: number | null;
  rSquared: number | null;
  strategyMedianReturnPercent: number | null;
  benchmarkMedianReturnPercent: number | null;
};

export type StartTimingMarketStateSummary = {
  benchmark: BenchmarkSymbol;
  windowKey: StartTimingWindowKey;
  windowLabel: string;
  state: MarketStateKey;
  stateLabel: string;
  stateBadge: string;
  thresholdPercent: number;
  sampleCount: number;
  strategyMedianReturnPercent: number | null;
  strategyAverageReturnPercent: number | null;
  benchmarkMedianReturnPercent: number | null;
  strategyPositiveRate: number | null;
  outperformBenchmarkRate: number | null;
};

export type StartTimingAnalysis = {
  hasData: boolean;
  windows: typeof START_TIMING_WINDOWS;
  sensitivityPoints: StartTimingSensitivityPoint[];
  sensitivitySummary: StartTimingSensitivitySummary[];
  marketCorrelations: StartTimingMarketCorrelation[];
  marketStateSummary: StartTimingMarketStateSummary[];
};

export function buildStartTimingAnalysis({
  portfolioValueData,
  benchmarkData,
  lookbackTradingDays
}: {
  portfolioValueData: PortfolioValuePoint[];
  benchmarkData: BenchmarkData;
  lookbackTradingDays?: number;
}): StartTimingAnalysis {
  const portfolio = normalizeSeries(portfolioValueData);
  const spy = normalizeSeries(benchmarkData.spy);
  const qqq = normalizeSeries(benchmarkData.qqq);
  const lookback =
    Number.isFinite(lookbackTradingDays) && Number(lookbackTradingDays) > 0
      ? Math.floor(Number(lookbackTradingDays))
      : portfolio.length;

  const sensitivityPoints = buildSensitivityPoints(portfolio, spy, qqq, lookback);
  const sensitivitySummary = START_TIMING_WINDOWS.map((window) =>
    buildSensitivitySummary(sensitivityPoints, window)
  );
  const marketCorrelations = START_TIMING_WINDOWS.flatMap((window) =>
    BENCHMARKS.map((benchmark) => buildMarketCorrelation(sensitivityPoints, window, benchmark))
  );
  const marketStateSummary = START_TIMING_WINDOWS.flatMap((window) =>
    BENCHMARKS.flatMap((benchmark) =>
      MARKET_STATES.map((state) => buildMarketStateSummary(sensitivityPoints, window, benchmark, state))
    )
  );

  return {
    hasData: sensitivitySummary.some((summary) => summary.sampleCount > 0),
    windows: START_TIMING_WINDOWS,
    sensitivityPoints,
    sensitivitySummary,
    marketCorrelations,
    marketStateSummary
  };
}

function normalizeSeries(points: Array<PortfolioValuePoint | BenchmarkSeriesPoint>): SeriesPoint[] {
  const byDate = new Map<string, number>();

  for (const point of points) {
    const date = toDateKey(point.date);
    const value = Number(point.value);
    if (!date || !Number.isFinite(value) || value <= 0) {
      continue;
    }
    byDate.set(date, value);
  }

  return Array.from(byDate.entries())
    .map(([date, value]) => ({ date, value }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

function toDateKey(value: unknown): string | null {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
      return trimmed;
    }
    const parsed = new Date(trimmed);
    return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 10);
  }
  return null;
}

function buildSensitivityPoints(
  portfolio: SeriesPoint[],
  spy: SeriesPoint[],
  qqq: SeriesPoint[],
  lookbackTradingDays: number
): StartTimingSensitivityPoint[] {
  if (portfolio.length === 0) {
    return [];
  }

  const startIndex = Math.max(0, portfolio.length - lookbackTradingDays);
  const points: StartTimingSensitivityPoint[] = [];

  for (let index = startIndex; index < portfolio.length; index += 1) {
    const point = portfolio[index];
    const returns = {} as Record<StartTimingWindowKey, StartTimingSensitivityReturn>;

    for (const window of START_TIMING_WINDOWS) {
      returns[window.key] = {
        strategy: getForwardReturnAtIndex(portfolio, index, window.tradingDays),
        spy: getForwardReturnFromDate(spy, point.date, window.tradingDays),
        qqq: getForwardReturnFromDate(qqq, point.date, window.tradingDays)
      };
    }

    points.push({ date: point.date, returns });
  }

  return points;
}

function buildSensitivitySummary(
  points: StartTimingSensitivityPoint[],
  window: StartTimingWindow
): StartTimingSensitivitySummary {
  const rows = points.map((point) => point.returns[window.key]).filter(Boolean);
  const strategyValues = rows
    .map((row) => row.strategy)
    .filter(isFiniteNumber);
  const spyValues = rows
    .map((row) => row.spy)
    .filter(isFiniteNumber);
  const qqqValues = rows
    .map((row) => row.qqq)
    .filter(isFiniteNumber);
  const pairedSpyRows = rows.filter(
    (row) => isFiniteNumber(row.strategy) && isFiniteNumber(row.spy)
  );

  return {
    key: window.key,
    label: window.label,
    tradingDays: window.tradingDays,
    sampleCount: strategyValues.length,
    strategyMedianReturnPercent: median(strategyValues),
    spyMedianReturnPercent: median(spyValues),
    qqqMedianReturnPercent: median(qqqValues),
    strategyPositiveRate: ratio(strategyValues.filter((value) => value > 0).length, strategyValues.length),
    outperformSpyRate: ratio(
      pairedSpyRows.filter((row) => (row.strategy ?? 0) > (row.spy ?? 0)).length,
      pairedSpyRows.length
    )
  };
}

function buildMarketCorrelation(
  points: StartTimingSensitivityPoint[],
  window: StartTimingWindow,
  benchmark: BenchmarkConfig
): StartTimingMarketCorrelation {
  const pairs = getReturnPairs(points, window, benchmark.key);
  const correlation = pearsonCorrelation(pairs);

  return {
    benchmark: benchmark.symbol,
    windowKey: window.key,
    windowLabel: window.label,
    sampleCount: pairs.length,
    correlation,
    rSquared: correlation === null ? null : correlation * correlation,
    strategyMedianReturnPercent: median(pairs.map((pair) => pair.strategy)),
    benchmarkMedianReturnPercent: median(pairs.map((pair) => pair.benchmark))
  };
}

function buildMarketStateSummary(
  points: StartTimingSensitivityPoint[],
  window: StartTimingWindow,
  benchmark: BenchmarkConfig,
  state: MarketState
): StartTimingMarketStateSummary {
  const pairs = getReturnPairs(points, window, benchmark.key)
    .filter((pair) => getMarketState(pair.benchmark, window.sidewaysThresholdPercent) === state.key);
  const strategyValues = pairs.map((pair) => pair.strategy);
  const benchmarkValues = pairs.map((pair) => pair.benchmark);

  return {
    benchmark: benchmark.symbol,
    windowKey: window.key,
    windowLabel: window.label,
    state: state.key,
    stateLabel: state.label,
    stateBadge: state.badge,
    thresholdPercent: window.sidewaysThresholdPercent,
    sampleCount: pairs.length,
    strategyMedianReturnPercent: median(strategyValues),
    strategyAverageReturnPercent: average(strategyValues),
    benchmarkMedianReturnPercent: median(benchmarkValues),
    strategyPositiveRate: ratio(strategyValues.filter((value) => value > 0).length, strategyValues.length),
    outperformBenchmarkRate: ratio(
      pairs.filter((pair) => pair.strategy > pair.benchmark).length,
      pairs.length
    )
  };
}

function getReturnPairs(
  points: StartTimingSensitivityPoint[],
  window: StartTimingWindow,
  benchmarkKey: BenchmarkReturnKey
): ReturnPair[] {
  const pairs: ReturnPair[] = [];

  for (const point of points) {
    const returns = point.returns[window.key];
    const strategy = returns?.strategy;
    const benchmark = returns?.[benchmarkKey];
    if (isFiniteNumber(strategy) && isFiniteNumber(benchmark)) {
      pairs.push({
        date: point.date,
        strategy,
        benchmark
      });
    }
  }

  return pairs;
}

function getMarketState(returnPercent: number, sidewaysThresholdPercent: number): MarketStateKey {
  if (returnPercent < -sidewaysThresholdPercent) {
    return 'down';
  }
  if (returnPercent > sidewaysThresholdPercent) {
    return 'up';
  }
  return 'sideways';
}

function getForwardReturnAtIndex(series: SeriesPoint[], startIndex: number, tradingDays: number): number | null {
  const start = series[startIndex];
  const end = series[startIndex + tradingDays];
  if (!start || !end || start.value <= 0) {
    return null;
  }
  return ((end.value - start.value) / start.value) * 100;
}

function getForwardReturnFromDate(series: SeriesPoint[], date: string, tradingDays: number): number | null {
  const startIndex = findIndexAtOrAfter(series, date);
  if (startIndex === -1) {
    return null;
  }
  return getForwardReturnAtIndex(series, startIndex, tradingDays);
}

function findIndexAtOrAfter(series: SeriesPoint[], date: string): number {
  let low = 0;
  let high = series.length - 1;
  let result = -1;

  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    if (series[middle].date >= date) {
      result = middle;
      high = middle - 1;
    } else {
      low = middle + 1;
    }
  }

  return result;
}

function median(values: number[]): number | null {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (sorted.length === 0) {
    return null;
  }
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle];
  }
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function average(values: number[]): number | null {
  const finiteValues = values.filter(Number.isFinite);
  if (finiteValues.length === 0) {
    return null;
  }
  return finiteValues.reduce((sum, value) => sum + value, 0) / finiteValues.length;
}

function ratio(numerator: number, denominator: number): number | null {
  return denominator > 0 ? numerator / denominator : null;
}

function pearsonCorrelation(pairs: ReturnPair[]): number | null {
  if (pairs.length < 2) {
    return null;
  }

  const strategyMean = average(pairs.map((pair) => pair.strategy));
  const benchmarkMean = average(pairs.map((pair) => pair.benchmark));
  if (strategyMean === null || benchmarkMean === null) {
    return null;
  }

  let covariance = 0;
  let strategyVariance = 0;
  let benchmarkVariance = 0;

  for (const pair of pairs) {
    const strategyDelta = pair.strategy - strategyMean;
    const benchmarkDelta = pair.benchmark - benchmarkMean;
    covariance += strategyDelta * benchmarkDelta;
    strategyVariance += strategyDelta * strategyDelta;
    benchmarkVariance += benchmarkDelta * benchmarkDelta;
  }

  const denominator = Math.sqrt(strategyVariance * benchmarkVariance);
  return denominator > 0 ? covariance / denominator : null;
}

function isFiniteNumber(value: number | null | undefined): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}
