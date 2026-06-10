import type { TradeEntryForwardOutcome } from '../database/types';
import type { BenchmarkData, BenchmarkSeriesPoint, PortfolioValuePoint } from './backtestCharts';

const TRADING_DAYS_PER_YEAR = 252;

export const START_TIMING_WINDOWS = [
  { key: '1w', label: '1W', tradingDays: 5 },
  { key: '1m', label: '1M', tradingDays: 21 },
  { key: '3m', label: '3M', tradingDays: 63 },
  { key: '6m', label: '6M', tradingDays: 126 }
] as const;

type StartTimingWindow = (typeof START_TIMING_WINDOWS)[number];

type SeriesPoint = {
  date: string;
  value: number;
};

export type StartTimingSensitivityReturn = {
  strategy: number | null;
  spy: number | null;
  qqq: number | null;
};

export type StartTimingSensitivityPoint = {
  date: string;
  returns: Record<string, StartTimingSensitivityReturn>;
};

export type StartTimingSensitivitySummary = {
  key: string;
  label: string;
  tradingDays: number;
  sampleCount: number;
  strategyMedianReturnPercent: number | null;
  spyMedianReturnPercent: number | null;
  qqqMedianReturnPercent: number | null;
  strategyPositiveRate: number | null;
  outperformSpyRate: number | null;
};

export type StartTimingMomentumPoint = {
  date: string;
  strategy: number | null;
  spy: number | null;
  qqq: number | null;
};

export type StartTimingRegimeCard = {
  symbol: 'SPY' | 'QQQ';
  latestDate: string | null;
  return1wPercent: number | null;
  return1mPercent: number | null;
  return3mPercent: number | null;
  drawdownFromHighPercent: number | null;
  sma50DistancePercent: number | null;
  sma200DistancePercent: number | null;
  volatility20dPercent: number | null;
  above50Day: boolean | null;
  above200Day: boolean | null;
};

export type StartTimingDeploymentContext = {
  label: string;
  badge: string;
  reasons: string[];
  strategy1mReturnPercent: number | null;
  spy1mReturnPercent: number | null;
  excess1mReturnPercent: number | null;
  strategy3mReturnPercent: number | null;
  spy3mReturnPercent: number | null;
  excess3mReturnPercent: number | null;
};

export type StartTimingAnalysis = {
  hasData: boolean;
  windows: typeof START_TIMING_WINDOWS;
  sensitivityPoints: StartTimingSensitivityPoint[];
  sensitivitySummary: StartTimingSensitivitySummary[];
  momentumPoints: StartTimingMomentumPoint[];
  regimeCards: StartTimingRegimeCard[];
  entryOutcomes: TradeEntryForwardOutcome[];
  deploymentContext: StartTimingDeploymentContext;
};

export function buildStartTimingAnalysis({
  portfolioValueData,
  benchmarkData,
  entryForwardOutcomes = [],
  lookbackTradingDays = TRADING_DAYS_PER_YEAR
}: {
  portfolioValueData: PortfolioValuePoint[];
  benchmarkData: BenchmarkData;
  entryForwardOutcomes?: TradeEntryForwardOutcome[];
  lookbackTradingDays?: number;
}): StartTimingAnalysis {
  const portfolio = normalizeSeries(portfolioValueData);
  const spy = normalizeSeries(benchmarkData.spy);
  const qqq = normalizeSeries(benchmarkData.qqq);
  const lookback = Number.isFinite(lookbackTradingDays) && lookbackTradingDays > 0
    ? Math.floor(lookbackTradingDays)
    : TRADING_DAYS_PER_YEAR;

  const sensitivityPoints = buildSensitivityPoints(portfolio, spy, qqq, lookback);
  const sensitivitySummary = START_TIMING_WINDOWS.map((window) =>
    buildSensitivitySummary(sensitivityPoints, window)
  );
  const momentumPoints = buildMomentumPoints(portfolio, spy, qqq, lookback);
  const regimeCards: StartTimingRegimeCard[] = [
    buildRegimeCard('SPY', spy),
    buildRegimeCard('QQQ', qqq)
  ];
  const deploymentContext = buildDeploymentContext(portfolio, spy, regimeCards[0]);

  return {
    hasData: portfolio.length >= 2,
    windows: START_TIMING_WINDOWS,
    sensitivityPoints,
    sensitivitySummary,
    momentumPoints,
    regimeCards,
    entryOutcomes: normalizeEntryOutcomes(entryForwardOutcomes),
    deploymentContext
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
    const returns: Record<string, StartTimingSensitivityReturn> = {};

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
    .filter((value): value is number => value !== null && Number.isFinite(value));
  const spyValues = rows
    .map((row) => row.spy)
    .filter((value): value is number => value !== null && Number.isFinite(value));
  const qqqValues = rows
    .map((row) => row.qqq)
    .filter((value): value is number => value !== null && Number.isFinite(value));
  const pairedSpyRows = rows.filter(
    (row) =>
      row.strategy !== null &&
      Number.isFinite(row.strategy) &&
      row.spy !== null &&
      Number.isFinite(row.spy)
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

function buildMomentumPoints(
  portfolio: SeriesPoint[],
  spy: SeriesPoint[],
  qqq: SeriesPoint[],
  lookbackTradingDays: number
): StartTimingMomentumPoint[] {
  if (portfolio.length === 0) {
    return [];
  }

  const startIndex = Math.max(0, portfolio.length - lookbackTradingDays);
  const recentPortfolio = portfolio.slice(startIndex);
  const baseDate = recentPortfolio[0]?.date;
  if (!baseDate) {
    return [];
  }

  const strategyBase = recentPortfolio[0].value;
  const spyBase = getValueAtOrAfter(spy, baseDate);
  const qqqBase = getValueAtOrAfter(qqq, baseDate);

  return recentPortfolio.map((point) => ({
    date: point.date,
    strategy: rebase(point.value, strategyBase),
    spy: rebase(getValueOnOrBefore(spy, point.date), spyBase),
    qqq: rebase(getValueOnOrBefore(qqq, point.date), qqqBase)
  }));
}

function buildRegimeCard(symbol: 'SPY' | 'QQQ', series: SeriesPoint[]): StartTimingRegimeCard {
  const latest = series[series.length - 1] ?? null;
  if (!latest) {
    return {
      symbol,
      latestDate: null,
      return1wPercent: null,
      return1mPercent: null,
      return3mPercent: null,
      drawdownFromHighPercent: null,
      sma50DistancePercent: null,
      sma200DistancePercent: null,
      volatility20dPercent: null,
      above50Day: null,
      above200Day: null
    };
  }

  const recent = series.slice(Math.max(0, series.length - TRADING_DAYS_PER_YEAR));
  const peak = recent.reduce((max, point) => Math.max(max, point.value), latest.value);
  const sma50 = average(series.slice(Math.max(0, series.length - 50)).map((point) => point.value));
  const sma200 = average(series.slice(Math.max(0, series.length - 200)).map((point) => point.value));
  const sma50Distance = distanceFromAverage(latest.value, sma50);
  const sma200Distance = distanceFromAverage(latest.value, sma200);

  return {
    symbol,
    latestDate: latest.date,
    return1wPercent: getTrailingReturn(series, 5),
    return1mPercent: getTrailingReturn(series, 21),
    return3mPercent: getTrailingReturn(series, 63),
    drawdownFromHighPercent: peak > 0 ? ((latest.value - peak) / peak) * 100 : null,
    sma50DistancePercent: sma50Distance,
    sma200DistancePercent: sma200Distance,
    volatility20dPercent: annualizedVolatility(series, 20),
    above50Day: sma50Distance === null ? null : sma50Distance >= 0,
    above200Day: sma200Distance === null ? null : sma200Distance >= 0
  };
}

function buildDeploymentContext(
  portfolio: SeriesPoint[],
  spy: SeriesPoint[],
  spyRegime: StartTimingRegimeCard
): StartTimingDeploymentContext {
  const strategy1m = getTrailingReturn(portfolio, 21);
  const spy1m = getTrailingReturn(spy, 21);
  const strategy3m = getTrailingReturn(portfolio, 63);
  const spy3m = getTrailingReturn(spy, 63);
  const excess1m = strategy1m !== null && spy1m !== null ? strategy1m - spy1m : null;
  const excess3m = strategy3m !== null && spy3m !== null ? strategy3m - spy3m : null;

  const reasons: string[] = [];
  let cautionFlags = 0;
  let supportiveFlags = 0;

  if (portfolio.length < 64 || spy.length < 64) {
    reasons.push('Not enough recent data for a stable 3-month context.');
  }

  if (spyRegime.sma200DistancePercent !== null) {
    if (spyRegime.sma200DistancePercent < 0) {
      cautionFlags += 1;
      reasons.push('SPY is below its 200-day average.');
    } else {
      supportiveFlags += 1;
      reasons.push('SPY is above its 200-day average.');
    }
  }

  if (spyRegime.drawdownFromHighPercent !== null && spyRegime.drawdownFromHighPercent <= -10) {
    cautionFlags += 1;
    reasons.push('SPY is more than 10% below its 1-year high.');
  }

  if (excess1m !== null) {
    if (excess1m < -3) {
      cautionFlags += 1;
      reasons.push('The strategy has lagged SPY by more than 3 percentage points over 1 month.');
    } else if (excess1m >= 0) {
      supportiveFlags += 1;
      reasons.push('The strategy has kept pace with or exceeded SPY over 1 month.');
    }
  }

  if (strategy3m !== null && strategy3m > 0) {
    supportiveFlags += 1;
    reasons.push('The strategy has positive 3-month momentum.');
  }

  let label = 'Observe context';
  let badge = 'bg-secondary';
  if (cautionFlags >= 2) {
    label = 'Cautious context';
    badge = 'bg-warning text-dark';
  } else if (cautionFlags === 0 && supportiveFlags >= 2) {
    label = 'Normal context';
    badge = 'bg-success';
  }

  if (reasons.length === 0) {
    reasons.push('Recent strategy and benchmark context is mixed or incomplete.');
  }

  return {
    label,
    badge,
    reasons,
    strategy1mReturnPercent: strategy1m,
    spy1mReturnPercent: spy1m,
    excess1mReturnPercent: excess1m,
    strategy3mReturnPercent: strategy3m,
    spy3mReturnPercent: spy3m,
    excess3mReturnPercent: excess3m
  };
}

function normalizeEntryOutcomes(outcomes: TradeEntryForwardOutcome[]): TradeEntryForwardOutcome[] {
  const byKey = new Map(outcomes.map((outcome) => [outcome.windowKey, outcome]));

  return START_TIMING_WINDOWS.map((window) => {
    const existing = byKey.get(window.key);
    if (existing) {
      return {
        ...existing,
        label: existing.label || window.label,
        tradingDays: Number.isFinite(existing.tradingDays) ? existing.tradingDays : window.tradingDays,
        sampleCount: Math.max(0, Math.floor(Number(existing.sampleCount) || 0))
      };
    }

    return {
      windowKey: window.key,
      label: window.label,
      tradingDays: window.tradingDays,
      sampleCount: 0,
      medianStrategyReturnPercent: null,
      winRate: null,
      medianSpyReturnPercent: null,
      medianExcessReturnPercent: null,
      outperformSpyRate: null
    };
  });
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

function getTrailingReturn(series: SeriesPoint[], tradingDays: number): number | null {
  if (series.length <= tradingDays) {
    return null;
  }
  return getForwardReturnAtIndex(series, series.length - tradingDays - 1, tradingDays);
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

function findIndexOnOrBefore(series: SeriesPoint[], date: string): number {
  let low = 0;
  let high = series.length - 1;
  let result = -1;

  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    if (series[middle].date <= date) {
      result = middle;
      low = middle + 1;
    } else {
      high = middle - 1;
    }
  }

  return result;
}

function getValueAtOrAfter(series: SeriesPoint[], date: string): number | null {
  const index = findIndexAtOrAfter(series, date);
  return index >= 0 ? series[index].value : null;
}

function getValueOnOrBefore(series: SeriesPoint[], date: string): number | null {
  const index = findIndexOnOrBefore(series, date);
  return index >= 0 ? series[index].value : null;
}

function rebase(value: number | null, base: number | null): number | null {
  if (value === null || base === null || !Number.isFinite(value) || !Number.isFinite(base) || base <= 0) {
    return null;
  }
  return (value / base) * 100;
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

function distanceFromAverage(value: number, averageValue: number | null): number | null {
  if (averageValue === null || !Number.isFinite(averageValue) || averageValue <= 0) {
    return null;
  }
  return ((value - averageValue) / averageValue) * 100;
}

function annualizedVolatility(series: SeriesPoint[], tradingDays: number): number | null {
  if (series.length <= 2) {
    return null;
  }

  const returns: number[] = [];
  const startIndex = Math.max(1, series.length - tradingDays);
  for (let index = startIndex; index < series.length; index += 1) {
    const previous = series[index - 1];
    const current = series[index];
    if (previous.value > 0 && current.value > 0) {
      returns.push((current.value - previous.value) / previous.value);
    }
  }

  if (returns.length < 2) {
    return null;
  }

  const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const variance =
    returns.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) / (returns.length - 1);
  return Math.sqrt(variance) * Math.sqrt(TRADING_DAYS_PER_YEAR) * 100;
}
