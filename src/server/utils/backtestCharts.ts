import type { Candle } from '../../shared/types/StrategyTemplate';

export type DailySnapshotLike = {
  date: Date | string;
  cash: number;
  positionsValue: number;
  concurrentTrades?: number;
  missedTradesDueToCash?: number;
};

export type PortfolioValuePoint = {
  date: string;
  value: number;
  activeTrades?: number;
  missedTradesDueToCash?: number;
};

export type CashPercentagePoint = {
  date: string;
  cashPercentage: number;
  cash: number;
  totalValue: number;
  missedTradesDueToCash: number;
};

export type DrawdownPoint = {
  date: string;
  drawdown: number;
  peak: number;
};

export type DailyReturnDistributionPoint = {
  returnPercent: number;
  count: number;
};

export type BenchmarkSeriesPoint = {
  date: string;
  value: number;
};

export type BenchmarkData = {
  spy: BenchmarkSeriesPoint[];
  qqq: BenchmarkSeriesPoint[];
};

export type GetCandles = (
  tickers: string[],
  startDate?: Date,
  endDate?: Date
) => Promise<Record<string, Candle[]>>;

type DbLike = {
  candles: {
    getCandles: GetCandles;
  };
};

type StrategyLike = {
  parameters?: Record<string, unknown> | null;
} | null | undefined;

export function buildPortfolioValueDataFromSnapshots(dailySnapshots: DailySnapshotLike[]): PortfolioValuePoint[] {
  if (!Array.isArray(dailySnapshots)) {
    return [];
  }

  return dailySnapshots.map((snapshot) => {
    const cash = Number(snapshot.cash ?? 0);
    const positionsValue = Number(snapshot.positionsValue ?? 0);
    const totalValue = cash + positionsValue;
    const activeTrades = Number(snapshot.concurrentTrades ?? 0);
    const missedTrades = Number(snapshot.missedTradesDueToCash ?? 0);

    return {
      date: new Date(snapshot.date).toISOString().split('T')[0],
      value: Number.isFinite(totalValue) ? totalValue : 0,
      activeTrades: Number.isFinite(activeTrades) ? activeTrades : 0,
      missedTradesDueToCash: Number.isFinite(missedTrades) ? missedTrades : 0
    };
  });
}

export async function buildBenchmarkDataFromSnapshots(
  db: DbLike,
  dailySnapshots: DailySnapshotLike[],
  strategy: StrategyLike
): Promise<BenchmarkData> {
  if (!Array.isArray(dailySnapshots) || dailySnapshots.length === 0) {
    return { spy: [], qqq: [] };
  }

  const startDate = new Date(dailySnapshots[0].date);
  const endDate = new Date(dailySnapshots[dailySnapshots.length - 1].date);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return { spy: [], qqq: [] };
  }

  const initialCapital = Number(strategy?.parameters?.initialCapital) || 100000;
  const benchmarkCandles = await db.candles.getCandles(['SPY', 'QQQ'], startDate, endDate);

  const mapCandles = (candles: Candle[]): BenchmarkSeriesPoint[] => {
    if (!Array.isArray(candles) || candles.length === 0) {
      return [];
    }

    const firstPrice = Number(candles[0].close);

    return candles.map((candle, index) => {
      const date = new Date(candle.date).toISOString().split('T')[0];
      if (!Number.isFinite(firstPrice) || firstPrice === 0 || index === 0) {
        return { date, value: initialCapital };
      }

      const currentPrice = Number(candle.close);
      const multiplier =
        Number.isFinite(currentPrice) && firstPrice !== 0
          ? currentPrice / firstPrice
          : 0;
      return { date, value: initialCapital * multiplier };
    });
  };

  return {
    spy: mapCandles(benchmarkCandles['SPY']),
    qqq: mapCandles(benchmarkCandles['QQQ'])
  };
}

export function buildDrawdownDataFromPortfolio(portfolioData: PortfolioValuePoint[]): DrawdownPoint[] {
  if (!Array.isArray(portfolioData) || portfolioData.length === 0) {
    return [];
  }

  let peak = portfolioData[0].value;
  return portfolioData.map((point) => {
    if (point.value > peak) {
      peak = point.value;
    }
    const drawdown = peak !== 0 ? ((point.value - peak) / peak) * 100 : 0;
    return {
      date: point.date,
      drawdown,
      peak
    };
  });
}

export function buildDailyReturnDistribution(portfolioData: PortfolioValuePoint[]): DailyReturnDistributionPoint[] {
  if (!Array.isArray(portfolioData) || portfolioData.length < 2) {
    return [];
  }

  const sortedPortfolio = [...portfolioData].sort(
    (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
  );
  const bucketCounts = new Map<number, number>();

  for (let i = 1; i < sortedPortfolio.length; i += 1) {
    const previous = sortedPortfolio[i - 1];
    const current = sortedPortfolio[i];
    if (!Number.isFinite(previous.value) || !Number.isFinite(current.value) || previous.value === 0) {
      continue;
    }
    const changePercent = ((current.value - previous.value) / previous.value) * 100;
    const bucket = Math.round(changePercent);
    bucketCounts.set(bucket, (bucketCounts.get(bucket) || 0) + 1);
  }

  return Array.from(bucketCounts.entries())
    .map(([returnPercent, count]) => ({ returnPercent, count }))
    .sort((a, b) => a.returnPercent - b.returnPercent);
}

export function buildCashPercentageDataFromSnapshots(dailySnapshots: DailySnapshotLike[]): CashPercentagePoint[] {
  if (!Array.isArray(dailySnapshots)) {
    return [];
  }

  return dailySnapshots.map((snapshot) => {
    const cash = Number(snapshot.cash ?? 0);
    const positionsValue = Number(snapshot.positionsValue ?? 0);
    const totalValue = cash + positionsValue;
    const cashPercentage = totalValue > 0 ? (cash / totalValue) * 100 : 0;

    return {
      date: new Date(snapshot.date).toISOString().split('T')[0],
      cashPercentage: Math.round(cashPercentage * 100) / 100,
      cash,
      totalValue,
      missedTradesDueToCash: Number(snapshot.missedTradesDueToCash ?? 0)
    };
  });
}
