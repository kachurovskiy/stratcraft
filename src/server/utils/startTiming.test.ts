import { buildStartTimingAnalysis } from './startTiming';
import type { BenchmarkData, PortfolioValuePoint } from './backtestCharts';

function makeDate(index: number): string {
  const date = new Date(Date.UTC(2024, 0, 1 + index));
  return date.toISOString().slice(0, 10);
}

function makePortfolio(values: number[], startIndex = 0): PortfolioValuePoint[] {
  return values.map((value, index) => ({
    date: makeDate(startIndex + index),
    value,
    activeTrades: 0,
    missedTradesDueToCash: 0
  }));
}

function makeBenchmark(values: number[], startIndex = 0): BenchmarkData {
  const series = values.map((value, index) => ({
    date: makeDate(startIndex + index),
    value
  }));
  return {
    spy: series,
    qqq: series.map((point) => ({ ...point, value: point.value * 1.1 }))
  };
}

function makeSample(startIndex: number, portfolioValues: number[], benchmarkValues: number[]) {
  return {
    startDate: makeDate(startIndex),
    portfolioValueData: makePortfolio(portfolioValues, startIndex),
    benchmarkData: makeBenchmark(benchmarkValues, startIndex)
  };
}

describe('buildStartTimingAnalysis', () => {
  it('builds forward start-date returns with trading-day horizons', () => {
    const portfolio = makePortfolio(Array.from({ length: 30 }, (_, index) => 100 + index));
    const benchmark = makeBenchmark(Array.from({ length: 30 }, (_, index) => 100 + index * 0.5));

    const analysis = buildStartTimingAnalysis({
      samples: [{
        startDate: makeDate(0),
        portfolioValueData: portfolio,
        benchmarkData: benchmark
      }]
    });

    expect(analysis.hasData).toBe(true);
    expect(analysis.windows.map((window) => window.key)).toEqual(['1w', '1m', '3m']);
    expect(analysis.benchmarkChartData.spy).toHaveLength(30);
    expect(analysis.benchmarkChartData.spy[0]).toEqual({ date: makeDate(0), value: 100 });
    expect(analysis.sensitivityPoints).toHaveLength(1);
    expect(analysis.sensitivityPoints[0].returns['1w'].strategy).toBeCloseTo(5);
    expect(analysis.sensitivityPoints[0].returns['1w'].spy).toBeCloseTo(2.5);
    expect(analysis.sensitivityPoints[0].returns['1m'].strategy).toBeCloseTo(21);
    expect(analysis.sensitivitySummary.find((row) => row.key === '1w')?.sampleCount).toBe(1);
    expect(analysis.sensitivitySummary.find((row) => row.key === '1m')?.sampleCount).toBe(1);
    expect(analysis.sensitivitySummary.find((row) => row.key === '3m')?.sampleCount).toBe(0);
  });

  it('groups strategy returns by SPY and QQQ forward market state', () => {
    const analysis = buildStartTimingAnalysis({
      samples: [
        makeSample(0, [100, 100, 100, 100, 100, 110], [100, 100, 100, 100, 100, 104]),
        makeSample(10, [100, 100, 100, 100, 100, 95], [100, 100, 100, 100, 100, 97]),
        makeSample(20, [100, 100, 100, 100, 100, 101], [100, 100, 100, 100, 100, 100.5])
      ]
    });

    const spyUp = analysis.marketStateSummary.find((row) =>
      row.windowKey === '1w' && row.benchmark === 'SPY' && row.state === 'up'
    );
    const spyDown = analysis.marketStateSummary.find((row) =>
      row.windowKey === '1w' && row.benchmark === 'SPY' && row.state === 'down'
    );
    const spySideways = analysis.marketStateSummary.find((row) =>
      row.windowKey === '1w' && row.benchmark === 'SPY' && row.state === 'sideways'
    );
    const qqqUp = analysis.marketStateSummary.find((row) =>
      row.windowKey === '1w' && row.benchmark === 'QQQ' && row.state === 'up'
    );

    expect(spyUp?.sampleCount).toBe(1);
    expect(spyUp?.strategyMedianReturnPercent).toBeCloseTo(10);
    expect(spyDown?.sampleCount).toBe(1);
    expect(spyDown?.strategyMedianReturnPercent).toBeCloseTo(-5);
    expect(spySideways?.sampleCount).toBe(1);
    expect(spySideways?.strategyMedianReturnPercent).toBeCloseTo(1);
    expect(qqqUp?.sampleCount).toBe(1);
  });

  it('calculates strategy and benchmark forward-return correlations', () => {
    const analysis = buildStartTimingAnalysis({
      samples: [
        makeSample(0, [100, 100, 100, 100, 100, 110], [100, 100, 100, 100, 100, 104]),
        makeSample(10, [100, 100, 100, 100, 100, 95], [100, 100, 100, 100, 100, 97]),
        makeSample(20, [100, 100, 100, 100, 100, 101], [100, 100, 100, 100, 100, 100.5])
      ]
    });

    const spyCorrelation = analysis.marketCorrelations.find((row) =>
      row.windowKey === '1w' && row.benchmark === 'SPY'
    );

    expect(spyCorrelation?.sampleCount).toBe(3);
    expect(spyCorrelation?.correlation).toBeGreaterThan(0.95);
    expect(spyCorrelation?.rSquared).toBeGreaterThan(0.9);
  });
});
