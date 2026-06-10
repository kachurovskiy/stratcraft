import { buildStartTimingAnalysis } from './startTiming';
import type { BenchmarkData, PortfolioValuePoint } from './backtestCharts';
import type { TradeEntryForwardOutcome } from '../database/types';

function makeDate(index: number): string {
  const date = new Date(Date.UTC(2024, 0, 1 + index));
  return date.toISOString().slice(0, 10);
}

function makePortfolio(values: number[]): PortfolioValuePoint[] {
  return values.map((value, index) => ({
    date: makeDate(index),
    value,
    activeTrades: 0,
    missedTradesDueToCash: 0
  }));
}

function makeBenchmark(values: number[]): BenchmarkData {
  const series = values.map((value, index) => ({
    date: makeDate(index),
    value
  }));
  return {
    spy: series,
    qqq: series.map((point) => ({ ...point, value: point.value * 1.1 }))
  };
}

describe('buildStartTimingAnalysis', () => {
  it('builds forward start-date returns with trading-day horizons', () => {
    const portfolio = makePortfolio(Array.from({ length: 30 }, (_, index) => 100 + index));
    const benchmark = makeBenchmark(Array.from({ length: 30 }, (_, index) => 100 + index * 0.5));

    const analysis = buildStartTimingAnalysis({
      portfolioValueData: portfolio,
      benchmarkData: benchmark,
      lookbackTradingDays: 30
    });

    expect(analysis.hasData).toBe(true);
    expect(analysis.sensitivityPoints).toHaveLength(30);
    expect(analysis.sensitivityPoints[0].returns['1w'].strategy).toBeCloseTo(5);
    expect(analysis.sensitivityPoints[0].returns['1w'].spy).toBeCloseTo(2.5);
    expect(analysis.sensitivityPoints[29].returns['1w'].strategy).toBeNull();
    expect(analysis.sensitivitySummary.find((row) => row.key === '1w')?.sampleCount).toBe(25);
    expect(analysis.sensitivitySummary.find((row) => row.key === '1m')?.sampleCount).toBe(9);
  });

  it('builds market context from recent strategy and SPY momentum', () => {
    const portfolio = makePortfolio(Array.from({ length: 220 }, (_, index) => 100 + index));
    const benchmark = makeBenchmark(Array.from({ length: 220 }, (_, index) => 100 + index));

    const analysis = buildStartTimingAnalysis({
      portfolioValueData: portfolio,
      benchmarkData: benchmark,
      lookbackTradingDays: 252
    });

    const spy = analysis.regimeCards.find((card) => card.symbol === 'SPY');
    expect(spy?.above50Day).toBe(true);
    expect(spy?.above200Day).toBe(true);
    expect(analysis.deploymentContext.label).toBe('Normal context');
    expect(analysis.deploymentContext.excess1mReturnPercent).toBeCloseTo(0);
  });

  it('normalizes missing trade entry outcome windows', () => {
    const outcomes: TradeEntryForwardOutcome[] = [
      {
        windowKey: '1w',
        label: '1W',
        tradingDays: 5,
        sampleCount: 12,
        medianStrategyReturnPercent: 1.5,
        winRate: 0.58,
        medianSpyReturnPercent: 0.8,
        medianExcessReturnPercent: 0.7,
        outperformSpyRate: 0.6
      }
    ];

    const analysis = buildStartTimingAnalysis({
      portfolioValueData: makePortfolio([100, 101, 102]),
      benchmarkData: makeBenchmark([100, 100.5, 101]),
      entryForwardOutcomes: outcomes
    });

    expect(analysis.entryOutcomes).toHaveLength(4);
    expect(analysis.entryOutcomes[0].sampleCount).toBe(12);
    expect(analysis.entryOutcomes.find((row) => row.windowKey === '6m')?.sampleCount).toBe(0);
  });
});
