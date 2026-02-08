import type { StrategyPerformance } from '../../shared/types/StrategyTemplate';
import {
  computeTemplateScoreResults,
  type TemplateScoreSnapshot
} from './templateScore';

const fixedNow = new Date('2025-01-01T00:00:00Z');

const makePerformance = (overrides: Partial<StrategyPerformance> = {}): StrategyPerformance => ({
  totalTrades: 100,
  winningTrades: 50,
  losingTrades: 50,
  winRate: 0.5,
  totalReturn: 0.2,
  cagr: 0.1,
  sharpeRatio: 1,
  calmarRatio: 1,
  maxDrawdown: 0.2,
  maxDrawdownPercent: 10,
  avgTradeReturn: 0.01,
  bestTrade: 0.1,
  worstTrade: -0.05,
  totalTickers: 10,
  medianTradeDuration: 5,
  medianTradePnl: 100,
  medianTradePnlPercent: 0.01,
  medianConcurrentTrades: 1,
  avgTradeDuration: 5,
  avgTradePnl: 50,
  avgTradePnlPercent: 0.02,
  avgConcurrentTrades: 1,
  avgLosingPnl: -25,
  avgLosingPnlPercent: -0.01,
  avgWinningPnl: 75,
  avgWinningPnlPercent: 0.03,
  lastUpdated: fixedNow,
  ...overrides
});

const buildSnapshots = ({
  templateId = 'template-1',
  strategyId = 'strategy-1',
  periodMonths = 12,
  periodDays = 365,
  trainingCagr = 0.1,
  validationCagr = 0.1,
  validationDrawdownPercent = 10,
  totalTrades = 200
}: {
  templateId?: string;
  strategyId?: string;
  periodMonths?: number;
  periodDays?: number | null;
  trainingCagr?: number;
  validationCagr?: number;
  validationDrawdownPercent?: number;
  totalTrades?: number;
} = {}): TemplateScoreSnapshot[] => {
  const createdAt = new Date(fixedNow);
  const trainingPerformance = makePerformance({
    cagr: trainingCagr,
    maxDrawdownPercent: validationDrawdownPercent,
    totalTrades
  });
  const validationPerformance = makePerformance({
    cagr: validationCagr,
    maxDrawdownPercent: validationDrawdownPercent,
    totalTrades
  });

  return [
    {
      templateId,
      strategyId,
      periodMonths,
      periodDays,
      tickerScope: 'training',
      performance: trainingPerformance,
      createdAt
    },
    {
      templateId,
      strategyId,
      periodMonths,
      periodDays,
      tickerScope: 'validation',
      performance: validationPerformance,
      createdAt
    }
  ];
};

describe('template scoring', () => {
  const templateId = 'template-1';
  let dateNowSpy: jest.SpyInstance;

  beforeAll(() => {
    dateNowSpy = jest.spyOn(Date, 'now').mockReturnValue(fixedNow.getTime());
  });

  afterAll(() => {
    dateNowSpy.mockRestore();
  });

  it('increases period score as trades per year increase', () => {
    const lowTradeResults = computeTemplateScoreResults(buildSnapshots({ totalTrades: 25 }), {});
    const highTradeResults = computeTemplateScoreResults(buildSnapshots({ totalTrades: 200 }), {});

    const lowPeriod = lowTradeResults.breakdowns.get(templateId)?.periods[0];
    const highPeriod = highTradeResults.breakdowns.get(templateId)?.periods[0];

    expect(lowPeriod).toBeDefined();
    expect(highPeriod).toBeDefined();
    if (!lowPeriod || !highPeriod) {
      return;
    }

    expect(highPeriod.liquidityScore).toBeGreaterThan(lowPeriod.liquidityScore);
    expect(highPeriod.periodScore01).toBeGreaterThan(lowPeriod.periodScore01);
  });

  it('penalizes negative verify CAGR more than neutral', () => {
    const snapshots = buildSnapshots();
    const negativeResults = computeTemplateScoreResults(snapshots, {
      verificationByTemplate: new Map([[templateId, { verifyCagr: -0.2 }]])
    });
    const neutralResults = computeTemplateScoreResults(snapshots, {
      verificationByTemplate: new Map([[templateId, { verifyCagr: 0 }]])
    });

    const negativeMultiplier = negativeResults.breakdowns.get(templateId)?.verificationMultiplier;
    const neutralMultiplier = neutralResults.breakdowns.get(templateId)?.verificationMultiplier;

    expect(typeof negativeMultiplier).toBe('number');
    expect(typeof neutralMultiplier).toBe('number');
    if (typeof negativeMultiplier !== 'number' || typeof neutralMultiplier !== 'number') {
      return;
    }

    expect(negativeMultiplier).toBeLessThan(1);
    expect(negativeMultiplier).toBeLessThan(neutralMultiplier);
    expect(Math.abs(negativeMultiplier - 0.8)).toBeLessThan(Math.abs(neutralMultiplier - 0.8));
  });

  it('rewards positive verify CAGR above neutral', () => {
    const snapshots = buildSnapshots();
    const positiveResults = computeTemplateScoreResults(snapshots, {
      verificationByTemplate: new Map([[templateId, { verifyCagr: 0.2 }]])
    });
    const neutralResults = computeTemplateScoreResults(snapshots, {
      verificationByTemplate: new Map([[templateId, { verifyCagr: 0 }]])
    });

    const positiveMultiplier = positiveResults.breakdowns.get(templateId)?.verificationMultiplier;
    const neutralMultiplier = neutralResults.breakdowns.get(templateId)?.verificationMultiplier;

    expect(typeof positiveMultiplier).toBe('number');
    expect(typeof neutralMultiplier).toBe('number');
    if (typeof positiveMultiplier !== 'number' || typeof neutralMultiplier !== 'number') {
      return;
    }

    expect(positiveMultiplier).toBeGreaterThan(neutralMultiplier);
  });

  it('reduces risk score and period score with larger drawdowns', () => {
    const lowDrawdownResults = computeTemplateScoreResults(buildSnapshots({ validationDrawdownPercent: 5 }), {});
    const highDrawdownResults = computeTemplateScoreResults(buildSnapshots({ validationDrawdownPercent: 35 }), {});

    const lowPeriod = lowDrawdownResults.breakdowns.get(templateId)?.periods[0];
    const highPeriod = highDrawdownResults.breakdowns.get(templateId)?.periods[0];

    expect(lowPeriod).toBeDefined();
    expect(highPeriod).toBeDefined();
    if (!lowPeriod || !highPeriod) {
      return;
    }

    expect(lowPeriod.riskScore).toBeGreaterThan(highPeriod.riskScore);
    expect(lowPeriod.periodScore01).toBeGreaterThan(highPeriod.periodScore01);
  });

  it('keeps component averages and scores in UI bounds', () => {
    const results = computeTemplateScoreResults(buildSnapshots(), {});
    const breakdown = results.breakdowns.get(templateId);

    expect(breakdown).toBeDefined();
    if (!breakdown) {
      return;
    }

    const components = breakdown.componentAverages;
    Object.values(components).forEach(value => {
      expect(value).toBeGreaterThanOrEqual(0);
      expect(value).toBeLessThanOrEqual(1);
    });

    expect(breakdown.baseScore01).toBeGreaterThanOrEqual(0);
    expect(breakdown.baseScore01).toBeLessThanOrEqual(1);
    expect(breakdown.baseScore100).toBeGreaterThanOrEqual(0);
    expect(breakdown.baseScore100).toBeLessThanOrEqual(100);
    expect(breakdown.finalScore100).toBeGreaterThanOrEqual(0);
    expect(breakdown.finalScore100).toBeLessThanOrEqual(100);
  });
});
