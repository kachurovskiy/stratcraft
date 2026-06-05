import { createDefaultSettingsValue } from '../settings/defaults';
import type { StrategyPerformance } from '../types/StrategyTemplate';
import type { TemplateScoreSnapshot } from './templateScore';
import { computeTemplateScores } from './templateScore';

const createPerformance = (overrides: Partial<StrategyPerformance> = {}): StrategyPerformance => ({
  totalTrades: 1,
  winningTrades: 1,
  losingTrades: 0,
  winRate: 1,
  totalReturn: 0.25,
  cagr: 0.2,
  sharpeRatio: 1.4,
  calmarRatio: 1.1,
  maxDrawdown: 10,
  maxDrawdownPercent: 10,
  avgTradeReturn: 0.25,
  bestTrade: 0.25,
  worstTrade: 0.25,
  totalTickers: 5,
  medianTradeDuration: 10,
  medianTradePnl: 100,
  medianTradePnlPercent: 10,
  medianConcurrentTrades: 1,
  avgTradeDuration: 10,
  avgTradePnl: 100,
  avgTradePnlPercent: 10,
  avgConcurrentTrades: 1,
  avgLosingPnl: 0,
  avgLosingPnlPercent: 0,
  avgWinningPnl: 100,
  avgWinningPnlPercent: 10,
  lastUpdated: new Date('2026-01-01T00:00:00.000Z'),
  ...(overrides ?? {})
});

const createSnapshots = (): TemplateScoreSnapshot[] => [
  {
    templateId: 'template-1',
    strategyId: 'strategy-1',
    periodMonths: 12,
    periodDays: 365,
    tickerScope: 'training',
    performance: createPerformance({ cagr: 0.3, totalTrades: 12 }),
    createdAt: new Date('2026-01-01T00:00:00.000Z')
  },
  {
    templateId: 'template-1',
    strategyId: 'strategy-1',
    periodMonths: 12,
    periodDays: 365,
    tickerScope: 'validation',
    performance: createPerformance({ cagr: 0.2, totalTrades: 1 }),
    createdAt: new Date('2026-01-01T00:00:00.000Z')
  }
];

describe('templateScore settings integration', () => {
  test('uses the normalized settings snapshot from SettingsRepo.value', async () => {
    const baselineSettings = createDefaultSettingsValue();
    const relaxedLiquiditySettings = createDefaultSettingsValue();
    relaxedLiquiditySettings.templateScoring.tradeWeight = 0;

    const baseline = await computeTemplateScores(createSnapshots(), {
      settingsValue: baselineSettings.templateScoring
    });
    const relaxed = await computeTemplateScores(createSnapshots(), {
      settingsValue: relaxedLiquiditySettings.templateScoring
    });

    expect(relaxed.scores.get('template-1') ?? 0).toBeGreaterThan(baseline.scores.get('template-1') ?? 0);
  });

  test('ignores all-ticker snapshots when pairing training and validation periods', async () => {
    const settings = createDefaultSettingsValue();
    const result = await computeTemplateScores([
      {
        templateId: 'template-1',
        strategyId: 'strategy-1',
        periodMonths: 12,
        periodDays: 365,
        tickerScope: 'validation',
        performance: createPerformance({ cagr: 0.2, totalTrades: 12 }),
        createdAt: new Date('2026-01-01T00:00:00.000Z')
      },
      {
        templateId: 'template-1',
        strategyId: 'strategy-1',
        periodMonths: 12,
        periodDays: 365,
        tickerScope: 'all',
        performance: createPerformance({ cagr: 0.3, totalTrades: 12 }),
        createdAt: new Date('2026-01-01T00:00:00.000Z')
      }
    ], {
      settingsValue: settings.templateScoring
    });

    expect(result.scores.has('template-1')).toBe(false);
  });
});
