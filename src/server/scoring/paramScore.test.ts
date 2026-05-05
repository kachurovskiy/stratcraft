import { createDefaultSettingsValue } from '../settings/defaults';
import type { BacktestCacheRow } from './paramScore';
import { scoreBacktestParameters } from './paramScore';

const createRow = (totalTrades: number): BacktestCacheRow => ({
  id: 'row-1',
  parameters: { lookback: 10 },
  sharpe_ratio: 1.2,
  calmar_ratio: 1.1,
  total_return: 0.4,
  cagr: 0.2,
  max_drawdown: 10,
  max_drawdown_ratio: 0.1,
  win_rate: 0.6,
  total_trades: totalTrades
}) as BacktestCacheRow;

const createNeighborDensityRow = (id: string, x: number): BacktestCacheRow => ({
  id,
  parameters: { x },
  sharpe_ratio: 1,
  calmar_ratio: 1,
  total_return: 0.2,
  cagr: 0.2,
  max_drawdown: 0,
  max_drawdown_ratio: 0,
  win_rate: 0.5,
  total_trades: 50
}) as BacktestCacheRow;

describe('paramScore settings integration', () => {
  test('uses the normalized settings snapshot from SettingsRepo.value', async () => {
    const settings = createDefaultSettingsValue();
    settings.paramScoring.minTrades = 0;

    const result = await scoreBacktestParameters([createRow(1)], settings.paramScoring);

    expect(result.scored).toHaveLength(1);
    expect(result.availabilityById.get('row-1')).toEqual({ eligible: true });
  });

  test('applies stabilityGamma from settings', async () => {
    const settings = createDefaultSettingsValue();
    settings.paramScoring.minTrades = 0;
    settings.paramScoring.stabilityGamma = 0;

    const result = await scoreBacktestParameters([createRow(1)], settings.paramScoring);

    expect(result.scored).toHaveLength(1);
    expect(result.scored[0]?.stabilityScore).toBe(0);
    expect(result.scored[0]?.finalScore).toBeGreaterThan(0);
  });

  test('discounts sparse neighborhoods even when neighbor quality matches dense neighborhoods', async () => {
    const settings = createDefaultSettingsValue();
    settings.paramScoring.minTrades = 0;
    settings.paramScoring.drawdownLambda = 0;
    settings.paramScoring.stabilityGamma = 1;

    const result = await scoreBacktestParameters([
      createNeighborDensityRow('thin-seed', 0),
      createNeighborDensityRow('thin-neighbor', 0.01),
      createNeighborDensityRow('dense-seed', 10),
      createNeighborDensityRow('dense-neighbor-1', 10.01),
      createNeighborDensityRow('dense-neighbor-2', 10.02),
      createNeighborDensityRow('dense-neighbor-3', 10.03),
      createNeighborDensityRow('dense-neighbor-4', 10.04)
    ], settings.paramScoring);

    const thin = result.scored.find(candidate => (candidate.sourceRow as any)?.id === 'thin-seed');
    const dense = result.scored.find(candidate => (candidate.sourceRow as any)?.id === 'dense-seed');

    expect(result.scored[0]?.parameters.x).toBeGreaterThanOrEqual(10);
    expect(thin?.stabilityScore).toBeCloseTo(0.25);
    expect(dense?.stabilityScore).toBe(1);
    expect(dense?.finalScore).toBeGreaterThan(thin?.finalScore ?? 0);
  });
});
