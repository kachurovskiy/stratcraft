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

describe('paramScore settings integration', () => {
  test('uses the normalized settings snapshot from SettingsRepo.value', async () => {
    const settings = createDefaultSettingsValue();
    settings.paramScoring.minTrades = 0;

    const result = await scoreBacktestParameters([createRow(1)], {
      settingsRepo: { value: settings }
    });

    expect(result.scored).toHaveLength(1);
    expect(result.availabilityById.get('row-1')).toEqual({ eligible: true });
  });
});
