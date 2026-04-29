import { StrategyRegistry } from './registry';
import type { StrategyTemplate } from '../types/StrategyTemplate';

const template: StrategyTemplate = {
  id: 'adx_breakout',
  name: 'ADX Breakout',
  description: '',
  category: '',
  author: '',
  version: '',
  parameters: [
    {
      name: 'alpha',
      type: 'number',
      label: 'Alpha',
      description: '',
      required: true,
      default: 1
    },
    {
      name: 'beta',
      type: 'number',
      label: 'Beta',
      description: '',
      required: true,
      default: 2
    }
  ]
};

function createRegistryHarness(existingParameters: Record<string, unknown> | null) {
  const db = {
    strategies: {
      getStrategy: jest.fn().mockResolvedValue(
        existingParameters
          ? {
              parameters: existingParameters
            }
          : null
      ),
      deleteStrategy: jest.fn().mockResolvedValue({
        strategyDeleted: true,
        accountOperationsDeleted: 0,
        tradesDeleted: 0,
        backtestResultsDeleted: 0
      }),
      insertStrategy: jest.fn().mockResolvedValue(undefined)
    }
  };
  const registry = new StrategyRegistry(db as any);
  jest.spyOn(registry, 'getTemplates').mockReturnValue([template]);
  (registry as any).resolveDefaultStrategyParameters = jest.fn().mockResolvedValue({ alpha: 3, beta: 5 });

  return { db, registry };
}

describe('StrategyRegistry.refreshOutdatedDefaultStrategies', () => {
  test('recreates default strategies with non-optimal parameters', async () => {
    const { db, registry } = createRegistryHarness({ alpha: 1, beta: 2 });

    const summary = await registry.refreshOutdatedDefaultStrategies();

    expect(db.strategies.deleteStrategy).toHaveBeenCalledWith('default_adx_breakout');
    expect(db.strategies.insertStrategy).toHaveBeenCalledWith({
      id: 'default_adx_breakout',
      name: 'ADX Breakout',
      templateId: 'adx_breakout',
      parameters: { alpha: 3, beta: 5 },
      status: 'active'
    });
    expect(summary).toEqual({
      checked: 1,
      refreshed: 1,
      unchanged: 0,
      failures: []
    });
  });

  test('keeps default strategies that already have optimal parameters', async () => {
    const { db, registry } = createRegistryHarness({ beta: 5, alpha: 3 });

    const summary = await registry.refreshOutdatedDefaultStrategies();

    expect(db.strategies.deleteStrategy).not.toHaveBeenCalled();
    expect(db.strategies.insertStrategy).not.toHaveBeenCalled();
    expect(summary).toEqual({
      checked: 1,
      refreshed: 0,
      unchanged: 1,
      failures: []
    });
  });
});
