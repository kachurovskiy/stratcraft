import { createDispatchOperationsHandler } from './dispatchOperationsHandler';

function createOpenOperation() {
  const now = new Date('2026-08-03T08:00:00.000Z');
  return {
    id: 'operation-1',
    accountId: 'account-1',
    strategyId: 'strategy-1',
    tradeId: 'trade-1',
    ticker: 'AIV',
    operationType: 'open_position',
    quantity: 335,
    price: 2.66,
    stopLoss: 2.13,
    triggeredAt: now,
    status: 'pending',
    statusUpdatedAt: now,
    attemptCount: 0,
    createdAt: now,
    updatedAt: now
  };
}

function createHarness() {
  const operation = createOpenOperation();
  const account = {
    id: 'account-1',
    userId: 1,
    name: 'Paper',
    provider: 'alpaca',
    environment: 'paper',
    excludedTickers: [],
    excludedKeywords: [],
    apiKey: 'key',
    apiSecret: 'secret',
    createdAt: new Date('2026-08-01T00:00:00.000Z'),
    updatedAt: new Date('2026-08-01T00:00:00.000Z')
  };
  const candidate = {
    operation,
    account,
    strategyName: 'ADX',
    userId: 1,
    userEmail: null
  };
  const deps = {
    db: {
      accountOperations: {
        getPendingAccountOperationsForDispatch: jest.fn().mockResolvedValue([candidate]),
        recordAccountOperationAttempt: jest.fn().mockResolvedValue(undefined)
      },
      trades: {
        ensureLiveTradeForOperation: jest.fn().mockResolvedValue(undefined),
        updateTradeOrderIdForOperation: jest.fn().mockResolvedValue(undefined),
        updateTradeEntryCancelAfter: jest.fn().mockResolvedValue(undefined),
        updateTradeStopOrderId: jest.fn().mockResolvedValue(undefined),
        updateTradeStopLossFromOperation: jest.fn().mockResolvedValue(undefined)
      }
    },
    accountDataService: {
      dispatchOperation: jest.fn().mockResolvedValue({
        status: 'sent',
        reason: 'Order order-1',
        orderId: 'order-1',
        stopOrderId: 'stop-1',
        cancelAfter: new Date('2026-08-03T20:00:00.000Z')
      })
    },
    emailService: {
      sendOperationDispatchSummary: jest.fn()
    }
  } as any;
  const ctx = {
    job: {
      id: 'job-1',
      type: 'dispatch-operations',
      status: 'running',
      scheduledFor: new Date('2026-08-03T08:00:00.000Z'),
      createdAt: new Date('2026-08-03T08:00:00.000Z'),
      attempts: 1,
      maxRetries: 5
    },
    abortSignal: new AbortController().signal,
    loggingService: {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn()
    },
    scheduler: {}
  } as any;

  return { ctx, deps, operation };
}

describe('createDispatchOperationsHandler', () => {
  test('creates open-position live trade before writing Alpaca order ids', async () => {
    const { ctx, deps, operation } = createHarness();

    const handler = createDispatchOperationsHandler(deps);
    const result = await handler(ctx);

    expect(deps.db.trades.ensureLiveTradeForOperation).toHaveBeenCalledWith(operation, 1);
    expect(deps.db.trades.updateTradeOrderIdForOperation).toHaveBeenCalledWith(operation, 'order-1');
    expect(deps.db.trades.updateTradeStopOrderId).toHaveBeenCalledWith('trade-1', 'stop-1');
    expect(deps.db.trades.ensureLiveTradeForOperation.mock.invocationCallOrder[0]).toBeLessThan(
      deps.db.trades.updateTradeOrderIdForOperation.mock.invocationCallOrder[0]
    );
    expect(result).toEqual({
      message: 'Dispatched 1 operation(s)',
      meta: { sent: 1, failed: 0, skipped: 0 }
    });
  });
});
