import axios, { AxiosError } from 'axios';
import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';
import { OperationDispatchSummaryPayload } from '../../services/EmailService';
import { DispatchResult } from '../../services/AccountDataService';
import { AccountOperationType } from '../../../shared/types/StrategyTemplate';

const DISPATCH_SOURCE = 'dispatch-operations-job';
const OPERATION_DISPATCH_PRIORITY: Record<AccountOperationType, number> = {
  update_stop_loss: 0,
  close_position: 1,
  open_position: 2
};
const DEFAULT_OPERATION_PRIORITY = 99;

type UserDispatchSummary = OperationDispatchSummaryPayload & { email: string };

export function createDispatchOperationsHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    ctx.loggingService.info(DISPATCH_SOURCE, 'Running dispatch-operations job', logMetadata);
    const candidates = await deps.db.accountOperations.getPendingAccountOperationsForDispatch();

    if (candidates.length === 0) {
      ctx.loggingService.info(DISPATCH_SOURCE, 'No pending operations to dispatch', logMetadata);
      return { message: 'No operations to dispatch' };
    }

    const summaries = new Map<string, UserDispatchSummary>();
    let sentCount = 0;
    let failedCount = 0;
    let skippedCount = 0;

    const orderedCandidates = sortOperationsForDispatch(candidates);

    for (const candidate of orderedCandidates) {
      if (ctx.abortSignal.aborted) {
        throw new Error('Dispatch operations job aborted');
      }

      let result: DispatchResult;
      try {
        result = await deps.accountDataService.dispatchOperation(
          candidate.account,
          candidate.operation,
          ctx.abortSignal
        );
      } catch (error) {
        result = {
          status: 'failed',
          reason: extractErrorMessage(error)
        };
      }
      if (result.status === 'sent') {
        sentCount += 1;
      } else if (result.status === 'skipped') {
        skippedCount += 1;
      } else {
        failedCount += 1;
      }

      if (result.status === 'sent') {
        try {
          if (result.orderId) {
            await deps.db.trades.updateTradeOrderIdForOperation(candidate.operation, result.orderId);
          }
          if (candidate.operation.operationType === 'open_position') {
            await deps.db.trades.ensureLiveTradeForOperation(candidate.operation, candidate.userId ?? null);
            const cancelAfter = normalizeDate(result.cancelAfter);
            if (cancelAfter) {
              await deps.db.trades.updateTradeEntryCancelAfter(candidate.operation.tradeId, cancelAfter);
            }
            if (result.stopOrderId) {
              await deps.db.trades.updateTradeStopOrderId(candidate.operation.tradeId, result.stopOrderId);
            }
          } else if (candidate.operation.operationType === 'update_stop_loss') {
            await deps.db.trades.updateTradeStopLossFromOperation(candidate.operation);
            if (result.orderId) {
              await deps.db.trades.updateTradeStopOrderId(candidate.operation.tradeId, result.orderId);
            }
          }
        } catch (error) {
          ctx.loggingService.warn(DISPATCH_SOURCE, 'Failed to synchronize live trade record', {
            ...logMetadata,
            operationId: candidate.operation.id,
            strategyId: candidate.operation.strategyId,
            accountId: candidate.account.id,
            error: extractErrorMessage(error)
          });
        }
      }

      await deps.db.accountOperations.recordAccountOperationAttempt(
        candidate.operation,
        result.status,
        result.reason,
        result.orderId ?? null,
        result.payload ?? null
      );

      ctx.loggingService.info(DISPATCH_SOURCE, `Operation ${candidate.operation.id} ${result.status}`, {
        ...logMetadata,
        strategyId: candidate.operation.strategyId,
        accountId: candidate.account.id,
        ticker: candidate.operation.ticker,
        status: result.status,
        reason: result.reason
      });

      if (candidate.userEmail) {
        const summary = summaries.get(candidate.userEmail) ?? {
          email: candidate.userEmail,
          operations: []
        };
        summary.operations.push({
          accountName: candidate.account.name,
          accountProvider: candidate.account.provider,
          accountEnvironment: candidate.account.environment,
          ticker: candidate.operation.ticker,
          operationType: candidate.operation.operationType,
          quantity: candidate.operation.quantity ?? null,
          price: candidate.operation.price ?? null,
          orderType: candidate.operation.orderType ?? null,
          status: result.status,
          statusReason: result.reason
        });
        summaries.set(candidate.userEmail, summary);
      }
    }

    for (const summary of summaries.values()) {
      try {
        if (summary.operations.length > 0) {
          await deps.emailService.sendOperationDispatchSummary(summary.email, summary);
        }
      } catch (error) {
        ctx.loggingService.error(DISPATCH_SOURCE, 'Failed to send dispatch summary email', {
          ...logMetadata,
          email: summary.email,
          error: extractErrorMessage(error)
        });
      }
    }

    ctx.loggingService.info(DISPATCH_SOURCE, 'Dispatch job completed', {
      ...logMetadata,
      sent: sentCount,
      failed: failedCount,
      skipped: skippedCount
    });

    const summaryParts = [`Dispatched ${sentCount} operation(s)`];
    if (failedCount > 0) {
      summaryParts.push(`${failedCount} failed`);
    }
    if (skippedCount > 0) {
      summaryParts.push(`${skippedCount} skipped`);
    }

    return {
      message: summaryParts.join(', '),
      meta: { sent: sentCount, failed: failedCount, skipped: skippedCount }
    };
  };
}

function sortOperationsForDispatch<T extends { operation: { operationType: AccountOperationType } }>(
  candidates: T[]
): T[] {
  return [...candidates].sort((a, b) => {
    const priorityA = OPERATION_DISPATCH_PRIORITY[a.operation.operationType] ?? DEFAULT_OPERATION_PRIORITY;
    const priorityB = OPERATION_DISPATCH_PRIORITY[b.operation.operationType] ?? DEFAULT_OPERATION_PRIORITY;
    return priorityA - priorityB;
  });
}

function extractErrorMessage(error: unknown): string {
  if (axios.isAxiosError(error)) {
    const axiosError = error as AxiosError;
    const data = axiosError.response?.data as any;
    if (data) {
      if (typeof data === 'string') {
        return data;
      }
      if (typeof data.error === 'string') {
        return data.error;
      }
      if (typeof data.message === 'string') {
        return data.message;
      }
    }
    if (axiosError.message) {
      return axiosError.message;
    }
  }
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function normalizeDate(value: unknown): Date | null {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }
  return null;
}
