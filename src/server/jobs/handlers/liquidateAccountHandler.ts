import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const LIQUIDATE_SOURCE = 'liquidate-account-job';

const buildLiquidationSummary = (result: {
  totalPositions: number;
  submittedOrders: number;
  plannedOrders: number;
  skippedDeviationPositions: number;
  skippedMissingPricePositions: number;
  skippedMissingReferencePositions: number;
  failedOrders: number;
  dryRun: boolean;
}, discountPercent: number): string => {
  if (result.totalPositions === 0) {
    return result.dryRun ? 'Dry run complete. No positions to liquidate.' : 'No positions to liquidate.';
  }
  const discountLabel = discountPercent > 0 ? ` at ${discountPercent}% discount` : '';
  const baseLabel = result.dryRun ? 'Planned' : 'Submitted';
  const orderCount = result.dryRun ? result.plannedOrders : result.submittedOrders;
  const parts = [
    `${baseLabel} ${orderCount} limit order${orderCount === 1 ? '' : 's'}${discountLabel}.`
  ];
  if (result.skippedDeviationPositions > 0) {
    parts.push(
      `Skipped ${result.skippedDeviationPositions} position${result.skippedDeviationPositions === 1 ? '' : 's'} outside deviation band.`
    );
  }
  if (result.skippedMissingPricePositions > 0) {
    parts.push(
      `Skipped ${result.skippedMissingPricePositions} position${result.skippedMissingPricePositions === 1 ? '' : 's'} without latest price.`
    );
  }
  if (result.skippedMissingReferencePositions > 0) {
    parts.push(
      `Skipped ${result.skippedMissingReferencePositions} position${result.skippedMissingReferencePositions === 1 ? '' : 's'} without last close.`
    );
  }
  if (result.failedOrders > 0) {
    parts.push(
      `${result.failedOrders} order${result.failedOrders === 1 ? '' : 's'} failed to submit.`
    );
  }
  return parts.join(' ');
};

export function createLiquidateAccountHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const accountId = typeof ctx.job.metadata?.accountId === 'string' ? ctx.job.metadata.accountId.trim() : '';
    const userIdRaw = ctx.job.metadata?.userId;
    const userId = typeof userIdRaw === 'number' ? userIdRaw : Number(userIdRaw);

    if (!accountId || !Number.isFinite(userId)) {
      throw new Error('Liquidation job missing account metadata.');
    }

    const logMetadata = { jobId: ctx.job.id, accountId };
    ctx.loggingService.info(LIQUIDATE_SOURCE, 'Running liquidation job', logMetadata);

    const account = await deps.db.accounts.getAccountById(accountId, userId);
    if (!account) {
      throw new Error('Account not found or inaccessible.');
    }
    const provider = typeof account.provider === 'string' ? account.provider.trim().toLowerCase() : '';
    if (provider !== 'alpaca') {
      throw new Error('Liquidation is only supported for Alpaca accounts.');
    }

    const discountPercent = deps.db.settings.value.alpaca.accountLiquidationDiscountPercent;
    const deviationBandPercent = deps.db.settings.value.alpaca.accountLiquidationDeviationBandPercent;
    const dryRun = Boolean(ctx.job.metadata?.dryRun);

    const result = await deps.accountDataService.liquidatePositions(
      account,
      {
        discountPercent,
        deviationBandPercent,
        dryRun
      },
      ctx.abortSignal
    );

    const summary = buildLiquidationSummary(result, discountPercent);
    let emailError: string | null = null;

    const user = await deps.db.users.getUserByIdRow(userId);
    const email = typeof user?.email === 'string' ? user.email.trim() : '';
    if (email) {
      try {
        await deps.emailService.sendLiquidationSummary(email, {
          accountId: account.id,
          accountName: account.name,
          accountProvider: account.provider,
          accountEnvironment: account.environment,
          dryRun,
          discountPercent,
          deviationBandPercent,
          cancelledOrders: result.cancelledOrders,
          expectedLiquidationValue: result.expectedLiquidationValue,
          orders: result.orders
        });
      } catch (error) {
        emailError = error instanceof Error ? error.message : String(error);
        ctx.loggingService.warn(LIQUIDATE_SOURCE, 'Failed to send liquidation summary email', {
          ...logMetadata,
          error: emailError
        });
      }
    } else {
      ctx.loggingService.warn(LIQUIDATE_SOURCE, 'No email found for liquidation summary', {
        ...logMetadata,
        userId
      });
    }
    if (result.failedOrders > 0) {
      ctx.loggingService.warn(LIQUIDATE_SOURCE, 'Liquidation completed with failures', {
        ...logMetadata,
        failedOrders: result.failedOrders
      });
    }

    ctx.loggingService.info(LIQUIDATE_SOURCE, 'Liquidation completed', {
      ...logMetadata,
      submittedOrders: result.submittedOrders,
      plannedOrders: result.plannedOrders,
      skippedMissingPricePositions: result.skippedMissingPricePositions,
      skippedDeviationPositions: result.skippedDeviationPositions,
      skippedMissingReferencePositions: result.skippedMissingReferencePositions
    });

    return {
      message: summary,
      meta: {
        accountId,
        dryRun,
        discountPercent,
        deviationBandPercent,
        submittedOrders: result.submittedOrders,
        plannedOrders: result.plannedOrders,
        skippedDeviationPositions: result.skippedDeviationPositions,
        skippedMissingPricePositions: result.skippedMissingPricePositions,
        skippedMissingReferencePositions: result.skippedMissingReferencePositions,
        failedOrders: result.failedOrders,
        expectedLiquidationValue: result.expectedLiquidationValue,
        emailError
      }
    };
  };
}
