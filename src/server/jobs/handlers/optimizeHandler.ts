import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';

const OPTIMIZE_SOURCE = 'optimize-job';

export function createOptimizeHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const logMetadata = { jobId: ctx.job.id };
    let optimizedCount = 0;
    let verifyAttempted = 0;
    let verifiedCount = 0;
    const verifyFailures: string[] = [];
    let balanceAttempted = 0;
    let balancedCount = 0;
    const balanceFailures: string[] = [];
    const terminateOnAbort = () => {
      ctx.loggingService.warn(OPTIMIZE_SOURCE, 'Terminate request received, stopping optimize run', logMetadata);
      deps.engineCli.forceTerminateActiveProcess('optimize-preempted', logMetadata);
    };
    ctx.abortSignal.addEventListener('abort', terminateOnAbort);

    try {
      while (!ctx.abortSignal.aborted) {
        const template = await deps.db.templates.getTemplateNeedingLocalOptimization();
        if (!template) {
          ctx.loggingService.info(OPTIMIZE_SOURCE, 'No templates need optimization', logMetadata);
          break;
        }
        const remoteActive = await deps.db.remoteOptimizerJobs.hasActiveRemoteOptimizerJob(template.id);
        if (remoteActive) {
          ctx.loggingService.info(
            OPTIMIZE_SOURCE,
            `Skipping template ${template.name} (${template.id}) because a remote optimizer job is active`,
            logMetadata
          );
          continue;
        }

        ctx.loggingService.info(OPTIMIZE_SOURCE, `Optimizing template ${template.name} (${template.id})`, logMetadata);
        try {
          await deps.engineCli.run('optimize', [template.id], ctx.abortSignal, logMetadata);
          optimizedCount += 1;
          try {
            await deps.strategyRegistry.ensureDefaultStrategyForTemplate(template.id);
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            ctx.loggingService.error(OPTIMIZE_SOURCE, `Failed to recreate default strategy for ${template.id}`, {
              ...logMetadata,
              error: message
            });
          }
        } catch (error) {
          if (ctx.abortSignal.aborted) {
            throw new Error('Optimization cancelled');
          }
          const message = error instanceof Error ? error.message : String(error);
          ctx.loggingService.error(OPTIMIZE_SOURCE, `Optimization failed for ${template.id}`, {
            ...logMetadata,
            error: message
          });
          break;
        }
      }

      if (ctx.abortSignal.aborted) {
        throw new Error('Optimization cancelled');
      }

      const templateIds = await deps.db.templates.getAllTemplateIds();
      verifyAttempted = templateIds.length;
      if (verifyAttempted > 0) {
        ctx.loggingService.info(
          OPTIMIZE_SOURCE,
          `Starting verification for ${verifyAttempted} template(s) in alphabetical order`,
          logMetadata
        );
      }

      for (const templateId of templateIds) {
        if (ctx.abortSignal.aborted) {
          throw new Error('Verification cancelled');
        }
        ctx.loggingService.info(OPTIMIZE_SOURCE, `Verifying template ${templateId}`, logMetadata);
        try {
          await deps.engineCli.run('verify', [templateId], ctx.abortSignal, logMetadata);
          verifiedCount += 1;
        } catch (error) {
          if (ctx.abortSignal.aborted) {
            throw new Error('Verification cancelled');
          }
          const message = error instanceof Error ? error.message : String(error);
          verifyFailures.push(templateId);
          ctx.loggingService.error(OPTIMIZE_SOURCE, `Verification failed for ${templateId}`, {
            ...logMetadata,
            error: message
          });
        }
      }

      balanceAttempted = templateIds.length;
      if (balanceAttempted > 0) {
        ctx.loggingService.info(
          OPTIMIZE_SOURCE,
          `Starting balance runs for ${balanceAttempted} template(s) in alphabetical order`,
          logMetadata
        );
      }

      for (const templateId of templateIds) {
        if (ctx.abortSignal.aborted) {
          throw new Error('Balance cancelled');
        }
        ctx.loggingService.info(OPTIMIZE_SOURCE, `Balancing template ${templateId}`, logMetadata);
        try {
          await deps.engineCli.run('balance', [templateId], ctx.abortSignal, logMetadata);
          balancedCount += 1;
        } catch (error) {
          if (ctx.abortSignal.aborted) {
            throw new Error('Balance cancelled');
          }
          const message = error instanceof Error ? error.message : String(error);
          balanceFailures.push(templateId);
          ctx.loggingService.error(OPTIMIZE_SOURCE, `Balance run failed for ${templateId}`, {
            ...logMetadata,
            error: message
          });
        }
      }

      const optimizeMessage = optimizedCount > 0 ? `Optimized ${optimizedCount} templates` : 'No optimization required';
      const verifyMessage = verifyAttempted > 0
        ? `Verified ${verifiedCount}/${verifyAttempted} templates${verifyFailures.length ? ` (${verifyFailures.length} failed)` : ''}`
        : 'No templates verified';
      const balanceMessage = balanceAttempted > 0
        ? `Balanced ${balancedCount}/${balanceAttempted} templates${balanceFailures.length ? ` (${balanceFailures.length} failed)` : ''}`
        : 'No templates balanced';

      return {
        message: `${optimizeMessage}; ${verifyMessage}; ${balanceMessage}`,
        meta: {
          optimized: optimizedCount,
          verifyAttempted,
          verified: verifiedCount,
          verifyFailures,
          balanceAttempted,
          balanced: balancedCount,
          balanceFailures
        }
      };
    } finally {
      ctx.abortSignal.removeEventListener('abort', terminateOnAbort);
    }
  };
}
