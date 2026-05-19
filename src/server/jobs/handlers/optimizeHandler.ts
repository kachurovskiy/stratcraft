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
    let defaultRefreshChecked = 0;
    let defaultRefreshCount = 0;
    let defaultRefreshUnchanged = 0;
    const defaultRefreshFailures: string[] = [];
    let exploreAttempted = 0;
    let exploredCount = 0;
    const exploreFailures: string[] = [];
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

      ctx.loggingService.info(
        OPTIMIZE_SOURCE,
        'Checking default strategies for current best parameters after verification and balance',
        logMetadata
      );
      try {
        if (ctx.abortSignal.aborted) {
          throw new Error('Default strategy refresh cancelled');
        }
        const defaultRefresh = await deps.strategyRegistry.refreshOutdatedDefaultStrategies();
        defaultRefreshChecked = defaultRefresh.checked;
        defaultRefreshCount = defaultRefresh.refreshed;
        defaultRefreshUnchanged = defaultRefresh.unchanged;
        defaultRefreshFailures.push(...defaultRefresh.failures);
      } catch (error) {
        if (ctx.abortSignal.aborted) {
          throw new Error('Default strategy refresh cancelled');
        }
        const message = error instanceof Error ? error.message : String(error);
        ctx.loggingService.error(OPTIMIZE_SOURCE, 'Default strategy refresh failed', {
          ...logMetadata,
          error: message
        });
      }

      const exploreEnabled = deps.db.settings.value.optimizer.optimizerExploreEnabled;
      if (exploreEnabled) {
        const cacheCounts = await deps.db.backtestCache.getBacktestCacheTemplateCounts();
        const cacheCountsByTemplate = new Map(cacheCounts.map((entry) => [entry.templateId, entry.count]));
        const exploreTemplateIds = templateIds
          .filter((templateId) => templateId !== 'buy_and_hold')
          .sort((left, right) => {
            const leftCount = cacheCountsByTemplate.get(left) ?? 0;
            const rightCount = cacheCountsByTemplate.get(right) ?? 0;
            if (leftCount !== rightCount) {
              return leftCount - rightCount;
            }
            return left.localeCompare(right);
          });

        exploreAttempted = exploreTemplateIds.length;
        if (exploreAttempted > 0) {
          ctx.loggingService.info(
            OPTIMIZE_SOURCE,
            `Starting explore runs for ${exploreAttempted} template(s) ordered by fewest cached backtests`,
            logMetadata
          );
        }

        for (const templateId of exploreTemplateIds) {
          if (ctx.abortSignal.aborted) {
            throw new Error('Explore cancelled');
          }
          ctx.loggingService.info(OPTIMIZE_SOURCE, `Exploring template ${templateId}`, logMetadata);
          try {
            await deps.engineCli.run('explore', [templateId], ctx.abortSignal, logMetadata);
            exploredCount += 1;
          } catch (error) {
            if (ctx.abortSignal.aborted) {
              throw new Error('Explore cancelled');
            }
            const message = error instanceof Error ? error.message : String(error);
            exploreFailures.push(templateId);
            ctx.loggingService.error(OPTIMIZE_SOURCE, `Explore run failed for ${templateId}`, {
              ...logMetadata,
              error: message
            });
          }
        }
      } else {
        ctx.loggingService.info(
          OPTIMIZE_SOURCE,
          'Skipping explore runs because optimizer explore is disabled',
          logMetadata
        );
      }

      const optimizeMessage = optimizedCount > 0 ? `Optimized ${optimizedCount} templates` : 'No optimization required';
      const verifyMessage = verifyAttempted > 0
        ? `Verified ${verifiedCount}/${verifyAttempted} templates${verifyFailures.length ? ` (${verifyFailures.length} failed)` : ''}`
        : 'No templates verified';
      const balanceMessage = balanceAttempted > 0
        ? `Balanced ${balancedCount}/${balanceAttempted} templates${balanceFailures.length ? ` (${balanceFailures.length} failed)` : ''}`
        : 'No templates balanced';
      const exploreMessage = exploreEnabled
        ? exploreAttempted > 0
          ? `Explored ${exploredCount}/${exploreAttempted} templates${exploreFailures.length ? ` (${exploreFailures.length} failed)` : ''}`
          : 'No templates explored'
        : 'Explore disabled';

      return {
        message: `${optimizeMessage}; ${verifyMessage}; ${balanceMessage}; ${exploreMessage}`,
        meta: {
          optimized: optimizedCount,
          verifyAttempted,
          verified: verifiedCount,
          verifyFailures,
          balanceAttempted,
          balanced: balancedCount,
          balanceFailures,
          defaultRefreshAttempted: defaultRefreshChecked,
          defaultRefreshed: defaultRefreshCount,
          defaultRefreshSkipped: defaultRefreshUnchanged,
          defaultRefreshFailures,
          exploreEnabled,
          exploreAttempted,
          explored: exploredCount,
          exploreFailures
        }
      };
    } finally {
      ctx.abortSignal.removeEventListener('abort', terminateOnAbort);
    }
  };
}
