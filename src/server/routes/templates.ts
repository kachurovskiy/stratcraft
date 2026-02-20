import express, { NextFunction, Request, Response } from 'express';
import { TemplateParams } from '../../shared/types/Express';
import {
  Strategy,
  StrategyParameter,
  StrategyTemplate
} from '../../shared/types/StrategyTemplate';
import { getReqUserId, getCurrentUrl, formatBacktestPeriodLabel } from './utils';
import type {
  RemoteOptimizationJobSnapshot,
  RemoteOptimizationStatus
} from '../../shared/types/RemoteOptimization';
import {
  scoreBacktestParameters,
  type BacktestCacheRow,
  type ScoreAvailabilityResult
} from '../scoring/paramScore';
import { SETTING_KEYS } from '../constants';
import type { BacktestCachePerformancePoint } from '../database/types';
import {
  computeTemplateScoreResults,
  type TemplateScoreSnapshot,
  type TemplateVerificationMetrics
} from '../scoring/templateScore';

const router = express.Router();
const ACTIVE_REMOTE_JOB_STATUSES: RemoteOptimizationStatus[] = ['queued', 'running', 'handoff'];
const REMOTE_STATUS_LABELS: Record<RemoteOptimizationStatus, string> = {
  queued: 'Queued',
  running: 'Provisioning',
  handoff: 'Remote Server Running',
  succeeded: 'Succeeded',
  failed: 'Failed'
};

// Authentication middleware
const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

const requireAdmin = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
};

// Template Gallery
router.get('/', requireAuth, async (req: Request, res: Response) => {
  try {
    const templates = req.strategyRegistry.getTemplates();
    const remoteOptimizerService = req.remoteOptimizerService;
    const remoteOptimizationJobsByTemplate = new Map<string, RemoteOptimizationJobSnapshot[]>();
    const templateIds = templates.map((template: StrategyTemplate) => template.id);

    if (remoteOptimizerService?.listJobs) {
      const remoteJobs: RemoteOptimizationJobSnapshot[] = await remoteOptimizerService.listJobs();
      remoteJobs.forEach(job => {
        const existing = remoteOptimizationJobsByTemplate.get(job.templateId);
        if (existing) {
          existing.push(job);
        } else {
          remoteOptimizationJobsByTemplate.set(job.templateId, [job]);
        }
      });
    }

    const userId = getReqUserId(req);
    const [
      bestParamsByTemplate,
      localOptimizationVersions,
      performanceCloudEntries,
      backtestResultsByTemplate
    ] = await Promise.all([
      req.db.backtestCache.getBestParamsByTemplateIds(templateIds),
      req.db.templates.getTemplateLocalOptimizationVersions(),
      templateIds.length
        ? req.db.backtestCache.getTopBacktestCacheEntriesByTemplate(templateIds)
        : Promise.resolve([]),
      templateIds.length
        ? Promise.all(
            templateIds.map(templateId => req.db.backtestResults.getBacktestResultsForTemplate(templateId, userId))
          )
        : Promise.resolve([])
    ]);
    const defaultBacktestSnapshots: TemplateScoreSnapshot[] = [];
    backtestResultsByTemplate.forEach((results, index) => {
      const templateId = templateIds[index];
      const seen = new Set<string>();
      results.forEach(result => {
        const strategyId = result.strategyId;
        if (!strategyId || !strategyId.startsWith('default_')) {
          return;
        }
        const periodMonths = typeof result.periodMonths === 'number' ? result.periodMonths : 0;
        if (!Number.isFinite(periodMonths) || periodMonths <= 0) {
          return;
        }
        const tickerScope = result.tickerScope === 'validation' ? 'validation' : 'training';
        const key = `${strategyId}:${periodMonths}:${tickerScope}`;
        if (seen.has(key)) {
          return;
        }
        seen.add(key);

        defaultBacktestSnapshots.push({
          templateId,
          strategyId,
          periodMonths,
          periodDays: typeof result.periodDays === 'number' ? result.periodDays : null,
          tickerScope,
          performance: result.performance ?? null,
          createdAt: result.createdAt ?? null
        });
      });
    });

    const templateNameById = new Map(
      templates.map((template: StrategyTemplate) => [template.id, template.name] as const)
    );
    const performanceCloudPoints = (performanceCloudEntries as BacktestCachePerformancePoint[]).map(entry => ({
      templateId: entry.templateId,
      templateName: templateNameById.get(entry.templateId) ?? entry.templateId,
      calmarRatio: entry.calmarRatio,
      sharpeRatio: entry.sharpeRatio,
      verifyCalmarRatio: entry.verifyCalmarRatio,
      verifySharpeRatio: entry.verifySharpeRatio,
      cagr: entry.cagr,
      verifyCagr: entry.verifyCagr,
      maxDrawdownRatio: entry.maxDrawdownRatio,
      verifyMaxDrawdownRatio: entry.verifyMaxDrawdownRatio,
      totalTrades: entry.totalTrades,
      tickerCount: entry.tickerCount,
      createdAt: entry.createdAt
    }));

    const verificationByTemplate = new Map<string, TemplateVerificationMetrics>();
    Object.entries(bestParamsByTemplate).forEach(([templateId, bestParams]) => {
      const sourceRow = bestParams?.sourceRow;
      if (!sourceRow) {
        return;
      }
      verificationByTemplate.set(templateId, {
        verifySharpeRatio: sourceRow.verify_sharpe_ratio ?? null,
        verifyCalmarRatio: sourceRow.verify_calmar_ratio ?? null,
        verifyCagr: sourceRow.verify_cagr ?? null,
        verifyMaxDrawdownRatio: sourceRow.verify_max_drawdown_ratio ?? null
      });
    });

    const templateScoreResults = await computeTemplateScoreResults(defaultBacktestSnapshots, {
      verificationByTemplate,
      settingsRepo: req.db.settings
    });
    const templateScoreById = templateScoreResults.scores;
    const templateScoreBreakdowns = templateScoreResults.breakdowns;

    const templateViews: TemplateListItem[] = templates.map((template: StrategyTemplate) => {
      const bestParams = bestParamsByTemplate[template.id];
      const sharpeValue = typeof bestParams?.sharpeRatio === 'number'
        ? bestParams.sharpeRatio
        : null;
      const calmarValue = typeof bestParams?.calmarRatio === 'number'
        ? bestParams.calmarRatio
        : null;
      const cagrValue = typeof bestParams?.cagr === 'number'
        ? bestParams.cagr
        : null;
      const verifyCagrValue = typeof bestParams?.sourceRow?.verify_cagr === 'number'
        ? bestParams.sourceRow.verify_cagr
        : null;
      const resolvedTopAbsoluteGainTicker =
        typeof bestParams?.sourceRow?.top_abs_gain_ticker === 'string' &&
        bestParams.sourceRow.top_abs_gain_ticker.trim().length > 0
          ? bestParams.sourceRow.top_abs_gain_ticker.trim()
          : null;
      const resolvedTopRelativeGainTicker =
        typeof bestParams?.sourceRow?.top_rel_gain_ticker === 'string' &&
        bestParams.sourceRow.top_rel_gain_ticker.trim().length > 0
          ? bestParams.sourceRow.top_rel_gain_ticker.trim()
          : null;
      const hasTopTickerInsights = Boolean(
        resolvedTopAbsoluteGainTicker ||
        resolvedTopRelativeGainTicker
      );
      const localOptimizationVersion = typeof localOptimizationVersions[template.id] === 'number'
        ? localOptimizationVersions[template.id]
        : 0;
      const templateJobs = remoteOptimizationJobsByTemplate.get(template.id) ?? [];
      const activeJob = templateJobs.find(job => ACTIVE_REMOTE_JOB_STATUSES.includes(job.status));
      const activeStatusLabel = activeJob ? REMOTE_STATUS_LABELS[activeJob.status] ?? activeJob.status : null;
      const tooltip = activeJob
        ? `Remote optimizer job ${activeJob.id} (${activeJob.hetznerServerId ? `Hetzner #${activeJob.hetznerServerId}` : 'awaiting server'}) is ${(
            activeStatusLabel ?? activeJob.status
          ).toLowerCase()}.`
        : null;
      const remoteOptimization: TemplateRemoteOptimizationState = {
        hasRemoteOptimizer: Boolean(remoteOptimizerService),
        isActive: Boolean(activeJob),
        activeJobId: activeJob?.id ?? null,
        activeStatus: activeJob?.status ?? null,
        activeStatusLabel,
        activeServerId: activeJob?.hetznerServerId ?? null,
        activeServerIp: activeJob?.remoteServerIp ?? null,
        tooltip
      };
      const rawTemplateScore = templateScoreById.get(template.id);
      const breakdown = templateScoreBreakdowns.get(template.id);
      const templateScore = breakdown
        ? breakdown.finalScore100
        : typeof rawTemplateScore === 'number'
          ? Math.round(Math.min(1, Math.max(0, rawTemplateScore)) * 100)
          : null;
      const templateScoreBreakdown = breakdown ? {
        baseScore100: breakdown.baseScore100,
        finalScore100: breakdown.finalScore100,
        verificationMultiplier: breakdown.verificationMultiplier,
        componentAverages: breakdown.componentAverages,
        periodCount: breakdown.periodCount,
        lengthWeightAvg: breakdown.weights.lengthWeightAvg,
        recencyWeightAvg: breakdown.weights.recencyWeightAvg
      } : null;

      return {
        ...template,
        localOptimizationVersion,
        bestSharpe: sharpeValue,
        hasBestSharpe: sharpeValue !== null,
        bestCalmar: calmarValue,
        hasBestCalmar: calmarValue !== null,
        bestCagr: cagrValue,
        hasBestCagr: cagrValue !== null,
        bestVerifyCagr: verifyCagrValue,
        hasBestVerifyCagr: verifyCagrValue !== null,
        templateScore,
        hasTemplateScore: templateScore !== null,
        templateScoreBreakdown,
        topAbsoluteGainTicker: resolvedTopAbsoluteGainTicker,
        topRelativeGainTicker: resolvedTopRelativeGainTicker,
        hasTopTickers: hasTopTickerInsights,
        remoteOptimization
      };
    });

    templateViews.sort((a: TemplateListItem, b: TemplateListItem) => {
      const aScore = typeof a.templateScore === 'number' ? a.templateScore : -Infinity;
      const bScore = typeof b.templateScore === 'number' ? b.templateScore : -Infinity;
      if (aScore !== bScore) {
        return bScore - aScore;
      }
      const aCalmar = typeof a.bestCalmar === 'number' ? a.bestCalmar : -Infinity;
      const bCalmar = typeof b.bestCalmar === 'number' ? b.bestCalmar : -Infinity;
      return bCalmar - aCalmar;
    });

    const hasTemplates = templateViews.length > 0;

    res.render('pages/templates', {
      title: 'Strategy Templates',
      page: 'templates',
      user: req.user,
      templates: templateViews,
      hasTemplates,
      performanceCloudPoints,
      currentUrl: getCurrentUrl(req),
      isAdmin: req.user?.role === 'admin',
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error rendering templates page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load templates'
    });
  }
});

const MIN_CACHE_ENTRY_SHARPE = 0.5;

type TemplateBacktestCacheRow = BacktestCacheRow & {
  id: string;
  template_id: string;
  ticker_count: number;
  startDate: Date;
  endDate: Date;
  duration_minutes: number | null;
  tool: string | null;
  period_months: number | null;
  period_days: number | null;
  createdAt: Date;
};

type TemplateRemoteOptimizationState = {
  hasRemoteOptimizer: boolean;
  isActive: boolean;
  activeJobId: string | null;
  activeStatus: RemoteOptimizationStatus | null;
  activeStatusLabel: string | null;
  activeServerId: number | null;
  activeServerIp: string | null;
  tooltip: string | null;
};

type TemplateListItem = StrategyTemplate & {
  bestSharpe: number | null;
  hasBestSharpe: boolean;
  bestCalmar: number | null;
  hasBestCalmar: boolean;
  bestCagr: number | null;
  hasBestCagr: boolean;
  bestVerifyCagr: number | null;
  hasBestVerifyCagr: boolean;
  templateScore: number | null;
  hasTemplateScore: boolean;
  templateScoreBreakdown: {
    baseScore100: number;
    finalScore100: number;
    verificationMultiplier: number | null;
    componentAverages: {
      returnScore: number;
      consistencyScore: number;
      riskScore: number;
      liquidityScore: number;
    };
    periodCount: number;
    lengthWeightAvg: number;
    recencyWeightAvg: number;
  } | null;
  localOptimizationVersion: number;
  topAbsoluteGainTicker: string | null;
  topRelativeGainTicker: string | null;
  hasTopTickers: boolean;
  remoteOptimization: TemplateRemoteOptimizationState;
};

const formatParameterDisplayValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '--';
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return String(value);
    }
    const abs = Math.abs(value);
    const fractionDigits = abs >= 1 ? 2 : 4;
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 0,
      maximumFractionDigits: fractionDigits
    }).format(value);
  }
  if (typeof value === 'boolean') {
    return value ? 'True' : 'False';
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : '--';
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Array.isArray(value)) {
    return value.map(item => formatParameterDisplayValue(item)).join(', ');
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const getComparableParameterValue = (value: unknown): string => {
  if (value === undefined) {
    return '__MISSING__';
  }
  if (value === null) {
    return '__NULL__';
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value.toString() : String(value);
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  if (typeof value === 'string') {
    return value;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Array.isArray(value)) {
    return `[${value.map(item => getComparableParameterValue(item)).join(',')}]`;
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
};

router.get<TemplateParams>('/:templateId', requireAuth, async (req, res) => {
  try {
    const { templateId } = req.params;
    const template = req.strategyRegistry.getTemplate(templateId);

    if (!template) {
      return res.status(404).render('pages/error', {
        title: 'Template Not Found',
        error: `Template ${templateId} does not exist`
      });
    }

    const userId = getReqUserId(req);
    const strategies = await req.db.strategies.getStrategiesByTemplate(templateId, userId);
    const strategiesWithPerformance = await Promise.all(
      strategies.map(async (strategy: Strategy) => {
        const performance = await req.db.backtestResults.getStoredStrategyPerformance(strategy.id);
        return {
          ...strategy,
          performance
        };
      })
    );

    const backtestCacheRaw: TemplateBacktestCacheRow[] = await req.db.backtestCache.getBacktestCacheResultsForTemplate(templateId);

    const scoredCacheSummary = await scoreBacktestParameters(
      backtestCacheRaw as BacktestCacheRow[],
      { settingsRepo: req.db.settings }
    );
    const scoredCacheResults = scoredCacheSummary.scored;
    const bestCacheEntryId = (() => {
      const sourceRow = scoredCacheResults[0]?.sourceRow as { id?: unknown } | undefined;
      const id = typeof sourceRow?.id === 'string' ? sourceRow.id.trim() : '';
      return id.length > 0 ? id : null;
    })();
    const scoringMeta = new Map<string, { coreScore: number; stabilityScore: number; finalScore: number }>();
    const scoredRows: TemplateBacktestCacheRow[] = [];
    scoredCacheResults.forEach((result) => {
      const sourceRow = result.sourceRow as TemplateBacktestCacheRow | undefined;
      if (sourceRow && typeof sourceRow.id === 'string') {
        scoringMeta.set(sourceRow.id, {
          coreScore: result.coreScore,
          stabilityScore: result.stabilityScore,
          finalScore: result.finalScore
        });
        scoredRows.push(sourceRow);
      }
    });
    const scoredRowIds = new Set(scoringMeta.keys());
    const unscoredRows = backtestCacheRaw.filter(entry => !scoredRowIds.has(entry.id));
    const orderedBacktestCacheRows: TemplateBacktestCacheRow[] = [...scoredRows, ...unscoredRows];
    const scoreAvailabilityMap = new Map<string, ScoreAvailabilityResult>();
    orderedBacktestCacheRows.forEach(entry => {
      const availability =
        scoredCacheSummary.availabilityByRow.get(entry) ??
        scoredCacheSummary.availabilityById.get(entry.id);
      if (availability) {
        scoreAvailabilityMap.set(entry.id, availability);
      }
    });

    const rawBacktestCacheEntries = orderedBacktestCacheRows
      .map((entry: TemplateBacktestCacheRow) => {
        const sharpeRatio = typeof entry.sharpe_ratio === 'number' ? entry.sharpe_ratio : null;
        const calmarRatio = typeof entry.calmar_ratio === 'number' ? entry.calmar_ratio : null;
        const totalReturn = typeof entry.total_return === 'number' ? entry.total_return : null;
        const cagr = typeof entry.cagr === 'number' ? entry.cagr : null;
        const verifyCagr = typeof entry.verify_cagr === 'number' ? entry.verify_cagr : null;
        const maxDrawdownRatio = typeof entry.max_drawdown_ratio === 'number'
          ? entry.max_drawdown_ratio
          : null;
        const maxDrawdownPercent = typeof maxDrawdownRatio === 'number'
          ? Math.round(maxDrawdownRatio * 100)
          : null;
        const scoring = scoringMeta.get(entry.id);
        const coreScore = typeof scoring?.coreScore === 'number' ? scoring.coreScore : null;
        const stabilityScore = typeof scoring?.stabilityScore === 'number' ? scoring.stabilityScore : null;
        const finalScore = typeof scoring?.finalScore === 'number' ? scoring.finalScore : null;

        const availability = scoreAvailabilityMap.get(entry.id);
        const scoreUnavailableReason = availability && !availability.eligible
          ? availability.reason
          : null;

        return {
          id: entry.id,
          templateId: entry.template_id,
          parameters: entry.parameters,
          sharpeRatio,
          hasSharpeRatio: sharpeRatio !== null,
          calmarRatio,
          hasCalmarRatio: calmarRatio !== null,
          totalReturn,
          hasTotalReturn: totalReturn !== null,
          cagr,
          hasCagr: cagr !== null,
          verifyCagr,
          hasVerifyCagr: verifyCagr !== null,
          maxDrawdownRatio,
          maxDrawdownPercent,
          totalTrades: entry.total_trades,
          tickerCount: entry.ticker_count,
          startDate: entry.startDate,
          endDate: entry.endDate,
          durationMinutes: entry.duration_minutes ?? null,
          tool: entry.tool,
          periodMonths: entry.period_months,
          periodDays: entry.period_days,
          periodLabel: entry.period_months
            ? formatBacktestPeriodLabel(entry.period_months)
            : (entry.period_days ? `${entry.period_days}d` : 'N/A'),
          createdAt: entry.createdAt,
          coreScore,
          hasCoreScore: coreScore !== null,
          stabilityScore,
          hasStabilityScore: stabilityScore !== null,
          finalScore,
          hasFinalScore: finalScore !== null,
          hasScoreSet: coreScore !== null && stabilityScore !== null && finalScore !== null,
          scoreUnavailableReason
        };
      });

    const filteredBacktestCacheEntries = rawBacktestCacheEntries.filter(entry => {
      const sharpe = entry.sharpeRatio;
      return typeof sharpe === 'number' && sharpe >= MIN_CACHE_ENTRY_SHARPE;
    });

    const displayedBacktestCacheEntries = (() => {
      if (filteredBacktestCacheEntries.length === 0) {
        return filteredBacktestCacheEntries;
      }
      const keepCount = Math.max(1, Math.ceil(filteredBacktestCacheEntries.length * 0.8));
      if (keepCount >= filteredBacktestCacheEntries.length) {
        return filteredBacktestCacheEntries;
      }
      return filteredBacktestCacheEntries.slice(0, keepCount);
    })();

    const parameterMetadataMap = new Map<string, StrategyParameter>(
      template.parameters.map((param: StrategyParameter) => [param.name, param] as [string, StrategyParameter])
    );
    const parameterKeys = new Set<string>();
    displayedBacktestCacheEntries.forEach(entry => {
      Object.keys(entry.parameters ?? {}).forEach(key => parameterKeys.add(key));
    });

    const orderedParameterKeys = [
      ...template.parameters.map((param: StrategyParameter) => param.name).filter((name: string) => parameterKeys.has(name)),
      ...Array.from(parameterKeys).filter(key => !parameterMetadataMap.has(key)).sort()
    ];

    const backtestCacheParameterColumns = displayedBacktestCacheEntries.length === 0
      ? []
      : orderedParameterKeys.reduce<{ key: string; label: string }[]>((cols, key) => {
        const comparableValues = displayedBacktestCacheEntries.map(entry =>
          getComparableParameterValue(entry.parameters?.[key])
        );
        const uniqueValues = new Set(comparableValues);
        if (uniqueValues.size <= 1) {
          return cols;
        }
        const paramMeta = parameterMetadataMap.get(key);
        cols.push({
          key,
          label: paramMeta?.label ?? key
        });
        return cols;
      }, []);

    const backtestCacheEntries = displayedBacktestCacheEntries.map((entry, index, entries) => {
      const parameterCells = backtestCacheParameterColumns.map(column => {
        const value = entry.parameters?.[column.key];
        const currentComparable = getComparableParameterValue(value);
        const previousComparable = index > 0
          ? getComparableParameterValue(entries[index - 1].parameters?.[column.key])
          : null;
        const nextComparable = index < entries.length - 1
          ? getComparableParameterValue(entries[index + 1].parameters?.[column.key])
          : null;
        const isDifferent =
          (previousComparable !== null && previousComparable !== currentComparable) ||
          (nextComparable !== null && nextComparable !== currentComparable);
        return {
          key: column.key,
          value: formatParameterDisplayValue(value),
          isDifferent
        };
      });

      return {
        ...entry,
        parameterCells
      };
    });

    const formatParameterSummary = (entry: typeof backtestCacheEntries[number]): string => {
      const summaryParts: string[] = [];
      orderedParameterKeys.slice(0, 3).forEach(key => {
        const label = parameterMetadataMap.get(key)?.label ?? key;
        const value = formatParameterDisplayValue(entry.parameters?.[key]);
        summaryParts.push(`${label}: ${value}`);
      });
      return summaryParts.join(', ');
    };

    const performanceChartPoints = backtestCacheEntries
      .filter(entry => entry.hasSharpeRatio && entry.hasCalmarRatio && entry.hasTotalReturn)
      .map(entry => ({
        id: entry.id,
        sharpeRatio: entry.sharpeRatio,
        calmarRatio: entry.calmarRatio,
        totalReturn: entry.totalReturn,
        cagr: entry.cagr,
        verifyCagr: entry.verifyCagr,
        maxDrawdownRatio: entry.maxDrawdownRatio,
        maxDrawdownPercent: entry.maxDrawdownPercent,
        totalTrades: entry.totalTrades,
        tickerCount: entry.tickerCount,
        periodLabel: entry.periodLabel,
        createdAt: entry.createdAt,
        tool: entry.tool,
        parameterSummary: formatParameterSummary(entry)
      }));

    const hasPerformanceChartData = performanceChartPoints.length > 0;
    const cagrComparisonPointCount = performanceChartPoints.filter(point =>
      typeof point.cagr === 'number' &&
      typeof point.verifyCagr === 'number'
    ).length;
    const hasCagrComparisonChartData = cagrComparisonPointCount > 0;

    const parameterViews = template.parameters.map((param: StrategyParameter) => ({
      ...param,
      hasDefaultValue: param.default !== undefined && param.default !== null,
      defaultDisplay: param.default !== undefined && param.default !== null
        ? (typeof param.default === 'boolean' ? (param.default ? 'true' : 'false') : String(param.default))
        : null,
      hasMin: typeof param.min === 'number',
      hasMax: typeof param.max === 'number',
      hasStep: typeof param.step === 'number'
    }));

    const summary = {
      parameterCount: parameterViews.length,
      strategyCount: strategiesWithPerformance.length,
      cacheEntryCount: backtestCacheEntries.length
    };

    res.render('pages/template', {
      title: `${template.name} Template Overview`,
      page: 'templates',
      user: req.user,
      isAdmin: req.user?.role === 'admin',
      template,
      templateSummary: summary,
      templateParameters: parameterViews,
      strategies: strategiesWithPerformance,
      hasStrategies: strategiesWithPerformance.length > 0,
      backtestCacheEntries,
      backtestCacheParameterColumns,
      hasBacktestCacheParameterColumns: backtestCacheParameterColumns.length > 0,
      hasBacktestCache: backtestCacheEntries.length > 0,
      performanceChartPoints,
      hasPerformanceChartData,
      hasCagrComparisonChartData,
      cagrComparisonPointCount,
      bestCacheEntry: backtestCacheEntries[0] ?? null,
      bestCacheEntryId,
      currentUrl: getCurrentUrl(req),
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error rendering template detail page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load template details'
    });
  }
});

router.post<TemplateParams>('/:templateId/delete', requireAuth, requireAdmin, async (req, res) => {
  const { templateId } = req.params;
  try {
    const template = req.strategyRegistry.getTemplate(templateId);
    const isLightgbmModelTemplate = templateId.startsWith('lightgbm_');
    if (!template && !isLightgbmModelTemplate) {
      return res.redirect(`/templates?error=${encodeURIComponent(`Template ${templateId} not found`)}`);
    }

    const hasActiveRemoteJob = await req.db.remoteOptimizerJobs.hasActiveRemoteOptimizerJob(templateId);
    if (hasActiveRemoteJob) {
      return res.redirect(
        `/templates/${encodeURIComponent(templateId)}?error=${encodeURIComponent(
          'Stop remote optimization before deleting this template.'
        )}`
      );
    }

    if (isLightgbmModelTemplate) {
      const modelId = templateId.slice('lightgbm_'.length).trim();
      if (!modelId) {
        return res.redirect(
          `/templates/${encodeURIComponent(templateId)}?error=${encodeURIComponent('Invalid LightGBM template ID')}`
        );
      }

      await req.db.lightgbmModels.deleteLightgbmModel(modelId);
      await req.strategyRegistry.ensureLightgbmModelTemplates();

      const label = template?.name ?? templateId;
      return res.redirect(
        `/templates?success=${encodeURIComponent(`LightGBM model "${label}" deleted successfully`)}`
      );
    }

    if (!template) {
      return res.redirect(`/templates?error=${encodeURIComponent(`Template ${templateId} not found`)}`);
    }

    await req.db.templates.removeTemplatesByIds([templateId]);
    await req.strategyRegistry.disableTemplate(templateId);

    return res.redirect(
      `/templates?success=${encodeURIComponent(`Template "${template.name}" deleted successfully`)}`
    );
  } catch (error) {
    req.loggingService?.error?.('templates', 'Failed to delete template', {
      templateId,
      error: error instanceof Error ? error.message : String(error)
    });
    const message = error instanceof Error ? error.message : 'Failed to delete template';
    return res.redirect(
      `/templates/${encodeURIComponent(templateId)}?error=${encodeURIComponent(message)}`
    );
  }
});

router.post<TemplateParams>('/:templateId/remote-optimize', requireAuth, requireAdmin, async (req, res) => {
  const { templateId } = req.params;
  const acceptsHeader = req.headers?.accept;
  const acceptsJsonHeader = Array.isArray(acceptsHeader)
    ? acceptsHeader.some(header => typeof header === 'string' && header.includes('application/json'))
    : typeof acceptsHeader === 'string' && acceptsHeader.includes('application/json');
  const expectsJson = acceptsJsonHeader || Boolean(req.is('application/json'));
  const respondError = (status: number, message: string) => {
    if (expectsJson) {
      return res.status(status).json({ error: message });
    }
    return res.redirect(`/templates?error=${encodeURIComponent(message)}`);
  };

  try {
    const template = req.strategyRegistry.getTemplate(templateId);

    if (!template) {
      return respondError(404, `Template ${templateId} not found`);
    }

    if (!req.remoteOptimizerService) {
      return respondError(503, 'Remote optimization service is unavailable');
    }

    const job = await req.remoteOptimizerService.triggerOptimization({
      templateId,
      templateName: template.name ?? template.id,
      triggeredBy: {
        userId: req.user?.id ?? 'unknown',
        email: req.user?.email ?? 'unknown'
      }
    });

    if (expectsJson) {
      return res.json({
        ok: true,
        jobId: job.id,
        status: job.status,
        templateId: job.templateId
      });
    }

    const [rawPublicKey, rawKeyName] = await Promise.all([
      req.db.settings.getSettingValue(SETTING_KEYS.HETZNER_PUBLIC_KEY),
      req.db.settings.getSettingValue(SETTING_KEYS.HETZNER_SSH_KEY_NAME)
    ]);
    const sshKeyName = String(rawKeyName).trim();
    const publicKey = String(rawPublicKey).trim();
    const successMessage = `Remote optimizer queued (job ${job.id}). You'll receive an email when the server finishes. Ensure that public key "${publicKey}" is in "Hetzner -> Projects -> Security" with name "${sshKeyName}" before starting optimizers.`;
    return res.redirect(`/templates?success=${encodeURIComponent(successMessage)}`);
  } catch (error) {
    req.loggingService?.error?.('templates', 'Failed to trigger remote optimization', {
      templateId: req.params?.templateId,
      error: error instanceof Error ? error.message : String(error)
    });
    const message = error instanceof Error ? error.message : 'Failed to trigger remote optimization';
    return respondError(500, message);
  }
});

router.post<TemplateParams>('/:templateId/remote-optimize/stop', requireAuth, requireAdmin, async (req, res) => {
  const { templateId } = req.params;
  const { jobId } = req.body ?? {};
  const acceptsHeader = req.headers?.accept;
  const acceptsJsonHeader = Array.isArray(acceptsHeader)
    ? acceptsHeader.some(header => typeof header === 'string' && header.includes('application/json'))
    : typeof acceptsHeader === 'string' && acceptsHeader.includes('application/json');
  const expectsJson = acceptsJsonHeader || Boolean(req.is('application/json'));

  const respondError = (status: number, message: string) => {
    if (expectsJson) {
      return res.status(status).json({ error: message });
    }
    return res.redirect(`/templates?error=${encodeURIComponent(message)}`);
  };

  try {
    if (!req.remoteOptimizerService || typeof req.remoteOptimizerService.stopOptimization !== 'function') {
      return respondError(503, 'Remote optimization service is unavailable');
    }

    if (typeof jobId !== 'string' || jobId.trim().length === 0) {
      return respondError(400, 'A valid remote optimization job ID is required to stop optimization');
    }

    const jobSnapshot: RemoteOptimizationJobSnapshot | undefined = await req.remoteOptimizerService.getJob(jobId);
    if (!jobSnapshot || jobSnapshot.templateId !== templateId) {
      return respondError(404, `Remote optimization job ${jobId} not found for template ${templateId}`);
    }

    const result = await req.remoteOptimizerService.stopOptimization(jobId);

    if (expectsJson) {
      return res.json({
        ok: true,
        jobId: result.job.id,
        templateId: result.job.templateId,
        serverDeleted: result.serverDeleted
      });
    }

    const successMessage = result.serverDeleted
      ? `Remote optimization server terminated (job ${result.job.id}).`
      : `Remote optimization job ${result.job.id} marked as stopped.`;
    return res.redirect(`/templates?success=${encodeURIComponent(successMessage)}`);
  } catch (error) {
    req.loggingService?.error?.('templates', 'Failed to stop remote optimization', {
      templateId: req.params?.templateId,
      jobId,
      error: error instanceof Error ? error.message : String(error)
    });
    const message = error instanceof Error ? error.message : 'Failed to stop remote optimization';
    return respondError(500, message);
  }
});

export default router;
