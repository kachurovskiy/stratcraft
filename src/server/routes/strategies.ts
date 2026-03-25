import express, { NextFunction, Request, Response } from 'express';
import { StrategyIdParams, StrategyParams, StrategyTickerParams } from '../types/Express';
import {
  StrategyTemplate,
  StrategyParameter,
  Candle,
  AccountOperationStatus,
  AccountOperationType
} from '../types/StrategyTemplate';
import { AccountSnapshot } from '../types/Account';
import { LogEntry, LogLevel } from '../services/LoggingService';
import type { AccountSignalSkipRow } from '../database/types';
import { SETTING_KEYS } from '../constants';
import {
  BACKTEST_SCOPE_META,
  buildBacktestComparisonView,
  normalizeBacktestScope
} from '../scoring/backtestComparison';
import { formatSignalSkipReason } from '../utils/skipReasonFormatting';
import { resolveBacktestInitialCapitalSetting, resolveStrategyInitialCapital } from '../utils/initialCapital';
import { buildParameterContexts, formatParameterValue } from '../utils/parameters';
import backtestRoutes from './backtest';
import { getReqUserId, formatBacktestPeriodLabel, parsePageParam } from './utils';

const router = express.Router();

const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

const getSnapshotBadgeMeta = (snapshot?: AccountSnapshot) => {
  if (!snapshot) {
    return { label: 'Pending', variant: 'secondary' };
  }
  switch (snapshot.status) {
    case 'ready':
      return { label: 'Live', variant: 'success' };
    case 'unsupported':
      return { label: 'Manual', variant: 'warning' };
    default:
      return { label: 'Error', variant: 'danger' };
  }
};

const STRATEGY_OPTIONAL_SECTION_TIMEOUT_MS = 3_000;
const STRATEGY_ACCOUNT_TIMEOUT_MS = 4_000;
const STRATEGY_BACKTEST_COMPARISON_TIMEOUT_MS = 4_000;

const ACCOUNT_OPERATION_STATUS_ORDER: AccountOperationStatus[] = ['pending', 'sent', 'skipped', 'failed'];

const ACCOUNT_OPERATION_STATUS_META: Record<AccountOperationStatus, { label: string; badge: string }> = {
  pending: { label: 'Pending', badge: 'warning' },
  sent: { label: 'Sent', badge: 'success' },
  skipped: { label: 'Skipped', badge: 'secondary' },
  failed: { label: 'Failed', badge: 'danger' }
};

const loadStrategyOrRenderNotFound = async (req: Request, res: Response, strategyId: string) => {
  const userId = getReqUserId(req);
  const strategy = await req.db.strategies.getStrategy(strategyId, userId);

  if (!strategy) {
    res.status(404).render('pages/error', {
      title: 'Strategy Not Found',
      error: `Strategy ${strategyId} not found`
    });
    return null;
  }

  return strategy;
};

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

type StrategyOperationStatusFilter = 'all' | AccountOperationStatus;
type StrategyOperationTypeFilter = 'all' | AccountOperationType;
type StrategyOperationSortField = 'triggeredAt' | 'statusUpdatedAt' | 'createdAt';

const STRATEGY_OPERATION_PAGE_SIZE_OPTIONS = [50, 100, 250, 500, 1000];
const DEFAULT_STRATEGY_OPERATION_PAGE_SIZE = 100;

const STRATEGY_OPERATION_SORT_LABELS: Record<StrategyOperationSortField, string> = {
  triggeredAt: 'Triggered Time',
  statusUpdatedAt: 'Status Updated',
  createdAt: 'Created Time'
};

const STRATEGY_OPERATION_TYPE_LABELS: Record<AccountOperationType, string> = {
  open_position: 'Open Position',
  close_position: 'Close Position',
  update_stop_loss: 'Update Stop Loss'
};

type StrategyLogMetadataPair = {
  label: string;
  value: string;
};

interface StrategyLogView {
  levelLabel: string;
  levelBadgeClass: string;
  source: string;
  message: string;
  createdAt?: Date;
  metadataPairs: StrategyLogMetadataPair[];
  metadataByLabel: Record<string, string>;
}

type StrategyAccountSummary = {
  id: string;
  name: string;
  provider: string;
  environment: string;
  balance: number | null;
  hasBalance: boolean;
  currency: string | null;
  snapshotBadgeLabel: string;
  snapshotBadgeVariant: string;
  snapshotMessage: string | null;
  snapshotFetchedAt: Date | null;
};

type StrategyAccountLoadResult = {
  strategyAccount: StrategyAccountSummary | null;
  strategyAccountWarning: string | null;
};

const LOG_LEVEL_BADGES: Record<LogLevel, string> = {
  error: 'bg-danger',
  warn: 'bg-warning text-dark',
  info: 'bg-info text-dark',
  debug: 'bg-secondary'
};

const IGNORED_LOG_METADATA_KEYS = new Set(['strategyId']);

const loadOptionalStrategySection = async <T>({
  strategyId,
  label,
  fallback,
  load,
  timeoutMs = STRATEGY_OPTIONAL_SECTION_TIMEOUT_MS
}: {
  strategyId: string;
  label: string;
  fallback: T;
  load: () => Promise<T>;
  timeoutMs?: number;
}): Promise<T> => {
  let timeoutHandle: NodeJS.Timeout | null = null;

  try {
    return await Promise.race([
      load(),
      new Promise<T>((_, reject) => {
        timeoutHandle = setTimeout(() => {
          reject(new Error(`timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      })
    ]);
  } catch (error) {
    console.warn(`Strategy overview ${label} failed for ${strategyId}:`, error);
    return fallback;
  } finally {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
  }
};

const humanizeMetadataKey = (key: string): string => {
  if (!key) {
    return 'Detail';
  }
  return key
    .replace(/_/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/^./, (char) => char.toUpperCase());
};

const formatMetadataValue = (value: unknown): string => {
  if (value === null || value === undefined || value === '') {
    return '—';
  }
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value.toString() : String(value);
  }
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (Array.isArray(value)) {
    return value.map(formatMetadataValue).join(', ');
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return '[object]';
    }
  }
  return String(value);
};

const buildLogMetadataPairs = (metadata?: Record<string, any>): StrategyLogMetadataPair[] => {
  if (!metadata || typeof metadata !== 'object') {
    return [];
  }
  return Object.entries(metadata)
    .filter(([key]) => !IGNORED_LOG_METADATA_KEYS.has(key))
    .map(([key, value]) => ({
      label: humanizeMetadataKey(key),
      value: formatMetadataValue(value)
    }))
    .filter((pair) => pair.value && pair.value.length > 0);
};

const buildStrategyLogView = (log: LogEntry): StrategyLogView => {
  const level = (log.level ?? 'info') as LogLevel;
  const metadataPairs = buildLogMetadataPairs(log.metadata);
  const metadataByLabel = metadataPairs.reduce<Record<string, string>>((acc, pair) => {
    acc[pair.label] = pair.value;
    return acc;
  }, {});
  return {
    levelLabel: level.toUpperCase(),
    levelBadgeClass: LOG_LEVEL_BADGES[level as LogLevel] ?? 'bg-secondary',
    source: log.source ?? 'system',
    message: log.message,
    createdAt: log.created_at,
    metadataPairs,
    metadataByLabel
  };
};

function normalizeOperationStatusFilter(raw: unknown): StrategyOperationStatusFilter {
  if (typeof raw !== 'string') {
    return 'all';
  }
  const normalized = raw.toLowerCase();
  if (normalized === 'pending' || normalized === 'sent' || normalized === 'skipped' || normalized === 'failed') {
    return normalized as AccountOperationStatus;
  }
  return 'all';
}

function normalizeOperationTypeFilter(raw: unknown): StrategyOperationTypeFilter {
  if (typeof raw !== 'string') {
    return 'all';
  }
  const normalized = raw.toLowerCase();
  if (
    normalized === 'open_position' ||
    normalized === 'close_position' ||
    normalized === 'update_stop_loss'
  ) {
    return normalized as AccountOperationType;
  }
  return 'all';
}

function normalizeOperationSortField(raw: unknown): StrategyOperationSortField {
  if (raw === 'statusUpdatedAt' || raw === 'createdAt' || raw === 'triggeredAt') {
    return raw as StrategyOperationSortField;
  }
  return 'triggeredAt';
}

function normalizeOperationSortDirection(raw: unknown): 'asc' | 'desc' {
  if (typeof raw === 'string' && raw.toLowerCase() === 'asc') {
    return 'asc';
  }
  return 'desc';
}

function normalizeOperationTickerFilter(raw: unknown): string | undefined {
  if (typeof raw !== 'string') {
    return undefined;
  }
  const value = raw.trim().toUpperCase();
  return value.length > 0 ? value : undefined;
}

function normalizeTickerParam(raw: unknown): string | null {
  if (typeof raw !== 'string') {
    return null;
  }
  const value = raw.trim().toUpperCase();
  return value.length > 0 ? value : null;
}

function normalizeOperationPageSize(raw: unknown): number {
  const parsed = typeof raw === 'string' ? Number.parseInt(raw, 10) : Number(raw);
  if (Number.isFinite(parsed) && STRATEGY_OPERATION_PAGE_SIZE_OPTIONS.includes(parsed)) {
    return parsed;
  }
  return DEFAULT_STRATEGY_OPERATION_PAGE_SIZE;
}

function parseOperationDate(raw: unknown, mode: 'start' | 'end'): Date | undefined {
  if (typeof raw !== 'string') {
    return undefined;
  }
  const trimmed = raw.trim();
  if (!trimmed) {
    return undefined;
  }
  let parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime()) && /^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const suffix = mode === 'end' ? 'T23:59:59.999Z' : 'T00:00:00Z';
    parsed = new Date(`${trimmed}${suffix}`);
  }
  if (Number.isNaN(parsed.getTime())) {
    return undefined;
  }
  return parsed;
}

type SkipSourceKey = 'backtest' | 'planOperations';

type SkipReasonAggregate = {
  reason: string;
  details: string | null;
  count: number;
};

type SkipCellAggregate = {
  total: number;
  reasons: Map<string, SkipReasonAggregate>;
};

type SkipCellView = {
  total: number;
  hasItems: boolean;
  text: string;
};

type SkipRowView = {
  ticker: string;
  action: string;
  actionLabel: string;
  actionBadge: string;
  backtest: SkipCellView;
  planOperations: SkipCellView;
};

type SkipSummaryRowView = {
  label: string;
  backtest: number;
  planOperations: number;
};

type SkipSummaryView = {
  hasSignals: boolean;
  hasReasons: boolean;
  signals: SkipSummaryRowView[];
  reasons: SkipSummaryRowView[];
};

const SKIP_SOURCE_META: Record<SkipSourceKey, { label: string; source: string }> = {
  backtest: { label: 'Engine backtest', source: 'backtest' },
  planOperations: { label: 'Operation planning', source: 'plan_operations' }
};

const ACTION_META: Record<string, { label: string; badge: string }> = {
  buy: { label: 'Buy', badge: 'success' },
  sell: { label: 'Sell', badge: 'danger' }
};

const resolveSignalActionMeta = (action: string | null | undefined): { label: string; badge: string } => {
  const normalized = typeof action === 'string' ? action.toLowerCase() : '';
  if (ACTION_META[normalized]) {
    return ACTION_META[normalized];
  }
  if (!normalized) {
    return { label: 'Unknown', badge: 'secondary' };
  }
  return { label: normalized.toUpperCase(), badge: 'secondary' };
};

const SKIP_SOURCE_VIEW: Record<string, { label: string; badge: string }> = {
  [SKIP_SOURCE_META.backtest.source]: { label: SKIP_SOURCE_META.backtest.label, badge: 'secondary' },
  [SKIP_SOURCE_META.planOperations.source]: { label: SKIP_SOURCE_META.planOperations.label, badge: 'info' }
};


const SKIP_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

const normalizeSkipDate = (raw: unknown): string | null => {
  if (typeof raw !== 'string') {
    return null;
  }
  const trimmed = raw.trim();
  if (!trimmed || !SKIP_DATE_PATTERN.test(trimmed)) {
    return null;
  }
  const parsed = new Date(`${trimmed}T00:00:00Z`);
  return Number.isNaN(parsed.getTime()) ? null : trimmed;
};

const buildSkipReasonLine = (reason: SkipReasonAggregate): string => {
  const { label, detail } = formatSignalSkipReason(reason.reason, reason.details);
  let text = label;
  if (detail) {
    text += ` (${detail})`;
  }
  if (reason.count > 1) {
    text += ` x${reason.count}`;
  }
  return text;
};

const buildSkipCellView = (aggregate: SkipCellAggregate): SkipCellView => {
  const reasons = Array.from(aggregate.reasons.values()).sort((a, b) => {
    const reasonDiff = a.reason.localeCompare(b.reason);
    if (reasonDiff !== 0) {
      return reasonDiff;
    }
    return (a.details ?? '').localeCompare(b.details ?? '');
  });
  const text = reasons.map((reason) => buildSkipReasonLine(reason)).join(' | ');

  return {
    total: aggregate.total,
    hasItems: reasons.length > 0,
    text
  };
};

const buildSkipComparisonRows = (
  skips: AccountSignalSkipRow[]
): { rows: SkipRowView[]; totals: Record<SkipSourceKey, number>; summary: SkipSummaryView } => {
  const createCell = (): SkipCellAggregate => ({
    total: 0,
    reasons: new Map()
  });
  const rows = new Map<string, { ticker: string; action: string; backtest: SkipCellAggregate; planOperations: SkipCellAggregate }>();

  const sourceMap: Record<string, SkipSourceKey | undefined> = {
    backtest: 'backtest',
    plan_operations: 'planOperations'
  };

  for (const skip of skips) {
    const sourceKey = sourceMap[String(skip.source ?? '').toLowerCase()];
    if (!sourceKey) {
      continue;
    }
    const ticker = skip.ticker;
    const action = typeof skip.action === 'string' ? skip.action.toLowerCase() : '';
    const rowKey = `${ticker}|${action}`;
    let row = rows.get(rowKey);
    if (!row) {
      row = { ticker, action, backtest: createCell(), planOperations: createCell() };
      rows.set(rowKey, row);
    }

    const cell = row[sourceKey];
    cell.total += 1;
    const details = typeof skip.details === 'string' && skip.details.length > 0 ? skip.details : null;
    const reasonKey = `${skip.reason}|${details ?? ''}`;
    let reasonAgg = cell.reasons.get(reasonKey);
    if (!reasonAgg) {
      reasonAgg = {
        reason: skip.reason,
        details,
        count: 0
      };
      cell.reasons.set(reasonKey, reasonAgg);
    }
    reasonAgg.count += 1;
  }

  const totals: Record<SkipSourceKey, number> = {
    backtest: 0,
    planOperations: 0
  };

  const summarySignalCounts: Record<SkipSourceKey, { total: number; buy: number; sell: number; other: number }> = {
    backtest: { total: 0, buy: 0, sell: 0, other: 0 },
    planOperations: { total: 0, buy: 0, sell: 0, other: 0 }
  };
  const summaryReasonCounts: Record<SkipSourceKey, Map<string, number>> = {
    backtest: new Map(),
    planOperations: new Map()
  };

  const rowViews: SkipRowView[] = [];
  const actionSortOrder = (value: string) => (value === 'buy' ? 0 : value === 'sell' ? 1 : 2);
  const sortedRows = Array.from(rows.values()).sort((a, b) => {
    const tickerDiff = a.ticker.localeCompare(b.ticker);
    if (tickerDiff !== 0) {
      return tickerDiff;
    }
    return actionSortOrder(a.action) - actionSortOrder(b.action);
  });

  const hasOnlyReason = (cell: SkipCellAggregate, reason: string): boolean => {
    if (cell.total === 0) {
      return false;
    }
    return Array.from(cell.reasons.values()).every((entry) => entry.reason === reason);
  };

  const addSignalCounts = (sourceKey: SkipSourceKey, action: string, count: number) => {
    if (count <= 0) {
      return;
    }
    const bucket = summarySignalCounts[sourceKey];
    bucket.total += count;
    if (action === 'buy') {
      bucket.buy += count;
    } else if (action === 'sell') {
      bucket.sell += count;
    } else {
      bucket.other += count;
    }
  };

  const addReasonCounts = (sourceKey: SkipSourceKey, cell: SkipCellAggregate) => {
    const bucket = summaryReasonCounts[sourceKey];
    for (const entry of cell.reasons.values()) {
      const current = bucket.get(entry.reason) ?? 0;
      bucket.set(entry.reason, current + entry.count);
    }
  };

  const resolveActionMeta = (action: string): { label: string; badge: string } => {
    if (action === 'buy') {
      return { label: 'Buy', badge: 'success' };
    }
    if (action === 'sell') {
      return { label: 'Sell', badge: 'danger' };
    }
    if (!action) {
      return { label: 'Unknown', badge: 'secondary' };
    }
    return { label: action.toUpperCase(), badge: 'secondary' };
  };

  for (const row of sortedRows) {
    const shouldOmit =
      hasOnlyReason(row.backtest, 'sell_no_active_position') &&
      hasOnlyReason(row.planOperations, 'sell_no_active_position');
    if (shouldOmit) {
      continue;
    }

    const backtestView = buildSkipCellView(row.backtest);
    const planView = buildSkipCellView(row.planOperations);
    if (!backtestView.hasItems && !planView.hasItems) {
      continue;
    }

    totals.backtest += row.backtest.total;
    totals.planOperations += row.planOperations.total;
    addSignalCounts('backtest', row.action, row.backtest.total);
    addSignalCounts('planOperations', row.action, row.planOperations.total);
    addReasonCounts('backtest', row.backtest);
    addReasonCounts('planOperations', row.planOperations);

    const actionMeta = resolveActionMeta(row.action);
    rowViews.push({
      ticker: row.ticker,
      action: row.action,
      actionLabel: actionMeta.label,
      actionBadge: actionMeta.badge,
      backtest: backtestView,
      planOperations: planView
    });
  }

  const signalRows: SkipSummaryRowView[] = [
    {
      label: 'Total signals',
      backtest: summarySignalCounts.backtest.total,
      planOperations: summarySignalCounts.planOperations.total
    },
    {
      label: 'Buy signals',
      backtest: summarySignalCounts.backtest.buy,
      planOperations: summarySignalCounts.planOperations.buy
    },
    {
      label: 'Sell signals',
      backtest: summarySignalCounts.backtest.sell,
      planOperations: summarySignalCounts.planOperations.sell
    }
  ];

  if (summarySignalCounts.backtest.other > 0 || summarySignalCounts.planOperations.other > 0) {
    signalRows.push({
      label: 'Other signals',
      backtest: summarySignalCounts.backtest.other,
      planOperations: summarySignalCounts.planOperations.other
    });
  }

  const reasonKeys = new Set<string>();
  for (const key of summaryReasonCounts.backtest.keys()) {
    reasonKeys.add(key);
  }
  for (const key of summaryReasonCounts.planOperations.keys()) {
    reasonKeys.add(key);
  }

  const reasonRows = Array.from(reasonKeys)
    .map((reason) => {
      const backtestCount = summaryReasonCounts.backtest.get(reason) ?? 0;
      const planCount = summaryReasonCounts.planOperations.get(reason) ?? 0;
      const { label } = formatSignalSkipReason(reason, null);
      return {
        label,
        backtest: backtestCount,
        planOperations: planCount,
        total: backtestCount + planCount
      };
    })
    .sort((a, b) => {
      if (b.total !== a.total) {
        return b.total - a.total;
      }
      return a.label.localeCompare(b.label);
    })
    .map(({ label, backtest, planOperations }) => ({ label, backtest, planOperations }));

  return {
    rows: rowViews,
    totals,
    summary: {
      hasSignals: signalRows.some((row) => row.backtest > 0 || row.planOperations > 0),
      hasReasons: reasonRows.length > 0,
      signals: signalRows,
      reasons: reasonRows
    }
  };
};

function buildOperationsPageUrl(
  basePath: string,
  baseQuery: Record<string, string>,
  overrides: Record<string, string | number | undefined> = {}
) {
  const merged: Record<string, string> = { ...baseQuery };
  Object.entries(overrides).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') {
      delete merged[key];
      return;
    }
    merged[key] = String(value);
  });

  const params = new URLSearchParams();
  Object.entries(merged).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      params.set(key, value);
    }
  });

  const queryString = params.toString();
  return queryString ? `${basePath}?${queryString}` : basePath;
}

function buildStrategySignalDays(
  signals: Array<{ date: Date; ticker: string; strategyId: string; action: 'buy' | 'sell'; confidence: number | null }>,
  days: number = 7
) {
  const groupedByDate = new Map<string, Array<{ date: Date; ticker: string; action: 'buy' | 'sell'; confidence: number | null }>>();

  for (const signal of signals) {
    const dateKey = signal.date.toISOString().split('T')[0];
    if (!groupedByDate.has(dateKey)) {
      groupedByDate.set(dateKey, []);
    }
    groupedByDate.get(dateKey)!.push({
      date: signal.date,
      ticker: signal.ticker,
      action: signal.action,
      confidence: signal.confidence ?? null
    });
  }

  const actionSortOrder = (action: string) => {
    if (action === 'buy') return 0;
    if (action === 'sell') return 1;
    return 2;
  };

  const dayEntries = Array.from(groupedByDate.entries())
    .map(([isoDate, daySignals]) => {
      const actionGroupMap = new Map<
        string,
        Map<
          string,
          {
            confidence: number | null;
            confidenceDescriptor: string;
            tickers: string[];
          }
        >
      >();

      for (const sig of daySignals) {
        const action = (sig.action === 'sell' ? 'sell' : 'buy') as 'buy' | 'sell';
        if (!actionGroupMap.has(action)) {
          actionGroupMap.set(action, new Map());
        }
        const confidenceMap = actionGroupMap.get(action)!;

        const hasConfidence =
          sig.confidence !== null &&
          sig.confidence !== undefined &&
          typeof sig.confidence === 'number' &&
          Number.isFinite(sig.confidence);
        const numericConfidence = hasConfidence ? Number(sig.confidence) : null;
        const confidenceKey = numericConfidence !== null ? numericConfidence.toFixed(2) : 'none';
        const confidenceDescriptor =
          numericConfidence !== null ? `Confidence ${numericConfidence.toFixed(2)}` : 'No confidence score';

        if (!confidenceMap.has(confidenceKey)) {
          confidenceMap.set(confidenceKey, {
            confidence: numericConfidence,
            confidenceDescriptor,
            tickers: []
          });
        }

        const group = confidenceMap.get(confidenceKey)!;
        const ticker = sig.ticker.toUpperCase();
        if (!group.tickers.includes(ticker)) {
          group.tickers.push(ticker);
        }
      }

      const buildActionLabel = (action: string) => (action === 'sell' ? 'Sell Signals' : 'Buy Signals');
      const buildActionBadge = (action: string) => (action === 'sell' ? 'danger' : 'success');

      const actionSummaries = Array.from(actionGroupMap.entries())
        .map(([action, confidenceMap]) => {
          const groups = Array.from(confidenceMap.values())
            .map(group => {
              group.tickers.sort((a, b) => a.localeCompare(b));
              const tickerCount = group.tickers.length;
              return {
                confidence: group.confidence,
                confidenceDescriptor: group.confidenceDescriptor,
                confidenceDisplay: group.confidence !== null ? group.confidence.toFixed(2) : 'N/A',
                tickers: group.tickers,
                tickerCount,
                tickersLabel: group.tickers.join(', ')
              };
            })
            .sort((a, b) => {
              if (a.confidence === null) return 1;
              if (b.confidence === null) return -1;
              return b.confidence - a.confidence;
            });

          const totalTickers = groups.reduce((sum, group) => sum + group.tickerCount, 0);

          return {
            action,
            actionLabel: buildActionLabel(action),
            badgeClass: buildActionBadge(action),
            totalTickers,
            groups
          };
        })
        .filter(summary => summary.totalTickers > 0)
        .sort((a, b) => actionSortOrder(a.action) - actionSortOrder(b.action));

      const uniqueTickers = Array.from(new Set(daySignals.map(sig => sig.ticker.toUpperCase()))).sort(
        (a, b) => a.localeCompare(b)
      );

      const signalCount = daySignals.length;
      const uniqueTickerCount = uniqueTickers.length;
      const summaryLine = `${signalCount} signal${signalCount === 1 ? '' : 's'} • ${uniqueTickerCount} ticker${uniqueTickerCount === 1 ? '' : 's'}`;

      return {
        date: new Date(`${isoDate}T00:00:00Z`),
        isoDate,
        signalCount,
        uniqueTickerCount,
        uniqueTickers,
        uniqueTickersList: uniqueTickers.join(', '),
        summaryLine,
        actionSummaries
      };
    })
    .filter(day => day.signalCount > 0 && day.actionSummaries.length > 0)
    .sort((a, b) => b.isoDate.localeCompare(a.isoDate))
    .slice(0, days);

  return dayEntries;
}

// Create Strategy
router.get('/strategies/create', requireAuth, async (req: Request, res: Response) => {
  try {
    const templateIdParam = Array.isArray(req.query.template) ? req.query.template[0] : req.query.template;
    const paramsParamRaw = req.query.params;
    const paramsParam =
      typeof paramsParamRaw === 'string'
        ? paramsParamRaw
        : Array.isArray(paramsParamRaw) && typeof paramsParamRaw[0] === 'string'
          ? paramsParamRaw[0]
          : undefined;
    const accountIdParam = Array.isArray(req.query.accountId) ? req.query.accountId[0] : req.query.accountId;
    const availableTemplates = req.strategyRegistry.getTemplates();
    const tenYearPeriodMonths = 120;
    const requestedTemplateId = typeof templateIdParam === 'string' ? templateIdParam.trim() : '';
    let selectedTemplateId = requestedTemplateId;

    if (!selectedTemplateId) {
      const bestTenYearTemplate = await req.db.backtestCache.getBestTemplateBySharpe({ periodMonths: tenYearPeriodMonths });
      if (bestTenYearTemplate?.templateId) {
        selectedTemplateId = bestTenYearTemplate.templateId;
      } else {
        const bestAnyTemplate = await req.db.backtestCache.getBestTemplateBySharpe();
        if (bestAnyTemplate?.templateId) {
          selectedTemplateId = bestAnyTemplate.templateId;
        }
      }
    }

    if (!selectedTemplateId && availableTemplates.length > 0) {
      selectedTemplateId = availableTemplates[0].id;
    }

    let template = selectedTemplateId
      ? (req.strategyRegistry.getTemplate(selectedTemplateId, { includeDisabled: false }) as StrategyTemplate | undefined)
      : undefined;

    if (!template) {
      if (requestedTemplateId) {
        return res.status(404).render('pages/error', {
          title: 'Template Not Found',
          error: `Strategy template ${requestedTemplateId} was not found.`
        });
      }

      if (availableTemplates.length === 0) {
        return res.status(404).render('pages/error', {
          title: 'Template Not Found',
          error: 'No strategy templates are available.'
        });
      }

      const fallbackTemplate = availableTemplates[0];
      template = fallbackTemplate;
      selectedTemplateId = fallbackTemplate.id;
    }

    if (!template) {
      return res.status(500).render('pages/error', {
        title: 'Template Not Found',
        error: 'Unable to load a strategy template.'
      });
    }

    const safeTemplate: StrategyTemplate = template;

    let parameterSource: 'url' | 'best' | 'saved' | 'default' = 'default';
    let initialParameters: Record<string, any> = {};
    let bestBacktestSummary: {
      sharpeRatio: number | null;
      calmarRatio: number | null;
      totalReturn: number | null;
      cagr: number | null;
      maxDrawdownRatio: number | null;
      winRate: number | null;
      totalTrades: number | null;
      hasSharpeRatio: boolean;
      hasCalmarRatio: boolean;
      hasTotalReturn: boolean;
      hasCagr: boolean;
      hasMaxDrawdown: boolean;
      hasWinRate: boolean;
      hasTotalTrades: boolean;
    } | null = null;

    let urlParameterOverrides: Record<string, any> = {};

    if (paramsParam) {
      try {
        urlParameterOverrides = JSON.parse(paramsParam);
      } catch (error) {
        console.error('Error parsing params query for create strategy page:', error);
      }
    }

    const best = await req.db.backtestCache.getBestParams(safeTemplate.id);
    if (best && best.parameters) {
      initialParameters = { ...best.parameters };
      parameterSource = 'best';
      bestBacktestSummary = {
        sharpeRatio: typeof best.sharpeRatio === 'number' ? best.sharpeRatio : null,
        calmarRatio: typeof best.calmarRatio === 'number' ? best.calmarRatio : null,
        totalReturn: typeof best.totalReturn === 'number' ? best.totalReturn : null,
        cagr: typeof best.cagr === 'number' ? best.cagr : null,
        maxDrawdownRatio: typeof best.maxDrawdownRatio === 'number' ? best.maxDrawdownRatio : null,
        winRate: typeof best.winRate === 'number' ? best.winRate : null,
        totalTrades: typeof best.totalTrades === 'number' ? best.totalTrades : null,
        hasSharpeRatio: typeof best.sharpeRatio === 'number',
        hasCalmarRatio: typeof best.calmarRatio === 'number',
        hasTotalReturn: typeof best.totalReturn === 'number',
        hasCagr: typeof best.cagr === 'number',
        hasMaxDrawdown: typeof best.maxDrawdownRatio === 'number',
        hasWinRate: typeof best.winRate === 'number',
        hasTotalTrades: typeof best.totalTrades === 'number'
      };
    } else {
      const savedBestParams = await req.db.backtestBestParams.getSavedBestParams(safeTemplate.id);
      if (savedBestParams && typeof savedBestParams === 'object' && !Array.isArray(savedBestParams)) {
        initialParameters = { ...savedBestParams };
        parameterSource = 'saved';
      }
    }

    if (Object.keys(initialParameters).length === 0) {
      for (const param of safeTemplate.parameters) {
        if (param.default !== undefined) {
          initialParameters[param.name] = param.default;
        }
      }
    }

    if (Object.keys(urlParameterOverrides).length > 0) {
      initialParameters = {
        ...initialParameters,
        ...urlParameterOverrides
      };
      parameterSource = 'url';
    }

    const parameters = safeTemplate.parameters.map((param: StrategyParameter) => {
      const rawValue = Object.prototype.hasOwnProperty.call(initialParameters, param.name)
        ? initialParameters[param.name]
        : param.default;
      const valueString = formatParameterValue(rawValue, { type: param.type, emptyLabel: '' });

      return {
        ...param,
        inputValue: valueString,
        valueString,
        hasMin: param.min !== undefined,
        hasMax: param.max !== undefined,
        hasStep: param.step !== undefined
      };
    });
    const hasParameters = parameters.length > 0;

    let prefillNotice = '';
    if (parameterSource === 'url') {
      prefillNotice = 'Parameters pre-filled from shared configuration.';
    } else if (parameterSource === 'best' && bestBacktestSummary) {
      prefillNotice = 'Parameters pre-filled from the best known backtest for this template.';
    } else if (parameterSource === 'saved') {
      prefillNotice = 'Parameters pre-filled from the saved best params snapshot.';
    } else {
      prefillNotice = 'No optimized parameters found; using template defaults.';
    }

    const bestBacktestHasMetrics = Boolean(
      bestBacktestSummary &&
      (bestBacktestSummary.hasSharpeRatio ||
        bestBacktestSummary.hasCalmarRatio ||
        bestBacktestSummary.hasTotalReturn ||
        bestBacktestSummary.hasCagr ||
        bestBacktestSummary.hasMaxDrawdown ||
        bestBacktestSummary.hasWinRate ||
        bestBacktestSummary.hasTotalTrades)
    );

    const userId = getReqUserId(req);
    const rawAccounts = await req.db.accounts.getAccountsForUser(userId);
    const requestedAccountId = typeof accountIdParam === 'string' ? accountIdParam.trim() : '';
    let accountsForSelection: Array<{
      id: string;
      name: string;
      provider: string;
      environment: string;
      balance: number | null;
      currency: string | null;
      hasBalance: boolean;
    }> = [];

    if (rawAccounts.length > 0) {
      const snapshotMap = await req.accountDataService.fetchSnapshots(rawAccounts);
      accountsForSelection = rawAccounts.map((account: any) => {
        const snapshot = snapshotMap[account.id];
        const balance = typeof snapshot?.balance === 'number' && Number.isFinite(snapshot.balance)
          ? snapshot.balance
          : null;
        return {
          id: account.id,
          name: account.name,
          provider: account.provider,
          environment: account.environment,
          balance,
          currency: snapshot?.currency ?? null,
          hasBalance: balance !== null
        };
      });
    }

    let selectedAccountId = '';
    let selectedAccountForPrefill: {
      id: string;
      name: string;
      provider: string;
      environment: string;
      balance: number | null;
      currency: string | null;
      hasBalance: boolean;
    } | null = null;

    if (requestedAccountId) {
      selectedAccountForPrefill =
        accountsForSelection.find((account) => account.id === requestedAccountId) ?? null;
      if (selectedAccountForPrefill) {
        selectedAccountId = selectedAccountForPrefill.id;
      }
    }

    const strategyNamePrefill = selectedAccountForPrefill
      ? `${selectedAccountForPrefill.name} ${safeTemplate.name}`.trim()
      : '';

    const buildTemplateSwitchHref = (targetTemplateId: string) => {
      const params = new URLSearchParams();
      Object.entries(req.query).forEach(([key, value]) => {
        if (key === 'template') {
          return;
        }
        if (Array.isArray(value)) {
          value.forEach(v => {
            if (v !== undefined && v !== null) {
              params.append(key, String(v));
            }
          });
        } else if (value !== undefined && value !== null) {
          params.set(key, String(value));
        }
      });
      params.set('template', targetTemplateId);
      const queryString = params.toString();
      return queryString ? `${req.path}?${queryString}` : req.path;
    };

    const templateOptions = availableTemplates.map((templateOption: StrategyTemplate) => ({
      id: templateOption.id,
      name: templateOption.name,
      href: buildTemplateSwitchHref(templateOption.id),
      selected: templateOption.id === safeTemplate.id
    }));
    const hideInitialCapital = Boolean(selectedAccountId);

    res.render('pages/create-strategy', {
      title: 'Create Strategy',
      page: 'templates',
      user: req.user,
      template: safeTemplate,
      parameters,
      hasParameters,
      parameterSource,
      prefillNotice,
      bestBacktestSummary,
      bestBacktestHasMetrics,
      accounts: accountsForSelection,
      hasAccounts: accountsForSelection.length > 0,
      selectedAccountId,
      hideInitialCapital,
      strategyNamePrefill,
      templateOptions
    });
  } catch (error) {
    console.error('Error rendering create strategy page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load create strategy page'
    });
  }
});

// Create Strategy (POST)
router.post('/strategies/create', requireAuth, async (req: Request, res: Response) => {
  try {
    const { name, templateId, startDate, accountId, ...parameters } = req.body;

    if (!name || !templateId) {
      return res.status(400).render('pages/error', {
        title: 'Error',
        error: 'Name and template are required'
      });
    }

    const template = req.strategyRegistry.getTemplate(templateId, { includeDisabled: false });
    if (!template) {
      return res.status(404).render('pages/error', {
        title: 'Error',
        error: `Strategy template ${templateId} was not found.`
      });
    }

    let parsedParameters: Record<string, any>;
    try {
      parsedParameters = req.strategyRegistry.parseParameters(template, parameters);
    } catch (parseError: any) {
      return res.status(400).render('pages/error', {
        title: 'Error',
        error: parseError?.message || 'Invalid strategy parameters supplied.'
      });
    }

    const sanitizedAccountId = typeof accountId === 'string' ? accountId.trim() : '';
    const selectedAccountId = sanitizedAccountId.length > 0 ? sanitizedAccountId : null;
    const userId = getReqUserId(req);

    let requestedStartDate: Date | null = null;
    if (typeof startDate === 'string' && startDate.trim() !== '') {
      const trimmedStartDate = startDate.trim();
      const parsedDate = new Date(`${trimmedStartDate}T00:00:00Z`);
      if (Number.isNaN(parsedDate.getTime())) {
        return res.status(400).render('pages/error', {
          title: 'Error',
          error: 'Start date must be a valid calendar date (YYYY-MM-DD).'
        });
      }
      const today = new Date();
      if (parsedDate.getTime() > today.getTime()) {
        return res.status(400).render('pages/error', {
          title: 'Error',
          error: 'Start date cannot be in the future.'
        });
      }
      requestedStartDate = parsedDate;
    }

    let backtestStartDate: Date | null = requestedStartDate;

    if (selectedAccountId) {
      const account = await req.db.accounts.getAccountById(selectedAccountId, userId);
      if (!account) {
        return res.status(404).render('pages/error', {
          title: 'Error',
          error: 'The selected account could not be found.'
        });
      }

      const snapshotMap = await req.accountDataService.fetchSnapshots([account]);
      const snapshot = snapshotMap[selectedAccountId];
      const accountCash = typeof snapshot?.balance === 'number' && Number.isFinite(snapshot.balance)
        ? snapshot.balance
        : null;

      if (accountCash === null) {
        return res.status(400).render('pages/error', {
          title: 'Error',
          error: 'Unable to verify the account balance. Please refresh the account connection and try again.'
        });
      }

      parameters.initialCapital = accountCash;
      parsedParameters.initialCapital = accountCash;

      if (!backtestStartDate) {
        const latestCandleDate = await req.db.candles.getLatestGlobalCandleDate();
        const fallbackSource = latestCandleDate ?? new Date();
        const baseDate = new Date(Date.UTC(
          fallbackSource.getUTCFullYear(),
          fallbackSource.getUTCMonth(),
          fallbackSource.getUTCDate()
        ));
        baseDate.setUTCDate(baseDate.getUTCDate() + 1);
        backtestStartDate = baseDate;
      }
    }

    const strategyId = await req.strategyRegistry.createStrategy(
      name,
      templateId,
      parameters,
      userId,
      backtestStartDate ?? undefined,
      selectedAccountId ?? undefined
    );
    res.redirect(`/strategies/${strategyId}`);
  } catch (error: any) {
    console.error('Error creating strategy:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: error.message || 'Failed to create strategy'
    });
  }
});

// Strategy overview page
router.get<StrategyIdParams>('/strategies/:strategyId', requireAuth, async (req, res) => {
  try {
    const { strategyId } = req.params;
    const userId = getReqUserId(req);
    const strategy = await req.db.strategies.getStrategy(strategyId, userId);

    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${strategyId} not found`
      });
    }

    const template = req.strategyRegistry.getTemplate(strategy.templateId) as StrategyTemplate | undefined;
    const [
      backtestResults,
      performance,
      signalSummary,
      recentSignals,
      signalLineCounts,
      signalConfidenceMaxByDay,
      rawBacktestInitialCapital,
      rawStrategyLogs
    ] = await Promise.all([
      req.db.backtestResults.getBacktestResults(strategyId, 'all'),
      req.db.backtestResults.getStoredStrategyPerformance(strategyId),
      req.db.signals.getSignalSummary(strategyId),
      req.db.signals.getSignalsForStrategy(strategyId),
      req.db.signals.getSignalLineCountsByDay(strategyId, 3),
      req.db.signals.getSignalConfidenceMaxByDay(strategyId, 365),
      req.db.settings.getSettingValue(SETTING_KEYS.BACKTEST_INITIAL_CAPITAL),
      loadOptionalStrategySection<LogEntry[]>({
        strategyId,
        label: 'strategy logs',
        fallback: [],
        load: async () =>
          typeof req.loggingService?.getStrategyLogs === 'function'
            ? req.loggingService.getStrategyLogs(strategyId, 50)
            : []
      })
    ]);
    const loadQqqSignalPriceSeries = async (): Promise<Array<{ isoDate: string; close: number }>> => {
      if (signalLineCounts.length === 0) {
        return [];
      }

      const firstIsoDate = signalLineCounts[0].isoDate;
      const lastIsoDate = signalLineCounts[signalLineCounts.length - 1].isoDate;
      const firstDate = firstIsoDate ? new Date(`${firstIsoDate}T00:00:00Z`) : null;
      const lastDate = lastIsoDate ? new Date(`${lastIsoDate}T00:00:00Z`) : null;
      if (!firstDate || !lastDate || Number.isNaN(firstDate.getTime()) || Number.isNaN(lastDate.getTime())) {
        return [];
      }

      return loadOptionalStrategySection<Array<{ isoDate: string; close: number }>>({
        strategyId,
        label: 'QQQ signal overlay',
        fallback: [],
        load: async () => {
          const qqqCandlesResult = await req.db.candles.getCandles(['QQQ'], firstDate, lastDate);
          const qqqCandles = qqqCandlesResult?.['QQQ'] ?? [];
          return qqqCandles
            .map((candle: Candle) => {
              const dateValue = candle.date instanceof Date ? candle.date : new Date(candle.date);
              const isoDate = dateValue.toISOString().split('T')[0];
              const close = Number(candle.close);
              return Number.isFinite(close) ? { isoDate, close } : null;
            })
            .filter(
              (point: { isoDate: string; close: number } | null): point is { isoDate: string; close: number } =>
                Boolean(point)
            );
        }
      });
    };
    const strategyLogViews = rawStrategyLogs.map((entry: LogEntry) => buildStrategyLogView(entry));
    const backtestInitialCapital = resolveBacktestInitialCapitalSetting(rawBacktestInitialCapital);
    const initialCapital = resolveStrategyInitialCapital(strategy, backtestInitialCapital);
    const hasInitialCapital = Number.isFinite(initialCapital) && initialCapital > 0;

    const { parameterSummaries, extraParameters } = buildParameterContexts(template, strategy.parameters);

    const sortedBacktestResults = [...backtestResults].sort((a: any, b: any) => {
      const getTime = (value: any) => {
        if (!value) return 0;
        const date = value instanceof Date ? value : new Date(value);
        const time = date.getTime();
        return Number.isFinite(time) ? time : 0;
      };
      const getPeriodValue = (backtest: any) => {
        const months = Number(backtest?.periodMonths);
        if (Number.isFinite(months) && months > 0) {
          return months * 30;
        }
        const days = Number(backtest?.periodDays);
        if (Number.isFinite(days) && days > 0) {
          return days;
        }
        return 0;
      };
      const getScopeOrder = (scope: any) => {
        if (scope === 'training') return 0;
        if (scope === 'validation') return 1;
        if (scope === 'all') return 2;
        if (scope === 'live') return 3;
        return 4;
      };
      const periodDiff = getPeriodValue(b) - getPeriodValue(a);
      if (periodDiff !== 0) {
        return periodDiff;
      }
      const scopeDiff = getScopeOrder(a?.tickerScope) - getScopeOrder(b?.tickerScope);
      if (scopeDiff !== 0) {
        return scopeDiff;
      }
      return getTime(b.startDate) - getTime(a.startDate);
    });

    const backtests = sortedBacktestResults.map((backtest: any) => {
      const periodMonths = Number.isFinite(backtest.periodMonths) ? Number(backtest.periodMonths) : null;
      const periodDays = Number.isFinite(backtest.periodDays) ? Number(backtest.periodDays) : null;
      let periodLabel = 'N/A';

      if (periodMonths && periodMonths > 0) {
        periodLabel = formatBacktestPeriodLabel(periodMonths);
      } else if (periodDays && periodDays > 0) {
        periodLabel = `${periodDays}d`;
      }
      const tickerScope = normalizeBacktestScope(backtest.tickerScope);
      const scopeMeta = BACKTEST_SCOPE_META[tickerScope];
      const scopeLabel = scopeMeta.label;
      const scopeBadgeVariant = scopeMeta.badge;

      const calmarRatio = typeof backtest.performance?.calmarRatio === 'number'
        ? backtest.performance.calmarRatio
        : null;

      return {
        periodLabel,
        startDate: backtest.startDate,
        endDate: backtest.endDate,
        initialCapital: backtest.initialCapital,
        finalPortfolioValue: backtest.finalPortfolioValue,
        totalReturn: backtest.performance?.totalReturn ?? 0,
        totalTrades: backtest.performance?.totalTrades ?? 0,
        sharpeRatio: backtest.performance?.sharpeRatio ?? null,
        calmarRatio,
        createdAt: backtest.createdAt,
        detailHref: `/backtests/${backtest.id}`,
        tickerScope,
        scopeLabel,
        scopeBadgeVariant
      };
    });

    const latestTrainingBacktest = backtests.find(backtest => backtest.tickerScope === 'training');
    const latestBacktest = latestTrainingBacktest || (backtests.length > 0 ? backtests[0] : null);
    const clearBacktestConfirmMessage = strategy.accountId
      ? 'Are you sure you want to clear backtest data and simulated trades for this strategy? Trades executed on the linked account will be preserved.'
      : 'Are you sure you want to clear all backtest data for this strategy? This will remove all trades and performance metrics.';
    const loadStrategyAccount = async (): Promise<StrategyAccountLoadResult> => {
      if (!strategy.accountId) {
        return {
          strategyAccount: null,
          strategyAccountWarning: null
        };
      }

      return loadOptionalStrategySection<StrategyAccountLoadResult>({
        strategyId,
        label: 'linked account snapshot',
        timeoutMs: STRATEGY_ACCOUNT_TIMEOUT_MS,
        fallback: {
          strategyAccount: null,
          strategyAccountWarning: 'The linked account snapshot is taking too long to load. Try refreshing in a moment.'
        },
        load: async () => {
          const account = await req.db.accounts.getAccountById(strategy.accountId!, userId);
          if (!account) {
            return {
              strategyAccount: null,
              strategyAccountWarning:
                'The linked account could not be found. Trades will not be routed until you select a different account.'
            };
          }

          const snapshotMap = await req.accountDataService.fetchSnapshots([account]);
          const snapshot = snapshotMap[account.id];
          const badge = getSnapshotBadgeMeta(snapshot);
          const rawBalance = snapshot?.balance;
          const hasBalance = typeof rawBalance === 'number' && Number.isFinite(rawBalance);
          return {
            strategyAccount: {
              id: account.id,
              name: account.name,
              provider: account.provider,
              environment: account.environment,
              balance: hasBalance ? rawBalance ?? null : null,
              hasBalance,
              currency: snapshot?.currency ?? null,
              snapshotBadgeLabel: badge.label,
              snapshotBadgeVariant: badge.variant,
              snapshotMessage: snapshot?.message ?? null,
              snapshotFetchedAt: snapshot?.fetchedAt ?? null
            },
            strategyAccountWarning: null
          };
        }
      });
    };
    const loadBacktestComparison = async () => {
      if (!strategy.accountId) {
        return buildBacktestComparisonView({
          db: req.db,
          strategyId,
          userId,
          backtests: backtestResults,
          isEligible: false
        });
      }

      const hasEngine = backtestResults.some((backtest) => {
        const scope = normalizeBacktestScope(backtest.tickerScope);
        return scope === 'training' || scope === 'validation' || scope === 'all';
      });
      const hasLive = backtestResults.some((backtest) => normalizeBacktestScope(backtest.tickerScope) === 'live');

      return loadOptionalStrategySection({
        strategyId,
        label: 'backtest comparison',
        timeoutMs: STRATEGY_BACKTEST_COMPARISON_TIMEOUT_MS,
        fallback: {
          isEligible: true,
          hasEngine,
          hasLive,
          notice: 'Backtest comparison is taking too long to load. Try refreshing in a moment.',
          sampleDays: []
        },
        load: async () =>
          buildBacktestComparisonView({
            db: req.db,
            strategyId,
            userId,
            backtests: backtestResults,
            isEligible: true
          })
      });
    };
    const [qqqSignalPriceSeries, strategyAccountResult, backtestComparison] = await Promise.all([
      loadQqqSignalPriceSeries(),
      loadStrategyAccount(),
      loadBacktestComparison()
    ]);
    const { strategyAccount, strategyAccountWarning } = strategyAccountResult;

    res.render('pages/strategy', {
      title: `${strategy.name} - Strategy Overview`,
      page: 'dashboard',
      user: req.user,
      strategy: {
        ...strategy,
        performance,
        signalSummary
      },
      signalLineCounts,
      hasSignalLineCounts: signalLineCounts.length > 0,
      signalConfidenceMaxByDay,
      hasSignalConfidenceMaxByDay: signalConfidenceMaxByDay.length > 0,
      qqqSignalPriceSeries,
      strategySignalDays: buildStrategySignalDays(recentSignals, 7),
      template,
      backtests,
      hasBacktests: backtests.length > 0,
      backtestCount: backtests.length,
      latestBacktestRanAt: latestBacktest ? latestBacktest.createdAt : null,
      initialCapital,
      hasInitialCapital,
      parameterSummaries,
      parameterSummariesCount: parameterSummaries.length,
      extraParameters,
      extraParameterCount: extraParameters.length,
      strategyAccount,
      strategyAccountWarning,
      clearBacktestConfirmMessage,
      backtestComparison,
      strategyLogs: strategyLogViews,
      hasStrategyLogs: strategyLogViews.length > 0,
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error rendering strategy overview:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load strategy overview'
    });
  }
});

router.get<StrategyIdParams>('/strategies/:strategyId/tickers', requireAuth, async (req, res) => {
  try {
    const { strategyId } = req.params;
    const userId = getReqUserId(req);
    const strategy = await req.db.strategies.getStrategy(strategyId, userId);
    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${strategyId} not found`
      });
    }

    const ticker = normalizeTickerParam(req.query.ticker);
    if (!ticker) {
      return res.redirect(`/strategies/${strategyId}?error=${encodeURIComponent('Ticker is required')}`);
    }

    res.redirect(`/strategies/${strategyId}/tickers/${encodeURIComponent(ticker)}`);
  } catch (error) {
    console.error('Error resolving strategy ticker redirect:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to resolve strategy ticker'
    });
  }
});

router.get<StrategyTickerParams>('/strategies/:strategyId/tickers/:ticker', requireAuth, async (req, res) => {
  try {
    const { strategyId, ticker: tickerParam } = req.params;
    const userId = getReqUserId(req);
    const strategy = await req.db.strategies.getStrategy(strategyId, userId);
    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${strategyId} not found`
      });
    }

    const ticker = normalizeTickerParam(tickerParam);
    if (!ticker) {
      return res.status(400).render('pages/error', {
        title: 'Invalid Ticker',
        error: 'A valid ticker is required'
      });
    }

    const skipSources = [SKIP_SOURCE_META.backtest.source, SKIP_SOURCE_META.planOperations.source];
    const [signals, operations, trades, skips] = await Promise.all([
      req.db.signals.getSignalsForStrategyTicker(strategyId, ticker, userId),
      req.db.accountOperations.getAccountOperationsForStrategy(strategyId, undefined, {
        ticker,
        sortBy: 'triggeredAt',
        order: 'desc'
      }),
      req.db.trades.getTrades(strategyId, undefined, ticker, undefined, undefined, undefined, userId),
      req.db.accountSignalSkips.getAccountSignalSkipsForStrategyTicker(strategyId, ticker, skipSources)
    ]);

    const signalViews = signals.map((signal) => {
      const actionMeta = resolveSignalActionMeta(signal.action);
      return {
        ...signal,
        actionLabel: actionMeta.label,
        actionBadge: actionMeta.badge
      };
    });

    const signalSummary = {
      total: signalViews.length,
      buy: signalViews.filter((signal) => signal.action === 'buy').length,
      sell: signalViews.filter((signal) => signal.action === 'sell').length,
      latestDate: signalViews[0]?.date ?? null
    };

    const tradeViews = trades.map((trade) => {
      const sourceMeta = trade.entryOrderId
        ? { label: 'Live', badge: 'success' }
        : trade.backtestResultId
          ? { label: 'Backtest', badge: 'secondary' }
          : { label: 'Simulated', badge: 'warning' };
      const tradeValue = Math.abs(trade.price * trade.quantity);
      const pnl = typeof trade.pnl === 'number' ? trade.pnl : null;
      const pnlPercent = pnl !== null && tradeValue > 0 ? (pnl / tradeValue) * 100 : null;
      const pnlClass =
        pnlPercent === null
          ? 'text-muted'
          : pnlPercent > 0
            ? 'text-success'
            : pnlPercent < 0
              ? 'text-danger'
              : 'text-muted';

      return {
        ...trade,
        sourceLabel: sourceMeta.label,
        sourceBadge: sourceMeta.badge,
        pnlPercent,
        pnlClass,
        tradeUrl: `/trades/${trade.id}`,
        backtestUrl: trade.backtestResultId ? `/backtests/${trade.backtestResultId}` : null
      };
    });

    const tradeSummary = {
      total: tradeViews.length,
      live: tradeViews.filter((trade) => trade.sourceLabel === 'Live').length,
      backtest: tradeViews.filter((trade) => trade.sourceLabel === 'Backtest').length,
      simulated: tradeViews.filter((trade) => trade.sourceLabel === 'Simulated').length,
      latestDate: tradeViews[0]?.date ?? null
    };

    const operationSummary = operations.reduce(
      (acc, operation) => {
        acc.total += 1;
        if (operation.status === 'pending') {
          acc.pending += 1;
        } else if (operation.status === 'sent') {
          acc.sent += 1;
        } else if (operation.status === 'skipped') {
          acc.skipped += 1;
        } else if (operation.status === 'failed') {
          acc.failed += 1;
        }
        return acc;
      },
      {
        total: 0,
        pending: 0,
        sent: 0,
        skipped: 0,
        failed: 0,
        latestDate: operations[0]?.triggeredAt ?? null
      }
    );

    const operationViews = operations.map((operation) => ({
      ...operation,
      tradeUrl: operation.tradeId ? `/trades/${operation.tradeId}` : null
    }));

    const skipViews = skips.map((skip) => {
      const actionMeta = resolveSignalActionMeta(skip.action);
      const sourceMeta = SKIP_SOURCE_VIEW[String(skip.source ?? '').toLowerCase()] ?? {
        label: String(skip.source ?? 'Unknown'),
        badge: 'secondary'
      };
      const signalDate = skip.signal_date ? new Date(`${skip.signal_date}T00:00:00Z`) : null;
      const { label, detail } = formatSignalSkipReason(skip.reason, skip.details);
      return {
        id: skip.id,
        action: skip.action,
        source: skip.source,
        reason: label,
        details: detail,
        signalDate,
        createdAt: skip.created_at,
        actionLabel: actionMeta.label,
        actionBadge: actionMeta.badge,
        sourceLabel: sourceMeta.label,
        sourceBadge: sourceMeta.badge
      };
    });

    const skipSummary = {
      total: skipViews.length,
      backtest: skipViews.filter((skip) => String(skip.source ?? '').toLowerCase() === SKIP_SOURCE_META.backtest.source)
        .length,
      planOperations: skipViews.filter(
        (skip) => String(skip.source ?? '').toLowerCase() === SKIP_SOURCE_META.planOperations.source
      ).length,
      latestDate: skipViews[0]?.signalDate ?? null
    };

    let strategyAccountSummary: { id: string } | null = null;
    if (strategy.accountId) {
      const account = await req.db.accounts.getAccountById(strategy.accountId, userId);
      if (account) {
        strategyAccountSummary = {
          id: account.id
        };
      }
    }

    const isAdmin = req.user?.role === 'admin';
    let tickerLogViews: StrategyLogView[] = [];
    let tickerLogMetadataColumns: string[] = [];
    if (isAdmin && typeof req.loggingService?.getStrategyLogs === 'function') {
      const matcher = new RegExp(`\\b${escapeRegExp(ticker)}\\b`, 'i');
      const rawLogs = await req.loggingService.getStrategyLogs(strategyId, 200);
      tickerLogViews = rawLogs
        .filter((entry: LogEntry) => {
          if (typeof entry.message === 'string' && matcher.test(entry.message)) {
            return true;
          }
          if (entry.metadata) {
            try {
              const metadataText = JSON.stringify(entry.metadata);
              if (matcher.test(metadataText)) {
                return true;
              }
            } catch {
              // ignore metadata serialization errors
            }
          }
          return false;
        })
        .map((entry: LogEntry) => buildStrategyLogView(entry));
      tickerLogMetadataColumns = Array.from(
        new Set(
          tickerLogViews.flatMap((entry: StrategyLogView) => entry.metadataPairs.map((pair) => pair.label))
        )
      ).sort((a, b) => a.localeCompare(b));
    }

    res.render('pages/strategy-ticker', {
      title: `${strategy.name} ${ticker} - Ticker Activity`,
      page: 'dashboard',
      user: req.user,
      strategy,
      ticker,
      signals: signalViews,
      hasSignals: signalViews.length > 0,
      signalSummary,
      trades: tradeViews,
      hasTrades: tradeViews.length > 0,
      tradeSummary,
      operations: operationViews,
      hasOperations: operationViews.length > 0,
      operationSummary,
      skips: skipViews,
      hasSkips: skipViews.length > 0,
      skipSummary,
      strategyAccountSummary,
      tickerLogs: tickerLogViews,
      hasTickerLogs: tickerLogViews.length > 0,
      tickerLogMetadataColumns
    });
  } catch (error) {
    console.error('Error rendering strategy ticker page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load strategy ticker details'
    });
  }
});

router.get<StrategyIdParams>('/strategies/:strategyId/skips', requireAuth, async (req, res) => {
  try {
    const { strategyId } = req.params;
    const userId = getReqUserId(req);
    const strategy = await req.db.strategies.getStrategy(strategyId, userId);
    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${strategyId} not found`
      });
    }

    const sources = [SKIP_SOURCE_META.backtest.source, SKIP_SOURCE_META.planOperations.source];
    const latestDate = await req.db.accountSignalSkips.getLatestSignalSkipDateForStrategy(strategyId, sources);
    const requestedDate = normalizeSkipDate(req.query.date);
    const selectedDate = requestedDate ?? latestDate ?? '';

    let skips: AccountSignalSkipRow[] = [];
    if (selectedDate) {
      const date = new Date(`${selectedDate}T00:00:00Z`);
      skips = await req.db.accountSignalSkips.getAccountSignalSkipsForStrategyInRange(
        strategyId,
        date,
        date,
        sources
      );
    }

    const { rows, totals, summary } = buildSkipComparisonRows(skips);
    const hasAnySkips = Boolean(latestDate);
    const hasRows = rows.length > 0;

    res.render('pages/strategy-skips', {
      title: `${strategy.name} - Signal Skips`,
      page: 'dashboard',
      user: req.user,
      strategy,
      selectedDate,
      latestDate,
      hasAnySkips,
      hasRows,
      rows,
      totals,
      summary,
      sourceLabels: {
        backtest: SKIP_SOURCE_META.backtest.label,
        planOperations: SKIP_SOURCE_META.planOperations.label
      }
    });
  } catch (error) {
    console.error('Error rendering strategy skips page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load strategy skips'
    });
  }
});

router.get<StrategyIdParams>('/strategies/:strategyId/operations', requireAuth, async (req, res) => {
  try {
    const { strategyId } = req.params;
    const userId = getReqUserId(req);
    const strategy = await req.db.strategies.getStrategy(strategyId, userId);
    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${strategyId} not found`
      });
    }

    const statusFilter = normalizeOperationStatusFilter(req.query.status);
    const typeFilter = normalizeOperationTypeFilter(req.query.type);
    const tickerFilter = normalizeOperationTickerFilter(req.query.ticker);
    const sortField = normalizeOperationSortField(req.query.sort);
    const sortDirection = normalizeOperationSortDirection(req.query.direction);
    const pageSize = normalizeOperationPageSize(req.query.pageSize);
    const requestedPage = parsePageParam(req.query.page);
    const searchFilter = typeof req.query.search === 'string' ? req.query.search.trim() : '';
    const rawFrom = typeof req.query.from === 'string' ? req.query.from : '';
    const rawTo = typeof req.query.to === 'string' ? req.query.to : '';
    const fromDate = parseOperationDate(rawFrom, 'start');
    const toDate = parseOperationDate(rawTo, 'end');

    const statuses = statusFilter === 'all' ? undefined : [statusFilter];
    const operationTypes = typeFilter === 'all' ? undefined : [typeFilter];

    const filterOptions = {
      sortBy: sortField,
      order: sortDirection,
      ticker: tickerFilter,
      operationTypes,
      textFilter: searchFilter.length > 0 ? searchFilter : undefined,
      since: fromDate,
      until: toDate
    };

    const totalOperations = await req.db.accountOperations.countAccountOperationsForStrategy(strategyId, statuses, filterOptions);
    const totalPages = Math.max(1, Math.ceil(Math.max(totalOperations, 0) / pageSize));
    const page = Math.min(requestedPage, totalPages);
    const offset = (page - 1) * pageSize;

    const operations =
      totalOperations === 0
        ? []
        : await req.db.accountOperations.getAccountOperationsForStrategy(strategyId, statuses, {
            ...filterOptions,
            limit: pageSize,
            offset
          });

    const statusCounts = await req.db.accountOperations.getAccountOperationStatusCountsForStrategy(strategyId, filterOptions);

    const operationsPagePath = `/strategies/${strategyId}/operations`;
    const baseQuery: Record<string, string> = {};
    if (statusFilter !== 'all') {
      baseQuery.status = statusFilter;
    }
    if (typeFilter !== 'all') {
      baseQuery.type = typeFilter;
    }
    if (tickerFilter) {
      baseQuery.ticker = tickerFilter;
    }
    if (rawFrom) {
      baseQuery.from = rawFrom;
    }
    if (rawTo) {
      baseQuery.to = rawTo;
    }
    if (searchFilter) {
      baseQuery.search = searchFilter;
    }
    if (sortField !== 'triggeredAt') {
      baseQuery.sort = sortField;
    }
    if (sortDirection === 'asc') {
      baseQuery.direction = 'asc';
    }
    if (pageSize !== DEFAULT_STRATEGY_OPERATION_PAGE_SIZE) {
      baseQuery.pageSize = String(pageSize);
    }

    const pagination = {
      page,
      totalPages,
      total: totalOperations,
      pageSize,
      from: totalOperations === 0 ? 0 : offset + 1,
      to: totalOperations === 0 ? 0 : offset + operations.length,
      hasPrev: page > 1,
      hasNext: page < totalPages,
      firstUrl: page > 1 ? buildOperationsPageUrl(operationsPagePath, baseQuery, { page: 1 }) : null,
      prevUrl: page > 1 ? buildOperationsPageUrl(operationsPagePath, baseQuery, { page: page - 1 }) : null,
      nextUrl: page < totalPages ? buildOperationsPageUrl(operationsPagePath, baseQuery, { page: page + 1 }) : null,
      lastUrl: page < totalPages ? buildOperationsPageUrl(operationsPagePath, baseQuery, { page: totalPages }) : null
    };

    const statusOptions = [
      { value: 'all', label: `All (${totalOperations})` },
      ...ACCOUNT_OPERATION_STATUS_ORDER.map(status => ({
        value: status,
        label: `${ACCOUNT_OPERATION_STATUS_META[status].label} (${statusCounts[status] ?? 0})`
      }))
    ];

    const operationTypeOptions = [
      { value: 'all', label: 'All Types' },
      ...Object.entries(STRATEGY_OPERATION_TYPE_LABELS).map(([value, label]) => ({
        value,
        label: `${label}`
      }))
    ];

    const sortOptions = Object.entries(STRATEGY_OPERATION_SORT_LABELS).map(([value, label]) => ({
      value,
      label
    }));

    const statusSummary = [
      { key: 'total', label: 'Total Operations', value: totalOperations, badge: 'primary' },
      ...ACCOUNT_OPERATION_STATUS_ORDER.map(status => ({
        key: status,
        label: ACCOUNT_OPERATION_STATUS_META[status].label,
        value: statusCounts[status] ?? 0,
        badge: ACCOUNT_OPERATION_STATUS_META[status].badge
      }))
    ];

    let strategyAccountSummary: { id: string; name: string; provider: string; environment: string } | null = null;
    if (strategy.accountId) {
      const account = await req.db.accounts.getAccountById(strategy.accountId, userId);
      if (account) {
        strategyAccountSummary = {
          id: account.id,
          name: account.name,
          provider: account.provider,
          environment: account.environment
        };
      }
    }

    res.render('pages/strategy-operations', {
      title: `${strategy.name} - Operations`,
      page: 'dashboard',
      user: req.user,
      strategy,
      strategyAccountSummary,
      operations,
      hasOperations: totalOperations > 0,
      pagination,
      filters: {
        status: statusFilter,
        type: typeFilter,
        ticker: tickerFilter ?? '',
        sortBy: sortField,
        sortDirection,
        pageSize,
        search: searchFilter,
        from: rawFrom,
        to: rawTo
      },
      statusOptions,
      operationTypeOptions,
      sortOptions,
      pageSizeOptions: STRATEGY_OPERATION_PAGE_SIZE_OPTIONS,
      statusSummary,
      basePath: operationsPagePath,
      baseQuery
    });
  } catch (error) {
    console.error('Error rendering strategy operations page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load strategy operations'
    });
  }
});

// Strategy Actions
router.post<StrategyParams>('/strategies/:id/run', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const strategy = await loadStrategyOrRenderNotFound(req, res, id);
    if (!strategy) return;

    await req.db.strategies.updateStrategyStatus(id, 'active');

    res.redirect(`/strategies/${id}?success=Strategy started successfully`);
  } catch (error) {
    console.error('Error running strategy:', error);
    res.redirect(`/strategies/${req.params.id}?error=Failed to start strategy`);
  }
});

router.post<StrategyParams>('/strategies/:id/stop', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const strategy = await loadStrategyOrRenderNotFound(req, res, id);
    if (!strategy) return;

    await req.db.strategies.updateStrategyStatus(id, 'inactive');

    res.redirect(`/strategies/${id}?success=Strategy stopped successfully`);
  } catch (error) {
    console.error('Error stopping strategy:', error);
    res.redirect(`/strategies/${req.params.id}?error=Failed to stop strategy`);
  }
});

router.post<StrategyParams>('/strategies/:id/rename', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { name } = req.body;

    if (!name || name.trim() === '') {
      return res.redirect(`/strategies/${id}?error=Strategy name cannot be empty`);
    }

    const strategy = await loadStrategyOrRenderNotFound(req, res, id);
    if (!strategy) return;

    if (name.trim() === strategy.name) {
      return res.redirect(`/strategies/${id}?error=New name must be different from current name`);
    }

    await req.db.strategies.updateStrategyName(id, name.trim());

    res.redirect(`/strategies/${id}?success=Strategy renamed successfully`);
  } catch (error) {
    console.error('Error renaming strategy:', error);
    res.redirect(`/strategies/${req.params.id}?error=Failed to rename strategy`);
  }
});

router.post<StrategyParams>('/strategies/:id/delete', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const strategy = await loadStrategyOrRenderNotFound(req, res, id);
    if (!strategy) return;

    await req.db.strategies.deleteStrategy(id);

    res.redirect('/dashboard?success=Strategy deleted successfully');
  } catch (error) {
    console.error('Error deleting strategy:', error);
    res.redirect(`/strategies/${req.params.id}?error=Failed to delete strategy`);
  }
});

router.post<StrategyParams>('/strategies/:id/clear-backtest', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const strategy = await loadStrategyOrRenderNotFound(req, res, id);
    if (!strategy) return;

    const preserveAccountTrades = Boolean(strategy.accountId);
    const result = await req.db.strategies.clearStrategyBacktestResults(id, preserveAccountTrades);
    const tradesLabel = result.tradesDeleted === 1 ? 'trade' : 'trades';
    const message = preserveAccountTrades
      ? `Cleared ${result.tradesDeleted} backtest and simulated ${tradesLabel} and backtest results (account trades preserved)`
      : `Cleared ${result.tradesDeleted} ${tradesLabel} and backtest results successfully`;

    res.redirect(`/strategies/${id}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing strategy backtest data:', error);
    res.redirect(`/strategies/${req.params.id}?error=Failed to clear backtest data`);
  }
});

router.use(backtestRoutes);

export default router;
