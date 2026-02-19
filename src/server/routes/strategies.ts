import express, { NextFunction, Request, Response } from 'express';
import { BacktestParams, StrategyIdParams, StrategyParams } from '../../shared/types/Express';
import {
  StrategyTemplate,
  StrategyParameter,
  Candle,
  AccountOperationStatus,
  AccountOperationType
} from '../../shared/types/StrategyTemplate';
import { AccountSnapshot } from '../../shared/types/Account';
import { LogEntry, LogLevel } from '../services/LoggingService';
import type { AccountSignalSkipRow, TradeTickerStats } from '../database/types';
import { SETTING_KEYS } from '../constants';
import {
  BACKTEST_SCOPE_META,
  buildBacktestComparisonView,
  normalizeBacktestScope
} from '../scoring/backtestComparison';
import {
  buildBenchmarkDataFromSnapshots,
  buildCashPercentageDataFromSnapshots,
  buildDailyReturnDistribution,
  buildDrawdownDataFromPortfolio,
  buildPortfolioValueDataFromSnapshots,
  type PortfolioValuePoint
} from '../utils/backtestCharts';
import { resolveBacktestInitialCapitalSetting, resolveStrategyInitialCapital } from '../utils/initialCapital';
import { getReqUserId, getCurrentUrl, formatBacktestPeriodLabel, parsePageParam } from './utils';

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

interface ParameterSummaryView {
  name: string;
  label?: string;
  description?: string;
  type: StrategyParameter['type'];
  displayValue: string;
  defaultDisplay: string;
  hasOverride: boolean;
}

interface ExtraParameterView {
  name: string;
  displayValue: string;
}

const MAX_PARAMETER_DECIMALS = 15;

const trimTrailingZeros = (value: string): string =>
  value.replace(/(\.\d*?[1-9])0+$/u, '$1').replace(/\.0+$/u, '');

const formatNumericValue = (value: number): string => {
  if (!Number.isFinite(value)) {
    return String(value);
  }
  if (Object.is(value, -0)) {
    return '0';
  }
  const fixed = value.toFixed(MAX_PARAMETER_DECIMALS);
  const trimmed = trimTrailingZeros(fixed);
  return trimmed === '-0' ? '0' : trimmed;
};

const coerceNumericValue = (value: unknown): number | null => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
};

type FormatParameterValueOptions = {
  type?: StrategyParameter['type'];
  emptyLabel?: string;
  loose?: boolean;
};

const formatParameterValue = (
  value: unknown,
  { type, emptyLabel = 'N/A', loose = false }: FormatParameterValueOptions = {}
): string => {
  if (value === undefined || value === null || value === '') {
    return emptyLabel;
  }
  if (typeof value === 'boolean') {
    return value ? 'True' : 'False';
  }
  if (typeof value === 'number') {
    return formatNumericValue(value);
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (loose && !trimmed) {
      return emptyLabel;
    }
    if (loose) {
      const lowered = trimmed.toLowerCase();
      if (lowered === 'true' || lowered === 'false') {
        return lowered === 'true' ? 'True' : 'False';
      }
    }
    if (type === 'number' || loose) {
      const parsed = coerceNumericValue(trimmed);
      if (parsed !== null) {
        return formatNumericValue(parsed);
      }
    }
    return value;
  }
  return String(value);
};

const buildParameterContexts = (
  template?: StrategyTemplate | null,
  strategyParamsInput?: Record<string, unknown> | null
): {
  parameterSummaries: ParameterSummaryView[];
  extraParameters: ExtraParameterView[];
} => {
  const strategyParams = strategyParamsInput ?? {};

  const parameterSummaries = template
    ? template.parameters.map((param: StrategyParameter) => {
        const hasOverride = Object.prototype.hasOwnProperty.call(strategyParams, param.name);
        const rawValue = hasOverride ? (strategyParams as Record<string, unknown>)[param.name] : param.default;
        const rawDefault = param.default;
        const displayValue = formatParameterValue(rawValue, { type: param.type });
        const defaultDisplay = formatParameterValue(rawDefault, { type: param.type });
        return {
          name: param.name,
          label: param.label,
          description: param.description,
          type: param.type,
          displayValue,
          defaultDisplay,
          hasOverride: hasOverride && displayValue !== defaultDisplay
        };
      })
    : [];

  const templateParameterNames = new Set(
    template ? template.parameters.map((param: StrategyParameter) => param.name) : []
  );
  const extraParameters = Object.entries(strategyParams)
    .filter(([name]) => !templateParameterNames.has(name))
    .map(([name, value]) => ({
      name,
      displayValue: formatParameterValue(value, { loose: true })
    }));

  return { parameterSummaries, extraParameters };
};

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
  id?: number;
  level: string;
  levelLabel: string;
  levelBadgeClass: string;
  source: string;
  message: string;
  createdAt?: Date;
  metadataPairs: StrategyLogMetadataPair[];
  metadataByLabel: Record<string, string>;
  strategyId?: string;
}

type PortfolioDayMove = {
  date: string;
  change: number;
  changePercent: number;
  previousValue: number;
  value: number;
  activeTrades: number;
  missedTradesDueToCash: number;
  direction: 'up' | 'down' | 'flat';
};

const LOG_LEVEL_BADGES: Record<LogLevel, string> = {
  error: 'bg-danger',
  warn: 'bg-warning text-dark',
  info: 'bg-info text-dark',
  debug: 'bg-secondary'
};

const IGNORED_LOG_METADATA_KEYS = new Set(['strategyId']);

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
    id: log.id,
    level,
    levelLabel: level.toUpperCase(),
    levelBadgeClass: LOG_LEVEL_BADGES[level as LogLevel] ?? 'bg-secondary',
    source: log.source ?? 'system',
    message: log.message,
    createdAt: log.created_at,
    metadataPairs,
    metadataByLabel,
    strategyId: log.strategyId
  };
};

function buildPortfolioDayMovers(
  portfolioPoints: PortfolioValuePoint[],
  limit: number = 10
): { best: PortfolioDayMove[]; worst: PortfolioDayMove[] } {
  const normalizedLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 10;
  if (!Array.isArray(portfolioPoints) || portfolioPoints.length < 2) {
    return { best: [], worst: [] };
  }

  const normalizedPoints = portfolioPoints
    .map(point => {
      const timestamp = new Date(point.date).getTime();
      return {
        date: point.date,
        value: Number(point.value),
        activeTrades: Number(point.activeTrades) || 0,
        missedTradesDueToCash: Number(point.missedTradesDueToCash) || 0,
        timestamp
      };
    })
    .filter(point => Number.isFinite(point.timestamp) && Number.isFinite(point.value))
    .sort((a, b) => a.timestamp - b.timestamp);

  if (normalizedPoints.length < 2) {
    return { best: [], worst: [] };
  }

  const dayMoves: PortfolioDayMove[] = [];

  for (let i = 1; i < normalizedPoints.length; i += 1) {
    const previous = normalizedPoints[i - 1];
    const current = normalizedPoints[i];
    const change = current.value - previous.value;
    const changePercent = previous.value !== 0 ? (change / previous.value) * 100 : 0;
    const direction: PortfolioDayMove['direction'] =
      change > 0 ? 'up' : change < 0 ? 'down' : 'flat';

    dayMoves.push({
      date: current.date,
      change,
      changePercent,
      previousValue: previous.value,
      value: current.value,
      activeTrades: current.activeTrades,
      missedTradesDueToCash: current.missedTradesDueToCash,
      direction
    });
  }

  const bestDays = dayMoves
    .filter(move => move.change > 0)
    .sort((a, b) => {
      if (b.changePercent !== a.changePercent) {
        return b.changePercent - a.changePercent;
      }
      if (b.change !== a.change) {
        return b.change - a.change;
      }
      return new Date(b.date).getTime() - new Date(a.date).getTime();
    })
    .slice(0, normalizedLimit);

  const worstDays = dayMoves
    .filter(move => move.change < 0)
    .sort((a, b) => {
      if (a.changePercent !== b.changePercent) {
        return a.changePercent - b.changePercent;
      }
      if (a.change !== b.change) {
        return a.change - b.change;
      }
      return new Date(a.date).getTime() - new Date(b.date).getTime();
    })
    .slice(0, normalizedLimit);

  return { best: bestDays, worst: worstDays };
}

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
  action: string;
  reason: string;
  details: string | null;
  count: number;
};

type SkipTickerAggregate = {
  total: number;
  reasons: Map<string, SkipReasonAggregate>;
};

type SkipCellAggregate = {
  total: number;
  tickers: Map<string, SkipTickerAggregate>;
};

type SkipCellView = {
  total: number;
  hasItems: boolean;
  tickers: Array<{
    ticker: string;
    total: number;
    lines: Array<{ text: string }>;
  }>;
};

type SkipRowView = {
  letter: string;
  backtest: SkipCellView;
  planOperations: SkipCellView;
};

const SKIP_SOURCE_META: Record<SkipSourceKey, { label: string; source: string }> = {
  backtest: { label: 'Engine backtest', source: 'backtest' },
  planOperations: { label: 'Operation planning', source: 'plan_operations' }
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
  const actionLabel = reason.action ? reason.action.toUpperCase() : 'UNKNOWN';
  const reasonLabel = reason.reason;
  let text = `${actionLabel} - ${reasonLabel}`;
  if (reason.details) {
    text += ` (${reason.details})`;
  }
  if (reason.count > 1) {
    text += ` x${reason.count}`;
  }
  return text;
};

const buildSkipCellView = (aggregate: SkipCellAggregate): SkipCellView => {
  const tickers = Array.from(aggregate.tickers.entries())
    .map(([ticker, tickerAgg]) => {
      const reasons = Array.from(tickerAgg.reasons.values()).sort((a, b) => {
        const actionOrder = (value: string) => (value === 'buy' ? 0 : value === 'sell' ? 1 : 2);
        const actionDiff = actionOrder(a.action) - actionOrder(b.action);
        if (actionDiff !== 0) {
          return actionDiff;
        }
        const reasonDiff = a.reason.localeCompare(b.reason);
        if (reasonDiff !== 0) {
          return reasonDiff;
        }
        return (a.details ?? '').localeCompare(b.details ?? '');
      });
      return {
        ticker,
        total: tickerAgg.total,
        lines: reasons.map((reason) => ({ text: buildSkipReasonLine(reason) }))
      };
    })
    .sort((a, b) => a.ticker.localeCompare(b.ticker));

  return {
    total: aggregate.total,
    hasItems: tickers.length > 0,
    tickers
  };
};

const buildSkipComparisonRows = (
  skips: AccountSignalSkipRow[]
): { rows: SkipRowView[]; totals: Record<SkipSourceKey, number> } => {
  const alphabet = Array.from({ length: 26 }, (_, idx) => String.fromCharCode(65 + idx));
  const createCell = (): SkipCellAggregate => ({
    total: 0,
    tickers: new Map()
  });
  const rows = new Map<string, { backtest: SkipCellAggregate; planOperations: SkipCellAggregate }>();
  alphabet.forEach((letter) => {
    rows.set(letter, { backtest: createCell(), planOperations: createCell() });
  });

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
    const letter = ticker.charAt(0).toUpperCase();
    const row = rows.get(letter);
    if (!row) {
      continue;
    }

    const cell = row[sourceKey];
    cell.total += 1;
    let tickerAgg = cell.tickers.get(ticker);
    if (!tickerAgg) {
      tickerAgg = { total: 0, reasons: new Map() };
      cell.tickers.set(ticker, tickerAgg);
    }
    tickerAgg.total += 1;
    const action = typeof skip.action === 'string' ? skip.action.toLowerCase() : '';
    const details = typeof skip.details === 'string' && skip.details.length > 0 ? skip.details : null;
    const reasonKey = `${action}|${skip.reason}|${details ?? ''}`;
    let reasonAgg = tickerAgg.reasons.get(reasonKey);
    if (!reasonAgg) {
      reasonAgg = {
        action,
        reason: skip.reason,
        details,
        count: 0
      };
      tickerAgg.reasons.set(reasonKey, reasonAgg);
    }
    reasonAgg.count += 1;
  }

  const totals: Record<SkipSourceKey, number> = {
    backtest: 0,
    planOperations: 0
  };

  const rowViews: SkipRowView[] = [];
  for (const letter of alphabet) {
    const row = rows.get(letter);
    if (!row) {
      continue;
    }
    totals.backtest += row.backtest.total;
    totals.planOperations += row.planOperations.total;
    const backtestView = buildSkipCellView(row.backtest);
    const planView = buildSkipCellView(row.planOperations);
    if (!backtestView.hasItems && !planView.hasItems) {
      continue;
    }
    rowViews.push({
      letter,
      backtest: backtestView,
      planOperations: planView
    });
  }

  return { rows: rowViews, totals };
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
      ? (req.strategyRegistry.getTemplate(selectedTemplateId) as StrategyTemplate | undefined)
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

    let parameterSource: 'url' | 'best' | 'default' = 'default';
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
      currentUrl: getCurrentUrl(req),
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

    const template = req.strategyRegistry.getTemplate(templateId);
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
    const backtestResults = await req.db.backtestResults.getBacktestResults(strategyId, 'all');
    const performance = await req.db.backtestResults.getStoredStrategyPerformance(strategyId);
    const signalSummary = await req.db.signals.getSignalSummary(strategyId);
    const recentSignals = await req.db.signals.getSignalsForStrategy(strategyId);
    const signalLineCounts = await req.db.signals.getSignalLineCountsByDay(strategyId, 3);
    const signalConfidenceMaxByDay = await req.db.signals.getSignalConfidenceMaxByDay(strategyId, 365);
    let qqqSignalPriceSeries: Array<{ isoDate: string; close: number }> = [];
    if (signalLineCounts.length > 0) {
      const firstIsoDate = signalLineCounts[0].isoDate;
      const lastIsoDate = signalLineCounts[signalLineCounts.length - 1].isoDate;
      const firstDate = firstIsoDate ? new Date(`${firstIsoDate}T00:00:00Z`) : null;
      const lastDate = lastIsoDate ? new Date(`${lastIsoDate}T00:00:00Z`) : null;
      if (firstDate && lastDate && !Number.isNaN(firstDate.getTime()) && !Number.isNaN(lastDate.getTime())) {
        try {
          const qqqCandlesResult = await req.db.candles.getCandles(['QQQ'], firstDate, lastDate);
          const qqqCandles = qqqCandlesResult?.['QQQ'] ?? [];
          qqqSignalPriceSeries = qqqCandles
            .map((candle: Candle) => {
              const dateValue =
                candle.date instanceof Date ? candle.date : new Date(candle.date);
              const isoDate = dateValue.toISOString().split('T')[0];
              const close = Number(candle.close);
              return Number.isFinite(close) ? { isoDate, close } : null;
            })
            .filter(
              (point: { isoDate: string; close: number } | null): point is { isoDate: string; close: number } =>
                Boolean(point)
            );
        } catch (error) {
          console.warn(`Unable to load QQQ candles for signal overlay on strategy ${strategyId}:`, error);
        }
      }
    }
    const rawStrategyLogs: LogEntry[] =
      typeof req.loggingService?.getStrategyLogs === 'function'
        ? await req.loggingService.getStrategyLogs(strategyId, 50)
        : [];
    const strategyLogViews = rawStrategyLogs.map((entry: LogEntry) => buildStrategyLogView(entry));
    const metadataColumns = Array.from(
      new Set(
        strategyLogViews.flatMap((entry: StrategyLogView) => entry.metadataPairs.map((pair) => pair.label))
      )
    ).sort((a, b) => a.localeCompare(b));

    const rawBacktestInitialCapital = await req.db.settings.getSettingValue(SETTING_KEYS.BACKTEST_INITIAL_CAPITAL);
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
        id: backtest.id,
        periodMonths,
        periodDays,
        periodLabel,
        startDate: backtest.startDate,
        endDate: backtest.endDate,
        initialCapital: backtest.initialCapital,
        finalPortfolioValue: backtest.finalPortfolioValue,
        performance: backtest.performance,
        totalReturn: backtest.performance?.totalReturn ?? 0,
        totalTrades: backtest.performance?.totalTrades ?? 0,
        sharpeRatio: backtest.performance?.sharpeRatio ?? null,
        calmarRatio,
        hasCalmarRatio: calmarRatio !== null,
        createdAt: backtest.createdAt,
        detailHref: `/backtests/${backtest.id}`,
        tickerScope,
        scopeLabel,
        scopeBadgeVariant
      };
    });

    const latestTrainingBacktest = backtests.find(backtest => backtest.tickerScope === 'training');
    const latestBacktest = latestTrainingBacktest || (backtests.length > 0 ? backtests[0] : null);

    let strategyAccount: {
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
    } | null = null;
    let strategyAccountWarning: string | null = null;

    if (strategy.accountId) {
      const account = await req.db.accounts.getAccountById(strategy.accountId, userId);
      if (!account) {
        strategyAccountWarning =
          'The linked account could not be found. Trades will not be routed until you select a different account.';
      } else {
        const snapshotMap = await req.accountDataService.fetchSnapshots([account]);
        const snapshot = snapshotMap[account.id];
        const badge = getSnapshotBadgeMeta(snapshot);
        const rawBalance = snapshot?.balance;
        const hasBalance = typeof rawBalance === 'number' && Number.isFinite(rawBalance);
        strategyAccount = {
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
        };

      }
    }

    const clearBacktestConfirmMessage = strategy.accountId
      ? 'Are you sure you want to clear backtest data and simulated trades for this strategy? Trades executed on the linked account will be preserved.'
      : 'Are you sure you want to clear all backtest data for this strategy? This will remove all trades and performance metrics.';

    const backtestComparison = await buildBacktestComparisonView({
      db: req.db,
      strategyId,
      userId,
      backtests: backtestResults,
      isEligible: Boolean(strategy.accountId)
    });

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
      metadataColumns,
      strategyLogs: strategyLogViews,
      hasStrategyLogs: strategyLogViews.length > 0,
      currentUrl: getCurrentUrl(req),
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

    const { rows, totals } = buildSkipComparisonRows(skips);
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
      sourceLabels: {
        backtest: SKIP_SOURCE_META.backtest.label,
        planOperations: SKIP_SOURCE_META.planOperations.label
      },
      currentUrl: getCurrentUrl(req)
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
      baseQuery,
      currentUrl: getCurrentUrl(req)
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

// Backtest page for a specific backtest result
router.get<BacktestParams>('/backtests/:backtestId', requireAuth, async (req, res) => {
  try {
    const { backtestId } = req.params;
    const userId = getReqUserId(req);
    const targetBacktest = await req.db.backtestResults.getBacktestResultById(backtestId, userId);

    if (!targetBacktest) {
      return res.status(404).render('pages/error', {
        title: 'Backtest Not Found',
        error: `Backtest ${backtestId} not found`
      });
    }

    const strategy = await req.db.strategies.getStrategy(targetBacktest.strategyId, userId);

    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${targetBacktest.strategyId} not found`
      });
    }

    const template = req.strategyRegistry.getTemplate(strategy.templateId);
    const { parameterSummaries, extraParameters } = buildParameterContexts(template, strategy.parameters);

    const bestTrades = await req.db.trades.getBestTradesByPnlPercent(strategy.id, userId, 20, targetBacktest.id);
    const worstTrades = await req.db.trades.getWorstTradesByPnlPercent(strategy.id, userId, 20, targetBacktest.id);

    const tradeTickerStatsAll: TradeTickerStats[] = await req.db.trades.getTickerTradeStatsForBacktest(targetBacktest.id, userId);
    const tradesPageUrl = `/backtests/${targetBacktest.id}/trades`;
    const tradeDateInsightsUrl = `/backtests/${targetBacktest.id}/trades/date-insights`;

    const tickerScope = normalizeBacktestScope(targetBacktest.tickerScope);
    const scopeMeta = BACKTEST_SCOPE_META[tickerScope];
    const periodMonthsValue = Number(targetBacktest.periodMonths);
    const periodMonths = Number.isFinite(periodMonthsValue) ? periodMonthsValue : null;

    const basePerformance = await req.db.backtestResults.getStoredStrategyPerformance(strategy.id, {
      periodMonths: periodMonths ?? undefined,
      tickerScope
    });
    const performance = {
      ...basePerformance,
      ...(targetBacktest.performance ?? {})
    };
    performance.backtestStartDate = targetBacktest.startDate ?? performance.backtestStartDate;
    performance.backtestEndDate = targetBacktest.endDate ?? performance.backtestEndDate;
    performance.lastUpdated = targetBacktest.createdAt ?? performance.lastUpdated;
    performance.tickerScope = tickerScope;
    performance.backtestId = targetBacktest.id ?? performance.backtestId;

    const dailySnapshots = Array.isArray(targetBacktest.dailySnapshots) ? targetBacktest.dailySnapshots : [];
    const backtestInitialCapital = Number(targetBacktest.initialCapital);
    const portfolioValueData = buildPortfolioValueDataFromSnapshots(dailySnapshots);
    const { best: bestPortfolioDays, worst: worstPortfolioDays } = buildPortfolioDayMovers(
      portfolioValueData,
      10
    );
    const benchmarkData = await buildBenchmarkDataFromSnapshots(
      req.db,
      dailySnapshots,
      backtestInitialCapital
    );
    const drawdownData = buildDrawdownDataFromPortfolio(portfolioValueData);
    const dailyReturnDistributionData = buildDailyReturnDistribution(portfolioValueData);
    const cashPercentageData = buildCashPercentageDataFromSnapshots(dailySnapshots);

    const { success, error } = req.query;
    res.render('pages/backtest', {
      title: `${strategy.name} - Strategy Details`,
      page: 'dashboard',
      user: req.user,
      strategy: {
        ...strategy,
        performance
      },
      template,
      bestTrades,
      worstTrades,
      portfolioValueData,
      bestPortfolioDays,
      worstPortfolioDays,
      benchmarkData,
      drawdownData,
      dailyReturnDistributionData,
      tradeTickerStats: tradeTickerStatsAll,
      tradesPageUrl,
      tradeDateInsightsUrl,
      cashPercentageData,
      backtestInitialCapital,
      parameterSummaries,
      parameterSummariesCount: parameterSummaries.length,
      extraParameters,
      extraParameterCount: extraParameters.length,
      currentUrl: getCurrentUrl(req),
      success,
      error,
      selectedBacktestPeriodMonths: periodMonths,
      selectedBacktestScope: tickerScope,
      selectedBacktestScopeLabel: scopeMeta.label,
      selectedBacktestScopeBadgeVariant: scopeMeta.badge
    });
  } catch (error) {
    console.error('Error rendering strategy detail for specific backtest period:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load strategy details'
    });
  }
});

export default router;
