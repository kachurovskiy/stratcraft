import express, { NextFunction, Request, Response } from 'express';
import { BacktestParams, StrategyIdParams, TradeParams } from '../../shared/types/Express';
import {
  TradeChange,
  Trade,
  Candle,
  AccountOperation,
  AccountOperationStatus
} from '../../shared/types/StrategyTemplate';
import { SETTING_KEYS } from '../constants';
import { getReqUserId, getCurrentUrl, formatBacktestPeriodLabel, parsePageParam } from './utils';

const router = express.Router();

const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

type TradeStatusFilter = 'all' | 'active' | 'closed' | 'cancelled' | 'pending';
type TradeSortField = 'date' | 'createdAt' | 'pnl' | 'ticker';
const TRADES_PAGE_SIZE_OPTIONS = [100, 250, 500, 1000];
const DEFAULT_TRADES_PAGE_SIZE = 250;
const TRADE_SORT_OPTION_LABELS: Record<TradeSortField, string> = {
  date: 'Trade Date',
  createdAt: 'Created Date',
  pnl: 'P&L',
  ticker: 'Ticker'
};
const TRADE_OPERATION_STATUS_ORDER: AccountOperationStatus[] = ['pending', 'sent', 'skipped', 'failed'];
type TradeOrderType = 'entry' | 'stop' | 'exit' | 'operation';
type TradeOrderStatus = AccountOperationStatus | 'unknown';

interface TradeOrderView {
  label: string;
  type: TradeOrderType;
  orderId: string;
  status: TradeOrderStatus;
  statusReason: string | null;
  triggeredAt: Date | null;
  statusUpdatedAt: Date | null;
  operationType: AccountOperation['operationType'] | null | undefined;
  quantity: number | null | undefined;
  price: number | null | undefined;
  stopLoss: number | null | undefined;
}

const TRADE_ORDER_TYPE_ORDER: TradeOrderType[] = ['entry', 'stop', 'exit', 'operation'];

interface TradeOperationsSummary {
  total: number;
  counts: Record<AccountOperationStatus, number>;
  latestStatusUpdatedAt: Date | null;
}

function normalizeTradeStatusFilter(raw: unknown): TradeStatusFilter {
  if (typeof raw !== 'string') {
    return 'all';
  }
  const value = raw.toLowerCase();
  if (value === 'active' || value === 'closed' || value === 'cancelled' || value === 'pending') {
    return value as TradeStatusFilter;
  }
  return 'all';
}

function normalizeTradeSortField(raw: unknown): TradeSortField {
  if (raw === 'createdAt' || raw === 'pnl' || raw === 'ticker' || raw === 'date') {
    return raw as TradeSortField;
  }
  return 'date';
}

function normalizeTradeSortDirection(raw: unknown): 'asc' | 'desc' {
  if (typeof raw === 'string' && raw.toLowerCase() === 'asc') {
    return 'asc';
  }
  return 'desc';
}

function normalizeTradesPageSize(raw: unknown): number {
  const parsed = typeof raw !== 'string' ? Number(raw) : parseInt(raw, 10);
  if (Number.isFinite(parsed) && TRADES_PAGE_SIZE_OPTIONS.includes(parsed)) {
    return parsed;
  }
  return DEFAULT_TRADES_PAGE_SIZE;
}

function normalizeTickerFilter(raw: unknown): string | undefined {
  if (typeof raw !== 'string') {
    return undefined;
  }
  const value = raw.trim().toUpperCase();
  return value.length > 0 ? value : undefined;
}

function buildTradesPageUrl(
  basePath: string,
  baseQuery: Record<string, string>,
  overrides: Record<string, string | number | undefined> = {}
) {
  const params = new URLSearchParams();
  const merged = { ...baseQuery, ...overrides };
  Object.entries(merged).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') {
      return;
    }
    params.set(key, String(value));
  });
  const queryString = params.toString();
  return queryString ? `${basePath}?${queryString}` : basePath;
}

interface FormattedTradeChange {
  field: string;
  label: string;
  oldValueDisplay: string;
  newValueDisplay: string;
  changedAt: Date;
}

interface TradeChangeGroup {
  day: Date;
  changes: FormattedTradeChange[];
}

function parseDateParam(raw: unknown): { date: Date; dateString: string } | null {
  if (typeof raw !== 'string') {
    return null;
  }
  const trimmed = raw.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return null;
  }
  const date = new Date(`${trimmed}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return { date, dateString: trimmed };
}

function getUtcDayBounds(date: Date): { start: Date; end: Date } {
  const start = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0));
  const end = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 23, 59, 59, 999));
  return { start, end };
}

function toDateOnlyString(date?: Date): string | null {
  if (!date || Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString().split('T')[0];
}

function buildTradeChangeGroups(changes: TradeChange[] | undefined | null): TradeChangeGroup[] {
  if (!Array.isArray(changes) || changes.length === 0) {
    return [];
  }

  const sortedChanges = [...changes].sort((a, b) => {
    const aTime = new Date(a.changedAt).getTime();
    const bTime = new Date(b.changedAt).getTime();
    return aTime - bTime;
  });

  const groups = new Map<string, TradeChangeGroup>();

  sortedChanges.forEach(change => {
    const changedAt = new Date(change.changedAt);
    const dayKey = changedAt.toISOString().split('T')[0];
    if (!groups.has(dayKey)) {
      groups.set(dayKey, {
        day: new Date(`${dayKey}T00:00:00Z`),
        changes: []
      });
    }

    const group = groups.get(dayKey);
    if (group) {
      group.changes.push({
        field: change.field,
        label: formatTradeChangeField(change.field),
        oldValueDisplay: formatTradeChangeValue(change.oldValue),
        newValueDisplay: formatTradeChangeValue(change.newValue),
        changedAt
      });
    }
  });

  return Array.from(groups.values())
    .map(group => ({
      ...group,
      changes: group.changes.sort((a, b) => b.changedAt.getTime() - a.changedAt.getTime())
    }))
    .sort((a, b) => a.day.getTime() - b.day.getTime());
}

function normalizeTradeOrderIdValue(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function getLatestOperationForOrder(operations: AccountOperation[] | undefined | null): AccountOperation | null {
  if (!Array.isArray(operations) || operations.length === 0) {
    return null;
  }
  return operations.reduce<AccountOperation | null>((latest, candidate) => {
    if (!latest) {
      return candidate;
    }
    const latestTime = (latest.statusUpdatedAt ?? latest.triggeredAt).getTime();
    const candidateTime = (candidate.statusUpdatedAt ?? candidate.triggeredAt).getTime();
    return candidateTime > latestTime ? candidate : latest;
  }, null);
}

function buildTradeOrderViews(trade: Trade, operations: AccountOperation[] | undefined | null): TradeOrderView[] {
  const operationList = Array.isArray(operations) ? operations : [];
  const operationsByOrder = new Map<string, AccountOperation[]>();

  operationList.forEach(operation => {
    const orderId = normalizeTradeOrderIdValue(operation.orderId);
    if (!orderId) {
      return;
    }
    const key = orderId.toLowerCase();
    if (!operationsByOrder.has(key)) {
      operationsByOrder.set(key, []);
    }
    operationsByOrder.get(key)!.push(operation);
  });

  const seenOrderKeys = new Set<string>();
  const orderViews: TradeOrderView[] = [];

  const registerOrder = (label: string, orderId: string | null | undefined, type: TradeOrderType) => {
    const normalizedId = normalizeTradeOrderIdValue(orderId);
    if (!normalizedId) {
      return;
    }
    const key = normalizedId.toLowerCase();
    seenOrderKeys.add(key);
    const matchingOperations = operationsByOrder.get(key);
    const latestOperation = getLatestOperationForOrder(matchingOperations ?? null);
    orderViews.push({
      label,
      type,
      orderId: normalizedId,
      status: latestOperation?.status ?? 'unknown',
      statusReason: latestOperation?.statusReason ?? latestOperation?.reason ?? null,
      triggeredAt: latestOperation?.triggeredAt ?? null,
      statusUpdatedAt: latestOperation?.statusUpdatedAt ?? latestOperation?.triggeredAt ?? null,
      operationType: latestOperation?.operationType ?? null,
      quantity: latestOperation?.quantity ?? null,
      price: latestOperation?.price ?? null,
      stopLoss: latestOperation?.stopLoss ?? null
    });
  };

  registerOrder('Entry Order', trade.entryOrderId, 'entry');
  registerOrder('Stop Loss Order', trade.stopOrderId, 'stop');
  registerOrder('Exit Order', trade.exitOrderId, 'exit');

  operationsByOrder.forEach((operationEntries, key) => {
    if (seenOrderKeys.has(key)) {
      return;
    }
    const latestOperation = getLatestOperationForOrder(operationEntries);
    if (!latestOperation || !latestOperation.orderId) {
      return;
    }
    orderViews.push({
      label: 'Additional Order',
      type: 'operation',
      orderId: latestOperation.orderId,
      status: latestOperation.status ?? 'unknown',
      statusReason: latestOperation.statusReason ?? latestOperation.reason ?? null,
      triggeredAt: latestOperation.triggeredAt ?? null,
      statusUpdatedAt: latestOperation.statusUpdatedAt ?? latestOperation.triggeredAt ?? null,
      operationType: latestOperation.operationType ?? null,
      quantity: latestOperation.quantity ?? null,
      price: latestOperation.price ?? null,
      stopLoss: latestOperation.stopLoss ?? null
    });
  });

  return orderViews.sort((a, b) => {
    const typeComparison = TRADE_ORDER_TYPE_ORDER.indexOf(a.type) - TRADE_ORDER_TYPE_ORDER.indexOf(b.type);
    if (typeComparison !== 0) {
      return typeComparison;
    }
    const aTime = (a.statusUpdatedAt ?? a.triggeredAt ?? new Date(0)).getTime();
    const bTime = (b.statusUpdatedAt ?? b.triggeredAt ?? new Date(0)).getTime();
    return bTime - aTime;
  });
}

function buildTradeOperationsSummary(operations: AccountOperation[] | undefined | null): TradeOperationsSummary {
  const counts: Record<AccountOperationStatus, number> = {
    pending: 0,
    sent: 0,
    skipped: 0,
    failed: 0
  };

  if (!Array.isArray(operations) || operations.length === 0) {
    return {
      total: 0,
      counts,
      latestStatusUpdatedAt: null
    };
  }

  let latestStatusUpdatedAt: Date | null = null;

  operations.forEach(operation => {
    counts[operation.status] += 1;
    const candidate = operation.statusUpdatedAt || operation.triggeredAt;
    if (!latestStatusUpdatedAt || candidate.getTime() > latestStatusUpdatedAt.getTime()) {
      latestStatusUpdatedAt = candidate;
    }
  });

  return {
    total: operations.length,
    counts,
    latestStatusUpdatedAt
  };
}

function formatTradeChangeField(field: string): string {
  if (!field) {
    return 'Field';
  }
  const withSpaces = field
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
  return withSpaces.charAt(0).toUpperCase() + withSpaces.slice(1);
}

function formatTradeChangeValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '—';
  }

  if (typeof value === 'number') {
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

  if (value instanceof Date) {
    return new Intl.DateTimeFormat('en-GB', {
      timeZone: 'UTC',
      hour12: false,
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    }).format(value);
  }

  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed) && /\d{4}-\d{2}-\d{2}/.test(value)) {
      return new Intl.DateTimeFormat('en-GB', {
        timeZone: 'UTC',
        hour12: false,
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      }).format(new Date(parsed));
    }
    return value;
  }

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

router.get<BacktestParams>('/backtests/:backtestId/trades', requireAuth, async (req, res) => {
  try {
    const { backtestId } = req.params;
    const userId = getReqUserId(req);

    const backtest = await req.db.backtestResults.getBacktestResultById(backtestId, userId);
    if (!backtest) {
      return res.status(404).render('pages/error', {
        title: 'Backtest Not Found',
        error: `Backtest ${backtestId} not found`
      });
    }

    const strategy = await req.db.strategies.getStrategy(backtest.strategyId, userId);
    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${backtest.strategyId} not found`
      });
    }

    const statusFilter = normalizeTradeStatusFilter(req.query.status);
    const tickerFilter = normalizeTickerFilter(req.query.ticker);
    const requestedPage = parsePageParam(req.query.page);
    const pageSize = normalizeTradesPageSize(req.query.pageSize);
    const sortBy = normalizeTradeSortField(req.query.sort);
    const sortDirection = normalizeTradeSortDirection(req.query.direction);

    const totalTrades = await req.db.trades.countBacktestTrades(backtestId, userId, {
      status: statusFilter === 'all' ? undefined : statusFilter,
      ticker: tickerFilter
    });

    const totalPages = Math.max(1, Math.ceil(Math.max(totalTrades, 0) / pageSize));
    const page = Math.min(requestedPage, totalPages);
    const offset = (page - 1) * pageSize;

    const trades =
      totalTrades === 0
        ? []
        : await req.db.trades.getBacktestTrades(backtestId, userId, {
            status: statusFilter === 'all' ? undefined : statusFilter,
            ticker: tickerFilter,
            limit: pageSize,
            offset,
            sortBy,
            sortDirection
          });

    const statusCounts = await req.db.trades.getBacktestTradeStatusCounts(backtestId, userId);
    statusCounts.total = totalTrades;

    const tradesPagePath = `/backtests/${backtestId}/trades`;
    const baseQuery: Record<string, string> = {
      pageSize: String(pageSize),
      sort: sortBy,
      direction: sortDirection
    };
    if (statusFilter !== 'all') {
      baseQuery.status = statusFilter;
    }
    if (tickerFilter) {
      baseQuery.ticker = tickerFilter;
    }

    const pagination = {
      page,
      totalPages,
      total: totalTrades,
      pageSize,
      from: totalTrades === 0 ? 0 : offset + 1,
      to: totalTrades === 0 ? 0 : offset + trades.length,
      hasPrev: page > 1,
      hasNext: page < totalPages,
      firstUrl: page > 1 ? buildTradesPageUrl(tradesPagePath, baseQuery, { page: 1 }) : null,
      prevUrl: page > 1 ? buildTradesPageUrl(tradesPagePath, baseQuery, { page: page - 1 }) : null,
      nextUrl: page < totalPages ? buildTradesPageUrl(tradesPagePath, baseQuery, { page: page + 1 }) : null,
      lastUrl: page < totalPages ? buildTradesPageUrl(tradesPagePath, baseQuery, { page: totalPages }) : null
    };

    const statusOptions = [
      { value: 'all', label: `All (${totalTrades})` },
      { value: 'active', label: `Active (${statusCounts.active ?? 0})` },
      { value: 'closed', label: `Closed (${statusCounts.closed ?? 0})` },
      { value: 'pending', label: `Pending (${statusCounts.pending ?? 0})` },
      { value: 'cancelled', label: `Cancelled (${statusCounts.cancelled ?? 0})` }
    ];

    const statusSummary = [
      { key: 'total', label: 'Total Trades', value: totalTrades },
      { key: 'active', label: 'Active', value: statusCounts.active ?? 0 },
      { key: 'closed', label: 'Closed', value: statusCounts.closed ?? 0 },
      { key: 'pending', label: 'Pending', value: statusCounts.pending ?? 0 },
      { key: 'cancelled', label: 'Cancelled', value: statusCounts.cancelled ?? 0 }
    ];

    const sortOptions = Object.entries(TRADE_SORT_OPTION_LABELS).map(([value, label]) => ({
      value,
      label
    }));

    res.render('pages/trades', {
      mode: 'backtest',
      title: `${strategy.name} Trades`,
      page: 'dashboard',
      user: req.user,
      strategy,
      backtest,
      backtestPeriodLabel: formatBacktestPeriodLabel(backtest.periodMonths),
      trades,
      pagination,
      currentSortLabel: TRADE_SORT_OPTION_LABELS[sortBy],
      filters: {
        status: statusFilter,
        ticker: tickerFilter ?? '',
        sortBy,
        sortDirection,
        pageSize
      },
      statusOptions,
      sortOptions,
      pageSizeOptions: TRADES_PAGE_SIZE_OPTIONS,
      statusSummary,
      statusCounts,
      basePath: tradesPagePath,
      baseQuery,
      backtestLink: `/backtests/${backtest.id}`,
      tradesPagePath
    });
  } catch (error) {
    console.error('Error rendering backtest trades page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load trades'
    });
  }
});

router.get<BacktestParams>('/backtests/:backtestId/trades/date-insights', requireAuth, async (req, res) => {
  try {
    const { backtestId } = req.params;
    const parsedDate = parseDateParam(req.query.date);

    if (!parsedDate) {
      return res.status(400).render('pages/error', {
        title: 'Invalid Date',
        error: 'A valid YYYY-MM-DD date parameter is required'
      });
    }

    const userId = getReqUserId(req);
    const backtest = await req.db.backtestResults.getBacktestResultById(backtestId, userId);

    if (!backtest) {
      return res.status(404).render('pages/error', {
        title: 'Backtest Not Found',
        error: `Backtest ${backtestId} not found`
      });
    }

    const strategy = await req.db.strategies.getStrategy(backtest.strategyId, userId);
    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${backtest.strategyId} not found`
      });
    }

    const { date: targetDate, dateString } = parsedDate;
    const trades: Trade[] = await req.db.trades.getBacktestTradesForDate(backtestId, userId, targetDate);
    const { start: dayStart, end: dayEnd } = getUtcDayBounds(targetDate);

    const startDate = backtest.startDate ? new Date(backtest.startDate) : null;
    const endDate = backtest.endDate ? new Date(backtest.endDate) : null;
    const dateOutOfRange =
      Boolean(startDate && targetDate < startDate) || Boolean(endDate && targetDate > endDate);

    const uniqueTickers = Array.from(
      new Set(
        trades
          .map(trade => (typeof trade.ticker === 'string' ? trade.ticker.toUpperCase() : ''))
          .filter(Boolean)
      )
    );

    const candleLookup: Record<
      string,
      {
        price: number;
        isExact: boolean;
      }
    > = {};
    if (uniqueTickers.length > 0) {
      const candleWindowStart = new Date(targetDate);
      candleWindowStart.setDate(candleWindowStart.getDate() - 7);
      const candlesByTicker = await req.db.candles.getCandles(uniqueTickers, candleWindowStart, targetDate);
      uniqueTickers.forEach(ticker => {
        const candles = candlesByTicker[ticker] || [];
        let selectedCandle = candles.find((candle: Candle) => {
          const candleDate =
            candle.date instanceof Date ? candle.date : new Date(candle.date as unknown as string);
          return !Number.isNaN(candleDate.getTime()) && candleDate.toISOString().split('T')[0] === dateString;
        });
        if (!selectedCandle && candles.length > 0) {
          selectedCandle = candles[candles.length - 1];
        }
        if (selectedCandle && typeof selectedCandle.close === 'number') {
          const candleDate =
            selectedCandle.date instanceof Date
              ? selectedCandle.date
              : new Date(selectedCandle.date as unknown as string);
          candleLookup[ticker] = {
            price: selectedCandle.close,
            isExact: Boolean(candleDate) && candleDate.toISOString().split('T')[0] === dateString
          };
        }
      });
    }

    const insights = trades.map(trade => {
      const rawTicker = typeof trade.ticker === 'string' ? trade.ticker : '';
      const ticker = rawTicker ? rawTicker.toUpperCase() : 'UNKNOWN';
      const entryPrice = Number(trade.price) || 0;
      const quantity = Number(trade.quantity) || 0;
      const entryDateString = toDateOnlyString(trade.date);
      const exitDateString = toDateOnlyString(trade.exitDate);
      const isOpened = entryDateString === dateString;
      const isClosed = exitDateString === dateString;
      const exitTime = trade.exitDate ? trade.exitDate.getTime() : Number.POSITIVE_INFINITY;
      const isActive =
        (trade.date ? trade.date.getTime() : Number.NEGATIVE_INFINITY) <= dayEnd.getTime() &&
        exitTime >= dayStart.getTime();

      let priceOnDate: number = entryPrice;
      let priceSource: 'exit' | 'close' | 'closeFallback' | 'entry' = 'entry';
      if (isClosed && typeof trade.exitPrice === 'number' && trade.exitPrice > 0) {
        priceOnDate = trade.exitPrice;
        priceSource = 'exit';
      } else {
        const candleInfo = candleLookup[ticker];
        if (candleInfo && typeof candleInfo.price === 'number' && candleInfo.price > 0) {
          priceOnDate = candleInfo.price;
          priceSource = candleInfo.isExact ? 'close' : 'closeFallback';
        }
      }

      const moveValue = (priceOnDate - entryPrice) * quantity;
      const movePercent = entryPrice !== 0 ? ((priceOnDate - entryPrice) / entryPrice) * 100 : 0;
      const absMovePercent = Math.abs(movePercent);

      const eventTags = new Set<string>();
      if (isOpened) {
        eventTags.add('Opened');
      }
      if (isActive) {
        eventTags.add('Active');
      }
      if (isClosed) {
        eventTags.add('Closed');
      }

      return {
        id: trade.id,
        ticker,
        quantity,
        entryPrice,
        priceOnDate,
        priceSource,
        moveValue,
        movePercent,
        absMovePercent,
        entryDate: trade.date,
        exitDate: trade.exitDate ?? null,
        statusTags: Array.from(eventTags),
        tradeUrl: `/trades/${trade.id}`,
        direction: movePercent >= 0 ? 'up' : 'down'
      };
    });

    const sortedInsights = insights.sort((a, b) => {
      if (b.absMovePercent !== a.absMovePercent) {
        return b.absMovePercent - a.absMovePercent;
      }
      return b.moveValue - a.moveValue;
    });

    const rankedInsights = sortedInsights.map((item, index) => ({
      ...item,
      rank: index + 1
    }));

    const summary = {
      total: rankedInsights.length,
      opened: rankedInsights.filter(item => item.statusTags.includes('Opened')).length,
      active: rankedInsights.filter(item => item.statusTags.includes('Active')).length,
      closed: rankedInsights.filter(item => item.statusTags.includes('Closed')).length
    };

    res.render('pages/trade-date-insights', {
      title: `${strategy.name} - ${dateString} Trade Outliers`,
      page: 'dashboard',
      user: req.user,
      strategy,
      backtest,
      backtestPeriodLabel: formatBacktestPeriodLabel(backtest.periodMonths),
      trades: rankedInsights,
      summary,
      targetDate: targetDate,
      targetDateString: dateString,
      dateOutOfRange,
      tradesPageUrl: `/backtests/${backtestId}/trades`,
      backtestLink: `/backtests/${backtest.id}`,
      currentUrl: getCurrentUrl(req)
    });
  } catch (error) {
    console.error('Error rendering trade date insights page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load trade data for selected date'
    });
  }
});

router.get<StrategyIdParams>('/strategies/:strategyId/live-trades', requireAuth, async (req, res) => {
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

    const statusFilter = normalizeTradeStatusFilter(req.query.status);
    const tickerFilter = normalizeTickerFilter(req.query.ticker);
    const requestedPage = parsePageParam(req.query.page);
    const pageSize = normalizeTradesPageSize(req.query.pageSize);
    const sortBy = normalizeTradeSortField(req.query.sort);
    const sortDirection = normalizeTradeSortDirection(req.query.direction);

    const totalTrades = await req.db.trades.countLiveTradesForStrategy(strategyId, userId, {
      status: statusFilter === 'all' ? undefined : statusFilter,
      ticker: tickerFilter
    });
    const totalPages = Math.max(1, Math.ceil(Math.max(totalTrades, 0) / pageSize));
    const page = Math.min(requestedPage, totalPages);
    const offset = (page - 1) * pageSize;

    const trades =
      totalTrades === 0
        ? []
        : await req.db.trades.getLiveTradesForStrategy(strategyId, userId, {
            status: statusFilter === 'all' ? undefined : statusFilter,
            ticker: tickerFilter,
            limit: pageSize,
            offset,
            sortBy,
            sortDirection
          });

    const statusCounts = await req.db.trades.getLiveTradeStatusCounts(strategyId, userId);
    statusCounts.total = totalTrades;

    const tradesPagePath = `/strategies/${strategyId}/live-trades`;
    const baseQuery: Record<string, string> = {
      pageSize: String(pageSize),
      sort: sortBy,
      direction: sortDirection
    };
    if (statusFilter !== 'all') {
      baseQuery.status = statusFilter;
    }
    if (tickerFilter) {
      baseQuery.ticker = tickerFilter;
    }

    const pagination = {
      page,
      totalPages,
      total: totalTrades,
      pageSize,
      from: totalTrades === 0 ? 0 : offset + 1,
      to: totalTrades === 0 ? 0 : offset + trades.length,
      hasPrev: page > 1,
      hasNext: page < totalPages,
      firstUrl: page > 1 ? buildTradesPageUrl(tradesPagePath, baseQuery, { page: 1 }) : null,
      prevUrl: page > 1 ? buildTradesPageUrl(tradesPagePath, baseQuery, { page: page - 1 }) : null,
      nextUrl: page < totalPages ? buildTradesPageUrl(tradesPagePath, baseQuery, { page: page + 1 }) : null,
      lastUrl: page < totalPages ? buildTradesPageUrl(tradesPagePath, baseQuery, { page: totalPages }) : null
    };

    const statusOptions = [
      { value: 'all', label: `All (${totalTrades})` },
      { value: 'active', label: `Active (${statusCounts.active ?? 0})` },
      { value: 'closed', label: `Closed (${statusCounts.closed ?? 0})` },
      { value: 'pending', label: `Pending (${statusCounts.pending ?? 0})` },
      { value: 'cancelled', label: `Cancelled (${statusCounts.cancelled ?? 0})` }
    ];

    const statusSummary = [
      { key: 'total', label: 'Total Trades', value: totalTrades },
      { key: 'active', label: 'Active', value: statusCounts.active ?? 0 },
      { key: 'closed', label: 'Closed', value: statusCounts.closed ?? 0 },
      { key: 'pending', label: 'Pending', value: statusCounts.pending ?? 0 },
      { key: 'cancelled', label: 'Cancelled', value: statusCounts.cancelled ?? 0 }
    ];

    const sortOptions = Object.entries(TRADE_SORT_OPTION_LABELS).map(([value, label]) => ({
      value,
      label
    }));

    let liveAccount: { name: string; provider: string; environment: string } | null = null;
    if (strategy.accountId) {
      const account = await req.db.accounts.getAccountById(strategy.accountId, userId);
      if (account) {
        liveAccount = {
          name: account.name,
          provider: account.provider,
          environment: account.environment
        };
      }
    }

    res.render('pages/trades', {
      mode: 'live',
      title: `${strategy.name} Live Trades`,
      page: 'dashboard',
      user: req.user,
      strategy,
      liveAccount,
      trades,
      pagination,
      currentSortLabel: TRADE_SORT_OPTION_LABELS[sortBy],
      filters: {
        status: statusFilter,
        ticker: tickerFilter ?? '',
        sortBy,
        sortDirection,
        pageSize
      },
      statusOptions,
      sortOptions,
      pageSizeOptions: TRADES_PAGE_SIZE_OPTIONS,
      statusSummary,
      statusCounts,
      basePath: tradesPagePath,
      baseQuery,
      backtestLink: `/strategies/${strategy.id}`,
      tradesPagePath
    });
  } catch (error) {
    console.error('Error rendering live trades page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load live trades'
    });
  }
});

router.get<TradeParams>('/trades/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = getReqUserId(req);
    const trade = await req.db.trades.getTrade(id, userId);

    if (!trade) {
      return res.status(404).render('pages/error', {
        title: 'Trade Not Found',
        error: `Trade ${id} not found`
      });
    }

    const strategy = await req.db.strategies.getStrategy(trade.strategyId, userId);
    if (!strategy) {
      return res.status(404).render('pages/error', {
        title: 'Strategy Not Found',
        error: `Strategy ${trade.strategyId} not found`
      });
    }
    const template = req.strategyRegistry.getTemplate(strategy.templateId);

    const performance = await req.db.backtestResults.getStoredStrategyPerformance(trade.strategyId);

    const tradeDate = new Date(trade.date);
    const now = new Date();

    let startDate: Date;
    let endDate: Date;

    if (tradeDate > now) {
      startDate = new Date(tradeDate);
      startDate.setFullYear(tradeDate.getFullYear() - 1);
      endDate = new Date(tradeDate);
      endDate.setFullYear(tradeDate.getFullYear() + 1);
    } else {
      startDate = new Date(tradeDate);
      startDate.setFullYear(tradeDate.getFullYear() - 1);

      const oneYearAfterTrade = new Date(tradeDate);
      oneYearAfterTrade.setFullYear(tradeDate.getFullYear() + 1);

      endDate = oneYearAfterTrade < now ? oneYearAfterTrade : now;

      if (trade.exitDate) {
        const exitDate = new Date(trade.exitDate);
        const exitDatePlusMonth = new Date(exitDate.getTime() + (30 * 24 * 60 * 60 * 1000));
        if (exitDatePlusMonth > endDate) {
          endDate = exitDatePlusMonth;
        }
      }
    }

    const chartDataResult = await req.db.candles.getCandles([trade.ticker], startDate, endDate);
    const chartData = chartDataResult[trade.ticker] || [];

    const expectedEndDate = performance?.avgTradeDuration
      ? new Date(trade.date.getTime() + (performance.avgTradeDuration * 24 * 60 * 60 * 1000))
      : null;

    let expectedPriceTarget = null;
    if (performance?.avgWinningPnl && performance?.avgTradeDuration) {
      const tradeValue = trade.quantity * trade.price;
      const expectedPnlPercent = tradeValue > 0 ? (performance.avgWinningPnl / tradeValue) : 0;
      expectedPriceTarget = trade.price * (1 + expectedPnlPercent);
    }

    let simulatedFutureData = [];
    if (expectedEndDate && expectedPriceTarget && chartData.length > 0 && trade.status === 'active') {
      const lastCandle = chartData[chartData.length - 1];
      const lastPrice = lastCandle.close;

      if (expectedEndDate > now) {
        const simulationStartDate = now;
        const daysToExit = Math.ceil((expectedEndDate.getTime() - simulationStartDate.getTime()) / (1000 * 60 * 60 * 24));

        if (daysToExit > 0) {
          for (let i = 1; i <= daysToExit; i++) {
            const futureDate = new Date(simulationStartDate.getTime() + (i * 24 * 60 * 60 * 1000));

            const progress = i / daysToExit;
            const simulatedPrice = lastPrice + (expectedPriceTarget - lastPrice) * progress;

            const volatility = 0.02;
            const randomFactor = (Math.random() - 0.5) * volatility;
            const finalPrice = simulatedPrice * (1 + randomFactor);

            simulatedFutureData.push({
              ticker: trade.ticker,
              date: futureDate,
              open: simulatedPrice,
              high: Math.max(simulatedPrice, finalPrice),
              low: Math.min(simulatedPrice, finalPrice),
              close: finalPrice,
              volumeShares: Math.floor(Math.random() * 1000000) + 100000
            });
          }
        }
      }
    }

    const stopLossPercent = trade.stopLoss && trade.price > 0
      ? ((trade.stopLoss - trade.price) / trade.price) * 100
      : null;

    const tradeValue = Math.abs(trade.price * trade.quantity);
    const pnlPercent = trade.pnl && tradeValue > 0
      ? (trade.pnl / tradeValue) * 100
      : null;

    const tradeChangeGroups = buildTradeChangeGroups(trade.changes);
    const tradeOperations = await req.db.accountOperations.getAccountOperationsForTrade(trade.id, undefined, {
      sortBy: 'triggeredAt',
      order: 'desc'
    });
    const tradeOperationsSummary = buildTradeOperationsSummary(tradeOperations);
    const tradeOperationsStatusBreakdown = TRADE_OPERATION_STATUS_ORDER.map(status => ({
      status,
      count: tradeOperationsSummary.counts[status] ?? 0
    }));
    const tradeOrders = buildTradeOrderViews(trade, tradeOperations);
    const tradingViewChartsEnabledRaw = await req.db.settings.getSettingValue(
      SETTING_KEYS.TRADINGVIEW_CHARTS_ENABLED
    );
    const showTradingViewChart = tradingViewChartsEnabledRaw?.trim().toLowerCase() !== 'false';

    res.render('pages/trade-detail', {
      title: `Trade ${trade.ticker} - Trade Details`,
      page: 'dashboard',
      user: req.user,
      trade: {
        ...trade,
        stopLossPercent,
        pnlPercent
      },
      tradeChangeGroups,
      tradeOperations,
      tradeOperationsSummary,
      tradeOperationsStatusBreakdown,
      hasTradeOperations: tradeOperationsSummary.total > 0,
      tradeOrders,
      hasTradeOrders: tradeOrders.length > 0,
      strategy: {
        ...strategy,
        performance
      },
      template,
      chartData,
      simulatedFutureData,
      expectedEndDate,
      expectedPriceTarget,
      showTradingViewChart,
      currentUrl: getCurrentUrl(req)
    });
  } catch (error) {
    console.error('Error rendering trade detail:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load trade details'
    });
  }
});

export default router;
