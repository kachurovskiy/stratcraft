import express, { NextFunction, Request, Response } from 'express';
import { TickerQueryParams, TickerParams } from '../../shared/types/Express';
import { BacktestScope, Candle, Strategy, TickerInfo } from '../../shared/types/StrategyTemplate';
import { formatBacktestPeriodLabel, getReqUserId, parsePageParam } from './utils';

const router = express.Router();

const TICKERS_PER_PAGE = 100;
const ASSET_TYPE_LABELS: Record<string, string> = {
  equity: 'Equity',
  etf: 'ETF',
  leveraged_2x: 'Leveraged 2x',
  leveraged_3x: 'Leveraged 3x',
  leveraged_5x: 'Leveraged 5x',
  inverse_etf: 'Inverse ETF',
  inverse_leveraged_2x: 'Inverse Leveraged 2x',
  inverse_leveraged_3x: 'Inverse Leveraged 3x',
  inverse_leveraged_5x: 'Inverse Leveraged 5x',
  commodity_trust: 'Commodity Trust',
  bond_etf: 'Bond ETF',
  income_etf: 'Income ETF'
};
const SIGNAL_SIMULATION_LOOKBACK_DAYS = 365;

interface PriceVolumePoint {
  symbol: string;
  price: number;
  usdVolume: number;
  tradable: boolean;
  shortable: boolean;
  easyToBorrow: boolean;
}

interface NameWordStat {
  word: string;
  count: number;
  percentage: number;
}

interface TickerAnalyticsPayload {
  hasData: boolean;
  total: number;
  trainingCount: number;
  validationCount: number;
  zeroCandleCount: number;
  candleCounts: number[];
  firstCandleDates: string[];
  lastCandleDates: string[];
  latestPriceVolumePoints: PriceVolumePoint[];
}

const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

const requireAdmin = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
};

function buildPageWindow(current: number, total: number, maxVisible = 7): number[] {
  if (total <= 0) {
    return [];
  }
  const windowSize = Math.min(total, maxVisible);
  let start = Math.max(1, current - Math.floor(windowSize / 2));
  let end = start + windowSize - 1;
  if (end > total) {
    end = total;
    start = Math.max(1, end - windowSize + 1);
  }
  return Array.from({ length: end - start + 1 }, (_, idx) => start + idx);
}

function toFiniteNumber(value?: number | string | null): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const numericValue = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(numericValue) ? numericValue : null;
}

function toPositiveNumber(value?: number | string | null): number | null {
  const finite = toFiniteNumber(value);
  if (finite === null || finite <= 0) {
    return null;
  }
  return finite;
}

function deriveLatestUsdVolume(ticker: TickerInfo): number | null {
  const direct = toPositiveNumber(ticker.latestUsdVolume ?? null);
  if (direct !== null) {
    return direct;
  }
  const close = toPositiveNumber(ticker.latestClose ?? null);
  const shares = toPositiveNumber(ticker.latestVolumeShares ?? null);
  if (close !== null && shares !== null) {
    return toPositiveNumber(close * shares);
  }
  return null;
}

function buildTickerAnalytics(tickers: (TickerInfo & { tradeCount: number })[]): TickerAnalyticsPayload {
  const candleCounts = tickers
    .map(ticker => toFiniteNumber(ticker.candleCount ?? null))
    .filter((value): value is number => value !== null && value > 0);

  const firstCandleDates = tickers
    .map(ticker => ticker.firstCandleDate ? ticker.firstCandleDate : null)
    .filter((date): date is Date => date instanceof Date && !Number.isNaN(date.getTime()));

  const lastCandleDates = tickers
    .map(ticker => ticker.lastCandleDate ? ticker.lastCandleDate : null)
    .filter((date): date is Date => date instanceof Date && !Number.isNaN(date.getTime()));

  const latestPriceVolumePoints: PriceVolumePoint[] = tickers
    .map(ticker => {
      const price = toPositiveNumber(ticker.latestClose ?? null);
      const usdVolume = deriveLatestUsdVolume(ticker);
      if (price === null || usdVolume === null) {
        return null;
      }
      return {
        symbol: ticker.symbol,
        price,
        usdVolume,
        tradable: Boolean(ticker.tradable),
        shortable: Boolean(ticker.shortable),
        easyToBorrow: Boolean(ticker.easyToBorrow)
      };
    })
    .filter((point): point is PriceVolumePoint => point !== null);

  return {
    hasData: tickers.length > 0,
    total: tickers.length,
    trainingCount: tickers.filter(ticker => ticker.training).length,
    validationCount: tickers.filter(ticker => !ticker.training).length,
    zeroCandleCount: tickers.filter(ticker => (ticker.candleCount ?? 0) === 0).length,
    candleCounts,
    firstCandleDates: tickers
      .map(ticker => ticker.firstCandleDate ? ticker.firstCandleDate.toISOString() : null)
      .filter((value): value is string => typeof value === 'string'),
    lastCandleDates: tickers
      .map(ticker => ticker.lastCandleDate ? ticker.lastCandleDate.toISOString() : null)
      .filter((value): value is string => typeof value === 'string'),
    latestPriceVolumePoints
  };
}

const NAME_WORD_LIMIT = 50;
const MIN_NAME_WORD_LENGTH = 2;

function normalizeNameWord(word: string): string | null {
  if (!word) {
    return null;
  }
  const trimmed = word.trim().replace(/^[^A-Za-z0-9]+|[^A-Za-z0-9]+$/g, '');
  if (!trimmed) {
    return null;
  }
  const normalized = trimmed.toUpperCase();
  if (normalized.length < MIN_NAME_WORD_LENGTH) {
    return null;
  }
  if (!/[A-Z0-9]/.test(normalized)) {
    return null;
  }
  return normalized;
}

function extractNameWords(name?: string | null): Set<string> {
  const normalizedWords = new Set<string>();
  if (!name || typeof name !== 'string') {
    return normalizedWords;
  }
  const parts = name.split(/[^A-Za-z0-9]+/);
  for (const part of parts) {
    const normalized = normalizeNameWord(part);
    if (normalized) {
      normalizedWords.add(normalized);
    }
  }
  return normalizedWords;
}

function buildNameWordStats(
  tickers: TickerInfo[],
  limit = NAME_WORD_LIMIT
): { stats: NameWordStat[]; totalNamedTickers: number } {
  const counts = new Map<string, number>();
  let totalNamedTickers = 0;
  const normalizedLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : undefined;

  for (const ticker of tickers) {
    const words = extractNameWords(ticker.name);
    if (!words.size) {
      continue;
    }
    totalNamedTickers += 1;
    words.forEach(word => {
      counts.set(word, (counts.get(word) ?? 0) + 1);
    });
  }

  const stats = Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, normalizedLimit)
    .map(([word, count]) => ({
      word,
      count,
      percentage: totalNamedTickers > 0 ? count / totalNamedTickers : 0
    }));

  return {
    stats,
    totalNamedTickers
  };
}

router.get('/', requireAuth, async (req: Request, res: Response) => {
  try {
    const queryParams = req.query as TickerQueryParams;
    const { search, sort = 'volumeUsd', sortDirection = 'desc' } = queryParams;
    let tickers = await req.db.tickers.getTickersWithTradeCounts();

    if (search) {
      const searchLower = search.toLowerCase();
      tickers = tickers.filter((t: TickerInfo) => {
        const symbolMatch = t.symbol.toLowerCase().includes(searchLower);
        const nameMatch = typeof t.name === 'string' && t.name.toLowerCase().includes(searchLower);
        return symbolMatch || nameMatch;
      });
    }

    const isAscending = sortDirection === 'asc';

    if (sort === 'volumeUsd') {
      tickers.sort((a: TickerInfo, b: TickerInfo) => {
        const result = (b.volumeUsd || 0) - (a.volumeUsd || 0);
        return isAscending ? -result : result;
      });
    } else if (sort === 'symbol') {
      tickers.sort((a: TickerInfo, b: TickerInfo) => {
        const result = a.symbol.localeCompare(b.symbol);
        return isAscending ? result : -result;
      });
    } else if (sort === 'maxFluctuationRatio' as any) {
      tickers.sort((a: TickerInfo, b: TickerInfo) => {
        const result = (b.maxFluctuationRatio || 0) - (a.maxFluctuationRatio || 0);
        return isAscending ? -result : result;
      });
    } else if (sort === 'candleCount') {
      tickers.sort((a: TickerInfo, b: TickerInfo) => {
        const result = (b.candleCount || 0) - (a.candleCount || 0);
        return isAscending ? -result : result;
      });
    } else if (sort === 'lastCandleDate') {
      tickers.sort((a: TickerInfo, b: TickerInfo) => {
        if (!a.lastCandleDate && !b.lastCandleDate) return 0;
        if (!a.lastCandleDate) return isAscending ? -1 : 1;
        if (!b.lastCandleDate) return isAscending ? 1 : -1;
        const result = b.lastCandleDate.getTime() - a.lastCandleDate.getTime();
        return isAscending ? -result : result;
      });
    } else if (sort === 'tradeCount') {
      tickers.sort((a: TickerInfo, b: TickerInfo) => {
        const result = (b.tradeCount || 0) - (a.tradeCount || 0);
        return isAscending ? -result : result;
      });
    }

    const zeroCandleTickers = tickers.filter((t: TickerInfo) => t.candleCount === 0);
    const zeroCandlesCount = zeroCandleTickers.length;
    const zeroCandlesEasyToBorrowCount = zeroCandleTickers.filter((t: TickerInfo) => t.easyToBorrow).length;
    const zeroCandlesTradableCount = zeroCandleTickers.filter((t: TickerInfo) => t.tradable).length;
    const zeroCandlesNotTradableCount = zeroCandleTickers.filter((t: TickerInfo) => !t.tradable).length;
    const validationTickersCount = tickers.filter((t: TickerInfo) => !t.training).length;
    const totalTickers = tickers.length;
    const tickerAnalytics = buildTickerAnalytics(tickers as (TickerInfo & { tradeCount: number })[]);
    const { stats: nameWordStats, totalNamedTickers } = buildNameWordStats(tickers);
    const requestedPage = parsePageParam(queryParams.page);
    const totalPages = Math.max(1, Math.ceil(Math.max(totalTickers, 0) / TICKERS_PER_PAGE));
    const page = Math.min(requestedPage, totalPages);
    const offset = (page - 1) * TICKERS_PER_PAGE;
    const paginatedTickers = tickers.slice(offset, offset + TICKERS_PER_PAGE);
    const pagination = {
      page,
      totalPages,
      total: totalTickers,
      pageSize: TICKERS_PER_PAGE,
      from: totalTickers === 0 ? 0 : offset + 1,
      to: totalTickers === 0 ? 0 : Math.min(offset + TICKERS_PER_PAGE, totalTickers),
      hasPrev: page > 1,
      hasNext: page < totalPages,
      prevPage: page > 1 ? page - 1 : null,
      nextPage: page < totalPages ? page + 1 : null,
      pageNumbers: totalTickers === 0 ? [] : buildPageWindow(page, totalPages),
      shouldShowControls: totalPages > 1
    };

    res.render('pages/tickers', {
      title: 'All Tickers',
      page: 'tickers',
      user: req.user,
      tickers: paginatedTickers,
      assetTypeLabels: ASSET_TYPE_LABELS,
      search: search || '',
      sort,
      sortDirection,
      zeroCandlesCount,
      zeroCandlesEasyToBorrowCount,
      zeroCandlesTradableCount,
      zeroCandlesNotTradableCount,
      validationTickersCount,
      tickerAnalytics,
      nameWordStats,
      nameWordTickerCount: totalNamedTickers,
      pagination
    });
  } catch (error) {
    console.error('Error rendering tickers page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load tickers'
    });
  }
});

router.get<TickerParams>('/:symbol', requireAuth, async (req, res) => {
  try {
    const { symbol } = req.params;
    const ticker = await req.db.tickers.getTicker(symbol);

    if (!ticker) {
      return res.status(404).render('pages/error', {
        title: 'Ticker Not Found',
        error: `Ticker ${symbol} not found`
      });
    }

    const userId = getReqUserId(req);
    const tickerScope: BacktestScope = ticker.training ? 'training' : 'validation';
    const tickerBacktestScopeLabel = tickerScope === 'validation' ? 'Validation' : 'Training';

    const candlesResult: Record<string, Candle[]> = await req.db.candles.getCandles([symbol]);
    const candles = candlesResult[symbol] || [];
    const latestCandle = candles.length > 0 ? candles[candles.length - 1] : null;
    const firstCandle = candles.length > 0 ? candles[0] : null;
    const candleCount = candles.length;

    let minUsdVolume: number | null = null;
    let maxUsdVolume: number | null = null;
    let minUnadjustedClose: number | null = null;
    let maxUnadjustedClose: number | null = null;
    let minLow: number | null = null;
    let maxHigh: number | null = null;

    if (candleCount > 0) {
      for (const candle of candles) {
        const close = Number(candle.close ?? 0);
        const unadjustedClose = Number(candle.unadjustedClose ?? candle.close ?? 0);
        const low = Number(candle.low ?? 0);
        const high = Number(candle.high ?? 0);
        const volumeShares = Number(candle.volumeShares ?? 0);

        if (
          !Number.isFinite(close) ||
          !Number.isFinite(unadjustedClose) ||
          !Number.isFinite(low) ||
          !Number.isFinite(high) ||
          !Number.isFinite(volumeShares)
        ) {
          continue;
        }

        const usdVolume = close * volumeShares;
        if (!Number.isFinite(usdVolume)) {
          continue;
        }

        if (minUsdVolume === null || usdVolume < minUsdVolume) {
          minUsdVolume = usdVolume;
        }
        if (maxUsdVolume === null || usdVolume > maxUsdVolume) {
          maxUsdVolume = usdVolume;
        }

        if (minUnadjustedClose === null || unadjustedClose < minUnadjustedClose) {
          minUnadjustedClose = unadjustedClose;
        }
        if (maxUnadjustedClose === null || unadjustedClose > maxUnadjustedClose) {
          maxUnadjustedClose = unadjustedClose;
        }

        if (minLow === null || low < minLow) {
          minLow = low;
        }
        if (maxHigh === null || high > maxHigh) {
          maxHigh = high;
        }
      }
    }

    const signalSimulationEnd = new Date();
    const signalSimulationStart = new Date(signalSimulationEnd);
    signalSimulationStart.setUTCFullYear(signalSimulationStart.getUTCFullYear() - 1);
    signalSimulationStart.setUTCHours(0, 0, 0, 0);
    signalSimulationEnd.setUTCHours(23, 59, 59, 999);

    const [backtestPerformanceRows, tickerSignals, strategies] = await Promise.all([
      req.db.backtestResults.getTickerBacktestPerformance(symbol, userId, {
        tickerScope
      }),
      req.db.signals.getSignalsForTicker(symbol, userId, signalSimulationStart, signalSimulationEnd),
      req.db.strategies.getStrategies(userId)
    ]);

    const periodSet = new Set<number>();
    const strategyRows: Array<{
      strategyId: string;
      strategyName: string;
      cells: Record<number, {
        avgReturnPercent: number | null;
        tradeCount: number;
        hasData: boolean;
        backtestResultId: string;
        backtestCreatedAt: Date | null;
      }>;
    }> = [];
    const strategyIndex: Record<string, number> = {};

    for (const entry of backtestPerformanceRows) {
      const periodMonths = Number(entry.periodMonths);
      if (!Number.isFinite(periodMonths) || periodMonths <= 0) {
        continue;
      }
      periodSet.add(periodMonths);

      const tradeCount = Number(entry.tradeCount) || 0;
      const avgReturnNumeric =
        typeof entry.avgReturnPercent === 'number'
          ? entry.avgReturnPercent
          : Number(entry.avgReturnPercent);
      const normalizedAvgReturn = Number.isFinite(avgReturnNumeric) ? avgReturnNumeric : null;
      const hasData = tradeCount > 0 && normalizedAvgReturn !== null;
      const backtestCreatedAt =
        entry.backtestCreatedAt instanceof Date && !Number.isNaN(entry.backtestCreatedAt.getTime())
          ? entry.backtestCreatedAt
          : null;

      if (!(entry.strategyId in strategyIndex)) {
        strategyIndex[entry.strategyId] = strategyRows.length;
        strategyRows.push({
          strategyId: entry.strategyId,
          strategyName:
            typeof entry.strategyName === 'string' && entry.strategyName.length > 0
              ? entry.strategyName
              : 'Unnamed strategy',
          cells: {}
        });
      }

      const strategyRow = strategyRows[strategyIndex[entry.strategyId]];
      const existingCell = strategyRow.cells[periodMonths];
      const shouldReplace =
        !existingCell ||
        (backtestCreatedAt !== null &&
          (!existingCell.backtestCreatedAt ||
            backtestCreatedAt.getTime() > existingCell.backtestCreatedAt.getTime()));

      if (shouldReplace) {
        strategyRow.cells[periodMonths] = {
          avgReturnPercent: hasData ? normalizedAvgReturn : null,
          tradeCount,
          hasData,
          backtestResultId: entry.backtestResultId,
          backtestCreatedAt
        };
      }
    }

    strategyRows.sort((a, b) => a.strategyName.localeCompare(b.strategyName));

    const tickerBacktestTable = {
      periods: Array.from(periodSet)
        .sort((a, b) => b - a)
        .map(months => ({
          months,
          label: formatBacktestPeriodLabel(months)
        })),
      rows: strategyRows
    };

    const candleCloseByDate = new Map<string, number>();
    const candlePricesInWindow: Array<{ date: Date; price: number }> = [];
    for (const candle of candles) {
      if (!candle || !candle.date) {
        continue;
      }
      const dateValue = candle.date instanceof Date ? candle.date : new Date(candle.date);
      if (Number.isNaN(dateValue.getTime())) {
        continue;
      }
      const isoDate = dateValue.toISOString().split('T')[0];
      const closePrice = Number(candle.close);
      if (!isoDate || !Number.isFinite(closePrice)) {
        continue;
      }
      candleCloseByDate.set(isoDate, closePrice);
      if (dateValue >= signalSimulationStart && dateValue <= signalSimulationEnd) {
        candlePricesInWindow.push({ date: dateValue, price: closePrice });
      }
    }
    candlePricesInWindow.sort((a, b) => a.date.getTime() - b.date.getTime());

    const strategyNameMap = new Map<string, string>();
    strategies.forEach((strategy: Strategy) => {
      const strategyName =
        typeof strategy.name === 'string' && strategy.name.length > 0
          ? strategy.name
          : 'Unnamed strategy';
      strategyNameMap.set(strategy.id, strategyName);
    });

    const signalsByStrategy = new Map<
      string,
      Array<{ date: Date; ticker: string; strategyId: string; action: 'buy' | 'sell'; confidence: number | null }>
    >();
    for (const sig of tickerSignals) {
      if (!sig || typeof sig.strategyId !== 'string' || sig.strategyId.length === 0) {
        continue;
      }
      if (!signalsByStrategy.has(sig.strategyId)) {
        signalsByStrategy.set(sig.strategyId, []);
      }
      signalsByStrategy.get(sig.strategyId)!.push(sig);
    }

    const toIsoDate = (date: Date) => date.toISOString().split('T')[0];

    const tickerSignalSimulations = Array.from(signalsByStrategy.entries())
      .map(([strategyId, signals]) => {
        const buySignalCount = signals.filter(sig => sig.action === 'buy').length;
        const sellSignalCount = signals.filter(sig => sig.action === 'sell').length;

        const sortedSignals = signals
          .map(sig => {
            const dateValue = sig.date instanceof Date ? sig.date : new Date(sig.date);
            return { ...sig, date: dateValue };
          })
          .filter(sig => sig.date instanceof Date && !Number.isNaN(sig.date.getTime()))
          .sort((a, b) => a.date.getTime() - b.date.getTime());

        const trades: Array<{
          entryDate: Date;
          exitDate: Date;
          entryPrice: number;
          exitPrice: number;
          pnl: number;
          pnlPercent: number | null;
          entryConfidence: number | null;
        }> = [];

        const openPositions: Array<{ entryDate: Date; entryPrice: number; entryConfidence: number | null }> = [];
        let totalBuyCost = 0;
        let totalSellValue = 0;

        const closeAllPositions = (exitDate: Date, exitPrice: number) => {
          if (!openPositions.length) return;

          for (const position of openPositions) {
            const pnl = exitPrice - position.entryPrice;
            const pnlPercent =
              position.entryPrice !== 0 && Number.isFinite(position.entryPrice)
                ? (pnl / position.entryPrice) * 100
                : null;

            trades.push({
              entryDate: position.entryDate,
              exitDate,
              entryPrice: position.entryPrice,
              exitPrice,
              pnl,
              pnlPercent: Number.isFinite(pnlPercent) ? pnlPercent : null,
              entryConfidence: position.entryConfidence
            });

            totalBuyCost += position.entryPrice;
            totalSellValue += exitPrice;
          }

          openPositions.length = 0;
        };

        for (const sig of sortedSignals) {
          const isoDate = toIsoDate(sig.date);
          const price = candleCloseByDate.get(isoDate);
          if (price === undefined || !Number.isFinite(price)) {
            continue;
          }

          if (sig.action === 'buy') {
            const rawConfidence =
              typeof sig.confidence === 'number'
                ? sig.confidence
                : sig.confidence !== undefined && sig.confidence !== null
                  ? Number(sig.confidence)
                  : null;
            const entryConfidence = Number.isFinite(rawConfidence as number) ? (rawConfidence as number) : null;
            openPositions.push({ entryDate: sig.date, entryPrice: price, entryConfidence });
            continue;
          }

          if (sig.action === 'sell') {
            closeAllPositions(sig.date, price);
          }
        }

        if (openPositions.length > 0) {
          const exitCandidate = candlePricesInWindow.reduce<{ date: Date | null; price: number | null }>(
            (best, candle) => {
              if (candle.date <= signalSimulationEnd && (!best.date || candle.date > best.date)) {
                return { date: candle.date, price: candle.price };
              }
              return best;
            },
            { date: null, price: null }
          );

          if (exitCandidate.price !== null && exitCandidate.date) {
            closeAllPositions(exitCandidate.date, exitCandidate.price);
          }
        }

        const totalTrades = trades.length;
        const profitableTrades = trades.filter(trade => (trade.pnl ?? 0) > 0).length;
        const winRatePercent = totalTrades > 0 ? (profitableTrades / totalTrades) * 100 : null;
        const totalProfitPercent =
          totalBuyCost > 0 ? ((totalSellValue - totalBuyCost) / totalBuyCost) * 100 : null;

        return {
          strategyId,
          strategyName: strategyNameMap.get(strategyId) ?? strategyId,
          trades,
          totalTrades,
          profitableTrades,
          winRatePercent,
          totalProfitPercent,
          signalCount: signals.length,
          buySignalCount,
          sellSignalCount
        };
      });

    if (candlePricesInWindow.length > 0) {
      const first = candlePricesInWindow[0];
      const last = candlePricesInWindow[candlePricesInWindow.length - 1];
      const pnl = last.price - first.price;
      const pnlPercent =
        first.price !== 0 && Number.isFinite(first.price) ? (pnl / first.price) * 100 : null;
      const totalTrades = 1;
      const profitableTrades = pnl > 0 ? 1 : 0;
      const winRatePercent = totalTrades > 0 ? (profitableTrades / totalTrades) * 100 : null;
      const totalProfitPercent = pnlPercent;

      tickerSignalSimulations.push({
        strategyId: 'buy_and_hold_simulation',
        strategyName: 'Buy & Hold (1 share)',
        trades: [{
          entryDate: first.date,
          exitDate: last.date,
          entryPrice: first.price,
          exitPrice: last.price,
          pnl,
          pnlPercent: Number.isFinite(pnlPercent as number) ? (pnlPercent as number) : null,
          entryConfidence: null
        }],
        totalTrades,
        profitableTrades,
        winRatePercent,
        totalProfitPercent,
        signalCount: 1,
        buySignalCount: 1,
        sellSignalCount: 0
      });
    }

    tickerSignalSimulations.sort((a, b) => a.strategyName.localeCompare(b.strategyName));

    const hasUsdVolumeStats = minUsdVolume !== null && maxUsdVolume !== null;
    const hasUnadjustedCloseStats = minUnadjustedClose !== null && maxUnadjustedClose !== null;
    const hasPriceRangeStats = minLow !== null && maxHigh !== null;

    res.render('pages/ticker-detail', {
      title: `${symbol} - Ticker Details`,
      page: 'tickers',
      user: req.user,
      ticker,
      candles,
      firstCandle,
      latestCandle,
      chartData: candles,
      candleCount,
      minUsdVolume,
      maxUsdVolume,
      hasUsdVolumeStats,
      minUnadjustedClose,
      maxUnadjustedClose,
      hasUnadjustedCloseStats,
      minLow,
      maxHigh,
      hasPriceRangeStats,
      tickerBacktestTable,
      tickerBacktestScopeLabel,
      tickerSignalSimulations,
      signalSimulationLookbackDays: SIGNAL_SIMULATION_LOOKBACK_DAYS,
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error rendering ticker detail:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load ticker details'
    });
  }
});

router.delete<TickerParams>('/:symbol', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { symbol } = req.params;
    const ticker = await req.db.tickers.getTicker(symbol);
    if (!ticker) {
      return res.status(404).json({ error: `Ticker ${symbol} not found` });
    }

    await req.db.tickers.deleteTicker(symbol);

    res.json({ success: true, message: `Ticker ${symbol} deleted successfully` });
  } catch (error) {
    console.error('Error deleting ticker:', error);
    res.status(500).json({ error: 'Failed to delete ticker' });
  }
});

router.post<TickerParams>('/:symbol/clear-candles', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { symbol } = req.params;
    const ticker = await req.db.tickers.getTicker(symbol);
    if (!ticker) {
      return res.redirect(`/tickers/${symbol}?error=${encodeURIComponent(`Ticker ${symbol} not found`)}`);
    }

    const deleted = await req.db.candles.clearCandlesForTicker(symbol);
    req.db.tickers.invalidateCache();
    const message = `Cleared ${deleted} candle(s) for ${symbol}`;
    res.redirect(`/tickers/${symbol}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing ticker candles:', error);
    const { symbol } = req.params;
    const errorMessage = error instanceof Error ? error.message : 'Failed to clear candles';
    res.redirect(`/tickers/${symbol}?error=${encodeURIComponent(errorMessage)}`);
  }
});

export default router;
