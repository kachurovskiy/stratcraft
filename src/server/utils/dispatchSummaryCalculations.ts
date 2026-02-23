import type { CandlesRepo } from '../database/repos/CandlesRepo';
import type { Candle } from '../../shared/types/StrategyTemplate';

export type DispatchSummaryOperation = {
  ticker: string;
  operationType: string;
  quantity: number | null;
  price: number | null;
  orderType?: 'market' | 'limit' | null;
};

export type CashImpactSummary = {
  totalImpact: number;
  estimatedImpact: number;
  considered: number;
  missingPricing: number;
  eligible: number;
  limitOrders: number;
  limitAdjusted: number;
  limitMissing: number;
};

export type OrderSizeStats = {
  min: number;
  avg: number;
  max: number;
};

type TickerBehavior = {
  referencePrice: number | null;
  sigma: number | null;
};

const DEFAULT_SIGMA_LOOKBACK_DAYS = 20;
const DEFAULT_SIGMA_FALLBACK = 0.02;
const DEFAULT_SIGMA_FLOOR = 0.002;

export async function calculateEstimatedCashImpact(
  operations: DispatchSummaryOperation[],
  options: {
    candlesRepo: CandlesRepo;
    lookbackDays?: number;
  }
): Promise<CashImpactSummary | null> {
  let totalImpact = 0;
  let estimatedImpact = 0;
  let considered = 0;
  let missingPricing = 0;
  let eligible = 0;
  let limitOrders = 0;
  let limitAdjusted = 0;
  let limitMissing = 0;

  const tickerBehavior = await buildTickerBehaviorForLimitOrders(
    operations,
    options.candlesRepo,
    options.lookbackDays ?? DEFAULT_SIGMA_LOOKBACK_DAYS
  );

  for (const op of operations) {
    const direction = getOperationCashDirection(op.operationType);
    if (direction === 0) {
      continue;
    }

    eligible += 1;
    const quantity = normalizePositiveNumber(op.quantity);
    const price = normalizePositiveNumber(op.price);
    if (quantity === null || price === null) {
      missingPricing += 1;
      continue;
    }

    totalImpact += direction * quantity * price;

    let fillWeight = 1;
    if (op.orderType === 'limit') {
      limitOrders += 1;
      const estimate = estimateLimitFillWeight(op, tickerBehavior.get(op.ticker));
      if (estimate === null) {
        limitMissing += 1;
      } else {
        fillWeight = estimate;
        limitAdjusted += 1;
      }
    }

    estimatedImpact += direction * quantity * price * fillWeight;
    considered += 1;
  }

  if (eligible === 0) {
    return null;
  }

  return {
    totalImpact,
    estimatedImpact,
    considered,
    missingPricing,
    eligible,
    limitOrders,
    limitAdjusted,
    limitMissing
  };
}

export function calculateOrderSizeStats(
  operations: DispatchSummaryOperation[]
): OrderSizeStats | null {
  const sizes: number[] = [];
  for (const op of operations) {
    const quantity = normalizePositiveNumber(op.quantity);
    const price = normalizePositiveNumber(op.price);
    if (quantity === null || price === null) {
      continue;
    }
    sizes.push(Math.abs(quantity * price));
  }
  if (sizes.length === 0) {
    return null;
  }
  const min = Math.min(...sizes);
  const max = Math.max(...sizes);
  const avg = sizes.reduce((sum, value) => sum + value, 0) / sizes.length;
  return { min, avg, max };
}

const buildTickerBehaviorForLimitOrders = async (
  operations: DispatchSummaryOperation[],
  candlesRepo: CandlesRepo,
  lookbackDays: number
): Promise<Map<string, TickerBehavior>> => {
  const limitTickers = new Set<string>();
  for (const op of operations) {
    if (op.orderType === 'limit') {
      limitTickers.add(op.ticker);
    }
  }
  if (limitTickers.size === 0) {
    return new Map();
  }

  const tickers = Array.from(limitTickers);
  const lastDates = await candlesRepo.getLastCandleDates(tickers);
  const lastDateValues = Object.values(lastDates);
  if (lastDateValues.length === 0) {
    return new Map();
  }

  let earliestStart: Date | null = null;
  let latestEnd: Date | null = null;
  for (const lastDate of lastDateValues) {
    if (!latestEnd || lastDate > latestEnd) {
      latestEnd = lastDate;
    }
    const start = subtractDays(lastDate, lookbackDays);
    if (!earliestStart || start < earliestStart) {
      earliestStart = start;
    }
  }

  if (!earliestStart || !latestEnd) {
    return new Map();
  }

  const candlesByTicker = await candlesRepo.getCandles(tickers, earliestStart, latestEnd);
  const behavior = new Map<string, TickerBehavior>();

  for (const ticker of tickers) {
    const lastDate = lastDates[ticker];
    if (!lastDate) {
      continue;
    }
    const start = subtractDays(lastDate, lookbackDays);
    const candles = (candlesByTicker[ticker] ?? []).filter(
      (candle) => candle.date >= start && candle.date <= lastDate
    );
    const closes = extractClosesFromCandles(candles);
    const referencePrice = closes.length > 0 ? closes[closes.length - 1] : null;
    const sigma = estimateSigmaRolling(closes, lookbackDays);
    behavior.set(ticker, { referencePrice, sigma });
  }

  return behavior;
};

const extractClosesFromCandles = (candles: Candle[]): number[] => {
  const closes: number[] = [];
  for (const candle of candles) {
    const close = normalizePositiveNumber(candle.close);
    if (close !== null && close > 0) {
      closes.push(close);
    }
  }
  return closes;
};

const estimateLimitFillWeight = (
  op: DispatchSummaryOperation,
  behavior?: TickerBehavior
): number | null => {
  if (!behavior) {
    return null;
  }
  const referencePrice = normalizePositiveNumber(behavior.referencePrice);
  const sigma = normalizePositiveNumber(behavior.sigma);
  const limitPrice = normalizePositiveNumber(op.price);
  if (referencePrice === null || referencePrice <= 0 || sigma === null || limitPrice === null) {
    return null;
  }

  const side =
    op.operationType === 'open_position'
      ? 'buy'
      : op.operationType === 'close_position'
        ? 'sell'
        : null;
  if (!side) {
    return null;
  }

  if (side === 'buy' && limitPrice >= referencePrice) {
    return 1;
  }
  if (side === 'sell' && limitPrice <= referencePrice) {
    return 1;
  }

  const n =
    side === 'buy'
      ? (referencePrice - limitPrice) / referencePrice
      : (limitPrice - referencePrice) / referencePrice;
  if (!Number.isFinite(n) || n <= 0 || n >= 1) {
    return null;
  }

  return fillProbability(side, n, sigma);
};

export function estimateSigmaRolling(
  closes: number[],
  lookback = DEFAULT_SIGMA_LOOKBACK_DAYS
): number {
  const normalized: number[] = [];
  for (const close of closes) {
    const value = normalizePositiveNumber(close);
    if (value !== null && value > 0) {
      normalized.push(value);
    }
  }
  if (normalized.length < 2) {
    return DEFAULT_SIGMA_FALLBACK;
  }

  const returns = computeLogReturns(normalized);
  if (returns.length < 2) {
    return DEFAULT_SIGMA_FALLBACK;
  }

  const start = Math.max(0, returns.length - lookback);
  const window = returns.slice(start);
  if (window.length < 2) {
    return DEFAULT_SIGMA_FALLBACK;
  }

  const mean = window.reduce((sum, value) => sum + value, 0) / window.length;
  const variance =
    window.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (window.length - 1);
  if (!Number.isFinite(variance)) {
    return DEFAULT_SIGMA_FALLBACK;
  }
  const sigma = Math.sqrt(variance);
  if (!Number.isFinite(sigma)) {
    return DEFAULT_SIGMA_FALLBACK;
  }
  return sigma;
}

export function erf(x: number): number {
  if (!Number.isFinite(x)) {
    if (x === Infinity) {
      return 1;
    }
    if (x === -Infinity) {
      return -1;
    }
    return 0;
  }

  const sign = x < 0 ? -1 : 1;
  const abs = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * abs);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const y =
    1 -
    (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t) *
      Math.exp(-abs * abs);
  return sign * y;
}

export function normalCdf(x: number): number {
  return 0.5 * (1 + erf(x / Math.SQRT2));
}

export function fillProbability(side: 'buy' | 'sell', n: number, sigma: number): number {
  if (!Number.isFinite(n) || n <= 0 || n >= 1 || !Number.isFinite(sigma)) {
    return 0;
  }
  const a = side === 'buy' ? Math.log(1 / (1 - n)) : Math.log(1 + n);
  if (!Number.isFinite(a)) {
    return 0;
  }
  const sigmaAdj = Math.max(Math.abs(sigma), DEFAULT_SIGMA_FLOOR);
  const z = a / sigmaAdj;
  const p = 2 * (1 - normalCdf(z));
  if (!Number.isFinite(p)) {
    return 0;
  }
  return Math.max(0, Math.min(1, p));
}

export function expectedFills(
  orders: { side: 'buy' | 'sell'; n: number; closes: number[] }[],
  lookback = DEFAULT_SIGMA_LOOKBACK_DAYS
): { expected: number; perOrder: number[] } {
  let expected = 0;
  const perOrder: number[] = [];
  for (const order of orders) {
    const sigma = estimateSigmaRolling(order.closes, lookback);
    const pFill = fillProbability(order.side, order.n, sigma);
    perOrder.push(pFill);
    expected += pFill;
  }
  return { expected, perOrder };
}

const computeLogReturns = (closes: number[]): number[] => {
  const returns: number[] = [];
  for (let i = 1; i < closes.length; i += 1) {
    const prev = closes[i - 1];
    const current = closes[i];
    if (prev > 0 && current > 0) {
      const value = Math.log(current / prev);
      if (Number.isFinite(value)) {
        returns.push(value);
      }
    }
  }
  return returns;
};

const getOperationCashDirection = (operationType: string): number => {
  switch (operationType) {
    case 'open_position':
      return -1;
    case 'close_position':
    case 'update_stop_loss':
      return 1;
    default:
      return 0;
  }
};

const normalizePositiveNumber = (value: unknown): number | null => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  return Math.abs(value);
};

const subtractDays = (date: Date, days: number): Date => {
  const copy = new Date(date);
  copy.setDate(copy.getDate() - days);
  return copy;
};
