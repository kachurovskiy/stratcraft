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
  impact: number;
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
  volatility: number | null;
};

const DEFAULT_VOLATILITY_LOOKBACK_DAYS = 14;

export async function calculateEstimatedCashImpact(
  operations: DispatchSummaryOperation[],
  options: {
    candlesRepo: CandlesRepo;
    lookbackDays?: number;
  }
): Promise<CashImpactSummary | null> {
  let impact = 0;
  let considered = 0;
  let missingPricing = 0;
  let eligible = 0;
  let limitOrders = 0;
  let limitAdjusted = 0;
  let limitMissing = 0;

  const tickerBehavior = await buildTickerBehaviorForLimitOrders(
    operations,
    options.candlesRepo,
    options.lookbackDays ?? DEFAULT_VOLATILITY_LOOKBACK_DAYS
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

    impact += direction * quantity * price * fillWeight;
    considered += 1;
  }

  if (eligible === 0) {
    return null;
  }

  return { impact, considered, missingPricing, eligible, limitOrders, limitAdjusted, limitMissing };
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
    const referencePrice = extractReferencePrice(candles);
    const volatility = computeVolatilityFromCandles(candles);
    behavior.set(ticker, { referencePrice, volatility });
  }

  return behavior;
};

const extractReferencePrice = (candles: Candle[]): number | null => {
  if (candles.length === 0) {
    return null;
  }
  const lastCandle = candles[candles.length - 1];
  return normalizePositiveNumber(lastCandle.close);
};

const computeVolatilityFromCandles = (candles: Candle[]): number | null => {
  if (candles.length < 3) {
    return null;
  }
  const closes: number[] = [];
  for (const candle of candles) {
    const close = normalizePositiveNumber(candle.close);
    if (close !== null) {
      closes.push(close);
    }
  }
  if (closes.length < 3) {
    return null;
  }

  const returns: number[] = [];
  for (let i = 1; i < closes.length; i += 1) {
    const prev = closes[i - 1];
    const current = closes[i];
    if (prev > 0) {
      const dailyReturn = current / prev - 1;
      if (Number.isFinite(dailyReturn)) {
        returns.push(dailyReturn);
      }
    }
  }

  if (returns.length < 2) {
    return null;
  }

  const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const variance = returns.reduce((sum, value) => sum + (value - mean) ** 2, 0) / returns.length;
  if (!Number.isFinite(variance)) {
    return null;
  }
  return Math.sqrt(variance);
};

const estimateLimitFillWeight = (
  op: DispatchSummaryOperation,
  behavior?: TickerBehavior
): number | null => {
  if (!behavior) {
    return null;
  }
  const referencePrice = normalizePositiveNumber(behavior.referencePrice);
  const volatility = normalizePositiveNumber(behavior.volatility);
  const limitPrice = normalizePositiveNumber(op.price);
  if (referencePrice === null || referencePrice <= 0 || volatility === null || volatility <= 0 || limitPrice === null) {
    return null;
  }

  const isBuy = op.operationType === 'open_position';
  const isSell = op.operationType === 'close_position';
  if (!isBuy && !isSell) {
    return null;
  }

  if (isBuy && limitPrice >= referencePrice) {
    return 1;
  }
  if (isSell && limitPrice <= referencePrice) {
    return 1;
  }

  const distanceRatio = Math.abs(referencePrice - limitPrice) / referencePrice;
  if (!Number.isFinite(distanceRatio)) {
    return null;
  }

  const weight = 1 - distanceRatio / volatility;
  if (!Number.isFinite(weight)) {
    return null;
  }

  return Math.max(0, Math.min(1, weight));
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
