export type SkipReasonDisplay = {
  label: string;
  detail: string | null;
};

const REASON_LABELS: Record<string, string> = {
  operation_requested: 'Order submitted',
  limit_not_filled: 'Limit order not filled',
  discount_not_reached: 'Limit price not reached',
  market_order_price_cap_exceeded: 'Market order limit not reached',
  insufficient_cash: 'Insufficient buying power',
  insufficient_size: 'Position size below minimum',
  insufficient_volume: 'Minimum volume not met',
  price_out_of_range: 'Price outside supported range',
  missing_next_candle: 'Next candle missing',
  trade_already_open: 'Trade already open',
  position_exists: 'Position already open',
  short_selling_disabled: 'Short selling disabled',
  candle_disabled: 'Candle disabled',
  signal_excluded: 'Excluded by strategy',
  signal_not_tradable: 'Not tradable',
  signal_pending_buy_order: 'Existing buy order pending',
  signal_already_traded: 'Already traded',
  missing_candles: 'Missing candle data',
  missing_candle_for_date: 'No candle for signal date',
  price_unavailable: 'Price unavailable',
  sell_fraction_zero: 'Sell fraction is 0',
  sell_trade_after_latest_candle: 'Trade date after latest candle',
  sell_exit_order_pending: 'Exit order pending',
  sell_missing_candles: 'Missing candle data for exit',
  sell_missing_candle_for_date: 'No candle for exit date',
  sell_latest_candle_precedes_trade: 'Latest candle precedes trade',
  sell_no_active_position: 'No active position to sell',
  buy_signal_sync: 'Buy signal matched',
  sell_signal_sync: 'Sell signal matched'
};

const isCodeLike = (value: string): boolean => /^[a-z0-9_]+$/i.test(value);

const DETAIL_LABELS: Record<string, string> = {
  broker_buy_order_open: 'Broker buy order was open at planning time.',
  broker_position_exists: 'Broker position existed at planning time.'
};

const humanizeCode = (value: string): string => {
  const trimmed = value.trim();
  if (!trimmed) {
    return trimmed;
  }
  return trimmed
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
};

const maybeHumanizeDetail = (detail?: string | null): string | null => {
  if (!detail || typeof detail !== 'string') {
    return null;
  }
  const trimmed = detail.trim();
  if (!trimmed) {
    return null;
  }
  const normalized = trimmed.toLowerCase();
  const mappedDetail = DETAIL_LABELS[normalized];
  if (mappedDetail) {
    return mappedDetail;
  }
  if (normalized.startsWith('live_trade:')) {
    return `Ticker was locked by live trade ${trimmed.slice('live_trade:'.length)} at planning time.`;
  }
  if (normalized.startsWith('pending_open_operation:')) {
    return `Ticker had pending open operation ${trimmed.slice('pending_open_operation:'.length)} at planning time.`;
  }
  return isCodeLike(trimmed) ? humanizeCode(trimmed) : trimmed;
};

const formatLimitNotFilledDetail = (detail?: string | null): string | null => {
  if (!detail || typeof detail !== 'string') {
    return null;
  }
  const trimmed = detail.trim();
  if (!trimmed) {
    return null;
  }
  if (trimmed === 'invalid_limit_price') {
    return 'Limit price was invalid.';
  }
  if (trimmed === 'zero_volume') {
    return 'No volume on the execution candle.';
  }
  const lowAboveMatch = trimmed.match(/low\s+([0-9.]+)\s+above\s+limit\s+([0-9.]+)/i);
  if (lowAboveMatch) {
    return `Candle low ${lowAboveMatch[1]} stayed above limit ${lowAboveMatch[2]}.`;
  }
  const fillScoreMatch =
    trimmed.match(/fill_score\s+([0-9.]+),\s*penetration\s+([0-9.]+)%?,\s*volume_ratio\s+([0-9.]+)/i);
  if (fillScoreMatch) {
    return `Low dipped below limit by ${fillScoreMatch[2]}% but fill odds were low (score ${fillScoreMatch[1]}, volume ratio ${fillScoreMatch[3]}).`;
  }
  return trimmed;
};

const formatMarketOrderCapDetail = (detail?: string | null): string | null => {
  if (!detail || typeof detail !== 'string') {
    return null;
  }
  const trimmed = detail.trim();
  if (!trimmed) {
    return null;
  }
  const capMatch = trimmed.match(/reference\s+([0-9.]+),\s*cap\s+([0-9.]+),\s*low\s+([0-9.]+)/i);
  if (capMatch) {
    return `Candle low ${capMatch[3]} stayed above cap ${capMatch[2]} (ref ${capMatch[1]}).`;
  }
  const floorMatch = trimmed.match(/reference\s+([0-9.]+),\s*floor\s+([0-9.]+),\s*high\s+([0-9.]+)/i);
  if (floorMatch) {
    return `Candle high ${floorMatch[3]} stayed below floor ${floorMatch[2]} (ref ${floorMatch[1]}).`;
  }
  return trimmed;
};

const formatOperationRequestedDetail = (detail?: string | null): string | null => {
  if (!detail || typeof detail !== 'string') {
    return 'Order queued for dispatch.';
  }
  const normalized = detail.trim().toLowerCase();
  if (normalized === 'buy_signal_sync') {
    return 'Buy signal matched.';
  }
  if (normalized === 'sell_signal_sync') {
    return 'Sell signal matched.';
  }
  return maybeHumanizeDetail(detail);
};

export const formatSignalSkipReason = (
  reason: string | null | undefined,
  details?: string | null
): SkipReasonDisplay => {
  const normalizedReason = typeof reason === 'string' ? reason.trim().toLowerCase() : '';
  const label = REASON_LABELS[normalizedReason] ?? (reason ? humanizeCode(reason) : 'Unknown reason');

  switch (normalizedReason) {
    case 'limit_not_filled':
      return { label, detail: formatLimitNotFilledDetail(details) };
    case 'discount_not_reached':
      return { label, detail: details ?? 'Candle low stayed above the limit price.' };
    case 'market_order_price_cap_exceeded':
      return { label, detail: formatMarketOrderCapDetail(details) };
    case 'operation_requested':
      return { label, detail: formatOperationRequestedDetail(details) };
    case 'candle_disabled':
      return { label, detail: maybeHumanizeDetail(details) };
    default:
      return { label, detail: maybeHumanizeDetail(details) };
  }
};
