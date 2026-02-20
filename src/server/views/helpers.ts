type DateInput = Date | string | number;

const currencyFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD'
});

const priceFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});

const percentFormatter = new Intl.NumberFormat('en-US', {
  style: 'percent',
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});

const numberFormatter = new Intl.NumberFormat('en-US');
const fullDateTimeFormatter = new Intl.DateTimeFormat('en-US', {
  year: 'numeric',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  timeZoneName: 'short'
});

const unsafeJsonChars = /[<>&\u2028\u2029]/g;
const unsafeJsonCharMap: Record<string, string> = {
  '<': '\\u003c',
  '>': '\\u003e',
  '&': '\\u0026',
  '\u2028': '\\u2028',
  '\u2029': '\\u2029'
};

function getIsoParts(value: DateInput) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  const isoString = parsed.toISOString();
  return {
    date: isoString.slice(0, 10),
    time: isoString.slice(11, 19)
  };
}

function formatIsoDate(value: DateInput) {
  const parts = getIsoParts(value);
  return parts ? parts.date : 'Invalid Date';
}

function formatIsoDateTime(value: DateInput, includeSeconds = true) {
  const parts = getIsoParts(value);
  if (!parts) {
    return 'Invalid Date';
  }
  const time = includeSeconds ? parts.time : parts.time.slice(0, 5);
  return `${parts.date} ${time}`;
}

function formatTimeAgoShort(value: DateInput) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return 'Invalid Date';
  }
  const now = new Date();
  const diffMs = parsed.getTime() - now.getTime();
  const isFuture = diffMs > 0;
  const absMs = Math.abs(diffMs);
  const minutes = Math.floor(absMs / 60000);
  if (minutes < 60) {
    const valueLabel = `${Math.max(0, minutes)}m`;
    return isFuture ? `in ${valueLabel}` : `${valueLabel} ago`;
  }
  const hours = Math.floor(absMs / 3600000);
  if (hours < 24) {
    const valueLabel = `${Math.max(1, hours)}h`;
    return isFuture ? `in ${valueLabel}` : `${valueLabel} ago`;
  }
  const days = Math.floor(absMs / 86400000);
  const valueLabel = `${Math.max(1, days)}d`;
  return isFuture ? `in ${valueLabel}` : `${valueLabel} ago`;
}

function formatDurationMs(value: number) {
  const numeric = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return '--';
  }
  const totalSeconds = Math.floor(numeric / 1000);
  if (totalSeconds < 60) {
    return `${totalSeconds}s`;
  }
  const totalMinutes = Math.floor(totalSeconds / 60);
  if (totalMinutes < 60) {
    return `${totalMinutes}m`;
  }
  const totalHours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (totalHours < 24) {
    return `${totalHours}h ${minutes}m`;
  }
  const days = Math.floor(totalHours / 24);
  const hours = totalHours % 24;
  return `${days}d ${hours}h`;
}

function formatTimeAgoShortWithSeconds(value: DateInput) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return 'Invalid Date';
  }
  const diffMs = parsed.getTime() - Date.now();
  const isFuture = diffMs > 0;
  const label = formatDurationMs(Math.abs(diffMs));
  if (label === '--') {
    return '--';
  }
  return isFuture ? `in ${label}` : `${label} ago`;
}

function formatDateTimeWithTimezone(value: DateInput) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return 'Invalid Date';
  }
  return fullDateTimeFormatter.format(parsed);
}

export const viewHelpers = {
  formatCurrency(value: number) {
    return currencyFormatter.format(value);
  },
  formatPrice(value: number) {
    return priceFormatter.format(value);
  },
  formatRateAsPercent(value: number) {
    return percentFormatter.format(value);
  },
  formatPercentAsPercent(value: number) {
    return `${value.toFixed(2)}%`;
  },
  formatPnlPercent(pnl: number, quantity: number, price: number) {
    if (!pnl || !quantity || !price) return 'N/A';
    const tradeValue = Math.abs(quantity * price);
    if (!Number.isFinite(tradeValue) || tradeValue === 0) {
      return 'N/A';
    }
    const pnlPercent = pnl / tradeValue;
    return percentFormatter.format(pnlPercent);
  },
  formatNumber(value: number) {
    return numberFormatter.format(value);
  },
  formatDate(date: Date) {
    return formatIsoDate(date);
  },
  formatHoursAgo(date: Date) {
    const now = new Date();
    const past = new Date(date);
    const diffMs = now.getTime() - past.getTime();
    const diffHours = diffMs / (1000 * 60 * 60);
    return `${diffHours.toFixed(1)}h ago`;
  },
  formatDateTime(date: Date) {
    return formatIsoDateTime(date);
  },
  formatDateTimeShort(date: Date) {
    return formatIsoDateTime(date, false);
  },
  formatTimeAgoShort(date: DateInput) {
    return formatTimeAgoShort(date);
  },
  formatTimeAgoShortWithSeconds(date: DateInput) {
    return formatTimeAgoShortWithSeconds(date);
  },
  formatDateTimeWithTimezone(date: DateInput) {
    return formatDateTimeWithTimezone(date);
  },
  formatBacktestPeriod(start?: Date, end?: Date) {
    if (!start || !end) {
      return 'N/A';
    }
    const startDate = new Date(start);
    const endDate = new Date(end);
    if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
      return 'N/A';
    }
    const [earlier, later] =
      startDate.getTime() <= endDate.getTime() ? [startDate, endDate] : [endDate, startDate];
    const diffMs = later.getTime() - earlier.getTime();
    const averageMonthMs = 1000 * 60 * 60 * 24 * 30.436875;
    const roundedMonths = Math.max(0, Math.round(diffMs / averageMonthMs));
    return `${roundedMonths}`;
  },
  formatDuration(minutes: number) {
    if (minutes === undefined || minutes === null) return 'N/A';
    if (minutes < 1) {
      const seconds = Math.round(minutes * 60);
      return `${seconds}s`;
    }
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = Math.round(minutes % 60);
    if (hours > 0) {
      return `${hours}h ${remainingMinutes}m`;
    }
    return `${remainingMinutes}m`;
  },
  formatDurationMs(value: number) {
    return formatDurationMs(value);
  },
  formatLogTime(date: Date) {
    const logDate = new Date(date);
    if (Number.isNaN(logDate.getTime())) {
      return 'Invalid Date';
    }
    const today = new Date();
    const isoParts = getIsoParts(logDate);
    if (!isoParts) {
      return 'Invalid Date';
    }

    if (today.toDateString() === logDate.toDateString()) {
      return isoParts.time;
    }
    return `${isoParts.date} ${isoParts.time}`;
  },
  formatTotalReturnPercent(totalReturn: number, initialCapital: number) {
    if (!initialCapital || initialCapital === 0) return 'N/A';
    const percent = totalReturn / initialCapital;
    return percentFormatter.format(percent);
  },
  isToday(date: Date) {
    const today = new Date();
    const checkDate = new Date(date);
    return today.toDateString() === checkDate.toDateString();
  },
  eq(a: string | number, b: string | number) {
    return a === b;
  },
  ne(a: string | number, b: string | number) {
    return a !== b;
  },
  and(...args: any[]) {
    return args.every(arg => !!arg);
  },
  or(...args: any[]) {
    return args.some(arg => !!arg);
  },
  gt(a: string | number, b: string | number) {
    return a > b;
  },
  gte(a: string | number, b: string | number) {
    return a >= b;
  },
  lt(a: string | number, b: string | number) {
    return a < b;
  },
  lte(a: string | number, b: string | number) {
    return a <= b;
  },
  defined(value: any) {
    return value !== undefined;
  },
  includes(list: unknown, value: unknown) {
    if (!Array.isArray(list)) {
      return false;
    }
    return list.includes(value);
  },
  subtract(a: number, b: number) {
    return a - b;
  },
  divide(a: number, b: number) {
    return a / b;
  },
  multiply(a: number, b: number) {
    return a * b;
  },
  json(context: unknown) {
    try {
      const json = JSON.stringify(context);
      if (!json) return json;
      return json.replace(unsafeJsonChars, c => unsafeJsonCharMap[c] ?? c);
    } catch {
      return '';
    }
  },
  toFixed(value: number, digits: number) {
    return Number.isFinite(value) ? value.toFixed(digits) : 'N/A';
  },
  encodeURIComponent(value: string) {
    return encodeURIComponent(value);
  },
  operationTypeLabel(type: string) {
    switch ((type || '').toLowerCase()) {
      case 'open_position':
        return 'Open Position';
      case 'close_position':
        return 'Close Position';
      case 'update_stop_loss':
        return 'Update Stop Loss';
      default:
        return type || 'Unknown';
    }
  },
  operationStatusLabel(status: string) {
    switch ((status || '').toLowerCase()) {
      case 'pending':
        return 'Pending';
      case 'failed':
        return 'Failed';
      case 'sent':
        return 'Sent';
      case 'skipped':
        return 'Skipped';
      case 'unknown':
        return 'Unknown';
      default:
        return status || 'Unknown';
    }
  },
  operationStatusBadge(status: string) {
    switch ((status || '').toLowerCase()) {
      case 'pending':
        return 'warning';
      case 'failed':
        return 'danger';
      case 'sent':
        return 'success';
      case 'skipped':
        return 'secondary';
      case 'unknown':
        return 'dark';
      default:
        return 'dark';
    }
  },
  jobStatusBadge(status: string) {
    switch ((status || '').toLowerCase()) {
      case 'running':
        return 'info';
      case 'handoff':
        return 'primary';
      case 'succeeded':
        return 'success';
      case 'failed':
        return 'danger';
      case 'cancelled':
        return 'secondary';
      case 'queued':
        return 'warning';
      default:
        return 'secondary';
    }
  },
  calcDurationMinutes(start: Date, end: Date) {
    if (!start || !end) return '0';
    const startDate = new Date(start);
    const endDate = new Date(end);
    const minutes = (endDate.getTime() - startDate.getTime()) / 60000;
    return minutes > 0 ? minutes.toFixed(1) : '0';
  },
  limit(arr: any[], count: number) {
    if (!Array.isArray(arr)) return [];
    const n = typeof count === 'number' ? count : parseInt(String(count), 10);
    if (isNaN(n) || n < 0) return [];
    return arr.slice(0, n);
  },
  sortBy(arr: any[], key?: string) {
    if (!Array.isArray(arr)) return [];
    const cloned = [...arr];
    const getComparableValue = (item: any) => {
      if (!key) {
        return item;
      }
      const value = item?.[key];
      return value === undefined || value === null ? '' : value;
    };
    return cloned.sort((a, b) => {
      const valueA = getComparableValue(a);
      const valueB = getComparableValue(b);

      if (typeof valueA === 'number' && typeof valueB === 'number') {
        return valueA - valueB;
      }

      const stringA = String(valueA ?? '').toLowerCase();
      const stringB = String(valueB ?? '').toLowerCase();
      return stringA.localeCompare(stringB);
    });
  }
};
