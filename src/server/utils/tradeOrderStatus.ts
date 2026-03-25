import type { TradeCancellationSource } from '../types/StrategyTemplate';

export type EntryOrderCancellationInfo = {
  cancellationSource?: TradeCancellationSource | null;
  entryCancelAfter?: Date | null;
  entryOrderStatus?: string | null;
  entryOrderStatusUpdatedAt?: Date | null;
};

export type EntryOrderCancellationOptions = {
  includeEntryStatus?: boolean;
  fallbackToBroker?: boolean;
  defaultToAutoCancel?: boolean;
};

const CANCEL_GRACE_MS = 60_000;

export const formatEntryOrderStatusLabel = (value: string | null | undefined): string | null => {
  if (typeof value !== 'string') {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  if (normalized === 'canceled' || normalized === 'cancelled') {
    return 'cancelled';
  }
  if (normalized === 'expired') {
    return 'expired';
  }
  if (normalized === 'rejected') {
    return 'rejected';
  }
  if (normalized === 'filled') {
    return 'filled';
  }
  if (normalized === 'pending') {
    return 'pending';
  }
  return normalized.replace(/[_-]+/g, ' ');
};

export const resolveEntryOrderCancellationReason = (
  info: EntryOrderCancellationInfo,
  options: EntryOrderCancellationOptions = {}
): string | null => {
  if (info.cancellationSource === 'expiry') {
    return 'Cancelled after market close';
  }
  if (info.cancellationSource === 'exchange') {
    return 'Cancelled by broker';
  }
  if (info.entryCancelAfter && info.entryOrderStatusUpdatedAt) {
    const cancelAfterTime = info.entryCancelAfter.getTime();
    const updatedTime = info.entryOrderStatusUpdatedAt.getTime();
    if (updatedTime >= cancelAfterTime - CANCEL_GRACE_MS) {
      return 'Cancelled after market close';
    }
  }
  if (options.includeEntryStatus && info.entryOrderStatus) {
    const statusLabel = formatEntryOrderStatusLabel(info.entryOrderStatus);
    if (statusLabel) {
      return `Entry order ${statusLabel}`;
    }
  }
  if (options.defaultToAutoCancel && info.entryCancelAfter) {
    return 'Cancelled after market close';
  }
  if (options.fallbackToBroker) {
    return 'Cancelled by broker';
  }
  return null;
};
