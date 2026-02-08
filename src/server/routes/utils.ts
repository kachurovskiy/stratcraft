import type { Request } from 'express';

export function getReqUserId(req: Request): number {
  const rawId = req.user?.id ?? req.user?.userId;
  const parsedId = typeof rawId === 'string' ? Number.parseInt(rawId, 10) : rawId;

  if (typeof parsedId === 'number' && Number.isFinite(parsedId)) {
    if (req.user && typeof req.user === 'object') {
      req.user.id = parsedId;
    }
    return parsedId;
  }

  throw new Error('User ID missing on request');
}

export function getCurrentUrl(req: Request): string {
  return req.originalUrl || req.url;
}

export function formatBacktestPeriodLabel(periodMonths: number): string {
  if (!Number.isFinite(periodMonths) || periodMonths <= 0) {
    return 'N/A';
  }
  if (periodMonths >= 12) {
    const years = Math.floor(periodMonths / 12);
    const remainingMonths = periodMonths % 12;
    let label = `${years}y`;
    if (remainingMonths > 0) {
      label += ` ${remainingMonths}m`;
    }
    return label;
  }
  return `${periodMonths}m`;
}

export function parsePageParam(raw: unknown): number {
  const parsed = typeof raw === 'string' ? parseInt(raw, 10) : Number(raw);
  if (Number.isFinite(parsed) && parsed > 0) {
    return Math.floor(parsed);
  }
  return 1;
}
