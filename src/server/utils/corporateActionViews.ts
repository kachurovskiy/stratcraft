import type { CorporateActionRecord } from '../database/types';
import type { TradeChange } from '../types/StrategyTemplate';

const CORPORATE_ACTION_TYPE_LABELS: Record<string, string> = {
  reverse_split: 'Reverse Split',
  forward_split: 'Forward Split',
  unit_split: 'Unit Split',
  cash_dividend: 'Cash Dividend',
  stock_dividend: 'Stock Dividend',
  spin_off: 'Spin-Off',
  cash_merger: 'Cash Merger',
  stock_merger: 'Stock Merger',
  stock_and_cash_merger: 'Stock + Cash Merger',
  redemption: 'Redemption',
  name_change: 'Name Change',
  worthless_removal: 'Worthless Removal',
  rights_distribution: 'Rights Distribution'
};

export interface CorporateActionCardView {
  id: string;
  typeLabel: string;
  matchLabel?: 'Primary' | 'Related';
  primarySymbol: string;
  relatedSymbols: string[];
  processDate: Date;
  effectiveDate: Date | null;
  exDate: Date | null;
  recordDate: Date | null;
  payableDate: Date | null;
  displayDate?: Date;
  ratioLabel?: string | null;
  historyLabel?: string;
  payload: Record<string, unknown>;
}

function formatIdentifierLabel(value: string): string {
  if (!value) {
    return 'Corporate Action';
  }
  const withSpaces = value
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
  return withSpaces.charAt(0).toUpperCase() + withSpaces.slice(1);
}

function formatCorporateActionTypeLabel(actionType: string): string {
  return CORPORATE_ACTION_TYPE_LABELS[actionType] ?? formatIdentifierLabel(actionType);
}

function parseCorporateActionNumber(payload: Record<string, unknown>, key: string): number | null {
  const value = payload[key];
  const numericValue = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  return Number.isFinite(numericValue) ? numericValue : null;
}

function formatCorporateActionRate(value: number): string {
  if (Math.abs(value - Math.round(value)) <= 1e-6) {
    return String(Math.round(value));
  }
  return value.toFixed(4).replace(/0+$/, '').replace(/\.$/, '');
}

function buildCorporateActionRatioLabel(action: CorporateActionRecord): string | null {
  const oldRate = parseCorporateActionNumber(action.payload, 'old_rate');
  const newRate = parseCorporateActionNumber(action.payload, 'new_rate');
  if (!oldRate || !newRate || oldRate <= 0 || newRate <= 0) {
    return null;
  }
  return `${formatCorporateActionRate(newRate)}-for-${formatCorporateActionRate(oldRate)}`;
}

function getCorporateActionDisplayDate(action: CorporateActionRecord): Date {
  return action.exDate ?? action.effectiveDate ?? action.processDate ?? action.recordDate ?? action.payableDate;
}

function buildCorporateActionCardView(action: CorporateActionRecord): CorporateActionCardView {
  return {
    id: action.id,
    typeLabel: formatCorporateActionTypeLabel(action.actionType),
    primarySymbol: action.primarySymbol,
    relatedSymbols: action.relatedSymbols,
    processDate: action.processDate,
    effectiveDate: action.effectiveDate ?? null,
    exDate: action.exDate ?? null,
    recordDate: action.recordDate ?? null,
    payableDate: action.payableDate ?? null,
    payload: action.payload
  };
}

export function buildTickerCorporateActions(
  symbol: string,
  actions: CorporateActionRecord[]
): CorporateActionCardView[] {
  return actions.map((action) => ({
    ...buildCorporateActionCardView(action),
    matchLabel: action.primarySymbol === symbol ? 'Primary' : 'Related'
  }));
}

export function buildTradeCorporateActions(
  appliedActionIds: string[],
  actions: CorporateActionRecord[]
): CorporateActionCardView[] {
  if (appliedActionIds.length === 0 || actions.length === 0) {
    return [];
  }

  const actionsById = new Map<string, CorporateActionRecord>();
  for (const action of actions) {
    actionsById.set(action.id, action);
    const payloadId = typeof action.payload.corporate_action_id === 'string' ? action.payload.corporate_action_id.trim() : '';
    if (payloadId.length > 0) {
      actionsById.set(payloadId, action);
    }
  }

  return appliedActionIds
    .map((id) => actionsById.get(id))
    .filter((action): action is CorporateActionRecord => action !== undefined)
    .map((action) => {
      const view = buildCorporateActionCardView(action);
      const ratioLabel = buildCorporateActionRatioLabel(action);
      const historyLabel = ratioLabel ? `${view.typeLabel} ${ratioLabel} (${action.id})` : `${view.typeLabel} (${action.id})`;
      return {
        ...view,
        displayDate: getCorporateActionDisplayDate(action),
        ratioLabel,
        historyLabel
      };
    });
}

export function buildTradeCorporateActionHistoryChanges(
  tradeCorporateActions: CorporateActionCardView[],
  existingChanges: TradeChange[] | undefined | null
): TradeChange[] {
  if (tradeCorporateActions.length === 0) {
    return [];
  }

  const existingCorporateActionText = new Set(
    (existingChanges ?? [])
      .filter((change) => change.field === 'corporateAction')
      .map((change) => String(change.newValue ?? ''))
  );

  return tradeCorporateActions
    .filter((action): action is CorporateActionCardView & { historyLabel: string; displayDate: Date } =>
      Boolean(action.historyLabel && action.displayDate && !existingCorporateActionText.has(action.historyLabel))
    )
    .map((action) => ({
      field: 'corporateAction',
      oldValue: null,
      newValue: action.historyLabel,
      changedAt: action.displayDate
    }));
}
