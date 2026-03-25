import { StrategyParameter, StrategyTemplate } from '../types/StrategyTemplate';

interface ParameterSummaryView {
  name: string;
  label?: string;
  description?: string;
  displayValue: string;
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
          displayValue,
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

export { buildParameterContexts, formatParameterValue };
export type { ExtraParameterView, ParameterSummaryView };
