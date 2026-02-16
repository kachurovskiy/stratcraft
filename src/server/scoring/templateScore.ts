import type { BacktestScope, StrategyPerformance } from '../../shared/types/StrategyTemplate';
import { SETTING_KEYS, type SettingKey } from '../constants';
import { loadNumberSettingOverrides, normalizeNumber, type SettingsRepo } from '../utils/settings';

export type TemplateScoreSnapshot = {
  templateId: string;
  strategyId: string;
  periodMonths: number;
  periodDays: number | null;
  tickerScope: BacktestScope;
  performance: StrategyPerformance | null;
  createdAt: Date | null;
};

export type TemplateVerificationMetrics = {
  verifySharpeRatio?: number | null;
  verifyCalmarRatio?: number | null;
  verifyCagr?: number | null;
  verifyMaxDrawdownRatio?: number | null;
};

export type TemplateScoreSettings = {
  returnScale: number;
  validationNegativePenaltyStrength: number;
  drawdownLambda: number;
  tradeTarget: number;
  tradeWeight: number;
  recencyHalfLifeDays: number;
  verifySharpeScale: number;
  verifyCalmarScale: number;
  verifyCagrScale: number;
  verifyCagrNegScale: number;
  verifyDrawdownLambda: number;
  verifyMinMultiplier: number;
  verifyMaxMultiplier: number;
};

export type TemplateScoreOptions = {
  verificationByTemplate?: Map<string, TemplateVerificationMetrics>;
  templateScoreSettings?: Partial<TemplateScoreSettings>;
  settingsRepo?: SettingsRepo;
};

export type TemplateScoreBreakdown = {
  templateId: string;
  strategyId: string;
  strategyScore: number;
  finalScore: number;
  baseScore01: number;
  finalScore01: number;
  baseScore100: number;
  finalScore100: number;
  componentAverages: {
    returnScore: number;
    consistencyScore: number;
    riskScore: number;
    liquidityScore: number;
  };
  weights: {
    totalWeight: number;
    periodCount: number;
    lengthWeightAvg: number;
    recencyWeightAvg: number;
  };
  periods: TemplateScorePeriodBreakdown[];
  weightTotal: number;
  periodCount: number;
  lengthWeight: number;
  recencyWeight: number;
  verificationMultiplier: number | null;
};

export type TemplateScoreResults = {
  scores: Map<string, number>;
  breakdowns: Map<string, TemplateScoreBreakdown>;
};

export const DEFAULT_TEMPLATE_SCORE_SETTINGS: TemplateScoreSettings = {
  returnScale: 0.20,
  validationNegativePenaltyStrength: 2.0,
  drawdownLambda: 2.5,
  tradeTarget: 200,
  tradeWeight: 0.25,
  recencyHalfLifeDays: 365,
  verifySharpeScale: 2,
  verifyCalmarScale: 2,
  verifyCagrScale: 0.25,
  verifyCagrNegScale: 0.10,
  verifyDrawdownLambda: 2.5,
  verifyMinMultiplier: 0.8,
  verifyMaxMultiplier: 1.2
};

const TEMPLATE_SCORE_SETTING_KEYS: SettingKey[] = [
  SETTING_KEYS.TEMPLATE_SCORE_RETURN_SCALE,
  SETTING_KEYS.TEMPLATE_SCORE_VALIDATION_NEGATIVE_PENALTY_STRENGTH,
  SETTING_KEYS.TEMPLATE_SCORE_DRAWDOWN_LAMBDA,
  SETTING_KEYS.TEMPLATE_SCORE_TRADE_TARGET,
  SETTING_KEYS.TEMPLATE_SCORE_TRADE_WEIGHT,
  SETTING_KEYS.TEMPLATE_SCORE_RECENCY_HALF_LIFE_DAYS,
  SETTING_KEYS.TEMPLATE_SCORE_VERIFY_SHARPE_SCALE,
  SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CALMAR_SCALE,
  SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_SCALE,
  SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_NEG_SCALE,
  SETTING_KEYS.TEMPLATE_SCORE_VERIFY_DRAWDOWN_LAMBDA,
  SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER,
  SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MAX_MULTIPLIER
];

const TEMPLATE_SCORE_SETTING_MAPPING: Array<{
  settingKey: SettingKey;
  field: keyof TemplateScoreSettings;
}> = [
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_RETURN_SCALE, field: 'returnScale' },
  {
    settingKey: SETTING_KEYS.TEMPLATE_SCORE_VALIDATION_NEGATIVE_PENALTY_STRENGTH,
    field: 'validationNegativePenaltyStrength'
  },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_DRAWDOWN_LAMBDA, field: 'drawdownLambda' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_TRADE_TARGET, field: 'tradeTarget' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_TRADE_WEIGHT, field: 'tradeWeight' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_RECENCY_HALF_LIFE_DAYS, field: 'recencyHalfLifeDays' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_SHARPE_SCALE, field: 'verifySharpeScale' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CALMAR_SCALE, field: 'verifyCalmarScale' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_SCALE, field: 'verifyCagrScale' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_NEG_SCALE, field: 'verifyCagrNegScale' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_DRAWDOWN_LAMBDA, field: 'verifyDrawdownLambda' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER, field: 'verifyMinMultiplier' },
  { settingKey: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MAX_MULTIPLIER, field: 'verifyMaxMultiplier' }
];

const resolveTemplateScoreSettings = (
  overrides?: Partial<TemplateScoreSettings>
): TemplateScoreSettings => {
  const merged: TemplateScoreSettings = {
    ...DEFAULT_TEMPLATE_SCORE_SETTINGS,
    ...(overrides ?? {})
  };

  const verifyMinMultiplier = normalizeNumber(
    merged.verifyMinMultiplier,
    DEFAULT_TEMPLATE_SCORE_SETTINGS.verifyMinMultiplier,
    { min: 0 }
  );
  const verifyMaxMultiplier = Math.max(
    verifyMinMultiplier,
    normalizeNumber(
      merged.verifyMaxMultiplier,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.verifyMaxMultiplier,
      { min: 0 }
    )
  );

  return {
    returnScale: normalizeNumber(
      merged.returnScale,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.returnScale,
      { min: 1e-6 }
    ),
    validationNegativePenaltyStrength: normalizeNumber(
      merged.validationNegativePenaltyStrength,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.validationNegativePenaltyStrength,
      { min: 0 }
    ),
    drawdownLambda: normalizeNumber(
      merged.drawdownLambda,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.drawdownLambda,
      { min: 0 }
    ),
    tradeTarget: normalizeNumber(
      merged.tradeTarget,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.tradeTarget,
      { min: 1e-6 }
    ),
    tradeWeight: normalizeNumber(
      merged.tradeWeight,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.tradeWeight,
      { min: 0, max: 1 }
    ),
    recencyHalfLifeDays: normalizeNumber(
      merged.recencyHalfLifeDays,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.recencyHalfLifeDays,
      { min: 1e-6 }
    ),
    verifySharpeScale: normalizeNumber(
      merged.verifySharpeScale,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.verifySharpeScale,
      { min: 1e-6 }
    ),
    verifyCalmarScale: normalizeNumber(
      merged.verifyCalmarScale,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.verifyCalmarScale,
      { min: 1e-6 }
    ),
    verifyCagrScale: normalizeNumber(
      merged.verifyCagrScale,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.verifyCagrScale,
      { min: 1e-6 }
    ),
    verifyCagrNegScale: normalizeNumber(
      merged.verifyCagrNegScale,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.verifyCagrNegScale,
      { min: 1e-6 }
    ),
    verifyDrawdownLambda: normalizeNumber(
      merged.verifyDrawdownLambda,
      DEFAULT_TEMPLATE_SCORE_SETTINGS.verifyDrawdownLambda,
      { min: 0 }
    ),
    verifyMinMultiplier,
    verifyMaxMultiplier
  };
};

const loadTemplateScoreSettings = async (
  settingsRepo?: SettingsRepo
): Promise<Partial<TemplateScoreSettings>> => {
  return loadNumberSettingOverrides(settingsRepo, TEMPLATE_SCORE_SETTING_KEYS, TEMPLATE_SCORE_SETTING_MAPPING);
};

const resolveTemplateScoreSettingsFromOptions = async (
  options: TemplateScoreOptions
): Promise<TemplateScoreSettings> => {
  const settingsOverrides = await loadTemplateScoreSettings(options.settingsRepo);
  return resolveTemplateScoreSettings({
    ...settingsOverrides,
    ...(options.templateScoreSettings ?? {})
  });
};

const clampNumber = (value: number, minValue: number, maxValue: number): number => {
  if (!Number.isFinite(value)) {
    return minValue;
  }
  return Math.min(Math.max(value, minValue), maxValue);
};

const clamp01 = (value: number): number => clampNumber(value, 0, 1);

const computeConsistencyScore = (trainingCagr: number, validationCagr: number): number => {
  const denom = Math.abs(trainingCagr) + Math.abs(validationCagr);
  if (denom <= 1e-6) {
    return 1;
  }
  const shortfall = Math.max(0, trainingCagr - validationCagr);
  return 1 - clampNumber(shortfall / denom, 0, 1);
};

const computeTradesPerYear = (
  totalTrades: number,
  periodMonths: number,
  periodDays: number | null
): number | null => {
  if (!Number.isFinite(totalTrades) || totalTrades <= 0) {
    return null;
  }
  let years = periodMonths > 0 ? periodMonths / 12 : null;
  if (!years || !Number.isFinite(years) || years <= 0) {
    const days = typeof periodDays === 'number' && Number.isFinite(periodDays) ? periodDays : null;
    years = days && days > 0 ? days / 365.25 : null;
  }
  if (!years || !Number.isFinite(years) || years <= 0) {
    return null;
  }
  return totalTrades / years;
};

const computeRecencyWeight = (createdAt: Date | null, settings: TemplateScoreSettings): number => {
  if (!(createdAt instanceof Date) || Number.isNaN(createdAt.getTime())) {
    return 1;
  }
  const ageMs = Date.now() - createdAt.getTime();
  const ageDays = Math.max(0, ageMs / (1000 * 60 * 60 * 24));
  const halfLifeDays = Math.max(1e-6, settings.recencyHalfLifeDays);
  const decay = Math.exp(-Math.LN2 * ageDays / halfLifeDays);
  return 0.6 + 0.4 * decay;
};

const scoreReturn = (validationCagr: number, settings: TemplateScoreSettings): number => {
  if (!Number.isFinite(validationCagr)) {
    return 0;
  }
  if (validationCagr < 0) {
    return 0;
  }
  return 1 - Math.exp(-validationCagr / Math.max(settings.returnScale, 1e-6));
};

const scoreConsistency = (trainingCagr: number, validationCagr: number): number => {
  return clamp01(computeConsistencyScore(trainingCagr, validationCagr));
};

const scoreRisk = (validationDrawdown: number, settings: TemplateScoreSettings): number => {
  return Math.exp(-settings.drawdownLambda * Math.max(0, validationDrawdown));
};

const scoreLiquidity = (tradesPerYear: number, settings: TemplateScoreSettings): number => {
  const target = Math.max(1e-6, settings.tradeTarget);
  const confidence = 1 - Math.exp(-tradesPerYear / target);
  return (1 - settings.tradeWeight) + settings.tradeWeight * confidence;
};

const negativeValidationPenalty = (validationCagr: number, settings: TemplateScoreSettings): number => {
  if (!Number.isFinite(validationCagr) || validationCagr >= 0) {
    return 1;
  }
  return Math.exp(-settings.validationNegativePenaltyStrength * Math.abs(validationCagr));
};

const scorePositiveMetric = (value: number | null, scale: number): number | null => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  const normalized = Math.max(0, value);
  if (normalized <= 0) {
    return 0;
  }
  return 1 - Math.exp(-normalized / Math.max(scale, 1e-6));
};

const scoreSignedMetric = (value: number | null, posScale: number, negScale: number): number | null => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  if (value >= 0) {
    const score = 1 - Math.exp(-value / Math.max(posScale, 1e-6));
    return 0.5 + 0.5 * score;
  }
  const score = 1 - Math.exp(-Math.abs(value) / Math.max(negScale, 1e-6));
  return 0.5 - 0.5 * score;
};

const computeVerificationMultiplier = (
  metrics: TemplateVerificationMetrics | undefined,
  settings: TemplateScoreSettings
): number | null => {
  if (!metrics) {
    return null;
  }
  const verifyCagr = typeof metrics.verifyCagr === 'number' && Number.isFinite(metrics.verifyCagr)
    ? metrics.verifyCagr
    : null;
  const verifyDrawdown = typeof metrics.verifyMaxDrawdownRatio === 'number' &&
    Number.isFinite(metrics.verifyMaxDrawdownRatio)
    ? metrics.verifyMaxDrawdownRatio
    : null;
  const components: number[] = [];

  const sharpeScore = scorePositiveMetric(metrics.verifySharpeRatio ?? null, settings.verifySharpeScale);
  if (sharpeScore !== null) {
    components.push(sharpeScore);
  }

  const calmarScore = scorePositiveMetric(metrics.verifyCalmarRatio ?? null, settings.verifyCalmarScale);
  if (calmarScore !== null) {
    components.push(calmarScore);
  }

  const cagrScore = scoreSignedMetric(
    verifyCagr,
    settings.verifyCagrScale,
    settings.verifyCagrNegScale
  );
  if (cagrScore !== null) {
    components.push(cagrScore);
  }

  if (typeof verifyDrawdown === 'number' && Number.isFinite(verifyDrawdown)) {
    const drawdownScore = Math.exp(-settings.verifyDrawdownLambda * Math.max(0, verifyDrawdown));
    components.push(drawdownScore);
  }

  if (!components.length) {
    return null;
  }

  const product = components.reduce((acc, value) => acc * Math.max(0, value), 1);
  const verificationScore = Math.pow(product, 1 / components.length);
  if (!Number.isFinite(verificationScore)) {
    return null;
  }

  return settings.verifyMinMultiplier +
    (settings.verifyMaxMultiplier - settings.verifyMinMultiplier) * verificationScore;
};

export const computeTemplateScores = async (
  snapshots: TemplateScoreSnapshot[],
  options: TemplateScoreOptions = {}
): Promise<Map<string, number>> => {
  const results = await computeTemplateScoreResults(snapshots, options);
  return results.scores;
};

type TemplateScorePeriodBreakdown = {
  periodMonths: number;
  periodDays: number | null;
  createdAt: Date | null;
  trainingCagr: number;
  validationCagr: number;
  validationDrawdown: number;
  tradesPerYear: number;
  returnScore: number;
  consistencyScore: number;
  riskScore: number;
  liquidityScore: number;
  periodScore01: number;
  lengthWeight: number;
  recencyWeight: number;
  weight: number;
};

export const computeTemplateScoreResults = async (
  snapshots: TemplateScoreSnapshot[],
  options: TemplateScoreOptions = {}
): Promise<TemplateScoreResults> => {
  const templateScoreSettings = await resolveTemplateScoreSettingsFromOptions(options);
  const strategyMap = new Map<string, {
    templateId: string;
    periods: Map<number, {
      periodDays: number | null;
      training?: TemplateScoreSnapshot;
      validation?: TemplateScoreSnapshot;
    }>;
  }>();

  snapshots.forEach(snapshot => {
    if (!snapshot.performance || snapshot.periodMonths <= 0) {
      return;
    }
    const strategyId = snapshot.strategyId;
    let entry = strategyMap.get(strategyId);
    if (!entry) {
      entry = { templateId: snapshot.templateId, periods: new Map() };
      strategyMap.set(strategyId, entry);
    }
    let periodEntry = entry.periods.get(snapshot.periodMonths);
    if (!periodEntry) {
      periodEntry = { periodDays: snapshot.periodDays ?? null };
      entry.periods.set(snapshot.periodMonths, periodEntry);
    }
    if (snapshot.tickerScope === 'validation') {
      periodEntry.validation = snapshot;
    } else {
      periodEntry.training = snapshot;
    }
    if (!periodEntry.periodDays && snapshot.periodDays) {
      periodEntry.periodDays = snapshot.periodDays;
    }
  });

  const templateScoreById = new Map<string, number>();
  const templateBreakdownById = new Map<string, TemplateScoreBreakdown>();

  strategyMap.forEach((strategyEntry, strategyId) => {
    const periodScores: TemplateScorePeriodBreakdown[] = [];
    strategyEntry.periods.forEach((periodEntry, periodMonths) => {
      const training = periodEntry.training?.performance;
      const validation = periodEntry.validation?.performance;
      if (!training || !validation) {
        return;
      }

      const trainingCagr = typeof training.cagr === 'number' && Number.isFinite(training.cagr)
        ? training.cagr
        : null;
      const validationCagr = typeof validation.cagr === 'number' && Number.isFinite(validation.cagr)
        ? validation.cagr
        : null;
      if (trainingCagr === null || validationCagr === null) {
        return;
      }

      const validationDrawdown = typeof validation.maxDrawdownPercent === 'number' &&
        Number.isFinite(validation.maxDrawdownPercent)
        ? validation.maxDrawdownPercent / 100
        : null;
      if (validationDrawdown === null) {
        return;
      }

      const totalTrades = typeof validation.totalTrades === 'number' ? validation.totalTrades : null;
      const tradesPerYear = totalTrades === null
        ? null
        : computeTradesPerYear(totalTrades, periodMonths, periodEntry.periodDays ?? null);
      if (tradesPerYear === null) {
        return;
      }

      const returnScore = scoreReturn(validationCagr, templateScoreSettings);
      const consistencyScore = scoreConsistency(trainingCagr, validationCagr);
      const riskScore = scoreRisk(validationDrawdown, templateScoreSettings);
      const liquidityScore = scoreLiquidity(tradesPerYear, templateScoreSettings);
      let periodScore01 = returnScore * consistencyScore * riskScore * liquidityScore;
      periodScore01 *= negativeValidationPenalty(validationCagr, templateScoreSettings);
      if (!Number.isFinite(periodScore01)) {
        return;
      }
      periodScore01 = clamp01(periodScore01);

      const lengthWeight = Math.sqrt(Math.max(1, periodMonths));
      const recencyWeight = computeRecencyWeight(periodEntry.validation?.createdAt ?? null, templateScoreSettings);
      const weight = lengthWeight * recencyWeight;
      periodScores.push({
        periodMonths,
        periodDays: periodEntry.periodDays ?? null,
        createdAt: periodEntry.validation?.createdAt ?? null,
        trainingCagr,
        validationCagr,
        validationDrawdown,
        tradesPerYear,
        returnScore,
        consistencyScore,
        riskScore,
        liquidityScore,
        periodScore01,
        lengthWeight,
        recencyWeight,
        weight
      });
    });

    if (!periodScores.length) {
      return;
    }

    const totalWeight = periodScores.reduce((sum, item) => sum + item.weight, 0);
    const weightedScore = periodScores.reduce((sum, item) => sum + (item.periodScore01 * item.weight), 0);
    if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
      return;
    }
    const baseScore01 = clamp01(weightedScore / totalWeight);
    const templateId = strategyEntry.templateId;
    const existing = templateScoreById.get(templateId);
    if (existing === undefined || baseScore01 > existing) {
      const weightedAverage = (selector: (item: TemplateScorePeriodBreakdown) => number): number => {
        const sum = periodScores.reduce((acc, item) => acc + (selector(item) * item.weight), 0);
        return sum / totalWeight;
      };
      const componentAverages = {
        returnScore: weightedAverage(item => item.returnScore),
        consistencyScore: weightedAverage(item => item.consistencyScore),
        riskScore: weightedAverage(item => item.riskScore),
        liquidityScore: weightedAverage(item => item.liquidityScore)
      };
      const lengthWeightAvg = weightedAverage(item => item.lengthWeight);
      const recencyWeightAvg = weightedAverage(item => item.recencyWeight);
      const breakdown: TemplateScoreBreakdown = {
        templateId,
        strategyId,
        strategyScore: baseScore01,
        finalScore: baseScore01,
        baseScore01,
        finalScore01: baseScore01,
        baseScore100: Math.round(clamp01(baseScore01) * 100),
        finalScore100: Math.round(clamp01(baseScore01) * 100),
        componentAverages,
        weights: {
          totalWeight,
          periodCount: periodScores.length,
          lengthWeightAvg,
          recencyWeightAvg
        },
        periods: [...periodScores],
        weightTotal: totalWeight,
        periodCount: periodScores.length,
        lengthWeight: lengthWeightAvg,
        recencyWeight: recencyWeightAvg,
        verificationMultiplier: null
      };
      templateScoreById.set(templateId, baseScore01);
      templateBreakdownById.set(templateId, breakdown);
    }
  });

  if (options.verificationByTemplate && options.verificationByTemplate.size > 0) {
    templateScoreById.forEach((score, templateId) => {
      const metrics = options.verificationByTemplate?.get(templateId);
      const multiplier = computeVerificationMultiplier(metrics, templateScoreSettings);
      if (multiplier !== null) {
        const finalScore01 = score * multiplier;
        templateScoreById.set(templateId, finalScore01);
        const breakdown = templateBreakdownById.get(templateId);
        if (breakdown) {
          breakdown.verificationMultiplier = multiplier;
          breakdown.finalScore01 = finalScore01;
          breakdown.finalScore = finalScore01;
          breakdown.finalScore100 = Math.round(clamp01(finalScore01) * 100);
          templateBreakdownById.set(templateId, breakdown);
        }
      }
    });
  }

  return {
    scores: templateScoreById,
    breakdowns: templateBreakdownById
  };
};
