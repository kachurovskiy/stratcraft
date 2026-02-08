import type { BacktestScope, StrategyPerformance } from '../../shared/types/StrategyTemplate';

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

export type TemplateScoreOptions = {
  verificationByTemplate?: Map<string, TemplateVerificationMetrics>;
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

const RETURN_SCALE = 0.20;
const VALIDATION_NEGATIVE_PENALTY_STRENGTH = 2.0;
const TEMPLATE_SCORE_DRAWDOWN_LAMBDA = 2.5;
const TEMPLATE_SCORE_TRADE_TARGET = 200;
const TEMPLATE_SCORE_TRADE_WEIGHT = 0.25;
const TEMPLATE_SCORE_RECENCY_HALF_LIFE_DAYS = 365;
const TEMPLATE_SCORE_VERIFY_SHARPE_SCALE = 2;
const TEMPLATE_SCORE_VERIFY_CALMAR_SCALE = 2;
const TEMPLATE_SCORE_VERIFY_CAGR_SCALE = 0.25;
const TEMPLATE_SCORE_VERIFY_CAGR_NEG_SCALE = 0.10;
const TEMPLATE_SCORE_VERIFY_DRAWDOWN_LAMBDA = 2.5;
const TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER = 0.8;
const TEMPLATE_SCORE_VERIFY_MAX_MULTIPLIER = 1.2;

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

const computeRecencyWeight = (createdAt: Date | null): number => {
  if (!(createdAt instanceof Date) || Number.isNaN(createdAt.getTime())) {
    return 1;
  }
  const ageMs = Date.now() - createdAt.getTime();
  const ageDays = Math.max(0, ageMs / (1000 * 60 * 60 * 24));
  const halfLifeDays = Math.max(1e-6, TEMPLATE_SCORE_RECENCY_HALF_LIFE_DAYS);
  const decay = Math.exp(-Math.LN2 * ageDays / halfLifeDays);
  return 0.6 + 0.4 * decay;
};

const scoreReturn = (validationCagr: number): number => {
  if (!Number.isFinite(validationCagr)) {
    return 0;
  }
  if (validationCagr < 0) {
    return 0;
  }
  return 1 - Math.exp(-validationCagr / Math.max(RETURN_SCALE, 1e-6));
};

const scoreConsistency = (trainingCagr: number, validationCagr: number): number => {
  return clamp01(computeConsistencyScore(trainingCagr, validationCagr));
};

const scoreRisk = (validationDrawdown: number): number => {
  return Math.exp(-TEMPLATE_SCORE_DRAWDOWN_LAMBDA * Math.max(0, validationDrawdown));
};

const scoreLiquidity = (tradesPerYear: number): number => {
  const target = Math.max(1e-6, TEMPLATE_SCORE_TRADE_TARGET);
  const confidence = 1 - Math.exp(-tradesPerYear / target);
  return (1 - TEMPLATE_SCORE_TRADE_WEIGHT) + TEMPLATE_SCORE_TRADE_WEIGHT * confidence;
};

const negativeValidationPenalty = (validationCagr: number): number => {
  if (!Number.isFinite(validationCagr) || validationCagr >= 0) {
    return 1;
  }
  return Math.exp(-VALIDATION_NEGATIVE_PENALTY_STRENGTH * Math.abs(validationCagr));
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

const computeVerificationMultiplier = (metrics: TemplateVerificationMetrics | undefined): number | null => {
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

  const sharpeScore = scorePositiveMetric(metrics.verifySharpeRatio ?? null, TEMPLATE_SCORE_VERIFY_SHARPE_SCALE);
  if (sharpeScore !== null) {
    components.push(sharpeScore);
  }

  const calmarScore = scorePositiveMetric(metrics.verifyCalmarRatio ?? null, TEMPLATE_SCORE_VERIFY_CALMAR_SCALE);
  if (calmarScore !== null) {
    components.push(calmarScore);
  }

  const cagrScore = scoreSignedMetric(
    verifyCagr,
    TEMPLATE_SCORE_VERIFY_CAGR_SCALE,
    TEMPLATE_SCORE_VERIFY_CAGR_NEG_SCALE
  );
  if (cagrScore !== null) {
    components.push(cagrScore);
  }

  if (typeof verifyDrawdown === 'number' && Number.isFinite(verifyDrawdown)) {
    const drawdownScore = Math.exp(-TEMPLATE_SCORE_VERIFY_DRAWDOWN_LAMBDA * Math.max(0, verifyDrawdown));
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

  return TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER +
    (TEMPLATE_SCORE_VERIFY_MAX_MULTIPLIER - TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER) * verificationScore;
};

export const computeTemplateScores = (
  snapshots: TemplateScoreSnapshot[],
  options: TemplateScoreOptions = {}
): Map<string, number> => {
  return computeTemplateScoreResults(snapshots, options).scores;
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

export const computeTemplateScoreResults = (
  snapshots: TemplateScoreSnapshot[],
  options: TemplateScoreOptions = {}
): TemplateScoreResults => {
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

      const returnScore = scoreReturn(validationCagr);
      const consistencyScore = scoreConsistency(trainingCagr, validationCagr);
      const riskScore = scoreRisk(validationDrawdown);
      const liquidityScore = scoreLiquidity(tradesPerYear);
      let periodScore01 = returnScore * consistencyScore * riskScore * liquidityScore;
      periodScore01 *= negativeValidationPenalty(validationCagr);
      if (!Number.isFinite(periodScore01)) {
        return;
      }
      periodScore01 = clamp01(periodScore01);

      const lengthWeight = Math.sqrt(Math.max(1, periodMonths));
      const recencyWeight = computeRecencyWeight(periodEntry.validation?.createdAt ?? null);
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
      const multiplier = computeVerificationMultiplier(metrics);
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
