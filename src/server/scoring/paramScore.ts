
import type { QueryResultRow } from 'pg';
import { SETTING_KEYS, type SettingKey } from '../constants';
import { loadNumberSettingOverrides, normalizeNumber, type SettingsRepo } from '../utils/settings';

export type BacktestCacheRow = QueryResultRow & {
  parameters: Record<string, unknown>;
  sharpe_ratio: number;
  calmar_ratio: number;
  total_return: number;
  cagr: number;
  max_drawdown: number;
  max_drawdown_ratio: number;
  win_rate: number;
  total_trades: number;
  verify_sharpe_ratio?: number | null;
  verify_calmar_ratio?: number | null;
  verify_total_return?: number | null;
  verify_cagr?: number | null;
  verify_max_drawdown_ratio?: number | null;
  top_abs_gain_ticker?: string | null;
  top_rel_gain_ticker?: string | null;
};

export interface BestBacktestParamsResult {
  parameters: Record<string, unknown>;
  sharpeRatio: number;
  calmarRatio: number;
  totalReturn: number;
  cagr: number;
  maxDrawdown: number;
  maxDrawdownRatio: number;
  winRate: number | null;
  totalTrades: number | null;
  coreScore: number;
  stabilityScore: number;
  finalScore: number;
  sourceRow?: BacktestCacheRow;
}

type NormalizedCandidate = {
  parameters: Record<string, unknown>;
  sharpeRatio: number;
  calmarRatio: number;
  totalReturn: number;
  cagr: number;
  maxDrawdown: number;
  maxDrawdownRatio: number;
  winRate: number | null;
  totalTrades: number | null;
  coreScore: number;
  ddPenalty: number;
  verifySharpeRatio?: number | null;
  verifyCalmarRatio?: number | null;
  verifyTotalReturn?: number | null;
  verifyCagr?: number | null;
  verifyMaxDrawdownRatio?: number | null;
  sourceRow: BacktestCacheRow;
};

type ScoredCandidate = NormalizedCandidate & {
  stabilityScore: number;
  finalScore: number;
};

export type ScoreAvailabilityReasonCode =
  | 'missing_metrics'
  | 'missing_parameters'
  | 'missing_trades'
  | 'insufficient_trades';

export type ScoreAvailabilityResult =
  | { eligible: true }
  | { eligible: false; reasonCode: ScoreAvailabilityReasonCode; reason: string };

export type ScoreBacktestParametersSummary = {
  scored: BestBacktestParamsResult[];
  availabilityById: Map<string, ScoreAvailabilityResult>;
  availabilityByRow: Map<BacktestCacheRow, ScoreAvailabilityResult>;
};

type ParameterScaleMap = Map<string, number>;

export type ParamScoreSettings = {
  minTrades: number;
  drawdownLambda: number;
  neighborThreshold: number;
  coreScoreQuantile: number;
  pairwiseNeighborLimit: number;
  stabilityGamma: number;
};

export type ParamScoreOptions = {
  settingsRepo?: SettingsRepo;
  paramScoreSettings?: Partial<ParamScoreSettings>;
};

const PERCENTILE_TOLERANCE = 1e-12;
const CORE_SCORE_EPSILON = 1e-9;
export const DEFAULT_PARAM_SCORE_SETTINGS: ParamScoreSettings = {
  minTrades: 20,
  drawdownLambda: 3.5,
  neighborThreshold: 0.15,
  coreScoreQuantile: 0.6,
  pairwiseNeighborLimit: 1500,
  stabilityGamma: 2
};

const PARAM_SCORE_SETTING_KEYS: SettingKey[] = [
  SETTING_KEYS.PARAM_SCORE_MIN_TRADES,
  SETTING_KEYS.PARAM_SCORE_DRAWDOWN_LAMBDA,
  SETTING_KEYS.PARAM_SCORE_NEIGHBOR_THRESHOLD,
  SETTING_KEYS.PARAM_SCORE_CORE_SCORE_QUANTILE,
  SETTING_KEYS.PARAM_SCORE_PAIRWISE_NEIGHBOR_LIMIT
];

const PARAM_SCORE_SETTING_MAPPING: Array<{
  settingKey: SettingKey;
  field: keyof ParamScoreSettings;
}> = [
  { settingKey: SETTING_KEYS.PARAM_SCORE_MIN_TRADES, field: 'minTrades' },
  { settingKey: SETTING_KEYS.PARAM_SCORE_DRAWDOWN_LAMBDA, field: 'drawdownLambda' },
  { settingKey: SETTING_KEYS.PARAM_SCORE_NEIGHBOR_THRESHOLD, field: 'neighborThreshold' },
  { settingKey: SETTING_KEYS.PARAM_SCORE_CORE_SCORE_QUANTILE, field: 'coreScoreQuantile' },
  { settingKey: SETTING_KEYS.PARAM_SCORE_PAIRWISE_NEIGHBOR_LIMIT, field: 'pairwiseNeighborLimit' }
];

const resolveParamScoreSettings = (
  overrides?: Partial<ParamScoreSettings>
): ParamScoreSettings => {
  const merged: ParamScoreSettings = {
    ...DEFAULT_PARAM_SCORE_SETTINGS,
    ...(overrides ?? {})
  };

  return {
    minTrades: normalizeNumber(merged.minTrades, DEFAULT_PARAM_SCORE_SETTINGS.minTrades, { min: 0, integer: true }),
    drawdownLambda: normalizeNumber(merged.drawdownLambda, DEFAULT_PARAM_SCORE_SETTINGS.drawdownLambda, { min: 0 }),
    neighborThreshold: normalizeNumber(
      merged.neighborThreshold,
      DEFAULT_PARAM_SCORE_SETTINGS.neighborThreshold,
      { min: 0 }
    ),
    coreScoreQuantile: normalizeNumber(
      merged.coreScoreQuantile,
      DEFAULT_PARAM_SCORE_SETTINGS.coreScoreQuantile,
      { min: 0, max: 1 }
    ),
    pairwiseNeighborLimit: normalizeNumber(
      merged.pairwiseNeighborLimit,
      DEFAULT_PARAM_SCORE_SETTINGS.pairwiseNeighborLimit,
      { min: 1, integer: true }
    ),
    stabilityGamma: normalizeNumber(
      merged.stabilityGamma,
      DEFAULT_PARAM_SCORE_SETTINGS.stabilityGamma,
      { min: 0 }
    )
  };
};

const loadParamScoreSettings = async (
  settingsRepo?: SettingsRepo
): Promise<Partial<ParamScoreSettings>> => {
  return loadNumberSettingOverrides(settingsRepo, PARAM_SCORE_SETTING_KEYS, PARAM_SCORE_SETTING_MAPPING);
};

const resolveParamScoreSettingsFromOptions = async (
  options: ParamScoreOptions
): Promise<ParamScoreSettings> => {
  const settingsOverrides = await loadParamScoreSettings(options.settingsRepo);
  return resolveParamScoreSettings({
    ...settingsOverrides,
    ...(options.paramScoreSettings ?? {})
  });
};

const STABILITY_IGNORED_PARAMS = new Set(['initialCapital', 'maxLeverage', 'ticker']);

export const scoreBacktestParameters = async (
  rows: BacktestCacheRow[],
  options: ParamScoreOptions = {}
): Promise<ScoreBacktestParametersSummary> => {
  const scoreSettings = await resolveParamScoreSettingsFromOptions(options);
  const availabilityById = new Map<string, ScoreAvailabilityResult>();
  const availabilityByRow = new Map<BacktestCacheRow, ScoreAvailabilityResult>();
  const candidates: NormalizedCandidate[] = [];
  rows.forEach((row) => {
    const evaluation = evaluateCandidateRow(row, scoreSettings);
    recordAvailability(row, evaluation.availability, availabilityByRow, availabilityById);
    if (evaluation.candidate) {
      candidates.push(evaluation.candidate);
    }
  });

  if (!candidates.length) {
    return {
      scored: [],
      availabilityById,
      availabilityByRow
    };
  }

  const sharpePercentiles = computePercentileRanks(candidates.map(candidate => candidate.sharpeRatio));
  const calmarPercentiles = computePercentileRanks(candidates.map(candidate => candidate.calmarRatio));
  const returnPercentiles = computePercentileRanks(candidates.map(candidate => candidate.totalReturn));
  const verifySharpeValues: Array<{ idx: number; value: number }> = [];
  const verifyCalmarValues: Array<{ idx: number; value: number }> = [];
  const verifyReturnLikeValues: Array<{ idx: number; value: number }> = [];

  candidates.forEach((candidate, idx) => {
    const verifySharpe = candidate.verifySharpeRatio;
    if (verifySharpe !== null && verifySharpe !== undefined) {
      verifySharpeValues.push({ idx, value: verifySharpe });
    }

    const verifyCalmar = candidate.verifyCalmarRatio;
    if (verifyCalmar !== null && verifyCalmar !== undefined) {
      verifyCalmarValues.push({ idx, value: verifyCalmar });
    }

    const verifyReturnLike = candidate.verifyTotalReturn ?? candidate.verifyCagr;
    if (verifyReturnLike !== null && verifyReturnLike !== undefined) {
      verifyReturnLikeValues.push({ idx, value: verifyReturnLike });
    }
  });

  const verifySharpePercentiles = buildAlignedPercentiles(verifySharpeValues, candidates.length);
  const verifyCalmarPercentiles = buildAlignedPercentiles(verifyCalmarValues, candidates.length);
  const verifyReturnLikePercentiles = buildAlignedPercentiles(verifyReturnLikeValues, candidates.length);

  const scoredCandidates: ScoredCandidate[] = candidates.map((candidate, i) => {
    const coreTrain = Math.cbrt(
      (sharpePercentiles[i] + CORE_SCORE_EPSILON) *
      (calmarPercentiles[i] + CORE_SCORE_EPSILON) *
      (returnPercentiles[i] + CORE_SCORE_EPSILON)
    );
    let coreScore = coreTrain;
    const verifySharpePercentile = verifySharpePercentiles[i];
    const verifyCalmarPercentile = verifyCalmarPercentiles[i];
    const verifyReturnLikePercentile = verifyReturnLikePercentiles[i];
    if (
      verifySharpePercentile !== null &&
      verifyCalmarPercentile !== null &&
      verifyReturnLikePercentile !== null
    ) {
      const coreVerify = Math.cbrt(
        (verifySharpePercentile + CORE_SCORE_EPSILON) *
        (verifyCalmarPercentile + CORE_SCORE_EPSILON) *
        (verifyReturnLikePercentile + CORE_SCORE_EPSILON)
      );
      coreScore = Math.sqrt(coreTrain * coreVerify);
    }

    const ddPenaltyTrain = Math.exp(-scoreSettings.drawdownLambda * Math.max(0, candidate.maxDrawdownRatio));
    let ddPenalty = ddPenaltyTrain;
    const verifyDrawdown = candidate.verifyMaxDrawdownRatio;
    if (verifyDrawdown !== null && verifyDrawdown !== undefined) {
      const ddPenaltyVerify = Math.exp(-scoreSettings.drawdownLambda * Math.max(0, verifyDrawdown));
      ddPenalty = Math.sqrt(ddPenaltyTrain * ddPenaltyVerify);
    }

    return {
      ...candidate,
      coreScore,
      ddPenalty,
      stabilityScore: 0,
      finalScore: 0
    };
  });

  applyStabilityScores(scoredCandidates, scoreSettings);

  const sorted = scoredCandidates
    .map((candidate) => {
      const stability = clampNumber(candidate.stabilityScore, 0, 1);
      const stabilityFactor = Math.pow(stability, scoreSettings.stabilityGamma);
      const finalScore = candidate.coreScore * candidate.ddPenalty * stabilityFactor;
      return { ...candidate, finalScore };
    })
    .sort((a, b) => b.finalScore - a.finalScore);

  const scored = sorted.map((candidate) => ({
    parameters: candidate.parameters,
    sharpeRatio: candidate.sharpeRatio,
    calmarRatio: candidate.calmarRatio,
    totalReturn: candidate.totalReturn,
    cagr: candidate.cagr,
    maxDrawdown: candidate.maxDrawdown,
    maxDrawdownRatio: candidate.maxDrawdownRatio,
    winRate: candidate.winRate,
    totalTrades: candidate.totalTrades,
    coreScore: candidate.coreScore,
    stabilityScore: candidate.stabilityScore,
    finalScore: candidate.finalScore,
    sourceRow: candidate.sourceRow
  }));

  return {
    scored,
    availabilityById,
    availabilityByRow
  };
};

type CandidateEvaluationResult = {
  candidate: NormalizedCandidate | null;
  availability: ScoreAvailabilityResult;
};

const evaluateCandidateRow = (
  row: BacktestCacheRow,
  scoreSettings: ParamScoreSettings
): CandidateEvaluationResult => {
  const fail = (reasonCode: ScoreAvailabilityReasonCode, reason: string): CandidateEvaluationResult => ({
    candidate: null,
    availability: {
      eligible: false,
      reasonCode,
      reason
    }
  });

  const sharpe = parseNullableNumber(row.sharpe_ratio);
  const calmar = parseNullableNumber(row.calmar_ratio);
  const totalReturn = parseNullableNumber(row.total_return);
  if (sharpe === null || calmar === null || totalReturn === null) {
    return fail('missing_metrics', 'Missing Sharpe, Calmar, or total return metrics.');
  }
  const cagr = parseNullableNumber(row.cagr) ?? 0;
  const parameters = parseParameters(row.parameters);
  if (!parameters || Object.keys(parameters).length === 0) {
    return fail('missing_parameters', 'Parameter set is empty or invalid.');
  }
  const totalTradesValue = parseNullableNumber(row.total_trades);
  if (totalTradesValue === null) {
    return fail('missing_trades', 'Total trade count is missing.');
  }
  const totalTrades = Math.max(0, Math.round(totalTradesValue));
  if (scoreSettings.minTrades > 0 && totalTrades < scoreSettings.minTrades) {
    return fail(
      'insufficient_trades',
      `Requires at least ${scoreSettings.minTrades} trades (only ${totalTrades} recorded).`
    );
  }
  const candidate: NormalizedCandidate = {
    parameters,
    sharpeRatio: sharpe,
    calmarRatio: calmar,
    totalReturn,
    cagr,
    maxDrawdown: row.max_drawdown,
    maxDrawdownRatio: row.max_drawdown_ratio,
    winRate: row.win_rate,
    totalTrades,
    coreScore: 0,
    ddPenalty: 0,
    verifySharpeRatio: parseNullableNumber(row.verify_sharpe_ratio),
    verifyCalmarRatio: parseNullableNumber(row.verify_calmar_ratio),
    verifyTotalReturn: parseNullableNumber(row.verify_total_return),
    verifyCagr: parseNullableNumber(row.verify_cagr),
    verifyMaxDrawdownRatio: parseNullableNumber(row.verify_max_drawdown_ratio),
    sourceRow: row
  };

  return {
    candidate,
    availability: { eligible: true }
  };
};

const recordAvailability = (
  row: BacktestCacheRow,
  availability: ScoreAvailabilityResult,
  availabilityByRow: Map<BacktestCacheRow, ScoreAvailabilityResult>,
  availabilityById: Map<string, ScoreAvailabilityResult>
): void => {
  availabilityByRow.set(row, availability);
  const rowId = getRowId(row);
  if (rowId) {
    availabilityById.set(rowId, availability);
  }
};

const getRowId = (row: BacktestCacheRow): string | null => {
  const id = (row as any).id;
  return typeof id === 'string' && id.trim() ? id : null;
};

const computePercentileRanks = (values: number[]): number[] => {
  if (!values.length) {
    return [];
  }
  if (values.length === 1) {
    return [1];
  }

  const sorted = values.map((value, index) => ({ value, index })).sort((a, b) => a.value - b.value);
  const percentiles = new Array(values.length).fill(0);
  const denominator = sorted.length - 1;
  let i = 0;

  while (i < sorted.length) {
    let j = i + 1;
    while (j < sorted.length && Math.abs(sorted[j].value - sorted[i].value) <= PERCENTILE_TOLERANCE) {
      j += 1;
    }

    const lower = i;
    const upper = j - 1;
    const averageRank = (lower + upper) / 2;
    const percentile = denominator === 0 ? 1 : averageRank / denominator;

    for (let index = i; index < j; index += 1) {
      percentiles[sorted[index].index] = percentile;
    }

    i = j;
  }

  return percentiles;
};

const buildAlignedPercentiles = (
  values: Array<{ idx: number; value: number }>,
  totalCount: number
): Array<number | null> => {
  const aligned = new Array(totalCount).fill(null) as Array<number | null>;
  if (!values.length) {
    return aligned;
  }

  const percentiles = computePercentileRanks(values.map(entry => entry.value));
  values.forEach((entry, index) => {
    aligned[entry.idx] = percentiles[index];
  });

  return aligned;
};

const applyStabilityScores = (
  candidates: ScoredCandidate[],
  scoreSettings: ParamScoreSettings
): void => {
  const scaleMap = computeParameterScales(candidates);
  const qualityValues = candidates.map(candidate => candidate.coreScore * candidate.ddPenalty);
  let qualityMin = Number.POSITIVE_INFINITY;
  let qualityMax = Number.NEGATIVE_INFINITY;
  for (const value of qualityValues) {
    if (!Number.isFinite(value)) {
      continue;
    }
    qualityMin = Math.min(qualityMin, value);
    qualityMax = Math.max(qualityMax, value);
  }
  if (!Number.isFinite(qualityMin) || !Number.isFinite(qualityMax)) {
    qualityMin = 0;
    qualityMax = 0;
  }

  if (candidates.length <= scoreSettings.pairwiseNeighborLimit) {
    applyPairwiseStabilityScores(
      candidates,
      qualityValues,
      qualityMin,
      qualityMax,
      scoreSettings.neighborThreshold,
      scaleMap
    );
    return;
  }

  applyBucketedStabilityScores(
    candidates,
    qualityValues,
    qualityMin,
    qualityMax,
    scoreSettings.neighborThreshold,
    scaleMap
  );
};

const applyPairwiseStabilityScores = (
  candidates: ScoredCandidate[],
  qualityValues: number[],
  qualityMin: number,
  qualityMax: number,
  threshold: number,
  scaleMap: ParameterScaleMap
): void => {
  const qualityRange = qualityMax - qualityMin;
  const hasQualityRange = qualityRange > 1e-12;

  for (let i = 0; i < candidates.length; i += 1) {
    let neighborCount = 0;
    let neighborQualitySum = 0;

    for (let j = 0; j < candidates.length; j += 1) {
      if (i === j) {
        continue;
      }
      const distance = computeParameterDistance(candidates[i].parameters, candidates[j].parameters, scaleMap);
      if (distance <= threshold) {
        neighborCount += 1;
        neighborQualitySum += qualityValues[j];
      }
    }

    if (neighborCount > 0) {
      const neighborMean = neighborQualitySum / neighborCount;
      const normalized = hasQualityRange ? (neighborMean - qualityMin) / qualityRange : 1;
      candidates[i].stabilityScore = clampNumber(normalized, 0, 1);
    } else {
      candidates[i].stabilityScore = 0;
    }
  }
};

const applyBucketedStabilityScores = (
  candidates: ScoredCandidate[],
  qualityValues: number[],
  qualityMin: number,
  qualityMax: number,
  threshold: number,
  scaleMap: ParameterScaleMap
): void => {
  const qualityRange = qualityMax - qualityMin;
  const hasQualityRange = qualityRange > 1e-12;
  const step = Math.max(threshold, 0.01);
  const bucketMap = new Map<string, number[]>();
  const candidateKeys: string[][] = new Array(candidates.length);

  for (let i = 0; i < candidates.length; i += 1) {
    const keys = buildBucketKeysForCandidate(candidates[i].parameters, step, scaleMap);
    candidateKeys[i] = keys;

    for (const key of keys) {
      let bucket = bucketMap.get(key);
      if (!bucket) {
        bucket = [];
        bucketMap.set(key, bucket);
      }
      bucket.push(i);
    }
  }

  for (let i = 0; i < candidates.length; i += 1) {
    const neighborIndexes = new Set<number>([i]);
    for (const key of candidateKeys[i]) {
      const indexes = bucketMap.get(key);
      if (indexes) {
        indexes.forEach(index => neighborIndexes.add(index));
      }
    }

    let neighborCount = 0;
    let neighborQualitySum = 0;
    for (const neighborIndex of neighborIndexes) {
      if (neighborIndex === i) {
        continue;
      }
      const distance = computeParameterDistance(candidates[i].parameters, candidates[neighborIndex].parameters, scaleMap);
      if (distance <= threshold) {
        neighborCount += 1;
        neighborQualitySum += qualityValues[neighborIndex];
      }
    }

    if (neighborCount > 0) {
      const neighborMean = neighborQualitySum / neighborCount;
      const normalized = hasQualityRange ? (neighborMean - qualityMin) / qualityRange : 1;
      candidates[i].stabilityScore = clampNumber(normalized, 0, 1);
    } else {
      candidates[i].stabilityScore = 0;
    }
  }
};

const buildBucketKeysForCandidate = (
  parameters: Record<string, unknown>,
  step: number,
  scaleMap: ParameterScaleMap
): string[] => {
  const safeStep = Math.max(step, 0.01);
  const keys = new Set<string>();
  const vectorParts: string[] = [];
  const sortedKeys = Object.keys(parameters ?? {}).sort();

  for (const key of sortedKeys) {
    if (STABILITY_IGNORED_PARAMS.has(key)) {
      continue;
    }
    if (!scaleMap.has(key)) {
      continue;
    }
    const value = parameters[key];
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      continue;
    }

    const scale = scaleMap.get(key) ?? 1;
    const normalized = value / Math.max(scale, 1e-9);
    const quantized = Math.round(normalized / safeStep);
    vectorParts.push(`${key}=${quantized}`);
    keys.add(`${key}:${quantized}`);
    keys.add(`${key}:${quantized - 1}`);
    keys.add(`${key}:${quantized + 1}`);
  }

  if (!vectorParts.length) {
    keys.add('vector:__empty__');
    return Array.from(keys);
  }

  const vectorKey = `vector:${vectorParts.join('|')}`;
  keys.add(vectorKey);
  return Array.from(keys);
};

const computeParameterDistance = (
  a: Record<string, unknown>,
  b: Record<string, unknown>,
  scaleMap: ParameterScaleMap
): number => {
  const parameterKeys = new Set<string>();
  Object.keys(a ?? {}).forEach(key => {
    if (!STABILITY_IGNORED_PARAMS.has(key) && scaleMap.has(key)) {
      parameterKeys.add(key);
    }
  });
  Object.keys(b ?? {}).forEach(key => {
    if (!STABILITY_IGNORED_PARAMS.has(key) && scaleMap.has(key)) {
      parameterKeys.add(key);
    }
  });

  if (!parameterKeys.size) {
    return 0;
  }

  let sumSq = 0;
  let k = 0;
  for (const key of parameterKeys) {
    const rawA = a?.[key];
    const rawB = b?.[key];
    const valueA = typeof rawA === 'number' && Number.isFinite(rawA) ? rawA : null;
    const valueB = typeof rawB === 'number' && Number.isFinite(rawB) ? rawB : null;

    if (valueA !== null && valueB !== null) {
      const scale = Math.max(scaleMap.get(key) ?? 1, 1e-9);
      const z = Math.abs(valueA - valueB) / scale;
      sumSq += z * z;
    } else {
      sumSq += 1;
    }
    k += 1;
  }

  return Math.sqrt(sumSq / Math.max(1, k));
};

const computeParameterScales = (candidates: NormalizedCandidate[]): ParameterScaleMap => {
  const valuesByKey = new Map<string, number[]>();
  candidates.forEach(({ parameters }) => {
    Object.keys(parameters ?? {}).forEach((key) => {
      if (STABILITY_IGNORED_PARAMS.has(key)) {
        return;
      }
      const value = parameters[key];
      if (typeof value === 'number' && Number.isFinite(value)) {
        let arr = valuesByKey.get(key);
        if (!arr) {
          arr = [];
          valuesByKey.set(key, arr);
        }
        arr.push(value);
      }
    });
  });

  const scaleMap: ParameterScaleMap = new Map();
  valuesByKey.forEach((values, key) => {
    if (!values.length) {
      return;
    }
    values.sort((a, b) => a - b);
    const hiIndex = Math.floor((values.length - 1) * 0.9);
    const loIndex = Math.floor((values.length - 1) * 0.1);
    const p90 = values[hiIndex];
    const p10 = values[loIndex];
    const spread = p90 - p10;
    if (spread < 1e-8) {
      return;
    }
    const scale = Math.max(spread, 1e-6);
    scaleMap.set(key, scale);
  });

  return scaleMap;
};

const parseParameters = (raw: unknown): Record<string, unknown> | null => {
  if (raw === null || raw === undefined) {
    return null;
  }

  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed as Record<string, unknown> : null;
    } catch {
      return null;
    }
  }

  if (typeof raw === 'object') {
    return { ...(raw as Record<string, unknown>) };
  }

  return null;
};

const parseNullableNumber = (value: unknown): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const clampNumber = (value: number, minValue: number, maxValue: number): number => {
  if (!Number.isFinite(value)) {
    return minValue;
  }
  return Math.min(Math.max(value, minValue), maxValue);
};
