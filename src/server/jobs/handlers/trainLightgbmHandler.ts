import { randomUUID } from 'crypto';
import { promises as fs } from 'fs';
import os from 'os';
import path from 'path';
import { JobHandler } from '../JobScheduler';
import { JobHandlerDependencies } from '../types';
import type { LightgbmDatasetStatsSummary, LightgbmValidationMetricsSummary } from '../../database/types';

const TRAIN_SOURCE = 'train-lightgbm-job';
const TRAIN_SUMMARY_PREFIX = 'STRATCRAFT_LIGHTGBM_TRAIN_SUMMARY=';

const parseMetadataNumber = (value: unknown): number | null => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null;
  }
  return value;
};

export function createTrainLightgbmHandler(deps: JobHandlerDependencies): JobHandler {
  return async (ctx) => {
    const metadata = ctx.job.metadata ?? {};
    const modelName = typeof metadata.modelName === 'string' ? metadata.modelName.trim() : '';
    if (!modelName) {
      throw new Error('LightGBM training job missing model name.');
    }

    const numIterationsRaw = parseMetadataNumber(metadata.numIterations);
    const numIterations = numIterationsRaw !== null ? Math.max(1, Math.trunc(numIterationsRaw)) : null;
    const learningRateRaw = parseMetadataNumber(metadata.learningRate);
    const learningRate = learningRateRaw !== null ? Math.max(0.0, learningRateRaw) : null;
    const numLeavesRaw = parseMetadataNumber(metadata.numLeaves);
    const numLeaves = numLeavesRaw !== null ? Math.max(2, Math.trunc(numLeavesRaw)) : null;
    const maxDepthRaw = parseMetadataNumber(metadata.maxDepth);
    const maxDepth = maxDepthRaw !== null ? Math.max(-1, Math.trunc(maxDepthRaw)) : null;
    const minDataInLeafRaw = parseMetadataNumber(metadata.minDataInLeaf);
    const minDataInLeaf = minDataInLeafRaw !== null ? Math.max(1, Math.trunc(minDataInLeafRaw)) : null;
    const minGainToSplitRaw = parseMetadataNumber(metadata.minGainToSplit);
    const minGainToSplit = minGainToSplitRaw !== null ? Math.max(0.0, minGainToSplitRaw) : null;
    const lambdaL1Raw = parseMetadataNumber(metadata.lambdaL1);
    const lambdaL1 = lambdaL1Raw !== null ? Math.max(0.0, lambdaL1Raw) : null;
    const lambdaL2Raw = parseMetadataNumber(metadata.lambdaL2);
    const lambdaL2 = lambdaL2Raw !== null ? Math.max(0.0, lambdaL2Raw) : null;
    const featureFractionRaw = parseMetadataNumber(metadata.featureFraction);
    const featureFraction = featureFractionRaw !== null ? Math.min(1.0, Math.max(0.0, featureFractionRaw)) : null;
    const baggingFractionRaw = parseMetadataNumber(metadata.baggingFraction);
    const baggingFraction = baggingFractionRaw !== null ? Math.min(1.0, Math.max(0.0, baggingFractionRaw)) : null;
    const baggingFreqRaw = parseMetadataNumber(metadata.baggingFreq);
    const baggingFreq = baggingFreqRaw !== null ? Math.max(0, Math.trunc(baggingFreqRaw)) : null;
    const earlyStoppingRoundRaw = parseMetadataNumber(metadata.earlyStoppingRound);
    const earlyStoppingRound = earlyStoppingRoundRaw !== null ? Math.max(0, Math.trunc(earlyStoppingRoundRaw)) : null;

    const parseTrainingSummary = (output: string): any | null => {
      const lines = output.split(/\r?\n/);
      for (let idx = lines.length - 1; idx >= 0; idx -= 1) {
        const line = lines[idx];
        if (!line.startsWith(TRAIN_SUMMARY_PREFIX)) {
          continue;
        }
        const payload = line.slice(TRAIN_SUMMARY_PREFIX.length);
        try {
          return JSON.parse(payload);
        } catch {
          return null;
        }
      }
      return null;
    };

    const parseSummaryNumber = (value: unknown): number | null => {
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        return null;
      }
      return value;
    };

    const parseSummaryInteger = (value: unknown): number | null => {
      const raw = parseSummaryNumber(value);
      return raw !== null ? Math.trunc(raw) : null;
    };

    const sanitizeDatasetStats = (value: unknown): LightgbmDatasetStatsSummary | null => {
      if (!value || typeof value !== 'object') {
        return null;
      }
      const candidate = value as Record<string, any>;
      const rowCount = parseSummaryInteger(candidate.rowCount);
      const featureCount = parseSummaryInteger(candidate.featureCount);
      if (rowCount === null || featureCount === null) {
        return null;
      }
      const startDate = typeof candidate.startDate === 'string' ? candidate.startDate : null;
      const endDate = typeof candidate.endDate === 'string' ? candidate.endDate : null;
      const labelCountsRaw = candidate.labelCounts;
      const labelCounts: Record<string, number> = {};
      if (labelCountsRaw && typeof labelCountsRaw === 'object') {
        for (const [key, rawCount] of Object.entries(labelCountsRaw)) {
          const parsed = parseSummaryInteger(rawCount);
          if (typeof key === 'string' && parsed !== null) {
            labelCounts[key] = parsed;
          }
        }
      }
      return {
        rowCount,
        featureCount,
        startDate,
        endDate,
        labelCounts
      };
    };

    const sanitizeValidationMetrics = (value: unknown): LightgbmValidationMetricsSummary | null => {
      if (!value || typeof value !== 'object') {
        return null;
      }
      const candidate = value as Record<string, any>;
      const topK = parseSummaryInteger(candidate.topK);
      if (topK === null) {
        return null;
      }
      return {
        topK,
        positiveRate: parseSummaryNumber(candidate.positiveRate),
        positives: parseSummaryInteger(candidate.positives),
        totalRows: parseSummaryInteger(candidate.totalRows),
        dayCount: parseSummaryInteger(candidate.dayCount),
        precisionAtK: parseSummaryNumber(candidate.precisionAtK),
        hitRateAtK: parseSummaryNumber(candidate.hitRateAtK),
        ndcgAtK: parseSummaryNumber(candidate.ndcgAtK),
        avgMaxMultiple: parseSummaryNumber(candidate.avgMaxMultiple)
      };
    };

    const existingModel = await deps.db.lightgbmModels.getLightgbmModelByName(modelName);
    if (existingModel) {
      await deps.strategyRegistry.ensureLightgbmModelTemplates();
      await deps.strategyRegistry.ensureLightgbmDefaults();
      return {
        message: `LightGBM model \"${existingModel.name}\" already stored.`,
        meta: {
          modelId: existingModel.id,
          modelName: existingModel.name
        }
      };
    }

    const outputDir = path.join(os.tmpdir(), 'stratcraft');
    await fs.mkdir(outputDir, { recursive: true });
    const outputPath = path.join(outputDir, `lightgbm_${ctx.job.id}.txt`);

    const args = ['--output', outputPath];
    if (numIterations !== null) {
      args.push('--num-iterations', String(numIterations));
    }
    if (learningRate !== null) {
      args.push('--learning-rate', String(learningRate));
    }
    if (numLeaves !== null) {
      args.push('--num-leaves', String(numLeaves));
    }
    if (maxDepth !== null) {
      args.push('--max-depth', String(maxDepth));
    }
    if (minDataInLeaf !== null) {
      args.push('--min-data-in-leaf', String(minDataInLeaf));
    }
    if (minGainToSplit !== null) {
      args.push('--min-gain-to-split', String(minGainToSplit));
    }
    if (lambdaL1 !== null) {
      args.push('--lambda-l1', String(lambdaL1));
    }
    if (lambdaL2 !== null) {
      args.push('--lambda-l2', String(lambdaL2));
    }
    if (featureFraction !== null) {
      args.push('--feature-fraction', String(featureFraction));
    }
    if (baggingFraction !== null) {
      args.push('--bagging-fraction', String(baggingFraction));
    }
    if (baggingFreq !== null) {
      args.push('--bagging-freq', String(baggingFreq));
    }
    if (earlyStoppingRound !== null) {
      args.push('--early-stopping-round', String(earlyStoppingRound));
    }

    ctx.loggingService.info(TRAIN_SOURCE, `Training LightGBM model "${modelName}"`, {
      jobId: ctx.job.id,
      numIterations,
      learningRate,
      numLeaves,
      maxDepth,
      minDataInLeaf,
      minGainToSplit,
      lambdaL1,
      lambdaL2,
      featureFraction,
      baggingFraction,
      baggingFreq,
      earlyStoppingRound
    });

    try {
      const { stdout, stderr } = await deps.engineCli.runWithOutput('train-lightgbm', args, ctx.abortSignal, {
        jobId: ctx.job.id
      });

      const treeText = (await fs.readFile(outputPath, 'utf8')).trim();
      if (!treeText) {
        throw new Error('Trained LightGBM model output was empty.');
      }

      const summary = parseTrainingSummary(stdout) ?? parseTrainingSummary(stderr);
      const hyper = summary && typeof summary === 'object' ? (summary as Record<string, any>).hyperparameters : null;
      const hyperObj = hyper && typeof hyper === 'object' ? (hyper as Record<string, any>) : null;
      const numIterationsFinal = parseSummaryInteger(hyperObj?.numIterations) ?? numIterations;
      const learningRateFinal = parseSummaryNumber(hyperObj?.learningRate) ?? learningRate;
      const numLeavesFinal = parseSummaryInteger(hyperObj?.numLeaves) ?? numLeaves;
      const maxDepthFinal = parseSummaryInteger(hyperObj?.maxDepth) ?? maxDepth;
      const minDataInLeafFinal = parseSummaryInteger(hyperObj?.minDataInLeaf) ?? minDataInLeaf;
      const minGainToSplitFinal = parseSummaryNumber(hyperObj?.minGainToSplit) ?? minGainToSplit;
      const lambdaL1Final = parseSummaryNumber(hyperObj?.lambdaL1) ?? lambdaL1;
      const lambdaL2Final = parseSummaryNumber(hyperObj?.lambdaL2) ?? lambdaL2;
      const featureFractionFinal = parseSummaryNumber(hyperObj?.featureFraction) ?? featureFraction;
      const baggingFractionFinal = parseSummaryNumber(hyperObj?.baggingFraction) ?? baggingFraction;
      const baggingFreqFinal = parseSummaryInteger(hyperObj?.baggingFreq) ?? baggingFreq;
      const earlyStoppingRoundFinal = parseSummaryInteger(hyperObj?.earlyStoppingRound) ?? earlyStoppingRound;
      const trainDatasetStats = sanitizeDatasetStats((summary as Record<string, any>)?.trainDataset);
      const validationDatasetStats = sanitizeDatasetStats((summary as Record<string, any>)?.validationDataset);
      const validationMetrics = sanitizeValidationMetrics((summary as Record<string, any>)?.validationMetrics);

      const created = await deps.db.lightgbmModels.createLightgbmModel({
        id: randomUUID(),
        name: modelName,
        treeText,
        source: 'training',
        numIterations: numIterationsFinal,
        learningRate: learningRateFinal,
        numLeaves: numLeavesFinal,
        maxDepth: maxDepthFinal,
        minDataInLeaf: minDataInLeafFinal,
        minGainToSplit: minGainToSplitFinal,
        lambdaL1: lambdaL1Final,
        lambdaL2: lambdaL2Final,
        featureFraction: featureFractionFinal,
        baggingFraction: baggingFractionFinal,
        baggingFreq: baggingFreqFinal,
        earlyStoppingRound: earlyStoppingRoundFinal,
        trainDatasetStats,
        validationDatasetStats,
        validationMetrics,
        engineStdout: stdout,
        engineStderr: stderr,
        trainedAt: new Date()
      });
      await deps.strategyRegistry.ensureLightgbmModelTemplates();
      await deps.strategyRegistry.ensureLightgbmDefaults();

      return {
        message: `Trained LightGBM model "${created.name}".`,
        meta: {
          modelId: created.id,
          modelName: created.name
        }
      };
    } finally {
      try {
        await fs.unlink(outputPath);
      } catch {
        // Ignore cleanup errors
      }
    }
  };
}
