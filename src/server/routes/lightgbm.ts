import express, { NextFunction, Request, Response } from 'express';
import { randomUUID } from 'crypto';

const router = express.Router();

const LIGHTGBM_PAGE_PATH = '/admin/lightgbm';
const DEFAULT_LIGHTGBM_NUM_ITERATIONS = 800;
const DEFAULT_LIGHTGBM_LEARNING_RATE = 0.05;
const DEFAULT_LIGHTGBM_NUM_LEAVES = 15;
const DEFAULT_LIGHTGBM_MAX_DEPTH = 5;
const DEFAULT_LIGHTGBM_MIN_DATA_IN_LEAF = 100;
const DEFAULT_LIGHTGBM_MIN_GAIN_TO_SPLIT = 0.01;
const DEFAULT_LIGHTGBM_LAMBDA_L1 = 0.0;
const DEFAULT_LIGHTGBM_LAMBDA_L2 = 5.0;
const DEFAULT_LIGHTGBM_FEATURE_FRACTION = 0.6;
const DEFAULT_LIGHTGBM_BAGGING_FRACTION = 0.6;
const DEFAULT_LIGHTGBM_BAGGING_FREQ = 5;
const DEFAULT_LIGHTGBM_EARLY_STOPPING_ROUND = 100;

type LightgbmTrainHyperparams = {
  numIterations: number;
  learningRate: number;
  numLeaves: number;
  maxDepth: number;
  minDataInLeaf: number;
  minGainToSplit: number;
  lambdaL1: number;
  lambdaL2: number;
  featureFraction: number;
  baggingFraction: number;
  baggingFreq: number;
  earlyStoppingRound: number;
};

const DEFAULT_LIGHTGBM_TRAIN_PARAMS: LightgbmTrainHyperparams = {
  numIterations: DEFAULT_LIGHTGBM_NUM_ITERATIONS,
  learningRate: DEFAULT_LIGHTGBM_LEARNING_RATE,
  numLeaves: DEFAULT_LIGHTGBM_NUM_LEAVES,
  maxDepth: DEFAULT_LIGHTGBM_MAX_DEPTH,
  minDataInLeaf: DEFAULT_LIGHTGBM_MIN_DATA_IN_LEAF,
  minGainToSplit: DEFAULT_LIGHTGBM_MIN_GAIN_TO_SPLIT,
  lambdaL1: DEFAULT_LIGHTGBM_LAMBDA_L1,
  lambdaL2: DEFAULT_LIGHTGBM_LAMBDA_L2,
  featureFraction: DEFAULT_LIGHTGBM_FEATURE_FRACTION,
  baggingFraction: DEFAULT_LIGHTGBM_BAGGING_FRACTION,
  baggingFreq: DEFAULT_LIGHTGBM_BAGGING_FREQ,
  earlyStoppingRound: DEFAULT_LIGHTGBM_EARLY_STOPPING_ROUND,
};

const buildLightgbmModelName = (overrides: Partial<LightgbmTrainHyperparams> = {}): string => {
  const params: LightgbmTrainHyperparams = {
    ...DEFAULT_LIGHTGBM_TRAIN_PARAMS,
    ...overrides
  };

  const pairs: Array<{ key: string; value: number; defaultValue: number }> = [
    { key: 'num_iterations', value: params.numIterations, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.numIterations },
    { key: 'learning_rate', value: params.learningRate, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.learningRate },
    { key: 'num_leaves', value: params.numLeaves, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.numLeaves },
    { key: 'max_depth', value: params.maxDepth, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.maxDepth },
    { key: 'min_data_in_leaf', value: params.minDataInLeaf, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.minDataInLeaf },
    { key: 'min_gain_to_split', value: params.minGainToSplit, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.minGainToSplit },
    { key: 'lambda_l1', value: params.lambdaL1, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.lambdaL1 },
    { key: 'lambda_l2', value: params.lambdaL2, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.lambdaL2 },
    { key: 'feature_fraction', value: params.featureFraction, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.featureFraction },
    { key: 'bagging_fraction', value: params.baggingFraction, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.baggingFraction },
    { key: 'bagging_freq', value: params.baggingFreq, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.baggingFreq },
    { key: 'early_stopping_round', value: params.earlyStoppingRound, defaultValue: DEFAULT_LIGHTGBM_TRAIN_PARAMS.earlyStoppingRound }
  ];

  const selected = pairs
    .filter(({ value, defaultValue }) => value !== defaultValue)
    .map(({ key, value }) => `${key}=${value}`);

  return selected.length > 0 ? selected.join(' ') : 'defaults=true';
};

const buildLightgbmTrainingParamStringFromModel = (model: any): string | null => {
  const pairs: Array<[string, unknown]> = [
    ['num_iterations', model?.numIterations],
    ['learning_rate', model?.learningRate],
    ['num_leaves', model?.numLeaves],
    ['max_depth', model?.maxDepth],
    ['min_data_in_leaf', model?.minDataInLeaf],
    ['min_gain_to_split', model?.minGainToSplit],
    ['lambda_l1', model?.lambdaL1],
    ['lambda_l2', model?.lambdaL2],
    ['feature_fraction', model?.featureFraction],
    ['bagging_fraction', model?.baggingFraction],
    ['bagging_freq', model?.baggingFreq],
    ['early_stopping_round', model?.earlyStoppingRound]
  ];

  const parts: string[] = [];
  for (const [key, value] of pairs) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      parts.push(`${key}=${value}`);
    }
  }

  return parts.length > 0 ? parts.join(' ') : null;
};

const parseOptionalNumber = (raw: unknown): number | null => {
  if (typeof raw !== 'string') {
    return null;
  }
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const value = Number(trimmed);
  return Number.isFinite(value) ? value : null;
};

const pickDefaultLightgbmModelName = (base: string, usedNames: Set<string>): string => {
  if (!usedNames.has(base.toLowerCase())) {
    return base;
  }

  for (let dup = 2; dup < 1000; dup += 1) {
    const candidate = `${base} dup=${dup}`;
    if (!usedNames.has(candidate.toLowerCase())) {
      return candidate;
    }
  }

  return `${base} dup=${randomUUID().slice(0, 8)}`;
};

router.get('/', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const lightgbmModels = (await req.db.lightgbmModels.listLightgbmModels()).map(model => ({
      ...model,
      trainingParams: buildLightgbmTrainingParamStringFromModel(model)
    }));
    const usedLightgbmNames = new Set(lightgbmModels.map(model => model.name.toLowerCase()));
    const defaultLightgbmModelName = pickDefaultLightgbmModelName(buildLightgbmModelName(), usedLightgbmNames);

    const [trainingStartDate, trainingEndDate] = await Promise.all([
      req.db.settings.getSettingValue('LIGHTGBM_TRAINING_START_DATE'),
      req.db.settings.getSettingValue('LIGHTGBM_TRAINING_END_DATE')
    ]);

    const tickers = await req.db.tickers.getTickers();
    const trainingTickerCount = tickers.filter(ticker => ticker.training).length;
    const validationTickerCount = tickers.length - trainingTickerCount;

    res.render('pages/lightgbm', {
      title: 'LightGBM Models',
      page: 'lightgbm',
      lightgbmModels,
      defaultLightgbmModelName,
      lightgbmTrainingStartDate: trainingStartDate,
      lightgbmTrainingEndDate: trainingEndDate,
      trainingTickerCount,
      validationTickerCount,
      totalTickerCount: tickers.length,
      defaultLightgbmNumIterations: DEFAULT_LIGHTGBM_NUM_ITERATIONS,
      defaultLightgbmLearningRate: DEFAULT_LIGHTGBM_LEARNING_RATE,
      defaultLightgbmNumLeaves: DEFAULT_LIGHTGBM_NUM_LEAVES,
      defaultLightgbmMaxDepth: DEFAULT_LIGHTGBM_MAX_DEPTH,
      defaultLightgbmMinDataInLeaf: DEFAULT_LIGHTGBM_MIN_DATA_IN_LEAF,
      defaultLightgbmMinGainToSplit: DEFAULT_LIGHTGBM_MIN_GAIN_TO_SPLIT,
      defaultLightgbmLambdaL1: DEFAULT_LIGHTGBM_LAMBDA_L1,
      defaultLightgbmLambdaL2: DEFAULT_LIGHTGBM_LAMBDA_L2,
      defaultLightgbmFeatureFraction: DEFAULT_LIGHTGBM_FEATURE_FRACTION,
      defaultLightgbmBaggingFraction: DEFAULT_LIGHTGBM_BAGGING_FRACTION,
      defaultLightgbmBaggingFreq: DEFAULT_LIGHTGBM_BAGGING_FREQ,
      defaultLightgbmEarlyStoppingRound: DEFAULT_LIGHTGBM_EARLY_STOPPING_ROUND,
      user: req.user,
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error loading LightGBM page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load LightGBM models'
    });
  }
});

router.get('/models/output', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  const modelId = typeof req.query?.modelId === 'string' ? req.query.modelId.trim() : '';
  if (!modelId) {
    return res.status(400).json({ error: 'modelId is required' });
  }

  try {
    const output = await req.db.lightgbmModels.getLightgbmModelOutput(modelId);
    if (!output) {
      return res.status(404).json({ error: 'LightGBM model not found' });
    }
    return res.json(output);
  } catch (error) {
    console.error('Error loading LightGBM model output:', error);
    return res.status(500).json({ error: 'Failed to load LightGBM model output' });
  }
});

// Queue LightGBM training job (admin only)
router.post('/train', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const modelName = typeof req.body?.modelName === 'string' ? req.body.modelName.trim() : '';
    if (!modelName) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Model name is required to train LightGBM.')}`);
    }

    const existing = await req.db.lightgbmModels.getLightgbmModelByName(modelName);
    if (existing) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent(`Model name "${modelName}" already exists.`)}`);
    }

    const numIterationsRaw = parseOptionalNumber(req.body?.numIterations);
    if (numIterationsRaw !== null && (!Number.isInteger(numIterationsRaw) || numIterationsRaw <= 0)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Number of iterations must be a positive whole number.')}`);
    }
    const learningRateRaw = parseOptionalNumber(req.body?.learningRate);
    if (learningRateRaw !== null && (!Number.isFinite(learningRateRaw) || learningRateRaw <= 0 || learningRateRaw > 1)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Learning rate must be a number between 0 and 1.')}`);
    }

    const numLeavesRaw = parseOptionalNumber(req.body?.numLeaves);
    if (numLeavesRaw !== null && (!Number.isInteger(numLeavesRaw) || numLeavesRaw < 2)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Num leaves must be a whole number of at least 2.')}`);
    }
    const maxDepthRaw = parseOptionalNumber(req.body?.maxDepth);
    if (maxDepthRaw !== null && (!Number.isInteger(maxDepthRaw) || maxDepthRaw < -1)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Max depth must be -1 (no limit) or a whole number.')}`);
    }
    const minDataInLeafRaw = parseOptionalNumber(req.body?.minDataInLeaf);
    if (minDataInLeafRaw !== null && (!Number.isInteger(minDataInLeafRaw) || minDataInLeafRaw <= 0)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Min data in leaf must be a positive whole number.')}`);
    }
    const minGainToSplitRaw = parseOptionalNumber(req.body?.minGainToSplit);
    if (minGainToSplitRaw !== null && (!Number.isFinite(minGainToSplitRaw) || minGainToSplitRaw < 0)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Min gain to split must be a non-negative number.')}`);
    }
    const lambdaL1Raw = parseOptionalNumber(req.body?.lambdaL1);
    if (lambdaL1Raw !== null && (!Number.isFinite(lambdaL1Raw) || lambdaL1Raw < 0)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Lambda L1 must be a non-negative number.')}`);
    }
    const lambdaL2Raw = parseOptionalNumber(req.body?.lambdaL2);
    if (lambdaL2Raw !== null && (!Number.isFinite(lambdaL2Raw) || lambdaL2Raw < 0)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Lambda L2 must be a non-negative number.')}`);
    }
    const featureFractionRaw = parseOptionalNumber(req.body?.featureFraction);
    if (featureFractionRaw !== null && (!Number.isFinite(featureFractionRaw) || featureFractionRaw <= 0 || featureFractionRaw > 1)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Feature fraction must be a number between 0 and 1.')}`);
    }
    const baggingFractionRaw = parseOptionalNumber(req.body?.baggingFraction);
    if (baggingFractionRaw !== null && (!Number.isFinite(baggingFractionRaw) || baggingFractionRaw <= 0 || baggingFractionRaw > 1)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Bagging fraction must be a number between 0 and 1.')}`);
    }
    const baggingFreqRaw = parseOptionalNumber(req.body?.baggingFreq);
    if (baggingFreqRaw !== null && (!Number.isInteger(baggingFreqRaw) || baggingFreqRaw < 0)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Bagging freq must be a non-negative whole number.')}`);
    }
    const earlyStoppingRoundRaw = parseOptionalNumber(req.body?.earlyStoppingRound);
    if (earlyStoppingRoundRaw !== null && (!Number.isInteger(earlyStoppingRoundRaw) || earlyStoppingRoundRaw < 0)) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Early stopping round must be a non-negative whole number.')}`);
    }
    const metadata = {
      modelName,
      numIterations: numIterationsRaw ?? undefined,
      learningRate: learningRateRaw ?? undefined,
      numLeaves: numLeavesRaw ?? undefined,
      maxDepth: maxDepthRaw ?? undefined,
      minDataInLeaf: minDataInLeafRaw ?? undefined,
      minGainToSplit: minGainToSplitRaw ?? undefined,
      lambdaL1: lambdaL1Raw ?? undefined,
      lambdaL2: lambdaL2Raw ?? undefined,
      featureFraction: featureFractionRaw ?? undefined,
      baggingFraction: baggingFractionRaw ?? undefined,
      baggingFreq: baggingFreqRaw ?? undefined,
      earlyStoppingRound: earlyStoppingRoundRaw ?? undefined
    };

    const description = `Train LightGBM model: ${modelName}`;
    const job = req.jobScheduler.scheduleJob('train-lightgbm', {
      description,
      metadata
    });

    const message = `Queued LightGBM training job (${job.id}) for "${modelName}".`;
    res.redirect(`${LIGHTGBM_PAGE_PATH}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error queueing LightGBM training job:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to queue LightGBM training job';
    res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Add LightGBM model (admin only)
router.post('/models', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const modelName = typeof req.body?.modelName === 'string' ? req.body.modelName.trim() : '';
    const treeText = typeof req.body?.treeText === 'string' ? req.body.treeText.trim() : '';
    if (!modelName) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Model name is required.')}`);
    }
    if (!treeText) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Model tree text is required.')}`);
    }

    const existing = await req.db.lightgbmModels.getLightgbmModelByName(modelName);
    if (existing) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent(`Model name "${modelName}" already exists.`)}`);
    }

    const model = await req.db.lightgbmModels.createLightgbmModel({
      id: randomUUID(),
      name: modelName,
      treeText,
      source: 'manual'
    });
    await req.strategyRegistry.ensureLightgbmModelTemplates();
    await req.strategyRegistry.ensureLightgbmDefaults();

    const message = `Saved LightGBM model "${model.name}".`;
    res.redirect(`${LIGHTGBM_PAGE_PATH}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error saving LightGBM model:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to save LightGBM model';
    res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Update LightGBM model text (admin only)
router.post('/models/update', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const modelId = typeof req.body?.modelId === 'string' ? req.body.modelId.trim() : '';
    const treeText = typeof req.body?.treeText === 'string' ? req.body.treeText.trim() : '';
    if (!modelId) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Model ID is required to update tree text.')}`);
    }
    if (!treeText) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Updated tree text is required.')}`);
    }

    await req.db.lightgbmModels.updateLightgbmModelText(modelId, treeText);
    await req.strategyRegistry.ensureLightgbmModelTemplates();
    await req.strategyRegistry.ensureLightgbmDefaults();

    const message = 'LightGBM model text updated.';
    res.redirect(`${LIGHTGBM_PAGE_PATH}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error updating LightGBM model:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to update LightGBM model';
    res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Delete LightGBM model (admin only)
router.post('/models/delete', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const modelId = typeof req.body?.modelId === 'string' ? req.body.modelId.trim() : '';
    if (!modelId) {
      return res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent('Model ID is required to delete.')}`);
    }
    await req.db.lightgbmModels.deleteLightgbmModel(modelId);
    await req.strategyRegistry.ensureLightgbmModelTemplates();
    res.redirect(`${LIGHTGBM_PAGE_PATH}?success=${encodeURIComponent('LightGBM model deleted.')}`);
  } catch (error) {
    console.error('Error deleting LightGBM model:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to delete LightGBM model';
    res.redirect(`${LIGHTGBM_PAGE_PATH}?error=${encodeURIComponent(errorMessage)}`);
  }
});

export default router;
