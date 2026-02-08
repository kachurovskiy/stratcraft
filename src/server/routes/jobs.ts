import express, { NextFunction, Request, Response } from 'express';
import { JobType } from '../jobs/JobScheduler';

const router = express.Router();

const JOBS_PAGE_PATH = '/admin/jobs';
const JOB_LOGS_DEFAULT_LIMIT = 1000;
const JOB_LOGS_MAX_LIMIT = 5000;
const JOB_TYPE_OPTIONS: Array<{ value: JobType; label: string }> = [
  { value: 'candle-sync', label: 'Candle Sync' },
  { value: 'export-market-data', label: 'Refresh Market Data Snapshot' },
  { value: 'generate-signals', label: 'Generate Signals' },
  { value: 'backtest-active', label: 'Backtest Active' },
  { value: 'reconcile-trades', label: 'Reconcile Trades' },
  { value: 'plan-operations', label: 'Plan Operations' },
  { value: 'dispatch-operations', label: 'Dispatch Operations' },
  { value: 'optimize', label: 'Optimize' }
];
const VALID_JOB_TYPES = new Set(JOB_TYPE_OPTIONS.map(option => option.value));

const buildLightgbmTrainParamString = (metadata: Record<string, any> | undefined): string | null => {
  if (!metadata) {
    return null;
  }

  const pairs: Array<[string, unknown]> = [
    ['num_iterations', metadata.numIterations],
    ['learning_rate', metadata.learningRate],
    ['num_leaves', metadata.numLeaves],
    ['max_depth', metadata.maxDepth],
    ['min_data_in_leaf', metadata.minDataInLeaf],
    ['min_gain_to_split', metadata.minGainToSplit],
    ['lambda_l1', metadata.lambdaL1],
    ['lambda_l2', metadata.lambdaL2],
    ['feature_fraction', metadata.featureFraction],
    ['bagging_fraction', metadata.baggingFraction],
    ['bagging_freq', metadata.baggingFreq],
    ['early_stopping_round', metadata.earlyStoppingRound]
  ];

  const parts: string[] = [];
  for (const [key, value] of pairs) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      parts.push(`${key}=${value}`);
    }
  }

  return parts.length > 0 ? parts.join(' ') : null;
};

// Admin jobs page
router.get('/', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const jobTimeline = req.jobScheduler.getJobTimeline().map(job => ({
      ...job,
      isCancellable: job.status === 'queued' || job.status === 'running',
      lightgbmTrainParams: job.type === 'train-lightgbm' ? buildLightgbmTrainParamString(job.metadata) : null
    }));
    const remoteOptimizerJobs = [...await req.remoteOptimizerService.listJobs()].sort(
      (a, b) => b.createdAt.getTime() - a.createdAt.getTime()
    );
    res.render('pages/jobs', {
      title: 'Jobs',
      page: 'jobs',
      jobTimeline,
      remoteOptimizerJobs,
      jobTypeOptions: JOB_TYPE_OPTIONS,
      user: req.user,
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error loading jobs page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load jobs'
    });
  }
});

// Job log viewer (admin only)
router.get('/:jobId/logs', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const jobId = typeof req.params.jobId === 'string' ? req.params.jobId.trim() : '';
    if (!jobId) {
      return res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent('Job ID is required.')}`);
    }

    const job = req.jobScheduler.getJob(jobId);
    if (!job) {
      return res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent(`Job ${jobId} not found.`)}`);
    }

    const limitRaw = typeof req.query.limit === 'string' ? req.query.limit.trim() : '';
    const limitInput = limitRaw.length > 0 ? Number(limitRaw) : NaN;
    const pageLimit = Number.isFinite(limitInput)
      ? Math.min(JOB_LOGS_MAX_LIMIT, Math.max(1, Math.trunc(limitInput)))
      : JOB_LOGS_DEFAULT_LIMIT;

    const offsetInput = typeof req.query.offset === 'string' ? Number(req.query.offset) : NaN;
    const offset = Number.isFinite(offsetInput) ? Math.max(0, Math.trunc(offsetInput)) : 0;

    const jobLogs = await req.loggingService.getJobLogs(jobId, pageLimit, offset);
    const prevOffset = Math.max(0, offset - pageLimit);
    const nextOffset = offset + pageLimit;

    res.render('pages/job-logs', {
      title: `Job Logs`,
      page: 'jobs',
      user: req.user,
      job,
      jobLogs,
      pageLimit,
      offset,
      prevOffset,
      nextOffset,
      hasPrev: offset > 0,
      hasNext: jobLogs.length === pageLimit
    });
  } catch (error) {
    console.error('Error loading job logs:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load job logs'
    });
  }
});

// Queue job (admin only)
router.post('/queue', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const jobTypeInput = typeof req.body?.jobType === 'string' ? req.body.jobType.trim() : '';
    if (!VALID_JOB_TYPES.has(jobTypeInput as JobType)) {
      return res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent('Select a valid job type to queue.')}`);
    }

    const descriptionInput = typeof req.body?.description === 'string' ? req.body.description.trim() : '';
    const startAtInput = typeof req.body?.startAt === 'string' ? req.body.startAt.trim() : '';
    let startAt: Date | undefined;

    if (startAtInput) {
      const parsed = new Date(startAtInput);
      if (Number.isNaN(parsed.getTime())) {
        return res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent('Invalid scheduled time provided.')}`);
      }
      startAt = parsed;
    }

    const job = req.jobScheduler.scheduleJob(jobTypeInput as JobType, {
      description: descriptionInput || undefined,
      startAt
    });

    const message = `Queued ${job.type} job (${job.id}).`;
    res.redirect(`${JOBS_PAGE_PATH}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error queueing job:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to queue job';
    res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Cancel queued or running job (admin only)
router.post('/cancel', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const jobId = typeof req.body?.jobId === 'string' ? req.body.jobId.trim() : '';
    if (!jobId) {
      return res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent('Job ID is required to cancel a job.')}`);
    }

    const pendingJob = req.jobScheduler.getQueuedJobs().find(job => job.id === jobId);
    if (!pendingJob) {
      return res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent('Job is no longer queued or running.')}`);
    }

    const reasonInput = typeof req.body?.reason === 'string' ? req.body.reason.trim() : '';
    const reason = reasonInput || 'Cancelled by admin from jobs page';
    const cancelled = req.jobScheduler.cancelJob(jobId, reason);

    if (!cancelled) {
      return res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent('Unable to cancel job. It may have already finished.')}`);
    }

    const actionLabel = pendingJob.status === 'running' ? 'Cancellation requested for' : 'Cancelled';
    const message = `${actionLabel} ${pendingJob.type} job (${jobId}).`;
    res.redirect(`${JOBS_PAGE_PATH}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error cancelling job:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to cancel job';
    res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Delete finished remote optimization jobs (admin only)
router.post('/remote-optimizer-jobs/delete-finished', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const deletedCount = await req.db.remoteOptimizerJobs.deleteFinishedRemoteOptimizerJobs();
    const message = deletedCount === 0
      ? 'No finished remote optimization jobs to delete'
      : `Deleted ${deletedCount} finished remote optimization job${deletedCount === 1 ? '' : 's'}`;
    res.redirect(`${JOBS_PAGE_PATH}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error deleting finished remote optimization jobs:', error);
    const errorMessage = error instanceof Error
      ? error.message
      : 'Failed to delete finished remote optimization jobs';
    res.redirect(`${JOBS_PAGE_PATH}?error=${encodeURIComponent(errorMessage)}`);
  }
});

export default router;
