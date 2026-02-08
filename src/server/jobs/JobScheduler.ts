import { randomUUID } from 'crypto';
import { DEFAULT_AUTO_OPTIMIZATION_DELAY_SECONDS, SETTING_KEYS } from '../constants';
import type { Database } from '../database/Database';
import { LoggingService, LogSource } from '../services/LoggingService';

export type JobType =
  | 'engine-compile'
  | 'candle-sync'
  | 'export-market-data'
  | 'generate-signals'
  | 'backtest-active'
  | 'reconcile-trades'
  | 'plan-operations'
  | 'dispatch-operations'
  | 'optimize'
  | 'train-lightgbm';

export type JobStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';

export interface JobResult {
  message?: string;
  meta?: Record<string, any>;
}

export interface JobScheduleOptions {
  startAt?: Date;
  maxRetries?: number;
  description?: string;
  metadata?: Record<string, any>;
}

interface JobRecord {
  id: string;
  type: JobType;
  status: JobStatus;
  scheduledFor: Date;
  createdAt: Date;
  startedAt?: Date;
  finishedAt?: Date;
  attempts: number;
  maxRetries: number;
  description?: string;
  metadata?: Record<string, any>;
  result?: JobResult;
  lastError?: string;
  cancelReason?: string;
  cancellationRequestedAt?: Date;
  abortController: AbortController;
}

export interface JobSnapshot {
  id: string;
  type: JobType;
  status: JobStatus;
  scheduledFor: Date;
  createdAt: Date;
  startedAt?: Date;
  finishedAt?: Date;
  attempts: number;
  maxRetries: number;
  description?: string;
  metadata?: Record<string, any>;
  result?: JobResult;
  lastError?: string;
  cancelReason?: string;
  cancellationRequestedAt?: Date;
}

export interface JobHandlerContext {
  job: JobSnapshot;
  abortSignal: AbortSignal;
  loggingService: LoggingService;
  scheduler: JobScheduler;
}

export type JobHandler = (context: JobHandlerContext) => Promise<JobResult | void>;

export interface JobDashboardState {
  currentJob: JobSnapshot | null;
  queuedJobs: JobSnapshot[];
  recentJobs: JobSnapshot[];
  engineReady: boolean;
  idleSince: Date | null;
}

const JOB_SCHEDULER_SOURCE: LogSource = 'job-scheduler';
const OPTIMIZE_SCHEDULE_COOLDOWN_MS = 3 * 60 * 60 * 1000;

export class JobScheduler {
  private jobs = new Map<string, JobRecord>();
  private jobOrder: string[] = [];
  private currentJobId: string | null = null;
  private currentJobPromise: Promise<void> | null = null;
  private wakeTimer: NodeJS.Timeout | null = null;
  private idleTimer: NodeJS.Timeout | null = null;
  private idleDelayMs: number;
  private defaultMaxRetries: number;
  private initialRetryDelayMs: number;
  private retryDelayCeilingMs: number;
  private idleSince: Date | null = null;
  private autoOptimizationEnabled: boolean;
  private db: Database;

  constructor(
    private readonly loggingService: LoggingService,
    private readonly jobHandlers: Record<JobType, JobHandler>,
    db: Database
  ) {
    this.defaultMaxRetries = 5;
    this.idleDelayMs = 5 * 60 * 1000;
    this.initialRetryDelayMs = 15_000;
    this.retryDelayCeilingMs = 5 * 60 * 1000;
    this.autoOptimizationEnabled = true;
    this.db = db;
    this.planIdleOptimizationCheck();
  }

  async refreshAutoOptimizationSettings(): Promise<void> {
    const [enabledRaw, delayRaw] = await Promise.all([
      this.db.settings.getSettingValue(SETTING_KEYS.AUTO_OPTIMIZATION_ENABLED),
      this.db.settings.getSettingValue(SETTING_KEYS.AUTO_OPTIMIZATION_DELAY_SECONDS)
    ]);
    const enabled = enabledRaw === 'true';
    const delaySeconds = Number(delayRaw) || DEFAULT_AUTO_OPTIMIZATION_DELAY_SECONDS;

    let shouldReschedule = false;

    if (enabled !== this.autoOptimizationEnabled) {
      this.autoOptimizationEnabled = enabled;
      shouldReschedule = true;
    }

    const normalizedDelay = Math.max(0, delaySeconds * 1000);
    if (Number.isFinite(normalizedDelay) && normalizedDelay !== this.idleDelayMs) {
      this.idleDelayMs = normalizedDelay;
      shouldReschedule = true;
    }

    if (shouldReschedule) {
      this.planIdleOptimizationCheck();
    }
  }

  scheduleJob(type: JobType, options: JobScheduleOptions = {}): JobSnapshot {
    if (!this.jobHandlers[type]) {
      throw new Error(`No handler registered for job ${type}`);
    }

    const job: JobRecord = {
      id: randomUUID(),
      type,
      status: 'queued',
      scheduledFor: options.startAt ?? new Date(),
      createdAt: new Date(),
      attempts: 0,
      maxRetries: options.maxRetries ?? this.defaultMaxRetries,
      description: options.description,
      metadata: options.metadata,
      abortController: new AbortController()
    };

    this.jobs.set(job.id, job);
    this.jobOrder.push(job.id);
    this.recordActivity();

    if (type !== 'optimize') {
      this.preemptOptimizeJob(`Preempted by ${type} schedule`);
    }

    this.wakeUp();
    return this.toSnapshot(job);
  }

  cancelJob(jobId: string, reason: string = 'Cancelled by user request'): boolean {
    const job = this.jobs.get(jobId);
    if (!job) return false;

    if (job.status === 'queued') {
      job.status = 'cancelled';
      job.finishedAt = new Date();
      job.lastError = reason;
      job.cancelReason = reason;
      this.loggingService.info(JOB_SCHEDULER_SOURCE, `Cancelled queued job ${job.type}`, {
        jobId: job.id,
        reason
      });
      this.recordActivity();
      return true;
    }

    if (job.status === 'running') {
      job.cancelReason = reason;
      job.cancellationRequestedAt = new Date();
      job.abortController.abort();
      this.loggingService.info(JOB_SCHEDULER_SOURCE, `Cancellation requested for running job ${job.type}`, {
        jobId: job.id,
        reason
      });
      return true;
    }

    return false;
  }

  cancelJobsByType(type: JobType, reason?: string): number {
    let cancelled = 0;
    for (const job of this.jobs.values()) {
      if (job.type === type && this.cancelJob(job.id, reason)) {
        cancelled += 1;
      }
    }
    return cancelled;
  }

  cancelAllJobs(reason?: string): number {
    let cancelled = 0;
    for (const job of this.jobs.values()) {
      if (this.cancelJob(job.id, reason)) {
        cancelled += 1;
      }
    }
    return cancelled;
  }

  hasPendingJob(predicate: (job: JobSnapshot) => boolean): boolean {
    for (const job of this.jobs.values()) {
      if ((job.status === 'queued' || job.status === 'running') && predicate(this.toSnapshot(job))) {
        return true;
      }
    }
    return false;
  }

  getCurrentJob(): JobSnapshot | null {
    if (!this.currentJobId) return null;
    const job = this.jobs.get(this.currentJobId);
    return job ? this.toSnapshot(job) : null;
  }

  getJob(jobId: string): JobSnapshot | null {
    const job = this.jobs.get(jobId);
    return job ? this.toSnapshot(job) : null;
  }

  getQueuedJobs(): JobSnapshot[] {
    return Array.from(this.jobs.values())
      .filter(job => job.status === 'queued' || job.status === 'running')
      .sort((a, b) => a.scheduledFor.getTime() - b.scheduledFor.getTime())
      .map(job => this.toSnapshot(job));
  }

  getRecentJobs(limit: number = 10): JobSnapshot[] {
    return this.jobOrder
      .slice()
      .reverse()
      .map(id => this.jobs.get(id))
      .filter((job): job is JobRecord => Boolean(job))
      .slice(0, limit)
      .map(job => this.toSnapshot(job));
  }

  getJobTimeline(historyLimit: number = 12): JobSnapshot[] {
    const queued = this.getQueuedJobs();
    const seen = new Set(queued.map(job => job.id));
    const history = this.getRecentJobs(historyLimit)
      .filter(job => !seen.has(job.id))
      .sort(
        (a, b) => this.getJobStartTimestamp(b) - this.getJobStartTimestamp(a)
      );

    return [...queued, ...history];
  }

  private getJobStartTimestamp(job: JobSnapshot): number {
    if (job.scheduledFor) {
      return job.scheduledFor.getTime();
    }
    if (job.startedAt) {
      return job.startedAt.getTime();
    }
    return job.createdAt.getTime();
  }

  async shutdown(): Promise<void> {
    if (this.wakeTimer) {
      clearTimeout(this.wakeTimer);
      this.wakeTimer = null;
    }
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }

    if (this.currentJobId) {
      this.cancelJob(this.currentJobId, 'Scheduler shutting down');
    }

    if (this.currentJobPromise) {
      await this.currentJobPromise;
    }
  }

  private recordActivity(resetIdleSince: boolean = true): void {
    if (resetIdleSince) {
      this.idleSince = null;
    }

    this.planIdleOptimizationCheck();
  }

  private planIdleOptimizationCheck(): void {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
    }

    if (!this.autoOptimizationEnabled) {
      this.idleTimer = null;
      return;
    }

    this.idleTimer = setTimeout(() => {
      if (!this.autoOptimizationEnabled) {
        return;
      }
      if (!this.currentJobId) {
        const maintenancePending = this.hasPendingJob(
          job => job.type === 'optimize'
        );
        if (!maintenancePending && !this.getRecentSuccessfulOptimize()) {
          this.loggingService.debug(
            JOB_SCHEDULER_SOURCE,
            'Idle period detected, scheduling optimize maintenance pass'
          );
          const metadata = { reason: 'idle', trigger: 'maintenance' };
          this.scheduleJob('optimize', {
            description: 'Optimize pass scheduled during idle period',
            metadata: { ...metadata, stage: 'optimize' }
          });
        }
      }
    }, this.idleDelayMs);
  }

  private wakeUp(): void {
    if (this.currentJobId) {
      return;
    }

    if (this.wakeTimer) {
      clearTimeout(this.wakeTimer);
      this.wakeTimer = null;
    }

    const now = Date.now();
    const nextJob = this.getNextReadyJob(now);
    if (nextJob) {
      this.startJob(nextJob);
      return;
    }

    const upcoming = this.getNextScheduledJob();
    if (upcoming) {
      const delay = Math.max(0, upcoming.scheduledFor.getTime() - now);
      this.wakeTimer = setTimeout(() => this.wakeUp(), Math.min(delay, 60_000));
    } else {
      this.idleSince = this.idleSince ?? new Date();
    }
  }

  private getNextReadyJob(now: number): JobRecord | null {
    const queuedJobs = Array.from(this.jobs.values())
      .filter(job => job.status === 'queued' && job.scheduledFor.getTime() <= now)
      .sort((a, b) => {
        const diff = a.scheduledFor.getTime() - b.scheduledFor.getTime();
        if (diff !== 0) return diff;
        return a.createdAt.getTime() - b.createdAt.getTime();
      });

    return queuedJobs[0] ?? null;
  }

  private getNextScheduledJob(): JobRecord | null {
    return Array.from(this.jobs.values())
      .filter(job => job.status === 'queued')
      .sort((a, b) => {
        const diff = a.scheduledFor.getTime() - b.scheduledFor.getTime();
        if (diff !== 0) return diff;
        return a.createdAt.getTime() - b.createdAt.getTime();
      })[0] ?? null;
  }

  private startJob(job: JobRecord): void {
    job.status = 'running';
    job.startedAt = new Date();
    job.attempts += 1;
    this.currentJobId = job.id;
    this.recordActivity();

    const handler = this.jobHandlers[job.type];
    const context: JobHandlerContext = {
      job: this.toSnapshot(job),
      abortSignal: job.abortController.signal,
      loggingService: this.loggingService,
      scheduler: this
    };

    this.loggingService.info(JOB_SCHEDULER_SOURCE, `Starting job ${job.type}`, {
      jobId: job.id,
      attempt: job.attempts,
      description: job.description
    });

    this.currentJobPromise = this.executeJob(job, handler, context);
  }

  private async executeJob(
    job: JobRecord,
    handler: JobHandler,
    context: JobHandlerContext
  ): Promise<void> {
    try {
      const result = (await handler(context)) ?? undefined;
      if (job.abortController.signal.aborted && job.cancelReason) {
        job.status = 'cancelled';
        job.finishedAt = new Date();
        job.lastError = job.cancelReason;
        this.loggingService.warn(JOB_SCHEDULER_SOURCE, `Job ${job.type} cancelled`, {
          jobId: job.id,
          reason: job.cancelReason
        });
        return;
      }

      job.status = 'succeeded';
      job.finishedAt = new Date();
      job.result = result;
      this.loggingService.info(JOB_SCHEDULER_SOURCE, `Job ${job.type} succeeded`, {
        jobId: job.id,
        durationMs: job.finishedAt.getTime() - (job.startedAt?.getTime() ?? job.finishedAt.getTime()),
        metadata: job.metadata
      });
    } catch (error) {
      if (job.abortController.signal.aborted && job.cancelReason) {
        job.status = 'cancelled';
        job.finishedAt = new Date();
        job.lastError = job.cancelReason;
        this.loggingService.warn(JOB_SCHEDULER_SOURCE, `Job ${job.type} cancelled`, {
          jobId: job.id,
          reason: job.cancelReason
        });
        return;
      }

      const message = error instanceof Error ? error.message : String(error);
      job.lastError = message;

      if (job.attempts < job.maxRetries) {
        const retryDelay = this.computeRetryDelay(job.attempts);
        job.status = 'queued';
        job.startedAt = undefined;
        job.finishedAt = undefined;
        job.scheduledFor = new Date(Date.now() + retryDelay);
        job.abortController = new AbortController();
        this.loggingService.warn(JOB_SCHEDULER_SOURCE, `Job ${job.type} failed (attempt ${job.attempts}), retrying`, {
          jobId: job.id,
          retryInMs: retryDelay,
          error: message
        });
        this.currentJobId = null;
        this.currentJobPromise = null;
        this.wakeUp();
        return;
      }

      job.status = 'failed';
      job.finishedAt = new Date();
      this.loggingService.error(JOB_SCHEDULER_SOURCE, `Job ${job.type} exhausted retries`, {
        jobId: job.id,
        attempts: job.attempts,
        error: message
      });
    } finally {
      if (job.status !== 'queued') {
        job.abortController = new AbortController();
      }
      if (job.status !== 'queued') {
        this.currentJobId = null;
        this.currentJobPromise = null;
        this.idleSince = new Date();
        this.wakeUp();
      }
    }
  }

  private preemptOptimizeJob(reason: string): void {
    if (!this.currentJobId) return;
    const current = this.jobs.get(this.currentJobId);
    if (current?.type === 'optimize') {
      this.cancelJob(current.id, reason);
    }
  }

  private computeRetryDelay(attempts: number): number {
    const delay = this.initialRetryDelayMs * Math.pow(2, Math.max(0, attempts - 1));
    return Math.min(delay, this.retryDelayCeilingMs);
  }

  private getRecentSuccessfulOptimize(): JobRecord | null {
    let latest: JobRecord | null = null;
    for (const job of this.jobs.values()) {
      if (job.type !== 'optimize' || job.status !== 'succeeded' || !job.finishedAt) {
        continue;
      }
      if (!latest || job.finishedAt.getTime() > (latest.finishedAt?.getTime() ?? 0)) {
        latest = job;
      }
    }

    if (!latest) {
      return null;
    }

    const ageMs = Date.now() - latest.finishedAt!.getTime();
    return ageMs <= OPTIMIZE_SCHEDULE_COOLDOWN_MS ? latest : null;
  }

  private toSnapshot(job: JobRecord): JobSnapshot {
    return {
      id: job.id,
      type: job.type,
      status: job.status,
      scheduledFor: job.scheduledFor,
      createdAt: job.createdAt,
      startedAt: job.startedAt,
      finishedAt: job.finishedAt,
      attempts: job.attempts,
      maxRetries: job.maxRetries,
      description: job.description,
      metadata: job.metadata,
      result: job.result,
      lastError: job.lastError,
      cancelReason: job.cancelReason,
      cancellationRequestedAt: job.cancellationRequestedAt
    };
  }
}
