import axios, { AxiosInstance } from 'axios';
import {
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  randomBytes,
  randomUUID,
  type JsonWebKey
} from 'crypto';
import fs from 'fs';
import { promises as fsPromises } from 'fs';
import os from 'os';
import path from 'path';
import * as tar from 'tar';
import { Client as SSHClient, ConnectConfig } from 'ssh2';
import { LoggingService, LogSource } from './LoggingService';
import { EmailService } from './EmailService';
import { Database } from '../database/Database';
import type { JobScheduler } from '../jobs/JobScheduler';
import type { MtlsLockdownService } from './MtlsLockdownService';
import { SETTING_KEYS } from '../constants';
import { resolveFromEmail, resolveSiteName } from '../utils/appUrl';
import type { RemoteOptimizerJobEntity } from '../database/types';
import type {
  RemoteOptimizationJobSnapshot,
  RemoteOptimizationRequest
} from '../../shared/types/RemoteOptimization';

const REMOTE_OPTIMIZER_SOURCE: LogSource = 'remote-optimize';
const RESEND_EMAIL_ENDPOINT = 'https://api.resend.com/emails';
const REMOTE_WORKSPACE_DIR = '/root/stratcraft';
const REMOTE_ENGINE_DIR = `${REMOTE_WORKSPACE_DIR}/engine`;
const REMOTE_DATA_DIR = `${REMOTE_WORKSPACE_DIR}/data`;
const MARKET_DATA_FILENAME = 'market-data.bin';
const REMOTE_MARKET_DATA_PATH = `${REMOTE_DATA_DIR}/${MARKET_DATA_FILENAME}`;
const REMOTE_SCRIPT_REMOTE_PATH = '/root/remote-optimize.sh';
const REMOTE_SCRIPT_LOG_PATH = '/root/remote-optimize.log';
const REMOTE_STATUS_FILE_PATH = '/root/remote-optimize-status.json';
const REMOTE_SCRIPT_PID_PATH = '/root/remote-optimize.pid';
const REMOTE_API_MTLS_DIR = '/root/.stratcraft-api-mtls';
const REMOTE_API_MTLS_CA_CERT_PATH = `${REMOTE_API_MTLS_DIR}/ca.crt`;
const REMOTE_API_MTLS_CLIENT_CERT_PATH = `${REMOTE_API_MTLS_DIR}/client.crt`;
const REMOTE_API_MTLS_CLIENT_KEY_PATH = `${REMOTE_API_MTLS_DIR}/client.key`;
const REMOTE_COMMAND_OUTPUT_LIMIT = 4000;
const REMOTE_JOB_LOG_LIMIT = 400;
const REMOTE_SCRIPT_LOG_TAIL_LINES = 400;
const REMOTE_SCRIPT_LOG_MAX_BYTES = 256_000;
const REMOTE_JOB_PERSIST_DEBOUNCE_MS = 1000;
const REMOTE_JOB_RECONCILE_INTERVAL_MS = 60_000;
const REMOTE_JOB_RUNNING_STALE_AFTER_MS = 15 * 60 * 1000;
const REMOTE_JOB_QUEUED_STALE_AFTER_MS = 5 * 60 * 1000;
const MARKET_DATA_WAIT_INTERVAL_MS = 2000;
const MARKET_DATA_WAIT_LOG_INTERVAL_MS = 30_000;

interface RemoteOptimizationJobRecord extends RemoteOptimizationJobSnapshot {
  logBuffer: string[];
  remoteHandoffComplete?: boolean;
  remoteSshPrivateKey?: Buffer;
  currentStage?: string;
  failureStage?: string;
  failureDetails?: string;
}

interface HetznerServer {
  id: number;
  name: string;
  status: string;
  public_net?: {
    ipv4?: {
      ip?: string;
    };
  };
}

interface HetznerServerCreateResponse {
  server: HetznerServer;
}

interface CommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

interface RemoteApiMtlsConfig {
  enabled: boolean;
  remoteCaCertPath: string;
  remoteClientCertPath: string;
  remoteClientKeyPath: string;
}

export class RemoteOptimizationService {
  private readonly loggingService: LoggingService;
  private readonly emailService: EmailService;
  private readonly db: Database;
  private readonly jobScheduler: JobScheduler;
  private readonly mtlsLockdownService: MtlsLockdownService;
  private hetznerToken: string | null;
  private readonly serverType: string;
  private readonly serverLocation: string;
  private readonly serverImage: string;
  private readonly repoRoot: string;
  private readonly httpClient: AxiosInstance;
  private readonly jobPersistTimers = new Map<string, NodeJS.Timeout>();
  private ensureSshKeysPromise: Promise<void> | null = null;
  private reconcilePromise: Promise<void> | null = null;
  private lastReconcileAttempt = 0;

  constructor(
    loggingService: LoggingService,
    emailService: EmailService,
    db: Database,
    jobScheduler: JobScheduler,
    mtlsLockdownService: MtlsLockdownService
  ) {
    this.loggingService = loggingService;
    this.emailService = emailService;
    this.db = db;
    this.jobScheduler = jobScheduler;
    this.mtlsLockdownService = mtlsLockdownService;
    this.hetznerToken = null;
    this.serverType = 'cpx62';
    this.serverLocation = 'hel1';
    this.serverImage = 'ubuntu-24.04';
    this.repoRoot = path.resolve(__dirname, '../../..');
    this.httpClient = axios.create({
      baseURL: 'https://api.hetzner.cloud/v1',
      timeout: 30_000
    });

    this.ensureStaleJobReconciliation(true);
  }

  async getJob(jobId: string): Promise<RemoteOptimizationJobSnapshot | undefined> {
    const entity = await this.db.remoteOptimizerJobs.getRemoteOptimizerJob(jobId);
    if (!entity) {
      return undefined;
    }
    return this.buildSnapshotFromEntity(entity);
  }

  async listJobs(): Promise<RemoteOptimizationJobSnapshot[]> {
    this.ensureStaleJobReconciliation();
    const entities = await this.db.remoteOptimizerJobs.getRemoteOptimizerJobs();
    return entities.map(entity => this.buildSnapshotFromEntity(entity));
  }

  async getRemoteOptimizerLog(jobId: string): Promise<{
    job: RemoteOptimizationJobSnapshot;
    log: string;
    tailLines: number;
  }> {
    const trimmedJobId = jobId.trim();
    if (!trimmedJobId) {
      throw new Error('Job ID is required to load remote optimizer logs.');
    }

    const entity = await this.db.remoteOptimizerJobs.getRemoteOptimizerJob(trimmedJobId);
    if (!entity) {
      throw new Error(`Remote optimization job ${trimmedJobId} was not found.`);
    }
    if (entity.status !== 'running' && entity.status !== 'handoff') {
      throw new Error(`Remote optimization job ${trimmedJobId} is ${entity.status}; logs are only available while running.`);
    }
    if (!entity.remoteServerIp) {
      throw new Error(`Remote optimization job ${trimmedJobId} does not have a server IP yet.`);
    }

    const privateKey = await this.requireHetznerPrivateKey();
    const job: RemoteOptimizationJobRecord = {
      id: entity.id,
      templateId: entity.templateId,
      templateName: entity.templateName,
      status: entity.status,
      createdAt: entity.createdAt,
      startedAt: entity.startedAt,
      finishedAt: entity.finishedAt,
      remoteServerIp: entity.remoteServerIp ?? undefined,
      hetznerServerId: entity.hetznerServerId ?? undefined,
      triggeredBy: {
        userId: 'unknown',
        email: 'unknown'
      },
      logBuffer: [],
      currentStage: 'fetching-remote-logs'
    };
    job.remoteSshPrivateKey = Buffer.from(privateKey, 'utf8');

    const normalizeLogTail = (value: string): string => {
      if (!value) {
        return '';
      }
      const normalized = value.replace(/\r\n/g, '\n');
      const lines = normalized.split('\n');
      if (lines.length <= REMOTE_SCRIPT_LOG_TAIL_LINES) {
        return normalized.trimEnd();
      }
      return lines.slice(-REMOTE_SCRIPT_LOG_TAIL_LINES).join('\n');
    };

    let log = '';
    let logHeader = '';
    try {
      const fileResult = await this.readRemoteFileTail(job, REMOTE_SCRIPT_LOG_PATH, REMOTE_SCRIPT_LOG_MAX_BYTES);
      if (fileResult) {
        const sizeLabel = `${fileResult.size} bytes`;
        const truncatedLabel = fileResult.truncated ? ', truncated' : '';
        logHeader = `Log file: ${REMOTE_SCRIPT_LOG_PATH} (${sizeLabel}${truncatedLabel})`;
        log = normalizeLogTail(fileResult.content);
        if (!log && fileResult.size > 0) {
          const fallback = await this.execRemoteCommand(
            job,
            `tail -n ${REMOTE_SCRIPT_LOG_TAIL_LINES} ${REMOTE_SCRIPT_LOG_PATH} || true`
          );
          log = normalizeLogTail([fallback.stdout, fallback.stderr].filter(Boolean).join('\n'));
        }
      } else {
        logHeader = `Log file not found at ${REMOTE_SCRIPT_LOG_PATH}.`;
      }
    } catch (error) {
      logHeader = 'Failed to read remote log file.';
      log = this.describeError(error);
    } finally {
      job.remoteSshPrivateKey = undefined;
    }

    if (logHeader) {
      if (log) {
        log = `${logHeader}\n${log}`;
      } else {
        log = `${logHeader}\nLog file is empty.`;
      }
    }

    return {
      job: this.buildSnapshotFromEntity(entity),
      log,
      tailLines: REMOTE_SCRIPT_LOG_TAIL_LINES
    };
  }

  async triggerOptimization(request: RemoteOptimizationRequest): Promise<RemoteOptimizationJobSnapshot> {
    await this.requireHetznerToken();
    await this.ensureHetznerSshKeys();

    const jobId = randomUUID();
    const job: RemoteOptimizationJobRecord = {
      id: jobId,
      templateId: request.templateId,
      templateName: request.templateName,
      status: 'queued',
      createdAt: new Date(),
      triggeredBy: request.triggeredBy,
      logBuffer: []
    };

    this.scheduleJobPersist(job);
    await this.persistJob(job);
    this.log(job, 'Queued remote optimization job');

    this.runJob(job).catch(error => {
      this.log(
        job,
        `Remote optimization job crashed: ${error instanceof Error ? error.message : String(error)}`,
        'error'
      );
    });

    const snapshot = await this.getJob(jobId);
    if (!snapshot) {
      throw new Error('Failed to load remote optimization job after creation');
    }
    return snapshot;
  }

  async stopOptimization(jobId: string): Promise<{ job: RemoteOptimizationJobSnapshot; serverDeleted: boolean }> {
    if (!jobId) {
      throw new Error('Job ID is required to stop remote optimization');
    }

    await this.requireHetznerToken();

    const entity = await this.db.remoteOptimizerJobs.getRemoteOptimizerJob(jobId);
    if (!entity) {
      throw new Error(`Remote optimization job ${jobId} was not found`);
    }

    if (!this.isPersistedJobActive(entity)) {
      throw new Error(`Remote optimization job ${jobId} is already ${entity.status} and cannot be stopped.`);
    }

    let serverDeleted = false;
    if (entity.hetznerServerId) {
      try {
        const deletionResult = await this.deleteHetznerServerById(entity.hetznerServerId);
        serverDeleted = deletionResult === 'deleted';
        const logPayload = {
          jobId,
          templateId: entity.templateId,
          hetznerServerId: entity.hetznerServerId
        };
        if (serverDeleted) {
          this.loggingService.info(
            REMOTE_OPTIMIZER_SOURCE,
            `Deleted Hetzner server ${entity.hetznerServerId} for job ${jobId}`,
            logPayload
          );
        } else {
          this.loggingService.warn(
            REMOTE_OPTIMIZER_SOURCE,
            `Hetzner server ${entity.hetznerServerId} was already missing when stop was requested`,
            logPayload
          );
        }
      } catch (error) {
        const detail = this.describeError(error);
        this.loggingService.error(
          REMOTE_OPTIMIZER_SOURCE,
          `Failed to delete Hetzner server ${entity.hetznerServerId} for job ${jobId}: ${detail}`,
          {
            jobId,
            templateId: entity.templateId,
            hetznerServerId: entity.hetznerServerId
          }
        );
        throw new Error(`Failed to delete Hetzner server ${entity.hetznerServerId}: ${detail}`);
      }
    } else {
      this.loggingService.warn(
        REMOTE_OPTIMIZER_SOURCE,
        `Stop requested for job ${jobId}, but no Hetzner server ID was recorded.`,
        {
          jobId,
          templateId: entity.templateId
        }
      );
    }

    const updatedEntity: RemoteOptimizerJobEntity = {
      ...entity,
      status: 'failed',
      finishedAt: new Date()
    };

    await this.db.remoteOptimizerJobs.upsertRemoteOptimizerJob(updatedEntity);

    this.loggingService.warn(REMOTE_OPTIMIZER_SOURCE, `Remote optimization job ${jobId} was manually stopped`, {
      jobId,
      templateId: entity.templateId,
      serverDeleted,
      hetznerServerId: entity.hetznerServerId ?? null
    });

    const snapshot = this.buildSnapshotFromEntity(updatedEntity);
    return { job: snapshot, serverDeleted };
  }

  private async runJob(job: RemoteOptimizationJobRecord): Promise<void> {
    job.status = 'running';
    job.startedAt = new Date();
    this.scheduleJobPersist(job);

    let serverInfo: HetznerServer | null = null;
    let archivePath: string | null = null;
    let remoteScriptPath: string | null = null;
    let remoteApiMtlsConfig: RemoteApiMtlsConfig = this.buildDisabledRemoteApiMtlsConfig();

    try {
      await this.waitForMarketDataJobs(job);
      this.enterStage(job, 'initializing', 'Starting remote optimization job');
      const hetznerPrivateKey = await this.requireHetznerPrivateKey();
      job.remoteSshPrivateKey = Buffer.from(hetznerPrivateKey, 'utf8');
      const marketDataPath = this.resolveMarketDataSnapshotPath();
      await this.ensureMarketDataSnapshot(job, marketDataPath);

      serverInfo = await this.createHetznerServer(job);
      job.hetznerServerId = serverInfo.id;
      job.remoteServerIp = await this.waitForServerPublicIp(job, serverInfo.id);
      this.scheduleJobPersist(job);

      await this.waitForSsh(job);

      archivePath = await this.createEngineArchive(job);
      this.enterStage(job, 'uploading-engine', 'Uploading engine archive to remote host');
      await this.uploadFile(job, archivePath, '/tmp/engine.tar.gz');
      await this.extractEngineArchive(job);

      this.enterStage(job, 'uploading-market-data', 'Uploading market data snapshot to remote host');
      await this.uploadMarketDataSnapshot(job, marketDataPath);

      remoteApiMtlsConfig = await this.prepareRemoteApiMtlsConfig(job);

      this.enterStage(job, 'generating-remote-script', 'Generating remote optimizer script');
      remoteScriptPath = await this.createRemoteScript(job, remoteApiMtlsConfig);
      this.enterStage(job, 'uploading-remote-script', 'Uploading remote optimizer script');
      await this.uploadFile(job, remoteScriptPath, REMOTE_SCRIPT_REMOTE_PATH);
      this.enterStage(job, 'configuring-remote-script', 'Setting executable permissions on remote optimizer script');
      await this.execRemoteCommand(job, `chmod +x ${REMOTE_SCRIPT_REMOTE_PATH}`);

      this.enterStage(job, 'launching-remote-script', 'Launching remote optimizer script');
      await this.launchRemoteOptimizeProcess(job);

      job.status = 'handoff';
      job.resultSummary = 'Remote optimizer running on Hetzner. Await completion email before considering final.';
      job.remoteHandoffComplete = true;
      this.enterStage(job, 'handoff-complete', 'Remote optimizer hand-off complete. Monitoring Hetzner server state.');
      this.scheduleJobPersist(job);
    } catch (error) {
      job.status = 'failed';
      job.finishedAt = new Date();
      job.failureStage = job.currentStage;
      job.failureDetails = this.describeError(error);
      job.error = this.buildFailureMessage(job, error);
      this.log(job, `Remote optimization failed: ${job.error}`, 'error', {
        stage: job.failureStage,
        failureDetails: job.failureDetails
      });
      this.scheduleJobPersist(job);
      await this.notifyFailure(job, job.error ?? 'Remote optimizer setup failed.', {
        stage: job.failureStage,
        logTail: this.getJobLogTail(job)
      });
    } finally {
      if (remoteScriptPath) {
        await this.safeUnlink(remoteScriptPath);
      }
      if (archivePath) {
        await this.safeUnlink(archivePath);
      }
      if (!job.remoteHandoffComplete && serverInfo) {
        await this.deleteServer(job, serverInfo.id);
      }
      job.remoteSshPrivateKey = undefined;
    }
  }

  private async waitForMarketDataJobs(job: RemoteOptimizationJobRecord): Promise<void> {
    const scheduler = this.jobScheduler;
    const hasPendingMarketDataJob = (): boolean => {
      const now = Date.now();
      return scheduler.hasPendingJob(candidate => {
        if (candidate.type !== 'export-market-data') {
          return false;
        }
        if (candidate.status === 'running') {
          return true;
        }
        return candidate.status === 'queued' && candidate.scheduledFor.getTime() <= now;
      });
    };

    if (!hasPendingMarketDataJob()) {
      return;
    }

    this.enterStage(job, 'waiting-market-data-job', 'Waiting for market data snapshot job to finish');
    let lastLogAt = Date.now();

    while (hasPendingMarketDataJob()) {
      await this.delay(MARKET_DATA_WAIT_INTERVAL_MS);
      const now = Date.now();
      if (now - lastLogAt >= MARKET_DATA_WAIT_LOG_INTERVAL_MS) {
        this.log(job, 'Market data snapshot job still running; waiting', 'info', { stage: job.currentStage });
        lastLogAt = now;
      }
    }

    this.log(job, 'Market data snapshot job finished; continuing remote optimization', 'info', {
      stage: job.currentStage
    });
  }

  private async createHetznerServer(job: RemoteOptimizationJobRecord): Promise<HetznerServer> {
    const name = this.buildServerName(job);
    this.enterStage(job, 'provisioning-server', `Provisioning Hetzner server ${name}`);
    const userData = this.buildCloudInitUserData();
    const sshKeyName = await this.db.settings.getSettingValue(SETTING_KEYS.HETZNER_SSH_KEY_NAME);
    try {
      const response = await this.httpClient.post<HetznerServerCreateResponse>('/servers', {
        name,
        server_type: this.serverType,
        image: this.serverImage,
        location: this.serverLocation,
        user_data: userData,
        ssh_keys: sshKeyName ? [sshKeyName] : undefined,
        labels: {
          stratcraft: 'remote-optimize',
          template: job.templateId
        }
      });
      const server = response.data.server;
      this.log(job, `Created server ${server.id} (${name})`, 'info', { stage: job.currentStage });
      return server;
    } catch (error) {
      throw new Error(`Failed to create Hetzner server ${name}: ${this.describeError(error)}`);
    }
  }

  private async deleteServer(job: RemoteOptimizationJobRecord, serverId: number): Promise<void> {
    try {
      const result = await this.deleteHetznerServerById(serverId);
      if (result === 'deleted') {
        this.log(job, `Deleted Hetzner server ${serverId}`);
      } else {
        this.log(job, `Hetzner server ${serverId} was already missing`, 'warn');
      }
    } catch (error) {
      this.log(
        job,
        `Failed to delete server ${serverId}: ${this.describeError(error)}`,
        'warn'
      );
    }
  }

  private async deleteHetznerServerById(serverId: number): Promise<'deleted' | 'missing'> {
    try {
      await this.httpClient.delete(`/servers/${serverId}`);
      return 'deleted';
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return 'missing';
      }
      throw error;
    }
  }

  private async waitForServerPublicIp(job: RemoteOptimizationJobRecord, serverId: number): Promise<string> {
    this.enterStage(job, 'waiting-for-server-ip', `Waiting for server ${serverId} to become ready`);
    const started = Date.now();
    while (Date.now() - started < 10 * 60 * 1000) {
      let server: HetznerServer;
      try {
        const response = await this.httpClient.get<{ server: HetznerServer }>(`/servers/${serverId}`);
        server = response.data.server;
      } catch (error) {
        throw new Error(`Failed to load Hetzner server ${serverId}: ${this.describeError(error)}`);
      }
      const ip = server.public_net?.ipv4?.ip;
      if (server.status === 'running' && ip) {
        this.log(job, `Server ${serverId} is running at ${ip}`, 'info', { stage: job.currentStage });
        return ip;
      }
      await this.delay(5000);
    }
    throw new Error(`Timed out waiting for server ${serverId} to become ready`);
  }

  private async waitForSsh(job: RemoteOptimizationJobRecord): Promise<void> {
    if (!job.remoteServerIp) {
      throw new Error('Remote server IP is unknown; cannot wait for SSH');
    }
    this.enterStage(job, 'waiting-for-ssh', `Waiting for SSH availability on ${job.remoteServerIp}`);
    const started = Date.now();
    while (Date.now() - started < 5 * 60 * 1000) {
      try {
        await this.execRemoteCommand(job, 'exit 0');
        this.log(job, `SSH is available on ${job.remoteServerIp}`, 'info', { stage: job.currentStage });
        return;
      } catch {
        await this.delay(8000);
      }
    }
    throw new Error('SSH did not become available on the remote server within the expected window');
  }

  private async createEngineArchive(job: RemoteOptimizationJobRecord): Promise<string> {
    const archivePath = path.join(os.tmpdir(), `engine-${job.id}.tar.gz`);
    this.enterStage(job, 'packaging-engine', `Creating engine archive at ${archivePath}`);

    const engineRoot = path.join(this.repoRoot, 'engine');
    try {
      await fsPromises.access(engineRoot, fs.constants.R_OK);
    } catch {
      throw new Error(`Engine directory missing at ${engineRoot}`);
    }
    const excludedPrefixes = ['engine/vendor', 'engine/target'];
    const excludedBasenameMatchers = [
      (basename: string): boolean => basename === 'nohup.out' || basename.startsWith('nohup.out.')
    ];
    const skippedEntries = new Set<string>();
    const normalize = (entryPath: string): string => entryPath.replace(/\\/g, '/');

    await tar.create(
      {
        gzip: true,
        file: archivePath,
        cwd: this.repoRoot,
        portable: true,
        noMtime: true,
        filter: entry => {
          const normalized = normalize(entry);
          if (excludedPrefixes.some(prefix => normalized === prefix || normalized.startsWith(`${prefix}/`))) {
            return false;
          }
          const basenameIndex = normalized.lastIndexOf('/');
          const basename = basenameIndex === -1 ? normalized : normalized.slice(basenameIndex + 1);
          if (excludedBasenameMatchers.some(matcher => matcher(basename))) {
            if (!skippedEntries.has(normalized)) {
              this.log(job, `Skipping ${normalized} when creating engine archive`, 'info');
              skippedEntries.add(normalized);
            }
            return false;
          }
          return true;
        }
      },
      ['engine']
    );

    return archivePath;
  }

  private resolveMarketDataSnapshotPath(): string {
    return path.join(this.repoRoot, 'data', MARKET_DATA_FILENAME);
  }

  private async ensureMarketDataSnapshot(job: RemoteOptimizationJobRecord, snapshotPath: string): Promise<void> {
    this.enterStage(job, 'checking-market-data', `Checking market data snapshot at ${snapshotPath}`);
    try {
      await fsPromises.access(snapshotPath, fs.constants.R_OK);
    } catch (error) {
      throw new Error(
        `Market data snapshot missing at ${snapshotPath}. Generate it with export-market-data before creating a remote optimizer.`
      );
    }
  }

  private async uploadMarketDataSnapshot(job: RemoteOptimizationJobRecord, snapshotPath: string): Promise<void> {
    await this.execRemoteCommand(job, `mkdir -p ${REMOTE_DATA_DIR}`);
    await this.uploadFile(job, snapshotPath, REMOTE_MARKET_DATA_PATH);
  }

  private async uploadFile(job: RemoteOptimizationJobRecord, localPath: string, remotePath: string): Promise<void> {
    this.log(job, `Uploading file to remote: ${localPath} -> ${remotePath}`, 'info', { stage: job.currentStage });
    const config = this.getSshConfig(job);
    await new Promise<void>((resolve, reject) => {
      const conn = new SSHClient();
      const fail = (err: unknown): void => {
        conn.end();
        reject(new Error(`Failed to upload ${localPath} to ${remotePath}: ${this.describeError(err)}`));
      };
      this.registerKeyboardInteractiveHandler(conn);
      conn.on('ready', () => {
        conn.sftp((err, sftp) => {
          if (err || !sftp) {
            fail(err ?? new Error('Failed to establish SFTP session'));
            return;
          }
          sftp.fastPut(localPath, remotePath, uploadErr => {
            conn.end();
            if (uploadErr) {
              fail(uploadErr);
            } else {
              resolve();
            }
          });
        });
      });
      conn.on('error', fail);
      conn.connect(config);
    });
  }

  private async readRemoteFileTail(
    job: RemoteOptimizationJobRecord,
    remotePath: string,
    maxBytes: number
  ): Promise<{ content: string; size: number; truncated: boolean } | null> {
    const config = this.getSshConfig(job);
    return new Promise((resolve, reject) => {
      const conn = new SSHClient();
      const fail = (err: unknown): void => {
        conn.end();
        reject(new Error(`Failed to read ${remotePath}: ${this.describeError(err)}`));
      };
      this.registerKeyboardInteractiveHandler(conn);
      conn.on('ready', () => {
        conn.sftp((err, sftp) => {
          if (err || !sftp) {
            fail(err ?? new Error('Failed to establish SFTP session'));
            return;
          }
          sftp.stat(remotePath, (statErr, stats) => {
            if (statErr || !stats) {
              conn.end();
              if (this.isRemoteFileMissing(statErr)) {
                resolve(null);
                return;
              }
              reject(new Error(`Failed to stat ${remotePath}: ${this.describeError(statErr)}`));
              return;
            }
            const size = Number(stats.size ?? 0);
            if (!Number.isFinite(size) || size <= 0) {
              conn.end();
              resolve({ content: '', size: 0, truncated: false });
              return;
            }
            const safeMaxBytes = Math.max(1, Math.trunc(maxBytes));
            const start = Math.max(0, size - safeMaxBytes);
            const length = size - start;
            sftp.open(remotePath, 'r', (openErr, handle) => {
              if (openErr || !handle) {
                conn.end();
                fail(openErr ?? new Error('Failed to open remote log file'));
                return;
              }
              const buffer = Buffer.alloc(length);
              sftp.read(handle, buffer, 0, length, start, (readErr, bytesRead) => {
                sftp.close(handle, () => {
                  conn.end();
                  if (readErr) {
                    reject(new Error(`Failed to read ${remotePath}: ${this.describeError(readErr)}`));
                    return;
                  }
                  const content = buffer.slice(0, bytesRead).toString('utf8');
                  resolve({ content, size, truncated: start > 0 });
                });
              });
            });
          });
        });
      });
      conn.on('error', fail);
      conn.connect(config);
    });
  }

  private async execRemoteCommand(job: RemoteOptimizationJobRecord, command: string): Promise<CommandResult> {
    this.log(job, `Executing remote command: ${command}`, 'info', { stage: job.currentStage });
    const config = this.getSshConfig(job);
    return new Promise<CommandResult>((resolve, reject) => {
      const conn = new SSHClient();
      const failConnection = (err: unknown): void => {
        conn.end();
        reject(new Error(`SSH connection error during command "${command}": ${this.describeError(err)}`));
      };
      this.registerKeyboardInteractiveHandler(conn);
      conn.on('ready', () => {
        conn.exec(`set -euo pipefail; ${command}`, (err, stream) => {
          if (err) {
            failConnection(err);
            return;
          }
          let stdout = '';
          let stderr = '';
          stream.on('close', (code: number | null) => {
            conn.end();
            const exitCode = code ?? 0;
            if (exitCode === 0) {
              resolve({ stdout, stderr, exitCode });
            } else {
              const formattedStdout = this.trimCommandOutput(stdout);
              const formattedStderr = this.trimCommandOutput(stderr);
              console.log('Remote command failed stdout:', formattedStdout);
              console.log('Remote command failed stderr:', formattedStderr);
              this.log(
                job,
                `Remote command failed with exit code ${exitCode}: ${command}`,
                'error',
                {
                  command,
                  exitCode,
                  stdout: formattedStdout,
                  stderr: formattedStderr
                }
              );
              const summaryParts = [`Remote command failed with exit code ${exitCode}`, `command="${command}"`];
              if (formattedStderr) {
                summaryParts.push(`stderr=${formattedStderr}`);
              }
              if (formattedStdout) {
                summaryParts.push(`stdout=${formattedStdout}`);
              }
              const error = new Error(summaryParts.join('; '));
              (error as any).stdout = stdout;
              (error as any).stderr = stderr;
              reject(error);
            }
          });
          stream.on('data', (chunk: Buffer) => {
            stdout += chunk.toString();
          });
          stream.stderr.on('data', (chunk: Buffer) => {
            stderr += chunk.toString();
          });
        });
      });
      conn.on('error', failConnection);
      conn.connect(config);
    });
  }

  private async execRemoteCommandDetached(job: RemoteOptimizationJobRecord, command: string): Promise<void> {
    const normalizedCommand = command
      .split('\n')
      .map(part => part.trim())
      .filter(Boolean)
      .join(' && ');
    const ackTokenBase = `__SC_REMOTE_OPT_ACK__${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
    const ackToken = ackTokenBase.replace(/[^A-Za-z0-9_]/g, '');
    const ackPattern = new RegExp(`${ackToken}(?:\\r?\\n)?`, 'g');
    const commandWithAck = `${command}
printf '%s\\n' '${ackToken}'
`;
    this.log(job, `Executing remote command: ${normalizedCommand}`, 'info', { stage: job.currentStage });
    const config = this.getSshConfig(job);
    return new Promise<void>((resolve, reject) => {
      const conn = new SSHClient();
      this.registerKeyboardInteractiveHandler(conn);
      let stdout = '';
      let stderr = '';
      let completed = false;
      let ackTimeout: NodeJS.Timeout | null = null;
      const stripAckToken = (value: string): string => {
        if (!value) {
          return value;
        }
        return value.replace(ackPattern, '');
      };
      const cleanup = (): void => {
        if (ackTimeout) {
          clearTimeout(ackTimeout);
          ackTimeout = null;
        }
        conn.end();
      };
      const succeed = (): void => {
        if (completed) {
          return;
        }
        completed = true;
        cleanup();
        resolve();
      };
      const fail = (message: string, exitCode?: number | null): void => {
        if (completed) {
          return;
        }
        completed = true;
        cleanup();
        const formattedStdout = this.trimCommandOutput(stripAckToken(stdout));
        const formattedStderr = this.trimCommandOutput(stderr);
        this.log(
          job,
          `Remote command failed${exitCode !== undefined ? ` with exit code ${exitCode}` : ''}: ${normalizedCommand}`,
          'error',
          {
            command: normalizedCommand,
            exitCode: exitCode ?? undefined,
            stdout: formattedStdout,
            stderr: formattedStderr
          }
        );
        const summaryParts = [message, `command="${normalizedCommand}"`];
        if (exitCode !== undefined && exitCode !== null) {
          summaryParts[0] = `${message} (exit code ${exitCode})`;
        }
        if (formattedStderr) {
          summaryParts.push(`stderr=${formattedStderr}`);
        }
        if (formattedStdout) {
          summaryParts.push(`stdout=${formattedStdout}`);
        }
        const error = new Error(summaryParts.join('; '));
        (error as any).stdout = stdout;
        (error as any).stderr = stderr;
        reject(error);
      };
      ackTimeout = setTimeout(() => {
        fail('Timed out waiting for remote command acknowledgement');
      }, 60_000);
      conn.on('ready', () => {
        conn.exec(`set -euo pipefail; ${commandWithAck}`, (err, stream) => {
          if (err) {
            fail(err instanceof Error ? err.message : String(err));
            return;
          }
          stream.on('close', (code: number | null) => {
            if (completed) {
              return;
            }
            if (stdout.includes(ackToken)) {
              stdout = stripAckToken(stdout);
              succeed();
            } else {
              fail('Remote command exited before acknowledgement', code);
            }
          });
          stream.on('data', (chunk: Buffer) => {
            if (completed) {
              return;
            }
            stdout += chunk.toString();
            if (stdout.includes(ackToken)) {
              stdout = stripAckToken(stdout);
              succeed();
            }
          });
          stream.stderr.on('data', (chunk: Buffer) => {
            if (completed) {
              return;
            }
            stderr += chunk.toString();
          });
        });
      });
      conn.on('error', err => {
        fail(`SSH connection error: ${this.describeError(err)}`);
      });
      conn.connect(config);
    });
  }

  private async extractEngineArchive(job: RemoteOptimizationJobRecord): Promise<void> {
    this.enterStage(job, 'extracting-engine', 'Extracting engine archive on remote host');
    const command = [
      `mkdir -p ${REMOTE_WORKSPACE_DIR}`,
      `rm -rf ${REMOTE_ENGINE_DIR}`,
      `tar -xzf /tmp/engine.tar.gz -C ${REMOTE_WORKSPACE_DIR}`,
      'rm -f /tmp/engine.tar.gz'
    ].join(' && ');
    await this.execRemoteCommand(job, command);
    this.log(job, 'Uploaded engine directory to remote host');
  }

  private async launchRemoteOptimizeProcess(job: RemoteOptimizationJobRecord): Promise<void> {
    const command = `
cd /root
touch ${REMOTE_SCRIPT_LOG_PATH}
chmod 600 ${REMOTE_SCRIPT_LOG_PATH} || true
printf '%s\\n' "[$(date -Iseconds)] Remote optimizer log initialized" >> ${REMOTE_SCRIPT_LOG_PATH}
nohup bash ${REMOTE_SCRIPT_REMOTE_PATH} >> ${REMOTE_SCRIPT_LOG_PATH} 2>&1 < /dev/null &
PID=$!
printf '%s\\n' "$PID" > ${REMOTE_SCRIPT_PID_PATH}
`.trim();
    await this.execRemoteCommandDetached(job, command);
    this.log(job, 'Remote optimize script launched via nohup', 'info', { stage: job.currentStage });
  }

  private getSshConfig(job: RemoteOptimizationJobRecord): ConnectConfig {
    if (!job.remoteServerIp) {
      throw new Error('Remote server connection details are unavailable');
    }
    if (!job.remoteSshPrivateKey) {
      throw new Error('Remote optimizer SSH key is unavailable');
    }
    return {
      host: job.remoteServerIp,
      username: 'root',
      privateKey: job.remoteSshPrivateKey,
      readyTimeout: 60_000,
      tryKeyboard: false
    };
  }

  private getRequesterEmail(job: RemoteOptimizationJobRecord): string | undefined {
    const email = job.triggeredBy?.email?.trim();
    return email && email.length > 0 ? email : undefined;
  }

  private buildDisabledRemoteApiMtlsConfig(): RemoteApiMtlsConfig {
    return {
      enabled: false,
      remoteCaCertPath: '',
      remoteClientCertPath: '',
      remoteClientKeyPath: ''
    };
  }

  private async prepareRemoteApiMtlsConfig(job: RemoteOptimizationJobRecord): Promise<RemoteApiMtlsConfig> {
    const mtlsService = this.mtlsLockdownService;

    let lockdownEnabled = false;
    try {
      const state = await mtlsService.getLockdownState();
      lockdownEnabled = state.lockdownEnabled;
    } catch (error) {
      this.log(
        job,
        `Unable to determine mTLS lockdown state for remote callbacks: ${this.describeError(error)}`,
        'warn'
      );
      return this.buildDisabledRemoteApiMtlsConfig();
    }

    if (!lockdownEnabled) {
      return this.buildDisabledRemoteApiMtlsConfig();
    }

    const localCaCertPath = mtlsService.caCertPath;
    const localClientCertPath = mtlsService.clientCertPath;
    const localClientKeyPath = mtlsService.clientKeyPath;

    await this.ensureReadableFile(localCaCertPath, 'mTLS CA certificate');
    await this.ensureReadableFile(localClientCertPath, 'mTLS client certificate');
    await this.ensureReadableFile(localClientKeyPath, 'mTLS client key');

    this.enterStage(job, 'uploading-mtls-certificates', 'Uploading mTLS client certificate bundle for API callbacks');
    await this.execRemoteCommand(job, `mkdir -p ${REMOTE_API_MTLS_DIR} && chmod 700 ${REMOTE_API_MTLS_DIR}`);
    await this.uploadFile(job, localCaCertPath, REMOTE_API_MTLS_CA_CERT_PATH);
    await this.uploadFile(job, localClientCertPath, REMOTE_API_MTLS_CLIENT_CERT_PATH);
    await this.uploadFile(job, localClientKeyPath, REMOTE_API_MTLS_CLIENT_KEY_PATH);
    await this.execRemoteCommand(
      job,
      `chmod 600 ${REMOTE_API_MTLS_CA_CERT_PATH} ${REMOTE_API_MTLS_CLIENT_CERT_PATH} ${REMOTE_API_MTLS_CLIENT_KEY_PATH}`
    );
    this.log(job, 'Uploaded mTLS client certificate bundle for remote API callbacks', 'info', {
      stage: job.currentStage
    });

    return {
      enabled: true,
      remoteCaCertPath: REMOTE_API_MTLS_CA_CERT_PATH,
      remoteClientCertPath: REMOTE_API_MTLS_CLIENT_CERT_PATH,
      remoteClientKeyPath: REMOTE_API_MTLS_CLIENT_KEY_PATH
    };
  }

  private async ensureReadableFile(filePath: string, description: string): Promise<void> {
    try {
      await fsPromises.access(filePath, fs.constants.R_OK);
    } catch {
      throw new Error(`${description} is required at ${filePath} but was not found or is unreadable.`);
    }
  }

  private async createRemoteScript(
    job: RemoteOptimizationJobRecord,
    remoteApiMtlsConfig: RemoteApiMtlsConfig
  ): Promise<string> {
    const scriptPath = path.join(os.tmpdir(), `remote-optimize-${job.id}.sh`);
    const templateName = job.templateName || job.templateId;
    const templateId = job.templateId;
    const jobId = job.id;
    const emailTo = this.getRequesterEmail(job) ?? '';
    const rawResendKey = await this.db.settings.getSettingValue(SETTING_KEYS.RESEND_API_KEY);
    const resendKey = rawResendKey?.trim() ?? '';
    const hetznerToken = this.hetznerToken ?? '';
    const hetznerServerId = job.hetznerServerId ? String(job.hetznerServerId) : '';
    const resolvedFrom = await resolveFromEmail(this.db);
    const emailFrom = resolvedFrom
      ? (resolvedFrom.includes('<') ? resolvedFrom : `Remote Optimizer <${resolvedFrom}>`)
      : '';
    const siteName = await resolveSiteName(this.db);
    const apiMtlsCaCertPath = remoteApiMtlsConfig.enabled ? remoteApiMtlsConfig.remoteCaCertPath : '';
    const apiMtlsClientCertPath = remoteApiMtlsConfig.enabled ? remoteApiMtlsConfig.remoteClientCertPath : '';
    const apiMtlsClientKeyPath = remoteApiMtlsConfig.enabled ? remoteApiMtlsConfig.remoteClientKeyPath : '';
    const script = `#!/usr/bin/env bash
set -Eeuo pipefail

SITE_NAME="${this.escapeShell(siteName)}"
TEMPLATE_ID="${this.escapeShell(templateId)}"
TEMPLATE_NAME="${this.escapeShell(templateName)}"
JOB_ID="${this.escapeShell(jobId)}"
EMAIL_TO="${this.escapeShell(emailTo)}"
EMAIL_FROM="${this.escapeShell(emailFrom)}"
RESEND_API_KEY="${this.escapeShell(resendKey)}"
HETZNER_TOKEN="${this.escapeShell(hetznerToken)}"
HETZNER_SERVER_ID="${this.escapeShell(hetznerServerId)}"
ENGINE_DIR="${this.escapeShell(REMOTE_ENGINE_DIR)}"
DATA_DIR="${this.escapeShell(REMOTE_DATA_DIR)}"
LOG_FILE="${this.escapeShell(REMOTE_SCRIPT_LOG_PATH)}"
STATUS_FILE="${this.escapeShell(REMOTE_STATUS_FILE_PATH)}"
BACKTEST_API_MTLS_CA_CERT="${this.escapeShell(apiMtlsCaCertPath)}"
BACKTEST_API_MTLS_CLIENT_CERT="${this.escapeShell(apiMtlsClientCertPath)}"
BACKTEST_API_MTLS_CLIENT_KEY="${this.escapeShell(apiMtlsClientKeyPath)}"

touch "$LOG_FILE"
chmod 600 "$LOG_FILE"
rm -f "$STATUS_FILE"

write_status() {
  local status="$1"
  local message="$2"
  local completion="$3"
  local timestamp="$(date -Iseconds)"
  if [ -n "$completion" ]; then
    cat <<JSON > "$STATUS_FILE"
{
  "status": "$status",
  "message": "$message",
  "updatedAt": "$timestamp",
  "completedAt": "$completion"
}
JSON
  else
    cat <<JSON > "$STATUS_FILE"
{
  "status": "$status",
  "message": "$message",
  "updatedAt": "$timestamp"
}
JSON
  fi
}

send_email() {
  local status="$1"
  local message="$2"
  local completion="$3"

  if [ -z "$RESEND_API_KEY" ] || [ -z "$EMAIL_TO" ] || [ -z "$EMAIL_FROM" ]; then
    echo "Skipping email notification; missing configuration."
    return 0
  fi

  if ! SITE_NAME="$SITE_NAME" STATUS="$status" MESSAGE="$message" COMPLETED_AT="$completion" TEMPLATE_ID="$TEMPLATE_ID" TEMPLATE_NAME="$TEMPLATE_NAME" JOB_ID="$JOB_ID" LOG_FILE="$LOG_FILE" EMAIL_TO="$EMAIL_TO" EMAIL_FROM="$EMAIL_FROM" RESEND_API_KEY="$RESEND_API_KEY" python3 <<'PY'
import html
import json
import os
import sys
import urllib.error
import urllib.request

site_name = os.environ.get('SITE_NAME', 'StratCraft')
status = os.environ.get('STATUS', 'unknown')
message = os.environ.get('MESSAGE', '')
completed_at = os.environ.get('COMPLETED_AT', '')
template_name = os.environ.get('TEMPLATE_NAME', '')
template_id = os.environ.get('TEMPLATE_ID', '')
job_id = os.environ.get('JOB_ID', '')
log_path = os.environ.get('LOG_FILE')
email_to = os.environ.get('EMAIL_TO')
email_from = os.environ.get('EMAIL_FROM', '')
api_key = os.environ.get('RESEND_API_KEY')

snippet = ''
if log_path:
    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as handle:
            lines = handle.readlines()[-400:]
            snippet = ''.join(lines)
    except Exception as exc:
        snippet = f'Unable to read remote log: {exc}'

if not api_key or not email_to or not email_from:
    sys.exit(0)

subject = f"[{site_name}] Remote optimize {status} - {template_name or template_id}"
body = f"""
<p><strong>Template:</strong> {html.escape(template_name or template_id)}</p>
<p><strong>Template ID:</strong> {html.escape(template_id)}</p>
<p><strong>Job ID:</strong> {html.escape(job_id)}</p>
<p><strong>Status:</strong> {html.escape(status)}</p>
<p><strong>Message:</strong> {html.escape(message)}</p>
<p><strong>Completed:</strong> {html.escape(completed_at or 'n/a')}</p>
<pre style="background:#0f111a;color:#f5f6fa;padding:12px;border-radius:6px;white-space:pre-wrap;">{html.escape(snippet)}</pre>
"""

payload = {
    "from": email_from,
    "to": [email_to],
    "subject": subject,
    "html": body
}

req = urllib.request.Request(
    "${RESEND_EMAIL_ENDPOINT}",
    data=json.dumps(payload).encode('utf-8'),
    headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    },
    method="POST"
)

def read_response_body(resp):
    try:
        body = resp.read()
    except Exception:
        return ''
    if body is None:
        return ''
    if isinstance(body, bytes):
        return body.decode('utf-8', errors='ignore')
    return str(body)

try:
    with urllib.request.urlopen(req, timeout=30) as response:
        status_code = response.getcode() or 0
        response_body = read_response_body(response)
    if status_code < 200 or status_code >= 300:
        snippet = response_body[:2000] if response_body else ''
        detail = f" status={status_code}"
        if snippet:
            detail += f" body={snippet}"
        print(f"Failed to send completion email:{detail}", file=sys.stderr)
        sys.exit(1)
except urllib.error.HTTPError as exc:
    body = read_response_body(exc)
    snippet = body[:2000] if body else ''
    detail = f" HTTP {exc.code}"
    if exc.reason:
        detail += f" {exc.reason}"
    if snippet:
        detail += f" body={snippet}"
    print(f"Failed to send completion email:{detail}", file=sys.stderr)
    sys.exit(1)
except Exception as exc:
    print(f"Failed to send completion email: {exc}", file=sys.stderr)
    sys.exit(1)
PY
  then
    return 1
  fi
}

delete_server() {
  if [ -z "$HETZNER_TOKEN" ] || [ -z "$HETZNER_SERVER_ID" ]; then
    echo "Hetzner credentials missing; skipping self-deletion."
    return
  fi
  curl -s -X DELETE "https://api.hetzner.cloud/v1/servers/$HETZNER_SERVER_ID" \\
    -H "Authorization: Bearer $HETZNER_TOKEN" \\
    -H "Content-Type: application/json" >/dev/null 2>&1 || true
}

finalize() {
  local status="$1"
  local message="$2"
  local completion="$3"
  write_status "$status" "$message" "$completion"
  local email_failed=0
  if ! send_email "$status" "$message" "$completion"; then
    email_failed=1
  fi
  if [ "$status" = "success" ]; then
    if [ "$email_failed" -eq 1 ]; then
      echo "Email notification failed; proceeding with server deletion."
    fi
    delete_server
    return 0
  fi
  if [ "$email_failed" -eq 1 ]; then
    echo "Email notification failed; skipping server deletion for inspection."
    return 0
  fi
  delete_server
}

CURRENT_STAGE="initializing"

set_stage() {
  CURRENT_STAGE="$1"
  write_status "running" "Stage: $CURRENT_STAGE" ""
}

handle_error() {
  local exit_code=$?
  finalize "failed" "Stage $CURRENT_STAGE failed (exit code $exit_code)." "$(date -Iseconds)"
  exit "$exit_code"
}

trap handle_error ERR

set_stage "initializing"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "Starting remote optimization for $TEMPLATE_ID at $(date -Iseconds)"

export DEBIAN_FRONTEND=noninteractive
set_stage "installing-packages"
apt-get update
apt-get install -y build-essential pkg-config libssl-dev clang curl git unzip ca-certificates python3 python3-pip fail2ban
systemctl enable --now fail2ban || true

if [ ! -s "$HOME/.cargo/env" ]; then
  set_stage "installing-rust"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env"

mkdir -p "$DATA_DIR"
cd "$ENGINE_DIR"

set_stage "fetching-dependencies"
cargo fetch

set_stage "building-engine"
cargo build --release

set_stage "optimizing-template"
export BACKTEST_API_MTLS_CA_CERT
export BACKTEST_API_MTLS_CLIENT_CERT
export BACKTEST_API_MTLS_CLIENT_KEY
./target/release/engine optimize "$TEMPLATE_ID" --data-file "$DATA_DIR/market-data.bin"

finalize "success" "Remote optimization completed successfully." "$(date -Iseconds)"
exit 0
`;
    await fsPromises.writeFile(scriptPath, script, { mode: 0o755 });
    return scriptPath;
  }

  private async notifyFailure(
    job: RemoteOptimizationJobRecord,
    details: string,
    options: { stage?: string; logTail?: string } = {}
  ): Promise<void> {
    const notificationEmail = this.getRequesterEmail(job);
    if (!notificationEmail) {
      return;
    }
    try {
      const stageInfo = options.stage ? this.escapeHtml(options.stage) : 'n/a';
      const logTail = options.logTail ? this.escapeHtml(options.logTail) : '';
      const logSection = logTail
        ? `<pre style="background:#0f111a;color:#f5f6fa;padding:12px;border-radius:6px;white-space:pre-wrap;">${logTail}</pre>`
        : '<p><em>No recent logs available.</em></p>';
      await this.emailService.sendAdhocEmail({
        to: notificationEmail,
        subject: `Remote optimize failed to launch (${job.templateName})`,
        html: `
          <p>Remote optimizer setup for template <strong>${this.escapeHtml(job.templateName)}</strong> (${this.escapeHtml(job.templateId)}) failed before hand-off.</p>
          <p><strong>Job ID:</strong> ${this.escapeHtml(job.id)}</p>
          <p><strong>Stage:</strong> ${stageInfo}</p>
          <p><strong>Error:</strong> ${this.escapeHtml(details)}</p>
          <p><strong>Recent logs:</strong></p>
          ${logSection}
        `
      });
    } catch (error) {
      this.log(
        job,
        `Failed to send remote optimization setup failure email: ${error instanceof Error ? error.message : String(error)}`,
        'warn'
      );
    }
  }

  private async safeUnlink(targetPath: string): Promise<void> {
    try {
      await fsPromises.unlink(targetPath);
    } catch {
      // ignore
    }
  }

  async ensureHetznerSshKeys(): Promise<void> {
    if (this.ensureSshKeysPromise) {
      return this.ensureSshKeysPromise;
    }
    this.ensureSshKeysPromise = this.ensureHetznerSshKeysInternal()
      .finally(() => {
        this.ensureSshKeysPromise = null;
      });
    return this.ensureSshKeysPromise;
  }

  private async ensureHetznerSshKeysInternal(): Promise<void> {
    const [rawPrivateKey, rawPublicKey] = await Promise.all([
      this.db.settings.getSettingValue(SETTING_KEYS.HETZNER_PRIVATE_KEY),
      this.db.settings.getSettingValue(SETTING_KEYS.HETZNER_PUBLIC_KEY)
    ]);
    const privateKey = typeof rawPrivateKey === 'string' ? rawPrivateKey.trim() : '';
    const publicKey = typeof rawPublicKey === 'string' ? rawPublicKey.trim() : '';
    const hasPrivateKey = privateKey.length > 0;
    const hasPublicKey = publicKey.length > 0;

    if (hasPrivateKey && hasPublicKey) {
      return;
    }

    if (hasPrivateKey && !hasPublicKey) {
      const derived = this.tryDeriveOpenSshPublicKey(privateKey);
      if (derived) {
        await this.db.settings.upsertSettings({
          [SETTING_KEYS.HETZNER_PUBLIC_KEY]: derived
        });
        this.loggingService.info(
          REMOTE_OPTIMIZER_SOURCE,
          'Derived Hetzner SSH public key from configured private key.'
        );
      } else {
        this.loggingService.warn(
          REMOTE_OPTIMIZER_SOURCE,
          'Hetzner SSH public key is missing and could not be derived from the private key.'
        );
      }
      return;
    }

    if (!hasPrivateKey && hasPublicKey) {
      this.loggingService.warn(
        REMOTE_OPTIMIZER_SOURCE,
        'Hetzner SSH private key is missing; remote optimization will fail until it is configured.'
      );
      return;
    }

    const { privateKey: generatedPrivateKey, publicKey: generatedPublicKey } = this.generateSshKeyPair();
    await this.db.settings.upsertSettings({
      [SETTING_KEYS.HETZNER_PRIVATE_KEY]: generatedPrivateKey,
      [SETTING_KEYS.HETZNER_PUBLIC_KEY]: generatedPublicKey
    });
    this.loggingService.info(
      REMOTE_OPTIMIZER_SOURCE,
      'Generated Hetzner SSH keypair for remote optimization.'
    );
  }

  private async requireHetznerPrivateKey(): Promise<string> {
    const rawValue = await this.db.settings.getSettingValue(SETTING_KEYS.HETZNER_PRIVATE_KEY);
    const privateKey = typeof rawValue === 'string' ? rawValue.trim() : '';
    if (privateKey.length === 0) {
      throw new Error(
        'Hetzner SSH private key is not configured. Set HETZNER_PRIVATE_KEY in Settings to enable remote optimization.'
      );
    }
    return privateKey;
  }

  private generateSshKeyPair(): { privateKey: string; publicKey: string } {
    const { publicKey, privateKey } = generateKeyPairSync('ed25519');
    const publicJwk = publicKey.export({ format: 'jwk' }) as JsonWebKey;
    const privateJwk = privateKey.export({ format: 'jwk' }) as JsonWebKey;

    const openSshPublicKey = this.formatOpenSshPublicKey(publicJwk);
    const openSshPrivateKey = this.formatOpenSshPrivateKey(privateJwk);
    return { privateKey: openSshPrivateKey, publicKey: openSshPublicKey };
  }

  private tryDeriveOpenSshPublicKey(privateKey: string): string | null {
    try {
      const keyObj = createPrivateKey(privateKey);
      const publicKeyObj = createPublicKey(keyObj);
      const jwk = publicKeyObj.export({ format: 'jwk' }) as JsonWebKey;
      return this.formatOpenSshPublicKey(jwk);
    } catch (error) {
      this.loggingService.warn(
        REMOTE_OPTIMIZER_SOURCE,
        `Failed to derive Hetzner public key: ${this.describeError(error)}`
      );
      return null;
    }
  }

  private formatOpenSshPublicKey(jwk: JsonWebKey): string {
    if (jwk.kty === 'OKP' && jwk.crv === 'Ed25519' && jwk.x) {
      const keyType = 'ssh-ed25519';
      const keyData = Buffer.from(jwk.x, 'base64url');
      const payload = Buffer.concat([this.encodeSshString(keyType), this.encodeSshBuffer(keyData)]);
      return `${keyType} ${payload.toString('base64')}`;
    }

    if (jwk.kty === 'RSA' && jwk.e && jwk.n) {
      const keyType = 'ssh-rsa';
      const exponent = Buffer.from(jwk.e, 'base64url');
      const modulus = Buffer.from(jwk.n, 'base64url');
      const payload = Buffer.concat([
        this.encodeSshString(keyType),
        this.encodeSshMpint(exponent),
        this.encodeSshMpint(modulus)
      ]);
      return `${keyType} ${payload.toString('base64')}`;
    }

    throw new Error('Unsupported SSH key format for Hetzner public key generation.');
  }

  private formatOpenSshPrivateKey(jwk: JsonWebKey): string {
    if (jwk.kty !== 'OKP' || jwk.crv !== 'Ed25519' || !jwk.d || !jwk.x) {
      throw new Error('Unsupported SSH key format for Hetzner private key generation.');
    }

    const keyType = 'ssh-ed25519';
    const publicKey = Buffer.from(jwk.x, 'base64url');
    const privateSeed = Buffer.from(jwk.d, 'base64url');
    const privateKey = Buffer.concat([privateSeed, publicKey]);
    const checkInt = randomBytes(4);
    const checkValue = checkInt.readUInt32BE(0);

    const publicKeyPayload = Buffer.concat([
      this.encodeSshString(keyType),
      this.encodeSshBuffer(publicKey)
    ]);
    const publicKeyEntry = this.encodeSshBuffer(publicKeyPayload);

    const privateKeyPayload = Buffer.concat([
      this.encodeSshUInt32(checkValue),
      this.encodeSshUInt32(checkValue),
      this.encodeSshString(keyType),
      this.encodeSshBuffer(publicKey),
      this.encodeSshBuffer(privateKey),
      this.encodeSshString('')
    ]);

    const paddedPrivateKey = this.addOpenSshPadding(privateKeyPayload);
    const opensshKey = Buffer.concat([
      Buffer.from('openssh-key-v1\0', 'utf8'),
      this.encodeSshString('none'),
      this.encodeSshString('none'),
      this.encodeSshString(''),
      this.encodeSshUInt32(1),
      publicKeyEntry,
      this.encodeSshBuffer(paddedPrivateKey)
    ]);

    return this.wrapOpenSshKey(opensshKey);
  }

  private addOpenSshPadding(payload: Buffer): Buffer {
    const blockSize = 8;
    let paddingLength = blockSize - (payload.length % blockSize);
    if (paddingLength === 0) {
      paddingLength = blockSize;
    }
    const padding = Buffer.alloc(paddingLength);
    for (let i = 0; i < paddingLength; i += 1) {
      padding[i] = i + 1;
    }
    return Buffer.concat([payload, padding]);
  }

  private encodeSshString(value: string): Buffer {
    const raw = Buffer.from(value, 'utf8');
    return this.encodeSshBuffer(raw);
  }

  private encodeSshUInt32(value: number): Buffer {
    const buffer = Buffer.alloc(4);
    buffer.writeUInt32BE(value >>> 0, 0);
    return buffer;
  }

  private encodeSshBuffer(value: Buffer): Buffer {
    const length = Buffer.alloc(4);
    length.writeUInt32BE(value.length, 0);
    return Buffer.concat([length, value]);
  }

  private encodeSshMpint(value: Buffer): Buffer {
    if (value.length === 0) {
      return this.encodeSshBuffer(value);
    }
    let normalized = value;
    if (normalized[0] & 0x80) {
      normalized = Buffer.concat([Buffer.from([0]), normalized]);
    }
    return this.encodeSshBuffer(normalized);
  }

  private wrapOpenSshKey(payload: Buffer): string {
    const base64 = payload.toString('base64');
    const lines = base64.match(/.{1,70}/g) ?? [base64];
    return `-----BEGIN OPENSSH PRIVATE KEY-----\n${lines.join('\n')}\n-----END OPENSSH PRIVATE KEY-----`;
  }

  private async refreshHetznerToken(): Promise<string | null> {
    const rawToken = await this.db.settings.getSettingValue(SETTING_KEYS.HETZNER_API_TOKEN);
    const token = typeof rawToken === 'string' ? rawToken.trim() : '';
    this.hetznerToken = token.length > 0 ? token : null;

    const headers = this.httpClient.defaults.headers.common as Record<string, string>;
    if (this.hetznerToken) {
      headers.Authorization = `Bearer ${this.hetznerToken}`;
    } else {
      delete headers.Authorization;
    }

    return this.hetznerToken;
  }

  private async requireHetznerToken(): Promise<string> {
    const token = await this.refreshHetznerToken();
    if (!token) {
      throw new Error(
        'Hetzner API token is not configured. Set HETZNER_API_TOKEN in Settings to enable remote optimization.'
      );
    }
    return token;
  }

  private ensureStaleJobReconciliation(force = false): void {
    if (this.reconcilePromise) {
      return;
    }
    const now = Date.now();
    if (!force && now - this.lastReconcileAttempt < REMOTE_JOB_RECONCILE_INTERVAL_MS) {
      return;
    }
    this.lastReconcileAttempt = now;
    this.reconcilePromise = this.reconcileStaleJobs()
      .catch(error => {
        this.loggingService.warn(
          REMOTE_OPTIMIZER_SOURCE,
          `Failed to reconcile remote optimizer jobs: ${this.describeError(error)}`
        );
      })
      .finally(() => {
        this.reconcilePromise = null;
      });
  }

  private async reconcileStaleJobs(): Promise<void> {
    await this.refreshHetznerToken();
    const jobs = await this.db.remoteOptimizerJobs.getRemoteOptimizerJobs();
    for (const job of jobs) {
      if (job.status === 'handoff') {
        await this.reconcileHandoffJob(job);
        continue;
      }
      if (!this.isPersistedJobActive(job)) {
        continue;
      }
      const reason = await this.computePersistedStaleReason(job);
      if (reason) {
        await this.markPersistedJobAsFailed(job, reason);
      }
    }
  }

  private isPersistedJobActive(job: RemoteOptimizerJobEntity): boolean {
    return job.status === 'queued' || job.status === 'running' || job.status === 'handoff';
  }

  private async computePersistedStaleReason(job: RemoteOptimizerJobEntity): Promise<string | null> {
    const now = Date.now();
    if (job.status === 'handoff') {
      return null;
    }
    if (job.status === 'queued') {
      const age = now - job.createdAt.getTime();
      if (age > REMOTE_JOB_QUEUED_STALE_AFTER_MS) {
        return 'Remote optimizer job never started after service restart.';
      }
      return null;
    }
    if (job.status !== 'running') {
      return null;
    }
    if (job.hetznerServerId) {
      const serverState = await this.lookupHetznerServerState(job.hetznerServerId);
      if (serverState === 'missing') {
        return 'Hetzner server no longer exists.';
      }
      if (serverState === 'unresponsive') {
        return 'Hetzner server stopped responding.';
      }
      if (serverState === 'unknown') {
        return null;
      }
    }
    const startedAt = job.startedAt?.getTime() ?? job.createdAt.getTime();
    if (now - startedAt > REMOTE_JOB_RUNNING_STALE_AFTER_MS) {
      return 'Remote optimizer job stalled and has been marked as failed.';
    }
    return null;
  }

  private async reconcileHandoffJob(job: RemoteOptimizerJobEntity): Promise<void> {
    if (!job.hetznerServerId) {
      await this.markPersistedJobAsFailed(job, 'Hetzner server ID missing after remote hand-off.');
      return;
    }
    const serverState = await this.lookupHetznerServerState(job.hetznerServerId);
    if (serverState === 'running' || serverState === 'unknown') {
      return;
    }
    if (serverState === 'missing') {
      await this.markPersistedJobAsSucceeded(job, 'Hetzner server reported missing after remote completion.');
      return;
    }
    if (serverState === 'unresponsive') {
      this.loggingService.warn(
        REMOTE_OPTIMIZER_SOURCE,
        `Hetzner server ${job.hetznerServerId} is unresponsive for remote job ${job.id}`,
        {
          jobId: job.id,
          templateId: job.templateId,
          hetznerServerId: job.hetznerServerId
        }
      );
    }
  }

  private async markPersistedJobAsFailed(
    job: RemoteOptimizerJobEntity,
    reason: string
  ): Promise<void> {
    const updated: RemoteOptimizerJobEntity = {
      ...job,
      status: 'failed',
      finishedAt: new Date()
    };
    try {
      await this.db.remoteOptimizerJobs.upsertRemoteOptimizerJob(updated);
    } catch (error) {
      this.loggingService.warn(
        REMOTE_OPTIMIZER_SOURCE,
        `Failed to update stale remote optimizer job ${job.id}: ${this.describeError(error)}`
      );
    }
    this.loggingService.warn(REMOTE_OPTIMIZER_SOURCE, `Auto-failed remote optimizer job ${job.id}: ${reason}`, {
      jobId: job.id,
      templateId: job.templateId,
      autoFailed: true
    });
  }

  private async markPersistedJobAsSucceeded(
    job: RemoteOptimizerJobEntity,
    reason: string
  ): Promise<void> {
    const updated: RemoteOptimizerJobEntity = {
      ...job,
      status: 'succeeded',
      finishedAt: new Date(),
      hetznerServerId: null,
      remoteServerIp: null
    };
    try {
      await this.db.remoteOptimizerJobs.upsertRemoteOptimizerJob(updated);
    } catch (error) {
      this.loggingService.warn(
        REMOTE_OPTIMIZER_SOURCE,
        `Failed to update completed remote optimizer job ${job.id}: ${this.describeError(error)}`
      );
      return;
    }
    this.loggingService.info(
      REMOTE_OPTIMIZER_SOURCE,
      `Marked remote optimizer job ${job.id} as succeeded: ${reason}`,
      {
        jobId: job.id,
        templateId: job.templateId,
        autoResolved: true
      }
    );
  }

  private async lookupHetznerServerState(
    serverId: number
  ): Promise<'running' | 'missing' | 'unresponsive' | 'unknown'> {
    try {
      const response = await this.httpClient.get<{ server: HetznerServer }>(`/servers/${serverId}`);
      const server = response.data?.server;
      if (!server) {
        return 'missing';
      }
      const ip = server.public_net?.ipv4?.ip;
      if (server.status === 'running' && ip) {
        return 'running';
      }
      return 'unresponsive';
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const status = error.response?.status;
        if (status === 404) {
          return 'missing';
        }
        this.loggingService.warn(
          REMOTE_OPTIMIZER_SOURCE,
          `Failed to check Hetzner server ${serverId}: ${this.describeAxiosError(error)}`
        );
        return 'unknown';
      }
      this.loggingService.warn(
        REMOTE_OPTIMIZER_SOURCE,
        `Failed to check Hetzner server ${serverId}: ${this.describeError(error)}`
      );
      return 'unknown';
    }
  }

  private scheduleJobPersist(job: RemoteOptimizationJobRecord): void {
    const existing = this.jobPersistTimers.get(job.id);
    if (existing) {
      clearTimeout(existing);
    }
    const timer = setTimeout(() => {
      this.jobPersistTimers.delete(job.id);
      void this.persistJob(job);
    }, REMOTE_JOB_PERSIST_DEBOUNCE_MS);
    this.jobPersistTimers.set(job.id, timer);
  }

  private async persistJob(job: RemoteOptimizationJobRecord): Promise<void> {
    try {
      await this.db.remoteOptimizerJobs.upsertRemoteOptimizerJob(this.serializeJob(job));
    } catch (error) {
      this.loggingService.warn(
        REMOTE_OPTIMIZER_SOURCE,
        `Failed to persist remote optimizer job ${job.id}: ${this.describeError(error)}`
      );
    }
  }

  private serializeJob(job: RemoteOptimizationJobRecord): RemoteOptimizerJobEntity {
    return {
      id: job.id,
      templateId: job.templateId,
      templateName: job.templateName,
      status: job.status,
      createdAt: job.createdAt,
      startedAt: job.startedAt,
      finishedAt: job.finishedAt,
      hetznerServerId: job.hetznerServerId ?? null,
      remoteServerIp: job.remoteServerIp ?? null
    };
  }

  private log(
    job: RemoteOptimizationJobRecord,
    message: string,
    level: 'info' | 'warn' | 'error' = 'info',
    meta: Record<string, any> = {}
  ): void {
    const entry = `[${new Date().toISOString()}] ${message}`;
    job.logBuffer.push(entry);
    if (job.logBuffer.length > REMOTE_JOB_LOG_LIMIT) {
      job.logBuffer.splice(0, job.logBuffer.length - REMOTE_JOB_LOG_LIMIT);
    }
    const payload = {
      jobId: job.id,
      templateId: job.templateId,
      ...meta
    };
    if (level === 'info') {
      this.loggingService.info(REMOTE_OPTIMIZER_SOURCE, message, payload);
    } else if (level === 'warn') {
      this.loggingService.warn(REMOTE_OPTIMIZER_SOURCE, message, payload);
    } else {
      this.loggingService.error(REMOTE_OPTIMIZER_SOURCE, message, payload);
    }
    this.scheduleJobPersist(job);
  }

  private enterStage(job: RemoteOptimizationJobRecord, stage: string, message?: string): void {
    job.currentStage = stage;
    if (message) {
      this.log(job, message, 'info', { stage });
    }
  }

  private getJobLogTail(job: RemoteOptimizationJobRecord, maxEntries = 20): string | undefined {
    if (!job.logBuffer.length) {
      return undefined;
    }
    return job.logBuffer.slice(-maxEntries).join('\n');
  }

  private buildFailureMessage(job: RemoteOptimizationJobRecord, error: unknown): string {
    const detail = this.describeError(error);
    if (job.currentStage) {
      return `Stage "${job.currentStage}" failed: ${detail}`;
    }
    return detail;
  }

  private describeError(error: unknown): string {
    if (!error) {
      return 'Unknown error';
    }
    if (axios.isAxiosError(error)) {
      return this.describeAxiosError(error);
    }
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  }

  private isRemoteFileMissing(error: unknown): boolean {
    if (!error || typeof error !== 'object') {
      return false;
    }
    const code = (error as { code?: string | number }).code;
    if (code === 2 || code === 'ENOENT') {
      return true;
    }
    const message = (error as { message?: string }).message;
    return typeof message === 'string' && message.toLowerCase().includes('no such file');
  }

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private describeAxiosError(error: unknown): string {
    if (!error) {
      return 'Unknown Hetzner error';
    }
    if (axios.isAxiosError(error)) {
      const status = error.response?.status;
      const body =
        typeof error.response?.data === 'string'
          ? error.response.data
          : JSON.stringify(error.response?.data ?? {});
      return `status=${status ?? 'n/a'} body=${body}`;
    }
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  }

  private escapeHtml(value: string): string {
    return value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  private escapeShell(value: string | null | undefined): string {
    if (!value) {
      return '';
    }
    return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
  }

  private registerKeyboardInteractiveHandler(conn: SSHClient): void {
    conn.on('keyboard-interactive', (_name, _instructions, _lang, prompts, finish) => {
      if (!prompts.length) {
        finish([]);
        return;
      }
      finish(prompts.map(() => ''));
    });
  }

  private trimCommandOutput(output: string): string | undefined {
    if (!output) {
      return undefined;
    }
    const trimmed = output.trim();
    if (!trimmed) {
      return undefined;
    }
    if (trimmed.length <= REMOTE_COMMAND_OUTPUT_LIMIT) {
      return trimmed;
    }
    return `${trimmed.slice(trimmed.length - REMOTE_COMMAND_OUTPUT_LIMIT)} (truncated)`;
  }

  private buildCloudInitUserData(): string {
    const sshConfig = [
      'PermitRootLogin prohibit-password',
      'PasswordAuthentication no',
      'ChallengeResponseAuthentication no'
    ].join('\n');
    return `#cloud-config
write_files:
  - path: /etc/ssh/sshd_config.d/99-stratcraft.conf
    permissions: '0644'
    content: |
      ${sshConfig.replace(/\n/g, '\n      ')}
runcmd:
  - systemctl reload ssh || systemctl reload sshd
  - passwd -d root || true
`;
  }

  private buildServerName(job: RemoteOptimizationJobRecord): string {
    const raw = `sc-${job.templateId}-${Date.now()}`.toLowerCase();
    let sanitized = raw.replace(/[^a-z0-9.-]/g, '-').replace(/-+/g, '-').replace(/^-+|-+$/g, '');
    if (!sanitized.length) {
      sanitized = `sc-${Date.now()}`;
    }
    if (sanitized.length > 63) {
      sanitized = sanitized.slice(0, 63).replace(/-+$/g, '');
    }
    if (!sanitized.length) {
      sanitized = `sc-${Date.now()}`;
    }
    return sanitized;
  }

  private buildSnapshotFromEntity(entity: RemoteOptimizerJobEntity): RemoteOptimizationJobSnapshot {
    return {
      id: entity.id,
      templateId: entity.templateId,
      templateName: entity.templateName,
      status: entity.status,
      createdAt: entity.createdAt,
      startedAt: entity.startedAt,
      finishedAt: entity.finishedAt,
      hetznerServerId: entity.hetznerServerId ?? undefined,
      remoteServerIp: entity.remoteServerIp ?? undefined,
      triggeredBy: {
        userId: 'unknown',
        email: 'unknown'
      },
      remoteHandoffComplete: entity.status === 'handoff' || entity.status === 'succeeded'
    };
  }
}
