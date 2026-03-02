import express, { NextFunction, Request, Response } from 'express';
import fs from 'fs';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

const router = express.Router();

const ADMIN_SERVER_RESTART_COMMAND = 'sudo /sbin/reboot';
const DEPLOY_UPDATE_SCRIPT_PATH = '/usr/local/bin/stratcraft-update.sh';
const DEPLOY_TRIGGER_SCRIPT_PATH = '/usr/local/bin/stratcraft-manual-update-check.sh';
const DEPLOY_REPO_PATH = '/opt/stratcraft/stratcraft';
const COMMAND_OUTPUT_MAX_CHARS = 500;
const COMMAND_TIMEOUT_MS = 120000;
const MAX_CPU_METRIC_POINTS = 5000;
const MAX_DEPLOY_COMMITS = 100;
const stripAnsiCodes = (value: string): string => value.replace(/\u001b\[[0-9;]*[a-zA-Z]/g, '');

const sanitizeCommandOutput = (output?: string): string | undefined => {
  if (!output) {
    return undefined;
  }
  const trimmed = stripAnsiCodes(output).trim();
  if (!trimmed) {
    return undefined;
  }
  if (trimmed.length > COMMAND_OUTPUT_MAX_CHARS) {
    return `${trimmed.slice(0, COMMAND_OUTPUT_MAX_CHARS)}...`;
  }
  return trimmed;
};

type DeploymentCommitStatus = {
  repoPath: string;
  defaultBranch: string;
  behindCount: number;
  commitMessages: string[];
  truncated: boolean;
  error?: string;
};

async function runShellCommand(command: string): Promise<{ stdout: string; stderr: string; }> {
  return execAsync(command, {
    timeout: COMMAND_TIMEOUT_MS,
    maxBuffer: 10 * 1024 * 1024
  });
}

function buildCommandErrorMessage(error: unknown, fallback: string): string {
  const stderr = typeof (error as any)?.stderr === 'string' ? stripAnsiCodes((error as any).stderr).trim() : '';
  const stdout = typeof (error as any)?.stdout === 'string' ? stripAnsiCodes((error as any).stdout).trim() : '';
  if (error instanceof Error) {
    const base = stripAnsiCodes(error.message || fallback);
    const details = stderr || stdout;
    return details ? `${base}: ${details}` : base;
  }
  return stderr || stdout || fallback;
}

async function resolveDefaultBranch(repoPath: string): Promise<string> {
  try {
    const { stdout } = await runShellCommand(`git -C ${repoPath} symbolic-ref refs/remotes/origin/HEAD`);
    const ref = stdout.trim();
    if (ref) {
      const parts = ref.split('/');
      const branch = parts[parts.length - 1];
      if (branch) {
        return branch;
      }
    }
  } catch {
    // Fall back to master if origin/HEAD is unavailable.
  }
  return 'master';
}

function parseCommitMessages(output: string): string[] {
  return output
    .split(/\r?\n/)
    .map(line => stripAnsiCodes(line).trim())
    .filter(Boolean);
}

async function getDeploymentCommitStatus(): Promise<DeploymentCommitStatus> {
  if (!fs.existsSync(DEPLOY_REPO_PATH)) {
    return {
      repoPath: DEPLOY_REPO_PATH,
      defaultBranch: 'master',
      behindCount: 0,
      commitMessages: [],
      truncated: false,
      error: `Repository not found at ${DEPLOY_REPO_PATH}`
    };
  }

  const defaultBranch = await resolveDefaultBranch(DEPLOY_REPO_PATH);

  try {
    await runShellCommand(`git -C ${DEPLOY_REPO_PATH} fetch origin --quiet`);
  } catch (error) {
    return {
      repoPath: DEPLOY_REPO_PATH,
      defaultBranch,
      behindCount: 0,
      commitMessages: [],
      truncated: false,
      error: buildCommandErrorMessage(error, 'Failed to fetch repository updates')
    };
  }

  try {
    const { stdout: countOutput } = await runShellCommand(
      `git -C ${DEPLOY_REPO_PATH} rev-list --left-right --count HEAD...origin/${defaultBranch}`
    );
    const counts = countOutput.trim().split(/\s+/);
    const behindCount = Number.parseInt(counts[1] || '0', 10) || 0;

    const commitMessages: string[] = [];
    if (behindCount > 0) {
      const listCount = Math.min(behindCount, MAX_DEPLOY_COMMITS);
      const { stdout: logOutput } = await runShellCommand(
        `git -C ${DEPLOY_REPO_PATH} log --format=%s -n ${listCount} HEAD..origin/${defaultBranch}`
      );
      commitMessages.push(...parseCommitMessages(logOutput));
    }

    return {
      repoPath: DEPLOY_REPO_PATH,
      defaultBranch,
      behindCount,
      commitMessages,
      truncated: behindCount > commitMessages.length
    };
  } catch (error) {
    return {
      repoPath: DEPLOY_REPO_PATH,
      defaultBranch,
      behindCount: 0,
      commitMessages: [],
      truncated: false,
      error: buildCommandErrorMessage(error, 'Failed to read repository status')
    };
  }
}

function canUseDeploymentControls(): boolean {
  if (process.platform !== 'linux') return false;
  if (!Boolean(process.env.pm_id || process.env.PM2_HOME)) return false;
  return fs.existsSync(DEPLOY_UPDATE_SCRIPT_PATH) && fs.existsSync(DEPLOY_TRIGGER_SCRIPT_PATH);
}

// Deployment panel
router.get('/', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const deploymentControlsEnabled = canUseDeploymentControls();
    const cpuMetrics = req.cpuMetricsService.getSummary(MAX_CPU_METRIC_POINTS);
    const deploymentCommitStatus = deploymentControlsEnabled ? await getDeploymentCommitStatus() : null;

    res.render('pages/deployment', {
      title: 'Deployment',
      page: 'deployment',
      user: req.user,
      success: req.query.success as string,
      error: req.query.error as string,
      deploymentControlsEnabled,
      cpuMetrics,
      deploymentCommitStatus
    });
  } catch (error) {
    console.error('Error loading deployment panel:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load deployment panel'
    });
  }
});

// Trigger server update (admin only)
router.post('/trigger-update', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, (req: Request, res: Response) => {
  if (!canUseDeploymentControls()) {
    res.redirect('/admin/deployment?error=Deployment controls are unavailable for this host');
    return;
  }

  try {
    const triggerFile = '/tmp/stratcraft-manual-update-trigger';
    fs.writeFileSync(triggerFile, '');
    const triggeredBy = req.user?.email || req.user?.userId || 'unknown';
    req.loggingService.info('admin', `Manual update triggered by ${triggeredBy}`);
    const message = 'Server update triggered successfully. The update will start within the next minute.';
    res.redirect(`/admin/deployment?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error triggering server update:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to trigger server update';
    res.redirect(`/admin/deployment?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Restart application via PM2 (admin only)
router.post('/restart-app', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  if (!canUseDeploymentControls()) {
    res.status(400).type('text/plain').send('Deployment controls are unavailable for this host');
    return;
  }

  const pmId = typeof process.env.pm_id === 'string' ? process.env.pm_id.trim() : '';
  if (!pmId) {
    res.status(400).type('text/plain').send(`Restart is only available when ${res.locals.siteName} is running under PM2`);
    return;
  }

  const triggeredBy = req.user?.email || req.user?.userId || 'unknown';

  req.loggingService.warn('system', 'Manual application restart requested via deployment panel (SIGTERM)', {
    triggeredBy,
    pid: process.pid,
    pmId
  });

  res.once('finish', () => {
    setTimeout(() => {
      try {
        process.kill(process.pid, 'SIGTERM');
      } catch (error) {
        console.error('Failed to send SIGTERM for PM2 restart:', error);
        process.exit(1);
      }
    }, 250);
  });

  const message = 'Application restart command sent. The service should resume within a few seconds.';
  res.redirect(`/admin/deployment?success=${encodeURIComponent(message)}`);
});

// Reboot host server (admin only)
router.post('/restart-server', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  if (!canUseDeploymentControls()) {
    res.redirect('/admin/deployment?error=Deployment controls are unavailable for this host');
    return;
  }

  const restartCommand = ADMIN_SERVER_RESTART_COMMAND;
  const triggeredBy = req.user?.email || req.user?.userId || 'unknown';

  try {
    const { stdout, stderr } = await runShellCommand(restartCommand);
    req.loggingService.warn('system', 'Host server restart triggered via deployment panel', {
      triggeredBy,
      restartCommand,
      stdout: sanitizeCommandOutput(stdout),
      stderr: sanitizeCommandOutput(stderr)
    });

    const message = 'Server restart initiated. The host will reboot shortly and may be unavailable for up to a minute.';
    res.redirect(`/admin/deployment?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error restarting host server via deployment route:', error);
    req.loggingService.error('system', 'Host server restart command failed', {
      triggeredBy,
      restartCommand,
      error: error instanceof Error ? error.message : String(error),
      stdout: sanitizeCommandOutput((error as any)?.stdout),
      stderr: sanitizeCommandOutput((error as any)?.stderr)
    });
    const errorMessage = buildCommandErrorMessage(error, 'Failed to restart server');
    res.redirect(`/admin/deployment?error=${encodeURIComponent(errorMessage)}`);
  }
});

export default router;
