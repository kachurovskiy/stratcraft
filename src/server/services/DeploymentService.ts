import { exec } from 'child_process';
import fs from 'fs';
import { promisify } from 'util';

const execAsync = promisify(exec);

const COMMAND_TIMEOUT_MS = 120000;
const DEPLOY_UPDATE_SCRIPT_PATH = '/usr/local/bin/stratcraft-update.sh';
const DEPLOY_TRIGGER_SCRIPT_PATH = '/usr/local/bin/stratcraft-manual-update-check.sh';
const DEPLOY_REPO_PATH = '/opt/stratcraft/stratcraft';
const MAX_DEPLOY_COMMITS = 100;
const UPDATE_TRIGGER_FILE = '/tmp/stratcraft-manual-update-trigger';

const stripAnsiCodes = (value: string): string => value.replace(/\u001b\[[0-9;]*[a-zA-Z]/g, '');

export type DeploymentCommitStatus = {
  repoPath: string;
  defaultBranch: string;
  behindCount: number;
  commitMessages: string[];
  truncated: boolean;
  error?: string;
};

export type UpstreamStatus = { behindCount: number; error?: string };

async function runShellCommand(command: string): Promise<{ stdout: string; stderr: string }> {
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

function parseCommitMessages(output: string): string[] {
  return output
    .split(/\r?\n/)
    .map(line => stripAnsiCodes(line).trim())
    .filter(Boolean);
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

export class DeploymentService {
  canUseDeploymentControls(): boolean {
    if (process.platform !== 'linux') return false;
    if (!Boolean(process.env.pm_id || process.env.PM2_HOME)) return false;
    return fs.existsSync(DEPLOY_UPDATE_SCRIPT_PATH) && fs.existsSync(DEPLOY_TRIGGER_SCRIPT_PATH);
  }

  async triggerServerUpdate(): Promise<void> {
    if (!this.canUseDeploymentControls()) {
      throw new Error('Deployment controls are unavailable for this host');
    }
    fs.writeFileSync(UPDATE_TRIGGER_FILE, '');
  }

  async getDeploymentCommitStatus(): Promise<DeploymentCommitStatus> {
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

  async getUpstreamBehindCount(): Promise<UpstreamStatus> {
    const status = await this.getDeploymentCommitStatus();
    return { behindCount: status.behindCount, error: status.error };
  }
}

export const deploymentService = new DeploymentService();
