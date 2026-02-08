import express, { NextFunction, Request, Response } from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';
import type { LogSource } from '../services/LoggingService';

const { promises: fsPromises, constants: fsConstants } = fs;

const router = express.Router();

const PM2_LOG_LINE_LIMIT = 1000;
const DEFAULT_PM2_APP_NAME = process.env.PM2_APP_NAME || 'stratcraft';
const PM2_LOG_DIR_CANDIDATES = getPm2LogDirCandidates();

const INFO_LOG_PATH = resolveLogPath('info');
const ERROR_LOG_PATH = resolveLogPath('error');
const LOGS_PAGE_PATH = '/admin/logs';

// Admin logs page
router.get('/', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const sourceFilter = typeof req.query.source === 'string' ? req.query.source.trim() : '';
    const systemLogs = await req.loggingService.getSystemLogs(
      sourceFilter ? sourceFilter as LogSource : undefined,
      undefined,
      500
    );
    const logSources = await req.loggingService.getSystemLogSources();
    if (sourceFilter && !logSources.includes(sourceFilter)) {
      logSources.unshift(sourceFilter);
    }
    const pm2Logs = await loadPm2LogPreviews();

    res.render('pages/logs', {
      title: 'System Logs',
      page: 'logs',
      systemLogs,
      logSources,
      selectedSource: sourceFilter || undefined,
      pm2Logs,
      user: req.user,
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error loading admin logs:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load admin logs'
    });
  }
});

// Delete all system logs (admin only)
router.post('/delete-all-logs', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const deletedCount = await req.loggingService.deleteAllLogs();
    const message = deletedCount === 1
      ? 'Deleted 1 system log entry'
      : `Deleted ${deletedCount} system log entries`;
    res.redirect(`${LOGS_PAGE_PATH}?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error deleting system logs:', error);
    res.redirect(`${LOGS_PAGE_PATH}?error=Failed to delete system logs`);
  }
});

// Clear PM2 logs (admin only)
router.post('/clear-pm2-logs', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const results = await Promise.all([
      clearPm2LogFile('Info Log', INFO_LOG_PATH),
      clearPm2LogFile('Error Log', ERROR_LOG_PATH)
    ]);

    const cleared = results.filter(result => result.success).map(result => result.label);
    const failures = results
      .filter(result => !result.success)
      .map(result => `${result.label}${result.message ? `: ${result.message}` : ''}`);

    const params: string[] = [];

    if (cleared.length === 2) {
      params.push(`success=${encodeURIComponent('Cleared PM2 Info and Error Logs')}`);
    } else if (cleared.length === 1) {
      params.push(`success=${encodeURIComponent(`Cleared PM2 ${cleared[0]}`)}`);
    }

    if (failures.length) {
      params.push(`error=${encodeURIComponent(failures.join('; '))}`);
    }

    const query = params.length ? `?${params.join('&')}` : '';
    res.redirect(`${LOGS_PAGE_PATH}${query}`);
  } catch (error) {
    console.error('Error clearing PM2 logs:', error);
    res.redirect(`${LOGS_PAGE_PATH}?error=Failed to clear PM2 logs`);
  }
});

interface ExternalLogPreview {
  label: string;
  path?: string;
  lines: string[];
  lineLimit: number;
  available: boolean;
  lastModified?: string;
  error?: string;
}

interface ClearLogResult {
  label: string;
  success: boolean;
  message?: string;
}

async function loadPm2LogPreviews(): Promise<Record<'info' | 'error', ExternalLogPreview>> {
  const [info, error] = await Promise.all([
    buildLogPreview('Info Log', INFO_LOG_PATH, PM2_LOG_LINE_LIMIT),
    buildLogPreview('Error Log', ERROR_LOG_PATH, PM2_LOG_LINE_LIMIT)
  ]);

  return { info, error };
}

async function clearPm2LogFile(label: string, filePath: string | undefined): Promise<ClearLogResult> {
  if (!filePath) {
    return {
      label,
      success: false,
      message: 'Log path not configured'
    };
  }

  try {
    await fsPromises.access(filePath, fsConstants.W_OK);
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    const reason =
      err?.code === 'ENOENT'
        ? 'Log file not found'
        : err?.code === 'EACCES'
          ? 'Insufficient permissions to modify log file'
          : 'Unable to access log file';

    return {
      label,
      success: false,
      message: reason
    };
  }

  try {
    await fsPromises.truncate(filePath, 0);
    return { label, success: true };
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    return {
      label,
      success: false,
      message: err?.message || 'Failed to clear log file'
    };
  }
}

async function buildLogPreview(
  label: string,
  filePath: string | undefined,
  lineLimit: number
): Promise<ExternalLogPreview> {
  if (!filePath) {
    return {
      label,
      lines: [],
      lineLimit,
      available: false,
      error: 'Log path not configured'
    };
  }

  try {
    await fsPromises.access(filePath, fsConstants.R_OK);
  } catch (error) {
    const err = error as NodeJS.ErrnoException;
    const message =
      err?.code === 'ENOENT'
        ? 'Log file not found'
        : err?.code === 'EACCES'
          ? 'Insufficient permissions to read log file'
          : `Unable to access log file${err?.code ? ` (${err.code})` : ''}`;

    return {
      label,
      path: filePath,
      lines: [],
      lineLimit,
      available: false,
      error: message
    };
  }

  try {
    const { lines, mtime } = await readLastLines(filePath, lineLimit);
    return {
      label,
      path: filePath,
      lines,
      lineLimit,
      available: true,
      lastModified: mtime?.toISOString()
    };
  } catch (error) {
    console.error(`Failed to read PM2 log file ${filePath}:`, error);
    const err = error as NodeJS.ErrnoException;
    return {
      label,
      path: filePath,
      lines: [],
      lineLimit,
      available: false,
      error: err?.message || 'Failed to read log file'
    };
  }
}

async function readLastLines(
  filePath: string,
  maxLines: number
): Promise<{ lines: string[]; mtime?: Date; }> {
  const handle = await fsPromises.open(filePath, 'r');
  try {
    const stats = await handle.stat();
    if (stats.size === 0) {
      return { lines: [], mtime: stats.mtime };
    }

    const chunkSize = 64 * 1024;
    let position = stats.size;
    let collected = '';
    let newlineCount = 0;

    while (position > 0 && newlineCount <= maxLines + 5) {
      const readSize = Math.min(chunkSize, position);
      position -= readSize;

      const buffer = Buffer.alloc(readSize);
      const { bytesRead } = await handle.read(buffer, 0, readSize, position);
      if (bytesRead <= 0) {
        break;
      }

      const chunk = buffer.toString('utf8', 0, bytesRead);
      collected = chunk + collected;
      newlineCount += chunk.split('\n').length - 1;
    }

    const normalized = collected.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
    const segments = normalized.endsWith('\n')
      ? normalized.slice(0, -1).split('\n')
      : normalized.split('\n');
    const lines = segments
      .filter((line, index) => index !== segments.length - 1 || line.length > 0)
      .slice(-maxLines);

    return { lines, mtime: stats.mtime };
  } finally {
    await handle.close();
  }
}

function resolveLogPath(kind: 'info' | 'error'): string | undefined {
  const envPath =
    kind === 'info'
      ? process.env.ADMIN_PM2_INFO_LOG || process.env.PM2_INFO_LOG_PATH
      : process.env.ADMIN_PM2_ERROR_LOG || process.env.PM2_ERROR_LOG_PATH;

  if (envPath) {
    return envPath;
  }

  const fileNames =
    kind === 'info'
      ? ['out.log', `${DEFAULT_PM2_APP_NAME}-out.log`, `${DEFAULT_PM2_APP_NAME}.log`]
      : ['err.log', `${DEFAULT_PM2_APP_NAME}-error.log`, `${DEFAULT_PM2_APP_NAME}-err.log`];

  for (const dir of PM2_LOG_DIR_CANDIDATES) {
    for (const fileName of fileNames) {
      const candidatePath = path.join(dir, fileName);
      try {
        if (fs.existsSync(candidatePath)) {
          return candidatePath;
        }
      } catch {
        continue;
      }
    }
  }

  const fallbackDir = PM2_LOG_DIR_CANDIDATES.find(Boolean);
  return fallbackDir ? path.join(fallbackDir, fileNames[0]) : undefined;
}

function getPm2LogDirCandidates(): string[] {
  const candidates: (string | undefined)[] = [
    process.env.ADMIN_PM2_LOG_DIR,
    process.env.PM2_LOG_DIR,
    '/opt/stratcraft/stratcraft/logs'
  ];

  try {
    const homeDir = os.homedir();
    if (homeDir) {
      candidates.push(path.join(homeDir, '.pm2', 'logs'));
    }
  } catch {
    // ignore homedir resolution failures
  }

  const uniqueCandidates: string[] = [];
  for (const candidate of candidates) {
    if (candidate && !uniqueCandidates.includes(candidate)) {
      uniqueCandidates.push(candidate);
    }
  }

  return uniqueCandidates;
}

export default router;
