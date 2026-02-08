import express, { NextFunction, Request, Response } from 'express';
import multer from 'multer';
import { JobScheduler, JobType } from '../jobs/JobScheduler';
import { handleCsrfFailure, isCsrfRequestValid } from '../middleware/csrf';

const router = express.Router();

const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

const requireAdmin = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
};

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 100 * 1024 * 1024
  }
});

const BACKTEST_CACHE_EXPORT_HEADER = '-- StratCraft Backtest Cache Export';
const BACKTEST_CACHE_PAYLOAD_START = '/*BACKTEST_CACHE_EXPORT:START';
const BACKTEST_CACHE_PAYLOAD_END = 'BACKTEST_CACHE_EXPORT:END*/';
const BACKTEST_CACHE_SQL_COLUMNS = [
  'id',
  'template_id',
  'parameters',
  'sharpe_ratio',
  'calmar_ratio',
  'total_return',
  'cagr',
  'max_drawdown',
  'max_drawdown_ratio',
  'verify_sharpe_ratio',
  'verify_calmar_ratio',
  'verify_cagr',
  'verify_max_drawdown_ratio',
  'win_rate',
  'total_trades',
  'ticker_count',
  'start_date',
  'end_date',
  'period_days',
  'period_months',
  'duration_minutes',
  'tool',
  'top_abs_gain_ticker',
  'top_rel_gain_ticker',
  'created_at'
];

async function waitForJobTermination(
  jobScheduler: JobScheduler,
  jobType: JobType,
  timeoutMs = 15000
): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const currentJob = jobScheduler.getCurrentJob();
    if (!currentJob || currentJob.type !== jobType || currentJob.status !== 'running') {
      return true;
    }
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  return false;
}

async function cancelAllJobsForMaintenance(
  jobScheduler: JobScheduler | undefined,
  reason: string,
  timeoutMs = 30000
): Promise<{ cancelled: number; idle: boolean; pendingJobs: number; }> {
  if (!jobScheduler) {
    return { cancelled: 0, idle: true, pendingJobs: 0 };
  }

  const cancelled = jobScheduler.cancelAllJobs(reason);
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const pendingJobs = jobScheduler.getQueuedJobs().length;
    if (pendingJobs === 0) {
      return { cancelled, idle: true, pendingJobs: 0 };
    }
    await new Promise(resolve => setTimeout(resolve, 250));
  }

  const pendingJobs = jobScheduler.getQueuedJobs().length;
  return { cancelled, idle: pendingJobs === 0, pendingJobs };
}

router.get('/', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const [allUsers, backtestCacheStats, signalSummary, adminEntityCounts] = await Promise.all([
      req.authService.getAllUsers(),
      req.db.backtestCache.getBacktestCacheStats(),
      req.db.signals.getSignalSummary(),
      req.db.admin.getAdminEntityCounts()
    ]);

    const databaseEntityCounts = {
      ...adminEntityCounts,
      signals: typeof signalSummary?.count === 'number'
        ? signalSummary.count
        : adminEntityCounts.signals
    };

    res.render('pages/database', {
      title: 'Database Overview',
      page: 'database',
      user: req.user,
      allUsers,
      backtestCacheStats,
      databaseEntityCounts,
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error loading database overview:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load database overview'
    });
  }
});

// Clear all backtests (admin only)
router.post('/clear-all-backtests', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const result = await req.db.maintenance.clearAllBacktestResults();
    const message = `Successfully cleared ${result.backtestResultsDeleted} backtest result${result.backtestResultsDeleted === 1 ? '' : 's'} and ${result.backtestTradesDeleted} associated trade${result.backtestTradesDeleted === 1 ? '' : 's'} from the database`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing all backtests:', error);
    res.redirect('/admin/database?error=Failed to clear backtests from database');
  }
});

// Clear all trades (admin only)
router.post('/clear-all-trades', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const result = await req.db.maintenance.clearAllTrades();
    const message = `Successfully cleared ${result.tradesDeleted} trades and ${result.backtestResultsDeleted} backtest results from the database`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing all trades:', error);
    res.redirect('/admin/database?error=Failed to clear trades from database');
  }
});

// Clear all account operations (admin only)
router.post('/clear-all-account-operations', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const operationsDeleted = await req.db.accountOperations.clearAllAccountOperations();
    const message = `Successfully deleted ${operationsDeleted} account operations from the database`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing account operations:', error);
    res.redirect('/admin/database?error=Failed to delete account operations from database');
  }
});

// Clear all accounts (admin only)
router.post('/clear-all-accounts', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const result = await req.db.accounts.clearAllAccounts();
    const parts = [
      `Deleted ${result.accountsDeleted} account${result.accountsDeleted === 1 ? '' : 's'}`
    ];
    if (result.accountOperationsDeleted > 0) {
      parts.push(`cleared ${result.accountOperationsDeleted} account operation${result.accountOperationsDeleted === 1 ? '' : 's'}`);
    }
    if (result.strategiesUpdated > 0) {
      parts.push(`detached ${result.strategiesUpdated} strategie${result.strategiesUpdated === 1 ? 'y' : 's'} from accounts`);
    }
    const message = parts.join('. ');
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing accounts:', error);
    res.redirect('/admin/database?error=Failed to delete accounts from database');
  }
});

// Clear all strategies and trades (admin only)
router.post('/clear-all-strategies', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const result = await req.db.maintenance.clearAllStrategiesAndTrades();
    const message = `Successfully cleared ${result.strategiesDeleted} strategies, ${result.tradesDeleted} trades, and ${result.backtestResultsDeleted} backtest results from the database`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing all strategies and trades:', error);
    res.redirect('/admin/database?error=Failed to clear strategies and trades from database');
  }
});

// Clear all trading data (admin only)
router.post('/clear-all-trading-data', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const cancelResult = await cancelAllJobsForMaintenance(
      req.jobScheduler,
      'Admin requested trading data wipe'
    );
    if (!cancelResult.idle) {
      const reason = cancelResult.pendingJobs > 0
        ? `Unable to stop ${cancelResult.pendingJobs} background job(s).`
        : 'Unable to stop background jobs.';
      return res.redirect(`/admin/database?error=${encodeURIComponent(`${reason} Try again shortly.`)}`);
    }

    const result = await req.db.maintenance.clearAllTradingData();
    const message = `Successfully cleared all trading data: ${result.strategiesDeleted} strategies, ${result.tradesDeleted} trades, ${result.templatesDeleted} templates, and ${result.backtestResultsDeleted} backtest results from the database`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing all trading data:', error);
    res.redirect('/admin/database?error=Failed to clear trading data from database');
  }
});

// Delete all data except users (admin only)
router.post('/delete-all-data-except-users', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const cancelResult = await cancelAllJobsForMaintenance(
      req.jobScheduler,
      'Admin requested full database reset (excluding users)'
    );
    if (!cancelResult.idle) {
      const reason = cancelResult.pendingJobs > 0
        ? `Unable to stop ${cancelResult.pendingJobs} background job(s).`
        : 'Unable to stop background jobs.';
      return res.redirect(`/admin/database?error=${encodeURIComponent(`${reason} Try again once jobs finish.`)}`);
    }

    const result = await req.db.maintenance.clearAllDataExceptUsers();
    const triggeredBy = req.user?.email || req.user?.userId || 'unknown';
    req.loggingService.warn('system', 'Admin truncated all non-user database tables', {
      triggeredBy,
      truncatedTables: result.truncatedTables,
      totalRowEstimate: result.totalRowEstimate
    });

    const approxText = result.totalRowEstimate > 0
      ? `Approximately ${result.totalRowEstimate.toLocaleString()} rows were removed`
      : null;
    const messageParts = ['All non-user tables truncated successfully'];
    if (approxText) {
      messageParts.push(approxText);
    }
    const message = messageParts.join('. ');
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error truncating non-user data:', error);
    res.redirect('/admin/database?error=Failed to delete data');
  }
});

// Clear all candles (admin only)
  router.post('/clear-all-candles', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const candlesDeleted = await req.db.candles.clearAllCandles();
    req.db.tickers.invalidateCache();
    const message = `Successfully cleared ${candlesDeleted} candles from the database and cache`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing all candles:', error);
    res.redirect('/admin/database?error=Failed to clear candles from database');
  }
});

// Clear all tickers (admin only)
router.post('/clear-all-tickers', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const scheduler: JobScheduler | undefined = req.jobScheduler;
    if (scheduler) {
      const reason = 'Ticker deletion requested by admin';
      const cancelled = scheduler.cancelJobsByType('candle-sync', reason);
      if (cancelled > 0) {
        const stopped = await waitForJobTermination(scheduler, 'candle-sync');
        if (!stopped) {
          return res.redirect('/admin/database?error=Unable to cancel running candle sync job. Try again shortly.');
        }
      }
    }

    const result = await req.db.tickers.clearAllTickers();
    const message = `Deleted ${result.tickersDeleted} tickers, ${result.candlesDeleted} candles, ${result.tradesDeleted} trades, and ${result.signalsDeleted} signals`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing all tickers:', error);
    res.redirect('/admin/database?error=Failed to clear tickers from database');
  }
});

// Clear all signals (admin only)
router.post('/clear-all-signals', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const signalsDeleted = await req.db.signals.clearAllSignals();
    const message = `Successfully cleared ${signalsDeleted} signals from the database`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing all signals:', error);
    res.redirect('/admin/database?error=Failed to clear signals from database');
  }
});

// Bulk add tickers (admin only)
router.post('/bulk-add-tickers', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const { tickers } = req.body;

    if (!tickers || typeof tickers !== 'string') {
      return res.redirect('/admin/database?error=Please provide tickers to add');
    }

    // Parse tickers from whitespace-separated string
    const rawTickerList = tickers
      .split(/\s+/)
      .map(ticker => ticker.trim().toUpperCase())
      .filter(ticker => ticker.length > 0);

    if (rawTickerList.length === 0) {
      return res.redirect('/admin/database?error=No valid tickers provided');
    }

    // Validate tickers - only allow capital letters
    const validTickers: string[] = [];
    const invalidTickers: string[] = [];

    for (const ticker of rawTickerList) {
      if (/^[A-Z]+$/.test(ticker)) {
        validTickers.push(ticker);
      } else {
        invalidTickers.push(ticker);
      }
    }

    if (validTickers.length === 0) {
      return res.redirect('/admin/database?error=No valid tickers provided. Tickers must contain only capital letters.');
    }

    // Get existing tickers to check for duplicates
    const existingTickers = await req.db.tickers.getTickers();
    const existingSymbols = new Set(existingTickers.map((t: any) => t.symbol));

    const newTickers: string[] = [];
    const duplicateTickers: string[] = [];

    for (const ticker of validTickers) {
      if (existingSymbols.has(ticker)) {
        duplicateTickers.push(ticker);
      } else {
        newTickers.push(ticker);
      }
    }

    // Add only new tickers to database
    if (newTickers.length > 0) {
      await req.db.tickers.bulkInsertTickers(newTickers, { training: false });
    }

    // Build feedback message
    const messages: string[] = [];

    if (newTickers.length > 0) {
      messages.push(`Added ${newTickers.length} new ticker${newTickers.length === 1 ? '' : 's'}.`);
    }

    if (duplicateTickers.length > 0) {
      messages.push(`Failed to add ${duplicateTickers.length} existing ticker${duplicateTickers.length === 1 ? '' : 's'}: ${duplicateTickers.join(', ')}`);
    }

    if (invalidTickers.length > 0) {
      messages.push(`Rejected ${invalidTickers.length} invalid ticker${invalidTickers.length === 1 ? '' : 's'}: ${invalidTickers.join(', ')} (must be capital letters only)`);
    }

    const message = messages.join(' | ');
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error adding bulk tickers:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to add tickers';
    res.redirect(`/admin/database?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Bulk delete tickers (admin only)
router.post('/bulk-delete-tickers', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const { tickers } = req.body;

    if (!tickers || typeof tickers !== 'string') {
      return res.redirect('/admin/database?error=Please provide tickers to delete');
    }

    // Parse tickers from whitespace-separated string
    const rawTickerList = tickers
      .split(/\s+/)
      .map(ticker => ticker.trim().toUpperCase())
      .filter(ticker => ticker.length > 0);

    if (rawTickerList.length === 0) {
      return res.redirect('/admin/database?error=No valid tickers provided');
    }

    // Validate tickers - only allow capital letters
    const validTickers: string[] = [];
    const invalidTickers: string[] = [];

    for (const ticker of rawTickerList) {
      if (/^[A-Z]+$/.test(ticker)) {
        validTickers.push(ticker);
      } else {
        invalidTickers.push(ticker);
      }
    }

    if (validTickers.length === 0) {
      return res.redirect('/admin/database?error=No valid tickers provided. Tickers must contain only capital letters.');
    }

    // Get existing tickers to check which ones exist
    const existingTickers = await req.db.tickers.getTickers();
    const existingSymbols = new Set(existingTickers.map((t: any) => t.symbol));

    const existingTickersToDelete: string[] = [];
    const nonExistentTickers: string[] = [];

    for (const ticker of validTickers) {
      if (existingSymbols.has(ticker)) {
        existingTickersToDelete.push(ticker);
      } else {
        nonExistentTickers.push(ticker);
      }
    }

    // Delete only existing tickers from database
    if (existingTickersToDelete.length > 0) {
      await req.db.tickers.bulkDeleteTickers(existingTickersToDelete);
    }

    // Build feedback message
    const messages: string[] = [];

    if (existingTickersToDelete.length > 0) {
      messages.push(`Deleted ${existingTickersToDelete.length} ticker(s): ${existingTickersToDelete.join(', ')}`);
    }

    if (nonExistentTickers.length > 0) {
      messages.push(`Skipped ${nonExistentTickers.length} non-existent ticker(s): ${nonExistentTickers.join(', ')}`);
    }

    if (invalidTickers.length > 0) {
      messages.push(`Rejected ${invalidTickers.length} invalid ticker(s): ${invalidTickers.join(', ')} (must be capital letters only)`);
    }

    const message = messages.join(' | ');
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error deleting bulk tickers:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to delete tickers';
    res.redirect(`/admin/database?error=${encodeURIComponent(errorMessage)}`);
  }
});

router.get('/backtest-cache/export', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const entries = await req.db.backtestCache.getAllBacktestCacheEntries();
    const sql = buildBacktestCacheExportSql(entries);
    const timestamp = new Date().toISOString().replace(/[:]/g, '-');
    res.setHeader('Content-Type', 'application/sql; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="backtest-cache-${timestamp}.sql"`);
    res.send(sql);
  } catch (error) {
    console.error('Error exporting backtest cache:', error);
    res.status(500).send('Failed to export backtest cache');
  }
});

router.post('/backtest-cache/import', requireAuth, requireAdmin, upload.single('cacheFile'), async (req: Request, res: Response) => {
  try {
    if (!isCsrfRequestValid(req)) {
      handleCsrfFailure(req, res);
      return;
    }

    if (!req.file || !req.file.buffer) {
      return res.redirect('/admin/database?error=No cache export file provided');
    }

    const sqlText = req.file.buffer.toString('utf8');
    if (!sqlText.includes(BACKTEST_CACHE_EXPORT_HEADER)) {
      return res.redirect('/admin/database?error=Uploaded file is not a backtest cache export');
    }

    const payload = extractBacktestCachePayload(sqlText);
    const { inserted, skipped } = await req.db.backtestCache.replaceBacktestCacheEntries(payload);

    const parts = [`Imported ${inserted} backtest cache entr${inserted === 1 ? 'y' : 'ies'}`];
    if (skipped > 0) {
      parts.push(`Skipped ${skipped} invalid entr${skipped === 1 ? 'y' : 'ies'}`);
    }
    const message = parts.join('. ');
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error importing backtest cache export:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to import cache export';
    res.redirect(`/admin/database?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Clear all backtest cache entries (admin only)
router.post('/clear-backtest-cache', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const deletedCount = await req.db.backtestCache.clearBacktestCache();
    const resetCount = await req.db.templates.resetAllTemplateLocalOptimizationVersions();
    const parts = [
      `Cleared ${deletedCount} backtest cache entr${deletedCount === 1 ? 'y' : 'ies'}`,
      resetCount === 0
        ? 'All templates were already at Local Optimization version 0'
        : `Reset Local Optimization version to 0 for ${resetCount} template${resetCount === 1 ? '' : 's'}`
    ];
    const message = parts.join('. ');
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error clearing backtest cache:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to clear backtest cache';
    res.redirect(`/admin/database?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Clear backtest cache entries for a specific template (admin only)
router.post('/clear-backtest-cache-by-template', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  const templateId = (req.body.templateId ?? '').trim();

  if (!templateId) {
    return res.redirect('/admin/database?error=Template ID is required to clear cache entries');
  }

  try {
    const deletedCount = await req.db.backtestCache.clearBacktestCacheByTemplate(templateId);
    const message = deletedCount > 0
      ? `Removed ${deletedCount} backtest cache entries for template ${templateId}`
      : `No backtest cache entries found for template ${templateId}`;
    res.redirect(`/admin/database?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error(`Error clearing backtest cache for template ${templateId}:`, error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to clear template backtest cache entries';
    res.redirect(`/admin/database?error=${encodeURIComponent(errorMessage)}`);
  }
});

function buildBacktestCacheExportSql(entries: Record<string, any>[]): string {
  const headerLines = [
    BACKTEST_CACHE_EXPORT_HEADER,
    `-- Generated: ${new Date().toISOString()}`,
    `-- Rows: ${entries.length}`
  ];

  const payload = Buffer.from(JSON.stringify(entries), 'utf8').toString('base64');
  const payloadBlock = [
    BACKTEST_CACHE_PAYLOAD_START,
    payload,
    BACKTEST_CACHE_PAYLOAD_END
  ];

  const statements = [
    'BEGIN;',
    'TRUNCATE TABLE backtest_cache;',
    ...entries.map((entry) => buildBacktestCacheInsert(entry)),
    'COMMIT;'
  ];

  return [...headerLines, ...payloadBlock, ...statements].join('\n');
}

function buildBacktestCacheInsert(entry: Record<string, any>): string {
  const columnList = BACKTEST_CACHE_SQL_COLUMNS.join(', ');
  const values = BACKTEST_CACHE_SQL_COLUMNS.map((column) => escapeSqlLiteral(entry[column]));
  return `INSERT INTO backtest_cache (${columnList}) VALUES (${values.join(', ')});`;
}

function escapeSqlLiteral(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value.toString() : 'NULL';
  }
  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE';
  }
  if (value instanceof Date) {
    return `'${value.toISOString().replace(/'/g, '\'\'')}'`;
  }
  if (typeof value === 'object') {
    try {
      return `'${JSON.stringify(value).replace(/'/g, '\'\'')}'`;
    } catch {
      return `'${String(value).replace(/'/g, '\'\'')}'`;
    }
  }
  return `'${String(value).replace(/'/g, '\'\'')}'`;
}

function extractBacktestCachePayload(sqlText: string): Record<string, any>[] {
  const startIndex = sqlText.indexOf(BACKTEST_CACHE_PAYLOAD_START);
  if (startIndex === -1) {
    throw new Error('Cache export payload marker not found');
  }

  const payloadStart = startIndex + BACKTEST_CACHE_PAYLOAD_START.length;
  const endIndex = sqlText.indexOf(BACKTEST_CACHE_PAYLOAD_END, payloadStart);
  if (endIndex === -1) {
    throw new Error('Cache export payload end marker not found');
  }

  const base64 = sqlText.slice(payloadStart, endIndex).trim();
  if (!base64) {
    throw new Error('Cache export payload is empty');
  }

  try {
    const json = Buffer.from(base64, 'base64').toString('utf8');
    const parsed = JSON.parse(json);
    if (!Array.isArray(parsed)) {
      throw new Error('Invalid cache export payload format');
    }
    return parsed;
  } catch (error) {
    console.error('Failed to decode cache export payload:', error);
    throw new Error('Failed to parse cache export payload');
  }
}

export default router;
