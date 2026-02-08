import express, { NextFunction, Request, Response } from 'express';
import { timingSafeEqual } from 'crypto';
import { TemplateParams } from '../../shared/types/Express';
import { SETTING_KEYS } from '../constants';
const router = express.Router();

const BACKTEST_SECRET_HEADER = 'x-backtest-secret';

const extractBacktestSecret = (req: Request): string => {
  const headerSecret = req.get(BACKTEST_SECRET_HEADER) ?? '';
  return headerSecret.trim();
};

const secretsMatch = (provided: string, expected: string): boolean => {
  if (!provided || !expected) {
    return false;
  }
  const providedBuffer = Buffer.from(provided);
  const expectedBuffer = Buffer.from(expected);
  if (providedBuffer.length !== expectedBuffer.length) {
    return false;
  }
  return timingSafeEqual(providedBuffer, expectedBuffer);
};

const requireBacktestSecret = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const rawSecret = await req.db.settings.getSettingValue(SETTING_KEYS.BACKTEST_API_SECRET);
    const expectedSecret = typeof rawSecret === 'string' ? rawSecret.trim() : '';
    if (!expectedSecret) {
      return res.status(503).json({ error: 'Backtest API secret is not configured' });
    }

    const providedSecret = extractBacktestSecret(req);
    if (!secretsMatch(providedSecret, expectedSecret)) {
      const reason = (
        providedSecret.length === 0
          ? 'missing_backtest_secret_header'
          : providedSecret.length !== expectedSecret.length
            ? 'backtest_secret_length_mismatch'
            : 'backtest_secret_mismatch'
      );
      return res.status(401).json({
        error: 'Unauthorized',
        reason,
        header: BACKTEST_SECRET_HEADER,
        providedLength: providedSecret.length
      });
    }

    return next();
  } catch (error) {
    console.error('Error verifying backtest API secret:', error);
    return res.status(500).json({ error: 'Failed to authorize backtest request' });
  }
};

// Health check endpoint
router.get('/health', async (req: Request, res: Response) => {
  try {
    // Simple health check - just verify database is connected
    await req.db.tickers.getTickers();
    res.json({ status: 'ready', timestamp: new Date().toISOString() });
  } catch (error) {
    console.error('Health check failed:', error);
    res.status(503).json({ status: 'not ready', error: 'Database connection failed' });
  }
});

// Backtest cache endpoints
// Check if backtest result exists in cache
router.post('/backtest/check', requireBacktestSecret, async (req: Request, res: Response) => {
  try {
    const { templateId, parameters } = req.body;

    if (!templateId || !parameters) {
      return res.status(400).json({ error: 'Missing required fields: templateId and parameters' });
    }

    const cachedResult = await req.db.backtestCache.getBacktestCache(templateId, parameters);

    if (cachedResult) {
      res.json({
        exists: true,
        result: cachedResult
      });
    } else {
      res.json({ exists: false });
    }
  } catch (error) {
    console.error('Error checking backtest cache:', error);
    res.status(500).json({ error: 'Failed to check backtest cache' });
  }
});

// Ensure best-sharpe requests hit the database sequentially.
let bestSharpeQueue: Promise<void> = Promise.resolve();

const enqueueBestSharpeQuery = <T>(task: () => Promise<T>): Promise<T> => {
  const run = bestSharpeQueue.then(() => task());
  bestSharpeQueue = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
};

// Get best sharpe ratio for a template
router.get<TemplateParams>('/backtest/best/:templateId', requireBacktestSecret, async (req, res) => {
  try {
    const { templateId } = req.params;
    const result = await enqueueBestSharpeQuery(() => req.db.backtestCache.getBestParams(templateId));

    if (!result) {
      return res.status(404).json({ error: 'No backtest results found for this template' });
    }

    res.json(result);
  } catch (error) {
    console.error('Error fetching best sharpe ratio:', error);
    res.status(500).json({ error: 'Failed to fetch best sharpe ratio' });
  }
});

// Store backtest result in cache
router.post('/backtest/store', requireBacktestSecret, async (req: Request, res: Response) => {
  try {
    const {
      templateId,
      parameters,
      sharpeRatio,
      calmarRatio,
      totalReturn,
      cagr,
      maxDrawdown,
      maxDrawdownRatio,
      winRate,
      totalTrades,
      tickerCount,
      startDate,
      endDate,
      durationMinutes = 0,
      tool = 'unknown',
      topAbsoluteGainTicker,
      topRelativeGainTicker,
    } = req.body;

    if (!templateId || !parameters || sharpeRatio === undefined ||
        totalReturn === undefined || cagr === undefined || maxDrawdown === undefined ||
        winRate === undefined || totalTrades === undefined ||
        tickerCount === undefined || !startDate || !endDate) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const normalizedTrades = Number(totalTrades);
    const rawCalmarRatio = Number(calmarRatio);
    const normalizedCalmarRatio = Number.isFinite(rawCalmarRatio) ? rawCalmarRatio : 0;
    const normalizedTopAbsoluteGainTicker =
      typeof topAbsoluteGainTicker === 'string' && topAbsoluteGainTicker.trim().length > 0
        ? topAbsoluteGainTicker.trim()
        : null;
    const normalizedTopRelativeGainTicker =
      typeof topRelativeGainTicker === 'string' && topRelativeGainTicker.trim().length > 0
        ? topRelativeGainTicker.trim()
        : null;
    if (!Number.isFinite(normalizedTrades) || normalizedTrades <= 0) {
      return res.json({
        success: false,
        ignored: true,
        message: 'Backtest cache entry ignored because it produced zero trades'
      });
    }

    const normalizedMaxDrawdownRatio =
      typeof maxDrawdownRatio === 'number' && Number.isFinite(maxDrawdownRatio)
        ? maxDrawdownRatio
        : 0;
    const normalizedCagr = typeof cagr === 'number' ? cagr : Number(cagr);
    if (!Number.isFinite(normalizedCagr)) {
      return res.status(400).json({ error: 'Invalid CAGR provided' });
    }

    await req.db.backtestCache.storeBacktestCache(
      templateId,
      parameters,
      sharpeRatio,
      normalizedCalmarRatio,
      totalReturn,
      normalizedCagr,
      maxDrawdown,
      normalizedMaxDrawdownRatio,
      winRate,
      normalizedTrades,
      tickerCount,
      startDate,
      endDate,
      durationMinutes,
      tool,
      normalizedTopAbsoluteGainTicker,
      normalizedTopRelativeGainTicker,
    );

    res.json({ success: true, message: 'Backtest result cached successfully' });
  } catch (error) {
    console.error('Error storing backtest result:', error);
    res.status(500).json({ error: 'Failed to store backtest result' });
  }
});

export default router;
