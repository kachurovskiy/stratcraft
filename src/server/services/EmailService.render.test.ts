import path from 'path';
import fs from 'fs/promises';
import puppeteer, { Browser } from 'puppeteer';
import { EmailService, OperationDispatchSummaryPayload } from './EmailService';
import { LoggingService } from './LoggingService';
import type { Database } from '../database/Database';
import type { Candle } from '../../shared/types/StrategyTemplate';

const mockSend = jest.fn();

jest.mock('resend', () => {
  return {
    Resend: jest.fn().mockImplementation(() => ({
      emails: {
        send: mockSend,
      },
    })),
  };
});

jest.setTimeout(30000);

const buildCloseSeries = (start: number, count: number, step: number): number[] => (
  Array.from({ length: count }, (_, index) => start + index * step)
);

const buildCandles = (ticker: string, closes: number[]): Candle[] => {
  const startDate = new Date(Date.UTC(2024, 0, 1));
  return closes.map((close, index) => {
    const date = new Date(startDate);
    date.setUTCDate(date.getUTCDate() + index);
    return {
      ticker,
      date,
      open: close * 0.99,
      high: close * 1.01,
      low: close * 0.98,
      close,
      unadjustedClose: close,
      volumeShares: 1_000_000
    };
  });
};

const buildCandlesRepo = (candlesByTicker: Record<string, Candle[]>) => ({
  getLastCandleDates: jest.fn(async (tickers: string[]) => {
    const result: Record<string, Date> = {};
    for (const ticker of tickers) {
      const candles = candlesByTicker[ticker] ?? [];
      const lastCandle = candles[candles.length - 1];
      if (lastCandle) {
        result[ticker] = lastCandle.date;
      }
    }
    return result;
  }),
  getCandles: jest.fn(async (tickers: string[]) => {
    const result: Record<string, Candle[]> = {};
    for (const ticker of tickers) {
      result[ticker] = candlesByTicker[ticker] ?? [];
    }
    return result;
  })
});

const createLoggingService = (): LoggingService => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
}) as unknown as LoggingService;

const createDb = (candlesByTicker: Record<string, Candle[]>): Database => ({
  settings: {
    getSettingValue: jest.fn().mockImplementation(async (key: string) => {
      if (key === 'RESEND_API_KEY') {
        return 're_test_key';
      }
      if (key === 'DOMAIN') {
        return 'example.com';
      }
      return null;
    }),
    getRequiredSettingValue: jest.fn().mockResolvedValue('re_test_key')
  },
  candles: buildCandlesRepo(candlesByTicker)
} as unknown as Database);

const buildDefaultCandles = (): Record<string, Candle[]> => ({
  AAPL: buildCandles('AAPL', buildCloseSeries(190, 25, 1)),
  TSLA: buildCandles('TSLA', buildCloseSeries(240, 25, 1)),
  MSFT: buildCandles('MSFT', buildCloseSeries(400, 25, 2)),
  NVDA: buildCandles('NVDA', buildCloseSeries(110, 25, 1))
});

const renderPreviewScreenshot = async (html: string, filename: string): Promise<void> => {
  const wrappedHtml = `
    <html>
      <head>
        <meta charset="utf-8" />
        <style>
          body {
            margin: 24px;
            background: #f4f6fb;
            font-family: Arial, sans-serif;
          }
        </style>
      </head>
      <body>${html}</body>
    </html>
  `;

  const previewPath = path.join(__dirname, filename);
  await fs.rm(previewPath, { force: true });

  let browser: Browser | null = null;
  try {
    browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    await page.setViewport({ width: 1400, height: 900 });
    await page.setContent(wrappedHtml, { waitUntil: 'domcontentloaded' });
    await page.screenshot({ path: previewPath, fullPage: true });
  } finally {
    if (browser) {
      await browser.close();
    }
  }

  const stats = await fs.stat(previewPath);
  expect(stats.size).toBeGreaterThan(10_000);
};

const renderDispatchSummaryHtml = async (
  summary: OperationDispatchSummaryPayload,
  candlesByTicker: Record<string, Candle[]>
): Promise<string> => {
  const emailService = new EmailService(createLoggingService(), createDb(candlesByTicker));
  await emailService.sendOperationDispatchSummary('ops@example.com', summary);

  expect(mockSend).toHaveBeenCalledTimes(1);
  const emailPayload = mockSend.mock.calls[0][0];
  return String(emailPayload.html ?? '');
};

describe('EmailService dispatch summary preview', () => {
  beforeEach(() => {
    mockSend.mockReset();
    mockSend.mockResolvedValue({ data: { id: 'preview-email' } });
  });

  it('renders a sample dispatch email with limit orders to a PNG artifact', async () => {
    const candlesByTicker = buildDefaultCandles();
    const summary: OperationDispatchSummaryPayload = {
      operations: [
        {
          accountName: 'Growth Fund',
          accountProvider: 'Alpaca',
          accountEnvironment: 'Live',
          ticker: 'AAPL',
          operationType: 'open_position',
          quantity: 25,
          price: 190.5,
          orderType: 'limit',
          status: 'sent',
        },
        {
          accountName: 'Growth Fund',
          accountProvider: 'Alpaca',
          accountEnvironment: 'Live',
          ticker: 'TSLA',
          operationType: 'open_position',
          quantity: 10,
          price: 245.33,
          orderType: 'market',
          status: 'failed',
          statusReason: 'Insufficient buying power',
        },
        {
          accountName: 'Swing Desk',
          accountProvider: 'Alpaca',
          accountEnvironment: 'Paper',
          ticker: 'MSFT',
          operationType: 'close_position',
          quantity: 15,
          price: 460.25,
          orderType: 'limit',
          status: 'sent',
        },
        {
          accountName: 'Swing Desk',
          accountProvider: 'IBKR',
          accountEnvironment: 'Paper',
          ticker: 'NVDA',
          operationType: 'update_stop_loss',
          quantity: 8,
          price: 118.71,
          orderType: 'market',
          status: 'failed',
          statusReason: 'Rejected by broker',
        },
      ],
    };

    const html = await renderDispatchSummaryHtml(summary, candlesByTicker);
    expect(html).toContain('Total cash');
    expect(html).toContain('Estimated cash');
    await renderPreviewScreenshot(html, 'operation-dispatch-summary-limit.png');
  });

  it('renders a market-only dispatch summary with a single cash impact line', async () => {
    const candlesByTicker = buildDefaultCandles();
    const summary: OperationDispatchSummaryPayload = {
      operations: [
        {
          accountName: 'Core Holdings',
          accountProvider: 'Alpaca',
          accountEnvironment: 'Live',
          ticker: 'AAPL',
          operationType: 'open_position',
          quantity: 5,
          price: 201.35,
          orderType: 'market',
          status: 'sent',
        },
        {
          accountName: 'Core Holdings',
          accountProvider: 'Alpaca',
          accountEnvironment: 'Live',
          ticker: 'MSFT',
          operationType: 'close_position',
          quantity: 4,
          price: 423.2,
          orderType: 'market',
          status: 'sent',
        }
      ],
    };

    const html = await renderDispatchSummaryHtml(summary, candlesByTicker);
    expect(html).toContain('Total cash');
    expect(html).not.toContain('Estimated cash');
    await renderPreviewScreenshot(html, 'operation-dispatch-summary-market.png');
  });
});
