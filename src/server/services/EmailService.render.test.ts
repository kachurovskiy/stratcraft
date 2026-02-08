import path from 'path';
import fs from 'fs/promises';
import puppeteer, { Browser } from 'puppeteer';
import { EmailService, OperationDispatchSummaryPayload } from './EmailService';
import { LoggingService } from './LoggingService';
import type { Database } from '../database/Database';

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

describe('EmailService dispatch summary preview', () => {
  beforeEach(() => {
    mockSend.mockReset();
    mockSend.mockResolvedValue({ data: { id: 'preview-email' } });
  });

  it('renders a sample dispatch email to a PNG artifact', async () => {
    const loggingService = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
      debug: jest.fn(),
    } as unknown as LoggingService;

    const db = {
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
      }
    } as unknown as Database;

    const emailService = new EmailService(loggingService, db);
    const summary: OperationDispatchSummaryPayload = {
      operations: [
        {
          accountName: 'Growth Fund',
          accountProvider: 'Alpaca',
          accountEnvironment: 'Live',
          ticker: 'AAPL',
          operationType: 'open_position',
          quantity: 25,
          price: 192.13,
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
          price: 405.42,
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
          status: 'failed',
          statusReason: 'Rejected by broker',
        },
      ],
    };

    await emailService.sendOperationDispatchSummary('ops@example.com', summary);

    expect(mockSend).toHaveBeenCalledTimes(1);
    const emailPayload = mockSend.mock.calls[0][0];
    const html = String(emailPayload.html ?? '');

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

    const previewPath = path.join(__dirname, 'operation-dispatch-summary.png');
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
  });
});
