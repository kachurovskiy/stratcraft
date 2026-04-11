import type { AxiosResponse } from 'axios';
import { LoggingService } from '../../services/LoggingService';

export type RequestParams = Record<string, string | number | undefined>;

type RequestWithRetryOptions<T> = {
  url: string;
  request: () => Promise<AxiosResponse<T>>;
  logParams?: RequestParams;
  symbol?: string;
  sourceLabel: string;
  waitSeconds: number;
  redactKeys?: string[];
  abortSignal?: AbortSignal;
};

const MAX_RESPONSE_TEXT_LENGTH = 2000;
const ABORT_ERROR_MESSAGE = 'Request cancelled';

export function startOfDay(date: Date): Date {
  const normalized = new Date(date);
  normalized.setUTCHours(0, 0, 0, 0);
  return normalized;
}

export function formatDate(date: Date): string {
  return startOfDay(date).toISOString().split('T')[0];
}

export function sanitizeParams(params: RequestParams, redactKeys: string[] = []): RequestParams {
  const sanitized: RequestParams = { ...params };
  for (const key of redactKeys) {
    if (sanitized[key]) {
      sanitized[key] = '[REDACTED]';
    }
  }
  return sanitized;
}

function extractResponseText(data: unknown, maxLength: number = MAX_RESPONSE_TEXT_LENGTH): string | undefined {
  if (data === undefined || data === null) {
    return undefined;
  }
  let text: string;
  if (typeof data === 'string') {
    text = data;
  } else {
    try {
      text = JSON.stringify(data);
    } catch (err) {
      text = String(data);
    }
  }
  if (text.length > maxLength) {
    return `${text.slice(0, maxLength)}...[truncated]`;
  }
  return text;
}

export async function requestWithRetry<T>(
  loggingService: LoggingService,
  options: RequestWithRetryOptions<T>
): Promise<{ data: T; noData: boolean }> {
  const {
    url,
    request,
    logParams = {},
    symbol,
    sourceLabel,
    waitSeconds,
    redactKeys = [],
    abortSignal
  } = options;
  const sanitizedParams = sanitizeParams(logParams, redactKeys);

  try {
    if (abortSignal?.aborted) {
      throw new Error(ABORT_ERROR_MESSAGE);
    }
    const response = await request();
    return { data: response.data, noData: false };
  } catch (error: any) {
    if (abortSignal?.aborted || error?.code === 'ERR_CANCELED' || error?.name === 'CanceledError') {
      throw new Error(ABORT_ERROR_MESSAGE);
    }
    const status = error.response?.status;
    if (status === 429) {
      const responseText = error.response?.data
        ? (typeof error.response.data === 'string' ? error.response.data : JSON.stringify(error.response.data))
        : undefined;
      loggingService.warn('candle-job', `${sourceLabel} API rate limit exceeded, waiting ${waitSeconds} seconds...`, {
        url,
        params: sanitizedParams,
        sanitizedParams,
        response_text: responseText
      });
      await new Promise(resolve => setTimeout(resolve, waitSeconds * 1000));
      return requestWithRetry(loggingService, options);
    }

    if (status === 404) {
      if (symbol) {
        return { data: [] as unknown as T, noData: true };
      }
      loggingService.info('candle-job', `${sourceLabel} returned 404 (no data)`, {
        url,
        params: sanitizedParams
      });
      return { data: [] as unknown as T, noData: false };
    }

    loggingService.error('candle-job', `${sourceLabel} API request failed`, {
      url,
      params: sanitizedParams,
      sanitizedParams,
      status,
      error: error.message,
      response_text: extractResponseText(error.response?.data)
    });
    throw error;
  }
}
