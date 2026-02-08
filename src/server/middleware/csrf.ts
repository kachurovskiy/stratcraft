import crypto from 'crypto';
import type { NextFunction, Request, Response } from 'express';

const CSRF_COOKIE_NAME = 'csrf_token';
const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);
const BACKTEST_API_PATH_PREFIX = '/api/backtest/';

function extractToken(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function getRequestCsrfToken(req: Request): string | null {
  const headerToken =
    extractToken(req.get('x-csrf-token'));

  if (headerToken) {
    return headerToken;
  }

  const bodyToken = extractToken((req.body as Record<string, unknown> | undefined)?._csrf);
  if (bodyToken) {
    return bodyToken;
  }

  const queryToken = extractToken((req.query as Record<string, unknown> | undefined)?._csrf);
  return queryToken;
}

function isMultipartRequest(req: Request): boolean {
  const contentType = req.get('content-type') || '';
  return contentType.includes('multipart/form-data');
}

export function isCsrfRequestValid(req: Request): boolean {
  const cookieToken = extractToken(req.cookies?.[CSRF_COOKIE_NAME]);
  const requestToken = getRequestCsrfToken(req);
  return Boolean(cookieToken && requestToken && cookieToken === requestToken);
}

export function handleCsrfFailure(req: Request, res: Response): void {
  const contentType = req.get('content-type') || '';
  const wantsJson =
    req.path.startsWith('/api') ||
    (req.get('accept') || '').includes('application/json') ||
    contentType.includes('application/json') ||
    Boolean(req.get('x-csrf-token')) ||
    req.xhr;

  if (wantsJson) {
    res.status(403).json({ error: 'Invalid CSRF token. Please refresh and try again.' });
    return;
  }

  res.status(403).render('pages/error', {
    title: 'Security Check Failed',
    error: 'Invalid CSRF token. Please refresh and try again.'
  });
}

export function csrfMiddleware(req: Request, res: Response, next: NextFunction): void {
  let token = extractToken(req.cookies?.[CSRF_COOKIE_NAME]);

  if (!token) {
    token = crypto.randomBytes(32).toString('hex');
    res.cookie(CSRF_COOKIE_NAME, token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      path: '/'
    });
  }

  res.locals.csrfToken = token;

  if (SAFE_METHODS.has(req.method.toUpperCase())) {
    next();
    return;
  }

  if (req.path.startsWith(BACKTEST_API_PATH_PREFIX)) {
    next();
    return;
  }

  if (isMultipartRequest(req)) {
    if (extractToken(req.get('x-csrf-token'))) {
      if (isCsrfRequestValid(req)) {
        next();
      } else {
        handleCsrfFailure(req, res);
      }
      return;
    }

    if (req.path === '/admin/database/backtest-cache/import') {
      next();
      return;
    }

    handleCsrfFailure(req, res);
    return;
  }

  if (!isCsrfRequestValid(req)) {
    handleCsrfFailure(req, res);
    return;
  }

  next();
}
