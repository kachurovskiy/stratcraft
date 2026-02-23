import { csrfMiddleware } from './csrf';

type HeaderMap = Record<string, string>;

function createReq(options: {
  method?: string;
  path: string;
  headers?: HeaderMap;
  cookies?: Record<string, string>;
  body?: Record<string, unknown>;
  query?: Record<string, unknown>;
}): any {
  const normalizedHeaders: HeaderMap = {};
  for (const [key, value] of Object.entries(options.headers ?? {})) {
    normalizedHeaders[key.toLowerCase()] = value;
  }

  return {
    method: options.method ?? 'POST',
    path: options.path,
    cookies: options.cookies ?? {},
    body: options.body ?? {},
    query: options.query ?? {},
    xhr: false,
    get: (name: string) => normalizedHeaders[name.toLowerCase()]
  };
}

function createRes(): any {
  const res: any = {
    locals: {},
    cookie: jest.fn(),
    status: jest.fn().mockReturnThis(),
    json: jest.fn().mockReturnThis(),
    render: jest.fn().mockReturnThis()
  };
  return res;
}

describe('csrfMiddleware', () => {
  it('skips CSRF validation for backtest API endpoints', () => {
    const req = createReq({ path: '/api/backtest/check' });
    const res = createRes();
    const next = jest.fn();

    csrfMiddleware(req, res, next);

    expect(next).toHaveBeenCalledTimes(1);
    expect(res.status).not.toHaveBeenCalled();
  });

  it('rejects unsafe methods without valid tokens', () => {
    const req = createReq({
      path: '/dashboard',
      headers: { accept: 'application/json' }
    });
    const res = createRes();
    const next = jest.fn();

    csrfMiddleware(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(403);
    expect(res.json).toHaveBeenCalledWith({
      error: 'Invalid CSRF token. Please refresh and try again.'
    });
  });
});

