import express, { NextFunction, Request, Response } from 'express';
import { randomUUID } from 'crypto';
import {
  AccountEnvironment,
  AccountPortfolioHistoryRequest,
  AccountPosition,
  AccountSnapshot
} from '../../shared/types/Account';
import { AccountParams } from '../../shared/types/Express';
import { AccountOperation, Strategy, Trade } from '../../shared/types/StrategyTemplate';
import { getReqUserId } from './utils';

const router = express.Router();

const OPERATIONS_LOOKBACK_DAYS = 7;
const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

const normalizeEnvironment = (value: string): AccountEnvironment => {
  const normalized = String(value || '').toLowerCase();
  return normalized === 'live' ? 'live' : 'paper';
};

const normalizeTickerList = (value: string): string[] => {
  return Array.from(
    new Set(
      value
        .split(/[\s,]+/)
        .map((ticker) => ticker.trim().toUpperCase())
        .filter((ticker) => ticker.length > 0)
    )
  );
};

const normalizeKeywordList = (value: string): string[] => {
  return Array.from(
    new Set(
      value
        .split(/[\s,]+/)
        .map((keyword) => keyword.trim().toLowerCase())
        .filter((keyword) => keyword.length > 0)
    )
  );
};

const parseExcludedTickersInput = (rawValue: unknown) => {
  const text = typeof rawValue === 'string' ? rawValue.trim() : '';
  return {
    text,
    tickers: text.length === 0 ? [] : normalizeTickerList(text)
  };
};

const parseExcludedKeywordsInput = (rawValue: unknown) => {
  const text = typeof rawValue === 'string' ? rawValue.trim() : '';
  return {
    text,
    keywords: text.length === 0 ? [] : normalizeKeywordList(text)
  };
};

const extractQueryMessage = (param: unknown): string | undefined => {
  if (typeof param === 'string' && param.length > 0) {
    return param;
  }
  if (Array.isArray(param) && param.length > 0) {
    return param[0];
  }
  return undefined;
};

const getSnapshotBadgeMeta = (snapshot?: AccountSnapshot) => {
  if (!snapshot) {
    return { label: 'Pending', variant: 'secondary' };
  }
  switch (snapshot.status) {
    case 'ready':
      return { label: 'Live', variant: 'success' };
    case 'unsupported':
      return { label: 'Manual', variant: 'warning' };
    default:
      return { label: 'Error', variant: 'danger' };
  }
};

router.get('/new', requireAuth, (req: Request, res: Response) => {
  res.render('pages/create-account', {
    title: 'Add Account',
    page: 'accounts',
    user: req.user,
    form: {
      name: '',
      provider: '',
      environment: 'paper',
      apiKey: '',
      excludedTickers: '',
      excludedKeywords: ''
    }
  });
});

router.get<AccountParams>('/:id', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = getReqUserId(req);
    const lookbackStart = new Date(Date.now() - OPERATIONS_LOOKBACK_DAYS * 24 * 60 * 60 * 1000);

    const tradingAccount = await req.db.accounts.getAccountById(id, userId);
    if (!tradingAccount) {
      return res.status(404).render('pages/error', {
        title: 'Accounts',
        error: 'Account not found or inaccessible.'
      });
    }

    const snapshotMap: Record<string, AccountSnapshot> = await req.accountDataService.fetchSnapshots([tradingAccount]);
    const accountIds = [tradingAccount.id];
    const [strategies, recentOperations, accountPositions]: [Strategy[], AccountOperation[], AccountPosition[]] =
      await Promise.all([
        req.db.strategies.getStrategies(userId),
        req.db.accountOperations.getAccountOperationsForAccounts(accountIds, undefined, { since: lookbackStart, order: 'desc' }),
        req.accountDataService.fetchOpenPositions(tradingAccount)
      ]);

    type AccountStrategySummary = {
      id: string;
      name: string;
      status: string;
      detailHref: string;
      operationsTotal: number;
      operationsPending: number;
      operationsHref: string;
      liveTradesTotal: number;
      liveTradesActive: number;
      liveTradesPending: number;
      liveTradesHref: string;
    };

    const strategyLookup: Record<string, AccountStrategySummary> = {};
    const strategiesByAccount: Record<string, AccountStrategySummary[]> = {};

    strategies.forEach((strategy: Strategy) => {
      const summary: AccountStrategySummary = {
        id: strategy.id,
        name: strategy.name,
        status: strategy.status,
        detailHref: `/strategies/${strategy.id}`,
        operationsTotal: 0,
        operationsPending: 0,
        operationsHref: `/strategies/${strategy.id}/operations`,
        liveTradesTotal: 0,
        liveTradesActive: 0,
        liveTradesPending: 0,
        liveTradesHref: `/strategies/${strategy.id}/live-trades`
      };
      strategyLookup[strategy.id] = summary;
      if (strategy.accountId) {
        if (!strategiesByAccount[strategy.accountId]) {
          strategiesByAccount[strategy.accountId] = [];
        }
        strategiesByAccount[strategy.accountId].push(summary);
      }
    });

    Object.values(strategiesByAccount).forEach((list) => {
      list.sort((a, b) => a.name.localeCompare(b.name));
    });

    type DecoratedOperation = AccountOperation & {
      strategyMeta: AccountStrategySummary | null;
    };

    type AccountLiveTradeSummary = {
      strategyId: string;
      strategyName: string | null;
      trade: Trade;
    };
    type AccountLiveTradeEntry = AccountLiveTradeSummary & { accountId: string };

    const operationsByAccount: Record<string, DecoratedOperation[]> = {};
    recentOperations.forEach((operation: AccountOperation) => {
      const decorated: DecoratedOperation = {
        ...operation,
        strategyMeta: strategyLookup[operation.strategyId] ?? null
      };
      if (!operationsByAccount[operation.accountId]) {
        operationsByAccount[operation.accountId] = [];
      }
      operationsByAccount[operation.accountId].push(decorated);
    });

    const liveTradeEntries = await req.db.trades.getLiveTradesForAccounts(accountIds, userId);
    const liveTradesByAccount: Record<string, AccountLiveTradeSummary[]> = {};
    liveTradeEntries.forEach((entry: AccountLiveTradeEntry) => {
      if (!liveTradesByAccount[entry.accountId]) {
        liveTradesByAccount[entry.accountId] = [];
      }
      liveTradesByAccount[entry.accountId].push({
        strategyId: entry.strategyId,
        strategyName: entry.strategyName ?? strategyLookup[entry.strategyId]?.name ?? null,
        trade: entry.trade
      });
    });

    const snapshot = snapshotMap[tradingAccount.id];
    const balance =
      typeof snapshot?.balance === 'number' && Number.isFinite(snapshot.balance) ? snapshot.balance : null;
    const strategyPrefillParams = balance !== null ? JSON.stringify({ initialCapital: balance }) : null;
    const accountOperations = operationsByAccount[tradingAccount.id] ? [...operationsByAccount[tradingAccount.id]] : [];
    const strategyOperationsLookup = new Map<string, { total: number; pending: number }>();
    accountOperations.forEach(operation => {
      const strategyId = operation.strategyMeta?.id ?? operation.strategyId;
      if (!strategyId) {
        return;
      }
      if (!strategyOperationsLookup.has(strategyId)) {
        strategyOperationsLookup.set(strategyId, {
          total: 0,
          pending: 0
        });
      }
      const summary = strategyOperationsLookup.get(strategyId)!;
      summary.total += 1;
      if (operation.status === 'pending') {
        summary.pending += 1;
      }
    });
    const excludedTickers: string[] = Array.isArray(tradingAccount.excludedTickers) ? tradingAccount.excludedTickers : [];
    const excludedKeywords: string[] = Array.isArray(tradingAccount.excludedKeywords) ? tradingAccount.excludedKeywords : [];
    const accountLiveTrades = liveTradesByAccount[tradingAccount.id] ? [...liveTradesByAccount[tradingAccount.id]] : [];
    const strategyLiveTradesLookup = new Map<string, { total: number; active: number; pending: number }>();
    accountLiveTrades.forEach(entry => {
      const strategyId = entry.strategyId;
      if (!strategyId) {
        return;
      }
      if (!strategyLiveTradesLookup.has(strategyId)) {
        strategyLiveTradesLookup.set(strategyId, {
          total: 0,
          active: 0,
          pending: 0
        });
      }
      const summary = strategyLiveTradesLookup.get(strategyId)!;
      summary.total += 1;
      if (entry.trade.status === 'active') {
        summary.active += 1;
      }
      if (entry.trade.status === 'pending') {
        summary.pending += 1;
      }
    });
    const activeTradeTickerSet = new Set<string>();
    accountLiveTrades.forEach(entry => {
      if (entry.trade?.status !== 'active') {
        return;
      }
      activeTradeTickerSet.add(entry.trade.ticker);
    });
    const uncoveredPositions = (Array.isArray(accountPositions) ? accountPositions : [])
      .filter(position => {
        const ticker = position?.ticker;
        if (!ticker) {
          return false;
        }
        return !activeTradeTickerSet.has(ticker);
      })
      .sort((a, b) => {
        const valueA = Math.abs(a?.marketValue ?? 0);
        const valueB = Math.abs(b?.marketValue ?? 0);
        return valueB - valueA;
      });
    const strategiesForAccount = (strategiesByAccount[tradingAccount.id] ? [...strategiesByAccount[tradingAccount.id]] : []).map(
      (strategySummary) => {
        const operationsStats = strategyOperationsLookup.get(strategySummary.id);
        const liveTradeStats = strategyLiveTradesLookup.get(strategySummary.id);
        return {
          ...strategySummary,
          operationsTotal: operationsStats?.total ?? strategySummary.operationsTotal,
          operationsPending: operationsStats?.pending ?? strategySummary.operationsPending,
          liveTradesTotal: liveTradeStats?.total ?? strategySummary.liveTradesTotal,
          liveTradesActive: liveTradeStats?.active ?? strategySummary.liveTradesActive,
          liveTradesPending: liveTradeStats?.pending ?? strategySummary.liveTradesPending
        };
      }
    );
    let accountHistory = null;
    let accountHistoryError: string | null = null;
    const historyRequest: AccountPortfolioHistoryRequest = {
      timeframe: '1D'
    };
    const accountCreatedAt = tradingAccount.createdAt;
    if (accountCreatedAt instanceof Date && !Number.isNaN(accountCreatedAt.getTime())) {
      historyRequest.start = accountCreatedAt.toISOString();
    }
    if (historyRequest.start) {
      const now = new Date();
      historyRequest.end = now.toISOString();
      const startDate = new Date(historyRequest.start);
      if (!Number.isNaN(startDate.getTime()) && startDate.getTime() > now.getTime()) {
        historyRequest.start = historyRequest.end;
      }
    }
    try {
      accountHistory = await req.accountDataService.fetchPortfolioHistory(tradingAccount, historyRequest);
    } catch (historyError) {
      console.error('Failed to load account portfolio history:', historyError);
      accountHistoryError =
        historyError instanceof Error ? historyError.message : 'Unable to load portfolio history right now.';
    }
    const account =
    {
      id: tradingAccount.id,
      name: tradingAccount.name,
      provider: tradingAccount.provider,
      environment: tradingAccount.environment,
      createdAt: tradingAccount.createdAt,
      excludedTickers,
      excludedTickerCount: excludedTickers.length,
      excludedTickersInputValue: excludedTickers.join(' '),
      excludedKeywords,
      excludedKeywordCount: excludedKeywords.length,
      excludedKeywordsInputValue: excludedKeywords.join(' '),
      snapshot,
      snapshotBadge: getSnapshotBadgeMeta(snapshot),
      snapshotMessage: snapshot?.message ?? null,
      strategyPrefillParams,
      strategies: strategiesForAccount,
      uncoveredPositions,
      history: accountHistory,
      historyError: accountHistoryError
    };
    res.render('pages/account', {
      title: 'Account',
      page: 'account',
      user: req.user,
      account,
      operationsWindowDays: OPERATIONS_LOOKBACK_DAYS,
      success: extractQueryMessage(req.query.success),
      error: extractQueryMessage(req.query.error)
    });
  } catch (error) {
    console.error('Failed to load accounts:', error);
    res.status(500).render('pages/error', {
      title: 'Accounts',
      error: 'Unable to load accounts at this time.'
    });
  }
});

router.post('/', requireAuth, async (req: Request, res: Response) => {
  const { name, provider, environment, apiKey, apiSecret } = req.body;
  const trimmedName = typeof name === 'string' ? name.trim() : '';
  const trimmedProvider = typeof provider === 'string' ? provider.trim() : '';
  const normalizedEnvironment = normalizeEnvironment(environment);
  const cleanApiKey = typeof apiKey === 'string' ? apiKey.trim() : '';
  const cleanApiSecret = typeof apiSecret === 'string' ? apiSecret.trim() : '';
  const { text: excludedTickersInput, tickers: excludedTickers } = parseExcludedTickersInput(
    req.body?.excludedTickers
  );
  const { text: excludedKeywordsInput, keywords: excludedKeywords } = parseExcludedKeywordsInput(
    req.body?.excludedKeywords
  );
  const sanitizedForm = {
    name: trimmedName,
    provider: trimmedProvider,
    environment: normalizedEnvironment,
    apiKey: cleanApiKey,
    excludedTickers: excludedTickersInput,
    excludedKeywords: excludedKeywordsInput
  };

  if (!trimmedName) {
    return res.status(400).render('pages/create-account', {
      title: 'Add Account',
      page: 'accounts',
      user: req.user,
      form: sanitizedForm,
      error: 'Account name is required.'
    });
  }

  if (!trimmedProvider) {
    return res.status(400).render('pages/create-account', {
      title: 'Add Account',
      page: 'accounts',
      user: req.user,
      form: sanitizedForm,
      error: 'Provider is required.'
    });
  }

  if (!cleanApiKey || !cleanApiSecret) {
    return res.status(400).render('pages/create-account', {
      title: 'Add Account',
      page: 'accounts',
      user: req.user,
      form: sanitizedForm,
      error: 'API key and secret are required.'
    });
  }

  try {
    const userId = getReqUserId(req);
    const accountId = randomUUID();
    await req.db.accounts.createAccount({
      id: accountId,
      userId,
      name: trimmedName,
      provider: trimmedProvider,
      environment: normalizedEnvironment,
      excludedTickers,
      excludedKeywords,
      apiKey: cleanApiKey,
      apiSecret: cleanApiSecret
    });
    res.redirect(`/accounts/${accountId}?success=${encodeURIComponent('Account added successfully')}`);
  } catch (error) {
    console.error('Failed to create account:', error);
    res.status(500).render('pages/create-account', {
      title: 'Add Account',
      page: 'accounts',
      user: req.user,
      form: sanitizedForm,
      error: 'Failed to create account. Please try again.'
    });
  }
});

router.post<AccountParams>('/:id/restrictions', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const userId = getReqUserId(req);
    const account = await req.db.accounts.getAccountById(id, userId);
    if (!account) {
      return res.redirect('/?error=' + encodeURIComponent('Account not found or inaccessible.'));
    }

    const { tickers } = parseExcludedTickersInput(req.body?.excludedTickers);
    await req.db.accounts.updateAccountExcludedTickers(id, userId, tickers);
    const successMessage =
      tickers.length > 0
        ? `Blocked ${tickers.length} ticker${tickers.length === 1 ? '' : 's'} for ${account.name}.`
        : `Cleared ticker restrictions for ${account.name}.`;
    return res.redirect(`/accounts/${id}?success=${encodeURIComponent(successMessage)}`);
  } catch (error) {
    console.error('Failed to update account ticker restrictions:', error);
    return res.redirect(`/accounts/${id}?error=${encodeURIComponent('Unable to save ticker restrictions right now.')}`);
  }
});

router.post<AccountParams>('/:id/keyword-restrictions', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const userId = getReqUserId(req);
    const account = await req.db.accounts.getAccountById(id, userId);
    if (!account) {
      return res.redirect('/?error=' + encodeURIComponent('Account not found or inaccessible.'));
    }

    const { keywords } = parseExcludedKeywordsInput(req.body?.excludedKeywords);
    await req.db.accounts.updateAccountExcludedKeywords(id, userId, keywords);
    const successMessage =
      keywords.length > 0
        ? `Blocked ${keywords.length} keyword${keywords.length === 1 ? '' : 's'} for ${account.name}.`
        : `Cleared keyword restrictions for ${account.name}.`;
    return res.redirect(`/accounts/${id}?success=${encodeURIComponent(successMessage)}`);
  } catch (error) {
    console.error('Failed to update account keyword restrictions:', error);
    return res.redirect(`/accounts/${id}?error=${encodeURIComponent('Unable to save keyword restrictions right now.')}`);
  }
});

router.post<AccountParams>('/:id/reconcile-trades', requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const userId = getReqUserId(req);
    const account = await req.db.accounts.getAccountById(id, userId);
    if (!account) {
      return res.redirect('/?error=' + encodeURIComponent('Account not found or inaccessible.'));
    }

    const reconcilePending = req.jobScheduler.hasPendingJob(job => job.type === 'reconcile-trades');
    if (reconcilePending) {
      return res.redirect(`/accounts/${id}?error=${encodeURIComponent('Reconcile trades is already queued or running.')}`);
    }

    req.jobScheduler.scheduleJob('reconcile-trades', {
      description: `Manual reconcile for ${account.name}`,
      metadata: {
        trigger: 'account-untracked',
        accountId: id,
        skipPlanOperations: true
      }
    });

    return res.redirect(`/accounts/${id}?success=${encodeURIComponent('Reconcile trades job queued. Plan-operations will not run automatically.')}`);
  } catch (error) {
    console.error('Failed to queue reconcile trades job:', error);
    return res.redirect(`/accounts/${id}?error=${encodeURIComponent('Unable to reconcile trades right now.')}`);
  }
});

router.post<AccountParams>('/:id/delete', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = getReqUserId(req);
    const deleted = await req.db.accounts.deleteAccount(id, userId);
    if (!deleted) {
      return res.redirect('/?error=' + encodeURIComponent('Account not found or already deleted'));
    }
    res.redirect('/?success=' + encodeURIComponent('Account deleted successfully'));
  } catch (error) {
    console.error('Failed to delete account:', error);
    res.redirect('/?error=' + encodeURIComponent('Unable to delete account right now'));
  }
});

export default router;
