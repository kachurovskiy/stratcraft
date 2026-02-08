import type { QueryResultRow } from 'pg';
import type { BacktestScope, StrategyPerformance } from '../../../shared/types/StrategyTemplate';
import { DbClient, type QueryValue } from '../core/DbClient';
import { toNullableInteger, toNullableNumber } from '../core/valueParsers';
import type { BacktestResultRecord, TickerBacktestPerformanceRow } from '../types';

type BacktestResultsForTemplateRow = QueryResultRow & {
  id: string;
  strategy_id: string;
  strategy_name: string;
  start_date: Date;
  end_date: Date;
  period_days: number;
  period_months: number;
  performance: string;
  created_at: Date;
  ticker_scope: string;
};

type BacktestResultByIdRow = QueryResultRow & {
  id: string;
  strategy_id: string;
  start_date: Date;
  end_date: Date;
  period_days: number;
  period_months: number;
  initial_capital: number;
  final_portfolio_value: number;
  performance: string;
  daily_snapshots: string;
  tickers: string;
  ticker_scope: string;
  strategy_state: string | null;
  created_at: Date;
  owner_user_id: number | null;
};

type BacktestResultsRow = QueryResultRow & {
  id: string;
  start_date: Date;
  end_date: Date;
  period_days: number;
  period_months: number;
  initial_capital: number;
  final_portfolio_value: number;
  performance: string;
  daily_snapshots: string;
  tickers: string;
  strategy_state: string | null;
  created_at: Date;
  ticker_scope: string;
};

type StoredStrategyPerformanceRow = QueryResultRow & {
  id: string;
  performance: string;
  created_at: Date;
  start_date: Date;
  end_date: Date;
  ticker_scope: string;
};

type MedianSharpeRow = QueryResultRow & {
  strategy_id: string;
  median_sharpe: number | null;
};

type MedianCalmarRow = QueryResultRow & {
  strategy_id: string;
  median_calmar: number | null;
};

type StrategyCagrByPeriodRow = QueryResultRow & {
  strategy_id: string;
  period_months: number | null;
  cagr_value: number | null;
};

type AvailablePeriodRow = QueryResultRow & {
  period: number | null;
};

type TickerBacktestPerformanceDbRow = QueryResultRow & {
  strategy_id: string;
  strategy_name: string;
  period_months: number;
  backtest_result_id: string;
  backtest_created_at: Date;
  trade_count: number;
  avg_return_percent: number | null;
};

export class BacktestResultsRepo {
  constructor(private readonly db: DbClient) {}

  private parseBacktestTickers(jsonText: string): string[] {
    try {
      const parsed = JSON.parse(jsonText);
      if (!Array.isArray(parsed)) {
        return [];
      }
      return parsed
        .filter((value): value is string => typeof value === 'string')
        .map((value) => value.trim())
        .filter((value) => value.length > 0);
    } catch {
      return [];
    }
  }

  private parseDailySnapshots(jsonText: string): BacktestResultRecord['dailySnapshots'] {
    try {
      const parsed = JSON.parse(jsonText);
      if (!Array.isArray(parsed)) {
        return [];
      }

      return parsed
        .map((item): BacktestResultRecord['dailySnapshots'][number] | null => {
          if (!item || typeof item !== 'object') {
            return null;
          }
          const record = item as Record<string, unknown>;

          const dateValue = record.date;
          const date = dateValue instanceof Date ? dateValue : new Date(String(dateValue ?? ''));
          if (Number.isNaN(date.getTime())) {
            return null;
          }

          const cash = Number(record.cash ?? 0);
          const positionsValue = Number(record.positionsValue ?? record.positions_value ?? 0);

          const concurrentTradesValue = record.concurrentTrades ?? record.concurrent_trades;
          const concurrentTrades =
            concurrentTradesValue === null || concurrentTradesValue === undefined
              ? undefined
              : Number(concurrentTradesValue);

          const missedTradesValue = record.missedTradesDueToCash ?? record.missed_trades_due_to_cash;
          const missedTradesDueToCash =
            missedTradesValue === null || missedTradesValue === undefined ? undefined : Number(missedTradesValue);

          return {
            date,
            cash: Number.isFinite(cash) ? cash : 0,
            positionsValue: Number.isFinite(positionsValue) ? positionsValue : 0,
            concurrentTrades:
              concurrentTrades !== undefined && Number.isFinite(concurrentTrades)
                ? concurrentTrades
                : undefined,
            missedTradesDueToCash:
              missedTradesDueToCash !== undefined && Number.isFinite(missedTradesDueToCash)
                ? missedTradesDueToCash
                : undefined
          };
        })
        .filter((snapshot): snapshot is BacktestResultRecord['dailySnapshots'][number] => snapshot !== null);
    } catch {
      return [];
    }
  }

  private toBacktestScope(value: unknown): BacktestScope {
    if (value === 'validation' || value === 'training' || value === 'all' || value === 'live') {
      return value;
    }
    return 'training';
  }

  private mapMedianValue(value: unknown): number | null {
    return toNullableNumber(value);
  }

  private buildEmptyStrategyPerformance(): StrategyPerformance {
    return {
      totalTrades: 0,
      winningTrades: 0,
      losingTrades: 0,
      winRate: 0,
      totalReturn: 0,
      cagr: 0,
      sharpeRatio: 0,
      calmarRatio: 0,
      maxDrawdown: 0,
      maxDrawdownPercent: 0,
      avgTradeReturn: 0,
      bestTrade: 0,
      worstTrade: 0,
      totalTickers: 0,
      medianTradeDuration: 0,
      medianTradePnl: 0,
      medianTradePnlPercent: 0,
      medianConcurrentTrades: 0,
      avgTradeDuration: 0,
      avgTradePnl: 0,
      avgTradePnlPercent: 0,
      avgConcurrentTrades: 0,
      avgLosingPnl: 0,
      avgLosingPnlPercent: 0,
      avgWinningPnl: 0,
      avgWinningPnlPercent: 0,
      lastUpdated: new Date()
    };
  }

  private buildPerformanceScopeOrder(tickerScope?: BacktestScope): (BacktestScope | null)[] {
    if (tickerScope === 'all') {
      return [null];
    }
    if (tickerScope === 'live') {
      return ['live'];
    }
    if (tickerScope === 'validation') {
      return ['validation'];
    }
    if (tickerScope === 'training') {
      return ['training'];
    }
    return ['validation'];
  }

  private async fetchStoredStrategyPerformanceRow(
    strategyId: string,
    periodMonths: number | undefined,
    tickerScope: BacktestScope | null
  ): Promise<StoredStrategyPerformanceRow | null> {
    const params: QueryValue[] = [strategyId];
    let scopeFilterClause = '';
    let periodFilterClause = '';

    if (tickerScope && tickerScope !== 'all') {
      scopeFilterClause = ' AND ticker_scope = ?';
      params.push(tickerScope);
    }

    if (typeof periodMonths === 'number') {
      periodFilterClause = ' AND period_months = ?';
      params.push(periodMonths);
    }

    const row = await this.db.get<StoredStrategyPerformanceRow>(
      `
        SELECT id, performance, created_at, start_date, end_date, ticker_scope
        FROM backtest_results
        WHERE strategy_id = ?
        ${scopeFilterClause}
        ${periodFilterClause}
        ORDER BY created_at DESC
        LIMIT 1
      `,
      params
    );

    return row ?? null;
  }

  private mapStoredStrategyPerformanceRow(row: StoredStrategyPerformanceRow): StrategyPerformance {
    let performance: StrategyPerformance;
    try {
      performance = row.performance ? (JSON.parse(row.performance) as StrategyPerformance) : this.buildEmptyStrategyPerformance();
    } catch {
      performance = this.buildEmptyStrategyPerformance();
    }

    if (typeof performance.calmarRatio !== 'number' || !Number.isFinite(performance.calmarRatio)) {
      performance.calmarRatio = 0;
    }

    performance.lastUpdated = new Date(row.created_at);
    performance.backtestStartDate = new Date(row.start_date);
    performance.backtestEndDate = new Date(row.end_date);
    performance.backtestId = String(row.id);

    if (typeof row.ticker_scope === 'string' && row.ticker_scope.length > 0) {
      const normalizedScope = this.toBacktestScope(row.ticker_scope);
      if (normalizedScope) {
        performance.tickerScope = normalizedScope;
      }
    }

    return performance;
  }

  async getStoredStrategyPerformance(
    strategyId: string,
    options?: {
      periodMonths?: number;
      tickerScope?: BacktestScope;
    }
  ): Promise<StrategyPerformance> {
    try {
      const periodMonths =
        typeof options?.periodMonths === 'number' && Number.isFinite(options.periodMonths) ? options.periodMonths : undefined;
      const scopePreferences = this.buildPerformanceScopeOrder(options?.tickerScope);

      for (const scope of scopePreferences) {
        const row = await this.fetchStoredStrategyPerformanceRow(strategyId, periodMonths, scope);
        if (row) {
          return this.mapStoredStrategyPerformanceRow(row);
        }
      }
    } catch (error) {
      console.error(`Error retrieving stored performance for strategy ${strategyId}:`, error);
    }
    return this.buildEmptyStrategyPerformance();
  }

  async getBacktestResultsForTemplate(
    templateId: string,
    userId: number,
    options: { limit?: number } = {}
  ): Promise<
    Array<{
      id: string;
      strategyId: string;
      strategyName: string;
      startDate: Date;
      endDate: Date;
      periodDays: number;
      periodMonths: number;
      performance: StrategyPerformance | null;
      createdAt: Date;
      tickerScope: BacktestScope;
    }>
  > {
    const params: QueryValue[] = [templateId, userId];
    let limitClause = '';
    const limit = options?.limit;

    if (typeof limit === 'number' && Number.isFinite(limit) && limit > 0) {
      limitClause = 'LIMIT ?';
      params.push(limit);
    }

    const rows = await this.db.all<BacktestResultsForTemplateRow>(
      `
        SELECT br.id,
               br.strategy_id,
               s.name AS strategy_name,
               br.start_date,
               br.end_date,
               br.period_days,
               br.period_months,
               br.performance,
               br.created_at,
               br.ticker_scope
        FROM backtest_results br
        INNER JOIN strategies s ON s.id = br.strategy_id
        WHERE s.template_id = ?
          AND (s.user_id = ? OR s.user_id IS NULL)
          AND COALESCE(br.ticker_scope, 'training') != 'live'
        ORDER BY br.created_at DESC
        ${limitClause}
      `,
      params
    );

    return rows.map((row) => ({
      id: row.id,
      strategyId: row.strategy_id,
      strategyName: row.strategy_name,
      startDate: new Date(row.start_date),
      endDate: new Date(row.end_date),
      periodDays: toNullableInteger(row.period_days) ?? 0,
      periodMonths: toNullableInteger(row.period_months) ?? 0,
      performance: row.performance ? (JSON.parse(row.performance) as StrategyPerformance) : null,
      createdAt: new Date(row.created_at),
      tickerScope: this.toBacktestScope(row.ticker_scope ?? 'training')
    }));
  }

  async getBacktestResultById(backtestId: string, userId: number): Promise<BacktestResultRecord | null> {
    try {
      const row = await this.db.get<BacktestResultByIdRow>(
        `
          SELECT br.id,
                 br.strategy_id,
                 br.start_date,
                 br.end_date,
                 br.period_days,
                 br.period_months,
                 br.initial_capital,
                 br.final_portfolio_value,
                 br.performance,
                 br.daily_snapshots,
                 br.tickers,
                 br.ticker_scope,
                 br.strategy_state,
                 br.created_at,
                 s.user_id as owner_user_id
          FROM backtest_results br
          LEFT JOIN strategies s ON s.id = br.strategy_id
          WHERE br.id = ?
        `,
        [backtestId]
      );

      if (!row) {
        return null;
      }

      if (row.owner_user_id !== null && row.owner_user_id !== userId) {
        return null;
      }

      let parsedPerformance: StrategyPerformance | null = null;
      try {
        parsedPerformance = JSON.parse(row.performance) as StrategyPerformance;
      } catch {
        parsedPerformance = null;
      }

      const dailySnapshots = this.parseDailySnapshots(row.daily_snapshots);
      const tickers = this.parseBacktestTickers(row.tickers);

      let strategyState: unknown | undefined;
      if (row.strategy_state) {
        try {
          strategyState = JSON.parse(row.strategy_state);
        } catch {
          strategyState = undefined;
        }
      }

      return {
        id: row.id,
        strategyId: row.strategy_id,
        startDate: row.start_date,
        endDate: row.end_date,
        periodDays: row.period_days,
        periodMonths: row.period_months,
        initialCapital: row.initial_capital,
        finalPortfolioValue: row.final_portfolio_value,
        performance: parsedPerformance,
        dailySnapshots,
        tickers,
        strategyState,
        createdAt: row.created_at,
        tickerScope: this.toBacktestScope(row.ticker_scope)
      };
    } catch (error) {
      console.error(`Error retrieving backtest result ${backtestId}:`, error);
      return null;
    }
  }

  async getBacktestResults(strategyId: string, tickerScope?: BacktestScope): Promise<BacktestResultRecord[]> {
    try {
      const scope = tickerScope ?? 'training';
      const params: QueryValue[] = [strategyId];
      let scopeClause = '';

      if (scope && scope !== 'all') {
        scopeClause = ' AND ticker_scope = ?';
        params.push(scope);
      }

      const rows = await this.db.all<BacktestResultsRow>(
        `
          SELECT id, start_date, end_date, period_days, period_months, initial_capital, final_portfolio_value, performance, daily_snapshots, tickers, strategy_state, created_at, ticker_scope
          FROM backtest_results
          WHERE strategy_id = ?
          ${scopeClause}
          ORDER BY created_at DESC
        `,
        params
      );

      return rows.map((row) => {
        let performance: StrategyPerformance | null = null;
        try {
          performance = JSON.parse(row.performance) as StrategyPerformance;
        } catch {
          performance = null;
        }

        const dailySnapshots = this.parseDailySnapshots(row.daily_snapshots);
        const tickers = this.parseBacktestTickers(row.tickers);

        let strategyState: unknown | undefined;
        if (row.strategy_state) {
          try {
            strategyState = JSON.parse(row.strategy_state);
          } catch {
            strategyState = undefined;
          }
        }

        return {
          id: row.id,
          strategyId,
          startDate: row.start_date,
          endDate: row.end_date,
          periodDays: row.period_days,
          periodMonths: row.period_months,
          initialCapital: row.initial_capital,
          finalPortfolioValue: row.final_portfolio_value,
          performance,
          dailySnapshots,
          tickers,
          strategyState,
          createdAt: row.created_at,
          tickerScope: this.toBacktestScope(row.ticker_scope)
        };
      });
    } catch (error) {
      console.error(`Error retrieving backtest results for strategy ${strategyId}:`, error);
      return [];
    }
  }

  async getTickerBacktestPerformance(
    ticker: string,
    userId: number,
    options: { tickerScope?: BacktestScope } = {}
  ): Promise<TickerBacktestPerformanceRow[]> {
    const normalizedTicker = typeof ticker === 'string' ? ticker.trim().toUpperCase() : '';
    if (!normalizedTicker) {
      return [];
    }

    const params: QueryValue[] = [normalizedTicker, userId, userId];
    let scopeClause = '';
    if (options.tickerScope && options.tickerScope !== 'all') {
      scopeClause = " AND COALESCE(br.ticker_scope, 'training') = ?";
      params.push(options.tickerScope);
    }

    try {
      const rows = await this.db.all<TickerBacktestPerformanceDbRow>(
        `
          WITH target_trades AS (
            SELECT
              t.strategy_id,
              br.id AS backtest_result_id,
              br.period_months,
              COALESCE(br.ticker_scope, 'training') AS ticker_scope,
              br.created_at AS backtest_created_at,
              COALESCE(t.pnl, 0) AS pnl,
              ABS(COALESCE(t.quantity, 0) * COALESCE(t.price, 0)) AS trade_value
            FROM trades t
            INNER JOIN strategies s ON s.id = t.strategy_id
            INNER JOIN backtest_results br ON br.id = t.backtest_result_id
            WHERE t.backtest_result_id IS NOT NULL
              AND UPPER(t.ticker) = ?
              AND br.period_months IS NOT NULL
              AND br.period_months > 0
              AND LOWER(t.strategy_id) <> 'buy_and_hold'
              AND (s.user_id = ? OR s.user_id IS NULL)
              AND (COALESCE(t.user_id, s.user_id) = ? OR COALESCE(t.user_id, s.user_id) IS NULL)
              ${scopeClause}
          )
          SELECT
            tt.strategy_id,
            s.name AS strategy_name,
            tt.period_months,
            tt.backtest_result_id,
            tt.backtest_created_at,
            COUNT(*) FILTER (WHERE tt.trade_value > 0) AS trade_count,
            AVG(CASE WHEN tt.trade_value > 0 THEN (tt.pnl / tt.trade_value) * 100 END) AS avg_return_percent
          FROM target_trades tt
          INNER JOIN strategies s ON s.id = tt.strategy_id
          GROUP BY tt.strategy_id, s.name, tt.period_months, tt.backtest_result_id, tt.backtest_created_at
          HAVING COUNT(*) FILTER (WHERE tt.trade_value > 0) > 0
          ORDER BY s.name ASC, tt.period_months DESC, tt.backtest_created_at DESC
        `,
        params
      );

      return rows
        .map((row): TickerBacktestPerformanceRow | null => {
          const periodMonths = toNullableInteger(row.period_months) ?? 0;
          const tradeCount = toNullableInteger(row.trade_count) ?? 0;
          const avgReturn = toNullableNumber(row.avg_return_percent);

          const backtestResultId =
            typeof row.backtest_result_id === 'string' && row.backtest_result_id.length > 0 ? row.backtest_result_id : '';
          const backtestCreatedAt = row.backtest_created_at ? new Date(row.backtest_created_at) : null;

          if (periodMonths <= 0 || tradeCount <= 0 || !backtestResultId) {
            return null;
          }

          return {
            strategyId: row.strategy_id,
            strategyName: typeof row.strategy_name === 'string' && row.strategy_name.length > 0 ? row.strategy_name : 'Unnamed strategy',
            periodMonths,
            avgReturnPercent: avgReturn,
            tradeCount,
            backtestResultId,
            backtestCreatedAt
          };
        })
        .filter((row): row is TickerBacktestPerformanceRow => row !== null);
    } catch (error) {
      console.error(`Error retrieving backtest performance for ticker ${ticker}:`, error);
      return [];
    }
  }

  async getStrategySharpeMedians(strategyIds: string[]): Promise<Record<string, number | null>> {
    if (!Array.isArray(strategyIds) || strategyIds.length === 0) {
      return {};
    }

    const rows = await this.db.all<MedianSharpeRow>(
      `
        WITH sharpe_values AS (
          SELECT
            br.strategy_id,
            (NULLIF(br.performance, '')::jsonb ->> 'sharpeRatio')::DOUBLE PRECISION AS sharpe_value,
            (NULLIF(br.performance, '')::jsonb ->> 'totalTrades')::INTEGER AS total_trades
          FROM backtest_results br
          WHERE br.strategy_id = ANY(?::text[])
            AND COALESCE(br.ticker_scope, 'training') = 'validation'
        )
        SELECT
          strategy_id,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sharpe_value) AS median_sharpe
        FROM sharpe_values
        WHERE sharpe_value IS NOT NULL
          AND total_trades > 0
        GROUP BY strategy_id
      `,
      [strategyIds]
    );

    return rows.reduce((acc, row) => {
      const strategyId = row.strategy_id;
      if (typeof strategyId === 'string' && strategyId.length > 0) {
        acc[strategyId] = this.mapMedianValue(row.median_sharpe);
      }
      return acc;
    }, {} as Record<string, number | null>);
  }

  async getStrategyCalmarMedians(strategyIds: string[]): Promise<Record<string, number | null>> {
    if (!Array.isArray(strategyIds) || strategyIds.length === 0) {
      return {};
    }

    const rows = await this.db.all<MedianCalmarRow>(
      `
        WITH calmar_values AS (
          SELECT
            br.strategy_id,
            (NULLIF(br.performance, '')::jsonb ->> 'calmarRatio')::DOUBLE PRECISION AS calmar_value,
            (NULLIF(br.performance, '')::jsonb ->> 'totalTrades')::INTEGER AS total_trades
          FROM backtest_results br
          WHERE br.strategy_id = ANY(?::text[])
            AND COALESCE(br.ticker_scope, 'training') = 'validation'
        )
        SELECT
          strategy_id,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY calmar_value) AS median_calmar
        FROM calmar_values
        WHERE calmar_value IS NOT NULL
          AND total_trades > 0
        GROUP BY strategy_id
      `,
      [strategyIds]
    );

    return rows.reduce((acc, row) => {
      const strategyId = row.strategy_id;
      if (typeof strategyId === 'string' && strategyId.length > 0) {
        acc[strategyId] = this.mapMedianValue(row.median_calmar);
      }
      return acc;
    }, {} as Record<string, number | null>);
  }

  async getStrategyCagrByPeriod(
    strategyIds: string[],
    tickerScope: BacktestScope = 'validation'
  ): Promise<Record<string, { periodMonths: number; cagr: number }[]>> {
    if (!Array.isArray(strategyIds) || strategyIds.length === 0) {
      return {};
    }

    try {
      const rows = await this.db.all<StrategyCagrByPeriodRow>(
        `
          WITH ranked_results AS (
            SELECT
              br.strategy_id,
              br.period_months,
              (NULLIF(br.performance, '')::jsonb ->> 'cagr')::DOUBLE PRECISION AS cagr_value,
              (NULLIF(br.performance, '')::jsonb ->> 'totalTrades')::INTEGER AS total_trades,
              ROW_NUMBER() OVER (
                PARTITION BY br.strategy_id, br.period_months
                ORDER BY br.created_at DESC
              ) AS rn
            FROM backtest_results br
            WHERE br.strategy_id = ANY(?::text[])
              AND br.period_months IS NOT NULL
              AND br.period_months > 0
              AND COALESCE(br.ticker_scope, 'training') = ?
          )
          SELECT
            strategy_id,
            period_months,
            cagr_value
          FROM ranked_results
          WHERE rn = 1
            AND total_trades > 0
            AND cagr_value IS NOT NULL
        `,
        [strategyIds, tickerScope]
      );

      return rows.reduce((acc, row) => {
        const strategyId = row.strategy_id;
        const periodMonths = toNullableInteger(row.period_months);
        const cagr = toNullableNumber(row.cagr_value);

        if (typeof strategyId === 'string' && strategyId.length > 0 && periodMonths !== null && cagr !== null) {
          if (!acc[strategyId]) {
            acc[strategyId] = [];
          }
          acc[strategyId].push({ periodMonths, cagr });
        }

        return acc;
      }, {} as Record<string, { periodMonths: number; cagr: number }[]>);
    } catch (error) {
      console.error('Error getting CAGR by period for strategies:', error);
      return {};
    }
  }

  async getAvailableBacktestPeriods(): Promise<number[]> {
    try {
      const rows = await this.db.all<AvailablePeriodRow>(
        `
          SELECT DISTINCT br.period_months as period
          FROM backtest_results br
          INNER JOIN strategies s ON s.id = br.strategy_id
          WHERE br.period_months IS NOT NULL
            AND br.period_months > 0
            AND COALESCE(br.ticker_scope, 'training') = 'validation'
            AND COALESCE((NULLIF(br.performance, '')::jsonb ->> 'totalTrades')::INTEGER, 0) > 0
          ORDER BY period_months DESC
        `
      );
      return rows
        .map((row) => toNullableInteger(row.period))
        .filter((period): period is number => period !== null && period > 0);
    } catch (error) {
      console.error('Error retrieving available backtest periods:', error);
      return [];
    }
  }
}
