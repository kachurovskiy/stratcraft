import type { QueryResultRow } from 'pg';
import type {
  BacktestCacheHistoryRow,
  BacktestCacheLookupResult,
  BacktestCachePerformancePoint,
  BacktestCacheResultRow
} from '../types';
import { scoreBacktestParameters, type BacktestCacheRow, type BestBacktestParamsResult } from '../../scoring/paramScore';
import { DbClient, type QueryValue } from '../core/DbClient';
import { toInteger, toIsoString, toNullableInteger, toNullableNumber, toNumber } from '../core/valueParsers';
import { SettingsRepo } from './SettingsRepo';

type BacktestCacheDbRow = QueryResultRow & {
  id: string;
  template_id: string;
  parameters: string;
  sharpe_ratio: number;
  calmar_ratio: number;
  total_return: number;
  cagr: number;
  max_drawdown: number;
  max_drawdown_ratio: number;
  verify_sharpe_ratio?: number | null;
  verify_calmar_ratio?: number | null;
  verify_cagr?: number | null;
  verify_max_drawdown_ratio?: number | null;
  balance_training_sharpe_ratio?: number | null;
  balance_training_calmar_ratio?: number | null;
  balance_training_cagr?: number | null;
  balance_training_max_drawdown_ratio?: number | null;
  balance_validation_sharpe_ratio?: number | null;
  balance_validation_calmar_ratio?: number | null;
  balance_validation_cagr?: number | null;
  balance_validation_max_drawdown_ratio?: number | null;
  win_rate: number;
  total_trades: number;
  ticker_count?: number;
  start_date?: Date;
  end_date?: Date;
  period_days?: number;
  period_months?: number;
  duration_minutes?: number | null;
  tool?: string | null;
  top_abs_gain_ticker?: string | null;
  top_rel_gain_ticker?: string | null;
  created_at?: Date;
};

type BacktestCacheFullRow = BacktestCacheDbRow & {
  ticker_count: number;
  start_date: Date;
  end_date: Date;
  period_days: number;
  period_months: number;
  created_at: Date;
};

type BacktestCacheExportRow = {
  id: string;
  template_id: string;
  parameters: string;
  sharpe_ratio: number;
  calmar_ratio: number;
  total_return: number;
  cagr: number;
  max_drawdown: number;
  max_drawdown_ratio: number;
  verify_sharpe_ratio?: number | null;
  verify_calmar_ratio?: number | null;
  verify_cagr?: number | null;
  verify_max_drawdown_ratio?: number | null;
  balance_training_sharpe_ratio?: number | null;
  balance_training_calmar_ratio?: number | null;
  balance_training_cagr?: number | null;
  balance_training_max_drawdown_ratio?: number | null;
  balance_validation_sharpe_ratio?: number | null;
  balance_validation_calmar_ratio?: number | null;
  balance_validation_cagr?: number | null;
  balance_validation_max_drawdown_ratio?: number | null;
  win_rate: number;
  total_trades: number;
  ticker_count: number;
  start_date: string;
  end_date: string;
  period_days: number | null;
  period_months: number | null;
  duration_minutes: number | null;
  tool: string | null;
  top_abs_gain_ticker?: string | null;
  top_rel_gain_ticker?: string | null;
  created_at: string | null;
};

type BacktestCacheStatsRow = QueryResultRow & {
  total_count: number;
  unique_templates: number;
  avg_sharpe_ratio: number | null;
  avg_total_return: number | null;
  oldest_entry: Date | null;
  newest_entry: Date | null;
  min_ticker_count: number | null;
  max_ticker_count: number | null;
};

type BacktestCacheTemplateCountRow = QueryResultRow & {
  template_id: string;
  entry_count: number;
};

type TemplateMaxRow = QueryResultRow & { template_id: string; value: number | null };

type BestTemplateRow = QueryResultRow & { template_id: string; sharpe_ratio: number | null };

export class BacktestCacheRepo {
  constructor(
    private readonly db: DbClient,
    private readonly settings: SettingsRepo
  ) {}

  private serializeJsonConsistently(obj: Record<string, unknown>): string {
    return JSON.stringify(obj, Object.keys(obj).sort());
  }

  private parseBacktestCacheParameters(raw: unknown): Record<string, unknown> | null {
    if (raw === null || raw === undefined) {
      return null;
    }
    if (typeof raw === 'string') {
      const text = raw.trim();
      if (!text) {
        return null;
      }
      try {
        const parsed = JSON.parse(text);
        return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
      } catch {
        return null;
      }
    }
    if (raw instanceof Buffer) {
      return this.parseBacktestCacheParameters(raw.toString('utf8'));
    }
    if (typeof raw === 'object') {
      return { ...(raw as Record<string, unknown>) };
    }
    return null;
  }

  private mapBacktestCacheRow(row: BacktestCacheDbRow): BacktestCacheRow {
    const parameters = this.parseBacktestCacheParameters(row.parameters) ?? {};
    return {
      ...row,
      parameters,
      cagr: toNumber(row.cagr, 0),
      sharpe_ratio: toNumber(row.sharpe_ratio, 0),
      calmar_ratio: toNumber(row.calmar_ratio, 0),
      total_return: toNumber(row.total_return, 0),
      max_drawdown: toNumber(row.max_drawdown, 0),
      max_drawdown_ratio: toNumber(row.max_drawdown_ratio, 0),
      verify_sharpe_ratio: toNullableNumber(row.verify_sharpe_ratio),
      verify_calmar_ratio: toNullableNumber(row.verify_calmar_ratio),
      verify_cagr: toNullableNumber(row.verify_cagr),
      verify_max_drawdown_ratio: toNullableNumber(row.verify_max_drawdown_ratio),
      balance_training_sharpe_ratio: toNullableNumber(row.balance_training_sharpe_ratio),
      balance_training_calmar_ratio: toNullableNumber(row.balance_training_calmar_ratio),
      balance_training_cagr: toNullableNumber(row.balance_training_cagr),
      balance_training_max_drawdown_ratio: toNullableNumber(row.balance_training_max_drawdown_ratio),
      balance_validation_sharpe_ratio: toNullableNumber(row.balance_validation_sharpe_ratio),
      balance_validation_calmar_ratio: toNullableNumber(row.balance_validation_calmar_ratio),
      balance_validation_cagr: toNullableNumber(row.balance_validation_cagr),
      balance_validation_max_drawdown_ratio: toNullableNumber(row.balance_validation_max_drawdown_ratio),
      win_rate: toNumber(row.win_rate, 0),
      total_trades: Math.max(0, toInteger(row.total_trades, 0)),
      top_abs_gain_ticker:
        typeof row.top_abs_gain_ticker === 'string' && row.top_abs_gain_ticker.trim().length > 0
          ? row.top_abs_gain_ticker
          : null,
      top_rel_gain_ticker:
        typeof row.top_rel_gain_ticker === 'string' && row.top_rel_gain_ticker.trim().length > 0
          ? row.top_rel_gain_ticker
          : null
    };
  }

  async getBacktestCache(templateId: string, parameters: Record<string, unknown>): Promise<BacktestCacheLookupResult | null> {
    try {
      const parametersJson = this.serializeJsonConsistently(parameters);
      const cachedResult = await this.db.get<BacktestCacheFullRow>(
        `
          SELECT *
          FROM backtest_cache
          WHERE template_id = ? AND parameters = ?
        `,
        [templateId, parametersJson]
      );

      if (!cachedResult) {
        return null;
      }

      const parsedParameters = this.parseBacktestCacheParameters(cachedResult.parameters);
      if (!parsedParameters) {
        return null;
      }

      return {
        sharpeRatio: toNumber(cachedResult.sharpe_ratio, 0),
        calmarRatio: toNumber(cachedResult.calmar_ratio, 0),
        totalReturn: toNumber(cachedResult.total_return, 0),
        cagr: toNumber(cachedResult.cagr, 0),
        maxDrawdown: toNumber(cachedResult.max_drawdown, 0),
        maxDrawdownRatio: toNumber(cachedResult.max_drawdown_ratio, 0),
        verifySharpeRatio: toNullableNumber(cachedResult.verify_sharpe_ratio),
        verifyCalmarRatio: toNullableNumber(cachedResult.verify_calmar_ratio),
        verifyCagr: toNullableNumber(cachedResult.verify_cagr),
        verifyMaxDrawdownRatio: toNullableNumber(cachedResult.verify_max_drawdown_ratio),
        balanceTrainingSharpeRatio: toNullableNumber(cachedResult.balance_training_sharpe_ratio),
        balanceTrainingCalmarRatio: toNullableNumber(cachedResult.balance_training_calmar_ratio),
        balanceTrainingCagr: toNullableNumber(cachedResult.balance_training_cagr),
        balanceTrainingMaxDrawdownRatio: toNullableNumber(cachedResult.balance_training_max_drawdown_ratio),
        balanceValidationSharpeRatio: toNullableNumber(cachedResult.balance_validation_sharpe_ratio),
        balanceValidationCalmarRatio: toNullableNumber(cachedResult.balance_validation_calmar_ratio),
        balanceValidationCagr: toNullableNumber(cachedResult.balance_validation_cagr),
        balanceValidationMaxDrawdownRatio: toNullableNumber(cachedResult.balance_validation_max_drawdown_ratio),
        winRate: toNumber(cachedResult.win_rate, 0),
        totalTrades: toInteger(cachedResult.total_trades, 0),
        tickerCount: toInteger(cachedResult.ticker_count, 0),
        startDate: cachedResult.start_date.toISOString(),
        endDate: cachedResult.end_date.toISOString(),
        periodDays: cachedResult.period_days,
        periodMonths: cachedResult.period_months,
        parameters: parsedParameters
      };
    } catch (error) {
      console.error('Error getting backtest cache:', error);
      return null;
    }
  }

  async getBestParams(templateId: string): Promise<BestBacktestParamsResult | null> {
    try {
      const rows = await this.db.all<BacktestCacheDbRow>(
        `
          SELECT id, template_id, parameters, sharpe_ratio, calmar_ratio, total_return, cagr, max_drawdown,
                 max_drawdown_ratio, verify_sharpe_ratio, verify_calmar_ratio, verify_cagr, verify_max_drawdown_ratio,
                 balance_training_sharpe_ratio, balance_training_calmar_ratio, balance_training_cagr,
                 balance_training_max_drawdown_ratio, balance_validation_sharpe_ratio,
                 balance_validation_calmar_ratio, balance_validation_cagr, balance_validation_max_drawdown_ratio,
                 win_rate, total_trades, top_abs_gain_ticker, top_rel_gain_ticker
          FROM backtest_cache
          WHERE template_id = ?
          ORDER BY created_at DESC
        `,
        [templateId]
      );

      if (!rows.length) {
        return null;
      }

      const normalizedRows = rows.map((row) => this.mapBacktestCacheRow(row));
      const scored = await scoreBacktestParameters(normalizedRows, { settingsRepo: this.settings });
      return scored.scored[0] ?? null;
    } catch (error) {
      console.error(`Error getting best params for template ${templateId}:`, error);
      return null;
    }
  }

  async getBestParamsByTemplateIds(templateIds: string[]): Promise<Record<string, BestBacktestParamsResult>> {
    try {
      const rows = await this.db.all<BacktestCacheDbRow>(
        `
          SELECT id, template_id, parameters, sharpe_ratio, calmar_ratio, total_return, cagr, max_drawdown,
                 max_drawdown_ratio, verify_sharpe_ratio, verify_calmar_ratio, verify_cagr, verify_max_drawdown_ratio,
                 balance_training_sharpe_ratio, balance_training_calmar_ratio, balance_training_cagr,
                 balance_training_max_drawdown_ratio, balance_validation_sharpe_ratio,
                 balance_validation_calmar_ratio, balance_validation_cagr, balance_validation_max_drawdown_ratio,
                 win_rate, total_trades, top_abs_gain_ticker, top_rel_gain_ticker
          FROM backtest_cache
          WHERE template_id = ANY(?::text[])
          ORDER BY template_id, created_at DESC
        `,
        [templateIds]
      );

      const rowsByTemplate = new Map<string, BacktestCacheRow[]>();
      rows.forEach((row) => {
        const normalized = this.mapBacktestCacheRow(row);
        const existing = rowsByTemplate.get(row.template_id);
        if (existing) {
          existing.push(normalized);
        } else {
          rowsByTemplate.set(row.template_id, [normalized]);
        }
      });

      const results: Record<string, BestBacktestParamsResult> = {};
      for (const [templateId, templateRows] of rowsByTemplate.entries()) {
        if (!templateRows.length) {
          continue;
        }
        const scored = await scoreBacktestParameters(templateRows, { settingsRepo: this.settings });
        const best = scored.scored[0] ?? null;
        if (best) {
          results[templateId] = best;
        }
      }

      return results;
    } catch (error) {
      console.error('Error getting best params by template:', error);
      return {};
    }
  }

  async getBestTemplateBySharpe(
    options?: { periodMonths?: number }
  ): Promise<{ templateId: string; sharpeRatio: number | null } | null> {
    const filters: string[] = [];
    const params: QueryValue[] = [];

    if (options?.periodMonths && Number.isFinite(options.periodMonths) && options.periodMonths > 0) {
      filters.push('period_months = ?');
      params.push(options.periodMonths);
    }

    const whereClause = filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : '';

    try {
      const row = await this.db.get<BestTemplateRow>(
        `
          SELECT template_id, sharpe_ratio
          FROM backtest_cache
          ${whereClause}
          ORDER BY sharpe_ratio DESC
          LIMIT 1
        `,
        params
      );

      if (row && row.template_id) {
        return {
          templateId: row.template_id,
          sharpeRatio: toNullableNumber(row.sharpe_ratio)
        };
      }

      return null;
    } catch (error) {
      console.error('Error getting best template by Sharpe ratio:', error);
      return null;
    }
  }

  private calculatePeriodMetrics(start: string, end: string): { periodDays: number; periodMonths: number } {
    const startDate = new Date(start);
    const endDate = new Date(end);
    if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
      return { periodDays: 0, periodMonths: 0 };
    }

    const msPerDay = 24 * 60 * 60 * 1000;
    const startUtc = Date.UTC(startDate.getUTCFullYear(), startDate.getUTCMonth(), startDate.getUTCDate());
    const endUtc = Date.UTC(endDate.getUTCFullYear(), endDate.getUTCMonth(), endDate.getUTCDate());

    const diffMs = endUtc - startUtc;
    if (diffMs < 0) {
      return { periodDays: 0, periodMonths: 0 };
    }

    const dayDiff = Math.round(diffMs / msPerDay);
    const periodDays = Math.max(1, dayDiff);
    const periodMonths = Math.max(0, Math.round(periodDays / 30.4));

    return { periodDays, periodMonths };
  }

  private generateBacktestCacheId(
    templateId: string,
    parameters: Record<string, unknown>,
    startDate: string,
    endDate: string,
    tickerCount: number
  ): string {
    const parametersJson = this.serializeJsonConsistently(parameters);
    const keyComponents = `${templateId}|${parametersJson}|${startDate}|${endDate}|${tickerCount}`;

    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const crypto = require('crypto');
    const hash = crypto.createHash('md5').update(keyComponents).digest('hex').substring(0, 8);

    const startDateFormatted = new Date(startDate).toISOString().split('T')[0];
    const endDateFormatted = new Date(endDate).toISOString().split('T')[0];

    return `cache_${templateId}_${startDateFormatted}_${endDateFormatted}_${tickerCount}t_${hash}`;
  }

  async storeBacktestCache(
    templateId: string,
    parameters: Record<string, unknown>,
    sharpeRatio: number,
    calmarRatio: number,
    totalReturn: number,
    cagr: number,
    maxDrawdown: number,
    maxDrawdownRatio: number,
    winRate: number,
    totalTrades: number,
    tickerCount: number,
    startDate: string,
    endDate: string,
    durationMinutes: number,
    tool: string,
    topAbsoluteGainTicker?: string | null,
    topRelativeGainTicker?: string | null
  ): Promise<void> {
    if (!Number.isFinite(totalTrades) || totalTrades <= 0) {
      return;
    }

    try {
      const id = this.generateBacktestCacheId(templateId, parameters, startDate, endDate, tickerCount);
      const parametersJson = this.serializeJsonConsistently(parameters);

      const startDateTime = new Date(startDate).toISOString();
      const endDateTime = new Date(endDate).toISOString();
      const { periodDays, periodMonths } = this.calculatePeriodMetrics(startDateTime, endDateTime);
      const createdAt = new Date().toISOString();

      await this.db.run(
        `
          INSERT INTO backtest_cache
          (id, template_id, parameters, sharpe_ratio, calmar_ratio, total_return, cagr, max_drawdown, max_drawdown_ratio, win_rate, total_trades, ticker_count, start_date, end_date, period_days, period_months, duration_minutes, tool, top_abs_gain_ticker, top_rel_gain_ticker, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (id) DO UPDATE
          SET template_id = EXCLUDED.template_id,
              parameters = EXCLUDED.parameters,
              sharpe_ratio = EXCLUDED.sharpe_ratio,
              calmar_ratio = EXCLUDED.calmar_ratio,
              total_return = EXCLUDED.total_return,
              cagr = EXCLUDED.cagr,
              max_drawdown = EXCLUDED.max_drawdown,
              max_drawdown_ratio = EXCLUDED.max_drawdown_ratio,
              win_rate = EXCLUDED.win_rate,
              total_trades = EXCLUDED.total_trades,
              ticker_count = EXCLUDED.ticker_count,
              start_date = EXCLUDED.start_date,
              end_date = EXCLUDED.end_date,
              period_days = EXCLUDED.period_days,
              period_months = EXCLUDED.period_months,
              duration_minutes = EXCLUDED.duration_minutes,
              tool = EXCLUDED.tool,
              top_abs_gain_ticker = EXCLUDED.top_abs_gain_ticker,
              top_rel_gain_ticker = EXCLUDED.top_rel_gain_ticker
        `,
        [
          id,
          templateId,
          parametersJson,
          sharpeRatio,
          calmarRatio,
          totalReturn,
          cagr,
          maxDrawdown,
          maxDrawdownRatio,
          winRate,
          totalTrades,
          tickerCount,
          startDateTime,
          endDateTime,
          periodDays,
          periodMonths,
          durationMinutes,
          tool,
          topAbsoluteGainTicker ?? null,
          topRelativeGainTicker ?? null,
          createdAt
        ]
      );
    } catch (error) {
      console.error('Error storing backtest cache:', error);
      throw error;
    }
  }

  async getBacktestCacheStats(): Promise<{
    totalCount: number;
    uniqueTemplates: number;
    avgSharpeRatio: number;
    avgTotalReturn: number;
    oldestEntry: Date | null;
    newestEntry: Date | null;
    minTickerCount: number;
    maxTickerCount: number;
  }> {
    try {
      const stats = await this.db.get<BacktestCacheStatsRow>(
        `
          SELECT
            COUNT(*) as total_count,
            COUNT(DISTINCT template_id) as unique_templates,
            AVG(sharpe_ratio) as avg_sharpe_ratio,
            AVG(total_return) as avg_total_return,
            MIN(created_at) as oldest_entry,
            MAX(created_at) as newest_entry,
            MIN(ticker_count) as min_ticker_count,
            MAX(ticker_count) as max_ticker_count
          FROM backtest_cache
        `
      );

      return {
        totalCount: Math.max(0, toInteger(stats?.total_count, 0)),
        uniqueTemplates: Math.max(0, toInteger(stats?.unique_templates, 0)),
        avgSharpeRatio: toNumber(stats?.avg_sharpe_ratio, 0),
        avgTotalReturn: toNumber(stats?.avg_total_return, 0),
        oldestEntry: stats?.oldest_entry ? new Date(stats.oldest_entry) : null,
        newestEntry: stats?.newest_entry ? new Date(stats.newest_entry) : null,
        minTickerCount: Math.max(0, toInteger(stats?.min_ticker_count, 0)),
        maxTickerCount: Math.max(0, toInteger(stats?.max_ticker_count, 0))
      };
    } catch (error) {
      console.error('Error getting backtest cache stats:', error);
      return {
        totalCount: 0,
        uniqueTemplates: 0,
        avgSharpeRatio: 0,
        avgTotalReturn: 0,
        oldestEntry: null,
        newestEntry: null,
        minTickerCount: 0,
        maxTickerCount: 0
      };
    }
  }

  async getBacktestCacheTemplateCounts(): Promise<{ templateId: string; count: number }[]> {
    try {
      const rows = await this.db.all<BacktestCacheTemplateCountRow>(
        `
          SELECT template_id, COUNT(*) as entry_count
          FROM backtest_cache
          GROUP BY template_id
          ORDER BY template_id ASC
        `
      );

      return rows.map((row) => ({
        templateId: row.template_id,
        count: Math.max(0, toInteger(row.entry_count, 0))
      }));
    } catch (error) {
      console.error('Error getting backtest cache counts by template:', error);
      return [];
    }
  }

  async getLatestBacktestCacheResults(limit: number = 10): Promise<BacktestCacheResultRow[]> {
    try {
      const results = await this.db.all<BacktestCacheFullRow>(
        `
          SELECT *
          FROM backtest_cache
          ORDER BY created_at DESC
          LIMIT ?
        `,
        [limit]
      );

      return results.map((row) => ({
        ...row,
        parameters: this.parseBacktestCacheParameters(row.parameters) ?? {},
        startDate: row.start_date,
        endDate: row.end_date,
        createdAt: row.created_at,
        periodDays: row.period_days,
        periodMonths: row.period_months
      }));
    } catch (error) {
      console.error('Error getting latest backtest cache results:', error);
      return [];
    }
  }

  async getBestBacktestCacheSharpeByTemplate(): Promise<Record<string, number>> {
    try {
      const rows = await this.db.all<TemplateMaxRow>(
        `
          SELECT template_id, MAX(sharpe_ratio) as value
          FROM backtest_cache
          GROUP BY template_id
        `
      );

      return rows.reduce((acc, row) => {
        acc[row.template_id] = toNumber(row.value, 0);
        return acc;
      }, {} as Record<string, number>);
    } catch (error) {
      console.error('Error getting best backtest cache Sharpe ratios by template:', error);
      return {};
    }
  }

  async getBestBacktestCacheCalmarByTemplate(): Promise<
    Record<
      string,
      {
        calmarRatio: number | null;
        topAbsoluteGainTicker: string | null;
        topRelativeGainTicker: string | null;
      }
    >
  > {
    try {
      const rows = await this.db.all<
        QueryResultRow & {
          template_id: string;
          calmar_ratio: number;
          top_abs_gain_ticker: string | null;
          top_rel_gain_ticker: string | null;
        }
      >(
        `
          SELECT DISTINCT ON (template_id)
            template_id,
            calmar_ratio,
            sharpe_ratio,
            top_abs_gain_ticker,
            top_rel_gain_ticker
          FROM backtest_cache
          ORDER BY template_id, calmar_ratio DESC, sharpe_ratio DESC
        `
      );

      return rows.reduce((acc, row) => {
        acc[row.template_id] = {
          calmarRatio: toNullableNumber(row.calmar_ratio),
          topAbsoluteGainTicker:
            typeof row.top_abs_gain_ticker === 'string' && row.top_abs_gain_ticker.trim().length > 0
              ? row.top_abs_gain_ticker
              : null,
          topRelativeGainTicker:
            typeof row.top_rel_gain_ticker === 'string' && row.top_rel_gain_ticker.trim().length > 0
              ? row.top_rel_gain_ticker
              : null
        };
        return acc;
      }, {} as Record<string, { calmarRatio: number | null; topAbsoluteGainTicker: string | null; topRelativeGainTicker: string | null }>);
    } catch (error) {
      console.error('Error getting best backtest cache Calmar ratios by template:', error);
      return {};
    }
  }

  async getBestBacktestCacheCagrByTemplate(): Promise<Record<string, number>> {
    try {
      const rows = await this.db.all<TemplateMaxRow>(
        `
          SELECT template_id, MAX(cagr) as value
          FROM backtest_cache
          GROUP BY template_id
        `
      );

      return rows.reduce((acc, row) => {
        acc[row.template_id] = toNumber(row.value, 0);
        return acc;
      }, {} as Record<string, number>);
    } catch (error) {
      console.error('Error getting best backtest cache CAGR by template:', error);
      return {};
    }
  }

  async getTopBacktestCacheEntriesByTemplate(templateIds: string[]): Promise<BacktestCachePerformancePoint[]> {
    if (!Array.isArray(templateIds) || templateIds.length === 0) {
      return [];
    }

    try {
      const rows = await this.db.all<
        QueryResultRow & {
          id: string;
          template_id: string;
          sharpe_ratio: number;
          calmar_ratio: number;
          verify_sharpe_ratio: number | null;
          verify_calmar_ratio: number | null;
          cagr: number;
          verify_cagr: number | null;
          max_drawdown_ratio: number;
          verify_max_drawdown_ratio: number | null;
          total_trades: number;
          ticker_count: number;
          created_at: Date;
        }
      >(
        `
          SELECT
            id,
            template_id,
            sharpe_ratio,
            calmar_ratio,
            verify_sharpe_ratio,
            verify_calmar_ratio,
            cagr,
            verify_cagr,
            max_drawdown_ratio,
            verify_max_drawdown_ratio,
            total_trades,
            ticker_count,
            created_at
          FROM backtest_cache
          WHERE template_id = ANY(?::text[])
            AND sharpe_ratio IS NOT NULL
            AND calmar_ratio IS NOT NULL
            AND cagr IS NOT NULL
          ORDER BY template_id,
                   sharpe_ratio DESC NULLS LAST,
                   calmar_ratio DESC NULLS LAST,
                   cagr DESC NULLS LAST,
                   created_at DESC
        `,
        [templateIds]
      );

      return rows.map((row) => ({
        id: row.id,
        templateId: row.template_id,
        sharpeRatio: toNullableNumber(row.sharpe_ratio),
        calmarRatio: toNullableNumber(row.calmar_ratio),
        verifySharpeRatio: toNullableNumber(row.verify_sharpe_ratio),
        verifyCalmarRatio: toNullableNumber(row.verify_calmar_ratio),
        cagr: toNullableNumber(row.cagr),
        verifyCagr: toNullableNumber(row.verify_cagr),
        maxDrawdownRatio: toNullableNumber(row.max_drawdown_ratio),
        verifyMaxDrawdownRatio: toNullableNumber(row.verify_max_drawdown_ratio),
        totalTrades: toNullableNumber(row.total_trades),
        tickerCount: toNullableNumber(row.ticker_count),
        createdAt: row.created_at ? new Date(row.created_at) : null
      }));
    } catch (error) {
      console.error('Error getting top backtest cache entries by template:', error);
      return [];
    }
  }

  async getTopBacktestCacheResultsBySharpe(limit: number = 20): Promise<BacktestCacheResultRow[]> {
    try {
      const results = await this.db.all<BacktestCacheFullRow>(
        `
          SELECT * FROM backtest_cache
          ORDER BY sharpe_ratio DESC
          LIMIT ?
        `,
        [limit]
      );

      return results.map((row) => ({
        ...row,
        parameters: this.parseBacktestCacheParameters(row.parameters) ?? {},
        startDate: row.start_date,
        endDate: row.end_date,
        createdAt: row.created_at,
        periodDays: row.period_days,
        periodMonths: row.period_months
      }));
    } catch (error) {
      console.error('Error getting top backtest cache results by Sharpe ratio:', error);
      return [];
    }
  }

  async getTopBacktestCacheResultsByTemplate(templateId: string, limit: number = 5): Promise<BacktestCacheResultRow[]> {
    try {
      const results = await this.db.all<BacktestCacheFullRow>(
        `
          SELECT * FROM backtest_cache
          WHERE template_id = ?
          ORDER BY sharpe_ratio DESC
          LIMIT ?
        `,
        [templateId, limit]
      );

      return results.map((row) => ({
        ...row,
        parameters: this.parseBacktestCacheParameters(row.parameters) ?? {},
        startDate: row.start_date,
        endDate: row.end_date,
        createdAt: row.created_at,
        periodDays: row.period_days,
        periodMonths: row.period_months
      }));
    } catch (error) {
      console.error(`Error getting top backtest cache results for template ${templateId}:`, error);
      return [];
    }
  }

  async getBacktestCacheResultsForTemplate(templateId: string): Promise<BacktestCacheHistoryRow[]> {
    try {
      const results = await this.db.all<BacktestCacheFullRow>(
        `
          SELECT *
          FROM backtest_cache
          WHERE template_id = ?
          ORDER BY created_at ASC
        `,
        [templateId]
      );

      return results.map((row) => {
        const normalized = this.mapBacktestCacheRow(row);
        const parameters = normalized.parameters ?? {};
        const totalTrades = normalized.total_trades ?? 0;
        const periodDays = row.period_days;
        const periodMonths = row.period_months;
        return {
          ...normalized,
          id: row.id,
          template_id: row.template_id,
          parameters,
          total_trades: totalTrades,
          ticker_count: toInteger(row.ticker_count),
          duration_minutes: toNullableNumber(row.duration_minutes),
          tool: row.tool ?? null,
          period_days: periodDays,
          period_months: periodMonths,
          periodDays,
          periodMonths,
          startDate: row.start_date,
          endDate: row.end_date,
          createdAt: row.created_at
        };
      });
    } catch (error) {
      console.error(`Error getting backtest cache history for template ${templateId}:`, error);
      return [];
    }
  }

  async clearBacktestCache(): Promise<number> {
    try {
      const result = await this.db.run('DELETE FROM backtest_cache');
      return result.changes || 0;
    } catch (error) {
      console.error('Error clearing backtest cache:', error);
      throw error;
    }
  }

  async clearBacktestCacheByTemplate(templateId: string): Promise<number> {
    try {
      const result = await this.db.run('DELETE FROM backtest_cache WHERE template_id = ?', [templateId]);
      return result.changes || 0;
    } catch (error) {
      console.error(`Error clearing backtest cache for template ${templateId}:`, error);
      throw error;
    }
  }

  private mapBacktestCacheRowForExport(row: BacktestCacheDbRow): BacktestCacheExportRow {
    const startDate = toIsoString(row.start_date);
    const endDate = toIsoString(row.end_date);

    if (!startDate || !endDate) {
      throw new Error(`Backtest cache entry ${row?.id ?? 'unknown'} is missing start or end date`);
    }

    return {
      id: typeof row.id === 'string' ? row.id : String(row.id ?? ''),
      template_id: typeof row.template_id === 'string' ? row.template_id : String(row.template_id ?? ''),
      parameters: typeof row.parameters === 'string' ? row.parameters : JSON.stringify(row.parameters ?? {}),
      sharpe_ratio: toNumber(row.sharpe_ratio, 0),
      calmar_ratio: toNumber(row.calmar_ratio, 0),
      total_return: toNumber(row.total_return, 0),
      cagr: toNumber(row.cagr, 0),
      max_drawdown: toNumber(row.max_drawdown, 0),
      max_drawdown_ratio: toNumber(row.max_drawdown_ratio, 0),
      verify_sharpe_ratio: toNullableNumber(row.verify_sharpe_ratio),
      verify_calmar_ratio: toNullableNumber(row.verify_calmar_ratio),
      verify_cagr: toNullableNumber(row.verify_cagr),
      verify_max_drawdown_ratio: toNullableNumber(row.verify_max_drawdown_ratio),
      balance_training_sharpe_ratio: toNullableNumber(row.balance_training_sharpe_ratio),
      balance_training_calmar_ratio: toNullableNumber(row.balance_training_calmar_ratio),
      balance_training_cagr: toNullableNumber(row.balance_training_cagr),
      balance_training_max_drawdown_ratio: toNullableNumber(row.balance_training_max_drawdown_ratio),
      balance_validation_sharpe_ratio: toNullableNumber(row.balance_validation_sharpe_ratio),
      balance_validation_calmar_ratio: toNullableNumber(row.balance_validation_calmar_ratio),
      balance_validation_cagr: toNullableNumber(row.balance_validation_cagr),
      balance_validation_max_drawdown_ratio: toNullableNumber(row.balance_validation_max_drawdown_ratio),
      win_rate: toNumber(row.win_rate, 0),
      total_trades: Math.max(0, toInteger(row.total_trades, 0)),
      ticker_count: Math.max(0, toInteger(row.ticker_count, 0)),
      start_date: startDate,
      end_date: endDate,
      period_days: toNullableInteger(row.period_days),
      period_months: toNullableInteger(row.period_months),
      duration_minutes: toNullableNumber(row.duration_minutes),
      tool: typeof row.tool === 'string' && row.tool.trim().length > 0 ? row.tool : null,
      top_abs_gain_ticker:
        typeof row.top_abs_gain_ticker === 'string' && row.top_abs_gain_ticker.trim().length > 0
          ? row.top_abs_gain_ticker
          : null,
      top_rel_gain_ticker:
        typeof row.top_rel_gain_ticker === 'string' && row.top_rel_gain_ticker.trim().length > 0
          ? row.top_rel_gain_ticker
          : null,
      created_at: toIsoString(row.created_at)
    };
  }

  async getAllBacktestCacheEntries(): Promise<BacktestCacheExportRow[]> {
    try {
      const rows = await this.db.all<BacktestCacheDbRow>(
        `
          SELECT id, template_id, parameters, sharpe_ratio, calmar_ratio, total_return, cagr,
                 max_drawdown, max_drawdown_ratio, verify_sharpe_ratio, verify_calmar_ratio,
                 verify_cagr, verify_max_drawdown_ratio, balance_training_sharpe_ratio,
                 balance_training_calmar_ratio, balance_training_cagr, balance_training_max_drawdown_ratio,
                 balance_validation_sharpe_ratio, balance_validation_calmar_ratio,
                 balance_validation_cagr, balance_validation_max_drawdown_ratio, win_rate, total_trades, ticker_count,
                 start_date, end_date, period_days, period_months,
                 duration_minutes, tool, top_abs_gain_ticker,
                 top_rel_gain_ticker, created_at
          FROM backtest_cache
          ORDER BY created_at ASC, id ASC
        `
      );

      return rows.map((row) => this.mapBacktestCacheRowForExport(row));
    } catch (error) {
      console.error('Error retrieving backtest cache entries for export:', error);
      throw error;
    }
  }

  private sanitizeBacktestCacheImportRow(entry: Partial<BacktestCacheExportRow> | null | undefined): BacktestCacheExportRow | null {
    if (!entry || typeof entry !== 'object') {
      return null;
    }

    const id = typeof entry.id === 'string' ? entry.id.trim() : '';
    const templateId = typeof entry.template_id === 'string' ? entry.template_id.trim() : '';
    const parameters =
      typeof entry.parameters === 'string' ? entry.parameters : JSON.stringify(entry.parameters ?? {});
    const startDate = toIsoString(entry.start_date);
    const endDate = toIsoString(entry.end_date);

    if (!id || !templateId || !parameters || !startDate || !endDate) {
      return null;
    }

    return {
      id,
      template_id: templateId,
      parameters,
      sharpe_ratio: toNumber(entry.sharpe_ratio, 0),
      calmar_ratio: toNumber(entry.calmar_ratio, 0),
      total_return: toNumber(entry.total_return, 0),
      cagr: toNumber(entry.cagr, 0),
      max_drawdown: toNumber(entry.max_drawdown, 0),
      max_drawdown_ratio: toNumber(entry.max_drawdown_ratio, 0),
      verify_sharpe_ratio: toNullableNumber(entry.verify_sharpe_ratio),
      verify_calmar_ratio: toNullableNumber(entry.verify_calmar_ratio),
      verify_cagr: toNullableNumber(entry.verify_cagr),
      verify_max_drawdown_ratio: toNullableNumber(entry.verify_max_drawdown_ratio),
      balance_training_sharpe_ratio: toNullableNumber(entry.balance_training_sharpe_ratio),
      balance_training_calmar_ratio: toNullableNumber(entry.balance_training_calmar_ratio),
      balance_training_cagr: toNullableNumber(entry.balance_training_cagr),
      balance_training_max_drawdown_ratio: toNullableNumber(entry.balance_training_max_drawdown_ratio),
      balance_validation_sharpe_ratio: toNullableNumber(entry.balance_validation_sharpe_ratio),
      balance_validation_calmar_ratio: toNullableNumber(entry.balance_validation_calmar_ratio),
      balance_validation_cagr: toNullableNumber(entry.balance_validation_cagr),
      balance_validation_max_drawdown_ratio: toNullableNumber(entry.balance_validation_max_drawdown_ratio),
      win_rate: toNumber(entry.win_rate, 0),
      total_trades: Math.max(0, toInteger(entry.total_trades, 0)),
      ticker_count: Math.max(0, toInteger(entry.ticker_count, 0)),
      start_date: startDate,
      end_date: endDate,
      period_days: toNullableInteger(entry.period_days),
      period_months: toNullableInteger(entry.period_months),
      duration_minutes: toNullableNumber(entry.duration_minutes),
      tool: typeof entry.tool === 'string' && entry.tool.trim().length > 0 ? entry.tool.trim() : null,
      top_abs_gain_ticker:
        typeof entry.top_abs_gain_ticker === 'string' && entry.top_abs_gain_ticker.trim().length > 0
          ? entry.top_abs_gain_ticker.trim()
          : null,
      top_rel_gain_ticker:
        typeof entry.top_rel_gain_ticker === 'string' && entry.top_rel_gain_ticker.trim().length > 0
          ? entry.top_rel_gain_ticker.trim()
          : null,
      created_at: toIsoString(entry.created_at)
    };
  }

  async replaceBacktestCacheEntries(
    entries: Partial<BacktestCacheExportRow>[]
  ): Promise<{ inserted: number; skipped: number }> {
    const sanitizedEntries = Array.isArray(entries)
      ? entries
          .map((entry) => this.sanitizeBacktestCacheImportRow(entry))
          .filter((entry): entry is BacktestCacheExportRow => entry !== null)
      : [];

    const skipped = Array.isArray(entries) ? entries.length - sanitizedEntries.length : 0;

    await this.db.withTransaction(async (client) => {
      await this.db.run('DELETE FROM backtest_cache', [], client);
      for (const entry of sanitizedEntries) {
        await this.db.run(
          `INSERT INTO backtest_cache (
            id, template_id, parameters, sharpe_ratio, calmar_ratio, total_return, cagr,
            max_drawdown, max_drawdown_ratio, verify_sharpe_ratio, verify_calmar_ratio,
            verify_cagr, verify_max_drawdown_ratio, balance_training_sharpe_ratio,
            balance_training_calmar_ratio, balance_training_cagr, balance_training_max_drawdown_ratio,
            balance_validation_sharpe_ratio, balance_validation_calmar_ratio,
            balance_validation_cagr, balance_validation_max_drawdown_ratio, win_rate, total_trades, ticker_count,
            start_date, end_date, period_days, period_months,
            duration_minutes, tool, top_abs_gain_ticker,
            top_rel_gain_ticker, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            entry.id,
            entry.template_id,
            entry.parameters,
            entry.sharpe_ratio,
            entry.calmar_ratio,
            entry.total_return,
            entry.cagr,
            entry.max_drawdown,
            entry.max_drawdown_ratio,
            entry.verify_sharpe_ratio ?? null,
            entry.verify_calmar_ratio ?? null,
            entry.verify_cagr ?? null,
            entry.verify_max_drawdown_ratio ?? null,
            entry.balance_training_sharpe_ratio ?? null,
            entry.balance_training_calmar_ratio ?? null,
            entry.balance_training_cagr ?? null,
            entry.balance_training_max_drawdown_ratio ?? null,
            entry.balance_validation_sharpe_ratio ?? null,
            entry.balance_validation_calmar_ratio ?? null,
            entry.balance_validation_cagr ?? null,
            entry.balance_validation_max_drawdown_ratio ?? null,
            entry.win_rate,
            entry.total_trades,
            entry.ticker_count,
            entry.start_date,
            entry.end_date,
            entry.period_days ?? 0,
            entry.period_months ?? 0,
            entry.duration_minutes,
            entry.tool,
            entry.top_abs_gain_ticker ?? null,
            entry.top_rel_gain_ticker ?? null,
            entry.created_at ?? new Date().toISOString()
          ],
          client
        );
      }
    });

    return { inserted: sanitizedEntries.length, skipped };
  }

  async vacuum(): Promise<void> {
    try {
      await this.db.exec('VACUUM;');
    } catch (error) {
      console.error('Failed to vacuum database:', error);
      throw error;
    }
  }
}
