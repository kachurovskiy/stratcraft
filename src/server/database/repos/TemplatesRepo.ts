import type { PoolClient, QueryResultRow } from 'pg';
import { SETTING_KEYS } from '../../constants';
import type { StrategyTemplate } from '../../../shared/types/StrategyTemplate';
import { DbClient } from '../core/DbClient';
import { SettingsRepo } from './SettingsRepo';

type TemplateRow = QueryResultRow & {
  id: string;
  name: string;
  description: string | null;
  category: string | null;
  author: string | null;
  version: string | null;
  parameters: string;
  example_usage: string | null;
};

type TemplateVersionRow = QueryResultRow & {
  id: string;
  local_optimization_version: number;
};

type IdRow = QueryResultRow & { id: string };

export class TemplatesRepo {
  constructor(
    private readonly db: DbClient,
    private readonly settings: SettingsRepo
  ) {}

  private mapTemplateRow(row: TemplateRow): StrategyTemplate {
    let parameters: unknown = [];
    try {
      parameters = JSON.parse(row.parameters);
    } catch {
      parameters = [];
    }

    return {
      id: row.id,
      name: row.name,
      description: row.description ?? '',
      category: row.category ?? '',
      author: row.author ?? '',
      version: row.version ?? '',
      parameters: Array.isArray(parameters)
        ? (parameters as StrategyTemplate['parameters'])
        : ([] as StrategyTemplate['parameters']),
      exampleUsage: row.example_usage ?? undefined
    };
  }

  private async getLocalOptimizationVersionTarget(): Promise<number> {
    const rawValue = await this.settings.getSettingValue(SETTING_KEYS.LOCAL_OPTIMIZATION_VERSION);
    const parsed = rawValue !== null ? Number(rawValue) : NaN;
    if (!Number.isFinite(parsed)) {
      if (rawValue !== null) {
        console.warn('LOCAL_OPTIMIZATION_VERSION setting must be numeric.');
      }
      return 0;
    }
    return Math.max(0, Math.trunc(parsed));
  }

  async getTemplateNeedingLocalOptimization(): Promise<StrategyTemplate | null> {
    const targetLocalOptimizationVersion = await this.getLocalOptimizationVersionTarget();
    const row = await this.db.get<TemplateRow>(
      `
      WITH latest_results AS (
        SELECT br.*
        FROM backtest_results br
        JOIN (
          SELECT strategy_id, MAX(created_at) AS latest_created_at
          FROM backtest_results
          GROUP BY strategy_id
        ) latest
          ON latest.strategy_id = br.strategy_id
         AND latest.latest_created_at = br.created_at
      ),
      template_metrics AS (
        SELECT
          t.id,
          MAX(
            COALESCE(
              (NULLIF(lr.performance, '')::jsonb ->> 'sharpe_ratio')::DOUBLE PRECISION,
              (NULLIF(lr.performance, '')::jsonb ->> 'sharpeRatio')::DOUBLE PRECISION,
              -999999
            )
          ) AS best_sharpe_ratio
        FROM templates t
        LEFT JOIN strategies s ON s.template_id = t.id
        LEFT JOIN latest_results lr ON lr.strategy_id = s.id
        WHERE t.id != 'buy_and_hold'
          AND (
            COALESCE(t.local_optimization_version, 0) < ?
            OR NOT EXISTS (
              SELECT 1
              FROM backtest_cache bc
              WHERE bc.template_id = t.id
            )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM remote_optimizer_jobs roj
            WHERE roj.template_id = t.id
              AND roj.status IN ('queued', 'running', 'handoff')
          )
        GROUP BY t.id
      )
      SELECT t.*
      FROM templates t
      LEFT JOIN template_metrics tm ON tm.id = t.id
      WHERE t.id != 'buy_and_hold'
        AND (
          COALESCE(t.local_optimization_version, 0) < ?
          OR NOT EXISTS (
            SELECT 1
            FROM backtest_cache bc
            WHERE bc.template_id = t.id
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM remote_optimizer_jobs roj
          WHERE roj.template_id = t.id
            AND roj.status IN ('queued', 'running', 'handoff')
        )
      ORDER BY COALESCE(tm.best_sharpe_ratio, -999999) DESC, t.created_at ASC
      LIMIT 1
    `,
      [targetLocalOptimizationVersion, targetLocalOptimizationVersion]
    );

    return row ? this.mapTemplateRow(row) : null;
  }

  async getTemplateLocalOptimizationVersions(): Promise<Record<string, number>> {
    const rows = await this.db.all<TemplateVersionRow>(
      `
      SELECT id, COALESCE(local_optimization_version, 0) AS local_optimization_version
      FROM templates
    `
    );

    const versions: Record<string, number> = {};
    for (const row of rows) {
      const value =
        typeof row.local_optimization_version === 'number'
          ? row.local_optimization_version
          : Number(row.local_optimization_version);
      versions[row.id] = Number.isFinite(value) ? value : 0;
    }
    return versions;
  }

  async getAllTemplateIds(): Promise<string[]> {
    const rows = await this.db.all<IdRow>(
      `
      SELECT id
      FROM templates
      ORDER BY LOWER(id) ASC, id ASC
    `
    );
    return rows
      .map((row) => (typeof row.id === 'string' ? row.id.trim() : ''))
      .filter((id) => id.length > 0);
  }

  async resetAllTemplateLocalOptimizationVersions(): Promise<number> {
    const result = await this.db.run(
      `UPDATE templates
       SET local_optimization_version = 0
       WHERE local_optimization_version IS NULL OR local_optimization_version <> 0`
    );
    return result.rowCount;
  }

  async setTemplateLocalOptimizationVersion(templateId: string, version: number): Promise<void> {
    if (typeof templateId !== 'string' || templateId.trim().length === 0) {
      throw new Error('templateId is required to update local optimization version');
    }
    if (!Number.isFinite(version)) {
      throw new Error('A numeric version is required to update local optimization version');
    }
    const normalizedVersion = Math.max(0, Math.trunc(version));
    await this.db.run(
      `UPDATE templates
       SET local_optimization_version = CASE
         WHEN COALESCE(local_optimization_version, 0) < ?
           THEN ?
         ELSE local_optimization_version
       END
       WHERE id = ?`,
      [normalizedVersion, normalizedVersion, templateId]
    );
  }

  async upsertTemplate(template: StrategyTemplate): Promise<void> {
    await this.db.run(
      `INSERT INTO templates (id, name, description, category, author, version, parameters, example_usage)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET
         name = excluded.name,
         description = excluded.description,
         category = excluded.category,
         author = excluded.author,
         version = excluded.version,
         parameters = excluded.parameters,
         example_usage = excluded.example_usage`,
      [
        template.id,
        template.name,
        template.description ?? null,
        template.category ?? null,
        template.author ?? null,
        template.version ?? null,
        JSON.stringify(template.parameters ?? []),
        template.exampleUsage ?? null
      ]
    );
  }

  async getTemplatesNotIn(templateIds: string[]): Promise<string[]> {
    const keep = new Set(
      (templateIds ?? [])
        .map((id) => (typeof id === 'string' ? id.trim() : ''))
        .filter((id) => id.length > 0)
    );
    const rows = await this.db.all<IdRow>('SELECT id FROM templates');
    return rows
      .map((row) => row.id.trim())
      .filter((id) => id.length > 0 && !keep.has(id));
  }

  private async deleteStrategyRelatedData(strategyIds: string[], client: PoolClient): Promise<void> {
    const normalizedIds = Array.from(
      new Set(
        (strategyIds || [])
          .map((strategyId) => (typeof strategyId === 'string' ? strategyId.trim() : ''))
          .filter((strategyId) => strategyId.length > 0)
      )
    );

    if (normalizedIds.length === 0) {
      return;
    }

    const placeholders = normalizedIds.map(() => '?').join(', ');
    await this.db.run(
      `DELETE FROM account_operations WHERE strategy_id IN (${placeholders})`,
      normalizedIds,
      client
    );
    await this.db.run(`DELETE FROM trades WHERE strategy_id IN (${placeholders})`, normalizedIds, client);
    await this.db.run(
      `DELETE FROM backtest_results WHERE strategy_id IN (${placeholders})`,
      normalizedIds,
      client
    );
    await this.db.run(`DELETE FROM signals WHERE strategy_id IN (${placeholders})`, normalizedIds, client);
  }

  async removeTemplatesByIds(remove: string[]): Promise<string[]> {
    const normalized = (remove ?? [])
      .map((id) => (typeof id === 'string' ? id.trim() : ''))
      .filter((id) => id.length > 0);
    if (!normalized.length) {
      return [];
    }

    return this.db.withTransaction(async (client) => {
      const tpl = normalized.map(() => '?').join(', ');
      const strategyRows = await this.db.all<IdRow>(
        `SELECT id FROM strategies WHERE template_id IN (${tpl})`,
        normalized,
        client
      );
      const strategyIds = strategyRows
        .map((row) => row.id.trim())
        .filter((id) => id.length > 0);
      if (strategyIds.length) {
        await this.deleteStrategyRelatedData(strategyIds, client);
        await this.db.run(
          `DELETE FROM strategies WHERE id IN (${strategyIds.map(() => '?').join(', ')})`,
          strategyIds,
          client
        );
      }

      await this.db.run(`DELETE FROM backtest_cache WHERE template_id IN (${tpl})`, normalized, client);
      await this.db.run(`DELETE FROM remote_optimizer_jobs WHERE template_id IN (${tpl})`, normalized, client);
      await this.db.run(`DELETE FROM templates WHERE id IN (${tpl})`, normalized, client);
      return normalized;
    });
  }
}
