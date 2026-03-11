import type { QueryResultRow } from 'pg';
import { DbClient } from '../core/DbClient';

type BacktestBestParamsRow = QueryResultRow & {
  template_id: string;
  parameters: string;
  created_at: Date | null;
};

type BestParamsEntry = {
  templateId: string;
  parameters: Record<string, unknown>;
};

export class BacktestBestParamsRepo {
  constructor(private readonly db: DbClient) {}

  private serializeJsonConsistently(obj: Record<string, unknown>): string {
    return JSON.stringify(obj, Object.keys(obj).sort());
  }

  private parseParameters(raw: unknown): Record<string, unknown> | null {
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
        return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
          ? (parsed as Record<string, unknown>)
          : null;
      } catch {
        return null;
      }
    }
    if (raw instanceof Buffer) {
      return this.parseParameters(raw.toString('utf8'));
    }
    if (typeof raw === 'object' && !Array.isArray(raw)) {
      return { ...(raw as Record<string, unknown>) };
    }
    return null;
  }

  async getSavedBestParams(templateId: string): Promise<Record<string, unknown> | null> {
    const normalized = typeof templateId === 'string' ? templateId.trim() : '';
    if (!normalized) {
      return null;
    }

    try {
      const row = await this.db.get<BacktestBestParamsRow>(
        `
          SELECT parameters
          FROM backtest_best_params
          WHERE template_id = ?
        `,
        [normalized]
      );

      if (!row) {
        return null;
      }

      return this.parseParameters(row.parameters);
    } catch (error) {
      console.error(`Error getting saved best params for template ${normalized}:`, error);
      return null;
    }
  }

  async upsertSavedBestParamsBatch(
    entries: BestParamsEntry[]
  ): Promise<{ saved: number; skipped: number }> {
    const sanitized = (entries ?? [])
      .map((entry) => {
        const templateId = typeof entry.templateId === 'string' ? entry.templateId.trim() : '';
        if (!templateId) {
          return null;
        }
        if (!entry.parameters || typeof entry.parameters !== 'object' || Array.isArray(entry.parameters)) {
          return null;
        }
        return {
          templateId,
          parametersJson: this.serializeJsonConsistently(entry.parameters)
        };
      })
      .filter((entry): entry is { templateId: string; parametersJson: string } => entry !== null);

    const skipped = (entries?.length ?? 0) - sanitized.length;

    if (sanitized.length === 0) {
      return { saved: 0, skipped };
    }

    await this.db.withTransaction(async (client) => {
      for (const entry of sanitized) {
        await this.db.run(
          `
            INSERT INTO backtest_best_params (template_id, parameters, created_at)
            VALUES (?, ?, NOW())
            ON CONFLICT (template_id) DO UPDATE
            SET parameters = EXCLUDED.parameters,
                created_at = EXCLUDED.created_at
          `,
          [entry.templateId, entry.parametersJson],
          client
        );
      }
    });

    return { saved: sanitized.length, skipped };
  }
}
