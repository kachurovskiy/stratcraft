import type { PoolClient, QueryResultRow } from 'pg';
import { normalizeUppercaseString, normalizeUppercaseStrings } from '../../utils/stringNormalization';
import { DbClient, type QueryValue } from '../core/DbClient';
import { parseDate } from '../core/valueParsers';
import type { CorporateActionRecord, CorporateActionType } from '../types';

type LatestProcessDateRow = QueryResultRow & {
  process_date: string | null;
};

type CorporateActionRow = QueryResultRow & {
  id: string;
  action_type: CorporateActionType;
  primary_symbol: string;
  related_symbols: string[];
  process_date: string;
  effective_date: string | null;
  ex_date: string | null;
  record_date: string | null;
  payable_date: string | null;
  payload: Record<string, unknown>;
};

export class CorporateActionsRepo {
  constructor(private readonly db: DbClient) {}

  private mapCorporateActionRow(row: CorporateActionRow): CorporateActionRecord | null {
    const processDate = parseDate(row.process_date);
    if (!processDate) {
      return null;
    }

    return {
      id: row.id,
      actionType: row.action_type,
      primarySymbol: normalizeUppercaseString(row.primary_symbol),
      relatedSymbols: normalizeUppercaseStrings(row.related_symbols),
      processDate,
      effectiveDate: parseDate(row.effective_date) ?? null,
      exDate: parseDate(row.ex_date) ?? null,
      recordDate: parseDate(row.record_date) ?? null,
      payableDate: parseDate(row.payable_date) ?? null,
      payload: row.payload
    };
  }

  async getLatestProcessDate(): Promise<Date | null> {
    const row = await this.db.get<LatestProcessDateRow>(
      `SELECT MAX(process_date) AS process_date
         FROM corporate_actions`
    );
    return parseDate(row?.process_date);
  }

  async getCorporateActionsForSymbol(symbol: string): Promise<CorporateActionRecord[]> {
    const normalizedSymbol = normalizeUppercaseString(symbol);
    if (!normalizedSymbol) {
      return [];
    }

    const rows = await this.db.all<CorporateActionRow>(
      `SELECT id,
              action_type,
              primary_symbol,
              related_symbols,
              process_date,
              effective_date,
              ex_date,
              record_date,
              payable_date,
              payload
         FROM corporate_actions
        WHERE primary_symbol = ?
           OR ? = ANY(related_symbols)
        ORDER BY process_date DESC, effective_date DESC NULLS LAST, ex_date DESC NULLS LAST, id DESC`,
      [normalizedSymbol, normalizedSymbol]
    );

    return rows
      .map((row) => this.mapCorporateActionRow(row))
      .filter((action): action is CorporateActionRecord => action !== null);
  }

  async getCorporateActionsByIds(ids: string[]): Promise<CorporateActionRecord[]> {
    const normalizedIds = Array.from(
      new Set(
        ids
          .map((id) => (typeof id === 'string' ? id.trim() : ''))
          .filter((id) => id.length > 0)
      )
    );
    if (normalizedIds.length === 0) {
      return [];
    }

    const placeholders = normalizedIds.map(() => '?').join(', ');
    const rows = await this.db.all<CorporateActionRow>(
      `SELECT id,
              action_type,
              primary_symbol,
              related_symbols,
              process_date,
              effective_date,
              ex_date,
              record_date,
              payable_date,
              payload
         FROM corporate_actions
        WHERE id IN (${placeholders})
           OR COALESCE(NULLIF(payload ->> 'corporate_action_id', ''), id::text) IN (${placeholders})
        ORDER BY process_date ASC, effective_date ASC NULLS LAST, ex_date ASC NULLS LAST, id ASC`,
      [...normalizedIds, ...normalizedIds]
    );

    return rows
      .map((row) => this.mapCorporateActionRow(row))
      .filter((action): action is CorporateActionRecord => action !== null);
  }

  async upsertCorporateActions(actions: CorporateActionRecord[]): Promise<{ upserted: number }> {
    if (!Array.isArray(actions) || actions.length === 0) {
      return { upserted: 0 };
    }

    return this.db.withTransaction(async (client: PoolClient) => {
      const chunkSize = 250;
      let upserted = 0;

      for (let i = 0; i < actions.length; i += chunkSize) {
        const chunk = actions.slice(i, i + chunkSize);
        if (chunk.length === 0) {
          continue;
        }

        const placeholders: string[] = [];
        const params: QueryValue[] = [];

        for (const action of chunk) {
          placeholders.push('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)');
          params.push(
            action.id,
            action.actionType,
            action.primarySymbol,
            normalizeUppercaseStrings(action.relatedSymbols),
            action.processDate,
            action.effectiveDate ?? null,
            action.exDate ?? null,
            action.recordDate ?? null,
            action.payableDate ?? null,
            action.payload
          );
        }

        const result = await this.db.run(
          `
            INSERT INTO corporate_actions (
              id,
              action_type,
              primary_symbol,
              related_symbols,
              process_date,
              effective_date,
              ex_date,
              record_date,
              payable_date,
              payload,
              created_at,
              updated_at
            )
            VALUES ${placeholders.join(', ')}
            ON CONFLICT (id) DO UPDATE
            SET action_type = EXCLUDED.action_type,
                primary_symbol = EXCLUDED.primary_symbol,
                related_symbols = EXCLUDED.related_symbols,
                process_date = EXCLUDED.process_date,
                effective_date = EXCLUDED.effective_date,
                ex_date = EXCLUDED.ex_date,
                record_date = EXCLUDED.record_date,
                payable_date = EXCLUDED.payable_date,
                payload = EXCLUDED.payload,
                updated_at = CURRENT_TIMESTAMP
          `,
          params,
          client
        );

        upserted += result.rowCount;
      }

      return { upserted };
    });
  }
}
