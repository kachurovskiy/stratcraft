import type { PoolClient, QueryResultRow } from 'pg';
import { normalizeUppercaseStrings } from '../../utils/stringNormalization';
import { DbClient, type QueryValue } from '../core/DbClient';
import { parseDate } from '../core/valueParsers';
import type { CorporateActionRecord } from '../types';

type LatestProcessDateRow = QueryResultRow & {
  process_date: Date | string | null;
};

export class CorporateActionsRepo {
  constructor(private readonly db: DbClient) {}

  async getLatestProcessDate(): Promise<Date | null> {
    const row = await this.db.get<LatestProcessDateRow>(
      `SELECT MAX(process_date) AS process_date
         FROM corporate_actions`
    );
    return parseDate(row?.process_date);
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
          placeholders.push('(?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)');
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
