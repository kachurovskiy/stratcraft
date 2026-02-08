import type { Pool, PoolClient, QueryResultRow } from 'pg';
import { configurePgTypeParsers } from './pgTypeParsers';

configurePgTypeParsers();

export type QueryValue =
  | string
  | number
  | boolean
  | Date
  | null
  | undefined
  | Array<unknown>
  | Record<string, unknown>
  | object;

export type RunResult = { rowCount: number; changes: number };

type DbExecutor = Pool | PoolClient;

export class DbClient {
  constructor(private readonly pool: Pool) {}

  formatQuery(sql: string, params: QueryValue[] = []): { text: string; values: unknown[] } {
    if (!params.length) {
      return { text: sql, values: [] };
    }

    let placeholderIndex = 0;
    const text = sql.replace(/\?/g, () => {
      placeholderIndex += 1;
      return `$${placeholderIndex}`;
    });

    if (placeholderIndex !== params.length) {
      throw new Error(
        `SQL parameter mismatch. Expected ${placeholderIndex} parameters but received ${params.length}.`
      );
    }

    const values = params.map((value) => {
      if (value instanceof Date) return value;
      if (value === undefined) return null;
      return value;
    });

    return { text, values };
  }

  async run(sql: string, params: QueryValue[] = [], executor?: DbExecutor): Promise<RunResult> {
    const { text, values } = this.formatQuery(sql, params);
    const runner = executor ?? this.pool;
    const result = await runner.query(text, values);
    const rowCount = result.rowCount ?? 0;
    return { rowCount, changes: rowCount };
  }

  async exec(sql: string, executor?: DbExecutor): Promise<void> {
    const runner = executor ?? this.pool;
    await runner.query(sql);
  }

  async get<T extends QueryResultRow = QueryResultRow>(
    sql: string,
    params: QueryValue[] = [],
    executor?: DbExecutor
  ): Promise<T | undefined> {
    const { text, values } = this.formatQuery(sql, params);
    const runner = executor ?? this.pool;
    const result = await runner.query(text, values);
    return (result.rows[0] as T | undefined) ?? undefined;
  }

  async all<T extends QueryResultRow = QueryResultRow>(
    sql: string,
    params: QueryValue[] = [],
    executor?: DbExecutor
  ): Promise<T[]> {
    const { text, values } = this.formatQuery(sql, params);
    const runner = executor ?? this.pool;
    const result = await runner.query(text, values);
    return result.rows as T[];
  }

  async withTransaction<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        console.error('Failed to rollback transaction:', rollbackError);
      }
      throw error;
    } finally {
      client.release();
    }
  }
}
