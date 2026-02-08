import type { QueryResultRow } from 'pg';
import { DbClient, type QueryValue } from '../core/DbClient';
import type { SystemLogInsertInput, SystemLogQueryOptions, SystemLogRow } from '../types';

type SystemLogSourceRow = QueryResultRow & { source: string };

export class SystemLogsRepo {
  constructor(private readonly db: DbClient) {}

  async insertSystemLog(entry: SystemLogInsertInput): Promise<void> {
    const createdAt = entry.createdAt ?? new Date();

    await this.db.run(
      `INSERT INTO system_logs (source, level, message, metadata, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      [
        entry.source,
        entry.level,
        entry.message ?? 'No message',
        entry.metadata ? JSON.stringify(entry.metadata) : null,
        createdAt
      ]
    );
  }

  async getSystemLogs(options: SystemLogQueryOptions = {}): Promise<SystemLogRow[]> {
    const limit = Number.isFinite(options.limit) && options.limit! > 0 ? Math.floor(options.limit!) : 1000;
    const offset = Number.isFinite(options.offset) && options.offset! >= 0 ? Math.floor(options.offset!) : 0;

    let sql = 'SELECT * FROM system_logs WHERE 1=1';
    const params: QueryValue[] = [];
    if (options.source) {
      sql += ' AND source = ?';
      params.push(options.source);
    }
    if (options.level) {
      sql += ' AND level = ?';
      params.push(options.level);
    }

    sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);

    return this.db.all<SystemLogRow>(sql, params);
  }

  async getSystemLogsByStrategyId(
    strategyId: string,
    limit: number = 100,
    offset: number = 0
  ): Promise<SystemLogRow[]> {
    const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 100;
    const safeOffset = Number.isFinite(offset) && offset >= 0 ? Math.floor(offset) : 0;

    return this.db.all<SystemLogRow>(
      `SELECT * FROM system_logs
       WHERE (NULLIF(metadata, '')::jsonb ->> 'strategyId') = ?
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [strategyId, safeLimit, safeOffset]
    );
  }

  async getSystemLogsByJobId(
    jobId: string,
    limit: number = 1000,
    offset: number = 0
  ): Promise<SystemLogRow[]> {
    const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 1000;
    const safeOffset = Number.isFinite(offset) && offset >= 0 ? Math.floor(offset) : 0;

    return this.db.all<SystemLogRow>(
      `SELECT * FROM system_logs
       WHERE (NULLIF(metadata, '')::jsonb ->> 'jobId') = ?
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [jobId, safeLimit, safeOffset]
    );
  }

  async deleteSystemLogsOlderThan(cutoff: Date): Promise<number> {
    const result = await this.db.run('DELETE FROM system_logs WHERE created_at < ?', [cutoff]);
    return result.changes ?? 0;
  }

  async deleteAllSystemLogs(): Promise<number> {
    const result = await this.db.run('DELETE FROM system_logs');
    return result.changes ?? 0;
  }

  async getSystemLogSources(): Promise<string[]> {
    try {
      const rows = await this.db.all<SystemLogSourceRow>(
        "SELECT DISTINCT source FROM system_logs WHERE NULLIF(source, '') IS NOT NULL ORDER BY source ASC"
      );
      return rows
        .map((row) => (typeof row?.source === 'string' ? row.source.trim() : ''))
        .filter((source): source is string => Boolean(source));
    } catch (error) {
      console.error('Failed to load system log sources:', error);
      return [];
    }
  }
}
