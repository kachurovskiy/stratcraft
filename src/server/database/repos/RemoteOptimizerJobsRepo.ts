import type { QueryResultRow } from 'pg';
import type { RemoteOptimizationStatus } from '../../../shared/types/RemoteOptimization';
import type { RemoteOptimizerJobEntity } from '../types';
import { DbClient } from '../core/DbClient';

type RemoteOptimizerJobRow = QueryResultRow & {
  id: string;
  template_id: string;
  template_name: string;
  status: RemoteOptimizationStatus;
  created_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
  hetzner_server_id: number | null;
  remote_server_ip: string | null;
};

type ExistsRow = QueryResultRow & { exists: number };

export class RemoteOptimizerJobsRepo {
  constructor(private readonly db: DbClient) {}

  private mapRow(row: RemoteOptimizerJobRow): RemoteOptimizerJobEntity {
    return {
      id: row.id,
      templateId: row.template_id,
      templateName: row.template_name,
      status: row.status,
      createdAt: row.created_at,
      startedAt: row.started_at ?? undefined,
      finishedAt: row.finished_at ?? undefined,
      hetznerServerId: row.hetzner_server_id,
      remoteServerIp: row.remote_server_ip ?? null
    };
  }

  async hasActiveRemoteOptimizerJob(templateId: string): Promise<boolean> {
    const row = await this.db.get<ExistsRow>(
      `
        SELECT 1 as exists
        FROM remote_optimizer_jobs
        WHERE template_id = ?
          AND status IN ('queued', 'running', 'handoff')
        LIMIT 1
      `,
      [templateId]
    );
    return Boolean(row?.exists);
  }

  async getRemoteOptimizerJobs(): Promise<RemoteOptimizerJobEntity[]> {
    const rows = await this.db.all<RemoteOptimizerJobRow>(
      `
        SELECT
          id,
          template_id,
          template_name,
          status,
          created_at,
          started_at,
          finished_at,
          hetzner_server_id,
          remote_server_ip
        FROM remote_optimizer_jobs
        ORDER BY created_at ASC
      `
    );
    return rows.map((row) => this.mapRow(row));
  }

  async getRemoteOptimizerJob(jobId: string): Promise<RemoteOptimizerJobEntity | null> {
    const row = await this.db.get<RemoteOptimizerJobRow>(
      `
        SELECT
          id,
          template_id,
          template_name,
          status,
          created_at,
          started_at,
          finished_at,
          hetzner_server_id,
          remote_server_ip
        FROM remote_optimizer_jobs
        WHERE id = ?
        LIMIT 1
      `,
      [jobId]
    );
    return row ? this.mapRow(row) : null;
  }

  async upsertRemoteOptimizerJob(job: RemoteOptimizerJobEntity): Promise<void> {
    await this.db.run(
      `
        INSERT INTO remote_optimizer_jobs (
          id,
          template_id,
          template_name,
          status,
          created_at,
          started_at,
          finished_at,
          hetzner_server_id,
          remote_server_ip,
          updated_at
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW()
        )
        ON CONFLICT (id) DO UPDATE
        SET
          template_id = EXCLUDED.template_id,
          template_name = EXCLUDED.template_name,
          status = EXCLUDED.status,
          created_at = EXCLUDED.created_at,
          started_at = EXCLUDED.started_at,
          finished_at = EXCLUDED.finished_at,
          hetzner_server_id = EXCLUDED.hetzner_server_id,
          remote_server_ip = EXCLUDED.remote_server_ip,
          updated_at = NOW()
      `,
      [
        job.id,
        job.templateId,
        job.templateName,
        job.status,
        job.createdAt,
        job.startedAt ?? null,
        job.finishedAt ?? null,
        job.hetznerServerId ?? null,
        job.remoteServerIp ?? null
      ]
    );
  }

  async deleteFinishedRemoteOptimizerJobs(): Promise<number> {
    const result = await this.db.run(
      `
        DELETE FROM remote_optimizer_jobs
        WHERE finished_at IS NOT NULL
           OR status IN ('succeeded', 'failed')
      `
    );
    return result.rowCount;
  }
}
