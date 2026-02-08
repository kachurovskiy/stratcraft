import type { PoolClient, QueryResultRow } from 'pg';
import { DbClient } from '../core/DbClient';
import { parseDate, toNullableNumber } from '../core/valueParsers';
import type {
  LightgbmDatasetStatsSummary,
  LightgbmModelCreateInput,
  LightgbmModelRecord,
  LightgbmModelSource,
  LightgbmValidationMetricsSummary
} from '../types';

type LightgbmModelRow = QueryResultRow & {
  id: string;
  name: string;
  tree_text: string;
  source: LightgbmModelSource;
  num_iterations?: number | null;
  learning_rate?: number | null;
  num_leaves?: number | null;
  max_depth?: number | null;
  min_data_in_leaf?: number | null;
  min_gain_to_split?: number | null;
  lambda_l1?: number | null;
  lambda_l2?: number | null;
  feature_fraction?: number | null;
  bagging_fraction?: number | null;
  bagging_freq?: number | null;
  early_stopping_round?: number | null;
  train_dataset_stats?: unknown | null;
  validation_dataset_stats?: unknown | null;
  validation_metrics?: unknown | null;
  engine_stdout?: string | null;
  engine_stderr?: string | null;
  trained_at?: Date | null;
  created_at: Date;
  updated_at: Date;
  is_active: boolean;
};

type LightgbmOutputRow = QueryResultRow & {
  engine_stdout: string | null;
  engine_stderr: string | null;
};

type ExistsRow = QueryResultRow & { id: string };

type StrategyIdRow = QueryResultRow & { id: string };

export class LightgbmModelsRepo {
  constructor(private readonly db: DbClient) {}

  private parseJson<T>(value: unknown): T | null {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === 'object') {
      return value as T;
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!trimmed) {
        return null;
      }
      try {
        return JSON.parse(trimmed) as T;
      } catch {
        return null;
      }
    }
    if (value instanceof Buffer) {
      return this.parseJson<T>(value.toString('utf8'));
    }
    return null;
  }

  private mapRow(row: LightgbmModelRow): LightgbmModelRecord {
    const source: LightgbmModelSource = row.source === 'training' ? 'training' : 'manual';
    return {
      id: row.id,
      name: row.name,
      treeText: row.tree_text ?? '',
      source,
      numIterations: toNullableNumber(row.num_iterations),
      learningRate: toNullableNumber(row.learning_rate),
      numLeaves: toNullableNumber(row.num_leaves),
      maxDepth: toNullableNumber(row.max_depth),
      minDataInLeaf: toNullableNumber(row.min_data_in_leaf),
      minGainToSplit: toNullableNumber(row.min_gain_to_split),
      lambdaL1: toNullableNumber(row.lambda_l1),
      lambdaL2: toNullableNumber(row.lambda_l2),
      featureFraction: toNullableNumber(row.feature_fraction),
      baggingFraction: toNullableNumber(row.bagging_fraction),
      baggingFreq: toNullableNumber(row.bagging_freq),
      earlyStoppingRound: toNullableNumber(row.early_stopping_round),
      trainDatasetStats: this.parseJson<LightgbmDatasetStatsSummary>(row.train_dataset_stats),
      validationDatasetStats: this.parseJson<LightgbmDatasetStatsSummary>(row.validation_dataset_stats),
      validationMetrics: this.parseJson<LightgbmValidationMetricsSummary>(row.validation_metrics),
      engineStdout: typeof row.engine_stdout === 'string' ? row.engine_stdout : null,
      engineStderr: typeof row.engine_stderr === 'string' ? row.engine_stderr : null,
      trainedAt: parseDate(row.trained_at),
      createdAt: parseDate(row.created_at) ?? new Date(),
      updatedAt: parseDate(row.updated_at) ?? new Date(),
      isActive: Boolean(row.is_active)
    };
  }

  async listLightgbmModels(): Promise<LightgbmModelRecord[]> {
    const rows = await this.db.all<LightgbmModelRow>(
      `
        SELECT
          id,
          name,
          tree_text,
          source,
          num_iterations,
          learning_rate,
          num_leaves,
          max_depth,
          min_data_in_leaf,
          min_gain_to_split,
          lambda_l1,
          lambda_l2,
          feature_fraction,
          bagging_fraction,
          bagging_freq,
          early_stopping_round,
          train_dataset_stats,
          validation_dataset_stats,
          validation_metrics,
          trained_at,
          created_at,
          updated_at,
          is_active
        FROM lightgbm_models
        ORDER BY created_at DESC
      `
    );
    return rows.map((row) => this.mapRow(row));
  }

  async getLightgbmModelByName(name: string): Promise<LightgbmModelRecord | null> {
    const trimmed = typeof name === 'string' ? name.trim() : '';
    if (!trimmed) {
      return null;
    }
    const row = await this.db.get<LightgbmModelRow>(
      `
        SELECT
          id,
          name,
          tree_text,
          source,
          num_iterations,
          learning_rate,
          num_leaves,
          max_depth,
          min_data_in_leaf,
          min_gain_to_split,
          lambda_l1,
          lambda_l2,
          feature_fraction,
          bagging_fraction,
          bagging_freq,
          early_stopping_round,
          train_dataset_stats,
          validation_dataset_stats,
          validation_metrics,
          trained_at,
          created_at,
          updated_at,
          is_active
        FROM lightgbm_models
        WHERE LOWER(name) = LOWER(?)
        LIMIT 1
      `,
      [trimmed]
    );
    return row ? this.mapRow(row) : null;
  }

  async getActiveLightgbmModel(): Promise<LightgbmModelRecord | null> {
    const row = await this.db.get<LightgbmModelRow>(
      `
        SELECT
          id,
          name,
          tree_text,
          source,
          num_iterations,
          learning_rate,
          num_leaves,
          max_depth,
          min_data_in_leaf,
          min_gain_to_split,
          lambda_l1,
          lambda_l2,
          feature_fraction,
          bagging_fraction,
          bagging_freq,
          early_stopping_round,
          train_dataset_stats,
          validation_dataset_stats,
          validation_metrics,
          trained_at,
          created_at,
          updated_at,
          is_active
        FROM lightgbm_models
        WHERE is_active = TRUE
        ORDER BY updated_at DESC
        LIMIT 1
      `
    );
    return row ? this.mapRow(row) : null;
  }

  async getLightgbmModelOutput(
    modelId: string
  ): Promise<{ engineStdout: string | null; engineStderr: string | null } | null> {
    const trimmed = typeof modelId === 'string' ? modelId.trim() : '';
    if (!trimmed) {
      return null;
    }
    const row = await this.db.get<LightgbmOutputRow>(
      `
        SELECT
          engine_stdout,
          engine_stderr
        FROM lightgbm_models
        WHERE id = ?
        LIMIT 1
      `,
      [trimmed]
    );
    if (!row) {
      return null;
    }
    return {
      engineStdout: typeof row.engine_stdout === 'string' ? row.engine_stdout : null,
      engineStderr: typeof row.engine_stderr === 'string' ? row.engine_stderr : null
    };
  }

  async createLightgbmModel(input: LightgbmModelCreateInput): Promise<LightgbmModelRecord> {
    const row = await this.db.get<LightgbmModelRow>(
      `
        INSERT INTO lightgbm_models (
          id,
          name,
          tree_text,
          source,
          num_iterations,
          learning_rate,
          num_leaves,
          max_depth,
          min_data_in_leaf,
          min_gain_to_split,
          lambda_l1,
          lambda_l2,
          feature_fraction,
          bagging_fraction,
          bagging_freq,
          early_stopping_round,
          train_dataset_stats,
          validation_dataset_stats,
          validation_metrics,
          engine_stdout,
          engine_stderr,
          trained_at,
          is_active,
          created_at,
          updated_at
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, NOW(), NOW()
        )
        RETURNING
          id,
          name,
          tree_text,
          source,
          num_iterations,
          learning_rate,
          num_leaves,
          max_depth,
          min_data_in_leaf,
          min_gain_to_split,
          lambda_l1,
          lambda_l2,
          feature_fraction,
          bagging_fraction,
          bagging_freq,
          early_stopping_round,
          train_dataset_stats,
          validation_dataset_stats,
          validation_metrics,
          trained_at,
          created_at,
          updated_at,
          is_active
      `,
      [
        input.id,
        input.name.trim(),
        input.treeText,
        input.source,
        input.numIterations ?? null,
        input.learningRate ?? null,
        input.numLeaves ?? null,
        input.maxDepth ?? null,
        input.minDataInLeaf ?? null,
        input.minGainToSplit ?? null,
        input.lambdaL1 ?? null,
        input.lambdaL2 ?? null,
        input.featureFraction ?? null,
        input.baggingFraction ?? null,
        input.baggingFreq ?? null,
        input.earlyStoppingRound ?? null,
        input.trainDatasetStats ?? null,
        input.validationDatasetStats ?? null,
        input.validationMetrics ?? null,
        input.engineStdout ?? null,
        input.engineStderr ?? null,
        input.trainedAt ?? null
      ]
    );
    if (!row) {
      throw new Error('Failed to create LightGBM model record');
    }
    return this.mapRow(row);
  }

  async updateLightgbmModelText(modelId: string, treeText: string): Promise<void> {
    if (!modelId || !treeText) {
      throw new Error('modelId and treeText are required to update LightGBM model text');
    }
    const result = await this.db.run(
      `
        UPDATE lightgbm_models
        SET tree_text = ?, updated_at = NOW()
        WHERE id = ?
      `,
      [treeText, modelId]
    );
    if (result.rowCount === 0) {
      throw new Error('LightGBM model not found');
    }
  }

  async setActiveLightgbmModel(modelId: string): Promise<void> {
    if (!modelId) {
      throw new Error('modelId is required to activate LightGBM model');
    }
    const result = await this.db.run(
      `
        UPDATE lightgbm_models
        SET is_active = TRUE, updated_at = NOW()
        WHERE id = ?
      `,
      [modelId]
    );
    if (result.rowCount === 0) {
      throw new Error('LightGBM model not found');
    }
  }

  async listActiveLightgbmModels(): Promise<LightgbmModelRecord[]> {
    const rows = await this.db.all<LightgbmModelRow>(
      `
        SELECT
          id,
          name,
          tree_text,
          source,
          num_iterations,
          learning_rate,
          trained_at,
          created_at,
          updated_at,
          is_active
        FROM lightgbm_models
        WHERE is_active = TRUE
        ORDER BY updated_at DESC
      `
    );
    return rows.map((row) => this.mapRow(row));
  }

  private async deleteStrategyRelatedData(strategyIds: string[], client: PoolClient): Promise<void> {
    const normalizedIds = Array.from(new Set(strategyIds.map((id) => id.trim()).filter((id) => id.length > 0)));
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

  async deleteLightgbmModel(modelId: string): Promise<void> {
    if (!modelId) {
      throw new Error('modelId is required to delete LightGBM model');
    }
    const trimmedId = modelId.trim();
    if (!trimmedId) {
      throw new Error('modelId is required to delete LightGBM model');
    }
    const templateId = `lightgbm_${trimmedId}`;
    await this.db.withTransaction(async (client) => {
      const existing = await this.db.get<ExistsRow>(
        'SELECT id FROM lightgbm_models WHERE id = ?',
        [trimmedId],
        client
      );
      if (!existing) {
        throw new Error('LightGBM model not found');
      }

      const strategyRows = await this.db.all<StrategyIdRow>(
        'SELECT id FROM strategies WHERE template_id = ?',
        [templateId],
        client
      );
      const strategyIds = strategyRows.map((row) => row.id.trim()).filter((id) => id.length > 0);
      if (strategyIds.length) {
        await this.deleteStrategyRelatedData(strategyIds, client);
        await this.db.run(
          `DELETE FROM strategies WHERE id IN (${strategyIds.map(() => '?').join(', ')})`,
          strategyIds,
          client
        );
      }

      await this.db.run('DELETE FROM backtest_cache WHERE template_id = ?', [templateId], client);
      await this.db.run('DELETE FROM remote_optimizer_jobs WHERE template_id = ?', [templateId], client);
      await this.db.run('DELETE FROM templates WHERE id = ?', [templateId], client);
      await this.db.run('DELETE FROM lightgbm_models WHERE id = ?', [trimmedId], client);
    });
  }
}
