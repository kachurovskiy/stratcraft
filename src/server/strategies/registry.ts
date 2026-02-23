import { Database } from '../database/Database';
import { StrategyTemplate } from '../../shared/types/StrategyTemplate';
import { LoggingService } from '../services/LoggingService';
import { JobScheduler } from '../jobs/JobScheduler';
import fs from 'fs';
import { SETTING_KEYS } from '../constants';
import { resolveAppBaseUrl } from '../utils/appUrl';

const BASE_TEMPLATES: StrategyTemplate[] = [];
let LIGHTGBM_BASE_TEMPLATE: StrategyTemplate | null = null;
const templateFiles = fs.readdirSync(__dirname).filter(file => file.endsWith('.json'));
for (const file of templateFiles) {
  const filePath = `${__dirname}/${file}`;
  const template = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
  if (template?.id === 'lightgbm') {
    LIGHTGBM_BASE_TEMPLATE = template;
  } else {
    BASE_TEMPLATES.push(template);
  }
}

export class StrategyRegistry {
  private db: Database;
  private logging?: LoggingService;
  private jobScheduler?: JobScheduler;
  private templates: StrategyTemplate[] = [...BASE_TEMPLATES];
  private allTemplates: StrategyTemplate[] = [...BASE_TEMPLATES];
  private lightgbmTemplates: StrategyTemplate[] = [];
  private disabledTemplateIds: Set<string> = new Set();

  constructor(database: Database, loggingService?: LoggingService, jobScheduler?: JobScheduler) {
    this.db = database;
    this.logging = loggingService;
    this.jobScheduler = jobScheduler;
  }

  setJobScheduler(jobScheduler: JobScheduler): void {
    this.jobScheduler = jobScheduler;
  }

  async initialize(): Promise<void> {
    // Register all templates in the database
    for (const template of BASE_TEMPLATES) {
      await this.registerTemplate(template);
    }

    await this.ensureLightgbmModelTemplates();
    await this.loadDisabledTemplateIds();
    this.rebuildTemplateLists();
    await this.removeDeletedTemplates();

    // Create default strategies from templates
    await this.createDefaultStrategies();
    await this.ensureLightgbmDefaultStrategies();
  }

  private async registerTemplate(template: StrategyTemplate): Promise<void> {
    try {
      await this.db.templates.upsertTemplate(template);
    } catch (error) {
      console.error(`Failed to register template ${template.id}:`, error);
    }
  }

  private async createDefaultStrategies(): Promise<void> {
    // Check for existing default strategies specifically
    const existingDefaultStrategies = await this.db.strategies.getStrategiesByIdLike('default_%');
    const existingDefaultTemplateIds = new Set(existingDefaultStrategies.map(s => s.templateId));
    const existingDefaultStrategyIds = new Set(existingDefaultStrategies.map(s => s.id));

    for (const template of this.templates) {
      if (template.id === 'buy_and_hold') {
        await this.ensureBuyAndHoldDefaults(template, existingDefaultStrategyIds);
        continue;
      }

      if (template.id.startsWith('lightgbm_')) {
        continue;
      }

      if (existingDefaultTemplateIds.has(template.id)) {
        continue;
      }

      const mergedParams = await this.resolveDefaultStrategyParameters(template, 'Creating default strategy');

      const strategyName = `${template.name}`;

      try {
        await this.db.strategies.insertStrategy({
          id: `default_${template.id}`,
          name: strategyName,
          templateId: template.id,
          parameters: mergedParams,
          status: 'active'
        });
        existingDefaultTemplateIds.add(template.id);
        existingDefaultStrategyIds.add(`default_${template.id}`);
        this.logging?.info('StrategyManager', `Created default strategy for ${template.id}`, { strategyId: `default_${template.id}` });
      } catch (error) {
        this.logging?.error('StrategyManager', `Failed to create default strategy for ${template.id}`, {
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }
  }

  /**
   * Get best params from remote and merge with defaults. Returns null if fetch failed.
   */
  private async getBestMergedParamsOrNull(template: StrategyTemplate): Promise<Record<string, any> | null> {
    const fetchAny: any = (globalThis as any).fetch;
    if (!fetchAny) return null;

    try {
      const appBaseUrl = await resolveAppBaseUrl(this.db);
      if (!appBaseUrl) {
        return null;
      }
      const rawSecret = await this.db.settings.getSettingValue(SETTING_KEYS.BACKTEST_API_SECRET);
      const secret = typeof rawSecret === 'string' ? rawSecret.trim() : '';
      const headers: Record<string, string> = {};
      if (secret) {
        headers['x-backtest-secret'] = secret;
      }
      const res: any = await fetchAny(
        `${appBaseUrl}/api/backtest/best/${encodeURIComponent(template.id)}`,
        { headers }
      );
      if (!res || !res.ok) return null;
      const data: any = await res.json();
      if (!data || typeof data !== 'object' || !data.parameters || typeof data.parameters !== 'object') return null;

      const best: Record<string, any> = data.parameters as Record<string, any>;

      const merged: Record<string, any> = {};
      for (const def of template.parameters) {
        if (best && Object.prototype.hasOwnProperty.call(best, def.name) && best[def.name] !== undefined) {
          merged[def.name] = best[def.name];
        } else if (def.default !== undefined) {
          merged[def.name] = def.default;
        }
      }
      return merged;
    } catch (error) {
      this.logging?.warn('StrategyManager', `Remote best params fetch failed for ${template.id}`, {
        error: error instanceof Error ? error.message : String(error)
      });
      return null;
    }
  }

  /**
   * Get best params from local DB cache and merge with defaults. Returns null if none.
   */
  private async getLocalBestMergedParamsOrNull(template: StrategyTemplate): Promise<Record<string, any> | null> {
    try {
      const local = await this.db.backtestCache.getBestParams(template.id);
      if (!local || !local.parameters || typeof local.parameters !== 'object') {
        return null;
      }

      const best: Record<string, any> = local.parameters as Record<string, any>;
      const merged: Record<string, any> = {};
      for (const def of template.parameters) {
        if (best && Object.prototype.hasOwnProperty.call(best, def.name) && best[def.name] !== undefined) {
          merged[def.name] = best[def.name];
        } else if (def.default !== undefined) {
          merged[def.name] = def.default;
        }
      }
      this.logging?.info('StrategyManager', `Using local DB best params for ${template.id}`);
      return merged;
    } catch (error) {
      this.logging?.error('StrategyManager', `Local best params lookup failed for ${template.id}`, {
        error: error instanceof Error ? error.message : String(error)
      });
      return null;
    }
  }

  private async resolveDefaultStrategyParameters(
    template: StrategyTemplate,
    actionLabel: string
  ): Promise<Record<string, any>> {
    this.logging?.info(
      'StrategyManager',
      `${actionLabel} for template ${template.id} - fetching best params from remote`
    );
    let mergedParams = await this.getBestMergedParamsOrNull(template);
    if (!mergedParams) {
      this.logging?.warn('StrategyManager', `Remote best params unavailable for ${template.id}; trying local database cache`);
      mergedParams = await this.getLocalBestMergedParamsOrNull(template);
    }
    if (!mergedParams) {
      this.logging?.warn('StrategyManager', `No best params found (remote/local) for ${template.id}; using template defaults`);
      mergedParams = this.buildDefaultParameters(template);
    }
    return mergedParams;
  }

  private attachEnabledFlag(template: StrategyTemplate): StrategyTemplate {
    return {
      ...template,
      enabled: !this.disabledTemplateIds.has(template.id)
    };
  }

  private rebuildTemplateLists(): void {
    this.allTemplates = [...BASE_TEMPLATES, ...this.lightgbmTemplates];
    this.templates = this.allTemplates.filter((template) => !this.disabledTemplateIds.has(template.id));
  }

  getTemplate(
    templateId: string,
    options: { includeDisabled?: boolean } = {}
  ): StrategyTemplate | undefined {
    const includeDisabled = options.includeDisabled ?? true;
    const normalized = typeof templateId === 'string' ? templateId.trim() : '';
    if (!normalized) {
      return undefined;
    }
    const list = includeDisabled ? this.allTemplates : this.templates;
    const template = list.find(t => t.id === normalized);
    return template ? this.attachEnabledFlag(template) : undefined;
  }

  getTemplates(options: { includeDisabled?: boolean } = {}): StrategyTemplate[] {
    const includeDisabled = options.includeDisabled ?? false;
    const list = includeDisabled ? this.allTemplates : this.templates;
    return list.map(template => this.attachEnabledFlag(template));
  }

  isTemplateEnabled(templateId: string): boolean {
    const normalized = typeof templateId === 'string' ? templateId.trim() : '';
    if (!normalized) {
      return false;
    }
    return !this.disabledTemplateIds.has(normalized);
  }

  private async loadDisabledTemplateIds(): Promise<void> {
    this.disabledTemplateIds.clear();
    try {
      const ids = await this.db.templates.getDisabledTemplateIds();
      ids.forEach((id) => this.disabledTemplateIds.add(id));
    } catch (error) {
      this.logging?.error('StrategyManager', 'Failed to load disabled templates from database', {
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  async ensureDefaultStrategyForTemplate(templateId: string): Promise<void> {
    if (typeof templateId !== 'string' || templateId.trim().length === 0) {
      throw new Error('templateId is required to ensure default strategy');
    }

    const normalized = templateId.trim();
    if (!this.isTemplateEnabled(normalized)) {
      this.logging?.info('StrategyManager', `Skipping default strategy for disabled template ${normalized}`);
      return;
    }

    if (normalized.startsWith('lightgbm_')) {
      await this.ensureLightgbmDefaultStrategies();
      return;
    }

    const template = this.getTemplate(normalized);
    if (!template) {
      this.logging?.warn('StrategyManager', `Template ${normalized} not found while ensuring default strategy`);
      return;
    }

    if (template.id === 'buy_and_hold') {
      const existingDefaults = await this.db.strategies.getStrategiesByIdLike('default_buy_and_hold_%');
      const existingDefaultStrategyIds = new Set(existingDefaults.map(s => s.id));
      await this.ensureBuyAndHoldDefaults(template, existingDefaultStrategyIds);
      return;
    }

    const defaultStrategyId = `default_${template.id}`;
    const existingDefault = await this.db.strategies.getStrategiesByIdLike(defaultStrategyId);
    if (existingDefault.some(strategy => strategy.id === defaultStrategyId)) {
      return;
    }

    const mergedParams = await this.resolveDefaultStrategyParameters(template, 'Recreating default strategy');
    const strategyName = `${template.name}`;

    try {
      await this.db.strategies.insertStrategy({
        id: defaultStrategyId,
        name: strategyName,
        templateId: template.id,
        parameters: mergedParams,
        status: 'active'
      });
      this.logging?.info('StrategyManager', `Recreated default strategy for ${template.id}`, { strategyId: defaultStrategyId });
    } catch (error) {
      this.logging?.error('StrategyManager', `Failed to recreate default strategy for ${template.id}`, {
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  async setTemplateEnabled(templateId: string, enabled: boolean): Promise<void> {
    if (typeof templateId !== 'string' || templateId.trim().length === 0) {
      throw new Error('templateId is required to update template status');
    }

    const normalized = templateId.trim();
    const template = this.getTemplate(normalized, { includeDisabled: true });
    if (!template) {
      throw new Error(`Template ${normalized} not found`);
    }

    const updated = await this.db.templates.setTemplateEnabled(normalized, enabled);
    if (!updated) {
      await this.registerTemplate(template);
      await this.db.templates.setTemplateEnabled(normalized, enabled);
    }

    if (enabled) {
      this.disabledTemplateIds.delete(normalized);
    } else {
      this.disabledTemplateIds.add(normalized);
    }
    this.rebuildTemplateLists();
  }

  async refreshTemplateAvailability(): Promise<void> {
    await this.loadDisabledTemplateIds();
    this.rebuildTemplateLists();
  }

  private async removeDeletedTemplates(): Promise<void> {
    const templateIds = this.allTemplates.map(t => t.id);
    try {
      const toRemove = await this.db.templates.getTemplatesNotIn(templateIds);
      if (toRemove.length > 0) {
        this.logging?.info(
          'StrategyManager',
          `Removing data for deleted templates ${toRemove.join(', ')}, this can take a long time`,
          { templateIds: toRemove, templateCount: toRemove.length }
        );
      }
      const result = await this.db.templates.removeTemplatesByIds(toRemove);
      if (result.length > 0) {
        this.logging?.info('StrategyManager', 'Removed templates missing JSON files', {
          templateIds: result
        });
      }
    } catch (error) {
      this.logging?.error('StrategyManager', 'Failed to remove templates missing JSON files', {
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  private async ensureBuyAndHoldDefaults(
    template: StrategyTemplate,
    existingDefaultStrategyIds: Set<string>
  ): Promise<void> {
    const defaultTickerConfigs = [
      { ticker: 'SPY', id: `default_${template.id}_spy`, name: `${template.name} SPY` },
      { ticker: 'QQQ', id: `default_${template.id}_qqq`, name: `${template.name} QQQ` }
    ];

    for (const { ticker, id, name } of defaultTickerConfigs) {
      if (existingDefaultStrategyIds.has(id)) {
        continue;
      }

      const parameters = this.buildDefaultParameters(template, { ticker });

      try {
        await this.db.strategies.insertStrategy({
          id,
          name,
          templateId: template.id,
          parameters,
          status: 'active'
        });
        existingDefaultStrategyIds.add(id);
        this.logging?.info('StrategyManager', `Created default buy-and-hold strategy for ${ticker}`, { strategyId: id });
      } catch (error) {
        this.logging?.error('StrategyManager', `Failed to create default buy-and-hold strategy for ${ticker}`, {
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }
  }

  async createStrategy(
    name: string,
    templateId: string,
    parameters: Record<string, any>,
    userId?: number,
    backtestStartDate?: Date | null,
    accountId?: string | null
  ): Promise<string> {
    // Validate template exists
    if (!this.isTemplateEnabled(templateId)) {
      throw new Error(`Template ${templateId} is disabled`);
    }
    const template = this.getTemplate(templateId);
    if (!template) {
      throw new Error(`Template ${templateId} not found`);
    }

    // Parse parameters to convert string form data to proper types
    const parsedParameters = this.parseParameters(template, parameters);

    // Validate parameters
    this.validateParameters(template, parsedParameters);

    // Create strategy in database
    const strategyId = await this.db.strategies.createStrategy({
      name,
      userId: userId ?? null,
      templateId,
      parameters: parsedParameters,
      status: 'active',
      backtestStartDate: backtestStartDate ?? null,
      accountId: accountId ?? null
    });

    if (!accountId) this.scheduleSignalGenerationJob('Triggered by new strategy creation', { strategyId });

    return strategyId;
  }

  async ensureLightgbmDefaults(): Promise<void> {
    await this.ensureLightgbmDefaultStrategies();
  }

  async ensureLightgbmModelTemplates(): Promise<void> {
    if (!LIGHTGBM_BASE_TEMPLATE) {
      return;
    }

    const models = await this.db.lightgbmModels.listLightgbmModels();
    const lightgbmTemplates = models.map(model => this.buildLightgbmTemplate(model.id, model.name));
    for (const template of lightgbmTemplates) {
      await this.registerTemplate(template);
    }

    this.lightgbmTemplates = lightgbmTemplates;
    this.rebuildTemplateLists();
  }

  private async ensureLightgbmDefaultStrategies(): Promise<void> {
    if (!LIGHTGBM_BASE_TEMPLATE) {
      return;
    }

    const models = await this.db.lightgbmModels.listLightgbmModels();
    if (!models.length) {
      return;
    }

    const existingDefaultStrategies = await this.db.strategies.getStrategiesByIdLike('default_lightgbm_%');
    const existingDefaultStrategyIds = new Set(existingDefaultStrategies.map(s => s.id));

    for (const model of models) {
      const strategyId = `default_lightgbm_${model.id}`;
      if (existingDefaultStrategyIds.has(strategyId)) {
        continue;
      }
      const templateId = `lightgbm_${model.id}`;
      if (!this.isTemplateEnabled(templateId)) {
        this.logging?.info('StrategyManager', `Skipping default strategy for disabled template ${templateId}`);
        continue;
      }
      const parameters = this.buildDefaultParameters(LIGHTGBM_BASE_TEMPLATE);
      const strategyName = `${LIGHTGBM_BASE_TEMPLATE.name} (${model.name})`;

      try {
        await this.db.strategies.insertStrategy({
          id: strategyId,
          name: strategyName,
          templateId,
          parameters,
          status: 'active'
        });
        existingDefaultStrategyIds.add(strategyId);
        this.logging?.info('StrategyManager', `Created default LightGBM strategy for ${model.name}`, {
          strategyId
        });
      } catch (error) {
        this.logging?.error('StrategyManager', `Failed to create default LightGBM strategy for ${model.name}`, {
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }
  }

  private buildLightgbmTemplate(modelId: string, modelName: string): StrategyTemplate {
    if (!LIGHTGBM_BASE_TEMPLATE) {
      throw new Error('LightGBM base template not loaded');
    }
    return {
      ...LIGHTGBM_BASE_TEMPLATE,
      id: `lightgbm_${modelId}`,
      name: `${LIGHTGBM_BASE_TEMPLATE.name}: ${modelName}`,
      description: `LightGBM model ${modelName}.`
    };
  }

  private scheduleSignalGenerationJob(description: string, metadata?: Record<string, any>): void {
    if (!this.jobScheduler) {
      this.logging?.warn('StrategyManager', 'Unable to schedule signal generation job: scheduler unavailable');
      return;
    }

    const hasPendingSignalJob = this.jobScheduler.hasPendingJob(job => job.type === 'generate-signals' && job.status === 'queued');
    if (hasPendingSignalJob) {
      return;
    }

    this.jobScheduler.scheduleJob('generate-signals', {
      description,
      metadata
    });

    this.logging?.info('StrategyManager', 'Scheduled generate-signals job after strategy creation', metadata);
  }

  parseParameters(template: StrategyTemplate, parameters: Record<string, any>): Record<string, any> {
    const parsedParameters: Record<string, any> = {};

    for (const paramDef of template.parameters) {
      const value = parameters[paramDef.name];
      if (value !== undefined) {
        // Convert string values to appropriate types based on template definition
        if (paramDef.type === 'number') {
          const numValue = parseFloat(value);
          if (isNaN(numValue)) {
            throw new Error(`Parameter ${paramDef.name} must be a valid number`);
          }
          parsedParameters[paramDef.name] = numValue;
        } else {
          // Keep string values as strings
          parsedParameters[paramDef.name] = value;
        }
      }
    }

    return parsedParameters;
  }

  private buildDefaultParameters(template: StrategyTemplate, overrides: Record<string, any> = {}): Record<string, any> {
    const parameters: Record<string, any> = {};
    for (const def of template.parameters) {
      if (Object.prototype.hasOwnProperty.call(overrides, def.name)) {
        parameters[def.name] = overrides[def.name];
      } else if (def.default !== undefined) {
        parameters[def.name] = def.default;
      }
    }
    return parameters;
  }

  private validateParameters(template: StrategyTemplate, parameters: Record<string, any>): void {
    for (const paramDef of template.parameters) {
      if (paramDef.required && !(paramDef.name in parameters)) {
        throw new Error(`Required parameter ${paramDef.name} is missing`);
      }

      const value = parameters[paramDef.name];
      if (value !== undefined) {
        // Type validation
        if (paramDef.type === 'number' && typeof value !== 'number') {
          throw new Error(`Parameter ${paramDef.name} must be a number`);
        }
        if (paramDef.type === 'string' && typeof value !== 'string') {
          throw new Error(`Parameter ${paramDef.name} must be a string`);
        }
        // Range validation for numbers
        if (paramDef.type === 'number') {
          if (paramDef.min !== undefined && value < paramDef.min) {
            throw new Error(`Parameter ${paramDef.name} must be >= ${paramDef.min}`);
          }
          if (paramDef.max !== undefined && value > paramDef.max) {
            throw new Error(`Parameter ${paramDef.name} must be <= ${paramDef.max}`);
          }
        }
      }
    }
  }
}


