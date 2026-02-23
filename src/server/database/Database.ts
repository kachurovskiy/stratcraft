import { Pool, PoolConfig } from 'pg';
import { readFileSync } from 'fs';
import { join } from 'path';
import { DbClient } from './core/DbClient';
import { AccountsRepo } from './repos/AccountsRepo';
import { AccountSignalSkipsRepo } from './repos/AccountSignalSkipsRepo';
import { SettingsRepo } from './repos/SettingsRepo';
import { SignalsRepo } from './repos/SignalsRepo';
import { SystemLogsRepo } from './repos/SystemLogsRepo';
import { TemplatesRepo } from './repos/TemplatesRepo';
import { AccountOperationsRepo } from './repos/AccountOperationsRepo';
import { BacktestCacheRepo } from './repos/BacktestCacheRepo';
import { BacktestResultsRepo } from './repos/BacktestResultsRepo';
import { CandlesRepo } from './repos/CandlesRepo';
import { LightgbmModelsRepo } from './repos/LightgbmModelsRepo';
import { AdminRepo } from './repos/AdminRepo';
import { MaintenanceRepo } from './repos/MaintenanceRepo';
import { RemoteOptimizerJobsRepo } from './repos/RemoteOptimizerJobsRepo';
import { StrategiesRepo } from './repos/StrategiesRepo';
import { TickersRepo } from './repos/TickersRepo';
import { TradesRepo } from './repos/TradesRepo';
import { UsersRepo } from './repos/UsersRepo';

export type {
  AccountOperationDispatchCandidate,
  BacktestCacheHistoryRow,
  BacktestCacheLookupResult,
  BacktestCachePerformancePoint,
  BacktestCacheResultRow,
  BacktestDailySnapshot,
  BacktestResultRecord,
  LightgbmDatasetStatsSummary,
  LightgbmModelCreateInput,
  LightgbmModelRecord,
  LightgbmModelSource,
  LightgbmValidationMetricsSummary,
  RawUserRow,
  RemoteOptimizerJobEntity,
  RequestQuotaAction,
  RequestQuotaCheckResult,
  RequestQuotaIdentifierType,
  TickerAssetRecord,
  TickerAssetType,
  TickerBacktestPerformanceRow,
  TradeTickerStats,
  TickerWithCandleStats,
  UserSessionRecord
} from './types';

export class Database {
  private poolConfig: PoolConfig;
  private pool: Pool;
  private core: DbClient;
  private initialized = false;
  private databaseEnsured = false;

  readonly admin: AdminRepo;
  readonly accounts: AccountsRepo;
  readonly maintenance: MaintenanceRepo;
  readonly signals: SignalsRepo;
  readonly settings: SettingsRepo;
  readonly systemLogs: SystemLogsRepo;
  readonly templates: TemplatesRepo;
  readonly trades: TradesRepo;
  readonly tickers: TickersRepo;
  readonly candles: CandlesRepo;
  readonly strategies: StrategiesRepo;
  readonly users: UsersRepo;
  readonly accountOperations: AccountOperationsRepo;
  readonly accountSignalSkips: AccountSignalSkipsRepo;
  readonly backtestResults: BacktestResultsRepo;
  readonly backtestCache: BacktestCacheRepo;
  readonly remoteOptimizerJobs: RemoteOptimizerJobsRepo;
  readonly lightgbmModels: LightgbmModelsRepo;

  constructor() {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString || connectionString.trim().length === 0) {
      throw new Error('DATABASE_URL is not set. Configure it in .env.');
    }
    this.poolConfig = {
      connectionString,
    };

    this.pool = new Pool(this.poolConfig);
    this.core = new DbClient(this.pool);
    this.signals = new SignalsRepo(this.core);
    this.settings = new SettingsRepo(this.core);
    this.systemLogs = new SystemLogsRepo(this.core);
    this.templates = new TemplatesRepo(this.core, this.settings);
    this.trades = new TradesRepo(this.core);
    this.tickers = new TickersRepo(this.core);
    this.admin = new AdminRepo(this.core);
    this.maintenance = new MaintenanceRepo(this.core, this.tickers);
    this.candles = new CandlesRepo(this.core);
    this.strategies = new StrategiesRepo(this.core);
    this.accounts = new AccountsRepo(this.core, this.strategies);
    this.users = new UsersRepo(this.core, this.strategies);
    this.accountOperations = new AccountOperationsRepo(this.core, this.trades);
    this.accountSignalSkips = new AccountSignalSkipsRepo(this.core);
    this.backtestResults = new BacktestResultsRepo(this.core);
    this.backtestCache = new BacktestCacheRepo(this.core, this.settings);
    this.remoteOptimizerJobs = new RemoteOptimizerJobsRepo(this.core);
    this.lightgbmModels = new LightgbmModelsRepo(this.core);

    this.pool.on('error', (err) => {
      console.error('Unexpected PostgreSQL error:', err);
    });
  }

  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      await this.ensureDatabaseExists();
      // Read and execute schema (for new tables)
      const schemaPath = join(__dirname, 'pg.sql');
      const schema = readFileSync(schemaPath, 'utf8');

      // Use exec for multiple SQL statements
      await this.core.exec(schema);

      this.initialized = true;
      // Database initialized successfully
    } catch (error) {
      console.error('Failed to initialize database:', error);
      throw error;
    }
  }

  private async ensureDatabaseExists(): Promise<void> {
    if (this.databaseEnsured) {
      return;
    }

    const connectionString = this.poolConfig.connectionString;
    if (!connectionString) {
      this.databaseEnsured = true;
      return;
    }

    const targetUrl = new URL(connectionString);
    const databaseNameRaw = targetUrl.pathname.replace(/^\//, '');
    const databaseName = databaseNameRaw ? decodeURIComponent(databaseNameRaw) : 'postgres';

    if (!databaseName || databaseName === 'postgres') {
      this.databaseEnsured = true;
      return;
    }

    const adminUrl = new URL(connectionString);
    adminUrl.pathname = '/postgres';

    const adminPool = new Pool({
      ...this.poolConfig,
      connectionString: adminUrl.toString()
    });

    try {
      const existsResult = await adminPool.query('SELECT 1 FROM pg_database WHERE datname = $1', [databaseName]);
      if (!existsResult || existsResult.rowCount === 0) {
        const escapedDbName = databaseName.replace(/"/g, '""');
        await adminPool.query(`CREATE DATABASE "${escapedDbName}"`);
      }
      this.databaseEnsured = true;
    } catch (error: unknown) {
      const code = (
        typeof error === 'object' &&
        error !== null &&
        'code' in error &&
        typeof (error as { code?: unknown }).code === 'string'
      )
        ? (error as { code: string }).code
        : null;

      if (code === '42P04') {
        // Database already exists (race condition)
        this.databaseEnsured = true;
        return;
      }
      throw error;
    } finally {
      await adminPool.end();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
