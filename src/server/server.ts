import express from 'express';
import { Server as HttpServer } from 'http';
import { create } from 'express-handlebars';
import path from 'path';
import fs from 'fs';
import cookieParser from 'cookie-parser';
import dotenv from 'dotenv';
import { randomBytes } from 'crypto';
import { Database } from './database/Database';
import { CandleClient } from './api/CandleClient';
import { StrategyRegistry } from './strategies/registry';
import { InlineAssetsMiddleware } from './middleware/inlineAssets';
import { AuthService } from './services/AuthService';
import { AuthMiddleware } from './middleware/auth';
import { csrfMiddleware } from './middleware/csrf';
import { EmailService } from './services/EmailService';
import { CpuMetricsService } from './services/CpuMetricsService';
import { LoggingService } from './services/LoggingService';
import { MtlsLockdownService } from './services/MtlsLockdownService';
import { RemoteOptimizationService } from './services/RemoteOptimizationService';
import { AccountDataService } from './services/AccountDataService';
import { AlpacaAssetService } from './services/AlpacaAssetService';
import { JobScheduler } from './jobs/JobScheduler';
import { createJobHandlers } from './jobs/createJobHandlers';
import { EngineCliService } from './jobs/EngineCliService';
import { viewHelpers } from './views/helpers';
import { SETTING_KEYS } from './constants';
import { normalizeDomain, resolveSiteName } from './utils/appUrl';
import { DEFAULT_FOOTER_DISCLAIMER_HTML, resolveFooterDisclaimerHtml } from './utils/footerDisclaimer';
import { randomEmoji } from './utils/randomEmoji';

// Import routes
import apiRoutes from './routes/api';
import pageRoutes from './routes/pages';
import authRoutes from './routes/auth';
import deploymentRoutes from './routes/deployment';
import adminUserRoutes from './routes/users';
import adminLogRoutes from './routes/logs';
import adminJobsRoutes from './routes/jobs';
import adminLightgbmRoutes from './routes/lightgbm';
import accountRoutes from './routes/accounts';
import dashboardRoutes from './routes/dashboard';
import tickersRoutes from './routes/tickers';
import strategiesRoutes from './routes/strategies';
import templatesRoutes from './routes/templates';
import tradesRoutes from './routes/trades';
import settingsRoutes from './routes/settings';
import databaseRoutes from './routes/database';

dotenv.config();

export class Server {
  private app: express.Application;
  private db: Database;
  private candleClient: CandleClient;
  private strategyRegistry: StrategyRegistry;
  private inlineAssets: InlineAssetsMiddleware;
  private authService: AuthService;
  private authMiddleware: AuthMiddleware;
  private emailService: EmailService;
  private cpuMetricsService: CpuMetricsService;
  private loggingService: LoggingService;
  private accountDataService: AccountDataService;
  private alpacaAssetService: AlpacaAssetService;
  private engineCliService: EngineCliService;
  private jobScheduler: JobScheduler;
  private remoteOptimizationService: RemoteOptimizationService;
  private mtlsLockdownService: MtlsLockdownService;
  private server: HttpServer | null = null;

  constructor() {
    this.app = express();
    this.app.set('trust proxy', process.env.NODE_ENV === 'production' ? 'loopback' : false);
    this.db = new Database();
    this.loggingService = new LoggingService(this.db);
    this.mtlsLockdownService = new MtlsLockdownService();
    this.candleClient = new CandleClient(this.db, this.loggingService);
    this.emailService = new EmailService(this.loggingService, this.db, this.mtlsLockdownService);
    this.inlineAssets = new InlineAssetsMiddleware();
    this.authService = new AuthService(this.db, this.loggingService);
    this.authMiddleware = new AuthMiddleware(this.authService, this.loggingService);
    this.cpuMetricsService = new CpuMetricsService();
    this.accountDataService = new AccountDataService(this.loggingService, this.db);
    this.alpacaAssetService = new AlpacaAssetService(this.loggingService, this.db);
    this.engineCliService = new EngineCliService(this.loggingService);
    this.strategyRegistry = new StrategyRegistry(this.db, this.loggingService);
    this.jobScheduler = new JobScheduler(
      this.loggingService,
      createJobHandlers({
        db: this.db,
        candleClient: this.candleClient,
        engineCli: this.engineCliService,
        emailService: this.emailService,
        accountDataService: this.accountDataService,
        alpacaAssetService: this.alpacaAssetService,
        strategyRegistry: this.strategyRegistry
      }),
      this.db
    );
    this.strategyRegistry.setJobScheduler(this.jobScheduler);
    this.remoteOptimizationService = new RemoteOptimizationService(
      this.loggingService,
      this.emailService,
      this.db,
      this.jobScheduler,
      this.mtlsLockdownService
    );
  }

  private async ensureBacktestApiSecret(): Promise<void> {
    const rawValue = await this.db.settings.getSettingValue(SETTING_KEYS.BACKTEST_API_SECRET);
    const existing = typeof rawValue === 'string' ? rawValue.trim() : '';
    if (existing.length > 0) {
      return;
    }

    const generated = randomBytes(32).toString('hex');
    await this.db.settings.upsertSettings({
      [SETTING_KEYS.BACKTEST_API_SECRET]: generated
    });
    this.loggingService.info('system', 'Generated backtest API secret for remote cache endpoints.');
  }

  private async ensureEmailSecurityEmoji(): Promise<void> {
    if (await this.db.settings.getSettingValue(SETTING_KEYS.EMAIL_SECURITY_EMOJI)) return;

    const generated = randomEmoji();
    await this.db.settings.upsertSettings({
      [SETTING_KEYS.EMAIL_SECURITY_EMOJI]: generated
    });
    this.loggingService.info('system', `Generated email security emoji for outbound subjects: ${generated}`);
  }

  private async ensureDomainSetting(): Promise<void> {
    const envDomain = normalizeDomain(process.env.DOMAIN);
    if (!envDomain) {
      return;
    }

    const existingValue = await this.db.settings.getSettingValue(SETTING_KEYS.DOMAIN);
    const existingDomain = normalizeDomain(existingValue);
    if (existingDomain) {
      return;
    }

    await this.db.settings.upsertSettings({
      [SETTING_KEYS.DOMAIN]: envDomain
    });
    this.loggingService.info('system', 'Saved DOMAIN from environment into settings.');
  }

  async initialize(): Promise<void> {
    // Initialize database
    await this.db.initialize();
    await this.ensureDomainSetting();
    await this.ensureBacktestApiSecret();
    await this.ensureEmailSecurityEmoji();
    await this.jobScheduler.refreshAutoOptimizationSettings();
    this.cpuMetricsService.start();
    try {
      await this.remoteOptimizationService.ensureHetznerSshKeys();
    } catch (error) {
      this.loggingService.warn('system', 'Failed to ensure Hetzner SSH keys', {
        error: error instanceof Error ? error.message : String(error)
      });
    }

    // Initialize strategy registry
    await this.strategyRegistry.initialize();

    // Initialize inline assets middleware
    await this.inlineAssets.initialize();

    // Setup Handlebars
    const hbs = create({
      extname: '.hbs',
      defaultLayout: 'main',
      layoutsDir: path.join(__dirname, '../views/layouts'),
      partialsDir: path.join(__dirname, '../views/partials'),
      helpers: viewHelpers
    });

    this.app.engine('hbs', hbs.engine);
    this.app.set('view engine', 'hbs');
    this.app.set('views', path.join(__dirname, '../views'));

    // Middleware
    this.app.use(express.json());
    this.app.use(express.urlencoded({ extended: true }));
    this.app.use(cookieParser());
    this.app.use(csrfMiddleware);

    // Add inline assets middleware before routes
    this.app.use(this.inlineAssets.middleware());

    const manifestPath = path.join(__dirname, '../public/manifest.json');
    let manifestTemplate: Record<string, unknown> | null = null;
    try {
      manifestTemplate = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    } catch {
      manifestTemplate = null;
    }

    this.app.get('/manifest.json', async (_req, res) => {
      try {
        const siteName = await resolveSiteName(this.db);
        res.type('application/manifest+json').send({
          ...(manifestTemplate ?? {}),
          name: siteName,
          short_name: siteName
        });
      } catch {
        if (manifestTemplate) {
          res.type('application/manifest+json').send(manifestTemplate);
        } else {
          res.status(404).end();
        }
      }
    });

    // Serve static files from public directory
    this.app.use(express.static(path.join(__dirname, '../public')));

    // Make services available to routes
    this.app.use((req: any, res: any, next: any) => {
      req.db = this.db;
      req.candleClient = this.candleClient;
      req.strategyRegistry = this.strategyRegistry;
      req.authService = this.authService;
      req.authMiddleware = this.authMiddleware;
      req.cpuMetricsService = this.cpuMetricsService;
      req.emailService = this.emailService;
      req.loggingService = this.loggingService;
      req.accountDataService = this.accountDataService;
      req.jobScheduler = this.jobScheduler;
      req.remoteOptimizerService = this.remoteOptimizationService;
      req.mtlsLockdownService = this.mtlsLockdownService;
      next();
    });

    this.app.use(async (_req: any, res: any, next: any) => {
      try {
        const [siteName, footerDisclaimerHtml] = await Promise.all([
          resolveSiteName(this.db),
          resolveFooterDisclaimerHtml(this.db)
        ]);
        res.locals.siteName = siteName;
        res.locals.footerDisclaimerHtml = footerDisclaimerHtml;
      } catch {
        res.locals.siteName = 'StratCraft';
        res.locals.footerDisclaimerHtml = DEFAULT_FOOTER_DISCLAIMER_HTML;
      }
      next();
    });

    // Routes
    this.app.use('/auth', authRoutes);
    this.app.use('/admin/deployment', deploymentRoutes);
    this.app.use('/admin', adminUserRoutes);
    this.app.use('/admin/logs', adminLogRoutes);
    this.app.use('/admin/lightgbm', adminLightgbmRoutes);
    this.app.use('/admin/jobs', adminJobsRoutes);
    this.app.use('/api', apiRoutes);
    this.app.use('/accounts', accountRoutes);
    this.app.use('/dashboard', dashboardRoutes);
    this.app.use('/tickers', tickersRoutes);
    this.app.use('/templates', templatesRoutes);
    this.app.use('/admin/settings', settingsRoutes);
    this.app.use('/admin/database', databaseRoutes);
    this.app.use('/', tradesRoutes);
    this.app.use('/', strategiesRoutes);
    this.app.use('/', pageRoutes);

    // Error handling middleware
    this.app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
      console.error('Error:', err);
      res.status(500).render('pages/error', {
        title: 'Error',
        error: err.message || 'Internal Server Error'
      });
    });

    // 404 handler
    this.app.use((req: express.Request, res: express.Response) => {
      res.status(404).render('pages/error', {
        title: 'Page Not Found',
        error: 'The page you are looking for does not exist.'
      });
    });
  }

  async start(): Promise<void> {
    await this.initialize();

    const parsedPort = Number.parseInt(process.env.SERVER_PORT ?? '3000', 10);
    const serverPort = Number.isFinite(parsedPort) ? parsedPort : 3000;
    const serverHost = process.env.SERVER_HOST || 'localhost';

    return new Promise((resolve, reject) => {
      this.server = this.app.listen(serverPort, serverHost, async (err?: any) => {
        if (err) {
          reject(err);
        } else {
          this.loggingService.info('webserver', `Server running at http://${serverHost}:${serverPort}`);
          this.loggingService.warn(
            'system',
            'Disclaimer: StratCraft is not financial advice. Most retail traders lose money. Use at your own risk.'
          );

          try {
            this.loggingService.info('system', 'Scheduling engine compilation job');
            this.jobScheduler.scheduleJob('engine-compile', {
              description: 'Initial engine build on server start'
            });
          } catch (error) {
            console.error('Error scheduling engine compile job:', error);
            this.loggingService.error('system', 'Failed to schedule engine compile job', {
              error: error instanceof Error ? error.message : String(error)
            });
          }

          void this.mtlsLockdownService.handleExpiredClientCertificateOnStartup({
            loggingService: this.loggingService,
            db: this.db,
            emailService: this.emailService
          });

          resolve();
        }
      });
    });
  }


  async stop(): Promise<void> {
    return new Promise(async (resolve, reject) => {
      try {
        this.loggingService.info('webserver', 'Server shutdown initiated');

        // Stop job scheduler and background work
        await this.jobScheduler.shutdown();
        this.loggingService.info('system', 'Job scheduler stopped');

        this.cpuMetricsService.stop();

        // Shutdown logging service
        await this.loggingService.shutdown();

        if (this.server) {
          this.server.close((err?: any) => {
            if (err) {
              reject(err);
            } else {
              this.loggingService.info('webserver', 'Server stopped');
              resolve();
            }
          });
        } else {
          resolve();
        }
      } catch (error) {
        reject(error);
      }
    });
  }

  getApp(): express.Application {
    return this.app;
  }

  getDatabase(): Database {
    return this.db;
  }

  getCandleClient(): CandleClient {
    return this.candleClient;
  }

  getStrategyRegistry(): StrategyRegistry {
    return this.strategyRegistry;
  }

  getInlineAssets(): InlineAssetsMiddleware {
    return this.inlineAssets;
  }

  getJobScheduler(): JobScheduler {
    return this.jobScheduler;
  }

  getLoggingService(): LoggingService {
    return this.loggingService;
  }
}

// Main entry point - start the server if this file is run directly
if (require.main === module) {
  const server = new Server();

  const createShutdownHandler = (signal: NodeJS.Signals) => async (): Promise<void> => {
    server.getLoggingService().warn('system', `Received ${signal}, shutting down gracefully...`);
    try {
      await server.stop();
      process.exit(0);
    } catch (error) {
      console.error('Error during shutdown:', error);
      process.exit(1);
    }
  };

  // Handle graceful shutdown
  process.on('SIGINT', createShutdownHandler('SIGINT'));

  process.on('SIGTERM', createShutdownHandler('SIGTERM'));

  // Start the server
  server.start().catch((error) => {
    console.error('Failed to start server:', error);
    process.exit(1);
  });
}
