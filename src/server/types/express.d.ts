import type { CandleClient } from '../api/CandleClient';
import type { Database } from '../database/Database';
import type { JobScheduler } from '../jobs/JobScheduler';
import type { AuthMiddleware } from '../middleware/auth';
import type { AccountDataService } from '../services/AccountDataService';
import type { AuthService } from '../services/AuthService';
import type { CpuMetricsService } from '../services/CpuMetricsService';
import type { EmailService } from '../services/EmailService';
import type { LoggingService } from '../services/LoggingService';
import type { MtlsLockdownService } from '../services/MtlsLockdownService';
import type { RemoteOptimizationService } from '../services/RemoteOptimizationService';
import type { StrategyRegistry } from '../strategies/registry';

declare global {
  namespace Express {
    interface Request {
      db: Database;
      candleClient: CandleClient;
      strategyRegistry: StrategyRegistry;
      authService: AuthService;
      authMiddleware: AuthMiddleware;
      cpuMetricsService: CpuMetricsService;
      emailService: EmailService;
      loggingService: LoggingService;
      mtlsLockdownService: MtlsLockdownService;
      accountDataService: AccountDataService;
      jobScheduler: JobScheduler;
      remoteOptimizerService: RemoteOptimizationService;
    }
  }
}

export {};
