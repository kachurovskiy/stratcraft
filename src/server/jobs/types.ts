import { Database } from '../database/Database';
import { CandleClient } from '../api/CandleClient';
import { EngineCliService } from './EngineCliService';
import { EmailService } from '../services/EmailService';
import { AccountDataService } from '../services/AccountDataService';
import { AlpacaAssetService } from '../services/AlpacaAssetService';
import { StrategyRegistry } from '../strategies/registry';

export interface JobHandlerDependencies {
  db: Database;
  candleClient: CandleClient;
  engineCli: EngineCliService;
  emailService: EmailService;
  accountDataService: AccountDataService;
  alpacaAssetService: AlpacaAssetService;
  strategyRegistry: StrategyRegistry;
}
