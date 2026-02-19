import express, { NextFunction, Request, Response } from 'express';
import {
  DEFAULT_AUTO_OPTIMIZATION_DELAY_SECONDS,
  DEFAULT_BACKTEST_INITIAL_CAPITAL,
  DEFAULT_MTLS_ACCESS_CERT_PASSWORD,
  SETTING_KEYS,
  SETTING_KEY_LIST,
  type SettingKey
} from '../constants';
import { DEFAULT_FOOTER_DISCLAIMER_HTML } from '../utils/footerDisclaimer';

const SETTING_GROUPS = [
  {
    key: 'app',
    label: 'App',
    description: 'Base URL and domain settings.'
  },
  {
    key: 'data-provider',
    label: 'Candle Provider',
    description: 'Choose the default candle data provider.'
  },
  {
    key: 'alpaca',
    label: 'Alpaca',
    description: 'Brokerage credentials and endpoints.'
  },
  {
    key: 'eodhd',
    label: 'EODHD',
    description: 'Configure the EODHD candle data source.'
  },
  {
    key: 'tiingo',
    label: 'Tiingo',
    description: 'Configure the Tiingo candle data source.'
  },
  {
    key: 'candle-sync',
    label: 'Candle Sync',
    description: 'Controls for candle refresh behavior.'
  },
  {
    key: 'expense-ratios',
    label: 'Expense Ratios',
    description: 'Defaults used for ETF expense assumptions.'
  },
  {
    key: 'engine',
    label: 'Engine',
    description: 'Execution and backtest tuning for the Rust engine.'
  },
  {
    key: 'ticker-rules',
    label: 'Ticker Rules',
    description: 'Overrides for ticker handling.'
  },
  {
    key: 'optimizer',
    label: 'Optimizer',
    description: 'Optimizer versioning and training windows.'
  },
  {
    key: 'param-scoring',
    label: 'Param Scoring',
    description: 'Tune parameter scoring thresholds for optimization results.'
  },
  {
    key: 'template-scoring',
    label: 'Template Scoring',
    description: 'Tune how template scores are computed for the gallery.'
  },
  {
    key: 'user-access',
    label: 'User Access',
    description: 'Invitation settings for user access.'
  },
  {
    key: 'email',
    label: 'Email',
    description: 'Delivery credentials for outbound email.'
  }
] as const;

type SettingGroup = (typeof SETTING_GROUPS)[number];
type SettingGroupKey = SettingGroup['key'];

type SettingDefinition = {
  key: SettingKey;
  group: SettingGroupKey;
  label: string;
  description?: string;
  placeholder?: string;
  inputType?: 'text' | 'password' | 'number';
  min?: string;
  isTextarea?: boolean;
  rows?: number;
};

const SETTINGS_DEFINITIONS: SettingDefinition[] = [
  {
    key: SETTING_KEYS.SITE_NAME,
    group: 'app',
    label: 'Site Name',
    description: 'Displayed throughout the UI and outbound emails.',
    placeholder: 'StratCraft',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.DOMAIN,
    group: 'app',
    label: 'Domain',
    description: 'Domain for this deployment (no protocol, no port).',
    placeholder: 'example.com',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.FOOTER_DISCLAIMER_HTML,
    group: 'app',
    label: 'Footer Disclaimer HTML',
    description: 'Shown in the site footer. Basic HTML allowed: a, br, strong, em, b, i, code, small.',
    placeholder: DEFAULT_FOOTER_DISCLAIMER_HTML,
    isTextarea: true,
    rows: 3
  },
  {
    key: SETTING_KEYS.TRADINGVIEW_CHARTS_ENABLED,
    group: 'app',
    label: 'TradingView Charts Enabled',
    description: 'Set to false to hide TradingView embeds on trade and ticker pages (true/false).',
    placeholder: 'true',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.CANDLE_DATA_PROVIDER,
    group: 'data-provider',
    label: 'Candle Data Provider',
    description: 'Provider to use for candle data (EODHD, TIINGO, or ALPACA).',
    placeholder: 'TIINGO',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.EODHD_BASE_URL,
    group: 'eodhd',
    label: 'EODHD Base URL',
    description: 'Base endpoint for EODHD candle data.',
    placeholder: 'https://eodhd.com/api/eod',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.EODHD_API_TOKEN,
    group: 'eodhd',
    label: 'EODHD API Token',
    description: 'API token for EODHD data downloads.',
    placeholder: 'Enter your EODHD token',
    inputType: 'password'
  },
  {
    key: SETTING_KEYS.EODHD_RATE_LIMIT_WAIT_SECONDS,
    group: 'eodhd',
    label: 'EODHD Rate Limit Wait (Seconds)',
    description: 'Seconds to wait after a 429 rate limit response before retrying.',
    placeholder: '60',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.TIINGO_BASE_URL,
    group: 'tiingo',
    label: 'Tiingo Base URL',
    description: 'Base endpoint for Tiingo candle data.',
    placeholder: 'https://api.tiingo.com/tiingo/daily',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.TIINGO_API_TOKEN,
    group: 'tiingo',
    label: 'Tiingo API Token',
    description: 'API token for Tiingo data downloads.',
    placeholder: 'Enter your Tiingo token',
    inputType: 'password'
  },
  {
    key: SETTING_KEYS.TIINGO_RATE_LIMIT_WAIT_SECONDS,
    group: 'tiingo',
    label: 'Tiingo Rate Limit Wait (Seconds)',
    description: 'Seconds to wait after a 429 rate limit response before retrying.',
    placeholder: '60',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.CANDLE_MISMATCH_THRESHOLD,
    group: 'candle-sync',
    label: 'Candle Mismatch Threshold',
    description: 'Relative threshold that triggers full candle reloads (e.g. 0.01 = 1%).',
    placeholder: '0.01',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.CANDLE_SYNC_MAX_CONCURRENT_UPDATES,
    group: 'candle-sync',
    label: 'Candle Sync Max Concurrent Updates',
    description: 'Maximum number of tickers to update in parallel during candle sync.',
    placeholder: '5',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.CANDLE_SYNC_MATCHING_RATIO_THRESHOLD,
    group: 'candle-sync',
    label: 'Candle Sync Matching Ratio Threshold',
    description: 'Skip refresh when this share of tickers matches SPY (e.g. 0.98 = 98%).',
    placeholder: '0.98',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.AUTO_DAILY_CANDLE_SYNC_ENABLED,
    group: 'candle-sync',
    label: 'Automatic Daily Candle Sync Enabled',
    description: 'Set to false to stop scheduling the daily midnight candle sync job (true/false).',
    placeholder: 'true',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.ETF_BASE_EXPENSE_RATIO,
    group: 'expense-ratios',
    label: 'ETF Base Expense Ratio',
    description: 'Default expense ratio for standard ETFs.',
    placeholder: '0.0008',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.INVERSE_ETF_EXPENSE_RATIO,
    group: 'expense-ratios',
    label: 'Inverse ETF Expense Ratio',
    description: 'Default expense ratio for inverse ETFs.',
    placeholder: '0.009',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.COMMODITY_TRUST_EXPENSE_RATIO,
    group: 'expense-ratios',
    label: 'Commodity Trust Expense Ratio',
    description: 'Default expense ratio for commodity trusts (e.g., precious metals).',
    placeholder: '0.004',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.BOND_ETF_EXPENSE_RATIO,
    group: 'expense-ratios',
    label: 'Bond ETF Expense Ratio',
    description: 'Default expense ratio for bond ETFs.',
    placeholder: '0.001',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.INCOME_ETF_EXPENSE_RATIO,
    group: 'expense-ratios',
    label: 'Income ETF Expense Ratio',
    description: 'Default expense ratio for income/covered-call ETFs.',
    placeholder: '0.007',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.LEVERAGED_2X_EXPENSE_RATIO,
    group: 'expense-ratios',
    label: 'Leveraged 2x Expense Ratio',
    description: 'Default expense ratio for 2x leveraged ETFs.',
    placeholder: '0.009',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.LEVERAGED_3X_EXPENSE_RATIO,
    group: 'expense-ratios',
    label: 'Leveraged 3x Expense Ratio',
    description: 'Default expense ratio for 3x leveraged ETFs.',
    placeholder: '0.0095',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.LEVERAGED_5X_EXPENSE_RATIO,
    group: 'expense-ratios',
    label: 'Leveraged 5x Expense Ratio',
    description: 'Default expense ratio for 5x leveraged ETFs.',
    placeholder: '0.015',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TRADE_CLOSE_FEE_RATE,
    group: 'engine',
    label: 'Trade Close Fee Rate',
    description: 'Fraction of notional charged on exit (e.g. 0.0005 = 0.05%).',
    placeholder: '0.0005',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TRADE_SLIPPAGE_RATE,
    group: 'engine',
    label: 'Trade Slippage Rate',
    description: 'Fractional slippage applied on entry/exit (e.g. 0.003 = 0.3%).',
    placeholder: '0.003',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.SHORT_BORROW_FEE_ANNUAL_RATE,
    group: 'engine',
    label: 'Short Borrow Fee (Annual)',
    description: 'Annualized borrow cost for short positions (e.g. 0.003 = 0.3% per year).',
    placeholder: '0.003',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TRADE_ENTRY_PRICE_MIN,
    group: 'engine',
    label: 'Trade Entry Price Min',
    description: 'Minimum supported price for trade entries.',
    placeholder: '0.10',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TRADE_ENTRY_PRICE_MAX,
    group: 'engine',
    label: 'Trade Entry Price Max',
    description: 'Maximum supported price for trade entries.',
    placeholder: '1000',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_FOR_ENTRY,
    group: 'engine',
    label: 'Minimum Dollar Volume For Entry',
    description: 'Minimum dollar volume required before entering a trade.',
    placeholder: '150000',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.MINIMUM_DOLLAR_VOLUME_LOOKBACK,
    group: 'engine',
    label: 'Minimum Dollar Volume Lookback (Days)',
    description: 'Number of candles to check for minimum dollar volume (0 disables the check).',
    placeholder: '5',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.BACKTEST_ACTIVE_MONTHS,
    group: 'engine',
    label: 'Backtest Active Months',
    description: 'Comma-separated or JSON array of months for backtest-active runs.',
    placeholder: '1,3,6,12,24,36,48,60,120',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.BACKTEST_INITIAL_CAPITAL,
    group: 'engine',
    label: 'Backtest Initial Capital (USD)',
    description: 'Initial capital used for optimization and non-account backtests.',
    placeholder: String(DEFAULT_BACKTEST_INITIAL_CAPITAL),
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.ALPACA_PAPER_URL,
    group: 'alpaca',
    label: 'Alpaca Paper URL',
    description: 'Base endpoint for Alpaca paper account data.',
    placeholder: 'Enter Alpaca paper base URL',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.ALPACA_LIVE_URL,
    group: 'alpaca',
    label: 'Alpaca Live URL',
    description: 'Base endpoint for Alpaca live account data.',
    placeholder: 'Enter Alpaca live base URL',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.ALPACA_DATA_BASE_URL,
    group: 'alpaca',
    label: 'Alpaca Market Data Base URL',
    description: 'Base endpoint for Alpaca market data.',
    placeholder: 'https://data.alpaca.markets/v2',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.ALPACA_API_KEY,
    group: 'alpaca',
    label: 'Alpaca API Key',
    description: 'API key for Alpaca brokerage.',
    placeholder: 'Enter your Alpaca API key',
    inputType: 'password'
  },
  {
    key: SETTING_KEYS.ALPACA_API_SECRET,
    group: 'alpaca',
    label: 'Alpaca API Secret',
    description: 'API secret for Alpaca brokerage.',
    placeholder: 'Enter your Alpaca API secret',
    inputType: 'password'
  },
  {
    key: SETTING_KEYS.ALPACA_DATA_RATE_LIMIT_WAIT_SECONDS,
    group: 'alpaca',
    label: 'Alpaca Market Data Rate Limit Wait (Seconds)',
    description: 'Seconds to wait after a 429 rate limit response before retrying.',
    placeholder: '60',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.IGNORED_TICKERS,
    group: 'ticker-rules',
    label: 'Ignored Tickers',
    description: 'JSON array or comma-separated tickers to exclude from Alpaca sync.',
    placeholder: '["DSHK","TNK","IONM"]',
    isTextarea: true,
    rows: 3
  },
  {
    key: SETTING_KEYS.ALWAYS_VALIDATION_TICKERS,
    group: 'ticker-rules',
    label: 'Always Validation Tickers',
    description: 'JSON array or comma-separated tickers always reserved for validation.',
    placeholder: '["SPY","QQQ"]',
    isTextarea: true,
    rows: 3
  },
  {
    key: SETTING_KEYS.TRAINING_ALLOCATION_RATIO,
    group: 'ticker-rules',
    label: 'Training Allocation Ratio',
    description: 'Fraction of tickers assigned to training (e.g. 0.7 = 70%).',
    placeholder: '0.7',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.AUTO_OPTIMIZATION_ENABLED,
    group: 'optimizer',
    label: 'Automatic Optimization Enabled',
    description: 'Set to false to stop scheduling idle-time optimization jobs (true/false).',
    placeholder: 'true',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.AUTO_OPTIMIZATION_DELAY_SECONDS,
    group: 'optimizer',
    label: 'Automatic Optimization Delay (Seconds)',
    description: 'Seconds to wait after the scheduler is idle before running auto optimization.',
    placeholder: String(DEFAULT_AUTO_OPTIMIZATION_DELAY_SECONDS),
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.LIGHTGBM_TRAINING_START_DATE,
    group: 'optimizer',
    label: 'LightGBM Training Start Date',
    description: 'Start date (YYYY-MM-DD) for LightGBM training and validation data.',
    placeholder: 'YYYY-MM-DD',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.LIGHTGBM_TRAINING_END_DATE,
    group: 'optimizer',
    label: 'LightGBM Training End Date',
    description: 'End date (YYYY-MM-DD) for LightGBM training and validation data.',
    placeholder: 'YYYY-MM-DD',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.OPTIMIZER_TRAINING_START_DATE,
    group: 'optimizer',
    label: 'Optimizer Training Start Date',
    description: 'Start date (YYYY-MM-DD) for the optimizer training window.',
    placeholder: 'YYYY-MM-DD',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.OPTIMIZER_TRAINING_END_DATE,
    group: 'optimizer',
    label: 'Optimizer Training End Date',
    description: 'End date (YYYY-MM-DD) for the optimizer training window.',
    placeholder: 'YYYY-MM-DD',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.VERIFY_WINDOW_START_DATE,
    group: 'optimizer',
    label: 'Optimizer Verify Start Date',
    description: 'Start date (YYYY-MM-DD) for verification runs.',
    placeholder: 'YYYY-MM-DD',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.VERIFY_WINDOW_END_DATE,
    group: 'optimizer',
    label: 'Optimizer Verify End Date',
    description: 'End date (YYYY-MM-DD) for verification runs.',
    placeholder: 'YYYY-MM-DD',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.BALANCE_WINDOW_START_DATE,
    group: 'optimizer',
    label: 'Optimizer Balance Start Date',
    description: 'Start date (YYYY-MM-DD) for training/validation balance runs.',
    placeholder: 'YYYY-MM-DD',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.BALANCE_WINDOW_END_DATE,
    group: 'optimizer',
    label: 'Optimizer Balance End Date',
    description: 'End date (YYYY-MM-DD) for training/validation balance runs.',
    placeholder: 'YYYY-MM-DD',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.LOCAL_OPTIMIZATION_VERSION,
    group: 'optimizer',
    label: 'Local Optimization Version',
    description: 'Numeric version used for optimizer cache invalidation.',
    placeholder: '9',
    inputType: 'number'
  },
  {
    key: SETTING_KEYS.OPTIMIZATION_OBJECTIVE,
    group: 'optimizer',
    label: 'Optimization Objective',
    description: 'Objective metric for local search (CAGR or SHARPE).',
    placeholder: 'CAGR',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.HETZNER_API_TOKEN,
    group: 'optimizer',
    label: 'Hetzner API Token',
    description: 'API token for provisioning remote optimization servers. Must have read and write access; see https://docs.hetzner.com/cloud/api/getting-started/generating-api-token/.',
    placeholder: 'Enter your Hetzner API token',
    inputType: 'password'
  },
  {
    key: SETTING_KEYS.HETZNER_SERVER_TYPE,
    group: 'optimizer',
    label: 'Hetzner Server Type',
    description: 'Hetzner Cloud server type for remote optimizers (e.g. cpx62).',
    placeholder: 'cpx62',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.HETZNER_SERVER_LOCATION,
    group: 'optimizer',
    label: 'Hetzner Server Location',
    description: 'Hetzner Cloud location code for remote optimizers (e.g. hel1).',
    placeholder: 'hel1',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.HETZNER_SSH_KEY_NAME,
    group: 'optimizer',
    label: 'Hetzner SSH Key Name',
    description: 'Name of the SSH key registered in Hetzner -> Projects -> Security.',
    placeholder: 'hetzner-node',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.HETZNER_PUBLIC_KEY,
    group: 'optimizer',
    label: 'Hetzner SSH Public Key',
    description: 'Public key to add in Hetzner -> Projects -> Security so remote optimizers can connect.',
    placeholder: 'ssh-ed25519 AAAA...',
    isTextarea: true,
    rows: 3
  },
  {
    key: SETTING_KEYS.HETZNER_PRIVATE_KEY,
    group: 'optimizer',
    label: 'Hetzner SSH Private Key',
    description: 'Private key paired with the Hetzner SSH public key used for remote optimizer access.',
    placeholder: '-----BEGIN PRIVATE KEY-----',
    inputType: 'password',
    isTextarea: true,
    rows: 6
  },
  {
    key: SETTING_KEYS.LOCAL_OPTIMIZATION_STEP_MULTIPLIERS,
    group: 'optimizer',
    label: 'Local Optimization Step Multipliers',
    description: 'Comma-separated list of step multipliers for optimizer neighbor search.',
    placeholder: '-5,-4,-3,-2,-1,1,2,3,4,5',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.MAX_ALLOWED_DRAWDOWN_RATIO,
    group: 'optimizer',
    label: 'Max Allowed Drawdown Ratio',
    description: 'Optimizer rejects candidates above this ratio (e.g. 0.40 = 40%).',
    placeholder: '0.40',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.BACKTEST_API_SECRET,
    group: 'optimizer',
    label: 'Backtest API Secret',
    description: 'Shared secret required for /api/backtest/* endpoints (remote optimizer cache).',
    placeholder: 'Auto-generated on server start',
    inputType: 'password'
  },
  {
    key: SETTING_KEYS.PARAM_SCORE_MIN_TRADES,
    group: 'param-scoring',
    label: 'Param Score Min Trades',
    description: 'Minimum total trades required for parameter scoring. Increase to filter low-trade runs; decrease to allow fewer trades.',
    placeholder: '20',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.PARAM_SCORE_DRAWDOWN_LAMBDA,
    group: 'param-scoring',
    label: 'Param Score Drawdown Lambda',
    description: 'Lambda for drawdown penalty in parameter scoring. Increase to penalize drawdowns more; decrease to be more forgiving.',
    placeholder: '3.5',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.PARAM_SCORE_NEIGHBOR_THRESHOLD,
    group: 'param-scoring',
    label: 'Param Score Neighbor Threshold',
    description: 'Distance threshold for neighbor matching in stability scoring. Increase to count more neighbors; decrease to be stricter.',
    placeholder: '0.15',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.PARAM_SCORE_CORE_SCORE_QUANTILE,
    group: 'param-scoring',
    label: 'Param Score Core Quantile',
    description: 'Quantile (0-1) used as the core-score cutoff for good neighbors. Increase to require higher core scores; decrease to be more lenient.',
    placeholder: '0.6',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.PARAM_SCORE_PAIRWISE_NEIGHBOR_LIMIT,
    group: 'param-scoring',
    label: 'Param Score Pairwise Neighbor Limit',
    description: 'Maximum candidate count for pairwise neighbor scoring. Increase to keep exact scoring on larger sets; decrease to switch to bucketed sooner.',
    placeholder: '1500',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_RETURN_SCALE,
    group: 'template-scoring',
    label: 'Template Return Scale',
    description: 'Scale for converting validation CAGR into return score. Increase to make returns saturate slower; decrease to make high CAGRs reach top scores sooner.',
    placeholder: '0.20',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_VALIDATION_NEGATIVE_PENALTY_STRENGTH,
    group: 'template-scoring',
    label: 'Negative Validation CAGR Penalty Strength',
    description: 'Penalty strength for negative validation CAGR. Increase to punish negative CAGR more; decrease to soften the penalty.',
    placeholder: '2.0',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_DRAWDOWN_LAMBDA,
    group: 'template-scoring',
    label: 'Drawdown Penalty Lambda',
    description: 'Lambda for drawdown penalty in the risk score. Increase to penalize drawdowns more; decrease to be more forgiving.',
    placeholder: '2.5',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_TRADE_TARGET,
    group: 'template-scoring',
    label: 'Trades Per Year Target',
    description: 'Trades-per-year target used to reach full liquidity confidence. Increase to require more trading for full credit; decrease to grant credit with fewer trades.',
    placeholder: '200',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_TRADE_WEIGHT,
    group: 'template-scoring',
    label: 'Liquidity Weight',
    description: 'Weight of liquidity confidence in period scoring (0-1). Increase to make trade activity matter more; decrease to de-emphasize liquidity.',
    placeholder: '0.25',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_RECENCY_HALF_LIFE_DAYS,
    group: 'template-scoring',
    label: 'Recency Half-Life (Days)',
    description: 'Half-life in days for recency weighting. Increase to reduce recency bias; decrease to favor newer backtests more.',
    placeholder: '365',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_SHARPE_SCALE,
    group: 'template-scoring',
    label: 'Verify Sharpe Scale',
    description: 'Scale for verification Sharpe normalization. Increase to dampen Sharpe boosts; decrease to reward Sharpe more quickly.',
    placeholder: '2',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CALMAR_SCALE,
    group: 'template-scoring',
    label: 'Verify Calmar Scale',
    description: 'Scale for verification Calmar normalization. Increase to dampen Calmar boosts; decrease to reward Calmar more quickly.',
    placeholder: '2',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_SCALE,
    group: 'template-scoring',
    label: 'Verify CAGR Positive Scale',
    description: 'Scale for positive verification CAGR normalization. Increase to slow positive boosts; decrease to reward positive CAGR faster.',
    placeholder: '0.25',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_CAGR_NEG_SCALE,
    group: 'template-scoring',
    label: 'Verify CAGR Negative Scale',
    description: 'Scale for negative verification CAGR penalty. Increase to soften negative penalties; decrease to punish negative CAGR faster.',
    placeholder: '0.10',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_DRAWDOWN_LAMBDA,
    group: 'template-scoring',
    label: 'Verify Drawdown Lambda',
    description: 'Lambda for verification drawdown penalty. Increase to penalize verify drawdowns more; decrease to be more forgiving.',
    placeholder: '2.5',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER,
    group: 'template-scoring',
    label: 'Verify Min Multiplier',
    description: 'Minimum multiplier when verification score is weak. Increase to raise the floor; decrease to allow harsher penalties.',
    placeholder: '0.8',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.TEMPLATE_SCORE_VERIFY_MAX_MULTIPLIER,
    group: 'template-scoring',
    label: 'Verify Max Multiplier',
    description: 'Maximum multiplier when verification score is strong. Increase to allow bigger boosts; decrease to cap the upside.',
    placeholder: '1.2',
    inputType: 'number',
    min: '0'
  },
  {
    key: SETTING_KEYS.INVITE_LINK_VALID_DAYS,
    group: 'user-access',
    label: 'Invite Link Valid Days',
    description: 'Number of days an invitation link remains valid.',
    placeholder: '7',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.SESSION_COOKIE_VALID_DAYS,
    group: 'user-access',
    label: 'Session Cookie Valid Days',
    description: 'Number of days a login session cookie stays valid.',
    placeholder: '30',
    inputType: 'number',
    min: '1'
  },
  {
    key: SETTING_KEYS.MTLS_ACCESS_CERT_PASSWORD,
    group: 'user-access',
    label: 'Access Certificate Password',
    description: `Password used when exporting stratcraft-access.p12 (Android requires a non-empty password). Default: ${DEFAULT_MTLS_ACCESS_CERT_PASSWORD}.`,
    placeholder: DEFAULT_MTLS_ACCESS_CERT_PASSWORD,
    inputType: 'password'
  },
  {
    key: SETTING_KEYS.EMAIL_SECURITY_EMOJI,
    group: 'email',
    label: 'Email Security Emoji',
    description: 'Prepended to every outbound email subject to help detect phishing.',
    placeholder: '🔒',
    inputType: 'text'
  },
  {
    key: SETTING_KEYS.RESEND_API_KEY,
    group: 'email',
    label: 'Resend API Key',
    description: 'API key used to send outbound emails via Resend.',
    placeholder: 'Enter your Resend API key',
    inputType: 'password'
  }
];

const router = express.Router();

const requireAuth = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
};

const requireAdmin = (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
};

router.get('/', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const settingsMap = await req.db.settings.getSettingsByKeys(SETTING_KEY_LIST);
    const settings = SETTINGS_DEFINITIONS.map(definition => {
      const rawValue = settingsMap[definition.key];
      const value = typeof rawValue === 'string' ? rawValue : '';

      if (definition.inputType === 'password' && value.length > 0) {
        return {
          ...definition,
          value: '*'.repeat(value.length)
        };
      }

      return {
        ...definition,
        value
      };
    });

    const settingsByGroup = SETTING_GROUPS.map(group => ({
      ...group,
      settings: settings.filter(definition => definition.group === group.key)
    })).filter(group => group.settings.length > 0);

    res.render('pages/settings', {
      title: 'Settings',
      page: 'settings',
      user: req.user,
      groups: settingsByGroup,
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error loading settings page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load settings page'
    });
  }
});

router.post('/', requireAuth, requireAdmin, async (req: Request, res: Response) => {
  try {
    const existingSettings = await req.db.settings.getSettingsByKeys(SETTING_KEY_LIST);
    const updates: Record<string, string> = {};
    for (const definition of SETTINGS_DEFINITIONS) {
      const rawValue = req.body?.[definition.key];
      const inputValue = Array.isArray(rawValue)
        ? String(rawValue[0] ?? '')
        : typeof rawValue === 'string'
          ? rawValue
          : '';
      const trimmedValue = inputValue.trim();
      const existingValue =
        typeof existingSettings[definition.key] === 'string'
          ? (existingSettings[definition.key] as string)
          : '';

      if (definition.inputType === 'password') {
        const maskedValue = existingValue.length > 0 ? '*'.repeat(existingValue.length) : '';
        if (trimmedValue.length === 0 || (maskedValue && trimmedValue === maskedValue)) {
          continue;
        }
      }

      updates[definition.key] = trimmedValue;
    }

    await req.db.settings.upsertSettings(updates);
    await req.jobScheduler.refreshAutoOptimizationSettings();

    const hasPendingMarketDataJob = req.jobScheduler.hasPendingJob(
      job => job.type === 'export-market-data'
    );
    if (!hasPendingMarketDataJob) {
      req.jobScheduler.scheduleJob('export-market-data', {
        description: 'Refresh market data snapshot after settings update',
        metadata: {
          trigger: 'settings-update',
          updatedKeys: Object.keys(updates)
        }
      });
    }

    res.redirect('/admin/settings?success=Settings updated successfully');
  } catch (error) {
    console.error('Error updating settings:', error);
    res.redirect('/admin/settings?error=Failed to update settings');
  }
});

export default router;
