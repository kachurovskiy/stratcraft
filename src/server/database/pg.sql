-- StratCraft Database Schema (PostgreSQL)
-- Converted from the SQLite schema to support the migration to PostgreSQL.
-- Data migration is intentionally omitted; the new database starts empty.

-- Core reference tables
CREATE TABLE IF NOT EXISTS tickers (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    tradable BOOLEAN NOT NULL DEFAULT TRUE,
    shortable BOOLEAN NOT NULL DEFAULT FALSE,
    easy_to_borrow BOOLEAN NOT NULL DEFAULT FALSE,
    asset_type TEXT,
    expense_ratio DOUBLE PRECISION,
    market_cap DOUBLE PRECISION,
    volume_usd DOUBLE PRECISION,
    max_fluctuation_ratio DOUBLE PRECISION,
    last_updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    training BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS candles (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    unadjusted_close DOUBLE PRECISION,
    volume_shares BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ticker, date),
    FOREIGN KEY (ticker) REFERENCES tickers(symbol)
);

CREATE TABLE IF NOT EXISTS templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT,
    author TEXT,
    version TEXT,
    local_optimization_version INTEGER DEFAULT 0,
    parameters TEXT NOT NULL,
    example_usage TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lightgbm_models (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    tree_text TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    num_iterations INTEGER,
    learning_rate DOUBLE PRECISION,
    num_leaves INTEGER,
    max_depth INTEGER,
    min_data_in_leaf INTEGER,
    min_gain_to_split DOUBLE PRECISION,
    lambda_l1 DOUBLE PRECISION,
    lambda_l2 DOUBLE PRECISION,
    feature_fraction DOUBLE PRECISION,
    bagging_fraction DOUBLE PRECISION,
    bagging_freq INTEGER,
    early_stopping_round INTEGER,
    train_dataset_stats JSONB,
    validation_dataset_stats JSONB,
    validation_metrics JSONB,
    engine_stdout TEXT,
    engine_stderr TEXT,
    trained_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lightgbm_models_active ON lightgbm_models(is_active);

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    otp_code TEXT,
    otp_expires_at TIMESTAMPTZ,
    invite_token_hash TEXT,
    invite_expires_at TIMESTAMPTZ,
    invite_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_token TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_ip TEXT,
    device_type TEXT NOT NULL DEFAULT 'unknown',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS request_quotas (
    id BIGSERIAL PRIMARY KEY,
    action TEXT NOT NULL,
    identifier_type TEXT NOT NULL,
    identifier TEXT NOT NULL,
    window_started_at TIMESTAMPTZ NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (action, identifier_type, identifier)
);

CREATE INDEX IF NOT EXISTS idx_request_quotas_action ON request_quotas(action);

CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    environment TEXT NOT NULL DEFAULT 'paper',
    excluded_tickers TEXT NOT NULL DEFAULT '[]',
    excluded_keywords TEXT NOT NULL DEFAULT '[]',
    api_key TEXT NOT NULL,
    api_secret TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS strategies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    user_id BIGINT,
    account_id TEXT,
    template_id TEXT NOT NULL,
    parameters TEXT NOT NULL,
    backtest_start_date TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'active',
    last_backtest_duration_minutes DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (template_id) REFERENCES templates(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS backtest_results (
    id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ NOT NULL,
    period_days INTEGER NOT NULL DEFAULT 0,
    period_months INTEGER NOT NULL DEFAULT 0,
    initial_capital DOUBLE PRECISION NOT NULL,
    final_portfolio_value DOUBLE PRECISION NOT NULL,
    performance TEXT NOT NULL,
    daily_snapshots TEXT NOT NULL,
    tickers TEXT NOT NULL,
    ticker_scope TEXT NOT NULL DEFAULT 'training',
    strategy_state TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id)
);

CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    user_id BIGINT,
    backtest_result_id TEXT,
    ticker TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    pnl DOUBLE PRECISION,
    fee DOUBLE PRECISION NOT NULL DEFAULT 0,
    exit_price DOUBLE PRECISION,
    exit_date DATE,
    stop_loss DOUBLE PRECISION,
    stop_loss_triggered BOOLEAN DEFAULT FALSE,
    entry_order_id TEXT,
    entry_cancel_after TIMESTAMPTZ,
    stop_order_id TEXT,
    exit_order_id TEXT,
    changes TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (ticker) REFERENCES tickers(symbol),
    FOREIGN KEY (backtest_result_id) REFERENCES backtest_results(id)
);

CREATE TABLE IF NOT EXISTS account_operations (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    quantity INTEGER,
    price DOUBLE PRECISION,
    stop_loss DOUBLE PRECISION,
    previous_stop_loss DOUBLE PRECISION,
    triggered_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    status_reason TEXT,
    status_updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    reason TEXT,
    order_id TEXT,
    last_payload TEXT,
    order_type TEXT,
    discount_applied BOOLEAN,
    signal_confidence DOUBLE PRECISION,
    account_cash_at_plan DOUBLE PRECISION,
    days_held INTEGER,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(id),
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS account_signal_skips (
    id BIGSERIAL PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    account_id TEXT,
    ticker TEXT NOT NULL,
    signal_date DATE NOT NULL,
    action TEXT NOT NULL,
    source TEXT NOT NULL,
    reason TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS system_logs (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    metadata TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS settings (
    setting_key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS backtest_cache (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL,
    parameters TEXT NOT NULL,
    sharpe_ratio DOUBLE PRECISION NOT NULL,
    calmar_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_return DOUBLE PRECISION NOT NULL,
    cagr DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_drawdown DOUBLE PRECISION NOT NULL,
    max_drawdown_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    verify_sharpe_ratio DOUBLE PRECISION,
    verify_calmar_ratio DOUBLE PRECISION,
    verify_cagr DOUBLE PRECISION,
    verify_max_drawdown_ratio DOUBLE PRECISION,
    win_rate DOUBLE PRECISION NOT NULL,
    total_trades INTEGER NOT NULL,
    ticker_count INTEGER NOT NULL,
    start_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ NOT NULL,
    period_days INTEGER NOT NULL DEFAULT 0,
    period_months INTEGER NOT NULL DEFAULT 0,
    duration_minutes DOUBLE PRECISION,
    tool TEXT,
    top_abs_gain_ticker TEXT,
    top_rel_gain_ticker TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signals (
    id TEXT PRIMARY KEY,
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    user_id BIGINT,
    action TEXT NOT NULL,
    confidence DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE CASCADE,
    FOREIGN KEY (ticker) REFERENCES tickers(symbol),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS remote_optimizer_jobs (
    id UUID PRIMARY KEY,
    template_id TEXT NOT NULL,
    template_name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    hetzner_server_id INTEGER,
    remote_server_ip TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO settings (setting_key, value)
VALUES
    ('SITE_NAME', 'StratCraft'),
    ('DOMAIN', ''),
    ('TRADINGVIEW_CHARTS_ENABLED', 'true'),
    ('CANDLE_DATA_PROVIDER', 'TIINGO'),
    ('EODHD_BASE_URL', 'https://eodhd.com/api/eod'),
    ('EODHD_RATE_LIMIT_WAIT_SECONDS', '60'),
    ('TIINGO_BASE_URL', 'https://api.tiingo.com/tiingo/daily'),
    ('TIINGO_RATE_LIMIT_WAIT_SECONDS', '60'),
    ('CANDLE_MISMATCH_THRESHOLD', '0.01'),
    ('ALPACA_PAPER_URL', 'https://paper-api.alpaca.markets/v2'),
    ('ALPACA_LIVE_URL', 'https://api.alpaca.markets/v2'),
    ('ALPACA_DATA_BASE_URL', 'https://data.alpaca.markets/v2'),
    ('ALPACA_DATA_RATE_LIMIT_WAIT_SECONDS', '60'),
    ('IGNORED_TICKERS', '[]'),
    ('ALWAYS_VALIDATION_TICKERS', '["SPY","QQQ"]'),
    ('TRAINING_ALLOCATION_RATIO', '0.7'),
    ('LIGHTGBM_TRAINING_START_DATE', '2021-01-01'),
    ('LIGHTGBM_TRAINING_END_DATE', '2024-12-31'),
    ('OPTIMIZER_TRAINING_START_DATE', '2021-01-01'),
    ('OPTIMIZER_TRAINING_END_DATE', '2024-12-31'),
    ('VERIFY_WINDOW_START_DATE', '2025-01-01'),
    ('VERIFY_WINDOW_END_DATE', '2025-12-31'),
    ('LOCAL_OPTIMIZATION_VERSION', '9'),
    ('OPTIMIZATION_OBJECTIVE', 'CAGR'),
    ('AUTO_OPTIMIZATION_ENABLED', 'true'),
    ('AUTO_OPTIMIZATION_DELAY_SECONDS', '300'),
    ('HETZNER_SERVER_TYPE', 'cpx62'),
    ('HETZNER_SERVER_LOCATION', 'hel1'),
    ('HETZNER_SSH_KEY_NAME', 'hetzner-node'),
    ('LOCAL_OPTIMIZATION_STEP_MULTIPLIERS', '-4,-3,-2,-1,1,2,3,4'),
    ('MAX_ALLOWED_DRAWDOWN_RATIO', '0.30'),
    ('CANDLE_SYNC_MAX_CONCURRENT_UPDATES', '5'),
    ('CANDLE_SYNC_MATCHING_RATIO_THRESHOLD', '0.98'),
    ('AUTO_DAILY_CANDLE_SYNC_ENABLED', 'true'),
    ('COMMODITY_TRUST_EXPENSE_RATIO', '0.004'),
    ('BOND_ETF_EXPENSE_RATIO', '0.001'),
    ('ETF_BASE_EXPENSE_RATIO', '0.0008'),
    ('INCOME_ETF_EXPENSE_RATIO', '0.007'),
    ('INVERSE_ETF_EXPENSE_RATIO', '0.009'),
    ('LEVERAGED_2X_EXPENSE_RATIO', '0.009'),
    ('LEVERAGED_3X_EXPENSE_RATIO', '0.0095'),
    ('LEVERAGED_5X_EXPENSE_RATIO', '0.015'),
    ('TRADE_CLOSE_FEE_RATE', '0.0005'),
    ('TRADE_SLIPPAGE_RATE', '0.003'),
    ('SHORT_BORROW_FEE_ANNUAL_RATE', '0.003'),
    ('TRADE_ENTRY_PRICE_MIN', '0.10'),
    ('TRADE_ENTRY_PRICE_MAX', '1000'),
    ('MINIMUM_DOLLAR_VOLUME_FOR_ENTRY', '150000'),
    ('MINIMUM_DOLLAR_VOLUME_LOOKBACK', '5'),
    ('BACKTEST_ACTIVE_MONTHS', '1,3,6,12,24,36,48,60,120'),
    ('PARAM_SCORE_MIN_TRADES', '20'),
    ('PARAM_SCORE_DRAWDOWN_LAMBDA', '3.5'),
    ('PARAM_SCORE_NEIGHBOR_THRESHOLD', '0.15'),
    ('PARAM_SCORE_CORE_SCORE_QUANTILE', '0.6'),
    ('PARAM_SCORE_PAIRWISE_NEIGHBOR_LIMIT', '1500'),
    ('TEMPLATE_SCORE_RETURN_SCALE', '0.20'),
    ('TEMPLATE_SCORE_VALIDATION_NEGATIVE_PENALTY_STRENGTH', '2.0'),
    ('TEMPLATE_SCORE_DRAWDOWN_LAMBDA', '2.5'),
    ('TEMPLATE_SCORE_TRADE_TARGET', '200'),
    ('TEMPLATE_SCORE_TRADE_WEIGHT', '0.25'),
    ('TEMPLATE_SCORE_RECENCY_HALF_LIFE_DAYS', '365'),
    ('TEMPLATE_SCORE_VERIFY_SHARPE_SCALE', '2'),
    ('TEMPLATE_SCORE_VERIFY_CALMAR_SCALE', '2'),
    ('TEMPLATE_SCORE_VERIFY_CAGR_SCALE', '0.25'),
    ('TEMPLATE_SCORE_VERIFY_CAGR_NEG_SCALE', '0.10'),
    ('TEMPLATE_SCORE_VERIFY_DRAWDOWN_LAMBDA', '2.5'),
    ('TEMPLATE_SCORE_VERIFY_MIN_MULTIPLIER', '0.8'),
    ('TEMPLATE_SCORE_VERIFY_MAX_MULTIPLIER', '1.2'),
    ('INVITE_LINK_VALID_DAYS', '7'),
    ('MTLS_ACCESS_CERT_PASSWORD', 'stratcraft'),
    ('SESSION_COOKIE_VALID_DAYS', '30')
ON CONFLICT (setting_key) DO NOTHING;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_candles_ticker_date ON candles(ticker, date);
CREATE INDEX IF NOT EXISTS idx_candles_date ON candles(date);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at ON user_sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_trades_strategy_id ON trades(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker);
CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(date);
CREATE INDEX IF NOT EXISTS idx_strategies_template_id ON strategies(template_id);
CREATE INDEX IF NOT EXISTS idx_strategies_user_id ON strategies(user_id);
CREATE INDEX IF NOT EXISTS idx_trades_user_id ON trades(user_id);
CREATE INDEX IF NOT EXISTS idx_accounts_user_id ON accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_accounts_provider ON accounts(provider);
CREATE INDEX IF NOT EXISTS idx_account_operations_account_status ON account_operations(account_id, status);
CREATE INDEX IF NOT EXISTS idx_account_operations_strategy_status ON account_operations(strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_account_operations_status ON account_operations(status);
CREATE INDEX IF NOT EXISTS idx_account_operations_trade_id ON account_operations(trade_id);
CREATE INDEX IF NOT EXISTS idx_account_signal_skips_strategy_date ON account_signal_skips(strategy_id, signal_date);
CREATE INDEX IF NOT EXISTS idx_account_signal_skips_created_at ON account_signal_skips(created_at);
CREATE INDEX IF NOT EXISTS idx_backtest_results_strategy_id ON backtest_results(strategy_id);
CREATE INDEX IF NOT EXISTS idx_system_logs_source ON system_logs(source);
CREATE INDEX IF NOT EXISTS idx_system_logs_level ON system_logs(level);
CREATE INDEX IF NOT EXISTS idx_system_logs_created_at ON system_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_system_logs_source_level ON system_logs(source, level);
CREATE INDEX IF NOT EXISTS idx_backtest_cache_template_id ON backtest_cache(template_id);
CREATE INDEX IF NOT EXISTS idx_backtest_cache_sharpe_ratio ON backtest_cache(sharpe_ratio);
CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(date);
CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker);
CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy_id);
CREATE INDEX IF NOT EXISTS idx_signals_user_id ON signals(user_id);
CREATE INDEX IF NOT EXISTS idx_signals_user_date_ticker ON signals(user_id, date, ticker);
CREATE INDEX IF NOT EXISTS idx_remote_optimizer_jobs_template_id ON remote_optimizer_jobs(template_id);
CREATE INDEX IF NOT EXISTS idx_remote_optimizer_jobs_status ON remote_optimizer_jobs(status);
