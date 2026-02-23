use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use engine::commands::{
    backtest_accounts, backtest_active, balance, export_market_data, generate_signals, optimize,
    plan_operations, reconcile_trades, verify,
};
use engine::context::AppContext;
use engine::data_context::MarketData;
use engine::database::Database;
use engine::models::{GeneratedSignal, SignalAction, TradeStatus};
use engine::optimizer_status::OptimizerStatus;
use reqwest::Client as HttpClient;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value};
use std::collections::BTreeMap;
use std::f64::consts::PI;
use std::fmt::Write;
use std::fs;
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Once, OnceLock};
use std::thread;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_postgres::Client;

const DEFAULT_TEST_DB_NAME: &str = "stratcraft_test_pipeline";
const SMOKE_TEST_DB_NAME: &str = "stratcraft_test_pipeline_smoke";
const BACKTEST_ACCOUNTS_DB_NAME: &str = "stratcraft_test_backtest_accounts";
const EXPORT_MARKET_DATA_DB_NAME: &str = "stratcraft_test_export_market_data";
const OPTIMIZE_DB_NAME: &str = "stratcraft_test_optimize";
const PLAN_OPERATIONS_DB_NAME: &str = "stratcraft_test_plan_operations";
const ORDER_LIFECYCLE_DB_NAME: &str = "stratcraft_test_order_lifecycle";
const RECONCILE_TRADES_DB_NAME: &str = "stratcraft_test_reconcile_trades";
const VERIFY_DB_NAME: &str = "stratcraft_test_verify";
const TOTAL_DAYS: i64 = 730;
const SMOKE_TEST_DAYS: i64 = 45;
const APPROX_DAYS_PER_MONTH: f64 = 30.4;
const SUMMARY_SNAPSHOT: &str = "backtest_pipeline.txt";
const TRADES_DIR: &str = "trades";
const EXPORT_SNAPSHOT_FILE: &str = "market-data-smoke.bin";
const OPTIMIZE_SNAPSHOT_FILE: &str = "market-data-optimize.bin";
const VERIFY_SNAPSHOT_FILE: &str = "market-data-verify.bin";

fn ensure_test_env() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        std::env::set_var("DOMAIN", "");
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

static PIPELINE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn acquire_pipeline_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    PIPELINE_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .await
}

async fn wait_for_alpaca_stub(base_url: &str) -> Result<()> {
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to create Alpaca stub health check client")?;
    let url = format!("{}/account", base_url.trim_end_matches('/'));

    for _ in 0..40 {
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            _ => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }

    Err(anyhow!("Alpaca stub did not respond at {}", url))
}

#[derive(Clone, Copy)]
struct PipelineRunConfig {
    snapshot_dir: &'static str,
    allow_short_selling: Option<bool>,
}

impl Default for PipelineRunConfig {
    fn default() -> Self {
        Self {
            snapshot_dir: "snapshots",
            allow_short_selling: None,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_smoke_small_dataset() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create_with_name(SMOKE_TEST_DB_NAME).await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data_for_days(SMOKE_TEST_DAYS).await?;
    let strategy_seeds = test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: None,
        })
        .await?;

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    generate_signals::run(&app_context).await?;
    let approx_months = ((SMOKE_TEST_DAYS as f64) / APPROX_DAYS_PER_MONTH).ceil() as u32;
    let months = vec![approx_months.max(1)];
    backtest_active::run(&app_context, backtest_active::BacktestScope::All, &months).await?;

    let db = Database::new(test_db.database_url()).await?;
    for seed in strategy_seeds {
        let result = db
            .load_latest_backtest_result(&seed.id, None, "all")
            .await?
            .ok_or_else(|| anyhow!("missing backtest for {}", seed.id))?;
        assert!(
            !result.daily_snapshots.is_empty(),
            "expected snapshots for {}",
            seed.id
        );
        assert!(
            !result.tickers.is_empty(),
            "expected tickers for {}",
            seed.id
        );
        assert_eq!(
            result.performance.total_trades as usize,
            result.trades.len(),
            "trade count mismatch for {}",
            seed.id
        );
        assert_eq!(
            result.ticker_scope.as_deref(),
            Some("all"),
            "unexpected ticker scope for {}",
            seed.id
        );
        assert!(
            result.start_date <= result.end_date,
            "start date after end date for {}",
            seed.id
        );
    }

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_accounts_smoke() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create_with_name(BACKTEST_ACCOUNTS_DB_NAME).await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data_for_days(SMOKE_TEST_DAYS).await?;
    test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: None,
        })
        .await?;

    let templates = load_templates()?;
    let template = templates
        .first()
        .ok_or_else(|| anyhow!("No templates available for account strategy"))?;
    let account_strategy = test_db.seed_account_strategy(template).await?;

    let trade_date = baseline_start_date() + ChronoDuration::days(5);
    test_db
        .seed_account_trade(&account_strategy.id, "AAA", trade_date)
        .await?;

    let signal_date = baseline_start_date()
        .and_hms_opt(0, 0, 0)
        .expect("valid date")
        .and_utc()
        + ChronoDuration::days(10);
    let signals = vec![GeneratedSignal {
        date: signal_date,
        ticker: "AAA".to_string(),
        action: SignalAction::Buy,
        confidence: Some(0.9),
    }];

    let mut db = Database::new(test_db.database_url()).await?;
    db.upsert_strategy_signals(&account_strategy.id, &signals)
        .await?;

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    backtest_accounts::run(&app_context).await?;

    let result = db
        .load_latest_backtest_result(&account_strategy.id, None, "all")
        .await?
        .ok_or_else(|| anyhow!("missing backtest for {}", account_strategy.id))?;
    assert!(
        !result.daily_snapshots.is_empty(),
        "expected snapshots for {}",
        account_strategy.id
    );
    assert!(
        result.tickers.iter().any(|ticker| ticker == "AAA"),
        "expected AAA in tickers for {}",
        account_strategy.id
    );
    assert_eq!(
        result.ticker_scope.as_deref(),
        Some("all"),
        "unexpected ticker scope for {}",
        account_strategy.id
    );

    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_market_data_smoke() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create_with_name(EXPORT_MARKET_DATA_DB_NAME).await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data_for_days(SMOKE_TEST_DAYS).await?;
    test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: None,
        })
        .await?;

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    let output_path = export_snapshot_file_path(EXPORT_SNAPSHOT_FILE);
    if output_path.exists() {
        fs::remove_file(&output_path)?;
    }
    export_market_data::run(&app_context, &output_path).await?;
    assert!(
        output_path.exists(),
        "expected market data snapshot at {}",
        output_path.display()
    );

    let status = OptimizerStatus::new();
    let market_data = MarketData::load_from_file(&output_path, &status)?;
    assert!(
        !market_data.tickers().is_empty(),
        "expected tickers in snapshot"
    );
    assert!(
        !market_data.unique_dates().is_empty(),
        "expected dates in snapshot"
    );
    let templates = load_templates()?;
    let template = templates
        .first()
        .ok_or_else(|| anyhow!("No templates available for snapshot check"))?;
    assert!(
        market_data.template(&template.id).is_some(),
        "expected template {} in snapshot",
        template.id
    );

    fs::remove_file(&output_path)?;
    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optimize_smoke() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create_with_name(OPTIMIZE_DB_NAME).await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data_for_days(SMOKE_TEST_DAYS).await?;
    test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: None,
        })
        .await?;
    test_db
        .update_setting("LOCAL_OPTIMIZATION_STEP_MULTIPLIERS", "0")
        .await?;
    test_db
        .update_setting("LOCAL_OPTIMIZATION_VERSION", "1")
        .await?;

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    let output_path = export_snapshot_file_path(OPTIMIZE_SNAPSHOT_FILE);
    if output_path.exists() {
        fs::remove_file(&output_path)?;
    }
    export_market_data::run(&app_context, &output_path).await?;

    let template = load_templates()?
        .into_iter()
        .find(|candidate| {
            candidate.parameters.iter().any(|param| {
                param
                    .get("type")
                    .and_then(|value| value.as_str())
                    .map(|kind| kind == "number")
                    .unwrap_or(false)
                    && param.get("min").is_some()
                    && param.get("max").is_some()
                    && param.get("step").is_some()
            })
        })
        .ok_or_else(|| anyhow!("No optimizable template found"))?;

    optimize::run(&app_context, &template.id, &output_path).await?;

    let db = Database::new(test_db.database_url()).await?;
    let updated_template = db
        .get_template(&template.id)
        .await?
        .ok_or_else(|| anyhow!("Missing template {} after optimize", template.id))?;
    assert_eq!(
        updated_template.local_optimization_version, 1,
        "expected local optimization version update for {}",
        template.id
    );

    let deleted_strategy = db
        .get_strategy_config(&format!("default_{}", template.id))
        .await?;
    assert!(
        deleted_strategy.is_none(),
        "expected default strategy cleanup for {}",
        template.id
    );

    fs::remove_file(&output_path)?;
    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_operations_smoke() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create_with_name(PLAN_OPERATIONS_DB_NAME).await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data_for_days(SMOKE_TEST_DAYS).await?;
    test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: None,
        })
        .await?;

    let templates = load_templates()?;
    let template = templates
        .first()
        .ok_or_else(|| anyhow!("No templates available for plan operations"))?;
    let account_strategy = test_db.seed_account_strategy(template).await?;

    let signal_date = baseline_start_date()
        .and_hms_opt(0, 0, 0)
        .expect("valid date")
        .and_utc()
        + ChronoDuration::days(10);
    let signals = vec![GeneratedSignal {
        date: signal_date,
        ticker: "AAA".to_string(),
        action: SignalAction::Buy,
        confidence: Some(0.9),
    }];

    let mut db = Database::new(test_db.database_url()).await?;
    db.upsert_strategy_signals(&account_strategy.id, &signals)
        .await?;

    let stub = AlpacaStub::start(AlpacaStubResponses::default())?;
    wait_for_alpaca_stub(&stub.base_url).await?;
    test_db
        .update_setting("ALPACA_PAPER_URL", &stub.base_url)
        .await?;

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    plan_operations::run(&app_context).await?;

    let operation_count = test_db
        .count_account_operations(&account_strategy.id)
        .await?;
    assert!(
        operation_count > 0,
        "expected account operations for {}",
        account_strategy.id
    );

    drop(stub);
    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_lifecycle_end_to_end() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create_with_name(ORDER_LIFECYCLE_DB_NAME).await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data_for_days(SMOKE_TEST_DAYS).await?;
    test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: None,
        })
        .await?;

    let templates = load_templates()?;
    let template = templates
        .first()
        .ok_or_else(|| anyhow!("No templates available for order lifecycle"))?;
    let account_strategy = test_db.seed_account_strategy(template).await?;

    let signal_date = baseline_start_date()
        .and_hms_opt(0, 0, 0)
        .expect("valid date")
        .and_utc()
        + ChronoDuration::days(10);
    let signals = vec![GeneratedSignal {
        date: signal_date,
        ticker: "AAA".to_string(),
        action: SignalAction::Buy,
        confidence: Some(0.9),
    }];

    let mut db = Database::new(test_db.database_url()).await?;
    db.upsert_strategy_signals(&account_strategy.id, &signals)
        .await?;

    let filled_at = baseline_start_date()
        .and_hms_opt(0, 0, 0)
        .expect("valid date")
        .and_utc()
        + ChronoDuration::days(12);
    let stub = AlpacaStub::start(AlpacaStubResponses::filled_order(
        "order-entry",
        "AAA",
        101.0,
        filled_at,
    ))?;
    wait_for_alpaca_stub(&stub.base_url).await?;
    test_db
        .update_setting("ALPACA_PAPER_URL", &stub.base_url)
        .await?;

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    plan_operations::run(&app_context).await?;

    let operation = test_db
        .fetch_pending_open_operation(&account_strategy.id)
        .await?;

    let live_trades = db.get_strategy_live_trades(&account_strategy.id).await?;
    assert!(
        live_trades.is_empty(),
        "expected no live trades before dispatch for {}",
        account_strategy.id
    );

    test_db
        .dispatch_open_operation(&operation, "order-entry")
        .await?;

    reconcile_trades::run(&app_context).await?;

    let trades = db.get_strategy_live_trades(&account_strategy.id).await?;
    let trade = trades
        .iter()
        .find(|candidate| candidate.id == operation.trade_id)
        .ok_or_else(|| anyhow!("Missing trade {} after reconciliation", operation.trade_id))?;
    assert_eq!(trade.status, TradeStatus::Active);
    assert!(
        (trade.price - 101.0).abs() < 1e-6,
        "expected filled price for {}",
        trade.id
    );

    drop(stub);
    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconcile_trades_smoke() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create_with_name(RECONCILE_TRADES_DB_NAME).await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data_for_days(SMOKE_TEST_DAYS).await?;
    test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: None,
        })
        .await?;

    let templates = load_templates()?;
    let template = templates
        .first()
        .ok_or_else(|| anyhow!("No templates available for reconcile trades"))?;
    let account_strategy = test_db.seed_account_strategy(template).await?;

    let trade_date = baseline_start_date() + ChronoDuration::days(5);
    let trade_id = test_db
        .seed_pending_account_trade(&account_strategy.id, "AAA", trade_date)
        .await?;

    let filled_at = baseline_start_date()
        .and_hms_opt(0, 0, 0)
        .expect("valid date")
        .and_utc()
        + ChronoDuration::days(7);
    let stub = AlpacaStub::start(AlpacaStubResponses::filled_order(
        "order-entry",
        "AAA",
        101.0,
        filled_at,
    ))?;
    wait_for_alpaca_stub(&stub.base_url).await?;
    test_db
        .update_setting("ALPACA_PAPER_URL", &stub.base_url)
        .await?;

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    reconcile_trades::run(&app_context).await?;

    let db = Database::new(test_db.database_url()).await?;
    let trades = db.get_strategy_live_trades(&account_strategy.id).await?;
    let trade = trades
        .iter()
        .find(|candidate| candidate.id == trade_id)
        .ok_or_else(|| anyhow!("Missing trade {} after reconciliation", trade_id))?;
    assert_eq!(trade.status, TradeStatus::Active);
    assert!(
        (trade.price - 101.0).abs() < 1e-6,
        "expected filled price for {}",
        trade_id
    );

    drop(stub);
    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_balance_smoke() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create_with_name(VERIFY_DB_NAME).await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data_for_days(SMOKE_TEST_DAYS).await?;
    test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: None,
        })
        .await?;

    let start_date = baseline_start_date();
    let end_date = baseline_start_date() + ChronoDuration::days(SMOKE_TEST_DAYS - 1);
    test_db
        .update_setting(
            "VERIFY_WINDOW_START_DATE",
            &start_date.format("%Y-%m-%d").to_string(),
        )
        .await?;
    test_db
        .update_setting(
            "VERIFY_WINDOW_END_DATE",
            &end_date.format("%Y-%m-%d").to_string(),
        )
        .await?;
    test_db
        .update_setting(
            "BALANCE_WINDOW_START_DATE",
            &start_date.format("%Y-%m-%d").to_string(),
        )
        .await?;
    test_db
        .update_setting(
            "BALANCE_WINDOW_END_DATE",
            &end_date.format("%Y-%m-%d").to_string(),
        )
        .await?;

    let template = load_templates()?
        .into_iter()
        .find(|candidate| {
            candidate.parameters.iter().any(|param| {
                param
                    .get("type")
                    .and_then(|value| value.as_str())
                    .map(|kind| kind == "number")
                    .unwrap_or(false)
                    && param.get("min").is_some()
                    && param.get("max").is_some()
                    && param.get("step").is_some()
            })
        })
        .ok_or_else(|| anyhow!("No optimizable template found for verify"))?;

    let cache_id = "cache_verify_smoke";
    test_db
        .seed_backtest_cache(&template, cache_id, start_date, end_date)
        .await?;

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    let output_path = export_snapshot_file_path(VERIFY_SNAPSHOT_FILE);
    if output_path.exists() {
        fs::remove_file(&output_path)?;
    }
    export_market_data::run(&app_context, &output_path).await?;

    verify::run(&app_context, &template.id, &output_path).await?;
    balance::run(&app_context, &template.id, &output_path).await?;

    let verification = test_db.get_backtest_cache_verify_values(cache_id).await?;
    assert!(
        verification.verify_sharpe_ratio.is_some(),
        "expected verify_sharpe_ratio for {}",
        cache_id
    );
    assert!(
        verification.verify_calmar_ratio.is_some(),
        "expected verify_calmar_ratio for {}",
        cache_id
    );
    assert!(
        verification.verify_cagr.is_some(),
        "expected verify_cagr for {}",
        cache_id
    );
    assert!(
        verification.verify_max_drawdown_ratio.is_some(),
        "expected verify_max_drawdown_ratio for {}",
        cache_id
    );

    let balance_values = test_db.get_backtest_cache_balance_values(cache_id).await?;
    assert!(
        balance_values.balance_training_cagr.is_some(),
        "expected balance_training_cagr for {}",
        cache_id
    );
    assert!(
        balance_values.balance_validation_cagr.is_some(),
        "expected balance_validation_cagr for {}",
        cache_id
    );

    fs::remove_file(&output_path)?;
    test_db.cleanup().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_pipeline_snapshot() -> Result<()> {
    ensure_test_env();
    let _guard = acquire_pipeline_test_lock().await;
    run_pipeline_snapshot(PipelineRunConfig::default()).await?;
    run_pipeline_snapshot(PipelineRunConfig {
        snapshot_dir: "snapshots_allow_short",
        allow_short_selling: Some(true),
    })
    .await
}

async fn run_pipeline_snapshot(config: PipelineRunConfig) -> Result<()> {
    std::env::set_var("RAYON_NUM_THREADS", "2");
    let test_db = TestDatabase::create().await?;
    test_db.apply_schema().await?;
    test_db.seed_market_data().await?;
    let strategy_seeds = test_db
        .seed_strategies(StrategySeedConfig {
            allow_short_selling_override: config.allow_short_selling,
        })
        .await?;

    if std::env::var("STRATCRAFT_DEBUG_PARAMS").is_ok() {
        let client = connect(test_db.database_url()).await?;
        let rows = client
            .query(
                "SELECT id, template_id, parameters FROM strategies ORDER BY id",
                &[],
            )
            .await?;
        for row in rows {
            let id: String = row.get(0);
            let template: String = row.get(1);
            let params: String = row.get(2);
            println!("{} ({}) => {}", id, template, params);
        }
    }

    let app_context = AppContext::initialize(Some(test_db.database_url().to_string())).await?;
    generate_signals::run(&app_context).await?;
    let backtest_scope = backtest_active::BacktestScope::All;
    let months = vec![((TOTAL_DAYS as f64) / APPROX_DAYS_PER_MONTH).ceil() as u32];
    backtest_active::run(&app_context, backtest_scope, &months).await?;

    let snapshots =
        capture_snapshot(test_db.database_url(), &strategy_seeds, backtest_scope).await?;
    persist_snapshot_bundle(snapshots, config.snapshot_dir)?;

    test_db.cleanup().await?;
    Ok(())
}

struct TestDatabase {
    admin_url: String,
    database_url: String,
    db_name: String,
    cleaned: bool,
}

#[derive(Clone)]
struct StrategySeed {
    id: String,
    template_id: String,
}

#[derive(Clone, Copy)]
struct StrategySeedConfig {
    allow_short_selling_override: Option<bool>,
}

struct PendingAccountOperation {
    id: String,
    trade_id: String,
    ticker: String,
    quantity: i32,
    price: f64,
    stop_loss: Option<f64>,
    triggered_at: DateTime<Utc>,
    strategy_id: String,
}

#[derive(Clone, Deserialize)]
struct TemplateFile {
    id: String,
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    author: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(rename = "exampleUsage", default)]
    example_usage: Option<String>,
    parameters: Vec<Value>,
}

impl TemplateFile {
    fn default_parameters_map(&self) -> JsonMap<String, Value> {
        let mut map = JsonMap::new();
        for param in &self.parameters {
            let Some(name) = param.get("name").and_then(|value| value.as_str()) else {
                continue;
            };
            if let Some(default_value) = param.get("default") {
                map.insert(
                    name.to_string(),
                    Self::normalize_default(name, default_value.clone()),
                );
            }
        }
        map
    }

    fn normalize_default(name: &str, value: Value) -> Value {
        let lower = name.to_ascii_lowercase();
        if lower.contains("period") || lower.contains("lookback") {
            if let Some(num) = value.as_f64() {
                if num < 1.0 {
                    return Value::from(1.0);
                }
            }
        }
        value
    }
}

#[derive(Default)]
struct SnapshotBundle {
    summary: String,
    trade_files: BTreeMap<String, String>,
}

struct BacktestCacheVerifyValues {
    verify_sharpe_ratio: Option<f64>,
    verify_calmar_ratio: Option<f64>,
    verify_cagr: Option<f64>,
    verify_max_drawdown_ratio: Option<f64>,
}

struct BacktestCacheBalanceValues {
    balance_training_cagr: Option<f64>,
    balance_validation_cagr: Option<f64>,
}

impl TestDatabase {
    async fn create() -> Result<Self> {
        let db_name = std::env::var("STRATCRAFT_TEST_DATABASE_NAME")
            .unwrap_or_else(|_| DEFAULT_TEST_DB_NAME.to_string());
        Self::create_with_name(&db_name).await
    }

    async fn create_with_name(db_name: &str) -> Result<Self> {
        let root = test_db_root_url()?;
        let trimmed = root.trim_end_matches('/');
        let admin_url = format!("{}/postgres", trimmed);
        let database_url = format!("{}/{}", trimmed, db_name);

        let admin_client = connect(&admin_url).await?;
        drop_database_with_client(&admin_client, db_name).await?;
        admin_client
            .batch_execute(&format!("CREATE DATABASE {} TEMPLATE template0", db_name))
            .await?;

        Ok(Self {
            admin_url,
            database_url,
            db_name: db_name.to_string(),
            cleaned: false,
        })
    }

    async fn cleanup(mut self) -> Result<()> {
        self.drop_database().await?;
        self.cleaned = true;
        Ok(())
    }

    fn database_url(&self) -> &str {
        &self.database_url
    }

    async fn apply_schema(&self) -> Result<()> {
        let schema = std::fs::read_to_string(schema_file_path())?;
        let client = connect(self.database_url()).await?;
        client.batch_execute(&schema).await?;
        Ok(())
    }

    async fn seed_market_data(&self) -> Result<()> {
        self.seed_market_data_for_days(TOTAL_DAYS).await
    }

    async fn seed_market_data_for_days(&self, total_days: i64) -> Result<()> {
        let mut client = connect(self.database_url()).await?;
        let tx = client.transaction().await?;
        let price_scale = 0.025;
        for (idx, ticker) in TickerSeed::universe().iter().enumerate() {
            let is_training = idx % 4 != 0;
            tx.execute(
                "INSERT INTO tickers (symbol, market_cap, volume_usd, max_fluctuation_ratio, training)
                 VALUES ($1, $2, $3, $4, $5)",
                &[
                    &ticker.symbol,
                    &((idx as f64 + 1.0) * 400_000_000.0),
                    &((idx as f64 + 1.0) * 20_000_000.0),
                    &0.5_f64,
                    &is_training,
                ],
            )
            .await?;
        }

        let stmt = tx
            .prepare(
                "INSERT INTO candles (ticker, date, open, high, low, close, unadjusted_close, volume_shares)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .await?;

        let baseline = baseline_start_date();
        for day in 0..total_days {
            let current = baseline + ChronoDuration::days(day);
            let day_f = day as f64;
            for (idx, ticker) in TickerSeed::universe().iter().enumerate() {
                let idx_f = idx as f64 + 1.0;
                let fast_wave = (day_f / (6.0 + idx_f)).sin();
                let slow_wave = (day_f / (35.0 + idx_f * 0.6) + idx_f * 0.15).cos();
                let seasonal_wave = ((day_f / 90.0) * PI + idx_f * 0.25).sin();
                let momentum_wave = (day_f / (3.0 + idx_f * 0.2)).sin();
                let regime = match (day / 120) % 3 {
                    0 => 1.0,
                    1 => -0.65,
                    _ => 0.35,
                };
                let trend_strength = match (day / 120) % 3 {
                    0 => 1.9,
                    1 => 1.4,
                    _ => 1.05,
                };
                let direction = if (day / 180) % 2 == 0 { 1.25 } else { -0.9 };
                let drift = ticker.drift * (1.0 + 0.35 * seasonal_wave) * trend_strength;
                let base_trend = ticker.base_price + day_f * drift * direction;
                let swing =
                    9.5 * seasonal_wave + 4.6 * slow_wave + 2.4 * fast_wave + 3.4 * momentum_wave;
                let mut close = (base_trend + swing + regime * 6.0 + idx_f) * price_scale;
                if close < 1.0 {
                    close = 1.0;
                }

                let volatility_regime = match (day / 60) % 4 {
                    0 => 1.0,
                    1 => 1.45,
                    2 => 1.8,
                    _ => 1.25,
                };
                let intraday_range = (1.2
                    + (fast_wave.abs() * 2.6)
                    + (slow_wave.abs() * 1.9)
                    + (momentum_wave.abs() * 1.6)
                    + idx_f * 0.1)
                    * volatility_regime
                    * price_scale;
                let open = (close - fast_wave * intraday_range * 0.45).max(1.0);
                let high = close + intraday_range * (1.05 + 0.05 * idx_f);
                let low = (close - intraday_range * (0.95 + 0.05 * idx_f)).max(1.0);
                let volatility = fast_wave.abs() + slow_wave.abs() + seasonal_wave.abs();
                let volume =
                    (750_000.0 + idx_f * 140_000.0 + day_f * 150.0 + 260_000.0 * volatility) as i64;

                tx.execute(
                    &stmt,
                    &[
                        &ticker.symbol,
                        &current,
                        &open,
                        &high,
                        &low,
                        &close,
                        &close,
                        &volume,
                    ],
                )
                .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    async fn seed_strategies(&self, config: StrategySeedConfig) -> Result<Vec<StrategySeed>> {
        let templates = load_templates()?;
        let client = connect(self.database_url()).await?;
        let start_date = baseline_start_date()
            .and_hms_opt(0, 0, 0)
            .expect("valid date")
            .and_utc();

        for template in &templates {
            let parameters_json = serde_json::to_string(&template.parameters)?;
            client
                .execute(
                    "INSERT INTO templates (id, name, description, category, author, version, parameters, example_usage)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    &[
                        &template.id,
                        &template.name,
                        &template.description,
                        &template.category,
                        &template.author,
                        &template.version,
                        &parameters_json,
                        &template.example_usage,
                    ],
                )
                .await?;
        }

        let mut seeds = Vec::new();
        for template in &templates {
            let mut defaults = template.default_parameters_map();
            if let Some(allow_short) = config.allow_short_selling_override {
                defaults.insert(
                    "allowShortSelling".to_string(),
                    Value::from(if allow_short { 1 } else { 0 }),
                );
            }
            let parameters_value = Value::Object(defaults.clone());
            let parameters_text = serde_json::to_string(&parameters_value)?;
            let strategy_id = format!("default_{}", template.id);
            client
                .execute(
                    "INSERT INTO strategies (id, name, template_id, parameters, backtest_start_date, status)
                     VALUES ($1, $2, $3, $4, $5, 'active')",
                    &[
                        &strategy_id,
                        &format!("Default {}", template.name),
                        &template.id,
                        &parameters_text,
                        &start_date,
                    ],
                )
                .await?;

            seeds.push(StrategySeed {
                id: strategy_id,
                template_id: template.id.clone(),
            });
        }

        Ok(seeds)
    }

    async fn seed_account_strategy(&self, template: &TemplateFile) -> Result<StrategySeed> {
        let client = connect(self.database_url()).await?;
        let email = format!("account_{}@example.com", template.id);
        let user_row = client
            .query_one(
                "INSERT INTO users (email) VALUES ($1) RETURNING id",
                &[&email],
            )
            .await?;
        let user_id: i64 = user_row.get(0);
        let account_id = format!("acct_{}", template.id);
        client
            .execute(
                "INSERT INTO accounts (id, user_id, name, provider, api_key, api_secret)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &account_id,
                    &user_id,
                    &format!("Account {}", template.name),
                    &"alpaca",
                    &"test_key",
                    &"test_secret",
                ],
            )
            .await?;

        let parameters_value = Value::Object(template.default_parameters_map());
        let parameters_text = serde_json::to_string(&parameters_value)?;
        let strategy_id = format!("account_{}", template.id);
        client
            .execute(
                "INSERT INTO strategies (id, name, user_id, account_id, template_id, parameters, status)
                 VALUES ($1, $2, $3, $4, $5, $6, 'active')",
                &[
                    &strategy_id,
                    &format!("Account {}", template.name),
                    &user_id,
                    &account_id,
                    &template.id,
                    &parameters_text,
                ],
            )
            .await?;

        Ok(StrategySeed {
            id: strategy_id,
            template_id: template.id.clone(),
        })
    }

    async fn seed_account_trade(
        &self,
        strategy_id: &str,
        ticker: &str,
        trade_date: NaiveDate,
    ) -> Result<()> {
        let client = connect(self.database_url()).await?;
        let trade_id = format!("live_{}_{}", strategy_id, ticker);
        client
            .execute(
                "INSERT INTO trades (id, strategy_id, ticker, quantity, price, date, status, entry_order_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &trade_id,
                    &strategy_id,
                    &ticker,
                    &10_i32,
                    &100.0_f64,
                    &trade_date,
                    &"active",
                    &"order-1",
                ],
            )
            .await?;
        Ok(())
    }

    async fn seed_pending_account_trade(
        &self,
        strategy_id: &str,
        ticker: &str,
        trade_date: NaiveDate,
    ) -> Result<String> {
        let client = connect(self.database_url()).await?;
        let trade_id = format!("pending_{}_{}", strategy_id, ticker);
        client
            .execute(
                "INSERT INTO trades (id, strategy_id, ticker, quantity, price, date, status, entry_order_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &trade_id,
                    &strategy_id,
                    &ticker,
                    &10_i32,
                    &100.0_f64,
                    &trade_date,
                    &"pending",
                    &"order-entry",
                ],
            )
            .await?;
        Ok(trade_id)
    }

    async fn update_setting(&self, key: &str, value: &str) -> Result<()> {
        let client = connect(self.database_url()).await?;
        client
            .execute(
                "INSERT INTO settings (setting_key, value, updated_at)
                 VALUES ($1, $2, CURRENT_TIMESTAMP)
                 ON CONFLICT (setting_key)
                 DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP",
                &[&key, &value],
            )
            .await?;
        Ok(())
    }

    async fn count_account_operations(&self, strategy_id: &str) -> Result<i64> {
        let client = connect(self.database_url()).await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM account_operations WHERE strategy_id = $1",
                &[&strategy_id],
            )
            .await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn fetch_pending_open_operation(
        &self,
        strategy_id: &str,
    ) -> Result<PendingAccountOperation> {
        let client = connect(self.database_url()).await?;
        let row = client
            .query_opt(
                "SELECT id, trade_id, ticker, quantity, price, stop_loss, triggered_at, strategy_id
                 FROM account_operations
                 WHERE strategy_id = $1
                   AND operation_type = 'open_position'
                   AND status = 'pending'
                 ORDER BY triggered_at
                 LIMIT 1",
                &[&strategy_id],
            )
            .await?
            .ok_or_else(|| anyhow!("missing open_position operation for {}", strategy_id))?;

        let id: String = row.get(0);
        let trade_id: String = row.get(1);
        let ticker: String = row.get(2);
        let quantity: Option<i32> = row.get(3);
        let price: Option<f64> = row.get(4);
        let stop_loss: Option<f64> = row.get(5);
        let triggered_at: DateTime<Utc> = row.get(6);
        let strategy_id: String = row.get(7);

        let quantity = quantity.ok_or_else(|| anyhow!("missing quantity for operation {}", id))?;
        let price = price.ok_or_else(|| anyhow!("missing price for operation {}", id))?;

        Ok(PendingAccountOperation {
            id,
            trade_id,
            ticker,
            quantity,
            price,
            stop_loss,
            triggered_at,
            strategy_id,
        })
    }

    async fn dispatch_open_operation(
        &self,
        operation: &PendingAccountOperation,
        order_id: &str,
    ) -> Result<()> {
        let client = connect(self.database_url()).await?;
        let trade_date = operation.triggered_at.date_naive();
        let user_id: Option<i64> = None;

        client
            .execute(
                "INSERT INTO trades (id, strategy_id, user_id, ticker, quantity, price, date, status, stop_loss, entry_order_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)
                 ON CONFLICT (id) DO NOTHING",
                &[
                    &operation.trade_id,
                    &operation.strategy_id,
                    &user_id,
                    &operation.ticker,
                    &operation.quantity,
                    &operation.price,
                    &trade_date,
                    &operation.stop_loss,
                    &order_id,
                ],
            )
            .await?;

        client
            .execute(
                "UPDATE account_operations
                 SET status = 'sent',
                     order_id = $1,
                     status_updated_at = CURRENT_TIMESTAMP,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE id = $2",
                &[&order_id, &operation.id],
            )
            .await?;

        Ok(())
    }

    async fn seed_backtest_cache(
        &self,
        template: &TemplateFile,
        cache_id: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<()> {
        let client = connect(self.database_url()).await?;
        let start_ts = start_date
            .and_hms_opt(0, 0, 0)
            .expect("valid date")
            .and_utc();
        let end_ts = end_date.and_hms_opt(0, 0, 0).expect("valid date").and_utc();
        let parameters = Value::Object(template.default_parameters_map());
        let params_text = serde_json::to_string(&parameters)?;
        client
            .execute(
                "INSERT INTO backtest_cache
                 (id, template_id, parameters, sharpe_ratio, calmar_ratio, total_return, cagr, max_drawdown, max_drawdown_ratio, win_rate, total_trades, ticker_count, start_date, end_date)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                &[
                    &cache_id,
                    &template.id,
                    &params_text,
                    &0.5_f64,
                    &0.4_f64,
                    &1000.0_f64,
                    &0.1_f64,
                    &500.0_f64,
                    &0.2_f64,
                    &0.55_f64,
                    &10_i32,
                    &1_i32,
                    &start_ts,
                    &end_ts,
                ],
            )
            .await?;
        Ok(())
    }

    async fn get_backtest_cache_verify_values(
        &self,
        cache_id: &str,
    ) -> Result<BacktestCacheVerifyValues> {
        let client = connect(self.database_url()).await?;
        let row = client
            .query_one(
                "SELECT verify_sharpe_ratio, verify_calmar_ratio, verify_cagr, verify_max_drawdown_ratio
                 FROM backtest_cache WHERE id = $1",
                &[&cache_id],
            )
            .await?;
        Ok(BacktestCacheVerifyValues {
            verify_sharpe_ratio: row.get(0),
            verify_calmar_ratio: row.get(1),
            verify_cagr: row.get(2),
            verify_max_drawdown_ratio: row.get(3),
        })
    }

    async fn get_backtest_cache_balance_values(
        &self,
        cache_id: &str,
    ) -> Result<BacktestCacheBalanceValues> {
        let client = connect(self.database_url()).await?;
        let row = client
            .query_one(
                "SELECT balance_training_cagr, balance_validation_cagr
                 FROM backtest_cache WHERE id = $1",
                &[&cache_id],
            )
            .await?;
        Ok(BacktestCacheBalanceValues {
            balance_training_cagr: row.get(0),
            balance_validation_cagr: row.get(1),
        })
    }

    async fn drop_database(&self) -> Result<()> {
        let client = connect(&self.admin_url).await?;
        drop_database_with_client(&client, &self.db_name).await
    }
}

fn test_db_root_url() -> Result<String> {
    dotenvy::dotenv().ok();
    let url =
        std::env::var("DATABASE_URL").map_err(|_| anyhow!("DATABASE_URL must be set in .env"))?;
    let url = url
        .split('?')
        .next()
        .unwrap_or(url.as_str())
        .trim_end_matches('/');
    let root = url.rsplit_once('/').map(|(root, _)| root).unwrap_or(url);
    Ok(root.to_string())
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let admin_url = self.admin_url.clone();
            let db_name = self.db_name.clone();
            handle.spawn(async move {
                if let Ok(client) = connect(&admin_url).await {
                    let _ = drop_database_with_client(&client, &db_name).await;
                }
            });
        }
    }
}

async fn capture_snapshot(
    database_url: &str,
    seeds: &[StrategySeed],
    scope: backtest_active::BacktestScope,
) -> Result<SnapshotBundle> {
    let db = Database::new(database_url).await?;
    let scope_label = match scope {
        backtest_active::BacktestScope::Validation => "validation",
        backtest_active::BacktestScope::All => "all",
        backtest_active::BacktestScope::Training => "training",
    };
    let mut sorted = seeds.to_vec();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));
    let mut bundle = SnapshotBundle::default();

    for seed in sorted {
        let result = db
            .load_latest_backtest_result(&seed.id, None, scope_label)
            .await?
            .ok_or_else(|| anyhow!("missing backtest for {}", seed.id))?;

        writeln!(
            bundle.summary,
            "{}|{}|start={}|end={}|final={:.2}|total_return={:.6}|sharpe={:.6}|trades={}|tickers={}",
            seed.id,
            seed.template_id,
            result.start_date.format("%Y-%m-%d"),
            result.end_date.format("%Y-%m-%d"),
            result.final_portfolio_value,
            result.performance.total_return,
            result.performance.sharpe_ratio,
            result.performance.total_trades,
            result.tickers.len()
        )?;

        let mut trades = result.trades.clone();
        trades.sort_by(|a, b| {
            a.date
                .cmp(&b.date)
                .then_with(|| a.ticker.cmp(&b.ticker))
                .then_with(|| a.id.cmp(&b.id))
        });

        let trade_vec = bundle
            .trade_files
            .entry(seed.template_id.clone())
            .or_insert_with(String::new);

        if trades.is_empty() {
            writeln!(
                trade_vec,
                "{}|NO_TRADES|final={:.2}|total_return={:.6}|sharpe={:.6}",
                seed.id,
                result.final_portfolio_value,
                result.performance.total_return,
                result.performance.sharpe_ratio
            )?;
            continue;
        }

        for trade in trades {
            let entry_date = trade.date.format("%Y-%m-%d");
            let exit_date = trade
                .exit_date
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "-".to_string());
            let side = if trade.quantity >= 0 { "long" } else { "short" };
            writeln!(
                trade_vec,
                "{}|{}|{}|{}|qty={}|entry={:.2}@{}|exit={:.2}@{}|pnl={:.2}|fee={:.4}|status={}",
                seed.id,
                entry_date,
                trade.ticker,
                side,
                trade.quantity,
                trade.price,
                entry_date,
                trade.exit_price.unwrap_or(trade.price),
                exit_date,
                trade.pnl.unwrap_or(0.0),
                trade.fee.unwrap_or(0.0),
                trade.status.as_str()
            )?;
        }
    }

    Ok(bundle)
}

fn persist_snapshot_bundle(bundle: SnapshotBundle, snapshot_dir: &str) -> Result<()> {
    let summary_path = snapshot_file_path(snapshot_dir, SUMMARY_SNAPSHOT);
    println!("writing snapshot {}", summary_path.display());
    write_snapshot_file(&summary_path, &bundle.summary)?;

    for (template, contents) in bundle.trade_files {
        let filename = format!("{}/{}.txt", TRADES_DIR, template);
        let path = snapshot_file_path(snapshot_dir, &filename);
        println!("writing snapshot {}", path.display());
        write_snapshot_file(&path, &contents)?;
    }

    Ok(())
}

fn write_snapshot_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, contents)?;
    Ok(())
}

fn load_templates() -> Result<Vec<TemplateFile>> {
    let mut entries: Vec<_> = std::fs::read_dir(templates_dir_path())?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("json"))
                .unwrap_or(false)
            {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    entries.sort();

    let mut templates = Vec::with_capacity(entries.len());
    for path in entries {
        let raw = std::fs::read_to_string(&path)?;
        let template: TemplateFile = serde_json::from_str(&raw)?;
        templates.push(template);
    }
    templates.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(templates)
}

fn snapshot_file_path(snapshot_dir: &str, file_name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(snapshot_dir)
        .join(file_name)
}

fn export_snapshot_file_path(file_name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("tmp")
        .join(file_name)
}

#[derive(Clone)]
struct AlpacaStubResponses {
    account_json: String,
    positions_json: String,
    orders_json: String,
    order_json: String,
}

impl AlpacaStubResponses {
    fn default() -> Self {
        Self {
            account_json: serde_json::json!({ "cash": "100000" }).to_string(),
            positions_json: "[]".to_string(),
            orders_json: "[]".to_string(),
            order_json: build_order_json("order-1", "AAA", "filled", Some(100.0), Some(Utc::now())),
        }
    }

    fn filled_order(
        order_id: &str,
        symbol: &str,
        filled_price: f64,
        filled_at: DateTime<Utc>,
    ) -> Self {
        Self {
            account_json: serde_json::json!({ "cash": "100000" }).to_string(),
            positions_json: "[]".to_string(),
            orders_json: "[]".to_string(),
            order_json: build_order_json(
                order_id,
                symbol,
                "filled",
                Some(filled_price),
                Some(filled_at),
            ),
        }
    }
}

struct AlpacaStub {
    base_url: String,
    shutdown: mpsc::Sender<()>,
    handle: Option<thread::JoinHandle<()>>,
}

impl AlpacaStub {
    fn start(responses: AlpacaStubResponses) -> Result<Self> {
        let mut listener: Option<TcpListener> = None;
        for _ in 0..64 {
            let port = fastrand::u16(40_000..60_000);
            if let Ok(bound) = TcpListener::bind(("127.0.0.1", port)) {
                listener = Some(bound);
                break;
            }
        }
        let listener = match listener {
            Some(listener) => listener,
            None => TcpListener::bind("127.0.0.1:0")?,
        };
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let base_url = format!("http://{}", addr);
        let (shutdown, shutdown_rx) = mpsc::channel();
        let shared = Arc::new(responses);

        let handle = thread::spawn(move || loop {
            if shutdown_rx.try_recv().is_ok() {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    let responses = Arc::clone(&shared);
                    let _ = stream.set_nonblocking(false);
                    let _ = handle_alpaca_request(stream, &responses);
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        });

        Ok(Self {
            base_url,
            shutdown,
            handle: Some(handle),
        })
    }
}

impl Drop for AlpacaStub {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn handle_alpaca_request(
    mut stream: std::net::TcpStream,
    responses: &AlpacaStubResponses,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(());
    }

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Ok(());
    }
    let method = parts[0];
    let raw_path = parts[1];
    let mut path_only = raw_path.split('?').next().unwrap_or(raw_path);

    // Normalize absolute-form request targets (proxies) and Alpaca's `/v2` prefix.
    if let Some(stripped) = path_only
        .strip_prefix("http://")
        .or_else(|| path_only.strip_prefix("https://"))
    {
        if let Some(idx) = stripped.find('/') {
            path_only = &stripped[idx..];
        } else {
            path_only = "/";
        }
    }
    while path_only.starts_with("//") {
        path_only = &path_only[1..];
    }
    if let Some(rest) = path_only.strip_prefix("/v2") {
        path_only = if rest.is_empty() { "/" } else { rest };
    }

    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 {
            break;
        }
        if header == "\r\n" {
            break;
        }
    }

    match (method, path_only) {
        ("GET", "/account") => write_json_response(&mut stream, "200 OK", &responses.account_json),
        ("GET", "/positions") => {
            write_json_response(&mut stream, "200 OK", &responses.positions_json)
        }
        ("GET", "/orders") => write_json_response(&mut stream, "200 OK", &responses.orders_json),
        ("GET", path) if path.starts_with("/orders/") => {
            write_json_response(&mut stream, "200 OK", &responses.order_json)
        }
        ("GET", path) if path.starts_with("/orders:by_client_order_id/") => {
            write_json_response(&mut stream, "200 OK", &responses.order_json)
        }
        ("DELETE", path) if path.starts_with("/orders") => {
            write_empty_response(&mut stream, "204 No Content")
        }
        _ => write_empty_response(&mut stream, "404 Not Found"),
    }
}

fn write_json_response(
    stream: &mut std::net::TcpStream,
    status: &str,
    body: &str,
) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())
}

fn write_empty_response(stream: &mut std::net::TcpStream, status: &str) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        status
    );
    stream.write_all(response.as_bytes())
}

fn build_order_json(
    order_id: &str,
    symbol: &str,
    status: &str,
    filled_price: Option<f64>,
    filled_at: Option<DateTime<Utc>>,
) -> String {
    serde_json::json!({
        "id": order_id,
        "symbol": symbol,
        "status": status,
        "side": "buy",
        "type": "market",
        "filled_qty": 10.0,
        "filled_avg_price": filled_price,
        "filled_at": filled_at.map(|dt| dt.to_rfc3339()),
    })
    .to_string()
}

fn schema_file_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("src")
        .join("server")
        .join("database")
        .join("pg.sql")
}

fn templates_dir_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("src")
        .join("server")
        .join("strategies")
}

fn baseline_start_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2021, 1, 4).expect("valid date")
}

struct TickerSeed {
    symbol: &'static str,
    base_price: f64,
    drift: f64,
}

impl TickerSeed {
    const fn universe() -> &'static [TickerSeed] {
        &[
            TickerSeed {
                symbol: "AAA",
                base_price: 90.0,
                drift: 0.07,
            },
            TickerSeed {
                symbol: "BBB",
                base_price: 80.0,
                drift: 0.06,
            },
            TickerSeed {
                symbol: "CCC",
                base_price: 70.0,
                drift: 0.05,
            },
            TickerSeed {
                symbol: "DDD",
                base_price: 60.0,
                drift: 0.04,
            },
            TickerSeed {
                symbol: "EEE",
                base_price: 50.0,
                drift: 0.03,
            },
            TickerSeed {
                symbol: "SPY",
                base_price: 400.0,
                drift: 0.02,
            },
        ]
    }
}

async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres error: {}", err);
        }
    });
    Ok(client)
}

async fn drop_database_with_client(client: &Client, db_name: &str) -> Result<()> {
    client
        .execute(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = $1 AND pid <> pg_backend_pid()",
            &[&db_name],
        )
        .await
        .ok();
    client
        .batch_execute(&format!("DROP DATABASE IF EXISTS {}", db_name))
        .await?;
    Ok(())
}
