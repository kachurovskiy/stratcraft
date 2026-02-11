use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use engine::{
    commands::{
        backtest_accounts, backtest_active, export_market_data, generate_signals, optimize,
        plan_operations, reconcile_trades, train_lightgbm, verify,
    },
    context::AppContext,
    strategy,
};
use log::{info, warn};
use std::env;
use std::path::PathBuf;

const DEFAULT_LGBM_MODEL_REL_PATH: &str = "src/models/lightgbm_model.txt";
const DEFAULT_MARKET_DATA_FILE: &str = "../data/market-data.bin";

#[derive(Parser)]
#[command(name = "engine")]
#[command(about = "A high-performance strategy optimization tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Optimize strategy parameters
    Optimize {
        /// Template ID to optimize
        template_id: String,
        /// Path to the market data snapshot file
        #[arg(long = "data-file", value_name = "PATH")]
        data_file: Option<PathBuf>,
    },
    /// Verify top cached parameter sets over the configured verification window across all tickers
    Verify {
        /// Template ID to verify
        template_id: String,
        /// Path to the market data snapshot file
        #[arg(long = "data-file", value_name = "PATH")]
        data_file: Option<PathBuf>,
    },
    /// Generate missing signals for active strategies
    GenerateSignals,
    /// Backtest all active strategies and refresh stored results
    BacktestActive {
        /// Ticker scope to backtest (validation uses only validation tickers, training uses training tickers, all uses the full set)
        #[arg(long, value_enum, default_value_t = backtest_active::BacktestScope::Validation)]
        scope: backtest_active::BacktestScope,
        /// Comma or space separated list of months of history to include (approx. 30.4 days per month)
        #[arg(value_delimiter = ',', num_args = 1..)]
        months: Vec<u32>,
    },
    /// Backtest strategies linked to live accounts using all tickers
    BacktestAccounts,
    /// Rebuild account operations for strategies that have both account and start date defined
    PlanOperations,
    /// Reconcile live trades with broker order states
    ReconcileTrades,
    /// Export market data snapshot for remote optimizers
    ExportMarketData {
        /// Destination file for the snapshot
        #[arg(short, long = "output", value_name = "PATH")]
        output: Option<PathBuf>,
    },
    /// Train the LightGBM model using in-database market data
    TrainLightgbm {
        /// Destination for the trained model (defaults to engine/src/models/lightgbm_model.txt)
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Number of boosting iterations
        #[arg(long)]
        num_iterations: Option<u32>,
        /// Learning rate
        #[arg(long)]
        learning_rate: Option<f64>,
        /// Number of leaves in one tree
        #[arg(long)]
        num_leaves: Option<u32>,
        /// Maximum tree depth (-1 means no limit)
        #[arg(long)]
        max_depth: Option<i32>,
        /// Minimum number of observations in one leaf
        #[arg(long)]
        min_data_in_leaf: Option<u32>,
        /// Minimum gain to split
        #[arg(long)]
        min_gain_to_split: Option<f64>,
        /// L1 regularization
        #[arg(long)]
        lambda_l1: Option<f64>,
        /// L2 regularization
        #[arg(long)]
        lambda_l2: Option<f64>,
        /// Feature fraction (0..=1)
        #[arg(long)]
        feature_fraction: Option<f64>,
        /// Bagging fraction (0..=1)
        #[arg(long)]
        bagging_fraction: Option<f64>,
        /// Bagging frequency (0 disables bagging)
        #[arg(long)]
        bagging_freq: Option<u32>,
        /// Early stopping rounds (0 disables early stopping)
        #[arg(long)]
        early_stopping_round: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let Cli { command } = cli;

    // Pin Rayon to 16 logical processors for consistent parallelism during heavy workloads.
    env::set_var("RAYON_NUM_THREADS", "16");

    let database_url = env::var("DATABASE_URL").ok();
    if database_url.is_none() && command_requires_database(&command) {
        return Err(anyhow!(
            "DATABASE_URL must be set for this command. For offline runs, use a market data snapshot."
        ));
    }
    let app_context = AppContext::initialize(database_url).await?;
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("Starting engine. Not financial advice. Most retail traders lose money. Use at your own risk.");

    if !matches!(command, Commands::TrainLightgbm { .. }) {
        if let Err(err) = load_lightgbm_model(&app_context).await {
            warn!("LightGBM model load skipped: {err}");
        }
    }

    match command {
        Commands::Optimize {
            template_id,
            data_file,
        } => {
            let market_data_path = resolve_market_data_path(data_file);
            optimize::run(&app_context, &template_id, &market_data_path).await?;
        }
        Commands::Verify {
            template_id,
            data_file,
        } => {
            let market_data_path = resolve_market_data_path(data_file);
            verify::run(&app_context, &template_id, &market_data_path).await?;
        }
        Commands::GenerateSignals => {
            generate_signals::run(&app_context).await?;
        }
        Commands::BacktestActive { scope, months } => {
            backtest_active::run(&app_context, scope, &months).await?;
        }
        Commands::BacktestAccounts => {
            backtest_accounts::run(&app_context).await?;
        }
        Commands::PlanOperations => {
            plan_operations::run(&app_context).await?;
        }
        Commands::ReconcileTrades => {
            reconcile_trades::run(&app_context).await?;
        }
        Commands::ExportMarketData { output } => {
            let output_path = resolve_market_data_path(output);
            export_market_data::run(&app_context, &output_path).await?;
        }
        Commands::TrainLightgbm {
            output,
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
        } => {
            let fallback_path = PathBuf::from(DEFAULT_LGBM_MODEL_REL_PATH);
            train_lightgbm::run(
                &app_context,
                output.or_else(|| Some(fallback_path)),
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
            )
            .await?;
        }
    }

    Ok(())
}

async fn load_lightgbm_model(app_context: &AppContext) -> Result<()> {
    if let Ok(db) = app_context.database().await {
        match db.get_lightgbm_models().await {
            Ok(models) if !models.is_empty() => {
                for (idx, model) in models.iter().enumerate() {
                    strategy::lightgbm::register_model_text(&model.id, &model.tree_text, idx == 0)?;
                }
                info!("Loaded {} LightGBM model(s) from database", models.len());
                return Ok(());
            }
            Ok(_) => {
                warn!("No LightGBM model found in database; falling back to local file.");
            }
            Err(err) => {
                warn!("Failed to read LightGBM models from database: {err}");
            }
        }
    } else {
        warn!("Database connection unavailable; falling back to local model file.");
    }

    let default_model_path = strategy::lightgbm::default_model_path();
    if let Err(err) = strategy::lightgbm::load_model_if_exists(&default_model_path) {
        warn!(
            "LightGBM model not loaded from {}: {}",
            default_model_path.display(),
            err
        );
    }

    Ok(())
}

fn resolve_market_data_path(cli_value: Option<PathBuf>) -> PathBuf {
    if let Some(path) = cli_value {
        return path;
    }

    PathBuf::from(DEFAULT_MARKET_DATA_FILE)
}

fn command_requires_database(command: &Commands) -> bool {
    match command {
        Commands::Optimize { data_file, .. } => data_file.is_none(),
        Commands::Verify { .. }
        | Commands::GenerateSignals
        | Commands::BacktestActive { .. }
        | Commands::BacktestAccounts
        | Commands::PlanOperations
        | Commands::ReconcileTrades
        | Commands::ExportMarketData { .. }
        | Commands::TrainLightgbm { .. } => true,
    }
}
