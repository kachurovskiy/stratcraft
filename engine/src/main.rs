use anyhow::anyhow;
use clap::{Parser, Subcommand};
use engine::{
    commands::{
        backtest_accounts, backtest_active, backtest_start_timing, balance, explore,
        export_market_data, generate_signals, optimize, plan_operations, reconcile_trades, verify,
    },
    context::AppContext,
};
use log::info;
use std::env;
use std::path::PathBuf;

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
    /// Compute training/validation balance metrics for cached parameter sets
    Balance {
        /// Template ID to balance
        template_id: String,
        /// Path to the market data snapshot file
        #[arg(long = "data-file", value_name = "PATH")]
        data_file: Option<PathBuf>,
    },
    /// Explore single-step parameter variations for cached backtests
    Explore {
        /// Template ID to explore
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
        #[arg(long, value_enum)]
        scope: backtest_active::BacktestScope,
        /// Comma or space separated list of months of history to include (approx. 30.4 days per month)
        #[arg(value_delimiter = ',', num_args = 1..)]
        months: Vec<u32>,
    },
    /// Backtest strategies linked to live accounts using all tickers
    BacktestAccounts,
    /// Compute horizon-limited start timing samples for one strategy
    BacktestStartTiming {
        /// Strategy ID to sample
        #[arg(long = "strategy-id")]
        strategy_id: String,
        /// Ticker scope to backtest
        #[arg(long, value_enum, default_value = "validation")]
        scope: backtest_active::BacktestScope,
        /// Number of weekly start dates to consider
        #[arg(long, default_value_t = 104)]
        weeks: usize,
    },
    /// Rebuild account operations for strategies that have both account and start date defined
    PlanOperations,
    /// Reconcile live trades with broker order states
    ReconcileTrades,
    /// Export market data snapshot for local optimization
    ExportMarketData {
        /// Destination file for the snapshot
        #[arg(short, long = "output", value_name = "PATH")]
        output: Option<PathBuf>,
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
        Commands::Balance {
            template_id,
            data_file,
        } => {
            let market_data_path = resolve_market_data_path(data_file);
            balance::run(&app_context, &template_id, &market_data_path).await?;
        }
        Commands::Explore {
            template_id,
            data_file,
        } => {
            let market_data_path = resolve_market_data_path(data_file);
            explore::run(&app_context, &template_id, &market_data_path).await?;
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
        Commands::BacktestStartTiming {
            strategy_id,
            scope,
            weeks,
        } => {
            backtest_start_timing::run(&app_context, &strategy_id, scope, weeks).await?;
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
        Commands::Explore { data_file, .. } => data_file.is_none(),
        Commands::Verify { .. }
        | Commands::Balance { .. }
        | Commands::GenerateSignals
        | Commands::BacktestActive { .. }
        | Commands::BacktestAccounts
        | Commands::BacktestStartTiming { .. }
        | Commands::PlanOperations
        | Commands::ReconcileTrades
        | Commands::ExportMarketData { .. } => true,
    }
}
