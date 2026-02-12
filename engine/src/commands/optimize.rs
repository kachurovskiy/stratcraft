use crate::commands::market_data_snapshot::ensure_market_data_file;
use crate::config::{require_setting_date, EngineRuntimeSettings};
use crate::context::{AppContext, MarketDataFilters};
use crate::data_context::{MarketData, TickerScope};
use crate::optimizer_status::OptimizerStatus;
use anyhow::Result;
use log::{info, warn};
use std::path::Path;

pub async fn run(app: &AppContext, template_id: &str, market_data_file: &Path) -> Result<()> {
    info!(
        "Received optimize command for template_id={} (auto parameter detection)",
        template_id
    );
    ensure_market_data_file(market_data_file).await?;
    info!(
        "Using market data snapshot from {}",
        market_data_file.display()
    );

    let settings = match app.database().await {
        Ok(db) => db.get_all_settings().await?,
        Err(error) => {
            warn!(
                "Database unavailable ({}). Using settings from market data snapshot.",
                error
            );
            let status = OptimizerStatus::new();
            let snapshot = MarketData::load_from_file(market_data_file, &status)?;
            snapshot.settings().clone()
        }
    };
    let training_start = require_setting_date(&settings, "OPTIMIZER_TRAINING_START_DATE")?;
    let training_end = require_setting_date(&settings, "OPTIMIZER_TRAINING_END_DATE")?;
    info!(
        "Restricting optimization to training tickers and {} - {} market data window",
        training_start.format("%Y-%m-%d"),
        training_end.format("%Y-%m-%d")
    );
    let runtime_settings = EngineRuntimeSettings::from_settings_map(&settings)?;
    let objective_label = runtime_settings.local_optimization_objective.label();
    info!(
        "Objective: maximize {} while keeping max drawdown at or below {:.0}%.",
        objective_label,
        runtime_settings.max_allowed_drawdown_ratio * 100.0
    );
    let mut context = app
        .engine_context_from_file(
            market_data_file,
            TickerScope::TrainingOnly,
            Some(MarketDataFilters {
                start_date: Some(training_start),
                end_date: Some(training_end),
            }),
        )
        .await?;
    let mut optimizer = context.optimizer();
    let (param_names, param_ranges) = optimizer.detect_optimizable_parameters(template_id).await?;
    optimizer
        .optimize_local_search(template_id, &param_names, &param_ranges)
        .await
}
