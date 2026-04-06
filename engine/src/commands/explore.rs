use crate::commands::market_data_snapshot::ensure_market_data_file;
use crate::config::require_setting_date;
use crate::context::{AppContext, MarketDataFilters};
use crate::data_context::{MarketData, TickerScope};
use crate::param_utils::{add_single_parameter_neighbor_variations, clamp_to_bounds};
use anyhow::Result;
use log::{info, warn};
use std::collections::HashSet;
use std::path::Path;

pub async fn run(app: &AppContext, template_id: &str, market_data_file: &Path) -> Result<()> {
    info!("Received explore command for template_id={}", template_id);
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
            let snapshot = MarketData::load_from_file(market_data_file)?;
            snapshot.settings().clone()
        }
    };

    let training_start = require_setting_date(&settings, "OPTIMIZER_TRAINING_START_DATE")?;
    let training_end = require_setting_date(&settings, "OPTIMIZER_TRAINING_END_DATE")?;
    info!(
        "Restricting explore runs to training tickers and {} - {} market data window",
        training_start.format("%Y-%m-%d"),
        training_end.format("%Y-%m-%d")
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

    let (param_names, param_ranges) =
        match optimizer.detect_optimizable_parameters(template_id).await {
            Ok(result) => result,
            Err(error) => {
                let message = error.to_string();
                if message.contains("No optimizable parameters") {
                    info!("Skipping explore for {}: {}", template_id, message);
                    return Ok(());
                }
                return Err(error);
            }
        };

    let mut baseline_params = optimizer.build_baseline_parameters(template_id).await?;
    clamp_to_bounds(&mut baseline_params, &param_ranges, &param_names);

    let step_multipliers = [-1.0_f64, 1.0_f64];
    let mut seen_variations = HashSet::new();
    let mut neighbor_variations = Vec::new();
    add_single_parameter_neighbor_variations(
        &param_names,
        &param_ranges,
        &step_multipliers,
        &baseline_params,
        &mut seen_variations,
        &mut neighbor_variations,
    );

    if neighbor_variations.is_empty() {
        info!(
            "No neighbor variations to explore for template {}",
            template_id
        );
        return Ok(());
    }

    info!(
        "Running explore backtests for {} variation(s) on template {}",
        neighbor_variations.len(),
        template_id
    );
    let results = optimizer
        .run_parameter_batch(template_id, &neighbor_variations, true)
        .await?;

    info!(
        "Explore completed for template {}: {} result(s) returned",
        template_id,
        results.len()
    );

    Ok(())
}
