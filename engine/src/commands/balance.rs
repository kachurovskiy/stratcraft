use crate::commands::market_data_snapshot::ensure_market_data_file;
use crate::config::require_setting_date;
use crate::context::{AppContext, MarketDataFilters};
use crate::data_context::TickerScope;
use crate::database::Database;
use crate::optimizer::parameter_signature;
use anyhow::Result;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::path::Path;

enum BalanceScope {
    Training,
    Validation,
}

impl BalanceScope {
    fn label(&self) -> &'static str {
        match self {
            BalanceScope::Training => "training",
            BalanceScope::Validation => "validation",
        }
    }

    fn ticker_scope(&self) -> TickerScope {
        match self {
            BalanceScope::Training => TickerScope::TrainingOnly,
            BalanceScope::Validation => TickerScope::ValidationOnly,
        }
    }
}

pub async fn run(app: &AppContext, template_id: &str, market_data_file: &Path) -> Result<()> {
    info!("Received balance command for template_id={}", template_id);
    ensure_market_data_file(market_data_file).await?;
    info!(
        "Using market data snapshot from {}",
        market_data_file.display()
    );

    let db = app.database().await?;
    let cache_entries = db.backtest_cache_entries_for_template(template_id).await?;
    if cache_entries.is_empty() {
        info!(
            "No cached backtest rows found for template {} to balance",
            template_id
        );
        return Ok(());
    }

    let settings = db.get_all_settings().await?;
    let balance_start = require_setting_date(&settings, "BALANCE_WINDOW_START_DATE")?;
    let balance_end = require_setting_date(&settings, "BALANCE_WINDOW_END_DATE")?;
    let start_label = balance_start.format("%Y-%m-%d").to_string();
    let end_label = balance_end.format("%Y-%m-%d").to_string();

    info!(
        "Preparing to compute balance metrics for {} cached parameter set(s) across training and validation tickers on {} - {} data",
        cache_entries.len(),
        start_label,
        end_label
    );

    let filters = MarketDataFilters {
        start_date: Some(balance_start),
        end_date: Some(balance_end),
    };

    let mut parameter_sets = Vec::with_capacity(cache_entries.len());
    let mut ids_by_signature: HashMap<String, Vec<String>> = HashMap::new();
    let mut scheduled_signatures = HashSet::new();
    for entry in cache_entries {
        let signature = parameter_signature(&entry.parameters);
        ids_by_signature
            .entry(signature.clone())
            .or_default()
            .push(entry.id);
        if scheduled_signatures.insert(signature) {
            parameter_sets.push(entry.parameters);
        }
    }

    if parameter_sets.is_empty() {
        info!(
            "No valid parameter sets available to balance for template {}",
            template_id
        );
        return Ok(());
    }

    let training_updated = run_balance_scope(
        app,
        &db,
        template_id,
        market_data_file,
        &parameter_sets,
        &ids_by_signature,
        filters,
        BalanceScope::Training,
        &start_label,
        &end_label,
    )
    .await?;

    let validation_updated = run_balance_scope(
        app,
        &db,
        template_id,
        market_data_file,
        &parameter_sets,
        &ids_by_signature,
        filters,
        BalanceScope::Validation,
        &start_label,
        &end_label,
    )
    .await?;

    info!(
        "Balance completed: updated {} training and {} validation cached row(s) for template {}",
        training_updated, validation_updated, template_id
    );

    Ok(())
}

async fn run_balance_scope(
    app: &AppContext,
    db: &Database,
    template_id: &str,
    market_data_file: &Path,
    parameter_sets: &[HashMap<String, f64>],
    ids_by_signature: &HashMap<String, Vec<String>>,
    filters: MarketDataFilters,
    scope: BalanceScope,
    start_label: &str,
    end_label: &str,
) -> Result<usize> {
    let scope_label = scope.label();
    let ticker_scope = scope.ticker_scope();

    let mut context = match app
        .engine_context_from_file(market_data_file, ticker_scope, Some(filters))
        .await
    {
        Ok(context) => context,
        Err(error) => {
            warn!(
                "Skipping balance {} run for template {}: {}",
                scope_label, template_id, error
            );
            return Ok(0);
        }
    };
    let mut optimizer = context.optimizer();

    info!(
        "Running balance backtests for {} parameter set(s) on {} - {} candles ({} tickers)",
        parameter_sets.len(),
        start_label,
        end_label,
        scope_label
    );
    let results = optimizer
        .run_parameter_batch(template_id, parameter_sets, false)
        .await?;

    if results.is_empty() {
        info!(
            "Balance {} produced no results for template {}",
            scope_label, template_id
        );
        return Ok(0);
    }

    info!(
        "Received {} balance {} result(s) for {} requested parameter set(s)",
        results.len(),
        scope_label,
        parameter_sets.len()
    );

    let mut updated = 0;
    for result in results {
        let signature = parameter_signature(&result.parameters);
        if let Some(ids) = ids_by_signature.get(&signature) {
            for cache_id in ids {
                match scope {
                    BalanceScope::Training => {
                        db.update_backtest_cache_balance_training(
                            cache_id,
                            Some(result.sharpe_ratio),
                            Some(result.calmar_ratio),
                            Some(result.cagr),
                            Some(result.max_drawdown_ratio),
                        )
                        .await?;
                    }
                    BalanceScope::Validation => {
                        db.update_backtest_cache_balance_validation(
                            cache_id,
                            Some(result.sharpe_ratio),
                            Some(result.calmar_ratio),
                            Some(result.cagr),
                            Some(result.max_drawdown_ratio),
                        )
                        .await?;
                    }
                }
                updated += 1;
            }
        } else {
            warn!(
                "Balance {} result with signature {} did not match cached entries",
                scope_label, signature
            );
        }
    }

    info!(
        "Balance {} completed: updated {} cached row(s) for template {}",
        scope_label, updated, template_id
    );

    Ok(updated)
}
