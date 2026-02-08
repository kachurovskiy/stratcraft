use crate::commands::market_data_snapshot::ensure_market_data_file;
use crate::config::require_setting_date;
use crate::context::{AppContext, MarketDataFilters};
use crate::data_context::TickerScope;
use crate::optimizer::parameter_signature;
use anyhow::Result;
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::path::Path;

pub async fn run(app: &AppContext, template_id: &str, market_data_file: &Path) -> Result<()> {
    info!("Received verify command for template_id={}", template_id);
    ensure_market_data_file(market_data_file).await?;
    info!(
        "Using market data snapshot from {}",
        market_data_file.display()
    );

    let db = app.database().await?;
    let cache_entries = db.backtest_cache_entries_for_template(template_id).await?;
    if cache_entries.is_empty() {
        info!(
            "No cached backtest rows found for template {} to verify",
            template_id
        );
        return Ok(());
    }

    let settings = db.get_all_settings().await?;
    let verify_start = require_setting_date(&settings, "VERIFY_WINDOW_START_DATE")?;
    let verify_end = require_setting_date(&settings, "VERIFY_WINDOW_END_DATE")?;
    info!(
        "Preparing to verify {} cached parameter set(s) across all tickers on {} - {} data",
        cache_entries.len(),
        verify_start.format("%Y-%m-%d"),
        verify_end.format("%Y-%m-%d")
    );
    let filters = MarketDataFilters {
        start_date: Some(verify_start),
        end_date: Some(verify_end),
    };

    let mut context = app
        .engine_context_from_file(market_data_file, TickerScope::AllTickers, Some(filters))
        .await?;
    let mut optimizer = context.optimizer();

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
            "No valid parameter sets available to verify for template {}",
            template_id
        );
        return Ok(());
    }

    info!(
        "Running verification backtests for {} parameter set(s) on {} - {} candles (all tickers)",
        parameter_sets.len(),
        verify_start.format("%Y-%m-%d"),
        verify_end.format("%Y-%m-%d")
    );
    let results = optimizer
        .run_parameter_batch(template_id, &parameter_sets, false)
        .await?;

    if results.is_empty() {
        info!(
            "Verification produced no results for template {}",
            template_id
        );
        return Ok(());
    }

    info!(
        "Received {} verification result(s) for {} requested parameter set(s)",
        results.len(),
        parameter_sets.len()
    );

    let mut updated = 0;
    for result in results {
        let signature = parameter_signature(&result.parameters);
        if let Some(ids) = ids_by_signature.get(&signature) {
            for cache_id in ids {
                db.update_backtest_cache_verification(
                    cache_id,
                    Some(result.sharpe_ratio),
                    Some(result.calmar_ratio),
                    Some(result.cagr),
                    Some(result.max_drawdown_ratio),
                )
                .await?;
                updated += 1;
            }
        } else {
            warn!(
                "Verification result with signature {} did not match cached entries",
                signature
            );
        }
    }

    info!(
        "Verification completed: updated {} cached row(s) for template {}",
        updated, template_id
    );

    Ok(())
}
