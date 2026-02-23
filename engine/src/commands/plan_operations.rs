use crate::alpaca::AlpacaClient;
use crate::config::EngineRuntimeSettings;
use crate::context::AppContext;
use crate::engine::Engine;
use anyhow::{Context, Result};
use log::{info, warn};
use reqwest::Client;
use serde_json::json;
use std::collections::HashSet;
use std::time::Duration;

pub async fn run(app: &AppContext) -> Result<()> {
    let mut db = app.database().await?;
    let settings = db.get_all_settings().await?;
    let runtime_settings = EngineRuntimeSettings::from_settings_map(&settings)?;
    let strategies = db.get_active_strategies().await?;
    if strategies.is_empty() {
        info!("No active strategies found");
        return Ok(());
    }

    let http_client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to create HTTP client for account state fetches")?;

    let mut processed = 0usize;
    let mut skipped = 0usize;

    for strategy in strategies.into_iter().filter(|s| s.account_id.is_some()) {
        let Some(account_id) = strategy.account_id.clone() else {
            continue;
        };

        let creds = match db.get_account_credentials(&account_id).await? {
            Some(creds) => creds,
            None => {
                skipped += 1;
                warn!(
                    "Skipping strategy {} - account {} not found",
                    strategy.name, account_id
                );
                continue;
            }
        };

        if !creds.provider.eq_ignore_ascii_case("alpaca") {
            skipped += 1;
            warn!(
                "Skipping strategy {} - unsupported account provider {}",
                strategy.name, creds.provider
            );
            continue;
        }

        let alpaca_client = match AlpacaClient::new(&http_client, &creds, &settings) {
            Ok(client) => client,
            Err(err) => {
                skipped += 1;
                warn!(
                    "Skipping strategy {} - failed to initialize Alpaca client: {}",
                    strategy.name, err
                );
                continue;
            }
        };
        let account_state = match alpaca_client.fetch_account_state().await {
            Ok(state) => state,
            Err(err) => {
                skipped += 1;
                warn!(
                    "Skipping strategy {} - failed to fetch account state: {}",
                    strategy.name, err
                );
                continue;
            }
        };

        let latest_signal_date = db.get_latest_signal_date(&strategy.id).await?;
        let signals = if let Some(date) = latest_signal_date {
            db.get_signals_for_strategy_in_range(&strategy.id, date, date)
                .await?
        } else {
            Vec::new()
        };
        if signals.is_empty() && account_state.positions.is_empty() {
            skipped += 1;
            warn!(
                "Skipping strategy {} - no recent signals or open trades",
                strategy.name
            );
            continue;
        }

        let mut candle_symbols: HashSet<String> = signals
            .iter()
            .map(|signal| signal.ticker.trim().to_uppercase())
            .filter(|ticker| !ticker.is_empty())
            .collect();
        for position in &account_state.positions {
            let ticker = position.ticker.trim().to_uppercase();
            if !ticker.is_empty() {
                candle_symbols.insert(ticker);
            }
        }
        if candle_symbols.is_empty() {
            skipped += 1;
            warn!(
                "Skipping strategy {} - signals missing tickers",
                strategy.name
            );
            continue;
        }

        let mut symbol_list: Vec<String> = candle_symbols.drain().collect();
        symbol_list.sort();
        let ticker_metadata = db.get_ticker_metadata(&symbol_list).await?;
        let candles = db.get_candles_for_tickers(&symbol_list).await?;
        if candles.is_empty() {
            skipped += 1;
            warn!(
                "Skipping strategy {} - no candles for tickers {:?}",
                strategy.name, symbol_list
            );
            continue;
        }

        let max_candle_date = candles.iter().map(|c| c.date).max();
        let target_date = latest_signal_date.or(max_candle_date);
        let Some(target_date) = target_date else {
            skipped += 1;
            warn!(
                "Skipping strategy {} - unable to determine target date",
                strategy.name
            );
            continue;
        };

        let engine = Engine::from_parameters(&strategy.parameters, runtime_settings.clone());
        let effective_buying_power = engine.effective_buying_power_for_account(&account_state);
        info!(
            "Strategy {} (account {}) effective buying power for sizing: {:.2}",
            strategy.name, account_id, effective_buying_power
        );

        let excluded_keywords: Vec<String> = strategy
            .excluded_keywords
            .iter()
            .map(|keyword| keyword.trim().to_ascii_lowercase())
            .filter(|keyword| !keyword.is_empty())
            .collect();
        let mut excluded_tickers: HashSet<String> = strategy
            .excluded_tickers
            .iter()
            .map(|ticker| ticker.trim().to_uppercase())
            .filter(|ticker| !ticker.is_empty())
            .collect();
        if !excluded_keywords.is_empty() {
            for symbol in &symbol_list {
                let symbol_lower = symbol.to_ascii_lowercase();
                let name_lower = ticker_metadata
                    .get(symbol)
                    .and_then(|info| info.name.as_deref())
                    .map(|name| name.to_ascii_lowercase());
                let matches_keyword = excluded_keywords.iter().any(|keyword| {
                    symbol_lower.contains(keyword)
                        || name_lower
                            .as_deref()
                            .map(|name| name.contains(keyword))
                            .unwrap_or(false)
                });
                if matches_keyword {
                    excluded_tickers.insert(symbol.clone());
                }
            }
        }

        let existing_trades = db.get_strategy_live_trades(&strategy.id).await?;
        let existing_buy_operations_today = db
            .count_buy_operations_for_day(&strategy.id, target_date)
            .await?
            .max(0) as usize;

        let plan = engine.plan_account_operations(
            &strategy.id,
            &account_id,
            &signals,
            &candles,
            target_date,
            &account_state,
            &excluded_tickers,
            &existing_trades,
            existing_buy_operations_today,
            &ticker_metadata,
        );

        if !plan.skipped_signals.is_empty() {
            if let Err(err) = db
                .insert_account_signal_skips(
                    &strategy.id,
                    Some(&account_id),
                    "plan_operations",
                    &plan.skipped_signals,
                )
                .await
            {
                warn!(
                    "Failed to record signal skip reasons for strategy {}: {}",
                    strategy.name, err
                );
            }
        }

        if plan.operations.is_empty() {
            skipped += 1;
            let metadata = json!({
                "strategyId": strategy.id,
                "latestDate": target_date,
                "notes": plan.notes,
            });
            db.insert_system_log(
                "plan-operations-job",
                "info",
                &format!(
                    "No account operations generated for strategy {}",
                    strategy.name
                ),
                Some(metadata),
            )
            .await?;
            continue;
        }

        db.replace_account_operations_for_strategy(&account_id, &strategy.id, &plan.operations)
            .await?;

        processed += 1;
        info!(
            "Planned {} operation{} for {} as of {}",
            plan.operations.len(),
            if plan.operations.len() == 1 { "" } else { "s" },
            strategy.name,
            target_date.format("%Y-%m-%d")
        );
    }

    info!(
        "Completed operation planning for {} strateg{} ({} skipped)",
        processed,
        if processed == 1 { "y" } else { "ies" },
        skipped
    );
    Ok(())
}
