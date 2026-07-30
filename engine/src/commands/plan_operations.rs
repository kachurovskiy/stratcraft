use crate::alpaca::AlpacaClient;
use crate::config::EngineRuntimeSettings;
use crate::context::AppContext;
use crate::engine::{AccountStateSnapshot, Engine};
use crate::models::{AccountSignalSkip, SignalAction};
use anyhow::{Context, Result};
use log::{info, warn};
use reqwest::Client;
use serde_json::json;
use std::collections::{HashMap, HashSet};
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
    let mut strategies_by_account: HashMap<String, usize> = HashMap::new();
    for strategy in strategies.iter().filter(|s| s.account_id.is_some()) {
        if let Some(account_id) = strategy.account_id.as_deref() {
            *strategies_by_account
                .entry(account_id.to_string())
                .or_insert(0) += 1;
        }
    }
    let mut account_state_cache: HashMap<String, AccountStateSnapshot> = HashMap::new();

    for strategy in strategies.into_iter().filter(|s| s.account_id.is_some()) {
        let Some(account_id) = strategy.account_id.clone() else {
            continue;
        };

        let shared_account_strategy_count =
            strategies_by_account.get(&account_id).copied().unwrap_or(1);
        let has_strategy_allocation = strategy
            .allocated_cash
            .filter(|value| value.is_finite() && *value > 0.0)
            .is_some();
        if shared_account_strategy_count > 1 && !has_strategy_allocation {
            skipped += 1;
            warn!(
                "Skipping strategy {} - shared account {} requires a positive live allocation",
                strategy.name, account_id
            );
            let metadata = json!({
                "strategyId": strategy.id,
                "accountId": account_id,
                "sharedAccountStrategyCount": shared_account_strategy_count,
            });
            db.insert_system_log(
                "plan-operations-job",
                "warn",
                &format!(
                    "Skipped strategy {} because shared account strategies require live allocations",
                    strategy.name
                ),
                Some(metadata),
            )
            .await?;
            continue;
        }

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
        let account_state = if let Some(cached) = account_state_cache.get(&account_id) {
            cached.clone()
        } else {
            match alpaca_client.fetch_account_state().await {
                Ok(state) => {
                    account_state_cache.insert(account_id.clone(), state.clone());
                    state
                }
                Err(err) => {
                    skipped += 1;
                    warn!(
                        "Skipping strategy {} - failed to fetch account state: {}",
                        strategy.name, err
                    );
                    continue;
                }
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

        let existing_trades = db.get_strategy_live_trades(&strategy.id).await?;
        let engine = Engine::from_parameters(&strategy.parameters, runtime_settings.clone());

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

        let existing_buy_operations_today = db
            .count_buy_operations_for_day(&strategy.id, target_date)
            .await?
            .max(0) as usize;
        let account_ticker_locks = db
            .get_account_ticker_locks(&account_id, &strategy.id)
            .await?;
        let reserved_open_value = db
            .get_account_pending_open_operation_value(&account_id, &strategy.id)
            .await?;
        let effective_account_state =
            account_state_after_pending_reservations(&account_state, reserved_open_value);

        let plan = engine.plan_account_operations_with_locks(
            &strategy.id,
            &account_id,
            &signals,
            &candles,
            target_date,
            &effective_account_state,
            strategy.allocated_cash,
            &excluded_tickers,
            &account_ticker_locks,
            &existing_trades,
            existing_buy_operations_today,
            &ticker_metadata,
        );

        let mut signal_skips: Vec<AccountSignalSkip> = plan.skipped_signals.clone();
        for operation in &plan.operations {
            let action = match operation.reason.as_deref() {
                Some("buy_signal_sync") => SignalAction::Buy,
                Some("sell_signal_sync") => SignalAction::Sell,
                _ => continue,
            };
            signal_skips.push(AccountSignalSkip {
                ticker: operation.ticker.clone(),
                signal_date: target_date,
                action,
                reason: "operation_requested".to_string(),
                details: operation.reason.clone(),
            });
        }

        if !signal_skips.is_empty() {
            if let Err(err) = db
                .insert_account_signal_skips(
                    &strategy.id,
                    Some(&account_id),
                    "plan_operations",
                    &signal_skips,
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

fn account_state_after_pending_reservations(
    account_state: &AccountStateSnapshot,
    reserved_open_value: f64,
) -> AccountStateSnapshot {
    let reserved = if reserved_open_value.is_finite() && reserved_open_value > 0.0 {
        reserved_open_value
    } else {
        0.0
    };
    if reserved <= 0.0 {
        return account_state.clone();
    }

    let mut adjusted = account_state.clone();
    adjusted.available_cash = (account_state.available_cash - reserved).max(0.0);
    adjusted.buying_power = account_state
        .buying_power
        .map(|buying_power| (buying_power - reserved).max(0.0));
    adjusted
}
