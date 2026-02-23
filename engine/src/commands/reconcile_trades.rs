use crate::alpaca::{AlpacaClient, OrderEvaluation, OrderState};
use crate::context::AppContext;
use crate::database::Database;
use crate::engine::AccountPositionState;
use crate::models::{Trade, TradeStatus};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use log::{info, warn};
use reqwest::Client;
use std::{
    collections::{HashMap, HashSet},
    time::Duration as StdDuration,
};

const PNL_EPSILON: f64 = 1e-6;

pub async fn run(app: &AppContext) -> Result<()> {
    let db = app.database().await?;
    let candidates = db.get_live_trades_with_accounts().await?;
    if candidates.is_empty() {
        info!("No live trades require reconciliation");
        return Ok(());
    }
    let settings = db.get_all_settings().await?;

    let http_client = Client::builder()
        .timeout(StdDuration::from_secs(30))
        .build()
        .context("failed to construct HTTP client")?;

    let mut grouped: HashMap<String, Vec<Trade>> = HashMap::new();
    for candidate in candidates {
        grouped
            .entry(candidate.account_id)
            .or_default()
            .push(candidate.trade);
    }

    let mut reconciled = 0usize;
    let mut skipped = 0usize;

    for (account_id, trades) in grouped {
        let Some(credentials) = db.get_account_credentials(&account_id).await? else {
            warn!(
                "Skipping {} trade(s) for account {} without credentials",
                trades.len(),
                account_id
            );
            skipped += trades.len();
            continue;
        };

        if !credentials.provider.eq_ignore_ascii_case("alpaca") {
            warn!(
                "Skipping {} trade(s) for unsupported provider {} on account {}",
                trades.len(),
                credentials.provider,
                account_id
            );
            skipped += trades.len();
            continue;
        }

        let client = match AlpacaClient::new(&http_client, &credentials, &settings) {
            Ok(client) => client,
            Err(err) => {
                warn!(
                    "Skipping {} trade(s) for account {}: Alpaca client init failed: {}",
                    trades.len(),
                    account_id,
                    err
                );
                skipped += trades.len();
                continue;
            }
        };
        let account_state = match client.fetch_account_state().await {
            Ok(state) => Some(state),
            Err(err) => {
                warn!(
                    "Failed to fetch account state for account {}: {}",
                    account_id, err
                );
                None
            }
        };

        let positions: Vec<AccountPositionState> = account_state
            .as_ref()
            .map(|state| state.positions.clone())
            .unwrap_or_default();

        let mut position_prices = match fetch_last_candle_closes(&db, &trades, &positions).await {
            Ok(prices) => prices,
            Err(err) => {
                warn!(
                    "Failed to fetch candle closes for account {}: {}",
                    account_id, err
                );
                HashMap::new()
            }
        };

        if !positions.is_empty() {
            for position in &positions {
                if position_prices.contains_key(&position.ticker) {
                    continue;
                }
                if let Some(price) = position.current_price {
                    if price.is_finite() && price > 0.0 {
                        position_prices.insert(position.ticker.clone(), price);
                    }
                }
            }
        }

        for mut trade in trades {
            match reconcile_trade(&client, &mut trade, &position_prices, &positions).await {
                Ok(true) => {
                    db.ensure_ticker_exists(&trade.ticker).await?;
                    db.persist_trade_reconciliation(&trade).await?;
                    reconciled += 1;
                }
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        "Failed to reconcile trade {} for strategy {}: {}",
                        trade.id, trade.strategy_id, err
                    );
                    skipped += 1;
                }
            }
        }
    }

    info!(
        "Reconciled {} trade{} ({} skipped)",
        reconciled,
        if reconciled == 1 { "" } else { "s" },
        skipped
    );

    Ok(())
}

async fn reconcile_trade(
    client: &AlpacaClient<'_>,
    trade: &mut Trade,
    position_prices: &HashMap<String, f64>,
    positions: &[AccountPositionState],
) -> Result<bool> {
    if !(trade.entry_order_id.is_some()
        || trade.stop_order_id.is_some()
        || trade.exit_order_id.is_some())
    {
        return Ok(false);
    }

    let entry_eval = if let Some(order_id) = trade.entry_order_id.as_deref() {
        client.evaluate_order(order_id).await?
    } else {
        None
    };
    let stop_eval = if let Some(order_id) = trade.stop_order_id.as_deref() {
        client.evaluate_order(order_id).await?
    } else {
        None
    };
    let exit_eval = if let Some(order_id) = trade.exit_order_id.as_deref() {
        client.evaluate_order(order_id).await?
    } else {
        None
    };

    if entry_order_ready_for_cancellation(trade, &entry_eval) {
        if let Some(order_id) = trade
            .entry_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if client.cancel_order(order_id).await? {
                info!(
                    "Cancelled pending entry order {} for trade {} on strategy {}",
                    order_id, trade.id, trade.strategy_id
                );
                apply_cancellation(trade, Utc::now());
                return Ok(true);
            }
        }
    }

    if let Some(eval) = stop_eval
        .as_ref()
        .filter(|evaluation| matches!(evaluation.state, OrderState::Filled))
    {
        apply_closure(trade, eval, true);
        return Ok(true);
    }

    if let Some(eval) = exit_eval
        .as_ref()
        .filter(|evaluation| matches!(evaluation.state, OrderState::Filled))
    {
        apply_closure(trade, eval, false);
        return Ok(true);
    }

    let mut changed = false;

    if let Some(eval) = entry_eval
        .as_ref()
        .filter(|evaluation| matches!(evaluation.state, OrderState::Filled))
    {
        let changed_at = eval.changed_at();
        if trade.status == TradeStatus::Pending {
            trade.set_status(TradeStatus::Active, changed_at);
            changed = true;
        }
        if let Some(price) = eval.filled_price {
            if trade.price != price {
                trade.set_price(price, changed_at);
                changed = true;
            }
        }
        let filled_date = normalize_trade_date(changed_at);
        if trade.date != filled_date {
            trade.set_date(filled_date, changed_at);
            changed = true;
        }
    }

    let position_match = find_position_match(trade, positions);
    if trade.status == TradeStatus::Pending
        && (entry_eval.is_none()
            || entry_eval
                .as_ref()
                .map(|evaluation| matches!(evaluation.state, OrderState::Cancelled))
                .unwrap_or(false))
    {
        if let Some(position) = position_match {
            let changed_at = Utc::now();
            trade.set_status(TradeStatus::Active, changed_at);
            if position.avg_entry_price.is_finite()
                && position.avg_entry_price > 0.0
                && (trade.price - position.avg_entry_price).abs() > PNL_EPSILON
            {
                trade.set_price(position.avg_entry_price, changed_at);
            }
            if trade.ticker != position.ticker {
                trade.set_ticker(position.ticker.clone(), changed_at);
            }
            changed = true;
        }
    }

    if let Some(position) = position_match {
        if trade.ticker != position.ticker {
            trade.set_ticker(position.ticker.clone(), Utc::now());
            changed = true;
        }
    }

    if stop_eval
        .as_ref()
        .map(|evaluation| matches!(evaluation.state, OrderState::Cancelled))
        .unwrap_or(false)
    {
        if position_match.is_some() && trade.stop_order_id.is_some() {
            trade.set_stop_order_id(None, Utc::now());
            changed = true;
        }
    }

    if should_cancel_trade(
        trade,
        &entry_eval,
        &stop_eval,
        &exit_eval,
        position_match.is_some(),
    ) {
        apply_cancellation(trade, Utc::now());
        return Ok(true);
    }

    if update_mark_to_market_pnl(trade, position_prices) {
        changed = true;
    }

    Ok(changed)
}

fn apply_closure(trade: &mut Trade, evaluation: &OrderEvaluation, is_stop: bool) {
    let changed_at = evaluation.changed_at();
    trade.set_status(TradeStatus::Closed, changed_at);
    if let Some(price) = evaluation.filled_price {
        trade.set_exit_price(Some(price), changed_at);
    }
    trade.set_exit_date(Some(changed_at), changed_at);
    trade.set_stop_loss_triggered(Some(is_stop), changed_at);
    if let Some(exit_price) = trade.exit_price {
        let pnl = (exit_price - trade.price) * trade.quantity as f64;
        trade.set_pnl(Some(pnl), changed_at);
    }
}

fn apply_cancellation(trade: &mut Trade, changed_at: DateTime<Utc>) {
    trade.set_status(TradeStatus::Cancelled, changed_at);
    trade.set_exit_price(None, changed_at);
    trade.set_exit_date(None, changed_at);
    trade.set_stop_loss_triggered(Some(false), changed_at);
    trade.set_pnl(None, changed_at);
}

async fn fetch_last_candle_closes(
    db: &Database,
    trades: &[Trade],
    positions: &[AccountPositionState],
) -> Result<HashMap<String, f64>> {
    let mut tickers = HashSet::new();
    for trade in trades {
        tickers.insert(trade.ticker.clone());
    }
    for position in positions {
        tickers.insert(position.ticker.clone());
    }
    if tickers.is_empty() {
        return Ok(HashMap::new());
    }

    let mut symbol_list: Vec<String> = tickers.into_iter().collect();
    symbol_list.sort();

    let candles = db.get_candles_for_tickers(&symbol_list).await?;
    let mut latest = HashMap::new();
    for candle in candles {
        let ticker = candle.ticker.clone();
        let should_replace = latest
            .get(&ticker)
            .map(|(date, _)| candle.date > *date)
            .unwrap_or(true);
        if should_replace {
            latest.insert(ticker, (candle.date, candle.close));
        }
    }

    Ok(latest
        .into_iter()
        .map(|(ticker, (_, close))| (ticker, close))
        .collect())
}

fn find_position_match<'a>(
    trade: &Trade,
    positions: &'a [AccountPositionState],
) -> Option<&'a AccountPositionState> {
    if positions.is_empty() {
        return None;
    }

    let trade_qty = trade.quantity;

    let exact_match = positions
        .iter()
        .find(|position| position.quantity == trade_qty && position.ticker == trade.ticker);
    if exact_match.is_some() {
        return exact_match;
    }

    let mut candidates: Vec<&AccountPositionState> = positions
        .iter()
        .filter(|position| position.quantity == trade_qty)
        .collect();
    if candidates.is_empty() {
        return None;
    }

    candidates.retain(|position| prices_close(position.avg_entry_price, trade.price));
    if candidates.len() == 1 {
        return Some(candidates[0]);
    }

    None
}

fn prices_close(a: f64, b: f64) -> bool {
    if !a.is_finite() || !b.is_finite() || a <= 0.0 || b <= 0.0 {
        return false;
    }
    let magnitude = a.abs().max(b.abs());
    let abs_tolerance = if magnitude >= 1.0 { 0.02 } else { 0.002 };
    let rel_tolerance = 0.02 * magnitude;
    (a - b).abs() <= abs_tolerance || (a - b).abs() <= rel_tolerance
}

fn normalize_trade_date(date: DateTime<Utc>) -> DateTime<Utc> {
    date.date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight should always be valid")
        .and_utc()
}

fn update_mark_to_market_pnl(
    trade: &mut Trade,
    last_close_by_ticker: &HashMap<String, f64>,
) -> bool {
    if !matches!(trade.status, TradeStatus::Pending | TradeStatus::Active) {
        return false;
    }

    let Some(current_price) = last_close_by_ticker.get(&trade.ticker) else {
        return false;
    };

    let pnl = (current_price - trade.price) * trade.quantity as f64;
    if trade
        .pnl
        .map(|existing| (existing - pnl).abs() > PNL_EPSILON)
        .unwrap_or(true)
    {
        trade.set_pnl(Some(pnl), Utc::now());
        return true;
    }

    false
}

fn should_cancel_trade(
    trade: &Trade,
    entry: &Option<OrderEvaluation>,
    stop_order: &Option<OrderEvaluation>,
    exit_order: &Option<OrderEvaluation>,
    has_position_match: bool,
) -> bool {
    if trade.status == TradeStatus::Pending {
        if let Some(evaluation) = entry.as_ref() {
            if matches!(evaluation.state, OrderState::Cancelled) && !has_position_match {
                return true;
            }
        }
    }

    if matches!(trade.status, TradeStatus::Pending | TradeStatus::Active) {
        if let Some(stop_eval) = stop_order.as_ref() {
            if matches!(stop_eval.state, OrderState::Cancelled) && !has_position_match {
                let exit_missing = trade.exit_order_id.is_none();
                let exit_cancelled = exit_order
                    .as_ref()
                    .map(|evaluation| matches!(evaluation.state, OrderState::Cancelled))
                    .unwrap_or(false);
                if exit_missing || exit_cancelled {
                    return true;
                }
            }
        }
    }

    false
}

fn entry_order_ready_for_cancellation(trade: &Trade, entry: &Option<OrderEvaluation>) -> bool {
    if trade.status != TradeStatus::Pending {
        return false;
    }
    let Some(cancel_deadline) = trade.entry_cancel_after else {
        return false;
    };
    if Utc::now() < cancel_deadline {
        return false;
    }
    entry
        .as_ref()
        .map(|evaluation| matches!(evaluation.state, OrderState::Pending))
        .unwrap_or(false)
}
