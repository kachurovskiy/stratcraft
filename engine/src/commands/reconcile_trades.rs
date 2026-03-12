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

        let trades_snapshot = trades.clone();
        for mut trade in trades {
            match reconcile_trade(
                &client,
                &mut trade,
                &position_prices,
                &positions,
                &trades_snapshot,
            )
            .await
            {
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
    trades: &[Trade],
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

    let mut changed = false;
    if let Some(eval) = entry_eval.as_ref() {
        if update_order_status(trade, TradeOrderKind::Entry, eval) {
            changed = true;
        }
    }
    if let Some(eval) = stop_eval.as_ref() {
        if update_order_status(trade, TradeOrderKind::Stop, eval) {
            changed = true;
        }
    }
    if let Some(eval) = exit_eval.as_ref() {
        if update_order_status(trade, TradeOrderKind::Exit, eval) {
            changed = true;
        }
    }

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

    if let Some(ticker) = detect_renamed_ticker(
        trade,
        positions,
        &entry_eval,
        &stop_eval,
        &exit_eval,
        trades,
    ) {
        trade.set_ticker(ticker, Utc::now());
        changed = true;
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
            let filled_date = normalize_trade_date(changed_at);
            if trade.date != filled_date {
                trade.set_date(filled_date, changed_at);
            }
            changed = true;
        }
    }

    if let Some(position) = position_match {
        if trade.status == TradeStatus::Active
            && position.avg_entry_price.is_finite()
            && position.avg_entry_price > 0.0
            && (trade.price - position.avg_entry_price).abs() > PNL_EPSILON
        {
            trade.set_price(position.avg_entry_price, Utc::now());
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
    if trade.status == TradeStatus::Pending {
        if let Some(cancel_after) = trade.entry_cancel_after {
            let expected_date = normalize_trade_date(cancel_after);
            if trade.date != expected_date {
                trade.set_date(expected_date, changed_at);
            }
        }
    }
    trade.set_status(TradeStatus::Cancelled, changed_at);
    trade.set_exit_price(None, changed_at);
    trade.set_exit_date(None, changed_at);
    trade.set_stop_loss_triggered(Some(false), changed_at);
    trade.set_pnl(None, changed_at);
    mark_order_cancelled(trade, TradeOrderKind::Entry, changed_at);
    mark_order_cancelled(trade, TradeOrderKind::Stop, changed_at);
    mark_order_cancelled(trade, TradeOrderKind::Exit, changed_at);
}

enum TradeOrderKind {
    Entry,
    Stop,
    Exit,
}

fn mark_order_cancelled(trade: &mut Trade, kind: TradeOrderKind, changed_at: DateTime<Utc>) {
    let (order_id, status) = match kind {
        TradeOrderKind::Entry => (
            trade.entry_order_id.clone(),
            trade.entry_order_status.clone(),
        ),
        TradeOrderKind::Stop => (trade.stop_order_id.clone(), trade.stop_order_status.clone()),
        TradeOrderKind::Exit => (trade.exit_order_id.clone(), trade.exit_order_status.clone()),
    };

    let status_ref = status.as_deref();
    let should_mark = should_mark_order_cancelled(order_id.as_deref(), status_ref);
    let should_backfill_timestamp = !should_mark && is_cancelled_status(status_ref);

    if should_mark {
        match kind {
            TradeOrderKind::Entry => {
                trade.set_entry_order_status(Some("cancelled".to_string()), changed_at);
                trade.entry_order_status_updated_at = Some(changed_at);
            }
            TradeOrderKind::Stop => {
                trade.set_stop_order_status(Some("cancelled".to_string()), changed_at);
                trade.stop_order_status_updated_at = Some(changed_at);
            }
            TradeOrderKind::Exit => {
                trade.set_exit_order_status(Some("cancelled".to_string()), changed_at);
                trade.exit_order_status_updated_at = Some(changed_at);
            }
        }
        return;
    }

    if should_backfill_timestamp {
        match kind {
            TradeOrderKind::Entry => {
                if trade.entry_order_status_updated_at.is_none() {
                    trade.entry_order_status_updated_at = Some(changed_at);
                }
            }
            TradeOrderKind::Stop => {
                if trade.stop_order_status_updated_at.is_none() {
                    trade.stop_order_status_updated_at = Some(changed_at);
                }
            }
            TradeOrderKind::Exit => {
                if trade.exit_order_status_updated_at.is_none() {
                    trade.exit_order_status_updated_at = Some(changed_at);
                }
            }
        }
    }
}

fn should_mark_order_cancelled(order_id: Option<&str>, status: Option<&str>) -> bool {
    let has_order_id = order_id
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    if !has_order_id {
        return false;
    }
    if is_filled_status(status) || is_cancelled_status(status) {
        return false;
    }
    true
}

fn is_filled_status(value: Option<&str>) -> bool {
    matches!(normalize_order_status(value).as_deref(), Some("filled"))
}

fn is_cancelled_status(value: Option<&str>) -> bool {
    matches!(
        normalize_order_status(value).as_deref(),
        Some("cancelled" | "canceled")
    )
}

fn normalize_order_status(value: Option<&str>) -> Option<String> {
    value
        .map(|status| status.trim().to_lowercase())
        .filter(|status| !status.is_empty())
}

fn update_order_status(
    trade: &mut Trade,
    kind: TradeOrderKind,
    evaluation: &OrderEvaluation,
) -> bool {
    let status_label = order_state_label(evaluation.state);
    let status_value = Some(status_label.to_string());
    let changed_at = evaluation.changed_at();
    let status_updated_at = evaluation.timestamp.or_else(|| {
        if matches!(evaluation.state, OrderState::Filled | OrderState::Cancelled) {
            Some(changed_at)
        } else {
            None
        }
    });

    let mut changed = false;

    match kind {
        TradeOrderKind::Entry => {
            if trade.entry_order_status.as_deref() != Some(status_label) {
                trade.set_entry_order_status(status_value, changed_at);
                changed = true;
            }
            if trade.entry_order_status_updated_at != status_updated_at {
                trade.entry_order_status_updated_at = status_updated_at;
                changed = true;
            }
        }
        TradeOrderKind::Stop => {
            if trade.stop_order_status.as_deref() != Some(status_label) {
                trade.set_stop_order_status(status_value, changed_at);
                changed = true;
            }
            if trade.stop_order_status_updated_at != status_updated_at {
                trade.stop_order_status_updated_at = status_updated_at;
                changed = true;
            }
        }
        TradeOrderKind::Exit => {
            if trade.exit_order_status.as_deref() != Some(status_label) {
                trade.set_exit_order_status(status_value, changed_at);
                changed = true;
            }
            if trade.exit_order_status_updated_at != status_updated_at {
                trade.exit_order_status_updated_at = status_updated_at;
                changed = true;
            }
        }
    }

    changed
}

fn order_state_label(state: OrderState) -> &'static str {
    match state {
        OrderState::Pending => "pending",
        OrderState::Filled => "filled",
        OrderState::Cancelled => "cancelled",
    }
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
    positions
        .iter()
        .find(|position| position.quantity == trade.quantity && position.ticker == trade.ticker)
}

fn detect_renamed_ticker(
    trade: &Trade,
    positions: &[AccountPositionState],
    entry_eval: &Option<OrderEvaluation>,
    stop_eval: &Option<OrderEvaluation>,
    exit_eval: &Option<OrderEvaluation>,
    trades: &[Trade],
) -> Option<String> {
    if trade.status != TradeStatus::Active {
        return None;
    }

    if find_position_match(trade, positions).is_some() {
        return None;
    }

    if let Some(symbol) = resolve_renamed_order_symbol(trade, &[entry_eval, stop_eval, exit_eval]) {
        return Some(symbol);
    }

    let position = find_renamed_position_match(trade, positions)?;
    if has_trade_shape_collision(trade, &position.ticker, trades) {
        return None;
    }
    Some(position.ticker.clone())
}

fn resolve_renamed_order_symbol(
    trade: &Trade,
    evaluations: &[&Option<OrderEvaluation>],
) -> Option<String> {
    let mut symbols = evaluations
        .iter()
        .filter_map(|evaluation| evaluation.as_ref())
        .filter_map(|evaluation| evaluation.symbol.as_deref())
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(str::to_uppercase)
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();

    match symbols.as_slice() {
        [symbol] if symbol != &trade.ticker => Some(symbol.clone()),
        _ => None,
    }
}

fn find_renamed_position_match<'a>(
    trade: &Trade,
    positions: &'a [AccountPositionState],
) -> Option<&'a AccountPositionState> {
    if trade.status != TradeStatus::Active {
        return None;
    }

    if find_position_match(trade, positions).is_some() {
        return None;
    }

    let mut candidates: Vec<&AccountPositionState> = positions
        .iter()
        .filter(|position| position.quantity == trade.quantity)
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

fn has_trade_shape_collision(trade: &Trade, ticker: &str, trades: &[Trade]) -> bool {
    trades.iter().any(|other| {
        other.id != trade.id
            && matches!(other.status, TradeStatus::Pending | TradeStatus::Active)
            && other.ticker == ticker
            && other.quantity == trade.quantity
            && prices_close(other.price, trade.price)
    })
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
    if trade.status != TradeStatus::Active {
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_trade(ticker: &str, quantity: i32, price: f64) -> Trade {
        Trade {
            id: format!("trade-{}", ticker),
            strategy_id: "strategy".to_string(),
            ticker: ticker.to_string(),
            quantity,
            price,
            date: Utc
                .with_ymd_and_hms(2024, 1, 2, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            status: TradeStatus::Pending,
            pnl: None,
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss: None,
            stop_loss_triggered: Some(false),
            entry_order_id: Some("entry-order".to_string()),
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            entry_order_status: None,
            entry_order_status_updated_at: None,
            stop_order_status: None,
            stop_order_status_updated_at: None,
            exit_order_status: None,
            exit_order_status_updated_at: None,
            changes: Vec::new(),
        }
    }

    fn sample_active_trade(ticker: &str, quantity: i32, price: f64) -> Trade {
        let mut trade = sample_trade(ticker, quantity, price);
        trade.status = TradeStatus::Active;
        trade
    }

    fn sample_eval(symbol: &str) -> Option<OrderEvaluation> {
        Some(OrderEvaluation {
            state: OrderState::Pending,
            filled_price: None,
            symbol: Some(symbol.to_string()),
            timestamp: None,
        })
    }

    #[test]
    fn find_position_match_requires_same_ticker() {
        let trade = sample_trade("GPRO", 697, 1.04);
        let positions = vec![AccountPositionState {
            ticker: "UPXI".to_string(),
            quantity: 697,
            avg_entry_price: 1.04,
            current_price: Some(1.08),
        }];

        assert!(find_position_match(&trade, &positions).is_none());
    }

    #[test]
    fn find_position_match_keeps_same_ticker_fill_detection() {
        let trade = sample_trade("UPXI", 697, 1.04);
        let positions = vec![
            AccountPositionState {
                ticker: "GPRO".to_string(),
                quantity: 697,
                avg_entry_price: 1.04,
                current_price: Some(1.08),
            },
            AccountPositionState {
                ticker: "UPXI".to_string(),
                quantity: 697,
                avg_entry_price: 1.04,
                current_price: Some(1.08),
            },
        ];

        let matched = find_position_match(&trade, &positions).expect("expected UPXI match");
        assert_eq!(matched.ticker, "UPXI");
    }

    #[test]
    fn detect_renamed_ticker_does_not_rename_pending_trade_from_position_shape() {
        let trade = sample_trade("GPRO", 697, 1.04);
        let positions = vec![AccountPositionState {
            ticker: "UPXI".to_string(),
            quantity: 697,
            avg_entry_price: 1.04,
            current_price: Some(1.08),
        }];

        assert_eq!(
            detect_renamed_ticker(&trade, &positions, &None, &None, &None, &[]),
            None
        );
    }

    #[test]
    fn detect_renamed_ticker_uses_order_symbol_for_active_trade() {
        let trade = sample_active_trade("FB", 10, 200.0);
        let positions = vec![AccountPositionState {
            ticker: "META".to_string(),
            quantity: 10,
            avg_entry_price: 200.0,
            current_price: Some(250.0),
        }];

        assert_eq!(
            detect_renamed_ticker(&trade, &positions, &None, &sample_eval("META"), &None, &[]),
            Some("META".to_string())
        );
    }

    #[test]
    fn detect_renamed_ticker_falls_back_to_unique_position_for_active_trade() {
        let trade = sample_active_trade("FB", 10, 200.0);
        let positions = vec![AccountPositionState {
            ticker: "META".to_string(),
            quantity: 10,
            avg_entry_price: 200.01,
            current_price: Some(250.0),
        }];

        assert_eq!(
            detect_renamed_ticker(&trade, &positions, &None, &None, &None, &[]),
            Some("META".to_string())
        );
    }

    #[test]
    fn detect_renamed_ticker_skips_when_shape_collision_exists() {
        let trade = sample_active_trade("CDXS", 120, 3.5);
        let positions = vec![AccountPositionState {
            ticker: "HUMA".to_string(),
            quantity: 120,
            avg_entry_price: 3.5,
            current_price: Some(3.6),
        }];
        let other_trade = sample_active_trade("HUMA", 120, 3.5);
        let trades = vec![trade.clone(), other_trade];

        assert_eq!(
            detect_renamed_ticker(&trade, &positions, &None, &None, &None, &trades),
            None
        );
    }

    #[test]
    fn update_mark_to_market_pnl_skips_pending_trades() {
        let mut trade = sample_trade("AAA", 10, 100.0);
        let prices = HashMap::from([(String::from("AAA"), 102.5)]);

        let changed = update_mark_to_market_pnl(&mut trade, &prices);

        assert!(!changed);
        assert_eq!(trade.pnl, None);
    }

    #[test]
    fn apply_cancellation_updates_date_from_entry_cancel_after() {
        let mut trade = sample_trade("AAA", 10, 100.0);
        trade.entry_cancel_after = Some(
            Utc.with_ymd_and_hms(2026, 3, 9, 20, 0, 0)
                .single()
                .expect("valid cancel-after"),
        );

        let changed_at = Utc
            .with_ymd_and_hms(2026, 3, 10, 5, 0, 0)
            .single()
            .expect("valid timestamp");
        apply_cancellation(&mut trade, changed_at);

        assert_eq!(
            trade.date,
            Utc.with_ymd_and_hms(2026, 3, 9, 0, 0, 0)
                .single()
                .expect("valid date")
        );
        assert_eq!(trade.status, TradeStatus::Cancelled);
    }
}
