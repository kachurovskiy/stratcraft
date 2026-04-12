use crate::alpaca::{AlpacaClient, OrderEvaluation, OrderState};
use crate::config::{EngineConfig, StopLossConfig};
use crate::context::AppContext;
use crate::database::{Database, TradeCorporateActionRecord, TradeReconciliationCandidate};
use crate::engine::AccountPositionState;
use crate::models::{Trade, TradeCancellationSource, TradeStatus};
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

    let mut grouped: HashMap<String, Vec<TradeReconciliationCandidate>> = HashMap::new();
    for candidate in candidates {
        grouped
            .entry(candidate.account_id.clone())
            .or_default()
            .push(candidate);
    }

    let mut reconciled = 0usize;
    let mut skipped = 0usize;

    for (account_id, mut account_candidates) in grouped {
        let trade_count = account_candidates.len();
        let Some(credentials) = db.get_account_credentials(&account_id).await? else {
            warn!(
                "Skipping {} trade(s) for account {} without credentials",
                trade_count, account_id
            );
            skipped += trade_count;
            continue;
        };

        if !credentials.provider.eq_ignore_ascii_case("alpaca") {
            warn!(
                "Skipping {} trade(s) for unsupported provider {} on account {}",
                trade_count, credentials.provider, account_id
            );
            skipped += trade_count;
            continue;
        }

        let client = match AlpacaClient::new(&http_client, &credentials, &settings) {
            Ok(client) => client,
            Err(err) => {
                warn!(
                    "Skipping {} trade(s) for account {}: Alpaca client init failed: {}",
                    trade_count, account_id, err
                );
                skipped += trade_count;
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
        let trades: Vec<Trade> = account_candidates
            .iter()
            .map(|candidate| candidate.trade.clone())
            .collect();

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

        let stop_loss_configs = match fetch_stop_loss_configs(&db, &trades).await {
            Ok(configs) => configs,
            Err(err) => {
                warn!(
                    "Failed to load stop loss configs for account {}: {}",
                    account_id, err
                );
                HashMap::new()
            }
        };

        let apply_manual_corporate_actions =
            credentials.environment.trim().eq_ignore_ascii_case("paper");

        for candidate in account_candidates.iter_mut() {
            let trade = &mut candidate.trade;
            match reconcile_trade(
                &db,
                &client,
                trade,
                &mut candidate.applied_corporate_actions,
                &position_prices,
                &positions,
                &stop_loss_configs,
                apply_manual_corporate_actions,
            )
            .await
            {
                Ok(true) => {
                    db.ensure_ticker_exists(&trade.ticker).await?;
                    db.persist_trade_reconciliation(trade, &candidate.applied_corporate_actions)
                        .await?;
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
    db: &Database,
    client: &AlpacaClient<'_>,
    trade: &mut Trade,
    applied_corporate_actions: &mut Vec<String>,
    position_prices: &HashMap<String, f64>,
    positions: &[AccountPositionState],
    stop_loss_configs: &HashMap<String, Option<StopLossConfig>>,
    apply_manual_corporate_actions: bool,
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
    let stop_loss_config = stop_loss_configs
        .get(trade.strategy_id.as_str())
        .and_then(|config| config.as_ref());

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
                apply_cancellation(trade, Utc::now(), TradeCancellationSource::Expiry);
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
                update_stop_loss_for_fill(trade, price, changed_at, stop_loss_config);
                changed = true;
            }
        }
        let filled_date = normalize_trade_date(changed_at);
        if trade.date != filled_date {
            trade.set_date(filled_date, changed_at);
            changed = true;
        }
    }

    let initial_position_match = find_position_match(trade, positions);
    if trade.status == TradeStatus::Pending
        && (entry_eval.is_none()
            || entry_eval
                .as_ref()
                .map(|evaluation| matches!(evaluation.state, OrderState::Cancelled))
                .unwrap_or(false))
    {
        if let Some(position) = initial_position_match {
            let changed_at = Utc::now();
            trade.set_status(TradeStatus::Active, changed_at);
            if position.avg_entry_price.is_finite()
                && position.avg_entry_price > 0.0
                && (trade.price - position.avg_entry_price).abs() > PNL_EPSILON
            {
                trade.set_price(position.avg_entry_price, changed_at);
                update_stop_loss_for_fill(
                    trade,
                    position.avg_entry_price,
                    changed_at,
                    stop_loss_config,
                );
            }
            let filled_date = normalize_trade_date(changed_at);
            if trade.date != filled_date {
                trade.set_date(filled_date, changed_at);
            }
            changed = true;
        }
    }

    let symbol_chain = resolve_ticker_chain_from_corporate_actions(db, trade).await?;
    if let Some(ticker) = symbol_chain
        .last()
        .filter(|ticker| **ticker != trade.ticker)
    {
        trade.set_ticker(ticker.clone(), Utc::now());
        changed = true;
    }

    if apply_manual_corporate_actions && trade.status == TradeStatus::Active {
        let trade_corporate_actions = db
            .get_trade_corporate_actions(&symbol_chain, trade.date)
            .await?;
        if apply_trade_corporate_actions(trade, applied_corporate_actions, &trade_corporate_actions)
        {
            changed = true;
        }
        if trade.status == TradeStatus::Closed {
            return Ok(true);
        }
    }

    let position_match = find_position_match(trade, positions);
    let has_synthetic_position =
        apply_manual_corporate_actions && !applied_corporate_actions.is_empty();
    let has_position_match =
        position_match.is_some() || initial_position_match.is_some() || has_synthetic_position;

    if let Some(position) = position_match {
        if trade.status == TradeStatus::Active
            && position.avg_entry_price.is_finite()
            && position.avg_entry_price > 0.0
            && (trade.price - position.avg_entry_price).abs() > PNL_EPSILON
        {
            let changed_at = Utc::now();
            trade.set_price(position.avg_entry_price, changed_at);
            update_stop_loss_for_fill(
                trade,
                position.avg_entry_price,
                changed_at,
                stop_loss_config,
            );
            changed = true;
        }
    }

    if stop_eval
        .as_ref()
        .map(|evaluation| matches!(evaluation.state, OrderState::Cancelled))
        .unwrap_or(false)
    {
        if has_position_match && trade.stop_order_id.is_some() {
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
        has_position_match,
    ) {
        apply_cancellation(trade, Utc::now(), TradeCancellationSource::Exchange);
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

fn apply_cancellation(
    trade: &mut Trade,
    changed_at: DateTime<Utc>,
    source: TradeCancellationSource,
) {
    if trade.status == TradeStatus::Pending {
        if let Some(cancel_after) = trade.entry_cancel_after {
            let expected_date = normalize_trade_date(cancel_after);
            if trade.date != expected_date {
                trade.set_date(expected_date, changed_at);
            }
        }
    }
    trade.set_cancellation_source(Some(source), changed_at);
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

async fn resolve_ticker_chain_from_corporate_actions(
    db: &Database,
    trade: &Trade,
) -> Result<Vec<String>> {
    if trade.status != TradeStatus::Active {
        return Ok(vec![trade.ticker.clone()]);
    }

    let mut visited = HashSet::new();
    let mut chain = vec![trade.ticker.clone()];
    let mut current_symbol = trade.ticker.clone();

    while visited.insert(current_symbol.clone()) {
        let Some(next_symbol) = db
            .find_name_change_successor(&current_symbol, trade.date)
            .await?
        else {
            break;
        };
        if next_symbol == current_symbol {
            break;
        }
        chain.push(next_symbol.clone());
        current_symbol = next_symbol;
    }

    Ok(chain)
}

fn apply_trade_corporate_actions(
    trade: &mut Trade,
    applied_corporate_actions: &mut Vec<String>,
    actions: &[TradeCorporateActionRecord],
) -> bool {
    let mut applied_any = false;

    for action in actions {
        if applied_corporate_actions
            .iter()
            .any(|existing| existing == &action.action_id)
        {
            continue;
        }

        let action_changed = apply_trade_corporate_action(trade, action);
        applied_corporate_actions.push(action.action_id.clone());
        applied_any = true;
        if action_changed {
            info!(
                "Applied paper {} corporate action {} to trade {}",
                action.action_type, action.action_id, trade.id
            );
        }

        if trade.status == TradeStatus::Closed {
            break;
        }
    }

    applied_any
}

fn apply_trade_corporate_action(trade: &mut Trade, action: &TradeCorporateActionRecord) -> bool {
    let is_quantity_adjustment = matches!(
        action.action_type.as_str(),
        "stock_dividend" | "forward_split" | "reverse_split" | "unit_split"
    );
    let ratio = if is_quantity_adjustment {
        if !action.old_rate.is_finite()
            || !action.new_rate.is_finite()
            || action.old_rate <= 0.0
            || action.new_rate <= 0.0
        {
            return false;
        }
        action.new_rate / action.old_rate
    } else {
        1.0
    };
    if !ratio.is_finite() || ratio <= 0.0 {
        return false;
    }

    let old_abs_quantity = trade.quantity.abs();
    if old_abs_quantity == 0 {
        return false;
    }

    let exact_new_abs_quantity = old_abs_quantity as f64 * ratio;
    let new_abs_quantity = round_corporate_action_quantity(exact_new_abs_quantity);
    let cash_per_old_share = if action.cash.is_finite() {
        action.cash
    } else {
        0.0
    };
    let changed_at = normalize_trade_date(action.effective_at);

    if new_abs_quantity == 0 {
        let cash_total = old_abs_quantity as f64 * cash_per_old_share;
        if !cash_total.is_finite() || cash_total <= 0.0 {
            return false;
        }
        let exit_price = cash_total / old_abs_quantity as f64;
        trade.set_status(TradeStatus::Closed, changed_at);
        trade.set_exit_price(Some(exit_price), changed_at);
        trade.set_exit_date(Some(changed_at), changed_at);
        trade.set_stop_loss_triggered(Some(false), changed_at);
        trade.set_pnl(
            Some((exit_price - trade.price) * trade.quantity as f64),
            changed_at,
        );
        return true;
    }

    let remaining_basis =
        (old_abs_quantity as f64 * trade.price) - (old_abs_quantity as f64 * cash_per_old_share);
    let new_signed_quantity = if trade.quantity < 0 {
        -new_abs_quantity
    } else {
        new_abs_quantity
    };
    let mut changed = false;

    if new_signed_quantity != trade.quantity {
        trade.set_quantity(new_signed_quantity, changed_at);
        changed = true;
    }

    let new_price = remaining_basis / new_abs_quantity as f64;
    if new_price.is_finite() && (trade.price - new_price).abs() > PNL_EPSILON {
        trade.set_price(new_price, changed_at);
        changed = true;
    }

    if is_quantity_adjustment {
        if let Some(stop_loss) = trade.stop_loss {
            let new_stop_loss = stop_loss / ratio;
            if new_stop_loss.is_finite()
                && new_stop_loss > 0.0
                && (stop_loss - new_stop_loss).abs() > PNL_EPSILON
            {
                trade.set_stop_loss(Some(new_stop_loss), changed_at);
                changed = true;
            }
        }
    }

    changed
}

fn round_corporate_action_quantity(quantity: f64) -> i32 {
    if !quantity.is_finite() || quantity <= 0.0 {
        return 0;
    }

    let rounded = quantity.round();
    if (quantity - rounded).abs() <= PNL_EPSILON {
        rounded as i32
    } else {
        quantity.floor() as i32
    }
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

fn update_stop_loss_for_fill(
    trade: &mut Trade,
    fill_price: f64,
    changed_at: DateTime<Utc>,
    stop_loss_config: Option<&StopLossConfig>,
) -> bool {
    let Some(config) = stop_loss_config else {
        return false;
    };
    if config.mode != 0 {
        return false;
    }
    if trade.stop_loss.is_none() {
        return false;
    }
    if !fill_price.is_finite() || fill_price <= 0.0 {
        return false;
    }
    if !config.ratio.is_finite() || config.ratio <= 0.0 || config.ratio >= 1.0 {
        return false;
    }

    let is_short = trade.quantity < 0;
    let new_stop = if is_short {
        fill_price * (1.0 + config.ratio)
    } else {
        fill_price * (1.0 - config.ratio)
    };
    if !new_stop.is_finite() || new_stop <= 0.0 {
        return false;
    }

    if trade
        .stop_loss
        .map(|value| (value - new_stop).abs() > PNL_EPSILON)
        .unwrap_or(true)
    {
        trade.set_stop_loss(Some(new_stop), changed_at);
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

async fn fetch_stop_loss_configs(
    db: &Database,
    trades: &[Trade],
) -> Result<HashMap<String, Option<StopLossConfig>>> {
    let mut configs = HashMap::new();
    for trade in trades {
        if configs.contains_key(&trade.strategy_id) {
            continue;
        }
        let config = match db.get_strategy_config(&trade.strategy_id).await? {
            Some(strategy) => Some(EngineConfig::from_parameters(&strategy.parameters).stop_loss),
            None => {
                warn!(
                    "Missing strategy config for trade {} on strategy {}",
                    trade.id, trade.strategy_id
                );
                None
            }
        };
        configs.insert(trade.strategy_id.clone(), config);
    }
    Ok(configs)
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
            cancellation_source: None,
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

    fn sample_corporate_action(
        action_type: &str,
        action_id: &str,
        effective_at: DateTime<Utc>,
        cash: f64,
        old_rate: f64,
        new_rate: f64,
    ) -> TradeCorporateActionRecord {
        TradeCorporateActionRecord {
            action_id: action_id.to_string(),
            action_type: action_type.to_string(),
            effective_at,
            cash,
            old_rate,
            new_rate,
        }
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
    fn apply_trade_corporate_actions_updates_reverse_split_quantity_price_and_stop() {
        let mut trade = sample_trade("AAA", 25, 10.0);
        trade.status = TradeStatus::Active;
        trade.stop_loss = Some(8.0);
        let mut applied = Vec::new();
        let actions = vec![sample_corporate_action(
            "reverse_split",
            "ca-1",
            Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            2.0,
            10.0,
            1.0,
        )];

        let changed = apply_trade_corporate_actions(&mut trade, &mut applied, &actions);

        assert!(changed);
        assert_eq!(trade.quantity, 2);
        assert!((trade.price - 100.0).abs() < 1e-6);
        assert_eq!(trade.stop_loss, Some(80.0));
        assert_eq!(applied, vec![String::from("ca-1")]);
    }

    #[test]
    fn apply_trade_corporate_actions_adjusts_cash_dividend_only_once() {
        let mut trade = sample_trade("AAA", 10, 100.0);
        trade.status = TradeStatus::Active;
        let mut applied = Vec::new();
        let actions = vec![sample_corporate_action(
            "cash_dividend",
            "cash-1",
            Utc.with_ymd_and_hms(2024, 2, 5, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            1.0,
            1.0,
            1.0,
        )];

        let first_changed = apply_trade_corporate_actions(&mut trade, &mut applied, &actions);
        let second_changed = apply_trade_corporate_actions(&mut trade, &mut applied, &actions);

        assert!(first_changed);
        assert!(!second_changed);
        assert!((trade.price - 99.0).abs() < 1e-6);
        assert_eq!(applied, vec![String::from("cash-1")]);
    }

    #[test]
    fn apply_trade_corporate_actions_closes_fully_cashed_out_reverse_split() {
        let mut trade = sample_trade("AAA", 5, 10.0);
        trade.status = TradeStatus::Active;
        let mut applied = Vec::new();
        let actions = vec![sample_corporate_action(
            "reverse_split",
            "cashout-1",
            Utc.with_ymd_and_hms(2024, 2, 9, 0, 0, 0)
                .single()
                .expect("valid timestamp"),
            2.0,
            10.0,
            1.0,
        )];

        let changed = apply_trade_corporate_actions(&mut trade, &mut applied, &actions);

        assert!(changed);
        assert_eq!(trade.status, TradeStatus::Closed);
        assert_eq!(trade.exit_price, Some(2.0));
        assert_eq!(
            trade.exit_date,
            Some(
                Utc.with_ymd_and_hms(2024, 2, 9, 0, 0, 0)
                    .single()
                    .expect("valid timestamp")
            )
        );
        assert_eq!(applied, vec![String::from("cashout-1")]);
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
        apply_cancellation(&mut trade, changed_at, TradeCancellationSource::Expiry);

        assert_eq!(
            trade.date,
            Utc.with_ymd_and_hms(2026, 3, 9, 0, 0, 0)
                .single()
                .expect("valid date")
        );
        assert_eq!(trade.status, TradeStatus::Cancelled);
        assert_eq!(
            trade.cancellation_source,
            Some(TradeCancellationSource::Expiry)
        );
    }

    #[test]
    fn update_stop_loss_for_fill_updates_percent_stop() {
        let mut trade = sample_trade("AAA", 10, 100.0);
        trade.stop_loss = Some(95.0);
        let config = StopLossConfig {
            mode: 0,
            ratio: 0.05,
            atr_period: 14,
            atr_multiplier: 2.0,
        };
        let changed_at = Utc
            .with_ymd_and_hms(2026, 3, 11, 0, 0, 0)
            .single()
            .expect("valid timestamp");

        let updated = update_stop_loss_for_fill(&mut trade, 90.0, changed_at, Some(&config));

        assert!(updated);
        assert!((trade.stop_loss.unwrap() - 85.5).abs() < 1e-6);
    }
}
