use crate::candle_utils::{
    group_candles_by_ticker_with, group_candles_for_tickers, normalize_ticker_symbol,
};
use crate::config::{EngineConfig, EngineRuntimeSettings};
use crate::indicators::estimate_annualized_volatility_from_candles;
use crate::models::*;
use crate::param_utils::coerce_binary_param;
use crate::performance::PerformanceCalculator;
use crate::signals::{
    generate_signal_with_filters, maybe_create_generated_signal, SignalGenerationParams,
};
use crate::strategy::Strategy;
use crate::trading_rules::{
    compute_trailing_stop, determine_position_size, has_minimum_dollar_volume, initial_stop_loss,
    stop_loss_exit_price, PositionSizingOutcome, PositionSizingParams, TrailingStopParams,
    PRICE_EPSILON,
};
use anyhow::{anyhow, ensure, Result};
use chrono::{DateTime, Duration, Utc};
use log::warn;
use std::collections::{hash_map::DefaultHasher, HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use uuid::Uuid;

const PNL_EPSILON: f64 = 1e-6;
const SECONDS_PER_YEAR: f64 = 365.0 * 24.0 * 60.0 * 60.0;

#[derive(Debug, PartialEq, Eq)]
enum EntrySignalOutcome {
    Executed,
    Skipped {
        reason: &'static str,
        details: Option<String>,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum SellSignalOutcome {
    Executed { closed_count: usize },
    Skipped { reason: &'static str },
}

struct SignalDecision {
    action: SignalAction,
    confidence: f64,
}

struct BacktestLoopResult {
    cash: f64,
    active_trades: Vec<Trade>,
    closed_trades: Vec<Trade>,
    daily_snapshots: Vec<BacktestDataPoint>,
    generated_signals: Vec<GeneratedSignal>,
    signal_skips: Vec<AccountSignalSkip>,
}

struct BacktestResumeState {
    loop_start_index: usize,
    cash: f64,
    active_trades: Vec<Trade>,
    closed_trades: Vec<Trade>,
    daily_snapshots: Vec<BacktestDataPoint>,
    generated_signals: Vec<GeneratedSignal>,
    max_portfolio_value: f64,
    start_date: DateTime<Utc>,
}

pub struct PlannedOperations {
    pub operations: Vec<AccountOperationPlan>,
    pub notes: Vec<String>,
    pub skipped_signals: Vec<AccountSignalSkip>,
}

#[derive(Debug, Clone)]
pub struct AccountStateSnapshot {
    pub available_cash: f64,
    pub buying_power: Option<f64>,
    pub held_tickers: HashSet<String>,
    pub open_buy_orders: HashSet<String>,
    pub open_sell_orders: HashSet<String>,
    pub positions: Vec<AccountPositionState>,
    pub stop_orders: HashMap<String, Vec<AccountStopOrderState>>,
}

#[derive(Debug, Clone)]
pub struct AccountPositionState {
    pub ticker: String,
    pub quantity: i32,
    pub avg_entry_price: f64,
    pub current_price: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct AccountStopOrderState {
    pub quantity: i32,
    pub stop_price: f64,
    pub side: String,
}

pub struct Engine {
    pub config: EngineConfig,
    runtime_settings: EngineRuntimeSettings,
    ticker_expense_map: Arc<HashMap<String, f64>>,
}

impl Engine {
    #[allow(dead_code)]
    pub fn new(runtime_settings: EngineRuntimeSettings) -> Self {
        Self {
            config: EngineConfig::default(),
            runtime_settings,
            ticker_expense_map: Arc::new(HashMap::new()),
        }
    }

    // Construct an Engine configured from a parameter map.
    pub fn from_parameters(
        parameters: &HashMap<String, f64>,
        runtime_settings: EngineRuntimeSettings,
    ) -> Self {
        Self {
            config: EngineConfig::from_parameters(parameters),
            runtime_settings,
            ticker_expense_map: Arc::new(HashMap::new()),
        }
    }

    pub fn set_runtime_settings(&mut self, settings: EngineRuntimeSettings) {
        self.runtime_settings = settings;
    }

    pub fn set_ticker_expense_map(&mut self, expense_map: Arc<HashMap<String, f64>>) {
        self.ticker_expense_map = expense_map;
    }

    fn expense_ratio_for(&self, ticker: &str) -> f64 {
        if let Some(value) = self.ticker_expense_map.get(ticker) {
            *value
        } else {
            let upper = ticker.to_ascii_uppercase();
            self.ticker_expense_map.get(&upper).copied().unwrap_or(0.0)
        }
    }

    fn resolve_trading_start_index(
        unique_dates: &[DateTime<Utc>],
        requested_start: DateTime<Utc>,
    ) -> usize {
        if unique_dates.is_empty() {
            return 0;
        }

        let last_index = unique_dates.len() - 1;
        match unique_dates.binary_search(&requested_start) {
            Ok(idx) => idx.min(last_index),
            Err(0) => 0,
            Err(idx) => (idx - 1).min(last_index),
        }
    }

    pub fn backtest(
        &self,
        strategy: Option<&dyn Strategy>,
        strategy_id: &str,
        tickers: &[String],
        all_candles: &[Candle],
        unique_dates: &[DateTime<Utc>],
        provided_signals: Option<&[GeneratedSignal]>,
        start_date_override: Option<DateTime<Utc>>,
        existing_backtest: Option<&BacktestResult>,
    ) -> Result<BacktestRun> {
        if unique_dates.is_empty() {
            return Err(anyhow!("unique_dates cannot be empty"));
        }

        let mut tickers_for_run: Vec<String> = tickers.to_vec();
        if provided_signals.is_none() {
            if let Some(strategy_ref) = strategy {
                if let Some(target) = strategy_ref.target_ticker() {
                    if let Some(existing) = tickers
                        .iter()
                        .find(|candidate| candidate.eq_ignore_ascii_case(&target))
                    {
                        tickers_for_run = vec![existing.clone()];
                    } else {
                        tickers_for_run = vec![target];
                    }
                }
            }
        }

        if let (Some(strategy_ref), Some(existing)) = (strategy, existing_backtest) {
            if let Some(state) = existing.strategy_state.as_ref() {
                if state.template_id == strategy_ref.get_template_id() {
                    if let Err(err) = strategy_ref.restore_state(&state.data) {
                        warn!(
                            "Failed to restore state for strategy {}: {}",
                            state.template_id, err
                        );
                    }
                }
            }
        }

        let candles_by_ticker = group_candles_for_tickers(&tickers_for_run, all_candles);
        if let Some(strategy_ref) = strategy {
            if strategy_ref.get_template_id().starts_with("lightgbm") {
                crate::strategy::lightgbm::prime_cross_sectional_context_from_ref_map(
                    &candles_by_ticker,
                );
            }
        }
        let mut resume_state = if let Some(existing) = existing_backtest {
            self.prepare_resume_state(existing, unique_dates)?
        } else {
            None
        };
        let resume_start_date = resume_state.as_ref().map(|state| state.start_date);
        let loop_start_index = resume_state
            .as_ref()
            .map(|state| state.loop_start_index)
            .unwrap_or(0);

        let (loop_result, start_date, template_id) = if let Some(signals) = provided_signals {
            let trading_start_index = start_date_override
                .map(|target| Self::resolve_trading_start_index(unique_dates, target))
                .unwrap_or(0);
            let start_date = resume_start_date.unwrap_or(unique_dates[trading_start_index]);
            let mut signal_map: HashMap<(DateTime<Utc>, String), &GeneratedSignal> = HashMap::new();
            for signal in signals
                .iter()
                .filter(|signal| matches!(signal.action, SignalAction::Buy | SignalAction::Sell))
            {
                signal_map.insert((signal.date, signal.ticker.clone()), signal);
            }

            let loop_result = self.run_backtest_loop(
                &tickers_for_run,
                unique_dates,
                &candles_by_ticker,
                trading_start_index,
                loop_start_index,
                |ticker, _index, current_date, _ticker_candles| {
                    signal_map
                        .get(&(current_date, ticker.clone()))
                        .map(|signal| SignalDecision {
                            action: signal.action.clone(),
                            confidence: signal.confidence.unwrap_or(0.0),
                        })
                },
                resume_state.take(),
                true,
            );

            (loop_result, start_date, strategy_id.to_string())
        } else if let Some(strategy) = strategy {
            let min_data_points = strategy.get_min_data_points();
            let default_start_index =
                std::cmp::min(min_data_points, unique_dates.len().saturating_sub(1));
            let trading_start_index = start_date_override
                .map(|target| Self::resolve_trading_start_index(unique_dates, target))
                .unwrap_or(default_start_index);
            let start_date = resume_start_date.unwrap_or(unique_dates[trading_start_index]);

            // Excluded tickers are deployment-time settings, not optimization parameters
            let empty_excluded: HashSet<String> = HashSet::new();

            let loop_result = self.run_backtest_loop(
                &tickers_for_run,
                unique_dates,
                &candles_by_ticker,
                trading_start_index,
                loop_start_index,
                |ticker, index, current_date, ticker_candles| {
                    // Convert to owned slice for the shared function
                    let candles_slice: Vec<Candle> =
                        ticker_candles.iter().map(|c| (**c).clone()).collect();

                    // Use the shared signal generation function with optimization parameters
                    if let Some(generated_signal) =
                        generate_signal_with_filters(SignalGenerationParams {
                            strategy,
                            ticker,
                            candles: &candles_slice,
                            candle_index: index,
                            date: current_date,
                            excluded_tickers: &empty_excluded, // No ticker exclusions during optimization
                        })
                    {
                        Some(SignalDecision {
                            action: generated_signal.action,
                            confidence: generated_signal.confidence.unwrap_or(0.0),
                        })
                    } else {
                        None
                    }
                },
                resume_state.take(),
                false,
            );

            (
                loop_result,
                start_date,
                strategy.get_template_id().to_string(),
            )
        } else {
            return Err(anyhow!(
                "A strategy or precomputed signals must be provided for backtesting"
            ));
        };

        let BacktestLoopResult {
            mut cash,
            mut active_trades,
            closed_trades,
            daily_snapshots,
            generated_signals: loop_generated_signals,
            signal_skips,
        } = loop_result;

        let mut generated_signals = loop_generated_signals;
        if let Some(signals) = provided_signals {
            generated_signals = signals.to_vec();
        }

        let final_date = unique_dates
            .last()
            .cloned()
            .expect("unique_dates is not empty");

        self.remove_future_dated_trades(&mut active_trades, &mut cash, final_date);

        let positions_value = self.calculate_positions_value(&active_trades);
        let final_portfolio_value = cash + positions_value;

        let mut trades = closed_trades;
        trades.extend(active_trades);

        self.validate_trades(&trades, &candles_by_ticker, final_date)?;

        // Use the actual first snapshot date as the start_date to ensure consistency
        let actual_start_date = daily_snapshots
            .first()
            .map(|snapshot| snapshot.date)
            .unwrap_or(start_date);

        let performance = PerformanceCalculator::calculate_performance(
            &trades,
            self.config.initial_capital,
            final_portfolio_value,
            actual_start_date,
            final_date,
            &daily_snapshots,
        );

        let strategy_state = strategy.and_then(|strategy_ref| {
            strategy_ref
                .snapshot_state()
                .map(|data| StrategyStateSnapshot {
                    template_id: strategy_ref.get_template_id().to_string(),
                    data,
                })
        });

        let result = BacktestResult {
            id: Uuid::new_v4().to_string(),
            strategy_id: template_id,
            start_date: actual_start_date,
            end_date: final_date,
            initial_capital: self.config.initial_capital,
            final_portfolio_value,
            performance,
            daily_snapshots,
            trades,
            tickers: tickers_for_run.clone(),
            ticker_scope: None,
            strategy_state,
            created_at: Utc::now(),
        };

        Ok(BacktestRun {
            result,
            signals: generated_signals,
            signal_skips,
        })
    }

    fn run_backtest_loop<'a, F>(
        &self,
        tickers: &[String],
        unique_dates: &[DateTime<Utc>],
        candles_by_ticker: &HashMap<String, Vec<&'a Candle>>,
        trading_start_index: usize,
        loop_start_index: usize,
        mut signal_provider: F,
        resume_state: Option<BacktestResumeState>,
        track_signal_skips: bool,
    ) -> BacktestLoopResult
    where
        F: FnMut(&String, usize, DateTime<Utc>, &Vec<&'a Candle>) -> Option<SignalDecision>,
    {
        let mut active_trades;
        let mut closed_trades;
        let mut daily_snapshots;
        let mut generated_signals;
        let mut signal_skips: Vec<AccountSignalSkip> = Vec::new();
        let mut cash;
        let mut max_portfolio_value;
        let mut ticker_cursors: HashMap<&String, usize> =
            tickers.iter().map(|ticker| (ticker, 0)).collect();

        if let Some(state) = resume_state {
            active_trades = state.active_trades;
            closed_trades = state.closed_trades;
            daily_snapshots = state.daily_snapshots;
            generated_signals = state.generated_signals;
            cash = state.cash;
            max_portfolio_value = state.max_portfolio_value;
        } else {
            active_trades = Vec::new();
            closed_trades = Vec::new();
            daily_snapshots = Vec::new();
            generated_signals = Vec::new();
            cash = self.config.initial_capital;
            max_portfolio_value = self.config.initial_capital;
        }
        for (date_index, &current_date) in unique_dates.iter().enumerate().skip(loop_start_index) {
            let mut missed_trades_due_to_cash_today = 0;

            self.update_active_trades(
                &mut active_trades,
                &mut closed_trades,
                &mut cash,
                candles_by_ticker,
                current_date,
            );

            // Only create snapshots and check trading signals once we've reached trading_start_index
            if date_index >= trading_start_index {
                let ordered_tickers = Self::ordered_tickers_for_date(tickers, current_date);
                for ticker in ordered_tickers {
                    if let Some(ticker_candles) = candles_by_ticker.get(ticker) {
                        let cursor = ticker_cursors
                            .get_mut(ticker)
                            .expect("ticker cursor missing");
                        while *cursor < ticker_candles.len()
                            && ticker_candles[*cursor].date < current_date
                        {
                            *cursor += 1;
                        }
                        if *cursor < ticker_candles.len()
                            && ticker_candles[*cursor].date == current_date
                        {
                            let index = *cursor;
                            if let Some(signal) =
                                signal_provider(ticker, index, current_date, ticker_candles)
                            {
                                let SignalDecision { action, confidence } = signal;

                                if let Some(generated) = maybe_create_generated_signal(
                                    current_date,
                                    ticker.as_str(),
                                    &action,
                                    confidence,
                                ) {
                                    generated_signals.push(generated);
                                }

                                match action {
                                    SignalAction::Buy => {
                                        let next_candle = ticker_candles.get(index + 1).copied();
                                        if self.config.allow_short_selling {
                                            self.close_short_positions(
                                                &mut active_trades,
                                                &mut closed_trades,
                                                &mut cash,
                                                ticker,
                                                next_candle,
                                            );
                                        }
                                        let outcome = self.execute_buy_signal(
                                            &mut active_trades,
                                            &mut cash,
                                            ticker,
                                            ticker_candles[index],
                                            next_candle,
                                            ticker_candles,
                                            index,
                                            confidence,
                                        );
                                        if let EntrySignalOutcome::Skipped { reason, details } =
                                            outcome
                                        {
                                            if reason == "insufficient_cash" {
                                                missed_trades_due_to_cash_today += 1;
                                            }
                                            if track_signal_skips {
                                                signal_skips.push(AccountSignalSkip {
                                                    ticker: ticker.clone(),
                                                    signal_date: current_date,
                                                    action: SignalAction::Buy,
                                                    reason: reason.to_string(),
                                                    details,
                                                });
                                            }
                                        }
                                    }
                                    SignalAction::Sell => {
                                        let sell_outcome = self.execute_sell_signal(
                                            &mut active_trades,
                                            &mut closed_trades,
                                            &mut cash,
                                            ticker,
                                            ticker_candles[index],
                                            confidence,
                                        );
                                        let sell_executed = match &sell_outcome {
                                            SellSignalOutcome::Executed { closed_count } => {
                                                *closed_count > 0
                                            }
                                            _ => false,
                                        };
                                        let mut short_outcome = None;
                                        if self.config.allow_short_selling
                                            && !Self::has_active_long_position(
                                                &active_trades,
                                                ticker,
                                            )
                                        {
                                            let outcome = self.execute_short_entry(
                                                &mut active_trades,
                                                &mut cash,
                                                ticker,
                                                ticker_candles[index],
                                                ticker_candles.get(index + 1).copied(),
                                                ticker_candles,
                                                index,
                                                confidence,
                                            );
                                            if let EntrySignalOutcome::Skipped { reason, .. } =
                                                &outcome
                                            {
                                                if *reason == "insufficient_cash" {
                                                    missed_trades_due_to_cash_today += 1;
                                                }
                                            }
                                            short_outcome = Some(outcome);
                                        }

                                        let acted = sell_executed
                                            || matches!(
                                                short_outcome.as_ref(),
                                                Some(EntrySignalOutcome::Executed)
                                            );
                                        if !acted && track_signal_skips {
                                            let reason_details = match short_outcome {
                                                Some(EntrySignalOutcome::Skipped {
                                                    reason,
                                                    details,
                                                }) => Some((reason, details)),
                                                _ => match sell_outcome {
                                                    SellSignalOutcome::Skipped { reason } => {
                                                        Some((reason, None))
                                                    }
                                                    _ => None,
                                                },
                                            };

                                            if let Some((reason, details)) = reason_details {
                                                signal_skips.push(AccountSignalSkip {
                                                    ticker: ticker.clone(),
                                                    signal_date: current_date,
                                                    action: SignalAction::Sell,
                                                    reason: reason.to_string(),
                                                    details,
                                                });
                                            }
                                        }
                                    }
                                    SignalAction::Hold => {}
                                }
                            }
                        }
                    }
                }
            }

            let mut positions_value = self.calculate_positions_value(&active_trades);
            let mut portfolio_value = cash + positions_value;

            if portfolio_value < 0.0 && !active_trades.is_empty() {
                warn!(
                    "Portfolio value {:.2} fell below zero on {}; initiating forced liquidation.",
                    portfolio_value, current_date
                );
                self.force_liquidation(
                    &mut active_trades,
                    &mut closed_trades,
                    &mut cash,
                    candles_by_ticker,
                    current_date,
                );
                positions_value = self.calculate_positions_value(&active_trades);
                portfolio_value = cash + positions_value;
            }

            if portfolio_value > max_portfolio_value {
                max_portfolio_value = portfolio_value;
            }

            let executed_active_count = active_trades.len() as i32;

            // Only record snapshots from trading_start_index onwards
            if date_index >= trading_start_index {
                daily_snapshots.push(BacktestDataPoint {
                    date: current_date,
                    portfolio_value,
                    cash,
                    positions_value,
                    concurrent_trades: executed_active_count,
                    missed_trades_due_to_cash: missed_trades_due_to_cash_today,
                });
            }
        }

        BacktestLoopResult {
            cash,
            active_trades,
            closed_trades,
            daily_snapshots,
            generated_signals,
            signal_skips,
        }
    }

    fn prepare_resume_state(
        &self,
        existing: &BacktestResult,
        unique_dates: &[DateTime<Utc>],
    ) -> Result<Option<BacktestResumeState>> {
        if unique_dates.is_empty() {
            return Ok(None);
        }
        let last_available = *unique_dates
            .last()
            .expect("Checked unique_dates is not empty");
        if existing.end_date >= last_available {
            return Ok(None);
        }

        let resume_from = existing.end_date + Duration::days(1);
        if resume_from > last_available {
            return Ok(None);
        }

        let mut loop_start_index = Self::resolve_trading_start_index(unique_dates, resume_from);
        while loop_start_index < unique_dates.len()
            && unique_dates[loop_start_index] <= existing.end_date
        {
            loop_start_index += 1;
        }
        if loop_start_index >= unique_dates.len() {
            return Ok(None);
        }

        let cash = existing
            .daily_snapshots
            .last()
            .map(|snapshot| snapshot.cash)
            .unwrap_or(self.config.initial_capital);
        let max_portfolio_value = self.get_max_portfolio_value(&existing.daily_snapshots);

        let closed_trades: Vec<Trade> = existing
            .trades
            .iter()
            .filter(|trade| trade.status != TradeStatus::Active)
            .cloned()
            .collect();
        let active_trades: Vec<Trade> = existing
            .trades
            .iter()
            .filter(|trade| trade.status == TradeStatus::Active)
            .cloned()
            .collect();

        Ok(Some(BacktestResumeState {
            loop_start_index,
            cash,
            active_trades,
            closed_trades,
            daily_snapshots: existing.daily_snapshots.clone(),
            generated_signals: Vec::new(),
            max_portfolio_value,
            start_date: existing.start_date,
        }))
    }

    fn get_max_portfolio_value(&self, snapshots: &[BacktestDataPoint]) -> f64 {
        let mut max_portfolio_value = self.config.initial_capital;

        for snapshot in snapshots {
            if snapshot.portfolio_value > max_portfolio_value {
                max_portfolio_value = snapshot.portfolio_value;
            }
        }

        max_portfolio_value
    }

    fn update_active_trades(
        &self,
        active_trades: &mut Vec<Trade>,
        closed_trades: &mut Vec<Trade>,
        cash: &mut f64,
        candles_by_ticker: &HashMap<String, Vec<&Candle>>,
        current_date: DateTime<Utc>,
    ) {
        let mut to_close = Vec::new();

        for (i, trade) in active_trades.iter_mut().enumerate() {
            if current_date < trade.date {
                // Trade has not reached its scheduled entry date yet; ignore until it does.
                continue;
            }
            if let Some(ticker_candles) = candles_by_ticker.get(&trade.ticker) {
                // Use the most recent candle on or before the current date
                let current_candle = ticker_candles
                    .iter()
                    .rev()
                    .find(|c| c.date <= current_date)
                    .copied();
                if let Some(current_candle) = current_candle {
                    let current_index = ticker_candles
                        .iter()
                        .position(|c| c.date == current_candle.date);
                    let current_price = current_candle.close;
                    let quantity = trade.quantity as f64;
                    trade.pnl = Some((current_price - trade.price) * quantity);

                    // Check for time-based exit
                    let days_held = (current_date - trade.date).num_days();
                    if days_held >= self.config.max_holding_days.into() {
                        let exit_price = self.apply_exit_slippage_with_candle(
                            current_price,
                            trade.quantity < 0,
                            current_candle,
                        );
                        trade.set_exit_price(Some(exit_price), current_date);
                        trade.set_exit_date(Some(current_date), current_date);
                        let fee = self.calculate_trade_close_fee(
                            trade.ticker.as_str(),
                            trade.quantity,
                            exit_price,
                            trade.date,
                            current_date,
                        );
                        trade.pnl = Some((exit_price - trade.price) * trade.quantity as f64 - fee);
                        trade.set_fee(Some(fee), current_date);
                        trade.set_status(TradeStatus::Closed, current_date);
                        to_close.push(i);
                        continue;
                    }

                    // Trailing stop update (ATR)
                    if let (Some(curr_stop), Some(idx)) = (trade.stop_loss, current_index) {
                        if trade.date < current_date {
                            if let Some(update) = compute_trailing_stop(TrailingStopParams {
                                stop_loss_mode: self.config.stop_loss.mode,
                                atr_multiplier: self.config.stop_loss.atr_multiplier,
                                atr_period: self.config.stop_loss.atr_period,
                                ticker_candles,
                                candle_index: idx,
                                current_candle,
                                current_stop: curr_stop,
                                is_short: trade.quantity < 0,
                                planning_close: None,
                            }) {
                                trade.set_stop_loss(Some(update.value()), current_date);
                            }
                        }
                    }

                    if let Some(stop_loss) = trade.stop_loss {
                        if let Some(raw_exit_price) =
                            stop_loss_exit_price(current_candle, stop_loss, trade.quantity < 0)
                        {
                            let exit_price = self.apply_exit_slippage_with_candle(
                                raw_exit_price,
                                trade.quantity < 0,
                                current_candle,
                            );
                            trade.set_exit_price(Some(exit_price), current_date);
                            trade.set_exit_date(Some(current_date), current_date);
                            let fee = self.calculate_trade_close_fee(
                                trade.ticker.as_str(),
                                trade.quantity,
                                exit_price,
                                trade.date,
                                current_date,
                            );
                            trade.pnl =
                                Some((exit_price - trade.price) * trade.quantity as f64 - fee);
                            trade.set_fee(Some(fee), current_date);
                            trade.set_status(TradeStatus::Closed, current_date);
                            trade.set_stop_loss_triggered(Some(true), current_date);
                            to_close.push(i);
                            continue;
                        }
                    }
                }
            }
        }

        // Close trades in reverse order to maintain indices
        for &i in to_close.iter().rev() {
            let trade = active_trades.remove(i);
            let exit_price = trade.exit_price.unwrap_or(0.0);
            let exit_date = trade.exit_date.unwrap_or(trade.date);
            let trade_value = exit_price * trade.quantity as f64;
            let fee = trade.fee.unwrap_or_else(|| {
                self.calculate_trade_close_fee(
                    trade.ticker.as_str(),
                    trade.quantity,
                    exit_price,
                    trade.date,
                    exit_date,
                )
            });
            *cash += trade_value - fee;
            closed_trades.push(trade);
        }
    }

    fn execute_buy_signal(
        &self,
        active_trades: &mut Vec<Trade>,
        cash: &mut f64,
        ticker: &str,
        candle: &Candle,
        next_candle_opt: Option<&Candle>,
        ticker_candles: &Vec<&Candle>,
        index: usize,
        confidence: f64,
    ) -> EntrySignalOutcome {
        let guard_price = match Self::guard_price_from_candle(candle) {
            Some(price) if self.entry_price_supported(price) => price,
            _ => {
                return EntrySignalOutcome::Skipped {
                    reason: "price_out_of_range",
                    details: None,
                }
            }
        };
        let Some(next_candle) = next_candle_opt else {
            return EntrySignalOutcome::Skipped {
                reason: "missing_next_candle",
                details: None,
            };
        };
        let Some(next_index) = index.checked_add(1) else {
            return EntrySignalOutcome::Skipped {
                reason: "missing_next_candle",
                details: None,
            };
        };
        if next_index >= ticker_candles.len() {
            return EntrySignalOutcome::Skipped {
                reason: "missing_next_candle",
                details: None,
            };
        }
        if !has_minimum_dollar_volume(
            ticker_candles,
            next_index,
            self.runtime_settings.minimum_dollar_volume_lookback,
            self.runtime_settings.minimum_dollar_volume_for_entry,
        ) {
            return EntrySignalOutcome::Skipped {
                reason: "insufficient_volume",
                details: None,
            };
        }
        let mut price = next_candle.open;
        let mut is_limit_entry = false;
        let trade_date = next_candle.date;

        if self.config.buy_discount_ratio > 0.0 {
            let discounted_price = candle.close * (1.0 - self.config.buy_discount_ratio);
            if next_candle.low <= discounted_price {
                price = next_candle.open.min(discounted_price);
                is_limit_entry = true;
            } else {
                return EntrySignalOutcome::Skipped {
                    reason: "discount_not_reached",
                    details: None,
                };
            }
        }
        if !is_limit_entry {
            price = self.apply_entry_slippage_with_candle(price, false, next_candle);
        }
        debug_assert!(self.entry_price_supported(guard_price));

        if active_trades
            .iter()
            .any(|t| t.ticker == ticker && t.date == trade_date)
        {
            return EntrySignalOutcome::Skipped {
                reason: "trade_already_open",
                details: None,
            };
        }

        let realized_vol = if (self.config.position_sizing.mode == 2
            || self.config.position_sizing.mode == 3)
            && self.config.position_sizing.vol_target_annual > 0.0
        {
            Some(estimate_annualized_volatility_from_candles(
                ticker_candles,
                index,
                self.config.position_sizing.vol_lookback,
            ))
        } else {
            None
        };

        let allocation = match determine_position_size(PositionSizingParams {
            price,
            available_cash: *cash,
            trade_size_ratio: self.config.trade_size_ratio,
            minimum_trade_size: self.config.minimum_trade_size,
            position_sizing_mode: self.config.position_sizing.mode,
            confidence,
            vol_target_annual: self.config.position_sizing.vol_target_annual,
            realized_vol,
        }) {
            PositionSizingOutcome::Sized(allocation) => allocation,
            PositionSizingOutcome::TooSmall => {
                return EntrySignalOutcome::Skipped {
                    reason: "insufficient_size",
                    details: None,
                }
            }
            PositionSizingOutcome::InsufficientCash { required } => {
                return EntrySignalOutcome::Skipped {
                    reason: "insufficient_cash",
                    details: Some(format!("need {:.2}, have {:.2}", required, *cash)),
                }
            }
        };

        *cash -= allocation.trade_value;

        let stop_loss = initial_stop_loss(
            self.config.stop_loss.mode,
            self.config.stop_loss.atr_multiplier,
            self.config.stop_loss.atr_period,
            self.config.stop_loss.ratio,
            price,
            ticker_candles,
            index,
            false,
        );

        let trade = Trade {
            id: Uuid::new_v4().to_string(),
            strategy_id: "backtest".to_string(),
            ticker: ticker.to_string(),
            quantity: allocation.quantity,
            price,
            date: trade_date,
            status: TradeStatus::Active,
            pnl: None,
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        };
        active_trades.push(trade);

        EntrySignalOutcome::Executed
    }

    fn execute_short_entry(
        &self,
        active_trades: &mut Vec<Trade>,
        cash: &mut f64,
        ticker: &str,
        candle: &Candle,
        next_candle_opt: Option<&Candle>,
        ticker_candles: &Vec<&Candle>,
        index: usize,
        confidence: f64,
    ) -> EntrySignalOutcome {
        let guard_price = match Self::guard_price_from_candle(candle) {
            Some(price) if self.entry_price_supported(price) => price,
            _ => {
                return EntrySignalOutcome::Skipped {
                    reason: "price_out_of_range",
                    details: None,
                }
            }
        };
        if !self.config.allow_short_selling {
            return EntrySignalOutcome::Skipped {
                reason: "short_selling_disabled",
                details: None,
            };
        }
        let Some(next_candle) = next_candle_opt else {
            return EntrySignalOutcome::Skipped {
                reason: "missing_next_candle",
                details: None,
            };
        };
        let Some(next_index) = index.checked_add(1) else {
            return EntrySignalOutcome::Skipped {
                reason: "missing_next_candle",
                details: None,
            };
        };
        if next_index >= ticker_candles.len() {
            return EntrySignalOutcome::Skipped {
                reason: "missing_next_candle",
                details: None,
            };
        }
        if !has_minimum_dollar_volume(
            ticker_candles,
            next_index,
            self.runtime_settings.minimum_dollar_volume_lookback,
            self.runtime_settings.minimum_dollar_volume_for_entry,
        ) {
            return EntrySignalOutcome::Skipped {
                reason: "insufficient_volume",
                details: None,
            };
        }
        let mut price = next_candle.open;
        let trade_date = next_candle.date;

        if Self::has_active_long_position(active_trades, ticker)
            || Self::has_active_short_position(active_trades, ticker)
        {
            return EntrySignalOutcome::Skipped {
                reason: "position_exists",
                details: None,
            };
        }

        if active_trades
            .iter()
            .any(|t| t.ticker == ticker && t.date == trade_date)
        {
            return EntrySignalOutcome::Skipped {
                reason: "trade_already_open",
                details: None,
            };
        }
        price = self.apply_entry_slippage_with_candle(price, true, next_candle);
        debug_assert!(self.entry_price_supported(guard_price));

        let realized_vol = if (self.config.position_sizing.mode == 2
            || self.config.position_sizing.mode == 3)
            && self.config.position_sizing.vol_target_annual > 0.0
        {
            Some(estimate_annualized_volatility_from_candles(
                ticker_candles,
                index,
                self.config.position_sizing.vol_lookback,
            ))
        } else {
            None
        };

        let allocation = match determine_position_size(PositionSizingParams {
            price,
            available_cash: *cash,
            trade_size_ratio: self.config.trade_size_ratio,
            minimum_trade_size: self.config.minimum_trade_size,
            position_sizing_mode: self.config.position_sizing.mode,
            confidence,
            vol_target_annual: self.config.position_sizing.vol_target_annual,
            realized_vol,
        }) {
            PositionSizingOutcome::Sized(allocation) => allocation,
            PositionSizingOutcome::TooSmall => {
                return EntrySignalOutcome::Skipped {
                    reason: "insufficient_size",
                    details: None,
                }
            }
            PositionSizingOutcome::InsufficientCash { required } => {
                return EntrySignalOutcome::Skipped {
                    reason: "insufficient_cash",
                    details: Some(format!("need {:.2}, have {:.2}", required, *cash)),
                }
            }
        };

        *cash += allocation.trade_value;

        let stop_loss = initial_stop_loss(
            self.config.stop_loss.mode,
            self.config.stop_loss.atr_multiplier,
            self.config.stop_loss.atr_period,
            self.config.stop_loss.ratio,
            price,
            ticker_candles,
            index,
            true,
        );

        let trade = Trade {
            id: Uuid::new_v4().to_string(),
            strategy_id: "backtest".to_string(),
            ticker: ticker.to_string(),
            quantity: -(allocation.quantity),
            price,
            date: trade_date,
            status: TradeStatus::Active,
            pnl: None,
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        };
        active_trades.push(trade);

        EntrySignalOutcome::Executed
    }

    fn execute_sell_signal(
        &self,
        active_trades: &mut Vec<Trade>,
        closed_trades: &mut Vec<Trade>,
        cash: &mut f64,
        ticker: &str,
        candle: &Candle,
        _confidence: f64,
    ) -> SellSignalOutcome {
        let fraction = coerce_binary_param(self.config.sell_fraction, 1.0);
        if fraction == 0.0 {
            return SellSignalOutcome::Skipped {
                reason: "sell_fraction_zero",
            };
        }

        let mut to_close = Vec::new();

        for (i, trade) in active_trades.iter_mut().enumerate() {
            if trade.ticker != ticker || trade.status != TradeStatus::Active {
                continue;
            }
            if trade.quantity <= 0 {
                continue;
            }
            if candle.date < trade.date {
                // Ignore trades whose entries have not occurred yet.
                continue;
            }

            if fraction >= 1.0 {
                let exit_price = self.apply_exit_slippage_with_candle(candle.close, false, candle);
                let exit_date = candle.date;
                let fee = self.calculate_trade_close_fee(
                    trade.ticker.as_str(),
                    trade.quantity,
                    exit_price,
                    trade.date,
                    exit_date,
                );
                let pnl = (exit_price - trade.price) * trade.quantity as f64 - fee;
                let trade_value = exit_price * trade.quantity as f64;

                trade.set_exit_price(Some(exit_price), exit_date);
                trade.set_exit_date(Some(exit_date), exit_date);
                trade.pnl = Some(pnl);
                trade.set_status(TradeStatus::Closed, exit_date);
                trade.set_fee(Some(fee), exit_date);

                *cash += trade_value - fee;
                to_close.push(i);
            }
        }

        // Close trades in reverse order to maintain indices
        for &i in to_close.iter().rev() {
            let trade = active_trades.remove(i);
            closed_trades.push(trade);
        }

        if to_close.is_empty() {
            SellSignalOutcome::Skipped {
                reason: "sell_no_active_position",
            }
        } else {
            SellSignalOutcome::Executed {
                closed_count: to_close.len(),
            }
        }
    }

    fn close_short_positions(
        &self,
        active_trades: &mut Vec<Trade>,
        closed_trades: &mut Vec<Trade>,
        cash: &mut f64,
        ticker: &str,
        execution_candle: Option<&Candle>,
    ) {
        let Some(candle) = execution_candle else {
            return;
        };
        let mut to_close = Vec::new();

        for (i, trade) in active_trades.iter_mut().enumerate() {
            if trade.ticker != ticker || trade.status != TradeStatus::Active {
                continue;
            }
            if trade.quantity >= 0 {
                continue;
            }
            if candle.date < trade.date {
                continue;
            }

            let exit_price = self.apply_exit_slippage_with_candle(candle.open, true, candle);
            let exit_date = candle.date;
            let fee = self.calculate_trade_close_fee(
                trade.ticker.as_str(),
                trade.quantity,
                exit_price,
                trade.date,
                exit_date,
            );
            let pnl = (exit_price - trade.price) * trade.quantity as f64 - fee;
            trade.set_exit_price(Some(exit_price), exit_date);
            trade.set_exit_date(Some(exit_date), exit_date);
            trade.pnl = Some(pnl);
            trade.set_status(TradeStatus::Closed, exit_date);
            trade.set_fee(Some(fee), exit_date);
            trade.set_stop_loss_triggered(Some(false), exit_date);

            let trade_value = exit_price * trade.quantity as f64;
            *cash += trade_value - fee;

            to_close.push(i);
        }

        for &i in to_close.iter().rev() {
            let trade = active_trades.remove(i);
            closed_trades.push(trade);
        }
    }

    fn has_active_long_position(active_trades: &[Trade], ticker: &str) -> bool {
        active_trades.iter().any(|trade| {
            trade.ticker == ticker && trade.status == TradeStatus::Active && trade.quantity > 0
        })
    }

    fn has_active_short_position(active_trades: &[Trade], ticker: &str) -> bool {
        active_trades.iter().any(|trade| {
            trade.ticker == ticker && trade.status == TradeStatus::Active && trade.quantity < 0
        })
    }

    fn calculate_positions_value(&self, active_trades: &[Trade]) -> f64 {
        active_trades
            .iter()
            .map(|trade| {
                let entry_value = trade.price * trade.quantity as f64;
                let pnl = trade.pnl.unwrap_or(0.0);
                entry_value + pnl
            })
            .sum()
    }

    fn force_liquidation(
        &self,
        active_trades: &mut Vec<Trade>,
        closed_trades: &mut Vec<Trade>,
        cash: &mut f64,
        candles_by_ticker: &HashMap<String, Vec<&Candle>>,
        current_date: DateTime<Utc>,
    ) {
        if active_trades.is_empty() {
            return;
        }

        let mut to_close = Vec::new();

        for (i, trade) in active_trades.iter_mut().enumerate() {
            if trade.status != TradeStatus::Active {
                continue;
            }
            if current_date < trade.date {
                continue;
            }

            let exit_candle = candles_by_ticker
                .get(&trade.ticker)
                .and_then(|candles| candles.iter().rev().find(|c| c.date <= current_date))
                .copied();
            let exit_price_raw = exit_candle.map(|c| c.close).unwrap_or(trade.price);
            let exit_price = if let Some(exit_candle) = exit_candle {
                self.apply_exit_slippage_with_candle(
                    exit_price_raw,
                    trade.quantity < 0,
                    exit_candle,
                )
            } else {
                self.apply_exit_slippage(exit_price_raw, trade.quantity < 0)
            };
            let exit_date = current_date;
            let fee = self.calculate_trade_close_fee(
                trade.ticker.as_str(),
                trade.quantity,
                exit_price,
                trade.date,
                exit_date,
            );
            let pnl = (exit_price - trade.price) * trade.quantity as f64 - fee;

            trade.set_exit_price(Some(exit_price), exit_date);
            trade.set_exit_date(Some(exit_date), exit_date);
            trade.pnl = Some(pnl);
            trade.set_status(TradeStatus::Closed, exit_date);
            trade.set_fee(Some(fee), exit_date);
            trade.set_stop_loss_triggered(Some(false), exit_date);

            let trade_value = exit_price * trade.quantity as f64;
            *cash += trade_value - fee;

            to_close.push(i);
        }

        for &idx in to_close.iter().rev() {
            let trade = active_trades.remove(idx);
            closed_trades.push(trade);
        }
    }

    // Cancel trades whose entry dates fall after the final mark date (e.g., when an early
    // stop terminates the loop before their execution) and refund their reserved capital.
    fn remove_future_dated_trades(
        &self,
        active_trades: &mut Vec<Trade>,
        cash: &mut f64,
        cutoff_date: DateTime<Utc>,
    ) {
        let mut index = 0;
        while index < active_trades.len() {
            if active_trades[index].date > cutoff_date {
                let trade = active_trades.remove(index);
                *cash += trade.price * trade.quantity as f64;
            } else {
                index += 1;
            }
        }
    }

    fn calculate_trade_close_fee(
        &self,
        ticker: &str,
        quantity: i32,
        exit_price: f64,
        entry_date: DateTime<Utc>,
        exit_date: DateTime<Utc>,
    ) -> f64 {
        if quantity == 0 || exit_price <= 0.0 || !exit_price.is_finite() {
            return 0.0;
        }

        let notional = exit_price * (quantity as f64).abs();
        if notional <= 0.0 || !notional.is_finite() {
            return 0.0;
        }

        let mut fee = notional * self.runtime_settings.trade_close_fee_rate;
        let holding_seconds = exit_date
            .signed_duration_since(entry_date)
            .num_seconds()
            .max(0) as f64;
        let years_held = if holding_seconds > 0.0 {
            holding_seconds / SECONDS_PER_YEAR
        } else {
            0.0
        };

        if quantity < 0 && years_held.is_finite() && years_held > 0.0 {
            fee += notional * self.runtime_settings.short_borrow_fee_annual_rate * years_held;
        }

        if quantity > 0 {
            let expense_ratio = self.expense_ratio_for(ticker);
            if expense_ratio.is_finite() && expense_ratio > 0.0 && years_held.is_finite() {
                fee += notional * expense_ratio * years_held.max(0.0);
            }
        }

        fee
    }

    fn apply_entry_slippage(&self, price: f64, is_short: bool) -> f64 {
        let slippage_rate = self.runtime_settings.trade_slippage_rate;
        if is_short {
            price * (1.0 - slippage_rate)
        } else {
            price * (1.0 + slippage_rate)
        }
    }

    fn apply_exit_slippage(&self, price: f64, is_short: bool) -> f64 {
        let slippage_rate = self.runtime_settings.trade_slippage_rate;
        if is_short {
            price * (1.0 + slippage_rate)
        } else {
            price * (1.0 - slippage_rate)
        }
    }

    fn apply_entry_slippage_with_candle(&self, price: f64, is_short: bool, candle: &Candle) -> f64 {
        let slipped = self.apply_entry_slippage(price, is_short);
        Self::clamp_price_to_candle_bounds(slipped, candle)
    }

    fn apply_exit_slippage_with_candle(&self, price: f64, is_short: bool, candle: &Candle) -> f64 {
        let slipped = self.apply_exit_slippage(price, is_short);
        Self::clamp_price_to_candle_bounds(slipped, candle)
    }

    fn clamp_price_to_candle_bounds(price: f64, candle: &Candle) -> f64 {
        if !price.is_finite() {
            return price;
        }
        let mut low = candle.low;
        let mut high = candle.high;
        if !low.is_finite() || !high.is_finite() {
            if let Some((min_price, max_price)) = Self::candle_price_bounds(candle) {
                low = min_price;
                high = max_price;
            } else {
                return price;
            }
        }
        let lower = low.min(high);
        let upper = low.max(high);
        if !lower.is_finite() || !upper.is_finite() {
            return price;
        }
        if price < lower {
            lower
        } else if price > upper {
            upper
        } else {
            price
        }
    }

    fn entry_price_supported(&self, price: f64) -> bool {
        price.is_finite()
            && price >= self.runtime_settings.trade_entry_price_min
            && price <= self.runtime_settings.trade_entry_price_max
    }

    fn guard_price_from_candle(candle: &Candle) -> Option<f64> {
        let price = candle.unadjusted_close.unwrap_or(candle.close);
        if price.is_finite() && price > 0.0 {
            Some(price)
        } else {
            None
        }
    }

    fn validate_trades(
        &self,
        trades: &[Trade],
        candles_by_ticker: &HashMap<String, Vec<&Candle>>,
        mark_date: DateTime<Utc>,
    ) -> Result<()> {
        for trade in trades {
            ensure!(trade.quantity != 0, "Trade {} has zero quantity", trade.id);

            ensure!(
                trade.price.is_finite(),
                "Trade {} entry price is not finite",
                trade.id
            );

            let ticker_candles = candles_by_ticker.get(&trade.ticker).ok_or_else(|| {
                anyhow!(
                    "Trade {} references ticker {} with no candle data",
                    trade.id,
                    trade.ticker
                )
            })?;

            let entry_candle = ticker_candles
                .iter()
                .find(|c| c.date == trade.date)
                .ok_or_else(|| {
                    anyhow!(
                        "Trade {} entry date {} missing candle for {}",
                        trade.id,
                        trade.date,
                        trade.ticker
                    )
                })?;
            let (entry_min, entry_max) =
                Self::candle_price_bounds(entry_candle).ok_or_else(|| {
                    anyhow!(
                        "Trade {} entry candle {} has invalid price data",
                        trade.id,
                        trade.date
                    )
                })?;

            ensure!(
                self.price_within_bounds(trade.price, entry_min, entry_max),
                "Trade {} entry price {:.4} outside {} range [{:.4}, {:.4}] on {}",
                trade.id,
                trade.price,
                trade.ticker,
                entry_min,
                entry_max,
                trade.date
            );

            match trade.status {
                TradeStatus::Pending | TradeStatus::Cancelled => {
                    ensure!(
                        trade.exit_price.is_none() && trade.exit_date.is_none(),
                        "Open trade {} unexpectedly has exit data",
                        trade.id
                    );
                }
                TradeStatus::Active => {
                    ensure!(
                        trade.exit_price.is_none() && trade.exit_date.is_none(),
                        "Active trade {} unexpectedly has exit data",
                        trade.id
                    );

                    let pnl = trade.pnl.ok_or_else(|| {
                        anyhow!("Active trade {} missing pnl mark-to-market", trade.id)
                    })?;
                    let mark_candle = ticker_candles
                        .iter()
                        .rev()
                        .find(|c| c.date <= mark_date)
                        .ok_or_else(|| {
                            anyhow!(
                                "Trade {} has no candle to mark position as of {}",
                                trade.id,
                                mark_date
                            )
                        })?;
                    ensure!(
                        mark_candle.date >= trade.date,
                        "Trade {} mark date predates entry",
                        trade.id
                    );

                    let expected_pnl = (mark_candle.close - trade.price) * trade.quantity as f64;
                    ensure!(
                        Self::pnl_within_reason(pnl, expected_pnl),
                        "Trade {} pnl {:.6} inconsistent with mark {:.6} (close {:.4}, entry {:.4}, qty {})",
                        trade.id,
                        pnl,
                        expected_pnl,
                        mark_candle.close,
                        trade.price,
                        trade.quantity
                    );
                }
                TradeStatus::Closed => {
                    let exit_date = trade
                        .exit_date
                        .ok_or_else(|| anyhow!("Closed trade {} is missing exit_date", trade.id))?;
                    ensure!(
                        exit_date >= trade.date,
                        "Trade {} exit date {} precedes entry {}",
                        trade.id,
                        exit_date,
                        trade.date
                    );

                    let exit_price = trade.exit_price.ok_or_else(|| {
                        anyhow!("Closed trade {} is missing exit_price", trade.id)
                    })?;
                    ensure!(
                        exit_price.is_finite(),
                        "Trade {} exit price is not finite",
                        trade.id
                    );

                    let exit_candle = ticker_candles
                        .iter()
                        .rev()
                        .find(|c| c.date <= exit_date)
                        .ok_or_else(|| {
                            anyhow!(
                                "Trade {} exit date {} has no candle at or before that date for {}",
                                trade.id,
                                exit_date,
                                trade.ticker
                            )
                        })?;

                    let (exit_min, exit_max) =
                        Self::candle_price_bounds(exit_candle).ok_or_else(|| {
                            anyhow!(
                                "Trade {} exit candle {} has invalid price data",
                                trade.id,
                                exit_candle.date
                            )
                        })?;

                    ensure!(
                        self.price_within_bounds(exit_price, exit_min, exit_max),
                        "Trade {} exit price {:.4} outside {} range [{:.4}, {:.4}] on/ before {}",
                        trade.id,
                        exit_price,
                        trade.ticker,
                        exit_min,
                        exit_max,
                        exit_date
                    );

                    let actual_pnl = trade
                        .pnl
                        .ok_or_else(|| anyhow!("Closed trade {} missing pnl", trade.id))?;
                    let fee = self.calculate_trade_close_fee(
                        trade.ticker.as_str(),
                        trade.quantity,
                        exit_price,
                        trade.date,
                        exit_date,
                    );
                    if let Some(recorded_fee) = trade.fee {
                        ensure!(
                            Self::pnl_within_reason(recorded_fee, fee),
                            "Trade {} fee {:.6} inconsistent with expected {:.6}",
                            trade.id,
                            recorded_fee,
                            fee
                        );
                    }
                    let expected_pnl = (exit_price - trade.price) * trade.quantity as f64 - fee;
                    ensure!(
                        Self::pnl_within_reason(actual_pnl, expected_pnl),
                        "Trade {} pnl {:.6} inconsistent with exit {:.6} (exit {:.4}, entry {:.4}, qty {})",
                        trade.id,
                        actual_pnl,
                        expected_pnl,
                        exit_price,
                        trade.price,
                        trade.quantity
                    );
                }
            }
        }

        Ok(())
    }

    fn candle_price_bounds(candle: &Candle) -> Option<(f64, f64)> {
        let mut min_price = f64::INFINITY;
        let mut max_price = f64::NEG_INFINITY;

        for value in [candle.open, candle.high, candle.low, candle.close] {
            if !value.is_finite() {
                continue;
            }
            if value < min_price {
                min_price = value;
            }
            if value > max_price {
                max_price = value;
            }
        }

        if min_price.is_infinite() || max_price.is_infinite() {
            None
        } else {
            Some((min_price, max_price))
        }
    }

    fn price_within_bounds(&self, price: f64, min_price: f64, max_price: f64) -> bool {
        if !price.is_finite() || !min_price.is_finite() || !max_price.is_finite() {
            return false;
        }
        let lower = min_price.min(max_price);
        let upper = max_price.max(min_price);
        let magnitude = lower.abs().max(upper.abs()).max(price.abs()).max(1.0);
        let tolerance = magnitude * self.runtime_settings.trade_slippage_rate + PRICE_EPSILON;
        price + tolerance >= lower && price <= upper + tolerance
    }

    fn pnl_within_reason(actual: f64, expected: f64) -> bool {
        if !actual.is_finite() || !expected.is_finite() {
            return false;
        }
        let tolerance = PNL_EPSILON * (1.0 + actual.abs().max(expected.abs()));
        (actual - expected).abs() <= tolerance
    }

    fn resolve_account_buying_power(&self, account_state: &AccountStateSnapshot) -> f64 {
        let cash = if account_state.available_cash.is_finite() {
            account_state.available_cash.max(0.0)
        } else {
            0.0
        };
        let leverage = if self.config.max_leverage.is_finite() && self.config.max_leverage >= 1.0 {
            self.config.max_leverage
        } else {
            1.0
        };
        let mut exposure = 0.0;
        let mut position_value = 0.0;
        for position in &account_state.positions {
            let price = position.current_price.unwrap_or(position.avg_entry_price);
            if !price.is_finite() || price <= 0.0 {
                continue;
            }
            let value = position.quantity as f64 * price;
            position_value += value;
            exposure += value.abs();
        }
        let equity = cash + position_value;
        let leverage_cap = if equity.is_finite() {
            equity.max(0.0) * leverage
        } else {
            0.0
        };
        let remaining_by_leverage = (leverage_cap - exposure).max(0.0);
        let buying_power = account_state
            .buying_power
            .filter(|value| value.is_finite() && *value >= 0.0);

        match buying_power {
            Some(bp) => bp.min(remaining_by_leverage),
            None => cash,
        }
    }

    pub fn effective_buying_power_for_account(&self, account_state: &AccountStateSnapshot) -> f64 {
        self.resolve_account_buying_power(account_state)
    }

    pub fn plan_account_operations(
        &self,
        strategy_id: &str,
        account_id: &str,
        signals: &[GeneratedSignal],
        candles: &[Candle],
        target_date: DateTime<Utc>,
        account_state: &AccountStateSnapshot,
        excluded_tickers: &HashSet<String>,
        existing_trades: &[Trade],
        existing_buy_operations_today: usize,
        ticker_metadata: &HashMap<String, TickerInfo>,
    ) -> PlannedOperations {
        let mut notes = Vec::new();
        let mut skipped_signals: Vec<AccountSignalSkip> = Vec::new();
        if candles.is_empty() {
            notes.push("no_candles_provided".to_string());
            return PlannedOperations {
                operations: Vec::new(),
                notes,
                skipped_signals,
            };
        }

        let candles_by_ticker = group_candles_by_ticker_with(candles, None, |candle| {
            normalize_ticker_symbol(candle.ticker.as_str())
        });
        if candles_by_ticker.is_empty() {
            notes.push("no_candles_for_tracked_tickers".to_string());
            return PlannedOperations {
                operations: Vec::new(),
                notes,
                skipped_signals,
            };
        }

        let mut available_cash = self.resolve_account_buying_power(account_state);
        if available_cash <= 0.0 {
            notes.push("account_cash_unavailable".to_string());
        }

        let mut operations = Vec::new();
        let mut record_skip =
            |ticker: &str, action: SignalAction, reason: &str, details: Option<String>| {
                skipped_signals.push(AccountSignalSkip {
                    ticker: ticker.to_string(),
                    signal_date: target_date,
                    action,
                    reason: reason.to_string(),
                    details,
                });
            };

        let mut latest_live_trade_dates: HashMap<String, DateTime<Utc>> = HashMap::new();
        for trade in existing_trades
            .iter()
            .filter(|trade| matches!(trade.status, TradeStatus::Pending | TradeStatus::Active))
        {
            let ticker = trade.ticker.trim().to_uppercase();
            if ticker.is_empty() {
                continue;
            }
            latest_live_trade_dates
                .entry(ticker)
                .and_modify(|existing| {
                    if trade.date > *existing {
                        *existing = trade.date;
                    }
                })
                .or_insert(trade.date);
        }

        let mut sell_signals: HashMap<String, &GeneratedSignal> = HashMap::new();
        for signal in signals.iter().filter(|signal| {
            matches!(signal.action, SignalAction::Sell) && signal.date == target_date
        }) {
            let ticker = signal.ticker.trim().to_uppercase();
            if ticker.is_empty() {
                notes.push("signal_missing_ticker".to_string());
                continue;
            }
            sell_signals.entry(ticker).or_insert(signal);
        }

        let mut actionable_signals: Vec<(u64, String, &GeneratedSignal)> = signals
            .iter()
            .filter(|signal| {
                matches!(signal.action, SignalAction::Buy) && signal.date == target_date
            })
            .map(|signal| {
                let ticker = signal.ticker.trim().to_uppercase();
                let hash = Self::ticker_date_hash(ticker.as_str(), target_date);
                (hash, ticker, signal)
            })
            .collect();
        actionable_signals.sort_by(|(hash_a, ticker_a, _), (hash_b, ticker_b, _)| {
            hash_a.cmp(hash_b).then_with(|| ticker_a.cmp(ticker_b))
        });
        let existing_buy_ops = existing_buy_operations_today > 0;
        if existing_buy_ops {
            notes.push("buy_operations_already_planned_for_day".to_string());
            for (_, ticker, _signal) in actionable_signals {
                if ticker.is_empty() {
                    notes.push("signal_missing_ticker".to_string());
                    continue;
                }
                record_skip(&ticker, SignalAction::Buy, "buy_ops_already_planned", None);
            }
        } else {
            for (_, ticker, signal) in actionable_signals {
                if ticker.is_empty() {
                    notes.push("signal_missing_ticker".to_string());
                    continue;
                }

                if excluded_tickers.contains(&ticker) {
                    notes.push(format!("signal_{}_excluded", ticker));
                    record_skip(&ticker, SignalAction::Buy, "signal_excluded", None);
                    continue;
                }

                if let Some(metadata) = ticker_metadata.get(&ticker) {
                    if !metadata.tradable {
                        notes.push(format!("signal_{}_not_tradable", ticker));
                        record_skip(&ticker, SignalAction::Buy, "signal_not_tradable", None);
                        continue;
                    }
                }

                if account_state.open_buy_orders.contains(&ticker) {
                    notes.push(format!("signal_{}_pending_buy_order", ticker));
                    record_skip(&ticker, SignalAction::Buy, "signal_pending_buy_order", None);
                    continue;
                }

                if let Some(last_trade_date) = latest_live_trade_dates.get(&ticker) {
                    if *last_trade_date >= target_date {
                        notes.push(format!("signal_{}_already_traded", ticker));
                        record_skip(&ticker, SignalAction::Buy, "signal_already_traded", None);
                        continue;
                    }
                }

                let Some(ticker_candles) = candles_by_ticker.get(&ticker) else {
                    notes.push(format!("missing_candles_for_{}", ticker));
                    record_skip(&ticker, SignalAction::Buy, "missing_candles", None);
                    continue;
                };
                let Some((candle_index, current_candle)) = ticker_candles
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, candle)| candle.date == target_date)
                    .map(|(index, candle)| (index, *candle))
                else {
                    notes.push(format!("no_candle_for_signal_{}_on_date", ticker));
                    record_skip(&ticker, SignalAction::Buy, "missing_candle_for_date", None);
                    continue;
                };
                let planning_close = Self::planning_reference_price(current_candle);
                if !self.entry_price_supported(planning_close) {
                    notes.push(format!("signal_{}_price_out_of_range", ticker));
                    record_skip(&ticker, SignalAction::Buy, "price_out_of_range", None);
                    continue;
                }

                if !has_minimum_dollar_volume(
                    ticker_candles,
                    candle_index,
                    self.runtime_settings.minimum_dollar_volume_lookback,
                    self.runtime_settings.minimum_dollar_volume_for_entry,
                ) {
                    notes.push(format!("signal_{}_insufficient_volume", ticker));
                    record_skip(&ticker, SignalAction::Buy, "insufficient_volume", None);
                    continue;
                }

                let (order_type, maybe_price, discount_applied) =
                    if self.config.buy_discount_ratio.is_finite()
                        && self.config.buy_discount_ratio > 0.0
                    {
                        let price = planning_close * (1.0 - self.config.buy_discount_ratio);
                        if price.is_finite() && price > 0.0 {
                            ("limit", Some(price), true)
                        } else {
                            ("limit", None, true)
                        }
                    } else {
                        ("market", Some(planning_close), false)
                    };

                let Some(price) = maybe_price else {
                    notes.push(format!("price_unavailable_for_{}", ticker));
                    record_skip(&ticker, SignalAction::Buy, "price_unavailable", None);
                    continue;
                };

                let signal_confidence = signal.confidence.unwrap_or(1.0);
                let realized_vol = if (self.config.position_sizing.mode == 2
                    || self.config.position_sizing.mode == 3)
                    && self.config.position_sizing.vol_target_annual > 0.0
                {
                    Some(estimate_annualized_volatility_from_candles(
                        ticker_candles,
                        candle_index,
                        self.config.position_sizing.vol_lookback,
                    ))
                } else {
                    None
                };

                let allocation = match determine_position_size(PositionSizingParams {
                    price,
                    available_cash,
                    trade_size_ratio: self.config.trade_size_ratio,
                    minimum_trade_size: self.config.minimum_trade_size,
                    position_sizing_mode: self.config.position_sizing.mode,
                    confidence: signal_confidence,
                    vol_target_annual: self.config.position_sizing.vol_target_annual,
                    realized_vol,
                }) {
                    PositionSizingOutcome::Sized(allocation) => allocation,
                    PositionSizingOutcome::TooSmall => {
                        notes.push(format!("signal_{}_insufficient_size", ticker));
                        record_skip(&ticker, SignalAction::Buy, "insufficient_size", None);
                        continue;
                    }
                    PositionSizingOutcome::InsufficientCash { required } => {
                        notes.push(format!(
                            "insufficient_cash_for_signal_{} (need {:.2}, have {:.2})",
                            ticker, required, available_cash
                        ));
                        record_skip(
                            &ticker,
                            SignalAction::Buy,
                            "insufficient_cash",
                            Some(format!("need {:.2}, have {:.2}", required, available_cash)),
                        );
                        continue;
                    }
                };

                let stop_loss = initial_stop_loss(
                    self.config.stop_loss.mode,
                    self.config.stop_loss.atr_multiplier,
                    self.config.stop_loss.atr_period,
                    self.config.stop_loss.ratio,
                    price,
                    ticker_candles,
                    candle_index,
                    false,
                );

                let trade_id = format!(
                    "{}-plan",
                    generate_trade_id(strategy_id, account_id, &ticker, target_date)
                );

                available_cash -= allocation.trade_value;
                operations.push(AccountOperationPlan {
                    trade_id,
                    ticker: ticker.clone(),
                    quantity: Some(allocation.quantity),
                    price: Some(price),
                    stop_loss,
                    previous_stop_loss: None,
                    triggered_at: target_date,
                    operation_type: AccountOperationType::OpenPosition,
                    reason: Some("buy_signal_sync".to_string()),
                    order_type: Some(order_type.to_string()),
                    discount_applied: Some(discount_applied),
                    signal_confidence: signal.confidence,
                    account_cash_at_plan: Some(account_state.available_cash),
                    days_held: None,
                });
            }
        }

        let mut live_trade_refs: Vec<(u64, &Trade)> = existing_trades
            .iter()
            .filter(|trade| trade.status == TradeStatus::Active)
            .map(|trade| {
                (
                    Self::ticker_date_hash(trade.ticker.as_str(), target_date),
                    trade,
                )
            })
            .collect();
        live_trade_refs.sort_by(|(hash_a, trade_a), (hash_b, trade_b)| {
            hash_a
                .cmp(hash_b)
                .then_with(|| trade_a.ticker.cmp(&trade_b.ticker))
                .then_with(|| trade_a.id.cmp(&trade_b.id))
        });
        let mut pending_sell_signals: HashSet<String> = sell_signals.keys().cloned().collect();
        for (_, trade) in live_trade_refs {
            if trade.date > target_date {
                notes.push(format!(
                    "trade {} occurs after latest candle {}",
                    trade.id, target_date
                ));
                if pending_sell_signals.remove(&trade.ticker) {
                    record_skip(
                        &trade.ticker,
                        SignalAction::Sell,
                        "sell_trade_after_latest_candle",
                        None,
                    );
                }
                continue;
            }

            if trade
                .exit_order_id
                .as_deref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
            {
                notes.push(format!("trade_{}_pending_exit_order", trade.id));
                if pending_sell_signals.remove(&trade.ticker) {
                    record_skip(
                        &trade.ticker,
                        SignalAction::Sell,
                        "sell_exit_order_pending",
                        None,
                    );
                }
                continue;
            }

            let Some(ticker_candles) = candles_by_ticker.get(&trade.ticker) else {
                notes.push(format!("missing_candles_for_{}", trade.ticker));
                if pending_sell_signals.remove(&trade.ticker) {
                    record_skip(
                        &trade.ticker,
                        SignalAction::Sell,
                        "sell_missing_candles",
                        None,
                    );
                }
                continue;
            };
            let Some((candle_index, current_candle)) = ticker_candles
                .iter()
                .enumerate()
                .rev()
                .find(|(_, candle)| candle.date == target_date)
                .map(|(index, candle)| (index, *candle))
            else {
                notes.push(format!("no_candle_for_{}_on_latest_date", trade.ticker));
                if pending_sell_signals.remove(&trade.ticker) {
                    record_skip(
                        &trade.ticker,
                        SignalAction::Sell,
                        "sell_missing_candle_for_date",
                        None,
                    );
                }
                continue;
            };
            let planning_close = Self::planning_reference_price(current_candle);
            let current_date = current_candle.date;
            if current_date < trade.date {
                notes.push(format!(
                    "latest_candle_for_{} precedes trade {}",
                    trade.ticker, trade.id
                ));
                if pending_sell_signals.remove(&trade.ticker) {
                    record_skip(
                        &trade.ticker,
                        SignalAction::Sell,
                        "sell_latest_candle_precedes_trade",
                        None,
                    );
                }
                continue;
            }

            let days_held = current_date.signed_duration_since(trade.date).num_days();
            let days_held_i32 = i32::try_from(days_held).unwrap_or(i32::MAX);

            if let Some(signal) = sell_signals.get(&trade.ticker) {
                operations.push(AccountOperationPlan {
                    trade_id: trade.id.clone(),
                    ticker: trade.ticker.clone(),
                    quantity: Some(trade.quantity),
                    price: Some(planning_close),
                    stop_loss: trade.stop_loss,
                    previous_stop_loss: None,
                    triggered_at: current_date,
                    operation_type: AccountOperationType::ClosePosition,
                    reason: Some("sell_signal_sync".to_string()),
                    order_type: Some("market".to_string()),
                    discount_applied: None,
                    signal_confidence: signal.confidence,
                    account_cash_at_plan: None,
                    days_held: Some(days_held_i32),
                });
                pending_sell_signals.remove(&trade.ticker);
                continue;
            }

            if self.config.max_holding_days > 0 && days_held >= self.config.max_holding_days as i64
            {
                operations.push(AccountOperationPlan {
                    trade_id: trade.id.clone(),
                    ticker: trade.ticker.clone(),
                    quantity: Some(trade.quantity),
                    price: Some(planning_close),
                    stop_loss: trade.stop_loss,
                    previous_stop_loss: None,
                    triggered_at: current_date,
                    operation_type: AccountOperationType::ClosePosition,
                    reason: Some("max_holding_days".to_string()),
                    order_type: None,
                    discount_applied: None,
                    signal_confidence: None,
                    account_cash_at_plan: None,
                    days_held: Some(days_held_i32),
                });
                continue;
            }

            if let Some(curr_stop) = trade.stop_loss {
                if trade.date < current_date {
                    if self.should_repair_missing_stop(account_state, trade) {
                        operations.push(AccountOperationPlan {
                            trade_id: trade.id.clone(),
                            ticker: trade.ticker.clone(),
                            quantity: Some(trade.quantity),
                            price: Some(planning_close),
                            stop_loss: Some(curr_stop),
                            previous_stop_loss: None,
                            triggered_at: current_date,
                            operation_type: AccountOperationType::UpdateStopLoss,
                            reason: Some("stop_missing".to_string()),
                            order_type: None,
                            discount_applied: None,
                            signal_confidence: None,
                            account_cash_at_plan: None,
                            days_held: None,
                        });
                        continue;
                    }

                    if let Some(update) = compute_trailing_stop(TrailingStopParams {
                        stop_loss_mode: self.config.stop_loss.mode,
                        atr_multiplier: self.config.stop_loss.atr_multiplier,
                        atr_period: self.config.stop_loss.atr_period,
                        ticker_candles,
                        candle_index,
                        current_candle,
                        current_stop: curr_stop,
                        is_short: trade.quantity < 0,
                        planning_close: Some(planning_close),
                    }) {
                        let new_stop = update.value();
                        operations.push(AccountOperationPlan {
                            trade_id: trade.id.clone(),
                            ticker: trade.ticker.clone(),
                            quantity: Some(trade.quantity),
                            price: Some(planning_close),
                            stop_loss: Some(new_stop),
                            previous_stop_loss: Some(curr_stop),
                            triggered_at: current_date,
                            operation_type: AccountOperationType::UpdateStopLoss,
                            reason: Some(update.reason().to_string()),
                            order_type: None,
                            discount_applied: None,
                            signal_confidence: None,
                            account_cash_at_plan: None,
                            days_held: None,
                        });
                    }
                }
            }
        }

        for ticker in pending_sell_signals {
            record_skip(&ticker, SignalAction::Sell, "sell_no_active_position", None);
        }

        PlannedOperations {
            operations,
            notes,
            skipped_signals,
        }
    }

    fn ordered_tickers_for_date<'a>(tickers: &'a [String], date: DateTime<Utc>) -> Vec<&'a String> {
        let mut ordered: Vec<(u64, &'a String)> = tickers
            .iter()
            .map(|ticker| (Self::ticker_date_hash(ticker.as_str(), date), ticker))
            .collect();
        ordered.sort_by(|(hash_a, ticker_a), (hash_b, ticker_b)| {
            hash_a.cmp(hash_b).then_with(|| ticker_a.cmp(ticker_b))
        });
        ordered.into_iter().map(|(_, ticker)| ticker).collect()
    }

    fn ticker_date_hash(ticker: &str, date: DateTime<Utc>) -> u64 {
        let mut hasher = DefaultHasher::new();
        ticker.hash(&mut hasher);
        date.timestamp().hash(&mut hasher);
        hasher.finish()
    }

    fn planning_reference_price(candle: &Candle) -> f64 {
        candle.unadjusted_close.unwrap_or(candle.close)
    }

    fn should_repair_missing_stop(
        &self,
        account_state: &AccountStateSnapshot,
        trade: &Trade,
    ) -> bool {
        if trade.stop_loss.is_none() {
            return false;
        }

        let ticker = &trade.ticker;

        let has_position = account_state
            .positions
            .iter()
            .any(|position| position.quantity == trade.quantity && position.ticker == *ticker);
        if !has_position {
            return false;
        }

        let desired_side = if trade.quantity < 0 { "buy" } else { "sell" };
        let desired_qty = trade.quantity.abs();
        let has_stop_order = account_state
            .stop_orders
            .get(ticker)
            .map(|orders| {
                orders
                    .iter()
                    .any(|order| order.quantity.abs() == desired_qty && order.side == desired_side)
            })
            .unwrap_or(false);
        if has_stop_order {
            return false;
        }

        let has_side_order = if trade.quantity < 0 {
            account_state.open_buy_orders.contains(ticker)
        } else {
            account_state.open_sell_orders.contains(ticker)
        };

        !has_side_order
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LocalOptimizationObjective;
    use crate::models::{AccountOperationType, SignalAction, StrategySignal, Trade, TradeStatus};
    use crate::trading_rules::PRICE_EPSILON;
    use chrono::{Duration, Utc};
    use std::collections::{HashMap, HashSet};

    fn test_runtime_settings() -> EngineRuntimeSettings {
        EngineRuntimeSettings {
            trade_close_fee_rate: 0.0005,
            trade_slippage_rate: 0.003,
            short_borrow_fee_annual_rate: 0.003,
            trade_entry_price_min: 0.10,
            trade_entry_price_max: 1000.0,
            minimum_dollar_volume_for_entry: 150_000.0,
            minimum_dollar_volume_lookback: 5,
            local_optimization_version: 9,
            local_optimization_step_multipliers: vec![
                -5.0, -4.0, -3.0, -2.0, -1.0, 1.0, 2.0, 3.0, 4.0, 5.0,
            ],
            local_optimization_objective: LocalOptimizationObjective::Cagr,
            max_allowed_drawdown_ratio: 0.40,
        }
    }

    fn create_date(days_offset: i64) -> DateTime<Utc> {
        Utc::now()
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(Utc)
            .unwrap()
            + Duration::days(days_offset)
    }

    fn generate_candles(ticker: &str, prices: Vec<f64>) -> (Vec<Candle>, Vec<DateTime<Utc>>) {
        let mut candles = Vec::new();
        let mut dates = Vec::new();
        for (i, &price) in prices.iter().enumerate() {
            let date = create_date(i as i64);
            dates.push(date);
            candles.push(Candle {
                ticker: ticker.to_string(),
                date,
                open: price,
                high: price,
                low: price,
                close: price,
                unadjusted_close: Some(price),
                volume_shares: 10_000_000,
            });
        }
        (candles, dates)
    }

    fn generate_candles_with_history(
        ticker: &str,
        mut prices: Vec<f64>,
    ) -> (Vec<Candle>, Vec<DateTime<Utc>>, usize) {
        let required_history = test_runtime_settings()
            .minimum_dollar_volume_lookback
            .saturating_sub(1);
        if prices.is_empty() {
            return (Vec::new(), Vec::new(), required_history);
        }
        let first_price = prices[0];
        let mut extended = Vec::with_capacity(required_history + prices.len());
        extended.extend(std::iter::repeat(first_price).take(required_history));
        extended.append(&mut prices);
        let (candles, dates) = generate_candles(ticker, extended);
        (candles, dates, required_history)
    }

    fn generate_spy_candles(count: usize) -> Vec<Candle> {
        if count == 0 {
            return Vec::new();
        }
        let (candles, _) = generate_candles("SPY", vec![450.0; count]);
        candles
    }

    #[test]
    fn run_loop_forces_liquidation_when_equity_negative() {
        let engine = Engine::new(test_runtime_settings());

        let (candles, dates) = generate_candles("TEST", vec![50.0, 120.0]);
        let candle_refs: Vec<&Candle> = candles.iter().collect();
        let mut candles_by_ticker = HashMap::new();
        candles_by_ticker.insert("TEST".to_string(), candle_refs);

        let active_trade = Trade {
            id: "short".to_string(),
            strategy_id: "strategy".to_string(),
            ticker: "TEST".to_string(),
            quantity: -200,
            price: 50.0,
            date: dates[0],
            status: TradeStatus::Active,
            pnl: None,
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss: None,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        };

        let resume_state = BacktestResumeState {
            loop_start_index: 0,
            cash: 0.0,
            active_trades: vec![active_trade],
            closed_trades: Vec::new(),
            daily_snapshots: Vec::new(),
            generated_signals: Vec::new(),
            max_portfolio_value: engine.config.initial_capital,
            start_date: dates[0],
        };

        let tickers = vec!["TEST".to_string()];
        let result = engine.run_backtest_loop(
            &tickers,
            &dates,
            &candles_by_ticker,
            0,
            0,
            |_, _, _, _| None,
            Some(resume_state),
            false,
        );

        assert!(result.active_trades.is_empty());
        assert_eq!(result.closed_trades.len(), 1);
        assert!(result.cash < 0.0);
        assert!(result
            .daily_snapshots
            .last()
            .map(|snapshot| snapshot.portfolio_value < 0.0 && snapshot.positions_value == 0.0)
            .unwrap_or(false));
    }

    fn with_spy_reference(candles: &[Candle]) -> Vec<Candle> {
        let mut combined = candles.to_vec();
        combined.extend(generate_spy_candles(candles.len()));
        combined
    }

    #[test]
    fn test_resolve_trading_start_index_rounds_down_within_same_day() {
        let day_zero = create_date(0);
        let day_one = create_date(1);
        let unique_dates = vec![day_zero, day_one];
        let intraday_start = day_zero + Duration::hours(15);

        assert_eq!(
            Engine::resolve_trading_start_index(&unique_dates, intraday_start),
            0
        );
    }

    #[test]
    fn test_resolve_trading_start_index_clamps_to_last_available_day() {
        let day_zero = create_date(0);
        let day_one = create_date(1);
        let unique_dates = vec![day_zero, day_one];
        let future_start = day_one + Duration::days(5);

        assert_eq!(
            Engine::resolve_trading_start_index(&unique_dates, future_start),
            1
        );
    }

    struct MockStrategy {
        signals: HashMap<(String, DateTime<Utc>), StrategySignal>,
    }

    impl Strategy for MockStrategy {
        fn generate_signal(
            &self,
            ticker: &str,
            candles: &[Candle],
            index: usize,
        ) -> StrategySignal {
            let current_date = candles[index].date;
            self.signals
                .get(&(ticker.to_string(), current_date))
                .cloned()
                .unwrap_or(StrategySignal {
                    action: SignalAction::Hold,
                    confidence: 0.0,
                })
        }

        fn get_min_data_points(&self) -> usize {
            0
        }

        fn get_template_id(&self) -> &str {
            "mock_strategy"
        }
    }

    #[test]
    fn test_backtest_constant_price() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "CONST".to_string();
        let spy = "SPY".to_string();
        let (candles, unique_dates, history_offset) =
            generate_candles_with_history(&ticker, vec![100.0, 100.0, 100.0, 100.0]);
        let all_candles = with_spy_reference(&candles);

        let mut signals = HashMap::new();
        signals.insert(
            (ticker.clone(), unique_dates[history_offset]),
            StrategySignal {
                action: SignalAction::Buy,
                confidence: 1.0,
            },
        );
        signals.insert(
            (ticker.clone(), unique_dates[history_offset + 1]),
            StrategySignal {
                action: SignalAction::Sell,
                confidence: 1.0,
            },
        );
        let strategy = MockStrategy { signals };

        let BacktestRun { result, .. } = engine
            .backtest(
                Some(&strategy),
                strategy.get_template_id(),
                &[ticker.clone(), spy.clone()],
                &all_candles,
                &unique_dates,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.trades.len(), 1);
        let trade = &result.trades[0];
        let entry_candle = candles
            .iter()
            .find(|c| c.date == trade.date)
            .expect("entry candle missing");
        let expected_entry =
            engine.apply_entry_slippage_with_candle(entry_candle.open, false, entry_candle);
        assert!((trade.price - expected_entry).abs() < 1e-9);
        let exit_date = trade.exit_date.expect("trade should have exit_date");
        let exit_candle = candles
            .iter()
            .find(|c| c.date == exit_date)
            .expect("exit candle missing");
        let expected_exit =
            engine.apply_exit_slippage_with_candle(exit_candle.close, false, exit_candle);
        assert!((trade.exit_price.unwrap() - expected_exit).abs() < 1e-9);
        let exit_price = trade.exit_price.unwrap();
        let fee = engine.calculate_trade_close_fee(
            trade.ticker.as_str(),
            trade.quantity,
            exit_price,
            trade.date,
            exit_date,
        );
        let expected_pnl = (exit_price - trade.price) * trade.quantity as f64 - fee;
        assert!((trade.pnl.unwrap() - expected_pnl).abs() < 1e-9);
        assert!(
            (result.final_portfolio_value - (engine.config.initial_capital + trade.pnl.unwrap()))
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn test_limit_buy_skips_slippage() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.buy_discount_ratio = 0.05;

        let ticker = "LIMIT".to_string();
        let spy = "SPY".to_string();
        let (candles, unique_dates, history_offset) =
            generate_candles_with_history(&ticker, vec![100.0, 95.0, 110.0]);
        let all_candles = with_spy_reference(&candles);

        let mut signals = HashMap::new();
        signals.insert(
            (ticker.clone(), unique_dates[history_offset]),
            StrategySignal {
                action: SignalAction::Buy,
                confidence: 1.0,
            },
        );
        signals.insert(
            (ticker.clone(), unique_dates[history_offset + 1]),
            StrategySignal {
                action: SignalAction::Sell,
                confidence: 1.0,
            },
        );
        let strategy = MockStrategy { signals };

        let BacktestRun { result, .. } = engine
            .backtest(
                Some(&strategy),
                strategy.get_template_id(),
                &[ticker.clone(), spy.clone()],
                &all_candles,
                &unique_dates,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.trades.len(), 1);
        let trade = &result.trades[0];
        let discounted_price =
            candles[history_offset].close * (1.0 - engine.config.buy_discount_ratio);
        let next_open = candles[history_offset + 1].open;
        let expected_entry = next_open.min(discounted_price);
        assert!((trade.price - expected_entry).abs() < 1e-9);
        let slippage_entry = expected_entry * (1.0 + engine.runtime_settings.trade_slippage_rate);
        assert!(
            (trade.price - slippage_entry).abs() > 1e-6,
            "limit entry should not include slippage"
        );
    }

    #[test]
    fn test_backtest_skips_low_volume_entries_but_keeps_signal() {
        let engine = Engine::new(test_runtime_settings());

        let ticker = "LOWV".to_string();
        let spy = "SPY".to_string();
        let day_zero = create_date(0);
        let day_one = create_date(1);
        let unique_dates = vec![day_zero, day_one];

        let candles = vec![
            Candle {
                ticker: ticker.clone(),
                date: day_zero,
                open: 10.0,
                high: 10.0,
                low: 10.0,
                close: 10.0,
                unadjusted_close: Some(10.0),
                volume_shares: 1_000,
            },
            Candle {
                ticker: ticker.clone(),
                date: day_one,
                open: 10.5,
                high: 10.5,
                low: 10.1,
                close: 10.5,
                unadjusted_close: Some(10.5),
                volume_shares: 1_000,
            },
            Candle {
                ticker: spy.clone(),
                date: day_zero,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                unadjusted_close: Some(100.0),
                volume_shares: 5_000_000,
            },
            Candle {
                ticker: spy.clone(),
                date: day_one,
                open: 101.0,
                high: 102.0,
                low: 100.5,
                close: 101.5,
                unadjusted_close: Some(101.5),
                volume_shares: 5_000_000,
            },
        ];

        let mut signals = HashMap::new();
        signals.insert(
            (ticker.clone(), day_zero),
            StrategySignal {
                action: SignalAction::Buy,
                confidence: 0.8,
            },
        );
        let strategy = MockStrategy { signals };

        let BacktestRun {
            result, signals, ..
        } = engine
            .backtest(
                Some(&strategy),
                strategy.get_template_id(),
                &[ticker.clone(), spy.clone()],
                &candles,
                &unique_dates,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(
            result.trades.len(),
            0,
            "engine should refuse to enter trades for illiquid tickers"
        );

        assert_eq!(
            signals.len(),
            1,
            "engine should still surface generated signals even if volume is low"
        );
        assert!(
            matches!(signals[0].action, SignalAction::Buy),
            "generated signal should retain its original buy action"
        );
    }

    #[test]
    fn test_execute_buy_signal_enforces_minimum_dollar_volume() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "ILLQ".to_string();
        let total_candles = engine
            .runtime_settings
            .minimum_dollar_volume_lookback
            .max(2);
        let signal_index = total_candles - 2;
        let entry_index = signal_index + 1;
        let make_candles = |volumes: Vec<i64>| -> Vec<Candle> {
            assert!(
                volumes.len() >= entry_index + 1,
                "need at least signal and entry candles"
            );
            volumes
                .into_iter()
                .enumerate()
                .map(|(i, volume)| Candle {
                    ticker: ticker.clone(),
                    date: create_date(i as i64),
                    open: 10.0,
                    high: 10.0,
                    low: 10.0,
                    close: 10.0,
                    unadjusted_close: Some(10.0),
                    volume_shares: volume,
                })
                .collect()
        };
        let required_volume_shares =
            ((engine.runtime_settings.minimum_dollar_volume_for_entry / 10.0).ceil() as i64).max(1);

        let mut illiquid_volumes = vec![required_volume_shares; total_candles];
        illiquid_volumes[entry_index] = 100; // below requirement in the most recent candle
        let illiquid = make_candles(illiquid_volumes);
        let illiquid_refs: Vec<&Candle> = illiquid.iter().collect();
        let mut cash = engine.config.initial_capital;
        let mut active_trades = Vec::new();
        let skipped = engine.execute_buy_signal(
            &mut active_trades,
            &mut cash,
            &ticker,
            illiquid_refs[signal_index],
            illiquid_refs.get(entry_index).copied(),
            &illiquid_refs,
            signal_index,
            1.0,
        );
        assert!(matches!(skipped, EntrySignalOutcome::Skipped { .. }));
        assert!(active_trades.is_empty());

        let liquid = make_candles(vec![required_volume_shares * 2; total_candles]);
        let liquid_refs: Vec<&Candle> = liquid.iter().collect();
        let mut cash_liquid = engine.config.initial_capital;
        let mut active_trades_liquid = Vec::new();
        let executed = engine.execute_buy_signal(
            &mut active_trades_liquid,
            &mut cash_liquid,
            &ticker,
            liquid_refs[signal_index],
            liquid_refs.get(entry_index).copied(),
            &liquid_refs,
            signal_index,
            1.0,
        );
        assert!(matches!(executed, EntrySignalOutcome::Executed));
        assert_eq!(active_trades_liquid.len(), 1);
    }

    #[test]
    fn test_execute_buy_signal_rejects_price_outside_supported_range() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "PRNG".to_string();

        let (expensive_candles, _) = generate_candles(&ticker, vec![2_000.0, 25.0]);
        let expensive_refs: Vec<&Candle> = expensive_candles.iter().collect();
        let mut cash = engine.config.initial_capital;
        let mut active_trades = Vec::new();
        let skipped_high = engine.execute_buy_signal(
            &mut active_trades,
            &mut cash,
            &ticker,
            expensive_refs[0],
            expensive_refs.get(1).copied(),
            &expensive_refs,
            0,
            1.0,
        );
        assert!(matches!(skipped_high, EntrySignalOutcome::Skipped { .. }));
        assert!(active_trades.is_empty());

        let (cheap_candles, _) = generate_candles(&ticker, vec![0.05, 25.0]);
        let cheap_refs: Vec<&Candle> = cheap_candles.iter().collect();
        let mut cheap_cash = engine.config.initial_capital;
        let mut cheap_trades = Vec::new();
        let skipped_low = engine.execute_buy_signal(
            &mut cheap_trades,
            &mut cheap_cash,
            &ticker,
            cheap_refs[0],
            cheap_refs.get(1).copied(),
            &cheap_refs,
            0,
            1.0,
        );
        assert!(matches!(skipped_low, EntrySignalOutcome::Skipped { .. }));
        assert!(cheap_trades.is_empty());
    }

    #[test]
    fn test_backtest_resumes_from_existing_result() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "RSUM".to_string();
        let spy = "SPY".to_string();
        let prices = vec![100.0, 105.0, 110.0];
        let (candles_full, unique_dates_full, history_offset) =
            generate_candles_with_history(&ticker, prices);
        let split_index = history_offset + 2;
        let candles_initial: Vec<Candle> = candles_full[..split_index].to_vec();
        let unique_dates_initial: Vec<DateTime<Utc>> = unique_dates_full[..split_index].to_vec();
        let candles_initial_with_spy = with_spy_reference(&candles_initial);
        let candles_full_with_spy = with_spy_reference(&candles_full);

        let buy_signal = GeneratedSignal {
            date: unique_dates_full[history_offset],
            ticker: ticker.clone(),
            action: SignalAction::Buy,
            confidence: Some(1.0),
        };
        let sell_signal = GeneratedSignal {
            date: unique_dates_full[history_offset + 2],
            ticker: ticker.clone(),
            action: SignalAction::Sell,
            confidence: Some(1.0),
        };
        let initial_signals = vec![buy_signal.clone()];
        let BacktestRun {
            result: initial_result,
            ..
        } = engine
            .backtest(
                None,
                "resume_template",
                &[ticker.clone(), spy.clone()],
                &candles_initial_with_spy,
                &unique_dates_initial,
                Some(&initial_signals),
                Some(unique_dates_initial[0]),
                None,
            )
            .unwrap();

        assert_eq!(initial_result.trades.len(), 1);
        assert_eq!(
            initial_result.trades[0].status,
            TradeStatus::Active,
            "position should remain open before resume"
        );

        let all_signals = vec![buy_signal, sell_signal];
        let BacktestRun {
            result: resumed_result,
            ..
        } = engine
            .backtest(
                None,
                "resume_template",
                &[ticker.clone(), spy.clone()],
                &candles_full_with_spy,
                &unique_dates_full,
                Some(&all_signals),
                Some(unique_dates_full[0]),
                Some(&initial_result),
            )
            .unwrap();

        assert_eq!(resumed_result.start_date, initial_result.start_date);
        assert_eq!(
            resumed_result.end_date,
            unique_dates_full[history_offset + 2]
        );
        assert_eq!(resumed_result.trades.len(), 1);
        let trade = &resumed_result.trades[0];
        assert_eq!(trade.status, TradeStatus::Closed);
        assert_eq!(trade.exit_date, Some(unique_dates_full[history_offset + 2]));
        assert!(
            resumed_result
                .daily_snapshots
                .iter()
                .any(|snapshot| snapshot.date == unique_dates_full[history_offset + 2]),
            "resume should append new daily snapshot"
        );
    }

    #[test]
    fn test_backtest_growing_price() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "GROW".to_string();
        let spy = "SPY".to_string();
        let prices = (100..=130).map(|i| i as f64).collect();
        let (candles, unique_dates, history_offset) =
            generate_candles_with_history(&ticker, prices);
        let all_candles = with_spy_reference(&candles);

        let mut signals = HashMap::new();
        signals.insert(
            (ticker.clone(), unique_dates[history_offset]),
            StrategySignal {
                action: SignalAction::Buy,
                confidence: 1.0,
            },
        );
        signals.insert(
            (ticker.clone(), unique_dates[unique_dates.len() - 1]),
            StrategySignal {
                action: SignalAction::Sell,
                confidence: 1.0,
            },
        );
        let strategy = MockStrategy { signals };

        let BacktestRun { result, .. } = engine
            .backtest(
                Some(&strategy),
                strategy.get_template_id(),
                &[ticker.clone(), spy.clone()],
                &all_candles,
                &unique_dates,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.trades.len(), 1);
        let trade = &result.trades[0];
        let entry_candle = candles
            .iter()
            .find(|c| c.date == trade.date)
            .expect("entry candle missing");
        let expected_entry =
            engine.apply_entry_slippage_with_candle(entry_candle.open, false, entry_candle);
        assert!((trade.price - expected_entry).abs() < 1e-9);
        let exit_date = trade.exit_date.expect("trade should have exit_date");
        let exit_candle = candles
            .iter()
            .find(|c| c.date == exit_date)
            .expect("exit candle missing");
        let expected_exit =
            engine.apply_exit_slippage_with_candle(exit_candle.close, false, exit_candle);
        assert!((trade.exit_price.unwrap() - expected_exit).abs() < 1e-9);
        assert!(trade.pnl.unwrap() > 0.0);
        assert!(result.final_portfolio_value > engine.config.initial_capital);
        assert!(result.performance.sharpe_ratio > 0.0);
        assert!(result.performance.total_return > 0.0);
    }

    #[test]
    fn test_open_trades_remain_active() {
        let mut params = HashMap::new();
        params.insert("tradeSizeRatio".to_string(), 1.0);
        params.insert("minimumTradeSize".to_string(), 0.0);
        let engine = Engine::from_parameters(&params, test_runtime_settings());

        let ticker = "OPEN".to_string();
        let spy = "SPY".to_string();
        let (candles, unique_dates, history_offset) =
            generate_candles_with_history(&ticker, vec![100.0, 105.0, 110.0]);
        let all_candles = with_spy_reference(&candles);

        let mut signals = HashMap::new();
        signals.insert(
            (ticker.clone(), unique_dates[history_offset]),
            StrategySignal {
                action: SignalAction::Buy,
                confidence: 1.0,
            },
        );
        let strategy = MockStrategy { signals };

        let BacktestRun { result, .. } = engine
            .backtest(
                Some(&strategy),
                strategy.get_template_id(),
                &[ticker.clone(), spy.clone()],
                &all_candles,
                &unique_dates,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.trades.len(), 1);
        let trade = &result.trades[0];
        assert_eq!(trade.status, TradeStatus::Active);
        assert!(trade.exit_price.is_none());
        assert!(trade.exit_date.is_none());
        assert!(result.final_portfolio_value > engine.config.initial_capital);
    }

    fn sample_account_state(cash: f64) -> AccountStateSnapshot {
        AccountStateSnapshot {
            available_cash: cash,
            buying_power: None,
            held_tickers: HashSet::new(),
            open_buy_orders: HashSet::new(),
            open_sell_orders: HashSet::new(),
            positions: Vec::new(),
            stop_orders: HashMap::new(),
        }
    }

    fn sample_account_state_with_holdings(
        cash: f64,
        holdings: &[(&str, i32, f64)],
        stop_price: Option<f64>,
    ) -> AccountStateSnapshot {
        let mut held_tickers: HashSet<String> = HashSet::new();
        let mut positions = Vec::new();
        let mut stop_orders = HashMap::new();

        for (ticker, qty, price) in holdings {
            let ticker_str = ticker.to_string();
            held_tickers.insert(ticker_str.clone());
            positions.push(AccountPositionState {
                ticker: ticker_str.clone(),
                quantity: *qty,
                avg_entry_price: *price,
                current_price: Some(*price),
            });
            if let Some(stop) = stop_price {
                stop_orders
                    .entry(ticker_str.clone())
                    .or_insert_with(Vec::new)
                    .push(AccountStopOrderState {
                        quantity: *qty,
                        stop_price: stop,
                        side: if *qty > 0 {
                            "sell".to_string()
                        } else {
                            "buy".to_string()
                        },
                    });
            }
        }

        AccountStateSnapshot {
            available_cash: cash,
            buying_power: None,
            held_tickers,
            open_buy_orders: HashSet::new(),
            open_sell_orders: HashSet::new(),
            positions,
            stop_orders,
        }
    }

    fn sample_active_trade(
        id: &str,
        strategy_id: &str,
        ticker: &str,
        quantity: i32,
        price: f64,
        date: DateTime<Utc>,
        stop_loss: Option<f64>,
    ) -> Trade {
        Trade {
            id: id.to_string(),
            strategy_id: strategy_id.to_string(),
            ticker: ticker.to_string(),
            quantity,
            price,
            date,
            status: TradeStatus::Active,
            pnl: None,
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        }
    }

    #[test]
    fn test_plan_account_operations_adds_market_buy_for_signal() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.buy_discount_ratio = 0.0;

        let (candles, dates, history_offset) =
            generate_candles_with_history("BUY", vec![100.0, 110.0]);
        let signal_date = dates[history_offset + 1];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "BUY".to_string(),
            action: SignalAction::Buy,
            confidence: Some(1.0),
        }];
        let state = sample_account_state(50_000.0);

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[],
            0,
            &HashMap::new(),
        );
        let buy = plan
            .operations
            .iter()
            .find(|op| op.operation_type == AccountOperationType::OpenPosition)
            .expect("expected buy op");
        assert_eq!(buy.ticker, "BUY");
        assert_eq!(buy.triggered_at, signal_date);
        assert_eq!(buy.price, Some(candles[history_offset + 1].close));
        assert_eq!(buy.order_type.as_deref(), Some("market"));
    }

    #[test]
    fn test_plan_account_operations_uses_limit_when_discount_enabled() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.buy_discount_ratio = 0.05;

        let (candles, dates, history_offset) =
            generate_candles_with_history("LIM", vec![50.0, 60.0]);
        let signal_date = dates[history_offset + 1];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "LIM".to_string(),
            action: SignalAction::Buy,
            confidence: Some(0.9),
        }];
        let state = sample_account_state(25_000.0);

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[],
            0,
            &HashMap::new(),
        );
        let buy = plan
            .operations
            .iter()
            .find(|op| op.operation_type == AccountOperationType::OpenPosition)
            .expect("expected limit buy op");
        let expected_price =
            candles[history_offset + 1].close * (1.0 - engine.config.buy_discount_ratio);
        assert!((buy.price.unwrap() - expected_price).abs() < 1e-6);
        assert_eq!(buy.order_type.as_deref(), Some("limit"));
        assert_eq!(buy.discount_applied, Some(true));
    }

    #[test]
    fn test_plan_account_operations_skips_when_cash_insufficient() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.buy_discount_ratio = 0.0;

        let (candles, dates, history_offset) =
            generate_candles_with_history("TINY", vec![10.0, 15.0]);
        let signal_date = dates[history_offset + 1];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "TINY".to_string(),
            action: SignalAction::Buy,
            confidence: Some(1.0),
        }];
        let state = sample_account_state(1.0);

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[],
            0,
            &HashMap::new(),
        );
        assert!(plan.operations.is_empty());
        assert!(plan.notes.iter().any(|note| {
            note.contains("insufficient_cash") || note.contains("insufficient_size")
        }));
    }

    #[test]
    fn test_plan_account_operations_skips_when_price_out_of_range() {
        let engine = Engine::new(test_runtime_settings());
        let (candles, dates, history_offset) = generate_candles_with_history("XRNG", vec![2_000.0]);
        let signal_date = dates[history_offset];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "XRNG".to_string(),
            action: SignalAction::Buy,
            confidence: Some(1.0),
        }];
        let state = sample_account_state(100_000.0);

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[],
            0,
            &HashMap::new(),
        );

        assert!(
            plan.operations.is_empty(),
            "price guard should skip generating buy orders"
        );
        assert!(plan
            .notes
            .iter()
            .any(|note| note == "signal_XRNG_price_out_of_range"));
    }

    #[test]
    fn test_plan_account_operations_skips_when_volume_insufficient() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.buy_discount_ratio = 0.0;

        let (mut candles, dates, history_offset) =
            generate_candles_with_history("DRY", vec![100.0, 105.0]);
        let signal_index = history_offset + 1;
        let signal_date = dates[signal_index];
        candles[signal_index].volume_shares = 1_000;

        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "DRY".to_string(),
            action: SignalAction::Buy,
            confidence: Some(0.9),
        }];
        let state = sample_account_state(50_000.0);

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[],
            0,
            &HashMap::new(),
        );

        assert!(
            plan.operations.is_empty(),
            "expected no operations when volume is insufficient"
        );
        assert!(plan
            .notes
            .iter()
            .any(|note| note == "signal_DRY_insufficient_volume"));
    }

    #[test]
    fn test_plan_account_operations_allows_adding_to_existing_position() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.buy_discount_ratio = 0.0;

        let (candles, dates, history_offset) =
            generate_candles_with_history("HOLD", vec![50.0, 55.0]);
        let entry_date = dates[history_offset];
        let signal_date = dates[history_offset + 1];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "HOLD".to_string(),
            action: SignalAction::Buy,
            confidence: Some(0.5),
        }];
        let state = sample_account_state_with_holdings(30_000.0, &[("HOLD", 10, 50.0)], None);

        let existing_trade = sample_active_trade(
            "existing-hold",
            "strategy",
            "HOLD",
            10,
            50.0,
            entry_date,
            Some(45.0),
        );

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[existing_trade],
            0,
            &HashMap::new(),
        );

        let buy_count = plan
            .operations
            .iter()
            .filter(|op| op.operation_type == AccountOperationType::OpenPosition)
            .count();
        assert_eq!(buy_count, 1, "expected buy even when already holding");
        assert!(
            plan.notes.iter().all(|note| !note.contains("already_held")),
            "should not emit held ticker notes"
        );
    }

    #[test]
    fn test_plan_account_operations_skips_trades_already_recorded_for_date() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.buy_discount_ratio = 0.0;

        let (candles, dates, history_offset) =
            generate_candles_with_history("DUPE", vec![50.0, 55.0]);
        let signal_date = dates[history_offset + 1];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "DUPE".to_string(),
            action: SignalAction::Buy,
            confidence: Some(0.5),
        }];
        let state = sample_account_state_with_holdings(25_000.0, &[], None);

        let existing_trade = sample_active_trade(
            "dupe-existing",
            "strategy",
            "DUPE",
            10,
            50.0,
            signal_date,
            Some(45.0),
        );

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[existing_trade],
            0,
            &HashMap::new(),
        );

        let open_count = plan
            .operations
            .iter()
            .filter(|op| op.operation_type == AccountOperationType::OpenPosition)
            .count();
        assert_eq!(
            open_count, 0,
            "expected no open operations when trade already exists for the date"
        );
        assert!(
            plan.notes
                .iter()
                .any(|note| note.contains("already_traded")),
            "expected note about duplicate trade"
        );
    }

    #[test]
    fn test_plan_account_operations_skips_when_existing_buy_ops_present() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.buy_discount_ratio = 0.0;

        let (candles, dates, history_offset) =
            generate_candles_with_history("LOCK", vec![20.0, 25.0]);
        let signal_date = dates[history_offset + 1];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "LOCK".to_string(),
            action: SignalAction::Buy,
            confidence: Some(0.7),
        }];
        let state = sample_account_state(5_000.0);

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[],
            1,
            &HashMap::new(),
        );

        let open_buys = plan
            .operations
            .iter()
            .filter(|op| op.operation_type == AccountOperationType::OpenPosition)
            .count();
        assert_eq!(
            open_buys, 0,
            "expected buys to be skipped when existing operations are present"
        );
        assert!(
            plan.notes
                .iter()
                .any(|note| note == "buy_operations_already_planned_for_day"),
            "expected note about existing buy operations"
        );
    }

    #[test]
    fn test_plan_account_operations_adds_close_for_sell_signal() {
        let engine = Engine::new(test_runtime_settings());

        let (candles, dates, history_offset) =
            generate_candles_with_history("SELL", vec![100.0, 95.0]);
        let signal_date = dates[history_offset + 1];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "SELL".to_string(),
            action: SignalAction::Sell,
            confidence: Some(0.6),
        }];
        let state = sample_account_state_with_holdings(0.0, &[("SELL", 10, 100.0)], Some(90.0));

        let existing_trade = sample_active_trade(
            "sell-trade",
            "strategy",
            "SELL",
            10,
            100.0,
            signal_date,
            Some(90.0),
        );

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[existing_trade],
            0,
            &HashMap::new(),
        );

        let close = plan
            .operations
            .iter()
            .find(|op| op.operation_type == AccountOperationType::ClosePosition)
            .expect("expected close operation");
        assert_eq!(close.ticker, "SELL");
        assert_eq!(close.trade_id, "sell-trade");
        assert_eq!(close.reason.as_deref(), Some("sell_signal_sync"));
        assert_eq!(close.order_type.as_deref(), Some("market"));
        assert_eq!(close.signal_confidence, Some(0.6));
    }

    #[test]
    fn test_plan_account_operations_skips_close_when_exit_order_pending() {
        let engine = Engine::new(test_runtime_settings());

        let (candles, dates, history_offset) =
            generate_candles_with_history("WAIT", vec![100.0, 95.0]);
        let signal_date = dates[history_offset + 1];
        let signals = vec![GeneratedSignal {
            date: signal_date,
            ticker: "WAIT".to_string(),
            action: SignalAction::Sell,
            confidence: Some(0.4),
        }];
        let state = sample_account_state_with_holdings(0.0, &[("WAIT", 10, 100.0)], Some(90.0));

        let mut existing_trade = sample_active_trade(
            "wait-trade",
            "strategy",
            "WAIT",
            10,
            100.0,
            signal_date,
            Some(90.0),
        );
        existing_trade.exit_order_id = Some("alpaca-exit".to_string());

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            signal_date,
            &state,
            &HashSet::new(),
            &[existing_trade],
            0,
            &HashMap::new(),
        );

        assert!(
            plan.operations
                .iter()
                .all(|op| op.operation_type != AccountOperationType::ClosePosition),
            "expected close operation to be skipped when an exit order is pending"
        );
        assert!(plan
            .notes
            .iter()
            .any(|note| note == "trade_wait-trade_pending_exit_order"));
    }

    #[test]
    fn test_plan_account_operations_updates_trailing_stop() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.stop_loss.mode = 1;
        engine.config.stop_loss.atr_multiplier = 2.0;
        engine.config.stop_loss.atr_period = 3;

        let (candles, dates) = generate_candles("PLAN", vec![100.0, 110.0, 120.0]);
        let signals = Vec::<GeneratedSignal>::new();
        let state = sample_account_state_with_holdings(0.0, &[("PLAN", 10, 90.0)], Some(90.0));

        let mut existing_trade = sample_active_trade(
            "plan-existing",
            "strategy",
            "PLAN",
            10,
            90.0,
            dates[0],
            Some(90.0),
        );
        existing_trade.stop_order_id = Some("alpaca-stop".to_string());

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            dates[2],
            &state,
            &HashSet::new(),
            &[existing_trade],
            0,
            &HashMap::new(),
        );

        let op = plan
            .operations
            .iter()
            .find(|op| op.operation_type == AccountOperationType::UpdateStopLoss)
            .expect("expected stop update");
        assert!(
            op.stop_loss.unwrap() > 90.0,
            "trailing stop should have increased"
        );
    }

    #[test]
    fn test_plan_account_operations_repairs_missing_stop() {
        let engine = Engine::new(test_runtime_settings());

        let (candles, dates) = generate_candles("MISS", vec![100.0, 105.0]);
        let signals = Vec::<GeneratedSignal>::new();
        let state = sample_account_state_with_holdings(0.0, &[("MISS", 10, 100.0)], None);

        let existing_trade = sample_active_trade(
            "missing-stop",
            "strategy",
            "MISS",
            10,
            100.0,
            dates[0],
            Some(90.0),
        );

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            dates[1],
            &state,
            &HashSet::new(),
            &[existing_trade],
            0,
            &HashMap::new(),
        );

        let op = plan
            .operations
            .iter()
            .find(|op| op.operation_type == AccountOperationType::UpdateStopLoss)
            .expect("expected stop repair operation");
        assert_eq!(op.reason.as_deref(), Some("stop_missing"));
        assert_eq!(op.stop_loss, Some(90.0));
    }

    #[test]
    fn test_plan_account_operations_uses_existing_trade_history() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.max_holding_days = 1;

        let (candles, dates) = generate_candles("OLD", vec![100.0, 101.0, 102.0]);
        let signals = Vec::<GeneratedSignal>::new();
        let state = sample_account_state_with_holdings(0.0, &[("OLD", 5, 95.0)], Some(90.0));

        let existing_trade = Trade {
            id: "existing-trade".to_string(),
            strategy_id: "strategy".to_string(),
            ticker: "OLD".to_string(),
            quantity: 5,
            price: 95.0,
            date: dates[0],
            status: TradeStatus::Active,
            pnl: None,
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss: Some(90.0),
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        };

        let plan = engine.plan_account_operations(
            "strategy",
            "acct",
            &signals,
            &candles,
            dates[2],
            &state,
            &HashSet::new(),
            &[existing_trade.clone()],
            0,
            &HashMap::new(),
        );

        let close = plan
            .operations
            .iter()
            .find(|op| op.operation_type == AccountOperationType::ClosePosition)
            .expect("expected close operation");
        assert_eq!(close.reason.as_deref(), Some("max_holding_days"));
        assert_eq!(close.trade_id, existing_trade.id);
        assert_eq!(close.days_held, Some(2));
    }

    #[test]
    fn test_execute_short_entry_rejects_price_outside_supported_range() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.allow_short_selling = true;
        let ticker = "SRNG".to_string();

        let (expensive_candles, _) = generate_candles(&ticker, vec![2_000.0, 50.0]);
        let expensive_refs: Vec<&Candle> = expensive_candles.iter().collect();
        let mut active_trades = Vec::new();
        let mut cash = engine.config.initial_capital;
        let skipped_high = engine.execute_short_entry(
            &mut active_trades,
            &mut cash,
            &ticker,
            expensive_refs[0],
            expensive_refs.get(1).copied(),
            &expensive_refs,
            0,
            1.0,
        );
        assert!(matches!(skipped_high, EntrySignalOutcome::Skipped { .. }));
        assert!(active_trades.is_empty());

        let (cheap_candles, _) = generate_candles(&ticker, vec![0.05, 50.0]);
        let cheap_refs: Vec<&Candle> = cheap_candles.iter().collect();
        let mut cheap_trades = Vec::new();
        let mut cheap_cash = engine.config.initial_capital;
        let skipped_low = engine.execute_short_entry(
            &mut cheap_trades,
            &mut cheap_cash,
            &ticker,
            cheap_refs[0],
            cheap_refs.get(1).copied(),
            &cheap_refs,
            0,
            1.0,
        );
        assert!(matches!(skipped_low, EntrySignalOutcome::Skipped { .. }));
        assert!(cheap_trades.is_empty());
    }

    #[test]
    fn test_execute_short_entry_adds_cash_and_sets_stop() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.allow_short_selling = true;
        let ticker = "SHORT".to_string();
        let (candles, _, history_offset) =
            generate_candles_with_history(&ticker, vec![100.0, 98.0]);
        let refs: Vec<&Candle> = candles.iter().collect();
        let signal_index = history_offset;
        let next_index = signal_index + 1;
        let mut active_trades = Vec::new();
        let mut cash = 10_000.0;

        let outcome = engine.execute_short_entry(
            &mut active_trades,
            &mut cash,
            &ticker,
            refs[signal_index],
            Some(refs[next_index]),
            &refs,
            signal_index,
            1.0,
        );

        assert!(matches!(outcome, EntrySignalOutcome::Executed));
        assert_eq!(active_trades.len(), 1);
        let trade = active_trades.last().unwrap();
        assert!(trade.quantity < 0);
        let expected_cash = 10_000.0 + trade.price * (-trade.quantity) as f64;
        assert!(
            (cash - expected_cash).abs() < PRICE_EPSILON,
            "cash {} expected {}",
            cash,
            expected_cash
        );
        assert!(
            trade.stop_loss.unwrap_or(0.0) > trade.price,
            "short trades should set a stop above entry"
        );
    }

    #[test]
    fn test_close_short_positions_realizes_pnl() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.allow_short_selling = true;
        let ticker = "SHT".to_string();
        let (candles, _, history_offset) =
            generate_candles_with_history(&ticker, vec![100.0, 105.0, 95.0]);
        let refs: Vec<&Candle> = candles.iter().collect();
        let signal_index = history_offset;
        let mut active_trades = Vec::new();
        let mut closed_trades = Vec::new();
        let mut cash = 10_000.0;

        let enter = engine.execute_short_entry(
            &mut active_trades,
            &mut cash,
            &ticker,
            refs[signal_index],
            Some(refs[signal_index + 1]),
            &refs,
            signal_index,
            1.0,
        );
        assert!(matches!(enter, EntrySignalOutcome::Executed));
        assert_eq!(active_trades.len(), 1);
        let initial_cash = 10_000.0;

        engine.close_short_positions(
            &mut active_trades,
            &mut closed_trades,
            &mut cash,
            &ticker,
            Some(refs[signal_index + 2]),
        );

        assert!(active_trades.is_empty());
        assert_eq!(closed_trades.len(), 1);
        let pnl = closed_trades[0].pnl.unwrap();
        assert!(
            (cash - initial_cash - pnl).abs() < PRICE_EPSILON,
            "cash delta {:.4} should equal pnl {:.4}",
            cash - initial_cash,
            pnl
        );
    }

    #[test]
    fn test_sell_fraction_rounds_to_full_exit() {
        let mut engine = Engine::new(test_runtime_settings());
        engine.config.sell_fraction = 0.5;

        let entry_date = create_date(0);
        let exit_date = create_date(1);
        let entry_price = 50.0;
        let exit_price = 65.0;

        let candle = Candle {
            ticker: "TEST".to_string(),
            date: exit_date,
            open: exit_price,
            high: exit_price,
            low: exit_price,
            close: exit_price,
            unadjusted_close: Some(exit_price),
            volume_shares: 1_000,
        };

        let mut active_trades = vec![Trade {
            id: "partial".to_string(),
            strategy_id: "strategy".to_string(),
            ticker: candle.ticker.clone(),
            quantity: 6,
            price: entry_price,
            date: entry_date,
            status: TradeStatus::Active,
            pnl: Some(999.0),
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss: None,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        }];
        let mut closed_trades = Vec::new();
        let mut cash = 0.0;

        engine.execute_sell_signal(
            &mut active_trades,
            &mut closed_trades,
            &mut cash,
            &candle.ticker,
            &candle,
            0.0,
        );

        assert_eq!(closed_trades.len(), 1);
        assert_eq!(closed_trades[0].quantity, 6);
        assert_eq!(active_trades.len(), 0);
    }

    #[test]
    fn test_atr_stop_uses_signal_day_history() {
        let mut params = HashMap::new();
        params.insert("stopLossMode".to_string(), 1.0);
        params.insert("atrMultiplier".to_string(), 1.0);
        params.insert("atrPeriod".to_string(), 3.0);
        let engine = Engine::from_parameters(&params, test_runtime_settings());

        let ticker = "ATR".to_string();
        let mut candles = Vec::new();
        let mut unique_dates = Vec::new();
        let base_definitions = [
            (100.0, 110.0, 90.0, 100.0),
            (101.0, 112.0, 102.0, 105.0),
            (103.0, 113.0, 103.0, 110.0),
            (130.0, 160.0, 100.0, 150.0),
            (131.0, 135.0, 120.0, 130.0),
        ];
        let history_offset = engine
            .runtime_settings
            .minimum_dollar_volume_lookback
            .saturating_sub(1);
        let mut definitions: Vec<(f64, f64, f64, f64)> = Vec::new();
        definitions.extend(std::iter::repeat(base_definitions[0]).take(history_offset));
        definitions.extend(base_definitions);

        for (i, (open, high, low, close)) in definitions.iter().enumerate() {
            let date = create_date(i as i64);
            unique_dates.push(date);
            candles.push(Candle {
                ticker: ticker.clone(),
                date,
                open: *open,
                high: *high,
                low: *low,
                close: *close,
                unadjusted_close: Some(*close),
                volume_shares: 10_000_000,
            });
        }

        let mut signals = HashMap::new();
        signals.insert(
            (ticker.clone(), unique_dates[history_offset + 2]),
            StrategySignal {
                action: SignalAction::Buy,
                confidence: 1.0,
            },
        );
        let strategy = MockStrategy { signals };

        let BacktestRun { result, .. } = engine
            .backtest(
                Some(&strategy),
                strategy.get_template_id(),
                &[ticker.clone()],
                &candles,
                &unique_dates,
                None,
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.trades.len(), 1);
        let trade = &result.trades[0];
        assert!(trade.stop_loss.is_some());

        let atr_components = [20.0, 12.0, 10.0];
        let expected_atr = atr_components.iter().sum::<f64>() / atr_components.len() as f64;
        let entry_candle = &candles[history_offset + 3];
        let entry_price =
            engine.apply_entry_slippage_with_candle(entry_candle.open, false, entry_candle);
        let expected_stop = entry_price - expected_atr;
        assert!(
            (trade.stop_loss.unwrap() - expected_stop).abs() < 1e-6,
            "expected stop {:.6}, got {:.6}",
            expected_stop,
            trade.stop_loss.unwrap()
        );
    }

    #[test]
    fn test_update_active_trades_skip_future_entries() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "FUT".to_string();
        let (candles, unique_dates) = generate_candles(&ticker, vec![100.0, 95.0, 90.0]);
        let mut candles_by_ticker: HashMap<String, Vec<&Candle>> = HashMap::new();
        candles_by_ticker.insert(ticker.clone(), candles.iter().collect());

        let future_entry_date = unique_dates[2];
        let mut active_trades = vec![Trade {
            id: "future-trade".to_string(),
            strategy_id: "test".to_string(),
            ticker: ticker.clone(),
            quantity: 10,
            price: 110.0,
            date: future_entry_date,
            status: TradeStatus::Active,
            pnl: None,
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss: Some(105.0),
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        }];
        let mut closed_trades = Vec::new();
        let mut cash = 0.0;

        engine.update_active_trades(
            &mut active_trades,
            &mut closed_trades,
            &mut cash,
            &candles_by_ticker,
            unique_dates[0],
        );

        assert!(
            active_trades[0].exit_date.is_none(),
            "trade should not close before reaching its entry date"
        );
        assert!(closed_trades.is_empty(), "no trades should close early");
        assert_eq!(cash, 0.0, "cash should remain unchanged before entry");
    }

    #[test]
    fn test_validate_trades_rejects_exit_before_entry() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "BAD".to_string();
        let (candles, unique_dates) = generate_candles(&ticker, vec![100.0, 105.0]);
        let mut candles_by_ticker: HashMap<String, Vec<&Candle>> = HashMap::new();
        candles_by_ticker.insert(ticker.clone(), candles.iter().collect());

        let trade = Trade {
            id: "exit-before-entry".to_string(),
            strategy_id: "test".to_string(),
            ticker: ticker.clone(),
            quantity: 10,
            price: candles[0].open,
            date: unique_dates[0],
            status: TradeStatus::Closed,
            pnl: Some(0.0),
            fee: None,
            exit_price: Some(candles[0].close),
            exit_date: Some(unique_dates[0] - Duration::days(1)),
            stop_loss: None,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        };

        assert!(engine
            .validate_trades(&[trade], &candles_by_ticker, unique_dates[1])
            .is_err());
    }

    #[test]
    fn test_validate_trades_rejects_impossible_prices() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "IMP".to_string();
        let (candles, unique_dates) = generate_candles(&ticker, vec![100.0, 110.0]);
        let mut candles_by_ticker: HashMap<String, Vec<&Candle>> = HashMap::new();
        candles_by_ticker.insert(ticker.clone(), candles.iter().collect());

        let trade = Trade {
            id: "bad-price".to_string(),
            strategy_id: "test".to_string(),
            ticker: ticker.clone(),
            quantity: 5,
            price: candles[0].high + 5.0,
            date: unique_dates[0],
            status: TradeStatus::Active,
            pnl: None,
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss: None,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        };

        assert!(engine
            .validate_trades(&[trade], &candles_by_ticker, unique_dates[1])
            .is_err());
    }

    #[test]
    fn test_validate_trades_rejects_bad_closed_pnl() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "PNL".to_string();
        let (candles, unique_dates) = generate_candles(&ticker, vec![100.0, 120.0]);
        let mut candles_by_ticker: HashMap<String, Vec<&Candle>> = HashMap::new();
        candles_by_ticker.insert(ticker.clone(), candles.iter().collect());

        let trade = Trade {
            id: "bad-pnl-closed".to_string(),
            strategy_id: "test".to_string(),
            ticker: ticker.clone(),
            quantity: 5,
            price: candles[0].open,
            date: unique_dates[0],
            status: TradeStatus::Closed,
            pnl: Some(10.0),
            fee: None,
            exit_price: Some(candles[1].close),
            exit_date: Some(unique_dates[1]),
            stop_loss: None,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        };

        assert!(engine
            .validate_trades(&[trade], &candles_by_ticker, unique_dates[1])
            .is_err());
    }

    #[test]
    fn test_validate_trades_rejects_bad_active_pnl() {
        let engine = Engine::new(test_runtime_settings());
        let ticker = "APNL".to_string();
        let (candles, unique_dates) = generate_candles(&ticker, vec![100.0, 110.0]);
        let mut candles_by_ticker: HashMap<String, Vec<&Candle>> = HashMap::new();
        candles_by_ticker.insert(ticker.clone(), candles.iter().collect());

        let trade = Trade {
            id: "bad-pnl-active".to_string(),
            strategy_id: "test".to_string(),
            ticker: ticker.clone(),
            quantity: 5,
            price: candles[0].open,
            date: unique_dates[0],
            status: TradeStatus::Active,
            pnl: Some(999.0),
            fee: None,
            exit_price: None,
            exit_date: None,
            stop_loss: None,
            stop_loss_triggered: Some(false),
            entry_order_id: None,
            entry_cancel_after: None,
            stop_order_id: None,
            exit_order_id: None,
            changes: Vec::new(),
        };

        assert!(engine
            .validate_trades(&[trade], &candles_by_ticker, unique_dates[1])
            .is_err());
    }
}
