use crate::config::{resolve_backtest_initial_capital, EngineRuntimeSettings};
use crate::data_context::{MarketData, TickerScope};
use crate::database::Database;
use crate::engine::Engine;
use crate::models::{
    AccountSignalSkip, BacktestResult, GeneratedSignal, StrategyConfig, StrategyStateSnapshot,
};
use crate::retry::retry_db_operation;
use crate::strategy_utils::calculate_period_days_local;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use crossbeam_channel::{bounded, Receiver, Sender};
use log::{info, warn};
use serde_json::json;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const START_TIMING_MAX_HORIZON_TRADING_DAYS: usize = 126;

struct StrategyBacktestTask {
    id: String,
    name: String,
    template_id: String,
    parameters: HashMap<String, f64>,
    signals: Vec<GeneratedSignal>,
    start_date_override: chrono::DateTime<chrono::Utc>,
    months_filter: Option<i64>,
    existing_backtest: Option<BacktestResult>,
    account_id: Option<String>,
}

struct CompletedBacktestPayload {
    result: BacktestResult,
    signal_skips: Vec<AccountSignalSkip>,
}

struct StrategyBacktestResultMsg {
    id: String,
    name: String,
    template_id: String,
    duration_minutes: f64,
    run: StdResult<CompletedBacktestPayload, String>,
    months_filter: Option<i64>,
    account_id: Option<String>,
}

struct StartTimingBacktestTask {
    start_date: DateTime<Utc>,
    template_id: String,
    parameters: HashMap<String, f64>,
    signals: Vec<GeneratedSignal>,
    tickers: Vec<String>,
    sample_dates: Vec<DateTime<Utc>>,
}

struct StartTimingBacktestResultMsg {
    start_date: DateTime<Utc>,
    duration_minutes: f64,
    run: StdResult<BacktestResult, String>,
}

struct StrategyBacktestSuccess {
    id: String,
    name: String,
    duration_minutes: f64,
    run: BacktestResult,
    months_filter: Option<i64>,
    account_id: Option<String>,
    signal_skips: Vec<AccountSignalSkip>,
}

fn strategy_has_linked_account(strategy: &StrategyConfig) -> bool {
    strategy
        .account_id
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

pub struct ActiveStrategyBacktester<'a> {
    db: &'a mut Database,
    data: &'a MarketData,
    backtested_strategy_ids: &'a mut HashSet<String>,
    ticker_scope: TickerScope,
}

impl<'a> ActiveStrategyBacktester<'a> {
    pub fn new(
        db: &'a mut Database,
        data: &'a MarketData,
        backtested_strategy_ids: &'a mut HashSet<String>,
        ticker_scope: TickerScope,
    ) -> Self {
        Self {
            db,
            data,
            backtested_strategy_ids,
            ticker_scope,
        }
    }

    pub async fn run_with_selection(
        &mut self,
        months: Option<u32>,
        has_account: bool,
    ) -> Result<()> {
        if !self.data.has_data() {
            warn!("No market data available to run backtests.");
            return Ok(());
        }

        if let Some(month_value) = months {
            if month_value == 0 {
                return Err(anyhow!("Months argument must be greater than zero"));
            }
        }

        let strategies = self.db.get_active_strategies().await?;
        let strategies: Vec<_> = strategies
            .into_iter()
            .filter(|strategy| strategy_has_linked_account(strategy) == has_account)
            .collect();
        if strategies.is_empty() {
            let selection_label = if has_account {
                "account-linked"
            } else {
                "unlinked active"
            };
            info!("No {} strategies found in the database.", selection_label);
            return Ok(());
        }

        let earliest_available = *self
            .data
            .unique_dates()
            .first()
            .expect("Checked unique_dates is not empty");
        let latest_available = *self
            .data
            .unique_dates()
            .last()
            .expect("Checked unique_dates is not empty");
        let earliest_account_start = strategies
            .iter()
            .filter_map(|strategy| strategy.backtest_start_date)
            .map(|date| {
                if date < earliest_available {
                    earliest_available
                } else {
                    date
                }
            })
            .min();

        let (unique_dates_window, all_candles_window, default_period_start) =
            if let Some(month_value) = months {
                let hours = ((month_value as f64) * 30.4 * 24.0).ceil() as i64;
                let candidate_start = latest_available - Duration::hours(hours);
                let adjusted_start = if candidate_start < earliest_available {
                    earliest_available
                } else {
                    candidate_start
                };
                let forced_start = match earliest_account_start {
                    Some(custom_start) if custom_start < adjusted_start => custom_start,
                    _ => adjusted_start,
                };
                let filtered_dates: Vec<_> = self
                    .data
                    .unique_dates()
                    .iter()
                    .filter(|&date| *date >= forced_start)
                    .cloned()
                    .collect();
                (
                    Arc::new(filtered_dates),
                    // Keep pre-window candles available for entry-volume, sizing, and stop lookbacks.
                    self.data.all_candles_arc(),
                    adjusted_start,
                )
            } else {
                (
                    self.data.unique_dates_arc(),
                    self.data.all_candles_arc(),
                    earliest_available,
                )
            };

        if unique_dates_window.is_empty() {
            warn!("Selected backtest window does not contain any market data.");
            return Ok(());
        }

        let backtest_window_start = *unique_dates_window.first().unwrap();
        let backtest_window_end = *unique_dates_window.last().unwrap();

        if let Some(month_value) = months {
            info!(
                "Backtesting window constrained to approximately {} month{} ({} to {})",
                month_value,
                if month_value == 1 { "" } else { "s" },
                default_period_start,
                backtest_window_end
            );
        }

        if all_candles_window.is_empty() {
            warn!("Selected backtest window does not contain any candle data.");
            return Ok(());
        }

        let global_months_filter = months.map(|value| value as i64);
        let period_days = {
            let start_date = default_period_start.date_naive();
            let end_date = backtest_window_end.date_naive();
            if end_date < start_date {
                0
            } else {
                let diff = (end_date - start_date).num_days();
                if diff <= 0 {
                    1
                } else {
                    diff
                }
            }
        };
        let default_lookup_period_months = global_months_filter.unwrap_or_else(|| {
            if period_days <= 0 {
                0
            } else {
                ((period_days as f64) / 30.4).round() as i64
            }
        });

        type RunnableStrategy = (
            StrategyConfig,
            chrono::DateTime<chrono::Utc>,
            Option<i64>,
            Option<BacktestResult>,
        );
        let mut runnable_strategies: Vec<RunnableStrategy> = Vec::new();
        let mut skipped_strategies = 0usize;

        for strategy in strategies {
            let has_linked_account = strategy_has_linked_account(&strategy);
            let is_account_strategy = strategy.backtest_start_date.is_some() || has_linked_account;
            if is_account_strategy
                && months.is_some()
                && self.backtested_strategy_ids.contains(&strategy.id)
            {
                skipped_strategies += 1;
                continue;
            }

            let mut effective_start = strategy.backtest_start_date.unwrap_or(default_period_start);
            let mut first_trade_date: Option<DateTime<Utc>> = None;
            if has_linked_account {
                let first_filled_trade = self
                    .db
                    .get_strategy_first_filled_trade_date(&strategy.id)
                    .await?;
                let Some(first_filled_trade) = first_filled_trade else {
                    info!(
                        "Skipping account strategy {} because no filled trades are available yet",
                        strategy.name
                    );
                    skipped_strategies += 1;
                    continue;
                };
                first_trade_date = Some(first_filled_trade);
                // Shift back one trading day so signals can open the first live entry date.
                let trade_index = match unique_dates_window.binary_search(&first_filled_trade) {
                    Ok(idx) => idx,
                    Err(idx) => idx.saturating_sub(1),
                };
                let signal_index = trade_index.saturating_sub(1);
                effective_start = *unique_dates_window.get(signal_index).unwrap_or_else(|| {
                    unique_dates_window
                        .first()
                        .expect("unique_dates_window is not empty")
                });
            }
            if let Some(first_trade_date) = first_trade_date {
                if first_trade_date < backtest_window_start {
                    info!(
                        "Skipping account strategy {} because its first filled trade date {} is before available data starting at {}",
                        strategy.name, first_trade_date, backtest_window_start
                    );
                    skipped_strategies += 1;
                    continue;
                }
            }
            if effective_start < backtest_window_start {
                if has_linked_account {
                    info!(
                        "Skipping account strategy {} because its first filled trade date {} is before available data starting at {}",
                        strategy.name, effective_start, backtest_window_start
                    );
                    skipped_strategies += 1;
                    continue;
                }
                effective_start = backtest_window_start;
            }
            if effective_start > backtest_window_end {
                info!(
                    "Skipping strategy {} because the start date {} is after available data ending at {}",
                    strategy.name, effective_start, backtest_window_end
                );
                self.db
                    .persist_strategy_event(
                        &strategy.id,
                        "warn",
                        "Skipped backtest (start date beyond available data)",
                        json!({
                            "operation": "backtest",
                            "reason": "start_after_available_data",
                            "requestedStart": effective_start,
                            "windowEnd": backtest_window_end,
                        }),
                    )
                    .await;
                skipped_strategies += 1;
                continue;
            }

            let months_filter_for_strategy = if is_account_strategy {
                None
            } else {
                Some(default_lookup_period_months)
            };

            let latest_end_date = self
                .db
                .get_latest_backtest_end_date(
                    &strategy.id,
                    months_filter_for_strategy,
                    self.ticker_scope.result_label(),
                )
                .await?;
            if let Some(existing_end) = latest_end_date {
                if existing_end >= backtest_window_end {
                    skipped_strategies += 1;
                    continue;
                }
            }

            let mut existing_backtest: Option<BacktestResult> = None;
            if is_account_strategy {
                if let Some(existing_end) = latest_end_date {
                    let latest_result = self
                        .db
                        .load_latest_backtest_result(
                            &strategy.id,
                            months_filter_for_strategy,
                            self.ticker_scope.result_label(),
                        )
                        .await?;
                    if let Some(result) = latest_result {
                        if result.end_date != existing_end {
                            warn!(
                                "Latest backtest result for {} ends at {}, but tracking indicated {}. Falling back to full rerun.",
                                strategy.name, result.end_date, existing_end
                            );
                        } else if result.start_date > effective_start {
                            warn!(
                                "Existing backtest for {} starts at {} which is after requested start {}; performing full rerun.",
                                strategy.name, result.start_date, effective_start
                            );
                        } else {
                            let resume_start = result.end_date + Duration::days(1);
                            info!(
                                "Resuming backtest for {} from {} (previous end {})",
                                strategy.name, resume_start, result.end_date
                            );
                            existing_backtest = Some(result);
                            if let Some(existing) = &existing_backtest {
                                self.db
                                    .persist_strategy_event(
                                        &strategy.id,
                                        "info",
                                        "Resuming backtest from last stored end date",
                                        json!({
                                            "operation": "backtest",
                                            "reason": "resume_partial_backtest",
                                            "resumeStart": resume_start,
                                            "previousEnd": existing.end_date,
                                        }),
                                    )
                                    .await;
                            }
                        }
                    }
                }
            }

            runnable_strategies.push((
                strategy,
                effective_start,
                months_filter_for_strategy,
                existing_backtest,
            ));
        }

        if runnable_strategies.is_empty() {
            info!("All active strategies already have up-to-date backtests; skipping run");
            return Ok(());
        }

        if skipped_strategies > 0 {
            info!(
                "Skipping {} backtest{} with no new candles",
                skipped_strategies,
                if skipped_strategies == 1 { "" } else { "s" }
            );
        }

        let total = runnable_strategies.len();

        let num_workers = std::cmp::min(total, std::cmp::max(1, num_cpus::get()));
        info!(
            "Using {} worker threads for active strategy backtests",
            num_workers
        );

        let (task_tx, task_rx): (Sender<StrategyBacktestTask>, Receiver<StrategyBacktestTask>) =
            bounded(total);
        let (result_tx, result_rx): (
            Sender<StrategyBacktestResultMsg>,
            Receiver<StrategyBacktestResultMsg>,
        ) = bounded(total);

        let ticker_universe = self.data.tickers_arc();
        let ticker_expense_map = self.data.ticker_expense_map_arc();
        let runtime_settings = EngineRuntimeSettings::from_settings_map(self.data.settings())?;
        let backtest_initial_capital = resolve_backtest_initial_capital(self.data.settings());
        let mut handles = Vec::new();
        for _ in 0..num_workers {
            let rx = task_rx.clone();
            let result_tx = result_tx.clone();
            let all_candles = all_candles_window.clone();
            let unique_dates = unique_dates_window.clone();
            let tickers = ticker_universe.clone();
            let expense_map = ticker_expense_map.clone();
            let runtime_settings = runtime_settings.clone();

            let handle = thread::spawn(move || {
                while let Ok(task) = rx.recv() {
                    let StrategyBacktestTask {
                        id,
                        name,
                        template_id,
                        parameters,
                        signals,
                        start_date_override,
                        months_filter,
                        existing_backtest,
                        account_id,
                    } = task;
                    let start = Instant::now();
                    let run_result: StdResult<CompletedBacktestPayload, String> = {
                        let mut engine =
                            Engine::from_parameters(&parameters, runtime_settings.clone());
                        engine.set_ticker_expense_map(expense_map.clone());
                        let filtered_tickers = if signals.is_empty() {
                            None
                        } else {
                            let mut unique = BTreeSet::new();
                            for signal in &signals {
                                unique.insert(signal.ticker.clone());
                            }
                            Some(unique.into_iter().collect::<Vec<String>>())
                        };
                        let tickers_slice: &[String] = if let Some(ref list) = filtered_tickers {
                            list.as_slice()
                        } else {
                            tickers.as_slice()
                        };
                        let provided_signals = Some(signals.as_slice());
                        let result = engine.backtest(
                            None,
                            &template_id,
                            tickers_slice,
                            all_candles.as_slice(),
                            unique_dates.as_slice(),
                            provided_signals,
                            Some(start_date_override),
                            existing_backtest.as_ref(),
                        );
                        match result {
                            Ok(run) => Ok(CompletedBacktestPayload {
                                result: run.result,
                                signal_skips: run.signal_skips,
                            }),
                            Err(e) => Err(e.to_string()),
                        }
                    };
                    let duration_minutes = start.elapsed().as_secs_f64() / 60.0;

                    let message = StrategyBacktestResultMsg {
                        id,
                        name,
                        template_id,
                        duration_minutes,
                        run: run_result,
                        months_filter,
                        account_id,
                    };

                    if result_tx.send(message).is_err() {
                        break;
                    }
                }
            });
            handles.push(handle);
        }

        for (strategy, effective_start, months_filter, existing_backtest) in runnable_strategies {
            let has_linked_account = strategy_has_linked_account(&strategy);
            let signal_end = if has_linked_account && unique_dates_window.len() > 1 {
                unique_dates_window[unique_dates_window.len() - 2]
            } else {
                backtest_window_end
            };
            let signals = self
                .db
                .get_signals_for_strategy_in_range(&strategy.id, effective_start, signal_end)
                .await?;
            let mut parameters = strategy.parameters.clone();
            if !has_linked_account {
                parameters.insert("initialCapital".to_string(), backtest_initial_capital);
            }
            task_tx.send(StrategyBacktestTask {
                id: strategy.id.clone(),
                name: strategy.name.clone(),
                template_id: strategy.template_id.clone(),
                parameters,
                signals,
                start_date_override: effective_start,
                months_filter,
                existing_backtest,
                account_id: strategy.account_id.clone(),
            })?;
        }
        drop(task_tx);

        let mut completed_runs = 0usize;
        let mut failures: Vec<String> = Vec::new();
        let mut pending_persistence: Vec<StrategyBacktestSuccess> = Vec::new();

        while completed_runs < total {
            match result_rx.recv() {
                Ok(message) => {
                    completed_runs += 1;
                    match message.run {
                        Ok(payload) => {
                            let calmar_ratio = payload.result.performance.calmar_ratio;
                            let sharpe = payload.result.performance.sharpe_ratio;
                            info!(
                                "Completed backtest for {} (Calmar {:.4}, Sharpe {:.4}, {:.1}m)",
                                message.name, calmar_ratio, sharpe, message.duration_minutes
                            );
                            let success = StrategyBacktestSuccess {
                                id: message.id,
                                name: message.name,
                                duration_minutes: message.duration_minutes,
                                run: payload.result,
                                months_filter: message.months_filter,
                                account_id: message.account_id.clone(),
                                signal_skips: payload.signal_skips,
                            };
                            pending_persistence.push(success);
                        }
                        Err(error) => {
                            warn!(
                                "Backtest failed for strategy {} ({}): {}",
                                message.id, message.template_id, error
                            );
                            let error_for_log = error.clone();
                            self.db
                                .persist_strategy_event(
                                    &message.id,
                                    "error",
                                    "Backtest failed",
                                    json!({
                                            "operation": "backtest",
                                            "reason": "engine_error",
                                            "templateId": message.template_id,
                                            "error": error_for_log,
                                    }),
                                )
                                .await;
                            failures.push(format!("{} ({})", message.id, error));
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }

        for handle in handles {
            let _ = handle.join();
        }

        let total_successes = pending_persistence.len();
        if total_successes > 0 {
            info!(
                "Persisting {} backtest result{} sequentially",
                total_successes,
                if total_successes == 1 { "" } else { "s" }
            );
        }
        for success in pending_persistence {
            match self.persist_backtest_success(success).await {
                Some(error) => {
                    failures.push(error);
                }
                None => {}
            }
        }

        if !failures.is_empty() {
            warn!(
                "Backtesting completed with {} failure{}",
                failures.len(),
                if failures.len() == 1 { "" } else { "s" }
            );
        }

        Ok(())
    }

    pub async fn run_start_timing_samples(
        &mut self,
        strategy_id: &str,
        weeks: usize,
    ) -> Result<usize> {
        if !self.data.has_data() {
            warn!("No market data available to run start timing backtests.");
            return Ok(0);
        }

        let strategy = self
            .db
            .get_strategy_config(strategy_id)
            .await?
            .ok_or_else(|| anyhow!("Strategy {} not found", strategy_id))?;

        let unique_dates = self.data.unique_dates().to_vec();

        let scope_label = format!("timing_{}", self.ticker_scope.result_label());
        let existing_sample_ranges = self
            .db
            .get_start_timing_backtest_ranges(strategy_id, &scope_label)
            .await?;
        let candidates = build_start_timing_sample_candidates(
            unique_dates.as_slice(),
            weeks,
            &existing_sample_ranges,
        );

        if candidates.is_empty() {
            info!(
                "No missing start timing samples for strategy {} in {} scope",
                strategy_id, scope_label
            );
            return Ok(0);
        }

        let runtime_settings = EngineRuntimeSettings::from_settings_map(self.data.settings())?;
        let backtest_initial_capital = resolve_backtest_initial_capital(self.data.settings());
        let ticker_expense_map = self.data.ticker_expense_map_arc();
        let mut tasks = Vec::new();

        for (start_date, start_index, end_index) in candidates {
            let signal_end = unique_dates[end_index];
            let signals = self
                .db
                .get_signals_for_strategy_in_range(strategy_id, start_date, signal_end)
                .await?;
            if signals.is_empty() {
                warn!(
                    "Skipping start timing sample for {} at {} because no signals were found",
                    strategy.name, start_date
                );
                continue;
            }

            let mut parameters = strategy.parameters.clone();
            parameters.insert("initialCapital".to_string(), backtest_initial_capital);

            let tickers = collect_signal_tickers(&signals);
            let sample_dates = unique_dates[start_index..=end_index].to_vec();
            tasks.push(StartTimingBacktestTask {
                start_date,
                template_id: strategy.template_id.clone(),
                parameters,
                signals,
                tickers,
                sample_dates,
            });
        }

        if tasks.is_empty() {
            info!(
                "No start timing samples with signals for strategy {} in {} scope",
                strategy_id, scope_label
            );
            return Ok(0);
        }

        let total = tasks.len();
        let num_workers = std::cmp::min(total, std::cmp::max(1, num_cpus::get()));
        info!(
            "Using {} worker threads for {} start timing sample{}",
            num_workers,
            total,
            if total == 1 { "" } else { "s" }
        );

        let (task_tx, task_rx): (
            Sender<StartTimingBacktestTask>,
            Receiver<StartTimingBacktestTask>,
        ) = bounded(total);
        let (result_tx, result_rx): (
            Sender<StartTimingBacktestResultMsg>,
            Receiver<StartTimingBacktestResultMsg>,
        ) = bounded(total);

        let all_candles = self.data.all_candles_arc();
        let mut handles = Vec::new();
        for _ in 0..num_workers {
            let rx = task_rx.clone();
            let result_tx = result_tx.clone();
            let all_candles = all_candles.clone();
            let ticker_expense_map = ticker_expense_map.clone();
            let runtime_settings = runtime_settings.clone();

            let handle = thread::spawn(move || {
                while let Ok(task) = rx.recv() {
                    let StartTimingBacktestTask {
                        start_date,
                        template_id,
                        parameters,
                        signals,
                        tickers,
                        sample_dates,
                    } = task;
                    let start = Instant::now();
                    let run = {
                        let mut engine =
                            Engine::from_parameters(&parameters, runtime_settings.clone());
                        engine.set_ticker_expense_map(ticker_expense_map.clone());
                        engine
                            .backtest(
                                None,
                                &template_id,
                                tickers.as_slice(),
                                all_candles.as_slice(),
                                sample_dates.as_slice(),
                                Some(signals.as_slice()),
                                Some(start_date),
                                None,
                            )
                            .map(|run| run.result)
                            .map_err(|error| error.to_string())
                    };
                    let duration_minutes = start.elapsed().as_secs_f64() / 60.0;

                    if result_tx
                        .send(StartTimingBacktestResultMsg {
                            start_date,
                            duration_minutes,
                            run,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            handles.push(handle);
        }

        for task in tasks {
            task_tx.send(task)?;
        }
        drop(task_tx);

        let mut completed_runs = 0usize;
        let mut failures = Vec::new();
        let mut pending_persistence: Vec<(DateTime<Utc>, f64, BacktestResult)> = Vec::new();

        while completed_runs < total {
            match result_rx.recv() {
                Ok(message) => {
                    completed_runs += 1;
                    match message.run {
                        Ok(result) => {
                            info!(
                                "Completed start timing sample for {} at {} ({:.1}m)",
                                strategy.name, message.start_date, message.duration_minutes
                            );
                            pending_persistence.push((
                                message.start_date,
                                message.duration_minutes,
                                result,
                            ));
                        }
                        Err(error) => {
                            warn!(
                                "Start timing sample failed for {} at {}: {}",
                                strategy.name, message.start_date, error
                            );
                            let error_for_log = error.clone();
                            self.db
                                .persist_strategy_event(
                                    strategy_id,
                                    "error",
                                    "Start timing backtest sample failed",
                                    json!({
                                        "operation": "start_timing_backtest",
                                        "requestedStart": message.start_date,
                                        "scope": scope_label,
                                        "error": error_for_log,
                                    }),
                                )
                                .await;
                            failures.push(format!("{} ({})", message.start_date, error));
                        }
                    }
                }
                Err(_) => break,
            }
        }

        for handle in handles {
            let _ = handle.join();
        }

        let mut completed = 0usize;
        for (start_date, duration_minutes, mut result) in pending_persistence {
            result.id = build_start_timing_backtest_id(strategy_id, &scope_label, start_date);
            result.strategy_id = strategy.id.clone();
            result.ticker_scope = Some(scope_label.clone());
            result.strategy_state = Some(StrategyStateSnapshot {
                template_id: "start_timing".to_string(),
                data: json!({
                    "source": "horizon_limited_start_timing",
                    "requestedStart": start_date,
                    "maxHorizonTradingDays": START_TIMING_MAX_HORIZON_TRADING_DAYS,
                    "scope": self.ticker_scope.result_label(),
                }),
            });

            self.db
                .upsert_start_timing_backtest(strategy_id, &result, &scope_label)
                .await?;
            self.db
                .persist_strategy_event(
                    strategy_id,
                    "info",
                    "Start timing backtest sample completed",
                    json!({
                        "operation": "start_timing_backtest",
                        "requestedStart": start_date,
                        "actualStart": result.start_date,
                        "endDate": result.end_date,
                        "maxHorizonTradingDays": START_TIMING_MAX_HORIZON_TRADING_DAYS,
                        "scope": scope_label,
                        "totalTrades": result.performance.total_trades,
                        "initialCapital": result.initial_capital,
                        "finalPortfolioValue": result.final_portfolio_value,
                        "durationMinutes": duration_minutes,
                    }),
                )
                .await;

            completed += 1;
        }

        if !failures.is_empty() {
            warn!(
                "Start timing completed with {} failure{}",
                failures.len(),
                if failures.len() == 1 { "" } else { "s" }
            );
        }

        Ok(completed)
    }

    async fn persist_backtest_success(
        &mut self,
        success: StrategyBacktestSuccess,
    ) -> Option<String> {
        use crate::models::generate_trade_id;

        let StrategyBacktestSuccess {
            id,
            name,
            duration_minutes,
            mut run,
            months_filter,
            account_id,
            signal_skips,
        } = success;

        run.ticker_scope = Some(self.ticker_scope.result_label().to_string());
        run.strategy_id = id.clone();
        for trade in run.trades.iter_mut() {
            trade.strategy_id = id.clone();
            let base_id = generate_trade_id(&id, &run.id, &trade.ticker, trade.date);
            if let Some(suffix_index) = trade.id.find("-partial-") {
                let suffix = &trade.id[suffix_index..];
                trade.id = format!("{}{}", base_id, suffix);
            } else {
                trade.id = base_id.clone();
            }
            if trade.stop_loss_triggered.is_none() {
                trade.stop_loss_triggered = Some(false);
            }
        }

        let persist_context = format!("persisting backtest results for strategy {}", id);
        if let Err(error) = retry_db_operation!(persist_context, async {
            self.db
                .replace_strategy_backtest_data(
                    &id,
                    &run,
                    months_filter,
                    self.ticker_scope.result_label(),
                )
                .await
        }) {
            warn!(
                "Failed to persist backtest results for strategy {}: {}",
                id, error
            );
            return Some(format!("{} ({})", id, error));
        }

        if duration_minutes.is_finite() {
            if let Err(error) = self
                .db
                .update_strategy_backtest_duration(&id, duration_minutes)
                .await
            {
                warn!(
                    "Failed to update backtest duration for strategy {}: {}",
                    id, error
                );
            }
        }

        if let Some(account_id) = account_id.as_deref() {
            if !signal_skips.is_empty() {
                if let Err(error) = self
                    .db
                    .insert_account_signal_skips(&id, Some(account_id), "backtest", &signal_skips)
                    .await
                {
                    warn!(
                        "Failed to persist backtest signal skips for strategy {}: {}",
                        id, error
                    );
                }
            }
        }

        if months_filter.is_none() {
            self.backtested_strategy_ids.insert(id.clone());
        }

        let trade_count = run.performance.total_trades;
        let log_level = if trade_count == 0 { "warn" } else { "info" };
        let log_message = if trade_count == 0 {
            "Backtest completed without generating trades".to_string()
        } else {
            format!(
                "Backtest completed with {} trade{} (Calmar {:.2}, Sharpe {:.2})",
                trade_count,
                if trade_count == 1 { "" } else { "s" },
                run.performance.calmar_ratio,
                run.performance.sharpe_ratio
            )
        };
        let duration_minutes_value = if duration_minutes.is_finite() {
            Some(duration_minutes)
        } else {
            None
        };
        self.db
            .persist_strategy_event(
                &id,
                log_level,
                log_message,
                json!({
                    "operation": "backtest",
                    "startDate": run.start_date,
                    "endDate": run.end_date,
                    "periodDays": calculate_period_days_local(&run.start_date, &run.end_date),
                    "monthsFilter": months_filter,
                    "durationMinutes": duration_minutes_value,
                    "totalTrades": trade_count,
                    "tickersTested": run.tickers.len(),
                    "initialCapital": run.initial_capital,
                    "finalPortfolioValue": run.final_portfolio_value,
                    "sharpe": run.performance.sharpe_ratio,
                    "accountId": account_id,
                }),
            )
            .await;

        info!(
            "Persisted backtest for {} (Calmar {:.4}, Sharpe {:.4})",
            name, run.performance.calmar_ratio, run.performance.sharpe_ratio
        );

        None
    }
}

fn build_weekly_start_candidates(
    unique_dates: &[DateTime<Utc>],
    weeks: usize,
) -> Vec<DateTime<Utc>> {
    if unique_dates.is_empty() || weeks == 0 {
        return Vec::new();
    }

    let latest = *unique_dates.last().expect("unique_dates is not empty");
    let first_target = latest - Duration::days((weeks as i64) * 7);
    let mut candidates = Vec::new();

    for week in 0..weeks {
        let target = first_target + Duration::days((week as i64) * 7);
        let index = match unique_dates.binary_search(&target) {
            Ok(idx) => idx,
            Err(idx) => idx.min(unique_dates.len().saturating_sub(1)),
        };
        if let Some(candidate) = unique_dates.get(index) {
            if candidates.last().copied() != Some(*candidate) {
                candidates.push(*candidate);
            }
        }
    }

    candidates
}

fn build_start_timing_sample_candidates(
    unique_dates: &[DateTime<Utc>],
    weeks: usize,
    existing_sample_ranges: &HashMap<DateTime<Utc>, DateTime<Utc>>,
) -> Vec<(DateTime<Utc>, usize, usize)> {
    if unique_dates.len() < 2 || weeks == 0 {
        return Vec::new();
    }

    let earliest_available = *unique_dates.first().expect("unique_dates is not empty");
    let latest_available = *unique_dates.last().expect("unique_dates is not empty");
    let latest_index = unique_dates.len() - 1;

    build_weekly_start_candidates(unique_dates, weeks)
        .into_iter()
        .filter_map(|date| {
            let start_index = unique_dates.binary_search(&date).ok()?;
            if date < earliest_available || date >= latest_available {
                return None;
            }

            let desired_end_index = start_index
                .saturating_add(START_TIMING_MAX_HORIZON_TRADING_DAYS)
                .min(latest_index);
            if desired_end_index <= start_index {
                return None;
            }

            let desired_end = unique_dates[desired_end_index];
            if existing_sample_ranges
                .get(&date)
                .map(|existing_end| *existing_end >= desired_end)
                .unwrap_or(false)
            {
                return None;
            }

            Some((date, start_index, desired_end_index))
        })
        .collect()
}

fn collect_signal_tickers(signals: &[GeneratedSignal]) -> Vec<String> {
    let mut tickers = BTreeSet::new();
    for signal in signals {
        tickers.insert(signal.ticker.clone());
    }
    tickers.into_iter().collect()
}

fn build_start_timing_backtest_id(
    strategy_id: &str,
    ticker_scope: &str,
    start_date: DateTime<Utc>,
) -> String {
    format!(
        "start_timing_{}_{}_{}",
        strategy_id,
        ticker_scope,
        start_date.format("%Y%m%d")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_date(day_offset: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("valid test date")
            .with_timezone(&Utc)
            + Duration::days(day_offset)
    }

    fn test_dates(count: i64) -> Vec<DateTime<Utc>> {
        (0..count).map(test_date).collect()
    }

    #[test]
    fn includes_recent_partial_start_timing_candidates() {
        let dates = test_dates(160);
        let candidates = build_start_timing_sample_candidates(&dates, 13, &HashMap::new());

        let last = candidates.last().expect("expected recent candidate");
        assert_eq!(last.0, test_date(152));
        assert_eq!(last.1, 152);
        assert_eq!(last.2, 159);
    }

    #[test]
    fn reruns_recent_partial_start_timing_candidates_until_current() {
        let dates = test_dates(160);
        let mut existing_sample_ranges = HashMap::new();
        existing_sample_ranges.insert(test_date(152), test_date(158));

        let candidates = build_start_timing_sample_candidates(&dates, 13, &existing_sample_ranges);
        assert!(candidates
            .iter()
            .any(|(date, _, _)| *date == test_date(152)));

        existing_sample_ranges.insert(test_date(152), test_date(159));
        let candidates = build_start_timing_sample_candidates(&dates, 13, &existing_sample_ranges);
        assert!(!candidates
            .iter()
            .any(|(date, _, _)| *date == test_date(152)));
    }
}
