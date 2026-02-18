use crate::config::{resolve_backtest_initial_capital, EngineRuntimeSettings};
use crate::data_context::{MarketData, TickerScope};
use crate::database::Database;
use crate::engine::Engine;
use crate::models::{AccountSignalSkip, BacktestResult, GeneratedSignal, StrategyConfig};
use crate::optimizer_status::OptimizerStatus;
use crate::retry::retry_db_operation;
use crate::strategy_utils::calculate_period_days_local;
use anyhow::{anyhow, Result};
use chrono::Duration;
use crossbeam_channel::{bounded, Receiver, Sender};
use log::{info, warn};
use serde_json::json;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

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

struct StrategyBacktestSuccess {
    id: String,
    name: String,
    duration_minutes: f64,
    run: BacktestResult,
    months_filter: Option<i64>,
    account_id: Option<String>,
    signal_skips: Vec<AccountSignalSkip>,
}

#[derive(Clone, Copy)]
pub enum StrategySelection {
    All,
    AccountLinkedOnly,
    WithoutAccounts,
}

impl StrategySelection {
    fn description(self) -> &'static str {
        match self {
            StrategySelection::All => "active",
            StrategySelection::AccountLinkedOnly => "account-linked",
            StrategySelection::WithoutAccounts => "unlinked active",
        }
    }

    fn matches(self, strategy: &StrategyConfig) -> bool {
        match self {
            StrategySelection::All => true,
            StrategySelection::AccountLinkedOnly => strategy_has_linked_account(strategy),
            StrategySelection::WithoutAccounts => !strategy_has_linked_account(strategy),
        }
    }
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
    status: &'a OptimizerStatus,
    data: &'a MarketData,
    backtested_strategy_ids: &'a mut HashSet<String>,
    ticker_scope: TickerScope,
}

impl<'a> ActiveStrategyBacktester<'a> {
    pub fn new(
        db: &'a mut Database,
        status: &'a OptimizerStatus,
        data: &'a MarketData,
        backtested_strategy_ids: &'a mut HashSet<String>,
        ticker_scope: TickerScope,
    ) -> Self {
        Self {
            db,
            status,
            data,
            backtested_strategy_ids,
            ticker_scope,
        }
    }

    pub async fn run_with_selection(
        &mut self,
        months: Option<u32>,
        selection: StrategySelection,
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
            .filter(|strategy| selection.matches(strategy))
            .collect();
        if strategies.is_empty() {
            info!(
                "No {} strategies found in the database.",
                selection.description()
            );
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
                let filtered_candles: Vec<_> = self
                    .data
                    .all_candles()
                    .iter()
                    .filter(|&c| c.date >= forced_start)
                    .cloned()
                    .collect();
                (
                    Arc::new(filtered_dates),
                    Arc::new(filtered_candles),
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
                effective_start = first_filled_trade;
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
            self.status
                .set_phase("Active strategies already up-to-date");
            self.status.set_progress(0, 0, 0, None);
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
        self.status.set_phase(format!(
            "Backtesting {} active strategies in parallel",
            total
        ));
        self.status.set_progress(total, 0, 0, None);

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
            let signals = self
                .db
                .get_signals_for_strategy_in_range(
                    &strategy.id,
                    effective_start,
                    backtest_window_end,
                )
                .await?;
            let mut parameters = strategy.parameters.clone();
            if !strategy_has_linked_account(&strategy) {
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
                            self.status.set_phase(format!(
                                "Completed {}/{} strategies (awaiting persistence; last: {})",
                                completed_runs, total, message.name
                            ));
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
                            self.status.set_progress(
                                total,
                                completed_runs,
                                failures.len(),
                                Some(calmar_ratio),
                            );
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
                            self.status.set_phase(format!(
                                "Completed {}/{} strategies (last failure: {})",
                                completed_runs, total, message.name
                            ));
                            self.status
                                .set_progress(total, completed_runs, failures.len(), None);
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
            self.status.set_phase(format!(
                "Persisting {} backtest result{} sequentially",
                total_successes,
                if total_successes == 1 { "" } else { "s" }
            ));
        }
        let mut persisted_successes = 0usize;
        for success in pending_persistence {
            let strategy_name = success.name.clone();
            let calmar_ratio = success.run.performance.calmar_ratio;
            match self.persist_backtest_success(success).await {
                Some(error) => {
                    failures.push(error);
                    self.status.set_phase(format!(
                        "Persisted {}/{} results (last failure: {})",
                        persisted_successes, total_successes, strategy_name
                    ));
                    self.status
                        .set_progress(total, completed_runs, failures.len(), None);
                }
                None => {
                    persisted_successes += 1;
                    self.status.set_phase(format!(
                        "Persisted {}/{} results (last success: {})",
                        persisted_successes, total_successes, strategy_name
                    ));
                    self.status.set_progress(
                        total,
                        completed_runs,
                        failures.len(),
                        Some(calmar_ratio),
                    );
                }
            }
        }

        if failures.is_empty() {
            self.status.set_phase("Backtesting completed successfully");
        } else {
            warn!(
                "Backtesting completed with {} failure{}",
                failures.len(),
                if failures.len() == 1 { "" } else { "s" }
            );
            self.status.set_phase(format!(
                "Completed with {} failure{}",
                failures.len(),
                if failures.len() == 1 { "" } else { "s" }
            ));
        }
        self.status
            .set_progress(total, completed_runs, failures.len(), None);

        Ok(())
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
