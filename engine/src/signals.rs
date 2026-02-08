use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use log::{info, warn};
use serde_json::json;

use crate::data_context::MarketData;
use crate::database::Database;
use crate::models::SignalAction;
use crate::models::{Candle, GeneratedSignal, StrategyConfig};
use crate::retry::retry_db_operation;
use crate::strategy::{create_strategy, Strategy};
use chrono::{DateTime, Utc};

use crate::optimizer_status::OptimizerStatus;

/// Builds a `GeneratedSignal` when the action is tradable and confidence is usable.
/// Returns `None` for non-trading actions or invalid confidence values.
pub fn maybe_create_generated_signal(
    date: DateTime<Utc>,
    ticker: &str,
    action: &SignalAction,
    confidence: f64,
) -> Option<GeneratedSignal> {
    if !matches!(action, SignalAction::Buy | SignalAction::Sell) {
        return None;
    }

    let confidence = if confidence.is_finite() {
        Some(confidence)
    } else {
        None
    };

    Some(GeneratedSignal {
        date,
        ticker: ticker.to_string(),
        action: action.clone(),
        confidence,
    })
}

/// Parameters for generating a signal with filtering
pub struct SignalGenerationParams<'a> {
    pub strategy: &'a dyn Strategy,
    pub ticker: &'a str,
    pub candles: &'a [Candle],
    pub candle_index: usize,
    pub date: DateTime<Utc>,
    pub excluded_tickers: &'a HashSet<String>,
}

pub fn generate_signal_with_filters(params: SignalGenerationParams) -> Option<GeneratedSignal> {
    let SignalGenerationParams {
        strategy,
        ticker,
        candles,
        candle_index,
        date,
        excluded_tickers,
    } = params;

    // Check if ticker is excluded
    if !excluded_tickers.is_empty() {
        let ticker_upper = ticker.to_ascii_uppercase();
        if excluded_tickers.contains(&ticker_upper) {
            return None;
        }
    }

    // Check minimum data requirements
    let min_data_points = strategy.get_min_data_points();
    if candle_index < min_data_points || candle_index >= candles.len() {
        return None;
    }
    // Generate the signal
    let signal = strategy.generate_signal(ticker, &candles[..=candle_index], candle_index);

    // Convert to GeneratedSignal if it's a tradable action
    maybe_create_generated_signal(date, ticker, &signal.action, signal.confidence)
}

pub struct SignalManager<'a> {
    db: &'a mut Database,
    status: &'a OptimizerStatus,
    data: &'a MarketData,
}

impl<'a> SignalManager<'a> {
    pub fn new(db: &'a mut Database, status: &'a OptimizerStatus, data: &'a MarketData) -> Self {
        Self { db, status, data }
    }

    pub async fn generate_missing_signals(&mut self) -> Result<()> {
        let unique_dates = self.data.unique_dates();
        let tickers = self.data.tickers();

        if unique_dates.is_empty() {
            warn!("No candle dates available; skipping signal generation");
            return Ok(());
        }

        let strategies = self.db.get_active_strategies().await?;
        if strategies.is_empty() {
            info!("No active strategies found in the database.");
            return Ok(());
        }

        let earliest_candle_date = *unique_dates
            .first()
            .expect("unique_dates is confirmed non-empty");

        let candles_by_ticker = self.data.cloned_candles_by_ticker();
        let shared_candles = Arc::new(candles_by_ticker);
        let shared_tickers = Arc::new(tickers.to_vec());

        let total = strategies.len();
        self.status.set_phase(format!(
            "Generating signals for {} active strategies",
            total
        ));
        self.status.set_progress(total, 0, 0, None);

        let mut processed = 0usize;
        let mut failed_jobs = 0usize;
        let mut total_inserted = 0usize;
        let mut signal_jobs = Vec::new();
        let mut cached_lightgbm_refs: Option<HashMap<String, Vec<&Candle>>> = None;

        for strategy in strategies {
            let StrategyConfig {
                id,
                name,
                template_id,
                parameters,
                backtest_start_date: strategy_start_date,
                excluded_tickers,
                ..
            } = strategy;
            info!("Preparing signal generation for strategy {}", id);
            let strategy_instance = match create_strategy(&template_id, parameters.clone()) {
                Ok(instance) => instance,
                Err(err) => {
                    warn!(
                        "Skipping signal generation for strategy {} ({}): {}",
                        id, template_id, err
                    );
                    processed += 1;
                    self.status
                        .set_progress(total, processed, failed_jobs, None);
                    continue;
                }
            };

            if strategy_instance.get_template_id().starts_with("lightgbm") {
                let ref_map = cached_lightgbm_refs.get_or_insert_with(|| {
                    let mut map: HashMap<String, Vec<&Candle>> = HashMap::new();
                    for (ticker, candle_list) in shared_candles.iter() {
                        map.insert(ticker.clone(), candle_list.iter().collect());
                    }
                    map
                });
                crate::strategy::lightgbm::prime_cross_sectional_context_from_ref_map(ref_map);
            }

            let latest_signal_date = self.db.get_latest_signal_date(&id).await?;
            let mut start_date = latest_signal_date.unwrap_or(earliest_candle_date);
            if let Some(strategy_start) = strategy_start_date {
                if strategy_start > start_date {
                    start_date = strategy_start;
                }
            }
            if start_date < earliest_candle_date {
                start_date = earliest_candle_date;
            }

            let target_dates: Vec<_> = unique_dates
                .iter()
                .copied()
                .filter(|d| *d >= start_date)
                .collect();

            if target_dates.is_empty() {
                processed += 1;
                self.status
                    .set_progress(total, processed, failed_jobs, None);
                info!(
                    "No new dates to generate signals for strategy {} (effective start date: {})",
                    id, start_date
                );
                self.db
                    .persist_strategy_event(
                        &id,
                        "info",
                        "No new candle dates available for signal generation",
                        json!({
                            "operation": "signal_generation",
                            "reason": "no_new_dates",
                            "effectiveStartDate": start_date,
                            "latestSignalDate": latest_signal_date,
                            "strategyStartDate": strategy_start_date,
                        }),
                    )
                    .await;
                continue;
            }

            let range_start = start_date;
            let range_end = *target_dates
                .last()
                .expect("target_dates is confirmed non-empty");

            let existing_signals = self
                .db
                .get_signals_for_strategy_in_range(&id, range_start, range_end)
                .await?;
            let existing_dates: HashSet<_> = existing_signals.iter().map(|s| s.date).collect();

            let dates_to_generate: Vec<_> = target_dates
                .iter()
                .copied()
                .filter(|d| !existing_dates.contains(d))
                .collect();

            if dates_to_generate.is_empty() {
                processed += 1;
                self.status
                    .set_progress(total, processed, failed_jobs, None);
                info!(
                    "All signals already exist for strategy {} in the target date range ({} to {})",
                    id, range_start, range_end
                );
                self.db
                    .persist_strategy_event(
                        &id,
                        "info",
                        "Signals already generated for requested window",
                        json!({
                            "operation": "signal_generation",
                            "reason": "already_up_to_date",
                            "rangeStart": range_start,
                            "rangeEnd": range_end,
                        }),
                    )
                    .await;
                continue;
            }

            signal_jobs.push(SignalGenerationJob {
                id,
                name,
                strategy: strategy_instance,
                dates_to_generate,
                excluded_tickers,
            });
        }

        if signal_jobs.is_empty() {
            self.status
                .set_progress(total, processed, failed_jobs, None);
            self.status.set_phase("Idle");
            info!("No new signals required for any active strategy.");
            return Ok(());
        }

        let cpu_budget = num_cpus::get().saturating_sub(1).max(1);
        let worker_limit = std::cmp::max(1, std::cmp::min(signal_jobs.len(), cpu_budget));
        info!(
            "Launching signal generation with {} concurrent worker{}",
            worker_limit,
            if worker_limit == 1 { "" } else { "s" }
        );

        let mut pending_jobs = signal_jobs.into_iter();
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

        for _ in 0..worker_limit {
            if let Some(job) = pending_jobs.next() {
                let tickers = Arc::clone(&shared_tickers);
                let candles = Arc::clone(&shared_candles);
                in_flight.push(tokio::task::spawn_blocking(move || {
                    run_signal_generation_job(job, tickers, candles)
                }));
            }
        }

        while let Some(handle) = in_flight.next().await {
            match handle {
                Ok(result) => {
                    let SignalGenerationJobResult {
                        id,
                        name,
                        requested_dates,
                        signals,
                    } = result;

                    if signals.is_empty() {
                        self.db
                            .persist_strategy_event(
                                &id,
                                "warn",
                                "Signal generation produced no actionable signals",
                                json!({
                                    "operation": "signal_generation",
                                    "reason": "no_signals_produced",
                                    "requestedDates": requested_dates.len(),
                                    "firstRequestedDate": requested_dates.first(),
                                    "lastRequestedDate": requested_dates.last(),
                                }),
                            )
                            .await;
                        processed += 1;
                        self.status
                            .set_progress(total, processed, failed_jobs, None);
                        continue;
                    }

                    let signal_context = format!(
                        "upserting {} signals for strategy {} ({})",
                        signals.len(),
                        name,
                        id
                    );
                    let inserted = retry_db_operation!(signal_context, async {
                        self.db.upsert_strategy_signals(&id, &signals).await
                    })?;

                    total_inserted += inserted;
                    let unique_date_count =
                        signals.iter().map(|s| s.date).collect::<HashSet<_>>().len();
                    info!(
                        "Generated {} signals for strategy {} across {} dates",
                        inserted, name, unique_date_count
                    );
                    self.db
                        .persist_strategy_event(
                            &id,
                            "info",
                            format!(
                                "Generated {} signals across {} day{}",
                                inserted,
                                unique_date_count,
                                if unique_date_count == 1 { "" } else { "s" }
                            ),
                            json!({
                                "operation": "signal_generation",
                                "insertedSignals": inserted,
                                "uniqueDates": unique_date_count,
                                "firstRequestedDate": requested_dates.first(),
                                "lastRequestedDate": requested_dates.last(),
                            }),
                        )
                        .await;
                }
                Err(join_err) => {
                    failed_jobs += 1;
                    warn!("Signal generation worker failed: {}", join_err);
                }
            }

            processed += 1;
            self.status
                .set_progress(total, processed, failed_jobs, None);

            if let Some(job) = pending_jobs.next() {
                let tickers = Arc::clone(&shared_tickers);
                let candles = Arc::clone(&shared_candles);
                in_flight.push(tokio::task::spawn_blocking(move || {
                    run_signal_generation_job(job, tickers, candles)
                }));
            }
        }

        self.status.set_progress(total, total, failed_jobs, None);
        self.status.set_phase("Idle");
        info!(
            "Signal generation completed; inserted {} new signals",
            total_inserted
        );

        Ok(())
    }
}

struct SignalGenerationJob {
    id: String,
    name: String,
    strategy: Box<dyn Strategy + Send + Sync>,
    dates_to_generate: Vec<chrono::DateTime<chrono::Utc>>,
    excluded_tickers: Vec<String>,
}

struct SignalGenerationJobResult {
    id: String,
    name: String,
    requested_dates: Vec<chrono::DateTime<chrono::Utc>>,
    signals: Vec<GeneratedSignal>,
}

fn run_signal_generation_job(
    job: SignalGenerationJob,
    tickers: Arc<Vec<String>>,
    candles_by_ticker: Arc<HashMap<String, Vec<Candle>>>,
) -> SignalGenerationJobResult {
    let SignalGenerationJob {
        id,
        name,
        strategy,
        dates_to_generate,
        excluded_tickers,
    } = job;

    let mut generated_signals = Vec::new();
    let target_ticker = strategy.target_ticker();
    let single_ticker: Option<Vec<String>> = target_ticker.as_ref().map(|target| {
        let mut list = Vec::with_capacity(1);
        if let Some(existing) = tickers
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(target))
        {
            list.push(existing.clone());
        } else {
            list.push(target.clone());
        }
        list
    });

    let tickers_to_iterate: &[String] = if let Some(ref list) = single_ticker {
        list.as_slice()
    } else {
        tickers.as_ref()
    };
    let blocked_tickers: HashSet<String> = excluded_tickers
        .into_iter()
        .map(|ticker| ticker.to_ascii_uppercase())
        .collect();
    for date in dates_to_generate.iter() {
        for ticker in tickers_to_iterate.iter() {
            let candles = match candles_by_ticker.get(ticker) {
                Some(list) => list,
                None => continue,
            };

            if let Ok(candle_index) = candles.binary_search_by(|c| c.date.cmp(date)) {
                // Use the shared signal generation function
                if let Some(generated) = generate_signal_with_filters(SignalGenerationParams {
                    strategy: strategy.as_ref(),
                    ticker,
                    candles,
                    candle_index,
                    date: *date,
                    excluded_tickers: &blocked_tickers,
                }) {
                    generated_signals.push(generated);
                }
            }
        }
    }

    let mut dedup = BTreeMap::<(chrono::DateTime<chrono::Utc>, String), GeneratedSignal>::new();
    for signal in generated_signals {
        dedup.insert((signal.date, signal.ticker.clone()), signal);
    }

    SignalGenerationJobResult {
        id,
        name,
        requested_dates: dates_to_generate,
        signals: dedup.into_values().collect(),
    }
}
