use crate::app_url::resolve_api_base_url;
use crate::backtest_api_client::build_async_client;
use crate::cache::{CacheManager, CacheStoreParams};
use crate::config::{
    resolve_backtest_initial_capital, EngineRuntimeSettings, LocalOptimizationObjective,
};
use crate::data_context::MarketData;
use crate::database::Database;
use crate::engine::Engine;
use crate::models::{
    encode_string_parameter, BacktestTask, BacktestTaskResult, Candle, OptimizationResult,
    ParameterRange, StrategyTemplate, Trade,
};
use crate::param_utils::{add_single_parameter_neighbor_variations, clamp_to_bounds};
use crate::strategy::create_strategy;
use anyhow::{anyhow, Result};
use chrono::prelude::*;
use crossbeam_channel::{bounded, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

enum VariationOutcome {
    NoChange,
    Improved(OptimizationResult),
}

pub(crate) fn parameter_signature(parameters: &HashMap<String, f64>) -> String {
    let mut sorted: Vec<_> = parameters.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    format!("{:?}", sorted)
}

fn collect_numeric_parameter_ranges(
    template: &StrategyTemplate,
) -> (Vec<String>, HashMap<String, ParameterRange>) {
    let mut parameters_to_optimize = Vec::new();
    let mut parameter_ranges = HashMap::new();

    for param in &template.parameters {
        if param.r#type != "number" {
            continue;
        }

        let (Some(min), Some(max), Some(step)) = (param.min, param.max, param.step) else {
            continue;
        };

        let name = param.name.clone();
        parameters_to_optimize.push(name.clone());
        parameter_ranges.insert(name, ParameterRange { min, max, step });
    }

    (parameters_to_optimize, parameter_ranges)
}

pub struct OptimizationEngine<'a> {
    db: Option<&'a mut Database>,
    cache_manager: &'a CacheManager,
    data: &'a MarketData,
}

impl<'a> OptimizationEngine<'a> {
    fn is_drawdown_within_limit(result: &OptimizationResult, max_drawdown_ratio: f64) -> bool {
        let ratio = result.max_drawdown_ratio;
        ratio.is_finite() && ratio <= max_drawdown_ratio
    }

    fn objective_score(result: &OptimizationResult, objective: LocalOptimizationObjective) -> f64 {
        let score = match objective {
            LocalOptimizationObjective::Cagr => result.cagr,
            LocalOptimizationObjective::Sharpe => result.sharpe_ratio,
        };
        if score.is_finite() {
            score
        } else {
            f64::NEG_INFINITY
        }
    }

    async fn load_strategy_template(&mut self, template_id: &str) -> Result<StrategyTemplate> {
        if let Some(template) = self.data.template(template_id) {
            return Ok(template);
        }

        if let Some(db) = self.db_ref() {
            match db.get_template(template_id).await {
                Ok(Some(template)) => return Ok(template),
                Ok(None) => {
                    warn!(
                        "Template {} not found in database. Falling back to local registry.",
                        template_id
                    );
                }
                Err(error) => {
                    warn!(
                        "Failed to load template {} from database: {}. Falling back to local registry.",
                        template_id, error
                    );
                }
            }
        } else {
            warn!(
                "Database unavailable while loading template {}. Using local registry.",
                template_id
            );
        }

        Err(anyhow!(
            "Template {} not found in cached snapshot or database",
            template_id
        ))
    }

    fn db_ref(&self) -> Option<&Database> {
        self.db.as_deref()
    }

    fn db_mut(&mut self) -> Option<&mut Database> {
        self.db.as_deref_mut()
    }

    pub fn new(
        db: Option<&'a mut Database>,
        cache_manager: &'a CacheManager,
        data: &'a MarketData,
    ) -> Self {
        Self {
            db,
            cache_manager,
            data,
        }
    }

    pub async fn detect_optimizable_parameters(
        &mut self,
        template_id: &str,
    ) -> Result<(Vec<String>, HashMap<String, ParameterRange>)> {
        let template = self.load_strategy_template(template_id).await?;

        let (parameters_to_optimize, parameter_ranges) =
            collect_numeric_parameter_ranges(&template);

        if parameters_to_optimize.is_empty() {
            return Err(anyhow!("No optimizable parameters found for this strategy"));
        }

        info!(
            "Auto-detected parameters to optimize: {:?}",
            parameters_to_optimize
        );

        Ok((parameters_to_optimize, parameter_ranges))
    }

    pub async fn optimize_local_search(
        &mut self,
        template_id: &str,
        parameters_to_optimize: &[String],
        parameter_ranges: &HashMap<String, ParameterRange>,
    ) -> Result<()> {
        let runtime_settings = EngineRuntimeSettings::from_settings_map(self.data.settings())?;
        let backtest_initial_capital = resolve_backtest_initial_capital(self.data.settings());
        let local_optimization_version = runtime_settings.local_optimization_version;
        let max_drawdown_ratio = runtime_settings.max_allowed_drawdown_ratio;
        let objective = runtime_settings.local_optimization_objective;
        let objective_label = objective.label();
        let step_multipliers = runtime_settings
            .local_optimization_step_multipliers
            .as_slice();
        let template = self.load_strategy_template(template_id).await?;

        info!(
            "Starting local search optimization for template: {}",
            template_id
        );

        let mut current_params = self.load_baseline_parameters(template_id, &template).await;
        current_params.insert("initialCapital".to_string(), backtest_initial_capital);

        clamp_to_bounds(
            &mut current_params,
            parameter_ranges,
            parameters_to_optimize,
        );

        let mut best_result: Option<OptimizationResult> = None;
        let mut best_score = f64::NEG_INFINITY;

        loop {
            let mut seen_variations = HashSet::new();
            let mut neighbor_variations = Vec::new();

            clamp_to_bounds(
                &mut current_params,
                parameter_ranges,
                parameters_to_optimize,
            );

            if best_result.is_none() {
                neighbor_variations.push(current_params.clone());
            }

            add_single_parameter_neighbor_variations(
                parameters_to_optimize,
                parameter_ranges,
                step_multipliers,
                &current_params,
                &mut seen_variations,
                &mut neighbor_variations,
            );

            if neighbor_variations.is_empty() {
                break;
            }

            match self
                .evaluate_variation_batch(
                    template_id,
                    &neighbor_variations,
                    best_score,
                    max_drawdown_ratio,
                    objective,
                )
                .await?
            {
                VariationOutcome::Improved(result) => {
                    let score = Self::objective_score(&result, objective);
                    if best_result.is_none() {
                        info!(
                            "Initial valid candidate: {} {:.4} (CAGR {:.2}%) with max drawdown {:.2}%.",
                            objective_label,
                            score,
                            result.cagr * 100.0,
                            result.max_drawdown_ratio * 100.0
                        );
                    } else {
                        info!(
                            "New best {}: {:.4} (previous: {:.4}), CAGR {:.2}%, max drawdown {:.2}%.",
                            objective_label,
                            score,
                            best_score,
                            result.cagr * 100.0,
                            result.max_drawdown_ratio * 100.0
                        );
                    }

                    let params_changed = result.parameters != current_params;
                    best_score = score;
                    current_params = result.parameters.clone();
                    best_result = Some(result);

                    if !params_changed {
                        break;
                    }
                }
                VariationOutcome::NoChange => break,
            }
        }

        let Some(best_result) = best_result else {
            info!(
                "No backtests were executed for the starting batch; stopping optimization early."
            );
            return Ok(());
        };

        let final_score = Self::objective_score(&best_result, objective);
        info!(
            "Local search finished. Best {}: {:.4} (CAGR {:.2}%) with max drawdown {:.2}%.",
            objective_label,
            final_score,
            best_result.cagr * 100.0,
            best_result.max_drawdown_ratio * 100.0
        );

        let final_results = self
            .run_parallel_backtests(template_id, &[current_params.clone()], true)
            .await?;

        if final_results.is_empty() {
            info!("Final validation produced no results; reusing best observed variation.");
            self.print_results(std::slice::from_ref(&best_result), 1);
        } else {
            self.print_results(&final_results, 1);
        }
        if let Some(db) = self.db_ref() {
            match db
                .update_template_local_optimization_version(template_id, local_optimization_version)
                .await
            {
                Ok(_) => info!("Set template {} local_optimization_version", template_id),
                Err(e) => warn!(
                    "Failed to persist local optimization version for template {}: {}",
                    template_id, e
                ),
            }
        } else {
            warn!(
                "Skipping template version update for {} because database is unavailable",
                template_id
            );
        }
        let default_strategy_id = format!("default_{}", template_id);
        if let Some(db) = self.db_mut() {
            match db.delete_strategy_and_related(&default_strategy_id).await {
                Ok(_) => info!("Deleted default strategy {}", default_strategy_id),
                Err(e) => warn!(
                    "Failed to delete default strategy {}: {}",
                    default_strategy_id, e
                ),
            }
        } else {
            warn!(
                "Skipping cleanup for {} because database is unavailable",
                default_strategy_id
            );
        }
        Ok(())
    }

    async fn evaluate_variation_batch(
        &mut self,
        template_id: &str,
        variations: &[HashMap<String, f64>],
        best_score: f64,
        max_drawdown_ratio: f64,
        objective: LocalOptimizationObjective,
    ) -> Result<VariationOutcome> {
        if variations.is_empty() {
            return Ok(VariationOutcome::NoChange);
        }

        let results = self
            .run_parallel_backtests(template_id, variations, true)
            .await?;

        if results.is_empty() {
            return Ok(VariationOutcome::NoChange);
        }

        let total_evaluated = results.len();
        let feasible_results: Vec<_> = results
            .into_iter()
            .filter(|result| Self::is_drawdown_within_limit(result, max_drawdown_ratio))
            .collect();

        let rejected = total_evaluated.saturating_sub(feasible_results.len());
        if rejected > 0 {
            info!(
                "Rejected {} variation(s) with drawdown above {:.0}%.",
                rejected,
                max_drawdown_ratio * 100.0
            );
        }

        if let Some(best_in_batch) = feasible_results.iter().max_by(|a, b| {
            Self::objective_score(a, objective)
                .partial_cmp(&Self::objective_score(b, objective))
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            let best_in_batch_score = Self::objective_score(best_in_batch, objective);
            if best_in_batch_score > best_score {
                Ok(VariationOutcome::Improved(best_in_batch.clone()))
            } else {
                Ok(VariationOutcome::NoChange)
            }
        } else {
            info!(
                "All evaluated variations exceeded the {:.0}% drawdown limit.",
                max_drawdown_ratio * 100.0
            );
            Ok(VariationOutcome::NoChange)
        }
    }

    fn get_default_parameters_from_template(
        &self,
        template: &StrategyTemplate,
    ) -> HashMap<String, f64> {
        let mut params = HashMap::new();
        for p in &template.parameters {
            if let Some(default) = &p.default {
                if let Some(num) = default.as_f64() {
                    params.insert(p.name.clone(), num);
                } else if let Some(text) = default.as_str() {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        params.insert(p.name.clone(), encode_string_parameter(trimmed));
                    }
                } else if let Some(boolean) = default.as_bool() {
                    params.insert(p.name.clone(), if boolean { 1.0 } else { 0.0 });
                }
            }
        }
        params
    }

    fn merge_with_template_defaults_numeric(
        &self,
        template: &StrategyTemplate,
        incoming: &HashMap<String, f64>,
    ) -> HashMap<String, f64> {
        let mut merged = self.get_default_parameters_from_template(template);
        for (k, v) in incoming {
            merged.insert(k.clone(), *v);
        }
        merged
    }

    pub async fn run_parameter_batch(
        &mut self,
        template_id: &str,
        variations: &[HashMap<String, f64>],
        use_cache: bool,
    ) -> Result<Vec<OptimizationResult>> {
        self.run_parallel_backtests(template_id, variations, use_cache)
            .await
    }

    async fn load_baseline_parameters(
        &self,
        template_id: &str,
        template: &StrategyTemplate,
    ) -> HashMap<String, f64> {
        let Some(api_base_url) = resolve_api_base_url(self.data.settings()) else {
            warn!(
                "Backtest API base URL is not configured; using template defaults for {}.",
                template_id
            );
            return self.get_default_parameters_from_template(template);
        };
        let url = format!("{}/backtest/best/{}", api_base_url, template_id);
        info!("Fetching best known parameters from {}", url);

        let api_secret = self
            .data
            .settings()
            .get("BACKTEST_API_SECRET")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let client = match build_async_client(None) {
            Ok(client) => client,
            Err(err) => {
                warn!(
                    "Failed to build HTTP client for best-parameter fetch: {}. Using defaults for {}.",
                    err, template_id
                );
                return self.get_default_parameters_from_template(template);
            }
        };
        let mut request = client.get(&url);
        if let Some(secret) = api_secret.as_deref() {
            request = request.header("x-backtest-secret", secret);
        }

        match request.send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<OptimizationResult>().await
            {
                Ok(best_known) => {
                    info!(
                        "Found best known parameters (CAGR {:.2}%, Calmar {:.4}).",
                        best_known.cagr * 100.0,
                        best_known.calmar_ratio
                    );
                    self.merge_with_template_defaults_numeric(template, &best_known.parameters)
                }
                Err(err) => {
                    warn!(
                        "Failed to parse best parameters for {}: {}. Falling back to defaults.",
                        template_id, err
                    );
                    self.get_default_parameters_from_template(template)
                }
            },
            Ok(resp) => {
                warn!(
                    "Failed to fetch best parameters for {} (status: {}). Using defaults.",
                    template_id,
                    resp.status()
                );
                self.get_default_parameters_from_template(template)
            }
            Err(err) => {
                warn!(
                    "Failed to connect to server to fetch best parameters for {}: {}. Starting with defaults.",
                    template_id, err
                );
                self.get_default_parameters_from_template(template)
            }
        }
    }

    async fn run_parallel_backtests(
        &mut self,
        template_id: &str,
        variations: &[HashMap<String, f64>],
        use_cache: bool,
    ) -> Result<Vec<OptimizationResult>> {
        if variations.is_empty() {
            return Ok(Vec::new());
        }

        let variation_count = variations.len();
        let runtime_settings = EngineRuntimeSettings::from_settings_map(self.data.settings())?;
        let backtest_initial_capital = resolve_backtest_initial_capital(self.data.settings());
        info!("Running {} backtests...", variation_count);

        let num_workers = std::cmp::min(variation_count, std::cmp::max(1, num_cpus::get()));
        info!("Using {} worker threads", num_workers);

        let (tx, rx): (Sender<BacktestTask>, Receiver<BacktestTask>) = bounded(variation_count);
        let (result_tx, result_rx): (Sender<BacktestTaskResult>, Receiver<BacktestTaskResult>) =
            bounded(variation_count);

        let mut handles = Vec::new();
        for _worker_id in 0..num_workers {
            let rx = rx.clone();
            let result_tx = result_tx.clone();
            let all_candles = self.data.all_candles_arc();
            let unique_dates = self.data.unique_dates_arc();
            let tickers = self.data.tickers_arc();
            let ticker_expense_map = self.data.ticker_expense_map_arc();
            let cache_manager = self.cache_manager.clone();
            let use_cache = use_cache;
            let runtime_settings = runtime_settings.clone();

            let handle = thread::spawn(move || {
                while let Ok(task) = rx.recv() {
                    let start_time = Instant::now();
                    let result = Self::run_single_backtest(
                        all_candles.as_slice(),
                        unique_dates.as_slice(),
                        tickers.as_slice(),
                        ticker_expense_map.clone(),
                        runtime_settings.clone(),
                        &cache_manager,
                        &task,
                        use_cache,
                    );
                    let duration = start_time.elapsed();

                    if let Some(opt_result) = &result.result {
                        let params_str = task
                            .parameters
                            .iter()
                            .map(|(k, v)| {
                                let formatted_value = format!("{:.4}", v);
                                let trimmed_value =
                                    formatted_value.trim_end_matches('0').trim_end_matches('.');
                                let cleaned_value =
                                    if trimmed_value.is_empty() || trimmed_value == "-0" {
                                        "0"
                                    } else {
                                        trimmed_value
                                    };
                                format!("{}: {}", k, cleaned_value)
                            })
                            .collect::<Vec<String>>()
                            .join(", ");
                        info!(
                            "Worker finished task {} in {:.0}m. CAGR: {:.2}%, Max DD: {:.2}%, Sharpe: {:.4}, Return: ${:.2}, Params: [{}]",
                            task.id,
                            duration.as_secs_f64() / 60.0,
                            opt_result.cagr * 100.0,
                            opt_result.max_drawdown_ratio * 100.0,
                            opt_result.sharpe_ratio,
                            opt_result.total_return,
                            params_str
                        );
                    } else if let Some(error) = &result._error {
                        warn!(
                            "Worker finished task {} in {:.0}m with error: {}",
                            task.id,
                            duration.as_secs_f64() / 60.0,
                            error
                        );
                    }

                    if let Err(_e) = result_tx.send(result) {
                        break;
                    }
                }
            });
            handles.push(handle);
        }

        for (i, parameters) in variations.iter().enumerate() {
            let mut parameters = parameters.clone();
            parameters.insert("initialCapital".to_string(), backtest_initial_capital);
            let task = BacktestTask {
                id: format!("{}_{}", template_id, i),
                template_id: template_id.to_string(),
                parameters,
            };
            tx.send(task)?;
        }

        drop(tx);

        let mut results = Vec::new();
        let mut completed = 0;
        let mut failed_workers = 0;
        let pb = ProgressBar::new(variation_count as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
                )
                .unwrap()
                .progress_chars("#>-"),
        );

        while completed < variation_count {
            match result_rx.recv_timeout(std::time::Duration::from_millis(200)) {
                Ok(result) => {
                    completed += 1;
                    pb.set_position(completed as u64);

                    if let Some(opt_result) = result.result {
                        results.push(opt_result);
                    } else {
                        failed_workers += 1;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    warn!("Result channel closed unexpectedly. Some results may be lost.");
                    break;
                }
            }
        }

        if failed_workers > 0 {
            warn!(
                "Backtesting completed with {} worker errors",
                failed_workers
            );
            pb.finish_with_message("Backtesting completed with errors");
        } else {
            pb.finish_with_message("Backtesting completed");
        }

        for handle in handles {
            handle.join().unwrap();
        }
        Ok(results)
    }

    fn run_single_backtest(
        all_candles: &[Candle],
        unique_dates: &[DateTime<Utc>],
        tickers: &[String],
        ticker_expense_map: Arc<HashMap<String, f64>>,
        runtime_settings: EngineRuntimeSettings,
        cache_manager: &CacheManager,
        task: &BacktestTask,
        use_cache: bool,
    ) -> BacktestTaskResult {
        if use_cache {
            if let Some(cached_result) =
                cache_manager.check_cache(&task.template_id, &task.parameters)
            {
                return BacktestTaskResult {
                    _task_id: task.id.clone(),
                    result: Some(cached_result),
                    _error: None,
                };
            }
        }

        let strategy = match create_strategy(&task.template_id, task.parameters.clone()) {
            Ok(s) => s,
            Err(e) => {
                return BacktestTaskResult {
                    _task_id: task.id.clone(),
                    result: None,
                    _error: Some(e.to_string()),
                };
            }
        };

        let start_time = Instant::now();
        let mut engine = Engine::from_parameters(&task.parameters, runtime_settings);
        engine.set_ticker_expense_map(ticker_expense_map);
        let backtest_run = match engine.backtest(
            Some(strategy.as_ref()),
            &task.template_id,
            tickers,
            all_candles,
            unique_dates,
            None,
            None,
            None,
        ) {
            Ok(result) => result,
            Err(e) => {
                return BacktestTaskResult {
                    _task_id: task.id.clone(),
                    result: None,
                    _error: Some(e.to_string()),
                };
            }
        };
        let duration_minutes = start_time.elapsed().as_secs_f64() / 60.0;
        let (top_abs_gain_ticker, top_rel_gain_ticker) =
            extract_top_ticker_gains(&backtest_run.result.trades);

        let optimization_result = OptimizationResult {
            parameters: task.parameters.clone(),
            cagr: backtest_run.result.performance.cagr,
            sharpe_ratio: backtest_run.result.performance.sharpe_ratio,
            total_return: backtest_run.result.performance.total_return,
            max_drawdown: backtest_run.result.performance.max_drawdown,
            max_drawdown_ratio: backtest_run.result.performance.max_drawdown_percent / 100.0,
            win_rate: backtest_run.result.performance.win_rate,
            total_trades: backtest_run.result.performance.total_trades,
            calmar_ratio: backtest_run.result.performance.calmar_ratio,
        };
        if use_cache {
            cache_manager.store_cache(CacheStoreParams {
                template_id: task.template_id.clone(),
                parameters: task.parameters.clone(),
                result: optimization_result.clone(),
                ticker_count: tickers.len() as i32,
                start_date: unique_dates[0],
                end_date: unique_dates[unique_dates.len() - 1],
                duration_minutes,
                top_absolute_gain_ticker: top_abs_gain_ticker,
                top_relative_gain_ticker: top_rel_gain_ticker,
            });
        }

        BacktestTaskResult {
            _task_id: task.id.clone(),
            result: Some(optimization_result),
            _error: None,
        }
    }

    fn print_results(&self, results: &[OptimizationResult], top_n: usize) {
        println!(
            "\n=== TOP {} STRATEGY VARIANTS ===\n",
            std::cmp::min(top_n, results.len())
        );

        for (i, result) in results.iter().take(top_n).enumerate() {
            println!("Rank {}:", i + 1);
            println!("  CAGR: {:.2}%", result.cagr * 100.0);
            println!("  Calmar Ratio: {:.4}", result.calmar_ratio);
            println!("  Sharpe Ratio: {:.4}", result.sharpe_ratio);
            println!("  Total Return: ${:.2}", result.total_return);
            println!(
                "  Max Drawdown: ${:.2} (ratio {:.4}, {:.2}%)",
                result.max_drawdown,
                result.max_drawdown_ratio,
                result.max_drawdown_ratio * 100.0
            );
            println!("  Win Rate: {:.2}%", result.win_rate * 100.0);
            println!("  Total Trades: {}", result.total_trades);
            println!("  Parameters:");
            for (key, value) in &result.parameters {
                println!("    {}: {}", key, value);
            }
            println!();
        }
    }
}

fn extract_top_ticker_gains(trades: &[Trade]) -> (Option<String>, Option<String>) {
    let mut aggregated: HashMap<String, (f64, f64)> = HashMap::new();

    for trade in trades {
        let Some(pnl) = trade.pnl else {
            continue;
        };
        if !pnl.is_finite() {
            continue;
        }
        let quantity = trade.quantity.abs() as f64;
        let mut notional = quantity * trade.price.abs();
        if !notional.is_finite() || notional.is_sign_negative() {
            notional = 0.0;
        }
        let entry = aggregated
            .entry(trade.ticker.clone())
            .or_insert((0.0_f64, 0.0_f64));
        entry.0 += pnl;
        if notional > 0.0 {
            entry.1 += notional;
        }
    }

    let mut top_absolute: Option<(String, f64)> = None;
    let mut top_relative: Option<(String, f64)> = None;

    for (ticker, (total_pnl, total_notional)) in aggregated.into_iter() {
        if total_pnl.is_finite()
            && top_absolute
                .as_ref()
                .map(|(_, best)| total_pnl > *best)
                .unwrap_or(true)
        {
            top_absolute = Some((ticker.clone(), total_pnl));
        }

        if total_notional > 0.0 && total_pnl.is_finite() {
            let ratio = (total_pnl / total_notional) * 100.0;
            if top_relative
                .as_ref()
                .map(|(_, best)| ratio > *best)
                .unwrap_or(true)
            {
                top_relative = Some((ticker, ratio));
            }
        }
    }

    (
        top_absolute.map(|(ticker, _)| ticker),
        top_relative.map(|(ticker, _)| ticker),
    )
}
