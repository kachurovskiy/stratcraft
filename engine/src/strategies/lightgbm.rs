use crate::indicators;
use crate::models::*;
use crate::param_utils::{get_param_f64_clamped, get_param_usize_rounded_clamped};
use crate::strategy_utils::{buy_signal, hold_signal, meets_confidence_threshold, sell_signal};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use log::{debug, info, warn};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

pub const SIGNAL_BUCKET_COUNT: usize = 2;
pub const SIGNAL_BUCKET_NAMES: [&str; SIGNAL_BUCKET_COUNT] = ["no_hit", "hit"];
pub const SIGNAL_BUCKET_NEGATIVE: usize = 0;
pub const SIGNAL_BUCKET_POSITIVE: usize = 1;
const BUY_PROBABILITY_THRESHOLD: f64 = 0.5;

#[derive(Clone, Copy, Debug)]
pub struct FeatureConfig {
    pub rsi_period: usize,
    pub atr_period: usize,
    pub stochastic_period: usize,
    pub stochastic_smooth: usize,
    pub cci_period: usize,
    pub bollinger_period: usize,
    pub bollinger_std: f64,
    pub momentum_short: usize,
    pub momentum_long: usize,
    pub volatility_short: usize,
    pub volatility_long: usize,
    pub ma_fast: usize,
    pub ma_slow: usize,
    pub ma_trend: usize,
    pub ma_trend_slow: usize,
    pub correlation_window: usize,
}

impl FeatureConfig {
    pub fn default() -> Self {
        Self {
            rsi_period: 14,
            atr_period: 14,
            stochastic_period: 14,
            stochastic_smooth: 3,
            cci_period: 20,
            bollinger_period: 20,
            bollinger_std: 2.0,
            momentum_short: 20,
            momentum_long: 60,
            volatility_short: 5,
            volatility_long: 20,
            ma_fast: 10,
            ma_slow: 50,
            ma_trend: 20,
            ma_trend_slow: 200,
            correlation_window: 20,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FeatureVector {
    pub values: Vec<f64>,
}

#[derive(Debug)]
struct LightGBMSummary {
    invocations: usize,
    scored: usize,
    buys: usize,
    sells: usize,
    holds: usize,
    skipped_out_of_bounds: usize,
    skipped_insufficient_history: usize,
    feature_failures: usize,
    probability_missing: usize,
    probability_sum: f64,
    probability_min: f64,
    probability_max: f64,
}

impl Default for LightGBMSummary {
    fn default() -> Self {
        Self {
            invocations: 0,
            scored: 0,
            buys: 0,
            sells: 0,
            holds: 0,
            skipped_out_of_bounds: 0,
            skipped_insufficient_history: 0,
            feature_failures: 0,
            probability_missing: 0,
            probability_sum: 0.0,
            probability_min: f64::INFINITY,
            probability_max: f64::NEG_INFINITY,
        }
    }
}

impl LightGBMSummary {
    fn record_invocation(&mut self) {
        self.invocations += 1;
    }

    fn record_out_of_bounds(&mut self) {
        self.skipped_out_of_bounds += 1;
    }

    fn record_history_gap(&mut self) {
        self.skipped_insufficient_history += 1;
    }

    fn record_feature_failure(&mut self) {
        self.feature_failures += 1;
    }

    fn record_probability_missing(&mut self) {
        self.probability_missing += 1;
    }

    fn record_scored_decision(&mut self, action: &SignalAction, probability: f64) {
        self.scored += 1;
        self.probability_sum += probability;
        self.probability_min = self.probability_min.min(probability);
        self.probability_max = self.probability_max.max(probability);

        match action {
            SignalAction::Buy => self.buys += 1,
            SignalAction::Sell => self.sells += 1,
            SignalAction::Hold => self.holds += 1,
        }
    }

    fn average_probability(&self) -> Option<f64> {
        if self.scored == 0 {
            None
        } else {
            Some(self.probability_sum / self.scored as f64)
        }
    }

    fn min_probability(&self) -> Option<f64> {
        if self.probability_min.is_finite() {
            Some(self.probability_min)
        } else {
            None
        }
    }

    fn max_probability(&self) -> Option<f64> {
        if self.probability_max.is_finite() {
            Some(self.probability_max)
        } else {
            None
        }
    }

    fn total_skipped(&self) -> usize {
        self.skipped_out_of_bounds
            + self.skipped_insufficient_history
            + self.feature_failures
            + self.probability_missing
    }

    fn describe(&self) -> String {
        let mut parts = Vec::new();
        parts.push(format!("calls={}", self.invocations));
        parts.push(format!(
            "scored={} (buy={}, sell={}, hold={})",
            self.scored, self.buys, self.sells, self.holds
        ));

        let skipped = self.total_skipped();
        parts.push(format!(
            "skipped={} (oob={}, history={}, features={}, prob={})",
            skipped,
            self.skipped_out_of_bounds,
            self.skipped_insufficient_history,
            self.feature_failures,
            self.probability_missing
        ));

        if let Some(avg) = self.average_probability() {
            let min = self
                .min_probability()
                .map(|value| format!("{value:.4}"))
                .unwrap_or_else(|| "-".to_string());
            let max = self
                .max_probability()
                .map(|value| format!("{value:.4}"))
                .unwrap_or_else(|| "-".to_string());
            parts.push(format!("prob(avg={avg:.4}, min={min}, max={max})"));
        } else {
            parts.push("prob(avg=-, min=-, max=-)".to_string());
        }

        parts.join(", ")
    }
}

const EPSILON: f64 = 1e-12;

fn safe_div(numerator: f64, denominator: f64) -> f64 {
    if denominator.abs() <= EPSILON {
        0.0
    } else {
        numerator / denominator
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    let mut sum = 0.0;
    let mut count = 0usize;
    for value in values {
        if value.is_finite() {
            sum += *value;
            count += 1;
        }
    }

    if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    }
}

fn std_dev(values: &[f64]) -> Option<f64> {
    let mean_value = mean(values)?;
    let mut sum = 0.0;
    let mut count = 0usize;

    for value in values {
        if value.is_finite() {
            let diff = *value - mean_value;
            sum += diff * diff;
            count += 1;
        }
    }

    if count < 2 {
        None
    } else {
        Some((sum / count as f64).sqrt())
    }
}

fn rolling_slice<'a>(data: &'a [f64], end_idx: usize, window: usize) -> Option<&'a [f64]> {
    if window == 0 || end_idx + 1 < window || end_idx >= data.len() {
        None
    } else {
        Some(&data[end_idx + 1 - window..=end_idx])
    }
}

fn rolling_mean_at(data: &[f64], end_idx: usize, window: usize) -> Option<f64> {
    rolling_slice(data, end_idx, window).and_then(mean)
}

fn rolling_std_at(data: &[f64], end_idx: usize, window: usize) -> Option<f64> {
    rolling_slice(data, end_idx, window).and_then(std_dev)
}

fn rolling_max_at(data: &[f64], end_idx: usize, window: usize) -> Option<f64> {
    let slice = rolling_slice(data, end_idx, window)?;
    let mut best: Option<f64> = None;
    for value in slice.iter().copied() {
        if value.is_finite() {
            best = Some(best.map_or(value, |existing| existing.max(value)));
        }
    }
    best
}

fn rolling_min_at(data: &[f64], end_idx: usize, window: usize) -> Option<f64> {
    let slice = rolling_slice(data, end_idx, window)?;
    let mut best: Option<f64> = None;
    for value in slice.iter().copied() {
        if value.is_finite() {
            best = Some(best.map_or(value, |existing| existing.min(value)));
        }
    }
    best
}

fn rolling_corr_at(x: &[f64], y: &[f64], end_idx: usize, window: usize) -> Option<f64> {
    let x_slice = rolling_slice(x, end_idx, window)?;
    let y_slice = rolling_slice(y, end_idx, window)?;
    if x_slice.len() != y_slice.len() {
        return None;
    }

    let mut paired: Vec<(f64, f64)> = Vec::with_capacity(x_slice.len());
    for (a, b) in x_slice.iter().zip(y_slice.iter()) {
        if a.is_finite() && b.is_finite() {
            paired.push((*a, *b));
        }
    }
    if paired.len() < 2 {
        return None;
    }

    let mean_x = paired.iter().map(|(a, _)| *a).sum::<f64>() / paired.len() as f64;
    let mean_y = paired.iter().map(|(_, b)| *b).sum::<f64>() / paired.len() as f64;

    let mut numerator = 0.0;
    let mut denom_x = 0.0;
    let mut denom_y = 0.0;
    for (a, b) in paired {
        let dx = a - mean_x;
        let dy = b - mean_y;
        numerator += dx * dy;
        denom_x += dx * dx;
        denom_y += dy * dy;
    }

    let denom = (denom_x * denom_y).sqrt();
    if denom <= EPSILON {
        None
    } else {
        Some(numerator / denom)
    }
}

fn percentile_ranks(entries: &[(String, f64)], default: f64) -> HashMap<String, f64> {
    if entries.is_empty() {
        return HashMap::new();
    }

    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

    let last_index = (sorted.len().saturating_sub(1)).max(1) as f64;
    let mut ranks: HashMap<String, f64> = HashMap::new();
    let mut idx = 0usize;
    while idx < sorted.len() {
        let value = sorted[idx].1;
        let mut end = idx + 1;
        while end < sorted.len() && (sorted[end].1 - value).abs() <= EPSILON {
            end += 1;
        }
        let mid_rank = ((idx + end - 1) as f64 / 2.0) / last_index;
        for pos in idx..end {
            ranks.insert(sorted[pos].0.clone(), mid_rank);
        }
        idx = end;
    }

    for (ticker, _) in entries {
        ranks.entry(ticker.clone()).or_insert(default);
    }

    ranks
}

fn zscore_map(entries: &[(String, f64)]) -> HashMap<String, f64> {
    let mut result = HashMap::new();
    if entries.is_empty() {
        return result;
    }

    let valid_values: Vec<f64> = entries
        .iter()
        .map(|(_, value)| *value)
        .filter(|value| value.is_finite())
        .collect();
    if valid_values.is_empty() {
        return result;
    }
    let mean_val = mean(&valid_values).unwrap_or(0.0);
    let variance = valid_values
        .iter()
        .map(|value| {
            let diff = *value - mean_val;
            diff * diff
        })
        .sum::<f64>()
        / valid_values.len() as f64;
    let std_val = variance.sqrt().max(EPSILON);

    for (ticker, value) in entries {
        result.insert(ticker.clone(), (*value - mean_val) / std_val);
    }
    result
}

#[derive(Clone, Copy, Debug, Default)]
struct CrossSectionalFeatures {
    return_rank: f64,
    momentum_rank: f64,
    volatility_rank: f64,
    volume_rank: f64,
    return_zscore: f64,
    momentum_zscore: f64,
}

#[derive(Clone)]
struct TickerCrossSeries {
    return_1d: Vec<f64>,
    momentum_20: Vec<f64>,
    volatility_20: Vec<f64>,
    volume_ratio_20: Vec<f64>,
    date_index: HashMap<DateTime<Utc>, usize>,
}

impl TickerCrossSeries {
    fn from_refs(candles: &[&Candle]) -> Option<Self> {
        if candles.len() < 2 {
            return None;
        }

        let mut closes = Vec::with_capacity(candles.len());
        let mut volumes = Vec::with_capacity(candles.len());
        let mut date_index = HashMap::with_capacity(candles.len());

        for (idx, candle) in candles.iter().enumerate() {
            closes.push(candle.close);
            volumes.push(candle.volume_shares as f64);
            date_index.insert(candle.date, idx);
        }

        let mut return_1d = vec![f64::NAN; closes.len()];
        for i in 1..closes.len() {
            let prev = closes[i - 1];
            if prev.abs() > EPSILON {
                return_1d[i] = (closes[i] - prev) / prev;
            }
        }

        let mut momentum_20 = vec![f64::NAN; closes.len()];
        for i in 20..closes.len() {
            let past = closes[i - 20];
            if past.abs() > EPSILON {
                momentum_20[i] = closes[i] / past - 1.0;
            }
        }

        let mut volatility_20 = vec![f64::NAN; closes.len()];
        for i in 1..closes.len() {
            if let Some(std) = rolling_std_at(&return_1d, i, 20) {
                volatility_20[i] = std;
            }
        }

        let mut volume_ratio_20 = vec![f64::NAN; volumes.len()];
        for i in 0..volumes.len() {
            if let Some(avg_vol) = rolling_mean_at(&volumes, i, 20) {
                if avg_vol > 0.0 && volumes[i].is_finite() {
                    volume_ratio_20[i] = (volumes[i] / avg_vol) - 1.0;
                }
            }
        }

        Some(Self {
            return_1d,
            momentum_20,
            volatility_20,
            volume_ratio_20,
            date_index,
        })
    }
}

pub struct CrossSectionalContext {
    per_ticker: HashMap<String, TickerCrossSeries>,
    per_date_cache: DashMap<DateTime<Utc>, Arc<HashMap<String, CrossSectionalFeatures>>>,
}

impl CrossSectionalContext {
    pub fn new(candles_by_ticker: &HashMap<String, Vec<&Candle>>) -> Option<Self> {
        let mut per_ticker: HashMap<String, TickerCrossSeries> = HashMap::new();
        for (ticker, candles) in candles_by_ticker {
            if let Some(series) = TickerCrossSeries::from_refs(candles) {
                per_ticker.insert(ticker.clone(), series);
            }
        }

        if per_ticker.is_empty() {
            None
        } else {
            Some(Self {
                per_ticker,
                per_date_cache: DashMap::new(),
            })
        }
    }

    fn build_snapshot_for_date(
        &self,
        date: DateTime<Utc>,
    ) -> Option<HashMap<String, CrossSectionalFeatures>> {
        let (return_entries, momentum_entries, volatility_entries, volume_entries) = self
            .per_ticker
            .par_iter()
            .fold(
                || (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                |mut acc, (ticker, series)| {
                    if let Some(&idx) = series.date_index.get(&date) {
                        let ret = series.return_1d.get(idx).copied().unwrap_or(f64::NAN);
                        if ret.is_finite() {
                            acc.0.push((ticker.clone(), ret));
                        }

                        let momentum = series.momentum_20.get(idx).copied().unwrap_or(f64::NAN);
                        if momentum.is_finite() {
                            acc.1.push((ticker.clone(), momentum));
                        }

                        let volatility = series.volatility_20.get(idx).copied().unwrap_or(f64::NAN);
                        if volatility.is_finite() {
                            acc.2.push((ticker.clone(), volatility));
                        }

                        let vol_change =
                            series.volume_ratio_20.get(idx).copied().unwrap_or(f64::NAN);
                        if vol_change.is_finite() {
                            acc.3.push((ticker.clone(), vol_change));
                        }
                    }
                    acc
                },
            )
            .reduce(
                || (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                |mut a, mut b| {
                    a.0.append(&mut b.0);
                    a.1.append(&mut b.1);
                    a.2.append(&mut b.2);
                    a.3.append(&mut b.3);
                    a
                },
            );

        if return_entries.is_empty() {
            return None;
        }

        let involved_tickers: HashSet<String> = return_entries
            .iter()
            .map(|(t, _)| t.clone())
            .chain(momentum_entries.iter().map(|(t, _)| t.clone()))
            .chain(volatility_entries.iter().map(|(t, _)| t.clone()))
            .chain(volume_entries.iter().map(|(t, _)| t.clone()))
            .collect();

        let rank_returns = percentile_ranks(&return_entries, 0.5);
        let rank_momentum = percentile_ranks(&momentum_entries, 0.5);
        let rank_volatility = percentile_ranks(&volatility_entries, 0.5);
        let rank_volume = percentile_ranks(&volume_entries, 0.5);

        let z_returns = zscore_map(&return_entries);
        let z_momentum = zscore_map(&momentum_entries);

        let mut snapshot: HashMap<String, CrossSectionalFeatures> =
            HashMap::with_capacity(involved_tickers.len());
        for ticker in involved_tickers {
            snapshot.insert(
                ticker.clone(),
                CrossSectionalFeatures {
                    return_rank: *rank_returns.get(&ticker).unwrap_or(&0.5),
                    momentum_rank: *rank_momentum.get(&ticker).unwrap_or(&0.5),
                    volatility_rank: *rank_volatility.get(&ticker).unwrap_or(&0.5),
                    volume_rank: *rank_volume.get(&ticker).unwrap_or(&0.5),
                    return_zscore: *z_returns.get(&ticker).unwrap_or(&0.0),
                    momentum_zscore: *z_momentum.get(&ticker).unwrap_or(&0.0),
                },
            );
        }

        Some(snapshot)
    }

    fn snapshot_for_date(
        &self,
        date: DateTime<Utc>,
    ) -> Option<Arc<HashMap<String, CrossSectionalFeatures>>> {
        if let Some(existing) = self.per_date_cache.get(&date) {
            return Some(existing.clone());
        }

        match self.per_date_cache.entry(date) {
            Entry::Occupied(existing) => Some(existing.get().clone()),
            Entry::Vacant(slot) => {
                let snapshot = self.build_snapshot_for_date(date)?;
                let snapshot_arc = Arc::new(snapshot);
                slot.insert(snapshot_arc.clone());
                Some(snapshot_arc)
            }
        }
    }

    fn get_features(&self, ticker: &str, date: DateTime<Utc>) -> Option<CrossSectionalFeatures> {
        self.snapshot_for_date(date)
            .and_then(|snapshot| snapshot.get(ticker).copied())
    }
}

static CROSS_SECTIONAL_CONTEXT: OnceLock<Mutex<Option<Arc<CrossSectionalContext>>>> =
    OnceLock::new();

fn cross_context_slot() -> &'static Mutex<Option<Arc<CrossSectionalContext>>> {
    CROSS_SECTIONAL_CONTEXT.get_or_init(|| Mutex::new(None))
}

fn set_global_cross_sectional_context(context: Option<Arc<CrossSectionalContext>>) {
    if let Ok(mut slot) = cross_context_slot().lock() {
        *slot = context;
    }
}

fn get_global_cross_sectional_context() -> Option<Arc<CrossSectionalContext>> {
    cross_context_slot()
        .lock()
        .ok()
        .and_then(|slot| slot.clone())
}

pub fn prime_cross_sectional_context_from_ref_map(
    candles_by_ticker: &HashMap<String, Vec<&Candle>>,
) -> Option<()> {
    let context = CrossSectionalContext::new(candles_by_ticker).map(Arc::new);
    set_global_cross_sectional_context(context.clone());
    context.map(|_| ())
}

#[allow(dead_code)]
pub fn prime_cross_sectional_context_from_owned_map(
    candles_by_ticker: &HashMap<String, Vec<Candle>>,
) -> Option<()> {
    let mut ref_map: HashMap<String, Vec<&Candle>> = HashMap::new();
    for (ticker, candles) in candles_by_ticker {
        ref_map.insert(ticker.clone(), candles.iter().collect());
    }
    prime_cross_sectional_context_from_ref_map(&ref_map)
}

#[derive(Debug)]
enum FeatureStatus {
    Vector(FeatureVector),
    OutOfBounds,
    InsufficientHistory,
    Failed,
}

pub struct LightGBMStrategy {
    template_id: String,
    feature_config: FeatureConfig,
    min_confidence: f64,
    model_bias: f64,
    model_id: Option<String>,
    decision_summary: Mutex<LightGBMSummary>,
}

static LIGHTGBM_MODELS: OnceLock<DashMap<String, Arc<LightGBMBooster>>> = OnceLock::new();
static LIGHTGBM_DEFAULT_MODEL_ID: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static LIGHTGBM_MISSING_LOGGED: OnceLock<DashSet<String>> = OnceLock::new();

fn lightgbm_models() -> &'static DashMap<String, Arc<LightGBMBooster>> {
    LIGHTGBM_MODELS.get_or_init(DashMap::new)
}

fn lightgbm_missing_logged() -> &'static DashSet<String> {
    LIGHTGBM_MISSING_LOGGED.get_or_init(DashSet::new)
}

fn set_default_model_id(model_id: String) {
    let entry = LIGHTGBM_DEFAULT_MODEL_ID.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = entry.lock() {
        *guard = Some(model_id);
    }
}

fn get_default_model_id() -> Option<String> {
    LIGHTGBM_DEFAULT_MODEL_ID
        .get_or_init(|| Mutex::new(None))
        .lock()
        .ok()
        .and_then(|guard| guard.clone())
}

fn log_missing_model_once(model_id: &str) {
    if lightgbm_missing_logged().insert(model_id.to_string()) {
        debug!(
            "LightGBM model {} not registered; skipping model scoring.",
            model_id
        );
    }
}

#[derive(Debug)]
struct LightGBMTree {
    split_features: Vec<usize>,
    thresholds: Vec<f64>,
    left_child: Vec<i32>,
    right_child: Vec<i32>,
    leaf_values: Vec<f64>,
    shrinkage: f64,
}

impl LightGBMTree {
    fn from_lines(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Result<Self> {
        let mut num_leaves: Option<usize> = None;
        let mut split_features = Vec::new();
        let mut thresholds = Vec::new();
        let mut left_child = Vec::new();
        let mut right_child = Vec::new();
        let mut leaf_values = Vec::new();
        let mut shrinkage = 1.0;

        while let Some(peeked) = lines.peek() {
            if peeked.starts_with("Tree=") {
                break;
            }
            let line = lines.next().unwrap().trim();

            if line.starts_with("num_leaves=") {
                num_leaves = Some(parse_value(line, "num_leaves=")?);
            } else if line.starts_with("split_feature=") {
                split_features = parse_array(line, "split_feature=")?;
            } else if line.starts_with("threshold=") {
                thresholds = parse_array(line, "threshold=")?;
            } else if line.starts_with("left_child=") {
                left_child = parse_array(line, "left_child=")?;
            } else if line.starts_with("right_child=") {
                right_child = parse_array(line, "right_child=")?;
            } else if line.starts_with("leaf_value=") {
                leaf_values = parse_array(line, "leaf_value=")?;
            } else if line.starts_with("shrinkage=") {
                shrinkage = parse_value(line, "shrinkage=")?;
            }
        }

        let internal_nodes = split_features.len();
        if thresholds.len() != internal_nodes
            || left_child.len() != internal_nodes
            || right_child.len() != internal_nodes
        {
            return Err(anyhow!(
                "LightGBM tree definition invalid: split/child/threshold length mismatch"
            ));
        }

        let declared_leaves = num_leaves.unwrap_or_else(|| leaf_values.len());
        if declared_leaves != leaf_values.len() {
            return Err(anyhow!(
                "LightGBM tree leaf count mismatch: expected {declared_leaves}, found {}",
                leaf_values.len()
            ));
        }

        Ok(Self {
            split_features,
            thresholds,
            left_child,
            right_child,
            leaf_values,
            shrinkage,
        })
    }

    fn predict(&self, features: &[f64]) -> f64 {
        let mut node_idx = 0usize;
        loop {
            let feature_idx = self
                .split_features
                .get(node_idx)
                .copied()
                .unwrap_or_default();
            let threshold = self.thresholds.get(node_idx).copied().unwrap_or(0.0);
            let feature_value = *features.get(feature_idx).unwrap_or(&0.0);
            let child = if feature_value <= threshold {
                self.left_child.get(node_idx).copied().unwrap_or(-1)
            } else {
                self.right_child.get(node_idx).copied().unwrap_or(-1)
            };

            if child < 0 {
                let leaf_idx = (-child - 1) as usize;
                return self.leaf_values.get(leaf_idx).copied().unwrap_or_default()
                    * self.shrinkage;
            }

            node_idx = child as usize;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LightGBMObjectiveKind {
    Binary,
    Multiclass,
}

#[derive(Debug)]
struct LightGBMBooster {
    trees: Vec<LightGBMTree>,
    feature_count: usize,
    sigmoid: f64,
    num_classes: usize,
    num_tree_per_iteration: usize,
    objective: LightGBMObjectiveKind,
}

impl LightGBMBooster {
    fn from_model_text(text: &str) -> Result<Self> {
        let mut lines = text.lines().peekable();
        let mut trees = Vec::new();
        let mut max_feature_idx: Option<usize> = None;
        let mut sigmoid = 1.0;
        let mut num_classes: usize = 1;
        let mut num_tree_per_iteration: usize = 1;
        let mut objective = LightGBMObjectiveKind::Binary;

        while let Some(line) = lines.next() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            if trimmed.starts_with("objective=") {
                sigmoid = extract_sigmoid(trimmed);
                if trimmed.contains("multiclass") {
                    objective = LightGBMObjectiveKind::Multiclass;
                } else {
                    objective = LightGBMObjectiveKind::Binary;
                }
            } else if trimmed.starts_with("num_class=") {
                num_classes = parse_value(trimmed, "num_class=")?;
            } else if trimmed.starts_with("num_tree_per_iteration=") {
                num_tree_per_iteration = parse_value(trimmed, "num_tree_per_iteration=")?;
            } else if trimmed.starts_with("max_feature_idx=") {
                max_feature_idx = Some(parse_value(trimmed, "max_feature_idx=")?);
            }

            if trimmed.starts_with("Tree=") {
                let tree = LightGBMTree::from_lines(&mut lines)?;
                trees.push(tree);
            }
        }

        if trees.is_empty() {
            return Err(anyhow!("LightGBM model contained no trees"));
        }

        let inferred_max_feature = trees
            .iter()
            .flat_map(|tree| tree.split_features.iter())
            .copied()
            .max()
            .unwrap_or(0);
        let feature_count = max_feature_idx
            .map(|idx| idx + 1)
            .unwrap_or(inferred_max_feature + 1);
        if matches!(objective, LightGBMObjectiveKind::Multiclass) && num_classes < 2 {
            num_classes = num_tree_per_iteration.max(2);
        }
        if num_tree_per_iteration == 0 {
            num_tree_per_iteration = 1;
        }

        Ok(Self {
            trees,
            feature_count,
            sigmoid,
            num_classes,
            num_tree_per_iteration,
            objective,
        })
    }

    fn predict_distribution(&self, features: &[f64]) -> Option<Vec<f64>> {
        if features.len() < self.feature_count || self.trees.is_empty() {
            return None;
        }

        let mut raw_scores = vec![0.0; self.num_tree_per_iteration];
        if raw_scores.is_empty() {
            return None;
        }
        for (tree_idx, tree) in self.trees.iter().enumerate() {
            let bucket = tree_idx % self.num_tree_per_iteration;
            if let Some(slot) = raw_scores.get_mut(bucket) {
                *slot += tree.predict(features);
            }
        }

        match self.objective {
            LightGBMObjectiveKind::Binary => {
                let logit = raw_scores.first().copied().unwrap_or(0.0) * self.sigmoid;
                let probability = 1.0 / (1.0 + (-logit).exp());
                Some(vec![
                    (1.0 - probability).clamp(0.0, 1.0),
                    probability.clamp(0.0, 1.0),
                ])
            }
            LightGBMObjectiveKind::Multiclass => {
                let class_count = self.num_classes.max(2);
                let mut trimmed = raw_scores;
                trimmed.truncate(class_count);
                let max_score = trimmed
                    .iter()
                    .copied()
                    .fold(f64::NEG_INFINITY, |acc, value| acc.max(value));
                let mut exp_scores: Vec<f64> = trimmed
                    .iter()
                    .map(|score| (score - max_score).exp())
                    .collect();
                let denom = exp_scores.iter().sum::<f64>();
                if !denom.is_finite() || denom <= EPSILON {
                    return None;
                }
                for score in exp_scores.iter_mut() {
                    *score /= denom;
                }
                Some(exp_scores)
            }
        }
    }

    fn num_features(&self) -> usize {
        self.feature_count
    }
}

fn parse_value<T>(line: &str, prefix: &str) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let raw = line
        .strip_prefix(prefix)
        .ok_or_else(|| anyhow!("Expected prefix {prefix}"))?;
    raw.trim().parse::<T>().map_err(|err| {
        anyhow!(
            "Failed to parse value for {prefix} from \"{line}\" while loading LightGBM model: {err}"
        )
    })
}

fn parse_array<T>(line: &str, prefix: &str) -> Result<Vec<T>>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let raw = line
        .strip_prefix(prefix)
        .ok_or_else(|| anyhow!("Expected prefix {prefix}"))?;
    raw.split_whitespace()
        .map(|token| {
            token
                .parse::<T>()
                .map_err(|err| anyhow!("Failed to parse value {token} for {prefix}: {err}"))
        })
        .collect()
}

fn extract_sigmoid(objective_line: &str) -> f64 {
    objective_line
        .split_whitespace()
        .find_map(|token| token.strip_prefix("sigmoid:"))
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(1.0)
}

fn register_lightgbm_model(model_id: &str, text: &str, set_default: bool) -> Result<()> {
    let booster = LightGBMBooster::from_model_text(text)?;
    lightgbm_models().insert(model_id.to_string(), Arc::new(booster));
    if set_default || get_default_model_id().is_none() {
        set_default_model_id(model_id.to_string());
    }
    Ok(())
}

pub fn register_model_text(model_id: &str, text: &str, set_default: bool) -> Result<()> {
    if model_id.trim().is_empty() {
        return Err(anyhow!("LightGBM model id cannot be empty"));
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("LightGBM model text was empty."));
    }
    register_lightgbm_model(model_id, trimmed, set_default)
}

fn load_lightgbm_booster_for_model(model_id: Option<&str>) -> Result<Option<Arc<LightGBMBooster>>> {
    let resolved = if let Some(id) = model_id {
        id.to_string()
    } else if let Some(default_id) = get_default_model_id() {
        default_id
    } else {
        return Ok(None);
    };

    if let Some(entry) = lightgbm_models().get(&resolved) {
        return Ok(Some(entry.value().clone()));
    }

    log_missing_model_once(&resolved);
    Ok(None)
}

fn minimum_history_needed(config: &FeatureConfig) -> usize {
    config
        .ma_trend_slow
        .max(config.ma_slow + 1)
        .max(config.ma_fast + 1)
        .max(config.momentum_long + 1)
        .max(config.momentum_short + 1)
        .max(config.volatility_long + 1)
        .max(config.volatility_short + 1)
        .max(config.bollinger_period + 1)
        .max(config.correlation_window + 1)
        .max(config.atr_period + 1)
        .max(config.stochastic_period + 1)
        .max(config.cci_period + 1)
}

#[derive(Clone)]
pub struct PrecomputedInputs {
    closes: Vec<f64>,
    highs: Vec<f64>,
    lows: Vec<f64>,
    opens: Vec<f64>,
    volumes: Vec<f64>,
    returns: Vec<f64>,
    log_returns: Vec<f64>,
    volume_changes: Vec<f64>,
    ma_fast_series: Vec<f64>,
    ma_slow_series: Vec<f64>,
    ma_trend_series: Vec<f64>,
    ma_trend_slow_series: Vec<f64>,
    rsi_series: Vec<f64>,
    percent_k_series: Vec<f64>,
    macd_hist_series: Vec<f64>,
    typical_prices: Vec<f64>,
    bb_upper: Vec<f64>,
    bb_middle: Vec<f64>,
    bb_lower: Vec<f64>,
    atr_series: Vec<f64>,
}

pub fn precompute_inputs_for_ticker(
    candles: &[&Candle],
    config: FeatureConfig,
) -> Option<PrecomputedInputs> {
    if candles.is_empty() {
        return None;
    }

    let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
    let highs: Vec<f64> = candles.iter().map(|c| c.high).collect();
    let lows: Vec<f64> = candles.iter().map(|c| c.low).collect();
    let opens: Vec<f64> = candles.iter().map(|c| c.open).collect();
    let volumes: Vec<f64> = candles.iter().map(|c| c.volume_shares as f64).collect();

    let mut returns = vec![0.0; closes.len()];
    let mut log_returns = vec![0.0; closes.len()];
    for i in 1..closes.len() {
        let prev = closes[i - 1];
        if prev.abs() > EPSILON {
            returns[i] = (closes[i] - prev) / prev;
            log_returns[i] = (closes[i] / prev).ln();
        }
    }

    let mut volume_changes = vec![0.0; volumes.len()];
    for i in 1..volumes.len() {
        let prev = volumes[i - 1];
        if prev.abs() > EPSILON {
            volume_changes[i] = (volumes[i] / prev) - 1.0;
        }
    }

    let ma_fast_series = indicators::calculate_sma(&closes, config.ma_fast);
    let ma_slow_series = indicators::calculate_sma(&closes, config.ma_slow);
    let ma_trend_series = indicators::calculate_sma(&closes, config.ma_trend);
    let ma_trend_slow_series = indicators::calculate_sma(&closes, config.ma_trend_slow);
    let rsi_series = indicators::calculate_rsi(&closes, config.rsi_period);

    let stoch_period = config.stochastic_period.max(2);
    let mut percent_k_series = vec![f64::NAN; closes.len()];
    for i in (stoch_period - 1)..closes.len() {
        let start = i + 1 - stoch_period;
        let highest_high = highs[start..=i]
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        let lowest_low = lows[start..=i]
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let range = highest_high - lowest_low;
        percent_k_series[i] = if range.abs() > EPSILON {
            safe_div(closes[i] - lowest_low, range) * 100.0
        } else {
            50.0
        };
    }

    let (_, _, macd_hist_series) = indicators::calculate_macd(&closes, 12, 26, 9);

    let typical_prices: Vec<f64> = closes
        .iter()
        .zip(highs.iter())
        .zip(lows.iter())
        .map(|((c, h), l)| (c + h + l) / 3.0)
        .collect();

    let (bb_upper, bb_middle, bb_lower) = indicators::calculate_bollinger_bands(
        &closes,
        config.bollinger_period,
        config.bollinger_std,
    );

    let mut atr_series = vec![0.0; candles.len()];
    for i in 0..candles.len() {
        atr_series[i] =
            indicators::calculate_atr_from_candles(candles, i, config.atr_period).unwrap_or(0.0);
    }

    Some(PrecomputedInputs {
        closes,
        highs,
        lows,
        opens,
        volumes,
        returns,
        log_returns,
        volume_changes,
        ma_fast_series,
        ma_slow_series,
        ma_trend_series,
        ma_trend_slow_series,
        rsi_series,
        percent_k_series,
        macd_hist_series,
        typical_prices,
        bb_upper,
        bb_middle,
        bb_lower,
        atr_series,
    })
}

pub fn compute_features_from_precomputed(
    ticker: &str,
    candles: &[&Candle],
    candle_index: usize,
    config: FeatureConfig,
    pre: &PrecomputedInputs,
    cross_context: Option<Arc<CrossSectionalContext>>,
) -> Option<FeatureVector> {
    if candles.len() <= candle_index {
        return None;
    }

    let required_history = minimum_history_needed(&config);
    if candle_index + 1 < required_history {
        return None;
    }

    let close_now = pre.closes[candle_index];
    let open_now = pre.opens[candle_index];
    let high_now = pre.highs[candle_index];
    let low_now = pre.lows[candle_index];
    let prev_close = if candle_index > 0 {
        pre.closes[candle_index - 1]
    } else {
        close_now
    };

    let daily_return = pre.returns[candle_index];
    let log_return = pre.log_returns[candle_index];
    let overnight_return = if prev_close.abs() > EPSILON {
        (open_now / prev_close) - 1.0
    } else {
        0.0
    };
    let high_low_range = safe_div(high_now - low_now, close_now.abs());
    let close_open_return = safe_div(close_now - open_now, open_now.abs());
    let gap_return = overnight_return;

    let rolling_mean_short =
        rolling_mean_at(&pre.returns, candle_index, config.volatility_short).unwrap_or(0.0);
    let rolling_mean_long =
        rolling_mean_at(&pre.returns, candle_index, config.volatility_long).unwrap_or(0.0);
    let rolling_std_short =
        rolling_std_at(&pre.returns, candle_index, config.volatility_short).unwrap_or(0.0);
    let rolling_std_long =
        rolling_std_at(&pre.returns, candle_index, config.volatility_long).unwrap_or(0.0);
    let rolling_sharpe_short = if rolling_std_short.abs() <= EPSILON {
        0.0
    } else {
        rolling_mean_short / rolling_std_short
    };
    let rolling_sharpe_long = if rolling_std_long.abs() <= EPSILON {
        0.0
    } else {
        rolling_mean_long / rolling_std_long
    };
    let rolling_max_return =
        rolling_max_at(&pre.returns, candle_index, config.volatility_long).unwrap_or(0.0);
    let rolling_min_return =
        rolling_min_at(&pre.returns, candle_index, config.volatility_long).unwrap_or(0.0);

    let momentum_short = if candle_index + 1 > config.momentum_short {
        let past = pre.closes[candle_index + 1 - config.momentum_short];
        if past.abs() > EPSILON {
            (close_now / past) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };
    let momentum_long = if candle_index + 1 > config.momentum_long {
        let past = pre.closes[candle_index + 1 - config.momentum_long];
        if past.abs() > EPSILON {
            (close_now / past) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    let lagged_return_2d = if candle_index >= 2 {
        let past = pre.closes[candle_index - 2];
        if past.abs() > EPSILON {
            (close_now / past) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };
    let lagged_return_5d = if candle_index >= 5 {
        let past = pre.closes[candle_index - 5];
        if past.abs() > EPSILON {
            (close_now / past) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    let volume_change_1d = pre.volume_changes[candle_index];
    let volume_mean_ratio_10 = rolling_mean_at(&pre.volumes, candle_index, 10)
        .map_or(0.0, |avg| safe_div(pre.volumes[candle_index], avg) - 1.0);
    let volume_mean_ratio_20 = rolling_mean_at(&pre.volumes, candle_index, 20)
        .map_or(0.0, |avg| safe_div(pre.volumes[candle_index], avg) - 1.0);
    let volume_volatility_20 = rolling_std_at(&pre.volume_changes, candle_index, 20).unwrap_or(0.0);
    let dollar_volume_log = (close_now.abs() * pre.volumes[candle_index].abs()).ln_1p();

    let ma_fast = pre
        .ma_fast_series
        .get(candle_index)
        .copied()
        .unwrap_or(close_now);
    let ma_slow = pre
        .ma_slow_series
        .get(candle_index)
        .copied()
        .unwrap_or(close_now);
    let ma_trend = pre
        .ma_trend_series
        .get(candle_index)
        .copied()
        .unwrap_or(close_now);
    let ma_trend_slow = pre
        .ma_trend_slow_series
        .get(candle_index)
        .copied()
        .unwrap_or(close_now);

    let ma_fast_over_slow = safe_div(ma_fast, ma_slow);
    let ma_trend_over_slow = safe_div(ma_trend, ma_trend_slow);
    let price_rel_ma_trend = safe_div(close_now, ma_trend) - 1.0;
    let price_rel_ma_slow = safe_div(close_now, ma_slow) - 1.0;
    let trend_strength = safe_div(close_now - ma_slow, ma_trend_slow);
    let ma_trend_slope_5 = if candle_index >= 5 {
        let past = pre
            .ma_trend_series
            .get(candle_index - 5)
            .copied()
            .unwrap_or(ma_trend);
        safe_div(ma_trend - past, past.abs().max(EPSILON))
    } else {
        0.0
    };

    let rsi_value = pre
        .rsi_series
        .get(candle_index)
        .copied()
        .unwrap_or(50.0)
        .clamp(0.0, 100.0);

    let percent_k = pre.percent_k_series[candle_index].clamp(0.0, 100.0);
    let percent_d = if candle_index + 1 >= config.stochastic_smooth {
        let start = candle_index + 1 - config.stochastic_smooth;
        let smooth_slice: Vec<f64> = pre.percent_k_series[start..=candle_index]
            .iter()
            .copied()
            .filter(|value| value.is_finite())
            .collect();
        if smooth_slice.is_empty() {
            percent_k
        } else {
            mean(&smooth_slice).unwrap_or(percent_k)
        }
    } else {
        percent_k
    };

    let macd_hist = pre
        .macd_hist_series
        .get(candle_index)
        .copied()
        .unwrap_or(0.0);

    let typical_price_now = *pre
        .typical_prices
        .get(candle_index)
        .unwrap_or(&((close_now + high_now + low_now) / 3.0));
    let cci_value = if candle_index + 1 >= config.cci_period {
        let start = candle_index + 1 - config.cci_period;
        let window = &pre.typical_prices[start..=candle_index];
        let tp_mean = mean(window).unwrap_or(0.0);
        let mean_dev = window
            .iter()
            .map(|value| (*value - tp_mean).abs())
            .sum::<f64>()
            / window.len() as f64;
        if mean_dev <= EPSILON {
            0.0
        } else {
            (typical_price_now - tp_mean) / (0.015 * mean_dev)
        }
    } else {
        0.0
    };

    let williams_r = if candle_index + 1 >= config.stochastic_period {
        let start = candle_index + 1 - config.stochastic_period;
        let highest_high = pre.highs[start..=candle_index]
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        let lowest_low = pre.lows[start..=candle_index]
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let range = highest_high - lowest_low;
        if range.abs() > EPSILON {
            -100.0 * (highest_high - close_now) / range
        } else {
            -50.0
        }
    } else {
        -50.0
    };

    let atr = pre.atr_series.get(candle_index).copied().unwrap_or(0.0);
    let atr_normalized = safe_div(atr, close_now.abs());

    let bb_index = candle_index + 1 - config.bollinger_period;
    let (bollinger_pct_b, bollinger_bandwidth) =
        if candle_index + 1 >= config.bollinger_period && bb_index < pre.bb_upper.len() {
            let upper = pre.bb_upper[bb_index];
            let lower = pre.bb_lower[bb_index];
            let middle = pre
                .bb_middle
                .get(candle_index)
                .copied()
                .unwrap_or(close_now);
            let span = upper - lower;
            let pct_b = if span.abs() > EPSILON {
                (close_now - lower) / span
            } else {
                0.5
            };
            let bandwidth = if middle.abs() > EPSILON {
                span / middle
            } else {
                0.0
            };
            (pct_b, bandwidth)
        } else {
            (0.5, 0.0)
        };

    let breakout_high_ratio = rolling_max_at(&pre.closes, candle_index, config.bollinger_period)
        .map_or(1.0, |max_c| safe_div(close_now, max_c));
    let breakout_low_ratio = rolling_min_at(&pre.closes, candle_index, config.bollinger_period)
        .map_or(1.0, |min_c| safe_div(close_now, min_c));

    let upper_shadow = (high_now - open_now.max(close_now)).max(0.0);
    let lower_shadow = (open_now.min(close_now) - low_now).max(0.0);
    let body = (close_now - open_now).abs().max(EPSILON);
    let upper_shadow_ratio = safe_div(upper_shadow, close_now.abs().max(EPSILON));
    let lower_shadow_ratio = safe_div(lower_shadow, close_now.abs().max(EPSILON));
    let shadow_to_body_upper = safe_div(upper_shadow, body);
    let shadow_to_body_lower = safe_div(lower_shadow, body);

    let return_volume_corr = rolling_corr_at(
        &pre.returns,
        &pre.volume_changes,
        candle_index,
        config.correlation_window,
    )
    .unwrap_or(0.0);

    let cross_sectional = cross_context
        .or_else(get_global_cross_sectional_context)
        .as_ref()
        .and_then(|ctx| ctx.get_features(ticker, candles[candle_index].date))
        .unwrap_or_default();

    let mut values = Vec::new();
    values.extend_from_slice(&[
        daily_return,
        log_return,
        overnight_return,
        high_low_range,
        close_open_return,
        gap_return,
        rolling_mean_short,
        rolling_mean_long,
        rolling_std_short,
        rolling_std_long,
        rolling_sharpe_short,
        rolling_sharpe_long,
        rolling_max_return,
        rolling_min_return,
        momentum_short,
        momentum_long,
        lagged_return_2d,
        lagged_return_5d,
        volume_change_1d,
        volume_mean_ratio_10,
        volume_mean_ratio_20,
        volume_volatility_20,
        dollar_volume_log,
        ma_fast_over_slow,
        ma_trend_over_slow,
        price_rel_ma_trend,
        price_rel_ma_slow,
        trend_strength,
        ma_trend_slope_5,
        rsi_value,
        percent_k,
        percent_d,
        macd_hist,
        cci_value,
        williams_r,
        atr_normalized,
        bollinger_pct_b,
        bollinger_bandwidth,
        breakout_high_ratio,
        breakout_low_ratio,
        upper_shadow_ratio,
        lower_shadow_ratio,
        shadow_to_body_upper,
        shadow_to_body_lower,
        return_volume_corr,
        cross_sectional.return_rank,
        cross_sectional.momentum_rank,
        cross_sectional.volatility_rank,
        cross_sectional.volume_rank,
        cross_sectional.return_zscore,
        cross_sectional.momentum_zscore,
    ]);

    Some(FeatureVector { values })
}

#[allow(dead_code)]
pub fn compute_features_from_refs(
    ticker: &str,
    candles: &[&Candle],
    candle_index: usize,
    config: FeatureConfig,
    cross_context: Option<Arc<CrossSectionalContext>>,
) -> Option<FeatureVector> {
    if candles.len() <= candle_index {
        return None;
    }

    let required_history = minimum_history_needed(&config);
    if candle_index + 1 < required_history {
        return None;
    }

    let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
    let highs: Vec<f64> = candles.iter().map(|c| c.high).collect();
    let lows: Vec<f64> = candles.iter().map(|c| c.low).collect();
    let opens: Vec<f64> = candles.iter().map(|c| c.open).collect();
    let volumes: Vec<f64> = candles.iter().map(|c| c.volume_shares as f64).collect();

    let close_now = closes[candle_index];
    let open_now = opens[candle_index];
    let high_now = highs[candle_index];
    let low_now = lows[candle_index];
    let prev_close = if candle_index > 0 {
        closes[candle_index - 1]
    } else {
        close_now
    };

    let mut returns = vec![0.0; closes.len()];
    let mut log_returns = vec![0.0; closes.len()];
    for i in 1..closes.len() {
        let prev = closes[i - 1];
        if prev.abs() > EPSILON {
            returns[i] = (closes[i] - prev) / prev;
            log_returns[i] = (closes[i] / prev).ln();
        }
    }

    let mut volume_changes = vec![0.0; volumes.len()];
    for i in 1..volumes.len() {
        let prev = volumes[i - 1];
        if prev.abs() > EPSILON {
            volume_changes[i] = (volumes[i] / prev) - 1.0;
        }
    }

    let daily_return = returns[candle_index];
    let log_return = log_returns[candle_index];
    let overnight_return = if prev_close.abs() > EPSILON {
        (open_now / prev_close) - 1.0
    } else {
        0.0
    };
    let high_low_range = safe_div(high_now - low_now, close_now.abs());
    let close_open_return = safe_div(close_now - open_now, open_now.abs());
    let gap_return = overnight_return;

    let rolling_mean_short =
        rolling_mean_at(&returns, candle_index, config.volatility_short).unwrap_or(0.0);
    let rolling_mean_long =
        rolling_mean_at(&returns, candle_index, config.volatility_long).unwrap_or(0.0);
    let rolling_std_short =
        rolling_std_at(&returns, candle_index, config.volatility_short).unwrap_or(0.0);
    let rolling_std_long =
        rolling_std_at(&returns, candle_index, config.volatility_long).unwrap_or(0.0);
    let rolling_sharpe_short = if rolling_std_short.abs() <= EPSILON {
        0.0
    } else {
        rolling_mean_short / rolling_std_short
    };
    let rolling_sharpe_long = if rolling_std_long.abs() <= EPSILON {
        0.0
    } else {
        rolling_mean_long / rolling_std_long
    };
    let rolling_max_return =
        rolling_max_at(&returns, candle_index, config.volatility_long).unwrap_or(0.0);
    let rolling_min_return =
        rolling_min_at(&returns, candle_index, config.volatility_long).unwrap_or(0.0);

    let momentum_short = if candle_index + 1 > config.momentum_short {
        let past = closes[candle_index + 1 - config.momentum_short];
        if past.abs() > EPSILON {
            (close_now / past) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };
    let momentum_long = if candle_index + 1 > config.momentum_long {
        let past = closes[candle_index + 1 - config.momentum_long];
        if past.abs() > EPSILON {
            (close_now / past) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    let lagged_return_2d = if candle_index >= 2 {
        let past = closes[candle_index - 2];
        if past.abs() > EPSILON {
            (close_now / past) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };
    let lagged_return_5d = if candle_index >= 5 {
        let past = closes[candle_index - 5];
        if past.abs() > EPSILON {
            (close_now / past) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    };

    let volume_change_1d = volume_changes[candle_index];
    let volume_mean_ratio_10 = rolling_mean_at(&volumes, candle_index, 10)
        .map_or(0.0, |avg| safe_div(volumes[candle_index], avg) - 1.0);
    let volume_mean_ratio_20 = rolling_mean_at(&volumes, candle_index, 20)
        .map_or(0.0, |avg| safe_div(volumes[candle_index], avg) - 1.0);
    let volume_volatility_20 = rolling_std_at(&volume_changes, candle_index, 20).unwrap_or(0.0);
    let dollar_volume_log = (close_now.abs() * volumes[candle_index].abs()).ln_1p();

    let ma_fast_series = indicators::calculate_sma(&closes, config.ma_fast);
    let ma_slow_series = indicators::calculate_sma(&closes, config.ma_slow);
    let ma_trend_series = indicators::calculate_sma(&closes, config.ma_trend);
    let ma_trend_slow_series = indicators::calculate_sma(&closes, config.ma_trend_slow);

    let ma_fast = ma_fast_series
        .get(candle_index)
        .copied()
        .unwrap_or(close_now);
    let ma_slow = ma_slow_series
        .get(candle_index)
        .copied()
        .unwrap_or(close_now);
    let ma_trend = ma_trend_series
        .get(candle_index)
        .copied()
        .unwrap_or(close_now);
    let ma_trend_slow = ma_trend_slow_series
        .get(candle_index)
        .copied()
        .unwrap_or(close_now);

    let ma_fast_over_slow = safe_div(ma_fast, ma_slow);
    let ma_trend_over_slow = safe_div(ma_trend, ma_trend_slow);
    let price_rel_ma_trend = safe_div(close_now, ma_trend) - 1.0;
    let price_rel_ma_slow = safe_div(close_now, ma_slow) - 1.0;
    let trend_strength = safe_div(close_now - ma_slow, ma_trend_slow);
    let ma_trend_slope_5 = if candle_index >= 5 {
        let past = ma_trend_series
            .get(candle_index - 5)
            .copied()
            .unwrap_or(ma_trend);
        safe_div(ma_trend - past, past.abs().max(EPSILON))
    } else {
        0.0
    };

    let rsi_series = indicators::calculate_rsi(&closes, config.rsi_period);
    let rsi_value = rsi_series
        .get(candle_index)
        .copied()
        .unwrap_or(50.0)
        .clamp(0.0, 100.0);

    let stoch_period = config.stochastic_period.max(2);
    let mut percent_k_series = vec![f64::NAN; candle_index + 1];
    for i in (stoch_period - 1)..=candle_index {
        let start = i + 1 - stoch_period;
        let highest_high = highs[start..=i]
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        let lowest_low = lows[start..=i]
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let range = highest_high - lowest_low;
        percent_k_series[i] = if range.abs() > EPSILON {
            safe_div(closes[i] - lowest_low, range) * 100.0
        } else {
            50.0
        };
    }
    let percent_k = percent_k_series[candle_index].clamp(0.0, 100.0);
    let percent_d = if candle_index + 1 >= config.stochastic_smooth {
        let start = candle_index + 1 - config.stochastic_smooth;
        let smooth_slice: Vec<f64> = percent_k_series[start..=candle_index]
            .iter()
            .cloned()
            .filter(|value| value.is_finite())
            .collect();
        if smooth_slice.is_empty() {
            percent_k
        } else {
            mean(&smooth_slice).unwrap_or(percent_k)
        }
    } else {
        percent_k
    };

    let (_, _, macd_hist_series) = indicators::calculate_macd(&closes, 12, 26, 9);
    let macd_hist = macd_hist_series.get(candle_index).copied().unwrap_or(0.0);

    let typical_prices: Vec<f64> = closes
        .iter()
        .zip(highs.iter())
        .zip(lows.iter())
        .map(|((c, h), l)| (c + h + l) / 3.0)
        .collect();
    let cci_value = if candle_index + 1 >= config.cci_period {
        let start = candle_index + 1 - config.cci_period;
        let window = &typical_prices[start..=candle_index];
        let tp_mean = mean(window).unwrap_or(0.0);
        let mean_dev = window
            .iter()
            .map(|value| (*value - tp_mean).abs())
            .sum::<f64>()
            / window.len() as f64;
        if mean_dev <= EPSILON {
            0.0
        } else {
            (typical_prices[candle_index] - tp_mean) / (0.015 * mean_dev)
        }
    } else {
        0.0
    };

    let williams_r = if candle_index + 1 >= config.stochastic_period {
        let start = candle_index + 1 - config.stochastic_period;
        let highest_high = highs[start..=candle_index]
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        let lowest_low = lows[start..=candle_index]
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let range = highest_high - lowest_low;
        if range.abs() > EPSILON {
            -100.0 * (highest_high - close_now) / range
        } else {
            -50.0
        }
    } else {
        -50.0
    };

    let atr = indicators::calculate_atr_from_candles(candles, candle_index, config.atr_period)
        .unwrap_or(0.0);
    let atr_normalized = safe_div(atr, close_now.abs());

    let (bb_upper, bb_middle, bb_lower) = indicators::calculate_bollinger_bands(
        &closes,
        config.bollinger_period,
        config.bollinger_std,
    );
    let bb_index = candle_index + 1 - config.bollinger_period;
    let (bollinger_pct_b, bollinger_bandwidth) =
        if candle_index + 1 >= config.bollinger_period && bb_index < bb_upper.len() {
            let upper = bb_upper[bb_index];
            let lower = bb_lower[bb_index];
            let middle = bb_middle.get(candle_index).copied().unwrap_or(close_now);
            let span = upper - lower;
            let pct_b = if span.abs() > EPSILON {
                (close_now - lower) / span
            } else {
                0.5
            };
            let bandwidth = if middle.abs() > EPSILON {
                span / middle
            } else {
                0.0
            };
            (pct_b, bandwidth)
        } else {
            (0.5, 0.0)
        };

    let breakout_high_ratio = rolling_max_at(&closes, candle_index, config.bollinger_period)
        .map_or(1.0, |max_c| safe_div(close_now, max_c));
    let breakout_low_ratio = rolling_min_at(&closes, candle_index, config.bollinger_period)
        .map_or(1.0, |min_c| safe_div(close_now, min_c));

    let upper_shadow = (high_now - open_now.max(close_now)).max(0.0);
    let lower_shadow = (open_now.min(close_now) - low_now).max(0.0);
    let body = (close_now - open_now).abs().max(EPSILON);
    let upper_shadow_ratio = safe_div(upper_shadow, close_now.abs().max(EPSILON));
    let lower_shadow_ratio = safe_div(lower_shadow, close_now.abs().max(EPSILON));
    let shadow_to_body_upper = safe_div(upper_shadow, body);
    let shadow_to_body_lower = safe_div(lower_shadow, body);

    let return_volume_corr = rolling_corr_at(
        &returns,
        &volume_changes,
        candle_index,
        config.correlation_window,
    )
    .unwrap_or(0.0);

    let cross_sectional = cross_context
        .or_else(get_global_cross_sectional_context)
        .as_ref()
        .and_then(|ctx| ctx.get_features(ticker, candles[candle_index].date))
        .unwrap_or_default();

    let mut values = Vec::new();
    values.extend_from_slice(&[
        daily_return,
        log_return,
        overnight_return,
        high_low_range,
        close_open_return,
        gap_return,
        rolling_mean_short,
        rolling_mean_long,
        rolling_std_short,
        rolling_std_long,
        rolling_sharpe_short,
        rolling_sharpe_long,
        rolling_max_return,
        rolling_min_return,
        momentum_short,
        momentum_long,
        lagged_return_2d,
        lagged_return_5d,
        volume_change_1d,
        volume_mean_ratio_10,
        volume_mean_ratio_20,
        volume_volatility_20,
        dollar_volume_log,
        ma_fast_over_slow,
        ma_trend_over_slow,
        price_rel_ma_trend,
        price_rel_ma_slow,
        trend_strength,
        ma_trend_slope_5,
        rsi_value,
        percent_k,
        percent_d,
        macd_hist,
        cci_value,
        williams_r,
        atr_normalized,
        bollinger_pct_b,
        bollinger_bandwidth,
        breakout_high_ratio,
        breakout_low_ratio,
        upper_shadow_ratio,
        lower_shadow_ratio,
        shadow_to_body_upper,
        shadow_to_body_lower,
        return_volume_corr,
        cross_sectional.return_rank,
        cross_sectional.momentum_rank,
        cross_sectional.volatility_rank,
        cross_sectional.volume_rank,
        cross_sectional.return_zscore,
        cross_sectional.momentum_zscore,
    ]);

    Some(FeatureVector { values })
}

impl LightGBMStrategy {
    pub fn new(template_id: String, parameters: HashMap<String, f64>) -> Self {
        let feature_config = FeatureConfig {
            rsi_period: get_param_usize_rounded_clamped(&parameters, "rsiPeriod", 14, 5, 60),
            atr_period: get_param_usize_rounded_clamped(&parameters, "atrPeriod", 14, 5, 60),
            stochastic_period: get_param_usize_rounded_clamped(
                &parameters,
                "stochPeriod",
                14,
                5,
                60,
            ),
            stochastic_smooth: get_param_usize_rounded_clamped(
                &parameters,
                "stochSmooth",
                3,
                1,
                10,
            ),
            cci_period: get_param_usize_rounded_clamped(&parameters, "cciPeriod", 20, 10, 90),
            bollinger_period: get_param_usize_rounded_clamped(
                &parameters,
                "bollingerPeriod",
                20,
                10,
                90,
            ),
            bollinger_std: get_param_f64_clamped(&parameters, "bollingerStd", 2.0, 1.0, 4.0),
            momentum_short: get_param_usize_rounded_clamped(
                &parameters,
                "momentumShort",
                20,
                5,
                120,
            ),
            momentum_long: get_param_usize_rounded_clamped(
                &parameters,
                "momentumLong",
                60,
                20,
                240,
            ),
            volatility_short: get_param_usize_rounded_clamped(
                &parameters,
                "returnVolShort",
                5,
                2,
                90,
            ),
            volatility_long: get_param_usize_rounded_clamped(
                &parameters,
                "returnVolLong",
                20,
                5,
                180,
            ),
            ma_fast: get_param_usize_rounded_clamped(&parameters, "maFast", 10, 5, 60),
            ma_slow: get_param_usize_rounded_clamped(&parameters, "maSlow", 50, 20, 200),
            ma_trend: get_param_usize_rounded_clamped(&parameters, "maTrend", 20, 10, 90),
            ma_trend_slow: get_param_usize_rounded_clamped(
                &parameters,
                "maTrendSlow",
                200,
                100,
                320,
            ),
            correlation_window: get_param_usize_rounded_clamped(
                &parameters,
                "correlationWindow",
                20,
                5,
                180,
            ),
        };
        let min_confidence = get_param_f64_clamped(&parameters, "minConfidence", 0.1, 0.0, 1.0);
        let model_bias = get_param_f64_clamped(&parameters, "modelBias", 0.01, -2.0, 2.0);
        let model_id = template_id
            .strip_prefix("lightgbm_")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        Self {
            template_id,
            feature_config,
            min_confidence,
            model_bias,
            model_id,
            decision_summary: Mutex::new(LightGBMSummary::default()),
        }
    }

    pub fn default_model_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/models/lightgbm_model.txt")
    }

    pub fn load_model_from_path(path: impl AsRef<Path>) -> Result<()> {
        let path_buf = path.as_ref().to_path_buf();
        if !path_buf.exists() {
            return Err(anyhow!(
                "LightGBM model file not found: {}",
                path_buf.display()
            ));
        }
        let text = fs::read_to_string(&path_buf)
            .with_context(|| format!("Failed to read LightGBM model {}", path_buf.display()))?;
        register_lightgbm_model("default", &text, true)?;
        Ok(())
    }

    pub fn load_model_if_exists(path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if path.exists() {
            Self::load_model_from_path(path)?;
        }
        Ok(())
    }

    fn update_summary<F>(&self, update: F)
    where
        F: FnOnce(&mut LightGBMSummary),
    {
        if let Ok(mut summary) = self.decision_summary.lock() {
            update(&mut summary);
        }
    }

    fn feature_config(&self) -> FeatureConfig {
        self.feature_config
    }

    fn apply_model_bias(&self, probability: f64) -> f64 {
        if !self.model_bias.is_finite() || self.model_bias.abs() <= EPSILON {
            return probability.clamp(0.0, 1.0);
        }

        let clipped = probability.clamp(EPSILON, 1.0 - EPSILON);
        let logit = (clipped / (1.0 - clipped)).ln();
        let shifted = logit + self.model_bias;
        let adjusted = 1.0 / (1.0 + (-shifted).exp());
        adjusted.clamp(0.0, 1.0)
    }

    fn probability_from_distribution(&self, distribution: &[f64]) -> f64 {
        if distribution.len() >= SIGNAL_BUCKET_COUNT {
            distribution[SIGNAL_BUCKET_POSITIVE].clamp(0.0, 1.0)
        } else {
            distribution.last().copied().unwrap_or(0.5).clamp(0.0, 1.0)
        }
    }

    fn collect_features(
        &self,
        ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> FeatureStatus {
        let config = self.feature_config();
        let required = minimum_history_needed(&config);

        if candles.len() <= candle_index {
            return FeatureStatus::OutOfBounds;
        }

        if candle_index + 1 < required {
            return FeatureStatus::InsufficientHistory;
        }

        let candle_refs: Vec<&Candle> = candles.iter().collect();
        let cross_context = get_global_cross_sectional_context();
        match compute_features_from_refs(ticker, &candle_refs, candle_index, config, cross_context)
        {
            Some(features) => {
                debug!(
                    "LightGBM features @{} for {} => {} values",
                    candle_index,
                    ticker,
                    features.values.len()
                );
                FeatureStatus::Vector(features)
            }
            None => {
                debug!(
                    "LightGBM feature computation failed at candle {} for {}",
                    candle_index, ticker
                );
                FeatureStatus::Failed
            }
        }
    }

    fn predict_distribution(&self, features: &FeatureVector) -> Option<Vec<f64>> {
        let booster = match load_lightgbm_booster_for_model(self.model_id.as_deref()) {
            Ok(Some(model)) => model,
            Ok(None) => return None,
            Err(err) => {
                warn!("LightGBM model unavailable: {err}");
                return None;
            }
        };

        if booster.num_features() != features.values.len() {
            warn!(
                "LightGBM model expects {} feature(s), but {} were computed",
                booster.num_features(),
                features.values.len()
            );
            return None;
        }

        booster.predict_distribution(&features.values)
    }

    fn log_summary(&self) {
        if let Ok(summary) = self.decision_summary.lock() {
            if summary.invocations == 0 {
                return;
            }

            info!("LightGBM summary: {}", summary.describe());
        }
    }
}

impl Drop for LightGBMStrategy {
    fn drop(&mut self) {
        self.log_summary();
    }
}

impl super::Strategy for LightGBMStrategy {
    fn get_template_id(&self) -> &str {
        &self.template_id
    }

    fn generate_signal(
        &self,
        ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> StrategySignal {
        self.update_summary(|summary| summary.record_invocation());

        let snapshot = match self.collect_features(ticker, candles, candle_index) {
            FeatureStatus::Vector(features) => features,
            FeatureStatus::OutOfBounds => {
                self.update_summary(|summary| summary.record_out_of_bounds());
                debug!(
                    "LightGBM holding: candle_index {} out of bounds for {} candles",
                    candle_index,
                    candles.len()
                );
                return hold_signal();
            }
            FeatureStatus::InsufficientHistory => {
                self.update_summary(|summary| summary.record_history_gap());
                debug!(
                    "LightGBM holding: insufficient history at candle {}",
                    candle_index
                );
                return hold_signal();
            }
            FeatureStatus::Failed => {
                self.update_summary(|summary| summary.record_feature_failure());
                debug!(
                    "LightGBM holding: feature snapshot unavailable @{}",
                    candle_index
                );
                return hold_signal();
            }
        };

        let distribution = match self.predict_distribution(&snapshot) {
            Some(values) if !values.is_empty() => values,
            _ => {
                self.update_summary(|summary| summary.record_probability_missing());
                debug!(
                    "LightGBM holding: probability distribution missing @{}",
                    candle_index
                );
                return hold_signal();
            }
        };

        if distribution.iter().any(|value| !value.is_finite()) {
            self.update_summary(|summary| summary.record_probability_missing());
            debug!(
                "LightGBM holding: probability distribution invalid @{}",
                candle_index
            );
            return hold_signal();
        }

        let raw_probability = self.probability_from_distribution(&distribution);
        let probability = self.apply_model_bias(raw_probability);

        if let Some((bucket_idx, bucket_prob)) = distribution
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(Ordering::Equal))
        {
            let bucket_name = SIGNAL_BUCKET_NAMES
                .get(bucket_idx)
                .copied()
                .unwrap_or("bucket");
            debug!(
                "LightGBM top bucket {} prob={:.4} aggregated={:.4}",
                bucket_name, bucket_prob, probability
            );
        } else {
            debug!("LightGBM aggregated probability {:.4}", probability);
        }

        let confidence = ((probability - 0.5).abs() * 2.0).clamp(0.0, 1.0);

        let action = if probability >= BUY_PROBABILITY_THRESHOLD
            && meets_confidence_threshold(confidence, self.min_confidence)
        {
            SignalAction::Buy
        } else {
            SignalAction::Hold
        };

        self.update_summary(|summary| summary.record_scored_decision(&action, probability));

        match action {
            SignalAction::Buy => buy_signal(confidence),
            SignalAction::Sell => sell_signal(confidence),
            SignalAction::Hold => hold_signal(),
        }
    }

    fn get_min_data_points(&self) -> usize {
        let config = self.feature_config();

        minimum_history_needed(&config).max(60)
    }
}

pub fn default_model_path() -> PathBuf {
    LightGBMStrategy::default_model_path()
}

pub fn load_model_from_path(path: impl AsRef<Path>) -> Result<()> {
    LightGBMStrategy::load_model_from_path(path)
}

pub fn load_model_if_exists(path: impl AsRef<Path>) -> Result<()> {
    LightGBMStrategy::load_model_if_exists(path)
}

#[cfg(test)]
mod tests {
    use super::LightGBMStrategy;
    use std::collections::HashMap;

    #[test]
    fn probability_from_distribution_binary_uses_positive_class() {
        let strat = LightGBMStrategy::new("lightgbm_test".to_string(), HashMap::new());

        let p_hit = strat.probability_from_distribution(&[0.15, 0.85]);
        assert!(
            (p_hit - 0.85).abs() < 1e-6,
            "expected hit prob 0.85, got {p_hit}"
        );
    }
}
