use anyhow::{anyhow, Context, Result};
use log::{info, warn};
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::fmt::Write as FmtWrite;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use uuid::Uuid;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::Serialize;

use crate::config::{require_setting_date, EngineRuntimeSettings};
use crate::context::AppContext;
use crate::data_context::{MarketData, TickerScope};
use crate::models::Candle;
use crate::strategy::lightgbm::{
    compute_features_from_precomputed, default_model_path, load_model_from_path,
    precompute_inputs_for_ticker, CrossSectionalContext, FeatureConfig,
};
use crate::trading_rules::has_minimum_dollar_volume;

const EXTREME_HORIZON_BARS: usize = 252; // ~1 year
const EXTREME_TARGET_MULTIPLE: f64 = 5.0; // 5x (tune: 3x, 5x, 10x)
const EXTREME_USE_HIGH: bool = true; // use high watermark
const MAX_LOOKAHEAD_BARS: usize = EXTREME_HORIZON_BARS;
const EARLY_STOPPING_ROUNDS: u32 = 100;
const EXTREME_LABEL_COUNT: usize = 2;
const EXTREME_LABEL_NEGATIVE: u8 = 0;
const EXTREME_LABEL_POSITIVE: u8 = 1;
const EXTREME_LABEL_NAMES: [&str; EXTREME_LABEL_COUNT] = ["no_hit", "hit"];
const TOP_K: usize = 10;
const LABEL_GAINS: [u32; 6] = [0, 1, 3, 7, 15, 31];
const RANK_LABEL_LEVELS: u8 = LABEL_GAINS.len() as u8;
const RANK_LABEL_BINS: [f64; 5] = [2.0, 3.0, 4.0, 5.0, 7.0];
const RANK_LABEL_CAP_MULTIPLE: f64 = 10.0;
const TRAIN_AUGMENT_DUPLICATES_PER_ROW: usize = 1;
const TRAIN_AUGMENT_NOISE_RELATIVE: f64 = 0.01;
const TRAIN_AUGMENT_NOISE_ABS: f64 = 1e-4;
const TRAIN_AUGMENT_WEIGHT_SCALE: f64 = 0.5;
const TRAIN_AUGMENT_SEED: u64 = 4242;

#[derive(Clone)]
struct TrainingRow {
    date: DateTime<Utc>,
    features: Vec<f64>,
    label: u8,
    rank_label: u8,
    max_multiple: f64,
    weight: f64,
}

#[derive(Clone)]
struct ScoredRow {
    score: f64,
    label: u8,
    rank_label: u8,
    max_multiple: f64,
}

struct DatasetStats {
    row_count: usize,
    feature_count: usize,
    start_date: Option<String>,
    end_date: Option<String>,
    histogram: [usize; EXTREME_LABEL_COUNT],
    feature_summaries: Vec<DatasetFeatureSummary>,
}

struct DatasetFeatureSummary {
    index: usize,
    observed_finite: usize,
    observed_total: usize,
    non_finite: usize,
    missing: usize,
    min: Option<f64>,
    max: Option<f64>,
    mean: Option<f64>,
    std_dev: Option<f64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LightgbmHyperparametersSummary {
    num_iterations: u32,
    learning_rate: f64,
    num_leaves: u32,
    max_depth: i32,
    min_data_in_leaf: u32,
    min_gain_to_split: f64,
    lambda_l1: f64,
    lambda_l2: f64,
    feature_fraction: f64,
    bagging_fraction: f64,
    bagging_freq: u32,
    early_stopping_round: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LightgbmDatasetSummary {
    row_count: usize,
    feature_count: usize,
    start_date: Option<String>,
    end_date: Option<String>,
    label_counts: BTreeMap<String, usize>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LightgbmValidationMetricsSummary {
    top_k: usize,
    positive_rate: f64,
    positives: usize,
    total_rows: usize,
    day_count: usize,
    precision_at_k: Option<f64>,
    hit_rate_at_k: Option<f64>,
    ndcg_at_k: Option<f64>,
    avg_max_multiple: Option<f64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LightgbmTrainingSummary {
    hyperparameters: LightgbmHyperparametersSummary,
    train_dataset: LightgbmDatasetSummary,
    validation_dataset: LightgbmDatasetSummary,
    validation_metrics: Option<LightgbmValidationMetricsSummary>,
}

pub async fn run(
    app: &AppContext,
    output_path: Option<PathBuf>,
    num_iterations: Option<u32>,
    learning_rate: Option<f64>,
    num_leaves: Option<u32>,
    max_depth: Option<i32>,
    min_data_in_leaf: Option<u32>,
    min_gain_to_split: Option<f64>,
    lambda_l1: Option<f64>,
    lambda_l2: Option<f64>,
    feature_fraction: Option<f64>,
    bagging_fraction: Option<f64>,
    bagging_freq: Option<u32>,
    early_stopping_round: Option<u32>,
) -> Result<()> {
    let db = app.database().await?;
    info!("Starting LightGBM training");
    let market_data = MarketData::load(&db, TickerScope::AllTickers).await?;
    let runtime_settings = EngineRuntimeSettings::from_settings_map(market_data.settings())?;

    let destination: PathBuf = output_path
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                Path::new(env!("CARGO_MANIFEST_DIR")).join(path)
            }
        })
        .unwrap_or_else(default_model_path);

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}", parent.display()))?;
    }

    let training_start_date =
        require_setting_date(market_data.settings(), "LIGHTGBM_TRAINING_START_DATE")?;
    let training_end_date =
        require_setting_date(market_data.settings(), "LIGHTGBM_TRAINING_END_DATE")?;
    let training_start = training_start_date
        .and_hms_opt(0, 0, 0)
        .expect("training start date at midnight should be valid")
        .and_utc();
    let training_end = training_end_date
        .and_hms_opt(0, 0, 0)
        .expect("training end date at midnight should be valid")
        .and_utc();
    let ticker_infos = db.get_tickers_with_candle_counts().await?;
    let mut training_tickers: HashSet<String> = HashSet::new();
    let mut validation_tickers: HashSet<String> = HashSet::new();
    for info in ticker_infos {
        if info.training {
            training_tickers.insert(info.symbol);
        } else {
            validation_tickers.insert(info.symbol);
        }
    }
    if training_tickers.is_empty() {
        return Err(anyhow!(
            "No training tickers with candle data were found in the database"
        ));
    }
    if validation_tickers.is_empty() {
        return Err(anyhow!(
            "No validation tickers with candle data were found in the database"
        ));
    }

    let feature_config = FeatureConfig::default();
    info!(
        "Building training rows with default feature config for {} to {} ({} training tickers)...",
        training_start.date_naive(),
        training_end.date_naive(),
        training_tickers.len()
    );
    let mut train_rows = build_training_rows(
        &market_data,
        feature_config,
        training_start,
        training_end,
        Some(&training_tickers),
        runtime_settings.minimum_dollar_volume_for_entry,
        runtime_settings.minimum_dollar_volume_lookback,
    )?;
    if train_rows.is_empty() {
        return Err(anyhow!(
            "No training rows could be generated from available market data"
        ));
    }
    let augmented = augment_training_rows_with_noise(&mut train_rows);
    if augmented > 0 {
        info!(
            "Augmented training rows with {} noisy duplicate{} ({} per row)",
            augmented,
            if augmented == 1 { "" } else { "s" },
            TRAIN_AUGMENT_DUPLICATES_PER_ROW
        );
    }
    info!("Finished building {} training rows", train_rows.len());

    info!(
        "Building validation rows for {} to {} ({} validation tickers)...",
        training_start.date_naive(),
        training_end.date_naive(),
        validation_tickers.len()
    );
    let mut validation_rows = build_training_rows(
        &market_data,
        feature_config,
        training_start,
        training_end,
        Some(&validation_tickers),
        runtime_settings.minimum_dollar_volume_for_entry,
        runtime_settings.minimum_dollar_volume_lookback,
    )?;
    let mut post_training_additions = 0usize;
    let mut post_training_end: Option<DateTime<Utc>> = None;
    {
        let candles_by_ticker = market_data.candles_by_ticker();
        for (ticker, candle_refs) in &candles_by_ticker {
            if !training_tickers.contains(ticker) && !validation_tickers.contains(ticker) {
                continue;
            }
            if let Some(latest_after_training) = candle_refs
                .iter()
                .filter(|candle| candle.date > training_end)
                .map(|candle| candle.date)
                .max()
            {
                post_training_end = Some(match post_training_end {
                    Some(existing) if existing > latest_after_training => existing,
                    _ => latest_after_training,
                });
            }
        }
    }
    if let Some(post_training_end_date) = post_training_end {
        let post_training_start = training_end + Duration::seconds(1);
        let mut post_training_tickers = training_tickers.clone();
        post_training_tickers.extend(validation_tickers.iter().cloned());
        info!(
            "Building post-training validation rows for {} to {} ({} tickers)...",
            post_training_start.date_naive(),
            post_training_end_date.date_naive(),
            post_training_tickers.len()
        );
        let post_training_rows = build_training_rows(
            &market_data,
            feature_config,
            post_training_start,
            post_training_end_date,
            Some(&post_training_tickers),
            runtime_settings.minimum_dollar_volume_for_entry,
            runtime_settings.minimum_dollar_volume_lookback,
        )?;
        post_training_additions = post_training_rows.len();
        if post_training_additions > 0 {
            validation_rows.extend(post_training_rows);
            info!(
                "Added {} post-training validation rows",
                post_training_additions
            );
        } else {
            info!("No post-training validation rows were generated");
        }
    } else {
        info!(
            "No post-training validation candles available beyond {}",
            training_end.date_naive()
        );
    }
    if validation_rows.is_empty() {
        return Err(anyhow!(
            "No validation rows could be generated from available market data"
        ));
    }
    if post_training_additions > 0 {
        info!(
            "Finished building {} validation rows (including {} rows from after {})",
            validation_rows.len(),
            post_training_additions,
            training_end.date_naive()
        );
    } else {
        info!(
            "Finished building {} validation rows",
            validation_rows.len()
        );
    }
    sort_rows_by_date(&mut train_rows);
    sort_rows_by_date(&mut validation_rows);
    print_group_size_stats(&train_rows, "training");
    print_group_size_stats(&validation_rows, "validation");
    let train_hist = class_histogram(&train_rows);
    let valid_hist = class_histogram(&validation_rows);
    println!(
        "Training rows={} {}; validation rows={} {}; horizon={} bars",
        train_rows.len(),
        format_histogram(&train_hist),
        validation_rows.len(),
        format_histogram(&valid_hist),
        EXTREME_HORIZON_BARS,
    );

    let train_dataset_summary = summarize_dataset(&train_rows);
    let validation_dataset_summary = summarize_dataset(&validation_rows);

    let exe_path = resolve_lightgbm_executable()?;
    info!("Using LightGBM executable at {}", exe_path.display());

    let train_dataset_path =
        std::env::temp_dir().join(format!("lightgbm_train_{}.svm", Uuid::new_v4()));
    let validation_dataset_path =
        std::env::temp_dir().join(format!("lightgbm_valid_{}.svm", Uuid::new_v4()));
    info!(
        "Writing training dataset to {} ({} rows) and validation dataset to {} ({} rows)",
        train_dataset_path.display(),
        train_rows.len(),
        validation_dataset_path.display(),
        validation_rows.len()
    );
    write_libsvm_dataset(&train_rows, &train_dataset_path)?;
    let train_weight_path = write_dataset_weights(&train_rows, &train_dataset_path)?;
    let train_query_path = write_dataset_queries(&train_rows, &train_dataset_path)?;
    write_libsvm_dataset(&validation_rows, &validation_dataset_path)?;
    let validation_weight_path = write_dataset_weights(&validation_rows, &validation_dataset_path)?;
    let validation_query_path = write_dataset_queries(&validation_rows, &validation_dataset_path)?;
    let train_profile_path = write_dataset_profile_html(&train_rows, "training")?;
    let validation_profile_path = write_dataset_profile_html(&validation_rows, "validation")?;
    info!(
        "Dataset profiles available at {} (training) and {} (validation)",
        train_profile_path.display(),
        validation_profile_path.display()
    );
    println!(
        "Dataset profile reports: train={} validation={}",
        train_profile_path.display(),
        validation_profile_path.display()
    );

    let num_iterations = num_iterations.unwrap_or(800);
    let learning_rate = learning_rate.unwrap_or(0.05);
    let num_leaves = num_leaves.unwrap_or(15);
    let max_depth = max_depth.unwrap_or(5);
    let min_data_in_leaf = min_data_in_leaf.unwrap_or(100);
    let min_gain_to_split = min_gain_to_split.unwrap_or(0.01);
    let lambda_l1 = lambda_l1.unwrap_or(0.0);
    let lambda_l2 = lambda_l2.unwrap_or(5.0);
    let feature_fraction = feature_fraction.unwrap_or(0.6);
    let bagging_fraction = bagging_fraction.unwrap_or(0.6);
    let bagging_freq = bagging_freq.unwrap_or(5);
    let early_stopping_round = early_stopping_round.unwrap_or(EARLY_STOPPING_ROUNDS);

    let eval_at = format!("eval_at={}", TOP_K);
    let truncation_level = format!("lambdarank_truncation_level={}", TOP_K);
    let label_gain = label_gain_param();
    let num_iterations_param = format!("num_iterations={}", num_iterations);
    let learning_rate_param = format!("learning_rate={}", learning_rate);
    let num_leaves_param = format!("num_leaves={}", num_leaves);
    let max_depth_param = format!("max_depth={}", max_depth);
    let min_data_in_leaf_param = format!("min_data_in_leaf={}", min_data_in_leaf);
    let min_gain_to_split_param = format!("min_gain_to_split={}", min_gain_to_split);
    let lambda_l1_param = format!("lambda_l1={}", lambda_l1);
    let lambda_l2_param = format!("lambda_l2={}", lambda_l2);
    let feature_fraction_param = format!("feature_fraction={}", feature_fraction);
    let bagging_fraction_param = format!("bagging_fraction={}", bagging_fraction);
    let bagging_freq_param = format!("bagging_freq={}", bagging_freq);
    let data_param = format!("data={}", train_dataset_path.to_string_lossy());
    let valid_data_param = format!("valid_data={}", validation_dataset_path.to_string_lossy());
    let output_model_param = format!("output_model={}", destination.to_string_lossy());
    let early_stopping_round_display = if early_stopping_round == 0 {
        String::from("disabled")
    } else {
        early_stopping_round.to_string()
    };

    info!(
        "Launching LightGBM: num_iterations={}, learning_rate={}, num_leaves={}, max_depth={}, min_data_in_leaf={}, min_gain_to_split={}, lambda_l1={}, lambda_l2={}, feature_fraction={}, bagging_fraction={}, bagging_freq={}, early_stopping_round={}",
        num_iterations,
        learning_rate,
        num_leaves,
        max_depth,
        min_data_in_leaf,
        min_gain_to_split,
        lambda_l1,
        lambda_l2,
        feature_fraction,
        bagging_fraction,
        bagging_freq,
        early_stopping_round_display,
    );

    let mut args = vec![
        String::from("task=train"),
        String::from("objective=lambdarank"),
        String::from("metric=ndcg"),
        eval_at,
        truncation_level,
        label_gain,
        num_iterations_param,
        learning_rate_param,
        num_leaves_param,
        max_depth_param,
        min_data_in_leaf_param,
        min_gain_to_split_param,
        lambda_l1_param,
        lambda_l2_param,
        feature_fraction_param,
        bagging_fraction_param,
        bagging_freq_param,
        data_param,
        valid_data_param,
    ];
    if early_stopping_round > 0 {
        args.push(format!("early_stopping_round={}", early_stopping_round));
    }
    args.push(String::from("first_metric_only=false"));
    args.push(output_model_param);
    args.push(String::from("verbosity=2"));

    let status = Command::new(&exe_path)
        .args(&args)
        .status()
        .context("Failed to spawn lightgbm.exe for training")?;

    let validation_metrics = if status.success() {
        evaluate_validation_set(
            &exe_path,
            &destination,
            &validation_dataset_path,
            &validation_rows,
        )
    } else {
        None
    };

    let _ = fs::remove_file(&train_dataset_path);
    let _ = fs::remove_file(&validation_dataset_path);
    let _ = fs::remove_file(&train_weight_path);
    let _ = fs::remove_file(&validation_weight_path);
    let _ = fs::remove_file(&train_query_path);
    let _ = fs::remove_file(&validation_query_path);

    if !status.success() {
        return Err(anyhow!("lightgbm.exe training failed with status {status}"));
    }

    info!("LightGBM training complete");
    println!("Saved LightGBM model to {}", destination.display());

    if let Err(err) = load_model_from_path(&destination) {
        warn!("Model was trained and saved, but failed to register for inference: {err}");
    }

    let training_summary = LightgbmTrainingSummary {
        hyperparameters: LightgbmHyperparametersSummary {
            num_iterations,
            learning_rate,
            num_leaves,
            max_depth,
            min_data_in_leaf,
            min_gain_to_split,
            lambda_l1,
            lambda_l2,
            feature_fraction,
            bagging_fraction,
            bagging_freq,
            early_stopping_round,
        },
        train_dataset: train_dataset_summary,
        validation_dataset: validation_dataset_summary,
        validation_metrics,
    };

    match serde_json::to_string(&training_summary) {
        Ok(payload) => println!("STRATCRAFT_LIGHTGBM_TRAIN_SUMMARY={payload}"),
        Err(err) => warn!("Failed to serialize LightGBM training summary: {err}"),
    }

    Ok(())
}

fn write_libsvm_dataset(rows: &[TrainingRow], path: &Path) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);

    for row in rows {
        let mut line = format!("{}", row.rank_label);
        for (idx, value) in row.features.iter().enumerate() {
            line.push(' ');
            line.push_str(&format!("{}:{:.10}", idx, value));
        }
        line.push('\n');
        writer.write_all(line.as_bytes())?;
    }

    writer.flush()?;
    Ok(())
}

fn write_dataset_weights(rows: &[TrainingRow], dataset_path: &Path) -> Result<PathBuf> {
    let weight_path = dataset_weight_path(dataset_path);
    let mut writer = BufWriter::new(File::create(&weight_path)?);
    for row in rows {
        writer.write_all(format!("{:.6}\n", row.weight).as_bytes())?;
    }
    writer.flush()?;
    Ok(weight_path)
}

fn dataset_weight_path(dataset_path: &Path) -> PathBuf {
    let mut os: OsString = dataset_path.as_os_str().to_os_string();
    os.push(".weight");
    PathBuf::from(os)
}

fn resolve_lightgbm_executable() -> Result<PathBuf> {
    if cfg!(windows) {
        let exe_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor/lightgbm.exe");
        if exe_path.exists() {
            return Ok(exe_path);
        }
        return Err(anyhow!(
            "lightgbm.exe not found at {}; cannot train model",
            exe_path.display()
        ));
    }

    let vendor_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor/lightgbm");
    if vendor_path.exists() {
        return Ok(vendor_path);
    }

    if let Some(path) = find_in_path("lightgbm") {
        return Ok(path);
    }

    Err(anyhow!(
        "lightgbm executable not found in vendor/ or PATH; install LightGBM CLI to train models"
    ))
}

fn find_in_path(binary: &str) -> Option<PathBuf> {
    let path_value = std::env::var_os("PATH")?;
    for entry in std::env::split_paths(&path_value) {
        let candidate = entry.join(binary);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn write_dataset_queries(rows: &[TrainingRow], dataset_path: &Path) -> Result<PathBuf> {
    let query_path = dataset_query_path(dataset_path);
    let mut writer = BufWriter::new(File::create(&query_path)?);

    if rows.is_empty() {
        writer.flush()?;
        return Ok(query_path);
    }

    let mut current_date = rows[0].date.date_naive();
    let mut count = 0usize;
    for row in rows {
        let row_date = row.date.date_naive();
        if row_date != current_date {
            writer.write_all(format!("{count}\n").as_bytes())?;
            current_date = row_date;
            count = 1;
        } else {
            count += 1;
        }
    }
    if count > 0 {
        writer.write_all(format!("{count}\n").as_bytes())?;
    }

    writer.flush()?;
    Ok(query_path)
}

fn dataset_query_path(dataset_path: &Path) -> PathBuf {
    let mut os: OsString = dataset_path.as_os_str().to_os_string();
    os.push(".query");
    PathBuf::from(os)
}

fn build_training_rows(
    market_data: &MarketData,
    features_config: FeatureConfig,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    allowed_tickers: Option<&HashSet<String>>,
    min_dollar_volume_for_entry: f64,
    min_dollar_volume_lookback: usize,
) -> Result<Vec<TrainingRow>> {
    if start_date > end_date {
        return Err(anyhow!(
            "Training start date {} occurs after end date {}",
            start_date,
            end_date
        ));
    }
    info!("Preparing candle map for training feature generation");
    let candles_by_ticker = market_data.candles_by_ticker();
    let mut filtered_candles_by_ticker: HashMap<String, Vec<&Candle>> =
        HashMap::with_capacity(candles_by_ticker.len());
    for (ticker, candle_refs) in candles_by_ticker {
        if let Some(allowed) = allowed_tickers {
            if !allowed.contains(&ticker) {
                continue;
            }
        }
        let filtered_refs: Vec<&Candle> = candle_refs
            .iter()
            .copied()
            .filter(|candle| candle.date >= start_date && candle.date <= end_date)
            .collect();
        if filtered_refs.len() >= 2 {
            filtered_candles_by_ticker.insert(ticker, filtered_refs);
        }
    }

    if filtered_candles_by_ticker.is_empty() {
        return Err(anyhow!(
            "No candles remain after applying the {} to {} window for the selected tickers",
            start_date,
            end_date
        ));
    }

    let cross_context = CrossSectionalContext::new(&filtered_candles_by_ticker).map(Arc::new);
    if cross_context.is_some() {
        info!("Cross-sectional snapshots will be generated lazily during feature extraction");
    }
    info!(
        "Generating feature rows in parallel for {} tickers",
        filtered_candles_by_ticker.len()
    );
    let rows: Vec<TrainingRow> = filtered_candles_by_ticker
        .par_iter()
        .map(|(ticker, candle_refs)| {
            if candle_refs.len() < 2 {
                return Vec::new();
            }

            let cross_context = cross_context.clone();
            let precomputed = match precompute_inputs_for_ticker(candle_refs, features_config) {
                Some(value) => value,
                None => return Vec::new(),
            };
            let max_idx = candle_refs.len().saturating_sub(MAX_LOOKAHEAD_BARS);
            (0..max_idx)
                .into_par_iter()
                .filter_map(|idx| {
                    if !has_minimum_dollar_volume(
                        candle_refs,
                        idx,
                        min_dollar_volume_lookback,
                        min_dollar_volume_for_entry,
                    ) {
                        return None;
                    }
                    let snapshot = compute_features_from_precomputed(
                        ticker,
                        candle_refs,
                        idx,
                        features_config,
                        &precomputed,
                        cross_context.clone(),
                    )?;
                    if snapshot.values.iter().any(|value| !value.is_finite()) {
                        return None;
                    }
                    let (label, max_multiple) = compute_extreme_label(candle_refs, idx)?;
                    Some(TrainingRow {
                        date: candle_refs[idx].date,
                        features: snapshot.values.clone(),
                        label,
                        rank_label: compute_rank_label(max_multiple),
                        max_multiple,
                        weight: 1.0,
                    })
                })
                .collect::<Vec<TrainingRow>>()
        })
        .reduce(Vec::new, |mut acc, mut ticker_rows| {
            acc.append(&mut ticker_rows);
            acc
        });

    let mut rows = rows;
    apply_extreme_sampling_and_weights(&mut rows);
    Ok(rows)
}

fn apply_extreme_sampling_and_weights(rows: &mut Vec<TrainingRow>) {
    const ZERO_GAIN_KEEP_PROB: f64 = 0.3;
    const POS_WEIGHT_BONUS: f64 = 2.0;

    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    let mut rows_by_day: BTreeMap<NaiveDate, Vec<TrainingRow>> = BTreeMap::new();
    for row in rows.drain(..) {
        rows_by_day
            .entry(row.date.date_naive())
            .or_default()
            .push(row);
    }

    let mut kept = Vec::with_capacity(rows_by_day.values().map(|rows| rows.len()).sum());
    for (_day, mut day_rows) in rows_by_day {
        let mut zero_rows = Vec::new();
        for mut row in day_rows.drain(..) {
            if row.rank_label > 0 {
                let capped = row.max_multiple.min(RANK_LABEL_CAP_MULTIPLE).max(1.0);
                let quality = (capped - 1.0) / (RANK_LABEL_CAP_MULTIPLE - 1.0);
                row.weight = 1.0 + (quality * POS_WEIGHT_BONUS);
                kept.push(row);
            } else {
                zero_rows.push(row);
            }
        }

        if !zero_rows.is_empty() {
            zero_rows.shuffle(&mut rng);
            let mut keep_count = ((zero_rows.len() as f64) * ZERO_GAIN_KEEP_PROB).round() as usize;
            if keep_count == 0 {
                keep_count = 1;
            }
            keep_count = keep_count.min(zero_rows.len());
            for mut row in zero_rows.into_iter().take(keep_count) {
                row.weight = 1.0;
                kept.push(row);
            }
        }
    }
    *rows = kept;
}

fn augment_training_rows_with_noise(rows: &mut Vec<TrainingRow>) -> usize {
    if TRAIN_AUGMENT_DUPLICATES_PER_ROW == 0 || rows.is_empty() {
        return 0;
    }

    let original_len = rows.len();
    rows.reserve(original_len.saturating_mul(TRAIN_AUGMENT_DUPLICATES_PER_ROW));
    let mut rng = rand::rngs::StdRng::seed_from_u64(TRAIN_AUGMENT_SEED);
    let mut added = 0usize;

    for idx in 0..original_len {
        let base = rows[idx].clone();
        for _ in 0..TRAIN_AUGMENT_DUPLICATES_PER_ROW {
            let mut row = base.clone();
            row.features = row
                .features
                .iter()
                .map(|value| jitter_feature(*value, &mut rng))
                .collect();
            row.weight *= TRAIN_AUGMENT_WEIGHT_SCALE;
            rows.push(row);
            added += 1;
        }
    }

    added
}

fn jitter_feature(value: f64, rng: &mut rand::rngs::StdRng) -> f64 {
    if !value.is_finite() {
        return value;
    }

    let mut noisy = if value.abs() < TRAIN_AUGMENT_NOISE_ABS {
        value + rng.gen_range(-TRAIN_AUGMENT_NOISE_ABS..=TRAIN_AUGMENT_NOISE_ABS)
    } else {
        let jitter = rng.gen_range(-TRAIN_AUGMENT_NOISE_RELATIVE..=TRAIN_AUGMENT_NOISE_RELATIVE);
        value * (1.0 + jitter)
    };

    if (0.0..=1.0).contains(&value) {
        noisy = noisy.clamp(0.0, 1.0);
    }

    noisy
}

fn class_histogram(rows: &[TrainingRow]) -> [usize; EXTREME_LABEL_COUNT] {
    let mut histogram = [0usize; EXTREME_LABEL_COUNT];
    for row in rows {
        let idx = row.label as usize;
        if let Some(slot) = histogram.get_mut(idx) {
            *slot += 1;
        }
    }
    histogram
}

fn format_histogram(histogram: &[usize; EXTREME_LABEL_COUNT]) -> String {
    let parts: Vec<String> = histogram
        .iter()
        .enumerate()
        .map(|(idx, count)| format!("{}={}", EXTREME_LABEL_NAMES[idx], count))
        .collect();
    format!("label_counts({})", parts.join(", "))
}

fn evaluate_validation_set(
    exe_path: &Path,
    model_path: &Path,
    validation_dataset_path: &Path,
    validation_rows: &[TrainingRow],
) -> Option<LightgbmValidationMetricsSummary> {
    if validation_rows.is_empty() {
        return None;
    }

    let predictions_path =
        std::env::temp_dir().join(format!("lightgbm_valid_pred_{}.txt", Uuid::new_v4()));
    let status = Command::new(exe_path)
        .args([
            "task=predict",
            &format!("data={}", validation_dataset_path.to_string_lossy()),
            &format!("input_model={}", model_path.to_string_lossy()),
            &format!("output_result={}", predictions_path.to_string_lossy()),
            "verbosity=1",
        ])
        .status();

    let status = match status {
        Ok(status) => status,
        Err(err) => {
            warn!(
                "Validation evaluation skipped: failed to spawn lightgbm.exe for prediction ({err})"
            );
            return None;
        }
    };

    if !status.success() {
        warn!(
            "Validation evaluation skipped: lightgbm.exe prediction failed with status {}",
            status
        );
        let _ = fs::remove_file(&predictions_path);
        return None;
    }

    let raw_predictions = match fs::read_to_string(&predictions_path) {
        Ok(contents) => contents,
        Err(err) => {
            warn!("Validation evaluation skipped: could not read predictions ({err})");
            let _ = fs::remove_file(&predictions_path);
            return None;
        }
    };
    let _ = fs::remove_file(&predictions_path);

    let mut scores: Vec<f64> = Vec::new();
    for line in raw_predictions.lines() {
        if let Some(token) = line.split_whitespace().next() {
            if let Ok(value) = token.parse::<f64>() {
                scores.push(value);
            }
        }
    }

    if scores.len() != validation_rows.len() {
        warn!(
            "Validation evaluation skipped: expected {} prediction rows, got {}",
            validation_rows.len(),
            scores.len()
        );
        return None;
    }

    let mut rows_by_day: HashMap<NaiveDate, Vec<ScoredRow>> = HashMap::new();
    let mut positives = 0usize;
    for (row, score) in validation_rows.iter().zip(scores.iter()) {
        if row.label == EXTREME_LABEL_POSITIVE {
            positives += 1;
        }
        let safe_score = if score.is_finite() { *score } else { 0.0 };
        rows_by_day
            .entry(row.date.date_naive())
            .or_default()
            .push(ScoredRow {
                score: safe_score,
                label: row.label,
                rank_label: row.rank_label,
                max_multiple: row.max_multiple,
            });
    }

    let total_rows = validation_rows.len() as f64;
    let positive_rate = positives as f64 / total_rows.max(1.0);
    println!(
        "Validation positive rate: {:.4}% ({}/{})",
        positive_rate * 100.0,
        positives,
        validation_rows.len()
    );

    let mut day_count = 0usize;
    let mut precision_sum = 0.0;
    let mut hit_days = 0usize;
    let mut avg_multiple_sum = 0.0;
    let mut avg_multiple_days = 0usize;
    let mut ndcg_sum = 0.0;
    let mut ndcg_days = 0usize;

    for rows in rows_by_day.values_mut() {
        rows.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        let k = TOP_K.min(rows.len());
        if k == 0 {
            continue;
        }
        day_count += 1;
        let topk = &rows[..k];
        let positives_in_topk = topk
            .iter()
            .filter(|row| row.label == EXTREME_LABEL_POSITIVE)
            .count();
        precision_sum += positives_in_topk as f64 / k as f64;
        if positives_in_topk > 0 {
            hit_days += 1;
        }
        let mut multiple_sum = 0.0;
        let mut multiple_count = 0usize;
        for row in topk {
            if row.max_multiple.is_finite() {
                multiple_sum += row.max_multiple;
                multiple_count += 1;
            }
        }
        if multiple_count > 0 {
            avg_multiple_sum += multiple_sum / multiple_count as f64;
            avg_multiple_days += 1;
        }

        let ndcg = compute_ndcg_at_k(rows, k);
        if ndcg.is_finite() {
            ndcg_sum += ndcg;
            ndcg_days += 1;
        }
    }

    if day_count > 0 {
        let precision_at_k = precision_sum / day_count as f64;
        let hit_rate = hit_days as f64 / day_count as f64;
        println!(
            "Validation precision@{}: {:.2}% ({} days)",
            TOP_K,
            precision_at_k * 100.0,
            day_count
        );
        println!(
            "Validation hit-rate@{}: {:.2}% ({} days)",
            TOP_K,
            hit_rate * 100.0,
            day_count
        );
        if ndcg_days > 0 {
            println!(
                "Validation ndcg@{}: {:.4} ({} days)",
                TOP_K,
                ndcg_sum / ndcg_days as f64,
                ndcg_days
            );
        } else {
            println!("Validation ndcg@{}: n/a", TOP_K);
        }
    } else {
        println!("Validation precision@{}: n/a", TOP_K);
        println!("Validation hit-rate@{}: n/a", TOP_K);
        println!("Validation ndcg@{}: n/a", TOP_K);
    }

    if avg_multiple_days > 0 {
        let avg_multiple = avg_multiple_sum / avg_multiple_days as f64;
        println!(
            "Validation avg max multiple (top {}): {:.2}x",
            TOP_K, avg_multiple
        );
    } else {
        println!("Validation avg max multiple (top {}): n/a", TOP_K);
    }

    let precision_at_k = if day_count > 0 {
        Some(precision_sum / day_count as f64)
    } else {
        None
    };
    let hit_rate_at_k = if day_count > 0 {
        Some(hit_days as f64 / day_count as f64)
    } else {
        None
    };
    let ndcg_at_k = if ndcg_days > 0 {
        Some(ndcg_sum / ndcg_days as f64)
    } else {
        None
    };
    let avg_max_multiple = if avg_multiple_days > 0 {
        Some(avg_multiple_sum / avg_multiple_days as f64)
    } else {
        None
    };

    Some(LightgbmValidationMetricsSummary {
        top_k: TOP_K,
        positive_rate,
        positives,
        total_rows: validation_rows.len(),
        day_count,
        precision_at_k,
        hit_rate_at_k,
        ndcg_at_k,
        avg_max_multiple,
    })
}

fn write_dataset_profile_html(rows: &[TrainingRow], dataset_name: &str) -> Result<PathBuf> {
    let stats = gather_dataset_stats(rows);
    let mut html = String::new();
    html.push_str("<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    let _ = write!(&mut html, "<title>{} dataset profile</title>", dataset_name);
    html.push_str("<style>body{font-family:Arial,Helvetica,sans-serif;margin:24px;color:#111;}h1{margin-bottom:8px;}table{border-collapse:collapse;margin-top:12px;width:100%;}th,td{padding:6px 8px;border:1px solid #d0d0d0;font-size:13px;}tr:nth-child(even){background:#f7f7f7;}th{background:#efefef;text-align:left;}td.num{text-align:right;} .summary-grid{display:flex;gap:16px;flex-wrap:wrap;margin-top:8px;} .summary-card{border:1px solid #d0d0d0;border-radius:6px;padding:12px 16px;min-width:180px;} .summary-label{font-size:12px;color:#555;text-transform:uppercase;letter-spacing:0.05em;} .summary-value{font-size:18px;font-weight:600;color:#111;}</style></head><body>");
    let _ = write!(&mut html, "<h1>Dataset profile: {}</h1>", dataset_name);
    html.push_str("<div class=\"summary-grid\">");
    let _ = write!(
        &mut html,
        "<div class=\"summary-card\"><div class=\"summary-label\">Rows</div><div class=\"summary-value\">{}</div></div>",
        stats.row_count
    );
    let _ = write!(
        &mut html,
        "<div class=\"summary-card\"><div class=\"summary-label\">Max features per row</div><div class=\"summary-value\">{}</div></div>",
        stats.feature_count
    );
    let start = stats
        .start_date
        .clone()
        .unwrap_or_else(|| "n/a".to_string());
    let end = stats.end_date.clone().unwrap_or_else(|| "n/a".to_string());
    let _ = write!(
        &mut html,
        "<div class=\"summary-card\"><div class=\"summary-label\">Date range</div><div class=\"summary-value\">{} - {}</div></div>",
        start, end
    );
    html.push_str("</div>");
    html.push_str("<h2>Class distribution</h2><table><thead><tr><th>Bucket</th><th>Count</th><th>Percent</th></tr></thead><tbody>");
    for (idx, count) in stats.histogram.iter().enumerate() {
        let percent = if stats.row_count > 0 {
            (*count as f64 / stats.row_count as f64) * 100.0
        } else {
            0.0
        };
        let _ = write!(
            &mut html,
            "<tr><td>{}</td><td class=\"num\">{}</td><td class=\"num\">{:.2}%</td></tr>",
            EXTREME_LABEL_NAMES[idx], count, percent
        );
    }
    html.push_str("</tbody></table>");
    html.push_str("<h2>Feature summary</h2>");
    if stats.feature_summaries.is_empty() {
        html.push_str("<p>No features detected in this dataset.</p>");
    } else {
        html.push_str("<table><thead><tr><th>Feature #</th><th>Finite coverage</th><th>Any coverage</th><th>Mean</th><th>Std dev</th><th>Min</th><th>Max</th><th>Non-finite</th><th>Missing</th></tr></thead><tbody>");
        for summary in &stats.feature_summaries {
            let finite_pct = format_percentage(summary.observed_finite, stats.row_count);
            let any_pct = format_percentage(summary.observed_total, stats.row_count);
            let _ = write!(
                &mut html,
                "<tr><td>{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td></tr>",
                summary.index,
                finite_pct,
                any_pct,
                format_optional(summary.mean),
                format_optional(summary.std_dev),
                format_optional(summary.min),
                format_optional(summary.max),
                summary.non_finite,
                summary.missing
            );
        }
        html.push_str("</tbody></table>");
    }
    html.push_str("</body></html>");
    let safe_name: String = dataset_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let output_path = std::env::temp_dir().join(format!(
        "lightgbm_{}_profile_{}.html",
        safe_name,
        Uuid::new_v4()
    ));
    fs::write(&output_path, html)?;
    Ok(output_path)
}

fn summarize_dataset(rows: &[TrainingRow]) -> LightgbmDatasetSummary {
    let row_count = rows.len();
    let feature_count = rows.iter().map(|row| row.features.len()).max().unwrap_or(0);
    let (start_date, end_date) = match rows.iter().map(|row| row.date.date_naive()).min() {
        Some(start) => {
            let end = rows
                .iter()
                .map(|row| row.date.date_naive())
                .max()
                .unwrap_or(start);
            (Some(start.to_string()), Some(end.to_string()))
        }
        None => (None, None),
    };

    let histogram = class_histogram(rows);
    let mut label_counts = BTreeMap::new();
    for (idx, count) in histogram.iter().enumerate() {
        label_counts.insert(EXTREME_LABEL_NAMES[idx].to_string(), *count);
    }

    LightgbmDatasetSummary {
        row_count,
        feature_count,
        start_date,
        end_date,
        label_counts,
    }
}

fn gather_dataset_stats(rows: &[TrainingRow]) -> DatasetStats {
    let row_count = rows.len();
    let feature_count = rows.iter().map(|row| row.features.len()).max().unwrap_or(0);
    let histogram = class_histogram(rows);
    let mut min_date: Option<&DateTime<Utc>> = None;
    let mut max_date: Option<&DateTime<Utc>> = None;
    for row in rows {
        let date = &row.date;
        if min_date.map(|current| date < current).unwrap_or(true) {
            min_date = Some(date);
        }
        if max_date.map(|current| date > current).unwrap_or(true) {
            max_date = Some(date);
        }
    }
    let feature_summaries = if feature_count > 0 {
        compute_feature_summaries(rows, feature_count)
    } else {
        Vec::new()
    };
    DatasetStats {
        row_count,
        feature_count,
        start_date: min_date.map(|dt| dt.to_rfc3339()),
        end_date: max_date.map(|dt| dt.to_rfc3339()),
        histogram,
        feature_summaries,
    }
}

fn compute_feature_summaries(
    rows: &[TrainingRow],
    feature_count: usize,
) -> Vec<DatasetFeatureSummary> {
    if feature_count == 0 {
        return Vec::new();
    }
    let mut mins = vec![f64::INFINITY; feature_count];
    let mut maxs = vec![f64::NEG_INFINITY; feature_count];
    let mut sums = vec![0.0; feature_count];
    let mut sum_squares = vec![0.0; feature_count];
    let mut finite_counts = vec![0usize; feature_count];
    let mut non_finite_counts = vec![0usize; feature_count];
    let mut present_counts = vec![0usize; feature_count];

    for row in rows {
        for (idx, value) in row.features.iter().enumerate().take(feature_count) {
            present_counts[idx] += 1;
            if value.is_finite() {
                finite_counts[idx] += 1;
                if *value < mins[idx] {
                    mins[idx] = *value;
                }
                if *value > maxs[idx] {
                    maxs[idx] = *value;
                }
                sums[idx] += *value;
                sum_squares[idx] += value * value;
            } else {
                non_finite_counts[idx] += 1;
            }
        }
    }

    (0..feature_count)
        .map(|idx| {
            let observed_finite = finite_counts[idx];
            let observed_total = present_counts[idx];
            let mean = if observed_finite > 0 {
                Some(sums[idx] / observed_finite as f64)
            } else {
                None
            };
            let variance = if observed_finite > 0 {
                let count = observed_finite as f64;
                let avg = sums[idx] / count;
                let mean_square = sum_squares[idx] / count;
                Some((mean_square - avg.powi(2)).max(0.0))
            } else {
                None
            };
            let std_dev = variance.map(|var| var.sqrt());
            DatasetFeatureSummary {
                index: idx,
                observed_finite,
                observed_total,
                non_finite: non_finite_counts[idx],
                missing: rows.len().saturating_sub(observed_total),
                min: if observed_finite > 0 {
                    Some(mins[idx])
                } else {
                    None
                },
                max: if observed_finite > 0 {
                    Some(maxs[idx])
                } else {
                    None
                },
                mean,
                std_dev,
            }
        })
        .collect()
}

fn format_percentage(count: usize, total: usize) -> String {
    if total == 0 {
        return "0 (0.00%)".to_string();
    }
    let percent = (count as f64 / total as f64) * 100.0;
    format!("{count} ({percent:.2}%)")
}

fn format_optional(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.6}", v))
        .unwrap_or_else(|| "n/a".to_string())
}

fn compute_max_multiple(candle_refs: &[&Candle], idx: usize, horizon: usize) -> Option<f64> {
    if idx + 1 >= candle_refs.len() {
        return None;
    }
    let start = candle_refs[idx].close;
    if !start.is_finite() {
        return None;
    }
    let end = (idx + horizon).min(candle_refs.len() - 1);

    let mut max_price = f64::NEG_INFINITY;
    for j in (idx + 1)..=end {
        let price = if EXTREME_USE_HIGH {
            candle_refs[j].high
        } else {
            candle_refs[j].close
        };
        if price.is_finite() && price > max_price {
            max_price = price;
        }
    }
    if !max_price.is_finite() {
        return None;
    }

    Some(max_price / start.max(f64::EPSILON))
}

fn compute_extreme_label(candle_refs: &[&Candle], idx: usize) -> Option<(u8, f64)> {
    let multiple = compute_max_multiple(candle_refs, idx, EXTREME_HORIZON_BARS)?;
    let label = if multiple >= EXTREME_TARGET_MULTIPLE {
        EXTREME_LABEL_POSITIVE
    } else {
        EXTREME_LABEL_NEGATIVE
    };
    Some((label, multiple))
}

fn compute_rank_label(max_multiple: f64) -> u8 {
    if !max_multiple.is_finite() {
        return 0;
    }
    for (idx, threshold) in RANK_LABEL_BINS.iter().enumerate() {
        if max_multiple < *threshold {
            return idx as u8;
        }
    }
    RANK_LABEL_LEVELS.saturating_sub(1)
}

fn compute_ndcg_at_k(rows: &[ScoredRow], k: usize) -> f64 {
    if k == 0 || rows.is_empty() {
        return f64::NAN;
    }
    let limit = k.min(rows.len());
    let mut dcg = 0.0;
    for (idx, row) in rows.iter().take(limit).enumerate() {
        let rel = label_gain(row.rank_label) as f64;
        if rel > 0.0 {
            let denom = ((idx + 2) as f64).log2();
            dcg += rel / denom;
        }
    }

    let mut ideal = rows.to_vec();
    ideal.sort_by(|a, b| {
        b.rank_label
            .partial_cmp(&a.rank_label)
            .unwrap_or(Ordering::Equal)
    });
    let mut idcg = 0.0;
    for (idx, row) in ideal.iter().take(limit).enumerate() {
        let rel = label_gain(row.rank_label) as f64;
        if rel > 0.0 {
            let denom = ((idx + 2) as f64).log2();
            idcg += rel / denom;
        }
    }

    if idcg > 0.0 {
        dcg / idcg
    } else {
        f64::NAN
    }
}

fn label_gain(label: u8) -> u32 {
    let idx = label as usize;
    LABEL_GAINS.get(idx).copied().unwrap_or(0)
}

fn label_gain_param() -> String {
    let mut output = String::from("label_gain=");
    for (idx, gain) in LABEL_GAINS.iter().enumerate() {
        if idx > 0 {
            output.push(',');
        }
        let _ = write!(&mut output, "{}", gain);
    }
    output
}

fn sort_rows_by_date(rows: &mut Vec<TrainingRow>) {
    rows.sort_by(|a, b| {
        let date_cmp = a.date.cmp(&b.date);
        if date_cmp != Ordering::Equal {
            return date_cmp;
        }
        let label_cmp = a.label.cmp(&b.label);
        if label_cmp != Ordering::Equal {
            return label_cmp;
        }
        a.max_multiple
            .partial_cmp(&b.max_multiple)
            .unwrap_or(Ordering::Equal)
    });
}

fn print_group_size_stats(rows: &[TrainingRow], label: &str) {
    if rows.is_empty() {
        println!("Group sizes ({}): n/a", label);
        return;
    }

    let mut counts = Vec::new();
    let mut current_date = rows[0].date.date_naive();
    let mut count = 0usize;
    for row in rows {
        let row_date = row.date.date_naive();
        if row_date != current_date {
            counts.push(count);
            current_date = row_date;
            count = 1;
        } else {
            count += 1;
        }
    }
    if count > 0 {
        counts.push(count);
    }

    counts.sort_unstable();
    let min = *counts.first().unwrap_or(&0);
    let max = *counts.last().unwrap_or(&0);
    let p50 = percentile_value(&counts, 0.50);
    let p95 = percentile_value(&counts, 0.95);

    println!(
        "Group sizes ({}): min={} p50={} p95={} max={} (days={})",
        label,
        min,
        p50,
        p95,
        max,
        counts.len()
    );
}

fn percentile_value(sorted: &[usize], percentile: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let clamped = percentile.clamp(0.0, 1.0);
    let idx = ((sorted.len() - 1) as f64 * clamped).round() as usize;
    sorted[idx]
}
