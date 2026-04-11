use crate::commands::market_data_snapshot::ensure_market_data_file;
use crate::config::{require_setting_date, resolve_backtest_initial_capital};
use crate::context::{AppContext, MarketDataFilters};
use crate::data_context::{MarketData, TickerScope};
use crate::models::ParameterRange;
use crate::param_utils::{
    add_single_parameter_neighbor_variations, clamp_to_bounds, parameter_signature,
};
use anyhow::Result;
use log::{info, warn};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::path::Path;

const DETERMINISTIC_EXPLORE_SAMPLE_SEED: u64 = 0x5EED_1000;
const MAX_CACHED_EXPLORE_PARAMETER_SETS: usize = 10_000;
const MAX_NEW_EXPLORE_PARAMETER_SETS: usize = 1_000;

struct ExploreVariationPlan {
    seed_count: usize,
    neighbor_variations: Vec<HashMap<String, f64>>,
}

pub async fn run(app: &AppContext, template_id: &str, market_data_file: &Path) -> Result<()> {
    info!("Received explore command for template_id={}", template_id);
    ensure_market_data_file(market_data_file).await?;
    info!(
        "Using market data snapshot from {}",
        market_data_file.display()
    );

    let (settings, cached_parameter_sets) = match app.database().await {
        Ok(db) => (
            db.get_all_settings().await?,
            db.backtest_cache_entries_for_template(template_id)
                .await?
                .into_iter()
                .map(|entry| entry.parameters)
                .collect::<Vec<_>>(),
        ),
        Err(error) => {
            warn!(
                "Database unavailable ({}). Using settings from market data snapshot.",
                error
            );
            let snapshot = MarketData::load_from_file(market_data_file)?;
            (snapshot.settings().clone(), Vec::new())
        }
    };

    if cached_parameter_sets.is_empty() {
        info!(
            "No cached backtest rows found for template {}. Skipping explore.",
            template_id
        );
        return Ok(());
    }

    if cached_parameter_sets.len() > MAX_CACHED_EXPLORE_PARAMETER_SETS {
        info!(
            "Skipping explore for template {} because it has {} cached parameter set(s), above the {} limit.",
            template_id,
            cached_parameter_sets.len(),
            MAX_CACHED_EXPLORE_PARAMETER_SETS
        );
        return Ok(());
    }

    let training_start = require_setting_date(&settings, "OPTIMIZER_TRAINING_START_DATE")?;
    let training_end = require_setting_date(&settings, "OPTIMIZER_TRAINING_END_DATE")?;
    info!(
        "Restricting explore runs to training tickers and {} - {} market data window",
        training_start.format("%Y-%m-%d"),
        training_end.format("%Y-%m-%d")
    );

    let mut context = app
        .engine_context_from_file(
            market_data_file,
            TickerScope::TrainingOnly,
            Some(MarketDataFilters {
                start_date: Some(training_start),
                end_date: Some(training_end),
            }),
        )
        .await?;
    let mut optimizer = context.optimizer();

    let (param_names, param_ranges) =
        match optimizer.detect_optimizable_parameters(template_id).await {
            Ok(result) => result,
            Err(error) => {
                let message = error.to_string();
                if message.contains("No optimizable parameters") {
                    info!("Skipping explore for {}: {}", template_id, message);
                    return Ok(());
                }
                return Err(error);
            }
        };

    let mut baseline_params = optimizer.build_baseline_parameters(template_id).await?;
    clamp_to_bounds(&mut baseline_params, &param_ranges, &param_names);
    let backtest_initial_capital = resolve_backtest_initial_capital(&settings);

    let step_multipliers = [-1.0_f64, 1.0_f64];
    let ExploreVariationPlan {
        seed_count,
        mut neighbor_variations,
    } = build_explore_variation_plan(
        &baseline_params,
        &cached_parameter_sets,
        &param_names,
        &param_ranges,
        &step_multipliers,
    );

    info!(
        "Generated {} neighbor variation(s) from {} unique cached parameter seed(s) for template {}",
        neighbor_variations.len(),
        seed_count,
        template_id
    );

    let cached_variation_count = filter_cached_neighbor_variations(
        &mut neighbor_variations,
        &cached_parameter_sets,
        backtest_initial_capital,
    );
    if cached_variation_count > 0 {
        info!(
            "Skipping {} already cached explore variation(s) for template {}",
            cached_variation_count, template_id
        );
    }

    let sampled_away_count =
        sample_parameter_sets(&mut neighbor_variations, MAX_NEW_EXPLORE_PARAMETER_SETS);
    if sampled_away_count > 0 {
        info!(
            "Randomly selected {} uncached explore variation(s) out of {} eligible variation(s) for template {} (skipped {})",
            neighbor_variations.len(),
            neighbor_variations.len() + sampled_away_count,
            template_id,
            sampled_away_count
        );
    }

    if neighbor_variations.is_empty() {
        info!(
            "No new neighbor variations to explore for template {}",
            template_id
        );
        return Ok(());
    }

    info!(
        "Running explore backtests for up to {} new variation(s) on template {}",
        MAX_NEW_EXPLORE_PARAMETER_SETS, template_id
    );
    let results = optimizer
        .run_parameter_batch(template_id, &neighbor_variations, true)
        .await?;

    info!(
        "Explore completed for template {}: {} result(s) returned",
        template_id,
        results.len()
    );

    Ok(())
}

fn build_explore_variation_plan(
    baseline_params: &HashMap<String, f64>,
    cached_parameter_sets: &[HashMap<String, f64>],
    param_names: &[String],
    param_ranges: &HashMap<String, ParameterRange>,
    step_multipliers: &[f64],
) -> ExploreVariationPlan {
    let mut seen_variations = HashSet::new();
    let mut neighbor_variations = Vec::new();
    let mut seen_seed_signatures = HashSet::new();

    let mut push_seed_neighbors = |mut seed_params: HashMap<String, f64>| {
        seed_params.remove("initialCapital");
        clamp_to_bounds(&mut seed_params, param_ranges, param_names);

        let signature = parameter_signature(&seed_params);
        if !seen_seed_signatures.insert(signature) {
            return;
        }

        add_single_parameter_neighbor_variations(
            param_names,
            param_ranges,
            step_multipliers,
            &seed_params,
            &mut seen_variations,
            &mut neighbor_variations,
        );
    };

    for cached_params in cached_parameter_sets {
        let mut seed_params = baseline_params.clone();
        for (key, value) in cached_params {
            seed_params.insert(key.clone(), *value);
        }
        push_seed_neighbors(seed_params);
    }

    ExploreVariationPlan {
        seed_count: seen_seed_signatures.len(),
        neighbor_variations,
    }
}

fn filter_cached_neighbor_variations(
    neighbor_variations: &mut Vec<HashMap<String, f64>>,
    cached_parameter_sets: &[HashMap<String, f64>],
    backtest_initial_capital: f64,
) -> usize {
    let cached_signatures = cached_parameter_sets
        .iter()
        .map(parameter_signature)
        .collect::<HashSet<_>>();
    let original_count = neighbor_variations.len();

    neighbor_variations.retain(|params| {
        let mut params_with_initial_capital = params.clone();
        params_with_initial_capital.insert("initialCapital".to_string(), backtest_initial_capital);
        let signature = parameter_signature(&params_with_initial_capital);
        !cached_signatures.contains(&signature)
    });

    original_count.saturating_sub(neighbor_variations.len())
}

fn sample_parameter_sets(
    parameter_sets: &mut Vec<HashMap<String, f64>>,
    max_new_parameter_sets: usize,
) -> usize {
    if parameter_sets.len() <= max_new_parameter_sets {
        return 0;
    }

    let mut rng = StdRng::seed_from_u64(DETERMINISTIC_EXPLORE_SAMPLE_SEED);
    parameter_sets.shuffle(&mut rng);
    let skipped = parameter_sets.len() - max_new_parameter_sets;
    parameter_sets.truncate(max_new_parameter_sets);
    skipped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(entries: &[(&str, f64)]) -> HashMap<String, f64> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), *value))
            .collect()
    }

    fn signatures(variations: &[HashMap<String, f64>]) -> Vec<String> {
        let mut values = variations
            .iter()
            .map(parameter_signature)
            .collect::<Vec<_>>();
        values.sort();
        values
    }

    #[test]
    fn build_explore_variation_plan_uses_all_cached_parameter_sets() {
        let baseline = params(&[("length", 10.0)]);
        let cached = vec![params(&[("length", 0.0)]), params(&[("length", 10.0)])];
        let param_names = vec!["length".to_string()];
        let param_ranges = HashMap::from([(
            "length".to_string(),
            ParameterRange {
                min: 0.0,
                max: 20.0,
                step: 10.0,
            },
        )]);

        let plan = build_explore_variation_plan(
            &baseline,
            &cached,
            &param_names,
            &param_ranges,
            &[-1.0, 1.0],
        );

        assert_eq!(plan.seed_count, 2);
        assert_eq!(
            signatures(&plan.neighbor_variations),
            vec![
                parameter_signature(&params(&[("length", 0.0)])),
                parameter_signature(&params(&[("length", 10.0)])),
                parameter_signature(&params(&[("length", 20.0)])),
            ]
        );
    }

    #[test]
    fn build_explore_variation_plan_returns_empty_without_cache() {
        let baseline = params(&[("length", 10.0)]);
        let param_names = vec!["length".to_string()];
        let param_ranges = HashMap::from([(
            "length".to_string(),
            ParameterRange {
                min: 0.0,
                max: 20.0,
                step: 10.0,
            },
        )]);

        let plan =
            build_explore_variation_plan(&baseline, &[], &param_names, &param_ranges, &[-1.0, 1.0]);

        assert_eq!(plan.seed_count, 0);
        assert!(plan.neighbor_variations.is_empty());
    }

    #[test]
    fn build_explore_variation_plan_ignores_initial_capital_when_deduping_seed_sets() {
        let baseline = params(&[("length", 10.0)]);
        let cached = vec![
            params(&[("length", 10.0), ("initialCapital", 10_000.0)]),
            params(&[("length", 10.0), ("initialCapital", 25_000.0)]),
        ];
        let param_names = vec!["length".to_string()];
        let param_ranges = HashMap::from([(
            "length".to_string(),
            ParameterRange {
                min: 0.0,
                max: 20.0,
                step: 10.0,
            },
        )]);

        let plan = build_explore_variation_plan(
            &baseline,
            &cached,
            &param_names,
            &param_ranges,
            &[-1.0, 1.0],
        );

        assert_eq!(plan.seed_count, 1);
        assert_eq!(
            signatures(&plan.neighbor_variations),
            vec![
                parameter_signature(&params(&[("length", 0.0)])),
                parameter_signature(&params(&[("length", 20.0)])),
            ]
        );
    }

    #[test]
    fn filter_cached_neighbor_variations_skips_matching_current_initial_capital() {
        let mut neighbor_variations = vec![
            params(&[("length", 0.0)]),
            params(&[("length", 10.0)]),
            params(&[("length", 20.0)]),
        ];
        let cached = vec![params(&[("length", 10.0), ("initialCapital", 5_000.0)])];

        let skipped = filter_cached_neighbor_variations(&mut neighbor_variations, &cached, 5_000.0);

        assert_eq!(skipped, 1);
        assert_eq!(
            signatures(&neighbor_variations),
            vec![
                parameter_signature(&params(&[("length", 0.0)])),
                parameter_signature(&params(&[("length", 20.0)])),
            ]
        );
    }

    #[test]
    fn filter_cached_neighbor_variations_keeps_different_initial_capital() {
        let mut neighbor_variations = vec![params(&[("length", 10.0)])];
        let cached = vec![params(&[("length", 10.0), ("initialCapital", 10_000.0)])];

        let skipped = filter_cached_neighbor_variations(&mut neighbor_variations, &cached, 5_000.0);

        assert_eq!(skipped, 0);
        assert_eq!(
            signatures(&neighbor_variations),
            vec![parameter_signature(&params(&[("length", 10.0)]))]
        );
    }

    #[test]
    fn sample_parameter_sets_is_deterministic() {
        let build_parameter_sets = || {
            (0..6)
                .map(|index| params(&[("length", index as f64)]))
                .collect::<Vec<_>>()
        };

        let mut first = build_parameter_sets();
        let mut second = build_parameter_sets();

        let first_skipped = sample_parameter_sets(&mut first, 3);
        let second_skipped = sample_parameter_sets(&mut second, 3);

        assert_eq!(first_skipped, 3);
        assert_eq!(second_skipped, 3);
        assert_eq!(signatures(&first), signatures(&second));
    }
}
