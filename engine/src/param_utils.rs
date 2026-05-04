use crate::models::{ParameterRange, StrategyTemplate};
use std::collections::{HashMap, HashSet};

const MULTI_START_MIN_SEEDS: usize = 8;
const MULTI_START_MAX_SEEDS: usize = 24;
const MULTI_START_ATTEMPT_MULTIPLIER: usize = 12;
const MAX_VOLUME_USD_PARAMETER: &str = "maxVolumeUsd";
const MILLION: f64 = 1_000_000.0;
const TEN_MILLION: f64 = 10_000_000.0;
const HUNDRED_MILLION: f64 = 100_000_000.0;
const BILLION: f64 = 1_000_000_000.0;
const TEN_BILLION: f64 = 10_000_000_000.0;
const STEP_EPSILON: f64 = 1e-9;

/// Extract a parameter as usize with a default value
pub fn get_param_usize(params: &HashMap<String, f64>, key: &str, default: usize) -> usize {
    params.get(key).map(|&v| v as usize).unwrap_or(default)
}

/// Extract a parameter as f64 with a default value
pub fn get_param_f64(params: &HashMap<String, f64>, key: &str, default: f64) -> f64 {
    params.get(key).copied().unwrap_or(default)
}

/// Extract a parameter as f64, clamped to a range with finite checks
pub fn get_param_f64_clamped(
    params: &HashMap<String, f64>,
    key: &str,
    default: f64,
    min: f64,
    max: f64,
) -> f64 {
    let raw = params.get(key).copied().unwrap_or(default);
    if !raw.is_finite() {
        return default;
    }
    raw.clamp(min, max)
}

/// Extract a parameter as usize, rounded and clamped to a range with finite checks
pub fn get_param_usize_rounded_clamped(
    params: &HashMap<String, f64>,
    key: &str,
    default: usize,
    min: usize,
    max: usize,
) -> usize {
    let raw = params.get(key).copied().unwrap_or(default as f64);
    if !raw.is_finite() {
        return default;
    }
    raw.round().clamp(min as f64, max as f64).max(min as f64) as usize
}

/// Extract a parameter as usize with a minimum value (no rounding or finite checks)
pub fn get_param_usize_at_least(
    params: &HashMap<String, f64>,
    key: &str,
    default: usize,
    min: usize,
) -> usize {
    params
        .get(key)
        .copied()
        .unwrap_or(default as f64)
        .max(min as f64) as usize
}

/// Clamp a raw parameter value to bounds with a finite check
pub fn clamp_f64(value: f64, default: f64, min: f64, max: f64) -> f64 {
    if !value.is_finite() {
        return default;
    }
    value.clamp(min, max)
}

/// Get a parameter value with a default fallback
pub fn get_param(params: &HashMap<String, f64>, key: &str, default: f64) -> f64 {
    params.get(key).copied().unwrap_or(default)
}

pub fn coerce_binary_param(value: f64, default: f64) -> f64 {
    if !value.is_finite() {
        return default;
    }

    if value >= 0.5 {
        1.0
    } else {
        0.0
    }
}

/// Get a parameter rounded to an i32
pub fn get_rounded_param(params: &HashMap<String, f64>, key: &str, default: i32) -> i32 {
    params
        .get(key)
        .copied()
        .map(|v| v.round() as i32)
        .unwrap_or(default)
}

/// Get a parameter rounded to a non-negative i32
pub fn get_rounded_param_nonneg(params: &HashMap<String, f64>, key: &str, default: i32) -> i32 {
    params
        .get(key)
        .copied()
        .map(|v| v.round().max(0.0) as i32)
        .unwrap_or(default)
}

/// Get a parameter as usize with a minimum value
pub fn get_usize_param_min(
    params: &HashMap<String, f64>,
    key: &str,
    default: usize,
    min: usize,
) -> usize {
    params
        .get(key)
        .copied()
        .filter(|v| v.is_finite())
        .map(|v| v.round().max(min as f64) as usize)
        .unwrap_or(default)
}

/// Clamp parameter values to their defined bounds
pub fn clamp_to_bounds(
    params: &mut HashMap<String, f64>,
    parameter_ranges: &HashMap<String, ParameterRange>,
    keys: &[String],
) {
    for key in keys {
        if let (Some(range), Some(value)) = (parameter_ranges.get(key), params.get_mut(key)) {
            *value = value.clamp(range.min, range.max);
        }
    }
}

pub fn parameter_signature(parameters: &HashMap<String, f64>) -> String {
    let mut sorted: Vec<_> = parameters.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    format!("{:?}", sorted)
}

pub fn collect_numeric_parameter_ranges(
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

pub fn target_multi_start_refinement_count(parameter_count: usize) -> usize {
    if parameter_count <= 1 {
        return 2;
    }
    if parameter_count <= 3 {
        return 3;
    }
    4
}

fn target_multi_start_seed_count(parameter_count: usize) -> usize {
    if parameter_count == 0 {
        return 1;
    }

    parameter_count
        .saturating_mul(4)
        .clamp(MULTI_START_MIN_SEEDS, MULTI_START_MAX_SEEDS)
}

fn is_prime(value: usize) -> bool {
    if value < 2 {
        return false;
    }
    if value == 2 {
        return true;
    }
    if value % 2 == 0 {
        return false;
    }

    let mut divisor = 3usize;
    while divisor.saturating_mul(divisor) <= value {
        if value % divisor == 0 {
            return false;
        }
        divisor += 2;
    }

    true
}

fn prime_for_dimension(index: usize) -> usize {
    const PRIMES: [usize; 16] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53];

    if let Some(prime) = PRIMES.get(index) {
        return *prime;
    }

    let mut found = PRIMES.len();
    let mut candidate = PRIMES[PRIMES.len() - 1] + 2;
    loop {
        if is_prime(candidate) {
            if found == index {
                return candidate;
            }
            found += 1;
        }
        candidate += 2;
    }
}

fn halton(index: usize, base: usize) -> f64 {
    if index == 0 || base < 2 {
        return 0.0;
    }

    let mut result = 0.0;
    let mut factor = 1.0 / base as f64;
    let mut current = index;

    while current > 0 {
        result += factor * (current % base) as f64;
        current /= base;
        factor /= base as f64;
    }

    result
}

fn quantize_sample_to_range(range: &ParameterRange, sample: f64) -> Option<f64> {
    if !range.min.is_finite()
        || !range.max.is_finite()
        || !range.step.is_finite()
        || range.step <= 0.0
    {
        return None;
    }

    if range.max < range.min {
        return None;
    }

    let span = range.max - range.min;
    if span <= 1e-9 {
        return Some(range.min);
    }

    let steps = ((span / range.step).round()).max(0.0) as usize;
    if steps == 0 {
        return Some(range.min.clamp(range.min, range.max));
    }

    let step_index = (sample.clamp(0.0, 1.0) * steps as f64)
        .round()
        .clamp(0.0, steps as f64) as usize;
    let value = range.min + step_index as f64 * range.step;
    Some(value.clamp(range.min, range.max))
}

pub fn build_multi_start_seeds(
    baseline_params: &HashMap<String, f64>,
    parameters_to_optimize: &[String],
    parameter_ranges: &HashMap<String, ParameterRange>,
) -> Vec<HashMap<String, f64>> {
    build_multi_start_seeds_with_limit(
        baseline_params,
        parameters_to_optimize,
        parameter_ranges,
        None,
    )
}

fn max_volume_usd_up_step(value: f64) -> f64 {
    if value < TEN_MILLION - STEP_EPSILON {
        MILLION
    } else if value < HUNDRED_MILLION - STEP_EPSILON {
        TEN_MILLION
    } else if value < BILLION - STEP_EPSILON {
        HUNDRED_MILLION
    } else if value < TEN_BILLION - STEP_EPSILON {
        BILLION
    } else {
        TEN_BILLION
    }
}

fn max_volume_usd_down_step(value: f64) -> f64 {
    if value <= TEN_MILLION + STEP_EPSILON {
        MILLION
    } else if value <= HUNDRED_MILLION + STEP_EPSILON {
        TEN_MILLION
    } else if value <= BILLION + STEP_EPSILON {
        HUNDRED_MILLION
    } else if value <= TEN_BILLION + STEP_EPSILON {
        BILLION
    } else {
        TEN_BILLION
    }
}

fn next_aligned_step_value(value: f64, step: f64) -> f64 {
    ((value / step).floor() + 1.0) * step
}

fn previous_aligned_step_value(value: f64, step: f64) -> f64 {
    ((value / step).ceil() - 1.0).max(0.0) * step
}

fn stepped_max_volume_usd_candidate(current_value: f64, multiplier: f64) -> Option<f64> {
    if !current_value.is_finite() || !multiplier.is_finite() || multiplier.abs() < STEP_EPSILON {
        return None;
    }

    let step_count = multiplier.abs().round();
    if (step_count - multiplier.abs()).abs() > STEP_EPSILON || step_count > 100.0 {
        let step = if multiplier.is_sign_positive() {
            max_volume_usd_up_step(current_value)
        } else {
            max_volume_usd_down_step(current_value)
        };
        return Some(current_value + multiplier * step);
    }

    let mut value = current_value;
    for _ in 0..step_count as usize {
        value = if multiplier.is_sign_positive() {
            next_aligned_step_value(value, max_volume_usd_up_step(value))
        } else {
            previous_aligned_step_value(value, max_volume_usd_down_step(value))
        };
    }

    Some(value)
}

pub fn build_multi_start_seeds_with_limit(
    baseline_params: &HashMap<String, f64>,
    parameters_to_optimize: &[String],
    parameter_ranges: &HashMap<String, ParameterRange>,
    seed_limit: Option<usize>,
) -> Vec<HashMap<String, f64>> {
    let target_seed_count = match seed_limit {
        Some(0) => 1,
        Some(count) => count.max(1),
        None => target_multi_start_seed_count(parameters_to_optimize.len()),
    };
    let mut seeds = Vec::with_capacity(target_seed_count);
    let mut seen_signatures = HashSet::new();

    let mut baseline = baseline_params.clone();
    clamp_to_bounds(&mut baseline, parameter_ranges, parameters_to_optimize);
    let baseline_signature = parameter_signature(&baseline);
    seen_signatures.insert(baseline_signature);
    seeds.push(baseline);

    if parameters_to_optimize.is_empty() || seeds.len() >= target_seed_count {
        return seeds;
    }

    let max_attempts = target_seed_count
        .saturating_mul(MULTI_START_ATTEMPT_MULTIPLIER)
        .max(target_seed_count);
    for sample_index in 1..=max_attempts {
        if seeds.len() >= target_seed_count {
            break;
        }

        let mut candidate = baseline_params.clone();
        for (dimension, param_name) in parameters_to_optimize.iter().enumerate() {
            let Some(range) = parameter_ranges.get(param_name) else {
                continue;
            };

            let sample = halton(sample_index, prime_for_dimension(dimension));
            if let Some(value) = quantize_sample_to_range(range, sample) {
                candidate.insert(param_name.clone(), value);
            }
        }

        clamp_to_bounds(&mut candidate, parameter_ranges, parameters_to_optimize);
        let signature = parameter_signature(&candidate);
        if seen_signatures.insert(signature) {
            seeds.push(candidate);
        }
    }

    seeds
}

/// Get a parameter rounded to an i32, returns None if not found or not finite
pub fn rounded_param(params: &HashMap<String, f64>, key: &str) -> Option<i32> {
    params
        .get(key)
        .copied()
        .filter(|v| v.is_finite())
        .map(|v| v.round() as i32)
}

/// Get a finite parameter value, returns None if not found or not finite
pub fn finite_param(params: &HashMap<String, f64>, key: &str) -> Option<f64> {
    params.get(key).copied().filter(|v| v.is_finite())
}

/// Check if a parameter is inactive based on the current parameter configuration
pub fn parameter_is_inactive(param_name: &str, params: &HashMap<String, f64>) -> bool {
    match param_name {
        "initialCapital" | "maxLeverage" => true,
        "stopLossRatio" => {
            if let Some(mode) = rounded_param(params, "stopLossMode") {
                mode == 1
            } else {
                false
            }
        }
        "atrPeriod" | "atrMultiplier" => {
            if let Some(mode) = rounded_param(params, "stopLossMode") {
                mode != 1
            } else {
                false
            }
        }
        "volTargetAnnual" => {
            if let (Some(mode), Some(vol_target)) = (
                rounded_param(params, "positionSizingMode"),
                finite_param(params, "volTargetAnnual"),
            ) {
                (mode != 2 && mode != 3) || vol_target <= 0.0
            } else {
                false
            }
        }
        "volLookback" => {
            if let (Some(mode), Some(vol_target)) = (
                rounded_param(params, "positionSizingMode"),
                finite_param(params, "volTargetAnnual"),
            ) {
                (mode != 2 && mode != 3) || vol_target <= 0.0
            } else {
                false
            }
        }
        _ => false,
    }
}

fn push_neighbor_variation(
    neighbor_params: HashMap<String, f64>,
    changed_params: &[&str],
    current_params: &HashMap<String, f64>,
    seen_variations: &mut HashSet<String>,
    neighbor_variations: &mut Vec<HashMap<String, f64>>,
) {
    if changed_params.is_empty() {
        return;
    }

    if changed_params.iter().all(|param| {
        parameter_is_inactive(param, current_params)
            && parameter_is_inactive(param, &neighbor_params)
    }) {
        return;
    }

    let mut sorted_params: Vec<_> = neighbor_params.iter().collect();
    sorted_params.sort_by(|a, b| a.0.cmp(b.0));
    let key = format!("{:?}", sorted_params);

    if seen_variations.insert(key) {
        neighbor_variations.push(neighbor_params);
    }
}

/// Add neighbor parameter variations by adjusting one parameter at a time
pub fn add_single_parameter_neighbor_variations(
    parameters_to_optimize: &[String],
    parameter_ranges: &HashMap<String, ParameterRange>,
    step_multipliers: &[f64],
    current_params: &HashMap<String, f64>,
    seen_variations: &mut HashSet<String>,
    neighbor_variations: &mut Vec<HashMap<String, f64>>,
) {
    for param in parameters_to_optimize {
        let range = match parameter_ranges.get(param) {
            Some(r) => r,
            None => continue,
        };
        let current_value = match current_params.get(param) {
            Some(v) => *v,
            None => continue,
        };

        for &multiplier in step_multipliers {
            let mut neighbor_params = current_params.clone();
            let candidate = if param == MAX_VOLUME_USD_PARAMETER {
                match stepped_max_volume_usd_candidate(current_value, multiplier) {
                    Some(value) => value,
                    None => continue,
                }
            } else {
                current_value + multiplier * range.step
            };

            if candidate < range.min - 1e-9 || candidate > range.max + 1e-9 {
                continue;
            }

            let new_value = candidate.clamp(range.min, range.max);
            if (new_value - current_value).abs() < 1e-9 {
                continue;
            }

            neighbor_params.insert(param.clone(), new_value);

            push_neighbor_variation(
                neighbor_params,
                &[param.as_str()],
                current_params,
                seen_variations,
                neighbor_variations,
            );
        }
    }
}

pub fn add_fixed_parameter_value_variations(
    param_name: &str,
    parameter_ranges: &HashMap<String, ParameterRange>,
    candidate_values: &[f64],
    current_params: &HashMap<String, f64>,
    seen_variations: &mut HashSet<String>,
    neighbor_variations: &mut Vec<HashMap<String, f64>>,
) {
    if candidate_values.is_empty() {
        return;
    }
    let range = match parameter_ranges.get(param_name) {
        Some(range) => range,
        None => return,
    };
    let current_value = match current_params.get(param_name) {
        Some(value) => *value,
        None => return,
    };

    for &candidate in candidate_values {
        if !candidate.is_finite() {
            continue;
        }
        if candidate < range.min - 1e-9 || candidate > range.max + 1e-9 {
            continue;
        }
        let new_value = candidate.clamp(range.min, range.max);
        if (new_value - current_value).abs() < 1e-9 {
            continue;
        }

        let mut neighbor_params = current_params.clone();
        neighbor_params.insert(param_name.to_string(), new_value);

        push_neighbor_variation(
            neighbor_params,
            &[param_name],
            current_params,
            seen_variations,
            neighbor_variations,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_multi_start_seeds_keeps_baseline_and_respects_grid() {
        let mut baseline = HashMap::new();
        baseline.insert("length".to_string(), 10.0);
        baseline.insert("threshold".to_string(), 0.5);
        baseline.insert("fixed".to_string(), 99.0);

        let parameters_to_optimize = vec!["length".to_string(), "threshold".to_string()];
        let mut ranges = HashMap::new();
        ranges.insert(
            "length".to_string(),
            ParameterRange {
                min: 5.0,
                max: 15.0,
                step: 5.0,
            },
        );
        ranges.insert(
            "threshold".to_string(),
            ParameterRange {
                min: 0.0,
                max: 1.0,
                step: 0.25,
            },
        );

        let seeds = build_multi_start_seeds(&baseline, &parameters_to_optimize, &ranges);

        assert!(
            seeds.len() > 1,
            "expected exploratory seeds beyond the baseline"
        );
        assert_eq!(seeds[0], baseline, "baseline seed should be first");

        for seed in seeds {
            let length = seed.get("length").copied().unwrap_or_default();
            let threshold = seed.get("threshold").copied().unwrap_or_default();
            let fixed = seed.get("fixed").copied().unwrap_or_default();

            assert!((5.0..=15.0).contains(&length));
            assert!((0.0..=1.0).contains(&threshold));
            assert!(
                ((length - 5.0) / 5.0 - ((length - 5.0) / 5.0).round()).abs() < 1e-9,
                "length must stay aligned to the configured step"
            );
            assert!(
                (threshold / 0.25 - (threshold / 0.25).round()).abs() < 1e-9,
                "threshold must stay aligned to the configured step"
            );
            assert_eq!(fixed, 99.0, "non-optimized parameters should be preserved");
        }
    }

    #[test]
    fn max_volume_usd_neighbors_use_piecewise_steps() {
        let parameters_to_optimize = vec![MAX_VOLUME_USD_PARAMETER.to_string()];
        let parameter_ranges = HashMap::from([(
            MAX_VOLUME_USD_PARAMETER.to_string(),
            ParameterRange {
                min: 150_000.0,
                max: 51_000_000_000.0,
                step: TEN_BILLION,
            },
        )]);
        let current_params = HashMap::from([(MAX_VOLUME_USD_PARAMETER.to_string(), 50_000_000.0)]);
        let mut seen_variations = HashSet::new();
        let mut neighbor_variations = Vec::new();

        add_single_parameter_neighbor_variations(
            &parameters_to_optimize,
            &parameter_ranges,
            &[-1.0, 1.0],
            &current_params,
            &mut seen_variations,
            &mut neighbor_variations,
        );

        let mut observed_values: Vec<_> = neighbor_variations
            .iter()
            .filter_map(|params| params.get(MAX_VOLUME_USD_PARAMETER).copied())
            .collect();
        observed_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        assert_eq!(
            observed_values,
            vec![40_000_000.0, 60_000_000.0],
            "50M should move by 10M, not by the template's 10B step"
        );
    }

    #[test]
    fn max_volume_usd_neighbors_switch_steps_at_boundaries() {
        let cases = [
            (10_000_000.0, 9_000_000.0, 20_000_000.0),
            (100_000_000.0, 90_000_000.0, 200_000_000.0),
            (1_000_000_000.0, 900_000_000.0, 2_000_000_000.0),
            (10_000_000_000.0, 9_000_000_000.0, 20_000_000_000.0),
        ];

        for (current, expected_down, expected_up) in cases {
            assert_eq!(
                stepped_max_volume_usd_candidate(current, -1.0),
                Some(expected_down)
            );
            assert_eq!(
                stepped_max_volume_usd_candidate(current, 1.0),
                Some(expected_up)
            );
        }
    }
}
