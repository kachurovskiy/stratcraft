use crate::models::ParameterRange;
use std::collections::{HashMap, HashSet};

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
        "initialCapital" => true,
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
            let candidate = current_value + multiplier * range.step;

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
