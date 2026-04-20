use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use std::collections::HashMap;

const BACKTEST_INITIAL_CAPITAL_SETTING: &str = "BACKTEST_INITIAL_CAPITAL";
const DEFAULT_BACKTEST_INITIAL_CAPITAL: f64 = 100000.0;
const DEFAULT_MARKET_ORDER_PRICE_CAP_RATIO: f64 = 0.08;
const DEFAULT_LIMIT_BUY_PENETRATION_RATIO: f64 = 0.005;
const DEFAULT_MIN_VOLUME_USD: f64 = 150_000.0;
const DEFAULT_MAX_VOLUME_USD: f64 = 51_000_000_000.0;
const DEFAULT_LOCAL_OPTIMIZATION_MAX_UNADJUSTED_PRICE_VALUES: [f64; 6] =
    [3.0, 5.0, 7.0, 10.0, 15.0, 20.0];
const DEFAULT_LOCAL_OPTIMIZATION_MAX_VOLUME_USD_VALUES: [f64; 3] =
    [1_000_000.0, 10_000_000.0, 100_000_000.0];

pub fn resolve_backtest_initial_capital(settings: &HashMap<String, String>) -> f64 {
    let raw = settings
        .get(BACKTEST_INITIAL_CAPITAL_SETTING)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    let parsed = raw
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0);
    parsed.unwrap_or(DEFAULT_BACKTEST_INITIAL_CAPITAL)
}

/// Configuration for position sizing strategies
#[derive(Debug, Clone)]
pub struct PositionSizingConfig {
    pub mode: i32, // 0=fixed, 1=confidence, 2=vol_target, 3=conf+vol
    pub vol_target_annual: f64,
    pub vol_lookback: usize,
}

impl Default for PositionSizingConfig {
    fn default() -> Self {
        Self {
            mode: 0,
            vol_target_annual: 0.0,
            vol_lookback: 20,
        }
    }
}

/// Configuration for stop loss strategies
#[derive(Debug, Clone)]
pub struct StopLossConfig {
    pub mode: i32, // 0=percent, 1=atr
    pub ratio: f64,
    pub atr_period: usize,
    pub atr_multiplier: f64,
}

impl Default for StopLossConfig {
    fn default() -> Self {
        Self {
            mode: 0,
            ratio: 0.05,
            atr_period: 20,
            atr_multiplier: 2.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalOptimizationObjective {
    Cagr,
    Sharpe,
}

impl LocalOptimizationObjective {
    pub fn parse(raw: &str, setting_key: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "cagr" => Ok(Self::Cagr),
            "sharpe" | "sharpe_ratio" => Ok(Self::Sharpe),
            other => Err(anyhow!(
                "{} must be CAGR or SHARPE (value: {})",
                setting_key,
                other
            )),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Cagr => "CAGR",
            Self::Sharpe => "Sharpe ratio",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineRuntimeSettings {
    pub trade_close_fee_rate: f64,
    pub trade_slippage_rate: f64,
    pub limit_buy_penetration_ratio: f64,
    pub short_borrow_fee_annual_rate: f64,
    pub market_order_price_cap_ratio: f64,
    pub trade_entry_price_min: f64,
    pub trade_entry_price_max: f64,
    pub minimum_dollar_volume_for_entry: f64,
    pub minimum_dollar_volume_lookback: usize,
    pub local_optimization_version: i32,
    pub local_optimization_multi_start_seeds: usize,
    pub local_optimization_step_multipliers: Vec<f64>,
    pub local_optimization_max_unadjusted_price_values: Vec<f64>,
    pub local_optimization_max_volume_usd_values: Vec<f64>,
    pub local_optimization_objective: LocalOptimizationObjective,
    pub local_optimization_objective_2: LocalOptimizationObjective,
    pub max_allowed_drawdown_ratio: f64,
}

impl EngineRuntimeSettings {
    pub fn from_settings_map(settings: &HashMap<String, String>) -> Result<Self> {
        let trade_close_fee_rate =
            require_setting_f64(settings, "TRADE_CLOSE_FEE_RATE", Some(0.0), None)?;
        let trade_slippage_rate =
            require_setting_f64(settings, "TRADE_SLIPPAGE_RATE", Some(0.0), None)?;
        let limit_buy_penetration_ratio =
            require_setting_f64(settings, "LIMIT_BUY_PENETRATION_RATIO", Some(0.0), None)
                .unwrap_or(DEFAULT_LIMIT_BUY_PENETRATION_RATIO);
        let short_borrow_fee_annual_rate =
            require_setting_f64(settings, "SHORT_BORROW_FEE_ANNUAL_RATE", Some(0.0), None)?;
        let market_order_price_cap_ratio =
            require_setting_f64(settings, "MARKET_ORDER_PRICE_CAP_RATIO", Some(0.0), None)
                .unwrap_or(DEFAULT_MARKET_ORDER_PRICE_CAP_RATIO);
        let trade_entry_price_min =
            require_setting_f64(settings, "TRADE_ENTRY_PRICE_MIN", Some(0.0), None)?;
        let trade_entry_price_max =
            require_setting_f64(settings, "TRADE_ENTRY_PRICE_MAX", Some(0.0), None)?;
        let minimum_dollar_volume_for_entry =
            require_setting_f64(settings, "MINIMUM_DOLLAR_VOLUME_FOR_ENTRY", Some(0.0), None)?;
        let minimum_dollar_volume_lookback =
            require_setting_usize(settings, "MINIMUM_DOLLAR_VOLUME_LOOKBACK", 0)?;
        let local_optimization_version =
            require_setting_i32(settings, "LOCAL_OPTIMIZATION_VERSION", 0)?;
        let local_optimization_multi_start_seeds = match settings
            .get("LOCAL_OPTIMIZATION_MULTI_START_SEEDS")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            Some(raw) => {
                let value = raw.parse::<f64>().map_err(|_| {
                    anyhow!(
                        "Setting LOCAL_OPTIMIZATION_MULTI_START_SEEDS must be a number (value: {})",
                        raw
                    )
                })?;
                if !value.is_finite() {
                    return Err(anyhow!(
                        "Setting LOCAL_OPTIMIZATION_MULTI_START_SEEDS must be finite (value: {})",
                        raw
                    ));
                }
                if value.fract() != 0.0 {
                    return Err(anyhow!(
                        "Setting LOCAL_OPTIMIZATION_MULTI_START_SEEDS must be an integer (value: {})",
                        raw
                    ));
                }
                if value < 0.0 {
                    return Err(anyhow!(
                        "Setting LOCAL_OPTIMIZATION_MULTI_START_SEEDS must be >= 0 (value: {})",
                        raw
                    ));
                }
                value as usize
            }
            None => 0,
        };
        let local_optimization_step_multipliers =
            require_setting_f64_list(settings, "LOCAL_OPTIMIZATION_STEP_MULTIPLIERS")?;
        let local_optimization_max_unadjusted_price_values = match settings
            .get("LOCAL_OPTIMIZATION_MAX_UNADJUSTED_PRICE_VALUES")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            Some(raw) => {
                parse_f64_list(raw, "LOCAL_OPTIMIZATION_MAX_UNADJUSTED_PRICE_VALUES", true)?
            }
            None => DEFAULT_LOCAL_OPTIMIZATION_MAX_UNADJUSTED_PRICE_VALUES.to_vec(),
        };
        let local_optimization_max_volume_usd_values = match settings
            .get("LOCAL_OPTIMIZATION_MAX_VOLUME_USD_VALUES")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            Some(raw) => parse_f64_list(raw, "LOCAL_OPTIMIZATION_MAX_VOLUME_USD_VALUES", true)?,
            None => DEFAULT_LOCAL_OPTIMIZATION_MAX_VOLUME_USD_VALUES.to_vec(),
        };
        let raw_local_optimization_objective = settings
            .get("OPTIMIZATION_OBJECTIVE")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("cagr");
        let local_optimization_objective = LocalOptimizationObjective::parse(
            raw_local_optimization_objective,
            "OPTIMIZATION_OBJECTIVE",
        )?;
        let raw_local_optimization_objective_2 = settings
            .get("OPTIMIZATION_OBJECTIVE_2")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("cagr");
        let local_optimization_objective_2 = LocalOptimizationObjective::parse(
            raw_local_optimization_objective_2,
            "OPTIMIZATION_OBJECTIVE_2",
        )?;
        let max_allowed_drawdown_ratio =
            require_setting_f64(settings, "MAX_ALLOWED_DRAWDOWN_RATIO", Some(0.0), Some(1.0))?;

        if trade_entry_price_max < trade_entry_price_min {
            return Err(anyhow!(
                "TRADE_ENTRY_PRICE_MAX ({}) must be >= TRADE_ENTRY_PRICE_MIN ({})",
                trade_entry_price_max,
                trade_entry_price_min
            ));
        }

        Ok(Self {
            trade_close_fee_rate,
            trade_slippage_rate,
            limit_buy_penetration_ratio,
            short_borrow_fee_annual_rate,
            market_order_price_cap_ratio,
            trade_entry_price_min,
            trade_entry_price_max,
            minimum_dollar_volume_for_entry,
            minimum_dollar_volume_lookback,
            local_optimization_version,
            local_optimization_multi_start_seeds,
            local_optimization_step_multipliers,
            local_optimization_max_unadjusted_price_values,
            local_optimization_max_volume_usd_values,
            local_optimization_objective,
            local_optimization_objective_2,
            max_allowed_drawdown_ratio,
        })
    }
}

/// Main engine configuration struct that groups all parameters
#[derive(Debug, Clone)]
pub struct EngineConfig {
    // Capital and trading parameters
    pub initial_capital: f64,
    pub trade_size_ratio: f64,
    pub sell_fraction: f64,
    pub minimum_trade_size: f64,
    pub minimum_size_as_allocation: f64,
    pub min_unadjusted_price: f64,
    pub max_unadjusted_price: f64,
    pub min_volume_usd: f64,
    pub max_volume_usd: f64,
    pub max_leverage: f64,
    pub allow_short_selling: bool,
    // Buy parameters
    pub buy_discount_ratio: f64,

    // Holding and limits
    pub max_holding_days: i32,

    // Grouped configurations
    pub position_sizing: PositionSizingConfig,
    pub stop_loss: StopLossConfig,

    // Raw parameters for reference
    pub raw_parameters: HashMap<String, f64>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            initial_capital: 100000.0,
            trade_size_ratio: 0.02,
            sell_fraction: 1.0,
            minimum_trade_size: 50.0,
            minimum_size_as_allocation: 0.0,
            min_unadjusted_price: 0.1,
            max_unadjusted_price: 1000.0,
            min_volume_usd: DEFAULT_MIN_VOLUME_USD,
            max_volume_usd: DEFAULT_MAX_VOLUME_USD,
            max_leverage: 1.0,
            allow_short_selling: false,
            buy_discount_ratio: 0.0,
            max_holding_days: 365,
            position_sizing: PositionSizingConfig::default(),
            stop_loss: StopLossConfig::default(),
            raw_parameters: HashMap::new(),
        }
    }
}

impl EngineConfig {
    /// Create a new EngineConfig from a parameter map
    pub fn from_parameters(parameters: &HashMap<String, f64>) -> Self {
        use crate::param_utils::*;

        let max_leverage_raw = get_param(parameters, "maxLeverage", 1.0);
        let max_leverage = if max_leverage_raw.is_finite() && max_leverage_raw >= 1.0 {
            max_leverage_raw
        } else {
            1.0
        };
        let min_unadjusted_price_raw = get_param(parameters, "minUnadjustedPrice", 0.1);
        let min_unadjusted_price =
            if min_unadjusted_price_raw.is_finite() && min_unadjusted_price_raw >= 0.1 {
                min_unadjusted_price_raw
            } else {
                0.1
            };
        let max_unadjusted_price_raw = get_param(parameters, "maxUnadjustedPrice", 1000.0);
        let max_unadjusted_price = if max_unadjusted_price_raw.is_finite()
            && max_unadjusted_price_raw >= min_unadjusted_price
        {
            max_unadjusted_price_raw
        } else {
            1000.0f64.max(min_unadjusted_price)
        };
        let min_volume_usd_raw = get_param(parameters, "minVolumeUsd", DEFAULT_MIN_VOLUME_USD);
        let min_volume_usd = if min_volume_usd_raw.is_finite() && min_volume_usd_raw >= 0.0 {
            min_volume_usd_raw
        } else {
            DEFAULT_MIN_VOLUME_USD
        };
        let max_volume_usd_raw = get_param(parameters, "maxVolumeUsd", DEFAULT_MAX_VOLUME_USD);
        let max_volume_usd =
            if max_volume_usd_raw.is_finite() && max_volume_usd_raw >= min_volume_usd {
                max_volume_usd_raw
            } else {
                DEFAULT_MAX_VOLUME_USD.max(min_volume_usd)
            };

        Self {
            initial_capital: get_param(parameters, "initialCapital", 100000.0),
            trade_size_ratio: get_param(parameters, "tradeSizeRatio", 0.02),
            sell_fraction: coerce_binary_param(get_param(parameters, "sellFraction", 1.0), 1.0),
            minimum_trade_size: get_param(parameters, "minimumTradeSize", 50.0),
            minimum_size_as_allocation: clamp_f64(
                get_param(parameters, "minimumSizeAsAllocation", 0.0),
                0.0,
                0.0,
                1.0,
            ),
            min_unadjusted_price,
            max_unadjusted_price,
            min_volume_usd,
            max_volume_usd,
            max_leverage,
            allow_short_selling: get_param(parameters, "allowShortSelling", 0.0) >= 0.5,
            buy_discount_ratio: get_param(parameters, "buyDiscountRatio", 0.0),
            max_holding_days: get_rounded_param(parameters, "maxHoldingDays", 365),
            position_sizing: PositionSizingConfig {
                mode: get_rounded_param(parameters, "positionSizingMode", 0),
                vol_target_annual: get_param(parameters, "volTargetAnnual", 0.0),
                vol_lookback: get_usize_param_min(parameters, "volLookback", 20, 1),
            },
            stop_loss: StopLossConfig {
                mode: get_rounded_param(parameters, "stopLossMode", 0),
                ratio: get_param(parameters, "stopLossRatio", 0.05),
                atr_period: get_usize_param_min(parameters, "atrPeriod", 20, 1),
                atr_multiplier: get_param(parameters, "atrMultiplier", 2.0),
            },
            raw_parameters: parameters.clone(),
        }
    }
}

fn require_setting<'a>(settings: &'a HashMap<String, String>, key: &str) -> Result<&'a str> {
    settings
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Missing required setting {}", key))
}

pub fn require_setting_date(settings: &HashMap<String, String>, key: &str) -> Result<NaiveDate> {
    let raw = require_setting(settings, key)?;
    NaiveDate::parse_from_str(raw, "%Y-%m-%d").map_err(|_| {
        anyhow!(
            "Setting {} must be a date in YYYY-MM-DD format (value: {})",
            key,
            raw
        )
    })
}

fn require_setting_f64(
    settings: &HashMap<String, String>,
    key: &str,
    min: Option<f64>,
    max: Option<f64>,
) -> Result<f64> {
    let raw = require_setting(settings, key)?;
    let value = raw
        .parse::<f64>()
        .map_err(|_| anyhow!("Setting {} must be a number (value: {})", key, raw))?;
    if !value.is_finite() {
        return Err(anyhow!("Setting {} must be finite (value: {})", key, raw));
    }
    if let Some(min_value) = min {
        if value < min_value {
            return Err(anyhow!(
                "Setting {} must be >= {} (value: {})",
                key,
                min_value,
                raw
            ));
        }
    }
    if let Some(max_value) = max {
        if value > max_value {
            return Err(anyhow!(
                "Setting {} must be <= {} (value: {})",
                key,
                max_value,
                raw
            ));
        }
    }
    Ok(value)
}

fn require_setting_usize(
    settings: &HashMap<String, String>,
    key: &str,
    min: usize,
) -> Result<usize> {
    let raw = require_setting(settings, key)?;
    let value = raw
        .parse::<f64>()
        .map_err(|_| anyhow!("Setting {} must be a number (value: {})", key, raw))?;
    if !value.is_finite() {
        return Err(anyhow!("Setting {} must be finite (value: {})", key, raw));
    }
    if value.fract() != 0.0 {
        return Err(anyhow!(
            "Setting {} must be an integer (value: {})",
            key,
            raw
        ));
    }
    if value < min as f64 {
        return Err(anyhow!(
            "Setting {} must be >= {} (value: {})",
            key,
            min,
            raw
        ));
    }
    Ok(value as usize)
}

fn require_setting_i32(settings: &HashMap<String, String>, key: &str, min: i32) -> Result<i32> {
    let raw = require_setting(settings, key)?;
    let value = raw
        .parse::<i32>()
        .map_err(|_| anyhow!("Setting {} must be an integer (value: {})", key, raw))?;
    if value < min {
        return Err(anyhow!(
            "Setting {} must be >= {} (value: {})",
            key,
            min,
            raw
        ));
    }
    Ok(value)
}

fn require_setting_f64_list(settings: &HashMap<String, String>, key: &str) -> Result<Vec<f64>> {
    let raw = require_setting(settings, key)?;
    parse_f64_list(raw, key, false)
}

fn parse_f64_list(raw: &str, key: &str, allow_empty: bool) -> Result<Vec<f64>> {
    let trimmed = raw.trim().trim_matches(|c| c == '[' || c == ']');
    let mut values = Vec::new();

    for part in trimmed.split(|c: char| c == ',' || c.is_whitespace()) {
        let entry = part.trim();
        if entry.is_empty() {
            continue;
        }
        let value = entry
            .parse::<f64>()
            .map_err(|_| anyhow!("Setting {} must be a list of numbers (value: {})", key, raw))?;
        if !value.is_finite() {
            return Err(anyhow!(
                "Setting {} must contain only finite numbers (value: {})",
                key,
                raw
            ));
        }
        values.push(value);
    }

    if values.is_empty() && !allow_empty {
        return Err(anyhow!(
            "Setting {} must contain at least one number (value: {})",
            key,
            raw
        ));
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required_settings() -> HashMap<String, String> {
        HashMap::from([
            ("TRADE_CLOSE_FEE_RATE".to_string(), "0.0005".to_string()),
            ("TRADE_SLIPPAGE_RATE".to_string(), "0.02".to_string()),
            (
                "SHORT_BORROW_FEE_ANNUAL_RATE".to_string(),
                "0.003".to_string(),
            ),
            ("TRADE_ENTRY_PRICE_MIN".to_string(), "0.1".to_string()),
            ("TRADE_ENTRY_PRICE_MAX".to_string(), "1000".to_string()),
            (
                "MINIMUM_DOLLAR_VOLUME_FOR_ENTRY".to_string(),
                "150000".to_string(),
            ),
            (
                "MINIMUM_DOLLAR_VOLUME_LOOKBACK".to_string(),
                "5".to_string(),
            ),
            ("LOCAL_OPTIMIZATION_VERSION".to_string(), "9".to_string()),
            (
                "LOCAL_OPTIMIZATION_STEP_MULTIPLIERS".to_string(),
                "-1,1".to_string(),
            ),
            ("MAX_ALLOWED_DRAWDOWN_RATIO".to_string(), "0.3".to_string()),
        ])
    }

    #[test]
    fn runtime_settings_default_secondary_objective_is_cagr() {
        let mut settings = required_settings();
        settings.insert("OPTIMIZATION_OBJECTIVE".to_string(), "SHARPE".to_string());

        let runtime_settings = EngineRuntimeSettings::from_settings_map(&settings).unwrap();

        assert_eq!(
            runtime_settings.local_optimization_objective,
            LocalOptimizationObjective::Sharpe
        );
        assert_eq!(
            runtime_settings.local_optimization_objective_2,
            LocalOptimizationObjective::Cagr
        );
        assert_eq!(
            runtime_settings.local_optimization_max_volume_usd_values,
            DEFAULT_LOCAL_OPTIMIZATION_MAX_VOLUME_USD_VALUES.to_vec()
        );
    }

    #[test]
    fn runtime_settings_reports_invalid_secondary_objective_key() {
        let mut settings = required_settings();
        settings.insert("OPTIMIZATION_OBJECTIVE_2".to_string(), "NOPE".to_string());

        let error = EngineRuntimeSettings::from_settings_map(&settings).unwrap_err();

        assert!(error
            .to_string()
            .contains("OPTIMIZATION_OBJECTIVE_2 must be CAGR or SHARPE"));
    }
}
