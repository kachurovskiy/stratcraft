use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use std::collections::HashMap;

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
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "cagr" => Ok(Self::Cagr),
            "sharpe" | "sharpe_ratio" => Ok(Self::Sharpe),
            other => Err(anyhow!(
                "OPTIMIZATION_OBJECTIVE must be CAGR or SHARPE (value: {})",
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
    pub short_borrow_fee_annual_rate: f64,
    pub trade_entry_price_min: f64,
    pub trade_entry_price_max: f64,
    pub minimum_dollar_volume_for_entry: f64,
    pub minimum_dollar_volume_lookback: usize,
    pub local_optimization_version: i32,
    pub local_optimization_step_multipliers: Vec<f64>,
    pub local_optimization_objective: LocalOptimizationObjective,
    pub max_allowed_drawdown_ratio: f64,
}

impl EngineRuntimeSettings {
    pub fn from_settings_map(settings: &HashMap<String, String>) -> Result<Self> {
        let trade_close_fee_rate =
            require_setting_f64(settings, "TRADE_CLOSE_FEE_RATE", Some(0.0), None)?;
        let trade_slippage_rate =
            require_setting_f64(settings, "TRADE_SLIPPAGE_RATE", Some(0.0), None)?;
        let short_borrow_fee_annual_rate =
            require_setting_f64(settings, "SHORT_BORROW_FEE_ANNUAL_RATE", Some(0.0), None)?;
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
        let local_optimization_step_multipliers =
            require_setting_f64_list(settings, "LOCAL_OPTIMIZATION_STEP_MULTIPLIERS")?;
        let raw_local_optimization_objective = settings
            .get("OPTIMIZATION_OBJECTIVE")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("cagr");
        let local_optimization_objective =
            LocalOptimizationObjective::parse(raw_local_optimization_objective)?;
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
            short_borrow_fee_annual_rate,
            trade_entry_price_min,
            trade_entry_price_max,
            minimum_dollar_volume_for_entry,
            minimum_dollar_volume_lookback,
            local_optimization_version,
            local_optimization_step_multipliers,
            local_optimization_objective,
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

        Self {
            initial_capital: get_param(parameters, "initialCapital", 100000.0),
            trade_size_ratio: get_param(parameters, "tradeSizeRatio", 0.02),
            sell_fraction: coerce_binary_param(get_param(parameters, "sellFraction", 1.0), 1.0),
            minimum_trade_size: get_param(parameters, "minimumTradeSize", 50.0),
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

    if values.is_empty() {
        return Err(anyhow!(
            "Setting {} must contain at least one number (value: {})",
            key,
            raw
        ));
    }

    Ok(values)
}
