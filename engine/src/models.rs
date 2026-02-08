use anyhow::{anyhow, Result as AnyResult};
use chrono::{DateTime, Utc};
use log::warn;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub ticker: String,
    pub date: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub unadjusted_close: Option<f64>,
    pub volume_shares: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeChange {
    pub field: String,
    pub old_value: Value,
    pub new_value: Value,
    pub changed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: String,
    pub strategy_id: String,
    pub ticker: String,
    pub quantity: i32,
    pub price: f64,
    pub date: DateTime<Utc>,
    pub status: TradeStatus,
    pub pnl: Option<f64>,
    #[serde(default)]
    pub fee: Option<f64>,
    pub exit_price: Option<f64>,
    pub exit_date: Option<DateTime<Utc>>,
    pub stop_loss: Option<f64>,
    pub stop_loss_triggered: Option<bool>,
    #[serde(default)]
    pub entry_order_id: Option<String>,
    #[serde(default)]
    pub entry_cancel_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub stop_order_id: Option<String>,
    #[serde(default)]
    pub exit_order_id: Option<String>,
    pub changes: Vec<TradeChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AccountOperationType {
    OpenPosition,
    ClosePosition,
    UpdateStopLoss,
}

impl AccountOperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AccountOperationType::OpenPosition => "open_position",
            AccountOperationType::ClosePosition => "close_position",
            AccountOperationType::UpdateStopLoss => "update_stop_loss",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountOperationPlan {
    pub trade_id: String,
    pub ticker: String,
    pub quantity: Option<i32>,
    pub price: Option<f64>,
    pub stop_loss: Option<f64>,
    pub previous_stop_loss: Option<f64>,
    pub triggered_at: DateTime<Utc>,
    pub operation_type: AccountOperationType,
    pub reason: Option<String>,
    pub order_type: Option<String>,
    pub discount_applied: Option<bool>,
    pub signal_confidence: Option<f64>,
    pub account_cash_at_plan: Option<f64>,
    pub days_held: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TradeStatus {
    Pending,
    Active,
    Closed,
    Cancelled,
}

impl TradeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TradeStatus::Pending => "pending",
            TradeStatus::Active => "active",
            TradeStatus::Closed => "closed",
            TradeStatus::Cancelled => "cancelled",
        }
    }
}

impl Trade {
    pub fn record_change<V>(
        &mut self,
        field: &str,
        old_value: &V,
        new_value: &V,
        changed_at: DateTime<Utc>,
    ) where
        V: Serialize,
    {
        if let (Ok(old_json), Ok(new_json)) = (
            serde_json::to_value(old_value),
            serde_json::to_value(new_value),
        ) {
            if old_json == new_json {
                return;
            }

            self.changes.push(TradeChange {
                field: field.to_string(),
                old_value: old_json,
                new_value: new_json,
                changed_at,
            });
        }
    }

    pub fn set_exit_price(&mut self, value: Option<f64>, changed_at: DateTime<Utc>) {
        let old = self.exit_price;
        self.record_change("exitPrice", &old, &value, changed_at);
        self.exit_price = value;
    }

    pub fn set_price(&mut self, value: f64, changed_at: DateTime<Utc>) {
        let old = self.price;
        self.record_change("price", &old, &value, changed_at);
        self.price = value;
    }

    pub fn set_date(&mut self, value: DateTime<Utc>, changed_at: DateTime<Utc>) {
        let old = self.date;
        self.record_change("date", &old, &value, changed_at);
        self.date = value;
    }

    pub fn set_exit_date(&mut self, value: Option<DateTime<Utc>>, changed_at: DateTime<Utc>) {
        let old = self.exit_date;
        self.record_change("exitDate", &old, &value, changed_at);
        self.exit_date = value;
    }

    pub fn set_status(&mut self, status: TradeStatus, changed_at: DateTime<Utc>) {
        let old = self.status.clone();
        self.record_change("status", &old, &status, changed_at);
        self.status = status;
    }

    pub fn set_stop_loss(&mut self, value: Option<f64>, changed_at: DateTime<Utc>) {
        let old = self.stop_loss;
        self.record_change("stopLoss", &old, &value, changed_at);
        self.stop_loss = value;
    }

    pub fn set_stop_loss_triggered(&mut self, value: Option<bool>, changed_at: DateTime<Utc>) {
        let old = self.stop_loss_triggered;
        self.record_change("stopLossTriggered", &old, &value, changed_at);
        self.stop_loss_triggered = value;
    }

    pub fn set_quantity(&mut self, quantity: i32, changed_at: DateTime<Utc>) {
        let old = self.quantity;
        self.record_change("quantity", &old, &quantity, changed_at);
        self.quantity = quantity;
    }

    pub fn set_fee(&mut self, value: Option<f64>, changed_at: DateTime<Utc>) {
        let old = self.fee;
        self.record_change("fee", &old, &value, changed_at);
        self.fee = value;
    }

    pub fn set_pnl(&mut self, value: Option<f64>, changed_at: DateTime<Utc>) {
        let old = self.pnl;
        self.record_change("pnl", &old, &value, changed_at);
        self.pnl = value;
    }

    pub fn set_ticker(&mut self, value: String, changed_at: DateTime<Utc>) {
        let old = self.ticker.clone();
        self.record_change("ticker", &old, &value, changed_at);
        self.ticker = value;
    }

    pub fn set_stop_order_id(&mut self, value: Option<String>, changed_at: DateTime<Utc>) {
        let old = self.stop_order_id.clone();
        self.record_change("stopOrderId", &old, &value, changed_at);
        self.stop_order_id = value;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyPerformance {
    pub total_trades: i32,
    pub winning_trades: i32,
    pub losing_trades: i32,
    pub win_rate: f64,
    pub total_return: f64,
    pub cagr: f64,
    pub sharpe_ratio: f64,
    pub calmar_ratio: f64,
    pub max_drawdown: f64,
    pub max_drawdown_percent: f64,
    pub avg_trade_return: f64,
    pub best_trade: f64,
    pub worst_trade: f64,
    pub total_tickers: i32,
    pub median_trade_duration: f64,
    pub median_trade_pnl: f64,
    pub median_trade_pnl_percent: f64,
    pub median_concurrent_trades: f64,
    pub avg_trade_duration: f64,
    pub avg_trade_pnl: f64,
    pub avg_trade_pnl_percent: f64,
    pub avg_concurrent_trades: f64,
    pub avg_losing_pnl: f64,
    pub avg_losing_pnl_percent: f64,
    pub avg_winning_pnl: f64,
    pub avg_winning_pnl_percent: f64,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestDataPoint {
    pub date: DateTime<Utc>,
    pub portfolio_value: f64,
    pub cash: f64,
    pub positions_value: f64,
    pub concurrent_trades: i32,
    pub missed_trades_due_to_cash: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyStateSnapshot {
    pub template_id: String,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    pub id: String,
    pub strategy_id: String,
    pub start_date: DateTime<Utc>,
    pub end_date: DateTime<Utc>,
    pub initial_capital: f64,
    pub final_portfolio_value: f64,
    pub performance: StrategyPerformance,
    pub daily_snapshots: Vec<BacktestDataPoint>,
    pub trades: Vec<Trade>,
    pub tickers: Vec<String>,
    pub ticker_scope: Option<String>,
    pub strategy_state: Option<StrategyStateSnapshot>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct BacktestRun {
    pub result: BacktestResult,
    #[allow(dead_code)]
    pub signals: Vec<GeneratedSignal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyTemplate {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub category: Option<String>,
    pub author: Option<String>,
    pub version: Option<String>,
    pub local_optimization_version: i32,
    pub parameters: Vec<StrategyParameter>,
    pub example_usage: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyParameter {
    pub name: String,
    pub r#type: String,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub step: Option<f64>,
    pub default: Option<serde_json::Value>,
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub id: String,
    pub name: String,
    pub template_id: String,
    pub account_id: Option<String>,
    pub excluded_tickers: Vec<String>,
    pub excluded_keywords: Vec<String>,
    pub parameters: HashMap<String, f64>,
    pub backtest_start_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct AccountCredentials {
    pub id: String,
    pub provider: String,
    pub environment: String,
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterRange {
    pub min: f64,
    pub max: f64,
    pub step: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationResult {
    #[serde(deserialize_with = "deserialize_parameters_map")]
    pub parameters: HashMap<String, f64>,
    #[serde(alias = "cagr", default)]
    pub cagr: f64,
    #[serde(alias = "sharpeRatio")]
    pub sharpe_ratio: f64,
    #[serde(alias = "totalReturn")]
    pub total_return: f64,
    #[serde(alias = "maxDrawdown")]
    pub max_drawdown: f64,
    #[serde(alias = "maxDrawdownRatio", default)]
    pub max_drawdown_ratio: f64,
    #[serde(alias = "winRate")]
    pub win_rate: f64,
    #[serde(alias = "totalTrades")]
    pub total_trades: i32,
    #[serde(alias = "calmarRatio", default)]
    pub calmar_ratio: f64,
}

const STRING_PARAM_NAN_TAG: u64 = 0x7ff8_0000_0000_0000;
const STRING_PARAM_NAN_MASK: u64 = 0x0007_ffff_ffff_ffff;

static STRING_PARAM_REGISTRY: OnceLock<Mutex<HashMap<u64, String>>> = OnceLock::new();
static STRING_PARAM_COUNTER: AtomicU64 = AtomicU64::new(1);

fn string_param_registry() -> &'static Mutex<HashMap<u64, String>> {
    STRING_PARAM_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn encode_string_parameter(value: &str) -> f64 {
    let normalized = value.trim();
    let mut id = STRING_PARAM_COUNTER.fetch_add(1, Ordering::Relaxed) & STRING_PARAM_NAN_MASK;
    if id == 0 {
        id = 1;
    }
    if let Ok(mut registry) = string_param_registry().lock() {
        registry.insert(id, normalized.to_string());
    }
    f64::from_bits(STRING_PARAM_NAN_TAG | id)
}

fn decode_string_parameter_value(value: f64) -> Option<String> {
    let bits = value.to_bits();
    if bits & STRING_PARAM_NAN_TAG != STRING_PARAM_NAN_TAG {
        return None;
    }
    let id = bits & STRING_PARAM_NAN_MASK;
    string_param_registry()
        .lock()
        .ok()
        .and_then(|registry| registry.get(&id).cloned())
}

fn normalize_parameter_map(raw: HashMap<String, Value>) -> HashMap<String, f64> {
    let mut cleaned = HashMap::with_capacity(raw.len());

    for (key, value) in raw.into_iter() {
        if let Some(num) = value.as_f64() {
            if num.is_finite() {
                cleaned.insert(key, num);
            } else {
                warn!(
                    "Skipping parameter `{}` due to non-finite numeric value {}",
                    key, value
                );
            }
            continue;
        }

        if let Some(text) = value.as_str() {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                warn!("Skipping parameter `{}` due to empty string value", key);
                continue;
            }
            match trimmed.parse::<f64>() {
                Ok(parsed) if parsed.is_finite() => {
                    cleaned.insert(key, parsed);
                }
                Ok(parsed) => {
                    warn!(
                        "Skipping parameter `{}` due to non-finite parsed value {}",
                        key, parsed
                    );
                }
                Err(_) => {
                    let encoded = encode_string_parameter(trimmed);
                    cleaned.insert(key, encoded);
                }
            }
            continue;
        }

        if let Some(boolean) = value.as_bool() {
            cleaned.insert(key, if boolean { 1.0 } else { 0.0 });
            continue;
        }

        if value.is_null() {
            warn!("Skipping parameter `{}` due to null value", key);
        } else if value.is_array() || value.is_object() {
            warn!(
                "Skipping parameter `{}` due to unsupported composite value {}",
                key, value
            );
        } else {
            warn!(
                "Skipping parameter `{}` due to unexpected JSON value {}",
                key, value
            );
        }
    }

    cleaned
}

pub fn get_string_parameter(parameters: &HashMap<String, f64>, key: &str) -> Option<String> {
    parameters
        .get(key)
        .and_then(|value| decode_string_parameter_value(*value))
}

fn deserialize_parameters_map<'de, D>(deserializer: D) -> Result<HashMap<String, f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = HashMap::<String, Value>::deserialize(deserializer)?;
    Ok(normalize_parameter_map(raw))
}

pub fn parse_parameter_map_from_json(json: &str) -> AnyResult<HashMap<String, f64>> {
    let raw: HashMap<String, Value> =
        serde_json::from_str(json).map_err(|error| anyhow!("Invalid parameter JSON: {}", error))?;
    Ok(normalize_parameter_map(raw))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategySignal {
    pub action: SignalAction,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct GeneratedSignal {
    pub date: DateTime<Utc>,
    pub ticker: String,
    pub action: SignalAction,
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalAction {
    Buy,
    Sell,
    Hold,
}

impl SignalAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            SignalAction::Buy => "buy",
            SignalAction::Sell => "sell",
            SignalAction::Hold => "hold",
        }
    }
}

impl FromStr for SignalAction {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "buy" => Ok(SignalAction::Buy),
            "sell" => Ok(SignalAction::Sell),
            "hold" => Ok(SignalAction::Hold),
            other => Err(anyhow!("Unknown signal action '{}'", other)),
        }
    }
}

pub fn generate_trade_id(
    strategy_id: &str,
    backtest_id: &str,
    ticker: &str,
    date: DateTime<Utc>,
) -> String {
    format!(
        "{}_{}_{}_{}",
        strategy_id,
        backtest_id,
        ticker,
        date.format("%Y-%m-%d")
    )
}

pub fn generate_signal_id(strategy_id: &str, ticker: &str, date: DateTime<Utc>) -> String {
    format!("{}_{}_{}", strategy_id, ticker, date.format("%Y-%m-%d"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerInfo {
    pub symbol: String,
    pub name: Option<String>,
    pub tradable: bool,
    pub shortable: bool,
    pub easy_to_borrow: bool,
    #[serde(default)]
    pub asset_type: Option<String>,
    #[serde(default)]
    pub expense_ratio: Option<f64>,
    pub market_cap: Option<f64>,
    pub volume_usd: Option<f64>,
    pub max_fluctuation_ratio: Option<f64>,
    pub last_updated: Option<DateTime<Utc>>,
    pub candle_count: Option<i64>,
    pub training: bool,
}

// API response structures for caching
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ApiCheckResponse {
    pub exists: bool,
    pub result: Option<OptimizationResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ApiStoreResponse {
    pub success: bool,
    pub message: Option<String>,
}

// Performance calculation structures
#[derive(Debug, Clone)]
pub struct DrawdownInfo {
    pub max_drawdown: f64,
    pub max_drawdown_percent: f64,
}

// Worker communication structures
#[derive(Debug, Clone)]
pub struct BacktestTask {
    pub id: String,
    pub template_id: String,
    pub parameters: HashMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct BacktestTaskResult {
    pub _task_id: String,
    pub result: Option<OptimizationResult>,
    pub _error: Option<String>,
}
