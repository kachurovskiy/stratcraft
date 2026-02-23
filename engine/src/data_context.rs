use crate::database::Database;
use crate::models::{Candle, StrategyParameter, StrategyTemplate, TickerInfo};
use crate::optimizer_status::OptimizerStatus;
use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

const MARKET_DATA_SNAPSHOT_VERSION: u32 = 5;
const SNAPSHOT_ALLOWED_SETTINGS: [&str; 22] = [
    "BACKTEST_INITIAL_CAPITAL",
    "BACKTEST_API_SECRET",
    "BALANCE_WINDOW_END_DATE",
    "BALANCE_WINDOW_START_DATE",
    "DOMAIN",
    "LIGHTGBM_TRAINING_END_DATE",
    "LIGHTGBM_TRAINING_START_DATE",
    "LOCAL_OPTIMIZATION_STEP_MULTIPLIERS",
    "LOCAL_OPTIMIZATION_VERSION",
    "MAX_ALLOWED_DRAWDOWN_RATIO",
    "MINIMUM_DOLLAR_VOLUME_FOR_ENTRY",
    "MINIMUM_DOLLAR_VOLUME_LOOKBACK",
    "OPTIMIZATION_OBJECTIVE",
    "OPTIMIZER_TRAINING_END_DATE",
    "OPTIMIZER_TRAINING_START_DATE",
    "SHORT_BORROW_FEE_ANNUAL_RATE",
    "TRADE_CLOSE_FEE_RATE",
    "TRADE_ENTRY_PRICE_MAX",
    "TRADE_ENTRY_PRICE_MIN",
    "TRADE_SLIPPAGE_RATE",
    "VERIFY_WINDOW_END_DATE",
    "VERIFY_WINDOW_START_DATE",
];

#[derive(Clone, Copy)]
pub enum TickerScope {
    AllTickers,
    TrainingOnly,
    ValidationOnly,
}

impl TickerScope {
    pub fn allows(self, info: &TickerInfo) -> bool {
        match self {
            TickerScope::AllTickers => true,
            TickerScope::TrainingOnly => info.training,
            TickerScope::ValidationOnly => !info.training,
        }
    }

    pub fn result_label(self) -> &'static str {
        match self {
            TickerScope::AllTickers => "all",
            TickerScope::TrainingOnly => "training",
            TickerScope::ValidationOnly => "validation",
        }
    }
}

#[derive(Serialize, Deserialize)]
struct MarketDataSnapshot {
    version: u32,
    generated_at: DateTime<Utc>,
    tickers: Vec<String>,
    unique_dates: Vec<DateTime<Utc>>,
    candles: Vec<Candle>,
    #[serde(default)]
    templates: HashMap<String, SnapshotTemplate>,
    #[serde(default)]
    ticker_expense_map: HashMap<String, f64>,
    #[serde(default)]
    settings: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotTemplate {
    id: String,
    name: String,
    description: Option<String>,
    category: Option<String>,
    author: Option<String>,
    version: Option<String>,
    local_optimization_version: i32,
    example_usage: Option<String>,
    created_at: DateTime<Utc>,
    parameters: Vec<SnapshotParameter>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotParameter {
    name: String,
    #[serde(rename = "type")]
    param_type: String,
    min: Option<f64>,
    max: Option<f64>,
    step: Option<f64>,
    default_json: Option<String>,
    description: Option<String>,
}

impl SnapshotTemplate {
    fn from_strategy_template(template: &StrategyTemplate) -> Self {
        Self {
            id: template.id.clone(),
            name: template.name.clone(),
            description: template.description.clone(),
            category: template.category.clone(),
            author: template.author.clone(),
            version: template.version.clone(),
            local_optimization_version: template.local_optimization_version,
            example_usage: template.example_usage.clone(),
            created_at: template.created_at,
            parameters: template
                .parameters
                .iter()
                .map(SnapshotParameter::from_strategy_parameter)
                .collect(),
        }
    }

    fn into_strategy_template(self) -> Result<StrategyTemplate> {
        let parameters = self
            .parameters
            .into_iter()
            .map(|param| param.into_strategy_parameter())
            .collect::<Result<Vec<_>>>()?;

        Ok(StrategyTemplate {
            id: self.id,
            name: self.name,
            description: self.description,
            category: self.category,
            author: self.author,
            version: self.version,
            local_optimization_version: self.local_optimization_version,
            parameters,
            example_usage: self.example_usage,
            created_at: self.created_at,
        })
    }
}

impl SnapshotParameter {
    fn from_strategy_parameter(param: &StrategyParameter) -> Self {
        let default_json = param
            .default
            .as_ref()
            .and_then(|value| serde_json::to_string(value).ok());
        Self {
            name: param.name.clone(),
            param_type: param.r#type.clone(),
            min: param.min,
            max: param.max,
            step: param.step,
            default_json,
            description: param.description.clone(),
        }
    }

    fn into_strategy_parameter(self) -> Result<StrategyParameter> {
        let default = match self.default_json {
            Some(json) => {
                let value: Value =
                    serde_json::from_str(&json).context("Invalid parameter default JSON")?;
                Some(value)
            }
            None => None,
        };

        Ok(StrategyParameter {
            name: self.name,
            r#type: self.param_type,
            min: self.min,
            max: self.max,
            step: self.step,
            default,
            description: self.description,
        })
    }
}

fn scrub_snapshot_settings(settings: &HashMap<String, String>) -> HashMap<String, String> {
    settings
        .iter()
        .filter(|(key, _)| SNAPSHOT_ALLOWED_SETTINGS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

pub struct MarketData {
    all_candles: Arc<Vec<Candle>>,
    unique_dates: Arc<Vec<DateTime<Utc>>>,
    tickers: Arc<Vec<String>>,
    candles_by_ticker_indices: Arc<HashMap<String, Vec<usize>>>,
    templates: Arc<HashMap<String, StrategyTemplate>>,
    ticker_expense_map: Arc<HashMap<String, f64>>,
    settings: Arc<HashMap<String, String>>,
}

impl MarketData {
    pub async fn load(db: &Database, scope: TickerScope) -> Result<Self> {
        info!("Getting tickers with candle data...");
        let ticker_infos = db.get_tickers_with_candle_counts().await?;

        let meets_requirements = |info: &TickerInfo| {
            let has_candles = info.candle_count.unwrap_or(0) > 0;
            let within_limit = info
                .max_fluctuation_ratio
                .map(|r| (0.03..10.0).contains(&r))
                .unwrap_or(true);
            (has_candles, within_limit)
        };

        let mut tickers: Vec<String> = Vec::new();
        for info in &ticker_infos {
            if !scope.allows(info) {
                continue;
            }
            let (has_candles, within_limit) = meets_requirements(info);
            if has_candles && within_limit {
                tickers.push(info.symbol.clone());
            }
        }

        info!("Loading candle data from database...");
        let mut all_candles = db.get_all_candles().await?;

        let ticker_set: HashSet<String> = tickers.iter().cloned().collect();
        all_candles.retain(|c| ticker_set.contains(&c.ticker));

        let mut ticker_expense_map: HashMap<String, f64> = HashMap::new();
        for info in &ticker_infos {
            if !ticker_set.contains(&info.symbol) {
                continue;
            }
            if let Some(ratio) = info.expense_ratio {
                if ratio.is_finite() && ratio > 0.0 {
                    ticker_expense_map.insert(info.symbol.clone(), ratio);
                }
            }
        }

        let mut candle_counts: HashMap<String, usize> = HashMap::new();
        for candle in &all_candles {
            *candle_counts.entry(candle.ticker.clone()).or_insert(0) += 1;
        }

        tickers.retain(|symbol| candle_counts.contains_key(symbol));
        if tickers.is_empty() || all_candles.is_empty() {
            return Err(anyhow!(
                "No candle data available after applying ticker and history filters"
            ));
        }

        let mut unique_date_set = BTreeSet::new();
        for candle in &all_candles {
            unique_date_set.insert(candle.date);
        }
        let unique_dates: Vec<_> = unique_date_set.into_iter().collect();
        if unique_dates.is_empty() {
            return Err(anyhow!(
                "No trading dates available after applying ticker and history filters"
            ));
        }

        info!(
            "Loaded {} candles for {} tickers across {} unique dates",
            all_candles.len(),
            tickers.len(),
            unique_dates.len()
        );

        let candles_by_ticker_indices = Self::build_candle_index(&all_candles);

        let templates_vec = db.get_all_templates().await?;
        if templates_vec.is_empty() {
            return Err(anyhow!("No strategy templates available in the database"));
        }
        let templates: HashMap<String, StrategyTemplate> = templates_vec
            .into_iter()
            .map(|template| (template.id.clone(), template))
            .collect();

        let settings = db.get_all_settings().await?;

        Self::from_components(
            tickers,
            unique_dates,
            all_candles,
            candles_by_ticker_indices,
            templates,
            ticker_expense_map,
            settings,
        )
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P, status: &OptimizerStatus) -> Result<Self> {
        let path = path.as_ref();
        status.set_phase(format!(
            "Loading market data snapshot from {}",
            path.display()
        ));
        let file = File::open(path).with_context(|| {
            format!("Failed to open market data snapshot at {}", path.display())
        })?;
        let reader = BufReader::new(file);
        let snapshot: MarketDataSnapshot =
            bincode::deserialize_from(reader).context("Snapshot decode failed")?;

        if snapshot.version != MARKET_DATA_SNAPSHOT_VERSION {
            return Err(anyhow!(
                "Market data snapshot version mismatch (found {}, expected {})",
                snapshot.version,
                MARKET_DATA_SNAPSHOT_VERSION
            ));
        }

        status.set_phase("Reconstructing market data snapshot");
        let candles_by_ticker_indices = Self::build_candle_index(&snapshot.candles);

        let templates = snapshot
            .templates
            .into_iter()
            .map(|(id, template)| {
                template
                    .into_strategy_template()
                    .map(|t| (id.clone(), t))
                    .with_context(|| format!("Invalid template definition for {}", id))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Self::from_components(
            snapshot.tickers,
            snapshot.unique_dates,
            snapshot.candles,
            candles_by_ticker_indices,
            templates,
            snapshot.ticker_expense_map,
            snapshot.settings,
        )
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create snapshot directory {}", parent.display())
                })?;
            }
        }

        let file = File::create(path).with_context(|| {
            format!(
                "Unable to create market data snapshot at {}",
                path.display()
            )
        })?;
        let mut writer = BufWriter::new(file);
        let settings = scrub_snapshot_settings(self.settings.as_ref());
        let snapshot = MarketDataSnapshot {
            version: MARKET_DATA_SNAPSHOT_VERSION,
            generated_at: Utc::now(),
            tickers: self.tickers.as_ref().clone(),
            unique_dates: self.unique_dates.as_ref().clone(),
            candles: self.all_candles.as_ref().clone(),
            templates: self
                .templates
                .as_ref()
                .iter()
                .map(|(id, template)| {
                    (
                        id.clone(),
                        SnapshotTemplate::from_strategy_template(template),
                    )
                })
                .collect(),
            ticker_expense_map: self.ticker_expense_map.as_ref().clone(),
            settings,
        };
        bincode::serialize_into(&mut writer, &snapshot)
            .context("Failed to serialize market data snapshot")?;
        writer
            .flush()
            .context("Failed to flush market data snapshot to disk")?;
        Ok(())
    }

    fn from_components(
        tickers: Vec<String>,
        unique_dates: Vec<DateTime<Utc>>,
        all_candles: Vec<Candle>,
        candles_by_ticker_indices: HashMap<String, Vec<usize>>,
        templates: HashMap<String, StrategyTemplate>,
        ticker_expense_map: HashMap<String, f64>,
        settings: HashMap<String, String>,
    ) -> Result<Self> {
        if tickers.is_empty() || unique_dates.is_empty() || all_candles.is_empty() {
            return Err(anyhow!(
                "Market data snapshot has no usable tickers or candles"
            ));
        }
        if templates.is_empty() {
            return Err(anyhow!(
                "Market data snapshot has no strategy templates available"
            ));
        }

        Ok(Self {
            all_candles: Arc::new(all_candles),
            unique_dates: Arc::new(unique_dates),
            tickers: Arc::new(tickers),
            candles_by_ticker_indices: Arc::new(candles_by_ticker_indices),
            templates: Arc::new(templates),
            ticker_expense_map: Arc::new(ticker_expense_map),
            settings: Arc::new(settings),
        })
    }

    fn build_candle_index(candles: &[Candle]) -> HashMap<String, Vec<usize>> {
        let mut candles_by_ticker_indices: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, candle) in candles.iter().enumerate() {
            candles_by_ticker_indices
                .entry(candle.ticker.clone())
                .or_default()
                .push(index);
        }
        for indices in candles_by_ticker_indices.values_mut() {
            indices.sort_by_key(|&idx| candles[idx].date);
        }
        candles_by_ticker_indices
    }

    pub fn has_data(&self) -> bool {
        !self.tickers.is_empty() && !self.unique_dates.is_empty()
    }

    pub fn tickers(&self) -> &[String] {
        self.tickers.as_slice()
    }

    pub fn unique_dates(&self) -> &[DateTime<Utc>] {
        self.unique_dates.as_slice()
    }

    pub fn all_candles(&self) -> &[Candle] {
        self.all_candles.as_slice()
    }

    pub fn tickers_arc(&self) -> Arc<Vec<String>> {
        Arc::clone(&self.tickers)
    }

    pub fn ticker_expense_map_arc(&self) -> Arc<HashMap<String, f64>> {
        Arc::clone(&self.ticker_expense_map)
    }

    pub fn settings(&self) -> &HashMap<String, String> {
        self.settings.as_ref()
    }

    pub fn unique_dates_arc(&self) -> Arc<Vec<DateTime<Utc>>> {
        Arc::clone(&self.unique_dates)
    }

    pub fn all_candles_arc(&self) -> Arc<Vec<Candle>> {
        Arc::clone(&self.all_candles)
    }

    pub fn candles_by_ticker(&self) -> HashMap<String, Vec<&Candle>> {
        let all = self.all_candles();
        self.candles_by_ticker_indices
            .iter()
            .map(|(ticker, indices)| {
                let refs = indices.iter().map(|&idx| &all[idx]).collect::<Vec<_>>();
                (ticker.clone(), refs)
            })
            .collect()
    }

    pub fn cloned_candles_by_ticker(&self) -> HashMap<String, Vec<Candle>> {
        let all = self.all_candles();
        self.candles_by_ticker_indices
            .iter()
            .map(|(ticker, indices)| {
                let clones = indices
                    .iter()
                    .map(|&idx| all[idx].clone())
                    .collect::<Vec<_>>();
                (ticker.clone(), clones)
            })
            .collect()
    }

    pub fn candles_by_ticker_indices_arc(&self) -> Arc<HashMap<String, Vec<usize>>> {
        Arc::clone(&self.candles_by_ticker_indices)
    }

    pub fn template(&self, template_id: &str) -> Option<StrategyTemplate> {
        self.templates.get(template_id).cloned()
    }

    pub fn setting_value(&self, setting_key: &str) -> Option<&str> {
        self.settings
            .as_ref()
            .get(setting_key)
            .map(|value| value.as_str())
    }
}

impl MarketData {
    fn rebuild_from_filtered_components(
        tickers: Vec<String>,
        candles: Vec<Candle>,
        templates: Arc<HashMap<String, StrategyTemplate>>,
        ticker_expense_map: HashMap<String, f64>,
        settings: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        if tickers.is_empty() {
            return Err(anyhow!(
                "Ticker scope filtering removed all symbols from the market data snapshot"
            ));
        }
        if candles.is_empty() {
            return Err(anyhow!(
                "No candle data remains after applying market data filters"
            ));
        }

        let mut unique_date_set = BTreeSet::new();
        for candle in &candles {
            unique_date_set.insert(candle.date);
        }

        let unique_dates: Vec<_> = unique_date_set.into_iter().collect();
        if unique_dates.is_empty() {
            return Err(anyhow!(
                "Filtered market data has no trading dates remaining"
            ));
        }

        let candles_by_ticker_indices = Self::build_candle_index(&candles);

        Ok(Self {
            all_candles: Arc::new(candles),
            unique_dates: Arc::new(unique_dates),
            tickers: Arc::new(tickers),
            candles_by_ticker_indices: Arc::new(candles_by_ticker_indices),
            templates,
            ticker_expense_map: Arc::new(ticker_expense_map),
            settings,
        })
    }

    pub fn restrict_to_tickers(self, allowed_tickers: &HashSet<String>) -> Result<Self> {
        if allowed_tickers.is_empty() {
            return Err(anyhow!(
                "Ticker scope filtering requires at least one allowed ticker"
            ));
        }

        let MarketData {
            all_candles,
            tickers,
            templates,
            ticker_expense_map,
            settings,
            ..
        } = self;

        let mut filtered_tickers = Vec::new();
        let mut allowed_intersection: HashSet<String> = HashSet::new();
        for ticker in tickers.as_ref() {
            if allowed_tickers.contains(ticker) {
                allowed_intersection.insert(ticker.clone());
                filtered_tickers.push(ticker.clone());
            }
        }

        if filtered_tickers.is_empty() {
            return Err(anyhow!(
                "Market data snapshot does not contain any tickers that match the requested scope"
            ));
        }

        let filtered_candles: Vec<Candle> = all_candles
            .as_ref()
            .iter()
            .filter(|c| allowed_intersection.contains(&c.ticker))
            .cloned()
            .collect();

        if filtered_candles.is_empty() {
            return Err(anyhow!(
                "No candle data remains after restricting to the requested ticker scope"
            ));
        }

        let filtered_expense_map: HashMap<String, f64> = ticker_expense_map
            .as_ref()
            .iter()
            .filter(|(ticker, _)| allowed_intersection.contains(*ticker))
            .map(|(ticker, value)| (ticker.clone(), *value))
            .collect();

        Self::rebuild_from_filtered_components(
            filtered_tickers,
            filtered_candles,
            templates,
            filtered_expense_map,
            settings,
        )
    }

    pub fn restrict_to_date_range(
        self,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<Self> {
        if start_date.is_none() && end_date.is_none() {
            return Ok(self);
        }

        if let (Some(start), Some(end)) = (start_date, end_date) {
            if start > end {
                return Err(anyhow!(
                    "Invalid market data date range: {} is after {}",
                    start,
                    end
                ));
            }
        }

        let range_description = match (start_date, end_date) {
            (Some(start), Some(end)) => {
                format!("{} - {}", start.format("%Y-%m-%d"), end.format("%Y-%m-%d"))
            }
            (Some(start), None) => format!("{} onward", start.format("%Y-%m-%d")),
            (None, Some(end)) => format!("through {}", end.format("%Y-%m-%d")),
            _ => "entire dataset".to_string(),
        };

        let start_bound = start_date;
        let end_bound = end_date;

        let MarketData {
            all_candles,
            tickers,
            templates,
            ticker_expense_map,
            settings,
            ..
        } = self;

        let filtered_candles: Vec<Candle> = all_candles
            .as_ref()
            .iter()
            .filter(|c| {
                let date = c.date.date_naive();
                if let Some(start) = start_bound {
                    if date < start {
                        return false;
                    }
                }
                if let Some(end) = end_bound {
                    if date > end {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        if filtered_candles.is_empty() {
            return Err(anyhow!(
                "No candle data remains after restricting to {}",
                range_description
            ));
        }

        let mut remaining_ticker_set: HashSet<String> = HashSet::new();
        for candle in &filtered_candles {
            remaining_ticker_set.insert(candle.ticker.clone());
        }

        let filtered_tickers: Vec<String> = tickers
            .as_ref()
            .iter()
            .filter(|ticker| remaining_ticker_set.contains(*ticker))
            .cloned()
            .collect();

        if filtered_tickers.is_empty() {
            return Err(anyhow!(
                "Restricting the market data to {} removed all tickers",
                range_description
            ));
        }

        let filtered_expense_map: HashMap<String, f64> = ticker_expense_map
            .as_ref()
            .iter()
            .filter(|(ticker, _)| remaining_ticker_set.contains(*ticker))
            .map(|(ticker, value)| (ticker.clone(), *value))
            .collect();

        Self::rebuild_from_filtered_components(
            filtered_tickers,
            filtered_candles,
            templates,
            filtered_expense_map,
            settings,
        )
    }
}
