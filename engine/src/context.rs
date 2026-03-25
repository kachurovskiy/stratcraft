use crate::app_url::resolve_api_base_url;
use crate::backtester::ActiveStrategyBacktester;
use crate::cache::CacheManager;
use crate::data_context::{MarketData, TickerScope};
use crate::database::Database;
use crate::models::TickerInfo;
use crate::optimizer::OptimizationEngine;
use crate::signals::SignalManager;
use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use log::{info, warn};
use std::collections::HashSet;
use std::path::Path;

#[derive(Clone)]
pub struct AppContext {
    database_url: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct MarketDataFilters {
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
}

impl MarketDataFilters {
    pub fn is_empty(&self) -> bool {
        self.start_date.is_none() && self.end_date.is_none()
    }
}

impl AppContext {
    pub async fn initialize(database_url: Option<String>) -> Result<Self> {
        Ok(Self { database_url })
    }

    pub async fn database(&self) -> Result<Database> {
        let Some(database_url) = self.database_url.as_deref() else {
            return Err(anyhow!(
                "DATABASE_URL must be set to use database-backed engine commands."
            ));
        };
        Database::new(database_url).await
    }

    pub async fn engine_context_training_tickers(&self) -> Result<EngineContext> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            anyhow!("DATABASE_URL must be set to load market data from the database.")
        })?;
        EngineContext::initialize(database_url, TickerScope::TrainingOnly).await
    }

    pub async fn engine_context_validation_tickers(&self) -> Result<EngineContext> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            anyhow!("DATABASE_URL must be set to load market data from the database.")
        })?;
        EngineContext::initialize(database_url, TickerScope::ValidationOnly).await
    }

    pub async fn engine_context_all_tickers(&self) -> Result<EngineContext> {
        let database_url = self.database_url.as_deref().ok_or_else(|| {
            anyhow!("DATABASE_URL must be set to load market data from the database.")
        })?;
        EngineContext::initialize(database_url, TickerScope::AllTickers).await
    }

    pub async fn engine_context_from_file<P: AsRef<Path>>(
        &self,
        data_file: P,
        ticker_scope: TickerScope,
        filters: Option<MarketDataFilters>,
    ) -> Result<EngineContext> {
        EngineContext::initialize_with_market_data_file(
            self.database_url.as_deref(),
            data_file,
            ticker_scope,
            filters,
        )
        .await
    }
}

pub struct EngineContext {
    db: Option<Database>,
    cache_manager: CacheManager,
    market_data: MarketData,
    backtested_strategy_ids: HashSet<String>,
    ticker_scope: TickerScope,
}

impl EngineContext {
    pub async fn initialize<S: AsRef<str>>(
        database_url: S,
        ticker_scope: TickerScope,
    ) -> Result<Self> {
        let db = Database::new(database_url).await?;
        let market_data = MarketData::load(&db, ticker_scope).await?;
        Ok(Self::from_components(Some(db), market_data, ticker_scope))
    }

    pub async fn initialize_with_market_data_file<P: AsRef<Path>>(
        database_url: Option<&str>,
        data_file: P,
        ticker_scope: TickerScope,
        filters: Option<MarketDataFilters>,
    ) -> Result<Self> {
        let db = match database_url {
            Some(url) if !url.trim().is_empty() => match Database::new(url).await {
                Ok(db) => Some(db),
                Err(error) => {
                    warn!(
                        "Database connection unavailable ({}). Continuing with local market data snapshot only.",
                        error
                    );
                    None
                }
            },
            _ => {
                warn!("Database URL not provided. Using local market data snapshot only.");
                None
            }
        };
        let filters = filters.unwrap_or_default();
        let mut market_data = MarketData::load_from_file(data_file)?;
        market_data = Self::restrict_snapshot_scope(market_data, ticker_scope, db.as_ref()).await?;
        market_data = Self::apply_market_data_filters(market_data, &filters)?;
        Ok(Self::from_components(db, market_data, ticker_scope))
    }

    fn from_components(
        db: Option<Database>,
        market_data: MarketData,
        ticker_scope: TickerScope,
    ) -> Self {
        let has_db = db.is_some();
        let backtest_secret = market_data.settings().get("BACKTEST_API_SECRET").cloned();
        let api_base_url = resolve_api_base_url(market_data.settings());
        let cache_manager = CacheManager::new(backtest_secret, api_base_url, has_db);

        Self {
            db,
            cache_manager,
            market_data,
            backtested_strategy_ids: HashSet::new(),
            ticker_scope,
        }
    }

    pub fn optimizer(&mut self) -> OptimizationEngine<'_> {
        OptimizationEngine::new(self.db.as_mut(), &self.cache_manager, &self.market_data)
    }

    pub fn backtester(&mut self) -> ActiveStrategyBacktester<'_> {
        let db = self
            .db
            .as_mut()
            .expect("ActiveStrategyBacktester requires a database connection");
        ActiveStrategyBacktester::new(
            db,
            &self.market_data,
            &mut self.backtested_strategy_ids,
            self.ticker_scope,
        )
    }

    pub fn signal_manager(&mut self) -> SignalManager<'_> {
        let db = self
            .db
            .as_mut()
            .expect("SignalManager requires a database connection");
        SignalManager::new(db, &self.market_data)
    }

    async fn restrict_snapshot_scope(
        market_data: MarketData,
        ticker_scope: TickerScope,
        db: Option<&Database>,
    ) -> Result<MarketData> {
        if matches!(ticker_scope, TickerScope::AllTickers) {
            return Ok(market_data);
        }

        let Some(db) = db else {
            warn!(
                "Database unavailable; unable to enforce {} ticker scope for snapshot data",
                ticker_scope.result_label()
            );
            return Ok(market_data);
        };

        let ticker_infos = db.get_tickers_with_candle_counts().await?;
        let before = market_data.tickers().len();
        let filtered =
            Self::restrict_snapshot_scope_with_infos(market_data, ticker_scope, &ticker_infos)?;
        let after = filtered.tickers().len();
        info!(
            "Restricted market data snapshot to {} tickers for {} scope (from {})",
            after,
            ticker_scope.result_label(),
            before
        );
        Ok(filtered)
    }

    fn restrict_snapshot_scope_with_infos(
        market_data: MarketData,
        ticker_scope: TickerScope,
        ticker_infos: &[TickerInfo],
    ) -> Result<MarketData> {
        let allowed: HashSet<String> = ticker_infos
            .iter()
            .filter(|info| ticker_scope.allows(info))
            .map(|info| info.symbol.clone())
            .collect();
        market_data.restrict_to_tickers(&allowed)
    }

    fn apply_market_data_filters(
        market_data: MarketData,
        filters: &MarketDataFilters,
    ) -> Result<MarketData> {
        if filters.is_empty() {
            return Ok(market_data);
        }

        let mut filtered = market_data;
        if filters.start_date.is_some() || filters.end_date.is_some() {
            let before_dates = filtered.unique_dates().len();
            let earliest_before = filtered.unique_dates().first().cloned();
            let latest_before = filtered.unique_dates().last().cloned();
            filtered = filtered.restrict_to_date_range(filters.start_date, filters.end_date)?;
            let after_dates = filtered.unique_dates().len();
            let earliest_after = filtered.unique_dates().first().cloned();
            let latest_after = filtered.unique_dates().last().cloned();

            let described_range = match (filters.start_date, filters.end_date) {
                (Some(start), Some(end)) => {
                    format!("{} - {}", start.format("%Y-%m-%d"), end.format("%Y-%m-%d"))
                }
                (Some(start), None) => format!("{} onward", start.format("%Y-%m-%d")),
                (None, Some(end)) => format!("through {}", end.format("%Y-%m-%d")),
                _ => "entire dataset".to_string(),
            };

            info!(
                "Restricted market data snapshot to {} ({} => {}; {} dates -> {})",
                described_range,
                match (earliest_before, latest_before) {
                    (Some(s), Some(e)) => format!("{} - {}", s, e),
                    _ => "n/a".to_string(),
                },
                match (earliest_after, latest_after) {
                    (Some(s), Some(e)) => format!("{} - {}", s, e),
                    _ => "n/a".to_string(),
                },
                before_dates,
                after_dates
            );
        }

        Ok(filtered)
    }

    #[cfg(test)]
    async fn initialize_with_market_data_file_for_test<P: AsRef<Path>>(
        data_file: P,
        ticker_scope: TickerScope,
        filters: Option<MarketDataFilters>,
        ticker_infos: Vec<TickerInfo>,
    ) -> Result<Self> {
        let filters = filters.unwrap_or_default();
        let mut market_data = MarketData::load_from_file(data_file)?;
        if !matches!(ticker_scope, TickerScope::AllTickers) {
            market_data =
                Self::restrict_snapshot_scope_with_infos(market_data, ticker_scope, &ticker_infos)?;
        }
        market_data = Self::apply_market_data_filters(market_data, &filters)?;
        Ok(Self::from_components(None, market_data, ticker_scope))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Candle;
    use anyhow::Context;
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::fs::{self, File};
    use std::io::{BufWriter, Write};
    use std::path::{Path, PathBuf};
    use uuid::Uuid;

    #[derive(Serialize, Deserialize)]
    struct TestMarketDataSnapshot {
        version: u32,
        generated_at: DateTime<Utc>,
        tickers: Vec<String>,
        unique_dates: Vec<DateTime<Utc>>,
        candles: Vec<Candle>,
        #[serde(default)]
        templates: HashMap<String, TestSnapshotTemplate>,
        #[serde(default)]
        ticker_expense_map: HashMap<String, f64>,
        #[serde(default)]
        settings: HashMap<String, String>,
    }

    #[derive(Serialize, Deserialize)]
    struct TestSnapshotTemplate {
        id: String,
        name: String,
        description: Option<String>,
        category: Option<String>,
        author: Option<String>,
        version: Option<String>,
        local_optimization_version: i32,
        example_usage: Option<String>,
        created_at: DateTime<Utc>,
        parameters: Vec<TestSnapshotParameter>,
    }

    #[derive(Serialize, Deserialize)]
    struct TestSnapshotParameter {
        name: String,
        #[serde(rename = "type")]
        param_type: String,
        min: Option<f64>,
        max: Option<f64>,
        step: Option<f64>,
        default_json: Option<String>,
        description: Option<String>,
    }

    fn temp_snapshot_path() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("market_data_snapshot_{}.bin", Uuid::new_v4()));
        path
    }

    fn write_snapshot(path: &Path, snapshot: TestMarketDataSnapshot) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        bincode::serialize_into(&mut writer, &snapshot)
            .context("Failed to serialize test snapshot")?;
        writer.flush().context("Failed to flush test snapshot")?;
        Ok(())
    }

    fn candle(ticker: &str, date: DateTime<Utc>) -> Candle {
        Candle {
            ticker: ticker.to_string(),
            date,
            open: 100.0,
            high: 110.0,
            low: 90.0,
            close: 105.0,
            unadjusted_close: None,
            volume_shares: 1_000,
            disabled: None,
        }
    }

    fn ticker_info(symbol: &str, training: bool) -> TickerInfo {
        TickerInfo {
            symbol: symbol.to_string(),
            name: None,
            tradable: true,
            shortable: false,
            easy_to_borrow: false,
            asset_type: None,
            expense_ratio: None,
            market_cap: None,
            volume_usd: None,
            max_fluctuation_ratio: None,
            last_updated: None,
            candle_count: Some(1),
            training,
        }
    }

    #[tokio::test]
    async fn initialize_with_market_data_file_filters_scope_and_dates() -> Result<()> {
        let date1 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let date2 = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let date3 = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();

        let snapshot = TestMarketDataSnapshot {
            version: crate::data_context::MARKET_DATA_SNAPSHOT_VERSION,
            generated_at: date3,
            tickers: vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()],
            unique_dates: vec![date1, date2, date3],
            candles: vec![
                candle("AAA", date1),
                candle("AAA", date2),
                candle("BBB", date2),
                candle("CCC", date3),
            ],
            templates: HashMap::from([(
                "tmpl-1".to_string(),
                TestSnapshotTemplate {
                    id: "tmpl-1".to_string(),
                    name: "Test Template".to_string(),
                    description: None,
                    category: None,
                    author: None,
                    version: None,
                    local_optimization_version: 1,
                    example_usage: None,
                    created_at: date1,
                    parameters: vec![TestSnapshotParameter {
                        name: "threshold".to_string(),
                        param_type: "number".to_string(),
                        min: None,
                        max: None,
                        step: None,
                        default_json: None,
                        description: None,
                    }],
                },
            )]),
            ticker_expense_map: HashMap::from([
                ("AAA".to_string(), 0.01),
                ("BBB".to_string(), 0.02),
                ("CCC".to_string(), 0.03),
            ]),
            settings: HashMap::from([("DOMAIN".to_string(), "example.test".to_string())]),
        };

        let snapshot_path = temp_snapshot_path();
        write_snapshot(&snapshot_path, snapshot)?;

        let filters = MarketDataFilters {
            start_date: Some(date2.date_naive()),
            end_date: Some(date2.date_naive()),
        };
        let ticker_infos = vec![
            ticker_info("AAA", true),
            ticker_info("BBB", false),
            ticker_info("CCC", true),
        ];

        let context = EngineContext::initialize_with_market_data_file_for_test(
            &snapshot_path,
            TickerScope::TrainingOnly,
            Some(filters),
            ticker_infos,
        )
        .await?;

        let market_data = &context.market_data;
        assert_eq!(market_data.tickers(), &["AAA".to_string()]);
        assert_eq!(market_data.unique_dates(), &[date2]);

        let all_candles = market_data.all_candles();
        assert_eq!(all_candles.len(), 1);
        assert_eq!(all_candles[0].ticker, "AAA");
        assert_eq!(all_candles[0].date, date2);

        let index_map = market_data.candles_by_ticker_indices_arc();
        assert_eq!(index_map.len(), 1);
        assert!(index_map.contains_key("AAA"));
        assert!(!index_map.contains_key("BBB"));
        assert!(!index_map.contains_key("CCC"));

        let indices = index_map.get("AAA").expect("AAA indices should exist");
        assert_eq!(indices.len(), 1);
        assert!(indices.iter().all(|&idx| idx < all_candles.len()));
        assert_eq!(all_candles[indices[0]].ticker, "AAA");
        assert_eq!(all_candles[indices[0]].date, date2);

        let expense_map = market_data.ticker_expense_map_arc();
        assert_eq!(expense_map.len(), 1);
        assert!(expense_map.contains_key("AAA"));
        assert!(!expense_map.contains_key("BBB"));
        assert!(!expense_map.contains_key("CCC"));

        fs::remove_file(&snapshot_path).ok();
        Ok(())
    }
}
