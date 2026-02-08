use crate::app_url::resolve_api_base_url;
use crate::backtester::ActiveStrategyBacktester;
use crate::cache::CacheManager;
use crate::data_context::{MarketData, TickerScope};
use crate::database::Database;
use crate::optimizer::OptimizationEngine;
use crate::optimizer_status::OptimizerStatus;
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
    status: OptimizerStatus,
    backtested_strategy_ids: HashSet<String>,
    ticker_scope: TickerScope,
}

impl EngineContext {
    pub async fn initialize<S: AsRef<str>>(
        database_url: S,
        ticker_scope: TickerScope,
    ) -> Result<Self> {
        let status = OptimizerStatus::new();
        status.set_phase("Connecting to database");
        let db = Database::new(database_url).await?;
        status.set_phase("Loading market data");
        let market_data = MarketData::load(&db, ticker_scope).await?;
        Ok(Self::from_components(
            Some(db),
            market_data,
            status,
            ticker_scope,
        ))
    }

    pub async fn initialize_with_market_data_file<P: AsRef<Path>>(
        database_url: Option<&str>,
        data_file: P,
        ticker_scope: TickerScope,
        filters: Option<MarketDataFilters>,
    ) -> Result<Self> {
        let status = OptimizerStatus::new();
        status.set_phase("Connecting to database");
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
        let mut market_data = MarketData::load_from_file(data_file, &status)?;
        market_data = Self::restrict_snapshot_scope(market_data, ticker_scope, db.as_ref()).await?;
        market_data = Self::apply_market_data_filters(market_data, &filters)?;
        Ok(Self::from_components(db, market_data, status, ticker_scope))
    }

    fn from_components(
        db: Option<Database>,
        market_data: MarketData,
        status: OptimizerStatus,
        ticker_scope: TickerScope,
    ) -> Self {
        let has_db = db.is_some();
        let backtest_secret = market_data.settings().get("BACKTEST_API_SECRET").cloned();
        let api_base_url = resolve_api_base_url(market_data.settings());
        let cache_manager = CacheManager::new(backtest_secret, api_base_url, has_db);
        status.set_phase("Idle");

        Self {
            db,
            cache_manager,
            market_data,
            status,
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
            &self.status,
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
        SignalManager::new(db, &self.status, &self.market_data)
    }

    pub fn status_handle(&self) -> OptimizerStatus {
        self.status.clone()
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
        let allowed: HashSet<String> = ticker_infos
            .into_iter()
            .filter(|info| ticker_scope.allows(info))
            .map(|info| info.symbol.clone())
            .collect();

        let before = market_data.tickers().len();
        let filtered = market_data.restrict_to_tickers(&allowed)?;
        let after = filtered.tickers().len();
        info!(
            "Restricted market data snapshot to {} tickers for {} scope (from {})",
            after,
            ticker_scope.result_label(),
            before
        );
        Ok(filtered)
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
}
