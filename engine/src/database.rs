use crate::models::*;
use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::{DateTime, NaiveDate, Utc};
use hex;
use log::{error, warn};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tokio_postgres::{Client, NoTls, Row};
use uuid::Uuid;

// Limit per-transaction inserts to keep massive signal batches manageable.
const SIGNAL_INSERT_CHUNK_SIZE: usize = 500_000;
const ENCRYPTION_PREFIX: &str = "enc:v1:";
const ENCRYPTION_IV_LENGTH: usize = 12;
const DATABASE_KEY_ENV_VAR: &str = "DATABASE_KEY";

pub struct TradeReconciliationCandidate {
    pub trade: Trade,
    pub account_id: String,
}

pub struct BacktestCacheEntry {
    pub id: String,
    pub template_id: String,
    pub parameters: HashMap<String, f64>,
    pub calmar_ratio: f64,
    pub verify_complete: bool,
    pub balance_training_complete: bool,
    pub balance_validation_complete: bool,
}

pub struct LightgbmModelRecord {
    pub id: String,
    pub name: String,
    pub tree_text: String,
}

pub struct Database {
    client: Client,
}

impl Database {
    pub async fn new<S: AsRef<str>>(database_url: S) -> Result<Self> {
        let database_url = database_url.as_ref().to_string();
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
            .await
            .with_context(|| format!("failed to connect to PostgreSQL at {}", database_url))?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                error!("PostgreSQL connection error: {}", err);
            }
        });

        Ok(Self { client })
    }

    pub async fn get_setting_value(&self, setting_key: &str) -> Result<Option<String>> {
        let row = self
            .client
            .query_opt(
                "SELECT value FROM settings WHERE setting_key = $1",
                &[&setting_key],
            )
            .await?;
        Ok(row.map(|row| row.get::<_, String>(0)))
    }

    pub async fn get_all_settings(&self) -> Result<HashMap<String, String>> {
        let rows = self
            .client
            .query("SELECT setting_key, value FROM settings", &[])
            .await?;
        let mut settings = HashMap::with_capacity(rows.len());
        for row in rows {
            let key: String = row.get(0);
            let raw_value: String = row.get(1);
            let value = decrypt_database_value(&raw_value)
                .with_context(|| format!("failed to decrypt setting {}", key))?;
            settings.insert(key, value);
        }
        Ok(settings)
    }

    pub async fn get_lightgbm_models(&self) -> Result<Vec<LightgbmModelRecord>> {
        let rows = self
            .client
            .query(
                "SELECT id, name, tree_text
                 FROM lightgbm_models
                 ORDER BY updated_at DESC",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| LightgbmModelRecord {
                id: row.get(0),
                name: row.get(1),
                tree_text: row.get(2),
            })
            .collect())
    }

    pub async fn insert_system_log(
        &self,
        source: &str,
        level: &str,
        message: &str,
        metadata: Option<Value>,
    ) -> Result<()> {
        let created_at = Utc::now();
        let metadata_text = metadata.map(|value| value.to_string());

        self.client
            .execute(
                "INSERT INTO system_logs (source, level, message, metadata, created_at)
                 VALUES ($1, $2, $3, $4, $5)",
                &[&source, &level, &message, &metadata_text, &created_at],
            )
            .await?;

        Ok(())
    }

    pub async fn insert_strategy_log(
        &self,
        level: &str,
        strategy_id: &str,
        message: &str,
        metadata: Option<Value>,
    ) -> Result<()> {
        let mut merged = Map::new();
        merged.insert(
            "strategyId".to_string(),
            Value::String(strategy_id.to_string()),
        );

        if let Some(extra) = metadata {
            match extra {
                Value::Object(map) => {
                    for (key, value) in map {
                        merged.insert(key, value);
                    }
                }
                other => {
                    merged.insert("details".to_string(), other);
                }
            }
        }

        self.insert_system_log(
            "StrategyManager",
            level,
            message,
            Some(Value::Object(merged)),
        )
        .await
    }

    pub async fn insert_account_signal_skips(
        &mut self,
        strategy_id: &str,
        account_id: Option<&str>,
        source: &str,
        skips: &[AccountSignalSkip],
    ) -> Result<()> {
        if skips.is_empty() {
            return Ok(());
        }

        let created_at = Utc::now();
        let account_id = account_id.filter(|value| !value.trim().is_empty());
        let tx = self.client.transaction().await?;

        for skip in skips {
            let signal_date = skip.signal_date.date_naive();
            let action = skip.action.as_str();
            tx.execute(
                "INSERT INTO account_signal_skips (strategy_id, account_id, ticker, signal_date, action, source, reason, details, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                &[
                    &strategy_id,
                    &account_id,
                    &skip.ticker,
                    &signal_date,
                    &action,
                    &source,
                    &skip.reason,
                    &skip.details,
                    &created_at,
                ],
            )
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn persist_strategy_event(
        &self,
        strategy_id: &str,
        level: &str,
        message: impl Into<String>,
        metadata: Value,
    ) {
        let text = message.into();
        if let Err(err) = self
            .insert_strategy_log(level, strategy_id, &text, Some(metadata))
            .await
        {
            warn!(
                "Failed to persist strategy log for {}: {}",
                strategy_id, err
            );
        }
    }

    pub async fn get_all_candles(&self) -> Result<Vec<Candle>> {
        let rows = self
            .client
            .query(
                "SELECT ticker, date, open, high, low, close, unadjusted_close, volume_shares
                 FROM candles
                 ORDER BY date, ticker",
                &[],
            )
            .await?;

        let mut candles = Vec::with_capacity(rows.len());
        for row in rows {
            let date: NaiveDate = row.get(1);
            candles.push(Candle {
                ticker: row.get(0),
                date: naive_date_to_datetime(date),
                open: row.get(2),
                high: row.get(3),
                low: row.get(4),
                close: row.get(5),
                unadjusted_close: row.get::<_, Option<f64>>(6),
                volume_shares: row.get(7),
            });
        }

        Ok(candles)
    }

    pub async fn get_candles_for_tickers(&self, symbols: &[String]) -> Result<Vec<Candle>> {
        if symbols.is_empty() {
            return Ok(Vec::new());
        }

        let symbols_param: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
        let rows = self
            .client
            .query(
                "SELECT ticker, date, open, high, low, close, unadjusted_close, volume_shares
                 FROM candles
                 WHERE ticker = ANY($1)
                 ORDER BY date, ticker",
                &[&symbols_param],
            )
            .await?;

        let mut candles = Vec::with_capacity(rows.len());
        for row in rows {
            let date: NaiveDate = row.get(1);
            candles.push(Candle {
                ticker: row.get(0),
                date: naive_date_to_datetime(date),
                open: row.get(2),
                high: row.get(3),
                low: row.get(4),
                close: row.get(5),
                unadjusted_close: row.get::<_, Option<f64>>(6),
                volume_shares: row.get(7),
            });
        }

        Ok(candles)
    }

    pub async fn get_tickers_with_candle_counts(&self) -> Result<Vec<TickerInfo>> {
        let rows = self
            .client
            .query(
                "SELECT t.symbol, t.name, t.tradable, t.shortable, t.easy_to_borrow, t.asset_type, t.expense_ratio, t.market_cap, t.volume_usd, t.max_fluctuation_ratio, t.last_updated, t.training,
                        COUNT(c.id) AS candle_count
                 FROM tickers t
                 LEFT JOIN candles c ON t.symbol = c.ticker
                 GROUP BY t.symbol, t.name, t.tradable, t.shortable, t.easy_to_borrow, t.asset_type, t.expense_ratio, t.market_cap, t.volume_usd, t.max_fluctuation_ratio, t.last_updated, t.training
                 ORDER BY candle_count DESC",
                &[],
            )
            .await?;

        let mut tickers = Vec::with_capacity(rows.len());
        for row in rows {
            tickers.push(TickerInfo {
                symbol: row.get(0),
                name: row.get(1),
                tradable: row.get(2),
                shortable: row.get(3),
                easy_to_borrow: row.get(4),
                asset_type: row.get(5),
                expense_ratio: row.get(6),
                market_cap: row.get(7),
                volume_usd: row.get(8),
                max_fluctuation_ratio: row.get(9),
                last_updated: row.get(10),
                training: row.get(11),
                candle_count: Some(row.get(12)),
            });
        }

        Ok(tickers)
    }

    pub async fn get_ticker_metadata(
        &self,
        symbols: &[String],
    ) -> Result<HashMap<String, TickerInfo>> {
        if symbols.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = self
            .client
            .query(
                "SELECT symbol, name, tradable, shortable, easy_to_borrow, asset_type, expense_ratio, training
                 FROM tickers
                 WHERE symbol = ANY($1)",
                &[&symbols],
            )
            .await?;

        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            let symbol: String = row.get(0);
            map.insert(
                symbol.clone(),
                TickerInfo {
                    symbol,
                    name: row.get(1),
                    tradable: row.get(2),
                    shortable: row.get(3),
                    easy_to_borrow: row.get(4),
                    asset_type: row.get(5),
                    expense_ratio: row.get(6),
                    market_cap: None,
                    volume_usd: None,
                    max_fluctuation_ratio: None,
                    last_updated: None,
                    candle_count: None,
                    training: row.get(7),
                },
            );
        }

        Ok(map)
    }

    pub async fn ensure_ticker_exists(&self, symbol: &str) -> Result<()> {
        self.client
            .execute(
                "INSERT INTO tickers (symbol, tradable, shortable, easy_to_borrow, training)
                 VALUES ($1, false, false, false, false)
                 ON CONFLICT (symbol) DO NOTHING",
                &[&symbol],
            )
            .await?;

        Ok(())
    }

    pub async fn get_template(&self, template_id: &str) -> Result<Option<StrategyTemplate>> {
        let row = self
            .client
            .query_opt(
                "SELECT id, name, description, category, author, version, local_optimization_version, parameters, example_usage, created_at
                 FROM templates
                 WHERE id = $1",
                &[&template_id],
            )
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let parameters_json: String = row.get(7);
        let parameters: Vec<StrategyParameter> = serde_json::from_str(&parameters_json)
            .with_context(|| {
                format!(
                    "Failed to parse parameters JSON for template {}",
                    template_id
                )
            })?;

        Ok(Some(StrategyTemplate {
            id: row.get(0),
            name: row.get(1),
            description: row.get(2),
            category: row.get(3),
            author: row.get(4),
            version: row.get(5),
            local_optimization_version: row.get::<_, i32>(6),
            parameters,
            example_usage: row.get(8),
            created_at: row.get(9),
        }))
    }

    pub async fn get_all_templates(&self) -> Result<Vec<StrategyTemplate>> {
        let rows = self
            .client
            .query(
                "SELECT id, name, description, category, author, version, local_optimization_version, parameters, example_usage, created_at
                 FROM templates",
                &[],
            )
            .await?;

        let mut templates = Vec::with_capacity(rows.len());
        for row in rows {
            let parameters_json: String = row.get(7);
            let parameters: Vec<StrategyParameter> = serde_json::from_str(&parameters_json)
                .with_context(|| {
                    let template_id: String = row.get(0);
                    format!(
                        "Failed to parse parameters JSON for template {}",
                        template_id
                    )
                })?;

            templates.push(StrategyTemplate {
                id: row.get(0),
                name: row.get(1),
                description: row.get(2),
                category: row.get(3),
                author: row.get(4),
                version: row.get(5),
                local_optimization_version: row.get::<_, i32>(6),
                parameters,
                example_usage: row.get(8),
                created_at: row.get(9),
            });
        }

        Ok(templates)
    }

    pub async fn update_template_local_optimization_version(
        &self,
        template_id: &str,
        version: i32,
    ) -> Result<()> {
        self.client
            .execute(
                "UPDATE templates SET local_optimization_version = $1 WHERE id = $2",
                &[&version, &template_id],
            )
            .await?;
        Ok(())
    }

    /// Delete a strategy and any related persisted data (signals, account operations,
    /// trades and backtest results). This is used to remove server-created default
    /// strategies (for example `default_<template_id>`) so they can be recreated
    /// with updated parameters on next server/registry startup.
    pub async fn delete_strategy_and_related(&mut self, strategy_id: &str) -> Result<()> {
        let tx = self.client.transaction().await?;

        tx.execute(
            "DELETE FROM signals WHERE strategy_id = $1",
            &[&strategy_id],
        )
        .await?;

        tx.execute(
            "DELETE FROM account_operations WHERE strategy_id = $1",
            &[&strategy_id],
        )
        .await?;

        tx.execute(
            "DELETE FROM account_signal_skips WHERE strategy_id = $1",
            &[&strategy_id],
        )
        .await?;

        tx.execute("DELETE FROM trades WHERE strategy_id = $1", &[&strategy_id])
            .await?;

        tx.execute(
            "DELETE FROM backtest_results WHERE strategy_id = $1",
            &[&strategy_id],
        )
        .await?;

        tx.execute("DELETE FROM strategies WHERE id = $1", &[&strategy_id])
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_active_strategies(&self) -> Result<Vec<StrategyConfig>> {
        let rows = self
            .client
            .query(
                "SELECT
                    s.id,
                    s.name,
                    s.template_id,
                    s.account_id,
                    s.parameters,
                    s.backtest_start_date,
                    COALESCE(a.excluded_tickers, '[]') AS excluded_tickers,
                    COALESCE(a.excluded_keywords, '[]') AS excluded_keywords
                 FROM strategies s
                 LEFT JOIN accounts a ON s.account_id = a.id
                 WHERE s.status = 'active'
                 ORDER BY CASE WHEN s.last_backtest_duration_minutes IS NULL THEN 1 ELSE 0 END,
                          s.last_backtest_duration_minutes ASC",
                &[],
            )
            .await?;

        let mut strategies = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.get(0);
            let params_json: String = row.get(4);
            let parameters = parse_parameter_map_from_json(&params_json)
                .with_context(|| format!("Failed to parse parameters for strategy {}", id))?;
            let excluded_tickers_json: String = row.get(6);
            let excluded_keywords_json: String = row.get(7);
            let excluded_tickers = parse_excluded_tickers(&excluded_tickers_json);
            let excluded_keywords = parse_excluded_keywords(&excluded_keywords_json);

            strategies.push(StrategyConfig {
                id,
                name: row.get(1),
                template_id: row.get(2),
                account_id: row.get(3),
                excluded_tickers,
                excluded_keywords,
                parameters,
                backtest_start_date: row.get(5),
            });
        }

        Ok(strategies)
    }

    pub async fn get_strategy_config(&self, strategy_id: &str) -> Result<Option<StrategyConfig>> {
        let row = self
            .client
            .query_opt(
                "SELECT
                    s.id,
                    s.name,
                    s.template_id,
                    s.account_id,
                    s.parameters,
                    s.backtest_start_date
                 FROM strategies s
                 WHERE s.id = $1",
                &[&strategy_id],
            )
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let params_json: String = row.get(4);
        let parameters = parse_parameter_map_from_json(&params_json)
            .with_context(|| format!("Failed to parse parameters for strategy {}", strategy_id))?;

        Ok(Some(StrategyConfig {
            id: row.get(0),
            name: row.get(1),
            template_id: row.get(2),
            account_id: row.get(3),
            excluded_tickers: Vec::new(),
            excluded_keywords: Vec::new(),
            parameters,
            backtest_start_date: row.get(5),
        }))
    }

    pub async fn get_account_credentials(
        &self,
        account_id: &str,
    ) -> Result<Option<AccountCredentials>> {
        let row = self
            .client
            .query_opt(
                "SELECT id, provider, environment, api_key, api_secret
                 FROM accounts
                 WHERE id = $1",
                &[&account_id],
            )
            .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let id: String = row.get(0);
        let api_key: String = row.get(3);
        let api_secret: String = row.get(4);

        Ok(Some(AccountCredentials {
            id: id.clone(),
            provider: row.get(1),
            environment: row.get(2),
            api_key: decrypt_database_value(&api_key)
                .with_context(|| format!("failed to decrypt api_key for account {}", id))?,
            api_secret: decrypt_database_value(&api_secret)
                .with_context(|| format!("failed to decrypt api_secret for account {}", id))?,
        }))
    }

    pub async fn update_strategy_backtest_duration(
        &self,
        strategy_id: &str,
        duration_minutes: f64,
    ) -> Result<()> {
        self.client
            .execute(
                "UPDATE strategies
                 SET last_backtest_duration_minutes = $1, updated_at = CURRENT_TIMESTAMP
                 WHERE id = $2",
                &[&duration_minutes, &strategy_id],
            )
            .await?;
        Ok(())
    }

    pub async fn get_latest_backtest_end_date(
        &self,
        strategy_id: &str,
        months_filter: Option<i64>,
        ticker_scope: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let months_filter = months_filter.map(clamp_i64_to_i32);
        let row = if let Some(months) = months_filter {
            self.client
                .query_opt(
                    "SELECT end_date
                     FROM backtest_results
                     WHERE strategy_id = $1 AND period_months = $2 AND ticker_scope = $3
                     ORDER BY end_date DESC
                     LIMIT 1",
                    &[&strategy_id, &months, &ticker_scope],
                )
                .await?
        } else {
            self.client
                .query_opt(
                    "SELECT end_date
                     FROM backtest_results
                     WHERE strategy_id = $1 AND ticker_scope = $2
                     ORDER BY end_date DESC
                     LIMIT 1",
                    &[&strategy_id, &ticker_scope],
                )
                .await?
        };

        Ok(row.map(|row| row.get(0)))
    }

    pub async fn load_latest_backtest_result(
        &self,
        strategy_id: &str,
        months_filter: Option<i64>,
        ticker_scope: &str,
    ) -> Result<Option<BacktestResult>> {
        let months_filter_i32 = months_filter.map(clamp_i64_to_i32);
        let row = if let Some(months) = months_filter_i32 {
            self.client
                .query_opt(
                    "SELECT id, start_date, end_date, initial_capital, final_portfolio_value, performance, daily_snapshots, tickers, ticker_scope, strategy_state, created_at
                     FROM backtest_results
                     WHERE strategy_id = $1 AND period_months = $2 AND ticker_scope = $3
                     ORDER BY end_date DESC
                     LIMIT 1",
                    &[&strategy_id, &months, &ticker_scope],
                )
                .await?
        } else {
            self.client
                .query_opt(
                    "SELECT id, start_date, end_date, initial_capital, final_portfolio_value, performance, daily_snapshots, tickers, ticker_scope, strategy_state, created_at
                     FROM backtest_results
                     WHERE strategy_id = $1 AND ticker_scope = $2
                     ORDER BY end_date DESC
                     LIMIT 1",
                    &[&strategy_id, &ticker_scope],
                )
                .await?
        };

        let Some(row) = row else {
            return Ok(None);
        };

        let backtest_id: String = row.get(0);
        let performance_json: String = row.get(5);
        let snapshots_json: String = row.get(6);
        let tickers_json: String = row.get(7);
        let scope_label: String = row.get(8);
        let strategy_state_json: Option<String> = row.get(9);

        let performance = deserialize_performance(&performance_json)?;
        let daily_snapshots = deserialize_snapshots(&snapshots_json)?;
        let tickers: Vec<String> = serde_json::from_str(&tickers_json)
            .map_err(|err| anyhow!("Failed to parse tickers JSON: {}", err))?;
        let strategy_state = strategy_state_json
            .map(|raw| {
                serde_json::from_str(&raw)
                    .map_err(|err| anyhow!("Failed to parse strategy state JSON: {}", err))
            })
            .transpose()?;
        let trades = self
            .load_trades_for_backtest(&backtest_id, strategy_id)
            .await?;

        Ok(Some(BacktestResult {
            id: backtest_id,
            strategy_id: strategy_id.to_string(),
            start_date: row.get(1),
            end_date: row.get(2),
            initial_capital: row.get(3),
            final_portfolio_value: row.get(4),
            performance,
            daily_snapshots,
            trades,
            tickers,
            ticker_scope: Some(scope_label),
            strategy_state,
            created_at: row.get(10),
        }))
    }

    pub async fn replace_strategy_backtest_data(
        &mut self,
        strategy_id: &str,
        result: &BacktestResult,
        months_filter: Option<i64>,
        ticker_scope: &str,
    ) -> Result<()> {
        let performance_json = serialize_performance(&result.performance)?;
        let snapshots_json = serialize_snapshots(&result.daily_snapshots)?;
        let tickers_json = serde_json::to_string(&result.tickers)?;
        let period_days = calculate_period_days(&result.start_date, &result.end_date);
        let target_months = months_filter;
        let period_months = target_months.unwrap_or_else(|| calculate_period_months(period_days));
        let strategy_state_json = result
            .strategy_state
            .as_ref()
            .map(|snapshot| serde_json::to_string(snapshot))
            .transpose()?;

        let tx = self.client.transaction().await?;
        if ticker_scope.eq_ignore_ascii_case("live") {
            tx.execute(
                "INSERT INTO backtest_results (id, strategy_id, start_date, end_date, period_days, period_months, initial_capital, final_portfolio_value, performance, daily_snapshots, tickers, ticker_scope, strategy_state)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                 ON CONFLICT (id) DO UPDATE SET
                    strategy_id = EXCLUDED.strategy_id,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    period_days = EXCLUDED.period_days,
                    period_months = EXCLUDED.period_months,
                    initial_capital = EXCLUDED.initial_capital,
                    final_portfolio_value = EXCLUDED.final_portfolio_value,
                    performance = EXCLUDED.performance,
                    daily_snapshots = EXCLUDED.daily_snapshots,
                    tickers = EXCLUDED.tickers,
                    ticker_scope = EXCLUDED.ticker_scope,
                    strategy_state = EXCLUDED.strategy_state,
                    created_at = CURRENT_TIMESTAMP",
                &[
                    &result.id,
                    &strategy_id,
                    &result.start_date,
                    &result.end_date,
                    &clamp_i64_to_i32(period_days),
                    &clamp_i64_to_i32(period_months),
                    &result.initial_capital,
                    &result.final_portfolio_value,
                    &performance_json,
                    &snapshots_json,
                    &tickers_json,
                    &ticker_scope,
                    &strategy_state_json,
                ],
            )
            .await?;

            tx.commit().await?;
            return Ok(());
        }

        let target_months_i32 = target_months.map(clamp_i64_to_i32);
        let existing_rows = if let Some(target_months) = target_months_i32 {
            tx.query(
                "SELECT id
                 FROM backtest_results
                 WHERE strategy_id = $1 AND period_months = $2 AND ticker_scope = $3",
                &[&strategy_id, &target_months, &ticker_scope],
            )
            .await?
        } else {
            tx.query(
                "SELECT id
                 FROM backtest_results
                 WHERE strategy_id = $1 AND ticker_scope = $2",
                &[&strategy_id, &ticker_scope],
            )
            .await?
        };

        for row in existing_rows {
            let existing_id: String = row.get(0);
            tx.execute(
                "DELETE FROM trades WHERE backtest_result_id = $1",
                &[&existing_id],
            )
            .await?;
        }

        if let Some(target_months) = target_months_i32 {
            tx.execute(
                "DELETE FROM backtest_results WHERE strategy_id = $1 AND period_months = $2 AND ticker_scope = $3",
                &[&strategy_id, &target_months, &ticker_scope],
            )
            .await?;
        } else {
            tx.execute(
                "DELETE FROM backtest_results WHERE strategy_id = $1 AND ticker_scope = $2",
                &[&strategy_id, &ticker_scope],
            )
            .await?;
        }

        tx.execute(
            "INSERT INTO backtest_results (id, strategy_id, start_date, end_date, period_days, period_months, initial_capital, final_portfolio_value, performance, daily_snapshots, tickers, ticker_scope, strategy_state)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            &[
                &result.id,
                &strategy_id,
                &result.start_date,
                &result.end_date,
                &clamp_i64_to_i32(period_days),
                &clamp_i64_to_i32(period_months),
                &result.initial_capital,
                &result.final_portfolio_value,
                &performance_json,
                &snapshots_json,
                &tickers_json,
                &ticker_scope,
                &strategy_state_json,
            ],
        )
        .await?;

        if !result.trades.is_empty() {
            let stmt = tx
                .prepare(
                    "INSERT INTO trades (id, strategy_id, backtest_result_id, ticker, quantity, price, date, status, pnl, fee, exit_price, exit_date, stop_loss, stop_loss_triggered, changes)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                )
                .await?;

            for trade in &result.trades {
                let trade_date = trade.date.date_naive();
                let exit_date = trade.exit_date.map(|d| d.date_naive());
                let changes_json =
                    serde_json::to_string(&trade.changes).context("Failed to serialize trades")?;
                let fee_value = trade.fee.unwrap_or(0.0);

                tx.execute(
                    &stmt,
                    &[
                        &trade.id,
                        &strategy_id,
                        &result.id,
                        &trade.ticker,
                        &trade.quantity,
                        &trade.price,
                        &trade_date,
                        &trade.status.as_str(),
                        &trade.pnl,
                        &fee_value,
                        &trade.exit_price,
                        &exit_date,
                        &trade.stop_loss,
                        &trade.stop_loss_triggered.unwrap_or(false),
                        &changes_json,
                    ],
                )
                .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn link_live_trades_to_backtest(
        &self,
        strategy_id: &str,
        backtest_result_id: &str,
    ) -> Result<()> {
        self.client
            .execute(
                "UPDATE trades
                 SET backtest_result_id = $1
                 WHERE strategy_id = $2
                   AND entry_order_id IS NOT NULL",
                &[&backtest_result_id, &strategy_id],
            )
            .await?;

        Ok(())
    }

    pub async fn replace_account_operations_for_strategy(
        &mut self,
        account_id: &str,
        strategy_id: &str,
        operations: &[AccountOperationPlan],
    ) -> Result<()> {
        let tx = self.client.transaction().await?;
        tx.execute(
            "DELETE FROM account_operations
             WHERE strategy_id = $1
               AND status IN ('pending', 'approved', 'failed', 'ignored')",
            &[&strategy_id],
        )
        .await?;

        if !operations.is_empty() {
            let stmt = tx
                .prepare(
                    "INSERT INTO account_operations
                     (id, account_id, strategy_id, trade_id, ticker, operation_type, quantity, price, stop_loss, previous_stop_loss, triggered_at, reason, order_type, discount_applied, signal_confidence, account_cash_at_plan, days_held)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
                )
                .await?;

            for op in operations {
                let op_id = Uuid::new_v4().to_string();

                tx.execute(
                    &stmt,
                    &[
                        &op_id,
                        &account_id,
                        &strategy_id,
                        &op.trade_id,
                        &op.ticker,
                        &op.operation_type.as_str(),
                        &op.quantity,
                        &op.price,
                        &op.stop_loss,
                        &op.previous_stop_loss,
                        &op.triggered_at,
                        &op.reason,
                        &op.order_type,
                        &op.discount_applied,
                        &op.signal_confidence,
                        &op.account_cash_at_plan,
                        &op.days_held,
                    ],
                )
                .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_strategy_live_trades(&self, strategy_id: &str) -> Result<Vec<Trade>> {
        let rows = self
            .client
            .query(
                "SELECT id, ticker, quantity, price, date, status, pnl, fee, exit_price, exit_date, stop_loss, stop_loss_triggered, changes, entry_order_id, entry_cancel_after, stop_order_id, exit_order_id
                 FROM trades t
                 WHERE t.strategy_id = $1
                   AND t.entry_order_id IS NOT NULL
                 ORDER BY t.date, t.id",
                &[&strategy_id],
            )
            .await?;

        let mut trades = Vec::with_capacity(rows.len());
        for row in rows {
            trades.push(Self::map_trade_row(&row, strategy_id)?);
        }
        Ok(trades)
    }

    pub async fn get_strategy_first_filled_trade_date(
        &self,
        strategy_id: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let row = self
            .client
            .query_one(
                "SELECT MIN(date)
                 FROM trades
                 WHERE strategy_id = $1
                   AND entry_order_id IS NOT NULL
                   AND status IN ('active', 'closed')",
                &[&strategy_id],
            )
            .await?;

        let date: Option<NaiveDate> = row.get(0);
        Ok(date.map(naive_date_to_datetime))
    }

    pub async fn get_live_trades_with_accounts(&self) -> Result<Vec<TradeReconciliationCandidate>> {
        let rows = self
            .client
            .query(
                "SELECT t.id, t.ticker, t.quantity, t.price, t.date, t.status, t.pnl, t.fee, t.exit_price, t.exit_date, t.stop_loss, t.stop_loss_triggered, t.changes, t.entry_order_id, t.entry_cancel_after, t.stop_order_id, t.exit_order_id, s.account_id, t.strategy_id
                 FROM trades t
                 INNER JOIN strategies s ON s.id = t.strategy_id
                 WHERE s.account_id IS NOT NULL
                   AND t.status IN ('pending', 'active')
                   AND t.entry_order_id IS NOT NULL
                 ORDER BY t.date, t.id",
                &[],
            )
            .await?;

        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let account_id: String = row.get(17);
            if account_id.trim().is_empty() {
                continue;
            }
            let strategy_id: String = row.get(18);
            let trade = Self::map_trade_row(&row, &strategy_id)?;
            result.push(TradeReconciliationCandidate { trade, account_id });
        }
        Ok(result)
    }

    pub async fn get_latest_account_operation_date(
        &self,
        strategy_id: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let row = self
            .client
            .query_opt(
                "SELECT triggered_at
                 FROM account_operations
                 WHERE strategy_id = $1
                 ORDER BY triggered_at DESC
                 LIMIT 1",
                &[&strategy_id],
            )
            .await?;

        Ok(row.map(|row| row.get(0)))
    }

    pub async fn count_buy_operations_for_day(
        &self,
        strategy_id: &str,
        target_date: DateTime<Utc>,
    ) -> Result<i64> {
        let op_type = AccountOperationType::OpenPosition.as_str();
        let date_only = target_date.date_naive();
        let row = self
            .client
            .query_one(
                "SELECT COUNT(*)
                 FROM account_operations
                 WHERE strategy_id = $1
                   AND operation_type = $2
                   AND triggered_at::date = $3",
                &[&strategy_id, &op_type, &date_only],
            )
            .await?;

        Ok(row.get::<_, i64>(0))
    }

    pub async fn get_signals_for_strategy_in_range(
        &self,
        strategy_id: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<Vec<GeneratedSignal>> {
        if start_date > end_date {
            return Ok(Vec::new());
        }

        let start = start_date.date_naive();
        let end = end_date.date_naive();

        let rows = self
            .client
            .query(
                "SELECT date, ticker, action, confidence
                 FROM signals
                 WHERE strategy_id = $1
                   AND date BETWEEN $2 AND $3
                 ORDER BY date, ticker",
                &[&strategy_id, &start, &end],
            )
            .await?;

        let mut signals = Vec::with_capacity(rows.len());
        for row in rows {
            let date: NaiveDate = row.get(0);
            let action_str: String = row.get(2);
            let action = SignalAction::from_str(&action_str).map_err(|_| {
                anyhow!(
                    "Invalid action '{}' retrieved for strategy {}",
                    action_str,
                    strategy_id
                )
            })?;

            signals.push(GeneratedSignal {
                date: naive_date_to_datetime(date),
                ticker: row.get(1),
                action,
                confidence: row.get(3),
            });
        }

        Ok(signals)
    }

    pub async fn get_latest_signal_date(&self, strategy_id: &str) -> Result<Option<DateTime<Utc>>> {
        let row = self
            .client
            .query_one(
                "SELECT MAX(date) FROM signals WHERE strategy_id = $1",
                &[&strategy_id],
            )
            .await?;

        let latest: Option<NaiveDate> = row.get(0);
        Ok(latest.map(naive_date_to_datetime))
    }

    pub async fn upsert_strategy_signals(
        &mut self,
        strategy_id: &str,
        signals: &[GeneratedSignal],
    ) -> Result<usize> {
        if signals.is_empty() {
            return Ok(0);
        }

        let user_id: Option<i64> = self
            .client
            .query_opt(
                "SELECT user_id FROM strategies WHERE id = $1",
                &[&strategy_id],
            )
            .await?
            .and_then(|row| row.get(0));

        let mut inserted = 0usize;
        for chunk in signals.chunks(SIGNAL_INSERT_CHUNK_SIZE) {
            let tx = self.client.transaction().await?;
            let stmt = tx
                .prepare(
                    "INSERT INTO signals (id, date, ticker, strategy_id, user_id, action, confidence)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (id) DO UPDATE
                     SET date = EXCLUDED.date,
                         ticker = EXCLUDED.ticker,
                         strategy_id = EXCLUDED.strategy_id,
                         user_id = EXCLUDED.user_id,
                         action = EXCLUDED.action,
                         confidence = EXCLUDED.confidence",
                )
                .await?;

            for signal in chunk {
                let signal_id = generate_signal_id(strategy_id, &signal.ticker, signal.date);
                let signal_date = signal.date.date_naive();
                let changed = tx
                    .execute(
                        &stmt,
                        &[
                            &signal_id,
                            &signal_date,
                            &signal.ticker,
                            &strategy_id,
                            &user_id,
                            &signal.action.as_str(),
                            &signal.confidence,
                        ],
                    )
                    .await?;
                if changed > 0 {
                    inserted += 1;
                }
            }

            tx.commit().await?;
        }

        Ok(inserted)
    }

    async fn load_trades_for_backtest(
        &self,
        backtest_id: &str,
        strategy_id: &str,
    ) -> Result<Vec<Trade>> {
        let rows = self
            .client
            .query(
                "SELECT id, ticker, quantity, price, date, status, pnl, fee, exit_price, exit_date, stop_loss, stop_loss_triggered, changes, entry_order_id, entry_cancel_after, stop_order_id, exit_order_id
                 FROM trades
                 WHERE backtest_result_id = $1
                 ORDER BY date, id",
                &[&backtest_id],
            )
            .await?;

        let mut trades = Vec::with_capacity(rows.len());
        for row in rows {
            trades.push(Self::map_trade_row(&row, strategy_id)?);
        }

        Ok(trades)
    }

    pub async fn persist_trade_reconciliation(&self, trade: &Trade) -> Result<()> {
        let trade_date = trade.date.date_naive();
        let exit_date = trade.exit_date.map(|date| date.date_naive());
        let stop_loss_triggered = trade.stop_loss_triggered.unwrap_or(false);
        let changes_json = serde_json::to_string(&trade.changes)
            .map_err(|err| anyhow!("Failed to serialize trade changes: {}", err))?;
        let fee_value = trade.fee.unwrap_or(0.0);
        let status = trade.status.as_str();

        self.client
            .execute(
                "UPDATE trades
                 SET status = $1,
                     pnl = $2,
                     fee = $3,
                     exit_price = $4,
                     exit_date = $5,
                     stop_loss_triggered = $6,
                     changes = $7,
                     price = $8,
                     date = $9,
                     ticker = $10,
                     stop_order_id = $11
                 WHERE id = $12",
                &[
                    &status,
                    &trade.pnl,
                    &fee_value,
                    &trade.exit_price,
                    &exit_date,
                    &stop_loss_triggered,
                    &changes_json,
                    &trade.price,
                    &trade_date,
                    &trade.ticker,
                    &trade.stop_order_id,
                    &trade.id,
                ],
            )
            .await?;

        Ok(())
    }

    pub async fn backtest_cache_entries_for_template(
        &self,
        template_id: &str,
    ) -> Result<Vec<BacktestCacheEntry>> {
        let rows = self
            .client
            .query(
                "SELECT id,
                        template_id,
                        parameters,
                        calmar_ratio,
                        (verify_sharpe_ratio IS NOT NULL
                         AND verify_calmar_ratio IS NOT NULL
                         AND verify_cagr IS NOT NULL
                         AND verify_max_drawdown_ratio IS NOT NULL) AS verify_complete,
                        (balance_training_sharpe_ratio IS NOT NULL
                         AND balance_training_calmar_ratio IS NOT NULL
                         AND balance_training_cagr IS NOT NULL
                         AND balance_training_max_drawdown_ratio IS NOT NULL) AS balance_training_complete,
                        (balance_validation_sharpe_ratio IS NOT NULL
                         AND balance_validation_calmar_ratio IS NOT NULL
                         AND balance_validation_cagr IS NOT NULL
                         AND balance_validation_max_drawdown_ratio IS NOT NULL) AS balance_validation_complete
                 FROM backtest_cache
                 WHERE template_id = $1
                 ORDER BY created_at DESC",
                &[&template_id],
            )
            .await?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.get("id");
            let params_text: String = row.get("parameters");
            match parse_parameter_map_from_json(&params_text) {
                Ok(parameters) => entries.push(BacktestCacheEntry {
                    id,
                    template_id: row.get("template_id"),
                    parameters,
                    calmar_ratio: row.get("calmar_ratio"),
                    verify_complete: row.get("verify_complete"),
                    balance_training_complete: row.get("balance_training_complete"),
                    balance_validation_complete: row.get("balance_validation_complete"),
                }),
                Err(error) => warn!(
                    "Skipping cached parameters {} for template {} due to parse error: {}",
                    id, template_id, error
                ),
            }
        }

        Ok(entries)
    }

    pub async fn update_backtest_cache_verification(
        &self,
        cache_id: &str,
        sharpe_ratio: Option<f64>,
        calmar_ratio: Option<f64>,
        cagr: Option<f64>,
        max_drawdown_ratio: Option<f64>,
    ) -> Result<()> {
        let normalize_metric = |value: Option<f64>| -> Option<f64> {
            value.and_then(|v| if v.is_finite() { Some(v) } else { None })
        };

        let normalized_sharpe = normalize_metric(sharpe_ratio);
        let normalized_calmar = normalize_metric(calmar_ratio);
        let normalized_cagr = normalize_metric(cagr);
        let normalized_dd_ratio = normalize_metric(max_drawdown_ratio);

        self.client
            .execute(
                "UPDATE backtest_cache
                 SET verify_sharpe_ratio = $1,
                     verify_calmar_ratio = $2,
                     verify_cagr = $3,
                     verify_max_drawdown_ratio = $4
                 WHERE id = $5",
                &[
                    &normalized_sharpe,
                    &normalized_calmar,
                    &normalized_cagr,
                    &normalized_dd_ratio,
                    &cache_id,
                ],
            )
            .await?;

        Ok(())
    }

    pub async fn update_backtest_cache_balance_training(
        &self,
        cache_id: &str,
        sharpe_ratio: Option<f64>,
        calmar_ratio: Option<f64>,
        cagr: Option<f64>,
        max_drawdown_ratio: Option<f64>,
    ) -> Result<()> {
        let normalize_metric = |value: Option<f64>| -> Option<f64> {
            value.and_then(|v| if v.is_finite() { Some(v) } else { None })
        };

        let normalized_sharpe = normalize_metric(sharpe_ratio);
        let normalized_calmar = normalize_metric(calmar_ratio);
        let normalized_cagr = normalize_metric(cagr);
        let normalized_dd_ratio = normalize_metric(max_drawdown_ratio);

        self.client
            .execute(
                "UPDATE backtest_cache
                 SET balance_training_sharpe_ratio = $1,
                     balance_training_calmar_ratio = $2,
                     balance_training_cagr = $3,
                     balance_training_max_drawdown_ratio = $4
                 WHERE id = $5",
                &[
                    &normalized_sharpe,
                    &normalized_calmar,
                    &normalized_cagr,
                    &normalized_dd_ratio,
                    &cache_id,
                ],
            )
            .await?;

        Ok(())
    }

    pub async fn update_backtest_cache_balance_validation(
        &self,
        cache_id: &str,
        sharpe_ratio: Option<f64>,
        calmar_ratio: Option<f64>,
        cagr: Option<f64>,
        max_drawdown_ratio: Option<f64>,
    ) -> Result<()> {
        let normalize_metric = |value: Option<f64>| -> Option<f64> {
            value.and_then(|v| if v.is_finite() { Some(v) } else { None })
        };

        let normalized_sharpe = normalize_metric(sharpe_ratio);
        let normalized_calmar = normalize_metric(calmar_ratio);
        let normalized_cagr = normalize_metric(cagr);
        let normalized_dd_ratio = normalize_metric(max_drawdown_ratio);

        self.client
            .execute(
                "UPDATE backtest_cache
                 SET balance_validation_sharpe_ratio = $1,
                     balance_validation_calmar_ratio = $2,
                     balance_validation_cagr = $3,
                     balance_validation_max_drawdown_ratio = $4
                 WHERE id = $5",
                &[
                    &normalized_sharpe,
                    &normalized_calmar,
                    &normalized_cagr,
                    &normalized_dd_ratio,
                    &cache_id,
                ],
            )
            .await?;

        Ok(())
    }

    fn map_trade_row(row: &Row, strategy_id: &str) -> Result<Trade> {
        let status_str: String = row.get(5);
        let status = parse_trade_status(&status_str)?;
        let trade_date: NaiveDate = row.get(4);
        let exit_date: Option<NaiveDate> = row.get(9);
        let changes_json: String = row.get(12);
        let entry_order_id: Option<String> = row.get(13);
        let entry_cancel_after: Option<DateTime<Utc>> = row.get(14);
        let stop_order_id: Option<String> = row.get(15);
        let exit_order_id: Option<String> = row.get(16);
        let changes: Vec<TradeChange> = serde_json::from_str(&changes_json)
            .map_err(|err| anyhow!("Failed to parse trade changes JSON: {}", err))?;
        let fee_value: Option<f64> = row.get(7);

        Ok(Trade {
            id: row.get(0),
            strategy_id: strategy_id.to_string(),
            ticker: row.get(1),
            quantity: row.get(2),
            price: row.get(3),
            date: naive_date_to_datetime(trade_date),
            status,
            pnl: row.get(6),
            fee: Some(fee_value.unwrap_or(0.0)),
            exit_price: row.get(8),
            exit_date: exit_date.map(naive_date_to_datetime),
            stop_loss: row.get(10),
            stop_loss_triggered: row.get(11),
            entry_order_id,
            entry_cancel_after,
            stop_order_id,
            exit_order_id,
            changes,
        })
    }
}

fn parse_excluded_tickers(json: &str) -> Vec<String> {
    let parsed: Vec<String> = serde_json::from_str(json).unwrap_or_default();
    let mut seen = HashSet::new();
    let mut cleaned = Vec::with_capacity(parsed.len());
    for ticker in parsed {
        let normalized = ticker.trim().to_ascii_uppercase();
        if normalized.is_empty() {
            continue;
        }
        if seen.insert(normalized.clone()) {
            cleaned.push(normalized);
        }
    }
    cleaned
}

fn parse_excluded_keywords(json: &str) -> Vec<String> {
    let parsed: Vec<String> = serde_json::from_str(json).unwrap_or_default();
    let mut seen = HashSet::new();
    let mut cleaned = Vec::with_capacity(parsed.len());
    for keyword in parsed {
        let normalized = keyword.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if seen.insert(normalized.clone()) {
            cleaned.push(normalized);
        }
    }
    cleaned
}

fn decrypt_database_value(value: &str) -> Result<String> {
    if value.is_empty() || !value.starts_with(ENCRYPTION_PREFIX) {
        return Ok(value.to_string());
    }

    let key = load_database_key()?;
    let payload = value
        .strip_prefix(ENCRYPTION_PREFIX)
        .ok_or_else(|| anyhow!("Encrypted value has an invalid format."))?;
    let parts: Vec<&str> = payload.split(':').collect();
    if parts.len() != 3 {
        return Err(anyhow!("Encrypted value has an invalid format."));
    }

    let iv = BASE64_STANDARD
        .decode(parts[0])
        .map_err(|_| anyhow!("Encrypted value payload is invalid."))?;
    let data = BASE64_STANDARD
        .decode(parts[1])
        .map_err(|_| anyhow!("Encrypted value payload is invalid."))?;
    let tag = BASE64_STANDARD
        .decode(parts[2])
        .map_err(|_| anyhow!("Encrypted value payload is invalid."))?;

    if iv.len() != ENCRYPTION_IV_LENGTH || tag.is_empty() {
        return Err(anyhow!("Encrypted value payload is invalid."));
    }

    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|_| anyhow!("Failed to initialize cipher."))?;
    let nonce = Nonce::from_slice(&iv);
    let mut encrypted = Vec::with_capacity(data.len() + tag.len());
    encrypted.extend_from_slice(&data);
    encrypted.extend_from_slice(&tag);
    let plaintext = cipher
        .decrypt(nonce, encrypted.as_ref())
        .map_err(|_| anyhow!("Failed to decrypt encrypted value."))?;

    String::from_utf8(plaintext).context("Decrypted value is not valid UTF-8.")
}

fn load_database_key() -> Result<[u8; 32]> {
    let raw = std::env::var(DATABASE_KEY_ENV_VAR).unwrap_or_default();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!(
            "{} is required to encrypt and decrypt secrets. Generate one with \"openssl rand -hex 32\".",
            DATABASE_KEY_ENV_VAR
        ));
    }

    if trimmed.len() == 64 && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        let decoded = hex::decode(trimmed).map_err(|_| invalid_database_key_error(0))?;
        let key: [u8; 32] = decoded
            .try_into()
            .map_err(|decoded: Vec<u8>| invalid_database_key_error(decoded.len()))?;
        return Ok(key);
    }

    let decoded = BASE64_STANDARD.decode(trimmed).unwrap_or_default();
    let key: [u8; 32] = decoded
        .try_into()
        .map_err(|decoded: Vec<u8>| invalid_database_key_error(decoded.len()))?;
    Ok(key)
}

fn invalid_database_key_error(key_length: usize) -> anyhow::Error {
    anyhow!(
        "{} must be a 32-byte key encoded as 64 hex characters or base64. Key length {}",
        DATABASE_KEY_ENV_VAR,
        key_length
    )
}

fn serialize_performance(perf: &StrategyPerformance) -> Result<String> {
    let value = json!({
        "totalTrades": perf.total_trades,
        "winningTrades": perf.winning_trades,
        "losingTrades": perf.losing_trades,
        "winRate": perf.win_rate,
        "totalReturn": perf.total_return,
        "cagr": perf.cagr,
        "sharpeRatio": perf.sharpe_ratio,
        "calmarRatio": perf.calmar_ratio,
        "maxDrawdown": perf.max_drawdown,
        "maxDrawdownPercent": perf.max_drawdown_percent,
        "avgTradeReturn": perf.avg_trade_return,
        "bestTrade": perf.best_trade,
        "worstTrade": perf.worst_trade,
        "totalTickers": perf.total_tickers,
        "medianTradeDuration": perf.median_trade_duration,
        "medianTradePnl": perf.median_trade_pnl,
        "medianTradePnlPercent": perf.median_trade_pnl_percent,
        "medianConcurrentTrades": perf.median_concurrent_trades,
        "avgTradeDuration": perf.avg_trade_duration,
        "avgTradePnl": perf.avg_trade_pnl,
        "avgTradePnlPercent": perf.avg_trade_pnl_percent,
        "avgConcurrentTrades": perf.avg_concurrent_trades,
        "avgLosingPnl": perf.avg_losing_pnl,
        "avgLosingPnlPercent": perf.avg_losing_pnl_percent,
        "avgWinningPnl": perf.avg_winning_pnl,
        "avgWinningPnlPercent": perf.avg_winning_pnl_percent,
        "lastUpdated": perf.last_updated.to_rfc3339(),
    });

    serde_json::to_string(&value)
        .map_err(|err| anyhow::anyhow!("Failed to serialize performance: {}", err))
}

fn serialize_snapshots(snapshots: &[BacktestDataPoint]) -> Result<String> {
    let values: Vec<_> = snapshots
        .iter()
        .map(|snapshot| {
            json!({
                "date": snapshot.date.to_rfc3339(),
                "portfolioValue": snapshot.portfolio_value,
                "cash": snapshot.cash,
                "positionsValue": snapshot.positions_value,
                "concurrentTrades": snapshot.concurrent_trades,
                "missedTradesDueToCash": snapshot.missed_trades_due_to_cash,
            })
        })
        .collect();

    serde_json::to_string(&values)
        .map_err(|err| anyhow::anyhow!("Failed to serialize snapshots: {}", err))
}

fn deserialize_performance(json_str: &str) -> Result<StrategyPerformance> {
    let mut value: Value = serde_json::from_str(json_str).map_err(|err| {
        error!(
            "Failed to parse performance JSON payload: {} (error: {})",
            json_str, err
        );
        anyhow!("Failed to deserialize performance JSON: {}", err)
    })?;

    if let Value::Object(ref mut map) = value {
        const FLOAT_FIELDS: &[&str] = &[
            "winRate",
            "totalReturn",
            "cagr",
            "sharpeRatio",
            "calmarRatio",
            "maxDrawdown",
            "maxDrawdownPercent",
            "avgTradeReturn",
            "bestTrade",
            "worstTrade",
            "medianTradeDuration",
            "medianTradePnl",
            "medianTradePnlPercent",
            "medianConcurrentTrades",
            "avgTradeDuration",
            "avgTradePnl",
            "avgTradePnlPercent",
            "avgConcurrentTrades",
            "avgLosingPnl",
            "avgLosingPnlPercent",
            "avgWinningPnl",
            "avgWinningPnlPercent",
        ];
        const INT_FIELDS: &[&str] = &[
            "totalTrades",
            "winningTrades",
            "losingTrades",
            "totalTickers",
        ];

        for field in FLOAT_FIELDS {
            ensure_f64_field(map, field);
        }

        for field in INT_FIELDS {
            ensure_i32_field(map, field);
        }

        ensure_timestamp_field(map, "lastUpdated");
    }

    serde_json::from_value(value).map_err(|err| {
        error!(
            "Failed to convert performance JSON payload: {} (error: {})",
            json_str, err
        );
        anyhow!("Failed to deserialize performance JSON: {}", err)
    })
}

fn deserialize_snapshots(json_str: &str) -> Result<Vec<BacktestDataPoint>> {
    serde_json::from_str(json_str)
        .map_err(|err| anyhow!("Failed to deserialize snapshots JSON: {}", err))
}

fn ensure_f64_field(map: &mut Map<String, Value>, key: &str) {
    if matches!(map.get(key), None | Some(Value::Null)) {
        map.insert(key.to_string(), Value::from(0.0));
    }
}

fn ensure_i32_field(map: &mut Map<String, Value>, key: &str) {
    if matches!(map.get(key), None | Some(Value::Null)) {
        map.insert(key.to_string(), Value::from(0));
    }
}

fn ensure_timestamp_field(map: &mut Map<String, Value>, key: &str) {
    let needs_default = match map.get(key) {
        None | Some(Value::Null) => true,
        Some(Value::String(value)) if value.trim().is_empty() => true,
        _ => false,
    };

    if needs_default {
        map.insert(key.to_string(), Value::String(Utc::now().to_rfc3339()));
    }
}

fn parse_trade_status(value: &str) -> Result<TradeStatus> {
    match value.to_ascii_lowercase().as_str() {
        "pending" => Ok(TradeStatus::Pending),
        "active" => Ok(TradeStatus::Active),
        "closed" => Ok(TradeStatus::Closed),
        "cancelled" => Ok(TradeStatus::Cancelled),
        other => Err(anyhow!("Unknown trade status '{}'", other)),
    }
}

fn naive_date_to_datetime(date: NaiveDate) -> DateTime<Utc> {
    date.and_hms_opt(0, 0, 0)
        .expect("midnight should always be valid")
        .and_utc()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine;
    use serde_json::json;
    use std::sync::{Mutex, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    const TEST_DATABASE_KEY_HEX: &str =
        "0f0e0d0c0b0a090807060504030201000102030405060708090a0b0c0d0e0f10";

    #[test]
    fn deserialize_performance_handles_null_numbers() {
        let payload = json!({
            "totalTrades": null,
            "winningTrades": 3,
            "losingTrades": null,
            "winRate": null,
              "totalReturn": null,
              "cagr": null,
              "sharpeRatio": null,
              "calmarRatio": null,
              "maxDrawdown": null,
            "maxDrawdownPercent": null,
            "avgTradeReturn": null,
            "bestTrade": null,
            "worstTrade": null,
            "totalTickers": null,
            "medianTradeDuration": null,
            "medianTradePnl": null,
            "medianTradePnlPercent": null,
            "medianConcurrentTrades": null,
            "avgTradeDuration": null,
            "avgTradePnl": null,
            "avgTradePnlPercent": null,
            "avgConcurrentTrades": null,
            "avgLosingPnl": null,
            "avgLosingPnlPercent": null,
            "avgWinningPnl": null,
            "avgWinningPnlPercent": null,
            "lastUpdated": null
        });

        let performance =
            deserialize_performance(&payload.to_string()).expect("Null fields should be handled");

        assert_eq!(performance.total_trades, 0);
        assert_eq!(performance.winning_trades, 3);
        assert_eq!(performance.losing_trades, 0);
        assert_eq!(performance.win_rate, 0.0);
        assert!(performance.last_updated.timestamp() > 0);
    }

    #[test]
    fn decrypt_database_value_returns_plaintext_when_not_encrypted() {
        let plaintext = "paper-key";
        let decrypted = decrypt_database_value(plaintext).expect("plaintext should pass through");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn decrypt_database_value_decrypts_enc_v1_payload() {
        let guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock should not be poisoned");
        std::env::set_var(DATABASE_KEY_ENV_VAR, TEST_DATABASE_KEY_HEX);

        let key_bytes = hex::decode(TEST_DATABASE_KEY_HEX).expect("test key should decode");
        let key: [u8; 32] = key_bytes
            .try_into()
            .expect("test key should contain exactly 32 bytes");
        let encrypted = encrypt_test_value("super-secret", &key);
        let decrypted = decrypt_database_value(&encrypted).expect("ciphertext should decrypt");
        assert_eq!(decrypted, "super-secret");

        std::env::remove_var(DATABASE_KEY_ENV_VAR);
        drop(guard);
    }

    #[test]
    fn decrypt_database_value_requires_database_key_for_encrypted_values() {
        let guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock should not be poisoned");
        std::env::remove_var(DATABASE_KEY_ENV_VAR);

        let error = decrypt_database_value("enc:v1:iv:data:tag")
            .expect_err("encrypted values should fail without DATABASE_KEY");
        assert!(error.to_string().contains(DATABASE_KEY_ENV_VAR));
        drop(guard);
    }

    fn encrypt_test_value(value: &str, key: &[u8; 32]) -> String {
        let iv: [u8; ENCRYPTION_IV_LENGTH] = [7, 10, 14, 22, 31, 33, 46, 59, 61, 72, 81, 99];
        let cipher =
            Aes256Gcm::new_from_slice(key).expect("test cipher should initialize with key");
        let nonce = Nonce::from_slice(&iv);
        let ciphertext_and_tag = cipher
            .encrypt(nonce, value.as_bytes())
            .expect("test encryption should succeed");
        let split_at = ciphertext_and_tag
            .len()
            .checked_sub(16)
            .expect("ciphertext should include a GCM auth tag");
        let (ciphertext, tag) = ciphertext_and_tag.split_at(split_at);

        format!(
            "{}{}:{}:{}",
            ENCRYPTION_PREFIX,
            BASE64_STANDARD.encode(iv),
            BASE64_STANDARD.encode(ciphertext),
            BASE64_STANDARD.encode(tag)
        )
    }
}

fn clamp_i64_to_i32(value: i64) -> i32 {
    if value > i32::MAX as i64 {
        i32::MAX
    } else if value < i32::MIN as i64 {
        i32::MIN
    } else {
        value as i32
    }
}

fn calculate_period_days(start: &DateTime<Utc>, end: &DateTime<Utc>) -> i64 {
    let start_date = start.date_naive();
    let end_date = end.date_naive();

    if end_date < start_date {
        return 0;
    }

    let diff = (end_date - start_date).num_days();
    if diff <= 0 {
        1
    } else {
        diff
    }
}

fn calculate_period_months(period_days: i64) -> i64 {
    if period_days <= 0 {
        0
    } else {
        ((period_days as f64) / 30.4).round() as i64
    }
}
