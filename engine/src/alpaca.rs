use crate::engine::{AccountPositionState, AccountStateSnapshot, AccountStopOrderState};
use crate::models::AccountCredentials;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use log::{info, warn};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::de::{self, DeserializeOwned, Deserializer, Visitor};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;
use tokio::time::sleep;

const ALPACA_PAPER_URL_SETTING: &str = "ALPACA_PAPER_URL";
const ALPACA_LIVE_URL_SETTING: &str = "ALPACA_LIVE_URL";
const ORDER_QUERY_LIMIT: &str = "500";
const ORDER_QUERY_LIMIT_NUM: usize = 500;
const ORDER_MAX_PAGES: usize = 100;
const REQUEST_DELAY: Duration = Duration::from_millis(350);

pub struct AlpacaClient<'a> {
    http: &'a Client,
    base_url: String,
    headers: HeaderMap,
}

impl<'a> AlpacaClient<'a> {
    pub fn new(
        http: &'a Client,
        creds: &AccountCredentials,
        settings: &HashMap<String, String>,
    ) -> Result<Self> {
        let base_url = resolve_alpaca_base_url(&creds.environment, settings)?;

        let mut headers = HeaderMap::new();
        headers.insert(
            "APCA-API-KEY-ID",
            HeaderValue::from_str(&creds.api_key).context("invalid Alpaca API key")?,
        );
        headers.insert(
            "APCA-API-SECRET-KEY",
            HeaderValue::from_str(&creds.api_secret).context("invalid Alpaca API secret")?,
        );

        Ok(Self {
            http,
            base_url,
            headers,
        })
    }

    pub async fn fetch_account_state(&self) -> Result<AccountStateSnapshot> {
        let account: AlpacaAccount = self.get("/account").await?;
        let positions: Vec<AlpacaPosition> = self.get("/positions").await?;
        let orders = self.fetch_open_orders().await?;

        let mut held_tickers = HashSet::new();
        let mut account_positions = Vec::new();
        for entry in positions {
            if let Some(symbol) = normalize_symbol(entry.symbol.as_deref()) {
                let side = entry.side.as_deref().unwrap_or("long").trim().to_string();
                let qty = entry.qty.unwrap_or(0.0).round() as i32;
                if qty == 0 {
                    continue;
                }
                let signed_qty = if side.eq_ignore_ascii_case("short") {
                    -qty.abs()
                } else {
                    qty.abs()
                };
                held_tickers.insert(symbol.clone());
                account_positions.push(AccountPositionState {
                    ticker: symbol,
                    quantity: signed_qty,
                    avg_entry_price: entry.avg_entry_price.unwrap_or(0.0),
                    current_price: entry.current_price,
                });
            }
        }

        let mut open_buy_orders = HashSet::new();
        let mut open_sell_orders = HashSet::new();
        let mut stop_orders: HashMap<String, Vec<AccountStopOrderState>> = HashMap::new();
        for entry in orders {
            let Some(symbol) = normalize_symbol(entry.symbol.as_deref()) else {
                continue;
            };

            let order_side = normalize_side(entry.side.as_deref());
            match order_side.as_deref() {
                Some("buy") => {
                    open_buy_orders.insert(symbol.clone());
                }
                Some("sell") => {
                    open_sell_orders.insert(symbol.clone());
                }
                _ => {}
            }

            let order_type = entry
                .order_type
                .as_deref()
                .map(|raw| raw.trim().to_lowercase());
            if let Some(order_type) = order_type {
                if order_type == "stop" || order_type == "stop_limit" {
                    if let Some(stop_price) = entry.stop_price {
                        let qty = entry.qty.unwrap_or(0.0).round() as i32;
                        stop_orders
                            .entry(symbol.clone())
                            .or_insert_with(Vec::new)
                            .push(AccountStopOrderState {
                                quantity: qty,
                                stop_price,
                                side: order_side.clone().unwrap_or_default(),
                            });
                    }
                }
            }
        }

        Ok(AccountStateSnapshot {
            available_cash: account.cash.unwrap_or(0.0).max(0.0),
            held_tickers,
            open_buy_orders,
            open_sell_orders,
            positions: account_positions,
            stop_orders,
        })
    }

    pub async fn evaluate_order(&self, order_id: &str) -> Result<Option<OrderEvaluation>> {
        let trimmed = order_id.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        let Some(order) = self.fetch_order(trimmed).await? else {
            warn!("Order {} not found on Alpaca", trimmed);
            return Ok(None);
        };

        let status = order.normalized_status();
        let filled_price = order.filled_price();
        let filled_at = order.filled_timestamp();

        let state = match status.as_str() {
            "filled" | "done_for_day" => OrderState::Filled,
            "partially_filled" => {
                if order.filled_quantity().unwrap_or(0.0) > 0.0 {
                    OrderState::Filled
                } else {
                    OrderState::Pending
                }
            }
            value if is_cancel_status(value) => OrderState::Cancelled,
            _ => OrderState::Pending,
        };

        Ok(Some(OrderEvaluation {
            state,
            filled_price,
            timestamp: filled_at,
        }))
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<bool> {
        let trimmed = order_id.trim();
        if trimmed.is_empty() {
            return Ok(false);
        }

        let path = format!("/orders/{}", trimmed);
        if self.delete_order(trimmed, &path).await? {
            return Ok(true);
        }

        let client_path = format!("/orders:by_client_order_id/{}", trimmed);
        self.delete_order(trimmed, &client_path).await
    }

    async fn fetch_open_orders(&self) -> Result<Vec<AlpacaOrder>> {
        let mut all_orders = Vec::new();
        let mut after_order_id: Option<String> = None;
        let mut pages = 0usize;

        loop {
            if pages >= ORDER_MAX_PAGES {
                break;
            }
            pages += 1;

            let mut query_params = vec![
                ("status", "open"),
                ("direction", "asc"),
                ("limit", ORDER_QUERY_LIMIT),
                ("nested", "false"),
            ];
            if let Some(after_id) = after_order_id.as_deref() {
                query_params.push(("after_order_id", after_id));
            }

            let entries: Vec<AlpacaOrder> = self.get_with_query("/orders", &query_params).await?;
            if entries.is_empty() {
                break;
            }

            let is_last_page = entries.len() < ORDER_QUERY_LIMIT_NUM;
            let last_id = entries.iter().rev().find_map(extract_order_id);
            all_orders.extend(entries);

            if is_last_page {
                break;
            }

            if let Some(last_id) = last_id {
                after_order_id = Some(last_id);
            } else {
                break;
            }
        }

        Ok(all_orders)
    }

    async fn delete_order(&self, order_ref: &str, path: &str) -> Result<bool> {
        sleep(REQUEST_DELAY).await;
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .delete(url)
            .headers(self.headers.clone())
            .send()
            .await
            .with_context(|| format!("request {} failed", path))?;

        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            warn!(
                "Alpaca reported order {} missing while cancelling (status 404)",
                order_ref
            );
            return Ok(false);
        }

        if status == StatusCode::UNPROCESSABLE_ENTITY {
            // See alpaca-delete-order.txt for the 422 behavior (order no longer cancelable).
            info!(
                "Alpaca rejected cancellation of order {} because it is not cancelable (422)",
                order_ref
            );
            return Ok(false);
        }

        if status != StatusCode::NO_CONTENT {
            response.error_for_status()?;
        }

        Ok(true)
    }

    async fn fetch_order(&self, order_id: &str) -> Result<Option<AlpacaOrder>> {
        if let Some(order) = self.get_optional(&format!("/orders/{}", order_id)).await? {
            return Ok(Some(order));
        }
        self.get_optional(&format!("/orders:by_client_order_id/{}", order_id))
            .await
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        sleep(REQUEST_DELAY).await;
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .get(url)
            .headers(self.headers.clone())
            .send()
            .await
            .with_context(|| format!("GET {}{} failed", self.base_url, path))?
            .error_for_status()
            .with_context(|| format!("GET {}{} returned error", self.base_url, path))?;
        let value = response
            .json::<T>()
            .await
            .context("failed to parse Alpaca response")?;
        Ok(value)
    }

    async fn get_optional<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        sleep(REQUEST_DELAY).await;
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .get(url)
            .headers(self.headers.clone())
            .send()
            .await
            .with_context(|| format!("request {} failed", path))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = response.error_for_status()?;
        let payload = response
            .json::<T>()
            .await
            .context("failed to parse Alpaca response")?;
        Ok(Some(payload))
    }

    async fn get_with_query<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        sleep(REQUEST_DELAY).await;
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .get(url)
            .headers(self.headers.clone())
            .query(query)
            .send()
            .await
            .with_context(|| format!("GET {}{} with query failed", self.base_url, path))?
            .error_for_status()
            .with_context(|| format!("GET {}{} returned error", self.base_url, path))?;
        let value = response
            .json::<T>()
            .await
            .context("failed to parse Alpaca response")?;
        Ok(value)
    }
}

fn resolve_alpaca_base_url(
    environment: &str,
    settings: &HashMap<String, String>,
) -> Result<String> {
    let is_live = environment.trim().eq_ignore_ascii_case("live");
    let setting_key = if is_live {
        ALPACA_LIVE_URL_SETTING
    } else {
        ALPACA_PAPER_URL_SETTING
    };
    let configured = settings
        .get(setting_key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    match configured {
        Some(value) => Ok(value.to_string()),
        None => Err(anyhow!("Missing required setting {}", setting_key)),
    }
}

pub struct OrderEvaluation {
    pub state: OrderState,
    pub filled_price: Option<f64>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl OrderEvaluation {
    pub fn changed_at(&self) -> DateTime<Utc> {
        self.timestamp.unwrap_or_else(Utc::now)
    }
}

#[derive(Clone, Copy)]
pub enum OrderState {
    Pending,
    Filled,
    Cancelled,
}

#[derive(Debug, Deserialize)]
struct AlpacaAccount {
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    cash: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct AlpacaPosition {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    side: Option<String>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    qty: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    avg_entry_price: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    current_price: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct AlpacaOrder {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    client_order_id: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    side: Option<String>,
    #[serde(rename = "type", default)]
    order_type: Option<String>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    qty: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    stop_price: Option<f64>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    filled_qty: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    filled_avg_price: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    limit_price: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_f64_opt")]
    trail_price: Option<f64>,
    #[serde(default)]
    filled_at: Option<String>,
    #[serde(default)]
    updated_at: Option<String>,
    #[serde(default)]
    submitted_at: Option<String>,
}

impl AlpacaOrder {
    fn normalized_status(&self) -> String {
        self.status
            .as_deref()
            .unwrap_or("unknown")
            .trim()
            .to_lowercase()
    }

    fn filled_price(&self) -> Option<f64> {
        self.filled_avg_price
            .or(self.limit_price)
            .or(self.stop_price)
            .or(self.trail_price)
    }

    fn filled_timestamp(&self) -> Option<DateTime<Utc>> {
        parse_timestamp(self.filled_at.as_deref())
            .or_else(|| parse_timestamp(self.updated_at.as_deref()))
            .or_else(|| parse_timestamp(self.submitted_at.as_deref()))
    }

    fn filled_quantity(&self) -> Option<f64> {
        self.filled_qty
    }

    #[allow(dead_code)]
    fn total_quantity(&self) -> Option<f64> {
        self.qty
    }
}

fn deserialize_f64_opt<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct F64OptVisitor;

    impl<'de> Visitor<'de> for F64OptVisitor {
        type Value = Option<f64>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a number or string")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(value))
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(value as f64))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(value as f64))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.trim().parse::<f64>().ok())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value.trim().parse::<f64>().ok())
        }
    }

    deserializer.deserialize_any(F64OptVisitor)
}

fn normalize_symbol(value: Option<&str>) -> Option<String> {
    value
        .map(|symbol| symbol.trim().to_uppercase())
        .filter(|symbol| !symbol.is_empty())
}

fn normalize_side(value: Option<&str>) -> Option<String> {
    value
        .map(|side| side.trim().to_lowercase())
        .filter(|side| !side.is_empty())
}

fn extract_order_id(value: &AlpacaOrder) -> Option<String> {
    value
        .id
        .as_deref()
        .or_else(|| value.client_order_id.as_deref())
        .map(|id| id.trim())
        .filter(|id| !id.is_empty())
        .map(|id| id.to_string())
}

fn parse_timestamp(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|value| {
        DateTime::parse_from_rfc3339(value)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
    })
}

fn is_cancel_status(status: &str) -> bool {
    matches!(
        status,
        "canceled"
            | "cancelled"
            | "expired"
            | "rejected"
            | "stopped"
            | "suspended"
            | "pending_cancel"
    )
}
