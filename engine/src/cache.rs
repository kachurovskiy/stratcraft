use crate::backtest_api_client::build_blocking_client;
use crate::models::*;
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_LOCAL_API_PORT: u16 = 3000;
const MAX_ERROR_BODY_CHARS: usize = 2048;

#[derive(Clone)]
pub struct CacheManager {
    pub local_cache: Arc<DashMap<String, OptimizationResult>>,
    has_db: bool,
    remote_timeout: Duration,
    remote_request_gate: Arc<Mutex<Option<Instant>>>,
    remote_api_secret: Option<String>,
    remote_api_base_url: Option<String>,
}

pub struct CacheStoreParams {
    pub template_id: String,
    pub parameters: HashMap<String, f64>,
    pub result: OptimizationResult,
    pub ticker_count: i32,
    pub start_date: chrono::DateTime<chrono::Utc>,
    pub end_date: chrono::DateTime<chrono::Utc>,
    pub duration_minutes: f64,
    pub top_absolute_gain_ticker: Option<String>,
    pub top_relative_gain_ticker: Option<String>,
}

impl CacheManager {
    pub fn new(
        remote_api_secret: Option<String>,
        remote_api_base_url: Option<String>,
        has_db: bool,
    ) -> Self {
        let remote_api_secret = remote_api_secret
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let remote_api_base_url = remote_api_base_url
            .map(|value| value.trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty());
        Self {
            local_cache: Arc::new(DashMap::new()),
            has_db,
            remote_timeout: Duration::from_secs(30),
            remote_request_gate: Arc::new(Mutex::new(None)),
            remote_api_secret,
            remote_api_base_url,
        }
    }

    fn throttle_remote_request(&self) {
        const MIN_GAP: Duration = Duration::from_secs(1);
        if let Ok(mut guard) = self.remote_request_gate.lock() {
            if let Some(last) = *guard {
                let elapsed = last.elapsed();
                if elapsed < MIN_GAP {
                    thread::sleep(MIN_GAP - elapsed);
                }
            }
            *guard = Some(Instant::now());
        }
    }

    fn retry_with_backoff<F, T>(mut operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        const MAX_RETRIES: usize = 3;
        const BASE_DELAY_MS: u64 = 1000;
        const MAX_DELAY_MS: u64 = 10000;

        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            match operation() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < MAX_RETRIES {
                        let delay_ms =
                            (BASE_DELAY_MS * 2_u64.pow(attempt as u32)).min(MAX_DELAY_MS);
                        let jitter_range = (delay_ms as f64 * 0.25) as u64;
                        let jitter = fastrand::u64(0..=jitter_range * 2);
                        let final_delay =
                            delay_ms.saturating_sub(jitter_range).saturating_add(jitter);
                        log::debug!(
                            "Attempt {} failed, retrying in {}ms",
                            attempt + 1,
                            final_delay
                        );
                        thread::sleep(Duration::from_millis(final_delay));
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("retry_with_backoff exhausted attempts")))
    }

    pub fn get_cache_key(template_id: &str, parameters: &HashMap<String, f64>) -> String {
        let params_json = serde_json::to_string(parameters).unwrap_or_default();
        format!("{}:{}", template_id, params_json)
    }

    pub fn check_cache(
        &self,
        template_id: &str,
        parameters: &HashMap<String, f64>,
    ) -> Option<OptimizationResult> {
        let cache_key = Self::get_cache_key(template_id, parameters);

        if let Some(result) = self.local_cache.get(&cache_key) {
            return Some(result.clone());
        }

        let use_local_api = self.has_db && std::env::var("SERVER_PORT").is_ok();
        let api_base_url = if use_local_api {
            resolve_local_api_base_url()
        } else {
            let Some(api_base_url) = self.remote_api_base_url.clone() else {
                return None;
            };
            api_base_url
        };

        match self.check_remote_cache(&api_base_url, template_id, parameters, !use_local_api) {
            Ok(Some(result)) => {
                self.local_cache.insert(cache_key, result.clone());
                Some(result)
            }
            Ok(None) => None,
            Err(e) => {
                log::warn!("Failed to check cache API: {:?}", e);
                None
            }
        }
    }

    pub fn store_cache(&self, params: CacheStoreParams) {
        let use_local_api = self.has_db && std::env::var("SERVER_PORT").is_ok();
        let api_base_url = if use_local_api {
            resolve_local_api_base_url()
        } else {
            let Some(api_base_url) = self.remote_api_base_url.clone() else {
                return;
            };
            api_base_url
        };
        let timeout = self.remote_timeout;
        let api_secret = self.remote_api_secret.clone();

        thread::spawn(move || {
            if let Err(e) =
                Self::store_remote_cache_static(&api_base_url, timeout, params, api_secret)
            {
                log::warn!("Failed to store cache: {:?}", e);
            }
        });
    }

    fn check_remote_cache(
        &self,
        api_base_url: &str,
        template_id: &str,
        parameters: &HashMap<String, f64>,
        throttle: bool,
    ) -> Result<Option<OptimizationResult>> {
        let template_id = template_id.to_string();
        let parameters = parameters.clone();
        let timeout = self.remote_timeout;
        let api_secret = self.remote_api_secret.clone();

        Self::retry_with_backoff(|| {
            if throttle {
                self.throttle_remote_request();
            }
            let client = build_blocking_client(Some(timeout))?;
            let url = format!("{}/backtest/check", api_base_url);

            let request_body = serde_json::json!({
                "templateId": template_id,
                "parameters": parameters,
            });

            let mut request = client.post(url.as_str()).json(&request_body);
            if let Some(secret) = api_secret.as_deref() {
                request = request.header("x-backtest-secret", secret);
            }
            let response = request.send()?;

            if response.status().is_success() {
                let api_response: ApiCheckResponse = response.json()?;
                Ok(api_response.result)
            } else {
                let status = response.status();
                let body = response.text().unwrap_or_default();
                log::warn!(
                    "API check failed: status={} url={} template_id={} has_secret={} body={}",
                    status,
                    url,
                    template_id,
                    api_secret.is_some(),
                    truncate_for_log(&body, MAX_ERROR_BODY_CHARS)
                );
                Ok(None)
            }
        })
    }

    fn store_remote_cache_static(
        api_base_url: &str,
        timeout: Duration,
        params: CacheStoreParams,
        api_secret: Option<String>,
    ) -> Result<()> {
        let CacheStoreParams {
            template_id,
            parameters,
            result,
            ticker_count,
            start_date,
            end_date,
            duration_minutes,
            top_absolute_gain_ticker,
            top_relative_gain_ticker,
        } = params;

        Self::retry_with_backoff(move || {
            let client = build_blocking_client(Some(timeout))?;
            let url = format!("{}/backtest/store", api_base_url);

            let request_body = serde_json::json!({
                "templateId": template_id,
                "parameters": parameters,
                "cagr": result.cagr,
                "sharpeRatio": result.sharpe_ratio,
                "calmarRatio": result.calmar_ratio,
                "totalReturn": result.total_return,
                "maxDrawdown": result.max_drawdown,
                "maxDrawdownRatio": result.max_drawdown_ratio,
                "winRate": result.win_rate,
                "totalTrades": result.total_trades,
                "tickerCount": ticker_count,
                "startDate": start_date.to_rfc3339(),
                "endDate": end_date.to_rfc3339(),
                "durationMinutes": duration_minutes,
                "tool": "rust-cli",
                "topAbsoluteGainTicker": top_absolute_gain_ticker,
                "topRelativeGainTicker": top_relative_gain_ticker,
            });

            let mut request = client.post(&url).json(&request_body);
            if let Some(secret) = api_secret.as_deref() {
                request = request.header("x-backtest-secret", secret);
            }
            let response = request.send()?;

            if response.status().is_success() {
                let api_response: ApiStoreResponse = response.json()?;
                if !api_response.success {
                    log::warn!("API store returned failure: {:?}", api_response.message);
                }
            } else {
                let status = response.status();
                let body = response.text().unwrap_or_default();
                log::warn!(
                    "API store failed: status={} url={} template_id={} has_secret={} body={}",
                    status,
                    url,
                    template_id,
                    api_secret.is_some(),
                    truncate_for_log(&body, MAX_ERROR_BODY_CHARS)
                );
            }

            Ok(())
        })
    }
}

fn truncate_for_log(value: &str, max_chars: usize) -> String {
    let trimmed = value.trim();
    let mut iter = trimmed.chars();
    let mut out = String::new();
    for _ in 0..max_chars {
        let Some(ch) = iter.next() else {
            return trimmed.to_string();
        };
        out.push(ch);
    }
    if iter.next().is_some() {
        out.push('…');
    }
    out
}

fn resolve_local_api_base_url() -> String {
    let parsed_port = std::env::var("SERVER_PORT")
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|value| *value != 0)
        .unwrap_or(DEFAULT_LOCAL_API_PORT);
    format!("http://localhost:{}/api", parsed_port)
}
