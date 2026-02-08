use crate::models::*;
use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

pub trait Strategy {
    fn get_template_id(&self) -> &str;
    fn generate_signal(
        &self,
        ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> StrategySignal;
    fn target_ticker(&self) -> Option<String> {
        None
    }
    #[allow(dead_code)]
    fn get_min_data_points(&self) -> usize;
    fn snapshot_state(&self) -> Option<Value> {
        None
    }
    fn restore_state(&self, _state: &Value) -> Result<()> {
        Ok(())
    }
}

#[path = "strategies/rsi.rs"]
pub mod rsi;

pub use rsi::RSIStrategy;

#[path = "strategies/macd.rs"]
pub mod macd;

pub use macd::MACDStrategy;

#[path = "strategies/williams_r.rs"]
pub mod williams_r;

pub use williams_r::WilliamsRStrategy;

#[path = "strategies/adx.rs"]
pub mod adx;

pub use adx::ADXStrategy;

#[path = "strategies/atr.rs"]
pub mod atr;

pub use atr::ATRStrategy;

#[path = "strategies/lightgbm.rs"]
pub mod lightgbm;

pub use lightgbm::LightGBMStrategy;

#[path = "strategies/weighted_momentum.rs"]
pub mod weighted_momentum;

pub use weighted_momentum::WeightedMomentumStrategy;

#[path = "strategies/psar.rs"]
pub mod psar;

pub use psar::PSARStrategy;

#[path = "strategies/buy_and_hold.rs"]
pub mod buy_and_hold;

pub use buy_and_hold::BuyAndHoldStrategy;

pub fn create_strategy(
    template_id: &str,
    parameters: HashMap<String, f64>,
) -> Result<Box<dyn Strategy + Send + Sync>> {
    if template_id.starts_with("lightgbm_") {
        return Ok(Box::new(LightGBMStrategy::new(
            template_id.to_string(),
            parameters,
        )));
    }

    match template_id {
        "rsi" => Ok(Box::new(RSIStrategy::new(parameters))),
        "macd" => Ok(Box::new(MACDStrategy::new(parameters))),
        "williams_r" => Ok(Box::new(WilliamsRStrategy::new(parameters))),
        "adx" => Ok(Box::new(ADXStrategy::new(parameters))),
        "atr" => Ok(Box::new(ATRStrategy::new(parameters))),
        "buy_and_hold" => Ok(Box::new(BuyAndHoldStrategy::new(parameters))),
        "lightgbm" => Ok(Box::new(LightGBMStrategy::new(
            template_id.to_string(),
            parameters,
        ))),
        "psar" => Ok(Box::new(PSARStrategy::new(parameters))),
        "weighted_momentum" => Ok(Box::new(WeightedMomentumStrategy::new(parameters))),
        _ => Err(anyhow::anyhow!(
            "Unknown strategy template: {}",
            template_id
        )),
    }
}
