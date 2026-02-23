use crate::indicators;
use crate::models::*;
use crate::param_utils::{get_param_f64, get_param_usize};
use crate::strategy_utils::{buy_signal, hold_signal, meets_confidence_threshold, sell_signal};
use std::collections::HashMap;

pub struct RSIStrategy {
    pub template_id: String,
    period: usize,
    oversold_level: f64,
    overbought_level: f64,
    min_confidence: f64,
}

impl RSIStrategy {
    pub fn new(parameters: HashMap<String, f64>) -> Self {
        let period = get_param_usize(&parameters, "period", 14);
        let oversold_level = get_param_f64(&parameters, "oversoldLevel", 30.0);
        let overbought_level = get_param_f64(&parameters, "overboughtLevel", 70.0);
        let min_confidence = get_param_f64(&parameters, "minConfidence", 0.6);
        Self {
            template_id: "rsi".to_string(),
            period,
            oversold_level,
            overbought_level,
            min_confidence,
        }
    }
}

impl super::Strategy for RSIStrategy {
    fn get_template_id(&self) -> &str {
        &self.template_id
    }

    fn generate_signal(
        &self,
        _ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> super::StrategySignal {
        let n = candles.len();
        if n < self.period + 1 || candle_index < self.period || candle_index >= n {
            return hold_signal();
        }

        let current_rsi =
            indicators::calculate_rsi_at(candles, self.period, candle_index).unwrap_or(50.0);

        // Buy signal: RSI is oversold
        if current_rsi < self.oversold_level {
            let confidence =
                ((self.oversold_level - current_rsi) / self.oversold_level + 0.5).min(1.0);
            if meets_confidence_threshold(confidence, self.min_confidence) {
                return buy_signal(confidence);
            }
        }

        // Sell signal: RSI is overbought
        if current_rsi > self.overbought_level {
            let confidence =
                ((current_rsi - self.overbought_level) / (100.0 - self.overbought_level) + 0.5)
                    .min(1.0);
            if meets_confidence_threshold(confidence, self.min_confidence) {
                return sell_signal(confidence);
            }
        }

        hold_signal()
    }

    fn get_min_data_points(&self) -> usize {
        self.period.max(50)
    }
}
