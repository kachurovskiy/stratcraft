use crate::indicators;
use crate::models::*;
use crate::param_utils::{get_param_f64, get_param_usize};
use crate::strategy_utils::{buy_signal, hold_signal, meets_confidence_threshold, sell_signal};
use std::collections::HashMap;

pub struct MACDStrategy {
    pub template_id: String,
    min_confidence: f64,
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
    min_data_points: usize,
}

impl MACDStrategy {
    pub fn new(parameters: HashMap<String, f64>) -> Self {
        let min_confidence = get_param_f64(&parameters, "minConfidence", 0.6);
        let fast_period = get_param_usize(&parameters, "fastPeriod", 12);
        let slow_period = get_param_usize(&parameters, "slowPeriod", 26);
        let signal_period = get_param_usize(&parameters, "signalPeriod", 9);
        let slow_for_min = get_param_f64(&parameters, "slowPeriod", 26.0);
        let signal_for_min = get_param_f64(&parameters, "signalPeriod", 9.0);
        let min_data_points = (slow_for_min.max(50.0) + signal_for_min) as usize;
        Self {
            template_id: "macd".to_string(),
            min_confidence,
            fast_period,
            slow_period,
            signal_period,
            min_data_points,
        }
    }
}

impl super::Strategy for MACDStrategy {
    fn get_template_id(&self) -> &str {
        &self.template_id
    }

    fn generate_signal(
        &self,
        _ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> StrategySignal {
        let prices: Vec<f64> = candles[..=candle_index].iter().map(|c| c.close).collect();
        let (macd_line, signal_line, _histogram) = indicators::calculate_macd(
            &prices,
            self.fast_period,
            self.slow_period,
            self.signal_period,
        );

        if macd_line.len() < 2 || signal_line.len() < 2 {
            return hold_signal();
        }

        let current_macd = macd_line[macd_line.len() - 1];
        let prev_macd = macd_line[macd_line.len() - 2];
        let current_signal = signal_line[signal_line.len() - 1];
        let prev_signal = signal_line[signal_line.len() - 2];

        // Check for MACD line crossing above signal line (bullish)
        if prev_macd <= prev_signal && current_macd > current_signal {
            let confidence =
                (current_macd - current_signal).abs() / current_signal.abs() * 10.0 + 0.5;
            let confidence = confidence.min(1.0);
            if meets_confidence_threshold(confidence, self.min_confidence) {
                return buy_signal(confidence);
            }
        }

        // Check for MACD line crossing below signal line (bearish)
        if prev_macd >= prev_signal && current_macd < current_signal {
            let confidence =
                (current_macd - current_signal).abs() / current_signal.abs() * 10.0 + 0.5;
            let confidence = confidence.min(1.0);
            if meets_confidence_threshold(confidence, self.min_confidence) {
                return sell_signal(confidence);
            }
        }

        hold_signal()
    }

    fn get_min_data_points(&self) -> usize {
        self.min_data_points
    }
}
