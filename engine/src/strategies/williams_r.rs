use crate::models::*;
use crate::param_utils::{get_param_f64, get_param_usize_at_least};
use std::collections::HashMap;

pub struct WilliamsRStrategy {
    template_id: String,
    period: usize,
    oversold_level: f64,
    overbought_level: f64,
    min_confidence: f64,
}

impl WilliamsRStrategy {
    pub fn new(parameters: HashMap<String, f64>) -> Self {
        let period = get_param_usize_at_least(&parameters, "period", 14, 1);
        let oversold_level = get_param_f64(&parameters, "oversoldLevel", -90.0);
        let overbought_level = get_param_f64(&parameters, "overboughtLevel", -10.0);
        let min_confidence = get_param_f64(&parameters, "minConfidence", 0.5);
        Self {
            template_id: "williams_r".to_string(),
            period,
            oversold_level,
            overbought_level,
            min_confidence,
        }
    }

    fn calculate_williams_r(
        &self,
        highs: &[f64],
        lows: &[f64],
        closes: &[f64],
        period: usize,
    ) -> Vec<f64> {
        if period == 0 || highs.len() < period {
            return Vec::new();
        }

        let mut williams_r_values = Vec::new();

        for i in period - 1..highs.len() {
            let window_start = i + 1 - period;
            let slice_high = &highs[window_start..=i];
            let slice_low = &lows[window_start..=i];
            let current_close = closes[i];

            let highest_high = slice_high.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            let lowest_low = slice_low.iter().fold(f64::INFINITY, |a, &b| a.min(b));

            let williams_r = if highest_high == lowest_low {
                -50.0 // Neutral value when high == low
            } else {
                ((highest_high - current_close) / (highest_high - lowest_low)) * -100.0
            };

            williams_r_values.push(williams_r);
        }

        williams_r_values
    }
}

impl super::Strategy for WilliamsRStrategy {
    fn get_template_id(&self) -> &str {
        &self.template_id
    }

    fn generate_signal(
        &self,
        _ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> StrategySignal {
        let highs: Vec<f64> = candles[..=candle_index].iter().map(|c| c.high).collect();
        let lows: Vec<f64> = candles[..=candle_index].iter().map(|c| c.low).collect();
        let closes: Vec<f64> = candles[..=candle_index].iter().map(|c| c.close).collect();

        let williams_r_values = self.calculate_williams_r(&highs, &lows, &closes, self.period);

        if williams_r_values.is_empty() {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let current_williams_r = williams_r_values[williams_r_values.len() - 1];

        // Buy signal: Williams %R is oversold
        if current_williams_r < self.oversold_level {
            let confidence =
                ((self.oversold_level - current_williams_r) / self.oversold_level.abs() + 0.5)
                    .min(1.0);
            if confidence >= self.min_confidence - 1e-6 {
                return StrategySignal {
                    action: SignalAction::Buy,
                    confidence,
                };
            }
        }

        // Sell signal: Williams %R is overbought
        if current_williams_r > self.overbought_level {
            let confidence =
                ((current_williams_r - self.overbought_level) / self.overbought_level.abs() + 0.5)
                    .min(1.0);
            if confidence >= self.min_confidence - 1e-6 {
                return StrategySignal {
                    action: SignalAction::Sell,
                    confidence,
                };
            }
        }

        StrategySignal {
            action: SignalAction::Hold,
            confidence: 0.0,
        }
    }

    fn get_min_data_points(&self) -> usize {
        self.period.max(50)
    }
}
