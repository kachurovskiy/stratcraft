use crate::models::*;
use crate::param_utils::{get_param_f64, get_param_usize_at_least};
use std::collections::HashMap;

pub struct WeightedMomentumStrategy {
    template_id: String,
    roc_period1: usize,
    sma_period1: usize,
    roc_period2: usize,
    sma_period2: usize,
    weight1: f64,
    weight2: f64,
    min_confidence: f64,
}

impl WeightedMomentumStrategy {
    pub fn new(parameters: HashMap<String, f64>) -> Self {
        let roc_period1 = get_param_usize_at_least(&parameters, "rocPeriod1", 21, 1);
        let sma_period1 = get_param_usize_at_least(&parameters, "smaPeriod1", 21, 1);
        let roc_period2 = get_param_usize_at_least(&parameters, "rocPeriod2", 21, 1);
        let sma_period2 = get_param_usize_at_least(&parameters, "smaPeriod2", 21, 1);
        let weight1 = get_param_f64(&parameters, "weight1", 1.0);
        let weight2 = get_param_f64(&parameters, "weight2", 1.0);
        let min_confidence = get_param_f64(&parameters, "minConfidence", 0.5);
        Self {
            template_id: "weighted_momentum".to_string(),
            roc_period1,
            sma_period1,
            roc_period2,
            sma_period2,
            weight1,
            weight2,
            min_confidence,
        }
    }

    fn calculate_roc(values: &[f64], period: usize) -> Vec<f64> {
        if period == 0 || values.len() <= period {
            return Vec::new();
        }

        let mut roc = Vec::with_capacity(values.len() - period);
        for idx in period..values.len() {
            let previous = values[idx - period];
            if previous.abs() < f64::EPSILON {
                roc.push(0.0);
            } else {
                roc.push((values[idx] - previous) / previous * 100.0);
            }
        }
        roc
    }

    fn simple_sma(values: &[f64], period: usize) -> Vec<f64> {
        if period == 0 || values.len() < period {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(values.len() - period + 1);
        for end in (period - 1)..values.len() {
            let start = end + 1 - period;
            let sum: f64 = values[start..=end].iter().sum();
            result.push(sum / period as f64);
        }
        result
    }
}

impl super::Strategy for WeightedMomentumStrategy {
    fn get_template_id(&self) -> &str {
        &self.template_id
    }

    fn generate_signal(
        &self,
        _ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> StrategySignal {
        if candles.is_empty() || candle_index >= candles.len() {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let min_points = self.get_min_data_points();
        if candle_index + 1 < min_points {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let closes: Vec<f64> = candles[..=candle_index].iter().map(|c| c.close).collect();
        let roc1 = Self::calculate_roc(&closes, self.roc_period1);
        let roc2 = Self::calculate_roc(&closes, self.roc_period2);

        if roc1.len() < self.sma_period1 || roc2.len() < self.sma_period2 {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let smoothed1 = Self::simple_sma(&roc1, self.sma_period1);
        let smoothed2 = Self::simple_sma(&roc2, self.sma_period2);

        if smoothed1.is_empty() || smoothed2.is_empty() {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let osc = smoothed1.last().copied().unwrap_or(0.0) * self.weight1
            + smoothed2.last().copied().unwrap_or(0.0) * self.weight2;
        let confidence = (osc.abs() / 10.0).min(1.0);

        if confidence < self.min_confidence {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        if osc > 0.0 {
            StrategySignal {
                action: SignalAction::Buy,
                confidence,
            }
        } else if osc < 0.0 {
            StrategySignal {
                action: SignalAction::Sell,
                confidence,
            }
        } else {
            StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            }
        }
    }

    fn get_min_data_points(&self) -> usize {
        50usize.max((self.roc_period1 + self.sma_period1).max(self.roc_period2 + self.sma_period2))
    }
}
