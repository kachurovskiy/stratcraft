use std::collections::HashMap;

use crate::models::{Candle, SignalAction, StrategySignal};
use crate::param_utils::get_param_f64;

pub struct PSARStrategy {
    template_id: String,
    step: f64,
    max_acceleration: f64,
    min_confidence: f64,
}

impl PSARStrategy {
    pub fn new(parameters: HashMap<String, f64>) -> Self {
        let step = get_param_f64(&parameters, "step", 0.02).max(0.0001);
        let max_acceleration = get_param_f64(&parameters, "max", 0.2).max(step);
        let min_confidence = get_param_f64(&parameters, "minConfidence", 0.3);
        Self {
            template_id: "psar".to_string(),
            step,
            max_acceleration,
            min_confidence,
        }
    }

    fn calculate_psar(highs: &[f64], lows: &[f64], step: f64, max_acceleration: f64) -> Vec<f64> {
        if highs.len() < 2 || lows.len() < 2 || highs.len() != lows.len() {
            return Vec::new();
        }

        let mut psar_values = Vec::with_capacity(highs.len());
        let mut is_uptrend = highs[1] >= highs[0];
        let mut psar_prev = if is_uptrend { lows[0] } else { highs[0] };
        let mut ep = if is_uptrend {
            highs[0].max(highs[1])
        } else {
            lows[0].min(lows[1])
        };
        let mut af = step;

        psar_values.push(psar_prev);

        for idx in 1..highs.len() {
            let mut psar = psar_prev + af * (ep - psar_prev);

            if is_uptrend {
                psar = psar.min(lows[idx - 1]);
                if idx > 1 {
                    psar = psar.min(lows[idx - 2]);
                }
                if highs[idx] > ep {
                    ep = highs[idx];
                    af = (af + step).min(max_acceleration);
                }
                if lows[idx] < psar {
                    is_uptrend = false;
                    psar = ep;
                    ep = lows[idx];
                    af = step;
                }
            } else {
                psar = psar.max(highs[idx - 1]);
                if idx > 1 {
                    psar = psar.max(highs[idx - 2]);
                }
                if lows[idx] < ep {
                    ep = lows[idx];
                    af = (af + step).min(max_acceleration);
                }
                if highs[idx] > psar {
                    is_uptrend = true;
                    psar = ep;
                    ep = highs[idx];
                    af = step;
                }
            }

            psar_values.push(psar);
            psar_prev = psar;
        }

        psar_values
    }
}

impl super::Strategy for PSARStrategy {
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

        let highs: Vec<f64> = candles[..=candle_index].iter().map(|c| c.high).collect();
        let lows: Vec<f64> = candles[..=candle_index].iter().map(|c| c.low).collect();
        let psar_values = Self::calculate_psar(&highs, &lows, self.step, self.max_acceleration);

        if psar_values.len() < 2 {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let current_psar = *psar_values.last().unwrap();
        let previous_psar = psar_values[psar_values.len() - 2];
        let current_price = candles[candle_index].close;
        let previous_close = candles[candle_index - 1].close;
        let denom = current_psar.abs().max(1e-9);

        if current_price > current_psar && previous_close <= previous_psar {
            let confidence =
                (((current_price - current_psar) / denom) * 10.0 + 0.5).clamp(0.0, 1.0);
            if confidence >= self.min_confidence - 1e-6 {
                return StrategySignal {
                    action: SignalAction::Buy,
                    confidence,
                };
            }
        }

        if current_price < current_psar && previous_close >= previous_psar {
            let confidence =
                (((current_psar - current_price) / denom) * 10.0 + 0.5).clamp(0.0, 1.0);
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
        50
    }
}
