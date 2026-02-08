use crate::indicators::compute_atr_series;
use crate::models::*;
use crate::param_utils::get_param_f64;
use std::collections::HashMap;

pub struct ATRStrategy {
    template_id: String,
    period: usize,
    min_period_points: usize,
    volatility_threshold: f64,
    min_confidence: f64,
}

impl ATRStrategy {
    pub fn new(parameters: HashMap<String, f64>) -> Self {
        let raw_period = get_param_f64(&parameters, "period", 14.0);
        let period = raw_period.round().clamp(5.0, 50.0) as usize;
        let min_period_points = raw_period.round().max(50.0) as usize;
        let volatility_threshold = get_param_f64(&parameters, "volatilityThreshold", 1.5);
        let min_confidence = get_param_f64(&parameters, "minConfidence", 0.3);
        Self {
            template_id: "atr".to_string(),
            period,
            min_period_points,
            volatility_threshold,
            min_confidence,
        }
    }
}

impl super::Strategy for ATRStrategy {
    fn get_template_id(&self) -> &str {
        &self.template_id
    }

    fn generate_signal(
        &self,
        _ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> StrategySignal {
        if candles.is_empty() || candle_index < self.period + 5 || candle_index >= candles.len() {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let series = compute_atr_series(candles, self.period);

        // Require at least 5 ATR observations up to current index
        if candle_index < self.period + 4 {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let current_atr = series.atr[candle_index];
        let avg_atr = series.atr_sma5[candle_index];
        if !current_atr.is_finite() || !avg_atr.is_finite() {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let current_candle = &candles[candle_index];
        let prev_close = candles[candle_index - 1].close;
        let current_price = current_candle.close;

        // Current true range for intra-signal logic
        let true_range = (current_candle.high - current_candle.low)
            .max((current_candle.high - prev_close).abs())
            .max((current_candle.low - prev_close).abs());

        // Price momentum
        let price_change = if prev_close != 0.0 {
            (current_price - prev_close) / prev_close
        } else {
            0.0
        };
        let price_change_percent = price_change.abs() * 100.0;

        // Buy: volatility expansion with upward momentum or low-vol breakout
        let low_vol_breakout =
            current_atr < avg_atr * 0.9 && true_range > avg_atr * (self.volatility_threshold * 0.8);
        let high_vol_momentum =
            current_atr > avg_atr * 1.1 && price_change > 0.0 && price_change_percent > 1.0;

        if low_vol_breakout || high_vol_momentum {
            let mut confidence = 0.3f64;
            if low_vol_breakout {
                confidence +=
                    ((avg_atr * self.volatility_threshold - current_atr) / avg_atr * 2.0).min(0.4);
            }
            if high_vol_momentum {
                confidence += (price_change_percent / 5.0).min(0.3);
            }
            confidence = confidence.min(1.0);
            if confidence >= self.min_confidence - 1e-6 {
                return StrategySignal {
                    action: SignalAction::Buy,
                    confidence,
                };
            }
        }

        // Sell: high volatility with downward momentum or extreme volatility
        let high_vol_reversal = current_atr > avg_atr * self.volatility_threshold
            && price_change < 0.0
            && price_change_percent > 1.0;
        let extreme_volatility = current_atr > avg_atr * (self.volatility_threshold * 1.5);

        if high_vol_reversal || extreme_volatility {
            let mut confidence = 0.3f64;
            if high_vol_reversal {
                confidence += (price_change_percent / 5.0).min(0.3);
                confidence +=
                    ((current_atr - avg_atr * self.volatility_threshold) / avg_atr).min(0.2);
            }
            if extreme_volatility {
                confidence +=
                    ((current_atr - avg_atr * self.volatility_threshold) / avg_atr).min(0.4);
            }
            confidence = confidence.min(1.0);
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
        self.min_period_points + 5
    }
}
