use crate::indicators;
use crate::models::*;
use crate::param_utils::{clamp_f64, get_param_f64_clamped, get_param_usize_rounded_clamped};
use std::collections::HashMap;

struct IndicatorSnapshot {
    current: indicators::ADXOutput,
    adx_rising: bool,
    ema_value: Option<f64>,
    weakness_triggered: bool,
}

pub struct ADXStrategy {
    template_id: String,
    period: usize,
    ema_period: usize,
    trend_strength: f64,
    di_diff_min: f64,
    adx_slope_lookback: usize,
    weakness_exit_days: usize,
    min_confidence: f64,
}

impl ADXStrategy {
    pub fn new(parameters: HashMap<String, f64>) -> Self {
        let period = get_param_usize_rounded_clamped(&parameters, "period", 14, 5, 50);
        let ema_period = get_param_usize_rounded_clamped(&parameters, "emaPeriod", 60, 10, 200);
        let trend_raw = parameters
            .get("trendStrength")
            .copied()
            .or_else(|| parameters.get("minAdx").copied())
            .unwrap_or(24.0);
        let trend_strength = clamp_f64(trend_raw, 24.0, 15.0, 50.0);
        let di_diff_min = get_param_f64_clamped(&parameters, "diDiffMin", 5.0, 0.0, 30.0);
        let adx_slope_lookback =
            get_param_usize_rounded_clamped(&parameters, "adxSlopeLookback", 9, 1, 10);
        let weakness_exit_days =
            get_param_usize_rounded_clamped(&parameters, "weaknessExitDays", 15, 0, 20);
        let min_confidence = get_param_f64_clamped(&parameters, "minConfidence", 0.3, 0.0, 1.0);
        Self {
            template_id: "adx".to_string(),
            period,
            ema_period,
            trend_strength,
            di_diff_min,
            adx_slope_lookback,
            weakness_exit_days,
            min_confidence,
        }
    }

    fn collect_indicator_snapshot(
        &self,
        candles: &[Candle],
        candle_index: usize,
        adx_period: usize,
        ema_period: usize,
        adx_slope_lookback: usize,
        weakness_exit_days: usize,
        trend_strength: f64,
    ) -> Option<IndicatorSnapshot> {
        if candle_index < adx_period {
            return None;
        }

        let slice = &candles[..=candle_index];
        let mut highs: Vec<f64> = Vec::with_capacity(slice.len());
        let mut lows: Vec<f64> = Vec::with_capacity(slice.len());
        let mut closes: Vec<f64> = Vec::with_capacity(slice.len());
        for candle in slice {
            highs.push(candle.high);
            lows.push(candle.low);
            closes.push(candle.close);
        }

        let adx_values = indicators::calculate_adx(&highs, &lows, &closes, adx_period);
        let adx_index = candle_index - adx_period;
        if adx_index >= adx_values.len() {
            return None;
        }

        let current = adx_values[adx_index];
        let prev_idx = adx_index.saturating_sub(adx_slope_lookback);
        let was = adx_values[prev_idx];
        let adx_rising = current.adx > was.adx;
        let ema_value = if ema_period == 0 {
            closes.get(candle_index).copied()
        } else {
            let ema_values = indicators::calculate_ema(&closes, ema_period);
            ema_values.get(candle_index).copied()
        };

        let mut weakness_triggered = false;
        if weakness_exit_days > 0 {
            let mut weak_count = 0usize;
            let mut idx = adx_index;
            loop {
                if adx_values[idx].adx < trend_strength {
                    weak_count += 1;
                    if weak_count >= weakness_exit_days {
                        weakness_triggered = true;
                        break;
                    }
                } else {
                    break;
                }
                if idx == 0 {
                    break;
                }
                idx -= 1;
            }
        }

        Some(IndicatorSnapshot {
            current,
            adx_rising,
            ema_value,
            weakness_triggered,
        })
    }
}

impl super::Strategy for ADXStrategy {
    fn get_template_id(&self) -> &str {
        &self.template_id
    }

    fn generate_signal(
        &self,
        _ticker: &str,
        candles: &[Candle],
        candle_index: usize,
    ) -> StrategySignal {
        if candles.is_empty() || candle_index == 0 || candle_index >= candles.len() {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        if candle_index < self.period {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        let snapshot = match self.collect_indicator_snapshot(
            candles,
            candle_index,
            self.period,
            self.ema_period,
            self.adx_slope_lookback,
            self.weakness_exit_days,
            self.trend_strength,
        ) {
            Some(snapshot) => snapshot,
            None => {
                return StrategySignal {
                    action: SignalAction::Hold,
                    confidence: 0.0,
                };
            }
        };

        // EMA trend filter: require price > EMA once enough samples exist
        let has_trend_sample = self.ema_period > 0 && candle_index + 1 >= self.ema_period;
        let trend_state = if has_trend_sample {
            snapshot
                .ema_value
                .map(|ema| candles[candle_index].close > ema)
        } else {
            None
        };

        if snapshot.weakness_triggered {
            return StrategySignal {
                action: SignalAction::Sell,
                confidence: 0.7,
            };
        }

        // Only trade when trend is strong and improving
        if snapshot.current.adx < self.trend_strength || !snapshot.adx_rising {
            return StrategySignal {
                action: SignalAction::Hold,
                confidence: 0.0,
            };
        }

        // Buy: DI+ dominance, strong ADX, and price above EMA (only once EMA is available)
        if snapshot.current.pdi > snapshot.current.mdi
            && (snapshot.current.pdi - snapshot.current.mdi) >= self.di_diff_min
            && trend_state.unwrap_or(false)
        {
            let mut confidence = 0.4
                + (snapshot.current.adx - self.trend_strength) / 50.0
                + (snapshot.current.pdi - snapshot.current.mdi) / 50.0;
            if snapshot.adx_rising {
                confidence += 0.1;
            }
            confidence = confidence.clamp(0.0, 1.0);
            if confidence >= self.min_confidence - 1e-6 {
                return StrategySignal {
                    action: SignalAction::Buy,
                    confidence,
                };
            }
        }

        // Sell: DI- dominance
        if snapshot.current.mdi > snapshot.current.pdi
            && (snapshot.current.mdi - snapshot.current.pdi) >= self.di_diff_min
        {
            let confidence = (0.4
                + (snapshot.current.adx - self.trend_strength) / 50.0
                + (snapshot.current.mdi - snapshot.current.pdi) / 50.0)
                .clamp(0.0, 1.0);
            if confidence >= self.min_confidence - 1e-6 {
                return StrategySignal {
                    action: SignalAction::Sell,
                    confidence,
                };
            }
        }

        // Secondary sell: if trend filter fails (only meaningful when in position; safe to emit)
        if matches!(trend_state, Some(false)) {
            return StrategySignal {
                action: SignalAction::Sell,
                confidence: 0.55,
            };
        }

        StrategySignal {
            action: SignalAction::Hold,
            confidence: 0.0,
        }
    }

    fn get_min_data_points(&self) -> usize {
        std::cmp::max(
            std::cmp::max(self.period * 2, self.ema_period + self.period),
            50,
        )
    }
}
