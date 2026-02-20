use crate::indicators::calculate_atr_from_candles;
use crate::models::Candle;

pub const PRICE_EPSILON: f64 = 1e-6;

pub fn has_minimum_dollar_volume(
    candles: &[&Candle],
    end_index: usize,
    lookback: usize,
    minimum_dollar_volume: f64,
) -> bool {
    if minimum_dollar_volume <= 0.0 || lookback == 0 {
        return true;
    }
    if candles.is_empty() || end_index >= candles.len() {
        return false;
    }
    if end_index + 1 < lookback {
        return false;
    }
    let start_index = end_index + 1 - lookback;
    for idx in start_index..=end_index {
        let candle = candles[idx];
        let usd_volume = candle.high * candle.volume_shares as f64;
        if usd_volume + PRICE_EPSILON < minimum_dollar_volume {
            return false;
        }
    }
    true
}

#[derive(Debug, Clone, PartialEq)]
pub struct PositionAllocation {
    pub quantity: i32,
    pub trade_value: f64,
}

#[derive(Debug, PartialEq)]
pub enum PositionSizingOutcome {
    Sized(PositionAllocation),
    TooSmall,
    InsufficientCash { required: f64 },
}

pub struct PositionSizingParams {
    pub price: f64,
    pub available_cash: f64,
    pub trade_size_ratio: f64,
    pub minimum_trade_size: f64,
    pub position_sizing_mode: i32,
    pub confidence: f64,
    pub vol_target_annual: f64,
    pub realized_vol: Option<f64>,
}

pub fn determine_position_size(params: PositionSizingParams) -> PositionSizingOutcome {
    let PositionSizingParams {
        price,
        available_cash,
        trade_size_ratio,
        minimum_trade_size,
        position_sizing_mode,
        confidence,
        vol_target_annual,
        realized_vol,
    } = params;

    if price <= 0.0 || !price.is_finite() || !available_cash.is_finite() {
        return PositionSizingOutcome::TooSmall;
    }

    let mut sizing_multiplier = 1.0;
    if position_sizing_mode == 1 || position_sizing_mode == 3 {
        let conf = confidence.clamp(0.0, 1.0);
        sizing_multiplier *= conf.max(0.3);
    }

    if (position_sizing_mode == 2 || position_sizing_mode == 3)
        && vol_target_annual > 0.0
        && vol_target_annual.is_finite()
    {
        if let Some(vol) = realized_vol {
            if vol > 0.0 && vol.is_finite() {
                let vol_scale = (vol_target_annual / vol).clamp(0.0, 1.0);
                sizing_multiplier *= if vol_scale.is_finite() {
                    vol_scale
                } else {
                    1.0
                };
            }
        }
    }

    let trade_allocation = available_cash.max(0.0) * trade_size_ratio.max(0.0) * sizing_multiplier;
    let desired_shares = if trade_allocation <= 0.0 {
        0.0
    } else {
        trade_allocation / price
    };
    let mut quantity = desired_shares.floor().max(0.0) as i32;

    let mut trade_value = quantity as f64 * price;

    if quantity > 0 && trade_value < minimum_trade_size {
        quantity = (minimum_trade_size / price).ceil() as i32;
        trade_value = quantity as f64 * price;
    }

    if quantity <= 0 {
        if available_cash + PRICE_EPSILON < price {
            return PositionSizingOutcome::InsufficientCash { required: price };
        }
        return PositionSizingOutcome::TooSmall;
    }

    if trade_value > available_cash + PRICE_EPSILON {
        return PositionSizingOutcome::InsufficientCash {
            required: trade_value,
        };
    }

    PositionSizingOutcome::Sized(PositionAllocation {
        quantity,
        trade_value,
    })
}

pub fn initial_stop_loss(
    stop_loss_mode: i32,
    atr_multiplier: f64,
    atr_period: usize,
    stop_loss_ratio: f64,
    price: f64,
    ticker_candles: &Vec<&Candle>,
    index: usize,
    is_short: bool,
) -> Option<f64> {
    if stop_loss_mode == 1 && atr_multiplier > 0.0 {
        return calculate_atr_from_candles(ticker_candles, index, atr_period).and_then(|atr| {
            if atr > 0.0 && atr.is_finite() {
                if is_short {
                    Some(price + atr_multiplier * atr)
                } else {
                    Some(price - atr_multiplier * atr)
                }
            } else {
                None
            }
        });
    }

    if stop_loss_ratio.is_finite() && stop_loss_ratio > 0.0 && stop_loss_ratio < 1.0 {
        return if is_short {
            Some(price * (1.0 + stop_loss_ratio))
        } else {
            Some(price * (1.0 - stop_loss_ratio))
        };
    }

    None
}

#[derive(Debug, PartialEq)]
pub enum TrailingStopUpdate {
    Atr(f64),
}

impl TrailingStopUpdate {
    pub fn value(&self) -> f64 {
        match self {
            TrailingStopUpdate::Atr(value) => *value,
        }
    }

    pub fn reason(&self) -> &'static str {
        match self {
            TrailingStopUpdate::Atr(_) => "atr_trailing",
        }
    }
}

pub struct TrailingStopParams<'a> {
    pub stop_loss_mode: i32,
    pub atr_multiplier: f64,
    pub atr_period: usize,
    pub ticker_candles: &'a [&'a Candle],
    pub candle_index: usize,
    pub current_candle: &'a Candle,
    pub current_stop: f64,
    pub is_short: bool,
    pub planning_close: Option<f64>,
}

pub fn compute_trailing_stop(params: TrailingStopParams) -> Option<TrailingStopUpdate> {
    let TrailingStopParams {
        stop_loss_mode,
        atr_multiplier,
        atr_period,
        ticker_candles,
        candle_index,
        current_candle,
        current_stop,
        is_short,
        planning_close,
    } = params;

    let reference_close = planning_close.unwrap_or(current_candle.close);

    if stop_loss_mode == 1 && atr_multiplier > 0.0 {
        if let Some(atr) = calculate_atr_from_candles(ticker_candles, candle_index, atr_period) {
            if atr > 0.0 && atr.is_finite() {
                let potential = if is_short {
                    reference_close + atr_multiplier * atr
                } else {
                    reference_close - atr_multiplier * atr
                };
                if (!is_short && potential > current_stop) || (is_short && potential < current_stop)
                {
                    return Some(TrailingStopUpdate::Atr(potential));
                }
            }
        }
    }

    None
}

pub fn stop_loss_exit_price(
    current_candle: &Candle,
    stop_loss: f64,
    is_short: bool,
) -> Option<f64> {
    if !is_short {
        if current_candle.low <= stop_loss {
            if current_candle.open <= stop_loss {
                Some(current_candle.open)
            } else {
                Some(stop_loss)
            }
        } else {
            None
        }
    } else if current_candle.high >= stop_loss {
        if current_candle.open >= stop_loss {
            Some(current_candle.open)
        } else {
            Some(stop_loss)
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use chrono::Utc;

    const TEST_MIN_DOLLAR_VOLUME_FOR_ENTRY: f64 = 150_000.0;
    const TEST_MIN_DOLLAR_VOLUME_LOOKBACK: usize = 5;

    fn candle(date_offset: i64, open: f64, high: f64, low: f64, close: f64, volume: i64) -> Candle {
        Candle {
            ticker: "T".to_string(),
            date: Utc::now() + Duration::days(date_offset),
            open,
            high,
            low,
            close,
            unadjusted_close: Some(close),
            volume_shares: volume,
        }
    }

    #[test]
    fn test_minimum_dollar_volume_check() {
        let liquid: Vec<Candle> = (0..TEST_MIN_DOLLAR_VOLUME_LOOKBACK as i64)
            .map(|offset| candle(offset, 10.0, 10.0, 10.0, 10.0, 20_000))
            .collect();
        let liquid_refs: Vec<&Candle> = liquid.iter().collect();
        assert!(has_minimum_dollar_volume(
            &liquid_refs,
            liquid_refs.len() - 1,
            TEST_MIN_DOLLAR_VOLUME_LOOKBACK,
            TEST_MIN_DOLLAR_VOLUME_FOR_ENTRY
        ));

        let mut weak: Vec<Candle> = (0..TEST_MIN_DOLLAR_VOLUME_LOOKBACK as i64)
            .map(|offset| candle(offset, 10.0, 10.0, 10.0, 10.0, 20_000))
            .collect();
        weak[2].volume_shares = 5_000; // Drops below $150k
        let weak_refs: Vec<&Candle> = weak.iter().collect();
        assert!(!has_minimum_dollar_volume(
            &weak_refs,
            weak_refs.len() - 1,
            TEST_MIN_DOLLAR_VOLUME_LOOKBACK,
            TEST_MIN_DOLLAR_VOLUME_FOR_ENTRY
        ));

        let short_history: Vec<Candle> = (0..4)
            .map(|offset| candle(offset, 10.0, 10.0, 10.0, 10.0, 20_000))
            .collect();
        let short_refs: Vec<&Candle> = short_history.iter().collect();
        assert!(!has_minimum_dollar_volume(
            &short_refs,
            short_refs.len() - 1,
            TEST_MIN_DOLLAR_VOLUME_LOOKBACK,
            TEST_MIN_DOLLAR_VOLUME_FOR_ENTRY
        ));
    }

    #[test]
    fn test_position_size_detects_cash_and_size() {
        let outcome = determine_position_size(PositionSizingParams {
            price: 10.0,
            available_cash: 1000.0,
            trade_size_ratio: 0.5,
            minimum_trade_size: 100.0,
            position_sizing_mode: 0,
            confidence: 1.0,
            vol_target_annual: 0.0,
            realized_vol: None,
        });
        match outcome {
            PositionSizingOutcome::Sized(allocation) => {
                assert_eq!(allocation.quantity, 50);
                assert!((allocation.trade_value - 500.0).abs() < 1e-9);
            }
            _ => panic!("unexpected outcome"),
        }

        let too_small = determine_position_size(PositionSizingParams {
            price: 100.0,
            available_cash: 50.0,
            trade_size_ratio: 0.1,
            minimum_trade_size: 100.0,
            position_sizing_mode: 0,
            confidence: 1.0,
            vol_target_annual: 0.0,
            realized_vol: None,
        });
        assert_eq!(
            too_small,
            PositionSizingOutcome::InsufficientCash { required: 100.0 }
        );

        let insufficient = determine_position_size(PositionSizingParams {
            price: 100.0,
            available_cash: 100.0,
            trade_size_ratio: 1.0,
            minimum_trade_size: 1000.0,
            position_sizing_mode: 0,
            confidence: 1.0,
            vol_target_annual: 0.0,
            realized_vol: None,
        });
        assert!(matches!(
            insufficient,
            PositionSizingOutcome::InsufficientCash { .. }
        ));
    }

    #[test]
    fn test_position_size_too_small_when_allocation_below_one_share() {
        let too_small = determine_position_size(PositionSizingParams {
            price: 100.0,
            available_cash: 1000.0,
            trade_size_ratio: 0.001,
            minimum_trade_size: 10.0,
            position_sizing_mode: 0,
            confidence: 1.0,
            vol_target_annual: 0.0,
            realized_vol: None,
        });
        assert_eq!(too_small, PositionSizingOutcome::TooSmall);
    }

    #[test]
    fn test_position_size_insufficient_cash_for_allocation() {
        let sized = determine_position_size(PositionSizingParams {
            price: 105.0,
            available_cash: 1000.0,
            trade_size_ratio: 2.0,
            minimum_trade_size: 100.0,
            position_sizing_mode: 0,
            confidence: 1.0,
            vol_target_annual: 0.0,
            realized_vol: None,
        });

        assert_eq!(
            sized,
            PositionSizingOutcome::InsufficientCash {
                required: 19.0 * 105.0
            }
        );
    }

    #[test]
    fn test_position_size_applies_minimum_trade_size() {
        let sized = determine_position_size(PositionSizingParams {
            price: 20.0,
            available_cash: 1000.0,
            trade_size_ratio: 0.05,
            minimum_trade_size: 100.0,
            position_sizing_mode: 0,
            confidence: 1.0,
            vol_target_annual: 0.0,
            realized_vol: None,
        });

        match sized {
            PositionSizingOutcome::Sized(allocation) => {
                assert_eq!(allocation.quantity, 5);
                assert!((allocation.trade_value - 100.0).abs() < 1e-9);
            }
            _ => panic!("expected sized allocation"),
        }
    }

    #[test]
    fn test_initial_stop_loss_percent() {
        let candles = vec![candle(0, 10.0, 12.0, 8.0, 11.0, 1000)];
        assert_eq!(
            initial_stop_loss(0, 0.0, 14, 0.1, 10.0, &vec![&candles[0]], 0, false).unwrap(),
            9.0
        );
        assert_eq!(
            initial_stop_loss(0, 0.0, 14, 0.1, 10.0, &vec![&candles[0]], 0, true).unwrap(),
            11.0
        );
    }

    #[test]
    fn test_trailing_stop_atr() {
        let candles = vec![
            candle(0, 10.0, 12.0, 9.0, 11.0, 1000),
            candle(1, 12.0, 16.0, 11.0, 15.0, 1000),
        ];
        let candle_refs = vec![&candles[0], &candles[1]];
        let update = compute_trailing_stop(TrailingStopParams {
            stop_loss_mode: 1,
            atr_multiplier: 1.0,
            atr_period: 2,
            ticker_candles: &candle_refs,
            candle_index: 1,
            current_candle: &candles[1],
            current_stop: 10.0,
            is_short: false,
            planning_close: None,
        })
        .unwrap();
        assert!(matches!(update, TrailingStopUpdate::Atr(_)));
        assert!(update.value() > 10.0);

        let short_update = compute_trailing_stop(TrailingStopParams {
            stop_loss_mode: 1,
            atr_multiplier: 1.0,
            atr_period: 2,
            ticker_candles: &candle_refs,
            candle_index: 1,
            current_candle: candle_refs[1],
            current_stop: 20.0,
            is_short: true,
            planning_close: None,
        })
        .unwrap();
        assert!(short_update.value() < 20.0);
    }

    #[test]
    fn test_stop_loss_exit_price_prefers_open_gap() {
        let mut base = candle(0, 10.0, 12.0, 8.0, 11.0, 1000);
        base.open = 9.0;
        base.close = 10.0;
        base.low = 8.5;
        base.high = 12.0;
        let candle = base;
        assert_eq!(stop_loss_exit_price(&candle, 9.5, false), Some(9.0));
        assert!(stop_loss_exit_price(&candle, 8.0, false).is_none());

        let mut short_candle = candle;
        short_candle.open = 12.5;
        short_candle.high = 12.5;
        short_candle.low = 9.0;
        assert_eq!(stop_loss_exit_price(&short_candle, 12.0, true), Some(12.5));
        short_candle.open = 11.5;
        assert_eq!(stop_loss_exit_price(&short_candle, 12.0, true), Some(12.0));
        assert!(stop_loss_exit_price(&short_candle, 13.0, true).is_none());
    }
}
