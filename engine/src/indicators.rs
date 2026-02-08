use crate::models::Candle;
pub fn calculate_sma(prices: &[f64], period: usize) -> Vec<f64> {
    if prices.is_empty() {
        return Vec::new();
    }
    if period == 0 {
        return vec![prices[0]; prices.len()];
    }
    if period == 1 {
        return prices.to_vec();
    }
    if prices.len() < period {
        return vec![prices[0]; prices.len()];
    }

    let mut sma_values = Vec::with_capacity(prices.len());
    for _ in 0..period - 1 {
        sma_values.push(prices[0]);
    }

    let mut window_sum: f64 = prices[..period].iter().sum();
    sma_values.push(window_sum / period as f64);
    for i in period..prices.len() {
        window_sum += prices[i] - prices[i - period];
        sma_values.push(window_sum / period as f64);
    }

    sma_values
}

pub fn calculate_ema(prices: &[f64], period: usize) -> Vec<f64> {
    if prices.is_empty() {
        return Vec::new();
    }

    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut ema_values = Vec::new();
    ema_values.push(prices[0]);

    for i in 1..prices.len() {
        let ema = (prices[i] * multiplier) + (ema_values[i - 1] * (1.0 - multiplier));
        ema_values.push(ema);
    }

    ema_values
}

pub fn calculate_macd(
    prices: &[f64],
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let fast_ema = calculate_ema(prices, fast_period);
    let slow_ema = calculate_ema(prices, slow_period);

    let mut macd_line = Vec::new();
    for i in 0..prices.len() {
        macd_line.push(fast_ema[i] - slow_ema[i]);
    }

    let signal_line = calculate_ema(&macd_line, signal_period);

    let mut histogram = Vec::new();
    for i in 0..macd_line.len() {
        histogram.push(macd_line[i] - signal_line[i]);
    }

    (macd_line, signal_line, histogram)
}

#[derive(Clone, Copy, Debug)]
pub struct ADXOutput {
    pub adx: f64,
    pub pdi: f64,
    pub mdi: f64,
}

pub fn calculate_adx(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Vec<ADXOutput> {
    if period == 0 || highs.len() < period + 1 {
        return Vec::new();
    }

    let mut tr_values = Vec::new();
    let mut dm_plus_values = Vec::new();
    let mut dm_minus_values = Vec::new();

    // Calculate True Range and Directional Movement
    for i in 1..highs.len() {
        let tr = (highs[i] - lows[i])
            .max((highs[i] - closes[i - 1]).abs())
            .max((lows[i] - closes[i - 1]).abs());
        tr_values.push(tr);

        let up_move = highs[i] - highs[i - 1];
        let down_move = lows[i - 1] - lows[i];

        let dm_plus = if up_move > down_move && up_move > 0.0 {
            up_move
        } else {
            0.0
        };
        dm_plus_values.push(dm_plus);

        let dm_minus = if down_move > up_move && down_move > 0.0 {
            down_move
        } else {
            0.0
        };
        dm_minus_values.push(dm_minus);
    }

    // Calculate smoothed values
    let mut atr_values = Vec::new();
    let mut di_plus_values = Vec::new();
    let mut di_minus_values = Vec::new();

    let start = period - 1;
    for i in start..tr_values.len() {
        let window_start = i + 1 - period;
        let atr = tr_values[window_start..=i].iter().sum::<f64>() / period as f64;
        atr_values.push(atr);

        let dm_plus_sum = dm_plus_values[window_start..=i].iter().sum::<f64>();
        let dm_minus_sum = dm_minus_values[window_start..=i].iter().sum::<f64>();

        let di_plus = if atr > 0.0 {
            (dm_plus_sum / atr) * 100.0
        } else {
            0.0
        };
        let di_minus = if atr > 0.0 {
            (dm_minus_sum / atr) * 100.0
        } else {
            0.0
        };

        di_plus_values.push(di_plus);
        di_minus_values.push(di_minus);
    }

    // Calculate ADX
    let mut adx_outputs = Vec::new();
    for i in 0..di_plus_values.len() {
        let di_sum = di_plus_values[i] + di_minus_values[i];
        let dx = if di_sum > 0.0 {
            ((di_plus_values[i] - di_minus_values[i]).abs() / di_sum) * 100.0
        } else {
            0.0
        };
        adx_outputs.push(ADXOutput {
            adx: dx,
            pdi: di_plus_values[i],
            mdi: di_minus_values[i],
        });
    }

    adx_outputs
}

fn rsi_from_avgs(avg_gain: f64, avg_loss: f64) -> f64 {
    if avg_loss == 0.0 && avg_gain == 0.0 {
        50.0
    } else if avg_loss == 0.0 {
        100.0
    } else if avg_gain == 0.0 {
        0.0
    } else {
        let rs = avg_gain / avg_loss;
        100.0 - 100.0 / (1.0 + rs)
    }
}

pub fn calculate_rsi(prices: &[f64], period: usize) -> Vec<f64> {
    if prices.is_empty() {
        return Vec::new();
    }
    if period == 0 || prices.len() < period + 1 {
        return vec![50.0; prices.len()];
    }

    let mut rsi_values = vec![50.0; prices.len()];
    let mut sum_gain = 0.0f64;
    let mut sum_loss = 0.0f64;
    for i in 1..=period {
        let delta = prices[i] - prices[i - 1];
        if delta >= 0.0 {
            sum_gain += delta;
        } else {
            sum_loss += -delta;
        }
    }

    let mut avg_gain = sum_gain / period as f64;
    let mut avg_loss = sum_loss / period as f64;
    rsi_values[period] = rsi_from_avgs(avg_gain, avg_loss);

    for i in (period + 1)..prices.len() {
        let delta = prices[i] - prices[i - 1];
        let gain = if delta > 0.0 { delta } else { 0.0 };
        let loss = if delta < 0.0 { -delta } else { 0.0 };
        avg_gain = (avg_gain * (period as f64 - 1.0) + gain) / period as f64;
        avg_loss = (avg_loss * (period as f64 - 1.0) + loss) / period as f64;
        rsi_values[i] = rsi_from_avgs(avg_gain, avg_loss);
    }

    rsi_values
}

pub fn calculate_rsi_at(candles: &[Candle], period: usize, candle_index: usize) -> Option<f64> {
    if period == 0 || candle_index < period || candle_index >= candles.len() {
        return None;
    }
    let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
    calculate_rsi(&closes, period).get(candle_index).copied()
}

pub fn calculate_bollinger_bands(
    prices: &[f64],
    period: usize,
    std_dev: f64,
) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    if period == 0 || prices.len() < period {
        return (Vec::new(), Vec::new(), Vec::new());
    }
    let middle = calculate_sma(prices, period);
    let mut upper = Vec::new();
    let mut lower = Vec::new();

    let start = period - 1;
    for i in start..prices.len() {
        let window_start = i + 1 - period;
        let slice = &prices[window_start..=i];
        let mean = middle[window_start];
        let variance = slice.iter().map(|&val| (val - mean).powi(2)).sum::<f64>() / period as f64;
        let standard_deviation = variance.sqrt();

        upper.push(mean + (std_dev * standard_deviation));
        lower.push(mean - (std_dev * standard_deviation));
    }

    (upper, middle, lower)
}

pub fn calculate_vwap(highs: &[f64], lows: &[f64], closes: &[f64], volumes: &[f64]) -> Vec<f64> {
    let mut vwap_values = Vec::new();
    let mut cumulative_pv = 0.0;
    let mut cumulative_volume = 0.0;

    for i in 0..closes.len() {
        let typical_price = (highs[i] + lows[i] + closes[i]) / 3.0;
        let pv = typical_price * volumes[i];

        cumulative_pv += pv;
        cumulative_volume += volumes[i];

        if cumulative_volume > 0.0 {
            vwap_values.push(cumulative_pv / cumulative_volume);
        } else {
            vwap_values.push(typical_price);
        }
    }

    vwap_values
}

pub fn calculate_atr(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Vec<f64> {
    if period == 0 || highs.len() < period + 1 {
        return Vec::new();
    }

    let mut tr_values = Vec::new();

    // Calculate True Range
    for i in 1..highs.len() {
        let tr = (highs[i] - lows[i])
            .max((highs[i] - closes[i - 1]).abs())
            .max((lows[i] - closes[i - 1]).abs());
        tr_values.push(tr);
    }

    // Calculate ATR as SMA of TR
    let mut atr_values = Vec::new();
    let start = period - 1;
    for i in start..tr_values.len() {
        let window_start = i + 1 - period;
        let atr = tr_values[window_start..=i].iter().sum::<f64>() / period as f64;
        atr_values.push(atr);
    }

    atr_values
}

pub fn calculate_atr_from_candles(candles: &[&Candle], index: usize, period: usize) -> Option<f64> {
    if period == 0 || index == 0 {
        return Some(0.0);
    }
    if index >= candles.len() {
        return None;
    }

    let window = period.saturating_sub(1);
    let start = index.saturating_sub(window);
    if start >= candles.len() {
        return None;
    }

    let mut true_ranges: Vec<f64> = Vec::with_capacity(index - start + 1);
    for i in start..=index {
        let candle = candles[i];
        let high = candle.high;
        let low = candle.low;
        let prev_close = if i > 0 {
            candles[i - 1].close
        } else {
            candle.close
        };
        let tr = (high - low)
            .max((high - prev_close).abs())
            .max((low - prev_close).abs());
        true_ranges.push(tr);
    }

    if true_ranges.is_empty() {
        return Some(0.0);
    }

    Some(true_ranges.iter().sum::<f64>() / true_ranges.len() as f64)
}

pub fn estimate_annualized_volatility_from_candles(
    candles: &[&Candle],
    index: usize,
    lookback: usize,
) -> f64 {
    if index == 0 || lookback < 2 || index >= candles.len() {
        return 0.0;
    }

    let start = index.saturating_sub(lookback - 1).max(1);
    if start > index {
        return 0.0;
    }

    let mut returns: Vec<f64> = Vec::with_capacity(index - start + 1);
    for i in start..=index {
        let prev_close = candles[i - 1].close;
        let current_close = candles[i].close;
        if prev_close > 0.0 {
            returns.push((current_close - prev_close) / prev_close);
        }
    }

    if returns.len() < 2 {
        return 0.0;
    }

    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (returns.len() as f64 - 1.0);

    let daily_std_dev = variance.max(0.0).sqrt();
    daily_std_dev * 252.0_f64.sqrt()
}

#[derive(Clone)]
pub struct SuperTrendOutput {
    pub value: f64,
    pub direction: i32,
}

pub fn calculate_super_trend(
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    period: usize,
    multiplier: f64,
) -> Vec<SuperTrendOutput> {
    if period == 0 {
        return Vec::new();
    }
    let atr = calculate_atr(highs, lows, closes, period);
    let mut result: Vec<SuperTrendOutput> = Vec::new();

    let atr_padded: Vec<f64> = std::iter::repeat_n(f64::NAN, period).chain(atr).collect();

    for i in period..highs.len() {
        let current_atr = atr_padded[i];
        if current_atr.is_nan() {
            result.push(SuperTrendOutput {
                value: 0.0,
                direction: 1,
            });
            continue;
        }

        let median_price = (highs[i] + lows[i]) / 2.0;
        let mut upper_band = median_price + multiplier * current_atr;
        let mut lower_band = median_price - multiplier * current_atr;

        if let Some(prev) = result.last() {
            if prev.direction == 1 {
                // Up trend
                lower_band = lower_band.max(prev.value);
            } else {
                // Down trend
                upper_band = upper_band.min(prev.value);
            }
        }

        let mut direction = 1;
        let mut value = lower_band;

        if let Some(prev) = result.last() {
            if prev.direction == 1 && closes[i] < prev.value {
                direction = -1;
                value = upper_band;
            } else if prev.direction == -1 && closes[i] > prev.value {
                direction = 1;
                value = lower_band;
            } else {
                direction = prev.direction;
                value = if direction == 1 {
                    lower_band
                } else {
                    upper_band
                };
            }
        }

        result.push(SuperTrendOutput { value, direction });
    }

    let padding: Vec<SuperTrendOutput> = std::iter::repeat_n(
        SuperTrendOutput {
            value: 0.0,
            direction: 1,
        },
        highs.len() - result.len(),
    )
    .collect();
    [padding, result].concat()
}

pub struct KeltnerChannelOutput {
    pub upper: Vec<f64>,
    #[allow(dead_code)]
    pub middle: Vec<f64>,
    pub lower: Vec<f64>,
}

pub fn calculate_keltner_channels(
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    period: usize,
    multiplier: f64,
) -> KeltnerChannelOutput {
    if period == 0 {
        return KeltnerChannelOutput {
            upper: Vec::new(),
            middle: Vec::new(),
            lower: Vec::new(),
        };
    }
    let ema_close = calculate_ema(closes, period);
    let atr = calculate_atr(highs, lows, closes, period);

    let atr_padded: Vec<f64> = std::iter::repeat_n(f64::NAN, period).chain(atr).collect();

    let mut upper: Vec<f64> = Vec::new();
    let mut lower: Vec<f64> = Vec::new();

    for i in 0..ema_close.len() {
        if i < atr_padded.len() && !atr_padded[i].is_nan() {
            upper.push(ema_close[i] + atr_padded[i] * multiplier);
            lower.push(ema_close[i] - atr_padded[i] * multiplier);
        } else {
            upper.push(f64::NAN);
            lower.push(f64::NAN);
        }
    }

    KeltnerChannelOutput {
        upper,
        middle: ema_close,
        lower,
    }
}

pub struct SqueezeMomentumOutput {
    pub momentum: f64,
    #[allow(dead_code)]
    pub in_squeeze: bool,
}

pub fn calculate_squeeze_momentum(
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    bb_period: usize,
    bb_multiplier: f64,
    kc_period: usize,
    kc_multiplier: f64,
) -> Vec<SqueezeMomentumOutput> {
    if bb_period == 0 || kc_period == 0 {
        return vec![];
    }
    let (bb_upper, _, bb_lower) = calculate_bollinger_bands(closes, bb_period, bb_multiplier);
    let kc = calculate_keltner_channels(highs, lows, closes, kc_period, kc_multiplier);

    let mut result: Vec<SqueezeMomentumOutput> = Vec::new();

    for i in 0..closes.len() {
        if i < bb_upper.len() && i < kc.upper.len() {
            let in_squeeze = bb_upper[i] < kc.upper[i] && bb_lower[i] > kc.lower[i];

            let momentum_slice = &closes[i.saturating_sub(bb_period.saturating_sub(1))..=i];
            let momentum = if momentum_slice.len() == bb_period {
                let sma: f64 = momentum_slice.iter().sum::<f64>() / bb_period as f64;
                closes[i] - sma
            } else {
                0.0
            };
            result.push(SqueezeMomentumOutput {
                momentum,
                in_squeeze,
            });
        } else {
            result.push(SqueezeMomentumOutput {
                momentum: 0.0,
                in_squeeze: false,
            });
        }
    }

    result
}

pub fn calculate_obv(closes: &[f64], volumes: &[f64]) -> Vec<f64> {
    let mut obv_values = vec![0.0; closes.len()];
    for i in 1..closes.len() {
        if closes[i] > closes[i - 1] {
            obv_values[i] = obv_values[i - 1] + volumes[i];
        } else if closes[i] < closes[i - 1] {
            obv_values[i] = obv_values[i - 1] - volumes[i];
        } else {
            obv_values[i] = obv_values[i - 1];
        }
    }
    obv_values
}

pub fn calculate_adl(highs: &[f64], lows: &[f64], closes: &[f64], volumes: &[f64]) -> Vec<f64> {
    let mut adl_values = vec![0.0; closes.len()];
    for i in 0..closes.len() {
        let money_flow_multiplier = if highs[i] - lows[i] > 0.0 {
            ((closes[i] - lows[i]) - (highs[i] - closes[i])) / (highs[i] - lows[i])
        } else {
            0.0
        };
        let money_flow_volume = money_flow_multiplier * volumes[i];
        adl_values[i] = if i > 0 {
            adl_values[i - 1] + money_flow_volume
        } else {
            money_flow_volume
        };
    }
    adl_values
}

pub fn calculate_mfi(
    highs: &[f64],
    lows: &[f64],
    closes: &[f64],
    volumes: &[f64],
    period: usize,
) -> Vec<f64> {
    let mut typical_prices = Vec::new();
    for i in 0..closes.len() {
        typical_prices.push((highs[i] + lows[i] + closes[i]) / 3.0);
    }

    let mut money_flows = Vec::new();
    for i in 0..closes.len() {
        money_flows.push(typical_prices[i] * volumes[i]);
    }

    let mut mfi_values = vec![50.0; closes.len()];
    for (i, mfi_value) in mfi_values
        .iter_mut()
        .enumerate()
        .take(closes.len())
        .skip(period)
    {
        let mut positive_money_flow = 0.0;
        let mut negative_money_flow = 0.0;
        let window_start = i + 1 - period;
        for j in window_start..=i {
            if typical_prices[j] > typical_prices[j - 1] {
                positive_money_flow += money_flows[j];
            } else {
                negative_money_flow += money_flows[j];
            }
        }

        if negative_money_flow > 0.0 {
            let money_ratio = positive_money_flow / negative_money_flow;
            *mfi_value = 100.0 - (100.0 / (1.0 + money_ratio));
        } else {
            *mfi_value = 100.0;
        }
    }
    mfi_values
}

#[derive(Clone)]
pub struct AtrCacheEntry {
    pub period: usize,
    pub length: usize,
    pub atr: Vec<f64>,
    pub atr_sma5: Vec<f64>,
}

pub fn compute_atr_series(candles: &[Candle], period: usize) -> AtrCacheEntry {
    let n = candles.len();
    let mut atr = vec![f64::NAN; n];
    let mut atr_sma5 = vec![f64::NAN; n];

    if n > 0 {
        // Compute Wilder ATR aligned to candle indices
        let mut tr_sum = 0.0f64;
        let mut prev_close = candles[0].close;
        for i in 1..n {
            let c = &candles[i];
            let high_low = c.high - c.low;
            let high_prev = (c.high - prev_close).abs();
            let low_prev = (c.low - prev_close).abs();
            let tr = high_low.max(high_prev).max(low_prev);

            if i <= period {
                tr_sum += tr;
                if i == period {
                    atr[i] = tr_sum / period as f64;
                }
            } else {
                let prev_atr = atr[i - 1];
                atr[i] = ((prev_atr * (period as f64 - 1.0)) + tr) / period as f64;
            }
            prev_close = c.close;
        }

        // 5-period SMA of ATR where valid (from index >= period + 4)
        // Mirror the TS logic exactly using an explicit in-window counter,
        // which makes behavior robust even if NaNs appear at the edges.
        let mut sma_window_sum = 0.0f64;
        let mut count_in_window = 0usize;
        for i in 0..n {
            let v = atr[i];
            if v.is_finite() {
                sma_window_sum += v;
                count_in_window += 1;
            }
            let drop_index = i as isize - 5;
            if drop_index >= 0 {
                let dropped = atr[drop_index as usize];
                if dropped.is_finite() {
                    sma_window_sum -= dropped;
                    count_in_window -= 1;
                }
            }
            if i >= period + 4 {
                // From this point onward we should always have 5 valid ATR values in the window
                // (indices i-4..=i). Divide by 5 to align with TS implementation.
                if count_in_window == 5 {
                    atr_sma5[i] = sma_window_sum / 5.0;
                }
            }
        }
    }

    let entry = AtrCacheEntry {
        period,
        length: n,
        atr,
        atr_sma5,
    };
    entry
}
