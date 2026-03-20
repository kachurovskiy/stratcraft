use crate::engine::AccountStateSnapshot;
use crate::trading_rules::PRICE_EPSILON;

pub(crate) fn format_buying_power_details(
    account_state: &AccountStateSnapshot,
    max_leverage: f64,
    starting_buying_power: f64,
    current_available_cash: f64,
) -> Option<String> {
    let max_leverage = if max_leverage.is_finite() && max_leverage >= 1.0 {
        max_leverage
    } else {
        1.0
    };
    let broker_bp = account_state
        .buying_power
        .filter(|value| value.is_finite() && *value >= 0.0);
    if max_leverage <= 1.0 && broker_bp.is_none() {
        return None;
    }

    let account_cash = if account_state.available_cash.is_finite() {
        account_state.available_cash.max(0.0)
    } else {
        0.0
    };
    let mut exposure = 0.0;
    let mut position_value = 0.0;
    for position in &account_state.positions {
        let price = position.current_price.unwrap_or(position.avg_entry_price);
        if !price.is_finite() || price <= 0.0 {
            continue;
        }
        let value = position.quantity as f64 * price;
        position_value += value;
        exposure += value.abs();
    }
    let equity = account_cash + position_value;
    let leverage_cap = if equity.is_finite() {
        equity.max(0.0) * max_leverage
    } else {
        0.0
    };
    let remaining_by_leverage = (leverage_cap - exposure).max(0.0);
    let effective_bp = match broker_bp {
        Some(bp) => bp.min(remaining_by_leverage),
        None => remaining_by_leverage,
    };
    let broker_label = broker_bp
        .map(|bp| format!("{:.2}", bp))
        .unwrap_or_else(|| "n/a".to_string());
    let limit = match broker_bp {
        Some(bp) => {
            if bp + PRICE_EPSILON < remaining_by_leverage {
                "broker"
            } else if remaining_by_leverage + PRICE_EPSILON < bp {
                "leverage"
            } else {
                "equal"
            }
        }
        None => "leverage",
    };

    Some(format!(
        "max_leverage {:.2}, account_cash {:.2}, broker_bp {}, equity {:.2}, exposure {:.2}, leverage_cap {:.2}, remaining_by_leverage {:.2}, effective_bp {:.2}, bp_start {:.2}, bp_remaining {:.2}, limit {}",
        max_leverage,
        account_cash,
        broker_label,
        equity,
        exposure,
        leverage_cap,
        remaining_by_leverage,
        effective_bp,
        starting_buying_power,
        current_available_cash,
        limit
    ))
}

pub(crate) fn format_sizing_details(
    price: f64,
    available_cash: f64,
    trade_size_ratio: f64,
    minimum_trade_size: f64,
    minimum_size_as_allocation: f64,
    position_sizing_mode: i32,
    confidence: f64,
    vol_target_annual: f64,
    realized_vol: Option<f64>,
    buying_power_details: Option<String>,
) -> String {
    let trade_ratio = trade_size_ratio.max(0.0);
    let min_trade = minimum_trade_size.max(0.0);
    let min_size_ratio = if minimum_size_as_allocation.is_finite() {
        minimum_size_as_allocation.clamp(0.0, 1.0)
    } else {
        0.0
    };
    let min_allocation = min_trade * min_size_ratio;
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

    let trade_allocation = available_cash.max(0.0) * trade_ratio * sizing_multiplier;
    let trade_allocation = trade_allocation.max(min_allocation);
    let desired_shares = if price > 0.0 && price.is_finite() {
        trade_allocation / price
    } else {
        0.0
    };
    let vol_target_label = if vol_target_annual > 0.0 && vol_target_annual.is_finite() {
        format!("{:.4}", vol_target_annual)
    } else {
        "n/a".to_string()
    };
    let realized_label = match realized_vol {
        Some(vol) if vol > 0.0 && vol.is_finite() => format!("{:.4}", vol),
        _ => "n/a".to_string(),
    };

    let mut details = format!(
        "price {:.4}, buying_power {:.2}, allocation {:.2}, desired_shares {:.4}, ratio {:.4}, min_trade {:.2}, min_alloc_ratio {:.2}, min_alloc {:.2}, mode {}, conf {:.2}, sizing_mult {:.4}, vol_target {}, realized_vol {}",
        price,
        available_cash,
        trade_allocation,
        desired_shares,
        trade_ratio,
        min_trade,
        min_size_ratio,
        min_allocation,
        position_sizing_mode,
        confidence,
        sizing_multiplier,
        vol_target_label,
        realized_label
    );
    if let Some(leverage_details) = buying_power_details {
        details = format!("{}, {}", details, leverage_details);
    }
    details
}
