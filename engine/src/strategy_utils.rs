use crate::models::StrategySignal;

/// Create a hold signal (default action when no trade signal is generated)
pub fn hold_signal() -> StrategySignal {
    StrategySignal {
        action: crate::models::SignalAction::Hold,
        confidence: 0.0,
    }
}

/// Create a buy signal with the given confidence
pub fn buy_signal(confidence: f64) -> StrategySignal {
    StrategySignal {
        action: crate::models::SignalAction::Buy,
        confidence,
    }
}

/// Create a sell signal with the given confidence
pub fn sell_signal(confidence: f64) -> StrategySignal {
    StrategySignal {
        action: crate::models::SignalAction::Sell,
        confidence,
    }
}

/// Check if confidence meets the minimum threshold
pub fn meets_confidence_threshold(confidence: f64, min_confidence: f64) -> bool {
    confidence >= min_confidence - 1e-6
}

/// Calculate the number of days in the period between two UTC DateTime values
pub fn calculate_period_days_local(
    start: &chrono::DateTime<chrono::Utc>,
    end: &chrono::DateTime<chrono::Utc>,
) -> i64 {
    let start_date = start.date_naive();
    let end_date = end.date_naive();

    if end_date < start_date {
        return 0;
    }

    let diff = (end_date - start_date).num_days();
    if diff <= 0 {
        1
    } else {
        diff
    }
}
