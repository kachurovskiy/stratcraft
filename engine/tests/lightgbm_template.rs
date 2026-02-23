use std::collections::HashMap;

use chrono::{Duration, TimeZone, Utc};
use engine::models::{Candle, SignalAction};
use engine::strategy;
use engine::strategy::lightgbm::register_model_text;

fn sample_model_text() -> &'static str {
    "objective=binary sigmoid:1\nnum_class=1\nnum_tree_per_iteration=1\nmax_feature_idx=0\nTree=0\nnum_leaves=2\nsplit_feature=0\nthreshold=0.5\nleft_child=-1\nright_child=-2\nleaf_value=0.1 0.2\nshrinkage=1\n"
}

fn constant_buy_model_text() -> &'static str {
    // LightGBMStrategy computes 51 input features, so max_feature_idx must be 50.
    "objective=binary sigmoid:1\nnum_class=1\nnum_tree_per_iteration=1\nmax_feature_idx=50\nTree=0\nnum_leaves=2\nsplit_feature=0\nthreshold=1000000000\nleft_child=-1\nright_child=-2\nleaf_value=6 -6\nshrinkage=1\n"
}

fn build_candles(ticker: &str, count: usize) -> Vec<Candle> {
    let start = Utc
        .with_ymd_and_hms(2021, 1, 4, 0, 0, 0)
        .single()
        .expect("valid start date");

    (0..count)
        .map(|idx| {
            let t = idx as f64;
            let base = 100.0 + t * 0.05;
            let wiggle = (t / 9.0).sin() * 0.8 + (t / 21.0).cos() * 0.3;
            let close = (base + wiggle).max(1.0);
            let open = (close * (1.0 - 0.002)).max(1.0);
            let high = close.max(open) * 1.004;
            let low = close.min(open) * 0.996;
            Candle {
                ticker: ticker.to_string(),
                date: start + Duration::days(idx as i64),
                open,
                high,
                low,
                close,
                unadjusted_close: None,
                volume_shares: 1_000_000 + idx as i64,
            }
        })
        .collect()
}

#[test]
fn lightgbm_model_templates_register_and_run_without_trainer() {
    let model_text = sample_model_text();
    register_model_text("model-a", model_text, true).expect("register model-a");
    register_model_text("model-b", model_text, false).expect("register model-b");

    let strategy_a = strategy::create_strategy("lightgbm_model-a", HashMap::new())
        .expect("create strategy for model-a");
    let strategy_b = strategy::create_strategy("lightgbm_model-b", HashMap::new())
        .expect("create strategy for model-b");

    let candles: Vec<Candle> = Vec::new();
    let signal_a = strategy_a.generate_signal("AAPL", &candles, 0);
    let signal_b = strategy_b.generate_signal("AAPL", &candles, 0);

    assert!(matches!(signal_a.action, SignalAction::Hold));
    assert!(matches!(signal_b.action, SignalAction::Hold));
}

#[test]
fn lightgbm_end_to_end_fake_trainer_produces_buy_signal() {
    register_model_text("model-e2e", constant_buy_model_text(), true).expect("register model-e2e");

    let strategy = strategy::create_strategy("lightgbm_model-e2e", HashMap::new())
        .expect("create strategy for model-e2e");

    let min_points = strategy.get_min_data_points();
    let candles = build_candles("AAPL", min_points + 5);
    let signal = strategy.generate_signal("AAPL", &candles, candles.len() - 1);

    assert!(matches!(signal.action, SignalAction::Buy));
    assert!(signal.confidence > 0.9, "confidence={}", signal.confidence);
}
