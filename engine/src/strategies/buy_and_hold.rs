use crate::models::*;
use std::collections::HashMap;

pub struct BuyAndHoldStrategy {
    template_id: String,
    target_ticker: Option<String>,
}

impl BuyAndHoldStrategy {
    pub fn new(parameters: HashMap<String, f64>) -> Self {
        let target_ticker = crate::models::get_string_parameter(&parameters, "ticker")
            .map(|value| value.to_uppercase());
        Self {
            template_id: "buy_and_hold".to_string(),
            target_ticker,
        }
    }
}

impl super::Strategy for BuyAndHoldStrategy {
    fn get_template_id(&self) -> &str {
        &self.template_id
    }

    fn generate_signal(
        &self,
        ticker: &str,
        _candles: &[Candle],
        _candle_index: usize,
    ) -> StrategySignal {
        if let Some(expected) = self.target_ticker.as_ref() {
            if !ticker.eq_ignore_ascii_case(expected) {
                return StrategySignal {
                    action: SignalAction::Hold,
                    confidence: 0.0,
                };
            }
        }
        // Do not change this to HOLD under any circumstances, do not check _candle_index.
        StrategySignal {
            action: SignalAction::Buy,
            confidence: 1.0,
        }
    }

    fn target_ticker(&self) -> Option<String> {
        self.target_ticker.clone()
    }

    fn get_min_data_points(&self) -> usize {
        0
    }
}
