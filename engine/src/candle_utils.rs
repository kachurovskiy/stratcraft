use crate::models::Candle;
use std::collections::HashMap;

/// Groups candles (by reference) keyed by ticker, optionally filtering to a known set.
pub fn group_candles_for_tickers<'a>(
    tickers: &[String],
    candles: &'a [Candle],
) -> HashMap<String, Vec<&'a Candle>> {
    group_candles_by_ticker_with(candles, Some(tickers), |c| Some(c.ticker.clone()))
}

/// Groups every candle in the slice keyed by its native ticker.
pub fn group_all_candles_by_ticker<'a>(candles: &'a [Candle]) -> HashMap<String, Vec<&'a Candle>> {
    group_candles_by_ticker_with(candles, None, |c| Some(c.ticker.clone()))
}

/// Shared implementation for grouping candles keyed by ticker with optional filtering/custom keys.
pub fn group_candles_by_ticker_with<'a, F>(
    candles: &'a [Candle],
    tickers: Option<&[String]>,
    mut key_selector: F,
) -> HashMap<String, Vec<&'a Candle>>
where
    F: FnMut(&Candle) -> Option<String>,
{
    let mut grouped: HashMap<String, Vec<&Candle>> = if let Some(list) = tickers {
        list.iter()
            .map(|ticker| (ticker.clone(), Vec::new()))
            .collect()
    } else {
        HashMap::new()
    };
    let restrict_to_known = tickers.is_some();

    for candle in candles {
        let Some(key) = key_selector(candle) else {
            continue;
        };

        if let Some(bucket) = grouped.get_mut(&key) {
            bucket.push(candle);
            continue;
        }

        if !restrict_to_known {
            grouped.entry(key).or_default().push(candle);
        }
    }

    grouped.retain(|_, values| !values.is_empty());
    for values in grouped.values_mut() {
        values.sort_by(|a, b| a.date.cmp(&b.date));
    }

    grouped
}

/// Clones grouped candle references into owned vectors.
pub fn clone_grouped_candles(
    grouped: &HashMap<String, Vec<&Candle>>,
) -> HashMap<String, Vec<Candle>> {
    grouped
        .iter()
        .map(|(ticker, refs)| {
            (
                ticker.clone(),
                refs.iter().map(|c| (*c).clone()).collect::<Vec<_>>(),
            )
        })
        .collect()
}

/// Normalizes a ticker string by trimming whitespace and uppercasing.
pub fn normalize_ticker_symbol(value: &str) -> Option<String> {
    let normalized = value.trim().to_uppercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

#[cfg(test)]
mod tests {
    use super::group_candles_for_tickers;
    use crate::models::Candle;
    use chrono::{Duration, TimeZone, Utc};

    #[test]
    fn group_candles_filters_and_sorts() {
        let tickers = vec!["AAA".to_string(), "BBB".to_string()];
        let base = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let mut candles = vec![
            Candle {
                ticker: "AAA".to_string(),
                date: base + Duration::days(1),
                open: 101.0,
                high: 105.0,
                low: 99.0,
                close: 104.0,
                unadjusted_close: Some(104.0),
                volume_shares: 1_000,
            },
            Candle {
                ticker: "AAA".to_string(),
                date: base,
                open: 100.0,
                high: 102.0,
                low: 99.0,
                close: 101.0,
                unadjusted_close: Some(101.0),
                volume_shares: 1_000,
            },
            Candle {
                ticker: "ZZZ".to_string(),
                date: base,
                open: 50.0,
                high: 51.0,
                low: 49.0,
                close: 50.5,
                unadjusted_close: Some(50.5),
                volume_shares: 500,
            },
        ];

        candles.swap(0, 1);

        let grouped = group_candles_for_tickers(&tickers, &candles);
        assert_eq!(grouped.len(), 1);
        assert!(grouped.get("BBB").is_none());

        let aaa = grouped.get("AAA").expect("AAA data missing");
        assert_eq!(aaa.len(), 2);
        assert!(aaa[0].date <= aaa[1].date);
    }
}
