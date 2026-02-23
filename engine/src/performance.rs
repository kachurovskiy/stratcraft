use crate::models::*;
use chrono::{DateTime, Utc};
use statrs::statistics::Statistics;
use std::cmp::Ordering;

pub struct PerformanceCalculator;

impl PerformanceCalculator {
    pub fn calculate_performance(
        trades: &[Trade],
        initial_capital: f64,
        final_portfolio_value: f64,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
        daily_snapshots: &[BacktestDataPoint],
    ) -> StrategyPerformance {
        let executed_trades: Vec<&Trade> = trades.iter().collect();
        let total_trades = executed_trades.len() as i32;

        let mut trade_pnls = Vec::with_capacity(executed_trades.len());
        let mut trade_pnl_percents = Vec::with_capacity(executed_trades.len());
        let mut winning_trade_pnls = Vec::new();
        let mut winning_trade_percents = Vec::new();
        let mut losing_trade_pnls = Vec::new();
        let mut losing_trade_percents = Vec::new();

        for trade in executed_trades.iter().copied() {
            let pnl = trade.pnl.unwrap_or(0.0);
            let exposure = (trade.price * trade.quantity as f64).abs();
            let pnl_percent = if exposure > 0.0 {
                (pnl / exposure) * 100.0
            } else {
                0.0
            };

            trade_pnls.push(pnl);
            trade_pnl_percents.push(pnl_percent);

            if pnl > 0.0 {
                winning_trade_pnls.push(pnl);
                winning_trade_percents.push(pnl_percent);
            } else if pnl < 0.0 {
                losing_trade_pnls.push(pnl);
                losing_trade_percents.push(pnl_percent);
            }
        }
        let winning_trades = winning_trade_pnls.len() as i32;
        let losing_trades = losing_trade_pnls.len() as i32;
        let win_rate = if total_trades > 0 {
            winning_trades as f64 / total_trades as f64
        } else {
            0.0
        };

        let final_portfolio_value = if final_portfolio_value.is_finite() {
            final_portfolio_value
        } else {
            daily_snapshots
                .last()
                .map(|s| s.portfolio_value)
                .unwrap_or(initial_capital)
        };

        let total_return = if final_portfolio_value.is_finite() {
            final_portfolio_value - initial_capital
        } else {
            0.0
        };

        let cagr =
            Self::calculate_cagr(initial_capital, final_portfolio_value, start_date, end_date);
        let sharpe_ratio = Self::calculate_sharpe_ratio(daily_snapshots);
        let drawdown_info = Self::calculate_max_drawdown(daily_snapshots);
        let calmar_ratio = Self::calculate_calmar_ratio(cagr, drawdown_info.max_drawdown_percent);

        let avg_trade_pnl = Self::average(&trade_pnls);
        let best_trade = if trade_pnls.is_empty() {
            0.0
        } else {
            trade_pnls.iter().copied().fold(f64::NEG_INFINITY, f64::max)
        };
        let best_trade = if best_trade.is_finite() {
            best_trade
        } else {
            0.0
        };

        let worst_trade = if trade_pnls.is_empty() {
            0.0
        } else {
            trade_pnls.iter().copied().fold(f64::INFINITY, f64::min)
        };
        let worst_trade = if worst_trade.is_finite() {
            worst_trade
        } else {
            0.0
        };

        let total_tickers = executed_trades
            .iter()
            .map(|t| &t.ticker)
            .collect::<std::collections::HashSet<_>>()
            .len() as i32;

        let trade_durations: Vec<f64> = executed_trades
            .iter()
            .filter_map(|t| {
                if let (Some(entry_date), Some(exit_date)) = (Some(t.date), t.exit_date) {
                    Some((exit_date - entry_date).num_days() as f64)
                } else {
                    None
                }
            })
            .collect();

        let median_trade_duration = Self::median(&trade_durations);
        let avg_trade_duration = Self::average(&trade_durations);

        let median_trade_pnl = Self::median(&trade_pnls);
        let median_trade_pnl_percent = Self::median(&trade_pnl_percents);
        let avg_trade_pnl_percent = Self::average(&trade_pnl_percents);

        let concurrent_trades: Vec<i32> = daily_snapshots
            .iter()
            .map(|s| s.concurrent_trades)
            .collect();
        let median_concurrent_trades = if !concurrent_trades.is_empty() {
            let mut sorted = concurrent_trades.clone();
            sorted.sort();
            sorted[sorted.len() / 2] as f64
        } else {
            0.0
        };

        let avg_concurrent_trades = if !concurrent_trades.is_empty() {
            concurrent_trades.iter().sum::<i32>() as f64 / concurrent_trades.len() as f64
        } else {
            0.0
        };

        let avg_losing_pnl = Self::average(&losing_trade_pnls);
        let avg_losing_pnl_percent = Self::average(&losing_trade_percents);

        let avg_winning_pnl = Self::average(&winning_trade_pnls);
        let avg_winning_pnl_percent = Self::average(&winning_trade_percents);

        StrategyPerformance {
            total_trades,
            winning_trades,
            losing_trades,
            win_rate,
            total_return,
            cagr,
            sharpe_ratio,
            calmar_ratio,
            max_drawdown: drawdown_info.max_drawdown,
            max_drawdown_percent: drawdown_info.max_drawdown_percent,
            avg_trade_return: avg_trade_pnl,
            best_trade,
            worst_trade,
            total_tickers,
            median_trade_duration,
            median_trade_pnl,
            median_trade_pnl_percent,
            median_concurrent_trades,
            avg_trade_duration,
            avg_trade_pnl,
            avg_trade_pnl_percent,
            avg_concurrent_trades,
            avg_losing_pnl,
            avg_losing_pnl_percent,
            avg_winning_pnl,
            avg_winning_pnl_percent,
            last_updated: Utc::now(),
        }
    }

    fn calculate_cagr(
        initial_capital: f64,
        final_portfolio_value: f64,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> f64 {
        if initial_capital <= 0.0 || !final_portfolio_value.is_finite() {
            return 0.0;
        }

        if end_date <= start_date {
            return 0.0;
        }

        let duration = end_date - start_date;
        let years = duration.num_seconds() as f64 / (365.25_f64 * 24.0 * 60.0 * 60.0);

        if years <= 0.0 {
            return 0.0;
        }

        let total_return_ratio = final_portfolio_value / initial_capital;
        if total_return_ratio <= 0.0 {
            return -1.0;
        }

        total_return_ratio.powf(1.0 / years) - 1.0
    }

    fn average(values: &[f64]) -> f64 {
        let mut sum = 0.0;
        let mut count = 0usize;

        for value in values.iter().copied() {
            if value.is_finite() {
                sum += value;
                count += 1;
            }
        }

        if count == 0 {
            0.0
        } else {
            sum / count as f64
        }
    }

    fn median(values: &[f64]) -> f64 {
        let mut filtered: Vec<f64> = values
            .iter()
            .copied()
            .filter(|value| value.is_finite())
            .collect();

        if filtered.is_empty() {
            return 0.0;
        }

        filtered.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let mid = filtered.len() / 2;

        if filtered.len().is_multiple_of(2) {
            (filtered[mid - 1] + filtered[mid]) / 2.0
        } else {
            filtered[mid]
        }
    }

    pub fn calculate_sharpe_ratio(daily_snapshots: &[BacktestDataPoint]) -> f64 {
        if daily_snapshots.len() < 2 {
            return 0.0;
        }

        let returns: Vec<f64> = daily_snapshots
            .windows(2)
            .map(|window| {
                let prev_value = window[0].portfolio_value;
                let curr_value = window[1].portfolio_value;
                if prev_value > 0.0 {
                    (curr_value - prev_value) / prev_value
                } else {
                    0.0
                }
            })
            .collect();

        if returns.is_empty() {
            return 0.0;
        }

        let mean_return = returns.clone().mean();
        let std_dev = returns.std_dev();

        if std_dev == 0.0 {
            return 0.0;
        }

        // Annualize the Sharpe ratio (assuming daily returns)
        let annualized_return = mean_return * 252.0;
        let annualized_volatility = std_dev * (252.0_f64).sqrt();
        let risk_free_rate = 0.02; // 2% risk-free rate

        (annualized_return - risk_free_rate) / annualized_volatility
    }

    fn calculate_calmar_ratio(cagr: f64, max_drawdown_percent: f64) -> f64 {
        if !cagr.is_finite() || !max_drawdown_percent.is_finite() {
            return 0.0;
        }

        let drawdown_ratio = (max_drawdown_percent / 100.0).abs();
        if drawdown_ratio <= f64::EPSILON {
            return 0.0;
        }

        cagr / drawdown_ratio
    }

    fn calculate_max_drawdown(daily_snapshots: &[BacktestDataPoint]) -> DrawdownInfo {
        if daily_snapshots.is_empty() {
            return DrawdownInfo {
                max_drawdown: 0.0,
                max_drawdown_percent: 0.0,
            };
        }

        let mut max_drawdown = 0.0;
        let mut max_drawdown_percent = 0.0;
        let mut peak_value = daily_snapshots[0].portfolio_value;

        for snapshot in daily_snapshots {
            if snapshot.portfolio_value > peak_value {
                peak_value = snapshot.portfolio_value;
            } else {
                let drawdown = peak_value - snapshot.portfolio_value;
                let drawdown_percent = if peak_value > 0.0 {
                    (drawdown / peak_value) * 100.0
                } else {
                    0.0
                };

                if drawdown > max_drawdown {
                    max_drawdown = drawdown;
                }
                if drawdown_percent > max_drawdown_percent {
                    max_drawdown_percent = drawdown_percent;
                }
            }
        }

        DrawdownInfo {
            max_drawdown,
            max_drawdown_percent,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(dead_code)]

    use super::*;
    use chrono::TimeZone;

    #[test]
    fn calculates_total_return_and_cagr_using_backtest_dates() {
        let start_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let end_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        let daily_snapshots = vec![
            BacktestDataPoint {
                date: start_date,
                portfolio_value: 100_000.0,
                cash: 100_000.0,
                positions_value: 0.0,
                concurrent_trades: 0,
                missed_trades_due_to_cash: 0,
            },
            BacktestDataPoint {
                date: end_date,
                portfolio_value: 121_000.0,
                cash: 121_000.0,
                positions_value: 0.0,
                concurrent_trades: 0,
                missed_trades_due_to_cash: 0,
            },
        ];

        let performance = PerformanceCalculator::calculate_performance(
            &[],
            100_000.0,
            121_000.0,
            start_date,
            end_date,
            &daily_snapshots,
        );

        assert!((performance.total_return - 21_000.0).abs() < 1e-9);
        let duration_seconds = (end_date - start_date).num_seconds() as f64;
        let years = duration_seconds / (365.25_f64 * 24.0 * 60.0 * 60.0);
        let total_return_ratio = 121_000.0_f64 / 100_000.0_f64;
        let expected_cagr = total_return_ratio.powf(1.0_f64 / years) - 1.0_f64;
        assert!((performance.cagr - expected_cagr).abs() < 1e-9);
    }

    #[test]
    fn computes_trade_percentages_relative_to_exposure() {
        let start_date = Utc.with_ymd_and_hms(2022, 6, 1, 0, 0, 0).unwrap();
        let mid_date = Utc.with_ymd_and_hms(2022, 6, 10, 0, 0, 0).unwrap();
        let end_date = Utc.with_ymd_and_hms(2022, 6, 30, 0, 0, 0).unwrap();

        let trades = vec![
            Trade {
                id: "t1".to_string(),
                strategy_id: "s1".to_string(),
                ticker: "AAA".to_string(),
                quantity: 10,
                price: 100.0,
                date: start_date,
                status: TradeStatus::Closed,
                pnl: Some(100.0),
                fee: None,
                exit_price: Some(110.0),
                exit_date: Some(mid_date),
                stop_loss: None,
                stop_loss_triggered: Some(false),
                entry_order_id: None,
                entry_cancel_after: None,
                stop_order_id: None,
                exit_order_id: None,
                changes: Vec::new(),
            },
            Trade {
                id: "t2".to_string(),
                strategy_id: "s1".to_string(),
                ticker: "BBB".to_string(),
                quantity: 10,
                price: 50.0,
                date: start_date,
                status: TradeStatus::Closed,
                pnl: Some(-25.0),
                fee: None,
                exit_price: Some(47.5),
                exit_date: Some(end_date),
                stop_loss: None,
                stop_loss_triggered: Some(false),
                entry_order_id: None,
                entry_cancel_after: None,
                stop_order_id: None,
                exit_order_id: None,
                changes: Vec::new(),
            },
        ];

        let daily_snapshots = vec![
            BacktestDataPoint {
                date: start_date,
                portfolio_value: 10_000.0,
                cash: 10_000.0,
                positions_value: 0.0,
                concurrent_trades: 0,
                missed_trades_due_to_cash: 0,
            },
            BacktestDataPoint {
                date: end_date,
                portfolio_value: 10_050.0,
                cash: 10_050.0,
                positions_value: 0.0,
                concurrent_trades: 0,
                missed_trades_due_to_cash: 0,
            },
        ];

        let performance = PerformanceCalculator::calculate_performance(
            &trades,
            10_000.0,
            10_050.0,
            start_date,
            end_date,
            &daily_snapshots,
        );

        assert!((performance.avg_trade_pnl_percent - 2.5).abs() < 1e-9);
        assert!((performance.median_trade_pnl_percent - 2.5).abs() < 1e-9);
        assert!((performance.avg_winning_pnl_percent - 10.0).abs() < 1e-9);
        assert!((performance.avg_losing_pnl_percent + 5.0).abs() < 1e-9);
        assert!((performance.total_return - 50.0).abs() < 1e-9);
    }
}
