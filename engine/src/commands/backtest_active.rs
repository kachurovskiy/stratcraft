use crate::backtester::StrategySelection;
use crate::candle_utils::{group_candles_for_tickers, normalize_ticker_symbol};
use crate::config::EngineConfig;
use crate::context::{AppContext, EngineContext};
use crate::database::Database;
use crate::models::{
    BacktestDataPoint, BacktestResult, Candle, StrategyStateSnapshot, Trade, TradeStatus,
};
use crate::performance::PerformanceCalculator;
use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use log::{info, warn};
use serde_json::json;
use std::collections::{HashMap, HashSet};

const LIVE_TICKER_SCOPE: &str = "live";

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum BacktestScope {
    Validation,
    Training,
    All,
}

impl BacktestScope {
    fn label(self) -> &'static str {
        match self {
            BacktestScope::Validation => "validation",
            BacktestScope::Training => "training",
            BacktestScope::All => "all",
        }
    }

    async fn build_context(self, app: &AppContext) -> Result<EngineContext> {
        match self {
            BacktestScope::Validation => app.engine_context_validation_tickers().await,
            BacktestScope::Training => app.engine_context_training_tickers().await,
            BacktestScope::All => app.engine_context_all_tickers().await,
        }
    }
}

pub async fn run(app: &AppContext, scope: BacktestScope, months: &[u32]) -> Result<()> {
    let mut context = scope.build_context(app).await.map_err(|error| {
        warn!(
            "Unable to initialize {} ticker backtest context: {}",
            scope.label(),
            error
        );
        error
    })?;

    for month in months {
        info!(
            "Running {} backtest for active strategies (window: {}m)",
            scope.label(),
            month,
        );
        context
            .backtester()
            .run_with_selection(Some(*month), StrategySelection::WithoutAccounts)
            .await?;
    }
    info!(
        "Completed {} backtesting run for active strategies",
        scope.label()
    );

    if let Err(err) = refresh_live_backtests(app).await {
        warn!("Failed to refresh live backtests: {}", err);
    }

    Ok(())
}

async fn refresh_live_backtests(app: &AppContext) -> Result<()> {
    let mut db = app.database().await?;
    let candidates = db.get_live_trades_with_accounts().await?;
    if candidates.is_empty() {
        return Ok(());
    }

    let mut strategy_ids: HashSet<String> = HashSet::new();

    for candidate in candidates {
        strategy_ids.insert(candidate.trade.strategy_id.clone());
    }

    update_live_backtests(&mut db, &strategy_ids).await
}

pub async fn update_live_backtests(
    db: &mut Database,
    strategy_ids: &HashSet<String>,
) -> Result<()> {
    let now = Utc::now();

    for strategy_id in strategy_ids {
        let Some(strategy) = db.get_strategy_config(strategy_id).await? else {
            warn!(
                "Skipping live backtest update - strategy {} not found",
                strategy_id
            );
            continue;
        };

        let trades = db.get_strategy_live_trades(strategy_id).await?;
        let mut evaluated = prepare_live_trades_for_backtest(&trades);
        if evaluated.is_empty() {
            continue;
        }

        let initial_capital = EngineConfig::from_parameters(&strategy.parameters).initial_capital;

        evaluated.sort_by(|a, b| a.date.cmp(&b.date).then(a.id.cmp(&b.id)));

        let tickers = collect_trade_tickers(&evaluated);
        let candles = db.get_candles_for_tickers(&tickers).await?;
        let candles_by_ticker = group_candles_for_tickers(&tickers, &candles);

        let (start_date, end_date) =
            determine_live_backtest_window(&evaluated, &candles_by_ticker, now);
        let start_date = normalize_trade_date(start_date);
        let end_date = normalize_trade_date(end_date);

        if end_date < start_date {
            continue;
        }

        evaluated.retain(|trade| trade.date <= end_date);
        if evaluated.is_empty() {
            continue;
        }

        let dates = build_live_backtest_dates(&evaluated, &candles_by_ticker, start_date, end_date);
        let snapshots =
            build_live_snapshots(&evaluated, initial_capital, &dates, &candles_by_ticker);
        let actual_start_date = snapshots
            .first()
            .map(|snapshot| snapshot.date)
            .unwrap_or(start_date);
        let last_prices = latest_prices_for_date(&candles_by_ticker, end_date);
        apply_mark_to_market_pnl(&mut evaluated, &last_prices);
        let final_portfolio_value = snapshots
            .last()
            .map(|snapshot| snapshot.portfolio_value)
            .unwrap_or(initial_capital);
        let performance = PerformanceCalculator::calculate_performance(
            &evaluated,
            initial_capital,
            final_portfolio_value,
            actual_start_date,
            end_date,
            &snapshots,
        );

        let (active_count, closed_count) = count_trade_statuses(&evaluated);
        let strategy_state = StrategyStateSnapshot {
            template_id: "live_trades".to_string(),
            data: json!({
                "source": "alpaca",
                "reconciledAt": now.to_rfc3339(),
                "activeTrades": active_count,
                "closedTrades": closed_count,
            }),
        };

        let result = BacktestResult {
            id: format!("live_{}", strategy_id),
            strategy_id: strategy_id.clone(),
            start_date: actual_start_date,
            end_date,
            initial_capital,
            final_portfolio_value,
            performance,
            daily_snapshots: snapshots,
            trades: Vec::new(),
            tickers,
            ticker_scope: Some(LIVE_TICKER_SCOPE.to_string()),
            strategy_state: Some(strategy_state),
            created_at: now,
        };

        db.replace_strategy_backtest_data(strategy_id, &result, None, LIVE_TICKER_SCOPE)
            .await?;
        db.link_live_trades_to_backtest(strategy_id, &result.id)
            .await?;
    }

    Ok(())
}

fn prepare_live_trades_for_backtest(trades: &[Trade]) -> Vec<Trade> {
    let mut evaluated = Vec::new();

    for trade in trades {
        if !matches!(trade.status, TradeStatus::Active | TradeStatus::Closed) {
            continue;
        }
        let mut candidate = trade.clone();
        let Some(normalized) = normalize_ticker_symbol(&candidate.ticker) else {
            continue;
        };
        candidate.ticker = normalized;
        if candidate.status == TradeStatus::Closed && candidate.pnl.is_none() {
            candidate.pnl = compute_closed_trade_pnl(&candidate);
        }
        if candidate.status == TradeStatus::Closed && candidate.pnl.is_none() {
            candidate.pnl = Some(0.0);
        }
        evaluated.push(candidate);
    }

    evaluated
}

fn compute_closed_trade_pnl(trade: &Trade) -> Option<f64> {
    trade.exit_price.map(|exit_price| {
        let mut pnl = (exit_price - trade.price) * trade.quantity as f64;
        if let Some(fee) = trade.fee {
            pnl -= fee;
        }
        pnl
    })
}

fn determine_live_backtest_window(
    trades: &[Trade],
    candles_by_ticker: &HashMap<String, Vec<&Candle>>,
    fallback: DateTime<Utc>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let start_date = trades
        .iter()
        .map(|trade| trade.date)
        .min()
        .unwrap_or(fallback);

    let last_trade_date = trades
        .iter()
        .map(|trade| trade.exit_date.unwrap_or(trade.date))
        .max()
        .unwrap_or(start_date);

    let last_candle_date = candles_by_ticker
        .values()
        .filter_map(|candles| candles.last().map(|c| c.date))
        .max();

    let has_active = trades
        .iter()
        .any(|trade| trade.status == TradeStatus::Active);

    let mut end_date = if let Some(last_candle_date) = last_candle_date {
        last_candle_date
    } else if has_active {
        fallback
    } else {
        last_trade_date
    };

    if end_date < last_trade_date {
        end_date = last_trade_date;
    }

    (start_date, end_date)
}

fn build_live_snapshots(
    trades: &[Trade],
    initial_capital: f64,
    dates: &[DateTime<Utc>],
    candles_by_ticker: &HashMap<String, Vec<&Candle>>,
) -> Vec<BacktestDataPoint> {
    if dates.is_empty() {
        return Vec::new();
    }

    struct LiveTradeWindow<'a> {
        trade: &'a Trade,
        entry_date: DateTime<Utc>,
        exit_date: Option<DateTime<Utc>>,
    }

    let trade_windows: Vec<LiveTradeWindow> = trades
        .iter()
        .map(|trade| LiveTradeWindow {
            trade,
            entry_date: normalize_trade_date(trade.date),
            exit_date: trade.exit_date.map(normalize_trade_date),
        })
        .collect();

    let mut ticker_cursors: HashMap<String, usize> = candles_by_ticker
        .keys()
        .map(|ticker| (ticker.clone(), 0))
        .collect();
    let mut latest_price_by_ticker: HashMap<String, f64> = HashMap::new();

    let mut snapshots = Vec::with_capacity(dates.len());
    for date in dates {
        for (ticker, candles) in candles_by_ticker {
            if let Some(cursor) = ticker_cursors.get_mut(ticker) {
                while *cursor < candles.len() && candles[*cursor].date <= *date {
                    latest_price_by_ticker.insert(ticker.clone(), candles[*cursor].close);
                    *cursor += 1;
                }
            }
        }

        let mut positions_value = 0.0;
        let mut total_pnl = 0.0;
        let mut concurrent_trades = 0;

        for window in &trade_windows {
            if *date < window.entry_date {
                continue;
            }

            let is_closed = if let Some(exit_date) = window.exit_date {
                *date >= exit_date
            } else {
                window.trade.status != TradeStatus::Active
            };

            if is_closed {
                total_pnl += window.trade.pnl.unwrap_or(0.0);
                continue;
            }

            concurrent_trades += 1;
            let current_price = latest_price_by_ticker
                .get(&window.trade.ticker)
                .copied()
                .unwrap_or(window.trade.price);
            let pnl = (current_price - window.trade.price) * window.trade.quantity as f64;
            total_pnl += pnl;
            positions_value += current_price * window.trade.quantity as f64;
        }

        let portfolio_value = initial_capital + total_pnl;
        let cash = portfolio_value - positions_value;

        snapshots.push(BacktestDataPoint {
            date: *date,
            portfolio_value,
            cash,
            positions_value,
            concurrent_trades,
            missed_trades_due_to_cash: 0,
        });
    }

    snapshots
}

fn build_live_backtest_dates(
    trades: &[Trade],
    candles_by_ticker: &HashMap<String, Vec<&Candle>>,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Vec<DateTime<Utc>> {
    let mut dates: Vec<DateTime<Utc>> = candles_by_ticker
        .values()
        .flat_map(|candles| candles.iter().map(|c| c.date))
        .filter(|date| *date >= start_date && *date <= end_date)
        .collect();

    for trade in trades {
        let entry = normalize_trade_date(trade.date);
        if entry >= start_date && entry <= end_date {
            dates.push(entry);
        }
        if let Some(exit_date) = trade.exit_date.map(normalize_trade_date) {
            if exit_date >= start_date && exit_date <= end_date {
                dates.push(exit_date);
            }
        }
    }

    dates.push(start_date);
    dates.push(end_date);
    dates.sort();
    dates.dedup();
    dates
}

fn latest_prices_for_date(
    candles_by_ticker: &HashMap<String, Vec<&Candle>>,
    target_date: DateTime<Utc>,
) -> HashMap<String, f64> {
    let mut latest = HashMap::new();
    for (ticker, candles) in candles_by_ticker {
        if let Some(candle) = candles.iter().rev().find(|c| c.date <= target_date) {
            latest.insert(ticker.clone(), candle.close);
        }
    }
    latest
}

fn apply_mark_to_market_pnl(trades: &mut [Trade], last_close_by_ticker: &HashMap<String, f64>) {
    for trade in trades {
        if trade.status != TradeStatus::Active {
            continue;
        }
        if let Some(close) = last_close_by_ticker.get(&trade.ticker) {
            trade.pnl = Some((close - trade.price) * trade.quantity as f64);
        } else if trade.pnl.is_none() {
            trade.pnl = Some(0.0);
        }
    }
}

fn collect_trade_tickers(trades: &[Trade]) -> Vec<String> {
    let mut tickers: HashSet<String> = HashSet::new();
    for trade in trades {
        let ticker = trade.ticker.trim().to_uppercase();
        if !ticker.is_empty() {
            tickers.insert(ticker);
        }
    }
    let mut list: Vec<String> = tickers.into_iter().collect();
    list.sort();
    list
}

fn count_trade_statuses(trades: &[Trade]) -> (usize, usize) {
    let mut active = 0usize;
    let mut closed = 0usize;

    for trade in trades {
        match trade.status {
            TradeStatus::Active => active += 1,
            TradeStatus::Closed => closed += 1,
            _ => {}
        }
    }

    (active, closed)
}

fn normalize_trade_date(date: DateTime<Utc>) -> DateTime<Utc> {
    date.date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight should always be valid")
        .and_utc()
}
