use crate::commands::backtest_active::BacktestScope;
use crate::context::AppContext;
use anyhow::Result;
use log::{info, warn};

pub async fn run(
    app: &AppContext,
    strategy_id: &str,
    scope: BacktestScope,
    weeks: usize,
) -> Result<()> {
    let mut context = scope.build_context(app).await.map_err(|error| {
        warn!(
            "Unable to initialize {} ticker start timing context: {}",
            scope.label(),
            error
        );
        error
    })?;

    let completed = context
        .backtester()
        .run_start_timing_samples(strategy_id, weeks)
        .await?;

    info!(
        "Completed {} horizon-limited start timing sample{} for strategy {} using {} scope",
        completed,
        if completed == 1 { "" } else { "s" },
        strategy_id,
        scope.label()
    );

    Ok(())
}
