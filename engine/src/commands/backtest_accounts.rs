use crate::backtester::StrategySelection;
use crate::context::AppContext;
use anyhow::Result;
use log::{info, warn};

pub async fn run(app: &AppContext) -> Result<()> {
    let mut context = app.engine_context_all_tickers().await.map_err(|error| {
        warn!(
            "Unable to initialize all ticker backtest context for account strategies: {}",
            error
        );
        error
    })?;

    info!("Running backtests for account-linked strategies using all tickers");
    context
        .backtester()
        .run_with_selection(None, StrategySelection::AccountLinkedOnly)
        .await?;
    info!("Completed backtests for account-linked strategies");

    Ok(())
}
