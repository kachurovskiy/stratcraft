use crate::context::AppContext;
use anyhow::Result;
use log::info;

pub async fn run(app: &AppContext) -> Result<()> {
    info!("Generating signals for active strategies using dynamic lookback");
    let mut context = app.engine_context_all_tickers().await?;
    context.signal_manager().generate_missing_signals().await?;
    info!("Completed signal generation");
    Ok(())
}
