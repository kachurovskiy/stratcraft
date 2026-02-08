use crate::context::AppContext;
use crate::data_context::{MarketData, TickerScope};
use anyhow::Result;
use log::info;
use std::path::Path;

pub async fn run(app: &AppContext, output_path: &Path) -> Result<()> {
    info!(
        "Generating market data snapshot at {}",
        output_path.display()
    );

    let db = app.database().await?;
    let market_data = MarketData::load(&db, TickerScope::AllTickers).await?;

    market_data.save_to_file(output_path)?;
    info!(
        "Market data snapshot successfully written to {}",
        output_path.display()
    );

    Ok(())
}
