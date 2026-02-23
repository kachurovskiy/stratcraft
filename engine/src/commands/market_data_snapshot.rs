use anyhow::{anyhow, Result};
use std::path::Path;
use tokio::fs;

pub async fn ensure_market_data_file(path: &Path) -> Result<()> {
    if fs::metadata(path).await.is_ok() {
        return Ok(());
    }

    Err(anyhow!(
        "Market data snapshot not found at {}. Generate it with `export-market-data` before running this command.",
        path.display()
    ))
}
