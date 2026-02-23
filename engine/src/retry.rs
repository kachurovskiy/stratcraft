macro_rules! retry_db_operation {
    ($context:expr, $operation:expr) => {{
        const MAX_ATTEMPTS: u32 = 3;
        const RETRY_DELAY_SECS: u64 = 3;

        let context_value: String = $context.into();
        let mut attempt = 1;

        loop {
            match ($operation).await {
                Ok(value) => break Ok(value),
                Err(err) if attempt >= MAX_ATTEMPTS => break Err(err),
                Err(err) => {
                    log::warn!(
                        "Attempt {}/{} for {} failed: {}. Retrying in {}s.",
                        attempt,
                        MAX_ATTEMPTS,
                        context_value,
                        err,
                        RETRY_DELAY_SECS
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
                    attempt += 1;
                }
            }
        }
    }};
}

pub(crate) use retry_db_operation;
