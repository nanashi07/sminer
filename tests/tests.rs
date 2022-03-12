#[cfg(test)]
mod decoder;

#[cfg(test)]
mod persist;

use std::ops::Add;

use chrono::{Duration, Utc};
use sminer::provider::yahoo::consume;
use sminer::{init_log, Result};

#[tokio::test]
async fn test_collect_yahoo_tickers() -> Result<()> {
    init_log("TRACE").await?;
    let end_time = Utc::now().add(Duration::minutes(2)).timestamp();
    consume(
        "wss://streamer.finance.yahoo.com/",
        vec![
            "SPY", "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "LABD", "LABU", "TNA", "TZA",
            "UDOW", "SDOW",
        ],
        end_time,
    )
    .await?;

    Ok(())
}
