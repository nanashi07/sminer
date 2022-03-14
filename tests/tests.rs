#[cfg(test)]
mod analysis;
#[cfg(test)]
mod persist;
#[cfg(test)]
mod provider;

use chrono::{Duration, TimeZone, Utc};
use log::info;
use sminer::provider::yahoo::consume;
use sminer::{init_log, Result};
use std::ops::Add;

const YAHOO_WS: &str = "wss://streamer.finance.yahoo.com/";

#[tokio::test]
#[ignore = "manually run only"]
async fn test_consume_yahoo_tickers() -> Result<()> {
    init_log("INFO").await?;
    let end_time = Utc::now().add(Duration::minutes(2)).timestamp();
    info!(
        "Start consuming yahoo tickers, expected to stop at {}",
        Utc.timestamp_millis(end_time),
    );
    consume(
        YAHOO_WS,
        vec![
            "SPY", "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "LABD", "LABU", "TNA", "TZA",
            "UDOW", "SDOW",
        ],
        Option::None,
    )
    .await?;

    Ok(())
}
