#[cfg(test)]
mod provider;

#[cfg(test)]
mod persist;

use std::ops::Add;

use chrono::{Duration, TimeZone, Utc};
use log::info;
use sminer::provider::yahoo::consume;
use sminer::{init_log, Result};

const YAHOO_WS: &str = "wss://streamer.finance.yahoo.com/";

#[tokio::test]
async fn test_consume_yahoo_tickers() -> Result<()> {
    init_log("TRACE").await?;
    let end_time = Utc::now().add(Duration::minutes(2)).timestamp();
    info!(
        "Start consuming yahoo tickers, expected to stop at {}",
        Utc.timestamp(end_time / 1000, (end_time % 1000) as u32)
    );
    consume(
        YAHOO_WS,
        vec![
            "SPY", "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "LABD", "LABU", "TNA", "TZA",
            "UDOW", "SDOW",
        ],
        Option::Some(end_time),
    )
    .await?;

    Ok(())
}
