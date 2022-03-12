#[cfg(test)]
mod provider;

#[cfg(test)]
mod persist;

use std::ops::Add;

use chrono::{Duration, Utc};
use sminer::provider::yahoo::consume;
use sminer::{init_log, Result};

const YAHOO_WS: &str = "wss://streamer.finance.yahoo.com/";

#[tokio::test]
async fn test_consume_yahoo_tickers() -> Result<()> {
    init_log("TRACE").await?;
    let end_time = Utc::now().add(Duration::minutes(2)).timestamp();
    consume(
        YAHOO_WS,
        vec![
            "SPY", "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "LABD", "LABU", "TNA", "TZA",
            "UDOW", "SDOW",
        ],
        end_time,
    )
    .await?;

    Ok(())
}
