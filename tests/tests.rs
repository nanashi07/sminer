#[cfg(test)]
mod decoder;

#[cfg(test)]
mod persist;

use sminer::provider::yahoo::next;
use sminer::{init_log, Result};

#[tokio::test]
async fn test_collect_yahoo_tickers() -> Result<()> {
    init_log("TRACE").await?;
    next(
        "wss://streamer.finance.yahoo.com/",
        vec![
            "SPY", "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "LABD", "LABU", "TNA", "TZA",
            "UDOW", "SDOW",
        ],
    )
    .await?;

    Ok(())
}
