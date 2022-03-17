#[cfg(test)]
mod analysis;
#[cfg(test)]
mod persist;
#[cfg(test)]
mod provider;

use chrono::{Duration, TimeZone, Utc};
use log::info;
use sminer::analysis::init_dispatcher;
use sminer::provider::yahoo::consume;
use sminer::vo::core::AppContext;
use sminer::{init_log, Result};
use std::ops::Add;
use tokio::runtime::Runtime;

const YAHOO_WS: &str = "wss://streamer.finance.yahoo.com/";

// cargo test --package sminer --test tests -- test_consume_yahoo_tickers --exact --nocapture --ignored
#[test]
#[ignore = "manually run only"]
fn test_consume_yahoo_tickers() -> Result<()> {
    let rt = Runtime::new().unwrap();
    let _: Result<()> = rt.block_on(async {
        let context = AppContext::new();
        init_log("INFO").await?;
        init_dispatcher(&context.sender, &context.persistence).await?;
        // FIXME: temp init
        context.persistence.init_mongo().await?;

        let end_time = Utc::now().add(Duration::minutes(2)).timestamp();
        info!(
            "Start consuming yahoo tickers, expected to stop at {}",
            Utc.timestamp_millis(end_time),
        );
        consume(
            &context,
            YAHOO_WS,
            vec![
                "SPY", "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "LABD", "LABU", "TNA",
                "TZA", "UDOW", "SDOW", "YANG", "YINN",
            ],
            Option::None,
        )
        .await?;
        Ok(())
    });
    Ok(())
}
