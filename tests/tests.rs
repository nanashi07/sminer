#[cfg(test)]
mod analysis;
#[cfg(test)]
mod persist;
#[cfg(test)]
mod provider;
#[cfg(test)]
mod vo;

use chrono::{Duration, TimeZone, Utc};
use log::info;
use sminer::provider::yahoo::consume;
use sminer::vo::core::{AppConfig, AppContext};
use sminer::{init_log, Result};
use std::ops::Add;
use std::sync::Arc;
use tokio::runtime::Runtime;

const YAHOO_WS: &str = "wss://streamer.finance.yahoo.com/";

// cargo test --package sminer --test tests -- test_consume_yahoo_tickers --exact --nocapture --ignored
#[test]
#[ignore = "manually run only"]
fn test_consume_yahoo_tickers() -> Result<()> {
    let rt = Runtime::new().unwrap();
    let _: Result<()> = rt.block_on(async {
        init_log("INFO").await?;
        let context = AppContext::new(AppConfig::load("config.yaml")?)
            .init()
            .await?;

        let end_time = Utc::now().add(Duration::minutes(2)).timestamp();
        info!(
            "Start consuming yahoo tickers, expected to stop at {}",
            Utc.timestamp_millis(end_time),
        );

        let symbols = context.config.symbols();

        info!("Loaded symbols: {:?}", &symbols);

        consume(&Arc::clone(&context), YAHOO_WS, symbols, Option::None).await?;

        Ok(())
    });
    Ok(())
}
