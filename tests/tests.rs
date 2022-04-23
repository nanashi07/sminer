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
use tokio::runtime::Runtime;

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
        let config = context.config();

        let end_time = Utc::now().add(Duration::minutes(2)).timestamp();
        info!(
            "Start consuming yahoo tickers, expected to stop at {}",
            Utc.timestamp_millis(end_time),
        );

        let symbols = config.symbols();
        let uri = &config.platform.yahoo.uri;

        info!("Loaded symbols: {:?}", &symbols);

        consume(&context, &uri, &symbols).await?;

        Ok(())
    });
    Ok(())
}

#[test]
fn test_runtime_performance() -> Result<()> {
    let start = Utc::now().timestamp_millis();
    for _ in 1..5000 {
        log::debug!("test")
    }
    let end = Utc::now().timestamp_millis();
    println!("log cost : {}", Duration::milliseconds(end - start));

    let start = Utc::now().timestamp_millis();
    for _ in 1..5000 {
        let _ = tokio::runtime::Runtime::new().unwrap();
    }
    let end = Utc::now().timestamp_millis();
    println!("runtime new cost : {}", Duration::milliseconds(end - start));

    let start = Utc::now().timestamp_millis();
    for _ in 1..5000 {
        let _ = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    }
    let end = Utc::now().timestamp_millis();
    println!(
        "new_multi_thread cost : {}",
        Duration::milliseconds(end - start)
    );

    let start = Utc::now().timestamp_millis();
    for _ in 1..5000 {
        let _ = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
    }
    let end = Utc::now().timestamp_millis();
    println!(
        "new_current_thread cost : {}",
        Duration::milliseconds(end - start)
    );

    let start = Utc::now().timestamp_millis();
    for _ in 1..5000 {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _: Result<()> = rt.block_on(async {
            log::debug!("test");
            Ok(())
        });
    }
    let end = Utc::now().timestamp_millis();
    println!("new block cost : {}", Duration::milliseconds(end - start));

    let start = Utc::now().timestamp_millis();
    for _ in 1..5000 {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _: Result<()> = rt.block_on(async {
            log::debug!("test");
            Ok(())
        });
    }
    let end = Utc::now().timestamp_millis();
    println!(
        "new_current_thread block cost : {}",
        Duration::milliseconds(end - start)
    );

    let start = Utc::now().timestamp_millis();
    for _ in 1..5000 {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _: Result<()> = rt.block_on(async {
            log::debug!("test");
            Ok(())
        });
    }
    let end = Utc::now().timestamp_millis();
    println!(
        "new_current_thread with time block cost : {}",
        Duration::milliseconds(end - start)
    );

    let start = Utc::now().timestamp_millis();
    for _ in 1..5000 {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()
            .unwrap();
        let _: Result<()> = rt.block_on(async {
            log::debug!("test");
            Ok(())
        });
    }
    let end = Utc::now().timestamp_millis();
    println!(
        "new_current_thread with time/io block cost : {}",
        Duration::milliseconds(end - start)
    );

    Ok(())
}

fn type_of<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

macro_rules! append_text {
    (buffer: $buf:expr, $($arg:tt)+) => {
        let option: Option<&mut Vec<String>> = $buf;
        if let Some(buffer) = option {
            buffer.push(format!($($arg)+));
        } else {
            log::info!($($arg)+);
        }
    };
    ($($arg:tt)+) => {
        log::info!($($arg)+);
    };
}

#[test]
fn test_append_text() {
    let a = 1;
    let b = "2";
    let mut c: Vec<String> = Vec::new();

    println!("a: {}", type_of(a));
    println!("b: {}", type_of(b));
    println!("c: {}", type_of(&c));

    append_text!(buffer: Some(&mut c), "bb {} {}", "cc", "dd");
    append_text!(buffer: Some(&mut c), "ee {} {}", "cc", "dd");
    append_text!(buffer: None, "bb {} {}", "cc", "dd");

    println!("{:?}", &c);
}
