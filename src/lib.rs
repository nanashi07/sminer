pub mod decoder;
pub mod persist;
pub mod proto;
pub mod provider;
pub mod vo;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const YAHOO_WS: &str = "wss://streamer.finance.yahoo.com/";

use std::str::FromStr;

use log::LevelFilter;
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Root},
    Config,
};

pub fn init_log(level: &str) -> Result<()> {
    let stdout = ConsoleAppender::builder().build();
    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(
            Root::builder()
                .appender("stdout")
                .build(LevelFilter::from_str(level).unwrap_or(LevelFilter::Info)),
        )?;

    let _ = log4rs::init_config(config)?;
    Ok(())
}
