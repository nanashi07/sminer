pub mod analysis;
pub mod persist;
pub mod proto;
pub mod provider;
pub mod vo;

use log::LevelFilter;
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Root},
    Config,
};
use std::error::Error;
use std::str::FromStr;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub async fn init_log(level: &str) -> Result<()> {
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
