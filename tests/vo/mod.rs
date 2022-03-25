use chrono::{TimeZone, Utc};
use config::Config;
use log::info;
use sminer::{
    init_log,
    vo::{biz::Ticker, core::AppConfig},
    Result,
};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};

#[tokio::test]
async fn test_load_config() -> Result<()> {
    init_log("DEBUG").await?;
    let settings = Config::builder()
        .add_source(config::File::with_name("./config.yaml"))
        .set_default("analysis.output.baseFolder", "tmp")?
        .set_default("dataSource.mongodb.target", "yahoo")?
        .build()?;
    info!("settings = {:?}", &settings);

    println!("");

    let config: AppConfig = settings.try_deserialize::<AppConfig>()?;
    info!("config = {:?}", &config);

    Ok(())
}

#[test]
fn print_data() -> Result<()> {
    let file = "tmp/TQQQ.tickers20220323";

    let f = File::open(file)?;
    let reader = BufReader::new(f);
    let tickers: Vec<Ticker> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| serde_json::from_str::<Ticker>(&line).unwrap())
        .collect();

    for ticker in tickers {
        println!(
            "id: {}, time: {}, market: {:?}, price: {}, volume: {}",
            ticker.id,
            Utc.timestamp_millis(ticker.time),
            ticker.market_hours,
            ticker.price,
            ticker.day_volume
        )
    }

    Ok(())
}

#[test]
fn parse_bool() -> Result<()> {
    let true_string = "true";
    let false_string = "false";

    println!(
        "parse {}: {}",
        true_string,
        true_string.parse::<bool>().unwrap()
    );
    println!(
        "parse {}: {}",
        false_string,
        false_string.parse::<bool>().unwrap()
    );

    Ok(())
}
