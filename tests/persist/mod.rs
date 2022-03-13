use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use chrono::{DateTime, TimeZone, Utc};
use elasticsearch::{http::request::JsonBody, BulkParts, IndexParts};
use futures::TryStreamExt;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sminer::{
    init_log,
    persist::{
        es::get_es_client,
        mongo::{get_mongo_client, query_ticker},
    },
    vo::{MarketHoursType, QuoteType, Ticker},
    Result,
};

#[tokio::test]
#[ignore = "used for import data"]
async fn test_import_into_mongo() -> Result<()> {
    init_log("DEBUG").await?;

    let file = "yahoo20220311";

    let f = File::open(format!("/Users/nanashi07/Downloads/{}.tickers.db", file))?;
    let reader = BufReader::new(f);

    let tickers: Vec<Ticker> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| {
            let ticker: Ticker = serde_json::from_str(&line).unwrap();
            ticker
        })
        .collect();

    let client = get_mongo_client().await?;
    let db = client.database(file);
    let typed_collection = db.collection::<Ticker>("tickers");
    typed_collection.insert_many(tickers, None).await?;

    Ok(())
}

#[tokio::test]
#[ignore = "used for test imported data"]
async fn test_query_ticker() -> Result<()> {
    init_log("TRACE").await?;
    let mut cursor = query_ticker("yahoo20220309", "TQQQ").await?;
    while let Some(ticker) = cursor.try_next().await? {
        info!("{:?}", ticker);
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticTicker {
    pub id: String,
    pub price: f32,
    pub time: String,

    pub quote_type: QuoteType,
    pub market_hours: MarketHoursType,
    pub day_volume: i64,
    pub day_high: f32,
    pub day_low: f32,
    pub change: f32,
}

impl From<Ticker> for ElasticTicker {
    fn from(t: Ticker) -> Self {
        ElasticTicker {
            time: Utc.timestamp_millis(t.time).to_rfc3339(),
            id: t.id,
            price: t.price,
            quote_type: t.quote_type,
            market_hours: t.market_hours,
            day_volume: t.day_volume,
            day_high: t.day_high,
            day_low: t.day_low,
            change: t.change,
        }
    }
}

#[tokio::test]
#[ignore = "used for test imported data"]
async fn test_import_into_es_single() -> Result<()> {
    init_log("INFO").await?;

    let file = "yahoo20220310";

    let f = File::open(format!("/Users/nanashi07/Downloads/{}.tickers.db", file))?;
    let reader = BufReader::new(f);

    let tickers: Vec<ElasticTicker> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| {
            let ticker: Ticker = serde_json::from_str(&line).unwrap();
            ticker
        })
        .map(|t| ElasticTicker::from(t))
        .collect();

    info!("ticker size: {}", &tickers.len());

    let client = get_es_client().await?;

    for ticker in tickers {
        let response = client
            .index(IndexParts::Index("tickers"))
            .body(json!(ticker))
            .send()
            .await?;

        let successful = response.status_code().is_success();
        if !successful {
            info!("result = {}, {:?}", successful, ticker);
        }
    }

    Ok(())
}

#[tokio::test]
#[ignore = "used for test imported data"]
async fn test_import_into_es_bulk() -> Result<()> {
    init_log("INFO").await?;

    let file = "yahoo20220309";

    let f = File::open(format!("/Users/nanashi07/Downloads/{}.tickers.db", file))?;
    let reader = BufReader::new(f);

    let tickers: Vec<JsonBody<_>> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .take(10)
        .map(|line| {
            let ticker: Ticker = serde_json::from_str(&line).unwrap();
            ticker
        })
        .map(|t| ElasticTicker::from(t))
        .map(|t| json!(t).into())
        .collect();

    info!("ticker size: {}", &tickers.len());

    let client = get_es_client().await?;

    let response = client
        .bulk(BulkParts::Index("tickers-bulk"))
        .body(tickers)
        .send()
        .await?;

    let response_body = response.json::<Value>().await?;
    info!("response = {}", response_body);

    Ok(())
}
