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
        es::{get_elasticsearch_client, ElasticTicker},
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

#[tokio::test]
#[ignore = "used for test imported data"]
async fn test_import_into_es_single() -> Result<()> {
    init_log("DEBUG").await?;

    let files = vec!["yahoo20220309", "yahoo20220310", "yahoo20220311"];

    for file in files {
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

        info!("ticker size: {} for file {}", &tickers.len(), file);

        for ticker in tickers {
            let _ = ticker.save_to_elasticsearch().await?;
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

    let client = get_elasticsearch_client().await?;

    let response = client
        .bulk(BulkParts::Index("tickers-bulk"))
        .body(tickers)
        .send()
        .await?;

    let response_body = response.json::<Value>().await?;
    info!("response = {}", response_body);

    Ok(())
}
