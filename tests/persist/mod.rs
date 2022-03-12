use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use futures::TryStreamExt;
use log::info;
use sminer::{
    init_log,
    persist::mongo::{get_connection, query_ticker},
    vo::Ticker,
    Result,
};

#[tokio::test]
#[ignore = "used for import data"]
async fn test_import() -> Result<()> {
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

    let client = get_connection().await?;
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
