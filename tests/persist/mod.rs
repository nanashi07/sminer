use elasticsearch::{http::request::JsonBody, BulkParts};
use futures::TryStreamExt;
use log::{debug, info};
use serde_json::{json, Value};
use sminer::{
    init_log,
    persist::{
        es::{get_elasticsearch_client, ElasticTicker},
        mongo::{get_mongo_client, query_ticker},
        PersistenceContext,
    },
    vo::biz::Ticker,
    Result,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
};

pub fn read_from_file(file: &str) -> Result<Vec<String>> {
    let f = File::open(format!("tmp/{}", file))?;
    let reader = BufReader::new(f);

    let lines: Vec<String> = reader.lines().into_iter().map(|w| w.unwrap()).collect();
    Ok(lines)
}

#[tokio::test]
#[ignore = "used for import data"]
async fn test_import_into_mongo() -> Result<()> {
    init_log("DEBUG").await?;

    let files = vec!["tickers20220309", "tickers20220310", "tickers20220311"];
    let client = get_mongo_client().await?;

    for file in files {
        let tickers: Vec<Ticker> = read_from_file(file)?
            .iter()
            .map(|line| {
                let ticker: Ticker = serde_json::from_str(line).unwrap();
                ticker
            })
            .collect();
        info!("Loaded tickers: {} for {}", tickers.len(), file);

        let db = client.database("yahoo");
        let typed_collection = db.collection::<Ticker>(&format!("tickers{}", &file[7..]));
        typed_collection.insert_many(tickers, None).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "used for test imported data"]
async fn test_query_ticker() -> Result<()> {
    init_log("TRACE").await?;
    let mut cursor = query_ticker("yahoo", "tickers20220311").await?;
    while let Some(ticker) = cursor.try_next().await? {
        info!("{:?}", ticker);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "used for test imported data"]
async fn test_export_mongo_by_order() -> Result<()> {
    init_log("INFO").await?;

    let collections = vec!["tickers20220309", "tickers20220310", "tickers20220311"];
    for collection in collections {
        let mut cursor = query_ticker("yahoo", collection).await?;
        std::fs::create_dir_all("tmp")?;
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&format!("tmp/{}", collection))?;
        let mut writer = BufWriter::new(file);
        info!("Export collection: {}", collection);
        while let Some(ticker) = cursor.try_next().await? {
            debug!("{:?}", ticker); // TODO: write file
            let json = serde_json::to_string(&ticker)?;
            write!(&mut writer, "{}\n", &json)?;
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "used for test imported data"]
async fn test_import_into_es_single() -> Result<()> {
    init_log("INFO").await?;

    let files = vec!["tickers20220309", "tickers20220310", "tickers20220311"];

    let persistence = PersistenceContext::new();

    for file in files {
        let tickers: Vec<ElasticTicker> = read_from_file(file)?
            .iter()
            .map(|line| {
                let ticker: Ticker = serde_json::from_str(&line).unwrap();
                ticker
            })
            .map(|t| ElasticTicker::from(&t))
            .collect();

        info!("ticker size: {} for file {}", &tickers.len(), file);

        for ticker in tickers {
            debug!("ticker = {:?}", &ticker);
            let _ = ticker.save_to_elasticsearch(&persistence).await?;
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "used for test imported data"]
async fn test_import_into_es_bulk() -> Result<()> {
    init_log("INFO").await?;

    let file = "tickers20220309";

    let tickers: Vec<JsonBody<_>> = read_from_file(file)?
        .iter()
        .take(10)
        .map(|line| {
            let ticker: Ticker = serde_json::from_str(&line).unwrap();
            ticker
        })
        .map(|t| ElasticTicker::from(&t))
        .map(|t| json!(t).into())
        .collect();

    info!("ticker size: {}", &tickers.len());

    let client = get_elasticsearch_client().await?;

    // FIXME: bulk failed
    let response = client
        .bulk(BulkParts::Index("tickers-bulk"))
        .body(tickers)
        .send()
        .await?;

    let response_body = response.json::<Value>().await?;
    info!("response = {}", response_body);

    Ok(())
}
