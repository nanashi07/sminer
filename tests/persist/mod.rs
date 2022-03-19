use sminer::Result;
use std::{
    fs::File,
    io::{BufRead, BufReader},
};

fn read_from_file(file: &str) -> Result<Vec<String>> {
    let f = File::open(format!("tmp/{}", file))?;
    let reader = BufReader::new(f);

    let lines: Vec<String> = reader.lines().into_iter().map(|w| w.unwrap()).collect();
    Ok(lines)
}

mod mongo {
    use crate::persist::read_from_file;
    use futures::TryStreamExt;
    use log::{debug, info};
    use mongodb::bson::Document;
    use sminer::{
        init_log,
        persist::mongo::{get_mongo_client, query_ticker, DATABASE_NAME},
        vo::biz::Ticker,
        Result,
    };
    use std::{
        fs::OpenOptions,
        io::{BufWriter, Write},
    };

    const MONGO_URI: &str = "mongodb://root:password@localhost:27017";

    #[tokio::test]
    #[ignore = "used for import data"]
    async fn test_import_into_mongo() -> Result<()> {
        init_log("DEBUG").await?;

        let files = vec![
            "tickers20220309",
            "tickers20220310",
            "tickers20220311",
            "tickers20220314",
            "tickers20220315",
            "tickers20220316",
            "tickers20220317",
            "tickers20220318",
        ];
        let client = get_mongo_client(MONGO_URI).await?;

        for file in files {
            let tickers: Vec<Ticker> = read_from_file(file)?
                .iter()
                .map(|line| serde_json::from_str::<Ticker>(line).unwrap())
                .collect();
            info!("Loaded tickers: {} for {}", tickers.len(), file);

            let db = client.database(DATABASE_NAME);

            // delete original
            let collection = db.collection::<Document>(file);
            collection.drop(None).await?;

            let typed_collection = db.collection::<Ticker>(&format!("tickers{}", &file[7..]));
            typed_collection.insert_many(tickers, None).await?;
            info!("Import {} done", file);
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "used for test imported data"]
    async fn test_query_ticker() -> Result<()> {
        init_log("TRACE").await?;
        let mut cursor = query_ticker(MONGO_URI, DATABASE_NAME, "tickers20220311").await?;
        while let Some(ticker) = cursor.try_next().await? {
            info!("{:?}", ticker);
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "used for test imported data"]
    async fn test_export_mongo_by_order() -> Result<()> {
        init_log("INFO").await?;

        let collections = vec![
            "tickers20220309",
            "tickers20220310",
            "tickers20220311",
            "tickers20220314",
            "tickers20220315",
            "tickers20220316",
            "tickers20220317",
            "tickers20220318",
        ];
        for collection in collections {
            let mut cursor = query_ticker(MONGO_URI, DATABASE_NAME, collection).await?;
            std::fs::create_dir_all("tmp")?;
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&format!("tmp/{}", collection))?;
            let mut writer = BufWriter::new(file);
            info!("Export collection: {}", collection);
            while let Some(ticker) = cursor.try_next().await? {
                if ticker.id == "SPY" {
                    continue;
                }
                debug!("{:?}", ticker);
                // write file
                let json = serde_json::to_string(&ticker)?;
                write!(&mut writer, "{}\n", &json)?;
            }
            info!("Collection {} exported", collection);
        }
        Ok(())
    }
}

mod elastic {
    use crate::persist::read_from_file;
    use elasticsearch::{
        http::request::JsonBody, indices::IndicesDeleteParts, BulkParts, Elasticsearch,
    };
    use log::{debug, error, info};
    use serde_json::{json, Value};
    use sminer::{
        init_log,
        persist::{
            es::{get_elasticsearch_client, ElasticTicker},
            DataSource, PersistenceContext,
        },
        vo::{biz::Ticker, core::AppConfig},
        Result,
    };
    use std::sync::Arc;

    const ELASTICSEARCH_URI: &str = "http://localhost:9200";

    #[tokio::test]
    #[ignore = "used for test imported data"]
    async fn test_import_into_es_single() -> Result<()> {
        init_log("INFO").await?;
        let mut config = AppConfig::new();
        config.data_source.elasticsearch.uri = ELASTICSEARCH_URI.to_string();
        let persistence = Arc::new(PersistenceContext::new(Arc::new(config)));

        let files = vec!["tickers20220309"];

        for file in files {
            let tickers: Vec<ElasticTicker> = read_from_file(file)?
                .iter()
                .take(1000) // only import 1000 documents
                .map(|line| serde_json::from_str::<Ticker>(line).unwrap())
                .map(|t| ElasticTicker::from(t))
                .collect();

            info!("ticker size: {} for file {}", &tickers.len(), file);

            for ticker in tickers {
                debug!("ticker = {:?}", &ticker);
                ticker
                    .save_to_elasticsearch(Arc::clone(&persistence))
                    .await?;
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
            .take(10) // take 10 documents for test only
            .map(|line| serde_json::from_str::<Ticker>(line).unwrap())
            .map(|t| ElasticTicker::from(t))
            .map(|t| json!(t).into())
            .collect();

        info!("ticker size: {}", &tickers.len());

        let client = get_elasticsearch_client(ELASTICSEARCH_URI).await?;

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

    #[tokio::test]
    #[ignore]
    async fn test_delete_index() -> Result<()> {
        init_log("INFO").await?;
        let mut config = AppConfig::new();
        config.data_source.elasticsearch.uri = ELASTICSEARCH_URI.to_string();
        let persistence = Arc::new(PersistenceContext::new(Arc::new(config)));
        let client: Elasticsearch = persistence.get_connection()?;

        let response = client
            .indices()
            .delete(IndicesDeleteParts::Index(&["tickers-2022-03-09"]))
            .send()
            .await?;

        if response.status_code().is_success() {
            let response_body = response.json::<Value>().await?;
            info!("body = {:?}", response_body);
        } else {
            error!("response: {:?}", response);
        }
        Ok(())
    }
}
