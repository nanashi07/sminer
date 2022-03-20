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
    use mongodb::{bson::Document, Client};
    use sminer::{
        init_log,
        persist::{mongo::query_ticker, DataSource, PersistenceContext},
        vo::{biz::Ticker, core::AppConfig},
        Result,
    };
    use std::{
        fs::OpenOptions,
        io::{BufWriter, Write},
        sync::Arc,
    };

    #[tokio::test]
    #[ignore = "used for import data"]
    async fn test_import_into_mongo() -> Result<()> {
        init_log("DEBUG").await?;

        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));
        context.init_mongo().await?;
        let config = Arc::clone(&context.config);
        let db_name = config.data_source.mongodb.target.as_ref().unwrap();
        let client: Client = context.get_connection()?;

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

        for file in files {
            let tickers: Vec<Ticker> = read_from_file(file)?
                .iter()
                .map(|line| serde_json::from_str::<Ticker>(line).unwrap())
                .collect();
            info!("Loaded tickers: {} for {}", tickers.len(), file);

            let db = client.database(db_name);

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

        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));
        context.init_mongo().await?;
        let config = Arc::clone(&context.config);
        let db_name = config.data_source.mongodb.target.as_ref().unwrap();
        let client: Client = context.get_connection()?;

        let mut cursor = query_ticker(&client, db_name, "tickers20220311").await?;
        while let Some(ticker) = cursor.try_next().await? {
            info!("{:?}", ticker);
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "used for test imported data"]
    async fn test_export_mongo_by_order() -> Result<()> {
        init_log("INFO").await?;

        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));
        context.init_mongo().await?;
        let config = Arc::clone(&context.config);
        let db_name = config.data_source.mongodb.target.as_ref().unwrap();
        let client: Client = context.get_connection()?;

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
            let mut cursor = query_ticker(&client, db_name, collection).await?;
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
    use chrono::{TimeZone, Utc};
    use elasticsearch::{
        http::request::JsonBody, indices::IndicesDeleteParts, BulkParts, Elasticsearch,
    };
    use log::{debug, error, info};
    use serde_json::{json, Value};
    use sminer::{
        init_log,
        persist::{
            es::{take_digitals, ElasticTicker},
            DataSource, PersistenceContext,
        },
        vo::{biz::Ticker, core::AppConfig},
        Result,
    };
    use std::sync::Arc;

    #[tokio::test]
    #[ignore = "used for test imported data"]
    async fn test_import_into_es_single() -> Result<()> {
        init_log("INFO").await?;
        let context = Arc::new(PersistenceContext::new(Arc::new(AppConfig::load(
            "config.yaml",
        )?)));

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
                ticker.save_to_elasticsearch(Arc::clone(&context)).await?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "used for test imported data"]
    async fn test_import_into_es_bulk() -> Result<()> {
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

        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));

        for file in files {
            let tickers: Vec<ElasticTicker> = read_from_file(file)?
                .iter()
                .map(|line| serde_json::from_str::<Ticker>(line).unwrap())
                .map(|t| ElasticTicker::from(t))
                .collect();

            info!("ticker size: {} for {}", &tickers.len(), &file);

            let client: Elasticsearch = context.get_connection()?;

            let mut body: Vec<JsonBody<_>> = Vec::new();
            for ticker in tickers {
                // https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
                body.push(json!({"index": {}}).into());
                body.push(json!(ticker).into());
            }

            // generate index name
            let digital = take_digitals(&file);
            let time =
                Utc.datetime_from_str(&format!("{} 00:00:00", digital), "%Y%m%d %H:%M:%S")?;
            let index_name = format!("tickers-{}", time.format("%Y-%m-%d"));

            // drop index first
            context.drop_index(&take_digitals(&file)).await?;

            let response = client
                .bulk(BulkParts::Index(&index_name))
                .body(body)
                .send()
                .await?;

            info!("response = {} for {}", response.status_code(), &file);
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "used for test imported data"]
    async fn test_bulk_index() -> Result<()> {
        init_log("TRACE").await?;
        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));

        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(4);

        // add the first operation and document
        body.push(json!({"index": {"_id": "1"}}).into());
        body.push(
            json!({
                "id": 1,
                "user": "kimchy",
                "post_date": "2009-11-15T00:00:00Z",
                "message": "Trying out Elasticsearch, so far so good?"
            })
            .into(),
        );

        // add the second operation and document
        body.push(json!({"index": {"_id": "2"}}).into());
        body.push(
            json!({
                "id": 2,
                "user": "forloop",
                "post_date": "2020-01-08T00:00:00Z",
                "message": "Bulk indexing with the rust client, yeah!"
            })
            .into(),
        );

        let client: Elasticsearch = context.get_connection()?;
        let _ = client
            .bulk(BulkParts::Index("tweets"))
            .body(body)
            .send()
            .await?;

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_delete_index() -> Result<()> {
        init_log("INFO").await?;
        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));
        let client: Elasticsearch = context.get_connection()?;

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
