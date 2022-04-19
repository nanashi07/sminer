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
        persist::{
            mongo::{get_reglar_market_start_time, query_ticker},
            DataSource, PersistenceContext,
        },
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
        let config = context.config();
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
            "tickers20220321",
        ];

        for file in files {
            let tickers: Vec<Ticker> = read_from_file(file)?
                .iter()
                .map(|line| serde_json::from_str::<Ticker>(line).unwrap())
                .collect();
            info!("Loaded tickers: {} for {}", tickers.len(), file);

            let db = client.database(db_name);

            if context.config().truncat_enabled() {
                // delete original
                info!("Drop collection {}.{} for clean data", &db_name, &file);
                let collection = db.collection::<Document>(&file);
                collection.drop(None).await?;
            } else {
                debug!("Ignore drop collection: {}.{}", &db_name, &file);
            }

            let typed_collection = db.collection::<Ticker>(&file);
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
        let config = context.config();
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
    async fn test_query_market_start_time() -> Result<()> {
        init_log("DEBUG").await?;

        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));
        context.init_mongo().await?;
        let config = context.config();
        let db_name = config.data_source.mongodb.target.as_ref().unwrap();
        let client: Client = context.get_connection()?;

        let time = 1647271806000_i64;
        if let Ok(result) = get_reglar_market_start_time(&client, db_name, time).await {
            println!("time = {}", result);
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "used for test imported data"]
    async fn test_export_mongo_by_order() -> Result<()> {
        init_log("INFO").await?;

        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));
        context.init_mongo().await?;
        let config = context.config();
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
    use elasticsearch::{
        http::request::JsonBody, indices::IndicesDeleteParts, BulkParts, Elasticsearch,
    };
    use log::{debug, error, info};
    use serde_json::{json, Value};
    use sminer::{
        init_log,
        persist::{
            es::{take_index_time, ticker_index_name, ElasticTicker},
            DataSource, PersistenceContext,
        },
        vo::{biz::Ticker, core::AppConfig},
        Result,
    };
    use std::{cmp::max, sync::Arc};

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
            "tickers20220321",
            "tickers20220322",
            "tickers20220323",
        ];

        let context = PersistenceContext::new(Arc::new(AppConfig::load("config.yaml")?));

        for file in files {
            let mut tickers: Vec<ElasticTicker> = read_from_file(file)?
                .iter()
                .map(|line| serde_json::from_str::<Ticker>(line).unwrap())
                .map(|t| ElasticTicker::from(t))
                .collect();

            // FIXME: calculate volume
            let mut last_volume = 0;
            tickers.iter_mut().for_each(|ticker| {
                ticker.volume = max(0, ticker.day_volume - last_volume);
                last_volume = ticker.day_volume;
            });

            info!("ticker size: {} for {}", &tickers.len(), &file);

            let client: Elasticsearch = context.get_connection()?;

            let mut body: Vec<JsonBody<_>> = Vec::new();
            for ticker in tickers {
                // https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
                body.push(json!({"index": {}}).into());
                body.push(json!(ticker).into());
            }

            // drop index first
            let index_time = take_index_time(file);
            let index_name = ticker_index_name(&index_time);
            context.delete_index(&index_name).await?;

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
            .delete(IndicesDeleteParts::Index(&["sminer-tickers-2022-03-09"]))
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

mod grafana {

    use chrono::{DateTime, Utc};
    use log::info;
    use sminer::{
        init_log,
        persist::grafana::{add_annotation, list_annotations, remove_annotation},
        vo::core::AppConfig,
        Result,
    };
    use std::sync::Arc;

    #[test]
    fn test_json_string() -> Result<()> {
        let value: serde_json::Value = serde_json::json!({
            "dashboardId": 1,
            "panelId": 2,
            "time": 1646848456000 as i64,
            "text": "222"
        });

        println!("{}", value);

        Ok(())
    }

    // cargo test --package sminer --test tests -- persist::grafana::test_add_annotation --exact --nocapture
    #[tokio::test]
    #[ignore = "manually test"]
    async fn test_add_annotation() -> Result<()> {
        init_log("Trace").await?;
        let config = Arc::new(AppConfig::load("config.yaml").unwrap());
        let time = DateTime::parse_from_rfc3339("2022-03-09T19:11:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let time2 = DateTime::parse_from_rfc3339("2022-03-09T19:15:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        add_annotation(
            Arc::clone(&config),
            &time,
            "my test",
            &vec!["aaa".to_owned(), "bbb".to_owned(), "ccc".to_owned()],
            1,
            2,
        )
        .await?;

        add_annotation(
            Arc::clone(&config),
            &time2,
            "my test2",
            &vec!["111".to_owned(), "222".to_owned(), "333".to_owned()],
            1,
            3,
        )
        .await?;
        Ok(())
    }

    // cargo test --package sminer --test tests -- persist::grafana::test_list_annotation --exact --nocapture
    #[tokio::test]
    #[ignore = "manually test"]
    async fn test_list_annotation() -> Result<()> {
        init_log("Trace").await?;
        let config = Arc::new(AppConfig::load("config.yaml").unwrap());
        let from = DateTime::parse_from_rfc3339("2022-03-09T12:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2022-03-09T22:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let annotations = list_annotations(
            Arc::clone(&config),
            Some(from),
            Some(to),
            None,
            None,
            &Vec::new(),
        )
        .await?;

        for annotation in annotations {
            info!("{:?}", &annotation);
        }
        Ok(())
    }

    // cargo test --package sminer --test tests -- persist::grafana::test_delete_annotation --exact --nocapture --ignored
    #[tokio::test]
    #[ignore = "manually test"]
    async fn test_delete_annotation() -> Result<()> {
        init_log("Trace").await?;
        let config = Arc::new(AppConfig::load("config.yaml").unwrap());
        let from = DateTime::parse_from_rfc3339("2022-03-09T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2022-03-09T23:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);

        let mut count = 1;
        while count > 0 {
            let annotations = list_annotations(
                Arc::clone(&config),
                Some(from),
                Some(to),
                None,
                None,
                &Vec::new(),
            )
            .await?;
            count = annotations.len();
            for annotation in annotations {
                info!("delete annotation: {}", &annotation.id);
                remove_annotation(Arc::clone(&config), annotation.id).await?;
            }
        }

        Ok(())
    }
}
