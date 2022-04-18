use super::{DataSource, PersistenceContext};
use crate::{
    vo::{
        biz::Ticker,
        core::{AppConfig, AppContext},
    },
    Result,
};
use chrono::{Duration, TimeZone, Utc};
use futures::TryStreamExt;
use log::{debug, info, trace};
use mongodb::{
    bson::{doc, Document},
    options::{ClientOptions, FindOptions},
    Client, Cursor,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    sync::Arc,
    thread,
};

pub async fn get_mongo_client(uri: &str) -> Result<Client> {
    let client_options = ClientOptions::parse(uri).await?;
    let client = Client::with_options(client_options)?;
    Ok(client)
}

impl DataSource<Client> for PersistenceContext {
    fn get_connection(&self) -> Result<Client> {
        let mutex = Arc::clone(&self.mongo_connections);
        let mut pool = mutex.lock().unwrap();
        if pool.is_empty() {
            // TODO: send command for creating connection
            for _ in 0..10 {
                if pool.is_empty() {
                    info!("sleep for 1s");
                    thread::sleep(std::time::Duration::from_secs(1));
                }
            }
            let client = pool.pop().unwrap();
            Ok(client)
        } else {
            let client = pool.pop().unwrap();
            Ok(client)
        }
    }

    fn close_connection(&self, client: Client) -> Result<()> {
        let mutex = Arc::clone(&self.mongo_connections);
        let mut pool = mutex.lock().unwrap();
        pool.push(client);
        Ok(())
    }
}

impl PersistenceContext {
    pub async fn drop_collection(&self, name: &str) -> Result<()> {
        let config = self.config();
        let db_name = config.data_source.mongodb.target.as_ref().unwrap();
        let client: Client = self.get_connection()?;
        let db = client.database(db_name);
        info!("Drop MongoDB collection: {}", name);
        let collection = db.collection::<Document>(name);
        collection.drop(None).await?;
        self.close_connection(client)?;
        Ok(())
    }
}

impl Ticker {
    pub async fn save_to_mongo(&self, context: Arc<PersistenceContext>) -> Result<()> {
        let collection_name = format!(
            "tickers{}",
            Utc.timestamp_millis(self.time).format("%Y%m%d")
        );
        let config = context.config();
        let db_name = config.data_source.mongodb.target.as_ref().unwrap();
        let client: Client = context.get_connection()?;
        let db = client.database(db_name);
        let collection = db.collection::<Self>(&collection_name);

        let _ = collection.insert_one(self, None).await?;
        context.close_connection(client)?;
        Ok(())
    }
}

pub async fn get_start_time(context: Arc<PersistenceContext>, config: Arc<AppConfig>) -> i64 {
    let db_name = config.data_source.mongodb.target.as_ref().unwrap();
    let now = Utc::now().timestamp_millis();
    let client = context.get_connection().unwrap();
    let start_time = get_reglar_market_start_time(&client, db_name, now)
        .await
        .unwrap();
    context.close_connection(client).unwrap();
    start_time
}

pub async fn get_reglar_market_start_time(
    client: &Client,
    db_name: &str,
    time: i64,
) -> Result<i64> {
    let start_time = time % Duration::days(1).num_milliseconds();
    let end_time = start_time + Duration::days(1).num_milliseconds();
    let collection_name = format!("tickers{}", Utc.timestamp_millis(time).format("%Y%m%d"));
    let db = client.database(db_name);
    let collection = db.collection::<Ticker>(&collection_name);
    let mut cursor = collection
        .find(
            doc! {
                "market_hours" : "RegularMarket",
                "$and" : [
                    { "time": { "$gt": start_time } },
                    { "time": { "$lt": end_time } }
                ]
            },
            FindOptions::builder()
                .sort(doc! { "time" : 1, "day_volume": 1 })
                .build(),
        )
        .await?;

    if let Some(ticker) = cursor.try_next().await? {
        Ok(ticker.time)
    } else {
        Ok(0)
    }
}

pub async fn query_ticker(
    client: &Client,
    db_name: &str,
    collection: &str,
) -> Result<Cursor<Ticker>> {
    let db = client.database(db_name);
    let collection = db.collection::<Ticker>(collection);
    let cursor = collection
        .find(
            doc! {},
            FindOptions::builder()
                .sort(doc! { "time" : 1, "day_volume": 1 })
                .build(),
        )
        .await?;
    Ok(cursor)
}

pub async fn import(context: &AppContext, path: &str) -> Result<()> {
    info!("Import messages from {}", &path);

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let tickers: Vec<Ticker> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| serde_json::from_str::<Ticker>(&line).unwrap())
        .collect();

    info!("Loaded tickers: {} for {}", tickers.len(), path);

    let persistence = context.persistence();
    let config = context.config();
    let db_name = config.data_source.mongodb.target.as_ref().unwrap();
    let client: Client = persistence.get_connection()?;
    let db = client.database(db_name);

    let collection_name = Path::new(&path).file_name().unwrap().to_str().unwrap();

    // delete original
    if context.config().truncat_enabled() {
        info!(
            "Drop collection {}.{} for clean data",
            &db_name, &collection_name
        );
        let collection = db.collection::<Document>(&collection_name);
        collection.drop(None).await?;
    } else {
        debug!("Ignore drop collection: {}.{}", &db_name, &collection_name);
    }

    info!("Importing data into {}.{}", &db_name, &collection_name);
    let typed_collection = db.collection::<Ticker>(&collection_name);
    typed_collection.insert_many(tickers, None).await?;
    info!("Import {} done", &path);

    persistence.close_connection(client)?;

    Ok(())
}

pub async fn export(context: &AppContext, name: &str) -> Result<()> {
    let persistence = context.persistence();
    let config = context.config();
    let db_name = config.data_source.mongodb.target.as_ref().unwrap();
    let base_path = config.replay.outputs.base_folder.as_str();
    let client: Client = persistence.get_connection()?;
    let path = format!("{}/{}", &base_path, &name);

    std::fs::create_dir_all(&base_path)?;

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    let mut writer = BufWriter::new(file);

    info!("Export collection: {}", &name);
    let mut cursor = query_ticker(&client, &db_name, &name).await?;
    while let Some(ticker) = cursor.try_next().await? {
        trace!("{:?}", ticker);
        // write file
        let json = serde_json::to_string(&ticker)?;
        write!(&mut writer, "{}\n", &json)?;
    }
    info!("File {} exported", &path);
    persistence.close_connection(client)?;

    Ok(())
}
