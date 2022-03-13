use super::{DataSource, PersistenceContext};
use crate::{vo::biz::Ticker, Result};
use chrono::{TimeZone, Utc};
use futures::executor::block_on;
use mongodb::{
    bson::doc,
    options::{ClientOptions, FindOptions},
    Client, Cursor,
};
use std::sync::Arc;

pub async fn get_mongo_client() -> Result<Client> {
    let client_options = ClientOptions::parse("mongodb://root:password@localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    Ok(client)
}

impl DataSource<Client> for PersistenceContext {
    fn get_connection(&self) -> Result<Client> {
        let mutex = Arc::clone(&self.mongo_connections);
        let mut pool = mutex.lock().unwrap();
        if pool.is_empty() {
            let client = block_on(get_mongo_client())?;
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

impl Ticker {
    pub async fn save_to_mongo(&self, datasource: &dyn DataSource<Client>) -> Result<()> {
        let database_name = format!("yahoo{}", Utc.timestamp_millis(self.time).format("%Y%m%d"));
        let client = datasource.get_connection()?;
        let db = client.database(database_name.as_str());
        let typed_collection = db.collection::<Ticker>("tickers");

        let _ = typed_collection.insert_one(self, None).await?;
        Ok(())
    }
}

pub async fn query_ticker(db_name: &str, collection: &str) -> Result<Cursor<Ticker>> {
    let client = get_mongo_client().await?;
    let db = client.database(db_name);
    let typed_collection = db.collection::<Ticker>(collection);
    let cursor = typed_collection
        .find(
            doc! {},
            FindOptions::builder()
                .sort(doc! { "time" : 1, "day_volume": 1 })
                .build(),
        )
        .await?;
    Ok(cursor)
}
