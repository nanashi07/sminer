use super::{DataSource, DataSource2, PersistenceContext};
use crate::{vo::biz::Ticker, Result};
use chrono::{TimeZone, Utc};
use futures::{
    executor::{block_on, LocalPool},
    future::{ready, Ready},
    Future,
};
use mongodb::{
    bson::doc,
    options::{ClientOptions, FindOptions},
    Client, Cursor,
};
use std::error::Error;
use std::{
    sync::{mpsc::channel, Arc},
    thread,
};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::runtime::Handle;

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

// impl DataSource2<Client> for PersistenceContext {
//     type Output = Ready<Result<Client>>;

//     fn get_connection2(&self) -> Self::Output {
//         let mutex = Arc::clone(&self.mongo_connections);
//         let mut pool = mutex.lock().unwrap();
//         if pool.is_empty() {
//             let jj = async {
//                 //
//             };
//             let client = get_mongo_client();
//             // ready(Ok(()))
//             // Ok(client)
//         } else {
//             let client = pool.pop().unwrap();
//             ready(Ok(client))
//         }
//     }
// }

// #[async_trait]
// impl DataSource3<Client> for PersistenceContext {
//     async fn get_connection3(&self) -> Result<Client> {
//         let mutex = Arc::clone(&self.mongo_connections);
//         let mut pool = mutex.lock().unwrap();
//         if pool.is_empty() {
//             let client = get_mongo_client().await?;
//             Ok(client)
//         } else {
//             let client = pool.pop().unwrap();
//             Ok(client)
//         }
//     }
// }

impl Ticker {
    pub async fn save_to_mongo(
        &self,
        datasource: Option<Box<dyn DataSource<Client>>>,
    ) -> Result<()> {
        let collection_name = format!(
            "tickers{}",
            Utc.timestamp_millis(self.time).format("%Y%m%d")
        );
        let client = if let Some(ds) = datasource {
            ds.get_connection()?
        } else {
            get_mongo_client().await?
        };
        // let client = datasource.get_connection()?;
        let db = client.database("yahoo");
        let typed_collection = db.collection::<Ticker>(&collection_name);

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
