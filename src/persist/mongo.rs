use crate::{vo::Ticker, Result};
use chrono::{TimeZone, Utc};
use mongodb::{
    bson::doc,
    options::{ClientOptions, FindOptions},
    Client, Cursor,
};

pub async fn get_mongo_client() -> Result<Client> {
    let client_options = ClientOptions::parse("mongodb://root:password@localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    Ok(client)
}

impl Ticker {
    pub async fn save_to_mongo(&self) -> Result<()> {
        let database_name = format!(
            "yahoo{}",
            Utc.timestamp(self.time / 1000 as i64, (self.time % 1000) as u32)
                .format("%Y%m%d")
        );
        let client = get_mongo_client().await?;
        let db = client.database(database_name.as_str());
        let typed_collection = db.collection::<Ticker>("tickers");

        let _ = typed_collection.insert_one(self, None).await?;
        Ok(())
    }
}

pub async fn query_ticker(db_name: &str, symbol: &str) -> Result<Cursor<Ticker>> {
    let client = get_mongo_client().await?;
    let db = client.database(db_name);
    let typed_collection = db.collection::<Ticker>("tickers");
    let cursor = typed_collection
        .find(
            doc! { "market_hours": "RegularMarket", "id": symbol },
            FindOptions::builder().sort(doc! { "time" : 1 }).build(),
        )
        .await?;
    Ok(cursor)
}
