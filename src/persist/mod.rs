use chrono::Local;
use mongodb::{options::ClientOptions, Client};

use crate::{vo::Ticker, Result};

pub async fn save_one(ticker: &Ticker) -> Result<()> {
    let database_name = format!("yahoo{}", Local::now().format("%Y%m%d"));
    let client_options = ClientOptions::parse("mongodb://root:password@localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    let db = client.database(database_name.as_str());
    let typed_collection = db.collection::<Ticker>("tickers");

    let _ = typed_collection.insert_one(ticker, None).await?;
    Ok(())
}
