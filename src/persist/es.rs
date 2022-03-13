use super::{DataSource, PersistenceContext};
use crate::{
    vo::biz::{MarketHoursType, QuoteType, Ticker},
    Result,
};
use chrono::{DateTime, TimeZone, Utc};
use elasticsearch::{
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    Elasticsearch, IndexParts,
};
use futures::executor::block_on;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

pub async fn get_elasticsearch_client() -> Result<Elasticsearch> {
    let url = Url::parse("http://localhost:9200")?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);

    Ok(client)
}

impl DataSource<Elasticsearch> for PersistenceContext {
    fn get_connection(&self) -> Result<Elasticsearch> {
        let mutex = Arc::clone(&self.elastic_connections);
        let mut pool = mutex.lock().unwrap();
        if pool.is_empty() {
            let client = block_on(get_elasticsearch_client())?;
            Ok(client)
        } else {
            let client = pool.pop().unwrap();
            Ok(client)
        }
    }

    fn close_connection(&self, client: Elasticsearch) -> Result<()> {
        let mutex = Arc::clone(&self.elastic_connections);
        let mut pool = mutex.lock().unwrap();
        pool.push(client);
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DataType {
    Realtime,
    SecondTen,
    SecondThirty,
    MinuteOne,
    MinuteTwo,
    MinuteThree,
    MinuteFour,
    MinuteFive,
    MinuteTen,
    MinuteTwenty,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticTicker {
    pub id: String,
    pub price: f32,
    pub time: String,

    pub quote_type: QuoteType,
    pub market_hours: MarketHoursType,
    pub day_volume: i64,
    pub day_high: f32,
    pub day_low: f32,
    pub change: f32,
}

impl From<Ticker> for ElasticTicker {
    fn from(t: Ticker) -> Self {
        ElasticTicker {
            time: Utc.timestamp_millis(t.time).to_rfc3339(),
            id: t.id,
            price: t.price,
            quote_type: t.quote_type,
            market_hours: t.market_hours,
            day_volume: t.day_volume,
            day_high: t.day_high,
            day_low: t.day_low,
            change: t.change,
        }
    }
}

impl ElasticTicker {
    pub async fn save_to_elasticsearch(
        &self,
        pool: &dyn DataSource<Elasticsearch>,
    ) -> Result<bool> {
        let client = pool.get_connection()?;

        let time = DateTime::parse_from_rfc3339(&self.time)?;

        let response = client
            .index(IndexParts::Index(&format!(
                "tickers-{}",
                time.format("%Y-%m-%d")
            )))
            .body(json!(self))
            .send()
            .await?;

        let successful = response.status_code().is_success();
        pool.close_connection(client)?;
        if !successful {
            warn!("result = {:?}, {:?}", response, self);
            Ok(false)
        } else {
            Ok(true)
        }
    }
}
