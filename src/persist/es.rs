use super::{DataSource, PersistenceContext};
use crate::{
    proto::biz::TickerEvent,
    vo::biz::{MarketHoursType, QuoteType, Ticker},
    Result,
};
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use elasticsearch::{
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    indices::IndicesDeleteParts,
    Elasticsearch, IndexParts,
};
use futures::executor::block_on;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

async fn get_elasticsearch_client(uri: &str) -> Result<Elasticsearch> {
    let url = Url::parse(uri)?;
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
            let uri = &self.config.data_source.elasticsearch.uri;
            let client = block_on(get_elasticsearch_client(uri))?;
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

impl PersistenceContext {
    pub async fn drop_index(&self, name: &str) -> Result<()> {
        let time = Utc.datetime_from_str(&format!("{} 00:00:00", name), "%Y%m%d %H:%M:%S")?;
        let index_name = &format!("tickers-{}", time.format("%Y-%m-%d"));
        let client: Elasticsearch = self.get_connection()?;
        info!("Delete Elasticsearch index: {}", index_name);
        let response = client
            .indices()
            .delete(IndicesDeleteParts::Index(&[index_name]))
            .send()
            .await?;
        self.close_connection(client)?;
        if response.status_code().is_success() {
            info!("Index {} has been removed", index_name);
        } else {
            warn!("Index {} removed failed", index_name);
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticTicker {
    // Symbol name
    pub id: String,
    // TimeUnit
    pub unit: i64,
    pub price: f32,
    pub time: String,

    pub quote_type: QuoteType,
    pub market_hours: MarketHoursType,
    pub day_volume: i64,
    pub day_volume_diff: i64,
    pub change: f32,
}

impl From<Ticker> for ElasticTicker {
    fn from(t: Ticker) -> Self {
        ElasticTicker {
            id: t.id.clone(),
            unit: 0,
            price: t.price,
            time: Utc.timestamp_millis(t.time).to_rfc3339(),
            quote_type: t.quote_type,
            market_hours: t.market_hours,
            day_volume: t.day_volume,
            day_volume_diff: 0,
            change: t.change,
        }
    }
}

impl From<TickerEvent> for ElasticTicker {
    fn from(t: TickerEvent) -> Self {
        ElasticTicker {
            id: t.id.clone(),
            unit: 0,
            price: t.price,
            time: Utc.timestamp_millis(t.time).to_rfc3339(),
            quote_type: t.quote_type.try_into().unwrap(),
            market_hours: t.market_hours.try_into().unwrap(),
            day_volume: t.day_volume,
            day_volume_diff: 0,
            change: t.change,
        }
    }
}

impl ElasticTicker {
    // Get ticker info time
    fn timestamp(&self) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(&self.time).unwrap()
    }
    // Resolve index name by ticker info time
    fn index_name(&self) -> String {
        format!("tickers-{}", self.timestamp().format("%Y-%m-%d"))
    }
    pub async fn save_to_elasticsearch(&self, datasource: Arc<PersistenceContext>) -> Result<()> {
        let client: Elasticsearch = datasource.get_connection()?;

        let response = client
            .index(IndexParts::Index(&self.index_name()))
            .body(json!(self))
            .send()
            .await?;

        let successful = response.status_code().is_success();
        datasource.close_connection(client)?;
        if !successful {
            warn!("result = {:?}, {:?}", response, self);
        }
        Ok(())
    }
}
