use super::{DataSource, PersistenceContext};
use crate::{
    proto::biz::TickerEvent,
    vo::{
        biz::{MarketHoursType, Protfolio, QuoteType, Ticker, TradeInfo},
        core::AppContext,
    },
    Result,
};
use chrono::{DateTime, TimeZone, Utc};
use elasticsearch::{
    http::{
        request::JsonBody,
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    indices::IndicesDeleteParts,
    BulkParts, Elasticsearch, IndexParts,
};
use futures::executor::block_on;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    sync::Arc,
};

const DATE_FORMAT: &str = "%Y-%m-%d";
const INDEX_PREFIX_TICKER: &str = "sminer-ticker";
const INDEX_PREFIX_PROTFOLIO: &str = "sminer-protfolio";
const INDEX_PREFIX_SLOPE: &str = "sminer-slope";
const INDEX_PREFIX_TRADE: &str = "sminer-trade";

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
    pub async fn delete_index(&self, index_name: &str) -> Result<()> {
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
    pub volume: i64,
    pub change: f32,

    // Period type
    pub period_type: i32,
}

impl From<Ticker> for ElasticTicker {
    fn from(t: Ticker) -> Self {
        Self {
            id: t.id.clone(),
            unit: 0,
            price: t.price,
            time: Utc.timestamp_millis(t.time).to_rfc3339(),
            quote_type: t.quote_type,
            market_hours: t.market_hours,
            day_volume: t.day_volume,
            volume: t.volume.unwrap_or(0),
            change: t.change,
            period_type: 0,
        }
    }
}

impl From<TickerEvent> for ElasticTicker {
    fn from(t: TickerEvent) -> Self {
        Self {
            id: t.id.clone(),
            unit: 0,
            price: t.price,
            time: Utc.timestamp_millis(t.time).to_rfc3339(),
            quote_type: t.quote_type.try_into().unwrap(),
            market_hours: t.market_hours.try_into().unwrap(),
            day_volume: t.day_volume,
            volume: t.volume,
            change: t.change,
            period_type: 0,
        }
    }
}

impl ElasticTicker {
    // Get ticker info time
    fn timestamp(&self) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&self.time)
            .unwrap()
            .with_timezone(&Utc)
    }

    pub async fn save_to_elasticsearch(&self, datasource: Arc<PersistenceContext>) -> Result<()> {
        let client: Elasticsearch = datasource.get_connection()?;
        let index_name = ticker_index_name(&self.timestamp());
        let response = client
            .index(IndexParts::Index(&index_name))
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

    pub async fn batch_save_to_elasticsearch(
        datasource: Arc<PersistenceContext>,
        items: &Vec<ElasticTicker>,
    ) -> Result<()> {
        let index_name = ticker_index_name(&items[0].timestamp());

        let mut body: Vec<JsonBody<_>> = Vec::new();
        for item in items {
            body.push(json!({"index": {}}).into());
            body.push(json!(item).into());
        }

        let client: Elasticsearch = datasource.get_connection()?;
        let response = client
            .bulk(BulkParts::Index(&index_name))
            .body(body)
            .send()
            .await?;

        let successful = response.status_code().is_success();
        datasource.close_connection(client)?;
        if !successful {
            warn!("result = {:?}", response);
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ElasticTrade {
    pub id: String,
    pub time: String,
    pub timestamp: i64,

    pub kind: char,
    pub unit: String,
    pub slope: f64,
}

impl ElasticTrade {
    pub fn from(trade: &TradeInfo) -> Vec<Self> {
        trade
            .states
            .iter()
            .flat_map(|(unit, slopes)| {
                slopes.iter().enumerate().map(|(index, slope)| Self {
                    id: trade.id.clone(),
                    time: Utc.timestamp_millis(trade.time).to_rfc3339(),
                    timestamp: trade.time,
                    kind: trade.kind,
                    unit: format!("{}{:03}", &unit.clone(), &index),
                    slope: *slope,
                })
            })
            .collect::<Vec<Self>>()
    }
}

fn take_digitals(str: &str) -> String {
    let filename = Path::new(str).file_name().unwrap().to_str().unwrap();
    filename
        .chars()
        .filter(|c| c.is_numeric())
        .collect::<String>()
}

pub fn take_index_time(name: &str) -> DateTime<Utc> {
    let digital = take_digitals(name);
    Utc.datetime_from_str(&format!("{} 00:00:00", digital), "%Y%m%d %H:%M:%S")
        .unwrap()
}

pub fn slope_index_name(time: &DateTime<Utc>) -> String {
    format!("{}-{}", INDEX_PREFIX_SLOPE, time.format(DATE_FORMAT))
}

pub fn ticker_index_name(time: &DateTime<Utc>) -> String {
    format!("{}-{}", INDEX_PREFIX_TICKER, time.format(DATE_FORMAT))
}

pub fn protfolio_index_name(time: &DateTime<Utc>) -> String {
    format!("{}-{}", INDEX_PREFIX_PROTFOLIO, time.format(DATE_FORMAT))
}

pub fn trade_index_name(time: &DateTime<Utc>) -> String {
    format!("{}-{}", INDEX_PREFIX_TRADE, time.format(DATE_FORMAT))
}

pub async fn index_tickers_from_file(context: &AppContext, path: &str) -> Result<()> {
    info!("Import messages from {}", &path);

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut tickers: Vec<ElasticTicker> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| serde_json::from_str::<Ticker>(&line).unwrap())
        .map(|t| ElasticTicker::from(t))
        .collect();

    let mut previous_volume: HashMap<String, i64> = HashMap::new();
    // calculate volume
    for ticker in tickers.iter_mut() {
        let prev = *previous_volume.get(&ticker.id).unwrap_or(&0);
        if prev < ticker.day_volume {
            ticker.volume = ticker.day_volume - prev;
            previous_volume.insert(ticker.id.to_string(), ticker.day_volume);
        }
    }

    info!("ticker size: {} for {}", &tickers.len(), &path);

    // generate index name
    let digital = take_digitals(&path);
    let time = Utc.datetime_from_str(&format!("{} 00:00:00", digital), "%Y%m%d %H:%M:%S")?;
    let index_name = ticker_index_name(&time);

    bulk_index(&context, &index_name, &tickers).await?;

    Ok(())
}

pub async fn index_protfolios_from_file(context: &AppContext, path: &str) -> Result<()> {
    info!("Import messages from {}", &path);

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let protfolios: Vec<Protfolio> = reader
        .lines()
        .into_iter()
        .map(|w| w.unwrap())
        .map(|line| serde_json::from_str::<Protfolio>(&line).unwrap())
        .collect();

    info!("Protfolio size: {} for {}", &protfolios.len(), &path);

    // generate index name
    let time = Utc.timestamp_millis(protfolios.first().unwrap().time);
    let index_name = protfolio_index_name(&time);

    bulk_index(&context, &index_name, &protfolios).await?;

    Ok(())
}

pub async fn bulk_index<T>(context: &AppContext, index_name: &str, list: &Vec<T>) -> Result<()>
where
    T: Serialize + Debug,
{
    let persistence = context.persistence();
    let client: Elasticsearch = persistence.get_connection()?;

    let mut body: Vec<JsonBody<_>> = Vec::new();
    for item in list {
        body.push(json!({"index": {}}).into());
        body.push(json!(item).into());
    }

    debug!(
        "Bulk import messages into index: {}, count: {}",
        &index_name,
        list.len()
    );
    let response = client
        .bulk(BulkParts::Index(&index_name))
        .body(body)
        .send()
        .await?;

    debug!(
        "response {} for index {}",
        response.status_code(),
        &index_name
    );

    Ok(())
}
