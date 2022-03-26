use super::{DataSource, PersistenceContext};
use crate::{
    proto::biz::TickerEvent,
    vo::{
        biz::{MarketHoursType, Protfolio, QuoteType, SlopeLine, Ticker},
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
    cmp::max,
    collections::HashMap,
    fmt::Debug,
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
};

const INDEX_PREFIX_TICKER: &str = "sminer-ticker";
const INDEX_PREFIX_PROTFOLIO: &str = "sminer-protfolio";
const INDEX_PREFIX_SLOPE: &str = "sminer-slope";

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
        ElasticTicker {
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
        ElasticTicker {
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
}

fn take_digitals(str: &str) -> String {
    str.chars().filter(|c| c.is_numeric()).collect::<String>()
}

pub fn take_index_time(name: &str) -> DateTime<Utc> {
    let digital = take_digitals(name);
    Utc.datetime_from_str(&format!("{} 00:00:00", digital), "%Y%m%d %H:%M:%S")
        .unwrap()
}

pub fn slope_index_name(time: &DateTime<Utc>) -> String {
    format!("{}-{}", INDEX_PREFIX_SLOPE, time.format("%Y-%m-%d"))
}

pub fn ticker_index_name(time: &DateTime<Utc>) -> String {
    format!("{}-{}", INDEX_PREFIX_TICKER, time.format("%Y-%m-%d"))
}

pub fn protfolio_index_name(time: &DateTime<Utc>) -> String {
    format!("{}-{}", INDEX_PREFIX_PROTFOLIO, time.format("%Y-%m-%d"))
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

    let persistence = context.persistence();
    let client: Elasticsearch = persistence.get_connection()?;

    let mut body: Vec<JsonBody<_>> = Vec::new();
    for ticker in tickers {
        body.push(json!({"index": {}}).into());
        body.push(json!(ticker).into());
    }

    // generate index name
    let digital = take_digitals(&path);
    let time = Utc.datetime_from_str(&format!("{} 00:00:00", digital), "%Y%m%d %H:%M:%S")?;
    let index_name = ticker_index_name(&time);

    info!("Bulk import messages into index: {}", &index_name);
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

    index_protfolios(&context, &protfolios).await?;

    Ok(())
}

pub async fn index_protfolios(context: &AppContext, protfolios: &Vec<Protfolio>) -> Result<()> {
    let persistence = context.persistence();
    let client: Elasticsearch = persistence.get_connection()?;

    let mut body: Vec<JsonBody<_>> = Vec::new();
    for protfolio in protfolios {
        body.push(json!({"index": {}}).into());
        body.push(json!(protfolio).into());
    }

    // generate index name
    let time = Utc.timestamp_millis(protfolios.first().unwrap().time);
    let index_name = protfolio_index_name(&time);

    debug!("Bulk import messages into index: {}", &index_name);
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

pub async fn index_slope_points(context: &AppContext, slope_points: &Vec<SlopeLine>) -> Result<()> {
    let persistence = context.persistence();
    let client: Elasticsearch = persistence.get_connection()?;

    let mut body: Vec<JsonBody<_>> = Vec::new();
    for point in slope_points {
        body.push(json!({"index": {}}).into());
        body.push(json!(point).into());
    }

    // generate index name
    let time = Utc.timestamp_millis(slope_points.first().unwrap().time);
    let index_name = slope_index_name(&time);

    debug!("Bulk import messages into index: {}", &index_name);
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
