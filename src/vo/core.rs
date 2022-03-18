use super::biz::Ticker;
use crate::{
    analysis::init_dispatcher,
    persist::{es::ElasticTicker, PersistenceContext},
    proto::biz::TickerEvent,
    Result,
};
use config::Config;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, LinkedList},
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast::{channel, Sender};

#[derive(Debug)]
pub struct AppContext {
    pub config: Arc<AppConfig>,
    pub persistence: Arc<PersistenceContext>,
    pub tickers: Arc<RwLock<HashMap<String, RwLock<LinkedList<Ticker>>>>>,
    pub statistic: Arc<RwLock<HashMap<String, RwLock<LinkedList<Ticker>>>>>,
    // Sender for persist data
    pub data_sender: Sender<TickerEvent>,
    // Sender for statistic calculation
    pub statistic_sender: Sender<TickerEvent>,
}

impl AppContext {
    pub fn new(config: AppConfig) -> AppContext {
        let (data_sender, _) = channel::<TickerEvent>(2048);
        let (statistic_sender, _) = channel::<TickerEvent>(2048);
        let config_ref = Arc::new(config);
        let c = Arc::clone(&config_ref);
        AppContext {
            config: config_ref,
            persistence: Arc::new(PersistenceContext::new(c)),
            tickers: Arc::new(RwLock::new(HashMap::new())),
            statistic: Arc::new(RwLock::new(HashMap::new())),
            data_sender,
            statistic_sender,
        }
    }
    pub async fn init(self) -> Result<Arc<AppContext>> {
        let me = Arc::new(self);
        init_dispatcher(&Arc::clone(&me)).await?;
        // FIXME: init mongo for temp solution
        let config = &me.config;
        Arc::clone(&me).persistence.init_mongo(config).await?;
        Ok(Arc::clone(&me))
    }
    pub async fn dispatch(&self, ticker: &Ticker) -> Result<()> {
        self.data_sender.send(ticker.into())?;
        self.statistic_sender.send(ticker.into())?;
        Ok(())
    }
    pub async fn dispatch_direct(&self, ticker: &Ticker) -> Result<()> {
        // save data
        ticker.save_to_mongo(Arc::clone(&self.persistence)).await?;
        let es_ticker: ElasticTicker = (*ticker).clone().into();
        es_ticker
            .save_to_elasticsearch(Arc::clone(&self.persistence))
            .await?;
        // TODO: calculate

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    #[serde(rename = "dataSource")]
    pub data_source: DataSource,
    pub tickers: TickerList,
}

impl AppConfig {
    pub fn new() -> AppConfig {
        AppConfig {
            data_source: DataSource {
                mongodb: DataSourceInfo { uri: String::new() },
                elasticsearch: DataSourceInfo { uri: String::new() },
            },
            tickers: TickerList {
                symbols: vec![
                    TickerGroup {
                        bull: Symbol { id: String::new() },
                        bear: Symbol { id: String::new() },
                    },
                    TickerGroup {
                        bull: Symbol { id: String::new() },
                        bear: Symbol { id: String::new() },
                    },
                ],
            },
        }
    }
    pub fn load(file: &str) -> Result<AppConfig> {
        let settings = Config::builder()
            .add_source(config::File::with_name(file))
            .build()?;

        let config: AppConfig = settings.try_deserialize::<AppConfig>()?;
        Ok(config)
    }
    pub fn symbols(&self) -> Vec<&str> {
        self.tickers
            .symbols
            .iter()
            .flat_map(|g| [&g.bear.id, &g.bull.id])
            .map(|id| id.as_str())
            .collect::<Vec<&str>>()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataSource {
    pub mongodb: DataSourceInfo,
    pub elasticsearch: DataSourceInfo,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataSourceInfo {
    pub uri: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TickerList {
    pub symbols: Vec<TickerGroup>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TickerGroup {
    pub bull: Symbol,
    pub bear: Symbol,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Symbol {
    pub id: String,
}
