use super::biz::Ticker;
use crate::{
    analysis::init_dispatcher,
    persist::{es::ElasticTicker, PersistenceContext},
    proto::biz::TickerEvent,
    Result,
};
use std::{
    collections::{HashMap, LinkedList},
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast::{channel, Sender};

#[derive(Debug)]
pub struct AppContext {
    pub persistence: Arc<PersistenceContext>,
    pub tickers: Arc<RwLock<HashMap<String, RwLock<LinkedList<Ticker>>>>>,
    pub statistic: Arc<RwLock<HashMap<String, RwLock<LinkedList<Ticker>>>>>,
    // Sender for persist data
    pub data_sender: Sender<TickerEvent>,
    // Sender for statistic calculation
    pub statistic_sender: Sender<TickerEvent>,
}

impl AppContext {
    pub fn new() -> AppContext {
        let (data_sender, _) = channel::<TickerEvent>(2048);
        let (statistic_sender, _) = channel::<TickerEvent>(2048);

        AppContext {
            persistence: Arc::new(PersistenceContext::new()),
            tickers: Arc::new(RwLock::new(HashMap::new())),
            statistic: Arc::new(RwLock::new(HashMap::new())),
            data_sender,
            statistic_sender,
        }
    }
    pub async fn init(self, _: &Config) -> Result<Arc<AppContext>> {
        let me = Arc::new(self);
        init_dispatcher(&Arc::clone(&me)).await?;
        // FIXME: init mongo for temp solution
        Arc::clone(&me).persistence.init_mongo().await?;
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
        // calculate

        Ok(())
    }
}

#[derive(Debug)]
pub struct Config {}

impl Config {
    pub fn new() -> Config {
        Config {}
    }
}
