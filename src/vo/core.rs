use super::biz::Ticker;
use crate::{
    persist::{es::ElasticTicker, PersistenceContext},
    proto::biz::TickerEvent,
    Result,
};
use std::{
    collections::{HashMap, LinkedList},
    sync::Arc,
};
use tokio::sync::broadcast::{channel, Sender};

pub struct AppContext {
    pub persistence: Arc<PersistenceContext>,
    pub tickers: HashMap<String, LinkedList<Ticker>>,

    pub sender: Sender<TickerEvent>,
    pub calculate: Sender<TickerEvent>,
}

impl AppContext {
    pub fn new() -> AppContext {
        let (tx1, _) = channel::<TickerEvent>(2048);
        let (tx2, _) = channel::<TickerEvent>(64);

        AppContext {
            persistence: Arc::new(PersistenceContext::new()),
            tickers: HashMap::new(),
            sender: tx1,
            calculate: tx2,
        }
    }
    pub async fn dispatch(&self, ticker: &Ticker) -> Result<()> {
        self.sender.send(ticker.into())?;
        // self.calculate.send(ticker.into())?;
        Ok(())
    }
    pub async fn dispatch_direct(&self, ticker: &Ticker) -> Result<()> {
        ticker.save_to_mongo(Arc::clone(&self.persistence)).await?;
        let es_ticker: ElasticTicker = (*ticker).clone().into();
        es_ticker
            .save_to_elasticsearch(Arc::clone(&self.persistence))
            .await?;
        Ok(())
    }
}

pub struct Config {}
