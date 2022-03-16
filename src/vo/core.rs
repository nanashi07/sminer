use super::biz::Ticker;
use crate::{persist::PersistenceContext, proto::biz::TickerEvent, Result};
use std::{
    collections::{HashMap, LinkedList},
    sync::Arc,
};
use tokio::sync::broadcast::{channel, Sender};

pub struct AppContext {
    pub persistence: Arc<PersistenceContext>,
    pub tickers: HashMap<String, LinkedList<Ticker>>,

    pub sender: Sender<TickerEvent>,
}

impl AppContext {
    pub fn new() -> AppContext {
        let (tx, _) = channel::<TickerEvent>(128);

        AppContext {
            persistence: Arc::new(PersistenceContext::new()),
            tickers: HashMap::new(),
            sender: tx,
        }
    }
    pub async fn dispatch(&self, ticker: &Ticker) -> Result<()> {
        self.sender.send(ticker.into())?;
        Ok(())
    }
}

pub struct Config {}
