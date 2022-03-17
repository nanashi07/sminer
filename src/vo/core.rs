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
    pub calculate: Sender<TickerEvent>,
}

impl AppContext {
    pub fn new() -> AppContext {
        let (tx1, _) = channel::<TickerEvent>(64);
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
}

pub struct Config {}
