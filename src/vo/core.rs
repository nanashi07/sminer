use super::biz::Ticker;
use crate::persist::PersistenceContext;
use std::collections::{HashMap, LinkedList};

pub struct AppContext {
    pub persistence: PersistenceContext,
    pub tickers: HashMap<String, LinkedList<Ticker>>,
}

impl AppContext {
    pub fn new() -> AppContext {
        AppContext {
            persistence: PersistenceContext::new(),
            tickers: HashMap::new(),
        }
    }
}
